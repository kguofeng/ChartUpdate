"""
Consolidated Chart Updater — replaces charts_updater.py through charts_updater5.py.

Usage:
    python charts_updater_all.py                     # Run ALL charts (default)
    python charts_updater_all.py "LMCI" "PMI"        # Run specific charts by name substring
    python charts_updater_all.py --group updater3     # Run charts from original file 3
    python charts_updater_all.py --list               # List all available chart names
"""

# ==============================================================================
# SECTION 1: IMPORTS
# ==============================================================================
import os
import re
import sys
import time
import random
import itertools
import argparse
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import threading
import matplotlib
matplotlib.use('Agg')   # MUST be before pyplot import — forces non-interactive
                        # file-only backend; no Tk/GUI event loop, fully thread-safe
import matplotlib.pyplot as plt

import matplotlib.image as mpimg
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter, MultipleLocator, FormatStrFormatter
from matplotlib.dates import WeekdayLocator, DateFormatter, MO
from matplotlib.gridspec import GridSpec
from matplotlib.colors import TwoSlopeNorm
from pandas.tseries.offsets import BDay, CustomBusinessDay
from dateutil.relativedelta import relativedelta

from xbbg import blp
from sklearn.linear_model import LinearRegression
import requests

import bbgui
from chart_utils import clean_data, calculate_and_plot, base_series_to_date
from exante_utils import get_data as exante_get_data
from ecom_utils import bfill_cny
from download_bi_monetary_data import get_adjusted_m0_data, compute_yoy_growth

# Lazy imports for statsmodels (scipy compatibility issues in Python 3.13)
try:
    import statsmodels.api as sm
    from statsmodels.regression.rolling import RollingOLS
    STATSMODELS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: statsmodels import failed ({e}). Some charts may not be generated.")
    print("To fix: pip install --upgrade statsmodels scipy")
    sm = None
    RollingOLS = None
    STATSMODELS_AVAILABLE = False
_bbg_lock = threading.Lock()
# ==============================================================================
# SECTION 2: GLOBAL CONSTANTS
# ==============================================================================
G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")
G_END_DATE = datetime.today()
_CHART_UPDATE_DIR = os.path.dirname(os.path.abspath(__file__))
G_CHART_DIR = Path(r"O:\Tian\Portal\Charts\ChartDataBase")
FONTSIZE = 14

# Module-level toggle for quick dev iteration
RUN_ONLY = None  # Set to ["LMCI", "PMI"] to run a subset, None = all

# ==============================================================================
# SECTION 3: SHARED HELPER FUNCTIONS
# ==============================================================================

# --- Intraday correlation toolkit (from charts_updater4.py) ---

def _intracorr_bdh_flat(tickers, field, start, end, **kw):
    """Fetch Bloomberg daily data and flatten MultiIndex columns."""
    raw = blp.bdh(tickers=tickers, flds=[field], start_date=start, end_date=end, **kw)
    if isinstance(raw.columns, pd.MultiIndex):
        if field in raw.columns.get_level_values(-1):
            raw = raw.xs(field, level=-1, axis=1)
        else:
            raw = raw.xs(field, level=0, axis=1)
    raw.index = pd.to_datetime(raw.index)
    return raw.sort_index()


def _intracorr_align_and_diff(df, sofr_col):
    """Return daily changes aligned on common business-day index."""
    out = df.copy()
    out = out.reindex(pd.bdate_range(out.index.min(), out.index.max()))
    out = out.ffill(limit=3)
    out = out.diff()
    out = out.dropna(how='all')
    return out


def _intracorr_sofr_ticker(tenor):
    return f'USOSFR{tenor} CMPT Curncy'


def _intracorr_ensure_2d(df):
    if isinstance(df, pd.Series):
        return df.to_frame()
    return df


def _intracorr_directional_beta(x, y, label, min_move):
    """Compute up/down beta and r-squared for a pair of daily-change series."""
    xy = pd.concat([x.rename('x'), y.rename('y')], axis=1).dropna()
    up = xy[xy['x'] > min_move]
    dn = xy[xy['x'] < -min_move]
    out = {}
    for tag, sub in [('up', up), ('dn', dn)]:
        if len(sub) < 5:
            out[f'{label}_{tag}_beta'] = np.nan
            out[f'{label}_{tag}_r2'] = np.nan
            out[f'{label}_{tag}_n'] = 0
            continue
        X = sub[['x']].values
        Y = sub['y'].values
        reg = LinearRegression(fit_intercept=True).fit(X, Y)
        out[f'{label}_{tag}_beta'] = reg.coef_[0]
        out[f'{label}_{tag}_r2'] = reg.score(X, Y)
        out[f'{label}_{tag}_n'] = len(sub)
    return out


def _intracorr_compute_bar_betas(diff_df, sofr_col, rate_cols, min_move):
    rows = []
    for col in rate_cols:
        res = _intracorr_directional_beta(diff_df[sofr_col], diff_df[col], col, min_move)
        rows.append(res)
    return pd.DataFrame(rows)


def _intracorr_rolling_directional_betas(diff_df, sofr_col, ccy_col, min_move, window):
    """Rolling up/down beta of ccy_col vs sofr_col."""
    xy = pd.concat([diff_df[sofr_col].rename('x'),
                     diff_df[ccy_col].rename('y')], axis=1).dropna()
    dates, up_b, dn_b = [], [], []
    for end in range(window, len(xy)):
        sub = xy.iloc[end - window:end]
        up = sub[sub['x'] > min_move]
        dn = sub[sub['x'] < -min_move]
        d = xy.index[end]
        dates.append(d)
        if len(up) >= 5:
            reg = LinearRegression(fit_intercept=True).fit(up[['x']].values, up['y'].values)
            up_b.append(reg.coef_[0])
        else:
            up_b.append(np.nan)
        if len(dn) >= 5:
            reg = LinearRegression(fit_intercept=True).fit(dn[['x']].values, dn['y'].values)
            dn_b.append(reg.coef_[0])
        else:
            dn_b.append(np.nan)
    return pd.DataFrame({'date': dates, 'up_beta': up_b, 'dn_beta': dn_b}).set_index('date')


def _intracorr_last_roll_date(df):
    return df.dropna(how='all').index.max()


def _intracorr_style_time_axis(ax, start, end):
    span_days = (end - start).days
    if span_days <= 120:
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=MO))
        ax.xaxis.set_major_formatter(DateFormatter('%d-%b'))
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(DateFormatter('%b-%y'))
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', fontsize=8)


def _intracorr_plot_region_grid(bar_df, roll_dict, region_tag, sofr_label,
                                 bar_start, roll_start, end_date, out_path):
    """6-column grid: bar up/dn then rolling up/dn per currency."""
    ccys = list(roll_dict.keys())
    n = len(ccys)
    fig, axes = plt.subplots(n, 4, figsize=(22, 3.0 * n),
                              gridspec_kw={'width_ratios': [1, 1, 2, 2]})
    if n == 1:
        axes = axes[np.newaxis, :]

    for i, ccy in enumerate(ccys):
        cols_up = [c for c in bar_df.columns if c.endswith('_up_beta')]
        cols_dn = [c for c in bar_df.columns if c.endswith('_dn_beta')]

        ccy_up_col = [c for c in cols_up if ccy in c or bar_df.columns[0].startswith(ccy)]
        ccy_dn_col = [c for c in cols_dn if ccy in c or bar_df.columns[0].startswith(ccy)]

        # Bar charts
        ax_up_bar = axes[i, 0]
        ax_dn_bar = axes[i, 1]
        rdf = roll_dict[ccy]

        if not rdf.empty:
            ax_up_bar.barh([ccy], [rdf['up_beta'].iloc[-5:].mean() if len(rdf) >= 5 else np.nan],
                           color='steelblue', edgecolor='navy')
            ax_dn_bar.barh([ccy], [rdf['dn_beta'].iloc[-5:].mean() if len(rdf) >= 5 else np.nan],
                           color='salmon', edgecolor='darkred')

        ax_up_bar.set_title('Up β' if i == 0 else '', fontsize=9)
        ax_dn_bar.set_title('Dn β' if i == 0 else '', fontsize=9)
        ax_up_bar.set_ylabel(ccy, fontsize=10, fontweight='bold')

        # Rolling charts
        ax_up_roll = axes[i, 2]
        ax_dn_roll = axes[i, 3]

        if not rdf.empty:
            ax_up_roll.plot(rdf.index, rdf['up_beta'], color='steelblue', lw=1.5)
            ax_up_roll.axhline(1.0, ls='--', color='grey', lw=0.8)
            ax_dn_roll.plot(rdf.index, rdf['dn_beta'], color='salmon', lw=1.5)
            ax_dn_roll.axhline(1.0, ls='--', color='grey', lw=0.8)

        ax_up_roll.set_title('Rolling Up β' if i == 0 else '', fontsize=9)
        ax_dn_roll.set_title('Rolling Dn β' if i == 0 else '', fontsize=9)

        _intracorr_style_time_axis(ax_up_roll, roll_start, end_date)
        _intracorr_style_time_axis(ax_dn_roll, roll_start, end_date)

        for ax in [ax_up_bar, ax_dn_bar, ax_up_roll, ax_dn_roll]:
            ax.grid(True, ls=':', alpha=0.4)

    fig.suptitle(f'{region_tag} 5Y vs {sofr_label} — Up/Down Beta',
                 fontsize=14, y=1.01)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved {out_path}')


# --- Intraday high-frequency correlation helpers (from charts_updater4.py) ---

def _hf_as_utc_index(df):
    """Ensure DataFrame index is tz-aware UTC."""
    if df.index.tz is None:
        return df.tz_localize('UTC')
    return df.tz_convert('UTC')


def _hf_extract_close(raw, ticker):
    """Pull the 'close' column for a given ticker from bdib output."""
    if isinstance(raw.columns, pd.MultiIndex):
        if ticker in raw.columns.get_level_values(0):
            sub = raw[ticker]
        else:
            sub = raw.xs(ticker, axis=1, level=1)
        if 'close' in sub.columns:
            return sub['close']
        return sub.iloc[:, -1]
    if 'close' in raw.columns:
        return raw['close']
    return raw.iloc[:, -1]


def _hf_pull_intraday_minutes(ticker, days, interval=1):
    """Pull last N days of intraday minute bars via xbbg."""
    raw = blp.bdib(ticker, dt='last', session='allday',
                   typ='TRADE', interval=interval)
    return _hf_extract_close(raw, ticker)


def _hf_align_minutes(s1, s2):
    """Align two minute-bar series on their intersection."""
    idx = s1.index.intersection(s2.index)
    return s1.reindex(idx), s2.reindex(idx)


def _hf_last_every_n_minutes(s, n=5):
    """Downsample minute bars to every N minutes (last value)."""
    return s.resample(f'{n}min').last().dropna()


def _hf_rolling_corr(s1, s2, window):
    """Rolling correlation between two series."""
    return s1.rolling(window, min_periods=max(10, window // 3)).corr(s2)


def _hf_rolling_partial_corr_xyz(x, y, z, window):
    """Partial correlation rho_xy.z via pairwise formula, rolling."""
    r_xy = _hf_rolling_corr(x, y, window)
    r_xz = _hf_rolling_corr(x, z, window)
    r_yz = _hf_rolling_corr(y, z, window)
    num = r_xy - r_xz * r_yz
    denom = np.sqrt((1 - r_xz ** 2) * (1 - r_yz ** 2))
    out = num / denom
    out[(denom <= 1e-12) | (~np.isfinite(denom))] = np.nan
    return out


def _hf_rolling_beta(y, x, window):
    """Rolling OLS beta of y on x."""
    cov = y.rolling(window).cov(x)
    var = x.rolling(window).var()
    return cov / var


def _hf_rolling_corr_time_capped(s1, s2, window, max_time=None):
    """Rolling correlation, optionally capping at a time of day."""
    if max_time is not None:
        mask = s1.index.time <= max_time
        s1, s2 = s1[mask], s2[mask]
    return _hf_rolling_corr(s1, s2, window)


def _hf_last_plotted_timestamp(s):
    """Return last non-NaN timestamp."""
    valid = s.dropna()
    return valid.index[-1] if len(valid) > 0 else None


def _hf_setup_intraday_corr_ax(ax, title, ylabel='Correlation'):
    """Standard axis formatting for intraday correlation charts."""
    ax.set_title(title, fontsize=11)
    ax.set_ylabel(ylabel)
    ax.axhline(0, color='black', lw=0.8)
    ax.grid(True, ls=':', alpha=0.5)
    ax.legend(loc='best', fontsize=8)


# --- Oil Beta helpers ---

def _oil_beta_as_utc_index(idx):
    ts = pd.DatetimeIndex(idx)
    return ts.tz_localize('UTC') if ts.tz is None else ts.tz_convert('UTC')


def _oil_beta_extract_close(df, ticker):
    sidx = _oil_beta_as_utc_index(df.index)
    if isinstance(df.columns, pd.MultiIndex):
        if ticker in df.columns.get_level_values(0):
            close = df[ticker].loc[:, 'close']
        else:
            close = df.xs('close', axis=1, level=-1).iloc[:, 0]
    else:
        close = df['close']
    close = pd.Series(close.values, index=sidx, name=ticker).sort_index()
    close = close[~close.index.duplicated(keep='last')]
    return close


def _oil_beta_pull_intraday(ticker, n_days, ref):
    """Pull 1-min bars via blp.bdib() per-day loop."""
    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=n_days - 1)
    dates = pd.date_range(start, end, freq='D')
    parts = []
    for d in dates:
        dt_str = d.strftime('%Y-%m-%d')
        try:
            df = blp.bdib(ticker=ticker, dt=dt_str, ref=ref)
            if df is None or df.empty:
                continue
            ser = _oil_beta_extract_close(df, ticker)
            if not ser.empty:
                parts.append(ser)
        except Exception as e:
            print(f"[oil_beta][{ticker}] {dt_str} skipped: {e}")
            continue
    if not parts:
        raise RuntimeError(f"No intraday data for {ticker} over last {n_days} days.")
    out = pd.concat(parts).sort_index()
    out.index = _oil_beta_as_utc_index(out.index)
    return out.rename(ticker)


def _oil_beta_to_minute_last(s):
    s = s.copy()
    s.index = s.index.floor('T')
    return s[~s.index.duplicated(keep='last')]


def _oil_beta_last_every_n_minutes(df_minute, n):
    df = df_minute.copy()
    df.index = df.index.floor('T')
    df = df[~df.index.duplicated(keep='last')]
    pos = np.arange(len(df))
    blk_id = pos // n
    out = df.groupby(blk_id, sort=True).tail(1)
    return out


def _oil_beta_find_session_segments(index, gap_threshold_minutes=60):
    """Scan DatetimeIndex for gaps > gap_threshold, return list of (start, end) int slices."""
    if len(index) < 2:
        return [(0, len(index))]
    diffs = np.diff(index.asi8) / 1e9 / 60  # minutes
    segments = []
    seg_start = 0
    for i, d in enumerate(diffs):
        if d > gap_threshold_minutes:
            segments.append((seg_start, i + 1))
            seg_start = i + 1
    segments.append((seg_start, len(index)))
    return segments


def _oil_beta_rolling_stats_gap_aware(
        oil_ret, asset_ret, roll_window,
        gap_threshold_min=60, min_segment_blocks=24):
    """
    Rolling correlation + directional correlation, gap-aware.

    Returns dict with keys:
        'corr'     – unconditional rolling correlation (unchanged)
        'corr_up'  – rolling correlation computed only on oil-up blocks     [NEW]
        'corr_dn'  – rolling correlation computed only on oil-down blocks   [NEW]

    Why directional *correlation* instead of directional *beta*?
    ────────────────────────────────────────────────────────────
    • Beta = cov(oil,asset) / var(oil).  In a quiet window var(oil) can be
      very small, making beta blow up → chart-dominating spikes.
    • Normalising beta by vol ratio fixes the scale problem but re-introduces
      spikes because you're then dividing by asset realised vol, which can
      also be tiny in a quiet window.
    • Correlation = beta × σ_oil / σ_asset is bounded [-1, 1] by construction.
      It can never spike.  Directional correlation (corr computed on the
      oil-up or oil-down subset) preserves the asymmetry information without
      any of the instability.
    """
    xy = pd.concat([oil_ret.rename('x'), asset_ret.rename('y')], axis=1).dropna()
    empty = {
        'corr':    pd.Series(dtype=float),
        'corr_up': pd.Series(dtype=float),
        'corr_dn': pd.Series(dtype=float),
    }
    if len(xy) < roll_window:
        return empty

    segments = _oil_beta_find_session_segments(xy.index, gap_threshold_min)

    corr_idx, corr_vals = [], []
    up_idx,   up_vals   = [], []
    dn_idx,   dn_vals   = [], []

    for seg_start, seg_end in segments:
        seg = xy.iloc[seg_start:seg_end]
        if len(seg) < min_segment_blocks:
            continue

        for end_pos in range(roll_window, len(seg)):
            window = seg.iloc[end_pos - roll_window:end_pos]
            ts = window.index[-1]

            # ── Unconditional correlation (unchanged logic) ──────────────
            c = window['x'].corr(window['y'])
            corr_idx.append(ts)
            corr_vals.append(c)

            # ── Directional correlation (NEW – replaces OLS beta) ────────
            up = window[window['x'] > 0]
            dn = window[window['x'] < 0]

            # Require min 5 observations each direction; np.nan otherwise.
            # corr() returns NaN automatically if std == 0, so no extra guard needed.
            up_idx.append(ts)
            up_vals.append(up['x'].corr(up['y']) if len(up) >= 5 else np.nan)

            dn_idx.append(ts)
            dn_vals.append(dn['x'].corr(dn['y']) if len(dn) >= 5 else np.nan)

    return {
        'corr':    pd.Series(corr_vals, index=pd.DatetimeIndex(corr_idx), name='corr'),
        'corr_up': pd.Series(up_vals,   index=pd.DatetimeIndex(up_idx),   name='corr_up'),
        'corr_dn': pd.Series(dn_vals,   index=pd.DatetimeIndex(dn_idx),   name='corr_dn'),
    }
def _fetch_one_day(ticker, dt_str, ref, lock):
    """Fetch one day of bdib data for one ticker. Designed for thread pool."""
    try:
        # If your Bloomberg session is shared and not thread-safe, acquire lock here.
        # xbbg opens a fresh socket per call so usually the lock is not needed.
        with lock:
            df = blp.bdib(ticker=ticker, dt=dt_str, ref=ref)
        if df is None or df.empty:
            return None
        ser = _oil_beta_extract_close(df, ticker)
        return ser if not ser.empty else None
    except Exception as e:
        print(f"[oil_beta][{ticker}] {dt_str} skipped: {e}")
        return None


def _oil_beta_pull_intraday_parallel(ticker, n_days, ref, max_day_workers=8):
    """
    Pull N days of 1-min bars concurrently.

    max_day_workers: Bloomberg typically handles 8-12 concurrent intraday
                     requests before throttling.  Keep ≤ 10 to be safe.
    """
    end   = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=n_days - 1)
    dates = pd.date_range(start, end, freq='D')

    parts = [None] * len(dates)

    with ThreadPoolExecutor(max_workers=max_day_workers) as pool:
        # Submit all days; keep index so we can maintain ordering if needed
        future_to_idx = {
            pool.submit(_fetch_one_day, ticker, d.strftime('%Y-%m-%d'), ref, _bbg_lock): i
            for i, d in enumerate(dates)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            result = future.result()
            if result is not None:
                parts[idx] = result

    parts = [p for p in parts if p is not None]
    if not parts:
        raise RuntimeError(f"No intraday data for {ticker} over last {n_days} days.")

    out = pd.concat(parts).sort_index()
    out.index = _oil_beta_as_utc_index(out.index)
    return out.rename(ticker)



# ---------------------------------------------------------------------------
# Level 2: parallelised per-ticker pull + computation
# ---------------------------------------------------------------------------
def _process_one_ticker(label, ticker, oil_m, n_days, ref,
                         block_minutes, roll_window,
                         gap_threshold_min, min_segment_blocks,
                         max_day_workers):
    """
    Full pipeline for one asset ticker:
      pull → align → resample → log-ret → rolling stats → LT corr

    Returns (label, result_dict) or (label, Exception).
    Called from a thread pool, so must be thread-safe.
    matplotlib must NOT be called here.
    """
    try:
        print(f"[oil_beta] Pulling {label}: {ticker}")
        asset_min = _oil_beta_pull_intraday_parallel(
            ticker, n_days, ref, max_day_workers=max_day_workers)
        asset_m = _oil_beta_to_minute_last(asset_min)

        # Align on union index, forward-fill gaps ≤ 3 bars, drop remaining NaN
        idx = oil_m.index.union(asset_m.index).sort_values()
        px  = pd.concat([oil_m.reindex(idx), asset_m.reindex(idx)], axis=1)
        px  = px.ffill(limit=3).dropna()

        px_blk    = _oil_beta_last_every_n_minutes(px, block_minutes)
        ret       = np.log(px_blk).diff().dropna()
        oil_ret   = ret.iloc[:, 0]
        asset_ret = ret.iloc[:, 1]

        results = _oil_beta_rolling_stats_gap_aware(
            oil_ret, asset_ret, roll_window, gap_threshold_min, min_segment_blocks)

        lt_corr = oil_ret.corr(asset_ret)

        return label, {
            'results': results,
            'lt_corr': lt_corr,
            'ok':      True,
        }

    except Exception as e:
        print(f"[oil_beta] Error for {label}: {e}")
        return label, {'ok': False, 'error': e}

# ---------------------------------------------------------------------------
# Master orchestrator (drop-in replacement for _oil_beta_compute_chart)
# ---------------------------------------------------------------------------


def _oil_beta_compute_chart(
        ticker_map, oil_ticker, ref_ticker, chart_title,
        save_name, n_days=120, block_minutes=10,
        roll_window=36, plot_days=7,
        gap_threshold_min=60, min_segment_blocks=24,
        max_ticker_workers=5,    # parallel asset tickers
        max_day_workers=8):      # parallel days per ticker
    """
    Parallel drop-in replacement for the original _oil_beta_compute_chart.

    Concurrency model
    ─────────────────
    ┌─ ThreadPool (max_ticker_workers) ──────────────────────────────────┐
    │  ticker A  →  ThreadPool (max_day_workers)  →  compute stats  ─┐  │
    │  ticker B  →  ThreadPool (max_day_workers)  →  compute stats  ─┤  │
    │  ...                                                            │  │
    └─────────────────────────────────────────────────────────────────┘  │
    Collect all results dict ◄───────────────────────────────────────────┘
    Plot serially (matplotlib is not thread-safe)

    Tuning tips
    ───────────
    max_ticker_workers × max_day_workers  should stay ≤ ~40 concurrent
    Bloomberg connections to avoid throttling.  Default 5 × 8 = 40.
    If you see Bloomberg timeouts, reduce max_day_workers first.
    """
    try:
        OUT_DIR = Path(G_CHART_DIR)
    except Exception:
        OUT_DIR = Path.cwd()

    # ── Pull oil once (serial — all assets depend on it) ─────────────────
    print(f"[oil_beta] Pulling oil: {oil_ticker}")
    oil_min = _oil_beta_pull_intraday_parallel(
        oil_ticker, n_days, ref_ticker, max_day_workers=max_day_workers)
    oil_m = _oil_beta_to_minute_last(oil_min)

    labels = list(ticker_map.keys())

    # ── Pull + compute all asset tickers in parallel ──────────────────────
    computed = {}   # label → result dict

    with ThreadPoolExecutor(max_workers=max_ticker_workers) as pool:
        futures = {
            pool.submit(
                _process_one_ticker,
                label, ticker_map[label], oil_m,
                n_days, ref_ticker,
                block_minutes, roll_window,
                gap_threshold_min, min_segment_blocks,
                max_day_workers
            ): label
            for label in labels
        }
        for future in as_completed(futures):
            label, result = future.result()
            computed[label] = result

    # ── Plot (serial — matplotlib not thread-safe) ────────────────────────
    ncols = 3
    nrows = int(np.ceil(len(labels) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(18, 4 * nrows))
    axes = np.atleast_2d(axes)

    for k, label in enumerate(labels):   # preserve original ticker_map order
        r, c = divmod(k, ncols)
        ax   = axes[r, c]
        res  = computed.get(label, {'ok': False, 'error': 'Not computed'})

        if not res['ok']:
            ax.set_title(f"{label}: error", fontsize=10)
            ax.text(0.5, 0.5, str(res.get('error', ''))[:60],
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=8, color='red')
            continue

        results = res['results']
        lt_corr = res['lt_corr']

        if results['corr'].empty:
            ax.set_title(f"{label}: no data", fontsize=10)
            ax.axis('off')
            continue

        cutoff    = results['corr'].index[-1] - pd.Timedelta(days=plot_days)
        corr_plot = results['corr'].loc[cutoff:]
        up_plot   = results['corr_up'].reindex(corr_plot.index)
        dn_plot   = results['corr_dn'].reindex(corr_plot.index)

        pos = np.arange(len(corr_plot))
        ax.plot(pos, up_plot.values,   color='steelblue', lw=1.2, label='Corr Up')
        ax.plot(pos, dn_plot.values,   color='salmon',    lw=1.2, label='Corr Dn')
        ax.plot(pos, corr_plot.values, color='grey', alpha=0.5, lw=1.0,
                label=f'Corr={results["corr"].iloc[-1]:.3f}')
        ax.axhline(lt_corr, color='black', lw=1.0, label=f'LT corr={lt_corr:.2f}')
        ax.axhline(0, color='grey', ls='--', lw=0.8)

        ax.set_ylim(-1.05, 1.05)
        ax.set_yticks([-1, -0.5, 0, 0.5, 1])

        dates_only   = corr_plot.index.normalize()
        unique_dates = dates_only.unique()
        major_pos    = [int(pos[dates_only == d][0]) for d in unique_dates]
        major_lbl    = [d.strftime('%b %d') for d in unique_dates]
        ax.set_xticks(major_pos)
        ax.set_xticklabels(major_lbl, fontsize=7, rotation=45, ha='right')

        ax.set_title(f"{label}", fontsize=10)
        ax.grid(True, ls=':', alpha=0.4)
        ax.legend(loc='best', fontsize=7)

    for k in range(len(labels), nrows * ncols):
        r, c = divmod(k, ncols)
        axes[r, c].axis('off')

    fig.suptitle(chart_title, fontsize=14, fontweight='bold', y=1.01)
    fig.tight_layout()
    outpath = OUT_DIR / save_name
    fig.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"[oil_beta] Saved: {outpath}")


# --- MAS DLI helpers (from charts_updater5.py) ---

MAS_DLI_W_NEER = 0.6
MAS_DLI_W_SORA = 0.4
MAS_DLI_NEER_VAR_DIV = 2.0
MAS_DLI_C_DLI = "#4472C4"
MAS_DLI_C_PROXY = "#ED7D31"
MAS_DLI_C_NEER_BAR = "#70AD47"
MAS_DLI_C_SORA_BAR = "#FFC000"
MAS_DLI_C_ZERO = "black"
MAS_DLI_LINE_W = 2.5
MAS_DLI_BAR_WIDTH_DAYS = 25


def _mas_dli_fetch_daily_bbg(tickers, start, end):
    """Fetch daily Bloomberg data for MAS DLI."""
    df = blp.bdh(tickers, "PX_LAST", start, end)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    return df


def _mas_dli_to_monthly_bm_last(df_daily):
    """Resample daily to business-month-end, normalize to month-start timestamps."""
    try:
        m = df_daily.resample("BME").last()
    except ValueError:
        m = df_daily.resample("BM").last()
    m.index = m.index.to_period("M").to_timestamp()
    return m


def _mas_dli_load_dli_csv(csv_path):
    """Load MAS DLI 3-month change data from CSV."""
    d = pd.read_csv(csv_path, parse_dates=["date"])
    d["MAS_DLI_3m_change"] = pd.to_numeric(d["MAS_DLI_3m_change_pct"], errors="coerce")
    d = d.dropna(subset=["date", "MAS_DLI_3m_change"]).copy()
    d["date"] = d["date"].dt.to_period("M").dt.to_timestamp()
    d = d.set_index("date").sort_index()
    return d["MAS_DLI_3m_change"]


def _mas_dli_stacked_two_series_excel_like(ax, x, a, b, label_a, label_b, color_a, color_b, width_days):
    """Excel-like stacking for mixed signs."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    a_pos = np.clip(a, 0, None)
    a_neg = np.clip(a, None, 0)
    b_pos = np.clip(b, 0, None)
    b_neg = np.clip(b, None, 0)
    ax.bar(x, a_pos, width=width_days, label=label_a, color=color_a, align="center")
    ax.bar(x, a_neg, width=width_days, color=color_a, align="center")
    ax.bar(x, b_pos, width=width_days, bottom=a_pos, label=label_b, color=color_b, align="center")
    ax.bar(x, b_neg, width=width_days, bottom=a_neg, color=color_b, align="center")


def _mas_dli_compute_proxy_3m(neer_m, sora_m):
    """Compute 3-month proxy for MAS DLI."""
    out = pd.DataFrame({"NEER": neer_m, "SORA": sora_m}).copy()
    out["NEER_3m_pct"] = (out["NEER"] / out["NEER"].shift(3) - 1.0) * 100.0
    out["NEER_scaled_3m"] = out["NEER_3m_pct"] / MAS_DLI_NEER_VAR_DIV
    out["SORA_3m_pp"] = out["SORA"] - out["SORA"].shift(3)
    out["NEER_contrib_3m"] = MAS_DLI_W_NEER * out["NEER_scaled_3m"]
    out["SORA_contrib_3m"] = MAS_DLI_W_SORA * out["SORA_3m_pp"]
    out["Proxy_SORA_3m"] = out["NEER_contrib_3m"] + out["SORA_contrib_3m"]
    return out


def _mas_dli_compute_proxy_monthly_bc(neer_m, sora_m):
    """Compute monthly change proxy (BC calculated)."""
    out = pd.DataFrame({"NEER": neer_m, "SORA": sora_m}).copy()
    out["NEER_m_pct"] = (out["NEER"] / out["NEER"].shift(1) - 1.0) * 100.0
    out["SORA_m_pp"] = out["SORA"] - out["SORA"].shift(1)
    out["NEER_contrib_m"] = MAS_DLI_W_NEER * (out["NEER_m_pct"] / MAS_DLI_NEER_VAR_DIV)
    out["SORA_contrib_m"] = MAS_DLI_W_SORA * out["SORA_m_pp"]
    out["Proxy_m"] = out["NEER_contrib_m"] + out["SORA_contrib_m"]
    return out


# --- Updater5 helpers ---

def _u5_last_data_date_df(df):
    return df.dropna(how='all').index.max()


def _u5_last_data_date_series(s):
    return s.dropna().index[-1] if not s.dropna().empty else None


def _u5_monthly_series(s):
    """Resample to month-end."""
    return s.resample('M').last()


def _u5_normalize_monthly_series(s, name):
    if s is None or s.empty:
        raise RuntimeError(f"{name} series is empty after pull.")
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    s = s[~s.index.duplicated(keep="last")]
    s.index = s.index.to_period("M").to_timestamp("M")
    return s


# --- EPSGrowth helper (from charts_updater3.py) ---

def _plot_country_chart(title, country_data, save_name, chart_dir):
    """
    Plot EPSGrowth vs GDP-Wages x Operating Margin for a set of countries.
    country_data: list of (country_name, eps_series, gdp_wages_margin_series)
    """
    n = len(country_data)
    fig, axes = plt.subplots(n, 1, figsize=(14, 4 * n), sharex=False)
    if n == 1:
        axes = [axes]
    for i, (cname, eps, gwm) in enumerate(country_data):
        ax = axes[i]
        ax.set_title(cname, fontsize=12)
        if eps is not None and not eps.empty:
            ax.plot(eps.index, eps.values, label='EPS Growth', color='blue')
        ax2 = ax.twinx()
        if gwm is not None and not gwm.empty:
            ax2.plot(gwm.index, gwm.values, label='GDP-Wages x Op Margin', color='red', linestyle='--')
        ax.set_ylabel('EPS Growth', color='blue')
        ax2.set_ylabel('GDP-Wages x Op Margin', color='red')
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=8)
        ax.grid(True, ls=':', alpha=0.5)
    fig.suptitle(title, fontsize=14)
    plt.tight_layout()
    plt.savefig(os.path.join(chart_dir, save_name))
    plt.close(fig)


# ==============================================================================
# SECTION 4: CHART FUNCTIONS
# ==============================================================================

# --- Charts from charts_updater.py ---

def chart_20d_change_of_gsusfci():
    """20d Change of GSUSFCI -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")
    data = bbgui.bdh("GSUSFCI Index", "PX_LAST", START_DATE, END_DATE)
    data['20d diff'] = data.diff(20)
    data = data.dropna()
    data['20d diff'].plot()
    plt.hlines(data['20d diff'][-1], data['20d diff'].index[0],
               data['20d diff'].index[-1], colors="r")
    plt.xlabel("20d change in GS US Financial Conditions")
    plt.title(f"current FCI change at (red line) {data['20d diff'][-1]: 0.2f}")
    plt.savefig(os.path.join(G_CHART_DIR, "20d Change of GSUSFCI"))


def chart_hsi_vs_hibor3y():
    """HSI vs Hibor3y -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")
    data = bbgui.bdh(["HDSW3 Curncy", "HSI Index"],
                     "PX_LAST", START_DATE, END_DATE)
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["HDSW3 Curncy"], color="r", label="HDSWAP3Y")
    ax1.legend(loc=3)
    ax1.set_ylabel("HDSW3 Curncy")
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["HSI Index"], color="b", label="HSI")
    ax2.legend(loc=1)
    ax2.set_ylabel("HSI Index")
    plt.title(f"HSI vs HK 3y swap")
    plt.savefig(os.path.join(G_CHART_DIR, "HSI vs Hibor3y"))


def chart_eurusd_vs_ust():
    """EURUSD vs UST -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")
    data = bbgui.bdh(["EURUSD Curncy", "USGG7YR Index",
                     "USGG10YR Index"], "PX_LAST", START_DATE, END_DATE)
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["EURUSD Curncy"], color="r", label="EURUSD")
    ax1.legend(loc=3)
    ax1.set_ylabel("EURUSD Curncy")
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["USGG7YR Index"], color="b", label="7y TSY")
    ax2.plot(data.index, data["USGG10YR Index"], color="y", label="10y TSY")
    ax2.legend(loc=1)
    ax2.set_ylabel("US Treasury")
    plt.title(f"EURUSD vs US Gov Bond Px")
    plt.savefig(os.path.join(G_CHART_DIR, "EURUSD vs UST"))


def chart_lmci():
    """LMCI -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/92", "%d/%m/%y")
    data = bbgui.bdh(["KCMTLMCI Index"], "PX_LAST", START_DATE, END_DATE)
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["KCMTLMCI Index"], color="r", label="KCMTLMCI Index")
    ax1.legend(loc=0)
    ax1.set_ylabel("labor market condition index")
    plt.title(
        f"Current level at {data['KCMTLMCI Index'][-1]:0.2f} in {data.index[-1].strftime('%Y-%m')}")
    plt.savefig(os.path.join(G_CHART_DIR, "LMCI"))


def chart_pmi():
    """PMI -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/90", "%d/%m/%y")
    data = bbgui.bdh(["KOSPI Index", "MPMIUSCA Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='QUARTERLY')
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["KOSPI Index"], color="r", label="KOSPI Index")
    ax1.legend(loc=3)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["MPMIUSCA Index"], color="b", label="MPMIUSCA Index")
    ax2.legend(loc=1)
    plt.title(f"KOSPI vs US Composite PMI")
    plt.savefig(os.path.join(G_CHART_DIR, "PMI"))


def chart_lei():
    """LEI -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/90", "%d/%m/%y")
    data = bbgui.bdh(["LEI YOY Index", "EUCBLIYY Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='QUARTERLY')
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["LEI YOY Index"], color="r", label="US LEI")
    ax1.legend(loc=3)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["EUCBLIYY Index"], color="b", label="Eur LEI")
    ax2.legend(loc=1)
    plt.title(f"EUR vs US LEI")
    plt.savefig(os.path.join(G_CHART_DIR, "LEI"))


def chart_capacity_utilization():
    """CapacityUti -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/80", "%d/%m/%y")
    data = bbgui.bdh(["CPTICHNG Index", "FEDL01 Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='QUARTERLY')
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["CPTICHNG Index"], color="r",
             label="US capacity utilization (CPTICHNG Index)")
    ax1.legend(loc=3)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["FEDL01 Index"],
             color="b", label="effective fed fund")
    ax2.legend(loc=1)
    plt.title(f"Fed fund rate vs Capacity Utilization")
    plt.savefig(os.path.join(G_CHART_DIR, "CapacityUti"))


def chart_us_cpi_vs_wage():
    """USCPIvsWage -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/98", "%d/%m/%y")
    data = bbgui.bdh(["WGTROVER Index", "CPI YOY Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='QUARTERLY')
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["WGTROVER Index"], color="r",
             label="Wage Growth Tracker Atlanta Fed (WGTROVER Index)")
    ax1.legend(loc=3)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["CPI YOY Index"], color="b", label="US CPI YoY SA")
    ax2.legend(loc=1)
    plt.title(f"US CPI vs Wage Growth")
    plt.savefig(os.path.join(G_CHART_DIR, "USCPIvsWage"))


def chart_ez_wage():
    """EZWage -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/98", "%d/%m/%y")
    data = bbgui.bdh(["LNTNWEAY Index", "ECOXEAS Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='QUARTERLY')
    data["ECOXEAS Index YoY"] = data["ECOXEAS Index"].pct_change(4)
    data = data.dropna()
    fig, ax1 = plt.subplots()
    ax1.plot(data.index, data["LNTNWEAY Index"], color="r",
             label="L: Eurostat Labor Cost YoY (LNTNWEAY Index)")
    ax1.legend(loc=3)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["ECOXEAS Index YoY"], color="b",
             label="R: Eurzone Nominal GDP YoY SA(ECOXEAS Index)")
    ax2.legend(loc=1)
    plt.title(f"EUR Wage Growth vs Nominal GDP")
    plt.savefig(os.path.join(G_CHART_DIR, "EZWage"))


def chart_withheld_tax_vs_total_nfp():
    """Withheld Tax vs Total NFP -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/82", "%d/%m/%y")
    tax = bbgui.bdh("USCBFTWI Index", "PX_LAST",
                    START_DATE, END_DATE, interval="DAILY")
    tax['year'] = tax.index.year
    tax['month'] = tax.index.month
    tax_sum = tax.groupby(['year', 'month']).sum()
    tax_sum.columns = ["Withheld Tax"]
    tax_sum["Tax YoY"] = tax_sum.pct_change(12) * 100

    data = bbgui.bdh(["CPI YOY Index", "NFP T Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='MONTHLY')
    data['year'] = data.index.year
    data['month'] = data.index.month
    data = data.reset_index()
    data = data.set_index(['year', 'month'])
    data_combined = data.merge(tax_sum, left_index=True, right_index=True)
    data_combined['Real Tax YoY'] = data_combined['Tax YoY'] - \
        data_combined['CPI YOY Index']

    data_combined['Tax YoY 6month EWM'] = data_combined['Tax YoY'].ewm(
        span=6).mean()
    data_combined['Real Tax YoY 6month EWM'] = data_combined['Real Tax YoY'].ewm(
        span=6).mean()
    data_combined = data_combined.dropna()

    fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data_combined['date'], data_combined["NFP T Index"],
             color="r", label="Total NFP")
    ax1.legend(loc=3)
    ax2 = ax1.twinx()
    ax2.plot(data_combined['date'], data_combined["Tax YoY 6month EWM"],
             color="blue", label="Withheld Tax YoY 6month EWM")
    ax2.plot(data_combined['date'], data_combined["Real Tax YoY 6month EWM"], color="black",
             label="Real Withheld Tax YoY 6month EWM")
    ax2.hlines(0, xmin=data_combined['date'].to_list()[
               0], xmax=data_combined['date'].to_list()[-1])
    ax2.legend(loc=4)
    ax1.text(data_combined['date'].to_list()[0], 78000,
             "* real withheld tax YoY = withheld tax YoY - CPI YoY")
    ax1.text(data_combined['date'].to_list()[0], 150000,
             "* A turn in withheld Tax YoY signals the peak of total NFP")

    # Second subplot (data after 2010)
    ax3 = axes[1]
    data_filtered = data_combined[data_combined['date'] > datetime(2010, 1, 1)]
    ax3.plot(data_filtered['date'], data_filtered["NFP T Index"],
             color="r", label="Total NFP")
    ax3.legend(loc=3)

    ax4 = ax3.twinx()
    ax4.plot(data_filtered['date'], data_filtered["Tax YoY 6month EWM"],
             color="blue", label="Withheld Tax YoY 6month EWM")
    ax4.plot(data_filtered['date'], data_filtered["Real Tax YoY 6month EWM"], color="black",
             label="Real Withheld Tax YoY 6month EWM")
    ax4.hlines(0, xmin=data_filtered['date'].min(),
               xmax=data_filtered['date'].max(), color='gray')
    ax4.legend(loc=4)

    plt.title(f"Withheld Tax YoY and Total NFP")
    plt.savefig(os.path.join(G_CHART_DIR, "Withheld Tax vs Total NFP"))


def chart_usd_ad_line():
    """USD AD Line -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = END_DATE - timedelta(days=365 * 3)
    g10ccy_list = [
        f"USD{i} BGN Curncy" for i in 'EUR JPY CAD AUD NZD GBP CHF NOK SEK'.split(" ")]
    asiaccy_list = [
        f"USD{i} CMPT Curncy" for i in 'THB TWD KRW SGD PHP MYR IDR INR CNH'.split(" ")]
    latamccy_list = [
        f"USD{i} BGN Curncy" for i in 'PEN MXN COP CLP BRL'.split(" ")]
    emeaccy_list = [
        f"USD{i} CMPL Curncy" for i in 'ZAR AED ILS TRY PLN HUF CZK'.split(" ")]

    data = bbgui.bdh(['DXY Curncy', "ASIADOLR Index"], "PX_LAST",
                     START_DATE, END_DATE, interval='DAILY')
    for ccy_list, ccy_group in zip([g10ccy_list, asiaccy_list, latamccy_list, emeaccy_list],
                                   ["g10", "asia", 'latam', 'emea']):
        data_ccy = bbgui.bdh(ccy_list, "CHG_NET_1D",
                             START_DATE, END_DATE, interval='DAILY')
        data_sign = (data_ccy > 0)
        data_ccy["A/D Line"] = (2 * (data_sign.sum(axis=1)) -
                                data_ccy.shape[1]) / data_ccy.shape[1]
        data[ccy_group] = data_ccy["A/D Line"].ewm(span=22 * 6).mean()
    data = data.iloc[252:, :]

    fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data["g10"], label="g10")
    ax1.plot(data.index, data["asia"], label="asia")
    ax1.legend(loc=2)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["DXY Curncy"], color="black",
             label="DXY Index", linestyle="dashed")
    ax2.legend(loc=4)
    plt.title(f"Breadth of USD Strength/Weakness: Advance line - Decline line %")
    ax1.text(data.index[0], 0.1,
             "* 6month EWA  of (Advance line - Decline line)/ the number of ccy")
    ax1.text(data.index[0], 0.075,
             "* G10 ccy: EUR JPY CAD AUD NZD GBP CHF NOK SEK")
    ax1.text(data.index[0], 0.05,
             "* Asia ccy: THB TWD KRW SGD PHP MYR IDR INR CNH")

    ax3 = axes[1]
    ax3.plot(data.index, data["latam"], label="latam")
    ax3.plot(data.index, data["emea"], label="emea")
    ax3.legend(loc=2)
    ax4 = ax3.twinx()
    ax4.plot(data.index, data["DXY Curncy"], color="black",
             label="DXY Index", linestyle="dashed")
    ax4.legend(loc=4)
    ax3.text(data.index[0], -0.1, "* Latam ccy: PEN MXN COP CLP BRL")
    ax3.text(data.index[0], -0.2, "* Emea ccy: ZAR AED ILS TRY PLN HUF CZK")

    plt.tight_layout()
    plt.xticks(rotation=30)
    plt.savefig(os.path.join(G_CHART_DIR, "USD AD Line"))


def chart_emdm_pb_ratio():
    """EMDM PBratio -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = END_DATE - timedelta(days=365 * 3)
    data = bbgui.bdh(['MXEF Index', "MXWO Index"],
                     "BEST_PX_BPS_RATIO", START_DATE, END_DATE, interval='DAILY')
    data['EM/DM PB Ratio'] = data['MXEF Index'] / data['MXWO Index']
    data = data.dropna()

    fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data["EM/DM PB Ratio"], label="EM/DM PB Ratio")
    ax1.legend(loc=2)
    ax1.set_title("EM Equities Cheapness")

    ax2 = axes[1]
    ax2.plot(data.index, data['MXEF Index'], label="EM PB: MXEF")
    ax2.plot(data.index, data["MXWO Index"], label="DM PB: MXWO")
    ax2.legend(loc=2)
    ax2.set_title("EM and DB PB Ratio")

    plt.tight_layout()
    plt.savefig(os.path.join(G_CHART_DIR, "EMDM PBratio"))


def chart_em_msci_pe_ratio():
    """EM MSCI PEratio -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/06", "%d/%m/%y")
    data = bbgui.bdh(['MXEF Index'], "BEST_PE_RATIO",
                     START_DATE, END_DATE, interval='DAILY')
    data["mean"] = data['MXEF Index'].rolling(252).mean()
    data["std"] = data['MXEF Index'].rolling(252).std()
    data = data.dropna()

    fig, ax1 = plt.subplots(1, 1, figsize=(8, 6))
    ax1.plot(data.index, data["mean"], label="1y rolling mean")
    ax1.plot(data.index, data["mean"] + 1 * data['std'], linestyle="dashed")
    ax1.plot(data.index, data["mean"] - 1 * data['std'], linestyle="dashed")
    ax1.plot(data.index, data["MXEF Index"] - 0.5 *
             data['std'], label="MSCI EM P/E Ratio")
    ax1.legend(loc=2)
    ax1.set_title("EM Equity PE with 1std Band")
    plt.tight_layout()
    plt.savefig(os.path.join(G_CHART_DIR, "EM MSCI PEratio"))


def chart_cmbs_spread_over_hyg():
    """CMBS Spread over HYG -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("25/01/22", "%d/%m/%y")
    data1 = bbgui.bdh(["CMBX BBB- CDSI S16 PRC Corp",
                      "CMBX BBB- CDSI S15 PRC Corp"], "P1554", START_DATE, END_DATE)
    data2 = bbgui.bdh(["HYG US Equity"], "YAS_ISPREAD_TO_GOVT",
                      START_DATE, END_DATE)
    data3 = bbgui.bdh(["CMBS US Equity"], "PX_LAST", START_DATE, END_DATE)
    data_combine = pd.concat([data1, data2, data3], axis=1)
    data_combine = data_combine.fillna(method='ffill', limit=1)
    data_combine = data_combine.dropna(thresh=3)
    data_combine["CMBS BBB- 16 over HYG Spread"] = data_combine['CMBX BBB- CDSI S16 PRC Corp'] - data_combine[
        'HYG US Equity']
    data_combine["CMBS BBB- 15 over HYG Spread"] = data_combine['CMBX BBB- CDSI S15 PRC Corp'] - data_combine[
        'HYG US Equity']
    data_combine["CMBS BBB- over HYG Spread Synthetic"] = data_combine["CMBS BBB- 15 over HYG Spread"]

    data_combine.loc[data_combine["CMBS BBB- 16 over HYG Spread"].dropna().index, "CMBS BBB- over HYG Spread Synthetic"] = \
        data_combine["CMBS BBB- 16 over HYG Spread"].dropna().values
    data = data_combine

    fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data["CMBS BBB- over HYG Spread Synthetic"], label="CMBS BBB- over HYG Spread, Synthetic (L)",
             linewidth=3)
    ax1.legend(loc=2)
    ax1.set_title(
        f"the United States CMBS CDS over HYG G-Spread; updated at {data.index[-1].strftime('%d-%b-%Y')}")

    ax2 = axes[1]
    ax2.plot(data.index, data["HYG US Equity"],
             label="HYG US Equity", color="red")
    ax2.plot(data.index, data["CMBX BBB- CDSI S16 PRC Corp"],
             label="CMBX BBB- CDSI S16 PRC Corp", color="black")
    ax2.plot(data.index, data["CMBX BBB- CDSI S15 PRC Corp"],
             label="CMBX BBB- CDSI S15 PRC Corp", color="green")
    ax2.legend(loc=2)
    ax2.set_title(
        f"CMBS series, HYG and CMBS ETF; updated at {data.index[-1].strftime('%d-%b-%Y')}")
    ax3 = ax2.twinx()
    ax3.plot(data.index, data["CMBS US Equity"],
             label="CMBS US Equity (R)", color="blue")
    ax3.legend(loc=4)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "CMBS Spread over HYG"))


def chart_us_construction_sector():
    """United States Construction Sector -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/1970", "%d/%m/%Y")
    data = bbgui.bdh(["NHSPSTOT Index",  # permits to start
                      "HUUCTHUC Index",  # units under construction
                      "USECTOT Index",  # construction employment in nfp
                      ], startDate=START_DATE, endDate=END_DATE, interval="Monthly")

    data["NHSPSTOT Index 6MMA"] = data["NHSPSTOT Index"].rolling(6).mean()
    data["NHSPSTOT Index MoM 6MMA"] = data["NHSPSTOT Index"].pct_change(
        1).rolling(6).mean()

    data["HUUCTHUC Index 6MMA"] = data["HUUCTHUC Index"].rolling(6).mean()
    data["HUUCTHUC Index MoM 12MMA"] = data["HUUCTHUC Index"].diff(1).rolling(12).mean()
    data["USECTOT Index MoM 12MMA"] = data["USECTOT Index"].diff(1).rolling(12).mean()

    data["HUUCTHUC Index MoM 3MMA"] = data["HUUCTHUC Index"].diff(1).rolling(3).mean()
    data["USECTOT Index MoM 3MMA"] = data["USECTOT Index"].diff(1).rolling(3).mean()
    fig, axes = plt.subplots(2, 2, figsize=(4 * 4, 6 * 2))
    ax1 = axes[0, 0]
    ax1.plot(data.index, data["NHSPSTOT Index 6MMA"],
             label="Start&Permits (6MMA)", color="red")
    ax1.legend(loc=3)
    ax1.set_title(
        f"the Construction Cycle Lags; updated at {data.index[-1].strftime('%d-%b-%Y')}")
    ax11 = ax1.twinx()
    ax11.plot(data.index, data["HUUCTHUC Index 6MMA"],
              label="Units Under Construction(R): 6MMA", color="blue")
    ax11.legend(loc=4)

    ax2 = axes[0, 1]
    ax2.plot(data.index, data["HUUCTHUC Index MoM 12MMA"],
             label="Units Under Construction MoM: 12MMA", color="red")
    ax2.legend(loc=2)
    ax2.set_ylabel("thousand units")
    ax2.set_title(
        f"Units Under Construction VS. Construction Employees (M/M Change); updated at {data.index[-1].strftime('%d-%b-%Y')}")
    ax22 = ax2.twinx()
    ax22.set_ylabel("Employments in Construction (Thousands)")
    ax22.plot(data.index, data["USECTOT Index MoM 12MMA"],
              label="Construction employment, 12MMA (R)", color="blue")
    ax22.legend(loc=4)

    ax3 = axes[1, 0]
    data_cut = data.loc[data.index > '2000']
    ax3.axhline(y=0, color='grey', alpha=0.5)
    ax3.set_title(
        f"Units Under Construction VS. Construction Employees (M/M Change); updated at {data.index[-1].strftime('%d-%b-%Y')}")
    ax3.set_ylabel("Employments in Construction (Thousands)")
    ax3.plot(data_cut.index, data_cut["USECTOT Index MoM 3MMA"],
              label="Construction employment, 3MMA (R)", color="blue")
    ax3.legend(loc=4)

    ax4 = axes[1, 1]
    ax4.plot(data.loc["2022":, :].index, (data.loc["2022":, "NHSPSTOT Index"] - 1805) / 1805,
             label="Drawdown in Building Permits", color="red")
    ax4.set_title("Drawdown %in Building Permtis,  peak at 1805 in April 2022")
    ax4.legend(loc=3)

    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "United States Construction Sector"))


def chart_shanghai_future_vs_lme_copper_spread():
    """Shanghai Future vs LME Copper Spread -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2016", "%d/%m/%Y")
    data = bbgui.bdh(["LP1 Comdty",  # LME Copper
                      "CU1 Comdty",  # Shanghai Exchange Copper
                      "USDCNY Curncy"
                      ], flds="PX_LAST", startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data = clean_data(data)
    data.loc[:, "CU1 Comdty in USD"] = data.loc[:,
                                                "CU1 Comdty"] / data.loc[:, "USDCNY Curncy"]

    data["China Copper /  LME Copper, USD"] = data["CU1 Comdty in USD"] / \
        data["LP1 Comdty"]
    data["China Copper /  LME Copper, USD 20d MA"] = data["China Copper /  LME Copper, USD"].rolling(
        20).mean()

    fig, axes = plt.subplots(1, 1, figsize=(16, 6 * 1))
    ax1 = axes
    ax1.plot(data.index, data["China Copper /  LME Copper, USD 20d MA"],
             label="China Copper /  LME Copper, USD 20d MA", color="red")
    ax1.legend(loc=3)
    ax1.set_title(
        f"Copper 1st Futures Price in US: Shanghai vs London; updated at {data.index[-1].strftime('%d-%b-%Y')}")
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Shanghai Future vs LME Copper Spread"))


def chart_lme_metal_index_vs_crb_raw_material_index():
    """LME Metal Index vs CRB Raw Material Index -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2015", "%d/%m/%Y")
    data = bbgui.bdh(["LMEX Index",
                      "CRB RIND Index",
                      ], flds="PX_LAST", startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data = clean_data(data)

    data['LMEX Index N'] = data['LMEX Index'] / data['LMEX Index'][0] * 100
    data['CRB RIND Index N'] = data['CRB RIND Index'] / \
        data['CRB RIND Index'][0] * 100
    data['LME/CRB'] = data['LMEX Index N'] / data['CRB RIND Index N']

    fig, axes = plt.subplots(2, 1, figsize=(16, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data['LMEX Index N'],
             label="London Mental Exchange Mental Index in US$", color="red")
    ax1.plot(data.index, data['CRB RIND Index N'],
             label="Commodity Research Bureau Raw Industrial Material Index in US$", color="blue")
    ax1.legend(loc=3)
    ax1.set_title(
        f"LME Index vs CRB Material Index; updated at {data.index[-1].strftime('%d-%b-%Y')}")
    ax1.text(0, -0.1, "* Both series rebased to 100 at 1st Jan. 2015",
             transform=ax1.transAxes)
    ax1.text(0.02, 0.8,
             "A higher LME index may hint that there are more speculatively financial demands than actual industrial demands",
             transform=ax1.transAxes, fontsize=12)

    ax2 = axes[1]
    ax2.plot(data.index, data['LME/CRB'],
             label="LME/CRB ratio")
    ax2.legend(loc=3)
    ax2.set_title(f"LME Index / CRB Index ratio")

    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "LME Metal Index vs CRB Raw Material Index"))


def chart_us_treasury_curve_beta_to_2y():
    """US Treasury Curve Beta to 2y Treasury Yield -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/1970", "%d/%m/%Y")
    data = bbgui.bdh(["USGG30YR Index",
                      "USGG10YR Index",
                      "USGG2YR Index",
                      ], flds="Chg_Net_5D", startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data["USYC1030 Index"] = data["USGG30YR Index"] - data["USGG10YR Index"]
    data = clean_data(data)

    # Define the independent and dependent variables
    y = data['USYC1030 Index']
    X = data['USGG2YR Index']
    # Define the regression model
    window_size = 66
    model = RollingOLS(y, X, window=window_size)
    # Calculate the rolling regression coefficients
    rolling_coef = model.fit().params
    data["3month Beta"] = rolling_coef
    data["3month Beta 1m MA"] = data["3month Beta"].rolling(22).mean()

    fig, axes = plt.subplots(2, 1, figsize=(16, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data["3month Beta"],
             label="3m Beta", color="Blue")
    ax1.axhline(y=data["3month Beta"][-1], color='red', linestyle='--')

    ax1.legend(loc=3)
    ax1.text(0.02, 0.7, "A subdued curve sensitivity to changes in front-end yields is a hint of late cycle",
             fontsize=16, transform=ax1.transAxes)
    ax1.set_title(
        f"Rolling 3-month beta of weekly changes in 10s30s treasury curve to weekly changes in 2-year treasury yiled; updated at {data.index[-1].strftime('%d-%b-%Y')}")

    ax2 = axes[1]
    ax2.plot(data.index[-252 * 1:], data["3month Beta"][-252 * 1:],
             label="USYC1030", color="blue")
    ax2.axhline(y=data["3month Beta"][-1], color='red', linestyle='--')
    ax2.legend(loc=2)
    ax2.set_title("1 week changes")
    plt.tight_layout()

    plt.savefig(Path(G_CHART_DIR, "US Treasury Curve Beta to 2y Treasury Yield"))


def chart_us_household_durable_demand():
    """U M Sentiment vs Durable Goods (Current) -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/1985", "%d/%m/%Y")
    data = bbgui.bdh(["CONSDURF Index", "CONSDURU Index", "PCE DRBC Index"], flds="PX_LAST", startDate=START_DATE,
                     endDate=END_DATE, interval="Monthly")
    data = clean_data(data)
    data['Good-Bad Spread'] = data['CONSDURF Index'] - data['CONSDURU Index']
    data['Good-Bad Spread % 3M Avg'] = data['Good-Bad Spread'].rolling(3).mean()

    data["PCE DRBC Index YoY"] = data["PCE DRBC Index"].dropna(
    ).pct_change(periods=12) * 100
    data["PCE DRBC Index YoY % 3M Avg"] = data["PCE DRBC Index YoY"].rolling(
        3).mean()

    fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data['Good-Bad Spread % 3M Avg'],
             label="U. Mich Cons. Sent. Conditions for Buying Large HH Durables  Good-Bad Spread (%, 3M Avg,., LS)",
             color="Blue")
    ax1.set_ylim(-40, 100)
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["PCE DRBC Index YoY % 3M Avg"],
             label="Real Personal Consumption Expenditures Durable Goods (Y/Y, 3M Avg., RS)", color="red")
    ax2.set_ylim(-24, 20)
    ax1.legend(loc=3, fontsize=8)
    ax2.legend(loc=2, fontsize=8)
    ax1.set_title(
        f"Household Durable Demand and U. Mich Sentiment; updated at {data.index[-1].strftime('%d-%b-%Y')}")

    ax3 = axes[1]
    img = mpimg.imread(
        Path(G_CHART_DIR, "U M Sentiment vs Durable Goods (long history).png"))
    ax3.imshow(img)
    ax3.set_title(
        f"Full series dating back to 1985")
    plt.savefig(Path(G_CHART_DIR, "U M Sentiment vs Durable Goods (Current)"))


def chart_china_real_estate_investment():
    """China Real Estate Investment -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2006", "%d/%m/%Y")
    data = bbgui.bdh(["CHRXIRCY Index",
                      "CHFQM021 Index"], flds="PX_LAST", startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data = clean_data(data)

    data["CHRXIRCY Index 3mma"] = data["CHRXIRCY Index"].rolling(3).mean()
    data["CHFQM021 Index YoY 3mma"] = (
        data["CHFQM021 Index"] - 100).rolling(3).mean()

    fig, ax1 = plt.subplots(1, 1, figsize=(16, 6))

    ax1.plot(data.index, data["CHRXIRCY Index 3mma"],
             label=" China Property Investment in Real Estate Development-Construction YoY 3mma % (L)", color="Blue")
    ax1.legend(loc=3)
    ax1.set_title(
        f"China Real Estate Investment annual change 3m MA vs. Imports of primary good annual change; updated at {data.index[-1].strftime('%d-%b-%Y')}")

    ax2 = ax1.twinx()
    ax2.plot(data.index, data["CHFQM021 Index YoY 3mma"],
             label="China Import Volume Index Primay Industrial Supply YoY 3mma %(R)", color="red")
    ax2.legend(loc=4)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "China Real Estate Investment"))


def chart_china_cement_glass_auto_sales():
    """China Cement, Glass, Auto Sales YoY -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2006", "%d/%m/%Y")
    ticker_dict = {"CNVSTTL Index": "Auto Sales",
                   "CHMMCEMT Index": "Cement Production",
                   "CHMMROSL Index": "Plate Glass Production"}
    data = bbgui.bdh(list(ticker_dict.keys()), flds="PX_LAST",
                     startDate=START_DATE, endDate=END_DATE, interval="Monthly")

    data = data.apply(bfill_cny, axis=0).rename(ticker_dict, axis=1)
    data = clean_data(data)
    data_pct = round(data.pct_change(12) * 100, 2)

    fig, axes = plt.subplots(2, 1, figsize=(16, 6 * 2))
    data_pct[['Cement Production', 'Plate Glass Production']].plot(ax=axes[0])
    axes[0].set_title("Cement/Glass/Auto Sales YoY %, NSA")
    ax2 = axes[0].twinx()
    data_pct[['Auto Sales']].plot(ax=ax2, color='black')
    axes[0].legend(loc=3)
    ax2.legend(loc=4)
    axes[0].set_ylabel("Cement/Glass Production")
    ax2.set_ylabel("Auto Sales")
    ax2.axhline(y=0, color='red', linestyle='--')

    data_pct[['Cement Production', 'Plate Glass Production']
             ].iloc[-36:, :].plot(ax=axes[1])
    axes[1].set_title("Cement/Glass/Auto Sales YoY %, NSA, last 36months")
    ax4 = axes[1].twinx()
    data_pct.iloc[-36:, :][['Auto Sales']].plot(ax=ax4, color='black')
    axes[1].legend(loc=3)
    ax4.legend(loc=4)
    axes[1].set_ylabel("Cement/Glass Production")
    ax4.set_ylabel("Auto Sales")
    ax4.axhline(y=0, color='red', linestyle='--')
    plt.savefig(Path(G_CHART_DIR, "China Cement, Glass, Auto Sales YoY"))


def chart_loans_to_nonbank_fi():
    """Loans to Non-bank Financial Institutions -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2015", "%d/%m/%Y")
    data = bbgui.bdh(["CNLNNFIM Index"], flds="PX_LAST",
                     startDate=START_DATE, endDate=END_DATE, interval="Monthly")
    data = clean_data(data)
    data['cumsum'] = data["CNLNNFIM Index"].cumsum()
    fig, axes = plt.subplots(2, 1, figsize=(16, 6 * 2))
    ax1 = axes[0]
    ax1.plot(data.index, data["cumsum"],
             label="Cum Sum with 2015 Jan=0, unit: CNY Billion ", color="Blue")
    ax1.axhline(y=0, color='red', linestyle='--')
    ax1.legend(loc=3)
    ax1.set_title(
        f"China Domestic CNY Loan Newly Increased - Non-banking Financial Instituition; updated at {data.index[-1].strftime('%d-%b-%Y')}")

    ax2 = axes[1]
    ax2.plot(data.index, data["CNLNNFIM Index"],
             label="Monthly Change", color="red")
    ax2.axhline(y=0, color='black', linestyle='--')
    ax2.legend(loc=3)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Loans to Non-bank Financial Institutions"))


def chart_australia_monthly_cpi():
    """Australia Monthly CPI -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2015", "%d/%m/%Y")
    data = bbgui.bdh(["ACPMXVS Index", "ACPMISA Index", "ACPMXVLY Index", "ACPMELEC Index"], flds="PX_LAST",
                     startDate=START_DATE, endDate=END_DATE, interval="Monthly")

    data = clean_data(data)
    data['ex vol and travel, 3m Annualize %'] = round(
        ((data["ACPMXVS Index"] / data["ACPMXVS Index"].shift(3)) - 1) * 4 * 100, 2)
    data['ex vol and travel, 6m Annualize %'] = round(
        ((data["ACPMXVS Index"] / data["ACPMXVS Index"].shift(6)) - 1) * 2 * 100, 2)
    data['ex vol and travel, YoY'] = round(
        data["ACPMXVS Index"].pct_change(12) * 100, 2)

    data["All items SA YoY"] = round(data['ACPMISA Index'].pct_change(12) * 100, 2)

    data['ex vol, travel and elec Index'] = (
        data["ACPMXVS Index"] * (1 - 0.1174) - data["ACPMELEC Index"] * 0.0236) / (1 - 0.0236 - 0.1174)
    data['ex vol, travel and elec, 3m Annualize %'] = round(
        ((data["ex vol, travel and elec Index"] / data["ex vol, travel and elec Index"].shift(3)) - 1) * 4 * 100, 2)
    data['ex vol, travel and elec, 6m Annualize %'] = round(
        ((data["ex vol, travel and elec Index"] / data["ex vol, travel and elec Index"].shift(6)) - 1) * 2 * 100, 2)
    data['ex vol, travel and elec, YoY'] = round(
        data["ex vol, travel and elec Index"].pct_change(12) * 100, 2)

    data = data.rename({"ACPMXVLY Index": "ex vol, YoY"}, axis=1)

    fig, axes = plt.subplots(3, 1, figsize=(16, 6 * 3))
    ax1 = axes[0]
    ax1.plot(data.index, data['ex vol and travel, 3m Annualize %'],
             label="ex vol and travel, 3m Annualize %", color="Blue")
    ax1.plot(data.index, data['ex vol and travel, 6m Annualize %'],
             label="ex vol and travel, 6m Annualize %", color="Red")
    ax1.plot(data.index, data['ex vol and travel, YoY'],
             label='ex vol and travel, YoY', color="Black")

    ax1.axhline(y=0, color='red', linestyle='--')
    ax1.legend(loc=3)
    ax1.set_title(
        f"Australia monthly CPI excluding volatile items and holiday travel; Seasonally adjusted, updated at {data.index[-1].strftime('%d-%b-%Y')}")

    ax2 = axes[1]
    ax2.plot(data.index, data["All items SA YoY"],
             label="All items YoY", color="Blue")
    ax2.plot(data.index, data["ex vol, YoY"],
             label="All items excluding volatile items, YoY", color="Red")
    ax2.plot(data.index, data['ex vol and travel, YoY'],
             label="All items excluding volatile items excluding volatile items and travel, All items YoY", color="Black")

    ax2.legend(loc=3)

    ax3 = axes[2]
    ax3.plot(data.index, data['ex vol, travel and elec, 3m Annualize %'],
             label="ex vol, travel and elec, 3m Annualize %", color="Blue")
    ax3.plot(data.index, data['ex vol, travel and elec, 6m Annualize %'],
             label="ex vol, travel and elec, 6m Annualize %", color="Red")
    ax3.plot(data.index, data['ex vol, travel and elec, YoY'],
             label='ex vol, travel and elec, YoY', color="Black")

    ax3.axhline(y=0, color='red', linestyle='--')
    ax3.legend(loc=3)

    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Australia Monthly CPI"))


def chart_indonesia_wages_per_day():
    """Indonesia Wages Per day -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2009", "%d/%m/%Y")
    ticker_dict = {"IDWGFDN Index": "Agriculture",
                   "IDWGCDN Index": "Construction",
                   "IDWGWHN Index": "Hairdresser",
                   "IDWGSMN Index": "Servant"}

    data = bbgui.bdh(list(ticker_dict.keys()), flds="PX_LAST",
                     startDate=START_DATE, endDate=END_DATE, interval="Monthly")
    data = clean_data(data).rename(ticker_dict, axis=1)
    data_pct = round(data.pct_change(12) * 100, 2)

    fig, axes = plt.subplots(2, 1, figsize=(16, 6 * 2))
    data_pct.plot(ax=axes[0])
    axes[0].set_title("Indonesia Wage for Workers per Day Nominal, YoY")
    data_pct.iloc[-36:, :].plot(ax=axes[1])
    axes[1].set_title(
        "Indonesia Wage for Workers per Day Nominal, YoY, rebased froom Jan 2021")
    axes[1].set_ylabel("IDR")

    # Format y-axis ticks with thousand separators
    def format_thousand_separator(x, pos):
        return '{:,.0f}'.format(x)

    axes[1].yaxis.set_major_formatter(FuncFormatter(format_thousand_separator))
    axes[0].tick_params(axis='y', which='both', direction='in',
                        right=True, left=True, labelleft=True, labelright=True)
    axes[1].tick_params(axis='y', which='both', direction='in',
                        right=True, left=True, labelleft=True, labelright=True)
    plt.savefig(Path(G_CHART_DIR, "Indonesia Wages Per day"))


def chart_singapore_domestic_liquidity():
    """Singapore Domestic Liquidity -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2003", "%d/%m/%Y")
    ticker_dict = {"SIMSM1Y% Index": "Singapore M1 YoY",
                   "SIOFRUS Index": "Foreign Reserves in US$"}

    data = bbgui.bdh(list(ticker_dict.keys()), flds="PX_LAST",
                     startDate=START_DATE, endDate=END_DATE, interval="Monthly")
    data = clean_data(data).rename(ticker_dict, axis=1)

    data["Foreign Reserves in US$ YoY"] = round(
        data["Foreign Reserves in US$"].pct_change(12) * 100, 2)

    fig, ax = plt.subplots(1, 1, figsize=(16, 6 * 2))
    data.loc[:, ['Singapore M1 YoY', 'Foreign Reserves in US$ YoY']].plot(ax=ax)
    ax.set_title("Singapore Domestic Liquidity and MAS Selling Foreign Reserves")
    plt.savefig(Path(G_CHART_DIR, "Singapore Domestic Liquidity"))


def chart_breadth_of_usd_strength():
    """Breadth of USD Strength -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2007", "%d/%m/%Y")
    data = bbgui.bdh(['EUR F180 Curncy',
                      'JPY F180 Curncy',
                      'CAD F180 Curncy',
                      'AUD F180 Curncy',
                      'NZD F180 Curncy',
                      'GBP F180 Curncy',
                      'CHF F180 Curncy',
                      'NOK F180 Curncy',
                      'SEK F180 Curncy'], flds="PX_LAST", startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data = clean_data(data)
    for i in ['EUR F180 Curncy', 'AUD F180 Curncy', 'NZD F180 Curncy', 'GBP F180 Curncy']:
        data[i] = 1 / data[i]

    # Calculate the advance/decline line
    advances = (data.diff() > 0).sum(axis=1)
    declines = (data.diff() < 0).sum(axis=1)
    advance_decline_line = (advances - declines) / data.shape[1]
    # Add the advance/decline line as a new column in the DataFrame
    data['Advance/Decline Line'] = advance_decline_line.cumsum()
    data['Advance/Decline Line 60days MA'] = data['Advance/Decline Line'].rolling(
        60).mean()

    fig, axes = plt.subplots(2, 2, figsize=(16, 6 * 2))
    ax = axes[0, 0]
    data.loc[:, ['Advance/Decline Line',
                 'Advance/Decline Line 60days MA']].plot(ax=ax)
    ax.set_title("G10: 9 crosses with EUR JPY CAD AUD NZD GBP CHF NOK SEK")
    ax = axes[1, 0]
    data.loc["2018/01/01":, ['Advance/Decline Line',
                             'Advance/Decline Line 60days MA']].plot(ax=ax)
    ax.set_title("From 2018 onwards")

    data = bbgui.bdh(['CNH F180 Curncy',
                      'NTN+1M F180 Curncy',
                      'KWN+1M F180 Curncy',
                      'SGD F180 Curncy',
                      'THB F180 Curncy',
                      'IHN+1M F180 Curncy',
                      'IRN+1M F180 Curncy',
                      'MRN+1M F180 Curncy',
                      'PPN+1M F180 Curncy',
                      'ZAR F180 Curncy',
                      'TRY F180 Curncy',
                      'ILS F180 Curncy',
                      'CZK F180 Curncy',
                      'PLN F180 Curncy',
                      'HUF F180 Curncy',
                      'MXN F180 Curncy',
                      'BCN+1M F180 Curncy',
                      'CLP F180 Curncy',
                      "COP F180 Curncy"], flds="PX_LAST", startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data = clean_data(data)

    # Calculate the advance/decline line
    advances = (data.diff() > 0).sum(axis=1)
    declines = (data.diff() < 0).sum(axis=1)
    advance_decline_line = (advances - declines) / data.shape[1]
    # Add the advance/decline line as a new column in the DataFrame
    data['Advance/Decline Line'] = advance_decline_line.cumsum()
    data['Advance/Decline Line 60days MA'] = data['Advance/Decline Line'].rolling(
        60).mean()

    ax = axes[0, 1]
    data.loc[:, ['Advance/Decline Line',
                 'Advance/Decline Line 60days MA']].plot(ax=ax)
    ax.set_title("EM: 19 crosses with KRW CNH IDR INR MYR PHP SGD THB TWD MXN BRL CLP COP ZAR TRY ILS CZK PLN HUF",
                 wrap=True)
    ax = axes[1, 1]
    data.loc["2018/01/01":, ['Advance/Decline Line',
                             'Advance/Decline Line 60days MA']].plot(ax=ax)
    ax.set_title("From 2018 onwards")
    fig.suptitle(
        "USD advances minus declines lines and 60d moving average, higher means dollar stronger", y=1)
    fig.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Breadth of USD Strength"))


def chart_equity_bearbull_breadth():
    """Equity BearBull Breadth -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2021", "%d/%m/%Y")
    data = bbgui.bdh(['SPX Index'],
                     flds=["NUM_MEMB_PX_GT_50D_MOV_AVG",
                           "NUM_MEMB_WITH_14D_RSI_GT_70", "NUM_MEMB_WITH_14D_RSI_LT_30"],
                     startDate=START_DATE, endDate=END_DATE, interval="Daily")
    data = clean_data(data)

    data.loc[:, ('SPX Index', 'NUM_MEMB_PX_GT_50D_MOV_AVG 60days MA')] = data.loc[:, ('SPX Index',
                                                                                      'NUM_MEMB_PX_GT_50D_MOV_AVG')].rolling(
        66).mean()
    data.loc[:, ('SPX Index', 'NUM_MEMB_WITH_14D_RSI_GT_70 7days MA')] = data.loc[:, ('SPX Index',
                                                                                      'NUM_MEMB_WITH_14D_RSI_GT_70')].rolling(
        7).mean()
    data.loc[:, ('SPX Index', 'NUM_MEMB_WITH_14D_RSI_LT_30 7days MA')] = data.loc[:, ('SPX Index',
                                                                                      'NUM_MEMB_WITH_14D_RSI_LT_30')].rolling(
        7).mean()

    fig, axes = plt.subplots(3, 1, figsize=(16, 6 * 2))
    data.loc[:, ('SPX Index', 'NUM_MEMB_PX_GT_50D_MOV_AVG')].plot(ax=axes[0])
    data.loc[:, ('SPX Index', 'NUM_MEMB_PX_GT_50D_MOV_AVG 60days MA')
             ].plot(ax=axes[0])
    axes[0].legend(["Number of members > 50day MA", "60days MA of the series"])
    axes[0].set_title(
        "USD advances minus declines 60d moving average (9 cross with EUR JPY CAD AUD NZD GBP CHF NOK SEK)")

    data.loc[:, [('SPX Index', 'NUM_MEMB_WITH_14D_RSI_GT_70'),
                 ('SPX Index', 'NUM_MEMB_WITH_14D_RSI_GT_70 7days MA')]].plot(ax=axes[1])
    axes[1].legend(["Number of members with 14 Day RSI >70",
                   "7days MA of the series"])

    data.loc[:, [('SPX Index', 'NUM_MEMB_WITH_14D_RSI_LT_30'),
                 ('SPX Index', 'NUM_MEMB_WITH_14D_RSI_LT_30 7days MA')]].plot(ax=axes[2])
    axes[2].legend(["Number of members with 14 Day RSI > 30",
                   "7days MA of the series"])

    plt.savefig(Path(G_CHART_DIR, "Equity BearBull Breadth"))


def chart_commod_px_vs_credit_impulse():
    """Commod Px vs Commod Country Credit Impulse -- from charts_updater.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2008", "%d/%m/%Y")
    data = bbgui.bdh(["SPGSCI Index"], flds="PX_LAST",
                     startDate=START_DATE, endDate=END_DATE, interval="Monthly")
    data = data.append(pd.DataFrame(columns=data.columns,
                                    index=pd.date_range(start=data.index[-1] + pd.DateOffset(months=1), periods=6,
                                                        freq='M'))).sort_index()
    data["GS Commodity Index YoY"] = data.pct_change(12)
    data["GS Commodity Index YoY 6m advance"] = data["GS Commodity Index YoY"].shift(
        6)

    new_index = []
    for i, j in zip(data.index.year, data.index.month):
        new_index.append(datetime(i, j, 1))
    data.index = new_index

    data2 = exante_get_data(['RU.CREDIT.TOTAL.IMPL.12M.M',
                      'ZA.CREDIT.TOTAL.IMPL.12M.M',
                      'MX.CREDIT.TOTAL.IMPL.12M.M',
                      'BR.CREDIT.TOTAL.IMPL.12M.M',
                      'CL.CREDIT.TOTAL.IMPL.12M.M',
                      'CO.CREDIT.TOTAL.IMPL.12M.M',
                      ], startDate='2010-01-01', endDate=None).sort_index()

    data3 = exante_get_data(['RU.CREDIT.TOTAL.IMPL.6M.M',
                      'ZA.CREDIT.TOTAL.IMPL.6M.M',
                      'MX.CREDIT.TOTAL.IMPL.6M.M',
                      'BR.CREDIT.TOTAL.IMPL.6M.M',
                      'CL.CREDIT.TOTAL.IMPL.6M.M',
                      'CO.CREDIT.TOTAL.IMPL.6M.M'], startDate='2010-01-01', endDate=None).sort_index()

    data4 = pd.DataFrame(index=data2.index)
    data4["6m/6m credit impulse"] = data2.mean(axis=1, skipna=True)
    data4["YoY credit impulse"] = data3.mean(axis=1, skipna=True)

    id = pd.to_datetime(data4.index)
    new_index = []
    for i, j in zip(id.year, id.month):
        new_index.append(datetime(i, j, 1))
    data4.index = new_index
    data4 = clean_data(data4)

    data_merge = data.merge(data4, how='outer', left_index=True, right_index=True)

    fig, ax = plt.subplots(1, 1, figsize=(16, 6 * 2))
    ax.plot(data_merge.index[:-6], data["GS Commodity Index YoY 6m advance"][:-6] * 100,
            label="GS Commodity Index YoY 6months Advance% L", c='r')
    ax.plot(data_merge.index[-7:], data["GS Commodity Index YoY 6m advance"][-7:] * 100, label="6months in advance",
            linestyle="--", c='r')
    ax.legend()
    ax.set_ylabel('Commodity Price YoY')

    ax2 = ax.twinx()
    ax2.plot(data_merge.index, data_merge['YoY credit impulse']
             * 100, label='YoY% Credit Impulse R', c='lightblue')
    ax2.plot(data_merge.index, data_merge['6m/6m credit impulse']
             * 100, label='6m/6m% Credit Impulse R', c='darkblue')
    ax2.legend(loc=1)
    ax2.set_ylabel('Credit Impulse')
    plt.title(
        "Commodities prices advanced by 6m vs. Commodities country domestic credit impulse(equaly weighted average of brazil, chile, colombia, mexico, south africa and russia)")
    plt.savefig(Path(G_CHART_DIR, "Commod Px vs Commod Country Credit Impulse"))


def chart_domestic_vs_external_bond_yield():
    """Domestic vs External bond Yield -- from charts_updater.py"""
    END_DATE = datetime.today()
    data1 = blp.bdp(['GTUSDID10YR Corp',
                     'GTUSDPH10YR Corp',
                     'GTUSDBR10YR Corp',
                     'GTUSDMX10YR Corp',
                     'GTUSDCO10YR Corp',
                     'GTUSDCL10YR Corp',
                     'GTUSDPE10YR Corp',
                     'GTUSDTR10YR Corp'], flds=["YLD_YTM_MID"])

    rename_dict = {'GTUSDID10YR Corp': 'Indo',
                   'GTUSDPH10YR Corp': 'Philippines',
                   'GTUSDBR10YR Corp': 'Brazil',
                   'GTUSDMX10YR Corp': 'Mexico',
                   'GTUSDCO10YR Corp': 'Colombia ',
                   'GTUSDCL10YR Corp': 'Chile ',
                   'GTUSDPE10YR Corp': 'Peru',
                   'GTUSDTR10YR Corp': 'Turkey'}
    data1 = data1.rename(rename_dict).sort_index()

    data2 = blp.bdp(['GTPHP10YR Corp',
                     'GTBRL10YR Corp'], flds=["YLD_YTM_MID"])

    data3 = blp.bdp(['GIDN10YR Index',
                     'BV100476 BVLI Index',
                     'BV100477 BVLI Index',
                     'CLGB10Y Index',
                     'BV100995 BVLI Index',
                     'BV100965 BVLI Index'], flds=["PX_LAST"]).rename({"px_last": "yld_ytm_mid"}, axis=1)
    data2 = data2.append(data3)

    rename_dict = {'GIDN10YR Index': 'Indo',
                   'GTPHP10YR Corp': 'Philippines',
                   'GTBRL10YR Corp': 'Brazil',
                   'BV100476 BVLI Index': 'Mexico',
                   'BV100477 BVLI Index': 'Colombia ',
                   'CLGB10Y Index': 'Chile ',
                   'BV100995 BVLI Index': 'Peru',
                   'BV100965 BVLI Index': 'Turkey'}
    data2 = data2.rename(rename_dict).sort_index()

    fig, ax = plt.subplots(1, 1, figsize=(16, 6))
    ax.scatter(data1, data2, label=data1.index)
    plt.xlabel('External Bond Yield% (USD 10Y)')
    plt.ylabel('Domestic Bond Yield% (10Y)')
    plt.title('Domestic vs External bond Yield')
    # Set a threshold for label overlap
    overlap_threshold = 0.2
    labels = data1.index
    x, y = data1['yld_ytm_mid'].values, data2['yld_ytm_mid'].values
    # Add labels with arrows for points that are close
    for i, label in enumerate(labels):
        plt.annotate(label, (x[i], y[i]), textcoords="offset points", xytext=(
            0, 10), ha='center')
    plt.plot([0, 10], [0, 10], color='red', linestyle='--', label='45-degree Line')
    plt.savefig(Path(G_CHART_DIR, "Domestic vs External bond Yield"))


# UPDATER1_CHARTS = [
#     ("20d Change of GSUSFCI", chart_20d_change_of_gsusfci),
#     ("HSI vs Hibor3y", chart_hsi_vs_hibor3y),
#     ("EURUSD vs UST", chart_eurusd_vs_ust),
#     ("LMCI", chart_lmci),
#     ("PMI", chart_pmi),
#     ("LEI", chart_lei),
#     ("CapacityUti", chart_capacity_utilization),
#     ("USCPIvsWage", chart_us_cpi_vs_wage),
#     ("EZWage", chart_ez_wage),
#     ("Withheld Tax vs Total NFP", chart_withheld_tax_vs_total_nfp),
#     ("USD AD Line", chart_usd_ad_line),
#     ("EMDM PBratio", chart_emdm_pb_ratio),
#     ("EM MSCI PEratio", chart_em_msci_pe_ratio),
#     ("CMBS Spread over HYG", chart_cmbs_spread_over_hyg),
#     ("United States Construction Sector", chart_us_construction_sector),
#     ("Shanghai Future vs LME Copper Spread", chart_shanghai_future_vs_lme_copper_spread),
#     ("LME Metal Index vs CRB Raw Material Index", chart_lme_metal_index_vs_crb_raw_material_index),
#     ("US Treasury Curve Beta to 2y Treasury Yield", chart_us_treasury_curve_beta_to_2y),
#     ("U M Sentiment vs Durable Goods (Current)", chart_us_household_durable_demand),
#     ("China Real Estate Investment", chart_china_real_estate_investment),
#     ("China Cement, Glass, Auto Sales YoY", chart_china_cement_glass_auto_sales),
#     ("Loans to Non-bank Financial Institutions", chart_loans_to_nonbank_fi),
#     ("Australia Monthly CPI", chart_australia_monthly_cpi),
#     ("Indonesia Wages Per day", chart_indonesia_wages_per_day),
#     ("Singapore Domestic Liquidity", chart_singapore_domestic_liquidity),
#     ("Breadth of USD Strength", chart_breadth_of_usd_strength),
#     ("Equity BearBull Breadth", chart_equity_bearbull_breadth),
#     ("Commod Px vs Commod Country Credit Impulse", chart_commod_px_vs_credit_impulse),
#     ("Domestic vs External bond Yield", chart_domestic_vs_external_bond_yield),
# ]

# --- Charts from charts_updater2.py ---

def chart_manu_pmi_and_core_pce():
    """Mani PMI and Core PCE -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/1980", "%d/%m/%Y")
    data = bbgui.bdh(["NAPMPMI Index", "NAPMNMI Index", "PCE CYOY Index"], "PX_LAST", START_DATE, END_DATE)
    new_index = data.index + pd.DateOffset(months=18)
    data.index = new_index
    data = data.shift(-6)
    data['NAPMPMI Index F'] = data['NAPMPMI Index'].shift(18)
    data['NAPMNMI Index F'] = data['NAPMNMI Index'].shift(18)
    data["PCE CYOY Index D"] = data["PCE CYOY Index"].diff(12)

    fig, ax = plt.subplots(1, 1, figsize=(16, 6))
    ax.plot(data.index, data['NAPMPMI Index F'], label="Manu PMI", color="red")
    ax.plot(data.index, data['NAPMNMI Index F'], label="Service PMI", color="blue")
    ax2 = ax.twinx()
    ax2.plot(data.index, data['PCE CYOY Index D'], label="Core PCE YoY 12month Changes", color="black")
    ax.legend(loc=3)
    ax2.legend(loc=4)
    ax.set_ylabel("PMI")
    ax2.set_ylabel("PCE %")
    ax.set_title("Manu PMI 6months ahead vs Core PCE 12month changes")
    plt.savefig(Path(G_CHART_DIR, "Mani PMI and Core PCE"))


def chart_labor_quality_and_wages():
    """Labor Quality and Wages -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/1980", "%d/%m/%Y")
    data = bbgui.bdh(["SBOIQUAL Index", "ECI YOY Index"], "PX_LAST", START_DATE, END_DATE)
    data['ECI YOY Index'] = data['ECI YOY Index'].ffill(limit=2)

    fig, ax = plt.subplots(1, 1, figsize=(16, 6))
    ax.plot(data.index, data["SBOIQUAL Index"], label="NFIB Labor Quality", color="red")
    ax2 = ax.twinx()
    ax2.plot(data.index, data['ECI YOY Index'], label="Employment Cost Index", color="blue")
    ax.legend(loc=3)
    ax2.legend(loc=4)
    ax.set_ylabel("NFIB Labor Quality")
    ax2.set_ylabel("Employment Cost Index %")
    ax.set_title("Labor Quality is a leading indicator for Wages")
    plt.savefig(Path(G_CHART_DIR, "Labor Quality and Wages"))


def chart_australia_wage():
    """Australia Total Compensation, WPi and Hours Worked -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/1980", "%d/%m/%Y")
    ticker_list = ["AUUPAUE Index",  # underemployment rate
                   "AUNDUR Index",  # underutilization rate
                   "AUGDCPWS Index",  # Total Compensation of Employees, GDP basis
                   "AUWCBY Index",  # Wage Price Index including bonus
                   "AUHRAMTL Index",  # Aggregate Monthly Hours Worked SA
                   "AULF64LT Index",  # Australia 15-64 total labour force SA
                   ]
    df_list = []
    for i in ticker_list:
        _data = bbgui.bdh([i], "PX_LAST", START_DATE, END_DATE, interval='MONTHLY')
        _data.index = _data.index.to_period('M').start_time
        df_list.append(_data)

    data = pd.concat(df_list, axis=1)
    data.loc[:, ["AUGDCPWS Index", "AUWCBY Index"]] = data.loc[:, ["AUGDCPWS Index", "AUWCBY Index"]].ffill(
        limit=2)  # WPI and Total Compen is quarter
    data = data.rename({"AUWCBY Index": "WPI incl bonus, YoY"}, axis=1)
    data['Total Compensation YoY'] = data["AUGDCPWS Index"].pct_change(12) * 100
    data['Hours Work YoY'] = data["AUHRAMTL Index"].pct_change(12) * 100
    data['Hours Worked Per Capita'] = data["AUHRAMTL Index"] / data["AULF64LT Index"]
    data = data.rename({"AUWCBY Index": "WPI incl bonus, YoY"}, axis=1)

    fig, ax = plt.subplots(1, 1, figsize=(32, 12))
    bar_data = (data[['Hours Work YoY', "WPI incl bonus, YoY"]].T).dropna(axis=1)
    width = 16
    fontsize = 14
    ax.bar(bar_data.columns, bar_data.loc['Hours Work YoY', :], width=width, color="blue", label="Hours Work YoY (L)")
    ax.bar(bar_data.columns, bar_data.loc['WPI incl bonus, YoY', :], bottom=bar_data.loc['Hours Work YoY', :], width=width,
           color='grey', label="WPI incl bonus YoY (L)")
    ax.set_ylabel('YoY %', fontsize=fontsize)
    ax.set_title(f"Compensation of Employees vs wage rates + hours worked - updated at {data.index[-1].strftime('%Y-%b')}",
                 fontsize=fontsize)
    ax.legend(loc='upper left')

    ax2 = ax.twinx()
    ax2.plot(data.loc[bar_data.columns, 'Total Compensation YoY'], color='red', marker="o",
             label="Total Compensation YoY (R)")
    ax2.set_ylabel('YoY %', fontsize=fontsize)
    ax2.legend(loc='upper right')
    ax2.set_ylim(ax.get_ylim())

    plt.savefig(Path(G_CHART_DIR, "Australia Total Compensation, WPi and Hours Worked"))

    data["Underutilisation rate YoY"] = data["AUNDUR Index"].diff(12)
    data['WPI: change in YoY growth'] = data["WPI incl bonus, YoY"].diff(12)

    fig, ax = plt.subplots(1, 1, figsize=(32, 12))

    data2 = data.loc['2000':, :]
    ax.plot(data2.index, data2['WPI: change in YoY growth'], color='blue', label='WPI: change in YoY growth (L)')
    ax.hlines(y=0, xmin=data2.index[0], xmax=data2.index[-1], color='black', linestyle='--')
    ax.legend(loc='upper left')

    ax2 = ax.twinx()
    ax2.plot(pd.date_range(start=data2.index[6], periods=len(data2), freq='M'), -data2["Underutilisation rate YoY"],
             color='red', label='Underutilisation rate YoY: advanced 6months (inverse, R)')
    ax2.legend(loc='upper right')
    ax.set_title(f"Underutilisation rate leads growth in wage rates - updated at {data2.index[-1].strftime('%Y-%b')}",
                 fontsize=fontsize)

    plt.savefig(Path(G_CHART_DIR, "Underutilization Rate Leads Australian Wate Rates"))


def chart_australia_unemployment_expectations():
    """Australia Unemployment Expectations Leads Unemployment Rate -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2000", "%d/%m/%Y")
    data = bbgui.bdh(["AUUNI Index", "AULFUNEM Index"], "PX_LAST", START_DATE, END_DATE, interval='MONTHLY')

    fig, ax = plt.subplots(1, 1, figsize=(16, 6))

    ax.plot(data.index, data["AULFUNEM Index"], color='blue', label='Unemployment rate (L)')
    ax.legend(loc='upper left')

    ax2 = ax.twinx()
    ax2.plot(pd.date_range(start=data.index[8], periods=len(data), freq='M'), data["AUUNI Index"], color='red',
             label='Unemployment expectations (Westpac/Melbourne Survey): advanced 8months (R)')
    ax2.legend(loc='upper right')

    ax.set_title(
        f"Unemployment expectations historically lead the actual unemployment rate - updated at {data.index[-1].strftime('%Y-%b')}",
        fontsize=FONTSIZE)
    plt.savefig(Path(G_CHART_DIR, "Australia Unemployment Expectations Leads Unemployment Rate"))


def chart_us_ism_pmi():
    """US ISM PMI and Share of Companies Reporting Decreases -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime.strptime("01/01/2017", "%d/%m/%Y")

    manu_list = ["NAPMMNOW Index", "NAPMMDDA Index", "NAPMMEML Index"]  # supplier delivery: faster
    service_list = ["NAPMNNOL Index", "NAPMNMBL Index", "NAPMNMEL Index"]

    data = bbgui.bdh(["NAPMPMI Index"] + manu_list + ["NAPMNMI Index"] + service_list, "PX_LAST", START_DATE, END_DATE,
                     interval='MONTHLY')

    data.loc[:, "Manu Share"] = data[manu_list].mean(axis=1)
    data.loc[:, "Service Share"] = data[service_list].mean(axis=1)

    fig, axes = plt.subplots(1, 2, figsize=(2 * 12, 9))
    ax1 = axes[0]
    ax1.plot(data.index, data["NAPMPMI Index"], color='blue', label='ISM Manufacturing PMI (L)')
    ax1.axhline(y=50, color='gray', linestyle='--', linewidth=1)
    ax1.set_ylim(30, 75)
    ax1.legend(loc='upper left')
    ax2 = ax1.twinx()
    ax2.plot(data.index, data["Manu Share"], color='red',
             label='ISM Manufacturing, Share of Companies Reporting Decreases (R, inverted)')
    ax2.set_ylim(0, 35)
    ax2.invert_yaxis()
    ax2.legend(loc='lower right')

    ax3 = axes[1]
    ax3.plot(data.index, data["NAPMNMI Index"], color='blue', label='ISM Services PMI (L)')
    ax3.axhline(y=50, color='gray', linestyle='--', linewidth=1)
    ax3.legend(loc='upper left')
    ax4 = ax3.twinx()
    ax4.plot(data.index, data["Service Share"], color='red',
             label='ISM Services, Share of Companies Reporting Decreases (R, inverted)')
    ax4.legend(loc='lower right')
    ax4.invert_yaxis()

    fig.suptitle(
        f"ISM PMI vs avg shares of companies reporting decreases (avg across new orders, biz activity or shipments, and employment, SA) - updated at {data.index[-1].strftime('%Y-%b')}",
        fontsize=FONTSIZE)
    plt.savefig(Path(G_CHART_DIR, "US ISM PMI and Share of Companies Reporting Decreases"))


def chart_india_exp_yoy_excl_interest():
    """India Exp YOY excl Interest -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = END_DATE.replace(year=END_DATE.year - 4)
    data = blp.bdh("INFFTOEX Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    data.index = pd.to_datetime(data.index)
    int_data = blp.bdh("INFFNPRI Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    int_data.index = pd.to_datetime(int_data.index)
    data["Govt Expenses excl Interest"] = data['INFFTOEX Index'] - int_data['INFFNPRI Index']
    yoy_growth_excl_interest = data["Govt Expenses excl Interest"].pct_change(12) * 100
    latest = yoy_growth_excl_interest.last_valid_index().strftime("%b %y")
    gst = blp.bdh("GSTXTXCO Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    gst.index = pd.to_datetime(gst.index)
    gst['YOY Change'] = gst["GSTXTXCO Index"].pct_change(12) * 100
    gst_latest = gst.last_valid_index().strftime("%b %y")

    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(gst.index, gst['YOY Change'], label='YoY GST Collection Growth', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("YoY Growth (%) for GST Collection")
    ax1.tick_params(axis='y')
    ax1.set_ylim(auto=True)
    ax1.locator_params(axis='y', nbins=10)
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(
        f"Year-over-Year (YoY) Growth of India's Government Expenditure Excluding Interest as of {latest} and GST Collection as of {gst_latest}")
    ax1.axhline(0, color='red', lw=1)
    ax2 = ax1.twinx()
    line2 = ax2.plot(yoy_growth_excl_interest.index, yoy_growth_excl_interest,
                     label='YoY Expenditure Growth Excluding Interest', color='b')
    ax2.set_ylabel("YoY Growth (%) for Government Expenditure Excl. Interest")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True)
    plt.savefig(Path(G_CHART_DIR, "India Exp YOY excl Interest"))


def chart_yield_and_ccy_betas():
    """5Y Implied By Beta.png -- from charts_updater2.py"""
    END_DATE = datetime.today()

    five_year_dict = {
        "USD": "USGG5YR Index", "CNY": "GCNY5YR Index", "JPY": "GJGB5 Index", "BRL": "BCSFUPDV Curncy",
        "TWD": "TDSWNI5 Curncy", "IDR": "GTIDR5YR Corp", "THB": "GVTL5YR Index", "MYR": "MRSWNI5 Curncy",
        "PHP": "GTPHP5YR Corp", "INR": "IRSWNI5 Index", "CLP": "CHSWP5 Curncy", "ILS": "GISR5YR Index",
        "NOK": "NKS5Y Curncy", "ZAR": "SASW5 Curncy", "NZD": "GNZGB5 Index", "SEK": "SKSW5 Curncy",
        "CZK": "CZGB5YR Index", "KRW": "GVSK5YR Index", "CHF": "GSWISS05 Index", "AUD": "ADSW5 Curncy",
        "MXN": "MPSWF5E Curncy", "COP": "CLSWIB5 Curncy", "GBP": "GUKG5 Index", "CAD": "GCAN5YR Index",
        "EUR": "GECU5YR Index", "PLN": "POGB5YR Index", "HUF": "GHGB5YR Index", "SGD": "SDSOA5 Curncy"
    }

    two_year_dict = {
        "USD": "USGG2YR Index", "CNY": "GCNY2YR Index", "JPY": "GJGB2 Index", "BRL": "BCSFPPDV BLP Curncy",
        "TWD": "TDSWNI2 Curncy", "IDR": "GTIDR2YR Corp", "THB": "GVTL2YR Index", "MYR": "MRSWNI2 Curncy",
        "PHP": "GTPHP2YR Corp", "INR": "IRSWNI2 Index", "CLP": "CHSWP2 Curncy", "ILS": "GISR2YR Index",
        "NOK": "NKS2Y Curncy", "ZAR": "SASW2 Curncy", "NZD": "GNZGB2 Index", "SEK": "SKSW2 Curncy",
        "CZK": "CZGB2YR Index", "KRW": "GVSK2YR Index", "CHF": "GSWISS02 Index", "AUD": "ADSWAP2 Curncy",
        "MXN": "MPSWF2B Curncy", "COP": "CLSWIB2 Curncy", "GBP": "GUKG2 Index", "CAD": "GCAN2YR Index",
        "EUR": "GECU2YR Index", "PLN": "POGB2YR Index", "HUF": "GHGB2YR Index", "SGD": "SDSOA2 Curncy"
    }
    fx_dict = {
        "USD": "DXY Curncy", "CNY": "USDCNY Curncy", "JPY": "USDJPY Curncy", "BRL": "USDBRL Curncy", "TWD": "USDTWD Curncy", "IDR": "USDIDR Curncy", "THB": "USDTHB Curncy", "MYR": "USDMYR Curncy",
        "PHP": "USDPHP Curncy", "INR": "USDINR Curncy", "CLP": "USDCLP Curncy", "ILS": "USDILS Curncy", "NOK": "USDNOK Curncy", "ZAR": "USDZAR Curncy", "NZD": "USDNZD Curncy", "SEK": "USDSEK Curncy",
        "CZK": "USDCZK Curncy", "KRW": "USDKRW Curncy", "CHF": "USDCHF Curncy", "AUD": "USDAUD Curncy", "MXN": "USDMXN Curncy", "COP": "USDCOP Curncy", "GBP": "USDGBP Curncy", "CAD": "USDCAD Curncy",
        "EUR": "USDEUR Curncy", "PLN": "USDPLN Curncy", "HUF": "USDHUF Curncy"
    }
    START_DATE = END_DATE - timedelta(days=2 * 365)

    # 5y yields beta
    five_year_tickers = list(five_year_dict.values())
    data_5y = blp.bdh(five_year_tickers, "LAST PRICE", START_DATE, END_DATE, "QtTyp = Y").droplevel(1, axis=1)
    inverse_five_year_dict = {v: k for k, v in five_year_dict.items()}
    data_5y.rename(columns=inverse_five_year_dict, inplace=True)
    data_5y.index = pd.to_datetime(data_5y.index)
    data_5y = data_5y.iloc[:-1].ffill(limit=10)
    latest = data_5y.index[-1].strftime("%d-%b-%Y")
    calculate_and_plot(data_5y, f"Actual vs Expected Move in 5-Year Yields (Last 20 Days) as of {latest}", "5Y Implied By Beta.png", method='yield')

    # 2x5 yields beta
    two_year_tickers = list(two_year_dict.values())
    data_2y = blp.bdh(two_year_tickers, "LAST PRICE", START_DATE, END_DATE, "QtTyp = Y").droplevel(1, axis=1)
    inverse_two_year_dict = {v: k for k, v in two_year_dict.items()}
    data_2y.rename(columns=inverse_two_year_dict, inplace=True)
    data_2y.index = pd.to_datetime(data_2y.index)
    data_2y = data_2y.iloc[:-1].ffill(limit=10)
    common_dates = data_5y.index.intersection(data_2y.index)
    data_5y = data_5y.loc[common_dates]
    data_2y = data_2y.loc[common_dates]
    common_currencies = data_5y.columns.intersection(data_2y.columns)
    data_5y = data_5y[common_currencies]
    data_2y = data_2y[common_currencies]
    spread_data = data_5y - data_2y
    calculate_and_plot(spread_data, f"Actual vs Expected Move in 2x5 Spreads (Last 20 Days) as of {latest}", "2x5 Implied By Beta.png", method='yield', multiply_by_100=True)

    # CCY beta
    five_year_tickers_fx = list(fx_dict.values())
    data_fx = blp.bdh(five_year_tickers_fx, "LAST PRICE", START_DATE, END_DATE).droplevel(1, axis=1)
    inverse_fx_dict = {v: k for k, v in fx_dict.items()}
    data_fx.rename(columns=inverse_fx_dict, inplace=True)
    data_fx.index = pd.to_datetime(data_fx.index)
    data_fx = data_fx.iloc[:-1].ffill(limit=5)
    calculate_and_plot(data_fx, "Actual vs Expected Move in USDCCY (Last 20 Days)", "USDCCY Implied By Beta.png", method='fx')


def chart_euro_area_pmi_vs_sentix():
    """Euro Area PMI vs Sentix -- from charts_updater2.py"""
    END_DATE = datetime.today()
    end_date = (END_DATE.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    START_DATE = end_date - timedelta(days=3 * 365)
    pmi = blp.bdh("MPMIEZMA Index", "PX_LAST", START_DATE, end_date).droplevel(1, axis=1)
    pmi.index = pd.to_datetime(pmi.index)
    pmi['12m Change'] = pmi["MPMIEZMA Index"].diff(12)
    latest = pmi.last_valid_index().strftime("%b %y")
    sentix = blp.bdh(["SNTEEUH6 Index", "SNTEEUH0 Index"], "PX_LAST", START_DATE, end_date).droplevel(1, axis=1)
    sentix.index = pd.to_datetime(sentix.index)
    sentix['Change'] = sentix["SNTEEUH6 Index"] - sentix["SNTEEUH0 Index"]
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(pmi.index, pmi['12m Change'], label='Manufacturing PMI (12-Month Change)', color='black')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Manufacturing PMI (12-Month Change)", color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.axhline(0, color='red', lw=1)
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.grid(True)
    ax2 = ax1.twinx()
    line2 = ax2.plot(sentix.index, sentix['Change'], label='Sentix Expectations (6-Month Ahead)', color='g')
    ax2.set_ylabel("Sentix Expectations (6-Month Ahead)", color='black')
    ax2.tick_params(axis='y', labelcolor='g')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=45)
    ax1.set_title(
        f"Eurozone Manufacturing PMI (12-Month Change) and Sentix Expectations (6-Month Ahead) - updated as of {latest}")
    plt.savefig(Path(G_CHART_DIR, "Euro Area PMI vs Sentix"))


def chart_australia_westpac_leading_index():
    """Australia Westpac Leading Index vs GDP -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2010, 1, 1)
    gdp = blp.bdh("AUNAGDPY Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    gdp.index = pd.to_datetime(gdp.index)
    westpac = blp.bdh("AULILEAD Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    westpac.index = pd.to_datetime(westpac.index)
    latest = westpac.last_valid_index().strftime("%b %y")
    westpac.index = westpac.index + pd.DateOffset(months=6)
    fig, ax1 = plt.subplots(figsize=(20, 10))
    line1 = ax1.plot(westpac.index, westpac["AULILEAD Index"], label='Australia Westpac Leading Index', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Australia Westpac Leading Index")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"Australia Westpac Leading Index (6month advance) vs GDP YoY as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(gdp.index, gdp['AUNAGDPY Index'], label='YoY GDP Growth', color='b')
    ax2.set_ylabel("YoY GDP Growth")
    ax2.tick_params(axis='y')
    ax2.axhline(0, color='red', lw=1, linestyle='--')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.axvline(pd.to_datetime((westpac.index[-1] - pd.DateOffset(months=6)).strftime("%Y-%m")), color='gray',
                linestyle='--', label="Latest Westpac Date")
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    ax1.set_ylim(92, 102)
    ax2.set_ylim(-8, 11)
    plt.savefig(Path(G_CHART_DIR, r"Australia Westpac Leading Index vs GDP", bbox_inches='tight'))


def chart_acm_term_premium_vs_ism():
    """10y ACM vs 5s10s30s UST Correlation vs ISM Manufacturing -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2010, 1, 1)
    ism = blp.bdh("NAPMPMI Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    ism.index = pd.to_datetime(ism.index)
    usts = blp.bdh(["USGG5YR Index", "USGG10YR Index", "USGG30YR Index", "ACMTP10  Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    usts.index = pd.to_datetime(usts.index)
    usts['5s10s30s'] = 2 * usts['USGG10YR Index'] - usts['USGG5YR Index'] - usts['USGG30YR Index']
    acm = blp.bdh("ACMTP10  Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    acm.index = pd.to_datetime(acm.index)
    usts['ust_5d_chg'] = usts['5s10s30s'] - usts['5s10s30s'].shift(5, freq='B')
    usts['acm_5d_chg'] = usts["ACMTP10  Index"] - usts["ACMTP10  Index"].shift(5, freq='B')
    usts.dropna(inplace=True)
    usts['correlation'] = usts['acm_5d_chg'].rolling(window=200).corr(usts['ust_5d_chg'])
    latest = ism.last_valid_index().strftime("%b %y")
    common_start_date = min(usts.index.min(), ism.index.min())
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 15), sharex=False, gridspec_kw={'height_ratios': [1, 1]})
    line1 = ax1.plot(usts.index, usts["correlation"], label='10y ACM TP vs 5s10s30s correlation', color='grey')
    ax1.set_ylabel("10y ACM TP vs 5s10s30s correlation")
    ax1.set_xlim([common_start_date, usts.index.max()])
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax1.set_title(f"10y ACM Term Premium vs 5s10s30s UST Correlation and ISM Manufacturing as of {latest}")
    ax1.grid(True)
    ax1_twin = ax1.twinx()
    line2 = ax1_twin.plot(ism.index, ism["NAPMPMI Index"], label='ISM Manufacturing PMI SA', color='r')
    ax1_twin.set_ylabel("ISM Manufacturing PMI SA")
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    line3 = ax2.plot(usts.index, usts["ACMTP10  Index"], label='10y ACM TP', color='grey')
    ax2.set_ylabel("10y ACM TP")
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax2.set_title(f"10y ACM Term Premium vs ISM Manufacturing as of {latest}")
    ax2.grid(True)
    ax2_twin = ax2.twinx()
    line4 = ax2_twin.plot(ism.index, ism["NAPMPMI Index"], label='ISM Manufacturing PMI SA', color='r')
    ax2_twin.set_ylabel("ISM Manufacturing PMI SA")
    ax2.set_xlim([common_start_date, usts.index.max()])
    lines = line3 + line4
    labels = [l.get_label() for l in lines]
    ax2.legend(lines, labels, loc="upper right")
    ax2.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, r"10y ACM vs 5s10s30s UST Correlation vs ISM Manufacturing", bbox_inches='tight'))


def chart_sg_tradable_core_inflation():
    """Singapore Tradable Core Inflation vs Trading Partners -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2012, 12, 31)
    DIVISOR = 0.6582
    sg_core = blp.bdh("SMASCORE Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    sg_core.index = pd.to_datetime(sg_core.index)
    latest = sg_core.last_valid_index().strftime("%b %y")
    other_cpi = ["CPI YOY Index", "CNCPIYOY Index", "MACPIYOY Index", "ECCPEMUY Index", "TWCPIYOY Index",
                 "JNCPIYOY Index", "KOCPIYOY Index", "HKCPIY Index", "IDCPIY Index", "THCPIYOY Index"]
    trading_partners = blp.bdh(other_cpi, "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    trading_partners.index = pd.to_datetime(trading_partners.index)
    trading_partners['Average_CPI'] = trading_partners.mean(axis=1)
    trading_partners = trading_partners.shift(periods=3, freq='M')
    non_tradables_list = ["SICPHSES Index", "SICWHSES Index", "SICPEDUC Index", "SICWEDUC Index", "SICPMEDT Index",
                          "SICWMEDT Index", "SICPFUEL Index", "SICWFUEL Index",
                          "SICPCOMM Index", "SICWCOMM Index", "SICPPUB Index", "SICWPUB Index"]
    non_tradebles_df = blp.bdh(non_tradables_list, "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    non_tradebles_df.index = pd.to_datetime(non_tradebles_df.index)
    non_tradebles_df['SICWMEDT Index'].fillna(method='ffill', inplace=True)
    non_tradebles_df['Household Services Inflation'] = (
            non_tradebles_df["SICPHSES Index"].pct_change(12) * (non_tradebles_df["SICWHSES Index"] / 100) / DIVISOR)
    non_tradebles_df['Education Services Inflation'] = (
            non_tradebles_df["SICPEDUC Index"].pct_change(12) * (non_tradebles_df["SICWEDUC Index"] / 100) / DIVISOR)
    non_tradebles_df['Medical Services Inflation'] = (
            non_tradebles_df["SICPMEDT Index"].pct_change(12) * (non_tradebles_df["SICWMEDT Index"] / 100) / DIVISOR)
    non_tradebles_df['Utilities Inflation'] = (
            non_tradebles_df["SICPFUEL Index"].pct_change(12) * (non_tradebles_df["SICWFUEL Index"] / 100) / DIVISOR)
    non_tradebles_df['Communication Inflation'] = (
            non_tradebles_df["SICPCOMM Index"].pct_change(12) * (non_tradebles_df["SICWCOMM Index"] / 100) / DIVISOR)
    non_tradebles_df['Public Transport Inflation'] = (
            non_tradebles_df["SICPPUB Index"].pct_change(12) * (non_tradebles_df["SICWPUB Index"] / 100) / DIVISOR)
    non_tradable_inflation = (
            non_tradebles_df['Household Services Inflation'] + non_tradebles_df['Education Services Inflation'] +
            non_tradebles_df['Medical Services Inflation'] +
            non_tradebles_df['Utilities Inflation'] + non_tradebles_df['Communication Inflation'] + non_tradebles_df[
                'Public Transport Inflation'])
    non_tradable_weight = (non_tradebles_df['SICWHSES Index'] + non_tradebles_df['SICWEDUC Index'] + non_tradebles_df[
        'SICWMEDT Index'] + non_tradebles_df['SICWFUEL Index'] +
                           non_tradebles_df['SICWCOMM Index'] + non_tradebles_df['SICWPUB Index']) / 100
    weight2 = DIVISOR / (DIVISOR - non_tradable_weight)
    sg_core['tradable inflation'] = (sg_core['SMASCORE Index'] - non_tradable_inflation * 100)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(sg_core.index, sg_core["SMASCORE Index"], label="Singapore's Core Inflation (65% of total basket)",
                     color='blue')
    line2 = ax1.plot(trading_partners.index, trading_partners["Average_CPI"],
                     label="Avg Inflation (Top 10 Trading Partners, 3M lead)", color='black')
    line3 = ax1.plot(sg_core.index, sg_core["tradable inflation"] * weight2,
                     label="Singapore's Core Inflation : Tradable (40%)",
                     color='red')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("YoY Inflation (%)")
    ax1.set_title(
        f"Singapore's Tradable Core Inflation vs Average Inflation of Top 10 Trading Partners (3M lead) as of {latest}")
    ax1.axhline(0, color='black', lw=1, linestyle='--')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax1.legend(loc="best")
    ax1.axvline(sg_core.index[-1], color='gray', linestyle='--', label="Latest Core Inflation Date")
    ax1.tick_params(axis='x', rotation=45)
    plt.savefig(Path(G_CHART_DIR, r"Singapore Tradable Core Inflation vs Trading Partners", bbox_inches='tight'))


def chart_us_share_of_total_unemployment():
    """US Share of Total Unemployment -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    unemployment = blp.bdh(["USJLOSER Index", "USJLJOBL Index", "USJLREEN Index", "USJLNENT Index", "USUETOT Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    unemployment.index = pd.to_datetime(unemployment.index)
    latest = unemployment.last_valid_index().strftime("%b %y")
    unemployment["Demand"] = (unemployment["USJLOSER Index"] / unemployment["USUETOT Index"]) * 100
    unemployment["Supply"] = ((unemployment["USJLJOBL Index"] + unemployment["USJLREEN Index"] + unemployment["USJLNENT Index"]) / unemployment["USUETOT Index"]) * 100
    end_date = unemployment.index.max()
    start_date = end_date.replace(year=end_date.year - 10)
    latest_unemployment = unemployment[(unemployment.index >= start_date) & (unemployment.index <= end_date)]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 18), sharex=False, gridspec_kw={'height_ratios': [1, 1]})
    line1 = ax1.plot(unemployment.index, unemployment["Demand"], label='Demand-Driven', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Demand-Driven (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=36))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"US Share Of Total Unemployment as of {latest}")
    ax1_twin = ax1.twinx()
    line2 = ax1_twin.plot(unemployment.index, unemployment['Supply'], label='Supply-Driven', color='black')
    ax1_twin.set_ylabel("Supply-Driven (%)")
    ax1_twin.tick_params(axis='y')
    ax1_twin.axhline(50, color='black', lw=1, linestyle='--')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    ax1.yaxis.set_major_locator(MultipleLocator(10))
    ax1_twin.yaxis.set_major_locator(MultipleLocator(10))
    ax1.set_ylim(0, 100)
    ax1_twin.set_ylim(0, 100)
    line3 = ax2.plot(latest_unemployment.index, latest_unemployment["Demand"], label='Demand-Driven', color='g')
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Demand-Driven (%)")
    ax2.tick_params(axis='y')
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=36))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax2.set_title(f"Past 10 Years US Share Of Total Unemployment as of {latest}")
    ax2_twin = ax2.twinx()
    line4 = ax2_twin.plot(latest_unemployment.index, latest_unemployment['Supply'], label='Supply-Driven', color='black')
    ax2_twin.set_ylabel("Supply-Driven (%)")
    ax2_twin.tick_params(axis='y')
    ax2_twin.axhline(50, color='black', lw=1, linestyle='--')
    lines = line3 + line4
    labels = [l.get_label() for l in lines]
    ax2.legend(lines, labels, loc="upper right")
    ax2.tick_params(axis='x', rotation=60)
    ax2.yaxis.set_major_locator(MultipleLocator(10))
    ax2_twin.yaxis.set_major_locator(MultipleLocator(10))
    ax2.set_ylim(0, 100)
    ax2_twin.set_ylim(0, 100)
    plt.savefig(Path(G_CHART_DIR, r"US Share of Total Unemployment", bbox_inches='tight'))


def chart_india_budget_balance_vs_credit():
    """India Budget Balance vs Credit -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2012, 1, 1)
    budget = blp.bdh("EHBBIN Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    budget.index = pd.to_datetime(budget.index)
    credit = blp.bdh("IBCDINDT Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    credit.index = pd.to_datetime(credit.index)
    latest = budget.last_valid_index().strftime("%b %y")
    budget['12m MA'] = budget["EHBBIN Index"].rolling(4).mean()
    credit['annual change'] = credit['IBCDINDT Index'].pct_change(12) * 100
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(budget.index, budget["EHBBIN Index"], label='India Budget Balance %GDP (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("India Budget Balance %GDP")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"India Budget Balance vs Total Credit Annual Change as of {latest}")
    line2 = ax1.plot(budget.index, budget["12m MA"], label='India Budget Balance %GDP (12m MA) (Left Axis)', color='purple')
    ax2 = ax1.twinx()
    line3 = ax2.plot(credit.index, credit['annual change'], label='YoY Credit Growth (Right Axis)', color='b')
    ax2.set_ylabel("YoY Credit Growth (%)")
    ax2.tick_params(axis='y')
    lines = line1 + line2 + line3
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    ax1.grid(True)
    plt.savefig(Path(G_CHART_DIR, r"India Budget Balance vs Credit", bbox_inches='tight'))


def chart_usdjpy_exporter_breakeven():
    """USDJPY vs Japan Exporter Breakeven Rate -- from charts_updater2.py"""
    END_DATE = datetime.today()
    breakeven_values = [
        175.4, 140.9, 128.1, 133.3, 129.7, 126.2, 124.0, 117.5, 107.8, 104.0,
        106.2, 110.4, 112.7, 106.5, 107.0, 115.3, 114.9, 105.9, 102.6, 104.5,
        106.6, 104.7, 97.3, 92.9, 86.3, 82.0, 83.9, 92.2, 99.0, 103.2,
        100.5, 100.6, 99.8, 100.2, 99.8, 101.5, 114.5, 123.0
    ]
    START_DATE = datetime(1986, 1, 1)
    END_DATE_USDJPY = datetime(2024, 12, 1)
    usdjpy = blp.bdh("USDJPY Curncy", "PX_LAST", START_DATE, END_DATE_USDJPY, Per='Y').droplevel(1, axis=1)
    usdjpy.index = pd.to_datetime(usdjpy.index)
    usdjpy["Breakeven Rate"] = breakeven_values
    usdjpy["Difference"] = usdjpy["USDJPY Curncy"] - usdjpy["Breakeven Rate"]
    latest = usdjpy.last_valid_index().strftime("%b %y")
    fig, ax1 = plt.subplots(figsize=(25, 7))
    line1 = ax1.plot(usdjpy.index, usdjpy["USDJPY Curncy"], label='USDJPY (Left Axis)', color='g')
    line2 = ax1.plot(usdjpy.index, usdjpy["Breakeven Rate"], label='Breakeven Rate (Left Axis)', color='b')
    ax1.axhline(0, color='red', lw=1, linestyle='--')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("USDJPY vs Japan Exporter Breakeven Rate")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=48))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.set_title(f"USDJPY vs Japan Exporter Breakeven Rate as of {latest}")
    ax2 = ax1.twinx()
    ax2.bar(usdjpy.index, usdjpy['Difference'], color="purple", width=15, label="USDJPY - Breakeven Rate (Right Axis)")
    ax2.set_ylabel("USDJPY - Breakeven Rate", color="black")
    ax2.tick_params(axis='y')
    ax2.axhline(0, color='black', lw=1, linestyle='--')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    fig.legend(loc="upper left", bbox_to_anchor=(0.125, 0.88))
    ax1.tick_params(axis='x', rotation=60)
    ax1.set_ylim(60, None)
    ax1.grid(True)
    plt.savefig(Path(G_CHART_DIR, r"USDJPY vs Japan Exporter Breakeven Rate", bbox_inches='tight'))


def chart_fed_wage_growth_vs_unemployment_gap():
    """Fed Wage Growth vs Unemployment Gap -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2012, 1, 1)
    data = blp.bdh(["WGTROVRA Index", "CBOPNRUE Index", "USURTOT Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    data.index = pd.to_datetime(data.index)
    latest = data.last_valid_index().strftime("%b %y")
    data.dropna(inplace=True)
    data["Unemployment Gap"] = data["USURTOT Index"] - data["CBOPNRUE Index"]
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(data.index, data["WGTROVRA Index"], label='Atlanta Fed Wage Growth (%) (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Wage Growth (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"Fed Wage Growth vs Unemployment Gap as of {latest}")
    ax1.invert_yaxis()
    ax2 = ax1.twinx()
    line2 = ax2.plot(data.index, data["Unemployment Gap"], label='Unemployment Gap (Unemployment Rate - NAIRU) (Right Axis)', color='b')
    ax2.set_ylabel("Unemployment Gap (%)")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, r"Fed Wage Growth vs Unemployment Gap", bbox_inches='tight'))


def chart_nz_filled_jobs():
    """NZ Filled Jobs Seasonally Adjusted -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2018, 1, 1)
    employment = blp.bdh("NZEMFJAS Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    employment.index = pd.to_datetime(employment.index)
    latest = employment.last_valid_index().strftime("%b %y")
    unemployment_rate = blp.bdh("NZLFUNER Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    unemployment_rate.index = pd.to_datetime(unemployment_rate.index)
    employment['Monthly Change'] = employment['NZEMFJAS Index'].pct_change() * 100
    employment['Yearly Change'] = employment['NZEMFJAS Index'].pct_change(12) * 100
    employment['NZEMFJAS Index'] = employment['NZEMFJAS Index'] / 1000
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(employment.index, employment["NZEMFJAS Index"], label='NZ Filled Jobs (Left Axis)', color='r')
    ax1.set_xlabel("Date")
    ax1.set_ylim(2000, None)
    ax1.set_ylabel("NZ Filled Jobs (Left Axis), 000s")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"NZ Filled Jobs Seasonally Adjusted as of {latest}")
    ax2 = ax1.twinx()
    ax2.bar(employment.index, employment['Monthly Change'], color="purple", width=20, label="Filled Jobs m/m Change (Right Axis)")
    line2 = ax2.plot(employment.index, employment["Yearly Change"], label='Filled Jobs y/y Change (Right Axis)', color='g')
    ax2.set_ylabel("Filled Jobs Change (%)", color="black")
    ax2.tick_params(axis='y')
    ax2.axhline(0, color='black', lw=1, linestyle='--')
    ax2.yaxis.set_major_locator(MultipleLocator(0.5))
    ax3 = ax1.twinx()
    ax3.plot(unemployment_rate.index, unemployment_rate['NZLFUNER Index'], color="blue", linestyle='--', label="Unemployment Rate (Second Right Axis)")
    ax3.set_ylabel("Unemployment Rate (%)", color="blue")
    ax3.spines['right'].set_position(('outward', 60))
    ax3.tick_params(axis='y', colors='blue')
    ax3.yaxis.set_major_locator(MultipleLocator(0.5))
    ax3.invert_yaxis()
    fig.legend(loc="upper left", bbox_to_anchor=(0.125, 0.88))
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, r"NZ Filled Jobs Seasonally Adjusted", bbox_inches='tight'))


def chart_generic_10y_swap_spread():
    """10Y Generic Swap Spread -- from charts_updater2.py"""
    END_DATE = datetime.today()
    debt_list = {"US": "GDDI111G Index", "AUD": "GDDI193G Index", "NZD": "GDDI196C Index", "GBP": "GDDI112G Index", "CAD": "GDDI156G Index", "GER": "GDDI134G Index"}
    debt_data = blp.bdp(list(debt_list.values()), "LAST PRICE")
    inverse_debt = {v: k for k, v in debt_list.items()}
    debt_data.rename(index=inverse_debt, inplace=True)
    yield_list = ["USGG10YR Index", "GTAUD10Y Govt", "GTNZD10Y Govt", "GTGBP10Y Govt", "GTCAD10Y Govt", "GTDEM10Y Govt", "USOSFR10 Curncy", "ADSWAP10 Curncy", "ADSO10 Curncy", "NDSWAP10 Curncy", "NDSO10 Curncy", "BPSWS10 Curncy", "CDSO10 BGN Curncy", "EUSA10 Curncy"]
    yield_data = blp.bdh(yield_list, "LAST PRICE", END_DATE - BDay(7), END_DATE - BDay(1), "QtTyp = Y").ffill(limit=3).iloc[-1:, :].droplevel(1, axis=1)
    yield_data["US Swap Spread"] = (yield_data["USGG10YR Index"] - yield_data["USOSFR10 Curncy"]) * 100
    yield_data["NZD Swap Spread"] = (yield_data["GTNZD10Y Govt"] - yield_data["NDSWAP10 Curncy"]) * 100
    yield_data["NZD Swap Spread OIS"] = (yield_data["GTNZD10Y Govt"] - yield_data["NDSO10 Curncy"]) * 100
    yield_data["AUD Swap Spread"] = (yield_data["GTAUD10Y Govt"] - yield_data["ADSWAP10 Curncy"]) * 100
    yield_data["AUD Swap Spread OIS"] = (yield_data["GTAUD10Y Govt"] - yield_data["ADSO10 Curncy"]) * 100
    yield_data["GBP Swap Spread"] = (yield_data["GTGBP10Y Govt"] - yield_data["BPSWS10 Curncy"]) * 100
    yield_data["CAD Swap Spread"] = (yield_data["GTCAD10Y Govt"] - yield_data["CDSO10 BGN Curncy"]) * 100
    yield_data["GER Swap Spread"] = (yield_data["GTDEM10Y Govt"] - yield_data["EUSA10 Curncy"]) * 100
    swap_spreads = {
        'US': yield_data['US Swap Spread'].iloc[0],
        'NZD': yield_data['NZD Swap Spread'].iloc[0],
        'NZD (OIS)': yield_data['NZD Swap Spread OIS'].iloc[0],
        'AUD': yield_data['AUD Swap Spread'].iloc[0],
        'AUD (OIS)': yield_data['AUD Swap Spread OIS'].iloc[0],
        'GBP': yield_data['GBP Swap Spread'].iloc[0],
        'CAD': yield_data['CAD Swap Spread'].iloc[0],
        'GER': yield_data['GER Swap Spread'].iloc[0]
    }
    plot_df = pd.DataFrame({
        'Country': list(swap_spreads.keys()),
        'Swap Spread': list(swap_spreads.values()),
        '% GDP': [debt_data.loc[country_code.replace(" (OIS)", ""), 'last_price'] if country_code.replace(" (OIS)", "") in debt_data.index else None for country_code in swap_spreads.keys()]
    })
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(plot_df['% GDP'], plot_df['Swap Spread'], color='blue')
    for i, txt in enumerate(plot_df['Country']):
        ax.annotate(txt, (plot_df['% GDP'][i], plot_df['Swap Spread'][i]), textcoords="offset points", xytext=(5, 5), ha='center')
    ax.set_xlim(0, plot_df['% GDP'].max() + 10)
    ax.set_title('Generic 10y Bond - Swap vs Government Debt (% GDP)')
    ax.set_xlabel('Government Debt (% GDP)')
    ax.set_ylabel('10y Generic Bond - Swap Spread (Basis Points)')
    ax.axhline(0, color='black', linewidth=0.5)
    ax.grid(True, linestyle='--', linewidth=0.5)
    plt.savefig(Path(G_CHART_DIR, r"10Y Generic Swap Spread", bbox_inches='tight'))


def chart_2y_acm_vs_swap_fly():
    """2y ACM vs Swap Fly -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2010, 1, 1)
    usts = blp.bdh(["USOSFR1 Curncy", "S0490FS 1Y1Y BLC Curncy", "S0490FS 2Y1Y BLC Curncy", "ACMTP02 Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    usts.index = pd.to_datetime(usts.index)
    usts['fly'] = (2 * usts['S0490FS 1Y1Y BLC Curncy'] - usts['USOSFR1 Curncy'] - usts['S0490FS 2Y1Y BLC Curncy']) * 100
    usts.dropna(inplace=True)
    latest = usts.last_valid_index().strftime("%b %y")
    fig, ax1 = plt.subplots(figsize=(20, 7))
    line1 = ax1.plot(usts.index, usts["ACMTP02 Index"], label='2y ACM TP', color='grey')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("2y ACM TP")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax1.set_title(f"2y ACM Term Premium vs 1y/1y1y/2y1y Swap Fly as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(usts.index, usts["fly"], label='1y/1y1y/2y1y Fly', color='r')
    ax2.set_ylabel("1y/1y1y/2y1y Fly (bps)")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    ax1.grid(True)
    plt.savefig(Path(G_CHART_DIR, r"2y ACM vs Swap Fly", bbox_inches='tight'))


def chart_acm_correlation():
    """10y ACM vs 5s10s30s UST Correlation vs ACM Correlation -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2010, 1, 1)
    usts = blp.bdh(["USGG5YR Index", "USGG10YR Index", "USGG30YR Index", "ACMTP10 Index", "ACMTP02 Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    usts.index = pd.to_datetime(usts.index)
    usts['5s10s30s'] = 2 * usts['USGG10YR Index'] - usts['USGG5YR Index'] - usts['USGG30YR Index']
    latest = usts.last_valid_index().strftime("%b %y")
    usts['ust_5d_chg'] = usts['5s10s30s'] - usts['5s10s30s'].shift(5, freq='B')
    usts['acm2y_5d_chg'] = usts["ACMTP02 Index"] - usts["ACMTP02 Index"].shift(5, freq='B')
    usts['acm10y_5d_chg'] = usts["ACMTP10 Index"] - usts["ACMTP10 Index"].shift(5, freq='B')
    usts.dropna(inplace=True)
    usts['ust correlation'] = usts['acm10y_5d_chg'].rolling(window=100).corr(usts['ust_5d_chg'])
    usts['acm correlation'] = usts['acm10y_5d_chg'].rolling(window=100).corr(usts['acm2y_5d_chg'])
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(usts.index, usts["ust correlation"], label='10y ACM TP vs 5s10s30s correlation', color='grey')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("10y ACM TP vs 5s10s30s correlation")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    ax1.set_title(f"10y ACM vs 5s10s30s UST Correlation and 2Y vs 10Y ACM Correlation as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(usts.index, usts["acm correlation"], label='2Y vs 10Y ACM Correlation', color='r')
    ax2.set_ylabel("2Y vs 10Y ACM Correlation")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper right")
    ax1.tick_params(axis='x', rotation=60)
    ax1.grid(True)
    plt.savefig(Path(G_CHART_DIR, r"10y ACM vs 5s10s30s UST Correlation vs ACM Correlation", bbox_inches='tight'))


def chart_orders_inventory_change_vs_ip():
    """Orders Inventory Change vs IP -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    gdp = blp.bdh(["GDNSCHWN Index", "UKGRABMI Index", "JGDPOGDP Index", "ENGKEMU Index", "CGE9MP Index", "USDJPY Curncy", "USDGBP Curncy", "USDEUR Curncy", "USDCAD Curncy"], "PX_LAST", START_DATE, END_DATE, Per='M').droplevel(1, axis=1)
    gdp.index = pd.to_datetime(gdp.index)
    latest = gdp.last_valid_index().strftime("%b %y")
    gdp["UKGRABMI Index"] = gdp["UKGRABMI Index"] / 1000
    gdp["ENGKEMU Index"] = gdp["ENGKEMU Index"] / 1000
    gdp["UKGRABMI Index"] = gdp["UKGRABMI Index"] / gdp["USDGBP Curncy"]
    gdp["JGDPOGDP Index"] = gdp["JGDPOGDP Index"] / gdp["USDJPY Curncy"]
    gdp["ENGKEMU Index"] = gdp["ENGKEMU Index"] / gdp["USDEUR Curncy"]
    gdp["CGE9MP Index"] = gdp["CGE9MP Index"] / gdp["USDCAD Curncy"]
    gdp["Total GDP"] = gdp["GDNSCHWN Index"] + gdp["UKGRABMI Index"] + gdp["JGDPOGDP Index"] + gdp["ENGKEMU Index"] + gdp["CGE9MP Index"]
    gdp["US Weights"] = gdp["GDNSCHWN Index"] / gdp["Total GDP"]
    gdp["UK Weights"] = gdp["UKGRABMI Index"] / gdp["Total GDP"]
    gdp["JPY Weights"] = gdp["JGDPOGDP Index"] / gdp["Total GDP"]
    gdp["EUR Weights"] = gdp["ENGKEMU Index"] / gdp["Total GDP"]
    gdp["CAD Weights"] = gdp["CGE9MP Index"] / gdp["Total GDP"]
    gdp = gdp.dropna()
    ip = blp.bdh(["IP Index", "JNIP Index", "UKIPI Index", "EUITEMU Index", "CAGPINDP Index"], "PX_LAST", START_DATE, END_DATE, Per='Q').droplevel(1, axis=1)
    ip.index = pd.to_datetime(ip.index)
    columns_to_ffill = ['JNIP Index', 'UKIPI Index', 'EUITEMU Index', 'CAGPINDP Index']
    ip[columns_to_ffill] = ip[columns_to_ffill].fillna(method='ffill')
    ip = ip.dropna()
    ip.index = ip.index.to_period('M')
    gdp.index = gdp.index.to_period('M')
    aligned_ip = ip.join(gdp, how='inner')
    aligned_ip['US Weighted IP'] = aligned_ip['IP Index'] * aligned_ip['US Weights']
    aligned_ip['Japan Weighted IP'] = aligned_ip['JNIP Index'] * aligned_ip['JPY Weights']
    aligned_ip['UK Weighted IP'] = aligned_ip['UKIPI Index'] * aligned_ip['UK Weights']
    aligned_ip['EU Weighted IP'] = aligned_ip['EUITEMU Index'] * aligned_ip['EUR Weights']
    aligned_ip['Canada Weighted IP'] = aligned_ip['CAGPINDP Index'] * aligned_ip['CAD Weights']
    aligned_ip['Global Weighted IP'] = (
        aligned_ip['US Weighted IP'] +
        aligned_ip['Japan Weighted IP'] +
        aligned_ip['UK Weighted IP'] +
        aligned_ip['EU Weighted IP'] +
        aligned_ip['Canada Weighted IP']
    )
    aligned_ip["Global IP YoY Change"] = aligned_ip["Global Weighted IP"].pct_change(4) * 100
    orders_inventories = blp.bdh(["NAPMNEWO Index", "NAPMINV Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    orders_inventories.index = pd.to_datetime(orders_inventories.index)
    orders_inventories['Ratio'] = orders_inventories['NAPMNEWO Index'] / orders_inventories['NAPMINV Index']
    orders_inventories['Annual Ratio Change'] = orders_inventories['Ratio'].pct_change(12) * 100
    aligned_ip.index = aligned_ip.index.to_timestamp(how='end')
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(orders_inventories.index, orders_inventories["Annual Ratio Change"], label='Orders/ Inventory Ratio Y/Y Change (%) (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Orders/ Inventory Ratio Annual Change (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"Orders/ Inventory Change vs IP as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(aligned_ip.index, aligned_ip["Global IP YoY Change"], label='Global IP Y/Y Change (%) (US, UK, Japan, Canada, Europe)  (Right Axis)', color='b')
    ax2.set_ylabel("Global Industrial Production Y/Y Change(%)")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, r"Orders Inventory Change vs IP", bbox_inches='tight'))


def chart_eci_vs_nfib_labour_quality():
    """Employment Cost Index vs NFIB Labour Quality -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    quality = blp.bdh("SBOIQUAL Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    employment = blp.bdh("ECICCVYY Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    quality.index = pd.to_datetime(quality.index)
    employment.index = pd.to_datetime(employment.index)
    quality['Yoy Change'] = quality['SBOIQUAL Index'].pct_change(12) * 100
    latest = employment.last_valid_index().strftime("%b %y")
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(quality.index, quality["Yoy Change"], label='NFIB Labour Quality Y/Y Change (%) (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("NFIB Labour Quality Y/Y Change (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"Employment Cost Index vs NFIB Labour Quality as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(employment.index, employment["ECICCVYY Index"], label='Employmet Cost Index Y/Y Change (%) (Right Axis)', color='b')
    ax2.set_ylabel("Labour Quality Y/Y Change (%)")
    ax2.tick_params(axis='y')
    ax1.axhline(0, color='black', lw=1, linestyle='--')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, r"Employment Cost Index vs NFIB Labour Quality", bbox_inches='tight'))


def chart_usts_vs_uncertainty_index():
    """USTs vs Uncertainty Index -- from charts_updater2.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    uncertainty = blp.bdh("EPUCGLCP Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    usts = blp.bdh("GIND10YR Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    uncertainty.index = pd.to_datetime(uncertainty.index)
    usts.index = pd.to_datetime(usts.index)
    latest = uncertainty.last_valid_index().strftime("%b %y")
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(usts.index, usts["GIND10YR Index"], label='10 Year USTs (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("10 Year USTs")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"10 Year USTs vs Global Economic Policy Uncertainty Index as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(uncertainty.index, uncertainty["EPUCGLCP Index"], label='Global Economic Policy Uncertainty Index (Right Axis)', color='b')
    ax2.set_ylabel("Global Economic Policy Uncertainty Index")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, r"USTs vs Uncertainty Index", bbox_inches='tight'))


# UPDATER2_CHARTS = [
#     ("Mani PMI and Core PCE", chart_manu_pmi_and_core_pce),
#     ("Labor Quality and Wages", chart_labor_quality_and_wages),
#     ("Australia Total Compensation, WPi and Hours Worked", chart_australia_wage),
#     ("Australia Unemployment Expectations Leads Unemployment Rate", chart_australia_unemployment_expectations),
#     ("US ISM PMI and Share of Companies Reporting Decreases", chart_us_ism_pmi),
#     ("India Exp YOY excl Interest", chart_india_exp_yoy_excl_interest),
#     ("5Y Implied By Beta.png", chart_yield_and_ccy_betas),
#     ("Euro Area PMI vs Sentix", chart_euro_area_pmi_vs_sentix),
#     ("Australia Westpac Leading Index vs GDP", chart_australia_westpac_leading_index),
#     ("10y ACM vs 5s10s30s UST Correlation vs ISM Manufacturing", chart_acm_term_premium_vs_ism),
#     ("Singapore Tradable Core Inflation vs Trading Partners", chart_sg_tradable_core_inflation),
#     ("US Share of Total Unemployment", chart_us_share_of_total_unemployment),
#     ("India Budget Balance vs Credit", chart_india_budget_balance_vs_credit),
#     ("USDJPY vs Japan Exporter Breakeven Rate", chart_usdjpy_exporter_breakeven),
#     ("Fed Wage Growth vs Unemployment Gap", chart_fed_wage_growth_vs_unemployment_gap),
#     ("NZ Filled Jobs Seasonally Adjusted", chart_nz_filled_jobs),
#     ("10Y Generic Swap Spread", chart_generic_10y_swap_spread),
#     ("2y ACM vs Swap Fly", chart_2y_acm_vs_swap_fly),
#     ("10y ACM vs 5s10s30s UST Correlation vs ACM Correlation", chart_acm_correlation),
#     ("Orders Inventory Change vs IP", chart_orders_inventory_change_vs_ip),
#     ("Employment Cost Index vs NFIB Labour Quality", chart_eci_vs_nfib_labour_quality),
#     ("USTs vs Uncertainty Index", chart_usts_vs_uncertainty_index),
# ]

# --- Charts from charts_updater3.py ---

def chart_marginal_propensity_to_save():
    """Marginal Propensity To Save -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2016, 1, 1)
    base = '2019-12-31'
    korea = blp.bdh(
        ["KOMBTDL6 Index", "KOMBTD61 Index", "KOMBTD12 Index", "KOMBTD23 Index", "KOMBTDO3 Index", "KOMBDDMD Index"],
        "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    korea.index = pd.to_datetime(korea.index)
    latest = korea.last_valid_index().strftime("%b %y")
    korea["Time Deposits"] = korea.iloc[:, :5].sum(axis=1)
    korea["Marginal Propensity to Save"] = korea["Time Deposits"] / korea["KOMBDDMD Index"]
    korea = base_series_to_date(korea, "Marginal Propensity to Save", base_date=base)
    thailand = blp.bdh(["TLDGTATD Index", "TLDGTADD Index", "TLDGTASD Index"], "PX_LAST", START_DATE, END_DATE).droplevel(
        1, axis=1)
    thailand.index = pd.to_datetime(thailand.index)
    thailand["Time Deposits"] = thailand["TLDGTATD Index"] - thailand["TLDGTADD Index"] - thailand["TLDGTASD Index"]
    thailand["Marginal Propensity to Save"] = thailand["Time Deposits"] / thailand["TLDGTADD Index"]
    thailand = base_series_to_date(thailand, "Marginal Propensity to Save", base_date=base)
    china = blp.bdh(["CHBDLBPT Index", "CHBDLBPE Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    china.index = pd.to_datetime(china.index)
    china["Marginal Propensity to Save"] = china["CHBDLBPT Index"] / china["CHBDLBPE Index"]
    china = base_series_to_date(china, "Marginal Propensity to Save", base_date=base)
    US = blp.bdh(["PPIDBK11 Index", "PPIDBK12 Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    US.index = pd.to_datetime(US.index)
    US["Marginal Propensity to Save"] = US["PPIDBK12 Index"] / US["PPIDBK11 Index"]
    US = base_series_to_date(US, "Marginal Propensity to Save", base_date=base)
    AU = blp.bdh(["AUBKATRD Index", "AUBKACOD Index", "AUBKALDD Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1,
                                                                                                                      axis=1)
    AU.index = pd.to_datetime(AU.index)
    AU["Time Deposits"] = AU["AUBKATRD Index"] + AU["AUBKACOD Index"]
    AU["Marginal Propensity to Save"] = AU["Time Deposits"] / AU["AUBKALDD Index"]
    AU = base_series_to_date(AU, "Marginal Propensity to Save", base_date=base)
    nzd = blp.bdh(["NZBBLTDB Index", "NZBBLTBL Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    nzd.index = pd.to_datetime(nzd.index)
    nzd["Marginal Propensity to Save"] = nzd["NZBBLTDB Index"] / nzd["NZBBLTBL Index"]
    nzd = base_series_to_date(nzd, "Marginal Propensity to Save", base_date=base)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(korea.index, korea["Marginal Propensity to Save (Indexed)"], label='Korea MPS (Left Axis)',
                     color='green')
    line2 = ax1.plot(thailand.index, thailand["Marginal Propensity to Save (Indexed)"], label='Thailand MPS (Left Axis)',
                     color='orange')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Marginal Propensity to Save Indexed to 2019-12-31 (KRW & THB)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax2 = ax1.twinx()
    line3 = ax2.plot(AU.index, AU["Marginal Propensity to Save (Indexed)"], label='Australia MPS (Right Axis)',
                     color='blue')
    line4 = ax2.plot(nzd.index, nzd["Marginal Propensity to Save (Indexed)"], label='New Zealand MPS (Right Axis)',
                     color='purple')
    line5 = ax2.plot(china.index, china["Marginal Propensity to Save (Indexed)"], label='China MPS (Right Axis)',
                     color='salmon')
    line6 = ax2.plot(US.index, US["Marginal Propensity to Save (Indexed)"], label='US MPS (Right Axis)', color='black')
    ax2.set_ylabel("Marginal Propensity to Save Indexed to 2019-12-31 (AUD, NZD, CNY, USD)")
    ax2.tick_params(axis='y')
    ax1.set_title(f"Marginal Propensity to Save (2019 Dec = 100) as of {latest}")
    lines = line1 + line2 + line3 + line4 + line5 + line6
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, "Marginal Propensity To Save"), bbox_inches='tight')
    plt.close(fig)


def chart_nfp_vs_ceo_confidence_and_operating_profits():
    """NFP vs CEO Confidence and Operating Profits -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2003, 1, 1)
    business = blp.bdh(["CEOCINDX Index", "CPFTYOY Index"], "PX_LAST", START_DATE, END_DATE, Per='Q').droplevel(1, axis=1)
    nfp = blp.bdh(["NFP NYOY Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    nfp.index = pd.to_datetime(nfp.index)
    business.index = pd.to_datetime(business.index)
    latest = business.last_valid_index().strftime("%b %y")
    business["CEOCINDX Index"] = business["CEOCINDX Index"].fillna(method='ffill')
    business = business.dropna()
    business['CEOCINDX_4Q_MA'] = business['CEOCINDX Index'].rolling(window=4).mean()
    business["CEOCINDX YoY"] = business['CEOCINDX_4Q_MA'].pct_change(4) * 100
    future_dates = pd.date_range(start=business.index[-1] + pd.offsets.QuarterEnd(1),
                                 periods=4, freq='Q')
    future_df = pd.DataFrame(index=future_dates, columns=business.columns)
    business_extended = pd.concat([business, future_df])
    business_extended['CEOCINDX_YoY_Advanced'] = business_extended['CEOCINDX YoY'].shift(4)
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(business_extended.index, business_extended["CEOCINDX_YoY_Advanced"],
                     label='CEO Confidence 4Q MA Y/Y Change (%) (Advanced) (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("CEO Confidence Percentage Change (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"NFP Employment Growth vs CEO Confidence (Advanced 1 Year) and Operating Profits as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(business_extended.index, business_extended["CPFTYOY Index"], label='Operating Profits  (Right Axis)',
                     color='b')
    ax2.set_ylabel("Operating Profits Percentage Change (%)")
    ax2.tick_params(axis='y')
    ax3 = ax1.twinx()
    line3 = ax3.plot(nfp.index, nfp['NFP NYOY Index'], color="red",
                     label="NFP Employment Growth Y/Y (%) (Second Right Axis)")
    ax3.set_ylabel("NFP Employment Growth Y/Y (%)", color="red")
    ax3.set_ylim(-1, 4)
    ax3.spines['right'].set_position(('outward', 60))
    ax3.tick_params(axis='y', colors='red')
    lines = line1 + line2 + line3
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, "NFP vs CEO Confidence and Operating Profits"), bbox_inches='tight')
    plt.close(fig)


def chart_inventory_to_shipment_ratios():
    """Inventory To Shipment Ratios -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    ratio = blp.bdh(["JNISIVR Index", "KOPII Index", "KOPSI Index"], "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    ratio.index = pd.to_datetime(ratio.index)
    latest = ratio.last_valid_index().strftime("%b %y")
    ratio["Korea Ratio"] = (ratio["KOPII Index"] / ratio["KOPSI Index"]) * 100
    ratio["Japan 3M Average"] = ratio["JNISIVR Index"].rolling(window=3).mean()
    ratio["Korea 3M Average"] = ratio["Korea Ratio"].rolling(window=3).mean()
    ratio["Japan 3M Average Y/Y"] = ratio["Japan 3M Average"].pct_change(12) * 100
    ratio["Korea 3M Average Y/Y"] = ratio["Korea 3M Average"].pct_change(12) * 100
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 18), sharex=False, gridspec_kw={'height_ratios': [1, 1]})
    line1 = ax1.plot(ratio.index, ratio["Japan 3M Average Y/Y"],
                     label='Japan Inventory/Shipment Ratio 3M Average Y/Y Change (%)', color='red')
    line2 = ax1.plot(ratio.index, ratio["Korea 3M Average Y/Y"],
                     label='Korea Inventory/Shipment Ratio 3M Average Y/Y Change (%)', color='green')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Inventory/Shipment Ratio Y/Y Change (%) ")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"Inventory To Shipment Ratios Y/Y Change (%) as of {latest}")
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    latest_ratio = ratio[(ratio.index >= datetime(2019, 1, 1))]
    line3 = ax2.plot(latest_ratio.index, latest_ratio["Japan 3M Average Y/Y"],
                     label='Japan Inventory/Shipment Ratio 3M Average Y/Y Change (%)', color='red')
    line4 = ax2.plot(latest_ratio.index, latest_ratio["Korea 3M Average Y/Y"],
                     label='Korea Inventory/Shipment Ratio 3M Average Y/Y Change (%)', color='green')
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Inventory/Shipment Ratio Y/Y Change (%) ")
    ax2.tick_params(axis='y')
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax2.set_title(f"Inventory To Shipment Ratios Y/Y Change (%) Since 2019 as of {latest}")
    lines = line3 + line4
    labels = [l.get_label() for l in lines]
    ax2.legend(lines, labels, loc="upper left")
    ax2.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, "Inventory To Shipment Ratios"), bbox_inches='tight')
    plt.close(fig)


def chart_service_activity():
    """Service Activity -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    services = blp.bdh(["NAPMPMI Index", "NAPMNMI Index", "RHOTPNAT Index", "NRASRPI Index", "USHBTRAF Index"], "PX_LAST",
                       START_DATE, END_DATE).droplevel(1, axis=1)
    services.index = pd.to_datetime(services.index)
    latest = services.last_valid_index().strftime("%b %y")
    services["Non Manu/Manu"] = services["NAPMNMI Index"] / services["NAPMPMI Index"]
    last_4_cols = services.columns[-4:]
    for col in last_4_cols:
        services[f"{col}_YoY_Change"] = services[col].pct_change(periods=12) * 100
        services[f"{col}_3M_MA_YoY_Change"] = services[f"{col}_YoY_Change"].rolling(3).mean()
    label_col_map = {
        'RHOTPNAT Index': ('Hotel Price Per Room', 'red'),
        'NRASRPI Index': ('Restaurant Performance', 'green'),
        'USHBTRAF Index': ('Buyers Traffic', 'blue'),
    }
    fig, axs = plt.subplots(nrows=4, ncols=1, figsize=(20, 24))
    ax1 = axs[0]
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    line1 = ax1.plot(services.index, services["RHOTPNAT Index_YoY_Change"],
                     label='Hotel Price Per Room Y/Y Change (%) (Left Axis)', color='red')
    line2 = ax3.plot(services.index, services["NRASRPI Index_YoY_Change"],
                     label='Restaurant Performance Y/Y Change (%) (Second Right Axis)', color='green')
    line3 = ax2.plot(services.index, services["USHBTRAF Index_YoY_Change"],
                     label='Buyers Traffic Y/Y Change (%) (Right Axis)', color='blue')
    line4 = ax1.plot(services.index, services["Non Manu/Manu_YoY_Change"],
                     label='Non-Manufacturing/Manufacturing PMI Y/Y Change (%) (Left Axis)', linewidth=2.5, color='purple')
    ax2.set_ylabel("Buyers Traffic Percentage Change (%)", color='blue')
    ax2.tick_params(axis='y', colors='blue')
    ax3.set_ylabel("Restaurant Performance Percentage Change (%)", color="green")
    ax3.spines['right'].set_position(('outward', 60))
    ax3.tick_params(axis='y', colors='green')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Percentage Changes (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"Service Activity as of {latest}")
    lines = line1 + line2 + line3 + line4
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    other_cols = [col for col in last_4_cols if col != 'Non Manu/Manu']
    for i, col in enumerate(other_cols):
        ax4 = axs[i + 1]
        ax5 = ax4.twinx()
        col_label, col_color = label_col_map.get(col, (col, 'black'))
        line1 = ax4.plot(services.index, services['Non Manu/Manu_3M_MA_YoY_Change'],
                         label='Non-Manufacturing/Manufacturing PMI 3M MA Y/Y Change (%) (Left Axis)', color='purple')
        line2 = ax5.plot(services.index, services[f'{col}_3M_MA_YoY_Change'],
                         label=f'{col_label} 3M MA Y/Y Change (%) (Right Axis)', color=col_color)
        ax4.set_title(f"Non-Manufacturing/Manufacturing PMI vs {col_label} 3M MA Y/Y Change as of {latest}")
        ax4.set_xlabel("Date")
        ax4.set_ylabel("Non Manu/Manu 3M MA Y/Y Percentage Changes (%)")
        ax5.set_ylabel(f"{col_label} 3M MA Y/Y Percentage Changes (%)")
        ax5.tick_params(axis='y')
        ax4.xaxis.set_major_locator(mdates.MonthLocator(interval=8))
        ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
        ax4.tick_params(axis='x', rotation=60)
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc="upper left")
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Service Activity"), bbox_inches='tight')
    plt.close(fig)


def chart_us_money_velocity_vs_core_cpi():
    """US Money Velocity vs Core CPI -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2014, 1, 1)
    velocity = blp.bdh("VELOM2 Index", "PX_LAST", START_DATE, END_DATE, Per='Q').droplevel(1, axis=1)
    cpi = blp.bdh("CPI XYOY Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    cpi.index = pd.to_datetime(cpi.index)
    velocity.index = pd.to_datetime(velocity.index)
    latest = cpi.last_valid_index().strftime("%b %y")
    velocity["Change"] = velocity["VELOM2 Index"].pct_change(4) * 100
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(velocity.index, velocity["Change"], label='US Money Velocity (Nominal GDP/M2) Y/Y(%) (Left Axis)',
                     color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("US Money Velocity Y/Y (%)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[3, 6, 9, 12]))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"US Money Velocity vs Core CPI as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(cpi.index, cpi["CPI XYOY Index"], label='Core CPI  (Right Axis)', color='b')
    ax2.set_ylabel("Core CPI Y/Y (%)")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, "US Money Velocity vs Core CPI"), bbox_inches='tight')
    plt.close(fig)


def chart_china_railway_freight_traffic_turnover():
    """China Railway Freight Traffic Turnover -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2014, 1, 1)
    freight = blp.bdh("CNRWRFTO Index", "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
    freight.index = pd.to_datetime(freight.index)
    latest = freight.last_valid_index().strftime("%b %y")
    freight['Monthly'] = 0.0
    for i in range(len(freight)):
        if freight.index[i].month == 1:
            freight['Monthly'].iloc[i] = freight["CNRWRFTO Index"].iloc[i]
        else:
            freight['Monthly'].iloc[i] = freight["CNRWRFTO Index"].iloc[i] - freight["CNRWRFTO Index"].iloc[i - 1]
    freight['12M Trailing Sum'] = freight['Monthly'].rolling(window=12).sum()
    freight['6M MA'] = freight['12M Trailing Sum'].rolling(window=6).mean()
    freight["YoY Change"] = freight['12M Trailing Sum'].pct_change(periods=12) * 100
    freight['YoY Change 6M MA'] = freight["YoY Change"].rolling(window=6).mean()
    fig, ax1 = plt.subplots(figsize=(20, 6))
    line1 = ax1.plot(freight.index, freight["6M MA"],
                     label='6M Moving Average of 12M Trailing Sum Freight Turnover (Left Axis)', color='g')
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Freight Turnover (billions)")
    ax1.tick_params(axis='y')
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.set_title(f"China Railway Freight Traffic Turnover as of {latest}")
    ax2 = ax1.twinx()
    line2 = ax2.plot(freight.index, freight["YoY Change 6M MA"],
                     label='6M Moving Average of Freight Turnover Y/Y (%) (Right Axis)', color='b')
    ax2.set_ylabel("Freight Turnover Y/Y (%)")
    ax2.tick_params(axis='y')
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")
    ax1.tick_params(axis='x', rotation=60)
    plt.savefig(Path(G_CHART_DIR, "China Railway Freight Traffic Turnover"), bbox_inches='tight')
    plt.close(fig)

def chart_em_composite_eps_vs_index_px():
    """EM Composite EPS vs Index Px -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2010, 1, 1)
    country_indices = ['SHSZ300 Index', 'NSE500 Index', 'IBOV Index', 'KOSPI Index', 'TWSE Index', 'MEXBOL Index',
                       'SET Index', 'SASEIDX Index']
    fields = ['PX_LAST', 'BEST_EPS', 'CUR_MKT_CAP']
    country_name_map = {'SHSZ300 Index': 'China', 'NSE500 Index': 'India', 'IBOV Index': 'Brazil',
                        'KOSPI Index': 'South Korea', 'TWSE Index': 'Taiwan', 'MEXBOL Index': 'Mexico',
                        'SET Index': 'Thailand', 'SASEIDX Index': 'South Africa'}
    eps_data = blp.bdh(country_indices, fields, START_DATE, END_DATE)
    eps_data.index = pd.to_datetime(eps_data.index)
    eps_data = eps_data.resample('M').mean()
    eps_data[('KOSPI Index', 'BEST_EPS')].loc['2025-02-28'] = 277.1574
    currencies = ['USDCNY Curncy', 'USDINR Curncy', 'USDBRL Curncy', 'USDKRW Curncy', 'USDTWD Curncy', 'USDMXN Curncy',
                  'USDTHB Curncy', 'USDSAR Curncy']
    currency_data = blp.bdh(currencies, 'PX_LAST', START_DATE, END_DATE)
    currency_data.index = pd.to_datetime(currency_data.index)
    currency_data = currency_data.resample('M').mean()
    currency_map = {'SHSZ300 Index': 'USDCNY Curncy', 'NSE500 Index': 'USDINR Curncy', 'IBOV Index': 'USDBRL Curncy',
                    'KOSPI Index': 'USDKRW Curncy', 'TWSE Index': 'USDTWD Curncy', 'MEXBOL Index': 'USDMXN Curncy',
                    'SET Index': 'USDTHB Curncy', 'SASEIDX Index': 'USDSAR Curncy'}
    for market in country_indices:
        cur_ticker = currency_map[market]
        cs = currency_data[cur_ticker]
        if isinstance(cs, pd.DataFrame):
            cs = cs.iloc[:, 0]
        cs = cs.reindex(eps_data.index).ffill()
        eps_data[(market, 'CUR_MKT_CAP_USD')] = eps_data[(market, 'CUR_MKT_CAP')] / cs
    rebased_index = {}
    rebased_eps = {}
    for market in country_indices:
        first_px = eps_data[(market, 'PX_LAST')].iloc[0]
        first_eps = eps_data[(market, 'BEST_EPS')].iloc[0]
        rebased_index[market] = eps_data[(market, 'PX_LAST')] / first_px * 100
        rebased_eps[market] = eps_data[(market, 'BEST_EPS')] / first_eps * 100
    weights_df = pd.DataFrame({market: eps_data[(market, 'CUR_MKT_CAP_USD')] for market in country_indices},
                              index=eps_data.index)
    total_cap = weights_df.sum(axis=1)
    weights_df = weights_df.divide(total_cap, axis=0)
    composite_index = pd.Series(index=eps_data.index, dtype=float)
    composite_eps = pd.Series(index=eps_data.index, dtype=float)
    composite_index.iloc[0] = 100.0
    composite_eps.iloc[0] = 100.0
    for i in range(1, len(eps_data.index)):
        curr_date = eps_data.index[i]
        prev_date = eps_data.index[i - 1]
        comp_return = 0.0
        comp_eps_return = 0.0
        for market in country_indices:
            pct_change = (rebased_index[market].loc[curr_date] / rebased_index[market].loc[prev_date]) - 1
            pct_eps_change = (rebased_eps[market].loc[curr_date] / rebased_eps[market].loc[prev_date]) - 1
            weight = weights_df.loc[curr_date, market]
            comp_return += weight * pct_change
            comp_eps_return += weight * pct_eps_change
        composite_index.loc[curr_date] = composite_index.loc[prev_date] * (1 + comp_return)
        composite_eps.loc[curr_date] = composite_eps.loc[prev_date] * (1 + comp_eps_return)
    fig, axs = plt.subplots(3, 3, figsize=(18, 15))
    plt.subplots_adjust(wspace=0.4)
    axs = axs.flatten()
    ax = axs[0]
    line1, = ax.plot(composite_index.index, composite_index, color='blue', label='Composite Index (Rebased)')
    ax.set_title("Composite EM", fontsize=12)
    ax.set_ylabel("Composite Index (Rebased)", color='blue')
    ax.tick_params(axis='y', colors='blue')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.tick_params(axis='x', rotation=45)
    ax2 = ax.twinx()
    line2, = ax2.plot(composite_eps.index, composite_eps, color='red', label='Composite EPS (Rebased)')
    ax2.set_ylabel("Composite EPS (Rebased)", color='red')
    ax2.tick_params(axis='y', colors='red')
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    legend = ax.legend(lines, labels, loc='upper left', fontsize=9)
    latest_comp = composite_index.last_valid_index().strftime('%b %Y')
    ax.text(0.03, 0.83, f"Latest: {latest_comp}", transform=ax.transAxes, fontsize=8, color='black')
    for j, market in enumerate(country_indices):
        ax = axs[j + 1]
        line1, = ax.plot(eps_data.index, eps_data[(market, 'PX_LAST')], color='blue', label='Index Px')
        ax.set_title(f"{country_name_map[market]} ({market})", fontsize=12)
        ax.set_ylabel("Index Px", color='blue')
        ax.tick_params(axis='y', colors='blue')
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.tick_params(axis='x', rotation=45)
        ax2 = ax.twinx()
        line2, = ax2.plot(eps_data.index, eps_data[(market, 'BEST_EPS')], color='red', label='Fwd EPS')
        ax2.set_ylabel("Fwd EPS", color='red')
        ax2.tick_params(axis='y', colors='red')
        lines = [line1, line2]
        labels = [l.get_label() for l in lines]
        legend = ax.legend(lines, labels, loc='upper left', fontsize=9)
        latest_market = eps_data[(market, 'PX_LAST')].dropna().last_valid_index().strftime('%b %Y')
        ax.text(0.03, 0.83, f"Latest: {latest_market}", transform=ax.transAxes, fontsize=8, color='black')
    plt.savefig(Path(G_CHART_DIR, "EM Composite EPS vs Index Px"), bbox_inches='tight')
    plt.close(fig)


def chart_prime_age_employment_rate():
    """Prime Age Employment Rate -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(1980, 1, 1)
    employment_rate = blp.bdh("USER54SA Index", "PX_LAST", START_DATE, END_DATE)
    employment_rate.index = pd.to_datetime(employment_rate.index)
    employment_rate.columns = ["Employment Rate"]
    prime_participation_rate = blp.bdh("PRUSQNTS Index", "PX_LAST", START_DATE, END_DATE)
    prime_participation_rate.index = pd.to_datetime(prime_participation_rate.index)
    prime_participation_rate.columns = ["Participation Rate"]
    official_unemployment_rate = 1 - (employment_rate["Employment Rate"] / prime_participation_rate["Participation Rate"])
    official_unemployment_rate = official_unemployment_rate.to_frame(name="Official Unemployment Rate")
    common_dates = employment_rate.dropna().index.intersection(prime_participation_rate.dropna().index).intersection(
        official_unemployment_rate.dropna().index)
    if not common_dates.empty:
        last_date = common_dates.max()
        last_date_str = last_date.strftime("%b %Y")
    else:
        last_date = None
        last_date_str = "N/A"
    fig, axs = plt.subplots(2, 1, figsize=(12, 12))
    start_zoom = datetime(2018, 1, 1)
    emp_zoom = employment_rate.loc[employment_rate.index >= start_zoom]
    part_zoom = prime_participation_rate.loc[prime_participation_rate.index >= start_zoom]
    unemp_zoom = official_unemployment_rate.loc[official_unemployment_rate.index >= start_zoom]
    ax_zoom = axs[0]
    line_emp_zoom, = ax_zoom.plot(emp_zoom.index, emp_zoom["Employment Rate"],
                                  color="royalblue", linewidth=2, label="Employment-Population Ratio")
    line_part_zoom, = ax_zoom.plot(part_zoom.index, part_zoom["Participation Rate"],
                                   color="red", linewidth=1.5, linestyle="-", alpha=0.6, label="Labor Participation Rate")
    ax_zoom.set_ylabel("Employment & Participation (%)", fontsize=12)
    ax_zoom.set_title("Prime Age (25-54) Employment-Population Ratio (2018-Present)", fontsize=14)
    ax_zoom.xaxis.set_major_locator(mdates.YearLocator())
    ax_zoom.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax_zoom.tick_params(axis='x', rotation=45)
    ax_zoom.set_xlim(start_zoom, END_DATE)
    ax_zoom_unemp = ax_zoom.twinx()
    line_unemp_zoom, = ax_zoom_unemp.plot(unemp_zoom.index, unemp_zoom["Official Unemployment Rate"],
                                          color="black", linewidth=2, alpha=0.5, label="Calc. Unemployment Rate")
    ax_zoom_unemp.set_ylabel("Unemployment Rate", fontsize=12, color="black")
    ax_zoom_unemp.tick_params(axis='y', labelcolor="black")
    ax_zoom_unemp.set_ylim(0, 0.3)
    common_zoom = emp_zoom.dropna().index.intersection(part_zoom.dropna().index).intersection(unemp_zoom.dropna().index)
    if not common_zoom.empty:
        last_date_top = common_zoom.max()
        last_date_top_str = last_date_top.strftime("%b %Y")
    else:
        last_date_top = None
        last_date_top_str = "N/A"
    if last_date_top is not None:
        last_emp_value_top = emp_zoom.loc[last_date_top, "Employment Rate"]
        last_part_value_top = part_zoom.loc[last_date_top, "Participation Rate"]
        last_unemp_value_top = unemp_zoom.loc[last_date_top, "Official Unemployment Rate"]
        ax_zoom.plot(last_date_top, last_emp_value_top, marker='o', markersize=8, color="royalblue",
                     markeredgecolor="black")
        ax_zoom.plot(last_date_top, last_part_value_top, marker='o', markersize=8, color="red", markeredgecolor="black")
        ax_zoom_unemp.plot(last_date_top, last_unemp_value_top, marker='o', markersize=8, color="black",
                           markeredgecolor="black")
        ax_zoom.text(last_date_top, last_emp_value_top, f" {last_emp_value_top:.2f}", color="royalblue", fontsize=10,
                     va='center', ha='left')
        ax_zoom.text(last_date_top, last_part_value_top, f" {last_part_value_top:.2f}", color="red", fontsize=10,
                     va='center', ha='left')
        ax_zoom_unemp.text(last_date_top, last_unemp_value_top, f" {last_unemp_value_top:.2%}", color="black", fontsize=10,
                           va='center', ha='left')
        ax_zoom.text(0.95, 0.92, f"Latest: {last_date_top_str}", transform=ax_zoom.transAxes,
                     fontsize=12, ha='right', va='top', color="black")
    ax_zoom.legend(loc="upper left", fontsize=10)
    ax_zoom_unemp.legend(loc="upper right", fontsize=10)
    ax_full = axs[1]
    line_emp_full, = ax_full.plot(employment_rate.index, employment_rate["Employment Rate"],
                                  color="royalblue", linewidth=2, label="Employment-Population Ratio")
    line_part_full, = ax_full.plot(prime_participation_rate.index, prime_participation_rate["Participation Rate"],
                                   color="red", linewidth=1.5, linestyle="-", alpha=0.6, label="Labor Participation Rate")
    ax_full.set_xlabel("Date", fontsize=12)
    ax_full.set_ylabel("Employment & Participation (%)", fontsize=12)
    ax_full.set_title("Prime Age (25-54) Employment-Population Ratio (1980-2025)", fontsize=14)
    ax_full.xaxis.set_major_locator(mdates.YearLocator())
    ax_full.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax_full.tick_params(axis='x', rotation=45)
    ax_full_unemp = ax_full.twinx()
    line_unemp_full, = ax_full_unemp.plot(official_unemployment_rate.index,
                                          official_unemployment_rate["Official Unemployment Rate"],
                                          color="black", linewidth=2, alpha=0.5, label="Calc. Unemployment Rate")
    ax_full_unemp.set_ylabel("Unemployment Rate", fontsize=12, color="black")
    ax_full_unemp.tick_params(axis='y', labelcolor="black")
    ax_full_unemp.set_ylim(0, 0.3)
    common_full = employment_rate.dropna().index.intersection(prime_participation_rate.dropna().index).intersection(
        official_unemployment_rate.dropna().index)
    if not common_full.empty:
        last_date_full = common_full.max()
        last_date_full_str = last_date_full.strftime("%b %Y")
    else:
        last_date_full = None
        last_date_full_str = "N/A"
    if last_date_full is not None:
        last_emp_value_full = employment_rate.loc[last_date_full, "Employment Rate"]
        last_part_value_full = prime_participation_rate.loc[last_date_full, "Participation Rate"]
        last_unemp_value_full = official_unemployment_rate.loc[last_date_full, "Official Unemployment Rate"]
        ax_full.plot(last_date_full, last_emp_value_full, marker='o', markersize=8, color="royalblue",
                     markeredgecolor="black")
        ax_full.plot(last_date_full, last_part_value_full, marker='o', markersize=8, color="red", markeredgecolor="black")
        ax_full_unemp.plot(last_date_full, last_unemp_value_full, marker='o', markersize=8, color="black",
                           markeredgecolor="black")
        ax_full.text(last_date_full, last_emp_value_full, f" {last_emp_value_full:.2f}", color="royalblue", fontsize=10,
                     va='center', ha='left')
        ax_full.text(last_date_full, last_part_value_full, f" {last_part_value_full:.2f}", color="red", fontsize=10,
                     va='center', ha='left')
        ax_full_unemp.text(last_date_full, last_unemp_value_full, f" {last_unemp_value_full:.2%}", color="black",
                           fontsize=10, va='center', ha='left')
        ax_full.text(0.95, 0.92, f"Latest: {last_date_full_str}", transform=ax_full.transAxes,
                     fontsize=12, ha='right', va='top', color="black")
    ax_full.legend(loc="upper left", fontsize=10)
    ax_full_unemp.legend(loc="upper right", fontsize=10)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Prime Age Employment Rate"), bbox_inches='tight')
    plt.close(fig)


def chart_ahe_yoy_vs_u6():
    """AHEYoY vs U6 -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(1994, 1, 1)
    ahe_yoy2 = blp.bdh("USHEYOY Index", "PX_LAST", START_DATE, END_DATE)
    U6_unemployment = blp.bdh("USUDMAER Index", "PX_LAST", START_DATE, END_DATE)
    philips_curve_data = ahe_yoy2.merge(U6_unemployment, left_index=True, right_index=True, how='inner')
    philips_curve_data.columns = ['ahe', 'u6']
    philips_curve_data.index = pd.to_datetime(philips_curve_data.index)
    last_valid_date = philips_curve_data.dropna().last_valid_index()
    last_valid_str = last_valid_date.strftime("%b %Y") if last_valid_date is not None else "N/A"
    period1 = philips_curve_data.loc[pd.Timestamp('1994-01-01'):pd.Timestamp('2000-12-31')]
    period2 = philips_curve_data.loc[pd.Timestamp('2003-01-01'):pd.Timestamp('2007-12-31')]
    period3 = philips_curve_data.loc[pd.Timestamp('2010-01-01'):pd.Timestamp('2019-12-31')]
    period4 = philips_curve_data.loc[pd.Timestamp('2021-01-01'):]
    others = philips_curve_data.copy()
    others = others.drop(period1.index, errors='ignore')
    others = others.drop(period2.index, errors='ignore')
    others = others.drop(period3.index, errors='ignore')
    others = others.drop(period4.index, errors='ignore')
    fig = plt.figure(figsize=(10, 6))
    plt.scatter(others['ahe'], others['u6'], color='lightblue', label='Other', alpha=0.7)
    plt.scatter(period1['ahe'], period1['u6'], color='blue', label='1994-2000', alpha=0.8)
    plt.scatter(period2['ahe'], period2['u6'], color='orange', label='2003-2007', alpha=0.8)
    plt.scatter(period3['ahe'], period3['u6'], color='green', label='2010-2019', alpha=0.8)
    plt.scatter(period4['ahe'], period4['u6'], color='red', label='2021-till now', alpha=0.8)
    last_point = philips_curve_data.dropna().iloc[-1]
    plt.scatter(last_point['ahe'], last_point['u6'], s=100, facecolors='none', edgecolors='black', linewidth=3,
                label='Last')
    plt.xlabel("AHE YoY", fontsize=12)
    plt.ylabel("U6 Unemployment", fontsize=12)
    plt.title("Phillips Curve: AHE YoY% vs. U6 Unemployment", fontsize=14)
    plt.legend(loc="upper left", fontsize=10)
    plt.tight_layout()
    ax = plt.gca()
    ax.text(0.95, 0.95, f"Latest: {last_valid_str}", transform=ax.transAxes,
            fontsize=10, ha='right', va='top', color='black')
    ax.text(0.95, 0.98, "USHEYOY rather than AHE YOY% Index due to more data, trend similar", transform=ax.transAxes,
            fontsize=10, ha='right', va='top', color='black')
    plt.savefig(Path(G_CHART_DIR, "AHEYoY vs U6"), bbox_inches='tight')
    plt.close(fig)


def chart_fd_gdp_and_credit_growth():
    """FD_GDP_and_Credit_Growth -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)
    fiscal_deficit = blp.bdh("INFFFIDE Index", "PX_LAST", START_DATE, END_DATE)
    fiscal_deficit.index = pd.to_datetime(fiscal_deficit.index)
    india_nominal_gdp = blp.bdh("IGQNEGDP Index", "PX_LAST", START_DATE, END_DATE)
    india_nominal_gdp.index = pd.to_datetime(india_nominal_gdp.index)
    non_financial_sector_credit = blp.bdh("CPNFINCD Index", "PX_LAST", START_DATE, END_DATE)
    non_financial_sector_credit.index = pd.to_datetime(non_financial_sector_credit.index)
    non_financial_sector_credit_growth = (non_financial_sector_credit / non_financial_sector_credit.shift(3) - 1) * 100
    non_financial_sector_credit_growth.columns = ['CreditGrowth']
    india_nominal_gdp_monthly = india_nominal_gdp.resample("M").ffill(limit=3)
    fd_gdp_data = india_nominal_gdp_monthly.merge(fiscal_deficit, left_index=True, right_index=True, how='outer') \
        .ffill(limit=3).dropna()
    fd_gdp_data.columns = ['GDP', 'FD']
    fd_gdp_data['FD/GDP'] = (fd_gdp_data['FD'] / fd_gdp_data['GDP']).rolling(12).mean() * 100
    fd_gdp_data = fd_gdp_data.merge(non_financial_sector_credit_growth, left_index=True, right_index=True, how='left') \
        .ffill(limit=3)
    last_valid_date_fd = fd_gdp_data['FD/GDP'].dropna().last_valid_index()
    last_fdgdp_value = fd_gdp_data.loc[last_valid_date_fd, 'FD/GDP'] if last_valid_date_fd is not None else None
    last_valid_date_fd_str = last_valid_date_fd.strftime("%b %y") if last_valid_date_fd is not None else "N/A"
    last_valid_date_credit = fd_gdp_data['CreditGrowth'].dropna().last_valid_index()
    last_credit_value = fd_gdp_data.loc[
        last_valid_date_credit, 'CreditGrowth'] if last_valid_date_credit is not None else None
    last_valid_date_credit_str = last_valid_date_credit.strftime("%b %y") if last_valid_date_credit is not None else "N/A"
    fig, ax = plt.subplots(figsize=(12, 6))
    line_fdgdp, = ax.plot(fd_gdp_data.index, fd_gdp_data['FD/GDP'], color='blue', linewidth=2, label='FD/GDP (4Q MA)')
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Fiscal Deficit (%GDP)", color='blue', fontsize=12)
    ax.tick_params(axis='y', labelcolor='blue')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax2 = ax.twinx()
    line_credit, = ax2.plot(fd_gdp_data.index, fd_gdp_data['CreditGrowth'], color='red', linewidth=1.5,
                            label='Credit Growth')
    ax2.set_ylabel("Credit Growth (YoY%)", color='red', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='red')
    ax.text(0.95, 0.95, f"Latest FD/GDP: {last_valid_date_fd_str}", transform=ax.transAxes,
            fontsize=12, ha='right', va='top', color='blue')
    ax2.text(0.95, 0.85, f"Latest Credit: {last_valid_date_credit_str}", transform=ax2.transAxes,
             fontsize=12, ha='right', va='top', color='red')
    if last_valid_date_fd is not None:
        ax.plot(last_valid_date_fd, last_fdgdp_value, marker='o', markersize=8, color='blue', markeredgecolor='black')
        ax.text(last_valid_date_fd, last_fdgdp_value, f" {last_fdgdp_value:.2f} ({last_valid_date_fd_str})",
                color='blue', fontsize=10, va='center', ha='left')
    if last_valid_date_credit is not None:
        ax2.plot(last_valid_date_credit, last_credit_value, marker='o', markersize=8, color='red', markeredgecolor='black')
        ax2.text(last_valid_date_credit, last_credit_value, f" {last_credit_value:.2f} ({last_valid_date_credit_str})",
                 color='red', fontsize=10, va='center', ha='left')
    ax.set_title("Fiscal Deficit-to-GDP Ratio and Non-Financial Sector Credit Growth", fontsize=14)
    lines = [line_fdgdp, line_credit]
    labels = [line.get_label() for line in lines]
    ax.legend(lines, labels, loc="upper left", fontsize=10)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "FD_GDP_and_Credit_Growth"), bbox_inches='tight')
    plt.close(fig)

def chart_eps_growth_vs_gdp_wages_x_oper_margin():
    """EPSGrowth vs GDP-WagesXOperMargin (US, EU, UK, JP) and EPSGrowth vs GDP-WagesXOperMargin (AU, CN, IN, TW) -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(1990, 1, 1)
    indices_tickers = ['SPX Index', 'SXXP Index', 'UKX Index', 'TPX Index', 'ASX Index', 'SHSZ300 Index', 'NSE500 Index',
                       'TWSE Index']
    fields = ['TRAIL_12M_EPS', "OPER_MARGIN"]
    nom_gdp_tickers = ["GDP CURY Index", 'ENGK27Y Index', 'UKGRYBAQ Index', 'OEJPNGBK Index', 'AUGDPCY Index',
                       'OECNNGAE Index', 'INBGDNQY Index', 'ECOXTWS Index']
    wage_growth_tickers = ['COMPNFRY Index', 'LNTN27Y Index', 'UKAWYWHO Index', 'JNLSUCTL Index', 'AUWCBY Index',
                           'CHINWAG Index', 'INBGRIWG Index', 'TWMERY Index']
    nom_gdp_data = blp.bdh(nom_gdp_tickers, "PX_LAST", START_DATE, END_DATE)
    wage_growth_data = blp.bdh(wage_growth_tickers, "PX_LAST", START_DATE, END_DATE)
    indices_data = blp.bdh(indices_tickers, fields, START_DATE, END_DATE)
    margin_data = indices_data.xs("OPER_MARGIN", axis=1, level=1, drop_level=True)
    eps_data = indices_data.xs("TRAIL_12M_EPS", axis=1, level=1, drop_level=True)
    for df in [nom_gdp_data, wage_growth_data, margin_data, eps_data]:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
    wage_growth_data['CHINWAG Index'] = wage_growth_data['CHINWAG Index'].pct_change(periods=12) * 100
    wage_growth_data = wage_growth_data.ffill(limit=2)
    nom_gdp_data['OECNNGAE Index'] = nom_gdp_data['OECNNGAE Index'].pct_change(periods=4) * 100
    nom_gdp_data['ECOXTWS Index'] = nom_gdp_data['ECOXTWS Index'].pct_change(periods=4) * 100
    nom_gdp_data = nom_gdp_data.resample("M").ffill(limit=2)
    margin_data.loc[(margin_data.index > "2003-11-05") & (margin_data.index < "2003-12-18"), 'TPX Index'] = np.nan
    eps_data.loc[(eps_data.index > "2003-10-30") & (eps_data.index < "2003-12-18"), 'TPX Index'] = np.nan
    eps_data.loc[(eps_data.index > "2005-05-15") & (eps_data.index < "2005-05-20"), 'TPX Index'] = np.nan
    eps_data.loc[(eps_data.index > "2005-10-16") & (eps_data.index < "2005-11-03"), 'SPX Index'] = np.nan
    margin_data = margin_data.ffill(limit=21).resample("M").last()
    eps_data = eps_data.ffill(limit=21)
    eps_data_monthly = eps_data.resample("M").last()
    eps_growth = eps_data_monthly.pct_change(periods=12) * 100
    mapping = {
        'SPX Index': {'gdp': 'GDP CURY Index', 'wage': 'COMPNFRY Index', 'country_name': 'US'},
        'SXXP Index': {'gdp': 'ENGK27Y Index', 'wage': 'LNTN27Y Index', 'country_name': 'EU'},
        'UKX Index': {'gdp': 'UKGRYBAQ Index', 'wage': 'UKAWYWHO Index', 'country_name': 'UK'},
        'TPX Index': {'gdp': 'OEJPNGBK Index', 'wage': 'JNLSUCTL Index', 'country_name': 'JP'},
        'ASX Index': {'gdp': 'AUGDPCY Index', 'wage': 'AUWCBY Index', 'country_name': 'AU'},
        'SHSZ300 Index': {'gdp': 'OECNNGAE Index', 'wage': 'CHINWAG Index', 'country_name': 'CN'},
        'NSE500 Index': {'gdp': 'INBGDNQY Index', 'wage': 'INBGRIWG Index', 'country_name': 'IN'},
        'TWSE Index': {'gdp': 'ECOXTWS Index', 'wage': 'TWMERY Index', 'country_name': 'TW'}
    }
    set1 = ['SPX Index', 'SXXP Index', 'UKX Index', 'TPX Index']
    set2 = ['ASX Index', 'SHSZ300 Index', 'NSE500 Index', 'TWSE Index']

    def _plot_country_chart_local(country_list, mapping_dict, eps_gr, ngdp_data, wg_data, marg_data, countries_label):
        fig, axes = plt.subplots(nrows=4, ncols=2, figsize=(15, 20))
        axes = axes.reshape(4, 2)
        for i, country in enumerate(country_list):
            tickers_info = mapping_dict[country]
            country_name = tickers_info.get('country_name', '')
            df_eps = eps_gr[country].dropna().to_frame(name='EPS Growth')
            df_comp = pd.concat([
                ngdp_data[tickers_info['gdp']].rename('GDP Growth'),
                wg_data[tickers_info['wage']].rename('Wage Growth'),
                marg_data[country].rename('Margin')
            ], axis=1).dropna()
            df_comp['Composite'] = (df_comp['GDP Growth'] - df_comp['Wage Growth']) * df_comp['Margin']
            last_eps_date = df_eps.index[-1] if not df_eps.empty else None
            last_comp_date = df_comp.index[-1] if not df_comp.empty else None
            annotation_text = (f"Last EPS: {last_eps_date.strftime('%Y-%m') if last_eps_date else 'N/A'}\n"
                               f"Last Composite: {last_comp_date.strftime('%Y-%m') if last_comp_date else 'N/A'}")
            ax_left = axes[i, 0]
            ax_left.plot(df_eps.index, df_eps['EPS Growth'], label='EPS Growth YoY%', linestyle='-')
            ax_left.plot(df_comp.index, df_comp['Composite'], label='(GDP YoY% - Wages YoY%) x Oper.Margin(pct)',
                         color='orange', marker='.', linestyle='--')
            ax_left.set_title(f"{country} ({country_name}) - Full Series")
            ax_left.set_xlabel("Date")
            ax_left.set_ylabel("%")
            lines1, labels1 = ax_left.get_legend_handles_labels()
            ax_left.legend(lines1, labels1, loc='upper left')
            ax_left.grid(True)
            ax_left.text(0.72, 0.95, annotation_text, transform=ax_left.transAxes, fontsize=9,
                         verticalalignment='top', bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.4))
            df_eps_recent = df_eps.loc[df_eps.index >= pd.to_datetime("2016-01-01")]
            df_comp_recent = df_comp.loc[df_comp.index >= pd.to_datetime("2016-01-01")]
            recent_last_eps = df_eps_recent.index[-1] if not df_eps_recent.empty else None
            recent_last_comp = df_comp_recent.index[-1] if not df_comp_recent.empty else None
            annotation_text_recent = (f"Last EPS: {recent_last_eps.strftime('%Y-%m') if recent_last_eps else 'N/A'}\n"
                                      f"Last Composite: {recent_last_comp.strftime('%Y-%m') if recent_last_comp else 'N/A'}")
            ax_right = axes[i, 1]
            ax_right.plot(df_eps_recent.index, df_eps_recent['EPS Growth'], label='EPS Growth YoY%', linestyle='-')
            ax_right.plot(df_comp_recent.index, df_comp_recent['Composite'],
                          label='(GDP YoY% - Wages YoY%) x Oper.Margin(pct)', color='orange', marker='.', linestyle='-')
            ax_right.set_title(f"{country} ({country_name}) - From 2016")
            ax_right.set_xlabel("Date")
            ax_right.set_ylabel("%")
            lines1, labels1 = ax_right.get_legend_handles_labels()
            ax_right.legend(lines1, labels1, loc='upper left')
            ax_right.grid(True)
            ax_right.text(0.72, 0.95, annotation_text_recent, transform=ax_right.transAxes, fontsize=9,
                         verticalalignment='top', bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.4))
        plt.tight_layout()
        plt.savefig(Path(G_CHART_DIR, f"EPSGrowth vs GDP-WagesXOperMargin ({countries_label})"), bbox_inches='tight')
        plt.close(fig)

    _plot_country_chart_local(set1, mapping, eps_growth, nom_gdp_data, wage_growth_data, margin_data, "US, EU, UK, JP")
    _plot_country_chart_local(set2, mapping, eps_growth, nom_gdp_data, wage_growth_data, margin_data, "AU, CN, IN, TW")


def chart_spx_risk_premium():
    """SPX Risk Premium -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(1980, 1, 1)
    spx_index = blp.bdh("SPX Index", "PX_LAST", START_DATE, END_DATE)
    spx_eps = blp.bdh("SPX Index", "BEST_EPS", START_DATE, END_DATE)
    ty_yield = blp.bdh("USGG10YR Index", "PX_LAST", START_DATE, END_DATE)
    spx_index = spx_index.merge(spx_eps, left_index=True, right_index=True, how='inner')
    spx_index = spx_index.merge(ty_yield, left_index=True, right_index=True, how='inner')
    spx_index.columns = ['spx', 'eps', 'ty']
    spx_index['ty'] = spx_index['ty'] / 100
    spx_index['yield'] = spx_index['eps'] / spx_index['spx']
    spx_index['riskpremium'] = (spx_index['yield'] - spx_index['ty']) * 100
    last_valid_date = spx_index['riskpremium'].dropna().last_valid_index()
    last_rp_value = spx_index.loc[last_valid_date, 'riskpremium']
    fig, ax = plt.subplots(figsize=(12, 6))
    line_rp, = ax.plot(spx_index.index, spx_index['riskpremium'], color='red', label='Risk Premium')
    ax.axhline(0, color='black', linestyle='--')
    ax.set_title("SPX Risk Premium (Fwd Earnings Yield - US10Y Yield)", fontsize=14)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Risk Premium (%)", fontsize=12)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax.text(0.95, 0.95, f"Latest: {last_valid_date.strftime('%d %b %Y')}",
            transform=ax.transAxes, fontsize=12, ha='right', va='top', color='black')
    ax.plot(last_valid_date, last_rp_value, marker='o', markersize=8, markerfacecolor='white',
            markeredgecolor='red', linewidth=0)
    ax.text(last_valid_date, last_rp_value, f" {last_rp_value:.2f}", color='red',
            fontsize=10, va='center')
    ax.legend(loc="upper left", fontsize=10)
    plt.savefig(Path(G_CHART_DIR, "SPX Risk Premium"), bbox_inches='tight')
    plt.close(fig)


def chart_msciworld_yields_corr():
    """MSCIWorld Yields Corr -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(1995, 1, 1)
    yields_msci_tickers = ['USGG10YR Index', 'GTDEM10Y Govt', 'GTJPY10Y Govt', 'USGG2YR Index', 'GTDEM2Y Govt',
                           'GTJPY2Y Govt', 'MXWO Index']
    yields_msci_data = blp.bdh(yields_msci_tickers, "PX_LAST", START_DATE, END_DATE)
    yields_msci_data.columns = yields_msci_data.columns.get_level_values(0)
    yields_msci_data.index = pd.to_datetime(yields_msci_data.index)
    yields_msci_data = yields_msci_data.dropna()
    yields_msci_data['sum 10y yields'] = yields_msci_data['USGG10YR Index'] + yields_msci_data['GTDEM10Y Govt'] + \
                                         yields_msci_data['GTJPY10Y Govt']
    yields_msci_data['sum 2y yields'] = yields_msci_data['USGG2YR Index'] + yields_msci_data['GTDEM2Y Govt'] + \
                                        yields_msci_data['GTJPY2Y Govt']
    yields_msci_data['US2s10s'] = yields_msci_data['USGG10YR Index'] - yields_msci_data['USGG2YR Index']
    yields_msci_weekly = yields_msci_data.resample('W').last()
    yields_msci_weekly['sum_10y_change'] = yields_msci_weekly['sum 10y yields'].diff()
    yields_msci_weekly['sum_2y_change'] = yields_msci_weekly['sum 2y yields'].diff()
    yields_msci_weekly['US2s10s_change'] = yields_msci_weekly['US2s10s'].diff()
    yields_msci_weekly['MXWO_pct_change'] = yields_msci_weekly['MXWO Index'].pct_change()
    yields_msci_weekly = yields_msci_weekly.dropna()
    orig_index = yields_msci_weekly.index
    yields_msci_weekly = yields_msci_weekly.reset_index(drop=True)
    yields_msci_weekly['rolling 6m corr (2y rates, equities)'] = yields_msci_weekly['sum_2y_change'].rolling(window=26,
                                                                                                             min_periods=22).corr(
        yields_msci_weekly['MXWO_pct_change'])
    yields_msci_weekly['rolling 12m corr (2y rates, equities)'] = yields_msci_weekly['sum_2y_change'].rolling(window=52,
                                                                                                              min_periods=48).corr(
        yields_msci_weekly['MXWO_pct_change'])
    yields_msci_weekly['rolling 2y corr (2y rates, equities)'] = yields_msci_weekly['sum_2y_change'].rolling(window=104,
                                                                                                             min_periods=100).corr(
        yields_msci_weekly['MXWO_pct_change'])
    yields_msci_weekly['rolling 6m corr (10y rates, equities)'] = yields_msci_weekly['sum_10y_change'].rolling(window=26,
                                                                                                               min_periods=22).corr(
        yields_msci_weekly['MXWO_pct_change'])
    yields_msci_weekly['rolling 12m corr (10y rates, equities)'] = yields_msci_weekly['sum_10y_change'].rolling(window=52,
                                                                                                                min_periods=48).corr(
        yields_msci_weekly['MXWO_pct_change'])
    yields_msci_weekly['rolling 2y corr (10y rates, equities)'] = yields_msci_weekly['sum_10y_change'].rolling(window=104,
                                                                                                               min_periods=100).corr(
        yields_msci_weekly['MXWO_pct_change'])
    yields_msci_weekly = yields_msci_weekly.set_index(orig_index)
    fig, axs = plt.subplots(3, 3, figsize=(15, 12))
    underlying_rows = ['sum 2y yields', 'sum 10y yields', 'US2s10s']
    corr_rows = [
        ['rolling 6m corr (2y rates, equities)', 'rolling 12m corr (2y rates, equities)',
         'rolling 2y corr (2y rates, equities)'],
        ['rolling 6m corr (10y rates, equities)', 'rolling 12m corr (10y rates, equities)',
         'rolling 2y corr (10y rates, equities)'],
        ['rolling 6m corr (10y rates, equities)', 'rolling 12m corr (10y rates, equities)',
         'rolling 2y corr (10y rates, equities)']]
    all_last_dates = []
    for i in range(3):
        for j in range(3):
            if j == 0:
                data = yields_msci_weekly[yields_msci_weekly.index >= pd.Timestamp('2015-01-01')]
            else:
                data = yields_msci_weekly
            ax = axs[i, j]
            corr_col = corr_rows[i][j]
            underlying_col = underlying_rows[i]
            l1, = ax.plot(data[corr_col], label=f'{corr_col}(LHS)', color='blue')
            ax.axhline(y=0, color='black', linestyle='--')
            ax2 = ax.twinx()
            l2, = ax2.plot(data[underlying_col], label=f'{underlying_col}(RHS)', color='red')
            lines = [l1, l2]
            labels = [l.get_label() for l in lines]
            ax.legend(lines, labels, loc='lower left')
            valid = data[[corr_col, underlying_col]].dropna()
            if not valid.empty:
                r = valid[corr_col].corr(valid[underlying_col])
                ax.text(0.73, 0.96, f"corr = {r:.3f}", transform=ax.transAxes, fontsize=10,
                        verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.3))
                all_last_dates.append(valid.index.max())
            ax.set_title(f"{corr_col} vs {underlying_col}")
    title_text = "Rolling 6M/12M/2Y corr b/w MSCI World and sum of (US, Japan, German) 2Y,10Y yields vs. sum of yields, US 2s10s"
    plt.suptitle(title_text, fontsize=18, y=0.98)
    subtitle_text = ("rolling x mths corr of MSCI World and sum of (US,DE,JP) yields plotted against sum of yields/us2s10s."
                     " 'corr' on chart = corr of these two series")
    fig.text(0.5, 0.93, subtitle_text, ha='center', fontsize=12)
    if all_last_dates:
        overall_last_date = min(all_last_dates)
        fig.text(0.98, 0.915, f"Last valid data: {overall_last_date.date()}", ha='right', va='top',
                 fontsize=12, bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(Path(G_CHART_DIR, "MSCIWorld Yields Corr"), bbox_inches='tight')
    plt.close(fig)


def chart_comm_bank_sec_hldgs_vs_dxy():
    """CommBankSecHldgs vs DXY -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(1995, 1, 1)
    shift_window = 52
    comm_bank_sec_data = blp.bdh('ALBNSCBC Index', "PX_LAST", START_DATE, END_DATE)
    comm_bank_sec_data.columns = comm_bank_sec_data.columns.get_level_values(0)
    comm_bank_sec_data.index = pd.to_datetime(comm_bank_sec_data.index)
    gdp_data = blp.bdh('GDP CUR$ Index', "PX_LAST", START_DATE, END_DATE)
    gdp_data.columns = gdp_data.columns.get_level_values(0)
    gdp_data.index = pd.to_datetime(gdp_data.index)
    dxy_data = blp.bdh('DXY Index', "PX_LAST", START_DATE, END_DATE)
    dxy_data.columns = dxy_data.columns.get_level_values(0)
    dxy_data.index = pd.to_datetime(dxy_data.index)
    merged = pd.merge_asof(comm_bank_sec_data, gdp_data, left_index=True, right_index=True, direction='backward')
    merged['Comm_Sec_PctGdp'] = merged['ALBNSCBC Index'] / merged['GDP CUR$ Index'] * 100
    dxy_data['YoY Change'] = dxy_data['DXY Index'].pct_change(252) * 100
    merged['YoY Change Holdings'] = (merged['Comm_Sec_PctGdp'].pct_change(52) * 100)
    last_valid_dxy = dxy_data['YoY Change'].last_valid_index()
    last_valid_holdings = merged['YoY Change Holdings'].last_valid_index()
    freq = pd.infer_freq(merged.index)
    if freq is None:
        freq = 'W'
    new_end = merged.index.max() + pd.DateOffset(weeks=shift_window)
    new_index = pd.date_range(start=merged.index.min(), end=new_end, freq=freq)
    merged_extended = merged.reindex(new_index)
    merged_extended['YoY Change Holdings Lead'] = merged_extended['YoY Change Holdings'].shift(shift_window)
    fig = plt.figure(figsize=(12, 6))
    plt.plot(dxy_data.index, dxy_data['YoY Change'], label='DXY YoY Change (%)', color='blue')
    plt.plot(merged_extended.index, merged_extended['YoY Change Holdings Lead'],
             label=f'Holdings % of GDP YoY Change (%) (Lead {shift_window} Weeks)', color='red')
    plt.title(f'YoY Change: Comm Banks Securities Hldgs % of GDP (Lead {shift_window} Weeks) vs DXY Index')
    plt.xlabel('Date')
    plt.ylabel('YoY Change (%)')
    plt.legend()
    plt.axhline(y=0, color='black')
    plt.text(0.98, 0.98,
             f"Last DXY date: {last_valid_dxy.date()}\nLast Hldgs date: {last_valid_holdings.date()}",
             transform=plt.gca().transAxes, ha='right', va='top', fontsize=9,
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))
    plt.savefig(Path(G_CHART_DIR, "CommBankSecHldgs vs DXY"), bbox_inches='tight')
    plt.close(fig)


def chart_emdm_composite_currliab_fwdsales_ratio():
    """EMDM Composite CurrLiab-FwdSales Ratio -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2008, 1, 1)
    em_country_indices = ['SHSZ300 Index', 'NSE500 Index', 'IBOV Index', 'KOSPI Index', 'TWSE Index', 'MEXBOL Index',
                          'SET Index', 'SASEIDX Index']
    dm_country_indices = ['SPX Index', 'RTY Index', 'UKX Index', 'SX5E Index', 'TPX Index', 'ASX Index']
    fields = ['PX_LAST', 'BEST_SALES', 'BS_CUR_LIAB', 'CUR_MKT_CAP']
    em_country_name_map = {'SHSZ300 Index': 'China', 'NSE500 Index': 'India', 'IBOV Index': 'Brazil',
                           'KOSPI Index': 'South Korea', 'TWSE Index': 'Taiwan', 'MEXBOL Index': 'Mexico',
                           'SET Index': 'Thailand', 'SASEIDX Index': 'South Africa'}
    em_currencies = ['USDCNY Curncy', 'USDINR Curncy', 'USDBRL Curncy', 'USDKRW Curncy', 'USDTWD Curncy', 'USDMXN Curncy',
                     'USDTHB Curncy', 'USDSAR Curncy']
    em_currency_map = {'SHSZ300 Index': 'USDCNY Curncy', 'NSE500 Index': 'USDINR Curncy', 'IBOV Index': 'USDBRL Curncy',
                       'KOSPI Index': 'USDKRW Curncy', 'TWSE Index': 'USDTWD Curncy', 'MEXBOL Index': 'USDMXN Curncy',
                       'SET Index': 'USDTHB Curncy', 'SASEIDX Index': 'USDSAR Curncy'}
    em_sales_liab_data = blp.bdh(em_country_indices, fields, START_DATE, END_DATE)
    em_sales_liab_data.index = pd.to_datetime(em_sales_liab_data.index)
    em_sales_liab_data = em_sales_liab_data.resample('M').mean()
    em_currency_data = blp.bdh(em_currencies, 'PX_LAST', START_DATE, END_DATE)
    em_currency_data.index = pd.to_datetime(em_currency_data.index)
    em_currency_data = em_currency_data.resample('M').mean()
    dm_country_name_map = {'SPX Index': 'US', 'NDX Index': 'US', 'RTY Index': 'US', 'UKX Index': 'UK', 'SX5E Index': 'EU',
                           'SHSZ300 Index': 'CN', 'TPX Index': 'JP', 'ASX Index': 'AU'}
    dm_sales_liab_data = blp.bdh(dm_country_indices, fields, START_DATE, END_DATE)
    dm_sales_liab_data.index = pd.to_datetime(dm_sales_liab_data.index)
    dm_sales_liab_data = dm_sales_liab_data.resample('M').mean()
    dm_currencies = ['GBPUSD Curncy', 'EURUSD Curncy', 'USDJPY Curncy', 'AUDUSD Curncy']
    dm_currency_data = blp.bdh(dm_currencies, 'PX_LAST', START_DATE, END_DATE)
    dm_currency_data.index = pd.to_datetime(dm_currency_data.index)
    dm_currency_data = dm_currency_data.resample('M').mean()
    dm_currency_map = {'SPX Index': 1, 'RTY Index': 1, 'UKX Index': 'GBPUSD Curncy', 'SX5E Index': 'EURUSD Curncy',
                       'TPX Index': 'USDJPY Curncy', 'ASX Index': 'AUDUSD Curncy'}
    for market in em_country_indices:
        cur_ticker = em_currency_map[market]
        cs = em_currency_data[cur_ticker]
        if isinstance(cs, pd.DataFrame):
            cs = cs.iloc[:, 0]
        cs = cs.reindex(em_sales_liab_data.index).ffill(limit=1)
        em_sales_liab_data[(market, 'CUR_MKT_CAP_USD')] = em_sales_liab_data[(market, 'CUR_MKT_CAP')] / cs
        em_sales_liab_data[(market, 'payable/sales_ratio')] = em_sales_liab_data[(market, 'BS_CUR_LIAB')] / \
                                                              em_sales_liab_data[(market, 'BEST_SALES')]
        em_sales_liab_data[(market, 'payable/sales_ratio')] = em_sales_liab_data[(market, 'payable/sales_ratio')].rolling(
            12).mean()
    em_sales_liab_data = em_sales_liab_data.iloc[11:, :]
    em_rebased_ratio = {}
    for market in dm_country_indices:
        cur_ticker = dm_currency_map[market]
        if cur_ticker == 1:
            cs = pd.Series(1, index=dm_sales_liab_data.index)
        else:
            cs = dm_currency_data[cur_ticker]
            if isinstance(cs, pd.DataFrame):
                cs = cs.iloc[:, 0]
            cs = cs.reindex(dm_sales_liab_data.index).ffill(limit=1)
            if cur_ticker in ['GBPUSD Curncy', 'EURUSD Curncy', 'AUDUSD Curncy']:
                cs = 1 / cs
        dm_sales_liab_data[(market, 'CUR_MKT_CAP_USD')] = dm_sales_liab_data[(market, 'CUR_MKT_CAP')] / cs
        dm_sales_liab_data[(market, 'payable/sales_ratio')] = dm_sales_liab_data[(market, 'BS_CUR_LIAB')] / \
                                                              dm_sales_liab_data[(market, 'BEST_SALES')]
        dm_sales_liab_data[(market, 'payable/sales_ratio')] = dm_sales_liab_data[(market, 'payable/sales_ratio')].rolling(
            12).mean()
    dm_sales_liab_data = dm_sales_liab_data.iloc[11:, :]
    dm_rebased_ratio = {}
    for market in em_country_indices:
        first_ratio = em_sales_liab_data[(market, 'payable/sales_ratio')].iloc[0]
        em_rebased_ratio[market] = em_sales_liab_data[(market, 'payable/sales_ratio')] / first_ratio * 100
    em_weights_df = pd.DataFrame({market: em_sales_liab_data[(market, 'CUR_MKT_CAP_USD')] for market in em_country_indices},
                                 index=em_sales_liab_data.index)
    total_cap = em_weights_df.sum(axis=1)
    em_weights_df = em_weights_df.divide(total_cap, axis=0)
    em_composite_ratio = pd.Series(index=em_sales_liab_data.index, dtype=float)
    em_composite_ratio.iloc[0] = 100.0
    for i in range(1, len(em_sales_liab_data.index)):
        curr_date = em_sales_liab_data.index[i]
        prev_date = em_sales_liab_data.index[i - 1]
        comp_ratio_return = 0.0
        for market in em_country_indices:
            pct_ratio_change = (em_rebased_ratio[market].loc[curr_date] / em_rebased_ratio[market].loc[prev_date]) - 1
            weight = em_weights_df.loc[curr_date, market]
            comp_ratio_return += weight * pct_ratio_change
        em_composite_ratio.loc[curr_date] = em_composite_ratio.loc[prev_date] * (1 + comp_ratio_return)
    for market in dm_country_indices:
        first_ratio = dm_sales_liab_data[(market, 'payable/sales_ratio')].iloc[0]
        dm_rebased_ratio[market] = dm_sales_liab_data[(market, 'payable/sales_ratio')] / first_ratio * 100
    dm_weights_df = pd.DataFrame({market: dm_sales_liab_data[(market, 'CUR_MKT_CAP_USD')] for market in dm_country_indices},
                                 index=dm_sales_liab_data.index)
    total_cap = dm_weights_df.sum(axis=1)
    dm_weights_df = dm_weights_df.divide(total_cap, axis=0)
    dm_composite_ratio = pd.Series(index=dm_sales_liab_data.index, dtype=float)
    dm_composite_ratio.iloc[0] = 100.0
    for i in range(1, len(dm_sales_liab_data.index)):
        curr_date = dm_sales_liab_data.index[i]
        prev_date = dm_sales_liab_data.index[i - 1]
        comp_ratio_return = 0.0
        for market in dm_country_indices:
            pct_ratio_change = (dm_rebased_ratio[market].loc[curr_date] / dm_rebased_ratio[market].loc[prev_date]) - 1
            weight = dm_weights_df.loc[curr_date, market]
            comp_ratio_return += weight * pct_ratio_change
        dm_composite_ratio.loc[curr_date] = dm_composite_ratio.loc[prev_date] * (1 + comp_ratio_return)
    fig, axs = plt.subplots(3, 1, figsize=(8, 24))
    plt.subplots_adjust(wspace=0.4, hspace=0.3)
    axs = axs.flatten()
    ax = axs[0]
    line1, = ax.plot(em_composite_ratio.index, em_composite_ratio, color='red', label='EM Composite Ratio (Rebased)')
    line2, = ax.plot(dm_composite_ratio.index, dm_composite_ratio, color='blue', label='DM Composite Ratio (Rebased)')
    ax.set_title("Composite DM/EM CurrLiab to FwdSales Ratio", fontsize=12)
    ax.set_ylabel("Composite Ratio (Rebased)", color='blue')
    ax.tick_params(axis='y', colors='blue')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.tick_params(axis='x', rotation=45)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, linestyle='--', alpha=0.7)
    if em_composite_ratio.last_valid_index() is not None:
        em_latest_comp = em_composite_ratio.last_valid_index().strftime('%b %Y')
        ax.text(0.78, 0.94, f"Latest EM: {em_latest_comp}", transform=ax.transAxes, fontsize=10, color='black')
    if dm_composite_ratio.last_valid_index() is not None:
        dm_latest_comp = dm_composite_ratio.last_valid_index().strftime('%b %Y')
        ax.text(0.78, 0.97, f"Latest DM: {dm_latest_comp}", transform=ax.transAxes, fontsize=10, color='black')
    ax = axs[1]
    for j, market in enumerate(dm_country_indices):
        line, = ax.plot(dm_sales_liab_data.index,
                        dm_sales_liab_data[(market, 'payable/sales_ratio')],
                        label=f"{market} ({dm_country_name_map[market]})")
    ax.set_title("DM: CurrLiab to FwdSales Ratio", fontsize=12)
    ax.set_ylabel("Ratio", color='blue')
    ax.tick_params(axis='y', colors='blue')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.tick_params(axis='x', rotation=45)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, linestyle='--', alpha=0.7)
    for j, market in enumerate(dm_country_indices):
        latest_market = dm_sales_liab_data[(market, 'PX_LAST')].dropna().last_valid_index().strftime('%b %Y')
        ax.text(0.85, 0.98 - j * 0.02, f"{market}: {latest_market}", transform=ax.transAxes, fontsize=6, color='black')
    ax = axs[2]
    for j, market in enumerate(em_country_indices):
        line, = ax.plot(em_sales_liab_data.index,
                        em_sales_liab_data[(market, 'payable/sales_ratio')],
                        label=f"{market} ({em_country_name_map[market]})")
    ax.set_title("EM: CurrLiab to FwdSales Ratio", fontsize=12)
    ax.set_ylabel("Ratio", color='blue')
    ax.tick_params(axis='y', colors='blue')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.tick_params(axis='x', rotation=45)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, linestyle='--', alpha=0.7)
    for j, market in enumerate(em_country_indices):
        latest_market = em_sales_liab_data[(market, 'PX_LAST')].dropna().last_valid_index().strftime('%b %Y')
        ax.text(0.83, 0.98 - j * 0.02, f"{market}: {latest_market}", transform=ax.transAxes, fontsize=6, color='black')
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "EMDM Composite CurrLiab-FwdSales Ratio"), bbox_inches='tight')
    plt.close(fig)

def chart_cn_banks_claims_non_fin():
    """CN Banks Claims on Non-Fin Sector to Non-Fin Deposits Ratio -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2006, 1, 1)
    ch_nonfinancial_data = blp.bdh(["CHFANFS Index", "CHDLDIBM Index"], "PX_LAST", START_DATE, END_DATE)
    ch_nonfinancial_data.columns = ['claims', 'deposits']
    ch_nonfinancial_data['nonfinLDR'] = ch_nonfinancial_data['claims'] / ch_nonfinancial_data['deposits']
    last_valid_date = ch_nonfinancial_data['nonfinLDR'].dropna().last_valid_index()
    last_value = ch_nonfinancial_data.loc[last_valid_date, 'nonfinLDR']
    fig, ax = plt.subplots(figsize=(12, 6))
    line, = ax.plot(ch_nonfinancial_data.index, ch_nonfinancial_data['nonfinLDR'], color='red',
                    label='Claims on Non-Financial Sector-to-Deposits Ratio')
    ax.set_title("CN: Bank Claims on Non-Fin Sector to Non-Fin Deposits Ratio", fontsize=14)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Loan-Deposit Ratio", fontsize=12)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax.text(0.98, 0.97, f"Latest: {last_valid_date.strftime('%b %Y')}",
            transform=ax.transAxes, fontsize=12, ha='right', va='top', color='black')
    ax.plot(last_valid_date, last_value, marker='o', markersize=6, markerfacecolor='white',
            markeredgecolor='red', linewidth=0)
    ax.text(last_valid_date, last_value, f" {last_value:.3f}", color='red',
            fontsize=10, va='center')
    ax.legend(loc="upper left", fontsize=10)
    plt.savefig(Path(G_CHART_DIR, "CN Banks Claims on Non-Fin Sector to Non-Fin Deposits Ratio"), bbox_inches='tight')
    plt.close(fig)


def chart_useujp_real_rates_diff_vs_fx():
    """USEUJP_RealRatesDiff_vs_FX.png -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2005, 1, 1)
    GRAPH_START_DATE = '2022-01-01'
    tickers = ['USGG10YR Index', 'GTDEM10Y Govt', 'GTJPY10Y Govt', 'USGG2YR Index', 'GTDEM2Y Govt', 'GTJPY2Y Govt',
               'USSWIT2 Curncy', 'USSWIT10 Curncy', 'GRSWIT2 Curncy', 'GRSWIT10 Curncy', 'JYSWIT2 BLC Index',
               'JYSWIT10 BLC Index',
               'EURUSD Curncy', 'USDJPY Curncy']
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    for df in [all_data]:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
    all_data['real_2y_us'] = all_data['USGG2YR Index'] - all_data['USSWIT2 Curncy']
    all_data['real_10y_us'] = all_data['USGG10YR Index'] - all_data['USSWIT10 Curncy']
    all_data['real_2y_de'] = all_data['GTDEM2Y Govt'] - all_data['GRSWIT2 Curncy']
    all_data['real_10y_de'] = all_data['GTDEM10Y Govt'] - all_data['GRSWIT10 Curncy']
    all_data['eurusd_2y_sprd'] = all_data['real_2y_de'] - all_data['real_2y_us']
    all_data['eurusd_10y_sprd'] = all_data['real_10y_de'] - all_data['real_10y_us']
    all_data['real_2y_jp'] = all_data['GTDEM2Y Govt'] - all_data['JYSWIT2 BLC Index']
    all_data['real_10y_jp'] = all_data['GTDEM10Y Govt'] - all_data['JYSWIT10 BLC Index']
    all_data['usdjpy_2y_sprd'] = all_data['real_2y_us'] - all_data['real_2y_jp']
    all_data['usdjpy_10y_sprd'] = all_data['real_10y_us'] - all_data['real_10y_jp']
    near_data = all_data.loc[all_data.index > GRAPH_START_DATE]
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    # Top left: EURUSD 2Y Spread vs. EURUSD Curncy
    ax = axes[0, 0]
    ax.plot(near_data.index, near_data['eurusd_2y_sprd'], color='red', label='EURUSD 2Y Spread')
    last_valid_index = near_data['eurusd_2y_sprd'].dropna().index[-1]
    last_value = near_data['eurusd_2y_sprd'].dropna().iloc[-1]
    ax.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='red')
    ax.set_ylabel("2Y Spread", color='red')
    ax.tick_params(axis='y', labelcolor='red')
    ax.set_title("EURUSD 2Y Spread vs. EURUSD Curncy")
    ax2 = ax.twinx()
    ax2.plot(near_data.index, near_data['EURUSD Curncy'], color='black', label='EURUSD Curncy')
    last_valid_index = near_data['EURUSD Curncy'].dropna().index[-1]
    last_value = near_data['EURUSD Curncy'].dropna().iloc[-1]
    ax2.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='black')
    ax2.set_ylabel("EURUSD Curncy", color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(handles1 + handles2, labels1 + labels2, loc='upper left', fontsize='small')
    common_dates = near_data['eurusd_2y_sprd'].dropna().index.intersection(near_data['EURUSD Curncy'].dropna().index)
    if not common_dates.empty:
        last_valid_date = common_dates.max().strftime('%Y-%m-%d')
        ax.text(0.98, 0.98, f"Last: {last_valid_date}", transform=ax.transAxes,
                ha='right', va='top', fontsize='x-small')
    # Top right: EURUSD 10Y Spread vs. EURUSD Curncy
    ax = axes[0, 1]
    ax.plot(near_data.index, near_data['eurusd_10y_sprd'], color='red', label='EURUSD 10Y Spread')
    last_valid_index = near_data['eurusd_10y_sprd'].dropna().index[-1]
    last_value = near_data['eurusd_10y_sprd'].dropna().iloc[-1]
    ax.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='red')
    ax.set_ylabel("10Y Spread", color='red')
    ax.tick_params(axis='y', labelcolor='red')
    ax.set_title("EURUSD 10Y Spread vs. EURUSD Curncy")
    ax2 = ax.twinx()
    ax2.plot(near_data.index, near_data['EURUSD Curncy'], color='black', label='EURUSD Curncy')
    last_valid_index = near_data['EURUSD Curncy'].dropna().index[-1]
    last_value = near_data['EURUSD Curncy'].dropna().iloc[-1]
    ax2.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='black')
    ax2.set_ylabel("EURUSD Curncy", color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(handles1 + handles2, labels1 + labels2, loc='upper left', fontsize='small')
    common_dates = near_data['eurusd_10y_sprd'].dropna().index.intersection(near_data['EURUSD Curncy'].dropna().index)
    if not common_dates.empty:
        last_valid_date = common_dates.max().strftime('%Y-%m-%d')
        ax.text(0.98, 0.98, f"Last: {last_valid_date}", transform=ax.transAxes,
                ha='right', va='top', fontsize='x-small')
    # Bottom left: USDJPY 2Y Spread vs. USDJPY Curncy
    ax = axes[1, 0]
    ax.plot(near_data.index, near_data['usdjpy_2y_sprd'], color='red', label='USDJPY 2Y Spread')
    last_valid_index = near_data['usdjpy_2y_sprd'].dropna().index[-1]
    last_value = near_data['usdjpy_2y_sprd'].dropna().iloc[-1]
    ax.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='red')
    ax.set_ylabel("2Y Spread", color='red')
    ax.tick_params(axis='y', labelcolor='red')
    ax.set_title("USDJPY 2Y Spread vs. USDJPY Curncy")
    ax2 = ax.twinx()
    ax2.plot(near_data.index, near_data['USDJPY Curncy'], color='black', label='USDJPY Curncy')
    last_valid_index = near_data['USDJPY Curncy'].dropna().index[-1]
    last_value = near_data['USDJPY Curncy'].dropna().iloc[-1]
    ax2.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='black')
    ax2.set_ylabel("USDJPY Curncy", color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(handles1 + handles2, labels1 + labels2, loc='upper left', fontsize='small')
    common_dates = near_data['usdjpy_2y_sprd'].dropna().index.intersection(near_data['USDJPY Curncy'].dropna().index)
    if not common_dates.empty:
        last_valid_date = common_dates.max().strftime('%Y-%m-%d')
        ax.text(0.98, 0.98, f"Last: {last_valid_date}", transform=ax.transAxes,
                ha='right', va='top', fontsize='x-small')
    # Bottom right: USDJPY 10Y Spread vs. USDJPY Curncy
    ax = axes[1, 1]
    ax.plot(near_data.index, near_data['usdjpy_10y_sprd'], color='red', label='USDJPY 10Y Spread')
    last_valid_index = near_data['usdjpy_10y_sprd'].dropna().index[-1]
    last_value = near_data['usdjpy_10y_sprd'].dropna().iloc[-1]
    ax.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='red')
    ax.set_ylabel("10Y Spread", color='red')
    ax.tick_params(axis='y', labelcolor='red')
    ax.set_title("USDJPY 10Y Spread vs. USDJPY Curncy")
    ax2 = ax.twinx()
    ax2.plot(near_data.index, near_data['USDJPY Curncy'], color='black', label='USDJPY Curncy')
    last_valid_index = near_data['USDJPY Curncy'].dropna().index[-1]
    last_value = near_data['USDJPY Curncy'].dropna().iloc[-1]
    ax2.plot(last_valid_index, last_value, marker='o', markersize=6, markerfacecolor='none', color='black')
    ax2.set_ylabel("USDJPY Curncy", color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    handles1, labels1 = ax.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(handles1 + handles2, labels1 + labels2, loc='upper left', fontsize='small')
    common_dates = near_data['usdjpy_10y_sprd'].dropna().index.intersection(near_data['USDJPY Curncy'].dropna().index)
    if not common_dates.empty:
        last_valid_date = common_dates.max().strftime('%Y-%m-%d')
        ax.text(0.98, 0.98, f"Last: {last_valid_date}", transform=ax.transAxes,
                ha='right', va='top', fontsize='x-small')
    for ax in fig.axes:
        plt.setp(ax.get_xticklabels(), rotation=45)
    fig.suptitle("US & DE/JP Real Rates Differential vs EURUSD/USDJPY")
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "USEUJP_RealRatesDiff_vs_FX.png"), bbox_inches='tight')
    plt.close(fig)


def chart_china_grid_investment_copper():
    """China_GridInvestment_Copper_Price_Imports.png -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2015, 2, 1)
    tickers = ['CNLCIGRY Index', 'CNLCIGRD Index', 'CNIVCOPP Index', 'HG1 COMB Comdty', 'CCSMCUG1 Index', 'CNEVCOPP Index']
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    for df in [all_data]:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
    all_data = all_data.resample('M').mean()
    all_data['Net Copper Imports'] = all_data['CNIVCOPP Index'] - all_data['CNEVCOPP Index']
    last_valid = all_data['CNLCIGRY Index'].last_valid_index()
    all_data.loc[:last_valid, 'CNLCIGRY Index'] = all_data.loc[:last_valid, 'CNLCIGRY Index'].fillna(0)
    all_data['CNLCIGRY Index'] = all_data['CNLCIGRY Index'].dropna().rolling(6).mean()
    last_valid_date_grid = all_data['CNLCIGRY Index'].dropna().last_valid_index()
    last_grid_value = all_data.loc[last_valid_date_grid, 'CNLCIGRY Index'] if last_valid_date_grid is not None else None
    last_valid_date_grid_str = last_valid_date_grid.strftime("%b %y") if last_valid_date_grid is not None else "N/A"
    last_valid_date_hg_price = all_data['CCSMCUG1 Index'].dropna().last_valid_index()
    last_price_value = all_data.loc[
        last_valid_date_hg_price, 'CCSMCUG1 Index'] if last_valid_date_hg_price is not None else None
    last_valid_date_hg_price_str = last_valid_date_hg_price.strftime(
        "%d %b %y") if last_valid_date_hg_price is not None else "N/A"
    last_valid_date_hg_imp = all_data['Net Copper Imports'].dropna().last_valid_index()
    last_imp_value = all_data.loc[
        last_valid_date_hg_imp, 'Net Copper Imports'] if last_valid_date_hg_imp is not None else None
    last_valid_date_hg_imp_str = last_valid_date_hg_imp.strftime("%b %y") if last_valid_date_hg_imp is not None else "N/A"
    fig, ax = plt.subplots(figsize=(12, 6))
    line_grid, = ax.plot(all_data.index, all_data['CNLCIGRY Index'], color='black', linewidth=2,
                         label="China's Grid Investment YoY % (6m MA)")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("China's Grid Investment YoY % (6m MA)", color='black', fontsize=12)
    ax.tick_params(axis='y', labelcolor='black')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax2 = ax.twinx()
    line_price, = ax2.plot(all_data.index, all_data['CCSMCUG1 Index'], color='green', linewidth=2, alpha=0.9,
                           label="China (Changjiang) Copper Price")
    ax2.set_ylabel("China (Changjiang) Copper Price", color='green', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='green')
    ax3 = ax.twinx()
    ax3.spines["right"].set_position(("outward", 60))
    line_imports, = ax3.plot(all_data.index, all_data['Net Copper Imports'], color='red', linewidth=2, alpha=0.9,
                             label="China Net Copper Imports")
    ax3.set_ylabel("China Net Copper Imports", color='red', fontsize=12)
    ax3.tick_params(axis='y', labelcolor='red')
    if last_valid_date_grid is not None:
        ax.plot(last_valid_date_grid, last_grid_value, marker='o', markersize=8, color='black', markeredgecolor='black')
        ax.text(last_valid_date_grid, last_grid_value,
                f" {last_grid_value:.2f} ({last_valid_date_grid_str})",
                color='black', fontsize=10, va='center', ha='left')
    if last_valid_date_hg_price is not None:
        ax2.plot(last_valid_date_hg_price, last_price_value, marker='o', markersize=8, color='green',
                 markeredgecolor='black')
        ax2.text(last_valid_date_hg_price, last_price_value,
                 f" {last_price_value:.2f} ({last_valid_date_hg_price_str})",
                 color='green', fontsize=10, va='center', ha='left')
    if last_valid_date_hg_imp is not None:
        ax3.plot(last_valid_date_hg_imp, last_imp_value, marker='o', markersize=8, color='red', markeredgecolor='black')
        ax3.text(last_valid_date_hg_imp, last_imp_value,
                 f" {last_imp_value:.2f} ({last_valid_date_hg_imp_str})",
                 color='red', fontsize=10, va='center', ha='left')
    ax.set_title("China's Grid Investment vs Copper Price/Net Copper Imports", fontsize=14)
    lines = [line_grid, line_price, line_imports]
    labels = [line.get_label() for line in lines]
    ax.legend(lines, labels, loc="upper left", fontsize=10)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "China_GridInvestment_Copper_Price_Imports.png"), bbox_inches='tight')
    plt.close(fig)


def chart_gs_cyc_vs_def_hy_embi_regression():
    """GSCycVsDef BarCapUSHYsprd JPEMBIsprd.png -- from charts_updater3.py"""
    END_DATE = datetime.today()
    if not STATSMODELS_AVAILABLE:
        print("Skipping GSCycVsDef regression chart: statsmodels not available")
        return
    START_DATE = datetime(2015, 1, 1)
    REG_START_DATE = '2015-01-01'
    REG_END_DATE = datetime.today()
    tickers = ['CSI BARC Index', 'JPEIGLSP Index', 'GSPUCYDE Index']
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    for df in [all_data]:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
    all_data['CSI BARC Index'] = all_data['CSI BARC Index'] * 100
    all_data.columns = ['BarCap US HY Sprd', 'JP EMBI Sprd', 'GS CycVsDef (ex.Comdty)']
    pairs = [
        {"x": "GS CycVsDef (ex.Comdty)", "y": "BarCap US HY Sprd",
         "title": "BarCap US HY Sprd vs. GS CycVsDef (ex.Comdty)"},
        {"x": "GS CycVsDef (ex.Comdty)", "y": "JP EMBI Sprd", "title": "JP EMBI Sprd vs. GS CycVsDef (ex.Comdty)"},
        {"x": "BarCap US HY Sprd", "y": "JP EMBI Sprd", "title": "EMBI Sprd vs. BarCap US HY Sprd"}
    ]
    marker_style = dict(marker='o', markersize=9, markerfacecolor='none',
                        markeredgecolor='black', markeredgewidth=2)
    fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(12, 18))
    for i, pair in enumerate(pairs):
        x_col = pair["x"]
        y_col = pair["y"]
        df_temp = all_data[[x_col, y_col]].dropna()
        df_reg = df_temp.loc[REG_START_DATE:REG_END_DATE]
        if df_reg.empty:
            continue
        x_reg = df_reg[x_col]
        y_reg = df_reg[y_col]
        X_reg = sm.add_constant(x_reg)
        model = sm.OLS(y_reg, X_reg).fit()
        a = model.params["const"]
        b = model.params[x_col]
        r2 = model.rsquared
        X_full = sm.add_constant(df_temp[x_col])
        yhat_full = model.predict(X_full)
        ax_left = axs[i, 0]
        ax_left.plot(df_temp.index, df_temp[y_col], label="Actual")
        ax_left.plot(df_temp.index, yhat_full, label="Predicted", linestyle="--")
        ax_left.set_xlabel("Date")
        ax_left.set_ylabel(y_col)
        ax_left.set_title(pair["title"])
        ax_left.legend()
        last_date_full = df_temp.index[-1]
        last_actual = df_temp[y_col].iloc[-1]
        last_pred = yhat_full[-1] if isinstance(yhat_full, np.ndarray) else yhat_full.iloc[-1]
        ax_left.plot(last_date_full, last_actual, **marker_style)
        ax_left.plot(last_date_full, last_pred, **marker_style)
        ax_left.annotate(f"{round(last_actual)}",
                         xy=(last_date_full, last_actual),
                         xytext=(-20, 10), textcoords='offset points',
                         color="black", fontweight="bold",
                         arrowprops=dict(arrowstyle="->", color="black", lw=0.5))
        ax_left.annotate(f"{round(last_pred)}",
                         xy=(last_date_full, last_pred),
                         xytext=(20, -15), textcoords='offset points',
                         color="black", fontweight="bold",
                         arrowprops=dict(arrowstyle="->", color="black", lw=0.5))
        last_valid_date = df_temp.index.max()
        last_valid_str = last_valid_date.strftime("%Y-%m-%d")
        eq_text = (f"y = {a:.2f} + {b:.2f}*{x_col}\n"
                   f"$R^2$ = {r2:.2f}\n"
                   f"Last valid date: {last_valid_str}")
        ax_left.text(0.05, 0.95, eq_text, transform=ax_left.transAxes,
                     verticalalignment="top",
                     bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))
        ax_right = axs[i, 1]
        colors = mdates.date2num(df_reg.index)
        sc = ax_right.scatter(x_reg, y_reg, c=colors, cmap='jet', alpha=0.5)
        x_line = np.linspace(x_reg.min(), x_reg.max(), 100)
        X_line = sm.add_constant(x_line)
        y_line = model.predict(X_line)
        ax_right.plot(x_line, y_line, color="red")
        last_x_train = x_reg.iloc[-1]
        last_y_train = y_reg.iloc[-1]
        ax_right.plot(last_x_train, last_y_train, **marker_style)
        ax_right.annotate("Last",
                          xy=(last_x_train, last_y_train),
                          xytext=(10, 10), textcoords='offset points',
                          color="black", fontweight="bold",
                          arrowprops=dict(arrowstyle="->", color="black", lw=0.5))
        ax_right.set_xlabel(x_col)
        ax_right.set_ylabel(y_col)
        ax_right.set_title(pair["title"])
        cbar = fig.colorbar(sc, ax=ax_right)
        cbar.set_label("Date")
        cbar.ax.yaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    reg_period_text = f"Regression trained on {REG_START_DATE} to {REG_END_DATE.strftime('%Y-%m-%d')}"
    fig.text(0.99, 0.98, reg_period_text, ha="right", va="top", fontsize=12, color="black")
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(Path(G_CHART_DIR, "GSCycVsDef BarCapUSHYsprd JPEMBIsprd.png"), bbox_inches='tight')
    plt.close(fig)


def chart_msci_india_em_relative_vs_copper_brent():
    """MSCI_India_EM_Relative_vs_CopperBrentRatio.png -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2002, 2, 1)
    tickers = ['MXIN Index', 'MXEF Index', 'CO1 Comdty', 'HG1 COMB Comdty']
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    for df in [all_data]:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
    last_date_with_data = all_data.dropna().index[-1]
    all_data = all_data.resample('M').last()
    all_data['Copper/Brent'] = (all_data['HG1 COMB Comdty'] / all_data['CO1 Comdty']).pct_change(12) * 100
    all_data['MSCI India/MSCI EM'] = (all_data['MXIN Index'] / all_data['MXEF Index']).pct_change(12) * 100
    fig, ax = plt.subplots(figsize=(12, 6))
    line1, = ax.plot(all_data.index, all_data['Copper/Brent'], color='orange', linewidth=2,
                     label="Copper/Brent Ratio (YoY Chng %)")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Copper/Brent Ratio (YoY Chng %)", color='orange', fontsize=12)
    ax.tick_params(axis='y', labelcolor='orange')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax2 = ax.twinx()
    line2, = ax2.plot(all_data.index, all_data['MSCI India/MSCI EM'], color='indigo', linewidth=2, alpha=0.9,
                      label="MSCI India/MSCI EM Rel. Performance (YoY Chng %)")
    ax2.set_ylabel("MSCI India/MSCI EM Rel. Performance (YoY Chng %)", color='indigo', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='indigo')
    ax.set_title("MSCI India/MSCI EM Rel. Performance vs Copper/Brent Ratio", fontsize=14)
    lines = [line1, line2]
    labels = [line.get_label() for line in lines]
    ax.set_ylim(-60, 140)
    ax2.set_ylim(-30, 70)
    ax.axhline(y=0, color='black')
    ax.legend(lines, labels, loc="upper left", fontsize=10)
    plt.text(0.95, 0.95, f"Last data: {last_date_with_data.strftime('%Y-%m-%d')}",
             horizontalalignment='right', verticalalignment='top',
             transform=ax.transAxes, fontsize=10, bbox=dict(facecolor='white', alpha=0.7))
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "MSCI_India_EM_Relative_vs_CopperBrentRatio.png"), bbox_inches='tight')
    plt.close(fig)


def chart_sensex_yoy_vs_gdp_minus_10y():
    """SensexYoY_vs_GDPminus10Y.png -- from charts_updater3.py"""
    END_DATE = datetime.today()
    START_DATE = datetime(2002, 2, 1)
    tickers = ['SENSEX Index', 'INBGDRQY Index', 'GIND10YR Index']
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    for df in [all_data]:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
    all_data['INBGDRQY Index'] = all_data['INBGDRQY Index'].ffill(limit=130)
    all_data['GIND10YR Index'] = all_data['GIND10YR Index'].ffill(limit=1)
    all_data['Sensex YoY'] = all_data['SENSEX Index'].pct_change(252) * 100
    all_data['GDP - 10Y'] = (all_data['INBGDRQY Index'] - all_data['GIND10YR Index'])
    valid_data = all_data.dropna(subset=['Sensex YoY', 'GDP - 10Y'])
    last_date_with_data = valid_data.index[-1]
    fig, ax = plt.subplots(figsize=(12, 6))
    line1, = ax.plot(all_data.index, all_data['GDP - 10Y'], color='#1f77b4', linewidth=2,
                     label="Real GDP Growth - 10Y Bond Yield")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Real GDP Growth - 10Y Bond Yield", color='#1f77b4', fontsize=12)
    ax.tick_params(axis='y', labelcolor='#1f77b4')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax2 = ax.twinx()
    line2, = ax2.plot(all_data.index, all_data['Sensex YoY'], color='#ff7f0e', linewidth=2, alpha=0.9, label="Sensex YoY %")
    ax2.set_ylabel("Sensex YoY %", color='#ff7f0e', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#ff7f0e')
    ax.set_title("Sensex YoY % vs (Real GDP Growth - 10Y Bond Yield (%))", fontsize=14)
    lines = [line1, line2]
    labels = [line.get_label() for line in lines]
    ax.set_ylim(-10, 10)
    ax.legend(lines, labels, loc="upper left", fontsize=10)
    plt.text(0.95, 0.95, f"Last data: {last_date_with_data.strftime('%Y-%m-%d')}",
             horizontalalignment='right', verticalalignment='top',
             transform=ax.transAxes, fontsize=10, bbox=dict(facecolor='white', alpha=0.7))
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "SensexYoY_vs_GDPminus10Y.png"), bbox_inches='tight')
    plt.close(fig)


# UPDATER3_CHARTS = [
#     ("Marginal Propensity To Save", chart_marginal_propensity_to_save),
#     ("NFP vs CEO Confidence and Operating Profits", chart_nfp_vs_ceo_confidence_and_operating_profits),
#     ("Inventory To Shipment Ratios", chart_inventory_to_shipment_ratios),
#     ("Service Activity", chart_service_activity),
#     ("US Money Velocity vs Core CPI", chart_us_money_velocity_vs_core_cpi),
#     ("China Railway Freight Traffic Turnover", chart_china_railway_freight_traffic_turnover),
#     ("EM Composite EPS vs Index Px", chart_em_composite_eps_vs_index_px),
#     ("Prime Age Employment Rate", chart_prime_age_employment_rate),
#     ("AHEYoY vs U6", chart_ahe_yoy_vs_u6),
#     ("FD_GDP_and_Credit_Growth", chart_fd_gdp_and_credit_growth),
#     ("EPSGrowth vs GDP-WagesXOperMargin (US, EU, UK, JP)", chart_eps_growth_vs_gdp_wages_x_oper_margin),
#     ("EPSGrowth vs GDP-WagesXOperMargin (AU, CN, IN, TW)", chart_eps_growth_vs_gdp_wages_x_oper_margin),
#     ("SPX Risk Premium", chart_spx_risk_premium),
#     ("MSCIWorld Yields Corr", chart_msciworld_yields_corr),
#     ("CommBankSecHldgs vs DXY", chart_comm_bank_sec_hldgs_vs_dxy),
#     ("EMDM Composite CurrLiab-FwdSales Ratio", chart_emdm_composite_currliab_fwdsales_ratio),
#     ("CN Banks Claims on Non-Fin Sector to Non-Fin Deposits Ratio", chart_cn_banks_claims_non_fin),
#     ("USEUJP_RealRatesDiff_vs_FX.png", chart_useujp_real_rates_diff_vs_fx),
#     ("China_GridInvestment_Copper_Price_Imports.png", chart_china_grid_investment_copper),
#     ("GSCycVsDef BarCapUSHYsprd JPEMBIsprd.png", chart_gs_cyc_vs_def_hy_embi_regression),
#     ("MSCI_India_EM_Relative_vs_CopperBrentRatio.png", chart_msci_india_em_relative_vs_copper_brent),
#     ("SensexYoY_vs_GDPminus10Y.png", chart_sensex_yoy_vs_gdp_minus_10y),
# ]


# --- Charts from charts_updater4.py ---

def chart_etf_flow_divergence():
    """ETF Flow Divergence -- from charts_updater4.py"""
    END_DATE = datetime.today()

    START_DATE = datetime.strptime("01/01/18", "%d/%m/%y")
    us_etfs = ['SPY US Equity', 'VTI US Equity', 'QQQ US Equity', 'IVV US Equity', 'IWM US Equity']
    eu_etfs = ['EZU US Equity', 'EZU UP Equity', 'IEUR US Equity', 'IEUR UP Equity',
               'FEZ US Equity', 'FEZ UP Equity', 'VGK US Equity', 'VGK UP Equity',
               'HEDJ US Equity', 'HEDJ UP Equity', 'FEP US Equity', 'FEP UP Equity']
    fields = ['FUND_FLOW', 'FUND_TOTAL_ASSETS']

    def get_data(etfs):
        df = blp.bdh(etfs, fields, START_DATE).dropna()
        df.columns.set_names(['Ticker', 'Field'], inplace=True)
        return df.swaplevel(0, 1, axis=1)

    def combine_us_up(df):
        combined = {}
        tickers = sorted(set(t.replace(' US Equity', '').replace(' UP Equity', '') for t in df.columns.levels[1]))
        for base in tickers:
            cols = [col for col in df.columns if col[1].startswith(base)]
            subset = df[cols].groupby(level=0, axis=1).sum()
            subset.columns = pd.MultiIndex.from_product([[col for col in subset.columns], [base]])
            subset.columns.set_names(['Field', 'Ticker'], inplace=True)
            combined[base] = subset
        return pd.concat(combined.values(), axis=1).sort_index(axis=1)

    def compute_normalized_flow(df):
        flow = df['FUND_FLOW']
        assets = df['FUND_TOTAL_ASSETS']
        return flow / assets

    def rolling_zscore(series, window=252 * 3):
        mean = series.rolling(window).mean()
        std = series.rolling(window).std()
        return (series - mean) / std

    us_data = get_data(us_etfs)
    eu_data = get_data(eu_etfs)
    us_combined = us_data.copy()
    eu_combined = combine_us_up(eu_data)
    us_norm = compute_normalized_flow(us_combined)
    eu_norm = compute_normalized_flow(eu_combined)

    us_norm_avg = us_norm.mean(axis=1).iloc[:-1]
    eu_norm_avg = eu_norm.mean(axis=1).iloc[:-1]
    us_total = us_combined['FUND_FLOW'].sum(axis=1).iloc[:-1]
    eu_total = eu_combined['FUND_FLOW'].sum(axis=1).iloc[:-1]

    us_3m_norm = us_norm_avg.rolling(window=63).sum()
    eu_3m_norm = eu_norm_avg.rolling(window=63).sum()
    us_3m_total = us_total.rolling(window=63).sum()
    eu_3m_total = eu_total.rolling(window=63).sum()

    us_z = rolling_zscore(us_3m_norm)
    eu_z = rolling_zscore(eu_3m_norm)

    z_start = us_z.dropna().index[0]
    last_date_str = us_3m_total.index[-1].strftime('%Y-%m-%d')

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 6))

    ax1.plot(us_z.loc[z_start:], label='USD Normalized Flows Z-Score', color='black')
    ax1.plot(eu_z.loc[z_start:], label='EUR Normalized Flows Z-Score', color='skyblue')
    ax1.set_title(f"3M Normalized Net Equity ETF Flow Z-Score (3Y Rolling)\nUpdated as of {last_date_str}")
    ax1.set_ylabel("Z-Score")
    ax1.axhline(0, color='gray', linestyle='--')
    ax1.legend(loc="upper left")
    ax1.set_xlabel("Date")
    ax1.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax1.tick_params(axis='x', rotation=60)

    line_us, = ax2.plot(us_3m_total.index, us_3m_total / 1e3, label='USD Flow', color='black')
    ax2.set_ylabel("USD Flow (Bio USD)", color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.axhline(0, color='gray', linestyle='--')

    ax2b = ax2.twinx()
    line_eu, = ax2b.plot(eu_3m_total.index, eu_3m_total / 1e3, label='EUR Flow', color='skyblue')
    ax2b.set_ylabel("EUR Flow (Bio USD)", color='skyblue')
    ax2b.tick_params(axis='y', labelcolor='skyblue')
    ax2b.axhline(0, color='gray', linestyle='--')

    ax2.set_title(f"3M Net Equity ETF Fund Flows (USD vs EUR)\nUpdated as of {last_date_str}")
    ax2.set_xlabel("Date")
    ax2.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
    ax2.tick_params(axis='x', rotation=60)
    ax2.legend([line_us, line_eu], ['USD Flow', 'EUR Flow'], loc='upper left')
    plt.tight_layout()
    chart_path = G_CHART_DIR / "ETF_Flow_Divergence.png"
    plt.savefig(chart_path, bbox_inches='tight')


def chart_oer_vs_caseshiller_adv18m():
    """OER vs CaseShiller 20-City Adv 18m -- from charts_updater4.py"""
    END_DATE = datetime.today()

    START_DATE = datetime(2002, 2, 1)
    tickers = ['SPCS20Y% Index', 'CPRHOERY Index']
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(all_data.columns, pd.MultiIndex):
        all_data.columns = all_data.columns.get_level_values(0)
    all_data.index = pd.to_datetime(all_data.index)
    case = all_data['SPCS20Y% Index']
    case_adv = case.copy()
    case_adv.index = case_adv.index + pd.DateOffset(months=18)
    case_adv.name = 'CaseShiller_Adv18m'
    oer = all_data['CPRHOERY Index']
    last_oer = oer.dropna().index[-1]
    last_cs_ext = case_adv.index.max()
    PLOT_START = datetime(2005, 1, 1)
    fig, ax = plt.subplots(figsize=(12, 6))
    mask_cs = case_adv.index >= PLOT_START
    ax.plot(
        case_adv.index[mask_cs],
        case_adv[mask_cs],
        color='#1f77b4',
        linewidth=2,
        label="Case-Shiller 20-City YoY% (adv 18m)")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Case-Shiller YoY% (adv 18m)", color='#1f77b4', fontsize=12)
    ax.tick_params(axis='y', labelcolor='#1f77b4')
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax.set_xlim(PLOT_START, last_cs_ext)
    ax2 = ax.twinx()
    mask_oer = oer.index >= PLOT_START
    ax2.plot(
        oer.index[mask_oer],
        oer[mask_oer],
        color='#ff7f0e',
        linewidth=2,
        alpha=0.9,
        label="OER YoY%")
    ax2.set_ylabel("OER YoY%", color='#ff7f0e', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#ff7f0e')
    ax.set_title(
        "OER YoY% vs Case-Shiller 20-City YoY% (adv 18m)",
        fontsize=14)
    lines = ax.get_lines() + ax2.get_lines()
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc="upper left", fontsize=10)
    plt.text(
        0.95, 0.95,
        f"Last OER data: {last_oer.strftime('%Y-%m-%d')}",
        horizontalalignment='right',
        verticalalignment='top',
        transform=ax.transAxes,
        fontsize=10,
        bbox=dict(facecolor='white', alpha=0.7))
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "OER_vs_CaseShiller20_Adv18m.png"), bbox_inches='tight')


def chart_opec_vs_nonopec_production_yoy():
    """OPEC vs Non-OPEC Oil Production YoY -- from charts_updater4.py"""
    END_DATE = datetime.today()

    START_DATE = datetime(2002, 1, 1)
    tickers = [
        'ST14WO Index',
        'OPCRTOTL Index'
    ]
    all_data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(all_data.columns, pd.MultiIndex):
        all_data.columns = all_data.columns.get_level_values(0)
    all_data.index = pd.to_datetime(all_data.index)
    all_data['World_1000'] = all_data['ST14WO Index'] * 1000
    all_data['NonOPEC']    = all_data['World_1000'] - all_data['OPCRTOTL Index']
    all_data['OPEC_YoY']     = all_data['OPCRTOTL Index'].pct_change(12) * 100
    all_data['NonOPEC_YoY']  = all_data['NonOPEC'].pct_change(12) * 100
    valid = all_data.dropna(subset=['OPEC_YoY', 'NonOPEC_YoY'])
    last_date = valid.index[-1]
    last_opec  = valid['OPEC_YoY'].iloc[-1]
    last_non   = valid['NonOPEC_YoY'].iloc[-1]
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(valid.index, valid['OPEC_YoY'],     label="OPEC Production YoY%",    linewidth=2)
    ax.plot(valid.index, valid['NonOPEC_YoY'],  label="Non-OPEC Production YoY%", linewidth=2)
    ax.axhline(0, color='black', linewidth=1)
    ax.scatter([last_date], [last_opec], marker='o', color='#1f77b4', edgecolor='k', zorder=5)
    ax.text(
        last_date, last_opec,
        f"{last_opec:.1f}%",
        va='bottom', ha='right',
        fontsize=10,)
    ax.scatter([last_date], [last_non], marker='o', color='#ff7f0e', edgecolor='k', zorder=5)
    ax.text(
        last_date, last_non,
        f"{last_non:.1f}%",
        va='top', ha='right',
        fontsize=10,)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Year-over-Year % Change", fontsize=12)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.xticks(rotation=45)
    ax.set_title("YoY % Change: OPEC vs Non-OPEC Oil Production", fontsize=14)
    ax.legend(loc="upper left", fontsize=10)
    plt.text(
        0.95, 0.92,
        f"Last data: {last_date.strftime('%Y-%m-%d')}",
        transform=ax.transAxes,
        ha='right', va='top',
        fontsize=10,
        bbox=dict(facecolor='white', alpha=0.7))
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "OPEC_vs_NonOPEC_Production_YoY.png"), bbox_inches='tight')


def chart_bis_debt_service_ratios():
    """BIS Non-Financial Debt Service Ratios -- from charts_updater4.py"""
    END_DATE = datetime.today()

    START_DATE = datetime(2002, 1, 1)
    tickers = {
        'US'       : 'BDSRUSP Index',
        'China'    : 'BDSRCNP Index',
        'Germany'  : 'BDSRDEP Index',
        'India'    : 'BDSRINP Index',
        'Russia'   : 'BDSRRUP Index',
        'Korea'    : 'BDSRKRP Index',
        'Malaysia' : 'BDSRMYP Index',
        'Indonesia': 'BDSRIDP Index',
        'Thailand' : 'BDSRTHP Index',
        'Brazil'   : 'BDSRBRP Index',
        'Czech'    : 'BDSRCZP Index'}
    all_data = blp.bdh(list(tickers.values()), "PX_LAST", START_DATE, END_DATE)
    if isinstance(all_data.columns, pd.MultiIndex):
        all_data.columns = all_data.columns.get_level_values(0)
    all_data.index = pd.to_datetime(all_data.index)
    fig, (ax1, ax2, ax3) = plt.subplots(
        ncols=3,
        figsize=(24, 6),
        gridspec_kw={'width_ratios': [2, 2, 1]})
    left_countries = ['US', 'China', 'Germany']
    left_codes = [tickers[c] for c in left_countries]
    for country in left_countries:
        s = all_data[tickers[country]].dropna()
        line, = ax1.plot(s.index, s.values, label=country, linewidth=2)
        ax1.text(
            s.index[-1], s.values[-1], f"{s.values[-1]:.1f}",
            color=line.get_color(), fontsize=8, ha='left', va='center')
    last_left = all_data[left_codes].dropna().index[-1]
    ax1.text(
        0.95, 0.95, f"Last date: {last_left.strftime('%Y-%m-%d')}",
        transform=ax1.transAxes, ha='right', va='top', fontsize=8)
    ax1.set_title("Non-Financial Debt Service Ratio: US, China & Germany")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Debt Service Ratio (%)")
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.tick_params(axis='x', rotation=45)
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(True)
    right_countries = [c for c in tickers if c not in left_countries]
    right_codes = [tickers[c] for c in right_countries]
    for country in right_countries:
        s = all_data[tickers[country]].dropna()
        line, = ax2.plot(s.index, s.values, label=country, linewidth=2)
        ax2.text(
            s.index[-1], s.values[-1], f"{s.values[-1]:.1f}",
            color=line.get_color(), fontsize=8, ha='left', va='center')
    last_mid = all_data[right_codes].dropna().index[-1]
    ax2.text(
        0.95, 0.95, f"Last date: {last_mid.strftime('%Y-%m-%d')}",
        transform=ax2.transAxes, ha='right', va='top', fontsize=8)
    ax2.set_title("Non-Financial Debt Service Ratio: EM")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Debt Service Ratio (%)")
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax2.tick_params(axis='x', rotation=45)
    ax2.legend(loc="upper left", fontsize=8)
    ax2.grid(True)
    latest_vals = {
        country: all_data[tickers[country]].dropna().iloc[-1]
        for country in tickers}
    latest_series = pd.Series(latest_vals).sort_values(ascending=False)
    bars = ax3.bar(latest_series.index, latest_series.values)
    for bar in bars:
        height = bar.get_height()
        ax3.text(
            bar.get_x() + bar.get_width() / 2,
            height, f"{height:.1f}", ha='center', va='bottom', fontsize=8)
    ax3.set_title("Latest Debt Service Ratio\n(all countries)")
    ax3.set_xlabel("Country")
    ax3.set_ylabel("Debt Service Ratio (%)")
    ax3.set_xticks(range(len(latest_series)))
    ax3.set_xticklabels(latest_series.index, rotation=45, ha='right')
    ax3.grid(axis='y')
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "BIS_DebtServiceRatios.png"), bbox_inches='tight')


def chart_china_commodities_prices_trend():
    """China Commodity Prices & Composite Indicator -- from charts_updater4.py"""
    END_DATE = datetime.today()

    names = {
        'CIOSHEBE Index': "Iron Ore Hebei/Tangshan",
        'CEFWOPCT Index': "Portland Cement (Bulk)",
        'CDSPHRAV Index': "Steel (Hot Rolled Sheet)",
        'CHNCBRPR Index': "Poly-Butadiene Rubber",
        'CCKPTANG Index': "Met Coal Grade 1"
    }
    tickers = list(names.keys())
    START_DATE = datetime(2002, 1, 1)
    raw = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    raw.index = pd.to_datetime(raw.index)
    data = raw.ffill(limit=30)
    common_start = data.dropna().index[0]
    base = data.loc[common_start]
    indexed = data.div(base).mul(100)
    z = (data - data.mean()) / data.std()
    composite = z.mean(axis=1)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
                                   gridspec_kw={'height_ratios': [2, 2], 'hspace': 0.3})
    fig.suptitle("China Commodity Prices & Composite Indicator", fontsize=16)
    fig.text(0.5, 0.93,
             f"Top: prices indexed to 100 at {common_start.date()}  |  Bottom: Composite = avg of z-scores (of all historical data)",
             ha='center', fontsize=10)
    fig.text(0.98, 0.95, f"Last data: {END_DATE.date()}",
             ha='right', va='top',
             fontsize=9, bbox=dict(facecolor='white', edgecolor='black'))
    cmap = plt.get_cmap('tab10')
    for i, ticker in enumerate(tickers):
        ax1.plot(indexed.index, indexed[ticker], label=names[ticker], color=cmap(i), lw=1.5)
    ax1.set_title(f"Normalized China Commodity Prices (base=100 @ {common_start.date()})")
    ax1.set_ylabel("Index (100 = price on base date)")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(True)
    ax2.plot(composite.index, composite, color='black', lw=2)
    ax2.set_title("Composite: Avg z-score of Prices")
    ax2.set_ylabel("Avg z-score")
    ax2.set_xlabel("Date")
    ax2.grid(True)
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.setp(ax2.get_xticklabels(), rotation=45)
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    plt.savefig(Path(G_CHART_DIR, "China_Commodities_Prices_Trend.png"), bbox_inches='tight')


def chart_reer_lt_deviation():
    """REER Deviations from LT Average -- from charts_updater4.py"""
    END_DATE = datetime.today()

    dm_map = {
        'CTTWBRUS Index': 'US', 'CTTWBRCH Index': 'Switzerland',
        'CTTWBREU Index': 'Euro Area', 'CTTWBRGB Index': 'UK',
        'CTTWBRCA Index': 'Canada', 'CTTWBRJP Index': 'Japan',
        'CTTWBRAU Index': 'Australia', 'CTTWBRNZ Index': 'New Zealand',
        'CTTWBRNO Index': 'Norway', 'CTTWBRSE Index': 'Sweden'}
    asia_map = {
        'CTTWBRCN Index': 'China', 'CTTWBRIN Index': 'India',
        'CTTWBRID Index': 'Indonesia', 'CTTWBRTW Index': 'Taiwan',
        'CTTWBRTH Index': 'Thailand', 'CTTWBRKR Index': 'Korea',
        'CTTWBRMY Index': 'Malaysia', 'CTTWBRSG Index': 'Singapore',
        'CTTWBRPH Index': 'Philippines', 'CTTWBRVN Index': 'Vietnam'}
    other_map = {
        'CTTWBRTR Index': 'Turkey', 'CTTWBRMX Index': 'Mexico',
        'CTTWBRZA Index': 'South Africa', 'CTTWBRCL Index': 'Chile',
        'CTTWBRIL Index': 'Israel', 'CTTWBRBR Index': 'Brazil'}
    all_map = {**dm_map, **asia_map, **other_map}
    START_DATE = datetime(2002, 1, 1)
    tickers = list(all_map.keys())
    data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    data.index = pd.to_datetime(data.index)
    last_date = data.index[-1]
    window = 252 * 10
    avg10 = data.loc[data.index >= last_date - relativedelta(years=10)].mean()
    rolling_mean = data.rolling(window=window, min_periods=window).mean()
    rolling_std  = data.rolling(window=window, min_periods=window).std()
    zscore = (data - rolling_mean) / rolling_std
    PLOT_START_ABS = last_date - relativedelta(years=10)
    PLOT_START_Z   = last_date - relativedelta(years=5)
    fig = plt.figure(figsize=(28, 14))
    gs = GridSpec(2, 4, figure=fig,
                  width_ratios=[1, 1, 1, 0.9],
                  height_ratios=[1, 1],
                  wspace=0.2,
                  hspace=0.3)
    fig.suptitle("Citi Broad Trade-weighted REER & Deviation from LT (10-year) average",
                 fontsize=18, y=0.98)
    fig.text(0.5, 0.94,
             "REER Z-score calculated using a 10-year rolling window",
             ha='center', va='top', fontsize=13)
    fig.text(0.98, 0.92, f"Last data: {last_date.date()}",
             ha='right', va='top', fontsize=11,
             bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))
    ax_abs_dm    = fig.add_subplot(gs[0, 0])
    ax_abs_asia  = fig.add_subplot(gs[0, 1])
    ax_abs_oth   = fig.add_subplot(gs[0, 2])
    ax_table     = fig.add_subplot(gs[:, 3])
    ax_z_dm      = fig.add_subplot(gs[1, 0])
    ax_z_asia    = fig.add_subplot(gs[1, 1])
    ax_z_oth     = fig.add_subplot(gs[1, 2])
    mask_abs = data.index >= PLOT_START_ABS
    mask_z = zscore.index >= PLOT_START_Z
    for ax, mp, label in zip(
        [ax_abs_dm, ax_abs_asia, ax_abs_oth],
        [dm_map, asia_map, other_map],
        ["DM", "Asia", "Others"]):
        last_vals = {code: data[code].loc[last_date] for code in mp}
        sorted_codes = sorted(mp.keys(), key=lambda c: last_vals[c], reverse=True)
        lines = []
        for code in sorted_codes:
            country = mp[code]
            ln, = ax.plot(data.index[mask_abs], data[code][mask_abs], label=country, lw=1.5)
            lines.append(ln)
        ax.set_title(f"REER (abs): {label}")
        ax.set_ylabel("REER Index" if ax is ax_abs_dm else "")
        ax.legend(lines, [mp[c] for c in sorted_codes], fontsize=8, loc="upper left")
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.tick_params(axis='x', rotation=45)
    for ax, mp, label in zip(
        [ax_z_dm, ax_z_asia, ax_z_oth],
        [dm_map, asia_map, other_map],
        ["DM", "Asia", "Others"]):
        last_z = {code: zscore[code].iloc[-1] for code in mp}
        sorted_codes = sorted(mp.keys(), key=lambda c: last_z[c], reverse=True)
        lines = []
        for code in sorted_codes:
            country = mp[code]
            ln, = ax.plot(zscore.index[mask_z], zscore[code][mask_z], label=country, lw=1.5)
            lines.append(ln)
        ax.axhline(0, color='black', lw=1)
        ax.set_title(f"REER 10y z-score: {label}")
        ax.set_ylabel("Z-score" if ax is ax_z_dm else "")
        ax.legend(lines, [mp[c] for c in sorted_codes], fontsize=8, loc="upper left")
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.tick_params(axis='x', rotation=45)
    ax_table.axis('off')
    latest_vals = data.iloc[-1]
    abs_dev = latest_vals - avg10
    tbl = pd.DataFrame({
        "Country": [all_map[tk] for tk in tickers],
        "10Y Avg": [avg10[tk] for tk in tickers],
        "Z-Score": [zscore[tk].iloc[-1] for tk in tickers],
        "Abs Dev": [abs_dev[tk] for tk in tickers],})
    tbl = tbl.sort_values("Z-Score", ascending=False).reset_index(drop=True)
    norm_z   = TwoSlopeNorm(vmin=tbl["Z-Score"].min(), vcenter=0, vmax=tbl["Z-Score"].max())
    norm_dev = TwoSlopeNorm(vmin=tbl["Abs Dev"].min(), vcenter=0, vmax=tbl["Abs Dev"].max())
    cmap = plt.cm.RdYlGn
    cell_colors = [['white'] * 4]
    for _, row in tbl.iterrows():
        cell_colors.append(['white', 'white', cmap(norm_z(row["Z-Score"])), cmap(norm_dev(row["Abs Dev"]))])
    cell_text = [tbl.columns.tolist()]
    for _, row in tbl.iterrows():
        cell_text.append([
            row["Country"], f"{row['10Y Avg']:.2f}", f"{row['Z-Score']:.2f}", f"{row['Abs Dev']:.2f}"])
    table = ax_table.table(cellText=cell_text, cellColours=cell_colors, cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.5)
    fig.subplots_adjust(left=0.05, right=0.97, top=0.88, bottom=0.05, wspace=0.2, hspace=0.3)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(Path(G_CHART_DIR, "REER_LT_Deviation.png"), bbox_inches='tight')


def chart_idr_total_loans_to_gdp_yoy():
    """Indonesia Bank Loans as % of GDP -- from charts_updater4.py"""
    END_DATE = datetime.today()

    START_DATE = datetime(2002, 1, 1)
    tickers = [
        'IDLPTOTL Index',
        'IDGRP Index'
    ]
    data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    data.index = pd.to_datetime(data.index)
    data = data.ffill(limit=2)
    ratio = data['IDLPTOTL Index'] / data['IDGRP Index'] * 100
    speed = ratio.diff(12)
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    ln1, = ax1.plot(
        ratio.index, ratio,
        color='tab:orange', alpha=0.5, lw=2,
        label="Loans/GDP Ratio (%)"
    )
    ax1.set_ylabel("Loans/GDP Ratio (%)", color='tab:orange', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='tab:orange')
    ln2, = ax2.plot(
        speed.index, speed,
        color='tab:blue', lw=2,
        label="Δ Loans/GDP (YoY, p.p.)"
    )
    ax2.set_ylabel("Δ Loans/GDP (p.p.)", color='tab:blue', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='tab:blue')
    ax2.axhline(0, color='black', linewidth=1, linestyle='--')
    last_ratio_date = ratio.dropna().index[-1]
    last_ratio_val  = ratio.dropna().iloc[-1]
    ax1.scatter([last_ratio_date], [last_ratio_val], color='tab:orange', edgecolor='k', zorder=5)
    ax1.text(
        last_ratio_date, last_ratio_val,
        f"{last_ratio_val:.1f}%",
        ha='left', va='bottom',
        fontsize=10, color='tab:orange',
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none')
    )
    last_speed_date = speed.dropna().index[-1]
    last_speed_val  = speed.dropna().iloc[-1]
    ax2.scatter([last_speed_date], [last_speed_val], color='tab:blue', edgecolor='k', zorder=5)
    ax2.text(
        last_speed_date, last_speed_val,
        f"{last_speed_val:.2f} p.p.",
        ha='left', va='bottom',
        fontsize=10, color='tab:blue',
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none')
    )
    fig.text(
        0.98, 0.95,
        f"Last data: {last_ratio_date.date()}",
        ha='right', va='top',
        fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round')
    )
    ax1.set_title("Indonesia: Bank Loans as % of GDP – Level and YoY Change", fontsize=14)
    ax1.set_xlabel("Date", fontsize=12)
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax1.get_xticklabels(), rotation=45)
    handles = [ln1, ln2]
    labels  = [ln.get_label() for ln in handles]
    ax1.legend(handles, labels, loc="upper left", fontsize=10)
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax2.grid(False)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "IDR_TotalLoans_to_GDP_YoY.png"), bbox_inches='tight')


def chart_equities_pe_eps_vs_price():
    """Rolling 6M Change in Fwd P/E, Price, and Fwd EPS -- from charts_updater4.py"""
    END_DATE = datetime.today()

    indices = [
        'SPX Index', 'NDQ Index', 'SX7E Index',
        'HSI Index', 'SHSZ300 Index', 'TWSE Index',
        'NKY Index', 'NIFTY Index', 'KOSPI Index'
    ]
    START_DATE = datetime(2015, 1, 1)
    data_dict = {}
    last_dates = []
    for idx in indices:
        df = blp.bdh(
            [idx],
            ["PX_LAST", "PE_RATIO", "BEST_PE_RATIO", "TRAIL_12M_EPS", "BEST_EPS"],
            START_DATE, END_DATE
        )
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(1)
        df.index = pd.to_datetime(df.index)
        df = df.ffill(limit=12)
        df['P/E Change']   = (df['BEST_PE_RATIO'].pct_change(126) * 100).rolling(10).mean()
        df['Price Change'] = (df['PX_LAST'].pct_change(126) * 100).rolling(10).mean()
        df['EPS Change']   = (df['BEST_EPS'].pct_change(126) * 100).rolling(10).mean()
        data_dict[idx] = df
        last_dates.append(df.index.max())
    last_date = min(last_dates)
    fig, axes = plt.subplots(3, 3, figsize=(24, 20), sharex=True)
    axes = axes.flatten()
    for ax in axes:
        ax.set_facecolor('#f7f7f7')
        ax.grid(True, linestyle='--', linewidth=0.6, alpha=0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    for ax, idx in zip(axes, indices):
        df = data_dict[idx]
        ln1, = ax.plot(df.index, df['P/E Change'],   color='blue', lw=2.5, label='6m P/E Change')
        ln2, = ax.plot(df.index, df['Price Change'], color='red',   lw=2.5, label='6m Price Change')
        ln3, = ax.plot(df.index, df['EPS Change'],   color='green', lw=2.5, label='6m EPS Change')
        ax.axhline(0, color='gray', lw=1, linestyle=':')
        for series, line, fmt, va in [
            ('P/E Change', ln1, '{:.1f}%', 'bottom'),
            ('Price Change', ln2, '{:.1f}%', 'bottom'),
            ('EPS Change', ln3, '{:.1f}%', 'top')
        ]:
            ser = df[series].dropna()
            if not ser.empty:
                x0, y0 = ser.index[-1], ser.iloc[-1]
                ax.annotate(fmt.format(y0), xy=(x0, y0),
                            xytext=(5, 0), textcoords='offset points',
                            fontsize=12, color=line.get_color(), va=va)
        ax.set_title(idx, fontsize=18, pad=12)
        ax.set_ylabel('Change (%)', fontsize=14)
        if ax.get_subplotspec().is_last_row():
            ax.xaxis.set_major_locator(mdates.YearLocator(2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            for lbl in ax.get_xticklabels():
                lbl.set_rotation(45)
                lbl.set_fontsize(12)
        else:
            ax.set_xticklabels([])
    axes[0].legend(fontsize=14, loc='upper left')
    fig.suptitle('Rolling 6 Month Change in Fwd P/E, Price, and Fwd EPS (10d MA for all)', fontsize=26, y=0.96)
    fig.text(0.98, 0.92, f'Last data: {last_date.date()}', ha='right', va='top',
             fontsize=18, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(Path(G_CHART_DIR, 'Equities_PE_EPS_vs_Price.png'), bbox_inches='tight')


def chart_indonesia_bi_liquidity():
    """Indonesia BI Outstanding OMO Monitor -- from charts_updater4.py"""
    END_DATE = datetime.today()

    START_DATE = datetime(2017, 1, 1)
    volume_tickers = [
        'IFSW1M Index', 'IFSW3M Index', 'IFSW6M Index', 'IFSW12M Index',
        'IDFB1MTH Index', 'IDFB1MPM Index', 'IDFB3MTH Index', 'IDFB3MPM Index',
        'IDFB1MRP Index', 'USTDONUS Index', 'USTD1WUS Index', 'USTD2WUS Index',
        'USTD1MUS Index', 'USTD3MUS Index', 'BITDS1W Index', 'BITDS2W Index',
        'BITDS1M Index', 'BITDS3M Index']
    price_tickers = [
        'SBI9SBA6 Index', 'SBI9SBA9 Index',
        'IDTV1MAC Index', 'IDTV3MAC Index', 'IDTV6MAC Index', 'IDTV12MAC Index',
        'IDTU1MAC Index', 'IDTU3MAC Index']
    raw_vol = blp.bdh(volume_tickers, 'PR013', START_DATE, END_DATE)
    raw_px  = blp.bdh(price_tickers,  'PX_LAST', START_DATE, END_DATE)
    vols = raw_vol.xs('PR013', axis=1, level=1) if isinstance(raw_vol.columns, pd.MultiIndex) else raw_vol.copy()
    pxs  = raw_px.xs('PX_LAST', axis=1, level=1) if isinstance(raw_px.columns, pd.MultiIndex) else raw_px.copy()
    flows = pd.concat([vols, pxs], axis=1)
    flows.index = pd.to_datetime(flows.index)
    flows = flows.ffill(limit=1)
    flow_map = {
        'IFSW1M Index': 'Swap 1M', 'IFSW3M Index': 'Swap 3M', 'IFSW6M Index': 'Swap 6M', 'IFSW12M Index': 'Swap 12M',
        'IDFB1MTH Index': 'DNDF 1M AM', 'IDFB1MPM Index': 'DNDF 1M PM',
        'IDFB3MTH Index': 'DNDF 3M AM', 'IDFB3MPM Index': 'DNDF 3M PM', 'IDFB1MRP Index': 'DNDF Rollover',
        'USTDONUS Index': 'FX TD O/N-3d', 'USTD1WUS Index': 'FX TD 1W', 'USTD2WUS Index': 'FX TD 2W',
        'USTD1MUS Index': 'FX TD 1M', 'USTD3MUS Index': 'FX TD 3M',
        'BITDS1W Index': 'Shariah FX TD 1W', 'BITDS2W Index': 'Shariah FX TD 2W',
        'BITDS1M Index': 'Shariah FX TD 1M', 'BITDS3M Index': 'Shariah FX TD 3M',
        'SBI9SBA6 Index': 'SBBI 6M', 'SBI9SBA9 Index': 'SBBI 9M',
        'IDTV1MAC Index': 'SVBI 1M', 'IDTV3MAC Index': 'SVBI 3M',
        'IDTV6MAC Index': 'SVBI 6M', 'IDTV12MAC Index': 'SVBI 12M',
        'IDTU1MAC Index': 'SUVBI 1M', 'IDTU3MAC Index': 'SUVBI 3M'}
    flows.rename(columns=flow_map, inplace=True)
    flows['DNDF 1M'] = flows['DNDF 1M AM'].fillna(0) + flows['DNDF 1M PM'].fillna(0)
    flows['DNDF 3M'] = flows['DNDF 3M AM'].fillna(0) + flows['DNDF 3M PM'].fillna(0)
    windows_fx = {
        'Swap 1M': 30, 'Swap 3M': 90, 'Swap 6M': 180, 'Swap 12M': 365,
        'DNDF 1M': 30, 'DNDF 3M': 90, 'DNDF Rollover': 30,
        'FX TD O/N-3d': 3, 'FX TD 1W': 7, 'FX TD 2W': 14, 'FX TD 1M': 30, 'FX TD 3M': 90,
        'Shariah FX TD 1W': 7, 'Shariah FX TD 2W': 14, 'Shariah FX TD 1M': 30, 'Shariah FX TD 3M': 90,
        'SBBI 6M': 180, 'SBBI 9M': 270,
        'SVBI 1M': 30, 'SVBI 3M': 90, 'SVBI 6M': 180, 'SVBI 12M': 365,
        'SUVBI 1M': 30, 'SUVBI 3M': 90
    }
    stock_fx = pd.DataFrame(index=flows.index)
    for comp, wnd in windows_fx.items():
        if comp in flows:
            stock_fx[comp] = flows[comp].rolling(wnd, min_periods=1).sum()
    agg_fx = pd.DataFrame(index=stock_fx.index)
    agg_fx['Swaps']             = stock_fx[[c for c in stock_fx if c.startswith('Swap')]].sum(axis=1)
    agg_fx['DNDF']              = stock_fx[['DNDF 1M', 'DNDF 3M', 'DNDF Rollover']].sum(axis=1) / 1e6
    agg_fx['FX Term Deposits']  = stock_fx[[c for c in stock_fx if c.startswith('FX TD')]].sum(axis=1)
    agg_fx['Shariah FX TD']     = stock_fx[[c for c in stock_fx if c.startswith('Shariah FX TD')]].sum(axis=1)
    agg_fx['SBBI Valas']        = stock_fx[['SBBI 6M', 'SBBI 9M']].sum(axis=1)
    agg_fx['SVBI']              = stock_fx[[c for c in stock_fx if c.startswith('SVBI')]].sum(axis=1) / 1e3
    agg_fx['SUVBI']             = stock_fx[[c for c in stock_fx if c.startswith('SUVBI')]].sum(axis=1) / 1e3
    agg_fx = agg_fx[agg_fx.index >= START_DATE]
    conv_tix = {
        'BILPSBIT Index': 'SBI Issuance', 'BILPSDBI Index': 'SDBI Issuance',
        'BILPSRBI Index': 'SRBI Issuance', 'BILPRRPO Index': 'Conventional Reverse Repo',
        'BILPBILP Index': 'Conventional Repo', 'BILPTDPO Index': 'Conventional Term Deposit',
        'BILPDFVL Index': 'Deposit Facility', 'BILPLDFC Index': 'Lending Facility'}
    raw_conv = blp.bdh(list(conv_tix.keys()), 'PX_LAST', START_DATE, END_DATE)
    stock_conv = raw_conv.xs('PX_LAST', axis=1, level=1) if isinstance(raw_conv.columns, pd.MultiIndex) else raw_conv.copy()
    stock_conv.rename(columns=conv_tix, inplace=True)
    stock_conv = stock_conv.sort_index().ffill(limit=1)
    stock_conv['Conventional Repo']  *= -1
    stock_conv['Lending Facility']   *= -1
    cols = [c for c in stock_conv.columns if c != 'SRBI Issuance'] + ['SRBI Issuance']
    stock_conv = stock_conv[cols]
    pas_tix = {
        'BIRVSA1W Index': 'PasBI 1W', 'BIRVSA2W Index': 'PasBI 2W',
        'BIRVSA1M Index': 'PasBI 1M', 'BIRVSA3M Index': 'PasBI 3M'}
    raw_pas = blp.bdh(list(pas_tix.keys()), 'PR013', START_DATE, END_DATE)
    vols_pas = raw_pas.xs('PR013', axis=1, level=1) if isinstance(raw_pas.columns, pd.MultiIndex) else raw_pas.copy()
    vols_pas.rename(columns=pas_tix, inplace=True)
    vols_pas = vols_pas.sort_index().ffill(limit=1)
    win_pas = {'PasBI 1W': 7, 'PasBI 2W': 14, 'PasBI 1M': 30, 'PasBI 3M': 90}
    stock_pas = pd.DataFrame({n: vols_pas[n].rolling(w, min_periods=1).sum() for n, w in win_pas.items()})
    stock_pas['PaSBI'] = stock_pas.sum(axis=1)
    stock_pas = stock_pas[['PaSBI']]
    sh_tix = {
        'BILPSBIS Index': 'SBIS Issuance', 'BILPSKBI Index': 'SUKBI Issuance',
        'BILPRRSB Index': 'Sharia Reverse Repo', 'BILPRPSB Index': 'Sharia Repo',
        'BILPDFVS Index': 'Deposit Facility Syariah', 'BILPLDFS Index': 'Lending Facility Syariah'}
    raw_sh = blp.bdh(list(sh_tix.keys()), 'PX_LAST', START_DATE, END_DATE)
    stock_sh = raw_sh.xs('PX_LAST', axis=1, level=1) if isinstance(raw_sh.columns, pd.MultiIndex) else raw_sh.copy()
    stock_sh.rename(columns=sh_tix, inplace=True)
    stock_sh = stock_sh.sort_index().ffill(limit=1)
    stock_sh['Sharia Repo']                *= -1
    stock_sh['Lending Facility Syariah']   *= -1
    stock_sharia = pd.concat([stock_sh, stock_pas], axis=1).sort_index()
    for df in (agg_fx, stock_conv, stock_sharia):
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(25, 25), sharex=True)
    fig.suptitle("Indonesia BI Outstanding OMO Monitor (Conventional, Shariah, ForeignCcy)", fontsize=24, y=0.93)
    datasets = [
        (stock_conv,   "Conventional Domestic", "IDR Billion"),
        (stock_sharia, "Shariah Domestic", "IDR Billion"),
        (agg_fx,       "Foreign Ccy", "USD Million"),]
    for ax, (df, subtitle, ylabel) in zip(axes, datasets):
        df.plot.area(ax=ax, linewidth=0, alpha=0.8)
        total = df.sum(axis=1)
        ax.plot(total.index, total, color='black', lw=2, label='Total')
        last_date = total.index[-1]
        last_val  = total.iloc[-1]
        ax.scatter(last_date, last_val, color='black', zorder=5)
        ax.annotate(f"{last_val:.2f}",
                    xy=(last_date, last_val),
                    xytext=(10, -10),
                    textcoords='offset points',
                    fontsize=12,
                    fontweight='bold')
        ax.text(0.98, 0.98, str(pd.to_datetime(last_date).date()),
                transform=ax.transAxes, ha='right', va='top',
                fontsize=14,
                bbox=dict(boxstyle='round,pad=0.5', fc='white', ec='black', lw=1))
        ax.set_title(subtitle, loc='left', fontsize=18, fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=16)
        ax.legend(loc='upper left', fontsize=14)
    axes[-1].set_xlabel("Date", fontsize=16)
    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(axes[-1].get_xticklabels(), rotation=45, fontsize=14)
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    plt.savefig(Path(G_CHART_DIR, 'IndonesiaBILiquidity.png'), bbox_inches='tight')
def chart_westpac_asia_surprise_vs_equities():
    """Westpac Asia Positive Surprise vs Asia Equities -- from charts_updater4.py"""
    END_DATE = datetime.today()

    indices = [
        'HSI Index', 'SHSZ300 Index', 'TWSE Index',
        'STI Index', 'KOSPI Index'
    ]
    westpac_ticker = "WSURASIP Index"
    START_DATE = datetime(2010, 1, 1)
    raw = blp.bdh(
        indices,
        ["PX_LAST", "INDX_MARKET_CAP"],
        START_DATE,
        END_DATE, currency='USD'
    )
    raw.columns.names = ["ticker", "field"]
    raw.index = pd.to_datetime(raw.index)
    prices_m = (
        raw.xs("PX_LAST", level="field", axis=1)
           .resample("W")
           .last()
           .ffill(limit=2)
    )
    mktcaps_m = (
        raw.xs("INDX_MARKET_CAP", level="field", axis=1)
           .resample("W")
           .last()
           .ffill(limit=2)
    )
    weights       = mktcaps_m.div(mktcaps_m.sum(axis=1), axis=0)
    weighted_idx  = (prices_m * weights).sum(axis=1)
    ln_eq = np.log(weighted_idx)
    w = blp.bdh([westpac_ticker], "PX_LAST", START_DATE, END_DATE)
    w.index = pd.to_datetime(w.index)
    w_m = w[westpac_ticker].resample('W').last().ffill(limit=2).rolling(26).mean()
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(w_m.index, w_m, color='tab:blue', lw=2, label="Westpac Asia Surprise")
    ax.set_ylabel("Westpac Surprise Index", color='tab:blue')
    ax.tick_params(axis='y', labelcolor='tab:blue')
    ax2 = ax.twinx()
    ax2.plot(ln_eq.index, ln_eq, color='tab:orange', lw=2, label="Log(Mkt-Cap Weighted Asia Equities)")
    ax2.set_ylabel("Asia Equities (Natural log)", color='tab:orange')
    ax2.tick_params(axis='y', labelcolor='tab:orange')
    ax.set_title("Westpac Asia Positive Surprise (6m MA) vs Asia Equities (CN/HK/TW/SK/SG) (Mkt Cap Wgt, log)", fontsize=12)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax.get_xticklabels(), rotation=45)
    ax.grid(True, linestyle=':', alpha=0.6)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='upper left')
    last_date = min(w_m.index.max(), ln_eq.index.max())
    plt.text(0.98, 0.95, f"Last data: {last_date.date()}",
             ha='right', va='top', transform=fig.transFigure,
             bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Westpac_Asia_Surprise_vs_Asia_Equities.png"),
                bbox_inches='tight')


def chart_bsp_liquidity():
    """Philippines BSP Liquidity -- from charts_updater4.py"""
    END_DATE = datetime.today()

    names = {
        'PHRMSDA Index':  'Overnight Deposit Facility',
        'PHRRPO Index':   'Overnight Lending Facility',
        'PHRMTDF Index':  'Term Deposit Facility',
        'PHRMRRF Index':  'RRP Facility',
        'PHRMBSPS Index': 'BSP Bills',
        'PHRMPRD Index':  'Peso Rediscounting',}
    tickers = list(names.keys())
    START_DATE = datetime(2011, 1, 1)
    CUTOFF     = pd.Timestamp('2018-10-31')
    raw = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(raw.columns, pd.MultiIndex):
        if "PX_LAST" in raw.columns.get_level_values(-1):
            df = raw.xs("PX_LAST", level=-1, axis=1)
        else:
            df = raw.xs("PX_LAST", level=0, axis=1)
    else:
        df = raw.copy()
    df.index = pd.to_datetime(df.index)
    df = df.ffill(limit=2)
    df = df.apply(pd.to_numeric, errors='coerce')
    mask_pre = df.index < CUTOFF
    df.loc[mask_pre, 'PHRMRRF Index'] = df.loc[mask_pre, 'PHRRPO Index']
    df.loc[mask_pre, 'PHRRPO Index'] = np.nan
    df = df.rename(columns=names)
    df_flip = -df
    df_plot = df_flip.fillna(0)
    total = df_flip.sum(axis=1, min_count=1)
    last_valid_date = total.dropna().index[-1]
    last_total_val  = total.dropna().iloc[-1]
    fig, ax = plt.subplots(figsize=(14, 8))
    cols = list(df_plot.columns)
    colors = plt.get_cmap('tab20').colors[:len(cols)]
    ax.stackplot(
        df_plot.index,
        df_plot[cols].T.values,
        labels=cols,
        colors=colors,
        alpha=0.85
    )
    olf_name = names['PHRRPO Index']
    if olf_name in df_flip.columns:
        olf_color = colors[cols.index(olf_name)]
        ax.plot(df_flip.index, df_flip[olf_name], color=olf_color, lw=1.2, zorder=5, label="_nolegend_")
    ax.plot(total.index, total, color='black', lw=2.2, label='Total', zorder=6)
    ax.set_title("Philippines BSP Liquidity (+ve = withdrawal)", fontsize=15)
    ax.set_ylabel("Amount")
    ax.set_xlabel("Date")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax.get_xticklabels(), rotation=45)
    ax.grid(True, linestyle=':', alpha=0.5)
    handles, labels = ax.get_legend_handles_labels()
    if 'Total' in labels:
        i = labels.index('Total')
        handles = [handles[i]] + handles[:i] + handles[i+1:]
        labels  = [labels[i]]  + labels[:i]  + labels[i+1:]
    ax.legend(handles, labels, loc='upper left', fontsize=9, ncol=2)
    ax.scatter([last_valid_date], [last_total_val], color='black', zorder=7)
    ax.text(
        last_valid_date, last_total_val,
        f"{last_total_val:,.0f}",
        ha='left', va='bottom', fontsize=9,
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))
    fig.text(
        0.98, 0.95, f"Last data: {last_valid_date.date()}",
        ha='right', va='top', fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "BSP_Liquidity.png"), bbox_inches='tight')


def chart_india_bonds_activity_and_volumes():
    """India CCIL Net Activity & Volumes + India Rates -- from charts_updater4.py"""
    END_DATE = datetime.today()

    group_map = {
        'CCILFORB Index': 'Foreign Banks',
        'CCILPUBB Index': 'Public Banks',
        'CCILPVTB Index': 'Private Banks',
        'CCILMFND Index': 'Mutual Funds',
        'CCILOTHE Index': 'Others',
        'CCILPDEA Index': 'Primary Dealers',
    }
    volume_map = {
        'CCILOUCG Index': 'IGBs Volume',
        'CCILOUSG Index': 'SDLs Volume',
        'CCILOUTB Index': 'Tbills Volume',
        'CCILOUVL Index': 'Total Volumes',
    }
    yields_map = {
        'GIND10YR Index':      'India 10Y IGB',
        '.INBSS10 U Index':    'IN 10Y Bond-Swap',
        '.IN10S30S Index':     'IGB 10s-30s Curve',
    }

    groups  = list(group_map.keys())
    volumes = list(volume_map.keys())
    yields_ = list(yields_map.keys())

    def bdh_flat(tickers, field, start_dt, end_dt):
        """Bloomberg BDH -> wide DataFrame with ticker columns."""
        df = blp.bdh(tickers, field, start_dt, end_dt)
        if isinstance(df.columns, pd.MultiIndex):
            if field in df.columns.get_level_values(-1):
                df = df.xs(field, level=-1, axis=1)
            else:
                df = df.xs(field, level=0, axis=1)
        df.index = pd.to_datetime(df.index)
        return df

    def prepare_panels(start_dt, end_dt, ma_window):
        """Fetch, clean, and return panel data for a given window and MA length."""
        g = bdh_flat(groups,  "PX_LAST", start_dt, end_dt).ffill(limit=2).rename(columns=group_map)
        v = bdh_flat(volumes, "PX_LAST", start_dt, end_dt).ffill(limit=2).rename(columns=volume_map)
        y = bdh_flat(yields_, "PX_LAST", start_dt, end_dt).ffill(limit=2).rename(columns=yields_map)
        g = g.apply(pd.to_numeric, errors='coerce')
        v = v.apply(pd.to_numeric, errors='coerce')
        y = y.apply(pd.to_numeric, errors='coerce')
        g_ma = g.rolling(window=ma_window, min_periods=1).mean()
        v_ma = v.rolling(window=ma_window, min_periods=1).mean()
        g_total = g_ma.sum(axis=1, min_count=1)
        y10   = y['India 10Y IGB']
        bs10  = y['IN 10Y Bond-Swap']
        curve = y['IGB 10s-30s Curve']
        last_date = min(g_ma.index.max(), v_ma.index.max(), y.index.max())
        return {
            "groups_ma": g_ma,
            "groups_total": g_total,
            "volume_ma": v_ma,
            "y10": y10,
            "bs10": bs10,
            "curve": curve,
            "last_date": last_date,
        }

    START_LONG = datetime(2015, 1, 1)
    START_RECENT = END_DATE - relativedelta(months=6)
    left  = prepare_panels(START_LONG, END_DATE, ma_window=20)
    right = prepare_panels(START_RECENT, END_DATE, ma_window=5)

    fig, axes = plt.subplots(3, 2, figsize=(22, 14), sharex=False)
    (ax_top_L, ax_top_R), (ax_mid_L, ax_mid_R), (ax_bot_L, ax_bot_R) = axes

    def style_axis(ax):
        ax.grid(True, which='major', linestyle=':', linewidth=0.6, color='#dddddd', alpha=0.9)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.tick_params(axis='x', rotation=45, labelsize=9)

    for ax, data, title_suffix in [
        (ax_top_L, left,  "-- 20d MA (since 2015)"),
        (ax_top_R, right, "-- 5d MA (last 6 months)")
    ]:
        g_ma = data["groups_ma"]
        for col in g_ma.columns:
            ax.plot(g_ma.index, g_ma[col], lw=1.6, alpha=0.95, label=col, zorder=2)
        ax.plot(data["groups_total"].index, data["groups_total"], color='black', lw=3.0,
                label=f"Total ({'20d' if ax is ax_top_L else '5d'} MA)", zorder=3)
        ax.axhline(0, color='#999999', lw=1.0, ls='--', zorder=1)
        ax.set_title(f"Net Activity by Group {title_suffix}", fontsize=13)
        ax.set_ylabel("Amount")
        ax.legend(ncol=3, fontsize=8, loc='upper left')
        style_axis(ax)
        ax.text(0.985, 0.01, f"Last: {data['last_date'].date()}",
                transform=ax.transAxes, ha='right', va='bottom',
                fontsize=9, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

    for ax, data, title_suffix in [
        (ax_mid_L, left,  "-- 20d MA (since 2015)"),
        (ax_mid_R, right, "-- 5d MA (last 6 months)")
    ]:
        v_ma = data["volume_ma"]
        for col in v_ma.columns:
            lw = 3.0 if col == 'Total Volumes' else 1.8
            color = 'black' if col == 'Total Volumes' else None
            ax.plot(v_ma.index, v_ma[col], lw=lw, color=color, alpha=0.95, label=col)
        ax.set_title(f"Volumes by Product {title_suffix}", fontsize=13)
        ax.set_ylabel("Volume")
        ax.legend(ncol=3, fontsize=8, loc='upper left')
        style_axis(ax)

    def plot_yields(ax, data, title_suffix):
        ln1, = ax.plot(data["y10"].index, data["y10"], color='tab:blue', lw=2.0, label='India 10Y IGB')
        ax.set_ylabel('10Y IGB', color='tab:blue')
        ax.tick_params(axis='y', labelcolor='tab:blue')
        ax.grid(True, which='major', linestyle=':', linewidth=0.6, color='#dddddd', alpha=0.9)
        ax_r1 = ax.twinx()
        ln2, = ax_r1.plot(data["bs10"].index, data["bs10"], color='tab:orange', lw=2.0, label='IN 10Y Bond-Swap')
        ax_r1.set_ylabel('10Y Bond-Swap', color='tab:orange')
        ax_r1.tick_params(axis='y', labelcolor='tab:orange')
        ax_r2 = ax.twinx()
        ax_r2.spines['right'].set_position(('axes', 1.10))
        ln3, = ax_r2.plot(data["curve"].index, data["curve"], color='tab:green', lw=1.8, ls='--', label='IGB 10s-30s Curve')
        ax_r2.set_ylabel('10s-30s Curve', color='tab:green')
        ax_r2.tick_params(axis='y', labelcolor='tab:green')
        ax.set_title(f"India Rates {title_suffix}", fontsize=13)
        style_axis(ax)
        lines = [ln1, ln2, ln3]
        labels = [l.get_label() for l in lines]
        ax.legend(lines, labels, loc='upper left', fontsize=8)

    plot_yields(ax_bot_L, left,  "-- raw (since 2015)")
    plot_yields(ax_bot_R, right, "-- raw (last 6 months)")
    ax_bot_L.set_xlabel("Date")
    ax_bot_R.set_xlabel("Date")
    fig.suptitle("India CCIL -- Net Activity & Volumes (MA variants) + India Rates", fontsize=16, y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(Path(G_CHART_DIR, "India Bonds Activity & Volumes.png"), bbox_inches='tight')
def chart_gold_vs_thb_hf_correlation():
    """Gold vs THB High Frequency Correlation -- from charts_updater4.py"""
    END_DATE = datetime.today()

    def as_utc_index(idx):
        ts = pd.DatetimeIndex(idx)
        return (ts.tz_localize('UTC') if ts.tz is None else ts.tz_convert('UTC'))

    def extract_close(df, ticker):
        sidx = df.index
        sidx = as_utc_index(sidx)
        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.get_level_values(0):
                close = df[ticker].loc[:, 'close']
            else:
                close = df.xs('close', axis=1, level=-1).iloc[:, 0]
        else:
            close = df['close']
        close = pd.Series(close.values, index=sidx, name=ticker).sort_index()
        close = close[~close.index.duplicated(keep='last')]
        return close

    def pull_intraday_minutes_last_n_days(ticker, n_days, ref, event=None):
        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=n_days - 1)
        dates = pd.date_range(start, end, freq='D')
        parts = []
        for d in dates:
            dt_str = d.strftime('%Y-%m-%d')
            try:
                kwargs = dict(ticker=ticker, dt=dt_str, ref=ref)
                if event is not None:
                    kwargs['event'] = event
                df = blp.bdib(**kwargs)
                if df is None or df.empty:
                    continue
                ser = extract_close(df, ticker)
                if not ser.empty:
                    parts.append(ser)
            except Exception as e:
                print(f"[{ticker}] {dt_str} skipped: {e}")
                continue
        if not parts:
            raise RuntimeError(f"No intraday data returned for {ticker} over last {n_days} days.")
        out = pd.concat(parts).sort_index()
        out.index = as_utc_index(out.index)
        return out.rename(ticker)

    def align_minutes(s1, s2, method='ffill'):
        def to_minute_last(s):
            s = s.copy()
            s.index = s.index.floor('T')
            return s[~s.index.duplicated(keep='last')]
        s1m, s2m = to_minute_last(s1), to_minute_last(s2)
        if method == 'inner':
            df = pd.concat([s1m, s2m], axis=1, join='inner').sort_index()
        else:
            idx = s1m.index.union(s2m.index).sort_values()
            df = pd.concat([s1m.reindex(idx), s2m.reindex(idx)], axis=1)
            df = df.ffill(limit=3).dropna()
        return df

    def last_every_n_minutes(df_minute, n):
        if n <= 0:
            raise ValueError("n must be positive")
        df = df_minute.copy()
        df.index = df.index.floor('T')
        df = df[~df.index.duplicated(keep='last')]
        pos = np.arange(len(df))
        blk_id = pos // n
        out = df.groupby(blk_id, sort=True).tail(1)
        return out

    def rolling_corr(x, y, window):
        pair = pd.concat([x, y], axis=1).dropna()
        r = pair.iloc[:, 0].rolling(window, min_periods=window).corr(pair.iloc[:, 1])
        r.name = 'rolling_corr'
        return r.dropna()

    def rolling_partial_corr_xyz(df_xy_z, xcol, ycol, zcol, window):
        out_vals, out_idx = [], []
        arr = df_xy_z[[xcol, ycol, zcol]].dropna()
        for i in range(window, len(arr) + 1):
            w = arr.iloc[i - window:i]
            c = w.corr()
            r_xy, r_xz, r_yz = c.loc[xcol, ycol], c.loc[xcol, zcol], c.loc[ycol, zcol]
            denom = np.sqrt(max(1e-12, (1 - r_xz**2)) * max(1e-12, (1 - r_yz**2)))
            pc = (r_xy - r_xz * r_yz) / denom
            out_vals.append(pc)
            out_idx.append(w.index[-1])
        return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name=f'pcorr_{xcol}_{ycol}_|_{zcol}')

    def rolling_beta(y_on_x_df, xcol, ycol, window, standardize=False):
        arr = y_on_x_df[[xcol, ycol]].dropna()
        if standardize:
            arr = (arr - arr.rolling(window, min_periods=window).mean()) / \
                  arr.rolling(window, min_periods=window).std(ddof=0)
            arr = arr.dropna()
        out_vals, out_idx = [], []
        for i in range(window, len(arr) + 1):
            w = arr.iloc[i - window:i]
            x = w[xcol]; y = w[ycol]
            vx = x.var()
            beta = (x.cov(y) / vx) if vx > 0 else np.nan
            out_vals.append(beta)
            out_idx.append(w.index[-1])
        return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name=f'beta_{ycol}_on_{xcol}')

    def _last_plotted_timestamp(*series):
        valid = [s.index.max() for s in series if len(s)]
        if not valid:
            return None
        ts = max(valid)
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            ts = ts.tz_localize('UTC')
        return ts

    lookback_days   = 120
    ref_ticker      = 'ES1 Index'
    gold_tkr        = 'XAUUSD Curncy'
    thb_tkr         = 'THB BGN Curncy'
    dxy_tkr         = 'BBDXY Index'
    block_minutes   = 240
    ROLL_WIN_BLOCKS = 30
    STANDARDIZE_BETA = False

    gold_min = pull_intraday_minutes_last_n_days(gold_tkr, lookback_days, ref=ref_ticker)
    thb_min = pull_intraday_minutes_last_n_days(thb_tkr, lookback_days, ref=ref_ticker)
    dxy_min = pull_intraday_minutes_last_n_days(dxy_tkr, lookback_days, ref=ref_ticker)

    px_min_2 = align_minutes(gold_min, thb_min, method='ffill')
    px_min_3 = (
        px_min_2
        .join(dxy_min.reindex(px_min_2.index), how='left')
        .ffill(limit=3)
        .dropna())
    px_min_3.columns = [gold_tkr, thb_tkr, dxy_tkr]
    px_block = last_every_n_minutes(px_min_3, block_minutes)
    ret_blk = np.log(px_block).diff().dropna()
    ren = {gold_tkr: 'XAUUSD', thb_tkr: 'THB', dxy_tkr: 'DXY'}
    ret_blk = ret_blk.rename(columns=ren)
    roll_corr = rolling_corr(ret_blk['XAUUSD'], ret_blk['THB'], ROLL_WIN_BLOCKS)
    roll_pcorr_usd  = rolling_partial_corr_xyz(ret_blk, 'XAUUSD', 'THB', 'DXY', ROLL_WIN_BLOCKS)
    roll_beta_      = rolling_beta(ret_blk, xcol='XAUUSD', ycol='THB',
                                   window=ROLL_WIN_BLOCKS, standardize=STANDARDIZE_BETA)
    last_ts = _last_plotted_timestamp(roll_corr, roll_pcorr_usd, roll_beta_)
    last_sgt = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M %Z') if last_ts is not None else 'n/a'
    last_updated_text = f"Last Updated:{last_sgt} SGT"
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    axes[0].plot(roll_corr.index, roll_corr.values, label='Rolling corr')
    axes[0].axhline(0, lw=1)
    axes[0].set_title(f'5-day Rolling Corr (4H data): {gold_tkr} vs {thb_tkr}')
    axes[0].legend(loc='upper right')
    axes[0].grid(True, alpha=0.3)
    axes[1].plot(roll_pcorr_usd.index, roll_pcorr_usd.values,
                 label='USD-neutral partial corr', color='orange')
    axes[1].axhline(0, lw=1)
    axes[1].set_title('DXY-adjusted rolling partial correlation')
    axes[1].legend(loc='upper right')
    axes[1].grid(True, alpha=0.3)
    beta_label = ('Rolling beta (THB on XAUUSD)'
                  if not STANDARDIZE_BETA else
                  'Rolling beta (standardized)')
    axes[2].plot(roll_beta_.index, roll_beta_.values, label=f'{beta_label}', color='green')
    axes[2].axhline(0, lw=1)
    axes[2].set_title(beta_label)
    axes[2].legend(loc='upper right')
    axes[2].grid(True, alpha=0.3)
    axes[-1].xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=1))
    axes[-1].xaxis.set_major_formatter(DateFormatter('%b %d'))
    fig.text(
        0.99, 0.98, last_updated_text,
        ha='right', va='top',
        bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
        fontsize=9)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Gold vs THB High Frequency Correlation.png"), bbox_inches='tight')


def chart_hscei_vs_cgb30y_hf_correlation():
    """HSCEIvCGB30Y High Frequency Correlation (first instance) -- from charts_updater4.py"""
    END_DATE = datetime.today()

    def as_utc_index(idx):
        ts = pd.DatetimeIndex(idx)
        return (ts.tz_localize('UTC') if ts.tz is None else ts.tz_convert('UTC'))

    def extract_close(df, ticker):
        sidx = df.index
        sidx = as_utc_index(sidx)
        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.get_level_values(0):
                close = df[ticker].loc[:, 'close']
            else:
                close = df.xs('close', axis=1, level=-1).iloc[:, 0]
        else:
            close = df['close']
        close = pd.Series(close.values, index=sidx, name=ticker).sort_index()
        close = close[~close.index.duplicated(keep='last')]
        return close

    def pull_intraday_minutes_last_n_days(ticker, n_days, ref, event=None):
        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=n_days - 1)
        dates = pd.date_range(start, end, freq='D')
        parts = []
        for d in dates:
            dt_str = d.strftime('%Y-%m-%d')
            try:
                kwargs = dict(ticker=ticker, dt=dt_str, ref=ref)
                if event is not None:
                    kwargs['event'] = event
                df = blp.bdib(**kwargs)
                if df is None or df.empty:
                    continue
                ser = extract_close(df, ticker)
                if not ser.empty:
                    parts.append(ser)
            except Exception as e:
                print(f"[{ticker}] {dt_str} skipped: {e}")
                continue
        if not parts:
            raise RuntimeError(f"No intraday data returned for {ticker} over last {n_days} days.")
        out = pd.concat(parts).sort_index()
        out.index = as_utc_index(out.index)
        return out.rename(ticker)

    def align_minutes(s1, s2, method='ffill'):
        def to_minute_last(s):
            s = s.copy()
            s.index = s.index.floor('T')
            return s[~s.index.duplicated(keep='last')]
        s1m, s2m = to_minute_last(s1), to_minute_last(s2)
        if method == 'inner':
            df = pd.concat([s1m, s2m], axis=1, join='inner').sort_index()
        else:
            idx = s1m.index.union(s2m.index).sort_values()
            df = pd.concat([s1m.reindex(idx), s2m.reindex(idx)], axis=1)
            df = df.ffill(limit=3).dropna()
        return df

    def last_every_n_minutes(df_minute, n):
        if n <= 0:
            raise ValueError("n must be positive")
        df = df_minute.copy()
        df.index = df.index.floor('T')
        df = df[~df.index.duplicated(keep='last')]
        pos = np.arange(len(df))
        blk_id = pos // n
        out = df.groupby(blk_id, sort=True).tail(1)
        return out

    def _last_plotted_timestamp(*series):
        valid = [s.index.max() for s in series if len(s)]
        if not valid:
            return None
        ts = max(valid)
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            ts = ts.tz_localize('UTC')
        return ts

    lookback_days = 120
    ref_ticker = 'HSCEI Index'
    hscei_tkr   = 'HC1 Index'
    cgb30y_tkr  = 'TBS1 Comdty'
    hscei_min = pull_intraday_minutes_last_n_days(hscei_tkr, lookback_days, ref=ref_ticker)
    cgb30y_min = pull_intraday_minutes_last_n_days(cgb30y_tkr, lookback_days, ref=ref_ticker)
    block_minutes = 180
    px_min   = align_minutes(hscei_min, cgb30y_min, method='ffill')
    px_min.columns = ['HSCEI', '30Y CGB']
    px_block = last_every_n_minutes(px_min, block_minutes)
    ret_blk = px_block.pct_change()
    window = 20
    roll_corr = (ret_blk['HSCEI']
                 .rolling(window=window, min_periods=window)
                 .corr(ret_blk['30Y CGB']))
    last_ts   = _last_plotted_timestamp(roll_corr)
    last_sgt  = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M') if last_ts is not None else 'n/a'
    last_updated_text = f"Last Updated:{last_sgt} SGT"
    fig, ax = plt.subplots(figsize=(10, 5))
    roll_corr.plot(ax=ax)
    ax.set_title('Rolling 10-Day Correlation (3H data)\nHSCEI Futs vs CGB 30Y Futs')
    ax.set_xlabel('Date')
    ax.set_ylabel('Correlation')
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=1))
    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
    fig.text(
        0.99, 0.98, last_updated_text,
        ha='right', va='top',
        bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
        fontsize=9)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "HSCEIvCGB30Y_HighFrequencyCorrelation.png"), bbox_inches='tight')


def chart_hscei_vs_cgb30y_hf_correlation_2():
    """HSCEIvCGB30Y High Frequency Correlation (second instance) -- from charts_updater4.py"""
    END_DATE = datetime.today()

    def as_utc_index(idx):
        ts = pd.DatetimeIndex(idx)
        return (ts.tz_localize('UTC') if ts.tz is None else ts.tz_convert('UTC'))

    def extract_close(df, ticker):
        sidx = df.index
        sidx = as_utc_index(sidx)
        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.get_level_values(0):
                close = df[ticker].loc[:, 'close']
            else:
                close = df.xs('close', axis=1, level=-1).iloc[:, 0]
        else:
            close = df['close']
        close = pd.Series(close.values, index=sidx, name=ticker).sort_index()
        close = close[~close.index.duplicated(keep='last')]
        return close

    def pull_intraday_minutes_last_n_days(ticker, n_days, ref, event=None):
        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=n_days - 1)
        dates = pd.date_range(start, end, freq='D')
        parts = []
        for d in dates:
            dt_str = d.strftime('%Y-%m-%d')
            try:
                kwargs = dict(ticker=ticker, dt=dt_str, ref=ref)
                if event is not None:
                    kwargs['event'] = event
                df = blp.bdib(**kwargs)
                if df is None or df.empty:
                    continue
                ser = extract_close(df, ticker)
                if not ser.empty:
                    parts.append(ser)
            except Exception as e:
                print(f"[{ticker}] {dt_str} skipped: {e}")
                continue
        if not parts:
            raise RuntimeError(f"No intraday data returned for {ticker} over last {n_days} days.")
        out = pd.concat(parts).sort_index()
        out.index = as_utc_index(out.index)
        return out.rename(ticker)

    def align_minutes(s1, s2, method='ffill'):
        def to_minute_last(s):
            s = s.copy()
            s.index = s.index.floor('T')
            return s[~s.index.duplicated(keep='last')]
        s1m, s2m = to_minute_last(s1), to_minute_last(s2)
        if method == 'inner':
            df = pd.concat([s1m, s2m], axis=1, join='inner').sort_index()
        else:
            idx = s1m.index.union(s2m.index).sort_values()
            df = pd.concat([s1m.reindex(idx), s2m.reindex(idx)], axis=1)
            df = df.ffill(limit=3).dropna()
        return df

    def last_every_n_minutes(df_minute, n):
        if n <= 0:
            raise ValueError("n must be positive")
        df = df_minute.copy()
        df.index = df.index.floor('T')
        df = df[~df.index.duplicated(keep='last')]
        pos = np.arange(len(df))
        blk_id = pos // n
        out = df.groupby(blk_id, sort=True).tail(1)
        return out

    def _last_plotted_timestamp(*series):
        valid = [s.index.max() for s in series if len(s)]
        if not valid:
            return None
        ts = max(valid)
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            ts = ts.tz_localize('UTC')
        return ts

    lookback_days = 120
    ref_ticker = 'HSCEI Index'
    hscei_tkr   = 'HC1 Index'
    cgb30y_tkr  = 'TBS1 Comdty'
    hscei_min = pull_intraday_minutes_last_n_days(hscei_tkr, lookback_days, ref=ref_ticker)
    cgb30y_min = pull_intraday_minutes_last_n_days(cgb30y_tkr, lookback_days, ref=ref_ticker)
    block_minutes = 180
    px_min   = align_minutes(hscei_min, cgb30y_min, method='ffill')
    px_min.columns = ['HSCEI', '30Y CGB']
    px_block = last_every_n_minutes(px_min, block_minutes)
    ret_blk = px_block.pct_change()
    window = 20
    roll_corr = (ret_blk['HSCEI']
                 .rolling(window=window, min_periods=window)
                 .corr(ret_blk['30Y CGB']))
    last_ts   = _last_plotted_timestamp(roll_corr)
    last_sgt  = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M') if last_ts is not None else 'n/a'
    last_updated_text = f"Last Updated:{last_sgt} SGT"
    fig, ax = plt.subplots(figsize=(10, 5))
    roll_corr.plot(ax=ax)
    ax.set_title('Rolling 10-Day Correlation (3H data)\nHSCEI Futs vs CGB 30Y Futs')
    ax.set_xlabel('Date')
    ax.set_ylabel('Correlation')
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=1))
    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
    fig.text(
        0.99, 0.98, last_updated_text,
        ha='right', va='top',
        bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
        fontsize=9)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "HSCEIvCGB30Y_HighFrequencyCorrelation.png"), bbox_inches='tight')
def chart_usdcnh_vs_audusd_hf_correlation():
    """USDCNH vs AUDUSD High Frequency Correlation -- from charts_updater4.py"""
    END_DATE = datetime.today()

    def as_utc_index(idx):
        ts = pd.DatetimeIndex(idx)
        return (ts.tz_localize('UTC') if ts.tz is None else ts.tz_convert('UTC'))

    def extract_close(df, ticker):
        sidx = df.index
        sidx = as_utc_index(sidx)
        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.get_level_values(0):
                close = df[ticker].loc[:, 'close']
            else:
                close = df.xs('close', axis=1, level=-1).iloc[:, 0]
        else:
            close = df['close']
        close = pd.Series(close.values, index=sidx, name=ticker).sort_index()
        close = close[~close.index.duplicated(keep='last')]
        return close

    def pull_intraday_minutes_last_n_days(ticker, n_days, ref, event=None):
        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=n_days - 1)
        dates = pd.date_range(start, end, freq='D')
        parts = []
        for d in dates:
            dt_str = d.strftime('%Y-%m-%d')
            try:
                kwargs = dict(ticker=ticker, dt=dt_str, ref=ref)
                if event is not None:
                    kwargs['event'] = event
                df = blp.bdib(**kwargs)
                if df is None or df.empty:
                    continue
                ser = extract_close(df, ticker)
                if not ser.empty:
                    parts.append(ser)
            except Exception as e:
                print(f"[{ticker}] {dt_str} skipped: {e}")
                continue
        if not parts:
            raise RuntimeError(f"No intraday data returned for {ticker} over last {n_days} days.")
        out = pd.concat(parts).sort_index()
        out.index = as_utc_index(out.index)
        return out.rename(ticker)

    def align_minutes(s1, s2, method='ffill'):
        def to_minute_last(s):
            s = s.copy()
            s.index = s.index.floor('T')
            return s[~s.index.duplicated(keep='last')]
        s1m, s2m = to_minute_last(s1), to_minute_last(s2)
        if method == 'inner':
            df = pd.concat([s1m, s2m], axis=1, join='inner').sort_index()
        else:
            idx = s1m.index.union(s2m.index).sort_values()
            df = pd.concat([s1m.reindex(idx), s2m.reindex(idx)], axis=1)
            df = df.ffill(limit=3).dropna()
        return df

    def last_every_n_minutes(df_minute, n):
        if n <= 0:
            raise ValueError("n must be positive")
        df = df_minute.copy()
        df.index = df.index.floor('T')
        df = df[~df.index.duplicated(keep='last')]
        pos = np.arange(len(df))
        blk_id = pos // n
        out = df.groupby(blk_id, sort=True).tail(1)
        return out

    def rolling_corr(x, y, window):
        pair = pd.concat([x, y], axis=1).dropna()
        r = pair.iloc[:, 0].rolling(window, min_periods=window).corr(pair.iloc[:, 1])
        r.name = 'rolling_corr'
        return r.dropna()

    def rolling_partial_corr_xyz(df_xy_z, xcol, ycol, zcol, window):
        out_vals, out_idx = [], []
        arr = df_xy_z[[xcol, ycol, zcol]].dropna()
        for i in range(window, len(arr) + 1):
            w = arr.iloc[i - window:i]
            c = w.corr()
            r_xy, r_xz, r_yz = c.loc[xcol, ycol], c.loc[xcol, zcol], c.loc[ycol, zcol]
            denom = np.sqrt(max(1e-12, (1 - r_xz**2)) * max(1e-12, (1 - r_yz**2)))
            pc = (r_xy - r_xz * r_yz) / denom
            out_vals.append(pc)
            out_idx.append(w.index[-1])
        return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name=f'pcorr_{xcol}_{ycol}_|_{zcol}')

    def rolling_beta(y_on_x_df, xcol, ycol, window, standardize=False):
        arr = y_on_x_df[[xcol, ycol]].dropna()
        if standardize:
            arr = (arr - arr.rolling(window, min_periods=window).mean()) / \
                  arr.rolling(window, min_periods=window).std(ddof=0)
            arr = arr.dropna()
        out_vals, out_idx = [], []
        for i in range(window, len(arr) + 1):
            w = arr.iloc[i - window:i]
            x = w[xcol]; y = w[ycol]
            vx = x.var()
            beta = (x.cov(y) / vx) if vx > 0 else np.nan
            out_vals.append(beta)
            out_idx.append(w.index[-1])
        return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name=f'beta_{ycol}_on_{xcol}')

    def _last_plotted_timestamp(*series):
        valid = [s.index.max() for s in series if len(s)]
        if not valid:
            return None
        ts = max(valid)
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            ts = ts.tz_localize('UTC')
        return ts

    lookback_days     = 120
    block_minutes     = 240
    ROLL_WIN_BLOCKS   = 60
    STANDARDIZE_BETA  = False

    ref_ticker = 'ES1 Index'
    dxy_tkr    = 'BBDXY Index'
    cnh_tkr = 'USDCNH Curncy'
    aud_tkr = 'AUDUSD Curncy'

    cnh_min = pull_intraday_minutes_last_n_days(cnh_tkr, lookback_days, ref=ref_ticker)
    aud_min = pull_intraday_minutes_last_n_days(aud_tkr, lookback_days, ref=ref_ticker)
    dxy_min = pull_intraday_minutes_last_n_days(dxy_tkr, lookback_days, ref=ref_ticker)

    px_min_2 = align_minutes(cnh_min, aud_min, method='ffill')
    px_min_3 = (
        px_min_2
        .join(dxy_min.reindex(px_min_2.index), how='left')
        .ffill(limit=3)
        .dropna()
    )
    px_min_3.columns = [cnh_tkr, aud_tkr, dxy_tkr]

    px_block = last_every_n_minutes(px_min_3, block_minutes)
    ret_blk  = np.log(px_block).diff().dropna()

    ren = {cnh_tkr: 'USDCNH', aud_tkr: 'AUDUSD', dxy_tkr: 'DXY'}
    ret_blk = ret_blk.rename(columns=ren)

    roll_corr       = rolling_corr(ret_blk['USDCNH'], ret_blk['AUDUSD'], ROLL_WIN_BLOCKS)
    roll_pcorr_usd  = rolling_partial_corr_xyz(ret_blk, 'USDCNH', 'AUDUSD', 'DXY', ROLL_WIN_BLOCKS)
    roll_beta_      = rolling_beta(ret_blk, xcol='USDCNH', ycol='AUDUSD',
                                   window=ROLL_WIN_BLOCKS, standardize=STANDARDIZE_BETA)

    last_ts   = _last_plotted_timestamp(roll_corr, roll_pcorr_usd, roll_beta_)
    last_sgt  = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M %Z') if last_ts is not None else 'n/a'
    last_info = f"Last Updated: {last_sgt} SGT"

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    axes[0].plot(roll_corr.index, roll_corr.values, label='Rolling corr (USDCNH vs AUDUSD)')
    axes[0].axhline(0, lw=1)
    axes[0].set_title('5-day Rolling Corr (4H data): USDCNH vs AUDUSD')
    axes[0].legend(loc='upper right')
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(roll_pcorr_usd.index, roll_pcorr_usd.values,
                 label='USD-neutral partial corr', color='orange')
    axes[1].axhline(0, lw=1)
    axes[1].set_title('DXY-adjusted rolling partial correlation')
    axes[1].legend(loc='upper right')
    axes[1].grid(True, alpha=0.3)

    beta_label = ('Rolling beta (AUDUSD on USDCNH)' if not STANDARDIZE_BETA
                  else 'Rolling beta (standardized)')
    axes[2].plot(roll_beta_.index, roll_beta_.values, label=beta_label, color='green')
    axes[2].axhline(0, lw=1)
    axes[2].set_title(beta_label)
    axes[2].legend(loc='upper right')
    axes[2].grid(True, alpha=0.3)

    axes[-1].xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=1))
    axes[-1].xaxis.set_major_formatter(DateFormatter('%b %d'))
    plt.setp(axes[-1].get_xticklabels(), rotation=0)

    fig.text(0.99, 0.98, last_info,
             ha='right', va='top',
             bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
             fontsize=9)

    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "USDCNH_vs_AUDUSD_High_Frequency_Correlation.png"), bbox_inches='tight')


def chart_xauusd_vs_es_km_aud_intraday_correlation():
    """Intraday Rolling 2H Correlation: XAUUSD vs ESA / KMA / AUDUSD -- from charts_updater4.py"""
    END_DATE = datetime.today()

    def as_utc_index(idx):
        ts = pd.DatetimeIndex(idx)
        return (ts.tz_localize('UTC') if ts.tz is None else ts.tz_convert('UTC'))

    def extract_close(df, ticker):
        sidx = df.index
        sidx = as_utc_index(sidx)
        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.get_level_values(0):
                close = df[ticker].loc[:, 'close']
            else:
                close = df.xs('close', axis=1, level=-1).iloc[:, 0]
        else:
            close = df['close']
        close = pd.Series(close.values, index=sidx, name=ticker).sort_index()
        close = close[~close.index.duplicated(keep='last')]
        return close

    def pull_intraday_minutes_last_n_days(ticker, n_days, ref, event=None):
        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=n_days - 1)
        dates = pd.date_range(start, end, freq='D')
        parts = []
        for d in dates:
            dt_str = d.strftime('%Y-%m-%d')
            try:
                kwargs = dict(ticker=ticker, dt=dt_str, ref=ref)
                if event is not None:
                    kwargs['event'] = event
                df = blp.bdib(**kwargs)
                if df is None or df.empty:
                    continue
                ser = extract_close(df, ticker)
                if not ser.empty:
                    parts.append(ser)
            except Exception as e:
                print(f"[{ticker}] {dt_str} skipped: {e}")
                continue
        if not parts:
            raise RuntimeError(f"No intraday data returned for {ticker} over last {n_days} days.")
        out = pd.concat(parts).sort_index()
        out.index = as_utc_index(out.index)
        return out.rename(ticker)

    def last_every_n_minutes(df_minute, n):
        if n <= 0:
            raise ValueError("n must be positive")
        df = df_minute.copy()
        df.index = df.index.floor('T')
        df = df[~df.index.duplicated(keep='last')]
        pos = np.arange(len(df))
        blk_id = pos // n
        out = df.groupby(blk_id, sort=True).tail(1)
        return out

    def rolling_corr_time_capped(x, y, window, max_td):
        pair = pd.concat([x, y], axis=1).dropna()
        out_vals, out_idx = [], []
        min_periods = window // 2
        for i in range(window, len(pair) + 1):
            w = pair.iloc[i - window:i]
            span = w.index[-1] - w.index[0]
            if span > max_td:
                cutoff = w.index[-1] - max_td
                w = w[w.index >= cutoff]
            corr = w.iloc[:, 0].corr(w.iloc[:, 1]) if len(w) >= min_periods else np.nan
            out_vals.append(corr)
            out_idx.append(pair.index[i - 1])
        return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name='rolling_corr')

    def _last_plotted_timestamp(*series):
        valid = [s.index.max() for s in series if len(s)]
        if not valid:
            return None
        ts = max(valid)
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            ts = ts.tz_localize('UTC')
        return ts

    def _setup_intraday_corr_ax(ax, corr_sub):
        sgt_idx    = corr_sub.index
        pos        = np.arange(len(corr_sub))
        dates_only = sgt_idx.normalize()
        ax.plot(pos, corr_sub['XAUUSD vs ESA'].values,
                label='XAUUSD vs ESA',    color='tab:blue')
        ax.plot(pos, corr_sub['XAUUSD vs KMA'].values,
                label='XAUUSD vs KMA',    color='tab:orange')
        ax.plot(pos, corr_sub['XAUUSD vs AUDUSD'].values,
                label='XAUUSD vs AUDUSD', color='tab:green')
        ax.axhline(0, color='gray', lw=1, linestyle='--')
        unique_dates = dates_only.unique()
        for d in unique_dates[1:]:
            first_pos = pos[dates_only == d][0]
            ax.axvline(first_pos, color='lightgrey', lw=0.8, zorder=0)
        major_pos = [int(pos[dates_only == d][0]) for d in unique_dates]
        major_lbl = [d.strftime('%b %d') for d in unique_dates]
        ax.set_xticks(major_pos)
        ax.set_xticklabels(major_lbl, fontsize=9, rotation=45, ha='right')
        minor_pos, minor_lbl, _prev_key = [], [], None
        for i, t in enumerate(sgt_idx):
            key = (t.date(), t.hour // 2 * 2)
            if key != _prev_key:
                minor_pos.append(int(pos[i]))
                minor_lbl.append(f"{key[1]:02d}:00")
                _prev_key = key
        ax.set_xticks(minor_pos, minor=True)
        ax.set_xticklabels(minor_lbl, fontsize=7, rotation=45, ha='right', minor=True)
        ax.tick_params(axis='x', which='major', pad=15)
        ax.set_ylabel('Correlation')
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)
        ax.set_xlabel('Time (SGT)')

    lookback_days   = 10
    ref_ticker      = 'ES1 Index'
    gold_tkr        = 'XAUUSD Curncy'
    es_tkr          = 'ESA Index'
    km_tkr          = 'KMA Index'
    aud_tkr         = 'AUDUSD Curncy'
    block_minutes   = 3
    ROLL_WIN        = 40
    MAX_SPAN        = pd.Timedelta(minutes=135)

    gold_min = pull_intraday_minutes_last_n_days(gold_tkr, lookback_days, ref=ref_ticker)
    es_min   = pull_intraday_minutes_last_n_days(es_tkr,   lookback_days, ref=ref_ticker)
    km_min   = pull_intraday_minutes_last_n_days(km_tkr,   lookback_days, ref=ref_ticker)
    aud_min  = pull_intraday_minutes_last_n_days(aud_tkr,  lookback_days, ref=ref_ticker)

    def _to_minute_last(s):
        s = s.copy()
        s.index = s.index.floor('T')
        return s[~s.index.duplicated(keep='last')]

    _series = [gold_min, es_min, km_min, aud_min]
    _cleaned = [_to_minute_last(s) for s in _series]
    _idx = _cleaned[0].index
    for _s in _cleaned[1:]:
        _idx = _idx.union(_s.index)
    _idx = _idx.sort_values()
    px_min = pd.concat([s.reindex(_idx) for s in _cleaned], axis=1)
    px_min = px_min.ffill(limit=3).dropna()
    px_min.columns = [gold_tkr, es_tkr, km_tkr, aud_tkr]

    px_block = last_every_n_minutes(px_min, block_minutes)
    ret_blk  = np.log(px_block).diff().dropna()

    ren = {gold_tkr: 'XAUUSD', es_tkr: 'ESA', km_tkr: 'KMA', aud_tkr: 'AUDUSD'}
    ret_blk = ret_blk.rename(columns=ren)

    roll_corr_es  = rolling_corr_time_capped(ret_blk['XAUUSD'], ret_blk['ESA'],    ROLL_WIN, MAX_SPAN)
    roll_corr_km  = rolling_corr_time_capped(ret_blk['XAUUSD'], ret_blk['KMA'],    ROLL_WIN, MAX_SPAN)
    roll_corr_aud = rolling_corr_time_capped(ret_blk['XAUUSD'], ret_blk['AUDUSD'], ROLL_WIN, MAX_SPAN)

    corr_df = pd.DataFrame({
        'XAUUSD vs ESA':    roll_corr_es,
        'XAUUSD vs KMA':    roll_corr_km,
        'XAUUSD vs AUDUSD': roll_corr_aud,
    })
    corr_df.index = corr_df.index.tz_convert('Asia/Singapore')
    corr_df = corr_df.dropna(how='all')

    last_ts  = _last_plotted_timestamp(roll_corr_es, roll_corr_km, roll_corr_aud)
    last_sgt = (last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M %Z')
                if last_ts is not None else 'n/a')
    last_updated_text = f"Last Updated: {last_sgt} SGT"

    fig, (ax_full, ax_zoom) = plt.subplots(2, 1, figsize=(14, 10))

    _setup_intraday_corr_ax(ax_full, corr_df)
    ax_full.set_title('Intraday Rolling 2H Correlation (3-min bars): XAUUSD vs ESA / KMA / AUDUSD')

    _last_2_dates = corr_df.index.normalize().unique()[-2:]
    corr_zoom = corr_df[corr_df.index.normalize() >= _last_2_dates[0]]
    _setup_intraday_corr_ax(ax_zoom, corr_zoom)
    ax_zoom.set_title('Last 2 Days (Magnified)')

    fig.text(
        0.99, 0.99, last_updated_text,
        ha='right', va='top',
        bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
        fontsize=9)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "XAUUSD_vs_ES_KM_AUD_Intraday_Correlation.png"), bbox_inches='tight')



# --- Charts from charts_updater5.py ---


def chart_dm_asia_rates_beta():
    """DM/Asia Rates Beta -- from charts_updater5.py"""
    END_DATE = datetime.today()

    TENOR         = '5'
    FIELD         = 'PX_LAST'
    MIN_MOVE      = 0.003
    ROLL_WIN      = 22
    TZ            = 'Asia/Singapore'
    END_TS        = pd.Timestamp.now(tz=TZ).normalize()
    END           = END_TS.date()
    START_BAR     = (END_TS - pd.DateOffset(months=3) - pd.Timedelta(days=3)).date()
    START_ROLL    = (END_TS - relativedelta(years=1) - pd.Timedelta(days=5)).date()

    try:
        OUT_DIR = Path(G_CHART_DIR)
    except Exception:
        OUT_DIR = Path.cwd()

    DM_RATES_5Y = {
        'EUR': 'EUSA5 CMPT Curncy',
        'GBP': 'BPSWS5 CMPT Curncy',
        'CAD': 'CDSO5 CMPT Curncy',
        'CHF': 'SFSNT5 CMPT Curncy',
        'NZD': 'NDSWAP5 CMPT Curncy',
        'AUD': 'ADSWAP5 CMPT Curncy',
        'JPY': 'JYSO5 CMPT Curncy',
    }
    ASIA_RATES_5Y = {
        'MYR': 'MRSWNI5 CMPT Curncy',
        'HKD': 'HDSW5 CMPT Curncy',
        'THB': 'TBSWH5 BGNT Curncy',
        'CNH': 'CCSWO5 CMPT Curncy',
        'INR': 'IRSWNI5 CMPT Curncy',
        'SGD': 'SDSOA5 CMPT Curncy',
        'IDR': 'GTIDR5YR @BGN Corp',
        'TWD': 'TDSWNI5 CMPT Curncy',
        'KRW': 'KWSWNI5 CMPT Curncy',
    }

    def sofr_ticker(tenor):
        return f'USOSFR{tenor} CMPT Curncy'

    DM_RATES   = DM_RATES_5Y
    ASIA_RATES = ASIA_RATES_5Y

    def bdh_flat(tickers, field, start_date, end_date):
        out = blp.bdh(tickers=list(tickers), flds=[field], start_date=start_date, end_date=end_date)
        if isinstance(out.columns, pd.MultiIndex):
            out.columns = [c[0] for c in out.columns]
        out.index = pd.to_datetime(out.index)
        return out.sort_index()

    def align_and_diff(sofr, locals_df, shift_sofr_days=0):
        x = sofr.rename('SOFR').to_frame().join(locals_df, how='inner').dropna()
        x = x.asfreq('B').interpolate(limit_area='inside')
        x['d_SOFR_raw'] = x['SOFR'].diff()
        if shift_sofr_days != 0:
            x['d_SOFR'] = x['d_SOFR_raw'].shift(shift_sofr_days)
        else:
            x['d_SOFR'] = x['d_SOFR_raw']
        for c in locals_df.columns:
            x[f'd_{c}'] = x[c].diff()
        x = x.dropna()
        mask_small = x['d_SOFR'].abs() < MIN_MOVE
        drop_cols = ['d_SOFR'] + [f'd_{c}' for c in locals_df.columns]
        x.loc[mask_small, drop_cols] = np.nan
        x = x.dropna()
        return x

    def extract_source(tkr):
        m = re.search(r'\b(CMPT|CMPN|CMPL)\b', tkr)
        return m.group(1) if m else None

    def sofr_ticker_for_source(tenor, source):
        return f"USOSFR{tenor} {source} Curncy"

    def ensure_2d(df_or_ser, col_name=None):
        if isinstance(df_or_ser, pd.Series):
            return df_or_ser.to_frame(name=col_name or df_or_ser.name or 'value')
        return df_or_ser

    def group_region_by_source(region_map, default_source="CMPT"):
        groups = {}
        for ccy, tkr in region_map.items():
            src = extract_source(tkr) or default_source
            groups.setdefault(src, {})[ccy] = tkr
        return groups

    def directional_beta(sub, colx, coly, direction):
        if direction == 'up':
            sl = sub[sub[colx] > 0.0]
        else:
            sl = sub[sub[colx] < 0.0]
        if sl.empty or np.isclose(sl[colx].sum(), 0.0):
            return np.nan
        return sl[coly].sum() / sl[colx].sum()

    def compute_bar_betas(region_map, shift_sofr_days=0):
        rows = []
        by_src = group_region_by_source(region_map)
        for src, submap in by_src.items():
            if not submap:
                continue
            x_tkr = sofr_ticker_for_source(TENOR, src)
            x = bdh_flat([x_tkr], FIELD, START_BAR, END)
            x = ensure_2d(x, col_name=x_tkr).rename(columns={x.columns[0]: 'SOFR'})
            y = bdh_flat(submap.values(), FIELD, START_BAR, END)
            y = ensure_2d(y)
            tick_to_ccy = {v: k for k, v in submap.items()}
            y = y.rename(columns=tick_to_ccy)
            df = align_and_diff(x['SOFR'], y, shift_sofr_days=shift_sofr_days)
            for ccy in y.columns:
                b_up = directional_beta(df, 'd_SOFR', f'd_{ccy}', 'up')
                b_dn = directional_beta(df, 'd_SOFR', f'd_{ccy}', 'down')
                rows.append({'ccy': ccy, 'beta_up': b_up, 'beta_down': b_dn})
        res = pd.DataFrame(rows).set_index('ccy')
        res = res.reindex(list(region_map.keys()))
        return res

    def rolling_directional_betas(region_map, shift_sofr_days=0):
        out = {}
        by_src = group_region_by_source(region_map)
        for src, submap in by_src.items():
            if not submap:
                continue
            x_tkr = sofr_ticker_for_source(TENOR, src)
            x = bdh_flat([x_tkr], FIELD, START_ROLL, END)
            x = ensure_2d(x, col_name=x_tkr).rename(columns={x.columns[0]: 'SOFR'})
            y = bdh_flat(submap.values(), FIELD, START_ROLL, END)
            y = ensure_2d(y)
            tick_to_ccy = {v: k for k, v in submap.items()}
            y = y.rename(columns=tick_to_ccy)
            df = align_and_diff(x['SOFR'], y, shift_sofr_days=shift_sofr_days)
            N = len(df)
            if N < ROLL_WIN:
                continue
            for ccy in y.columns:
                colx, coly = 'd_SOFR', f'd_{ccy}'
                up_vals, dn_vals, idx = [], [], []
                for i in range(ROLL_WIN, N + 1):
                    sub = df.iloc[i - ROLL_WIN:i]
                    up = sub[sub[colx] > 0]
                    dn = sub[sub[colx] < 0]
                    b_up = (up[coly].sum() / up[colx].sum()) if (not up.empty and not np.isclose(up[colx].sum(), 0.0)) else np.nan
                    b_dn = (dn[coly].sum() / dn[colx].sum()) if (not dn.empty and not np.isclose(dn[colx].sum(), 0.0)) else np.nan
                    up_vals.append(b_up); dn_vals.append(b_dn); idx.append(sub.index[-1])
                out[ccy] = pd.DataFrame({'beta_up': up_vals, 'beta_down': dn_vals}, index=pd.Index(idx, name='Date'))
        return out

    def last_roll_date(roll_dict):
        return max(
            (df.index.max() for df in roll_dict.values() if df is not None and not df.empty)
        )

    def style_time_axis(ax):
        ax.grid(True, alpha=0.8, linestyle=':', linewidth=0.8, color='#aaaaaa')
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_fontsize(8)

    def plot_region_grid(region_name, bar_df, roll_dict,
                         ncols, fig_size=(12, 12), save_name="out.png"):
        currencies = list(bar_df.index)
        n_roll = len(currencies)
        nrows_roll = int(np.ceil(n_roll / ncols))
        total_rows = 1 + nrows_roll
        fig = plt.figure(figsize=fig_size)
        gs = fig.add_gridspec(total_rows, ncols, hspace=0.45, wspace=0.25)
        ax_bar = fig.add_subplot(gs[0, :])
        xpos = np.arange(len(currencies))
        w = 0.35
        ax_bar.bar(xpos - w/2, bar_df['beta_up'],   width=w, label='Beta-up',   color='tab:blue')
        ax_bar.bar(xpos + w/2, bar_df['beta_down'], width=w, label='Beta-down', color='tab:red')
        ax_bar.set_xticks(xpos)
        ax_bar.set_xticklabels(currencies, rotation=0)
        ax_bar.set_ylabel('Beta')
        ax_bar.set_title(f"{region_name} {TENOR}Y vs US SOFR {TENOR}Y -- Directional Betas (last ~3m)")
        ax_bar.axhline(0, color='grey', lw=0.8)
        ax_bar.grid(True, axis='y', alpha=0.8, linestyle=':', linewidth=0.8, color='#aaaaaa')
        ax_bar.legend(loc='upper left', fontsize=8)
        for i, (bup, bdn) in enumerate(zip(bar_df['beta_up'], bar_df['beta_down'])):
            if pd.notna(bup):
                ax_bar.text(i - w/2, bup, f"{bup:.2f}", ha='center', va='bottom', fontsize=8)
            if pd.notna(bdn):
                ax_bar.text(i + w/2, bdn, f"{bdn:.2f}", ha='center', va='bottom', fontsize=8)
        axes_roll = []
        for k, ccy in enumerate(currencies):
            r = 1 + (k // ncols)
            c = k % ncols
            ax = fig.add_subplot(gs[r, c])
            roll = roll_dict.get(ccy)
            if roll is None or roll.empty:
                ax.set_title(f"{ccy}: (no data)")
                ax.axis('off')
            else:
                ax.plot(roll.index, roll['beta_up'],   color='tab:blue', lw=1.6, label='Beta-up')
                ax.plot(roll.index, roll['beta_down'], color='tab:red',  lw=1.6, label='Beta-down')
                ax.axhline(0, color='grey', lw=0.8)
                ax.set_title(f"{ccy} -- Rolling 22d")
                ax.set_ylabel("B")
                style_time_axis(ax)
                if k == 0:
                    ax.legend(loc='upper left', fontsize=8)
            axes_roll.append(ax)
        last_row_axes = [ax for i, ax in enumerate(axes_roll) if (i // ncols) == (nrows_roll - 1)]
        for ax in last_row_axes:
            ax.set_xlabel("Date")
        try:
            last_dt = last_roll_date(roll_dict)
            fig.text(
                0.985, 0.92, f"Last updated: {pd.Timestamp(last_dt).date()}",
                ha='right', va='top', fontsize=9,
                bbox=dict(facecolor='white', edgecolor='black', boxstyle='round')
            )
        except ValueError:
            pass
        fig.tight_layout()
        fig.savefig(OUT_DIR / save_name, bbox_inches='tight')
        plt.close(fig)

    dm_bar   = compute_bar_betas(DM_RATES, shift_sofr_days=0)
    dm_roll  = rolling_directional_betas(DM_RATES, shift_sofr_days=0)
    asia_bar = compute_bar_betas(ASIA_RATES, shift_sofr_days=0)
    asia_roll= rolling_directional_betas(ASIA_RATES, shift_sofr_days=0)
    plot_region_grid(
        "DM", dm_bar, dm_roll,
        ncols=2, fig_size=(12, 15),
        save_name=f"DM_{TENOR}Y_vs_SOFR_rolling_beta.png")
    plot_region_grid(
        "Asia", asia_bar, asia_roll,
        ncols=3, fig_size=(15, 15),
        save_name=f"Asia_{TENOR}Y_vs_SOFR_rolling_beta.png")


def chart_us_inventories_tracker():
    """US Inventories Tracker -- from charts_updater5.py"""
    END_DATE = datetime.today()

    retail_inventories_tickers = [
        'RSRSTOTL Index', 'RSRSMOTV Index', 'RSRSFURN Index',
        'RSRSBUIL Index', 'RSRSFOOD Index', 'RSRSCLOT Index', 'RSRSGENR Index',
    ]
    wholesale_inventories_tickers = [
        'MWINDRBL Index', 'MWINNDRB Index', 'MWINTOT Index',
    ]
    soft_data_current_tickers = [
        'NAPMINV Index', 'EMPRINVT Index', 'OUTFIVF Index',
        'KCLSIFIN Index', 'TROSINIX Index', 'RCHSILFG Index',
    ]
    soft_data_expectations_tickers = [
        'KC6SIFIN Index', 'OUMFIVF Index', 'TROSIFIX Index',
        'RC6SEMFG Index', 'EMPR6INV Index',
    ]
    label_map = {
        'RSRSTOTL Index': 'Retail Inventories (Total)',
        'RSRSMOTV Index': 'Motor Vehicles & Parts',
        'RSRSFURN Index': 'Furniture',
        'RSRSBUIL Index': 'Building Materials',
        'RSRSFOOD Index': 'Food & Beverage',
        'RSRSCLOT Index': 'Clothing',
        'RSRSGENR Index': 'General Merchandise',
        'MWINDRBL Index': 'Wholesale Durable',
        'MWINNDRB Index': 'Wholesale Nondurable',
        'MWINTOT Index':  'Wholesale Total',
    }
    label_short_map = {
        'NAPMINV Index':  'ISM Mfg Inv',
        'EMPRINVT Index': 'Empire State Inv',
        'OUTFIVF Index':  'Philadelphia Fed Inv',
        'KCLSIFIN Index': 'Kansas Fed Inv',
        'TROSINIX Index': 'Dallas Fed Retail Inv',
        'RCHSILFG Index': 'Richmond Fed Inv',
        'KC6SIFIN Index': 'Kansas Fed Inv',
        'OUMFIVF Index':  'Philadelphia Fed Inv',
        'TROSIFIX Index': 'Dallas Fed Retail Inv',
        'RC6SEMFG Index': 'Richmond Fed Inv',
        'EMPR6INV Index': 'Empire State Inv',
    }

    FIELD       = "PX_LAST"
    START_DATE  = datetime(2018, 1, 1)
    SAVE_PATH   = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
    OUTFILE     = SAVE_PATH / "US_Inventories_Tracker.png"

    def _last_data_date_df(df):
        if df is None or df.empty:
            return pd.NaT
        kept = df.dropna(how="all")
        return kept.index.max() if not kept.empty else pd.NaT

    def _last_data_date_series(s):
        if s is None or s.empty:
            return pd.NaT
        kept = s.dropna()
        return kept.index.max() if not kept.empty else pd.NaT

    def bdh_flat(tickers, field=FIELD, start=START_DATE, end=END_DATE):
        df = blp.bdh(tickers=list(tickers), flds=[field], start_date=start, end_date=end)
        if isinstance(df.columns, pd.MultiIndex):
            if field in df.columns.get_level_values(-1):
                df = df.xs(field, level=-1, axis=1)
            else:
                df = df.xs(field, level=0, axis=1)
        df.index = pd.to_datetime(df.index)
        return df.sort_index()

    def monthly_series(df):
        m = df.resample('M').last().ffill(limit=2)
        return m.apply(pd.to_numeric, errors='coerce')

    def compute_3m3m_pct(df_m):
        sum3 = df_m.rolling(3, min_periods=3).sum()
        return sum3.pct_change(3) * 100.0

    def plot_soft_current_ISM_vs_fedavg(ax, df_m, fed_avg_series, title,
                                        label_ISM, label_FED_NOW, color_by_label,
                                        lhs_ticker='NAPMINV Index'):
        if lhs_ticker not in df_m.columns:
            lhs_ticker = df_m.columns[0]
        ln_lhs, = ax.plot(df_m.index, df_m[lhs_ticker], lw=1.9,
                          color=color_by_label[label_ISM], label=f"{label_ISM} (lhs)")
        ax.set_ylim(25, 75)
        ax.axhline(0, color='grey', lw=0.9, ls='--')
        ax.set_title(title, fontsize=12)
        ax.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
        ax2 = ax.twinx()
        ln_rhs, = ax2.plot(fed_avg_series.index, fed_avg_series, lw=2.0,
                           color=color_by_label[label_FED_NOW], label=f"{label_FED_NOW} (rhs)")
        ax2.set_ylim(-25, 25)
        ax.legend([ln_lhs, ln_rhs], [ln_lhs.get_label(), ln_rhs.get_label()],
                  loc='upper left', fontsize=8, ncol=2)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.tick_params(axis='x', rotation=45)

    def plot_soft_expectations_fedavg(ax, fed_avg_series, title, label_FED_EXP, color_by_label):
        ln, = ax.plot(fed_avg_series.index, fed_avg_series, lw=2.0,
                      color=color_by_label[label_FED_EXP], label=label_FED_EXP)
        ax.axhline(0, color='grey', lw=0.9, ls='--')
        ax.set_title(title, fontsize=12)
        ax.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
        ax.legend([ln], [ln.get_label()], loc='upper left', fontsize=8, ncol=1)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.tick_params(axis='x', rotation=45)

    retail_raw    = bdh_flat(retail_inventories_tickers)
    wholesale_raw = bdh_flat(wholesale_inventories_tickers)
    soft_now_raw  = bdh_flat(soft_data_current_tickers)
    soft_exp_raw  = bdh_flat(soft_data_expectations_tickers)

    retail_m    = monthly_series(retail_raw)
    wholesale_m = monthly_series(wholesale_raw)
    soft_now_m  = monthly_series(soft_now_raw)
    soft_exp_m  = monthly_series(soft_exp_raw)

    retail_3m3m    = compute_3m3m_pct(retail_m)
    wholesale_3m3m = compute_3m3m_pct(wholesale_m)

    retail_3m3m    = retail_3m3m.rename(columns=lambda c: label_map.get(c, c))
    wholesale_3m3m = wholesale_3m3m.rename(columns=lambda c: label_map.get(c, c))

    ISM_TKR = 'NAPMINV Index'
    fed_now_cols = [c for c in soft_now_m.columns if c != ISM_TKR]
    fed_now_avg  = soft_now_m[fed_now_cols].mean(axis=1, skipna=True)
    fed_exp_avg  = soft_exp_m.mean(axis=1, skipna=True)

    label_ISM      = label_short_map.get(ISM_TKR, ISM_TKR)
    label_FED_NOW  = 'Fed Surveys Inventories (avg)'
    label_FED_EXP  = 'Fed Surveys Inventories (6m avg)'

    cmap = plt.get_cmap('tab10')
    color_by_label = {}
    for extra in [label_ISM, label_FED_NOW, label_FED_EXP]:
        if extra not in color_by_label:
            color_by_label[extra] = cmap(len(color_by_label) % 10)

    fig, axes = plt.subplots(2, 2, figsize=(18, 10))
    (ax11, ax12), (ax21, ax22) = axes

    for c in retail_3m3m.columns:
        ax11.plot(retail_3m3m.index, retail_3m3m[c], lw=1.7, label=c)
    ax11.axhline(0, color='grey', lw=0.9, ls='--')
    ax11.set_title("Retail Inventories -- 3m/3m%", fontsize=12)
    ax11.set_ylabel("%")
    ax11.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
    ax11.legend(fontsize=8, ncol=2, loc='upper left')
    ax11.xaxis.set_major_locator(mdates.YearLocator())
    ax11.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax11.tick_params(axis='x', rotation=45)
    ax11.set_ylim(-10, 10)

    for c in wholesale_3m3m.columns:
        ax12.plot(wholesale_3m3m.index, wholesale_3m3m[c], lw=1.8, label=c)
    ax12.axhline(0, color='grey', lw=0.9, ls='--')
    ax12.set_title("Wholesale Inventories -- 3m/3m%", fontsize=12)
    ax12.set_ylabel("%")
    ax12.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
    ax12.legend(fontsize=8, ncol=1, loc='upper left')
    ax12.xaxis.set_major_locator(mdates.YearLocator())
    ax12.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax12.tick_params(axis='x', rotation=45)

    plot_soft_current_ISM_vs_fedavg(
        ax21, soft_now_m, fed_now_avg,
        "Soft Data -- Current Inventories (ISM LHS; Fed Surveys avg RHS)",
        label_ISM, label_FED_NOW, color_by_label,
        lhs_ticker=ISM_TKR)

    plot_soft_expectations_fedavg(
        ax22, fed_exp_avg,
        "Soft Data -- 6-Month Expectations (Fed Surveys avg)",
        label_FED_EXP, color_by_label)

    last_retail_date   = _last_data_date_df(retail_3m3m)
    last_wh_date       = _last_data_date_df(wholesale_3m3m)
    lhs_series_for_soft_now = soft_now_m[ISM_TKR] if ISM_TKR in soft_now_m.columns else soft_now_m.iloc[:, 0]
    last_soft_now_date = max(_last_data_date_series(lhs_series_for_soft_now),
                             _last_data_date_series(fed_now_avg))
    last_soft_exp_date = _last_data_date_series(fed_exp_avg)

    for ax, d in [(ax11, last_retail_date),
                  (ax12, last_wh_date),
                  (ax21, last_soft_now_date),
                  (ax22, last_soft_exp_date)]:
        if pd.notna(d):
            ax.text(0.99, 0.98, f"Last data: {d.date()}",
                    transform=ax.transAxes,
                    ha='right', va='top', fontsize=8,
                    bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.2'))

    fig.suptitle("US Inventories & Surveys -- Retail/Wholesale (3m/3m%) & Soft Data",
                 fontsize=14, y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(OUTFILE, bbox_inches='tight')
    plt.close(fig)


def chart_dxy_rolling_attribution():
    """DXY Rolling Attribution -- from charts_updater5.py"""
    END_DATE = datetime.today()

    FIELD       = 'PX_LAST'
    START_DATE  = datetime(2020, 1, 1)
    ROLL_WIN    = 63
    SAVE_PATH   = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
    OUTFILE     = SAVE_PATH / "DXY_Rolling_Attribution.png"
    TICKERS = {
        'DXY':     'DXY Index',
        'SPX':     'SPX Index',
        'VIX':     'VIX Index',
        'US10Y':   'USOSFR10 Index',
        'US1Y1Y':  'S0042FS 1Y1Y BLC Curncy',
    }

    def bdh_flat(tickers, field=FIELD, start=START_DATE, end=END_DATE):
        df = blp.bdh(tickers=list(tickers), flds=[field], start_date=start, end_date=end)
        if isinstance(df.columns, pd.MultiIndex):
            if field in df.columns.get_level_values(-1):
                df = df.xs(field, level=-1, axis=1)
            else:
                df = df.xs(field, level=0, axis=1)
        df.index = pd.to_datetime(df.index)
        return df.sort_index()

    def transform_series(wide):
        w = wide.copy()
        w = w.asfreq('B').ffill(limit=2)
        out = pd.DataFrame(index=w.index)
        if 'DXY' in w:    out['DXY']    = 100.0 * np.log(w['DXY']).diff()
        if 'SPX' in w:    out['SPX']    = 100.0 * np.log(w['SPX']).diff()
        if 'VIX' in w:    out['VIX']    = w['VIX'].diff()
        if 'US10Y' in w:  out['US10Y']  = 100.0 * w['US10Y'].diff()
        if 'US1Y1Y' in w: out['US1Y1Y'] = 100.0 * w['US1Y1Y'].diff()
        return out.dropna(how='any')

    def rolling_two_stage(df, y_col, x_stage1, x_stage2_list, win):
        if not STATSMODELS_AVAILABLE or sm is None:
            print("Warning: statsmodels not available, skipping rolling_two_stage regression")
            return pd.DataFrame(columns=['R2_stage1'] + [f'R2_{x}' for x in x_stage2_list])
        idx = []
        series = {'R2_stage1': []}
        for x2 in x_stage2_list:
            series[f'R2_{x2}'] = []
        n = len(df)
        for i in range(win, n + 1):
            sub = df.iloc[i - win:i][[y_col, x_stage1] + x_stage2_list].dropna()
            if len(sub) < 20:
                continue
            y  = sub[y_col].values
            X1 = sm.add_constant(sub[[x_stage1]].values, has_constant='add')
            try:
                m1 = sm.OLS(y, X1).fit()
            except Exception:
                continue
            r2_1   = m1.rsquared
            resid1 = y - m1.fittedvalues
            r2_2s = {}
            for x2 in x_stage2_list:
                X2 = sm.add_constant(sub[[x2]].values, has_constant='add')
                try:
                    m2   = sm.OLS(resid1, X2).fit()
                    r2_2 = m2.rsquared
                except Exception:
                    r2_2 = np.nan
                r2_2s[x2] = r2_2
            idx.append(sub.index[-1])
            series['R2_stage1'].append(r2_1)
            for x2 in x_stage2_list:
                series[f'R2_{x2}'].append(r2_2s[x2])
        if not idx:
            return pd.DataFrame(columns=['R2_stage1'] + [f'R2_{x}' for x in x_stage2_list])
        out = pd.DataFrame(series, index=pd.Index(idx, name='Date'))
        return out

    raw = bdh_flat(list(TICKERS.values()))
    raw.columns = list(TICKERS.keys())
    chg = transform_series(raw)

    res_spx = rolling_two_stage(chg, y_col='DXY', x_stage1='SPX',
                                x_stage2_list=['US1Y1Y', 'US10Y'], win=ROLL_WIN)
    res_vix = rolling_two_stage(chg, y_col='DXY', x_stage1='VIX',
                                x_stage2_list=['US1Y1Y', 'US10Y'], win=ROLL_WIN)

    WIN_3M = 63
    WIN_1M = 21
    corr_3m = pd.DataFrame({
        'DXY~SPX': chg['DXY'].rolling(WIN_3M).corr(chg['SPX']),
        'DXY~VIX_inv': -chg['DXY'].rolling(WIN_3M).corr(chg['VIX']),
    }).dropna()
    corr_1m = pd.DataFrame({
        'DXY~SPX': chg['DXY'].rolling(WIN_1M).corr(chg['SPX']),
        'DXY~VIX_inv': -chg['DXY'].rolling(WIN_1M).corr(chg['VIX']),
    }).dropna()
    cutoff_2y = pd.Timestamp.today().normalize() - pd.DateOffset(years=2)
    corr_1m = corr_1m[corr_1m.index >= cutoff_2y]

    import matplotlib.gridspec as gridspec
    fig = plt.figure(figsize=(14, 14))
    gs = gridspec.GridSpec(3, 2, height_ratios=[1.0, 1.0, 0.9], hspace=0.35, wspace=0.25)

    ax1 = fig.add_subplot(gs[0, :])
    if not res_spx.empty:
        ax1.plot(res_spx.index, res_spx['R2_stage1'],  label='R2: DXY ~ SPX (stage 1)', color='black',     lw=2.2)
        ax1.plot(res_spx.index, res_spx['R2_US1Y1Y'],  label='R2: resid ~ 1Y1Y',        color='tab:blue',  lw=2.0)
        ax1.plot(res_spx.index, res_spx['R2_US10Y'],   label='R2: resid ~ 10Y',         color='tab:orange',lw=2.0)
        ax1.set_ylim(0, 1)
        ax1.set_title('Stage-1: DXY ~ SPX  |  Stage-2: residual ~ {1Y1Y, 10Y}  (rolling 3m)', fontsize=12)
        ax1.grid(True, linestyle=':', linewidth=0.8, alpha=0.8)
        ax1.legend(loc='upper left', fontsize=9)
    else:
        ax1.text(0.5, 0.5, 'Insufficient data for SPX regression', ha='center', va='center')
        ax1.axis('off')

    ax2 = fig.add_subplot(gs[1, :])
    if not res_vix.empty:
        ax2.plot(res_vix.index, res_vix['R2_stage1'],  label='R2: DXY ~ VIX (stage 1)', color='black',     lw=2.2)
        ax2.plot(res_vix.index, res_vix['R2_US1Y1Y'],  label='R2: resid ~ 1Y1Y',          color='tab:blue',  lw=2.0)
        ax2.plot(res_vix.index, res_vix['R2_US10Y'],   label='R2: resid ~ 10Y',           color='tab:orange',lw=2.0)
        ax2.set_ylim(0, 1)
        ax2.set_title('Stage-1: DXY ~ VIX (absolute)  |  Stage-2: residual ~ {1Y1Y, 10Y}  (rolling 3m)', fontsize=12)
        ax2.grid(True, linestyle=':', linewidth=0.8, alpha=0.8)
        ax2.legend(loc='upper left', fontsize=9)
    else:
        ax2.text(0.5, 0.5, 'Insufficient data for VIX regression', ha='center', va='center')
        ax2.axis('off')

    ax3 = fig.add_subplot(gs[2, 0])
    if not corr_3m.empty:
        ax3.plot(corr_3m.index, corr_3m['DXY~SPX'],     label='corr(DXY, SPX) - 3m',  color='tab:green', lw=1.8)
        ax3.plot(corr_3m.index, corr_3m['DXY~VIX_inv'], label='corr(DXY, VIX)(inv.) - 3m', color='tab:red',   lw=1.8)
        ax3.set_ylim(-1, 1)
        ax3.axhline(0, color='grey', lw=0.8)
        ax3.set_title('Rolling correlation (3 months)', fontsize=12)
        ax3.grid(True, linestyle=':', linewidth=0.8, alpha=0.8)
        ax3.legend(loc='upper left', fontsize=9)
    else:
        ax3.text(0.5, 0.5, 'No data for 3m correlations', ha='center', va='center')
        ax3.axis('off')

    ax4 = fig.add_subplot(gs[2, 1])
    if not corr_1m.empty:
        ax4.plot(corr_1m.index, corr_1m['DXY~SPX'],     label='corr(DXY, SPX) - 1m',  color='tab:green', lw=1.8)
        ax4.plot(corr_1m.index, corr_1m['DXY~VIX_inv'], label='corr(DXY, VIX)(inv.) - 1m', color='tab:red',   lw=1.8)
        ax4.set_ylim(-1, 1)
        ax4.axhline(0, color='grey', lw=0.8)
        ax4.set_title('Rolling correlation (1 month)', fontsize=12)
        ax4.grid(True, linestyle=':', linewidth=0.8, alpha=0.8)
        ax4.legend(loc='upper left', fontsize=9)
    else:
        ax4.text(0.5, 0.5, 'No data for 1m correlations (last 2y)', ha='center', va='center')
        ax4.axis('off')

    for ax in [ax2, ax3, ax4]:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.tick_params(axis='x', rotation=45)

    fig.suptitle('DXY Two-Stage Rolling Attribution (3M Rolling) + DXY Correlation', fontsize=15, y=0.98)
    last_candidates = []
    for df_ in [res_spx, res_vix, corr_3m, corr_1m]:
        if df_ is not None and not df_.empty:
            last_candidates.append(df_.index.max())
    if last_candidates:
        last_date = min(last_candidates)
        fig.text(0.985, 0.985, f"Last data: {pd.to_datetime(last_date).date()}",
                 ha='right', va='top', fontsize=9,
                 bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(OUTFILE, bbox_inches='tight')
    plt.close(fig)


def chart_gold_etf_fund_flows():
    """Gold ETF Fund Flows -- from charts_updater5.py"""
    END_DATE = datetime.today()

    NAMES = {
        'GLD US Equity':  'SPDR Gold Shares (GLD)',
        'IAU US Equity':  'iShares Gold Trust (IAU)',
        'IGLN LN Equity': 'iShares Physical Gold (IGLN)',
        'GLDM US Equity': 'SPDR Gold MiniShares (GLDM)',
        'SGLD LN Equity': 'Invesco Physical Gold (SGLD)',
        'SGOL US Equity': 'abrdn Physical Gold Shares (SGOL)',
        'GOLD AU Equity': 'Global X Physical Gold (GOLD AU)',
    }
    TICKERS = list(NAMES.keys())
    START_DATE = datetime(2015, 1, 1)
    SAVE_PATH   = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
    OUTFILE = SAVE_PATH / "Gold_ETF_FundFlows.png"

    def normalize_ccy(ccy):
        if ccy is None:
            return 'USD'
        c = ccy.upper()
        if c in {'GBP', 'GBX', 'GBP*', 'GBP CURNCY', 'GBP CURN',
                 'GBP PENCE', 'GBPGBX', 'GBP/GBX', 'GBP(PENCE)',
                 'GBP PENCE STERLING', 'GBP PENCE STER', 'GBp'}:
            return 'GBP'
        return c

    def usd_per_ccy_series(ccy, start, end):
        if ccy == 'USD':
            idx = pd.date_range(start, end, freq='B')
            return pd.Series(1.0, index=idx)
        pair1 = f'{ccy}USD Curncy'
        pair2 = f'USD{ccy} Curncy'
        px = blp.bdh([pair1, pair2], 'PX_LAST', start, end)
        if isinstance(px.columns, pd.MultiIndex):
            if 'PX_LAST' in px.columns.get_level_values(-1):
                px = px.xs('PX_LAST', axis=1, level=-1)
            else:
                px = px.xs('PX_LAST', axis=1, level=0)
        s1 = px.get(pair1)
        s2 = px.get(pair2)
        n1 = int(s1.notna().sum()) if s1 is not None else 0
        n2 = int(s2.notna().sum()) if s2 is not None else 0
        if n1 >= n2 and n1 > 0:
            ser = s1.copy()
        elif n2 > 0:
            ser = 1.0 / s2
        else:
            raise ValueError(f"No FX data for {ccy} vs USD (tried {pair1} and {pair2})")
        ser.index = pd.to_datetime(ser.index)
        return ser

    ccy_df = blp.bdp(TICKERS, flds='CRNCY')
    ccy_map = {t: normalize_ccy(ccy_df.loc[t, 'crncy']) for t in ccy_df.index}

    raw_flows = blp.bdh(tickers=TICKERS, flds='FUND_FLOW',
                        start_date=START_DATE, end_date=END_DATE)
    if isinstance(raw_flows.columns, pd.MultiIndex):
        if "FUND_FLOW" in raw_flows.columns.get_level_values(-1):
            flows_native = raw_flows.xs("FUND_FLOW", level=-1, axis=1)
        else:
            flows_native = raw_flows.xs("FUND_FLOW", level=0, axis=1)
    else:
        flows_native = raw_flows.copy()
    flows_native.index = pd.to_datetime(flows_native.index)
    flows_native = flows_native.apply(pd.to_numeric, errors='coerce')
    last_reported_date = flows_native.dropna(how='all').index.max()
    bidx = pd.date_range(flows_native.index.min(), flows_native.index.max(), freq='B')
    flows_native_b = flows_native.reindex(bidx).fillna(0.0)

    usd_per_ccy = {}
    for t in flows_native_b.columns:
        ccy = ccy_map.get(t, 'USD')
        if ccy not in usd_per_ccy:
            fx = usd_per_ccy_series(ccy, bidx.min(), bidx.max())
            fx = fx.reindex(bidx).ffill(limit=3)
            usd_per_ccy[ccy] = fx

    flows_usd = pd.DataFrame(index=bidx)
    for t in flows_native_b.columns:
        ccy = ccy_map.get(t, 'USD')
        flows_usd[t] = flows_native_b[t].values * usd_per_ccy[ccy].values

    total_usd = flows_usd.sum(axis=1)
    sum20     = total_usd.rolling(20, min_periods=1).sum()
    roll_12m  = total_usd.rolling('252D').sum()
    last_data_date = pd.Timestamp(last_reported_date) if last_reported_date in total_usd.index else total_usd.dropna().index.max()
    last_sum20_val  = sum20.loc[last_data_date]

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    ax1, ax2 = axes
    ax1.plot(sum20.index, sum20.values, lw=2.0, label='20D Sum')
    ax1.axhline(0, lw=1, zorder=0)
    ax1.grid(True, linestyle=':', alpha=0.5)
    ax1.set_ylabel("USD")
    ax1.legend(loc='upper left', fontsize=9)

    ax2.plot(roll_12m.index, roll_12m.values, lw=2.0, label='12M Rolling Sum')
    ax2.axhline(0, lw=1, zorder=0)
    ax2.grid(True, linestyle=':', alpha=0.5)
    ax2.set_ylabel("USD")
    ax2.set_xlabel("Date")
    ax2.legend(loc='upper left', fontsize=9)

    for ax in axes:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax2.get_xticklabels(), rotation=45)

    fig.suptitle("Aggregate Gold ETF Fund Flows (USD Millions)", y=0.98, fontsize=15)
    fig.text(
        0.98, 0.96, f"Last data: {pd.to_datetime(last_data_date).date()}",
        ha='right', va='top', fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25')
    )
    ax1.scatter([last_data_date], [last_sum20_val], zorder=5)
    ax1.text(last_data_date, last_sum20_val, f"{last_sum20_val:,.0f}", ha='left', va='bottom', fontsize=8)
    plt.tight_layout()
    plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved chart to: {OUTFILE}")


def chart_vnd_vs_cr_band():
    """VND Spot vs SBV Interbank Rate Band -- from charts_updater5.py"""
    END_DATE = datetime.today()

    TICKERS = {
        'VND T130 Curncy': 'VND Spot',
        'SBVNUSD Index':   'CR',
    }
    START_DATE = datetime(2018, 1, 1)
    CUTOFF     = pd.Timestamp('2018-01-01')
    SAVE_PATH = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
    OUTFILE   = SAVE_PATH / "VND_vs_CR_Band.png"

    raw = blp.bdh(tickers=list(TICKERS.keys()), flds='PX_LAST',
                  start_date=START_DATE, end_date=END_DATE)
    if isinstance(raw.columns, pd.MultiIndex):
        if "PX_LAST" in raw.columns.get_level_values(-1):
            df = raw.xs("PX_LAST", level=-1, axis=1)
        else:
            df = raw.xs("PX_LAST", level=0, axis=1)
    else:
        df = raw.copy()
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric, errors='coerce')
    df = df.rename(columns=TICKERS)

    last_reported_date = df.dropna(how='all').index.max()
    bidx = pd.date_range(df.index.min(), df.index.max(), freq='B')
    df_b = df.reindex(bidx)
    df_b['CR']        = df_b['CR'].ffill(limit=2)
    df_b['VND Spot']  = df_b['VND Spot'].ffill(limit=2)

    valid_mask = df_b[['VND Spot', 'CR']].notna().all(axis=1)
    if not valid_mask.any():
        raise ValueError("No overlapping data between VND Spot and CR.")
    df_b = df_b.loc[valid_mask.idxmax():]

    df_w = df_b.loc[df_b.index >= CUTOFF].copy()
    if df_w.empty:
        raise ValueError("No data available on or after 2018-01-01 after alignment.")

    cr    = df_w['CR']
    spot  = df_w['VND Spot']
    upper = cr * 1.05
    lower = cr * 0.95

    in_band  = spot.where((spot >= lower) & (spot <= upper))
    out_band = spot.where((spot > upper) | (spot < lower))

    last_data_date = df_w.dropna(how='all').index.max()
    latest_idx  = df_w[['VND Spot','CR']].dropna().index.max()
    latest_spot = df_w.at[latest_idx, 'VND Spot']
    latest_cr   = df_w.at[latest_idx, 'CR']

    pct_dist = (spot / cr - 1.0) * 100.0
    latest_pct_idx = pct_dist.dropna().index.max()
    latest_pct_val = pct_dist.loc[latest_pct_idx]

    cr_for_dd          = cr.ffill()
    cr_running_max_win = cr_for_dd.expanding(min_periods=1).max()
    cr_dd_plot         = cr_for_dd - cr_running_max_win

    latest_dd_idx = cr_dd_plot.dropna().index.max()
    latest_dd_val = cr_dd_plot.loc[latest_dd_idx]

    fig, (ax, ax2, ax3) = plt.subplots(
        3, 1, figsize=(14, 12), sharex=True,
        gridspec_kw={'height_ratios': [2.1, 1.0, 1.0], 'hspace': 0.08}
    )

    ax.fill_between(cr.index, lower.values, upper.values, color='#B3D7FF', alpha=0.35, label='+/-5% band')
    ax.plot(cr.index, cr.values, color='#003f5c', lw=2.2, label='CR')
    ax.plot(in_band.index, in_band.values, color='black', lw=1.8, label='VND Spot (in band)')
    ax.plot(out_band.index, out_band.values, color='red', lw=1.8, label='VND Spot (out of band)', zorder=5)
    ax.plot(latest_idx, latest_spot, marker='o', color='black', ms=5, zorder=6)
    ax.annotate(f"{latest_spot:,.0f}",
                xy=(latest_idx, latest_spot), xytext=(6, 8),
                textcoords='offset points', ha='left', va='bottom',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.7))
    ax.plot(latest_idx, latest_cr, marker='o', color='#003f5c', ms=5, zorder=6)
    ax.annotate(f"{latest_cr:,.0f}",
                xy=(latest_idx, latest_cr), xytext=(6, -14),
                textcoords='offset points', ha='left', va='top',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.7))
    ax.set_title("VND Spot vs CR (+/-5% Band)", fontsize=15)
    ax.set_ylabel("USDVND")
    ax.grid(True, linestyle=':', alpha=0.5)
    ax.legend(loc='upper left', fontsize=9)

    ax2.plot(pct_dist.index, pct_dist.values, lw=1.8, label='Spot vs CR (% diff)')
    ax2.axhline(0, linestyle='--', alpha=0.5)
    ax2.axhline(5, linestyle=':', alpha=0.7, label='+/-5% band')
    ax2.axhline(-5, linestyle=':', alpha=0.7)
    ax2.plot(latest_pct_idx, latest_pct_val, marker='o', ms=5, zorder=6)
    ax2.annotate(f"{latest_pct_val:+.2f}%",
                 xy=(latest_pct_idx, latest_pct_val), xytext=(6, 8),
                 textcoords='offset points', ha='left', va='bottom',
                 bbox=dict(facecolor='white', edgecolor='none', alpha=0.7))
    ax2.set_ylabel("Spot - CR (%)")
    ax2.grid(True, linestyle=':', alpha=0.5)
    ax2.legend(loc='upper left', fontsize=9)

    ax3.plot(cr_dd_plot.index, cr_dd_plot.values, lw=1.8, label='CR drawdown (from prior high)')
    ax3.fill_between(cr_dd_plot.index, cr_dd_plot.values, 0, alpha=0.25)
    ax3.axhline(0, linestyle='--', alpha=0.6)
    ax3.plot(latest_dd_idx, latest_dd_val, marker='o', ms=5, zorder=6)
    ax3.annotate(f"{latest_dd_val:+.0f}",
                 xy=(latest_dd_idx, latest_dd_val),
                 xytext=(6, 8 if latest_dd_val >= 0 else -14),
                 textcoords='offset points', ha='left',
                 va='bottom' if latest_dd_val >= 0 else 'top',
                 bbox=dict(facecolor='white', edgecolor='none', alpha=0.7))
    ax3.set_ylabel("CR Drawdown (USDVND)")
    ax3.set_xlabel("Date")
    ax3.grid(True, linestyle=':', alpha=0.5)
    ax3.legend(loc='upper left', fontsize=9)

    ax3.xaxis.set_major_locator(mdates.YearLocator())
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax3.get_xticklabels(), rotation=45)

    fig.text(
        0.98, 0.96, f"Last data: {pd.to_datetime(last_data_date).date()}",
        ha='right', va='top', fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25')
    )
    plt.tight_layout()
    plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved chart to: {OUTFILE}")


def chart_idr_dndf_expiry_schedule():
    """Indonesia IDR DNDF Expiry Schedule -- from charts_updater5.py"""
    END_DATE = datetime.today()

    START = (pd.Timestamp.today().normalize() - pd.Timedelta(days=370))
    END   = pd.Timestamp.today().normalize()

    TKS_1M = ['IDFB1MTH Index', 'IDFB1MPM Index', 'IDFB1MRP Index']
    TKS_3M = ['IDFB3MTH Index', 'IDFB3MPM Index']
    ALL_TKS = TKS_1M + TKS_3M
    FIELD = 'VOLUME'
    PNG_OUT = G_CHART_DIR / "IDR_DNDF_Expiry_Schedule.png"

    def flatten_xbbg(raw, field):
        if isinstance(raw.columns, pd.MultiIndex):
            if field in raw.columns.get_level_values(-1):
                return raw.xs(field, axis=1, level=-1)
            return raw.xs(field, axis=1, level=0)
        return raw.copy()

    def bds_calendar_non_settlement(cal_code, start, end):
        ovrds = [
            ('SETTLEMENT_CALENDAR_CODE', cal_code),
            ('CALENDAR_START_DATE', start.strftime('%Y%m%d')),
            ('CALENDAR_END_DATE',   end.strftime('%Y%m%d')),
        ]
        df = blp.bds('USD Curncy', 'CALENDAR_NON_SETTLEMENT_DATES', ovrds=ovrds)
        if df is None or df.empty:
            return pd.DatetimeIndex([])
        col = next((c for c in df.columns if 'NON' in c.upper() and 'SETTLEMENT' in c.upper()), None)
        if not col:
            return pd.DatetimeIndex([])
        dates = pd.to_datetime(df[col], errors='coerce').dropna().unique()
        return pd.to_datetime(sorted(dates))

    def make_business_day(holidays):
        return CustomBusinessDay(weekmask='Mon Tue Wed Thu Fri', holidays=pd.to_datetime(holidays))

    def is_business_day(ts, hol_set):
        return (ts.weekday() < 5) and (ts.normalize() not in hol_set)

    def roll_to_next_business_day(ts, hol_set):
        d = ts.normalize()
        while not is_business_day(d, hol_set):
            d += pd.Timedelta(days=1)
        return d

    def add_business_days(ts, n, cbd):
        return (ts.normalize() + n * cbd).normalize()

    def safe_sum_cols(df, cols):
        present = [c for c in cols if c in df.columns]
        if present:
            return df[present].sum(axis=1, min_count=1)
        return pd.Series(index=df.index, dtype=float)

    CAL_START = START - pd.Timedelta(days=10)
    CAL_END   = END + pd.DateOffset(months=4)

    hol_id = bds_calendar_non_settlement('CDR_ID', CAL_START, CAL_END)
    hol_us = bds_calendar_non_settlement('CDR_US', CAL_START, CAL_END)
    hol_union = pd.DatetimeIndex(sorted(set(hol_id) | set(hol_us)))

    hol_set_id     = set(pd.to_datetime(hol_id).normalize())
    hol_set_union  = set(pd.to_datetime(hol_union).normalize())

    CBD_SETTLE = make_business_day(hol_union)
    CBD_ID     = make_business_day(hol_id)

    raw = blp.bdh(tickers=ALL_TKS, flds=FIELD, start_date=START, end_date=END, Per='D')
    vols = flatten_xbbg(raw, FIELD)
    vols.index = pd.to_datetime(vols.index)
    vols = vols.apply(pd.to_numeric, errors='coerce')

    last_auction_date = vols.dropna(how='all').index.max()
    v1m = safe_sum_cols(vols, TKS_1M)
    v3m = safe_sum_cols(vols, TKS_3M)
    v1m = v1m.where(v1m.notna() & (v1m != 0))
    v3m = v3m.where(v3m.notna() & (v3m != 0))
    auctions = pd.DataFrame({'1M': v1m, '3M': v3m}).dropna(how='all')

    rows = []
    for dt, row in auctions.iterrows():
        auct = pd.Timestamp(dt).normalize()
        for tenor, months in [('1M', 1), ('3M', 3)]:
            vol = row.get(tenor)
            if pd.notna(vol) and vol != 0:
                vd0 = add_business_days(auct, 2, CBD_SETTLE)
                vd1 = vd0 + relativedelta(months=months)
                vd  = roll_to_next_business_day(pd.Timestamp(vd1), hol_set_union)
                fix = add_business_days(vd, -2, CBD_ID)
                rows.append({
                    'auction_date': auct.date(),
                    'tenor': tenor,
                    'volume': float(vol),
                    'value_date': vd.date(),
                    'fixing_date': fix.date(),
                })

    schedule = pd.DataFrame(rows).sort_values(['fixing_date', 'tenor'])

    today = pd.Timestamp.today().normalize().date()
    if not schedule.empty:
        upcoming = schedule[schedule['fixing_date'] >= today]
        upcoming_totals = (upcoming.groupby('fixing_date')['volume']
                           .sum().rename('total_volume').sort_index())
    else:
        upcoming = pd.DataFrame(columns=['auction_date','tenor','volume','value_date','fixing_date'])
        upcoming_totals = pd.Series(name='total_volume', dtype=float)
    if not upcoming_totals.empty:
        end_bar = pd.to_datetime(upcoming_totals.index.max())
    else:
        end_bar = pd.Timestamp.today().normalize() + 10 * CBD_SETTLE

    bidx = pd.date_range(pd.Timestamp(today), end_bar, freq=CBD_SETTLE)
    totals_dt = upcoming_totals.copy()
    if not totals_dt.empty:
        totals_dt.index = pd.to_datetime(totals_dt.index)
    bar_series = totals_dt.reindex(bidx, fill_value=0.0)
    bar_series_mn = bar_series / 1_000_000.0

    fig = plt.figure(figsize=(16, 7), constrained_layout=True)
    gs = fig.add_gridspec(nrows=1, ncols=2, width_ratios=[1, 2])

    ax_tbl = fig.add_subplot(gs[0, 0])
    ax_tbl.axis('off')
    if not upcoming_totals.empty:
        tbl_df = upcoming_totals.reset_index().rename(columns={'fixing_date': 'Fixing Date', 'total_volume': 'Total Volume'})
        tbl_df['Fixing Date'] = pd.to_datetime(tbl_df['Fixing Date']).dt.strftime('%Y-%m-%d')
        tbl_df['Total Volume'] = tbl_df['Total Volume'].map(lambda x: f"{x:,.0f}")
        table = ax_tbl.table(
            cellText=tbl_df.values,
            colLabels=tbl_df.columns.tolist(),
            loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.0, 1.1)
    else:
        ax_tbl.text(0.5, 0.5, "No upcoming DNDF fixings.", ha='center', va='center', fontsize=12)
    ax_tbl.set_title("Upcoming DNDF Fixings", fontsize=12, pad=8)

    ax_bar = fig.add_subplot(gs[0, 1])
    ax_bar.bar(bar_series_mn.index, bar_series_mn.values, width=1.0, align='center')
    ax_bar.set_title("Upcoming DNDF Fixings Profile (Millions)", fontsize=12)
    ax_bar.set_ylabel("Amount (millions)")
    ax_bar.set_xlabel("Date")
    ax_bar.xaxis.set_major_locator(mdates.MonthLocator())
    ax_bar.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax_bar.get_xticklabels(), rotation=45)
    ax_bar.grid(True, linestyle=':', alpha=0.5)

    if pd.notna(last_auction_date):
        fig.text(
            0.98, 0.98, f"Last auction data: {pd.to_datetime(last_auction_date).date()}",
            ha='right', va='top', fontsize=10,
            bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'))

    fig.suptitle("Indonesia DNDF Fixing Calendar (1M incl. Rollover & 3M)", y=1.03, fontsize=14)
    plt.savefig(PNG_OUT, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved chart to: {PNG_OUT}")


def chart_india_banking_seasonality():
    """India Banking Seasonality -- from charts_updater5.py"""
    END_DATE = datetime.today()

    TICKER = 'INBGBKLQ Index'
    FIELD = 'PX_LAST'
    TODAY = pd.Timestamp.today().normalize()
    CURR_YEAR = TODAY.year
    YEARS_10 = list(range(CURR_YEAR - 10, CURR_YEAR))
    START = pd.Timestamp(f'{YEARS_10[0]}-01-01')
    END   = pd.Timestamp(f'{YEARS_10[-1]}-12-31')
    PNG_OUT = G_CHART_DIR / "INBGBKLQ_Seasonality.png"

    raw = blp.bdh(tickers=[TICKER], flds=FIELD, start_date=START, end_date=END, Per='D')
    if isinstance(raw.columns, pd.MultiIndex):
        if FIELD in raw.columns.get_level_values(-1):
            ser = raw.xs(FIELD, axis=1, level=-1)[TICKER]
        else:
            ser = raw.xs(FIELD, axis=1, level=0)[TICKER]
    else:
        ser = raw.iloc[:, 0]
    ser = pd.to_numeric(ser, errors='coerce')
    ser.index = pd.to_datetime(ser.index)
    ser = ser.dropna()
    last_data_date = ser.index.max()

    def level_by_doy_df(level_series, years):
        frames = {}
        for y in years:
            ys = level_series[level_series.index.year == y]
            if ys.empty:
                continue
            s = ys.copy()
            s.index = s.index.dayofyear
            frames[y] = s
        if not frames:
            return pd.DataFrame()
        all_doys = sorted(set().union(*[set(v.index) for v in frames.values()]))
        df = pd.DataFrame({y: frames[y].reindex(all_doys) for y in frames})
        return df

    doy_levels = level_by_doy_df(ser, YEARS_10)
    if not doy_levels.empty:
        doy_levels = doy_levels[doy_levels.index <= 365]

    avg_level = doy_levels.mean(axis=1) if not doy_levels.empty else pd.Series(dtype=float)
    p25 = doy_levels.quantile(0.25, axis=1) if not doy_levels.empty else pd.Series(dtype=float)
    p75 = doy_levels.quantile(0.75, axis=1) if not doy_levels.empty else pd.Series(dtype=float)

    if not avg_level.empty:
        base_year = 2001
        base_date = pd.Timestamp(f'{base_year}-01-01')
        doy = pd.Index(avg_level.index).astype(int).sort_values()
        x_dates = base_date + pd.to_timedelta(doy - 1, unit='D')
        avg_level = avg_level.reindex(doy)
        p25 = p25.reindex(doy)
        p75 = p75.reindex(doy)
    else:
        x_dates = pd.DatetimeIndex([])

    def last_on_or_before(s, dt):
        s2 = s.loc[:dt].dropna()
        return s2.iloc[-1] if len(s2) else np.nan

    def year_level_changes(level_series, years):
        ch_31, ch_yr = {}, {}
        for y in years:
            d_sep_end = pd.Timestamp(f'{y}-09-30')
            d_oct_31  = pd.Timestamp(f'{y}-10-31')
            d_dec_31  = pd.Timestamp(f'{y}-12-31')
            v_sep = last_on_or_before(level_series, d_sep_end)
            v_31  = last_on_or_before(level_series, d_oct_31)
            v_dec = last_on_or_before(level_series, d_dec_31)
            ch_31[y] = (v_31 - v_sep) if np.isfinite(v_sep) and np.isfinite(v_31) else np.nan
            ch_yr[y] = (v_dec - v_sep) if np.isfinite(v_sep) and np.isfinite(v_dec) else np.nan
        s31 = pd.Series(ch_31).dropna()
        syr = pd.Series(ch_yr).dropna()
        return s31, syr

    def year_hiSep_loOct_change(level_series, years):
        changes, labels = {}, {}
        for y in years:
            sep_lo = pd.Timestamp(f'{y}-09-15')
            sep_hi = pd.Timestamp(f'{y}-09-30')
            oct_lo = pd.Timestamp(f'{y}-10-01')
            oct_hi = pd.Timestamp(f'{y}-10-31')
            s1 = level_series[(level_series.index >= sep_lo) & (level_series.index <= sep_hi)]
            s2 = level_series[(level_series.index >= oct_lo) & (level_series.index <= oct_hi)]
            if s1.empty or s2.empty:
                continue
            idx_max_sep = s1.idxmax()
            val_max_sep = s1.loc[idx_max_sep]
            idx_min_oct = s2.idxmin()
            val_min_oct = s2.loc[idx_min_oct]
            changes[y] = val_min_oct - val_max_sep
            labels[y] = f"{idx_max_sep.strftime('%d%b')}-{idx_min_oct.strftime('%d%b')}"
        return pd.Series(changes).dropna(), labels

    def month_peak_to_next_trough(level_series, years):
        out = {m: [] for m in range(1, 13)}
        for y in years:
            for m in range(1, 13):
                cur_start = pd.Timestamp(f"{y}-{m:02d}-01")
                cur_end   = cur_start + pd.offsets.MonthEnd(1)
                if m < 12:
                    nxt_y, nxt_m = y, m + 1
                else:
                    nxt_y, nxt_m = y + 1, 1
                if nxt_y > years[-1]:
                    continue
                nxt_start = pd.Timestamp(f"{nxt_y}-{nxt_m:02d}-01")
                nxt_end   = nxt_start + pd.offsets.MonthEnd(1)
                s_cur = level_series.loc[cur_start:cur_end]
                s_nxt = level_series.loc[nxt_start:nxt_end]
                if s_cur.empty or s_nxt.empty:
                    continue
                max_cur = s_cur.max()
                min_nxt = s_nxt.min()
                if np.isfinite(max_cur) and np.isfinite(min_nxt):
                    out[m].append(min_nxt - max_cur)
        return {m: vals for m, vals in out.items() if vals}

    bars_31, bars_yr = year_level_changes(ser, YEARS_10)
    bars_hilo, hilo_labels = year_hiSep_loOct_change(ser, YEARS_10)
    month_changes = month_peak_to_next_trough(ser, YEARS_10)

    fig = plt.figure(figsize=(14, 12), constrained_layout=True)
    gs = fig.add_gridspec(nrows=3, ncols=2, height_ratios=[2.2, 1.5, 1.5])

    ax_seas = fig.add_subplot(gs[0, :])
    if len(x_dates) > 0:
        ax_seas.plot(x_dates, avg_level.values, linewidth=2.0, label='Avg level (10y)')
        ax_seas.fill_between(x_dates, p25.values, p75.values, alpha=0.25, label='25-75% band')
        ax_seas.set_title("Seasonality -- Average Level (Last 10 Full Years)")
        ax_seas.set_ylabel("Index level")
        ax_seas.xaxis.set_major_locator(mdates.MonthLocator())
        ax_seas.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        ax_seas.grid(True, linestyle=':', alpha=0.5)
        ax_seas.legend(loc='upper left', fontsize=8)
    else:
        ax_seas.text(0.5, 0.5, "Insufficient data for seasonality.", ha='center', va='center')
        ax_seas.set_axis_off()

    def draw_year_bars(ax, series, title, ylabel="Level change", per_bar_labels=None):
        if series is None or series.empty:
            ax.text(0.5, 0.5, "No data", ha='center', va='center')
            ax.set_axis_off()
            return
        years = series.index.astype(int).tolist()
        vals = series.values
        bars = ax.bar(years, vals)
        if per_bar_labels:
            pad = 0.01 * (np.nanmax(np.abs(vals)) if np.isfinite(np.nanmax(np.abs(vals))) else 1.0)
            for i, rect in enumerate(bars):
                yr = years[i]
                lab = per_bar_labels.get(yr)
                if not lab:
                    continue
                y = rect.get_height()
                va = 'bottom' if y >= 0 else 'top'
                y_text = y + pad if y >= 0 else y - pad
                ax.text(rect.get_x() + rect.get_width() / 2.0, y_text, lab,
                        ha='center', va=va, rotation=90, fontsize=8)
        ax.set_title(title)
        ax.set_xlabel("Year")
        ax.set_ylabel(ylabel)
        ax.set_xticks(years)
        ax.grid(True, linestyle=':', alpha=0.5)

    ax_bar1 = fig.add_subplot(gs[1, 0])
    draw_year_bars(ax_bar1, bars_31, "End Sep -> Oct 31")
    ax_bar2 = fig.add_subplot(gs[1, 1])
    draw_year_bars(ax_bar2, bars_yr, "End Sep -> Dec 31")
    ax_bar3 = fig.add_subplot(gs[2, 0])
    draw_year_bars(ax_bar3, bars_hilo, "Max(15-30 Sep) -> Min(Oct)", per_bar_labels=hilo_labels)

    ax_box = fig.add_subplot(gs[2, 1])
    ax_box.set_title("Monthly Peak -> Next-Month Trough (Last 10y) -- Level Change")
    ax_box.set_ylabel("Level change (next-month min - current-month max)")
    if month_changes:
        month_order = [m for m in range(1, 13) if m in month_changes]
        data = [month_changes[m] for m in month_order]
        labels = [pd.Timestamp(2001, m, 1).strftime('%b') for m in month_order]
        ax_box.boxplot(data, labels=labels, showmeans=True, meanline=True, patch_artist=True)
        ax_box.grid(True, linestyle=':', alpha=0.5)
    else:
        ax_box.text(0.5, 0.5, "No data", ha='center', va='center')
        ax_box.set_axis_off()

    if pd.notna(last_data_date):
        fig.text(
            0.98, 0.985, f"Last data: {pd.to_datetime(last_data_date).date()}",
            ha='right', va='top', fontsize=10,
            bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'),
        )

    fig.suptitle("INBGBKLQ Index -- Seasonality & Post-September Level Changes", y=0.995, fontsize=15)
    plt.savefig(PNG_OUT, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved chart to: {PNG_OUT}")




def chart_inr_basis_vs_reserves_and_liq():
    """INR Basis vs Reserves and Banking Liquidity -- from charts_updater5.py"""
    END_DATE = datetime.today()

    START = pd.Timestamp('2010-01-01')
    END   = pd.Timestamp.today().normalize()
    LOOKBACK_5Y_START = END - pd.DateOffset(years=5)

    TKS = [
        'IRNI3M Curncy', 'IRSWNIC Curncy',
        'IRNI12M Curncy', 'IRSWNI1 Curncy',
        'IRNI2Y Curncy', 'IRSWNI2 Curncy',
        'USDINR Curncy',
        'INMORES$ Index', 'INMOGOL$ Index',
        'INBGBKLQ Index', 'MKTIREPO Index', 'MKTIRRPO Index',
    ]
    FIELD = 'PX_LAST'
    PNG_OUT1 = G_CHART_DIR / "INR Basis vs Reserves.png"
    PNG_OUT2 = G_CHART_DIR / "INR Basis vs Banking Liq.png"

    def flatten_xbbg(raw, field):
        if isinstance(raw.columns, pd.MultiIndex):
            if field in raw.columns.get_level_values(-1):
                return raw.xs(field, axis=1, level=-1)
            return raw.xs(field, axis=1, level=0)
        return raw.copy()

    def bday_align_ffill(df, ffill_limit=10, ffill_limits=None, fillna_values=None):
        if df is None or df.empty:
            return df
        df = df.copy()
        df = df[~df.index.duplicated(keep="last")].sort_index()
        bidx = pd.bdate_range(df.index.min(), df.index.max())
        out = df.reindex(bidx)
        limits = ffill_limits or {}
        for col in out.columns:
            lim = limits.get(col, ffill_limit)
            if lim is None:
                out[col] = out[col].ffill()
            elif lim > 0:
                out[col] = out[col].ffill(limit=int(lim))
        if fillna_values:
            for col, val in fillna_values.items():
                if col in out.columns:
                    out[col] = out[col].fillna(val)
        return out

    def rolling_change(series, window_bdays):
        return series - series.shift(window_bdays)

    def last5y(s):
        return s.loc[LOOKBACK_5Y_START:] if s is not None else None

    def col_or_raise(df_, col):
        if col not in df_.columns:
            raise KeyError(f"Missing required Bloomberg series: {col}. Available: {list(df_.columns)}")
        s = df_[col].copy()
        s.name = col
        return s

    def corr_1y(a, b):
        return a.rolling(252).corr(b)

    def plot_pair(ax, sA, sB, labelA, labelB, colorA, colorB, title):
        ax.plot(sA.index, sA.values, color=colorA, label=labelA, linewidth=1.6)
        ax.set_ylabel(labelA, color=colorA)
        ax.tick_params(axis='y', colors=colorA)
        ax2 = ax.twinx()
        ax2.plot(sB.index, sB.values, color=colorB, label=labelB, linewidth=1.6, linestyle='--')
        ax2.set_ylabel(labelB, color=colorB)
        ax2.tick_params(axis='y', colors=colorB)
        ax.set_title(title)
        ax.grid(True, linestyle=':', alpha=0.5)
        hA, lA = ax.get_legend_handles_labels()
        hB, lB = ax2.get_legend_handles_labels()
        ax.legend(hA + hB, lA + lB, loc='upper left', fontsize=9)
        return ax, ax2

    def plot_corr(ax, s, title):
        ax.plot(s.index, s.values, color='#444444', linewidth=1.8)
        ax.axhline(0, color='k', linewidth=1)
        ax.set_ylim(-1.05, 1.05)
        ax.set_title(title)
        ax.set_ylabel("Corr")
        ax.grid(True, linestyle=':', alpha=0.5)

    def ma5(s):
        return s.rolling(5, min_periods=5).mean()

    raw = blp.bdh(tickers=TKS, flds=FIELD, start_date=START, end_date=END, Per='D')
    df = flatten_xbbg(raw, FIELD)
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric, errors='coerce')

    df_b = bday_align_ffill(
        df, ffill_limit=10,
        ffill_limits={
            'INBGBKLQ Index': 3,
            'MKTIREPO Index': 0,
            'MKTIRRPO Index': 0,
        },
        fillna_values={
            'MKTIREPO Index': 0.0,
            'MKTIRRPO Index': 0.0,
        },
    )

    basis_3m = col_or_raise(df_b, 'IRNI3M Curncy')  - col_or_raise(df_b, 'IRSWNIC Curncy')
    basis_1y = col_or_raise(df_b, 'IRNI12M Curncy') - col_or_raise(df_b, 'IRSWNI1 Curncy')
    basis_2y = col_or_raise(df_b, 'IRNI2Y Curncy')  - col_or_raise(df_b, 'IRSWNI2 Curncy')
    usdinr   = col_or_raise(df_b, 'USDINR Curncy')
    res_tot  = col_or_raise(df_b, 'INMORES$ Index')
    res_gold = col_or_raise(df_b, 'INMOGOL$ Index')
    res_ex   = res_tot - res_gold
    bank_liq = col_or_raise(df_b, 'INBGBKLQ Index')
    vrr      = col_or_raise(df_b, 'MKTIREPO Index')
    vrrr     = col_or_raise(df_b, 'MKTIRRPO Index')
    bank_liq_vrr = bank_liq - vrr + vrrr

    stack_for_last1 = pd.concat([basis_1y, basis_2y, usdinr, res_ex], axis=1)
    last_data_date1 = stack_for_last1.dropna(how='all').index.max()
    stack_for_last2 = pd.concat([basis_3m, basis_1y, basis_2y, bank_liq, bank_liq_vrr], axis=1)
    last_data_date2 = stack_for_last2.dropna(how='all').index.max()

    chg6_basis_3m   = rolling_change(basis_3m, 126)
    chg3_basis_3m   = rolling_change(basis_3m,  63)
    chg6_basis_1y   = rolling_change(basis_1y, 126)
    chg3_basis_1y   = rolling_change(basis_1y,  63)
    chg6_basis_2y   = rolling_change(basis_2y, 126)
    chg3_basis_2y   = rolling_change(basis_2y,  63)
    chg6_res_ex     = rolling_change(res_ex,   126)
    chg3_res_ex     = rolling_change(res_ex,    63)
    chg6_usd        = rolling_change(usdinr,   126)
    chg3_usd        = rolling_change(usdinr,    63)
    chg6_liq        = rolling_change(bank_liq, 126)
    chg3_liq        = rolling_change(bank_liq,  63)
    chg6_liq_vrr    = rolling_change(bank_liq_vrr, 126)
    chg3_liq_vrr    = rolling_change(bank_liq_vrr,  63)

    res_ex_inv = -res_ex
    chg6_res_ex_inv = -chg6_res_ex
    chg3_res_ex_inv = -chg3_res_ex

    # Rolling 1Y correlations -- Chart 1
    corr_level_b1y_resinv  = corr_1y(basis_1y, res_ex_inv)
    corr_chg6_b1y_resinv   = corr_1y(chg6_basis_1y, chg6_res_ex_inv)
    corr_chg3_b1y_resinv   = corr_1y(chg3_basis_1y, chg3_res_ex_inv)
    corr_level_b1y_usd     = corr_1y(basis_1y, usdinr)
    corr_chg6_b1y_usd      = corr_1y(chg6_basis_1y, chg6_usd)
    corr_chg3_b1y_usd      = corr_1y(chg3_basis_1y, chg3_usd)
    corr_level_b2y_resinv  = corr_1y(basis_2y, res_ex_inv)
    corr_chg6_b2y_resinv   = corr_1y(chg6_basis_2y, chg6_res_ex_inv)
    corr_chg3_b2y_resinv   = corr_1y(chg3_basis_2y, chg3_res_ex_inv)

    # Rolling 1Y correlations -- Chart 2
    corr_level_liq_b3m     = corr_1y(basis_3m, bank_liq)
    corr_chg6_liq_b3m      = corr_1y(chg6_basis_3m, chg6_liq)
    corr_chg3_liq_b3m      = corr_1y(chg3_basis_3m, chg3_liq)
    corr_level_liq_b1y     = corr_1y(basis_1y, bank_liq)
    corr_chg6_liq_b1y      = corr_1y(chg6_basis_1y, chg6_liq)
    corr_chg3_liq_b1y      = corr_1y(chg3_basis_1y, chg3_liq)
    corr_level_liq_b2y     = corr_1y(basis_2y, bank_liq)
    corr_chg6_liq_b2y      = corr_1y(chg6_basis_2y, chg6_liq)
    corr_chg3_liq_b2y      = corr_1y(chg3_basis_2y, chg3_liq)
    corr_level_liqv_b3m    = corr_1y(basis_3m, bank_liq_vrr)
    corr_chg6_liqv_b3m     = corr_1y(chg6_basis_3m, chg6_liq_vrr)
    corr_chg3_liqv_b3m     = corr_1y(chg3_basis_3m, chg3_liq_vrr)
    corr_level_liqv_b1y    = corr_1y(basis_1y, bank_liq_vrr)
    corr_chg6_liqv_b1y     = corr_1y(chg6_basis_1y, chg6_liq_vrr)
    corr_chg3_liqv_b1y     = corr_1y(chg3_basis_1y, chg3_liq_vrr)
    corr_level_liqv_b2y    = corr_1y(basis_2y, bank_liq_vrr)
    corr_chg6_liqv_b2y     = corr_1y(chg6_basis_2y, chg6_liq_vrr)
    corr_chg3_liqv_b2y     = corr_1y(chg3_basis_2y, chg3_liq_vrr)

    # Last 5y slices
    basis_3m_5   = last5y(basis_3m);  basis_1y_5   = last5y(basis_1y);  basis_2y_5   = last5y(basis_2y)
    res_ex_inv_5 = last5y(res_ex_inv); usd_5 = last5y(usdinr)
    bank_liq_5 = last5y(bank_liq); bank_liq_vrr_5 = last5y(bank_liq_vrr)
    chg6_basis_1y_5 = last5y(chg6_basis_1y); chg3_basis_1y_5 = last5y(chg3_basis_1y)
    chg6_basis_2y_5 = last5y(chg6_basis_2y); chg3_basis_2y_5 = last5y(chg3_basis_2y)
    chg6_res_ex_inv_5 = last5y(chg6_res_ex_inv); chg3_res_ex_inv_5 = last5y(chg3_res_ex_inv)
    chg6_usd_5 = last5y(chg6_usd); chg3_usd_5 = last5y(chg3_usd)

    corr_level_b1y_resinv_5 = last5y(corr_level_b1y_resinv)
    corr_chg6_b1y_resinv_5  = last5y(corr_chg6_b1y_resinv)
    corr_chg3_b1y_resinv_5  = last5y(corr_chg3_b1y_resinv)
    corr_level_b1y_usd_5 = last5y(corr_level_b1y_usd)
    corr_chg6_b1y_usd_5  = last5y(corr_chg6_b1y_usd)
    corr_chg3_b1y_usd_5  = last5y(corr_chg3_b1y_usd)
    corr_level_b2y_resinv_5 = last5y(corr_level_b2y_resinv)
    corr_chg6_b2y_resinv_5  = last5y(corr_chg6_b2y_resinv)
    corr_chg3_b2y_resinv_5  = last5y(corr_chg3_b2y_resinv)

    c_basis = '#1f77b4'; c_fx = '#ff7f0e'; c_res = '#2ca02c'

    # ========= Chart 1: Reserves + USDINR =========
    fig1 = plt.figure(figsize=(18, 20), constrained_layout=True)
    gs1 = fig1.add_gridspec(nrows=7, ncols=3, height_ratios=[2.4, 1.3, 1.3, 1.3, 1.3, 1.3, 1.3])

    ax_top = fig1.add_subplot(gs1[0, :])
    l1 = ax_top.plot(basis_1y.index, basis_1y.values, color=c_basis, label='INR 1Y Basis (LHS)', linewidth=1.9)[0]
    ax_top.set_ylabel("Basis", color=c_basis); ax_top.tick_params(axis='y', colors=c_basis)
    ax_top2 = ax_top.twinx()
    l2 = ax_top2.plot(usdinr.index, usdinr.values, color=c_fx, label='USDINR (RHS-1)', linewidth=1.4)[0]
    ax_top2.set_ylabel("USDINR", color=c_fx); ax_top2.tick_params(axis='y', colors=c_fx)
    ax_top3 = ax_top.twinx()
    ax_top3.spines['right'].set_position(('axes', 1.1))
    ax_top3.set_frame_on(True); ax_top3.patch.set_visible(False)
    l3 = ax_top3.plot(res_ex.index, res_ex.values, color=c_res, label='Reserves ex-gold (RHS-2)', linewidth=1.6, linestyle='--')[0]
    ax_top3.set_ylabel("Reserves ex-gold (USD)", color=c_res); ax_top3.tick_params(axis='y', colors=c_res)
    ax_top.set_title("INR 1Y Basis vs USDINR vs RBI Reserves ex-gold")
    ax_top.grid(True, linestyle=':', alpha=0.5)
    handles, labels = ax_top.get_legend_handles_labels()
    h2, l2_ = ax_top2.get_legend_handles_labels()
    h3, l3_ = ax_top3.get_legend_handles_labels()
    ax_top.legend(handles + h2 + h3, labels + l2_ + l3_, loc='upper left', fontsize=9)

    # Block 1: 1Y Basis vs Reserves (rows 1-2)
    ax11 = fig1.add_subplot(gs1[1, 0]); plot_pair(ax11, basis_1y_5, res_ex_inv_5, "Basis (1Y)", "- Reserves ex-gold", c_basis, c_res, "Levels (last 5y)")
    ax12 = fig1.add_subplot(gs1[1, 1]); plot_pair(ax12, chg6_basis_1y_5, chg6_res_ex_inv_5, "6M chg Basis (1Y)", "6M chg (- Reserves)", c_basis, c_res, "6M Changes (last 5y)")
    ax13 = fig1.add_subplot(gs1[1, 2]); plot_pair(ax13, chg3_basis_1y_5, chg3_res_ex_inv_5, "3M chg Basis (1Y)", "3M chg (- Reserves)", c_basis, c_res, "3M Changes (last 5y)")
    ax21 = fig1.add_subplot(gs1[2, 0]); plot_corr(ax21, corr_level_b1y_resinv_5, "Rolling 1Y Corr (levels, last 5y)")
    ax22 = fig1.add_subplot(gs1[2, 1]); plot_corr(ax22, corr_chg6_b1y_resinv_5, "Rolling 1Y Corr (6M changes, last 5y)")
    ax23 = fig1.add_subplot(gs1[2, 2]); plot_corr(ax23, corr_chg3_b1y_resinv_5, "Rolling 1Y Corr (3M changes, last 5y)")

    # Block 2: 2Y Basis vs Reserves (rows 3-4)
    ax31 = fig1.add_subplot(gs1[3, 0]); plot_pair(ax31, basis_2y_5, res_ex_inv_5, "Basis (2Y)", "- Reserves ex-gold", c_basis, c_res, "Levels (last 5y)")
    ax32 = fig1.add_subplot(gs1[3, 1]); plot_pair(ax32, chg6_basis_2y_5, chg6_res_ex_inv_5, "6M chg Basis (2Y)", "6M chg (- Reserves)", c_basis, c_res, "6M Changes (last 5y)")
    ax33 = fig1.add_subplot(gs1[3, 2]); plot_pair(ax33, chg3_basis_2y_5, chg3_res_ex_inv_5, "3M chg Basis (2Y)", "3M chg (- Reserves)", c_basis, c_res, "3M Changes (last 5y)")
    ax41 = fig1.add_subplot(gs1[4, 0]); plot_corr(ax41, corr_level_b2y_resinv_5, "Rolling 1Y Corr (levels, last 5y)")
    ax42 = fig1.add_subplot(gs1[4, 1]); plot_corr(ax42, corr_chg6_b2y_resinv_5, "Rolling 1Y Corr (6M changes, last 5y)")
    ax43 = fig1.add_subplot(gs1[4, 2]); plot_corr(ax43, corr_chg3_b2y_resinv_5, "Rolling 1Y Corr (3M changes, last 5y)")

    # Block 3: 1Y Basis vs USDINR (rows 5-6)
    ax51 = fig1.add_subplot(gs1[5, 0]); plot_pair(ax51, basis_1y_5, usd_5, "Basis (1Y)", "USDINR", c_basis, c_fx, "Levels (last 5y)")
    ax52 = fig1.add_subplot(gs1[5, 1]); plot_pair(ax52, chg6_basis_1y_5, chg6_usd_5, "6M chg Basis (1Y)", "6M chg USDINR", c_basis, c_fx, "6M Changes (last 5y)")
    ax53 = fig1.add_subplot(gs1[5, 2]); plot_pair(ax53, chg3_basis_1y_5, chg3_usd_5, "3M chg Basis (1Y)", "3M chg USDINR", c_basis, c_fx, "3M Changes (last 5y)")
    ax61 = fig1.add_subplot(gs1[6, 0]); plot_corr(ax61, corr_level_b1y_usd_5, "Rolling 1Y Corr (levels, last 5y)")
    ax62 = fig1.add_subplot(gs1[6, 1]); plot_corr(ax62, corr_chg6_b1y_usd_5, "Rolling 1Y Corr (6M changes, last 5y)")
    ax63 = fig1.add_subplot(gs1[6, 2]); plot_corr(ax63, corr_chg3_b1y_usd_5, "Rolling 1Y Corr (3M changes, last 5y)")

    for ax in [ax21, ax22, ax23, ax41, ax42, ax43, ax61, ax62, ax63]:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax.get_xticklabels(), rotation=45)

    if pd.notna(last_data_date1):
        fig1.text(0.985, 0.985, f"Last data: {pd.to_datetime(last_data_date1).date()}",
                  ha='right', va='top', fontsize=10,
                  bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'))

    fig1.suptitle("INR Basis vs USDINR & RBI Reserves ex-gold -- Multi-horizon Levels, Changes, and Correlations",
                  y=0.995, fontsize=15)
    fig1.savefig(PNG_OUT1, dpi=150, bbox_inches='tight')
    plt.close(fig1)
    print(f"Saved chart to: {PNG_OUT1}")

    # ========= Chart 2: Basis vs Banking Liquidity (5D MA) =========
    basis_3m_ma5 = ma5(basis_3m); basis_1y_ma5 = ma5(basis_1y); basis_2y_ma5 = ma5(basis_2y)
    bank_liq_ma5 = ma5(bank_liq); bank_liq_vrr_ma5 = ma5(bank_liq_vrr)

    stack_for_last2_ma5 = pd.concat([basis_3m_ma5, basis_1y_ma5, basis_2y_ma5, bank_liq_ma5, bank_liq_vrr_ma5], axis=1)
    last_data_date2_ma5 = stack_for_last2_ma5.dropna(how='all').index.max()

    chg6_basis_3m_ma5 = rolling_change(basis_3m_ma5, 126); chg3_basis_3m_ma5 = rolling_change(basis_3m_ma5, 63)
    chg6_basis_1y_ma5 = rolling_change(basis_1y_ma5, 126); chg3_basis_1y_ma5 = rolling_change(basis_1y_ma5, 63)
    chg6_basis_2y_ma5 = rolling_change(basis_2y_ma5, 126); chg3_basis_2y_ma5 = rolling_change(basis_2y_ma5, 63)
    chg6_liq_ma5 = rolling_change(bank_liq_ma5, 126); chg3_liq_ma5 = rolling_change(bank_liq_ma5, 63)
    chg6_liq_vrr_ma5 = rolling_change(bank_liq_vrr_ma5, 126); chg3_liq_vrr_ma5 = rolling_change(bank_liq_vrr_ma5, 63)

    corr_level_liq_b3m_ma5 = corr_1y(basis_3m_ma5, bank_liq_ma5); corr_chg6_liq_b3m_ma5 = corr_1y(chg6_basis_3m_ma5, chg6_liq_ma5); corr_chg3_liq_b3m_ma5 = corr_1y(chg3_basis_3m_ma5, chg3_liq_ma5)
    corr_level_liq_b1y_ma5 = corr_1y(basis_1y_ma5, bank_liq_ma5); corr_chg6_liq_b1y_ma5 = corr_1y(chg6_basis_1y_ma5, chg6_liq_ma5); corr_chg3_liq_b1y_ma5 = corr_1y(chg3_basis_1y_ma5, chg3_liq_ma5)
    corr_level_liq_b2y_ma5 = corr_1y(basis_2y_ma5, bank_liq_ma5); corr_chg6_liq_b2y_ma5 = corr_1y(chg6_basis_2y_ma5, chg6_liq_ma5); corr_chg3_liq_b2y_ma5 = corr_1y(chg3_basis_2y_ma5, chg3_liq_ma5)
    corr_level_liqv_b3m_ma5 = corr_1y(basis_3m_ma5, bank_liq_vrr_ma5); corr_chg6_liqv_b3m_ma5 = corr_1y(chg6_basis_3m_ma5, chg6_liq_vrr_ma5); corr_chg3_liqv_b3m_ma5 = corr_1y(chg3_basis_3m_ma5, chg3_liq_vrr_ma5)
    corr_level_liqv_b1y_ma5 = corr_1y(basis_1y_ma5, bank_liq_vrr_ma5); corr_chg6_liqv_b1y_ma5 = corr_1y(chg6_basis_1y_ma5, chg6_liq_vrr_ma5); corr_chg3_liqv_b1y_ma5 = corr_1y(chg3_basis_1y_ma5, chg3_liq_vrr_ma5)
    corr_level_liqv_b2y_ma5 = corr_1y(basis_2y_ma5, bank_liq_vrr_ma5); corr_chg6_liqv_b2y_ma5 = corr_1y(chg6_basis_2y_ma5, chg6_liq_vrr_ma5); corr_chg3_liqv_b2y_ma5 = corr_1y(chg3_basis_2y_ma5, chg3_liq_vrr_ma5)

    # 5y slices for MA
    basis_3m_ma5_5 = last5y(basis_3m_ma5); basis_1y_ma5_5 = last5y(basis_1y_ma5); basis_2y_ma5_5 = last5y(basis_2y_ma5)
    bank_liq_ma5_5 = last5y(bank_liq_ma5); bank_liq_vrr_ma5_5 = last5y(bank_liq_vrr_ma5)
    chg6_basis_3m_ma5_5 = last5y(chg6_basis_3m_ma5); chg3_basis_3m_ma5_5 = last5y(chg3_basis_3m_ma5)
    chg6_basis_1y_ma5_5 = last5y(chg6_basis_1y_ma5); chg3_basis_1y_ma5_5 = last5y(chg3_basis_1y_ma5)
    chg6_basis_2y_ma5_5 = last5y(chg6_basis_2y_ma5); chg3_basis_2y_ma5_5 = last5y(chg3_basis_2y_ma5)
    chg6_liq_ma5_5 = last5y(chg6_liq_ma5); chg3_liq_ma5_5 = last5y(chg3_liq_ma5)
    chg6_liq_vrr_ma5_5 = last5y(chg6_liq_vrr_ma5); chg3_liq_vrr_ma5_5 = last5y(chg3_liq_vrr_ma5)
    corr_level_liq_b3m_ma5_5 = last5y(corr_level_liq_b3m_ma5); corr_chg6_liq_b3m_ma5_5 = last5y(corr_chg6_liq_b3m_ma5); corr_chg3_liq_b3m_ma5_5 = last5y(corr_chg3_liq_b3m_ma5)
    corr_level_liq_b1y_ma5_5 = last5y(corr_level_liq_b1y_ma5); corr_chg6_liq_b1y_ma5_5 = last5y(corr_chg6_liq_b1y_ma5); corr_chg3_liq_b1y_ma5_5 = last5y(corr_chg3_liq_b1y_ma5)
    corr_level_liq_b2y_ma5_5 = last5y(corr_level_liq_b2y_ma5); corr_chg6_liq_b2y_ma5_5 = last5y(corr_chg6_liq_b2y_ma5); corr_chg3_liq_b2y_ma5_5 = last5y(corr_chg3_liq_b2y_ma5)
    corr_level_liqv_b3m_ma5_5 = last5y(corr_level_liqv_b3m_ma5); corr_chg6_liqv_b3m_ma5_5 = last5y(corr_chg6_liqv_b3m_ma5); corr_chg3_liqv_b3m_ma5_5 = last5y(corr_chg3_liqv_b3m_ma5)
    corr_level_liqv_b1y_ma5_5 = last5y(corr_level_liqv_b1y_ma5); corr_chg6_liqv_b1y_ma5_5 = last5y(corr_chg6_liqv_b1y_ma5); corr_chg3_liqv_b1y_ma5_5 = last5y(corr_chg3_liqv_b1y_ma5)
    corr_level_liqv_b2y_ma5_5 = last5y(corr_level_liqv_b2y_ma5); corr_chg6_liqv_b2y_ma5_5 = last5y(corr_chg6_liqv_b2y_ma5); corr_chg3_liqv_b2y_ma5_5 = last5y(corr_chg3_liqv_b2y_ma5)

    c_b3m = '#1f77b4'; c_b1y = '#155a9c'; c_b2y = '#0b2f6b'; c_liq = '#2ca02c'; c_liqv = '#d62728'

    fig2 = plt.figure(figsize=(18, 34), constrained_layout=True)
    gs2 = fig2.add_gridspec(nrows=13, ncols=3, height_ratios=[2.4] + [1.3]*12)

    ax2_top = fig2.add_subplot(gs2[0, :])
    ax2_top.plot(basis_3m_ma5.index, basis_3m_ma5.values, color=c_b3m, label='INR 3M Basis (5D MA, LHS)', linewidth=1.6)
    ax2_top.plot(basis_1y_ma5.index, basis_1y_ma5.values, color=c_b1y, label='INR 1Y Basis (5D MA, LHS)', linewidth=1.8)
    ax2_top.plot(basis_2y_ma5.index, basis_2y_ma5.values, color=c_b2y, label='INR 2Y Basis (5D MA, LHS)', linewidth=1.8)
    ax2_top.set_ylabel("Basis (5D MA)", color=c_b1y); ax2_top.tick_params(axis='y', colors=c_b1y)
    ax2_top.grid(True, linestyle=':', alpha=0.5)
    ax2_top_r = ax2_top.twinx()
    ax2_top_r.plot(bank_liq_ma5.index, bank_liq_ma5.values, color=c_liq, label='Banking Liquidity (5D MA, RHS)', linewidth=1.6, linestyle='--')
    ax2_top_r.plot(bank_liq_vrr_ma5.index, bank_liq_vrr_ma5.values, color=c_liqv, label='Banking Liq incl. VRR/VRRR (5D MA, RHS)', linewidth=1.6, linestyle='--')
    ax2_top_r.set_ylabel("Liquidity (5D MA)", color=c_liq); ax2_top_r.tick_params(axis='y', colors=c_liq)
    hL, lL = ax2_top.get_legend_handles_labels(); hR, lR = ax2_top_r.get_legend_handles_labels()
    ax2_top.legend(hL + hR, lL + lR, loc='upper left', fontsize=9)
    ax2_top.set_title("INR Basis (5D MA) vs Banking Liquidity (5D MA, incl. VRR/VRRR)")

    # Section A: Banking Liquidity vs 3M/1Y/2Y
    a11 = fig2.add_subplot(gs2[1, 0]); plot_pair(a11, basis_3m_ma5_5, bank_liq_ma5_5, "Basis (3M, 5D MA)", "Banking Liq (5D MA)", c_b3m, c_liq, "Levels (last 5y)")
    a12 = fig2.add_subplot(gs2[1, 1]); plot_pair(a12, chg6_basis_3m_ma5_5, chg6_liq_ma5_5, "6M chg Basis (3M)", "6M chg Liq", c_b3m, c_liq, "6M Changes (last 5y)")
    a13 = fig2.add_subplot(gs2[1, 2]); plot_pair(a13, chg3_basis_3m_ma5_5, chg3_liq_ma5_5, "3M chg Basis (3M)", "3M chg Liq", c_b3m, c_liq, "3M Changes (last 5y)")
    a21 = fig2.add_subplot(gs2[2, 0]); plot_corr(a21, corr_level_liq_b3m_ma5_5, "Rolling 1Y Corr (levels)")
    a22 = fig2.add_subplot(gs2[2, 1]); plot_corr(a22, corr_chg6_liq_b3m_ma5_5, "Rolling 1Y Corr (6M changes)")
    a23 = fig2.add_subplot(gs2[2, 2]); plot_corr(a23, corr_chg3_liq_b3m_ma5_5, "Rolling 1Y Corr (3M changes)")

    b11 = fig2.add_subplot(gs2[3, 0]); plot_pair(b11, basis_1y_ma5_5, bank_liq_ma5_5, "Basis (1Y, 5D MA)", "Banking Liq (5D MA)", c_b1y, c_liq, "Levels (last 5y)")
    b12 = fig2.add_subplot(gs2[3, 1]); plot_pair(b12, chg6_basis_1y_ma5_5, chg6_liq_ma5_5, "6M chg Basis (1Y)", "6M chg Liq", c_b1y, c_liq, "6M Changes (last 5y)")
    b13 = fig2.add_subplot(gs2[3, 2]); plot_pair(b13, chg3_basis_1y_ma5_5, chg3_liq_ma5_5, "3M chg Basis (1Y)", "3M chg Liq", c_b1y, c_liq, "3M Changes (last 5y)")
    b21 = fig2.add_subplot(gs2[4, 0]); plot_corr(b21, corr_level_liq_b1y_ma5_5, "Rolling 1Y Corr (levels)")
    b22 = fig2.add_subplot(gs2[4, 1]); plot_corr(b22, corr_chg6_liq_b1y_ma5_5, "Rolling 1Y Corr (6M changes)")
    b23 = fig2.add_subplot(gs2[4, 2]); plot_corr(b23, corr_chg3_liq_b1y_ma5_5, "Rolling 1Y Corr (3M changes)")

    c11 = fig2.add_subplot(gs2[5, 0]); plot_pair(c11, basis_2y_ma5_5, bank_liq_ma5_5, "Basis (2Y, 5D MA)", "Banking Liq (5D MA)", c_b2y, c_liq, "Levels (last 5y)")
    c12 = fig2.add_subplot(gs2[5, 1]); plot_pair(c12, chg6_basis_2y_ma5_5, chg6_liq_ma5_5, "6M chg Basis (2Y)", "6M chg Liq", c_b2y, c_liq, "6M Changes (last 5y)")
    c13 = fig2.add_subplot(gs2[5, 2]); plot_pair(c13, chg3_basis_2y_ma5_5, chg3_liq_ma5_5, "3M chg Basis (2Y)", "3M chg Liq", c_b2y, c_liq, "3M Changes (last 5y)")
    c21 = fig2.add_subplot(gs2[6, 0]); plot_corr(c21, corr_level_liq_b2y_ma5_5, "Rolling 1Y Corr (levels)")
    c22 = fig2.add_subplot(gs2[6, 1]); plot_corr(c22, corr_chg6_liq_b2y_ma5_5, "Rolling 1Y Corr (6M changes)")
    c23 = fig2.add_subplot(gs2[6, 2]); plot_corr(c23, corr_chg3_liq_b2y_ma5_5, "Rolling 1Y Corr (3M changes)")

    # Section B: Banking Liq incl. VRR/VRRR vs 3M/1Y/2Y
    d11 = fig2.add_subplot(gs2[7, 0]); plot_pair(d11, basis_3m_ma5_5, bank_liq_vrr_ma5_5, "Basis (3M, 5D MA)", "Liq incl VRR/VRRR", c_b3m, c_liqv, "Levels (last 5y)")
    d12 = fig2.add_subplot(gs2[7, 1]); plot_pair(d12, chg6_basis_3m_ma5_5, chg6_liq_vrr_ma5_5, "6M chg Basis (3M)", "6M chg Liq+VRR", c_b3m, c_liqv, "6M Changes (last 5y)")
    d13 = fig2.add_subplot(gs2[7, 2]); plot_pair(d13, chg3_basis_3m_ma5_5, chg3_liq_vrr_ma5_5, "3M chg Basis (3M)", "3M chg Liq+VRR", c_b3m, c_liqv, "3M Changes (last 5y)")
    d21 = fig2.add_subplot(gs2[8, 0]); plot_corr(d21, corr_level_liqv_b3m_ma5_5, "Rolling 1Y Corr (levels)")
    d22 = fig2.add_subplot(gs2[8, 1]); plot_corr(d22, corr_chg6_liqv_b3m_ma5_5, "Rolling 1Y Corr (6M changes)")
    d23 = fig2.add_subplot(gs2[8, 2]); plot_corr(d23, corr_chg3_liqv_b3m_ma5_5, "Rolling 1Y Corr (3M changes)")

    e11 = fig2.add_subplot(gs2[9, 0]); plot_pair(e11, basis_1y_ma5_5, bank_liq_vrr_ma5_5, "Basis (1Y, 5D MA)", "Liq incl VRR/VRRR", c_b1y, c_liqv, "Levels (last 5y)")
    e12 = fig2.add_subplot(gs2[9, 1]); plot_pair(e12, chg6_basis_1y_ma5_5, chg6_liq_vrr_ma5_5, "6M chg Basis (1Y)", "6M chg Liq+VRR", c_b1y, c_liqv, "6M Changes (last 5y)")
    e13 = fig2.add_subplot(gs2[9, 2]); plot_pair(e13, chg3_basis_1y_ma5_5, chg3_liq_vrr_ma5_5, "3M chg Basis (1Y)", "3M chg Liq+VRR", c_b1y, c_liqv, "3M Changes (last 5y)")
    e21 = fig2.add_subplot(gs2[10, 0]); plot_corr(e21, corr_level_liqv_b1y_ma5_5, "Rolling 1Y Corr (levels)")
    e22 = fig2.add_subplot(gs2[10, 1]); plot_corr(e22, corr_chg6_liqv_b1y_ma5_5, "Rolling 1Y Corr (6M changes)")
    e23 = fig2.add_subplot(gs2[10, 2]); plot_corr(e23, corr_chg3_liqv_b1y_ma5_5, "Rolling 1Y Corr (3M changes)")

    f11 = fig2.add_subplot(gs2[11, 0]); plot_pair(f11, basis_2y_ma5_5, bank_liq_vrr_ma5_5, "Basis (2Y, 5D MA)", "Liq incl VRR/VRRR", c_b2y, c_liqv, "Levels (last 5y)")
    f12 = fig2.add_subplot(gs2[11, 1]); plot_pair(f12, chg6_basis_2y_ma5_5, chg6_liq_vrr_ma5_5, "6M chg Basis (2Y)", "6M chg Liq+VRR", c_b2y, c_liqv, "6M Changes (last 5y)")
    f13 = fig2.add_subplot(gs2[11, 2]); plot_pair(f13, chg3_basis_2y_ma5_5, chg3_liq_vrr_ma5_5, "3M chg Basis (2Y)", "3M chg Liq+VRR", c_b2y, c_liqv, "3M Changes (last 5y)")
    f21 = fig2.add_subplot(gs2[12, 0]); plot_corr(f21, corr_level_liqv_b2y_ma5_5, "Rolling 1Y Corr (levels)")
    f22 = fig2.add_subplot(gs2[12, 1]); plot_corr(f22, corr_chg6_liqv_b2y_ma5_5, "Rolling 1Y Corr (6M changes)")
    f23 = fig2.add_subplot(gs2[12, 2]); plot_corr(f23, corr_chg3_liqv_b2y_ma5_5, "Rolling 1Y Corr (3M changes)")

    corr_axes_2 = [a21, a22, a23, b21, b22, b23, c21, c22, c23, d21, d22, d23, e21, e22, e23, f21, f22, f23]
    for ax in corr_axes_2:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax.get_xticklabels(), rotation=45)

    if pd.notna(last_data_date2_ma5):
        fig2.text(0.985, 0.985, f"Last data: {pd.to_datetime(last_data_date2_ma5).date()}",
                  ha='right', va='top', fontsize=10,
                  bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'))

    fig2.suptitle("INR Basis vs Banking Liquidity (5D MA) -- Multi-horizon Levels, Changes, and Correlations",
                  y=0.995, fontsize=15)
    fig2.savefig(PNG_OUT2, dpi=150, bbox_inches='tight')
    plt.close(fig2)
    print(f"Saved chart to: {PNG_OUT2}")




def chart_universal_claimants():
    """DWP Universal Claimants vs UK Inactive Long-Term Sick -- from charts_updater5.py"""
    END_DATE = datetime.today()

    BASE_URL = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1"
    DB_ID = "str:database:UC_Monthly"
    MEASURE_ID = "str:count:UC_Monthly:V_F_UC_CASELOAD_FULL"
    MONTH_FIELD_ID = "str:field:UC_Monthly:F_UC_DATE:DATE_NAME"
    COND_FIELD_ID = "str:field:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME"
    NO_WORK_VALUE_ID = (
        "str:value:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME:"
        "C_UC_CONDITIONALITY_REGIME:BC"
    )
    API_KEY = '65794a30655841694f694a4b563151694c434a68624763694f694a49557a49314e694a392e65794a7063334d694f694a7a644849756333526c6247786863694973496e4e3159694936496d746e6457396d5a57356e4d5445785147647459576c734c6d4e7662534973496d6c68644349364d5463324e7a4d7a4f5441774d4377695958566b496a6f69633352794c6d396b59534a392e775a546b38737577764158553750433476376c327155796f6a714e676f6e334c4463497179357438617673'
    if not API_KEY:
        raise RuntimeError("Set STATXPLORE_API_KEY env var first.")

    session = requests.Session()
    session.headers.update({
        "APIKey": API_KEY,
        "Accept-Language": "en",
        "User-Agent": "statxplore-python/1.0",
    })
    RETRIABLE = {429, 502, 503, 504}

    def request_json(method, url, params=None, json_body=None, timeout=180, max_attempts=8):
        for attempt in range(1, max_attempts + 1):
            resp = session.request(method, url, params=params, json=json_body, timeout=timeout)
            if resp.status_code in RETRIABLE:
                ra = resp.headers.get("Retry-After")
                if ra and ra.isdigit():
                    delay = int(ra)
                else:
                    delay = min(60, 2 ** (attempt - 1)) + random.uniform(0, 0.5)
                if attempt == max_attempts:
                    raise requests.HTTPError(f"{resp.status_code} from {url}\n{resp.text[:2000]}", response=resp)
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError("Unreachable")

    def find_cube_values(obj):
        def looks_like_values_list(x):
            if not isinstance(x, list) or any(isinstance(v, (list, dict)) for v in x):
                return False
            for v in x:
                if v is None: continue
                if isinstance(v, (int, float)): continue
                if isinstance(v, str):
                    try: float(v)
                    except ValueError: return False
                else: return False
            return True
        if looks_like_values_list(obj): return obj
        if isinstance(obj, dict):
            for k in ("values", "value", "cells", "data"):
                if k in obj:
                    hit = find_cube_values(obj[k])
                    if hit is not None: return hit
            for v in obj.values():
                hit = find_cube_values(v)
                if hit is not None: return hit
        if isinstance(obj, list):
            for v in obj:
                hit = find_cube_values(v)
                if hit is not None: return hit
        return None

    def get_first_cube(resp_json):
        cubes = resp_json.get("cubes")
        if cubes is None:
            raise RuntimeError(f"No 'cubes' in response. Keys: {list(resp_json.keys())}")
        if isinstance(cubes, list):
            if not cubes: raise RuntimeError("'cubes' is empty list.")
            return cubes[0]
        if isinstance(cubes, dict):
            first = next(iter(cubes.values()), None)
            if first is None: raise RuntimeError("'cubes' is empty dict.")
            return first
        raise RuntimeError(f"Unexpected 'cubes' type: {type(cubes)}")

    def _flatten_listlike(x):
        out = []
        def rec(v):
            if isinstance(v, (list, tuple)):
                for w in v: rec(w)
            else: out.append(v)
        rec(x)
        return out

    def _extract_cube_values_flat(resp_json, measure_idx=0):
        cubes = resp_json.get("cubes")
        if cubes is None: raise RuntimeError(f"No 'cubes'. Keys: {list(resp_json.keys())}")
        if isinstance(cubes, list): cube = cubes[measure_idx]
        elif isinstance(cubes, dict): cube = list(cubes.values())[measure_idx]
        else: raise RuntimeError(f"Unexpected 'cubes' type: {type(cubes)}")
        if isinstance(cube, dict):
            if len(cube) == 1: cube_payload = next(iter(cube.values()))
            elif "values" in cube: cube_payload = cube["values"]
            else: cube_payload = next(iter(cube.values()))
        else: cube_payload = cube
        return _flatten_listlike(cube_payload)

    def _expand_grid_like_tidyr(lists):
        rev = lists[::-1]
        for prod in itertools.product(*rev):
            yield prod[::-1]

    def table_to_df(resp_json):
        fields = resp_json["fields"]
        field_labels = [f["label"] for f in fields]
        items_labels = []
        for f in fields:
            labels = []
            for it in f["items"]:
                lbl = it.get("labels")
                if isinstance(lbl, list): labels.append(" / ".join(map(str, lbl)))
                else: labels.append(str(lbl))
            items_labels.append(labels)
        combos = list(_expand_grid_like_tidyr(items_labels))
        values = _extract_cube_values_flat(resp_json, measure_idx=0)
        if len(values) != len(combos):
            raise RuntimeError(f"Mismatch: {len(values)} values vs {len(combos)} combos.")
        df = pd.DataFrame(combos, columns=field_labels)
        measure_label = resp_json["measures"][0]["label"]
        df[measure_label] = pd.to_numeric(values, errors="coerce")
        return df

    def parse_month(s):
        s = str(s).strip()
        for fmt in ("%b %Y", "%B %Y", "%Y-%m", "%b-%Y"):
            try: return pd.to_datetime(datetime.strptime(s, fmt).date())
            except ValueError: pass
        return pd.to_datetime(s, errors="coerce")

    def _field_label_by_id(resp_json, field_id, fallback_idx):
        for f in resp_json.get("fields", []):
            if f.get("id") == field_id or f.get("uri") == field_id or f.get("name") == field_id:
                return f.get("label", resp_json["fields"][fallback_idx]["label"])
        return resp_json["fields"][fallback_idx]["label"]

    query = {
        "database": DB_ID,
        "measures": [MEASURE_ID],
        "recodes": { COND_FIELD_ID: { "map": [[NO_WORK_VALUE_ID]], "total": False } },
        "dimensions": [ [MONTH_FIELD_ID], [COND_FIELD_ID] ]
    }
    resp = request_json("POST", f"{BASE_URL}/table", json_body=query)
    df = table_to_df(resp)

    month_col = _field_label_by_id(resp, MONTH_FIELD_ID, fallback_idx=0)
    cond_col = _field_label_by_id(resp, COND_FIELD_ID, fallback_idx=1)
    measure_label = resp["measures"][0]["label"]

    if cond_col in df.columns and df[cond_col].nunique(dropna=True) > 1:
        df = df[df[cond_col].str.lower().str.contains("no work", na=False)].copy()
    df = df[df[month_col].str.lower() != "total"].copy()
    df["Month"] = df[month_col].map(parse_month)
    df = df.dropna(subset=["Month"]).sort_values("Month")

    dwp_series = pd.Series(df[measure_label].values, index=pd.to_datetime(df["Month"]))
    dwp_series.name = "Universal Claimants: No work requirements"
    dwp_series = _u5_normalize_monthly_series(dwp_series, "DWP")
    if dwp_series.dropna().empty:
        raise RuntimeError("DWP series contains no valid data after cleaning.")
    last_dwp = dwp_series.dropna().index.max()

    START_DATE = dwp_series.index.min().to_pydatetime()
    tickers = ["UKLFLF69 Index"]
    bbg = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(bbg.columns, pd.MultiIndex):
        bbg.columns = bbg.columns.get_level_values(0)
    bbg.index = pd.to_datetime(bbg.index)
    bbg_series = bbg["UKLFLF69 Index"].dropna()
    bbg_series.name = "UK Inactive Long-Term Sick"
    if bbg_series.empty:
        raise RuntimeError("Bloomberg series 'UKLFLF69 Index' returned no data.")
    last_bbg = bbg_series.index[-1]

    PLOT_START = datetime(2015, 1, 1)
    fig, ax = plt.subplots(figsize=(12, 6))
    mask_dwp = dwp_series.index >= PLOT_START
    ax.plot(dwp_series.index[mask_dwp], dwp_series[mask_dwp], color="#1f77b4", linewidth=2, label="Universal Claimants: No work requirements")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Universal Claimants", color="#1f77b4", fontsize=12)
    ax.tick_params(axis="y", labelcolor="#1f77b4")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.xticks(rotation=45)

    ax2 = ax.twinx()
    mask_bbg = bbg_series.index >= PLOT_START
    ax2.plot(bbg_series.index[mask_bbg], bbg_series[mask_bbg], color="#ff7f0e", linewidth=2, alpha=0.9, label="UK Inactive Long-Term Sick")
    ax2.set_ylabel("UK Inactive Long-Term Sick", color="#ff7f0e", fontsize=12)
    ax2.tick_params(axis="y", labelcolor="#ff7f0e")

    ax.set_xlim(PLOT_START, max(last_dwp, last_bbg))
    ax.set_title("Universal Claimants (No work requirements) vs UK Inactive Long-Term Sick", fontsize=14)
    lines = ax.get_lines() + ax2.get_lines()
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc="upper left", fontsize=10)

    plt.text(0.95, 0.95, f"Last DWP data: {last_dwp.strftime('%Y-%m-%d')}\nLast BBG data: {last_bbg.strftime('%Y-%m-%d')}",
             horizontalalignment="right", verticalalignment="top", transform=ax.transAxes, fontsize=10,
             bbox=dict(facecolor="white", alpha=0.7))

    plt.tight_layout()
    SAVE_PATH = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
    OUTFILE   = SAVE_PATH / "UniversalClaimants_NoWorkReq_vs_UKInactive_LongTermSick.png"
    plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved chart to: {OUTFILE}")


def chart_indonesia_money_supply_yoy():
    """Indonesia Money Supply YoY -- from charts_updater5.py"""
    END_DATE = datetime.today()

    TICKERS = {
        'IDM1PYOY Index': 'M1 YoY',
        'IDM2YOY Index':  'M2 YoY',
    }
    START_DATE = datetime(2019, 1, 1)
    PLOT_START = datetime(2020, 1, 1)
    SAVE_PATH = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
    OUTFILE   = SAVE_PATH / "Indonesia_MoneySupply_YoY.png"

    try:
        raw = blp.bdh(tickers=list(TICKERS.keys()), flds='PX_LAST',
                      start_date=START_DATE, end_date=END_DATE, Per='M', Fill='P')
    except TypeError:
        raw = blp.bdh(tickers=list(TICKERS.keys()), flds='PX_LAST',
                      start_date=START_DATE, end_date=END_DATE, periodicitySelection='MONTHLY')

    m0_level = get_adjusted_m0_data()
    m0_level = _u5_normalize_monthly_series(m0_level, "BI Adjusted M0")

    if isinstance(raw.columns, pd.MultiIndex):
        if "PX_LAST" in raw.columns.get_level_values(-1):
            df = raw.xs("PX_LAST", level=-1, axis=1)
        else:
            df = raw.xs("PX_LAST", level=0, axis=1)
    else:
        df = raw.copy()
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric, errors='coerce')
    df = df.rename(columns=TICKERS)
    df.index = df.index.to_period('M').to_timestamp('M')
    df = df.sort_index()

    base_yoy = compute_yoy_growth(m0_level)
    base_yoy = _u5_normalize_monthly_series(base_yoy, "BI Adjusted M0 YoY")

    if "M1 YoY" not in df.columns or "M2 YoY" not in df.columns:
        raise RuntimeError(f"Missing expected Bloomberg series: {list(df.columns)}")
    full_idx = df.index.union(base_yoy.index).sort_values()
    plot_df = pd.DataFrame(index=full_idx)
    plot_df['M1 YoY']         = df['M1 YoY'].reindex(full_idx)
    plot_df['M2 YoY']         = df['M2 YoY'].reindex(full_idx)
    plot_df['Base Money YoY'] = base_yoy.reindex(full_idx)
    plot_df = plot_df[plot_df.index >= PLOT_START]

    last_data_date = plot_df.dropna(how='all').index.max()
    fig, ax = plt.subplots(1, 1, figsize=(14, 6))
    l1, = ax.plot(plot_df.index, plot_df['M1 YoY'].values, lw=2.0, label='M1 YoY (Index)')
    l2, = ax.plot(plot_df.index, plot_df['M2 YoY'].values, lw=2.0, label='M2 YoY (Index)')
    l3, = ax.plot(plot_df.index, plot_df['Base Money YoY'].values, lw=2.0, label='Base Money YoY')

    ax.axhline(0, lw=1, zorder=0)
    ax.grid(True, linestyle=':', alpha=0.5)
    ax.set_title("Indonesia Money Growth (YoY %, Monthly)", fontsize=15)
    ax.set_ylabel("YoY (%)")
    ax.set_xlabel("Date")
    ax.legend(loc='upper left', fontsize=9)

    series_info = [("M1 YoY", l1.get_color()), ("M2 YoY", l2.get_color()), ("Base Money YoY", l3.get_color())]
    last_pts = []
    for col, color in series_info:
        s = plot_df[col].dropna()
        if not s.empty:
            d = s.index.max()
            v = float(s.loc[d])
            last_pts.append((col, d, v, color))
    last_pts_sorted = sorted(last_pts, key=lambda x: x[2])
    n = len(last_pts_sorted)
    spacing = 14; mid = (n - 1) / 2.0
    y_offsets = {}
    for i, (col, d, v, color) in enumerate(last_pts_sorted):
        y_offsets[col] = int((i - mid) * spacing)
    for col, d, v, color in last_pts:
        ax.scatter([d], [v], color=color, edgecolor='white', linewidth=0.8, zorder=6)
        ax.annotate(f"{v:+.1f}%", xy=(d, v), xytext=(10, y_offsets[col]),
                    textcoords='offset points', ha='left', va='center', color=color,
                    bbox=dict(facecolor='white', edgecolor=color, alpha=0.85, boxstyle='round,pad=0.2'), zorder=7)

    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax.get_xticklabels(), rotation=45)

    fig.text(0.98, 0.96, f"Last data: {pd.to_datetime(last_data_date).date()}",
             ha='right', va='top', fontsize=10,
             bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'))
    plt.tight_layout()
    plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved chart to: {OUTFILE}")


def chart_usdvnd_t123_ftse():
    """USDVND T123 vs FTSE Fix -- from charts_updater5.py"""
    END_DATE = datetime.today()

    USD_TKR  = "USDVND Curncy"
    T123_TKR = "VND T123 Curncy"
    FTSE_TKR = "VND FTSE Curncy"
    START_DATE = END_DATE - relativedelta(years=10)
    PIP_SIZE = 1.0; PIP_X_TH = 15.0; PCT_X_TH = 0.3
    SAVE_PATH = Path(G_CHART_DIR) if "G_CHART_DIR" in globals() else Path.cwd()
    OUTFILE   = SAVE_PATH / "USDVND_T123_FTSE_and_diffs_10y.png"

    raw = blp.bdh(tickers=[USD_TKR, T123_TKR, FTSE_TKR],
                  flds=["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_BID"],
                  start_date=START_DATE, end_date=END_DATE)
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        if USD_TKR not in df.columns.get_level_values(0) and USD_TKR in df.columns.get_level_values(1):
            df.columns = df.columns.swaplevel(0, 1)
        df.columns = df.columns.set_names(["ticker", "field"])
    else:
        raise ValueError("Expected MultiIndex columns (ticker, field).")
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric, errors="coerce")

    spot = df[USD_TKR][["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"]].rename(columns={
        "PX_OPEN": "Open", "PX_HIGH": "High", "PX_LOW": "Low", "PX_LAST": "Close"})
    t123 = df[T123_TKR]["PX_LAST"].rename("T123")
    ftse = df[FTSE_TKR]["PX_BID"].rename("FTSE")
    all_df = pd.concat([spot, t123, ftse], axis=1)

    diff_df = all_df[["T123", "FTSE"]].dropna().copy()
    diff_df["Diff_pips"] = (diff_df["FTSE"] - diff_df["T123"]) / PIP_SIZE
    diff_df["Diff_pct"]  = (diff_df["FTSE"] / diff_df["T123"] - 1.0) * 100.0

    rng_df = all_df[["Low", "High", "FTSE"]].dropna().copy()
    oor_mask = (rng_df["FTSE"] < rng_df["Low"]) | (rng_df["FTSE"] > rng_df["High"])
    oor_dates = rng_df.index[oor_mask]

    fig, (ax_top, ax_pip, ax_pct) = plt.subplots(
        3, 1, figsize=(16, 10), sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.2, 1.2], "hspace": 0.08})

    ax_top.plot(all_df.index, all_df["Close"], lw=1.6, label="USDVND Spot (Close)")
    ax_top.plot(all_df.index, all_df["T123"],  lw=1.3, label="T123 (PX_LAST)")
    ax_top.plot(all_df.index, all_df["FTSE"],  lw=1.3, label="FTSE Fix (PX_BID)")
    for d in oor_dates:
        ax_top.axvspan(d - pd.Timedelta(hours=12), d + pd.Timedelta(hours=12), alpha=0.18, color="red")
    ax_top.set_title("USDVND Spot vs T123 vs FTSE (10Y) -- shaded = FTSE outside spot Low/High")
    ax_top.set_ylabel("USDVND")
    ax_top.grid(True, linestyle=":", alpha=0.5)
    ax_top.legend(loc="upper left", fontsize=9)

    x = diff_df.index; pip_vals = diff_df["Diff_pips"]
    ax_pip.bar(x, pip_vals.values, width=1.0)
    ax_pip.axhline(0, linestyle="--", alpha=0.6)
    ax_pip.set_ylabel("FTSE - T123 (pips)")
    ax_pip.grid(True, linestyle=":", alpha=0.5)
    pip_x_mask = pip_vals.abs() > PIP_X_TH
    ax_pip.scatter(x[pip_x_mask], pip_vals[pip_x_mask].values, marker="x", s=50, linewidths=2, zorder=5, label=f"|diff| > {PIP_X_TH:g} pips")
    ax_pip.legend(loc="upper left", fontsize=9)

    pct_vals = diff_df["Diff_pct"]
    ax_pct.bar(x, pct_vals.values, width=1.0)
    ax_pct.axhline(0, linestyle="--", alpha=0.6)
    ax_pct.set_ylabel("FTSE - T123 (%)")
    ax_pct.set_xlabel("Date")
    ax_pct.grid(True, linestyle=":", alpha=0.5)
    pct_x_mask = pct_vals.abs() > PCT_X_TH
    ax_pct.scatter(x[pct_x_mask], pct_vals[pct_x_mask].values, marker="x", s=50, linewidths=2, zorder=5, label=f"|diff| > {PCT_X_TH:g}%")
    ax_pct.legend(loc="upper left", fontsize=9)

    plt.tight_layout()
    plt.savefig(OUTFILE, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to: {OUTFILE}")


def chart_mas_dli():
    """MAS DLI Charts -- from charts_updater5.py"""
    END_DATE = datetime.today()

    MAS_DLI_TICKER_NEER = "CTSGSGD Index"
    MAS_DLI_TICKER_SORA = "SORACA3M Index"
    MAS_DLI_CSV_PATH = Path(os.path.dirname(os.path.abspath(__file__))) / "mas_dli_from_excel.csv"
    MAS_DLI_START_DATE = datetime(2011, 10, 1)
    MAS_DLI_END_DATE = END_DATE
    MAS_DLI_OUTFILE = G_CHART_DIR / "MAS_DLI_Charts.png"

    mas_dli_3m = _mas_dli_load_dli_csv(MAS_DLI_CSV_PATH)
    print(f"MAS DLI CSV loaded: {len(mas_dli_3m)} points")

    mas_dli_daily = _mas_dli_fetch_daily_bbg([MAS_DLI_TICKER_NEER, MAS_DLI_TICKER_SORA], MAS_DLI_START_DATE, MAS_DLI_END_DATE)
    mas_dli_m = _mas_dli_to_monthly_bm_last(mas_dli_daily)

    mas_dli_proxy_3m = _mas_dli_compute_proxy_3m(mas_dli_m[MAS_DLI_TICKER_NEER], mas_dli_m[MAS_DLI_TICKER_SORA])
    mas_dli_proxy_m = _mas_dli_compute_proxy_monthly_bc(mas_dli_m[MAS_DLI_TICKER_NEER], mas_dli_m[MAS_DLI_TICKER_SORA])

    mas_dli_proxy_3m.index = mas_dli_proxy_3m.index.to_period('M').to_timestamp()
    mas_dli_3m.index = mas_dli_3m.index.to_period('M').to_timestamp()

    mas_dli_df_all = mas_dli_proxy_3m.join(mas_dli_3m.rename("MAS_DLI_3m"), how="left")
    mas_dli_df_all = mas_dli_df_all.dropna(subset=["Proxy_SORA_3m", "NEER_contrib_3m", "SORA_contrib_3m"], how="any")

    mas_dli_df_chart2 = mas_dli_df_all[mas_dli_df_all.index >= pd.Timestamp("2019-01-01")].copy()
    mas_dli_proxy_m = mas_dli_proxy_m.dropna(subset=["Proxy_m", "NEER_contrib_m", "SORA_contrib_m"])

    mas_dli_two_years_ago = pd.Timestamp.now() - pd.DateOffset(years=2)
    mas_dli_proxy_m_2y = mas_dli_proxy_m[mas_dli_proxy_m.index >= mas_dli_two_years_ago].copy()

    mas_dli_fig, (mas_dli_ax1, mas_dli_ax2, mas_dli_ax3) = plt.subplots(3, 1, figsize=(14, 15))

    # Subplot 1: MAS DLI vs Proxy
    mas_dli_ax1.plot(mas_dli_df_all.index, mas_dli_df_all["Proxy_SORA_3m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W, label="Proxy (60% S$NEER & 40% SORA, variance scaled)")
    mas_dli_ax1.plot(mas_dli_df_all.index, mas_dli_df_all["MAS_DLI_3m"], color=MAS_DLI_C_DLI, linewidth=MAS_DLI_LINE_W, label="MAS DLI")
    mas_dli_ax1.set_title("MAS DLI and Proxy", fontsize=12, fontweight='bold')
    mas_dli_ax1.grid(True, axis="y", alpha=0.3)
    mas_dli_ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    mas_dli_ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(mas_dli_ax1.get_xticklabels(), rotation=45, ha="right")
    mas_dli_ax1.set_ylim(top=1.5)
    mas_dli_ax1.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
    mas_dli_ax1.legend(loc="upper right", fontsize=9)

    proxy_last = mas_dli_df_all["Proxy_SORA_3m"].dropna()
    if len(proxy_last) > 0:
        mas_dli_ax1.annotate(f"{proxy_last.iloc[-1]:.2f}", xy=(proxy_last.index[-1], proxy_last.iloc[-1]),
                             xytext=(5, 0), textcoords='offset points', fontsize=9, color=MAS_DLI_C_PROXY,
                             bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_PROXY, alpha=0.8, boxstyle='round,pad=0.2'))
    dli_last = mas_dli_df_all["MAS_DLI_3m"].dropna()
    if len(dli_last) > 0:
        mas_dli_ax1.annotate(f"{dli_last.iloc[-1]:.2f}", xy=(dli_last.index[-1], dli_last.iloc[-1]),
                             xytext=(5, -15), textcoords='offset points', fontsize=9, color=MAS_DLI_C_DLI,
                             bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_DLI, alpha=0.8, boxstyle='round,pad=0.2'))

    # Subplot 2: Stacked 3m with lines (since 2019)
    _mas_dli_stacked_two_series_excel_like(
        ax=mas_dli_ax2, x=mas_dli_df_chart2.index,
        a=mas_dli_df_chart2["NEER_contrib_3m"], b=mas_dli_df_chart2["SORA_contrib_3m"],
        label_a="S$NEER contribution", label_b="SORA contribution",
        color_a=MAS_DLI_C_NEER_BAR, color_b=MAS_DLI_C_SORA_BAR, width_days=MAS_DLI_BAR_WIDTH_DAYS)
    mas_dli_ax2.plot(mas_dli_df_chart2.index, mas_dli_df_chart2["MAS_DLI_3m"], color=MAS_DLI_C_DLI, linewidth=MAS_DLI_LINE_W, label="MAS DLI")
    mas_dli_ax2.plot(mas_dli_df_chart2.index, mas_dli_df_chart2["Proxy_SORA_3m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W, label="Proxy")
    mas_dli_ax2.axhline(0, color=MAS_DLI_C_ZERO, linewidth=1.2)
    mas_dli_ax2.set_title("MAS DLI and Proxy (change over three months, since 2019)", fontsize=12, fontweight='bold')
    mas_dli_ax2.grid(True, axis="y", alpha=0.3)
    mas_dli_ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    mas_dli_ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(mas_dli_ax2.get_xticklabels(), rotation=45, ha="right")
    mas_dli_ax2.set_ylim(-1.0, 1.5)
    mas_dli_ax2.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
    mas_dli_ax2.legend(loc="upper right", fontsize=9)

    # Subplot 3: Monthly BC (last 2 years)
    _mas_dli_stacked_two_series_excel_like(
        ax=mas_dli_ax3, x=mas_dli_proxy_m_2y.index,
        a=mas_dli_proxy_m_2y["NEER_contrib_m"], b=mas_dli_proxy_m_2y["SORA_contrib_m"],
        label_a="S$NEER contribution (monthly, variance scaled)", label_b="SORA contribution (monthly)",
        color_a=MAS_DLI_C_NEER_BAR, color_b=MAS_DLI_C_SORA_BAR, width_days=MAS_DLI_BAR_WIDTH_DAYS)
    mas_dli_ax3.plot(mas_dli_proxy_m_2y.index, mas_dli_proxy_m_2y["Proxy_m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W, label="Proxy (monthly change)")
    mas_dli_ax3.axhline(0, color=MAS_DLI_C_ZERO, linewidth=1.2)
    mas_dli_ax3.set_title("DLI Proxy (monthly change, BC calculated, last 2 years)", fontsize=12, fontweight='bold')
    mas_dli_ax3.grid(True, axis="y", alpha=0.3)
    mas_dli_ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    mas_dli_ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(mas_dli_ax3.get_xticklabels(), rotation=45, ha="right")
    mas_dli_ax3.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
    mas_dli_ax3.legend(loc="upper right", fontsize=9)

    proxy_m_last = mas_dli_proxy_m_2y["Proxy_m"].dropna()
    if len(proxy_m_last) > 0:
        mas_dli_ax3.annotate(f"{proxy_m_last.iloc[-1]:.2f}", xy=(proxy_m_last.index[-1], proxy_m_last.iloc[-1]),
                             xytext=(5, 0), textcoords='offset points', fontsize=9, color=MAS_DLI_C_PROXY,
                             bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_PROXY, alpha=0.8, boxstyle='round,pad=0.2'))
        last_data_str = proxy_m_last.index[-1].strftime('%d %b %Y')
    else:
        last_data_str = "N/A"

    mas_dli_fig.text(0.98, 0.98, f"Last data: {last_data_str}", transform=mas_dli_fig.transFigure,
                     fontsize=10, ha='right', va='top',
                     bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))
    plt.tight_layout()
    plt.savefig(MAS_DLI_OUTFILE, dpi=150, bbox_inches='tight')
    plt.close(mas_dli_fig)
    print(f"Saved MAS DLI chart to: {MAS_DLI_OUTFILE}")


def chart_korea_mmf_aum():
    """Korea MMF Total AUM -- from charts_updater5.py"""
    END_DATE = datetime.today()

    try:
        from crawl_mmf_aum import load_cache as mmf_load_cache, save_cache as mmf_save_cache, crawl_mmf_trend
        MMF_CRAWLER_AVAILABLE = True
    except ImportError as e:
        print(f"Warning: Could not import crawl_mmf_aum module: {e}")
        MMF_CRAWLER_AVAILABLE = False

    if not MMF_CRAWLER_AVAILABLE:
        print("MMF crawler not available. Skipping MMF chart generation.")
        return

    MMF_SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    MMF_CACHE_FILE = MMF_SCRIPT_DIR / "mmf_aum_cache.csv"
    MMF_OUTFILE = Path(G_CHART_DIR) / "korea_mmf_aum.png"

    def plot_mmf_chart_for_updater(df, output_path):
        if df.empty:
            print("No MMF data to plot"); return
        df = df.sort_values("date").copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates(subset=["date"], keep="last")
        df["mmf_aum_tn"] = df["mmf_total_aum"] / 10000.0
        one_year_ago = pd.Timestamp.today().normalize() - pd.DateOffset(years=1)
        df_1y = df[df["date"] >= one_year_ago].copy()
        if df_1y.empty:
            print("No MMF data in the last year"); return
        df_1y = df_1y.set_index("date").sort_index()
        df_1y["change_30d"] = df_1y["mmf_aum_tn"].diff(30)
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), height_ratios=[1.2, 1])
        ax1.plot(df_1y.index, df_1y["mmf_aum_tn"], linewidth=1.5, color="#1f77b4")
        ax1.fill_between(df_1y.index, df_1y["mmf_aum_tn"], alpha=0.1, color="#1f77b4")
        ax1.set_title("Korea MMF Total AUM (in KRW tn) - Last 1 Year", fontsize=14, fontweight="bold")
        ax1.set_ylabel("MMF Total AUM (KRW tn)")
        from matplotlib.ticker import FuncFormatter as _FF
        ax1.yaxis.set_major_formatter(_FF(lambda x, p: f"{x:,.1f}"))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(df_1y.index.min(), df_1y.index.max())
        last_date = df_1y.index[-1]; last_val = df_1y["mmf_aum_tn"].iloc[-1]
        ax1.annotate(f"{last_val:,.1f}", xy=(last_date, last_val), xytext=(5, 0), textcoords='offset points', fontsize=10, color="#1f77b4",
                     bbox=dict(facecolor='white', edgecolor="#1f77b4", alpha=0.8, boxstyle='round,pad=0.2'))
        change_data = df_1y["change_30d"].dropna()
        if not change_data.empty:
            colors = ['#2ca02c' if x >= 0 else '#d62728' for x in change_data.values]
            ax2.bar(change_data.index, change_data.values, color=colors, alpha=0.7, width=1)
            ax2.axhline(0, color='black', linewidth=0.8)
            ax2.set_title("30-Day Change (in KRW tn)", fontsize=12, fontweight="bold")
            ax2.set_ylabel("Change (KRW tn)")
            ax2.yaxis.set_major_formatter(_FF(lambda x, p: f"{x:,.1f}"))
            ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
            plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")
            ax2.grid(True, alpha=0.3, axis='y')
            ax2.set_xlim(df_1y.index.min(), df_1y.index.max())
        fig.text(0.98, 0.98, f"Last data: {last_date.strftime('%d %b %Y')}", transform=fig.transFigure,
                 fontsize=10, ha='right', va='top',
                 bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))
        plt.tight_layout()
        plt.savefig(str(output_path), dpi=150, bbox_inches="tight")
        print(f"Saved MMF chart to: {output_path}")
        plt.close()

    try:
        mmf_cache_df = mmf_load_cache()
        mmf_needs_update = True
        if len(mmf_cache_df) > 0:
            mmf_cache_max_date = mmf_cache_df["date"].max()
            mmf_today = pd.Timestamp.today().normalize()
            if (mmf_today - mmf_cache_max_date).days <= 1:
                mmf_needs_update = False
        if mmf_needs_update:
            if len(mmf_cache_df) > 0:
                mmf_start_date = (mmf_cache_df["date"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                mmf_start_date = "2017-01-01"
            mmf_end_date = END_DATE.strftime("%Y-%m-%d")
            mmf_new_df = crawl_mmf_trend(start_date=mmf_start_date, end_date=mmf_end_date, headless=True)
            if mmf_new_df is not None and len(mmf_new_df) > 0:
                if len(mmf_cache_df) > 0:
                    mmf_combined = pd.concat([mmf_cache_df, mmf_new_df], ignore_index=True)
                else:
                    mmf_combined = mmf_new_df
                mmf_save_cache(mmf_combined)
                mmf_df = mmf_combined
            else:
                mmf_df = mmf_cache_df
        else:
            mmf_df = mmf_cache_df
        if len(mmf_df) > 0:
            plot_mmf_chart_for_updater(mmf_df, MMF_OUTFILE)
    except Exception as e:
        print(f"Error processing MMF data: {e}")
        import traceback; traceback.print_exc()


def chart_india_it_services():
    """India IT Services Companies -- from charts_updater5.py"""
    END_DATE = datetime.today()

    INDIA_IT_TICKERS = ['TCS IN Equity', 'WPRO IN Equity', 'HCLT IN Equity', 'INFO IN Equity', 'CTSH US Equity']
    INDIA_IT_NAMES = {
        'TCS IN Equity': 'TCS', 'WPRO IN Equity': 'Wipro', 'HCLT IN Equity': 'HCL Tech',
        'INFO IN Equity': 'Infosys', 'CTSH US Equity': 'Cognizant',
    }
    INDIA_SERVICES_EXPORT_TICKER = 'INITSEXP Index'
    NIFTY_TICKER = 'NIFTY Index'
    INDIA_IT_START_DATE = datetime(2010, 1, 1)
    INDIA_IT_OUTFILE = Path(G_CHART_DIR) / "india_it_services.png"

    try:
        # Revenue Growth (RR033)
        revenue_growth_raw = blp.bdh(tickers=INDIA_IT_TICKERS, flds=['RR033'], start_date=INDIA_IT_START_DATE, end_date=END_DATE)
        if isinstance(revenue_growth_raw.columns, pd.MultiIndex):
            revenue_growth_raw.columns = [col[0] for col in revenue_growth_raw.columns]
        revenue_growth_raw.index = pd.to_datetime(revenue_growth_raw.index)
        revenue_growth_avg = revenue_growth_raw.mean(axis=1).dropna()
        revenue_growth_avg.name = 'Avg Revenue Growth YoY'

        # Services Exports
        services_export_raw = blp.bdh(tickers=[INDIA_SERVICES_EXPORT_TICKER], flds=['PX_LAST'], start_date=INDIA_IT_START_DATE, end_date=END_DATE)
        if isinstance(services_export_raw.columns, pd.MultiIndex):
            services_export_raw.columns = [col[0] for col in services_export_raw.columns]
        services_export_raw.index = pd.to_datetime(services_export_raw.index)
        services_export = services_export_raw.iloc[:, 0].dropna()
        services_export_12m = services_export.rolling(window=12, min_periods=12).sum()
        services_export_yoy = services_export_12m.pct_change(periods=12) * 100
        services_export_yoy = services_export_yoy.dropna()

        # Share Prices
        prices_raw = blp.bdh(tickers=INDIA_IT_TICKERS + [NIFTY_TICKER], flds=['PX_LAST'], start_date=INDIA_IT_START_DATE, end_date=END_DATE)
        if isinstance(prices_raw.columns, pd.MultiIndex):
            prices_raw.columns = [col[0] for col in prices_raw.columns]
        prices_raw.index = pd.to_datetime(prices_raw.index)
        prices_companies = prices_raw[INDIA_IT_TICKERS].dropna()
        it_index = pd.Series(dtype=float); nifty_indexed = pd.Series(dtype=float); relative_perf = pd.Series(dtype=float)
        if not prices_companies.empty:
            common_start = prices_companies.index[0]
            prices_companies = prices_companies[prices_companies.index >= common_start]
            prices_indexed = (prices_companies / prices_companies.iloc[0]) * 100
            it_index = prices_indexed.mean(axis=1); it_index.name = 'IT Services Index'
            nifty_prices = prices_raw[NIFTY_TICKER].dropna()
            nifty_prices = nifty_prices[nifty_prices.index >= common_start]
            if not nifty_prices.empty:
                nifty_indexed = (nifty_prices / nifty_prices.iloc[0]) * 100; nifty_indexed.name = 'NIFTY Index'
                common_idx = it_index.index.intersection(nifty_indexed.index)
                relative_perf = (it_index.loc[common_idx] / nifty_indexed.loc[common_idx]) * 100

        # Employee Count (RR121)
        employees_raw = blp.bdh(tickers=INDIA_IT_TICKERS, flds=['RR121'], start_date=INDIA_IT_START_DATE, end_date=END_DATE)
        if isinstance(employees_raw.columns, pd.MultiIndex):
            employees_raw.columns = [col[0] for col in employees_raw.columns]
        employees_raw.index = pd.to_datetime(employees_raw.index)
        five_years_ago = pd.Timestamp.today().normalize() - pd.DateOffset(years=5)
        employees_5y = employees_raw[employees_raw.index >= five_years_ago]
        if not employees_5y.empty and len(employees_5y.columns) > 1:
            missing_counts = employees_5y.isna().sum()
            worst_company = missing_counts.idxmax()
            employees_filtered = employees_raw.drop(columns=[worst_company])
        else:
            employees_filtered = employees_raw
        employees_complete = employees_filtered.dropna(how='any')
        employees_total = employees_complete.sum(axis=1); employees_total.name = 'Total Employees'
        employees_12m_change = pd.Series(dtype=float)
        if not employees_total.empty:
            employees_total_filled = employees_total.asfreq('D').ffill()
            employees_12m_change = employees_total_filled.diff(periods=365).dropna()

        # Margins (RR057, RR243)
        gross_margin_raw = blp.bdh(tickers=INDIA_IT_TICKERS, flds=['RR057'], start_date=INDIA_IT_START_DATE, end_date=END_DATE)
        if isinstance(gross_margin_raw.columns, pd.MultiIndex):
            gross_margin_raw.columns = [col[0] for col in gross_margin_raw.columns]
        gross_margin_raw.index = pd.to_datetime(gross_margin_raw.index)
        gross_margin_avg = gross_margin_raw.mean(axis=1).dropna()

        profit_margin_raw = blp.bdh(tickers=INDIA_IT_TICKERS, flds=['RR243'], start_date=INDIA_IT_START_DATE, end_date=END_DATE)
        if isinstance(profit_margin_raw.columns, pd.MultiIndex):
            profit_margin_raw.columns = [col[0] for col in profit_margin_raw.columns]
        profit_margin_raw.index = pd.to_datetime(profit_margin_raw.index)
        profit_margin_avg = profit_margin_raw.mean(axis=1).dropna()

        # Create chart with 4 subplots
        fig, axes = plt.subplots(4, 1, figsize=(14, 20))
        ax1, ax2, ax3, ax4 = axes

        # Subplot 1: Revenue Growth vs Services Exports
        if not revenue_growth_avg.empty:
            ax1.plot(revenue_growth_avg.index, revenue_growth_avg.values, color='tab:blue', linewidth=2, label='IT Cos Avg Revenue Growth YoY%', marker='o', markersize=3)
        if not services_export_yoy.empty:
            ax1.plot(services_export_yoy.index, services_export_yoy.values, color='tab:orange', linewidth=2, label='India Services Exports YoY%', linestyle='--')
        ax1.set_title("IT Companies Revenue Growth vs India Services Exports Growth (YoY%)", fontsize=12, fontweight='bold')
        ax1.set_ylabel("YoY Growth %"); ax1.axhline(0, color='gray', linewidth=0.8, linestyle='--')
        ax1.grid(True, alpha=0.3); ax1.legend(loc='upper left', fontsize=9)
        ax1.xaxis.set_major_locator(mdates.YearLocator()); ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')

        # Subplot 2: Share Price Index vs NIFTY
        if not it_index.empty:
            ax2.plot(it_index.index, it_index.values, color='tab:blue', linewidth=2, label='IT Services Index')
        if not nifty_indexed.empty:
            ax2.plot(nifty_indexed.index, nifty_indexed.values, color='tab:green', linewidth=2, label='NIFTY Index')
        ax2_twin = ax2.twinx()
        if not relative_perf.empty:
            ax2_twin.plot(relative_perf.index, relative_perf.values, color='tab:red', linewidth=1.5, label='IT vs NIFTY (Relative)', linestyle='--', alpha=0.7)
            ax2_twin.axhline(100, color='tab:red', linewidth=0.8, linestyle=':', alpha=0.5)
        ax2.set_title("IT Services Share Price Index vs NIFTY (Base=100)", fontsize=12, fontweight='bold')
        ax2.set_ylabel("Index Level"); ax2_twin.set_ylabel("Relative Performance", color='tab:red')
        ax2.grid(True, alpha=0.3)
        lines1, labels1 = ax2.get_legend_handles_labels(); lines2, labels2 = ax2_twin.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)
        ax2.xaxis.set_major_locator(mdates.YearLocator()); ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')

        # Subplot 3: Employee Count
        if not employees_total.empty:
            ax3.plot(employees_total.index, employees_total.values / 1000, color='tab:blue', linewidth=2, label='Total Employees (thousands)', marker='o', markersize=3)
        ax3_twin = ax3.twinx()
        if not employees_12m_change.empty:
            emp_sampled = employees_12m_change.resample('Q').last().dropna()
            colors_emp = ['tab:green' if x >= 0 else 'tab:red' for x in emp_sampled.values]
            ax3_twin.bar(emp_sampled.index, emp_sampled.values / 1000, width=60, color=colors_emp, alpha=0.5, label='12M Change (thousands)')
        ax3.set_title("Total Employees (5 IT Companies)", fontsize=12, fontweight='bold')
        ax3.set_ylabel("Total Employees (thousands)", color='tab:blue')
        ax3.grid(True, alpha=0.3)
        lines1, labels1 = ax3.get_legend_handles_labels(); lines2, labels2 = ax3_twin.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)
        ax3.xaxis.set_major_locator(mdates.YearLocator()); ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')

        # Subplot 4: Margins
        ax4_twin = ax4.twinx()
        if not gross_margin_avg.empty:
            ax4.plot(gross_margin_avg.index, gross_margin_avg.values, color='tab:blue', linewidth=2, label='Avg Gross Margin %', marker='o', markersize=3)
        if not profit_margin_avg.empty:
            ax4_twin.plot(profit_margin_avg.index, profit_margin_avg.values, color='tab:green', linewidth=2, label='Avg Profit Margin %', marker='s', markersize=3)
        ax4.set_title("Average Gross Margin & Profit Margin", fontsize=12, fontweight='bold')
        ax4.set_ylabel("Gross Margin %", color='tab:blue'); ax4_twin.set_ylabel("Profit Margin %", color='tab:green')
        ax4.grid(True, alpha=0.3)
        lines1, labels1 = ax4.get_legend_handles_labels(); lines2, labels2 = ax4_twin.get_legend_handles_labels()
        ax4.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)
        ax4.xaxis.set_major_locator(mdates.YearLocator()); ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax4.get_xticklabels(), rotation=45, ha='right')

        fig.suptitle("India IT Services Companies Analysis\n(TCS, Wipro, HCL Tech, Infosys, Cognizant)", fontsize=14, fontweight='bold', y=0.995)
        all_dates = []
        for s in [revenue_growth_avg, services_export_yoy, it_index, employees_total, gross_margin_avg, profit_margin_avg]:
            if not s.empty: all_dates.append(s.index[-1])
        if all_dates:
            last_data_date = max(all_dates)
            fig.text(0.98, 0.99, f"Last data: {last_data_date.strftime('%d %b %Y')}", transform=fig.transFigure,
                     fontsize=10, ha='right', va='top',
                     bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))
        plt.tight_layout(rect=[0, 0, 1, 0.97])
        plt.savefig(INDIA_IT_OUTFILE, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved India IT Services chart to: {INDIA_IT_OUTFILE}")
    except Exception as e:
        print(f"Error generating India IT Services chart: {e}")
        import traceback; traceback.print_exc()


# UPDATER5_CHARTS = [
#     ("DM_5Y_vs_SOFR_rolling_beta", chart_dm_asia_rates_beta),
#     ("US_Inventories_Tracker", chart_us_inventories_tracker),
#     ("DXY_Rolling_Attribution", chart_dxy_rolling_attribution),
#     ("Gold_ETF_FundFlows", chart_gold_etf_fund_flows),
#     ("VND_vs_CR_Band", chart_vnd_vs_cr_band),
#     ("IDR_DNDF_Expiry_Schedule", chart_idr_dndf_expiry_schedule),
#     ("INBGBKLQ_Seasonality", chart_india_banking_seasonality),
#     ("INR_Basis_vs_Reserves_and_Liq", chart_inr_basis_vs_reserves_and_liq),
#     ("UniversalClaimants", chart_universal_claimants),
#     ("Indonesia_MoneySupply_YoY", chart_indonesia_money_supply_yoy),
#     ("USDVND_T123_FTSE", chart_usdvnd_t123_ftse),
#     ("MAS_DLI_Charts", chart_mas_dli),
#     ("Korea_MMF_AUM", chart_korea_mmf_aum),
#     ("India_IT_Services", chart_india_it_services),
# ]


def chart_asia_fx_vs_oil_intraday_beta():
    """Asia FX vs Oil Intraday Beta -- rolling 360min Correlation (10min blocks)"""
    ticker_map = OrderedDict([
        ('KRW NDF', 'KWN+1M Curncy'),
        ('INR NDF', 'IRN+1M Curncy'),
        ('TWD NDF', 'NTN+1M Curncy'),
        ('IDR NDF', 'IHN+1M Curncy'),
        ('PHP NDF', 'PPN+1M Curncy'),
        ('MYR NDF', 'MRN+1M Curncy'),
        ('THB',     'THB BGN Curncy'),
        ('CNH',     'USDCNH Curncy'),
        ('SGD',     'USDSGD Curncy'),
        ('SGNEER',  'CTSGDBP Index'),
    ])
    _oil_beta_compute_chart(
        ticker_map=ticker_map,
        oil_ticker='CO1 Comdty',
        ref_ticker='ES1 Index',
        chart_title='Asia FX vs Oil — Intraday Rolling 360min Correlation (10min blocks)',
        save_name='Asia_FX_vs_Oil_Intraday_Beta.png',
    )


def chart_g10_fx_vs_oil_intraday_beta():
    """G10 FX vs Oil Intraday Beta -- rolling 360min Correlation (10min blocks)"""
    ticker_map = OrderedDict([
        ('EURUSD', 'EURUSD Curncy'),
        ('USDJPY', 'USDJPY Curncy'),
        ('GBPUSD', 'GBPUSD Curncy'),
        ('AUDUSD', 'AUDUSD Curncy'),
        ('NZDUSD', 'NZDUSD Curncy'),
        ('USDCAD', 'USDCAD Curncy'),
        ('USDCHF', 'USDCHF Curncy'),
        ('USDNOK', 'USDNOK Curncy'),
        ('USDSEK', 'USDSEK Curncy'),
    ])
    _oil_beta_compute_chart(
        ticker_map=ticker_map,
        oil_ticker='CO1 Comdty',
        ref_ticker='ES1 Index',
        chart_title='G10 FX vs Oil — Intraday Rolling 360min Correlation (10min blocks)',
        save_name='G10_FX_vs_Oil_Intraday_Beta.png',
    )


def chart_equities_vs_oil_intraday_beta():
    """Equities vs Oil Intraday Beta -- rolling 360min Correlation (10min blocks)"""
    ticker_map = OrderedDict([
        ('S&P 500',    'ES1 Index'),
        ('Nasdaq',     'NQ1 Index'),
        ('Nikkei',     'NK1 Index'),
        ('KOSPI 200',  'KM1 Index'),
        ('HSCEI',      'HC1 Index'),
        ('NIFTY',  'JGS1 Index'),
        ('Euro Stoxx', 'VG1 Index'),
        ('TAIEX',      'FT1 Index'),
    ])
    _oil_beta_compute_chart(
        ticker_map=ticker_map,
        oil_ticker='CO1 Comdty',
        ref_ticker='ES1 Index',
        chart_title='Equities vs Oil — Intraday Rolling 360min Correlation (10min blocks)',
        save_name='Equities_vs_Oil_Intraday_Beta.png',
    )



# --- New charts (updater6) ---


def chart_india_rates_vs_equity_valuations():
    """India Rates Relative to Equity Valuations vs Market Performance.

    Series 1 (left):  Equity-Bond multiple ratio = NTM PE / (100 / 10Y yield)
    Series 2 (right): 2-year forward CAGR in MSCI India (inverted, lagged 2yr)
    """
    END_DATE = datetime.today()
    START_DATE = datetime(1998, 1, 1)

    # --- pull data ---
    pe_data = blp.bdh("MXIN Index", "BEST_PE_RATIO", START_DATE, END_DATE)
    if isinstance(pe_data.columns, pd.MultiIndex):
        pe_data.columns = pe_data.columns.get_level_values(0)
    pe_data.index = pd.to_datetime(pe_data.index)
    pe_data.columns = ["PE"]

    yld_data = blp.bdh("GIND10YR Index", "PX_LAST", START_DATE, END_DATE)
    if isinstance(yld_data.columns, pd.MultiIndex):
        yld_data.columns = yld_data.columns.get_level_values(0)
    yld_data.index = pd.to_datetime(yld_data.index)
    yld_data.columns = ["Yield"]

    px_data = blp.bdh("MXIN Index", "PX_LAST", START_DATE, END_DATE)
    if isinstance(px_data.columns, pd.MultiIndex):
        px_data.columns = px_data.columns.get_level_values(0)
    px_data.index = pd.to_datetime(px_data.index)
    px_data.columns = ["Px"]

    # resample to monthly to smooth out noise
    pe_m = pe_data["PE"].resample("M").last().ffill()
    yld_m = yld_data["Yield"].resample("M").last().ffill()
    px_m = px_data["Px"].resample("M").last().ffill()

    # --- Series 1: Equity/Bond multiple ratio ---
    # Bond PE = 100 / yield; ratio = NTM PE / Bond PE = NTM PE * yield / 100
    equity_bond_ratio = pe_m * yld_m / 100.0

    # --- Series 2: 2-year forward CAGR (lagged 2yr) ---
    # For each date t, fwd_cagr(t) = (Px[t+24m] / Px[t])^0.5 - 1
    fwd_cagr = pd.Series(index=px_m.index, dtype=float)
    for i, dt in enumerate(px_m.index):
        future_dt = dt + relativedelta(months=24)
        # find closest available future date
        future_vals = px_m.loc[px_m.index >= future_dt]
        if future_vals.empty:
            continue
        future_px = future_vals.iloc[0]
        current_px = px_m.iloc[i]
        if current_px > 0 and not np.isnan(future_px):
            fwd_cagr.iloc[i] = ((future_px / current_px) ** 0.5 - 1) * 100

    # --- plot ---
    fig, ax = plt.subplots(figsize=(14, 7))
    valid_ratio = equity_bond_ratio.dropna()
    line1, = ax.plot(valid_ratio.index, valid_ratio.values, color='#1f77b4',
                     linewidth=1.8, label='Equity/Bond Multiple (NTM PE × 10Y Yield / 100)')
    ax.set_ylabel('Equity / Bond Multiple Ratio', color='#1f77b4', fontsize=12)
    ax.tick_params(axis='y', labelcolor='#1f77b4')
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    ax2 = ax.twinx()
    valid_cagr = fwd_cagr.dropna()
    line2, = ax2.plot(valid_cagr.index, valid_cagr.values, color='#ff7f0e',
                      linewidth=1.8, alpha=0.85,
                      label='2Y Fwd CAGR in MSCI India (%, inv.)')
    ax2.set_ylabel('2Y Forward CAGR (%, inverted)', color='#ff7f0e', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#ff7f0e')
    ax2.invert_yaxis()

    last_dt = valid_ratio.index[-1].strftime('%b %Y')
    ax.set_title(f'India: Equity/Bond Multiple vs 2Y Forward MSCI India Returns  (as of {last_dt})',
                 fontsize=13)
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=9)
    ax.grid(True, ls=':', alpha=0.4)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "India_Rates_vs_Equity_Valuations.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)


def chart_nasdaq_divergence():
    """NASDAQ Divergence: NDX index (log) vs average 52-wk drawdown of NDX stocks (inverted)."""
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)

    # --- 1) Get current NDX members ---
    members_df = blp.bds("NDX Index", "INDX_MWEIGHT")
    if 'member_ticker_and_exchange_code' in members_df.columns:
        member_tickers = members_df['member_ticker_and_exchange_code'].tolist()
    elif members_df.shape[1] >= 1:
        member_tickers = members_df.iloc[:, 0].tolist()
    else:
        raise RuntimeError("Could not parse NDX Index members from Bloomberg")

    # ensure each ticker ends with " Equity"
    member_tickers = [t if t.strip().endswith('Equity') else t.strip() + ' Equity'
                      for t in member_tickers]

    # --- 2) Pull NDX index level ---
    ndx = blp.bdh("NDX Index", "PX_LAST", START_DATE, END_DATE)
    if isinstance(ndx.columns, pd.MultiIndex):
        ndx.columns = ndx.columns.get_level_values(0)
    ndx.index = pd.to_datetime(ndx.index)
    ndx = ndx.iloc[:, 0]

    # --- 3) Pull all member prices ---
    member_px = blp.bdh(member_tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(member_px.columns, pd.MultiIndex):
        member_px.columns = member_px.columns.get_level_values(0)
    member_px.index = pd.to_datetime(member_px.index)

    # --- 4) Compute average 52-week drawdown ---
    rolling_max = member_px.rolling(window=252, min_periods=126).max()
    drawdown = (member_px / rolling_max - 1) * 100  # in %
    avg_drawdown = drawdown.mean(axis=1)  # average across stocks

    # --- 5) Plot ---
    fig, ax = plt.subplots(figsize=(14, 7))
    line1, = ax.plot(ndx.index, ndx.values, color='#1f77b4', linewidth=1.5,
                     label='NASDAQ 100 Index (log scale)')
    ax.set_yscale('log')
    ax.set_ylabel('NASDAQ 100 (log scale)', color='#1f77b4', fontsize=12)
    ax.tick_params(axis='y', labelcolor='#1f77b4')
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    ax2 = ax.twinx()
    valid_dd = avg_drawdown.dropna()
    line2, = ax2.plot(valid_dd.index, valid_dd.values, color='red', linewidth=1.2,
                      alpha=0.8, label='Avg 52-Wk Drawdown of NDX Stocks (%)')
    ax2.set_ylabel('Avg 52-Wk Drawdown (%, inverted)', color='red', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='red')
    ax2.invert_yaxis()  # 0 at top, most negative at bottom

    last_dt = ndx.dropna().index[-1].strftime('%b %Y')
    ax.set_title(f'NASDAQ 100 Divergence: Index Level vs Avg Constituent Drawdown  (as of {last_dt})',
                 fontsize=13)
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=9)
    ax.grid(True, ls=':', alpha=0.4)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "NASDAQ_Divergence.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)


def chart_china_domestic_credit_impulse():
    """China Domestic Credit Impulse.

    Credit impulse  = (TSF - equity issuance) 12m rolling sum, then 12m change
    Govt spending   = GS China Fiscal Spending Index (12m change)
    Both expressed as % of nominal GDP.
    """
    END_DATE = datetime.today()
    START_DATE = datetime(2003, 1, 1)

    # --- Pull TSF aggregate and equity issuance (monthly flows, CNY bn) ---
    tsf_tickers = ["CNLNASF Index", "CNLNDNFS Index"]
    tsf_raw = blp.bdh(tsf_tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(tsf_raw.columns, pd.MultiIndex):
        tsf_raw.columns = tsf_raw.columns.get_level_values(0)
    tsf_raw.index = pd.to_datetime(tsf_raw.index)

    # --- Government fiscal spending index ---
    fiscal_raw = blp.bdh("GSXACFIS Index", "PX_LAST", START_DATE, END_DATE)
    if isinstance(fiscal_raw.columns, pd.MultiIndex):
        fiscal_raw.columns = fiscal_raw.columns.get_level_values(0)
    fiscal_raw.index = pd.to_datetime(fiscal_raw.index)
    fiscal = fiscal_raw.iloc[:, 0]

    # --- China quarterly nominal GDP (CNY bn) ---
    gdp_raw = blp.bdh("CNNGPQ$ Index", "PX_LAST", START_DATE, END_DATE, Per='Q')
    if isinstance(gdp_raw.columns, pd.MultiIndex):
        gdp_raw.columns = gdp_raw.columns.get_level_values(0)
    gdp_raw.index = pd.to_datetime(gdp_raw.index)
    gdp_q = gdp_raw.iloc[:, 0]
    # trailing 4-quarter GDP (annualised), resample to monthly
    gdp_annual = gdp_q.rolling(4).sum()
    gdp_m = gdp_annual.resample('M').last().ffill()

    # --- Credit impulse ---
    tsf_flow = tsf_raw["CNLNASF Index"].fillna(0)
    eq_flow = tsf_raw["CNLNDNFS Index"].fillna(0)
    adj_tsf_flow = tsf_flow - eq_flow  # monthly flow ex equity issuance

    # 12-month rolling sum → annual credit flow
    annual_credit_flow = adj_tsf_flow.rolling(12).sum()
    # 12-month change of annual flow → credit impulse (CNY bn)
    credit_impulse = annual_credit_flow - annual_credit_flow.shift(12)

    # --- Government spending impulse ---
    # GSXACFIS is a level index — compute 12m change
    fiscal_impulse = fiscal - fiscal.shift(12)

    # --- Align dates and express as % of GDP ---
    combined = pd.DataFrame({
        'credit_impulse': credit_impulse,
        'fiscal_impulse': fiscal_impulse,
        'gdp': gdp_m,
    }).dropna()

    combined['credit_pct_gdp'] = combined['credit_impulse'] / combined['gdp'] * 100
    combined['fiscal_pct_gdp'] = combined['fiscal_impulse'] / combined['gdp'] * 100
    combined['total'] = combined['credit_pct_gdp'] + combined['fiscal_pct_gdp']

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar(combined.index, combined['credit_pct_gdp'], width=25, color='steelblue',
           alpha=0.7, label='Credit Impulse (TSF ex Equity, % GDP)')
    ax.bar(combined.index, combined['fiscal_pct_gdp'], width=25, bottom=combined['credit_pct_gdp'],
           color='darkorange', alpha=0.7, label='Govt Spending Impulse (% GDP)')
    ax.plot(combined.index, combined['total'], color='black', linewidth=1.5,
            label='Total Impulse (% GDP)')
    ax.axhline(0, color='grey', linewidth=0.8)

    ax.set_ylabel('% of GDP', fontsize=12)
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    last_dt = combined.index[-1].strftime('%b %Y')
    ax.set_title(f'China Domestic Credit & Fiscal Impulse (% GDP)  (as of {last_dt})',
                 fontsize=13)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, ls=':', alpha=0.4)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "China_Domestic_Credit_Impulse.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)


def chart_country_ip_vs_em_stock_prices():
    """Country Industrial Production YoY (advanced 6m) vs local-ccy equity index.

    9 countries: China, India, Taiwan, Korea, Japan, Brazil, Indonesia, Thailand, Vietnam.
    """
    END_DATE = datetime.today()
    START_DATE = datetime(2000, 1, 1)

    countries = OrderedDict([
        ('China',     {'ip': 'CHVAIOY Index',  'eq': 'SHSZ300 Index'}),
        ('India',     {'ip': 'INPIINDY Index', 'eq': 'NIFTY Index'}),
        ('Taiwan',    {'ip': 'TWINDPIY Index', 'eq': 'TWSE Index'}),
        ('Korea',     {'ip': 'KOIPIYOY Index', 'eq': 'KOSPI Index'}),
        ('Japan',     {'ip': 'JNIPSYOY Index', 'eq': 'TPX Index'}),
        ('Brazil',    {'ip': 'BZIPYOY% Index', 'eq': 'IBOV Index'}),
        ('Indonesia', {'ip': 'IDMPIYOY Index', 'eq': 'JCI Index'}),
        ('Thailand',  {'ip': 'THMPIN2Y Index', 'eq': 'SET Index'}),
        ('Vietnam',   {'ip': 'VIPITYOY Index', 'eq': 'VNINDEX Index'}),
    ])

    # pull all tickers in one go
    all_ip_tickers = [v['ip'] for v in countries.values()]
    all_eq_tickers = [v['eq'] for v in countries.values()]

    ip_data = blp.bdh(all_ip_tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(ip_data.columns, pd.MultiIndex):
        ip_data.columns = ip_data.columns.get_level_values(0)
    ip_data.index = pd.to_datetime(ip_data.index)

    eq_data = blp.bdh(all_eq_tickers, "PX_LAST", START_DATE, END_DATE)
    if isinstance(eq_data.columns, pd.MultiIndex):
        eq_data.columns = eq_data.columns.get_level_values(0)
    eq_data.index = pd.to_datetime(eq_data.index)

    # resample IP to monthly (it's already YoY %)
    ip_m = ip_data.resample('M').last().ffill()
    eq_m = eq_data.resample('M').last().ffill()

    # --- Plot 3x3 grid ---
    fig, axs = plt.subplots(3, 3, figsize=(20, 16))
    axs = axs.flatten()

    for i, (cname, tickers) in enumerate(countries.items()):
        ax = axs[i]
        ip_ticker = tickers['ip']
        eq_ticker = tickers['eq']

        ip_series = ip_m[ip_ticker].dropna() if ip_ticker in ip_m.columns else pd.Series(dtype=float)
        eq_series = eq_m[eq_ticker].dropna() if eq_ticker in eq_m.columns else pd.Series(dtype=float)

        # advance IP by 6 months
        if not ip_series.empty:
            ip_advanced = ip_series.copy()
            ip_advanced.index = ip_advanced.index + pd.DateOffset(months=6)
            line1, = ax.plot(ip_advanced.index, ip_advanced.values, color='steelblue',
                             linewidth=1.5, label=f'IP YoY % (adv 6m)')
            ax.set_ylabel('IP YoY %', color='steelblue', fontsize=9)
            ax.tick_params(axis='y', labelcolor='steelblue', labelsize=8)

        ax2 = ax.twinx()
        if not eq_series.empty:
            line2, = ax2.plot(eq_series.index, eq_series.values, color='#ff7f0e',
                              linewidth=1.3, alpha=0.85, label=f'{eq_ticker.split()[0]}')
            ax2.set_ylabel('Equity Index', color='#ff7f0e', fontsize=9)
            ax2.tick_params(axis='y', labelcolor='#ff7f0e', labelsize=8)

        ax.set_title(cname, fontsize=11, fontweight='bold')
        ax.xaxis.set_major_locator(mdates.YearLocator(4))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.tick_params(axis='x', rotation=45, labelsize=8)
        ax.grid(True, ls=':', alpha=0.3)

        # combined legend
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc='upper left', fontsize=7)

    last_dt = END_DATE.strftime('%b %Y')
    fig.suptitle(f'Country IP YoY (Advanced 6m) vs Local-Ccy Equity Index  (as of {last_dt})',
                 fontsize=14, y=1.01)
    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, "Country_IP_vs_EM_Stock_Prices.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)


# ==============================================================================
# SECTION 5: CHART REGISTRY
# ==============================================================================

CHART_REGISTRY = OrderedDict([
    # --- From charts_updater.py (updater1) ---
    ("20d Change of GSUSFCI",                          chart_20d_change_of_gsusfci),
    ("HSI vs Hibor3y",                                  chart_hsi_vs_hibor3y),
    ("EURUSD vs UST",                                   chart_eurusd_vs_ust),
    ("LMCI",                                            chart_lmci),
    ("PMI",                                             chart_pmi),
    ("LEI",                                             chart_lei),
    ("CapacityUti",                                     chart_capacity_utilization),
    ("USCPIvsWage",                                     chart_us_cpi_vs_wage),
    ("EZWage",                                          chart_ez_wage),
    ("Withheld Tax vs Total NFP",                       chart_withheld_tax_vs_total_nfp),
    ("USD AD Line",                                     chart_usd_ad_line),
    ("EMDM PBratio",                                    chart_emdm_pb_ratio),
    ("EM MSCI PEratio",                                 chart_em_msci_pe_ratio),
    ("CMBS Spread over HYG",                            chart_cmbs_spread_over_hyg),
    ("United States Construction Sector",               chart_us_construction_sector),
    ("Shanghai Future vs LME Copper Spread",            chart_shanghai_future_vs_lme_copper_spread),
    ("LME Metal Index vs CRB Raw Material Index",       chart_lme_metal_index_vs_crb_raw_material_index),
    ("US Treasury Curve Beta to 2y Treasury Yield",     chart_us_treasury_curve_beta_to_2y),
    ("U M Sentiment vs Durable Goods (Current)",        chart_us_household_durable_demand),
    ("China Real Estate Investment",                    chart_china_real_estate_investment),
    ("China Cement, Glass, Auto Sales YoY",             chart_china_cement_glass_auto_sales),
    ("Loans to Non-bank Financial Institutions",        chart_loans_to_nonbank_fi),
    ("Australia Monthly CPI",                           chart_australia_monthly_cpi),
    ("Indonesia Wages Per day",                         chart_indonesia_wages_per_day),
    ("Singapore Domestic Liquidity",                    chart_singapore_domestic_liquidity),
    ("Breadth of USD Strength",                         chart_breadth_of_usd_strength),
    ("Equity BearBull Breadth",                         chart_equity_bearbull_breadth),
    ("Commod Px vs Commod Country Credit Impulse",      chart_commod_px_vs_credit_impulse),
    ("Domestic vs External bond Yield",                 chart_domestic_vs_external_bond_yield),

    # --- From charts_updater2.py (updater2) ---
    ("Mani PMI and Core PCE",                           chart_manu_pmi_and_core_pce),
    ("Labor Quality and Wages",                         chart_labor_quality_and_wages),
    ("Australia Total Compensation, WPi and Hours Worked", chart_australia_wage),
    ("Australia Unemployment Expectations",             chart_australia_unemployment_expectations),
    ("US ISM PMI",                                      chart_us_ism_pmi),
    ("India Exp YOY excl Interest",                     chart_india_exp_yoy_excl_interest),
    ("5Y Implied By Beta",                              chart_yield_and_ccy_betas),
    ("Euro Area PMI vs Sentix",                         chart_euro_area_pmi_vs_sentix),
    ("Australia Westpac Leading Index vs GDP",          chart_australia_westpac_leading_index),
    ("10y ACM vs 5s10s30s UST Correlation vs ISM Manufacturing", chart_acm_term_premium_vs_ism),
    ("Singapore Tradable Core Inflation",               chart_sg_tradable_core_inflation),
    ("US Share of Total Unemployment",                  chart_us_share_of_total_unemployment),
    ("India Budget Balance vs Credit",                  chart_india_budget_balance_vs_credit),
    ("USDJPY vs Japan Exporter Breakeven Rate",         chart_usdjpy_exporter_breakeven),
    ("Fed Wage Growth vs Unemployment Gap",             chart_fed_wage_growth_vs_unemployment_gap),
    ("NZ Filled Jobs Seasonally Adjusted",              chart_nz_filled_jobs),
    ("10Y Generic Swap Spread",                         chart_generic_10y_swap_spread),
    ("2y ACM vs Swap Fly",                              chart_2y_acm_vs_swap_fly),
    ("10y ACM vs 5s10s30s UST Correlation vs ACM Correlation", chart_acm_correlation),
    ("Orders Inventory Change vs IP",                   chart_orders_inventory_change_vs_ip),
    ("Employment Cost Index vs NFIB Labour Quality",    chart_eci_vs_nfib_labour_quality),
    ("USTs vs Uncertainty Index",                       chart_usts_vs_uncertainty_index),

    # --- From charts_updater3.py (updater3) ---
    ("Marginal Propensity To Save",                     chart_marginal_propensity_to_save),
    ("NFP vs CEO Confidence and Operating Profits",     chart_nfp_vs_ceo_confidence_and_operating_profits),
    ("Inventory To Shipment Ratios",                    chart_inventory_to_shipment_ratios),
    ("Service Activity",                                chart_service_activity),
    ("US Money Velocity vs Core CPI",                   chart_us_money_velocity_vs_core_cpi),
    ("China Railway Freight Traffic Turnover",          chart_china_railway_freight_traffic_turnover),
    ("EM Composite EPS vs Index Px",                    chart_em_composite_eps_vs_index_px),
    ("Prime Age Employment Rate",                       chart_prime_age_employment_rate),
    ("AHEYoY vs U6",                                    chart_ahe_yoy_vs_u6),
    ("FD_GDP_and_Credit_Growth",                        chart_fd_gdp_and_credit_growth),
    ("EPSGrowth vs GDP-WagesXOperMargin",               chart_eps_growth_vs_gdp_wages_x_oper_margin),
    ("SPX Risk Premium",                                chart_spx_risk_premium),
    ("MSCIWorld Yields Corr",                           chart_msciworld_yields_corr),
    ("CommBankSecHldgs vs DXY",                         chart_comm_bank_sec_hldgs_vs_dxy),
    ("EMDM Composite CurrLiab-FwdSales Ratio",         chart_emdm_composite_currliab_fwdsales_ratio),
    ("CN Banks Claims on Non-Fin Sector",               chart_cn_banks_claims_non_fin),
    ("USEUJP Real Rates Diff vs FX",                    chart_useujp_real_rates_diff_vs_fx),
    ("China Grid Investment Copper",                    chart_china_grid_investment_copper),
    ("GSCycVsDef BarCapUSHYsprd JPEMBIsprd",            chart_gs_cyc_vs_def_hy_embi_regression),
    ("MSCI India EM Relative vs CopperBrentRatio",      chart_msci_india_em_relative_vs_copper_brent),
    ("SensexYoY vs GDPminus10Y",                        chart_sensex_yoy_vs_gdp_minus_10y),

    # --- From charts_updater4.py (updater4) ---
    ("ETF Flow Divergence",                             chart_etf_flow_divergence),
    ("OER vs CaseShiller 20-City Adv 18m",              chart_oer_vs_caseshiller_adv18m),
    ("OPEC vs Non-OPEC Oil Production YoY",             chart_opec_vs_nonopec_production_yoy),
    ("BIS Non-Financial Debt Service Ratios",           chart_bis_debt_service_ratios),
    ("China Commodity Prices & Composite Indicator",    chart_china_commodities_prices_trend),
    ("REER Deviations from LT Average",                 chart_reer_lt_deviation),
    ("Indonesia Bank Loans as % of GDP",                chart_idr_total_loans_to_gdp_yoy),
    ("Rolling 6M Change in Fwd PE, Price, and Fwd EPS", chart_equities_pe_eps_vs_price),
    ("Indonesia BI Outstanding OMO Monitor",            chart_indonesia_bi_liquidity),
    ("Westpac Asia Positive Surprise vs Asia Equities", chart_westpac_asia_surprise_vs_equities),
    ("Philippines BSP Liquidity",                       chart_bsp_liquidity),
    ("India CCIL Net Activity & Volumes",               chart_india_bonds_activity_and_volumes),
    ("Gold vs THB High Frequency Correlation",          chart_gold_vs_thb_hf_correlation),
    ("HSCEIvCGB30Y HF Correlation",                     chart_hscei_vs_cgb30y_hf_correlation),
    ("HSCEIvCGB30Y HF Correlation (2)",                 chart_hscei_vs_cgb30y_hf_correlation_2),
    ("USDCNH vs AUDUSD HF Correlation",                 chart_usdcnh_vs_audusd_hf_correlation),
    ("XAUUSD vs ES KM AUD Intraday Correlation",        chart_xauusd_vs_es_km_aud_intraday_correlation),

    # --- From charts_updater5.py (updater5) ---
    ("DM/Asia Rates Beta",                              chart_dm_asia_rates_beta),
    ("US Inventories Tracker",                          chart_us_inventories_tracker),
    ("DXY Rolling Attribution",                         chart_dxy_rolling_attribution),
    ("Gold ETF Fund Flows",                             chart_gold_etf_fund_flows),
    ("VND Spot vs SBV Interbank Rate Band",             chart_vnd_vs_cr_band),
    ("Indonesia IDR DNDF Expiry Schedule",              chart_idr_dndf_expiry_schedule),
    ("India Banking Seasonality",                       chart_india_banking_seasonality),
    ("INR Basis vs Reserves and Banking Liquidity",     chart_inr_basis_vs_reserves_and_liq),
    ("DWP Universal Claimants vs UK Inactive Long-Term Sick", chart_universal_claimants),
    ("Indonesia Money Supply YoY",                      chart_indonesia_money_supply_yoy),
    ("USDVND T123 vs FTSE Fix",                         chart_usdvnd_t123_ftse),
    ("MAS DLI Charts",                                  chart_mas_dli),
    ("Korea MMF Total AUM",                             chart_korea_mmf_aum),
    ("India IT Services Companies",                     chart_india_it_services),

    # --- Oil Beta intraday charts ---
    ("Asia FX vs Oil Intraday Beta",                    chart_asia_fx_vs_oil_intraday_beta),
    ("G10 FX vs Oil Intraday Beta",                     chart_g10_fx_vs_oil_intraday_beta),
    ("Equities vs Oil Intraday Beta",                   chart_equities_vs_oil_intraday_beta),

    # --- New charts (updater6) ---
    ("India Rates vs Equity Valuations",               chart_india_rates_vs_equity_valuations),
    ("NASDAQ Divergence",                              chart_nasdaq_divergence),
    ("China Domestic Credit Impulse",                  chart_china_domestic_credit_impulse),
    ("Country IP vs EM Stock Prices",                  chart_country_ip_vs_em_stock_prices),
])

CHART_GROUPS = {
    # --- Charts that pull daily / monthly / quarterly Bloomberg data (blp.bdh, bbgui.bdh) ---
    "daily_bbg": [
        "20d Change of GSUSFCI", "HSI vs Hibor3y", "EURUSD vs UST", "LMCI", "PMI",
        "LEI", "CapacityUti", "USCPIvsWage", "EZWage", "Withheld Tax vs Total NFP",
        "USD AD Line", "EMDM PBratio", "EM MSCI PEratio", "CMBS Spread over HYG",
        "United States Construction Sector", "Shanghai Future vs LME Copper Spread",
        "LME Metal Index vs CRB Raw Material Index", "US Treasury Curve Beta to 2y Treasury Yield",
        "U M Sentiment vs Durable Goods (Current)", "China Real Estate Investment",
        "China Cement, Glass, Auto Sales YoY", "Loans to Non-bank Financial Institutions",
        "Australia Monthly CPI", "Indonesia Wages Per day", "Singapore Domestic Liquidity",
        "Breadth of USD Strength", "Equity BearBull Breadth",
        "Domestic vs External bond Yield",
        "Mani PMI and Core PCE", "Labor Quality and Wages",
        "Australia Total Compensation, WPi and Hours Worked",
        "Australia Unemployment Expectations", "US ISM PMI",
        "India Exp YOY excl Interest", "5Y Implied By Beta",
        "Euro Area PMI vs Sentix", "Australia Westpac Leading Index vs GDP",
        "10y ACM vs 5s10s30s UST Correlation vs ISM Manufacturing",
        "Singapore Tradable Core Inflation", "US Share of Total Unemployment",
        "India Budget Balance vs Credit", "USDJPY vs Japan Exporter Breakeven Rate",
        "Fed Wage Growth vs Unemployment Gap", "NZ Filled Jobs Seasonally Adjusted",
        "10Y Generic Swap Spread", "2y ACM vs Swap Fly",
        "10y ACM vs 5s10s30s UST Correlation vs ACM Correlation",
        "Orders Inventory Change vs IP", "Employment Cost Index vs NFIB Labour Quality",
        "USTs vs Uncertainty Index",
        "Marginal Propensity To Save", "NFP vs CEO Confidence and Operating Profits",
        "Inventory To Shipment Ratios", "Service Activity",
        "US Money Velocity vs Core CPI", "China Railway Freight Traffic Turnover",
        "EM Composite EPS vs Index Px", "Prime Age Employment Rate",
        "AHEYoY vs U6", "FD_GDP_and_Credit_Growth",
        "EPSGrowth vs GDP-WagesXOperMargin", "SPX Risk Premium",
        "MSCIWorld Yields Corr", "CommBankSecHldgs vs DXY",
        "EMDM Composite CurrLiab-FwdSales Ratio",
        "CN Banks Claims on Non-Fin Sector",
        "USEUJP Real Rates Diff vs FX", "China Grid Investment Copper",
        "GSCycVsDef BarCapUSHYsprd JPEMBIsprd",
        "MSCI India EM Relative vs CopperBrentRatio",
        "SensexYoY vs GDPminus10Y",
        "ETF Flow Divergence", "OER vs CaseShiller 20-City Adv 18m",
        "OPEC vs Non-OPEC Oil Production YoY", "BIS Non-Financial Debt Service Ratios",
        "China Commodity Prices & Composite Indicator",
        "REER Deviations from LT Average", "Indonesia Bank Loans as % of GDP",
        "Rolling 6M Change in Fwd PE, Price, and Fwd EPS",
        "Indonesia BI Outstanding OMO Monitor",
        "Westpac Asia Positive Surprise vs Asia Equities",
        "Philippines BSP Liquidity", "India CCIL Net Activity & Volumes",
        "DM/Asia Rates Beta", "US Inventories Tracker", "DXY Rolling Attribution",
        "Gold ETF Fund Flows", "VND Spot vs SBV Interbank Rate Band",
        "Indonesia IDR DNDF Expiry Schedule", "India Banking Seasonality",
        "INR Basis vs Reserves and Banking Liquidity",
        "USDVND T123 vs FTSE Fix",
        "India IT Services Companies",
        "India Rates vs Equity Valuations",
        "NASDAQ Divergence",
        "China Domestic Credit Impulse",
        "Country IP vs EM Stock Prices",
    ],
    # --- Charts that use intraday Bloomberg data (blp.bdib) ---
    "intraday_bbg": [
        "Gold vs THB High Frequency Correlation",
        "HSCEIvCGB30Y HF Correlation",
        "HSCEIvCGB30Y HF Correlation (2)",
        "USDCNH vs AUDUSD HF Correlation",
        "XAUUSD vs ES KM AUD Intraday Correlation",
        "Asia FX vs Oil Intraday Beta",
        "G10 FX vs Oil Intraday Beta",
        "Equities vs Oil Intraday Beta",
    ],
    # --- Charts that use web scraping, HTTP APIs, or external data downloads ---
    "web_scraping": [
        "DWP Universal Claimants vs UK Inactive Long-Term Sick",  # UK DWP Stat-Xplore API
        "Korea MMF Total AUM",                                    # Selenium (crawl_mmf_aum)
        "Indonesia Money Supply YoY",                             # download_bi_monetary_data
        "Commod Px vs Commod Country Credit Impulse",             # Exante API
    ],
    # --- Charts that use CSV files or other non-Bloomberg / non-web sources ---
    "other": [
        "MAS DLI Charts",  # loads mas_dli_from_excel.csv + Bloomberg
    ],
}


# ==============================================================================
# SECTION 6: RUNNER
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run chart updater — consolidated from 5 original scripts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python charts_updater_all.py                          # Run ALL charts
  python charts_updater_all.py "LMCI" "PMI"             # Run charts matching names
  python charts_updater_all.py --group daily_bbg         # Run daily Bloomberg charts
  python charts_updater_all.py --group intraday_bbg      # Run intraday Bloomberg charts
  python charts_updater_all.py --group web_scraping      # Run web-scraping charts
  python charts_updater_all.py --list                    # List all chart names
""")
    parser.add_argument('charts', nargs='*', help='Chart names (substring match)')
    parser.add_argument('--group', '-g', choices=list(CHART_GROUPS.keys()),
                        help='Run only charts from a specific group (daily_bbg, intraday_bbg, web_scraping, other)')
    parser.add_argument('--list', '-l', action='store_true',
                        help='List all available chart names and exit')
    args = parser.parse_args()

    if args.list:
        print(f"\nAvailable charts ({len(CHART_REGISTRY)} total):\n")
        for group_name, chart_names in CHART_GROUPS.items():
            print(f"  [{group_name}] ({len(chart_names)} charts)")
            for name in chart_names:
                print(f"    - {name}")
            print()
        return

    # Determine which charts to run
    if RUN_ONLY is not None:
        charts_to_run = [(n, f) for n, f in CHART_REGISTRY.items() if n in RUN_ONLY]
    elif args.group:
        group_names = CHART_GROUPS.get(args.group, [])
        charts_to_run = [(n, f) for n, f in CHART_REGISTRY.items() if n in group_names]
    elif args.charts:
        charts_to_run = []
        for pattern in args.charts:
            pat_lower = pattern.lower()
            for n, f in CHART_REGISTRY.items():
                if pat_lower in n.lower() and (n, f) not in charts_to_run:
                    charts_to_run.append((n, f))
    else:
        charts_to_run = list(CHART_REGISTRY.items())

    if not charts_to_run:
        print("No charts matched. Use --list to see available charts.")
        return

    print(f"\n{'=' * 70}")
    print(f"CHART UPDATER — {len(charts_to_run)} chart(s) to run")
    print(f"{'=' * 70}\n")

    results = []
    total_start = time.time()

    for idx, (name, func) in enumerate(charts_to_run, 1):
        print(f"[{idx}/{len(charts_to_run)}] Running: {name} ...")
        chart_start = time.time()
        try:
            plt.close('all')
            func()
            elapsed = time.time() - chart_start
            results.append((name, True, elapsed, None))
            print(f"  -> OK ({elapsed:.1f}s)")
        except Exception as e:
            elapsed = time.time() - chart_start
            tb = traceback.format_exc()
            results.append((name, False, elapsed, f"{e}\n{tb}"))
            print(f"  -> FAILED ({elapsed:.1f}s): {e}")
        finally:
            plt.close('all')

    total_elapsed = time.time() - total_start
    succeeded = sum(1 for _, ok, _, _ in results if ok)
    failed = sum(1 for _, ok, _, _ in results if not ok)

    print(f"\n{'=' * 70}")
    print(f"CHART UPDATE SUMMARY  ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"Total: {len(results)} charts in {total_elapsed:.0f}s")
    print(f"Succeeded: {succeeded}  |  Failed: {failed}")
    print(f"{'=' * 70}")

    if failed > 0:
        print(f"\nFAILED CHARTS:")
        for name, ok, elapsed, err in results:
            if not ok:
                print(f"  [{elapsed:.1f}s] {name}")
                # Print first line of error
                if err:
                    first_line = err.split('\n')[0]
                    print(f"         {first_line}")

    print(f"\nTIMINGS (all charts):")
    for name, ok, elapsed, _ in results:
        status = "OK" if ok else "XX"
        print(f"  [{status}] {elapsed:6.1f}s  {name}")


if __name__ == '__main__':
    main()