import os
from datetime import datetime, timedelta
from pathlib import Path
from pandas.tseries.offsets import BDay
import matplotlib.dates as mdates
from matplotlib.dates import WeekdayLocator, DateFormatter, MO
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
from matplotlib.gridspec import GridSpec
import pandas as pd
import numpy as np
import re
import random
import time
import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.colors import TwoSlopeNorm
from matplotlib.ticker import FormatStrFormatter
from xbbg import blp
# Lazy import for statsmodels due to scipy compatibility issues in Python 3.13
try:
    import statsmodels.api as sm
    STATSMODELS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: statsmodels import failed ({e}). Some charts may not be generated.")
    print("To fix: pip install --upgrade statsmodels scipy")
    sm = None
    STATSMODELS_AVAILABLE = False
from pandas.tseries.offsets import CustomBusinessDay
from download_bi_monetary_data import get_adjusted_m0_data, compute_yoy_growth

G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = Path(r"O:\Tian\Portal\Charts\ChartDataBase")  # general chart directory
FONTSIZE = 14

### Asia/DM Rates beta up/beta down
TENOR         = '5'         
FIELD         = 'PX_LAST'
MIN_MOVE      = 0.003        # 0.3 bp threshold on SOFR moves
ROLL_WIN      = 22           
TZ            = 'Asia/Singapore'
END_TS        = pd.Timestamp.now(tz=TZ).normalize()
END           = END_TS.date()
START_BAR     = (END_TS - pd.DateOffset(months=3) - pd.Timedelta(days=3)).date()  # bar chart ~3m (unchanged)
START_ROLL    = (END_TS - relativedelta(years=1) - pd.Timedelta(days=5)).date()   # rolling = last 1y

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

def sofr_ticker(tenor: str) -> str:
    return f'USOSFR{tenor} CMPT Curncy'
DM_RATES   = DM_RATES_5Y
ASIA_RATES = ASIA_RATES_5Y

def bdh_flat(tickers, field, start_date, end_date):
    """xbbg.bdh wrapper → wide DataFrame indexed by date, one col per ticker."""
    out = blp.bdh(tickers=list(tickers), flds=[field], start_date=start_date, end_date=end_date)
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [c[0] for c in out.columns]
    out.index = pd.to_datetime(out.index)
    return out.sort_index()

def align_and_diff(sofr: pd.Series, locals_df: pd.DataFrame, shift_sofr_days: int = 0) -> pd.DataFrame:
    """
    Inner-join SOFR and locals, align to business days (with light interpolation),
    then compute daily diffs. If shift_sofr_days=1, SOFR diffs are shifted forward by 1 day
    so that local(t) is compared against SOFR(t-1).
    """
    x = sofr.rename('SOFR').to_frame().join(locals_df, how='inner').dropna()
    x = x.asfreq('B').interpolate(limit_area='inside')

    x['d_SOFR_raw'] = x['SOFR'].diff()

    # Optional lag: compare local today vs SOFR yesterday → SOFR lead by +1 in alignment
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

def extract_source(tkr: str):
    """
    Return the Bloomberg pricing source token in the ticker (CMPT/CMPN/CMPL) or None if not found.
    """
    m = re.search(r'\b(CMPT|CMPN|CMPL)\b', tkr)
    return m.group(1) if m else None

def sofr_ticker_for_source(tenor: str, source: str) :
    """
    Construct a SOFR ticker with the requested pricing source.
    NOTE: keeps your existing format: USOSFR{tenor} <SRC> Curncy  (no 'Y' after tenor)
    """
    return f"USOSFR{tenor} {source} Curncy"

def ensure_2d(df_or_ser, col_name=None):
    """Return a DataFrame even if a Series came back from BDH."""
    if isinstance(df_or_ser, pd.Series):
        return df_or_ser.to_frame(name=col_name or df_or_ser.name or 'value')
    return df_or_ser

def group_region_by_source(region_map: dict, default_source: str = "CMPT"):
    """
    Group the region's currency tickers by pricing source, falling back to `default_source`
    when the ticker doesn't contain CMPT/CMPN/CMPL.
    Returns: {'CMPT': {'EUR': '...', ...}, 'CMPL': {...}, ...}
    """
    groups: dict[str, dict] = {}
    for ccy, tkr in region_map.items():
        src = extract_source(tkr) or default_source
        groups.setdefault(src, {})[ccy] = tkr
    return groups
def directional_beta(sub: pd.DataFrame, colx: str, coly: str, direction: str):
    """Beta = sum(dY) / sum(dX) when dX > 0 ('up') or dX < 0 ('down')."""
    if direction == 'up':
        sl = sub[sub[colx] > 0.0]
    else:
        sl = sub[sub[colx] < 0.0]
    if sl.empty or np.isclose(sl[colx].sum(), 0.0):
        return np.nan
    return sl[coly].sum() / sl[colx].sum()

def compute_bar_betas(region_map: dict, shift_sofr_days: int = 0):
    rows = []
    by_src = group_region_by_source(region_map)  # {'CMPT': {...}, 'CMPL': {...}, ...}

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

def rolling_directional_betas(region_map: dict, shift_sofr_days: int = 0):
    out: dict[str, pd.DataFrame] = {}
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
def last_roll_date(roll_dict: dict):
    return max(
        (df.index.max() for df in roll_dict.values() if df is not None and not df.empty)
    )
def style_time_axis(ax):
    ax.grid(True, alpha=0.8, linestyle=':', linewidth=0.8, color='#aaaaaa')   # darker gridlines
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(45); lbl.set_fontsize(8)

def plot_region_grid(region_name: str, bar_df: pd.DataFrame, roll_dict: dict,
                     ncols: int, fig_size=(12, 12), save_name: str = "out.png"):
    """
    Figure layout:
      - Row 0: bar chart spanning all columns
      - Below: rolling panels for each currency in a grid with ncols columns
    """
    currencies = list(bar_df.index)
    n_roll = len(currencies)
    nrows_roll = int(np.ceil(n_roll / ncols))
    total_rows = 1 + nrows_roll
    fig = plt.figure(figsize=fig_size)
    gs = fig.add_gridspec(total_rows, ncols, hspace=0.45, wspace=0.25)
    ax_bar = fig.add_subplot(gs[0, :])
    x = np.arange(len(currencies))
    w = 0.35
    ax_bar.bar(x - w/2, bar_df['beta_up'],   width=w, label='Beta-up',   color='tab:blue')
    ax_bar.bar(x + w/2, bar_df['beta_down'], width=w, label='Beta-down', color='tab:red')
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(currencies, rotation=0)
    ax_bar.set_ylabel('Beta (ΔLocal / ΔSOFR)')
    ax_bar.set_title(f"{region_name} {TENOR}Y vs US SOFR {TENOR}Y — Directional Betas (last ~3m, |ΔSOFR|≥0.3bp)")
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
            ax.set_title(f"{ccy} — Rolling 22d")
            ax.set_ylabel("β")
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

### Inventory charts
retail_inventories_tickers = [
    'RSRSTOTL Index',  # Retail Inventories Total
    'RSRSMOTV Index',  # Motor vehicles and parts
    'RSRSFURN Index',  # Furniture & home furnishings
    'RSRSBUIL Index',  # Building materials & supplies
    'RSRSFOOD Index',  # Food & Beverage stores
    'RSRSCLOT Index',  # Clothing stores
    'RSRSGENR Index',  # General merchandise
]

wholesale_inventories_tickers = [
    'MWINDRBL Index',  # Merchant wholesalers Durable Goods
    'MWINNDRB Index',  # Merchant wholesalers Nondurable Goods
    'MWINTOT Index',   # Merchant wholesalers Total
]

soft_data_current_tickers = [
    'NAPMINV Index',   # ISM Manufacturing Inventories
    'EMPRINVT Index',  # Empire State Inventories SA
    'OUTFIVF Index',   # Philly Fed current inventories
    'KCLSIFIN Index',  # Kansas Fed Finished Goods Inventories (m/m)
    'TROSINIX Index',  # Dallas Fed Retail Inventories
    'RCHSILFG Index',  # Richmond Fed Finished Products Inventories
]

soft_data_expectations_tickers = [
    'KC6SIFIN Index',  # Kansas Fed 6m expected Finished Goods Inventories
    'OUMFIVF Index',   # Philly Fed 6m expected inventories
    'TROSIFIX Index',  # Dallas Fed Retail Inventories 6m
    'RC6SEMFG Index',  # Richmond Fed Finished Prods Inventories 6m
    'EMPR6INV Index',  # Empire State Inventories SA 6m ahead
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
    # Current
    'NAPMINV Index':  'ISM Mfg Inv',
    'EMPRINVT Index': 'Empire State Inv',
    'OUTFIVF Index':  'Philadelphia Fed Inv',
    'KCLSIFIN Index': 'Kansas Fed Inv',
    'TROSINIX Index': 'Dallas Fed Retail Inv',
    'RCHSILFG Index': 'Richmond Fed Inv',
    # Expectations (6m)
    'KC6SIFIN Index': 'Kansas Fed Inv',
    'OUMFIVF Index':  'Philadelphia Fed Inv',
    'TROSIFIX Index': 'Dallas Fed Retail Inv',
    'RC6SEMFG Index': 'Richmond Fed Inv',
    'EMPR6INV Index': 'Empire State Inv',}

FIELD       = "PX_LAST"
START_DATE  = datetime(2018, 1, 1)
END_DATE    = datetime.today()
SAVE_PATH   = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
OUTFILE     = SAVE_PATH / "US_Inventories_Tracker.png"
# --- helpers for per-chart last data dates ---
def _last_data_date_df(df: pd.DataFrame) -> pd.Timestamp:
    """Return the max index where the DataFrame has at least one non-NaN."""
    if df is None or df.empty:
        return pd.NaT
    kept = df.dropna(how="all")
    return kept.index.max() if not kept.empty else pd.NaT

def _last_data_date_series(s: pd.Series) -> pd.Timestamp:
    """Return the max index where the Series is non-NaN."""
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
    """
    Expectations: only Fed Surveys avg on a single axis.
    """
    ln, = ax.plot(fed_avg_series.index, fed_avg_series, lw=2.0,
                  color=color_by_label[label_FED_EXP], label=label_FED_EXP)
    ax.axhline(0, color='grey', lw=0.9, ls='--')
    ax.set_title(title, fontsize=12)
    ax.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
    ax.legend([ln], [ln.get_label()], loc='upper left', fontsize=8, ncol=1)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.tick_params(axis='x', rotation=45)

# --- data pull & transforms ---
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

ISM_TKR = 'NAPMINV Index'  # ISM Manufacturing Inventories
fed_now_cols = [c for c in soft_now_m.columns if c != ISM_TKR]
fed_now_avg  = soft_now_m[fed_now_cols].mean(axis=1, skipna=True)
fed_exp_avg  = soft_exp_m.mean(axis=1, skipna=True)

label_ISM      = label_short_map.get(ISM_TKR, ISM_TKR)
label_FED_NOW  = 'Fed Surveys Inventories (avg)'
label_FED_EXP  = 'Fed Surveys Inventories (6m avg)'

all_soft_tickers = soft_data_current_tickers + soft_data_expectations_tickers
all_soft_labels  = [label_short_map.get(t, t) for t in all_soft_tickers]

if 'color_by_label' not in globals():
    cmap = plt.get_cmap('tab10')
    color_by_label = {}
for extra in [label_ISM, label_FED_NOW, label_FED_EXP]:
    if extra not in color_by_label:
        color_by_label[extra] = plt.get_cmap('tab10')(len(color_by_label) % 10)

# --- plotting ---
fig, axes = plt.subplots(2, 2, figsize=(18, 10))
(ax11, ax12), (ax21, ax22) = axes

# TL: Retail Inventories — 3m/3m %
for c in retail_3m3m.columns:
    ax11.plot(retail_3m3m.index, retail_3m3m[c], lw=1.7, label=c)
ax11.axhline(0, color='grey', lw=0.9, ls='--')
ax11.set_title("Retail Inventories — 3m/3m%", fontsize=12)
ax11.set_ylabel("%")
ax11.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
ax11.legend(fontsize=8, ncol=2, loc='upper left')
ax11.xaxis.set_major_locator(mdates.YearLocator())
ax11.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
ax11.tick_params(axis='x', rotation=45)
ax11.set_ylim(-10, 10)

# TR: Wholesale Inventories — 3m/3m %
for c in wholesale_3m3m.columns:
    ax12.plot(wholesale_3m3m.index, wholesale_3m3m[c], lw=1.8, label=c)
ax12.axhline(0, color='grey', lw=0.9, ls='--')
ax12.set_title("Wholesale Inventories — 3m/3m%", fontsize=12)
ax12.set_ylabel("%")
ax12.grid(True, linestyle=':', linewidth=0.7, color='#cccccc', alpha=0.9)
ax12.legend(fontsize=8, ncol=1, loc='upper left')
ax12.xaxis.set_major_locator(mdates.YearLocator())
ax12.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
ax12.tick_params(axis='x', rotation=45)

plot_soft_current_ISM_vs_fedavg(
    ax21, soft_now_m, fed_now_avg,
    "Soft Data — Current Inventories (ISM LHS; Fed Surveys avg RHS)",
    label_ISM, label_FED_NOW, color_by_label,
    lhs_ticker=ISM_TKR)

plot_soft_expectations_fedavg(
    ax22, fed_exp_avg,
    "Soft Data — 6-Month Expectations (Fed Surveys avg)",
    label_FED_EXP, color_by_label)

# --- per-chart last data dates & annotations ---
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

fig.suptitle("US Inventories & Surveys — Retail/Wholesale (3m/3m%) & Soft Data",
             fontsize=14, y=0.98)

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig(OUTFILE, bbox_inches='tight')

del (retail_raw, wholesale_raw, soft_now_raw, soft_exp_raw,
     retail_m, wholesale_m, soft_now_m, soft_exp_m,
     retail_3m3m, wholesale_3m3m, fig, axes, ax11, ax12, ax21, ax22)
## DXY sensitivity to 2y, 10y, equities
FIELD       = 'PX_LAST'
START_DATE  = datetime(2020, 1, 1)
END_DATE    = datetime.today()
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

def transform_series(wide: pd.DataFrame) -> pd.DataFrame:
    """
    Create daily changes on a business-day grid:
      - DXY, SPX: 100 * Δln(price)
      - VIX: Δlevel (absolute change in index points)
      - US10Y, US1Y1Y: 100 * Δrate (bp)
    """
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
    """
    Rolling two-stage:
      1) y ~ x_stage1                     -> R2_stage1
      2) residuals(y|x_stage1) ~ x_stage2 -> R2_stage2 (one series per x_stage2)
    Returns: DataFrame with columns ['R2_stage1', f'R2_{x_stage2}', ...]
    """
    # Check if statsmodels is available
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
        # stage-2 for each x2 separately
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

# Rolling: (1) SPX as stage-1 driver; (2) VIX (absolute Δ) as stage-1 driver
res_spx = rolling_two_stage(
    chg, y_col='DXY',
    x_stage1='SPX',
    x_stage2_list=['US1Y1Y', 'US10Y'],
    win=ROLL_WIN
)

res_vix = rolling_two_stage(
    chg, y_col='DXY',
    x_stage1='VIX',             
    x_stage2_list=['US1Y1Y', 'US10Y'],
    win=ROLL_WIN
)

WIN_3M = 63
WIN_1M = 21

corr_3m = pd.DataFrame({
    'DXY~SPX': chg['DXY'].rolling(WIN_3M).corr(chg['SPX']),
    'DXY~VIX_inv': -chg['DXY'].rolling(WIN_3M).corr(chg['VIX']),  # invert VIX corr
}).dropna()

corr_1m = pd.DataFrame({
    'DXY~SPX': chg['DXY'].rolling(WIN_1M).corr(chg['SPX']),
    'DXY~VIX_inv': -chg['DXY'].rolling(WIN_1M).corr(chg['VIX']),  # invert VIX corr
}).dropna()

cutoff_2y = pd.Timestamp.today().normalize() - pd.DateOffset(years=2)
corr_1m = corr_1m[corr_1m.index >= cutoff_2y]

import matplotlib.gridspec as gridspec

fig = plt.figure(figsize=(14, 14))
gs = gridspec.GridSpec(3, 2, height_ratios=[1.0, 1.0, 0.9], hspace=0.35, wspace=0.25)

# Row 1: DXY ~ SPX (stage-1), residual ~ {1Y1Y, 10Y}
ax1 = fig.add_subplot(gs[0, :])
if not res_spx.empty:
    ax1.plot(res_spx.index, res_spx['R2_stage1'],  label='R²: DXY ~ SPX (stage 1)', color='black',     lw=2.2)
    ax1.plot(res_spx.index, res_spx['R2_US1Y1Y'],  label='R²: resid ~ 1Y1Y',        color='tab:blue',  lw=2.0)
    ax1.plot(res_spx.index, res_spx['R2_US10Y'],   label='R²: resid ~ 10Y',         color='tab:orange',lw=2.0)
    ax1.set_ylim(0, 1)
    ax1.set_title('Stage-1: DXY ~ SPX  |  Stage-2: residual ~ {1Y1Y, 10Y}  (rolling 3m)', fontsize=12)
    ax1.grid(True, linestyle=':', linewidth=0.8, alpha=0.8)
    ax1.legend(loc='upper left', fontsize=9)
else:
    ax1.text(0.5, 0.5, 'Insufficient data for SPX regression', ha='center', va='center')
    ax1.axis('off')

# Row 2: DXY ~ VIX (abs Δ), residual ~ {1Y1Y, 10Y}
ax2 = fig.add_subplot(gs[1, :])
if not res_vix.empty:
    ax2.plot(res_vix.index, res_vix['R2_stage1'],  label='R²: DXY ~ VIX Δ (stage 1)', color='black',     lw=2.2)
    ax2.plot(res_vix.index, res_vix['R2_US1Y1Y'],  label='R²: resid ~ 1Y1Y',          color='tab:blue',  lw=2.0)
    ax2.plot(res_vix.index, res_vix['R2_US10Y'],   label='R²: resid ~ 10Y',           color='tab:orange',lw=2.0)
    ax2.set_ylim(0, 1)
    ax2.set_title('Stage-1: DXY ~ VIX (absolute Δ)  |  Stage-2: residual ~ {1Y1Y, 10Y}  (rolling 3m)', fontsize=12)
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
del raw, chg, res_spx, res_vix, ax

### Gold ETF Fund Flows
NAMES = {
    'GLD US Equity':  'SPDR Gold Shares (GLD)',
    'IAU US Equity':  'iShares Gold Trust (IAU)',
    'IGLN LN Equity': 'iShares Physical Gold (IGLN)',
    'GLDM US Equity': 'SPDR Gold MiniShares (GLDM)',
    'SGLD LN Equity': 'Invesco Physical Gold (SGLD)',
    'SGOL US Equity': 'abrdn Physical Gold Shares (SGOL)',
    'GOLD AU Equity': 'Global X Physical Gold (GOLD AU)',}
TICKERS = list(NAMES.keys())
START_DATE = datetime(2015, 1, 1)
END_DATE   = datetime.today()
SAVE_PATH   = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
OUTFILE = SAVE_PATH / f"Gold_ETF_FundFlows.png"

def normalize_ccy(ccy: str) -> str:
    """Map odd Bloomberg currency mnemonics to ISO (e.g., GBp/GBX -> GBP)."""
    if ccy is None:
        return 'USD'
    c = ccy.upper()
    if c in {'GBP', 'GBX', 'GBP*', 'GBP CURNCY', 'GBP CURN'}:
        return 'GBP'
    if c in {'GBP PENCE', 'GBX', 'GBPGBX', 'GBP/GBX', 'GBP(PENCE)', 'GBP PENCE STERLING', 'GBP PENCE STER'}:
        return 'GBP'
    if c in {'GBp', 'GBX'}:
        return 'GBP'
    return c

def usd_per_ccy_series(ccy: str, start: datetime, end: datetime) -> pd.Series:
    """
    Return a daily series of USD per 1 CCY.
    Try 'CCYUSD Curncy' (direct USD/CCY quote) first; if empty, try 'USDCCY Curncy' and invert.
    """
    if ccy == 'USD':
        idx = pd.date_range(start, end, freq='B')
        return pd.Series(1.0, index=idx)

    pair1 = f'{ccy}USD Curncy'  # USD per CCY
    pair2 = f'USD{ccy} Curncy'  # CCY per USD (needs inversion)

    px = blp.bdh([pair1, pair2], 'PX_LAST', start, end)
    if isinstance(px.columns, pd.MultiIndex):
        if 'PX_LAST' in px.columns.get_level_values(-1):
            px = px.xs('PX_LAST', axis=1, level=-1)
        else:
            px = px.xs('PX_LAST', axis=1, level=0)

    s1 = px.get(pair1)
    s2 = px.get(pair2)

    # Prefer the series with more actual data
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

raw_flows = blp.bdh(
    tickers=TICKERS,
    flds='FUND_FLOW',
    start_date=START_DATE,
    end_date=END_DATE,)

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
sum20      = total_usd.rolling(20, min_periods=1).sum()
roll_12m  = total_usd.rolling('252D').sum()
last_data_date = pd.Timestamp(last_reported_date) if last_reported_date in total_usd.index else total_usd.dropna().index.max()
last_sum20_val  = sum20.loc[last_data_date]
last_12m_val   = roll_12m.loc[last_data_date]

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
print(f"Saved chart to: {OUTFILE}")
del (
    raw_flows, flows_native, flows_native_b,
    bidx, ccy_df, ccy_map, usd_per_ccy,
    flows_usd, total_usd, sum20, roll_12m,
    last_reported_date, last_data_date, last_sum20_val, last_12m_val,
    fig, axes, ax1, ax2)


#### VND Spot vs SBV Interbank Rate Band
TICKERS = {
    'VND T130 Curncy': 'VND Spot',
    'SBVNUSD Index':   'CR',  # rename label to CR (Central Rate)
}

# Common window for ALL charts
START_DATE = datetime(2018, 1, 1)
END_DATE   = datetime.today()
CUTOFF     = pd.Timestamp('2018-01-01')

SAVE_PATH = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
OUTFILE   = SAVE_PATH / f"VND_vs_CR_Band.png"

raw = blp.bdh(
    tickers=list(TICKERS.keys()),
    flds='PX_LAST',
    start_date=START_DATE,
    end_date=END_DATE,
)

# Handle Bloomberg’s MultiIndex vs. single index return shape
if isinstance(raw.columns, pd.MultiIndex):
    if "PX_LAST" in raw.columns.get_level_values(-1):
        df = raw.xs("PX_LAST", level=-1, axis=1)
    else:
        df = raw.xs("PX_LAST", level=0, axis=1)
else:
    df = raw.copy()

# Normalize/clean
df.index = pd.to_datetime(df.index)
df = df.apply(pd.to_numeric, errors='coerce')
df = df.rename(columns=TICKERS)

# Track the last actual reported date (any column non-null)
last_reported_date = df.dropna(how='all').index.max()

# Reindex to business days and forward-fill short gaps (≤ 2 days)
bidx = pd.date_range(df.index.min(), df.index.max(), freq='B')
df_b = df.reindex(bidx)
df_b['CR']        = df_b['CR'].ffill(limit=2)
df_b['VND Spot']  = df_b['VND Spot'].ffill(limit=2)

# Require overlapping observations
valid_mask = df_b[['VND Spot', 'CR']].notna().all(axis=1)
if not valid_mask.any():
    raise ValueError("No overlapping data between VND Spot and CR.")
df_b = df_b.loc[valid_mask.idxmax():]  # start from first overlap

# Apply the common 2018+ cutoff to EVERYTHING
df_w = df_b.loc[df_b.index >= CUTOFF].copy()
if df_w.empty:
    raise ValueError("No data available on or after 2018-01-01 after alignment.")

# Series shortcuts (windowed)
cr    = df_w['CR']
spot  = df_w['VND Spot']
upper = cr * 1.05
lower = cr * 0.95

# Split in-band vs out-of-band observations
in_band  = spot.where((spot >= lower) & (spot <= upper))
out_band = spot.where((spot > upper) | (spot < lower))

# Latest values for markers/labels (within the window)
last_data_date = df_w.dropna(how='all').index.max()
latest_idx  = df_w[['VND Spot','CR']].dropna().index.max()
latest_spot = df_w.at[latest_idx, 'VND Spot']
latest_cr   = df_w.at[latest_idx, 'CR']

# Middle panel: % distance of Spot from CR (windowed)
pct_dist = (spot / cr - 1.0) * 100.0
latest_pct_idx = pct_dist.dropna().index.max()
latest_pct_val = pct_dist.loc[latest_pct_idx]

# Bottom panel: CR cumulative drawdown from prior high (windowed)
# Use a NaN-robust running max; forward-fill inside window so weekends/holidays don't break it
cr_for_dd          = cr.ffill()  # unlimited ffill *inside* the window to keep drawdown stable
cr_running_max_win = cr_for_dd.expanding(min_periods=1).max()
cr_dd_plot         = cr_for_dd - cr_running_max_win  # <= 0; 0 at new highs

latest_dd_idx = cr_dd_plot.dropna().index.max()
latest_dd_val = cr_dd_plot.loc[latest_dd_idx]

# --- Plot: 3 rows, shared x so all panels line up perfectly ---
fig, (ax, ax2, ax3) = plt.subplots(
    3, 1, figsize=(14, 12), sharex=True,
    gridspec_kw={'height_ratios': [2.1, 1.0, 1.0], 'hspace': 0.08}
)

# Top: band chart
ax.fill_between(cr.index, lower.values, upper.values, color='#B3D7FF', alpha=0.35, label='±5% band')
ax.plot(cr.index, cr.values, color='#003f5c', lw=2.2, label='CR')
ax.plot(in_band.index, in_band.values, color='black', lw=1.8, label='VND Spot (in band)')
ax.plot(out_band.index, out_band.values, color='red', lw=1.8, label='VND Spot (out of band)', zorder=5)

# Latest value labels on the main chart
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

ax.set_title("VND Spot vs CR (±5% Band)", fontsize=15)
ax.set_ylabel("USDVND")
ax.grid(True, linestyle=':', alpha=0.5)
ax.legend(loc='upper left', fontsize=9)

# Middle: % distance of Spot from CR
ax2.plot(pct_dist.index, pct_dist.values, lw=1.8, label='Spot vs CR (% diff)')
ax2.axhline(0, linestyle='--', alpha=0.5)
ax2.axhline(5, linestyle=':', alpha=0.7, label='±5% band')
ax2.axhline(-5, linestyle=':', alpha=0.7)
ax2.plot(latest_pct_idx, latest_pct_val, marker='o', ms=5, zorder=6)
ax2.annotate(f"{latest_pct_val:+.2f}%",
             xy=(latest_pct_idx, latest_pct_val), xytext=(6, 8),
             textcoords='offset points', ha='left', va='bottom',
             bbox=dict(facecolor='white', edgecolor='none', alpha=0.7))
ax2.set_ylabel("Spot − CR (%)")
ax2.grid(True, linestyle=':', alpha=0.5)
ax2.legend(loc='upper left', fontsize=9)

# Bottom: CR cumulative drawdown from prior high (same 2018+ window)
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

# Shared X formatting (applied once on the bottom axis)
ax3.xaxis.set_major_locator(mdates.YearLocator())
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
plt.setp(ax3.get_xticklabels(), rotation=45)

# Figure annotation
fig.text(
    0.98, 0.96, f"Last data: {pd.to_datetime(last_data_date).date()}",
    ha='right', va='top', fontsize=10,
    bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25')
)

plt.tight_layout()
plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
print(f"Saved chart to: {OUTFILE}")

# Tidy up large objects
del (
    raw, df, df_b, bidx, valid_mask, df_w,
    cr, spot, upper, lower, in_band, out_band,
    pct_dist, cr_for_dd, cr_running_max_win, cr_dd_plot,
    latest_idx, latest_spot, latest_cr, latest_pct_idx, latest_pct_val,
    latest_dd_idx, latest_dd_val,
    fig, ax, ax2, ax3, last_reported_date, last_data_date
)


# Indonesia IDR DNDF Expiry Schedule
START = (pd.Timestamp.today().normalize() - pd.Timedelta(days=370))
END   = pd.Timestamp.today().normalize()

TKS_1M = ['IDFB1MTH Index', 'IDFB1MPM Index', 'IDFB1MRP Index']  
TKS_3M = ['IDFB3MTH Index', 'IDFB3MPM Index']
ALL_TKS = TKS_1M + TKS_3M
FIELD = 'VOLUME'
PNG_OUT = G_CHART_DIR / "IDR_DNDF_Expiry_Schedule.png"

def flatten_xbbg(raw: pd.DataFrame, field: str) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        if field in raw.columns.get_level_values(-1):
            return raw.xs(field, axis=1, level=-1)
        return raw.xs(field, axis=1, level=0)
    return raw.copy()

def bds_calendar_non_settlement(cal_code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
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

def make_business_day(holidays: pd.DatetimeIndex) -> CustomBusinessDay:
    return CustomBusinessDay(weekmask='Mon Tue Wed Thu Fri', holidays=pd.to_datetime(holidays))

def is_business_day(ts, hol_set) -> bool:
    return (ts.weekday() < 5) and (ts.normalize() not in hol_set)

def roll_to_next_business_day(ts, hol_set) -> pd.Timestamp:
    d = ts.normalize()
    while not is_business_day(d, hol_set):
        d += pd.Timedelta(days=1)
    return d

def add_business_days(ts: pd.Timestamp, n: int, cbd: CustomBusinessDay) -> pd.Timestamp:
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

CBD_SETTLE = make_business_day(hol_union)  # for value-date steps (ID ∪ US)
CBD_ID     = make_business_day(hol_id)     # for fixing T-2 (ID only)

raw = blp.bdh(
    tickers=ALL_TKS,
    flds=FIELD,
    start_date=START,
    end_date=END,
    Per='D',
)
vols = flatten_xbbg(raw, FIELD)
vols.index = pd.to_datetime(vols.index)
vols = vols.apply(pd.to_numeric, errors='coerce')

last_auction_date = vols.dropna(how='all').index.max()
v1m = safe_sum_cols(vols, TKS_1M)
v3m = safe_sum_cols(vols, TKS_3M)
v1m = v1m.where(v1m.notna() & (v1m != 0))
v3m = v3m.where(v3m.notna() & (v3m != 0))
auctions = pd.DataFrame({'1M': v1m, '3M': v3m}).dropna(how='all')

# Build expiry/value/fixing schedule
#  value_date: (auction + 2BD on ID∪US) + tenor_months, then roll forward under ID∪US
#  fixing_date: value_date - 2BD under ID-only (US holidays ignored on the backward count)
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
print(f"Saved chart to: {PNG_OUT}")

del (
    START, END, TKS_1M, TKS_3M, ALL_TKS, FIELD,
    flatten_xbbg, bds_calendar_non_settlement, make_business_day,
    is_business_day, roll_to_next_business_day, add_business_days,
    CAL_START, CAL_END, hol_id, hol_us, hol_union, hol_set_id, hol_set_union,
    CBD_SETTLE, CBD_ID,
    raw, vols, last_auction_date, v1m, v3m, auctions,
    rows, schedule, today, upcoming, upcoming_totals,
    end_bar, bidx, totals_dt, bar_series, bar_series_mn,
    fig, gs, ax_tbl, ax_bar, PNG_OUT)


#### India Banking Seasonality
TICKER = 'INBGBKLQ Index'
FIELD = 'PX_LAST'

TODAY = pd.Timestamp.today().normalize()
CURR_YEAR = TODAY.year

YEARS_10 = list(range(CURR_YEAR - 10, CURR_YEAR))  
START = pd.Timestamp(f'{YEARS_10[0]}-01-01')
END   = pd.Timestamp(f'{YEARS_10[-1]}-12-31')

PNG_OUT = G_CHART_DIR / "INBGBKLQ_Seasonality.png"

raw = blp.bdh(
    tickers=[TICKER],
    flds=FIELD,
    start_date=START,
    end_date=END,
    Per='D',
)

# Flatten to a simple Series
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
    doy_levels = doy_levels[doy_levels.index <= 365]  # drop leap day

avg_level = doy_levels.mean(axis=1) if not doy_levels.empty else pd.Series(dtype=float)
p25 = doy_levels.quantile(0.25, axis=1) if not doy_levels.empty else pd.Series(dtype=float)
p75 = doy_levels.quantile(0.75, axis=1) if not doy_levels.empty else pd.Series(dtype=float)

# Build x-axis dates using a scalar anchor (avoid broadcast mismatch)
if not avg_level.empty:
    base_year = 2001  # non-leap
    base_date = pd.Timestamp(f'{base_year}-01-01')
    doy = pd.Index(avg_level.index).astype(int).sort_values()
    x_dates = base_date + pd.to_timedelta(doy - 1, unit='D')
    # Align values to sorted DOY
    avg_level = avg_level.reindex(doy)
    p25 = p25.reindex(doy)
    p75 = p75.reindex(doy)
else:
    x_dates = pd.DatetimeIndex([])

# -------------------------
# Helpers for period changes
# -------------------------
def last_on_or_before(s, dt):
    s2 = s.loc[:dt].dropna()
    return s2.iloc[-1] if len(s2) else np.nan

def year_level_changes(level_series, years):
    """Signed LEVEL changes: End-Sep→Oct31, End-Sep→Dec31."""
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

# Sep-15–30 max → Oct min (signed change) + date labels
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
        labels[y] = f"{idx_max_sep.strftime('%d%b')}\u2013{idx_min_oct.strftime('%d%b')}"
    return pd.Series(changes).dropna(), labels

# Monthly peak → next-month trough distribution (per month across years)
def month_peak_to_next_trough(level_series, years):
    """
    For each year and each month m, compute:
      change = min(level in next month) - max(level in current month)
    Returns dict: {month (1..12): [changes across years]}, with empty months removed.
    """
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
                continue  # next month outside our pull window

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

# -------------------------
# Plot: 3 rows total
#   Row 1: Seasonality (full width)
#   Rows 2–3: 2×2 with the four charts
# -------------------------
fig = plt.figure(figsize=(14, 12), constrained_layout=True)
gs = fig.add_gridspec(nrows=3, ncols=2, height_ratios=[2.2, 1.5, 1.5])

# Row 1: Seasonality
ax_seas = fig.add_subplot(gs[0, :])
if len(x_dates) > 0:
    ax_seas.plot(x_dates, avg_level.values, linewidth=2.0, label='Avg level (10y)')
    ax_seas.fill_between(x_dates, p25.values, p75.values, alpha=0.25, label='25–75% band')
    ax_seas.set_title("Seasonality — Average Level (Last 10 Full Years)")
    ax_seas.set_ylabel("Index level")
    ax_seas.xaxis.set_major_locator(mdates.MonthLocator())
    ax_seas.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax_seas.grid(True, linestyle=':', alpha=0.5)
    ax_seas.legend(loc='upper left', fontsize=8)
else:
    ax_seas.text(0.5, 0.5, "Insufficient data for seasonality.", ha='center', va='center')
    ax_seas.set_axis_off()

# Helper: vertical bar chart (index = year)
def draw_year_bars(ax, series, title, ylabel="Level change", per_bar_labels=None):
    if series is None or series.empty:
        ax.text(0.5, 0.5, "No data", ha='center', va='center')
        ax.set_axis_off()
        return
    years = series.index.astype(int).tolist()
    vals = series.values
    bars = ax.bar(years, vals)
    # Optional per-bar labels (e.g., '17Sep–23Oct'), rotated to fit
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

# Row 2
ax_bar1 = fig.add_subplot(gs[1, 0])
draw_year_bars(ax_bar1, bars_31, "End Sep → Oct 31")

ax_bar2 = fig.add_subplot(gs[1, 1])
draw_year_bars(ax_bar2, bars_yr, "End Sep → Dec 31")

# Row 3
ax_bar3 = fig.add_subplot(gs[2, 0])
draw_year_bars(ax_bar3, bars_hilo, "Max(15–30 Sep) → Min(Oct)", per_bar_labels=hilo_labels)

ax_box = fig.add_subplot(gs[2, 1])
ax_box.set_title("Monthly Peak → Next-Month Trough (Last 10y) — Level Change")
ax_box.set_ylabel("Level change (next-month min − current-month max)")
if month_changes:
    # Chronological month order; only months with data
    month_order = [m for m in range(1, 13) if m in month_changes]
    data = [month_changes[m] for m in month_order]
    labels = [pd.Timestamp(2001, m, 1).strftime('%b') for m in month_order]
    ax_box.boxplot(
        data, labels=labels, showmeans=True, meanline=True, patch_artist=True
    )
    ax_box.grid(True, linestyle=':', alpha=0.5)
else:
    ax_box.text(0.5, 0.5, "No data", ha='center', va='center')
    ax_box.set_axis_off()

# Only the "Last data" stamp
if pd.notna(last_data_date):
    fig.text(
        0.98, 0.985, f"Last data: {pd.to_datetime(last_data_date).date()}",
        ha='right', va='top', fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'),
    )

fig.suptitle("INBGBKLQ Index — Seasonality & Post-September Level Changes", y=0.995, fontsize=15)

# Save
plt.savefig(PNG_OUT, dpi=150, bbox_inches='tight')
print(f"Saved chart to: {PNG_OUT}")


### INR Basis vs Reserves ex gold and banking liq
START = pd.Timestamp('2010-01-01')
END   = pd.Timestamp.today().normalize()
LOOKBACK_5Y_START = END - pd.DateOffset(years=5)

TKS = [
    'IRNI3M Curncy',    # INR 3M
    'IRSWNIC Curncy',   # INR IRS (3M leg; per your convention)

    'IRNI12M Curncy',   # INR 12M
    'IRSWNI1 Curncy',   # INR IRS 1Y
    'IRNI2Y Curncy',    # INR 2Y
    'IRSWNI2 Curncy',   # INR IRS 2Y

    'USDINR Curncy',    # USDINR spot

    'INMORES$ Index',   # RBI Total Reserves (USD)
    'INMOGOL$ Index',   # RBI Gold Reserves (USD)

    'INBGBKLQ Index',   # Banking Liquidity
    'MKTIREPO Index',   # VRR (Repo)
    'MKTIRRPO Index',   # VRRR (Reverse Repo)
]

FIELD = 'PX_LAST'


# Output (expects G_CHART_DIR to be set in your environment/session)
PNG_OUT1 = G_CHART_DIR / "INR Basis vs Reserves.png"
PNG_OUT2 = G_CHART_DIR / "INR Basis vs Banking Liq.png"

def flatten_xbbg(raw: pd.DataFrame, field: str) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        if field in raw.columns.get_level_values(-1):
            return raw.xs(field, axis=1, level=-1)
        return raw.xs(field, axis=1, level=0)
    return raw.copy()

def bday_align_ffill(
    df: pd.DataFrame,
    ffill_limit: int = 10,
    ffill_limits = None,
    fillna_values = None,
) -> pd.DataFrame:
    """
    Reindex to business days, then apply per-column forward fills and/or fillna.
    - ffill_limit: default ffill limit for columns not in ffill_limits
    - ffill_limits: per-column ffill limit overrides (use 0 to disable ffill)
    - fillna_values: per-column fillna values applied after ffill
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df = df[~df.index.duplicated(keep="last")].sort_index()

    bidx = pd.bdate_range(df.index.min(), df.index.max())
    out = df.reindex(bidx)

    limits = ffill_limits or {}
    # Apply per-column ffill (limit=0 means no forward-fill)
    for col in out.columns:
        lim = limits.get(col, ffill_limit)
        if lim is None:
            out[col] = out[col].ffill()
        elif lim > 0:
            out[col] = out[col].ffill(limit=int(lim))
        else:
            # lim == 0 -> no ffill
            pass

    # Apply per-column fillna after ffill
    if fillna_values:
        for col, val in fillna_values.items():
            if col in out.columns:
                out[col] = out[col].fillna(val)

    return out


def rolling_change(series: pd.Series, window_bdays: int) -> pd.Series:
    return series - series.shift(window_bdays)

def last5y(s: pd.Series) -> pd.Series:
    return s.loc[LOOKBACK_5Y_START:] if s is not None else None

# =========================
# Pull data (no Fill arg)
# =========================
raw = blp.bdh(
    tickers=TKS,
    flds=FIELD,
    start_date=START,
    end_date=END,
    Per='D',
)
df = flatten_xbbg(raw, FIELD)
df.index = pd.to_datetime(df.index)
df = df.apply(pd.to_numeric, errors='coerce')

# Align to business days, ffill (reserves are weekly)
df_b = bday_align_ffill(
    df,
    ffill_limit=10,  # keep existing default behavior for most series (incl. reserves weekly gaps)
    ffill_limits={
        'INBGBKLQ Index': 3,   # Banking liquidity: only allow small ffill
        'MKTIREPO Index': 0,   # no ffill; we'll fillna to 0
        'MKTIRRPO Index': 0,   # no ffill; we'll fillna to 0
    },
    fillna_values={
        'MKTIREPO Index': 0.0,
        'MKTIRRPO Index': 0.0,
    },
)

# =========================
# Derived series
# =========================
def col_or_raise(df_: pd.DataFrame, col: str) -> pd.Series:
    if col not in df_.columns:
        raise KeyError(f"Missing required Bloomberg series: {col}. Available: {list(df_.columns)}")
    s = df_[col].copy()
    s.name = col
    return s

# Core series
basis_3m = col_or_raise(df_b, 'IRNI3M Curncy')  - col_or_raise(df_b, 'IRSWNIC Curncy')
basis_1y = col_or_raise(df_b, 'IRNI12M Curncy') - col_or_raise(df_b, 'IRSWNI1 Curncy')
basis_2y = col_or_raise(df_b, 'IRNI2Y Curncy')  - col_or_raise(df_b, 'IRSWNI2 Curncy')

usdinr   = col_or_raise(df_b, 'USDINR Curncy')

# Reserves
res_tot  = col_or_raise(df_b, 'INMORES$ Index')
res_gold = col_or_raise(df_b, 'INMOGOL$ Index')
res_ex   = res_tot - res_gold

# Banking liquidity
bank_liq = col_or_raise(df_b, 'INBGBKLQ Index')
vrr      = col_or_raise(df_b, 'MKTIREPO Index')
vrrr     = col_or_raise(df_b, 'MKTIRRPO Index')
bank_liq_vrr = bank_liq - vrr + vrrr

# For “Last data” stamp(s)
stack_for_last1 = pd.concat([basis_1y, basis_2y, usdinr, res_ex], axis=1)
last_data_date1 = stack_for_last1.dropna(how='all').index.max()

stack_for_last2 = pd.concat([basis_3m, basis_1y, basis_2y, bank_liq, bank_liq_vrr], axis=1)
last_data_date2 = stack_for_last2.dropna(how='all').index.max()

# =========================
# Changes (63/126 bdays)
# =========================
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

# Inverted levels for reserves-ex gold (for the reserves relationship charts)
res_ex_inv = -res_ex
chg6_res_ex_inv = -chg6_res_ex
chg3_res_ex_inv = -chg3_res_ex

# =========================
# Rolling 1Y correlations (252bd windows)
# =========================
def corr_1y(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.rolling(252).corr(b)

# --- Chart 1: basis vs reserves (inv) + USDINR ---
corr_level_b1y_resinv  = corr_1y(basis_1y, res_ex_inv)
corr_chg6_b1y_resinv   = corr_1y(chg6_basis_1y, chg6_res_ex_inv)
corr_chg3_b1y_resinv   = corr_1y(chg3_basis_1y, chg3_res_ex_inv)

corr_level_b1y_usd     = corr_1y(basis_1y, usdinr)
corr_chg6_b1y_usd      = corr_1y(chg6_basis_1y, chg6_usd)
corr_chg3_b1y_usd      = corr_1y(chg3_basis_1y, chg3_usd)

corr_level_b2y_resinv  = corr_1y(basis_2y, res_ex_inv)
corr_chg6_b2y_resinv   = corr_1y(chg6_basis_2y, chg6_res_ex_inv)
corr_chg3_b2y_resinv   = corr_1y(chg3_basis_2y, chg3_res_ex_inv)

# --- Chart 2: basis vs banking liquidity ---
corr_level_liq_b3m     = corr_1y(basis_3m, bank_liq)
corr_chg6_liq_b3m      = corr_1y(chg6_basis_3m, chg6_liq)
corr_chg3_liq_b3m      = corr_1y(chg3_basis_3m, chg3_liq)

corr_level_liq_b1y     = corr_1y(basis_1y, bank_liq)
corr_chg6_liq_b1y      = corr_1y(chg6_basis_1y, chg6_liq)
corr_chg3_liq_b1y      = corr_1y(chg3_basis_1y, chg3_liq)

corr_level_liq_b2y     = corr_1y(basis_2y, bank_liq)
corr_chg6_liq_b2y      = corr_1y(chg6_basis_2y, chg6_liq)
corr_chg3_liq_b2y      = corr_1y(chg3_basis_2y, chg3_liq)

# --- Chart 2: basis vs banking liq incl. VRR/VRRR ---
corr_level_liqv_b3m    = corr_1y(basis_3m, bank_liq_vrr)
corr_chg6_liqv_b3m     = corr_1y(chg6_basis_3m, chg6_liq_vrr)
corr_chg3_liqv_b3m     = corr_1y(chg3_basis_3m, chg3_liq_vrr)

corr_level_liqv_b1y    = corr_1y(basis_1y, bank_liq_vrr)
corr_chg6_liqv_b1y     = corr_1y(chg6_basis_1y, chg6_liq_vrr)
corr_chg3_liqv_b1y     = corr_1y(chg3_basis_1y, chg3_liq_vrr)

corr_level_liqv_b2y    = corr_1y(basis_2y, bank_liq_vrr)
corr_chg6_liqv_b2y     = corr_1y(chg6_basis_2y, chg6_liq_vrr)
corr_chg3_liqv_b2y     = corr_1y(chg3_basis_2y, chg3_liq_vrr)

# =========================
# Restrict “blocks” to last 5y
# =========================
basis_3m_5   = last5y(basis_3m)
basis_1y_5   = last5y(basis_1y)
basis_2y_5   = last5y(basis_2y)

res_ex_inv_5 = last5y(res_ex_inv)
usd_5        = last5y(usdinr)

bank_liq_5      = last5y(bank_liq)
bank_liq_vrr_5  = last5y(bank_liq_vrr)

chg6_basis_3m_5 = last5y(chg6_basis_3m)
chg3_basis_3m_5 = last5y(chg3_basis_3m)

chg6_basis_1y_5 = last5y(chg6_basis_1y)
chg3_basis_1y_5 = last5y(chg3_basis_1y)
chg6_basis_2y_5 = last5y(chg6_basis_2y)
chg3_basis_2y_5 = last5y(chg3_basis_2y)

chg6_res_ex_inv_5 = last5y(chg6_res_ex_inv)
chg3_res_ex_inv_5 = last5y(chg3_res_ex_inv)
chg6_usd_5        = last5y(chg6_usd)
chg3_usd_5        = last5y(chg3_usd)

chg6_liq_5        = last5y(chg6_liq)
chg3_liq_5        = last5y(chg3_liq)
chg6_liq_vrr_5    = last5y(chg6_liq_vrr)
chg3_liq_vrr_5    = last5y(chg3_liq_vrr)

corr_level_b1y_resinv_5 = last5y(corr_level_b1y_resinv)
corr_chg6_b1y_resinv_5  = last5y(corr_chg6_b1y_resinv)
corr_chg3_b1y_resinv_5  = last5y(corr_chg3_b1y_resinv)

corr_level_b1y_usd_5 = last5y(corr_level_b1y_usd)
corr_chg6_b1y_usd_5  = last5y(corr_chg6_b1y_usd)
corr_chg3_b1y_usd_5  = last5y(corr_chg3_b1y_usd)

corr_level_b2y_resinv_5 = last5y(corr_level_b2y_resinv)
corr_chg6_b2y_resinv_5  = last5y(corr_chg6_b2y_resinv)
corr_chg3_b2y_resinv_5  = last5y(corr_chg3_b2y_resinv)

corr_level_liq_b3m_5  = last5y(corr_level_liq_b3m)
corr_chg6_liq_b3m_5   = last5y(corr_chg6_liq_b3m)
corr_chg3_liq_b3m_5   = last5y(corr_chg3_liq_b3m)

corr_level_liq_b1y_5  = last5y(corr_level_liq_b1y)
corr_chg6_liq_b1y_5   = last5y(corr_chg6_liq_b1y)
corr_chg3_liq_b1y_5   = last5y(corr_chg3_liq_b1y)

corr_level_liq_b2y_5  = last5y(corr_level_liq_b2y)
corr_chg6_liq_b2y_5   = last5y(corr_chg6_liq_b2y)
corr_chg3_liq_b2y_5   = last5y(corr_chg3_liq_b2y)

corr_level_liqv_b3m_5 = last5y(corr_level_liqv_b3m)
corr_chg6_liqv_b3m_5  = last5y(corr_chg6_liqv_b3m)
corr_chg3_liqv_b3m_5  = last5y(corr_chg3_liqv_b3m)

corr_level_liqv_b1y_5 = last5y(corr_level_liqv_b1y)
corr_chg6_liqv_b1y_5  = last5y(corr_chg6_liqv_b1y)
corr_chg3_liqv_b1y_5  = last5y(corr_chg3_liqv_b1y)

corr_level_liqv_b2y_5 = last5y(corr_level_liqv_b2y)
corr_chg6_liqv_b2y_5  = last5y(corr_chg6_liqv_b2y)
corr_chg3_liqv_b2y_5  = last5y(corr_chg3_liqv_b2y)

# =========================# =========================
# Plot helpers
# =========================
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

# =========================
# Chart 1: Reserves + USDINR (reordered blocks)
# =========================
fig1 = plt.figure(figsize=(18, 20), constrained_layout=True)
gs1 = fig1.add_gridspec(nrows=7, ncols=3, height_ratios=[2.4, 1.3, 1.3, 1.3, 1.3, 1.3, 1.3])

c_basis = '#1f77b4'  # blue
c_fx    = '#ff7f0e'  # orange
c_res   = '#2ca02c'  # green

# --- Row 0: Big 3-axis chart ---
ax_top  = fig1.add_subplot(gs1[0, :])

l1 = ax_top.plot(basis_1y.index, basis_1y.values, color=c_basis, label='INR 1Y Basis (LHS)', linewidth=1.9)[0]
ax_top.set_ylabel("Basis", color=c_basis)
ax_top.tick_params(axis='y', colors=c_basis)

ax_top2 = ax_top.twinx()
l2 = ax_top2.plot(usdinr.index, usdinr.values, color=c_fx, label='USDINR (RHS-1)', linewidth=1.4)[0]
ax_top2.set_ylabel("USDINR", color=c_fx)
ax_top2.tick_params(axis='y', colors=c_fx)

ax_top3 = ax_top.twinx()
ax_top3.spines['right'].set_position(('axes', 1.1))
ax_top3.set_frame_on(True); ax_top3.patch.set_visible(False)

l3 = ax_top3.plot(res_ex.index, res_ex.values, color=c_res, label='Reserves ex-gold (RHS-2)', linewidth=1.6, linestyle='--')[0]
ax_top3.set_ylabel("Reserves ex-gold (USD)", color=c_res)
ax_top3.tick_params(axis='y', colors=c_res)

ax_top.set_title("INR 1Y Basis vs USDINR vs RBI Reserves ex-gold")
ax_top.grid(True, linestyle=':', alpha=0.5)

handles, labels = ax_top.get_legend_handles_labels()
h2, l2_ = ax_top2.get_legend_handles_labels()
h3, l3_ = ax_top3.get_legend_handles_labels()
ax_top.legend(handles + h2 + h3, labels + l2_ + l3_, loc='upper left', fontsize=9)

# ---------- Block 1: 1Y Basis vs Reserves ex-gold (inv), last 5y ----------
# Row 1: levels | 6M | 3M
ax11 = fig1.add_subplot(gs1[1, 0])
plot_pair(ax11, basis_1y_5, res_ex_inv_5,
          "Basis (1Y)", "− Reserves ex-gold (USD)",
          c_basis, c_res, "Levels (last 5y)")

ax12 = fig1.add_subplot(gs1[1, 1])
plot_pair(ax12, chg6_basis_1y_5, chg6_res_ex_inv_5,
          "6M Δ Basis (1Y)", "6M Δ (− Reserves ex-gold)",
          c_basis, c_res, "6M Changes (last 5y)")

ax13 = fig1.add_subplot(gs1[1, 2])
plot_pair(ax13, chg3_basis_1y_5, chg3_res_ex_inv_5,
          "3M Δ Basis (1Y)", "3M Δ (− Reserves ex-gold)",
          c_basis, c_res, "3M Changes (last 5y)")

# Row 2: corr(levels) | corr(6M) | corr(3M)
ax21 = fig1.add_subplot(gs1[2, 0])
plot_corr(ax21, corr_level_b1y_resinv_5, "Rolling 1Y Corr (levels, last 5y)")

ax22 = fig1.add_subplot(gs1[2, 1])
plot_corr(ax22, corr_chg6_b1y_resinv_5, "Rolling 1Y Corr (6M changes, last 5y)")

ax23 = fig1.add_subplot(gs1[2, 2])
plot_corr(ax23, corr_chg3_b1y_resinv_5, "Rolling 1Y Corr (3M changes, last 5y)")

# ---------- Block 2: 2Y Basis vs Reserves ex-gold (inv), last 5y ----------
# Row 3: levels | 6M | 3M
ax31 = fig1.add_subplot(gs1[3, 0])
plot_pair(ax31, basis_2y_5, res_ex_inv_5,
          "Basis (2Y)", "− Reserves ex-gold (USD)",
          c_basis, c_res, "Levels (last 5y)")

ax32 = fig1.add_subplot(gs1[3, 1])
plot_pair(ax32, chg6_basis_2y_5, chg6_res_ex_inv_5,
          "6M Δ Basis (2Y)", "6M Δ (− Reserves ex-gold)",
          c_basis, c_res, "6M Changes (last 5y)")

ax33 = fig1.add_subplot(gs1[3, 2])
plot_pair(ax33, chg3_basis_2y_5, chg3_res_ex_inv_5,
          "3M Δ Basis (2Y)", "3M Δ (− Reserves ex-gold)",
          c_basis, c_res, "3M Changes (last 5y)")

# Row 4: corr(levels) | corr(6M) | corr(3M)
ax41 = fig1.add_subplot(gs1[4, 0])
plot_corr(ax41, corr_level_b2y_resinv_5, "Rolling 1Y Corr (levels, last 5y)")

ax42 = fig1.add_subplot(gs1[4, 1])
plot_corr(ax42, corr_chg6_b2y_resinv_5, "Rolling 1Y Corr (6M changes, last 5y)")

ax43 = fig1.add_subplot(gs1[4, 2])
plot_corr(ax43, corr_chg3_b2y_resinv_5, "Rolling 1Y Corr (3M changes, last 5y)")

# ---------- Block 3: 1Y Basis vs USDINR, last 5y ----------
# Row 5: levels | 6M | 3M
ax51 = fig1.add_subplot(gs1[5, 0])
plot_pair(ax51, basis_1y_5, usd_5,
          "Basis (1Y)", "USDINR",
          c_basis, c_fx, "Levels (last 5y)")

ax52 = fig1.add_subplot(gs1[5, 1])
plot_pair(ax52, chg6_basis_1y_5, chg6_usd_5,
          "6M Δ Basis (1Y)", "6M Δ USDINR",
          c_basis, c_fx, "6M Changes (last 5y)")

ax53 = fig1.add_subplot(gs1[5, 2])
plot_pair(ax53, chg3_basis_1y_5, chg3_usd_5,
          "3M Δ Basis (1Y)", "3M Δ USDINR",
          c_basis, c_fx, "3M Changes (last 5y)")

# Row 6: corr(levels) | corr(6M) | corr(3M)
ax61 = fig1.add_subplot(gs1[6, 0])
plot_corr(ax61, corr_level_b1y_usd_5, "Rolling 1Y Corr (levels, last 5y)")

ax62 = fig1.add_subplot(gs1[6, 1])
plot_corr(ax62, corr_chg6_b1y_usd_5, "Rolling 1Y Corr (6M changes, last 5y)")

ax63 = fig1.add_subplot(gs1[6, 2])
plot_corr(ax63, corr_chg3_b1y_usd_5, "Rolling 1Y Corr (3M changes, last 5y)")

# X-axis formatting (apply to corr rows)
for ax in [ax21, ax22, ax23, ax41, ax42, ax43, ax61, ax62, ax63]:
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax.get_xticklabels(), rotation=45)

# Only "Last data" stamp (chart 1)
if pd.notna(last_data_date1):
    fig1.text(
        0.985, 0.985, f"Last data: {pd.to_datetime(last_data_date1).date()}",
        ha='right', va='top', fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'),
    )

fig1.suptitle("INR Basis vs USDINR & RBI Reserves ex-gold — Multi-horizon Levels, Changes, and Correlations",
              y=0.995, fontsize=15)

fig1.savefig(PNG_OUT1, dpi=150, bbox_inches='tight')
plt.close(fig1)
print(f"Saved chart to: {PNG_OUT1}")

# =========================
# Chart 2: Basis vs Banking Liquidity (and incl. VRR/VRRR) — 5D Moving Average
# Layout: 13 rows x 3 cols
#  - Row 0 (full width): big 2-axis chart (5D MA basis on LHS; 5D MA liquidity series on RHS)
#  - Rows 1-6: Banking Liquidity vs 3m/1y/2y basis (each block: levels/6M/3M + rolling corr)
#  - Rows 7-12: Banking Liq incl. VRR/VRRR vs 3m/1y/2y basis (same)
# =========================
def ma5(s: pd.Series) -> pd.Series:
    # Use full window so the 5D MA is a true 5-point mean (first 4 points become NaN)
    return s.rolling(5, min_periods=5).mean()

# --- 5D MA series (chart 2 only) ---
basis_3m_ma5 = ma5(basis_3m)
basis_1y_ma5 = ma5(basis_1y)
basis_2y_ma5 = ma5(basis_2y)

bank_liq_ma5     = ma5(bank_liq)
bank_liq_vrr_ma5 = ma5(bank_liq_vrr)

# For “Last data” stamp (chart 2)
stack_for_last2_ma5 = pd.concat([basis_3m_ma5, basis_1y_ma5, basis_2y_ma5, bank_liq_ma5, bank_liq_vrr_ma5], axis=1)
last_data_date2_ma5 = stack_for_last2_ma5.dropna(how='all').index.max()

# --- Changes on MA series (63/126 bdays) ---
chg6_basis_3m_ma5 = rolling_change(basis_3m_ma5, 126)
chg3_basis_3m_ma5 = rolling_change(basis_3m_ma5,  63)

chg6_basis_1y_ma5 = rolling_change(basis_1y_ma5, 126)
chg3_basis_1y_ma5 = rolling_change(basis_1y_ma5,  63)

chg6_basis_2y_ma5 = rolling_change(basis_2y_ma5, 126)
chg3_basis_2y_ma5 = rolling_change(basis_2y_ma5,  63)

chg6_liq_ma5      = rolling_change(bank_liq_ma5, 126)
chg3_liq_ma5      = rolling_change(bank_liq_ma5,  63)

chg6_liq_vrr_ma5  = rolling_change(bank_liq_vrr_ma5, 126)
chg3_liq_vrr_ma5  = rolling_change(bank_liq_vrr_ma5,  63)

# --- Rolling 1Y correlations on MA series ---
corr_level_liq_b3m_ma5  = corr_1y(basis_3m_ma5, bank_liq_ma5)
corr_chg6_liq_b3m_ma5   = corr_1y(chg6_basis_3m_ma5, chg6_liq_ma5)
corr_chg3_liq_b3m_ma5   = corr_1y(chg3_basis_3m_ma5, chg3_liq_ma5)

corr_level_liq_b1y_ma5  = corr_1y(basis_1y_ma5, bank_liq_ma5)
corr_chg6_liq_b1y_ma5   = corr_1y(chg6_basis_1y_ma5, chg6_liq_ma5)
corr_chg3_liq_b1y_ma5   = corr_1y(chg3_basis_1y_ma5, chg3_liq_ma5)

corr_level_liq_b2y_ma5  = corr_1y(basis_2y_ma5, bank_liq_ma5)
corr_chg6_liq_b2y_ma5   = corr_1y(chg6_basis_2y_ma5, chg6_liq_ma5)
corr_chg3_liq_b2y_ma5   = corr_1y(chg3_basis_2y_ma5, chg3_liq_ma5)

corr_level_liqv_b3m_ma5 = corr_1y(basis_3m_ma5, bank_liq_vrr_ma5)
corr_chg6_liqv_b3m_ma5  = corr_1y(chg6_basis_3m_ma5, chg6_liq_vrr_ma5)
corr_chg3_liqv_b3m_ma5  = corr_1y(chg3_basis_3m_ma5, chg3_liq_vrr_ma5)

corr_level_liqv_b1y_ma5 = corr_1y(basis_1y_ma5, bank_liq_vrr_ma5)
corr_chg6_liqv_b1y_ma5  = corr_1y(chg6_basis_1y_ma5, chg6_liq_vrr_ma5)
corr_chg3_liqv_b1y_ma5  = corr_1y(chg3_basis_1y_ma5, chg3_liq_vrr_ma5)

corr_level_liqv_b2y_ma5 = corr_1y(basis_2y_ma5, bank_liq_vrr_ma5)
corr_chg6_liqv_b2y_ma5  = corr_1y(chg6_basis_2y_ma5, chg6_liq_vrr_ma5)
corr_chg3_liqv_b2y_ma5  = corr_1y(chg3_basis_2y_ma5, chg3_liq_vrr_ma5)

# --- Last 5y slices (MA series) ---
basis_3m_ma5_5     = last5y(basis_3m_ma5)
basis_1y_ma5_5     = last5y(basis_1y_ma5)
basis_2y_ma5_5     = last5y(basis_2y_ma5)

bank_liq_ma5_5     = last5y(bank_liq_ma5)
bank_liq_vrr_ma5_5 = last5y(bank_liq_vrr_ma5)

chg6_basis_3m_ma5_5 = last5y(chg6_basis_3m_ma5)
chg3_basis_3m_ma5_5 = last5y(chg3_basis_3m_ma5)

chg6_basis_1y_ma5_5 = last5y(chg6_basis_1y_ma5)
chg3_basis_1y_ma5_5 = last5y(chg3_basis_1y_ma5)

chg6_basis_2y_ma5_5 = last5y(chg6_basis_2y_ma5)
chg3_basis_2y_ma5_5 = last5y(chg3_basis_2y_ma5)

chg6_liq_ma5_5      = last5y(chg6_liq_ma5)
chg3_liq_ma5_5      = last5y(chg3_liq_ma5)

chg6_liq_vrr_ma5_5  = last5y(chg6_liq_vrr_ma5)
chg3_liq_vrr_ma5_5  = last5y(chg3_liq_vrr_ma5)

corr_level_liq_b3m_ma5_5  = last5y(corr_level_liq_b3m_ma5)
corr_chg6_liq_b3m_ma5_5   = last5y(corr_chg6_liq_b3m_ma5)
corr_chg3_liq_b3m_ma5_5   = last5y(corr_chg3_liq_b3m_ma5)

corr_level_liq_b1y_ma5_5  = last5y(corr_level_liq_b1y_ma5)
corr_chg6_liq_b1y_ma5_5   = last5y(corr_chg6_liq_b1y_ma5)
corr_chg3_liq_b1y_ma5_5   = last5y(corr_chg3_liq_b1y_ma5)

corr_level_liq_b2y_ma5_5  = last5y(corr_level_liq_b2y_ma5)
corr_chg6_liq_b2y_ma5_5   = last5y(corr_chg6_liq_b2y_ma5)
corr_chg3_liq_b2y_ma5_5   = last5y(corr_chg3_liq_b2y_ma5)

corr_level_liqv_b3m_ma5_5 = last5y(corr_level_liqv_b3m_ma5)
corr_chg6_liqv_b3m_ma5_5  = last5y(corr_chg6_liqv_b3m_ma5)
corr_chg3_liqv_b3m_ma5_5  = last5y(corr_chg3_liqv_b3m_ma5)

corr_level_liqv_b1y_ma5_5 = last5y(corr_level_liqv_b1y_ma5)
corr_chg6_liqv_b1y_ma5_5  = last5y(corr_chg6_liqv_b1y_ma5)
corr_chg3_liqv_b1y_ma5_5  = last5y(corr_chg3_liqv_b1y_ma5)

corr_level_liqv_b2y_ma5_5 = last5y(corr_level_liqv_b2y_ma5)
corr_chg6_liqv_b2y_ma5_5  = last5y(corr_chg6_liqv_b2y_ma5)
corr_chg3_liqv_b2y_ma5_5  = last5y(corr_chg3_liqv_b2y_ma5)

# --- Build figure ---
fig2 = plt.figure(figsize=(18, 34), constrained_layout=True)
gs2 = fig2.add_gridspec(nrows=13, ncols=3, height_ratios=[2.4] + [1.3]*12)

c_b3m  = '#1f77b4'
c_b1y  = '#155a9c'
c_b2y  = '#0b2f6b'
c_liq  = '#2ca02c'
c_liqv = '#d62728'

# --- Row 0: Big chart (MA basis LHS, MA liquidity RHS) ---
ax2_top = fig2.add_subplot(gs2[0, :])

ax2_top.plot(basis_3m_ma5.index, basis_3m_ma5.values, color=c_b3m, label='INR 3M Basis (5D MA, LHS)', linewidth=1.6)
ax2_top.plot(basis_1y_ma5.index, basis_1y_ma5.values, color=c_b1y, label='INR 1Y Basis (5D MA, LHS)', linewidth=1.8)
ax2_top.plot(basis_2y_ma5.index, basis_2y_ma5.values, color=c_b2y, label='INR 2Y Basis (5D MA, LHS)', linewidth=1.8)
ax2_top.set_ylabel("Basis (5D MA)", color=c_b1y)
ax2_top.tick_params(axis='y', colors=c_b1y)
ax2_top.grid(True, linestyle=':', alpha=0.5)

ax2_top_r = ax2_top.twinx()
ax2_top_r.plot(bank_liq_ma5.index, bank_liq_ma5.values, color=c_liq, label='Banking Liquidity (5D MA, RHS)', linewidth=1.6, linestyle='--')
ax2_top_r.plot(bank_liq_vrr_ma5.index, bank_liq_vrr_ma5.values, color=c_liqv, label='Banking Liq incl. VRR/VRRR (5D MA, RHS)', linewidth=1.6, linestyle='--')
ax2_top_r.set_ylabel("Liquidity (5D MA)", color=c_liq)
ax2_top_r.tick_params(axis='y', colors=c_liq)

hL, lL = ax2_top.get_legend_handles_labels()
hR, lR = ax2_top_r.get_legend_handles_labels()
ax2_top.legend(hL + hR, lL + lR, loc='upper left', fontsize=9)
ax2_top.set_title("INR Basis (5D MA) vs Banking Liquidity (5D MA, incl. VRR/VRRR)")

# ----- Section A: Banking Liquidity vs 3M/1Y/2Y (MA series) -----
# Block A1: liq vs 3M basis (rows 1-2)
a11 = fig2.add_subplot(gs2[1, 0])
plot_pair(a11, basis_3m_ma5_5, bank_liq_ma5_5, "Basis (3M, 5D MA)", "Banking Liquidity (5D MA)", c_b3m, c_liq, "Levels (last 5y)")
a12 = fig2.add_subplot(gs2[1, 1])
plot_pair(a12, chg6_basis_3m_ma5_5, chg6_liq_ma5_5, "6M Δ Basis (3M, 5D MA)", "6M Δ Liquidity (5D MA)", c_b3m, c_liq, "6M Changes (last 5y)")
a13 = fig2.add_subplot(gs2[1, 2])
plot_pair(a13, chg3_basis_3m_ma5_5, chg3_liq_ma5_5, "3M Δ Basis (3M, 5D MA)", "3M Δ Liquidity (5D MA)", c_b3m, c_liq, "3M Changes (last 5y)")

a21 = fig2.add_subplot(gs2[2, 0])
plot_corr(a21, corr_level_liq_b3m_ma5_5, "Rolling 1Y Corr (levels, last 5y)")
a22 = fig2.add_subplot(gs2[2, 1])
plot_corr(a22, corr_chg6_liq_b3m_ma5_5, "Rolling 1Y Corr (6M changes, last 5y)")
a23 = fig2.add_subplot(gs2[2, 2])
plot_corr(a23, corr_chg3_liq_b3m_ma5_5, "Rolling 1Y Corr (3M changes, last 5y)")

# Block A2: liq vs 1Y basis (rows 3-4)
b11 = fig2.add_subplot(gs2[3, 0])
plot_pair(b11, basis_1y_ma5_5, bank_liq_ma5_5, "Basis (1Y, 5D MA)", "Banking Liquidity (5D MA)", c_b1y, c_liq, "Levels (last 5y)")
b12 = fig2.add_subplot(gs2[3, 1])
plot_pair(b12, chg6_basis_1y_ma5_5, chg6_liq_ma5_5, "6M Δ Basis (1Y, 5D MA)", "6M Δ Liquidity (5D MA)", c_b1y, c_liq, "6M Changes (last 5y)")
b13 = fig2.add_subplot(gs2[3, 2])
plot_pair(b13, chg3_basis_1y_ma5_5, chg3_liq_ma5_5, "3M Δ Basis (1Y, 5D MA)", "3M Δ Liquidity (5D MA)", c_b1y, c_liq, "3M Changes (last 5y)")

b21 = fig2.add_subplot(gs2[4, 0])
plot_corr(b21, corr_level_liq_b1y_ma5_5, "Rolling 1Y Corr (levels, last 5y)")
b22 = fig2.add_subplot(gs2[4, 1])
plot_corr(b22, corr_chg6_liq_b1y_ma5_5, "Rolling 1Y Corr (6M changes, last 5y)")
b23 = fig2.add_subplot(gs2[4, 2])
plot_corr(b23, corr_chg3_liq_b1y_ma5_5, "Rolling 1Y Corr (3M changes, last 5y)")

# Block A3: liq vs 2Y basis (rows 5-6)
c11 = fig2.add_subplot(gs2[5, 0])
plot_pair(c11, basis_2y_ma5_5, bank_liq_ma5_5, "Basis (2Y, 5D MA)", "Banking Liquidity (5D MA)", c_b2y, c_liq, "Levels (last 5y)")
c12 = fig2.add_subplot(gs2[5, 1])
plot_pair(c12, chg6_basis_2y_ma5_5, chg6_liq_ma5_5, "6M Δ Basis (2Y, 5D MA)", "6M Δ Liquidity (5D MA)", c_b2y, c_liq, "6M Changes (last 5y)")
c13 = fig2.add_subplot(gs2[5, 2])
plot_pair(c13, chg3_basis_2y_ma5_5, chg3_liq_ma5_5, "3M Δ Basis (2Y, 5D MA)", "3M Δ Liquidity (5D MA)", c_b2y, c_liq, "3M Changes (last 5y)")

c21 = fig2.add_subplot(gs2[6, 0])
plot_corr(c21, corr_level_liq_b2y_ma5_5, "Rolling 1Y Corr (levels, last 5y)")
c22 = fig2.add_subplot(gs2[6, 1])
plot_corr(c22, corr_chg6_liq_b2y_ma5_5, "Rolling 1Y Corr (6M changes, last 5y)")
c23 = fig2.add_subplot(gs2[6, 2])
plot_corr(c23, corr_chg3_liq_b2y_ma5_5, "Rolling 1Y Corr (3M changes, last 5y)")

# ----- Section B: Banking Liquidity incl. VRR/VRRR vs 3M/1Y/2Y (MA series) -----
# Block B1: liq_vrr vs 3M basis (rows 7-8)
d11 = fig2.add_subplot(gs2[7, 0])
plot_pair(d11, basis_3m_ma5_5, bank_liq_vrr_ma5_5, "Basis (3M, 5D MA)", "Liq incl. VRR/VRRR (5D MA)", c_b3m, c_liqv, "Levels (last 5y)")
d12 = fig2.add_subplot(gs2[7, 1])
plot_pair(d12, chg6_basis_3m_ma5_5, chg6_liq_vrr_ma5_5, "6M Δ Basis (3M, 5D MA)", "6M Δ Liq incl. VRR/VRRR (5D MA)", c_b3m, c_liqv, "6M Changes (last 5y)")
d13 = fig2.add_subplot(gs2[7, 2])
plot_pair(d13, chg3_basis_3m_ma5_5, chg3_liq_vrr_ma5_5, "3M Δ Basis (3M, 5D MA)", "3M Δ Liq incl. VRR/VRRR (5D MA)", c_b3m, c_liqv, "3M Changes (last 5y)")

d21 = fig2.add_subplot(gs2[8, 0])
plot_corr(d21, corr_level_liqv_b3m_ma5_5, "Rolling 1Y Corr (levels, last 5y)")
d22 = fig2.add_subplot(gs2[8, 1])
plot_corr(d22, corr_chg6_liqv_b3m_ma5_5, "Rolling 1Y Corr (6M changes, last 5y)")
d23 = fig2.add_subplot(gs2[8, 2])
plot_corr(d23, corr_chg3_liqv_b3m_ma5_5, "Rolling 1Y Corr (3M changes, last 5y)")

# Block B2: liq_vrr vs 1Y basis (rows 9-10)
e11 = fig2.add_subplot(gs2[9, 0])
plot_pair(e11, basis_1y_ma5_5, bank_liq_vrr_ma5_5, "Basis (1Y, 5D MA)", "Liq incl. VRR/VRRR (5D MA)", c_b1y, c_liqv, "Levels (last 5y)")
e12 = fig2.add_subplot(gs2[9, 1])
plot_pair(e12, chg6_basis_1y_ma5_5, chg6_liq_vrr_ma5_5, "6M Δ Basis (1Y, 5D MA)", "6M Δ Liq incl. VRR/VRRR (5D MA)", c_b1y, c_liqv, "6M Changes (last 5y)")
e13 = fig2.add_subplot(gs2[9, 2])
plot_pair(e13, chg3_basis_1y_ma5_5, chg3_liq_vrr_ma5_5, "3M Δ Basis (1Y, 5D MA)", "3M Δ Liq incl. VRR/VRRR (5D MA)", c_b1y, c_liqv, "3M Changes (last 5y)")

e21 = fig2.add_subplot(gs2[10, 0])
plot_corr(e21, corr_level_liqv_b1y_ma5_5, "Rolling 1Y Corr (levels, last 5y)")
e22 = fig2.add_subplot(gs2[10, 1])
plot_corr(e22, corr_chg6_liqv_b1y_ma5_5, "Rolling 1Y Corr (6M changes, last 5y)")
e23 = fig2.add_subplot(gs2[10, 2])
plot_corr(e23, corr_chg3_liqv_b1y_ma5_5, "Rolling 1Y Corr (3M changes, last 5y)")

# Block B3: liq_vrr vs 2Y basis (rows 11-12)
f11 = fig2.add_subplot(gs2[11, 0])
plot_pair(f11, basis_2y_ma5_5, bank_liq_vrr_ma5_5, "Basis (2Y, 5D MA)", "Liq incl. VRR/VRRR (5D MA)", c_b2y, c_liqv, "Levels (last 5y)")
f12 = fig2.add_subplot(gs2[11, 1])
plot_pair(f12, chg6_basis_2y_ma5_5, chg6_liq_vrr_ma5_5, "6M Δ Basis (2Y, 5D MA)", "6M Δ Liq incl. VRR/VRRR (5D MA)", c_b2y, c_liqv, "6M Changes (last 5y)")
f13 = fig2.add_subplot(gs2[11, 2])
plot_pair(f13, chg3_basis_2y_ma5_5, chg3_liq_vrr_ma5_5, "3M Δ Basis (2Y, 5D MA)", "3M Δ Liq incl. VRR/VRRR (5D MA)", c_b2y, c_liqv, "3M Changes (last 5y)")

f21 = fig2.add_subplot(gs2[12, 0])
plot_corr(f21, corr_level_liqv_b2y_ma5_5, "Rolling 1Y Corr (levels, last 5y)")
f22 = fig2.add_subplot(gs2[12, 1])
plot_corr(f22, corr_chg6_liqv_b2y_ma5_5, "Rolling 1Y Corr (6M changes, last 5y)")
f23 = fig2.add_subplot(gs2[12, 2])
plot_corr(f23, corr_chg3_liqv_b2y_ma5_5, "Rolling 1Y Corr (3M changes, last 5y)")

# X-axis formatting (corr rows in chart 2)
corr_axes_2 = [a21, a22, a23, b21, b22, b23, c21, c22, c23, d21, d22, d23, e21, e22, e23, f21, f22, f23]
for ax in corr_axes_2:
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax.get_xticklabels(), rotation=45)

# "Last data" stamp (chart 2)
if pd.notna(last_data_date2_ma5):
    fig2.text(
        0.985, 0.985, f"Last data: {pd.to_datetime(last_data_date2_ma5).date()}",
        ha='right', va='top', fontsize=10,
        bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25'),
    )

fig2.suptitle("INR Basis vs Banking Liquidity (5D MA) — Multi-horizon Levels, Changes, and Correlations",
              y=0.995, fontsize=15)

fig2.savefig(PNG_OUT2, dpi=150, bbox_inches='tight')
plt.close(fig2)
print(f"Saved chart to: {PNG_OUT2}")




# ### Challenger Stdev move
# START_DATE = datetime(2005, 1, 1)
# END_DATE   = datetime.today()
# FIELD      = "PX_LAST"
# ROLL_Z     = 36   # rolling window (months) for z-score
# STREAK_LEN = 2    # number of consecutive last months with positive z-score
# SAVE_PATH  = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
# OUTFILE    = SAVE_PATH / "Challenger_Industries_PosZ_Streak_Breadth.png"

# TICKERS = [
#     'CHALGOVT Index','CHALTECH Index','CHALCONS Index','CHALRETA Index','CHALAERO Index',
#     'CHALCOMP Index','CHALFINT Index','CHALAPPA Index','CHALAUTO Index','CHALCHEM Index',
#     'CHALCOMM Index','CHALCONP Index','CHALEDUT Index','CHALELEC Index','CHALENER Index',
#     'CHALENTE Index','CHALFINA Index','CHALFOOD Index','CHALHLCP Index','CHALINGO Index',
#     'CHALINSU Index','CHALLEGL Index','CHALMEDI Index','CHALMINE Index','CHALNPRF Index',
#     'CHALPHAR Index','CHALREAL Index','CHALSERV Index','CHALTELE Index','CHALTRAN Index',
#     'CHALUTIL Index','CHALWARE Index'
# ]

# # (optional) nicer labels for future extensions
# NICE = {
#     'CHALGOVT Index':'Government','CHALTECH Index':'Technology','CHALCONS Index':'Construction',
#     'CHALRETA Index':'Retail','CHALAERO Index':'Aerospace','CHALCOMP Index':'Computers',
#     'CHALFINT Index':'Finance (Total)','CHALAPPA Index':'Apparel','CHALAUTO Index':'Auto',
#     'CHALCHEM Index':'Chemicals','CHALCOMM Index':'Comms','CHALCONP Index':'Consumer Products',
#     'CHALEDUT Index':'Education','CHALELEC Index':'Electronics','CHALENER Index':'Energy',
#     'CHALENTE Index':'Entertainment','CHALFINA Index':'Financial (Banking)','CHALFOOD Index':'Food',
#     'CHALHLCP Index':'Healthcare','CHALINGO Index':'Industrial Goods','CHALINSU Index':'Insurance',
#     'CHALLEGL Index':'Legal','CHALMEDI Index':'Media','CHALMINE Index':'Mining',
#     'CHALNPRF Index':'Non-Profit','CHALPHAR Index':'Pharma','CHALREAL Index':'Real Estate',
#     'CHALSERV Index':'Services','CHALTELE Index':'Telecom','CHALTRAN Index':'Transport',
#     'CHALUTIL Index':'Utilities','CHALWARE Index':'Warehousing'
# }

# # ----------------------------
# # Helpers
# # ----------------------------
# def bdh_flat(tickers, field=FIELD, start=START_DATE, end=END_DATE):
#     df = blp.bdh(tickers=list(tickers), flds=[field], start_date=start, end_date=end)
#     if isinstance(df.columns, pd.MultiIndex):
#         # xbbg typically returns (ticker, field) or (field, ticker); keep tickers as columns
#         if field in df.columns.get_level_values(-1):
#             df = df.xs(field, level=-1, axis=1)
#         else:
#             df = df.xs(field, level=0, axis=1)
#     df.index = pd.to_datetime(df.index)
#     return df.sort_index()

# def rolling_zscore(df: pd.DataFrame, window: int) -> pd.DataFrame:
#     """
#     Rolling z-score per column over `window` periods.
#     z_t = (x_t - mean_{t-window+1..t}) / std_{t-window+1..t}
#     """
#     roll_mean = df.rolling(window=window, min_periods=window).mean()
#     roll_std  = df.rolling(window=window, min_periods=window).std(ddof=0)
#     z = (df - roll_mean) / roll_std
#     z = z.replace([np.inf, -np.inf], np.nan)
#     return z

# def count_consecutive_positive(zdf: pd.DataFrame, streak_len: int) -> pd.Series:
#     """
#     For each month t, count the number of columns where the last `streak_len`
#     monthly z-scores are strictly > 0 (i.e., positive in each of the last N months).
#     """
#     pos = (zdf > 0).astype(float)
#     # rolling sum over time per column; equals streak_len if all last N are positive
#     pos_sum = pos.rolling(window=streak_len, min_periods=streak_len).sum()
#     cond = (pos_sum == streak_len)
#     # Count across columns (industries) per month
#     return cond.sum(axis=1).rename(f'Industries with last {streak_len}m z>0')

# # ----------------------------
# # Fetch & prepare
# # ----------------------------
# raw = bdh_flat(TICKERS, FIELD, START_DATE, END_DATE)
# raw['CHALWARE Index'][-1] = 47878
# raw['CHALTECH Index'][-1] = 33281

# # Enforce month frequency; Challenger data are monthly but we normalize
# m = raw.resample('M').last()

# # Fill ALL NAs with 0 (per your preference)
# m_filled = m.fillna(0)

# # Month-over-month change (z-score will be on MoM changes)
# mom = m_filled.diff()

# # Rolling z-score on MoM changes
# z_mom = rolling_zscore(mom, window=ROLL_Z)

# # Count industries where last STREAK_LEN months all have positive z-scores
# breadth_pos_streak = count_consecutive_positive(z_mom, streak_len=STREAK_LEN).dropna()

# # ----------------------------
# # Plot
# # ----------------------------
# fig, ax = plt.subplots(figsize=(14, 6))
# ax.plot(breadth_pos_streak.index, breadth_pos_streak.values, lw=2.2, color='#2a9d8f',
#         label=f'Count of industries with last {STREAK_LEN} months z>0 (rolling {ROLL_Z}m z)')

# ax.set_title(f"Challenger Job Cuts — Breadth of Positive z-score Streaks\n"
#              f"(z on MoM changes; window={ROLL_Z}m; streak={STREAK_LEN}m)", fontsize=13)
# ax.set_ylabel("# of industries")
# ax.set_xlabel("Date")
# ax.set_ylim(0, len(TICKERS))
# ax.axhline(0, color='grey', lw=0.8)
# ax.grid(True, linestyle=':', alpha=0.6)
# ax.legend(loc='upper left', fontsize=9)

# ax.xaxis.set_major_locator(mdates.YearLocator(base=2))
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
# plt.setp(ax.get_xticklabels(), rotation=45)

# # Last data box
# last_dt = m_filled.index.max()
# fig.text(0.985, 0.985, f"Last data: {last_dt.date()}",
#          ha='right', va='top', fontsize=9,
#          bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

# plt.tight_layout()
# plt.savefig(OUTFILE, bbox_inches='tight')

# ##################
# OUTFILE2 = SAVE_PATH / "Challenger_Industries_StdDev_and_Exceedances2.png"

# # Monthly (these are monthly series already, but we enforce a clean monthly index)
# m = raw.resample('M').last()

# # Fill ALL NAs with zero as requested
# m_filled = m.fillna(0)

# # Month-over-month change (absolute difference of levels)
# mom = m_filled.diff()

# # Stdev per industry (over full sample)
# stdev = mom.std(skipna=True).rename('stdev')

# # --- Breadth buckets with ±0.5σ thresholds ---
# half_sigma = 0.5 * stdev

# gt_pos = mom.gt(half_sigma, axis='columns')                 # > +0.5σ
# lt_neg = mom.lt(-half_sigma, axis='columns')                # < -0.5σ
# mid    = mom.ge(-half_sigma, axis='columns') & mom.le(half_sigma, axis='columns')  # between −0.5σ and +0.5σ

# count_pos = gt_pos.sum(axis=1).rename('> +0.5σ').dropna()
# count_mid = mid.sum(axis=1).rename('−0.5σ … +0.5σ').dropna()
# count_neg = lt_neg.sum(axis=1).rename('< −0.5σ').dropna()

# # For each month: count industries where |Δ| > stdev[industry]
# # Build boolean mask with broadcasting
# # (abs(mom) and stdev aligned by columns)
# abs_mom = mom.abs()
# # replicate stdev into rows (pandas will align by columns automatically)
# exceed_mask = abs_mom.gt(stdev, axis='columns')
# exceed_count = exceed_mask.sum(axis=1).rename('Industries |Δ| > 1σ')

# # Drop initial NaN from diff (first month)
# exceed_count = exceed_count.dropna()

# def labelize(cols):
#     return [nice.get(c, c) for c in cols]
# # ----------------------------
# # Plot
# # ----------------------------
# fig, (axL, axR) = plt.subplots(1, 2, figsize=(18, 8))
# fig.suptitle("US Challenger Job Cuts — Industry Volatility & Broad Surprises", fontsize=16, y=0.98)

# # Left: stdev bar chart (sorted)
# stdev_sorted = stdev.sort_values(ascending=False)
# axL.barh(labelize(stdev_sorted.index.tolist())[::-1], stdev_sorted.values[::-1], color='#4C78A8')
# axL.set_title("Stdev of MoM Changes (2005–present)", fontsize=12)
# axL.set_xlabel("Jobs (MoM stdev)")
# axL.grid(True, axis='x', linestyle=':', alpha=0.6)
# # tidy y labels
# axL.tick_params(axis='y', labelsize=8)

# axR.plot(count_pos.index, count_pos.values, lw=2.0, color='#1f77b4', label='Count > +0.5σ')
# axR.plot(count_mid.index, count_mid.values, lw=2.0, color='#2ca02c', label='Count −0.5σ…+0.5σ')
# axR.plot(count_neg.index, count_neg.values, lw=2.0, color='#d62728', label='Count < −0.5σ')

# axR.set_title("Breadth of MoM Moves by σ-bucket (±0.5σ, monthly)", fontsize=12)
# axR.set_ylabel("# of industries")
# axR.set_xlabel("Date")
# axR.set_ylim(0, len(tickers))
# axR.axhline(0, color='grey', lw=0.8)
# axR.grid(True, linestyle=':', alpha=0.6)
# axR.legend(loc='upper left', fontsize=9)
# axR.xaxis.set_major_locator(mdates.YearLocator(base=2))
# axR.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
# plt.setp(axR.get_xticklabels(), rotation=45)
# # Last data box
# last_dt = m_filled.index.max()
# fig.text(0.985, 0.985, f"Last data: {last_dt.date()}",
#          ha='right', va='top', fontsize=9,
#          bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

# plt.tight_layout(rect=[0, 0, 1, 0.95])
# plt.savefig(OUTFILE2, bbox_inches='tight')

# # ----------------------------
# # Cleanup
# # ----------------------------
# #del raw, m, m_filled, mom, stdev, stdev_sorted, abs_mom, exceed_mask, exceed_count, last_dt, fig, axL, axR



# # =========================
# # Params
# # =========================
# START = pd.Timestamp("2019-01-01")
# END = pd.Timestamp.today().normalize()
# WINDOW = 66  # ~3 months (BDays)
# FIELD = "PX_LAST"

# # Tickers
# INDIA = ["JGS1 Index", "NSEIT Index"]  # India broad + NIFTY IT
# SPX = "ES1 Index"
# CONTROL = "UX1 Index"  # S&P 500 Equal Weight
# COMPARATORS = [
#     SPX,
#     "GSTMTAIP Index",  # GS AI basket
#     "KM1 Index",       # KOSPI
#     "TWSE Index",      # Taiwan Weighted
#     "HI1 Index",       # China equities (per user spec)
# ]
# ALL_TKS = sorted(set(INDIA + COMPARATORS + [SPX, CONTROL]))

# # Output dir
# G_CHART_DIR = Path(os.getenv("G_CHART_DIR", "."))
# G_CHART_DIR.mkdir(parents=True, exist_ok=True)
# PNG_OUT = G_CHART_DIR / "India_AntiAI_Rolling_PartialCorr.png"

# # =========================
# # Helpers
# # =========================

# def flatten_xbbg(raw: pd.DataFrame, field: str) -> pd.DataFrame:
#     """Extract a single-field view from xbbg-like MultiIndex columns."""
#     if not isinstance(raw.columns, pd.MultiIndex):
#         return raw.copy()
#     for lvl in range(raw.columns.nlevels - 1, -1, -1):
#         if field in raw.columns.get_level_values(lvl):
#             out = raw.xs(field, axis=1, level=lvl, drop_level=False)
#             if isinstance(out.columns, pd.MultiIndex):
#                 out.columns = out.columns.droplevel(lvl)
#             return out
#     return pd.DataFrame(index=raw.index)


# def load_history(tickers, start: pd.Timestamp, end: pd.Timestamp, field: str) -> pd.DataFrame:
#     if blp is None:
#         raise RuntimeError("xbbg/blp is not available in this environment.")
#     raw = blp.bdh(tickers=tickers, flds=field, start_date=start, end_date=end, Per="D")
#     df = flatten_xbbg(raw, field)
#     df.index = pd.to_datetime(df.index, utc=False).tz_localize(None)
#     df = df.sort_index()
#     df = df.apply(pd.to_numeric, errors="coerce")
#     return df


# def log_returns(df: pd.DataFrame) -> pd.DataFrame:
#     """Compute daily log returns; non-positive levels become NaN before log."""
#     safe = df.where(df > 0)
#     return np.log(safe).diff()


# def rolling_corr(a: pd.Series, b: pd.Series, window: int) -> pd.Series:
#     if a is None or b is None:
#         return pd.Series(dtype=float)
#     return a.rolling(window, min_periods=max(15, window // 3)).corr(b)


# def rolling_partial_corr(x: pd.Series, y: pd.Series, z: pd.Series, window: int) -> pd.Series:
#     """3-variable partial correlation rho_xy.z via the pairwise-ρ formula per window.
#     ρ_xy·z = (ρ_xy - ρ_xz ρ_yz) / sqrt((1-ρ_xz^2)(1-ρ_yz^2))
#     Will return NaN when denominator ~ 0 (e.g., y == z == control var).
#     """
#     r_xy = rolling_corr(x, y, window)
#     r_xz = rolling_corr(x, z, window)
#     r_yz = rolling_corr(y, z, window)

#     num = r_xy - (r_xz * r_yz)
#     denom = np.sqrt((1 - r_xz**2) * (1 - r_yz**2))
#     out = num / denom
#     out[(denom <= 1e-12) | (~np.isfinite(denom))] = np.nan
#     return out


# def plot_corr_axis(ax: plt.Axes, s: pd.Series, title: str):
#     if s is None or s.dropna().empty:
#         ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
#         ax.set_title(title)
#         ax.grid(False)
#         ax.set_xticks([])
#         ax.set_yticks([])
#         return
#     ax.plot(s.index, s.values, linewidth=1.6)
#     ax.axhline(0, linewidth=1)
#     ax.set_ylim(-1.05, 1.05)
#     ax.set_title(title)
#     ax.grid(True, linestyle=":", alpha=0.5)
#     ax.xaxis.set_major_locator(mdates.YearLocator())
#     ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

# # -----------------
# # Fetch & prep
# # -----------------
# df = load_history(ALL_TKS, START, END, FIELD)
# rets = log_returns(df)

# # Last data stamp uses all series
# last_data_date = df.dropna(how="all").index.max()

# # -----------------
# # Compute correlations
# # -----------------
# jgs1 = rets.get("JGS1 Index")
# nseit = rets.get("NSEIT Index")
# ctrl = rets.get(CONTROL)

# # Pre-compute all pairwise corr & partials row-wise
# results: dict[tuple[str, str], pd.Series] = {}
# for comp in COMPARATORS:
#     y = rets.get(comp)
#     # Standard rolling 66D correlations
#     results[(comp, "corr_jgs1") ] = rolling_corr(jgs1, y, WINDOW)
#     results[(comp, "corr_nseit")] = rolling_corr(nseit, y, WINDOW)
#     # Partial corr vs SPX
#     results[(comp, "pcorr_jgs1")] = rolling_partial_corr(jgs1, y, ctrl, WINDOW)
#     results[(comp, "pcorr_nseit")] = rolling_partial_corr(nseit, y, ctrl, WINDOW)

# # -----------------
# # Plot (rows = comparators, cols = 4)
# # -----------------
# nrows = len(COMPARATORS)
# fig, axes = plt.subplots(nrows=nrows, ncols=4, figsize=(20, 2.2 * nrows + 2), sharex=False)

# # Friendly labels
# friendly = {
#     "JGS1 Index": "India (JGS1)",
#     "NSEIT Index": "NIFTY IT",
#     "ES1 Index": "S&P 500",
#     "UX1 Index": "VIX Index",
#     "GSTMTAIP Index": "GS AI Basket",
#     "KM1 Index": "KOSPI",
#     "TWSE Index": "TWSE",
#     "HI1 Index": "China Eq (HI1)",
# }

# # Column headers
# col_titles = [
#     "66D Corr: JGS1 vs {idx}",
#     "66D Corr: NSEIT vs {idx}",
#     "Partial Corr | VIX: JGS1 vs {idx}",
#     "Partial Corr | VIX: NSEIT vs {idx}",
# ]

# for r, comp in enumerate(COMPARATORS):
#     idx_label = friendly.get(comp, comp)
#     # Left 2 columns: simple rolling corr
#     plot_corr_axis(axes[r, 0], results[(comp, "corr_jgs1")], col_titles[0].format(idx=idx_label))
#     plot_corr_axis(axes[r, 1], results[(comp, "corr_nseit")], col_titles[1].format(idx=idx_label))
#     # Right 2 columns: partial corr wrt SPW
#     plot_corr_axis(axes[r, 2], results[(comp, "pcorr_jgs1")], col_titles[2].format(idx=idx_label))
#     plot_corr_axis(axes[r, 3], results[(comp, "pcorr_nseit")], col_titles[3].format(idx=idx_label))

# # Overall title & last-data stamp
# fig.suptitle(
#     "India 'Anti-AI' Trade — Rolling 3M Correlations & SPW-Partial Correlations\n"
#     "(Daily log returns; window = 66 business days)",
#     y=0.995, fontsize=14
# )

# if pd.notna(last_data_date):
#     fig.text(
#         0.99, 0.01, f"Last data: {pd.to_datetime(last_data_date).date()}",
#         ha="right", va="bottom", fontsize=9,
#         bbox=dict(facecolor="white", edgecolor="black", boxstyle="round,pad=0.25"),
#     )

# fig.tight_layout(rect=[0, 0.03, 1, 0.97])
# plt.savefig(PNG_OUT, dpi=150, bbox_inches="tight")
# print(f"Saved chart to: {PNG_OUT}")

# -----------------------------
# Stat-Xplore Open Data API
# -----------------------------
BASE_URL = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1"

DB_ID = "str:database:UC_Monthly"  # People on Universal Credit

MEASURE_ID = "str:count:UC_Monthly:V_F_UC_CASELOAD_FULL"

# IMPORTANT: Month/time comes from the date dimension table, not V_F_UC_CASELOAD_FULL
MONTH_FIELD_ID = "str:field:UC_Monthly:F_UC_DATE:DATE_NAME"

COND_FIELD_ID = "str:field:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME"

# From your console output:
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

def request_json(method: str, url: str, *, params=None, json_body=None, timeout=180, max_attempts=8):
    """HTTP with retry/backoff for occasional Stat-Xplore 503s."""
    for attempt in range(1, max_attempts + 1):
        resp = session.request(method, url, params=params, json=json_body, timeout=timeout)

        if resp.status_code in RETRIABLE:
            ra = resp.headers.get("Retry-After")
            if ra and ra.isdigit():
                delay = int(ra)
            else:
                delay = min(60, 2 ** (attempt - 1)) + random.uniform(0, 0.5)

            if attempt == max_attempts:
                raise requests.HTTPError(
                    f"{resp.status_code} from {url}\n{resp.text[:2000]}",
                    response=resp
                )
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError("Unreachable")


def find_cube_values(obj):
    """Find the first flat list of numeric-like values inside the cube."""
    def looks_like_values_list(x):
        if not isinstance(x, list) or any(isinstance(v, (list, dict)) for v in x):
            return False
        for v in x:
            if v is None:
                continue
            if isinstance(v, (int, float)):
                continue
            if isinstance(v, str):
                # allow numeric strings and some common non-numeric markers
                try:
                    float(v)
                except ValueError:
                    return False
            else:
                return False
        return True

    if looks_like_values_list(obj):
        return obj

    if isinstance(obj, dict):
        for k in ("values", "value", "cells", "data"):
            if k in obj:
                hit = find_cube_values(obj[k])
                if hit is not None:
                    return hit
        for v in obj.values():
            hit = find_cube_values(v)
            if hit is not None:
                return hit

    if isinstance(obj, list):
        for v in obj:
            hit = find_cube_values(v)
            if hit is not None:
                return hit

    return None


def get_first_cube(resp_json: dict):
    cubes = resp_json.get("cubes")
    if cubes is None:
        raise RuntimeError(f"No 'cubes' in response. Full response keys: {list(resp_json.keys())}")

    if isinstance(cubes, list):
        if not cubes:
            raise RuntimeError("Response 'cubes' is an empty list.")
        return cubes[0]

    if isinstance(cubes, dict):
        # sometimes APIs return dict keyed by measure
        first = next(iter(cubes.values()), None)
        if first is None:
            raise RuntimeError("Response 'cubes' is an empty dict.")
        return first

    raise RuntimeError(f"Unexpected 'cubes' type: {type(cubes)}")


import itertools
import pandas as pd

def _flatten_listlike(x):
    """Recursively flatten nested Python lists/tuples into a single flat list."""
    out = []
    def rec(v):
        if isinstance(v, (list, tuple)):
            for w in v:
                rec(w)
        else:
            out.append(v)
    rec(x)
    return out

def _extract_cube_values_flat(resp_json: dict, measure_idx: int = 0):
    """
    Stat-Xplore 'cubes' often looks like:
      cubes: [ { "<measure-uri>": [[...values...]] } ]
    or other small variations. This returns a flat list of cell values.
    """
    cubes = resp_json.get("cubes")

    if cubes is None:
        raise RuntimeError(f"No 'cubes' in response. Keys: {list(resp_json.keys())}")

    # cubes can be list or dict depending on parser/endpoint variants
    if isinstance(cubes, list):
        cube = cubes[measure_idx]
    elif isinstance(cubes, dict):
        cube = list(cubes.values())[measure_idx]
    else:
        raise RuntimeError(f"Unexpected 'cubes' type: {type(cubes)}")

    # cube item is often a dict keyed by measure uri
    if isinstance(cube, dict):
        if len(cube) == 1:
            cube_payload = next(iter(cube.values()))
        elif "values" in cube:
            cube_payload = cube["values"]
        else:
            # fallback: take first value
            cube_payload = next(iter(cube.values()))
    else:
        cube_payload = cube

    # flatten whatever nesting exists (R does unlist())
    return _flatten_listlike(cube_payload)

def _expand_grid_like_tidyr(lists):
    """
    Mimic tidyr::expand_grid ordering used by statxplorer:
    first field varies fastest.
    """
    rev = lists[::-1]
    for prod in itertools.product(*rev):
        yield prod[::-1]

def table_to_df(resp_json: dict) -> pd.DataFrame:
    fields = resp_json["fields"]
    field_labels = [f["label"] for f in fields]

    items_labels = []
    for f in fields:
        labels = []
        for it in f["items"]:
            lbl = it.get("labels")
            if isinstance(lbl, list):
                labels.append(" / ".join(map(str, lbl)))
            else:
                labels.append(str(lbl))
        items_labels.append(labels)

    combos = list(_expand_grid_like_tidyr(items_labels))
    values = _extract_cube_values_flat(resp_json, measure_idx=0)

    if len(values) != len(combos):
        # useful debugging if it ever happens again
        raise RuntimeError(
            f"Mismatch: {len(values)} values vs {len(combos)} combos. "
            f"Field sizes = {[len(x) for x in items_labels]}. "
            f"First cube type = {type(resp_json.get('cubes'))}."
        )

    df = pd.DataFrame(combos, columns=field_labels)
    measure_label = resp_json["measures"][0]["label"]
    df[measure_label] = pd.to_numeric(values, errors="coerce")
    return df



def parse_month(s: str):
    s = str(s).strip()
    # Typical Stat-Xplore month labels are like "May 2013"
    for fmt in ("%b %Y", "%B %Y", "%Y-%m", "%b-%Y"):
        try:
            return pd.to_datetime(datetime.strptime(s, fmt).date())
        except ValueError:
            pass
    return pd.to_datetime(s, errors="coerce")

def _field_label_by_id(resp_json: dict, field_id: str, fallback_idx: int) -> str:
    """
    Best-effort lookup of a field label by id/uri/name.
    Falls back to the field at fallback_idx if no match found.
    """
    for f in resp_json.get("fields", []):
        if f.get("id") == field_id or f.get("uri") == field_id or f.get("name") == field_id:
            return f.get("label", resp_json["fields"][fallback_idx]["label"])
    return resp_json["fields"][fallback_idx]["label"]

def _normalize_monthly_series(s: pd.Series, name: str) -> pd.Series:
    if s is None or s.empty:
        raise RuntimeError(f"{name} series is empty after pull.")
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    s = s[~s.index.duplicated(keep="last")]
    s.index = s.index.to_period("M").to_timestamp("M")
    return s

# Build query:
# Month on one axis, Conditionality on the other, but recoded to ONLY "No work requirements".
query = {
    "database": DB_ID,
    "measures": [MEASURE_ID],
    "recodes": {
        COND_FIELD_ID: {
            "map": [[NO_WORK_VALUE_ID]],
            "total": False
        }
    },
    "dimensions": [
        [MONTH_FIELD_ID],
        [COND_FIELD_ID]
    ]
}

resp = request_json("POST", f"{BASE_URL}/table", json_body=query)
df = table_to_df(resp)

month_col = _field_label_by_id(resp, MONTH_FIELD_ID, fallback_idx=0)
cond_col = _field_label_by_id(resp, COND_FIELD_ID, fallback_idx=1)
measure_label = resp["measures"][0]["label"]

# Keep only the single conditionality value (if multiple returned); drop totals; parse month
if cond_col in df.columns and df[cond_col].nunique(dropna=True) > 1:
    df = df[df[cond_col].str.lower().str.contains("no work", na=False)].copy()
df = df[df[month_col].str.lower() != "total"].copy()
df["Month"] = df[month_col].map(parse_month)
df = df.dropna(subset=["Month"]).sort_values("Month")

# --- Build the DWP series (from your df) ---
dwp_series = pd.Series(df[measure_label].values, index=pd.to_datetime(df["Month"]))
dwp_series.name = "Universal Claimants: No work requirements"
dwp_series = _normalize_monthly_series(dwp_series, "DWP")
if dwp_series.dropna().empty:
    raise RuntimeError("DWP series contains no valid data after cleaning.")
last_dwp = dwp_series.dropna().index.max()

# --- Pull Bloomberg series ---
START_DATE = dwp_series.index.min().to_pydatetime()
END_DATE = datetime.today()

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

# --- Plot ---
PLOT_START = datetime(2015, 1, 1)

fig, ax = plt.subplots(figsize=(12, 6))

# Left axis: DWP
mask_dwp = dwp_series.index >= PLOT_START
ax.plot(
    dwp_series.index[mask_dwp],
    dwp_series[mask_dwp],
    color="#1f77b4",
    linewidth=2,
    label="Universal Claimants: No work requirements"
)
ax.set_xlabel("Date", fontsize=12)
ax.set_ylabel("Universal Claimants", color="#1f77b4", fontsize=12)
ax.tick_params(axis="y", labelcolor="#1f77b4")

ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.xticks(rotation=45)

# Right axis: Bloomberg
ax2 = ax.twinx()
mask_bbg = bbg_series.index >= PLOT_START
ax2.plot(
    bbg_series.index[mask_bbg],
    bbg_series[mask_bbg],
    color="#ff7f0e",
    linewidth=2,
    alpha=0.9,
    label="UK Inactive Long-Term Sick"
)
ax2.set_ylabel("UK Inactive Long-Term Sick", color="#ff7f0e", fontsize=12)
ax2.tick_params(axis="y", labelcolor="#ff7f0e")

# X limits
ax.set_xlim(PLOT_START, max(last_dwp, last_bbg))

ax.set_title(
    "Universal Claimants (No work requirements) vs UK Inactive Long-Term Sick",
    fontsize=14
)

# Combined legend
lines = ax.get_lines() + ax2.get_lines()
labels = [l.get_label() for l in lines]
ax.legend(lines, labels, loc="upper left", fontsize=10)

# Last data dates box
plt.text(
    0.95, 0.95,
    f"Last DWP data: {last_dwp.strftime('%Y-%m-%d')}\n"
    f"Last BBG data: {last_bbg.strftime('%Y-%m-%d')}",
    horizontalalignment="right",
    verticalalignment="top",
    transform=ax.transAxes,
    fontsize=10,
    bbox=dict(facecolor="white", alpha=0.7)
)

plt.tight_layout()
SAVE_PATH = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
OUTFILE   = SAVE_PATH / "UniversalClaimants_NoWorkReq_vs_UKInactive_LongTermSick.png"
# plt.savefig(Path(G_CHART_DIR, "UniversalClaimants_NoWorkReq_vs_UKInactive_LongTermSick.png"),
#             bbox_inches="tight")
plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
print(f"Saved chart to: {OUTFILE}")
del bbg, bbg_series, dwp_series, ax, ax2, fig


#### Indonesia Money Aggregates (MONTHLY): M1 YoY, M2 YoY, Base Money YoY (manual)
TICKERS = {
    'IDM1PYOY Index': 'M1 YoY',
    'IDM2YOY Index':  'M2 YoY',
}

START_DATE = datetime(2019, 1, 1)  # Need data from 2019 to compute YoY for 2020
END_DATE   = datetime.today()
PLOT_START = datetime(2020, 1, 1)  # Only plot from 2020 onwards

SAVE_PATH = Path(G_CHART_DIR) if 'G_CHART_DIR' in globals() else Path.cwd()
OUTFILE   = SAVE_PATH / "Indonesia_MoneySupply_YoY.png"

# --- Pull MONTHLY data from Bloomberg ---
try:
    raw = blp.bdh(
        tickers=list(TICKERS.keys()),
        flds='PX_LAST',
        start_date=START_DATE,
        end_date=END_DATE,
        Per='M',          # xbbg-style monthly periodicity
        Fill='P',         # fill with previous where needed (optional)
    )
except TypeError:
    raw = blp.bdh(
        tickers=list(TICKERS.keys()),
        flds='PX_LAST',
        start_date=START_DATE,
        end_date=END_DATE,
        periodicitySelection='MONTHLY'  # alternative API style
    )
m0_level = get_adjusted_m0_data()
m0_level = _normalize_monthly_series(m0_level, "BI Adjusted M0")

# Handle Bloomberg’s MultiIndex vs. single index return shape
if isinstance(raw.columns, pd.MultiIndex):
    if "PX_LAST" in raw.columns.get_level_values(-1):
        df = raw.xs("PX_LAST", level=-1, axis=1)
    else:
        df = raw.xs("PX_LAST", level=0, axis=1)
else:
    df = raw.copy()

# Clean/rename
df.index = pd.to_datetime(df.index)
df = df.apply(pd.to_numeric, errors='coerce')
df = df.rename(columns=TICKERS)

# Normalize index to month-end timestamps (helps if Bloomberg returns mixed month dates)
df.index = df.index.to_period('M').to_timestamp('M')
df = df.sort_index()


base_yoy = compute_yoy_growth(m0_level)
base_yoy = _normalize_monthly_series(base_yoy, "BI Adjusted M0 YoY")

# Plot dataframe (all YoY %)
if "M1 YoY" not in df.columns or "M2 YoY" not in df.columns:
    raise RuntimeError(f"Missing expected Bloomberg series in Indonesia data: {list(df.columns)}")
full_idx = df.index.union(base_yoy.index).sort_values()
plot_df = pd.DataFrame(index=full_idx)
plot_df['M1 YoY']         = df['M1 YoY'].reindex(full_idx)
plot_df['M2 YoY']         = df['M2 YoY'].reindex(full_idx)
plot_df['Base Money YoY'] = base_yoy.reindex(full_idx)

# Filter to only show data from 2020 onwards
plot_df = plot_df[plot_df.index >= PLOT_START]

# Last date with any data
last_data_date = plot_df.dropna(how='all').index.max()
fig, ax = plt.subplots(1, 1, figsize=(14, 6))

# --- Plot lines and keep handles (so we can re-use the same colors for labels) ---
l1, = ax.plot(plot_df.index, plot_df['M1 YoY'].values, lw=2.0, label='M1 YoY (Index)')
l2, = ax.plot(plot_df.index, plot_df['M2 YoY'].values, lw=2.0, label='M2 YoY (Index)')
l3, = ax.plot(plot_df.index, plot_df['Base Money YoY'].values, lw=2.0, label='Base Money YoY')

ax.axhline(0, lw=1, zorder=0)
ax.grid(True, linestyle=':', alpha=0.5)
ax.set_title("Indonesia Money Growth (YoY %, Monthly)", fontsize=15)
ax.set_ylabel("YoY (%)")
ax.set_xlabel("Date")
ax.legend(loc='upper left', fontsize=9)

# --- Last value markers/labels: color-coded + staggered so they don't overlap ---
series_info = [
    ("M1 YoY", l1.get_color()),
    ("M2 YoY", l2.get_color()),
    ("Base Money YoY", l3.get_color()),
]

last_pts = []
for col, color in series_info:
    s = plot_df[col].dropna()
    if not s.empty:
        d = s.index.max()
        v = float(s.loc[d])
        last_pts.append((col, d, v, color))

# Stagger labels by ranking values (prevents overlap when values are close)
# For 3 series this gives offsets like [-14, 0, +14] points.
last_pts_sorted = sorted(last_pts, key=lambda x: x[2])  # sort by value
n = len(last_pts_sorted)
spacing = 14  # points between labels
mid = (n - 1) / 2.0

y_offsets = {}
for i, (col, d, v, color) in enumerate(last_pts_sorted):
    y_offsets[col] = int((i - mid) * spacing)

for col, d, v, color in last_pts:
    ax.scatter([d], [v], color=color, edgecolor='white', linewidth=0.8, zorder=6)
    ax.annotate(
        f"{v:+.1f}%",
        xy=(d, v),
        xytext=(10, y_offsets[col]),     # x offset + staggered y offsets
        textcoords='offset points',
        ha='left', va='center',
        color=color,
        bbox=dict(facecolor='white', edgecolor=color, alpha=0.85, boxstyle='round,pad=0.2'),
        zorder=7
    )

# X formatting
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
plt.setp(ax.get_xticklabels(), rotation=45)

# Figure annotation
fig.text(
    0.98, 0.96, f"Last data: {pd.to_datetime(last_data_date).date()}",
    ha='right', va='top', fontsize=10,
    bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.25')
)

plt.tight_layout()
plt.savefig(OUTFILE, dpi=150, bbox_inches='tight')
print(f"Saved chart to: {OUTFILE}")

del raw, df, plot_df, base_yoy, last_data_date, fig, ax



# from datetime import datetime, timedelta
# from pathlib import Path

# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# from matplotlib.patches import Rectangle
# from matplotlib.lines import Line2D

# # ------------------------------------------------------------
# # Settings
# # ------------------------------------------------------------
# TICKERS = {
#     "USDVND BGN Curncy": "USDVND (BGN)",
#     "VND T130 Curncy":   "VND T130",
# }
# FIELDS_OHLC = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"]

# END_DATE   = datetime.today()
# START_DATE = END_DATE - timedelta(days=120)   # pull extra history to ensure we can get 40 valid overlapping days
# N_DAYS     = 40  # last 40 overlapping business days

# SAVE_PATH = Path(G_CHART_DIR) if "G_CHART_DIR" in globals() else Path.cwd()
# OUTFILE   = SAVE_PATH / "USDVND_BGN_OHLC_with_T130_last40.png"

# # ------------------------------------------------------------
# # Bloomberg pull
# # ------------------------------------------------------------
# raw = blp.bdh(
#     tickers=list(TICKERS.keys()),
#     flds=FIELDS_OHLC,               # will pull OHLC for both; we'll use USDVND's OHLC and T130's PX_LAST
#     start_date=START_DATE,
#     end_date=END_DATE,
# )

# # Normalize MultiIndex to (ticker, field) if needed
# if isinstance(raw.columns, pd.MultiIndex):
#     lvl0 = set(raw.columns.get_level_values(0))
#     lvl1 = set(raw.columns.get_level_values(1))
#     tickers_set = set(TICKERS.keys())

#     # If tickers look like they're on level 1, swap
#     if len(tickers_set & lvl0) < len(tickers_set & lvl1):
#         raw.columns = raw.columns.swaplevel(0, 1)

#     raw.columns = raw.columns.set_names(["ticker", "field"])

# df = raw.copy()
# df.index = pd.to_datetime(df.index)
# df = df.apply(pd.to_numeric, errors="coerce")

# # ------------------------------------------------------------
# # Build aligned dataset (USDVND OHLC + T130 point)
# # ------------------------------------------------------------
# usd_ticker  = "USDVND BGN Curncy"
# t130_ticker = "VND T130 Curncy"

# usd_ohlc = df[usd_ticker][FIELDS_OHLC].rename(columns={
#     "PX_OPEN": "Open",
#     "PX_HIGH": "High",
#     "PX_LOW":  "Low",
#     "PX_LAST": "Close",
# })

# t130 = df[t130_ticker]["PX_LAST"].rename("T130")

# # Require overlapping days (USD OHLC complete + T130 present)
# mask = usd_ohlc.notna().all(axis=1) & t130.notna()
# plot_df = pd.concat([usd_ohlc, t130], axis=1).loc[mask].tail(N_DAYS)

# if plot_df.empty or len(plot_df) < 5:
#     raise ValueError("Not enough overlapping USDVND OHLC and T130 data to plot (check tickers/fields).")

# # ------------------------------------------------------------
# # Plot: candlestick (box+whisker) with T130 as X
# # ------------------------------------------------------------
# x = np.arange(len(plot_df))
# w = 0.60  # candle body width

# fig, ax = plt.subplots(figsize=(14, 6))

# for i, (_, r) in enumerate(plot_df.iterrows()):
#     o, h, l, c, t = r["Open"], r["High"], r["Low"], r["Close"], r["T130"]

#     # Whisker (Low to High)
#     ax.plot([i, i], [l, h], linewidth=1)

#     # Box (Open to Close)
#     bottom = min(o, c)
#     height = abs(c - o)
#     # ensure visible even if o==c
#     if height == 0:
#         height = (h - l) * 0.001 if (h - l) > 0 else 0.1

#     rect = Rectangle(
#         (i - w/2, bottom),
#         w,
#         height,
#         alpha=0.35,
#         edgecolor="black",
#         facecolor=("green" if c >= o else "red"),
#         linewidth=1,
#     )
#     ax.add_patch(rect)

#     # T130 marker
#     ax.scatter(i, t, marker="x", s=55, linewidths=2, zorder=5)

# # X-axis labels (dates)
# date_labels = [d.strftime("%Y-%m-%d") for d in plot_df.index]
# tick_step = max(1, len(plot_df)//8)  # ~8 ticks across
# ax.set_xticks(x[::tick_step])
# ax.set_xticklabels(date_labels[::tick_step], rotation=45, ha="right")

# ax.set_title(f"USDVND (BGN) OHLC (candlestick) with VND T130 marked (X) — last {len(plot_df)} business days")
# ax.set_ylabel("USDVND")
# ax.grid(True, linestyle=":", alpha=0.5)

# # Legend (custom handles)
# legend_items = [
#     Rectangle((0,0), 1, 1, facecolor="green", alpha=0.35, edgecolor="black", label="Up day (Close ≥ Open)"),
#     Rectangle((0,0), 1, 1, facecolor="red",   alpha=0.35, edgecolor="black", label="Down day (Close < Open)"),
#     Line2D([0], [0], marker="x", linestyle="None", markersize=8, markeredgewidth=2, label="VND T130"),
# ]
# ax.legend(handles=legend_items, loc="upper left", fontsize=9)

# plt.tight_layout()
# plt.savefig(OUTFILE, dpi=150, bbox_inches="tight")
# print(f"Saved chart to: {OUTFILE}")

# from datetime import datetime
# from pathlib import Path

# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
# from matplotlib.patches import Rectangle
# from matplotlib.lines import Line2D

# # -----------------------------
# # Manual T130 values (override)
# # -----------------------------
# YEAR = datetime.today().year  # change if needed (e.g., 2025)

# t130_points = [
#     ("15-Jan", 26284),
#     ("16-Jan", 26271),
#     ("19-Jan", 26271),
#     ("20-Jan", 26266),
#     ("21-Jan", 26270),
#     ("22-Jan", 26274),
#     ("23-Jan", 26257),
#     ("26-Jan", 26193),
#     ("27-Jan", 26135),
#     ("28-Jan", 26100),
#     ("29-Jan", 26064),
#     ("30-Jan", 25913),
# ]

# t130_idx = pd.to_datetime([f"{d}-{YEAR}" for d, _ in t130_points], format="%d-%b-%Y")
# t130_manual = pd.Series([v for _, v in t130_points], index=t130_idx, name="T130")

# START_DATE = t130_manual.index.min().to_pydatetime()
# END_DATE   = t130_manual.index.max().to_pydatetime()

# # -----------------------------
# # Bloomberg pull: USDVND OHLC
# # -----------------------------
# USD_TICKER = "USDVND BGN Curncy"
# FIELDS_OHLC = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"]

# raw = blp.bdh(
#     tickers=[USD_TICKER],
#     flds=FIELDS_OHLC,
#     start_date=START_DATE,
#     end_date=END_DATE,
# )

# # Handle MultiIndex columns if returned
# if isinstance(raw.columns, pd.MultiIndex):
#     # Expect (ticker, field) or (field, ticker) depending on wrapper
#     if USD_TICKER not in raw.columns.get_level_values(0) and USD_TICKER in raw.columns.get_level_values(1):
#         raw.columns = raw.columns.swaplevel(0, 1)
#     raw.columns = raw.columns.set_names(["ticker", "field"])
#     usd = raw[USD_TICKER].copy()
# else:
#     # If your wrapper returns single-level, rename directly
#     usd = raw.copy()

# usd.index = pd.to_datetime(usd.index)
# usd = usd.apply(pd.to_numeric, errors="coerce").rename(columns={
#     "PX_OPEN": "Open",
#     "PX_HIGH": "High",
#     "PX_LOW":  "Low",
#     "PX_LAST": "Close",
# })

# # Align to manual dates only (and require full OHLC + T130)
# plot_df = pd.concat([usd, t130_manual], axis=1).loc[t130_manual.index]
# plot_df = plot_df.dropna(subset=["Open", "High", "Low", "Close", "T130"])

# if plot_df.empty:
#     raise ValueError("No overlapping USDVND OHLC data for the provided T130 dates.")

# # -----------------------------
# # Plot: Candlestick + X for T130
# # -----------------------------
# x = np.arange(len(plot_df))
# w = 0.60

# fig, ax = plt.subplots(figsize=(14, 6))

# for i, (_, r) in enumerate(plot_df.iterrows()):
#     o, h, l, c, t = r["Open"], r["High"], r["Low"], r["Close"], r["T130"]

#     # Whisker (Low -> High)
#     ax.plot([i, i], [l, h], linewidth=1)

#     # Body (Open -> Close)
#     bottom = min(o, c)
#     height = abs(c - o)
#     if height == 0:
#         height = (h - l) * 0.001 if (h - l) > 0 else 0.1

#     rect = Rectangle(
#         (i - w/2, bottom), w, height,
#         alpha=0.35, edgecolor="black",
#         facecolor=("green" if c >= o else "red"),
#         linewidth=1,
#     )
#     ax.add_patch(rect)

#     # T130 marker
#     ax.scatter(i, t, marker="x", s=70, linewidths=2, zorder=5)

# # X labels
# date_labels = [d.strftime("%d-%b") for d in plot_df.index]
# ax.set_xticks(x)
# ax.set_xticklabels(date_labels, rotation=45, ha="right")

# ax.set_title(f"USDVND (BGN) OHLC with VND T130 (X) — {date_labels[0]} to {date_labels[-1]} ({YEAR})")
# ax.set_ylabel("USDVND")
# ax.grid(True, linestyle=":", alpha=0.5)

# legend_items = [
#     Rectangle((0,0), 1, 1, facecolor="green", alpha=0.35, edgecolor="black", label="Up day (Close ≥ Open)"),
#     Rectangle((0,0), 1, 1, facecolor="red",   alpha=0.35, edgecolor="black", label="Down day (Close < Open)"),
#     Line2D([0], [0], marker="x", linestyle="None", markersize=9, markeredgewidth=2, label="VND T130 (manual)"),
# ]
# ax.legend(handles=legend_items, loc="upper left", fontsize=9)

# SAVE_PATH = Path(G_CHART_DIR) if "G_CHART_DIR" in globals() else Path.cwd()
# OUTFILE   = SAVE_PATH / "USDVND_BGN_OHLC_with_T130_manual_15Jan_onward.png"

# plt.tight_layout()
# plt.savefig(OUTFILE, dpi=150, bbox_inches="tight")
# print(f"Saved chart to: {OUTFILE}")
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Inputs
# -----------------------------
USD_TKR  = "USDVND Curncy"
T123_TKR = "VND T123 Curncy"
FTSE_TKR = "VND FTSE Curncy"

END_DATE   = datetime.today()
START_DATE = END_DATE - relativedelta(years=10)

PIP_SIZE = 1.0          # 1 pip = 1 VND (change to 10.0 if your desk uses 10 VND = 1 pip)
PIP_X_TH = 15.0         # mark X if |pip diff| > 15
PCT_X_TH = 0.3          # mark X if |% diff|  > 0.3

SAVE_PATH = Path(G_CHART_DIR) if "G_CHART_DIR" in globals() else Path.cwd()
OUTFILE   = SAVE_PATH / "USDVND_T123_FTSE_and_diffs_10y.png"

# -----------------------------
# Bloomberg pull
# -----------------------------
raw = blp.bdh(
    tickers=[USD_TKR, T123_TKR, FTSE_TKR],
    flds=["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_BID"],
    start_date=START_DATE,
    end_date=END_DATE,
)

df = raw.copy()

# Normalize MultiIndex to (ticker, field)
if isinstance(df.columns, pd.MultiIndex):
    if USD_TKR not in df.columns.get_level_values(0) and USD_TKR in df.columns.get_level_values(1):
        df.columns = df.columns.swaplevel(0, 1)
    df.columns = df.columns.set_names(["ticker", "field"])
else:
    raise ValueError("Expected MultiIndex columns (ticker, field). Adjust this block for your Bloomberg wrapper.")

df.index = pd.to_datetime(df.index)
df = df.apply(pd.to_numeric, errors="coerce")

# -----------------------------
# Extract series
# -----------------------------
spot = df[USD_TKR][["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"]].rename(columns={
    "PX_OPEN": "Open", "PX_HIGH": "High", "PX_LOW": "Low", "PX_LAST": "Close"
})

t123 = df[T123_TKR]["PX_LAST"].rename("T123")
ftse = df[FTSE_TKR]["PX_BID"].rename("FTSE")  # as requested

# Combine
all_df = pd.concat([spot, t123, ftse], axis=1)

# Diffs require T123 + FTSE
diff_df = all_df[["T123", "FTSE"]].dropna().copy()
diff_df["Diff_pips"] = (diff_df["FTSE"] - diff_df["T123"]) / PIP_SIZE
diff_df["Diff_pct"]  = (diff_df["FTSE"] / diff_df["T123"] - 1.0) * 100.0

# Out-of-range (FTSE outside spot day's Low/High)
rng_df = all_df[["Low", "High", "FTSE"]].dropna().copy()
oor_mask = (rng_df["FTSE"] < rng_df["Low"]) | (rng_df["FTSE"] > rng_df["High"])
oor_dates = rng_df.index[oor_mask]

# -----------------------------
# Plot: 3 panels, shared x-axis
# -----------------------------
fig, (ax_top, ax_pip, ax_pct) = plt.subplots(
    3, 1, figsize=(16, 10), sharex=True,
    gridspec_kw={"height_ratios": [2.1, 1.2, 1.2], "hspace": 0.08}
)

# --- Top: 3 time series together ---
# (use spot close as the "spot price" line; still uses OHLC for out-of-range test)
ax_top.plot(all_df.index, all_df["Close"], lw=1.6, label="USDVND Spot (Close)")
ax_top.plot(all_df.index, all_df["T123"],  lw=1.3, label="T123 (PX_LAST)")
ax_top.plot(all_df.index, all_df["FTSE"],  lw=1.3, label="FTSE Fix (PX_BID)")

# Shade days where fix is outside spot day's trading range
for d in oor_dates:
    ax_top.axvspan(d - pd.Timedelta(hours=12), d + pd.Timedelta(hours=12), alpha=0.18, color="red")

ax_top.set_title("USDVND Spot vs T123 vs FTSE (10Y) — shaded = FTSE outside spot Low/High")
ax_top.set_ylabel("USDVND")
ax_top.grid(True, linestyle=":", alpha=0.5)
ax_top.legend(loc="upper left", fontsize=9)

# --- Middle: pip diff bars + X on |diff| > 15 ---
x = diff_df.index
pip_vals = diff_df["Diff_pips"]

ax_pip.bar(x, pip_vals.values, width=1.0)  # daily bars (matplotlib treats datetime spacing reasonably)
ax_pip.axhline(0, linestyle="--", alpha=0.6)
ax_pip.set_ylabel("FTSE − T123 (pips)")
ax_pip.grid(True, linestyle=":", alpha=0.5)

pip_x_mask = pip_vals.abs() > PIP_X_TH
ax_pip.scatter(
    x[pip_x_mask],
    pip_vals[pip_x_mask].values,
    marker="x",
    s=50,
    linewidths=2,
    zorder=5,
    label=f"|diff| > {PIP_X_TH:g} pips"
)
ax_pip.legend(loc="upper left", fontsize=9)

# --- Bottom: % diff bars + X on |diff| > 0.3% ---
pct_vals = diff_df["Diff_pct"]

ax_pct.bar(x, pct_vals.values, width=1.0)
ax_pct.axhline(0, linestyle="--", alpha=0.6)
ax_pct.set_ylabel("FTSE − T123 (%)")
ax_pct.set_xlabel("Date")
ax_pct.grid(True, linestyle=":", alpha=0.5)

pct_x_mask = pct_vals.abs() > PCT_X_TH
ax_pct.scatter(
    x[pct_x_mask],
    pct_vals[pct_x_mask].values,
    marker="x",
    s=50,
    linewidths=2,
    zorder=5,
    label=f"|diff| > {PCT_X_TH:g}%"
)
ax_pct.legend(loc="upper left", fontsize=9)

plt.tight_layout()
plt.savefig(OUTFILE, dpi=150, bbox_inches="tight")
print(f"Saved chart to: {OUTFILE}")

# Optional quick counts
print(f"Days with |pip diff| > {PIP_X_TH:g}: {pip_x_mask.sum()}")
print(f"Days with |% diff|  > {PCT_X_TH:g}: {pct_x_mask.sum()}")
print(f"Days FTSE outside spot Low/High: {len(oor_dates)}")

###############################################################################
# MAS DLI Chart
###############################################################################
# Tickers
MAS_DLI_TICKER_NEER = "CTSGSGD Index"      # S$NEER (daily)
MAS_DLI_TICKER_SORA = "SORACA3M Index"     # 3m compounded SORA (daily)

MAS_DLI_CSV_PATH = Path(__file__).parent / "mas_dli_from_excel.csv"

MAS_DLI_START_DATE = datetime(2011, 10, 1)  # at least 3 months before first plotted month
MAS_DLI_END_DATE = datetime.today()

# Weights + scaling (as per Excel logic)
MAS_DLI_W_NEER = 0.6
MAS_DLI_W_SORA = 0.4
MAS_DLI_NEER_VAR_DIV = 2.0

# Styling
MAS_DLI_C_DLI = "#4472C4"
MAS_DLI_C_PROXY = "#ED7D31"
MAS_DLI_C_NEER_BAR = "#70AD47"
MAS_DLI_C_SORA_BAR = "#FFC000"
MAS_DLI_C_ZERO = "black"
MAS_DLI_LINE_W = 2.5
MAS_DLI_BAR_WIDTH_DAYS = 25


def mas_dli_fetch_daily_bbg(tickers, start, end):
    """Fetch daily Bloomberg data for MAS DLI."""
    df = blp.bdh(tickers, "PX_LAST", start, end)
    # flatten MultiIndex columns (xbbg sometimes returns MultiIndex)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    return df


def mas_dli_to_monthly_bm_last(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Resample daily to business-month-end (last obs),
    then normalize index to month-start timestamps for clean labeling.
    """
    # Handle both old and new pandas frequency aliases
    try:
        m = df_daily.resample("BME").last()  # pandas >= 2.2
    except ValueError:
        m = df_daily.resample("BM").last()   # pandas < 2.2
    m.index = m.index.to_period("M").to_timestamp()
    return m


def mas_dli_load_dli_csv(csv_path) -> pd.Series:
    """Load MAS DLI 3-month change data from CSV."""
    d = pd.read_csv(csv_path, parse_dates=["date"])
    d["MAS_DLI_3m_change"] = pd.to_numeric(d["MAS_DLI_3m_change_pct"], errors="coerce")
    d = d.dropna(subset=["date", "MAS_DLI_3m_change"]).copy()
    d["date"] = d["date"].dt.to_period("M").dt.to_timestamp()
    d = d.set_index("date").sort_index()
    return d["MAS_DLI_3m_change"]


def mas_dli_stacked_two_series_excel_like(ax, x, a, b, label_a, label_b, color_a, color_b, width_days):
    """
    Excel-like stacking for mixed signs:
    - positives stack upward from 0
    - negatives stack downward from 0
    """
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


def mas_dli_compute_proxy_3m(neer_m: pd.Series, sora_m: pd.Series) -> pd.DataFrame:
    """Compute 3-month proxy for MAS DLI."""
    out = pd.DataFrame({"NEER": neer_m, "SORA": sora_m}).copy()

    # 3m % change in NEER
    out["NEER_3m_pct"] = (out["NEER"] / out["NEER"].shift(3) - 1.0) * 100.0
    out["NEER_scaled_3m"] = out["NEER_3m_pct"] / MAS_DLI_NEER_VAR_DIV

    # 3m pp change in SORA
    out["SORA_3m_pp"] = out["SORA"] - out["SORA"].shift(3)

    # contributions + proxy
    out["NEER_contrib_3m"] = MAS_DLI_W_NEER * out["NEER_scaled_3m"]
    out["SORA_contrib_3m"] = MAS_DLI_W_SORA * out["SORA_3m_pp"]
    out["Proxy_SORA_3m"] = out["NEER_contrib_3m"] + out["SORA_contrib_3m"]

    return out


def mas_dli_compute_proxy_monthly_bc(neer_m: pd.Series, sora_m: pd.Series) -> pd.DataFrame:
    """
    Compute monthly change proxy (BC calculated):
    - NEER leg uses monthly % change, variance-scaled (/2) before weighting in the proxy
    - SORA leg uses monthly pp change
    """
    out = pd.DataFrame({"NEER": neer_m, "SORA": sora_m}).copy()

    # monthly % change NEER
    out["NEER_m_pct"] = (out["NEER"] / out["NEER"].shift(1) - 1.0) * 100.0

    # monthly pp change SORA
    out["SORA_m_pp"] = out["SORA"] - out["SORA"].shift(1)

    # contributions used in the proxy (NEER variance-scaled)
    out["NEER_contrib_m"] = MAS_DLI_W_NEER * (out["NEER_m_pct"] / MAS_DLI_NEER_VAR_DIV)
    out["SORA_contrib_m"] = MAS_DLI_W_SORA * out["SORA_m_pp"]
    out["Proxy_m"] = out["NEER_contrib_m"] + out["SORA_contrib_m"]

    return out


def mas_dli_plot_chart1_dli_vs_proxy(df_all: pd.DataFrame, save_path: Path = None):
    """
    Chart 1: MAS DLI vs Proxy (SORA only).
    Proxy extends to the latest Bloomberg month; DLI stops where it stops.
    """
    fig, ax = plt.subplots(figsize=(12, 5))

    # Plot proxy first so axis naturally extends to latest proxy date
    ax.plot(df_all.index, df_all["Proxy_SORA_3m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W,
            label="Proxy (60% S$NEER & 40% SORA, variance scaled)")

    ax.plot(df_all.index, df_all["MAS_DLI_3m"], color=MAS_DLI_C_DLI, linewidth=MAS_DLI_LINE_W,
            label="MAS DLI")

    ax.set_title("MAS DLI and Proxy")
    ax.grid(True, axis="y", alpha=0.3)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_ylim(top=1.5)
    ax.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.35), frameon=False, ncol=1)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved MAS DLI Chart 1 to: {save_path}")

    return fig, ax


def mas_dli_plot_chart2_stacked_3m_with_lines(df_all: pd.DataFrame, start="2019-01-01", save_path: Path = None):
    """
    Chart 2: thick stacked bars (NEER + SORA contributions, 3m),
    plus MAS DLI and Proxy lines, plus black y=0 line.
    """
    d = df_all[df_all.index >= pd.Timestamp(start)].copy()

    fig, ax = plt.subplots(figsize=(12, 5))

    mas_dli_stacked_two_series_excel_like(
        ax=ax,
        x=d.index,
        a=d["NEER_contrib_3m"],
        b=d["SORA_contrib_3m"],
        label_a="S$NEER contribution",
        label_b="SORA contribution",
        color_a=MAS_DLI_C_NEER_BAR,
        color_b=MAS_DLI_C_SORA_BAR,
        width_days=MAS_DLI_BAR_WIDTH_DAYS,
    )

    # overlay lines
    ax.plot(d.index, d["MAS_DLI_3m"], color=MAS_DLI_C_DLI, linewidth=MAS_DLI_LINE_W, label="MAS DLI")
    ax.plot(d.index, d["Proxy_SORA_3m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W,
            label="Proxy (60% S$NEER & 40% SORA, variance scaled)")

    ax.axhline(0, color=MAS_DLI_C_ZERO, linewidth=1.2)

    ax.set_title("MAS DLI and Proxy (change over three months)")
    ax.grid(True, axis="y", alpha=0.3)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_ylim(-1.0, 1.5)
    ax.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.40), frameon=False, ncol=1)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved MAS DLI Chart 2 to: {save_path}")

    return fig, ax


def mas_dli_plot_chart3_monthly_bc(df_m: pd.DataFrame, start=None, save_path: Path = None):
    """
    Chart 3: DLI Proxy (monthly change, BC calculated).
    Thick stacked bars (variance-scaled NEER contrib + SORA contrib), optional proxy line, black y=0 line.
    """
    d = df_m.copy()
    if start is not None:
        d = d[d.index >= pd.Timestamp(start)].copy()

    fig, ax = plt.subplots(figsize=(12, 5))

    mas_dli_stacked_two_series_excel_like(
        ax=ax,
        x=d.index,
        a=d["NEER_contrib_m"],
        b=d["SORA_contrib_m"],
        label_a="S$NEER contribution (monthly, variance scaled)",
        label_b="SORA contribution (monthly)",
        color_a=MAS_DLI_C_NEER_BAR,
        color_b=MAS_DLI_C_SORA_BAR,
        width_days=MAS_DLI_BAR_WIDTH_DAYS,
    )

    # optional proxy line
    ax.plot(d.index, d["Proxy_m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W, label="Proxy (monthly change)")

    ax.axhline(0, color=MAS_DLI_C_ZERO, linewidth=1.2)

    ax.set_title("DLI Proxy (monthly change, BC calculated)")
    ax.grid(True, axis="y", alpha=0.3)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.40), frameon=False, ncol=1)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved MAS DLI Chart 3 to: {save_path}")

    return fig, ax


# --- Generate and save MAS DLI charts (combined into one figure with 3 subplots) ---
MAS_DLI_OUTFILE = G_CHART_DIR / "MAS_DLI_Charts.png"

# 1) Load DLI (monthly index, already 3m change series) from CSV
mas_dli_3m = mas_dli_load_dli_csv(MAS_DLI_CSV_PATH)
print(f"MAS DLI CSV loaded: {len(mas_dli_3m)} points, range {mas_dli_3m.index.min()} to {mas_dli_3m.index.max()}")

# 2) Pull daily from Bloomberg and convert to monthly
mas_dli_daily = mas_dli_fetch_daily_bbg([MAS_DLI_TICKER_NEER, MAS_DLI_TICKER_SORA], MAS_DLI_START_DATE, MAS_DLI_END_DATE)
mas_dli_m = mas_dli_to_monthly_bm_last(mas_dli_daily)
print(f"Bloomberg monthly data: {len(mas_dli_m)} points, range {mas_dli_m.index.min()} to {mas_dli_m.index.max()}")

# 3) Compute proxy (3m) & monthly proxy (BC)
mas_dli_proxy_3m = mas_dli_compute_proxy_3m(mas_dli_m[MAS_DLI_TICKER_NEER], mas_dli_m[MAS_DLI_TICKER_SORA])
mas_dli_proxy_m = mas_dli_compute_proxy_monthly_bc(mas_dli_m[MAS_DLI_TICKER_NEER], mas_dli_m[MAS_DLI_TICKER_SORA])

# 4) Normalize timestamps to ensure proper join
# Convert both indexes to the same format (Period -> start timestamp)
mas_dli_proxy_3m.index = mas_dli_proxy_3m.index.to_period('M').to_timestamp()
mas_dli_3m.index = mas_dli_3m.index.to_period('M').to_timestamp()

# 5) Combine for chart 1 & 2.
# IMPORTANT: use proxy index as the master index so proxy plots to latest even if DLI ends earlier.
mas_dli_df_all = mas_dli_proxy_3m.join(mas_dli_3m.rename("MAS_DLI_3m"), how="left")
print(f"After join: MAS_DLI_3m non-null count = {mas_dli_df_all['MAS_DLI_3m'].notna().sum()}")

# drop early rows where 3m proxy can't be computed yet
mas_dli_df_all = mas_dli_df_all.dropna(subset=["Proxy_SORA_3m", "NEER_contrib_3m", "SORA_contrib_3m"], how="any")
print(f"After dropna: {len(mas_dli_df_all)} rows, MAS_DLI_3m non-null = {mas_dli_df_all['MAS_DLI_3m'].notna().sum()}")

# Prepare data for charts
mas_dli_df_chart2 = mas_dli_df_all[mas_dli_df_all.index >= pd.Timestamp("2019-01-01")].copy()
mas_dli_proxy_m = mas_dli_proxy_m.dropna(subset=["Proxy_m", "NEER_contrib_m", "SORA_contrib_m"])

# Filter Monthly BC to last 2 years
mas_dli_two_years_ago = pd.Timestamp.now() - pd.DateOffset(years=2)
mas_dli_proxy_m_2y = mas_dli_proxy_m[mas_dli_proxy_m.index >= mas_dli_two_years_ago].copy()

# Create figure with 3 subplots (vertically stacked)
mas_dli_fig, (mas_dli_ax1, mas_dli_ax2, mas_dli_ax3) = plt.subplots(3, 1, figsize=(14, 15))

# ---- Subplot 1: MAS DLI vs Proxy ----
# Plot proxy first (extends to latest), then DLI on top
mas_dli_ax1.plot(mas_dli_df_all.index, mas_dli_df_all["Proxy_SORA_3m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W,
        label="Proxy (60% S$NEER & 40% SORA, variance scaled)")
mas_dli_ax1.plot(mas_dli_df_all.index, mas_dli_df_all["MAS_DLI_3m"], color=MAS_DLI_C_DLI, linewidth=MAS_DLI_LINE_W,
        label="MAS DLI")
mas_dli_ax1.set_title("MAS DLI and Proxy", fontsize=12, fontweight='bold')
mas_dli_ax1.grid(True, axis="y", alpha=0.3)
mas_dli_ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
mas_dli_ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
plt.setp(mas_dli_ax1.get_xticklabels(), rotation=45, ha="right")
mas_dli_ax1.set_ylim(top=1.5)
mas_dli_ax1.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
mas_dli_ax1.legend(loc="upper right", fontsize=9)

# Add latest value labels for subplot 1
mas_dli_proxy_last = mas_dli_df_all["Proxy_SORA_3m"].dropna()
if len(mas_dli_proxy_last) > 0:
    mas_dli_proxy_last_date = mas_dli_proxy_last.index[-1]
    mas_dli_proxy_last_val = mas_dli_proxy_last.iloc[-1]
    mas_dli_ax1.annotate(f"{mas_dli_proxy_last_val:.2f}", xy=(mas_dli_proxy_last_date, mas_dli_proxy_last_val),
                         xytext=(5, 0), textcoords='offset points', fontsize=9, color=MAS_DLI_C_PROXY,
                         bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_PROXY, alpha=0.8, boxstyle='round,pad=0.2'))
mas_dli_dli_last = mas_dli_df_all["MAS_DLI_3m"].dropna()
if len(mas_dli_dli_last) > 0:
    mas_dli_dli_last_date = mas_dli_dli_last.index[-1]
    mas_dli_dli_last_val = mas_dli_dli_last.iloc[-1]
    mas_dli_ax1.annotate(f"{mas_dli_dli_last_val:.2f}", xy=(mas_dli_dli_last_date, mas_dli_dli_last_val),
                         xytext=(5, -15), textcoords='offset points', fontsize=9, color=MAS_DLI_C_DLI,
                         bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_DLI, alpha=0.8, boxstyle='round,pad=0.2'))

# ---- Subplot 2: Stacked 3m with lines (since 2019) ----
mas_dli_stacked_two_series_excel_like(
    ax=mas_dli_ax2,
    x=mas_dli_df_chart2.index,
    a=mas_dli_df_chart2["NEER_contrib_3m"],
    b=mas_dli_df_chart2["SORA_contrib_3m"],
    label_a="S$NEER contribution",
    label_b="SORA contribution",
    color_a=MAS_DLI_C_NEER_BAR,
    color_b=MAS_DLI_C_SORA_BAR,
    width_days=MAS_DLI_BAR_WIDTH_DAYS,
)
# Overlay lines on top of bars
mas_dli_ax2.plot(mas_dli_df_chart2.index, mas_dli_df_chart2["MAS_DLI_3m"], color=MAS_DLI_C_DLI, linewidth=MAS_DLI_LINE_W, label="MAS DLI")
mas_dli_ax2.plot(mas_dli_df_chart2.index, mas_dli_df_chart2["Proxy_SORA_3m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W,
        label="Proxy (60% S$NEER & 40% SORA, variance scaled)")
mas_dli_ax2.axhline(0, color=MAS_DLI_C_ZERO, linewidth=1.2)
mas_dli_ax2.set_title("MAS DLI and Proxy (change over three months, since 2019)", fontsize=12, fontweight='bold')
mas_dli_ax2.grid(True, axis="y", alpha=0.3)
mas_dli_ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
mas_dli_ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
plt.setp(mas_dli_ax2.get_xticklabels(), rotation=45, ha="right")
mas_dli_ax2.set_ylim(-1.0, 1.5)
mas_dli_ax2.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
mas_dli_ax2.legend(loc="upper right", fontsize=9)

# Add latest value labels for subplot 2
mas_dli_proxy_last2 = mas_dli_df_chart2["Proxy_SORA_3m"].dropna()
if len(mas_dli_proxy_last2) > 0:
    mas_dli_proxy_last2_date = mas_dli_proxy_last2.index[-1]
    mas_dli_proxy_last2_val = mas_dli_proxy_last2.iloc[-1]
    mas_dli_ax2.annotate(f"{mas_dli_proxy_last2_val:.2f}", xy=(mas_dli_proxy_last2_date, mas_dli_proxy_last2_val),
                         xytext=(5, 0), textcoords='offset points', fontsize=9, color=MAS_DLI_C_PROXY,
                         bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_PROXY, alpha=0.8, boxstyle='round,pad=0.2'))
mas_dli_dli_last2 = mas_dli_df_chart2["MAS_DLI_3m"].dropna()
if len(mas_dli_dli_last2) > 0:
    mas_dli_dli_last2_date = mas_dli_dli_last2.index[-1]
    mas_dli_dli_last2_val = mas_dli_dli_last2.iloc[-1]
    mas_dli_ax2.annotate(f"{mas_dli_dli_last2_val:.2f}", xy=(mas_dli_dli_last2_date, mas_dli_dli_last2_val),
                         xytext=(5, -15), textcoords='offset points', fontsize=9, color=MAS_DLI_C_DLI,
                         bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_DLI, alpha=0.8, boxstyle='round,pad=0.2'))

# ---- Subplot 3: Monthly BC (last 2 years only) ----
mas_dli_stacked_two_series_excel_like(
    ax=mas_dli_ax3,
    x=mas_dli_proxy_m_2y.index,
    a=mas_dli_proxy_m_2y["NEER_contrib_m"],
    b=mas_dli_proxy_m_2y["SORA_contrib_m"],
    label_a="S$NEER contribution (monthly, variance scaled)",
    label_b="SORA contribution (monthly)",
    color_a=MAS_DLI_C_NEER_BAR,
    color_b=MAS_DLI_C_SORA_BAR,
    width_days=MAS_DLI_BAR_WIDTH_DAYS,
)
mas_dli_ax3.plot(mas_dli_proxy_m_2y.index, mas_dli_proxy_m_2y["Proxy_m"], color=MAS_DLI_C_PROXY, linewidth=MAS_DLI_LINE_W, label="Proxy (monthly change)")
mas_dli_ax3.axhline(0, color=MAS_DLI_C_ZERO, linewidth=1.2)
mas_dli_ax3.set_title("DLI Proxy (monthly change, BC calculated, last 2 years)", fontsize=12, fontweight='bold')
mas_dli_ax3.grid(True, axis="y", alpha=0.3)
mas_dli_ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
mas_dli_ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
plt.setp(mas_dli_ax3.get_xticklabels(), rotation=45, ha="right")
mas_dli_ax3.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
mas_dli_ax3.legend(loc="upper right", fontsize=9)

# Add latest value label for subplot 3
mas_dli_proxy_m_last = mas_dli_proxy_m_2y["Proxy_m"].dropna()
if len(mas_dli_proxy_m_last) > 0:
    mas_dli_proxy_m_last_date = mas_dli_proxy_m_last.index[-1]
    mas_dli_proxy_m_last_val = mas_dli_proxy_m_last.iloc[-1]
    mas_dli_ax3.annotate(f"{mas_dli_proxy_m_last_val:.2f}", xy=(mas_dli_proxy_m_last_date, mas_dli_proxy_m_last_val),
                         xytext=(5, 0), textcoords='offset points', fontsize=9, color=MAS_DLI_C_PROXY,
                         bbox=dict(facecolor='white', edgecolor=MAS_DLI_C_PROXY, alpha=0.8, boxstyle='round,pad=0.2'))

# Add last data date annotation to the figure
mas_dli_last_data_date = mas_dli_proxy_m_last_date.strftime('%d %b %Y') if len(mas_dli_proxy_m_last) > 0 else "N/A"
mas_dli_fig.text(0.98, 0.98, f"Last data: {mas_dli_last_data_date}", transform=mas_dli_fig.transFigure,
                 fontsize=10, ha='right', va='top',
                 bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))

plt.tight_layout()
plt.savefig(MAS_DLI_OUTFILE, dpi=150, bbox_inches='tight')
print(f"Saved MAS DLI chart to: {MAS_DLI_OUTFILE}")

print("MAS DLI charts generated successfully.")

# =============================================================================
# KOREA MMF TOTAL AUM CHART
# =============================================================================
print("\n" + "="*60)
print("Generating Korea MMF Total AUM chart...")
print("="*60)

# Import MMF crawler functions
try:
    from crawl_mmf_aum import load_cache as mmf_load_cache, save_cache as mmf_save_cache, crawl_mmf_trend, plot_mmf_chart
    MMF_CRAWLER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import crawl_mmf_aum module: {e}")
    MMF_CRAWLER_AVAILABLE = False

# MMF Configuration
MMF_SCRIPT_DIR = Path(os.path.dirname(__file__))
MMF_CACHE_FILE = MMF_SCRIPT_DIR / "mmf_aum_cache.csv"
MMF_OUTFILE = Path(G_CHART_DIR) / "korea_mmf_aum.png"

def plot_mmf_chart_for_updater(df, output_path):
    """Plot MMF Total AUM (last 1 year) with 30-day change subplot. Units in KRW trillion."""
    if df.empty:
        print("No MMF data to plot")
        return

    df = df.sort_values("date").copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset=["date"], keep="last")

    # Convert units: 100 million KRW -> KRW trillion (divide by 10000)
    df["mmf_aum_tn"] = df["mmf_total_aum"] / 10000.0

    # Filter to last 1 year
    one_year_ago = pd.Timestamp.today().normalize() - pd.DateOffset(years=1)
    df_1y = df[df["date"] >= one_year_ago].copy()

    if df_1y.empty:
        print("No MMF data in the last year")
        return

    # Calculate 30 calendar day change
    df_1y = df_1y.set_index("date").sort_index()
    df_1y["change_30d"] = df_1y["mmf_aum_tn"].diff(30)  # Approximate 30 calendar days

    # Create figure with 2 subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), height_ratios=[1.2, 1])

    # --- Subplot 1: MMF Total AUM (last 1 year) ---
    dates = df_1y.index
    values = df_1y["mmf_aum_tn"].values

    # Plot as line with segments (break at gaps > 5 days)
    segments_x = []
    segments_y = []
    current_x = [dates[0]]
    current_y = [values[0]]

    for i in range(1, len(dates)):
        gap_days = (dates[i] - dates[i-1]).days
        if gap_days > 5:
            segments_x.append(current_x)
            segments_y.append(current_y)
            current_x = [dates[i]]
            current_y = [values[i]]
        else:
            current_x.append(dates[i])
            current_y.append(values[i])

    segments_x.append(current_x)
    segments_y.append(current_y)

    # Plot each segment
    for seg_x, seg_y in zip(segments_x, segments_y):
        ax1.plot(seg_x, seg_y, linewidth=1.5, color="#1f77b4")

    # Add light fill under the data
    ax1.fill_between(df_1y.index, df_1y["mmf_aum_tn"], alpha=0.1, color="#1f77b4")

    ax1.set_title("Korea MMF Total AUM (in KRW tn) - Last 1 Year", fontsize=14, fontweight="bold")
    ax1.set_ylabel("MMF Total AUM (KRW tn)")

    # Format y-axis with 1 decimal
    from matplotlib.ticker import FuncFormatter
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:,.1f}"))

    # Format x-axis dates
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")

    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(df_1y.index.min(), df_1y.index.max())

    # Add latest value annotation
    last_date = df_1y.index[-1]
    last_val = df_1y["mmf_aum_tn"].iloc[-1]
    ax1.annotate(f"{last_val:,.1f}", xy=(last_date, last_val),
                xytext=(5, 0), textcoords='offset points', fontsize=10, color="#1f77b4",
                bbox=dict(facecolor='white', edgecolor="#1f77b4", alpha=0.8, boxstyle='round,pad=0.2'))

    # --- Subplot 2: 30-Day Change ---
    change_data = df_1y["change_30d"].dropna()
    if not change_data.empty:
        # Color bars based on positive/negative
        colors = ['#2ca02c' if x >= 0 else '#d62728' for x in change_data.values]
        ax2.bar(change_data.index, change_data.values, color=colors, alpha=0.7, width=1)
        ax2.axhline(0, color='black', linewidth=0.8)

        ax2.set_title("30-Day Change (in KRW tn)", fontsize=12, fontweight="bold")
        ax2.set_ylabel("Change (KRW tn)")
        ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:,.1f}"))

        # Format x-axis dates
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")

        ax2.grid(True, alpha=0.3, axis='y')
        ax2.set_xlim(df_1y.index.min(), df_1y.index.max())

        # Add latest change value annotation
        last_change_date = change_data.index[-1]
        last_change_val = change_data.iloc[-1]
        change_color = '#2ca02c' if last_change_val >= 0 else '#d62728'
        ax2.annotate(f"{last_change_val:+,.1f}", xy=(last_change_date, last_change_val),
                    xytext=(5, 0), textcoords='offset points', fontsize=10, color=change_color,
                    bbox=dict(facecolor='white', edgecolor=change_color, alpha=0.8, boxstyle='round,pad=0.2'))

    # Add last data date annotation to figure
    fig.text(0.98, 0.98, f"Last data: {last_date.strftime('%d %b %Y')}", transform=fig.transFigure,
             fontsize=10, ha='right', va='top',
             bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))

    plt.tight_layout()
    plt.savefig(str(output_path), dpi=150, bbox_inches="tight")
    print(f"Saved MMF chart to: {output_path}")
    plt.close()

# Run MMF crawler and generate chart
if MMF_CRAWLER_AVAILABLE:
    try:
        # Load existing cache
        mmf_cache_df = mmf_load_cache()
        print(f"Existing MMF cache: {len(mmf_cache_df)} records")

        # Determine if we need to fetch new data
        mmf_needs_update = True
        if len(mmf_cache_df) > 0:
            mmf_cache_max_date = mmf_cache_df["date"].max()
            mmf_today = pd.Timestamp.today().normalize()
            # Only update if cache is more than 1 day old
            if (mmf_today - mmf_cache_max_date).days <= 1:
                print(f"MMF cache is up to date (last data: {mmf_cache_max_date.strftime('%Y-%m-%d')})")
                mmf_needs_update = False

        if mmf_needs_update:
            print("Fetching new MMF data from KOFIA...")
            # Calculate start date for incremental update
            if len(mmf_cache_df) > 0:
                mmf_start_date = (mmf_cache_df["date"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                mmf_start_date = "2017-01-01"
            mmf_end_date = datetime.today().strftime("%Y-%m-%d")

            # Run crawler
            mmf_new_df = crawl_mmf_trend(
                start_date=mmf_start_date,
                end_date=mmf_end_date,
                headless=True
            )

            # Merge with existing cache
            if mmf_new_df is not None and len(mmf_new_df) > 0:
                if len(mmf_cache_df) > 0:
                    mmf_combined = pd.concat([mmf_cache_df, mmf_new_df], ignore_index=True)
                else:
                    mmf_combined = mmf_new_df
                mmf_save_cache(mmf_combined)
                mmf_df = mmf_combined
                print(f"Updated MMF cache: {len(mmf_df)} total records")
            else:
                mmf_df = mmf_cache_df
                print("No new MMF data fetched, using existing cache")
        else:
            mmf_df = mmf_cache_df

        # Generate chart
        if len(mmf_df) > 0:
            plot_mmf_chart_for_updater(mmf_df, MMF_OUTFILE)
            print("Korea MMF chart generated successfully.")
        else:
            print("No MMF data available to plot")

    except Exception as e:
        print(f"Error processing MMF data: {e}")
        import traceback
        traceback.print_exc()
else:
    print("MMF crawler not available. Skipping MMF chart generation.")

# =============================================================================
# INDIA IT SERVICES COMPANIES CHART
# =============================================================================
print("\n" + "="*60)
print("Generating India IT Services Companies chart...")
print("="*60)

# Configuration
INDIA_IT_TICKERS = ['TCS IN Equity', 'WPRO IN Equity', 'HCLT IN Equity', 'INFO IN Equity', 'CTSH US Equity']
INDIA_IT_NAMES = {
    'TCS IN Equity': 'TCS',
    'WPRO IN Equity': 'Wipro',
    'HCLT IN Equity': 'HCL Tech',
    'INFO IN Equity': 'Infosys',
    'CTSH US Equity': 'Cognizant'
}
INDIA_SERVICES_EXPORT_TICKER = 'INITSEXP Index'
NIFTY_TICKER = 'NIFTY Index'
INDIA_IT_START_DATE = datetime(2010, 1, 1)
INDIA_IT_END_DATE = datetime.today()
INDIA_IT_OUTFILE = Path(G_CHART_DIR) / "india_it_services.png"

try:
    # --- Fetch Revenue Growth Data (RR033) ---
    print("Fetching revenue growth data (RR033)...")
    revenue_growth_raw = blp.bdh(
        tickers=INDIA_IT_TICKERS,
        flds=['RR033'],
        start_date=INDIA_IT_START_DATE,
        end_date=INDIA_IT_END_DATE
    )
    # Flatten multi-index columns
    if isinstance(revenue_growth_raw.columns, pd.MultiIndex):
        revenue_growth_raw.columns = [col[0] for col in revenue_growth_raw.columns]
    revenue_growth_raw.index = pd.to_datetime(revenue_growth_raw.index)

    # Calculate average revenue growth across companies
    revenue_growth_avg = revenue_growth_raw.mean(axis=1).dropna()
    revenue_growth_avg.name = 'Avg Revenue Growth YoY'

    # --- Fetch India Services Exports Data ---
    print("Fetching India services exports data...")
    services_export_raw = blp.bdh(
        tickers=[INDIA_SERVICES_EXPORT_TICKER],
        flds=['PX_LAST'],
        start_date=INDIA_IT_START_DATE,
        end_date=INDIA_IT_END_DATE
    )
    if isinstance(services_export_raw.columns, pd.MultiIndex):
        services_export_raw.columns = [col[0] for col in services_export_raw.columns]
    services_export_raw.index = pd.to_datetime(services_export_raw.index)
    services_export = services_export_raw.iloc[:, 0].dropna()

    # Calculate rolling 12-month sum and YoY growth
    services_export_12m = services_export.rolling(window=12, min_periods=12).sum()
    services_export_yoy = services_export_12m.pct_change(periods=12) * 100
    services_export_yoy = services_export_yoy.dropna()
    services_export_yoy.name = 'India Services Exports YoY%'

    # --- Fetch Share Prices (PX_LAST) ---
    print("Fetching share price data...")
    prices_raw = blp.bdh(
        tickers=INDIA_IT_TICKERS + [NIFTY_TICKER],
        flds=['PX_LAST'],
        start_date=INDIA_IT_START_DATE,
        end_date=INDIA_IT_END_DATE
    )
    if isinstance(prices_raw.columns, pd.MultiIndex):
        prices_raw.columns = [col[0] for col in prices_raw.columns]
    prices_raw.index = pd.to_datetime(prices_raw.index)

    # Find common start date where all companies have data
    prices_companies = prices_raw[INDIA_IT_TICKERS].dropna()
    if not prices_companies.empty:
        common_start = prices_companies.index[0]
        prices_companies = prices_companies[prices_companies.index >= common_start]

        # Create index (base = 100 at common start)
        prices_indexed = (prices_companies / prices_companies.iloc[0]) * 100

        # Create equal-weighted index of IT companies
        it_index = prices_indexed.mean(axis=1)
        it_index.name = 'IT Services Index'

        # Get NIFTY and rebase
        nifty_prices = prices_raw[NIFTY_TICKER].dropna()
        nifty_prices = nifty_prices[nifty_prices.index >= common_start]
        if not nifty_prices.empty:
            nifty_indexed = (nifty_prices / nifty_prices.iloc[0]) * 100
            nifty_indexed.name = 'NIFTY Index'

            # Calculate relative performance (IT Index / NIFTY)
            common_idx = it_index.index.intersection(nifty_indexed.index)
            relative_perf = (it_index.loc[common_idx] / nifty_indexed.loc[common_idx]) * 100
            relative_perf.name = 'IT vs NIFTY (Relative)'
        else:
            nifty_indexed = pd.Series()
            relative_perf = pd.Series()
    else:
        it_index = pd.Series()
        nifty_indexed = pd.Series()
        relative_perf = pd.Series()

    # --- Fetch Employee Count Data (RR121) ---
    print("Fetching employee count data (RR121)...")
    employees_raw = blp.bdh(
        tickers=INDIA_IT_TICKERS,
        flds=['RR121'],
        start_date=INDIA_IT_START_DATE,
        end_date=INDIA_IT_END_DATE
    )
    if isinstance(employees_raw.columns, pd.MultiIndex):
        employees_raw.columns = [col[0] for col in employees_raw.columns]
    employees_raw.index = pd.to_datetime(employees_raw.index)

    # Calculate total employees - only for dates where ALL companies have data
    employees_complete = employees_raw.dropna(how='any')  # Only keep rows with all companies
    employees_total = employees_complete.sum(axis=1)
    employees_total.name = 'Total Employees'
    # Forward fill to get continuous series for rolling calculation
    if not employees_total.empty:
        employees_total_filled = employees_total.asfreq('D').ffill()
        employees_12m_change = employees_total_filled.diff(periods=365)  # Approximate 12 months
        employees_12m_change = employees_12m_change.dropna()
        employees_12m_change.name = '12M Change'
    else:
        employees_12m_change = pd.Series()

    # --- Fetch Margin Data (RR057 Gross Margin, RR243 Profit Margin) ---
    print("Fetching margin data (RR057, RR243)...")
    gross_margin_raw = blp.bdh(
        tickers=INDIA_IT_TICKERS,
        flds=['RR057'],
        start_date=INDIA_IT_START_DATE,
        end_date=INDIA_IT_END_DATE
    )
    if isinstance(gross_margin_raw.columns, pd.MultiIndex):
        gross_margin_raw.columns = [col[0] for col in gross_margin_raw.columns]
    gross_margin_raw.index = pd.to_datetime(gross_margin_raw.index)
    gross_margin_avg = gross_margin_raw.mean(axis=1).dropna()
    gross_margin_avg.name = 'Avg Gross Margin'

    profit_margin_raw = blp.bdh(
        tickers=INDIA_IT_TICKERS,
        flds=['RR243'],
        start_date=INDIA_IT_START_DATE,
        end_date=INDIA_IT_END_DATE
    )
    if isinstance(profit_margin_raw.columns, pd.MultiIndex):
        profit_margin_raw.columns = [col[0] for col in profit_margin_raw.columns]
    profit_margin_raw.index = pd.to_datetime(profit_margin_raw.index)
    profit_margin_avg = profit_margin_raw.mean(axis=1).dropna()
    profit_margin_avg.name = 'Avg Profit Margin'

    # --- Create the Chart ---
    print("Creating India IT Services chart...")
    fig, axes = plt.subplots(4, 1, figsize=(14, 20))
    ax1, ax2, ax3, ax4 = axes

    # === Subplot 1: Revenue Growth vs India Services Exports (same scale) ===
    if not revenue_growth_avg.empty:
        ax1.plot(revenue_growth_avg.index, revenue_growth_avg.values, color='tab:blue', linewidth=2,
                 label='IT Companies Avg Revenue Growth YoY%', marker='o', markersize=3)
    if not services_export_yoy.empty:
        ax1.plot(services_export_yoy.index, services_export_yoy.values, color='tab:orange', linewidth=2,
                 label='India Services Exports YoY%', linestyle='--')

    ax1.set_title("IT Companies Revenue Growth vs India Services Exports Growth (YoY%)", fontsize=12, fontweight='bold')
    ax1.set_ylabel("YoY Growth %")
    ax1.axhline(0, color='gray', linewidth=0.8, linestyle='--')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')
    ax1.legend(loc='upper left', fontsize=9)

    # Add latest values
    if not revenue_growth_avg.empty:
        last_rev_date = revenue_growth_avg.index[-1]
        last_rev_val = revenue_growth_avg.iloc[-1]
        ax1.annotate(f"{last_rev_val:.1f}%", xy=(last_rev_date, last_rev_val),
                     xytext=(5, 0), textcoords='offset points', fontsize=9, color='tab:blue',
                     bbox=dict(facecolor='white', edgecolor='tab:blue', alpha=0.8, boxstyle='round,pad=0.2'))
    if not services_export_yoy.empty:
        last_exp_date = services_export_yoy.index[-1]
        last_exp_val = services_export_yoy.iloc[-1]
        ax1.annotate(f"{last_exp_val:.1f}%", xy=(last_exp_date, last_exp_val),
                     xytext=(5, -15), textcoords='offset points', fontsize=9, color='tab:orange',
                     bbox=dict(facecolor='white', edgecolor='tab:orange', alpha=0.8, boxstyle='round,pad=0.2'))

    # === Subplot 2: Share Price Index vs NIFTY ===
    if not it_index.empty:
        ax2.plot(it_index.index, it_index.values, color='tab:blue', linewidth=2, label='IT Services Index')
    if not nifty_indexed.empty:
        ax2.plot(nifty_indexed.index, nifty_indexed.values, color='tab:green', linewidth=2, label='NIFTY Index')

    ax2_twin = ax2.twinx()
    if not relative_perf.empty:
        ax2_twin.plot(relative_perf.index, relative_perf.values, color='tab:red', linewidth=1.5,
                      label='IT vs NIFTY (Relative)', linestyle='--', alpha=0.7)
        ax2_twin.axhline(100, color='tab:red', linewidth=0.8, linestyle=':', alpha=0.5)

    ax2.set_title("IT Services Share Price Index vs NIFTY (Base=100 at common start)", fontsize=12, fontweight='bold')
    ax2.set_ylabel("Index Level", color='black')
    ax2_twin.set_ylabel("Relative Performance", color='tab:red')
    ax2_twin.tick_params(axis='y', labelcolor='tab:red')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')

    # Combined legend
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)

    # Add latest values
    if not it_index.empty:
        last_it_date = it_index.index[-1]
        last_it_val = it_index.iloc[-1]
        ax2.annotate(f"{last_it_val:.0f}", xy=(last_it_date, last_it_val),
                     xytext=(5, 5), textcoords='offset points', fontsize=9, color='tab:blue',
                     bbox=dict(facecolor='white', edgecolor='tab:blue', alpha=0.8, boxstyle='round,pad=0.2'))

    # === Subplot 3: Employee Count ===
    if not employees_total.empty:
        ax3.plot(employees_total.index, employees_total.values / 1000, color='tab:blue', linewidth=2,
                 label='Total Employees (thousands)', marker='o', markersize=3)

    ax3_twin = ax3.twinx()
    if not employees_12m_change.empty:
        # Resample to show cleaner 12m change data
        employees_12m_sampled = employees_12m_change.resample('Q').last().dropna()
        colors_emp = ['tab:green' if x >= 0 else 'tab:red' for x in employees_12m_sampled.values]
        ax3_twin.bar(employees_12m_sampled.index, employees_12m_sampled.values / 1000,
                     width=60, color=colors_emp, alpha=0.5, label='12M Change (thousands)')

    ax3.set_title("Total Employees (5 IT Companies)", fontsize=12, fontweight='bold')
    ax3.set_ylabel("Total Employees (thousands)", color='tab:blue')
    ax3_twin.set_ylabel("12M Change (thousands)", color='gray')
    ax3.tick_params(axis='y', labelcolor='tab:blue')
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_locator(mdates.YearLocator())
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')

    lines1, labels1 = ax3.get_legend_handles_labels()
    lines2, labels2 = ax3_twin.get_legend_handles_labels()
    ax3.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)

    # Add latest value
    if not employees_total.empty:
        last_emp_date = employees_total.index[-1]
        last_emp_val = employees_total.iloc[-1]
        ax3.annotate(f"{last_emp_val/1000:.0f}k", xy=(last_emp_date, last_emp_val/1000),
                     xytext=(5, 0), textcoords='offset points', fontsize=9, color='tab:blue',
                     bbox=dict(facecolor='white', edgecolor='tab:blue', alpha=0.8, boxstyle='round,pad=0.2'))

    # === Subplot 4: Margins (separate axes) ===
    ax4_twin = ax4.twinx()

    if not gross_margin_avg.empty:
        ax4.plot(gross_margin_avg.index, gross_margin_avg.values, color='tab:blue', linewidth=2,
                 label='Avg Gross Margin %', marker='o', markersize=3)
    if not profit_margin_avg.empty:
        ax4_twin.plot(profit_margin_avg.index, profit_margin_avg.values, color='tab:green', linewidth=2,
                      label='Avg Profit Margin %', marker='s', markersize=3)

    ax4.set_title("Average Gross Margin & Profit Margin", fontsize=12, fontweight='bold')
    ax4.set_ylabel("Gross Margin %", color='tab:blue')
    ax4_twin.set_ylabel("Profit Margin %", color='tab:green')
    ax4.tick_params(axis='y', labelcolor='tab:blue')
    ax4_twin.tick_params(axis='y', labelcolor='tab:green')
    ax4.grid(True, alpha=0.3)
    ax4.xaxis.set_major_locator(mdates.YearLocator())
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.setp(ax4.get_xticklabels(), rotation=45, ha='right')

    # Combined legend
    lines1, labels1 = ax4.get_legend_handles_labels()
    lines2, labels2 = ax4_twin.get_legend_handles_labels()
    ax4.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)

    # Add latest values
    if not gross_margin_avg.empty:
        last_gm_date = gross_margin_avg.index[-1]
        last_gm_val = gross_margin_avg.iloc[-1]
        ax4.annotate(f"{last_gm_val:.1f}%", xy=(last_gm_date, last_gm_val),
                     xytext=(5, 5), textcoords='offset points', fontsize=9, color='tab:blue',
                     bbox=dict(facecolor='white', edgecolor='tab:blue', alpha=0.8, boxstyle='round,pad=0.2'))
    if not profit_margin_avg.empty:
        last_pm_date = profit_margin_avg.index[-1]
        last_pm_val = profit_margin_avg.iloc[-1]
        ax4_twin.annotate(f"{last_pm_val:.1f}%", xy=(last_pm_date, last_pm_val),
                          xytext=(5, -10), textcoords='offset points', fontsize=9, color='tab:green',
                          bbox=dict(facecolor='white', edgecolor='tab:green', alpha=0.8, boxstyle='round,pad=0.2'))

    # Add overall title and last data annotation
    fig.suptitle("India IT Services Companies Analysis\n(TCS, Wipro, HCL Tech, Infosys, Cognizant)",
                 fontsize=14, fontweight='bold', y=0.995)

    # Find latest data date across all series
    all_dates = []
    for s in [revenue_growth_avg, services_export_yoy, it_index, employees_total, gross_margin_avg, profit_margin_avg]:
        if not s.empty:
            all_dates.append(s.index[-1])
    if all_dates:
        last_data_date = max(all_dates)
        fig.text(0.98, 0.99, f"Last data: {last_data_date.strftime('%d %b %Y')}", transform=fig.transFigure,
                 fontsize=10, ha='right', va='top',
                 bbox=dict(facecolor='white', edgecolor='gray', alpha=0.9, boxstyle='round,pad=0.3'))

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(INDIA_IT_OUTFILE, dpi=150, bbox_inches='tight')
    print(f"Saved India IT Services chart to: {INDIA_IT_OUTFILE}")
    plt.close()

    print("India IT Services chart generated successfully.")

except Exception as e:
    print(f"Error generating India IT Services chart: {e}")
    import traceback
    traceback.print_exc()
