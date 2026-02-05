from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FormatStrFormatter
from xbbg import blp

# ---------------- Inputs ----------------
TICKER_NEER = "CTSGSGD Index"      # S$NEER (daily)
TICKER_SORA = "SORACA3M Index"     # 3m compounded SORA (daily)

DLI_CSV_PATH = "mas_dli_from_excel.csv"  # from the Excel export step above

START_DATE = datetime(2011, 10, 1)  # at least 3 months before your first plotted month
END_DATE = datetime.today()

# Weights + scaling (as per Excel logic)
W_NEER = 0.6
W_SORA = 0.4
NEER_VAR_DIV = 2.0

# ---------------- Styling ----------------
C_DLI = "#4472C4"
C_PROXY = "#ED7D31"
C_NEER_BAR = "#70AD47"
C_SORA_BAR = "#FFC000"
C_ZERO = "black"

LINE_W = 2.5
BAR_WIDTH_DAYS = 25  # thicker/more noticeable bars

# ---------------- Helpers ----------------
def fetch_daily_bbg(tickers, start, end):
    df = blp.bdh(tickers, "PX_LAST", start, end)

    # flatten MultiIndex columns (xbbg sometimes returns MultiIndex)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.index = pd.to_datetime(df.index)
    return df


def to_monthly_bm_last(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Resample daily to business-month-end (last obs),
    then normalize index to month-start timestamps for clean labeling.
    """
    m = df_daily.resample("BM").last()
    m.index = m.index.to_period("M").to_timestamp()
    return m


def load_dli_csv(csv_path: str) -> pd.Series:
    d = pd.read_csv(csv_path, parse_dates=["date"])
    d["MAS_DLI_3m_change"] = pd.to_numeric(d["MAS_DLI_3m_change_pct"], errors="coerce")
    d = d.dropna(subset=["date", "MAS_DLI_3m_change"]).copy()

    d["date"] = d["date"].dt.to_period("M").dt.to_timestamp()
    d = d.set_index("date").sort_index()

    return d["MAS_DLI_3m_change"]


def stacked_two_series_excel_like(ax, x, a, b, label_a, label_b, color_a, color_b, width_days):
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


# ---------------- Calculations ----------------
def compute_proxy_3m(neer_m: pd.Series, sora_m: pd.Series) -> pd.DataFrame:
    out = pd.DataFrame({"NEER": neer_m, "SORA": sora_m}).copy()

    # 3m % change in NEER
    out["NEER_3m_pct"] = (out["NEER"] / out["NEER"].shift(3) - 1.0) * 100.0
    out["NEER_scaled_3m"] = out["NEER_3m_pct"] / NEER_VAR_DIV

    # 3m pp change in SORA
    out["SORA_3m_pp"] = out["SORA"] - out["SORA"].shift(3)

    # contributions + proxy
    out["NEER_contrib_3m"] = W_NEER * out["NEER_scaled_3m"]
    out["SORA_contrib_3m"] = W_SORA * out["SORA_3m_pp"]
    out["Proxy_SORA_3m"] = out["NEER_contrib_3m"] + out["SORA_contrib_3m"]

    return out


def compute_proxy_monthly_bc(neer_m: pd.Series, sora_m: pd.Series) -> pd.DataFrame:
    """
    Chart 3: monthly change proxy ('BC calculated' block in your sheet):
    - NEER leg uses monthly % change, but variance-scaled (/2) before weighting in the proxy
    - SORA leg uses monthly pp change
    """
    out = pd.DataFrame({"NEER": neer_m, "SORA": sora_m}).copy()

    # monthly % change NEER
    out["NEER_m_pct"] = (out["NEER"] / out["NEER"].shift(1) - 1.0) * 100.0

    # monthly pp change SORA
    out["SORA_m_pp"] = out["SORA"] - out["SORA"].shift(1)

    # contributions used in the proxy (NEER variance-scaled)
    out["NEER_contrib_m"] = W_NEER * (out["NEER_m_pct"] / NEER_VAR_DIV)
    out["SORA_contrib_m"] = W_SORA * out["SORA_m_pp"]
    out["Proxy_m"] = out["NEER_contrib_m"] + out["SORA_contrib_m"]

    return out


# ---------------- Plotting ----------------
def plot_chart1_dli_vs_proxy(df_all: pd.DataFrame):
    """
    Chart 1: MAS DLI vs Proxy (SORA only).
    Proxy extends to the latest Bloomberg month; DLI stops where it stops.
    """
    fig, ax = plt.subplots(figsize=(12, 5))

    # Plot proxy first so axis naturally extends to latest proxy date
    ax.plot(df_all.index, df_all["Proxy_SORA_3m"], color=C_PROXY, linewidth=LINE_W,
            label="Proxy (60% S$NEER & 40% SORA, variance scaled)")

    ax.plot(df_all.index, df_all["MAS_DLI_3m"], color=C_DLI, linewidth=LINE_W,
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


def plot_chart2_stacked_3m_with_lines(df_all: pd.DataFrame, start="2019-01-01"):
    """
    Chart 2: thick stacked bars (NEER + SORA contributions, 3m),
    plus MAS DLI and Proxy lines, plus black y=0 line.
    """
    d = df_all[df_all.index >= pd.Timestamp(start)].copy()

    fig, ax = plt.subplots(figsize=(12, 5))

    stacked_two_series_excel_like(
        ax=ax,
        x=d.index,
        a=d["NEER_contrib_3m"],
        b=d["SORA_contrib_3m"],
        label_a="S$NEER contribution",
        label_b="SORA contribution",
        color_a=C_NEER_BAR,
        color_b=C_SORA_BAR,
        width_days=BAR_WIDTH_DAYS,
    )

    # overlay lines
    ax.plot(d.index, d["MAS_DLI_3m"], color=C_DLI, linewidth=LINE_W, label="MAS DLI")
    ax.plot(d.index, d["Proxy_SORA_3m"], color=C_PROXY, linewidth=LINE_W,
            label="Proxy (60% S$NEER & 40% SORA, variance scaled)")

    ax.axhline(0, color=C_ZERO, linewidth=1.2)

    ax.set_title("MAS DLI and Proxy (change over three months)")
    ax.grid(True, axis="y", alpha=0.3)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_ylim(-1.0, 1.5)
    ax.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.40), frameon=False, ncol=1)
    plt.tight_layout()


def plot_chart3_monthly_bc(df_m: pd.DataFrame, start=None):
    """
    Chart 3: DLI Proxy (monthly change, BC calculated).
    Thick stacked bars (variance-scaled NEER contrib + SORA contrib), optional proxy line, black y=0 line.
    """
    d = df_m.copy()
    if start is not None:
        d = d[d.index >= pd.Timestamp(start)].copy()

    fig, ax = plt.subplots(figsize=(12, 5))

    stacked_two_series_excel_like(
        ax=ax,
        x=d.index,
        a=d["NEER_contrib_m"],
        b=d["SORA_contrib_m"],
        label_a="S$NEER contribution (monthly, variance scaled)",
        label_b="SORA contribution (monthly)",
        color_a=C_NEER_BAR,
        color_b=C_SORA_BAR,
        width_days=BAR_WIDTH_DAYS,
    )

    # optional proxy line (useful; comment out if you want bars-only)
    ax.plot(d.index, d["Proxy_m"], color=C_PROXY, linewidth=LINE_W, label="Proxy (monthly change)")

    ax.axhline(0, color=C_ZERO, linewidth=1.2)

    ax.set_title("DLI Proxy (monthly change, BC calculated)")
    ax.grid(True, axis="y", alpha=0.3)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%y"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.yaxis.set_major_formatter(FormatStrFormatter("%.1f"))
    # Let y autoscale; if you prefer fixed bounds, uncomment:
    # ax.set_ylim(-1.0, 1.5)

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.40), frameon=False, ncol=1)
    plt.tight_layout()


# ---------------- Run ----------------
def main():
    # 1) Load DLI (monthly index, already 3m change series) from CSV
    dli_3m = load_dli_csv(DLI_CSV_PATH)

    # 2) Pull daily from Bloomberg and convert to monthly
    daily = fetch_daily_bbg([TICKER_NEER, TICKER_SORA], START_DATE, END_DATE)
    m = to_monthly_bm_last(daily)

    # 3) Compute proxy (3m) & monthly proxy (BC)
    proxy_3m = compute_proxy_3m(m[TICKER_NEER], m[TICKER_SORA])
    proxy_m = compute_proxy_monthly_bc(m[TICKER_NEER], m[TICKER_SORA])

    # 4) Combine for chart 1 & 2.
    # IMPORTANT: use proxy index as the master index so proxy plots to latest even if DLI ends earlier.
    df_all = proxy_3m.join(dli_3m.rename("MAS_DLI_3m"), how="left")

    # drop early rows where 3m proxy can't be computed yet
    df_all = df_all.dropna(subset=["Proxy_SORA_3m", "NEER_contrib_3m", "SORA_contrib_3m"], how="any")

    # 5) Plot charts
    plot_chart1_dli_vs_proxy(df_all)
    plot_chart2_stacked_3m_with_lines(df_all, start="2019-01-01")

    # For chart 3, use available monthly data (set start=None to use all)
    proxy_m = proxy_m.dropna(subset=["Proxy_m", "NEER_contrib_m", "SORA_contrib_m"])
    plot_chart3_monthly_bc(proxy_m, start=None)

    plt.show()


if __name__ == "__main__":
    main()
