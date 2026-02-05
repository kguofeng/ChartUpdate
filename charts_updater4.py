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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.colors import TwoSlopeNorm
from xbbg import blp

G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = Path(r"O:\Tian\Portal\Charts\ChartDataBase")  # general chart directory
FONTSIZE = 14

# ETF flows to US and Europe equity
START_DATE = datetime.strptime("01/01/18", "%d/%m/%y")
us_etfs = ['SPY US Equity', 'VTI US Equity', 'QQQ US Equity', 'IVV US Equity', 'IWM US Equity']#, 'SPLG US Equity']
eu_etfs = ['EZU US Equity', 'EZU UP Equity', 'IEUR US Equity', 'IEUR UP Equity',
           'FEZ US Equity', 'FEZ UP Equity', 'VGK US Equity', 'VGK UP Equity',
           'HEDJ US Equity', 'HEDJ UP Equity', 'FEP US Equity', 'FEP UP Equity']
fields = ['FUND_FLOW', 'FUND_TOTAL_ASSETS']


def get_data(etfs):
    df = blp.bdh(etfs, fields, START_DATE).dropna()
    df.columns.set_names(['Ticker', 'Field'], inplace=True)
    return df.swaplevel(0, 1, axis=1)  # Put Field first


us_data = get_data(us_etfs)
eu_data = get_data(eu_etfs)


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


us_combined = us_data.copy()
eu_combined = combine_us_up(eu_data)


def compute_normalized_flow(df):
    flow = df['FUND_FLOW']
    assets = df['FUND_TOTAL_ASSETS']
    return flow / assets


us_norm = compute_normalized_flow(us_combined)
eu_norm = compute_normalized_flow(eu_combined)

# === Aggregate and Drop Last Day ===
us_norm_avg = us_norm.mean(axis=1).iloc[:-1]
eu_norm_avg = eu_norm.mean(axis=1).iloc[:-1]
us_total = us_combined['FUND_FLOW'].sum(axis=1).iloc[:-1]
eu_total = eu_combined['FUND_FLOW'].sum(axis=1).iloc[:-1]

# === Rolling 3M Sum ===
us_3m_norm = us_norm_avg.rolling(window=63).sum()
eu_3m_norm = eu_norm_avg.rolling(window=63).sum()
us_3m_total = us_total.rolling(window=63).sum()
eu_3m_total = eu_total.rolling(window=63).sum()


# === Rolling Z-Score (3Y = 756 trading days) ===
def rolling_zscore(series, window=252 * 3):
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mean) / std


us_z = rolling_zscore(us_3m_norm)
eu_z = rolling_zscore(eu_3m_norm)

z_start = us_z.dropna().index[0]
last_date_str = us_3m_total.index[-1].strftime('%Y-%m-%d')

# === Plotting ===
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 6))

# --- Chart 1: Z-Score Divergence ---
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

# --- Chart 2: Total Flows with Dual Axis ---
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



# Case-Shiller 20-City Composite Home Prices YoY% (Advanced 18 months) vs Owner’s Equivalent Rent (OER) YoY%
START_DATE = datetime(2002, 2, 1)
END_DATE = datetime.today()
tickers = ['SPCS20Y% Index',  # Case-Shiller 20-City Composite Home Prices YoY%
           'CPRHOERY Index']  # Owner’s Equivalent Rent (OER) YoY%
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
del all_data, case, case_adv, oer, ax, ax2, fig


# OPEC vs Non-OPEC Oil Supply YoY% Change
START_DATE = datetime(2002, 1, 1)
END_DATE   = datetime.today()
tickers = [
    'ST14WO Index',    # World Crude Oil & Liquid Fuels Production (million bbl/day)
    'OPCRTOTL Index'   # OPEC Crude Oil Production (thousand bbl/day)
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
del all_data, valid, fig, ax


## BIS Non-Financial Debt Service Ratios
START_DATE = datetime(2002, 1, 1)
END_DATE   = datetime.today()
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
del all_data, latest_series, latest_vals, left_codes, right_codes, tickers, left_countries, right_countries, fig, ax1, ax2, ax3, s, line, bars


# China Commodity Prices
names = {
    'CIOSHEBE Index': "Iron Ore Hebei/Tangshan",
    'CEFWOPCT Index': "Portland Cement (Bulk)",
    'CDSPHRAV Index': "Steel (Hot Rolled Sheet)",
    'CHNCBRPR Index': "Poly-Butadiene Rubber",
    'CCKPTANG Index': "Met Coal Grade 1"
}
tickers = list(names.keys())
START_DATE = datetime(2002, 1, 1)
END_DATE   = datetime.today()
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
                               gridspec_kw={'height_ratios':[2,2],'hspace':0.3})
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
plt.tight_layout(rect=[0,0,1,0.90])
plt.savefig(Path(G_CHART_DIR, "China_Commodities_Prices_Trend.png"), bbox_inches='tight')
del raw, data, indexed, z, composite


# REER Deviations from LT avg
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
END_DATE   = datetime.today()
tickers = list(all_map.keys())
data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
if isinstance(data.columns, pd.MultiIndex):
    data.columns = data.columns.get_level_values(0)
data.index = pd.to_datetime(data.index)
last_date = data.index[-1]
window = 252*10
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
cell_colors = [['white']*4]
for _, row in tbl.iterrows():
    cell_colors.append(['white', 'white', cmap(norm_z(row["Z-Score"])), cmap(norm_dev(row["Abs Dev"]))])
cell_text = [tbl.columns.tolist()]
for _, row in tbl.iterrows():
    cell_text.append([
        row["Country"], f"{row['10Y Avg']:.2f}", f"{row['Z-Score']:.2f}", f"{row['Abs Dev']:.2f}"])
table = ax_table.table( cellText=cell_text, cellColours=cell_colors, cellLoc='center',loc='center')
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 1.5)
fig.subplots_adjust(left=0.05, right=0.97, top=0.88, bottom=0.05, wspace=0.2, hspace=0.3)
plt.tight_layout(rect=[0, 0, 1, 0.94])
plt.savefig(Path(G_CHART_DIR, "REER_LT_Deviation.png"), bbox_inches='tight')
del data, avg10, zscore, rolling_mean, rolling_std, latest_vals, abs_dev, tbl
del fig, ax_abs_dm, ax_abs_asia, ax_abs_oth, ax_z_dm, ax_z_asia, ax_z_oth, ax_table


# 1. Define date range and tickers
START_DATE = datetime(2002, 1, 1)
END_DATE   = datetime.today()
tickers = [
    'IDLPTOTL Index',   # Bank Loans (IDR bn)
    'IDGRP Index'       # Nominal GDP (IDR bn)
]

# 2. Pull and clean data
data = blp.bdh(tickers, "PX_LAST", START_DATE, END_DATE)
if isinstance(data.columns, pd.MultiIndex):
    data.columns = data.columns.get_level_values(0)
data.index = pd.to_datetime(data.index)
data = data.ffill(limit=2)

# 3. Compute Loans/GDP ratio (%) and YoY change (p.p.)
ratio = data['IDLPTOTL Index'] / data['IDGRP Index'] * 100
speed = ratio.diff(12)

# 4. Plot on separate y-axes
fig, ax1 = plt.subplots(figsize=(12, 6))
ax2 = ax1.twinx()

# – Plot level of Loans/GDP ratio on left axis
ln1, = ax1.plot(
    ratio.index, ratio,
    color='tab:orange', alpha=0.5, lw=2,
    label="Loans/GDP Ratio (%)"
)
ax1.set_ylabel("Loans/GDP Ratio (%)", color='tab:orange', fontsize=12)
ax1.tick_params(axis='y', labelcolor='tab:orange')

# – Plot YoY change on right axis
ln2, = ax2.plot(
    speed.index, speed,
    color='tab:blue', lw=2,
    label="Δ Loans/GDP (YoY, p.p.)"
)
ax2.set_ylabel("Δ Loans/GDP (p.p.)", color='tab:blue', fontsize=12)
ax2.tick_params(axis='y', labelcolor='tab:blue')

# – Zero line for YoY change
ax2.axhline(0, color='black', linewidth=1, linestyle='--')

# 5. Annotate last data points
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

# 6. Last-date box
fig.text(
    0.98, 0.95,
    f"Last data: {last_ratio_date.date()}",
    ha='right', va='top',
    fontsize=10,
    bbox=dict(facecolor='white', edgecolor='black', boxstyle='round')
)

# 7. Titles, labels, and formatting
ax1.set_title("Indonesia: Bank Loans as % of GDP – Level and YoY Change", fontsize=14)
ax1.set_xlabel("Date", fontsize=12)
ax1.xaxis.set_major_locator(mdates.YearLocator())
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
plt.setp(ax1.get_xticklabels(), rotation=45)

# 8. Combined legend
handles = [ln1, ln2]
labels  = [ln.get_label() for ln in handles]
ax1.legend(handles, labels, loc="upper left", fontsize=10)

# 9. Grid
ax1.grid(True, linestyle=':', alpha=0.6)
ax2.grid(False)

# 10. Save
plt.tight_layout()

plt.savefig(Path(G_CHART_DIR, "IDR_TotalLoans_to_GDP_YoY.png"), bbox_inches='tight')
# Clean up
del data, ratio, speed, fig, ax1, ax2


# # 1. Map each index to its CPI ticker
# indices = {
#     'SPX Index':   'CPURNSA Index',    # US CPI
#     'NDQ Index':   'CPURNSA Index',    # US CPI
#     'RTY Index':   'CPURNSA Index',    # US CPI
#     'SX7E Index':  'CPALEMU Index',    # Euro Area CPI
#     'HSI Index':   'ECOPCNN Index',    # China CPI
#     'NIFTY Index': 'INFUTOT Index'     # India CPI
# }

# START_DATE = datetime(2002, 1, 1)
# END_DATE   = datetime.today()

# capes = {}
# stats = {}

# for idx_name, cpi_tkr in indices.items():
#     # 2a. Pull daily data
#     df_price = blp.bdh([idx_name],   "PX_LAST",  START_DATE, END_DATE)
#     df_eps   = blp.bdh([idx_name],   "TRAIL_12M_EPS", START_DATE, END_DATE)
#     df_cpi   = blp.bdh([cpi_tkr],    "PX_LAST",  START_DATE, END_DATE)

#     # 2b. Flatten columns & datetime index
#     for df in (df_price, df_eps, df_cpi):
#         if isinstance(df.columns, pd.MultiIndex):
#             df.columns = df.columns.get_level_values(0)
#         df.index = pd.to_datetime(df.index)

#     # 2c. Resample to month-end
#     price_m = df_price[idx_name].resample('M').last()
#     eps_m   = df_eps[idx_name].resample('M').last()
#     cpi_m   = df_cpi[cpi_tkr].resample('M').last()

#     # 2d. Forward-fill any occasional gaps
#     price_m = price_m.ffill(limit=2)
#     eps_m   = eps_m.ffill(limit=2)
#     cpi_m   = cpi_m.ffill(limit=2)

#     # 3. Inflation-adjust earnings to today's dollars
#     current_cpi = cpi_m.iloc[-1]
#     eps_real    = eps_m * (current_cpi / cpi_m)

#     # 4. Compute 10-year rolling average of real EPS (120 months)
#     eps_ma10y = eps_real.rolling(window=120, min_periods=120).mean()

#     # 5. Compute Shiller CAPE and drop NaNs
#     cape = (price_m / eps_ma10y).dropna()
#     capes[idx_name] = cape

#     # 6. Compute stats
#     stats[idx_name] = {'mean': cape.mean(), 'std': cape.std()}


# # 7. Plot all six in a 2×3 grid
# fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharex=False)
# axes = axes.flatten()

# for ax, idx_name in zip(axes, indices.keys()):
#     cape = capes[idx_name]
#     mu  = stats[idx_name]['mean']
#     sd  = stats[idx_name]['std']

#     ax.plot(cape.index, cape, label="CAPE", lw=2)
#     ax.axhline(mu,    color='black', lw=1, label="Mean")
#     ax.axhline(mu+sd, color='gray',  lw=1, ls='--', label="+1 σ")
#     ax.axhline(mu-sd, color='gray',  lw=1, ls='--', label="–1 σ")

#     ax.set_title(idx_name, fontsize=12)
#     ax.xaxis.set_major_locator(mdates.YearLocator(5))
#     ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
#     ax.tick_params(axis='x', rotation=45)
#     ax.grid(True, linestyle=':', alpha=0.6)

#     # only show legend on the first subplot
#     if idx_name == list(indices.keys())[0]:
#         ax.legend(loc="upper left", fontsize=8)

# fig.suptitle("Shiller CAPE for Major Global Indices", fontsize=16, y=0.98)
# fig.tight_layout(rect=[0, 0, 1, 0.95])
# fig.savefig(Path(G_CHART_DIR, "Shiller_CAPE.png"), bbox_inches='tight')
# del capes, stats, fig, axes

# 1. Define indices
indices = [
    'SPX Index', 'NDQ Index', 'SX7E Index',
    'HSI Index', 'SHSZ300 Index', 'TWSE Index',
    'NKY Index', 'NIFTY Index', 'KOSPI Index'
]

# 2. Date range
START_DATE = datetime(2015, 1, 1)
END_DATE   = datetime.today()

# 3. Fetch and prepare data
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
    # Rolling 6-month (~126 trading days) YoY change
    df['P/E Change']   = (df['BEST_PE_RATIO'].pct_change(126) * 100).rolling(10).mean()
    df['Price Change'] = (df['PX_LAST'].pct_change(126) * 100).rolling(10).mean()
    df['EPS Change']   = (df['BEST_EPS'].pct_change(126) * 100).rolling(10).mean()
    data_dict[idx] = df
    last_dates.append(df.index.max())

last_date = min(last_dates)

# 4. Plot aesthetics and layout
fig, axes = plt.subplots(3, 3, figsize=(24, 20), sharex=True)
axes = axes.flatten()

for ax in axes:
    # clean background and grids
    ax.set_facecolor('#f7f7f7')
    ax.grid(True, linestyle='--', linewidth=0.6, alpha=0.5)
    # remove top/right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

for ax, idx in zip(axes, indices):
    df = data_dict[idx]
    # plot lines with distinct colors and width
    ln1, = ax.plot(df.index, df['P/E Change'],   color='blue', lw=2.5, label='6m P/E Change')
    ln2, = ax.plot(df.index, df['Price Change'], color='red',   lw=2.5, label='6m Price Change')
    ln3, = ax.plot(df.index, df['EPS Change'],   color='green', lw=2.5, label='6m EPS Change')
    # zero line
    ax.axhline(0, color='gray', lw=1, linestyle=':')
    # annotate last values
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

    # title and y-label
    ax.set_title(idx, fontsize=18, pad=12)
    ax.set_ylabel('Change (%)', fontsize=14)
    # x-axis ticks only on bottom row
    if ax.get_subplotspec().is_last_row():
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45)
            lbl.set_fontsize(12)
    else:
        ax.set_xticklabels([])

# global legend on first subplot
axes[0].legend(fontsize=14, loc='upper left')

# main title and last-date box
fig.suptitle('Rolling 6 Month Change in Fwd P/E, Price, and Fwd EPS (10d MA for all)', fontsize=26, y=0.96)
fig.text(0.98, 0.92, f'Last data: {last_date.date()}', ha='right', va='top',
         fontsize=18, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

plt.tight_layout(rect=[0, 0, 1, 0.93])
plt.savefig(Path(G_CHART_DIR, 'Equities_PE_EPS_vs_Price.png'), bbox_inches='tight')


### BI Liquidity
START_DATE = datetime(2017, 1, 1)
END_DATE   = datetime.today()

volume_tickers = [
    'IFSW1M Index','IFSW3M Index','IFSW6M Index','IFSW12M Index',
    'IDFB1MTH Index','IDFB1MPM Index','IDFB3MTH Index','IDFB3MPM Index',
    'IDFB1MRP Index','USTDONUS Index','USTD1WUS Index','USTD2WUS Index',
    'USTD1MUS Index','USTD3MUS Index','BITDS1W Index','BITDS2W Index',
    'BITDS1M Index','BITDS3M Index']
price_tickers = [
#    'YOUR_SPOT_TICKER','YOUR_FORWARD_TICKER',
    'SBI9SBA6 Index','SBI9SBA9 Index',
    'IDTV1MAC Index','IDTV3MAC Index','IDTV6MAC Index','IDTV12MAC Index',
    'IDTU1MAC Index','IDTU3MAC Index']
raw_vol = blp.bdh(volume_tickers, 'PR013', START_DATE, END_DATE)
raw_px  = blp.bdh(price_tickers,  'PX_LAST', START_DATE, END_DATE)
vols = raw_vol.xs('PR013', axis=1, level=1) if isinstance(raw_vol.columns, pd.MultiIndex) else raw_vol.copy()
pxs  = raw_px.xs('PX_LAST', axis=1, level=1) if isinstance(raw_px.columns, pd.MultiIndex) else raw_px.copy()
flows = pd.concat([vols, pxs], axis=1)
flows.index = pd.to_datetime(flows.index)
flows = flows.ffill(limit=1)
flow_map = {
    'IFSW1M Index':'Swap 1M','IFSW3M Index':'Swap 3M','IFSW6M Index':'Swap 6M','IFSW12M Index':'Swap 12M',
    'IDFB1MTH Index':'DNDF 1M AM','IDFB1MPM Index':'DNDF 1M PM',
    'IDFB3MTH Index':'DNDF 3M AM','IDFB3MPM Index':'DNDF 3M PM','IDFB1MRP Index':'DNDF Rollover',
    'USTDONUS Index':'FX TD O/N–3d','USTD1WUS Index':'FX TD 1W','USTD2WUS Index':'FX TD 2W',
    'USTD1MUS Index':'FX TD 1M','USTD3MUS Index':'FX TD 3M',
    'BITDS1W Index':'Shariah FX TD 1W','BITDS2W Index':'Shariah FX TD 2W',
    'BITDS1M Index':'Shariah FX TD 1M','BITDS3M Index':'Shariah FX TD 3M',
    # 'SPOT_TICKER':'Spot','FORWARD_TICKER':'Forward',
    'SBI9SBA6 Index':'SBBI 6M','SBI9SBA9 Index':'SBBI 9M',
    'IDTV1MAC Index':'SVBI 1M','IDTV3MAC Index':'SVBI 3M',
    'IDTV6MAC Index':'SVBI 6M','IDTV12MAC Index':'SVBI 12M',
    'IDTU1MAC Index':'SUVBI 1M','IDTU3MAC Index':'SUVBI 3M'}
flows.rename(columns=flow_map, inplace=True)
flows['DNDF 1M'] = flows['DNDF 1M AM'].fillna(0) + flows['DNDF 1M PM'].fillna(0)
flows['DNDF 3M'] = flows['DNDF 3M AM'].fillna(0) + flows['DNDF 3M PM'].fillna(0)
windows_fx = {
 #   'Spot':1,'Forward':1,
    'Swap 1M':30,'Swap 3M':90,'Swap 6M':180,'Swap 12M':365,
    'DNDF 1M':30,'DNDF 3M':90,'DNDF Rollover':30,
    'FX TD O/N–3d':3,'FX TD 1W':7,'FX TD 2W':14,'FX TD 1M':30,'FX TD 3M':90,
    'Shariah FX TD 1W':7,'Shariah FX TD 2W':14,'Shariah FX TD 1M':30,'Shariah FX TD 3M':90,
    'SBBI 6M':180,'SBBI 9M':270,
    'SVBI 1M':30,'SVBI 3M':90,'SVBI 6M':180,'SVBI 12M':365,
    'SUVBI 1M':30,'SUVBI 3M':90
}

stock_fx = pd.DataFrame(index=flows.index)
for comp, wnd in windows_fx.items():
    if comp in flows:
        stock_fx[comp] = flows[comp].rolling(wnd, min_periods=1).sum()

agg_fx = pd.DataFrame(index=stock_fx.index)
#agg_fx['Spot & Forward']    = stock_fx[['Spot','Forward']].sum(axis=1)
agg_fx['Swaps']             = stock_fx[[c for c in stock_fx if c.startswith('Swap')]].sum(axis=1)
agg_fx['DNDF']              = stock_fx[['DNDF 1M','DNDF 3M','DNDF Rollover']].sum(axis=1)/1e6
agg_fx['FX Term Deposits']  = stock_fx[[c for c in stock_fx if c.startswith('FX TD')]].sum(axis=1)
agg_fx['Shariah FX TD']     = stock_fx[[c for c in stock_fx if c.startswith('Shariah FX TD')]].sum(axis=1)
agg_fx['SBBI Valas']        = stock_fx[['SBBI 6M','SBBI 9M']].sum(axis=1)
agg_fx['SVBI']              = stock_fx[[c for c in stock_fx if c.startswith('SVBI')]].sum(axis=1)/1e3
agg_fx['SUVBI']             = stock_fx[[c for c in stock_fx if c.startswith('SUVBI')]].sum(axis=1)/1e3

agg_fx = agg_fx[agg_fx.index >= START_DATE]
conv_tix = {
    'BILPSBIT Index':'SBI Issuance','BILPSDBI Index':'SDBI Issuance',
    'BILPSRBI Index':'SRBI Issuance','BILPRRPO Index':'Conventional Reverse Repo',
    'BILPBILP Index':'Conventional Repo','BILPTDPO Index':'Conventional Term Deposit',
    'BILPDFVL Index':'Deposit Facility','BILPLDFC Index':'Lending Facility'}
raw_conv = blp.bdh(list(conv_tix.keys()), 'PX_LAST', START_DATE, END_DATE)
stock_conv = raw_conv.xs('PX_LAST', axis=1, level=1) if isinstance(raw_conv.columns, pd.MultiIndex) else raw_conv.copy()
stock_conv.rename(columns=conv_tix, inplace=True)
stock_conv = stock_conv.sort_index().ffill(limit=1)
stock_conv['Conventional Repo']  *= -1
stock_conv['Lending Facility']   *= -1
cols = [c for c in stock_conv.columns if c != 'SRBI Issuance'] + ['SRBI Issuance']
stock_conv = stock_conv[cols]
pas_tix = {
    'BIRVSA1W Index':'PasBI 1W','BIRVSA2W Index':'PasBI 2W',
    'BIRVSA1M Index':'PasBI 1M','BIRVSA3M Index':'PasBI 3M'}
raw_pas = blp.bdh(list(pas_tix.keys()), 'PR013', START_DATE, END_DATE)
vols_pas = raw_pas.xs('PR013', axis=1, level=1) if isinstance(raw_pas.columns, pd.MultiIndex) else raw_pas.copy()
vols_pas.rename(columns=pas_tix, inplace=True)
vols_pas = vols_pas.sort_index().ffill(limit=1)
win_pas = {'PasBI 1W':7,'PasBI 2W':14,'PasBI 1M':30,'PasBI 3M':90}
stock_pas = pd.DataFrame({n: vols_pas[n].rolling(w, min_periods=1).sum() for n,w in win_pas.items()})
stock_pas['PaSBI'] = stock_pas.sum(axis=1)
stock_pas = stock_pas[['PaSBI']]
sh_tix = {
    'BILPSBIS Index':'SBIS Issuance','BILPSKBI Index':'SUKBI Issuance',
    'BILPRRSB Index':'Sharia Reverse Repo','BILPRPSB Index':'Sharia Repo',
    'BILPDFVS Index':'Deposit Facility Syariah','BILPLDFS Index':'Lending Facility Syariah'}
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
    (stock_conv,  "Conventional Domestic", "IDR Billion"),
    (stock_sharia,"Shariah Domestic", "IDR Billion"),
    (agg_fx,      "Foreign Ccy", "USD Million"),]
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

## Westpac Asia positive surprise index vs Asia equities (CN, HK, TW, SK, SG) (local ccy mkt cap weighted, natural logged)
indices = [
    'HSI Index', 'SHSZ300 Index', 'TWSE Index',
    'STI Index', 'KOSPI Index'
]
westpac_ticker = "WSURASIP Index"
START_DATE = datetime(2010, 1, 1)
END_DATE   = datetime.today()
raw = blp.bdh(
    indices,
    ["PX_LAST", "INDX_MARKET_CAP"],
    START_DATE,
    END_DATE, currency='USD'
)
raw.columns.names = ["ticker", "field"]
raw.index = pd.to_datetime(raw.index)

prices_m  = (
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

# ## Equity Dispersion/Correlation
# START_DATE = datetime(2015, 1, 1)
# END_DATE   = datetime.today()
# tickers = ['DSPX Index', 'SPX 3M RC BVOL Index', 'SPX 3M IC BVOL Index']
# data  = blp.bdh(tickers, "PX_LAST", START_DATE,END_DATE)

###  BSP Liquidity
names = {
    'PHRMSDA Index':  'Overnight Deposit Facility',
    'PHRRPO Index':   'Overnight Lending Facility',   # <- will be truncated to start after 2018-10-31
    'PHRMTDF Index':  'Term Deposit Facility',
    'PHRMRRF Index':  'RRP Facility',                 # <- will receive pre-2018-10-31 Lending values
    'PHRMBSPS Index': 'BSP Bills',
    'PHRMPRD Index':  'Peso Rediscounting',}
tickers = list(names.keys())
START_DATE = datetime(2011, 1, 1)
END_DATE   = datetime.today()
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
del raw, df, df_flip, df_plot, total, fig, ax, handles, labels, last_total_val, last_valid_date, cols, colors, mask_pre, olf_name


### India Bond FLows
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
    '.IN10S30S Index':     'IGB 10s–30s Curve',
}

groups  = list(group_map.keys())
volumes = list(volume_map.keys())
yields_ = list(yields_map.keys())

# -------------------------------
# Helpers
# -------------------------------
def bdh_flat(tickers, field, start_dt, end_dt):
    """Bloomberg BDH → wide DataFrame with ticker columns."""
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
    # Pull
    g = bdh_flat(groups,  "PX_LAST", start_dt, end_dt).ffill(limit=2).rename(columns=group_map)
    v = bdh_flat(volumes, "PX_LAST", start_dt, end_dt).ffill(limit=2).rename(columns=volume_map)
    y = bdh_flat(yields_, "PX_LAST", start_dt, end_dt).ffill(limit=2).rename(columns=yields_map)

    # Ensure numeric
    g = g.apply(pd.to_numeric, errors='coerce')
    v = v.apply(pd.to_numeric, errors='coerce')
    y = y.apply(pd.to_numeric, errors='coerce')

    # Rolling MA for top two panels
    g_ma = g.rolling(window=ma_window, min_periods=1).mean()
    v_ma = v.rolling(window=ma_window, min_periods=1).mean()

    g_total = g_ma.sum(axis=1, min_count=1)

    # Raw yields
    y10   = y['India 10Y IGB']
    bs10  = y['IN 10Y Bond-Swap']
    curve = y['IGB 10s–30s Curve']

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

# -------------------------------
# Date ranges
# -------------------------------
END_DATE = datetime.today()
START_LONG = datetime(2015, 1, 1)
START_RECENT = END_DATE - relativedelta(months=6)

left  = prepare_panels(START_LONG, END_DATE, ma_window=20)  # 20d MA
right = prepare_panels(START_RECENT, END_DATE, ma_window=5) # 5d MA

# -------------------------------
# Plot (3 rows × 2 cols)
# -------------------------------
fig, axes = plt.subplots(3, 2, figsize=(22, 14), sharex=False)
(ax_top_L, ax_top_R), (ax_mid_L, ax_mid_R), (ax_bot_L, ax_bot_R) = axes

def style_axis(ax):
    ax.grid(True, which='major', linestyle=':', linewidth=0.6, color='#dddddd', alpha=0.9)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.tick_params(axis='x', rotation=45, labelsize=9)

# ----- Top row: Net activity by group -----
for ax, data, title_suffix in [
    (ax_top_L, left,  "— 20d MA (since 2015)"),
    (ax_top_R, right, "— 5d MA (last 6 months)")
]:
    g_ma = data["groups_ma"]
    # lines per group
    for col in g_ma.columns:
        ax.plot(g_ma.index, g_ma[col], lw=1.6, alpha=0.95, label=col, zorder=2)
    # total in thick black
    ax.plot(data["groups_total"].index, data["groups_total"], color='black', lw=3.0,
            label=f"Total ({'20d' if ax is ax_top_L else '5d'} MA)", zorder=3)
    ax.axhline(0, color='#999999', lw=1.0, ls='--', zorder=1)
    ax.set_title(f"Net Activity by Group {title_suffix}", fontsize=13)
    ax.set_ylabel("Amount")
    ax.legend(ncol=3, fontsize=8, loc='upper left')
    style_axis(ax)
    # last-date box (per column)
    ax.text(0.985, 0.01, f"Last: {data['last_date'].date()}",
            transform=ax.transAxes, ha='right', va='bottom',
            fontsize=9, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

# ----- Middle row: Volumes by product -----
for ax, data, title_suffix in [
    (ax_mid_L, left,  "— 20d MA (since 2015)"),
    (ax_mid_R, right, "— 5d MA (last 6 months)")
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

# ----- Bottom row: Yields (raw, separate y-axes) -----
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
    ln3, = ax_r2.plot(data["curve"].index, data["curve"], color='tab:green', lw=1.8, ls='--', label='IGB 10s–30s Curve')
    ax_r2.set_ylabel('10s–30s Curve', color='tab:green')
    ax_r2.tick_params(axis='y', labelcolor='tab:green')

    ax.set_title(f"India Rates {title_suffix}", fontsize=13)
    style_axis(ax)

    # unified legend
    lines = [ln1, ln2, ln3]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=8)

plot_yields(ax_bot_L, left,  "— raw (since 2015)")
plot_yields(ax_bot_R, right, "— raw (last 6 months)")

# Shared x-labels on bottom row
ax_bot_L.set_xlabel("Date")
ax_bot_R.set_xlabel("Date")

# Figure title + save
fig.suptitle("India CCIL — Net Activity & Volumes (MA variants) + India Rates", fontsize=16, y=0.995)
plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig(Path(G_CHART_DIR, "India Bonds Activity & Volumes.png"), bbox_inches='tight')
del group_map, volume_map, yields_map, groups, volumes, yields_, left, right, axes, fig


# ## Rolling 5d 4hourly correlation (XAUUSD vs THB+1M)
lookback_days   = 120                 
ref_ticker      = 'ES1 Index'         # use ES session as reference (23h)
gold_tkr        = 'XAUUSD Curncy'
thb_tkr       = 'THB BGN Curncy'
dxy_tkr       = 'BBDXY Index'
block_minutes   = 240                 # sample every N minutes (last tick in each block)
ROLL_WIN_BLOCKS = 30                  # rolling window in "blocks" (e.g., ~10 ref-days if 6 blocks/day)
STANDARDIZE_BETA = False              # if True, beta reduces to correlation (both z-scored)

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
    r = pair.iloc[:,0].rolling(window, min_periods=window).corr(pair.iloc[:,1])
    r.name = 'rolling_corr'
    return r.dropna()

def rolling_partial_corr_xyz(df_xy_z, xcol, ycol, zcol, window):
    """Partial corr of x & y controlling for z using the closed-form 3-var formula."""
    out_vals, out_idx = [], []
    arr = df_xy_z[[xcol, ycol, zcol]].dropna()
    for i in range(window, len(arr)+1):
        w = arr.iloc[i-window:i]
        c = w.corr()
        r_xy, r_xz, r_yz = c.loc[xcol, ycol], c.loc[xcol, zcol], c.loc[ycol, zcol]
        denom = np.sqrt(max(1e-12, (1 - r_xz**2)) * max(1e-12, (1 - r_yz**2)))
        pc = (r_xy - r_xz * r_yz) / denom
        out_vals.append(pc)
        out_idx.append(w.index[-1])
    return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name=f'pcorr_{xcol}_{ycol}_|_{zcol}')

def rolling_beta(y_on_x_df, xcol, ycol, window, standardize=False):
    """OLS slope of y on x over a rolling window."""
    arr = y_on_x_df[[xcol, ycol]].dropna()
    if standardize:
        arr = (arr - arr.rolling(window, min_periods=window).mean()) / \
              arr.rolling(window, min_periods=window).std(ddof=0)
        arr = arr.dropna()
    out_vals, out_idx = [], []
    for i in range(window, len(arr)+1):
        w = arr.iloc[i-window:i]
        x = w[xcol]; y = w[ycol]
        vx = x.var()
        beta = (x.cov(y) / vx) if vx > 0 else np.nan
        out_vals.append(beta)
        out_idx.append(w.index[-1])
    return pd.Series(out_vals, index=pd.DatetimeIndex(out_idx), name=f'beta_{ycol}_on_{xcol}')

def _last_plotted_timestamp(*series):
    # Get the max timestamp among non-empty series; ensure tz-aware UTC
    valid = [s.index.max() for s in series if len(s)]
    if not valid:
        return None
    ts = max(valid)
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        ts = ts.tz_localize('UTC')
    return ts
gold_min = pull_intraday_minutes_last_n_days(gold_tkr, lookback_days, ref=ref_ticker)
thb_min = pull_intraday_minutes_last_n_days(thb_tkr, lookback_days, ref=ref_ticker)
dxy_min = pull_intraday_minutes_last_n_days(dxy_tkr, lookback_days, ref=ref_ticker)

# 2) Align gold & THB(+1M) on minute grid with short ffill, then add BBDXY the same way
px_min_2 = align_minutes(gold_min, thb_min, method='ffill')

### Align with DXY
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
roll_beta       = rolling_beta(ret_blk, xcol='XAUUSD', ycol='THB',
                               window=ROLL_WIN_BLOCKS, standardize=STANDARDIZE_BETA)
last_ts = _last_plotted_timestamp(roll_corr, roll_pcorr_usd, roll_beta)
last_sgt = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M %Z') if last_ts is not None else 'n/a'
last_updated_text = f"Last Updated:{last_sgt} SGT"
fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
axes[0].plot(roll_corr.index, roll_corr.values, label=f'Rolling corr')
axes[0].axhline(0, lw=1)
axes[0].set_title(f'5-day Rolling Corr (4H data): {gold_tkr} vs {thb_tkr}')
axes[0].legend(loc='upper right')
axes[0].grid(True, alpha=0.3)
axes[1].plot(roll_pcorr_usd.index, roll_pcorr_usd.values,
             label=f'USD-neutral partial corr', color = 'orange')
axes[1].axhline(0, lw=1)
axes[1].set_title('DXY-adjusted rolling partial correlation')
axes[1].legend(loc='upper right')
axes[1].grid(True, alpha=0.3)
beta_label = ('Rolling beta (THB on XAUUSD)'
              if not STANDARDIZE_BETA else
              'Rolling beta (standardized)')
axes[2].plot(roll_beta.index, roll_beta.values, label=f'{beta_label}', color = 'green')
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
del ref_ticker, gold_tkr, thb_tkr, dxy_tkr, gold_min, dxy_min, thb_min, px_min_3, px_block, ret_blk, roll_corr, roll_pcorr_usd, roll_beta



# ## Rolling 5d 4hourly correlation (HSCEI Futs vs CGB Futs)
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
del ref_ticker, hscei_tkr, cgb30y_tkr, hscei_min, cgb30y_min, block_minutes, px_min, px_block, ret_blk, roll_corr

# ## Rolling 10d 4hourly correlation (USDCNH vs CGB Futs)
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
del ref_ticker, hscei_tkr, cgb30y_tkr, hscei_min, cgb30y_min, block_minutes, px_min, px_block, ret_blk, roll_corr


# --- Config for CNH vs AUD (4h blocks, last 10 days) ---
lookback_days     = 120
block_minutes     = 240 
ROLL_WIN_BLOCKS   = 60     
STANDARDIZE_BETA  = False   

ref_ticker = 'ES1 Index'  
dxy_tkr    = 'BBDXY Index'   
cnh_tkr = 'USDCNH Curncy'
aud_tkr = 'AUDUSD Curncy'


# --- Pull intraday (assumes your helper funcs exist) ---
cnh_min = pull_intraday_minutes_last_n_days(cnh_tkr, lookback_days, ref=ref_ticker)
aud_min = pull_intraday_minutes_last_n_days(aud_tkr, lookback_days, ref=ref_ticker)
dxy_min = pull_intraday_minutes_last_n_days(dxy_tkr, lookback_days, ref=ref_ticker)

# --- Align CNH & AUD on minute grid, then add DXY; ffill lightly as before ---
px_min_2 = align_minutes(cnh_min, aud_min, method='ffill')
px_min_3 = (
    px_min_2
    .join(dxy_min.reindex(px_min_2.index), how='left')
    .ffill(limit=3)
    .dropna()
)
px_min_3.columns = [cnh_tkr, aud_tkr, dxy_tkr]

# --- Sample last tick every 4h block, compute log-returns ---
px_block = last_every_n_minutes(px_min_3, block_minutes)
ret_blk  = np.log(px_block).diff().dropna()

# Friendly column names
ren = {cnh_tkr: 'USDCNH', aud_tkr: 'AUDUSD', dxy_tkr: 'DXY'}
ret_blk = ret_blk.rename(columns=ren)

# --- Rolling stats (window in blocks) ---
roll_corr       = rolling_corr(ret_blk['USDCNH'], ret_blk['AUDUSD'], ROLL_WIN_BLOCKS)
roll_pcorr_usd  = rolling_partial_corr_xyz(ret_blk, 'USDCNH', 'AUDUSD', 'DXY', ROLL_WIN_BLOCKS)
roll_beta       = rolling_beta(ret_blk, xcol='USDCNH', ycol='AUDUSD',
                               window=ROLL_WIN_BLOCKS, standardize=STANDARDIZE_BETA)

# --- Last updated (SGT) ---
last_ts   = _last_plotted_timestamp(roll_corr, roll_pcorr_usd, roll_beta)
last_sgt  = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M %Z') if last_ts is not None else 'n/a'
last_info = f"Last Updated: {last_sgt} SGT"

# --- Plot ---
fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

# 1) Rolling correlation
axes[0].plot(roll_corr.index, roll_corr.values, label='Rolling corr (USDCNH vs AUDUSD)')
axes[0].axhline(0, lw=1)
axes[0].set_title('5-day Rolling Corr (4H data): USDCNH vs AUDUSD')
axes[0].legend(loc='upper right')
axes[0].grid(True, alpha=0.3)

# 2) USD-neutral partial correlation
axes[1].plot(roll_pcorr_usd.index, roll_pcorr_usd.values,
             label='USD-neutral partial corr', color='orange')
axes[1].axhline(0, lw=1)
axes[1].set_title('DXY-adjusted rolling partial correlation')
axes[1].legend(loc='upper right')
axes[1].grid(True, alpha=0.3)

# 3) Rolling beta (AUD on CNH)
beta_label = ('Rolling beta (AUDUSD on USDCNH)' if not STANDARDIZE_BETA
              else 'Rolling beta (standardized)')
axes[2].plot(roll_beta.index, roll_beta.values, label=beta_label, color='green')
axes[2].axhline(0, lw=1)
axes[2].set_title(beta_label)
axes[2].legend(loc='upper right')
axes[2].grid(True, alpha=0.3)

# X-axis formatting
axes[-1].xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=1))
axes[-1].xaxis.set_major_formatter(DateFormatter('%b %d'))
plt.setp(axes[-1].get_xticklabels(), rotation=0)

# Header box
fig.text(0.99, 0.98, last_info,
         ha='right', va='top',
         bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
         fontsize=9)

plt.tight_layout()
plt.savefig(Path(G_CHART_DIR, "USDCNH_vs_AUDUSD_High_Frequency_Correlation.png"), bbox_inches='tight')

# Cleanup
del (cnh_tkr, aud_tkr, cnh_min, aud_min, dxy_min,
     px_min_2, px_min_3, px_block, ret_blk,
     roll_corr, roll_pcorr_usd, roll_beta, last_ts, last_sgt, last_info)

# # ## Rolling 10d 4hourly correlation (USDCNH vs CGB Futs)
# lookback_days = 120        
# ref_ticker = 'SPX Index'  
# hscei_tkr   = 'EURUSD BGN Curncy'
# cgb30y_tkr  = 'NVDA US Equity'   
# hscei_min = pull_intraday_minutes_last_n_days(hscei_tkr, lookback_days, ref=ref_ticker)
# cgb30y_min = pull_intraday_minutes_last_n_days(cgb30y_tkr, lookback_days, ref=ref_ticker)
# block_minutes = 30
# px_min   = align_minutes(hscei_min, cgb30y_min, method='ffill') 
# px_min.columns = ['EURUSD', 'NVDA']
# px_block = last_every_n_minutes(px_min, block_minutes)
# ret_blk = px_block.pct_change()
# window = 130
# roll_corr = (ret_blk['EURUSD']
#              .rolling(window=window, min_periods=window)
#              .corr(ret_blk['NVDA']))
# last_ts   = _last_plotted_timestamp(roll_corr)
# last_sgt  = last_ts.tz_convert('Asia/Singapore').strftime('%Y-%m-%d %H:%M') if last_ts is not None else 'n/a'
# last_updated_text = f"Last Updated:{last_sgt} SGT"
# fig, ax = plt.subplots(figsize=(10, 5))
# roll_corr.plot(ax=ax)
# ax.set_title('Rolling 10-Day Correlation (3H data)\nEURUSD vs NVDA')
# ax.set_xlabel('Date')
# ax.set_ylabel('Correlation')
# ax.grid(True, linestyle='--', alpha=0.4)
# ax.xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=1))
# ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
# fig.text(
#     0.99, 0.98, last_updated_text,
#     ha='right', va='top',
#     bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.5', alpha=0.85),
#     fontsize=9)
# plt.tight_layout()


# plt.savefig(Path(G_CHART_DIR, "EURUSDvNVDA_HighFrequencyCorrelation.png"), bbox_inches='tight')
# del ref_ticker, hscei_tkr, cgb30y_tkr, hscei_min, cgb30y_min, block_minutes, px_min, px_block, ret_blk, roll_corr