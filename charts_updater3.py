from datetime import datetime, timedelta
from pathlib import Path
from pandas.tseries.offsets import BDay
import matplotlib.dates as mdates
from matplotlib.ticker import MultipleLocator
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from xbbg import blp
from chart_utils import *
import statsmodels.api as sm

G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase"  # general chart directory
FONTSIZE = 14

# Marginal Propensity To Save
START_DATE = datetime(2016, 1, 1)
base = '2019-12-31'
# Korea uses corp + households in KRW. Sum up all duration deposits to get time deposit
korea = blp.bdh(
    ["KOMBTDL6 Index", "KOMBTD61 Index", "KOMBTD12 Index", "KOMBTD23 Index", "KOMBTDO3 Index", "KOMBDDMD Index"],
    "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
korea.index = pd.to_datetime(korea.index)
latest = korea.last_valid_index().strftime("%b %y")
korea["Time Deposits"] = korea.iloc[:, :5].sum(axis=1)
korea["Marginal Propensity to Save"] = korea["Time Deposits"] / korea["KOMBDDMD Index"]
korea = base_series_to_date(korea, "Marginal Propensity to Save", base_date=base)
# Reads in Total, Demand, Savings. Subtract to get time
thailand = blp.bdh(["TLDGTATD Index", "TLDGTADD Index", "TLDGTASD Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(
    1, axis=1)
thailand.index = pd.to_datetime(thailand.index)
thailand["Time Deposits"] = thailand["TLDGTATD Index"] - thailand["TLDGTADD Index"] - thailand["TLDGTASD Index"]
thailand["Marginal Propensity to Save"] = thailand["Time Deposits"] / thailand["TLDGTADD Index"]
thailand = base_series_to_date(thailand, "Marginal Propensity to Save", base_date=base)
# Uses Individual deposit into Large Sized State owned Commercial banks in CNY
china = blp.bdh(["CHBDLBPT Index", "CHBDLBPE Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
china.index = pd.to_datetime(china.index)
china["Marginal Propensity to Save"] = china["CHBDLBPT Index"] / china["CHBDLBPE Index"]
china = base_series_to_date(china, "Marginal Propensity to Save", base_date=base)
# Uses demand deposits of commercial banks, but time and other deposits as an est for time deposits
US = blp.bdh(["PPIDBK11 Index", "PPIDBK12 Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
US.index = pd.to_datetime(US.index)
US["Marginal Propensity to Save"] = US["PPIDBK12 Index"] / US["PPIDBK11 Index"]
US = base_series_to_date(US, "Marginal Propensity to Save", base_date=base)
# Uses CD and term deposits to calculate time deposits
AU = blp.bdh(["AUBKATRD Index", "AUBKACOD Index", "AUBKALDD Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1,
                                                                                                                  axis=1)
AU.index = pd.to_datetime(AU.index)
AU["Time Deposits"] = AU["AUBKATRD Index"] + AU["AUBKACOD Index"]
AU["Marginal Propensity to Save"] = AU["Time Deposits"] / AU["AUBKALDD Index"]
AU = base_series_to_date(AU, "Marginal Propensity to Save", base_date=base)
# Uses Transactional balances as demand deposits
nzd = blp.bdh(["NZBBLTDB Index", "NZBBLTBL Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del korea, china, US, thailand, AU, nzd, fig, ax1, ax2

# NFP Employment Growth vs CEO Confidence and Operating Profits
START_DATE = datetime(2003, 1, 1)
business = blp.bdh(["CEOCINDX Index", "CPFTYOY Index"], "PX_LAST", START_DATE, G_END_DATE, Per='Q').droplevel(1, axis=1)
nfp = blp.bdh(["NFP NYOY Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
ax3.spines['right'].set_position(('outward', 60))  # Offset the second right y-axis
ax3.tick_params(axis='y', colors='red')
lines = line1 + line2 + line3
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="upper left")
ax1.tick_params(axis='x', rotation=60)
plt.savefig(Path(G_CHART_DIR, "NFP vs CEO Confidence and Operating Profits"), bbox_inches='tight')
del business, business_extended, fig, ax1, ax2, ax3

# Inventory Shipment Ratio
START_DATE = datetime(2000, 1, 1)
ratio = blp.bdh(["JNISIVR Index", "KOPII Index", "KOPSI Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del ratio, latest_ratio, fig, ax1, ax2

# Service Activity
START_DATE = datetime(2000, 1, 1)
services = blp.bdh(["NAPMPMI Index", "NAPMNMI Index", "RHOTPNAT Index", "NRASRPI Index", "USHBTRAF Index"], "PX_LAST",
                   START_DATE, G_END_DATE).droplevel(1, axis=1)

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
ax3.spines['right'].set_position(('outward', 60))  # Offset the second right y-axis
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
del services, fig, ax1, ax2, ax3

# Predict Inflation
START_DATE = datetime(2014, 1, 1)
velocity = blp.bdh("VELOM2 Index", "PX_LAST", START_DATE, G_END_DATE, Per='Q').droplevel(1, axis=1)
cpi = blp.bdh("CPI XYOY Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
ax1.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[3, 6, 9, 12]))  # Quarterly: March, June, September, December
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
del velocity, cpi, fig, ax1, ax2

# China Freight Turnover
START_DATE = datetime(2014, 1, 1)
freight = blp.bdh("CNRWRFTO Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
freight.index = pd.to_datetime(freight.index)
latest = freight.last_valid_index().strftime("%b %y")

# Initialize the 'Monthly' column
freight['Monthly'] = 0.0

# Calculate 'Monthly' values
for i in range(len(freight)):
    if freight.index[i].month == 1:
        # January: Monthly value is the same as the YTD value
        freight['Monthly'].iloc[i] = freight["CNRWRFTO Index"].iloc[i]
    else:
        # Other months: Difference between current and previous month's YTD values
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
del freight, fig, ax1, ax2

# EM Composite EPS vs Index Px
START_DATE = datetime(2010, 1, 1)
country_indices = ['SHSZ300 Index', 'NSE500 Index', 'IBOV Index', 'KOSPI Index', 'TWSE Index', 'MEXBOL Index',
                   'SET Index', 'SASEIDX Index']
fields = ['PX_LAST', 'BEST_EPS', 'CUR_MKT_CAP']
country_name_map = {'SHSZ300 Index': 'China', 'NSE500 Index': 'India', 'IBOV Index': 'Brazil',
                    'KOSPI Index': 'South Korea', 'TWSE Index': 'Taiwan', 'MEXBOL Index': 'Mexico',
                    'SET Index': 'Thailand', 'SASEIDX Index': 'South Africa'}
eps_data = blp.bdh(country_indices, fields, START_DATE, G_END_DATE)
eps_data.index = pd.to_datetime(eps_data.index)
eps_data = eps_data.resample('M').mean()
eps_data[('KOSPI Index', 'BEST_EPS')].loc['2025-02-28'] = 277.1574

currencies = ['USDCNY Curncy', 'USDINR Curncy', 'USDBRL Curncy', 'USDKRW Curncy', 'USDTWD Curncy', 'USDMXN Curncy',
              'USDTHB Curncy', 'USDSAR Curncy']
currency_data = blp.bdh(currencies, 'PX_LAST', START_DATE, G_END_DATE)
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
del eps_data, rebased_index, rebased_eps, composite_index, composite_eps, weights_df, fig, axs

# Prime Age Employment-Population Ratio
START_DATE = datetime(1980, 1, 1)
employment_rate = blp.bdh("USER54SA Index", "PX_LAST", START_DATE, G_END_DATE)
employment_rate.index = pd.to_datetime(employment_rate.index)
employment_rate.columns = ["Employment Rate"]
prime_participation_rate = blp.bdh("PRUSQNTS Index", "PX_LAST", START_DATE, G_END_DATE)
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
ax_zoom.set_xlim(start_zoom, G_END_DATE)
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
del employment_rate, prime_participation_rate, official_unemployment_rate, emp_zoom, part_zoom, unemp_zoom, common_zoom, last_date_top, last_date_top_str, last_emp_value_top, last_part_value_top, last_unemp_value_top, common_full, last_date_full, last_date_full_str, last_emp_value_full, last_part_value_full, last_unemp_value_full, line_emp_zoom, line_part_zoom, line_unemp_zoom, line_emp_full, line_part_full, line_unemp_full, ax_zoom, ax_zoom_unemp, ax_full, ax_full_unemp, axs, fig, start_zoom

# Phillips Curve AHE vs U6 Unemployment
START_DATE = datetime(1994, 1, 1)
ahe_yoy2 = blp.bdh("USHEYOY Index", "PX_LAST", START_DATE, G_END_DATE)
U6_unemployment = blp.bdh("USUDMAER Index", "PX_LAST", START_DATE, G_END_DATE)
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
plt.figure(figsize=(10, 6))
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
del ahe_yoy2, U6_unemployment, philips_curve_data, period1, period2, period3, period4, others

# India Fiscal Deficit/GDP ratio and Non-Financial Sector Credit Growth
START_DATE = datetime(2000, 1, 1)
fiscal_deficit = blp.bdh("INFFFIDE Index", "PX_LAST", START_DATE, G_END_DATE)
fiscal_deficit.index = pd.to_datetime(fiscal_deficit.index)
india_nominal_gdp = blp.bdh("IGQNEGDP Index", "PX_LAST", START_DATE, G_END_DATE)
india_nominal_gdp.index = pd.to_datetime(india_nominal_gdp.index)
non_financial_sector_credit = blp.bdh("CPNFINCD Index", "PX_LAST", START_DATE, G_END_DATE)
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
del fig, ax, fiscal_deficit, india_nominal_gdp, non_financial_sector_credit, non_financial_sector_credit_growth, india_nominal_gdp_monthly, fd_gdp_data, line_fdgdp, line_credit, last_valid_date_fd, last_fdgdp_value, last_valid_date_fd_str, last_valid_date_credit, last_credit_value, last_valid_date_credit_str

# EPS YoY% vs (Nom. GDP YoY% - Wages YoY%) x Oper. Margin %
#     All data resampled to monthly, quarterly data ffill (limit=2) to fill for quarter.
#     TPX EPS fluctuates around 0 pre-2012, causes large moves in EPS Growth
START_DATE = datetime(1990, 1, 1)
indices_tickers = ['SPX Index', 'SXXP Index', 'UKX Index', 'TPX Index', 'ASX Index', 'SHSZ300 Index', 'NSE500 Index',
                   'TWSE Index']
fields = ['TRAIL_12M_EPS', "OPER_MARGIN"]
nom_gdp_tickers = ["GDP CURY Index", 'ENGK27Y Index', 'UKGRYBAQ Index', 'OEJPNGBK Index', 'AUGDPCY Index',
                   'OECNNGAE Index', 'INBGDNQY Index', 'ECOXTWS Index']
wage_growth_tickers = ['COMPNFRY Index', 'LNTN27Y Index', 'UKAWYWHO Index', 'JNLSUCTL Index', 'AUWCBY Index',
                       'CHINWAG Index', 'INBGRIWG Index', 'TWMERY Index']
nom_gdp_data = blp.bdh(nom_gdp_tickers, "PX_LAST", START_DATE, G_END_DATE)
wage_growth_data = blp.bdh(wage_growth_tickers, "PX_LAST", START_DATE, G_END_DATE)
indices_data = blp.bdh(indices_tickers, fields, START_DATE, G_END_DATE)
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


def plot_country_chart(country_list, mapping, eps_growth, nom_gdp_data, wage_growth_data, margin_data, countries):
    fig, axes = plt.subplots(nrows=4, ncols=2, figsize=(15, 20))
    axes = axes.reshape(4, 2)

    for i, country in enumerate(country_list):
        tickers = mapping[country]
        country_name = tickers.get('country_name', '')

        df_eps = eps_growth[country].dropna().to_frame(name='EPS Growth')
        df_comp = pd.concat([
            nom_gdp_data[tickers['gdp']].rename('GDP Growth'),
            wage_growth_data[tickers['wage']].rename('Wage Growth'),
            margin_data[country].rename('Margin')
        ], axis=1).dropna()
        df_comp['Composite'] = (df_comp['GDP Growth'] - df_comp['Wage Growth']) * df_comp['Margin']
        last_eps_date = df_eps.index[-1] if not df_eps.empty else None
        last_comp_date = df_comp.index[-1] if not df_comp.empty else None
        annotation_text = (f"Last EPS: {last_eps_date.strftime('%Y-%m') if last_eps_date else 'N/A'}\n"
                           f"Last Composite: {last_comp_date.strftime('%Y-%m') if last_comp_date else 'N/A'}")

        ax_left = axes[i, 0]
        ax_left.plot(df_eps.index, df_eps['EPS Growth'], label='EPS Growth YoY%', linestyle='-')
        ax_left.plot(df_comp.index, df_comp['Composite'], label='(GDP YoY% - Wages YoY%) × Oper.Margin(pct)',
                     color='orange', marker='.', linestyle='--')
        ax_left.set_title(f"{country} ({country_name}) - Full Series")
        ax_left.set_xlabel("Date")
        ax_left.set_ylabel("%")
        lines1, labels1 = ax_left.get_legend_handles_labels()
        ax_left.legend(lines1, labels1, loc='upper left')
        ax_left.grid(True)
        ax_left.text(0.72, 0.95, annotation_text, transform=ax_left.transAxes, fontsize=9,
                     verticalalignment='top', bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.4))

        # Right subplot
        df_eps_recent = df_eps.loc[df_eps.index >= pd.to_datetime("2016-01-01")]
        df_comp_recent = df_comp.loc[df_comp.index >= pd.to_datetime("2016-01-01")]
        recent_last_eps = df_eps_recent.index[-1] if not df_eps_recent.empty else None
        recent_last_comp = df_comp_recent.index[-1] if not df_comp_recent.empty else None
        annotation_text_recent = (f"Last EPS: {recent_last_eps.strftime('%Y-%m') if recent_last_eps else 'N/A'}\n"
                                  f"Last Composite: {recent_last_comp.strftime('%Y-%m') if recent_last_comp else 'N/A'}")
        ax_right = axes[i, 1]
        ax_right.plot(df_eps_recent.index, df_eps_recent['EPS Growth'], label='EPS Growth YoY%', linestyle='-')
        ax_right.plot(df_comp_recent.index, df_comp_recent['Composite'],
                      label='(GDP YoY% - Wages YoY%) × Oper.Margin(pct)', color='orange', marker='.', linestyle='-')
        ax_right.set_title(f"{country} ({country_name}) - From 2016")
        ax_right.set_xlabel("Date")
        ax_right.set_ylabel("%")
        lines1, labels1 = ax_right.get_legend_handles_labels()
        ax_right.legend(lines1, labels1, loc='upper left')
        ax_right.grid(True)
        ax_right.text(0.72, 0.95, annotation_text_recent, transform=ax_right.transAxes, fontsize=9,
                      verticalalignment='top', bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.4))

    plt.tight_layout()
    plt.savefig(Path(G_CHART_DIR, f"EPSGrowth vs GDP-WagesXOperMargin ({countries})"), bbox_inches='tight')


# Figure 1 for set1 (SPX, SXXP, UKX, TPX)
plot_country_chart(set1, mapping, eps_growth, nom_gdp_data, wage_growth_data, margin_data, "US, EU, UK, JP")
# Figure 2 for set2 (ASX, SHSZ300, NSE500, TWSE)
plot_country_chart(set2, mapping, eps_growth, nom_gdp_data, wage_growth_data, margin_data, "AU, CN, IN, TW")

del wage_growth_data, nom_gdp_data, margin_data, eps_data, eps_growth, eps_data_monthly, mapping, set1, set2

# SPX Risk Premium vs TY
START_DATE = datetime(1980, 1, 1)
spx_index = blp.bdh("SPX Index", "PX_LAST", START_DATE, G_END_DATE)
spx_eps = blp.bdh("SPX Index", "BEST_EPS", START_DATE, G_END_DATE)
ty_yield = blp.bdh("USGG10YR Index", "PX_LAST", START_DATE, G_END_DATE)
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
del spx_index, spx_eps, ty_yield, line_rp, fig, ax

# Corr (MSCI World, Yields) vs yields
START_DATE = datetime(1995, 1, 1)
END_DATE = datetime.today()
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
del corr_rows, underlying_rows, yields_msci_tickers, yields_msci_data, yields_msci_weekly, orig_index, fig, axs

# YoY Change: Comm Banks Securities Hldgs % of GDP (Lead 52/130 weeks) vs DXY Index
START_DATE = datetime(1995, 1, 1)
END_DATE = datetime.today()
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
plt.figure(figsize=(12, 6))
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
del comm_bank_sec_data, gdp_data, dxy_data, merged, last_valid_dxy, last_valid_holdings, merged_extended, new_end, new_index

# EM/DM CurrentLiab to FwdSales
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

em_sales_liab_data = blp.bdh(em_country_indices, fields, START_DATE, G_END_DATE)
em_sales_liab_data.index = pd.to_datetime(em_sales_liab_data.index)
em_sales_liab_data = em_sales_liab_data.resample('M').mean()

em_currency_data = blp.bdh(em_currencies, 'PX_LAST', START_DATE, G_END_DATE)
em_currency_data.index = pd.to_datetime(em_currency_data.index)
em_currency_data = em_currency_data.resample('M').mean()

dm_country_name_map = {'SPX Index': 'US', 'NDX Index': 'US', 'RTY Index': 'US', 'UKX Index': 'UK', 'SX5E Index': 'EU',
                       'SHSZ300 Index': 'CN', 'TPX Index': 'JP', 'ASX Index': 'AU'}

dm_sales_liab_data = blp.bdh(dm_country_indices, fields, START_DATE, G_END_DATE)
dm_sales_liab_data.index = pd.to_datetime(dm_sales_liab_data.index)
dm_sales_liab_data = dm_sales_liab_data.resample('M').mean()

dm_currencies = ['GBPUSD Curncy', 'EURUSD Curncy', 'USDJPY Curncy', 'AUDUSD Curncy']
dm_currency_data = blp.bdh(dm_currencies, 'PX_LAST', START_DATE, G_END_DATE)
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

# Display latest dates from each series (if available)
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
del em_sales_liab_data, dm_sales_liab_data, em_composite_ratio, dm_composite_ratio, em_weights_df, dm_weights_df, fig, axs

# China: Bank Claims on Non-Financial Sector to Non-Financial Deposits Ratio
START_DATE = datetime(2006, 1, 1)
ch_nonfinancial_data = blp.bdh(["CHFANFS Index", "CHDLDIBM Index"], "PX_LAST", START_DATE, G_END_DATE)
ch_nonfinancial_data.columns = ['claims', 'deposits']
ch_nonfinancial_data['nonfinLDR'] = ch_nonfinancial_data['claims'] / ch_nonfinancial_data['deposits']
last_valid_date = ch_nonfinancial_data['nonfinLDR'].dropna().last_valid_index()  # .strftime('%b %Y')
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
del ch_nonfinancial_data, last_valid_date, last_value, line, fig, ax

## 10y and 2y average real bond yield diff between Germany (Japan) and US, vs EURUSD (USDJPY)
START_DATE = datetime(2005, 1, 1)
END_DATE = datetime.today()
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
del all_data, GRAPH_START_DATE, near_data, common_dates, fig, ax

# China Grid Investment (YoY%) vs China Copper Price & Copper Imports
START_DATE = datetime(2015, 2, 1)
END_DATE = datetime.today()
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
del all_data, last_valid, last_valid_date_grid, last_grid_value, last_valid_date_grid_str, last_valid_date_hg_price, last_price_value, last_valid_date_hg_price_str, last_valid_date_hg_imp, last_valid_date_hg_imp_str, fig, ax

## Regression US HY Sprd vs CycVsDef, EMBI Sprd vs CycVsDef, EMBI Sprd vs US HY Sprd 
START_DATE = datetime(2015, 1, 1)
END_DATE = datetime.today()
REG_START_DATE = '2015-01-01'
REG_END_DATE = datetime.today()
tickers = ['CSI BARC Index',  # BarCap US Corporate HY YTW - 10-Yr Treasury Spread.
           'JPEIGLSP Index',  # JPM EMBI Core Global Spread, duration ~7yr
           'GSPUCYDE Index',
           # Morgan Stanley Cyclical vs Defensive - Equal notional pair trade long GSXUCYCL (GS US Cyclical) vs GSXUDEFS (GS US Defs), daily rebal
           ]
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
del START_DATE, END_DATE, REG_START_DATE, REG_END_DATE, tickers, all_data, pairs, fig, axs, i, pair, x_col, y_col, df_temp, df_reg, x_reg, y_reg, X_reg, model, a, b, r2, X_full, yhat_full, ax_left, last_valid_date, last_valid_str, eq_text, ax_right, colors, sc, x_line, X_line, y_line, cbar, reg_period_text

# India/MSCI EM rel. performance vs Brent/Copper
START_DATE = datetime(2002, 2, 1)
END_DATE = datetime.today()
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
del all_data, ax, ax2, df, fig, line1, line2, lines

# Sensex YoY Returns vs Gap between Real GDP growth and 10Y bond yield 
START_DATE = datetime(2002, 2, 1)
END_DATE = datetime.today()
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
del all_data, ax, ax2, df, fig, line1, line2, lines
