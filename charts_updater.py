from exante_utils import get_data
from ecom_utils import bfill_cny
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter
from statsmodels.regression.rolling import RollingOLS
from xbbg import blp

import bbgui
from chart_utils import clean_data

G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase"  # general chart directory


# gsfci chart
START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")
END_DATE = datetime.today()
data = bbgui.bdh("GSUSFCI Index", "PX_LAST", START_DATE, END_DATE)
data['20d diff'] = data.diff(20)
data = data.dropna()
data['20d diff'].plot()
plt.hlines(data['20d diff'][-1], data['20d diff'].index[0],
           data['20d diff'].index[-1], colors="r")
plt.xlabel("20d change in GS US Financial Conditions")
plt.title(f"current FCI change at (red line) {data['20d diff'][-1]: 0.2f}")
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\20d Change of GSUSFCI")

# hkd swap vs hsi chart
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\HSI vs Hibor3y")

# US 7-10y Gov Bond Price vs. EURUSD exchange rate
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\EURUSD vs UST")

# labor market condition index
START_DATE = datetime.strptime("01/01/92", "%d/%m/%y")
END_DATE = datetime.today()
data = bbgui.bdh(["KCMTLMCI Index"], "PX_LAST", START_DATE, END_DATE)
data = data.dropna()
fig, ax1 = plt.subplots()
ax1.plot(data.index, data["KCMTLMCI Index"], color="r", label="KCMTLMCI Index")
ax1.legend(loc=0)
ax1.set_ylabel("labor market condition index")
plt.title(
    f"Current level at {data['KCMTLMCI Index'][-1]:0.2f} in {data.index[-1].strftime('%Y-%m')}")
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\LMCI")

# Kosdaq/Kospi vs. Global PMI
START_DATE = datetime.strptime("01/01/90", "%d/%m/%y")
END_DATE = datetime.today()
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\PMI")

# LEI
START_DATE = datetime.strptime("01/01/90", "%d/%m/%y")
END_DATE = datetime.today()
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\LEI")

# Capacity Utilization
START_DATE = datetime.strptime("01/01/80", "%d/%m/%y")
END_DATE = datetime.today()
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\CapacityUti")

# USCPIvsWage
START_DATE = datetime.strptime("01/01/98", "%d/%m/%y")
END_DATE = datetime.today()
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\USCPIvsWage")

# EU GDP vsWage
START_DATE = datetime.strptime("01/01/98", "%d/%m/%y")
END_DATE = datetime.today()
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\EZWage")

# US Withheld Tax vs Jobs
START_DATE = datetime.strptime("01/01/82", "%d/%m/%y")
END_DATE = datetime.today()
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
# fig, ax1 = plt.subplots()
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\Withheld Tax vs Total NFP")
del data_combined, data_filtered

# USD advances minus declines 60d moving average (G10 9 cross rates with EUR JPY CAD AUD NZD GBP CHF NOK SEK)
START_DATE = G_END_DATE - timedelta(days=365 * 3)
END_DATE = G_END_DATE
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
                            data_ccy.shape[1]) / data_ccy.shape[1]  # cal A/D line
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\USD AD Line")

del data_ccy

# Relative EM/DM P/B ratio to measure EM equities cheapness
START_DATE = G_END_DATE - timedelta(days=365 * 3)
END_DATE = G_END_DATE
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\EMDM PBratio")

# EM PE
START_DATE = datetime.strptime("01/01/06", "%d/%m/%y")
END_DATE = G_END_DATE
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
plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\EM MSCI PEratio")

# # Singapore trade weighted SGD vs. GDP deflator: services producing industries
# START_DATE = datetime.strptime("01/01/00", "%d/%m/%y")
# END_DATE = G_END_DATE
# data = bbgui.bdh(['SDFLSVC Index', "MSCESGTW Index"], "PX_LAST",
#                  START_DATE, END_DATE, interval='QUARTERLY')
# data = data.fillna(method="ffill", limit=1)
# data['year'] = data.index.year
# data['month'] = data.index.month
# data = data.drop_duplicates(subset=["year", "month"], keep="last")
# data = data.dropna()

# fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))
# ax1 = axes[0]
# ax1.plot(data.index, data['SDFLSVC Index'],
#          label="GDP Deflator for services producing industries")
# ax1.legend(loc=3)
# ax1.text(data.index[0], 120,
#          "*Good correlation, deflation = currency depreciation in singapore")
# ax1.set_title("SGD TWI vs GDP Deflator")

# ax2 = ax1.twinx()
# ax2.plot(data.index, data["MSCESGTW Index"],
#          label="SGD TWI Index (R)", color="red")
# ax2.legend(loc=4)

# ax3 = axes[1]
# ax3.plot(data.index[-12:], data['SDFLSVC Index'][-12:],
#          label="GDP Deflator for services producing sector")
# ax3.legend(loc=3)
# ax3.set_title("shorter series")

# ax4 = ax3.twinx()
# ax4.plot(data.index[-12:], data["MSCESGTW Index"]
#          [-12:], label="SGD TWI Index (R)", color="red")
# ax4.legend(loc=4)

# plt.tight_layout()
# plt.savefig(r"O:\Tian\Portal\Charts\ChartDataBase\SGD TWI vs GDP deflator")

# United States CMBS spread
START_DATE = datetime.strptime("25/01/22", "%d/%m/%y")
END_DATE = G_END_DATE
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
del data1, data2, data3, data_combine

# United States Construction Sector Data
START_DATE = datetime.strptime("01/01/1970", "%d/%m/%Y")
END_DATE = G_END_DATE
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
ax3.axhline(y=0, color = 'grey', alpha = 0.5)
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
del data

# Shanghai futures vs LME copper spread
START_DATE = datetime.strptime("01/01/2016", "%d/%m/%Y")
END_DATE = G_END_DATE
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
del data

# LME metal index vs CRB raw material index, a higher LME index hints that there are more speculative demand than industrial demand
START_DATE, END_DATE = datetime.strptime("01/01/2015", "%d/%m/%Y"), G_END_DATE
data = bbgui.bdh(["LMEX Index",
                  # LME Mental Index,   The London Metal Exchange LMEX Index is calculated once a day on the basis of the closing prices of the six primary metals:
                  # copper, aluminum, lead, tin, zinc and nickel. It has a base value of 1000 starting in 1984.
                  "CRB RIND Index",
                  # Commodity Research Bureau Raw Industrial Index, unweighted geometric mean of the 13 individual commodity price
                  # Price sources are United States but not worldwide
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
del data

# 3m beta of weekly changes in 10x30s US to weekly changes of 2y UST
START_DATE, END_DATE = datetime.strptime("01/01/1970", "%d/%m/%Y"), G_END_DATE
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
del data

# US Household demand picture
START_DATE, END_DATE = datetime.strptime("01/01/1985", "%d/%m/%Y"), G_END_DATE
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
del data, img

# citi ecom surprise index vs stokc price in lcy ccy mkt cap weighted
# START_DATE, END_DATE = datetime.strptime("01/01/1985", "%d/%m/%Y"), G_END_DATE
# data = bbgui.bdh(["CONSDURF Index", "CONSDURU Index", "PCE DRBC Index"], flds="PX_LAST", startDate=START_DATE,
#                  endDate=END_DATE, interval="Monthly")
# data = clean_data(data)
#
# "CESIAUD Index"	"AS51 Index"
# "CESISGD Index"	"STI Index"
# CESIHKD Index	HIS Index
# CESIKRW Index	KOSPI Index
# CESITWD Index	TWSE Index
# CESICNY Index	SHCOMP Index
# CESIUSD Index	SPX Index

# China Real Estate Investment annual change 3m MA vs. imports of primary good annual change
START_DATE, END_DATE = datetime.strptime("01/01/2006", "%d/%m/%Y"), G_END_DATE
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
del data

# Cement/Plate of glass/auto sales YOY growth of China
# certain parts of China growing, illustrate how the economy is shifting
START_DATE, END_DATE = datetime.strptime("01/01/2006", "%d/%m/%Y"), G_END_DATE
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
del data, data_pct, ticker_dict

# Bank loans to non-bank financial institutions, China
START_DATE, END_DATE = datetime.strptime("01/01/2015", "%d/%m/%Y"), G_END_DATE
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
del data

# Australia Monthly CPI All groups excl volatile items & holiday travel, seasonally adjusted, and exclude elec
START_DATE, END_DATE = datetime.strptime("01/01/2015", "%d/%m/%Y"), G_END_DATE
data = bbgui.bdh(["ACPMXVS Index", "ACPMISA Index", "ACPMXVLY Index", "ACPMELEC Index"], flds="PX_LAST", startDate=START_DATE,
                 endDate=END_DATE, interval="Monthly")

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
del data

# Indonesia wage growth
START_DATE, END_DATE = datetime.strptime("01/01/2009", "%d/%m/%Y"), G_END_DATE
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
del data, data_pct, ticker_dict

# Singapore liquidity
START_DATE, END_DATE = datetime.strptime("01/01/2003", "%d/%m/%Y"), G_END_DATE
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
del data, fig, ticker_dict

# USD advances minus declines 60d moving average (9 cross with EUR JPY CAD AUD NZD GBP CHF NOK SEK)
# Measuring breadth of USD strength/weakness
START_DATE, END_DATE = datetime.strptime("01/01/2007", "%d/%m/%Y"), G_END_DATE
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
del data, fig

# SNP Breadth and RSI
START_DATE, END_DATE = datetime.strptime("01/01/2021", "%d/%m/%Y"), G_END_DATE
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
del data, fig

# Commodities prices (GS commodities index) advanced by 6m, vs. Commodities country domestic credit impulse (equaly weighted average of brazil, chile, colombia,
#   mexico, south africa and russia)
START_DATE, END_DATE = datetime.strptime("01/01/2008", "%d/%m/%Y"), G_END_DATE
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


data2 = get_data(['RU.CREDIT.TOTAL.IMPL.12M.M',
                  'ZA.CREDIT.TOTAL.IMPL.12M.M',
                  'MX.CREDIT.TOTAL.IMPL.12M.M',
                  'BR.CREDIT.TOTAL.IMPL.12M.M',
                  'CL.CREDIT.TOTAL.IMPL.12M.M',
                  'CO.CREDIT.TOTAL.IMPL.12M.M',
                  ], startDate='2010-01-01', endDate=None).sort_index()

data3 = get_data(['RU.CREDIT.TOTAL.IMPL.6M.M',
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
del data, data2, data3, data4, data_merge, id, new_index, fig

# Domestic vs External bond yield scatter plot across countries
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
del data1, data2, data3, x, y, labels, rename_dict, fig

# Fixed Investment as a % of global nominal GDP, compare countries
