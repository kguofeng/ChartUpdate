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
import bbgui

G_START_DATE = datetime.strptime("01/01/15", "%d/%m/%y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase"  # general chart directory
FONTSIZE = 14

# # EM Asia export volume and China Freight Index
# START_DATE, END_DATE = datetime.strptime("01/11/2010", "%d/%m/%Y"), G_END_DATE
# data = pd.read_excel("https://www.cpb.nl/sites/default/files/omnidownload/CPB-World-Trade-Monitor-June-2023.xlsx",
#                      skiprows=3, index_col=1)
# unnamed_cols = [col for col in data.columns if "Unnamed" in col]
# data = data.drop(columns=unnamed_cols)
# data = data.iloc[29:31, :].T
# data.index = pd.to_datetime(data.index, format="%Ym%m")
# data.index = data.index + pd.offsets.MonthEnd(0)
# data2 = bbgui.bdh(["SHSPCCFI Index"], "PX_LAST", START_DATE, END_DATE, interval='MONTHLY')
# data2.index = data2.index + pd.offsets.MonthEnd(0)

# data = data2.merge(data, left_index=True, right_index=True, how='left')
# data.columns = [col.strip() for col in data.columns]
# data2 = data.copy()

# data["SHSPCCFI Index"] = data["SHSPCCFI Index"].apply(lambda x: np.log(x))
# fig, axes = plt.subplots(2, 1, figsize=(16, 6 * 2))
# ax = axes[0]
# ax.plot(data.index, data['SHSPCCFI Index'],
#         label="Shanghai Shipping Exchange China (Export) Containerized Freight Index (L)", color='red')
# ax.legend(loc=3)
# ax.set_ylabel("China Freight Index")
# ax2 = ax.twinx()
# ax2.plot(data.index, data['China'], label="World Export - China (R)")
# ax2.plot(data.index, data['Emerging Asia excl China'], label="World Export - Emerging Asia exl China (R)")
# ax2.set_ylabel("Merchandise World Trade, export volumes SA, fixed base 2010=100")
# ax2.legend(loc=4)

# data = data2.pct_change(12) * 100
# ax3 = axes[1]
# ax3.plot(data.index, data['SHSPCCFI Index'],
#          label="Shanghai Shipping Exchange China (Export) Containerized Freight Index (L)", color='red')
# ax3.legend(loc=3)
# ax3.set_ylabel("China Freight Index, YoY%")
# ax4 = ax3.twinx()
# ax4.plot(data.index, data['China'], label="World Export - China (R)")
# ax4.plot(data.index, data['Emerging Asia excl China'], label="World Export - Emerging Asia exl China (R)")
# ax4.set_ylabel("Merchandise World Trade, export volumes SA, YoY%")
# ax4.legend(loc=4)
# ax.set_title("Asia Export Volume and China Freight Index")
# plt.savefig(Path(G_CHART_DIR, "Asia Export Volume and China Freight Index"))
# del data, data2, fig

# EM Asia export volume and China Freight Index
START_DATE, END_DATE = datetime.strptime("01/01/1980", "%d/%m/%Y"), G_END_DATE
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
del data, new_index, fig

# EM Asia export volume and China Freight Index
START_DATE, END_DATE = datetime.strptime("01/01/1980", "%d/%m/%Y"), G_END_DATE
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
del data, fig

# Australia Wage
START_DATE, END_DATE = datetime.strptime("01/01/1980", "%d/%m/%Y"), G_END_DATE
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
del data, bar_data, fig, ax, df_list, data2,

# Australia Unemployment rate and unemployment expectation
START_DATE, END_DATE = datetime.strptime("01/01/2000", "%d/%m/%Y"), G_END_DATE
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
    fontsize=fontsize)
plt.savefig(Path(G_CHART_DIR, "Australia Unemployment Expectations Leads Unemployment Rate"))
del data, fig, ax, ax2

# USD ISM
START_DATE, END_DATE = datetime.strptime("01/01/2017", "%d/%m/%Y"), G_END_DATE

manu_list = ["NAPMMNOW Index", "NAPMMDDA Index", "NAPMMEML Index"]  # supplier delivery: faster
service_list = ["NAPMNNOL Index", "NAPMNMBL Index", "NAPMNMEL Index"]

data = bbgui.bdh(["NAPMPMI Index"] + manu_list + ["NAPMNMI Index"] + service_list, "PX_LAST", START_DATE, END_DATE,
                 interval='MONTHLY')

data.loc[:, "Manu Share"] = data[manu_list].mean(axis=1)
data.loc[:, "Service Share"] = data[service_list].mean(axis=1)

fig, axes = plt.subplots(1, 2, figsize=(2 * 12, 9))
ax1 = axes[0]
ax1.plot(data.index, data["NAPMPMI Index"], color='blue', label='ISM Manufacturing PMI (L)')
ax1.axhline(y=50, color='gray', linestyle='--', linewidth=1)  # Adding horizontal line at 50
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
ax3.axhline(y=50, color='gray', linestyle='--', linewidth=1)  # Adding horizontal line at 50
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
del data, fig, ax1, ax2, ax3, ax4

# India govt expenditure excl interest payments Yoy
START_DATE = G_END_DATE.replace(year=G_END_DATE.year - 4)
data = blp.bdh("INFFTOEX Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
data.index = pd.to_datetime(data.index)  # gstctc
int_data = blp.bdh("INFFNPRI Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
int_data.index = pd.to_datetime(int_data.index)  # Ensure index is datetime for year extraction
data["Govt Expenses excl Interest"] = data['INFFTOEX Index'] - int_data['INFFNPRI Index']
yoy_growth_excl_interest = data["Govt Expenses excl Interest"].pct_change(12) * 100
latest = yoy_growth_excl_interest.last_valid_index().strftime("%b %y")
gst = blp.bdh("GSTXTXCO Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del data, int_data, yoy_growth_excl_interest, fig, ax1, ax2, gst

# Change in 5y yields and 2x5 spreads and CCYs
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
    "EUR": "USDEUR Curncy","PLN": "USDPLN Curncy", "HUF": "USDHUF Curncy"
}
START_DATE = G_END_DATE - timedelta(days=2 * 365)

# 5y yields beta
five_year_tickers = list(five_year_dict.values())
data_5y = blp.bdh(five_year_tickers, "LAST PRICE", START_DATE, G_END_DATE, "QtTyp = Y").droplevel(1, axis=1)
inverse_five_year_dict = {v: k for k, v in five_year_dict.items()}
data_5y.rename(columns=inverse_five_year_dict, inplace=True)
data_5y.index = pd.to_datetime(data_5y.index)
data_5y = data_5y.iloc[:-1].ffill(limit = 10)
latest = data_5y.index[-1].strftime("%d-%b-%Y")
calculate_and_plot(data_5y, f"Actual vs Expected Move in 5-Year Yields (Last 20 Days) as of {latest}", "5Y Implied By Beta.png",method='yield')

# 2x5 yields beta
two_year_tickers = list(two_year_dict.values())
data_2y = blp.bdh(two_year_tickers, "LAST PRICE", START_DATE, G_END_DATE, "QtTyp = Y").droplevel(1, axis=1)
inverse_two_year_dict = {v: k for k, v in two_year_dict.items()}
data_2y.rename(columns=inverse_two_year_dict, inplace=True)
data_2y.index = pd.to_datetime(data_2y.index)
data_2y = data_2y.iloc[:-1].ffill(limit = 10)
common_dates = data_5y.index.intersection(data_2y.index)
data_5y = data_5y.loc[common_dates]
data_2y = data_2y.loc[common_dates]
common_currencies = data_5y.columns.intersection(data_2y.columns)
data_5y = data_5y[common_currencies]
data_2y = data_2y[common_currencies]
spread_data = data_5y - data_2y
calculate_and_plot(spread_data, f"Actual vs Expected Move in 2x5 Spreads (Last 20 Days) as of {latest}", "2x5 Implied By Beta.png", method = 'yield', multiply_by_100=True)

#CCY beta
five_year_tickers_fx = list(fx_dict.values())
data_fx = blp.bdh(five_year_tickers_fx, "LAST PRICE", START_DATE, G_END_DATE).droplevel(1, axis=1)
inverse_fx_dict = {v: k for k, v in fx_dict.items()}
data_fx.rename(columns=inverse_fx_dict, inplace=True)
data_fx.index = pd.to_datetime(data_fx.index)
data_fx = data_fx.iloc[:-1].ffill(limit = 5)
calculate_and_plot(data_fx,"Actual vs Expected Move in USDCCY (Last 20 Days)","USDCCY Implied By Beta.png",method='fx')
del data_2y, data_5y, spread_data, data_fx



# Euro Area PMI vs Sentix
end_date = (datetime.today().replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
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
del pmi, sentix, fig, ax1, ax2

# Australia Westpac Leading Index
START_DATE = datetime(2010, 1, 1)
gdp = blp.bdh("AUNAGDPY Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
gdp.index = pd.to_datetime(gdp.index)
westpac = blp.bdh("AULILEAD Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del gdp, westpac, fig, ax1, ax2

# ACM Term Premium Vs ISM
START_DATE = datetime(2010, 1, 1)
ism = blp.bdh("NAPMPMI Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
ism.index = pd.to_datetime(ism.index)
usts = blp.bdh(["USGG5YR Index", "USGG10YR Index", "USGG30YR Index","ACMTP10  Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
usts.index = pd.to_datetime(usts.index)
usts['5s10s30s'] = 2*usts['USGG10YR Index'] - usts['USGG5YR Index'] - usts['USGG30YR Index']
acm = blp.bdh("ACMTP10  Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del ism, usts, acm, fig, ax1, ax2

# SG Tradable Core Inflation vs Average Inflation of Trading Partners
START_DATE = datetime(2012, 12, 31)
DIVISOR = 0.6582
sg_core = blp.bdh("SMASCORE Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
sg_core.index = pd.to_datetime(sg_core.index)
latest = sg_core.last_valid_index().strftime("%b %y")
other_cpi = ["CPI YOY Index", "CNCPIYOY Index", "MACPIYOY Index", "ECCPEMUY Index", "TWCPIYOY Index",
             "JNCPIYOY Index", "KOCPIYOY Index", "HKCPIY Index", "IDCPIY Index", "THCPIYOY Index"]
trading_partners = blp.bdh(other_cpi, "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
trading_partners.index = pd.to_datetime(trading_partners.index)
trading_partners['Average_CPI'] = trading_partners.mean(axis=1)
trading_partners = trading_partners.shift(periods=3, freq='M')
non_tradables_list = ["SICPHSES Index", "SICWHSES Index", "SICPEDUC Index", "SICWEDUC Index", "SICPMEDT Index",
                      "SICWMEDT Index", "SICPFUEL Index", "SICWFUEL Index",
                      "SICPCOMM Index", "SICWCOMM Index", "SICPPUB Index", "SICWPUB Index", ]
non_tradebles_df = blp.bdh(non_tradables_list, "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del sg_core, trading_partners, non_tradebles_df, fig, ax1

# US Share of Total Unemployment
START_DATE = datetime(2000, 1, 1)
unemployment = blp.bdh(["USJLOSER Index", "USJLJOBL Index","USJLREEN Index","USJLNENT Index","USUETOT Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
unemployment.index = pd.to_datetime(unemployment.index)
latest = unemployment.last_valid_index().strftime("%b %y")
unemployment["Demand"] = (unemployment["USJLOSER Index"]/unemployment["USUETOT Index"]) * 100
unemployment["Supply"] = ((unemployment["USJLJOBL Index"] + unemployment["USJLREEN Index"] + unemployment["USJLNENT Index"])/unemployment["USUETOT Index"]) * 100
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
ax1_twin.axhline(50, color='black', lw=1, linestyle = '--')
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="upper right")
ax1.tick_params(axis='x', rotation=60)
ax1.yaxis.set_major_locator(MultipleLocator(10))
ax1_twin.yaxis.set_major_locator(MultipleLocator(10))
ax1.set_ylim(0,100)
ax1_twin.set_ylim(0,100)
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
ax2_twin.axhline(50, color='black', lw=1, linestyle = '--')
lines = line3 + line4
labels = [l.get_label() for l in lines]
ax2.legend(lines, labels, loc="upper right")
ax2.tick_params(axis='x', rotation=60)
ax2.yaxis.set_major_locator(MultipleLocator(10))
ax2_twin.yaxis.set_major_locator(MultipleLocator(10))
ax2.set_ylim(0,100)
ax2_twin.set_ylim(0,100)
plt.savefig(Path(G_CHART_DIR, r"US Share of Total Unemployment", bbox_inches='tight'))
del unemployment, latest_unemployment, fig, ax1, ax1_twin, ax2, ax2_twin

# India Fiscal Balance vs Total Credit Change
START_DATE = datetime(2012, 1, 1)
budget = blp.bdh("EHBBIN Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
budget.index = pd.to_datetime(budget.index)
credit = blp.bdh("IBCDINDT Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del budget, credit, fig, ax1, ax2

# JPY Exporter Breakeven Rate
# Breakeven values can be found at https://www.esri.cao.go.jp/en/stat/ank/ank-e.html
breakeven_values = [
    175.4, 140.9, 128.1, 133.3, 129.7, 126.2, 124.0, 117.5, 107.8, 104.0,
    106.2, 110.4, 112.7, 106.5, 107.0, 115.3, 114.9, 105.9, 102.6, 104.5,
    106.6, 104.7, 97.3, 92.9, 86.3, 82.0, 83.9, 92.2, 99.0, 103.2,
    100.5, 100.6, 99.8, 100.2, 99.8, 101.5, 114.5, 123.0
]
START_DATE = datetime(1986, 1, 1)
END_DATE = datetime(2024, 12, 1)
usdjpy = blp.bdh("USDJPY Curncy", "PX_LAST", START_DATE, END_DATE,Per = 'Y').droplevel(1, axis=1)
usdjpy.index = pd.to_datetime(usdjpy.index)
usdjpy["Breakeven Rate"] = breakeven_values
usdjpy["Difference"] = usdjpy["USDJPY Curncy"] - usdjpy["Breakeven Rate"]
latest = usdjpy.last_valid_index().strftime("%b %y")
fig, ax1 = plt.subplots(figsize=(25, 7))
line1 = ax1.plot(usdjpy.index, usdjpy["USDJPY Curncy"], label='USDJPY (Left Axis)', color='g')
line2 = ax1.plot(usdjpy.index, usdjpy["Breakeven Rate"], label='Breakeven Rate (Left Axis)', color='b')
ax1.axhline(0, color='red', lw=1, linestyle = '--')
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
ax2.axhline(0, color='black', lw=1, linestyle = '--')
lines = line1 + line2 
labels = [l.get_label() for l in lines]
fig.legend(loc="upper left", bbox_to_anchor=(0.125, 0.88))
ax1.tick_params(axis='x', rotation=60)
ax1.set_ylim(60, None)
ax1.grid(True)
plt.savefig(Path(G_CHART_DIR, r"USDJPY vs Japan Exporter Breakeven Rate", bbox_inches='tight'))
del usdjpy, fig, ax1, ax2

# Fed Wage growth vs Unemployment gap
START_DATE = datetime(2012, 1, 1)
data = blp.bdh(["WGTROVRA Index", "CBOPNRUE Index", "USURTOT Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del data, fig, ax1, ax2

# NZ Filled Jobs
START_DATE = datetime(2018, 1, 1)
employment = blp.bdh("NZEMFJAS Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
employment.index = pd.to_datetime(employment.index)
latest = employment.last_valid_index().strftime("%b %y")
unemployment_rate = blp.bdh("NZLFUNER Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
unemployment_rate.index = pd.to_datetime(unemployment_rate.index)
employment['Monthly Change'] = employment['NZEMFJAS Index'].pct_change() * 100
employment['Yearly Change'] = employment['NZEMFJAS Index'].pct_change(12) * 100
employment['NZEMFJAS Index'] = employment['NZEMFJAS Index']/1000
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
ax2.axhline(0, color='black', lw=1, linestyle = '--')
ax2.yaxis.set_major_locator(MultipleLocator(0.5))
ax3 = ax1.twinx()
ax3.plot(unemployment_rate.index, unemployment_rate['NZLFUNER Index'], color="blue", linestyle='--', label="Unemployment Rate (Second Right Axis)")
ax3.set_ylabel("Unemployment Rate (%)", color="blue")
ax3.spines['right'].set_position(('outward', 60))  # Offset the second right y-axis
ax3.tick_params(axis='y', colors='blue')
ax3.yaxis.set_major_locator(MultipleLocator(0.5))
ax3.invert_yaxis()
fig.legend(loc="upper left", bbox_to_anchor=(0.125, 0.88))
ax1.tick_params(axis='x', rotation=60)
plt.savefig(Path(G_CHART_DIR, r"NZ Filled Jobs Seasonally Adjusted", bbox_inches='tight'))
del employment, unemployment_rate, fig, ax1, ax2, ax3

# Generic 10y swap Spread
debt_list = {"US":"GDDI111G Index", "AUD": "GDDI193G Index", "NZD": "GDDI196C Index", "GBP": "GDDI112G Index", "CAD": "GDDI156G Index", "GER": "GDDI134G Index"}
debt_data = blp.bdp(list(debt_list.values()), "LAST PRICE")
inverse_debt = {v: k for k, v in debt_list.items()}
debt_data.rename(index=inverse_debt, inplace=True)
yield_list = ["USGG10YR Index", "GTAUD10Y Govt", "GTNZD10Y Govt", "GTGBP10Y Govt", "GTCAD10Y Govt", "GTDEM10Y Govt", "USOSFR10 Curncy", "ADSWAP10 Curncy", "ADSO10 Curncy", "NDSWAP10 Curncy", "NDSO10 Curncy", "BPSWS10 Curncy", "CDSO10 BGN Curncy", "EUSA10 Curncy"] 
yield_data = blp.bdh(yield_list, "LAST PRICE", G_END_DATE - BDay(7), G_END_DATE - BDay(1), "QtTyp = Y").ffill(limit=3).iloc[-1:, :].droplevel(1, axis=1)
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
    ax.annotate(txt, (plot_df['% GDP'][i], plot_df['Swap Spread'][i]), textcoords="offset points", xytext=(5,5), ha='center')
ax.set_xlim(0, plot_df['% GDP'].max() + 10)
ax.set_title('Generic 10y Bond - Swap vs Government Debt (% GDP)')
ax.set_xlabel('Government Debt (% GDP)')
ax.set_ylabel('10y Generic Bond - Swap Spread (Basis Points)')
ax.axhline(0, color='black', linewidth=0.5)  
ax.grid(True, linestyle='--', linewidth=0.5)
plt.savefig(Path(G_CHART_DIR, r"10Y Generic Swap Spread", bbox_inches='tight'))
del yield_data, plot_df, fig, ax

# ACM Term Premium Vs Fly
START_DATE = datetime(2010, 1, 1)
usts = blp.bdh(["USOSFR1 Curncy", "S0490FS 1Y1Y BLC Curncy", "S0490FS 2Y1Y BLC Curncy","ACMTP02 Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
usts.index = pd.to_datetime(usts.index)
usts['fly'] = (2*usts['S0490FS 1Y1Y BLC Curncy'] - usts['USOSFR1 Curncy'] - usts['S0490FS 2Y1Y BLC Curncy']) * 100
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
del usts, fig, ax1, ax2

# ACM Term Premium Correlation
START_DATE = datetime(2010, 1, 1)
usts = blp.bdh(["USGG5YR Index", "USGG10YR Index", "USGG30YR Index","ACMTP10 Index","ACMTP02 Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
usts.index = pd.to_datetime(usts.index)
usts['5s10s30s'] = 2*usts['USGG10YR Index'] - usts['USGG5YR Index'] - usts['USGG30YR Index']
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
del usts, fig, ax1, ax2

# Orders Inventory Change vs IP
START_DATE = datetime(2000, 1, 1)
gdp = blp.bdh(["GDNSCHWN Index", "UKGRABMI Index", "JGDPOGDP Index", "ENGKEMU Index", "CGE9MP Index", "USDJPY Curncy", "USDGBP Curncy", "USDEUR Curncy", "USDCAD Curncy"], "PX_LAST", START_DATE, G_END_DATE, Per = 'M').droplevel(1, axis=1)
gdp.index = pd.to_datetime(gdp.index)
latest = gdp.last_valid_index().strftime("%b %y")
gdp["UKGRABMI Index"]  = gdp["UKGRABMI Index"]/1000
gdp["ENGKEMU Index"]  = gdp["ENGKEMU Index"]/1000
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
ip = blp.bdh(["IP Index", "JNIP Index", "UKIPI Index", "EUITEMU Index", "CAGPINDP Index"], "PX_LAST", START_DATE, G_END_DATE, Per = 'Q').droplevel(1, axis=1)
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
orders_inventories = blp.bdh(["NAPMNEWO Index", "NAPMINV Index"], "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del gdp, ip, aligned_ip, orders_inventories, ax1, ax2, fig

# Employment Cost Index vs NFIB Labour Quality
START_DATE = datetime(2000, 1, 1)
quality = blp.bdh("SBOIQUAL Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
employment = blp.bdh("ECICCVYY Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
ax1.axhline(0, color='black', lw=1, linestyle = '--')
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="upper left")
ax1.tick_params(axis='x', rotation=60)
plt.savefig(Path(G_CHART_DIR, r"Employment Cost Index vs NFIB Labour Quality", bbox_inches='tight'))
del quality, employment, fig, ax1, ax2

#USTs vs Uncertainty Index
START_DATE = datetime(2000, 1, 1)
uncertainty = blp.bdh("EPUCGLCP Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
usts = blp.bdh("GIND10YR Index", "PX_LAST", START_DATE, G_END_DATE).droplevel(1, axis=1)
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
del uncertainty, usts, fig, ax1, ax2

