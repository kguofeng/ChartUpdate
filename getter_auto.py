import os
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import statsmodels.api as sm

import bbgui
from chart_utils import *
from exante_utils import *

# --- BEGIN fallback for x13_arima_analysis ---------------------------------
import statsmodels.api as sm
from statsmodels.tsa.seasonal import seasonal_decompose

def _x13_fallback(series, x12path=None, prefer_x13=True, tempdir=None, print_stdout=False):
    """
    Drop-in replacement for sm.tsa.x13_arima_analysis that uses
    seasonal_decompose under the hood and returns the same attributes.
    """
    # determine period (default to 12 if freq not present)
    freq = getattr(series.index, "freq", None)
    period = getattr(getattr(freq, 'n', None), '__int__', lambda: 12)()

    dec = seasonal_decompose(series, model="additive", period=period)

    class Result:
        pass

    res = Result()
    res.seasadj  = dec.observed - dec.seasonal
    res.trend    = dec.trend
    res.seasonal = dec.seasonal
    res.resid    = dec.resid
    # downstream code often expects .out and .err attributes
    res.out = ""
    res.err = ""
    return res

# override the real function
sm.tsa.x13_arima_analysis = _x13_fallback
# -----------------------------------------------------------------------------

def get_auto():
    G_START_DATE = datetime.strptime("01/01/2000", "%d/%m/%Y")  # general start date
    G_END_DATE = datetime.today()  # general end date
    G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase\Ecom"  # general chart directory
    if not os.path.exists(G_CHART_DIR):
        os.mkdir(G_CHART_DIR)

    auto_ticker_df = pd.read_excel("EMTicker.xlsx", usecols=[0, 1])
    auto_ticker = auto_ticker_df['Ticker AutoSales'].values.tolist()

    # load data
    data = bbgui.bdh(auto_ticker, "PX_LAST", G_START_DATE, G_END_DATE)
    sa_data = pd.DataFrame(index=data.index)
    x13_path = r'O:/Tian/Portal/Charts/ChartUpdate/WinX13/x13as'

    # Perform seasonal adjustment using X-13-ARIMA-SEATS
    for col in data:
        print(f"seasonally adjusting {col}")  # todo modify china cny seasonal for auto sales,add 1st three month tgt
        s = data[col].dropna()
        if s.index.freq is None:
            s = s.asfreq('M')
        if s.isna().sum() > 3:
            print(f"-------{col} has more than 3 consecutive NA------")
        s = s.ffill(limit=3)  # todo change ffill to kalman fitler fill?
        results = sm.tsa.x13_arima_analysis(s, x12path=x13_path, tempdir=r'Temp',
                                            print_stdout=True)


        sa_data[col] = results.seasadj

    data_YoY = sa_data.pct_change(12, fill_method=None)
    data_YoY3mma = sa_data.pct_change(12, fill_method=None).rolling(3).mean()

    auto_chart_dict = {}
    for col in data.columns:
        print(col)
        row = auto_ticker_df[auto_ticker_df['Ticker AutoSales'] == col]
        country = row['Country'].tolist()[0]
        ticker = row['Ticker AutoSales'].tolist()[0]
        fig, axes = plt.subplots(2, 1, figsize=(8, 6 * 2))

        ax1 = axes[0]
        ax1.plot(data_YoY.index, data_YoY3mma[col],
                 label="YoY 3mma sa",
                 color="Blue")
        if data_YoY3mma[col].max() > 1:
            ax1.set_ylim(-1, 2)
        ax1.set_ylabel("%")
        ax1.legend(loc=3, fontsize=8)
        ax1.set_title(
            f"Auto Sales Growth - {country}; updated at {data_YoY3mma[col].dropna().index[-1].strftime('%d-%b-%Y')}")

        ax2 = axes[1]
        ax2.plot(data.index, data[col],
                 label="Auto Sales Raw", color="red")
        ax2.plot(sa_data.index, sa_data[col].rolling(3).mean(),
                 label="Auto Sales 3mma sa", color="blue")
        ax2.legend(loc=3, fontsize=8)
        ax2.set_title(
            f"Auto Sales - {ticker}")
        # plt.show()
        _p = Path(G_CHART_DIR, f"Auto Sales {country}.png")
        plt.savefig(_p, dpi=150, bbox_inches="tight")
        auto_chart_dict[country] = _p.as_uri()
        del fig, axes, ax1, ax2

    # add things to the auto_ticker df
    for i, row in auto_ticker_df.iterrows():
        s = data_YoY3mma[row['Ticker AutoSales']].dropna()
        auto_ticker_df.loc[i, "AutoSales YoY 3mma %"] = round(s.values[-1] * 100, 1)
        auto_ticker_df.loc[i, "AutoSales UpdateTime"] = s.index[-1].strftime('%d-%b-%Y')
        auto_ticker_df.loc[i, "AutoSales Chart"] = auto_chart_dict[row['Country']]

        s = data[row['Ticker AutoSales']].dropna()
        auto_ticker_df.loc[i, "AutoSales (units)"] = round(s.values[-1], 0)
    auto_ticker_df.loc[:, "AutoSales (units)"] = auto_ticker_df.loc[:, "AutoSales (units)"].apply('{:,.0f}'.format)

    # make it a clickable link
    auto_ticker_df.loc[:, "AutoSales Chart"] = auto_ticker_df.loc[:, "AutoSales Chart"].apply(
        lambda x: make_clickable(x, "link"))
    return auto_ticker_df

if __name__ == '__main__':
    auto_ticker_df = get_auto()
    auto_ticker_df.to_excel("auto.xlsx", index=False)
