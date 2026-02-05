import os
from pathlib import Path
import time
import shutil
import matplotlib.pyplot as plt
import statsmodels.api as sm
import pandas as pd
from chart_utils import *
from ecom_utils import *


def get_cement(source):
    G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase\Ecom"  # general chart directory
    if not os.path.exists(G_CHART_DIR):
        os.mkdir(G_CHART_DIR)

    # load data
    if source == 'ceic':
        data = pd.read_excel("cemendatat_ceic.xlsx", skiprows=1, index_col=0).sort_index()
        data.index.name = "Date"
        data.rename(columns=translate_dict, inplace=True)
        sa_data = pd.DataFrame(index=data.index)
        x13_path = r'O:/Tian/Portal/Charts/ChartUpdate/WinX13/x13as'

        cement_df = pd.DataFrame(data.columns.T, index=range(len(data.columns)), columns=['Country'])

        # Perform seasonal adjustment using X-13-ARIMA-SEATS
        for col in data:
            print(f"seasonally adjusting {col}")
            s = data[col]
            s = s[s.first_valid_index():]
            if col == 'China':
                s = set_freq(s, China=True)
            else:
                s = set_freq(s, False)

            if s.isna().sum() > 3:
                print(f"-------{col} has more than 3 consecutive NA------")
            s = s.fillna(method="ffill", limit=3)  # todo change ffill to kalman fitler fill?
            results = sm.tsa.x13_arima_analysis(s, x12path=x13_path, tempdir=r'Temp',
                                                print_stdout=True)
            sa_data[col] = results.seasadj

        #sa_data = data.copy()
    elif source == 'haver':
        tocopy = Path("G:\Emerging Markets\Faiz", "cement.xlsx")
        copyto = Path(os.getcwd(), "cement_haver.xlsx")
        shutil.copy2(tocopy, copyto)
        print(f"Copied data file from {tocopy} to {copyto}")
        print(f"File last updated time is {time.ctime(os.path.getmtime(copyto))}")
        data = pd.read_excel(copyto, skiprows=range(1, 10), index_col=0)
        data.index = pd.to_datetime(data.index, format='%b-%y')
        data.index.name = "Date"

        cement_df = pd.DataFrame(data.columns.T, index=range(len(data.columns)), columns=['Country'])

        sa_data = data.copy()
    else:
        raise NotImplementedError("Unrecognized source")

    data_YoY = sa_data.pct_change(12, fill_method=None)
    data_YoY3mma = sa_data.pct_change(12, fill_method=None).rolling(3).mean()

    chart_dict = {}
    for col in data.columns:
        print(col)
        country = col
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
            f"Cement Production - {country}; updated at {data_YoY3mma[col].dropna().index[-1].strftime('%d-%b-%Y')}")

        ax2 = axes[1]
        ax2.plot(data.index, data[col],
                 label="Cement Production Raw", color="red")
        ax2.plot(sa_data.index, sa_data[col].rolling(3).mean(),
                 label="Cement Production 3mma sa", color="blue")
        ax2.legend(loc=3, fontsize=8)
        ax2.set_title(
            f"Cement Production - {country}")
        # plt.show()
        _p = Path(G_CHART_DIR, f"Cement Production {country}")
        plt.savefig(_p)
        chart_dict[country] = _p.as_uri() + ".png"
        del fig, axes, ax1, ax2

    # add to the cement df
    for i, row in cement_df.iterrows():
        s = data_YoY3mma[row['Country']].dropna()
        cement_df.loc[i, "CementProd YoY 3mma %"] = round(s.values[-1] * 100, 1)
        cement_df.loc[i, "CementProd UpdateTime"] = s.index[-1].strftime('%d-%b-%Y')
        cement_df.loc[i, "CementProd Chart"] = chart_dict[row['Country']]

        s = data[row['Country']].dropna()
        cement_df.loc[i, "CementProd (volume/index) "] = round(s.values[-1], 0)
    cement_df.loc[:, "CementProd (volume/index) "] = cement_df.loc[:, "CementProd (volume/index) "].apply('{:,.0f}'.format)


    # make it a clickable link
    cement_df.loc[:, "CementProd Chart"] = cement_df.loc[:, "CementProd Chart"].apply(
        lambda x: make_clickable(x, "link"))
    return cement_df


if __name__ == '__main__':
    cement_df = get_cement("haver")
    cement_df.to_excel("cement.xlsx", index=False)
