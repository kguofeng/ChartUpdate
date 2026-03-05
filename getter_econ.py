import os
from datetime import datetime
from pathlib import Path

from tqdm import tqdm
from xbbg import blp

from exante_utils import *

START_Date = datetime(1980, 1, 1).strftime("%Y/%m/%d")
END_Date = datetime.today().strftime("%Y/%m/%d")

BUFFER_DIR = "buffer_economy"
if not os.path.exists(BUFFER_DIR):
    os.mkdir(BUFFER_DIR)


def get_econ():
    econ_ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="BbgEcon", skiprows=2)

    explain = econ_ticker_df[['Code', 'Country', 'Type']].set_index('Code')
    # fwd swap
    fwd_ticker = econ_ticker_df["1y1y swap rate"].values.flatten().tolist()
    fwddata = blp.bdp(fwd_ticker, "PX_LAST")
    fwddata2 = econ_ticker_df.merge(fwddata, how='left', left_on='1y1y swap rate', right_index=True)[
        ['Code', 'Country', 'Type', '1y1y swap rate', 'px_last']].set_index('Code')
    fwddata2.to_excel("ecom_fwdrate.xlsx")

    # econ ticker
    original_list = econ_ticker_df.columns.tolist()
    elements_to_remove = ['Code', 'Country', 'Type', '1y1y swap rate']
    updated_list = [x for x in original_list if x not in elements_to_remove]

    econ_ticker = econ_ticker_df[updated_list].values.flatten().tolist()

    data = blp.bdp(econ_ticker, "PX_LAST")
    data['Country'] = data.index.str[4:6]
    data['EconType'] = data.index.str[2:4]
    data['Year'] = data.index.str[7:9]
    for i, j in zip(['BB', 'CA', 'GD', 'PI', 'UP'], ['Budget', 'Current Act', 'GDP', 'CPI', 'Unemployment']):
        print(i)
        slice_data = data[data['EconType'] == i]
        _pivot = slice_data.pivot(index='Country', columns='Year', values='px_last')
        _pivot2 = _pivot.merge(explain, left_index=True, right_index=True)
        for col in ['Country', 'Type']:
            col_s = _pivot2.pop(col)
            _pivot2.insert(0, col, col_s)
        _pivot2.to_excel(f"ecom_{j}.xlsx")


def get_econ_export():
    econ_ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="BbgExport", skiprows=2)
    freqs = econ_ticker_df['Freq'].unique()
    export_table = pd.DataFrame(columns=["Country", "Export YoY%", "Export UpdateTime"])
    for id, row in tqdm(econ_ticker_df.iterrows()):
        _raw_s = blp.bdh(row['Export Ticker'], "PX_LAST", START_Date, END_Date)
        country = row['Country']
        if row['SA'] == "Y":
            _raw_s_sa = _raw_s
        else:
            _raw_s_sa = _raw_s

        if row['DataType'] == "Outright":
            _raw_s_sa_YoY = _raw_s_sa.pct_change(12) * 100
        else:
            _raw_s_sa_YoY = _raw_s_sa
        _raw_s_sa_YoY.to_pickle(Path(BUFFER_DIR, f"export_{country}"))

        country = row["Country"]
        export_table.loc[country, "Country"] = country
        export_table.loc[country, "Export YoY%"] = _raw_s_sa_YoY.iloc[-1, 0]
        export_table.loc[country, "Export UpdateTime"] = _raw_s_sa_YoY.index[-1].strftime("%d-%b-%Y")
    export_table.to_excel(f"ecom_export.xlsx")
    print("export data has generated successfully")


if __name__ == '__main__':
    get_econ()
    get_econ_export()
