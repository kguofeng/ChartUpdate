import calendar
import datetime
import os
from pathlib import Path

from exante_utils import *

Buffer = "buffer_credit_impulse"
if not os.path.exists(Path(Buffer)):
    os.mkdir(Path(Buffer))


def get_end_date(year, month):
    _, last_day = calendar.monthrange(year, month)
    return last_day


# def get_credit_impulse_table(countryList, startDate):
#     ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="Exante")
#     ticker_df = ticker_df[ticker_df['Country'].isin(countryList)]

#     tickers = (ticker_df[ticker_df['Type'] == 'EM']['6m']).tolist()
#     tickers_string = ','.join(tickers)
#     startDate_string = datetime.date(startDate.year, startDate.month,
#                                      get_end_date(startDate.year, startDate.month)).strftime("%Y-%m-%d")
#     cpl_data = get_data(tickers_string, startDate=startDate_string, endDate=None)

#     old_col = cpl_data.columns
#     new_col = []
#     for i in old_col:
#         j = ticker_df[ticker_df['Code'] == i[:2]]['Country'].tolist()[0]
#         new_col.append(j)

#     cpl_data.columns = new_col

#     cpl_table = pd.DataFrame(columns=['6month Credit Impulse %', 'CreditImpulse UpdateTime'])
#     for c in cpl_data.columns:
#         s = cpl_data[c].dropna()
#         cpl_table.loc[c, '6month Credit Impulse %'] = round(s.iloc[-1] * 100, 1)
#         cpl_table.loc[c, 'CreditImpulse UpdateTime'] = s.index[-1][:10]
#     cpl_table.index.name = 'Country'
#     return cpl_table


def get_credit_impulse_table6m(startDate):
    ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="Exante")
    tickers = (ticker_df[ticker_df['Type'] == 'EM']['6m']).tolist()
    tickers_string = ','.join(tickers)
    startDate_string = datetime.date(startDate.year, startDate.month, get_end_date(
        startDate.year, startDate.month)).strftime("%Y-%m-%d")
    cpl_data = get_data(
        tickers_string, startDate=startDate_string, endDate=None).sort_index()

    old_col = cpl_data.columns
    new_col = []
    for i in old_col:
        j = ticker_df[ticker_df['Code'] == i[:2]]['Country'].tolist()[0]
        new_col.append(j)

    cpl_data.columns = new_col
    cpl_data.to_csv(Path(Buffer, "exante_creditimpulse_6m.csv"))

    cpl_table = pd.DataFrame(
        columns=['6month Credit Impulse %', 'CreditImpulse UpdateTime'])
    for c in cpl_data.columns:
        s = cpl_data[c].dropna()
        cpl_table.loc[c, '6month Credit Impulse %'] = round(s.iloc[-1] * 100, 1)
        cpl_table.loc[c, 'CreditImpulse UpdateTime'] = s.index[-1][:10]
    cpl_table.index.name = 'Country'
    return cpl_table


def get_credit_impulse_table12m(startDate):
    ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="Exante")
    tickers = (ticker_df[ticker_df['Type'] == 'EM']['12m']).tolist()
    tickers_string = ','.join(tickers)
    startDate_string = datetime.date(startDate.year, startDate.month, get_end_date(
        startDate.year, startDate.month)).strftime("%Y-%m-%d")
    cpl_data = get_data(
        tickers_string, startDate=startDate_string, endDate=None)

    old_col = cpl_data.columns
    new_col = []
    for i in old_col:
        j = ticker_df[ticker_df['Code'] == i[:2]]['Country'].tolist()[0]
        new_col.append(j)

    cpl_data.columns = new_col
    cpl_data.to_csv(Path(Buffer, "exante_creditimpulse_12m.csv"))

    cpl_table = pd.DataFrame(
        columns=['12month Credit Impulse %', 'CreditImpulse UpdateTime'])
    for c in cpl_data.columns:
        s = cpl_data[c].dropna()
        cpl_table.loc[c, '12month Credit Impulse %'] = round(s.iloc[-1] * 100, 1)
        cpl_table.loc[c, 'CreditImpulse UpdateTime'] = s.index[-1][:10]
    cpl_table.index.name = 'Country'
    return cpl_table


def get_credit_impulse_tableM2(startDate):
    ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="Exante")
    tickers = (ticker_df[ticker_df['Type'] == 'EM']['M2']).dropna().tolist()

    from xbbg import blp
    m2_data = blp.bdh(tickers, 'PX_LAST', startDate, datetime.datetime.today()).droplevel(1, axis=1)

    for col in m2_data.columns:
        m2_data[col] = m2_data[col].interpolate(method='linear', limit_area='inside')

    m2_data_yoy = m2_data.pct_change(12)
    m2_data_yoy_12m = m2_data_yoy.diff(12)
    m2_data_yoy_6m = m2_data_yoy.diff(6)

    _m2_cry_dict = dict(zip(ticker_df['M2'], ticker_df['Country']))
    old_col = m2_data_yoy_12m.columns
    new_col = []
    for i in old_col:
        j = _m2_cry_dict[i]
        new_col.append(j)

    m2_data_yoy_12m.columns = new_col
    m2_data_yoy_12m.to_csv(Path(Buffer, "m2_creditimpulse_12m.csv"))
    m2_data_yoy_6m.columns = new_col
    m2_data_yoy_6m.to_csv(Path(Buffer, "m2_creditimpulse_6m.csv"))

    m2_table = pd.DataFrame(
        columns=['6month M2 Credit Impulse %', '12month M2 Credit Impulse %', 'M2 CreditImpulse UpdateTime'])

    for c in m2_data_yoy_12m.columns:
        s = m2_data_yoy_12m[c].dropna()
        m2_table.loc[c, '12month M2 Credit Impulse %'] = round(s.iloc[-1] * 100, 1)
        m2_table.loc[c, 'M2 CreditImpulse UpdateTime'] = s.index[-1].strftime("%Y-%b-%d")
        s = m2_data_yoy_6m[c].dropna()
        m2_table.loc[c, '6month M2 Credit Impulse %'] = round(s.iloc[-1] * 100, 1)

    m2_table.index.name = 'Country'

    return m2_table


if __name__ == '__main__':
    startDate = datetime.datetime.today() - datetime.timedelta(days=30 *
                                                                    12 * 30)  # get 30 year data
    impusle6 = get_credit_impulse_table6m(startDate)
    impusle6.to_excel("impulse6m.xlsx")
    impusle12 = get_credit_impulse_table12m(startDate)
    impusle12.to_excel("impulse12m.xlsx")
    impuslem2 = get_credit_impulse_tableM2(startDate)
    impuslem2.to_excel("impulsem2.xlsx")
