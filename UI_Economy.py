import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from dash import html, Dash, Input, Output, dash_table, dcc

import getter_credit_impulse
from ecom_utils_color import discrete_background_color_bins
from exante_utils import *
from getter_auto import get_auto
from getter_cement import get_cement
from getter_econ import get_econ, get_econ_export

parser = argparse.ArgumentParser()
# input options, data source
parser.add_argument("-d", "--debug", default="True",
                    help="run in debug mode or not, Defualt: True")
opts = parser.parse_args()

G_START_DATE = datetime.strptime("01/01/2000", "%d/%m/%Y")  # general start date
G_END_DATE = datetime.today()  # general end date
G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase\Ecom"  # general chart directory

if not os.path.exists(G_CHART_DIR):
    os.makedirs(G_CHART_DIR, exist_ok=True)

BufferCredit = "buffer_credit_impulse"

# ensure buffer directory exists
Path(BufferCredit).mkdir(parents=True, exist_ok=True)

DEBUG = bool(eval(opts.debug))

# DEBUG = True
print(f"run in debug mode: {DEBUG}")
DISPLAY_YEAR = datetime.today().strftime("%y")  # '26' in 2026
SORT_YEAR = DISPLAY_YEAR
PAGE_SIZE = 10

app = Dash(__name__)
chart_style = {'width': '150vh', 'height': '65vh', 'display': 'flex'}
chart_style1 = {'width': '50vh', 'height': '65vh', 'display': 'inline-block', 'marginRight': 20}
chart_style2 = {'width': '75vh', 'height': '65vh', 'display': 'inline-block', 'marginRight': 50}

# get auto
if DEBUG:
    auto_ticker_df = pd.read_excel("auto.xlsx")
else:
    auto_ticker_df = get_auto()
auto_table = dash_table.DataTable(auto_ticker_df.to_dict("records"),
                                  [{"name": i, "id": i} for i in auto_ticker_df.columns if i != "AutoSales Chart"] + [
                                      {"name": "AutoSales Chart", "id": "AutoSales Chart",
                                       "presentation": "markdown"}],
                                  id="auto_table",
                                  page_size=PAGE_SIZE,
                                  filter_action='native',
                                  sort_action='native',
                                  sort_mode='multi',
                                  sort_by=[{'column_id': "AutoSales YoY 3mma %", 'direction': 'desc'}],
                                  markdown_options={"html": True})

# get cement
if DEBUG:
    cement_df = pd.read_excel("cement.xlsx")
else:
    cement_df = get_cement("haver")
cement_table = dash_table.DataTable(cement_df.to_dict("records"),
                                    [{"name": i, "id": i} for i in cement_df.columns if i != "CementProd Chart"] + [
                                        {"name": "CementProd Chart", "id": "CementProd Chart",
                                         "presentation": "markdown"}],
                                    id="cement_table",
                                    page_size=PAGE_SIZE,
                                    filter_action='native',
                                    sort_action='native',
                                    sort_mode='multi',
                                    sort_by=[{'column_id': "CementProd YoY 3mma %", 'direction': 'desc'}],
                                    markdown_options={"html": True})

# get impulse
if DEBUG:
    impulse_ticker_df = pd.read_excel("impulse.xlsx")
else:
    startDate = datetime.today() - timedelta(days=30 * 12 * 30)  # get 30 year data
    impusle6 = getter_credit_impulse.get_credit_impulse_table6m(startDate)
    impusle6.to_excel("impulse6m.xlsx")
    impusle12 = getter_credit_impulse.get_credit_impulse_table12m(startDate)
    impusle12.to_excel("impulse12m.xlsx")
    impuslem2 = getter_credit_impulse.get_credit_impulse_tableM2(startDate)
    impuslem2.to_excel("impulsem2.xlsx")

    impulse_ticker_df = pd.concat([impusle6, impusle12, impuslem2], axis=1)
    impulse_ticker_df = impulse_ticker_df.loc[:, ~impulse_ticker_df.columns.duplicated()]
    impulse_ticker_df = impulse_ticker_df[
        ['6month Credit Impulse %', '12month Credit Impulse %', 'CreditImpulse UpdateTime',
         '6month M2 Credit Impulse %', '12month M2 Credit Impulse %', 'M2 CreditImpulse UpdateTime']]
    impulse_ticker_df.to_excel("impulse.xlsx")

impulse_table = dash_table.DataTable(impulse_ticker_df.to_dict("records"),
                                     [{"name": i, "id": i} for i in impulse_ticker_df.columns],
                                     id="impulse_table",
                                     page_size=PAGE_SIZE,
                                     filter_action='native',
                                     sort_action='native',
                                     sort_mode='multi',
                                     sort_by=[{'column_id': "6month Credit Impulse %", 'direction': 'desc'}],
                                     markdown_options={"html": True})

if not DEBUG:
    get_econ()
    get_econ_export()


col_list = ['Country', 'AutoSales YoY 3mma %', 'AutoSales UpdateTime', 'CementProd YoY 3mma %', 'CementProd UpdateTime',
            '6month Credit Impulse %', '12month Credit Impulse %', 'CreditImpulse UpdateTime',
            '6month M2 Credit Impulse %', '12month M2 Credit Impulse %', 'M2 CreditImpulse UpdateTime']

# add impulse cement and auto
display_df = \
    pd.merge(impulse_ticker_df, cement_df, on="Country", how="outer").merge(auto_ticker_df, on="Country", how="outer")[
        col_list]

# add other economic indicators (this year + next year if available)
nextyear = f"{(int(DISPLAY_YEAR) + 1) % 100:02d}"

for j in ['Budget', 'Current Act', 'GDP', 'CPI', 'Unemployment']:
    _econ_df = pd.read_excel(f"ecom_{j}.xlsx", index_col=0)

    # normalize year columns to strings (Excel sometimes reads them as ints)
    _econ_df.columns = [str(c) for c in _econ_df.columns]

    years_to_take = [y for y in [DISPLAY_YEAR, nextyear] if y in _econ_df.columns]
    if not years_to_take:
        # no matching year columns in this file; skip cleanly
        continue

    rename_map = {y: f"{j} {y}" for y in years_to_take}
    _econ_df2 = _econ_df[['Country'] + years_to_take].rename(columns=rename_map)

    display_df = display_df.merge(_econ_df2, how='outer', on='Country')


# add export
export_df = pd.read_excel("ecom_export.xlsx", index_col=0)[['Export YoY%', 'Export UpdateTime']]
display_df = display_df.merge(export_df, how="left", left_on='Country', right_index=True)

# add CPI next year
nextyear = str(int(DISPLAY_YEAR) + 1)
cpi_nextyear = pd.read_excel(f"ecom_CPI.xlsx", index_col=0)[['Country', nextyear, 'Type']].rename(
    columns={nextyear: f"CPI {nextyear}"})
display_df = display_df.merge(cpi_nextyear, how='left', left_on='Country', right_on='Country')

# add fwd rate
fwdrate = pd.read_excel(f"ecom_fwdrate.xlsx", index_col=0)[['Country', 'px_last']].rename(
    columns={'px_last': "1y1y swap rate"})
fwdrate.loc[:, "1y1y swap rate"] = round(fwdrate.loc[:, "1y1y swap rate"], 2)
display_df = display_df.merge(fwdrate, how='left', left_on='Country', right_on='Country')
cpi_fwd = display_df.get(f'CPI {nextyear}')
if cpi_fwd is not None:
    display_df['real rate'] = (display_df['1y1y swap rate'] - cpi_fwd).round(2)
else:
    display_df['real rate'] = pd.NA


# move Type to 1st columns
col_s = display_df.pop('Type')
display_df.insert(1, 'Type', col_s)
display_df['id'] = display_df["Country"]
display_df.set_index('id', inplace=True, drop=False)

desired_cols = [
    'Country', 'Type',
    f'GDP {DISPLAY_YEAR}', f'GDP {nextyear}',
    f'CPI {DISPLAY_YEAR}', f'CPI {nextyear}',
    f'Unemployment {DISPLAY_YEAR}', f'Unemployment {nextyear}',
    f'Budget {DISPLAY_YEAR}', f'Budget {nextyear}',
    f'Current Act {DISPLAY_YEAR}', f'Current Act {nextyear}',
    'Export YoY%', 'Export UpdateTime',
    '1y1y swap rate', 'real rate',
    '6month Credit Impulse %', '12month Credit Impulse %', 'CreditImpulse UpdateTime',
    '6month M2 Credit Impulse %', '12month M2 Credit Impulse %', 'M2 CreditImpulse UpdateTime',
    'AutoSales YoY 3mma %', 'AutoSales UpdateTime',
    'CementProd YoY 3mma %', 'CementProd UpdateTime',
    'id'
]

# reindex avoids KeyError if some forward-year columns don't exist for some indicators
display_df = display_df.reindex(columns=desired_cols)


(styles, legend) = discrete_background_color_bins(display_df, n_bins=8,
                                                  columns=['AutoSales YoY 3mma %',
                                                           '6month Credit Impulse %', f'Budget {DISPLAY_YEAR}',
                                                           f'GDP {DISPLAY_YEAR}', 'real rate'],
                                                  reverse_subset=[f'Budget {DISPLAY_YEAR}',
                                                                  f'Current Act {DISPLAY_YEAR}',
                                                                  f'Unemployment {DISPLAY_YEAR}',
                                                                  'real rate'])
display_table = dash_table.DataTable(display_df.to_dict("records"),
                                     columns=[{
                                                  "name": i, "id": i, "type": "numeric", "format": {"specifier": ".2f"}
                                              } if display_df[i].dtype in ["float64", "int64"] else {"name": i, "id": i}
                                              for i
                                              in
                                              display_df.columns],
                                     id="display_table",
                                     page_size=25,
                                     filter_action='native',
                                     sort_action='native',
                                     sort_mode='multi',
                                     style_data_conditional=styles
                                     )

# Define the initial line charts for credit impulse and swap rate
line_chart_ecom = dcc.Graph(id='line-chart-ecom')
line_chart_exante_ci = dcc.Graph(id='line-chart-exante')
line_chart_m2_ci = dcc.Graph(id='line-chart-m2')

button = dcc.Dropdown(
    id='button',
    options=[{'label': '3y', 'value': '3y'},
             {'label': '5y', 'value': '5y'},
             {'label': '10y', 'value': '10y'},
             {'label': 'All', 'value': 'All'}],
    value='5y'
)

app.layout = html.Div(children=[
    html.H1(children='Economic Monitor'),
    html.H2(children='Table'),
    html.Div(children=[display_table]),
    html.Div(children=[html.Span('time span:', style={'marginRight': '10px'}), button],
             style={'width': '200px', 'margin': 'auto'}),
    html.Div(children=[line_chart_ecom, line_chart_exante_ci, line_chart_m2_ci],
             style={'display': 'flex', 'flexDirection': 'row', 'width': '100%'}),
    html.H2(children='Credit Impulse'),
    html.A("Exante Credit Impulse", href="https://www.exantedata.com/global-credit-indicators/?l=total",
           style={"fontSize": "20px"}),
    html.Div(children=[impulse_table]),
    html.H2(children='Economic Indiactors'),
    html.H2(children='Auto Sales'),
    html.Div(children=[auto_table]),
    html.H2(children='Cement Production'),
    html.Div(children=[cement_table])
])

# preload credit data 
def _safe_read_csv(p):
    try:
        return pd.read_csv(Path(p), index_col=0, parse_dates=True).sort_index()
    except Exception:
        return pd.DataFrame()

meta_data_exante6 = _safe_read_csv(Path(BufferCredit, "exante_creditimpulse_6m.csv"))
meta_data_exante12 = _safe_read_csv(Path(BufferCredit, "exante_creditimpulse_12m.csv"))
meta_data_m26 = _safe_read_csv(Path(BufferCredit, "m2_creditimpulse_6m.csv"))
meta_data_m212 = _safe_read_csv(Path(BufferCredit, "m2_creditimpulse_12m.csv"))


col_mapping_dict = {
    f"Budget {DISPLAY_YEAR}": "ecom_Budget",
    f"Current Act {DISPLAY_YEAR}": "ecom_Current Act",
    f"GDP {DISPLAY_YEAR}": "ecom_GDP",
    f"CPI {DISPLAY_YEAR}": "ecom_CPI",
    f"CPI {str(int(DISPLAY_YEAR)+1)}": "ecom_CPI",
    f"Unemployment {DISPLAY_YEAR}": "ecom_Unemployment",
    "Export YoY%": "ecom_export",
    #     "1y1y swap rate": "ecom_fwdrate",  #todo add real rate
    #     "real rate": "ecom_realrate"
}


# update the charts
@app.callback(
    Output('line-chart-ecom', 'figure'),
    Input('display_table', 'active_cell'))
def update_ecom_charts(active_cell):
    fig1 = go.Figure()
    if active_cell:
        print("active cell : ", active_cell['column'], active_cell['column_id'])
        col = active_cell['column_id']
        if col in col_mapping_dict.keys():
            active_row = display_df[display_df['id'] == active_cell['row_id']].squeeze()
            country = active_row["Country"]
            file_name = col_mapping_dict[col]
            print("loading ", file_name)

            if file_name != "ecom_export":
                _selected_data = pd.read_excel(f"{file_name}.xlsx", index_col=2)
                selected_data = _selected_data.loc[country, :][2:]
            else:
                _export_file_name = Path("buffer_economy", f"export_{country}")
                print("loading ", _export_file_name)
                _selected_data = pd.read_pickle(f"{_export_file_name}")
                selected_data = _selected_data.dropna().iloc[:, 0]

            _date = selected_data.index
            fig1.add_trace(go.Scatter(x=_date, y=selected_data, mode='lines',
                                      name=country + " | " + f"{file_name}".split("_")[1]))
            fig1.update_layout(title='Economic Indicator', showlegend=True)
        else:
            pass
    else:
        pass
    return fig1


@app.callback(
    [Output('line-chart-exante', 'figure'), Output('line-chart-m2', 'figure')],
    [Input('display_table', 'active_cell'), Input('button', 'value')])
def update_line_charts(active_cell, value):
    fig1 = go.Figure()
    fig2 = go.Figure()

    if active_cell:
        selected_row = display_df[display_df['id'] == active_cell['row_id']].squeeze()

        if selected_row['Country'] not in meta_data_exante6.columns:
            return fig1, fig2

        selected_data = pd.concat([meta_data_exante6[selected_row['Country']],
                                   meta_data_exante12[selected_row['Country']],
                                   meta_data_m26[selected_row['Country']],
                                   meta_data_m212[selected_row['Country']]], axis=1)
        selected_data.columns = ["6m/6m exante", "YoY exante", "6m/6m M2", "YoY M2"]
        if value == '3y':
            filtered_data = selected_data[
                selected_data.index >= selected_data.index.max() - pd.Timedelta(weeks=3 * 52)]
        elif value == '5y':
            filtered_data = selected_data[
                selected_data.index >= selected_data.index.max() - pd.Timedelta(weeks=5 * 52)]
        elif value == '10y':
            filtered_data = selected_data[
                selected_data.index >= selected_data.index.max() - pd.Timedelta(weeks=10 * 52)]
        else:
            filtered_data = selected_data

        selected_date = filtered_data.index
        fig1.add_trace(go.Scatter(x=selected_date, y=filtered_data["6m/6m exante"], mode='lines',
                                  name=selected_row['Country'] + " | " + "6m/6m exante"))
        fig1.add_trace(go.Scatter(x=selected_date, y=filtered_data["YoY exante"], mode='lines',
                                  name=selected_row['Country'] + " | " + "YoY exante"))
        fig1.update_layout(title='Credit Impulse Exante', showlegend=True)

        fig2.add_trace(go.Scatter(x=selected_date, y=filtered_data["6m/6m M2"], mode='lines',
                                  name=selected_row['Country'] + " | " + "6m/6m M2"))
        fig2.add_trace(go.Scatter(x=selected_date, y=filtered_data["YoY M2"], mode='lines',
                                  name=selected_row['Country'] + " | " + "YoY M2"))
        fig2.update_layout(title='Credit Impulse M2', showlegend=True)
    else:
        pass
    return fig1, fig2

import getpass
username = getpass.getuser()
host = '10.194.31.111' if username == 'guofeng.koh' else '10.194.31.115'
if __name__ == '__main__':
    app.run(debug=True, host=host, port=8094)
