from dash import html, Dash, dash_table
import pandas as pd

def discrete_background_color_bins(df, n_bins=5, columns='all', reverse_subset=[]):
    import colorlover
    df = df.copy()
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
    if columns == 'all':
        if 'id' in df:
            df_numeric_columns = df.select_dtypes('number').drop(['id'], axis=1)
        else:
            df_numeric_columns = df.select_dtypes('number')
    else:
        df_numeric_columns = df[columns]

    styles = []
    legend = []

    for column in df_numeric_columns:
        df_max = df_numeric_columns[column].max()
        df_min = df_numeric_columns[column].min()
        ranges = [
            ((df_max - df_min) * i) + df_min
            for i in bounds
        ]

        for i in range(1, len(bounds)):
            min_bound = ranges[i - 1]
            max_bound = ranges[i]
            if column in reverse_subset:
                # reverse color code order if that is in reverse order col_dict
                if i >= len(bounds) / 2:
                    # Green to yellow range
                    backgroundColor = colorlover.scales[str(n_bins)]['seq']['Reds'][i - 1]
                else:
                    # Yellow to red range
                    backgroundColor = colorlover.scales[str(n_bins)]['seq']['Greens'][n_bins - i]

            else:
                if i >= len(bounds) / 2:
                    # Green to yellow range
                    backgroundColor = colorlover.scales[str(n_bins)]['seq']['Greens'][i - 1]
                else:
                    # Yellow to red range
                    backgroundColor = colorlover.scales[str(n_bins)]['seq']['Reds'][n_bins - i]
            color = 'white' if i > len(bounds) / 2. else 'inherit'

            styles.append({
                'if': {
                    'filter_query': (
                            '{{{column}}} >= {min_bound}' +
                            (' && {{{column}}} < {max_bound}' if (i < len(bounds) - 1) else '')
                    ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                    'column_id': column
                },
                'backgroundColor': backgroundColor,
                'color': color
            })
            legend.append(
                html.Div(style={'display': 'inline-block', 'width': '60px'}, children=[
                    html.Div(
                        style={
                            'backgroundColor': backgroundColor,
                            'borderLeft': '1px rgb(50, 50, 50) solid',
                            'height': '10px'
                        }
                    ),
                    html.Small(round(min_bound, 2), style={'paddingLeft': '2px'})
                ])
            )

    return (styles, html.Div(legend, style={'padding': '5px 0 5px 0'}))




if __name__ == '__main__':
    wide_data = [
        {'Firm': 'Acme', '2017': 13, '2018': 5, '2019': 10, '2020': 4},
        {'Firm': 'Olive', '2017': 3, '2018': 3, '2019': 13, '2020': 3},
        {'Firm': 'Barnwood', '2017': 6, '2018': 7, '2019': 3, '2020': 6},
        {'Firm': 'Henrietta', '2017': -3, '2018': -10, '2019': -5, '2020': -6},
    ]
    df = pd.DataFrame(wide_data)
    df = pd.read_excel('tmp.xlsx')
    app = Dash(__name__)

    col_dict = {
        'Budget 23': -1,
        'Current Act 23': -1,
        'Unemployment 23': -1,
        'real rate': -1
    }
    (styles, legend) = discrete_background_color_bins(df)

    app.layout = html.Div([
        html.Div(legend, style={'float': 'right'}),
        dash_table.DataTable(
            data=df.to_dict('records'),
            sort_action='native',
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_data_conditional=styles
        ),
    ])

    app.run_server(debug=True)
