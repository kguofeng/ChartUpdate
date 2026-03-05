import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

from sklearn.linear_model import LinearRegression
G_CHART_DIR = r"O:\Tian\Portal\Charts\ChartDataBase" 


def clean_data(df):
    front, last = df.iloc[:-1, :], df.iloc[-1:, :]
    front = front.ffill(limit=1)  # not fill NA for the last row -> it's pending release
    front = front.dropna()
    return pd.concat([front, last], axis=0)


operators = [['ge ', '>='],
             ['le ', '<='],
             ['lt ', '<'],
             ['gt ', '>'],
             ['ne ', '!='],
             ['eq ', '='],
             ['contains '],
             ['datestartswith ']]


def split_filter_part(filter_part):
    for operator_type in operators:
        for operator in operator_type:
            if operator in filter_part:
                name_part, value_part = filter_part.split(operator, 1)
                name = name_part[name_part.find('{') + 1: name_part.rfind('}')]

                value_part = value_part.strip()
                v0 = value_part[0]
                if (v0 == value_part[-1] and v0 in ("'", '"', '`')):
                    value = value_part[1: -1].replace('\\' + v0, v0)
                else:
                    try:
                        value = float(value_part)
                    except ValueError:
                        value = value_part

                # word operators need spaces after them in the filter string,
                # but we don't want these later
                return name, operator_type[0].strip(), value

def make_clickable(url, text):
    return f'<a target="_blank" href="{url}">{text}</a>'

def calculate_and_plot(data, title, filename, method='yield', multiply_by_100=False, Path = Path, G_CHART_DIR = G_CHART_DIR):
    if method == 'yield':
        # Exclude the last 20 days when calculating betas
        data_friday = data[:-20].resample('W-FRI').last()
        friday_changes = data_friday.diff(periods=1)
        # Adjust the index if necessary
        if data[:-20].index[-1] < friday_changes.index[-1]:
            friday_changes = friday_changes.iloc[:-1]
    elif method == 'fx':
        # Include all data when calculating betas
        data_friday = data.resample('W-FRI').last()
        friday_changes = data_friday.pct_change(periods=1)
        friday_changes = friday_changes.iloc[:-1]  # Exclude the first NaN row
    else:
        raise ValueError("Invalid method. Choose 'yield' or 'fx'.")

    w_w_changes = friday_changes.copy()
    beta_dict_lr = {}
    for column in w_w_changes.columns:
        if column != 'USD':
            df_pair = w_w_changes[['USD', column]].dropna()
            X = df_pair[['USD']].values
            y = df_pair[column].values
            model = LinearRegression(fit_intercept=False)
            model.fit(X, y)
            beta = model.coef_[0]
            beta_dict_lr[column] = beta
        else:
            beta_dict_lr[column] = 1

    if method == 'yield':
        # Use absolute differences for yields
        yield_changes = data.diff(periods=1)
        last_20_days = yield_changes.tail(20)
    elif method == 'fx':
        # Use percentage changes for FX rates
        yield_changes = data.pct_change(periods=1)
        last_20_days = yield_changes.tail(20)

    actual_moves_dict = {}
    expected_moves_dict = {}
    for currency in beta_dict_lr.keys():
        if currency != '':
            usd_changes = last_20_days['USD']
            currency_changes = last_20_days[currency]
            pair_data = pd.DataFrame({
                'USD': usd_changes,
                currency: currency_changes
            }).dropna()

            # Sum of absolute changes
            actual_move = pair_data[currency].sum()
            usd_move = pair_data['USD'].sum()

            beta = beta_dict_lr[currency]
            expected_move = beta * usd_move
            actual_moves_dict[currency] = actual_move
            expected_moves_dict[currency] = expected_move
            #print(currency, usd_changes, currency_changes, pair_data, actual_move, usd_move, beta, expected_move)

    actual_moves = pd.Series(actual_moves_dict)
    expected_moves = pd.Series(expected_moves_dict)
    if multiply_by_100:
        actual_moves *= 100
        expected_moves *= 100

    fig, ax = plt.subplots(figsize=(10, 6))
    for currency in expected_moves.index:
        ax.scatter(expected_moves[currency], actual_moves[currency])
        ax.annotate(f"{currency}", (expected_moves[currency], actual_moves[currency]), fontsize=7,
                    textcoords="offset points", xytext=(0, 5), ha='center')


    beta_table = pd.DataFrame({
        'Currency': list(beta_dict_lr.keys()),
        'Beta': [round(value, 2) for value in beta_dict_lr.values()]
    })
    ax2 = fig.add_axes([0.9, 0.1, 0.1, 0.8])  # [left, bottom, width, height]
    ax2.axis('off')  # Hide the axis
    table = ax2.table(
        cellText=beta_table.values,
        colLabels=beta_table.columns,
        cellLoc='center',
        loc='center'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)

    line_45, = ax.plot([min(expected_moves), max(expected_moves)], [min(expected_moves), max(expected_moves)],
                       color='black', linestyle='-', label="45° Line")
    ax.axhline(0, color='grey', linewidth=0.8)
    ax.axvline(0, color='grey', linewidth=0.8)

    if method == 'yield':
        ax.set_xlabel("Implied by beta to US yields")
        ax.set_ylabel("Actual Move")
    elif method == 'fx':
        ax.set_xlabel("Implied by beta to DXY (Past 2 Years)")
        ax.set_ylabel("Actual Move by USDCCY")

    ax.set_title(title)
    ax.legend(handles=[line_45])
    plt.savefig(Path(G_CHART_DIR, filename))

def base_series_to_date(df, column_name, base_date=""):
    if base_date not in df.index:
        base_date = df.index[df.index.get_indexer([base_date], method='nearest')[0]]
    base_value = df.loc[base_date, column_name]
    df[f'{column_name} (Indexed)'] = df[column_name] / base_value  * 100
    return df


