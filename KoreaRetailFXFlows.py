import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use('Agg')  # non-interactive backend for saving only

import time
import glob
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import win32com.client as win32
import shutil

# === Setup Chrome with silent download ===
download_dir = os.path.join("Buffer Korea Retail Flows")
# Create the directory if it doesn't exist; otherwise, empty it
if os.path.exists(download_dir):
    shutil.rmtree(download_dir)  # remove all contents
else:
    os.makedirs(download_dir)

download_dir = os.path.abspath(download_dir)
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument(
    '--user-agent="Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166"')  # reduce anti-crawling detection

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 20)

current_year = pd.Timestamp.today().year

# === Step 1: Navigate and download data ===
driver.get("https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/ann/BIP_CNST02001V.xml&menuNo=861")
time.sleep(3)

# Input dates
start_date = wait.until(EC.presence_of_element_located((By.ID, "inputCalendar1_input")))
start_date.clear()
start_date.send_keys('2020-01-01')
wait.until(EC.presence_of_element_located((By.ID, "inputCalendar2_input"))).send_keys(
    datetime.today().strftime('%Y-%m-%d'))

# Set "일" (Day) period
Select(wait.until(EC.presence_of_element_located((By.ID, 'ann_sd1_selectbox1_input_0')))).select_by_visible_text('일')

# Select "결제금액" (Payment Amount)
wait.until(EC.element_to_be_clickable((By.ID, 'ann_r2_radio1_input_1'))).click()

# Click 조회 (Search)
wait.until(EC.element_to_be_clickable((By.ID, "group186"))).click()

# Wait for results and trigger Excel download
wait.until(EC.presence_of_element_located((By.ID, 'grid1_body_table')))
time.sleep(2)
driver.execute_script("doExcelDown();")
time.sleep(3)

# Wait for file download
timeout, latest_file = 30, None
while timeout > 0:
    files = glob.glob(os.path.join(download_dir, "*.xls"))
    if files:
        latest_file = max(files, key=os.path.getctime)
        break
    time.sleep(1)
    timeout -= 1
driver.quit()
if not latest_file:
    raise Exception("Download failed")

# === Step 2: Load and process the Excel file ===
df_list = pd.read_html(latest_file, encoding='euc-kr')
df = df_list[0]
kor_to_eng = {
    '구분': 'Date', '구분.1': 'Asset Type',
    '유로시장': 'Euro Market', '미국': 'USA', '일본': 'Japan', '홍콩': 'Hong Kong',
    '중국': 'China', '기타국가': 'Other', '총합계': 'Total',
    '매도': 'Sell', '매수': 'Buy'
}
df.columns = pd.MultiIndex.from_tuples([
    (kor_to_eng.get(a.strip(), a.strip()), kor_to_eng.get(b.strip(), b.strip())) for a, b in df.columns
])
df[('Date', 'Asset Type')] = df[('Date', 'Asset Type')].replace({'주식': 'Equity', '채권': 'Bond'})

df_copy = df.copy()
df_copy.columns = ['{} - {}'.format(a, b) if b != a else a for a, b in df.columns]
df_copy.rename(columns={"Date - Asset Type": "Asset Type"}, inplace=True)
df_copy['Date'] = pd.to_datetime(df_copy['Date'].astype(str), format='%Y%m%d')
df_copy['Net Purchase'] = df_copy['Total - Buy'] - df_copy['Total - Sell']

# === Step 3: Prepare daily and YTD pivot data ===
current_year = pd.Timestamp.today().year
start_of_year = pd.Timestamp(year=current_year, month=1, day=1)

pivoted = (
    df_copy[df_copy['Date'] >= start_of_year]
    .pivot(index='Date', columns='Asset Type', values='Net Purchase')
    .fillna(0)
)
pivoted.rename(columns={"Equity": 'Equity Net', "Bond": 'Debt Net'}, inplace=True)
pivoted['Total Net'] = pivoted['Equity Net'] + pivoted['Debt Net']
pivoted['Equity MA'] = pivoted['Equity Net'].rolling(7).mean()
pivoted['Debt MA'] = pivoted['Debt Net'].rolling(7).mean()
pivoted['Total MA'] = pivoted['Total Net'].rolling(7).mean()

df_copy['Year'] = df_copy['Date'].dt.year
df_copy['DayOfYear'] = df_copy['Date'].dt.dayofyear
pivot = df_copy.pivot_table(index='DayOfYear', columns=['Year', 'Asset Type'], values='Net Purchase',
                            aggfunc='sum').cumsum() / 1000
equity_ytd = pivot.xs('Equity', level=1, axis=1)
debt_ytd = pivot.xs('Bond', level=1, axis=1)
total_ytd = equity_ytd.add(debt_ytd, fill_value=0)

# === Step 4: Plot and save all six charts ===
plot_paths = []


def save_plot(fig, name):
    path = os.path.join(download_dir, f"{name}.png")
    fig.savefig(path)
    plot_paths.append(path)
    plt.close(fig)


def plot_daily(df, net_col, ma_col, title, color):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(df.index, df[net_col], label='Net Purchases', color=color, alpha=0.6)
    ax.plot(df.index, df[ma_col], label='7-day MA', color='black')
    ax.axhline(0, color='gray', linewidth=1)
    ax.set_title(title)
    ax.set_ylabel("USD mn")
    ax.legend();
    ax.grid(True, linestyle='--', alpha=0.3)
    fig.tight_layout()
    save_plot(fig, title)


def plot_total_stacked(df):
    fig, ax = plt.subplots(figsize=(10, 5))

    eq = df['Equity Net']
    debt = df['Debt Net']
    total_ma = df['Total MA']
    idx = df.index

    same_sign = (eq * debt > 0)

    # Stacked where same sign
    ax.bar(idx[same_sign], eq[same_sign], color='orange', alpha=0.6)
    ax.bar(idx[same_sign], debt[same_sign], bottom=eq[same_sign], color='blue', alpha=0.6)

    # Unstacked where opposite signs
    ax.bar(idx[~same_sign], eq[~same_sign], color='orange', alpha=0.6, label='Equity Net')
    ax.bar(idx[~same_sign], debt[~same_sign], color='blue', alpha=0.6, label='Debt Net')

    # MA Line
    ax.plot(idx, total_ma, label='7-day MA', color='black', linewidth=1.5)

    ax.axhline(0, color='gray', linewidth=0.8)
    ax.set_title("Daily Foreign Equity + Debt Net Purchases ($mn)")
    ax.set_ylabel("USD mn")
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.legend(loc='upper left')
    fig.tight_layout()
    save_plot(fig, "Daily Foreign Equity + Debt Net Purchases")


def plot_ytd(df, title):
    fig, ax = plt.subplots(figsize=(10, 5))
    for year in df.columns:
        is_current = (year == current_year)
        ax.plot(
            df.index, df[year], label=str(year),
            linewidth=2.5 if is_current else 1.2,
            color='red' if is_current else None,
            linestyle='-' if is_current else '--',
            alpha=1 if is_current else 0.6
        )
    ax.axhline(0, color='black')
    ax.set_title(title)
    ax.set_ylabel("USD bn");
    ax.set_xlabel("Day of Year")
    ax.legend();
    ax.grid(True, linestyle='--', alpha=0.3)
    fig.tight_layout()
    save_plot(fig, title)


# Generate plots
plot_daily(pivoted, 'Equity Net', 'Equity MA', 'Daily Foreign Equity Net Purchases ($mn)', 'orange')
plot_daily(pivoted, 'Debt Net', 'Debt MA', 'Daily Foreign Debt Net Purchases ($mn)', 'blue')
plot_total_stacked(pivoted)
plot_ytd(equity_ytd, r'Foreign Equity YTD Net Purchase ($bn)')
plot_ytd(debt_ytd, r'Foreign Debt YTD Net Purchase ($bn)')
plot_ytd(total_ytd, 'Foreign Equity + Debt YTD Net Purchase ($bn)')

# === Step 5: Send Outlook email ===
last_date = pivoted.index.max().strftime('%Y-%b-%d')
outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.Subject = f"Korea retail flows - update time {last_date}"
recipients = ["jian.zhou@bluecrestcapital.com", "tian.qin@bluecrestcapital.com", "jinseop.lee@bluecrestcapital.com","jerry.koo@bluecrestcapital.com","guofeng.koh@bluecrestcapital.com"]
# recipients = ["jian.zhou@bluecrestcapital.com"]
# recipients = ["guofeng.koh@bluecrestcapital.com"]
mail.To = "; ".join(recipients)

# Start HTML with 2-row, 3-column table
mail.HTMLBody += "<h2>Korea retail flow charts</h2><br>"

mail.HTMLBody += """
<table width="100%" cellpadding="10" cellspacing="0" border="0" style="text-align:center;">
"""

for i, path in enumerate(plot_paths):
    cid = f"chart{i}"
    attachment = mail.Attachments.Add(Source=path)
    attachment.PropertyAccessor.SetProperty(
        "http://schemas.microsoft.com/mapi/proptag/0x3712001F", cid
    )

    if i % 3 == 0:
        mail.HTMLBody += "<tr>"

    mail.HTMLBody += f"""
        <td align="center" valign="top" style="padding:10px;">
            <img src="cid:{cid}" width="700" style="display:block; margin:auto;">
        </td>
    """

    if i % 3 == 2:
        mail.HTMLBody += "</tr>"

# Close last row if needed
if len(plot_paths) % 3 != 0:
    mail.HTMLBody += "</tr>"

mail.HTMLBody += "</table>"
mail.Send()  # Use mail.Display() for preview before sending
