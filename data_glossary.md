# Scanner Data Glossary

> Comprehensive reference for all tickers, data-pulling patterns, and external data sources used across the Scanner codebase.

---

## Table of Contents

- [Part 1: Bloomberg Ticker Glossary](#part-1-bloomberg-ticker-glossary)
- [Part 2: Bloomberg Data Pulling Guide](#part-2-bloomberg-data-pulling-guide)
- [Part 3: TSDB (via PyQCL) Documentation](#part-3-tsdb-via-pyqcl-documentation)
- [Part 4: PyQCL Pricing/Risk Functions](#part-4-pyqcl-pricingrisk-functions)
- [Part 5: Other Data Sources](#part-5-other-data-sources)

---

# Part 1: Bloomberg Ticker Glossary

Organized by **Region > Country > Asset Class (Market / Econ)**.

## G10

### US -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| SPX Index | S&P 500 | Equity Index | Daily, index points |
| NDQ Index | Nasdaq 100 | Equity Index | Daily, index points |
| RTY Index | Russell 2000 | Equity Index | Daily, index points |
| ESA Index | E-mini S&P 500 Futures | Equity Futures | Daily, index points |
| NQA Index | Nasdaq 100 Futures | Equity Futures | Daily, index points |
| RTYA Index | Russell 2000 Futures | Equity Futures | Daily, index points |
| ES1 Index | E-mini S&P 500 generic 1st | Equity Futures | Daily, index points |
| HYG US Equity | iShares HY Corp Bond ETF | Credit ETF | Daily, index points |
| CMBS US Equity | iShares CMBS ETF | Credit ETF | Daily, index points |
| MTUM US Equity | iShares MSCI USA Momentum ETF | Factor ETF | Daily, index points |
| SPY US Equity | SPDR S&P 500 ETF | Equity ETF | Daily, index points |
| VTI US Equity | Vanguard Total Stock Market ETF | Equity ETF | Daily, index points |
| QQQ US Equity | Invesco QQQ Trust | Equity ETF | Daily, index points |
| IVV US Equity | iShares Core S&P 500 ETF | Equity ETF | Daily, index points |
| IWM US Equity | iShares Russell 2000 ETF | Equity ETF | Daily, index points |
| DXY Index | US Dollar Index | FX Index | Daily, index level |
| DXY Curncy | US Dollar Index (Curncy) | FX | Daily, index level |
| BBDXY Index | Bloomberg Dollar Index | FX Index | Daily, index level |
| USOSFRA Curncy | USD SOFR 1M Swap | Rates | Daily, rate % |
| USOSFRB Curncy | USD SOFR 2M Swap | Rates | Daily, rate % |
| USOSFRC Curncy | USD SOFR 3M Swap | Rates | Daily, rate % |
| USOSFRF Curncy | USD SOFR 6M Swap | Rates | Daily, rate % |
| USOSFR1 Curncy | USD SOFR 1Y Swap | Rates | Daily, rate % |
| USOSFR2 Curncy | USD SOFR 2Y Swap | Rates | Daily, rate % |
| USOSFR3 Curncy | USD SOFR 3Y Swap | Rates | Daily, rate % |
| USOSFR5 Curncy | USD SOFR 5Y Swap | Rates | Daily, rate % |
| USOSFR7 Curncy | USD SOFR 7Y Swap | Rates | Daily, rate % |
| USOSFR10 Curncy | USD SOFR 10Y Swap | Rates | Daily, rate % |
| USOSFR15 Curncy | USD SOFR 15Y Swap | Rates | Daily, rate % |
| USOSFR30 Curncy | USD SOFR 30Y Swap | Rates | Daily, rate % |
| USOS0210 Curncy | USD SOFR 2s10s Spread | Rates | Daily, spread bp |
| USGG2YR Index | UST 2Y Yield | Govt Yield | Daily, yield % |
| USGG5YR Index | UST 5Y Yield | Govt Yield | Daily, yield % |
| USGG5Y5Y Index | UST 5Y5Y TIPS Breakeven | Inflation | Daily, yield % |
| USGG7YR Index | UST 7Y Yield | Govt Yield | Daily, yield % |
| USGG10YR Index | UST 10Y Yield | Govt Yield | Daily, yield % |
| USGG30YR Index | UST 30Y Yield | Govt Yield | Daily, yield % |
| USGGBE10 Index | UST 10Y Breakeven | Inflation | Daily, yield % |
| H15T10Y Index | H.15 10Y Treasury Yield | Govt Yield | Daily, yield % |
| H15T3Y Index | H.15 3Y Treasury Yield | Govt Yield | Daily, yield % |
| USSWIT2 Curncy | US 2Y Inflation Swap | Inflation Swap | Daily, rate % |
| USSWIT5 Curncy | US 5Y Inflation Swap | Inflation Swap | Daily, rate % |
| USSWIT10 Curncy | US 10Y Inflation Swap | Inflation Swap | Daily, rate % |
| USSWIT10 BGN Curncy | US 10Y Inflation Swap (BGN) | Inflation Swap | Daily, rate % |
| S0042FS 1Y1Y BLC Curncy | OIS 1Y1Y Forward | Rates Fwd | Daily, rate % |
| S0042FS 3M3M BLC Curncy | OIS 3M3M Forward | Rates Fwd | Daily, rate % |
| S0042FS 15M3M BLC Curncy | OIS 15M3M Forward | Rates Fwd | Daily, rate % |
| S0490FS 1Y1Y BLC Curncy | SOFR 1Y1Y Forward | Rates Fwd | Daily, rate % |
| S0490FS 2Y1Y BLC Curncy | SOFR 2Y1Y Forward | Rates Fwd | Daily, rate % |
| ACMTP02 Index | ACM Term Premium 2Y | Rates Model | Daily, % |
| ACMTP10 Index | ACM Term Premium 10Y | Rates Model | Daily, % |
| VIX Index | CBOE Volatility Index | Vol Index | Daily, vol points |
| CMBX BBB- CDSI S16 PRC Corp | CMBX BBB- Series 16 | Credit | Daily, spread bp |
| CMBX BBB- CDSI S15 PRC Corp | CMBX BBB- Series 15 | Credit | Daily, spread bp |
| CSI BARC Index | Credit Suisse/Barclays Credit Index | Credit | Daily, spread bp |
| JPEIGLSP Index | JPM EM Sovereign Spread | Credit | Daily, spread bp |
| ALBNSCBC Index | Commercial Banks Securities Holdings | Banking | Monthly, level |
| GDP CUR$ Index | US Total Public Debt Outstanding | Govt | Quarterly, USD mn |
| GDP CURY Index | US Nominal GDP Annual | Econ | Annual, USD mn |

### US -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| CESIUSD Index | Citi US Economic Surprise Index | Econ Surprise | Daily, index |
| JFRIUS Index | JPM US Revision Index | Econ Surprise | Daily, index |
| SOMHTOTL Index | Fed SOMA Holdings | Central Bank | Weekly, USD mn |
| BSPGCPUS Index | Fed Balance Sheet % GDP | Central Bank | Monthly, % GDP |
| EPUCNUSD Index | US Policy Uncertainty Index | Sentiment | Daily, index |
| EPUCGLCP Index | Global Policy Uncertainty Index | Sentiment | Monthly, index |
| GSUSFCI Index | GS US Financial Conditions Index | FCI | Daily, index |
| KCMTLMCI Index | Macro Credit Tightening Index | FCI | Daily, index |
| LEI YOY Index | US Leading Economic Indicators YoY | Econ Leading | Monthly, % YoY |
| CPI YOY Index | US CPI YoY | Inflation | Monthly, % YoY |
| CPI XYOY Index | US Core CPI YoY | Inflation | Monthly, % YoY |
| PCE CYOY Index | US Core PCE YoY | Inflation | Monthly, % YoY |
| PCE DRBC Index | PCE Durable Goods | Consumption | Monthly, level |
| CPTICHNG Index | US CPI Total Change | Inflation | Monthly, % MoM |
| WGTROVER Index | Atlanta Fed Wage Growth Tracker | Labor | Monthly, % YoY |
| WGTROVRA Index | Atlanta Fed Wage Growth Adjusted | Labor | Monthly, % YoY |
| NFP T Index | US Nonfarm Payrolls Total | Labor | Monthly, thousands |
| NFP NYOY Index | US Nonfarm Payrolls YoY | Labor | Monthly, % YoY |
| FEDL01 Index | Fed Funds Rate | Rates | Daily, rate % |
| NAPMPMI Index | ISM Manufacturing PMI | PMI | Monthly, index |
| NAPMNMI Index | ISM Non-Manufacturing PMI | PMI | Monthly, index |
| NAPMNEWO Index | ISM New Orders | PMI Sub | Monthly, index |
| NAPMINV Index | ISM Inventories | PMI Sub | Monthly, index |
| NAPMMNOW Index | ISM Manufacturing New Orders | PMI Sub | Monthly, index |
| NAPMMDDA Index | ISM Manufacturing Delivery Times | PMI Sub | Monthly, index |
| NAPMMEML Index | ISM Manufacturing Employment | PMI Sub | Monthly, index |
| NAPMNNOL Index | ISM Non-Mfg New Orders | PMI Sub | Monthly, index |
| NAPMNMBL Index | ISM Non-Mfg Business Activity | PMI Sub | Monthly, index |
| NAPMNMEL Index | ISM Non-Mfg Employment | PMI Sub | Monthly, index |
| NHSPSTOT Index | US Housing Permits to Start | Housing | Monthly, level |
| HUUCTHUC Index | US Housing Units Under Construction | Housing | Monthly, level |
| USECTOT Index | US Construction Employment | Labor | Monthly, thousands |
| SBOIQUAL Index | NFIB Small Business Labor Quality | Sentiment | Monthly, index |
| ECI YOY Index | Employment Cost Index YoY | Labor | Quarterly, % YoY |
| ECICCVYY Index | Employment Cost YoY Change | Labor | Quarterly, % YoY |
| CONSDURF Index | Consumer Durable Favorable | Sentiment | Monthly, index |
| CONSDURU Index | Consumer Durable Unfavorable | Sentiment | Monthly, index |
| CEOCINDX Index | CEO Confidence Index | Sentiment | Quarterly, index |
| CPFTYOY Index | Corporate Profits YoY | Corporate | Quarterly, % YoY |
| USJLOSER Index | US Job Losers | Labor | Monthly, thousands |
| USJLJOBL Index | US Job Leavers | Labor | Monthly, thousands |
| USJLREEN Index | US Reentrants | Labor | Monthly, thousands |
| USJLNENT Index | US New Entrants | Labor | Monthly, thousands |
| USUETOT Index | US Total Unemployed | Labor | Monthly, thousands |
| USER54SA Index | US Employment Rate SA | Labor | Monthly, % |
| PRUSQNTS Index | Prime Participation Rate | Labor | Monthly, % |
| USHEYOY Index | Average Hourly Earnings YoY | Labor | Monthly, % YoY |
| USUDMAER Index | US U6 Unemployment Rate | Labor | Monthly, % |
| USURTOT Index | US Unemployment Rate | Labor | Monthly, % |
| CBOPNRUE Index | CBO Natural Unemployment Rate | Labor | Quarterly, % |
| VELOM2 Index | M2 Velocity | Monetary | Quarterly, ratio |
| COMPNFRY Index | US Compensation NFR YoY | Wages | Quarterly, % YoY |
| GDNSCHWN Index | US Nominal GDP | GDP | Quarterly, level |
| IP Index | US Industrial Production | Activity | Monthly, index |
| SPCS20Y% Index | Case-Shiller Home Price Index YoY | Housing | Monthly, % YoY |
| CPRHOERY Index | CPI Owners Equiv Rent YoY | Inflation | Monthly, % YoY |
| RHOTPNAT Index | Hotel Price Per Room | Services | Monthly, USD |
| NRASRPI Index | Restaurant Performance | Services | Monthly, index |
| USHBTRAF Index | Buyers Traffic | Services | Monthly, index |
| PPIDBK11 Index | US Personal Deposits (Banking) | Banking | Monthly, level |
| PPIDBK12 Index | US Time Deposits (Banking) | Banking | Monthly, level |
| GDDI111G Index | US Govt Debt/GDP | Fiscal | Annual, % GDP |
| RSRSTOTL Index | Retail Inventories Total | Inventories | Monthly, level |
| RSRSMOTV Index | Motor Vehicle Inventories | Inventories | Monthly, level |
| RSRSFURN Index | Furniture Inventories | Inventories | Monthly, level |
| RSRSBUIL Index | Building Material Inventories | Inventories | Monthly, level |
| RSRSFOOD Index | Food/Beverage Inventories | Inventories | Monthly, level |
| RSRSCLOT Index | Clothing Inventories | Inventories | Monthly, level |
| RSRSGENR Index | General Merchandise Inventories | Inventories | Monthly, level |
| MWINDRBL Index | Wholesale Durable Inventories | Inventories | Monthly, level |
| MWINNDRB Index | Wholesale Nondurable Inventories | Inventories | Monthly, level |
| MWINTOT Index | Wholesale Total Inventories | Inventories | Monthly, level |
| EMPRINVT Index | Empire State Inventories | Survey | Monthly, index |
| OUTFIVF Index | Philadelphia Fed Inventories | Survey | Monthly, index |
| KCLSIFIN Index | Kansas Fed Inventories | Survey | Monthly, index |
| TROSINIX Index | Dallas Fed Retail Inventories | Survey | Monthly, index |
| RCHSILFG Index | Richmond Fed Inventories | Survey | Monthly, index |

### EU -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| SX5E Index | Euro Stoxx 50 | Equity Index | Daily, index points |
| SXXP Index | STOXX Europe 600 | Equity Index | Daily, index points |
| DAX Index | German DAX | Equity Index | Daily, index points |
| SX7E Index | STOXX Europe 600 Banks | Equity Index | Daily, index points |
| EURUSD Curncy | EUR/USD Spot | FX | Daily, FX rate |
| EURGBP Curncy | EUR/GBP Spot | FX | Daily, FX rate |
| EURCHF Curncy | EUR/CHF Spot | FX | Daily, FX rate |
| EURNOK Curncy | EUR/NOK Spot | FX | Daily, FX rate |
| EURSEK Curncy | EUR/SEK Spot | FX | Daily, FX rate |
| EURJPY Curncy | EUR/JPY Spot | FX | Daily, FX rate |
| EURAUD Curncy | EUR/AUD Spot | FX | Daily, FX rate |
| EURCAD Curncy | EUR/CAD Spot | FX | Daily, FX rate |
| EUSA1 Curncy | EUR 1Y Swap | Rates | Daily, rate % |
| EUSA2 Curncy | EUR 2Y Swap | Rates | Daily, rate % |
| EUSA5 Curncy | EUR 5Y Swap | Rates | Daily, rate % |
| EUSA10 Curncy | EUR 10Y Swap | Rates | Daily, rate % |
| .EUSA210 U Index | EUR 2s10s Swap Spread | Rates | Daily, spread bp |
| GTEUR2Y Govt | EUR Govt 2Y Yield | Govt Yield | Daily, yield % |
| GTEUR10Y Govt | EUR Govt 10Y Yield | Govt Yield | Daily, yield % |
| GTDEM10Y Govt | German Bund 10Y | Govt Yield | Daily, yield % |
| GTDEM2Y Govt | German Schatz 2Y | Govt Yield | Daily, yield % |
| GECU2YR Index | EU Govt 2Y Yield | Govt Yield | Daily, yield % |
| GECU5YR Index | EU Govt 5Y Yield | Govt Yield | Daily, yield % |
| GBTPGR10 Index | Italy BTP 10Y Spread | Govt Spread | Daily, spread bp |
| GDBR10 Index | German Bund 10Y (alt) | Govt Yield | Daily, yield % |
| .BTPBUND G Index | BTP-Bund Spread | Govt Spread | Daily, spread bp |
| .ECBGDP Index | ECB Balance Sheet % GDP | Central Bank | Monthly, % GDP |
| S0514FS 3Y1Y BLC Curncy | ESTR 3Y1Y Forward | Rates Fwd | Daily, rate % |
| S0133FS 3Y1Y BLC Curncy | EONIA 3Y1Y Forward | Rates Fwd | Daily, rate % |
| GRSWIT2 Curncy | Germany 2Y Inflation Swap | Inflation Swap | Daily, rate % |
| GRSWIT10 Curncy | Germany 10Y Inflation Swap | Inflation Swap | Daily, rate % |
| EZU US Equity | iShares MSCI Eurozone ETF | Equity ETF | Daily, index points |
| IEUR US Equity | iShares Core MSCI Europe | Equity ETF | Daily, index points |
| FEZ US Equity | SPDR Euro Stoxx 50 ETF | Equity ETF | Daily, index points |
| VGK US Equity | Vanguard FTSE Europe ETF | Equity ETF | Daily, index points |
| HEDJ US Equity | WisdomTree Europe Hedged | Equity ETF | Daily, index points |

### EU -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| MPMIEZMA Index | EU Mfg PMI | PMI | Monthly, index |
| SNTEEUH6 Index | Sentix EU Econ Expectations 6M | Sentiment | Monthly, index |
| SNTEEUH0 Index | Sentix EU Current Situation | Sentiment | Monthly, index |
| EUCBLIYY Index | EU CLI YoY | Econ | Monthly, % YoY |
| ECCPEMUY Index | EU CPI YoY | Inflation | Monthly, % YoY |
| ENGKEMU Index | EU Nominal GDP | GDP | Quarterly, level |
| EUITEMU Index | EU Industrial Production | Activity | Monthly, index |
| LNTN27Y Index | EU Wage Growth | Wages | Quarterly, % YoY |

### UK -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| UKX Index | FTSE 100 | Equity Index | Daily, index points |
| GBPUSD Curncy | GBP/USD Spot | FX | Daily, FX rate |
| BPSWS1 Curncy | GBP SONIA 1Y Swap | Rates | Daily, rate % |
| BPSWS2 Curncy | GBP SONIA 2Y Swap | Rates | Daily, rate % |
| BPSWS5 Curncy | GBP SONIA 5Y Swap | Rates | Daily, rate % |
| BPSWS10 Curncy | GBP SONIA 10Y Swap | Rates | Daily, rate % |
| GTGBP2Y Govt | UK Gilt 2Y Yield | Govt Yield | Daily, yield % |
| GTGBP10Y Govt | UK Gilt 10Y Yield | Govt Yield | Daily, yield % |
| GUKG5 Index | UK Gilt 5Y | Govt Yield | Daily, yield % |
| GUKG10 Index | UK Gilt 10Y (alt) | Govt Yield | Daily, yield % |
| GUKG2 Index | UK Gilt 2Y (alt) | Govt Yield | Daily, yield % |
| S0141FS 2Y1Y BLC Curncy | SONIA 2Y1Y Forward | Rates Fwd | Daily, rate % |

### UK -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| UKAWYWHO Index | UK Average Weekly Wage | Wages | Monthly, % YoY |
| UKGRABMI Index | UK GDP Monthly | GDP | Monthly, index |
| UKIPI Index | UK Industrial Production | Activity | Monthly, index |
| GDDI112G Index | UK Govt Debt/GDP | Fiscal | Annual, % GDP |
| UKLFLF69 Index | UK Labour Force | Labor | Monthly, thousands |

### JP -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| NKY Index | Nikkei 225 | Equity Index | Daily, index points |
| NKA Index | Nikkei Futures | Equity Futures | Daily, index points |
| TPX Index | TOPIX | Equity Index | Daily, index points |
| USDJPY Curncy | USD/JPY Spot | FX | Daily, FX rate |
| JYSO1 Curncy | JPY OIS 1Y Swap | Rates | Daily, rate % |
| JYSO2 Curncy | JPY OIS 2Y Swap | Rates | Daily, rate % |
| JYSO5 Curncy | JPY OIS 5Y Swap | Rates | Daily, rate % |
| JYSO10 Curncy | JPY OIS 10Y Swap | Rates | Daily, rate % |
| GTJPY2Y Govt | JGB 2Y Yield | Govt Yield | Daily, yield % |
| GTJPY10Y Govt | JGB 10Y Yield | Govt Yield | Daily, yield % |
| GJGB2 Index | JGB 2Y (alt) | Govt Yield | Daily, yield % |
| GJGB5 Index | JGB 5Y | Govt Yield | Daily, yield % |
| .JP10REAL G Index | Japan 10Y Real Yield | Real Yield | Daily, yield % |
| JYSWIT2 BLC Index | Japan 2Y Inflation Swap | Inflation Swap | Daily, rate % |
| JYSWIT10 BLC Index | Japan 10Y Inflation Swap | Inflation Swap | Daily, rate % |

### JP -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| JGDPOGDP Index | Japan GDP | GDP | Quarterly, level |
| OEJPNGBK Index | Japan Nominal GDP | GDP | Quarterly, level |
| JNIP Index | Japan Industrial Production | Activity | Monthly, index |
| JNLSUCTL Index | Japan Wage Data | Wages | Monthly, % YoY |
| JNCPIYOY Index | Japan CPI YoY | Inflation | Monthly, % YoY |
| JNISIVR Index | Japan Inventory/Shipment Ratio | Activity | Monthly, ratio |

### AU -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| ASX Index | ASX 200 | Equity Index | Daily, index points |
| AUDUSD Curncy | AUD/USD Spot | FX | Daily, FX rate |
| AUDNZD Curncy | AUD/NZD | FX Cross | Daily, FX rate |
| AUDCAD Curncy | AUD/CAD | FX Cross | Daily, FX rate |
| ADSWAP1 Curncy | AUD 1Y Swap | Rates | Daily, rate % |
| ADSWAP2 Curncy | AUD 2Y Swap | Rates | Daily, rate % |
| ADSWAP5 Curncy | AUD 5Y Swap | Rates | Daily, rate % |
| ADSWAP10 Curncy | AUD 10Y Swap | Rates | Daily, rate % |
| ADSW5 Curncy | AUD 5Y Swap (alt) | Rates | Daily, rate % |
| ADSO10 Curncy | AUD OIS 10Y | Rates | Daily, rate % |
| .ADSW2_10 U Index | AUD 2s10s Swap Spread | Rates | Daily, spread bp |
| GTAUD2Y Govt | AUD Govt 2Y | Govt Yield | Daily, yield % |
| GTAUD10Y Govt | AUD Govt 10Y | Govt Yield | Daily, yield % |

### AU -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| AUNAGDPY Index | AU GDP YoY | GDP | Quarterly, % YoY |
| AULILEAD Index | Westpac Leading Index | Econ | Monthly, index |
| AUWCBY Index | AU Wage Price Index incl bonus | Wages | Quarterly, % YoY |
| AUGDCPWS Index | AU Total Compensation of Employees | Wages | Quarterly, level |
| AUHRAMTL Index | AU Aggregate Monthly Hours | Labor | Monthly, level |
| AULF64LT Index | AU 15-64 Labour Force | Labor | Monthly, thousands |
| AUUPAUE Index | AU Underemployment Rate | Labor | Monthly, % |
| AUNDUR Index | AU Underutilization Rate | Labor | Monthly, % |
| AULFUNEM Index | AU Unemployment Rate | Labor | Monthly, % |
| GDDI193G Index | AU Govt Debt/GDP | Fiscal | Annual, % GDP |

### NZ -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| NZDUSD Curncy | NZD/USD Spot | FX | Daily, FX rate |
| NDSWAP1 Curncy | NZD 1Y Swap | Rates | Daily, rate % |
| NDSWAP2 Curncy | NZD 2Y Swap | Rates | Daily, rate % |
| NDSWAP5 Curncy | NZD 5Y Swap | Rates | Daily, rate % |
| NDSWAP10 Curncy | NZD 10Y Swap | Rates | Daily, rate % |
| NDSO10 Curncy | NZD OIS 10Y | Rates | Daily, rate % |
| .NDSW2_10 U Index | NZD 2s10s Swap Spread | Rates | Daily, spread bp |
| GTNZD2Y Govt | NZD Govt 2Y | Govt Yield | Daily, yield % |
| GTNZD10Y Govt | NZD Govt 10Y | Govt Yield | Daily, yield % |
| GNZGB2 Index | NZ Govt Bond 2Y (alt) | Govt Yield | Daily, yield % |
| GNZGB5 Index | NZ Govt Bond 5Y | Govt Yield | Daily, yield % |

### NZ -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| NZEMFJAS Index | NZ Filled Jobs SA | Labor | Monthly, level |
| NZLFUNER Index | NZ Unemployment Rate | Labor | Quarterly, % |
| GDDI196C Index | NZ Govt Debt/GDP | Fiscal | Annual, % GDP |

### CA -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDCAD Curncy | USD/CAD Spot | FX | Daily, FX rate |
| CDSO5 BGNT Curncy | CAD OIS 5Y Swap | Rates | Daily, rate % |
| CDSO10 BGN Curncy | CAD OIS 10Y Swap | Rates | Daily, rate % |
| GTCAD2Y Govt | CAD Govt 2Y | Govt Yield | Daily, yield % |
| GTCAD10Y Govt | CAD Govt 10Y | Govt Yield | Daily, yield % |
| GCAN2YR Index | Canada Govt 2Y (alt) | Govt Yield | Daily, yield % |
| GCAN5YR Index | Canada Govt 5Y | Govt Yield | Daily, yield % |

### CA -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| CGE9MP Index | Canada GDP | GDP | Quarterly, level |
| CAGPINDP Index | Canada Industrial Production | Activity | Monthly, index |
| GDDI156G Index | Canada Govt Debt/GDP | Fiscal | Annual, % GDP |

### CH -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| SMI Index | Swiss Market Index | Equity Index | Daily, index points |
| USDCHF Curncy | USD/CHF Spot | FX | Daily, FX rate |
| SFSNT5 BGNT Curncy | CHF 5Y Swap | Rates | Daily, rate % |
| GTCHF2Y Govt | CHF Govt 2Y | Govt Yield | Daily, yield % |
| GTCHF10Y Govt | CHF Govt 10Y | Govt Yield | Daily, yield % |
| GSWISS02 Index | Swiss Govt 2Y (alt) | Govt Yield | Daily, yield % |
| GSWISS05 Index | Swiss Govt 5Y | Govt Yield | Daily, yield % |

### NO -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| NKSW5 Curncy | NOK 5Y Swap | Rates | Daily, rate % |
| NKS2Y Curncy | NOK 2Y Swap | Rates | Daily, rate % |
| GTNOK2Y Govt | NOK Govt 2Y | Govt Yield | Daily, yield % |
| GTNOK10Y Govt | NOK Govt 10Y | Govt Yield | Daily, yield % |

### SE -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDSEK Curncy | USD/SEK | FX | Daily, FX rate |
| NOKSEK Curncy | NOK/SEK | FX Cross | Daily, FX rate |
| SKSW5 Curncy | SEK 5Y Swap | Rates | Daily, rate % |
| SKSW2 Curncy | SEK 2Y Swap | Rates | Daily, rate % |
| GTSEK2Y Govt | SEK Govt 2Y | Govt Yield | Daily, yield % |
| GTSEK10Y Govt | SEK Govt 10Y | Govt Yield | Daily, yield % |

---

## Asia

### CN -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| SHSZ300 Index | CSI 300 | Equity Index | Daily, index points |
| SHCOMP Index | Shanghai Composite | Equity Index | Daily, index points |
| HSCEI Index | Hang Seng China Enterprises | Equity Index | Daily, index points |
| HC1 Index | HSCEI Futures Generic 1st | Equity Futures | Daily, index points |
| USDCNH Curncy | USD/CNH Spot | FX | Daily, FX rate |
| USDCNY Curncy | USD/CNY Spot | FX | Daily, FX rate |
| CCSWNI1 Curncy | CNY IRS 1Y Swap | Rates | Daily, rate % |
| CCSWNI2 Curncy | CNY IRS 2Y Swap | Rates | Daily, rate % |
| CCSWNI3 Curncy | CNY IRS 3Y Swap | Rates | Daily, rate % |
| CCSWNI5 Curncy | CNY IRS 5Y Swap | Rates | Daily, rate % |
| CCSWNI7 Curncy | CNY IRS 7Y Swap | Rates | Daily, rate % |
| CCSWNI10 Curncy | CNY IRS 10Y Swap | Rates | Daily, rate % |
| CCSWO5 BGNT Curncy | CNH Offshore 5Y Swap | Rates | Daily, rate % |
| GCNY2YR Index | China Govt 2Y Yield | Govt Yield | Daily, yield % |
| GCNY5YR Index | China Govt 5Y Yield | Govt Yield | Daily, yield % |
| TBS1 Comdty | CGB 30Y Futures | Bond Futures | Daily, price points |

### CN -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| CNCPIYOY Index | China CPI YoY | Inflation | Monthly, % YoY |
| OECNNGAE Index | China Nominal GDP | GDP | Quarterly, level |
| CHINWAG Index | China Wage Growth | Wages | Annual, % YoY |
| CNVSTTL Index | Auto Sales | Activity | Monthly, level |
| CHMMCEMT Index | Cement Production | Activity | Monthly, level |
| CHMMROSL Index | Plate Glass Production | Activity | Monthly, level |
| CNLNNFIM Index | CN New Loans | Credit | Monthly, CNY bn |
| CNRWRFTO Index | CN Railway Freight | Activity | Monthly, level |
| CHRXIRCY Index | CN Real Retail Sales | Consumption | Monthly, % YoY |
| CHFQM021 Index | CN Industrial Output | Activity | Monthly, % YoY |
| CNNGPQ$ Index | CN Quarterly Nominal GDP | GDP | Quarterly, CNY bn |
| CNGDPYOY Index | CN Real GDP YoY | GDP | Quarterly, % YoY |
| CAQIBEJC Index | Beijing Air Quality Index | AQI | Daily, index |
| CAQICHDC Index | Chandigarh Air Quality Index | AQI | Daily, index |
| CAQIGUZC Index | Guangzhou Air Quality Index | AQI | Daily, index |
| CAQISHHC Index | Shanghai Air Quality Index | AQI | Daily, index |
| CAQISHYC Index | Shenyang Air Quality Index | AQI | Daily, index |

### KR -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| KOSPI Index | KOSPI | Equity Index | Daily, index points |
| KOSPI2 Index | KOSPI 200 | Equity Index | Daily, index points |
| KMA Index | KOSPI200 Futures | Equity Futures | Daily, index points |
| USDKRW Curncy | USD/KRW Spot | FX | Daily, FX rate |
| KWN+1M Curncy | KRW NDF 1M | FX NDF | Daily, FX rate |
| KWN12M Index | KRW 12M Forward Points | FX Forward | Daily, forward pts |
| KRW F043 Curncy | KRW Spot (alt) | FX | Daily, FX rate |
| KWSWNI1 Curncy | KRW IRS 1Y Swap | Rates | Daily, rate % |
| KWSWNI2 Curncy | KRW IRS 2Y Swap | Rates | Daily, rate % |
| KWSWNI3 Curncy | KRW IRS 3Y Swap | Rates | Daily, rate % |
| KWSWNI5 Curncy | KRW IRS 5Y Swap | Rates | Daily, rate % |
| KWSWNI7 Curncy | KRW IRS 7Y Swap | Rates | Daily, rate % |
| KWSWNI10 Curncy | KRW IRS 10Y Swap | Rates | Daily, rate % |
| GVSK2YR Index | Korea Treasury Bond 2Y | Govt Yield | Daily, yield % |
| GVSK5YR Index | Korea Treasury Bond 5Y | Govt Yield | Daily, yield % |
| GVSK10YR Index | Korea Treasury Bond 10Y | Govt Yield | Daily, yield % |
| GVSK30YR Index | Korea Treasury Bond 30Y | Govt Yield | Daily, yield % |
| KWSWO5 Curncy | KRW Onshore IRS 5Y | Rates | Daily, rate % |
| KEA Comdty | KTB 3Y Futures | Bond Futures | Daily, price points |
| KAAA Comdty | KTB 10Y Futures | Bond Futures | Daily, price points |
| KWRT1T KRWB Curncy | Korea Repo Rate | Rates | Daily, rate % |
| S0205FS 1Y1Y BLC Curncy | KRW 1Y1Y Forward | Rates Fwd | Daily, rate % |

### KR -- Econ

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| KOCPIYOY Index | Korea CPI YoY | Inflation | Monthly, % YoY |
| KOPII Index | Korea Production Index | Activity | Monthly, index |
| KOPSI Index | Korea Shipment Index | Activity | Monthly, index |
| KOEGSTOT Index | Korea Quarterly Nominal GDP | GDP | Quarterly, KRW bn |
| KOECTOTY Index | Korea Real GDP YoY | GDP | Quarterly, % YoY |

### IN -- Market

#### Equity Indices

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| NIFTY Index | Nifty 50 | Equity Index | Daily, index points |
| NSE500 Index | Nifty 500 | Equity Index | Daily, index points |
| SENSEX Index | BSE Sensex | Equity Index | Daily, index points |
| SHYF6 Index | BSE Sensex (alt ticker) | Equity Index | Daily, index points |
| NSESMCP Index | NSE Smallcap Index | Equity Index | Daily, index points |
| MXIN Index | MSCI India | Equity Index | Daily, index points |
| MXIN0UT Index | MSCI India Utilities | Equity Index | Daily, index points |

#### FX -- Spot & NDF Outrights

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDINR Curncy | USD/INR Spot | FX | Daily, FX rate |
| IRN+1W Curncy | INR NDF 1W Outright | FX NDF | Daily, FX rate |
| IRN+1M Curncy | INR NDF 1M Outright | FX NDF | Daily, FX rate |
| IRN+2M Curncy | INR NDF 2M Outright | FX NDF | Daily, FX rate |
| IRN+3M Curncy | INR NDF 3M Outright | FX NDF | Daily, FX rate |
| IRN+6M Curncy | INR NDF 6M Outright | FX NDF | Daily, FX rate |
| IRN+9M Curncy | INR NDF 9M Outright | FX NDF | Daily, FX rate |
| IRN+12M Curncy | INR NDF 12M Outright | FX NDF | Daily, FX rate |
| IRN+2Y Curncy | INR NDF 2Y Outright | FX NDF | Daily, FX rate |
| IRN+3Y Curncy | INR NDF 3Y Outright | FX NDF | Daily, FX rate |
| IRN+5Y Curncy | INR NDF 5Y Outright | FX NDF | Daily, FX rate |

#### FX -- NDF Forward Points

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRN1W Curncy | INR NDF 1W Fwd Points | FX Forward | Daily, forward pts |
| IRN1M Curncy | INR NDF 1M Fwd Points | FX Forward | Daily, forward pts |
| IRN2M Curncy | INR NDF 2M Fwd Points | FX Forward | Daily, forward pts |
| IRN3M Curncy | INR NDF 3M Fwd Points | FX Forward | Daily, forward pts |
| IRN6M Curncy | INR NDF 6M Fwd Points | FX Forward | Daily, forward pts |
| IRN9M Curncy | INR NDF 9M Fwd Points | FX Forward | Daily, forward pts |
| IRN12M Curncy | INR NDF 12M Fwd Points | FX Forward | Daily, forward pts |
| IRN2Y Curncy | INR NDF 2Y Fwd Points | FX Forward | Daily, forward pts |
| IRN3Y Curncy | INR NDF 3Y Fwd Points | FX Forward | Daily, forward pts |
| IRN5Y Curncy | INR NDF 5Y Fwd Points | FX Forward | Daily, forward pts |

#### FX -- Onshore Forward Points

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRO1W Curncy | INR Onshore 1W Fwd Points | FX Forward | Daily, forward pts |
| IRO1M Curncy | INR Onshore 1M Fwd Points | FX Forward | Daily, forward pts |
| IRO2M Curncy | INR Onshore 2M Fwd Points | FX Forward | Daily, forward pts |
| IRO3M Curncy | INR Onshore 3M Fwd Points | FX Forward | Daily, forward pts |
| IRO6M Curncy | INR Onshore 6M Fwd Points | FX Forward | Daily, forward pts |
| IRO9M Curncy | INR Onshore 9M Fwd Points | FX Forward | Daily, forward pts |
| IRO12M Curncy | INR Onshore 12M Fwd Points | FX Forward | Daily, forward pts |
| IRO2Y Curncy | INR Onshore 2Y Fwd Points | FX Forward | Daily, forward pts |
| IRO3Y Curncy | INR Onshore 3Y Fwd Points | FX Forward | Daily, forward pts |
| IRO5Y Curncy | INR Onshore 5Y Fwd Points | FX Forward | Daily, forward pts |

#### Govt Bond Yield Curve

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| GIND2Y Index | India Govt Bond 2Y Yield | Govt Yield | Daily, yield % |
| GIND3Y Index | India Govt Bond 3Y Yield | Govt Yield | Daily, yield % |
| GIND5Y Index | India Govt Bond 5Y Yield | Govt Yield | Daily, yield % |
| GTINR5Y Govt | India 5Y Govt Yield (Generic) | Govt Yield | Daily, yield % |
| GIND7Y Index | India Govt Bond 7Y Yield | Govt Yield | Daily, yield % |
| GIND10Y Index | India Govt Bond 10Y Yield | Govt Yield | Daily, yield % |
| GIND10YR Index | India Govt 10Y Yield (alt) | Govt Yield | Daily, yield % |
| GTINR10Y Govt | India 10Y Govt Yield (Generic) | Govt Yield | Daily, yield % |
| GIND15Y Index | India Govt Bond 15Y Yield | Govt Yield | Daily, yield % |
| GIND30Y Index | India Govt Bond 30Y Yield | Govt Yield | Daily, yield % |

#### NDOIS (Non-Deliverable OIS)

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRSWNIA Curncy | INR NDOIS 1M | Rates | Daily, rate % |
| IRSWNIB Curncy | INR NDOIS 2M | Rates | Daily, rate % |
| IRSWNIC Curncy | INR NDOIS 3M | Rates | Daily, rate % |
| IRSWNIF Curncy | INR NDOIS 6M | Rates | Daily, rate % |
| IRSWNI1 Curncy | INR NDOIS 1Y | Rates | Daily, rate % |
| IRSWNI2 Curncy | INR NDOIS 2Y | Rates | Daily, rate % |
| IRSWNI3 Curncy | INR NDOIS 3Y | Rates | Daily, rate % |
| IRSWNI5 Curncy | INR NDOIS 5Y | Rates | Daily, rate % |
| IRSWNI7 Curncy | INR NDOIS 7Y | Rates | Daily, rate % |
| IRSWNI10 Curncy | INR NDOIS 10Y | Rates | Daily, rate % |

#### Onshore OIS

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRSWOA Curncy | INR Onshore OIS 1M | Rates | Daily, rate % |
| IRSWOB Curncy | INR Onshore OIS 2M | Rates | Daily, rate % |
| IRSWOC Curncy | INR Onshore OIS 3M | Rates | Daily, rate % |
| IRSWOF Curncy | INR Onshore OIS 6M | Rates | Daily, rate % |
| IRSWO1 Curncy | INR Onshore OIS 1Y | Rates | Daily, rate % |
| IRSWO2 Curncy | INR Onshore OIS 2Y | Rates | Daily, rate % |
| IRSWO3 Curncy | INR Onshore OIS 3Y | Rates | Daily, rate % |
| IRSWO5 Curncy | INR Onshore OIS 5Y | Rates | Daily, rate % |
| IRSWO7 Curncy | INR Onshore OIS 7Y | Rates | Daily, rate % |
| IRSWO10 Curncy | INR Onshore OIS 10Y | Rates | Daily, rate % |

#### NDS (Non-Deliverable Cross-Currency Swap)

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRUSON1 Curncy | INR NDCCS 1Y | Rates | Daily, rate % |
| IRUSON2 Curncy | INR NDCCS 2Y | Rates | Daily, rate % |
| IRUSON3 Curncy | INR NDCCS 3Y | Rates | Daily, rate % |
| IRUSON5 Curncy | INR NDCCS 5Y | Rates | Daily, rate % |
| IRUSON7 Curncy | INR NDCCS 7Y | Rates | Daily, rate % |
| IRUSON10 Curncy | INR NDCCS 10Y | Rates | Daily, rate % |

#### NDOIS Forward Curve (S0266)

**Par rates:** `S0266P {tenor} BLC3 Curncy`

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| S0266P 1Y BLC3 Curncy | NDOIS 1Y Par Rate | Rates Fwd | Daily, rate % |
| S0266P 2Y BLC3 Curncy | NDOIS 2Y Par Rate | Rates Fwd | Daily, rate % |
| S0266P 3Y BLC3 Curncy | NDOIS 3Y Par Rate | Rates Fwd | Daily, rate % |
| S0266P 4Y BLC3 Curncy | NDOIS 4Y Par Rate | Rates Fwd | Daily, rate % |
| S0266P 5Y BLC3 Curncy | NDOIS 5Y Par Rate | Rates Fwd | Daily, rate % |
| S0266P 8Y BLC3 Curncy | NDOIS 8Y Par Rate | Rates Fwd | Daily, rate % |
| S0266P 9Y BLC3 Curncy | NDOIS 9Y Par Rate | Rates Fwd | Daily, rate % |

**Forward-starting:** `S0266FS {fwd}{tail} BLC Curncy` — "{fwd} forward, {tail} swap"

Available forward legs: 3M, 6M, 1Y, 2Y, 3Y, 4Y, 5Y
Available tail tenors: 1Y, 2Y, 3Y, 4Y, 5Y, 8Y, 9Y

Examples:

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| S0266FS 1Y1Y BLC Curncy | NDOIS 1Y fwd 1Y swap | Rates Fwd | Daily, rate % |
| S0266FS 2Y1Y BLC Curncy | NDOIS 2Y fwd 1Y swap | Rates Fwd | Daily, rate % |
| S0266FS 5Y5Y BLC Curncy | NDOIS 5Y fwd 5Y swap | Rates Fwd | Daily, rate % |
| S0266FS 3M3Y BLC Curncy | NDOIS 3M fwd 3Y swap | Rates Fwd | Daily, rate % |

#### NDCCS Forward Curve (S0157)

**Par rates:** `S0157P {tenor} BLC3 Curncy`

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| S0157P 1Y BLC3 Curncy | NDCCS 1Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 2Y BLC3 Curncy | NDCCS 2Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 3Y BLC3 Curncy | NDCCS 3Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 4Y BLC3 Curncy | NDCCS 4Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 5Y BLC3 Curncy | NDCCS 5Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 8Y BLC3 Curncy | NDCCS 8Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 9Y BLC3 Curncy | NDCCS 9Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 10Y BLC3 Curncy | NDCCS 10Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 15Y BLC3 Curncy | NDCCS 15Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 20Y BLC3 Curncy | NDCCS 20Y Par Rate | Rates Fwd | Daily, rate % |
| S0157P 30Y BLC3 Curncy | NDCCS 30Y Par Rate | Rates Fwd | Daily, rate % |

**Forward-starting:** `S0157FS {fwd}{tail} BLC Curncy` — "{fwd} forward, {tail} swap"

Available forward legs: 3M, 6M, 1Y, 2Y, 3Y, 4Y, 5Y, 10Y, 15Y, 30Y
Available tail tenors: 1Y, 2Y, 3Y, 4Y, 5Y, 8Y, 9Y, 10Y, 15Y, 20Y, 30Y

Examples:

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| S0157FS 1Y1Y BLC Curncy | NDCCS 1Y fwd 1Y swap | Rates Fwd | Daily, rate % |
| S0157FS 2Y5Y BLC Curncy | NDCCS 2Y fwd 5Y swap | Rates Fwd | Daily, rate % |
| S0157FS 5Y5Y BLC Curncy | NDCCS 5Y fwd 5Y swap | Rates Fwd | Daily, rate % |
| S0157FS 10Y10Y BLC Curncy | NDCCS 10Y fwd 10Y swap | Rates Fwd | Daily, rate % |

#### Sector Equities -- IT Services

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| TCS IN Equity | Tata Consultancy | Equity | Daily, local price |
| WPRO IN Equity | Wipro | Equity | Daily, local price |
| HCLT IN Equity | HCL Technologies | Equity | Daily, local price |
| INFO IN Equity | Infosys | Equity | Daily, local price |

#### Sector Equities -- Energy & Fuel

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| RELIANCE IN Equity | Reliance Industries | Equity | Daily, local price |
| ONGC IN Equity | Oil & Natural Gas Corp | Equity | Daily, local price |
| IOCL IN Equity | Indian Oil Corp | Equity | Daily, local price |
| BPCL IN Equity | Bharat Petroleum | Equity | Daily, local price |
| HPCL IN Equity | Hindustan Petroleum | Equity | Daily, local price |
| OINL IN Equity | Oil India Ltd | Equity | Daily, local price |
| GAIL IN Equity | GAIL India | Equity | Daily, local price |
| PLNG IN Equity | Petronet LNG | Equity | Daily, local price |
| COAL IN Equity | Coal India | Equity | Daily, local price |

#### Sector Equities -- Tourism & Aviation

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INDIGO IN Equity | IndiGo Airlines | Equity | Daily, local price |
| SJET IN Equity | SpiceJet | Equity | Daily, local price |
| GMRAIRPO IN Equity | GMR Airports | Equity | Daily, local price |
| IH IN Equity | Indian Hotels (Taj) | Equity | Daily, local price |
| LEMONTRE IN Equity | Lemon Tree Hotels | Equity | Daily, local price |

#### Sector Equities -- Real Estate

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| DLFU IN Equity | DLF Ltd | Equity | Daily, local price |
| GPL IN Equity | Godrej Properties | Equity | Daily, local price |
| OBER IN Equity | Oberoi Realty | Equity | Daily, local price |
| PEPL IN Equity | Prestige Estates | Equity | Daily, local price |
| BRGD IN Equity | Brigade Enterprises | Equity | Daily, local price |
| SOBHA IN Equity | Sobha Ltd | Equity | Daily, local price |
| MAHLIFE IN Equity | Mahindra Lifespace | Equity | Daily, local price |

#### Sector Equities -- Telecom

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| BHARTI IN Equity | Bharti Airtel | Equity | Daily, local price |
| IDEA IN Equity | Vodafone Idea | Equity | Daily, local price |

### IN -- Econ

#### GDP & National Accounts

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IGQREGDY Index | India Real GDP YoY | GDP | Quarterly, % YoY |
| IGQRYOY Index | India GVA (Gross Value Added) YoY | GDP | Quarterly, % YoY |
| IGQNEGDP Index | India Quarterly Nominal GDP | GDP | Quarterly, INR bn |
| INBGDRQY Index | India Real GDP Q YoY | GDP | Quarterly, % YoY |
| IGDRYOY Index | India GDP Financial Year Estimate YoY | GDP | Annual, % YoY |
| EHGDIN Index | India GDP YoY (Bloomberg Est.) | GDP | Quarterly, % YoY |
| IGDNPFCY Index | India Consumer Spending Nominal YoY | Consumption | Annual, % YoY |

#### Inflation & Prices

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INFUTOTY Index | India CPI Headline YoY | Inflation | Monthly, % YoY |
| INFINFY Index | India WPI Inflation YoY | Inflation | Monthly, % YoY |
| EGWPFUEL Index | India WPI Fuel & Power | Inflation | Monthly, index |
| IDWPOMEY Index | India WPI Manufacturing (Energy) | Inflation | Monthly, index |
| IDWPOMEG Index | India WPI Manufacturing (General) | Inflation | Monthly, index |
| INBGCEY Index | India WPI Commodities YoY | Inflation | Monthly, % YoY |
| INPWDDMG Index | India WPI Manufacturing | Inflation | Monthly, index |
| INPWDDMW Index | India WPI Manufacturing (Weighted) | Inflation | Monthly, index |
| INPWGAGN Index | India WPI Agriculture | Inflation | Monthly, index |
| INCGGESA Index | India WPI General (SA) | Inflation | Monthly, index |
| INPWTHGN Index | India WPI Total (General) | Inflation | Monthly, index |

#### Monetary Policy & Money Supply

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INRPYLDP Index | India RBI Repo Rate | Rates | As announced, rate % |
| INMSM1 Index | India Money Supply M1 | Monetary | Monthly, INR mn |
| INMSM3 Index | India Money Supply M3 | Monetary | Monthly, INR mn |
| INRECURR Index | India Reserve Money - Currency in Circ. | Monetary | Weekly, INR mn |
| INRESV Index | India Reserve Money | Monetary | Weekly, INR mn |
| IBCDBACY Index | India Bank Credit YoY | Credit | Monthly, % YoY |
| IBCDINDT Index | India Credit Growth | Credit | Monthly, % YoY |
| INBGBKLQ Index | India Banking Liquidity | Banking | Daily, INR bn |

#### Trade & Balance of Payments

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| ECOYBINN Index | India Trade Balance (Annualized) | Trade | Monthly, USD |
| ECOCINN Index | India Current Account Balance | Trade | Quarterly, USD |
| IBOPCURR Index | India BoP Current Account | Trade | Quarterly, USD bn |
| FDIVIND Index | India Foreign Direct Investment | Trade | Annual, USD |
| INITSEXP Index | India IT Services Exports | Trade | Monthly, USD mn |

#### Industrial Production & PMI

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INPIINDY Index | India Industrial Production YoY | Activity | Monthly, % YoY |
| MPMIINMA Index | HSBC India PMI Manufacturing | PMI | Monthly, index |
| MPMIINSA Index | HSBC India PMI Services | PMI | Monthly, index |
| MPMIINCA Index | HSBC India PMI Composite | PMI | Monthly, index |
| NISSMINI Index | India Mining Index | Activity | Monthly, index |
| INFRELE Index | India Electricity Production | Activity | Monthly, index |

#### Reserves & Fiscal

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INMORES$ Index | India FX Reserves | Reserves | Weekly, USD bn |
| 534.055 Index | India International Reserves (IMF) | Reserves | Monthly, USD |
| INFFFYTD Index | India Fiscal Deficit YTD | Fiscal | Monthly, INR bn |
| EHBBIN Index | India Budget Balance %GDP (SA) | Fiscal | Quarterly, % GDP |
| EHBBINY Index | India Budget Balance %GDP (NSA) | Fiscal | Annual, % GDP |

#### Labor Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INBGRIWG Index | India Wage Growth | Wages | Quarterly, % YoY |
| WBSOINDR Index | India Labor Force Participation Rate | Labor | Annual, % |

#### Agriculture

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| WHEAINYD Index | India Wheat Production YoY | Agriculture | Annual, % YoY |
| INWIWHEA Index | India Wheat Index | Agriculture | Monthly, index |
| INCNWHEP Index | India Wheat Production | Agriculture | Seasonal, level |
| INCNWHET Index | India Wheat Total | Agriculture | Seasonal, level |
| INBGAICI Index | India Agriculture Index | Agriculture | Monthly, index |
| INBGAICY Index | India Agriculture YoY | Agriculture | Monthly, % YoY |
| AFWHAHIN Index | India Wheat - Haryana | Agriculture | Seasonal, index |
| AFWHBSIN Index | India Wheat - Various States | Agriculture | Seasonal, index |
| AFWHDCIN Index | India Wheat - Delhi | Agriculture | Seasonal, index |
| AFWHESIN Index | India Wheat - Regional | Agriculture | Seasonal, index |
| AFWHEXIN Index | India Wheat - Export | Agriculture | Seasonal, index |

#### Vehicle Sales

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INPMVCRD Index | India Passenger Vehicle Sales Domestic | Auto Sales | Monthly, thousands |
| INPMVCCR Index | India Passenger Vehicle Sales Export | Auto Sales | Monthly, units |
| INPMVCUT Index | India Commercial Vehicle Sales Total | Auto Sales | Monthly, units |
| INUPADVO Index | India Two-Wheeler Sales Domestic | Auto Sales | Monthly, thousands |
| INUPDGVO Index | India Three-Wheeler Sales Domestic | Auto Sales | Monthly, thousands |
| INUPGGVO Index | India Tractor Sales Domestic | Auto Sales | Monthly, thousands |

#### Tourism

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| INTATOTY Index | India Foreign Tourist Arrivals YoY | Tourism | Monthly, % YoY |
| INTAOUTB Index | India Outbound Tourism | Tourism | Monthly, level |
| INEATOT Index | India Tourism Earnings Total | Tourism | Monthly, INR |
| INEATOT$ Index | India Tourism Earnings Total (USD) | Tourism | Monthly, USD |
| INEATO$Y Index | India Tourism Earnings (USD) YoY | Tourism | Monthly, % YoY |
| INEATOTY Index | India Tourism Earnings Total YoY | Tourism | Monthly, % YoY |
| INAEA$ Index | India Tourism Earnings per Arrival (USD) | Tourism | Monthly, USD |
| INAEA$Y Index | India Tourism Earnings per Arrival YoY | Tourism | Monthly, % YoY |

### TH -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| SET Index | SET Index | Equity Index | Daily, index points |
| THB+1M Curncy | THB NDF 1M | FX NDF | Daily, FX rate |
| THB BGN Curncy | THB Spot | FX | Daily, FX rate |
| TBSWH1 Curncy | THB IRS 1Y Swap | Rates | Daily, rate % |
| TBSWH2 Curncy | THB IRS 2Y Swap | Rates | Daily, rate % |
| TBSWH3 Curncy | THB IRS 3Y Swap | Rates | Daily, rate % |
| TBSWH5 Curncy | THB IRS 5Y Swap | Rates | Daily, rate % |
| TBSWH7 Curncy | THB IRS 7Y Swap | Rates | Daily, rate % |
| TBSWH10 Curncy | THB IRS 10Y Swap | Rates | Daily, rate % |
| GVTL2YR Index | Thailand Govt 2Y Yield | Govt Yield | Daily, yield % |
| GVTL5YR Index | Thailand Govt 5Y Yield | Govt Yield | Daily, yield % |
| GVTL10YR Index | Thailand Govt 10Y Yield | Govt Yield | Daily, yield % |
| GVTL30YR Index | Thailand Govt 30Y Yield | Govt Yield | Daily, yield % |
| THCPIYOY Index | Thailand CPI YoY | Inflation | Monthly, % YoY |

### MY -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| MRN+1M Curncy | MYR NDF 1M | FX NDF | Daily, FX rate |
| MRSWNI1 Curncy | MYR IRS 1Y Swap | Rates | Daily, rate % |
| MRSWNI2 Curncy | MYR IRS 2Y Swap | Rates | Daily, rate % |
| MRSWNI3 Curncy | MYR IRS 3Y Swap | Rates | Daily, rate % |
| MRSWNI5 Curncy | MYR IRS 5Y Swap | Rates | Daily, rate % |
| MRSWNI7 Curncy | MYR IRS 7Y Swap | Rates | Daily, rate % |
| MRSWNI10 Curncy | MYR IRS 10Y Swap | Rates | Daily, rate % |
| MACPIYOY Index | Malaysia CPI YoY | Inflation | Monthly, % YoY |

### SG -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| STI Index | Straits Times Index | Equity Index | Daily, index points |
| USDSGD Curncy | USD/SGD Spot | FX | Daily, FX rate |
| SDSOA1 Curncy | SGD SORA 1Y Swap | Rates | Daily, rate % |
| SDSOA2 Curncy | SGD SORA 2Y Swap | Rates | Daily, rate % |
| SDSOA3 Curncy | SGD SORA 3Y Swap | Rates | Daily, rate % |
| SDSOA5 Curncy | SGD SORA 5Y Swap | Rates | Daily, rate % |
| SDSOA7 Curncy | SGD SORA 7Y Swap | Rates | Daily, rate % |
| SDSOA10 Curncy | SGD SORA 10Y Swap | Rates | Daily, rate % |
| .SORA2_10 U Index | SGD SORA 2s10s | Rates | Daily, spread bp |
| SMASCORE Index | Singapore Core CPI | Inflation | Monthly, % YoY |
| CTSGSGD Index | MAS DLI NEER Ticker | FX Policy | Daily, index |
| SORACA3M Index | SORA 3M | Rates | Daily, rate % |
| SIMSM1Y% Index | Singapore M1 YoY | Monetary | Monthly, % YoY |
| SIOFRUS Index | Singapore Foreign Reserves | Reserves | Monthly, USD mn |

### HK -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| HSI Index | Hang Seng Index | Equity Index | Daily, index points |
| USDHKD Curncy | USD/HKD | FX | Daily, FX rate |
| HDSW1 Curncy | HKD 1Y Swap | Rates | Daily, rate % |
| HDSW2 Curncy | HKD 2Y Swap | Rates | Daily, rate % |
| HDSW3 Curncy | HKD 3Y Swap | Rates | Daily, rate % |
| HDSW5 Curncy | HKD 5Y Swap | Rates | Daily, rate % |
| HDSW10 Curncy | HKD 10Y Swap | Rates | Daily, rate % |
| HKCPIY Index | Hong Kong CPI YoY | Inflation | Monthly, % YoY |

### TW -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| TWSE Index | TWSE Index | Equity Index | Daily, index points |
| NTN+1M Curncy | TWD NDF 1M | FX NDF | Daily, FX rate |
| TDSWNI1 Curncy | TWD IRS 1Y | Rates | Daily, rate % |
| TDSWNI2 Curncy | TWD IRS 2Y | Rates | Daily, rate % |
| TDSWNI3 Curncy | TWD IRS 3Y | Rates | Daily, rate % |
| TDSWNI5 Curncy | TWD IRS 5Y | Rates | Daily, rate % |
| TDSWNI10 Curncy | TWD IRS 10Y | Rates | Daily, rate % |
| TWCPIYOY Index | Taiwan CPI YoY | Inflation | Monthly, % YoY |
| ECOXTWS Index | Taiwan GDP | GDP | Quarterly, level |
| TWMERY Index | Taiwan Wage Data | Wages | Monthly, % YoY |

### ID -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| JCI Index | Jakarta Composite | Equity Index | Daily, index points |
| IHN+1M Curncy | IDR NDF 1M | FX NDF | Daily, FX rate |
| GIDN2YR Index | Indonesia Govt 2Y Yield | Govt Yield | Daily, yield % |
| GTIDR2YR Corp | Indonesia Govt 2Y Yield (Corp) | Govt Yield | Daily, yield % |
| GIDN5YR Index | Indonesia Govt 5Y Yield | Govt Yield | Daily, yield % |
| GTIDR5YR Corp | Indonesia Govt 5Y Yield (Corp) | Govt Yield | Daily, yield % |
| GIDN10YR Index | Indonesia Govt 10Y | Govt Yield | Daily, yield % |
| GIDN30YR Index | Indonesia Govt 30Y Yield | Govt Yield | Daily, yield % |
| GTIDR1Y Govt | Indonesia Govt 1Y Yield | Govt Yield | Daily, yield % |
| IHSWAP5 Curncy | IDR ND CCS 5Y | Rates | Daily, rate % |
| IDCPIY Index | Indonesia CPI YoY | Inflation | Monthly, % YoY |
| IDM2YOY Index | Indonesia M2 YoY | Monetary | Monthly, % YoY |
| IDGRP Index | Indonesia Quarterly Nominal GDP | GDP | Quarterly, IDR bn |
| IDGDPY Index | Indonesia Real GDP YoY | GDP | Quarterly, % YoY |

### PH -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| PPN+1M Curncy | PHP NDF 1M | FX NDF | Daily, FX rate |
| GTPHP2YR Corp | Philippines Govt 2Y | Govt Yield | Daily, yield % |
| GTPHP5YR Corp | Philippines Govt 5Y | Govt Yield | Daily, yield % |
| GTPHP10YR Corp | Philippines Govt 10Y | Govt Yield | Daily, yield % |
| PPUSNI1 Curncy | PHP ND IRS 1Y | Rates | Daily, rate % |
| PPUSNI5 Curncy | PHP ND IRS 5Y | Rates | Daily, rate % |
| PHGDPC$ Index | Philippines Quarterly Nominal GDP | GDP | Quarterly, PHP bn |
| PHGDPYOY Index | Philippines Real GDP YoY | GDP | Quarterly, % YoY |

### VN -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| VND T130 Curncy | VND Spot | FX | Daily, FX rate |
| USDVND Curncy | USD/VND | FX | Daily, FX rate |
| SBVNUSD Index | VN Central Rate | FX Policy | Daily, FX rate |

---

## CEEMEA

### PL -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDPLN Curncy | USD/PLN | FX | Daily, FX rate |
| PZSW1 Curncy | PLN 1Y Swap | Rates | Daily, rate % |
| PZSW2 Curncy | PLN 2Y Swap | Rates | Daily, rate % |
| PZSW3 Curncy | PLN 3Y Swap | Rates | Daily, rate % |
| PZSW5 Curncy | PLN 5Y Swap | Rates | Daily, rate % |
| PZSW10 Curncy | PLN 10Y Swap | Rates | Daily, rate % |
| POGB2YR Index | Poland Govt 2Y Yield | Govt Yield | Daily, yield % |
| POGB5YR Index | Poland Govt 5Y Yield | Govt Yield | Daily, yield % |
| POGB10YR Index | Poland Govt 10Y Yield | Govt Yield | Daily, yield % |
| POGB30YR Index | Poland Govt 30Y Yield | Govt Yield | Daily, yield % |
| PODPL Index | Poland Quarterly Nominal GDP | GDP | Quarterly, PLN bn |
| POGDYOY Index | Poland Real GDP YoY | GDP | Quarterly, % YoY |

### CZ -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDCZK Curncy | USD/CZK | FX | Daily, FX rate |
| CKSW1 Curncy | CZK 1Y Swap | Rates | Daily, rate % |
| CKSW2 Curncy | CZK 2Y Swap | Rates | Daily, rate % |
| CKSW3 Curncy | CZK 3Y Swap | Rates | Daily, rate % |
| CKSW5 Curncy | CZK 5Y Swap | Rates | Daily, rate % |
| CKSW10 Curncy | CZK 10Y Swap | Rates | Daily, rate % |
| CZGB2YR Index | Czech Govt 2Y Yield | Govt Yield | Daily, yield % |
| CZGB5YR Index | Czech Govt 5Y Yield | Govt Yield | Daily, yield % |
| CZGB10YR Index | Czech Govt 10Y Yield | Govt Yield | Daily, yield % |
| CZGB30YR Index | Czech Govt 30Y Yield | Govt Yield | Daily, yield % |
| CZGDPCSA Index | Czech Quarterly Nominal GDP | GDP | Quarterly, CZK bn |
| CZGDPSAY Index | Czech Real GDP YoY | GDP | Quarterly, % YoY |

### HU -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDHUF Curncy | USD/HUF | FX | Daily, FX rate |
| HFSW1 Curncy | HUF 1Y Swap | Rates | Daily, rate % |
| HFSW2 Curncy | HUF 2Y Swap | Rates | Daily, rate % |
| HFSW3 Curncy | HUF 3Y Swap | Rates | Daily, rate % |
| HFSW5 Curncy | HUF 5Y Swap | Rates | Daily, rate % |
| HFSW10 Curncy | HUF 10Y Swap | Rates | Daily, rate % |
| GHGB2YR Index | Hungary Govt 2Y Yield | Govt Yield | Daily, yield % |
| GHGB5YR Index | Hungary Govt 5Y Yield | Govt Yield | Daily, yield % |
| GHGB10YR Index | Hungary Govt 10Y Yield | Govt Yield | Daily, yield % |
| GHGB30YR Index | Hungary Govt 30Y Yield | Govt Yield | Daily, yield % |
| HUGQT Index | Hungary Quarterly Nominal GDP | GDP | Quarterly, HUF bn |
| HUGPTOTL Index | Hungary Real GDP YoY | GDP | Quarterly, % YoY |

### IL -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDILS Curncy | USD/ILS | FX | Daily, FX rate |
| ISOIS1 Curncy | ILS OIS 1Y | Rates | Daily, rate % |
| ISOIS2 Curncy | ILS OIS 2Y | Rates | Daily, rate % |
| ISOIS3 Curncy | ILS OIS 3Y | Rates | Daily, rate % |
| ISOIS5 Curncy | ILS OIS 5Y | Rates | Daily, rate % |
| ISOIS10 Curncy | ILS OIS 10Y | Rates | Daily, rate % |
| GISR2YR Index | Israel Govt 2Y Yield | Govt Yield | Daily, yield % |
| GISR5YR Index | Israel Govt 5Y Yield | Govt Yield | Daily, yield % |
| GISR10YR Index | Israel Govt 10Y Yield | Govt Yield | Daily, yield % |
| GISR30YR Index | Israel Govt 30Y Yield | Govt Yield | Daily, yield % |
| ISGOPN Index | Israel Quarterly Nominal GDP | GDP | Quarterly, ILS bn |
| ISGDPNYY Index | Israel Real GDP YoY | GDP | Quarterly, % YoY |

### ZA -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| JALSH Index | JSE All Share | Equity Index | Daily, index points |
| USDZAR Curncy | USD/ZAR | FX | Daily, FX rate |
| SASW1 Curncy | ZAR 1Y Swap | Rates | Daily, rate % |
| SASW2 Curncy | ZAR 2Y Swap | Rates | Daily, rate % |
| SASW3 Curncy | ZAR 3Y Swap | Rates | Daily, rate % |
| SASW5 Curncy | ZAR 5Y Swap | Rates | Daily, rate % |
| SASW10 Curncy | ZAR 10Y Swap | Rates | Daily, rate % |
| GSAB2YR Index | South Africa Govt 2Y Yield | Govt Yield | Daily, yield % |
| GSAB5YR Index | South Africa Govt 5Y Yield | Govt Yield | Daily, yield % |
| GSAB10YR Index | South Africa Govt 10Y Yield | Govt Yield | Daily, yield % |
| GSAB30YR Index | South Africa Govt 30Y Yield | Govt Yield | Daily, yield % |
| ZANNGDP Index | South Africa Quarterly Nominal GDP | GDP | Quarterly, ZAR bn |
| SAGDVADY Index | South Africa Real GDP YoY | GDP | Quarterly, % YoY |

### TR -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| USDTRY Curncy | USD/TRY | FX | Daily, FX rate |
| TYSO1 Curncy | TRY OIS 1Y | Rates | Daily, rate % |
| TYSO2 Curncy | TRY OIS 2Y | Rates | Daily, rate % |
| TYSO3 Curncy | TRY OIS 3Y | Rates | Daily, rate % |
| TYSO5 Curncy | TRY OIS 5Y | Rates | Daily, rate % |
| TYSO10 Curncy | TRY OIS 10Y | Rates | Daily, rate % |
| GTUSDTR10YR Corp | Turkey USD Sovereign 10Y | Govt Yield | Daily, yield % |
| BV100965 BVLI Index | Turkey Local 10Y (BVAL) | Govt Yield | Daily, yield % |

### SA -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| SASEIDX Index | Saudi Tadawul Index | Equity Index | Daily, index points |
| USDSAR Curncy | USD/SAR | FX | Daily, FX rate |

---

## LatAm

### MX -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| MEXBOL Index | Mexico Bolsa IPC | Equity Index | Daily, index points |
| USDMXN Curncy | USD/MXN | FX | Daily, FX rate |
| MPSWL1 Curncy | MXN TIIE 1Y Swap | Rates | Daily, rate % |
| MPSWL2 Curncy | MXN TIIE 2Y Swap | Rates | Daily, rate % |
| MPSWL3 Curncy | MXN TIIE 3Y Swap | Rates | Daily, rate % |
| MPSWL5 Curncy | MXN TIIE 5Y Swap | Rates | Daily, rate % |
| MPSWL10 Curncy | MXN TIIE 10Y Swap | Rates | Daily, rate % |
| GTUSDMX10YR Corp | Mexico USD Sovereign 10Y | Govt Yield | Daily, yield % |
| BV100476 BVLI Index | Mexico Local 10Y (BVAL) | Govt Yield | Daily, yield % |
| MXNPSUNA Index | Mexico Quarterly Nominal GDP | GDP | Quarterly, MXN bn |
| MXGCTOT Index | Mexico Real GDP YoY | GDP | Quarterly, % YoY |

### CO -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| CLN+1M Curncy | COP NDF 1M | FX NDF | Daily, FX rate |
| CLSWIB1 Curncy | COP 1Y Swap | Rates | Daily, rate % |
| CLSWIB2 Curncy | COP 2Y Swap | Rates | Daily, rate % |
| CLSWIB3 Curncy | COP 3Y Swap | Rates | Daily, rate % |
| CLSWIB5 Curncy | COP 5Y Swap | Rates | Daily, rate % |
| CLSWIB10 Curncy | COP 10Y Swap | Rates | Daily, rate % |
| GTUSDCO10YR Corp | Colombia USD Sovereign 10Y | Govt Yield | Daily, yield % |
| BV100477 BVLI Index | Colombia Local 10Y (BVAL) | Govt Yield | Daily, yield % |
| COCUPIB Index | Colombia Quarterly Nominal GDP | GDP | Quarterly, COP bn |
| COCIPIBY Index | Colombia Real GDP YoY | GDP | Quarterly, % YoY |

### CL -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| CHN+1M Curncy | CLP NDF 1M | FX NDF | Daily, FX rate |
| CHSWP1 Curncy | CLP 1Y Swap | Rates | Daily, rate % |
| CHSWP2 Curncy | CLP 2Y Swap | Rates | Daily, rate % |
| CHSWP3 Curncy | CLP 3Y Swap | Rates | Daily, rate % |
| CHSWP5 Curncy | CLP 5Y Swap | Rates | Daily, rate % |
| CHSWP10 Curncy | CLP 10Y Swap | Rates | Daily, rate % |
| GTUSDCL10YR Corp | Chile USD Sovereign 10Y | Govt Yield | Daily, yield % |
| CLGB2Y Index | Chile Local 2Y Yield | Govt Yield | Daily, yield % |
| CLGB5Y Index | Chile Local 5Y Yield | Govt Yield | Daily, yield % |
| CLGB10Y Index | Chile Local 10Y Yield | Govt Yield | Daily, yield % |
| CLGB30Y Index | Chile Local 30Y Yield | Govt Yield | Daily, yield % |
| CLGDCURR Index | Chile Quarterly Nominal GDP | GDP | Quarterly, CLP bn |
| CLGDPNA% Index | Chile Real GDP YoY | GDP | Quarterly, % YoY |

### BR -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IBOV Index | Bovespa | Equity Index | Daily, index points |
| BCN+1M Curncy | BRL NDF 1M | FX NDF | Daily, FX rate |
| BCSFUPDV BLP Curncy | BRL 5Y Swap | Rates | Daily, rate % |
| GTUSDBR10YR Corp | Brazil USD Sovereign 10Y | Govt Yield | Daily, yield % |
| GTBRL2YR Corp | Brazil Local 2Y | Govt Yield | Daily, yield % |
| GTBRL5YR Corp | Brazil Local 5Y | Govt Yield | Daily, yield % |
| GTBRL10YR Corp | Brazil Local 10Y | Govt Yield | Daily, yield % |
| GTBRL30YR Corp | Brazil Local 30Y | Govt Yield | Daily, yield % |
| BZGDGDPQ Index | Brazil Quarterly Nominal GDP | GDP | Quarterly, BRL bn |
| BZGDYOY% Index | Brazil Real GDP YoY | GDP | Quarterly, % YoY |

### PE -- Market

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| GTUSDPE10YR Corp | Peru USD Sovereign 10Y | Govt Yield | Daily, yield % |
| BV100995 BVLI Index | Peru Local 10Y (BVAL) | Govt Yield | Daily, yield % |
| PRSRGDP Index | Peru Quarterly Nominal GDP | GDP | Quarterly, PEN bn |
| PRSCYOY Index | Peru Real GDP YoY | GDP | Quarterly, % YoY |

---

## Global / Cross-Region

### Commodities

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| BCOM Index | Bloomberg Commodity Index | Comdty Index | Daily, index level |
| CO1 Comdty | Brent Crude | Comdty | Daily, USD/bbl |
| CL1 Comdty | WTI Crude | Comdty | Daily, USD/bbl |
| SCO1 Comdty | Iron Ore | Comdty | Daily, USD/ton |
| XB1 COMB Comdty | Gasoline | Comdty | Daily, USD/gal |
| XAU Curncy | Gold | Precious Metal | Daily, USD/oz |
| XAUUSD Curncy | Gold (vs USD) | Precious Metal | Daily, USD/oz |
| XAG Curncy | Silver | Precious Metal | Daily, USD/oz |
| XPT Curncy | Platinum | Precious Metal | Daily, USD/oz |
| HG1 Comdty | Copper (COMEX) | Comdty | Daily, USD/lb |
| LP1 Comdty | LME Copper | Comdty | Daily, USD/ton |
| CU1 Comdty | Shanghai Copper | Comdty | Daily, CNY/ton |
| W A Comdty | Wheat | Comdty | Daily, USc/bu |
| S A Comdty | Soybean | Comdty | Daily, USc/bu |
| C A Comdty | Corn | Comdty | Daily, USc/bu |
| CRB CMDT Index | CRB Commodity Index | Comdty Index | Daily, index level |
| CRB RIND Index | CRB Raw Industrials | Comdty Index | Daily, index level |
| LMEX Index | LME Metal Index | Comdty Index | Daily, index level |
| SPGSCI Index | S&P GSCI | Comdty Index | Daily, index level |

### Crypto

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| XBTUSD BGN Curncy | Bitcoin | Crypto | Daily, USD |

### Global Equity / EM Indices

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| MXWO Index | MSCI World | Equity Index | Daily, index points |
| MXEF Index | MSCI EM | Equity Index | Daily, index points |
| EMFXDBE Index | EM FX Spot Index | FX Index | Daily, index level |
| ASIADOLR Index | Bloomberg Asia Dollar Index | FX Index | Daily, index level |

### Factor Indices

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| MSZZGRVL Index | MS Growth vs Value | Factor | Daily, index level |
| GSPUCYDE Index | GS US Cyclicals vs Defensives | Factor | Daily, index level |
| DBCAUCTA Index | DB X-Asset CTA Performance | Factor | Daily, index level |
| SPRP10T Index | Risk Parity Performance | Factor | Daily, index level |
| BXIIRCUE Index | Barclays Rate Carry Factor | Factor | Daily, index level |
| BXIIRMUE Index | Barclays Rate Momentum Factor | Factor | Daily, index level |
| BXIIRVUE Index | Barclays Rate Value Factor | Factor | Daily, index level |
| JMFXTNCR Index | JPM FX Carry Factor | Factor | Daily, index level |
| JMFXMEL4 Index | JPM FX Momentum Factor | Factor | Daily, index level |
| JMFXTNVR Index | JPM FX Value Factor | Factor | Daily, index level |
| BGSFXC Index | Bloomberg FX Carry Index | Factor | Daily, index level |
| UISFC1UE Index | UBS FX Carry Strategy | Factor | Daily, index level |

### Gold ETFs (Fund Flow Tracking)

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| GLD US Equity | SPDR Gold Shares | ETF | Daily, USD/share |
| IAU US Equity | iShares Gold Trust | ETF | Daily, USD/share |
| IGLN LN Equity | iShares Physical Gold | ETF | Daily, GBP/share |
| GLDM US Equity | SPDR Gold MiniShares | ETF | Daily, USD/share |
| SGLD LN Equity | Invesco Physical Gold | ETF | Daily, GBP/share |
| SGOL US Equity | abrdn Physical Gold Shares | ETF | Daily, USD/share |
| GOLD AU Equity | Global X Physical Gold | ETF | Daily, AUD/share |

### REER Indices

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| CTTWBRUS Index | US REER | FX | Daily, index level |
| CTTWBREU Index | EU REER | FX | Daily, index level |
| CTTWBRGB Index | UK REER | FX | Daily, index level |
| CTTWBRJP Index | Japan REER | FX | Daily, index level |
| CTTWBRAU Index | Australia REER | FX | Daily, index level |
| CTTWBRCN Index | China REER | FX | Daily, index level |
| CTTWBRIN Index | India REER | FX | Daily, index level |
| CTTWBRKR Index | Korea REER | FX | Daily, index level |
| CTTWBRID Index | Indonesia REER | FX | Daily, index level |
| CTTWBRTW Index | Taiwan REER | FX | Daily, index level |
| CTTWBRTH Index | Thailand REER | FX | Daily, index level |
| CTTWBRMY Index | Malaysia REER | FX | Daily, index level |
| CTTWBRSG Index | Singapore REER | FX | Daily, index level |
| CTTWBRPH Index | Philippines REER | FX | Daily, index level |
| CTTWBRVN Index | Vietnam REER | FX | Daily, index level |
| CTTWBRTR Index | Turkey REER | FX | Daily, index level |
| CTTWBRMX Index | Mexico REER | FX | Daily, index level |
| CTTWBRZA Index | South Africa REER | FX | Daily, index level |
| CTTWBRIL Index | Israel REER | FX | Daily, index level |
| CTTWBRBR Index | Brazil REER | FX | Daily, index level |
| CTTWBRCL Index | Chile REER | FX | Daily, index level |

### GS EM Consensus & Macro Tickers

#### Country Code Reference

| Country | CCY | 2-Letter Code (CC) | 3-Letter IMF Code (CCC) |
|---------|-----|---------------------|-------------------------|
| China | CNY | CN | CHN |
| India | INR | IN | IND |
| Korea | KRW | KR | KOR |
| Malaysia | MYR | MY | MYS |
| Thailand | THB | TH | THA |
| Indonesia | IDR | ID | IDN |
| Philippines | PHP | PH | PHL |
| Poland | PLN | PL | POL |
| Czech Republic | CZK | CZ | CZE |
| Hungary | HUF | HU | HUN |
| Romania | RON | RO | ROM |
| Israel | ILS | IL | ISR |
| South Africa | ZAR | ZA | ZAF |
| Mexico | MXN | MX | MEX |
| Colombia | COP | CO | COL |
| Chile | CLP | CL | CHL |
| Brazil | BRL | BR | BRA |
| Peru | PEN | PE | PER |

#### Ticker Patterns with Worked Examples

| Pattern | Description | Example Ticker | Example Meaning |
|---------|-------------|----------------|-----------------|
| `ECGD{CC} {QqYy} Index` | GDP consensus forecast per quarter | `ECGDMX 1Q26 Index` | Mexico GDP consensus for Q1 2026 |
| `ECPI{CC} {QqYy} Index` | CPI consensus forecast per quarter | `ECPIBR 4Q25 Index` | Brazil CPI consensus for Q4 2025 |
| `ECBB{CC} {Yy} Index` | Budget balance consensus | `ECBBZA 26 Index` | South Africa budget balance consensus 2026 |
| `IINE{CCC} Index` | IMF 4Y-ahead CPI forecast | `IINEMEX Index` | Mexico IMF 4Y-ahead CPI |
| `EHCA{CC} Index` | Current account % GDP | `EHCAKR Index` | Korea current account % GDP |
| `IGS%{CCC} Index` | Govt debt % GDP | `IGS%BRA Index` | Brazil govt debt % GDP |
| `BELT{CC}PG Index` | Potential growth | `BELTINPG Index` | India potential growth |
| `IGN${CCC} Index` | Annual nominal GDP | `IGN$IND Index` | India annual nominal GDP |
| `NIIP{code} Index` | Net intl investment position | `NIIPBRA Index` | Brazil NIIP % GDP |

#### Per-Country GDP & CPI Consensus Tickers (examples)

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| ECGDCN 1Q26 Index | China GDP consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECGDIN 1Q26 Index | India GDP consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECGDKR 1Q26 Index | Korea GDP consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECGDMX 1Q26 Index | Mexico GDP consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECGDBR 1Q26 Index | Brazil GDP consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECGDZA 1Q26 Index | South Africa GDP consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECPICN 1Q26 Index | China CPI consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECPIIN 1Q26 Index | India CPI consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECPIKR 1Q26 Index | Korea CPI consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECPIMX 1Q26 Index | Mexico CPI consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECPIBR 1Q26 Index | Brazil CPI consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| ECPIZA 1Q26 Index | South Africa CPI consensus Q1 2026 | Econ Consensus | Quarterly, % YoY |
| IINECHN Index | China IMF 4Y CPI | Econ Consensus | Annual, % |
| IINEIND Index | India IMF 4Y CPI | Econ Consensus | Annual, % |
| IINEKOR Index | Korea IMF 4Y CPI | Econ Consensus | Annual, % |
| IINEMEX Index | Mexico IMF 4Y CPI | Econ Consensus | Annual, % |
| IINEBRA Index | Brazil IMF 4Y CPI | Econ Consensus | Annual, % |
| IINEZAF Index | South Africa IMF 4Y CPI | Econ Consensus | Annual, % |
| EHCACN Index | China Current Account %GDP | Econ Macro | Annual, % GDP |
| EHCAIN Index | India Current Account %GDP | Econ Macro | Annual, % GDP |
| EHCAKR Index | Korea Current Account %GDP | Econ Macro | Annual, % GDP |
| EHCAMX Index | Mexico Current Account %GDP | Econ Macro | Annual, % GDP |
| EHCABR Index | Brazil Current Account %GDP | Econ Macro | Annual, % GDP |
| EHCAZA Index | South Africa Current Account %GDP | Econ Macro | Annual, % GDP |
| IGS%CHN Index | China Govt Debt %GDP | Econ Macro | Annual, % GDP |
| IGS%IND Index | India Govt Debt %GDP | Econ Macro | Annual, % GDP |
| IGS%BRA Index | Brazil Govt Debt %GDP | Econ Macro | Annual, % GDP |
| IGS%MEX Index | Mexico Govt Debt %GDP | Econ Macro | Annual, % GDP |
| IGS%ZAF Index | South Africa Govt Debt %GDP | Econ Macro | Annual, % GDP |
| BELTINPG Index | India Potential Growth | Econ Macro | Annual, % |
| BELTKRPG Index | Korea Potential Growth | Econ Macro | Annual, % |
| BELTCNPG Index | China Potential Growth | Econ Macro | Annual, % |
| BELTMXPG Index | Mexico Potential Growth | Econ Macro | Annual, % |
| BELTBRPG Index | Brazil Potential Growth | Econ Macro | Annual, % |
| IGN$IND Index | India Annual Nominal GDP | Econ Macro | Annual, USD mn |
| IGN$CHN Index | China Annual Nominal GDP | Econ Macro | Annual, USD mn |
| IGN$BRA Index | Brazil Annual Nominal GDP | Econ Macro | Annual, USD mn |
| IGN$MEX Index | Mexico Annual Nominal GDP | Econ Macro | Annual, USD mn |
| NIIPBRA Index | Brazil NIIP %GDP | Econ Macro | Annual, % GDP |
| NIIPIND Index | India NIIP %GDP | Econ Macro | Annual, % GDP |
| NIIPZAF Index | South Africa NIIP %GDP | Econ Macro | Annual, % GDP |

### Shipping/Trade

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| BDSRUSP Index | US Dry Bulk Shipping | Trade | Daily, index |
| BDSRCNP Index | China Dry Bulk Shipping | Trade | Daily, index |
| BDSRDEP Index | Germany Dry Bulk Shipping | Trade | Daily, index |
| BDSRINP Index | India Dry Bulk Shipping | Trade | Daily, index |
| BDSRKRP Index | Korea Dry Bulk Shipping | Trade | Daily, index |
| WSURASIP Index | Westpac Surprise Index (Asia) | Econ Surprise | Daily, index |

### Asia Rates Comparisons -- 1Y

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRSWNI1 Curncy | INR NDOIS 1Y | Rates | Daily, rate % |
| KWSWNI1 Curncy | KRW ND IRS 1Y | Rates | Daily, rate % |
| CCSWNI1 Curncy | CNY ND IRS 1Y | Rates | Daily, rate % |
| TBSWH1 Curncy | THB Onshore IRS 1Y | Rates | Daily, rate % |
| SDSOA1 Curncy | SGD SORA OIS 1Y | Rates | Daily, rate % |
| PPUSNI1 Curncy | PHP ND IRS 1Y | Rates | Daily, rate % |
| GTIDR1Y Govt | Indonesia Govt 1Y Yield | Govt Yield | Daily, yield % |
| MRSWNI1 Curncy | MYR ND IRS 1Y | Rates | Daily, rate % |
| JYSO1 Curncy | JPY OIS 1Y | Rates | Daily, rate % |

### Asia Rates Comparisons -- 5Y

| Ticker | Description | Asset Type | Units / Notes |
|--------|-------------|------------|---------------|
| IRSWNI5 Curncy | INR NDOIS 5Y | Rates | Daily, rate % |
| KWSWO5 Curncy | KRW Onshore IRS 5Y | Rates | Daily, rate % |
| CCSWNI5 Curncy | CNY ND IRS 5Y | Rates | Daily, rate % |
| TBSWH5 Curncy | THB Onshore IRS 5Y | Rates | Daily, rate % |
| SDSOA5 Curncy | SGD SORA OIS 5Y | Rates | Daily, rate % |
| PPUSNI5 Curncy | PHP ND IRS 5Y | Rates | Daily, rate % |
| IHSWAP5 Curncy | IDR ND CCS 5Y | Rates | Daily, rate % |
| MRSWNI5 Curncy | MYR ND IRS 5Y | Rates | Daily, rate % |
| JYSO5 Curncy | JPY OIS 5Y | Rates | Daily, rate % |

### Bloomberg Pricing Source Suffixes

Tickers can be appended with a pricing source suffix to specify a particular data snapshot time or pricing source. The base ticker (without suffix) is the canonical form used in this glossary. Common suffixes:

| Suffix | Description | Typical Use |
|--------|-------------|-------------|
| `BGN` | Bloomberg Generic (composite) | Default composite pricing |
| `BGNT` | Bloomberg Generic Tokyo Close | Snapshot at Tokyo close (15:00 JST / 14:00 SGT) |
| `CMPT` | Composite (specific time) | Composite pricing at a fixed time |
| `BLC` | Bloomberg Calculated | Derived/calculated rates (e.g., forwards) |
| `BLC3` | Bloomberg Calculated (variant 3) | Par rate curves |
| `BVAL` | Bloomberg Valuation | Bloomberg-evaluated pricing (bonds) |

**Example:** `IRSWNI5 Curncy` is the canonical NDOIS 5Y ticker. `IRSWNI5 BGNT Curncy` is the same rate snapshotted at Tokyo close. Both refer to the same instrument; the suffix only controls the pricing time.

---

# Part 2: Bloomberg Data Pulling Guide

All Bloomberg access requires a Bloomberg Terminal running on the same machine. No API keys needed -- libraries connect via the local `blpapi` C library automatically.

## Method 1: xbbg `blp.bdh()` -- Historical Daily Data

The primary method used across 22+ files.

### Function Signature

```python
from xbbg import blp

df = blp.bdh(
    tickers,             # str or list[str] -- Bloomberg tickers
    flds,                # str or list[str] -- Bloomberg field names
    start_date,          # str "YYYY-MM-DD" or datetime
    end_date,            # str "YYYY-MM-DD" or datetime
    Per='D',             # Periodicity: 'D','W','M','Q','Y'
    Fill='P',            # Fill: 'P' (previous), 'NA'
    Currency=None,       # Currency override, e.g. "USD"
    CshAdjNormal=True,   # Adjust for dividends (equities)
    PRICING_SOURCE=None, # e.g. "BVAL"
    **kwargs             # Additional overrides (e.g. CDR="SE")
)
```

### Returns

`pd.DataFrame` with `DatetimeIndex` and `MultiIndex` columns (level 0 = ticker, level 1 = field).

### Parameter Table

| Param | Type | Description | Example |
|-------|------|-------------|---------|
| `tickers` | `str` or `list[str]` | Bloomberg security identifiers | `"USGG10YR Index"`, `["ES1 Index", "TY1 Comdty"]` |
| `flds` | `str` or `list[str]` | Bloomberg field names | `"PX_LAST"`, `["PX_LAST", "PX_VOLUME"]` |
| `start_date` | `str` or `datetime` | Start of date range | `"2015-01-01"` |
| `end_date` | `str` or `datetime` | End of date range | `"2024-12-31"` |
| `Per` | `str` | Periodicity override | `'D'`, `'W'`, `'M'`, `'Q'`, `'Y'` |
| `Fill` | `str` | Fill method for missing values | `'P'` (previous), `'NA'` |
| `Currency` | `str` | Currency override | `"USD"` |
| `PRICING_SOURCE` | `str` | Pricing source | `"BVAL"` |
| `CshAdjNormal` | `bool` | Cash dividend adjustment | `True` |
| `CDR` | `str` (via kwargs) | Calendar override | `"SE"` (Seoul) |

### Working Code Examples

**Basic usage** -- `scanner_code/CTA_MomentumCalculator.py:22`:
```python
data = blp.bdh(All_list, "PX_LAST", START_DATE, END_DATE).droplevel(1, axis=1)
```

**With Currency override** -- `scanner_code/Models.py:41-48`:
```python
def get_bdh_df(tickers, start_date, end_date, Curr=None):
    if Curr is not None:
        df = blp.bdh(tickers, "PX_LAST", start_date, end_date, Currency=Curr)
    else:
        df = blp.bdh(tickers, "PX_LAST", start_date, end_date)
    df = df.droplevel(1, axis=1)
    df.index = pd.to_datetime(df.index)
    return df
```

**Quarterly periodicity** -- `charts/ChartUpdate/charts_updater_all.py:2548`:
```python
business = blp.bdh(["CEOCINDX Index", "CPFTYOY Index"], "PX_LAST",
                    START_DATE, END_DATE, Per='Q').droplevel(1, axis=1)
```

**Monthly periodicity** -- `charts/ChartUpdate/charts_updater_all.py:2332`:
```python
gdp = blp.bdh(["GDNSCHWN Index", "UKGRABMI Index", ...], "PX_LAST",
               START_DATE, END_DATE, Per='M').droplevel(1, axis=1)
```

**BVAL pricing source** -- `scanner_code/KtbFutRVCalculator.py:204`:
```python
bond_df = blp.bdh(f"{bond} Corp", "PX_CLEAN_MID", start_date, end_date,
                   PRICING_SOURCE="BVAL")
```

**Seoul calendar override** -- `scanner_code/KtbFutRVCalculator.py:218`:
```python
repo_rates = blp.bdh(REPO_TICKERS, "PX_LAST",
                      datetime.strptime(START_DATE, "%Y/%m/%d") - timedelta(30),
                      END_DATE, {"CDR": "SE"})
```

**Bond yields** -- `scanner_code/BondScanner.py:94`:
```python
yields = blp.bdh(tickers, "YLD_YTM_MID", start_date=START_DATE,
                  end_date=END_DATE).droplevel(1, axis=1)
```

**Fund flows** -- `charts/ChartUpdate/charts_updater_all.py:6446`:
```python
raw_flows = blp.bdh(tickers=TICKERS, flds='FUND_FLOW',
                     start_date=START_DATE, end_date=END_DATE)
```

**BloombergAdapter wrapper** -- `macrobt/macrobt/data/adapters/bloomberg_like.py:31-51`:
```python
class BloombergAdapter:
    def fetch(self, tickers, fields, start_date, end_date, **kwargs):
        df = _blp.bdh(tickers=tickers, flds=fields,
                       start_date=start_date, end_date=end_date, **kwargs)
        return df

    def fetch_close(self, tickers, start_date, end_date):
        df = self.fetch(tickers, ["PX_LAST"], start_date, end_date)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
```

### MultiIndex Column Handling Patterns

```python
# Pattern 1: Drop field level (single field, multiple tickers)
df = df.droplevel(1, axis=1)

# Pattern 2: Drop ticker level (single ticker)
df = df.droplevel(0, axis=1)

# Pattern 3: Cross-section by field
df = raw.xs("PX_LAST", level=-1, axis=1)

# Pattern 4: Get level values
df.columns = df.columns.get_level_values(0)

# Pattern 5: Check and flatten
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.droplevel(1)
```

---

## Method 2: xbbg `blp.bdp()` -- Point-in-Time / Reference Data

Used across 11+ files for current snapshots.

### Function Signature

```python
df = blp.bdp(
    tickers,    # str or list[str]
    flds,       # str or list[str]
)
```

### Returns

`pd.DataFrame` with tickers as index, fields as columns (lowercase column names).

### Working Code Examples

**Bond reference data** -- `scanner_code/BondScanner.py:93`:
```python
bond_dets = blp.bdp(tickers, ["Name", 'Maturity', "DUR_ADJ_MID"])
```

**Single field** -- `charts/ChartUpdate/getter_econ.py:24`:
```python
fwddata = blp.bdp(fwd_ticker, "PX_LAST")
```

**Multiple fields** -- `charts/CapitalFlowsMonitor.py:44`:
```python
wtd_mtd_data = blp.bdp(wtd_mtd_tickers, flds=["PX_Last", "LAST_UPDATE_DT"])
```

**Cached scalar wrapper** -- `scanner_code/KtbFutRVCalculator.py:70-80`:
```python
@lru_cache(maxsize=128)
def bdp_scalar(security: str, field: str):
    """Fetch a single scalar field via bdp with caching."""
    df = blp.bdp(security, field)
    if df is None or df.empty:
        return None
    df = _drop_bbg_multiindex_cols(df)
    if field in df.columns:
        return df.iloc[0][field]
    return df.iloc[0].squeeze()
```

---

## Method 3: xbbg `blp.bds()` -- Bulk/Set Data

Used in 3 files for structured data retrieval.

### Function Signature

```python
df = blp.bds(
    tickers,        # str -- single Bloomberg ticker
    flds,           # str -- bulk field name
    ovrds=None,     # list of tuples for overrides
)
```

### Returns

`pd.DataFrame` with one row per element in the bulk data set.

### Working Code Examples

**Deliverable bonds for futures** -- `scanner_code/KtbFutRVCalculator.py:106-131`:
```python
def get_deliverable_isins(future_ticker: str):
    """Pull deliverable bond ISINs for a futures contract."""
    df = blp.bds(future_ticker, "FUT_DLVRBLE_BNDS_ISINS")
    if df is None or df.empty:
        return []
    df = _drop_bbg_multiindex_cols(df)
    isin_col = next((c for c in df.columns if "isin" in str(c).lower()), None)
    if isin_col is None:
        isin_col = df.columns[-1]
    raw = df[isin_col].dropna().astype(str).str.strip().tolist()
    seen = set()
    out = []
    for x in raw:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out
```

**Calendar non-settlement dates** -- `charts/ChartUpdate/charts_updater_all.py:6666-6679`:
```python
def bds_calendar_non_settlement(cal_code, start, end):
    ovrds = [
        ('SETTLEMENT_CALENDAR_CODE', cal_code),
        ('CALENDAR_START_DATE', start.strftime('%Y%m%d')),
        ('CALENDAR_END_DATE',   end.strftime('%Y%m%d')),
    ]
    df = blp.bds('USD Curncy', 'CALENDAR_NON_SETTLEMENT_DATES', ovrds=ovrds)
    if df is None or df.empty:
        return pd.DatetimeIndex([])
    col = next((c for c in df.columns
                if 'NON' in c.upper() and 'SETTLEMENT' in c.upper()), None)
    if not col:
        return pd.DatetimeIndex([])
    dates = pd.to_datetime(df[col], errors='coerce').dropna().unique()
    return pd.to_datetime(sorted(dates))
```

---

## Method 4: xbbg Intraday Bars (Raw `IntradayBarRequest`)

### Raw IntradayBarRequest -- `scanner_code/TradingSessionAnalysis.py:147-189`

```python
from xbbg.core import conn, process

req = process.create_request(service='//blp/refdata', request='IntradayBarRequest')
req.set('security', ticker)
req.set('eventType', event)           # "TRADE"
req.set('interval', int(interval))    # minutes
req.set('startDateTime', start_utc.strftime("%Y-%m-%dT%H:%M:%S"))
req.set('endDateTime', end_utc.strftime("%Y-%m-%dT%H:%M:%S"))
conn.send_request(request=req)
df = pd.DataFrame(process.rec_events(func=process.process_bar)).set_index('time')
```

---

## Method 5: tia.bbg -- Legacy Method

Used in 7 files. Older wrapper around Bloomberg API.

### Import Patterns

```python
import tia.bbg.datamgr as bbgdm          # BbgDataManager for historical/reference
from tia.bbg import LocalTerminal as LT   # For intraday bars
```

### tia.bbg `bdh()` -- Historical Data

File: `scanner_code/BbmDownloader.py:122-157` and `charts/ChartUpdate/bbgui.py:123-156`

```python
def bdh(series=[], flds=['PX_LAST'], startDate=None, endDate=None,
        interval='DAILY', mgr=bbgdm.BbgDataManager()):
    '''python version of bloomberg Excel BDH function.'''
    if type(interval) == int:
        data = get_Intraday(series, 'TRADE', startDate, endDate, interval)
    else:
        dataMgr = mgr[series]
        data = dataMgr.get_historical(flds, startDate, endDate, period=interval)
    return data
```

**Valid intervals:** `DAILY`, `WEEKLY`, `MONTHLY`, `QUARTERLY`, `SEMI-ANNUAL`, `YEARLY`, or integer (minutes for intraday)

### tia.bbg `bdp()` -- Reference Data

File: `scanner_code/BbmDownloader.py:160-170`

```python
def bdp(series, flds):
    """python version of bloomberg Excel BDP function"""
    mgr = bbgdm.BbgDataManager()
    dmgr = mgr[series]
    return dmgr[flds]
```

### tia.bbg `LT.get_intraday_bar()` -- Intraday Bars

File: `scanner_code/BbmDownloader.py:108-120`

```python
def get_Intraday(series, event, startTime, endTime, interval):
    data = []
    for i in range(0, len(series)):
        temp = LT.get_intraday_bar(series[i], event, startTime, endTime,
                                    interval).as_frame()
        temp = temp.set_index('time')
        temp = temp['close'].to_frame()
        temp = temp.rename(columns={'close': series[i]})
        if i == 0:
            data = temp
        else:
            data = pd.concat([data, temp], axis=1)
    data = data.interpolate(limit=1, limit_direction='both')
    return data
```

### tia.bbg `mgr.get_attributes()` -- Security Attributes

File: `scanner_code/MmsScanner_utils.py:31-37`

```python
def checkBondName(bondName, mgr=bbgdm.BbgDataManager()):
    try:
        x = mgr[bondName].get_attributes('NAME')
    except:
        x = "SecException"
    return x
```

---

## Complete Bloomberg Field Reference

### Price/Rate Fields

| Field | Description | Used In |
|-------|-------------|---------|
| `PX_LAST` | Last price / rate | Almost all files |
| `PX_CLEAN_MID` | Clean mid price (bonds) | KtbFutRVCalculator.py |
| `YLD_YTM_MID` | Yield to maturity mid | BondScanner.py, KtbFutRVCalculator.py, MmsScanner_utils.py |
| `FUND_FLOW` | Fund flow data | charts_updater_all.py |
| `LAST PRICE` | Last price (alternative) | charts_updater_all.py |

### Reference/Metadata Fields

| Field | Description | Used In |
|-------|-------------|---------|
| `NAME` | Security name | BondScanner.py, MmsScanner_utils.py |
| `Maturity` | Bond maturity date | BondScanner.py |
| `DUR_ADJ_MID` | Modified duration mid | BondScanner.py |
| `CRNCY` | Currency | charts_updater_all.py |
| `LAST_UPDATE_DT` | Last update date | CapitalFlowsMonitor.py |
| `FWD_SCALE` | Forward point scale | XMTreasuryScanner.py |

### Futures-Specific Fields

| Field | Description | Used In |
|-------|-------------|---------|
| `FUT_DLVRBLE_BNDS_ISINS` | Deliverable bond ISINs (bulk) | KtbFutRVCalculator.py |
| `FUT_CUR_GEN_TICKER` | Current generic ticker | KtbFutRVCalculator.py |
| `FUT_CUR_TICKER` | Current ticker | KtbFutRVCalculator.py |
| `FUTURES_VALUATION_DATE` | Valuation/settlement date | KtbFutRVCalculator.py |
| `FUT_FIRST_TRADE_DT` | First trade date | KtbFutRVCalculator.py |

### Override Parameters

| Override | Description | Example |
|----------|-------------|---------|
| `Per` | Periodicity | `'D'`, `'W'`, `'M'`, `'Q'`, `'Y'` |
| `Fill` | Fill method | `'P'` (previous) |
| `Currency` | Currency conversion | `"USD"` |
| `PRICING_SOURCE` | Data source | `"BVAL"` |
| `CDR` | Calendar | `"SE"` (Seoul) |
| `CshAdjNormal` | Cash dividend adjustment | `True` |
| `SETTLEMENT_CALENDAR_CODE` | Settlement calendar (bds) | `'CDR_US'` |
| `CALENDAR_START_DATE` | Calendar range start (bds) | `'20250101'` |
| `CALENDAR_END_DATE` | Calendar range end (bds) | `'20251231'` |

---

# Part 3: TSDB (via PyQCL) Documentation

TSDB is the internal time series database accessed via the `pyQCL` library. It stores IRS rates, FX forwards, and FX vol data with standardized ticker conventions.

## TSDB Functions

### `qcl.tsdb_load(tickers)`

Load time series from TSDB. Returns an opaque object ID.

```python
import pyQCL as qcl

obj_id = qcl.tsdb_load(tickers)  # tickers: list[str] or str
df = qcl.tsdb_get_dataframe(obj_id)
```

**Example** -- `scanner_code/BasisDataGetter_APAC.py:121`:
```python
usdir = [f"usd_irs_sofr_{t}@rate" for t in usdir_tenor]
obj_id = qcl.tsdb_load(usdir)
usdir_df = qcl.tsdb_get_dataframe(obj_id)
```

### `qcl.tsdb_load_between_dates(tickers, snapshot, start, end)`

Load time series between specific dates.

| Param | Type | Description |
|-------|------|-------------|
| `tickers` | `list[str]` or `str` | TSDB ticker(s) |
| `snapshot` | `None` | Always None in this codebase |
| `start_date` | `str` | Start date as `'YYYY/MM/DD'` |
| `end_date` | `str` | End date as `'YYYY/MM/DD'` |

**Example** -- `scanner_code/Models.py:60`:
```python
obj_id_bt = qcl.tsdb_load_between_dates(
    ticker, None,
    (Today + BDay(-365 * 5)).strftime('%Y/%m/%d'),
    Today.strftime('%Y/%m/%d')
)
tsdb_df = qcl.tsdb_get_dataframe(obj_id_bt) * 10000  # convert to bp
```

**Example** -- `scanner_code/VolScanner - TSDB.py:155`:
```python
obj_id_bt = qcl.tsdb_load_between_dates(ticker_list, None, start, end)
metadata = qcl.tsdb_get_dataframe(obj_id_bt)
```

### `qcl.tsdb_get_dataframe(obj_id)`

Convert a TSDB object handle into a pandas DataFrame.

**Returns:** `pd.DataFrame` with `DatetimeIndex`; columns are lowercase ticker strings.

**Important notes:**
- IRS rates are returned as decimals (e.g., 0.05 for 5%). Many scripts multiply by `10000` to convert to basis points.
- Date columns (SettleDate, SpotDate) are returned as Excel serial numbers. Convert with:
  ```python
  pd.to_datetime(col, unit='D', origin=pd.Timestamp('1899-12-30'))
  ```

---

## TSDB Ticker Formats

### IRS Curve Tickers

**Pattern:** `{CCY}_IRS_{CURVE}_{TENOR}@{attribute}`

| Component | Description | Examples |
|-----------|-------------|----------|
| `CCY` | Currency code | `USD`, `EUR`, `KRW`, `INR` |
| `CURVE` | Curve/index name | `SOFR`, `6M`, `3M`, `OIS`, `TONAROIS` |
| `TENOR` | Maturity tenor | `5Y` (outright), `2Y_1Y` (forward-start) |
| `attribute` | Data field | `rate`, `c1m` (1m carry), `c3m` (3m carry) |

**Examples:**
```
USD_IRS_SOFR_5Y@rate           # 5Y spot-start SOFR swap rate
USD_IRS_SOFR_2Y_1Y@rate        # 1Y rate, 2Y forward
USD_IRS_SOFR_5Y@c1m            # 5Y SOFR 1-month carry
EUR_IRS_6M_10Y@rate            # 10Y EUR 6M swap rate
KRW_IRS_3M_3Y@rate             # 3Y KRW 3M swap rate
```

**Special rules:**
- **AUD**: Curve switches from `6M` to `3M` for tenors <= 3Y
- **CZK/PLN/HUF**: Curve switches from `6M` to `3M` for total tenor <= 1Y

### FX Forward Tickers

**Pattern:** `USD{CCY}_FX_{TENOR}@{attribute}`

| Component | Description | Examples |
|-----------|-------------|----------|
| `CCY` | Counter-currency | `KRW`, `INR`, `CNH` |
| `TENOR` | Forward tenor | `TDSP` (spot), `1M`, `3M`, `1Y` |
| `attribute` | Data field | `rate`, `SettleDate`, `SpotDate` |

**Examples:**
```
USDINR_FX_3M@rate              # 3M USD/INR forward rate
USDINR_FX_3M@SettleDate        # 3M forward settle date
USDINR_FX_TDSP@SpotDate        # Spot date
USDINR_FX_TDSP@rate            # Spot rate
```

### FX Vol Tickers

**Pattern:** `{PAIR}_FXO_{TENOR}_A@{field}`

| Component | Description | Examples |
|-----------|-------------|----------|
| `PAIR` | Currency pair | `USDKRW`, `EURUSD`, `AUDJPY` |
| `TENOR` | Option tenor | `1W`, `1M`, `3M`, `6M`, `1Y`, `2Y` |
| `field` | Vol metric | `atmvol`, `fly25d`, `fly10d`, `rr25d`, `rr10d`, `strike` |

**Examples:**
```
USDKRW_FXO_3M_A@atmvol        # 3M USDKRW ATM vol
USDKRW_FXO_1Y_A@rr25d         # 1Y USDKRW 25-delta risk reversal
USDKRW_FXO_6M_A@fly25d        # 6M USDKRW 25-delta butterfly
```

### Asia-Fix Prefix

**Pattern:** `sg1415~{TICKER}`

Used for USD rates at Singapore 14:15 fixing time.

```
sg1415~USD_IRS_SOFR_5Y@rate    # SOFR 5Y at SG 14:15 fix
sg1415~USD_IRS_SOFR_10Y@rate   # SOFR 10Y at SG 14:15 fix
```

Used in: `pull_sofr_asia.py`, `CurveScanner - XM.py`

---

## Complete Curve Name Mapping (20 Currencies)

### From `CurveScanner_Utils.py`

| CCY | Curve Name | Notes |
|-----|-----------|-------|
| USD | SOFR | |
| EUR | 6M | |
| JPY | TONAROIS | |
| AUD | 6M | Switches to 3M for tenors <= 3Y |
| NZD | 3M | |
| GBP | SONIA | |
| CAD | OIS | |
| CHF | SARON | |
| HKD | 3M | |
| SGD | SORA | |
| KRW | 3M | |
| CNY | 7D | |
| THB | THORON | |
| INR | OIS | |
| MYR | 3M | |
| TWD | 3M | |
| MXN | MXIBTIEF | |
| HUF | 6M | Switches to 3M for total tenor <= 1Y |
| CZK | 6M | Switches to 3M for total tenor <= 1Y |
| PLN | 6M | Switches to 3M for total tenor <= 1Y |

### From `BasisConfigure.py` -- APAC

| CCY | IR Prefix | Example Ticker |
|-----|----------|----------------|
| CNH | CNY_IRS_7D | `CNY_IRS_7D_3M@rate` |
| CNY | CNY_IRS_7D | `CNY_IRS_7D_1Y@rate` |
| HKD | HKD_IRS_3M | `HKD_IRS_3M_6M@rate` |
| INR | INR_IRS_OIS | `INR_IRS_OIS_2Y@rate` |
| KRW | KRW_IRS_3M | `KRW_IRS_3M_5Y@rate` |
| MYR | MYR_IRS_3M | `MYR_IRS_3M_3M@rate` |
| SGD | SGD_IRS_SORA | `SGD_IRS_SORA_1Y@rate` |
| THB | THB_IRS_THORON | `THB_IRS_THORON_6M@rate` |
| TWD | TWD_IRS_3M | `TWD_IRS_3M_2Y@rate` |

### From `BasisConfigure.py` -- CEEMEA/LatAm

| CCY | IR Prefix | Example Ticker |
|-----|----------|----------------|
| PLN | PLN_IRS_3M | `PLN_IRS_3M_1Y@rate` |
| CZK | CZK_IRS_3M | `CZK_IRS_3M_2Y@rate` |
| HUF | HUF_IRS_3M | `HUF_IRS_3M_6M@rate` |
| ILS | ILS_IRS_3M | `ILS_IRS_3M_3Y@rate` |
| ZAR | ZAR_IRS_3M | `ZAR_IRS_3M_5Y@rate` |
| TRY | TRY_IRS_TLREF | `TRY_IRS_TLREF_1Y@rate` |
| MXN | MXN_IRS_MXIBTIEF | `MXN_IRS_MXIBTIEF_3M@rate` |
| COP | COP_IRS_1D | `COP_IRS_1D_2Y@rate` |
| CLP | CLP_IRS_1D | `CLP_IRS_1D_1Y@rate` |

### From `MmsScanner_utils.py` -- IRS Convention Mapping

| CCY | Convention |
|-----|-----------|
| AUD | 3M (<=3.5Y) or 6M |
| NZD | 3M |
| EUR | 6M |
| GBP | OIS |
| USD | SOFR |
| CNY | 7D |
| HKD | 3M |
| MYR | 3M |
| INR | OIS |
| ILS | 3M |
| SGD | OIS |
| THB | OIS |
| ZAR | 3M |
| COP | OIS |
| MXN | OIS |
| default | OIS |

---

## Forward-Start Tenor Conventions

| Format | Meaning | Example |
|--------|---------|---------|
| `5Y` | 5Y spot-start | Outright 5Y rate |
| `2Y_1Y` | 1Y rate, starting in 2Y | Forward-start swap |
| `9M_1Y` | 1Y rate, starting in 9M | Used for 3m carry of 1Y_1Y |
| `3M_3M` | 3M FX swap, starting in 3M | Forward FX swap |

---

## Standard Tenor Lists

### BasisDataGetter Tenors (spot + forward)

```
spot_tenor: 1M, 2M, 3M, 4M, 5M, 6M, 7M, 8M, 9M, 10M, 11M, 1Y,
            13M, 14M, 15M, 18M, 21M, 2Y, 3Y
gap_1m:     1M_1M, 2M_1M, ..., 1Y_1M
gap_3m:     1M_3M, 2M_3M, ..., 18M_3M
gap_6m:     6M_6M, 1Y_6M, 18M_6M
gap_1y:     1Y_1Y, 2Y_1Y
```

### CurveScanner XMJ Tenors (cross-market)

```
xmkt_tenors:     1Y_1Y, 2Y_2Y, 5Y_5Y, 10Y_5Y, 15Y_5Y, 10Y_10Y, 20Y_10Y
xmkt_tenors_3m:  9M_1Y, 21M_2Y, 57M_5Y, 117M_5Y, 177M_5Y, 117M_10Y, 237M_10Y
```

### VolScanner Tenors

```
tsdb_tenor_list: 1W, 2W, 1M, 2M, 3M, 6M, 1Y, 2Y
```

---

## FX Vol Pairs Covered

```
G10:   EURUSD, GBPUSD, USDJPY, AUDUSD, NZDUSD, USDCAD, USDCHF,
       EURGBP, EURCHF, EURNOK, EURSEK, AUDJPY, AUDNZD
Asia:  USDKRW, USDCNH, USDHKD, USDIDR, USDINR, USDSGD, USDTWD, USDVND
CEEMEA: USDTRY, USDZAR, USDILS, EURPLN, EURHUF, EURCZK
LatAm: USDMXN, USDBRL, USDCLP, USDCOP
```

---

## BasisConfigure.py Reference Data

### Forward Points Scaling (`ccy_fwd_scale`)

| CCY | Scale |
|-----|-------|
| CNH, CNY, HKD, MYR, SGD, ILS, ZAR, MXN, CLP | 10000 |
| KRW, THB, HUF, COP | 100 |
| TWD, CZK | 1000 |
| TRY | 1 |

### FX Close Times (SGT)

| Market | Time (SGT) |
|--------|------------|
| Korea | 15:00 |
| India | 19:30 |
| Hong Kong, China, Taiwan, Singapore | 16:30 |
| Thailand | 17:30 |
| Malaysia | 17:00 |
| Israel | 21:30 |
| Poland | 23:00 |
| Hungary, Czechia, South Africa | 22:30 |
| Turkey | 22:00 |
| Mexico | 06:00 (T+1) |
| Colombia | 05:00 (T+1) |
| Chile | 04:00 (T+1) |

### Day-Count Conventions (`local_fx_dc`)

| Convention | Currencies |
|------------|------------|
| 360 | CNH, CNY, THB, TWD, CZK, HUF, TRY, MXN, COP, CLP |
| 365 | HKD, INR, KRW, MYR, SGD, PLN, ILS, ZAR |

---

## Files Using TSDB

| File | Functions Used |
|------|---------------|
| `CurveScanner_Utils.py` | Ticker construction (`get_tsdb_ticker()`) |
| `CurveScanner - Spread.py` | `tsdb_load_between_dates`, `tsdb_get_dataframe` |
| `CurveScanner - Fly.py` | `tsdb_load_between_dates`, `tsdb_get_dataframe` |
| `CurveScanner - XMJ.py` | `tsdb_load_between_dates`, `tsdb_get_dataframe` |
| `CurveScanner - XM.py` | `tsdb_load_between_dates`, `tsdb_get_dataframe` |
| `CurveScanner - UI.py` | `tsdb_load`, `tsdb_get_dataframe` |
| `VolScanner - TSDB.py` | `tsdb_load_between_dates`, `tsdb_get_dataframe` |
| `Models.py` | `tsdb_load_between_dates`, `tsdb_load`, `tsdb_get_dataframe` |
| `MmsScanner_utils.py` | `tsdb_load`, `tsdb_get_dataframe`, `qlib_market_settings` |
| `BasisScanner.py` | `tsdb_load`, `tsdb_get_dataframe` |
| `BasisDataGetter_APAC.py` | `tsdb_load`, `tsdb_get_dataframe` |
| `BasisDataGetter_Other.py` | `tsdb_load`, `tsdb_get_dataframe` |
| `basis_data_puller.py` | `qlib_mkt_make_hdl`, `qlib_pos_make_fxswap`, `rt_pos_time_series` |
| `BasisLive_Data.py` | `qlib_mkt_make_hdl`, `qlib_pos_make_fxswap`, `rt_pos_time_series` |
| `KtbFutRVCalculator.py` | `qlib_mkt_make_hdl`, `rt_create_position`, `qlib_bond_*` |
| `pull_sofr_asia.py` | `tsdb_load_between_dates`, `tsdb_get_dataframe` |

---

# Part 4: PyQCL Pricing/Risk Functions

## Market Environment Functions

### `qcl.qlib_mkt_get_live_ts()`

Returns current live market timestamp string.

```python
dt_live = qcl.qlib_mkt_get_live_ts()
```

### `qcl.qlib_mkt_make_hdl(date_time, scens=...)`

Create market environment handle for pricing.

| Param | Type | Description |
|-------|------|-------------|
| `date_time` | str or timestamp | Pricing date/time |
| `scens` | list or str | Scenario list |

**Scenario values:** `'UseBC'`, `'UseCB'`

```python
# Live market
scns = ['UseBC']
dt_live = qcl.qlib_mkt_get_live_ts()
envs = qcl.qlib_mkt_make_hdl(dt_live, scens=scns)

# Historical date
env = qcl.qlib_mkt_make_hdl(date_time="2025/01/15", scens="UseBC")
```

### `qcl.qlib_market_settings(dt)` (older API)

Used in `MmsScanner_utils.py` for older-style single-position pricing.

```python
dt = f"{START} {TIME}"
env = qcl.qlib_market_settings(dt)
```

---

## Position Creation Functions

### `qcl.rt_create_position(columns, values)`

Create an IRS position for pricing.

**Standard columns:** `["CAT", "CCY", "MAT", "START", "END", "COUPON", "NOMINAL", "DIRECTION"]`

| Field | Description | Examples |
|-------|-------------|----------|
| CAT | Instrument type | `"irs"` |
| CCY | Currency | `"KRW"`, `"USD"`, `"INR"` |
| MAT | Floating index | `"3M"`, `"OIS"`, `"SOFR"`, `"SORA"` |
| START | Start date/code | `"V"` (spot), `"3M"`, date string |
| END | End date/tenor | `"3M"`, `"1Y"`, `"10Y"`, date string |
| COUPON | Coupon type | `"a"` (auto/at-market) |
| NOMINAL | Notional | `"1000"` |
| DIRECTION | Pay/Receive | `"Pay"` |

```python
columns = ["CAT", "CCY", "MAT", "START", "END", "COUPON", "NOMINAL", "DIRECTION"]

# Spot-start 3M KRW IRS
pos = qcl.rt_create_position(columns,
      ("irs", "KRW", "3M", "V", "3M", "a", "1000", "Pay"))

# Forward-start: 1M start, 3M end
pos = qcl.rt_create_position(columns,
      ("irs", "KRW", "3M", "1M", "3M", "a", "1000", "Pay"))

# Date-based
pos = qcl.rt_create_position(columns,
      ("irs", "KRW", "3M", "2026/03/17", "3Y", "a", "1000", "Pay"))
```

### `qcl.qlib_pos_make_fxswap(ccy1, ccy2, start, end, coupon, ...)`

Create an FX swap position.

| Param | Description | Examples |
|-------|-------------|----------|
| ccy1 | Base currency | `"USD"` |
| ccy2 | Quote currency | `"KRW"`, `"INR"`, `"HKD"` |
| start | Near leg start | `"0d"` (spot), `"1M"`, `"3M"` |
| end | Far leg end | `"3M"`, `"1Y"`, `"2Y"` |
| coupon | Coupon type | `"a"` |
| p1-p5 | Other params | All `None` in this codebase |

```python
# Spot-start 3M FX swap
pos = qcl.qlib_pos_make_fxswap("USD", "KRW", "0d", "3M", "a",
                                None, None, None, None, None)

# Forward-start: 1M to 4M
pos = qcl.qlib_pos_make_fxswap("USD", "INR", "1M", "4M", "a",
                                None, None, None, None, None)
```

---

## Time Series & Pricing Functions

### `qcl.rt_pos_time_series(envs, positions, look_back, sample_time, no_risk, offset)`

Pull historical time series for positions. **Asynchronous** -- returns a handle.

| Param | Type | Description |
|-------|------|-------------|
| `envs` | env handle | From `qlib_mkt_make_hdl` |
| `positions` | list | Position list |
| `look_back` | str | Lookback: `"5d"`, `"90D"`, `"5y"`, `"10y"` |
| `sample_time` | str | Time of day: `"15:00:00"`, `"16:30:00"` |
| `no_risk` | bool | `True` to skip risk calc |
| `offset` | str or None | `"1D"` or `None` |

```python
handle = qcl.rt_pos_time_series(envs, positions,
                                look_back="5y",
                                sample_time="15:00:00",
                                no_risk=True, offset=None)
```

### `qcl.rt_pos_price(envs, positions)`

Get live/current prices for positions. **Asynchronous**.

```python
handle = qcl.rt_pos_price(envs, [pos])
```

---

## Status & Result Functions

### `qcl.rt_nr_status(handle)`

Check async operation status. Poll until `"Complete"` appears.

```python
while "Complete" not in qcl.rt_nr_status(handle):
    time.sleep(10)
```

### `qcl.rt_nr_dump(handle)`

Get results from completed async operation as `pd.DataFrame`.

**FX swap columns:** `PUB.TIME`, `NEAR.FX`, `FAR.FX`, `FWD.POINTS`, `POINTS.PER.DAY`, `DAYS.DIFF`, `START_DT`, `END_DT`

**IRS columns:** `ENV.DT`, `RATE.MODEL`

```python
raw = qcl.rt_nr_dump(handle)
```

### `qcl.rt_nr_inspect(result, pos, dt)` (older API)

Inspect detailed results; returns JSON-parseable string.

```python
inspect = qcl.rt_nr_inspect(result, pos, dt)
df = pd.read_json(inspect)
rate = df.iloc[1].loc['RATE'] * 100  # extract rate in %
```

---

## Bond-Specific Functions

From `scanner_code/KtbFutRVCalculator.py`:

### `qcl.qlib_bond_settlement_date(env, bondISIN, date)`

Get bond settlement date for a given pricing date.

```python
spot_settlement_date = qcl.qlib_bond_settlement_date(
    env, bondISIN, pricing_date.strftime("%Y/%m/%d"))
```

### `qcl.qlib_bond_repo_fwd(env, bondISIN, spot_settle, fwd_settle, price, repo_rate)`

Calculate bond forward price with repo rate. Returns an async handle.

```python
fwd_rate = qcl.qlib_bond_repo_fwd(
    env, bondISIN, spot_settlement_date, fwd_settlement_date,
    bondpx/100, repo_rate)
df = qcl.rt_nr_dump(fwd_rate)
yield_val = df.loc[0, "YIELD"]
```

---

## Complete PyQCL Workflow Example

```python
import pyQCL as qcl
import time

# 1. Set up market environment
scns = ['UseBC']
dt_live = qcl.qlib_mkt_get_live_ts()
envs = qcl.qlib_mkt_make_hdl(dt_live, scens=scns)

# 2. Create positions
columns = ["CAT", "CCY", "MAT", "START", "END", "COUPON", "NOMINAL", "DIRECTION"]
irs_pos = qcl.rt_create_position(columns,
          ("irs", "KRW", "3M", "V", "5Y", "a", "1000", "Pay"))
fxs_pos = qcl.qlib_pos_make_fxswap("USD", "KRW", "0d", "3M", "a",
                                     None, None, None, None, None)

# 3. Pull time series (async)
handle = qcl.rt_pos_time_series(envs, [irs_pos, fxs_pos],
                                look_back="5y",
                                sample_time="15:00:00",
                                no_risk=True, offset=None)

# 4. Wait for completion
while "Complete" not in qcl.rt_nr_status(handle):
    time.sleep(10)

# 5. Get results
df = qcl.rt_nr_dump(handle)
```

---

# Part 5: Other Data Sources

## 1. Exante Data API -- Credit Impulse

**Source:** Exante Data (exantedata.com)
**What it provides:** Credit impulse data for EM and DM countries (6-month and 12-month)
**Authentication:** Username/password -> Bearer token

### API Flow

```
POST /getToken          -> Bearer token
POST /Data/Data         -> Time series data
```

### Ticker Format

`{CC}.CREDIT.TOTAL.IMPL.{6M|12M}.M` where CC is 2-letter country code.

**Countries:** US, EU, GB, DE, FR, IT, SP, CA, AU, NZ, JP, KR, CN, IN, ID, MY, TH, PH, TR, ZA, IL, MX, BR, CL, CO, RU, CH, SE, NO, CZ, HU, PO

### Code Pattern

From `charts/ChartUpdate/exante_utils.py`:

```python
from exante_utils import get_data

ticker_df = pd.read_excel("EMTicker.xlsx", sheet_name="Exante")
tickers_string = ','.join(ticker_df['6m'].tolist())
cpl_data = get_data(tickers_string, startDate='2022-03-01', endDate=None)
```

### Files

- `charts/ChartUpdate/exante_utils.py` -- Reusable utility module
- `charts/Exante.py` -- Standalone script
- `charts/ChartUpdate/getter_credit_impulse.py` -- Builds credit impulse tables
- `charts/ChartUpdate/UI_Economy.py` -- Dashboard display

---

## 2. IMF WEO Data -- World Economic Outlook

**Source:** IMF World Economic Outlook database (released April/October yearly)
**What it provides:** Long-term CPI inflation forecasts (4-year ahead) for the GS EM fair-value model
**Data format:** Tab-separated or Excel files named `WEO{Apr|Oct}{YYYY}all.xls`
**Key field:** `WEO Subject Code == 'PCPIEPCH'` (End-of-period CPI change)

### Countries Covered

Brazil, Mexico, Colombia, Chile, Peru, Czech Republic, Poland, Hungary, Romania, Israel, South Africa, China, India, Indonesia, Korea, Malaysia, Thailand, Philippines

### Code Pattern

From `scanner_code/Models_GS_EM_utils.py`:

```python
file_pattern = os.path.join(data_dir, "WEO*all.xls")
files = glob.glob(file_pattern)
df = read_weo_file(file)
df = df[df['WEO Subject Code'] == 'PCPIEPCH']
```

### Files

- `scanner_code/Models_GS_EM_utils.py` -- `read_weo_file()` function
- `scanner_code/Models.py` -- Loads `imf_cpi_monthly_forecasts.csv`

### Data Paths

- Raw: `scanner_code/Models_GS_EM_externalfiles/WEO_data/`
- Pre-processed: `scanner_code/Models_GS_EM_externalfiles/imf_cpi_monthly_forecasts.csv`

---

## 3. Bank Indonesia SEKI -- Monetary Data

**Source:** Bank Indonesia SEKI statistical database
**What it provides:** Adjusted M0 (Base Money) monthly data in billions IDR, plus YoY growth
**URL:** `https://www.bi.go.id/seki/tabel/TABEL1_2.xls`
**Target row:** "Uang Primer Adjusted 1)" (Adjusted Base Money)

### Code Pattern

From `charts/ChartUpdate/download_bi_monetary_data.py`:

```python
from download_bi_monetary_data import get_adjusted_m0_data, compute_yoy_growth

m0_data = get_adjusted_m0_data()    # auto-downloads and caches
m0_yoy = compute_yoy_growth(m0_data)
```

**Features:** HTTP download with retry/backoff, local Excel cache, pickle cache (30-day validity)
**Buffer directory:** `charts/ChartUpdate/buffer_bi_monetary/`

### Files

- `charts/ChartUpdate/download_bi_monetary_data.py` -- Full download/parse/cache module
- `charts/ChartUpdate/charts_updater_all.py` -- Consumer

---

## 4. Haver Analytics / CEIC -- Cement Production

**Source:** Haver Analytics (via shared network drive), CEIC (via Excel export)
**What they provide:** Cement production data for EM countries

### Code Pattern

From `charts/ChartUpdate/getter_cement.py`:

```python
# Haver (network drive)
tocopy = Path("G:\\Emerging Markets\\Faiz", "cement.xlsx")
copyto = Path(os.getcwd(), "cement_haver.xlsx")
shutil.copy2(tocopy, copyto)
data = pd.read_excel(copyto, skiprows=range(1, 10), index_col=0)

# CEIC (local Excel)
data = pd.read_excel("cemendatat_ceic.xlsx", skiprows=1, index_col=0).sort_index()
```

Both feed into X-13-ARIMA-SEATS seasonal adjustment.

---

## 5. Web Scraping (Selenium)

### 5a. KOFIA FreeSIS -- Korean MMF AUM

**Source:** Korea Financial Investment Association (KOFIA)
**URL:** `https://freesis.kofia.or.kr/stat/FreeSIS.do`
**What it provides:** Historical Money Market Fund (MMF) Total AUM data
**Technology:** Selenium + Chrome WebDriver (headless)
**File:** `charts/ChartUpdate/crawl_mmf_aum.py`

### 5b. Seibro -- Korea Retail FX Flows

**Source:** Korea Securities Depository (KSD) Seibro
**URL:** `https://seibro.or.kr/`
**What it provides:** Korea daily foreign equity/bond net purchases by market (USA, Japan, Hong Kong, China, Euro Market, Other)
**Technology:** Selenium + Chrome WebDriver
**File:** `charts/KoreaRetailFXFlows.py`
**Note:** Also sends email via Outlook COM automation with charts embedded

---

## 6. UK DWP Stat-Xplore API -- Universal Credit

**Source:** UK Department for Work and Pensions (DWP) Stat-Xplore REST API
**What it provides:** Universal Credit monthly caseload data ("No work requirements" conditionality regime)
**Authentication:** API key in header

### Code Pattern

From `charts/ChartUpdate/charts_updater_all.py` (function `chart_universal_claimants()`):

```python
session = requests.Session()
session.headers.update({"APIKey": API_KEY})
query = {
    "database": DB_ID,
    "measures": [MEASURE_ID],
    "dimensions": [[MONTH_FIELD_ID], [COND_FIELD_ID]]
}
resp = request_json("POST", f"{BASE_URL}/table", json_body=query)
```

---

## 7. Wikipedia -- S&P 500 Constituents

**Source:** Wikipedia
**What it provides:** Current S&P 500 constituent list (tickers and sectors)
**Technology:** `requests.get()` + `BeautifulSoup` (lxml parser)

```python
url = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
res = requests.get(url, headers=HEADERS, verify=False)
bs = BeautifulSoup(res.text, 'lxml')
table = bs.find('table', {'id': 'constituents'})
```

**Files:** `scanner_code/Sentiment Breadth - Prod.ipynb`

---

## 8. MAS DLI -- Singapore Domestic Liquidity Indicator

**Source:** Local CSV file (manually exported from Excel)
**What it provides:** MAS DLI 3-month change percentage (monthly, from 2012 onwards)
**Data file:** `charts/ChartUpdate/mas_dli_from_excel.csv`
**Consumer:** `charts/ChartUpdate/MAS_DLI_chart.py`

Combined with Bloomberg data (S$NEER and 3m SORA) to compute a DLI proxy.

---

## 9. Samsung Securities Email -- KTB Futures Positioning

**Source:** Samsung Securities email attachments via Outlook COM API
**What it provides:** Korean Treasury Bond (KTB) futures investor positioning data (3Y and 10Y)
**Technology:** `win32com.client` (Outlook COM automation)
**Email folder:** Inbox > "KTB Position"

```python
outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
inbox = outlook.GetDefaultFolder(6).Folders.Item("KTB Position")
# Extract Excel attachment and parse sheets "3Y" and "10Y"
data_3y = pd.read_excel(Attatchment_Path, sheet_name="3Y", ...)
```

**File:** `charts/Ktb_Future_Position_Monitor.py`

---

## 10. Key Local Data Files

| File | Description | Used By |
|------|-------------|---------|
| `EMTicker.xlsx` | Central ticker mapping (sheets: Exante, BbgEcon, BbgExport) | getter_credit_impulse.py, getter_econ.py, getter_auto.py |
| `CB Inflation Targets.xlsx` | Central bank inflation target change dates | Models_GS_EM_utils.py |
| `imf_cpi_monthly_forecasts.csv` | Pre-processed IMF WEO forecasts (monthly) | Models.py |
| `CapitalInflows/Ticker_Excels.xlsx` | Bond and equity flow tickers by country | CapitalFlowsMonitor.py |
| `CTAtickers2.csv` | Master ticker list (113 tickers) for CTA models | CTA_MomentumCalculator.py |
| `mas_dli_from_excel.csv` | MAS DLI 3M change % | MAS_DLI_chart.py |
| `cement_haver.xlsx` / `cemendatat_ceic.xlsx` | Cement production data | getter_cement.py |

---

## Summary of All Data Sources

| Source | Type | Data Provided | Primary Files |
|--------|------|---------------|---------------|
| Bloomberg (xbbg) | Terminal API | Prices, yields, econ data | 22+ files |
| Bloomberg (tia.bbg) | Terminal API (legacy) | Same as above | 7 files |
| TSDB (PyQCL) | Internal DB | IRS rates, FX fwds, FX vols | 16+ files |
| PyQCL Pricing | Internal Lib | IRS/FX swap pricing, bond fwd | 6 files |
| Exante Data API | REST API | Credit impulse | exante_utils.py |
| IMF WEO | Manual download | CPI forecasts (4Y ahead) | Models_GS_EM_utils.py |
| Bank Indonesia SEKI | HTTP download | M0/Base Money | download_bi_monetary_data.py |
| Haver Analytics | Network drive | Cement production | getter_cement.py |
| CEIC | Local Excel | Cement production | getter_cement.py |
| KOFIA FreeSIS | Selenium scrape | Korea MMF AUM | crawl_mmf_aum.py |
| Seibro (KSD) | Selenium scrape | Korea retail FX flows | KoreaRetailFXFlows.py |
| UK DWP Stat-Xplore | REST API | Universal Credit claimants | charts_updater_all.py |
| Wikipedia | HTTP + BS4 | S&P 500 constituents | Sentiment Breadth.ipynb |
| MAS DLI | Local CSV | Singapore DLI index | MAS_DLI_chart.py |
| Samsung Securities | Outlook email | KTB futures positioning | Ktb_Future_Position_Monitor.py |
