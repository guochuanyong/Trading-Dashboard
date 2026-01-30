#Import modules
import pandas as pd
import requests
from collections import defaultdict
import yfinance as yf
from datetime import datetime
import pandas as pd
from time import sleep
from tqdm import tqdm
from io import StringIO

#Extract updated list of SP500 tickers
url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

user_agents = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

response = requests.get(url_sp500, headers=user_agents, timeout=30)
response.raise_for_status()

tables = pd.read_html(response.text)
df_sp500 = tables[0]

sector_to_tickers = defaultdict(list)

for symbol, sector in zip(df_sp500["Symbol"], df_sp500["GICS Sector"]):
    sector_to_tickers[sector].append(symbol)

dict_sp500 = dict(sector_to_tickers)

#Copy dict_sp500 to csv
rows = [
    {"Sector": sector, "Ticker Symbol": ticker}
    for sector, tickers in dict_sp500.items()
    for ticker in tickers
]

df_sp500_output = pd.DataFrame(rows)



# Make a copy to avoid mutating original
df_sp500_enriched = df_sp500_output.copy()

company_names = []
market_caps = []
trailing_pes = []

# tqdm wraps the iterable
for ticker in tqdm(
    df_sp500_enriched["Ticker Symbol"],
    desc="Fetching company metadata",
    unit="ticker"
):
    try:
        tk = yf.Ticker(ticker)
        info = tk.info

        company_name = info.get("shortName") or info.get("longName")
        market_cap = info.get("marketCap")
        trailing_pe = info.get("trailingPE")

    except Exception:
        company_name = None
        market_cap = None
        trailing_pe = None

    company_names.append(company_name)
    market_caps.append(market_cap)
    trailing_pes.append(trailing_pe)

    sleep(0.05)  # avoid Yahoo throttling

# Add columns
df_sp500_enriched["Company_Name"] = company_names
df_sp500_enriched["Market_Cap"] = market_caps
df_sp500_enriched["Trailing_PE"] = trailing_pes

# Convert to numeric
df_sp500_enriched["Market_Cap"] = pd.to_numeric(df_sp500_enriched["Market_Cap"], errors="coerce")
df_sp500_enriched["Trailing_PE"] = pd.to_numeric(df_sp500_enriched["Trailing_PE"],errors="coerce")

df_sp500_enriched.to_csv("sp500_tickers.csv", index=False)

# 1️⃣ Extract ticker list
tickers = df_sp500_output["Ticker Symbol"].dropna().unique().tolist()
print(f"Number of tickers: {len(tickers)}")

# -----------------------------
# 2️⃣ Download 10 years of daily prices (S&P 500 constituents)
# -----------------------------
df_prices = yf.download(
    tickers=tickers,
    period="10y",
    interval="1d",
    group_by="ticker",
    auto_adjust=False,
    threads=True,
    progress=True
)

# -----------------------------
# 3️⃣ Convert MultiIndex → long format
# -----------------------------
df_long = (
    df_prices
    .stack(level=0)
    .reset_index()
    .rename(columns={"level_1": "Ticker"})
)

# Rename Date column if needed
df_long.rename(columns={"Date": "Trade_Date"}, inplace=True)

# -----------------------------
# 4️⃣ Join sector information
# -----------------------------
df_final = df_long.merge(
    df_sp500_output[["Ticker Symbol", "Sector"]],
    left_on="Ticker",
    right_on="Ticker Symbol",
    how="left"
).drop(columns=["Ticker Symbol"])

# -----------------------------
# ✅ 4.5️⃣ Download S&P 500 index (^GSPC), flatten columns if needed, and append
# -----------------------------
df_spx = yf.download(
    tickers="^GSPC",
    period="10y",
    interval="1d",
    auto_adjust=False,
    progress=True
)

# ✅ Flatten MultiIndex columns returned by yfinance (prevents NaN rows on concat)
if isinstance(df_spx.columns, pd.MultiIndex):
    df_spx.columns = df_spx.columns.get_level_values(0)

df_spx = df_spx.reset_index()
df_spx.rename(columns={"Date": "Trade_Date"}, inplace=True)

df_spx["Ticker"] = "^GSPC"
df_spx["Sector"] = "Index"

# Ensure same base columns as df_final (Volume for ^GSPC can be missing; that's OK)
df_spx = df_spx[["Trade_Date", "Ticker", "Sector", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

# Append to your main dataset
df_final = pd.concat([df_final, df_spx], ignore_index=True)

# -----------------------------
# 5️⃣ Add metadata (optional but nice)
# -----------------------------
df_final["Data_Source"] = "Yahoo Finance (yfinance)"
df_final["Extracted_At"] = datetime.now()

# -----------------------------
# 6️⃣ Reorder columns (Power BI friendly)
# -----------------------------
df_final = df_final[
    [
        "Trade_Date",
        "Ticker",
        "Sector",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "Data_Source",
        "Extracted_At",
    ]
]

# -----------------------------
# 7️⃣ Save to CSV
# -----------------------------
output_file = "sp500_prices_daily_10y_plus_spx.csv"
df_final.to_csv(output_file, index=False)

print(f"Saved {len(df_final):,} rows to {output_file}")
