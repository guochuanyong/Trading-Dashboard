# Import modules
import pandas as pd
import requests
from collections import defaultdict
import yfinance as yf
from datetime import datetime
from time import sleep
from tqdm import tqdm
from io import StringIO
import re

# Extract updated list of NASDAQ-100 tickers (Current components)
url_nasdaq100 = "https://en.wikipedia.org/wiki/Nasdaq-100"

user_agents = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

response = requests.get(url_nasdaq100, headers=user_agents, timeout=30)
response.raise_for_status()

# Read HTML tables (wrap in StringIO)
tables = pd.read_html(StringIO(response.text))

def _norm_col(c):
    c = str(c)
    c = re.sub(r"\[.*?\]", "", c)   # remove footnote markers like [1]
    c = c.replace("\n", " ").strip()
    return c

# Find the "Current components" table by normalized column names
df_nasdaq100 = None
for t in tables:
    norm_cols = [_norm_col(c) for c in t.columns]
    if ("Ticker" in norm_cols) and ("ICB Industry" in norm_cols):
        t.columns = norm_cols  # rename columns to normalized names
        df_nasdaq100 = t
        break

if df_nasdaq100 is None:
    raise ValueError("Could not find NASDAQ-100 Current components table with columns: 'Ticker' and 'ICB Industry'")

sector_to_tickers = defaultdict(list)

# Sector comes from ICB Industry, ticker from Ticker
for ticker, sector in zip(df_nasdaq100["Ticker"], df_nasdaq100["ICB Industry"]):
    sector_to_tickers[sector].append(ticker)

dict_nasdaq100 = dict(sector_to_tickers)

# Copy dict_nasdaq100 to dataframe
rows = [
    {"Sector": sector, "Ticker Symbol": ticker}
    for sector, tickers in dict_nasdaq100.items()
    for ticker in tickers
]

df_nasdaq100_output = pd.DataFrame(rows)

# Make a copy to avoid mutating original
df_nasdaq100_enriched = df_nasdaq100_output.copy()

company_names = []
market_caps = []
trailing_pes = []

# tqdm wraps the iterable
for ticker in tqdm(
    df_nasdaq100_enriched["Ticker Symbol"],
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
df_nasdaq100_enriched["Company_Name"] = company_names
df_nasdaq100_enriched["Market_Cap"] = market_caps
df_nasdaq100_enriched["Trailing_PE"] = trailing_pes

# Convert to numeric
df_nasdaq100_enriched["Market_Cap"] = pd.to_numeric(df_nasdaq100_enriched["Market_Cap"], errors="coerce")
df_nasdaq100_enriched["Trailing_PE"] = pd.to_numeric(df_nasdaq100_enriched["Trailing_PE"], errors="coerce")

df_nasdaq100_enriched.to_csv("nasdaq100_tickers.csv", index=False)

# 1️⃣ Extract ticker list
tickers = df_nasdaq100_output["Ticker Symbol"].dropna().unique().tolist()
print(f"Number of tickers: {len(tickers)}")

# -----------------------------
# 2️⃣ Download 10 years of daily prices (NASDAQ-100 constituents)
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
    df_nasdaq100_output[["Ticker Symbol", "Sector"]],
    left_on="Ticker",
    right_on="Ticker Symbol",
    how="left"
).drop(columns=["Ticker Symbol"])

# -----------------------------
# ✅ 4.5️⃣ Download NASDAQ-100 index (^NDX), flatten columns if needed, and append
# -----------------------------
df_ndx = yf.download(
    tickers="^NDX",
    period="10y",
    interval="1d",
    auto_adjust=False,
    progress=True
)

# ✅ Flatten MultiIndex columns returned by yfinance (prevents NaN rows on concat)
if isinstance(df_ndx.columns, pd.MultiIndex):
    df_ndx.columns = df_ndx.columns.get_level_values(0)

df_ndx = df_ndx.reset_index()
df_ndx.rename(columns={"Date": "Trade_Date"}, inplace=True)

df_ndx["Ticker"] = "^NDX"
df_ndx["Sector"] = "Index"

# Ensure same base columns as df_final (Volume for ^NDX can be missing; that's OK)
df_ndx = df_ndx[["Trade_Date", "Ticker", "Sector", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

# Append to your main dataset
df_final = pd.concat([df_final, df_ndx], ignore_index=True)

# -----------------------------
# 5️⃣ Add metadata
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
output_file = "nasdaq100_prices_daily_10y_plus_ndx.csv"
df_final.to_csv(output_file, index=False)

print(f"Saved {len(df_final):,} rows to {output_file}")
