# Import modules
import pandas as pd
import requests
from collections import defaultdict
from io import StringIO

# Extract updated list of SP500 tickers
url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

user_agents = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

response = requests.get(url_sp500, headers=user_agents, timeout=30)
response.raise_for_status()

tables = pd.read_html(StringIO(response.text))
df_sp500 = tables[0]

sector_to_tickers = defaultdict(list)

for symbol, sector in zip(df_sp500["Symbol"], df_sp500["GICS Sector"]):
    sector_to_tickers[sector].append(symbol)

dict_sp500 = dict(sector_to_tickers)

# Copy dict_sp500 to csv
rows = [
    {"Sector": sector, "Ticker Symbol": ticker}
    for sector, tickers in dict_sp500.items()
    for ticker in tickers
]

df_sp500_output = pd.DataFrame(rows)

# Add timestamp
df_sp500_output["Last_Updated"] = pd.Timestamp.now()

df_sp500_output.to_csv("sp500_tickers.csv", index=False)
