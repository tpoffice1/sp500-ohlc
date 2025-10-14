"""
update_tickers.py
Fetches daily OHLC data for all S&P 500 tickers and writes latest_sp500.json.
"""

import pandas as pd
import yfinance as yf
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

# Paths
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
TICKERS_FILE = Path("tickers.txt")
OUTPUT_JSON = DATA_DIR / "latest_sp500.json"

# Load tickers
with open(TICKERS_FILE) as f:
    tickers = [t.strip() for t in f if t.strip()]

print(f"[i] Loaded {len(tickers)} tickers")

# Fetch latest OHLC for each ticker
records = []
start = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")
end = datetime.utcnow().strftime("%Y-%m-%d")

for i, ticker in enumerate(tickers, 1):
    try:
        df = yf.download(ticker, start=start, end=end, progress=False)
        if df.empty:
            print(f"[-] No data for {ticker}")
            continue
        last_row = df.iloc[-1]
        records.append({
            "symbol": ticker,
            "name": None,
            "open": round(float(last_row["Open"]), 2),
            "high": round(float(last_row["High"]), 2),
            "low": round(float(last_row["Low"]), 2),
            "close": round(float(last_row["Close"]), 2),
            "volume": int(last_row["Volume"]),
            "date": str(df.index[-1].date())
        })
        if i % 25 == 0:
            print(f"[+] Processed {i}/{len(tickers)} tickers")
        time.sleep(0.5)  # throttle to avoid rate limit
    except Exception as e:
        print(f"[!] Error fetching {ticker}: {e}")

# Save to JSON
with open(OUTPUT_JSON, "w") as f:
    json.dump(records, f, indent=2)

print(f"[✓] Wrote {len(records)} tickers → {OUTPUT_JSON}")
