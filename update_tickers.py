"""
update_tickers.py
Fetches latest OHLC data for S&P 500 tickers and writes latest_sp500.json
"""

import yfinance as yf
import json, time
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
TICKERS_FILE = Path("tickers.txt")
OUTPUT_JSON = DATA_DIR / "latest_sp500.json"

with open(TICKERS_FILE) as f:
    tickers = [t.strip() for t in f if t.strip()]

print(f"[i] Loaded {len(tickers)} tickers")

records = []
success, failed = 0, 0
start_date = (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d")
end_date = datetime.utcnow().strftime("%Y-%m-%d")

for i, tkr in enumerate(tickers, 1):
    try:
        df = yf.download(tkr, start=start_date, end=end_date, progress=False)
        if df.empty:
            print(f"[-] No data for {tkr}")
            failed += 1
            continue

        last = df.iloc[-1]
        rec = {
            "symbol": tkr,
            "name": None,
            "open": round(float(last['Open']), 2),
            "high": round(float(last['High']), 2),
            "low": round(float(last['Low']), 2),
            "close": round(float(last['Close']), 2),
            "volume": int(last['Volume']),
            "date": str(df.index[-1].date())
        }
        records.append(rec)
        success += 1

        if i % 25 == 0 or i == len(tickers):
            print(f"[+] {i}/{len(tickers)} processed ({success} OK, {failed} failed)")

        time.sleep(0.4)  # polite throttle
    except Exception as e:
        print(f"[!] Error fetching {tkr}: {e}")
        failed += 1
        continue

with open(OUTPUT_JSON, "w") as f:
    json.dump(records, f, indent=2)

print(f"[✓] Done. {success} tickers written to {OUTPUT_JSON} ({failed} failed).")
