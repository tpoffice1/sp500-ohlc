"""
update_tickers.py
Fetch daily OHLC for S&P 500 tickers and write data/latest_sp500.json
"""

from __future__ import annotations
import json
import time
import warnings
from datetime import datetime
from pathlib import Path

import yfinance as yf
import pandas as pd

# ---- quiet noisy warnings ----
warnings.simplefilter("ignore", FutureWarning)
pd.options.mode.copy_on_write = True  # pandas 2.x friendliness

# ---- paths ----
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
TICKERS_FILE = ROOT / "tickers.txt"
OUTPUT_JSON = DATA_DIR / "latest_sp500.json"

# ---- helpers ----
def to_float(x):
    try:
        # pandas scalar -> Python scalar
        if hasattr(x, "item"):
            x = x.item()
        return round(float(x), 2)
    except Exception:
        return None

def to_int(x):
    try:
        if hasattr(x, "item"):
            x = x.item()
        return int(x)
    except Exception:
        return None

def fetch_one(tkr: str) -> dict | None:
    """
    Fetch last OHLC for a single ticker. Returns dict or None.
    """
    try:
        t = yf.Ticker(tkr)
        # 5 trading days gives us a recent bar even around holidays/weekends
        df = t.history(period="5d", auto_adjust=False, actions=False)
        if df is None or df.empty:
            return None
        row = df.iloc[-1]

        return {
            "symbol": tkr,
            "name": None,  # optional: fill from your meta later
            "open": to_float(row.get("Open")),
            "high": to_float(row.get("High")),
            "low":  to_float(row.get("Low")),
            "close": to_float(row.get("Close")),
            "volume": to_int(row.get("Volume")),
            "date": str(getattr(df.index[-1], "date", lambda: df.index[-1])()),
        }
    except Exception as e:
        print(f"[!] {tkr}: {e}")
        return None

# ---- load tickers ----
if not TICKERS_FILE.exists():
    raise SystemExit(f"tickers.txt not found at {TICKERS_FILE}")

tickers = [t.strip() for t in TICKERS_FILE.read_text().splitlines() if t.strip()]
print(f"[i] Loaded {len(tickers)} tickers")

# ---- fetch loop ----
results: list[dict] = []
ok = err = 0
for i, tkr in enumerate(tickers, 1):
    rec = fetch_one(tkr)
    if rec:
        results.append(rec)
        ok += 1
    else:
        err += 1

    # progress every 25
    if i % 25 == 0 or i == len(tickers):
        print(f"[+] {i}/{len(tickers)} processed ({ok} ok, {err} failed)")

    # polite throttle (yfinance/yahoo rate-limits)
    time.sleep(0.25)

# ---- write output ----
OUTPUT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2))
print(f"[✓] Wrote {len(results)} rows to {OUTPUT_JSON} at {datetime.utcnow().isoformat()}Z")
