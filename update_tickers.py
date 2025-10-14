"""
update_tickers.py
Fetch daily OHLC for S&P 500 tickers, carry forward last-known values for failures,
and write data/latest_sp500.json + data/failed_tickers.txt
"""
from __future__ import annotations
import json, time, warnings
from datetime import datetime
from pathlib import Path
import yfinance as yf
import pandas as pd

# quiet noisy warnings
warnings.simplefilter("ignore", FutureWarning)
pd.options.mode.copy_on_write = True

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
TICKERS_FILE = ROOT / "tickers.txt"
OUT_JSON = DATA_DIR / "latest_sp500.json"
FAILED_TXT = DATA_DIR / "failed_tickers.txt"

def to_float(x):
    try:
        if hasattr(x, "item"): x = x.item()
        return round(float(x), 2)
    except Exception:
        return None

def to_int(x):
    try:
        if hasattr(x, "item"): x = x.item()
        return int(x)
    except Exception:
        return None

def fetch_one(tkr: str):
    t = yf.Ticker(tkr)
    df = t.history(period="5d", auto_adjust=False, actions=False)
    if df is None or df.empty:
        return None
    row = df.iloc[-1]
    # Note: no name lookup here; you can merge names later if you like
    return {
        "symbol": tkr,
        "name": None,
        "open":  to_float(row.get("Open")),
        "high":  to_float(row.get("High")),
        "low":   to_float(row.get("Low")),
        "close": to_float(row.get("Close")),
        "volume": to_int(row.get("Volume")),
        "date": str(getattr(df.index[-1], "date", lambda: df.index[-1])()),
    }

# load tickers
if not TICKERS_FILE.exists():
    raise SystemExit("tickers.txt not found")
tickers = [t.strip() for t in TICKERS_FILE.read_text().splitlines() if t.strip()]
print(f"[i] Loaded {len(tickers)} tickers")

# load last-known (for carry-forward)
last_known = {}
if OUT_JSON.exists():
    try:
        for r in json.loads(OUT_JSON.read_text()):
            last_known[r["symbol"]] = r
        print(f"[i] Loaded {len(last_known)} last-known rows for carry-forward")
    except Exception:
        pass

results, failed = [], []
ok = skip = 0

for i, tkr in enumerate(tickers, 1):
    rec = None
    try:
        rec = fetch_one(tkr)
    except Exception as e:
        print(f"[!] {tkr}: {e}")

    if rec:
        results.append(rec)
        ok += 1
    else:
        # if we have last-known data, carry it forward with same structure
        if tkr in last_known:
            carry = dict(last_known[tkr])
            # don’t override the date/close if truly stale; leave as-is
            results.append(carry)
            skip += 1
        else:
            failed.append(tkr)

    if i % 25 == 0 or i == len(tickers):
        print(f"[+] {i}/{len(tickers)} processed (ok={ok}, carry={skip}, failed={len(failed)})")

    time.sleep(0.25)  # polite throttle

# persist outputs
OUT_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2))
FAILED_TXT.write_text("\n".join(failed) + ("\n" if failed else ""))

print(f"[✓] Wrote {len(results)} rows to {OUT_JSON.name} at {datetime.utcnow().isoformat()}Z")
print(f"[i] carry-forward: {skip}, failed today: {len(failed)} (saved to {FAILED_TXT.name})")
