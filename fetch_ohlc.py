#!/usr/bin/env python3
import os, sys, time, json, math, pathlib, datetime as dt
from typing import List, Dict

import pandas as pd
import numpy as np
import yfinance as yf

ROOT = pathlib.Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
BUILD_DIR = ROOT / "build"
DATA_DIR.mkdir(exist_ok=True, parents=True)
BUILD_DIR.mkdir(exist_ok=True, parents=True)

TICKERS_FILE = ROOT / "tickers.txt"
OUT_JSON = DATA_DIR / "yesterday.json"
OUT_CSV  = DATA_DIR / "latest.csv"
BAD_TICKERS = BUILD_DIR / "bad_tickers.txt"

# ---------- helpers

def read_tickers(path: pathlib.Path) -> List[str]:
    tickers = []
    with path.open() as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            tickers.append(s.upper())
    return tickers

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def last_trading_day(today=None):
    tz = dt.timezone.utc
    today = today or dt.datetime.now(tz).date()
    # Go back up to 7 days to find the most recent trading day in the data
    return today

def frame_for_chunk(tickers: List[str]) -> pd.DataFrame:
    """
    Download last few days of 1D bars for a batch of tickers.
    We request 'period=7d' to make sure 'yesterday' exists even after weekends/holidays.
    """
    # yfinance returns a MultiIndex columns when group_by='ticker'
    df = yf.download(
        tickers=" ".join(tickers),
        period="7d",
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,
        timeout=30,
    )
    return df

def extract_yesterday_rows(df: pd.DataFrame, symbols: List[str]) -> Dict[str, Dict]:
    """
    From yfinance's multi-index columns (symbol first), select the last row (yesterday)
    for each symbol and return a dictionary of {symbol: {Date, Close, Volume, ...}}.
    """
    out = {}
    if df is None or df.empty:
        return out

    # yfinance sometimes returns a single-level columns if only one ticker
    if isinstance(df.columns, pd.MultiIndex):
        # last available row in index
        last_idx = df.index.max()
        for sym in symbols:
            if sym not in df.columns.get_level_values(0):
                continue
            sub = df[sym]
            row = sub.loc[last_idx:last_idx]
            if row.empty:
                continue
            r = row.iloc[0]
            # Build normalized record
            out[sym] = {
                "symbol": sym,
                "date": pd.to_datetime(last_idx).date().isoformat(),
                "open":  float(r.get("Open", np.nan)) if not pd.isna(r.get("Open", np.nan)) else None,
                "high":  float(r.get("High", np.nan)) if not pd.isna(r.get("High", np.nan)) else None,
                "low":   float(r.get("Low",  np.nan)) if not pd.isna(r.get("Low",  np.nan)) else None,
                "close": float(r.get("Close",np.nan)) if not pd.isna(r.get("Close",np.nan)) else None,
                "volume": int(r.get("Volume", 0)) if not pd.isna(r.get("Volume", np.nan)) else None,
            }
    else:
        # Single ticker case
        last_idx = df.index.max()
        r = df.loc[last_idx:last_idx].iloc[0]
        # We don't know the symbol here; caller must handle single-ticker mapping
    return out

def main():
    # 1) read tickers
    if not TICKERS_FILE.exists():
        print(f"ERROR: {TICKERS_FILE} not found", file=sys.stderr)
        sys.exit(1)

    symbols = read_tickers(TICKERS_FILE)
    symbols = [s.replace('.', '-') for s in symbols]

    if len(symbols) < 50:
        print(f"WARNING: only {len(symbols)} symbols read from {TICKERS_FILE}", file=sys.stderr)

    # 2) fetch in batches
    BATCH = 50
    RETRIES = 3
    SLEEP   = 2  # seconds between calls; be nice to Yahoo

    records = {}
    bad = []

    for chunk in chunked(symbols, BATCH):
        success = False
        for attempt in range(1, RETRIES + 1):
            try:
                df = frame_for_chunk(chunk)
                partial = extract_yesterday_rows(df, chunk)
                # mark missing in this chunk
                missing = [s for s in chunk if s not in partial]
                for s in missing:
                    # we won't declare 'bad' yet—retry first
                    pass
                # merge
                records.update(partial)
                success = True
                break
            except Exception as e:
                print(f"[batch {chunk[0]}…] attempt {attempt} failed: {e}", file=sys.stderr)
                time.sleep(1 + attempt)

        if not success:
            bad.extend(chunk)

        time.sleep(SLEEP)

    # anything we didn't fill after all batches becomes bad
    still_missing = [s for s in symbols if s not in records]
    for s in still_missing:
        if s not in bad:
            bad.append(s)

    # 3) build final list
    rows = []
    for s in symbols:
        rec = records.get(s)
        if not rec:
            continue
        rows.append(rec)

    # 4) output
    with OUT_JSON.open("w") as f:
        json.dump(rows, f, indent=2)

    # latest.csv (symbol,date,close,volume)
    csv_rows = []
    for r in rows:
        csv_rows.append({
            "Symbol": r["symbol"],
            "Date": r["date"],
            "Close": r["close"],
            "Volume": r["volume"],
        })
    pd.DataFrame(csv_rows).to_csv(OUT_CSV, index=False)

    if bad:
        BAD_TICKERS.write_text("\n".join(sorted(set(bad))) + "\n")
    else:
        if BAD_TICKERS.exists():
            BAD_TICKERS.unlink()

    print(f"Wrote {len(rows)} tickers; latest -> {OUT_CSV}, yesterday -> {OUT_JSON}")
    if bad:
        print(f"{len(bad)} missing; see {BAD_TICKERS}")

if __name__ == "__main__":
    main()

