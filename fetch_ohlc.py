#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import math
import pathlib
import datetime as dt
from typing import List, Dict, Tuple

import pandas as pd
import numpy as np
import yfinance as yf
import requests

# ---------- Paths
ROOT = pathlib.Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
BUILD_DIR = ROOT / "build"
DATA_DIR.mkdir(exist_ok=True, parents=True)
BUILD_DIR.mkdir(exist_ok=True, parents=True)

TICKERS_FILE = ROOT / "tickers.txt"
OUT_JSON = DATA_DIR / "yesterday.json"
OUT_CSV = DATA_DIR / "latest.csv"
BAD_TICKERS = BUILD_DIR / "bad_tickers.txt"

# ---------- HTTP session that looks like a real browser (prevents 403s)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})


# ---------- helpers
def read_tickers(path: pathlib.Path) -> List[str]:
    tickers: List[str] = []
    with path.open() as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            tickers.append(s.upper())
    return tickers


def last_trading_values(ticker: str, tries: int = 3, sleep_s: float = 1.0) -> Tuple[str, float, float]:
    """
    Return (date_str, close, volume) for the last available daily bar.
    Raises the last error if all retries fail.
    """
    last_err = None
    for _ in range(tries):
        try:
            t = yf.Ticker(ticker, session=SESSION)
            # Grab a week; last row will be the latest trading day
            df = t.history(period="7d", interval="1d", auto_adjust=False)
            if df is None or df.empty:
                raise ValueError("empty dataframe")

            row = df.iloc[-1]
            date_str = df.index[-1].strftime("%Y-%m-%d")
            close = float(row["Close"])
            vol = float(row["Volume"]) if not pd.isna(row["Volume"]) else 0.0
            return date_str, close, vol
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    raise last_err if last_err else RuntimeError("unknown download error")


def main() -> int:
    if not TICKERS_FILE.exists():
        print(f"ERROR: {TICKERS_FILE} not found", file=sys.stderr)
        return 2

    symbols = read_tickers(TICKERS_FILE)
    if not symbols:
        print("ERROR: tickers.txt is empty", file=sys.stderr)
        return 2

    records: List[Dict] = []
    bad: List[str] = []

    for i, sym in enumerate(symbols, 1):
        try:
            date_str, close, vol = last_trading_values(sym)
            records.append(
                {
                    "symbol": sym,
                    "date": date_str,
                    "close": round(close, 6),
                    "volume": int(vol),
                }
            )
        except Exception as e:
            bad.append(sym)
        # Light backoff to be nice to upstream
        if i % 25 == 0:
            time.sleep(0.5)

    # Write bad tickers (if any)
    if bad:
        BAD_TICKERS.write_text("\n".join(bad) + "\n", encoding="utf-8")

    # Write JSON
    # Sort by symbol for deterministic output
    records.sort(key=lambda r: r["symbol"])
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    # Write CSV with a few core fields
    df = pd.DataFrame.from_records(records, columns=["symbol", "date", "close", "volume"])
    df.to_csv(OUT_CSV, index=False)

    print(f"Wrote {len(records)} rows; bad tickers: {len(bad)}", flush=True)
    # Exit non-zero if we got fewer than 9 rows (guards earlier sanity checks)
    return 0 if len(records) >= 9 else 1


if __name__ == "__main__":
    sys.exit(main())
