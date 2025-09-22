#!/usr/bin/env python3
import os, sys, json, math, time, pathlib
from typing import List, Dict

import pandas as pd
import numpy as np
import yfinance as yf

# ---------- paths ----------
ROOT = pathlib.Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
BUILD_DIR = ROOT / "build"
DATA_DIR.mkdir(parents=True, exist_ok=True)
BUILD_DIR.mkdir(parents=True, exist_ok=True)

TICKERS_FILE = ROOT / "tickers.txt"
OUT_JSON = DATA_DIR / "yesterday.json"
OUT_CSV  = DATA_DIR / "latest.csv"
BAD_TICKERS = BUILD_DIR / "bad_tickers.txt"

# ---------- ticker normalization ----------
# Known Yahoo aliases and general rule: replace '.' with '-'
ALIASES = {
    "BRK.B": "BRK-B",
    "BF.B":  "BF-B",
    # add any others you find here…
}

def normalize_symbol(s: str) -> str:
    s = s.strip().upper()
    s = ALIASES.get(s, s)
    s = s.replace(".", "-")
    return s

def read_tickers(path: pathlib.Path) -> List[str]:
    tickers = []
    with path.open() as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            tickers.append(normalize_symbol(t))
    # de-duplicate while preserving order
    seen, out = set(), []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

# ---------- download helpers ----------
def last_trading_row(df: pd.DataFrame) -> pd.Series | None:
    """Return the last available OHLC row from a per-ticker frame."""
    if df is None or df.empty:
        return None
    # df may be 1D series (single ticker with columns) or multi-index after download
    if isinstance(df, pd.Series):
        return df
    return df.tail(1).squeeze()

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_batch(tickers: list[str]) -> dict[str, Dict] :
    """
    Use yf.download in batches for speed; return {ticker: row_dict or None}
    """
    out: dict[str, Dict] = {t: None for t in tickers}
    # 2d/1d: ask for a 2-day window and take the last row (avoids holiday/weekend)
    df = yf.download(
        tickers=" ".join(tickers),
        period="2d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        timeout=30,
    )

    # When multiple tickers are requested, yfinance returns a column MultiIndex
    # like (ticker, field). When a single ticker is requested, fields are columns directly.
    if isinstance(df.columns, pd.MultiIndex):
        for t in tickers:
            try:
                sub = df[t]
            except Exception:
                sub = None
            row = last_trading_row(sub)
            if row is not None and "Close" in row:
                out[t] = {
                    "symbol": t,
                    "date": row.name.strftime("%Y-%m-%d") if hasattr(row, "name") else "",
                    "open":  float(row.get("Open",  np.nan)) if not pd.isna(row.get("Open",  np.nan)) else None,
                    "high":  float(row.get("High",  np.nan)) if not pd.isna(row.get("High",  np.nan)) else None,
                    "low":   float(row.get("Low",   np.nan)) if not pd.isna(row.get("Low",   np.nan)) else None,
                    "close": float(row.get("Close", np.nan)) if not pd.isna(row.get("Close", np.nan)) else None,
                    "volume": int(row.get("Volume", np.nan)) if not pd.isna(row.get("Volume", np.nan)) else None,
                }
    else:
        # Single ticker case
        t = tickers[0]
        row = last_trading_row(df)
        if row is not None and "Close" in row:
            out[t] = {
                "symbol": t,
                "date": row.name.strftime("%Y-%m-%d") if hasattr(row, "name") else "",
                "open":  float(row.get("Open",  np.nan)) if not pd.isna(row.get("Open",  np.nan)) else None,
                "high":  float(row.get("High",  np.nan)) if not pd.isna(row.get("High",  np.nan)) else None,
                "low":   float(row.get("Low",   np.nan)) if not pd.isna(row.get("Low",   np.nan)) else None,
                "close": float(row.get("Close", np.nan)) if not pd.isna(row.get("Close", np.nan)) else None,
                "volume": int(row.get("Volume", np.nan)) if not pd.isna(row.get("Volume", np.nan)) else None,
            }

    return out

# ---------- main ----------
def main() -> int:
    tickers = read_tickers(TICKERS_FILE)
    if not tickers:
        print("No tickers found in tickers.txt", file=sys.stderr)
        return 1

    results: list[Dict] = []
    bad: list[str] = []

    # Batch generously to stay fast but not overload: ~60 per batch works well
    for batch in chunked(tickers, 60):
        try:
            got = fetch_batch(batch)
        except Exception as e:
            # If the whole batch blows up, mark all as bad in this batch
            print(f"[batch error] {e}", file=sys.stderr)
            for t in batch:
                bad.append(t)
            continue

        for t in batch:
            row = got.get(t)
            if not row or row.get("close") is None:
                bad.append(t)
            else:
                results.append(row)

    # Sort by symbol for stable outputs
    results.sort(key=lambda r: r["symbol"])

    # Write outputs
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, separators=(",", ":"))

    # CSV
    df = pd.DataFrame(results, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    df.to_csv(OUT_CSV, index=False)

    # bad tickers (one per line)
    with BAD_TICKERS.open("w", encoding="utf-8") as f:
        for t in bad:
            f.write(t + "\n")

    print(f"Wrote {len(results)} rows; bad tickers: {len(bad)}")
    # Always exit 0 so the workflow proceeds (sanity steps will catch empties)
    return 0

if __name__ == "__main__":
    sys.exit(main())
