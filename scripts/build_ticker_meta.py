#!/usr/bin/env python3
"""
Builds data/ticker_meta.json with: symbol, name, sector, industry, description.

Uses yfinance per-symbol lookups with sensible timeouts and fallbacks. This is
run in GitHub Actions, so be defensive but simple.
"""
from __future__ import annotations
import json
import time
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
TICKERS_TXT = ROOT / "tickers.txt"
OUT = DATA / "ticker_meta.json"

def load_tickers() -> list[str]:
    return [t.strip().upper() for t in TICKERS_TXT.read_text().splitlines() if t.strip()]

def get_meta(sym: str) -> dict:
    """
    Try to fetch longName / sector / industry / longBusinessSummary.
    yfinance may return None or raise; keep it resilient.
    """
    name = sector = industry = desc = None
    try:
        t = yf.Ticker(sym)
        # .info can be slow but still the most complete for profile fields.
        info = t.info or {}
        name = info.get("longName") or info.get("shortName")
        sector = info.get("sector")
        industry = info.get("industry")
        desc = info.get("longBusinessSummary")
    except Exception:
        pass
    return {
        "symbol": sym,
        "name": name,
        "sector": sector,
        "industry": industry,
        "description": desc,
    }

def main():
    tickers = load_tickers()
    out: list[dict] = []
    for i, sym in enumerate(tickers, 1):
        m = get_meta(sym)
        out.append(m)
        if i % 25 == 0:
            print(f"[meta] fetched {i}/{len(tickers)} …")
            # be a good citizen
            time.sleep(0.5)

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"[✓] wrote {OUT} with {len(out)} rows")

if __name__ == "__main__":
    main()
