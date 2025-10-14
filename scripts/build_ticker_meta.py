# scripts/build_ticker_meta.py
from __future__ import annotations
import json, time, math
from pathlib import Path
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1] if (Path(__file__).name == "build_ticker_meta.py") else Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
TICKERS_TXT = ROOT / "tickers.txt"
OUT = DATA / "ticker_meta.json"

def load_tickers() -> list[str]:
    return [t.strip() for t in TICKERS_TXT.read_text().splitlines() if t.strip()]

def info_of(t: yf.Ticker) -> dict:
    # get_info is heavy; keep throttle
    try:
        info = t.get_info()
    except Exception:
        time.sleep(0.2)
        try:
            info = t.get_info()
        except Exception:
            return {}
    return {
        "name": info.get("shortName") or info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
    }

def main():
    tickers = load_tickers()
    meta = []
    BATCH = 50
    for i in range(0, len(tickers), BATCH):
        batch = tickers[i:i+BATCH]
        print(f"[+] meta batch {i//BATCH+1}/{math.ceil(len(tickers)/BATCH)} ({len(batch)})")
        for sym in batch:
            t = yf.Ticker(sym)
            d = info_of(t)
            meta.append({
                "symbol": sym,
                "name": d.get("name"),
                "sector": d.get("sector"),
                "industry": d.get("industry")
            })
            time.sleep(0.1)
    OUT.write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"[✓] wrote {OUT} with {len(meta)} rows")

if __name__ == "__main__":
    main()

