# update_tickers.py — fast, batched, threaded; merges names
from __future__ import annotations
import json, math
from pathlib import Path
import yfinance as yf
import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
TICKERS_TXT = ROOT / "tickers.txt"
META_JSON = DATA / "ticker_meta.json"        # built by scripts/build_ticker_meta.py
OUT_JSON = DATA / "latest_sp500.json"

def load_tickers() -> list[str]:
    return [t.strip() for t in TICKERS_TXT.read_text().splitlines() if t.strip()]

def load_names() -> dict[str,str]:
    names = {}
    if META_JSON.exists():
        try:
            meta = json.loads(META_JSON.read_text())
            # expect [{"symbol":"AAPL","name":"Apple Inc."}, ...]
            for r in meta:
                s = (r.get("symbol") or "").upper()
                n = r.get("name")
                if s and n:
                    names[s] = n
        except Exception:
            pass
    return names

def pack_row(sym, row) -> dict:
    # row is a Pandas Series for that date
    def f(x):
        try: return None if pd.isna(x) else round(float(x), 2)
        except Exception: return None
    def i(x):
        try: return None if pd.isna(x) else int(x)
        except Exception: return None
    return {
        "symbol": sym,
        "name": None,  # filled after merge
        "open":  f(row.get("Open")),
        "high":  f(row.get("High")),
        "low":   f(row.get("Low")),
        "close": f(row.get("Close")),
        "volume": i(row.get("Volume")),
        "date":  str(getattr(row.name, "date", lambda: row.name)()),
    }

def main():
    tickers = load_tickers()
    names = load_names()
    print(f"[i] Loaded {len(tickers)} tickers; {len(names)} names available")

    # Batch to avoid giant single call; 80–120 per batch works well on Actions
    BATCH = 100
    out: list[dict] = []
    for bi in range(0, len(tickers), BATCH):
        batch = tickers[bi:bi+BATCH]
        print(f"[+] Batch {bi//BATCH+1}/{math.ceil(len(tickers)/BATCH)}: {len(batch)} tickers")

        # Multi-download, threaded; 5d period ensures a recent trading day
        df = yf.download(
            tickers=" ".join(batch),
            period="5d",
            auto_adjust=False,
            progress=False,
            group_by="ticker",
            threads=True
        )

        # yfinance returns either a MultiIndex (Ticker, Field, Date) or per-ticker frames
        if isinstance(df.columns, pd.MultiIndex):
            # MultiIndex: columns like (AAPL, Open) ...
            for sym in batch:
                try:
                    sub = df[sym].dropna(how="all")
                    if sub.empty: continue
                    row = sub.iloc[-1]
                    out.append(pack_row(sym, row))
                except Exception:
                    continue
        else:
            # Single ticker frame fallback (rare in multi-call, but safe)
            if df.empty:
                continue
            row = df.iloc[-1]
            # We can’t know which symbol; skip to be safe in this fallback
            continue

    # merge names
    for r in out:
        if not r.get("name"):
            r["name"] = names.get(r["symbol"])

    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"[✓] Wrote {len(out)} rows → {OUT_JSON}")

if __name__ == "__main__":
    main()
