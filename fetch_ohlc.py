#!/usr/bin/env python3
import os, io, json, time, sys, re
from datetime import datetime, timedelta
from typing import List, Tuple
import requests
import pandas as pd

# ---- config ----
REQUIRED = ["Date", "Open", "High", "Low", "Close", "Volume"]
RAW_DIR = "data/raw"
OUT_LATEST = "data/latest.csv"
OUT_YDAY_JSON = "data/yesterday.json"
BAD_TICKERS = "build/bad_tickers.txt"
TICKERS_TXT = "tickers.txt"

# Stooq daily CSV endpoint. We try two symbol forms per ticker.
# Examples:
#   https://stooq.com/q/d/l/?s=aapl&i=d
#   https://stooq.com/q/d/l/?s=aapl.us&i=d
STOOQ_URL = "https://stooq.com/q/d/l/?s={sym}&i=d"

# ----------------

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs("data", exist_ok=True)
    os.makedirs("build", exist_ok=True)

def read_tickers(path: str) -> List[str]:
    if not os.path.exists(path):
        print(f"{path} not found", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return [t.strip().upper() for t in f if t.strip()]

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "sp500-ohlc/CI"})
    return s

def is_html(text: str) -> bool:
    t = text.strip().lower()
    return t.startswith("<!doctype") or t.startswith("<html")

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # lower/strip then map variants to canonical
    lowered = {c: re.sub(r"\s+", "", str(c)).lower() for c in df.columns}
    df = df.rename(columns=lowered)
    mapping = {
        "date": "Date",
        "timestamp": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "adjclose": "Close",
        "adj_close": "Close",
        "volume": "Volume",
        "vol": "Volume",
    }
    for k, v in mapping.items():
        if k in df.columns:
            df = df.rename(columns={k: v})
    return df

def validate_df(df: pd.DataFrame) -> Tuple[bool, str]:
    if df is None or df.empty:
        return False, "empty dataframe"
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        return False, f"missing columns: {missing}"
    return True, ""

def fetch_stooq(sym: str, s: requests.Session) -> pd.DataFrame:
    # Try <sym> and <sym>.us
    candidates = [sym.lower(), f"{sym.lower()}.us"]
    last_err = None
    for c in candidates:
        url = STOOQ_URL.format(sym=c)
        try:
            r = s.get(url, timeout=5)
            if r.status_code != 200:
                last_err = f"http {r.status_code} for {url}"
                continue
            txt = r.text
            if not txt.strip() or is_html(txt):
                last_err = f"non-CSV response for {url}"
                continue
            df = pd.read_csv(io.StringIO(txt))
            df = normalize_headers(df)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"]).sort_values("Date")
            ok, why = validate_df(df)
            if ok:
                return df
            last_err = why
        except Exception as e:
            last_err = str(e)
            time.sleep(0.5)
    raise RuntimeError(last_err or "unknown stooq error")

def write_bad(sym: str, reason: str, preview: str = ""):
    with open(BAD_TICKERS, "a", encoding="utf-8") as f:
        f.write(f"{sym}\t{reason}\n")
        if preview:
            f.write(preview + "\n")

def last_trading_day(now: datetime) -> datetime:
    # simple weekend logic; skip today to avoid partial candles
    d = (now - timedelta(days=1)).date()
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d -= timedelta(days=1)
    return datetime(d.year, d.month, d.day)

def main():
    ensure_dirs()
    tickers = read_tickers(TICKERS_TXT)
    s = session()

    frames = []
    processed = []

    # reset bad list
    if os.path.exists(BAD_TICKERS):
        os.remove(BAD_TICKERS)

    for sym in tickers:
        try:
            df = fetch_stooq(sym, s)
            # keep canonical columns only
            df = df[REQUIRED].copy()
            # store raw
            raw_path = os.path.join(RAW_DIR, f"{sym}.csv")
            df.to_csv(raw_path, index=False)
            # tag for aggregate
            df["Symbol"] = sym
            frames.append(df)
            processed.append(sym)
        except Exception as e:
            # best-effort preview
            preview = ""
            try:
                preview = df.head(3).to_string(index=False)  # type: ignore[name-defined]
            except Exception:
                pass
            write_bad(sym, f"{e}", preview)
            continue

    if not frames:
        print("No valid ticker data; see build/bad_tickers.txt", file=sys.stderr)
        sys.exit(1)

    all_df = pd.concat(frames, ignore_index=True)
    all_df = all_df.sort_values(["Date", "Symbol"])
    all_df.to_csv(OUT_LATEST, index=False)

    yday = last_trading_day(datetime.now())
    ymask = all_df["Date"].dt.date == yday.date()
    ydf = all_df.loc[ymask, ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    # stringify Date for JSON
    ydf["Date"] = ydf["Date"].dt.strftime("%Y-%m-%d")
    records = [
        {
            "Symbol": r["Symbol"],
            "Date": r["Date"],
            "Open": float(r["Open"]),
            "High": float(r["High"]),
            "Low": float(r["Low"]),
            "Close": float(r["Close"]),
            "Volume": int(r["Volume"]),
        }
        for _, r in ydf.iterrows()
    ]
    with open(OUT_YDAY_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    # optionally rewrite tickers.txt to those that succeeded
    with open(TICKERS_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(processed) + "\n")

    print(
        f"Wrote {len(processed)} tickers; "
        f"latest -> {OUT_LATEST}, yesterday -> {OUT_YDAY_JSON}. "
        f"See {BAD_TICKERS} for any skipped tickers."
    )

if __name__ == "__main__":
    main()
