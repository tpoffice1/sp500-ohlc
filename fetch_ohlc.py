import os, io, json, time, csv, sys, re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
import requests
import pandas as pd

REQUIRED = ["Date", "Open", "High", "Low", "Close", "Volume"]
RAW_DIR = "data/raw"
OUT_LATEST = "data/latest.csv"
OUT_YDAY_JSON = "data/yesterday.json"
BAD_TICKERS = "build/bad_tickers.txt"
TICKERS_TXT = "tickers.txt"

SOURCE_URL_TPL = "https://example.datasource/{sym}.csv"  # replace with your real endpoint

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs("build", exist_ok=True)
    os.makedirs("data", exist_ok=True)

def session_with_retry() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "sp500-ohlc/1.0"})
    return s

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    lower = {c: re.sub(r"\s+", "", str(c)).lower() for c in df.columns}
    df = df.rename(columns=lower)
    # map variants to canonical names
    mapping = {
        "date": "Date", "timestamp": "Date",
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "adjclose": "Close", "adj_close": "Close",
        "volume": "Volume", "vol": "Volume"
    }
    # only rename if present
    for k, v in mapping.items():
        if k in df.columns:
            df = df.rename(columns={k: v})
    return df

def validate_df(df: pd.DataFrame, sym: str) -> Tuple[bool, str]:
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        return False, f"missing columns: {missing}"
    if df.empty:
        return False, "empty dataframe"
    return True, ""

def fetch_one(s: requests.Session, sym: str) -> pd.DataFrame:
    url = SOURCE_URL_TPL.format(sym=sym)
    for attempt in range(3):
        try:
            r = s.get(url, timeout=5)
            if r.status_code != 200:
                raise RuntimeError(f"http {r.status_code}")
            text = r.text.strip()
            if text.startswith("<!DOCTYPE") or text.lower().startswith("<html"):
                raise RuntimeError("html response")
            df = pd.read_csv(io.StringIO(text))
            df = normalize_headers(df)
            # parse Date if present under canonical name after normalize
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=False)
                df = df.dropna(subset=["Date"]).sort_values("Date")
            return df
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)
    raise RuntimeError("unreachable")

def last_trading_day(dt: datetime) -> datetime:
    # naive weekend logic; replace with an exchange calendar if needed
    d = dt.date()
    # step back from today (exclude today’s incomplete data)
    d = d - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return datetime(d.year, d.month, d.day)

def write_bad(sym: str, reason: str, head_preview: str = ""):
    with open(BAD_TICKERS, "a", encoding="utf-8") as f:
        f.write(f"{sym}\t{reason}\n")
        if head_preview:
            f.write(head_preview + "\n")

def main():
    ensure_dirs()

    # load tickers (one per line)
    if not os.path.exists(TICKERS_TXT):
        print(f"{TICKERS_TXT} not found", file=sys.stderr)
        sys.exit(1)
    with open(TICKERS_TXT, "r", encoding="utf-8") as f:
        tickers = [t.strip().upper() for t in f if t.strip()]

    s = session_with_retry()
    frames = []
    processed = []

    # clean previous bad list
    if os.path.exists(BAD_TICKERS):
        os.remove(BAD_TICKERS)

    for sym in tickers:
        try:
            df = fetch_one(s, sym)
            ok, why = validate_df(df, sym)
            if not ok:
                preview = df.head(3).to_string(index=False) if not df.empty else ""
                write_bad(sym, why, preview)
                continue

            # keep only canonical columns, in order
            df = df[REQUIRED].copy()
            # store raw
            raw_path = os.path.join(RAW_DIR, f"{sym}.csv")
            df.to_csv(raw_path, index=False)
            # tag with ticker for aggregate
            df["Symbol"] = sym
            frames.append(df)
            processed.append(sym)

        except Exception as e:
            err = str(e)
            preview = ""
            try:
                preview = df.head(3).to_string(index=False)  # may not exist
            except Exception:
                pass
            write_bad(sym, f"exception: {err}", preview)
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

    # serialize Date to ISO string for JSON
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

    # rewrite tickers.txt to reflect processed list (optional)
    with open(TICKERS_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(processed) + "\n")

    print(f"Wrote {len(processed)} tickers; {len(frames)} dataframes; "
          f"latest -> {OUT_LATEST}, yesterday -> {OUT_YDAY_JSON}")
    if os.path.exists(BAD_TICKERS):
        print(f"See {BAD_TICKERS} for skipped tickers.")

if __name__ == "__main__":
    main()
