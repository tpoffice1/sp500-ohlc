import os, sys, json, io, zipfile, time, requests, pandas as pd
from datetime import datetime, timedelta, timezone

# Stooq mirrors for the daily US ZIP. Sometimes .com 404s; .pl or http works.
ZIP_URLS = [
    "http://stooq.com/db/h/d_us_txt.zip",
    "https://stooq.pl/db/h/d_us_txt.zip",
    "https://stooq.com/db/h/d_us_txt.zip",
]
# Per-symbol CSV fallback
CSV_TPL = "https://stooq.com/q/d/l/?s={sym}&i=d"
UA_HDRS = {"User-Agent": "Mozilla/5.0 (GitHub Actions bot)"}

def to_stooq_symbol(ticker: str) -> str:
    return f"{ticker.strip().lower().replace('.', '-')}.us"

def load_tickers(path="tickers.txt"):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def most_recent_weekday_iso():
    d = datetime.now(timezone.utc) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")

def fetch_zip_with_retries():
    for url in ZIP_URLS:
        for attempt in range(2):
            try:
                r = requests.get(url, headers=UA_HDRS, timeout=60)
                if r.status_code == 200 and r.content:
                    print(f"[zip] using {url}")
                    return io.BytesIO(r.content)
                time.sleep(1 + attempt)
            except requests.RequestException:
                time.sleep(1 + attempt)
    print("[zip] all mirrors failed, falling back to per-symbol CSV")
    return None

def parse_from_zip(zf: zipfile.ZipFile, stooq_symbol: str, target_date: str):
    # ZIP files use capitalized headers and per-symbol txt paths
    path = f"data/daily/us/{stooq_symbol[0]}/{stooq_symbol}.txt"
    try:
        with zf.open(path) as f:
            df = pd.read_csv(
                f, header=0,
                names=["Date","Open","High","Low","Close","Volume"],
                dtype={"Date": "string"}
            )
        row = df[df["Date"] == target_date]
        if row.empty:
            return None
        r = row.iloc[0]
        return {
            "date": target_date,
            "open": float(r["Open"]),
            "high": float(r["High"]),
            "low":  float(r["Low"]),
            "close":float(r["Close"]),
        }
    except KeyError:
        return None
    except Exception:
        return None

def fetch_symbol_csv(stooq_symbol: str, target_date: str):
    """
    Download per-symbol CSV and safely extract OHLC for target_date.
    Handles odd/missing headers and empty responses by returning None.
    """
    url = CSV_TPL.format(sym=stooq_symbol)
    try:
        r = requests.get(url, headers=UA_HDRS, timeout=30)
        r.raise_for_status()
    except requests.RequestException:
        return None

    # Parse defensively
    try:
        df = pd.read_csv(io.StringIO(r.text))
    except Exception:
        return None
    if df.empty or df.columns.size == 0:
        return None

    # Normalize headers to lowercase and strip whitespace/BOM
    df.columns = [str(c).strip().lower() for c in df.columns]
    # We only proceed if the expected columns exist
    if "date" not in df.columns:
        return None
    for col in ("open","high","low","close"):
        if col not in df.columns:
            return None

    # Date compare as string to match ISO target_date
    try:
        df["date"] = df["date"].astype(str)
    except Exception:
        return None

    row = df[df["date"] == target_date]
    if row.empty:
        return None

    r0 = row.iloc[0]
    try:
        return {
            "date":  target_date,
            "open":  float(r0["open"]),
            "high":  float(r0["high"]),
            "low":   float(r0["low"]),
            "close": float(r0["close"]),
        }
    except Exception:
        return None

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else most_recent_weekday_iso()
    tickers = load_tickers()

    # Manual-run speedup: limit number of tickers with env var
    limit = os.environ.get("LIMIT_TICKERS")
    if limit:
        try:
            n = max(1, int(limit))
            tickers = tickers[:n]
            print(f"[limit] processing first {n} tickers for this run")
        except ValueError:
            pass
    print(f"[info] total tickers this run: {len(tickers)}")

    rows = []
    zf = None
    zbuf = fetch_zip_with_retries()
    if zbuf:
        try:
            zf = zipfile.ZipFile(zbuf)
        except zipfile.BadZipFile:
            zf = None

    if zf:  # ZIP path (fast)
        for i, t in enumerate(tickers, 1):
            sym = to_stooq_symbol(t)
            rec = parse_from_zip(zf, sym, target_date)
            if rec:
                rec["ticker"] = t
                rows.append(rec)
            if i % 50 == 0:
                print(f"[zip] {i}/{len(tickers)} processed…")
    else:   # CSV fallback (slower)
        for i, t in enumerate(tickers, 1):
            sym = to_stooq_symbol(t)
            rec = fetch_symbol_csv(sym, target_date)
            if rec:
                rec["ticker"] = t
                rows.append(rec)
            if i % 25 == 0:
                print(f"[csv] {i}/{len(tickers)} processed…")

    os.makedirs("docs", exist_ok=True)
    out_path = f"docs/{target_date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"date": target_date, "count": len(rows), "rows": rows}, f, ensure_ascii=False)

    with open("docs/latest.json", "w", encoding="utf-8") as f:
        json.dump({"redirect": f"{target_date}.json"}, f)

    # Maintain manifest of available dates
    idx_path = "docs/index.json"
    try:
        manifest = json.load(open(idx_path, "r", encoding="utf-8")) if os.path.exists(idx_path) else {"dates": []}
    except Exception:
        manifest = {"dates": []}
    dates = set(manifest.get("dates", []))
    dates.add(target_date)
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump({"dates": sorted(dates)}, f)

    print(f"[done] wrote {out_path} with {len(rows)} rows")

if __name__ == "__main__":
    main()
