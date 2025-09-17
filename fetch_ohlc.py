import os, sys, json, io, zipfile, time, requests, pandas as pd
from datetime import datetime, timedelta, timezone

ZIP_URLS = [
    "http://stooq.com/db/h/d_us_txt.zip",     # http .com (often works)
    "https://stooq.pl/db/h/d_us_txt.zip",     # https .pl mirror
    "https://stooq.com/db/h/d_us_txt.zip",    # https .com (sometimes 404)
]
CSV_TPL = "https://stooq.com/q/d/l/?s={sym}&i=d"  # per-symbol CSV fallback
UA_HDRS = {"User-Agent": "Mozilla/5.0 (GitHub Actions bot)"}

def to_stooq_symbol(ticker: str) -> str:
    return f"{ticker.strip().lower().replace('.', '-')}.us"

def load_tickers(path="tickers.txt"):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def most_recent_weekday_iso():
    d = datetime.now(timezone.utc) - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")

def fetch_zip_with_retries():
    for url in ZIP_URLS:
        for attempt in range(3):
            try:
                r = requests.get(url, headers=UA_HDRS, timeout=60)
                if r.status_code == 200 and r.content:
                    return io.BytesIO(r.content)
                else:
                    time.sleep(1 + attempt)
            except requests.RequestException:
                time.sleep(1 + attempt)
    return None  # signal to use fallback

def parse_from_zip(zf: zipfile.ZipFile, stooq_symbol: str, target_date: str):
    path = f"data/daily/us/{stooq_symbol[0]}/{stooq_symbol}.txt"
    try:
        with zf.open(path) as f:
            df = pd.read_csv(
                f,
                header=0,
                names=["Date","Open","High","Low","Close","Volume"],
                dtype={"Date":"string"}
            )
        row = df[df["Date"] == target_date]
        if row.empty:
            return None
        r = row.iloc[0]
        return {
            "date": target_date,
            "open": float(r["Open"]),
            "high": float(r["High"]),
            "low": float(r["Low"]),
            "close": float(r["Close"])
        }
    except KeyError:
        return None

def fetch_symbol_csv(stooq_symbol: str, target_date: str):
    url = CSV_TPL.format(sym=stooq_symbol)
    r = requests.get(url, headers=UA_HDRS, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    # Ensure Date column is string
    df["Date"] = df["Date"].astype(str)
    row = df[df["Date"] == target_date]
    if row.empty:
        return None
    r0 = row.iloc[0]
    return {
        "date": target_date,
        "open": float(r0["Open"]),
        "high": float(r0["High"]),
        "low": float(r0["Low"]),
        "close": float(r0["Close"])
    }

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else most_recent_weekday_iso()
    tickers = load_tickers()

    # Try ZIP first
    rows = []
    zbuf = fetch_zip_with_retries()
    if zbuf:
        try:
            zf = zipfile.ZipFile(zbuf)
        except zipfile.BadZipFile:
            zf = None
    else:
        zf = None

    if zf:
        for t in tickers:
            sym = to_stooq_symbol(t)
            rec = parse_from_zip(zf, sym, target_date)
            if rec:
                rec["ticker"] = t
                rows.append(rec)
    else:
        # Fallback: per-symbol CSV (reliable, a bit slower)
        for t in tickers:
            sym = to_stooq_symbol(t)
            try:
                rec = fetch_symbol_csv(sym, target_date)
                if rec:
                    rec["ticker"] = t
                    rows.append(rec)
            except requests.RequestException:
                continue  # skip on transient error

    os.makedirs("docs", exist_ok=True)
    out_path = f"docs/{target_date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"date": target_date, "count": len(rows), "rows": rows}, f, ensure_ascii=False)

    with open("docs/latest.json", "w", encoding="utf-8") as f:
        json.dump({"redirect": f"{target_date}.json"}, f)

    # Maintain a manifest of available dates
    idx_path = "docs/index.json"
    if os.path.exists(idx_path):
        try:
            manifest = json.load(open(idx_path, "r", encoding="utf-8"))
        except Exception:
            manifest = {"dates": []}
    else:
        manifest = {"dates": []}
    dates = set(manifest.get("dates", []))
    dates.add(target_date)
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump({"dates": sorted(dates)}, f)

    print(f"Wrote {out_path} with {len(rows)} rows; updated latest.json and index.json")

if __name__ == "__main__":
    main()
