import os, sys, json, io, zipfile, requests, pandas as pd
from datetime import datetime, timedelta, timezone

STOOQ_DAILY_ZIP = "https://stooq.com/db/h/d_us_txt.zip"  # free EOD US data

def to_stooq_symbol(ticker: str) -> str:
    return f"{ticker.strip().lower().replace('.', '-')}.us"

def load_tickers(path="tickers.txt"):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def fetch_zip(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return io.BytesIO(r.content)

def parse_symbol_from_zip(zf: zipfile.ZipFile, stooq_symbol: str, target_date: str):
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

def most_recent_weekday_iso():
    d = datetime.now(timezone.utc) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")

def load_manifest(path="docs/index.json"):
    if not os.path.exists(path):
        return {"dates": []}
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {"dates": []}

def save_manifest(dates, path="docs/index.json"):
    dates = sorted(set(dates))
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"dates": dates}, f)

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else most_recent_weekday_iso()
    tickers = load_tickers()

    zbuf = fetch_zip(STOOQ_DAILY_ZIP)
    zf = zipfile.ZipFile(zbuf)

    rows = []
    for t in tickers:
        sym = to_stooq_symbol(t)
        rec = parse_symbol_from_zip(zf, sym, target_date)
        if rec:
            rec["ticker"] = t
            rows.append(rec)

    os.makedirs("docs", exist_ok=True)
    out_path = f"docs/{target_date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"date": target_date, "count": len(rows), "rows": rows}, f, ensure_ascii=False)

    with open("docs/latest.json", "w", encoding="utf-8") as f:
        json.dump({"redirect": f"{target_date}.json"}, f)

    manifest = load_manifest()
    dates = set(manifest.get("dates", []))
    dates.add(target_date)
    save_manifest(sorted(dates))

    print(f"Wrote {out_path} with {len(rows)} rows; updated latest.json and index.json")

if __name__ == "__main__":
    main()
