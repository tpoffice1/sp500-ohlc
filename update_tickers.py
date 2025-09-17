import re
import sys
import pandas as pd
import requests
from io import StringIO

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def clean_ticker(t: str) -> str:
    t = str(t).strip().upper()
    t = re.sub(r"\s+", "", t)          # remove spaces
    t = re.sub(r"\[.*?\]", "", t)      # drop footnote markers like [1]
    return t

def main(out_path: str = "tickers.txt"):
    # Fetch with a browser-like UA so Wikipedia doesn't 403 us on Actions
    headers = {"User-Agent": "Mozilla/5.0 (GitHub Actions bot)"}
    resp = requests.get(WIKI_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    # Parse the HTML into dataframes
    tables = pd.read_html(StringIO(resp.text), flavor="lxml")

    # Find the table that has a ticker column
    target = None
    for df in tables:
        cols = [c.lower() for c in df.columns]
        if any(c in ("symbol", "ticker", "ticker symbol") for c in cols):
            target = df
            break
    if target is None:
        print("Could not find constituents table", file=sys.stderr)
        sys.exit(1)

    # Locate the exact column name used for tickers
    for cand in ("Symbol", "Ticker", "Ticker symbol"):
        if cand in target.columns:
            ticker_col = cand
            break

    tickers = [clean_ticker(x) for x in target[ticker_col].tolist()]
    tickers = [t for t in tickers if t and t != "NAN"]
    tickers = sorted(set(tickers))

    with open(out_path, "w", encoding="utf-8") as f:
        for t in tickers:
            f.write(t + "\n")

    print(f"Wrote {len(tickers)} tickers to {out_path}")

if __name__ == "__main__":
    main()
