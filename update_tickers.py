import pandas as pd
import re
import sys

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def clean_ticker(t):
    t = str(t).strip().upper()
    t = re.sub(r"\s+", "", t)
    t = re.sub(r"\[.*?\]", "", t)
    return t

def main(out_path="tickers.txt"):
    tables = pd.read_html(WIKI_URL, flavor="lxml")
    target = None
    for df in tables:
        cols = [c.lower() for c in df.columns]
        if any(c in ("symbol", "ticker", "ticker symbol") for c in cols):
            target = df
            break
    if target is None:
        print("Could not find constituents table", file=sys.stderr)
        sys.exit(1)

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
