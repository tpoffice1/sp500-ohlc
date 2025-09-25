#!/usr/bin/env python3
import json, os, time, sys, pathlib, urllib.parse, urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SRC = DATA_DIR / "yesterday.json"
OUT = DATA_DIR / "ticker_meta.json"

FMP_KEY = os.environ.get("FMP_API_KEY", "").strip()

def http_get(url, headers=None, timeout=15):
    h = {"User-Agent": "tptxdev-meta-builder/1.0 (+https://tptxdev.com)"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def read_symbols():
    with open(SRC, "r", encoding="utf-8") as f:
        raw = json.load(f)
    def pick(o, *ks):
        for k in ks:
            if isinstance(o, dict) and k in o and o[k]:
                return str(o[k])
        return None

    syms = []
    if isinstance(raw, list):
        for row in raw:
            sym = pick(row, "symbol", "ticker", "Symbol", "code", "sym")
            if sym: syms.append(sym.upper())
    elif isinstance(raw, dict):
        syms = [str(k).upper() for k in raw.keys()]

    # de-dup
    seen, uniq = set(), []
    for s in syms:
        if s and s not in seen:
            seen.add(s); uniq.append(s)
    return uniq

def guess_type(sym):
    s = sym.upper()
    if s in {"SPX","^GSPC","SPY"}: return "index"
    if s in {"BTC","BTCUSD","BTC-USD","BTCUSDT"}: return "crypto"
    return "equity"

def fmp_profile(sym):
    """Return {'name': ..., 'icon': ...} using FMP profile. Raises on HTTP errors."""
    url = f"https://financialmodelingprep.com/api/v3/profile/{urllib.parse.quote(sym)}?apikey={urllib.parse.quote(FMP_KEY)}"
    data = json.loads(http_get(url).decode("utf-8", "replace"))
    if isinstance(data, list) and data:
        rec = data[0]
        return {
            "name": rec.get("companyName") or rec.get("name") or "",
            "icon": rec.get("image") or ""
        }
    return {}

def sec_name_map():
    """
    Build {TICKER: COMPANY NAME} from SEC public JSON.
    https://www.sec.gov/files/company_tickers.json
    """
    # SEC asks for a descriptive UA; we already send one in http_get
    raw = http_get("https://www.sec.gov/files/company_tickers.json")
    j = json.loads(raw.decode("utf-8", "replace"))
    # SEC file is an object of { "0": {...}, "1": {...} }
    m = {}
    if isinstance(j, dict):
        for _, rec in j.items():
            t = (rec.get("ticker") or "").upper()
            nm = rec.get("title") or ""
            if t and nm: m[t] = nm
    return m

def main():
    syms = read_symbols()
    print(f"Found {len(syms)} symbols")
    names_by_sec = {}
    try:
        names_by_sec = sec_name_map()
        print(f"SEC names loaded: {len(names_by_sec)}")
    except Exception as e:
        print(f"[warn] SEC names not loaded: {e}", file=sys.stderr)

    meta = {}
    for i, s in enumerate(syms, 1):
        t = guess_type(s)
        name, icon = "", ""

        # Try FMP first if a key is present
        if FMP_KEY and t == "equity":
            try:
                prof = fmp_profile(s)
                name = prof.get("name") or ""
                icon = prof.get("icon") or ""
            except Exception as e:
                # Most common here is 401/Invalid key; fall back silently
                print(f"[warn] FMP {s}: {e}", file=sys.stderr)

        # Fallback to SEC for name (equities)
        if t == "equity" and not name:
            name = names_by_sec.get(s, "")

        # Crypto special-case (BTC)
        if t == "crypto":
            if s in {"BTC","BTC-USD","BTCUSD","BTCUSDT"}:
                if not name: name = "Bitcoin"
                if not icon: icon = "https://assets.coingecko.com/coins/images/1/thumb/bitcoin.png"

        # Index examples
        if t == "index":
            if s in {"SPY"}: name = name or "SPDR S&P 500 ETF"
            if s in {"SPX","^GSPC"}: name = name or "S&P 500 Index"

        row = {"type": t}
        if name: row["name"] = name
        if icon: row["icon"] = icon
        meta[s] = row

        if i % 100 == 0: print(f"  …{i}/{len(syms)}")
        time.sleep(0.05)  # gentle pace

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT} with {len(meta)} symbols")

if __name__ == "__main__":
    main()
