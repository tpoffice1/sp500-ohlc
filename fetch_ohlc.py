def fetch_symbol_csv(stooq_symbol: str, target_date: str):
    url = CSV_TPL.format(sym=stooq_symbol)
    r = requests.get(url, headers=UA_HDRS, timeout=30)
    r.raise_for_status()

    # Try to parse CSV safely
    df = pd.read_csv(io.StringIO(r.text))
    if df.empty:
        return None

    # Normalize headers (strip & lower)
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns:
        return None  # no usable data

    row = df[df["date"] == target_date]
    if row.empty:
        return None

    r0 = row.iloc[0]
    return {
        "date": target_date,
        "open": float(r0.get("open", 0)),
        "high": float(r0.get("high", 0)),
        "low": float(r0.get("low", 0)),
        "close": float(r0.get("close", 0)),
    }
