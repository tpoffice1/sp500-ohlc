def fetch_symbol_csv(stooq_symbol: str, target_date: str):
    """
    Download per-symbol CSV from Stooq and safely extract OHLC for target_date.
    Skips tickers that return empty or malformed data.
    """
    url = CSV_TPL.format(sym=stooq_symbol)
    r = requests.get(url, headers=UA_HDRS, timeout=30)
    r.raise_for_status()

    try:
        df = pd.read_csv(io.StringIO(r.text))
    except Exception:
        return None

    # Bail if no rows or no headers
    if df.empty or df.columns.size == 0:
        return None

    # Normalize headers
    df.columns = [c.strip().lower() for c in df.columns]

    if "date" not in df.columns:
        # Stooq sent something unexpected (maybe “no data” HTML)
        return None

    # Convert date column to string safely
    try:
        df["date"] = df["date"].astype(str)
    except Exception:
        return None

    row = df[df["date"] == target_date]
    if row.empty:
        return None

    r0 = row.iloc[0]
    return {
        "date": target_date,
        "open": float(r0.get("open", 0) or 0),
        "high": float(r0.get("high", 0) or 0),
        "low": float(r0.get("low", 0) or 0),
        "close": float(r0.get("close", 0) or 0),
    }
