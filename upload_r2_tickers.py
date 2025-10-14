#!/usr/bin/env python3
"""
Upload artifacts to Cloudflare R2.

- Always uploads: data/latest_sp500.json  ->  ohlc/latest_sp500.json
- Optional: upload per-ticker stubs from tickers.csv to ohlc/<TICKER>.json (disable with --no-stubs)

Env (GitHub Actions -> Secrets):
  R2_ACCOUNT_ID
  R2_ENDPOINT_URL = https://<ACCOUNT_ID>.r2.cloudflarestorage.com
  R2_BUCKET = ohlc
  R2_ACCESS_KEY_ID
  R2_SECRET_ACCESS_KEY
  R2_PUBLIC_BASE (optional)

Usage:
  python upload_r2_tickers.py
  python upload_r2_tickers.py --no-stubs
  python upload_r2_tickers.py --dry-run
"""

from __future__ import annotations
import argparse, io, json, os, sys
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
LATEST_JSON = DATA / "latest_sp500.json"
TICKERS_TXT = ROOT / "tickers.txt"

def env(name: str, required=True, default=""):
    v = os.getenv(name, default)
    if required and not v:
        print(f"[ERROR] missing env: {name}", file=sys.stderr)
        sys.exit(1)
    return v

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-stubs", action="store_true")
    args = ap.parse_args()

    account_id   = env("R2_ACCOUNT_ID")
    endpoint_url = env("R2_ENDPOINT_URL")
    bucket       = env("R2_BUCKET")
    access_key   = env("R2_ACCESS_KEY_ID")
    secret_key   = env("R2_SECRET_ACCESS_KEY")
    public_base  = os.getenv("R2_PUBLIC_BASE", "")

    print("== R2 config =====================")
    print("ACCOUNT   :", account_id[:6] + "…")
    print("ENDPOINT  :", endpoint_url)
    print("BUCKET    :", bucket)
    print("PUBLIC    :", public_base or "(none)")
    print("MODE      :", "DRY RUN" if args.dry_run else "UPLOAD")
    print("==================================")

    if not LATEST_JSON.exists():
        print(f"[ERROR] {LATEST_JSON} not found. Build step must run first.", file=sys.stderr)
        sys.exit(1)

    # S3 client for R2
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(s3={"addressing_style": "virtual"})
    )

    # --- Upload main aggregate file ---
    latest_body = LATEST_JSON.read_bytes()
    if args.dry_run:
        print(f"[DRY] would PUT ohlc/latest_sp500.json ({len(latest_body)} bytes)")
    else:
        s3.put_object(
            Bucket=bucket,
            Key="ohlc/latest_sp500.json",
            Body=io.BytesIO(latest_body),
            ContentType="application/json"
        )
        try:
            arr = json.loads(latest_body)
            print(f"[+] Uploaded ohlc/latest_sp500.json with {len(arr)} rows.")
        except Exception:
            print("[+] Uploaded ohlc/latest_sp500.json")

    # --- Optional: upload per-ticker stubs (symbol + nulls) ---
    if not args.no_stubs and TICKERS_TXT.exists():
        tickers = [t.strip() for t in TICKERS_TXT.read_text().splitlines() if t.strip()]
        uploaded = 0
        for sym in tickers:
            stub = {
                "symbol": sym,
                "name": None, "open": None, "high": None, "low": None,
                "close": None, "volume": None, "date": None
            }
            body = json.dumps(stub, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            key = f"ohlc/{sym}.json"

            if args.dry_run:
                print(f"[DRY] would PUT {key}")
            else:
                s3.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(body), ContentType="application/json")
                uploaded += 1
                if uploaded % 100 == 0:
                    print(f"  … {uploaded}/{len(tickers)} stubs")

        if not args.dry_run:
            print(f"[+] Uploaded {uploaded} per-ticker stubs.")

    if public_base:
        print("Example public URL:", f"{public_base.rstrip('/')}/ohlc/latest_sp500.json")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(130)
