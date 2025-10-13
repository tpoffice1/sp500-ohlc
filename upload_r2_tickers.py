#!/usr/bin/env python3
"""
Upload OHLC JSON stubs to Cloudflare R2 and publish an aggregate latest file.

Usage (local):
  python upload_r2_tickers.py --dry-run
  python upload_r2_tickers.py

Environment (via .env locally, or GitHub Actions secrets):
  R2_ACCOUNT_ID=...
  R2_ACCESS_KEY_ID=...
  R2_SECRET_ACCESS_KEY=...
  R2_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
  R2_BUCKET=ohlc
  R2_PUBLIC_BASE=https://pub-<id>.r2.dev        (optional, for info logs)
"""

from __future__ import annotations
import csv, io, json, os, sys, time
from pathlib import Path
from typing import List, Dict

import boto3
from botocore.config import Config
from dotenv import load_dotenv


def env(name: str, required: bool = True) -> str:
    v = os.getenv(name)
    if required and not v:
        print(f"[ERROR] Missing required env var {name}", file=sys.stderr)
        sys.exit(1)
    return v or ""


def read_tickers(csv_path: Path) -> List[str]:
    if not csv_path.exists():
        print(f"[ERROR] tickers.csv not found at {csv_path.resolve()}", file=sys.stderr)
        sys.exit(1)
    syms: List[str] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        for row in r:
            if not row: continue
            s = row[0].strip().upper()
            if not s or s in {"TICKER","SYMBOL"}: continue
            syms.append(s)
    if not syms:
        print("[WARN] tickers.csv was empty")
    return syms


def stub_row(sym: str) -> Dict[str, object]:
    # Keys align with your WP table defaults
    return {
        "symbol": sym,
        "name": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": None,
        "date": None
    }


def main():
    load_dotenv()

    # --- config
    account_id   = env("R2_ACCOUNT_ID")
    endpoint_url = env("R2_ENDPOINT_URL")
    bucket       = env("R2_BUCKET")
    access_key   = env("R2_ACCESS_KEY_ID")
    secret_key   = env("R2_SECRET_ACCESS_KEY")
    public_base  = env("R2_PUBLIC_BASE", required=False)
    dry_run      = ("--dry-run" in sys.argv)

    print("== R2 config =====================")
    print("ACCOUNT   :", account_id[:6] + "…")
    print("ENDPOINT  :", endpoint_url)
    print("BUCKET    :", bucket)
    print("PUBLIC    :", public_base or "(none)")
    print("MODE      :", "DRY RUN" if dry_run else "UPLOAD")
    print("==================================")

    if "YOUR_ACCOUNT_ID" in endpoint_url:
        print("[ERROR] R2_ENDPOINT_URL still has placeholder. Fix it.", file=sys.stderr)
        sys.exit(1)

    # R2 client
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(s3={"addressing_style": "virtual"})
    )

    # --- input
    tickers = read_tickers(Path("tickers.csv"))
    if not tickers:
        return

    # --- per-ticker stubs
    uploaded = 0
    for sym in tickers:
        key  = f"ohlc/{sym}.json"
        body = json.dumps(stub_row(sym), separators=(",", ":"), ensure_ascii=False)

        if dry_run:
            url = f"{public_base.rstrip('/')}/{key}" if public_base else f"s3://{bucket}/{key}"
            print(f"[DRY] would PUT {key} -> {url}")
        else:
            data = io.BytesIO(body.encode("utf-8"))
            s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
            uploaded += 1
            if uploaded % 100 == 0:
                print(f"Uploaded {uploaded}/{len(tickers)}...")

    # --- aggregate latest_sp500.json (array of rows)
    latest_rows = [stub_row(s) for s in tickers]
    latest_body = json.dumps(latest_rows, separators=(",", ":"), ensure_ascii=False)
    latest_key  = "ohlc/latest_sp500.json"

    manifest = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": len(tickers),
        "files": ["ohlc/latest_sp500.json"],
    }
    manifest_body = json.dumps(manifest, separators=(",", ":"))

    if dry_run:
        print(f"[DRY] would PUT {latest_key} (rows={len(tickers)})")
        print(f"[DRY] would PUT ohlc/manifest.json")
    else:
        s3.put_object(Bucket=bucket, Key=latest_key, Body=latest_body.encode("utf-8"),
                      ContentType="application/json")
        s3.put_object(Bucket=bucket, Key="ohlc/manifest.json",
                      Body=manifest_body.encode("utf-8"),
                      ContentType="application/json")
        print(f"[+] Uploaded {uploaded} per-ticker stubs.")
        print(f"[+] Uploaded {latest_key} with {len(tickers)} rows.")
        if public_base:
            print("Example:", f"{public_base.rstrip('/')}/{latest_key}")

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(130)
