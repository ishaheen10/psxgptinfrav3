#!/usr/bin/env python3
"""
Update filing_period for annual rows in Cloudflare D1 using the fiscal manifest.

This script loads the ticker -> fiscal_period mapping (month-day suffix),
builds a single UPDATE statement that patches every annual record, and posts
it to the Cloudflare D1 query endpoint. Run it whenever the manifest changes
instead of re-uploading the entire table.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


DEFAULT_MANIFEST = Path("artifacts/stage2/fiscal_year_end_manifest.json")
DEFAULT_TABLE = "financial_documents"


def load_manifest(path: Path) -> dict[str, str]:
    if not path.exists():
        raise SystemExit(f"Manifest not found: {path}")
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict) or not data:
        raise SystemExit(f"Manifest {path} missing ticker entries.")
    return {ticker.upper(): suffix for ticker, suffix in data.items()}


def build_update_sql(table: str, mapping: dict[str, str]) -> str:
    values_clause = ",\n        ".join(f"('{ticker}', '{suffix}')" for ticker, suffix in sorted(mapping.items()))
    return f"""
WITH fiscal(ticker, suffix) AS (
        VALUES
        {values_clause}
)
UPDATE {table}
SET filing_period = printf('%04d-%s', filing_year, (SELECT suffix FROM fiscal WHERE fiscal.ticker = {table}.ticker))
WHERE filing_type = 'annual'
  AND ticker IN (SELECT ticker FROM fiscal);
""".strip()


def post_to_d1(endpoint: str, api_token: str, sql: str) -> dict:
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    payload = {"sql": sql}
    response = requests.post(endpoint, json=payload, headers=headers, timeout=60)
    try:
        data = response.json()
    except ValueError as exc:
        raise SystemExit(f"D1 response not JSON: {exc}\nBody: {response.text[:200]}") from exc
    if not response.ok or not data.get("success", True):
        raise SystemExit(f"D1 update failed: {data.get('errors') or response.text}")
    return data


def resolve_env() -> tuple[str, str, str]:
    load_dotenv()
    account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")
    database_id = os.getenv("CLOUDFLARE_D1_DATABASE_ID")
    api_token = os.getenv("CLOUDFLARE_API_TOKEN")
    if not all([account_id, database_id, api_token]):
        raise SystemExit("Missing Cloudflare credentials. Set CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_D1_DATABASE_ID, and CLOUDFLARE_API_TOKEN.")
    return account_id, database_id, api_token


def main() -> None:
    load_dotenv()
    manifest_path = Path(os.getenv("FISCAL_MANIFEST_PATH", DEFAULT_MANIFEST)).expanduser()
    mapping = load_manifest(manifest_path)
    table_name = os.getenv("FINANCIAL_DOCS_TABLE", DEFAULT_TABLE)
    sql = build_update_sql(table_name, mapping)
    dry = os.getenv("DRY_RUN", "").strip().lower() in {"1", "true", "yes"}
    if dry:
        print(sql)
        return

    account_id, database_id, api_token = resolve_env()
    endpoint = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    result = post_to_d1(endpoint, api_token, sql)
    rows_reported = "unknown"
    payload = result.get("result")
    if isinstance(payload, dict):
        rows_reported = payload.get("changes", rows_reported)
    elif isinstance(payload, list) and payload:
        last_entry = payload[-1]
        if isinstance(last_entry, dict):
            rows_reported = last_entry.get("meta", {}).get("changes", rows_reported)
    print(f"Cloudflare D1 filing_period update complete ({rows_reported} rows affected).")


if __name__ == "__main__":
    main()
