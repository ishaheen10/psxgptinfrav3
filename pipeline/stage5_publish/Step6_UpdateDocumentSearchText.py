#!/usr/bin/env python3
"""
Update search_text in Cloudflare D1 for financial_documents_new.

This script reads generated page-wise search_text files and produces SQL updates keyed by:
  ticker + filing_type + filing_period + pg

It optionally uploads the SQL in batches and then rebuilds filings_fts_new so MATCH uses
the refreshed search_text values.

Usage:
  python pipeline/stage5_publish/Step6_UpdateDocumentSearchText.py --sql-only
  python pipeline/stage5_publish/Step6_UpdateDocumentSearchText.py
  python pipeline/stage5_publish/Step6_UpdateDocumentSearchText.py --search-root markdown_search_text_v5_full -y
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
import re

from Step4_CompileDocuments import (
    derive_filing_type_and_year,
    extract_filing_period,
    load_ticker_metadata,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SEARCH_ROOT = PROJECT_ROOT / "markdown_search_text_v5_full"
TICKER_META_PATH = PROJECT_ROOT / "tickers100.json"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage5"
OUTPUT_FILE = ARTIFACTS_DIR / "step6_update_document_search_text.sql"

D1_DATABASE = "psx"
DOCS_TABLE = "financial_documents_new"
FTS_TABLE = "filings_fts_new"

PAGE_FILE_PATTERN = re.compile(r"page_(\d+)\.md$", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update financial_documents_new.search_text in D1.")
    parser.add_argument("--search-root", default=str(DEFAULT_SEARCH_ROOT), help="Root directory of generated search_text files.")
    parser.add_argument("--output", default=str(OUTPUT_FILE), help="Output SQL file.")
    parser.add_argument("--sql-only", action="store_true", help="Generate SQL only, do not upload.")
    parser.add_argument("--upload-only", action="store_true", help="Upload existing SQL, do not regenerate.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Statements per upload batch.")
    parser.add_argument("--skip-fts-rebuild", action="store_true", help="Skip filings_fts_new rebuild statement.")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt.")
    return parser.parse_args()


def escape_sql(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    text = text.replace("\n", "\\n").replace("\r", "\\r")
    return f"'{text}'"


def normalize_search_text(text: str) -> str:
    return " ".join(text.strip().split())


def iter_updates(search_root: Path):
    ticker_meta = load_ticker_metadata(TICKER_META_PATH)
    for page_file in sorted(search_root.rglob("page_*.md")):
        relative = page_file.relative_to(search_root)
        parts = relative.parts
        if len(parts) < 4:
            continue

        ticker, folder, report_stem = parts[0], parts[1], parts[2]
        match = PAGE_FILE_PATTERN.search(page_file.name)
        if not match:
            continue

        pg = int(match.group(1))
        search_text = normalize_search_text(page_file.read_text(encoding="utf-8", errors="ignore"))

        ticker_upper = ticker.upper()
        ticker_info = ticker_meta.get(ticker_upper, {})
        filing_type, filing_year_hint = derive_filing_type_and_year(folder, report_stem)
        fiscal_period_suffix = (ticker_info.get("fiscal_period") or "").strip()
        filing_period = extract_filing_period(
            report_stem,
            folder,
            ticker_upper,
            filing_type,
            filing_year_hint,
            fiscal_period_suffix,
        )

        yield {
            "ticker": ticker_upper,
            "filing_type": filing_type,
            "filing_period": filing_period,
            "pg": pg,
            "search_text": search_text,
        }


def generate_update_sql(record: dict) -> str:
    return (
        f"UPDATE {DOCS_TABLE} "
        f"SET search_text = {escape_sql(record['search_text'])} "
        f"WHERE ticker = {escape_sql(record['ticker'])} "
        f"AND filing_type = {escape_sql(record['filing_type'])} "
        f"AND filing_period = {escape_sql(record['filing_period'])} "
        f"AND pg = {record['pg']};"
    )


def generate_sql(search_root: Path, output_path: Path, rebuild_fts: bool) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(f"-- search_text update for {DOCS_TABLE}\n")
        handle.write(f"-- generated: {datetime.now().isoformat()}\n")
        handle.write(f"-- source root: {search_root}\n\n")

        for record in iter_updates(search_root):
            handle.write(generate_update_sql(record) + "\n")
            count += 1
            if count % 50000 == 0:
                print(f"  Generated {count:,} UPDATE statements...")

        if rebuild_fts:
            handle.write(f"\nINSERT INTO {FTS_TABLE}({FTS_TABLE}) VALUES('rebuild');\n")

    print(f"Generated {count:,} UPDATE statements in {output_path}")
    if rebuild_fts:
        print(f"Included FTS rebuild for {FTS_TABLE}")
    return count


def upload_batch(statements: list[str], batch_num: int, total_batches: int) -> bool:
    sql = "\n".join(statements)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as temp:
        temp.write(sql)
        temp_path = Path(temp.name)

    try:
        result = subprocess.run(
            ["npx", "wrangler", "d1", "execute", D1_DATABASE, "--remote", f"--file={temp_path}"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        if '"success": true' in result.stdout or '"success":true' in result.stdout:
            print(f"  Batch {batch_num}/{total_batches} uploaded")
            return True
        print(f"  Batch {batch_num}/{total_batches} FAILED")
        if result.stderr.strip():
            print(result.stderr.strip()[:500])
        elif result.stdout.strip():
            print(result.stdout.strip()[:500])
        return False
    finally:
        temp_path.unlink(missing_ok=True)


def upload_sql(sql_file: Path, batch_size: int) -> bool:
    statements: list[str] = []
    with sql_file.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            if line.endswith(";"):
                statements.append(line)

    if not statements:
        print("No SQL statements found.")
        return True

    total_batches = (len(statements) + batch_size - 1) // batch_size
    print(f"Uploading {len(statements):,} statements in {total_batches} batches...")

    for index in range(0, len(statements), batch_size):
        batch = statements[index:index + batch_size]
        batch_num = index // batch_size + 1
        if not upload_batch(batch, batch_num, total_batches):
            return False
    return True


def main() -> None:
    args = parse_args()
    search_root = Path(args.search_root).expanduser()
    output_path = Path(args.output).expanduser()

    print("=" * 60)
    print("STEP 6X: UPDATE DOCUMENT SEARCH TEXT")
    print("=" * 60)
    print(f"Search root: {search_root}")
    print(f"Output SQL:  {output_path}")
    print(f"Database:    {D1_DATABASE}")
    print(f"Table:       {DOCS_TABLE}")
    print()

    if not search_root.exists():
        raise SystemExit(f"Search root not found: {search_root}")

    statement_count = 0
    if not args.upload_only:
        statement_count = generate_sql(search_root, output_path, rebuild_fts=not args.skip_fts_rebuild)
        if statement_count == 0:
            return

    if args.sql_only:
        print("SQL generated only. No upload performed.")
        return

    if not output_path.exists():
        raise SystemExit(f"SQL file not found: {output_path}")

    if not args.yes:
        confirm = input(f"Upload UPDATE statements from {output_path.name} to {DOCS_TABLE}? (y/N) ")
        if confirm.lower() != "y":
            print("Aborted.")
            return

    success = upload_sql(output_path, args.batch_size)

    print()
    print("=" * 60)
    if success:
        print("UPDATE COMPLETE")
    else:
        print("UPDATE FAILED")
    print("=" * 60)


if __name__ == "__main__":
    main()
