#!/usr/bin/env python3
"""
Step 6: Upload document pages to Cloudflare D1.

Generates SQL and uploads compiled JSONL data to the `financial_documents_new` table.
After verification, rename the table to replace the old one.

Input:  database_jsonl_compiled/**/*.jsonl (or --input <dir>)
Output: artifacts/stage5/step6_documents_upload.sql + upload to D1

Usage:
    python pipeline/stage5_publish/Step6_UploadDocuments.py              # Generate SQL + upload
    python pipeline/stage5_publish/Step6_UploadDocuments.py --sql-only   # Generate SQL only
    python pipeline/stage5_publish/Step6_UploadDocuments.py -y           # Skip confirmation
    python pipeline/stage5_publish/Step6_UploadDocuments.py --input database_jsonl_delta
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Sequence

from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "database_jsonl_compiled"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage5"
OUTPUT_FILE = ARTIFACTS_DIR / "step6_documents_upload.sql"

# D1 database name
D1_DATABASE = "psx"

# Table name - using _new suffix for safe swap
TABLE_NAME = "financial_documents_new"

COLUMNS: Sequence[str] = (
    "ticker",
    "industry",
    "filing_type",
    "filing_period",
    "filing_year",
    "section_tags",
    "pg",
    "jpg_path",
    "summary",
    "text",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload document pages to D1.")
    parser.add_argument(
        "--input", default=str(INPUT_DIR), help="Root directory for compiled JSONLs."
    )
    parser.add_argument("--output", default=str(OUTPUT_FILE), help="Output SQL file.")
    parser.add_argument("--sql-only", action="store_true", help="Generate SQL only, don't upload.")
    parser.add_argument("--upload-only", action="store_true", help="Upload existing SQL, don't regenerate.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for uploads.")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt.")
    parser.add_argument("--dry-run", action="store_true", help="Parse files but skip SQL generation.")
    return parser.parse_args()


def normalize_year(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    return value


def normalize_page(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    return value


def normalize_ticker(value: str | None, fallback: str = "") -> str:
    candidate = value or fallback or ""
    return candidate.upper()


# Max text length to avoid SQLITE_TOOBIG errors (D1 has ~1MB statement limit)
MAX_TEXT_LENGTH = 50000  # ~50KB per text field


def normalize_record(doc: Dict, ticker_hint: str) -> Dict:
    ticker = normalize_ticker(doc.get("ticker"), ticker_hint)
    filing_year = normalize_year(doc.get("filing_year"))
    pg = normalize_page(doc.get("pg") or doc.get("page_number"))

    # Truncate text if too long
    text = doc.get("text") or ""
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH] + "\n...[truncated]"

    summary = doc.get("summary") or ""
    if len(summary) > MAX_TEXT_LENGTH:
        summary = summary[:MAX_TEXT_LENGTH] + "...[truncated]"

    return {
        "ticker": ticker,
        "industry": doc.get("industry"),
        "filing_type": doc.get("filing_type"),
        "filing_period": doc.get("filing_period"),
        "filing_year": filing_year,
        "section_tags": doc.get("section_tags"),
        "pg": pg,
        "jpg_path": doc.get("jpg_path"),
        "summary": summary,
        "text": text,
    }


def load_records(root: Path) -> List[Dict]:
    records: List[Dict] = []
    files = sorted(root.rglob("*.jsonl"))
    for file_path in tqdm(files, desc="Reading JSONLs"):
        relative = file_path.relative_to(root)
        parts = relative.parts
        ticker_guess = parts[0] if parts else ""
        with file_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record = normalize_record(doc, ticker_guess)
                records.append(record)
    return records


def escape_sql(value) -> str:
    """Escape value for SQL string literal."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    # Escape single quotes by doubling them
    s = str(value).replace("'", "''")
    # Replace newlines with escaped newlines for SQL
    s = s.replace("\n", "\\n").replace("\r", "\\r")
    return f"'{s}'"


def generate_insert(record: Dict) -> str:
    """Generate INSERT statement for a record."""
    values = []
    for col in COLUMNS:
        val = record.get(col)
        values.append(escape_sql(val))

    cols_sql = ", ".join(f'"{c}"' for c in COLUMNS)
    vals_sql = ", ".join(values)

    return f"INSERT INTO {TABLE_NAME} ({cols_sql}) VALUES ({vals_sql});"


def generate_sql(input_root: Path, output_path: Path) -> int:
    """Generate SQL file from JSONL files. Returns record count."""
    records = load_records(input_root)
    print(f"Total records collected: {len(records):,}")

    if not records:
        print("No records to upload.")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing SQL to: {output_path}")

    with open(output_path, "w") as f_out:
        f_out.write(f"-- {TABLE_NAME} upload\n")
        f_out.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f_out.write(f"-- Records: {len(records):,}\n\n")

        for i, record in enumerate(records):
            sql = generate_insert(record)
            f_out.write(sql + "\n")

            if (i + 1) % 50000 == 0:
                print(f"  Processed {i + 1:,} records...")

    print(f"Generated: {output_path} ({len(records):,} rows)")
    return len(records)


def upload_batch(sql_content: str, batch_num: int, total_batches: int) -> bool:
    """Upload a batch of SQL statements."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(sql_content)
        temp_file = f.name

    try:
        result = subprocess.run(
            ['npx', 'wrangler', 'd1', 'execute', D1_DATABASE, '--remote', f'--file={temp_file}'],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )

        # Check for success in output (wrangler returns JSON with success field)
        if '"success": true' in result.stdout or '"success":true' in result.stdout:
            print(f"  Batch {batch_num}/{total_batches} uploaded")
            return True

        if result.returncode != 0 or '"success": false' in result.stdout:
            print(f"  Batch {batch_num}/{total_batches} FAILED")
            # Show actual error, filtering out Node warnings
            error_lines = [l for l in result.stderr.split('\n') if 'ExperimentalWarning' not in l and l.strip()]
            if error_lines:
                print(f"  Error: {' '.join(error_lines[:5])}")
            else:
                print(f"  Output: {result.stdout[:500]}")
            return False

        print(f"  Batch {batch_num}/{total_batches} uploaded")
        return True

    finally:
        os.unlink(temp_file)


def upload_sql(sql_file: Path, batch_size: int = 5000) -> bool:
    """Upload SQL file to D1 in batches."""
    print(f"Uploading: {sql_file}")
    print(f"Batch size: {batch_size}")

    insert_statements = []

    with open(sql_file) as f:
        for line in f:
            line_stripped = line.strip()
            if line_stripped.startswith('INSERT'):
                insert_statements.append(line_stripped)

    if not insert_statements:
        print("No data to upload")
        return True

    total_batches = (len(insert_statements) + batch_size - 1) // batch_size
    print(f"Uploading {len(insert_statements):,} rows in {total_batches} batches...")

    for i in range(0, len(insert_statements), batch_size):
        batch = insert_statements[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        batch_sql = '\n'.join(batch)

        if not upload_batch(batch_sql, batch_num, total_batches):
            print(f"\nUpload failed at batch {batch_num}")
            return False

    print(f"Upload complete: {len(insert_statements):,} rows")
    return True


def main() -> None:
    args = parse_args()
    input_root = Path(args.input).expanduser()
    output_path = Path(args.output)

    print("=" * 60)
    print("STEP 6: UPLOAD DOCUMENTS TO D1")
    print("=" * 60)
    print(f"Database: {D1_DATABASE}")
    print(f"Table:    {TABLE_NAME}")
    print(f"Input:    {input_root}")
    print()

    if not input_root.exists():
        raise SystemExit(f"Input directory not found: {input_root}")

    # Generate SQL
    row_count = 0
    if not args.upload_only:
        if args.dry_run:
            records = load_records(input_root)
            print(f"Dry run complete. Would upload {len(records):,} records.")
            return

        row_count = generate_sql(input_root, output_path)
        if row_count == 0:
            return

    if args.sql_only:
        print()
        print("SQL generated. To upload, run:")
        print(f"  python pipeline/stage5_publish/Step6_UploadDocuments.py --upload-only")
        return

    # Confirm upload
    if not args.yes:
        print()
        confirm = input(f"Upload {row_count:,} documents to {TABLE_NAME}? (y/N) ")
        if confirm.lower() != 'y':
            print("Aborted.")
            return

    # Upload
    if not output_path.exists():
        raise SystemExit(f"SQL file not found: {output_path}")

    print()
    success = upload_sql(output_path, args.batch_size)

    print()
    print("=" * 60)
    if success:
        print("UPLOAD COMPLETE")
        print("=" * 60)
        print()
        print("Next steps:")
        print(f"  1. Verify data in {TABLE_NAME}")
        print("  2. Drop old table: DROP TABLE financial_documents;")
        print(f"  3. Rename: ALTER TABLE {TABLE_NAME} RENAME TO financial_documents;")
    else:
        print("UPLOAD FAILED")
        print("=" * 60)


if __name__ == "__main__":
    main()
