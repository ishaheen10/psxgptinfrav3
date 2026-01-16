#!/usr/bin/env python3
"""
Step 3: Upload P&L Data to Cloudflare D1

Generates SQL and uploads flattened P&L data to the `financial_statements` table.

Input:  artifacts/stage4/pl_flat.jsonl
Output: artifacts/stage4/pl_upload.sql + upload to D1

Usage:
    python3 Step3_UploadPL.py                    # Generate SQL + upload
    python3 Step3_UploadPL.py --sql-only         # Generate SQL only
    python3 Step3_UploadPL.py --upload-only      # Upload existing SQL
    python3 Step3_UploadPL.py --batch-size 5000  # Custom batch size
"""

import argparse
import json
import subprocess
import tempfile
import os
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "artifacts" / "stage4" / "pl_flat.jsonl"
OUTPUT_SQL = PROJECT_ROOT / "artifacts" / "stage4" / "pl_upload.sql"

# D1 database name
D1_DATABASE = "psx"

# Table name
TABLE_NAME = "financial_statements"

# Columns matching the financial_statements schema
COLUMNS = [
    "ticker",
    "company_name",
    "industry",
    "unit_type",
    "period_type",
    "period_end",
    "period_duration",
    "fiscal_year",
    "section",
    "statement_type",
    "canonical_field",
    "original_name",
    "value",
    "method",
    "source_file"
]


def escape_sql(value) -> str:
    """Escape value for SQL string literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value).replace("'", "''")
    return f"'{s}'"


def generate_insert(row: dict) -> str:
    """Generate INSERT statement for a row."""
    values = []
    for col in COLUMNS:
        val = row.get(col)
        values.append(escape_sql(val))

    cols_sql = ", ".join(f'"{c}"' for c in COLUMNS)
    vals_sql = ", ".join(values)

    return f"INSERT INTO {TABLE_NAME} ({cols_sql}) VALUES ({vals_sql});"


def generate_sql(input_file: Path, output_file: Path) -> int:
    """Generate SQL file from JSONL. Returns row count."""
    print(f"Reading: {input_file}")

    row_count = 0
    with open(input_file) as f_in, open(output_file, 'w') as f_out:
        # Write header
        f_out.write(f"-- {TABLE_NAME} P&L upload\n")
        f_out.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f_out.write(f"-- Source: {input_file.name}\n\n")

        # Write inserts (table already created by user)
        for line in f_in:
            if not line.strip():
                continue
            row = json.loads(line)
            sql = generate_insert(row)
            f_out.write(sql + "\n")
            row_count += 1

            if row_count % 25000 == 0:
                print(f"  Processed {row_count:,} rows...")

    print(f"Generated: {output_file} ({row_count:,} rows)")
    return row_count


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

        if result.returncode != 0:
            print(f"  Batch {batch_num}/{total_batches} FAILED")
            print(f"  Error: {result.stderr[:500]}")
            return False

        print(f"  Batch {batch_num}/{total_batches} uploaded")
        return True

    finally:
        os.unlink(temp_file)


def upload_sql(sql_file: Path, batch_size: int = 5000) -> bool:
    """Upload SQL file to D1 in batches."""
    print(f"\nUploading: {sql_file}")
    print(f"Batch size: {batch_size}")

    # Collect INSERT statements only (table already exists)
    insert_statements = []

    with open(sql_file) as f:
        for line in f:
            line_stripped = line.strip()
            if line_stripped.startswith('INSERT'):
                insert_statements.append(line_stripped)

    # Upload INSERT statements in batches
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
            print("You can resume by running with --upload-only after fixing the issue")
            return False

    print(f"\nUpload complete: {len(insert_statements):,} rows")
    return True


def main():
    parser = argparse.ArgumentParser(description="Upload P&L data to D1")
    parser.add_argument("--sql-only", action="store_true", help="Generate SQL only, don't upload")
    parser.add_argument("--upload-only", action="store_true", help="Upload existing SQL, don't regenerate")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for uploads")
    args = parser.parse_args()

    # Generate SQL
    if not args.upload_only:
        if not INPUT_FILE.exists():
            print(f"ERROR: Input file not found: {INPUT_FILE}")
            print("Run Step1_FlattenPL.py first.")
            return 1

        row_count = generate_sql(INPUT_FILE, OUTPUT_SQL)

        if row_count == 0:
            print("No data to upload")
            return 0

    # Upload
    if not args.sql_only:
        if not OUTPUT_SQL.exists():
            print(f"ERROR: SQL file not found: {OUTPUT_SQL}")
            print("Run without --upload-only first.")
            return 1

        print()
        print("=" * 60)
        print("UPLOADING TO D1")
        print("=" * 60)
        print(f"Database: {D1_DATABASE}")
        print(f"Table: {TABLE_NAME}")
        print()

        confirm = input("Proceed with upload? (y/N) ")
        if confirm.lower() != 'y':
            print("Aborted. SQL file saved to:", OUTPUT_SQL)
            return 0

        success = upload_sql(OUTPUT_SQL, args.batch_size)
        if not success:
            return 1

    print()
    print("=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"SQL file: {OUTPUT_SQL}")
    if not args.sql_only:
        print("Data uploaded to D1 successfully")

    return 0


if __name__ == "__main__":
    exit(main())
