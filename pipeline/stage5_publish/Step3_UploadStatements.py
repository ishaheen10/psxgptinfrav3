#!/usr/bin/env python3
"""
Step 3: Upload Statement Data to Cloudflare D1

Generates SQL and uploads flattened statement data (PL, BS, CF) to the `financial_statements` table.

Input:  data/flat/{pl,bs,cf}.jsonl
Output: artifacts/stage5/step3_{pl,bs,cf}_upload.sql + upload to D1

Usage:
    python3 Step3_UploadStatements.py --type pl          # Upload P&L only
    python3 Step3_UploadStatements.py --type bs          # Upload Balance Sheet only
    python3 Step3_UploadStatements.py --type cf          # Upload Cash Flow only
    python3 Step3_UploadStatements.py --type all         # Upload all statement types
    python3 Step3_UploadStatements.py --type bs --sql-only   # Generate SQL only
    python3 Step3_UploadStatements.py --type cf --batch-size 3000
"""

import argparse
import json
import subprocess
import tempfile
import os
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_FLAT_DIR = PROJECT_ROOT / "data" / "flat"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage5"

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

# Statement type configs
STATEMENT_TYPES = {
    'pl': {'input': 'pl.jsonl', 'output': 'step3_pl_upload.sql', 'name': 'Profit & Loss'},
    'bs': {'input': 'bs.jsonl', 'output': 'step3_bs_upload.sql', 'name': 'Balance Sheet'},
    'cf': {'input': 'cf.jsonl', 'output': 'step3_cf_upload.sql', 'name': 'Cash Flow'},
}


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


def generate_sql(input_file: Path, output_file: Path, statement_name: str) -> int:
    """Generate SQL file from JSONL. Returns row count."""
    print(f"Reading: {input_file}")

    row_count = 0
    with open(input_file) as f_in, open(output_file, 'w') as f_out:
        f_out.write(f"-- {TABLE_NAME} {statement_name} upload\n")
        f_out.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f_out.write(f"-- Source: {input_file.name}\n\n")

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


def process_statement_type(stmt_type: str, config: dict, args) -> bool:
    """Process a single statement type."""
    input_file = DATA_FLAT_DIR / config['input']
    output_file = ARTIFACTS_DIR / config['output']
    statement_name = config['name']

    print()
    print("=" * 60)
    print(f"{statement_name.upper()}")
    print("=" * 60)

    # Generate SQL
    if not args.upload_only:
        if not input_file.exists():
            print(f"ERROR: Input file not found: {input_file}")
            print(f"Run Step1_Flatten{stmt_type.upper()}.py first.")
            return False

        row_count = generate_sql(input_file, output_file, statement_name)

        if row_count == 0:
            print("No data to upload")
            return True

    # Upload
    if not args.sql_only:
        if not output_file.exists():
            print(f"ERROR: SQL file not found: {output_file}")
            return False

        success = upload_sql(output_file, args.batch_size)
        if not success:
            return False

    return True


def main():
    parser = argparse.ArgumentParser(description="Upload statement data to D1")
    parser.add_argument("--type", required=True, choices=['pl', 'bs', 'cf', 'all'],
                        help="Statement type to upload")
    parser.add_argument("--sql-only", action="store_true", help="Generate SQL only, don't upload")
    parser.add_argument("--upload-only", action="store_true", help="Upload existing SQL, don't regenerate")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for uploads")
    args = parser.parse_args()

    print("=" * 60)
    print("UPLOAD STATEMENTS TO D1")
    print("=" * 60)
    print(f"Database: {D1_DATABASE}")
    print(f"Table: {TABLE_NAME}")

    # Determine which types to process
    if args.type == 'all':
        types_to_process = ['pl', 'bs', 'cf']
    else:
        types_to_process = [args.type]

    # Confirm upload
    if not args.sql_only:
        print()
        print(f"Statement types: {', '.join(types_to_process)}")
        confirm = input("Proceed with upload? (y/N) ")
        if confirm.lower() != 'y':
            print("Aborted.")
            return 0

    # Process each type
    success = True
    for stmt_type in types_to_process:
        config = STATEMENT_TYPES[stmt_type]
        if not process_statement_type(stmt_type, config, args):
            success = False
            if args.type != 'all':
                break

    print()
    print("=" * 60)
    if success:
        print("COMPLETE")
    else:
        print("COMPLETED WITH ERRORS")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
