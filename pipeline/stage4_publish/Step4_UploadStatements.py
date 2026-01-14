#!/usr/bin/env python3
"""
Step 3: Upload Unified Financial Statements to D1

Generates SQL and uploads to the `financial_statements` table.

Input:  statements_normalized.jsonl (output from Step2b_NormalizeUnits)
Output: financial_statements.sql + upload to D1

Usage:
    python3 Step3_UploadStatements.py                    # Generate SQL + upload
    python3 Step3_UploadStatements.py --sql-only         # Generate SQL only
    python3 Step3_UploadStatements.py --upload-only      # Upload existing SQL
    python3 Step3_UploadStatements.py --batch-size 5000  # Custom batch size
"""

import argparse
import json
import subprocess
import tempfile
import os
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "statements_normalized.jsonl"
OUTPUT_SQL = PROJECT_ROOT / "financial_statements.sql"

# Schema for financial_statements table
SCHEMA = """
DROP TABLE IF EXISTS financial_statements;

CREATE TABLE financial_statements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    company_name TEXT,
    industry TEXT,
    unit_type TEXT,
    period_type TEXT NOT NULL,
    period_end TEXT NOT NULL,
    period_duration TEXT,
    period_year INTEGER,
    section TEXT,
    statement_type TEXT NOT NULL,
    canonical_field TEXT NOT NULL,
    value REAL,
    mapping_type TEXT,
    original_name TEXT,
    original_names TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fs_ticker ON financial_statements(ticker);
CREATE INDEX idx_fs_period ON financial_statements(period_type, period_end);
CREATE INDEX idx_fs_statement ON financial_statements(statement_type);
CREATE INDEX idx_fs_field ON financial_statements(canonical_field);
CREATE INDEX idx_fs_ticker_period ON financial_statements(ticker, period_type, period_end);
CREATE INDEX idx_fs_ticker_field ON financial_statements(ticker, canonical_field);
"""

COLUMNS = [
    "ticker",
    "company_name",
    "industry",
    "unit_type",
    "period_type",
    "period_end",
    "period_duration",
    "period_year",
    "section",
    "statement_type",
    "canonical_field",
    "value",
    "mapping_type",
    "original_name",
    "original_names"
]


def escape_sql(value) -> str:
    """Escape value for SQL string literal."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        value = json.dumps(value)
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

    return f"INSERT INTO financial_statements ({cols_sql}) VALUES ({vals_sql});"


def generate_sql(input_file: Path, output_file: Path) -> int:
    """Generate SQL file from JSONL. Returns row count."""
    print(f"Reading: {input_file}")

    row_count = 0
    with open(input_file) as f_in, open(output_file, 'w') as f_out:
        # Write header
        f_out.write("-- financial_statements upload\n")
        f_out.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f_out.write("-- Schema and data\n\n")

        # Write schema
        f_out.write(SCHEMA)
        f_out.write("\n\n")

        # Write inserts
        for line in f_in:
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
            ['npx', 'wrangler', 'd1', 'execute', 'psx', '--remote', f'--file={temp_file}'],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )

        if result.returncode != 0:
            print(f"  Batch {batch_num}/{total_batches} FAILED: {result.stderr[:200]}")
            return False

        print(f"  Batch {batch_num}/{total_batches} uploaded")
        return True

    finally:
        os.unlink(temp_file)


def upload_sql(sql_file: Path, batch_size: int = 5000):
    """Upload SQL file to D1 in batches."""
    print(f"\nUploading: {sql_file}")
    print(f"Batch size: {batch_size}")

    # First, execute schema (everything before INSERT statements)
    schema_lines = []
    insert_statements = []

    with open(sql_file) as f:
        for line in f:
            line_stripped = line.strip()
            if line_stripped.startswith('INSERT'):
                insert_statements.append(line_stripped)
            elif line_stripped and not line_stripped.startswith('--'):
                schema_lines.append(line)

    # Upload schema first
    if schema_lines:
        print("Uploading schema...")
        schema_sql = ''.join(schema_lines)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(schema_sql)
            temp_file = f.name

        try:
            result = subprocess.run(
                ['npx', 'wrangler', 'd1', 'execute', 'psx', '--remote', f'--file={temp_file}'],
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT
            )
            if result.returncode != 0:
                print(f"Schema upload FAILED: {result.stderr}")
                return
            print("  Schema uploaded successfully")
        finally:
            os.unlink(temp_file)

    # Upload data in batches
    print(f"\nUploading {len(insert_statements):,} rows...")
    total_batches = (len(insert_statements) + batch_size - 1) // batch_size

    success = 0
    failed = 0

    for i in range(0, len(insert_statements), batch_size):
        batch_num = (i // batch_size) + 1
        batch = insert_statements[i:i + batch_size]
        sql_content = '\n'.join(batch)

        if upload_batch(sql_content, batch_num, total_batches):
            success += 1
        else:
            failed += 1

    print(f"\nDone: {success} batches succeeded, {failed} failed")
    print(f"Total rows uploaded: {success * batch_size if failed == 0 else 'partial'}")


def main():
    parser = argparse.ArgumentParser(description="Upload unified statements to D1")
    parser.add_argument("--sql-only", action="store_true", help="Generate SQL only, don't upload")
    parser.add_argument("--upload-only", action="store_true", help="Upload existing SQL, don't regenerate")
    parser.add_argument("--batch-size", type=int, default=5000, help="Rows per batch")
    parser.add_argument("--input", type=Path, default=INPUT_FILE, help="Input JSONL file")
    parser.add_argument("--output", type=Path, default=OUTPUT_SQL, help="Output SQL file")
    args = parser.parse_args()

    if not args.upload_only:
        if not args.input.exists():
            print(f"Error: {args.input} not found")
            print("Run Step2_UnifyStatements.py first")
            return

        row_count = generate_sql(args.input, args.output)

    if not args.sql_only:
        if not args.output.exists():
            print(f"Error: {args.output} not found")
            return

        upload_sql(args.output, args.batch_size)


if __name__ == "__main__":
    main()
