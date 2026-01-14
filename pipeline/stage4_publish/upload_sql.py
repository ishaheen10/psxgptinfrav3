#!/usr/bin/env python3
"""
Upload SQL file to D1 in batches.

Usage:
    python3 upload_sql.py financial_statements.sql
    python3 upload_sql.py financial_statements.sql --batch-size 5000
"""

import argparse
import subprocess
import tempfile
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_file", help="SQL file to upload")
    parser.add_argument("--batch-size", type=int, default=5000, help="Statements per batch")
    parser.add_argument("--skip-schema", action="store_true", help="Skip schema creation")
    args = parser.parse_args()

    sql_path = PROJECT_ROOT / args.sql_file if not Path(args.sql_file).is_absolute() else Path(args.sql_file)

    if not sql_path.exists():
        print(f"Error: {sql_path} not found")
        return

    print(f"Uploading: {sql_path}")
    print(f"Batch size: {args.batch_size}")

    # Read all statements
    statements = []
    with open(sql_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith('INSERT'):
                statements.append(line)

    print(f"Total statements: {len(statements):,}")

    # Calculate batches
    total_batches = (len(statements) + args.batch_size - 1) // args.batch_size
    print(f"Total batches: {total_batches}")

    # Upload in batches
    success = 0
    failed = 0

    for i in range(0, len(statements), args.batch_size):
        batch_num = (i // args.batch_size) + 1
        batch = statements[i:i + args.batch_size]
        sql_content = '\n'.join(batch)

        if upload_batch(sql_content, batch_num, total_batches):
            success += 1
        else:
            failed += 1

    print(f"\nDone: {success} batches succeeded, {failed} failed")


if __name__ == "__main__":
    main()
