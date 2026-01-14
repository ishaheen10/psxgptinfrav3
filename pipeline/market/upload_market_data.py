#!/usr/bin/env python3
"""
Upload market_data SQL to D1 in batches.

Usage:
    python3 upload_market_data.py [--batch-size N] [--start N]
"""

import subprocess
import argparse
import re
from pathlib import Path

SQL_FILE = Path(__file__).parent.parent.parent / "market_data_inserts.sql"
BATCH_SIZE = 500  # Statements per batch


def read_statements(filepath: Path) -> list[str]:
    """Read SQL statements from file."""
    statements = []
    current = []

    with open(filepath) as f:
        for line in f:
            # Skip comments
            if line.startswith("--"):
                continue

            current.append(line)

            # Statement ends with semicolon
            if line.rstrip().endswith(";"):
                statements.append("".join(current))
                current = []

    return statements


def execute_batch(statements: list[str], batch_num: int) -> bool:
    """Execute a batch of statements via wrangler."""
    # Combine statements
    sql = "\n".join(statements)

    # Write to temp file
    temp_file = Path(f"/tmp/market_data_batch_{batch_num}.sql")
    with open(temp_file, "w") as f:
        f.write(sql)

    # Execute via wrangler
    cmd = [
        "npx", "wrangler", "d1", "execute", "psx",
        "--remote", f"--file={temp_file}"
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"Error in batch {batch_num}: {result.stderr[:200]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"Timeout in batch {batch_num}")
        return False
    finally:
        temp_file.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Upload market_data to D1")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--start", type=int, default=0, help="Start from batch N")
    parser.add_argument("--limit", type=int, help="Limit number of batches")
    args = parser.parse_args()

    print(f"Reading statements from {SQL_FILE}...")
    statements = read_statements(SQL_FILE)
    print(f"Total statements: {len(statements)}")

    # Split into batches
    batches = []
    for i in range(0, len(statements), args.batch_size):
        batches.append(statements[i:i + args.batch_size])

    print(f"Total batches: {len(batches)} (batch size: {args.batch_size})")

    # Apply limits
    start = args.start
    end = len(batches)
    if args.limit:
        end = min(start + args.limit, len(batches))

    print(f"Processing batches {start} to {end - 1}...")

    success = 0
    failed = 0

    for i in range(start, end):
        batch = batches[i]
        print(f"  Batch {i}/{len(batches) - 1} ({len(batch)} statements)...", end=" ", flush=True)

        if execute_batch(batch, i):
            print("OK")
            success += 1
        else:
            print("FAILED")
            failed += 1

    print(f"\nDone! Success: {success}, Failed: {failed}")
    if failed > 0:
        print(f"Resume from batch {start + success} with: --start {start + success}")


if __name__ == "__main__":
    main()
