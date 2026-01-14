#!/usr/bin/env python3
"""
Calculate 52-week high/low for market_data table.

Uses a rolling 252 trading day window (approximately 1 year).

Usage:
    python3 calculate_52week.py [--ticker SYMBOL]
"""

import subprocess
import json
import argparse
from pathlib import Path
from collections import defaultdict

# Output file for SQL updates
OUTPUT_FILE = Path(__file__).parent.parent.parent / "52week_updates.sql"
BATCH_SIZE = 500

# 252 trading days ≈ 1 year
TRADING_DAYS_PER_YEAR = 252


def fetch_all_prices() -> dict:
    """Fetch all prices from market_data, grouped by symbol."""
    print("Fetching prices from market_data...")

    cmd = [
        "npx", "wrangler", "d1", "execute", "psx", "--remote",
        "--command", "SELECT symbol, date, closing_price FROM market_data ORDER BY symbol, date"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return {}

    # Parse JSON output
    output = result.stdout
    json_start = output.find('[')
    if json_start == -1:
        print("No JSON found in output")
        return {}

    data = json.loads(output[json_start:])
    results = data[0].get("results", [])

    # Group by symbol
    prices_by_symbol = defaultdict(list)
    for row in results:
        symbol = row["symbol"]
        date = row["date"]
        price = row["closing_price"]
        if price is not None:
            prices_by_symbol[symbol].append((date, price))

    print(f"  Loaded {len(results)} price records for {len(prices_by_symbol)} symbols")
    return dict(prices_by_symbol)


def calculate_52week_values(prices: list) -> list:
    """
    Calculate 52-week high/low for each date.

    prices: list of (date, price) tuples, sorted by date ascending
    returns: list of (date, week_52_high, week_52_low) tuples
    """
    results = []

    for i, (date, price) in enumerate(prices):
        # Look back up to 252 trading days
        start_idx = max(0, i - TRADING_DAYS_PER_YEAR + 1)
        window = prices[start_idx:i + 1]

        window_prices = [p for _, p in window]
        week_52_high = max(window_prices)
        week_52_low = min(window_prices)

        results.append((date, week_52_high, week_52_low))

    return results


def generate_updates(symbol: str, values: list) -> list:
    """Generate SQL UPDATE statements."""
    statements = []
    for date, high, low in values:
        sql = f"UPDATE market_data SET week_52_high = {high:.2f}, week_52_low = {low:.2f} WHERE symbol = '{symbol}' AND date = '{date}';"
        statements.append(sql)
    return statements


def execute_batch(statements: list, batch_num: int) -> bool:
    """Execute a batch of statements via wrangler."""
    sql = "\n".join(statements)
    temp_file = Path(f"/tmp/52week_batch_{batch_num}.sql")

    with open(temp_file, "w") as f:
        f.write(sql)

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
    parser = argparse.ArgumentParser(description="Calculate 52-week high/low")
    parser.add_argument("--ticker", type=str, help="Single ticker to process")
    parser.add_argument("--dry-run", action="store_true", help="Generate SQL without executing")
    args = parser.parse_args()

    # Fetch all prices
    prices_by_symbol = fetch_all_prices()
    if not prices_by_symbol:
        print("No prices found")
        return

    # Filter to single ticker if specified
    if args.ticker:
        ticker = args.ticker.upper()
        if ticker not in prices_by_symbol:
            print(f"Ticker {ticker} not found")
            return
        prices_by_symbol = {ticker: prices_by_symbol[ticker]}

    # Calculate 52-week values and generate updates
    print(f"\nCalculating 52-week high/low for {len(prices_by_symbol)} symbols...")
    all_statements = []

    for symbol, prices in sorted(prices_by_symbol.items()):
        values = calculate_52week_values(prices)
        statements = generate_updates(symbol, values)
        all_statements.extend(statements)

    print(f"  Generated {len(all_statements)} UPDATE statements")

    if args.dry_run:
        # Write to file
        with open(OUTPUT_FILE, "w") as f:
            f.write(f"-- 52-week high/low updates\n")
            f.write(f"-- Total: {len(all_statements)} statements\n\n")
            for stmt in all_statements:
                f.write(stmt + "\n")
        print(f"  Wrote to {OUTPUT_FILE}")
        return

    # Execute in batches
    print(f"\nExecuting {len(all_statements)} updates in batches of {BATCH_SIZE}...")

    batches = []
    for i in range(0, len(all_statements), BATCH_SIZE):
        batches.append(all_statements[i:i + BATCH_SIZE])

    success = 0
    failed = 0

    for i, batch in enumerate(batches):
        print(f"  Batch {i + 1}/{len(batches)} ({len(batch)} statements)...", end=" ", flush=True)
        if execute_batch(batch, i):
            print("OK")
            success += 1
        else:
            print("FAILED")
            failed += 1

    print(f"\nDone! Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()
