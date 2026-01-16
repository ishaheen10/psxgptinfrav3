#!/usr/bin/env python3
"""
Step 1: Flatten P&L Data for D1 Upload

Parses extracted_pl/*.md markdown files and outputs flat JSONL rows for financial_statements table.
Preserves original_name (Source Item) from markdown tables.

Input:  data/extracted_pl/*.md
Output: artifacts/stage4/pl_flat.jsonl

Usage:
    python3 Step1_FlattenPL.py
    python3 Step1_FlattenPL.py --ticker LUCK
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTED_PL_DIR = PROJECT_ROOT / "data" / "extracted_pl"
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "stage4"
OUTPUT_FILE = OUTPUT_DIR / "pl_flat.jsonl"

# Load ticker metadata
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
if TICKERS_FILE.exists():
    with open(TICKERS_FILE) as f:
        tickers_data = json.load(f)
    TICKER_META = {t['Symbol']: t for t in tickers_data}
else:
    TICKER_META = {}

# Fields that should NOT be unit-normalized (always in rupees)
NON_NORMALIZED_FIELDS = {'eps', 'diluted_eps', 'eps_continuing', 'eps_discontinued'}


def parse_filename(filename: str) -> dict:
    """Parse filename like AABS_annual_2024_consolidated.md"""
    stem = filename.replace('.md', '')
    parts = stem.rsplit('_', 2)  # Split from right to handle tickers with underscores

    if len(parts) < 3:
        return None

    section = parts[-1]  # consolidated or unconsolidated
    period_part = parts[-2]  # 2024 or 2024-03-31

    # Everything before is ticker_periodtype
    prefix = '_'.join(parts[:-2])

    # Split prefix into ticker and period_type
    if '_annual_' in stem:
        idx = prefix.rfind('_annual')
        ticker = prefix[:idx] if idx > 0 else prefix.split('_')[0]
        period_type = 'annual'
    elif '_quarterly_' in stem:
        idx = prefix.rfind('_quarterly')
        ticker = prefix[:idx] if idx > 0 else prefix.split('_')[0]
        period_type = 'quarterly'
    else:
        # Fallback
        ticker = parts[0]
        period_type = 'quarterly' if '-' in period_part else 'annual'

    return {
        'ticker': ticker,
        'period_type': period_type,
        'period_part': period_part,
        'section': section
    }


def parse_period_column(col_header: str) -> dict:
    """Parse column header like '3M Dec 2021' or '12M Sep 2024' or '30 Sep 2024'"""
    col_header = col_header.strip()

    # Pattern: 3M Dec 2021, 12M Sep 2024
    match = re.match(r'^(\d+M)\s+(\w+)\s+(\d{4})$', col_header)
    if match:
        duration = match.group(1)
        month_str = match.group(2)
        year = int(match.group(3))

        # Convert month name to number
        months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                  'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
        month = months.get(month_str[:3], 1)

        # Get last day of month
        if month == 12:
            last_day = 31
        elif month in [4, 6, 9, 11]:
            last_day = 30
        elif month == 2:
            last_day = 29 if year % 4 == 0 else 28
        else:
            last_day = 31

        period_end = f"{year}-{month:02d}-{last_day:02d}"
        return {'period_end': period_end, 'period_duration': duration}

    # Pattern: 30 Sep 2024 (balance sheet style)
    match = re.match(r'^(\d{1,2})\s+(\w+)\s+(\d{4})$', col_header)
    if match:
        day = int(match.group(1))
        month_str = match.group(2)
        year = int(match.group(3))

        months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                  'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
        month = months.get(month_str[:3], 1)

        period_end = f"{year}-{month:02d}-{day:02d}"
        return {'period_end': period_end, 'period_duration': None}

    return None


def parse_value(val_str: str) -> float:
    """Parse value string like '1,234,567' or '(123,456)' or '-'"""
    if not val_str or val_str.strip() in ['-', '', '—', '–']:
        return None

    val_str = val_str.strip()

    # Handle parentheses as negative
    negative = False
    if val_str.startswith('(') and val_str.endswith(')'):
        negative = True
        val_str = val_str[1:-1]

    # Remove commas and whitespace
    val_str = val_str.replace(',', '').replace(' ', '')

    # Handle negative sign
    if val_str.startswith('-'):
        negative = True
        val_str = val_str[1:]

    try:
        value = float(val_str)
        return -value if negative else value
    except ValueError:
        return None


def is_valid_canonical_field(field: str) -> bool:
    """Check if field name is a valid canonical field (not an extraction artifact)."""
    if not field:
        return False
    field = field.strip().strip('*')
    if not field:
        return False
    # Reject if starts with digit
    if field[0].isdigit():
        return False
    # Reject if contains '='
    if '=' in field:
        return False
    # Reject single/double uppercase letters
    if len(field) <= 2 and field.isupper():
        return False
    # Reject if has commas
    if ',' in field:
        return False
    # Reject if has spaces
    if ' ' in field:
        return False
    # Reject if too long
    if len(field) > 60:
        return False
    return True


def parse_markdown_file(filepath: Path) -> list[dict]:
    """Parse a markdown P&L file and return list of row dicts."""
    rows = []

    filename = filepath.name
    file_info = parse_filename(filename)
    if not file_info:
        return rows

    ticker = file_info['ticker']
    period_type = file_info['period_type']
    section = file_info['section']

    meta = TICKER_META.get(ticker, {})
    company_name = meta.get("Company Name", "")
    industry = meta.get("Industry", "")
    fiscal_period = meta.get("fiscal_period", "12-31")  # MM-DD

    content = filepath.read_text()

    # Extract unit_type
    unit_match = re.search(r'UNIT_TYPE:\s*(\w+)', content)
    unit_type = unit_match.group(1) if unit_match else 'thousands'

    # Find the markdown table
    lines = content.split('\n')
    in_table = False
    headers = []
    period_columns = []  # List of {'col_idx': int, 'period_end': str, 'period_duration': str}

    for line in lines:
        line = line.strip()

        # Detect table header row
        if line.startswith('| Source Item |'):
            in_table = True
            # Parse headers
            headers = [h.strip() for h in line.split('|')[1:-1]]

            # Find period columns (skip Source Item, Canonical, Ref)
            for idx, header in enumerate(headers):
                if idx < 3:  # Skip Source Item, Canonical, Ref
                    continue
                period_info = parse_period_column(header)
                if period_info:
                    period_columns.append({
                        'col_idx': idx,
                        'period_end': period_info['period_end'],
                        'period_duration': period_info['period_duration']
                    })
            continue

        # Skip separator row
        if in_table and line.startswith('|') and '---' in line:
            continue

        # Parse data rows
        if in_table and line.startswith('|'):
            cells = [c.strip() for c in line.split('|')[1:-1]]

            if len(cells) < 3:
                continue

            original_name = cells[0].strip().strip('*')
            canonical_field = cells[1].strip().strip('*')
            # cells[2] is Ref, skip it

            if not is_valid_canonical_field(canonical_field):
                continue

            # Extract values for each period column
            for period_col in period_columns:
                col_idx = period_col['col_idx']
                if col_idx >= len(cells):
                    continue

                value = parse_value(cells[col_idx])
                if value is None:
                    continue

                # Derive fiscal_year from period_end and fiscal_period
                period_end = period_col['period_end']
                period_year = int(period_end[:4])
                period_month = int(period_end[5:7])
                fiscal_month = int(fiscal_period.split('-')[0])

                # Fiscal year is the year the fiscal period ends
                # If fiscal year ends in month X, and current period is after X, fiscal_year = period_year
                # Otherwise fiscal_year = period_year (for annual) or derive from context
                if period_type == 'annual':
                    fiscal_year = period_year
                else:
                    # For quarterly, fiscal_year is trickier
                    # If fiscal ends Dec (12), fiscal_year = period_year
                    # If fiscal ends Jun (6), and period is Jul-Dec, fiscal_year = period_year + 1
                    if period_month > fiscal_month:
                        fiscal_year = period_year + 1
                    else:
                        fiscal_year = period_year

                # Determine period_duration if not already set
                period_duration = period_col['period_duration']
                if not period_duration:
                    period_duration = '12M' if period_type == 'annual' else '3M'

                row = {
                    "ticker": ticker,
                    "company_name": company_name,
                    "industry": industry,
                    "unit_type": "rupees" if canonical_field in NON_NORMALIZED_FIELDS else unit_type,
                    "period_type": period_type,
                    "period_end": period_end,
                    "period_duration": period_duration,
                    "fiscal_year": fiscal_year,
                    "section": section,
                    "statement_type": "profit_loss",
                    "canonical_field": canonical_field,
                    "original_name": original_name,
                    "value": value,
                    "method": "",  # Could be extracted if available
                    "source_file": filename
                }
                rows.append(row)

        # End of table
        if in_table and not line.startswith('|') and line:
            if 'SOURCE_PAGES' in line:
                break

    return rows


def main():
    parser = argparse.ArgumentParser(description="Flatten P&L data for D1")
    parser.add_argument("--ticker", help="Process single ticker only")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Get all markdown files
    all_files = sorted(EXTRACTED_PL_DIR.glob("*.md"))

    # Filter by ticker if specified
    if args.ticker:
        all_files = [f for f in all_files if f.name.startswith(args.ticker + '_')]

    # Exclude _extraction_results.json type files
    all_files = [f for f in all_files if not f.name.startswith('_')]

    print(f"Flattening P&L data from {len(all_files)} files...")
    print(f"Input:  {EXTRACTED_PL_DIR}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    total_rows = 0
    total_files = 0
    field_stats = defaultdict(int)
    ticker_stats = defaultdict(int)

    with open(OUTPUT_FILE, 'w') as out:
        for filepath in all_files:
            rows = parse_markdown_file(filepath)

            if not rows:
                continue

            for row in rows:
                out.write(json.dumps(row) + "\n")
                field_stats[row["canonical_field"]] += 1
                ticker_stats[row["ticker"]] += 1

            total_files += 1
            total_rows += len(rows)

            if total_files % 500 == 0:
                print(f"  Processed {total_files} files, {total_rows:,} rows...")

    # Summary
    print()
    print("=" * 60)
    print("FLATTEN COMPLETE")
    print("=" * 60)
    print(f"Files:    {total_files}")
    print(f"Tickers:  {len(ticker_stats)}")
    print(f"Rows:     {total_rows:,}")
    print(f"Unique fields: {len(field_stats)}")
    print(f"Output: {OUTPUT_FILE}")

    # Top 10 fields
    print()
    print("Top 10 fields:")
    for field, count in sorted(field_stats.items(), key=lambda x: -x[1])[:10]:
        print(f"  {field}: {count:,}")


if __name__ == "__main__":
    main()
