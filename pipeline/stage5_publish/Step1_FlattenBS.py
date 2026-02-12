#!/usr/bin/env python3
"""
Step 1: Flatten Balance Sheet Data for D1 Upload

Reads parsed JSON files from json_bs/ and outputs flat JSONL rows for financial_statements table.

Input:  data/json_bs/*.json
Output: data/flat/bs.jsonl

Usage:
    python3 Step1_FlattenBS.py
    python3 Step1_FlattenBS.py --ticker LUCK
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
JSON_BS_DIR = PROJECT_ROOT / "data" / "json_bs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "flat"
OUTPUT_FILE = OUTPUT_DIR / "bs.jsonl"

# Source page manifest (specific pages per statement type)
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
PDF_BASE_URL = "https://source.psxgpt.com/PDF_PAGES"

# QC issues file for flagging risky values
QC_ISSUES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step6_qc_bs_results.json"

# Arithmetic allowlist file (manually reviewed exceptions)
ARITHMETIC_ALLOWLIST_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_arithmetic_allowlist_bs.json"

# Fields that should never be negative for Balance Sheet
# Note: retained_earnings CAN be negative (accumulated deficit), treasury_shares are typically negative
NON_NEGATIVE_FIELDS = {'total_assets', 'total_equity_and_liabilities', 'cash_and_equivalents',
                       'property_equipment', 'inventory', 'receivables', 'total_current_assets',
                       'total_non_current_assets', 'share_capital'}

# Load ticker metadata
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
if TICKERS_FILE.exists():
    with open(TICKERS_FILE) as f:
        tickers_data = json.load(f)
    TICKER_META = {t['Symbol']: t for t in tickers_data}
else:
    TICKER_META = {}

# Load statement pages manifest (TICKER -> period -> section -> statement_type -> pages)
STATEMENT_PAGES = {}
if STATEMENT_PAGES_FILE.exists():
    with open(STATEMENT_PAGES_FILE) as f:
        STATEMENT_PAGES = json.load(f)

# Load QC issues for flagging
# Structure: {(ticker, period_end, section): [issue_descriptions]}
QC_ISSUE_LOOKUP = {}
if QC_ISSUES_FILE.exists():
    with open(QC_ISSUES_FILE) as f:
        qc_data = json.load(f)
    # BS QC results have different structure - look for accounting equation failures
    for ticker, result in qc_data.get('tickers', {}).items():
        for failure in result.get('checks', {}).get('accounting_equation', {}).get('failures', []):
            period = failure.get('period_end', '')
            section = failure.get('consolidation', 'consolidated')
            key = (ticker, period, section)
            if key not in QC_ISSUE_LOOKUP:
                QC_ISSUE_LOOKUP[key] = []
            QC_ISSUE_LOOKUP[key].append('accounting_equation_failure')

# Load arithmetic allowlist for qc_note
# Structure: {(ticker, fiscal_year, consolidation): reason}
ALLOWLIST_LOOKUP = {}
if ARITHMETIC_ALLOWLIST_FILE.exists():
    with open(ARITHMETIC_ALLOWLIST_FILE) as f:
        allowlist_data = json.load(f)
    for item in allowlist_data.get('allowlist', []):
        key = (item['ticker'], item.get('period', item.get('fiscal_year', '')), item['consolidation'])
        ALLOWLIST_LOOKUP[key] = item.get('reason', 'Manually reviewed')


def normalize_value(value: float, unit_type: str, canonical: str = None) -> float:
    """
    Normalize a value to thousands.
    - rupees: divide by 1000
    - millions: multiply by 1000
    - thousands: keep as is
    - Skip normalization for per-share fields
    """
    if value is None:
        return None

    # Skip normalization for per-share metrics
    if canonical and ('per_share' in canonical.lower() or 'eps' in canonical.lower()):
        return value

    unit_lower = unit_type.lower().strip() if unit_type else 'thousands'

    if unit_lower in ('rupees', 'rupee'):
        return value / 1000.0
    elif unit_lower == 'millions':
        return value * 1000.0
    elif 'thousands' in unit_lower:
        return value
    else:
        # Unknown unit, assume already in thousands
        return value


def get_qc_flag(ticker: str, period_end: str, section: str, field: str, value: float, fiscal_year: int) -> str:
    """
    Determine QC risk flag for a balance sheet value, including explanation if available.

    Returns:
        - 'unexpected_negative: <reason>' or just 'unexpected_negative'
        - 'qc_flagged: <reason>' for values flagged by QC checks
        - 'allowlisted: <reason>' for manually reviewed items without other flags
        - '': No issues
    """
    flag_type = ''

    # Check for unexpected negative values
    if field in NON_NEGATIVE_FIELDS and value is not None and value < 0:
        flag_type = 'unexpected_negative'

    # Check if flagged by QC checks
    qc_key = (ticker, period_end, section)
    if qc_key in QC_ISSUE_LOOKUP and not flag_type:
        flag_type = 'qc_flagged'

    # Get allowlist note if available
    allowlist_key = (ticker, fiscal_year, section)
    note = ALLOWLIST_LOOKUP.get(allowlist_key, '')

    # Build the flag string
    if flag_type and note:
        return f"{flag_type}: {note}"
    elif flag_type:
        return flag_type
    elif note:
        return f"allowlisted: {note}"
    else:
        return ''


def get_source_info(ticker: str, period_type: str, period_part: str, section: str) -> dict:
    """Look up source pages and URL from step2_statement_pages.json manifest.

    Manifest structure: TICKER -> period_key -> section -> statement_type -> [pages]
    """
    # Build period key (e.g., "annual_2024" or "quarterly_2024-03-31")
    if period_type == 'annual':
        period_key = f"annual_{period_part}"
        year = period_part if len(period_part) == 4 else f"20{period_part}"
        folder_pattern = f"{ticker}/{year}/{ticker}_Annual_{period_part}"
    else:
        period_key = f"quarterly_{period_part}"
        year = period_part[:4]
        folder_pattern = f"{ticker}/{year}/{ticker}_Quarterly_{period_part}"

    # Look up in manifest: TICKER -> period_key -> section -> BS
    pages = []
    if ticker in STATEMENT_PAGES:
        ticker_data = STATEMENT_PAGES[ticker]
        if period_key in ticker_data:
            period_data = ticker_data[period_key]
            if section in period_data:
                pages = period_data[section].get('BS', [])

    source_url = f"{PDF_BASE_URL}/{folder_pattern}"

    return {
        'source_pages': pages,
        'source_url': source_url
    }


def parse_json_bs_file(filepath: Path) -> list[dict]:
    """Parse a json_bs JSON file and return list of row dicts."""
    rows = []

    with open(filepath) as f:
        data = json.load(f)

    ticker = data['ticker']
    meta = TICKER_META.get(ticker, {})
    company_name = meta.get("Company Name", "")
    industry = meta.get("Industry", "")
    fiscal_period = meta.get("fiscal_period", "12-31")

    for period in data.get('periods', []):
        period_end = period['period_end']
        section = period['consolidation']
        filing_type = period.get('filing_type', 'quarterly')
        unit_type = period.get('unit_type', 'thousands')
        source_file = period.get('source_file', '')
        values = period.get('values', {})
        source_items = period.get('source_items', {})

        # Source info: prefer what's in the JSON, fall back to manifest lookup
        source_pages = period.get('source_pages', [])
        source_url = period.get('source_url', '')

        # Compute fiscal year
        period_year = int(period_end[:4])
        period_month = int(period_end[5:7])
        fiscal_month = int(fiscal_period.split('-')[0])

        if filing_type == 'annual':
            fiscal_year = period_year
        else:
            if period_month > fiscal_month:
                fiscal_year = period_year + 1
            else:
                fiscal_year = period_year

        # Each field becomes a row
        for canonical_field, raw_value in values.items():
            if raw_value is None:
                continue

            # Normalize value to thousands
            value = normalize_value(raw_value, unit_type, canonical_field)

            # Get original name from source_items
            original_name = source_items.get(canonical_field, canonical_field)

            # Get QC flag
            qc_flag = get_qc_flag(ticker, period_end, section, canonical_field, value, fiscal_year)

            row = {
                "ticker": ticker,
                "company_name": company_name,
                "industry": industry,
                "unit_type": "thousands",  # All values normalized to thousands
                "period_type": filing_type,
                "period_end": period_end,
                "period_duration": "PIT",
                "fiscal_year": fiscal_year,
                "section": section,
                "statement_type": "balance_sheet",
                "canonical_field": canonical_field,
                "original_name": original_name,
                "value": value,
                "method": "",
                "source_file": source_file,
                "source_pages": source_pages,
                "source_url": source_url,
                "qc_flag": qc_flag
            }
            rows.append(row)

    return rows


def is_primary_period(row: dict) -> bool:
    """
    Check if this row is from the primary period of its filing (not a comparative).

    For annual files: the period should match the filing year
    For quarterly files: the period_end should match the filing date
    """
    source_file = row.get('source_file', '')
    period_end = row.get('period_end', '')

    if '_annual_' in source_file:
        # Annual file: e.g., AABS_annual_2024_consolidated.md
        # Primary period is the filing year (2024)
        parts = source_file.split('_annual_')
        if len(parts) >= 2:
            filing_year = parts[1].split('_')[0]
            period_year = period_end[:4]
            return filing_year == period_year
    elif '_quarterly_' in source_file:
        # Quarterly file: e.g., AABS_quarterly_2024-03-31_consolidated.md
        # Primary period is the filing date
        parts = source_file.split('_quarterly_')
        if len(parts) >= 2:
            filing_date = parts[1].split('_')[0]
            return filing_date == period_end

    return True  # Default to primary if can't determine


def main():
    parser = argparse.ArgumentParser(description="Flatten Balance Sheet data for D1")
    parser.add_argument("--ticker", help="Process single ticker only")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_files = sorted(JSON_BS_DIR.glob("*.json"))

    if args.ticker:
        all_files = [f for f in all_files if f.stem == args.ticker]

    print(f"Flattening Balance Sheet data from {len(all_files)} JSON files...")
    print(f"Input:  {JSON_BS_DIR}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    # Collect all rows first, then deduplicate
    all_rows = {}  # key -> row (keep best source)
    total_files = 0
    duplicates_skipped = 0

    for filepath in all_files:
        rows = parse_json_bs_file(filepath)

        if not rows:
            continue

        for row in rows:
            key = (
                row["ticker"],
                row["period_end"],
                row["section"],
                row["canonical_field"]
            )

            if key in all_rows:
                existing = all_rows[key]
                existing_is_primary = is_primary_period(existing)
                new_is_primary = is_primary_period(row)

                # Prefer primary period over comparative
                if new_is_primary and not existing_is_primary:
                    all_rows[key] = row
                # If both are primary (or both comparative), prefer annual over quarterly
                elif new_is_primary == existing_is_primary:
                    existing_is_annual = 'annual' in existing['source_file']
                    new_is_annual = 'annual' in row['source_file']
                    if new_is_annual and not existing_is_annual:
                        all_rows[key] = row

                duplicates_skipped += 1
            else:
                all_rows[key] = row

        total_files += 1

        if total_files % 50 == 0:
            print(f"  Processed {total_files} files...")

    # Write deduplicated rows
    field_stats = defaultdict(int)
    ticker_stats = defaultdict(int)
    qc_flag_stats = defaultdict(int)

    with open(OUTPUT_FILE, 'w') as out:
        for row in all_rows.values():
            out.write(json.dumps(row) + "\n")
            field_stats[row["canonical_field"]] += 1
            ticker_stats[row["ticker"]] += 1
            if row.get("qc_flag"):
                # Extract flag type (before colon if present)
                flag_type = row["qc_flag"].split(":")[0].strip()
                qc_flag_stats[flag_type] += 1

    total_rows = len(all_rows)

    print()
    print("=" * 60)
    print("FLATTEN COMPLETE")
    print("=" * 60)
    print(f"Files:      {total_files}")
    print(f"Tickers:    {len(ticker_stats)}")
    print(f"Rows:       {total_rows:,}")
    print(f"Duplicates: {duplicates_skipped:,} (skipped)")
    print(f"Fields:     {len(field_stats)}")
    print()

    # QC flags summary
    if qc_flag_stats:
        print("QC Flags:")
        for flag, count in sorted(qc_flag_stats.items(), key=lambda x: -x[1]):
            print(f"  {flag}: {count:,}")
        print()

    print(f"Output: {OUTPUT_FILE}")

    print()
    print("Top 10 fields:")
    for field, count in sorted(field_stats.items(), key=lambda x: -x[1])[:10]:
        print(f"  {field}: {count:,}")


if __name__ == "__main__":
    main()
