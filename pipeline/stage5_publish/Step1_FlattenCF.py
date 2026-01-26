#!/usr/bin/env python3
"""
Step 1: Flatten Cash Flow Data for D1 Upload

Reads BOTH:
1. Derived quarterly CF data (3M periods) from quarterly_cf/
2. Cumulative periods (6M, 9M, 12M) from json_cf/

This preserves ALL period durations for complete coverage.

Input:  data/quarterly_cf/*.json (3M quarters)
        data/json_cf/*.json (cumulative periods)
Output: artifacts/stage4/cf_flat.jsonl

Usage:
    python3 Step1_FlattenCF.py
    python3 Step1_FlattenCF.py --ticker LUCK
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
QUARTERLY_CF_DIR = PROJECT_ROOT / "data" / "quarterly_cf"
JSON_CF_DIR = PROJECT_ROOT / "data" / "json_cf"
OUTPUT_DIR = PROJECT_ROOT / "data" / "flat"
OUTPUT_FILE = OUTPUT_DIR / "cf.jsonl"

# Source page manifest (specific pages per statement type)
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
PDF_BASE_URL = "https://source.psxgpt.com/PDF_PAGES"

# QC issues file for flagging risky values
QC_ISSUES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step6_qc_cf_results.json"

# Arithmetic allowlist file (manually reviewed exceptions)
ARITHMETIC_ALLOWLIST_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_arithmetic_allowlist_cf.json"

# Fields that should never be negative for Cash Flow
# Note: Most CF items CAN be negative (outflows), but cash balances should be positive
NON_NEGATIVE_FIELDS = {'cash_start', 'cash_end', 'cash_and_equivalents'}

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
# Structure: {ticker: [semantic_failures]}
QC_ISSUE_LOOKUP = {}
if QC_ISSUES_FILE.exists():
    with open(QC_ISSUES_FILE) as f:
        qc_data = json.load(f)
    # CF QC results have semantic failures at ticker level
    for result in qc_data.get('results', []):
        ticker = result.get('ticker', '')
        if result.get('checks', {}).get('semantic', {}).get('failed', 0) > 0:
            QC_ISSUE_LOOKUP[ticker] = 'semantic_equation_failure'

# Load arithmetic allowlist for qc_note
# Structure: {(ticker, fiscal_year, consolidation): reason}
ALLOWLIST_LOOKUP = {}
if ARITHMETIC_ALLOWLIST_FILE.exists():
    with open(ARITHMETIC_ALLOWLIST_FILE) as f:
        allowlist_data = json.load(f)
    for item in allowlist_data.get('allowlist', []):
        key = (item['ticker'], item['fiscal_year'], item['consolidation'])
        ALLOWLIST_LOOKUP[key] = item.get('reason', 'Manually reviewed')


def normalize_value(value: float, unit_type: str) -> float:
    """
    Normalize a value to thousands.
    - rupees: divide by 1000
    - millions: multiply by 1000
    - thousands: keep as is
    """
    if value is None:
        return None

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


def get_qc_flag(ticker: str, period_end: str, section: str, field: str, value: float, method: str, fiscal_year: int) -> str:
    """
    Determine QC risk flag for a cash flow value, including explanation if available.

    Returns:
        - 'derivation_anomaly: <reason>' or just 'derivation_anomaly'
        - 'unexpected_negative: <reason>' or just 'unexpected_negative'
        - 'allowlisted: <reason>' for manually reviewed items without other flags
        - '': No issues
    """
    flag_type = ''

    # Check for unexpected negative values
    if field in NON_NEGATIVE_FIELDS and value is not None and value < 0:
        if method and method != 'direct_3M' and method != 'direct':
            flag_type = 'derivation_anomaly'
        else:
            flag_type = 'unexpected_negative'

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


def get_source_info_from_source(ticker: str, source: str, section: str) -> dict:
    """
    Extract source pages from the source file reference.

    Source can be:
    - A filename like "AABS_quarterly_2021-03-31_consolidated.md"
    - A derivation description like "6M (AABS_quarterly_2021-06-30_consolidated.md) - Q1"
    """
    # Handle None source
    if source is None:
        return {'source_pages': [], 'source_url': ''}

    # Extract the primary source file from the source string
    if '.md' in source:
        # Find the first .md file mentioned
        match = re.search(r'([A-Z0-9]+_(annual|quarterly)_[\d-]+_\w+)\.md', source)
        if match:
            filename = match.group(1)
            # Parse the filename to get period info
            if '_annual_' in filename:
                parts = filename.split('_annual_')
                year = parts[1].split('_')[0]
                period_key = f"annual_{year}"
                folder_pattern = f"{ticker}/{year}/{ticker}_Annual_{year}"
            elif '_quarterly_' in filename:
                parts = filename.split('_quarterly_')
                date_part = parts[1].split('_')[0]
                year = date_part[:4]
                period_key = f"quarterly_{date_part}"
                folder_pattern = f"{ticker}/{year}/{ticker}_Quarterly_{date_part}"
            else:
                return {'source_pages': [], 'source_url': ''}

            # Look up pages
            pages = []
            if ticker in STATEMENT_PAGES:
                ticker_data = STATEMENT_PAGES[ticker]
                if period_key in ticker_data:
                    period_data = ticker_data[period_key]
                    if section in period_data:
                        pages = period_data[section].get('CF', [])

            return {
                'source_pages': pages,
                'source_url': f"{PDF_BASE_URL}/{folder_pattern}"
            }

    return {'source_pages': [], 'source_url': ''}


def parse_quarterly_file(filepath: Path) -> list[dict]:
    """Parse a quarterly_cf JSON file and return list of row dicts."""
    rows = []

    with open(filepath) as f:
        data = json.load(f)

    ticker = data['ticker']
    meta = TICKER_META.get(ticker, {})
    company_name = meta.get("Company Name", "")
    industry = meta.get("Industry", "")

    for quarter in data.get('quarters', []):
        period_end = quarter['period_end']
        fiscal_year = quarter['fiscal_year']
        section = quarter['consolidation']
        method = quarter.get('method', 'unknown')
        source = quarter.get('source', '')
        values = quarter.get('values', {})
        source_labels = quarter.get('source_labels', {})

        # Get source info
        source_info = get_source_info_from_source(ticker, source, section)

        # Each field becomes a row
        for canonical_field, value in values.items():
            if value is None:
                continue

            # Use source_labels for original_name if available, else fall back to canonical
            original_name = source_labels.get(canonical_field, canonical_field)

            # Get QC flag
            qc_flag = get_qc_flag(ticker, period_end, section, canonical_field, value, method, fiscal_year)

            row = {
                "ticker": ticker,
                "company_name": company_name,
                "industry": industry,
                "unit_type": "thousands",  # quarterly_cf is already normalized
                "period_type": "quarterly",
                "period_end": period_end,
                "period_duration": "3M",
                "fiscal_year": fiscal_year,
                "section": section,
                "statement_type": "cash_flow",
                "canonical_field": canonical_field,
                "original_name": original_name,
                "value": value,
                "method": method,
                "source_file": source if isinstance(source, str) else str(source),
                "source_pages": source_info['source_pages'],
                "source_url": source_info['source_url'],
                "qc_flag": qc_flag
            }
            rows.append(row)

    return rows


def get_fiscal_year(period_end: str, duration: str) -> int:
    """
    Derive fiscal year from period_end and duration.
    """
    try:
        date = datetime.strptime(period_end, '%Y-%m-%d')
        return date.year
    except:
        return int(period_end[:4])


def parse_json_cf_file(filepath: Path) -> list[dict]:
    """
    Parse a json_cf JSON file and return list of row dicts for CUMULATIVE periods only (6M, 9M, 12M).
    3M periods are handled by quarterly_cf which has better derivation.
    """
    rows = []

    with open(filepath) as f:
        data = json.load(f)

    ticker = data['ticker']
    meta = TICKER_META.get(ticker, {})
    company_name = meta.get("Company Name", "")
    industry = meta.get("Industry", "")

    for period in data.get('periods', []):
        duration = period.get('duration', '')

        # Skip 3M - those come from quarterly_cf with better derivation
        if duration == '3M':
            continue

        # Only include 6M, 9M, 12M (cumulative periods)
        if duration not in ('6M', '9M', '12M'):
            continue

        period_end = period['period_end']
        section = period['consolidation']
        source_filing = period.get('source_filing', '')
        values = period.get('values', {})
        source_labels = period.get('source_labels', {})
        unit_type = period.get('unit_type', 'thousands')  # Get original unit

        # Get source info directly from json_cf (already has source_pages)
        source_pages = period.get('source_pages', [])
        source_url = period.get('source_url', '')

        # Determine period_type
        if duration == '12M':
            period_type = 'annual'
        else:
            period_type = 'quarterly'  # 6M, 9M are YTD from quarterly filings

        fiscal_year = get_fiscal_year(period_end, duration)

        # Each field becomes a row
        for canonical_field, raw_value in values.items():
            if raw_value is None:
                continue

            # Normalize value to thousands
            value = normalize_value(raw_value, unit_type)

            # Use source_labels for original_name if available, else fall back to canonical
            original_name = source_labels.get(canonical_field, canonical_field)

            # Get QC flag (cumulative periods use 'direct' method)
            qc_flag = get_qc_flag(ticker, period_end, section, canonical_field, value, 'direct', fiscal_year)

            row = {
                "ticker": ticker,
                "company_name": company_name,
                "industry": industry,
                "unit_type": "thousands",  # All values normalized to thousands
                "period_type": period_type,
                "period_end": period_end,
                "period_duration": duration,
                "fiscal_year": fiscal_year,
                "section": section,
                "statement_type": "cash_flow",
                "canonical_field": canonical_field,
                "original_name": original_name,
                "value": value,
                "method": "direct",  # Cumulative periods are direct from source
                "source_file": source_filing,
                "source_pages": source_pages,
                "source_url": source_url,
                "qc_flag": qc_flag
            }
            rows.append(row)

    return rows


def main():
    parser = argparse.ArgumentParser(description="Flatten Cash Flow data for D1")
    parser.add_argument("--ticker", help="Process single ticker only")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Get all quarterly JSON files (for 3M periods)
    quarterly_files = sorted(QUARTERLY_CF_DIR.glob("*.json"))

    # Get all json_cf files (for cumulative periods: 6M, 9M, 12M)
    json_cf_files = sorted(JSON_CF_DIR.glob("*.json"))

    # Filter by ticker if specified
    if args.ticker:
        quarterly_files = [f for f in quarterly_files if f.stem == args.ticker]
        json_cf_files = [f for f in json_cf_files if f.stem == args.ticker]

    print(f"Flattening Cash Flow data...")
    print(f"  3M periods from:  {QUARTERLY_CF_DIR} ({len(quarterly_files)} files)")
    print(f"  Cumulative from:  {JSON_CF_DIR} ({len(json_cf_files)} files)")
    print(f"  Output: {OUTPUT_FILE}")
    print()

    # Collect all rows
    all_rows = []
    field_stats = defaultdict(int)
    ticker_stats = defaultdict(int)
    duration_stats = defaultdict(int)

    # Process quarterly files for 3M periods
    print("Processing 3M periods from quarterly_cf...")
    quarterly_count = 0
    for filepath in quarterly_files:
        rows = parse_quarterly_file(filepath)
        if rows:
            all_rows.extend(rows)
            quarterly_count += 1
            for row in rows:
                duration_stats[row['period_duration']] += 1

    print(f"  Loaded {quarterly_count} tickers with 3M periods")

    # Process json_cf files for cumulative periods (6M, 9M, 12M)
    print("Processing cumulative periods from json_cf...")
    json_cf_count = 0
    for filepath in json_cf_files:
        rows = parse_json_cf_file(filepath)
        if rows:
            all_rows.extend(rows)
            json_cf_count += 1
            for row in rows:
                duration_stats[row['period_duration']] += 1

    print(f"  Loaded {json_cf_count} tickers with cumulative periods")

    # Write rows
    qc_flag_stats = defaultdict(int)

    with open(OUTPUT_FILE, 'w') as out:
        for row in all_rows:
            out.write(json.dumps(row) + "\n")
            field_stats[row["canonical_field"]] += 1
            ticker_stats[row["ticker"]] += 1
            if row.get("qc_flag"):
                # Extract flag type (before colon if present)
                flag_type = row["qc_flag"].split(":")[0].strip()
                qc_flag_stats[flag_type] += 1

    total_rows = len(all_rows)

    # Summary
    print()
    print("=" * 60)
    print("FLATTEN COMPLETE")
    print("=" * 60)
    print(f"Tickers:    {len(ticker_stats)}")
    print(f"Rows:       {total_rows:,}")
    print(f"Fields:     {len(field_stats)}")
    print()
    print("Period durations:")
    for d, c in sorted(duration_stats.items()):
        print(f"  {d}: {c:,}")
    print()

    # QC flags summary
    if qc_flag_stats:
        print("QC Flags:")
        for flag, count in sorted(qc_flag_stats.items(), key=lambda x: -x[1]):
            print(f"  {flag}: {count:,}")
        print()

    print(f"Output: {OUTPUT_FILE}")

    # Top 10 fields
    print()
    print("Top 10 fields:")
    for field, count in sorted(field_stats.items(), key=lambda x: -x[1])[:10]:
        print(f"  {field}: {count:,}")


if __name__ == "__main__":
    main()
