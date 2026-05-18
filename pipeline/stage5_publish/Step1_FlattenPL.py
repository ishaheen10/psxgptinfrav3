#!/usr/bin/env python3
"""
Step 1: Flatten P&L Data for D1 Upload

Reads BOTH:
1. Derived quarterly P&L data (3M periods) from quarterly_pl/
2. Cumulative periods (6M, 9M, 12M) from json_pl/

This preserves ALL period durations for complete coverage.

Input:  data/quarterly_pl/*.json (3M quarters)
        data/json_pl/*.json (cumulative periods)
Output: artifacts/stage4/pl_flat.jsonl

Usage:
    python3 Step1_FlattenPL.py
    python3 Step1_FlattenPL.py --ticker LUCK
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
QUARTERLY_PL_DIR = PROJECT_ROOT / "data" / "quarterly_pl"
JSON_PL_DIR = PROJECT_ROOT / "data" / "json_pl"
LTM_PL_DIR = PROJECT_ROOT / "data" / "ltm_pl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "flat"
OUTPUT_FILE = OUTPUT_DIR / "pl.jsonl"

# Source page manifest (specific pages per statement type)
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
PDF_BASE_URL = "https://source.psxgpt.com/PDF_PAGES"

# QC issues file for flagging risky values
QC_ISSUES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_qc_issues.json"

# Arithmetic allowlist file (manually reviewed exceptions)
ARITHMETIC_ALLOWLIST_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_arithmetic_allowlist.json"

# Fields that should never be negative
NON_NEGATIVE_FIELDS = {'revenue_net', 'gross_revenue', 'dividend_income', 'interest_income'}

# Load ticker metadata
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
if TICKERS_FILE.exists():
    with open(TICKERS_FILE) as f:
        tickers_data = json.load(f)
    TICKER_META = {t['Symbol']: t for t in tickers_data}
else:
    TICKER_META = {}

# Canonical name normalization: remap variant names to canonical
# Applied to ALL tickers
EPS_CANONICAL_MAP = {
    'eps_basic': 'eps',
    'eps_diluted': 'eps',
    'eps_basic_diluted': 'eps',
}

# Revenue field remap for bank tickers only
# Banks use total_income instead of revenue_net
BANK_REVENUE_REMAP = {
    'revenue_net': 'total_income',
}

# Identify bank tickers from TICKER_META (Industry = 'Banking')
BANK_TICKERS = {
    sym for sym, meta in TICKER_META.items()
    if meta.get('Industry', '') == 'Banking'
}


def normalize_canonical(canonical: str, ticker: str) -> str:
    """
    Normalize variant canonical names to their canonical form.
    - eps_basic / eps_diluted / eps_basic_diluted -> eps (all tickers)
    - revenue_net -> total_income for bank tickers only
    """
    if canonical in EPS_CANONICAL_MAP:
        return EPS_CANONICAL_MAP[canonical]
    if ticker in BANK_TICKERS and canonical in BANK_REVENUE_REMAP:
        return BANK_REVENUE_REMAP[canonical]
    return canonical

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
    for issue in qc_data.get('issues', []):
        ticker = issue.get('ticker', '')
        period = issue.get('period_end', '')
        section = issue.get('consolidation', 'consolidated')
        key = (ticker, period, section)
        if key not in QC_ISSUE_LOOKUP:
            QC_ISSUE_LOOKUP[key] = []
        QC_ISSUE_LOOKUP[key].append(issue.get('issue', 'qc_issue'))

# Load arithmetic allowlist for qc_note
# Structure: {(ticker, fiscal_year, consolidation): reason}
ALLOWLIST_LOOKUP = {}
if ARITHMETIC_ALLOWLIST_FILE.exists():
    with open(ARITHMETIC_ALLOWLIST_FILE) as f:
        allowlist_data = json.load(f)
    for item in allowlist_data.get('allowlist', []):
        key = (item['ticker'], item['fiscal_year'], item['consolidation'])
        ALLOWLIST_LOOKUP[key] = item.get('reason', 'Manually reviewed')


def normalize_value(value: float, unit_type: str, canonical: str = None) -> float:
    """
    Normalize a value to thousands.
    - rupees: divide by 1000
    - millions: multiply by 1000
    - thousands: keep as is
    - Skip normalization for EPS fields (always in rupees per share)
    """
    if value is None:
        return None

    # Skip normalization for EPS (always in rupees per share)
    if canonical and 'eps' in canonical.lower():
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


def get_qc_flag(ticker: str, period_end: str, section: str, field: str, value: float, method: str, fiscal_year: int) -> str:
    """
    Determine QC risk flag for a value, including explanation if available.

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
    # Extract the primary source file from the source string
    if source and '.md' in source:
        # Find the first .md file mentioned
        import re
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
                        pages = period_data[section].get('PL', [])

            return {
                'source_pages': pages,
                'source_url': f"{PDF_BASE_URL}/{folder_pattern}"
            }

    return {'source_pages': [], 'source_url': ''}


def get_row_label_map(period_like: dict) -> dict:
    return period_like.get('source_items') or period_like.get('source_labels') or {}


def parse_quarterly_file(filepath: Path) -> list[dict]:
    """Parse a quarterly_pl JSON file and return list of row dicts."""
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
        source_labels = get_row_label_map(quarter)

        # Get source info
        source_info = get_source_info_from_source(ticker, source, section)

        # Each field becomes a row
        for canonical_field, value in values.items():
            if value is None:
                continue

            # Normalize canonical name (eps variants -> eps; revenue_net -> total_income for banks)
            canonical_field = normalize_canonical(canonical_field, ticker)

            # Use source_labels for original_name if available, else fall back to canonical
            original_name = source_labels.get(canonical_field) or canonical_field

            # Get QC risk flag (includes explanation if allowlisted)
            qc_flag = get_qc_flag(ticker, period_end, section, canonical_field, value, method, fiscal_year)

            row = {
                "ticker": ticker,
                "company_name": company_name,
                "industry": industry,
                "unit_type": "thousands",  # quarterly_pl is already normalized
                "period_type": "quarterly",
                "period_end": period_end,
                "period_duration": "3M",
                "fiscal_year": fiscal_year,
                "section": section,
                "statement_type": "profit_loss",
                "canonical_name": canonical_field,
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
    For 12M periods ending in Jun, fiscal year is same year.
    For quarterly periods, fiscal year depends on fiscal year-end month.
    """
    try:
        date = datetime.strptime(period_end, '%Y-%m-%d')
        # For simplicity, assume June fiscal year end for most companies
        # The actual fiscal year is derived in quarterly_pl, but for cumulative
        # periods we approximate based on the period end date
        if duration == '12M':
            return date.year
        else:
            # For 6M/9M, approximate fiscal year
            return date.year
    except:
        return int(period_end[:4])


def parse_json_pl_file(filepath: Path) -> list[dict]:
    """
    Parse a json_pl JSON file and return list of row dicts for CUMULATIVE periods only (6M, 9M, 12M).
    3M periods are handled by quarterly_pl which has better derivation.
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

        # Skip 3M - those come from quarterly_pl with better derivation
        if duration == '3M':
            continue

        # Only include 6M, 9M, 12M (cumulative periods)
        if duration not in ('6M', '9M', '12M'):
            continue

        period_end = period['period_end']
        section = period['consolidation']
        source_file = period.get('source_file', '')
        values = period.get('values', {})
        source_labels = get_row_label_map(period)
        unit_type = period.get('unit_type', 'thousands')  # Get original unit

        # Get source info directly from json_pl (already has source_pages)
        source_pages = period.get('source_pages', [])
        source_url = period.get('source_url', '')

        # Determine period_type
        if duration == '12M':
            period_type = 'annual'
        else:
            period_type = 'quarterly'  # 6M, 9M are YTD from quarterly filings

        fiscal_year = get_fiscal_year(period_end, duration)

        # Each field becomes a row
        for canonical_field, value_data in values.items():
            if value_data is None:
                continue

            # Normalize canonical name (eps variants -> eps; revenue_net -> total_income for banks)
            canonical_field = normalize_canonical(canonical_field, ticker)

            # Handle V2 format: values are dicts with {value, source_item, ref, is_calculated}
            if isinstance(value_data, dict):
                raw_value = value_data.get('value')
                original_name = value_data.get('source_item') or canonical_field
            else:
                # Fallback for V1 format (plain floats)
                raw_value = value_data
                original_name = source_labels.get(canonical_field) or canonical_field

            if raw_value is None:
                continue

            # Normalize value to thousands
            value = normalize_value(raw_value, unit_type, canonical_field)

            # Get QC risk flag (includes explanation if allowlisted)
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
                "statement_type": "profit_loss",
                "canonical_name": canonical_field,
                "original_name": original_name,
                "value": value,
                "method": "direct",  # Cumulative periods are direct from source
                "source_file": source_file,
                "source_pages": source_pages,
                "source_url": source_url,
                "qc_flag": qc_flag
            }
            rows.append(row)

    return rows


def parse_ltm_pl_file(filepath: Path) -> list[dict]:
    """Parse an ltm_pl JSON file and return list of row dicts for LTM periods."""
    rows = []

    with open(filepath) as f:
        data = json.load(f)

    ticker = data['ticker']
    meta = TICKER_META.get(ticker, {})
    company_name = meta.get("Company Name", "")
    industry = meta.get("Industry", "")

    for period in data.get('ltm_periods', []):
        period_end = period['period_end']
        section = period['consolidation']
        values = period.get('values', {})
        source_labels = get_row_label_map(period)
        fiscal_year = period.get('fiscal_year')
        method = period.get('method', 'derived')
        qc_flag = period.get('qc_flag', '')
        current_component = period.get('source_components', {}).get('current', {})
        source_file = current_component.get('source_file')
        source_pages = current_component.get('source_pages') or []
        source_url = current_component.get('source_url', '')

        for canonical_field, value in values.items():
            if value is None:
                continue

            # Normalize canonical name (eps variants -> eps; revenue_net -> total_income for banks)
            canonical_field = normalize_canonical(canonical_field, ticker)

            original_name = source_labels.get(canonical_field) or canonical_field

            row = {
                "ticker": ticker,
                "company_name": company_name,
                "industry": industry,
                "unit_type": "thousands",
                "period_type": "quarterly",
                "period_end": period_end,
                "period_duration": "LTM",
                "fiscal_year": fiscal_year,
                "section": section,
                "statement_type": "profit_loss",
                "canonical_name": canonical_field,
                "original_name": original_name,
                "value": value,
                "method": method,
                "source_file": source_file,
                "source_pages": source_pages,
                "source_url": source_url,
                "qc_flag": qc_flag,
            }
            rows.append(row)

    return rows


def main():
    parser = argparse.ArgumentParser(description="Flatten P&L data for D1")
    parser.add_argument("--ticker", help="Process single ticker only")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Get all quarterly JSON files (for 3M periods)
    quarterly_files = sorted(QUARTERLY_PL_DIR.glob("*.json"))

    # Get all json_pl files (for cumulative periods: 6M, 9M, 12M)
    json_pl_files = sorted(JSON_PL_DIR.glob("*.json"))
    ltm_pl_files = sorted(LTM_PL_DIR.glob("*.json"))

    # Filter by ticker if specified
    if args.ticker:
        quarterly_files = [f for f in quarterly_files if f.stem == args.ticker]
        json_pl_files = [f for f in json_pl_files if f.stem == args.ticker]
        ltm_pl_files = [f for f in ltm_pl_files if f.stem == args.ticker]

    print(f"Flattening P&L data...")
    print(f"  3M periods from:  {QUARTERLY_PL_DIR} ({len(quarterly_files)} files)")
    print(f"  Cumulative from:  {JSON_PL_DIR} ({len(json_pl_files)} files)")
    print(f"  LTM periods from: {LTM_PL_DIR} ({len(ltm_pl_files)} files)")
    print(f"  Output: {OUTPUT_FILE}")
    print()

    # Collect all rows
    all_rows = []
    field_stats = defaultdict(int)
    ticker_stats = defaultdict(int)
    duration_stats = defaultdict(int)
    qc_flag_stats = defaultdict(int)

    # Process quarterly files for 3M periods
    print("Processing 3M periods from quarterly_pl...")
    quarterly_count = 0
    for filepath in quarterly_files:
        rows = parse_quarterly_file(filepath)
        if rows:
            all_rows.extend(rows)
            quarterly_count += 1
            for row in rows:
                duration_stats[row['period_duration']] += 1

    print(f"  Loaded {quarterly_count} tickers with 3M periods")

    # Process json_pl files for cumulative periods (6M, 9M, 12M)
    print("Processing cumulative periods from json_pl...")
    json_pl_count = 0
    for filepath in json_pl_files:
        rows = parse_json_pl_file(filepath)
        if rows:
            all_rows.extend(rows)
            json_pl_count += 1
            for row in rows:
                duration_stats[row['period_duration']] += 1

    print(f"  Loaded {json_pl_count} tickers with cumulative periods")

    # Process ltm_pl files for LTM periods
    print("Processing LTM periods from ltm_pl...")
    ltm_pl_count = 0
    for filepath in ltm_pl_files:
        rows = parse_ltm_pl_file(filepath)
        if rows:
            all_rows.extend(rows)
            ltm_pl_count += 1
            for row in rows:
                duration_stats[row['period_duration']] += 1

    print(f"  Loaded {ltm_pl_count} tickers with LTM periods")

    # Write rows
    with open(OUTPUT_FILE, 'w') as out:
        for row in all_rows:
            out.write(json.dumps(row) + "\n")
            field_stats[row["canonical_name"]] += 1
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
