#!/usr/bin/env python3
"""
Step 2: Unify Statements with Embedded Mappings

Creates a single statements table where each row includes:
- Canonical field name (standardized)
- Original name(s) from source document
- Mapping type: 'direct', 'aggregate', or 'calculated'

Key design decisions:
- Balance sheets are point-in-time (no duration)
- P&L and Cash Flow have durations (3M, 6M, 9M, 12M, LTM)
- Deduplicates rows that appear in multiple filings
- Prefers primary source over comparative columns

Input:  statements_json/presented/*.json, statements_json/standardized/*.json
Output: statements_unified.jsonl

Usage:
    python3 Step2_UnifyStatements.py
    python3 Step2_UnifyStatements.py --ticker LUCK
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict, Counter
from itertools import combinations

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PRESENTED_DIR = PROJECT_ROOT / "statements_json" / "presented"
STANDARDIZED_DIR = PROJECT_ROOT / "statements_json" / "standardized"
OUTPUT_FILE = PROJECT_ROOT / "statements_unified.jsonl"

# Load ticker metadata
with open(PROJECT_ROOT / 'tickers_new.json') as f:
    tickers_data = json.load(f)
TICKER_TO_INDUSTRY = {t['Symbol']: t['Industry'] for t in tickers_data}
TICKER_TO_COMPANY = {t['Symbol']: t['Company Name'] for t in tickers_data}

MONTH_TO_NUM = {
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
}


def parse_value(value_str: str) -> float | None:
    """Parse formatted number string to float."""
    if value_str is None or value_str == '-' or value_str == '':
        return None
    value_str = str(value_str).replace(',', '')
    if value_str.startswith('(') and value_str.endswith(')'):
        value_str = '-' + value_str[1:-1]
    try:
        return float(value_str)
    except ValueError:
        return None


def parse_column_header(col: str, stmt_type: str = None) -> dict | None:
    """
    Parse column header to extract period info.

    For P&L and Cash Flow: "9M Sep 2025" -> duration=9M
    For Balance Sheet: "Sep 30, 2025" -> duration=None (point-in-time)
    """
    # Match: "9M Sep 2025" or "12M Dec 2024" (P&L, Cash Flow)
    match = re.match(r'(\d+)M\s+([A-Za-z]+)\s+(\d{4})', col)
    if match:
        month = match.group(2).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        year = match.group(3)
        day = '30' if month_num in ('04', '06', '09', '11') else ('28' if month_num == '02' else '31')
        return {
            'duration': f'{match.group(1)}M',
            'month': match.group(2),
            'year': int(year),
            'period_end': f'{year}-{month_num}-{day}',
            'is_point_in_time': False
        }

    # Match: "Sep 30, 2025" (Balance Sheet - point-in-time)
    match = re.match(r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})', col)
    if match:
        month = match.group(1).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        day = match.group(2).zfill(2)
        year = match.group(3)
        return {
            'duration': None,  # Balance sheets are point-in-time
            'month': match.group(1)[:3].capitalize(),
            'year': int(year),
            'period_end': f'{year}-{month_num}-{day}',
            'is_point_in_time': True
        }

    # Match: "LTM Sep 2025"
    match = re.match(r'LTM\s+([A-Za-z]+)\s+(\d{4})', col)
    if match:
        month = match.group(1).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        year = match.group(2)
        day = '30' if month_num in ('04', '06', '09', '11') else ('28' if month_num == '02' else '31')
        return {
            'duration': 'LTM',
            'month': match.group(1),
            'year': int(year),
            'period_end': f'{year}-{month_num}-{day}',
            'is_point_in_time': False
        }

    return None


def get_period_type_from_key(period_key: str) -> str:
    """Extract period type from period_key like 'annual_2024' or 'quarterly_2025-09-30'."""
    if period_key.startswith('annual'):
        return 'annual'
    elif period_key.startswith('quarterly'):
        return 'quarterly'
    return 'unknown'


def extract_calculated_3m(data: dict, period_key: str, section: str, stmt_type: str) -> dict:
    """Extract calculated 3M standalone quarters from a period."""
    result = {}

    period_data = data.get('periods', {}).get(period_key, {})

    # Try section (consolidated/unconsolidated) or statements directly
    if section in period_data:
        section_data = period_data[section]
    elif 'statements' in period_data and section == 'standalone':
        section_data = period_data['statements']
    else:
        return result

    calc_3m = section_data.get('calculated_3m', {}).get(stmt_type, {})

    for calc_col, calc_stmt in calc_3m.items():
        col_values = {}
        for row in calc_stmt.get('rows', []):
            line_item = row.get('Line Item', '')
            if not line_item or line_item == '-':
                continue
            value = parse_value(row.get(calc_col))
            if value is not None:
                col_values[line_item] = value

        if col_values:
            result[calc_col] = col_values

    return result


def extract_statement_values(data: dict, period_key: str, section: str, stmt_type: str) -> dict:
    """Extract line_item -> {column: value} from a statement."""
    result = {}

    period_data = data.get('periods', {}).get(period_key, {})

    # Try section (consolidated/unconsolidated) or statements directly
    if section in period_data:
        section_data = period_data[section]
    elif 'statements' in period_data and section == 'standalone':
        section_data = period_data['statements']
    else:
        return result

    stmt = section_data.get(stmt_type, {})
    columns = stmt.get('columns', [])

    for row in stmt.get('rows', []):
        line_item = row.get('Line Item', '')
        if not line_item or line_item == '-':
            continue

        for col in columns:
            if col == 'Line Item':
                continue
            value = parse_value(row.get(col))
            if value is not None:
                if line_item not in result:
                    result[line_item] = {}
                result[line_item][col] = value

    return result


# Semantic keywords for aggregate validation
AGGREGATE_KEYWORDS = {
    'revenue_deductions': ['tax', 'duty', 'excise', 'rebate', 'discount', 'commission', 'levy'],
    'operating_expenses': ['distribution', 'admin', 'selling', 'general', 'marketing', 'expense'],
    'taxation': ['tax', 'current', 'deferred', 'income tax'],
    'other_operating': ['other', 'expense', 'charge', 'impairment'],
    'finance_cost': ['finance', 'interest', 'markup', 'mark-up'],
    'depreciation_amortization': ['depreciation', 'amortization', 'amortisation'],
    'other_non_current_assets': ['long term', 'long-term', 'deposit', 'advance', 'receivable'],
    'other_current_assets': ['advance', 'deposit', 'prepayment', 'receivable', 'loan'],
    'other_current_liabilities': ['accrued', 'payable', 'provision', 'current portion'],
    'other_non_current_liabilities': ['deferred', 'long term', 'long-term', 'provision'],
    'reserves': ['reserve', 'surplus', 'premium'],
    'retained_earnings': ['unappropriated', 'retained', 'accumulated', 'profit'],
}


def is_valid_aggregate(std_item: str, component_names: list) -> bool:
    """Check if aggregate components are semantically related to the canonical field."""
    keywords = AGGREGATE_KEYWORDS.get(std_item, [])
    if not keywords:
        return False

    for name in component_names:
        name_lower = name.lower()
        for kw in keywords:
            if kw in name_lower:
                return True
    return False


def find_original_names(std_item: str, std_value: float, presented_values: dict, tolerance_pct: float = 0.1) -> tuple:
    """
    Find the original presented name(s) that map to a standardized value.

    Returns: (mapping_type, original_names)
        - ('direct', 'Original Name') - exact 1:1 match
        - ('aggregate', ['Name1', 'Name2']) - sum of multiple items
        - ('calculated', None) - derived/calculated value
    """
    if std_value is None:
        return ('calculated', None)

    # Build value -> items mapping
    val_to_items = defaultdict(list)
    items_list = []
    for item, cols in presented_values.items():
        for col, val in cols.items():
            val_to_items[val].append(item)
            items_list.append((item, val))

    # Try exact match first
    if std_value in val_to_items:
        items = val_to_items[std_value]
        best_match = items[0]
        std_lower = std_item.lower().replace('_', ' ')
        for item in items:
            if std_lower in item.lower() or item.lower() in std_lower:
                best_match = item
                break
        return ('direct', best_match)

    # Try fuzzy match (within tolerance)
    for val, items in val_to_items.items():
        if std_value != 0 and val != 0:
            diff_pct = abs(std_value - val) / max(abs(std_value), abs(val)) * 100
            if diff_pct < tolerance_pct:
                return ('direct', items[0])

    # Try to find aggregate (sum of 2 items)
    for (item1, val1), (item2, val2) in combinations(items_list, 2):
        if abs(val1 + val2 - std_value) < abs(std_value) * 0.001:
            components = [item1, item2]
            if is_valid_aggregate(std_item, components):
                return ('aggregate', components)

    # Try to find aggregate (sum of 3 items)
    if len(items_list) >= 3:
        for combo in combinations(items_list, 3):
            total = sum(v for _, v in combo)
            if abs(total - std_value) < abs(std_value) * 0.001:
                components = [item for item, _ in combo]
                if is_valid_aggregate(std_item, components):
                    return ('aggregate', components)

    return ('calculated', None)


def process_ticker(ticker: str, presented_data: dict, standardized_data: dict) -> list:
    """Process a single ticker and return unified rows with deduplication."""

    industry = TICKER_TO_INDUSTRY.get(ticker, 'Unknown')
    company_name = TICKER_TO_COMPANY.get(ticker, ticker)
    unit_type = standardized_data.get('unit_type', '000s')

    # Use a dict for deduplication: key = (stmt_type, section, period_end, canonical_field)
    # Value = row dict (we keep the first/best source)
    rows_by_key = {}

    std_periods = set(standardized_data.get('periods', {}).keys())
    pres_periods = set(presented_data.get('periods', {}).keys())

    # Sort periods so primary (current year) comes before comparatives
    for period_key in sorted(std_periods):
        period_type = get_period_type_from_key(period_key)

        for section in ['consolidated', 'unconsolidated', 'standalone']:
            # Get presented values for mapping
            pres_values = {}
            if period_key in pres_periods:
                for stmt_type in ['profit_loss', 'balance_sheet', 'cash_flow']:
                    stmt_pres = extract_statement_values(presented_data, period_key, section, stmt_type)
                    if not stmt_pres and section in ['unconsolidated', 'standalone']:
                        stmt_pres = extract_statement_values(presented_data, period_key, 'standalone', stmt_type)
                    pres_values[stmt_type] = stmt_pres

            # Process each statement type
            for stmt_type in ['profit_loss', 'balance_sheet', 'cash_flow']:
                std_values = extract_statement_values(standardized_data, period_key, section, stmt_type)

                if not std_values:
                    continue

                stmt_pres_values = pres_values.get(stmt_type, {})
                is_balance_sheet = (stmt_type == 'balance_sheet')

                for canonical_field, col_values in std_values.items():
                    for col, value in col_values.items():
                        col_info = parse_column_header(col, stmt_type)
                        if not col_info:
                            continue

                        # Find original name(s)
                        mapping_type, original_names = find_original_names(
                            canonical_field, value, stmt_pres_values
                        )

                        # Deduplication key
                        dedup_key = (stmt_type, section, col_info['period_end'], canonical_field)

                        # Skip if we already have this row (prefer first occurrence)
                        if dedup_key in rows_by_key:
                            continue

                        # For balance sheets: no duration (point-in-time)
                        # For P&L/Cash Flow: use duration from column header
                        if is_balance_sheet:
                            period_duration = None
                        else:
                            # P&L and Cash Flow MUST have a duration
                            # Skip columns with date-format (duration=None) - these are malformed
                            if col_info['duration'] is None:
                                continue
                            period_duration = col_info['duration']

                        row = {
                            'ticker': ticker,
                            'company_name': company_name,
                            'industry': industry,
                            'unit_type': unit_type,
                            'period_type': period_type,
                            'period_end': col_info['period_end'],
                            'period_duration': period_duration,
                            'period_year': col_info['year'],
                            'section': section if section != 'standalone' else 'standalone',
                            'statement_type': stmt_type,
                            'canonical_field': canonical_field,
                            'value': value,
                            'mapping_type': mapping_type,
                            'original_name': original_names if mapping_type == 'direct' else None,
                            'original_names': original_names if mapping_type == 'aggregate' else None
                        }
                        rows_by_key[dedup_key] = row

                # Process calculated_3m (standalone quarters)
                # Only for P&L and Cash Flow, not balance sheet
                if not is_balance_sheet:
                    calc_3m = extract_calculated_3m(standardized_data, period_key, section, stmt_type)
                    for calc_col, calc_values in calc_3m.items():
                        col_info = parse_column_header(calc_col, stmt_type)
                        if not col_info:
                            continue

                        for canonical_field, value in calc_values.items():
                            dedup_key = (stmt_type, section, col_info['period_end'], canonical_field)

                            if dedup_key in rows_by_key:
                                continue

                            row = {
                                'ticker': ticker,
                                'company_name': company_name,
                                'industry': industry,
                                'unit_type': unit_type,
                                'period_type': 'quarterly',
                                'period_end': col_info['period_end'],
                                'period_duration': '3M',
                                'period_year': col_info['year'],
                                'section': section if section != 'standalone' else 'standalone',
                                'statement_type': stmt_type,
                                'canonical_field': canonical_field,
                                'value': value,
                                'mapping_type': 'calculated',
                                'original_name': None,
                                'original_names': None
                            }
                            rows_by_key[dedup_key] = row

    # Process LTM data (only for P&L and Cash Flow)
    ltm_info = standardized_data.get('ltm_info', {})
    for section_key, section_data in ltm_info.items():
        section = section_key.split('_')[0] if '_' in section_key else section_key

        for stmt_type, stmt_list in section_data.get('statements', {}).items():
            # Skip balance sheet for LTM
            if stmt_type == 'balance_sheet':
                continue

            if not isinstance(stmt_list, list):
                stmt_list = [stmt_list]

            for stmt in stmt_list:
                ltm_col = stmt.get('ltm_col', '')
                col_info = parse_column_header(ltm_col, stmt_type)
                if not col_info:
                    continue

                for ltm_row in stmt.get('rows', []):
                    canonical_field = ltm_row.get('Line Item', '')
                    if not canonical_field or canonical_field == '-':
                        continue

                    value = parse_value(ltm_row.get(ltm_col))
                    if value is None:
                        continue

                    dedup_key = (stmt_type, section, col_info['period_end'], canonical_field)

                    if dedup_key in rows_by_key:
                        continue

                    row = {
                        'ticker': ticker,
                        'company_name': company_name,
                        'industry': industry,
                        'unit_type': unit_type,
                        'period_type': 'ltm',
                        'period_end': col_info['period_end'],
                        'period_duration': 'LTM',
                        'period_year': col_info['year'],
                        'section': section,
                        'statement_type': stmt_type,
                        'canonical_field': canonical_field,
                        'value': value,
                        'mapping_type': 'calculated',
                        'original_name': None,
                        'original_names': None
                    }
                    rows_by_key[dedup_key] = row

    return list(rows_by_key.values())


def main():
    parser = argparse.ArgumentParser(description="Unify statements with embedded mappings")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE, help="Output file")
    args = parser.parse_args()

    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = sorted([f.stem for f in STANDARDIZED_DIR.glob("*.json")])

    print(f"Unifying statements for {len(tickers)} tickers...")
    print(f"Output: {args.output}")
    print()

    all_rows = []
    mapping_stats = Counter()
    stmt_type_stats = Counter()

    for ticker in tickers:
        presented_file = PRESENTED_DIR / f"{ticker}.json"
        standardized_file = STANDARDIZED_DIR / f"{ticker}.json"

        if not standardized_file.exists():
            continue

        with open(standardized_file) as f:
            standardized_data = json.load(f)

        if presented_file.exists():
            with open(presented_file) as f:
                presented_data = json.load(f)
        else:
            presented_data = {'periods': {}}

        rows = process_ticker(ticker, presented_data, standardized_data)
        all_rows.extend(rows)

        for row in rows:
            mapping_stats[row['mapping_type']] += 1
            stmt_type_stats[row['statement_type']] += 1

        direct = sum(1 for r in rows if r['mapping_type'] == 'direct')
        agg = sum(1 for r in rows if r['mapping_type'] == 'aggregate')
        calc = sum(1 for r in rows if r['mapping_type'] == 'calculated')
        print(f"  {ticker}: {len(rows)} rows (direct={direct}, aggregate={agg}, calculated={calc})")

    # Write output
    print(f"\nWriting {len(all_rows):,} rows...")
    with open(args.output, 'w') as f:
        for row in all_rows:
            f.write(json.dumps(row) + '\n')

    # Summary
    print()
    print("=" * 60)
    print("UNIFICATION COMPLETE")
    print("=" * 60)
    print(f"Total rows: {len(all_rows):,}")
    print(f"Unique tickers: {len(set(r['ticker'] for r in all_rows))}")
    print()
    print("By statement type:")
    for st, count in stmt_type_stats.most_common():
        print(f"  {st}: {count:,}")
    print()
    print("Mapping types:")
    for mtype, count in mapping_stats.most_common():
        pct = count / len(all_rows) * 100
        print(f"  {mtype}: {count:,} ({pct:.1f}%)")

    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
