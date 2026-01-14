#!/usr/bin/env python3
"""
Step 0: Validate Mapping Between Presented and Standardized Statements

QC check that compares:
1. Line item mappings (original text -> canonical field)
2. Value consistency between presented and standardized
3. Coverage (what % of presented line items map to standardized)

Outputs:
- mapping_report.json: Full mapping details
- mapping_summary.txt: Human-readable summary

Usage:
    python3 Step0_ValidateMapping.py
    python3 Step0_ValidateMapping.py --ticker LUCK
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict, Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PRESENTED_DIR = PROJECT_ROOT / "statements_json" / "presented"
STANDARDIZED_DIR = PROJECT_ROOT / "statements_json" / "standardized"
OUTPUT_DIR = PROJECT_ROOT / "mapping_reports"


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


def normalize_line_item(text: str) -> str:
    """Normalize line item text for matching."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = ' '.join(text.split())
    return text


def extract_values(data: dict, section: str, stmt_type: str, period_key: str) -> dict:
    """Extract line item -> value mapping from a statement."""
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

    # Find the main value column (first non-"Line Item" column)
    value_col = None
    for col in columns:
        if col != 'Line Item':
            value_col = col
            break

    if not value_col:
        return result

    for row in stmt.get('rows', []):
        line_item = row.get('Line Item', '')
        if not line_item or line_item == '-':
            continue
        value = parse_value(row.get(value_col))
        if value is not None:
            result[line_item] = {
                'value': value,
                'column': value_col,
                'canonical_field': row.get('canonical_field')
            }

    return result


def compare_ticker(ticker: str, presented_data: dict, standardized_data: dict) -> dict:
    """Compare presented vs standardized for a ticker using VALUE-BASED matching."""
    result = {
        'ticker': ticker,
        'periods': {},
        'summary': {
            'total_presented_items': 0,
            'total_standardized_items': 0,
            'value_matches': 0,
            'value_mismatches': 0,
            'unmatched_presented': 0
        }
    }

    # Get all periods from both
    presented_periods = set(presented_data.get('periods', {}).keys())
    standardized_periods = set(standardized_data.get('periods', {}).keys())
    common_periods = presented_periods & standardized_periods

    for period_key in sorted(common_periods):
        period_result = {
            'in_presented': True,
            'in_standardized': True,
            'sections': {}
        }

        for section in ['consolidated', 'unconsolidated', 'standalone']:
            for stmt_type in ['profit_loss', 'balance_sheet', 'cash_flow']:
                presented_values = extract_values(presented_data, section, stmt_type, period_key)
                standardized_values = extract_values(standardized_data, section, stmt_type, period_key)

                if not presented_values or not standardized_values:
                    continue

                section_key = f"{section}_{stmt_type}"
                mappings = []

                # Build value -> canonical field mapping from standardized
                # In standardized, Line Item IS the canonical field name
                std_by_value = {}
                for canonical_field, info in standardized_values.items():
                    val = info['value']
                    if val not in std_by_value:
                        std_by_value[val] = []
                    std_by_value[val].append(canonical_field)

                # Match presented items to standardized by value
                for pres_item, pres_info in presented_values.items():
                    pres_value = pres_info['value']

                    mapping = {
                        'presented_item': pres_item,
                        'canonical_field': None,
                        'presented_value': pres_value,
                        'match_status': 'unmatched'
                    }

                    # Exact value match
                    if pres_value in std_by_value:
                        candidates = std_by_value[pres_value]
                        mapping['canonical_field'] = candidates[0] if len(candidates) == 1 else candidates
                        mapping['match_status'] = 'exact_match'
                        result['summary']['value_matches'] += 1
                    else:
                        # Try fuzzy match (within 0.1%)
                        found = False
                        for std_val, candidates in std_by_value.items():
                            if pres_value != 0 and std_val != 0:
                                diff_pct = abs(pres_value - std_val) / max(abs(pres_value), abs(std_val)) * 100
                                if diff_pct < 0.1:
                                    mapping['canonical_field'] = candidates[0] if len(candidates) == 1 else candidates
                                    mapping['match_status'] = 'fuzzy_match'
                                    mapping['diff_pct'] = round(diff_pct, 4)
                                    result['summary']['value_matches'] += 1
                                    found = True
                                    break
                        if not found:
                            result['summary']['unmatched_presented'] += 1

                    mappings.append(mapping)
                    result['summary']['total_presented_items'] += 1

                result['summary']['total_standardized_items'] += len(standardized_values)

                if mappings:
                    period_result['sections'][section_key] = {
                        'presented_count': len(presented_values),
                        'standardized_count': len(standardized_values),
                        'mappings': mappings
                    }

        if period_result['sections']:
            result['periods'][period_key] = period_result

    return result


def generate_global_mapping(all_results: list) -> dict:
    """Generate a global mapping of presented line items to canonical fields."""
    mapping = defaultdict(lambda: {'canonical_fields': Counter(), 'count': 0})

    for ticker_result in all_results:
        for period_key, period_data in ticker_result.get('periods', {}).items():
            for section_key, section_data in period_data.get('sections', {}).items():
                for m in section_data.get('mappings', []):
                    pres_item = m['presented_item']
                    canonical = m.get('canonical_field')
                    if canonical:
                        # Handle case where canonical is a list (multiple matches)
                        if isinstance(canonical, list):
                            for c in canonical:
                                mapping[pres_item]['canonical_fields'][c] += 1
                        else:
                            mapping[pres_item]['canonical_fields'][canonical] += 1
                    mapping[pres_item]['count'] += 1

    # Convert to regular dict and find most common mapping
    result = {}
    for pres_item, data in mapping.items():
        if data['canonical_fields']:
            most_common = data['canonical_fields'].most_common(1)[0]
            result[pres_item] = {
                'canonical_field': most_common[0],
                'confidence': most_common[1] / data['count'],
                'total_occurrences': data['count'],
                'alternatives': dict(data['canonical_fields'])
            }
        else:
            result[pres_item] = {
                'canonical_field': None,
                'confidence': 0,
                'total_occurrences': data['count'],
                'alternatives': {}
            }

    return result


def main():
    parser = argparse.ArgumentParser(description="Validate mapping between presented and standardized")
    parser.add_argument("--ticker", help="Process single ticker")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)

    # Get ticker list
    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = sorted([f.stem for f in PRESENTED_DIR.glob("*.json")])

    print(f"Validating mapping for {len(tickers)} tickers...")
    print(f"Presented: {PRESENTED_DIR}")
    print(f"Standardized: {STANDARDIZED_DIR}")
    print()

    all_results = []
    global_summary = {
        'total_tickers': len(tickers),
        'tickers_with_issues': 0,
        'total_value_matches': 0,
        'total_value_mismatches': 0,
        'total_unmatched': 0,
        'mismatches_by_ticker': []
    }

    for ticker in tickers:
        presented_file = PRESENTED_DIR / f"{ticker}.json"
        standardized_file = STANDARDIZED_DIR / f"{ticker}.json"

        if not presented_file.exists() or not standardized_file.exists():
            print(f"  {ticker}: Missing file (presented={presented_file.exists()}, standardized={standardized_file.exists()})")
            continue

        with open(presented_file) as f:
            presented_data = json.load(f)
        with open(standardized_file) as f:
            standardized_data = json.load(f)

        result = compare_ticker(ticker, presented_data, standardized_data)
        all_results.append(result)

        summary = result['summary']
        global_summary['total_value_matches'] += summary['value_matches']
        global_summary['total_value_mismatches'] += summary['value_mismatches']
        global_summary['total_unmatched'] += summary['unmatched_presented']

        if summary['value_mismatches'] > 0:
            global_summary['tickers_with_issues'] += 1
            global_summary['mismatches_by_ticker'].append({
                'ticker': ticker,
                'mismatches': summary['value_mismatches'],
                'matches': summary['value_matches']
            })

        status = "✓" if summary['value_mismatches'] == 0 else f"⚠ {summary['value_mismatches']} mismatches"
        print(f"  {ticker}: {summary['value_matches']} matches, {status}")

    # Generate global mapping
    print("\nGenerating global line item mapping...")
    global_mapping = generate_global_mapping(all_results)

    # Sort by occurrence count
    sorted_mapping = dict(sorted(
        global_mapping.items(),
        key=lambda x: x[1]['total_occurrences'],
        reverse=True
    ))

    # Write outputs
    report_file = OUTPUT_DIR / "mapping_report.json"
    with open(report_file, 'w') as f:
        json.dump({
            'summary': global_summary,
            'global_mapping': sorted_mapping,
            'ticker_details': all_results
        }, f, indent=2)

    # Write human-readable summary
    summary_file = OUTPUT_DIR / "mapping_summary.txt"
    with open(summary_file, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("PRESENTED -> STANDARDIZED MAPPING VALIDATION\n")
        f.write("=" * 70 + "\n\n")

        f.write("GLOBAL SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Tickers analyzed: {global_summary['total_tickers']}\n")
        f.write(f"Value matches: {global_summary['total_value_matches']:,}\n")
        f.write(f"Value mismatches: {global_summary['total_value_mismatches']:,}\n")
        f.write(f"Unmatched items: {global_summary['total_unmatched']:,}\n")

        total_compared = global_summary['total_value_matches'] + global_summary['total_value_mismatches']
        if total_compared > 0:
            match_rate = global_summary['total_value_matches'] / total_compared * 100
            f.write(f"Match rate: {match_rate:.1f}%\n")

        f.write(f"\nTickers with mismatches: {global_summary['tickers_with_issues']}\n")
        for t in sorted(global_summary['mismatches_by_ticker'], key=lambda x: -x['mismatches'])[:20]:
            f.write(f"  {t['ticker']}: {t['mismatches']} mismatches / {t['matches']} matches\n")

        f.write("\n" + "=" * 70 + "\n")
        f.write("TOP LINE ITEM MAPPINGS (by frequency)\n")
        f.write("=" * 70 + "\n\n")

        for i, (pres_item, info) in enumerate(list(sorted_mapping.items())[:100]):
            canonical = info['canonical_field'] or '[UNMAPPED]'
            conf = info['confidence'] * 100
            count = info['total_occurrences']
            f.write(f"{pres_item}\n")
            f.write(f"  -> {canonical} (conf: {conf:.0f}%, n={count})\n")
            if len(info['alternatives']) > 1:
                f.write(f"     alts: {info['alternatives']}\n")
            f.write("\n")

    # Print final summary
    print("\n" + "=" * 60)
    print("VALIDATION COMPLETE")
    print("=" * 60)
    print(f"Tickers: {global_summary['total_tickers']}")
    print(f"Value matches: {global_summary['total_value_matches']:,}")
    print(f"Value mismatches: {global_summary['total_value_mismatches']:,}")
    print(f"Unmatched: {global_summary['total_unmatched']:,}")

    total_compared = global_summary['total_value_matches'] + global_summary['total_value_mismatches']
    if total_compared > 0:
        match_rate = global_summary['total_value_matches'] / total_compared * 100
        print(f"Match rate: {match_rate:.1f}%")

    print(f"\nUnique line items in mapping: {len(global_mapping):,}")
    print(f"\nOutputs:")
    print(f"  {report_file}")
    print(f"  {summary_file}")


if __name__ == "__main__":
    main()
