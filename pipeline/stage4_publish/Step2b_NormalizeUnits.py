#!/usr/bin/env python3
"""
Step 2b: Normalize All Monetary Values to 000s (Thousands)

This step ensures all monetary values are in the same unit (000s) for database upload.

Unit conversions:
- "Rs" or "Rupees": divide by 1000
- "000s" or "thousands": no change
- "millions": multiply by 1000
- Unknown: log warning, treat as already normalized

Input:  statements_unified.jsonl
Output: statements_normalized.jsonl

Usage:
    python3 Step2b_NormalizeUnits.py
    python3 Step2b_NormalizeUnits.py --ticker GGGL  # Debug single ticker
"""

import argparse
import json
import re
from pathlib import Path
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

INPUT_FILE = PROJECT_ROOT / "statements_unified.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "statements_normalized.jsonl"

# Fields that should NOT be normalized (per-share values, ratios, etc.)
PER_SHARE_FIELDS = {
    'eps_basic', 'eps_diluted', 'eps', 'earnings_per_share',
    'dps', 'dividend_per_share', 'dividends_per_share',
    'book_value_per_share', 'nav_per_share',
    'shares_outstanding', 'weighted_avg_shares',
}


def normalize_unit_type(unit_type: str) -> str:
    """Normalize unit_type string to standard form."""
    if not unit_type:
        return 'unknown'

    ut = unit_type.lower().strip()

    if ut in ('rs', 'rupees', 'rs.', 'pkr'):
        return 'full_rupees'
    elif ut in ('000s', '000', 'thousands', '000\'s', "000's"):
        return '000s'
    elif ut in ('millions', 'mn', 'million'):
        return 'millions'
    else:
        return 'unknown'


def get_multiplier(unit_type: str) -> float:
    """Get multiplier to convert to 000s."""
    normalized = normalize_unit_type(unit_type)

    if normalized == 'full_rupees':
        return 0.001  # Divide by 1000
    elif normalized == '000s':
        return 1.0    # No change
    elif normalized == 'millions':
        return 1000.0  # Multiply by 1000
    else:
        return 1.0    # Unknown - assume already normalized


def should_normalize_field(canonical_field: str) -> bool:
    """Check if field should be normalized (not per-share values)."""
    if not canonical_field:
        return True

    cf_lower = canonical_field.lower()

    # Check against per-share fields
    for ps_field in PER_SHARE_FIELDS:
        if ps_field in cf_lower:
            return False

    # Per-share patterns
    if 'per_share' in cf_lower or 'per share' in cf_lower:
        return False

    return True


def main():
    parser = argparse.ArgumentParser(description="Normalize monetary values to 000s")
    parser.add_argument("--input", type=Path, default=INPUT_FILE, help="Input JSONL file")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE, help="Output JSONL file")
    parser.add_argument("--ticker", help="Debug single ticker")
    args = parser.parse_args()

    print(f"Normalizing units in {args.input}...")
    print(f"Output: {args.output}")
    print()

    stats = Counter()
    unit_type_examples = {}  # unit_type -> (ticker, before, after)
    per_share_preserved = []

    with open(args.input) as f_in, open(args.output, 'w') as f_out:
        for line in f_in:
            row = json.loads(line)

            # Filter for single ticker if specified
            if args.ticker and row['ticker'] != args.ticker:
                continue

            original_value = row.get('value')
            original_unit = row.get('unit_type', '000s')
            canonical_field = row.get('canonical_field', '')

            stats['total_rows'] += 1
            stats[f'unit_type_{normalize_unit_type(original_unit)}'] += 1

            # Check if this is a per-share field that shouldn't be normalized
            if not should_normalize_field(canonical_field):
                stats['per_share_preserved'] += 1
                if len(per_share_preserved) < 5:
                    per_share_preserved.append({
                        'ticker': row['ticker'],
                        'field': canonical_field,
                        'value': original_value
                    })
                # Don't modify value, just update unit_type for consistency
                row['unit_type'] = '000s'
                f_out.write(json.dumps(row) + '\n')
                continue

            # Get multiplier and normalize value
            multiplier = get_multiplier(original_unit)

            if original_value is not None and multiplier != 1.0:
                normalized_value = original_value * multiplier

                # Track example for each unit type
                norm_unit = normalize_unit_type(original_unit)
                if norm_unit not in unit_type_examples:
                    unit_type_examples[norm_unit] = {
                        'ticker': row['ticker'],
                        'field': canonical_field,
                        'original_unit': original_unit,
                        'before': original_value,
                        'after': normalized_value
                    }

                row['value'] = normalized_value
                stats['values_normalized'] += 1

            # Always set unit_type to 000s after normalization
            row['unit_type'] = '000s'

            f_out.write(json.dumps(row) + '\n')

    # Print summary
    print("=" * 60)
    print("NORMALIZATION COMPLETE")
    print("=" * 60)
    print(f"Total rows: {stats['total_rows']:,}")
    print(f"Values normalized: {stats['values_normalized']:,}")
    print(f"Per-share fields preserved: {stats['per_share_preserved']:,}")
    print()

    print("Unit types found:")
    for key, count in sorted(stats.items()):
        if key.startswith('unit_type_'):
            unit_name = key.replace('unit_type_', '')
            pct = count / stats['total_rows'] * 100
            print(f"  {unit_name}: {count:,} ({pct:.1f}%)")

    print()
    print("Examples of normalization:")
    for unit_type, example in unit_type_examples.items():
        if unit_type != '000s':  # Only show converted ones
            print(f"  {unit_type} -> 000s:")
            print(f"    Ticker: {example['ticker']}, Field: {example['field']}")
            print(f"    Before: {example['before']:,.2f} {example['original_unit']}")
            print(f"    After:  {example['after']:,.2f} 000s")

    if per_share_preserved:
        print()
        print("Per-share fields preserved (not normalized):")
        for ex in per_share_preserved[:3]:
            print(f"  {ex['ticker']}: {ex['field']} = {ex['value']}")

    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
