#!/usr/bin/env python3
"""
Step 2: QC Pre-Upload Validation

Validates flattened statement data before upload to D1.

Checks:
1. Required fields present (no nulls)
2. No duplicate rows (ticker + period_end + section + field)
3. Value sanity (reasonable ranges)
4. Coverage (quarters per ticker, fields per quarter)

Input:  data/flat/{pl,bs,cf}.jsonl
Output: artifacts/stage5/step2_qc_pre_upload_{pl,bs,cf}.json

Usage:
    python3 Step2_QCPreUpload.py --type pl    # QC P&L only
    python3 Step2_QCPreUpload.py --type bs    # QC Balance Sheet only
    python3 Step2_QCPreUpload.py --type cf    # QC Cash Flow only
    python3 Step2_QCPreUpload.py --type all   # QC all statement types
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_FLAT_DIR = PROJECT_ROOT / "data" / "flat"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage5"

# Statement type configs
STATEMENT_TYPES = {
    'pl': {
        'input': 'pl.jsonl',
        'output': 'step2_qc_pre_upload_pl.json',
        'name': 'Profit & Loss',
        'expected_fields': {"revenue_net", "gross_profit", "net_profit", "profit_before_tax"},
        'must_be_positive': {"revenue_gross", "revenue_net", "fee_income", "interest_income", "premium_income", "dividend_income"}
    },
    'bs': {
        'input': 'bs.jsonl',
        'output': 'step2_qc_pre_upload_bs.json',
        'name': 'Balance Sheet',
        'expected_fields': {"total_assets", "total_equity", "total_liabilities"},
        'must_be_positive': {"total_assets", "total_equity", "share_capital", "cash_and_equivalents", "inventory", "receivables"}
    },
    'cf': {
        'input': 'cf.jsonl',
        'output': 'step2_qc_pre_upload_cf.json',
        'name': 'Cash Flow',
        'expected_fields': {"cfo", "cfi", "cff"},
        'must_be_positive': set()  # CF items can all be negative
    },
}

# Required fields that must not be null
REQUIRED_FIELDS = ["ticker", "period_end", "canonical_field", "value", "statement_type"]

# Maximum reasonable value (10 trillion PKR in thousands = 10 billion thousands)
MAX_VALUE = 10_000_000_000


def load_data(input_file: Path) -> list[dict]:
    """Load flattened JSONL data."""
    rows = []
    with open(input_file) as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def check_required_fields(rows: list[dict]) -> list[dict]:
    """Check for missing required fields."""
    issues = []
    for i, row in enumerate(rows):
        for field in REQUIRED_FIELDS:
            if row.get(field) is None:
                issues.append({
                    "type": "missing_required_field",
                    "row": i,
                    "ticker": row.get("ticker"),
                    "period_end": row.get("period_end"),
                    "field": field
                })
    return issues


def check_duplicates(rows: list[dict]) -> list[dict]:
    """Check for duplicate rows (same ticker + period + duration + section + field + original_name)."""
    seen = {}
    issues = []

    for i, row in enumerate(rows):
        # Include period_duration in key - 3M vs 12M are different data points
        # Include original_name in key - multiple items can map to same canonical
        key = (
            row.get("ticker"),
            row.get("period_end"),
            row.get("period_duration"),  # Different durations are not duplicates
            row.get("section"),
            row.get("canonical_field"),
            row.get("original_name")  # Different line items are not duplicates
        )
        if key in seen:
            issues.append({
                "type": "duplicate_row",
                "row": i,
                "ticker": row.get("ticker"),
                "period_end": row.get("period_end"),
                "period_duration": row.get("period_duration"),
                "section": row.get("section"),
                "field": row.get("canonical_field"),
                "original_name": row.get("original_name"),
                "first_occurrence": seen[key]
            })
        else:
            seen[key] = i

    return issues


def check_value_sanity(rows: list[dict], must_be_positive: set) -> list[dict]:
    """Check for unreasonable values.

    Skips rows that already have a qc_flag - those have been reviewed during
    the flatten/derivation step and are known issues.
    """
    issues = []

    for i, row in enumerate(rows):
        value = row.get("value")
        field = row.get("canonical_field", "")
        ticker = row.get("ticker")
        unit_type = row.get("unit_type", "thousands")
        qc_flag = row.get("qc_flag", "")

        if value is None:
            continue

        # Skip rows with existing qc_flag - already reviewed
        if qc_flag:
            continue

        # Adjust max value based on unit type
        # Base MAX_VALUE is 10 trillion in thousands = 10 billion
        # For rupees, multiply by 1000
        max_val = MAX_VALUE * 1000 if unit_type == "rupees" else MAX_VALUE

        # Check for extremely large values
        if abs(value) > max_val:
            issues.append({
                "type": "value_too_large",
                "row": i,
                "ticker": ticker,
                "period_end": row.get("period_end"),
                "field": field,
                "value": value,
                "unit_type": unit_type
            })

        # Check for unexpected negatives
        if value < 0 and field in must_be_positive:
            if abs(value) > 1:  # Ignore rounding errors
                issues.append({
                    "type": "unexpected_negative",
                    "row": i,
                    "ticker": ticker,
                    "period_end": row.get("period_end"),
                    "field": field,
                    "value": value
                })

    return issues


def check_coverage(rows: list[dict], expected_fields: set) -> dict:
    """Analyze coverage statistics."""
    ticker_periods = defaultdict(set)
    period_fields = defaultdict(set)
    field_counts = defaultdict(int)

    for row in rows:
        ticker = row.get("ticker")
        period_end = row.get("period_end")
        section = row.get("section")
        field = row.get("canonical_field")

        ticker_periods[ticker].add((period_end, section))
        period_fields[(ticker, period_end, section)].add(field)
        field_counts[field] += 1

    # Tickers with very few periods
    low_coverage_tickers = []
    for ticker, periods in ticker_periods.items():
        if len(periods) < 4:
            low_coverage_tickers.append({
                "ticker": ticker,
                "periods": len(periods)
            })

    # Periods missing expected fields
    missing_fields = []
    for (ticker, period_end, section), fields in period_fields.items():
        missing = expected_fields - fields
        if missing and len(missing) >= len(expected_fields) - 1:
            missing_fields.append({
                "ticker": ticker,
                "period_end": period_end,
                "section": section,
                "missing": list(missing)
            })

    return {
        "total_tickers": len(ticker_periods),
        "total_periods": sum(len(p) for p in ticker_periods.values()),
        "total_rows": len(rows),
        "unique_fields": len(field_counts),
        "field_distribution": dict(sorted(field_counts.items(), key=lambda x: -x[1])),
        "low_coverage_tickers": sorted(low_coverage_tickers, key=lambda x: x["periods"]),
        "periods_missing_fields": missing_fields[:20]
    }


def run_qc(stmt_type: str, config: dict) -> dict:
    """Run QC for a single statement type."""
    input_file = DATA_FLAT_DIR / config['input']
    output_file = ARTIFACTS_DIR / config['output']
    statement_name = config['name']

    print()
    print("=" * 60)
    print(f"QC: {statement_name.upper()}")
    print("=" * 60)

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        print(f"Run Step1_Flatten{stmt_type.upper()}.py first.")
        return None

    print(f"Loading data from {input_file}...")
    rows = load_data(input_file)
    print(f"Loaded {len(rows):,} rows")
    print()

    # Run all checks
    print("Running QC checks...")

    issues = {
        "missing_required": check_required_fields(rows),
        "duplicates": check_duplicates(rows),
        "value_sanity": check_value_sanity(rows, config['must_be_positive']),
    }

    coverage = check_coverage(rows, config['expected_fields'])

    # Count total issues
    total_issues = sum(len(v) for v in issues.values())

    # Build report
    report = {
        "statement_type": stmt_type,
        "input_file": str(input_file),
        "total_rows": len(rows),
        "total_issues": total_issues,
        "issues_by_type": {k: len(v) for k, v in issues.items()},
        "coverage": coverage,
        "issues": issues
    }

    # Write report
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary
    print()
    print(f"Total rows:   {len(rows):,}")
    print(f"Total issues: {total_issues}")
    print()
    print("Issues by type:")
    for issue_type, issue_list in issues.items():
        status = "OK" if len(issue_list) == 0 else f"{len(issue_list)}"
        print(f"  {issue_type}: {status}")
    print()
    print("Coverage:")
    print(f"  Tickers: {coverage['total_tickers']}")
    print(f"  Periods: {coverage['total_periods']:,}")
    print(f"  Fields:  {coverage['unique_fields']}")

    if coverage['low_coverage_tickers']:
        print(f"\nLow coverage tickers ({len(coverage['low_coverage_tickers'])}):")
        for t in coverage['low_coverage_tickers'][:5]:
            print(f"  {t['ticker']}: {t['periods']} periods")

    print(f"\nReport: {output_file}")

    return report


def main():
    parser = argparse.ArgumentParser(description="QC pre-upload validation")
    parser.add_argument("--type", required=True, choices=['pl', 'bs', 'cf', 'all'],
                        help="Statement type to QC")
    args = parser.parse_args()

    # Determine which types to process
    if args.type == 'all':
        types_to_process = ['pl', 'bs', 'cf']
    else:
        types_to_process = [args.type]

    all_passed = True
    results = {}

    for stmt_type in types_to_process:
        config = STATEMENT_TYPES[stmt_type]
        report = run_qc(stmt_type, config)
        if report:
            results[stmt_type] = report
            if report['total_issues'] > 0:
                all_passed = False
        else:
            all_passed = False

    # Final summary
    print()
    print("=" * 60)
    print("QC SUMMARY")
    print("=" * 60)

    for stmt_type, report in results.items():
        status = "PASS" if report['total_issues'] == 0 else f"ISSUES: {report['total_issues']}"
        print(f"  {stmt_type.upper()}: {report['total_rows']:,} rows - {status}")

    if all_passed:
        print("\nAll checks passed. Ready for upload.")
        return 0
    else:
        print("\nReview issues before uploading!")
        return 1


if __name__ == "__main__":
    exit(main())
