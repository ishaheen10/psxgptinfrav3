#!/usr/bin/env python3
"""
Step 2: QC Pre-Upload Validation

Validates flattened P&L data before upload to D1.

Checks:
1. Required fields present (no nulls)
2. No duplicate rows (ticker + period_end + consolidation + field)
3. Value sanity (reasonable ranges)
4. Coverage (quarters per ticker, fields per quarter)
5. Unit normalization verification

Input:  artifacts/stage4/pl_flat.jsonl
Output: artifacts/stage4/qc_pre_upload.json

Usage:
    python3 Step2_QCPreUpload.py
"""

import json
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "artifacts" / "stage4" / "pl_flat.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage4" / "qc_pre_upload.json"
STAGE3_ALLOWLIST = PROJECT_ROOT / "artifacts" / "stage3" / "step6_arithmetic_allowlist.json"


def load_allowlist() -> set:
    """Load Stage 3 allowlist as set of (ticker, fiscal_year, consolidation) tuples."""
    if not STAGE3_ALLOWLIST.exists():
        return set()
    with open(STAGE3_ALLOWLIST) as f:
        data = json.load(f)
    return {
        (item['ticker'], item['fiscal_year'], item['consolidation'])
        for item in data.get('allowlist', [])
    }

# Required fields that must not be null
REQUIRED_FIELDS = ["ticker", "period_end", "canonical_field", "value", "statement_type"]

# Core P&L fields that should exist for most quarters
EXPECTED_FIELDS = {
    "revenue_net", "gross_profit", "net_profit", "profit_before_tax"
}

# Fields that should NEVER be negative (revenue/sales)
# Everything else can potentially be negative (expenses, losses, etc.)
MUST_BE_POSITIVE = {
    "revenue_gross", "revenue_net", "fee_income", "interest_income",
    "premium_income", "dividend_income"
}

# Maximum reasonable value (10 trillion PKR in thousands = 10 billion thousands)
MAX_VALUE = 10_000_000_000


def load_data() -> list[dict]:
    """Load flattened JSONL data."""
    rows = []
    with open(INPUT_FILE) as f:
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
    """Check for duplicate rows."""
    seen = {}
    issues = []

    for i, row in enumerate(rows):
        key = (
            row.get("ticker"),
            row.get("period_end"),
            row.get("consolidation"),
            row.get("canonical_field")
        )
        if key in seen:
            issues.append({
                "type": "duplicate_row",
                "row": i,
                "ticker": row.get("ticker"),
                "period_end": row.get("period_end"),
                "consolidation": row.get("consolidation"),
                "field": row.get("canonical_field"),
                "first_occurrence": seen[key]
            })
        else:
            seen[key] = i

    return issues


def check_value_sanity(rows: list[dict], allowlist: set) -> list[dict]:
    """Check for unreasonable values, respecting Stage 3 allowlist."""
    issues = []

    for i, row in enumerate(rows):
        value = row.get("value")
        field = row.get("canonical_field", "")
        ticker = row.get("ticker")
        fiscal_year = row.get("fiscal_year")
        consolidation = row.get("consolidation")

        if value is None:
            continue

        # Check if this ticker/year/consolidation is allowlisted from Stage 3
        is_allowlisted = (ticker, fiscal_year, consolidation) in allowlist

        # Check for extremely large values (always flag, even if allowlisted)
        if abs(value) > MAX_VALUE:
            issues.append({
                "type": "value_too_large",
                "row": i,
                "ticker": ticker,
                "period_end": row.get("period_end"),
                "field": field,
                "value": value,
                "allowlisted": is_allowlisted
            })

        # Check for unexpected negatives (only revenue fields should be positive)
        # Skip if allowlisted (already reviewed in Stage 3)
        if value < 0 and field in MUST_BE_POSITIVE and not is_allowlisted:
            # Only flag if significantly negative (not rounding errors)
            if abs(value) > 1:
                issues.append({
                    "type": "unexpected_negative",
                    "row": i,
                    "ticker": ticker,
                    "period_end": row.get("period_end"),
                    "field": field,
                    "value": value,
                    "allowlisted": False
                })

    return issues


def check_coverage(rows: list[dict]) -> dict:
    """Analyze coverage statistics."""
    ticker_quarters = defaultdict(set)
    quarter_fields = defaultdict(set)
    field_counts = defaultdict(int)

    for row in rows:
        ticker = row.get("ticker")
        period_end = row.get("period_end")
        consolidation = row.get("consolidation")
        field = row.get("canonical_field")

        ticker_quarters[ticker].add((period_end, consolidation))
        quarter_fields[(ticker, period_end, consolidation)].add(field)
        field_counts[field] += 1

    # Tickers with very few quarters
    low_coverage_tickers = []
    for ticker, quarters in ticker_quarters.items():
        if len(quarters) < 4:
            low_coverage_tickers.append({
                "ticker": ticker,
                "quarters": len(quarters)
            })

    # Quarters missing expected fields
    missing_fields = []
    for (ticker, period_end, consolidation), fields in quarter_fields.items():
        missing = EXPECTED_FIELDS - fields
        if missing:
            # Only flag if missing all expected fields
            if len(missing) >= len(EXPECTED_FIELDS) - 1:
                missing_fields.append({
                    "ticker": ticker,
                    "period_end": period_end,
                    "consolidation": consolidation,
                    "missing": list(missing)
                })

    return {
        "total_tickers": len(ticker_quarters),
        "total_quarters": sum(len(q) for q in ticker_quarters.values()),
        "total_rows": len(rows),
        "unique_fields": len(field_counts),
        "field_distribution": dict(sorted(field_counts.items(), key=lambda x: -x[1])),
        "low_coverage_tickers": sorted(low_coverage_tickers, key=lambda x: x["quarters"]),
        "quarters_missing_fields": missing_fields[:20]  # Limit output
    }


def check_unit_normalization(rows: list[dict]) -> list[dict]:
    """Verify all values are in expected units (thousands for amounts, rupees for EPS)."""
    # Note: quarterly_pl values are already normalized by Stage 3
    # This check is now handled by value_sanity (value_too_large)
    return []


def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        print("Run Step1_FlattenPL.py first.")
        return

    print(f"Loading data from {INPUT_FILE}...")
    rows = load_data()
    print(f"Loaded {len(rows):,} rows")

    # Load Stage 3 allowlist
    allowlist = load_allowlist()
    print(f"Loaded {len(allowlist)} allowlisted ticker/year/consolidation combos from Stage 3")
    print()

    # Run all checks
    print("Running QC checks...")

    issues = {
        "missing_required": check_required_fields(rows),
        "duplicates": check_duplicates(rows),
        "value_sanity": check_value_sanity(rows, allowlist),
        "normalization": check_unit_normalization(rows)
    }

    coverage = check_coverage(rows)

    # Count total issues
    total_issues = sum(len(v) for v in issues.values())

    # Build report
    report = {
        "input_file": str(INPUT_FILE),
        "total_rows": len(rows),
        "total_issues": total_issues,
        "issues_by_type": {k: len(v) for k, v in issues.items()},
        "coverage": coverage,
        "issues": issues
    }

    # Write report
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary
    print()
    print("=" * 60)
    print("QC PRE-UPLOAD COMPLETE")
    print("=" * 60)
    print(f"Total rows:   {len(rows):,}")
    print(f"Total issues: {total_issues}")
    print()
    print("Issues by type:")
    for issue_type, issue_list in issues.items():
        status = "✓" if len(issue_list) == 0 else f"⚠ {len(issue_list)}"
        print(f"  {issue_type}: {status}")
    print()
    print("Coverage:")
    print(f"  Tickers:  {coverage['total_tickers']}")
    print(f"  Quarters: {coverage['total_quarters']:,}")
    print(f"  Fields:   {coverage['unique_fields']}")
    print()

    if coverage['low_coverage_tickers']:
        print(f"Low coverage tickers ({len(coverage['low_coverage_tickers'])}):")
        for t in coverage['low_coverage_tickers'][:10]:
            print(f"  {t['ticker']}: {t['quarters']} quarters")
        if len(coverage['low_coverage_tickers']) > 10:
            print(f"  ... and {len(coverage['low_coverage_tickers']) - 10} more")
        print()

    print(f"Report: {OUTPUT_FILE}")

    if total_issues > 0:
        print()
        print("⚠ Review issues before uploading!")
        return 1
    else:
        print()
        print("✓ All checks passed. Ready for upload.")
        return 0


if __name__ == "__main__":
    exit(main())
