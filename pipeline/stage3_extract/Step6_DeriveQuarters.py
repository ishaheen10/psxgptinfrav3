#!/usr/bin/env python3
"""
Step 6: Derive Quarterly (3M) Statements

Derives all possible 3M quarterly periods from available data using:
- Direct 3M periods (extracted)
- Q2 = 6M - Q1
- Q3 = 9M - 6M, or 9M - Q1 - Q2
- Q4 = 12M - 9M, or 12M - Q1 - Q2 - Q3

QC checks:
- Derived revenue should not be negative
- Derived values should be reasonable relative to annual

Input:  data/json_pl/*.json
Output: data/quarterly_pl/*.json
        artifacts/stage3/step6_qc_issues.json

Usage:
    python3 Step6_DeriveQuarters.py
    python3 Step6_DeriveQuarters.py --ticker ENGRO
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "json_pl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "quarterly_pl"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
QC_OUTPUT = PROJECT_ROOT / "artifacts" / "stage3" / "step6_qc_issues.json"
ARITHMETIC_ALLOWLIST = PROJECT_ROOT / "artifacts" / "stage3" / "step6_arithmetic_allowlist.json"


def load_arithmetic_allowlist() -> set:
    """Load allowlist of ticker/FY/consolidation combinations to skip for arithmetic checks."""
    if not ARITHMETIC_ALLOWLIST.exists():
        return set()
    with open(ARITHMETIC_ALLOWLIST) as f:
        data = json.load(f)
    # Return as set of (ticker, fiscal_year, consolidation) tuples
    return {
        (item['ticker'], item['fiscal_year'], item['consolidation'])
        for item in data.get('allowlist', [])
    }


def load_fiscal_periods() -> dict:
    """Load fiscal period (month) for each ticker."""
    if not TICKERS_FILE.exists():
        return {}
    with open(TICKERS_FILE) as f:
        tickers = json.load(f)
    return {t['Symbol']: int(t.get('fiscal_period', '06-30').split('-')[0]) for t in tickers}


def load_industries() -> dict:
    """Load industry for each ticker."""
    if not TICKERS_FILE.exists():
        return {}
    with open(TICKERS_FILE) as f:
        tickers = json.load(f)
    return {t['Symbol']: t.get('Industry', '') for t in tickers}


# Industry to income field mapping
INDUSTRY_INCOME_FIELDS = {
    'Banking': 'net_interest_income',
    'Insurance': 'net_premium',
}


def get_income_field(industry: str) -> str:
    """Get the appropriate income field for an industry."""
    return INDUSTRY_INCOME_FIELDS.get(industry, 'revenue_net')


def get_quarter_end_date(fy_month: int, fy_year: int, quarter: int) -> str:
    """Calculate the end date for a given quarter in a fiscal year."""
    # FY starts the month after fy_month
    start_month = (fy_month % 12) + 1
    # Quarter end month
    q_end_month = ((start_month - 1 + quarter * 3) % 12) or 12
    # Year depends on whether we've crossed into the FY year
    if q_end_month > fy_month:
        year = fy_year - 1
    else:
        year = fy_year
    # Last day of month
    if q_end_month in [1, 3, 5, 7, 8, 10, 12]:
        day = 31
    elif q_end_month in [4, 6, 9, 11]:
        day = 30
    else:
        day = 28
    return f'{year}-{q_end_month:02d}-{day:02d}'


def find_period(periods: list, end_date: str, duration: str) -> dict | None:
    """Find a period with given end date and duration."""
    return next((p for p in periods if p['period_end'] == end_date and p['duration'] == duration), None)


def derive_quarter_values(base_values: dict, subtract_values: list[dict]) -> dict:
    """Derive quarter values by subtracting multiple periods from base."""
    result = {}
    for key, base_val in base_values.items():
        if base_val is None:
            result[key] = None
            continue

        derived = base_val
        for sub_vals in subtract_values:
            sub_val = sub_vals.get(key)
            if sub_val is not None:
                derived -= sub_val
            # If any subtraction value is None, we can't derive
            elif key in sub_vals:
                derived = None
                break
        result[key] = derived
    return result


def qc_derived_values(values: dict, method: str, industry: str = '') -> list[str]:
    """QC check derived values using industry-appropriate income field."""
    issues = []

    # Get industry-appropriate income field
    income_field = get_income_field(industry)
    income = values.get(income_field)

    # Check income is not negative
    if income is not None and income < 0:
        issues.append(f"Negative {income_field}: {income:,.0f}")

    return issues


def qc_arithmetic_check(quarters: list[dict], annual: dict, industry: str = '', tolerance: float = 0.05) -> list[str]:
    """
    Check that Q1 + Q2 + Q3 + Q4 = Annual for key fields (within tolerance).

    Only checks: revenue (industry-specific), gross_profit, net_profit
    These are single-line items unlikely to have field collision issues.

    Args:
        quarters: List of 4 quarter dicts with 'quarter' and 'values' keys
        annual: Annual period dict with 'values' key
        industry: Industry name for selecting appropriate revenue field
        tolerance: Allowed percentage difference (default 5% - accounts for
                   legitimate restatements between quarterly and annual filings)

    Returns:
        List of issue strings for fields that don't match
    """
    issues = []

    # Need exactly 4 quarters
    if len(quarters) != 4:
        return issues

    # Sort quarters by Q1, Q2, Q3, Q4
    q_map = {q['quarter']: q['values'] for q in quarters}
    if not all(f'Q{i}' in q_map for i in range(1, 5)):
        return issues

    annual_values = annual['values']

    # Only check these key fields (single-line items, no collision risk)
    income_field = get_income_field(industry)
    check_fields = [income_field, 'gross_profit', 'net_profit']

    for field in check_fields:
        annual_val = annual_values.get(field)
        if annual_val is None:
            continue

        # Get quarter values for this field
        q_vals = []
        all_present = True
        for i in range(1, 5):
            q_val = q_map[f'Q{i}'].get(field)
            if q_val is None:
                all_present = False
                break
            q_vals.append(q_val)

        if not all_present:
            continue

        # Sum quarters and compare to annual
        q_sum = sum(q_vals)

        # Handle zero annual value
        if annual_val == 0:
            if q_sum != 0:
                pct_diff = float('inf')
            else:
                continue  # Both zero, no issue
        else:
            pct_diff = abs(q_sum - annual_val) / abs(annual_val)

        if pct_diff > tolerance:
            issues.append(
                f"{field}: Q1+Q2+Q3+Q4={q_sum:,.0f} vs Annual={annual_val:,.0f} (diff={pct_diff*100:.1f}%)"
            )

    return issues


def process_ticker(ticker: str, data: dict, fy_month: int, industry: str = '') -> tuple[list, list]:
    """
    Process a ticker and derive all possible quarters.
    Returns (derived_quarters, qc_issues)
    """
    derived_quarters = []
    qc_issues = []

    for cons_type in ['consolidated', 'unconsolidated']:
        cons_periods = [p for p in data['periods'] if p.get('consolidation') == cons_type]
        if not cons_periods:
            continue

        # Find all annual periods
        annuals = [p for p in cons_periods if p['duration'] == '12M']

        for annual in annuals:
            fy_end = annual['period_end']
            fy_year = int(fy_end[:4])
            fy_end_month = int(fy_end[5:7])

            # Skip if FY end doesn't match expected
            if fy_end_month != fy_month:
                continue

            # Calculate quarter end dates
            q1_end = get_quarter_end_date(fy_month, fy_year, 1)
            q2_end = get_quarter_end_date(fy_month, fy_year, 2)
            q3_end = get_quarter_end_date(fy_month, fy_year, 3)
            q4_end = fy_end

            # Find available periods
            p_3m_q1 = find_period(cons_periods, q1_end, '3M')
            p_3m_q2 = find_period(cons_periods, q2_end, '3M')
            p_3m_q3 = find_period(cons_periods, q3_end, '3M')
            p_3m_q4 = find_period(cons_periods, q4_end, '3M')
            p_6m = find_period(cons_periods, q2_end, '6M')
            p_9m = find_period(cons_periods, q3_end, '9M')
            p_12m = annual

            fy_quarters = []

            # === Q1 ===
            q1_result = None
            if p_3m_q1:
                q1_result = {
                    'quarter': 'Q1',
                    'period_end': q1_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': 'direct_3M',
                    'source': p_3m_q1.get('source_filing'),
                    'values': p_3m_q1['values'].copy(),
                }
                fy_quarters.append(q1_result)

            # === Q2 ===
            q2_result = None
            if p_3m_q2:
                q2_result = {
                    'quarter': 'Q2',
                    'period_end': q2_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': 'direct_3M',
                    'source': p_3m_q2.get('source_filing'),
                    'values': p_3m_q2['values'].copy(),
                }
                fy_quarters.append(q2_result)
            elif p_6m and q1_result:
                # Q2 = 6M - Q1
                derived_values = derive_quarter_values(p_6m['values'], [q1_result['values']])
                issues = qc_derived_values(derived_values, '6M-Q1', industry)
                q2_result = {
                    'quarter': 'Q2',
                    'period_end': q2_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '6M-Q1',
                    'source': f"derived from {p_6m.get('source_filing')}",
                    'values': derived_values,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q2',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '6M-Q1',
                        'issues': issues,
                        'values': derived_values,
                    })
                else:
                    fy_quarters.append(q2_result)

            # === Q3 ===
            q3_result = None
            if p_3m_q3:
                q3_result = {
                    'quarter': 'Q3',
                    'period_end': q3_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': 'direct_3M',
                    'source': p_3m_q3.get('source_filing'),
                    'values': p_3m_q3['values'].copy(),
                }
                fy_quarters.append(q3_result)
            elif p_9m and p_6m:
                # Q3 = 9M - 6M
                derived_values = derive_quarter_values(p_9m['values'], [p_6m['values']])
                issues = qc_derived_values(derived_values, '9M-6M', industry)
                q3_result = {
                    'quarter': 'Q3',
                    'period_end': q3_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '9M-6M',
                    'source': f"derived from {p_9m.get('source_filing')}",
                    'values': derived_values,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q3',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '9M-6M',
                        'issues': issues,
                        'values': derived_values,
                    })
                else:
                    fy_quarters.append(q3_result)
            elif p_9m and q1_result and q2_result:
                # Q3 = 9M - Q1 - Q2
                derived_values = derive_quarter_values(p_9m['values'], [q1_result['values'], q2_result['values']])
                issues = qc_derived_values(derived_values, '9M-Q1-Q2', industry)
                q3_result = {
                    'quarter': 'Q3',
                    'period_end': q3_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '9M-Q1-Q2',
                    'source': f"derived from {p_9m.get('source_filing')}",
                    'values': derived_values,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q3',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '9M-Q1-Q2',
                        'issues': issues,
                        'values': derived_values,
                    })
                else:
                    fy_quarters.append(q3_result)

            # === Q4 ===
            q4_result = None
            if p_3m_q4:
                q4_result = {
                    'quarter': 'Q4',
                    'period_end': q4_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': 'direct_3M',
                    'source': p_3m_q4.get('source_filing'),
                    'values': p_3m_q4['values'].copy(),
                }
                fy_quarters.append(q4_result)
            elif p_9m:
                # Q4 = 12M - 9M
                derived_values = derive_quarter_values(p_12m['values'], [p_9m['values']])
                issues = qc_derived_values(derived_values, '12M-9M', industry)
                q4_result = {
                    'quarter': 'Q4',
                    'period_end': q4_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '12M-9M',
                    'source': f"derived from {p_12m.get('source_filing')}",
                    'values': derived_values,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q4',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '12M-9M',
                        'issues': issues,
                        'values': derived_values,
                    })
                else:
                    fy_quarters.append(q4_result)
            elif q1_result and q2_result and q3_result:
                # Q4 = 12M - Q1 - Q2 - Q3
                derived_values = derive_quarter_values(
                    p_12m['values'],
                    [q1_result['values'], q2_result['values'], q3_result['values']]
                )
                issues = qc_derived_values(derived_values, '12M-Q1-Q2-Q3', industry)
                q4_result = {
                    'quarter': 'Q4',
                    'period_end': q4_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '12M-Q1-Q2-Q3',
                    'source': f"derived from {p_12m.get('source_filing')}",
                    'values': derived_values,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q4',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '12M-Q1-Q2-Q3',
                        'issues': issues,
                        'values': derived_values,
                    })
                else:
                    fy_quarters.append(q4_result)

            # Run arithmetic check if we have all 4 quarters
            arith_issues = qc_arithmetic_check(fy_quarters, annual, industry)
            if arith_issues:
                qc_issues.append({
                    'ticker': ticker,
                    'quarter': 'FY',
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': 'arithmetic_check',
                    'issues': arith_issues,
                    'values': {},
                })

            derived_quarters.extend(fy_quarters)

    return derived_quarters, qc_issues


def main():
    parser = argparse.ArgumentParser(description="Derive quarterly (3M) statements")
    parser.add_argument("--ticker", help="Process only this ticker")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 6: DERIVE QUARTERLY (3M) STATEMENTS")
    print("=" * 70)

    # Load fiscal periods, industries, and arithmetic allowlist
    fiscal_periods = load_fiscal_periods()
    industries = load_industries()
    arithmetic_allowlist = load_arithmetic_allowlist()
    print(f"\nLoaded fiscal periods for {len(fiscal_periods)} tickers")
    print(f"Loaded industries for {len(industries)} tickers")
    print(f"Loaded {len(arithmetic_allowlist)} arithmetic check exceptions")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process tickers
    json_files = sorted(INPUT_DIR.glob("*.json"))
    if args.ticker:
        json_files = [f for f in json_files if f.stem == args.ticker]

    all_qc_issues = []
    stats = {
        'tickers': 0,
        'total_quarters': 0,
        'direct_3m': 0,
        'derived': 0,
        'qc_issues': 0,
        'by_method': defaultdict(int),
    }

    print(f"\nProcessing {len(json_files)} tickers...\n")

    for jf in json_files:
        ticker = jf.stem
        fy_month = fiscal_periods.get(ticker, 6)
        industry = industries.get(ticker, '')

        with open(jf) as f:
            data = json.load(f)

        quarters, issues = process_ticker(ticker, data, fy_month, industry)

        stats['tickers'] += 1
        stats['total_quarters'] += len(quarters)
        stats['qc_issues'] += len(issues)

        for q in quarters:
            method = q['method']
            stats['by_method'][method] += 1
            if method == 'direct_3M':
                stats['direct_3m'] += 1
            else:
                stats['derived'] += 1

        all_qc_issues.extend(issues)

        # Write output
        output = {
            'ticker': ticker,
            'fiscal_year_end_month': fy_month,
            'quarters': quarters,
        }

        output_file = OUTPUT_DIR / f"{ticker}.json"
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)

        issue_str = f" ({len(issues)} QC issues)" if issues else ""
        print(f"  {ticker}: {len(quarters)} quarters{issue_str}")

    # Filter out allowlisted issues (both arithmetic checks and negative derivations)
    filtered_issues = []
    skipped_count = 0
    for issue in all_qc_issues:
        key = (issue['ticker'], issue['fiscal_year'], issue['consolidation'])
        if key in arithmetic_allowlist:
            skipped_count += 1
            continue
        filtered_issues.append(issue)

    if skipped_count > 0:
        print(f"\n  Skipped {skipped_count} allowlisted issues")

    # Write QC issues
    QC_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(QC_OUTPUT, 'w') as f:
        json.dump({
            'total_issues': len(filtered_issues),
            'issues': filtered_issues,
        }, f, indent=2)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Tickers processed:    {stats['tickers']}")
    print(f"  Total quarters:       {stats['total_quarters']}")
    print(f"  Direct 3M:            {stats['direct_3m']}")
    print(f"  Derived:              {stats['derived']}")
    print(f"  QC issues (raw):      {stats['qc_issues']}")
    print(f"  QC issues (filtered): {len(filtered_issues)}")
    print()
    print("By derivation method:")
    for method, count in sorted(stats['by_method'].items(), key=lambda x: -x[1]):
        print(f"    {method}: {count}")
    print()
    print(f"Output: {OUTPUT_DIR}/")
    print(f"QC issues: {QC_OUTPUT}")


if __name__ == "__main__":
    main()
