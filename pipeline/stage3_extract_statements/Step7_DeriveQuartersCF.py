#!/usr/bin/env python3
"""
Step 7: Derive Quarterly (3M) Cash Flow Statements

Derives all possible 3M quarterly periods from available data using:
- Direct 3M periods (extracted)
- Q2 = 6M - Q1, or 9M - Q1 - Q3
- Q3 = 9M - 6M, or 9M - Q1 - Q2
- Q4 = 12M - 9M, or 12M - Q1 - Q2 - Q3, or 12M - 6M - Q3

QC checks:
- Derived net_cash_change should roughly match cfo + cfi + cff
- Flag large derivation anomalies

Input:  data/json_cf/*.json
Output: data/quarterly_cf/*.json
        artifacts/stage3/step7_cf_qc_issues.json

Usage:
    python3 Step7_DeriveQuartersCF.py
    python3 Step7_DeriveQuartersCF.py --ticker ENGRO
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "json_cf"
OUTPUT_DIR = PROJECT_ROOT / "data" / "quarterly_cf"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
QC_OUTPUT = PROJECT_ROOT / "artifacts" / "stage3" / "step7_cf_qc_issues.json"
ARITHMETIC_ALLOWLIST = PROJECT_ROOT / "artifacts" / "stage3" / "step7_arithmetic_allowlist_cf.json"


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


def normalize_period_values(period: dict) -> dict:
    """
    Normalize all values in a period to thousands.
    Modifies the period dict in place and returns it.
    """
    unit_type = period.get('unit_type', 'thousands')
    values = period.get('values', {})

    for canonical, val in values.items():
        if val is not None:
            values[canonical] = normalize_value(val, unit_type)

    # Update unit_type to reflect normalization
    period['unit_type'] = 'thousands'
    return period


def load_fiscal_periods() -> dict:
    """Load fiscal period (month) for each ticker."""
    if not TICKERS_FILE.exists():
        return {}
    with open(TICKERS_FILE) as f:
        tickers = json.load(f)
    return {t['Symbol']: int(t.get('fiscal_period', '06-30').split('-')[0]) for t in tickers}


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


def get_quarter_end_date(fy_month: int, fy_year: int, quarter: int) -> str:
    """Calculate the end date for a given quarter in a fiscal year."""
    start_month = (fy_month % 12) + 1
    q_end_month = ((start_month - 1 + quarter * 3) % 12) or 12
    if q_end_month > fy_month:
        year = fy_year - 1
    else:
        year = fy_year
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
            elif key in sub_vals:
                derived = None
                break
        result[key] = derived
    return result


def qc_derived_values(values: dict, method: str) -> list[str]:
    """QC check derived CF values."""
    issues = []

    # For CF, we check that net_cash_change roughly equals cfo + cfi + cff
    cfo = values.get('cfo')
    cfi = values.get('cfi')
    cff = values.get('cff')
    net_change = values.get('net_cash_change')

    if all(v is not None for v in [cfo, cfi, cff, net_change]):
        expected = cfo + cfi + cff
        if abs(net_change) > 0:
            diff_pct = abs(expected - net_change) / abs(net_change)
            # Allow 20% tolerance for derived values (some rounding/fx adjustments)
            if diff_pct > 0.20 and abs(expected - net_change) > 1000:  # 1M threshold
                issues.append(f"CFO+CFI+CFF={expected:,.0f} vs net_cash_change={net_change:,.0f} (diff={diff_pct*100:.1f}%)")

    return issues


def qc_arithmetic_check(quarters: list[dict], annual: dict, tolerance: float = 0.10) -> list[str]:
    """
    Check that Q1 + Q2 + Q3 + Q4 = Annual for key CF fields.
    Uses 10% tolerance (higher than P&L due to fx adjustments).
    """
    issues = []

    if len(quarters) != 4:
        return issues

    q_map = {q['quarter']: q['values'] for q in quarters}
    if not all(f'Q{i}' in q_map for i in range(1, 5)):
        return issues

    annual_values = annual['values']

    # Check these CF fields
    check_fields = ['cfo', 'cfi', 'cff', 'net_cash_change']

    for field in check_fields:
        annual_val = annual_values.get(field)
        if annual_val is None:
            continue

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

        q_sum = sum(q_vals)

        if annual_val == 0:
            if q_sum != 0 and abs(q_sum) > 1000:
                pct_diff = float('inf')
            else:
                continue
        else:
            pct_diff = abs(q_sum - annual_val) / abs(annual_val)

        if pct_diff > tolerance:
            issues.append(
                f"{field}: Q1+Q2+Q3+Q4={q_sum:,.0f} vs Annual={annual_val:,.0f} (diff={pct_diff*100:.1f}%)"
            )

    return issues


def process_ticker(ticker: str, data: dict, fy_month: int) -> tuple[list, list]:
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

        # Get source_labels from any period (same for all periods of this ticker/consolidation)
        source_labels = {}
        for p in cons_periods:
            if p.get('source_labels'):
                source_labels = p['source_labels']
                break

        # Find all annual periods
        annuals = [p for p in cons_periods if p['duration'] == '12M']

        for annual in annuals:
            fy_end = annual['period_end']
            fy_year = int(fy_end[:4])
            fy_end_month = int(fy_end[5:7])

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
                    'source_labels': source_labels,
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
                    'source_labels': source_labels,
                }
                fy_quarters.append(q2_result)
            elif p_6m and q1_result:
                derived_values = derive_quarter_values(p_6m['values'], [q1_result['values']])
                issues = qc_derived_values(derived_values, '6M-Q1')
                q2_result = {
                    'quarter': 'Q2',
                    'period_end': q2_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '6M-Q1',
                    'source': f"derived from {p_6m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q2',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '6M-Q1',
                        'issues': issues,
                    })
                fy_quarters.append(q2_result)
            elif p_9m and q1_result and p_3m_q3:
                # Q2 = 9M - Q1 - Q3 (when we have direct Q3 but no 6M)
                derived_values = derive_quarter_values(p_9m['values'], [q1_result['values'], p_3m_q3['values']])
                issues = qc_derived_values(derived_values, '9M-Q1-Q3')
                q2_result = {
                    'quarter': 'Q2',
                    'period_end': q2_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '9M-Q1-Q3',
                    'source': f"derived from {p_9m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q2',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '9M-Q1-Q3',
                        'issues': issues,
                    })
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
                    'source_labels': source_labels,
                }
                fy_quarters.append(q3_result)
            elif p_9m and p_6m:
                derived_values = derive_quarter_values(p_9m['values'], [p_6m['values']])
                issues = qc_derived_values(derived_values, '9M-6M')
                q3_result = {
                    'quarter': 'Q3',
                    'period_end': q3_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '9M-6M',
                    'source': f"derived from {p_9m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q3',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '9M-6M',
                        'issues': issues,
                    })
                fy_quarters.append(q3_result)
            elif p_9m and q1_result and q2_result:
                derived_values = derive_quarter_values(p_9m['values'], [q1_result['values'], q2_result['values']])
                issues = qc_derived_values(derived_values, '9M-Q1-Q2')
                q3_result = {
                    'quarter': 'Q3',
                    'period_end': q3_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '9M-Q1-Q2',
                    'source': f"derived from {p_9m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q3',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '9M-Q1-Q2',
                        'issues': issues,
                    })
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
                    'source_labels': source_labels,
                }
                fy_quarters.append(q4_result)
            elif p_9m:
                derived_values = derive_quarter_values(p_12m['values'], [p_9m['values']])
                issues = qc_derived_values(derived_values, '12M-9M')
                q4_result = {
                    'quarter': 'Q4',
                    'period_end': q4_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '12M-9M',
                    'source': f"derived from {p_12m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q4',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '12M-9M',
                        'issues': issues,
                    })
                fy_quarters.append(q4_result)
            elif q1_result and q2_result and q3_result:
                derived_values = derive_quarter_values(
                    p_12m['values'],
                    [q1_result['values'], q2_result['values'], q3_result['values']]
                )
                issues = qc_derived_values(derived_values, '12M-Q1-Q2-Q3')
                q4_result = {
                    'quarter': 'Q4',
                    'period_end': q4_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '12M-Q1-Q2-Q3',
                    'source': f"derived from {p_12m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q4',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '12M-Q1-Q2-Q3',
                        'issues': issues,
                    })
                fy_quarters.append(q4_result)
            elif p_6m and q3_result:
                # Q4 = 12M - 6M - Q3 (when we have 6M and Q3 but no 9M)
                derived_values = derive_quarter_values(
                    p_12m['values'],
                    [p_6m['values'], q3_result['values']]
                )
                issues = qc_derived_values(derived_values, '12M-6M-Q3')
                q4_result = {
                    'quarter': 'Q4',
                    'period_end': q4_end,
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': '12M-6M-Q3',
                    'source': f"derived from {p_12m.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q4',
                        'fiscal_year': fy_year,
                        'consolidation': cons_type,
                        'method': '12M-6M-Q3',
                        'issues': issues,
                    })
                fy_quarters.append(q4_result)

            # Run arithmetic check if we have all 4 quarters
            arith_issues = qc_arithmetic_check(fy_quarters, annual)
            if arith_issues:
                qc_issues.append({
                    'ticker': ticker,
                    'quarter': 'FY',
                    'fiscal_year': fy_year,
                    'consolidation': cons_type,
                    'method': 'arithmetic_check',
                    'issues': arith_issues,
                })

            derived_quarters.extend(fy_quarters)

        # === Handle orphan 3M periods (not covered by any fiscal year) ===
        # These are typically Q1/Q2/Q3 of the current fiscal year where no annual exists yet
        covered_dates = {q['period_end'] for q in derived_quarters if q['consolidation'] == cons_type}

        orphan_3m = [p for p in cons_periods
                     if p['duration'] == '3M' and p['period_end'] not in covered_dates]

        for orphan in orphan_3m:
            period_end = orphan['period_end']
            period_month = int(period_end[5:7])
            period_year = int(period_end[:4])

            # Determine fiscal year and quarter based on fiscal month
            # For June FYE: Jul-Sep=Q1, Oct-Dec=Q2, Jan-Mar=Q3, Apr-Jun=Q4
            months_after_fy = (period_month - fy_month) % 12
            if months_after_fy == 0:
                months_after_fy = 12
            quarter_num = (months_after_fy + 2) // 3

            # Fiscal year is the calendar year of the FY end
            if period_month > fy_month:
                fiscal_year = period_year + 1
            else:
                fiscal_year = period_year

            orphan_result = {
                'quarter': f'Q{quarter_num}',
                'period_end': period_end,
                'fiscal_year': fiscal_year,
                'consolidation': cons_type,
                'method': 'direct_3M',
                'source': orphan.get('source_filing'),
                'values': orphan['values'].copy(),
                'source_labels': source_labels,
            }
            derived_quarters.append(orphan_result)

        # === Handle orphan 9M periods (derive Q3 when no 12M annual exists yet) ===
        # Update covered dates after adding orphan 3M
        covered_dates = {q['period_end'] for q in derived_quarters if q['consolidation'] == cons_type}

        orphan_9m = [p for p in cons_periods
                     if p['duration'] == '9M' and p['period_end'] not in covered_dates]

        for orphan in orphan_9m:
            period_end = orphan['period_end']  # This is Q3 end date (same as 9M end)
            period_month = int(period_end[5:7])
            period_year = int(period_end[:4])

            # Determine fiscal year and quarter based on fiscal month
            months_after_fy = (period_month - fy_month) % 12
            if months_after_fy == 0:
                months_after_fy = 12
            quarter_num = (months_after_fy + 2) // 3

            # This should be Q3 (9M period)
            if quarter_num != 3:
                continue

            # Fiscal year is the calendar year of the FY end
            if period_month > fy_month:
                fiscal_year = period_year + 1
            else:
                fiscal_year = period_year

            # Calculate Q1 and Q2 end dates for this fiscal year
            q1_end = get_quarter_end_date(fy_month, fiscal_year, 1)
            q2_end = get_quarter_end_date(fy_month, fiscal_year, 2)

            # Try to find 6M period (ends at Q2 date)
            p_6m = find_period(cons_periods, q2_end, '6M')

            # Try to find Q1 and Q2 3M periods
            p_3m_q1 = find_period(cons_periods, q1_end, '3M')
            p_3m_q2 = find_period(cons_periods, q2_end, '3M')

            derived_values = None
            method = None

            if p_6m:
                # Q3 = 9M - 6M
                derived_values = derive_quarter_values(orphan['values'], [p_6m['values']])
                method = '9M-6M'
            elif p_3m_q1 and p_3m_q2:
                # Q3 = 9M - Q1 - Q2
                derived_values = derive_quarter_values(orphan['values'], [p_3m_q1['values'], p_3m_q2['values']])
                method = '9M-Q1-Q2'

            if derived_values and method:
                issues = qc_derived_values(derived_values, method)
                orphan_q3_result = {
                    'quarter': 'Q3',
                    'period_end': period_end,
                    'fiscal_year': fiscal_year,
                    'consolidation': cons_type,
                    'method': method,
                    'source': f"derived from {orphan.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q3',
                        'fiscal_year': fiscal_year,
                        'consolidation': cons_type,
                        'method': method,
                        'issues': issues,
                        'values': derived_values,
                    })
                derived_quarters.append(orphan_q3_result)

        # === Handle orphan 6M periods (derive Q2 when no 12M annual exists yet) ===
        # Update covered dates after adding orphan 3M and orphan 9M-derived Q3
        covered_dates = {q['period_end'] for q in derived_quarters if q['consolidation'] == cons_type}

        orphan_6m = [p for p in cons_periods
                     if p['duration'] == '6M' and p['period_end'] not in covered_dates]

        for orphan in orphan_6m:
            period_end = orphan['period_end']  # This is Q2 end date (same as 6M end)
            period_month = int(period_end[5:7])
            period_year = int(period_end[:4])

            # Determine fiscal year and quarter based on fiscal month
            months_after_fy = (period_month - fy_month) % 12
            if months_after_fy == 0:
                months_after_fy = 12
            quarter_num = (months_after_fy + 2) // 3

            # This should be Q2 (6M period)
            if quarter_num != 2:
                continue

            # Fiscal year is the calendar year of the FY end
            if period_month > fy_month:
                fiscal_year = period_year + 1
            else:
                fiscal_year = period_year

            # Calculate Q1 end date for this fiscal year
            q1_end = get_quarter_end_date(fy_month, fiscal_year, 1)

            # Try to find Q1 3M period
            p_3m_q1 = find_period(cons_periods, q1_end, '3M')

            if p_3m_q1:
                # Q2 = 6M - Q1
                derived_values = derive_quarter_values(orphan['values'], [p_3m_q1['values']])
                issues = qc_derived_values(derived_values, '6M-Q1')
                orphan_q2_result = {
                    'quarter': 'Q2',
                    'period_end': period_end,
                    'fiscal_year': fiscal_year,
                    'consolidation': cons_type,
                    'method': '6M-Q1',
                    'source': f"derived from {orphan.get('source_filing')}",
                    'values': derived_values,
                    'source_labels': source_labels,
                }
                if issues:
                    qc_issues.append({
                        'ticker': ticker,
                        'quarter': 'Q2',
                        'fiscal_year': fiscal_year,
                        'consolidation': cons_type,
                        'method': '6M-Q1',
                        'issues': issues,
                        'values': derived_values,
                    })
                derived_quarters.append(orphan_q2_result)

    return derived_quarters, qc_issues


def main():
    parser = argparse.ArgumentParser(description="Derive quarterly (3M) cash flow statements")
    parser.add_argument("--ticker", help="Process only this ticker")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 6: DERIVE QUARTERLY (3M) CASH FLOW STATEMENTS")
    print("=" * 70)

    # Load fiscal periods
    fiscal_periods = load_fiscal_periods()
    arithmetic_allowlist = load_arithmetic_allowlist()
    print(f"\nLoaded fiscal periods for {len(fiscal_periods)} tickers")
    print(f"Loaded {len(arithmetic_allowlist)} arithmetic check exceptions")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process tickers
    json_files = sorted(INPUT_DIR.glob("*.json"))
    if args.ticker:
        json_files = [f for f in json_files if f.stem == args.ticker]

    if not json_files:
        print(f"\nNo JSON files found in {INPUT_DIR}")
        print("Run Step5_JSONifyCF.py first.")
        return

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

        with open(jf) as f:
            data = json.load(f)

        # Normalize all period values to thousands before processing
        for period in data.get('periods', []):
            normalize_period_values(period)

        quarters, issues = process_ticker(ticker, data, fy_month)

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

    # Filter out allowlisted issues
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
            'total_raw_issues': len(all_qc_issues),
            'skipped_allowlisted': skipped_count,
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
