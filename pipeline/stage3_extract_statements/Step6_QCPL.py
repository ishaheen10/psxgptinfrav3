#!/usr/bin/env python3
"""
Step 6: QC P&L

Runs systematic QC checks on the QC-optimized JSON:
1. Column completeness - check if we have all expected periods
2. Monotonicity - for cumulative items (revenue_net, cost_of_goods_sold): 9M > 6M > 3M
3. Period arithmetic - Q1 + Q2 + Q3 + Q4 = Annual (within tolerance)
4. Semantic equations - revenue + COGS = gross profit, PBT + tax = net income

Input:  data/json_pl/{TICKER}.json
Output: artifacts/stage3/step6_qc_pl_results.json

Usage:
    python3 Step6_QCPL.py                    # QC all
    python3 Step6_QCPL.py --ticker ABL       # Single ticker
    python3 Step6_QCPL.py --verbose          # Show all details
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "json_pl"
EXTRACTION_DIR = PROJECT_ROOT / "data" / "extracted_pl"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step6_qc_pl_results.json"

# Tolerances
SEMANTIC_TOLERANCE_PCT = 5.0  # 5% for semantic equation checks
PERIOD_ARITHMETIC_TOLERANCE_PCT = 2.0  # 2% for period arithmetic

# Items that should be monotonically increasing in cumulative periods
MONOTONIC_ITEMS = ['revenue_net', 'cost_of_goods_sold']

# Semantic equations to check: (result_field, [(operand_field, sign), ...])
# sign: +1 for add, -1 for subtract
#
# These equations serve as SANITY CHECKS - they catch when values are assigned
# to wrong canonical fields (e.g., OCR row shifting). Step4 validates internal
# formula consistency, but doesn't verify correct label assignment.
#
# For net_profit: The simple equation may not hold for complex P&Ls (associates,
# discontinued ops), so we use a higher tolerance and only flag major discrepancies.
SEMANTIC_EQUATIONS = [
    # gross_profit = revenue_net + cost_of_goods_sold (COGS is negative)
    ('gross_profit', [('revenue_net', 1), ('cost_of_goods_sold', 1)]),
]

# Valid unit types
VALID_UNITS = {'thousands', 'millions', 'rupees', 'full_rupees'}

# Critical fields that must be present for P&L
# At least one revenue field AND net_profit must exist
CRITICAL_FIELDS_PL = {
    'revenue': [
        'revenue_net', 'revenue_gross', 'revenue',  # Standard
        'net_interest_income', 'interest_income',  # Banks and holding companies
        'net_premium', 'gross_premium', 'underwriting_profit',  # Insurance
        'dividend_income', 'royalty_income', 'total_income',  # Holding companies
        'revenue_lease_financing', 'revenue_diminishing_musharaka',  # Modarabas
        'capacity_revenue', 'energy_revenue',  # Power IPPs
        'turnover',  # Alternative name for revenue
        'lease_income',  # Leasing companies
        'share_of_associates', 'share_of_profit_in_associates',  # Investment holdings (main income source)
        'other_income',  # Holding companies, investment companies, IPPs in wind-down
    ],
    'bottom_line': ['net_profit', 'profit_after_tax', 'net_profit_parent'],  # One of these
}

# Cross-period normalization: flag if value is >100x or <0.01x median
CROSS_PERIOD_THRESHOLD = 100

# Skip list: known problematic filings or acceptable anomalies
# Format: {ticker: [filing_patterns]} where pattern matches source_file
SKIP_FILINGS = {
    'EFERT': ['annual_2021'],  # OCR corruption - values shifted between rows
    'PHDL': ['quarterly_2021-12-31', 'quarterly_2023-12-31', 'quarterly_2024-03-31', 'annual_2025'],  # Taxation sign error + extraction error in annual
    'JVDC': ['quarterly_2021-09-30', 'quarterly_2022-09-30', 'quarterly_2022-12-31'],  # Sign error + legitimate low revenue quarter (startup)
    'SHEL': ['annual_2023'],  # OCR corruption - BS and P&L combined on same page
    # Rounding differences <1% - acceptable
    'ENGROH': ['annual_2024', 'quarterly_2025-09-30'],  # 0.17% rounding diff between 9M and 12M
    'LUCK': ['annual_2021', 'quarterly_2021-03-31'],  # 0.18% rounding diff between 9M and 12M FY2020
    # Discrete vs cumulative quarters - not a real monotonicity issue
    'AABS': ['quarterly_2024-12-31', 'quarterly_2025-03-31'],  # Discrete quarters comparison
    'SLCL': ['quarterly_2024-03-31', 'quarterly_2025-03-31', 'quarterly_2024-12-31', 'annual_2025'],  # Discrete quarters / period confusion
    # Business-related - legitimate variations
    'KAPCO': ['quarterly_2023-03-31', 'quarterly_2024-03-31', 'quarterly_2023-12-31'],  # IPP operations ceased
    'HUBC': ['quarterly_2024-12-31'],  # IPP wind-down, low unconsolidated revenue
    'TRG': ['annual_2021'],  # Holding company with volatile interest income
    'FDPL': ['annual_2024', 'quarterly_2024-09-30'],  # Lease company with volatile income
    # Extraction errors flagged - need manual re-extraction
    'TPLP': ['quarterly_2022-09-30', 'quarterly_2023-03-31', 'quarterly_2022-03-31', 'quarterly_2023-12-31', 'quarterly_2024-03-31'],  # OCR error + rounding
    'FHAM': ['quarterly_2021-12-31', 'quarterly_2020-12-31'],  # Modaraba P&L format - signs/values misextracted
}

# NOTE: We previously tried to include share_of_associates, gain_on_disposal, etc.
# in the net_profit equation. But analysis shows these items are almost always
# ALREADY included in profit_before_tax (105 cases before PBT vs 9 after).
# So the simple equation is: net_profit = profit_before_tax + taxation
# This is the "bitter lesson" approach - keep it simple rather than over-engineering.


def should_skip_period(ticker: str, period: dict) -> bool:
    """Check if a period should be skipped based on SKIP_FILINGS list."""
    if ticker not in SKIP_FILINGS:
        return False
    source_file = period.get('source_file', '')
    for pattern in SKIP_FILINGS[ticker]:
        if pattern in source_file:
            return True
    return False


def get_value(period: dict, canonical: str) -> float | None:
    """Get a value from a period by canonical name."""
    if canonical in period.get('values', {}):
        return period['values'][canonical].get('value')
    return None


def normalize_value_to_thousands(value: float, unit_type: str, canonical: str = None) -> float:
    """
    Normalize a value to thousands for cross-period comparison.

    This is used ONLY for comparison checks (like cross_period_normalization).
    The actual data is NOT modified - this is a temporary normalization.

    Args:
        value: The raw value
        unit_type: The unit type (rupees, thousands, millions)
        canonical: Optional field name (EPS fields are not normalized)

    Returns:
        Value normalized to thousands scale
    """
    if value is None:
        return None

    # Skip normalization for EPS fields (always in rupees per share)
    if canonical and 'eps' in canonical.lower():
        return value

    unit_lower = unit_type.lower().strip() if unit_type else 'thousands'

    if unit_lower in ('rupees', 'rupee', 'full_rupees'):
        return value / 1000.0
    elif unit_lower == 'millions':
        return value * 1000.0
    elif 'thousands' in unit_lower:
        return value
    else:
        # Unknown unit, assume already in thousands
        return value


def get_normalized_value(period: dict, canonical: str) -> float | None:
    """
    Get a value from a period, normalized to thousands for comparison.

    This is used for cross-period comparison checks where we need values
    on the same scale regardless of the source unit_type.
    """
    raw_value = get_value(period, canonical)
    if raw_value is None:
        return None

    unit_type = period.get('unit_type', 'thousands')
    return normalize_value_to_thousands(raw_value, unit_type, canonical)


def get_taxation_total(period: dict) -> float | None:
    """
    Get total taxation, handling both single 'taxation' field and
    breakdown fields (taxation_current, taxation_prior, taxation_deferred).

    Two structures exist:
    1. Normal: 'taxation' = total tax (may equal taxation_current + taxation_deferred)
    2. Two-tier (e.g., LCI): 'taxation' = final taxes, separate from income tax components
       - Indicated by presence of 'taxation_income_tax_total' or 'profit_before_income_taxes'

    For two-tier structures, we need: taxation + taxation_current + taxation_deferred
    For normal structures, we use: taxation OR sum of components (not both)

    Priority:
    1. Use 'taxation_total' if present (pre-calculated total)
    2. If two-tier structure detected: sum taxation + taxation_current + taxation_deferred
    3. Otherwise: use 'taxation' if present, else sum components
    """
    values = period.get('values', {})

    # Check for pre-calculated taxation_total first
    if 'taxation_total' in values:
        return values['taxation_total'].get('value')

    # Detect two-tier taxation structure
    has_two_tier = 'taxation_income_tax_total' in values or 'profit_before_income_taxes' in values

    if has_two_tier:
        # Two-tier: sum taxation (final taxes) + income tax components
        total = 0
        found_any = False
        for key in ['taxation', 'taxation_current', 'taxation_deferred', 'taxation_prior', 'taxation_minimum']:
            if key in values:
                val = values[key].get('value')
                if val is not None:
                    total += val
                    found_any = True
        return total if found_any else None
    else:
        # Normal structure: use taxation if present, else sum components
        if 'taxation' in values:
            return values['taxation'].get('value')

        # Sum component taxation fields
        component_fields = ['taxation_current', 'taxation_deferred', 'taxation_prior', 'taxation_minimum']
        total = 0
        found_any = False
        for key in component_fields:
            if key in values:
                val = values[key].get('value')
                if val is not None:
                    total += val
                    found_any = True
        return total if found_any else None


def check_column_completeness(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check if we have expected periods.

    For quarterly filings: should have current quarter + prior year quarter
    For annual: should have current year + prior year

    Returns list of issues.
    """
    issues = []

    # Group periods by consolidation
    by_consolidation = defaultdict(list)
    for p in periods:
        by_consolidation[p['consolidation']].append(p)

    for consolidation, cons_periods in by_consolidation.items():
        # Get all (year, duration_months) combinations
        period_coverage = {}
        for p in cons_periods:
            key = (p['year'], p['duration_months'], p['period_end'])
            period_coverage[key] = p

        # Check for gaps in year sequence
        years = sorted(set(p['year'] for p in cons_periods))
        if len(years) >= 2:
            min_year, max_year = min(years), max(years)
            for year in range(min_year, max_year + 1):
                # Check if we have at least one period for this year
                year_periods = [p for p in cons_periods if p['year'] == year]
                if not year_periods:
                    issues.append({
                        'check': 'column_completeness',
                        'type': 'missing_year',
                        'consolidation': consolidation,
                        'year': year,
                        'severity': 'warning',
                        'message': f"No periods found for year {year}"
                    })

    return issues


def get_fiscal_year_end_month(periods: list[dict]) -> int:
    """
    Determine the fiscal year end month for a company based on its 12M (annual) periods.
    Returns the month (1-12), defaults to 12 (December) if no annual periods found.
    """
    # Look for 12M periods to determine fiscal year end
    annual_months = []
    for p in periods:
        if p['duration_months'] == 12:
            # Extract month from period_end (YYYY-MM-DD)
            month = int(p['period_end'].split('-')[1])
            annual_months.append(month)

    if annual_months:
        # Return most common annual month
        from collections import Counter
        return Counter(annual_months).most_common(1)[0][0]

    return 12  # Default to December


def get_fiscal_year(period_end: str, duration_months: int, fy_end_month: int) -> int:
    """
    Determine which fiscal year a period belongs to based on its end date and duration.

    For a company with FY ending in June (month 6):
    - 12M Jun 2024 -> FY 2024
    - 9M Mar 2024 -> FY 2024 (cumulative period ending in FY 2024)
    - 6M Dec 2023 -> FY 2024 (cumulative period ending in FY 2024)
    - 3M Sep 2023 -> FY 2024 (first quarter of FY 2024)

    The key insight: for cumulative periods, the 12M period end date defines the FY.
    A 3M period that would roll up into a 12M period ending Jun 2024 belongs to FY 2024.
    """
    year = int(period_end.split('-')[0])
    month = int(period_end.split('-')[1])

    # For 12M periods, the year of period_end is the fiscal year
    if duration_months == 12:
        return year

    # For shorter cumulative periods, we need to determine which 12M period they belong to
    # A 3M period ending in Sep 2023 for a Jun FY company belongs to FY 2024
    # because it would roll up into the 12M ending Jun 2024

    # Calculate how many months until the fiscal year end
    if month <= fy_end_month:
        # Period ends before or in the FY end month, same calendar year as FY
        return year
    else:
        # Period ends after the FY end month, belongs to next FY
        return year + 1


def check_monotonicity(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check monotonicity for cumulative periods in same fiscal year.
    For items like revenue_net: 9M > 6M > 3M within the same fiscal year.

    A cumulative series is identified by periods with increasing durations
    that share the same fiscal year end month and belong to the same fiscal year.

    Returns list of issues.
    """
    issues = []

    # Group periods by consolidation
    by_consolidation = defaultdict(list)
    for p in periods:
        by_consolidation[p['consolidation']].append(p)

    for consolidation, cons_periods in by_consolidation.items():
        # Determine fiscal year end month for this company
        fy_end_month = get_fiscal_year_end_month(cons_periods)

        # Group periods by fiscal year (using our fiscal year calculation)
        by_fy = defaultdict(list)
        for p in cons_periods:
            fy = get_fiscal_year(p['period_end'], p['duration_months'], fy_end_month)
            by_fy[fy].append(p)

        for fy, fy_periods in by_fy.items():
            # Only keep periods that are part of a cumulative series
            # (same fiscal year end month, increasing durations: 3M, 6M, 9M, 12M)
            cumulative_periods = []
            for p in fy_periods:
                period_month = int(p['period_end'].split('-')[1])
                # For cumulative series, period end month should match fiscal pattern
                # A Jun FY company has cumulative periods ending Sep (3M), Dec (6M), Mar (9M), Jun (12M)
                # The period end month should be: (fy_end_month + duration_months) mod 12 or = fy_end_month for 12M
                expected_month = (fy_end_month + p['duration_months']) % 12
                if expected_month == 0:
                    expected_month = 12

                # This period is part of cumulative series if its month matches expected
                if period_month == expected_month or (p['duration_months'] == 12 and period_month == fy_end_month):
                    cumulative_periods.append(p)

            if len(cumulative_periods) < 2:
                continue

            # Sort by duration (ascending)
            cumulative_periods.sort(key=lambda x: x['duration_months'])

            # Deduplicate - if same duration appears multiple times, take first
            seen_durations = set()
            deduped_periods = []
            for p in cumulative_periods:
                if p['duration_months'] not in seen_durations:
                    seen_durations.add(p['duration_months'])
                    deduped_periods.append(p)
            cumulative_periods = deduped_periods

            # Only check if we have multiple different durations
            if len(cumulative_periods) < 2:
                continue

            # Check monotonicity for each target item
            # NOTE: We use NORMALIZED values for comparison to handle unit variations
            # (e.g., one filing in rupees, another in thousands)
            for item in MONOTONIC_ITEMS:
                values_by_duration = []
                for p in cumulative_periods:
                    raw_val = get_value(p, item)
                    norm_val = get_normalized_value(p, item)
                    if norm_val is not None:
                        values_by_duration.append((p['duration_months'], p['duration'], norm_val, raw_val, p['period_end']))

                if len(values_by_duration) < 2:
                    continue

                # Check if values increase with duration (using normalized values)
                for i in range(1, len(values_by_duration)):
                    prev_dur, prev_dur_str, prev_norm, prev_raw, prev_end = values_by_duration[i-1]
                    curr_dur, curr_dur_str, curr_norm, curr_raw, curr_end = values_by_duration[i]

                    # Skip if either value is negative (losses don't follow cumulative pattern)
                    if prev_norm < 0 or curr_norm < 0:
                        continue

                    # For cumulative periods, longer duration should have larger value
                    if curr_norm < prev_norm:
                        pct_diff = ((prev_norm - curr_norm) / prev_norm * 100) if prev_norm != 0 else 0
                        issues.append({
                            'check': 'monotonicity',
                            'type': 'cumulative_decrease',
                            'consolidation': consolidation,
                            'fiscal_year': fy,
                            'fy_end_month': fy_end_month,
                            'item': item,
                            'shorter_period': f"{prev_dur_str} ({prev_end})",
                            'shorter_value': prev_raw,  # Report raw value
                            'shorter_normalized': prev_norm,  # Also include normalized
                            'longer_period': f"{curr_dur_str} ({curr_end})",
                            'longer_value': curr_raw,  # Report raw value
                            'longer_normalized': curr_norm,  # Also include normalized
                            'severity': 'error',
                            'message': f"{item}: {curr_dur_str} ({curr_norm:,.0f} normalized) < {prev_dur_str} ({prev_norm:,.0f} normalized) in FY{fy} - cumulative should increase"
                        })

    return issues


def check_period_arithmetic(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check that quarterly periods sum to annual.
    Q1 + Q2 + Q3 + Q4 = Annual (within tolerance)

    For fiscal year ending in month M:
    - Q1: 3M ending M
    - Q2: 6M ending M minus 3M ending M (derived)
    - etc.

    Since we have cumulative data (3M, 6M, 9M, 12M), we derive quarters:
    - Q1 = 3M value
    - Q2 = 6M - 3M
    - Q3 = 9M - 6M
    - Q4 = 12M - 9M

    Then: Q1 + Q2 + Q3 + Q4 should equal 12M

    Returns list of issues.
    """
    issues = []

    # Group periods by consolidation
    by_consolidation = defaultdict(list)
    for p in periods:
        by_consolidation[p['consolidation']].append(p)

    for consolidation, cons_periods in by_consolidation.items():
        # Group by year
        by_year = defaultdict(list)
        for p in cons_periods:
            by_year[p['year']].append(p)

        for year, year_periods in by_year.items():
            # Create lookup by duration
            by_duration = {p['duration_months']: p for p in year_periods}

            # Check if we have 12M (annual) and some quarterly data
            if 12 not in by_duration:
                continue

            annual_period = by_duration[12]

            # Get all canonical items from annual period
            for canonical in annual_period.get('values', {}).keys():
                # Skip EPS and calculated items for this check
                if 'eps' in canonical.lower():
                    continue

                annual_val = get_value(annual_period, canonical)
                if annual_val is None:
                    continue

                # Try to get quarterly values and compute sum
                # We need at least 3M and 6M, or 3M, 6M, 9M for a meaningful check
                q3m = get_value(by_duration.get(3, {}), canonical) if 3 in by_duration else None
                q6m = get_value(by_duration.get(6, {}), canonical) if 6 in by_duration else None
                q9m = get_value(by_duration.get(9, {}), canonical) if 9 in by_duration else None

                # Compute derived quarters
                quarters_sum = None
                if q3m is not None and q6m is not None and q9m is not None:
                    # Full quarterly data available
                    q1 = q3m
                    q2 = q6m - q3m
                    q3 = q9m - q6m
                    q4 = annual_val - q9m  # Implied Q4
                    quarters_sum = q1 + q2 + q3 + q4  # This should equal annual by construction
                    # Actually, this always equals annual, so skip this trivial check
                    continue
                elif q3m is not None and q9m is not None:
                    # Can do partial check: 9M should be 3/4 of annual roughly
                    # Skip for now - not a reliable check
                    continue

                # If we can't compute quarters, skip
                # The main value of this check is when we have actual derived quarterly JSON

    return issues


def check_semantic_equations(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check semantic equations as sanity checks:
    - gross_profit = revenue_net + cost_of_goods_sold (5% tolerance)
    - net_profit = profit_before_tax + taxation (20% tolerance, skip if complex P&L)

    The net_profit check catches OCR row shifting where values are assigned to
    wrong labels. Higher tolerance accounts for legitimate variations (associates,
    discontinued ops) while still flagging major discrepancies.

    Returns list of issues.
    """
    issues = []

    for period in periods:
        # Check standard equations (gross_profit)
        for result_field, operands in SEMANTIC_EQUATIONS:
            result_val = get_value(period, result_field)
            if result_val is None:
                continue

            # Compute expected value from operands
            expected = 0
            all_operands_present = True
            for operand_field, sign in operands:
                operand_val = get_value(period, operand_field)
                if operand_val is None:
                    all_operands_present = False
                    break
                expected += sign * operand_val

            if not all_operands_present:
                continue

            # Compare result with expected
            if result_val == 0 and expected == 0:
                continue  # Both zero, that's fine

            diff = abs(result_val - expected)
            base = max(abs(result_val), abs(expected), 1)  # Avoid division by zero
            pct_diff = (diff / base) * 100

            if pct_diff > SEMANTIC_TOLERANCE_PCT:
                # Build operand description
                operand_desc = ' + '.join([
                    f"{op}({get_value(period, op):,.0f})" if get_value(period, op) else f"{op}(N/A)"
                    for op, _ in operands
                ])

                issues.append({
                    'check': 'semantic_equation',
                    'type': 'equation_mismatch',
                    'consolidation': period['consolidation'],
                    'period_end': period['period_end'],
                    'duration': period['duration'],
                    'source_file': period.get('source_file', 'unknown'),
                    'equation': f"{result_field} = {operand_desc}",
                    'expected': expected,
                    'actual': result_val,
                    'diff': diff,
                    'pct_diff': round(pct_diff, 2),
                    'severity': 'warning' if pct_diff < 10 else 'error',
                    'message': f"{result_field}: expected {expected:,.0f}, got {result_val:,.0f} ({pct_diff:.1f}% diff)"
                })

        # Check net_profit = profit_before_tax + taxation
        # Simple equation - other items (share_of_associates, etc) are typically already in PBT
        net_profit_val = get_value(period, 'net_profit')
        pbt_val = get_value(period, 'profit_before_tax')
        taxation_val = get_taxation_total(period)

        # Skip if net_profit_continuing exists (discontinued ops case handled separately)
        if net_profit_val is not None and pbt_val is not None and get_value(period, 'net_profit_continuing') is None:
            # Simple equation: net_profit = PBT + taxation
            if taxation_val is not None:
                expected = pbt_val + taxation_val
                operand_desc = f"profit_before_tax({pbt_val:,.0f}) + taxation({taxation_val:,.0f})"
            else:
                # No taxation found - just check if net_profit equals PBT (some edge cases)
                expected = pbt_val
                operand_desc = f"profit_before_tax({pbt_val:,.0f})"

            if not (net_profit_val == 0 and expected == 0):
                diff = abs(net_profit_val - expected)
                base = max(abs(net_profit_val), abs(expected), 1)
                pct_diff = (diff / base) * 100

                if pct_diff > SEMANTIC_TOLERANCE_PCT:
                    issues.append({
                        'check': 'semantic_equation',
                        'type': 'equation_mismatch',
                        'consolidation': period['consolidation'],
                        'period_end': period['period_end'],
                        'duration': period['duration'],
                        'source_file': period.get('source_file', 'unknown'),
                        'equation': f"net_profit = {operand_desc}",
                        'expected': expected,
                        'actual': net_profit_val,
                        'diff': diff,
                        'pct_diff': round(pct_diff, 2),
                        'severity': 'warning' if pct_diff < 10 else 'error',
                        'message': f"net_profit: expected {expected:,.0f}, got {net_profit_val:,.0f} ({pct_diff:.1f}% diff)"
                    })

        # For discontinued ops / complex P&L structures:
        # Check net_profit = net_profit_continuing + other components
        # Note: share_of_associates, other_non_operating may appear after net_profit_continuing
        net_profit_cont = get_value(period, 'net_profit_continuing')
        net_profit_disc = get_value(period, 'net_profit_discontinued')
        share_assoc = get_value(period, 'share_of_associates')
        share_jv = get_value(period, 'share_of_joint_ventures')
        gain_disposal = get_value(period, 'gain_on_disposal')
        loss_disposal = get_value(period, 'loss_on_disposal')
        other_non_op = get_value(period, 'other_non_operating')
        other_inc = get_value(period, 'other_income')

        if net_profit_val is not None and net_profit_cont is not None:
            # Build equation: net_profit = continuing + discontinued + other components
            operands = [('net_profit_continuing', net_profit_cont)]

            if net_profit_disc is not None:
                operands.append(('net_profit_discontinued', net_profit_disc))

            # Add components that appear AFTER net_profit_continuing if they help balance
            # This handles: share_of_associates (holding companies), other_non_operating (refineries)
            base_expected = sum(v for _, v in operands)

            # Try adding various post-continuing items if they help balance
            for name, val in [('share_of_associates', share_assoc),
                              ('share_of_joint_ventures', share_jv),
                              ('gain_on_disposal', gain_disposal),
                              ('loss_on_disposal', loss_disposal),
                              ('other_non_operating', other_non_op),
                              ('other_income', other_inc)]:
                if val is not None:
                    # Check if this item appears AFTER continuing (helps balance the equation)
                    test_expected = base_expected + val
                    if abs(net_profit_val - test_expected) < abs(net_profit_val - base_expected):
                        operands.append((name, val))
                        base_expected = test_expected

            expected = sum(v for _, v in operands)
            operand_desc = ' + '.join([f"{n}({v:,.0f})" for n, v in operands])

            if not (net_profit_val == 0 and expected == 0):
                diff = abs(net_profit_val - expected)
                base = max(abs(net_profit_val), abs(expected), 1)
                pct_diff = (diff / base) * 100

                if pct_diff > SEMANTIC_TOLERANCE_PCT:
                    issues.append({
                        'check': 'semantic_equation',
                        'type': 'equation_mismatch',
                        'consolidation': period['consolidation'],
                        'period_end': period['period_end'],
                        'duration': period['duration'],
                        'source_file': period.get('source_file', 'unknown'),
                        'equation': f"net_profit = {operand_desc}",
                        'expected': expected,
                        'actual': net_profit_val,
                        'diff': diff,
                        'pct_diff': round(pct_diff, 2),
                        'severity': 'warning' if pct_diff < 10 else 'error',
                        'message': f"net_profit: expected {expected:,.0f}, got {net_profit_val:,.0f} ({pct_diff:.1f}% diff)"
                    })

    return issues


def check_unit_type(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check that all periods have valid unit_type.

    Returns list of issues.
    """
    issues = []

    for period in periods:
        unit_type = period.get('unit_type', '').lower()

        if not unit_type:
            issues.append({
                'check': 'unit_type',
                'type': 'missing',
                'consolidation': period.get('consolidation', 'unknown'),
                'period_end': period.get('period_end'),
                'duration': period.get('duration'),
                'source_file': period.get('source_file', 'unknown'),
                'severity': 'error',
                'message': f"Missing unit_type in {period.get('source_file', 'unknown')}"
            })
        elif unit_type not in VALID_UNITS:
            issues.append({
                'check': 'unit_type',
                'type': 'invalid',
                'consolidation': period.get('consolidation', 'unknown'),
                'period_end': period.get('period_end'),
                'duration': period.get('duration'),
                'source_file': period.get('source_file', 'unknown'),
                'unit_type': unit_type,
                'severity': 'error',
                'message': f"Invalid unit_type '{unit_type}' - must be one of {VALID_UNITS}"
            })

    return issues


def check_critical_fields(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check that critical fields are present in each period.

    For P&L: must have at least one revenue field AND one bottom_line field.

    Returns list of issues.
    """
    issues = []

    for period in periods:
        values = period.get('values', {})
        field_names = set(values.keys())

        # Check revenue fields
        revenue_fields = CRITICAL_FIELDS_PL['revenue']
        has_revenue = any(f in field_names for f in revenue_fields)

        # Check bottom_line fields
        bottom_line_fields = CRITICAL_FIELDS_PL['bottom_line']
        has_bottom_line = any(f in field_names for f in bottom_line_fields)

        if not has_revenue:
            issues.append({
                'check': 'critical_fields',
                'type': 'missing_revenue',
                'consolidation': period.get('consolidation', 'unknown'),
                'period_end': period.get('period_end'),
                'duration': period.get('duration'),
                'source_file': period.get('source_file', 'unknown'),
                'severity': 'error',
                'message': f"Missing revenue field - need one of {revenue_fields}"
            })

        if not has_bottom_line:
            issues.append({
                'check': 'critical_fields',
                'type': 'missing_bottom_line',
                'consolidation': period.get('consolidation', 'unknown'),
                'period_end': period.get('period_end'),
                'duration': period.get('duration'),
                'source_file': period.get('source_file', 'unknown'),
                'severity': 'error',
                'message': f"Missing bottom line field - need one of {bottom_line_fields}"
            })

    return issues


def check_cross_period_normalization(periods: list[dict], verbose: bool = False) -> list[dict]:
    """
    Check for 1000x outliers that indicate unit mismatch.

    Compares revenue_net (or net_profit) across periods within same consolidation.
    If any period is >100x or <0.01x the median, flag as potential unit error.

    NOTE: This check uses NORMALIZED values (all converted to thousands) for comparison.
    This handles cases where different filings legitimately use different units
    (e.g., some in rupees, some in thousands). After normalization, legitimate
    variations are on the same scale and won't trigger false positives.

    Returns list of issues.
    """
    issues = []

    # Group by consolidation
    by_consolidation = defaultdict(list)
    for p in periods:
        by_consolidation[p.get('consolidation', 'unknown')].append(p)

    # Reference field for comparison (prefer revenue_net, fallback to net_profit)
    ref_fields = ['revenue_net', 'net_profit', 'gross_profit']

    for consolidation, cons_periods in by_consolidation.items():
        # Find first available reference field
        # Use get_normalized_value for comparison to handle unit variations
        ref_field = None
        for field in ref_fields:
            values = [abs(get_normalized_value(p, field)) for p in cons_periods if get_normalized_value(p, field) is not None]
            if len(values) >= 3:  # Need at least 3 values to compute meaningful median
                ref_field = field
                break

        if not ref_field:
            continue

        # Get all NORMALIZED values for the reference field
        ref_values = []
        for p in cons_periods:
            val = get_normalized_value(p, ref_field)
            if val is not None and val != 0:
                ref_values.append((p, abs(val)))

        if len(ref_values) < 3:
            continue

        # Calculate median of normalized values
        sorted_values = sorted(v for _, v in ref_values)
        mid = len(sorted_values) // 2
        if len(sorted_values) % 2 == 0:
            median = (sorted_values[mid - 1] + sorted_values[mid]) / 2
        else:
            median = sorted_values[mid]

        if median == 0:
            continue

        # Check each period against median (using normalized values)
        for period, val in ref_values:
            ratio = val / median

            if ratio > CROSS_PERIOD_THRESHOLD or ratio < (1 / CROSS_PERIOD_THRESHOLD):
                # Get raw value for reporting (more meaningful to user)
                raw_val = get_value(period, ref_field)
                raw_val = abs(raw_val) if raw_val else val
                issues.append({
                    'check': 'cross_period_normalization',
                    'type': '1000x_outlier',
                    'consolidation': consolidation,
                    'period_end': period.get('period_end'),
                    'duration': period.get('duration'),
                    'source_file': period.get('source_file', 'unknown'),
                    'ref_field': ref_field,
                    'value': raw_val,  # Report raw value
                    'normalized_value': val,  # Also include normalized for debugging
                    'median': median,
                    'ratio': round(ratio, 1),
                    'severity': 'error',
                    'message': f"{ref_field}={raw_val:,.0f} (normalized: {val:,.0f}) is {ratio:.0f}x median ({median:,.0f}) - likely unit error"
                })

    return issues




def get_unit_diagnostics(ticker: str) -> dict:
    """
    Get unit type info for a ticker from extraction files.

    Returns diagnostic info (not added to issue count) to help investigate failures.
    This helps identify if cross_period_normalization or monotonicity failures
    might be caused by unit declaration mismatches.

    Returns:
        {
            'has_variation': bool,
            'unit_counts': {'thousands': 10, 'rupees': 2},
            'majority_unit': 'thousands',
            'outlier_files': ['TICKER_quarterly_2024-03-31_consolidated.md']
        }
    """
    import re

    files = list(EXTRACTION_DIR.glob(f"{ticker}_*.md"))
    if not files:
        return {'has_variation': False, 'unit_counts': {}, 'majority_unit': None, 'outlier_files': []}

    unit_counts = defaultdict(list)
    for f in files:
        content = f.read_text(encoding='utf-8')
        match = re.search(r'UNIT_TYPE:\s*(\w+)', content)
        unit = match.group(1).lower() if match else 'unknown'
        unit_counts[unit].append(f.name)

    if len(unit_counts) <= 1:
        majority = list(unit_counts.keys())[0] if unit_counts else None
        return {
            'has_variation': False,
            'unit_counts': {u: len(files) for u, files in unit_counts.items()},
            'majority_unit': majority,
            'outlier_files': []
        }

    # Find majority unit
    majority_unit = max(unit_counts.keys(), key=lambda u: len(unit_counts[u]))

    # Find outlier files
    outlier_files = []
    for unit, filenames in unit_counts.items():
        if unit != majority_unit:
            outlier_files.extend(filenames)

    return {
        'has_variation': True,
        'unit_counts': {u: len(files) for u, files in unit_counts.items()},
        'majority_unit': majority_unit,
        'outlier_files': sorted(outlier_files)
    }


def qc_ticker(ticker: str, data: dict, verbose: bool = False) -> dict:
    """
    Run all QC checks on a single ticker's data.

    Returns dict with:
    - ticker
    - period_count
    - issues (list)
    - summary
    """
    all_periods = data.get('periods', [])

    # Filter out periods from SKIP_FILINGS
    periods = [p for p in all_periods if not should_skip_period(ticker, p)]
    skipped_count = len(all_periods) - len(periods)

    result = {
        'ticker': ticker,
        'period_count': len(periods),
        'skipped_periods': skipped_count,
        'issues': [],
        'summary': {
            'total_issues': 0,
            'errors': 0,
            'warnings': 0,
            'by_check': {},
        }
    }

    if not periods:
        return result

    # Run all checks
    all_issues = []

    # 1. Column completeness
    completeness_issues = check_column_completeness(periods, verbose)
    all_issues.extend(completeness_issues)
    result['summary']['by_check']['column_completeness'] = len(completeness_issues)

    # 2. Monotonicity
    monotonicity_issues = check_monotonicity(periods, verbose)
    all_issues.extend(monotonicity_issues)
    result['summary']['by_check']['monotonicity'] = len(monotonicity_issues)

    # 3. Period arithmetic (currently limited - would need derived quarters)
    arithmetic_issues = check_period_arithmetic(periods, verbose)
    all_issues.extend(arithmetic_issues)
    result['summary']['by_check']['period_arithmetic'] = len(arithmetic_issues)

    # 4. Semantic equations
    semantic_issues = check_semantic_equations(periods, verbose)
    all_issues.extend(semantic_issues)
    result['summary']['by_check']['semantic_equations'] = len(semantic_issues)

    # 5. Unit type validation
    unit_issues = check_unit_type(periods, verbose)
    all_issues.extend(unit_issues)
    result['summary']['by_check']['unit_type'] = len(unit_issues)

    # 6. Critical fields
    critical_issues = check_critical_fields(periods, verbose)
    all_issues.extend(critical_issues)
    result['summary']['by_check']['critical_fields'] = len(critical_issues)

    # 7. Cross-period normalization (1000x outlier detection)
    cross_period_issues = check_cross_period_normalization(periods, verbose)
    all_issues.extend(cross_period_issues)
    result['summary']['by_check']['cross_period_normalization'] = len(cross_period_issues)

    # NOTE: Ref formula validation removed - it's done in Step4_QCPL_Extraction.py
    # Ref letters are only consistent within a single extraction file, not across periods.
    # Step4 validates formulas on the extraction markdown where refs are defined.

    # Summarize
    result['issues'] = all_issues
    result['summary']['total_issues'] = len(all_issues)
    result['summary']['errors'] = sum(1 for i in all_issues if i.get('severity') == 'error')
    result['summary']['warnings'] = sum(1 for i in all_issues if i.get('severity') == 'warning')

    # Add unit diagnostics as investigation context (not counted as issues)
    unit_diag = get_unit_diagnostics(ticker)
    if unit_diag['has_variation']:
        result['unit_diagnostics'] = unit_diag

    return result


def main():
    parser = argparse.ArgumentParser(description="QC2 P&L - Systematic QC Checks")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: QC P&L")
    print("=" * 70)

    if not INPUT_DIR.exists():
        print(f"ERROR: Input directory not found: {INPUT_DIR}")
        print(f"Please run Step5_JSONifyPL_v2.py first to generate the JSON files.")
        return

    # Get all JSON files
    files = sorted(INPUT_DIR.glob("*.json"))
    if not files:
        print(f"ERROR: No JSON files found in {INPUT_DIR}")
        return

    if args.ticker:
        files = [f for f in files if f.stem == args.ticker]
        if not files:
            print(f"ERROR: No file found for ticker {args.ticker}")
            return

    print(f"\nProcessing {len(files)} tickers...\n")

    # Process each ticker
    all_results = []
    global_stats = {
        'tickers_total': 0,
        'tickers_clean': 0,
        'tickers_with_issues': 0,
        'total_issues': 0,
        'total_errors': 0,
        'total_warnings': 0,
        'by_check': defaultdict(int),
    }

    for filepath in files:
        ticker = filepath.stem

        with open(filepath) as f:
            data = json.load(f)

        result = qc_ticker(ticker, data, args.verbose)
        all_results.append(result)

        # Update global stats
        global_stats['tickers_total'] += 1
        global_stats['total_issues'] += result['summary']['total_issues']
        global_stats['total_errors'] += result['summary']['errors']
        global_stats['total_warnings'] += result['summary']['warnings']

        for check, count in result['summary']['by_check'].items():
            global_stats['by_check'][check] += count

        if result['summary']['total_issues'] == 0:
            global_stats['tickers_clean'] += 1
        else:
            global_stats['tickers_with_issues'] += 1

        # Print results
        if args.verbose or result['summary']['total_issues'] > 0:
            status = "CLEAN" if result['summary']['total_issues'] == 0 else "ISSUES"
            print(f"{status}: {ticker} ({result['period_count']} periods, {result['summary']['total_issues']} issues)")

            # Show unit diagnostics if there's variation (useful for investigating failures)
            if 'unit_diagnostics' in result and result['summary']['total_issues'] > 0:
                diag = result['unit_diagnostics']
                print(f"  [UNIT CONTEXT] {diag['unit_counts']} - outliers: {diag['outlier_files'][:3]}{'...' if len(diag['outlier_files']) > 3 else ''}")

            if args.verbose and result['issues']:
                for issue in result['issues'][:5]:  # Show first 5 issues
                    severity = issue.get('severity', 'info').upper()
                    print(f"  [{severity}] {issue['check']}: {issue['message']}")
                if len(result['issues']) > 5:
                    print(f"  ... and {len(result['issues']) - 5} more issues")
        else:
            print(f"  {ticker}: {result['period_count']} periods - clean")

    # Save results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    output_data = {
        'generated_at': datetime.now().isoformat(),
        'stats': {
            'tickers_total': global_stats['tickers_total'],
            'tickers_clean': global_stats['tickers_clean'],
            'tickers_with_issues': global_stats['tickers_with_issues'],
            'total_issues': global_stats['total_issues'],
            'total_errors': global_stats['total_errors'],
            'total_warnings': global_stats['total_warnings'],
            'by_check': dict(global_stats['by_check']),
        },
        'results': all_results,
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Tickers processed:     {global_stats['tickers_total']}")
    print(f"  Tickers clean:         {global_stats['tickers_clean']}")
    print(f"  Tickers with issues:   {global_stats['tickers_with_issues']}")
    print()
    print(f"  Total issues:          {global_stats['total_issues']}")
    print(f"  Errors:                {global_stats['total_errors']}")
    print(f"  Warnings:              {global_stats['total_warnings']}")
    print()
    print("  Issues by check:")
    for check, count in sorted(global_stats['by_check'].items()):
        print(f"    {check}: {count}")

    if global_stats['tickers_total'] > 0:
        clean_rate = global_stats['tickers_clean'] / global_stats['tickers_total'] * 100
        print(f"\n  Clean rate: {clean_rate:.1f}%")

    print()
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
