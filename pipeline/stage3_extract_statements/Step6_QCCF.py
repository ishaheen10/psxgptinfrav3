#!/usr/bin/env python3
"""
Step 6: QC Cash Flow

Runs systematic QC checks on the QC-optimized JSON format:
1. Column completeness - check expected periods per filing type
2. Period arithmetic - Q1+Q2+Q3+Q4 should equal Annual (within tolerance)
3. Semantic equations - CF identity checks supporting TWO valid patterns:

   Pattern A (standard):
     - cfo + cfi + cff = net_cash_change
     - cash_start + net_cash_change + reconciling_adjustments = cash_end

   Pattern B (banks/oil companies - FX before net_cash_change):
     - cfo + cfi + cff + reconciling_adjustments = net_cash_change
     - cash_start + net_cash_change = cash_end

   (reconciling_adjustments include: fx_adjustment, fx_effect_on_cash,
    cash_flow_assets_held_for_sale, M&A effects, discontinued ops, etc.)

4. Unit type validation - Check for valid unit types
5. Critical fields check - Verify required fields present
6. Cross-period normalization - Detect 1000x outliers

Note: NO monotonicity check for CF - cash flows can be positive or negative.
NOTE: Source value matching is done in Step4_QCCF_Extraction.py

Input:  data/json_cf/{TICKER}.json
Output: artifacts/stage3/step6_qc_cf_results.json

Usage:
    python3 Step6_QCCF.py                    # QC all
    python3 Step6_QCCF.py --ticker LUCK      # Single ticker
    python3 Step6_QCCF.py --verbose          # Show all details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "json_cf"
EXTRACTION_DIR = PROJECT_ROOT / "data" / "extracted_cf"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step6_qc_cf_results.json"

# Tolerances
SEMANTIC_TOLERANCE_PCT = 5.0  # 5% for semantic equation checks
PERIOD_ARITHMETIC_TOLERANCE_PCT = 5.0  # 5% for period arithmetic (Q1+Q2+Q3+Q4 = Annual)

# Valid unit types
VALID_UNITS = {'thousands', 'millions', 'rupees', 'full_rupees'}

# Critical fields for Cash Flow
# Must have CFO, CFI, CFF and either net_cash_change or (cash_start + cash_end)
CRITICAL_FIELDS_CF = {
    'operating': ['cfo', 'cash_from_operations'],
    'investing': ['cfi', 'cash_from_investing'],
    'financing': ['cff', 'cash_from_financing'],
}

# Cross-period normalization: flag if value is >100x or <0.01x median
CROSS_PERIOD_THRESHOLD = 100

# Skip list: known problematic filings or acceptable anomalies
# Format: {ticker: [filing_patterns]} where pattern matches source_file
SKIP_FILINGS = {
    # OCR corruption - values shifted between rows
    'EFERT': ['annual_2021'],
    'SHEL': ['annual_2023'],  # BS and P&L combined on same page
    # Placeholder - will be populated as CF-specific issues are discovered
}


def should_skip_filing(ticker: str, filing: dict) -> bool:
    """Check if a filing should be skipped based on SKIP_FILINGS list."""
    if ticker not in SKIP_FILINGS:
        return False
    source_file = filing.get('source_file', '')
    for pattern in SKIP_FILINGS[ticker]:
        if pattern in source_file:
            return True
    return False


def get_value(period: dict, canonical: str) -> float | None:
    """Get a value from period by canonical name."""
    return period.get('values', {}).get(canonical)


def normalize_value_to_thousands(value: float, unit_type: str) -> float:
    """
    Normalize a value to thousands for cross-period comparison.

    This is used ONLY for comparison checks (like cross_period_normalization).
    The actual data is NOT modified - this is a temporary normalization.

    Args:
        value: The raw value
        unit_type: The unit type (rupees, thousands, millions)

    Returns:
        Value normalized to thousands scale
    """
    if value is None:
        return None

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


def get_normalized_value(filing: dict, canonical: str) -> float | None:
    """
    Get a value from a filing, normalized to thousands for comparison.

    This is used for cross-period comparison checks where we need values
    on the same scale regardless of the source unit_type.
    """
    raw_value = filing.get('values', {}).get(canonical)
    if raw_value is None:
        return None

    unit_type = filing.get('unit_type', 'thousands')
    return normalize_value_to_thousands(raw_value, unit_type)


def check_within_tolerance(expected: float, actual: float, tolerance_pct: float) -> tuple[bool, float]:
    """
    Check if actual is within tolerance_pct of expected.
    Returns (passed, pct_diff).
    """
    if expected == 0 and actual == 0:
        return True, 0.0

    if expected == 0:
        return False, float('inf')

    pct_diff = abs(actual - expected) / abs(expected) * 100
    return pct_diff <= tolerance_pct, pct_diff


def check_column_completeness(filing: dict) -> dict:
    """
    Check if filing has expected periods.

    For quarterly filings: should have current quarter + possibly prior year quarter
    For annual: should have current year + prior year
    """
    result = {
        'check': 'column_completeness',
        'status': 'pass',
        'periods_found': len(filing['periods']),
        'details': [],
    }

    period_type = filing['period_type']
    periods = filing['periods']

    if not periods:
        result['status'] = 'fail'
        result['details'].append("No periods found in filing")
        return result

    # For annual filings, typically expect 2 periods (current + prior year)
    # For quarterly, typically expect 2 periods (current + prior year same quarter)
    # But single period is acceptable in some cases

    if period_type == 'annual':
        durations = [p['duration'] for p in periods]
        annual_count = sum(1 for d in durations if d == '12M')
        if annual_count < 1:
            result['status'] = 'warning'
            result['details'].append(f"Annual filing has no 12M periods, found: {durations}")
    else:
        # Quarterly - check that we have at least one period
        if len(periods) < 1:
            result['status'] = 'fail'
            result['details'].append("Quarterly filing has no periods")

    # Check for missing key values in each period
    key_fields = ['cfo', 'cfi', 'cff', 'net_cash_change']
    for period in periods:
        missing = [f for f in key_fields if get_value(period, f) is None]
        if missing:
            result['status'] = 'warning'
            result['details'].append(f"Period {period['period_end']} missing key fields: {missing}")

    return result


def check_semantic_equations(filing: dict) -> dict:
    """
    Check CF semantic equations for all periods in a filing.

    Supports TWO valid CF presentation patterns:

    Pattern A (standard):
      - cfo + cfi + cff = net_cash_change
      - cash_start + net_cash_change + reconciling_adjustments = cash_end

    Pattern B (banks/oil companies - FX before net_cash_change):
      - cfo + cfi + cff + reconciling_adjustments = net_cash_change
      - cash_start + net_cash_change = cash_end

    Both patterns are valid - the QC tries both and passes if either works.
    """
    result = {
        'check': 'semantic_equations',
        'status': 'pass',
        'equations_checked': 0,
        'equations_passed': 0,
        'failures': [],
    }

    for period in filing['periods']:
        period_result = check_semantic_equations_period(period)
        result['equations_checked'] += period_result['equations_checked']
        result['equations_passed'] += period_result['equations_passed']
        result['failures'].extend(period_result['failures'])

    if result['failures']:
        result['status'] = 'fail'

    return result


def get_cash_reconciling_adjustments(period: dict) -> tuple[float, list]:
    """
    Get total of all cash reconciling adjustments and list of which ones were used.

    These are items that appear between net_cash_change and cash_end in the CF statement,
    such as FX effects, cash from held-for-sale assets, M&A effects, discontinued ops, etc.
    """
    # Known reconciling item fields (ordered by frequency)
    # These are items that appear between net_cash_change and cash_end
    RECONCILING_FIELDS = [
        # FX effects (most common)
        'fx_adjustment',                    # General FX adjustment (922 occurrences)
        'fx_effect_on_cash',               # FX effect on cash (253)

        # Held-for-sale assets
        'cash_flow_assets_held_for_sale',  # TPLP pattern (3)
        'net_cash_assets_held_for_sale',   # Similar to TPLP (2)
        'non_current_asset_held_for_sale', # Alternative mapping (1)

        # M&A / Business combinations
        'cash_from_discontinued_ops',      # Discontinued operations (2)
        'net_cash_inflow_amalgamation',    # FHAM pattern - M&A (2)
        'cash_from_merger',                # M&A (3)
        'cash_from_acquisition',           # M&A (2)
        'transfer_upon_amalgamation',      # M&A (1)
        'effect_of_amalgamation',          # M&A (1)
        'transfer_from_amalgamation',      # M&A - FHAM specific
        'cash_from_acquisitions',          # M&A (3)
        'cash_on_acquisition',             # M&A (1)
        'cash_from_business_combination',  # FCCL pattern (2)

        # Subsidiary disposals/transfers
        'cash_of_subsidiary_at_disposal',  # Subsidiary disposal (1)
        'cash_from_subsidiary_disposal',   # Subsidiary disposal (1)
        'cash_transferred_to_subsidiary',  # SRVI pattern (3)
        'cash_transferred_to_subsidiaries', # Variant (1)
        'cash_equivalents_of_ncpl',        # NCL specific (1)
        'cash_equivalents_ncpl',           # NCL variant (1)
        'cash_equivalents_from_demerged_associate',  # NCL demerger (1)

        # Opening balance adjustments (unusual patterns)
        'short_term_borrowings_start',     # MTL pattern - opening ST borrowings (4)
        'short_term_borrowings_transferred', # PKGS pattern (1)
    ]

    total = 0.0
    used_fields = []

    for field in RECONCILING_FIELDS:
        value = get_value(period, field)
        if value is not None and value != 0:
            total += value
            used_fields.append(field)

    return total, used_fields


def check_semantic_equations_period(period: dict) -> dict:
    """
    Check CF semantic equations for a single period.

    Supports TWO valid CF presentation patterns:

    Pattern A (standard):
      - cfo + cfi + cff = net_cash_change
      - cash_start + net_cash_change + reconciling_adjustments = cash_end

    Pattern B (banks/oil companies - FX before net_cash_change):
      - cfo + cfi + cff + reconciling_adjustments = net_cash_change
      - cash_start + net_cash_change = cash_end

    Both patterns are valid - the QC tries both and passes if either works.
    """
    result = {
        'check': 'semantic_equations',
        'status': 'pass',
        'equations_checked': 0,
        'equations_passed': 0,
        'failures': [],
    }

    period_end = period['period_end']
    duration = period['duration']

    cfo = get_value(period, 'cfo')
    cfi = get_value(period, 'cfi')
    cff = get_value(period, 'cff')
    net_cash_change = get_value(period, 'net_cash_change')
    cash_start = get_value(period, 'cash_start')
    cash_end = get_value(period, 'cash_end')
    reconciling_total, reconciling_fields = get_cash_reconciling_adjustments(period)

    # We need to check if the data fits Pattern A or Pattern B
    # Try both and pass if either works

    have_activity_data = all(v is not None for v in [cfo, cfi, cff, net_cash_change])
    have_cash_data = all(v is not None for v in [cash_start, net_cash_change, cash_end])

    if have_activity_data and have_cash_data:
        # Try Pattern A: cfo + cfi + cff = net_cash_change AND cash_start + net_cash_change + fx = cash_end
        pattern_a_activity_sum = cfo + cfi + cff
        pattern_a_activity_passed, pattern_a_activity_diff = check_within_tolerance(
            pattern_a_activity_sum, net_cash_change, SEMANTIC_TOLERANCE_PCT)

        pattern_a_cash_expected = cash_start + net_cash_change + reconciling_total
        pattern_a_cash_passed, pattern_a_cash_diff = check_within_tolerance(
            pattern_a_cash_expected, cash_end, SEMANTIC_TOLERANCE_PCT)

        # Try Pattern B: cfo + cfi + cff + fx = net_cash_change AND cash_start + net_cash_change = cash_end
        pattern_b_activity_sum = cfo + cfi + cff + reconciling_total
        pattern_b_activity_passed, pattern_b_activity_diff = check_within_tolerance(
            pattern_b_activity_sum, net_cash_change, SEMANTIC_TOLERANCE_PCT)

        pattern_b_cash_expected = cash_start + net_cash_change
        pattern_b_cash_passed, pattern_b_cash_diff = check_within_tolerance(
            pattern_b_cash_expected, cash_end, SEMANTIC_TOLERANCE_PCT)

        # Pattern A passes if both equations work
        pattern_a_passes = pattern_a_activity_passed and pattern_a_cash_passed
        # Pattern B passes if both equations work
        pattern_b_passes = pattern_b_activity_passed and pattern_b_cash_passed

        # If either pattern fully passes, we pass (count as 2 equations checked, 2 passed)
        if pattern_a_passes or pattern_b_passes:
            result['equations_checked'] += 2
            result['equations_passed'] += 2
        else:
            # Neither pattern fully passes - report failures for the better-performing pattern
            # (the one where at least one equation passes, or report Pattern A as default)
            result['equations_checked'] += 2

            # Check activity equation - pass if either pattern passes
            if pattern_a_activity_passed or pattern_b_activity_passed:
                result['equations_passed'] += 1
            else:
                # Report the better (smaller diff) failure
                if pattern_a_activity_diff <= pattern_b_activity_diff:
                    result['failures'].append({
                        'period': period_end,
                        'duration': duration,
                        'equation': 'cfo + cfi + cff = net_cash_change',
                        'expected': pattern_a_activity_sum,
                        'actual': net_cash_change,
                        'pct_diff': round(pattern_a_activity_diff, 2),
                    })
                else:
                    if reconciling_fields:
                        equation_str = f'cfo + cfi + cff + ({", ".join(reconciling_fields)}) = net_cash_change'
                    else:
                        equation_str = 'cfo + cfi + cff = net_cash_change'
                    result['failures'].append({
                        'period': period_end,
                        'duration': duration,
                        'equation': equation_str,
                        'expected': pattern_b_activity_sum,
                        'actual': net_cash_change,
                        'pct_diff': round(pattern_b_activity_diff, 2),
                    })

            # Check cash equation - pass if either pattern passes
            if pattern_a_cash_passed or pattern_b_cash_passed:
                result['equations_passed'] += 1
            else:
                # Report the better (smaller diff) failure
                if pattern_a_cash_diff <= pattern_b_cash_diff:
                    if reconciling_fields:
                        equation_str = f'cash_start + net_cash_change + ({", ".join(reconciling_fields)}) = cash_end'
                    else:
                        equation_str = 'cash_start + net_cash_change = cash_end'
                    result['failures'].append({
                        'period': period_end,
                        'duration': duration,
                        'equation': equation_str,
                        'expected': pattern_a_cash_expected,
                        'actual': cash_end,
                        'pct_diff': round(pattern_a_cash_diff, 2),
                        'reconciling_adjustments': reconciling_total,
                        'reconciling_fields': reconciling_fields,
                    })
                else:
                    result['failures'].append({
                        'period': period_end,
                        'duration': duration,
                        'equation': 'cash_start + net_cash_change = cash_end',
                        'expected': pattern_b_cash_expected,
                        'actual': cash_end,
                        'pct_diff': round(pattern_b_cash_diff, 2),
                    })

    elif have_activity_data:
        # Only have activity data - check both patterns for activity equation
        result['equations_checked'] += 1

        pattern_a_sum = cfo + cfi + cff
        pattern_a_passed, pattern_a_diff = check_within_tolerance(pattern_a_sum, net_cash_change, SEMANTIC_TOLERANCE_PCT)

        pattern_b_sum = cfo + cfi + cff + reconciling_total
        pattern_b_passed, pattern_b_diff = check_within_tolerance(pattern_b_sum, net_cash_change, SEMANTIC_TOLERANCE_PCT)

        if pattern_a_passed or pattern_b_passed:
            result['equations_passed'] += 1
        else:
            # Report the better failure
            if pattern_a_diff <= pattern_b_diff:
                result['failures'].append({
                    'period': period_end,
                    'duration': duration,
                    'equation': 'cfo + cfi + cff = net_cash_change',
                    'expected': pattern_a_sum,
                    'actual': net_cash_change,
                    'pct_diff': round(pattern_a_diff, 2),
                })
            else:
                if reconciling_fields:
                    equation_str = f'cfo + cfi + cff + ({", ".join(reconciling_fields)}) = net_cash_change'
                else:
                    equation_str = 'cfo + cfi + cff = net_cash_change'
                result['failures'].append({
                    'period': period_end,
                    'duration': duration,
                    'equation': equation_str,
                    'expected': pattern_b_sum,
                    'actual': net_cash_change,
                    'pct_diff': round(pattern_b_diff, 2),
                })

    elif have_cash_data:
        # Only have cash data - check both patterns for cash equation
        result['equations_checked'] += 1

        pattern_a_expected = cash_start + net_cash_change + reconciling_total
        pattern_a_passed, pattern_a_diff = check_within_tolerance(pattern_a_expected, cash_end, SEMANTIC_TOLERANCE_PCT)

        pattern_b_expected = cash_start + net_cash_change
        pattern_b_passed, pattern_b_diff = check_within_tolerance(pattern_b_expected, cash_end, SEMANTIC_TOLERANCE_PCT)

        if pattern_a_passed or pattern_b_passed:
            result['equations_passed'] += 1
        else:
            # Report the better failure
            if pattern_a_diff <= pattern_b_diff:
                if reconciling_fields:
                    equation_str = f'cash_start + net_cash_change + ({", ".join(reconciling_fields)}) = cash_end'
                else:
                    equation_str = 'cash_start + net_cash_change = cash_end'
                result['failures'].append({
                    'period': period_end,
                    'duration': duration,
                    'equation': equation_str,
                    'expected': pattern_a_expected,
                    'actual': cash_end,
                    'pct_diff': round(pattern_a_diff, 2),
                    'reconciling_adjustments': reconciling_total,
                    'reconciling_fields': reconciling_fields,
                })
            else:
                result['failures'].append({
                    'period': period_end,
                    'duration': duration,
                    'equation': 'cash_start + net_cash_change = cash_end',
                    'expected': pattern_b_expected,
                    'actual': cash_end,
                    'pct_diff': round(pattern_b_diff, 2),
                })

    if result['failures']:
        result['status'] = 'fail'

    return result


def check_period_arithmetic(ticker_data: dict) -> dict:
    """
    Check if Q1 + Q2 + Q3 + Q4 equals Annual for the same fiscal year.

    For CF items: cfo, cfi, cff, net_cash_change
    (Note: cash_start/cash_end are point-in-time, not flows)
    """
    result = {
        'check': 'period_arithmetic',
        'status': 'pass',
        'checks_performed': 0,
        'checks_passed': 0,
        'failures': [],
    }

    # Build deduplicated period lookup by (period_end, duration, consolidation)
    # Take the first occurrence (or we could prefer passing QC)
    period_lookup = {}  # (period_end, duration, consolidation) -> period data
    for period in ticker_data.get('periods', []):
        key = (period['period_end'], period['duration'], period['consolidation'])
        if key not in period_lookup:
            period_lookup[key] = period

    all_periods = list(period_lookup.values())

    # Find 12M (annual) periods - deduplicated by (period_end, consolidation)
    annual_seen = set()
    annual_periods = []
    for p in all_periods:
        if p['duration'] == '12M':
            key = (p['period_end'], p['consolidation'])
            if key not in annual_seen:
                annual_seen.add(key)
                annual_periods.append(p)

    for annual in annual_periods:
        fiscal_year_end = annual['period_end']
        consolidation = annual['consolidation']
        year = int(fiscal_year_end[:4])
        month = int(fiscal_year_end[5:7])

        # Find corresponding quarterly periods that sum to this annual
        # Quarters would be 3M periods ending in the 4 quarters of this fiscal year
        # For a Sep year-end: Q1 ends Dec (prior year), Q2 ends Mar, Q3 ends Jun, Q4 ends Sep
        # For a Dec year-end: Q1 ends Mar, Q2 ends Jun, Q3 ends Sep, Q4 ends Dec

        # Find 3M periods for the same consolidation (already deduplicated)
        quarters = [p for p in all_periods
                    if p['duration'] == '3M'
                    and p['consolidation'] == consolidation]

        # For CF, check flow items only
        flow_items = ['cfo', 'cfi', 'cff', 'net_cash_change']

        for item in flow_items:
            annual_val = get_value(annual, item)
            if annual_val is None:
                continue

            # Find quarters that could sum to this annual
            # Try to find 4 unique quarters in the same fiscal year
            # Deduplicate by period_end
            candidate_quarters = {}
            for q in quarters:
                q_date = q['period_end']
                q_year = int(q_date[:4])
                q_month = int(q_date[5:7])

                # Check if this quarter is within 12 months before the annual end
                # Simple heuristic: same year or year-1 for early quarters
                if q_year == year or (q_year == year - 1 and q_month > month):
                    q_val = get_value(q, item)
                    if q_val is not None and q_date not in candidate_quarters:
                        candidate_quarters[q_date] = q_val

            # If we have exactly 4 unique quarters, check sum
            if len(candidate_quarters) == 4:
                result['checks_performed'] += 1
                q_sum = sum(candidate_quarters.values())
                passed, pct_diff = check_within_tolerance(q_sum, annual_val, PERIOD_ARITHMETIC_TOLERANCE_PCT)

                if passed:
                    result['checks_passed'] += 1
                else:
                    result['failures'].append({
                        'fiscal_year_end': fiscal_year_end,
                        'consolidation': consolidation,
                        'item': item,
                        'quarters_sum': q_sum,
                        'annual_value': annual_val,
                        'pct_diff': round(pct_diff, 2),
                        'quarters': sorted(candidate_quarters.keys()),
                    })

    if result['failures']:
        result['status'] = 'fail'

    return result


def check_unit_type(filings: list) -> dict:
    """
    Check that all filings have valid unit_type.
    """
    result = {"passed": 0, "failed": 0, "issues": []}

    for filing in filings:
        unit_type = filing.get('unit_type', '').lower()

        if not unit_type:
            result["failed"] += 1
            result["issues"].append({
                "source_file": filing.get("source_file"),
                "issue": "missing",
                "message": f"Missing unit_type"
            })
        elif unit_type not in VALID_UNITS:
            result["failed"] += 1
            result["issues"].append({
                "source_file": filing.get("source_file"),
                "issue": "invalid",
                "unit_type": unit_type,
                "message": f"Invalid unit_type '{unit_type}'"
            })
        else:
            result["passed"] += 1

    return result


def check_critical_fields(filings: list) -> dict:
    """
    Check that critical fields are present.
    For CF: must have at least one field from each category (operating, investing, financing).
    """
    result = {"passed": 0, "failed": 0, "issues": []}

    for filing in filings:
        values = filing.get('values', {})
        field_names = set(values.keys())

        missing = []
        for category, fields in CRITICAL_FIELDS_CF.items():
            if not any(f in field_names for f in fields):
                missing.append(category)

        if not missing:
            result["passed"] += 1
        else:
            result["failed"] += 1
            result["issues"].append({
                "source_file": filing.get("source_file"),
                "missing_categories": missing,
                "message": f"Missing fields for: {', '.join(missing)}"
            })

    return result


def check_cross_period_normalization(filings: list) -> dict:
    """
    Check for 1000x outliers that indicate unit mismatch.
    Compares cfo (or net_cash_change) across filings.

    NOTE: This check uses NORMALIZED values (all converted to thousands) for comparison.
    This handles cases where different filings legitimately use different units
    (e.g., some in rupees, some in thousands). After normalization, legitimate
    variations are on the same scale and won't trigger false positives.
    """
    result = {"passed": 0, "failed": 0, "issues": []}

    # Reference field for comparison
    ref_fields = ['cfo', 'net_cash_change', 'cash_end']

    # Find first available reference field with enough data (using normalized values)
    ref_field = None
    for field in ref_fields:
        values = [abs(get_normalized_value(f, field)) for f in filings if get_normalized_value(f, field) is not None]
        if len(values) >= 3:
            ref_field = field
            break

    if not ref_field:
        result["passed"] = len(filings)
        return result

    # Get all NORMALIZED values for the reference field
    ref_values = []
    for f in filings:
        val = get_normalized_value(f, ref_field)
        if val is not None and val != 0:
            ref_values.append((f, abs(val)))

    if len(ref_values) < 3:
        result["passed"] = len(filings)
        return result

    # Calculate median of normalized values
    sorted_values = sorted(v for _, v in ref_values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 0:
        median = (sorted_values[mid - 1] + sorted_values[mid]) / 2
    else:
        median = sorted_values[mid]

    if median == 0:
        result["passed"] = len(filings)
        return result

    # Check each filing against median (using normalized values)
    for filing, val in ref_values:
        ratio = val / median

        if ratio > CROSS_PERIOD_THRESHOLD or ratio < (1 / CROSS_PERIOD_THRESHOLD):
            # Get raw value for reporting (more meaningful to user)
            raw_val = filing.get('values', {}).get(ref_field)
            raw_val = abs(raw_val) if raw_val else val
            result["failed"] += 1
            result["issues"].append({
                "source_file": filing.get("source_file"),
                "ref_field": ref_field,
                "value": raw_val,  # Report raw value
                "normalized_value": val,  # Also include normalized for debugging
                "median": median,
                "ratio": round(ratio, 1),
                "message": f"{ref_field}={raw_val:,.0f} (normalized: {val:,.0f}) is {ratio:.0f}x median ({median:,.0f}) - likely unit error"
            })
        else:
            result["passed"] += 1

    return result


def diagnose_unit_context(ticker: str) -> dict:
    """
    Get unit type info for a ticker from extraction files.

    Returns diagnostic info to help investigate failures.
    This helps identify if cross_period_normalization failures might be caused
    by unit declaration mismatches (e.g., one file says millions, others say thousands).

    Returns:
        {
            'has_variation': bool,
            'unit_counts': {'thousands': 10, 'millions': 1},
            'majority_unit': 'thousands',
            'outlier_files': ['TICKER_annual_2024_consolidated.md']
        }
    """
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


def validate_ticker(ticker_data: dict, verbose: bool = False) -> dict:
    """Validate all periods for a ticker."""
    ticker = ticker_data['ticker']

    # Get periods (flat list from Step5_JSONifyCF)
    all_periods = ticker_data.get('periods', [])
    # Filter out skipped periods based on source_file
    periods_to_check = [p for p in all_periods if not should_skip_filing(ticker, p)]
    skipped_count = len(all_periods) - len(periods_to_check)

    result = {
        'ticker': ticker,
        'filings_count': len(periods_to_check),
        'skipped_filings': skipped_count,
        'checks': {
            'completeness': {'passed': 0, 'failed': 0, 'warnings': 0},
            'semantic': {'passed': 0, 'failed': 0},
            'unit_type': None,
            'critical_fields': None,
            'cross_period_normalization': None,
            'unit_context': None,  # Diagnostic: unit type variation across extraction files
        },
        'period_arithmetic': None,
        'filing_results': [],
    }

    # Run semantic equation check on each period
    for period in periods_to_check:
        period_result = {
            'source_file': period['source_file'],
            'qc_status': period.get('source_qc_status', 'unknown'),
            'period_end': period['period_end'],
            'checks': [],
        }

        # Check semantic equations on this period
        semantic = check_semantic_equations_period(period)
        period_result['checks'].append(semantic)
        if semantic['status'] == 'pass':
            result['checks']['semantic']['passed'] += 1
        else:
            result['checks']['semantic']['failed'] += 1

        result['filing_results'].append(period_result)

    # Run period arithmetic check at ticker level
    filtered_ticker_data = {**ticker_data, 'periods': periods_to_check}
    period_arith = check_period_arithmetic(filtered_ticker_data)
    result['period_arithmetic'] = period_arith

    # Run ticker-level checks on periods
    # 4. Unit type check
    result['checks']['unit_type'] = check_unit_type(periods_to_check)

    # 5. Critical fields check
    result['checks']['critical_fields'] = check_critical_fields(periods_to_check)

    # 6. Cross-period normalization (1000x outlier detection)
    result['checks']['cross_period_normalization'] = check_cross_period_normalization(periods_to_check)

    # 7. Unit context diagnostic (detect unit_type variation across extraction files)
    result['checks']['unit_context'] = diagnose_unit_context(ticker)

    return result


def main():
    parser = argparse.ArgumentParser(description="QC2 Cash Flow - systematic checks on V2 JSON")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: QC CASH FLOW")
    print("=" * 70)

    if not INPUT_DIR.exists():
        print(f"ERROR: Input directory not found: {INPUT_DIR}")
        print(f"Run Step5_JSONifyCF_v2.py first to create the JSON files.")
        return

    files = sorted(INPUT_DIR.glob("*.json"))
    if args.ticker:
        files = [f for f in files if f.stem == args.ticker]

    if not files:
        print(f"ERROR: No JSON files found in {INPUT_DIR}")
        return

    print(f"Found {len(files)} ticker files to validate\n")

    # Validate all tickers
    all_results = []
    stats = defaultdict(int)

    for filepath in files:
        with open(filepath) as f:
            ticker_data = json.load(f)

        result = validate_ticker(ticker_data, args.verbose)
        all_results.append(result)

        stats['tickers'] += 1
        stats['filings'] += result['filings_count']

        # Count overall status
        completeness_ok = result['checks']['completeness']['failed'] == 0
        semantic_ok = result['checks']['semantic']['failed'] == 0
        period_arith_ok = result['period_arithmetic']['status'] == 'pass' if result['period_arithmetic'] else True

        all_ok = completeness_ok and semantic_ok and period_arith_ok
        if all_ok:
            stats['tickers_passed'] += 1
        else:
            stats['tickers_failed'] += 1

        stats['completeness_passed'] += result['checks']['completeness']['passed']
        stats['completeness_failed'] += result['checks']['completeness']['failed']
        stats['completeness_warnings'] += result['checks']['completeness']['warnings']
        stats['semantic_passed'] += result['checks']['semantic']['passed']
        stats['semantic_failed'] += result['checks']['semantic']['failed']

        if result['period_arithmetic']:
            stats['period_arith_checks'] += result['period_arithmetic']['checks_performed']
            stats['period_arith_passed'] += result['period_arithmetic']['checks_passed']

        # Track unit context variations
        unit_ctx = result['checks'].get('unit_context')
        if unit_ctx and unit_ctx.get('has_variation'):
            stats['unit_context_variations'] += 1

        # Print ticker result
        if args.verbose or not all_ok:
            status_str = "PASS" if all_ok else "FAIL"
            print(f"{status_str}: {result['ticker']}")

            # Show failures
            if not semantic_ok:
                for fr in result['filing_results']:
                    for check in fr['checks']:
                        if check['check'] == 'semantic_equations' and check['failures']:
                            for fail in check['failures'][:2]:
                                print(f"    Semantic: {fail['equation']} in {fail['period']}")
                                print(f"      Expected: {fail['expected']:,.0f}, Actual: {fail['actual']:,.0f} ({fail['pct_diff']:.1f}% diff)")

            if result['period_arithmetic'] and result['period_arithmetic']['failures']:
                for fail in result['period_arithmetic']['failures'][:2]:
                    print(f"    Period arithmetic: {fail['item']} for FY ending {fail['fiscal_year_end']}")
                    print(f"      Quarters sum: {fail['quarters_sum']:,.0f}, Annual: {fail['annual_value']:,.0f} ({fail['pct_diff']:.1f}% diff)")

            print()

    # Save results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump({
            'stats': dict(stats),
            'results': all_results,
        }, f, indent=2)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Tickers processed:     {stats['tickers']}")
    print(f"  Tickers passed:        {stats['tickers_passed']}")
    print(f"  Tickers failed:        {stats['tickers_failed']}")
    print(f"  Total filings:         {stats['filings']}")
    print()
    print("COMPLETENESS CHECK:")
    print(f"  Passed:                {stats['completeness_passed']}")
    print(f"  Failed:                {stats['completeness_failed']}")
    print(f"  Warnings:              {stats['completeness_warnings']}")
    print()
    print("SEMANTIC EQUATIONS:")
    print(f"  Passed:                {stats['semantic_passed']}")
    print(f"  Failed:                {stats['semantic_failed']}")
    if stats['semantic_passed'] + stats['semantic_failed'] > 0:
        rate = stats['semantic_passed'] / (stats['semantic_passed'] + stats['semantic_failed']) * 100
        print(f"  Pass rate:             {rate:.1f}%")
    print()
    print("PERIOD ARITHMETIC:")
    print(f"  Checks performed:      {stats['period_arith_checks']}")
    print(f"  Passed:                {stats['period_arith_passed']}")
    if stats['period_arith_checks'] > 0:
        rate = stats['period_arith_passed'] / stats['period_arith_checks'] * 100
        print(f"  Pass rate:             {rate:.1f}%")
    print()
    print("UNIT CONTEXT:")
    print(f"  Variations detected:   {stats['unit_context_variations']}")

    ticker_pass_rate = stats['tickers_passed'] / stats['tickers'] * 100 if stats['tickers'] > 0 else 0
    print(f"\n  Overall ticker pass rate: {ticker_pass_rate:.1f}%")

    # Show tickers with unit context variation
    unit_var_tickers = [(r['ticker'], r['checks']['unit_context'])
                        for r in all_results
                        if r['checks'].get('unit_context') and r['checks']['unit_context'].get('has_variation')]
    if unit_var_tickers:
        print()
        print("UNIT CONTEXT VARIATIONS:")
        for ticker, ctx in unit_var_tickers[:10]:
            print(f"  {ticker}: {ctx['unit_counts']} - outliers: {ctx['outlier_files'][:3]}")

    print()
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
