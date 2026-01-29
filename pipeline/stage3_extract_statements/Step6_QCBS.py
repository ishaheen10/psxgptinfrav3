#!/usr/bin/env python3
"""
Step 6: QC Balance Sheet

Runs systematic QC checks on the JSON-formatted BS data.

QC checks implemented:
1. Column completeness - Check if we have expected periods
2. Accounting equation - Verify Assets = Equity + Liabilities
3. Unit type validation - Check for valid unit types
4. Critical fields check - Verify required fields present
5. Cross-period normalization - Detect 1000x outliers

NOTE: Ref formula validation is done in Step4_QCBS_Extraction.py
NOTE: Sub-equation checks removed - redundant with ref formula check
NOTE: Source value matching is done in Step4_QCBS_Extraction.py

Input:  data/json_bs/{TICKER}.json
Output: artifacts/stage3/step6_qc_bs_results.json

Usage:
    python3 Step6_QCBS.py                 # Process all
    python3 Step6_QCBS.py --ticker LUCK   # Single ticker
    python3 Step6_QCBS.py --verbose       # Show details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "json_bs"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step6_qc_bs_results.json"
EXTRACTION_DIR = PROJECT_ROOT / "data" / "extracted_bs"
ALLOWLIST_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "bs_component_mismatch_allowlist.json"
AE_ALLOWLIST_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_arithmetic_allowlist_bs.json"

# Tolerance for accounting equation check
ACCOUNTING_EQUATION_TOLERANCE = 0.05  # 5%

# Component mismatch allowlist - loaded at module init
# Format: {(ticker, period, consolidation): reason}
COMPONENT_MISMATCH_ALLOWLIST = {}

# Accounting equation allowlist - for known AE failures
# Format: {(ticker, period_end, consolidation): reason}
AE_ALLOWLIST = {}


def load_component_mismatch_allowlist():
    """Load allowlist for valid BS presentations where components don't sum to total."""
    global COMPONENT_MISMATCH_ALLOWLIST
    if not ALLOWLIST_FILE.exists():
        return

    try:
        with open(ALLOWLIST_FILE) as f:
            data = json.load(f)

        for item in data.get('allowlist', []):
            ticker = item['ticker']
            consolidation = item['consolidation']
            reason = item.get('reason', 'Allowlisted')
            pattern = item.get('pattern', 'unknown')

            # Each item can have multiple periods
            for period in item.get('periods', []):
                key = (ticker, period, consolidation)
                COMPONENT_MISMATCH_ALLOWLIST[key] = {
                    'reason': reason,
                    'pattern': pattern
                }
    except Exception as e:
        print(f"Warning: Could not load allowlist: {e}")


def load_ae_allowlist():
    """Load allowlist for accounting equation failures that have been manually reviewed."""
    global AE_ALLOWLIST
    if not AE_ALLOWLIST_FILE.exists():
        return

    try:
        with open(AE_ALLOWLIST_FILE) as f:
            data = json.load(f)

        for item in data.get('allowlist', []):
            ticker = item['ticker']
            period = item['period']
            consolidation = item['consolidation']
            reason = item.get('reason', 'Allowlisted')

            # Convert period format if needed (annual_2023 -> 2023-12-31)
            if period.startswith('annual_'):
                year = period.replace('annual_', '')
                period_end = f"{year}-12-31"
            elif period.startswith('quarterly_'):
                period_end = period.replace('quarterly_', '')
            else:
                period_end = period

            key = (ticker, period_end, consolidation)
            AE_ALLOWLIST[key] = reason
    except Exception as e:
        print(f"Warning: Could not load AE allowlist: {e}")


# Load allowlists at module init
load_component_mismatch_allowlist()
load_ae_allowlist()

# Valid unit types
VALID_UNITS = {'thousands', 'millions', 'rupees', 'full_rupees'}

# Critical fields for Balance Sheet
# Must have total_assets AND (total_equity_and_liabilities OR both total_equity and total_liabilities)
CRITICAL_FIELDS_BS = {
    'assets': ['total_assets'],
    'liabilities_equity': ['total_equity_and_liabilities', 'total_liabilities', 'total_equity'],
}

# Cross-period normalization: flag if value is >100x or <0.01x median
CROSS_PERIOD_THRESHOLD = 100

# Skip list: known problematic filings or acceptable anomalies
# Format: {ticker: [filing_patterns]} where pattern matches source_file
SKIP_FILINGS = {
    # OCR corruption - values shifted between rows
    'EFERT': ['annual_2021'],
    'SHEL': ['annual_2023'],  # BS and P&L combined on same page
    # Source quality issues - OCR errors
    'AGHA': ['quarterly_2025-09-30'],  # OCR row misalignment - "-" not captured causing values to shift
    'MSCL': ['quarterly_2024-12-31'],  # CamScanner OCR digit errors - "120,609" read as "110,609"
    'TPLP': ['annual_2022'],  # OCR digit recognition - "7,748M" read as "5,348M" + possible PDF typo
    # Source quality issues - PDF arithmetic errors (original filing has wrong subtotals)
    'AGP': ['annual_2022'],  # Non-current liabilities subtotal off by 10,000
    'HUBC': ['quarterly_2024-09-30'],  # Current assets subtotal off by ~10M for Jun 2024
    'MUREB': ['quarterly_2023-03-31'],  # "Current portion of long term loan" not included in subtotal
    # Structural issues - insurance company special format
    'AICL': ['quarterly_2022-03-31'],  # "Insurance liabilities" is separate major category, not in subtotal
}


def should_skip_period(ticker: str, period: dict) -> bool:
    """Check if a period should be skipped based on SKIP_FILINGS list."""
    if ticker not in SKIP_FILINGS:
        return False
    source_file = period.get('source_file', '')
    filing_period = period.get('filing_period', '')
    for pattern in SKIP_FILINGS[ticker]:
        if pattern in source_file or pattern in filing_period:
            return True
    return False


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


def get_normalized_value(period: dict, canonical: str) -> float | None:
    """
    Get a value from a period, normalized to thousands for comparison.

    This is used for cross-period comparison checks where we need values
    on the same scale regardless of the source unit_type.
    """
    raw_value = period.get('values', {}).get(canonical)
    if raw_value is None:
        return None

    unit_type = period.get('unit_type', 'thousands')
    return normalize_value_to_thousands(raw_value, unit_type)


def check_column_completeness(ticker: str, periods: list) -> dict:
    """
    Check if we have all expected periods.

    For quarterly filings: should have current period + prior period
    For annual: should have current year-end + prior year-end

    Returns dict with check results.
    """
    result = {
        "status": "pass",
        "issues": [],
        "stats": {
            "total_periods": len(periods),
            "consolidated": 0,
            "unconsolidated": 0,
            "annual": 0,
            "quarterly": 0
        }
    }

    # Group periods by consolidation type and filing type
    by_consolidation = defaultdict(list)
    by_filing_type = defaultdict(list)

    for period in periods:
        by_consolidation[period["consolidation"]].append(period)
        by_filing_type[period["filing_type"]].append(period)
        result["stats"][period["consolidation"]] += 1
        result["stats"][period["filing_type"]] += 1

    # Check each filing has at least current + prior period columns
    filings_seen = set()
    for period in periods:
        filing_key = (period["filing_period"], period["consolidation"])
        if filing_key in filings_seen:
            continue
        filings_seen.add(filing_key)

        # Count how many date columns we have for this filing
        same_filing = [p for p in periods
                       if p["filing_period"] == period["filing_period"]
                       and p["consolidation"] == period["consolidation"]]

        # For annual, should have at least 1 period (current year)
        # For quarterly, should have at least 1 period (current quarter)
        # Note: Comparative periods may be duplicated from other filings
        if len(same_filing) < 1:
            result["issues"].append({
                "type": "missing_periods",
                "filing_period": period["filing_period"],
                "consolidation": period["consolidation"],
                "found": len(same_filing),
                "expected": "at least 1"
            })

    if result["issues"]:
        result["status"] = "warn"

    return result


def check_accounting_equation(period: dict, ticker: str = None) -> dict:
    """
    Check if the accounting equation holds: Assets = Equity + Liabilities

    Three checks:
    1. total_assets = total_equity_and_liabilities (direct comparison)
    2. total_assets = total_equity + total_liabilities (component sum)
    3. total_equity_and_liabilities = total_equity + total_liabilities (consistency check)

    Args:
        period: Period data dict
        ticker: Ticker symbol (used for allowlist lookup)

    Returns dict with check results.
    """
    result = {
        "status": "pass",
        "method": None,
        "values": {},
        "diff_pct": None,
        "tolerance": ACCOUNTING_EQUATION_TOLERANCE
    }

    values = period["values"]
    total_assets = values.get("total_assets")
    total_eq_liab = values.get("total_equity_and_liabilities")
    total_equity = values.get("total_equity")
    total_liabilities = values.get("total_liabilities")

    result["values"] = {
        "total_assets": total_assets,
        "total_equity_and_liabilities": total_eq_liab,
        "total_equity": total_equity,
        "total_liabilities": total_liabilities
    }

    # Check AE allowlist - if this period is allowlisted, skip the check
    period_end = period.get("period_end", "")
    consolidation = period.get("consolidation", "")
    ae_allowlist_key = (ticker, period_end, consolidation)
    if ae_allowlist_key in AE_ALLOWLIST:
        result["status"] = "pass"
        result["allowlisted"] = True
        result["allowlist_reason"] = AE_ALLOWLIST[ae_allowlist_key]
        return result

    if total_assets is None or total_assets == 0:
        result["status"] = "skip"
        result["reason"] = "missing total_assets"
        return result

    # Method 1: Compare total_assets vs total_equity_and_liabilities
    if total_eq_liab is not None and total_eq_liab != 0:
        result["method"] = "assets_vs_eq_liab"
        diff = abs(total_assets - total_eq_liab)
        diff_pct = diff / abs(total_assets)
        result["diff_pct"] = round(diff_pct * 100, 2)

        if diff_pct > ACCOUNTING_EQUATION_TOLERANCE:
            result["status"] = "fail"
            result["expected"] = total_assets
            result["actual"] = total_eq_liab
            return result

        # Additional check: verify total_equity_and_liabilities = total_equity + total_liabilities
        # This catches cases where stated total is correct but components are wrong
        if total_equity is not None and total_liabilities is not None:
            eq_plus_liab = total_equity + total_liabilities
            component_diff = abs(total_eq_liab - eq_plus_liab)
            component_diff_pct = component_diff / abs(total_eq_liab) if total_eq_liab != 0 else 0

            if component_diff_pct > ACCOUNTING_EQUATION_TOLERANCE:
                # Check if this is an allowlisted valid presentation
                period_end = period.get("period_end", "")
                consolidation = period.get("consolidation", "")
                allowlist_key = (ticker, period_end, consolidation)

                if allowlist_key in COMPONENT_MISMATCH_ALLOWLIST:
                    # Valid presentation pattern - skip this check
                    allowlist_info = COMPONENT_MISMATCH_ALLOWLIST[allowlist_key]
                    result["status"] = "pass"
                    result["allowlisted"] = True
                    result["allowlist_reason"] = allowlist_info['reason']
                    result["allowlist_pattern"] = allowlist_info['pattern']
                    return result

                result["status"] = "fail"
                result["method"] = "eq_liab_vs_components"
                result["diff_pct"] = round(component_diff_pct * 100, 2)
                result["expected"] = total_eq_liab
                result["actual"] = eq_plus_liab
                result["components"] = {
                    "total_equity": total_equity,
                    "total_liabilities": total_liabilities
                }
                result["issue"] = "stated total_equity_and_liabilities does not match total_equity + total_liabilities"
                return result

        # All checks passed
        result["status"] = "pass"
        return result

    # Method 2: Compare total_assets vs (equity + liabilities)
    if total_equity is not None and total_liabilities is not None:
        result["method"] = "assets_vs_eq_plus_liab"
        eq_plus_liab = total_equity + total_liabilities
        diff = abs(total_assets - eq_plus_liab)
        diff_pct = diff / abs(total_assets) if total_assets != 0 else 0
        result["diff_pct"] = round(diff_pct * 100, 2)

        if diff_pct <= ACCOUNTING_EQUATION_TOLERANCE:
            result["status"] = "pass"
        else:
            result["status"] = "fail"
            result["expected"] = total_assets
            result["actual"] = eq_plus_liab
            result["components"] = {
                "total_equity": total_equity,
                "total_liabilities": total_liabilities
            }
        return result

    # Cannot verify - missing fields
    result["status"] = "skip"
    result["reason"] = "missing equity/liabilities fields"
    return result


def check_unit_type(periods: list) -> dict:
    """
    Check that all periods have valid unit_type.
    """
    result = {"pass": 0, "fail": 0, "issues": []}

    for period in periods:
        unit_type = period.get('unit_type', '').lower()

        if not unit_type:
            result["fail"] += 1
            result["issues"].append({
                "period_end": period.get("period_end"),
                "consolidation": period.get("consolidation"),
                "issue": "missing",
                "message": f"Missing unit_type in {period.get('source_file', 'unknown')}"
            })
        elif unit_type not in VALID_UNITS:
            result["fail"] += 1
            result["issues"].append({
                "period_end": period.get("period_end"),
                "consolidation": period.get("consolidation"),
                "issue": "invalid",
                "unit_type": unit_type,
                "message": f"Invalid unit_type '{unit_type}'"
            })
        else:
            result["pass"] += 1

    return result


def check_critical_fields(periods: list) -> dict:
    """
    Check that critical fields are present.
    For BS: must have total_assets AND at least one of total_equity_and_liabilities, total_equity, total_liabilities.
    """
    result = {"pass": 0, "fail": 0, "issues": []}

    for period in periods:
        values = period.get('values', {})
        field_names = set(values.keys())

        # Check for total_assets
        has_assets = 'total_assets' in field_names

        # Check for liabilities+equity side
        has_liab_eq = any(f in field_names for f in CRITICAL_FIELDS_BS['liabilities_equity'])

        if has_assets and has_liab_eq:
            result["pass"] += 1
        else:
            result["fail"] += 1
            missing = []
            if not has_assets:
                missing.append("total_assets")
            if not has_liab_eq:
                missing.append("total_equity_and_liabilities or total_equity/total_liabilities")
            result["issues"].append({
                "period_end": period.get("period_end"),
                "consolidation": period.get("consolidation"),
                "filing_period": period.get("filing_period"),
                "missing": missing,
                "message": f"Missing critical fields: {', '.join(missing)}"
            })

    return result


def check_cross_period_normalization(periods: list) -> dict:
    """
    Check for 1000x outliers that indicate unit mismatch.
    Compares total_assets across periods within same consolidation.

    NOTE: This check uses NORMALIZED values (all converted to thousands) for comparison.
    This handles cases where different filings legitimately use different units
    (e.g., some in rupees, some in thousands). After normalization, legitimate
    variations are on the same scale and won't trigger false positives.
    """
    result = {"pass": 0, "fail": 0, "issues": []}

    # Group by consolidation
    by_consolidation = defaultdict(list)
    for p in periods:
        by_consolidation[p.get('consolidation', 'unknown')].append(p)

    ref_field = 'total_assets'

    for consolidation, cons_periods in by_consolidation.items():
        # Get all NORMALIZED values for total_assets
        ref_values = []
        for p in cons_periods:
            val = get_normalized_value(p, ref_field)
            if val is not None and val != 0:
                ref_values.append((p, abs(val)))

        if len(ref_values) < 3:
            # Not enough data points to check
            result["pass"] += len(ref_values)
            continue

        # Calculate median of normalized values
        sorted_values = sorted(v for _, v in ref_values)
        mid = len(sorted_values) // 2
        if len(sorted_values) % 2 == 0:
            median = (sorted_values[mid - 1] + sorted_values[mid]) / 2
        else:
            median = sorted_values[mid]

        if median == 0:
            result["pass"] += len(ref_values)
            continue

        # Check each period against median (using normalized values)
        for period, val in ref_values:
            ratio = val / median

            if ratio > CROSS_PERIOD_THRESHOLD or ratio < (1 / CROSS_PERIOD_THRESHOLD):
                # Get raw value for reporting (more meaningful to user)
                raw_val = period.get('values', {}).get(ref_field)
                raw_val = abs(raw_val) if raw_val else val
                result["fail"] += 1
                result["issues"].append({
                    "period_end": period.get("period_end"),
                    "consolidation": consolidation,
                    "filing_period": period.get("filing_period"),
                    "ref_field": ref_field,
                    "value": raw_val,  # Report raw value
                    "normalized_value": val,  # Also include normalized for debugging
                    "median": median,
                    "ratio": round(ratio, 1),
                    "message": f"{ref_field}={raw_val:,.0f} (normalized: {val:,.0f}) is {ratio:.0f}x median ({median:,.0f}) - likely unit error"
                })
            else:
                result["pass"] += 1

    return result


def diagnose_unit_context(ticker: str) -> dict:
    """
    Get unit type info for a ticker from extraction files.

    Returns diagnostic info to help investigate failures.
    This helps identify if cross_period_normalization or source_matching failures
    might be caused by unit declaration mismatches (e.g., one file says millions,
    others say thousands).

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


def qc_ticker(ticker: str, data: dict, verbose: bool = False) -> dict:
    """
    Run all QC checks for a ticker.
    """
    # Filter out skipped periods
    all_periods = data["periods"]
    periods = [p for p in all_periods if not should_skip_period(ticker, p)]
    skipped_count = len(all_periods) - len(periods)

    result = {
        "ticker": ticker,
        "total_periods": len(periods),
        "skipped_periods": skipped_count,
        "checks": {
            "completeness": None,
            "accounting_equation": {
                "pass": 0,
                "fail": 0,
                "skip": 0,
                "failures": []
            },
            # NOTE: ref_formulas removed - validated in Step4_QCBS_Extraction.py
            # NOTE: sub_equations removed - redundant with ref formula check (less info)
            # NOTE: source_matching removed - validated in Step4_QCBS_Extraction.py
            "unit_type": None,
            "critical_fields": None,
            "cross_period_normalization": None,
            "unit_context": None,  # Diagnostic: unit type variation across extraction files
        },
        "overall_status": "pass"
    }

    # 1. Column completeness check
    completeness = check_column_completeness(ticker, periods)
    result["checks"]["completeness"] = completeness

    # 2. Accounting equation check (per period)
    for period in periods:
        eq_result = check_accounting_equation(period, ticker=ticker)
        status = eq_result["status"]
        result["checks"]["accounting_equation"][status] += 1

        if status == "fail":
            result["checks"]["accounting_equation"]["failures"].append({
                "period_end": period["period_end"],
                "consolidation": period["consolidation"],
                "filing_period": period["filing_period"],
                "details": eq_result
            })
            result["overall_status"] = "fail"

    # NOTE: Ref formula validation removed - it's done in Step4_QCBS_Extraction.py
    # Ref letters are only consistent within a single extraction file, not across periods.
    # Step4 validates formulas on the extraction markdown where refs are defined.

    # NOTE: Source matching removed - it's done in Step4_QCBS_Extraction.py
    # Source matching checks values against source markdown pages.

    # 3. Unit type check
    unit_result = check_unit_type(periods)
    result["checks"]["unit_type"] = unit_result
    if unit_result["fail"] > 0:
        result["overall_status"] = "fail"

    # 5. Critical fields check
    critical_result = check_critical_fields(periods)
    result["checks"]["critical_fields"] = critical_result
    if critical_result["fail"] > 0 and result["overall_status"] == "pass":
        result["overall_status"] = "fail"

    # 6. Cross-period normalization (1000x outlier detection)
    cross_period_result = check_cross_period_normalization(periods)
    result["checks"]["cross_period_normalization"] = cross_period_result
    if cross_period_result["fail"] > 0 and result["overall_status"] == "pass":
        result["overall_status"] = "fail"

    # 7. Unit context diagnostic (detect unit_type variation across extraction files)
    unit_diag = diagnose_unit_context(ticker)
    result["checks"]["unit_context"] = unit_diag
    if unit_diag['has_variation'] and result["overall_status"] == "pass":
        result["overall_status"] = "warn"

    # Update overall status based on completeness
    if completeness["status"] != "pass" and result["overall_status"] == "pass":
        result["overall_status"] = "warn"

    return result


def main():
    parser = argparse.ArgumentParser(description="QC2 for Balance Sheet")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show details")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: QC BALANCE SHEET")
    print("=" * 70)
    print()

    # Check input directory exists
    if not INPUT_DIR.exists():
        print(f"ERROR: Input directory does not exist: {INPUT_DIR}")
        print("Run Step4_JSONifyBS.py first to create the JSON files.")
        return

    # Get input files
    files = list(INPUT_DIR.glob("*.json"))
    if args.ticker:
        files = [f for f in files if f.stem == args.ticker]

    if not files:
        print(f"No JSON files found in {INPUT_DIR}")
        return

    print(f"Input files: {len(files)}")
    print()

    # Run QC on each ticker
    all_results = {
        "summary": {
            "total_tickers": 0,
            "pass": 0,
            "warn": 0,
            "fail": 0,
            "accounting_equation_failures": 0,
            "unit_context_variations": 0
        },
        "tickers": {}
    }

    for filepath in sorted(files):
        ticker = filepath.stem

        with open(filepath) as f:
            data = json.load(f)

        qc_result = qc_ticker(ticker, data, verbose=args.verbose)
        all_results["tickers"][ticker] = qc_result
        all_results["summary"]["total_tickers"] += 1

        # Update summary
        status = qc_result["overall_status"]
        all_results["summary"][status] += 1

        eq_failures = len(qc_result["checks"]["accounting_equation"]["failures"])
        all_results["summary"]["accounting_equation_failures"] += eq_failures

        unit_ctx = qc_result["checks"]["unit_context"]
        if unit_ctx and unit_ctx.get("has_variation"):
            all_results["summary"]["unit_context_variations"] += 1

        if args.verbose:
            eq_check = qc_result["checks"]["accounting_equation"]
            print(f"{ticker}: {status.upper()} - "
                  f"eq_pass={eq_check['pass']}, eq_fail={eq_check['fail']}, "
                  f"periods={qc_result['total_periods']}")

    # Write results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(all_results, f, indent=2)

    # Print summary
    print()
    print("=" * 70)
    print("QC2 BS SUMMARY")
    print("=" * 70)
    summary = all_results["summary"]
    print(f"Total tickers:                    {summary['total_tickers']}")
    print(f"  Pass:                           {summary['pass']}")
    print(f"  Warn:                           {summary['warn']}")
    print(f"  Fail:                           {summary['fail']}")
    print()
    print(f"Accounting equation failures:     {summary['accounting_equation_failures']}")
    print(f"Unit context variations:          {summary['unit_context_variations']}")
    print()
    print(f"Output: {OUTPUT_FILE}")

    # Show failed tickers
    failed_tickers = [t for t, r in all_results["tickers"].items() if r["overall_status"] == "fail"]
    if failed_tickers and len(failed_tickers) <= 20:
        print()
        print("FAILED TICKERS:")
        for ticker in failed_tickers[:20]:
            result = all_results["tickers"][ticker]
            failures = result["checks"]["accounting_equation"]["failures"]
            for f in failures[:3]:
                print(f"  {ticker}: {f['period_end']} ({f['consolidation']}) - "
                      f"diff={f['details'].get('diff_pct', 'N/A')}%")

    # Show tickers with unit context variation
    unit_var_tickers = [t for t, r in all_results["tickers"].items()
                        if r["checks"]["unit_context"] and r["checks"]["unit_context"].get("has_variation")]
    if unit_var_tickers:
        print()
        print("UNIT CONTEXT VARIATIONS:")
        for ticker in unit_var_tickers[:10]:
            ctx = all_results["tickers"][ticker]["checks"]["unit_context"]
            print(f"  {ticker}: {ctx['unit_counts']} - outliers: {ctx['outlier_files'][:3]}")


if __name__ == "__main__":
    main()
