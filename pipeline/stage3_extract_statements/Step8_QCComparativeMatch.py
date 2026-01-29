#!/usr/bin/env python3
"""
Step 8: Cross-period comparative column checker.

For each extracted markdown file, parses both the current-period and comparative
(prior-period) columns. Then checks whether the comparative values match the
current-period values from the standalone file for that prior period.

Detects and categorizes:
- OCR_ERROR: Note reference captured as value (small value like 6, 10, 11 vs large value)
- UNIT_MISMATCH: ~1000x ratio difference (millions vs thousands confusion)
- WRONG_PAGE: Multi-year summary page used instead of actual statement
- EXTRACTION_ERROR: Other significant differences

Filters out false positives:
- Dual-currency files (USD vs PKR columns)
- Period duration mismatches (6M vs 3M)
- Normal restatements (within tolerance)

Usage:
    python3 pipeline/stage3_extract_statements/Step8_QCComparativeMatch.py
"""

import os
import re
import glob
import json
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2,
    "mar": 3, "march": 3, "apr": 4, "april": 4,
    "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# Tolerance thresholds
NORMAL_TOLERANCE = 0.10  # 10% - skip entirely, considered normal restatement
CRITICAL_TOLERANCE = 0.15  # 15% for critical fields to flag

# Critical fields that should match closely
CRITICAL_FIELDS = {
    'total_assets', 'total_equity', 'total_liabilities', 'total_equity_and_liabilities',
    'net_profit', 'revenue_net', 'profit_before_tax', 'cfo', 'cash_end', 'cash_start'
}

# Thresholds for issue categorization
NOTE_REF_MAX_VALUE = 50  # Values <= this that differ by >1000x are likely note references
UNIT_RATIO_MIN = 500  # Ratio > this suggests unit mismatch (thousands vs millions)
UNIT_RATIO_MAX = 2000  # Ratio in this range is classic 1000x unit issue


def categorize_issue(comp_val, stand_val, field):
    """
    Categorize the type of mismatch based on value patterns.
    Returns: (category, confidence, description)
    """
    if stand_val == 0 or comp_val == 0:
        return ("ZERO_VALUE", 0.5, "One value is zero")

    ratio = abs(comp_val / stand_val)
    inv_ratio = abs(stand_val / comp_val)
    max_ratio = max(ratio, inv_ratio)

    # Pattern 1: Note reference as value
    # One value is tiny (likely note ref like 6, 10, 11) while other is large
    small_val = min(abs(comp_val), abs(stand_val))
    large_val = max(abs(comp_val), abs(stand_val))

    if small_val <= NOTE_REF_MAX_VALUE and large_val > 10000 and max_ratio > 1000:
        return ("OCR_ERROR", 0.95, f"Note reference '{int(small_val)}' captured as value instead of actual number")

    # Pattern 2: Unit mismatch (~1000x difference)
    if UNIT_RATIO_MIN < max_ratio < UNIT_RATIO_MAX:
        return ("UNIT_MISMATCH", 0.85, f"~{int(max_ratio)}x difference suggests millions vs thousands confusion")

    # Pattern 3: Very large ratio (could be wrong page or major extraction error)
    if max_ratio > UNIT_RATIO_MAX:
        if max_ratio > 10000:
            return ("OCR_ERROR", 0.80, f"Extreme {int(max_ratio)}x difference - likely OCR error or note reference")
        return ("WRONG_PAGE", 0.70, f"Large {int(max_ratio)}x difference - possible wrong page or multi-year summary")

    # Pattern 4: Sign difference (one positive, one negative)
    # Only flag if magnitude also differs significantly (>10x) - otherwise it's a normal restatement
    if (comp_val > 0 and stand_val < 0) or (comp_val < 0 and stand_val > 0):
        if max_ratio > 10:
            return ("SIGN_ERROR", 0.75, f"Sign mismatch with {int(max_ratio)}x magnitude diff - likely OCR or extraction error")
        else:
            return ("RESTATEMENT", 0.30, "Sign flip with similar magnitude - normal restatement")

    # Pattern 5: Moderate difference
    if max_ratio > 2:
        return ("EXTRACTION_ERROR", 0.60, f"{int((max_ratio-1)*100)}% difference - needs investigation")

    return ("RESTATEMENT", 0.40, "Likely normal restatement or rounding")


def parse_date_from_header(header_text):
    """Parse dates from various header formats.
    Returns (date_str, period_months, is_usd) or (None, 0, False).
    """
    header_text = header_text.strip()
    is_usd = 'US$' in header_text or 'USD' in header_text or '(US' in header_text

    # Remove currency annotations for parsing
    clean_header = re.sub(r'\s*\(.*?\)\s*', ' ', header_text).strip()

    # Pattern 1: DD Mon YYYY (BS style — point-in-time)
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", clean_header)
    if m:
        day = int(m.group(1))
        month_str = m.group(2).lower()
        year = int(m.group(3))
        month = MONTH_MAP.get(month_str)
        if month:
            return f"{year}-{month:02d}-{day:02d}", 0, is_usd

    # Pattern 2: NM Mon YYYY (cumulative PL/CF)
    m = re.match(r"(\d+)M\s+(\w+)\s+(\d{4})", clean_header)
    if m:
        months = int(m.group(1))
        month_str = m.group(2).lower()
        year = int(m.group(3))
        month = MONTH_MAP.get(month_str)
        if month:
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            return f"{year}-{month:02d}-{last_day:02d}", months, is_usd

    return None, 0, is_usd


def parse_value(val_str):
    """Parse a numeric value from markdown table cell."""
    val_str = val_str.strip()
    if not val_str or val_str == "-" or val_str == "—" or val_str == "":
        return None
    val_str = val_str.replace("**", "")
    negative = False
    if val_str.startswith("(") and val_str.endswith(")"):
        negative = True
        val_str = val_str[1:-1]
    val_str = val_str.replace(",", "")
    try:
        val = float(val_str)
        return -val if negative else val
    except ValueError:
        return None


def parse_markdown_file(filepath):
    """
    Parse an extracted markdown file. Returns:
    - columns: [(date_str, period_months, is_usd, {canonical: value}), ...]
    - unit_type: str
    - is_dual_currency: bool
    """
    with open(filepath) as f:
        lines = f.readlines()

    unit_type = None
    column_dates = []  # [(date_str, period_months, is_usd, col_index)]
    column_values = {}  # col_index -> {canonical: value}
    header_found = False

    for line in lines:
        line = line.strip()

        if line.startswith("UNIT_TYPE:"):
            unit_type = line.split(":", 1)[1].strip()
            continue

        if line.startswith("|") and not header_found:
            cells = [c.strip() for c in line.split("|")]
            cells = [c for c in cells if c]
            if len(cells) >= 4 and ("Source" in cells[0]):
                for i in range(3, len(cells)):
                    date_str, period_months, is_usd = parse_date_from_header(cells[i])
                    if date_str:
                        column_dates.append((date_str, period_months, is_usd, i))
                        column_values[i] = {}
                header_found = True
                continue

        if line.startswith("|") and header_found and ("---" in line or ":---" in line):
            continue

        if line.startswith("|") and header_found:
            cells = [c.strip() for c in line.split("|")]
            cells = [c for c in cells if c]
            if len(cells) >= 4:
                canonical = cells[1].replace("**", "").strip()
                if not canonical:
                    continue
                for date_str, period_months, is_usd, col_idx in column_dates:
                    if col_idx < len(cells):
                        val = parse_value(cells[col_idx])
                        if val is not None:
                            column_values[col_idx][canonical] = val

    # Check if file has dual currency (both USD and non-USD columns)
    has_usd = any(is_usd for _, _, is_usd, _ in column_dates)
    has_non_usd = any(not is_usd for _, _, is_usd, _ in column_dates)
    is_dual_currency = has_usd and has_non_usd

    results = []
    for date_str, period_months, is_usd, col_idx in column_dates:
        results.append((date_str, period_months, is_usd, column_values.get(col_idx, {})))

    return results, unit_type, is_dual_currency


def run_check(statement_type):
    """Run comparative check for one statement type (bs, pl, cf)."""
    folder = os.path.join(DATA_DIR, f"extracted_{statement_type}")
    if not os.path.exists(folder):
        return [], {'dual_currency_skipped': 0, 'period_mismatch_skipped': 0}

    stats = {'dual_currency_skipped': 0, 'period_mismatch_skipped': 0, 'restatement_skipped': 0}

    # Step 1: Parse all files, index current-period values (non-USD only)
    # Key: (ticker, consolidation, period_end, period_months) -> {canonical: value}
    current_index = {}
    file_data = []
    dual_currency_files = set()

    for filepath in sorted(glob.glob(os.path.join(folder, "*.md"))):
        fname = os.path.basename(filepath)
        parts = fname.replace(".md", "").split("_")
        ticker = parts[0]
        consolidation = parts[-1]

        columns, unit_type, is_dual_currency = parse_markdown_file(filepath)
        if not columns:
            continue

        if is_dual_currency:
            dual_currency_files.add(fname)
            stats['dual_currency_skipped'] += 1
            continue

        unit_cat = "thousands" if unit_type == "thousands" else "rupees"

        # Index non-USD columns only
        for date_str, pm, is_usd, vals in columns:
            if vals and not is_usd:
                key = (ticker, consolidation, date_str, pm, unit_cat)
                if key not in current_index:
                    current_index[key] = (vals, unit_type)

        if not columns:
            continue

        current_date, current_pm, current_is_usd, _ = columns[0]

        # Store comparative columns for checking (columns 1+, non-USD only)
        for i, (date_str, period_months, is_usd, vals) in enumerate(columns):
            if i == 0:
                continue
            if not vals or is_usd:
                continue
            file_data.append({
                "ticker": ticker,
                "consolidation": consolidation,
                "filename": fname,
                "filepath": filepath,
                "current_date": current_date,
                "current_period_months": current_pm,
                "comparative_date": date_str,
                "comparative_period_months": period_months,
                "comparative_values": vals,
                "unit_type": unit_type,
                "statement_type": statement_type,
            })

    # Step 2: Match comparatives to standalone files
    mismatches = []

    for fd in file_data:
        ticker = fd["ticker"]
        consolidation = fd["consolidation"]
        comp_date = fd["comparative_date"]
        comp_period_months = fd["comparative_period_months"]
        comp_vals = fd["comparative_values"]
        unit_type = fd["unit_type"]
        unit_cat = "thousands" if unit_type == "thousands" else "rupees"

        # Find standalone with MATCHING period duration
        standalone_vals = None
        standalone_unit = None
        for try_unit in [unit_cat, "rupees" if unit_cat == "thousands" else "thousands"]:
            key = (ticker, consolidation, comp_date, comp_period_months, try_unit)
            if key in current_index:
                standalone_vals, standalone_unit = current_index[key]
                break

        if standalone_vals is None:
            continue

        # Determine scaling for unit conversion
        scale = 1.0
        s_cat = "thousands" if standalone_unit == "thousands" else "rupees"
        if unit_cat != s_cat:
            if unit_cat == "thousands" and s_cat == "rupees":
                scale = 1000.0
            elif unit_cat == "rupees" and s_cat == "thousands":
                scale = 1.0 / 1000.0

        # Compare fields
        field_mismatches = []
        actionable_issues = []
        fields_checked = 0
        fields_matched = 0
        issue_categories = set()

        for canonical, comp_val in comp_vals.items():
            if canonical not in standalone_vals:
                continue

            standalone_val = standalone_vals[canonical]
            adjusted_comp = comp_val * scale if scale != 1.0 else comp_val

            fields_checked += 1
            is_critical = canonical in CRITICAL_FIELDS

            if standalone_val == 0 and adjusted_comp == 0:
                fields_matched += 1
                continue

            # Calculate difference
            if standalone_val == 0:
                pct_diff = float('inf') if adjusted_comp != 0 else 0
            else:
                pct_diff = abs(adjusted_comp - standalone_val) / abs(standalone_val)

            # Skip if within normal tolerance (10%)
            if pct_diff <= NORMAL_TOLERANCE:
                fields_matched += 1
                continue

            # For critical fields, flag if >15%
            if is_critical and pct_diff > CRITICAL_TOLERANCE:
                category, confidence, description = categorize_issue(adjusted_comp, standalone_val, canonical)
                issue_categories.add(category)

                mismatch = {
                    "field": canonical,
                    "comparative_val": comp_val,
                    "standalone_val": standalone_val,
                    "pct_diff": f"{pct_diff:.1%}" if pct_diff != float('inf') else "inf",
                    "pct_diff_num": pct_diff if pct_diff != float('inf') else 999999,
                    "is_critical": True,
                    "category": category,
                    "confidence": confidence,
                    "description": description,
                }
                field_mismatches.append(mismatch)
                if category in ("OCR_ERROR", "UNIT_MISMATCH", "WRONG_PAGE", "SIGN_ERROR"):
                    actionable_issues.append(mismatch)
            elif pct_diff > 0.25:  # Non-critical but >25% difference
                field_mismatches.append({
                    "field": canonical,
                    "comparative_val": comp_val,
                    "standalone_val": standalone_val,
                    "pct_diff": f"{pct_diff:.1%}",
                    "pct_diff_num": pct_diff,
                    "is_critical": False,
                    "category": "RESTATEMENT",
                })

        # Only report if there are actionable issues (not just restatements)
        if actionable_issues:
            # Prioritize by category
            priority_order = {"OCR_ERROR": 0, "UNIT_MISMATCH": 1, "WRONG_PAGE": 2, "SIGN_ERROR": 3}
            primary_category = min(issue_categories, key=lambda c: priority_order.get(c, 99))

            mismatches.append({
                "ticker": ticker,
                "filename": fd["filename"],
                "current_date": fd["current_date"],
                "comparative_date": comp_date,
                "consolidation": consolidation,
                "unit_type": unit_type,
                "fields_checked": fields_checked,
                "fields_matched": fields_matched,
                "fields_mismatched": len(field_mismatches),
                "actionable_issues": len(actionable_issues),
                "primary_category": primary_category,
                "categories": list(issue_categories),
                "mismatched_fields": sorted(actionable_issues,
                    key=lambda x: (-x.get('pct_diff_num', 0)))[:5],
                "statement_type": statement_type,
            })
        elif field_mismatches:
            stats['restatement_skipped'] += 1

    return mismatches, stats


def main():
    all_mismatches = []
    total_stats = {'dual_currency_skipped': 0, 'period_mismatch_skipped': 0, 'restatement_skipped': 0}

    print("=" * 80)
    print("STEP 8: QC COMPARATIVE COLUMN MATCH")
    print("=" * 80)
    print(f"Normal tolerance (skip): {NORMAL_TOLERANCE:.0%}")
    print(f"Critical field threshold: {CRITICAL_TOLERANCE:.0%}")
    print()

    for stmt_type in ["bs", "pl", "cf"]:
        print(f"Scanning extracted_{stmt_type}/...")
        mismatches, stats = run_check(stmt_type)
        all_mismatches.extend(mismatches)
        for k, v in stats.items():
            total_stats[k] += v
        print(f"  Found {len(mismatches)} files with actionable issues")
        print(f"  Skipped: {stats['dual_currency_skipped']} dual-currency, {stats['restatement_skipped']} restatements")

    print()
    print("=" * 80)
    print(f"TOTAL: {len(all_mismatches)} files with actionable issues")
    print(f"Skipped: {total_stats['dual_currency_skipped']} dual-currency files")
    print(f"Skipped: {total_stats['restatement_skipped']} normal restatements")
    print("=" * 80)
    print()

    # Sort by category priority, then by severity
    category_priority = {"OCR_ERROR": 0, "UNIT_MISMATCH": 1, "WRONG_PAGE": 2, "SIGN_ERROR": 3}
    all_mismatches.sort(key=lambda x: (
        category_priority.get(x.get("primary_category", "OTHER"), 99),
        -x.get("actionable_issues", 0),
        x["ticker"]
    ))

    # Group by category
    by_category = defaultdict(list)
    for m in all_mismatches:
        by_category[m.get("primary_category", "OTHER")].append(m)

    # Report OCR_ERROR (note references as values)
    if by_category.get("OCR_ERROR"):
        items = by_category["OCR_ERROR"]
        print(f"OCR_ERROR ({len(items)} files — note reference captured as value):")
        print("-" * 60)
        for m in items[:20]:
            print(f"  {m['ticker']:8s} {m['statement_type'].upper():3s} {m['filename']}")
            for f in m["mismatched_fields"][:2]:
                print(f"           {f['field']}: {f['standalone_val']:,.0f} vs {f['comparative_val']:,.0f}")
                print(f"           -> {f.get('description', '')}")
        if len(items) > 20:
            print(f"  ... and {len(items) - 20} more")
        print()

    # Report UNIT_MISMATCH
    if by_category.get("UNIT_MISMATCH"):
        items = by_category["UNIT_MISMATCH"]
        print(f"UNIT_MISMATCH ({len(items)} files — ~1000x difference, likely wrong unit type):")
        print("-" * 60)
        for m in items[:15]:
            print(f"  {m['ticker']:8s} {m['statement_type'].upper():3s} {m['filename']}")
            for f in m["mismatched_fields"][:1]:
                ratio = abs(f['comparative_val'] / f['standalone_val']) if f['standalone_val'] else 0
                print(f"           {f['field']}: {f['standalone_val']:,.0f} vs {f['comparative_val']:,.0f} ({ratio:.0f}x)")
        if len(items) > 15:
            print(f"  ... and {len(items) - 15} more")
        print()

    # Report WRONG_PAGE
    if by_category.get("WRONG_PAGE"):
        items = by_category["WRONG_PAGE"]
        print(f"WRONG_PAGE ({len(items)} files — large difference, possible multi-year page):")
        print("-" * 60)
        for m in items[:15]:
            print(f"  {m['ticker']:8s} {m['statement_type'].upper():3s} {m['filename']}")
            for f in m["mismatched_fields"][:1]:
                print(f"           {f['field']}: {f['standalone_val']:,.0f} vs {f['comparative_val']:,.0f}")
        if len(items) > 15:
            print(f"  ... and {len(items) - 15} more")
        print()

    # Report SIGN_ERROR
    if by_category.get("SIGN_ERROR"):
        items = by_category["SIGN_ERROR"]
        print(f"SIGN_ERROR ({len(items)} files — positive/negative mismatch):")
        print("-" * 60)
        for m in items[:10]:
            print(f"  {m['ticker']:8s} {m['statement_type'].upper():3s} {m['filename']}")
            for f in m["mismatched_fields"][:1]:
                print(f"           {f['field']}: {f['standalone_val']:,.0f} vs {f['comparative_val']:,.0f}")
        if len(items) > 10:
            print(f"  ... and {len(items) - 10} more")
        print()

    # Summary
    print()
    print("=" * 80)
    print("SUMMARY BY CATEGORY")
    print("=" * 80)
    for cat in ["OCR_ERROR", "UNIT_MISMATCH", "WRONG_PAGE", "SIGN_ERROR"]:
        count = len(by_category.get(cat, []))
        if count > 0:
            print(f"  {cat:15s}: {count:4d} files")
    print(f"  {'TOTAL':15s}: {len(all_mismatches):4d} files")
    print()

    # Show affected tickers summary
    affected_tickers = set(m["ticker"] for m in all_mismatches)
    print(f"Affected tickers ({len(affected_tickers)}): {', '.join(sorted(affected_tickers)[:30])}")
    if len(affected_tickers) > 30:
        print(f"  ... and {len(affected_tickers) - 30} more")

    # Save full results
    output_path = os.path.join(PROJECT_ROOT, "artifacts", "stage3", "step8_comparative_mismatch_report.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(all_mismatches, f, indent=2, default=str)
    print(f"\nFull results saved to: {output_path}")


if __name__ == "__main__":
    main()
