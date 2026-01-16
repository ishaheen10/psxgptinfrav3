#!/usr/bin/env python3
"""
Step 4: QC P&L Extractions (Combined Formula + Source Match)

Validates P&L extractions via:
1. Formula validation - evaluates Ref column formulas (C=A+B)
2. Source match - compares extracted values against source markdown

Input:  data/extracted_pl/*.md
        markdown_pages/
        artifacts/stage3/step2_statement_pages.json
        artifacts/stage3/qc_accepted_exceptions.json (optional)
Output: artifacts/stage3/step4_qc_results.json

Usage:
    python3 Step4_QCPL.py                    # QC all
    python3 Step4_QCPL.py --ticker LUCK      # Single ticker
    python3 Step4_QCPL.py --verbose          # Show all details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "extracted_pl"
MARKDOWN_DIR = PROJECT_ROOT / "markdown_pages"
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
EXCEPTIONS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "qc_accepted_exceptions.json"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_qc_results.json"

# Tolerances
FORMULA_TOLERANCE_PCT = 0.25  # 0.25% for formula validation
SOURCE_MATCH_THRESHOLD = 0.97  # 97% match rate for source validation


def parse_number(s: str) -> float | None:
    """Parse a number from the table - parentheses mean negative."""
    if not s or s.strip() in ['', '-', '—', 'N/A', 'n/a']:
        return None

    s = s.strip().replace('**', '').replace(',', '').replace(' ', '')
    s = s.lstrip('$')

    # Parentheses indicate negative
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]

    try:
        return float(s)
    except ValueError:
        return None


def parse_ref(ref: str) -> tuple[str, str | None]:
    """Parse Ref column. Returns (name, formula) where formula is None for inputs."""
    ref = ref.strip().replace('**', '')
    if '=' in ref:
        parts = ref.split('=', 1)
        return parts[0].strip(), parts[1].strip()
    return ref.strip(), None


def parse_table(content: str) -> tuple[list[dict], list[str]]:
    """Parse the markdown table into rows. Returns (rows, columns)."""
    rows = []
    columns = []
    in_table = False

    for line in content.split('\n'):
        line = line.strip()
        if not line.startswith('|'):
            continue

        cells = [c.strip() for c in line.split('|')]
        cells = [c for c in cells if c]

        # Skip header separator
        if '---' in line:
            in_table = True
            continue

        if not in_table:
            # Header row - get period columns
            if len(cells) >= 4:
                columns = cells[3:]
            continue

        # Parse data row
        if len(cells) < 4:
            continue

        source_item = cells[0].replace('**', '').strip()
        canonical = cells[1].replace('**', '').strip()
        ref_raw = cells[2]

        # Skip if this looks like a header
        if 'Source Item' in source_item or 'Canonical' in canonical:
            continue

        ref_name, formula = parse_ref(ref_raw)

        # Parse values for each period column
        values = []
        for i in range(3, len(cells)):
            values.append(parse_number(cells[i]))

        rows.append({
            'source': source_item,
            'canonical': canonical,
            'ref': ref_name,
            'formula': formula,
            'is_calc': formula is not None,
            'values': values
        })

    return rows, columns


def evaluate_formula(formula: str, refs: dict, period_idx: int) -> float | None:
    """Evaluate a formula like 'A+B+C' using the refs dict."""
    formula = formula.strip()
    if not formula.startswith('+') and not formula.startswith('-'):
        formula = '+' + formula

    # Split into terms with their signs
    terms = re.findall(r'([+-])\s*([A-Z]+)', formula)

    if not terms:
        return None

    total = 0
    for sign, ref in terms:
        if ref not in refs:
            return None
        val = refs[ref][period_idx] if period_idx < len(refs[ref]) else None
        if val is None:
            return None

        if sign == '+':
            total += val
        else:
            total -= val

    return total


def get_unit_multiplier(content: str) -> float:
    """Get multiplier based on UNIT_TYPE."""
    match = re.search(r'UNIT_TYPE:\s*(\w+)', content)
    if match:
        unit = match.group(1).lower()
        if unit == 'thousands':
            return 1000
        elif unit == 'millions':
            return 1000000
        elif unit in ('rupees', 'full_rupees'):
            return 1
    return 1000  # Default to thousands


def extract_all_numbers(content: str) -> set[float]:
    """Extract all numbers from source content."""
    numbers = set()

    patterns = [
        r'\([\d,]+(?:\.\d+)?\)',  # (1,234,567)
        r'[\d,]+(?:\.\d+)?',       # 1,234,567
        r'\([\d]+(?: \d{3})+\)',   # (1 234 567) - space-separated in parens
        r'[\d]+(?: \d{3})+',       # 1 234 567 - space-separated
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, content):
            # Handle space-separated numbers by removing spaces
            num_str = match.group().replace(' ', '')
            val = parse_number(num_str)
            if val is not None and abs(val) > 0:
                numbers.add(abs(val))

    return numbers


def get_source_pages(extraction_path: Path, statement_pages: dict) -> list[Path]:
    """Get source markdown pages for an extraction."""
    stem = extraction_path.stem
    parts = stem.split('_')
    ticker = parts[0]
    period_type = parts[1]

    if period_type == 'annual':
        year = parts[2]
        consolidation = parts[3]
        period_key = f"annual_{year}"
        md_dir = MARKDOWN_DIR / ticker / year / f"{ticker}_Annual_{year}"
    else:
        date = parts[2]
        year = date.split('-')[0]
        consolidation = parts[3]
        period_key = f"quarterly_{date}"
        md_dir = MARKDOWN_DIR / ticker / year / f"{ticker}_Quarterly_{date}"

    if not md_dir.exists():
        return []

    pages = []
    if ticker in statement_pages:
        ticker_data = statement_pages[ticker]
        if period_key in ticker_data:
            period_data = ticker_data[period_key]
            if consolidation in period_data:
                pl_pages = period_data[consolidation].get('PL', [])
                for page_num in pl_pages:
                    page_file = md_dir / f"page_{page_num:03d}.md"
                    if page_file.exists():
                        pages.append(page_file)

    return pages


def validate_file(filepath: Path, statement_pages: dict, verbose: bool = False) -> dict:
    """Validate a single P&L extraction file (formula + source match)."""
    content = filepath.read_text()
    rows, columns = parse_table(content)

    result = {
        'file': filepath.name,
        'status': 'pass',
        # Formula validation
        'formula_total': 0,
        'formula_passed': 0,
        'formula_failed': 0,
        'formula_failures': [],
        # Source match
        'source_checked': 0,
        'source_matched': 0,
        'source_not_found': 0,
        'source_match_rate': None,
        'source_missing': [],
    }

    if not rows:
        result['status'] = 'error'
        result['error'] = 'no_table'
        return result

    num_periods = len(rows[0]['values']) if rows else 0
    refs = {}
    unit_multiplier = get_unit_multiplier(content)

    # === FORMULA VALIDATION ===
    for row in rows:
        ref = row['ref']
        formula = row['formula']
        values = row['values']
        refs[ref] = values

        if formula is None:
            continue

        result['formula_total'] += 1
        all_passed = True
        failure_details = []

        for period_idx in range(num_periods):
            if period_idx >= len(values):
                continue
            actual = values[period_idx]
            expected = evaluate_formula(formula, refs, period_idx)

            if actual is None or expected is None:
                continue

            diff = abs(expected - actual)
            if actual == 0 and expected == 0:
                passed = True
            elif actual == 0:
                passed = False
            else:
                pct_diff = (diff / abs(actual) * 100)
                passed = pct_diff < FORMULA_TOLERANCE_PCT

            if not passed:
                all_passed = False
                pct = (diff / abs(actual) * 100) if actual != 0 else float('inf')
                failure_details.append({
                    'period': period_idx + 1,
                    'expected': expected,
                    'actual': actual,
                    'diff': diff,
                    'pct': pct,
                })

        if all_passed:
            result['formula_passed'] += 1
        else:
            result['formula_failed'] += 1
            result['formula_failures'].append({
                'ref': ref,
                'formula': formula,
                'canonical': row['canonical'],
                'details': failure_details,
            })

    # === SOURCE MATCH VALIDATION ===
    source_pages = get_source_pages(filepath, statement_pages)

    if source_pages:
        source_content = '\n'.join([p.read_text() for p in source_pages])
        source_numbers = extract_all_numbers(source_content)

        if source_numbers:
            for row in rows:
                if row['is_calc']:
                    continue  # Skip calculated rows

                for i, val in enumerate(row['values']):
                    if val is None or val == 0:
                        continue

                    result['source_checked'] += 1
                    full_value = val * unit_multiplier

                    # Check if value exists in source
                    found = False
                    for src_val in source_numbers:
                        # Exact match (extracted value or scaled value)
                        if abs(val) == src_val or abs(full_value) == src_val:
                            found = True
                            break
                        # Check if source value divided by unit matches extracted (for full rupee sources)
                        if unit_multiplier > 1 and src_val > unit_multiplier:
                            scaled_src = src_val / unit_multiplier
                            if abs(abs(val) - scaled_src) / max(abs(val), 1) <= 0.001:  # 0.1% tolerance
                                found = True
                                break
                        # Small percentage tolerance
                        if src_val > 0:
                            diff_pct = abs(abs(val) - src_val) / src_val * 100
                            if diff_pct <= 0.01:
                                found = True
                                break

                    if found:
                        result['source_matched'] += 1
                    else:
                        result['source_not_found'] += 1
                        if len(result['source_missing']) < 5:
                            result['source_missing'].append({
                                'item': row['source'][:40],
                                'value': val,
                            })

            if result['source_checked'] > 0:
                result['source_match_rate'] = round(
                    result['source_matched'] / result['source_checked'] * 100, 1
                )

    # === DETERMINE OVERALL STATUS ===
    if result['formula_failed'] > 0:
        result['status'] = 'fail'
    elif result['source_match_rate'] is not None and result['source_match_rate'] < SOURCE_MATCH_THRESHOLD * 100:
        result['status'] = 'fail'

    return result


def main():
    parser = argparse.ArgumentParser(description="QC P&L extractions (formula + source match)")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 4: QC P&L EXTRACTIONS (FORMULA + SOURCE MATCH)")
    print("=" * 70)

    # Load statement pages manifest
    statement_pages = {}
    if STATEMENT_PAGES_FILE.exists():
        with open(STATEMENT_PAGES_FILE) as f:
            statement_pages = json.load(f)
        print(f"Loaded statement pages for {len(statement_pages)} tickers")

    # Load accepted exceptions
    exceptions = set()
    if EXCEPTIONS_FILE.exists():
        with open(EXCEPTIONS_FILE) as f:
            exc_data = json.load(f)
            for item in exc_data.get('files', []):
                exceptions.add(item['file'])
        print(f"Loaded {len(exceptions)} accepted exceptions")

    if not INPUT_DIR.exists():
        print(f"ERROR: Input directory not found: {INPUT_DIR}")
        return

    files = sorted(INPUT_DIR.glob("*.md"))
    if args.ticker:
        files = [f for f in files if f.name.startswith(args.ticker + "_")]

    print(f"Found {len(files)} files to validate\n")

    # Validate all files
    all_results = []
    stats = defaultdict(int)

    for f in files:
        result = validate_file(f, statement_pages, args.verbose)
        all_results.append(result)

        file_key = f.stem
        is_exception = file_key in exceptions

        stats['total_files'] += 1
        stats['formula_total'] += result['formula_total']
        stats['formula_passed'] += result['formula_passed']
        stats['formula_failed'] += result['formula_failed']
        stats['source_checked'] += result['source_checked']
        stats['source_matched'] += result['source_matched']
        stats['source_not_found'] += result['source_not_found']

        if result['formula_total'] == 0:
            stats['no_formulas'] += 1

        if result['status'] == 'pass':
            stats['files_passed'] += 1
        elif is_exception:
            stats['files_exception'] += 1
            result['status'] = 'exception'
        else:
            stats['files_failed'] += 1

        if args.verbose or result['status'] == 'fail':
            status_str = result['status'].upper()
            if is_exception:
                status_str = "EXCEPTION"
            print(f"{status_str}: {result['file']}")
            if result['formula_failures']:
                print(f"  Formulas: {result['formula_passed']}/{result['formula_total']} passed")
                for fail in result['formula_failures'][:2]:
                    print(f"    ✗ {fail['ref']}={fail['formula']} ({fail['canonical']})")
                    for d in fail['details'][:2]:
                        print(f"      Period {d['period']}: expected={d['expected']:,.0f}, actual={d['actual']:,.0f}, diff={d['diff']:,.0f} ({d['pct']:.2f}%)")
            if result['source_match_rate'] is not None and result['source_match_rate'] < 90:
                print(f"  Source match: {result['source_match_rate']}% ({result['source_matched']}/{result['source_checked']})")
            print()

    # Save results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump({
            'stats': dict(stats),
            'results': all_results,
        }, f, indent=2)

    # Print summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Total files:        {stats['total_files']}")
    print(f"  Files passed:       {stats['files_passed']}")
    print(f"  Files failed:       {stats['files_failed']}")
    print(f"  Files exception:    {stats['files_exception']}")
    print(f"  No formulas:        {stats['no_formulas']}")
    print()
    print(f"  Formula checks:     {stats['formula_total']}")
    print(f"  Formulas passed:    {stats['formula_passed']}")
    print(f"  Formulas failed:    {stats['formula_failed']}")
    print()
    print(f"  Source values:      {stats['source_checked']}")
    print(f"  Source matched:     {stats['source_matched']}")
    print(f"  Source not found:   {stats['source_not_found']}")

    if stats['formula_total'] > 0:
        formula_rate = stats['formula_passed'] / stats['formula_total'] * 100
        print(f"\n  Formula pass rate:  {formula_rate:.1f}%")

    if stats['source_checked'] > 0:
        source_rate = stats['source_matched'] / stats['source_checked'] * 100
        print(f"  Source match rate:  {source_rate:.1f}%")

    clean_rate = stats['files_passed'] / stats['total_files'] * 100 if stats['total_files'] > 0 else 0
    print(f"\n  Clean rate:         {clean_rate:.1f}%")

    print()
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
