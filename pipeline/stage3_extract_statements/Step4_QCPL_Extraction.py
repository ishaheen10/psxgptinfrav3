#!/usr/bin/env python3
"""
Step 4: QC Profit & Loss Extractions (Pre-JSONify)

Validates ref formulas directly on extraction .md files where data is complete.

For each extraction file:
1. Parse the table to get ref → value mapping for each column
2. For each formula (like C=A+B), validate sum matches actual value
3. Report pass/fail per file

Input:  data/extracted_pl/*.md
Output: artifacts/stage3/step4_qc_pl_extraction.json

Usage:
    python3 Step4_QCPL_Extraction.py                # Process all
    python3 Step4_QCPL_Extraction.py --ticker LUCK  # Single ticker
    python3 Step4_QCPL_Extraction.py --verbose      # Show details
"""

import argparse
import json
import re
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "extracted_pl"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_qc_pl_extraction.json"
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
MARKDOWN_PAGES_DIR = PROJECT_ROOT / "markdown_pages"

# Tolerance for formula validation (5%)
TOLERANCE_PCT = 5.0

# Load statement pages manifest once at module level
STATEMENT_PAGES = {}
if STATEMENT_PAGES_FILE.exists():
    with open(STATEMENT_PAGES_FILE) as f:
        STATEMENT_PAGES = json.load(f)


def parse_number(s: str) -> float | None:
    """Parse a number from table cell.

    Handles various formats found in financial statements:
    - $(7,031,603)$ : dollar signs with parentheses (negative)
    - (7,031,603)   : parentheses only (negative)
    - 7,031,603     : plain number with commas
    - -7,031,603    : leading minus sign
    - 7,031,603-    : trailing minus sign
    - 8 512 805     : spaces as thousands separator

    Returns:
        None: for empty cells or N/A (no data present)
        0.0: for dash (-/—) which represents zero in financial statements
        float: for actual numeric values
    """
    if not s or s.strip() == '':
        return None  # Empty cell - no data

    stripped = s.strip()
    if stripped in ['N/A', 'n/a']:
        return None  # Explicitly no data

    if stripped in ['-', '—']:
        return 0.0  # Dash means zero in financial context

    # Remove formatting: bold markers, commas, spaces, dollar signs
    s = stripped.replace('**', '').replace(',', '').replace(' ', '').replace('$', '')

    # Handle various negative formats
    is_negative = False

    # $(xxx)$ or (xxx) format - check AFTER removing $ signs
    if s.startswith('(') and s.endswith(')'):
        is_negative = True
        s = s[1:-1]
    # Trailing minus: xxx-
    elif s.endswith('-') and not s.startswith('-'):
        is_negative = True
        s = s[:-1]
    # Leading minus handled by float() naturally

    try:
        val = float(s)
        return -val if is_negative else val
    except ValueError:
        return None


def parse_extraction_file(filepath: Path) -> dict:
    """
    Parse a PL extraction .md file.

    P&L format: | Source Item | Canonical | Ref | 3M Jun 2024 | 3M Jun 2023 | ...

    Returns:
        {
            'columns': ['3M Jun 2024', '3M Jun 2023'],
            'rows': [
                {
                    'source': 'Revenue',
                    'canonical': 'revenue_net',
                    'ref': 'A',
                    'formula': None,
                    'values': [1000000, 900000]
                },
                ...
            ]
        }

    Or for PAGE_ERROR files (manifest mismatch):
        {
            'page_error': 'BALANCE_SHEET_ONLY',
            'columns': [],
            'rows': []
        }
    """
    content = filepath.read_text(encoding='utf-8')
    lines = content.split('\n')

    # Check for PAGE_ERROR (manifest mismatch - wrong page in extraction manifest)
    first_line = lines[0].strip() if lines else ''
    if first_line.startswith('PAGE_ERROR:'):
        error_type = first_line.replace('PAGE_ERROR:', '').strip()
        return {
            'page_error': error_type,
            'columns': [],
            'rows': []
        }

    result = {
        'columns': [],
        'rows': []
    }

    in_table = False

    for line in lines:
        line = line.strip()
        if not line.startswith('|'):
            continue

        parts = [p.strip() for p in line.split('|')]
        # Remove only first and last empty elements (from leading/trailing |)
        # but keep empty strings in middle to preserve column positions
        if parts and parts[0] == '':
            parts = parts[1:]
        if parts and parts[-1] == '':
            parts = parts[:-1]

        if len(parts) < 4:
            continue

        # Header row detection
        if 'Source Item' in line or 'Canonical' in line:
            # Columns are everything after Ref (index 3+)
            result['columns'] = [p.replace('**', '').strip() for p in parts[3:]]
            continue

        # Separator row
        if '---' in line or ':--' in line:
            in_table = True
            continue

        if not in_table and not result['columns']:
            continue

        # Data row
        source = parts[0].replace('**', '').strip()
        canonical = parts[1].replace('**', '').strip().lower()
        ref_raw = parts[2].replace('**', '').strip()

        # Skip empty/header rows
        if not ref_raw or ref_raw.lower() in ['ref', '']:
            continue

        # Parse ref and formula
        ref = ref_raw
        formula = None
        if '=' in ref_raw:
            ref_parts = ref_raw.split('=', 1)
            ref = ref_parts[0].strip()
            formula = ref_parts[1].strip()

        # Parse values for each column
        values = []
        for i in range(3, len(parts)):
            val = parse_number(parts[i])
            values.append(val)

        # Pad values if fewer than columns
        while len(values) < len(result['columns']):
            values.append(None)

        result['rows'].append({
            'source': source,
            'canonical': canonical,
            'ref': ref,
            'formula': formula,
            'values': values
        })

    return result


def check_column_structure(parsed: dict) -> dict:
    """
    Check for column structure issues like empty columns.

    Detects when header defines N columns but some columns have no/minimal data.

    Returns:
        {
            'has_issues': bool,
            'issues': [{'column': '3M Jun 2023', 'type': 'empty_column', 'fill_rate': 0.0}, ...]
        }
    """
    result = {
        'has_issues': False,
        'issues': []
    }

    columns = parsed['columns']
    rows = parsed['rows']

    if not columns or not rows:
        return result

    # Count non-null values per column
    for col_idx, col_name in enumerate(columns):
        total_rows = len(rows)
        non_null_count = 0

        for row in rows:
            if col_idx < len(row['values']):
                val = row['values'][col_idx]
                # Consider 0.0 as valid data, only None is empty
                if val is not None:
                    non_null_count += 1

        fill_rate = non_null_count / total_rows if total_rows > 0 else 0

        # Flag columns with <10% fill rate as empty
        if fill_rate < 0.10:
            result['has_issues'] = True
            result['issues'].append({
                'column': col_name,
                'type': 'empty_column',
                'fill_rate': round(fill_rate * 100, 1),
                'rows_with_data': non_null_count,
                'total_rows': total_rows
            })

    return result


def validate_formulas(parsed: dict) -> dict:
    """
    Validate all formulas in a parsed extraction.

    Returns:
        {
            'total_formulas': 10,
            'pass': 8,
            'fail': 2,
            'failures': [...]
        }
    """
    result = {
        'total_formulas': 0,
        'pass': 0,
        'fail': 0,
        'skip': 0,
        'failures': []
    }

    columns = parsed['columns']
    rows = parsed['rows']

    if not rows or not columns:
        return result

    # Build ref → row mapping
    ref_to_row = {}
    for row in rows:
        ref_to_row[row['ref']] = row

    # Check each formula for each column
    for row in rows:
        if not row['formula']:
            continue

        # Parse formula components
        formula = row['formula']
        component_refs = [c.strip() for c in formula.split('+')]

        # Validate for each column
        for col_idx, col_name in enumerate(columns):
            result['total_formulas'] += 1

            actual_value = row['values'][col_idx] if col_idx < len(row['values']) else None
            if actual_value is None:
                result['skip'] += 1
                continue

            # Sum components
            expected_value = 0
            missing = []
            components = {}

            for comp_ref in component_refs:
                comp_row = ref_to_row.get(comp_ref)
                if not comp_row:
                    missing.append(comp_ref)
                    continue

                comp_value = comp_row['values'][col_idx] if col_idx < len(comp_row['values']) else None
                if comp_value is None:
                    missing.append(f"{comp_ref}(None)")
                    continue

                components[comp_ref] = comp_value
                expected_value += comp_value

            # Skip if too many missing
            if len(missing) > len(component_refs) / 2:
                result['skip'] += 1
                continue

            # Calculate difference
            if actual_value == 0 and expected_value == 0:
                result['pass'] += 1
                continue

            if actual_value == 0:
                diff_pct = 100.0
            else:
                diff_pct = abs(actual_value - expected_value) / abs(actual_value) * 100

            if diff_pct <= TOLERANCE_PCT:
                result['pass'] += 1
            else:
                result['fail'] += 1
                result['failures'].append({
                    'column': col_name,
                    'canonical': row['canonical'],
                    'ref': row['ref'],
                    'formula': formula,
                    'expected': expected_value,
                    'actual': actual_value,
                    'diff_pct': round(diff_pct, 2),
                    'components': components,
                    'missing': missing
                })

    return result


def parse_filename(filename: str) -> dict | None:
    """
    Parse extraction filename to get ticker, period type, date, and consolidation.

    Format: TICKER_period-type_date_consolidation.md
    Examples:
        LUCK_quarterly_2024-03-31_consolidated.md
        ENGRO_annual_2023_unconsolidated.md

    Returns dict with ticker, period_type, date, consolidation, filing_period
    """
    name = filename.replace('.md', '')
    parts = name.split('_')

    if len(parts) < 4:
        return None

    ticker = parts[0]
    period_type = parts[1]  # 'quarterly' or 'annual'
    consolidation = parts[-1]  # last part

    # Date is everything between period_type and consolidation
    date_parts = parts[2:-1]
    date = '_'.join(date_parts) if date_parts else parts[2]

    # Build filing_period key (e.g., "quarterly_2024-03-31" or "annual_2023")
    filing_period = f"{period_type}_{date}"

    return {
        'ticker': ticker,
        'period_type': period_type,
        'date': date,
        'consolidation': consolidation,
        'filing_period': filing_period
    }


def get_source_markdown(ticker: str, filing_period: str, consolidation: str) -> str | None:
    """
    Load source markdown pages for a filing.
    """
    if ticker not in STATEMENT_PAGES:
        return None

    ticker_data = STATEMENT_PAGES[ticker]
    if filing_period not in ticker_data:
        return None

    period_data = ticker_data[filing_period]
    if consolidation not in period_data:
        return None

    pl_pages = period_data[consolidation].get('PL', [])
    if not pl_pages:
        return None

    # Build path to markdown pages
    if filing_period.startswith('annual_'):
        year = filing_period.replace('annual_', '')
        folder = f"{ticker}_Annual_{year}"
        folder_path = MARKDOWN_PAGES_DIR / ticker / year / folder
    else:
        date_part = filing_period.replace('quarterly_', '')
        year = date_part[:4]
        folder = f"{ticker}_Quarterly_{date_part}"
        folder_path = MARKDOWN_PAGES_DIR / ticker / year / folder

    if not folder_path.exists():
        return None

    # Load all PL pages
    content_parts = []
    for page_num in pl_pages:
        page_file = folder_path / f"page_{page_num:03d}.md"
        if page_file.exists():
            content_parts.append(page_file.read_text(encoding='utf-8'))

    return '\n'.join(content_parts) if content_parts else None


def extract_all_numbers(text: str) -> set[float]:
    """
    Extract all significant numbers from text.

    Handles various number formats:
    - Comma-separated: 1,234,567
    - Space-separated: 1 234 567 (common in OCR)
    - Plain: 1234567

    Returns set of absolute values > 1000 (to filter noise like note refs, percentages).
    Uses absolute values since sign conventions vary (parentheses vs minus).
    """
    numbers = set()

    # Pattern matches digit sequences with commas or spaces as separators
    pattern = r'\d[\d,\s]*\d|\d+'

    for match in re.findall(pattern, text):
        # Remove all separators
        s = match.replace(',', '').replace(' ', '')
        try:
            val = float(s)
            if val > 1000:  # Filter small numbers (note refs, percentages, etc.)
                numbers.add(val)
        except ValueError:
            pass

    return numbers


def fuzzy_match(extract_val: float, source_nums: set[float], tolerance: float = 0.005) -> tuple[bool, str]:
    """
    Check if extract_val matches any source number within tolerance.

    Handles LLM rounding (e.g., 5,209,348 extracted as 5,209,000).
    Default tolerance of 0.5% handles most rounding cases.

    Also checks for 1000x matches which indicate unit conversion errors.

    Returns:
        (matched: bool, match_type: str)
        match_type is 'exact', 'fuzzy', 'unit_1000x', or 'none'
    """
    for src_val in source_nums:
        if src_val == 0:
            continue
        # Check if values are within tolerance of each other
        diff = abs(extract_val - src_val) / src_val
        if diff <= tolerance:
            return (True, 'fuzzy' if diff > 0 else 'exact')

    # Check for 1000x match (unit conversion error)
    for src_val in source_nums:
        if src_val == 0:
            continue
        # Check if extraction is ~1000x source (LLM multiplied by 1000)
        ratio = extract_val / src_val
        if 990 < ratio < 1010:  # Within 1% of 1000x
            return (True, 'unit_1000x')
        # Check if extraction is ~1/1000 source (LLM divided by 1000)
        if 0.00099 < ratio < 0.00101:
            return (True, 'unit_1000x')

    return (False, 'none')


def check_source_matching(parsed: dict, source_content: str | None) -> dict:
    """
    Verify extracted values appear in source markdown using fuzzy number overlap.

    Simple approach: extract all numbers from source and extraction,
    check what percentage of extracted numbers exist in source (with tolerance).
    This is robust to different table formats, column orders, OCR variations,
    and LLM rounding.

    Also detects potential unit conversion errors (1000x differences).

    Returns:
        {
            'status': 'pass' | 'warn' | 'skip',
            'checked': int,
            'matched': int,
            'match_rate': float,
            'unit_issues': int,  # Count of 1000x matches (probable unit errors)
            'missing': [...]  # Numbers in extraction but not in source
        }
    """
    result = {
        'status': 'pass',
        'checked': 0,
        'matched': 0,
        'match_rate': None,
        'unit_issues': 0,
        'missing': []
    }

    if source_content is None:
        result['status'] = 'skip'
        result['reason'] = 'source not available'
        return result

    # Extract all numbers from source
    source_nums = extract_all_numbers(source_content)
    if not source_nums:
        result['status'] = 'skip'
        result['reason'] = 'no numbers found in source'
        return result

    # Extract numbers from the parsed extraction
    # Get values from all rows and columns
    extract_nums = set()
    for row in parsed['rows']:
        for val in row['values']:
            if val is not None and abs(val) > 1000:
                extract_nums.add(abs(val))

    if not extract_nums:
        result['status'] = 'skip'
        result['reason'] = 'no significant numbers in extraction'
        return result

    # Calculate overlap with fuzzy matching (handles LLM rounding)
    result['checked'] = len(extract_nums)
    matched_count = 0
    unit_issues = 0
    missing = []
    for ext_val in extract_nums:
        matched, match_type = fuzzy_match(ext_val, source_nums)
        if matched:
            matched_count += 1
            if match_type == 'unit_1000x':
                unit_issues += 1
        else:
            missing.append(ext_val)

    result['matched'] = matched_count
    result['unit_issues'] = unit_issues
    result['missing'] = sorted(missing)[:10]  # Limit to first 10

    # Calculate match rate
    result['match_rate'] = round(result['matched'] / result['checked'] * 100, 1)

    # Mark as warning if match rate is below threshold or unit issues detected
    if unit_issues > 0:
        result['status'] = 'warn'
        result['reason'] = f"Possible unit mismatch: {unit_issues} values are 1000x different"
    elif result['match_rate'] < 80:
        result['status'] = 'warn'
        result['reason'] = f"Low match rate: {result['match_rate']}%"

    return result


def process_file(filepath: Path, verbose: bool = False) -> dict:
    """Process a single extraction file."""
    parsed = parse_extraction_file(filepath)

    # Handle PAGE_ERROR (manifest mismatch)
    if 'page_error' in parsed:
        return {
            'file': filepath.name,
            'columns': 0,
            'rows': 0,
            'formulas': 0,
            'pass': 0,
            'fail': 0,
            'skip': 0,
            'failures': [],
            'column_issues': [],
            'has_formula_failures': False,
            'has_column_issues': False,
            'has_source_issues': False,
            'source_match': {'status': 'skip', 'reason': 'page_error'},
            'status': 'manifest_error',
            'page_error': parsed['page_error']
        }

    validation = validate_formulas(parsed)
    structure = check_column_structure(parsed)

    # Source matching - load source markdown and check
    file_info = parse_filename(filepath.name)
    source_content = None
    if file_info:
        source_content = get_source_markdown(
            file_info['ticker'],
            file_info['filing_period'],
            file_info['consolidation']
        )
    source_match = check_source_matching(parsed, source_content)

    # Track issues independently
    has_formula_failures = validation['fail'] > 0
    has_column_issues = structure['has_issues']
    has_source_issues = source_match['status'] == 'warn'

    # Determine status (for backward compatibility and summary)
    if has_formula_failures and has_column_issues:
        status = 'fail_and_column_issue'
    elif has_formula_failures:
        status = 'fail'
    elif has_column_issues:
        status = 'column_issue'
    else:
        status = 'pass'

    return {
        'file': filepath.name,
        'columns': len(parsed['columns']),
        'rows': len(parsed['rows']),
        'formulas': validation['total_formulas'],
        'pass': validation['pass'],
        'fail': validation['fail'],
        'skip': validation['skip'],
        'failures': validation['failures'],
        'column_issues': structure['issues'],
        'has_formula_failures': has_formula_failures,
        'has_column_issues': has_column_issues,
        'source_match': source_match,
        'has_source_issues': has_source_issues,
        'status': status
    }


def main():
    parser = argparse.ArgumentParser(description="QC PL Extractions (Pre-JSONify)")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show details")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 4: QC PROFIT & LOSS EXTRACTIONS")
    print("=" * 70)
    print()

    if not INPUT_DIR.exists():
        print(f"ERROR: Input directory not found: {INPUT_DIR}")
        return

    # Get files
    files = sorted(INPUT_DIR.glob("*.md"))
    if args.ticker:
        files = [f for f in files if f.name.startswith(f"{args.ticker}_")]

    if not files:
        print(f"No extraction files found in {INPUT_DIR}")
        return

    print(f"Processing {len(files)} extraction files...")
    print()

    # Process each file
    all_results = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_files': 0,
            'files_pass': 0,
            'files_with_formula_failures': 0,
            'files_with_column_issues': 0,
            'files_with_source_issues': 0,
            'files_manifest_error': 0,
            'total_formulas': 0,
            'formulas_pass': 0,
            'formulas_fail': 0,
            'formulas_skip': 0,
            'source_values_checked': 0,
            'source_values_matched': 0
        },
        'manifest_errors': [],  # Track manifest errors separately for Stage 2 feedback
        'formula_failures': [],  # Track files with formula failures
        'column_issues': [],  # Track column structure issues
        'source_issues': [],  # Track source matching issues
        'files': []
    }

    for filepath in files:
        result = process_file(filepath, args.verbose)
        all_results['files'].append(result)

        # Update summary
        all_results['summary']['total_files'] += 1
        all_results['summary']['total_formulas'] += result['formulas']
        all_results['summary']['formulas_pass'] += result['pass']
        all_results['summary']['formulas_fail'] += result['fail']
        all_results['summary']['formulas_skip'] += result['skip']

        # Track source matching stats
        sm = result.get('source_match', {})
        all_results['summary']['source_values_checked'] += sm.get('checked', 0)
        all_results['summary']['source_values_matched'] += sm.get('matched', 0)

        # Handle manifest errors
        if result['status'] == 'manifest_error':
            all_results['summary']['files_manifest_error'] += 1
            all_results['manifest_errors'].append({
                'file': result['file'],
                'error_type': result.get('page_error', 'unknown')
            })
        else:
            # Track formula failures, column issues, and source issues independently
            if result['has_formula_failures']:
                all_results['summary']['files_with_formula_failures'] += 1
                all_results['formula_failures'].append({
                    'file': result['file'],
                    'failures': result['failures']
                })

            if result['has_column_issues']:
                all_results['summary']['files_with_column_issues'] += 1
                all_results['column_issues'].append({
                    'file': result['file'],
                    'issues': result['column_issues']
                })

            if result.get('has_source_issues'):
                all_results['summary']['files_with_source_issues'] += 1
                all_results['source_issues'].append({
                    'file': result['file'],
                    'match_rate': sm.get('match_rate'),
                    'mismatched': sm.get('mismatched', []),
                    'not_found': sm.get('not_found', [])
                })

            if not result['has_formula_failures'] and not result['has_column_issues']:
                all_results['summary']['files_pass'] += 1

        # Print progress
        if result['status'] == 'manifest_error':
            print(f"MANIFEST_ERROR: {filepath.name} - {result.get('page_error', 'unknown')} (wrong page in extraction manifest)")
        else:
            issues = []
            if result['has_formula_failures']:
                issues.append(f"FAIL ({result['pass']}/{result['formulas']} formulas)")
            if result['has_column_issues']:
                issues.append(f"COLUMN_ISSUE ({len(result['column_issues'])} empty)")
            if result.get('has_source_issues'):
                issues.append(f"SOURCE_WARN ({sm.get('match_rate', 0)}% match)")

            if issues:
                print(f"{filepath.name}: {', '.join(issues)}")
                if args.verbose:
                    for f in result['failures'][:3]:
                        print(f"  {f['column']}: {f['canonical']} ({f['ref']}={f['formula']})")
                        print(f"    Expected: {f['expected']:,.0f}, Actual: {f['actual']:,.0f}, Diff: {f['diff_pct']}%")
                    for issue in result['column_issues']:
                        print(f"  Empty column: {issue['column']} ({issue['fill_rate']}% fill rate)")
                    for mismatch in sm.get('mismatched', [])[:2]:
                        print(f"  Source mismatch: {mismatch['canonical']} = {mismatch['extracted_value']:,.0f}, source has {mismatch['source_values']}")
            elif args.verbose:
                print(f"PASS: {filepath.name} - {result['pass']}/{result['formulas']} formulas OK")

    # Write results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(all_results, f, indent=2)

    # Print summary
    s = all_results['summary']
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total extraction files:    {s['total_files']}")
    print(f"  Pass (no issues):        {s['files_pass']}")
    print(f"  With formula failures:   {s['files_with_formula_failures']}")
    print(f"  With column issues:      {s['files_with_column_issues']}")
    print(f"  With source issues:      {s['files_with_source_issues']}")
    if s['files_manifest_error'] > 0:
        print(f"  Manifest errors:         {s['files_manifest_error']}")
    print()
    print(f"Total formulas checked:    {s['total_formulas']}")
    print(f"  Pass:                    {s['formulas_pass']} ({s['formulas_pass']/s['total_formulas']*100:.1f}%)" if s['total_formulas'] > 0 else "  Pass: 0")
    print(f"  Fail:                    {s['formulas_fail']} ({s['formulas_fail']/s['total_formulas']*100:.1f}%)" if s['total_formulas'] > 0 else "  Fail: 0")
    print(f"  Skip:                    {s['formulas_skip']}")
    print()
    print(f"Source matching:")
    print(f"  Values checked:          {s['source_values_checked']}")
    print(f"  Values matched:          {s['source_values_matched']}")
    if s['source_values_checked'] > 0:
        match_rate = s['source_values_matched'] / s['source_values_checked'] * 100
        print(f"  Match rate:              {match_rate:.1f}%")

    # Print manifest errors if any
    if all_results['manifest_errors']:
        print()
        print("MANIFEST ERRORS (fix Stage 2 extraction_manifest.json):")
        for err in all_results['manifest_errors']:
            print(f"  {err['file']}: {err['error_type']}")

    # Print formula failures if any
    if all_results['formula_failures']:
        print()
        print("FORMULA FAILURES:")
        for item in all_results['formula_failures']:
            print(f"  {item['file']}")

    # Print column issues if any
    if all_results['column_issues']:
        print()
        print("COLUMN ISSUES (empty columns):")
        for item in all_results['column_issues']:
            cols = [i['column'] for i in item['issues']]
            print(f"  {item['file']}: {', '.join(cols)}")

    # Print source issues if any
    if all_results['source_issues']:
        print()
        print("SOURCE MATCHING ISSUES:")
        for item in all_results['source_issues'][:20]:  # Limit to first 20
            print(f"  {item['file']}: {item['match_rate']}% match")

    print()
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
