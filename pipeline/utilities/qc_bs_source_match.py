#!/usr/bin/env python3
"""
Source-based QC for Balance Sheet extractions.

This script verifies:
1. EXTRACTION ACCURACY: Do extracted values match the source document?
2. SOURCE QUALITY: Does the source document's accounting equation balance?

This separates extraction errors from source document issues.
"""

import re
import json
from pathlib import Path
from typing import Optional
import argparse


def extract_numbers_from_row(line: str) -> list[int]:
    """
    Extract financial values from a markdown table row.
    Skips note reference columns (small numbers < 100).
    """
    if '|' not in line:
        return []

    parts = line.split('|')
    numbers = []

    for part in parts[2:]:  # Skip first two columns (item name, notes)
        part_clean = part.replace(',', '').replace('(', '-').replace(')', '').replace('-', '0').strip()
        # Remove any text/formatting
        part_clean = re.sub(r'[^\d\-]', '', part_clean)
        if part_clean:
            try:
                val = int(part_clean)
                # Only include if it looks like a financial value (>= 100)
                # This filters out note references like "22", "4", etc.
                if abs(val) >= 100 or val == 0:
                    numbers.append(val)
            except:
                pass

    return numbers


def find_source_row(source_content: str, source_item: str) -> Optional[str]:
    """
    Find a row in source that best matches the source_item text.
    Prefers:
    1. Exact cell match (first column matches exactly)
    2. Rows with financial values (not percentages or labels)
    """
    source_item_clean = source_item.strip().lower().replace('**', '')

    candidates = []

    for line in source_content.split('\n'):
        if '|' not in line:
            continue

        line_clean = line.lower().replace('**', '')
        parts = [p.strip().lower().replace('**', '') for p in line.split('|')]

        # Check if source_item is in this line
        if source_item_clean not in line_clean:
            continue

        # Score this candidate
        score = 0

        # Check first column for exact match
        if len(parts) > 1 and parts[1].strip() == source_item_clean:
            score += 100  # Strong preference for exact first-column match

        # Check if line has financial values (numbers > 1000)
        has_financial = False
        for part in parts[2:]:
            part_digits = re.sub(r'[^\d]', '', part.replace(',', ''))
            if part_digits and len(part_digits) >= 4:  # At least 4 digits
                has_financial = True
                break

        if has_financial:
            score += 50  # Prefer rows with financial values

        # Penalize partial matches
        if source_item_clean in line_clean and parts[1].strip() != source_item_clean:
            score -= 20  # Partial match penalty

        candidates.append((score, line))

    # Return the highest scoring candidate
    if candidates:
        candidates.sort(key=lambda x: -x[0])
        return candidates[0][1]

    return None


def parse_extracted_file(filepath: Path) -> dict:
    """Parse extracted BS file."""
    with open(filepath) as f:
        content = f.read()

    result = {
        'rows': [],
        'total_assets': None,
        'total_eq_liab': None,
    }

    for line in content.split('\n'):
        if line.startswith('|') and '|:---|' not in line and 'Source Item' not in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 5:
                source_item = parts[1].replace('**', '')
                canonical = parts[2].replace('**', '').lower()
                values = []
                for v in parts[4:]:
                    v_clean = v.replace(',', '').replace('**', '').replace('-', '0').strip()
                    try:
                        values.append(int(v_clean) if v_clean else 0)
                    except:
                        pass

                if source_item:
                    row = {
                        'source_item': source_item,
                        'canonical': canonical,
                        'values': values
                    }
                    result['rows'].append(row)

                    if 'total_assets' in canonical and 'total_equity' not in canonical:
                        result['total_assets'] = values[0] if values else 0
                    if 'total_equity_and_liabilities' in canonical:
                        result['total_eq_liab'] = values[0] if values else 0

    return result


def get_source_pages(ticker: str, period: str, extraction_manifest: dict) -> list[Path]:
    """Get source markdown pages for a filing."""
    # Build the manifest key
    # Format: TICKER_TICKER_Type_Period (e.g., AABS_AABS_Quarterly_2021-06-30)
    for key, entry in extraction_manifest.get('filings', {}).items():
        if ticker in key and period in key:
            filing_path = Path(entry['filing_path'])
            statement_pages = entry.get('pages', {}).get('statement', [])
            return [filing_path / f"page_{p:03d}.md" for p in statement_pages]
    return []


def load_source_content(source_pages: list[Path], statement_type: str = "balance") -> str:
    """
    Load content from source pages, prioritizing actual statement pages.
    Filters out summary/composition pages that have percentages instead of values.
    """
    actual_statement_pages = []
    summary_pages = []

    for page in source_pages:
        if page.exists():
            with open(page) as f:
                content = f.read()

            # Check if this is the actual statement (has financial values)
            # vs a summary/composition page (has percentages)
            is_actual = False

            # Look for indicators of actual statement
            if 'statement of financial position' in content.lower():
                is_actual = True
            elif 'rupees in' in content.lower():
                is_actual = True
            elif re.search(r'\|\s*\d{1,3}(?:,\d{3})+\s*\|', content):  # Has formatted numbers like 1,234,567
                is_actual = True

            # Check for summary/composition indicators
            if 'composition' in content.lower() and '%' in content:
                is_actual = False

            if is_actual:
                actual_statement_pages.append(content)
            else:
                summary_pages.append(content)

    # Prefer actual statement pages
    if actual_statement_pages:
        return '\n'.join(actual_statement_pages)
    return '\n'.join(summary_pages)


def qc_file(extracted_path: Path, extraction_manifest: dict) -> dict:
    """Run QC on a single extracted file."""
    result = {
        'file': extracted_path.name,
        'extraction_accuracy': {'checked': 0, 'matched': 0, 'mismatched': [], 'not_found': []},
        'source_quality': {'total_assets': None, 'total_eq_liab': None, 'balanced': None, 'diff_pct': None},
    }

    # Parse filename: TICKER_type_period_scope.md
    parts = extracted_path.stem.split('_')
    if len(parts) < 3:
        result['error'] = "Cannot parse filename"
        return result

    ticker = parts[0]
    period_type = parts[1]  # annual, quarterly
    period = parts[2]  # 2021 or 2021-06-30
    if period_type == 'quarterly' and len(parts) > 3:
        period = parts[2]  # Already has the date

    # Get source pages
    source_pages = get_source_pages(ticker, period, extraction_manifest)
    if not source_pages:
        result['error'] = f"No source pages found for {ticker} {period}"
        return result

    source_content = load_source_content(source_pages)
    if not source_content:
        result['error'] = "Could not load source content"
        return result

    # Parse extracted file
    extracted = parse_extracted_file(extracted_path)

    # Check extraction accuracy for key items
    # Focus on reliably matchable items (simple row format in source)
    reliable_canonicals = [
        'property_equipment', 'cash_and_equivalents', 'receivables',
        'inventory', 'payables', 'short_term_debt',
    ]
    # Items with complex source formats (multi-line, may have false negatives)
    complex_canonicals = [
        'total_assets', 'total_equity_and_liabilities',
        'share_capital', 'reserves', 'long_term_debt'
    ]
    key_canonicals = reliable_canonicals + complex_canonicals

    # Track reliable vs complex matches separately
    result['extraction_accuracy']['reliable_checked'] = 0
    result['extraction_accuracy']['reliable_matched'] = 0

    for row in extracted['rows']:
        # Check if this is a key item
        is_reliable = any(k in row['canonical'] for k in reliable_canonicals)
        is_complex = any(k in row['canonical'] for k in complex_canonicals)

        if not is_reliable and not is_complex:
            continue

        result['extraction_accuracy']['checked'] += 1
        if is_reliable:
            result['extraction_accuracy']['reliable_checked'] += 1

        source_line = find_source_row(source_content, row['source_item'])
        if source_line:
            source_vals = extract_numbers_from_row(source_line)
            extracted_val = row['values'][0] if row['values'] else 0

            if extracted_val in source_vals or (extracted_val == 0 and not source_vals):
                result['extraction_accuracy']['matched'] += 1
                if is_reliable:
                    result['extraction_accuracy']['reliable_matched'] += 1
            else:
                result['extraction_accuracy']['mismatched'].append({
                    'source_item': row['source_item'],
                    'canonical': row['canonical'],
                    'extracted': extracted_val,
                    'source_values': source_vals,
                    'is_reliable': is_reliable
                })
        else:
            result['extraction_accuracy']['not_found'].append(row['source_item'])

    # Check source quality (accounting equation)
    result['source_quality']['total_assets'] = extracted['total_assets']
    result['source_quality']['total_eq_liab'] = extracted['total_eq_liab']

    if extracted['total_assets'] and extracted['total_eq_liab']:
        diff = abs(extracted['total_assets'] - extracted['total_eq_liab'])
        diff_pct = 100 * diff / extracted['total_eq_liab']
        result['source_quality']['diff_pct'] = round(diff_pct, 1)
        result['source_quality']['balanced'] = diff_pct <= 5

    return result


def main():
    parser = argparse.ArgumentParser(description="Source-based QC for BS extractions")
    parser.add_argument("--file", type=str, help="QC a single file")
    parser.add_argument("--sample", type=int, default=0, help="QC a sample of N files")
    args = parser.parse_args()

    # Load extraction manifest
    manifest_path = Path("artifacts/stage2/step6_extraction_manifest.json")
    with open(manifest_path) as f:
        extraction_manifest = json.load(f)

    bs_dir = Path("data/extracted_bs")

    if args.file:
        files = [Path(args.file)]
    elif args.sample:
        files = sorted(bs_dir.glob("*.md"))[:args.sample]
    else:
        files = sorted(bs_dir.glob("*.md"))

    stats = {
        'total': 0,
        'extraction_perfect': 0,
        'extraction_issues': 0,
        'reliable_checked': 0,
        'reliable_matched': 0,
        'reliable_mismatched': 0,
        'source_balanced': 0,
        'source_unbalanced': 0,
        'errors': 0,
    }

    issues = []
    reliable_issues = []

    for filepath in files:
        result = qc_file(filepath, extraction_manifest)
        stats['total'] += 1

        if 'error' in result:
            stats['errors'] += 1
            continue

        # Check extraction accuracy
        acc = result['extraction_accuracy']
        if acc['checked'] > 0:
            if not acc['mismatched'] and not acc['not_found']:
                stats['extraction_perfect'] += 1
            else:
                stats['extraction_issues'] += 1
                if len(issues) < 10:
                    issues.append(result)

        # Track reliable items separately
        stats['reliable_checked'] += acc.get('reliable_checked', 0)
        stats['reliable_matched'] += acc.get('reliable_matched', 0)

        # Count reliable mismatches
        for m in acc.get('mismatched', []):
            if m.get('is_reliable'):
                stats['reliable_mismatched'] += 1
                if len(reliable_issues) < 10:
                    reliable_issues.append({
                        'file': result['file'],
                        'mismatch': m
                    })

        # Check source quality
        sq = result['source_quality']
        if sq['balanced'] is True:
            stats['source_balanced'] += 1
        elif sq['balanced'] is False:
            stats['source_unbalanced'] += 1

    print("=" * 70)
    print("SOURCE-BASED BS QC RESULTS")
    print("=" * 70)
    print(f"\nTotal files:                    {stats['total']}")

    print(f"\nEXTRACTION ACCURACY (RELIABLE ITEMS ONLY):")
    print(f"  Items checked:                {stats['reliable_checked']}")
    print(f"  Items matched:                {stats['reliable_matched']}")
    print(f"  Items mismatched:             {stats['reliable_mismatched']}")
    if stats['reliable_checked'] > 0:
        accuracy = 100 * stats['reliable_matched'] / stats['reliable_checked']
        print(f"  Accuracy rate:                {accuracy:.1f}%")

    print(f"\nSOURCE DOCUMENT QUALITY:")
    print(f"  Accounting equation balanced: {stats['source_balanced']}")
    print(f"  Accounting equation fails:    {stats['source_unbalanced']}")
    print(f"\nErrors (no source found):       {stats['errors']}")

    if reliable_issues:
        print("\n" + "-" * 70)
        print("RELIABLE ITEM MISMATCHES (may indicate real extraction errors):")
        for ri in reliable_issues[:10]:
            m = ri['mismatch']
            print(f"  {ri['file']}: {m['source_item']} - extracted={m['extracted']}, source={m['source_values']}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
