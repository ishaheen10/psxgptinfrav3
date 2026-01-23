#!/usr/bin/env python3
"""
Step 1b: Deterministic QC on Source Markdown (Pre-Classification)

Runs structural checks on source markdown pages BEFORE LLM classification.
These checks catch OCR issues that LLMs miss because the text "looks readable".

Checks:
1. Concatenated columns - Two numbers in same cell (table parsing failure)
2. Repeated lines - OCR loop producing duplicate content
3. Low unique ratio - Too many duplicate lines
4. Orphaned numbers - Numbers on lines without labels (row misalignment)

Input:  artifacts/stage3/step2_statement_pages.json (pages to check)
        artifacts/stage1/step7_skip_manifest.json (pages to skip)
        markdown_pages/
Output: artifacts/stage2/step1b_deterministic_qc.json

Usage:
    python Step1b_DeterministicQC.py
    python Step1b_DeterministicQC.py --ticker LUCK
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE1_ARTIFACTS, STAGE2_ARTIFACTS, STAGE3_ARTIFACTS

# Input/Output paths
STATEMENT_PAGES_FILE = STAGE3_ARTIFACTS / "step2_statement_pages.json"
SKIP_MANIFEST_FILE = STAGE1_ARTIFACTS / "step7_skip_manifest.json"
OUTPUT_FILE = STAGE2_ARTIFACTS / "step1b_deterministic_qc.json"

# Thresholds
CONCAT_MIN_MATCHES = 3        # Minimum concatenation matches to flag
REPEAT_LINE_THRESHOLD = 8     # Consecutive repeated lines
UNIQUE_RATIO_THRESHOLD = 0.30 # Minimum ratio of unique lines
ORPHAN_NUMBER_THRESHOLD = 5   # Minimum orphaned number lines to flag

# Patterns
# Two comma-formatted numbers separated by whitespace
CONCAT_PATTERN = re.compile(r'\d{1,3}(?:,\d{3})+\s+\d{1,3}(?:,\d{3})+')

# Line that's mostly just a number (orphaned from its label)
ORPHAN_NUMBER_PATTERN = re.compile(r'^\s*\|?\s*[\d,\.\(\)\-\$]+\s*\|?\s*$')

# Large number pattern for context
LARGE_NUMBER_PATTERN = re.compile(r'\d{1,3}(?:,\d{3}){2,}')


def load_skip_manifest() -> set:
    """Load set of pages to skip (urdu, edge pages)."""
    if not SKIP_MANIFEST_FILE.exists():
        return set()

    with open(SKIP_MANIFEST_FILE) as f:
        data = json.load(f)

    return {p['relative_path'] for p in data.get('pages', [])}


def load_statement_pages(ticker_filter: str = None) -> list:
    """Load statement pages from manifest."""
    if not STATEMENT_PAGES_FILE.exists():
        print(f"Statement pages file not found: {STATEMENT_PAGES_FILE}")
        return []

    with open(STATEMENT_PAGES_FILE) as f:
        manifest = json.load(f)

    pages = []

    for ticker, periods in manifest.items():
        if ticker_filter and ticker.upper() != ticker_filter.upper():
            continue

        for period, sections in periods.items():
            for section, stmts in sections.items():
                for stmt_type in ['PL', 'BS', 'CF']:
                    if stmt_type in stmts:
                        for page_num in stmts[stmt_type]:
                            pages.append({
                                'ticker': ticker,
                                'period': period,
                                'section': section,
                                'stmt_type': stmt_type,
                                'page_num': page_num
                            })

    return pages


def find_markdown_path(ticker: str, period: str, page_num: int) -> Path | None:
    """Find the markdown file path for a statement page."""
    ticker_dir = MARKDOWN_ROOT / ticker
    if not ticker_dir.exists():
        return None

    # Determine year from period
    if period.startswith('annual_'):
        year = period.replace('annual_', '')
    else:
        # quarterly_YYYY-MM-DD -> YYYY
        year = period.split('_')[1][:4]

    year_dir = ticker_dir / year
    if not year_dir.exists():
        return None

    # Find matching document folder
    period_normalized = period.replace('_', '-').replace('annual-', 'Annual_').replace('quarterly-', 'Quarterly_')

    for doc_dir in year_dir.iterdir():
        if not doc_dir.is_dir():
            continue

        # Match document folder name patterns
        doc_name = doc_dir.name.lower()
        if 'annual' in period.lower() and 'annual' in doc_name:
            page_file = doc_dir / f"page_{page_num:03d}.md"
            if page_file.exists():
                return page_file
        elif 'quarterly' in period.lower():
            # Extract date from period
            period_date = period.replace('quarterly_', '')
            if period_date.replace('-', '') in doc_name.replace('-', '').replace('_', ''):
                page_file = doc_dir / f"page_{page_num:03d}.md"
                if page_file.exists():
                    return page_file

    # Fallback: search all doc dirs
    for doc_dir in year_dir.iterdir():
        if doc_dir.is_dir():
            page_file = doc_dir / f"page_{page_num:03d}.md"
            if page_file.exists():
                return page_file

    return None


def check_concatenated_columns(content: str) -> dict:
    """Check for concatenated column values (OCR table parsing failure)."""
    matches = CONCAT_PATTERN.findall(content)

    return {
        'issue': 'concatenated_columns',
        'detected': len(matches) >= CONCAT_MIN_MATCHES,
        'count': len(matches),
        'examples': matches[:5],
        'severity': 'error' if len(matches) >= 10 else 'warning' if len(matches) >= CONCAT_MIN_MATCHES else 'ok'
    }


def check_repeated_lines(content: str) -> dict:
    """Check for repeated consecutive lines (OCR loop)."""
    lines = content.strip().split('\n')
    non_empty = [l.strip() for l in lines if l.strip()]

    if len(non_empty) < 10:
        return {'issue': 'repeated_lines', 'detected': False, 'max_run': 0, 'severity': 'ok'}

    max_run = 1
    current_run = 1
    max_run_line = ""
    prev = None

    for line in non_empty:
        if line == prev:
            current_run += 1
            if current_run > max_run:
                max_run = current_run
                max_run_line = line[:50]
        else:
            current_run = 1
        prev = line

    return {
        'issue': 'repeated_lines',
        'detected': max_run >= REPEAT_LINE_THRESHOLD,
        'max_run': max_run,
        'example': max_run_line if max_run >= REPEAT_LINE_THRESHOLD else '',
        'severity': 'error' if max_run >= REPEAT_LINE_THRESHOLD else 'ok'
    }


def check_unique_ratio(content: str) -> dict:
    """Check for low unique line ratio (duplicate content)."""
    lines = content.strip().split('\n')
    non_empty = [l.strip() for l in lines if l.strip()]

    if len(non_empty) < 10:
        return {'issue': 'low_unique_ratio', 'detected': False, 'ratio': 1.0, 'severity': 'ok'}

    unique = set(non_empty)
    ratio = len(unique) / len(non_empty)

    return {
        'issue': 'low_unique_ratio',
        'detected': ratio < UNIQUE_RATIO_THRESHOLD,
        'ratio': round(ratio, 3),
        'unique_lines': len(unique),
        'total_lines': len(non_empty),
        'severity': 'error' if ratio < UNIQUE_RATIO_THRESHOLD else 'ok'
    }


def check_orphaned_numbers(content: str) -> dict:
    """Check for orphaned number rows (row misalignment)."""
    lines = content.strip().split('\n')

    orphaned = []
    for i, line in enumerate(lines):
        # Line that's mostly just numbers with no text label
        if ORPHAN_NUMBER_PATTERN.match(line):
            # Verify it has a large number
            if LARGE_NUMBER_PATTERN.search(line):
                orphaned.append({'line': i + 1, 'content': line.strip()[:60]})

    return {
        'issue': 'orphaned_numbers',
        'detected': len(orphaned) >= ORPHAN_NUMBER_THRESHOLD,
        'count': len(orphaned),
        'examples': orphaned[:5],
        'severity': 'warning' if len(orphaned) >= ORPHAN_NUMBER_THRESHOLD else 'ok'
    }


def check_page(page_info: dict, skip_set: set) -> dict | None:
    """Run all checks on a single page."""
    # Find the markdown file
    md_path = find_markdown_path(
        page_info['ticker'],
        page_info['period'],
        page_info['page_num']
    )

    if not md_path:
        return None

    # Check if in skip list
    try:
        rel_path = str(md_path.relative_to(MARKDOWN_ROOT))
    except ValueError:
        rel_path = str(md_path)

    if rel_path in skip_set:
        return None  # Skip urdu/edge pages

    # Read content
    try:
        content = md_path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return None

    # Run checks
    checks = {
        'concat': check_concatenated_columns(content),
        'repeated': check_repeated_lines(content),
        'unique': check_unique_ratio(content),
        'orphaned': check_orphaned_numbers(content),
    }

    # Determine overall status
    issues = []
    for check_name, result in checks.items():
        if result['detected']:
            issues.append({
                'type': result['issue'],
                'severity': result['severity'],
                'details': {k: v for k, v in result.items() if k not in ['issue', 'detected', 'severity']}
            })

    return {
        'ticker': page_info['ticker'],
        'period': page_info['period'],
        'section': page_info['section'],
        'stmt_type': page_info['stmt_type'],
        'page_num': page_info['page_num'],
        'relative_path': rel_path,
        'status': 'fail' if any(i['severity'] == 'error' for i in issues) else 'warn' if issues else 'pass',
        'issues': issues
    }


def main():
    parser = argparse.ArgumentParser(description="Deterministic QC on source markdown")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 1b: DETERMINISTIC QC (Pre-Classification)")
    print("=" * 70)
    print()

    # Load skip manifest
    skip_set = load_skip_manifest()
    print(f"Pages in skip manifest: {len(skip_set)}")

    # Load statement pages
    pages = load_statement_pages(args.ticker)
    print(f"Statement pages to check: {len(pages)}")

    if args.ticker:
        print(f"Filtered to ticker: {args.ticker}")

    if not pages:
        print("No pages to check")
        return

    # Process pages
    results = []
    stats = {
        'total': 0,
        'pass': 0,
        'warn': 0,
        'fail': 0,
        'skipped': 0,
        'not_found': 0,
        'by_issue': defaultdict(int)
    }

    print()
    print("Running checks...")

    for i, page_info in enumerate(pages):
        if (i + 1) % 500 == 0:
            print(f"  Processed {i + 1}/{len(pages)}...")

        result = check_page(page_info, skip_set)

        if result is None:
            stats['skipped'] += 1
            continue

        stats['total'] += 1
        stats[result['status']] += 1

        for issue in result['issues']:
            stats['by_issue'][issue['type']] += 1

        if result['issues']:  # Only store pages with issues
            results.append(result)

    # Sort by severity
    results.sort(key=lambda x: (0 if x['status'] == 'fail' else 1, -len(x['issues'])))

    # Write output
    output = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_checked': stats['total'],
            'pass': stats['pass'],
            'warn': stats['warn'],
            'fail': stats['fail'],
            'skipped': stats['skipped'],
            'issues_by_type': dict(stats['by_issue'])
        },
        'pages_with_issues': results
    }

    with open(args.output, 'w') as f:
        json.dump(output, f, indent=2)

    # Print summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total checked:  {stats['total']}")
    print(f"  Pass:         {stats['pass']}")
    print(f"  Warning:      {stats['warn']}")
    print(f"  Fail:         {stats['fail']}")
    print(f"  Skipped:      {stats['skipped']} (urdu/edge pages)")

    if stats['by_issue']:
        print()
        print("Issues by type:")
        for issue_type, count in sorted(stats['by_issue'].items(), key=lambda x: -x[1]):
            print(f"  {issue_type}: {count}")

    if results:
        print()
        print("Top 15 pages with issues:")
        for r in results[:15]:
            issues_str = ', '.join(f"{i['type']}({i['severity']})" for i in r['issues'])
            print(f"  [{r['status'].upper()}] {r['ticker']}_{r['period']}_{r['section']} p{r['page_num']}: {issues_str}")

    print()
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
