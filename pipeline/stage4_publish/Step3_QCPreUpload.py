#!/usr/bin/env python3
"""
Step 4: QC Unified Statements

Validates the unified statements data and generates a detailed manifest
for analysis. Checks for common issues like:
- Balance sheets with incorrect durations
- LTM applied to balance sheets
- Missing expected periods
- 3M quarter availability by ticker

Input:  statements_normalized.jsonl (output from Step2b_NormalizeUnits)
Output: qc_unified_manifest.json (detailed QC results)

Usage:
    python3 Step4_QCUnifiedStatements.py
    python3 Step4_QCUnifiedStatements.py --output qc_report.json
"""

import argparse
import json
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_INPUT = PROJECT_ROOT / "statements_normalized.jsonl"
DEFAULT_OUTPUT = PROJECT_ROOT / "qc_unified_manifest.json"


def parse_period_key(period_key: str) -> dict:
    """Parse period_key like 'annual_2024' or 'quarterly_2025-09-30'."""
    if period_key.startswith('annual_'):
        return {'type': 'annual', 'year': int(period_key.split('_')[1])}
    elif period_key.startswith('quarterly_'):
        date_part = period_key.split('_')[1]
        year, month, day = date_part.split('-')
        return {'type': 'quarterly', 'year': int(year), 'month': int(month), 'date': date_part}
    return {'type': 'unknown'}


def run_qc(input_file: Path) -> dict:
    """Run QC checks and return manifest."""

    manifest = {
        'generated_at': datetime.now().isoformat(),
        'input_file': str(input_file),
        'summary': {},
        'issues': {
            'balance_sheet_wrong_duration': [],
            'ltm_on_balance_sheet': [],
            'duplicate_rows': [],
        },
        'by_ticker': {},
        'quarter_availability': {},
        'duration_by_statement_type': {},
    }

    # Counters
    total_rows = 0
    by_stmt_type = Counter()
    by_duration = Counter()
    by_period_type = Counter()
    duration_by_stmt = defaultdict(Counter)

    # Per-ticker tracking
    ticker_data = defaultdict(lambda: {
        'periods': set(),
        'statement_types': set(),
        '3m_quarters': set(),
        'issues': [],
        'row_count': 0,
    })

    # For duplicate detection
    seen_keys = set()

    print(f"Reading: {input_file}")

    with open(input_file) as f:
        for line_num, line in enumerate(f, 1):
            row = json.loads(line)
            total_rows += 1

            ticker = row.get('ticker')
            stmt_type = row.get('statement_type')
            duration = row.get('period_duration')
            period_type = row.get('period_type')
            period_end = row.get('period_end')
            section = row.get('section')
            field = row.get('canonical_field')

            # Update counters
            by_stmt_type[stmt_type] += 1
            by_duration[duration] += 1
            by_period_type[period_type] += 1
            duration_by_stmt[stmt_type][duration] += 1

            # Update ticker data
            td = ticker_data[ticker]
            td['row_count'] += 1
            td['periods'].add(period_end)
            td['statement_types'].add(stmt_type)

            # Track 3M quarters (only for P&L and Cash Flow, not balance sheet)
            if duration == '3M' and stmt_type in ('profit_loss', 'cash_flow'):
                td['3m_quarters'].add(period_end)

            # Issue: Balance sheet with sub-annual duration
            if stmt_type == 'balance_sheet' and duration in ('3M', '6M', '9M'):
                issue = {
                    'ticker': ticker,
                    'period_end': period_end,
                    'duration': duration,
                    'section': section,
                    'line': line_num,
                }
                manifest['issues']['balance_sheet_wrong_duration'].append(issue)
                td['issues'].append(f"BS with {duration} duration at {period_end}")

            # Issue: LTM on balance sheet
            if stmt_type == 'balance_sheet' and period_type == 'ltm':
                issue = {
                    'ticker': ticker,
                    'period_end': period_end,
                    'line': line_num,
                }
                manifest['issues']['ltm_on_balance_sheet'].append(issue)
                td['issues'].append(f"LTM on balance sheet at {period_end}")

            # Check for duplicates
            key = (ticker, stmt_type, section, period_end, field)
            if key in seen_keys:
                manifest['issues']['duplicate_rows'].append({
                    'ticker': ticker,
                    'stmt_type': stmt_type,
                    'section': section,
                    'period_end': period_end,
                    'field': field,
                    'line': line_num,
                })
            seen_keys.add(key)

            if total_rows % 50000 == 0:
                print(f"  Processed {total_rows:,} rows...")

    print(f"  Total: {total_rows:,} rows")

    # Build summary
    manifest['summary'] = {
        'total_rows': total_rows,
        'total_tickers': len(ticker_data),
        'by_statement_type': dict(by_stmt_type),
        'by_duration': dict(by_duration),
        'by_period_type': dict(by_period_type),
        'issue_counts': {
            'balance_sheet_wrong_duration': len(manifest['issues']['balance_sheet_wrong_duration']),
            'ltm_on_balance_sheet': len(manifest['issues']['ltm_on_balance_sheet']),
            'duplicate_rows': len(manifest['issues']['duplicate_rows']),
        }
    }

    # Duration by statement type
    manifest['duration_by_statement_type'] = {
        stmt: dict(durations) for stmt, durations in duration_by_stmt.items()
    }

    # Build quarter availability matrix
    all_quarters = set()
    for td in ticker_data.values():
        all_quarters.update(td['3m_quarters'])

    sorted_quarters = sorted(all_quarters)

    quarter_matrix = {}
    for ticker, td in ticker_data.items():
        quarters = td['3m_quarters']
        quarter_matrix[ticker] = {
            'count': len(quarters),
            'quarters': sorted(quarters),
            'has_quarters': {q: (q in quarters) for q in sorted_quarters}
        }

    manifest['quarter_availability'] = {
        'all_quarters': sorted_quarters,
        'by_ticker': quarter_matrix,
    }

    # Per-ticker summary (convert sets to lists for JSON)
    manifest['by_ticker'] = {
        ticker: {
            'row_count': td['row_count'],
            'periods': sorted(td['periods']),
            'statement_types': sorted(td['statement_types']),
            '3m_quarter_count': len(td['3m_quarters']),
            '3m_quarters': sorted(td['3m_quarters']),
            'issues': td['issues'],
        }
        for ticker, td in ticker_data.items()
    }

    # Trim large issue lists for the manifest (keep first 100)
    for issue_type in list(manifest['issues'].keys()):
        if len(manifest['issues'][issue_type]) > 100:
            manifest['issues'][issue_type] = manifest['issues'][issue_type][:100]
            manifest['issues'][f'{issue_type}_truncated'] = True

    return manifest


def print_report(manifest: dict):
    """Print summary report to console."""
    summary = manifest['summary']

    print("\n" + "=" * 70)
    print("QC REPORT: UNIFIED STATEMENTS")
    print("=" * 70)

    print(f"\nTotal rows: {summary['total_rows']:,}")
    print(f"Total tickers: {summary['total_tickers']}")

    print("\n--- BY STATEMENT TYPE ---")
    for st, count in sorted(summary['by_statement_type'].items()):
        print(f"  {st}: {count:,}")

    print("\n--- BY DURATION ---")
    for d, count in sorted(summary['by_duration'].items(), key=lambda x: (x[0] is None, x[0] or '')):
        d_display = d if d is not None else "(point-in-time)"
        print(f"  {d_display}: {count:,}")

    print("\n--- BY PERIOD TYPE ---")
    for pt, count in sorted(summary['by_period_type'].items()):
        print(f"  {pt}: {count:,}")

    print("\n--- DURATION BY STATEMENT TYPE ---")
    for stmt, durations in manifest['duration_by_statement_type'].items():
        print(f"  {stmt}:")
        for d, count in sorted(durations.items(), key=lambda x: (x[0] is None, x[0] or '')):
            d_display = d if d is not None else "(point-in-time)"
            # For balance sheets, point-in-time is correct; 3M/6M/9M would be wrong
            flag = " ⚠️" if stmt == 'balance_sheet' and d in ('3M', '6M', '9M') else ""
            if stmt == 'balance_sheet' and d is None:
                flag = " ✓"
            print(f"    {d_display}: {count:,}{flag}")

    print("\n--- ISSUES ---")
    for issue_type, count in summary['issue_counts'].items():
        flag = " ⚠️" if count > 0 else " ✓"
        print(f"  {issue_type}: {count:,}{flag}")

    # Quarter availability summary
    qa = manifest['quarter_availability']
    all_quarters = qa['all_quarters']

    print(f"\n--- 3M QUARTER AVAILABILITY ---")
    print(f"Quarter range: {all_quarters[0] if all_quarters else 'N/A'} to {all_quarters[-1] if all_quarters else 'N/A'}")
    print(f"Total quarters tracked: {len(all_quarters)}")

    # Distribution by count
    count_dist = Counter()
    for ticker, data in qa['by_ticker'].items():
        count_dist[data['count']] += 1

    print("\nTickers by # of 3M quarters (P&L + Cash Flow only):")
    for count in sorted(count_dist.keys(), reverse=True):
        tickers = [t for t, d in qa['by_ticker'].items() if d['count'] == count]
        examples = ', '.join(sorted(tickers)[:5])
        if len(tickers) > 5:
            examples += f" (+{len(tickers)-5} more)"
        print(f"  {count} quarters: {count_dist[count]} tickers - {examples}")


def main():
    parser = argparse.ArgumentParser(description="QC unified statements")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input JSONL file")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output manifest JSON")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: {args.input} not found")
        return

    manifest = run_qc(args.input)

    # Write manifest
    print(f"\nWriting manifest: {args.output}")
    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Print report
    print_report(manifest)

    print(f"\nFull manifest saved to: {args.output}")


if __name__ == "__main__":
    main()
