#!/usr/bin/env python3
"""
Step 2: QC Filing Coverage.

Checks whether all expected quarterly/annual filings are present.
Run immediately after download to identify gaps before processing.

Input:  markdown_pages/<ticker>/<year>/<filing>/
Output: artifacts/stage1/step2_qc_filing_coverage.json

Usage:
    python Step2_QCFilingCoverage.py
    python Step2_QCFilingCoverage.py --ticker SYS
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE1_ARTIFACTS

OUTPUT_FILE = STAGE1_ARTIFACTS / "step2_qc_filing_coverage.json"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"

# Load fiscal year end from tickers100.json
TICKER_FYE = {}
if TICKERS_FILE.exists():
    with open(TICKERS_FILE) as f:
        for t in json.load(f):
            fiscal_period = t.get('fiscal_period', '06-30')
            fye_month = int(fiscal_period.split('-')[0])
            TICKER_FYE[t['Symbol']] = fye_month


def parse_filing_info(filing_name: str) -> dict | None:
    """Parse filing folder name like 'SYS_Quarterly_2025-03-31' or 'SYS_Annual_2024'."""
    # Quarterly: TICKER_Quarterly_YYYY-MM-DD
    match = re.match(r'([A-Z]+)_Quarterly_(\d{4})-(\d{2})-(\d{2})', filing_name)
    if match:
        return {
            'ticker': match.group(1),
            'type': 'quarterly',
            'year': int(match.group(2)),
            'month': int(match.group(3)),
            'day': int(match.group(4)),
            'period_end': f"{match.group(2)}-{match.group(3)}-{match.group(4)}"
        }

    # Annual: TICKER_Annual_YYYY
    match = re.match(r'([A-Z]+)_Annual_(\d{4})', filing_name)
    if match:
        return {
            'ticker': match.group(1),
            'type': 'annual',
            'year': int(match.group(2)),
            'month': None,
            'day': None,
            'period_end': match.group(2)
        }

    return None


def detect_fiscal_year_end(filings: list) -> int:
    """Detect fiscal year end month from filing patterns."""
    quarterly_months = set(f['month'] for f in filings if f['type'] == 'quarterly')

    has_sep_quarterly = 9 in quarterly_months
    has_dec_quarterly = 12 in quarterly_months
    has_june_quarterly = 6 in quarterly_months

    if has_sep_quarterly or has_dec_quarterly:
        return 6  # June FYE
    if has_june_quarterly:
        return 12  # December FYE

    return 6  # Default to June


def analyze_ticker(ticker: str, ticker_dir: Path) -> dict:
    """Analyze filing coverage for a single ticker."""
    filings = []

    for year_dir in sorted(ticker_dir.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue

        for filing_dir in year_dir.iterdir():
            if not filing_dir.is_dir():
                continue

            info = parse_filing_info(filing_dir.name)
            if info:
                info['path'] = str(filing_dir.relative_to(PROJECT_ROOT))
                filings.append(info)

    if not filings:
        return {'ticker': ticker, 'filings': [], 'gaps': [], 'status': 'no_filings'}

    fiscal_year_end = TICKER_FYE.get(ticker) or detect_fiscal_year_end(filings)

    gaps = []
    years_to_check = [2024, 2025]

    for year in years_to_check:
        year_filings = [f for f in filings if f['year'] == year]
        quarterly = [f for f in year_filings if f['type'] == 'quarterly']

        if not quarterly:
            continue  # No quarterly filings for this year

        quarterly_months = sorted(f['month'] for f in quarterly)
        latest_month = max(quarterly_months)
        annual_month = fiscal_year_end
        standard_months = [3, 6, 9, 12]

        for month in standard_months:
            if month >= latest_month:
                continue
            if month == annual_month:
                continue
            if month not in quarterly_months:
                month_names = {3: 'Mar', 6: 'Jun', 9: 'Sep', 12: 'Dec'}
                quarter_name = month_names.get(month, f'Month {month}')
                day = 30 if month in (6, 9) else 31

                gaps.append({
                    'year': year,
                    'month': month,
                    'quarter': quarter_name,
                    'expected_filing': f"{ticker}_Quarterly_{year}-{month:02d}-{day}",
                })

    return {
        'ticker': ticker,
        'fiscal_year_end': fiscal_year_end,
        'years_covered': sorted(set(f['year'] for f in filings)),
        'total_filings': len(filings),
        'quarterly_count': len([f for f in filings if f['type'] == 'quarterly']),
        'annual_count': len([f for f in filings if f['type'] == 'annual']),
        'gaps': gaps,
        'status': 'has_gaps' if gaps else 'complete'
    }


def main():
    parser = argparse.ArgumentParser(description="QC filing coverage")
    parser.add_argument("--ticker", help="Check single ticker")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 1 STEP 2: QC FILING COVERAGE")
    print("=" * 70)
    print()

    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = sorted([d.name for d in MARKDOWN_ROOT.iterdir()
                         if d.is_dir() and not d.name.startswith('.')])

    print(f"Checking {len(tickers)} tickers for filing completeness...")
    print()

    results = []
    tickers_with_gaps = []

    for ticker in tickers:
        ticker_dir = MARKDOWN_ROOT / ticker
        if not ticker_dir.exists():
            continue

        result = analyze_ticker(ticker, ticker_dir)
        results.append(result)

        if result['gaps']:
            tickers_with_gaps.append(ticker)
            print(f"  {ticker}: {len(result['gaps'])} gaps")

    # Write output
    results_with_gaps = [r for r in results if r['gaps']]
    manifest = {
        'generated_at': datetime.now().isoformat(),
        'tickers_checked': len(results),
        'tickers_with_gaps': len(results_with_gaps),
        'gaps': [
            {
                'ticker': r['ticker'],
                'missing': [g['quarter'] + ' ' + str(g['year']) for g in r['gaps']],
            }
            for r in results_with_gaps
        ]
    }

    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Tickers checked: {len(results)}")
    if tickers_with_gaps:
        print(f"Tickers with gaps: {len(tickers_with_gaps)}")
        print()
        print("ACTION: Download missing filings before proceeding")
    else:
        print("All filings present - ready to proceed")

    print()
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
