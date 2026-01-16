#!/usr/bin/env python3
"""
Step 5: JSONify CF Extractions with Best-Source Selection

Converts extracted_cf markdown files to JSON with:
1. Period normalization (e.g., "3M Mar 2024" -> "2024-03-31")
2. Multi-filing deduplication: prefer latest filing that passes QC
3. Source tracing: each period records which filing it came from

Input:  data/extracted_cf/*.md
        artifacts/stage3/step4_cf_qc_results.json
Output: data/json_cf/{TICKER}.json

Usage:
    python3 Step5_JSONifyCF.py                    # Process all
    python3 Step5_JSONifyCF.py --ticker ABL       # Single ticker
    python3 Step5_JSONifyCF.py --verbose          # Show details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "extracted_cf"
OUTPUT_DIR = PROJECT_ROOT / "data" / "json_cf"
QC_RESULTS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_cf_qc_results.json"

# Month name to number mapping
MONTH_MAP = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}

# Month to last day mapping (non-leap year)
MONTH_DAYS = {
    1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
}

# Standard unit for output (all values normalized to thousands)
STANDARD_UNIT = "thousands"


def normalize_value(value: float, unit_type: str) -> float:
    """
    Normalize a value to thousands.
    - rupees/Rupees: divide by 1000
    - millions: multiply by 1000
    - thousands: keep as is
    """
    if value is None:
        return None

    unit_lower = unit_type.lower().strip()

    # Handle various unit formats
    if unit_lower in ('rupees', 'rupee'):
        return value / 1000.0
    elif unit_lower == 'millions':
        return value * 1000.0
    elif 'thousands' in unit_lower:
        return value  # Already in thousands
    else:
        # Unknown unit - assume thousands (most CF statements are in thousands)
        return value


def parse_period_column(col: str) -> dict | None:
    """
    Parse a period column header like "3M Mar 2024" or "12M Dec 2023".
    Returns dict with: duration, month, year, period_end (YYYY-MM-DD)
    """
    col = col.strip()

    # Pattern: "3M Mar 2024", "12M Dec 2023", "9M Sep 2024"
    match = re.match(r'(\d+)M\s+(\w+)\s+(\d{4})', col)
    if match:
        duration = f"{match.group(1)}M"
        month_str = match.group(2).lower()
        year = int(match.group(3))

        month = MONTH_MAP.get(month_str)
        if not month:
            return None

        # Get last day of month
        day = MONTH_DAYS[month]
        if month == 2 and year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            day = 29  # Leap year

        period_end = f"{year}-{month:02d}-{day:02d}"

        return {
            'duration': duration,
            'month': match.group(2),
            'year': year,
            'period_end': period_end,
            'original': col,
        }

    return None


def parse_filename(filename: str) -> dict | None:
    """
    Parse filename like "ABL_quarterly_2024-03-31_unconsolidated.md"
    Returns dict with: ticker, period_type, filing_date, consolidation
    """
    name = filename.replace('.md', '')
    parts = name.split('_')

    if len(parts) < 3:
        return None

    ticker = parts[0]
    period_type = parts[1]  # 'annual' or 'quarterly'

    if period_type == 'annual':
        # ABL_annual_2024_unconsolidated
        if len(parts) < 4:
            return None
        year = parts[2]
        consolidation = parts[3]
        filing_date = f"{year}-12-31"
    else:
        # ABL_quarterly_2024-03-31_unconsolidated
        if len(parts) < 4:
            return None
        filing_date = parts[2]
        consolidation = parts[3]

    return {
        'ticker': ticker,
        'period_type': period_type,
        'filing_date': filing_date,
        'consolidation': consolidation,
        'filename': filename,
    }


def parse_number(s: str) -> float | None:
    """Parse a number from the table - parentheses mean negative."""
    if not s or s.strip() in ['', '-', '—', 'N/A', 'n/a']:
        return None

    s = s.strip().replace('**', '')  # Remove bold markers
    s = s.replace(',', '')  # Remove commas

    # Parentheses indicate negative
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]

    try:
        return float(s)
    except ValueError:
        return None


def parse_markdown_file(filepath: Path) -> dict | None:
    """
    Parse an extracted_cf markdown file.
    Returns dict with metadata and rows.
    """
    content = filepath.read_text()
    lines = content.split('\n')

    result = {
        'unit_type': 'thousands',  # default
        'periods': [],  # List of period info dicts
        'rows': [],  # List of {canonical, source, ref, values: {period_end: value}}
    }

    # Parse header for unit type
    for line in lines:
        if line.startswith('UNIT_TYPE:'):
            result['unit_type'] = line.split(':', 1)[1].strip()
            break

    # Find and parse the table
    in_table = False
    headers = []

    for line in lines:
        line = line.strip()
        if not line.startswith('|'):
            continue

        # Parse header row
        if '---' in line:
            in_table = True
            continue

        parts = [p.strip() for p in line.split('|')]
        parts = [p for p in parts if p]  # Remove empty parts

        if not in_table:
            # This is the header row
            if len(parts) >= 4:
                headers = parts
                # Parse period columns (everything after Ref)
                for i in range(3, len(parts)):
                    period_info = parse_period_column(parts[i])
                    if period_info:
                        result['periods'].append(period_info)
            continue

        # Data row
        if len(parts) < 4:
            continue

        source_item = parts[0].replace('**', '')
        canonical = parts[1].replace('**', '')
        ref = parts[2].replace('**', '')

        # Skip header-like rows
        if 'Source Item' in source_item or 'Canonical' in canonical:
            continue

        # Skip empty rows
        if not canonical or canonical.strip() == '':
            continue

        # Parse values for each period
        values = {}
        for i, period_info in enumerate(result['periods']):
            col_idx = 3 + i
            if col_idx < len(parts):
                val = parse_number(parts[col_idx])
                if val is not None:
                    key = (period_info['period_end'], period_info['duration'])
                    values[key] = val

        if values:
            result['rows'].append({
                'canonical': canonical,
                'source': source_item,
                'ref': ref,
                'values': values,
            })

    return result


def load_qc_results() -> dict:
    """Load QC results and return dict of filename -> pass/fail."""
    qc_status = {}

    if not QC_RESULTS_FILE.exists():
        print(f"Warning: QC results not found at {QC_RESULTS_FILE}")
        return qc_status

    with open(QC_RESULTS_FILE) as f:
        data = json.load(f)

    for result in data.get('results', []):
        filename = result['file']
        status = result.get('status', 'unknown')
        if status == 'pass':
            qc_status[filename] = 'pass'
        elif result.get('formula_total', 0) == 0:
            qc_status[filename] = 'no_formulas'
        else:
            qc_status[filename] = 'fail'

    return qc_status


def is_current_period(candidate: dict) -> bool:
    """
    Determine if the period is the "current" period in the filing.
    Current period: period_end is within ~13 months of filing_date
    """
    try:
        period_end = datetime.strptime(candidate['period_end'], '%Y-%m-%d')
        filing_date = datetime.strptime(candidate['filing_date'], '%Y-%m-%d')
        months_diff = (filing_date.year - period_end.year) * 12 + (filing_date.month - period_end.month)
        return months_diff <= 13
    except:
        return True


def select_best_source(candidates: list[dict], qc_status: dict) -> dict:
    """
    Given multiple candidates for the same period, select the best one.
    Preference: current period > passes QC > latest filing date
    """
    if len(candidates) == 1:
        return candidates[0]

    # Separate into current period vs prior-year comparison
    current_period = [c for c in candidates if is_current_period(c)]
    prior_year = [c for c in candidates if not is_current_period(c)]

    # Prefer current period sources
    pool = current_period if current_period else prior_year
    pool_sorted = sorted(pool, key=lambda x: x['filing_date'], reverse=True)

    # First try to find a passing one
    for c in pool_sorted:
        status = qc_status.get(c['filename'], 'unknown')
        if status in ('pass', 'no_formulas'):
            return c

    return pool_sorted[0]


def process_ticker(ticker: str, files: list[Path], qc_status: dict, verbose: bool = False) -> dict:
    """
    Process all files for a single ticker and build the unified JSON.
    """
    period_candidates = defaultdict(lambda: defaultdict(list))

    for filepath in files:
        file_info = parse_filename(filepath.name)
        if not file_info:
            continue

        parsed = parse_markdown_file(filepath)
        if not parsed or not parsed['rows']:
            continue

        consolidation = file_info['consolidation']
        filing_date = file_info['filing_date']

        for period_info in parsed['periods']:
            period_end = period_info['period_end']
            duration = period_info['duration']
            period_key = (period_end, duration)

            period_rows = []
            for row in parsed['rows']:
                if period_key in row['values']:
                    period_rows.append({
                        'canonical': row['canonical'],
                        'source': row['source'],
                        'ref': row['ref'],
                        'value': row['values'][period_key],
                    })

            if period_rows:
                candidate = {
                    'filename': filepath.name,
                    'filing_date': filing_date,
                    'period_end': period_end,
                    'duration': duration,
                    'unit_type': parsed['unit_type'],
                    'rows': period_rows,
                }
                period_candidates[consolidation][period_key].append(candidate)

    # Select best source for each period
    result = {
        'ticker': ticker,
        'periods': [],
    }

    for consolidation in sorted(period_candidates.keys()):
        for period_key in sorted(period_candidates[consolidation].keys()):
            period_end, duration = period_key
            candidates = period_candidates[consolidation][period_key]
            best = select_best_source(candidates, qc_status)

            qc_result = qc_status.get(best['filename'], 'unknown')

            # Normalize values to standard unit (thousands)
            normalized_values = {}
            for row in best['rows']:
                canonical = row['canonical']
                raw_value = row['value']
                normalized_values[canonical] = normalize_value(raw_value, best['unit_type'])

            period_record = {
                'period_end': period_end,
                'duration': duration,
                'consolidation': consolidation,
                'unit_type': STANDARD_UNIT,
                'source_filing': best['filename'],
                'source_qc': qc_result,
                'values': normalized_values,
            }

            if len(candidates) > 1:
                period_record['alternate_sources'] = [
                    {
                        'filename': c['filename'],
                        'qc': qc_status.get(c['filename'], 'unknown'),
                    }
                    for c in candidates if c['filename'] != best['filename']
                ]

            result['periods'].append(period_record)

            if verbose:
                alt_count = len(candidates) - 1
                alt_str = f" (selected from {len(candidates)} sources)" if alt_count > 0 else ""
                print(f"  {consolidation} {period_end} {duration}: {best['filename']} [{qc_result}]{alt_str}")

    return result


def main():
    parser = argparse.ArgumentParser(description="JSONify CF extractions with best-source selection")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: JSONify CF EXTRACTIONS")
    print("=" * 70)

    if not INPUT_DIR.exists():
        print(f"ERROR: Input directory not found: {INPUT_DIR}")
        return

    # Load QC results
    print(f"\nLoading QC results from {QC_RESULTS_FILE}...")
    qc_status = load_qc_results()
    print(f"  Loaded QC status for {len(qc_status)} files")

    # Group files by ticker
    files = sorted(INPUT_DIR.glob("*.md"))
    files_by_ticker = defaultdict(list)

    for f in files:
        file_info = parse_filename(f.name)
        if file_info:
            files_by_ticker[file_info['ticker']].append(f)

    if args.ticker:
        if args.ticker not in files_by_ticker:
            print(f"ERROR: No files found for ticker {args.ticker}")
            return
        files_by_ticker = {args.ticker: files_by_ticker[args.ticker]}

    print(f"\nProcessing {len(files_by_ticker)} tickers...\n")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process each ticker
    stats = {
        'tickers': 0,
        'periods_total': 0,
        'periods_pass': 0,
        'periods_fail': 0,
    }

    for ticker in sorted(files_by_ticker.keys()):
        ticker_files = files_by_ticker[ticker]

        if args.verbose:
            print(f"\n{ticker} ({len(ticker_files)} files):")

        result = process_ticker(ticker, ticker_files, qc_status, args.verbose)

        stats['tickers'] += 1
        stats['periods_total'] += len(result['periods'])

        for period in result['periods']:
            if period['source_qc'] in ('pass', 'no_formulas'):
                stats['periods_pass'] += 1
            else:
                stats['periods_fail'] += 1

        # Write output
        output_file = OUTPUT_DIR / f"{ticker}.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)

        if not args.verbose:
            print(f"  {ticker}: {len(result['periods'])} periods")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Tickers processed:     {stats['tickers']}")
    print(f"  Total periods:         {stats['periods_total']}")
    print(f"  Periods (pass QC):     {stats['periods_pass']}")
    print(f"  Periods (fail QC):     {stats['periods_fail']}")
    print()
    print(f"Output: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
