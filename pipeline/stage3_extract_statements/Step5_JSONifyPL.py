#!/usr/bin/env python3
"""
Step 5: JSONify P&L Extractions (QC-Optimized Structure)

Converts extracted_pl markdown files to JSON optimized for QC lookups:
- Each period as an object with period_end, duration, consolidation
- Values keyed by canonical name for easy lookup
- source_item preserved for each value
- Includes source file and QC status

Input:  data/extracted_pl/*.md
Output: data/json_pl/{TICKER}.json

Usage:
    python3 Step5_JSONifyPL.py                    # Process all
    python3 Step5_JSONifyPL.py --ticker ABL       # Single ticker
    python3 Step5_JSONifyPL.py --verbose          # Show details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "extracted_pl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "json_pl"
QC_RESULTS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_qc_pl_extraction.json"
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
PDF_BASE_URL = "https://source.psxgpt.com/PDF_PAGES"

# Load statement pages manifest
STATEMENT_PAGES = {}
if STATEMENT_PAGES_FILE.exists():
    with open(STATEMENT_PAGES_FILE) as f:
        STATEMENT_PAGES = json.load(f)


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
            'duration_months': int(match.group(1)),
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
        # For annual, filing_date is end of fiscal year (approximate as Dec 31)
        filing_date = f"{year}-12-31"
    else:
        # ABL_quarterly_2024-03-31_unconsolidated
        if len(parts) < 4:
            return None
        filing_date = parts[2]  # e.g., "2024-03-31"
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
    Parse an extracted_pl markdown file.
    Returns dict with metadata and rows.
    """
    content = filepath.read_text()
    lines = content.split('\n')

    result = {
        'unit_type': 'thousands',  # default
        'periods': [],  # List of period info dicts
        'rows': [],  # List of {canonical, source, ref, formula, values: {period_key: value}}
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
        # Remove leading/trailing empty parts from pipe separators, but keep middle empties
        # "| A | | B |" -> ['', 'A', '', 'B', ''] -> ['A', '', 'B']
        if parts and parts[0] == '':
            parts = parts[1:]
        if parts and parts[-1] == '':
            parts = parts[:-1]

        if not in_table:
            # This is the header row
            # Expected: Source Item | Canonical | Ref | Period1 | Period2 | ...
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
        ref_raw = parts[2].replace('**', '')

        # Skip header-like rows
        if 'Source Item' in source_item or 'Canonical' in canonical:
            continue

        # Skip empty rows (separators)
        if not canonical or canonical.strip() == '':
            continue

        # Parse ref for formula
        ref_name = ref_raw
        formula = None
        if '=' in ref_raw:
            ref_parts = ref_raw.split('=', 1)
            ref_name = ref_parts[0].strip()
            formula = ref_parts[1].strip()

        # Parse values for each period
        values = {}
        for i, period_info in enumerate(result['periods']):
            col_idx = 3 + i
            if col_idx < len(parts):
                val = parse_number(parts[col_idx])
                if val is not None:
                    key = (period_info['period_end'], period_info['duration'])
                    values[key] = val

        if values:  # Only add rows with at least one value
            result['rows'].append({
                'canonical': canonical,
                'source_item': source_item,
                'ref': ref_name,
                'formula': formula,
                'is_calculated': formula is not None,
                'values': values,
            })

    return result


def load_qc_results() -> dict:
    """Load QC results and return dict of filename -> status."""
    qc_status = {}

    if not QC_RESULTS_FILE.exists():
        print(f"Warning: QC results not found at {QC_RESULTS_FILE}")
        return qc_status

    with open(QC_RESULTS_FILE) as f:
        data = json.load(f)

    # QC results file has 'files' list with per-file results
    for result in data.get('files', []):
        filename = result['file']
        status = result.get('status', 'unknown')
        if status == 'pass':
            qc_status[filename] = 'pass'
        elif result.get('formulas', 0) == 0:
            qc_status[filename] = 'no_formulas'
        else:
            qc_status[filename] = 'fail'

    return qc_status


def is_current_period(candidate: dict) -> bool:
    """
    Determine if the period is the "current" period in the filing (not a prior-year comparison).

    Current period: period_end is within ~13 months of filing_date
    Prior year: period_end is 13+ months before filing_date

    This helps avoid using restated prior-year values from newer filings.
    """
    try:
        period_end = datetime.strptime(candidate['period_end'], '%Y-%m-%d')
        filing_date = datetime.strptime(candidate['filing_date'], '%Y-%m-%d')

        # Calculate months difference
        months_diff = (filing_date.year - period_end.year) * 12 + (filing_date.month - period_end.month)

        # If period_end is within 13 months of filing_date, it's likely the current period
        # (13 months allows for some lag in filing dates)
        return months_diff <= 13
    except:
        return True  # Default to treating as current if we can't parse dates


def select_best_source(candidates: list[dict], qc_status: dict) -> dict:
    """
    Given multiple candidates for the same period, select the best one.

    Preference order:
    1. Current period that passes QC (with taxation data preferred)
    2. Prior-year comparison that passes QC (if current fails) - labeled as fallback
    3. Current period (even if failing) as last resort

    Each candidate has: period_end, duration, consolidation, filing_date, source_file, values, etc.
    """
    if len(candidates) == 1:
        best = candidates[0]
        best['source_type'] = 'primary' if is_current_period(best) else 'prior_year'
        return best

    # Separate into current period vs prior-year comparison
    current_period = [c for c in candidates if is_current_period(c)]
    prior_year = [c for c in candidates if not is_current_period(c)]

    def passes_qc(c):
        """Check if candidate passes QC."""
        status = qc_status.get(c.get('source_file', ''), 'unknown')
        return status in ('pass', 'no_formulas')

    def score_candidate(c):
        """Score a candidate - higher is better."""
        score = 0
        # Prefer candidates with taxation data
        if c.get('values', {}).get('taxation', {}).get('value') is not None:
            score += 100
        # Prefer QC pass
        if passes_qc(c):
            score += 50
        return score

    # Sort both lists by score then filing date
    current_sorted = sorted(current_period,
                            key=lambda x: (score_candidate(x), x.get('filing_date', '')),
                            reverse=True) if current_period else []
    prior_sorted = sorted(prior_year,
                          key=lambda x: (score_candidate(x), x.get('filing_date', '')),
                          reverse=True) if prior_year else []

    # 1. First choice: current period that passes QC
    current_passing = [c for c in current_sorted if passes_qc(c)]
    if current_passing:
        best = current_passing[0]
        best['source_type'] = 'primary'
        return best

    # 2. Second choice: prior-year that passes QC (if no current passes)
    prior_passing = [c for c in prior_sorted if passes_qc(c)]
    if prior_passing:
        best = prior_passing[0]
        best['source_type'] = 'prior_year_fallback'  # Label as fallback
        return best

    # 3. Last resort: best current period even if failing
    if current_sorted:
        best = current_sorted[0]
        best['source_type'] = 'primary'
        return best

    # 4. Final fallback: any prior year
    if prior_sorted:
        best = prior_sorted[0]
        best['source_type'] = 'prior_year_fallback'
        return best

    # Should never reach here
    return candidates[0]


def get_source_pages(ticker: str, period_type: str, filing_date: str, consolidation: str) -> dict:
    """
    Look up source pages from step2_statement_pages.json.
    """
    if period_type == 'annual':
        year = filing_date[:4]
        filing_period = f"annual_{year}"
        folder_pattern = f"{ticker}/{year}/{ticker}_Annual_{year}"
    else:
        filing_period = f"quarterly_{filing_date}"
        year = filing_date[:4]
        folder_pattern = f"{ticker}/{year}/{ticker}_Quarterly_{filing_date}"

    pages = []
    if ticker in STATEMENT_PAGES:
        ticker_data = STATEMENT_PAGES[ticker]
        if filing_period in ticker_data:
            period_data = ticker_data[filing_period]
            if consolidation in period_data:
                pages = period_data[consolidation].get('PL', [])

    return {
        'source_pages': pages,
        'source_url': f"{PDF_BASE_URL}/{folder_pattern}"
    }


def process_ticker(ticker: str, files: list[Path], qc_status: dict, verbose: bool = False) -> dict:
    """
    Process all files for a single ticker and build the QC-optimized JSON.
    Uses best-source selection to deduplicate periods appearing in multiple files.

    Output structure:
    {
        "ticker": "ABL",
        "periods": [
            {
                "period_end": "2024-03-31",
                "duration": "3M",
                "duration_months": 3,
                "year": 2024,
                "consolidation": "consolidated",
                "source_file": "ABL_quarterly_2024-03-31_consolidated.md",
                "source_qc_status": "pass",
                "source_pages": [57],
                "source_url": "...",
                "unit_type": "thousands",
                "values": {
                    "revenue_net": {
                        "value": 35918528,
                        "source_item": "Total Income"
                    },
                    ...
                }
            },
            ...
        ]
    }
    """
    result = {
        'ticker': ticker,
        'generated_at': datetime.now().isoformat(),
        'periods': [],
    }

    # Collect all candidates grouped by (consolidation, period_end, duration)
    # This allows us to deduplicate periods that appear in multiple files
    period_candidates = defaultdict(list)

    # Process each file and collect candidates
    for filepath in sorted(files):
        file_info = parse_filename(filepath.name)
        if not file_info:
            if verbose:
                print(f"  Skipping unparseable filename: {filepath.name}")
            continue

        parsed = parse_markdown_file(filepath)
        if not parsed or not parsed['rows']:
            if verbose:
                print(f"  Skipping empty/unparseable file: {filepath.name}")
            continue

        consolidation = file_info['consolidation']
        filing_date = file_info['filing_date']
        qc_result = qc_status.get(filepath.name, 'unknown')

        # Get source pages for this filing
        source_info = get_source_pages(ticker, file_info['period_type'], filing_date, consolidation)

        # Create a candidate for each unique (period_end, duration) in this file
        for period_info in parsed['periods']:
            period_end = period_info['period_end']
            duration = period_info['duration']
            period_key = (period_end, duration)

            # Extract values for this specific period (keep raw values, no normalization)
            values = {}
            for row in parsed['rows']:
                if period_key in row['values']:
                    raw_value = row['values'][period_key]

                    values[row['canonical']] = {
                        'value': raw_value,  # Keep raw value, normalize in Stage 5
                        'source_item': row['source_item'],
                        'ref': row['ref'],
                        'is_calculated': row['is_calculated'],
                    }
                    if row['formula']:
                        values[row['canonical']]['formula'] = row['formula']

            if values:
                candidate = {
                    'period_end': period_end,
                    'duration': duration,
                    'duration_months': period_info['duration_months'],
                    'year': period_info['year'],
                    'consolidation': consolidation,
                    'period_type': file_info['period_type'],
                    'filing_date': filing_date,
                    'source_file': filepath.name,
                    'source_qc_status': qc_result,
                    'source_pages': source_info['source_pages'],
                    'source_url': source_info['source_url'],
                    'unit_type': parsed['unit_type'],  # Keep original unit, normalize in Stage 5
                    'values': values,
                }
                # Key by (consolidation, period_end, duration) for deduplication
                dedup_key = (consolidation, period_end, duration)
                period_candidates[dedup_key].append(candidate)

    # Select best source for each unique period
    for dedup_key in sorted(period_candidates.keys()):
        candidates = period_candidates[dedup_key]
        best = select_best_source(candidates, qc_status)
        result['periods'].append(best)

        if verbose:
            consolidation, period_end, duration = dedup_key
            num_candidates = len(candidates)
            if num_candidates > 1:
                print(f"  {consolidation} {period_end} {duration}: {len(best['values'])} values [{best['source_qc_status']}] (selected from {num_candidates} candidates)")
            else:
                print(f"  {consolidation} {period_end} {duration}: {len(best['values'])} values [{best['source_qc_status']}]")

    # Sort periods by consolidation, then by period_end, then by duration
    result['periods'].sort(key=lambda p: (
        p['consolidation'],
        p['period_end'],
        p['duration_months']
    ), reverse=False)

    return result


def main():
    parser = argparse.ArgumentParser(description="JSONify P&L extractions V2 (QC-optimized)")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 4: JSONify P&L")
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
        'periods_exception': 0,
        'periods_unknown': 0,
    }

    for ticker in sorted(files_by_ticker.keys()):
        ticker_files = files_by_ticker[ticker]

        if args.verbose:
            print(f"\n{ticker} ({len(ticker_files)} files):")

        result = process_ticker(ticker, ticker_files, qc_status, args.verbose)

        # Update stats
        stats['tickers'] += 1
        stats['periods_total'] += len(result['periods'])

        for period in result['periods']:
            status = period['source_qc_status']
            if status == 'pass':
                stats['periods_pass'] += 1
            elif status == 'fail':
                stats['periods_fail'] += 1
            elif status == 'exception':
                stats['periods_exception'] += 1
            else:
                stats['periods_unknown'] += 1

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
    print(f"  Periods (exception):   {stats['periods_exception']}")
    print(f"  Periods (unknown):     {stats['periods_unknown']}")
    print()
    print(f"Output: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
