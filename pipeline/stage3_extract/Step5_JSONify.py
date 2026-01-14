#!/usr/bin/env python3
"""
Step 5: JSONify P&L Extractions with Best-Source Selection

Converts extracted_pl markdown files to JSON with:
1. Period normalization (e.g., "3M Mar 2024" -> "2024-03-31")
2. Multi-filing deduplication: prefer latest filing that passes QC
3. Source tracing: each period records which filing it came from

Input:  data/extracted_pl/*.md
        artifacts/stage3/step4a_qc_results.json
Output: data/json_pl/{TICKER}.json

Usage:
    python3 Step5_JSONify.py                    # Process all
    python3 Step5_JSONify.py --ticker ABL       # Single ticker
    python3 Step5_JSONify.py --verbose          # Show details
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
QC_RESULTS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_qc_results.json"

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

# Thresholds for cross-period consistency check
SCALE_MISMATCH_THRESHOLD_HIGH = 100  # If value > 100x median, likely wrong unit
SCALE_MISMATCH_THRESHOLD_LOW = 0.01  # If value < 0.01x median, likely wrong unit


def normalize_value(value: float, unit_type: str, canonical: str) -> float:
    """
    Normalize a value to thousands.
    - rupees/Rupees: divide by 1000
    - millions: multiply by 1000
    - thousands: keep as is
    - Skip normalization for EPS fields
    """
    if value is None:
        return None

    # Skip normalization for EPS (always in rupees per share)
    if canonical and 'eps' in canonical.lower():
        return value

    unit_lower = unit_type.lower().strip()

    # Handle various unit formats
    if unit_lower in ('rupees', 'rupee'):
        return value / 1000.0
    elif unit_lower == 'millions':
        return value * 1000.0
    elif 'thousands' in unit_lower:
        return value  # Already in thousands
    else:
        # Unknown unit, assume rupees (most raw values are in rupees)
        # But if values are small (< 100), they might already be in millions
        # If very large (> 1 billion), they're likely rupees
        if abs(value) > 1_000_000_000:  # > 1 billion, likely rupees
            return value / 1000.0
        return value  # Assume already normalized


def apply_cross_period_normalization(periods: list[dict], verbose: bool = False) -> int:
    """
    Detect and fix scale mismatches across periods for the same ticker.

    Uses revenue_net as the reference metric. If a period's revenue is way off
    from the median (>100x or <0.01x), it's likely in the wrong unit and we
    apply a correction factor to ALL values in that period.

    Returns number of periods corrected.
    """
    corrections = 0

    # Group periods by consolidation type
    by_consolidation = defaultdict(list)
    for p in periods:
        by_consolidation[p['consolidation']].append(p)

    for cons_type, cons_periods in by_consolidation.items():
        # Get revenue values (excluding None/0)
        revenues = []
        for p in cons_periods:
            rev = p['values'].get('revenue_net')
            if rev and abs(rev) > 0:
                revenues.append((p, rev))

        if len(revenues) < 2:
            continue

        # Calculate median revenue
        rev_values = sorted([r[1] for r in revenues])
        median_rev = rev_values[len(rev_values) // 2]

        if median_rev == 0:
            continue

        # Check each period for scale mismatch
        for period, rev in revenues:
            ratio = abs(rev / median_rev)

            correction_factor = None

            if ratio > SCALE_MISMATCH_THRESHOLD_HIGH:
                # Value is too large - divide by 1000
                correction_factor = 1.0 / 1000.0
                if verbose:
                    print(f"    SCALE FIX: {cons_type} {period['period_end']} - revenue {rev:,.0f} is {ratio:.0f}x median, dividing by 1000")
            elif ratio < SCALE_MISMATCH_THRESHOLD_LOW:
                # Value is too small - multiply by 1000
                correction_factor = 1000.0
                if verbose:
                    print(f"    SCALE FIX: {cons_type} {period['period_end']} - revenue {rev:,.0f} is {ratio:.4f}x median, multiplying by 1000")

            if correction_factor:
                # Apply correction to ALL values in this period (except EPS)
                for canonical, value in period['values'].items():
                    if value is not None and 'eps' not in canonical.lower():
                        period['values'][canonical] = value * correction_factor
                corrections += 1

    return corrections


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
        ref = parts[2].replace('**', '')

        # Skip header-like rows
        if 'Source Item' in source_item or 'Canonical' in canonical:
            continue

        # Skip empty rows (separators)
        if not canonical or canonical.strip() == '':
            continue

        # Parse values for each period
        # Key by (period_end, duration) to handle multiple durations for same date
        # e.g., both "3M Jun 2022" and "6M Jun 2022" have period_end "2022-06-30"
        values = {}
        for i, period_info in enumerate(result['periods']):
            col_idx = 3 + i
            if col_idx < len(parts):
                val = parse_number(parts[col_idx])
                if val is not None:
                    # Use tuple key (period_end, duration) to preserve both values
                    key = (period_info['period_end'], period_info['duration'])
                    values[key] = val

        if values:  # Only add rows with at least one value
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
        # Map status to qc_status
        if status == 'pass':
            qc_status[filename] = 'pass'
        elif status == 'exception':
            qc_status[filename] = 'pass'  # Treat exceptions as pass
        elif result.get('formula_total', 0) == 0:
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
    from datetime import datetime

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
    1. Current period from original filing (not restated prior-year from newer filing)
    2. Passes QC
    3. Latest filing date

    Each candidate has: filename, filing_date, period_end, duration, rows
    """
    if len(candidates) == 1:
        return candidates[0]

    # Separate into current period vs prior-year comparison
    current_period = [c for c in candidates if is_current_period(c)]
    prior_year = [c for c in candidates if not is_current_period(c)]

    # Prefer current period sources
    if current_period:
        # Sort by filing date descending within current period candidates
        current_sorted = sorted(current_period, key=lambda x: x['filing_date'], reverse=True)

        # First try to find a passing one
        for c in current_sorted:
            status = qc_status.get(c['filename'], 'unknown')
            if status in ('pass', 'no_formulas'):
                return c

        # If none pass, return the latest current period one
        return current_sorted[0]

    # Fall back to prior-year sources if no current period available
    prior_sorted = sorted(prior_year, key=lambda x: x['filing_date'], reverse=True)

    for c in prior_sorted:
        status = qc_status.get(c['filename'], 'unknown')
        if status in ('pass', 'no_formulas'):
            return c

    return prior_sorted[0]


def process_ticker(ticker: str, files: list[Path], qc_status: dict, verbose: bool = False) -> dict:
    """
    Process all files for a single ticker and build the unified JSON.
    """
    # Structure: {consolidation: {(period_end, duration): [candidates]}}
    # Using (period_end, duration) as key to keep 3M and 6M separate for same date
    period_candidates = defaultdict(lambda: defaultdict(list))

    # Parse all files and group by period
    for filepath in files:
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

        # Group rows by (period_end, duration) - both must match
        for period_info in parsed['periods']:
            period_end = period_info['period_end']
            duration = period_info['duration']
            period_key = (period_end, duration)

            # Extract values for this specific period (matching both end date AND duration)
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
                # Use (period_end, duration) as key to keep 3M and 6M separate
                period_candidates[consolidation][period_key].append(candidate)

    # Select best source for each period
    result = {
        'ticker': ticker,
        'periods': [],
    }

    for consolidation in sorted(period_candidates.keys()):
        # Keys are now (period_end, duration) tuples
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
                normalized_values[canonical] = normalize_value(raw_value, best['unit_type'], canonical)

            period_record = {
                'period_end': period_end,
                'duration': duration,  # Use duration from the key, not best['duration']
                'consolidation': consolidation,
                'unit_type': STANDARD_UNIT,  # All values normalized to thousands
                'source_filing': best['filename'],
                'source_qc': qc_result,
                'values': normalized_values,
            }

            # Add source tracing details
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

    # Cross-period scale normalization disabled - was incorrectly "fixing"
    # legitimate low-revenue quarters by multiplying by 1000
    # scale_fixes = apply_cross_period_normalization(result['periods'], verbose)
    # if scale_fixes > 0:
    #     result['_scale_fixes'] = scale_fixes

    return result


def main():
    parser = argparse.ArgumentParser(description="JSONify P&L extractions with best-source selection")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: JSONify P&L EXTRACTIONS")
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
        'periods_with_alternates': 0,
        'periods_pass': 0,
        'periods_fail': 0,
        'rescued': 0,  # Periods where we picked a passing alternate over a failing primary
        'scale_fixes': 0,  # Periods where cross-period normalization was applied
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
            if period['source_qc'] in ('pass', 'no_formulas'):
                stats['periods_pass'] += 1
            else:
                stats['periods_fail'] += 1

            if 'alternate_sources' in period:
                stats['periods_with_alternates'] += 1
                # Check if we rescued this period (picked passing over failing)
                alt_statuses = [a['qc'] for a in period['alternate_sources']]
                if period['source_qc'] in ('pass', 'no_formulas') and 'fail' in alt_statuses:
                    stats['rescued'] += 1

        # Scale fixes disabled
        # stats['scale_fixes'] += result.get('_scale_fixes', 0)

        # Write output
        output_file = OUTPUT_DIR / f"{ticker}.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)

        if not args.verbose:
            rescued_count = sum(
                1 for p in result['periods']
                if p['source_qc'] in ('pass', 'no_formulas')
                and 'alternate_sources' in p
                and any(a['qc'] == 'fail' for a in p['alternate_sources'])
            )
            status = f" ({rescued_count} rescued)" if rescued_count > 0 else ""
            print(f"  {ticker}: {len(result['periods'])} periods{status}")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Tickers processed:     {stats['tickers']}")
    print(f"  Total periods:         {stats['periods_total']}")
    print(f"  Periods (pass QC):     {stats['periods_pass']}")
    print(f"  Periods (fail QC):     {stats['periods_fail']}")
    print(f"  Periods with alts:     {stats['periods_with_alternates']}")
    print(f"  Rescued periods:       {stats['rescued']}")
    print()
    print(f"Output: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
