#!/usr/bin/env python3
"""
Step 5: Arrange Balance Sheet Extractions

Parses all BS extraction files and arranges them into a structured JSON
timeline by ticker, section (consolidated/unconsolidated), and date.

Balance sheets are point-in-time snapshots - no LTM or derivation needed.
Each file may contain multiple period columns (current + comparatives).

Input:  data/extracted_bs/*.md
        artifacts/stage3/bs_exclusions.json
Output: artifacts/stage3/bs_arranged.json

Usage:
    python3 Step5_ArrangeBS.py                # Process all
    python3 Step5_ArrangeBS.py --ticker LUCK  # Single ticker
    python3 Step5_ArrangeBS.py --verbose      # Show details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "extracted_bs"
EXCLUSIONS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "bs_exclusions.json"
QC_RESULTS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_bs_qc_results.json"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "bs_arranged.json"


def load_exclusions() -> set:
    """Load files to exclude from processing."""
    excluded = set()
    if EXCLUSIONS_FILE.exists():
        with open(EXCLUSIONS_FILE) as f:
            data = json.load(f)
            for item in data.get("exclude", []):
                excluded.add(item["file"])
    return excluded


def parse_date(date_str: str) -> str | None:
    """Parse date string to ISO format (YYYY-MM-DD)."""
    date_str = date_str.strip().replace("**", "")

    # Common formats: "30 Jun 2024", "31 December 2023", "30-Jun-24"
    patterns = [
        (r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})", "%d %b %Y"),
        (r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})", "%d %B %Y"),
        (r"(\d{1,2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2,4})", "%d-%b-%y"),
    ]

    for pattern, fmt in patterns:
        match = re.search(pattern, date_str, re.IGNORECASE)
        if match:
            try:
                # Reconstruct the date string from match groups
                if fmt == "%d-%b-%y":
                    date_part = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                else:
                    date_part = f"{match.group(1)} {match.group(2)} {match.group(3)}"
                dt = datetime.strptime(date_part, fmt)
                # Handle 2-digit years
                if dt.year < 100:
                    dt = dt.replace(year=dt.year + 2000)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

    return None


def parse_number(s: str) -> float | None:
    """Parse a number from the table."""
    if not s or s.strip() in ['', '-', '—', 'N/A', 'n/a', '0']:
        return 0.0

    s = s.strip().replace('**', '').replace(',', '').replace(' ', '')

    # Handle parentheses for negative numbers
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]

    try:
        return float(s)
    except ValueError:
        return None


def parse_bs_file(filepath: Path) -> dict | None:
    """
    Parse a BS extraction file.

    Returns:
        {
            "ticker": "LUCK",
            "filing_period": "annual_2024",
            "section": "consolidated",
            "unit_type": "thousands",
            "periods": {
                "2024-06-30": {"total_assets": 659661625, "total_equity": 310631448, ...},
                "2023-06-30": {"total_assets": 608359394, ...}
            }
        }
    """
    content = filepath.read_text(encoding='utf-8')
    lines = content.split('\n')

    result = {
        "ticker": None,
        "filing_period": None,
        "section": None,
        "unit_type": "rupees",
        "periods": {}
    }

    # Parse filename for metadata
    # Format: TICKER_period_section.md
    fname = filepath.stem
    parts = fname.rsplit('_', 1)
    if len(parts) == 2:
        result["section"] = parts[1]  # consolidated or unconsolidated
        ticker_period = parts[0]
        # Find where ticker ends and period begins
        for sep in ['_annual_', '_quarterly_']:
            if sep in ticker_period:
                idx = ticker_period.index(sep)
                result["ticker"] = ticker_period[:idx]
                result["filing_period"] = ticker_period[idx+1:]
                break

    # Parse content
    date_columns = []

    for line in lines:
        line = line.strip()

        # Unit type
        if line.startswith('UNIT_TYPE:'):
            result["unit_type"] = line.split(':')[1].strip().lower()
            continue

        # Skip non-table lines
        if not line.startswith('|'):
            continue

        cols = [c.strip() for c in line.split('|')]
        if len(cols) < 5:
            continue

        # Header row - extract date columns
        if 'Source Item' in line or 'Canonical' in line:
            # Columns: | Source Item | Canonical | Ref | Date1 | Date2 | ...
            for col in cols[4:]:
                date = parse_date(col)
                if date:
                    date_columns.append(date)
                    result["periods"][date] = {}
            continue

        # Separator row
        if line.startswith('|:') or line.startswith('|-'):
            continue

        # Data row
        if len(cols) >= 5 and date_columns:
            canonical = cols[2].replace('**', '').strip().lower()

            # Skip empty canonicals or header-like rows
            if not canonical or canonical in ['canonical', 'ref']:
                continue

            # Extract values for each date column
            for i, date in enumerate(date_columns):
                if i + 4 < len(cols):
                    value = parse_number(cols[i + 4])
                    if value is not None:
                        result["periods"][date][canonical] = value

    return result if result["ticker"] and result["periods"] else None


def get_primary_date(filing_period: str, section: str) -> str | None:
    """
    Determine the primary date for a filing.

    annual_2024 with fiscal_period 06-30 -> 2024-06-30
    quarterly_2024-03-31 -> 2024-03-31
    """
    if filing_period.startswith('annual_'):
        year = filing_period.replace('annual_', '')
        # Most Pakistani companies have June fiscal year end
        # TODO: Could load from tickers100.json for accuracy
        return f"{year}-06-30"
    elif filing_period.startswith('quarterly_'):
        date_part = filing_period.replace('quarterly_', '')
        return date_part
    return None


def check_accounting_equation(period_data: dict) -> tuple[bool, float]:
    """
    Check if Assets = Equity + Liabilities for a period.

    Returns (balances, diff_pct)
    """
    total_assets = period_data.get('total_assets')
    total_eq_liab = period_data.get('total_equity_and_liabilities')
    total_equity = period_data.get('total_equity')
    total_liabilities = period_data.get('total_liabilities')

    if total_assets is None or total_assets == 0:
        return True, 0  # Can't check, assume OK

    # Method 1: Compare total_assets vs total_equity_and_liabilities
    if total_eq_liab is not None and total_eq_liab != 0:
        diff = abs(total_assets - total_eq_liab)
        diff_pct = (diff / total_assets) * 100
        return diff_pct < 5.0, diff_pct  # 5% tolerance

    # Method 2: Compare total_assets vs (equity + liabilities)
    if total_equity is not None and total_liabilities is not None:
        eq_plus_liab = total_equity + total_liabilities
        if eq_plus_liab != 0:
            diff = abs(total_assets - eq_plus_liab)
            diff_pct = (diff / total_assets) * 100
            return diff_pct < 5.0, diff_pct

    # If we have assets but no way to verify equity+liabilities, reject
    # (missing critical fields)
    if total_assets > 0 and (total_eq_liab == 0 or total_eq_liab is None):
        if total_equity is None or total_liabilities is None:
            return False, 100.0  # Missing fields = can't verify = reject
        if total_equity == 0 and total_liabilities == 0:
            return False, 100.0  # All zeros = bad extraction

    return True, 0


def arrange_bs_data(files: list[Path], exclusions: set, verbose: bool = False) -> dict:
    """
    Arrange all BS files into structured timeline.

    Returns:
        {
            "LUCK": {
                "consolidated": {
                    "2024-06-30": {
                        "total_assets": 659661625,
                        "total_equity": 310631448,
                        ...
                        "_meta": {
                            "unit_type": "thousands",
                            "source_file": "LUCK_annual_2024_consolidated.md",
                            "is_primary": true
                        }
                    },
                    ...
                },
                "unconsolidated": {...}
            },
            ...
        }
    """
    result = defaultdict(lambda: defaultdict(dict))
    stats = {"processed": 0, "skipped": 0, "periods_added": 0, "periods_dedupe": 0, "periods_unbalanced": 0}

    for filepath in sorted(files):
        fname = filepath.name

        # Skip excluded files
        if fname in exclusions:
            if verbose:
                print(f"SKIP (excluded): {fname}")
            stats["skipped"] += 1
            continue

        parsed = parse_bs_file(filepath)
        if not parsed:
            if verbose:
                print(f"SKIP (parse error): {fname}")
            stats["skipped"] += 1
            continue

        ticker = parsed["ticker"]
        section = parsed["section"]
        unit_type = parsed["unit_type"]
        filing_period = parsed["filing_period"]
        primary_date = get_primary_date(filing_period, section)

        stats["processed"] += 1

        for date, values in parsed["periods"].items():
            is_primary = (date == primary_date)

            # Check if we already have data for this date
            existing = result[ticker][section].get(date)

            if existing:
                # Prefer primary period data over comparative
                existing_is_primary = existing.get("_meta", {}).get("is_primary", False)

                if existing_is_primary and not is_primary:
                    # Keep existing primary data
                    stats["periods_dedupe"] += 1
                    if verbose:
                        print(f"  DEDUPE: {ticker}/{section}/{date} - keeping primary from {existing['_meta']['source_file']}")
                    continue
                elif is_primary and not existing_is_primary:
                    # Replace with new primary data
                    stats["periods_dedupe"] += 1
                    if verbose:
                        print(f"  DEDUPE: {ticker}/{section}/{date} - replacing with primary from {fname}")

            # Check accounting equation before adding
            balances, diff_pct = check_accounting_equation(values)
            if not balances:
                stats["periods_unbalanced"] += 1
                if verbose:
                    print(f"  SKIP (unbalanced): {ticker}/{section}/{date} - {diff_pct:.1f}% diff")
                continue

            # Add/update the period data
            period_data = dict(values)
            period_data["_meta"] = {
                "unit_type": unit_type,
                "source_file": fname,
                "is_primary": is_primary
            }
            result[ticker][section][date] = period_data
            stats["periods_added"] += 1

    return dict(result), stats


def main():
    parser = argparse.ArgumentParser(description="Arrange BS extractions into timeline")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show details")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: ARRANGE BALANCE SHEET EXTRACTIONS")
    print("=" * 70)
    print()

    # Load exclusions
    exclusions = load_exclusions()
    print(f"Exclusions loaded: {len(exclusions)} files")

    # Get input files
    files = list(INPUT_DIR.glob("*.md"))
    if args.ticker:
        files = [f for f in files if f.name.startswith(f"{args.ticker}_")]

    print(f"Input files: {len(files)}")
    print()

    # Arrange data
    arranged, stats = arrange_bs_data(files, exclusions, verbose=args.verbose)

    # Sort dates within each ticker/section
    for ticker in arranged:
        for section in arranged[ticker]:
            dates = sorted(arranged[ticker][section].keys())
            arranged[ticker][section] = {d: arranged[ticker][section][d] for d in dates}

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(arranged, f, indent=2)

    # Summary stats
    total_tickers = len(arranged)
    total_periods = sum(
        len(dates)
        for ticker_data in arranged.values()
        for dates in ticker_data.values()
    )

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files processed:    {stats['processed']}")
    print(f"Files skipped:      {stats['skipped']}")
    print(f"Periods added:      {stats['periods_added']}")
    print(f"Periods deduped:    {stats['periods_dedupe']}")
    print(f"Periods unbalanced: {stats['periods_unbalanced']} (Assets ≠ Equity + Liabilities)")
    print()
    print(f"Tickers:          {total_tickers}")
    print(f"Total periods:    {total_periods}")
    print()
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
