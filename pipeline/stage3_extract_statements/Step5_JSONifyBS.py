#!/usr/bin/env python3
"""
Step 5: JSONify Balance Sheet Extractions

Converts extracted BS markdown files into per-ticker JSON files
optimized for QC lookups.

Input:  data/extracted_bs/*.md
Output: data/json_bs/{TICKER}.json

JSON structure:
{
    "ticker": "LUCK",
    "periods": [
        {
            "period_end": "2024-06-30",
            "consolidation": "consolidated",
            "filing_type": "annual",
            "filing_period": "annual_2024",
            "unit_type": "thousands",
            "source_file": "LUCK_annual_2024_consolidated.md",
            "source_pages": [12, 13],
            "values": {
                "total_assets": 659661625,
                "total_equity": 310631448,
                ...
            },
            "source_items": {
                "total_assets": "Total assets",
                "total_equity": "Total equity",
                ...
            }
        },
        ...
    ]
}

Usage:
    python3 Step5_JSONifyBS.py                # Process all
    python3 Step5_JSONifyBS.py --ticker LUCK  # Single ticker
    python3 Step5_JSONifyBS.py --verbose      # Show details
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "extracted_bs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "json_bs"
EXCLUSIONS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "bs_exclusions.json"
STATEMENT_PAGES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
QC_RESULTS_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step4_qc_bs_extraction.json"
PDF_BASE_URL = "https://source.psxgpt.com/PDF_PAGES"

# Load statement pages manifest
STATEMENT_PAGES = {}
if STATEMENT_PAGES_FILE.exists():
    with open(STATEMENT_PAGES_FILE) as f:
        STATEMENT_PAGES = json.load(f)


def get_source_info(ticker: str, filing_period: str, section: str) -> dict:
    """
    Look up source pages and URL from step2_statement_pages.json.

    Args:
        ticker: e.g., "LUCK"
        filing_period: e.g., "annual_2024" or "quarterly_2024-03-31"
        section: "consolidated" or "unconsolidated"

    Returns:
        {"source_pages": [12, 13], "source_url": "https://..."}
    """
    # Build folder pattern for source URL
    if filing_period.startswith('annual_'):
        year = filing_period.replace('annual_', '')
        folder_pattern = f"{ticker}/{year}/{ticker}_Annual_{year}"
    else:
        date_part = filing_period.replace('quarterly_', '')
        year = date_part[:4]
        folder_pattern = f"{ticker}/{year}/{ticker}_Quarterly_{date_part}"

    pages = []
    if ticker in STATEMENT_PAGES:
        ticker_data = STATEMENT_PAGES[ticker]
        if filing_period in ticker_data:
            period_data = ticker_data[filing_period]
            if section in period_data:
                pages = period_data[section].get('BS', [])

    return {
        'source_pages': pages,
        'source_url': f"{PDF_BASE_URL}/{folder_pattern}"
    }


def load_exclusions() -> set:
    """Load files to exclude from processing."""
    excluded = set()
    if EXCLUSIONS_FILE.exists():
        with open(EXCLUSIONS_FILE) as f:
            data = json.load(f)
            for item in data.get("exclude", []):
                excluded.add(item["file"])
    return excluded


def load_qc_results() -> dict:
    """Load QC results and return dict of filename -> pass/fail."""
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


def passes_qc(filename: str, qc_status: dict) -> bool:
    """Check if a file passes QC."""
    status = qc_status.get(filename, 'unknown')
    return status in ('pass', 'no_formulas')


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
            "filing_type": "annual",
            "section": "consolidated",
            "unit_type": "thousands",
            "periods": {
                "2024-06-30": {
                    "values": {"total_assets": 659661625, ...},
                    "source_items": {"total_assets": "Total Assets", ...}
                },
                ...
            }
        }
    """
    content = filepath.read_text(encoding='utf-8')
    lines = content.split('\n')

    result = {
        "ticker": None,
        "filing_period": None,
        "filing_type": None,
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
                result["filing_type"] = "annual" if "_annual_" in sep else "quarterly"
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
                    result["periods"][date] = {"values": {}, "source_items": {}, "refs": {}}
            continue

        # Separator row
        if line.startswith('|:') or line.startswith('|-'):
            continue

        # Data row
        if len(cols) >= 5 and date_columns:
            source_item = cols[1].replace('**', '').strip()
            canonical = cols[2].replace('**', '').replace('[', '').replace(']', '').strip().lower()
            ref = cols[3].strip() if len(cols) > 3 else ""

            # Skip empty canonicals or header-like rows
            if not canonical or canonical in ['canonical', 'ref']:
                continue

            # Extract values for each date column
            for i, date in enumerate(date_columns):
                if i + 4 < len(cols):
                    value = parse_number(cols[i + 4])
                    if value is not None:
                        result["periods"][date]["values"][canonical] = value
                        result["periods"][date]["source_items"][canonical] = source_item
                        if ref:
                            result["periods"][date]["refs"][canonical] = ref

    return result if result["ticker"] and result["periods"] else None


def get_primary_date(filing_period: str) -> str | None:
    """
    Determine the primary date for a filing.

    annual_2024 with fiscal_period 06-30 -> 2024-06-30
    quarterly_2024-03-31 -> 2024-03-31
    """
    if filing_period.startswith('annual_'):
        year = filing_period.replace('annual_', '')
        # Most Pakistani companies have June fiscal year end
        return f"{year}-06-30"
    elif filing_period.startswith('quarterly_'):
        date_part = filing_period.replace('quarterly_', '')
        return date_part
    return None


def process_files(files: list[Path], exclusions: set, qc_status: dict, verbose: bool = False) -> dict:
    """
    Process all BS files and organize by ticker.

    Uses QC-aware deduplication:
    1. Primary period that passes QC
    2. Prior-year comparison that passes QC (if primary fails) - labeled as fallback
    3. Primary period (even if failing) as last resort

    Returns:
        {
            "LUCK": {
                "ticker": "LUCK",
                "periods": [...]
            },
            ...
        }
    """
    # Temporary structure: {ticker: {(date, section): [candidate_list]}}
    # Collect all candidates first, then select best
    ticker_candidates = defaultdict(lambda: defaultdict(list))
    stats = {"processed": 0, "skipped": 0, "periods_added": 0, "periods_dedupe": 0, "periods_fallback": 0}

    # Phase 1: Collect all candidates
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
        filing_type = parsed["filing_type"]
        primary_date = get_primary_date(filing_period)

        stats["processed"] += 1

        # Get source info (pages and URL)
        source_info = get_source_info(ticker, filing_period, section)

        for date, period_data in parsed["periods"].items():
            is_primary = (date == primary_date)
            raw_values = dict(period_data["values"])  # Copy to avoid mutation
            source_items = dict(period_data["source_items"])
            refs = dict(period_data.get("refs", {}))

            # Normalize canonical field aliases
            FIELD_ALIASES = {
                'subtotal_equity': 'total_equity',
                'total_liabilities_and_equity': 'total_equity_and_liabilities',
            }
            for alias, canonical in FIELD_ALIASES.items():
                if alias in raw_values and canonical not in raw_values:
                    raw_values[canonical] = raw_values.pop(alias)
                    if alias in source_items:
                        source_items[canonical] = source_items.pop(alias)
                    if alias in refs:
                        refs[canonical] = refs.pop(alias)

            # Derive total_liabilities from TCL + TNCL if not present
            if raw_values.get('total_liabilities') is None:
                tcl = raw_values.get('total_current_liabilities')
                tncl = raw_values.get('total_non_current_liabilities')
                if tcl is not None and tncl is not None:
                    raw_values['total_liabilities'] = tcl + tncl
                    source_items['total_liabilities'] = '[Derived: TCL + TNCL]'

            # Derive missing totals from components (for capital employed format)
            if raw_values.get('total_assets') is None:
                nca = (raw_values.get('total_non_current_assets') or
                       raw_values.get('total_non_current_assets_sub'))
                ca = raw_values.get('total_current_assets')
                if nca is not None and ca is not None:
                    raw_values['total_assets'] = nca + ca
                    source_items['total_assets'] = '[Derived: NCA + CA]'

            # Derive total_equity_and_liabilities if missing
            te = raw_values.get('total_equity')
            tl = raw_values.get('total_liabilities')
            ta = raw_values.get('total_assets')
            if te is not None and tl is not None:
                expected_tel = te + tl
                current_tel = raw_values.get('total_equity_and_liabilities')

                if current_tel is None:
                    raw_values['total_equity_and_liabilities'] = expected_tel
                    source_items['total_equity_and_liabilities'] = '[Derived: TE + TL]'
                elif ta is not None and expected_tel > 0:
                    current_wrong = abs(current_tel - expected_tel) / expected_tel > 0.05
                    expected_matches_assets = abs(expected_tel - ta) / ta < 0.05 if ta > 0 else False
                    if current_wrong and expected_matches_assets:
                        raw_values['total_equity_and_liabilities'] = expected_tel
                        source_items['total_equity_and_liabilities'] = f'[Corrected: TE + TL, was {current_tel}]'

            # Derive total_assets from TEL if still missing
            if raw_values.get('total_assets') is None:
                tel = raw_values.get('total_equity_and_liabilities')
                if tel is not None:
                    raw_values['total_assets'] = tel
                    source_items['total_assets'] = '[Derived: TA = TEL]'

            # Build candidate record
            key = (date, section)
            file_qc_status = qc_status.get(fname, 'unknown')
            candidate = {
                "period_end": date,
                "consolidation": section,
                "filing_type": filing_type,
                "filing_period": filing_period,
                "unit_type": unit_type,
                "source_file": fname,
                "source_pages": source_info['source_pages'],
                "source_url": source_info['source_url'],
                "is_primary": is_primary,
                "passes_qc": passes_qc(fname, qc_status),
                "source_qc_status": file_qc_status,
                "values": raw_values,
                "source_items": source_items,
                "refs": refs
            }
            ticker_candidates[ticker][key].append(candidate)

    # Phase 2: Select best source for each period using QC-aware logic
    ticker_periods = {}
    for ticker in sorted(ticker_candidates.keys()):
        ticker_periods[ticker] = {}
        for key, candidates in ticker_candidates[ticker].items():
            if len(candidates) == 1:
                best = candidates[0]
                best['source_type'] = 'primary' if best['is_primary'] else 'prior_year'
            else:
                # Separate by primary vs prior-year
                primary_candidates = [c for c in candidates if c['is_primary']]
                prior_candidates = [c for c in candidates if not c['is_primary']]

                # 1. First choice: primary that passes QC
                primary_passing = [c for c in primary_candidates if c['passes_qc']]
                if primary_passing:
                    best = primary_passing[0]
                    best['source_type'] = 'primary'
                    stats["periods_dedupe"] += len(candidates) - 1
                # 2. Second choice: prior-year that passes QC (fallback)
                elif [c for c in prior_candidates if c['passes_qc']]:
                    prior_passing = [c for c in prior_candidates if c['passes_qc']]
                    best = prior_passing[0]
                    best['source_type'] = 'prior_year_fallback'
                    stats["periods_dedupe"] += len(candidates) - 1
                    stats["periods_fallback"] += 1
                    if verbose:
                        print(f"  FALLBACK: {ticker}/{key[1]}/{key[0]} - using prior-year {best['source_file']} (primary failed QC)")
                # 3. Last resort: best primary even if failing
                elif primary_candidates:
                    best = primary_candidates[0]
                    best['source_type'] = 'primary'
                    stats["periods_dedupe"] += len(candidates) - 1
                # 4. Final fallback: any prior
                else:
                    best = prior_candidates[0] if prior_candidates else candidates[0]
                    best['source_type'] = 'prior_year'
                    stats["periods_dedupe"] += len(candidates) - 1

            ticker_periods[ticker][key] = best
            stats["periods_added"] += 1

    # Convert to final structure
    result = {}
    for ticker in sorted(ticker_periods.keys()):
        periods = list(ticker_periods[ticker].values())
        # Sort by date, then by consolidation
        periods.sort(key=lambda p: (p["period_end"], p["consolidation"]))
        # Remove internal flags, keep source_type and source_qc_status
        for p in periods:
            del p["is_primary"]
            del p["passes_qc"]
            # source_qc_status is kept in output
        result[ticker] = {
            "ticker": ticker,
            "periods": periods
        }

    return result, stats


def main():
    parser = argparse.ArgumentParser(description="JSONify BS extractions (v2) with QC-aware deduplication")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show details")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5: JSONify Balance Sheet (with QC-aware deduplication)")
    print("=" * 70)
    print()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load exclusions
    exclusions = load_exclusions()
    print(f"Exclusions loaded: {len(exclusions)} files")

    # Load QC results
    qc_status = load_qc_results()
    print(f"QC results loaded: {len(qc_status)} files")

    # Get input files
    files = list(INPUT_DIR.glob("*.md"))
    if args.ticker:
        files = [f for f in files if f.name.startswith(f"{args.ticker}_")]

    print(f"Input files: {len(files)}")
    print()

    # Process data
    ticker_data, stats = process_files(files, exclusions, qc_status, verbose=args.verbose)

    # Write per-ticker JSON files (values kept in original units, normalized in Stage 5)
    for ticker, data in ticker_data.items():
        output_file = OUTPUT_DIR / f"{ticker}.json"
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

    # Summary stats
    total_tickers = len(ticker_data)
    total_periods = sum(len(d["periods"]) for d in ticker_data.values())

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files processed:    {stats['processed']}")
    print(f"Files skipped:      {stats['skipped']}")
    print(f"Periods added:      {stats['periods_added']}")
    print(f"Periods deduped:    {stats['periods_dedupe']}")
    print(f"Periods fallback:   {stats['periods_fallback']}")
    print()
    print(f"Tickers:          {total_tickers}")
    print(f"Total periods:    {total_periods}")
    print()
    print(f"Output directory: {OUTPUT_DIR}")

    # Show sample output
    if ticker_data:
        sample_ticker = list(ticker_data.keys())[0]
        sample_file = OUTPUT_DIR / f"{sample_ticker}.json"
        print(f"Sample output:    {sample_file}")


if __name__ == "__main__":
    main()
