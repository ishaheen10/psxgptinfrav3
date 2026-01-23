#!/usr/bin/env python3
"""
Step 6: JSONify Multi-Year

Convert multi-year markdown to structured JSON for database upload.
Uses step1_multiyear_manifest.json to link source pages.

Input:  data/extracted_multiyear/*.md
        artifacts/stage3/step1_multiyear_manifest.json
Output: data/json_multiyear/multi_year_normalized.jsonl
"""

import json
import re
from pathlib import Path
from collections import Counter
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load ticker metadata
with open(PROJECT_ROOT / 'tickers100.json') as f:
    tickers_data = json.load(f)
TICKER_TO_INDUSTRY = {t['Symbol']: t['Industry'] for t in tickers_data}
TICKER_TO_COMPANY = {t['Symbol']: t['Company Name'] for t in tickers_data}

INPUT_DIR = PROJECT_ROOT / 'data' / 'extracted_multiyear'
MANIFEST_FILE = PROJECT_ROOT / 'artifacts' / 'stage3' / 'step1_multiyear_manifest.json'
OUTPUT_FILE = PROJECT_ROOT / 'data' / 'json_multiyear' / 'multi_year_normalized.jsonl'

# Current year for filtering bad parses
CURRENT_YEAR = datetime.now().year

# Load manifest for source page linking
MANIFEST = {}
if MANIFEST_FILE.exists():
    with open(MANIFEST_FILE) as f:
        MANIFEST = json.load(f)


def get_source_pdf(ticker: str, year: str) -> str | None:
    """Get source PDF path from manifest."""
    if ticker in MANIFEST and year in MANIFEST[ticker]:
        pages = MANIFEST[ticker][year].get('pages', [])
        if pages:
            first_page = pages[0]
            return f"{ticker}/{year}/{ticker}_Annual_{year}/page_{first_page:03d}.pdf"
    return None


def parse_markdown_tables(content: str) -> list:
    """Extract tables from markdown content."""
    tables = []
    lines = content.split('\n')

    current_table = None
    current_section = None

    for line in lines:
        # Track section headers
        if line.startswith('### '):
            current_section = line.replace('### ', '').strip()
            current_section = re.sub(r'^\d+\.\s*', '', current_section)
            continue
        elif line.startswith('## '):
            current_section = line.replace('## ', '').strip()
            continue

        # Table row
        if '|' in line and not re.match(r'^\|[\s\-:]+\|$', line.strip()):
            cells = [c.strip() for c in line.split('|')]
            cells = [c for c in cells if c]

            if not cells:
                continue

            if current_table is None:
                # Header row
                current_table = {
                    'section': current_section,
                    'headers': cells,
                    'rows': []
                }
            elif re.match(r'^[\s\-:\|]+$', line):
                # Separator row, skip
                continue
            else:
                # Data row
                if current_table:
                    current_table['rows'].append(cells)
        else:
            # End of table
            if current_table and current_table['rows']:
                tables.append(current_table)
            current_table = None

    # Don't forget last table
    if current_table and current_table['rows']:
        tables.append(current_table)

    return tables


def parse_value(value_str: str) -> float | None:
    """Parse a numeric value from string."""
    if not value_str or value_str == '-':
        return None

    value_str = value_str.replace(',', '').replace('%', '').replace('**', '').strip()
    value_str = re.sub(r'\s+', '', value_str)

    # Handle parentheses for negatives
    if value_str.startswith('(') and value_str.endswith(')'):
        value_str = '-' + value_str[1:-1]

    try:
        return float(value_str)
    except ValueError:
        return None


def extract_year_from_header(header: str) -> int | None:
    """Extract year from column header like '2024' or 'FY2024'."""
    match = re.search(r'(19|20)\d{2}', header)
    if match:
        year = int(match.group())
        # Filter out future years (likely parsing errors)
        if year <= CURRENT_YEAR:
            return year
    return None


def flatten_file(md_path: Path) -> list:
    """Flatten a single multi-year markdown file."""
    rows = []

    # Parse filename: {ticker}_multiyear_{year}.md
    parts = md_path.stem.split('_')
    ticker = parts[0]
    report_year = parts[-1]

    industry = TICKER_TO_INDUSTRY.get(ticker, 'Unknown')
    company_name = TICKER_TO_COMPANY.get(ticker, ticker)
    source_pdf = get_source_pdf(ticker, report_year)

    # Parse markdown
    content = md_path.read_text()
    tables = parse_markdown_tables(content)

    for table in tables:
        headers = table['headers']
        section = table['section'] or 'summary'

        # Map column index to year
        col_years = {}
        for i, h in enumerate(headers):
            if i == 0:
                continue  # Skip line item column
            year = extract_year_from_header(h)
            if year:
                col_years[i] = year

        for row_data in table['rows']:
            if len(row_data) < 2:
                continue

            line_item = row_data[0].strip()

            # Skip header-like rows
            if not line_item or line_item.lower() in ['line item', 'particulars', 'description']:
                continue

            # Extract values for each year column
            for col_idx, data_year in col_years.items():
                if col_idx >= len(row_data):
                    continue

                value = parse_value(row_data[col_idx])
                if value is None:
                    continue

                db_row = {
                    'ticker': ticker,
                    'company_name': company_name,
                    'industry': industry,
                    'report_year': int(report_year),
                    'data_year': data_year,
                    'section': section,
                    'line_item': line_item,
                    'value': value,
                    'source_pdf': source_pdf,
                }
                rows.append(db_row)

    return rows


def main():
    if not INPUT_DIR.exists():
        print(f"Error: {INPUT_DIR} not found")
        return

    if not MANIFEST_FILE.exists():
        print(f"Warning: {MANIFEST_FILE} not found - source_pdf will be null")

    files = list(INPUT_DIR.glob('*.md'))
    print(f"Processing {len(files)} multi-year files...")

    all_rows = []
    for f in files:
        rows = flatten_file(f)
        all_rows.extend(rows)

    print(f"Total rows: {len(all_rows):,}")

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as out:
        for row in all_rows:
            out.write(json.dumps(row) + '\n')

    # Stats
    section_counts = Counter(r['section'] for r in all_rows)
    year_counts = Counter(r['data_year'] for r in all_rows)
    report_year_counts = Counter(r['report_year'] for r in all_rows)
    source_counts = Counter(bool(r.get('source_pdf')) for r in all_rows)

    print(f"\n=== Summary ===")
    print(f"Total rows: {len(all_rows):,}")
    print(f"Tickers: {len(set(r['ticker'] for r in all_rows))}")
    print(f"With source_pdf: {source_counts.get(True, 0):,}")
    print(f"Without source_pdf: {source_counts.get(False, 0):,}")

    print(f"\nBy section (top 10):")
    for k, v in section_counts.most_common(10):
        print(f"  {k[:50]}: {v:,}")

    print(f"\nBy data year:")
    for k, v in sorted(year_counts.items()):
        print(f"  {k}: {v:,}")

    print(f"\nBy report year:")
    for k, v in sorted(report_year_counts.items()):
        print(f"  {k}: {v:,}")

    print(f"\nOutput: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
