#!/usr/bin/env python3
"""
Step 3: JSONify Compensation

Convert compensation markdown to structured JSON for database upload.
Uses compensation_manifest.json to link source pages.

Input:  data/extracted_compensation/*.md
        artifacts/stage3/compensation_manifest.json
Output: data/json_compensation/management_comp.jsonl
"""

import json
import re
from pathlib import Path
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load ticker metadata
with open(PROJECT_ROOT / 'tickers100.json') as f:
    tickers_data = json.load(f)
TICKER_TO_INDUSTRY = {t['Symbol']: t['Industry'] for t in tickers_data}
TICKER_TO_COMPANY = {t['Symbol']: t['Company Name'] for t in tickers_data}

INPUT_DIR = PROJECT_ROOT / 'data' / 'extracted_compensation'
MANIFEST_FILE = PROJECT_ROOT / 'artifacts' / 'stage3' / 'compensation_manifest.json'
OUTPUT_FILE = PROJECT_ROOT / 'data' / 'json_compensation' / 'management_comp.jsonl'

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

    value_str = value_str.replace(',', '').replace('**', '').strip()

    # Handle parentheses for negatives
    if value_str.startswith('(') and value_str.endswith(')'):
        value_str = '-' + value_str[1:-1]

    try:
        return float(value_str)
    except ValueError:
        return None


def flatten_file(md_path: Path) -> list:
    """Flatten a single compensation markdown file."""
    rows = []

    # Parse filename: {ticker}_compensation_{year}.md
    parts = md_path.stem.split('_')
    ticker = parts[0]
    year = parts[-1]

    industry = TICKER_TO_INDUSTRY.get(ticker, 'Unknown')
    company_name = TICKER_TO_COMPANY.get(ticker, ticker)
    source_pdf = get_source_pdf(ticker, year)

    # Parse markdown
    content = md_path.read_text()
    tables = parse_markdown_tables(content)

    for table in tables:
        headers = table['headers']
        section = table['section'] or 'compensation'

        for row_data in table['rows']:
            if len(row_data) < 2:
                continue

            line_item = row_data[0]

            # Skip header-like rows
            if line_item.lower() in ['role', 'component', 'director name', '#', 'sr.', 'sr', 'no.', 'line item']:
                continue

            # Extract values for each column
            for i, value_str in enumerate(row_data[1:], 1):
                if i >= len(headers):
                    continue

                value = parse_value(value_str)
                if value is None:
                    continue

                column_name = headers[i] if i < len(headers) else f'col_{i}'

                db_row = {
                    'ticker': ticker,
                    'company_name': company_name,
                    'industry': industry,
                    'year': int(year),
                    'section': section,
                    'line_item': line_item,
                    'column': column_name,
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
    print(f"Processing {len(files)} compensation files...")

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
    year_counts = Counter(r['year'] for r in all_rows)
    source_counts = Counter(bool(r.get('source_pdf')) for r in all_rows)

    print(f"\n=== Summary ===")
    print(f"Total rows: {len(all_rows):,}")
    print(f"Tickers: {len(set(r['ticker'] for r in all_rows))}")
    print(f"With source_pdf: {source_counts.get(True, 0):,}")
    print(f"Without source_pdf: {source_counts.get(False, 0):,}")

    print(f"\nBy section (top 10):")
    for k, v in section_counts.most_common(10):
        print(f"  {k[:50]}: {v:,}")

    print(f"\nBy year:")
    for k, v in sorted(year_counts.items()):
        print(f"  {k}: {v:,}")

    print(f"\nOutput: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
