#!/usr/bin/env python3
"""
Step 4: Extract Multi-Year Historical Summaries

Extracts multi-year summary tables from annual reports to markdown.
These are "At a Glance" / "Six Year Summary" / "Key Financial Data" pages.

Input:  Pages tagged with 'multi_year' in extraction manifest
        markdown_pages/{TICKER}/{YEAR}/...
Output: data/extracted_multiyear/{TICKER}_multiyear_{YEAR}.md

Data Types:
- monetary: Balance Sheet, P&L, Cash Flow items (amounts in PKR)
- ratio: Financial ratios, percentages, per-share data

Units: Monetary in thousands (PKR '000), ratios as raw numbers.

Usage:
    python3 Step4_ExtractMultiYear.py                     # Process all
    python3 Step4_ExtractMultiYear.py --ticker LUCK       # Single ticker
    python3 Step4_ExtractMultiYear.py --year 2024         # Single year
"""

import argparse
import json
import os
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTION_MANIFEST = PROJECT_ROOT / "artifacts" / "stage2" / "step6_extraction_manifest.json"
MARKDOWN_PAGES = PROJECT_ROOT / "markdown_pages"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
OUTPUT_DIR = PROJECT_ROOT / "multiyear"
OUTPUT_JSONL = PROJECT_ROOT / "multiyear_flattened.jsonl"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage3"

WORKERS = 30


# =============================================================================
# EXTRACTION PROMPT
# =============================================================================

EXTRACTION_PROMPT = """Extract multi-year financial summary tables from these pages.

These are historical summaries like "Six Years at a Glance", "Horizontal Analysis",
"Vertical Analysis", or "Key Financial Data".

## UNIT DETECTION - CRITICAL

Look for the unit specification in the document:
- "Rupees in thousands" / "Rs. 000" / "(000)" → values in thousands
- "Rupees in millions" / "Rs. in millions" → multiply by 1000 for thousands
- "Rupees" (no multiplier) / large numbers → divide by 1000 for thousands

## OUTPUT FORMAT

Output a JSON object with this structure:

```json
{
  "unit_detected": "thousands",
  "tables": [
    {
      "name": "Balance Sheet Summary",
      "data_type": "monetary",
      "years": [2024, 2023, 2022, 2021, 2020, 2019],
      "items": [
        {"line_item": "Total Assets", "values": [500000, 450000, 400000, 350000, 300000, 250000]},
        {"line_item": "Total Liabilities", "values": [300000, 270000, 240000, 210000, 180000, 150000]}
      ]
    },
    {
      "name": "Profit & Loss Summary",
      "data_type": "monetary",
      "years": [2024, 2023, 2022, 2021, 2020, 2019],
      "items": [
        {"line_item": "Revenue", "values": [100000, 90000, 80000, 70000, 60000, 50000]}
      ]
    },
    {
      "name": "Financial Ratios",
      "data_type": "ratio",
      "years": [2024, 2023, 2022, 2021, 2020, 2019],
      "items": [
        {"line_item": "Current Ratio", "values": [1.5, 1.4, 1.3, 1.2, 1.1, 1.0]},
        {"line_item": "P/E Ratio", "values": [12.5, 11.0, 10.5, 9.8, 8.5, 7.2]}
      ]
    }
  ]
}
```

## RULES

1. All MONETARY values in THOUSANDS. Convert if source uses different units.
2. RATIOS stored as raw numbers (no % sign, just the value).
3. Use null for missing values.
4. Each table needs: name, data_type (monetary/ratio), years array, items array.
5. Years should be integers, most recent first.
6. Line items should have clear text descriptions.

SOURCE PAGES:
"""


# =============================================================================
# UTILITIES
# =============================================================================

def load_extraction_manifest() -> dict:
    """Load the extraction manifest from Stage 2."""
    if not EXTRACTION_MANIFEST.exists():
        return {}
    with open(EXTRACTION_MANIFEST) as f:
        return json.load(f)


def load_ticker_meta() -> dict:
    """Load ticker metadata."""
    if not TICKERS_FILE.exists():
        return {}
    with open(TICKERS_FILE) as f:
        tickers = json.load(f)
    return {t['Symbol']: t for t in tickers}


def get_multiyear_pages(manifest: dict, ticker: str, period: str) -> list:
    """Get multi-year page numbers for a filing."""
    filing_key = f"{ticker}_{period}"
    filing = manifest.get('filings', {}).get(filing_key, {})
    pages = filing.get('pages', {}).get('multi_year', [])
    return sorted(pages)


def read_pages(ticker: str, year: str, doc: str, page_nums: list) -> str:
    """Read markdown pages and combine."""
    content = []
    for page_num in page_nums:
        page_path = MARKDOWN_PAGES / ticker / year / doc / f"page_{page_num:03d}.md"
        if page_path.exists():
            content.append(f"<!-- Page {page_num} -->\n{page_path.read_text()}")
    return "\n\n---\n\n".join(content)


def extract_multiyear(client, pages_content: str, ticker: str, year: str) -> dict:
    """Call DeepSeek to extract multi-year data."""
    prompt = EXTRACTION_PROMPT + pages_content

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    text = response.choices[0].message.content
    return json.loads(text)


# =============================================================================
# FLATTEN TO JSONL
# =============================================================================

def flatten_multiyear(data: dict, ticker: str, source_year: str, ticker_meta: dict) -> list:
    """Flatten multi-year data to database rows."""
    rows = []
    meta = ticker_meta.get(ticker, {})

    for table in data.get('tables', []):
        table_name = table.get('name', 'Unknown')
        data_type = table.get('data_type', 'monetary')
        years = table.get('years', [])

        for item in table.get('items', []):
            line_item = item.get('line_item', '')
            values = item.get('values', [])

            for i, year in enumerate(years):
                value = values[i] if i < len(values) else None

                if value is None:
                    continue

                row = {
                    'ticker': ticker,
                    'company_name': meta.get('Company Name', ''),
                    'industry': meta.get('Industry', ''),
                    'source_year': int(source_year),
                    'data_year': int(year) if year else None,
                    'table_name': table_name,
                    'data_type': data_type,
                    'line_item': line_item,
                    'value': value,
                    'unit': 'thousands' if data_type == 'monetary' else 'ratio',
                }
                rows.append(row)

    return rows


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_filing(ticker: str, year: str, doc: str, pages: list, client,
                   ticker_meta: dict) -> tuple:
    """Process a single filing."""
    pages_content = read_pages(ticker, year, doc, pages)
    if not pages_content:
        return None, []

    try:
        data = extract_multiyear(client, pages_content, ticker, year)

        # Save individual JSON
        output_path = OUTPUT_DIR / f"{ticker}_{year}.json"
        output_path.write_text(json.dumps(data, indent=2))

        # Flatten
        rows = flatten_multiyear(data, ticker, year, ticker_meta)
        return data, rows

    except Exception as e:
        print(f"  ERROR {ticker}_{year}: {e}")
        return None, []


def main():
    parser = argparse.ArgumentParser(description="Extract multi-year summaries")
    parser.add_argument("--ticker", help="Single ticker")
    parser.add_argument("--year", help="Single year")
    parser.add_argument("--flatten-only", action="store_true",
                        help="Just flatten existing JSON files")
    parser.add_argument("--workers", type=int, default=WORKERS)
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 11: EXTRACT MULTI-YEAR SUMMARIES")
    print("=" * 70)
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    manifest = load_extraction_manifest()
    ticker_meta = load_ticker_meta()

    if args.flatten_only:
        # Just flatten existing files
        print("Flattening existing JSON files...")
        all_rows = []

        for json_path in OUTPUT_DIR.glob("*.json"):
            parts = json_path.stem.split('_')
            ticker = parts[0]
            year = parts[1] if len(parts) > 1 else ""

            data = json.loads(json_path.read_text())
            rows = flatten_multiyear(data, ticker, year, ticker_meta)
            all_rows.extend(rows)

        with open(OUTPUT_JSONL, 'w') as f:
            for row in all_rows:
                f.write(json.dumps(row) + '\n')

        print(f"Wrote {len(all_rows)} rows to {OUTPUT_JSONL}")
        return

    # Full extraction
    if OpenAI is None:
        print("ERROR: openai package not installed")
        return

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("ERROR: DEEPSEEK_API_KEY not set")
        return

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")

    # Find filings with multi-year pages
    filings_to_process = []

    for filing_key, filing_data in manifest.get('filings', {}).items():
        pages = filing_data.get('pages', {}).get('multi_year', [])
        if not pages:
            continue

        # Parse filing key
        parts = filing_key.split('_')
        ticker = parts[0]
        period = '_'.join(parts[1:])

        # Only annual reports have multi-year summaries
        if not period.startswith('Annual'):
            continue

        year_match = re.search(r'(\d{4})', period)
        if not year_match:
            continue
        year = year_match.group(1)

        # Get doc folder from filing path
        filing_path = filing_data.get('filing_path', '')
        doc = Path(filing_path).name if filing_path else f"{ticker}_Annual_{year}"

        if args.ticker and ticker != args.ticker.upper():
            continue
        if args.year and year != args.year:
            continue

        filings_to_process.append((ticker, year, doc, pages))

    print(f"Filings with multi-year pages: {len(filings_to_process)}")

    if not filings_to_process:
        print("No filings to process")
        return

    # Process
    all_rows = []
    success = errors = 0

    for i, (ticker, year, doc, pages) in enumerate(filings_to_process, 1):
        print(f"[{i}/{len(filings_to_process)}] {ticker}_{year} ({len(pages)} pages)")

        data, rows = process_filing(ticker, year, doc, pages, client, ticker_meta)

        if data:
            success += 1
            all_rows.extend(rows)
        else:
            errors += 1

    # Write flattened output
    with open(OUTPUT_JSONL, 'w') as f:
        for row in all_rows:
            f.write(json.dumps(row) + '\n')

    print()
    print("=" * 70)
    print(f"COMPLETE: {success} success, {errors} errors")
    print(f"Rows: {len(all_rows)}")
    print(f"Output: {OUTPUT_DIR}/")
    print(f"Flattened: {OUTPUT_JSONL}")
    print("=" * 70)


if __name__ == "__main__":
    main()
