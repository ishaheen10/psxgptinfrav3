#!/usr/bin/env python3
"""
Step 10: Extract Executive Compensation Data (Optional)

Extracts executive compensation tables from annual reports and flattens to JSONL.

Input:  Pages tagged with 'ceo_comp' in extraction manifest
        markdown_pages/{TICKER}/{YEAR}/...
Output: compensation/{TICKER}_{year}.json
        compensation_flattened.jsonl (for database upload)

Roles extracted:
- CEO
- Chairman
- Executive Directors
- Non-Executive Directors
- Executives (other senior management)

Fields per role:
- persons: Number of people
- base_salary: Base salary amount
- bonus: Performance bonus
- housing: Housing allowance
- retirement: Retirement benefits
- other_benefits: Other perks
- total: Total compensation

Units Terminology:
    All values stored in 'thousands' (PKR '000).
    Extraction prompt explicitly requests thousands.

Usage:
    python3 Step10_ExtractCompensation.py                     # Process all
    python3 Step10_ExtractCompensation.py --ticker HBL        # Single ticker
    python3 Step10_ExtractCompensation.py --year 2024         # Single year
    python3 Step10_ExtractCompensation.py --flatten-only      # Just flatten existing
"""

import argparse
import json
import os
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
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
OUTPUT_DIR = PROJECT_ROOT / "compensation"
OUTPUT_JSONL = PROJECT_ROOT / "compensation_flattened.jsonl"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage3"

WORKERS = 30
write_lock = Lock()


# =============================================================================
# EXTRACTION PROMPT
# =============================================================================

EXTRACTION_PROMPT = """Extract executive compensation data from these annual report pages.

## UNIT DETECTION - CRITICAL

Look for the unit specification in the document:
- "Rupees in thousands" / "Rs. 000" / "(000)" / "Rs '000" → values in thousands
- "Rupees in millions" / "Rs. in millions" → multiply by 1000 for thousands
- "Rupees" (no multiplier) / large numbers → divide by 1000 for thousands

## OUTPUT FORMAT

Output a JSON object with this structure:

```json
{
  "unit_detected": "thousands",
  "consolidated": {
    "ceo": {"persons": 1, "base_salary": 393071, "bonus": 332602, "housing": 43898, "retirement": 0, "other": 32517, "total": 802088},
    "chairman": {"persons": 1, "base_salary": 0, "bonus": 0, "housing": 0, "retirement": 0, "other": 13050, "total": 13050},
    "exec_directors": {"persons": 2, "base_salary": 150000, "bonus": 50000, "housing": 20000, "retirement": 5000, "other": 10000, "total": 235000},
    "non_exec_directors": {"persons": 5, "base_salary": 0, "bonus": 0, "housing": 0, "retirement": 0, "other": 98875, "total": 98875},
    "executives": {"persons": 32, "base_salary": 1200348, "bonus": 827305, "housing": 342676, "retirement": 57146, "other": 257181, "total": 2684656}
  },
  "unconsolidated": null
}
```

ALL VALUES MUST BE IN THOUSANDS. Convert if source uses different units.

Only include roles that have actual compensation data. Return null for sections not found.

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


def get_compensation_pages(manifest: dict, ticker: str, period: str) -> list:
    """Get compensation page numbers for a filing."""
    filing_key = f"{ticker}_{period}"
    filing = manifest.get('filings', {}).get(filing_key, {})
    pages = filing.get('pages', {}).get('ceo_comp', [])
    return sorted(pages)


def read_pages(ticker: str, year: str, doc: str, page_nums: list) -> str:
    """Read markdown pages and combine."""
    content = []
    for page_num in page_nums:
        page_path = MARKDOWN_PAGES / ticker / year / doc / f"page_{page_num:03d}.md"
        if page_path.exists():
            content.append(f"<!-- Page {page_num} -->\n{page_path.read_text()}")
    return "\n\n---\n\n".join(content)


def extract_compensation(client, pages_content: str, ticker: str, year: str) -> dict:
    """Call DeepSeek to extract compensation data."""
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

def flatten_compensation(data: dict, ticker: str, year: str, ticker_meta: dict) -> list:
    """Flatten compensation data to database rows."""
    rows = []
    meta = ticker_meta.get(ticker, {})

    for scope in ['consolidated', 'unconsolidated']:
        scope_data = data.get(scope)
        if not scope_data:
            continue

        for role, values in scope_data.items():
            if not isinstance(values, dict):
                continue

            row = {
                'ticker': ticker,
                'company_name': meta.get('Company Name', ''),
                'industry': meta.get('Industry', ''),
                'year': int(year),
                'scope': scope,
                'role': role,
                'persons': values.get('persons'),
                'base_salary': values.get('base_salary'),
                'bonus': values.get('bonus'),
                'housing': values.get('housing'),
                'retirement': values.get('retirement'),
                'other_benefits': values.get('other'),
                'total': values.get('total'),
                'unit': 'thousands',
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
        data = extract_compensation(client, pages_content, ticker, year)

        # Save individual JSON
        output_path = OUTPUT_DIR / f"{ticker}_{year}.json"
        output_path.write_text(json.dumps(data, indent=2))

        # Flatten
        rows = flatten_compensation(data, ticker, year, ticker_meta)
        return data, rows

    except Exception as e:
        print(f"  ERROR {ticker}_{year}: {e}")
        return None, []


def main():
    parser = argparse.ArgumentParser(description="Extract executive compensation")
    parser.add_argument("--ticker", help="Single ticker")
    parser.add_argument("--year", help="Single year")
    parser.add_argument("--flatten-only", action="store_true",
                        help="Just flatten existing JSON files")
    parser.add_argument("--workers", type=int, default=WORKERS)
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 10: EXTRACT COMPENSATION")
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
            rows = flatten_compensation(data, ticker, year, ticker_meta)
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

    # Find filings with compensation pages
    filings_to_process = []
    stats = manifest.get('stats', {})

    for filing_key, filing_data in manifest.get('filings', {}).items():
        pages = filing_data.get('pages', {}).get('ceo_comp', [])
        if not pages:
            continue

        # Parse filing key
        parts = filing_key.split('_')
        ticker = parts[0]
        period = '_'.join(parts[1:])

        # Only annual reports have compensation
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

    print(f"Filings with compensation pages: {len(filings_to_process)}")

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
