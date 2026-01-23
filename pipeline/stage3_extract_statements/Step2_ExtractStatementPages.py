#!/usr/bin/env python3
"""
Step 2: Extract Statement Page Numbers

Reads the full content of pages tagged as 'statement' and identifies which pages
contain which statement types (P&L, BS, CF) for Consolidated/Unconsolidated.

Input:  artifacts/stage3/step1_statement_manifest.json
Output: artifacts/stage3/step2_statement_pages.json

Format:
{
    "AABS": {
        "annual_2021": {
            "consolidated": {"PL": [37, 38], "BS": [36], "CF": [40]},
            "unconsolidated": {"PL": [26], "BS": [25], "CF": [28]}
        }
    }
}
"""

import argparse
import json
import os
import re
from pathlib import Path
from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MARKDOWN_DIR = PROJECT_ROOT / "markdown_pages"
STATEMENT_MANIFEST = PROJECT_ROOT / "artifacts" / "stage3" / "step1_statement_manifest.json"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"

def load_statement_manifest() -> dict:
    """Load statement manifest from Step 1."""
    with open(STATEMENT_MANIFEST) as f:
        return json.load(f)


def get_page_content(markdown_dir: Path, doc: str, page_num: int) -> str:
    """Read full content of a page."""
    page_file = markdown_dir / doc / f"page_{page_num:03d}.md"
    if not page_file.exists():
        return ""

    return page_file.read_text()


def build_prompt(ticker: str, period: str, page_contents: list) -> str:
    """Build prompt to identify statement pages."""
    pages_text = "\n\n---\n\n".join([
        f"<!-- Page {pg} -->\n{content}" for pg, content in page_contents
    ])

    return f"""Identify which pages contain financial statements for this PSX filing.

TICKER: {ticker}
PERIOD: {period}

For each page, determine if it contains:
- P&L (Profit & Loss / Income Statement)
- BS (Balance Sheet / Statement of Financial Position)
- CF (Cash Flow Statement)

Also identify if the statement is:
- consolidated (group-level, includes subsidiaries)
- unconsolidated (parent company only, standalone)

## OUTPUT FORMAT (JSON only, no markdown)

{{
    "consolidated": {{
        "PL": [page_numbers],
        "BS": [page_numbers],
        "CF": [page_numbers]
    }},
    "unconsolidated": {{
        "PL": [page_numbers],
        "BS": [page_numbers],
        "CF": [page_numbers]
    }}
}}

Notes:
- Use empty arrays [] if a statement type is not found
- A statement may span multiple pages - include all pages
- Look for keywords: "Consolidated", "Unconsolidated", "Standalone", "Separate", "Parent"
- IMPORTANT: If the filing only has ONE set of statements (company has no subsidiaries),
  put those pages under "consolidated" and leave "unconsolidated" empty.
  Only use "unconsolidated" when there are TWO separate sets of statements in the filing.

## PAGE CONTENTS

{pages_text}
"""


def identify_statement_pages(client: OpenAI, prompt: str) -> dict:
    """Call DeepSeek to identify statement pages."""
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "You identify financial statement pages in PSX filings. Output valid JSON only."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1,
        max_tokens=500
    )

    content = response.choices[0].message.content or "{}"

    # Clean up response
    content = content.strip()
    if content.startswith("```json"):
        content = content[7:]
    if content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]

    try:
        return json.loads(content.strip())
    except json.JSONDecodeError:
        return {"consolidated": {"PL": [], "BS": [], "CF": []},
                "unconsolidated": {"PL": [], "BS": [], "CF": []}}


def main():
    parser = argparse.ArgumentParser(description="Extract statement page numbers from PSX filings")
    parser.add_argument("--limit", type=int, help="Limit number of filings to process")
    parser.add_argument("--ticker", help="Process only this ticker")
    args = parser.parse_args()

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("ERROR: DEEPSEEK_API_KEY not set")
        return

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")

    print("=" * 70)
    print("STEP 2: EXTRACT STATEMENT PAGE NUMBERS")
    print("=" * 70)

    manifest = load_statement_manifest()

    # Build work queue
    work_items = []
    for ticker, data in manifest.items():
        if args.ticker and ticker != args.ticker:
            continue

        for period_type in ['annuals', 'quarterlies']:
            periods = data.get(period_type, {})

            for period, info in periods.items():
                doc = info.get('doc', '')
                statement_pages = info.get('statement_pages', [])

                if not statement_pages:
                    continue

                # Build period key
                if period_type == 'annuals':
                    period_key = f"annual_{period}"
                else:
                    period_key = f"quarterly_{period}"

                work_items.append({
                    'ticker': ticker,
                    'period': period,
                    'period_key': period_key,
                    'period_type': period_type,
                    'doc': doc,
                    'statement_pages': statement_pages,
                })

    if args.limit:
        work_items = work_items[:args.limit]

    print(f"Processing {len(work_items)} filings\n")

    results = {}
    for i, item in enumerate(work_items):
        ticker = item['ticker']
        period = item['period']
        period_key = item['period_key']
        period_type = item['period_type']
        doc = item['doc']
        statement_pages = item['statement_pages']

        # Get page contents
        ticker_dir = MARKDOWN_DIR / ticker / (period if period_type == 'annuals' else period.split('-')[0])
        page_contents = []

        for pg in statement_pages:
            content = get_page_content(ticker_dir, doc, pg)
            if content:
                page_contents.append((pg, content))

        if not page_contents:
            continue

        # Build and send prompt
        prompt = build_prompt(ticker, period_key, page_contents)
        result = identify_statement_pages(client, prompt)

        if ticker not in results:
            results[ticker] = {}
        results[ticker][period_key] = result

        print(f"  [{i+1}/{len(work_items)}] {ticker} {period_key}: "
              f"PL={result.get('consolidated', {}).get('PL', [])}, "
              f"BS={result.get('consolidated', {}).get('BS', [])}, "
              f"CF={result.get('consolidated', {}).get('CF', [])}")

    # Save results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*70}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Processed: {len(work_items)} filings")


if __name__ == "__main__":
    main()
