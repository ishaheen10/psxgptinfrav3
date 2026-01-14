#!/usr/bin/env python3
"""
Direct PDF to Statement Extraction using Gemini Vision

Bypasses OCR step - sends PDF page directly to Gemini for extraction.
Use this as a fallback when OCR-based extraction fails.

Supports all statement types:
- PL: Profit & Loss Statement
- BS: Balance Sheet
- CF: Cash Flow Statement

Usage:
    python3 ExtractFromPDF.py --statement-type PL --ticker GHGL --period quarterly_2024-12-31
    python3 ExtractFromPDF.py --statement-type BS --ticker ENGRO --period annual_2024
    python3 ExtractFromPDF.py --statement-type CF --ticker LUCK --period quarterly_2024-09-30
    python3 ExtractFromPDF.py --statement-type PL --manifest failing_files.json

Units Terminology:
    All values should be extracted in 'thousands' (PKR '000).
    The prompt explicitly requests this format.
"""

import argparse
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_PAGES = PROJECT_ROOT / "pdf_pages"
PAGE_MANIFEST = PROJECT_ROOT / "artifacts" / "stage3" / "Step9_statement_page_manifest.json"
SECTION_MANIFEST = PROJECT_ROOT / "section_manifest.json"
SCHEMA_FILE = PROJECT_ROOT / "canonical_schema_fixed.json"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"

# Output directories per statement type
OUTPUT_DIRS = {
    'PL': PROJECT_ROOT / "extracted_pl",
    'BS': PROJECT_ROOT / "extracted_bs",
    'CF': PROJECT_ROOT / "extracted_cf",
}

# Statement type configs
STMT_CONFIGS = {
    'PL': {
        'name': 'Profit & Loss Statement',
        'patterns': ['profit', 'loss', 'income statement', 'p&l'],
        'key': 'PL',
    },
    'BS': {
        'name': 'Balance Sheet',
        'patterns': ['balance sheet', 'statement of financial position', 'assets', 'liabilities'],
        'key': 'BS',
    },
    'CF': {
        'name': 'Cash Flow Statement',
        'patterns': ['cash flow', 'statement of cash flows', 'cash flows'],
        'key': 'CF',
    },
}

# Configure Gemini client
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None


def load_schema() -> dict:
    with open(SCHEMA_FILE) as f:
        return json.load(f)


def load_ticker_industries() -> dict:
    with open(TICKERS_FILE) as f:
        tickers = json.load(f)
    return {t["Symbol"]: t.get("Industry", "") for t in tickers}


def get_company_type(ticker: str, industries: dict) -> str:
    industry = industries.get(ticker, "").lower()
    if "bank" in industry:
        return "BANK"
    elif "insurance" in industry or "takaful" in industry:
        return "INSURANCE"
    return "CORPORATE"


def find_doc_folder(ticker: str, period: str) -> tuple:
    """Find the document folder and year for a ticker/period."""
    with open(SECTION_MANIFEST) as f:
        section_manifest = json.load(f)

    ticker_data = section_manifest.get(ticker, {})

    if period.startswith('annual_'):
        year = period.replace('annual_', '')
        period_data = ticker_data.get('annuals', {}).get(year, {})
    else:
        period_key = period.replace('quarterly_', '')
        year = period_key.split('-')[0]
        period_data = ticker_data.get('quarterlies', {}).get(period_key, {})

    doc = period_data.get('doc', '')
    return doc, year


def find_pdf_pages(ticker: str, period: str, stmt_type: str) -> list:
    """Find PDF page files for the specified statement type."""
    with open(PAGE_MANIFEST) as f:
        manifest = json.load(f)

    doc, year = find_doc_folder(ticker, period)
    if not doc:
        return []

    pages = []
    for f in manifest.get('filings', []):
        if f['ticker'] == ticker and f['period'] == period:
            stmt_info = f.get('statements', {}).get(stmt_type, {})

            # Get both consolidated and unconsolidated pages
            for scope in ['C', 'U']:
                scope_pages = stmt_info.get(scope, {}).get('pages', [])
                pages.extend(scope_pages)

            break

    # Build full paths
    pdf_files = []
    for page_num in sorted(set(pages)):
        pdf_path = PDF_PAGES / ticker / year / doc / f"page_{page_num:03d}.pdf"
        if pdf_path.exists():
            pdf_files.append((page_num, pdf_path))

    return pdf_files


def build_prompt(stmt_type: str, ticker: str, period: str, company_type: str, schema: dict) -> str:
    """Build extraction prompt for Gemini."""
    stmt_name = STMT_CONFIGS[stmt_type]['name']
    type_schema = schema.get(company_type, schema["CORPORATE"])

    if stmt_type == 'PL':
        fields = type_schema.get("profit_loss", [])
        field_list = "\n".join([f"- {f}" for f in fields[:20]])
    elif stmt_type == 'BS':
        fields = type_schema.get("balance_sheet", [])
        field_list = "\n".join([f"- {f}" for f in fields[:25]])
    else:  # CF
        fields = type_schema.get("cash_flow", [])
        field_list = "\n".join([f"- {f}" for f in fields[:20]])

    return f"""Extract the {stmt_name} from these PDF pages.

TICKER: {ticker}
PERIOD: {period}
COMPANY TYPE: {company_type}

OUTPUT FORMAT:
Return a markdown table with columns:
| Source Item | Canonical | Current Period | Prior Period |

Where:
- Source Item: Exact text from PDF
- Canonical: Map to one of these fields (or '-' if no match):
{field_list}

IMPORTANT:
- All values in THOUSANDS (divide by 1000 if shown in full rupees)
- Use negative numbers for expenses/outflows (not parentheses)
- Include both CONSOLIDATED and UNCONSOLIDATED if present
- Start each section with: ## CONSOLIDATED or ## UNCONSOLIDATED
- Then: ### {stmt_name.upper()}
- Add metadata: UNIT_TYPE: thousands

Example output:
## CONSOLIDATED
### {stmt_name.upper()}
UNIT_TYPE: thousands

| Source Item | Canonical | 12M Dec 2024 | 12M Dec 2023 |
|-------------|-----------|--------------|--------------|
| Revenue | revenue | 45234 | 41234 |
..."""


def extract_from_pdf(pdf_files: list, stmt_type: str, ticker: str, period: str,
                     company_type: str, schema: dict) -> str:
    """Send PDFs to Gemini and extract statement."""
    if not pdf_files:
        return ""

    prompt = build_prompt(stmt_type, ticker, period, company_type, schema)

    # Read PDF files and encode
    parts = [prompt]
    for page_num, pdf_path in pdf_files:
        pdf_bytes = pdf_path.read_bytes()
        parts.append(genai.types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"))

    # Call Gemini
    response = client.models.generate_content(
        model='gemini-1.5-flash',
        contents=parts
    )

    return response.text


def process_filing(ticker: str, period: str, stmt_type: str, schema: dict,
                   industries: dict, output_dir: Path) -> dict:
    """Process a single filing."""
    company_type = get_company_type(ticker, industries)
    pdf_files = find_pdf_pages(ticker, period, stmt_type)

    if not pdf_files:
        return {'status': 'no_pages', 'ticker': ticker, 'period': period}

    try:
        result = extract_from_pdf(pdf_files, stmt_type, ticker, period, company_type, schema)

        if result:
            output_path = output_dir / f"{ticker}_{period}.md"
            output_path.write_text(result)
            return {'status': 'success', 'ticker': ticker, 'period': period, 'pages': len(pdf_files)}
        else:
            return {'status': 'empty_response', 'ticker': ticker, 'period': period}

    except Exception as e:
        return {'status': 'error', 'ticker': ticker, 'period': period, 'error': str(e)}


def main():
    parser = argparse.ArgumentParser(description="Extract statements directly from PDF using Gemini")
    parser.add_argument("--statement-type", required=True, choices=['PL', 'BS', 'CF'],
                        help="Statement type to extract")
    parser.add_argument("--ticker", help="Single ticker")
    parser.add_argument("--period", help="Period (e.g., annual_2024, quarterly_2024-09-30)")
    parser.add_argument("--manifest", help="JSON file with list of ticker/period pairs to process")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers (default: 5)")
    args = parser.parse_args()

    if not client:
        print("ERROR: GEMINI_API_KEY not set")
        return

    if not args.ticker and not args.manifest:
        print("ERROR: Must specify --ticker or --manifest")
        return

    print("=" * 70)
    print(f"EXTRACT {args.statement_type} FROM PDF (GEMINI)")
    print("=" * 70)

    schema = load_schema()
    industries = load_ticker_industries()
    output_dir = OUTPUT_DIRS[args.statement_type]
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build list of filings to process
    filings = []
    if args.manifest:
        with open(args.manifest) as f:
            data = json.load(f)
            for item in data:
                if isinstance(item, dict):
                    filings.append((item['ticker'], item['period']))
                else:
                    # Assume format "TICKER_period"
                    parts = item.split('_', 1)
                    if len(parts) == 2:
                        filings.append((parts[0], parts[1]))
    else:
        filings.append((args.ticker, args.period))

    print(f"Filings to process: {len(filings)}")
    print(f"Output: {output_dir}")
    print()

    success = errors = 0

    if len(filings) == 1:
        # Single filing - don't use threading
        ticker, period = filings[0]
        result = process_filing(ticker, period, args.statement_type, schema, industries, output_dir)
        if result['status'] == 'success':
            print(f"SUCCESS: {ticker}_{period} ({result['pages']} pages)")
            success = 1
        else:
            print(f"FAILED: {ticker}_{period} - {result.get('error', result['status'])}")
            errors = 1
    else:
        # Multiple filings - use thread pool
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(process_filing, t, p, args.statement_type, schema, industries, output_dir): (t, p)
                for t, p in filings
            }

            for i, fut in enumerate(as_completed(futures), 1):
                ticker, period = futures[fut]
                result = fut.result()

                if result['status'] == 'success':
                    print(f"[{i}/{len(filings)}] {ticker}_{period}: SUCCESS")
                    success += 1
                else:
                    print(f"[{i}/{len(filings)}] {ticker}_{period}: {result.get('error', result['status'])}")
                    errors += 1

    print()
    print("=" * 70)
    print(f"COMPLETE: {success} success, {errors} errors")
    print("=" * 70)


if __name__ == "__main__":
    main()
