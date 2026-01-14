#!/usr/bin/env python3
"""
Extract P&L directly from PDF pages using Gemini Vision.

Matches the exact output format used by the targeted P&L extraction pipeline.

Usage:
    python3 ExtractPLFromPDF.py --ticker COLG --period quarterly_2024-12-31 --section consolidated --pages 8
    python3 ExtractPLFromPDF.py --manifest errors_to_reextract.json --workers 10
"""

import argparse
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_PAGES = PROJECT_ROOT / "pdf_pages"
MARKDOWN_PAGES = PROJECT_ROOT / "markdown_pages"
OUTPUT_DIR = PROJECT_ROOT / "data" / "extracted_pl_from_pdf"
SCHEMA_FILE = PROJECT_ROOT / "canonical_schema_fixed.json"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"

# Configure Gemini client
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None


EXTRACTION_PROMPT = """Extract the Profit & Loss statement from this PDF page into a structured markdown table with arithmetic reference columns.

## CONTEXT
- Ticker: {ticker}
- Period: {period}
- Section: {section}
- Company Type: {company_type}
{type_note}

## REQUIRED OUTPUT FORMAT

```markdown
# {ticker} - {period}
UNIT_TYPE: thousands | millions | rupees

## {section_upper}

### PROFIT & LOSS
| Source Item | Canonical | Ref | {columns_header} |
|:---|:---|:---|{columns_align}|
| Revenue | revenue_net | A | 1,234,567 | 1,100,000 | ... |
| Cost of sales | cost_of_revenue | B | (800,000) | (700,000) | ... |
| **Gross profit** | **gross_profit** | C=A+B | 434,567 | 400,000 | ... |
...

SOURCE_PAGES:
  PL: {section_lower}={page_nums}
```

## COLUMN EXTRACTION (CRITICAL)

- **Extract ALL data columns** from the source - there may be 2, 4, or more columns
- Common patterns:
  - Annual: 2 columns (current year, prior year)
  - Quarterly: 2-4 columns (Q current, Q prior, YTD current, YTD prior)
- **Do not skip columns** - if the source has 4 columns, output 4 columns
- Label columns correctly: "3M Mar 2025", "9M Mar 2025", "12M Jun 2024", etc.

## REF COLUMN (CRITICAL)

- **Input rows**: Sequential letters (A, B, C, ..., Z, AA, AB, ...)
- **Calc rows**: ALWAYS use addition. Example: C=A+B where B is negative like (800,000)
- The formula must ACTUALLY compute correctly. Verify: C = A + B should equal the stated value of C.

## SIGN CONVENTION (CRITICAL)

- Costs, expenses, taxes = NEGATIVE in parentheses: (800,000)
- Revenue, income, profit = POSITIVE: 1,234,567
- ALL formulas use ADDITION ONLY: C=A+B, E=C+D+F (never use minus sign in formulas)
- Example: Revenue 1,000 + Cost (600) = Profit 400 → written as C=A+B where A=1,000, B=(600)

## CANONICAL FIELD MAPPINGS

**Corporate:**
- revenue_net, cost_of_revenue, gross_profit
- operating_expenses, operating_profit
- other_income, finance_cost
- profit_before_tax, taxation, net_profit, eps

**Banks:**
- bank_interest_income, bank_interest_expense, net_interest_income
- fee_income, fx_income, dividend_income, trading_gains, non_interest_income
- operating_expenses, provisions, profit_before_tax, net_profit

**Insurance:**
- net_premium, claims_expense, commission_expense, underwriting_profit
- investment_income, profit_before_tax, net_profit

## FORMATTING RULES

1. Use parentheses for ALL negative values (costs, expenses, taxes, losses)
2. NUMBER FORMAT: Always use commas as thousand separators (1,234,567). If source uses spaces (1 234 567) or no separators, convert to comma format.
3. Bold (**) subtotals in BOTH Source Item AND Canonical columns
4. Period columns: 3M/6M/9M/12M + Mon + YYYY (e.g., "12M Dec 2024")
5. UNIT_TYPE: Look for "(Rs. in '000)" -> thousands, "(Rs. in millions)" -> millions
6. ONLY use + in formulas - NEVER use minus sign

## CRITICAL RULES

1. NEVER AGGREGATE: Extract each line item EXACTLY as it appears in the source. If source shows "Selling expenses", "Administrative expenses", "Other charges" as separate lines, keep them separate (do NOT combine into one "Operating expenses" row). Multiple rows CAN share the same canonical label.

2. NEVER HALLUCINATE: Only extract values that exist in the source. If the PDF page does NOT contain a Profit & Loss statement, output ONLY: "ERROR: No P&L statement found in source pages". Do NOT invent or guess values.

3. Extract ALL line items shown in the P&L
4. Formulas MUST be arithmetically correct - verify each one
5. Include intermediate subtotals (e.g., "Total operating expenses", "Profit before provisions")

Output ONLY the markdown content, nothing else."""


def get_type_specific_note(company_type: str) -> str:
    """Get brief company-type note."""
    if company_type == "BANK":
        return """
Key bank fields: bank_interest_income, bank_interest_expense -> net_interest_income (instead of gross_profit)
Non-interest income: fee_income, fx_income, dividend_income, trading_gains
Expenses: operating_expenses, provisions"""
    elif company_type == "INSURANCE":
        return """
Key insurance fields: gross_premium, net_premium, claims_expense -> underwriting_profit (instead of gross_profit)
Other income: investment_income"""
    return ""


def load_schema() -> dict:
    """Load canonical schema."""
    if SCHEMA_FILE.exists():
        with open(SCHEMA_FILE) as f:
            return json.load(f)
    return {}


def load_ticker_industries() -> dict:
    """Load ticker to industry mapping."""
    if TICKERS_FILE.exists():
        with open(TICKERS_FILE) as f:
            tickers = json.load(f)
        return {t["Symbol"]: t.get("Industry", "") for t in tickers}
    return {}


def get_company_type(ticker: str, industries: dict) -> str:
    """Determine company type from industry."""
    industry = industries.get(ticker, "").lower()
    if "bank" in industry:
        return "BANK"
    elif "insurance" in industry or "takaful" in industry:
        return "INSURANCE"
    return "CORPORATE"


def find_pdf_path(ticker: str, period: str, page_num: int) -> Path:
    """Find PDF page file."""
    if period.startswith('annual_'):
        year = period.replace('annual_', '')
        folder = f"{ticker}_Annual_{year}"
    else:
        date_part = period.replace('quarterly_', '')
        year = date_part.split('-')[0]
        folder = f"{ticker}_Quarterly_{date_part}"

    return PDF_PAGES / ticker / year / folder / f"page_{page_num:03d}.pdf"


def determine_columns(period: str) -> list[str]:
    """Determine likely column headers from period. Returns list of possible columns."""
    if period.startswith('annual_'):
        year = period.replace('annual_', '')
        return [f"12M Dec {year}", f"12M Dec {int(year)-1}"]
    else:
        date_part = period.replace('quarterly_', '')
        year, month, day = date_part.split('-')
        month_names = {'03': 'Mar', '06': 'Jun', '09': 'Sep', '12': 'Dec'}
        month_name = month_names.get(month, month)
        # Quarterly reports often have 4 columns: Q current, Q prior, YTD current, YTD prior
        q_duration = "3M"
        ytd_duration = {"03": "9M", "06": "12M", "09": "3M", "12": "6M"}.get(month, "9M")
        # For Q3 (March for June FY), show both quarter and 9M columns
        if month == "03":
            return [
                f"3M {month_name} {year}", f"3M {month_name} {int(year)-1}",
                f"9M {month_name} {year}", f"9M {month_name} {int(year)-1}"
            ]
        elif month == "09":
            return [
                f"3M {month_name} {year}", f"3M {month_name} {int(year)-1}",
                f"3M {month_name} {year}", f"3M {month_name} {int(year)-1}"
            ]
        else:
            return [f"3M {month_name} {year}", f"3M {month_name} {int(year)-1}"]


def extract_pl_from_pdf(
    ticker: str,
    period: str,
    section: str,
    page_nums: list[int],
    company_type: str
) -> str:
    """Extract P&L from PDF pages using Gemini."""

    # Find PDF files
    pdf_files = []
    for pn in page_nums:
        pdf_path = find_pdf_path(ticker, period, pn)
        if pdf_path.exists():
            pdf_files.append((pn, pdf_path))

    if not pdf_files:
        raise ValueError(f"No PDF files found for pages {page_nums}")

    # Determine likely columns (hint for the model)
    columns = determine_columns(period)
    columns_header = ' | '.join(columns)
    columns_align = ' | '.join(['---:'] * len(columns))

    # Build prompt
    type_note = get_type_specific_note(company_type)
    prompt = EXTRACTION_PROMPT.format(
        ticker=ticker,
        period=period,
        section=section,
        section_upper=section.upper(),
        section_lower=section.lower(),
        company_type=company_type,
        type_note=type_note,
        columns_header=columns_header,
        columns_align=columns_align,
        page_nums=','.join(str(p) for p in page_nums)
    )

    # Build request parts
    parts = [prompt]
    for page_num, pdf_path in pdf_files:
        pdf_bytes = pdf_path.read_bytes()
        parts.append(genai.types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"))

    # Call Gemini
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=parts
    )

    result = response.text

    # Clean up response (remove markdown code blocks if present)
    if result.startswith('```'):
        lines = result.split('\n')
        # Remove first and last lines if they're code block markers
        if lines[0].startswith('```'):
            lines = lines[1:]
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        result = '\n'.join(lines)

    return result


def get_pages_from_manifest(ticker: str, period: str, section: str) -> list[int]:
    """Get page numbers from authoritative statement pages manifest."""
    manifest_path = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"

    if not manifest_path.exists():
        print(f"WARNING: Manifest not found: {manifest_path}")
        return []

    with open(manifest_path) as f:
        manifest = json.load(f)

    # Look up pages from manifest
    if ticker not in manifest:
        print(f"WARNING: Ticker {ticker} not in manifest")
        return []

    if period not in manifest[ticker]:
        print(f"WARNING: Period {period} not in manifest for {ticker}")
        return []

    section_data = manifest[ticker][period].get(section, {})
    pages = section_data.get("PL", [])

    return pages


def process_single(
    ticker: str,
    period: str,
    section: str,
    pages: list[int],
    industries: dict,
    output_dir: Path
) -> dict:
    """Process a single extraction."""
    result = {
        'ticker': ticker,
        'period': period,
        'section': section,
        'pages': pages,
        'status': 'error'
    }

    try:
        company_type = get_company_type(ticker, industries)

        # Get pages if not specified - use authoritative manifest
        if not pages:
            pages = get_pages_from_manifest(ticker, period, section)

        if not pages:
            result['error'] = 'no_pages_found'
            return result

        result['pages'] = pages

        # Extract
        content = extract_pl_from_pdf(ticker, period, section, pages, company_type)

        if not content or '### PROFIT & LOSS' not in content:
            result['error'] = 'invalid_extraction'
            return result

        # Fix SOURCE_PAGES - replace Gemini's hallucinated pages with actual pages from manifest
        page_nums_str = ','.join(str(p) for p in pages)
        correct_source_pages = f"SOURCE_PAGES:\n  PL: {section}={page_nums_str}"

        # Remove any existing SOURCE_PAGES and append correct one
        lines = content.split('\n')
        filtered_lines = []
        skip_next = False
        for line in lines:
            if line.startswith('SOURCE_PAGES:'):
                skip_next = True
                continue
            if skip_next and line.startswith('  '):
                continue
            skip_next = False
            filtered_lines.append(line)

        content = '\n'.join(filtered_lines).rstrip() + '\n\n' + correct_source_pages + '\n'

        # Save
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{ticker}_{period}_{section}.md"
        output_file.write_text(content)

        result['status'] = 'success'
        result['output'] = str(output_file)

    except Exception as e:
        result['error'] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(description="Extract P&L from PDF using Gemini")
    parser.add_argument("--ticker", help="Ticker symbol")
    parser.add_argument("--period", help="Period (annual_2024 or quarterly_2024-12-31)")
    parser.add_argument("--section", choices=['consolidated', 'unconsolidated'],
                        help="Section to extract")
    parser.add_argument("--pages", help="Page numbers (comma-separated, e.g., 8,9)")
    parser.add_argument("--manifest", help="JSON manifest with files to process")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers")
    parser.add_argument("--output-dir", help="Output directory")
    args = parser.parse_args()

    if not client:
        print("ERROR: GEMINI_API_KEY not set")
        return

    print("=" * 60)
    print("EXTRACT P&L FROM PDF (GEMINI)")
    print("=" * 60)

    industries = load_ticker_industries()
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    # Build work list
    work_items = []

    if args.manifest:
        # Load from manifest
        with open(args.manifest) as f:
            manifest = json.load(f)

        for item in manifest:
            if isinstance(item, dict):
                work_items.append({
                    'ticker': item['ticker'],
                    'period': item['period'],
                    'section': item.get('section', 'consolidated'),
                    'pages': item.get('pages', [])
                })
            elif isinstance(item, str):
                # Format: TICKER_period_section
                parts = item.rsplit('_', 1)
                if len(parts) == 2:
                    ticker_period, section = parts
                    tp_parts = ticker_period.split('_', 1)
                    if len(tp_parts) == 2:
                        work_items.append({
                            'ticker': tp_parts[0],
                            'period': tp_parts[1],
                            'section': section,
                            'pages': []
                        })
    else:
        if not args.ticker or not args.period or not args.section:
            print("ERROR: Must specify --ticker, --period, --section OR --manifest")
            return

        pages = [int(p) for p in args.pages.split(',')] if args.pages else []
        work_items.append({
            'ticker': args.ticker,
            'period': args.period,
            'section': args.section,
            'pages': pages
        })

    print(f"Files to process: {len(work_items)}")
    print(f"Output: {output_dir}")
    print(f"Workers: {args.workers}")
    print()

    success = 0
    errors = 0
    results = []

    if len(work_items) == 1:
        # Single item
        item = work_items[0]
        result = process_single(
            item['ticker'], item['period'], item['section'],
            item['pages'], industries, output_dir
        )
        results.append(result)
        if result['status'] == 'success':
            print(f"SUCCESS: {item['ticker']}_{item['period']}_{item['section']}")
            success = 1
        else:
            print(f"FAILED: {result.get('error', 'unknown')}")
            errors = 1
    else:
        # Multiple items with threading
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(
                    process_single,
                    item['ticker'], item['period'], item['section'],
                    item['pages'], industries, output_dir
                ): item
                for item in work_items
            }

            for i, fut in enumerate(as_completed(futures), 1):
                item = futures[fut]
                result = fut.result()
                results.append(result)

                key = f"{item['ticker']}_{item['period']}_{item['section']}"
                if result['status'] == 'success':
                    print(f"[{i}/{len(work_items)}] {key}: SUCCESS")
                    success += 1
                else:
                    print(f"[{i}/{len(work_items)}] {key}: {result.get('error', 'failed')}")
                    errors += 1

    print()
    print("=" * 60)
    print(f"COMPLETE: {success} success, {errors} errors")
    print("=" * 60)

    # Save results
    results_file = output_dir / "_extraction_results.json"
    results_file.write_text(json.dumps(results, indent=2))
    print(f"Results saved: {results_file}")


if __name__ == "__main__":
    main()
