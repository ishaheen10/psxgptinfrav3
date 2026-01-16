#!/usr/bin/env python3
"""
Step 3: Extract Cash Flow Statements with Ref Column

Extracts Cash Flow statements with arithmetic reference columns:
- Input rows get letters (A, B, C, ...)
- Calc rows show formulas (C=A+B, F=D+E, etc.)

Input:  artifacts/stage3/step2_statement_pages.json
Output: data/extracted_cf/{ticker}_{period}_{section}.md

Usage:
    python3 Step3_ExtractCF.py                    # Process all
    python3 Step3_ExtractCF.py --ticker LUCK      # Single ticker
    python3 Step3_ExtractCF.py --limit 10         # First 10
    python3 Step3_ExtractCF.py --workers 50       # Parallel workers
"""

import argparse
import json
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MARKDOWN_DIR = PROJECT_ROOT / "markdown_pages"
STATEMENT_PAGES = PROJECT_ROOT / "artifacts" / "stage3" / "step2_statement_pages.json"
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
SCHEMA_FILE = PROJECT_ROOT / "canonical_schema_fixed.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "extracted_cf"
CHECKPOINT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step3_cf_checkpoint.json"

# DeepSeek config
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_EXTRACT_MODEL", "deepseek-reasoner")
MAX_RETRIES = 3
RETRY_WAIT = 5.0


def load_statement_pages() -> dict:
    """Load statement pages from Step 2."""
    with open(STATEMENT_PAGES) as f:
        return json.load(f)


def load_ticker_info() -> tuple[dict, dict]:
    """Load ticker to industry and fiscal period mappings."""
    if not TICKERS_FILE.exists():
        return {}, {}
    with open(TICKERS_FILE) as f:
        tickers = json.load(f)
    industries = {t["Symbol"]: t.get("Industry", "") for t in tickers}
    fiscal_periods = {t["Symbol"]: t.get("fiscal_period", "06-30") for t in tickers}
    return industries, fiscal_periods


def load_schema() -> dict:
    """Load canonical schema."""
    with open(SCHEMA_FILE) as f:
        return json.load(f)


def load_checkpoint() -> set:
    """Load completed items from checkpoint."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f).get("completed", []))
    return set()


def save_checkpoint(completed: set):
    """Save checkpoint."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"completed": sorted(completed)}, f, indent=2)


def get_company_type(ticker: str, industries: dict) -> str:
    """Get company type from ticker industry."""
    industry = industries.get(ticker, "").lower()
    if "bank" in industry:
        return "BANK"
    elif "insurance" in industry or "takaful" in industry:
        return "INSURANCE"
    return "CORPORATE"


def get_markdown_path(ticker: str, period: str) -> Path:
    """Get path to markdown pages for a filing."""
    if period.startswith('annual_'):
        year = period.replace('annual_', '')
        folder_name = f"{ticker}_Annual_{year}"
    elif period.startswith('quarterly_'):
        date_part = period.replace('quarterly_', '')
        year = date_part.split('-')[0]
        folder_name = f"{ticker}_Quarterly_{date_part}"
    else:
        return Path()
    return MARKDOWN_DIR / ticker / year / folder_name


def load_source_pages(markdown_path: Path, page_nums: list) -> list:
    """Load content from source pages."""
    pages = []
    for num in page_nums:
        page_file = markdown_path / f"page_{num:03d}.md"
        if page_file.exists():
            pages.append((num, page_file.read_text()))
    return pages


def get_type_specific_note(company_type: str) -> str:
    """Get brief company-type note for cash flow."""
    if company_type == "BANK":
        return """
Bank cash flows typically have multiple intermediate subtotals before reaching cfo/cfi/cff.
Extract ALL subtotals shown in the source - the structure varies by bank."""
    return ""


def build_prompt(pages: list, ticker: str, period: str, section: str,
                 company_type: str, schema: dict, fiscal_period: str = "06-30") -> str:
    """Build Cash Flow extraction prompt with Ref column."""

    type_schema = schema.get(company_type, schema["CORPORATE"])
    cf_fields = type_schema["cash_flow"]

    page_content = "\n\n---\n\n".join([
        f"<!-- Page {pg} -->\n{content}" for pg, content in pages
    ])

    type_note = get_type_specific_note(company_type)
    section_label = section.upper()

    # Parse fiscal period to get month names and quarter mapping
    fy_month = int(fiscal_period.split("-")[0])
    month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                   7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    month_names_full = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                        7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
    fy_month_name = month_names_full.get(fy_month, "June")
    fy_start_month = (fy_month % 12) + 1
    fy_start_month_name = month_names_full.get(fy_start_month, "July")

    # Build quarter-to-month mapping for this fiscal year
    q1_month = ((fy_month + 3 - 1) % 12) + 1   # 3 months after FY start
    q2_month = ((fy_month + 6 - 1) % 12) + 1   # 6 months after FY start
    q3_month = ((fy_month + 9 - 1) % 12) + 1   # 9 months after FY start
    q4_month = fy_month                         # 12 months = FY end

    quarter_mapping = f"""- {month_names[q1_month]} period → 3M (Q1)
- {month_names[q2_month]} period → 6M cumulative or 3M if "Quarter ended"
- {month_names[q3_month]} period → 9M cumulative or 3M if "Quarter ended"
- {month_names[q4_month]} period → 12M (full year) or 3M if "Quarter ended\""""

    return f"""Extract the {section_label} Cash Flow Statement from these PSX filing pages.

TICKER: {ticker}
PERIOD: {period}
SECTION: {section_label}
COMPANY TYPE: {company_type}
FISCAL YEAR END: {fy_month_name} ({fiscal_period})
{type_note}

## OUTPUT FORMAT

```markdown
# {ticker} - {period}
UNIT_TYPE: thousands | millions | rupees

## {section_label}

### CASH FLOW
| Source Item | Canonical | Ref | 12M Dec 2024 | 12M Dec 2023 |
|:---|:---|:---|---:|---:|
| [source line item] | [canonical field] | A | 500,000 | 450,000 |
| [source line item] | [canonical field] | B | 50,000 | 45,000 |
| ... | ... | ... | ... | ... |
| **[Operating section total]** | **cfo** | G=A+B+... | 475,000 | 462,000 |
| ... | ... | ... | ... | ... |
| **[Investing section total]** | **cfi** | J=H+I+... | (80,000) | (65,000) |
| ... | ... | ... | ... | ... |
| **[Financing section total]** | **cff** | M=K+L+... | 10,000 | (65,000) |
| [Net change line] | net_cash_change | N=G+J+M | 405,000 | 332,000 |
| [Opening cash line] | cash_start | O | 200,000 | 168,000 |
| [Closing cash line] | cash_end | P=N+O | 605,000 | 200,000 |
```

## EXTRACTION RULES

1. **Section**: Extract ONLY the {section_label} Cash Flow - ignore other sections
2. **Line items**: Extract each line EXACTLY as shown. Do NOT aggregate items. Multiple rows CAN share the same canonical label if they are distinct items.
3. **Subtotals**: Bold in both Source Item and Canonical columns (cfo, cfi, cff, net_cash_change)
4. **Numbers**: Use comma separators (1,234,567). Convert spaces or other formats.
5. **No hallucination**: Only extract values that exist in the source
6. **Note references are NOT values**: Numbers like "16", "17.1", "20" in a "Note" column are footnote references - ignore them. Only extract values from the numeric period columns.
7. **Column alignment**: Each value must come from the correct period column. Do not shift values between columns.

## UNIT_TYPE

Copy the EXACT unit from the source document header or footer:
- "(Rupees in '000)" or "(Rs. in thousands)" → thousands
- "(Rs. in millions)" → millions
- "(Rupees)" with no scale indicator → rupees

If no unit indicator is found, default to "thousands".

## SIGN CONVENTION

Copy the EXACT signs from the source document. Do NOT flip or interpret signs.
- Numbers in parentheses in source → output with parentheses: (100,000)
- Numbers without parentheses in source → output without: 100,000

## REF COLUMN

- **Input rows**: Sequential letters (A, B, C, ..., Z, AA, AB, ...)
- **Calc rows**: Formula showing addition, e.g., G=A+B+C (formulas use ADDITION ONLY, parentheses handle the sign)

## FORMULA VALIDATION

Values from the source are always correct - never adjust numbers to make formulas work.

Formulas must compute correctly:
- If you write F=A+B+C, then F's value must equal A+B+C
- If a formula doesn't validate, adjust the formula (which items are included), not the values

Mirror the source document's subtotal structure - don't impose a standard hierarchy.

## PERIOD COLUMNS

Extract EVERY numeric column in the table. Do not skip any columns.

If the source shows BOTH quarterly (3M) AND cumulative (6M/9M) columns for the same period end date, extract ALL of them.

**Column header format**: Duration + Month + Year (e.g., "3M Sep 2024", "12M Jun 2025")

**Duration from source text**:
- "Quarter ended" or "Three months ended" → 3M
- "Half year ended" or "Six months ended" → 6M
- "Nine months ended" → 9M
- "Year ended" or "Annual" → 12M

**This company's fiscal year ends in {fy_month_name} (starts {fy_start_month_name}). For this FY:**
{quarter_mapping}

If source says only "Period ended [date]" without specifying duration, use the mapping above.

## SECTION TOTALS

- **cfo, cfi, cff**: Map ONLY to final section totals, not intermediate subtotals
- Individual line items within each section use their specific canonical fields

## PAGE VALIDATION

If the page does NOT contain a Cash Flow statement, output ONLY one of these flags:
- `PAGE_ERROR: NO_CF_FOUND` - Page does not contain any Cash Flow statement
- `PAGE_ERROR: PROFIT_LOSS_ONLY` - Page only contains a Profit & Loss statement
- `PAGE_ERROR: BALANCE_SHEET_ONLY` - Page only contains a Balance Sheet
- `PAGE_ERROR: NOTES_ONLY` - Page only contains notes to financial statements

If the page contains a Cash Flow statement, extract it regardless of any consolidation labels.

## CANONICAL FIELDS

{', '.join(cf_fields)}

## SOURCE PAGES

{page_content}
"""


def extract_cf(client: OpenAI, prompt: str) -> str:
    """Call DeepSeek to extract Cash Flow with retry logic."""
    messages = [
        {"role": "system", "content": "You extract financial statements from PSX filings into structured markdown tables. Output ONLY the markdown, no explanations."},
        {"role": "user", "content": prompt}
    ]

    attempt = 0
    while True:
        attempt += 1
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=messages,
                temperature=0.1,
            )
            return response.choices[0].message.content or ""
        except Exception as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"DeepSeek request failed: {exc}") from exc
            time.sleep(RETRY_WAIT)


def clean_output(output: str) -> str:
    """Clean markdown code blocks from output."""
    output = output.strip()
    if output.startswith("```markdown"):
        output = output[len("```markdown"):].strip()
    if output.startswith("```"):
        output = output[3:].strip()
    if output.endswith("```"):
        output = output[:-3].strip()
    return output


def process_item(item: dict, schema: dict, client: OpenAI) -> dict:
    """Process a single extraction item."""
    ticker = item["ticker"]
    period = item["period"]
    section = item["section"]
    pages = item["pages"]
    company_type = item["company_type"]
    fiscal_period = item.get("fiscal_period", "06-30")
    item_key = item["key"]

    result = {
        "key": item_key,
        "ticker": ticker,
        "period": period,
        "section": section,
        "success": False,
        "error": None
    }

    try:
        # Load source pages
        markdown_path = get_markdown_path(ticker, period)
        source_pages = load_source_pages(markdown_path, pages)

        if not source_pages:
            result["error"] = f"No source pages found at {markdown_path}"
            return result

        # Build and execute prompt
        prompt = build_prompt(source_pages, ticker, period, section, company_type, schema, fiscal_period)
        output = extract_cf(client, prompt)
        output = clean_output(output)

        # Save output
        out_file = OUTPUT_DIR / f"{item_key}.md"
        out_file.write_text(output)

        result["success"] = True
        result["company_type"] = company_type
        result["page_count"] = len(source_pages)

    except Exception as e:
        result["error"] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(description="Extract Cash Flow statements with Ref column")
    parser.add_argument("--ticker", help="Process only this ticker")
    parser.add_argument("--year", help="Process only this year (e.g., 2024)")
    parser.add_argument("--annual-only", action="store_true", help="Process only annual reports")
    parser.add_argument("--manifest", help="JSON manifest file with list of file keys to process")
    parser.add_argument("--limit", type=int, help="Limit number of extractions")
    parser.add_argument("--reset", action="store_true", help="Reset checkpoint")
    parser.add_argument("--workers", type=int, default=50, help="Parallel workers")
    args = parser.parse_args()

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("ERROR: DEEPSEEK_API_KEY not set")
        return

    client = OpenAI(api_key=api_key, base_url=DEEPSEEK_API_BASE.rstrip("/"))

    print("=" * 70)
    print("STEP 3: EXTRACT CASH FLOW STATEMENTS WITH REF COLUMN")
    print("=" * 70)

    # Load data
    statement_pages = load_statement_pages()
    industries, fiscal_periods = load_ticker_info()
    schema = load_schema()

    # Load or reset checkpoint
    if args.reset:
        completed = set()
    else:
        completed = load_checkpoint()
        if completed:
            print(f"Resuming from checkpoint: {len(completed)} already completed")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load manifest if provided (list of file keys to re-extract)
    manifest_keys = None
    if args.manifest:
        with open(args.manifest) as f:
            manifest_data = json.load(f)
            # Support both list of keys and list of filenames
            manifest_keys = set()
            for item in manifest_data:
                # Remove .md extension if present
                key = item.replace('.md', '')
                manifest_keys.add(key)
            print(f"Loaded manifest with {len(manifest_keys)} items to re-extract")

    # Build work queue
    work_items = []
    for ticker, periods in statement_pages.items():
        if args.ticker and ticker != args.ticker:
            continue

        company_type = get_company_type(ticker, industries)
        fiscal_period = fiscal_periods.get(ticker, "06-30")

        for period, sections in periods.items():
            # Filter by year if specified
            if args.year and f"_{args.year}" not in period:
                continue
            # Filter annual only if specified
            if args.annual_only and not period.startswith("annual_"):
                continue

            for section in ["consolidated", "unconsolidated"]:
                pages = sections.get(section, {}).get("CF", [])
                if not pages:
                    continue

                item_key = f"{ticker}_{period}_{section}"

                # If manifest provided, only process items in manifest (skip checkpoint)
                if manifest_keys is not None:
                    if item_key not in manifest_keys:
                        continue
                    # Force re-extract for manifest items (ignore checkpoint)
                elif item_key in completed:
                    continue

                work_items.append({
                    "ticker": ticker,
                    "period": period,
                    "section": section,
                    "pages": pages,
                    "company_type": company_type,
                    "fiscal_period": fiscal_period,
                    "key": item_key,
                })

    if args.limit:
        work_items = work_items[:args.limit]

    print(f"Work items: {len(work_items)}")
    print(f"Workers: {args.workers}")
    print(f"Model: {DEEPSEEK_MODEL}")
    print(f"Output: {OUTPUT_DIR}/")
    print()

    if not work_items:
        print("No items to process")
        return

    # Process in parallel
    start_time = time.time()
    results = []
    type_counts = {"CORPORATE": 0, "BANK": 0, "INSURANCE": 0}
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_item, item, schema, client): item["key"]
            for item in work_items
        }

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)

            if result["success"]:
                ctype = result.get("company_type", "CORPORATE")
                type_counts[ctype] = type_counts.get(ctype, 0) + 1
                completed.add(result["key"])
                save_checkpoint(completed)
                status = f"OK ({ctype}, {result.get('page_count', 0)} pages)"
            else:
                errors.append(result)
                status = f"FAIL: {result['error']}"

            if i <= 5 or i % 50 == 0 or i == len(futures) or not result["success"]:
                print(f"[{i}/{len(futures)}] {result['ticker']} {result['period']} {result['section']}: {status}")

    elapsed = time.time() - start_time
    successes = sum(1 for r in results if r["success"])

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Processed: {successes}/{len(work_items)} successful")
    print(f"Types: {type_counts}")
    print(f"Time: {elapsed/60:.1f} min")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in errors[:10]:
            print(f"  {err['ticker']} {err['period']} {err['section']}: {err['error'][:60]}...")

    print(f"\nOutput: {OUTPUT_DIR}/")
    print(f"Completed total: {len(completed)}")


if __name__ == "__main__":
    main()
