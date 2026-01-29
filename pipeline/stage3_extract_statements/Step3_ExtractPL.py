#!/usr/bin/env python3
"""
Step 3: Extract Profit & Loss Statements with Ref Column

Extracts P&L statements with arithmetic reference columns:
- Input rows get letters (A, B, C, ...)
- Calc rows show formulas (C=A-B, F=D+E, etc.)

Input:  artifacts/stage3/step2_statement_pages.json
Output: data/extracted_pl/{ticker}_{period}_{section}.md

Usage:
    python3 Step3_ExtractPL.py                    # Process all
    python3 Step3_ExtractPL.py --ticker LUCK      # Single ticker
    python3 Step3_ExtractPL.py --limit 10         # First 10
    python3 Step3_ExtractPL.py --workers 50       # Parallel workers
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
OUTPUT_DIR = PROJECT_ROOT / "data" / "extracted_pl"
CHECKPOINT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step3_checkpoint.json"

# DeepSeek config
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_EXTRACT_MODEL", "deepseek-chat")
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
    """Get brief company-type note."""
    if company_type == "BANK":
        return """
Key bank fields: bank_interest_income, bank_interest_expense -> net_interest_income (instead of gross_profit)
Non-interest income: fee_income, fx_income, dividend_income, trading_gains
Expenses: operating_expenses, provisions

PROVISIONS AND REVERSALS: Banks show provisions with REVERSED parentheses convention.
- When the source shows provisions IN PARENTHESES like (2,710,139) → this is a REVERSAL/CREDIT → store as POSITIVE (adds to profit)
- When the source shows provisions WITHOUT parentheses like 2,976,973 → this is an EXPENSE → store as NEGATIVE (reduces profit)
- This is OPPOSITE of the normal parentheses convention - for bank provisions, parentheses = good (reversal), no parentheses = bad (expense)"""
    elif company_type == "INSURANCE":
        return """
Key insurance fields: gross_premium, net_premium, claims_expense -> underwriting_profit (instead of gross_profit)
Other income: investment_income

CRITICAL SIGN CONVENTION FOR INSURANCE:
- ALL expense items must be NEGATIVE (in parentheses): claims_expense, commission_expense, operating_expenses, acquisition_expenses, other_underwriting
- Reinsurance RECOVERIES are POSITIVE (they reduce the expense/add back money)
- Net claims = Claims (negative) + Recoveries (positive) = net negative
- Example: If "Insurance benefits" is 479,719 in source, store as (479,719) because it's an expense
- Example: If "Reinsurance recoveries" is 331,635 in source, store as 331,635 (positive, it's a recovery)

INSURANCE P&L STRUCTURE:
Insurance P&Ls have TWO separate expense sections, each with its own subtotal:
1. Claims section → ends with "Net claims" or "Net insurance benefits" subtotal
2. Operating expenses section → ends with "Total expenses" subtotal

These are SEPARATE subtotals. "Total expenses" does NOT include the claims subtotal.
The formula for each subtotal should only include items within that section.

HIERARCHICAL SUBTOTALS: Insurance P&Ls often show subtotals that already include prior line items.
- If "Net claims" = "Claims incurred" + "Reinsurance recoveries", don't include all three in formulas
- Use the subtotal OR the components, not both (to avoid double-counting)"""
    return ""


def build_prompt(pages: list, ticker: str, period: str, section: str,
                 company_type: str, schema: dict, fiscal_period: str = "06-30") -> str:
    """Build P&L extraction prompt with Ref column."""

    type_schema = schema.get(company_type, schema["CORPORATE"])
    pl_fields = type_schema["profit_loss"]

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
    fy_start_month = (fy_month % 12) + 1  # Month after FY end
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

    return f"""Extract the {section_label} Profit & Loss statement from these PSX filing pages.

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

### PROFIT & LOSS
| Source Item | Canonical | Ref | 12M Dec 2024 | 12M Dec 2023 |
|:---|:---|:---|---:|---:|
| Revenue | revenue_net | A | 1,234,567 | 1,100,000 |
| Cost of sales | cost_of_revenue | B | (800,000) | (700,000) |
| **Gross profit** | **gross_profit** | C=A+B | 434,567 | 400,000 |
| Selling expenses | operating_expenses | D | (80,000) | (60,000) |
| Administrative expenses | operating_expenses | E | (50,000) | (40,000) |
| Other charges | operating_expenses | F | (20,000) | (20,000) |
| Other income | other_income | G | 10,000 | 8,000 |
| **Operating profit** | **operating_profit** | H=C+D+E+F+G | 294,567 | 288,000 |
...
```

## EXTRACTION RULES

1. **Section**: Extract ONLY the {section_label} P&L - ignore other sections
2. **Line items**: Extract each line EXACTLY as shown. Do NOT aggregate (e.g., keep "Selling expenses" and "Administrative expenses" separate, do not combine into "Operating expenses"). Multiple rows CAN share the same canonical label.
3. **Subtotals**: Bold in both Source Item and Canonical columns
4. **Numbers**: Use comma separators (1,234,567). Convert spaces or other formats.
5. **No hallucination**: Only extract values that exist in the source

## UNIT_TYPE

Look for scale indicators in the document header, footer, or column headers. The key word to find is the SCALE (thousand/million), not the currency.

**Decision rules (check in this order):**
1. If you see "'000", "thousand", "in 000s", or "000's" anywhere → **thousands**
2. If you see "million" or "in millions" → **millions**
3. If you see ONLY "Rupees" or "Rs." with NO scale indicator → **rupees**
4. If no unit indicator found → default to **thousands**

**Common patterns that mean THOUSANDS:**
- "Rupees in thousand" → thousands
- "Rs. in '000" → thousands
- "(Rupees in '000)" → thousands
- "Amount in PKR '000" → thousands
- "(Rs. '000)" → thousands

**IMPORTANT**: "Rupees in thousand" means thousands, NOT rupees. The scale indicator always takes precedence over the currency name.

## CURRENCY

Some pages present data in multiple currencies (e.g., USD and PKR side by side). Always extract the PKR (Pakistan Rupees) values.

## SIGN CONVENTION

- Costs, expenses, taxes = NEGATIVE in parentheses: (800,000)
- Revenue, income, profit = POSITIVE: 1,234,567
- Formulas use ADDITION ONLY: C=A+B (parentheses handle the sign, never use minus)

**"Less:" and "Add:" prefixes**: Line items prefixed with "Less:" are deductions and must be stored as negatives (in parentheses), even if the source shows them without parentheses. Line items prefixed with "Add:" are additions and should be positive.

## REF COLUMN

- **Input rows**: Sequential letters (A, B, C, ..., Z, AA, AB, ...)
- **Calc rows**: Formula showing addition, e.g., C=A+B

## PERIOD COLUMNS

Extract EVERY numeric column in the table. Do not skip any columns.

If the source shows BOTH quarterly (3M) AND cumulative (6M/9M) columns for the same period end date, extract ALL of them. Example: a Sep quarterly filing may have "Quarter ended Sep 30" (3M) AND "Half year ended Sep 30" (6M) - extract both.

**Column header format**: Duration + Month + Year (e.g., "3M Sep 2024", "12M Jun 2025")

**Duration from source text**:
- "Quarter ended" or "Three months ended" → 3M
- "Half year ended" or "Six months ended" → 6M
- "Nine months ended" → 9M
- "Year ended" or "Annual" → 12M

**This company's fiscal year ends in {fy_month_name} (starts {fy_start_month_name}). For this FY:**
{quarter_mapping}

If source says only "Period ended [date]" without specifying duration, use the mapping above.

## PAGE VALIDATION

If pages do NOT contain a {section_label} P&L statement, output ONLY one flag:
- `PAGE_ERROR: NO_PL_FOUND`
- `PAGE_ERROR: BALANCE_SHEET_ONLY`
- `PAGE_ERROR: CASH_FLOW_ONLY`
- `PAGE_ERROR: NOTES_ONLY`

**IMPORTANT**: Extract whatever periods exist on the page, regardless of the filing name. A "quarterly" filing may contain 3M, 6M, or 9M periods - extract ALL of them. The filing name does NOT determine what periods are valid.

**IMPORTANT**: If the page contains a P&L statement (Profit & Loss, Income Statement, Statement of Comprehensive Income), extract it regardless of whether it has a "consolidated" or "unconsolidated" label. Many companies are standalone entities without subsidiaries and won't have consolidation labels - still extract their P&L.

## CANONICAL FIELDS

{', '.join(pl_fields)}

**IMPORTANT**: Do NOT force a match to these fields. If a line item doesn't clearly match any canonical field above, create an appropriate snake_case name that accurately describes it. Examples:
- "Profit from continuing operations" → `net_profit_continuing`
- "Loss from discontinued operations" → `net_profit_discontinued`
- "Share of profit from associates" → `share_of_associates`
- "Workers' Welfare Fund" → `workers_welfare_fund`

The goal is accurate data capture, not forcing everything into predefined buckets.

## UNIQUE CANONICAL FIELDS (ONE OCCURRENCE ONLY)

These key fields must appear EXACTLY ONCE in the output. If you see multiple candidates, use the TOTAL line:

- `gross_profit` - The single gross profit subtotal (revenue minus COGS)
- `operating_profit` - The single operating profit subtotal
- `profit_before_tax` - The single PBT line (before any attribution breakdown)
- `net_profit` - The single TOTAL net profit (before any attribution breakdown). Use `net_profit` even if the company has a loss - negative values represent losses.
- `eps` - The single total EPS figure

**ATTRIBUTION LINES** (appear AFTER net_profit): These show how net_profit is split between owners:
- "Profit attributable to owners of parent" → `net_profit_parent` (NOT net_profit)
- "Profit attributable to non-controlling interests" → `nci_income` (NOT net_profit)
- "Equity holders of the holding company" → `net_profit_parent`

**ASSOCIATES vs NCI**:
- "Share of profit from associates/joint ventures" → `share_of_associates` (NOT other_income, NOT nci_income)
- "Share in profit of associated companies" → `share_of_associates`
- "Non-controlling interests" → `nci_income`

**CONTINUING/DISCONTINUED OPERATIONS**:
- "Profit from continuing operations" / "Profit after tax from core operations" / "Profit after taxation from refinery operations" → `net_profit_continuing` (NOT profit_after_tax_core)
- "Profit/Loss from discontinued operations" → `net_profit_discontinued`
- The TOTAL after both → `net_profit`

**REVENUE FIELDS** (each canonical must appear only once):
- `revenue_net` - Only the FINAL net revenue line after all deductions. If multiple lines build up to net revenue, only the final subtotal gets `revenue_net`.
- `revenue_gross` - Gross revenue/sales before deductions
- `revenue_deductions` - Sales tax, excise duty, rebates, commissions, tariff adjustments, freight margins (these reduce gross to net)

Example: "Gross sales" (A) minus "Sales tax" (B) minus "Rebates" (C) = "Net sales" (D=A+B+C)
→ A: revenue_gross, B: revenue_deductions, C: revenue_deductions, D: revenue_net

**TAXATION FIELDS** (use these canonical names):
- Total taxation line → `taxation` (NOT total_taxation, income_tax, tax_expense)
- Current tax / Current year → `taxation_current` (NOT current_tax, current_tax_year)
- Deferred tax → `taxation_deferred` (NOT deferred_tax, deferred_tax_year)
- Prior period tax → `taxation_prior` (NOT prior_year_tax, current_tax_prior_year)
- If only one taxation line exists, use `taxation`

## SOURCE PAGES

{page_content}
"""


def extract_pl(client: OpenAI, prompt: str) -> str:
    """Call DeepSeek to extract P&L with retry logic."""
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
        output = extract_pl(client, prompt)
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
    parser = argparse.ArgumentParser(description="Extract P&L statements with Ref column")
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
    print("STEP 3: EXTRACT P&L STATEMENTS WITH REF COLUMN")
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
                pages = sections.get(section, {}).get("PL", [])
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
