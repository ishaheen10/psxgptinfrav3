#!/usr/bin/env python3
"""
Utility: Re-OCR Pages.

Re-runs OCR on pages from a specified manifest.
Uses Gemini Flash for vision-based OCR as a fallback to Mistral.

Called after:
- Step8_QCExtraction (for pages with quality issues: corruption, DATA MISSING, etc.)

Input:  --manifest <path to manifest>
Output: Updated markdown_pages/<ticker>/<year>/<doc>/page_###.md
        artifacts/utilities/reocr_YYYY-MM-DD_results.jsonl
        artifacts/utilities/reocr_YYYY-MM-DD_failures.json

Usage:
    python -m pipeline.utilities.ReOCR --manifest artifacts/stage1/corrupted_pages.json
    python -m pipeline.utilities.ReOCR --manifest artifacts/stage1/qc_data_missing.jsonl
    python -m pipeline.utilities.ReOCR --manifest corrupted.json --ticker LUCK

    # Focus on specific statement type (provides context for year validation):
    python -m pipeline.utilities.ReOCR --manifest cf_pages.json --statement-type CF
    python -m pipeline.utilities.ReOCR --manifest bs_pages.json --statement-type BS
    python -m pipeline.utilities.ReOCR --manifest pl_pages.json --statement-type PL

The --statement-type flag:
- Tells OCR to focus on extracting that statement type accurately
- Provides filing date context to validate year headers (prevents wrong years from adjacent tables)
- Allows simplification of other complex tables on the same page (e.g., Statement of Changes in Equity)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from google import genai
from tqdm import tqdm

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared import Checkpoint
from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE1_ARTIFACTS, UTILITIES_ARTIFACTS

# Configuration
PDF_PAGES_ROOT = PROJECT_ROOT / "pdf_pages"
CORRUPTED_MANIFEST = STAGE1_ARTIFACTS / "corrupted_pages.json"
DATA_MISSING_MANIFEST = STAGE1_ARTIFACTS / "qc_data_missing.jsonl"

# Date-stamped outputs in utilities folder
DATE_STAMP = datetime.now().strftime("%Y-%m-%d")
OUTPUT_LOG = UTILITIES_ARTIFACTS / f"reocr_{DATE_STAMP}_results.jsonl"
FAILURES_LOG = UTILITIES_ARTIFACTS / f"reocr_{DATE_STAMP}_failures.json"

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
MAX_WORKERS = int(os.getenv("REOCR_MAX_WORKERS", "50"))
MAX_RETRIES = 3
RETRY_WAIT = 5.0

# Configure Gemini client
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None


def setup_gemini():
    """Validate Gemini client is configured."""
    if not client:
        raise SystemExit("Missing GEMINI_API_KEY in environment")
    return client


def get_pdf_path(markdown_rel_path: str) -> Path:
    """Convert markdown path to PDF path."""
    # markdown_pages/TICKER/YEAR/FILING/page_001.md -> pdf_pages/TICKER/YEAR/FILING/page_001.pdf
    pdf_rel = Path(markdown_rel_path).with_suffix(".pdf")
    return PDF_PAGES_ROOT / pdf_rel


def parse_filing_context(rel_path: str) -> dict:
    """Extract filing context from path like TICKER/YEAR/TICKER_Quarterly_2025-06-30/page_009.md"""
    import re
    context = {"ticker": None, "period_end": None, "filing_type": None}

    # Extract from folder name: TICKER_Quarterly_2025-06-30 or TICKER_Annual_2024
    match = re.search(r'([A-Z0-9]+)_(Quarterly|Annual)_(\d{4}(?:-\d{2}-\d{2})?)', rel_path)
    if match:
        context["ticker"] = match.group(1)
        context["filing_type"] = match.group(2).lower()
        date_str = match.group(3)
        # Parse year from date
        context["period_end"] = date_str
        context["year"] = int(date_str[:4])

    return context


STATEMENT_TYPE_NAMES = {
    "CF": "Cash Flow Statement",
    "BS": "Balance Sheet / Statement of Financial Position",
    "PL": "Profit and Loss / Income Statement"
}


def build_ocr_prompt(filing_context: dict = None, statement_type: str = None) -> str:
    """Build OCR prompt with optional filing context and statement type focus."""

    # Base prompt
    prompt_parts = ["""Extract ALL text from this PDF page as clean, accurate markdown.

## TABLES

Financial documents contain tables with numeric data. Output tables using markdown table syntax with pipe (|) separators between columns. Do NOT output space-aligned columns without pipe separators.

**Critical requirements:**
- Capture ALL columns present in the table - count them carefully before extracting
- Every row must have values aligned to the correct columns
- If a cell is empty, leave it empty - never shift values between columns
- Preserve exact numeric values as shown (including commas, decimals, parentheses)
- Extract every row including subtotals and totals

**Quarterly reports often have 4+ columns** showing both quarter and year-to-date figures:
- Current quarter, prior year quarter, current YTD, prior year YTD
- All columns must be captured - do not stop at 2 columns"""]

    # Add statement-type specific instructions
    if statement_type and statement_type in STATEMENT_TYPE_NAMES:
        stmt_name = STATEMENT_TYPE_NAMES[statement_type]
        prompt_parts.append(f"""
## FOCUS: {stmt_name.upper()}

This page should contain a **{stmt_name}**. Focus on extracting this statement accurately.

**SIDE-BY-SIDE LAYOUTS:** Some PDF pages show two statements side-by-side (left and right).
- The {stmt_name} may be on the RIGHT side of the page
- Extract ONLY the {stmt_name}, ignore the other statement entirely
- Do NOT merge or confuse data from the two statements

**If multiple statements appear on this page:**
- Extract the {stmt_name} completely and accurately
- SKIP the Statement of Changes in Equity entirely - do not extract it
- The {stmt_name} is the priority - ensure its column headers and all data rows are captured correctly

**IMPORTANT:** Read the statement title carefully - it says "Unconsolidated" or "Consolidated". Copy the EXACT title as shown.""")

    # Add filing context for year validation
    if filing_context and filing_context.get("year"):
        year = filing_context["year"]
        prior_year = year - 1
        period_end = filing_context.get("period_end", "")
        filing_type = filing_context.get("filing_type", "quarterly")

        prompt_parts.append(f"""
## FILING CONTEXT (USE FOR VALIDATION)

This page is from a **{filing_type} filing** for period ending **{period_end}**.

**Year validation:** Column headers should reference years {year} and {prior_year} (current and prior year).
- If you see column headers with different years (e.g., {year-2} instead of {year}), this is likely OCR noise from an adjacent table
- Ensure the extracted table headers show the correct years: {year} (current) and {prior_year} (comparative)
- The filing date {period_end} confirms what years should appear""")

    # Standard closing instructions
    prompt_parts.append("""
## MULTIPLE STATEMENTS

Some pages contain multiple statements (e.g., Consolidated followed by Unconsolidated).
Extract all of them with clear headings.

## TEXT CONTENT

- Preserve headings with appropriate markdown levels
- Keep paragraph structure and dates exactly as written

## RULES

1. Accuracy over formatting - correct numbers matter more than pretty output
2. No interpretation - extract exactly what you see
3. No [DATA MISSING] markers - leave blank or use "..." if unreadable
4. Preserve original language including Urdu/Arabic

Return ONLY the markdown content, no explanations.""")

    return "\n".join(prompt_parts)


def ocr_with_gemini(gemini_client, pdf_path: Path, filing_context: dict = None, statement_type: str = None) -> str | None:
    """Run Gemini OCR on a PDF page."""
    if not pdf_path.exists():
        return None

    prompt = build_ocr_prompt(filing_context, statement_type)

    pdf_bytes = pdf_path.read_bytes()
    parts = [
        genai.types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        prompt
    ]

    for attempt in range(MAX_RETRIES):
        try:
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=parts
            )
            return response.text
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_WAIT)
            else:
                raise e

    return None


def load_pages_to_reocr(manifest_path: Path = None, ticker: str = None) -> list:
    """Load list of pages to re-OCR."""
    pages = []

    # Try corrupted_pages.json first
    if manifest_path:
        if manifest_path.suffix == ".json":
            with open(manifest_path) as f:
                data = json.load(f)
                pages = [p["relative_path"] for p in data.get("pages", [])]
        elif manifest_path.suffix == ".jsonl":
            with open(manifest_path) as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        pages.append(data.get("relative_path", data.get("path", "")))
    else:
        # Load from both default manifests
        if CORRUPTED_MANIFEST.exists():
            with open(CORRUPTED_MANIFEST) as f:
                data = json.load(f)
                pages.extend([p["relative_path"] for p in data.get("pages", [])])

        if DATA_MISSING_MANIFEST.exists():
            with open(DATA_MISSING_MANIFEST) as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        pages.append(data.get("relative_path", ""))

    # Filter by ticker if specified
    if ticker:
        pages = [p for p in pages if p.startswith(f"{ticker}/")]

    return list(set(pages))  # Dedupe


def process_page(gemini_client, rel_path: str, results_lock: Lock, results: list, statement_type: str = None) -> tuple:
    """Process a single page."""
    pdf_path = get_pdf_path(rel_path)
    markdown_path = MARKDOWN_ROOT / rel_path

    # Parse filing context from path
    filing_context = parse_filing_context(rel_path)

    try:
        new_content = ocr_with_gemini(gemini_client, pdf_path, filing_context, statement_type)

        if new_content:
            # Write new markdown
            markdown_path.parent.mkdir(parents=True, exist_ok=True)
            markdown_path.write_text(f"<!-- Page 1 -->\n\n{new_content}\n", encoding="utf-8")

            result = {"path": rel_path, "status": "success"}
            with results_lock:
                results.append(result)
            return ("success", rel_path)
        else:
            result = {"path": rel_path, "status": "failed", "error": "No content returned"}
            with results_lock:
                results.append(result)
            return ("failed", rel_path)

    except Exception as e:
        result = {"path": rel_path, "status": "failed", "error": str(e)}
        with results_lock:
            results.append(result)
        return ("failed", rel_path)


def main():
    parser = argparse.ArgumentParser(description="Re-OCR corrupted pages")
    parser.add_argument("--manifest", type=Path, help="Specific manifest file")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--statement-type", choices=["CF", "BS", "PL"],
                        help="Focus OCR on specific statement type (CF=Cash Flow, BS=Balance Sheet, PL=Profit/Loss)")
    args = parser.parse_args()

    print("=" * 70)
    print("STAGE 1 STEP 8: RE-OCR CORRUPTED PAGES")
    print("=" * 70)
    print()

    pages = load_pages_to_reocr(args.manifest, args.ticker)

    if not pages:
        print("No pages to re-OCR")
        return

    print(f"Pages to re-OCR: {len(pages)}")
    print(f"Workers: {args.workers}")
    print()

    gemini_client = setup_gemini()
    checkpoint = Checkpoint.load("Step8_ReOCR", stage=1)
    checkpoint.set_total(len(pages))

    results = []
    results_lock = Lock()
    successful = failed = skipped = 0

    # Filter already completed
    pages_to_process = [p for p in pages if p not in checkpoint.completed_items]

    if not pages_to_process:
        print("All pages already processed")
        checkpoint.finalize()
        return

    statement_type = getattr(args, 'statement_type', None)
    if statement_type:
        print(f"Statement focus: {STATEMENT_TYPE_NAMES.get(statement_type, statement_type)}")
        print()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_page, gemini_client, p, results_lock, results, statement_type): p
            for p in pages_to_process
        }

        with tqdm(total=len(futures), desc="Re-OCR") as pbar:
            for future in as_completed(futures):
                status, path = future.result()
                if status == "success":
                    successful += 1
                    checkpoint.complete(path)
                else:
                    failed += 1
                    checkpoint.fail(path, "OCR failed")
                pbar.update(1)

    checkpoint.finalize()

    # Write results log
    OUTPUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_LOG, 'w') as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    # Track permanent failures
    permanent_failures = [r["path"] for r in results if r["status"] == "failed"]
    if permanent_failures:
        skip_data = {
            "generated_at": datetime.now().isoformat(),
            "pages": permanent_failures,
            "reason": "Failed re-OCR after max retries"
        }
        with open(FAILURES_LOG, 'w') as f:
            json.dump(skip_data, f, indent=2)
        print(f"\nPermanent failures written to: {FAILURES_LOG}")

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Results: {OUTPUT_LOG}")


if __name__ == "__main__":
    main()
