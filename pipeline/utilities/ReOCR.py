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


def ocr_with_gemini(gemini_client, pdf_path: Path) -> str | None:
    """Run Gemini OCR on a PDF page."""
    if not pdf_path.exists():
        return None

    prompt = """Extract ALL text from this PDF page as clean, accurate markdown.

## TABLES (CRITICAL)

Financial documents contain tables with numeric data. Extract them precisely:

1. **Use proper markdown table syntax:**
   ```
   | Column 1 | Column 2 | Column 3 |
   |:---|---:|---:|
   | Row text | 1,234,567 | 2,345,678 |
   ```

2. **Column alignment is CRITICAL:**
   - Identify column headers FIRST
   - Track column positions - values must stay in their correct columns
   - Don't mix up adjacent columns
   - If a cell is empty, leave it empty (don't shift values)

3. **Number formatting:**
   - Preserve EXACT numeric values as shown (including commas)
   - Don't round or modify any numbers
   - Preserve decimal places exactly

4. **Extract EVERY row** - do not skip any rows, even if they look like subtotals

## QUARTERLY REPORTS (4+ COLUMNS)

Many quarterly financial reports show BOTH quarter AND year-to-date columns:

| Item | Q3 Mar 2025 | Q3 Mar 2024 | 9M Mar 2025 | 9M Mar 2024 |
|:---|---:|---:|---:|---:|
| Revenue | 27,013,604 | 23,940,134 | 74,269,074 | 64,069,671 |

- **Count ALL data columns** - there may be 4, 5, or even 6 columns
- **Distinguish period types**: "Quarter ended" vs "Nine months ended" vs "Year ended"
- **Extract ALL columns** - do not stop at 2 columns
- **Fix garbled headers**: If OCR produces duplicate dates like "30 June 2024 | 30 June 2024",
  infer correct headers from context (likely: Q3 current | Q3 prior | 9M current | 9M prior)

## MULTIPLE STATEMENTS ON SAME PAGE

Some pages contain multiple financial statements (e.g., Balance Sheet followed by P&L,
or Consolidated followed by Unconsolidated). Extract ALL of them with clear headings:

```
# Statement of Financial Position
| ... |

# Statement of Profit or Loss
| ... |
```

## TEXT CONTENT

- Preserve all headings with appropriate markdown levels (# ## ###)
- Keep paragraph structure intact
- Preserve dates exactly as written
- Keep any bold/italic formatting

## CRITICAL RULES

1. **Accuracy over formatting** - getting numbers right is more important than pretty output
2. **No interpretation** - extract exactly what you see, don't summarize or rephrase
3. **No [DATA MISSING] markers** - if you can't read something, leave it blank or use "..."
4. **Preserve original language** - if text is in Urdu/Arabic, still extract it

Return ONLY the markdown content, no explanations or commentary."""

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


def process_page(gemini_client, rel_path: str, results_lock: Lock, results: list) -> tuple:
    """Process a single page."""
    pdf_path = get_pdf_path(rel_path)
    markdown_path = MARKDOWN_ROOT / rel_path

    try:
        new_content = ocr_with_gemini(gemini_client, pdf_path)

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

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_page, gemini_client, p, results_lock, results): p
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
