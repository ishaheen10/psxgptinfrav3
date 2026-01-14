#!/usr/bin/env python3
"""
Step 4: Re-OCR pages with Gemini (vision-based).

For pages marked "ReOCR" - severe corruption that needs fresh OCR from PDF.
Gemini Flash extracts text directly from the PDF image.

Input:  artifacts/stage2/step2_repair_manifest.json
        pdf_pages/<path>
Output: markdown_pages/<path> (updated in place)
        artifacts/stage2/step4_repairs_reocr.jsonl

Usage:
    python Step4_RepairReOCR.py
    python Step4_RepairReOCR.py --limit 100
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai
from tqdm import tqdm

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared import Checkpoint
from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE2_ARTIFACTS

REPAIR_MANIFEST = STAGE2_ARTIFACTS / "step2_repair_manifest.json"
OUTPUT_LOG = STAGE2_ARTIFACTS / "step4_repairs_reocr.jsonl"
PDF_ROOT = PROJECT_ROOT / "pdf_pages"

# Configuration
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
MAX_RETRIES = 3
RETRY_WAIT = 5.0
MAX_WORKERS = int(os.getenv("STEP4_MAX_WORKERS", "50"))

# Configure Gemini client
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None

OCR_PROMPT = """Extract all text from this PDF page as clean markdown.

For tables:
- Use proper markdown table syntax with | separators
- Align columns properly
- Include ALL numeric values exactly as shown
- Preserve table structure

For text:
- Preserve headings with appropriate # levels
- Keep all numbers, dates, and amounts accurate
- Maintain paragraph structure

Return ONLY the markdown content, no explanations or commentary."""


def setup_gemini():
    """Validate Gemini client is configured."""
    if not client:
        raise SystemExit("Missing GEMINI_API_KEY")
    return client


def get_pdf_path(markdown_rel_path: str) -> Path:
    """Convert markdown path to PDF path."""
    pdf_rel = Path(markdown_rel_path).with_suffix(".pdf")
    return PDF_ROOT / pdf_rel


def ocr_with_gemini(gemini_client, pdf_path: Path) -> str | None:
    """Run Gemini OCR on a PDF page."""
    if not pdf_path.exists():
        return None

    pdf_bytes = pdf_path.read_bytes()
    parts = [
        genai.types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        OCR_PROMPT
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


def process_page(gemini_client, rel_path: str, lock: threading.Lock, results: list) -> tuple:
    """Process a single page."""
    pdf_path = get_pdf_path(rel_path)
    md_path = MARKDOWN_ROOT / rel_path

    try:
        new_content = ocr_with_gemini(gemini_client, pdf_path)

        if new_content:
            md_path.parent.mkdir(parents=True, exist_ok=True)
            md_path.write_text(f"<!-- Page 1 -->\n\n{new_content}\n", encoding="utf-8")

            result = {
                "relative_path": rel_path,
                "status": "success",
                "content_len": len(new_content),
                "repaired_at": datetime.now().isoformat()
            }
        else:
            result = {
                "relative_path": rel_path,
                "status": "failed",
                "error": "No content returned",
                "repaired_at": datetime.now().isoformat()
            }

        with lock:
            results.append(result)

        return ("success" if new_content else "failed", rel_path)

    except Exception as e:
        result = {
            "relative_path": rel_path,
            "status": "failed",
            "error": str(e),
            "repaired_at": datetime.now().isoformat()
        }
        with lock:
            results.append(result)
        return ("failed", rel_path)


def main():
    parser = argparse.ArgumentParser(description="Re-OCR pages with Gemini")
    parser.add_argument("--limit", type=int, help="Limit pages to process")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    OUTPUT_LOG.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 4: REPAIR ReOCR (Gemini)")
    print("=" * 70)
    print()

    # Load repair manifest
    if not REPAIR_MANIFEST.exists():
        print(f"Repair manifest not found: {REPAIR_MANIFEST}")
        print("Run Step2_BuildRepairManifest.py first")
        return

    with open(REPAIR_MANIFEST) as f:
        manifest = json.load(f)

    pages_reocr = manifest.get("pages_reocr", [])
    print(f"Pages needing ReOCR: {len(pages_reocr)}")

    if not pages_reocr:
        print("No pages need ReOCR")
        return

    # Get paths
    paths = [p["relative_path"] for p in pages_reocr]

    if args.limit:
        paths = paths[:args.limit]
        print(f"Limited to: {len(paths)}")

    # Setup
    gemini_client = setup_gemini()
    checkpoint = Checkpoint.load("Step4_RepairReOCR", stage=2)

    # Filter already done
    paths_to_process = [p for p in paths if p not in checkpoint.completed_items]
    print(f"Already re-OCR'd: {len(paths) - len(paths_to_process)}")
    print(f"To process: {len(paths_to_process)}")

    if not paths_to_process:
        print("\nAll pages already re-OCR'd")
        return

    checkpoint.set_total(len(paths_to_process))

    # Process
    results = []
    lock = threading.Lock()
    successful = failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_page, gemini_client, p, lock, results): p
            for p in paths_to_process
        }

        with tqdm(total=len(futures), desc="Re-OCR") as pbar:
            for future in as_completed(futures):
                status, path = future.result()
                if status == "success":
                    successful += 1
                    checkpoint.complete(path)
                else:
                    failed += 1
                    checkpoint.fail(path, "ReOCR failed")
                pbar.update(1)

    checkpoint.finalize()

    # Write results
    with open(OUTPUT_LOG, 'a') as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Log: {OUTPUT_LOG}")


if __name__ == "__main__":
    main()
