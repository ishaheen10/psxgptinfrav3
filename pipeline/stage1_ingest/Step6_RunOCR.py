#!/usr/bin/env python3
"""
Step 5: Run Mistral OCR on PDF pages.

Pulls single-page PDFs from R2 (via public URL), runs Mistral OCR, and stores
markdown output locally.

Input:  pdf_pages/<ticker>/<year>/<doc>/page_###.pdf (via R2 public URL)
Output: markdown_pages/<ticker>/<year>/<doc>/page_###.md

Cost: ~$0.002/page ($370 for 185,000 pages)

Usage:
    python -m pipeline.stage1_ingest.Step6_RunOCR
    python -m pipeline.stage1_ingest.Step6_RunOCR --manifest artifacts/stage3/mistral_reocr_manifest.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared import Checkpoint
from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT

# Configuration
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET_NAME") or "psx"
PDF_PUBLIC_BASE_URL = os.getenv("PDF_PUBLIC_BASE_URL") or "https://source.psxgpt.com"
PDF_PUBLIC_INCLUDE_BUCKET = os.getenv("PDF_PUBLIC_INCLUDE_BUCKET", "false").lower() in {"1", "true", "yes"}

PDF_ROOT = PROJECT_ROOT / "pdf_pages"
PDF_PREFIX = "PDF_PAGES"
OUTPUT_ROOT = MARKDOWN_ROOT
MISTRAL_MODEL = "mistral-ocr-latest"
INCLUDE_IMAGES = True
SKIP_EXISTING = os.getenv("STEP5_SKIP_EXISTING", "true").lower() in {"1", "true", "yes"}
MAX_RETRIES = 3
RETRY_WAIT = 5.0


def setup_mistral():
    """Setup Mistral client."""
    try:
        from mistralai import Mistral
    except ImportError:
        print("Installing mistralai package...")
        os.system("pip install mistralai")
        from mistralai import Mistral

    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        raise SystemExit("Missing MISTRAL_API_KEY in environment.")
    return Mistral(api_key=api_key)


def iter_local_pages(root: Path):
    """Iterate over local PDF pages."""
    if not root.exists():
        raise SystemExit(f"pdf_pages directory not found: {root}")
    for path in sorted(root.rglob("*.pdf")):
        yield path.relative_to(root)


def build_public_url(key: str) -> Optional[str]:
    """Build public URL for PDF."""
    if not PDF_PUBLIC_BASE_URL:
        return None
    path = key
    if PDF_PUBLIC_INCLUDE_BUCKET and R2_BUCKET_NAME:
        path = f"{R2_BUCKET_NAME.strip('/')}/{path}"
    return f"{PDF_PUBLIC_BASE_URL.rstrip('/')}/{path}"


def run_mistral(client, url: str) -> List[str]:
    """Run Mistral OCR on a URL."""
    payload = {
        "model": MISTRAL_MODEL,
        "document": {"type": "document_url", "document_url": url},
        "include_image_base64": INCLUDE_IMAGES,
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            response = client.ocr.process(**payload)
            break
        except Exception:
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(RETRY_WAIT)

    pages = getattr(response, "pages", []) or []
    blocks = []

    for idx, page in enumerate(pages, start=1):
        header = f"<!-- Page {idx} -->"
        text = (getattr(page, "markdown", "") or "").strip()
        blocks.append(f"{header}\n\n{text}\n")

    if not blocks:
        blocks.append("<!-- Page 1 -->\n\n")

    return blocks


def load_manifest(manifest_path: Path) -> List[Path]:
    """Load pages from a manifest file."""
    with open(manifest_path) as f:
        data = json.load(f)

    pages = []
    for item in data.get("pages", []):
        rel_path = item.get("relative_path", "")
        # Convert .md path to .pdf path
        pdf_path = rel_path.replace(".md", ".pdf")
        pages.append(Path(pdf_path))

    return pages


def main():
    parser = argparse.ArgumentParser(description="Run Mistral OCR on PDF pages")
    parser.add_argument("--manifest", type=Path, help="JSON manifest with pages to process (re-OCR mode)")
    args = parser.parse_args()

    print("=" * 70)
    print("STAGE 1 STEP 5: RUN MISTRAL OCR")
    print("=" * 70)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    mistral_client = setup_mistral()

    # Load pages from manifest or iterate all local pages
    if args.manifest:
        pages = load_manifest(args.manifest)
        print(f"Loaded {len(pages)} pages from manifest: {args.manifest}")
        skip_existing = False  # Always re-OCR manifest pages
    else:
        pages = list(iter_local_pages(PDF_ROOT))
        skip_existing = SKIP_EXISTING

    if not pages:
        print("No PDF pages to process.")
        return

    checkpoint = Checkpoint.load("Step5_RunOCR", stage=1)
    checkpoint.set_total(len(pages))

    processed = skipped = failed = 0
    start = time.time()

    for idx, relative in enumerate(pages, start=1):
        item_id = relative.as_posix()
        markdown_path = (OUTPUT_ROOT / relative).with_suffix(".md")

        # Skip existing only when not using manifest
        if skip_existing and markdown_path.exists():
            checkpoint.skip(item_id)
            skipped += 1
            continue

        # Skip checkpoint only when not using manifest
        if not args.manifest and item_id in checkpoint.completed_items:
            checkpoint.skip(item_id)
            skipped += 1
            continue

        checkpoint.mark_in_progress(item_id)
        print(f"[{idx}/{len(pages)}] {relative.as_posix()}")

        markdown_path.parent.mkdir(parents=True, exist_ok=True)

        key = f"{PDF_PREFIX.strip('/')}/{relative.as_posix()}"
        url = build_public_url(key)

        if not url:
            print(f"Unable to build URL for {key}")
            checkpoint.fail(item_id, "No URL")
            failed += 1
            continue

        try:
            blocks = run_mistral(mistral_client, url)
        except Exception as exc:
            print(f"OCR failed for {relative}: {exc}")
            checkpoint.fail(item_id, str(exc))
            failed += 1
            continue

        markdown_path.write_text("\n\n".join(blocks), encoding="utf-8")
        checkpoint.complete(item_id)
        processed += 1
        print(f"Wrote {markdown_path}")

    checkpoint.finalize()

    duration = time.time() - start
    print()
    print("=" * 70)
    print("STEP 5 SUMMARY")
    print("=" * 70)
    print(f"Processed: {processed}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Elapsed: {duration:.1f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
