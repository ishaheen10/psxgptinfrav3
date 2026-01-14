#!/usr/bin/env python3
"""
Step 3: Repair markdown with DeepSeek (text-based).

For pages marked "Fix" - minor issues that can be repaired from the markdown text.
DeepSeek cleans up misaligned columns, small gaps, OCR artifacts.

Input:  artifacts/stage2/step2_repair_manifest.json
        markdown_pages/<path>
Output: markdown_pages/<path> (updated in place)
        artifacts/stage2/step3_repairs_fix.jsonl

Usage:
    python Step3_RepairFix.py
    python Step3_RepairFix.py --limit 100
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

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared import Checkpoint
from shared.constants import MARKDOWN_ROOT, STAGE2_ARTIFACTS

REPAIR_MANIFEST = STAGE2_ARTIFACTS / "step2_repair_manifest.json"
OUTPUT_LOG = STAGE2_ARTIFACTS / "step3_repairs_fix.jsonl"

# Configuration
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_REPAIR_MODEL", "deepseek-chat")
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
TEMPERATURE = 0.0
MAX_CHARS = 6000
MAX_RETRIES = 3
RETRY_WAIT = 5.0
MAX_WORKERS = int(os.getenv("STEP3_MAX_WORKERS", "50"))

REPAIR_PROMPT = """You are a markdown repair specialist for financial documents.

The following markdown was OCR'd from a PSX (Pakistan Stock Exchange) filing page.
It has minor issues like:
- Misaligned table columns
- Missing cell separators
- OCR artifacts (stray characters)
- Broken table formatting

Please clean up the markdown while preserving ALL numeric values exactly as they appear.
Do not change any numbers, dates, or financial figures.

Focus on:
1. Fixing table alignment (ensure | separators are correct)
2. Removing stray characters that don't belong
3. Fixing broken headers/rows
4. Preserving all content

Return ONLY the cleaned markdown, no explanations."""


def setup_client():
    """Setup DeepSeek client."""
    from openai import OpenAI

    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise SystemExit("Missing DEEPSEEK_API_KEY")

    return OpenAI(api_key=api_key, base_url=DEEPSEEK_API_BASE)


def repair_markdown(client, text: str) -> str:
    """Repair markdown with DeepSeek."""
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS] + "\n\n[TRUNCATED]"

    messages = [
        {"role": "system", "content": REPAIR_PROMPT},
        {"role": "user", "content": text}
    ]

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=messages,
                temperature=TEMPERATURE
            )
            return response.choices[0].message.content
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_WAIT)
            else:
                raise e

    return text


def process_page(client, rel_path: str, lock: threading.Lock, results: list) -> tuple:
    """Process a single page."""
    md_path = MARKDOWN_ROOT / rel_path

    try:
        original = md_path.read_text(encoding="utf-8", errors="ignore")
        repaired = repair_markdown(client, original)

        # Write repaired content
        md_path.write_text(repaired, encoding="utf-8")

        result = {
            "relative_path": rel_path,
            "status": "success",
            "original_len": len(original),
            "repaired_len": len(repaired),
            "repaired_at": datetime.now().isoformat()
        }

        with lock:
            results.append(result)

        return ("success", rel_path)

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
    parser = argparse.ArgumentParser(description="Repair markdown with DeepSeek")
    parser.add_argument("--limit", type=int, help="Limit pages to process")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    OUTPUT_LOG.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 3: REPAIR FIX (DeepSeek)")
    print("=" * 70)
    print()

    # Load repair manifest
    if not REPAIR_MANIFEST.exists():
        print(f"Repair manifest not found: {REPAIR_MANIFEST}")
        print("Run Step2_BuildRepairManifest.py first")
        return

    with open(REPAIR_MANIFEST) as f:
        manifest = json.load(f)

    pages_fix = manifest.get("pages_fix", [])
    print(f"Pages needing Fix repair: {len(pages_fix)}")

    if not pages_fix:
        print("No pages need Fix repair")
        return

    # Get paths
    paths = [p["relative_path"] for p in pages_fix]

    if args.limit:
        paths = paths[:args.limit]
        print(f"Limited to: {len(paths)}")

    # Setup
    client = setup_client()
    checkpoint = Checkpoint.load("Step3_RepairFix", stage=2)

    # Filter already done
    paths_to_process = [p for p in paths if p not in checkpoint.completed_items]
    print(f"Already repaired: {len(paths) - len(paths_to_process)}")
    print(f"To process: {len(paths_to_process)}")

    if not paths_to_process:
        print("\nAll pages already repaired")
        return

    checkpoint.set_total(len(paths_to_process))

    # Process
    results = []
    lock = threading.Lock()
    successful = failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_page, client, p, lock, results): p
            for p in paths_to_process
        }

        with tqdm(total=len(futures), desc="Repairing") as pbar:
            for future in as_completed(futures):
                status, path = future.result()
                if status == "success":
                    successful += 1
                    checkpoint.complete(path)
                else:
                    failed += 1
                    checkpoint.fail(path, "Repair failed")
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
