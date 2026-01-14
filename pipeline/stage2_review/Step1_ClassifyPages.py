#!/usr/bin/env python3
"""
Step 1: Classify markdown pages with DeepSeek (combined call).

Single LLM call per page that returns:
1. Summary (<=80 words)
2. Section tags with confidence scores
3. Extraction quality score (OK/Fix/ReOCR)

This combines what was previously 3 separate DeepSeek calls (summarize, classify,
repair triage) into one efficient call per page.

Input:  artifacts/stage1/step9_classification_manifest.json
        markdown_pages/<ticker>/<year>/<doc>/page_###.md
Output: artifacts/stage2/step1_classification.jsonl

Usage:
    python Step1_ClassifyPages.py
    python Step1_ClassifyPages.py --ticker LUCK
    python Step1_ClassifyPages.py --limit 1000
    python Step1_ClassifyPages.py --workers 100
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
from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE1_ARTIFACTS, STAGE2_ARTIFACTS

# Input/Output paths
INPUT_MANIFEST = STAGE1_ARTIFACTS / "step9_classification_manifest.json"
OUTPUT_PATH = STAGE2_ARTIFACTS / "step1_classification.jsonl"

# DeepSeek configuration
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_REVIEW_MODEL", "deepseek-chat")
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
TEMPERATURE = float(os.getenv("STEP1_TEMPERATURE", "0.1"))
MAX_CHARS = int(os.getenv("STEP1_MAX_CHARS", "4000"))
MAX_RETRIES = int(os.getenv("STEP1_MAX_RETRIES", "3"))
RETRY_WAIT = float(os.getenv("STEP1_RETRY_WAIT", "5.0"))
MAX_WORKERS = int(os.getenv("STEP1_MAX_WORKERS", "200"))

# Valid section tags
ALLOWED_TAGS = {
    "statement",       # Primary financial statements (P&L, BS, CF, Changes in Equity)
    "statement_note",  # Numbered notes breaking down statement line items
    "md&a",           # Management discussion, chairman/CEO review, strategy, risks
    "useful_note",    # Material one-off items, litigation, tax disputes
    "multi_year",     # Multi-year summaries ("Six Years at a Glance", historical analysis)
    "ceo_comp",       # CEO/board compensation and remuneration tables
}

# Combined classification prompt
CLASSIFICATION_PROMPT = """You analyze PSX (Pakistan Stock Exchange) filing pages. For each page, provide:
1. A summary (2-3 sentences, <=80 words)
2. Section tags with confidence scores
3. An extraction quality score

Respond with strict JSON:
{
  "summary": "<<=80 word summary of what this page contains>",
  "section_tags": [
    {"tag": "<tag>", "confidence": 0.0-1.0}
  ],
  "extraction_score": "OK|Fix|ReOCR"
}

## ALLOWED TAGS (only emit if truly applicable)

- **statement**: Primary financial statement pages (Profit & Loss, Balance Sheet,
  Cash Flow, Statement of Changes in Equity, Comprehensive Income). These are the
  actual statement tables with line items and numeric columns, not notes about them.
  Look for headers like "Statement of Financial Position", "Profit and Loss Account",
  "Statement of Cash Flows".

- **statement_note**: Numbered notes that break down statement line items (Note 1,
  Note 2, etc.). Tables showing detailed breakdowns of assets, liabilities, revenue,
  expenses with sub-categories. Usually titled "Notes to the Financial Statements"
  with numbered sections.

- **md&a**: Management Discussion & Analysis. Chairman/CEO/Directors' review,
  strategy discussion, risk management, segment performance analysis, outlook.
  Narrative text about company performance.

- **useful_note**: Material disclosures about one-off items: tax disputes/penalties,
  litigation outcomes, unusual gains/losses, regulatory investigations, related party
  transactions. NOT routine boilerplate or standard accounting policies.

- **multi_year**: Multi-year historical summaries showing 3+ years of data in tables.
  INDICATORS (look for these patterns):
  - Headers: "Six Years at a Glance", "Five Year Summary", "Ten Year Review",
    "Horizontal Analysis", "Vertical Analysis", "Key Financial Data", "Growth at a Glance"
  - Tables with columns for multiple years (e.g., 2019, 2020, 2021, 2022, 2023, 2024)
  EXCLUDE if page header contains: "Statement of Compliance", "Notes to the Financial
  Statements", "Independent Auditor", "Auditors' Report" - these are NOT multi_year.

- **ceo_comp**: Executive/Director compensation and remuneration tables.
  INDICATORS (look for these patterns):
  - "Compensation of Directors", "Compensation of Key Management Personnel"
  - "Remuneration of Chief Executive", "Remuneration of Directors"
  - "Managerial Remuneration", "Total Compensation Expense"
  - Tables with columns for salary, bonus, benefits, perquisites, total
  MUST have both: (1) a table with numbers, AND (2) compensation-related keywords.
  Simple mentions of "CEO" or "Director" without compensation tables do NOT qualify.

## EXTRACTION SCORE

- **OK**: Tables are clean, numbers readable, columns aligned. Ready for extraction.
- **Fix**: Minor issues (slightly misaligned columns, small OCR artifacts, minor gaps)
  that can be fixed by text cleanup without re-OCR.
- **ReOCR**: Severe corruption (unreadable tables, garbled text, image-only content,
  major missing sections). Needs fresh OCR from PDF.

## CONFIDENCE SCORING

- 0.9+: Definitive match (clear header + table with expected content)
- 0.7-0.9: Strong match (most indicators present, clear intent)
- 0.5-0.7: Tentative (some indicators but uncertain)
- <0.5: Don't emit - if confidence is this low, the tag probably doesn't apply

If no tags apply, return {"section_tags": []}.

Respond with JSON only, no explanations."""


def setup_client():
    """Setup OpenAI-compatible client for DeepSeek."""
    from openai import OpenAI

    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise SystemExit("Missing DEEPSEEK_API_KEY in environment")

    return OpenAI(api_key=api_key, base_url=DEEPSEEK_API_BASE)


def truncate_text(text: str, limit: int) -> str:
    """Truncate text to character limit with notice."""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n...[TRUNCATED to {limit} chars]..."


def normalize_section_tags(raw_tags) -> list:
    """Normalize and validate section tags from LLM response."""
    if not raw_tags:
        return []

    tags = raw_tags if isinstance(raw_tags, list) else [raw_tags]
    normalized = []

    for item in tags:
        if isinstance(item, dict):
            tag = str(item.get("tag", "")).strip().lower()
            confidence = item.get("confidence", 0.5)
        elif isinstance(item, str):
            tag = item.strip().lower()
            confidence = 0.5
        else:
            continue

        # Skip invalid tags
        if tag not in ALLOWED_TAGS:
            continue

        # Normalize confidence
        try:
            confidence = float(confidence)
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            confidence = 0.5

        normalized.append({
            "tag": tag,
            "confidence": round(confidence, 2)
        })

    return normalized


def normalize_extraction_score(score: str) -> str:
    """Normalize extraction score to OK/Fix/ReOCR."""
    if not score:
        return "OK"

    score = str(score).strip().lower()

    # Map various formats to standard values
    if score in ("ok", "clean", "good"):
        return "OK"
    elif score in ("fix", "fix_flash", "minor"):
        return "Fix"
    elif score in ("reocr", "fix_pro", "severe", "bad"):
        return "ReOCR"
    else:
        return "OK"  # Default to OK if unclear


def classify_page(client, text: str) -> dict:
    """Classify a single page with DeepSeek (combined call)."""
    messages = [
        {"role": "system", "content": "You analyze PSX filing pages and always respond with strict JSON."},
        {"role": "user", "content": f"{CLASSIFICATION_PROMPT}\n\nPage content:\n{truncate_text(text, MAX_CHARS)}"}
    ]

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=messages,
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )
            content = response.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_WAIT * (attempt + 1))  # Exponential backoff
            else:
                raise e

    return {}


def process_page(client, rel_path: str, lock: threading.Lock, results: list) -> tuple:
    """Process a single page and append result to results list."""
    md_path = MARKDOWN_ROOT / rel_path

    try:
        text = md_path.read_text(encoding="utf-8", errors="ignore")
        result = classify_page(client, text)

        # Normalize the response
        record = {
            "relative_path": rel_path,
            "summary": result.get("summary", ""),
            "section_tags": normalize_section_tags(result.get("section_tags")),
            "extraction_score": normalize_extraction_score(result.get("extraction_score")),
            "classified_at": datetime.now().isoformat()
        }

        with lock:
            results.append(record)

        return ("success", rel_path)

    except Exception as e:
        error_record = {
            "relative_path": rel_path,
            "summary": "",
            "section_tags": [],
            "extraction_score": "ReOCR",  # Mark failed pages for re-OCR
            "error": str(e),
            "classified_at": datetime.now().isoformat()
        }
        with lock:
            results.append(error_record)
        return ("failed", rel_path)


def load_existing_paths(output_path: Path) -> set:
    """Load set of already-classified page paths."""
    if not output_path.exists():
        return set()

    paths = set()
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    data = json.loads(line)
                    if "relative_path" in data:
                        paths.add(data["relative_path"])
                except json.JSONDecodeError:
                    continue
    return paths


def load_manifest(manifest_path: Path, ticker_filter: str = None) -> list:
    """Load pages from Stage 1 classification manifest."""
    if not manifest_path.exists():
        raise SystemExit(f"Manifest not found: {manifest_path}\nRun Stage 1 Step 9 first.")

    with open(manifest_path) as f:
        manifest = json.load(f)

    pages = []
    filings = manifest.get("filings", {})

    for filing_key, filing_data in filings.items():
        # Filter by ticker if specified
        if ticker_filter:
            ticker = filing_key.split("/")[0]
            if ticker.upper() != ticker_filter.upper():
                continue

        for page_info in filing_data.get("pages", []):
            pages.append(page_info["path"])

    return pages


def main():
    parser = argparse.ArgumentParser(description="Classify pages with DeepSeek (combined call)")
    parser.add_argument("--ticker", help="Process single ticker only")
    parser.add_argument("--limit", type=int, help="Limit pages to process")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Concurrent workers")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--manifest", type=Path, default=INPUT_MANIFEST)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 1: CLASSIFY PAGES (Combined)")
    print("=" * 70)
    print()
    print(f"Model: {DEEPSEEK_MODEL}")
    print(f"Workers: {args.workers}")
    print(f"Input: {args.manifest}")
    print(f"Output: {args.output}")
    print()

    # Setup DeepSeek client
    client = setup_client()

    # Load pages from manifest
    all_pages = load_manifest(args.manifest, args.ticker)
    print(f"Pages in manifest: {len(all_pages)}")

    if args.ticker:
        print(f"Filtered to ticker: {args.ticker}")

    # Load already-processed pages
    existing_paths = load_existing_paths(args.output)
    print(f"Already classified: {len(existing_paths)}")

    # Filter to pending pages
    pending_pages = [p for p in all_pages if p not in existing_paths]
    print(f"Pending: {len(pending_pages)}")

    if args.limit:
        pending_pages = pending_pages[:args.limit]
        print(f"Limited to: {len(pending_pages)}")

    if not pending_pages:
        print("\nAll pages already classified. Nothing to do.")
        return

    # Setup checkpoint
    checkpoint = Checkpoint.load("Step1_ClassifyPages", stage=2)
    checkpoint.set_total(len(pending_pages))

    # Process pages with thread pool
    results = []
    lock = threading.Lock()
    successful = 0
    failed = 0

    print()
    print(f"Classifying {len(pending_pages)} pages...")
    print("-" * 70)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_page, client, path, lock, results): path
            for path in pending_pages
        }

        with tqdm(total=len(futures), desc="Classifying", unit="pages") as pbar:
            for future in as_completed(futures):
                status, path = future.result()
                if status == "success":
                    successful += 1
                    checkpoint.complete(path)
                else:
                    failed += 1
                    checkpoint.fail(path, "Classification failed")
                pbar.update(1)
                pbar.set_postfix({"ok": successful, "fail": failed})

    checkpoint.finalize()

    # Append results to output file
    with open(args.output, "a", encoding="utf-8") as f:
        for record in results:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Output: {args.output}")

    # Tag distribution
    tag_counts = {}
    score_counts = {"OK": 0, "Fix": 0, "ReOCR": 0}

    for record in results:
        for tag_info in record.get("section_tags", []):
            tag = tag_info.get("tag", "unknown")
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        score = record.get("extraction_score", "OK")
        score_counts[score] = score_counts.get(score, 0) + 1

    if tag_counts:
        print()
        print("Tag distribution:")
        for tag, count in sorted(tag_counts.items(), key=lambda x: -x[1]):
            print(f"  {tag}: {count}")

    print()
    print("Extraction scores:")
    for score, count in score_counts.items():
        print(f"  {score}: {count}")

    # Guidance for next step
    fix_count = score_counts.get("Fix", 0)
    reocr_count = score_counts.get("ReOCR", 0)

    if fix_count > 0 or reocr_count > 0:
        print()
        print("Next: Run Step2_BuildRepairManifest.py to route repairs")


if __name__ == "__main__":
    main()
