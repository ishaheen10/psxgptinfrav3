#!/usr/bin/env python3
"""
Generate BM25-oriented search_text for all pages in the Stage 1 classification manifest.

This uses the existing classification manifest so skipped Urdu pages, first/last pages,
and corrupted pages are excluded automatically.

Input:
  - artifacts/stage1/step9_classification_manifest.json
  - markdown_pages/<ticker>/<year>/<filing>/page_###.md

Output:
  - markdown_search_text/<ticker>/<year>/<filing>/page_###.md
  - artifacts/stage2/step1_search_text.jsonl
  - artifacts/stage2/step1_search_text_prompt.txt

Usage:
  python pipeline/stage2_review/Step1_GenerateSearchText.py
  python pipeline/stage2_review/Step1_GenerateSearchText.py --workers 12
  python pipeline/stage2_review/Step1_GenerateSearchText.py --ticker UBL
  python pipeline/stage2_review/Step1_GenerateSearchText.py --limit 100
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.checkpoint import Checkpoint  # noqa: E402
from shared.constants import MARKDOWN_ROOT, PROJECT_ROOT, STAGE1_ARTIFACTS, STAGE2_ARTIFACTS  # noqa: E402

INPUT_MANIFEST = STAGE1_ARTIFACTS / "step9_classification_manifest.json"
OUTPUT_ROOT = PROJECT_ROOT / "markdown_search_text"
OUTPUT_JSONL = STAGE2_ARTIFACTS / "step1_search_text.jsonl"
OUTPUT_PROMPT = STAGE2_ARTIFACTS / "step1_search_text_prompt.txt"

DEEPSEEK_MODEL = os.getenv("DEEPSEEK_REVIEW_MODEL", "deepseek-chat")
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
MAX_WORKERS = int(os.getenv("SEARCH_TEXT_WORKERS", "10"))
TEMPERATURE = float(os.getenv("SEARCH_TEXT_TEMPERATURE", "0.1"))
MAX_CHARS = int(os.getenv("SEARCH_TEXT_MAX_CHARS", "3500"))
MAX_RETRIES = int(os.getenv("SEARCH_TEXT_MAX_RETRIES", "3"))
RETRY_WAIT = float(os.getenv("SEARCH_TEXT_RETRY_WAIT", "5.0"))
REQUEST_TIMEOUT = float(os.getenv("SEARCH_TEXT_REQUEST_TIMEOUT", "120.0"))

# Pre-flight content thresholds (must match Step7_BuildSkipManifest.py)
THIN_CONTENT_THRESHOLD = 150
EMPTY_TABLE_THRESHOLD = 20

_IMG_RE = re.compile(r'!\[.*?\]\(.*?\)', re.DOTALL)
_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)
_HEADING_EXTRACT_RE = re.compile(r'^#{1,6}\s+(.+)$', re.MULTILINE)

# Normalized forms of GOOD EXAMPLES used in the prompt.
# If the model returns one of these verbatim it copied the example instead of reading the page.
_PROMPT_EXAMPLE_COPIES = {
    "directors' report, deposit growth drivers, low-cost deposits, fee income, home remittances, digital banking, weighted average cost of funds 15.2%",
    "working capital commentary, receivables buildup, circular debt, debt reduction, cash discipline, lng receivables pkr 310 billion, borrowings pkr 356 billion",
    "capacity expansion commentary, commissioned clinker line, production capacity 1.742 mtpa, captive power plant 15.7 mw, capacity utilization 91% to 78%",
    "risk management, fertilizer demand volatility, gas supply disruption, export pressure, inventory risk, urea sales decline 12%",
}

PROMPT = """You rewrite PSX filing pages into a single retrieval field for BM25 search.

Your goal is not to write a readable summary. Your goal is to produce a compact retrieval field that helps BM25 rank the right page and helps Scout quickly judge whether the page is useful.

Important:
- Ticker, filing type, and filing period are already stored as metadata and do not need to be repeated.
- Section or page-role cues can be included if they help retrieval.

`search_text` should combine 4 kinds of signal when they are truly present on the page:
1. structural signal: what kind of page or section this is
2. lexical signal: the exact business or finance terms on the page
3. conceptual signal: the main claim, driver, or theme
4. numerical signal: the most important labeled figures or comparisons

TARGET FORMAT
- Output only the `search_text` string, nothing else.
- Use compact phrase fragments, not full narrative sentences.
- Use commas to separate major chunks.
- Preserve normal casing and percent symbols where useful.
- Usually keep it between 18 and 40 words.
- Prefer 4 to 8 comma-separated fragments total.

RECOMMENDED SHAPE
- optional section cue,
- 2 to 3 core concept/claim phrases,
- 1 to 3 labeled metrics or comparisons,
- 1 to 3 retrieval phrases or synonyms

INCLUDE
1. Optional structural cue if it helps retrieval:
   - examples: `Directors' Report`, `Chairman's Review`, `CEO Review`, `Growth at a Glance`, `Risk Management`, `Statement Note`
2. Two or three lexical or conceptual phrases describing what the page is really about:
   - examples: `deposit growth drivers`, `working capital pressure`, `funding mix`, `capacity expansion commentary`, `export demand slowdown`
3. One to three labeled numerical facts or comparisons:
   - examples:
     - `Gross margin 34% to 28%`
     - `Capacity utilization 91% to 78%`
     - `Domestic branches 1,394`
     - `Weighted average cost of funds 15.2%`
4. One to three retrieval phrases or synonyms an analyst might search:
   - examples:
     - `cost of funds`
     - `deposit mix`
     - `receivables buildup`
     - `circular debt`
     - `commissioned clinker line`

DESIGN PRINCIPLES
- Prefer rare, page-specific, query-worthy terms.
- Prefer company-specific facts over macro context.
- Prefer causal drivers over generic labels.
- Prefer labeled numbers over raw numbers.
- Prefer comparative movement over static descriptions when available.
- Keep only the most retrieval-useful figures, not full table dumps.

SPARSE PAGE RULE
Some pages have very little content (a heading, a section label, a single image). When that happens:
- Return only what is explicitly on the page — never expand or infer missing details.
- A page with only a heading → return just the heading text (e.g. "Directors Report").
- A page with no facts or figures → a 2 to 4 word field is correct and expected.
- Never use a GOOD EXAMPLE below as a template when the page lacks that content.
- If the page is effectively empty, return an empty string.

DO NOT INCLUDE
- ticker, filing type, filing period, or filing year already available in metadata
- generic framing like:
  - `This page outlines`
  - `This page details`
  - `This page highlights`
- generic filler like:
  - `performance`
  - `strategy`
  - `outlook`
  - `growth`
  - `review`
  unless attached to a specific concept
- long enumerations of all table rows
- invented concepts or inferred claims not supported by the page

GOOD EXAMPLES (format reference only — never copy these when the page lacks this content)
- `Directors' Report, deposit growth drivers, low-cost deposits, fee income, home remittances, digital banking, weighted average cost of funds 15.2%`
- `Working capital commentary, receivables buildup, circular debt, debt reduction, cash discipline, LNG receivables PKR 310 billion, borrowings PKR 356 billion`
- `Capacity expansion commentary, commissioned clinker line, production capacity 1.742 MTPA, captive power plant 15.7 MW, capacity utilization 91% to 78%`
- `Risk Management, fertilizer demand volatility, gas supply disruption, export pressure, inventory risk, urea sales decline 12%`

BAD EXAMPLES
- `UBL 2024 annual report, this page highlights strategy and performance`
- `Directors' Report, Annual Report, company review, business update`
- `Value added, distributed to employees, government, depositors, shareholders, retained profits, borrowing from financial institutions, interest paid`
- `1,394, 1,450, 5.74, 7.65, 13.2`

QUALITY CHECK
Before answering, verify:
- Did I include structural + lexical + conceptual + numerical signal where available?
- Did I avoid repeating metadata already stored elsewhere?
- Did I keep only the top retrieval-worthy facts?
- Would this field help distinguish this page from nearby pages in the same filing?

Return only the final `search_text` string.
"""

BANNED_PREFIXES = (
    "macroeconomic review ",
    "external environment factors ",
    "this page ",
    "page outlines ",
    "page details ",
    "the company ",
)


def _compute_content_chars(text: str) -> int:
    text = _COMMENT_RE.sub('', text)
    text = _IMG_RE.sub('', text)
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    return len(text.strip())


def preflight_classify(text: str) -> tuple[str, str] | tuple[None, None]:
    """Return (skip_reason, canonical_output) if the page should bypass the LLM, else (None, None).

    Called before every LLM request so near-empty pages never reach the model.
    The canonical_output is written directly as the search_text.
    """
    body = _COMMENT_RE.sub('', text).strip()

    if len(body) < 20:
        return "blank", ""

    if len(re.sub(r'[|\s]', '', body)) < EMPTY_TABLE_THRESHOLD:
        return "empty_table", ""

    images = len(_IMG_RE.findall(body))
    content_chars = _compute_content_chars(text)

    if images >= 1 and content_chars < 40:
        return "image_only", ""

    if images == 0 and content_chars < 40:
        headings = [h.strip() for h in _HEADING_EXTRACT_RE.findall(body) if h.strip() and not h.strip().isdigit()]
        return "heading_only", " ".join(headings).lower()

    return None, None


def setup_client():
    from openai import OpenAI

    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise SystemExit("Missing DEEPSEEK_API_KEY in environment")

    return OpenAI(api_key=api_key, base_url=DEEPSEEK_API_BASE, timeout=REQUEST_TIMEOUT)


def truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n...[TRUNCATED to {limit} chars]..."


def load_manifest(manifest_path: Path, ticker_filter: str | None = None) -> list[str]:
    if not manifest_path.exists():
        raise SystemExit(f"Manifest not found: {manifest_path}\nRun Stage 1 Step 9 first.")

    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    pages: list[str] = []
    filings = manifest.get("filings", {})
    for filing_key, filing_data in filings.items():
        if ticker_filter:
            ticker = filing_key.split("/")[0]
            if ticker.upper() != ticker_filter.upper():
                continue
        for page_info in filing_data.get("pages", []):
            pages.append(page_info["path"])
    return pages


def output_path_for(relative_path: str, output_root: Path) -> Path:
    return output_root / relative_path


def load_existing_paths(output_root: Path) -> set[str]:
    if not output_root.exists():
        return set()

    paths: set[str] = set()
    for file_path in output_root.rglob("*.md"):
        paths.add(str(file_path.relative_to(output_root)))
    return paths


def normalize_search_text(value) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if text.startswith("```") and text.endswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    if ":" in text:
        prefix, remainder = text.split(":", 1)
        if prefix.strip().lower().replace("_", "").replace(" ", "") in {"searchtext"}:
            text = remainder.strip()
    text = text.strip().strip('"').strip("'").strip()
    text = " ".join(text.split())
    for prefix in BANNED_PREFIXES:
        if text.lower().startswith(prefix):
            text = text[len(prefix):].strip()
    # Post-flight: discard verbatim prompt-example copies
    if text.lower() in _PROMPT_EXAMPLE_COPIES:
        return ""
    return text


def generate_search_text(client, page_text: str, content_chars: int | None = None) -> str:
    thin_note = ""
    if content_chars is not None and content_chars < THIN_CONTENT_THRESHOLD:
        thin_note = (
            f"\n\n[Page has ~{content_chars} chars of content. "
            "Return only what is explicitly present — a short field of 2–5 words is correct.]"
        )
    messages = [
        {"role": "system", "content": "You write retrieval-optimized PSX filing page search_text. Return only the final search_text string."},
        {"role": "user", "content": f"{PROMPT}\n\nPage content:\n{truncate_text(page_text, MAX_CHARS)}{thin_note}"},
    ]

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=messages,
                temperature=TEMPERATURE,
            )
            content = response.choices[0].message.content
            return normalize_search_text(content)
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_WAIT * (attempt + 1))
    return ""


def process_one(
    client,
    relative_path: str,
    output_root: Path,
    output_jsonl: Path,
    write_lock: threading.Lock,
) -> tuple[str, str]:
    md_path = MARKDOWN_ROOT / relative_path
    if not md_path.exists():
        raise FileNotFoundError(f"Missing markdown page: {md_path}")

    page_text = md_path.read_text(encoding="utf-8", errors="ignore")

    preflight_reason, canonical = preflight_classify(page_text)
    if preflight_reason is not None:
        search_text = canonical
    else:
        content_chars = _compute_content_chars(page_text)
        search_text = generate_search_text(client, page_text, content_chars)

    output_path = output_path_for(relative_path, output_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(search_text + "\n", encoding="utf-8")

    record = {
        "relative_path": relative_path,
        "search_text": search_text,
        "generated_at": datetime.now().isoformat(),
        **({"preflight_skipped": preflight_reason} if preflight_reason else {}),
    }
    with write_lock:
        with output_jsonl.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    return ("success", relative_path)


def main():
    parser = argparse.ArgumentParser(description="Generate BM25 search_text from Stage 1 manifest")
    parser.add_argument("--ticker", help="Process single ticker only")
    parser.add_argument("--limit", type=int, help="Limit pages to process")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Concurrent workers")
    parser.add_argument("--manifest", type=Path, default=INPUT_MANIFEST)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--output-jsonl", type=Path, default=OUTPUT_JSONL)
    parser.add_argument("--output-prompt", type=Path, default=OUTPUT_PROMPT)
    args = parser.parse_args()

    args.output_root.mkdir(parents=True, exist_ok=True)
    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    args.output_prompt.write_text(PROMPT.strip() + "\n", encoding="utf-8")

    print("=" * 70)
    print("STAGE 2 STEP 1X: GENERATE SEARCH TEXT")
    print("=" * 70)
    print()
    print(f"Model: {DEEPSEEK_MODEL}")
    print(f"Workers: {args.workers}")
    print(f"Manifest: {args.manifest}")
    print(f"Output root: {args.output_root}")
    print(f"Output jsonl: {args.output_jsonl}")
    print()

    client = setup_client()

    all_pages = load_manifest(args.manifest, args.ticker)
    print(f"Pages in manifest: {len(all_pages)}")

    existing_paths = load_existing_paths(args.output_root)
    print(f"Already generated: {len(existing_paths)}")

    pending_pages = [p for p in all_pages if p not in existing_paths]
    print(f"Pending: {len(pending_pages)}")

    if args.limit:
        pending_pages = pending_pages[:args.limit]
        print(f"Limited to: {len(pending_pages)}")

    if not pending_pages:
        print("\nAll manifest pages already have search_text. Nothing to do.")
        return

    checkpoint = Checkpoint.load("Step1_GenerateSearchText", stage=2)
    checkpoint.set_total(len(pending_pages))

    write_lock = threading.Lock()
    success = 0
    failed = 0

    print()
    print(f"Generating search_text for {len(pending_pages)} pages...")
    print("-" * 70)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_one, client, rel_path, args.output_root, args.output_jsonl, write_lock): rel_path
            for rel_path in pending_pages
        }
        for index, future in enumerate(as_completed(futures), start=1):
            rel_path = futures[future]
            checkpoint.mark_in_progress(rel_path)
            try:
                status, _ = future.result()
                if status == "success":
                    success += 1
                    checkpoint.complete(rel_path)
                else:
                    failed += 1
                    checkpoint.fail(rel_path, "unknown status")
            except Exception as e:
                failed += 1
                checkpoint.fail(rel_path, str(e))

            if index % 100 == 0 or index == len(pending_pages):
                print(f"{index:,}/{len(pending_pages):,} done | success={success:,} failed={failed:,}")

    checkpoint.finalize()

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Success: {success:,}")
    print(f"Failed:  {failed:,}")
    print(f"Prompt:  {args.output_prompt}")
    print(f"Output:  {args.output_root}")
    print(f"JSONL:   {args.output_jsonl}")


if __name__ == "__main__":
    main()
