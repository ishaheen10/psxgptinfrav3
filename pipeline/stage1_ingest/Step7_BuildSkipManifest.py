#!/usr/bin/env python3
"""
Step 7: Build the page skip manifest.

Deterministically identifies pages to skip from classification/extraction:
- Urdu-heavy pages (ASCII ratio < 0.85 on content-only, excluding pipes/whitespace)
- Empty-table pages (only markdown table pipes, no real content)
- First/last 2 pages per filing (cover pages, back matter)
- Image-only pages (page is just image tags with no text)
- Heading-only pages (section dividers with a heading but no body)
- Blank pages (near-empty, e.g. a single dot or whitespace)
- Thin pages (too little extractable content to be useful)

Input:  markdown_pages/<ticker>/<year>/<doc>/page_###.md
Output: artifacts/stage1/step7_skip_manifest.json

Usage:
    python Step7_BuildSkipManifest.py
    python Step7_BuildSkipManifest.py --ticker SYS
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

_IMG_RE = re.compile(r'!\[.*?\]\(.*?\)', re.DOTALL)
_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)
_HEADING_LINE_RE = re.compile(r'^#{1,6}\s*', re.MULTILINE)

# Pages with fewer content chars than this are skipped as thin/heading-only/blank
THIN_CONTENT_THRESHOLD = 150
# After stripping pipes+whitespace, fewer chars than this → empty_table
EMPTY_TABLE_THRESHOLD = 20

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE1_ARTIFACTS, UTILITIES_ARTIFACTS

OUTPUT_PATH = STAGE1_ARTIFACTS / "step7_skip_manifest.json"


def get_latest_reocr_failures() -> Path | None:
    """Find the most recent reocr failures file in utilities."""
    if not UTILITIES_ARTIFACTS.exists():
        return None
    failures = sorted(UTILITIES_ARTIFACTS.glob("reocr_*_failures.json"), reverse=True)
    return failures[0] if failures else None


ASCII_THRESHOLD = 0.85
PAGE_RE = re.compile(r"page_(\d+)\.md$", re.IGNORECASE)


def compute_ascii_ratio(text: str) -> float:
    """ASCII ratio on content-only (pipes and whitespace excluded).

    Urdu text embedded in markdown tables has inflated ASCII ratios because
    pipe separators and spaces dominate the character count. Stripping them
    first gives an accurate read of whether the actual content is non-Latin.
    """
    content = re.sub(r'[|\s]', '', text)
    if not content:
        return 1.0
    ascii_chars = sum(1 for ch in content if ord(ch) < 128)
    return ascii_chars / len(content)


def compute_content_chars(text: str) -> int:
    """Chars remaining after stripping page comment, image tags, heading markers, and whitespace."""
    text = _COMMENT_RE.sub('', text)
    text = _IMG_RE.sub('', text)
    text = _HEADING_LINE_RE.sub('', text)
    return len(text.strip())


def classify_content(text: str) -> str | None:
    """Return a skip reason if the page content is too thin to be useful, else None.

    Checks (in order):
      empty_table   — only markdown table pipes, no real content
      blank         — near-empty after stripping the page comment
      image_only    — page consists only of image tags
      heading_only  — section divider: heading text but no body
      thin          — not enough content to produce a reliable search_text
    """
    # Strip the page comment to judge actual content
    body = _COMMENT_RE.sub('', text).strip()

    if len(body) < 20:
        return "blank"

    # Empty table: pipes and spaces but nothing else
    non_table = re.sub(r'[|\s]', '', body)
    if len(non_table) < EMPTY_TABLE_THRESHOLD:
        return "empty_table"

    images = len(_IMG_RE.findall(body))
    content_chars = compute_content_chars(text)

    if images >= 1 and content_chars < 40:
        return "image_only"

    if images == 0 and content_chars < 40:
        return "heading_only"

    if content_chars < THIN_CONTENT_THRESHOLD:
        return "thin"

    return None


def get_doc_page_bounds(files: list, root: Path) -> dict:
    """Get min/max page numbers for each document."""
    bounds = {}
    for path in files:
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        match = PAGE_RE.search(relative.name)
        if not match:
            continue
        page_num = int(match.group(1))
        doc_key = str(relative.parent)
        if doc_key in bounds:
            low, high = bounds[doc_key]
            bounds[doc_key] = (min(low, page_num), max(high, page_num))
        else:
            bounds[doc_key] = (page_num, page_num)
    return bounds


def main():
    parser = argparse.ArgumentParser(description="Build page skip manifest")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 1 STEP 7: BUILD SKIP MANIFEST")
    print("=" * 70)
    print()

    # Find all markdown files
    if args.ticker:
        search_root = MARKDOWN_ROOT / args.ticker
    else:
        search_root = MARKDOWN_ROOT

    if not search_root.exists():
        print(f"Markdown root not found: {search_root}")
        return

    files = sorted(search_root.rglob("*.md"))
    print(f"Found {len(files)} markdown pages")

    # Get document bounds for edge detection
    doc_bounds = get_doc_page_bounds(files, MARKDOWN_ROOT)

    # Load ReOCR failures (permanently corrupted)
    stage1_skips = set()
    failures_file = get_latest_reocr_failures()
    if failures_file:
        with open(failures_file) as f:
            data = json.load(f)
            stage1_skips = set(data.get("pages", []))
        print(f"Loaded {len(stage1_skips)} corrupted pages from {failures_file.name}")

    # Process each file
    skip_pages = []
    stats = defaultdict(int)

    for path in files:
        try:
            relative = str(path.relative_to(MARKDOWN_ROOT))
        except ValueError:
            continue

        match = PAGE_RE.search(path.name)
        if not match:
            continue

        page_num = int(match.group(1))
        doc_key = str(Path(relative).parent)
        skip_reason = None

        # Check: Stage 1 corrupted
        if relative in stage1_skips:
            skip_reason = "corrupted"

        # Check: Edge pages (first 2, last 2)
        if not skip_reason and doc_key in doc_bounds:
            low, high = doc_bounds[doc_key]
            if page_num <= low + 1 or page_num >= high - 1:
                skip_reason = "edge"

        # Check: content quality (blank, empty_table, image_only, heading_only, thin)
        # Run before the Urdu check so garbage pages get a precise reason.
        if not skip_reason:
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
                skip_reason = classify_content(text)
            except Exception:
                pass

        # Check: Urdu-heavy (ASCII ratio computed on content only, pipes/whitespace excluded)
        if not skip_reason:
            try:
                if compute_ascii_ratio(text) < ASCII_THRESHOLD:
                    skip_reason = "urdu"
            except Exception:
                pass

        if skip_reason:
            skip_pages.append({
                "relative_path": relative,
                "page": page_num,
                "reason": skip_reason
            })
            stats[skip_reason] += 1

    # Write output
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "total_pages": len(files),
        "skipped_pages": len(skip_pages),
        "by_reason": dict(stats),
        "pages": skip_pages
    }

    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total pages scanned: {len(files)}")
    print(f"Pages to skip: {len(skip_pages)}")
    for reason, count in sorted(stats.items()):
        print(f"  - {reason}: {count}")
    print(f"\nOutput: {args.output}")
    print(f"\nPages remaining for classification: {len(files) - len(skip_pages)}")


if __name__ == "__main__":
    main()
