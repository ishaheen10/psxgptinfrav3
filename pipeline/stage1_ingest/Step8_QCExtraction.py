#!/usr/bin/env python3
"""
Step 8: QC Extraction Readiness.

Detects pages with quality issues BEFORE proceeding to Stage 2.
Only checks pages NOT in skip_manifest.json (skips Urdu, edge pages).
Pages flagged here need Re-OCR before classification can work properly.

Checks:
1. Repeated identical rows (LLM loop corruption)
2. Low unique line ratio (garbage output)
3. Ultra-long lines (corrupted table separators)
4. [DATA MISSING] markers (OCR failed to read region)

Input:  markdown_pages/<ticker>/<year>/<doc>/page_###.md
        artifacts/stage1/step7_skip_manifest.json
Output: artifacts/stage1/step8_qc_issues.json

Usage:
    python Step8_QCExtraction.py
    python Step8_QCExtraction.py --ticker LUCK
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import PROJECT_ROOT, MARKDOWN_ROOT, STAGE1_ARTIFACTS

OUTPUT_FILE = STAGE1_ARTIFACTS / "step8_qc_issues.json"
SKIP_MANIFEST = STAGE1_ARTIFACTS / "step7_skip_manifest.json"

# Corruption thresholds
MAX_REPEAT_RUN = 12  # Flag if identical lines repeat >= this many times
MIN_UNIQUE_RATIO = 0.25  # Flag if unique lines / total < this ratio
MIN_LINES = 10  # Ignore very short pages
MAX_LINE_LENGTH = 2000  # Flag if any line exceeds this and is mostly pipes/dashes
DATA_MISSING_MARKER = "[DATA MISSING]"  # Mistral OCR marker for unreadable regions


def check_corruption(text: str) -> dict | None:
    """
    Check a markdown page for corruption patterns.

    Returns dict with corruption type if found, None if clean.
    """
    lines = text.strip().split('\n')
    non_empty_lines = [line for line in lines if line.strip()]

    if len(non_empty_lines) < MIN_LINES:
        return None  # Too short to check

    # Check 1: Repeated identical lines
    max_run = 1
    current_run = 1
    prev_line = None

    for line in non_empty_lines:
        if line == prev_line:
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 1
        prev_line = line

    if max_run >= MAX_REPEAT_RUN:
        return {"type": "repeated_lines", "max_run": max_run}

    # Check 2: Low unique line ratio
    unique_lines = set(non_empty_lines)
    unique_ratio = len(unique_lines) / len(non_empty_lines)

    if unique_ratio < MIN_UNIQUE_RATIO:
        return {"type": "low_unique_ratio", "ratio": round(unique_ratio, 3)}

    # Check 3: Ultra-long lines (corrupted separators)
    for line in non_empty_lines:
        if len(line) > MAX_LINE_LENGTH:
            # Check if it's mostly pipes/dashes (table separator corruption)
            special_chars = sum(1 for c in line if c in '|-_=')
            if special_chars / len(line) > 0.5:
                return {"type": "corrupted_separator", "line_length": len(line)}

    # Check 4: [DATA MISSING] markers (OCR couldn't read region)
    missing_count = text.count(DATA_MISSING_MARKER)
    if missing_count > 0:
        return {"type": "data_missing", "count": missing_count}

    return None


def load_skip_paths() -> set:
    """Load paths to skip from skip_manifest.json."""
    if not SKIP_MANIFEST.exists():
        return set()

    with open(SKIP_MANIFEST) as f:
        data = json.load(f)

    return {p["relative_path"] for p in data.get("pages", [])}


def scan_for_corruption(ticker: str = None, skip_paths: set = None) -> list:
    """Scan markdown pages for corruption, excluding skip pages."""
    results = []
    skip_paths = skip_paths or set()
    skipped_count = 0

    if ticker:
        search_dirs = [MARKDOWN_ROOT / ticker]
    else:
        search_dirs = [d for d in MARKDOWN_ROOT.iterdir() if d.is_dir()]

    for ticker_dir in search_dirs:
        if not ticker_dir.is_dir():
            continue

        for md_file in ticker_dir.rglob("*.md"):
            try:
                rel_path = str(md_file.relative_to(MARKDOWN_ROOT))

                # Skip if in skip manifest
                if rel_path in skip_paths:
                    skipped_count += 1
                    continue

                content = md_file.read_text(encoding='utf-8', errors='ignore')
                corruption = check_corruption(content)

                if corruption:
                    results.append({
                        "relative_path": rel_path,
                        "corruption": corruption
                    })
            except Exception:
                continue

    return results, skipped_count


def main():
    parser = argparse.ArgumentParser(description="Detect corrupted markdown pages")
    parser.add_argument("--ticker", help="Check single ticker")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 1 STEP 8: QC EXTRACTION READINESS")
    print("=" * 70)
    print()

    # Load skip manifest
    skip_paths = load_skip_paths()
    print(f"Loaded {len(skip_paths)} pages to skip (Urdu, edges)")
    print()

    print(f"Scanning: {MARKDOWN_ROOT}")
    print(f"Checks:")
    print(f"  - Repeated lines: >= {MAX_REPEAT_RUN} consecutive")
    print(f"  - Low unique ratio: < {MIN_UNIQUE_RATIO}")
    print(f"  - Corrupted separators: > {MAX_LINE_LENGTH} chars")
    print(f"  - Data missing: any '{DATA_MISSING_MARKER}' marker")
    print()

    corrupted, skipped_count = scan_for_corruption(args.ticker, skip_paths)

    if corrupted:
        print(f"FOUND: {len(corrupted)} pages with quality issues")
        print()

        # Group by corruption type
        by_type = defaultdict(list)
        for p in corrupted:
            by_type[p["corruption"]["type"]].append(p)

        for ctype, pages in by_type.items():
            print(f"  {ctype}: {len(pages)} pages")

        # Group by ticker
        by_ticker = defaultdict(int)
        for p in corrupted:
            ticker = p["relative_path"].split("/")[0]
            by_ticker[ticker] += 1

        print()
        print("Top affected tickers:")
        for t, count in sorted(by_ticker.items(), key=lambda x: -x[1])[:10]:
            print(f"  {t}: {count} pages")

        # Write output
        manifest = {
            "generated_at": datetime.now().isoformat(),
            "total_issues": len(corrupted),
            "by_type": {k: len(v) for k, v in by_type.items()},
            "pages": corrupted
        }

        with open(args.output, 'w') as f:
            json.dump(manifest, f, indent=2)

        print()
        print(f"Output: {args.output}")
        print()
        print("ACTION: python -m pipeline.utilities.ReOCR --manifest artifacts/stage1/step8_qc_issues.json")

    else:
        print("PASS: No quality issues detected")

        manifest = {
            "generated_at": datetime.now().isoformat(),
            "total_issues": 0,
            "by_type": {},
            "pages": []
        }

        with open(args.output, 'w') as f:
            json.dump(manifest, f, indent=2)


if __name__ == "__main__":
    main()
