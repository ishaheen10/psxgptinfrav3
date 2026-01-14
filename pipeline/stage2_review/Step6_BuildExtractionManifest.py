#!/usr/bin/env python3
"""
Step 6: Build the extraction manifest for Stage 3.

Consolidates classification results into a manifest that tells Stage 3
which pages contain financial content (statements, notes, compensation, multi-year).

NOTE: We do NOT attempt to identify statement types (P&L, BS, CF) here.
That determination is made in Stage 3 extraction.

Input:  artifacts/stage2/step1_classification.jsonl
        artifacts/stage2/step3_repairs_fix.jsonl (optional)
        artifacts/stage2/step4_repairs_reocr.jsonl (optional)
Output: artifacts/stage2/step6_extraction_manifest.json

The manifest maps each filing to its tagged pages:
{
  "LUCK_Annual_2024": {
    "filing_path": "markdown_pages/LUCK/2024/LUCK_Annual_2024/",
    "page_count": 156,
    "pages": {
      "statement": [45, 46, 47, 48, 50, 51],
      "statement_note": [52, 53, 54, ...],
      "md&a": [10, 11, 12, ...],
      "multi_year": [8, 9],
      "ceo_comp": [120, 121]
    },
    "repaired_pages": [46, 48]
  }
}

Usage:
    python Step6_BuildExtractionManifest.py
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import MARKDOWN_ROOT, STAGE2_ARTIFACTS

CLASSIFICATION_PATH = STAGE2_ARTIFACTS / "step1_classification.jsonl"
REPAIRS_FIX_LOG = STAGE2_ARTIFACTS / "step3_repairs_fix.jsonl"
REPAIRS_REOCR_LOG = STAGE2_ARTIFACTS / "step4_repairs_reocr.jsonl"
OUTPUT_PATH = STAGE2_ARTIFACTS / "step6_extraction_manifest.json"

PAGE_RE = re.compile(r"page_(\d+)\.md$", re.IGNORECASE)

# Tags that matter for extraction
EXTRACTION_TAGS = {"statement", "statement_note", "md&a", "useful_note", "multi_year", "ceo_comp"}

# Minimum confidence to include a tag
MIN_CONFIDENCE = 0.5


def parse_path(rel_path: str) -> tuple:
    """Parse relative path into (filing_key, page_num)."""
    # Format: TICKER/YEAR/FILING/page_NNN.md
    parts = Path(rel_path).parts

    if len(parts) < 4:
        return None, None

    ticker = parts[0]
    year = parts[1]
    filing = parts[2]
    filename = parts[3]

    match = PAGE_RE.search(filename)
    if not match:
        return None, None

    page_num = int(match.group(1))
    filing_key = f"{ticker}_{filing}"

    return filing_key, page_num


def load_classifications() -> dict:
    """Load classifications grouped by filing."""
    if not CLASSIFICATION_PATH.exists():
        return {}

    by_filing = defaultdict(list)

    with open(CLASSIFICATION_PATH) as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                rel_path = data.get("relative_path", "")
                filing_key, page_num = parse_path(rel_path)

                if filing_key and page_num:
                    by_filing[filing_key].append({
                        "page": page_num,
                        "path": rel_path,
                        "tags": data.get("section_tags", []),
                        "summary": data.get("summary", ""),
                        "extraction_score": data.get("extraction_score", "OK")
                    })
            except json.JSONDecodeError:
                continue

    return dict(by_filing)


def load_repaired_pages() -> dict:
    """Load repaired pages grouped by filing."""
    by_filing = defaultdict(list)

    for log_file in [REPAIRS_FIX_LOG, REPAIRS_REOCR_LOG]:
        if not log_file.exists():
            continue
        with open(log_file) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    if data.get("status") == "success":
                        rel_path = data.get("relative_path", "")
                        filing_key, page_num = parse_path(rel_path)
                        if filing_key and page_num:
                            by_filing[filing_key].append(page_num)
                except json.JSONDecodeError:
                    continue

    return dict(by_filing)


def extract_tags(tags_list: list) -> dict:
    """Extract high-confidence tags from tags list."""
    result = {}
    for tag_info in tags_list:
        tag = tag_info.get("tag", "")
        confidence = tag_info.get("confidence", 0)

        if tag in EXTRACTION_TAGS and confidence >= MIN_CONFIDENCE:
            result[tag] = confidence

    return result


def main():
    parser = argparse.ArgumentParser(description="Build extraction manifest")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE,
                        help="Minimum confidence to include a tag")
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 6: BUILD EXTRACTION MANIFEST")
    print("=" * 70)
    print()

    # Load data
    classifications = load_classifications()
    print(f"Filings with classifications: {len(classifications)}")

    repaired_pages = load_repaired_pages()
    print(f"Filings with repaired pages: {len(repaired_pages)}")

    # Build manifest
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "min_confidence": args.min_confidence,
        "filings": {}
    }

    # Stats
    stats = {
        "total_filings": 0,
        "total_pages_classified": 0,
        "filings_with_statements": 0,
        "filings_with_multi_year": 0,
        "filings_with_ceo_comp": 0,
        "pages_by_tag": defaultdict(int)
    }

    for filing_key, pages in classifications.items():
        if not pages:
            continue

        # Determine filing path
        sample_path = pages[0]["path"]
        parts = Path(sample_path).parts
        filing_path = f"markdown_pages/{'/'.join(parts[:3])}/"

        # Count total pages in filing
        filing_dir = MARKDOWN_ROOT / "/".join(parts[:3])
        page_count = len(list(filing_dir.glob("*.md"))) if filing_dir.exists() else len(pages)

        # Group pages by tag
        pages_by_tag = defaultdict(list)

        for page_info in pages:
            page_num = page_info["page"]
            tags = extract_tags(page_info.get("tags", []))

            for tag in tags:
                if page_num not in pages_by_tag[tag]:
                    pages_by_tag[tag].append(page_num)
                    stats["pages_by_tag"][tag] += 1

        # Sort page lists
        for tag in pages_by_tag:
            pages_by_tag[tag].sort()

        # Build filing entry
        filing_entry = {
            "filing_path": filing_path,
            "page_count": page_count,
            "pages": dict(pages_by_tag),
            "repaired_pages": sorted(repaired_pages.get(filing_key, []))
        }

        manifest["filings"][filing_key] = filing_entry
        stats["total_filings"] += 1
        stats["total_pages_classified"] += len(pages)

        # Update stats
        if "statement" in pages_by_tag:
            stats["filings_with_statements"] += 1
        if "multi_year" in pages_by_tag:
            stats["filings_with_multi_year"] += 1
        if "ceo_comp" in pages_by_tag:
            stats["filings_with_ceo_comp"] += 1

    manifest["stats"] = {
        "total_filings": stats["total_filings"],
        "total_pages_classified": stats["total_pages_classified"],
        "filings_with_statements": stats["filings_with_statements"],
        "filings_with_multi_year": stats["filings_with_multi_year"],
        "filings_with_ceo_comp": stats["filings_with_ceo_comp"],
        "pages_by_tag": dict(stats["pages_by_tag"])
    }

    # Write output
    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total filings: {stats['total_filings']}")
    print(f"Total pages classified: {stats['total_pages_classified']}")
    print()
    print("Filings by content type:")
    print(f"  With financial statements: {stats['filings_with_statements']}")
    print(f"  With multi-year summaries: {stats['filings_with_multi_year']}")
    print(f"  With CEO compensation: {stats['filings_with_ceo_comp']}")
    print()
    print("Pages by tag:")
    for tag, count in sorted(stats["pages_by_tag"].items(), key=lambda x: -x[1]):
        print(f"  {tag}: {count}")
    print()
    print(f"Output: {args.output}")
    print()
    print("Ready for Stage 3: Extract")


if __name__ == "__main__":
    main()
