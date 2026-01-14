#!/usr/bin/env python3
"""
Step 9: Build Classification Manifest.

Final Stage 1 output - builds a manifest of all pages ready for Stage 2 classification.
Consolidates skip manifest, corruption status, and page inventory.

Input:  markdown_pages/<ticker>/<year>/<doc>/page_###.md
        artifacts/stage1/skip_manifest.json
        artifacts/stage1/corrupted_pages.json
Output: artifacts/stage1/classification_manifest.json

Usage:
    python Step9_BuildClassificationManifest.py
    python Step9_BuildClassificationManifest.py --ticker SYS
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import MARKDOWN_ROOT, STAGE1_ARTIFACTS

OUTPUT_FILE = STAGE1_ARTIFACTS / "step9_classification_manifest.json"
SKIP_MANIFEST = STAGE1_ARTIFACTS / "step7_skip_manifest.json"
CORRUPTED_MANIFEST = STAGE1_ARTIFACTS / "step8_qc_issues.json"

PAGE_RE = re.compile(r"page_(\d+)\.md$", re.IGNORECASE)


def load_skip_paths() -> dict:
    """Load skip manifest with reasons."""
    if not SKIP_MANIFEST.exists():
        return {}

    with open(SKIP_MANIFEST) as f:
        data = json.load(f)

    return {p["relative_path"]: p["reason"] for p in data.get("pages", [])}


def load_corrupted_paths() -> set:
    """Load corrupted pages that still need fixing."""
    if not CORRUPTED_MANIFEST.exists():
        return set()

    with open(CORRUPTED_MANIFEST) as f:
        data = json.load(f)

    return {p["relative_path"] for p in data.get("pages", [])}


def build_manifest(ticker: str = None) -> dict:
    """Build the classification manifest."""

    # Load prior manifests
    skip_paths = load_skip_paths()
    corrupted_paths = load_corrupted_paths()

    # Find all markdown files
    if ticker:
        search_root = MARKDOWN_ROOT / ticker
    else:
        search_root = MARKDOWN_ROOT

    if not search_root.exists():
        return {"error": f"Markdown root not found: {search_root}"}

    # Organize by filing
    filings = defaultdict(lambda: {
        "pages": [],
        "skip_pages": [],
        "corrupted_pages": [],
    })

    for md_file in sorted(search_root.rglob("*.md")):
        match = PAGE_RE.search(md_file.name)
        if not match:
            continue

        page_num = int(match.group(1))
        rel_path = str(md_file.relative_to(MARKDOWN_ROOT))

        # Filing key: ticker/year/filing_name
        parts = rel_path.split("/")
        if len(parts) < 4:
            continue

        filing_key = "/".join(parts[:3])  # e.g., "LUCK/2024/LUCK_Annual_2024"

        page_info = {
            "page": page_num,
            "path": rel_path,
        }

        if rel_path in skip_paths:
            page_info["skip_reason"] = skip_paths[rel_path]
            filings[filing_key]["skip_pages"].append(page_info)
        elif rel_path in corrupted_paths:
            filings[filing_key]["corrupted_pages"].append(page_info)
        else:
            filings[filing_key]["pages"].append(page_info)

    # Build summary stats
    total_pages = 0
    ready_pages = 0
    skip_count = 0
    corrupted_count = 0

    for filing_key, filing_data in filings.items():
        filing_data["pages"].sort(key=lambda x: x["page"])
        filing_data["skip_pages"].sort(key=lambda x: x["page"])
        filing_data["corrupted_pages"].sort(key=lambda x: x["page"])

        n_pages = len(filing_data["pages"])
        n_skip = len(filing_data["skip_pages"])
        n_corrupt = len(filing_data["corrupted_pages"])

        total_pages += n_pages + n_skip + n_corrupt
        ready_pages += n_pages
        skip_count += n_skip
        corrupted_count += n_corrupt

    return {
        "generated_at": datetime.now().isoformat(),
        "stats": {
            "total_pages": total_pages,
            "ready_for_classification": ready_pages,
            "skipped": skip_count,
            "corrupted": corrupted_count,
            "total_filings": len(filings),
        },
        "filings": dict(filings),
    }


def main():
    parser = argparse.ArgumentParser(description="Build classification manifest for Stage 2")
    parser.add_argument("--ticker", help="Process single ticker")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 1 STEP 9: BUILD CLASSIFICATION MANIFEST")
    print("=" * 70)
    print()

    manifest = build_manifest(args.ticker)

    if "error" in manifest:
        print(f"ERROR: {manifest['error']}")
        return

    # Write output
    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Summary
    stats = manifest["stats"]
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total pages:              {stats['total_pages']:,}")
    print(f"Ready for classification: {stats['ready_for_classification']:,}")
    print(f"Skipped (Urdu/edges):     {stats['skipped']:,}")
    print(f"Corrupted (need ReOCR):   {stats['corrupted']:,}")
    print(f"Total filings:            {stats['total_filings']:,}")
    print()
    print(f"Output: {args.output}")

    if stats["corrupted"] > 0:
        print()
        print(f"WARNING: {stats['corrupted']} pages still corrupted!")
        print("         Run: python -m pipeline.utilities.ReOCR --manifest artifacts/stage1/step8_qc_issues.json")
    else:
        print()
        print("Stage 1 complete. Ready for Stage 2 classification.")


if __name__ == "__main__":
    main()
