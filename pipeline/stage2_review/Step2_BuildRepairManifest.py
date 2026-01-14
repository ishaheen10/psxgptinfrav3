#!/usr/bin/env python3
"""
Step 2: Build repair manifest from classification results.

Routes pages by extraction quality score:
- OK: No repair needed, ready for extraction
- Fix: Minor issues - DeepSeek can repair from markdown (Step 3)
- ReOCR: Severe corruption - needs Gemini re-OCR from PDF (Step 4)

Input:  artifacts/stage2/step1_classification.jsonl
Output: artifacts/stage2/step2_repair_manifest.json

Usage:
    python Step2_BuildRepairManifest.py
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import STAGE2_ARTIFACTS

CLASSIFICATION_PATH = STAGE2_ARTIFACTS / "step1_classification.jsonl"
OUTPUT_PATH = STAGE2_ARTIFACTS / "step2_repair_manifest.json"


def load_classifications() -> list:
    """Load all classification results."""
    if not CLASSIFICATION_PATH.exists():
        return []

    results = []
    with open(CLASSIFICATION_PATH) as f:
        for line in f:
            if line.strip():
                try:
                    results.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return results


def main():
    parser = argparse.ArgumentParser(description="Build repair manifest from classifications")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 2: BUILD REPAIR MANIFEST")
    print("=" * 70)
    print()

    classifications = load_classifications()
    print(f"Loaded {len(classifications)} classifications")

    if not classifications:
        print("No classifications found. Run Step1_ClassifyPages.py first.")
        return

    # Route by extraction_score
    by_score = defaultdict(list)
    no_score = []

    for c in classifications:
        score = c.get("extraction_score", "").upper()
        rel_path = c.get("relative_path", "")

        if not rel_path:
            continue

        if score in ("OK", "FIX", "REOCR"):
            by_score[score].append({
                "relative_path": rel_path,
                "summary": c.get("summary", ""),
                "section_tags": c.get("section_tags", [])
            })
        else:
            no_score.append(rel_path)

    # Build manifest
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "total_classified": len(classifications),
        "counts": {
            "OK": len(by_score.get("OK", [])),
            "Fix": len(by_score.get("FIX", [])),
            "ReOCR": len(by_score.get("REOCR", [])),
            "unknown": len(no_score)
        },
        "pages_ok": by_score.get("OK", []),
        "pages_fix": by_score.get("FIX", []),
        "pages_reocr": by_score.get("REOCR", [])
    }

    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Summary
    print()
    print("=" * 70)
    print("REPAIR ROUTING SUMMARY")
    print("=" * 70)
    print(f"OK (no repair needed): {manifest['counts']['OK']}")
    print(f"Fix (DeepSeek text repair): {manifest['counts']['Fix']}")
    print(f"ReOCR (Gemini PDF re-OCR): {manifest['counts']['ReOCR']}")
    print(f"Unknown score: {manifest['counts']['unknown']}")
    print()
    print(f"Output: {args.output}")

    if manifest['counts']['Fix'] > 0:
        print(f"\nNext: Run Step3_RepairFix.py to repair {manifest['counts']['Fix']} pages")
    if manifest['counts']['ReOCR'] > 0:
        print(f"Then: Run Step4_RepairReOCR.py to re-OCR {manifest['counts']['ReOCR']} pages")


if __name__ == "__main__":
    main()
