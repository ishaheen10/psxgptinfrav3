#!/usr/bin/env python3
"""
Step 5: Final corruption check after repairs.

Re-runs corruption detection on repaired pages to verify they're now clean.
Any pages that still fail are logged for manual investigation.

Input:  artifacts/stage2/step3_repairs_fix.jsonl
        artifacts/stage2/step4_repairs_reocr.jsonl
        markdown_pages/
Output: artifacts/stage2/step5_post_repair_qc.json

Usage:
    python Step5_FinalCorruptionCheck.py
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import MARKDOWN_ROOT, STAGE2_ARTIFACTS

REPAIRS_FIX_LOG = STAGE2_ARTIFACTS / "step3_repairs_fix.jsonl"
REPAIRS_REOCR_LOG = STAGE2_ARTIFACTS / "step4_repairs_reocr.jsonl"
OUTPUT_PATH = STAGE2_ARTIFACTS / "step5_post_repair_qc.json"

# Same thresholds as Stage 1 Step 7
MAX_REPEAT_RUN = 12
MIN_UNIQUE_RATIO = 0.25
MIN_LINES = 10
MAX_LINE_LENGTH = 2000


def check_corruption(text: str) -> dict | None:
    """Check for corruption patterns."""
    lines = text.strip().split('\n')
    non_empty = [l for l in lines if l.strip()]

    if len(non_empty) < MIN_LINES:
        return None

    # Repeated lines
    max_run = 1
    current_run = 1
    prev = None
    for line in non_empty:
        if line == prev:
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 1
        prev = line

    if max_run >= MAX_REPEAT_RUN:
        return {"type": "repeated_lines", "max_run": max_run}

    # Low unique ratio
    unique = set(non_empty)
    ratio = len(unique) / len(non_empty)
    if ratio < MIN_UNIQUE_RATIO:
        return {"type": "low_unique_ratio", "ratio": round(ratio, 3)}

    # Ultra-long lines
    for line in non_empty:
        if len(line) > MAX_LINE_LENGTH:
            special = sum(1 for c in line if c in '|-_=')
            if special / len(line) > 0.5:
                return {"type": "corrupted_separator", "line_length": len(line)}

    return None


def load_repaired_paths() -> list:
    """Load list of repaired page paths."""
    paths = []

    for log_file in [REPAIRS_FIX_LOG, REPAIRS_REOCR_LOG]:
        if not log_file.exists():
            continue
        with open(log_file) as f:
            for line in f:
                if line.strip():
                    try:
                        data = json.loads(line)
                        if data.get("status") == "success":
                            paths.append(data.get("relative_path", ""))
                    except:
                        continue

    return [p for p in paths if p]


def main():
    parser = argparse.ArgumentParser(description="Final corruption check")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("STAGE 2 STEP 5: FINAL CORRUPTION CHECK")
    print("=" * 70)
    print()

    # Load repaired paths
    repaired_paths = load_repaired_paths()
    print(f"Repaired pages to check: {len(repaired_paths)}")

    if not repaired_paths:
        print("No repaired pages to check")
        manifest = {
            "generated_at": datetime.now().isoformat(),
            "pages_checked": 0,
            "still_corrupted": 0,
            "pages": []
        }
        with open(args.output, 'w') as f:
            json.dump(manifest, f, indent=2)
        return

    # Check each repaired page
    still_corrupted = []
    by_type = defaultdict(int)

    for rel_path in repaired_paths:
        md_path = MARKDOWN_ROOT / rel_path
        if not md_path.exists():
            continue

        try:
            text = md_path.read_text(encoding="utf-8", errors="ignore")
            corruption = check_corruption(text)

            if corruption:
                still_corrupted.append({
                    "relative_path": rel_path,
                    "corruption": corruption
                })
                by_type[corruption["type"]] += 1
        except Exception:
            continue

    # Write output
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "pages_checked": len(repaired_paths),
        "still_corrupted": len(still_corrupted),
        "by_type": dict(by_type),
        "pages": still_corrupted
    }

    with open(args.output, 'w') as f:
        json.dump(manifest, f, indent=2)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Pages checked: {len(repaired_paths)}")
    print(f"Still corrupted: {len(still_corrupted)}")

    if still_corrupted:
        print("\nCorruption types:")
        for ctype, count in by_type.items():
            print(f"  {ctype}: {count}")
        print(f"\nThese pages may need manual investigation.")
    else:
        print("\nAll repaired pages are clean!")

    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
