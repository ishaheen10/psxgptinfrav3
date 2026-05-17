#!/usr/bin/env python3
"""
Audit generated search_text files for hallucination on thin/image-only source pages.

For each file in --search-root, loads the matching source page from markdown_pages/
and classifies the source as: IMAGE_ONLY, HEADING_ONLY, BLANK, THIN, or NORMAL.

Flags cases where source is thin but generated search_text is non-trivial,
which indicates likely hallucination.

Output:
  - Console summary by page category
  - artifacts/stage2/search_text_audit.jsonl  (flagged cases, full detail)
  - artifacts/stage2/search_text_audit.md     (human-readable sample report)

Usage:
  python pipeline/stage2_review/AuditSearchTextThinPages.py
  python pipeline/stage2_review/AuditSearchTextThinPages.py --search-root markdown_search_text_v5_full
  python pipeline/stage2_review/AuditSearchTextThinPages.py --content-threshold 150 --min-flag-words 6
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import MARKDOWN_ROOT, PROJECT_ROOT, STAGE2_ARTIFACTS  # noqa: E402

DEFAULT_SEARCH_ROOT = PROJECT_ROOT / "markdown_search_text_v5_full"
OUTPUT_JSONL = STAGE2_ARTIFACTS / "search_text_audit.jsonl"
OUTPUT_MD = STAGE2_ARTIFACTS / "search_text_audit.md"

# A page is flagged if source content_chars < this AND search_text has >= MIN_FLAG_WORDS
DEFAULT_CONTENT_THRESHOLD = 150
DEFAULT_MIN_FLAG_WORDS = 6

# Max suspicious cases to include in the .md report
MD_REPORT_LIMIT = 100

_IMG_RE = re.compile(r'!\[.*?\]\(.*?\)', re.DOTALL)
_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)
_HEADING_LINE_RE = re.compile(r'^#{1,6}\s*', re.MULTILINE)


def compute_content_chars(raw: str) -> int:
    """Chars remaining after stripping page comment, image tags, heading markers, blanks."""
    text = _COMMENT_RE.sub('', raw)
    text = _IMG_RE.sub('', text)
    text = _HEADING_LINE_RE.sub('', text)
    return len(text.strip())


def count_images(raw: str) -> int:
    return len(_IMG_RE.findall(raw))


def classify_source(raw: str, content_threshold: int) -> str:
    """Return one of: BLANK, IMAGE_ONLY, HEADING_ONLY, THIN, NORMAL."""
    raw_stripped = raw.strip()
    if len(raw_stripped) < 30:
        return "BLANK"

    images = count_images(raw)
    content_chars = compute_content_chars(raw)

    if images >= 1 and content_chars < 40:
        return "IMAGE_ONLY"

    if images == 0 and content_chars < 40:
        return "HEADING_ONLY"

    if content_chars < content_threshold:
        return "THIN"

    return "NORMAL"


def word_count(text: str) -> int:
    return len(text.split())


def audit_file(
    search_path: Path,
    search_root: Path,
    content_threshold: int,
    min_flag_words: int,
) -> dict | None:
    """Return audit record if flagged, else None."""
    relative = search_path.relative_to(search_root)
    source_path = MARKDOWN_ROOT / relative

    search_text = search_path.read_text(encoding="utf-8", errors="ignore").strip()
    st_words = word_count(search_text)

    if not source_path.exists():
        return {
            "relative_path": str(relative),
            "flag_reason": "SOURCE_MISSING",
            "source_raw_chars": 0,
            "source_content_chars": 0,
            "source_category": "MISSING",
            "search_text_words": st_words,
            "search_text": search_text,
            "source_excerpt": "",
        }

    raw = source_path.read_text(encoding="utf-8", errors="ignore")
    category = classify_source(raw, content_threshold)
    content_chars = compute_content_chars(raw)

    if category == "NORMAL":
        return None

    if st_words < min_flag_words:
        return None

    return {
        "relative_path": str(relative),
        "flag_reason": category,
        "source_raw_chars": len(raw),
        "source_content_chars": content_chars,
        "source_category": category,
        "source_images": count_images(raw),
        "search_text_words": st_words,
        "search_text": search_text,
        "source_excerpt": raw.strip()[:300],
    }


def render_md(flagged: list[dict], total: int, category_counts: Counter) -> str:
    lines = [
        "# Search Text Thin-Page Audit",
        "",
        f"Total generated files scanned: {total:,}",
        "",
        "## Source Page Category Breakdown",
        "",
    ]
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- **{cat}**: {count:,}")
    lines.append("")
    lines.append(f"## Flagged (thin source + non-trivial search_text): {len(flagged):,}")
    lines.append("")
    flag_by_reason: Counter = Counter(r["flag_reason"] for r in flagged)
    for reason, count in sorted(flag_by_reason.items(), key=lambda x: -x[1]):
        lines.append(f"- **{reason}**: {count:,}")
    lines.append("")
    lines.append(f"## Sample Cases (first {min(MD_REPORT_LIMIT, len(flagged))})")
    lines.append("")

    for record in flagged[:MD_REPORT_LIMIT]:
        lines.append(f"### {record['relative_path']}")
        lines.append("")
        lines.append(f"**Category:** {record['flag_reason']}  |  "
                     f"**Source content chars:** {record['source_content_chars']}  |  "
                     f"**Search text words:** {record['search_text_words']}")
        lines.append("")
        lines.append("**Generated search_text:**")
        lines.append("")
        lines.append(record["search_text"] or "_empty_")
        lines.append("")
        lines.append("**Source excerpt:**")
        lines.append("")
        lines.append("```")
        lines.append(record["source_excerpt"])
        lines.append("```")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main():
    parser = argparse.ArgumentParser(description="Audit search_text for thin-page hallucinations")
    parser.add_argument("--search-root", type=Path, default=DEFAULT_SEARCH_ROOT)
    parser.add_argument("--output-jsonl", type=Path, default=OUTPUT_JSONL)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    parser.add_argument("--content-threshold", type=int, default=DEFAULT_CONTENT_THRESHOLD,
                        help="Source content chars below this → flagged as thin (default 150)")
    parser.add_argument("--min-flag-words", type=int, default=DEFAULT_MIN_FLAG_WORDS,
                        help="Only flag if search_text has at least this many words (default 6)")
    args = parser.parse_args()

    if not args.search_root.exists():
        raise SystemExit(f"Search root not found: {args.search_root}")

    print(f"Scanning: {args.search_root}")
    print(f"Content threshold: {args.content_threshold} chars")
    print(f"Min flag words: {args.min_flag_words}")
    print()

    all_files = list(args.search_root.rglob("*.md"))
    total = len(all_files)
    print(f"Files to audit: {total:,}")

    category_counts: Counter = Counter()
    flagged: list[dict] = []

    for i, search_path in enumerate(sorted(all_files), start=1):
        if i % 10000 == 0:
            print(f"  {i:,}/{total:,} scanned | flagged so far: {len(flagged):,}")

        relative = search_path.relative_to(args.search_root)
        source_path = MARKDOWN_ROOT / relative

        if not source_path.exists():
            category_counts["MISSING"] += 1
            record = audit_file(search_path, args.search_root, args.content_threshold, args.min_flag_words)
            if record:
                flagged.append(record)
            continue

        raw = source_path.read_text(encoding="utf-8", errors="ignore")
        category = classify_source(raw, args.content_threshold)
        category_counts[category] += 1

        record = audit_file(search_path, args.search_root, args.content_threshold, args.min_flag_words)
        if record:
            flagged.append(record)

    flagged.sort(key=lambda r: r["relative_path"])

    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with args.output_jsonl.open("w", encoding="utf-8") as fh:
        for record in flagged:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    args.output_md.write_text(render_md(flagged, total, category_counts), encoding="utf-8")

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total scanned:  {total:,}")
    print()
    print("Source page categories:")
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        pct = 100 * count / total if total else 0
        print(f"  {cat:<15} {count:>8,}  ({pct:.1f}%)")
    print()
    flag_by_reason: Counter = Counter(r["flag_reason"] for r in flagged)
    print(f"Flagged (thin source + >= {args.min_flag_words} word search_text): {len(flagged):,}")
    for reason, count in sorted(flag_by_reason.items(), key=lambda x: -x[1]):
        pct = 100 * count / total if total else 0
        print(f"  {reason:<15} {count:>8,}  ({pct:.1f}%)")
    print()
    print(f"Report: {args.output_md}")
    print(f"JSONL:  {args.output_jsonl}")


if __name__ == "__main__":
    main()
