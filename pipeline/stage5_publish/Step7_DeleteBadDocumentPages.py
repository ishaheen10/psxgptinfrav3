#!/usr/bin/env python3
"""
Identify and delete thin/garbage/Urdu pages from financial_documents_new.

Scans all compiled JSONL records, classifies each page's text using the same
logic as Step7_BuildSkipManifest.py, and generates DELETE SQL for rows that
should never have been in the database.

Skip reasons detected:
  blank         - near-empty after stripping the page comment
  empty_table   - only markdown table pipes, no real content
  image_only    - page is just image tags with no text
  heading_only  - section divider heading with no body
  thin          - too little content to be useful (<150 chars)
  urdu          - Urdu-heavy text (ASCII ratio < 0.85 on content-only)

Output:
  artifacts/stage5/step7_delete_bad_pages.sql   (DELETE statements)
  artifacts/stage5/step7_delete_bad_pages.jsonl (audit trail of deleted rows)

Usage:
  python pipeline/stage5_publish/Step7_DeleteBadDocumentPages.py --sql-only
  python pipeline/stage5_publish/Step7_DeleteBadDocumentPages.py
  python pipeline/stage5_publish/Step7_DeleteBadDocumentPages.py -y
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "database_jsonl_compiled"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage5"
OUTPUT_SQL = ARTIFACTS_DIR / "step7_delete_bad_pages.sql"
OUTPUT_JSONL = ARTIFACTS_DIR / "step7_delete_bad_pages.jsonl"

D1_DATABASE = "psx"
TABLE_NAME = "financial_documents_new"

# Must match Step7_BuildSkipManifest.py
ASCII_THRESHOLD = 0.85
THIN_CONTENT_THRESHOLD = 150
EMPTY_TABLE_THRESHOLD = 20

_IMG_RE = re.compile(r'!\[.*?\]\(.*?\)', re.DOTALL)
_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)


def _compute_content_chars(text: str) -> int:
    text = _COMMENT_RE.sub('', text)
    text = _IMG_RE.sub('', text)
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    return len(text.strip())


def _ascii_ratio_content_only(text: str) -> float:
    """ASCII ratio excluding pipes and whitespace (catches Urdu buried in table markup)."""
    content = re.sub(r'[|\s]', '', text)
    if not content:
        return 1.0
    return sum(1 for ch in content if ord(ch) < 128) / len(content)


def classify_page(text: str) -> str | None:
    """Return a skip reason if this page should be deleted, else None."""
    body = _COMMENT_RE.sub('', text).strip()

    if len(body) < 20:
        return "blank"

    if len(re.sub(r'[|\s]', '', body)) < EMPTY_TABLE_THRESHOLD:
        return "empty_table"

    images = len(_IMG_RE.findall(body))
    content_chars = _compute_content_chars(text)

    if images >= 1 and content_chars < 40:
        return "image_only"

    if images == 0 and content_chars < 40:
        return "heading_only"

    if content_chars < THIN_CONTENT_THRESHOLD:
        return "thin"

    if _ascii_ratio_content_only(text) < ASCII_THRESHOLD:
        return "urdu"

    return None


def escape_sql(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def make_delete(record: dict) -> str:
    return (
        f"DELETE FROM {TABLE_NAME} "
        f"WHERE ticker = {escape_sql(record['ticker'])} "
        f"AND filing_type = {escape_sql(record['filing_type'])} "
        f"AND filing_period = {escape_sql(record['filing_period'])} "
        f"AND pg = {escape_sql(record['pg'])};"
    )


def scan_compiled_jsonl(input_dir: Path) -> tuple[list[dict], int]:
    """Return (bad_records, total_scanned)."""
    bad: list[dict] = []
    total = 0

    files = sorted(input_dir.rglob("*.jsonl"))
    for jsonl_file in files:
        with jsonl_file.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                total += 1
                text = record.get("text") or ""
                reason = classify_page(text)
                if reason:
                    bad.append({
                        "ticker": record.get("ticker"),
                        "filing_type": record.get("filing_type"),
                        "filing_period": record.get("filing_period"),
                        "filing_year": record.get("filing_year"),
                        "pg": record.get("pg"),
                        "skip_reason": reason,
                        "search_text": (record.get("search_text") or "")[:120],
                        "text_excerpt": text.strip()[:200],
                    })

    return bad, total


def write_sql(bad: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    by_reason = Counter(r["skip_reason"] for r in bad)
    with output_path.open("w", encoding="utf-8") as fh:
        fh.write(f"-- Delete bad/thin/Urdu pages from {TABLE_NAME}\n")
        fh.write(f"-- Generated: {datetime.now().isoformat()}\n")
        fh.write(f"-- Rows to delete: {len(bad):,}\n")
        for reason, count in sorted(by_reason.items()):
            fh.write(f"--   {reason}: {count:,}\n")
        fh.write("\n")
        for record in bad:
            fh.write(make_delete(record) + "\n")


def write_audit_jsonl(bad: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in bad:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def upload_sql(sql_file: Path, batch_size: int = 2000) -> bool:
    with open(sql_file) as fh:
        statements = [l.strip() for l in fh if l.strip().startswith("DELETE")]

    if not statements:
        print("No DELETE statements to upload.")
        return True

    total_batches = (len(statements) + batch_size - 1) // batch_size
    print(f"Uploading {len(statements):,} DELETE statements in {total_batches} batch(es)...")

    for i in range(0, len(statements), batch_size):
        batch_sql = "\n".join(statements[i : i + batch_size])
        batch_num = i // batch_size + 1

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp:
            tmp.write(batch_sql)
            tmp_path = tmp.name

        try:
            result = subprocess.run(
                ["npx", "wrangler", "d1", "execute", D1_DATABASE, "--remote", f"--file={tmp_path}"],
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT,
            )
            success = '"success": true' in result.stdout or '"success":true' in result.stdout
            if success:
                print(f"  Batch {batch_num}/{total_batches} OK")
            else:
                print(f"  Batch {batch_num}/{total_batches} FAILED")
                error_lines = [l for l in result.stderr.split("\n") if "ExperimentalWarning" not in l and l.strip()]
                if error_lines:
                    print("  " + "\n  ".join(error_lines[:5]))
                return False
        finally:
            os.unlink(tmp_path)

    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete bad/thin/Urdu pages from financial_documents_new")
    parser.add_argument("--input", type=Path, default=INPUT_DIR)
    parser.add_argument("--output-sql", type=Path, default=OUTPUT_SQL)
    parser.add_argument("--output-jsonl", type=Path, default=OUTPUT_JSONL)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--sql-only", action="store_true", help="Generate SQL only, do not upload")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    args = parser.parse_args()

    print("=" * 60)
    print("STEP 7: DELETE BAD DOCUMENT PAGES FROM D1")
    print("=" * 60)
    print(f"Database: {D1_DATABASE}")
    print(f"Table:    {TABLE_NAME}")
    print(f"Input:    {args.input}")
    print()

    if not args.input.exists():
        raise SystemExit(f"Input directory not found: {args.input}")

    print("Scanning compiled JSONL records...")
    bad, total = scan_compiled_jsonl(args.input)

    print(f"Total pages scanned: {total:,}")
    print(f"Pages to delete:     {len(bad):,}")
    print()

    by_reason = Counter(r["skip_reason"] for r in bad)
    for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
        pct = 100 * count / total if total else 0
        print(f"  {reason:<15} {count:>6,}  ({pct:.1f}%)")

    if not bad:
        print("\nNothing to delete.")
        return

    write_sql(bad, args.output_sql)
    write_audit_jsonl(bad, args.output_jsonl)
    print(f"\nSQL:   {args.output_sql}")
    print(f"JSONL: {args.output_jsonl}")

    if args.sql_only:
        print("\nSQL generated. Run without --sql-only to upload.")
        return

    print()
    if not args.yes:
        confirm = input(f"Delete {len(bad):,} rows from {TABLE_NAME}? (y/N) ")
        if confirm.lower() != "y":
            print("Aborted.")
            return

    print()
    ok = upload_sql(args.output_sql, args.batch_size)

    print()
    print("=" * 60)
    print("DELETE COMPLETE" if ok else "DELETE FAILED")
    print("=" * 60)
    if ok:
        print()
        print("Rebuild FTS after deletion:")
        print("  INSERT INTO filings_fts(filings_fts) VALUES ('rebuild');")


if __name__ == "__main__":
    main()
