#!/usr/bin/env python3
"""
Step 11 - Compile enriched JSONL database from markdown sources.

For each markdown file under ``markdown_pages`` (or --input),
this script:
    * splits the document into pages using <!-- Page X --> markers,
    * attaches the relevant Step9 summary, Step8 diagnostics, and ticker metadata,
    * writes JSONL records with the enriched schema to the output root.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Set, Tuple

PAGE_PATTERN = re.compile(r"<!--\s*Page\s+(\d+)\s*-->", re.IGNORECASE)
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
PAGE_FILE_PATTERN = re.compile(r"page_(\d+)\.md$", re.IGNORECASE)
INPUT_ROOT = Path("markdown_pages")
OUTPUT_ROOT = Path("database_jsonl_compiled")
SUMMARY_ROOT = Path("markdown_summary")
TICKER_META_PATH = Path("tickers100.json")
TAGS_MANIFEST = Path("artifacts/stage2/Step6_deepseek_reviews.jsonl")
SKIP_MANIFEST = Path("artifacts/stage2/Step7_combined_skip.jsonl")
OVERWRITE_OUTPUT = True


def load_ticker_metadata(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    mapping: Dict[str, Dict[str, str]] = {}
    for entry in data:
        symbol = entry.get("Symbol")
        if symbol:
            mapping[symbol.upper()] = entry
    return mapping


def split_markdown_pages(text: str) -> Dict[int, str]:
    matches = list(PAGE_PATTERN.finditer(text))
    if not matches:
        return {1: text.strip()}
    pages: Dict[int, str] = {}
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        block = text[start:end].strip()
        try:
            page_no = int(match.group(1))
        except (TypeError, ValueError):
            continue
        pages[page_no] = block
    return pages


PAGE_FILE_RE = re.compile(r"page_(\d+)\.md$", re.IGNORECASE)
SUMMARY_PAGE_RE = re.compile(r"<!--\s*Page\s+(\d+)\s*-->", re.IGNORECASE)


def load_summary_map(summary_dir: Path) -> Dict[int, str]:
    """Load page-wise summaries from summary_dir/page_###.md files."""
    if not summary_dir.exists():
        return {}
    sections: Dict[int, str] = {}
    for summary_file in summary_dir.glob("page_*.md"):
        match = PAGE_FILE_RE.search(summary_file.name)
        if not match:
            continue
        page_num = int(match.group(1))
        try:
            text = summary_file.read_text(encoding="utf-8")
            # Strip the <!-- Page X --> header if present
            text = SUMMARY_PAGE_RE.sub("", text, count=1).strip()
            sections[page_num] = text
        except Exception:  # noqa: BLE001
            continue
    return sections


def load_skip_page_map(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    skipped: Set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            rel = payload.get("relative_path") or payload.get("path")
            if isinstance(rel, str) and rel:
                skipped.add(rel.replace("\\", "/"))
    return skipped


def load_section_tags(path: Path) -> Dict[str, Dict[str, float]]:
    tags: Dict[str, Dict[str, float]] = {}
    if not path.exists():
        return tags
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            rel = payload.get("relative_path")
            section_tags = payload.get("section_tags")
            if isinstance(rel, str):
                key = rel.replace("\\", "/")
                tags[key] = section_tags if isinstance(section_tags, dict) else {}
    return tags


def derive_filing_type_and_year(folder_name: str, report_stem: str) -> Tuple[str, str]:
    """Infer filing type (annual vs quarterly) and year from folder/name hints."""

    folder_year = folder_name if re.fullmatch(r"\d{4}", folder_name) else ""
    year_match = re.search(r"(19|20)\d{2}", report_stem)
    filing_year = year_match.group(0) if year_match else folder_year

    stem_lower = report_stem.lower()
    has_explicit_quarter = "quarter" in stem_lower
    has_full_date = bool(DATE_PATTERN.search(report_stem))

    if has_explicit_quarter or has_full_date:
        return "quarterly", filing_year
    return "annual", filing_year or folder_year


def extract_filing_period(
    report_stem: str,
    folder: str,
    ticker: str,
    filing_type: str,
    filing_year_hint: str,
    fiscal_period_suffix: str | None,
) -> str:
    match = DATE_PATTERN.search(report_stem)
    if match:
        return match.group(0)
    if filing_type == "annual":
        return resolve_annual_period(report_stem, folder, filing_year_hint, fiscal_period_suffix, ticker)
    if DATE_PATTERN.fullmatch(folder):
        return folder
    return folder


def resolve_annual_period(
    report_stem: str,
    folder: str,
    filing_year_hint: str,
    fiscal_period_suffix: str | None,
    ticker: str,
) -> str:
    suffix = (fiscal_period_suffix or "").strip()
    if not suffix:
        raise ValueError(f"Missing fiscal_period metadata for annual filing {ticker} ({folder}/{report_stem})")
    year_candidate = filing_year_hint.strip()
    if not year_candidate:
        if re.fullmatch(r"\d{4}", folder):
            year_candidate = folder
        else:
            folder_match = re.search(r"(19|20)\d{2}", folder)
            if folder_match:
                year_candidate = folder_match.group(0)
    if not year_candidate:
        stem_match = re.search(r"(19|20)\d{2}", report_stem)
        if stem_match:
            year_candidate = stem_match.group(0)
    if not year_candidate:
        raise ValueError(f"Cannot determine filing year for annual report {ticker} ({folder}/{report_stem})")
    return f"{year_candidate}-{suffix}"


def process_document_dir(
    doc_dir: Path,
    input_root: Path,
    output_root: Path,
    summary_root: Path,
    ticker_meta: Dict[str, Dict[str, str]],
    overwrite: bool,
    skipped_pages: Set[str],
    section_tag_map: Dict[str, Dict[str, float]],
) -> None:
    page_files = sorted(doc_dir.glob("page_*.md"))
    if not page_files:
        return
    relative = doc_dir.relative_to(input_root)
    parts = relative.parts
    if len(parts) < 3:
        return
    ticker, folder, report_stem = parts[0], parts[1], parts[2]
    summary_path = summary_root / ticker / folder / report_stem
    output_path = output_root / ticker / folder / f"{report_stem}.jsonl"
    if output_path.exists() and not overwrite:
        return

    summary_map = load_summary_map(summary_path)
    ticker_upper = ticker.upper()
    ticker_info = ticker_meta.get(ticker_upper, {})
    industry = ticker_info.get("Industry", "")
    filing_type, filing_year_hint = derive_filing_type_and_year(folder, report_stem)
    fiscal_period_suffix = (ticker_info.get("fiscal_period") or "").strip()
    filing_period = extract_filing_period(
        report_stem,
        folder,
        ticker,
        filing_type,
        filing_year_hint,
        fiscal_period_suffix,
    )
    filing_year = filing_year_hint or (filing_period[:4] if len(filing_period) >= 4 else "")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for page_file in page_files:
            match = PAGE_FILE_PATTERN.search(page_file.name)
            if not match:
                continue
            page_no = int(match.group(1))
            rel_page = str(page_file.relative_to(input_root)).replace("\\", "/")
            if rel_page in skipped_pages:
                continue
            try:
                page_text = page_file.read_text(encoding="utf-8")
            except Exception:  # noqa: BLE001
                continue
            summary = summary_map.get(page_no, "")
            jpg_path = f"{ticker}/{folder}/{report_stem}/page_{page_no:03d}.jpg"
            section_tags = section_tag_map.get(rel_page, {})
            record = {
                "ticker": ticker_upper,
                "industry": industry,
                "filing_type": filing_type,
                "filing_period": filing_period,
                "filing_year": int(filing_year) if filing_year.isdigit() else filing_year,
                "section_tags": section_tags,
                "pg": page_no,
                "jpg_path": jpg_path,
                "summary": summary,
                "text": page_text,
            }
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    input_root = INPUT_ROOT
    output_root = OUTPUT_ROOT
    summary_root = SUMMARY_ROOT
    ticker_meta = load_ticker_metadata(TICKER_META_PATH)
    skip_pages = load_skip_page_map(SKIP_MANIFEST)
    section_tags = load_section_tags(TAGS_MANIFEST)

    if not input_root.exists():
        raise SystemExit(f"Input directory not found: {input_root}")

    for ticker_dir in sorted([p for p in input_root.iterdir() if p.is_dir()]):
        for year_dir in sorted([p for p in ticker_dir.iterdir() if p.is_dir()]):
            for doc_dir in sorted([p for p in year_dir.iterdir() if p.is_dir()]):
                process_document_dir(
                    doc_dir,
                    input_root,
                    output_root,
                    summary_root,
                    ticker_meta,
                    OVERWRITE_OUTPUT,
                    skip_pages,
                    section_tags,
                )


if __name__ == "__main__":
    main()
