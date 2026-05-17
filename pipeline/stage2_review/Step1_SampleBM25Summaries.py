#!/usr/bin/env python3
"""
Pilot a retrieval-optimized search_text prompt on a small fixed sample.

This does not modify the main pipeline outputs. It reads a list of markdown pages,
calls DeepSeek once per page, and writes a side-by-side review artifact so the
prompt can be tuned before regenerating all summaries.

Input:
  - artifacts/stage2/bm25_summary_sample_paths.json
  - markdown_pages/<ticker>/<year>/<filing>/page_###.md
  - markdown_summary/<ticker>/<year>/<filing>/page_###.md (current summary)

Output:
  - artifacts/stage2/bm25_summary_prompt.txt
  - artifacts/stage2/bm25_summary_sample.jsonl
  - artifacts/stage2/bm25_summary_sample.md

Usage:
  python pipeline/stage2_review/Step1_SampleBM25Summaries.py
  python pipeline/stage2_review/Step1_SampleBM25Summaries.py --workers 10
  python pipeline/stage2_review/Step1_SampleBM25Summaries.py --paths path/to/sample_paths.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import MARKDOWN_ROOT, PROJECT_ROOT  # noqa: E402

SAMPLE_PATHS = PROJECT_ROOT / "artifacts" / "stage2" / "bm25_summary_sample_paths.json"
OUTPUT_PROMPT = PROJECT_ROOT / "artifacts" / "stage2" / "bm25_summary_prompt.txt"
OUTPUT_JSONL = PROJECT_ROOT / "artifacts" / "stage2" / "bm25_summary_sample.jsonl"
OUTPUT_MD = PROJECT_ROOT / "artifacts" / "stage2" / "bm25_summary_sample.md"
CURRENT_SUMMARY_ROOT = PROJECT_ROOT / "markdown_summary"

DEEPSEEK_MODEL = os.getenv("DEEPSEEK_REVIEW_MODEL", "deepseek-chat")
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
TEMPERATURE = float(os.getenv("BM25_SUMMARY_TEMPERATURE", "0.1"))
MAX_CHARS = int(os.getenv("BM25_SUMMARY_MAX_CHARS", "3500"))
MAX_RETRIES = int(os.getenv("BM25_SUMMARY_MAX_RETRIES", "3"))
RETRY_WAIT = float(os.getenv("BM25_SUMMARY_RETRY_WAIT", "5.0"))
REQUEST_TIMEOUT = float(os.getenv("BM25_SUMMARY_REQUEST_TIMEOUT", "120.0"))

PROMPT = """You rewrite PSX filing pages into a single retrieval field for BM25 search.

Your job is NOT to write prose. Your job is to maximize discoverability for Scout retrieval.

Return strict JSON:
{
  "search_text": "<single lowercase search field>"
}

Rules for `search_text`:
- Output a single lowercase text blob, not sentences.
- Keep it very short: usually 18 to 35 words total.
- Front-load only the highest-signal concepts on the page.
- Include only 3 to 6 core concept phrases.
- Include only 0 to 4 labeled numeric facts as `label value unit`, not raw numbers alone.
- Include only 0 to 3 scope terms when they materially help retrieval, e.g. `domestic only`, `group basis`, `year end 2024`.
- Prefer concrete business nouns, metric labels, breakdown labels, and table concepts.
- For large tables, keep only the metrics most likely to be queried directly.
- If both macro context and company-specific facts are present, prefer the company-specific facts.
- Drop generic headings and document labels such as `directors report`, `macroeconomic review`, `external environment factors`, `consolidated financial statements` unless they are themselves likely query terms.
- Do not repeat obvious company-name or filing labels unless they materially help retrieval.
- Prefer analyst search phrases over page-title wording.
- Start with the first query-worthy concept or metric, not the page heading.
- Never start the field with phrases like `macroeconomic review`, `external environment factors`, `directors report`, `chairman review`, `notes to the financial statements`, or `consolidated financial statements`.
- Omit generic filler like management boilerplate, shareholder language, broad macro detail, and routine corporate wording.
- Avoid filler words and generic narration.
- Avoid invented terms or interpretation not supported by the page.
- Do not use punctuation except percent signs when needed.
- Keep the field dense but readable as whitespace-separated phrases.

Examples:
- Bad: `this page outlines the companys strategy and performance`
- Good: `deposit growth fee income digital banking transformation home remittances low cost deposits`

- Bad: `this page details operations in iraq`
- Good: `iraq cement subsidiary production capacity captive power plant regional market share amcmc`

- Bad: `1394 1450 5.74`
- Good: `domestic branches 1394 atms 1450 deposit market share 5.74 percent`

- Bad: `profit before tax pbt 150.2 billion ... customer base employee commitment investment portfolio revenue growth competitive edge digital products customer service`
- Good: `profit before tax 150.2 billion low cost deposits fee income home remittances digital banking final cash dividend 11 rupees`

- Bad: `macroeconomic review inflation 2.4 percent current account surplus 682 million usd`
- Good: `inflation 2.4 percent current account surplus 682 million usd fiscal deficit 1.2 percent gdp kse100 index returns 86.5 percent`

Respond with JSON only.
"""


@dataclass
class SamplePage:
    relative_path: str
    page_text: str
    current_summary: str


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


def load_paths(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit(f"Expected JSON array in {path}")
    return [str(x) for x in data]


def load_sample_page(relative_path: str) -> SamplePage:
    md_path = MARKDOWN_ROOT / relative_path
    summary_path = CURRENT_SUMMARY_ROOT / relative_path
    if not md_path.exists():
        raise FileNotFoundError(f"Missing markdown page: {md_path}")

    page_text = md_path.read_text(encoding="utf-8", errors="ignore")
    current_summary = ""
    if summary_path.exists():
        current_summary = summary_path.read_text(encoding="utf-8", errors="ignore").strip()
    return SamplePage(relative_path=relative_path, page_text=page_text, current_summary=current_summary)


def generate_summary(client, page_text: str) -> dict:
    messages = [
        {"role": "system", "content": "You write retrieval-optimized PSX filing page search_text and return strict JSON."},
        {"role": "user", "content": f"{PROMPT}\n\nPage content:\n{truncate_text(page_text, MAX_CHARS)}"},
    ]

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=messages,
                temperature=TEMPERATURE,
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
            return json.loads(content)
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_WAIT * (attempt + 1))
    return {}


def normalize_search_text(value) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().lower().split())


def process_one(client, relative_path: str) -> dict:
    sample = load_sample_page(relative_path)
    result = generate_summary(client, sample.page_text)
    search_text = normalize_search_text(result.get("search_text"))
    return {
        "relative_path": relative_path,
        "current_summary": sample.current_summary,
        "search_text": search_text,
        "page_excerpt": truncate_text(sample.page_text.strip(), 900),
    }


def render_markdown(records: list[dict]) -> str:
    lines = []
    lines.append("# BM25 Search Text Sample Review")
    lines.append("")
    lines.append(f"Model: `{DEEPSEEK_MODEL}`")
    lines.append("")
    for record in records:
        lines.append(f"## {record['relative_path']}")
        lines.append("")
        lines.append("**Current summary**")
        lines.append("")
        lines.append(record["current_summary"] or "_None_")
        lines.append("")
        lines.append("**Proposed BM25 search_text**")
        lines.append("")
        lines.append(record["search_text"] or "_None_")
        lines.append("")
        lines.append("**Page excerpt**")
        lines.append("")
        lines.append("```markdown")
        lines.append(record["page_excerpt"])
        lines.append("```")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main():
    parser = argparse.ArgumentParser(description="Pilot BM25-optimized search_text generation")
    parser.add_argument("--paths", type=Path, default=SAMPLE_PATHS)
    parser.add_argument("--output-prompt", type=Path, default=OUTPUT_PROMPT)
    parser.add_argument("--output-jsonl", type=Path, default=OUTPUT_JSONL)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    pages = load_paths(args.paths)
    if not pages:
        raise SystemExit("No sample pages configured")

    client = setup_client()
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_one, client, rel): rel for rel in pages}
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: pages.index(item["relative_path"]))

    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    args.output_prompt.write_text(PROMPT.strip() + "\n", encoding="utf-8")
    with args.output_jsonl.open("w", encoding="utf-8") as handle:
        for record in results:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    args.output_md.write_text(render_markdown(results), encoding="utf-8")

    print(f"Wrote {args.output_prompt}")
    print(f"Wrote {args.output_jsonl}")
    print(f"Wrote {args.output_md}")


if __name__ == "__main__":
    main()
