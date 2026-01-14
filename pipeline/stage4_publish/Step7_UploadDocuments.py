#!/usr/bin/env python3
"""
Generate SQL for financial_documents upload.

Reads: database_jsonl_compiled/**/*.jsonl
Outputs: financial_documents.sql

Then upload with:
    python3 pipeline/stage2_publish/upload_sql.py --file financial_documents.sql
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Sequence

from tqdm import tqdm


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "database_jsonl_compiled"
OUTPUT_FILE = PROJECT_ROOT / "financial_documents.sql"

COLUMNS: Sequence[str] = (
    "ticker",
    "industry",
    "filing_type",
    "filing_period",
    "filing_year",
    "section_tags",
    "pg",
    "jpg_path",
    "summary",
    "text",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate SQL for financial_documents upload.")
    parser.add_argument(
        "--input", default=str(INPUT_DIR), help="Root directory for compiled JSONLs."
    )
    parser.add_argument("--output", default=str(OUTPUT_FILE), help="Output SQL file.")
    parser.add_argument("--dry-run", action="store_true", help="Parse files but skip SQL generation.")
    return parser.parse_args()


def normalize_year(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    return value


def normalize_page(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    return value


def normalize_ticker(value: str | None, fallback: str = "") -> str:
    candidate = value or fallback or ""
    return candidate.upper()


def normalize_record(doc: Dict, ticker_hint: str) -> Dict:
    ticker = normalize_ticker(doc.get("ticker"), ticker_hint)
    filing_year = normalize_year(doc.get("filing_year"))
    pg = normalize_page(doc.get("pg") or doc.get("page_number"))

    return {
        "ticker": ticker,
        "industry": doc.get("industry"),
        "filing_type": doc.get("filing_type"),
        "filing_period": doc.get("filing_period"),
        "filing_year": filing_year,
        "section_tags": doc.get("section_tags"),
        "pg": pg,
        "jpg_path": doc.get("jpg_path"),
        "summary": doc.get("summary"),
        "text": doc.get("text"),
    }


def load_records(root: Path) -> List[Dict]:
    records: List[Dict] = []
    files = sorted(root.rglob("*.jsonl"))
    for file_path in tqdm(files, desc="Reading JSONLs"):
        relative = file_path.relative_to(root)
        parts = relative.parts
        ticker_guess = parts[0] if parts else ""
        with file_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record = normalize_record(doc, ticker_guess)
                records.append(record)
    return records


def escape_sql(value) -> str:
    """Escape value for SQL string literal."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    # Escape single quotes by doubling them
    s = str(value).replace("'", "''")
    return f"'{s}'"


def generate_insert(record: Dict) -> str:
    """Generate INSERT statement for a record."""
    values = []
    for col in COLUMNS:
        val = record.get(col)
        values.append(escape_sql(val))

    cols_sql = ", ".join(f'"{c}"' for c in COLUMNS)
    vals_sql = ", ".join(values)

    return f"INSERT INTO financial_documents ({cols_sql}) VALUES ({vals_sql});"


def main() -> None:
    args = parse_args()
    input_root = Path(args.input).expanduser()
    if not input_root.exists():
        raise SystemExit(f"Input directory not found: {input_root}")

    records = load_records(input_root)
    print(f"Total records collected: {len(records):,}")

    if args.dry_run or not records:
        print("Dry run complete; no SQL generated.")
        return

    output_path = Path(args.output)
    print(f"Writing SQL to: {output_path}")

    with open(output_path, "w") as f_out:
        f_out.write("-- financial_documents upload\n")
        f_out.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f_out.write(f"-- Records: {len(records):,}\n\n")

        for i, record in enumerate(records):
            sql = generate_insert(record)
            f_out.write(sql + "\n")

            if (i + 1) % 50000 == 0:
                print(f"  Processed {i + 1:,} records...")

    print(f"\nDone! Generated {len(records):,} INSERT statements")
    print(f"Output file: {output_path}")
    print(f"\nTo upload, run:")
    print(f"  python3 pipeline/stage2_publish/upload_sql.py --file {output_path}")


if __name__ == "__main__":
    main()
