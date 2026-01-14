#!/usr/bin/env python3
"""
Compute the delta between two Cloudflare-friendly JSONL databases.

Given a "previous" compiled dataset and a freshly compiled "current" dataset,
the script writes a third directory that contains only the records absent from
the previous snapshot. Feed the resulting directory to Step14UploadCloudflareD1
to avoid re-uploading rows that already exist remotely.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from tqdm import tqdm

IDENTITY_COLUMNS: Sequence[str] = (
    "ticker",
    "filing_type",
    "filing_period",
    "filing_year",
    "pg",
    "embed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare two compiled Cloudflare JSONL directories and emit only new rows."
    )
    parser.add_argument("--previous", required=True, help="Path to the prior compiled JSONL directory (dated snapshot).")
    parser.add_argument(
        "--current",
        default="database_jsonl_cf",
        help="Path to the latest compiled JSONL directory (default: database_jsonl_cf).",
    )
    parser.add_argument(
        "--output",
        default="database_jsonl_cf_upload",
        help="Directory that will receive JSONLs containing only rows missing in the previous snapshot "
        "(default: database_jsonl_cf_upload).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow writing to an existing --output directory by clearing it first.",
    )
    return parser.parse_args()


def iter_jsonl_files(root: Path) -> Iterable[Path]:
    yield from sorted(root.rglob("*.jsonl"))


def record_identity(record: Dict) -> Tuple:
    return tuple(record.get(column) for column in IDENTITY_COLUMNS)


def load_known_keys(root: Path) -> Tuple[Set[Tuple], int]:
    if not root.exists():
        return set(), 0
    keys: Set[Tuple] = set()
    total = 0
    files = list(iter_jsonl_files(root))
    for file_path in tqdm(files, desc="Scanning previous snapshot", unit="files"):
        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            keys.add(record_identity(record))
            total += 1
    return keys, total


def ensure_output_root(root: Path, overwrite: bool) -> None:
    if root.exists():
        if not overwrite:
            raise SystemExit(f"Output directory {root} already exists. Use --overwrite to replace it.")
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")


def collect_new_rows(current_root: Path, known_keys: Set[Tuple], output_root: Path) -> Tuple[int, int]:
    files = list(iter_jsonl_files(current_root))
    if not files:
        return 0, 0
    records_written = 0
    files_written = 0
    keys = set(known_keys)

    for file_path in tqdm(files, desc="Building delta snapshot", unit="files"):
        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError:
            continue
        keep: List[str] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            identity = record_identity(record)
            if identity in keys:
                continue
            keys.add(identity)
            keep.append(line)
        if keep:
            relative = file_path.relative_to(current_root)
            write_jsonl(output_root / relative, keep)
            files_written += 1
            records_written += len(keep)
    return records_written, files_written


def main() -> None:
    args = parse_args()
    previous_root = Path(args.previous).expanduser()
    current_root = Path(args.current).expanduser()
    output_root = Path(args.output).expanduser()

    if not current_root.exists():
        raise SystemExit(f"Current directory not found: {current_root}")
    if not previous_root.exists():
        print(f"Previous directory {previous_root} missing; treating as empty snapshot.")

    ensure_output_root(output_root, overwrite=args.overwrite)

    known_keys, previous_count = load_known_keys(previous_root)
    print(f"Previous snapshot rows: {previous_count:,}")

    records_written, files_written = collect_new_rows(current_root, known_keys, output_root)
    print(
        f"Wrote {records_written:,} new rows across {files_written} files "
        f"into {output_root}. Feed this directory to Step14UploadCloudflareD1."
    )


if __name__ == "__main__":
    main()
