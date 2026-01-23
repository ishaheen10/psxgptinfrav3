#!/usr/bin/env python3
"""
Step 5: QC Multi-Year Extraction

Validates extracted multi-year data against source pages.
Uses manifest to get the actual source pages, then checks if
extracted values appear in those pages.

Input:  data/json_multiyear/multi_year_normalized.jsonl
        artifacts/stage3/step1_multiyear_manifest.json
        markdown_pages/
Output: artifacts/stage4/step5_multiyear_qc.json
"""

import json
import re
from pathlib import Path
from collections import Counter, defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "json_multiyear" / "multi_year_normalized.jsonl"
MANIFEST_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step1_multiyear_manifest.json"
MARKDOWN_PAGES = PROJECT_ROOT / "markdown_pages"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage4" / "step5_multiyear_qc.json"

# Sample size for QC (full check would be slow)
SAMPLE_SIZE = 500


def load_manifest() -> dict:
    """Load multi-year manifest."""
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    return {}


def load_source_pages(ticker: str, year: str, pages: list) -> str:
    """Load and concatenate text from manifest pages."""
    combined = []
    for pg in pages:
        md_path = MARKDOWN_PAGES / ticker / year / f"{ticker}_Annual_{year}" / f"page_{pg:03d}.md"
        if md_path.exists():
            combined.append(md_path.read_text())
    return "\n".join(combined)


def value_in_text(value: float, text: str) -> bool:
    """Check if a value appears in text in any common format."""
    if value is None:
        return False

    # Generate patterns to search for
    patterns = []
    abs_val = abs(value)

    # Integer version
    if abs_val == int(abs_val):
        int_val = int(abs_val)
        patterns.append(str(int_val))
        patterns.append(f"{int_val:,}")
        # Also check scaled versions (thousands, millions)
        if int_val >= 1000:
            patterns.append(str(int_val // 1000))
            patterns.append(f"{int_val // 1000:,}")
        if int_val >= 1000000:
            patterns.append(str(int_val // 1000000))
    else:
        # Decimal - common for ratios
        patterns.append(f"{abs_val:.1f}")
        patterns.append(f"{abs_val:.2f}")
        patterns.append(str(abs_val))
        # Percentage format
        patterns.append(f"{abs_val}%")
        patterns.append(f"{abs_val:.1f}%")

    # Check each pattern
    for p in patterns:
        if p in text:
            return True

    return False


def main():
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found. Run Step6_JSONifyMultiYear first.")
        return

    manifest = load_manifest()
    if not manifest:
        print(f"Warning: No manifest found at {MANIFEST_FILE}")

    # Load records
    records = []
    with open(INPUT_FILE) as f:
        for line in f:
            records.append(json.loads(line))

    print(f"Loaded {len(records):,} multi-year records")

    # Sample for QC
    import random
    random.seed(42)
    if len(records) > SAMPLE_SIZE:
        sample = random.sample(records, SAMPLE_SIZE)
    else:
        sample = records

    print(f"QC checking {len(sample)} sampled records...\n")

    # Track results
    stats = Counter()
    by_ticker = defaultdict(lambda: Counter())
    by_section = defaultdict(lambda: Counter())
    mismatches = []

    # Cache for loaded pages
    page_cache = {}

    for i, record in enumerate(sample):
        if (i + 1) % 100 == 0:
            print(f"  Checked {i + 1}/{len(sample)}...")

        ticker = record['ticker']
        year = str(record['report_year'])
        value = record['value']
        line_item = record['line_item']
        section = record.get('section', 'unknown')

        stats['total'] += 1

        # Get manifest pages
        pages = manifest.get(ticker, {}).get(year, {}).get('pages', [])
        if not pages:
            stats['no_manifest'] += 1
            by_ticker[ticker]['no_manifest'] += 1
            continue

        # Load source text (cached)
        cache_key = (ticker, year)
        if cache_key not in page_cache:
            page_cache[cache_key] = load_source_pages(ticker, year, pages)
        source_text = page_cache[cache_key]

        if not source_text:
            stats['no_source'] += 1
            by_ticker[ticker]['no_source'] += 1
            continue

        # Check if value appears in source
        if value_in_text(value, source_text):
            stats['value_found'] += 1
            by_ticker[ticker]['value_found'] += 1
            by_section[section]['value_found'] += 1
        else:
            stats['value_missing'] += 1
            by_ticker[ticker]['value_missing'] += 1
            by_section[section]['value_missing'] += 1
            if len(mismatches) < 20:
                mismatches.append({
                    'ticker': ticker,
                    'year': year,
                    'line_item': line_item,
                    'value': value,
                    'section': section,
                    'pages': pages,
                })

    # Calculate match rate
    checked = stats['total'] - stats['no_manifest'] - stats['no_source']
    match_rate = stats['value_found'] / checked * 100 if checked > 0 else 0

    # Print results
    print("\n" + "=" * 60)
    print("MULTI-YEAR QC SUMMARY")
    print("=" * 60)
    print(f"  Sample size:        {stats['total']:,}")
    print(f"  No manifest entry:  {stats['no_manifest']:,}")
    print(f"  No source pages:    {stats['no_source']:,}")
    print(f"  Checked:            {checked:,}")
    print(f"  Value found:        {stats['value_found']:,} ({match_rate:.1f}%)")
    print(f"  Value missing:      {stats['value_missing']:,}")

    # By section
    print(f"\nBy section (sample):")
    for section, counts in sorted(by_section.items(), key=lambda x: sum(x[1].values()), reverse=True)[:10]:
        total = counts.get('value_found', 0) + counts.get('value_missing', 0)
        found = counts.get('value_found', 0)
        rate = found / total * 100 if total > 0 else 0
        print(f"  {section[:40]:40s}: {rate:.0f}% ({found}/{total})")

    # Problem tickers
    problem_tickers = [t for t, c in by_ticker.items()
                       if c.get('value_missing', 0) > c.get('value_found', 0)]
    if problem_tickers:
        print(f"\nTickers with >50% missing (sample):")
        for t in sorted(problem_tickers)[:10]:
            c = by_ticker[t]
            print(f"  {t}: found={c.get('value_found', 0)}, missing={c.get('value_missing', 0)}")

    # Sample mismatches
    if mismatches:
        print(f"\nSample mismatches:")
        for m in mismatches[:5]:
            print(f"  {m['ticker']} {m['year']}: {m['line_item'][:25]} = {m['value']}")

    # Conclusion
    print(f"\n" + "=" * 60)
    if match_rate >= 80:
        print("RESULT: PASS - Value match rate >= 80%")
    elif match_rate >= 60:
        print("RESULT: WARN - Value match rate 60-80%")
    else:
        print("RESULT: FAIL - Value match rate < 60%")
    print("=" * 60)

    # Save results
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output = {
        'sample_size': stats['total'],
        'checked': checked,
        'value_found': stats['value_found'],
        'value_missing': stats['value_missing'],
        'match_rate': match_rate,
        'status': 'pass' if match_rate >= 80 else ('warn' if match_rate >= 60 else 'fail'),
        'problem_tickers': problem_tickers[:20],
        'sample_mismatches': mismatches,
    }
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
