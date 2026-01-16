#!/usr/bin/env python3
"""
Step 1: Build Stage 3 Manifests from V3 Extraction Manifest

Transforms the Stage 2 extraction manifest into the format Stage 3 scripts expect.

Input:  artifacts/stage2/step6_extraction_manifest.json
Output: section_manifest.json       - Statement pages by ticker/period
        multiyear_manifest.json     - Multi-year summary pages (for exclusion & optional extraction)
        compensation_manifest.json  - CEO compensation pages (for optional extraction)

The V3 extraction manifest has page classifications from Stage 2.
This script transforms that into the legacy format that Step2_ExtractStatements.py expects.
"""

import json
import re
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTION_MANIFEST = PROJECT_ROOT / "artifacts" / "stage2" / "step6_extraction_manifest.json"
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "stage3"


def load_extraction_manifest() -> dict:
    """Load the V3 extraction manifest from Stage 2."""
    if not EXTRACTION_MANIFEST.exists():
        raise FileNotFoundError(f"Extraction manifest not found: {EXTRACTION_MANIFEST}")
    with open(EXTRACTION_MANIFEST) as f:
        return json.load(f)


def parse_filing_key(filing_key: str) -> tuple:
    """Parse filing key to extract ticker, filing type, and period.

    Filing keys can be:
    - AABS_AABS_Annual_2021 -> (AABS, annual, 2021)
    - LUCK_LUCK_Quarterly_2024-09-30 -> (LUCK, quarterly, 2024-09-30)
    - ENGRO_Annual_2024 -> (ENGRO, annual, 2024)
    """
    # Try to extract ticker and period
    annual_match = re.match(r'^([A-Z]+)_(?:[A-Z]+_)?Annual_(\d{4})$', filing_key)
    if annual_match:
        return annual_match.group(1), 'annual', annual_match.group(2)

    quarterly_match = re.match(r'^([A-Z]+)_(?:[A-Z]+_)?Quarterly_(\d{4}-\d{2}-\d{2})$', filing_key)
    if quarterly_match:
        return quarterly_match.group(1), 'quarterly', quarterly_match.group(2)

    return None, None, None


def get_doc_from_path(filing_path: str) -> str:
    """Extract document folder name from filing path."""
    if not filing_path:
        return ""
    # filing_path is like "markdown_pages/AABS/2021/AABS_Annual_2021/"
    path = Path(filing_path.rstrip('/'))
    return path.name


def main():
    print("=" * 70)
    print("STEP 1: BUILD MANIFESTS FROM V3 EXTRACTION MANIFEST")
    print("=" * 70)
    print()

    print(f"Reading: {EXTRACTION_MANIFEST}")
    manifest = load_extraction_manifest()

    filings = manifest.get('filings', {})
    print(f"Found {len(filings)} filings in extraction manifest")

    # Build output structures
    section_manifest = defaultdict(lambda: {'annuals': {}, 'quarterlies': {}})
    multiyear_manifest = defaultdict(dict)
    compensation_manifest = defaultdict(dict)

    stats = {
        'annuals': 0,
        'quarterlies': 0,
        'with_statements': 0,
        'with_multiyear': 0,
        'with_compensation': 0,
    }

    for filing_key, filing_data in filings.items():
        ticker, filing_type, period = parse_filing_key(filing_key)

        if not ticker:
            print(f"  Warning: Could not parse filing key: {filing_key}")
            continue

        pages = filing_data.get('pages', {})
        filing_path = filing_data.get('filing_path', '')
        doc = get_doc_from_path(filing_path)

        # Get page lists
        statement_pages = sorted(set(pages.get('statement', [])))
        multiyear_pages = sorted(set(pages.get('multi_year', [])))
        ceo_comp_pages = sorted(set(pages.get('ceo_comp', [])))

        # Build section manifest entry
        entry = {
            'doc': doc,
            'statement_pages': statement_pages,
        }

        if filing_type == 'annual':
            section_manifest[ticker]['annuals'][period] = entry
            stats['annuals'] += 1

            # Multi-year and compensation are only in annuals
            if multiyear_pages:
                multiyear_manifest[ticker][period] = {
                    'doc': doc,
                    'pages': multiyear_pages,
                }
                stats['with_multiyear'] += 1

            if ceo_comp_pages:
                compensation_manifest[ticker][period] = {
                    'doc': doc,
                    'pages': ceo_comp_pages,
                }
                stats['with_compensation'] += 1
        else:
            section_manifest[ticker]['quarterlies'][period] = entry
            stats['quarterlies'] += 1

        if statement_pages:
            stats['with_statements'] += 1

    # Convert defaultdicts to regular dicts for JSON serialization
    section_manifest = {k: dict(v) for k, v in section_manifest.items()}
    multiyear_manifest = dict(multiyear_manifest)
    compensation_manifest = dict(compensation_manifest)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Write output files
    section_path = OUTPUT_DIR / 'step1_section_manifest.json'
    multiyear_path = OUTPUT_DIR / 'step1_multiyear_manifest.json'
    compensation_path = OUTPUT_DIR / 'step1_compensation_manifest.json'

    with open(section_path, 'w') as f:
        json.dump(section_manifest, f, indent=2)

    with open(multiyear_path, 'w') as f:
        json.dump(multiyear_manifest, f, indent=2)

    with open(compensation_path, 'w') as f:
        json.dump(compensation_manifest, f, indent=2)

    # Print summary
    print()
    print("=" * 70)
    print("MANIFESTS CREATED")
    print("=" * 70)
    print(f"  section_manifest.json:      {len(section_manifest)} tickers")
    print(f"  multiyear_manifest.json:    {len(multiyear_manifest)} tickers")
    print(f"  compensation_manifest.json: {len(compensation_manifest)} tickers")
    print()
    print("FILING COUNTS")
    print(f"  Annual reports:     {stats['annuals']}")
    print(f"  Quarterly reports:  {stats['quarterlies']}")
    print(f"  With statements:    {stats['with_statements']}")
    print(f"  With multi-year:    {stats['with_multiyear']}")
    print(f"  With compensation:  {stats['with_compensation']}")
    print()
    print(f"Output: {OUTPUT_DIR}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
