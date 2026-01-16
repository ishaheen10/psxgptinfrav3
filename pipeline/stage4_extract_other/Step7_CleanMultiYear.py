#!/usr/bin/env python3
"""
Step 7: Clean Multi-Year Data

Post-process multi-year data to clean up issues and add data_type classification.

Issues addressed:
1. Remove rows where line_item is numeric only (unlabeled subtotal rows)
2. Add data_type field: 'monetary', 'ratio', 'per_share', 'count'
3. Normalize unit_type from section names where possible
4. Filter out Horizontal/Vertical Analysis percentage/index columns
5. Deduplicate by preferring most recent report_year for each data point

Input:  data/json_multiyear/multi_year_normalized.jsonl
Output: data/json_multiyear/multi_year_cleaned.jsonl
"""

import json
import re
from pathlib import Path
from collections import Counter, defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "json_multiyear" / "multi_year_normalized.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "json_multiyear" / "multi_year_cleaned.jsonl"

# Patterns for classifying data types
RATIO_PATTERNS = [
    r'ratio',
    r'percentage',
    r'\broa\b',  # Return on Assets
    r'\broe\b',  # Return on Equity
    r'return on',
    r'margin',
    r'yield',
    r'coverage',
    r'times',
    r'turnover',
    r'gearing',
    r'leverage',
    r'efficiency',
    r'utilization',
    r'%$',
]

PER_SHARE_PATTERNS = [
    r'per share',
    r'eps\b',
    r'earning.?per',
    r'dividend.?per',
    r'book value.?per',
    r'break.?up value',
    r'nav per',
    r'dps\b',
    r'bvps\b',
]

COUNT_PATTERNS = [
    r'number of',
    r'no\.? of',
    r'employees',
    r'branches',
    r'shares outstanding',
    r'issued shares',
    r'paid.?up shares',
]

# Sections that contain percentage/index values, not absolute amounts
ANALYSIS_SECTION_PATTERNS = [
    r'horizontal analysis',
    r'vertical analysis',
]

# Compile patterns for efficiency
RATIO_RE = re.compile('|'.join(RATIO_PATTERNS), re.IGNORECASE)
PER_SHARE_RE = re.compile('|'.join(PER_SHARE_PATTERNS), re.IGNORECASE)
COUNT_RE = re.compile('|'.join(COUNT_PATTERNS), re.IGNORECASE)
NUMERIC_ONLY_RE = re.compile(r'^[\d,\.\-\s\(\)]+$')
ANALYSIS_RE = re.compile('|'.join(ANALYSIS_SECTION_PATTERNS), re.IGNORECASE)


def classify_data_type(line_item: str, section: str) -> str:
    """Classify a line item into data type categories."""
    combined = f"{line_item} {section}".lower()

    if PER_SHARE_RE.search(combined):
        return 'per_share'
    elif RATIO_RE.search(combined):
        return 'ratio'
    elif COUNT_RE.search(combined):
        return 'count'
    else:
        return 'monetary'


def extract_unit_type(section: str) -> str | None:
    """Try to extract unit type from section name."""
    section_lower = section.lower()

    if any(x in section_lower for x in ['million', ' mn', '(mn)']):
        return 'millions'
    elif any(x in section_lower for x in ["'000", '000s', 'thousand', '(000)']):
        return '000s'
    elif 'rupees' in section_lower and 'million' not in section_lower and '000' not in section_lower:
        return 'Rs'

    return None


def is_numeric_line_item(line_item: str) -> bool:
    """Check if line item is purely numeric (problematic rows)."""
    if not line_item:
        return True
    # Remove common formatting and check if only numbers remain
    cleaned = line_item.strip()
    return bool(NUMERIC_ONLY_RE.match(cleaned))


def is_analysis_section(section: str) -> bool:
    """Check if section is a Horizontal/Vertical Analysis (contains indices, not amounts)."""
    return bool(ANALYSIS_RE.search(section))


def is_likely_index_value(value: float, line_item: str) -> bool:
    """Check if value looks like an index value (100 = base year)."""
    # Values exactly 100 or very close are likely base year indices
    if value is None:
        return False
    if value == 100.0:
        return True
    # Small values (< 500) for typically large items are suspicious
    if value < 500 and any(x in line_item.lower() for x in
                           ['total assets', 'total liabilities', 'revenue', 'sales',
                            'deposits', 'equity', 'capital']):
        return True
    return False


def clean_line_item(line_item: str) -> str:
    """Clean up formatting artifacts from line items."""
    if not line_item:
        return line_item

    # Remove markdown bold
    cleaned = line_item.replace('**', '')

    # Remove footnote markers like ¹) ²) etc.
    cleaned = re.sub(r'[¹²³⁴⁵⁶⁷⁸⁹⁰]+\)', '', cleaned)
    cleaned = re.sub(r'\s*\d+\)$', '', cleaned)  # Remove trailing "1)" etc.

    # Clean up extra whitespace
    cleaned = ' '.join(cleaned.split())

    return cleaned.strip()


# Low-confidence patterns to exclude
LOW_CONFIDENCE_PATTERNS = [
    # Operational metrics with units in line item
    r'\(MMCFT?\)',      # Gas volume
    r'\(MMBTU\)',       # Energy units
    r'\(Each\)',        # Count units
    r'\(Tons?\)',       # Weight
    r'\(KM\)',          # Distance
    r'\(Nos\.?\)',      # Numbers
    r'\(Rs\.?\s*Mn\)',  # Rupees in millions embedded in line item
    r'\(Rs\.?\s*000\)', # Rupees in thousands embedded
    r'\(Line km\)',     # Seismic survey
    r'\(Hectares?\)',   # Area
    r'\(MW\)',          # Power
    r'\(GWH\)',         # Energy
    r'\(000 Barrels\)', # Oil volume
    r"'000'\s*$",       # Trailing unit marker
]

# Operational line items that are not standard financial metrics
OPERATIONAL_LINE_ITEMS = [
    r'^gas sold',
    r'^gas purchased',
    r'^gas meters',
    r'^new connections',
    r'^lpg air mix',
    r'^mains.*transmission',
    r'^mains.*distribution',
    r'^seismic survey',
    r'^wells drilled',
    r'^production.*barrels',
    r'^cement.*dispatches',
    r'^clinker',
    r'^capacity utilization',
    r'^plant capacity',
]

LOW_CONFIDENCE_RE = re.compile('|'.join(LOW_CONFIDENCE_PATTERNS), re.IGNORECASE)
OPERATIONAL_RE = re.compile('|'.join(OPERATIONAL_LINE_ITEMS), re.IGNORECASE)

# Tickers with systematic extraction issues (low match rates)
EXCLUDE_TICKERS = {'SSGC', 'INDU'}


def is_low_confidence(line_item: str, section: str) -> bool:
    """Check if line item is low-confidence (operational metrics with embedded units)."""
    if not line_item:
        return False

    # Check for unit patterns embedded in line item
    if LOW_CONFIDENCE_RE.search(line_item):
        return True

    # Check for operational line items
    if OPERATIONAL_RE.search(line_item):
        return True

    return False


def main():
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found")
        return

    stats = Counter()
    records_by_key = defaultdict(list)  # For deduplication

    print(f"Reading {INPUT_FILE}...")
    with open(INPUT_FILE) as f:
        for line in f:
            record = json.loads(line)
            stats['total'] += 1

            line_item = record.get('line_item', '')
            section = record.get('section', '')
            value = record.get('value')
            ticker = record.get('ticker', '')

            # Filter out tickers with systematic issues
            if ticker in EXCLUDE_TICKERS:
                stats['filtered_ticker'] += 1
                continue

            # Filter out numeric-only line items
            if is_numeric_line_item(line_item):
                stats['filtered_numeric'] += 1
                continue

            # Filter out Horizontal/Vertical Analysis sections (contain indices)
            if is_analysis_section(section):
                stats['filtered_analysis'] += 1
                continue

            # Filter out likely index values (100 = base year)
            if is_likely_index_value(value, line_item):
                stats['filtered_index'] += 1
                continue

            # Filter out low-confidence line items (operational metrics with embedded units)
            if is_low_confidence(line_item, section):
                stats['filtered_low_conf'] += 1
                continue

            # Clean up line item formatting artifacts
            original_line_item = line_item
            line_item = clean_line_item(line_item)
            record['line_item'] = line_item
            if line_item != original_line_item:
                stats['cleaned_formatting'] += 1

            # Add data_type classification
            data_type = classify_data_type(line_item, section)
            record['data_type'] = data_type
            stats[f'type_{data_type}'] += 1

            # Try to extract unit_type from section if not present
            if 'unit_type' not in record or not record.get('unit_type'):
                unit_type = extract_unit_type(section)
                if unit_type:
                    record['unit_type'] = unit_type
                    stats['unit_extracted'] += 1

            # Key for deduplication: ticker + data_year + normalized line_item
            # Prefer the most recent report_year for each data point
            ticker = record.get('ticker', '')
            data_year = record.get('data_year', 0)
            line_item_norm = line_item.lower().strip()
            key = (ticker, data_year, line_item_norm, data_type)

            records_by_key[key].append(record)
            stats['pre_dedup'] += 1

    # Deduplicate: keep record from most recent report_year
    final_records = []
    for key, recs in records_by_key.items():
        # Sort by report_year descending, take first
        recs.sort(key=lambda r: r.get('report_year', 0), reverse=True)
        final_records.append(recs[0])
        if len(recs) > 1:
            stats['deduplicated'] += len(recs) - 1

    stats['kept'] = len(final_records)

    print(f"\nWriting {OUTPUT_FILE}...")
    # Sort output for consistency
    final_records.sort(key=lambda r: (r.get('ticker', ''), r.get('data_year', 0), r.get('line_item', '')))
    with open(OUTPUT_FILE, 'w') as f:
        for record in final_records:
            f.write(json.dumps(record) + '\n')

    # Print statistics
    print("\n=== Processing Statistics ===")
    print(f"Total input records:    {stats['total']:,}")
    print(f"Filtered (bad ticker):  {stats['filtered_ticker']:,}")
    print(f"Filtered (numeric):     {stats['filtered_numeric']:,}")
    print(f"Filtered (analysis):    {stats['filtered_analysis']:,}")
    print(f"Filtered (index vals):  {stats['filtered_index']:,}")
    print(f"Filtered (low conf):    {stats['filtered_low_conf']:,}")
    print(f"Records after filter:   {stats['pre_dedup']:,}")
    print(f"Cleaned formatting:     {stats['cleaned_formatting']:,}")
    print(f"Deduplicated:           {stats['deduplicated']:,}")
    print(f"Final records:          {stats['kept']:,}")
    print(f"\n=== Data Type Distribution ===")
    print(f"Monetary:               {stats['type_monetary']:,}")
    print(f"Ratio:                  {stats['type_ratio']:,}")
    print(f"Per Share:              {stats['type_per_share']:,}")
    print(f"Count:                  {stats['type_count']:,}")
    print(f"\nUnit types extracted:   {stats['unit_extracted']:,}")
    print(f"\nOutput: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
