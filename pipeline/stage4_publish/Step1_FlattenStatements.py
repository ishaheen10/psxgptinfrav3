#!/usr/bin/env python3
"""
Step 6: Flatten Financial Statements

Structural transformation only - no normalization or mapping.

This step:
1. Reads nested JSON from statements_final/ (or custom input dir)
2. Flattens to one row per line item per period
3. Preserves original line item text exactly as extracted
4. Links source PDF pages for auditability

Output:
- financial_statements_flat.jsonl (all line items, original text)
"""

import argparse
import json
import re
from pathlib import Path
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load ticker metadata
with open(PROJECT_ROOT / 'tickers_new.json') as f:
    tickers_data = json.load(f)
TICKER_TO_INDUSTRY = {t['Symbol']: t['Industry'] for t in tickers_data}
TICKER_TO_COMPANY = {t['Symbol']: t['Company Name'] for t in tickers_data}

# Default paths (can be overridden via args)
DEFAULT_INPUT_DIR = PROJECT_ROOT / 'statements_final'
PAGES_DIR = PROJECT_ROOT / 'database_jsonl_compiled'
DEFAULT_OUTPUT_FILE = PROJECT_ROOT / 'financial_statements_flat.jsonl'

# Cache for statement pages
PAGES_CACHE = {}

NUMERIC_ONLY_RE = re.compile(r'^[\d,\.\-\s\(\)]+$')


def is_numeric_line_item(line_item: str) -> bool:
    """Check if line item is purely numeric (problematic rows)."""
    if not line_item:
        return True
    cleaned = line_item.strip()
    return bool(NUMERIC_ONLY_RE.match(cleaned))


def clean_line_item_text(item: str) -> str:
    """Clean formatting artifacts from line item text."""
    if not item:
        return item

    item = item.replace('&nbsp;', ' ')
    item = item.replace('**', '')
    item = re.sub(r'[¹²³⁴⁵⁶⁷⁸⁹⁰]+\)', '', item)
    item = re.sub(r'\s*\d+\)$', '', item)
    item = ' '.join(item.split())

    return item.strip()


# =============================================================================
# SOURCE PAGE LINKING
# =============================================================================

def load_statement_pages(ticker: str, filing_type: str, period: str) -> dict:
    """Load pages tagged as 'statement' for a given filing."""
    if '-' in period:
        year = period.split('-')[0]
    else:
        year = period

    filing_type_cap = 'Annual' if filing_type == 'annual' else 'Quarterly'
    jsonl_path = PAGES_DIR / ticker / year / f"{ticker}_{filing_type_cap}_{period}.jsonl"

    if not jsonl_path.exists():
        return {}

    statement_pages = {}
    with open(jsonl_path) as f:
        for line in f:
            page = json.loads(line)
            tags = page.get('section_tags', {})
            if 'statement' in tags or 'statement_note' in tags:
                statement_pages[page['pg']] = {
                    'text': page['text'],
                    'jpg_path': page.get('jpg_path', '')
                }

    return statement_pages


def format_value_patterns(value: float) -> list:
    """Generate possible string representations of a numeric value."""
    if value is None:
        return []

    patterns = set()
    abs_val = abs(value)
    is_negative = value < 0

    def add_with_commas(v):
        if v == int(v):
            int_v = int(v)
            patterns.add(f"{int_v:,}")
            patterns.add(str(int_v))
        else:
            patterns.add(f"{v:,.3f}")
            patterns.add(f"{v:,.2f}")
            patterns.add(f"{v:.3f}")
            patterns.add(f"{v:.2f}")
            patterns.add(str(v))

    add_with_commas(abs_val)

    if abs_val == int(abs_val):
        int_val = int(abs_val)
        if is_negative:
            patterns.add(f"({int_val:,})")
            patterns.add(f"({int_val})")
            patterns.add(f"-{int_val:,}")
            patterns.add(f"-{int_val}")
        if int_val >= 1000:
            patterns.add(f"{int_val // 1000:,}")
        if int_val >= 1000000:
            patterns.add(f"{int_val // 1000000:,}")

    return list(patterns)


def find_source_page(line_item: str, value: float, statement_pages: dict, stmt_type: str = None) -> dict | None:
    """Find the page containing line item and/or value."""
    if not statement_pages:
        return None

    value_patterns = format_value_patterns(value)
    stmt_keywords = {
        'profit_loss': ['profit', 'loss', 'income', 'p&l'],
        'balance_sheet': ['financial position', 'balance sheet', 'assets', 'liabilities'],
        'cash_flow': ['cash flow', 'cash flows']
    }

    item_pages = []
    value_pages = []
    stmt_type_pages = []

    for pg, page_info in statement_pages.items():
        text = page_info['text']
        text_lower = text.lower()

        if line_item and line_item in text:
            item_pages.append((pg, page_info))

        if value_patterns:
            for pattern in value_patterns:
                if pattern in text:
                    value_pages.append((pg, page_info))
                    break

        if stmt_type and stmt_type in stmt_keywords:
            for kw in stmt_keywords[stmt_type]:
                if kw in text_lower:
                    stmt_type_pages.append((pg, page_info))
                    break

    if item_pages and value_patterns:
        for pg, page_info in item_pages:
            for pattern in value_patterns:
                if pattern in page_info['text']:
                    return {'pg': pg, 'jpg_path': page_info['jpg_path'], 'match_type': 'item_and_value'}

    if item_pages:
        pg, page_info = item_pages[0]
        return {'pg': pg, 'jpg_path': page_info['jpg_path'], 'match_type': 'item_only'}

    if value_pages:
        pg, page_info = value_pages[0]
        return {'pg': pg, 'jpg_path': page_info['jpg_path'], 'match_type': 'value_only'}

    if stmt_type_pages:
        pg, page_info = stmt_type_pages[0]
        return {'pg': pg, 'jpg_path': page_info['jpg_path'], 'match_type': 'statement_type'}

    if statement_pages:
        first_item = list(statement_pages.items())[0]
        pg, page_info = first_item
        return {'pg': pg, 'jpg_path': page_info['jpg_path'], 'match_type': 'any_statement'}

    return None


def parse_period_key(period_key: str) -> dict:
    """Parse period key like 'quarterly_2025-09-30' or 'annual_2024'."""
    if period_key.startswith('quarterly_'):
        return {'period_type': 'quarterly', 'period_end': period_key.replace('quarterly_', '')}
    elif period_key.startswith('annual_'):
        return {'period_type': 'annual', 'period_end': period_key.replace('annual_', '')}
    return {'period_type': 'unknown', 'period_end': period_key}


def get_ltm_source_pdfs(ticker: str, source_periods: dict, line_item: str, value: float, stmt_type: str) -> list:
    """Get source PDF pages for LTM rows."""
    source_pdfs = []

    for period_key_name in ['annual_period_key', 'quarterly_period_key']:
        period_key = source_periods.get(period_key_name)
        if period_key:
            period_info = parse_period_key(period_key)
            cache_key = (ticker, period_info['period_type'], period_info['period_end'])
            if cache_key not in PAGES_CACHE:
                PAGES_CACHE[cache_key] = load_statement_pages(ticker, period_info['period_type'], period_info['period_end'])
            pages = PAGES_CACHE[cache_key]
            if pages:
                result = find_source_page(line_item, value, pages, stmt_type)
                if result and result.get('jpg_path'):
                    pdf_path = result['jpg_path'].replace('.jpg', '.pdf')
                    if pdf_path not in source_pdfs:
                        source_pdfs.append(pdf_path)

    return source_pdfs


def get_source_info(row: dict) -> tuple:
    """Get source PDF path and match type for a row."""
    ticker = row['ticker']
    period_type = row['period_type']
    period_end = row['period_end']

    if period_type == 'ltm':
        return None, None

    cache_key = (ticker, period_type, period_end)
    if cache_key not in PAGES_CACHE:
        PAGES_CACHE[cache_key] = load_statement_pages(ticker, period_type, period_end)

    statement_pages = PAGES_CACHE[cache_key]
    if not statement_pages:
        return None, None

    line_item = row['line_item']
    result = find_source_page(line_item, row.get('value'), statement_pages, row.get('statement_type'))
    if result:
        pdf_path = result['jpg_path'].replace('.jpg', '.pdf') if result['jpg_path'] else None
        return pdf_path, result['match_type']

    return None, None


# =============================================================================
# PARSING UTILITIES
# =============================================================================

def parse_value(value_str: str) -> float | None:
    """Parse a formatted number string to float."""
    if value_str is None or value_str == '-' or value_str == '':
        return None
    value_str = str(value_str).replace(',', '')
    if value_str.startswith('(') and value_str.endswith(')'):
        value_str = '-' + value_str[1:-1]
    try:
        return float(value_str)
    except ValueError:
        return None


MONTH_TO_NUM = {
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'june': '06', 'july': '07', 'august': '08', 'september': '09',
    'october': '10', 'november': '11', 'december': '12'
}


def parse_column_header(col: str, period_key: str = None) -> dict | None:
    """Parse column header like '9M Sep 2025' or 'Sep 30, 2025'.

    Also handles simpler formats from standardized_statements_gemini:
    - "2024" or "2023 (Restated)" for annual
    - "June 30, 2025" for quarterly
    """
    # Match: "9M Sep 2025" or "12M Dec 2024"
    match = re.match(r'(\d+)M\s+([A-Za-z]+)\s+(\d{4})', col)
    if match:
        month = match.group(2).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        year = match.group(3)
        # For flow statements, use end of month (approximate with common dates)
        day = '30' if month_num in ('04', '06', '09', '11') else ('28' if month_num == '02' else '31')
        return {
            'duration': f'{match.group(1)}M',
            'month': match.group(2),
            'year': int(year),
            'period_end_derived': f'{year}-{month_num}-{day}'
        }

    # Match: "Sep 30, 2025" or "Dec 31, 2024" (balance sheet point-in-time)
    match = re.match(r'([A-Za-z]+)\s+(\d+),\s+(\d{4})', col)
    if match:
        month = match.group(1).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        day = match.group(2).zfill(2)
        year = match.group(3)
        # Derive duration from month for quarterly filings
        month_int = int(month_num)
        if month_int == 3:
            duration = '3M'
        elif month_int == 6:
            duration = '6M'
        elif month_int == 9:
            duration = '9M'
        else:
            duration = '12M'  # Dec or annual
        return {
            'duration': duration,
            'month': match.group(1),
            'year': int(year),
            'period_end_derived': f'{year}-{month_num}-{day}'
        }

    # Match: "LTM Sep 2025"
    match = re.match(r'LTM\s+([A-Za-z]+)\s+(\d{4})', col)
    if match:
        month = match.group(1).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        year = match.group(2)
        day = '30' if month_num in ('04', '06', '09', '11') else ('28' if month_num == '02' else '31')
        return {
            'duration': 'LTM',
            'month': match.group(1),
            'year': int(year),
            'period_end_derived': f'{year}-{month_num}-{day}'
        }

    # Match simple year: "2024" or "2023 (Restated)" for annual
    match = re.match(r'^(\d{4})(?:\s*\(.*\))?$', col.strip())
    if match:
        year = match.group(1)
        return {
            'duration': '12M',
            'month': 'Dec',
            'year': int(year),
            'period_end_derived': f'{year}-12-31'
        }

    # Match "Month Day, Year" without comma: "June 30 2025"
    match = re.match(r'([A-Za-z]+)\s+(\d+)\s+(\d{4})', col)
    if match:
        month = match.group(1).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        day = match.group(2).zfill(2)
        year = match.group(3)
        month_int = int(month_num)
        if month_int == 3:
            duration = '3M'
        elif month_int == 6:
            duration = '6M'
        elif month_int == 9:
            duration = '9M'
        else:
            duration = '12M'
        return {
            'duration': duration,
            'month': match.group(1),
            'year': int(year),
            'period_end_derived': f'{year}-{month_num}-{day}'
        }

    # Match "December 31, 2024" (full month name with comma)
    match = re.match(r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})', col)
    if match:
        month = match.group(1).lower()[:3]
        month_num = MONTH_TO_NUM.get(month, '12')
        day = match.group(2).zfill(2)
        year = match.group(3)
        month_int = int(month_num)
        if month_int == 3:
            duration = '3M'
        elif month_int == 6:
            duration = '6M'
        elif month_int == 9:
            duration = '9M'
        else:
            duration = '12M'
        return {
            'duration': duration,
            'month': match.group(1)[:3].capitalize(),
            'year': int(year),
            'period_end_derived': f'{year}-{month_num}-{day}'
        }

    return None


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def flatten_ticker(ticker: str, data: dict) -> list[dict]:
    """Flatten all data for a ticker into rows."""
    rows = []
    industry = TICKER_TO_INDUSTRY.get(ticker, 'Unknown')
    company_name = TICKER_TO_COMPANY.get(ticker, ticker)
    unit_type = data.get('unit_type', '000s')

    for period_key, period_data in data.get('periods', {}).items():
        period_info = parse_period_key(period_key)

        # Check for consolidated/unconsolidated OR direct 'statements' key
        sections_to_process = []
        for section in ['consolidated', 'unconsolidated']:
            if section in period_data:
                sections_to_process.append((section, period_data[section]))

        # Fallback: some files have 'statements' directly instead of consolidated/unconsolidated
        if not sections_to_process and 'statements' in period_data:
            sections_to_process.append(('standalone', period_data['statements']))

        for section, section_data in sections_to_process:

            for stmt_type in ['profit_loss', 'balance_sheet', 'cash_flow']:
                stmt = section_data.get(stmt_type, {})
                columns = stmt.get('columns', [])

                for row in stmt.get('rows', []):
                    line_item_raw = row.get('Line Item', '')
                    if not line_item_raw or line_item_raw == '-':
                        continue
                    if is_numeric_line_item(line_item_raw):
                        continue

                    line_item = clean_line_item_text(line_item_raw)
                    if not line_item:
                        continue

                    # Get balance sheet section (assets/liabilities/equity)
                    bs_section = row.get('bs_section') if stmt_type == 'balance_sheet' else None
                    # Get canonical field mapping from Step 4/5
                    canonical_field = row.get('canonical_field')

                    for col in columns:
                        if col == 'Line Item':
                            continue

                        col_info = parse_column_header(col)
                        if not col_info:
                            continue

                        value = parse_value(row.get(col))
                        if value is None:
                            continue

                        # Use column-derived period_end (from the column header date)
                        # instead of filename-derived period_end
                        period_end = col_info.get('period_end_derived', period_info['period_end'])

                        db_row = {
                            'ticker': ticker,
                            'company_name': company_name,
                            'industry': industry,
                            'unit_type': unit_type,
                            'period_type': period_info['period_type'],
                            'period_end': period_end,
                            'period_duration': col_info['duration'],
                            'period_month': col_info['month'],
                            'period_year': col_info['year'],
                            'section': section,
                            'statement_type': stmt_type,
                            'bs_section': bs_section,
                            'canonical_field': canonical_field,
                            'line_item': line_item,
                            'value': value
                        }
                        rows.append(db_row)

                # Process calculated 3M if present
                calc_3m = section_data.get('calculated_3m', {}).get(stmt_type, {})
                for calc_col, calc_stmt in calc_3m.items():
                    col_info = parse_column_header(calc_col)
                    if not col_info:
                        continue

                    # Use column-derived period_end for calculated periods too
                    period_end = col_info.get('period_end_derived', period_info['period_end'])

                    for calc_row in calc_stmt.get('rows', []):
                        line_item_raw = calc_row.get('Line Item', '')
                        if not line_item_raw or line_item_raw == '-':
                            continue
                        if is_numeric_line_item(line_item_raw):
                            continue

                        line_item = clean_line_item_text(line_item_raw)
                        if not line_item:
                            continue

                        bs_section = calc_row.get('bs_section') if stmt_type == 'balance_sheet' else None
                        canonical_field = calc_row.get('canonical_field')

                        value = parse_value(calc_row.get(calc_col))
                        if value is None:
                            continue

                        db_row = {
                            'ticker': ticker,
                            'company_name': company_name,
                            'industry': industry,
                            'unit_type': unit_type,
                            'period_type': period_info['period_type'],
                            'period_end': period_end,
                            'period_duration': col_info['duration'],
                            'period_month': col_info['month'],
                            'period_year': col_info['year'],
                            'section': section,
                            'statement_type': stmt_type,
                            'bs_section': bs_section,
                            'canonical_field': canonical_field,
                            'line_item': line_item,
                            'value': value
                        }
                        rows.append(db_row)

    # Process LTM data
    ltm_info = data.get('ltm_info', {})
    for section_key, section_data in ltm_info.items():
        section = section_key.split('_')[0] if '_' in section_key else section_key

        for stmt_type, stmt_list in section_data.get('statements', {}).items():
            if not isinstance(stmt_list, list):
                stmt_list = [stmt_list]

            for stmt in stmt_list:
                ltm_col = stmt.get('ltm_col', '')
                col_info = parse_column_header(ltm_col)
                if not col_info:
                    continue

                source_periods = stmt.get('source_periods', {})

                for row in stmt.get('rows', []):
                    line_item_raw = row.get('Line Item', '')
                    if not line_item_raw or line_item_raw == '-':
                        continue
                    if is_numeric_line_item(line_item_raw):
                        continue

                    line_item = clean_line_item_text(line_item_raw)
                    if not line_item:
                        continue

                    bs_section = row.get('bs_section') if stmt_type == 'balance_sheet' else None
                    canonical_field = row.get('canonical_field')

                    value = parse_value(row.get(ltm_col))
                    if value is None:
                        continue

                    db_row = {
                        'ticker': ticker,
                        'company_name': company_name,
                        'industry': industry,
                        'unit_type': unit_type,
                        'period_type': 'ltm',
                        'period_end': f"{col_info['year']}-{col_info['month']}",
                        'period_duration': 'LTM',
                        'period_month': col_info['month'],
                        'period_year': col_info['year'],
                        'section': section,
                        'statement_type': stmt_type,
                        'bs_section': bs_section,
                        'canonical_field': canonical_field,
                        'line_item': line_item,
                        'value': value,
                        '_source_periods': source_periods
                    }
                    rows.append(db_row)

    return rows


def main():
    parser = argparse.ArgumentParser(description="Flatten financial statements for database upload")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR, help="Input directory with JSON files")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_FILE, help="Output JSONL file")
    parser.add_argument("--skip-source-linking", action="store_true", help="Skip source page linking (faster)")
    args = parser.parse_args()

    INPUT_DIR = args.input_dir
    OUTPUT_FILE = args.output

    all_rows = []

    files = list(INPUT_DIR.glob('*.json'))
    print(f"Step 7: Flattening {len(files)} ticker files from {INPUT_DIR}...")

    for f in files:
        ticker = f.stem
        with open(f) as fp:
            data = json.load(fp)
        rows = flatten_ticker(ticker, data)
        all_rows.extend(rows)

    print(f"Total rows: {len(all_rows):,}")

    # Add source page info
    print(f"\nLinking source pages...")
    source_stats = Counter()

    for i, row in enumerate(all_rows):
        if row['period_type'] == 'ltm':
            source_periods = row.pop('_source_periods', {})
            if source_periods:
                source_pdfs = get_ltm_source_pdfs(
                    row['ticker'], source_periods, row['line_item'],
                    row.get('value'), row.get('statement_type')
                )
                row['source_pdfs'] = source_pdfs if source_pdfs else None
                row['source_pdf'] = None
                row['source_match'] = 'ltm_multi_source' if source_pdfs else None
                source_stats['ltm_linked' if source_pdfs else 'ltm_not_found'] += 1
            else:
                row['source_pdfs'] = None
                row['source_pdf'] = None
                row['source_match'] = None
                source_stats['ltm_no_periods'] += 1
        else:
            source_pdf, source_match = get_source_info(row)
            row['source_pdf'] = source_pdf
            row['source_pdfs'] = None
            row['source_match'] = source_match
            row.pop('_source_periods', None)
            source_stats[source_match or 'not_found'] += 1

        if (i + 1) % 25000 == 0:
            print(f"  Processed {i+1:,} rows...")

    # Write output
    print(f"\nWriting output...")
    with open(OUTPUT_FILE, 'w') as out:
        for row in all_rows:
            out.write(json.dumps(row) + '\n')

    # Print stats
    stmt_counts = Counter(r['statement_type'] for r in all_rows)
    period_counts = Counter(r['period_type'] for r in all_rows)
    unique_line_items = len(set(r['line_item'] for r in all_rows))

    print(f"\n{'='*60}")
    print("FLATTEN COMPLETE")
    print(f"{'='*60}")
    print(f"Total rows: {len(all_rows):,}")
    print(f"Unique line items: {unique_line_items:,}")
    print(f"Output: {OUTPUT_FILE}")

    print(f"\nBy statement type:")
    for k, v in stmt_counts.most_common():
        print(f"  {k}: {v:,}")

    print(f"\nBy period type:")
    for k, v in period_counts.most_common():
        print(f"  {k}: {v:,}")

    print(f"\nSource page linking:")
    for match_type, count in source_stats.most_common():
        pct = count / len(all_rows) * 100
        print(f"  {match_type}: {count:,} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
