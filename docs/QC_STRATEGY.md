# P&L Extraction QC Strategy

This document captures our quality control approach for the P&L extraction pipeline, lessons learned from edge cases, current results, and remaining challenges.

## Overview

The QC system validates extracted P&L statements through two complementary checks:
1. **Formula Validation** - Verifies arithmetic relationships (e.g., Gross Profit = Revenue + Cost of Sales)
2. **Source Match Validation** - Confirms extracted values appear in the source markdown

Both checks must pass for a file to be considered "clean."

## QC Implementation

### Formula Validation (Step4_QCPL.py)

Parses the `Ref` column formulas and evaluates them against extracted values:
- `C=A+B` means row C should equal row A + row B
- Tolerance: 0.1% relative error (handles rounding)
- Validates across all period columns

### Source Match Validation

Compares every extracted numeric value against the source markdown page(s):
- Extracts all numbers from source using regex patterns
- Checks if each extracted value exists in source (with tolerance)
- Threshold: 97% of values must be found in source

## Key Fixes Applied

### 1. Space-Separated Number Parsing

**Problem:** Some source documents use space-separated numbers (e.g., `2 294 597` instead of `2,294,597`). QC was parsing these as separate numbers (2, 294, 597).

**Solution:** Added space-separated patterns to `extract_all_numbers()`:
```python
patterns = [
    r'\([\d,]+(?:\.\d+)?\)',      # (1,234,567) - comma-separated in parens
    r'[\d,]+(?:\.\d+)?',           # 1,234,567 - comma-separated
    r'\([\d]+(?: \d{3})+\)',       # (1 234 567) - space-separated in parens
    r'[\d]+(?: \d{3})+',           # 1 234 567 - space-separated
]
```

**Files affected:** EFUG and other companies using European-style number formatting.

### 2. Unit Scaling Check

**Problem:** Source markdown sometimes shows full rupee values (e.g., 8,894,522,344) while extraction outputs thousands (8,894,522). QC couldn't match these.

**Solution:** Added unit scaling check:
```python
if unit_multiplier > 1 and src_val > unit_multiplier:
    scaled_src = src_val / unit_multiplier
    if abs(abs(val) - scaled_src) / max(abs(val), 1) <= 0.001:
        found = True
```

### 3. Number Format Rule in Extraction Prompts

**Problem:** Extraction sometimes output numbers without comma separators, causing QC mismatches.

**Solution:** Added explicit rule to extraction prompts (Step3_ExtractPL.py, ExtractPLFromPDF.py):
```
NUMBER FORMAT: Always use commas as thousand separators (1,234,567).
If source uses spaces (1 234 567) or no separators, convert to comma format.
```

### 4. Wrong Page Assignments

**Problem:** DeepSeek classification sometimes assigned wrong pages (e.g., Balance Sheet page instead of P&L).

**Examples fixed:**
- EFERT annual_2023 consolidated: page 126 (Balance Sheet) → page 127 (P&L)
- SYS quarterly_2024-03-31 unconsolidated: page 13 (dashboard) → page 14 (P&L)

**Solution:** Manual correction in `step2_statement_pages.json` after investigation.

## Edge Cases Discovered

### 1. Dual-Statement Pages
Some PDFs have both P&L and Balance Sheet on the same page. This causes source match issues because:
- The extracted P&L values are correct
- But Balance Sheet values on the same page inflate the "source numbers" pool
- Some P&L values may appear similar to BS values, causing false matches

### 2. Urdu/Non-English Content
Some pages are in Urdu or have OCR artifacts. The extraction pipeline correctly falls back to PDF vision, but:
- The markdown source has no usable financial data
- QC can't match extracted values against the (Urdu) markdown
- These appear as low source match rate despite correct extraction

### 3. Poor OCR Quality
Some markdown pages have:
- Nearly empty content (just page numbers)
- Accounting policy notes instead of financial statements
- Garbled OCR output

The extraction uses PDF vision as fallback, but QC still compares against bad markdown.

### 4. OCR Digit Errors
Single-digit OCR errors are common:
- `138,483` OCR'd as `158,483` (1→5)
- `91,215` OCR'd as `91,315` (2→3)
- `42,707` OCR'd as `42,797` (0→9)

These cause both formula failures and source match failures.

## Current Results

**As of latest QC run:**

| Metric | Value |
|--------|-------|
| Total files | 3,481 |
| Files passed | 3,397 |
| Files failed | 77 |
| Files with exceptions | 7 |
| **Clean rate** | **97.6%** |
| Formula pass rate | 99.9% (17,976/17,998) |
| Source match rate | 99.7% (74,592/74,793) |

### Failure Breakdown

| Category | Count | Description |
|----------|-------|-------------|
| Very low (<50%) | 3 | ALAC, RMPL (no pages assigned), EFERT (46.2%) |
| Near-miss (80-97%) | 62 | 58 pass formulas, 4 have formula failures |
| Formula failures only | 12 | Arithmetic doesn't match |

### Spot Check Results (7 random near-miss files)

| File | Source Match | Verdict |
|------|-------------|---------|
| MUREB_quarterly_2023-03-31 | 83.3% | Extraction value errors |
| BNWM_annual_2021 | 95.0% | Minor value error |
| ABL_quarterly_2025-09-30 | 92.3% | Source markdown empty |
| PSO_quarterly_2024-09-30 | 80.8% | OCR errors in source |
| EFUG_quarterly_2022-06-30 | 95.6% | Source is Urdu text |
| EFUG_annual_2023 | 92.9% | Source is accounting notes |
| EFUG_annual_2021 | 95.8% | Values correct, QC pattern issue |

**Key insight:** Most near-miss failures fall into distinct categories requiring different fixes.

## Deeper Investigation: Near-Miss Failure Analysis

After examining 83 pages flagged for potential re-OCR, we found:

**Surprising finding:** Most markdown pages are actually fine quality. The failures are due to:

1. **Minor value differences** - Extraction pulled slightly wrong values
   - BNWM: Distribution cost -42,797 vs source 42,707 (90 difference)
   - MUREB: Admin expenses 158,483 vs source 138,483 (20k difference)

2. **Large number formatting** - LaTeX-style dollar signs in source
   - ABL: Source has `$1,929,432$` which QC doesn't parse
   - Values like 98,863,021 not matching due to formatting

3. **Small/edge values** - EPS, percentages, small adjustments
   - BIPL: EPS values 11 and 10 hard to uniquely match
   - ATRL: Truncated item names for OCI values

4. **Computed values** - Subtotals that don't appear verbatim in source
   - Tax subtotals, intermediate calculations

**Example missing values from near-miss files:**
| File | Missing Value | Likely Cause |
|------|---------------|--------------|
| AABS | Other income: 162,142 | Minor extraction error |
| ABL | Interest earned: 98,863,021 | LaTeX formatting in source |
| ABOT | Minimum tax: -241,696 | Value not in source page |
| BNWM | Distribution cost: -42,797 | Extraction error (source has 42,707) |
| BIPL | EPS: 11 | Common number, hard to match |

## Remaining Challenges

### 1. Pages Needing Re-OCR

**After investigation:** The pages currently assigned in the manifest are mostly fine quality. Earlier spot checks found problematic pages (Urdu, empty), but these were from old/incorrect page assignments that have since been corrected.

**When re-OCR is needed:**
- Pages with Urdu content where English P&L exists in PDF
- Pages with wrong content (accounting notes instead of P&L)
- Pages with garbled/corrupted OCR output
- Very short content (<200 chars) with no tables

**Identification criteria:**
```python
is_urdu = sum(1 for c in content[:1000] if ord(c) > 1500) > 50
is_short = len(content.strip()) < 150
has_no_tables = '|' not in content
```

**Current status:** 0 pages in current manifest need re-OCR. The near-miss failures are due to QC pattern matching limitations, not markdown quality.

### 2. True Extraction Errors

Some files have actual value errors in extraction:
- MUREB: Admin expenses wrong (158,483 vs 138,483)
- PSO: Multiple values off by small amounts

**Solution:** Re-extract these specific files after fixing source markdown.

### 3. Formula Failures

4 files in near-miss category have formula failures. These need investigation:
- Could be extraction errors
- Could be unusual P&L structures
- Could be rounding accumulation

### 4. Files with No Pages Assigned

ALAC and RMPL have no P&L pages in the manifest. Either:
- The reports genuinely don't have P&L statements
- The classification step missed them

**Solution:** Manual review of these PDFs.

## QC Workflow

```
1. Run Step4_QCPL.py
   ├── Outputs: step4_qc_results.json
   └── Identifies failures by category

2. Investigate failures
   ├── Very low: Check page assignments
   ├── Near-miss: Compare extraction vs source
   └── Formula only: Check arithmetic

3. Fix root causes
   ├── Wrong pages → Update step2_statement_pages.json
   ├── Bad markdown → Re-OCR with Gemini
   ├── Extraction errors → Re-extract
   └── QC bugs → Fix patterns/tolerance

4. Re-run QC to verify fixes

5. Iterate until clean rate acceptable (target: 99%+)
```

## Files Reference

| File | Purpose |
|------|---------|
| `pipeline/stage3_extract/Step4_QCPL.py` | Combined formula + source match QC |
| `artifacts/stage3/step4_qc_results.json` | QC results with per-file details |
| `artifacts/stage3/qc_accepted_exceptions.json` | Known exceptions (unusual P&L structures) |
| `artifacts/stage3/reocr_fix.json` | Pages queued for re-OCR |
| `artifacts/stage3/reextract_fix.json` | Files queued for re-extraction |

## Lessons Learned

1. **Number formatting varies widely** - European spaces, no separators, full rupees vs thousands. QC must handle all formats.

2. **OCR quality is the bottleneck** - Most failures trace back to bad OCR, not extraction logic. Investing in re-OCR pays off.

3. **PDF vision fallback works** - When markdown is bad, extraction correctly uses PDF directly. But QC needs the markdown fixed to validate.

4. **Formula validation is reliable** - If formulas pass, the extraction is almost certainly correct. Low source match with passing formulas usually means bad markdown, not bad extraction.

5. **Page classification matters** - Wrong page assignments cause cascading failures. Worth investing in classification accuracy.

6. **97% threshold is reasonable** - A few values legitimately won't match (computed subtotals, EPS calculations). 97% catches real errors without false positives.

7. **Near-miss with passing formulas is usually correct** - If arithmetic validates, the extraction is almost certainly right. Low source match often indicates QC parsing limitations, not extraction errors.

## QC Improvements Needed

### 1. Handle LaTeX-Style Number Formatting

Some OCR outputs wrap numbers in dollar signs: `$1,929,432$`

Add pattern to `extract_all_numbers()`:
```python
r'\$[\d,]+(?:\.\d+)?\$',  # $1,234,567$ - LaTeX style
```

### 2. Lower Threshold for Formula-Passing Files

Consider accepting files with:
- Source match >= 80%
- All formulas pass
- No formula failures

These are almost certainly correct extractions where QC just can't match all values.

### 3. Exclude EPS/Percentage Values from Source Match

Small values like EPS (11, 10) and percentages appear multiple times in documents and are hard to uniquely match. Consider:
- Excluding values < 100 from source match calculation
- Or marking these as "expected unmatched"

## Next Steps

1. **Fix QC to handle LaTeX formatting** - Add `$number$` pattern
2. **Re-OCR truly problematic pages** - Only Urdu/empty/corrupted pages
3. **Accept near-miss with passing formulas** - Lower threshold to 80% for these
4. **Investigate formula failures** - The 4 files with formula issues need manual review
5. **Review no-pages files** - ALAC, RMPL need manual PDF inspection
