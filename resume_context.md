# Resume Context: Aligned Excel vs JSON Alignment Fix

## Issue Summary

**Problem**: Aligned Excel files (created by `Step1_AlignStatements.py`) had discrepancies with validated JSON data.

**Example**: ABL Jun 2024 CFO
- Aligned Excel: 173,726,722 ❌
- JSON: 173,226,722 ✅
- Difference: 500k (digit error or wrong column selection)

**Impact**: QC failures when comparing aligned files to JSON (e.g., 96.7% pass instead of 100%)

## Root Cause Analysis

Both pipelines use the **same source pages** (from `markdown_pages/`) but had different **column selection logic**.

### The Source Structure

Each quarterly filing contains **multiple period columns**:
```
Jun 2024 filing:
| Item | Canonical | Ref | 6M Jun 2024 | 6M Jun 2023 |
                           ↑ CURRENT      ↑ PRIOR YEAR
```

### JSON Pipeline (Correct)
- `Step5_JSONifyCF.py` has explicit `is_current_period()` logic
- Prefers **PRIMARY** source (current period column = first data column)
- Falls back to `prior_year_fallback` only when needed (rare: 12/thousands of periods)
- Result: Uses column 1 (6M Jun 2024) ✅

### Aligned Excel Pipeline (Was Incorrect)
- `Step1_AlignStatements.py` told DeepSeek to "extract ALL columns"
- Model had to guess which column to use
- Sometimes picked wrong column OR made digit transcription errors
- Non-deterministic with temperature=0.1
- Result: Sometimes used column 2 or made errors ❌

## Solutions Attempted

### ❌ Option 1: Rebuild Excel from JSON
- Read JSON data directly and populate Excel
- Pro: 100% alignment guaranteed, no LLM calls
- Con: Loses line item ordering, canonical field groupings, doesn't work for multi-canonical mappings
- **Rejected**: User said "don't reconstruct excel it won't work"

### ❌ Option 2: Use extracted files instead of raw OCR
- Feed `data/extracted_cf/*.md` (already parsed) to aligned extraction
- Add column selection metadata from JSON
- Pro: Uses same intermediate format
- Con: Over-engineered, still requires LLM interpretation
- **Rejected**: User said "still use source markdown_pages"

### ✅ Option 3: Simple Prompt Fix (FINAL SOLUTION)
- Keep using raw OCR from `markdown_pages/`
- Add explicit rule to prompt: **"ONLY extract {year} periods, IGNORE comparison columns"**
- Eliminates ambiguity - only ONE column per filing to choose from
- **Accepted**: Simple, effective, matches user's suggestion

## What We Changed

### File Modified
`pipeline/stage5_publish/Step1_AlignStatements.py`

### Changes Made

1. **Updated function signature**:
```python
def create_alignment_prompt(ticker: str, stmt_type: str, file_contents: dict[str, str],
                           company_type: str, year: int) -> str:
```

2. **Updated prompt** (key section):
```python
CRITICAL: Each source document has multiple period columns. ONLY extract {year} periods.
IGNORE all prior year comparison columns - do NOT extract any {year-1} data.

## EXTRACTION RULES
1. ONLY extract {year} periods - skip all {year-1} comparison columns
2. Extract each line EXACTLY as shown
...
```

3. **Reduced temperature**:
```python
temperature=0.0  # Was 0.1 - now fully deterministic
```

4. **Updated function call**:
```python
prompt = create_alignment_prompt(args.ticker, args.type, file_contents, company_type, args.year)
```

## Results - Before vs After

### Before Fix (ABL_cf_aligned_2025.md)
- Extracted both 2024 and 2023 columns
- QC Result: **58/60 passed (96.7%)**
- Jun 2024 CFO: 173,726,722 ❌
- Issues: 2 mismatches (500k diff)

### After Fix (ABL_cf_aligned_2024.md)
- Only extracted 2024 columns
- QC Result: **40/40 passed (100.0%)** ✅
- Jun 2024 CFO: 173,226,722 ✅
- Issues: **NONE**

## How to Use Going Forward

### 1. Generate Aligned Excel for a Year

```bash
# Generate markdown + Excel
python pipeline/stage5_publish/Step1_AlignStatements.py \
  --ticker ABL \
  --year 2024 \
  --type CF \
  --to-excel

# This creates:
# - data/excel/ABL_cf_aligned_2024.md (markdown)
# - data/excel/ABL_cf_aligned_2024.xlsx (Excel with formulas)
```

**Important**:
- Only extracts the specified year (e.g., 2024)
- Ignores all prior year comparison columns
- To get 2023 data, run separately with `--year 2023`

### 2. QC Against JSON

```bash
python pipeline/stage5_publish/Step2_QCAlignedStatements.py \
  data/excel/ABL_cf_aligned_2024.md
```

**Expected Output**:
```
Ticker: ABL, Type: CF
Periods checked: 4
Key canonicals checked: 40
Passed: 40 (100.0%)
Failed: 0
```

### 3. Investigate Failures (if any)

If QC shows failures:

```bash
# Run with verbose to see details
python pipeline/stage5_publish/Step2_QCAlignedStatements.py \
  data/excel/ABL_cf_aligned_2024.md --verbose
```

Check:
1. **Wrong column selected**: Verify prompt says "ONLY extract {year}"
2. **Digit transcription error**: Check source OCR page for correct value
3. **Prior year fallback case**: Check if JSON uses `prior_year_fallback` for this period
   ```python
   import json
   data = json.load(open('data/json_cf/ABL.json'))
   # Check source_type field for the period
   ```

### 4. Batch Process Multiple Tickers

```bash
# Example: Generate for top 10 tickers
for ticker in ABL LUCK ENGRO PPL OGDC HUB MCB UBL BAFL FFC; do
  echo "Processing $ticker..."
  python pipeline/stage5_publish/Step1_AlignStatements.py \
    --ticker $ticker \
    --year 2024 \
    --type CF \
    --to-excel

  # QC immediately
  python pipeline/stage5_publish/Step2_QCAlignedStatements.py \
    data/excel/${ticker}_cf_aligned_2024.md
done
```

## Key Insights

### Why This Works

1. **Eliminates ambiguity**: Only one column per filing → no guessing
2. **Matches JSON logic**: JSON also uses current period only
3. **Deterministic**: Temperature=0.0 ensures consistent extraction
4. **Simple**: No complex source tracking or metadata needed

### Edge Cases

**Prior Year Fallback** (12 cases out of thousands):
- Some periods use `prior_year_fallback` in JSON
- Current fix defaults to PRIMARY (first column)
- If these still mismatch after fix, can add per-period overrides
- But 99%+ of periods use PRIMARY, so this handles vast majority

**Restatements**:
- Not a factor here - both pipelines use the SAME filing for each period
- E.g., Jun 2024 comes from Jun 2024 filing (not Sep or Annual filing)
- Restatements would only matter if pulling from later filings

## Testing Checklist

When regenerating aligned files:

- [ ] Run Step1_AlignStatements.py with `--year YYYY`
- [ ] Verify output has only YYYY columns (not comparison years)
- [ ] Run Step2_QCAlignedStatements.py
- [ ] Confirm 100% pass rate
- [ ] Spot-check key totals (cfo, cfi, cff, net_cash_change, cash_end)
- [ ] Verify Excel formulas reference correct cells

## Files Reference

| File | Purpose |
|------|---------|
| `Step1_AlignStatements.py` | Generate aligned Excel from OCR (UPDATED) |
| `Step2_QCAlignedStatements.py` | Compare aligned vs JSON |
| `Step5_JSONifyCF.py` | Source of truth for column selection logic |
| `data/json_cf/*.json` | Validated JSON data (source of truth) |
| `data/excel/*_aligned_*.md` | Aligned markdown output |
| `data/excel/*_aligned_*.xlsx` | Aligned Excel with formulas |

## Common Commands

```bash
# Regenerate ABL 2024 CF
python pipeline/stage5_publish/Step1_AlignStatements.py --ticker ABL --year 2024 --type CF --to-excel

# QC it
python pipeline/stage5_publish/Step2_QCAlignedStatements.py data/excel/ABL_cf_aligned_2024.md

# Check JSON source decisions
python3 -c "
import json
data = json.load(open('data/json_cf/ABL.json'))
for p in data['periods']:
    if p.get('year') == 2024 and p.get('consolidation') == 'consolidated':
        print(f\"{p['duration']} {p['period_end']}: {p.get('source_type', 'primary')}\")
"

# Regenerate for 2023 (if needed)
python pipeline/stage5_publish/Step1_AlignStatements.py --ticker ABL --year 2023 --type CF --to-excel
```

## Next Session TODO

- [ ] Regenerate aligned files for all tickers in production
- [ ] Verify 100% QC pass rates across the board
- [ ] Handle any remaining edge cases (prior_year_fallback)
- [ ] Update documentation if needed
- [ ] Consider adding automated regression tests

---

**Status**: ✅ Fix validated and ready for production use
**Last Updated**: 2026-01-31
**Validated On**: ABL 2024 CF (100% QC pass)
