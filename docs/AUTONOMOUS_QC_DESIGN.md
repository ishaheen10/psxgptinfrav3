# Autonomous QC Loop Design

This document defines the vision and implementation plan for fully autonomous QC in the financial statement extraction pipeline.

---

## Data Flow Context

Understanding the pipeline data flow is essential for autonomous QC:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SOURCE: PDF Financial Reports                                               │
│ Location: database_pdfs/                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (OCR via Mistral/Gemini)
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: markdown_pages/{TICKER}/{YEAR}/{FILING}/page_NNN.md                │
│                                                                             │
│ What: Raw OCR output from PDF pages                                         │
│ Quality: May contain OCR errors (garbled text, wrong numbers, merged cells) │
│ Structure: One .md file per PDF page                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (LLM extraction via DeepSeek)
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: data/extracted_{pl,bs,cf}/{TICKER}_{filing_type}_{date}_{cons}.md  │
│                                                                             │
│ What: Structured extraction with canonical field mapping                    │
│ Format: Markdown table with columns:                                        │
│   - Source Item (original text from PDF)                                    │
│   - Canonical (standardized field name)                                     │
│   - Ref (row reference for arithmetic, e.g., "A", "B", "C=A+B")             │
│   - Period columns (e.g., "9M Mar 2023", "3M Mar 2022")                     │
│                                                                             │
│ Header: UNIT_TYPE indicates scale (thousands, millions, etc.)               │
│ Source: SOURCE_PAGES shows which markdown_pages were used                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step4_QC*.py - Formula QC)
┌─────────────────────────────────────────────────────────────────────────────┐
│ QC CHECKPOINT: artifacts/stage3/step4_qc_results_{pl,bs,cf}.json            │
│                                                                             │
│ What: Validates arithmetic within each extraction file                      │
│ Checks:                                                                     │
│   - Ref formulas evaluate correctly (e.g., C=A+B means row C = row A + B)   │
│   - Subtotals match their components                                        │
│   - Source match rate (extracted values found in source markdown)           │
│                                                                             │
│ Result: PASS/FAIL per file with specific formula failures                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step5_JSONify*.py)
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: data/json_{pl,bs,cf}/{TICKER}.json                                 │
│                                                                             │
│ What: All periods for a ticker in structured JSON                           │
│ Includes: Cross-period normalization (detect 1000x unit outliers)           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step6_QC*.py - Semantic QC)
┌─────────────────────────────────────────────────────────────────────────────┐
│ QC CHECKPOINT: artifacts/stage3/step6_qc_{pl,bs,cf}.json                    │
│                                                                             │
│ What: Validates semantic correctness                                        │
│ Checks:                                                                     │
│   - Critical fields present (total_assets, revenue_net, etc.)               │
│   - Accounting equations (Assets = Equity + Liabilities)                    │
│   - Monotonicity (9M > 6M > 3M for cumulative items)                        │
│   - Period arithmetic (Q1+Q2+Q3+Q4 = Annual)                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step7_DeriveQuarters*.py - PL/CF only)
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: data/quarterly_{pl,cf}/{TICKER}.json                               │
│                                                                             │
│ What: Quarterly (3M standalone) values derived from cumulative              │
│ Methods: direct_3M, 6M-Q1, 9M-6M, 12M-9M, 12M-Q1-Q2-Q3                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Files Reference

| Path | What It Contains |
|------|------------------|
| `markdown_pages/{TICKER}/{YEAR}/{FILING}/page_NNN.md` | Raw OCR from PDF page |
| `data/extracted_{pl,bs,cf}/{TICKER}_{type}_{date}_{cons}.md` | Structured extraction |
| `data/json_{pl,bs,cf}/{TICKER}.json` | All periods merged into JSON |
| `data/quarterly_{pl,cf}/{TICKER}.json` | Derived quarterly values |
| `artifacts/stage3/step2_statement_pages.json` | Source page locations for each ticker/filing |
| `artifacts/stage3/step4_qc_results_{pl,bs,cf}.json` | Per-file formula QC results |
| `artifacts/stage3/step6_qc_{pl,bs,cf}.json` | Semantic QC results |
| `artifacts/stage3/step7_arithmetic_allowlist{,_bs,_cf}.json` | Reviewed exceptions |
| `tickers100.json` | Ticker metadata (fiscal year end, industry) |

---

## Current State

The `/stage3-qc-loop` skill provides guidance but requires human decision-making for:
1. Categorizing failure root causes
2. Deciding between re-OCR, re-extract, manifest fix, or allowlist
3. Verifying source documents

## Vision: Fully Autonomous QC

```
Run QC → Auto-Triage → Auto-Fix (safe) → Flag (unsafe) → Re-run → Repeat
            ↓
    Decision Engine
    (reads source, compares, categorizes)
```

---

## QC Failure Types

### Formula Failures (Step4)

**What it means**: The Ref formulas in the extraction don't compute correctly.

**Example**: Row C claims `C=A+B` but the values don't add up.

**Possible causes**:
- OCR error in the source markdown
- LLM copied wrong values during extraction
- Rounding differences

### Semantic Failures (Step6)

**What it means**: Accounting equations don't balance, critical fields missing, or period arithmetic fails.

**Examples**:
- `total_assets ≠ total_equity + total_liabilities`
- `Q1+Q2+Q3+Q4 ≠ Annual` (beyond 1% tolerance)
- Missing `revenue_net` or `total_assets`

**Possible causes**:
- Period mislabeling in extraction
- Wrong values extracted from OCR
- Unit mismatch between periods
- Capital employed format (not standard BS)
- Incomplete extraction (manifest has 1 page, needs 2)

---

## Decision Tree for Each Failure Type

### 1. Formula Failures (Step4)

```
Formula fails for file X
    │
    ├─ Source match < 50%
    │   └─ DIAGNOSIS: Wrong page in manifest
    │   └─ ACTION: Find correct page, fix manifest, re-extract
    │   └─ AUTONOMOUS: PARTIAL (need to verify correct page)
    │
    ├─ Source match 50-90%
    │   └─ DIAGNOSIS: OCR corruption
    │   └─ ACTION: Re-OCR page, re-extract
    │   └─ AUTONOMOUS: YES (safe, just costs API)
    │
    └─ Source match > 90%
        └─ DIAGNOSIS: Extraction error OR source document error
        └─ ACTION: Check if source has same error
        │   ├─ Source also wrong → Add to allowlist
        │   └─ Source correct → Re-extract
        └─ AUTONOMOUS: PARTIAL (need source verification)
```

### 2. Critical Fields Missing (Step6)

```
Missing total_assets
    │
    ├─ Has total_non_current_assets AND total_current_assets
    │   └─ DIAGNOSIS: Capital employed format
    │   └─ ACTION: Derive in JSONify (already implemented)
    │   └─ AUTONOMOUS: YES (handled in code)
    │
    ├─ Has assets but missing equity/liabilities
    │   └─ DIAGNOSIS: Incomplete extraction (manifest has 1 page, needs 2)
    │   └─ ACTION: Check if page+1 has continuation, fix manifest
    │   └─ AUTONOMOUS: PARTIAL (need to verify page+1)
    │
    └─ Missing multiple sections
        └─ DIAGNOSIS: Wrong page entirely
        └─ ACTION: Find correct page in filing
        └─ AUTONOMOUS: NO (needs search)
```

### 3. Accounting Equation Failures (Step6)

```
total_assets ≠ total_equity + total_liabilities
    │
    ├─ Diff > 50%
    │   └─ DIAGNOSIS: Likely wrong column or format issue
    │   └─ ACTION: Check if capital employed format
    │   │   ├─ Yes → Add to skip list (not real failure)
    │   │   └─ No → Re-extract
    │   └─ AUTONOMOUS: PARTIAL
    │
    ├─ Diff 10-50%
    │   └─ DIAGNOSIS: Could be extraction or source error
    │   └─ ACTION: Compare extraction vs source markdown
    │   │   ├─ Extraction matches source, source has error → Allowlist
    │   │   └─ Extraction differs from source → Re-extract
    │   └─ AUTONOMOUS: YES (can compare programmatically)
    │
    └─ Diff < 10%
        └─ DIAGNOSIS: Minor rounding or sub-component issue
        └─ ACTION: Check if within tolerance → Pass or allowlist
        └─ AUTONOMOUS: YES
```

### 4. Unit Context Variations

```
Ticker has mixed units (thousands + rupees)
    │
    └─ DO NOT AUTO-FIX
    └─ ACTION: Flag for human review
    └─ AUTONOMOUS: NO (user explicit instruction)
```

---

## Investigation Workflow (Manual Fallback)

When autonomous triage can't resolve an issue, follow this workflow:

1. **Identify the failing ticker and fiscal year** from QC results

2. **Load the JSON data** to see all available periods:
   - What periods exist for that fiscal year?
   - What are the values for the failing field?
   - What source_filing did each period come from?

3. **Check for conflicts**:
   - Does the same (period_end, field) appear with different values?
   - Does the same value appear with different period durations?

4. **Trace back to extraction files**:
   - Read the relevant `extracted_{pl,bs,cf}/*.md` files
   - Check the period column headers
   - Verify against SOURCE_PAGES reference

5. **ALWAYS validate with source markdown**:
   - Find the source page location from: `artifacts/stage3/step2_statement_pages.json`
   - Read the original `markdown_pages` to see what the PDF actually said
   - Compare extracted values against the original OCR

6. **Determine the fix**:
   - If period label is wrong → edit the extraction file
   - If OCR error → re-OCR the page
   - If extraction missed data → re-extract
   - If source document has error → add to allowlist

7. **Apply fix and re-run QC**

---

## Required Components for Autonomy

### 1. Source Verification Function

```python
def verify_against_source(extraction_file: str) -> dict:
    """
    Compare extracted values against source markdown.

    Returns:
        {
            'source_pages': [12, 13],
            'extraction_matches_source': True/False,
            'source_has_arithmetic_error': True/False,
            'discrepancies': [
                {'field': 'total_assets', 'extracted': 100, 'source': 100000, 'ratio': 1000}
            ]
        }
    """
    # 1. Get source pages from manifest
    # 2. Read source markdown
    # 3. Parse numbers from source
    # 4. Compare with extracted values
    # 5. Check if source totals match source components
```

### 2. Page Continuation Detector

```python
def check_page_continuation(ticker: str, filing: str, current_page: int) -> dict:
    """
    Check if next page has continuation of statement.

    Returns:
        {
            'has_continuation': True/False,
            'continuation_page': 13,
            'continuation_type': 'equity_liabilities' | 'none'
        }
    """
    # Read page+1
    # Check for equity/liabilities headers
    # Check for table continuation markers
```

### 3. Format Detector

```python
def detect_bs_format(extraction_file: str) -> str:
    """
    Detect balance sheet presentation format.

    Returns: 'standard' | 'capital_employed' | 'net_assets' | 'insurance'
    """
    # Check for 'capital employed', 'net assets', 'working capital' labels
    # Check for insurance-specific items (Window Takaful)
```

### 4. Decision Engine

```python
def triage_failure(failure: dict) -> dict:
    """
    Analyze a QC failure and determine action.

    Returns:
        {
            'diagnosis': 'ocr_corruption' | 'wrong_page' | 'source_error' | 'format_issue',
            'action': 'reocr' | 'reextract' | 'fix_manifest' | 'allowlist' | 'flag_review',
            'autonomous': True/False,
            'confidence': 0.95,
            'details': '...'
        }
    """
```

### 5. Fix Executor

```python
def execute_fix(action: dict) -> bool:
    """
    Execute the recommended fix action.

    Handles:
    - reocr: Add to ReOCR manifest, run ReOCR
    - reextract: Delete file, run Step3_Extract with manifest
    - fix_manifest: Update step2_statement_pages.json
    - allowlist: Add to appropriate allowlist file
    """
```

---

## Implementation Plan

### Phase 1: Diagnostic Functions (No Auto-Fix)

Build the analysis tools that can categorize failures:

1. `verify_against_source()` - Compare extraction to source
2. `check_page_continuation()` - Find missing pages
3. `detect_bs_format()` - Identify non-standard formats
4. `triage_failure()` - Categorize and recommend action

Output: A triage report that tells Claude WHAT to do, but doesn't do it automatically.

### Phase 2: Safe Auto-Fixes

Implement autonomous fixes for high-confidence categories:

1. **Re-OCR + Re-extract** when source match 50-90%
2. **Add to allowlist** when source document has arithmetic error
3. **Derive missing fields** when components exist (already done for BS)

Requires: Clear confidence thresholds, iteration limits per file.

### Phase 3: Manifest Fixes

More complex - need to search for correct pages:

1. Detect when page is wrong (source match <50% or wrong statement type)
2. Search adjacent pages for correct content
3. Update manifest and re-extract

Requires: Page content classification, search within filing.

### Phase 4: Full Loop Orchestration

The skill becomes a true autonomous loop:

```python
while pass_rate < 99% and iterations < 10:
    run_qc()
    failures = get_failures()

    for f in failures:
        triage = triage_failure(f)

        if triage['autonomous'] and triage['confidence'] > 0.9:
            execute_fix(triage)
            fixed_count += 1
        else:
            flagged.append(f)

    if fixed_count == 0:
        # No more auto-fixable issues
        break

    re_run_qc()

report_remaining(flagged)
```

---

## Safety Guardrails

1. **Iteration limit per file**: Don't retry same file more than 3 times
2. **Total iteration limit**: Stop after 10 QC cycles
3. **Confidence threshold**: Only auto-fix if confidence > 0.9
4. **Batch size limit**: Don't re-OCR more than 50 pages per iteration (cost control)
5. **Human checkpoint**: After Phase 2 fixes, pause for review before Phase 3
6. **Unit variations**: NEVER auto-fix unit mismatches (user explicit instruction)

---

## Files to Create/Modify

| File | Purpose |
|------|---------|
| `pipeline/utilities/qc_diagnostic.py` | Source verification, format detection |
| `pipeline/utilities/qc_triage.py` | Decision engine, action recommender |
| `pipeline/utilities/qc_executor.py` | Auto-fix executor |
| `.claude/skills/stage3-qc-loop/SKILL.md` | Enhanced skill with decision tree |

---

## Example Autonomous Session

```
User: /stage3-qc-loop BS

Claude: Running BS QC...
  Pass: 113/138 (82%)
  Fail: 24

Triaging 24 failures...

AUTO-FIXABLE (high confidence):
  - CENI quarterly_2025-06-30: Missing page 13 in manifest → Fix manifest + re-extract
  - FFBL 6 periods: Source match 75% → Re-OCR + re-extract

NEEDS REVIEW (low confidence or unsafe):
  - NESTLE: Capital employed format (not real equation failure) → Add to skip?
  - 8 tickers: Unit context variation → User said don't auto-fix

Executing safe fixes...
  ✓ Fixed CENI manifest, re-extracted
  ✓ Re-OCR'd 12 FFBL pages, re-extracted

Re-running QC...
  Pass: 120/138 (87%)
  Fail: 17

[Continues until 99% or flags remaining for human review]
```

---

## Cost Considerations

| Action | API Cost | Risk |
|--------|----------|------|
| Re-OCR (Gemini) | ~$0.01/page | Low |
| Re-extract (DeepSeek) | ~$0.02/file | Low |
| Source verification | $0 (local) | None |
| Manifest fix | $0 (local) | Medium (if wrong) |

Budget per QC session: ~$5-10 for re-OCR/re-extract of problem files.

---

## Next Steps

1. **Implement `qc_diagnostic.py`** - The analysis layer
2. **Test on current BS failures** - Validate triage accuracy
3. **Add to skill** - Update SKILL.md with decision tree
4. **Implement executor** - For safe auto-fixes only
5. **Iterate** - Expand autonomous scope based on accuracy
