# QC Strategy V2 - Systematic Quality Control Framework

This document defines a systematic, stage-based QC approach for the financial statement extraction pipeline.

---

## Conceptual Overview: The 5 QC Layers

The QC strategy uses 5 complementary layers, each catching different types of issues:

```
Extraction (.md) → JSONify → Derived Quarters → Flatten → Upload
       ↓                ↓           ↓              ↓
   [1] Ref Formula  [2] Unit    [4] Period     [5] Final
   Check            Normalize   Arithmetic     Coverage
   (Internal)       (Cross-file) (Q1+Q2+Q3+Q4)
                                     ↓
                                [3] Semantic
                                Check
                                (Accounting Eq)
                                     ↓
                                Monotonicity
                                (9M > 6M > 3M)
```

### Layer 1: Ref Formula Check (Step4_QC*_Extraction.py)
**Purpose:** Internal consistency - "Does the extraction math add up?"

- Validates formulas like `D=A+B+C` on the raw extraction markdown
- Checks WITHIN a single filing (refs are per-file)
- **Catches:** LLM extraction errors, wrong row assignment, OCR corruption

**Pass Rate:** ~95%

### Layer 2: Unit Normalization (Step5_JSONify)
**Purpose:** Cross-filing consistency - "Are units consistent across filings?"

- Compares same metric (e.g., total_assets) across all filings for a ticker
- Detects 1000x outliers (rupees vs thousands mismatch)
- **Catches:** Wrong unit declaration, inconsistent unit handling

### Layer 3: Semantic/Accounting Equation Check (Step6_QC2*.py)
**Purpose:** External consistency - "Does this make accounting sense?"

| Statement | Check |
|-----------|-------|
| BS | `total_assets ≈ total_equity + total_liabilities` |
| CF | `CFO + CFI + CFF ≈ net_change_in_cash` |
| PL | `revenue - COGS ≈ gross_profit` |

Also includes **Monotonicity** for cumulative items: `9M > 6M > 3M`

**Key Insight:** Semantic checks catch completely DIFFERENT issues than ref formulas. There is **near-zero overlap** between them. An extraction can pass formula checks but fail semantic checks (e.g., columns misaligned, wrong period picked).

### Layer 4: Period Arithmetic (Step6_DeriveQuarters)
**Purpose:** Cross-period consistency - "Do derived quarters reconcile?"

```
Q4_value = 12M_value - Q1 - Q2 - Q3
```

- Validates derivation math is correct
- Detects unit mismatches between filings (Q4 derived as negative = red flag)
- **Catches:** Mixed units across filings, missing interim data

### Layer 5: Coverage & Final Gate (Step2_QCPreUpload)
**Purpose:** Completeness - "Is our data complete and sane?"

- All expected fiscal quarters present?
- No impossibly large values?
- No unexpected negatives in must-be-positive fields?
- **Catches:** Missing data, final sanity issues

---

### What Each Layer Catches (Summary)

| Layer | What it catches | Example |
|-------|-----------------|---------|
| **Ref Formula** | LLM wrote wrong number | `C=A+B` but extracted C ≠ A+B |
| **Unit Normalize** | Wrong unit declaration | One filing in rupees, others in thousands |
| **Semantic** | Wrong columns aligned | Assets=100 but Equity+Liabilities=100,000 |
| **Monotonicity** | Period confusion | Q3 cumulative revenue < Q2 cumulative |
| **Period Arithmetic** | Unit mismatch across filings | Q4 derived as negative |
| **Coverage** | Missing data | No Q2 2024 for ticker |

---

### Key Design Principle: Complementary Checks

**Ref Formula QC** and **Semantic QC** are both essential because they catch different failure modes:

- **Ref Formula:** Validates internal extraction math (did the LLM copy numbers correctly?)
- **Semantic:** Validates external accounting rules (do the numbers make real-world sense?)

In testing, we found **<1% overlap** between issues caught by each. Both are required for comprehensive QC.

---

## Detailed Implementation

The pipeline extracts three statement types (PL, CF, BS) through multiple stages. QC must validate at each stage with clear separation of concerns.

```
Extract (.md) → Step4 QC → JSONify → Step6 QC2 → Flatten → Pre-Upload QC → Upload
     ↓              ↓           ↓          ↓           ↓            ↓
  Raw values    Formula    Normalize   Semantic    Row-level    Final
  + formulas    + Source   + Dedupe    checks      sanity       gate
```

## Current State (Problems)

### 1. Duplicate Checks
- Source matching done in BOTH Step4 and Step6
- Wastes compute and causes confusion when they disagree

### 2. Inconsistent Cross-Statement
- Cross-period normalization only in BS (just added)
- PL and CF missing this critical unit validation
- Unit detection relies solely on LLM extraction (unreliable)

### 3. QC2 Source Matching Bug
- Compares values in thousands to source values in full rupees
- Causes false positive warnings (57 tickers flagged incorrectly for BS)

### 4. No Clear Ownership
- Formula validation mixed with semantic validation
- Hard to know which step should fix which problem

---

## Proposed Framework

### Principle: Each Stage Owns Specific Checks

| Stage | Responsibility | Fixes If Failed |
|-------|----------------|-----------------|
| Step4 (Post-Extract) | Extraction quality | Re-OCR + Re-extract |
| Step5 (JSONify) | Unit normalization | Auto-correct via cross-period |
| Step6 (QC2) | Semantic validity | Flag for review |
| Pre-Upload | Value sanity | Reject bad rows |

---

## Stage A: Post-Extraction Markdown QC (Step4_QC*md.py)

**Input:** `data/extracted_{pl,cf,bs}/*.md`
**Output:** `artifacts/stage3/step4_{pl,cf,bs}_qc_results.json`

### Purpose

Step4 validates that the LLM extraction produced a well-formed markdown table with:
1. Correct mathematical relationships (formulas)
2. Values traceable to source (source match)
3. Proper structure (unit declaration, column count, row count)

This is the FIRST line of defense - cheap checks that catch extraction errors before JSONify.

---

### Check 1: Unit Declaration (UNIT_TYPE)

| Check | Description | Status |
|-------|-------------|--------|
| **Exists** | UNIT_TYPE line present in markdown | ✅ Implemented (defaults to thousands) |
| **Valid value** | Must be: thousands, millions, rupees | ❌ TODO |
| **Unit sanity** | Value magnitude consistent with declared unit | ❌ TODO |

```python
# Validation logic
VALID_UNITS = {'thousands', 'millions', 'rupees', 'full_rupees'}

def check_unit_declaration(content: str) -> dict:
    match = re.search(r'UNIT_TYPE:\s*(\w+)', content)
    if not match:
        return {'passed': False, 'issue': 'UNIT_TYPE line missing'}

    unit = match.group(1).lower()
    if unit not in VALID_UNITS:
        return {'passed': False, 'issue': f'Invalid UNIT_TYPE: {unit}'}

    return {'passed': True, 'unit': unit}
```

**Unit Sanity Check (heuristic):**
```python
# If UNIT_TYPE=rupees but values look like they're already in thousands
# Flag for review (e.g., total_assets < 100,000 in rupees seems too small)
# If UNIT_TYPE=thousands but values have 12+ digits (trillions), flag as suspicious
```

---

### Check 2: Column Completeness

| Check | Description | Status |
|-------|-------------|--------|
| **Column count** | Extracted columns match expected for filing type | ❌ TODO |
| **Date headers** | All date columns are parseable | ❌ TODO |
| **Consistent count** | All rows have same number of value columns | ❌ TODO |

**Expected columns by filing type:**
```python
EXPECTED_COLUMNS = {
    'annual': {
        'min': 2,  # Current year, prior year
        'max': 2,
    },
    'quarterly': {
        'min': 2,  # Current quarter, prior year same quarter (minimal)
        'max': 4,  # Current Q, Prior Q, Current YTD, Prior YTD (full)
    }
}
```

**Implementation:**
```python
def check_column_completeness(rows: list, columns: list, filing_type: str) -> dict:
    expected = EXPECTED_COLUMNS[filing_type]
    actual = len(columns)

    result = {
        'passed': True,
        'expected_min': expected['min'],
        'expected_max': expected['max'],
        'actual': actual,
    }

    if actual < expected['min']:
        result['passed'] = False
        result['issue'] = f"Too few columns: {actual} < {expected['min']}"

    # Check all rows have consistent column count
    row_col_counts = [len(r['values']) for r in rows]
    if len(set(row_col_counts)) > 1:
        result['passed'] = False
        result['issue'] = f"Inconsistent row lengths: {set(row_col_counts)}"

    return result
```

**Compare to source markdown:**
```python
def count_source_columns(source_content: str) -> int:
    """Count columns in source table by counting | separators in header."""
    for line in source_content.split('\n'):
        if '|' in line and '---' not in line:
            # Count non-empty cells
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if len(cells) > 2:  # Likely a data row
                return len(cells) - 1  # Subtract label column
    return 0
```

---

### Check 3: Row Completeness

| Check | Description | Status |
|-------|-------------|--------|
| **Row count** | Minimum expected rows for statement type | ❌ TODO |
| **Ref sequence** | Ref letters are sequential (A, B, C...) | ❌ TODO |
| **Critical fields** | Required canonical fields present | ❌ TODO |

**Critical fields by statement:**
```python
CRITICAL_FIELDS = {
    'BS': ['total_assets', 'total_equity', 'total_liabilities', 'total_equity_and_liabilities'],
    'PL': ['revenue', 'gross_profit', 'operating_profit', 'net_profit'],  # or revenue_net for banks
    'CF': ['cfo', 'cfi', 'cff', 'net_change_in_cash'],
}

# Bank-specific (detected by industry or ticker)
BANK_CRITICAL_FIELDS = {
    'PL': ['net_interest_income', 'net_profit'],
}
```

**Ref sequence check:**
```python
def check_ref_sequence(rows: list) -> dict:
    """Check if Ref letters are sequential (helps detect missing rows)."""
    refs = [r['ref'] for r in rows if r['ref']]

    # Extract single-letter refs
    single_refs = [r for r in refs if len(r) == 1 and r.isalpha()]

    # Check for gaps
    gaps = []
    for i in range(len(single_refs) - 1):
        expected_next = chr(ord(single_refs[i]) + 1)
        if single_refs[i + 1] != expected_next:
            gaps.append(f"Gap between {single_refs[i]} and {single_refs[i + 1]}")

    return {
        'passed': len(gaps) == 0,
        'gaps': gaps,
        'total_rows': len(rows),
    }
```

---

### Check 4: Formula Validation (EXISTING)

| Check | Description | Status |
|-------|-------------|--------|
| **Ref formulas** | C=A+B evaluates correctly | ✅ Implemented |
| **Double-counting** | Detect subtotals in formulas | ✅ Implemented (BS only) |
| **Tolerance** | 0.25% for PL, 5% for BS/CF | ✅ Implemented |

**Current implementation handles:**
- Basic formulas: `C=A+B`, `D=A+B+C`
- Range formulas (CF): `sum(A:G)`
- Subtotal detection to avoid double-counting

---

### Check 5: Source Match (EXISTING)

| Check | Description | Status |
|-------|-------------|--------|
| **Value presence** | Each extracted value appears in source | ✅ Implemented |
| **Unit scaling** | Handle thousands vs rupees | ✅ Implemented |
| **Match threshold** | ≥97% match rate | ✅ Implemented |

---

### Check 6: Statement-Specific Sanity (NEW)

| Statement | Check | Threshold |
|-----------|-------|-----------|
| **BS** | total_assets ≈ total_equity_and_liabilities | 5% |
| **BS** | Formula coverage (starts from A/B) | ✅ Implemented |
| **PL** | net_profit appears | Required |
| **CF** | CFO + CFI + CFF ≈ net_change | 5% |

---

### Summary: Step4 Check Matrix

| # | Check | BS | PL | CF | Status |
|---|-------|----|----|----| -------|
| 1 | UNIT_TYPE exists | ✅ | ✅ | ✅ | Partial |
| 2 | UNIT_TYPE valid value | ✅ | ✅ | ✅ | ❌ TODO |
| 3 | Unit sanity (magnitude) | ✅ | ✅ | ✅ | ❌ TODO |
| 4 | Column count | ✅ | ✅ | ✅ | ❌ TODO |
| 5 | Date headers parseable | ✅ | ✅ | ✅ | ❌ TODO |
| 6 | Row count sanity | ✅ | ✅ | ✅ | ❌ TODO |
| 7 | Ref sequence | ✅ | ✅ | ✅ | ❌ TODO |
| 8 | Critical fields present | ✅ | ✅ | ✅ | ❌ TODO |
| 9 | Formula validation | ✅ | ✅ | ✅ | ✅ Done |
| 10 | Source match | ✅ | ✅ | ✅ | ✅ Done |
| 11 | Accounting equation | ✅ | - | - | ✅ Done |
| 12 | Formula coverage | ✅ | - | - | ✅ Done |
| 13 | CF identity | - | - | ✅ | ❌ TODO |

---

### Failure Actions

| Issue | Severity | Action |
|-------|----------|--------|
| UNIT_TYPE missing | ERROR | Re-extract |
| Invalid UNIT_TYPE | ERROR | Re-extract |
| Unit magnitude suspicious | WARN | Flag for Step5 cross-period check |
| Too few columns | ERROR | Re-extract or re-OCR |
| Inconsistent row lengths | ERROR | Re-OCR (table parsing issue) |
| Missing critical fields | ERROR | Re-extract |
| Formula fail | ERROR | Re-extract |
| Source match <80% | ERROR | Check page assignment, re-OCR |
| Source match 80-97% | WARN | Likely OK, QC pattern limitations |
| Accounting equation fail | ERROR | Re-extract or re-OCR |

---

## Stage B: JSONify with Normalization (Step5)

**Input:** `data/extracted_{pl,cf,bs}/*.md`
**Output:** `artifacts/stage3/json_{pl,cf,bs}_v2/{TICKER}.json`

### Normalization Steps

| Step | Description |
|------|-------------|
| **Parse extraction** | Read markdown, extract values |
| **Normalize to thousands** | Apply unit_type conversion |
| **Cross-period validation** | Detect outliers vs other filings |
| **Deduplicate** | Prefer annual over quarterly for same date |

### Cross-Period Normalization (CRITICAL)

```python
def apply_cross_period_normalization(periods: list) -> int:
    """
    Compare values across filings for same ticker.
    If one period's total_assets (or revenue_net) is >100x median,
    it's likely in wrong unit - apply correction.

    Reference metric:
    - PL: revenue_net
    - CF: cfo (cash from operations)
    - BS: total_assets
    """
    # Group by consolidation type
    # Calculate median of reference metric
    # Flag periods where value > 100x or < 0.01x median
    # Apply 1000x correction factor if detected
```

**This check catches:**
- Source says "thousands" but values are full rupees
- Inconsistent unit handling across filings
- OCR errors that produced wrong unit declarations

### Status
- [x] BS: Implemented
- [ ] PL: TODO - add to Step5_JSONifyPL_v2.py
- [ ] CF: TODO - add to Step5_JSONifyCF_v2.py

---

## Stage C: Semantic QC (Step6 QC2)

**Input:** `artifacts/stage3/json_{pl,cf,bs}_v2/{TICKER}.json`
**Output:** `artifacts/stage3/qc2_{pl,cf,bs}_results.json`

### Statement-Specific Checks

#### P&L Checks
| Check | Description | Threshold |
|-------|-------------|-----------|
| Column completeness | Expected periods exist | All fiscal quarters |
| Monotonicity | 9M > 6M > 3M for cumulative items | Strict (with exceptions) |
| Period arithmetic | Q1+Q2+Q3+Q4 = Annual | 5% tolerance |
| Semantic equations | revenue + COGS = gross_profit | 5% tolerance |

#### Cash Flow Checks
| Check | Description | Threshold |
|-------|-------------|-----------|
| Column completeness | Expected periods exist | All fiscal quarters |
| Period arithmetic | Q1+Q2+Q3+Q4 = Annual | 5% tolerance |
| CF Identity | CFO + CFI + CFF = net_change | 5% tolerance |
| Cash reconciliation | start + net_change = end | 5% tolerance |

#### Balance Sheet Checks
| Check | Description | Threshold |
|-------|-------------|-----------|
| Column completeness | Expected periods exist | All quarters |
| Accounting equation | Assets = Equity + Liabilities | 5% tolerance |
| ~~Source match~~ | ~~Values in source~~ | **REMOVED** (done in Step4) |

### Key Change: Remove Source Matching from QC2

**Rationale:**
1. Already done in Step4 (redundant)
2. Causes unit mismatch bugs (thousands vs rupees)
3. Semantic checks are what QC2 should focus on

---

## Stage D: Pre-Upload QC (Step2_QCPreUpload)

**Input:** `artifacts/stage5/{pl,cf,bs}_flat.jsonl`
**Output:** `artifacts/stage5/qc_pre_upload_{pl,cf,bs}.json`

### Row-Level Sanity Checks

| Check | Description |
|-------|-------------|
| Value sanity | No impossibly large values (>1 trillion in thousands) |
| Required fields | ticker, period_end, canonical_field, value all present |
| Sign consistency | must_be_positive fields are positive |
| Duplicate detection | No duplicate rows |

---

## Failure Categories and Actions

### Category 1: Formula Failures (Step4)
**Cause:** LLM extracted wrong values or formulas
**Action:** Re-extract the file

### Category 2: Source Match Failures (Step4)
**Sub-categories:**
- **<50% match:** Wrong page assigned → fix page mapping
- **50-80% match:** OCR quality issue → re-OCR the page
- **80-97% match:** Usually OK, QC pattern limitations

### Category 3: Unit Mismatches (Step5)
**Cause:** LLM declared wrong unit_type
**Action:** Cross-period normalization auto-corrects

### Category 4: Semantic Failures (Step6)
**Sub-categories:**
- **Accounting equation fail:** Column misalignment in extraction
- **Period arithmetic fail:** Missing quarters or wrong periods
- **Monotonicity fail:** Usually legitimate (restated quarters)

---

## Implementation Checklist

### Step4 Markdown QC Enhancements (NEW)
- [ ] **UNIT_TYPE validation** - Check exists + valid value (thousands/millions/rupees)
- [ ] **Unit magnitude sanity** - Flag if values seem wrong for declared unit
- [ ] **Column count check** - Verify expected columns for filing type
- [ ] **Date header validation** - All date columns parseable
- [ ] **Row consistency check** - All rows same column count
- [ ] **Ref sequence check** - Detect missing rows via gaps in A, B, C...
- [ ] **Critical fields check** - Required canonicals present (total_assets, etc.)
- [ ] **CF identity check** - CFO + CFI + CFF ≈ net_change (for CF only)

### Step5 JSONify Fixes
- [ ] Add cross_period_normalization to Step5_JSONifyPL.py
- [ ] Add cross_period_normalization to Step5_JSONifyCF.py
- [x] Add cross_period_normalization to Step5_JSONifyBS.py ✓

### Step6 QC2 Fixes
- [ ] Remove source matching from Step6_QC2BS.py (redundant with Step4)
- [ ] Remove source matching from Step6_QC2CF.py
- [ ] Remove source matching from Step6_QC2PL.py
- [ ] Add critical fields presence check to all QC2

### Standardization
- [ ] Unify QC result JSON schema across all statement types
- [ ] Create single QC summary script that aggregates all results
- [ ] Document expected pass rates for each check

### Nice-to-Have
- [ ] Create /qc-loop skill that runs the full QC cycle
- [ ] Dashboard for QC status across all statements

---

## Expected Pass Rates

| Stage | Check | Target |
|-------|-------|--------|
| Step4 | Formula validation | 99.9% |
| Step4 | Source match (≥97%) | 97% |
| Step5 | Cross-period (no outliers) | 99% |
| Step6 | Accounting equation | 99% |
| Step6 | Period arithmetic | 95% |
| Step6 | Monotonicity (PL only) | 90% |

---

## File Reference

| File | Purpose |
|------|---------|
| `Step4_QC{PL,CF,BS}.py` | Post-extraction QC (formula + source) |
| `Step5_JSONify{PL,CF,BS}_v2.py` | JSONify with normalization |
| `Step6_QC2{PL,CF,BS}.py` | Semantic QC on JSON data |
| `Step2_QCPreUpload.py` | Pre-upload sanity checks |

---

---

## Key Concepts from V1 Archive (psxgptinfra)

### From Step5b_QCCalculations.py

**1. Critical Fields Check**
Every statement must have required fields:
```python
CRITICAL_FIELDS = {
    'profit_loss': {
        'bank': ['net_interest_income', 'net_profit'],
        'non_bank': ['revenue', 'revenue_net', 'net_profit'],
    },
    'balance_sheet': ['total_assets', 'total_equity', 'total_liabilities'],
    'cash_flow': ['cfo', 'cfi', 'cff']
}
```
**Status:** Not currently checked in V3 - TODO add to QC2

**2. EPS/DPS Sanity Check**
Per-share fields should NOT be in thousands:
```python
if canonical in PER_SHARE_FIELDS and abs(val) > 5000:
    flag_as_issue("EPS > 5000 - possibly wrong units")
```
**Status:** Partially in V3 (skip normalization for EPS) but no sanity check

**3. Mapping Coverage**
Track % of rows with canonical field assigned:
```python
mapping_coverage = mapped_rows / total_rows * 100
# Flag if < 80% coverage
```
**Status:** Not tracked in V3 - useful for extraction quality metric

### From Step6b_QCCrossFiling.py

**4. Cross-Filing Value Comparison (CRITICAL)**
Same period/field should match across filings:
```python
# If Q3 Sep 2024 revenue appears in both Q3 and Q4 filings,
# values should match within 5% tolerance
if ratio > 1.05:
    flag_discrepancy()
```
**Status:** Partially implemented as cross_period_normalization but different purpose

**5. 1000x Unit Error Detection (CRITICAL)**
Specific check for unit mismatch between filings:
```python
if 900 < ratio < 1100:  # Ratio ~1000x
    flag_as_critical("1000x unit error - mixed Rs vs 000s")
```
**Status:** Similar to cross_period_normalization but this compares SAME period across DIFFERENT filings

**Key Distinction:**
- **cross_period_normalization:** Compares DIFFERENT periods within SAME ticker (median-based)
- **cross_filing_validation:** Compares SAME period across DIFFERENT filings (exact match)

Both are needed! One filing might have Q3 2024 in thousands, another in rupees.

---

## Issues Discovered in V3 Investigation (Jan 2026)

### 6. OCR Table Row Misalignment

**Problem:** PDF-to-markdown OCR sometimes shifts table rows, causing labels to not match their values.

**Example (AABS 2023-12-31):**
```markdown
# Source markdown (corrupted):
| Cash and bank balances |  | 3,264 | 1,930 |
| Total Assets |  |  |  |           # <-- Value missing!
|  |  | 13,389,259 | 12,445,606 |   # <-- Value orphaned below
```

**Result:** Extraction sees "Total Assets = 3,264" when actual total is 13,389,259.

**Detection methods:**
1. **Accounting equation check** - If Assets ≠ Equity + Liabilities by >50%, likely row shift
2. **Value magnitude check** - If total_assets < sum of visible components, row shifted
3. **Cross-filing validation** - Same period in another filing will have correct value

**Fix:** Re-OCR the page with improved table extraction prompt.

### 7. Missing Columns in Extraction

**Problem:** Multi-column tables (e.g., 4 date columns) may only have 2-3 columns extracted.

**Example:**
```
Source PDF has:        | Q3 2024 | Q3 2023 | 9M 2024 | 9M 2023 |
Extraction captured:   | Q3 2024 | Q3 2023 |         |         |
```

**Detection methods:**
1. **Column count check** - Compare extracted columns vs expected for filing type
   - Quarterly filings typically have 4 columns (current Q, prior Q, current YTD, prior YTD)
   - Annual filings typically have 2 columns (current year, prior year)
2. **Period coverage check** - For Q3 filing, should have both 3M and 9M periods
3. **Source markdown inspection** - Count `|` separators in header row

**Fix:** Re-extract with explicit column count instruction, or re-OCR if markdown is malformed.

### 8. Concatenated Column Values (OCR Table Parsing Failure)

**Problem:** OCR fails to separate table columns, resulting in multiple numeric values concatenated in a single cell.

**Example (AICL, BAHL, EFUG):**
```markdown
# Source markdown (corrupted):
| Net Insurance Premium | 23 | 3,756,123 2,898,305 |    # <-- Two values in one cell!
| Net Insurance Claims | 24 | $(2,172,668)$ $(1,580,017)$ |

# Should be:
| Net Insurance Premium | 23 | 3,756,123 | 2,898,305 |
| Net Insurance Claims | 24 | (2,172,668) | (1,580,017) |
```

**Impact:**
- Extraction may pick up only one value (usually first)
- Prior year / comparison columns completely lost
- Source match QC reports low match rate (40-70%) because extracted values can't match concatenated source
- 74 files affected as of Jan 2026 investigation

**Detection methods:**
1. **Regex pattern** - Two comma-separated numbers with space between: `\d{1,3}(?:,\d{3})+\s+\d{1,3}(?:,\d{3})+`
2. **Source match correlation** - Low match rate (<80%) + concatenation pattern = OCR issue
3. **Pre-extraction scan** - Run detection on source markdown BEFORE extraction attempt

**Detection implementation:**
```python
import re

CONCAT_PATTERN = re.compile(r'\d{1,3}(?:,\d{3})+\s+\d{1,3}(?:,\d{3})+')

def detect_concatenated_columns(markdown_content: str) -> dict:
    """Detect OCR table parsing failures where columns are concatenated."""
    matches = CONCAT_PATTERN.findall(markdown_content)
    return {
        'has_concat_issue': len(matches) > 0,
        'concat_count': len(matches),
        'examples': matches[:5],
        'severity': 'error' if len(matches) > 5 else 'warning'
    }
```

**Fix:** Re-OCR the page with improved table extraction prompt that emphasizes column separation.

**Affected ticker patterns (Jan 2026):**
- Banks with large balance sheets (BAHL, ABL, HMB)
- Insurance companies with complex P&L (AICL, EFUG)
- Companies with multi-year summary tables (SLCL, HUMNL)

---

## Stage 2 Deterministic QC (Implemented Jan 2026)

### Script: Step1b_DeterministicQC.py

**Purpose:** Run structural checks on source markdown BEFORE LLM classification to catch OCR issues that LLMs miss.

**Location:** `pipeline/stage2_review/Step1b_DeterministicQC.py`

**Input:**
- `artifacts/stage3/step2_statement_pages.json` (pages to check)
- `artifacts/stage1/step7_skip_manifest.json` (urdu/edge pages to skip)
- `markdown_pages/` (source files)

**Output:** `artifacts/stage2/step1b_deterministic_qc.json`

### Checks Implemented

| Check | Pattern | Threshold | Severity |
|-------|---------|-----------|----------|
| **Concatenated columns** | `\d{1,3}(?:,\d{3})+\s+\d{1,3}(?:,\d{3})+` | ≥10 matches = error, ≥3 = warning | error |
| **Repeated lines** | Consecutive identical lines | ≥8 consecutive | error |
| **Low unique ratio** | Duplicate lines / total lines | <30% unique | error |
| **Orphaned numbers** | Lines with only numbers (no labels) | ≥5 orphaned lines | warning |

### Results (Jan 2026 Run)

```
Total statement pages checked: 13,407
  Pass:         13,207 (98.5%)
  Warning:      83 (0.6%)
  Fail:         117 (0.9%)
  Skipped:      36 (urdu/edge pages)

Issues by type:
  concatenated_columns: 173 pages
  orphaned_numbers: 140 pages
  repeated_lines: 5 pages
```

### Investigation Findings

**Legitimate failures: 114 pages (97.4% accuracy)**
- `concatenated_columns + orphaned_numbers`: 88 pages - Table structure completely broken
- `concatenated_columns` only: 24 pages - Values merged but labels intact
- `repeated_lines`: 2 pages - Actual OCR loops

**False positives: 3 pages (2.6%)**
- All were `repeated_lines` with `<br>` tags (spacing after signatures, not data corruption)

### Correlation with Source Match Rate

| Category | Count | Interpretation |
|----------|-------|----------------|
| **Both flagged** (det QC fail + <95% match) | 21 | Strong ReOCR candidates |
| **Match rate low, det QC passed** | 97 | Non-structural issues (extraction/unit errors) |
| **Det QC fail, match rate OK** | 168 | OCR issues but extraction still worked |

**Key insight:** Deterministic QC catches structural OCR issues. Low match rates with passing det QC indicate different problems (wrong values extracted, unit mismatches) that need different remediation.

### Why LLM Classification Missed These

DeepSeek classification prompt defines:
- **OK**: "Tables are clean, numbers readable, columns aligned"
- **ReOCR**: "Severe corruption (unreadable tables, garbled text...)"

Concatenated columns *look* readable - `3,756,123 2,898,305` has perfectly readable numbers. DeepSeek doesn't understand these should be in separate columns. It's a **structural** issue, not a **readability** issue.

**85% of pages with concatenation issues were marked "OK"** by DeepSeek classification.

### Usage

```bash
# Run deterministic QC on all statement pages
python pipeline/stage2_review/Step1b_DeterministicQC.py

# Run for single ticker
python pipeline/stage2_review/Step1b_DeterministicQC.py --ticker LUCK
```

### Recommended Workflow

```
Stage 1: Ingest → Skip manifest (urdu/edge)
    ↓
Stage 2: Step1b_DeterministicQC.py → Flag structural issues
    ↓
Stage 2: Step1_ClassifyPages.py → LLM classification (skip flagged pages?)
    ↓
Stage 2: ReOCR flagged pages
    ↓
Stage 3: Extract
```

---

## Updated Implementation Checklist

### Critical Missing Checks (from V1)
- [ ] **Critical fields presence** - Add to QC2 for all statement types
- [ ] **EPS sanity check** - Flag EPS > 5000 as unit error
- [ ] **Cross-filing validation** - Same period must match across filings
- [ ] **1000x detection** - Flag ratio 900-1100x as critical unit error
- [ ] **Mapping coverage tracking** - Track % rows with canonical field

### Critical Missing Checks (from V3 Investigation)
- [ ] **OCR row misalignment detection** - Labels don't match values (rows shifted)
- [ ] **Column completeness check** - All columns from source captured in extraction
- [x] **Concatenated column detection** - Two numbers in same cell (OCR table parsing failure) ✓ Implemented in Step1b_DeterministicQC.py

### Immediate Fixes
- [ ] Add cross_period_normalization to Step5_JSONifyPL_v2.py
- [ ] Add cross_period_normalization to Step5_JSONifyCF_v2.py
- [ ] Remove source matching from Step6_QC2{BS,CF,PL}.py (redundant)

### New QC Steps Needed
- [ ] Create Step6b_QCCrossFiling.py for V3 (port from V1 archive)
- [ ] Add critical fields check to QC2
- [ ] Add EPS sanity check to JSONify or QC2

---

## Full QC Cycle Process (Documented Jan 2026)

This section documents the complete QC cycle as validated through P&L processing.

### Pipeline Sequence

```
Step4_JSONify{PL,CF,BS}.py    → Parse markdown, create period JSON
Step5_QC{PL,CF,BS}.py         → Formula + source match QC on extractions
Step6_DeriveQuarters{PL,CF}.py → Derive missing quarters (Q4 = 12M - Q1-Q2-Q3)
Step1_Flatten{PL,CF,BS}.py    → Convert to flat JSONL for upload
Step2_QCPreUpload.py --type X → Final sanity checks before upload
```

### Step2_QCPreUpload Issue Types

| Issue Type | Description | Action |
|------------|-------------|--------|
| `unexpected_negative` | Negative value in field that should be positive (e.g., revenue_net) | Investigate source |
| `value_too_large` | Value exceeds magnitude threshold | Verify unit type |
| `missing_required` | Required field missing | Re-extract |
| `duplicate_row` | Same ticker+period+field appears twice | Dedupe |

### Investigation Process for QC Issues

When QCPreUpload flags issues, follow this process:

1. **Create investigation manifest** (`artifacts/stage3/{type}_qc_investigation_manifest.json`)
   - Group issues by ticker
   - Include relevant source filings and page numbers
   - Track derivation method (direct_3M, 12M-Q1-Q2-Q3, etc.)

2. **Parallel investigation** - Use agents to investigate batches of tickers
   - Read source markdown pages
   - Verify values against original documents
   - Classify each issue

3. **Classification categories:**

| Category | Definition | Action |
|----------|------------|--------|
| `LEGITIMATE` | Value is correct - exists in source or is valid business situation | Pass as-is |
| `FIXABLE` | Derivation artifact or minor data inconsistency | Optional fix or accept |
| `OCR_ERROR` | Source markdown is corrupted | Re-OCR the page |
| `NEEDS_REVIEW` | Unclear - requires human judgment | Manual review |

4. **Update tally file** (`artifacts/stage3/{type}_qc_investigation_tally.txt`)

### P&L QC Findings (Jan 2026)

**Investigation Results:**
- Total issues: 301 (113 unique ticker+period combinations)
- Tickers affected: 23
- Classification: 112 LEGITIMATE, 1 FIXABLE, 0 OCR_ERROR, 0 NEEDS_REVIEW

**Root Causes Identified:**

1. **Legitimate Business Situations** (majority of "unexpected_negative"):
   - Investment holding companies with negative returns (DAWH)
   - Leasing companies with provision losses > income (FDPL, SLCL)
   - REIT fair value accounting losses (TPLP)
   - Bank dividend adjustments/reversals (MEBL)
   - Real estate restatements (JVDC)
   - Bad quarters where COGS > revenue (IBFL)

2. **Unit Type Mismatch** (majority of "value_too_large"):
   - Some companies report in rupees, not thousands (KOHC, MEHT)
   - Large companies naturally have large values (MUGHAL 45B+, UNITY 68B+)
   - QC threshold assumes thousands - flags rupees as "too large"

3. **Derivation Artifacts** (rare):
   - 9M interim > 12M annual due to audit adjustments (FABL dividend_income)
   - Immaterial impact - accept as rounding artifact

**Recommendations for QC Thresholds:**
- Adjust "value_too_large" threshold based on UNIT_TYPE declared in extraction
- Consider company size/industry when flagging magnitudes
- Accept that some negative values are legitimate (investment losses, fair value accounting)

### Expected Pass Rates (Updated)

| Stage | Check | P&L | CF | BS |
|-------|-------|-----|----|----|
| Step4/5 | Formula validation | 99% | 99% | 99% |
| Step4/5 | Source match (≥97%) | 97% | 97% | 97% |
| Step6 | Quarter derivation | 99% | TBD | N/A |
| Pre-Upload | Value sanity | 99%+ | TBD | TBD |

*Note: Most "failures" at Pre-Upload are legitimate values that trigger magnitude thresholds, not actual data quality issues.*

---

## P&L QC Implementation - Completed (Jan 2026)

This section documents the complete P&L QC implementation with lessons learned for BS/CF.

### Final Architecture (Actual Implementation)

```
Step3_ExtractPL.py     → LLM extraction to markdown
Step4_QCPL_Extraction.py → Ref formula validation (internal consistency)
Step4_JSONifyPL.py     → Parse markdown to JSON, load QC status
Step5_QCPL.py          → Semantic validation (accounting equations, monotonicity, cross-period)
Step6_DeriveQuartersPL.py → Derive Q4 from 12M - Q1 - Q2 - Q3
```

**Key Design Decision:** Separate extraction QC (Step4) from semantic QC (Step5). These catch DIFFERENT failure modes with <1% overlap.

### Step4: Extraction QC - What It Catches

| Check | Description | Tolerance |
|-------|-------------|-----------|
| **Ref formulas** | `C=A+B` evaluates correctly | 5% |
| **PAGE_ERROR detection** | Manifest pointed to wrong page | Structural |

**PAGE_ERROR Pattern:** When extraction finds wrong statement type (e.g., Balance Sheet when expecting P&L), it writes:
```markdown
PAGE_ERROR: BALANCE_SHEET_ONLY
```

This indicates a **Stage 2 manifest error** - the page assignment in `step2_statement_pages.json` is wrong. Fix the manifest and re-extract.

### Step5: Semantic QC - What It Catches

| Check | Description | Severity |
|-------|-------------|----------|
| **unit_type** | Valid declaration (thousands/millions/rupees) | error |
| **column_completeness** | Expected years have data | warning |
| **critical_fields** | Revenue + bottom line exist with values | error |
| **semantic_equations** | net_profit ≈ PBT + taxation | error (>10%), warning (5-10%) |
| **monotonicity** | 9M > 6M > 3M for cumulative items | error |
| **cross_period_normalization** | No 100x outliers vs median | error |
| **period_arithmetic** | Q1+Q2+Q3+Q4 ≈ 12M | error |

### Critical Fields - Business Model Variations

**Standard companies need:** `revenue_net` (or variant) + `net_profit`

**But different business models have different income sources:**

```python
CRITICAL_FIELDS_PL = {
    'revenue': [
        'revenue_net', 'revenue_gross', 'revenue',  # Standard
        'net_interest_income', 'interest_income',  # Banks
        'net_premium', 'gross_premium',  # Insurance
        'dividend_income', 'total_income',  # Holding companies
        'capacity_revenue', 'energy_revenue',  # Power IPPs
        'lease_income',  # Leasing companies
        'share_of_associates',  # Investment holdings
        'other_income',  # Holding companies, IPPs in wind-down
    ],
    'bottom_line': ['net_profit', 'profit_after_tax', 'net_profit_parent'],
}
```

**Key Insight:** When REF checks pass but critical_fields fails, it means the FIELD EXISTS (formula structure correct) but the VALUE is empty. This happens for:
- IPPs in non-operational periods (KAPCO, HUBC)
- Holding companies with no traditional revenue (PHDL, PIAHCLA)
- Investment companies with income from `other_income`

### Semantic Equations - Complex P&L Structures

**Basic equation:** `net_profit = profit_before_tax + taxation`

**But some companies have more complex structures:**

1. **Discontinued operations:**
   ```
   net_profit = net_profit_continuing + net_profit_discontinued
   ```

2. **Post-PBT items (refineries, holding companies):**
   ```
   net_profit = PBT + taxation + share_of_associates + other_non_operating + other_income
   ```

3. **Taxation sign convention errors:**
   - Some filings show taxation as POSITIVE (expense format): `PBT = 100, Tax = 20, Net = 80`
   - Formula written as `Net = PBT + Tax` but should be `Net = PBT - Tax`
   - REF check may pass if tolerance allows, but semantic check catches it

**Implementation:** Try multiple equation patterns before failing:
```python
# Try basic equation first
if passes(net_profit, pbt + taxation):
    return PASS

# Try with discontinued operations
if net_profit_continuing and passes(net_profit, continuing + discontinued):
    return PASS

# Try with post-PBT items for complex structures
for item in [share_of_associates, other_non_operating, other_income, ...]:
    if adding_item_helps_balance():
        return PASS
```

### Monotonicity - Discrete vs Cumulative Quarters

**The check assumes:** Cumulative reporting (9M > 6M > 3M)

**But some companies report:** Discrete quarters (Q2 only, not YTD)

| Reporting Style | Example | Valid Pattern |
|-----------------|---------|---------------|
| **Cumulative** | Q1=100, H1=250, 9M=400 | 400 > 250 > 100 ✓ |
| **Discrete** | Q1=100, Q2=150, Q3=150 | Q3 < Q2 is valid! |

**Detection:** When 6M < 3M by large margin (>10%), it's likely discrete quarters, not cumulative.

**Affected tickers:** AABS, SLCL, some JVDC filings

### Cross-Period Normalization - Unit Error Detection

**Purpose:** Detect when one filing has wrong unit declaration

**Method:** Compare value to median across all filings for same ticker
- If value > 100x median → likely in wrong unit (rupees vs thousands)
- If value < 0.01x median → likely in wrong unit or OCR error

**Example fix:** NATF annual_2025 declared `UNIT_TYPE: millions` but values were in thousands. Fixed by editing extraction file.

**Business variations that trigger false positives:**
- **Holding companies (TRG):** Interest income varies wildly (1K to 200K)
- **IPPs in wind-down (HUBC, KAPCO):** Revenue drops to near-zero
- **Lease companies (FDPL):** Volatile lease income

### Skip List Pattern

For known issues that can't be automatically fixed, add to `SKIP_FILINGS`:

```python
SKIP_FILINGS = {
    # OCR corruption - extraction has garbage data
    'EFERT': ['annual_2021'],  # Values shifted between rows
    'SHEL': ['annual_2023'],   # BS and P&L on same page, placeholder data

    # Taxation sign convention - semantic check fails but data is correct
    'PHDL': ['quarterly_2021-12-31', 'quarterly_2023-12-31'],
    'JVDC': ['quarterly_2021-09-30'],

    # Rounding <1% - acceptable variance
    'ENGROH': ['annual_2024'],  # 0.17% diff
    'LUCK': ['annual_2021'],    # 0.18% diff

    # Discrete vs cumulative quarters
    'AABS': ['quarterly_2024-12-31', 'quarterly_2025-03-31'],
    'SLCL': ['quarterly_2024-03-31', 'quarterly_2025-03-31'],

    # Business variations - legitimate
    'KAPCO': ['quarterly_2024-03-31'],  # IPP ceased operations
    'TRG': ['annual_2021'],             # Holding company volatile income
}
```

### Extraction Error Patterns

| Pattern | Detection | Root Cause | Fix |
|---------|-----------|------------|-----|
| **PAGE_ERROR** | File starts with `PAGE_ERROR:` | Wrong page in manifest | Fix `step2_statement_pages.json` |
| **Placeholder data** | Values like 1,234,567 or round numbers | OCR failure on combined pages | Re-OCR or manual extraction |
| **Sign convention** | Semantic equation fails, REF passes | Taxation shown positive | Add to skip list |
| **1000x outlier** | Cross-period flags extreme ratio | Wrong unit declaration | Edit UNIT_TYPE in extraction |
| **Missing columns** | Expected period missing | LLM skipped columns | Re-extract |

### Final Results: P&L QC

```
Tickers:      138
Clean:        137 (99.3%)
Issues:       1 (warning only - ATIL didn't publish 2023 unconsolidated)

Issues resolved:
  critical_fields:           18 → 0 (added other_income to alternatives)
  semantic_equations:        11 → 0 (sign convention → skip list)
  monotonicity:              11 → 0 (rounding/discrete/business → skip list)
  cross_period_normalization: 10 → 0 (fixed NATF unit + skip list)
```

---

## Applying P&L Lessons to BS and CF

### Balance Sheet Specific Considerations (Updated Jan 2026)

1. **Critical fields:**
   ```python
   CRITICAL_FIELDS_BS = {
       'totals': ['total_assets', 'total_equity_and_liabilities'],
       # Alternatives for different formats:
       'totals_alt': ['total_assets', 'total_equity', 'total_liabilities'],
   }
   ```
   Note: Capital employed format (NESTLE, PAKT, IBFL) may not have explicit total_assets - derive from NCA + CA.

2. **Semantic equation:** `total_assets ≈ total_equity + total_liabilities`
   - Works for ~95% of companies
   - **Fails for capital employed format** - these show "Net assets" or "Capital employed" instead
   - Watch for: minority interest presentation, insurance company formats

3. **No monotonicity needed:** BS is point-in-time, not cumulative

4. **No derived quarters:** Unlike P&L/CF, BS doesn't need Q4 = 12M - 9M derivation

5. **Cross-period normalization:** Use `total_assets` as reference metric

6. **Format variations that cause failures:**
   - **Capital employed format**: No explicit Total Assets line (NESTLE, PAKT, IBFL)
   - **Insurance format**: Window Takaful with separate balance sheets (AICL, EFUG, CENI)
   - **Non-standard equity presentation**: Unlabeled subtotals between NCI and Total Equity (ENGRO)

7. **Source document errors are common:**
   - Pakistani filings often have arithmetic errors in stated totals
   - 14 BS periods allowlisted due to source document errors (Jan 2026)
   - Allowlist file: `artifacts/stage3/step7_arithmetic_allowlist_bs.json`

8. **Multi-page manifest issues:**
   - BS often spans 2 pages (assets on one, equity+liabilities on next)
   - Single-page manifest results in incomplete extraction
   - Detection: has total_assets but missing total_equity_and_liabilities

### Cash Flow Specific Considerations

1. **Critical fields:**
   ```python
   CRITICAL_FIELDS_CF = {
       'components': ['cfo', 'cfi', 'cff'],
       'total': ['net_change_in_cash', 'net_cash_change'],
   }
   ```

2. **Semantic equations:**
   - `CFO + CFI + CFF ≈ net_change_in_cash`
   - `cash_start + net_change ≈ cash_end` (if both available)

3. **Monotonicity:** YES - CF is cumulative like P&L
   - But CFF can legitimately decrease (debt repayment in later quarters)
   - Focus on CFO for monotonicity check

4. **Cross-period normalization:** Use `cfo` as reference metric

5. **Expected business variations:**
   - Large one-time investments (CFF spikes)
   - IPOs/buybacks (CFI spikes)
   - Working capital seasonality (CFO varies)

6. **Sign conventions:** CF items often have confusing signs
   - Payments = negative, receipts = positive
   - Some companies flip signs for certain items
   - Be lenient with sign-related formula failures

### Common Skip List Candidates (Predict for BS/CF)

Based on P&L patterns, expect to skip:

| Type | P&L Example | BS/CF Prediction |
|------|-------------|------------------|
| **OCR corruption** | EFERT, SHEL | Same tickers likely affected |
| **Holding companies** | TRG, PHDL | Same tickers, volatile values |
| **IPPs** | KAPCO, HUBC | May have minimal CF in wind-down |
| **Rounding** | ENGROH, LUCK | Same tolerance issues |
| **Discrete quarters** | AABS, SLCL | Check if same pattern in CF |

---

## Flat File QC Flag System (Implemented Jan 2026)

This section documents the user-facing QC flag system in the flat output files.

### Design Philosophy

Users consuming the flat file need to know:
1. **Which values have quality concerns** (automated detection)
2. **Which values were manually reviewed** (human validation)
3. **Why a value was flagged** (explanation for context)

All of this is consolidated into a **single `qc_flag` column** with format: `type: explanation`

### Flag Types

| Flag Type | Meaning | Example |
|-----------|---------|---------|
| `allowlisted` | Value was manually reviewed and approved | `allowlisted: Investment company - (Loss)/Income line legitimately negative` |
| `unexpected_negative` | Direct negative in field that should be positive | `unexpected_negative: Holding company - investment loss` |
| `derivation_anomaly` | Negative value from YTD derivation (Q4 = 12M - 9M) | `derivation_anomaly: PDF verified: Quarterly restatement` |

### Flag Structure

```
qc_flag = "flag_type: explanation"
       OR "flag_type"  (if no explanation available)
       OR ""           (no issues)
```

**Examples from production data:**

```json
// Allowlisted - reviewed, value is fine despite being in flagged ticker/FY
{
  "ticker": "ADAMS",
  "period_end": "2020-12-31",
  "canonical_field": "revenue_net",
  "value": 479574.187,
  "qc_flag": "allowlisted: PDF verified: Quarterly restatement - Q1+Q2+Q3 > Annual in source filings"
}

// Unexpected negative with explanation
{
  "ticker": "DAWH",
  "period_end": "2020-03-31",
  "canonical_field": "revenue_net",
  "value": -1027182.0,
  "qc_flag": "unexpected_negative: Holding company - 'Return on investments' line legitimately negative (investment loss)"
}

// Derivation anomaly without explanation (unreviewed)
{
  "ticker": "AABS",
  "period_end": "2025-03-31",
  "canonical_field": "revenue_net",
  "value": -666255.0,
  "qc_flag": "derivation_anomaly"
}

// Derivation anomaly with explanation (reviewed)
{
  "ticker": "ADAMS",
  "period_end": "2021-09-30",
  "canonical_field": "revenue_net",
  "value": -262667.2,
  "qc_flag": "derivation_anomaly: PDF verified: Quarterly restatement - Q1+Q2+Q3 > Annual in source filings"
}
```

### Non-Negative Fields

These fields trigger flags when negative:

```python
NON_NEGATIVE_FIELDS = {'revenue_net', 'gross_revenue', 'dividend_income', 'interest_income'}
```

### Arithmetic Allowlist

File: `artifacts/stage3/step7_arithmetic_allowlist.json`

Contains manually reviewed ticker/fiscal_year/consolidation combinations with documented reasons. Structure:

```json
{
  "allowlist": [
    {
      "ticker": "TPLP",
      "fiscal_year": 2024,
      "consolidation": "consolidated",
      "reason": "PDF verified: Investment company - (Loss)/income line legitimately negative"
    }
  ]
}
```

**Categories in allowlist (43 entries as of Jan 2026):**

| Category | Count | Examples |
|----------|-------|----------|
| Explicit restatements | 8 | DAWH, ENGRO with "(Restated)" label in source |
| Investment/holding companies | 10 | TPLP, DAWH - legitimate negative income |
| Companies in liquidation/ceased ops | 3 | PHDL, KAPCO |
| Quarterly restatement patterns | 6 | ADAMS, YOUW, JVDC |
| Small absolute values | 3 | SLCL, GRYL - tiny numbers, large % variance |
| Problematic PDF extraction | 6 | EFERT (excluded per user) |
| Banking/Insurance complexity | 4 | AKBL, EFUG, MEBL, FABL |

### Implementation

In `Step1_FlattenPL.py`:

```python
def get_qc_flag(ticker, period_end, section, field, value, method, fiscal_year) -> str:
    flag_type = ''

    # Check for unexpected negative values
    if field in NON_NEGATIVE_FIELDS and value is not None and value < 0:
        if method and method != 'direct_3M' and method != 'direct':
            flag_type = 'derivation_anomaly'
        else:
            flag_type = 'unexpected_negative'

    # Get allowlist note if available
    note = ALLOWLIST_LOOKUP.get((ticker, fiscal_year, section), '')

    # Build the flag string
    if flag_type and note:
        return f"{flag_type}: {note}"
    elif flag_type:
        return flag_type
    elif note:
        return f"allowlisted: {note}"
    else:
        return ''
```

### Stats (P&L as of Jan 2026)

```
Total rows:           89,056
Rows with qc_flag:    3,549
  - allowlisted:      3,512  (reviewed, no active issue)
  - unexpected_negative: 21  (negative in direct extraction)
  - derivation_anomaly:  16  (negative from YTD subtraction)
```

### User Guidance

When consuming flat file data:

1. **Empty `qc_flag`** = Clean data, no concerns
2. **`allowlisted:` prefix** = Value was reviewed, explanation tells you why it was flagged and approved
3. **`unexpected_negative:` prefix** = Caution - negative value where positive expected. Read explanation.
4. **`derivation_anomaly:` prefix** = Caution - derived value came out negative. May indicate source data inconsistency.

For programmatic filtering, parse the prefix (before `:`) to categorize flag types.

---

## Source Tracking in Flat File (Implemented Jan 2026)

Every row in the flat file includes source provenance:

| Field | Description | Example |
|-------|-------------|---------|
| `source_file` | Source markdown filename | `AABS_quarterly_2021-03-31_consolidated.md` |
| `source_pages` | Array of PDF page numbers | `[11]` |
| `source_url` | URL to source PDF folder | `https://source.psxgpt.com/PDF_PAGES/AABS/2021/AABS_Quarterly_2021-03-31` |

This enables users to trace any value back to its original source document.

---

## Quarter Derivation Coverage (P&L Analysis Jan 2026)

### Coverage Summary

| Metric | Value |
|--------|-------|
| Total tickers | 138 |
| Full coverage (Q1 2021 - Q3 2025) | 99 tickers (72%) |
| Partial coverage | 39 tickers (28%) |
| Total quarters derived | 4,181 |
| Total gaps | 164 |
| Gaps due to missing source PDF | 161 (98%) |
| Fixable gaps | 3 (2%) |

### Derivation Methods

Quarters are derived using these methods (in priority order):

| Method | Description | Example |
|--------|-------------|---------|
| `direct_3M` | 3M column directly extracted | Q1 from quarterly filing |
| `12M-9M` | Annual minus 9M cumulative | Q4 = FY - 9M |
| `9M-6M` | 9M minus 6M cumulative | Q3 = 9M - 6M |
| `6M-Q1` | 6M minus Q1 | Q2 = 6M - Q1 |
| `12M-Q1-Q2-Q3` | Annual minus all prior quarters | Q4 when no 9M available |

### Comparison Column Recovery

Many "missing" 3M quarters are recovered from later filings' comparison columns:

```
AICL quarterly filing 2025-09-30 contains:
  - Column 1: 3M Sep 2025 (current)
  - Column 2: 3M Sep 2024 (comparison)  ← Recovers prior year Q3
  - Column 3: 9M Sep 2025 (current YTD)
  - Column 4: 9M Sep 2024 (comparison YTD)
```

This design means even if the original Q3 2024 filing wasn't processed, the Q3 2024 value can be recovered from the Q3 2025 filing.

### Gap Analysis

The 3 fixable gaps identified:

| Ticker | Period | Issue | Fix |
|--------|--------|-------|-----|
| PHDL | 2025-03 | page_005.md corrupted, backup exists | Restore from backup |
| KTML | 2025-03 | No 6M Dec 2024 data to derive from | Source doesn't exist |
| PIAHCLA | 2024-06 | No 3M Mar 2024 data to derive from | Source doesn't exist |

---

## Manual Review & Allowlist System (Consolidated Jan 2026)

This section documents the complete allowlist/skip system used across the pipeline for handling known issues.

### Two-Tier System Overview

The QC system uses two complementary mechanisms for handling known issues:

```
Tier 1: SKIP_FILINGS (Python code)     → Skip entire filings from QC checks
Tier 2: Allowlist JSON files           → Filter issues + add explanations to output
```

### Tier 1: SKIP_FILINGS (Filing-Level Skips)

**Location:** Hardcoded in Step6_QC{PL,BS,CF}.py files

**Purpose:** Skip entire filings from specific QC checks (monotonicity, semantic equations, etc.)

**Format:**
```python
SKIP_FILINGS = {
    'TICKER': ['filing_pattern1', 'filing_pattern2'],
    # Example:
    'LCI': ['quarterly_2024-09-30'],  # Two-tier taxation structure
    'TPLP': ['quarterly_2022-09-30', 'quarterly_2023-03-31'],  # OCR errors
    'AABS': ['quarterly_2024-12-31'],  # Discrete quarters
}
```

**When to use:**
- OCR corruption that can't be fixed
- Legitimate business variations that fail semantic checks
- Discrete vs cumulative quarter reporting differences
- Rounding differences <1%

**Categories:**
| Category | Example Tickers | Reason |
|----------|-----------------|--------|
| OCR corruption | EFERT, SHEL | Values shifted, combined pages |
| Taxation sign | PHDL, JVDC | Tax shown positive, formula fails |
| Rounding <1% | ENGROH, LUCK | Immaterial variance |
| Discrete quarters | AABS, SLCL | 6M value not cumulative |
| Business variations | KAPCO, HUBC, TRG | IPP wind-down, holding companies |
| Two-tier taxation | LCI | Has taxation_final + taxation_total |

### Tier 2: Arithmetic Allowlist JSON Files

**Location:**
```
artifacts/stage3/step7_arithmetic_allowlist.json      # P&L
artifacts/stage3/step7_arithmetic_allowlist_bs.json   # Balance Sheet
artifacts/stage3/step7_arithmetic_allowlist_cf.json   # Cash Flow
```

**Purpose:**
1. Filter known issues from Step7 DeriveQuarters QC output
2. Add explanatory notes to `qc_flag` field in flattened data

**Format:**
```json
{
  "_comment": "Allowlist for P&L arithmetic/derivation issues",
  "_reviewed_date": "2025-01-21",
  "allowlist": [
    {
      "ticker": "DAWH",
      "fiscal_year": 2023,
      "consolidation": "consolidated",
      "reason": "Explicit (Restated) label in source - 25% revenue adjustment"
    }
  ]
}
```

**When to use:**
- Legitimate negative values in non-negative fields
- Quarterly restatements (Q1+Q2+Q3 > Annual)
- Investment/holding company income patterns
- Reviewed derivation anomalies

### Flow Through Pipeline

```
Step6 QC → SKIP_FILINGS filters filing-level issues
    ↓
Step7 DeriveQuarters → Allowlist JSON filters arithmetic issues
    ↓
Step1 Flatten → Allowlist adds explanations to qc_flag
    ↓
Step2 QC PreUpload → Skips rows with qc_flag (already reviewed)
```

### Adding New Manual Exceptions

**For filing-level issues (affects all checks):**
1. Edit `SKIP_FILINGS` dict in `pipeline/stage3_extract_statements/Step6_QC{PL,BS,CF}.py`
2. Add comment explaining why

**For arithmetic/derivation issues:**
1. Edit `artifacts/stage3/step7_arithmetic_allowlist{,_bs,_cf}.json`
2. Add entry with ticker, fiscal_year, consolidation, and reason
3. Reason will appear in `qc_flag` field for users

### qc_flag Field in Output

The flattened output files (`data/flat/{pl,bs,cf}.jsonl`) include a `qc_flag` field:

| Flag Type | Meaning | Example |
|-----------|---------|---------|
| (empty) | Clean data, no concerns | |
| `allowlisted: reason` | Reviewed and approved | `allowlisted: Investment company - legitimate negative` |
| `unexpected_negative: reason` | Negative in direct extraction | `unexpected_negative: Holding company loss` |
| `derivation_anomaly: reason` | Negative from YTD derivation | `derivation_anomaly: Quarterly restatement` |

**Step2 QC PreUpload skips rows with qc_flag** - they've already been reviewed.

### Statistics (P&L as of Jan 2026)

```
SKIP_FILINGS entries:     12 tickers, ~30 filing patterns
Allowlist JSON entries:   43 ticker/FY/consolidation combinations

Flat file qc_flag stats:
  Total rows:              89,003
  allowlisted:             3,512 (reviewed, approved)
  unexpected_negative:     21 (direct negative values)
  derivation_anomaly:      16 (derived negative values)
```

---

## Appendix: Lessons from V1

1. **Formula validation is reliable** - If formulas pass, extraction is almost certainly correct
2. **Source match has limitations** - QC patterns can't match all number formats
3. **Cross-period comparison is powerful** - Catches unit errors that single-file checks miss
4. **OCR quality is the bottleneck** - Most failures trace to bad OCR, not extraction logic
5. **Near-miss with passing formulas is usually correct** - Don't over-reject
6. **Cross-filing validation catches what cross-period misses** - Same period in different filings must match
7. **1000x ratio is the signature of unit errors** - Extremely reliable detection method
8. **Critical fields must be present** - Missing revenue or total_assets indicates extraction failure

## Appendix: Lessons from P&L QC (Jan 2026)

9. **REF pass + semantic fail = different issues** - REF checks formula structure, semantic checks actual values
10. **Business model matters for critical fields** - Holding companies, IPPs, banks have different income structures
11. **<1% rounding is acceptable** - Don't chase perfection, accept small variances
12. **Discrete vs cumulative quarters exist** - Monotonicity check needs exceptions
13. **Skip list is valuable** - Document known issues rather than trying to fix everything
14. **PAGE_ERROR = manifest error** - Fix Stage 2, not Stage 3
15. **Taxation sign convention varies** - Some filings show tax as positive expense
16. **other_income can be main income** - For holding companies, investment companies, IPPs
17. **Placeholder data (1,234,567) indicates OCR failure** - Combined pages, need re-OCR
18. **Cross-period catches unit errors that REF misses** - Essential second layer

## Appendix: Lessons from Flat File QC (Jan 2026)

19. **Single qc_flag column is cleaner than separate flag + note** - Format `type: explanation` provides both programmatic filtering and human context
20. **Allowlist at ticker/FY/consolidation level works well** - Granular enough to be useful, not so granular it's unmanageable
21. **Three flag types cover the cases**: `allowlisted` (reviewed OK), `unexpected_negative` (direct negative), `derivation_anomaly` (derived negative)
22. **Derivation anomaly ≠ bad data** - Often indicates legitimate restatements or business variations, not extraction errors
23. **Source tracking essential for trust** - Users need to verify values against original documents
24. **Comparison columns recover "missing" quarters** - Later filings contain prior year values, reducing actual gaps
25. **98% of coverage gaps are missing source PDFs** - Only 2% are fixable extraction issues
26. **SKIP_FILINGS for bad PDFs, ALLOWLIST for legitimate variations** - Different mechanisms for different problems

## Appendix: Lessons from BS QC (Jan 2026)

27. **Balance Sheet has multiple presentation formats** - Not all BS follow standard Assets = Equity + Liabilities:
    - **Standard format**: Total Assets = Total Equity + Total Liabilities (most common)
    - **Capital employed format** (NESTLE, PAKT, IBFL): Non-current assets + Net working capital = Capital employed. No explicit "Total Assets" line.
    - **Net assets format**: Shows Net current assets, calculates Net assets = Total equity
    - **Insurance format** (AICL, EFUG, CENI): Window Takaful Operations with separate assets/liabilities categories

    Capital employed format causes false accounting equation failures because "Total capital employed" ≠ Total Assets.

28. **Derived fields can rescue missing totals** - When total_assets is missing but components exist, compute it:
    ```python
    if total_assets is None and total_non_current_assets and total_current_assets:
        total_assets = total_non_current_assets + total_current_assets
    ```
    This fixed 19 BS periods. Consider similar derivations for other statements.

29. **Unit variations can be LEGITIMATE** - Companies change reporting units over time (rupees → thousands). P&L units were carefully verified. **NEVER** use deterministic rules to auto-fix unit mismatches based on majority within a ticker. Cross-period normalization should DETECT outliers, not auto-correct them.

30. **Bracket notation in extractions needs stripping** - LLM uses `[total_assets]` for computed subtotals. JSONify must strip brackets:
    ```python
    canonical = cols[2].replace('[', '').replace(']', '').strip()
    ```
    Without this, critical fields check fails because `[total_assets]` ≠ `total_assets`.

31. **Multi-page statements with single-page manifest** - Common failure pattern where BS spans pages 12-13 but manifest only has page 12. Results in incomplete extraction (typically missing equity/liabilities side). Detection: critical_fields check fails for equity/liabilities but assets are present.

32. **BS has no period arithmetic or monotonicity** - Balance Sheet is point-in-time snapshot, not cumulative. No derived quarters (Q4 = 12M - 9M), no monotonicity checks (9M > 6M). Only semantic check is accounting equation.

33. **Allowlist files differ by statement type** -
    - P&L: `step7_arithmetic_allowlist.json` (used by DeriveQuarters + Flatten)
    - BS: `step7_arithmetic_allowlist_bs.json` (used by Step4 extraction QC)

    BS allowlist is at Step4 (formula validation) because BS doesn't have derived quarters.

34. **Source document errors are common in Pakistani filings** - Many filings have arithmetic errors in the source PDF itself (stated totals don't match component sums). These should be allowlisted with documented reason, not "fixed" in extraction.

35. **Insurance companies need special handling** - Multiple separate balance sheets (main + Window Takaful Operator + Participants' Fund). Formula validation may fail when these are combined in unexpected ways.

---

## Critical Gaps Still to Address

Based on P&L and BS QC experience, these items need implementation:

### High Priority

1. **Cross-filing validation** - Same period in different filings must match. Currently only have cross-period (same ticker, different periods). Need to add: same period in Q3 filing vs Q4 filing should match.

2. **Manifest page validation** - Pre-extraction check that BS/PL/CF pages are correctly assigned. Many failures trace to wrong pages in manifest.

3. **Derived field computation in JSONify** - Generalize the BS pattern of computing missing totals from components:
   - BS: total_assets = NCA + CA
   - CF: net_change_in_cash = CFO + CFI + CFF (if missing)
   - PL: gross_profit = revenue - COGS (if missing)

### Medium Priority

4. **Format detection in extraction** - LLM should detect and flag non-standard formats:
   - Capital employed format (BS)
   - Direct method cash flow (CF)
   - Segment reporting (PL)

5. **Unit sanity check using value magnitudes** - If total_assets < 1M in "thousands", probably wrong unit. Heuristic check at extraction time.

### Low Priority

6. **Page count validation** - BS typically needs 2 pages, single page often means incomplete. Flag for review.

7. **Cross-statement consistency** - BS total_assets should match CF ending_cash_balance context. PL net_profit should flow to CF/BS retained earnings.
