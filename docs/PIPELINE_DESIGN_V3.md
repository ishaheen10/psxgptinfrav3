# Pipeline Design V3

> Supersedes PIPELINE_DESIGN_V2.md. Consolidates lessons learned from P&L/BS extraction iterations and simplifies the architecture.

## Design Principles

1. **Fail fast, fix once**: Detect issues early, repair in one pass, don't re-process
2. **One LLM call where possible**: Combine summarize + classify + score into single call
3. **Sequential statement completion**: Finish P&L to 98%+ before touching BS (98% = per statement type, not per filing)
4. **Human-in-loop for the last 2%**: Automated loops hit diminishing returns
5. **Source tracing built-in**: Every extracted value tracks its page origin
6. **Incremental by default**: Never re-process what's already done; resume from checkpoints

---

## Incremental Processing & Checkpoints

### Incremental Mode (Default)

Every step checks what's already done before processing:

```python
def should_process(item_id: str, output_path: Path, source_hash: str = None) -> bool:
    """
    Skip processing if output exists and source unchanged.

    Args:
        item_id: Filing or page identifier
        output_path: Where output would be written
        source_hash: Optional hash of source content for change detection
    """
    if not output_path.exists():
        return True  # No output yet, must process

    if source_hash:
        stored_hash = get_stored_hash(item_id)
        if stored_hash != source_hash:
            return True  # Source changed, must reprocess

    return False  # Already done, skip
```

**Applied at every stage**:
- Stage 1: Skip OCR for pages that already have markdown
- Stage 2: Skip classification for pages already in classification.jsonl
- Stage 3: Skip extraction for filings that already have final JSON
- Stage 4: Skip upload for rows already in database

### Checkpoint Strategy

Every step writes a checkpoint file that tracks progress:

```
artifacts/stage{N}/checkpoint.json
```

**Checkpoint format**:
```json
{
  "step": "Step2_ClassifyPages",
  "stage": 2,
  "started_at": "2026-01-05T10:30:00Z",
  "updated_at": "2026-01-05T11:45:00Z",
  "status": "in_progress",
  "progress": {
    "total_items": 166500,
    "completed": 85000,
    "failed": 120,
    "skipped": 3500
  },
  "completed_items": ["LUCK_annual_2024_page_001", ...],
  "failed_items": {
    "ABOT_annual_2024_page_045": "API timeout after 3 retries"
  },
  "resume_from": "ENGRO_quarterly_2024-03-31_page_012"
}
```

**On script start**:
```python
def load_checkpoint(step_name: str) -> Checkpoint:
    checkpoint_path = ARTIFACTS / f"stage{STAGE}" / "checkpoint.json"

    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint["step"] == step_name and checkpoint["status"] == "in_progress":
            print(f"Resuming from checkpoint: {checkpoint['progress']['completed']}/{checkpoint['progress']['total_items']}")
            return checkpoint

    return new_checkpoint(step_name)
```

**On each item completion**:
```python
def update_checkpoint(checkpoint: Checkpoint, item_id: str, status: str):
    checkpoint["completed_items"].append(item_id)
    checkpoint["progress"]["completed"] += 1
    checkpoint["updated_at"] = datetime.now().isoformat()

    # Write every N items to avoid too much I/O
    if checkpoint["progress"]["completed"] % 100 == 0:
        save_checkpoint(checkpoint)
```

**On script completion**:
```python
def finalize_checkpoint(checkpoint: Checkpoint):
    checkpoint["status"] = "completed"
    checkpoint["completed_at"] = datetime.now().isoformat()
    save_checkpoint(checkpoint)
```

### Recovery from Failures

If a script crashes mid-run:
1. Re-run the same script
2. It loads checkpoint, sees `status: in_progress`
3. Skips already-completed items
4. Resumes from `resume_from` position

**Manual reset** (if checkpoint is corrupted):
```bash
rm artifacts/stage2/checkpoint.json
python pipeline/stage2_review/Step2_ClassifyPages.py  # Starts fresh but skips existing outputs
```

---

## Cost Model

**Rates:**
| Provider | Input | Output |
|----------|-------|--------|
| Mistral OCR | $0.002/page | - |
| DeepSeek | $0.24/M tokens | $0.42/M tokens |
| Gemini Flash | $0.15/M tokens | $1.25/M tokens |

**Corpus:**
- 185,000 pages across ~2,400 filings
- ~77 pages per filing average
- ~1,000 tokens per page of markdown

---

## Stage 1: INGEST

**Goal**: Convert PDFs to clean markdown with complete quarterly coverage.

```
PDF → QC Coverage → Split Pages → JPG Thumbnails → Upload R2 → Mistral OCR → Build Skip Manifest → QC Extraction → Classification Manifest
```

### Step 1: Download PDFs
- Download quarterly/annual reports from PSX website
- Store in `database_pdfs/<ticker>/<year>/`

### Step 2: QC - Filing Coverage
**Check**: Do we have all expected quarterly filings?
- 4 filings per ticker per year (Q1, Q2, Q3, Annual)
- Flag missing filings for manual sourcing

**Type**: Deterministic (file existence check)

**Output**: `artifacts/stage1/step2_qc_filing_coverage.json`

### Step 3: Split Pages
- Split multi-page PDFs into individual page PDFs
- Output: `pdf_pages/<ticker>/<year>/<filing>/page_NNN.pdf`

### Step 4: Convert to JPG
- Create thumbnail images for UI preview
- Specs: 360px width, 70% quality, 110 DPI
- Output: `pdf_thumbnails/<ticker>/<year>/<filing>/page_NNN.jpg`

### Step 5: Upload to R2
- Upload pdf_pages/, pdf_thumbnails/, database_pdfs/ to Cloudflare R2
- Uses local manifest to track uploads (no redundant API calls)

### Step 6: Run Mistral OCR
- Process all pages through Mistral OCR ($0.002/page)
- Fetches PDFs via public R2 URL
- Output: `markdown_pages/<ticker>/<year>/<filing>/page_NNN.md`

### Step 7: Build Skip Manifest
**Skip criteria** (deterministic):
| Criterion | Detection Method |
|-----------|------------------|
| Urdu-heavy pages | ASCII ratio < 0.85 |
| First/last 2 pages | Position in filing (cover pages, back matter) |
| Corrupted (from ReOCR) | Latest `artifacts/utilities/reocr_*_failures.json` |

**Output**: `artifacts/stage1/step7_skip_manifest.json`

### Step 8: QC - Extraction Readiness
**Checks** (deterministic):
| Check | Threshold | Indicates |
|-------|-----------|-----------|
| Repeated lines | ≥12 consecutive identical | OCR loop/hang |
| Unique line ratio | ≤25% | Garbage output |
| Ultra-long lines | >2000 chars of pipes/dashes | Corrupted table separator |
| Data missing | `[DATA MISSING]` marker | OCR failure |

**Output**: `artifacts/stage1/step8_qc_issues.json`

**Action**: Run ReOCR utility on flagged pages, then re-run Steps 7-9.

### Step 9: Build Classification Manifest
- Consolidate all pages ready for Stage 2 classification
- Excludes skipped pages (Urdu, edges, corrupted)
- Tracks corrupted pages count for monitoring

**Output**: `artifacts/stage1/step9_classification_manifest.json`

### ReOCR Utility (On-demand)
- Re-run OCR on flagged pages using Gemini Flash (vision fallback)
- Called after Step 8 identifies quality issues
- Date-stamped outputs for audit trail
- Permanent failures feed back into Step 7's skip manifest

**Output**: `artifacts/utilities/reocr_YYYY-MM-DD_results.jsonl`, `artifacts/utilities/reocr_YYYY-MM-DD_failures.json`

---

### Stage 1 Cost Calculation

| Step | Pages | Rate | Cost |
|------|-------|------|------|
| Initial OCR | 185,000 | $0.002/page | $370.00 |
| Re-OCR (2% failure) | 3,700 | $0.002/page | $7.40 |
| **Stage 1 Total** | | | **$377.40** |

**Per page: $0.00204**

---

## Stage 2: REVIEW

**Goal**: Classify pages, repair markdown quality issues, build extraction manifest.

```
Build Skip Manifest → DeepSeek (Summary + Tags + Score) → Route Repairs → Final Manifest
```

### Step 2.1: Build Skip Manifest (Deterministic)

**Note**: Most skip logic moved to Stage 1 Step 7. Stage 2 adds classification-specific skips.

**Additional skip criteria**:
| Criterion | Detection Method |
|-----------|------------------|
| Multi-year summary pages | Regex: ≥3 years + table + "at a Glance" |

**Input**: `artifacts/stage1/step7_skip_manifest.json` (Urdu, edges, corrupted)

**Output**: `artifacts/stage2/skip_manifest.json`

**Assumption**: ~10% of pages skipped → 166,500 pages proceed

### Step 2.2: DeepSeek Classification (Single Call)

**Input per page**:
- Page markdown (~1,000 tokens)
- Prompt (~200 tokens)

**Output** (JSON, ~150 tokens):
```json
{
  "summary": "Q3 2024 P&L showing revenue of 45.2B, gross profit 12.1B...",
  "section_tags": [
    {"tag": "statement", "confidence": 0.95, "statement_type": "P&L", "scope": "C"}
  ],
  "extraction_score": "OK"  // or "Fix" or "ReOCR"
}
```

**Extraction Score Meanings**:
| Score | Meaning | Action |
|-------|---------|--------|
| `OK` | Tables readable, numbers clear | Proceed to extraction |
| `Fix` | Minor issues (misaligned columns, noise) | DeepSeek can repair from markdown |
| `ReOCR` | Severe corruption (unreadable tables) | Gemini re-extracts from PDF image |

### Step 2.3: Repair - Fix (DeepSeek)

For pages scored `Fix`:
- Input: Original markdown + repair prompt
- Output: Cleaned markdown
- DeepSeek excels at text manipulation

**Assumption**: 15% of pages need Fix repair → 24,975 pages

### Step 2.4: Repair - ReOCR (Gemini Flash)

For pages scored `ReOCR`:
- Input: PDF page image (from R2)
- Output: Fresh markdown transcription
- Gemini excels at vision/OCR

**Assumption**: 5% of pages need ReOCR → 8,325 pages

### Step 2.5: Final Corruption Check

Re-run corruption detection (Step 1.4 checks) on:
- Pages that were repaired
- Only statement-tagged pages (others don't matter)

Any still-corrupted statement pages get one final Gemini ReOCR attempt.

### Step 2.6: Build Final Manifest

Consolidate into extraction manifest:
```json
{
  "LUCK_annual_2024": {
    "filing_path": "markdown_pages/LUCK_annual_2024/",
    "statement_pages": {
      "P&L": {"pages": [45, 46], "scope": ["C", "U"]},
      "BS": {"pages": [47, 48, 49], "scope": ["C", "U"]},
      "CF": {"pages": [50, 51], "scope": ["C"]}
    },
    "skip_pages": [1, 2, 150, 151],
    "repaired_pages": [46, 48]
  }
}
```

**Output**: `artifacts/stage2/extraction_manifest.json`

---

### Stage 2 Cost Calculation

| Step | Items | Input Tokens | Output Tokens | Cost |
|------|-------|--------------|---------------|------|
| Classification | 166,500 pages | 199.8M | 24.98M | $58.44 |
| Fix Repair (15%) | 24,975 pages | 29.97M | 29.97M | $19.78 |
| ReOCR (5%) | 8,325 pages | 14.15M | 12.49M | $17.73 |
| **Stage 2 Total** | | | | **$95.95** |

**Breakdown**:
- Classification: 199.8M × $0.24/M + 24.98M × $0.42/M = $47.95 + $10.49 = $58.44
- Fix: 29.97M × $0.24/M + 29.97M × $0.42/M = $7.19 + $12.59 = $19.78
- ReOCR: 14.15M × $0.15/M + 12.49M × $1.25/M = $2.12 + $15.61 = $17.73

**Per page (of 166,500 processed): $0.00058**

---

## Stage 3: EXTRACT

**Goal**: Extract all financial statements with source tracing, validate arithmetic, achieve 98%+ pass rate per statement type.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  FIRST PASS: Extract All Statements Together                              │
│  ─────────────────────────────────────────────                           │
│  For each filing:                                                         │
│    → Send all statement pages to DeepSeek                                 │
│    → Extract P&L + BS + CF (consolidated & unconsolidated)                │
│    → Output includes SOURCE PAGE NUMBERS for each value                   │
│    → JSONify immediately                                                  │
│    → Run arithmetic QC                                                    │
│                                                                           │
│  Result: ~95% pass, ~5% fail (by statement)                               │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  REPAIR PASS: Sequential by Statement Type                                │
│  ─────────────────────────────────────────                               │
│                                                                           │
│  FOR P&L:                                                                 │
│    1. Isolate P&L failures                                                │
│    2. Re-extract with P&L-specific prompt + identified P&L pages          │
│    3. QC → some fixed                                                     │
│    4. Re-run persistent failures (LLM non-determinism)                    │
│    5. PDF fallback (Gemini) for stubborn cases                            │
│    6. Cross-validate from later filings (recover missing periods)         │
│    7. Claude Code hook: investigate remaining failures                    │
│    8. Target: 98%+ P&L pass rate                                          │
│                                                                           │
│  THEN BS: (same pattern)                                                  │
│  THEN CF: (same pattern)                                                  │
│                                                                           │
│  Result: 98%+ pass rate per statement type                                │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  VALIDATION & CALCULATION                                                 │
│  ────────────────────────                                                │
│  1. Compile all statements per ticker-period into single file             │
│  2. Re-run arithmetic QC on compiled output                               │
│  3. Period hierarchy validation (6M > 3M, 9M > 6M, 12M > 9M)              │
│  4. Calculate 3M standalone quarters from cumulative                      │
│  5. Calculate LTM figures                                                 │
│  6. Track: as_reported vs calculated for each value                       │
│                                                                           │
│  Output: Clean statements ready for database                              │
└──────────────────────────────────────────────────────────────────────────┘
```

### Step 3.1: First Pass Extraction (DeepSeek)

**Input per filing**:
- All statement pages (~8 pages × 1,000 tokens = 8,000 tokens)
- Extraction prompt with schema (~1,000 tokens)

**Output per filing** (~4,000 tokens):
```markdown
## PROFIT & LOSS (Consolidated)
SOURCE_PAGES: [45, 46]

| Canonical | Source Item | 12M Dec 2024 | 12M Dec 2023 |
|-----------|-------------|--------------|--------------|
| revenue | Net Sales | 45,234,567 | 41,234,567 |
| cost_of_sales | Cost of Sales | (32,123,456) | (29,123,456) |
| gross_profit | Gross Profit | 13,111,111 | 12,111,111 |
...

## BALANCE SHEET (Consolidated)
SOURCE_PAGES: [47, 48]
...

## CASH FLOW (Consolidated)
SOURCE_PAGES: [50, 51]
...
```

**Key**: `SOURCE_PAGES` enables tracing back when QC fails.

### Step 3.2: JSONify

Convert markdown tables to JSON immediately:
```json
{
  "ticker": "LUCK",
  "period": "annual_2024",
  "scope": "consolidated",
  "statements": {
    "P&L": {
      "source_pages": [45, 46],
      "columns": ["12M Dec 2024", "12M Dec 2023"],
      "data": {
        "revenue": {"12M Dec 2024": 45234567, "12M Dec 2023": 41234567},
        "cost_of_sales": {"12M Dec 2024": -32123456, "12M Dec 2023": -29123456},
        ...
      }
    },
    "BS": {...},
    "CF": {...}
  }
}
```

### Step 3.3: Arithmetic QC (Deterministic)

**Checks per statement**:

| Statement | Equation | Tolerance |
|-----------|----------|-----------|
| P&L | `gross_profit = revenue - cost_of_sales` | 0.5% or 1,000 |
| P&L | `operating_profit = gross_profit - operating_expenses` | 0.5% or 1,000 |
| P&L | `net_profit = profit_before_tax - tax_expense` | 0.5% or 1,000 |
| BS | `total_assets = total_liabilities + total_equity + nci` | 0.5% or 1,000 |
| CF | `net_cash_change = operating_cf + investing_cf + financing_cf` | 0.5% or 1,000 |

**Output per filing**:
```json
{
  "filing": "LUCK_annual_2024",
  "results": {
    "P&L_C": "PASS",
    "P&L_U": "PASS",
    "BS_C": "PASS",
    "BS_U": "FAIL",  // total_assets ≠ total_liabilities + equity
    "CF_C": "PASS",
    "CF_U": "INCOMPLETE"  // missing required fields
  }
}
```

**Status values**:
- `PASS`: Arithmetic reconciles
- `FAIL`: Arithmetic doesn't reconcile
- `INCOMPLETE`: Missing required fields (can't check)

### Step 3.4: Statement-Specific Repair

For each failed/incomplete statement:

1. **Identify source pages** from first pass output
2. **Expand page range** (critical for Balance Sheet — see heuristic below)
3. **Re-extract** with statement-specific prompt:
   ```
   Extract ONLY the Balance Sheet from these pages.
   This is an Unconsolidated Balance Sheet for LUCK as at December 31, 2024.
   The filing also contains Consolidated statements - ignore those.
   ```
4. **QC again**

#### Balance Sheet Adjacent Page Heuristic

Balance sheets frequently span 2 pages (Assets on one page, Equity & Liabilities on the next). This caused the "107 incomplete BS" problem where only half was extracted.

**Default behavior for BS extraction** — always expand to adjacent pages:

```python
def get_bs_pages(manifest_pages: list[int], filing_page_count: int) -> list[int]:
    """
    Expand BS page range to include adjacent pages.

    If manifest says BS is on page 105, also include 104 and 106.
    This catches multi-page balance sheets where one half was missed.
    """
    expanded = set(manifest_pages)

    for page in manifest_pages:
        # Add previous page (often has Assets or Equity section)
        if page > 1:
            expanded.add(page - 1)
        # Add next page (often has continuation)
        if page < filing_page_count:
            expanded.add(page + 1)

    return sorted(expanded)


def is_valid_bs_page(page: int, skip_manifest: set[int]) -> bool:
    """Filter expanded pages against skip manifest."""
    return page not in skip_manifest
```

**When to apply**:
- Always for BS first-pass extraction (proactive)
- Always for BS repair passes
- Optional for P&L and CF (usually single page)

### Step 3.5: PDF Fallback (Gemini)

For failures that persist after 2 DeepSeek attempts:
- Send PDF page images directly to Gemini
- Fresh extraction without markdown intermediary
- Catches cases where OCR fundamentally failed

### Step 3.6: Cross-Validation Recovery

For periods that fail primary validation:
- Check if later filings contain prior-period columns
- Example: LUCK_annual_2025 might have "Dec 2024" column that validates

```json
{
  "period": "annual_2024",
  "status": "validated",
  "source_filing": "LUCK_annual_2025",  // Later filing
  "source_column": "12M Dec 2024",
  "method": "cross_validated"
}
```

**Historical recovery**: This recovered 12 P&L and 22 BS periods in previous runs.

### Step 3.7: Claude Code Hook Investigation

For the last ~2% of failures:
- Trigger Claude Code hook with failure details
- Human investigates: Is it source data quality? OCR issue? Edge case?
- Decision: Fix manually, flag as source error, or exclude

### Step 3.8: Period Hierarchy Validation

**Check**: Cumulative periods should be monotonically increasing
```
3M < 6M < 9M < 12M (for same fiscal year)
```

**Catches**:
- Mislabeled periods (6M labeled as 3M)
- Wrong sign conventions
- Column assignment errors

### Step 3.9: Calculate Derived Values

**3-Month Standalone Quarters**:
```
Q1_3M = Q1_cumulative (as reported)
Q2_3M = Q2_6M_cumulative - Q1_3M
Q3_3M = Q3_9M_cumulative - Q2_6M_cumulative
Q4_3M = Annual_12M - Q3_9M_cumulative
```

**LTM (Last Twelve Months)**:
```
LTM = Q1_current + Q2_current + Q3_current + Q4_current
    = Most recent 4 standalone quarters
```

**Track provenance**:
```json
{
  "revenue": {
    "value": 45234567,
    "provenance": "as_reported",  // or "calculated"
    "source_periods": ["Q3_9M_2024", "Q2_6M_2024"]  // if calculated
  }
}
```

### Step 3.10: Compile Final Output

Merge all statements for a ticker-period:
- P&L (C + U)
- BS (C + U)
- CF (C + U)
- Source page attribution
- Validation status
- Derived calculations with provenance

**Output**: `statements_final/{ticker}_{period}.json`

---

### Stage 3 Cost Calculation

| Step | Items | Input Tokens | Output Tokens | Cost |
|------|-------|--------------|---------------|------|
| First pass | 2,400 filings | 21.6M | 9.6M | $9.19 |
| Repair (5% fail) | 180 statements | 0.54M | 0.27M | $0.24 |
| PDF fallback (1%) | 72 statements | 0.43M | 0.11M | $0.20 |
| **Stage 3 Total** | | | | **$9.63** |

**Breakdown**:
- First pass: 2,400 × 9,000 = 21.6M input, 2,400 × 4,000 = 9.6M output
  - Cost: 21.6M × $0.24/M + 9.6M × $0.42/M = $5.18 + $4.03 = $9.21
- Repair: 120 filings × 1.5 statements = 180 re-extractions
  - Input: 180 × 3,000 = 0.54M, Output: 180 × 1,500 = 0.27M
  - Cost: 0.54M × $0.24/M + 0.27M × $0.42/M = $0.13 + $0.11 = $0.24
- PDF fallback: 24 filings × 3 statements = 72 Gemini calls
  - Input: 72 × 6,000 = 0.43M, Output: 72 × 1,500 = 0.11M
  - Cost: 0.43M × $0.15/M + 0.11M × $1.25/M = $0.06 + $0.14 = $0.20

**Per filing: $0.0040**

---

## Stage 4: PUBLISH

**Goal**: Flatten for database, run final QC, upload.

```
Flatten → Normalize Units → Final QC → Upload to D1
```

### Step 4.1: Flatten for Database

Convert hierarchical JSON to flat rows:
```json
// Input (hierarchical)
{"P&L": {"revenue": {"12M Dec 2024": 45234567}}}

// Output (flat row)
{
  "ticker": "LUCK",
  "statement_type": "P&L",
  "scope": "consolidated",
  "line_item": "revenue",
  "period": "12M Dec 2024",
  "value": 45234567,
  "unit": "thousands",
  "provenance": "as_reported",
  "source_filing": "LUCK_annual_2024",
  "source_pages": [45, 46]
}
```

### Step 4.2: Normalize Units

All values stored in thousands (PKR '000):
| Source Unit | Conversion |
|-------------|------------|
| Full Rupees | ÷ 1,000 |
| Millions | × 1,000 |
| Billions | × 1,000,000 |
| Thousands | No change |

### Step 4.3: Final QC (Deterministic)

| Check | Purpose |
|-------|---------|
| BS duration | Balance sheet must be point-in-time, no "12M" |
| Period coverage | Every ticker has expected quarters |
| Duplicate detection | No duplicate rows |
| Sign consistency | Expenses negative, revenue positive |
| Cross-period validation | YoY changes within reasonable bounds |
| Unit sanity | Values within expected ranges for company size |

### Step 4.4: Upload to Cloudflare D1

- Batch insert/upsert
- Track upload timestamp
- Maintain audit trail

---

### Stage 4 Cost Calculation

| Step | Cost |
|------|------|
| All steps | $0 (deterministic) |
| D1 writes | ~$0.50 (database ops) |
| **Stage 4 Total** | **~$0.50** |

---

## Total Cost Summary

### Base Case

| Stage | Cost | % of Total |
|-------|------|------------|
| Stage 1: Ingest (OCR) | $377.40 | 78.0% |
| Stage 2: Review | $95.95 | 19.8% |
| Stage 3: Extract | $9.63 | 2.0% |
| Stage 4: Publish | $0.50 | 0.1% |
| **Total** | **$483.48** | 100% |

**Per page: $0.0026** (0.26 cents)
**Per filing: $0.20**

### Sensitivity Analysis

| Scenario | Stage 2 Repairs | Stage 3 Failures | Total Cost | Per Page |
|----------|-----------------|------------------|------------|----------|
| Optimistic | 10% Fix, 2% ReOCR | 2% fail | $420 | $0.0023 |
| Base | 15% Fix, 5% ReOCR | 5% fail | $483 | $0.0026 |
| Pessimistic | 20% Fix, 10% ReOCR | 10% fail | $580 | $0.0031 |

### Cost Drivers

```
OCR (Mistral)     ████████████████████████████████████████  78%
Classification    ████████████                              12%
Fix Repair        ████                                       4%
ReOCR (Gemini)    ████                                       4%
Extraction        ██                                         2%
```

**Key insight**: OCR is 78% of cost. Extraction is only 2%. Don't over-optimize extraction at the expense of OCR quality.

---

## Key Differences from V2

| Aspect | V2 | V3 |
|--------|----|----|
| Stage 2 LLM calls | 4 separate calls | 1 combined call |
| Repair routing | DeepSeek triage → Gemini | Direct: DeepSeek for markdown, Gemini for vision |
| Extraction folders | Fragmented (pl_json, bs_json, etc.) | Single path per filing |
| QC approach | Separate scripts per statement | Unified, iterate by type |
| Failure handling | Automated loops | Human-in-loop for last 2% |
| Source tracing | Added post-hoc | Built into extraction output |
| Period validation | 3M vs annual only | Full hierarchy (3M < 6M < 9M < 12M) |
| Provenance tracking | Not tracked | Every value tagged as_reported/calculated |

---

## File Organization

```
markdown_pages/                    # Stage 1 output
  {ticker}_{filing}/
    page_001.md
    ...

artifacts/
  stage1/
    corrupted_pages.json
    skip_corrupted.json
  stage2/
    skip_manifest.json
    classification.jsonl
    repairs_applied.jsonl
    extraction_manifest.json
  stage3/
    first_pass_qc.json
    repair_log.jsonl
    cross_validation.json
    final_qc.json
  stage4/
    upload_manifest.json

statements_final/                  # Stage 3 output (clean)
  {ticker}_{period}.json

database_rows/                     # Stage 4 output (flattened)
  statements.jsonl
  compensation.jsonl
  multiyear.jsonl
```

---

## Scripts

### Stage 1
| Script | Purpose |
|--------|---------|
| `Step1_DownloadPDFs.py` | Scrape from PSX |
| `Step2_QCFilingCoverage.py` | Check filing completeness |
| `Step3_SplitPages.py` | PDF to page images |
| `Step4_ConvertToJPG.py` | Create thumbnails for UI |
| `Step5_UploadToR2.py` | Store in Cloudflare R2 |
| `Step6_RunOCR.py` | Mistral OCR |
| `Step7_BuildSkipManifest.py` | Build skip manifest (Urdu, edges, corrupted) |
| `Step8_QCExtraction.py` | Detect quality issues in markdown |
| `Step9_BuildClassificationManifest.py` | Build manifest for Stage 2 |

### Stage 2
| Step | Script | Purpose | LLM? |
|------|--------|---------|------|
| 1 | `Step1_ClassifyPages.py` | DeepSeek: summary + tags + extraction_score | Yes |
| 2 | `Step2_BuildRepairManifest.py` | Route pages by extraction_score (OK/Fix/ReOCR) | No |
| 3 | `Step3_RepairFix.py` | DeepSeek markdown repair for Fix pages | Yes |
| 4 | `Step4_RepairReOCR.py` | Gemini PDF transcription for ReOCR pages | Yes |
| 5 | `Step5_FinalCorruptionCheck.py` | Verify repairs worked | No |
| 6 | `Step6_BuildExtractionManifest.py` | Build manifest for Stage 3 | No |

### Stage 3

**Main Pipeline (16 steps):**

| Step | Script | Purpose | LLM? |
|------|--------|---------|------|
| 1 | `Step1_BuildManifests.py` | Build extraction manifests from Stage 2 | No |
| 2 | `Step2_ExtractStatements.py` | First pass: extract all statements with page attribution | Yes |
| 3 | `Step3_ConvertToJSON.py` | Convert extracted markdown to JSON | No |
| 4 | `Step4_QCArithmetic.py` | Validate arithmetic equations (PASS/FAIL/INCOMPLETE) | No |
| 5 | `Step5_RepairPL.py` | Targeted P&L re-extraction for failures | Yes |
| 6 | `Step6_RepairBS.py` | Targeted BS re-extraction for failures | Yes |
| 7 | `Step7_RepairCF.py` | Targeted CF re-extraction for failures | Yes |
| 8 | `Step8_ConvertRepairsToJSON.py` | Convert repaired extractions to JSON | No |
| 9 | `Step9_QCRepairs.py` | QC repaired statements | No |
| 10 | `Step10_CrossValidate.py` | Recover periods from later filings | No |
| 11 | `Step11_MergeStatements.py` | Merge per-filing → per-ticker, normalize units | No |
| 12 | `Step12_QCPeriodOrdering.py` | Validate 3M < 6M < 9M < 12M | No |
| 13 | `Step13_CalculateDerived.py` | Calculate 3M standalones, LTM | No |
| 14 | `Step14_QCFinal.py` | Final pass rates, coverage report | No |
| 15 | `Step15_ExtractCompensation.py` | Extract CEO/director compensation (optional) | Yes |
| 16 | `Step16_ExtractMultiYear.py` | Extract multi-year summaries (optional) | Yes |

**Utilities (On-Demand):**

| Utility | Purpose |
|---------|---------|
| `ReExtract.py` | Generic re-extraction for specific filings |
| `ReExtractPL.py` | Manual P&L re-extraction |
| `ReExtractBS.py` | Manual BS re-extraction |
| `ReExtractCF.py` | Manual CF re-extraction |
| `ExtractFromPDF.py` | Gemini vision fallback (bypasses OCR) |
| `trace_statement_pages.py` | Debug: find which pages have a statement |
| `validate_3m_vs_annual.py` | Validate quarterly rollup to annual |
| `ReOCR.py` | Re-OCR corrupted pages |

**Units Terminology:**
All monetary values stored in `thousands` (PKR '000). Extraction prompts explicitly request this format. Conversion from other units (rupees, millions) happens at extraction time.

### Stage 4 (Current Scripts)
| Script | Purpose |
|--------|---------|
| `Step0_ValidateMapping.py` | Validate field mappings |
| `Step1_FlattenStatements.py` | Hierarchical → flat rows |
| `Step2_UnifyStatements.py` | Unify with canonical mappings |
| `Step2b_NormalizeUnits.py` | Normalize to thousands |
| `Step3_QCPreUpload.py` | Pre-upload validation |
| `Step4_UploadStatements.py` | Upload statements to D1 |
| `Step5_CompileDocuments.py` | Compile document database |
| `Step6_ComputeDelta.py` | Compute incremental delta |
| `Step7_UploadDocuments.py` | Upload documents to D1 |
| `Step8_UpdatePeriods.py` | Update filing periods |

---

## Appendix: Arithmetic QC Equations

### Profit & Loss
```
gross_profit = revenue - cost_of_sales
operating_profit = gross_profit - distribution_costs - admin_expenses - other_expenses + other_income
profit_before_tax = operating_profit + finance_income - finance_costs + share_of_associates
net_profit = profit_before_tax - tax_expense
```

### Balance Sheet
```
# Primary check (preferred — uses source document's own equality)
total_assets = total_equity_and_liabilities

# Fallback check (if total_equity_and_liabilities not extracted)
total_assets = total_liabilities + total_equity + non_controlling_interest

# Component checks
total_assets = current_assets + non_current_assets
total_liabilities = current_liabilities + non_current_liabilities
total_equity = share_capital + reserves + retained_earnings
```

**Note**: Most balance sheets explicitly show `Total Equity and Liabilities` as a line item that equals `Total Assets`. Using this direct equality is more reliable than computing the sum.

### Cash Flow
```
net_cash_from_operating = operating_activities_subtotal + working_capital_changes
net_cash_from_investing = investing_inflows - investing_outflows
net_cash_from_financing = financing_inflows - financing_outflows
net_change_in_cash = net_cash_from_operating + net_cash_from_investing + net_cash_from_financing
closing_cash = opening_cash + net_change_in_cash
```

### Tolerance
```python
def within_tolerance(actual, expected, rel_tol=0.005, abs_tol=1000):
    """0.5% relative or 1,000 absolute, whichever is larger"""
    tolerance = max(abs(expected) * rel_tol, abs_tol)
    return abs(actual - expected) <= tolerance
```

---

## Appendix: Cross-Validation Logic

```python
def find_cross_validation_source(ticker, period, statement_type):
    """
    Look in later filings for prior-period columns that validate.

    Example: LUCK_annual_2023 P&L fails validation
    → Check LUCK_annual_2024 for "Dec 2023" column
    → Check LUCK_annual_2025 for "Dec 2023" column
    → If found and validates, use that as source
    """
    target_year = extract_year(period)

    for future_year in [target_year + 1, target_year + 2]:
        future_filing = f"{ticker}_annual_{future_year}"
        if not exists(future_filing):
            continue

        data = load_json(future_filing)
        for column in data[statement_type]["columns"]:
            if matches_period(column, period):
                if validates_arithmetic(data[statement_type], column):
                    return {
                        "source_filing": future_filing,
                        "source_column": column,
                        "method": "cross_validated"
                    }

    return None  # No cross-validation source found
```

---

## Appendix: Claude Code Hook Interface

When automated pipeline reaches 98% but has stubborn failures:

```python
# Hook trigger condition
if pass_rate >= 0.98 and failures > 0:
    trigger_investigation_hook(failures)

# Hook payload
{
    "trigger": "extraction_failure",
    "statement_type": "BS",
    "failures": [
        {
            "filing": "ABOT_annual_2024",
            "scope": "unconsolidated",
            "error": "INCOMPLETE - missing total_equity",
            "source_pages": [104, 105],
            "attempted_repairs": 2,
            "diagnosis": "Page 104 contains equity section but was not in manifest"
        }
    ],
    "action_requested": "investigate and recommend fix"
}
```

Human decides:
1. Manual fix (edit markdown or JSON)
2. Flag as source data quality issue
3. Exclude from database with note
4. Expand page range and re-extract

---

## Artifacts Folder Structure

The `artifacts/` folder contains all manifests, QC reports, and checkpoints that drive pipeline decisions. Each stage reads from previous stages' artifacts and writes its own.

```
artifacts/
├── stage1/
│   ├── step2_qc_filing_coverage.json     # Missing filings report
│   ├── step7_skip_manifest.json          # Pages to skip (Urdu, edges, corrupted)
│   ├── step8_qc_issues.json              # Pages with quality issues
│   └── step9_classification_manifest.json # Pages ready for Stage 2
│
├── stage2/
│   ├── checkpoint.json              # Resume state for Stage 2
│   ├── skip_manifest.json           # Combined skip list (Stage 1 + multi-year)
│   ├── classification.jsonl         # Per-page: summary, tags, extraction_score
│   ├── repairs_applied.jsonl        # Which pages were repaired and how
│   └── extraction_manifest.json     # Final manifest for Stage 3 (statement pages per filing)
│
├── stage3/
│   ├── checkpoint.json              # Resume state for Stage 3
│   ├── first_pass_qc.json           # Initial QC results (PASS/FAIL/INCOMPLETE per statement)
│   ├── repair_log.jsonl             # Re-extraction attempts and outcomes
│   ├── cross_validation.json        # Periods recovered from later filings
│   ├── period_hierarchy_qc.json     # 3M < 6M < 9M < 12M validation
│   ├── source_manifest.json         # For each period: which filing/column to use
│   └── final_qc.json                # Final pass rates per statement type
│
├── stage4/
│   ├── checkpoint.json              # Resume state for Stage 4
│   ├── pre_upload_qc.json           # Final validation before upload
│   └── upload_manifest.json         # What was uploaded, when, row counts
│
└── utilities/
    ├── reocr_YYYY-MM-DD_results.jsonl    # ReOCR run results (date-stamped)
    └── reocr_YYYY-MM-DD_failures.json    # Permanent ReOCR failures (date-stamped)
```

### Key Manifests by Stage

| Stage | Manifest | Purpose | Consumed By |
|-------|----------|---------|-------------|
| 1 | `step7_skip_manifest.json` | Pages to skip (Urdu, edges, corrupted) | Stage 1 Step 9, Stage 2 Step 1 |
| 1 | `step9_classification_manifest.json` | Pages ready for classification | Stage 2 Step 2 |
| util | `reocr_*_failures.json` | Permanent ReOCR failures | Stage 1 Step 7 |
| 2 | `skip_manifest.json` | All pages to skip | Stage 2 Step 2+ |
| 2 | `classification.jsonl` | Page metadata + repair routing | Stage 2 Step 3-4 |
| 2 | `extraction_manifest.json` | Statement pages per filing | Stage 3 Step 1 |
| 3 | `first_pass_qc.json` | What needs repair | Stage 3 Step 4 |
| 3 | `source_manifest.json` | Authoritative source for each period | Stage 4 Step 1 |
| 4 | `upload_manifest.json` | Audit trail | External systems |

### Manifest Schemas

**extraction_manifest.json** (Stage 2 → Stage 3):
```json
{
  "LUCK_annual_2024": {
    "filing_path": "markdown_pages/LUCK_annual_2024/",
    "page_count": 156,
    "statement_pages": {
      "P&L": {"pages": [45, 46], "scope": ["C", "U"]},
      "BS": {"pages": [47, 48, 49], "scope": ["C", "U"]},
      "CF": {"pages": [50, 51], "scope": ["C"]}
    },
    "skip_pages": [1, 2, 150, 151, 155, 156],
    "repaired_pages": [46, 48]
  }
}
```

**source_manifest.json** (Stage 3 → Stage 4):
```json
{
  "source_folder": "statements_final",
  "generated_at": "2026-01-05T12:00:00Z",
  "periods": {
    "LUCK_annual_2024_PL_C": {
      "ticker": "LUCK",
      "period": "annual_2024",
      "statement": "P&L",
      "scope": "consolidated",
      "status": "validated",
      "source_filing": "LUCK_annual_2024",
      "source_column": "12M Dec 2024",
      "source_pages": [45, 46],
      "method": "primary",
      "qc_result": "PASS"
    },
    "LUCK_annual_2023_PL_C": {
      "status": "validated",
      "source_filing": "LUCK_annual_2024",
      "source_column": "12M Dec 2023",
      "method": "cross_validated"
    }
  }
}
```

---

## Steps vs Utilities

### Steps (Pipeline Orchestration)

Steps are numbered scripts that run in sequence as part of the main pipeline flow.

**Characteristics**:
- Run in order: `Step1` → `Step2` → `Step3` → ...
- Write to `artifacts/` folder
- Track progress via checkpoints
- Have clear input/output contracts
- Designed for batch processing

**Location**: `pipeline/stage{N}_*/Step{N}_*.py`

**Example**:
```bash
# Run full Stage 2
python pipeline/stage2_review/Step1_BuildSkipManifest.py
python pipeline/stage2_review/Step2_ClassifyPages.py
python pipeline/stage2_review/Step3_RepairFix.py
# ... etc
```

### Utilities (Ad-hoc Operations)

Utilities are helper scripts for manual intervention, debugging, or one-off repairs.

**Characteristics**:
- Run on-demand, not as part of normal flow
- Often operate on specific tickers/filings
- May be called by Steps internally
- Used for investigation and recovery

**Location**: `pipeline/utilities/*.py`

**Current utilities and their purposes**:

| Utility | Purpose | When to Use |
|---------|---------|-------------|
| `ReOCR.py` | Re-run OCR on specific pages (outputs to `artifacts/utilities/` with date stamps) | After Step 8 identifies quality issues |
| `ReExtract.py` | Re-extract statements for specific filings | After fixing source markdown |
| `ReExtractPL.py` | Re-extract P&L only | P&L-specific failures |
| `ReExtractBS.py` | Re-extract BS only | BS-specific failures |
| `ReExtractCF.py` | Re-extract CF only | CF-specific failures |
| `reextract_base.py` | Base class for re-extraction utilities | Shared logic |
| `ExtractPLFromPDF.py` | Extract P&L directly from PDF | OCR fundamentally broken |
| `ExtractBSFromPDF.py` | Extract BS directly from PDF | OCR fundamentally broken |
| `ExtractCFFromPDF.py` | Extract CF directly from PDF | OCR fundamentally broken |
| `trace_statement_pages.py` | Find which pages contain a statement | Debugging missing data |
| `validate_3m_vs_annual.py` | Check quarterly rollup to annual | Post-extraction validation |
| `normalize_pl_columns.py` | Fix column naming issues | Schema normalization |
| `CreateMappings.py` | Create field mappings | Schema mapping |
| `calculate_52week.py` | Calculate 52-week metrics | Market data |
| `fetch_psx_current_shares.py` | Fetch shares outstanding | Market data |
| `populate_market_data.py` | Populate market data | Market data |
| `upload_market_data.py` | Upload market data to D1 | Market data upload |
| `upload_sql.py` | Generic SQL upload utility | Database operations |

### When Steps Call Utilities

Some Steps invoke utilities internally:

```python
# In Step4_RepairStatements.py
from pipeline.utilities.ReExtract import reextract_filing

for filing in failed_filings:
    reextract_filing(filing, statement_type="BS")
```

The Step handles orchestration (which filings, checkpointing, logging), while the utility handles the actual work.

### Decision: Step or Utility?

| If you need to... | Use |
|-------------------|-----|
| Run as part of normal pipeline | Step |
| Process all items of a type | Step |
| Track progress with checkpoint | Step |
| Fix a specific filing manually | Utility |
| Debug why something failed | Utility |
| One-off repair after investigation | Utility |

**Rule of thumb**: If it's in the critical path and runs on every pipeline execution, it's a Step. If it's for recovery/debugging, it's a Utility.

---

## Management Compensation & Multi-Year Extraction

These follow the same pattern as financial statements but with different page sources and schemas.

### Management Compensation

**Source**: Pages tagged `useful_note` with "compensation" / "remuneration" / "directors" keywords.

**Flow**:
```
Stage 2: Tag compensation pages in classification.jsonl
    ↓
Stage 3: Extract compensation tables (same first-pass → repair pattern)
    → JSONify with schema: {name, designation, salary, bonus, benefits, total}
    → QC: totals add up, no missing required fields
    ↓
Stage 4: Flatten to compensation.jsonl → Upload
```

**QC Checks**:
- `total_compensation = salary + bonus + benefits + other`
- All directors have designation
- Year-over-year sanity (no 100x changes)

### Multi-Year Summary

**Source**: Pages detected by `has_multiyear_analysis()` — tables with 3+ years of data.

**Flow**:
```
Stage 2: Detect and SKIP from statement extraction (already in skip_manifest)
    ↓
Stage 3: Separate extraction pass for multi-year pages
    → Extract key metrics across years (revenue, profit, assets, etc.)
    → JSONify with schema: {metric, year_1, year_2, ..., year_N}
    → QC: Values should roughly match annual statements for overlapping years
    ↓
Stage 4: Flatten to multiyear.jsonl → Upload
```

**QC Checks**:
- Cross-validate against extracted annual statements where years overlap
- Flag large discrepancies for investigation

---

## Repository Reorganization Instructions

When reorganizing the repo to match V3, follow these steps in order.

### Phase 1: Folder Structure

**Create new folders**:
```bash
mkdir -p artifacts/stage1 artifacts/stage2 artifacts/stage3 artifacts/stage4
mkdir -p statements_final
mkdir -p database_rows
```

**Rename stage folders** (keep "ingest"):
```bash
# Current → New
pipeline/stage1_ingest/     # Keep as-is
pipeline/stage2_review/     # Keep as-is
pipeline/stage3_extract/    # Keep as-is
pipeline/stage4_publish/    # Keep as-is
```

**Archive deprecated data folders** (migrate data when fixed):
```bash
mkdir -p archive/legacy_extraction
mv extracted_pl/ archive/legacy_extraction/
mv extracted_bs/ archive/legacy_extraction/
mv extracted_statements/ archive/legacy_extraction/
mv pl_json/ archive/legacy_extraction/
mv statements_bs_json/ archive/legacy_extraction/
mv statements_json/ archive/legacy_extraction/
mv compiled_statements_*/ archive/legacy_extraction/
mv standardized_statements_*/ archive/legacy_extraction/
```

### Phase 2: Script Consolidation

**Stage 1 scripts** (target state):
```
pipeline/stage1_ingest/
├── Step1_DownloadPDFs.py
├── Step2_QCFilingCoverage.py
├── Step3_SplitPages.py
├── Step4_ConvertToJPG.py
├── Step5_UploadToR2.py
├── Step6_RunOCR.py
├── Step7_BuildSkipManifest.py
├── Step8_QCExtraction.py
└── Step9_BuildClassificationManifest.py
```

**Stage 2 scripts** (target state):
```
pipeline/stage2_review/
├── Step1_BuildSkipManifest.py
├── Step2_ClassifyPages.py
├── Step3_RepairFix.py
├── Step4_RepairReOCR.py
├── Step5_FinalCorruptionCheck.py
└── Step6_BuildExtractionManifest.py
```

**Stage 3 scripts** (target state):
```
pipeline/stage3_extract/
├── Step1_ExtractStatements.py
├── Step2_JSONify.py
├── Step3_QCArithmetic.py
├── Step4_RepairStatements.py
├── Step5_PDFFallback.py
├── Step6_CrossValidate.py
├── Step7_ValidatePeriods.py
├── Step8_CalculateDerived.py
├── Step9_CompileFinal.py
├── Step10_ExtractCompensation.py      # Management compensation
├── Step11_ExtractMultiYear.py         # Multi-year summaries
└── Step12_QCFinal.py                  # Final pass rates
```

**Stage 4 scripts** (target state):
```
pipeline/stage4_publish/
├── Step1_Flatten.py
├── Step2_NormalizeUnits.py
├── Step3_FinalQC.py
└── Step4_Upload.py
```

**Utilities to keep**:
```
pipeline/utilities/
├── ReOCR.py                 # Called after Stage 1 Step 8; outputs to artifacts/utilities/
├── ReExtract.py             # Called by Stage 3 Step 4
├── ExtractFromPDF.py        # Merge PL/BS/CF PDF extractors into one
├── trace_statement_pages.py # Debugging utility
└── validate_3m_vs_annual.py # QC utility
```

**Scripts to archive** (replaced by new steps):
```bash
mkdir -p archive/legacy_scripts
mv pipeline/stage3_extract/Step2a_ExtractPL.py archive/legacy_scripts/
mv pipeline/stage3_extract/Step2b_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step2c_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step2d_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step2e_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step2f_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step2g_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step4a_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step4b_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step5a_* archive/legacy_scripts/
mv pipeline/stage3_extract/Step5b_* archive/legacy_scripts/
# ... etc (all the fragmented extraction scripts)
```

### Phase 3: Shell Scripts

**Update `scripts/run_ingest_and_review.sh`**:
```bash
#!/bin/bash
set -e

echo "=== Stage 1: Ingest ==="
python pipeline/stage1_ingest/Step1_DownloadPDFs.py
python pipeline/stage1_ingest/Step2_SplitPages.py
python pipeline/stage1_ingest/Step3_UploadToR2.py
python pipeline/stage1_ingest/Step4_RunOCR.py
python pipeline/stage1_ingest/Step5_QCCoverage.py
python pipeline/stage1_ingest/Step6_QCCorruption.py
python pipeline/stage1_ingest/Step7_ReOCR.py

echo "=== Stage 2: Review ==="
python pipeline/stage2_review/Step1_BuildSkipManifest.py
python pipeline/stage2_review/Step2_ClassifyPages.py
python pipeline/stage2_review/Step3_RepairFix.py
python pipeline/stage2_review/Step4_RepairReOCR.py
python pipeline/stage2_review/Step5_FinalCorruptionCheck.py
python pipeline/stage2_review/Step6_BuildExtractionManifest.py

echo "=== Stages 1-2 Complete ==="
```

**Update `scripts/run_extract.sh`**:
```bash
#!/bin/bash
set -e

echo "=== Stage 3: Extract ==="
python pipeline/stage3_extract/Step1_ExtractStatements.py
python pipeline/stage3_extract/Step2_JSONify.py
python pipeline/stage3_extract/Step3_QCArithmetic.py
python pipeline/stage3_extract/Step4_RepairStatements.py
python pipeline/stage3_extract/Step5_PDFFallback.py
python pipeline/stage3_extract/Step6_CrossValidate.py
python pipeline/stage3_extract/Step7_ValidatePeriods.py
python pipeline/stage3_extract/Step8_CalculateDerived.py
python pipeline/stage3_extract/Step9_CompileFinal.py
python pipeline/stage3_extract/Step10_ExtractCompensation.py
python pipeline/stage3_extract/Step11_ExtractMultiYear.py
python pipeline/stage3_extract/Step12_QCFinal.py

echo "=== Stage 3 Complete ==="
```

**Update `scripts/run_publish.sh`**:
```bash
#!/bin/bash
set -e

echo "=== Stage 4: Publish ==="
python pipeline/stage4_publish/Step1_Flatten.py
python pipeline/stage4_publish/Step2_NormalizeUnits.py
python pipeline/stage4_publish/Step3_FinalQC.py
python pipeline/stage4_publish/Step4_Upload.py

echo "=== Stage 4 Complete ==="
```

### Phase 4: Checkpoint Infrastructure

**Create shared checkpoint module**:
```
pipeline/shared/
├── __init__.py
├── checkpoint.py      # Checkpoint class with load/save/update
├── incremental.py     # should_process() logic
└── constants.py       # ARTIFACTS path, stage numbers
```

**Each Step imports from shared**:
```python
from pipeline.shared.checkpoint import Checkpoint
from pipeline.shared.incremental import should_process

def main():
    checkpoint = Checkpoint.load("Step2_ClassifyPages", stage=2)

    for item in items:
        if not should_process(item.id, output_path(item)):
            checkpoint.skip(item.id)
            continue

        # ... process item ...
        checkpoint.complete(item.id)

    checkpoint.finalize()
```

### Phase 5: Validation

After reorganization, verify:

```bash
# 1. All steps run without import errors
for stage in 1 2 3 4; do
    for script in pipeline/stage${stage}_*/Step*.py; do
        python -c "import ${script%.py}" 2>&1 | grep -i error
    done
done

# 2. Artifacts folders exist and are writable
for stage in 1 2 3 4; do
    touch artifacts/stage${stage}/test && rm artifacts/stage${stage}/test
done

# 3. Shell scripts are executable
chmod +x scripts/run_*.sh

# 4. No orphaned imports (references to archived scripts)
grep -r "from pipeline.stage3_extract.Step2a" pipeline/ --include="*.py"
grep -r "from pipeline.stage3_extract.Step5a" pipeline/ --include="*.py"
```

### Migration Checklist

When executing reorganization:

- [ ] Create new folder structure
- [ ] Archive legacy data folders (don't delete until verified)
- [ ] Rename/consolidate scripts to match target state
- [ ] Archive deprecated scripts
- [ ] Update shell scripts
- [ ] Create shared checkpoint module
- [ ] Update all steps to use shared checkpoint
- [ ] Run validation checks
- [ ] Update CLAUDE.md to reference V3 design
- [ ] Commit with message: "Reorganize repo to match PIPELINE_DESIGN_V3"
