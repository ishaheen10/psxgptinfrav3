# Pipeline Design V3

> Current state of the PSX Financial Data Extraction Pipeline as of January 2026.

## Design Principles

1. **Fail fast, fix once**: Detect issues early, repair in one pass, don't re-process
2. **Statement-type isolation**: Process PL, BS, CF separately through extraction → QC → JSONify → semantic QC
3. **Two-layer QC**: Formula validation (Step4) catches extraction errors; Semantic QC (Step6) catches accounting issues
4. **Human-in-loop for the last 2%**: Automated loops hit diminishing returns; use allowlists for known exceptions
5. **Source tracing built-in**: Every extracted value tracks its page origin
6. **Incremental by default**: Never re-process what's already done; resume from checkpoints

---

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: INGEST                                                             │
│ PDF → Split → JPG → R2 Upload → OCR → Skip Manifest → QC → Classification   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: REVIEW                                                             │
│ Classify Pages → Deterministic QC → Build Repair Manifest → Fix/ReOCR      │
│ → Final Check → Build Extraction Manifest                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: EXTRACT STATEMENTS (PL, BS, CF - processed separately)             │
│ Build Manifests → Extract → Formula QC → JSONify → Semantic QC              │
│ → Derive Quarters (PL/CF only)                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: EXTRACT OTHER (Compensation + Multi-Year)                          │
│ Extract → QC → JSONify → Clean                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 5: PUBLISH                                                            │
│ Flatten → QC Pre-Upload → Upload to D1                                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
psxgptinfrav3/
├── pipeline/
│   ├── shared/                     # Checkpoint & incremental processing
│   │   ├── checkpoint.py
│   │   ├── incremental.py
│   │   └── constants.py
│   ├── stage1_ingest/              # PDF → OCR
│   ├── stage2_review/              # Classification + Repair
│   ├── stage3_extract_statements/  # PL, BS, CF extraction
│   │   └── archive/                # Deprecated scripts
│   ├── stage4_extract_other/       # Compensation + Multi-Year
│   ├── stage5_publish/             # Flatten + Upload
│   ├── market/                     # Market data utilities
│   └── utilities/                  # Ad-hoc repair scripts
├── artifacts/                      # QC reports, manifests, checkpoints
│   ├── stage1/
│   ├── stage2/
│   ├── stage3/
│   ├── stage4/
│   ├── stage5/
│   └── utilities/
├── data/                           # Extracted data
│   ├── extracted_pl/               # Raw P&L extractions (.md)
│   ├── extracted_bs/               # Raw BS extractions (.md)
│   ├── extracted_cf/               # Raw CF extractions (.md)
│   ├── json_pl/                    # P&L in JSON format
│   ├── json_bs/                    # BS in JSON format
│   ├── json_cf/                    # CF in JSON format
│   ├── quarterly_pl/               # Derived quarterly P&L
│   ├── quarterly_cf/               # Derived quarterly CF
│   ├── flat/                       # Flattened JSONL for upload
│   ├── extracted_compensation/
│   ├── extracted_multiyear/
│   ├── json_compensation/
│   └── json_multiyear/
├── markdown_pages/                 # Symlink to OCR output
├── scripts/                        # Shell scripts to run pipeline
├── docs/                           # Design documentation
└── .claude/skills/                 # Claude Code skills
    └── stage3-qc-loop/
```

---

## Stage 1: INGEST

**Goal**: Convert PDFs to clean markdown with complete quarterly coverage.

### Scripts

| Step | Script | Purpose | LLM? |
|------|--------|---------|------|
| 1 | `Step1_DownloadPDFs.py` | Download from PSX | No |
| 2 | `Step2_QCFilingCoverage.py` | Check filing completeness | No |
| 3 | `Step3_SplitPages.py` | PDF → individual pages | No |
| 4 | `Step4_ConvertToJPG.py` | Create thumbnails | No |
| 5 | `Step5_UploadToR2.py` | Upload to Cloudflare R2 | No |
| 6 | `Step6_RunOCR.py` | Mistral OCR | Yes |
| 7 | `Step7_BuildSkipManifest.py` | Build skip list (Urdu, edges) | No |
| 8 | `Step8_QCExtraction.py` | Detect OCR quality issues | No |
| 9 | `Step9_BuildClassificationManifest.py` | Build manifest for Stage 2 | No |

### Key Artifacts

| File | Purpose |
|------|---------|
| `artifacts/stage1/step2_qc_filing_coverage.json` | Missing filings report |
| `artifacts/stage1/step7_skip_manifest.json` | Pages to skip |
| `artifacts/stage1/step8_qc_issues.json` | OCR quality issues |
| `artifacts/stage1/step9_classification_manifest.json` | Pages for Stage 2 |

---

## Stage 2: REVIEW

**Goal**: Classify pages, repair OCR issues, build extraction manifest.

### Scripts

| Step | Script | Purpose | LLM? |
|------|--------|---------|------|
| 1 | `Step1_ClassifyPages.py` | DeepSeek classification | Yes |
| 1b | `Step1b_DeterministicQC.py` | Structural OCR checks (concatenated columns, etc.) | No |
| 2 | `Step2_BuildRepairManifest.py` | Route pages by extraction_score | No |
| 3 | `Step3_RepairFix.py` | DeepSeek markdown repair | Yes |
| 4 | `Step4_RepairReOCR.py` | Gemini PDF transcription | Yes |
| 5 | `Step5_FinalCorruptionCheck.py` | Verify repairs worked | No |
| 6 | `Step6_BuildExtractionManifest.py` | Build manifest for Stage 3 | No |

### Key Artifacts

| File | Purpose |
|------|---------|
| `artifacts/stage2/step1_classification.jsonl` | Per-page classification |
| `artifacts/stage2/step1b_deterministic_qc.json` | Structural QC results |
| `artifacts/stage2/step2_repair_manifest.json` | Repair routing |
| `artifacts/stage2/step6_extraction_manifest.json` | Statement pages per filing |

---

## Stage 3: EXTRACT STATEMENTS

**Goal**: Extract P&L, BS, CF with source tracing, validate arithmetic, achieve 99%+ pass rate.

### Architecture: Statement-Type Isolation

Each statement type (PL, BS, CF) flows through the same steps independently:

```
Build Manifests → Extract → Formula QC → JSONify → Semantic QC → Derive Quarters*

* Derive Quarters applies to PL and CF only (cumulative → quarterly)
* BS is point-in-time, no derivation needed
```

### Scripts

| Step | Script | Purpose | LLM? |
|------|--------|---------|------|
| 1 | `Step1_BuildManifests.py` | Build extraction manifests | No |
| 2 | `Step2_ExtractStatementPages.py` | Extract all statement pages | Yes |
| 3 | `Step3_ExtractPL.py` | Extract P&L statements | Yes |
| 3 | `Step3_ExtractBS.py` | Extract Balance Sheets | Yes |
| 3 | `Step3_ExtractCF.py` | Extract Cash Flows | Yes |
| 4 | `Step4_QCPL_Extraction.py` | Formula + source match QC for P&L | No |
| 4 | `Step4_QCBS_Extraction.py` | Formula + source match QC for BS | No |
| 4 | `Step4_QCCF_Extraction.py` | Formula + source match QC for CF | No |
| 5 | `Step5_JSONifyPL.py` | Convert P&L to JSON | No |
| 5 | `Step5_JSONifyBS.py` | Convert BS to JSON | No |
| 5 | `Step5_JSONifyCF.py` | Convert CF to JSON | No |
| 6 | `Step6_QCPL.py` | Semantic QC for P&L | No |
| 6 | `Step6_QCBS.py` | Semantic QC for BS | No |
| 6 | `Step6_QCCF.py` | Semantic QC for CF | No |
| 7 | `Step7_DeriveQuartersPL.py` | Derive quarterly P&L (Q4 = 12M - 9M) | No |
| 7 | `Step7_DeriveQuartersCF.py` | Derive quarterly CF | No |

### Two-Layer QC System

**Layer 1: Formula QC (Step4)**
- Validates Ref formulas: `C=A+B` means row C should equal A + B
- Checks source match: extracted values appear in source markdown
- Tolerance: 5% for formulas, 97% threshold for source match

**Layer 2: Semantic QC (Step6)**
- Critical fields present (total_assets, revenue_net, etc.)
- Accounting equations (Assets = Equity + Liabilities)
- Monotonicity (9M > 6M > 3M for cumulative items)
- Cross-period normalization (detect 1000x unit outliers)

**Key insight**: These catch DIFFERENT failure modes with <1% overlap. Both are required.

### Key Artifacts

| File | Purpose |
|------|---------|
| `artifacts/stage3/step1_statement_manifest.json` | Statement pages to extract |
| `artifacts/stage3/step2_statement_pages.json` | Page assignments |
| `artifacts/stage3/step4_qc_pl_extraction.json` | P&L formula QC results |
| `artifacts/stage3/step4_qc_bs_extraction.json` | BS formula QC results |
| `artifacts/stage3/step4_qc_cf_extraction.json` | CF formula QC results |
| `artifacts/stage3/step6_qc_pl_results.json` | P&L semantic QC results |
| `artifacts/stage3/step6_qc_bs_results.json` | BS semantic QC results |
| `artifacts/stage3/step6_qc_cf_results.json` | CF semantic QC results |
| `artifacts/stage3/step7_arithmetic_allowlist.json` | P&L reviewed exceptions |
| `artifacts/stage3/step7_arithmetic_allowlist_bs.json` | BS reviewed exceptions |
| `artifacts/stage3/step7_arithmetic_allowlist_cf.json` | CF reviewed exceptions |

### Data Folders

| Folder | Contents |
|--------|----------|
| `data/extracted_pl/` | Raw P&L markdown extractions |
| `data/extracted_bs/` | Raw BS markdown extractions |
| `data/extracted_cf/` | Raw CF markdown extractions |
| `data/json_pl/` | P&L in JSON format (one file per ticker) |
| `data/json_bs/` | BS in JSON format |
| `data/json_cf/` | CF in JSON format |
| `data/quarterly_pl/` | Derived quarterly P&L values |
| `data/quarterly_cf/` | Derived quarterly CF values |

---

## Stage 4: EXTRACT OTHER

**Goal**: Extract compensation and multi-year summary data.

### Scripts

| Step | Script | Purpose | LLM? |
|------|--------|---------|------|
| 1 | `Step1_ExtractCompensation.py` | Extract director compensation | Yes |
| 2 | `Step2_QCCompensation.py` | QC compensation extraction | No |
| 3 | `Step3_JSONifyCompensation.py` | Convert to JSON | No |
| 4 | `Step4_ExtractMultiYear.py` | Extract multi-year summaries | Yes |
| 5 | `Step5_QCMultiYear.py` | QC multi-year extraction | No |
| 6 | `Step6_JSONifyMultiYear.py` | Convert to JSON | No |
| 7 | `Step7_CleanMultiYear.py` | Clean multi-year data | No |

### Key Artifacts

| File | Purpose |
|------|---------|
| `artifacts/stage4/step2_compensation_qc.json` | Compensation QC results |
| `artifacts/stage4/step5_multiyear_qc.json` | Multi-year QC results |

---

## Stage 5: PUBLISH

**Goal**: Flatten data, run final QC, upload to Cloudflare D1.

### Scripts

| Step | Script | Purpose |
|------|--------|---------|
| 1 | `Step1_FlattenPL.py` | Flatten P&L to JSONL |
| 1 | `Step1_FlattenBS.py` | Flatten BS to JSONL |
| 1 | `Step1_FlattenCF.py` | Flatten CF to JSONL |
| 2 | `Step2_QCPreUpload.py` | Final sanity checks |
| 3 | `Step3_UploadPL.py` | Upload P&L to D1 |
| 3 | `Step3_UploadStatements.py` | Upload all statements |

### Flat File Format

```json
{
  "ticker": "LUCK",
  "period_end": "2024-03-31",
  "duration": "3M",
  "fiscal_year": 2024,
  "consolidation": "consolidated",
  "canonical_field": "revenue_net",
  "value": 45234567,
  "method": "direct_3M",
  "source_file": "LUCK_quarterly_2024-03-31_consolidated.md",
  "source_pages": [45],
  "source_url": "https://source.psxgpt.com/...",
  "qc_flag": ""
}
```

### QC Flag Types

| Flag | Meaning |
|------|---------|
| (empty) | Clean data |
| `allowlisted: reason` | Reviewed and approved |
| `unexpected_negative: reason` | Negative in direct extraction |
| `derivation_anomaly: reason` | Negative from YTD derivation |

---

## Utilities

### ReOCR Utility

Location: `pipeline/utilities/ReOCR.py`

Re-run OCR on specific pages using Gemini Flash vision fallback.

```bash
# Single page
python pipeline/utilities/ReOCR.py --manifest path/to/manifest.json

# Manifest format
[
  {"ticker": "LUCK", "year": "2024", "filing": "LUCK_Annual_2024", "page": 45}
]
```

### PDF Extraction Fallback

Location: `pipeline/utilities/ExtractFromPDF.py`, `ExtractPLFromPDF.py`

Extract statements directly from PDF images when OCR is fundamentally broken.

### Ad-hoc Fix Scripts

| Script | Purpose |
|--------|---------|
| `fix_bs_formulas.py` | Fix BS formula coverage issues |
| `fix_pl_unit_mismatch.py` | Fix P&L unit mismatch (temporary) |
| `qc_bs_source_match.py` | Source-based BS QC |

---

## Checkpoint System

All steps use the shared checkpoint module for resume capability:

```python
from pipeline.shared.checkpoint import Checkpoint

checkpoint = Checkpoint.load("Step3_ExtractPL", stage=3)
checkpoint.set_total(len(items))

for item in items:
    if item.id in checkpoint.completed_items:
        continue
    # ... process ...
    checkpoint.complete(item.id)

checkpoint.finalize()
```

Checkpoints stored in `artifacts/stage{N}/step{N}_checkpoint.json`.

---

## Re-extraction Guidelines

**CRITICAL: Always use `--manifest` when re-extracting files.**

```bash
# Create manifest of files to re-extract
echo '["AABS_annual_2022_consolidated.md"]' > reextract.json

# Delete those specific files first
rm data/extracted_bs/AABS_annual_2022_consolidated.md

# Re-extract with manifest
python pipeline/stage3_extract_statements/Step3_ExtractBS.py --manifest reextract.json --workers 1
```

---

## Allowlist System

### Two-Tier Approach

**Tier 1: SKIP_FILINGS (in QC scripts)**
- Filing-level skips for known issues
- Used in Step6_QC*.py scripts

```python
SKIP_FILINGS = {
    'EFERT': ['annual_2021'],  # OCR corruption
    'AABS': ['quarterly_2024-12-31'],  # Discrete quarters
}
```

**Tier 2: Allowlist JSON files**
- Period-level exceptions with documented reasons
- Used by DeriveQuarters and Flatten scripts

```json
{
  "allowlist": [
    {
      "ticker": "DAWH",
      "fiscal_year": 2023,
      "consolidation": "consolidated",
      "reason": "Explicit (Restated) label in source"
    }
  ]
}
```

---

## Skills

### /stage3-qc-loop

Automated QC loop for any statement type. Target: 99%+ pass rate.

```
User: /stage3-qc-loop BS
```

**Triage categories:**
| Signal | Category | Action |
|--------|----------|--------|
| Source match < 50% | Wrong page | Fix manifest |
| Source match 50-90% | OCR issue | Re-OCR |
| Source match > 95%, formula fail | Code bug | Flag for review |

---

## Environment Variables

Required in `.env`:
- `DEEPSEEK_API_KEY` - Classification, extraction, repair
- `GEMINI_API_KEY` - ReOCR, PDF vision fallback
- `MISTRAL_API_KEY` - Initial OCR
- `CLOUDFLARE_ACCOUNT_ID` - R2 upload
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

---

## Cost Model

| Provider | Rate | Usage |
|----------|------|-------|
| Mistral OCR | $0.002/page | Stage 1 |
| DeepSeek | $0.24/M input, $0.42/M output | Stages 2-4 |
| Gemini Flash | $0.15/M input, $1.25/M output | ReOCR, PDF fallback |

**Estimated costs (185K pages, 2,400 filings):**
- Stage 1 (OCR): ~$370
- Stage 2 (Classification + Repair): ~$96
- Stage 3 (Extraction): ~$10
- Total: ~$480

---

## Current Status (January 2026)

| Statement | Extraction | Formula QC | Semantic QC | Flatten |
|-----------|------------|------------|-------------|---------|
| P&L | Done | 99%+ | 99%+ | Done |
| BS | Done | 99%+ | 99%+ | Done |
| CF | Done | 99%+ | In progress | Pending |

### Known Gaps

1. **Shell scripts outdated** - `run_stage3.sh` references non-existent scripts
2. **CF QC not complete** - Semantic QC pass rate needs work
3. **Autonomous QC** - Decision engine not yet implemented (see `docs/AUTONOMOUS_QC_DESIGN.md`)

---

## Appendix: Arithmetic QC Equations

### Profit & Loss
```
gross_profit = revenue - cost_of_sales
net_profit = profit_before_tax + taxation
```

### Balance Sheet
```
total_assets = total_equity_and_liabilities
# OR (fallback)
total_assets = total_equity + total_liabilities
```

### Cash Flow
```
net_change_in_cash = cfo + cfi + cff
closing_cash = opening_cash + net_change_in_cash
```

### Tolerance
```python
def within_tolerance(actual, expected, rel_tol=0.05, abs_tol=1000):
    tolerance = max(abs(expected) * rel_tol, abs_tol)
    return abs(actual - expected) <= tolerance
```
