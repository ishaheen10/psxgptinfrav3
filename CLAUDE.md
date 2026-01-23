# PSX Financial Data Pipeline V3

This is the clean V3 implementation of the PSX financial data extraction pipeline.

## Pipeline Overview

```
markdown_pages/ → Classification → Repair → Extract Statements → Extract Other → Publish
```

**5 Stages:**
- **Stage 1: Ingest** - Download PDFs, OCR to markdown (already done - symlinked)
- **Stage 2: Review** - Classify pages, repair OCR issues, build extraction manifest
- **Stage 3: Extract Statements** - Pull P&L, Balance Sheet, Cash Flow; two-layer QC (formula + semantic)
- **Stage 4: Extract Other** - Pull Compensation + Multi-Year summaries; validate source match
- **Stage 5: Publish** - Flatten, run pre-upload QC, upload to Cloudflare D1

## Directory Structure

```
psxgptinfrav3/
├── pipeline/
│   ├── shared/                     # Checkpoint & incremental processing
│   ├── stage1_ingest/              # PDF → OCR
│   ├── stage2_review/              # Classification + Repair
│   ├── stage3_extract_statements/  # P&L, BS, CF extraction (separate per type)
│   ├── stage4_extract_other/       # Compensation, Multi-Year extraction
│   ├── stage5_publish/             # Flatten + Upload to D1
│   └── utilities/                  # Ad-hoc repair scripts
├── artifacts/                      # QC reports, manifests, checkpoints
│   ├── stage1/
│   ├── stage2/
│   ├── stage3/
│   ├── stage4/
│   └── stage5/
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
├── markdown_pages/                 # Symlink to main repo's OCR output
├── scripts/                        # Shell scripts to run pipeline
├── docs/                           # Design documentation
└── .claude/skills/                 # Claude Code skills
    └── stage3-qc-loop/             # QC automation skill
```

## Key Files

| File | Purpose |
|------|---------|
| `artifacts/stage2/step6_extraction_manifest.json` | Stage 3 input: which pages have which statements |
| `artifacts/stage2/step1_classification.jsonl` | Per-page classification results |
| `artifacts/stage3/step2_statement_pages.json` | Page assignments for each ticker/filing |
| `artifacts/stage3/step4_qc_*_extraction.json` | Formula QC results |
| `artifacts/stage3/step6_qc_*_results.json` | Semantic QC results |
| `artifacts/stage3/step7_arithmetic_allowlist*.json` | Reviewed exceptions |
| `docs/PIPELINE_DESIGN_V3.md` | Full design document |

## Stage 3 Workflow (Statement Extraction)

Each statement type (PL, BS, CF) flows through these steps:

```
Step1 Build Manifests → Step3 Extract → Step4 Formula QC → Step5 JSONify → Step6 Semantic QC → Step7 Derive Quarters*

* Derive Quarters applies to PL and CF only (cumulative → quarterly)
```

**Two-Layer QC:**
- **Step4 (Formula QC)**: Validates Ref formulas (C=A+B), checks source match rate
- **Step6 (Semantic QC)**: Critical fields present, accounting equations, monotonicity

## Safe to Run (no LLM cost)

```bash
# Stage 1 QC
python pipeline/stage1_ingest/Step2_QCFilingCoverage.py
python pipeline/stage1_ingest/Step7_BuildSkipManifest.py
python pipeline/stage1_ingest/Step8_QCExtraction.py

# Stage 2 deterministic steps
python pipeline/stage2_review/Step1b_DeterministicQC.py
python pipeline/stage2_review/Step2_BuildRepairManifest.py
python pipeline/stage2_review/Step5_FinalCorruptionCheck.py
python pipeline/stage2_review/Step6_BuildExtractionManifest.py

# Stage 3 QC (no extraction)
python pipeline/stage3_extract_statements/Step4_QCPL_Extraction.py
python pipeline/stage3_extract_statements/Step4_QCBS_Extraction.py
python pipeline/stage3_extract_statements/Step4_QCCF_Extraction.py
python pipeline/stage3_extract_statements/Step5_JSONifyPL.py
python pipeline/stage3_extract_statements/Step6_QCPL.py
python pipeline/stage3_extract_statements/Step7_DeriveQuartersPL.py
```

## Requires User Approval (LLM calls, costs money)

```bash
# DeepSeek classification
python pipeline/stage2_review/Step1_ClassifyPages.py

# DeepSeek repair
python pipeline/stage2_review/Step3_RepairFix.py

# Gemini re-OCR
python pipeline/stage2_review/Step4_RepairReOCR.py
python pipeline/utilities/ReOCR.py --manifest path/to/manifest.json

# DeepSeek extraction
python pipeline/stage3_extract_statements/Step3_ExtractPL.py
python pipeline/stage3_extract_statements/Step3_ExtractBS.py
python pipeline/stage3_extract_statements/Step3_ExtractCF.py
```

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

Checkpoints are stored in `artifacts/stage{N}/step{N}_checkpoint.json`.

## Environment Variables

Required in `.env`:
- `DEEPSEEK_API_KEY` - For classification, extraction, and text repair
- `GEMINI_API_KEY` - For OCR repair (vision)
- `MISTRAL_API_KEY` - For initial OCR (Stage 1)
- `CLOUDFLARE_ACCOUNT_ID` - R2 upload
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

## Design Principles

1. **Fail fast, fix once** - Detect issues early, repair in one pass
2. **Statement-type isolation** - Process PL, BS, CF separately
3. **Two-layer QC** - Formula QC (Step4) + Semantic QC (Step6) catch different failure modes
4. **Incremental by default** - Never re-process what's already done
5. **Source tracing built-in** - Every value tracks its page origin
6. **Human-in-loop for last 2%** - Use allowlists for known exceptions

See `docs/PIPELINE_DESIGN_V3.md` for full design.

## Prompt Engineering Principles

When writing or modifying LLM extraction prompts:

1. **Focus on WHAT, not HOW** - State the principle/rule, not step-by-step implementation
2. **Keep instructions generalizable** - Avoid overly specific examples that may not transfer
3. **Trust the model** - Provide context and constraints, let the model figure out execution
4. **Concise over verbose** - A clear one-liner beats a detailed paragraph with examples

## Re-extraction Guidelines

**CRITICAL: Always use `--manifest` when re-extracting files to avoid unnecessary API costs.**

When re-extracting a subset of files:

1. **Always use --manifest** - Create a JSON list of filenames and pass via `--manifest path/to/manifest.json`. Never run extraction without a manifest when re-extracting.
2. **Match workers to files** - Use `--workers N` where N equals the number of files to re-extract for maximum parallelism
3. **Delete target files first** - Remove only the specific extracted files listed in your manifest
4. **Manifest format** - Simple JSON array of filenames: `["TICKER_period_section.md", ...]`

Example:
```bash
# Create manifest of files to re-extract
echo '["AABS_annual_2022_consolidated.md", "LUCK_quarterly_2024-03-31_consolidated.md"]' > reextract.json

# Delete those specific files
python3 -c "import json; [Path(f'data/extracted_bs/{f}').unlink() for f in json.load(open('reextract.json')) if Path(f'data/extracted_bs/{f}').exists()]"

# Re-extract with manifest
python pipeline/stage3_extract_statements/Step3_ExtractBS.py --manifest reextract.json --workers 2
```

## Unit Verification Guidelines

**CRITICAL: Never use arbitrary math thresholds to classify unit issues as false positives.**

When investigating unit variations or cross-period normalization (CPN) failures:

1. **Always compare source data** - Check the BS extraction `unit_type` against the P&L extraction `unit_type` for the same ticker/period
2. **Check actual extractions, not computed ratios** - Look at what the extraction declared (rupees vs thousands vs millions), not whether values are "within 2x" or ">100x"
3. **Verify against source PDFs when needed** - If extraction units conflict, check the actual PDF to determine which is correct

Example verification:
```python
# CORRECT: Compare declared units across statements
bs_unit = bs_data["unit_type"]  # e.g., "rupees"
pl_unit = pl_data["unit_type"]  # e.g., "thousands"
if bs_unit != pl_unit:
    # Real mismatch - one extraction has wrong unit

# WRONG: Using arbitrary math thresholds
if value > median * 100:
    # "Must be wrong units" - DON'T DO THIS
```

If BS and P&L extractions for the same period show different units, the extraction with the incorrect unit needs to be re-extracted with corrected unit detection.

## Source Verification Guidelines

**CRITICAL: Never conclude the source PDF has errors based only on markdown files.**

The `markdown_pages/` directory contains OCR output which can have errors. When investigating data issues:

1. **Always check pdf_pages/ for the actual source** - The `pdf_pages/` directory contains the original PDF page images. Use these to verify what the source document actually shows.
2. **OCR errors are common** - Sign errors, missing parentheses, misread digits, and formatting issues frequently occur in OCR output
3. **Don't blame the source prematurely** - If extracted values don't add up, first check if the OCR/markdown is correct before concluding the source PDF has errors

Verification workflow:
```bash
# 1. Find the source page number from manifest
python3 -c "import json; print(json.load(open('artifacts/stage3/step2_statement_pages.json'))['TICKER']['period']['consolidated']['CF'])"

# 2. View the actual PDF page
open pdf_pages/TICKER/YEAR/TICKER_Filing_Period/page_NNN.png
```

Only after confirming the PDF page matches the markdown should you conclude the source document has data quality issues.

## Skills

### /stage3-qc-loop

Run automated QC loop for any statement type (BS, CF, PL). Target: 99%+ pass rate.

```
/stage3-qc-loop BS
```

Triage categories:
- Source match < 50% → Wrong page → Fix manifest
- Source match 50-90% → OCR issue → Re-OCR
- Source match > 95%, formula fail → Code bug → Flag for review

See `docs/AUTONOMOUS_QC_DESIGN.md` for the vision of fully autonomous QC.
