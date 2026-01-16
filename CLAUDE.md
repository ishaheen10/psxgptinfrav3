# PSX Financial Data Pipeline V3

This is the clean V3 implementation of the PSX financial data extraction pipeline.

## Pipeline Overview

```
markdown_pages/ → Classification → Repair → Extract Statements → Extract Other → Publish
```

**5 Stages:**
- **Stage 1: Ingest** - Download PDFs, OCR to markdown (already done - symlinked)
- **Stage 2: Review** - Classify pages, repair OCR issues, build extraction manifest
- **Stage 3: Extract Statements** - Pull P&L, Balance Sheet, Cash Flow; validate arithmetic
- **Stage 4: Extract Other** - Pull Compensation + Multi-Year summaries; validate source match
- **Stage 5: Publish** - Flatten, unify, upload to Cloudflare D1

## Quick Start

```bash
# Stage 1 (QC only - markdown_pages/ already exists via symlink)
./scripts/run_stage1.sh

# Stage 2 (Classification + Repair)
./scripts/run_stage2.sh

# Stage 3 (Extract Statements)
./scripts/run_stage3.sh

# Stage 4 (Extract Other: Compensation + Multi-Year)
./scripts/run_stage4.sh

# Stage 5 (Publish)
./scripts/run_stage5.sh

# Or run Stages 1-2 together
./scripts/run_all.sh
```

## Directory Structure

```
psxgptinfrav3/
├── pipeline/
│   ├── shared/                # Checkpoint & incremental processing
│   ├── stage1_ingest/         # Steps 1-8 (Ingest)
│   ├── stage2_review/         # Steps 1-7 (Review)
│   ├── stage3_extract_statements/  # P&L, BS, CF extraction
│   ├── stage4_extract_other/  # Compensation, Multi-Year extraction
│   ├── stage5_publish/        # Flatten + Upload to D1
│   └── utilities/             # Ad-hoc repair scripts
├── artifacts/                 # QC reports, manifests, checkpoints
│   ├── stage1/
│   ├── stage2/
│   ├── stage3/
│   ├── stage4/
│   └── stage5/
├── data/                      # Extracted data
│   ├── extracted_compensation/
│   ├── extracted_multiyear/
│   ├── json_compensation/
│   └── json_multiyear/
├── markdown_pages/            # Symlink to main repo's OCR output
├── scripts/                   # Shell scripts to run pipeline
└── docs/                      # Design documentation
```

## Key Files

| File | Purpose |
|------|---------|
| `artifacts/stage2/extraction_manifest.json` | Stage 3 input: which pages have which statements |
| `artifacts/stage2/classification.jsonl` | Per-page classification results |
| `artifacts/stage2/skip_manifest.json` | Pages to skip (Urdu, edges, corrupted) |
| `docs/PIPELINE_DESIGN_V3.md` | Full design document |

## Critical: Verification Protocol

**Every change must be verified.** Use QC scripts before proceeding.

### Safe to Run (no LLM cost)

```bash
# Stage 1 QC
python pipeline/stage1_ingest/Step6_QCCoverage.py
python pipeline/stage1_ingest/Step7_QCCorruption.py

# Stage 2 deterministic steps
python pipeline/stage2_review/Step1_BuildSkipManifest.py
python pipeline/stage2_review/Step3_ClassifyRepairs.py
python pipeline/stage2_review/Step6_FinalCorruptionCheck.py
python pipeline/stage2_review/Step7_BuildExtractionManifest.py
```

### Requires User Approval (LLM calls, costs money)

```bash
# DeepSeek classification
python pipeline/stage2_review/Step2_ClassifyPages.py

# DeepSeek repair
python pipeline/stage2_review/Step4_RepairFix.py

# Gemini re-OCR
python pipeline/stage2_review/Step5_RepairReOCR.py
python pipeline/stage1_ingest/Step8_ReOCR.py
```

## Checkpoint System

All steps use the shared checkpoint module for resume capability:

```python
from pipeline.shared import Checkpoint

checkpoint = Checkpoint.load("Step2_ClassifyPages", stage=2)
checkpoint.set_total(len(items))

for item in items:
    if item.id in checkpoint.completed_items:
        continue
    # ... process ...
    checkpoint.complete(item.id)

checkpoint.finalize()
```

Checkpoints are stored in `artifacts/stage{N}/checkpoint.json`.

## Environment Variables

Required in `.env`:
- `DEEPSEEK_API_KEY` - For classification and text repair
- `GEMINI_API_KEY` - For OCR repair (vision)
- `MISTRAL_API_KEY` - For initial OCR (Stage 1)
- Cloudflare R2 credentials (for R2 upload)

## Design Principles

1. **Fail fast, fix once** - Detect issues early, repair in one pass
2. **One LLM call where possible** - Combine summarize + classify + score
3. **Incremental by default** - Never re-process what's already done
4. **Source tracing built-in** - Every value tracks its page origin
5. **Human-in-loop for last 2%** - Automated loops hit diminishing returns

See `docs/PIPELINE_DESIGN_V3.md` for full design.
