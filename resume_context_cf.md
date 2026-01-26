# Cash Flow (CF) Resume Context

## Current State (Jan 23, 2026 - Session 3 Final)

### Pipeline Status
- **BS**: Complete - 133 pass, 5 warn, 0 fail
- **PL**: Complete - 97.8% clean
- **CF**: Semantic QC at **99.0% pass rate** (44 failures remaining)

### CF QC Status (Step6_QCCF)
- **Tickers processed**: 138
- **Tickers passed**: 107 (77.5%)
- **Semantic equations**: 4283 passed, 44 failed (**99.0% pass rate**)
- **Tolerance**: 5%

### Session 3 Actions & Results

#### 1. JSONify Empty Column Bug (FIXED)
Fixed issue where prior-year values were assigned to wrong periods when current-year column was empty.
- **Impact**: Reduced failures from 61 → 53

#### 2. Re-OCR (3 pages)
Re-OCR'd pages with note references in value columns:
- ATRL_quarterly_2022-12-31 page 15
- PPL_quarterly_2024-03-31 page 24
- IBLHL_quarterly_2022-12-31 page 10

#### 3. Re-Extraction (10 files)
Re-extracted files with known issues:
- AKBL_quarterly_2025-09-30 (consolidated + unconsolidated)
- AVN_quarterly_2025-03-31_unconsolidated
- NETSOL_quarterly_2025-09-30_unconsolidated
- ASTL_annual_2023_consolidated
- THALL_quarterly_2023-12-31_unconsolidated
- NCPL_quarterly_2024-12-31_consolidated
- ATRL_quarterly_2022-12-31_unconsolidated
- PPL_quarterly_2024-03-31_unconsolidated
- IBLHL_quarterly_2022-12-31_consolidated

**Result**: 6 tickers fixed (ASTL, ATRL, IBLHL, NCPL, NETSOL, PPL)

### Progress Summary

| Step | Failures | Pass Rate | Action |
|------|----------|-----------|--------|
| Start of session | 61 | 98.6% | - |
| After JSONify fix | 53 | 98.8% | Empty column bug |
| After re-OCR + re-extract | 44 | 99.0% | 10 files re-processed |

### Remaining Failures (44 total)

#### Large (>100% diff) - 4 failures
All confirmed **source document errors** (arithmetic inconsistencies in source PDF):

| Ticker | Diff % | Issue |
|--------|--------|-------|
| AKBL | 785%, 658% | Source CFF total ≠ sum of components (6M discrepancy) |
| AVN | 286%, 107% | Source CFO wrong (61,081 vs -43,922 math error) |

#### Medium (20-100% diff) - 24 failures
Various tickers with extraction or source issues. Key ones:
- ENGROH: 94%, 97% - likely extraction
- NESTLE: 93% - unknown
- GVGL: 94% - unknown
- MCB: 90%, 80% (old periods 2014-2015) - likely old data issues
- EPCL: 86% - unknown

#### Small (10-20% diff) - 5 failures
Near tolerance threshold.

#### Borderline (<10% diff) - 13 failures
Could pass with slight tolerance increase. Includes:
- BAFL: 7.9%
- BOP: 9.1%
- FATIMA: 7.8%
- ICI: 8.9%
- MCB: 5.2%, 5.3%
- PIBTL: 8.6%
- PSEL: 8.2%
- SHFA: 8.8%
- THCCL: 7.1%

### Files Modified This Session
- `pipeline/stage3_extract_statements/Step5_JSONifyCF.py` - Empty column fix (lines 209-215)

### Remaining Issues Requiring Manual Review

1. **AKBL, AVN**: Source document arithmetic errors - need allowlist
2. **Medium failures (24)**: Mix of extraction and source issues
3. **Borderline failures (13)**: Could consider allowlist for known OK cases

### Key Commands

```bash
# Run CF semantic QC
python3 pipeline/stage3_extract_statements/Step6_QCCF.py

# Run full CF pipeline (QC -> JSONify -> Semantic QC)
python3 pipeline/stage3_extract_statements/Step4_QCCF_Extraction.py && \
python3 pipeline/stage3_extract_statements/Step5_JSONifyCF.py && \
python3 pipeline/stage3_extract_statements/Step6_QCCF.py

# Re-extract specific files
echo '["FILE.md"]' > manifest.json
rm data/extracted_cf/FILE.md
python3 pipeline/stage3_extract_statements/Step3_ExtractCF.py --manifest manifest.json

# Re-OCR specific pages
# Create manifest: {"pages": [{"relative_path": "TICKER/YEAR/FOLDER/page_NNN.md"}]}
python3 -m pipeline.utilities.ReOCR --manifest manifest.json
```

### Next Steps (if further improvement needed)

1. **Create allowlist** for AKBL, AVN (source document errors)
2. **Investigate medium failures** - prioritize by impact
3. **Consider borderline allowlist** for failures <10% with known explanations
