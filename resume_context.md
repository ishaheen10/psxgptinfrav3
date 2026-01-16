# Pipeline Resume Context

**Last Updated:** 2026-01-16
**Status:** Ready for Stage 4 (Publish)

---

## Current State

### Completed Extraction

| Data Type | Status | Records | QC Rate | Notes |
|-----------|--------|---------|---------|-------|
| **P&L Statements** | Done | 3,681 quarters | 99.2% | Direct + derived quarters |
| **Balance Sheet** | Done | - | - | Follows same pattern as P&L |
| **Cash Flow** | Done | - | - | Follows same pattern as P&L |
| **Management Compensation** | Done | 27,575 rows | 99.5% | From original repo |
| **Multi-Year Summaries** | Deferred | 83,471 rows | 98.7% | Missing canonical mapping |

### Focus for Next Iteration

**Statements + Management Compensation only.** Multi-year is deferred.

---

## Key Learnings: Compensation vs Multi-Year

### Management Compensation (Ready for Upload)

Normalization happens **in the LLM prompt** (`Step9_CompileCompensation.py` in original repo):

```
## ROLE MAPPING
- "CEO" / "Chief Executive" → ceo
- "Chairman" → chairman
- "Executive Director" → exec_directors
- "Non-Executive Director" → non_exec_directors
- "Executives" / "Key Management Personnel" → executives

## COLUMN MAPPING
- "Managerial remuneration" / "Salary" → base_salary
- "Bonus" / "Variable" → bonus
- "House rent" / "Housing" → housing
- "Provident fund" / "Gratuity" → retirement
- Medical, Utilities, etc. → other_benefits
```

**Output format:** Structured with canonical fields (`role`, `base_salary`, `bonus`, etc.)

### Multi-Year Summaries (Deferred)

- LLM prompt extracts **verbatim line items** (no canonical mapping)
- Original repo's `multi_year.jsonl` has `line_item` (canonical) + `line_item_original`
- The canonical mapping ("Cash and balances" → `cash_and_bank`) was done **outside pipeline**
- No mapping file or script found - likely done in notebook/interactive session

**Decision:** Skip multi-year for V3 iteration. Would require creating canonical mapping.

---

## Pipeline Structure (V3)

```
pipeline/
├── stage1_ingest/          # OCR (symlinked from original)
├── stage2_review/          # Classification + repair
├── stage3_extract/         # Statement extraction (PL/BS/CF)
├── stage4_extract_other/   # Compensation + Multi-Year
│   ├── Step1_ExtractCompensation.py
│   ├── Step2_QCCompensation.py      # 99.5% source match
│   ├── Step3_JSONifyCompensation.py
│   ├── Step4_ExtractMultiYear.py
│   ├── Step5_QCMultiYear.py         # 98.7% source match
│   ├── Step6_JSONifyMultiYear.py
│   └── Step7_CleanMultiYear.py
└── stage4_publish/         # Flatten, unify, upload
```

---

## QC Approach

### Statements (PL/BS/CF)
- Formula validation (totals = sum of parts)
- Source page match
- Cross-period consistency
- Allowlist for legitimate edge cases

### Compensation
- Source value match only (no formulas to validate)
- Uses manifest to find source pages
- 99.5% of values found on source pages

### Multi-Year
- Source value match only
- 98.7% of values found on source pages
- No semantic validation (no canonical mapping)

---

## Data Files

### Compensation (Ready)
```
data/json_compensation/management_comp.jsonl
├── Fields: ticker, company_name, industry, year, section, line_item, column, value, source_pdf
└── Records: 27,575
```

### Multi-Year (Deferred)
```
data/json_multiyear/multi_year_normalized.jsonl
├── Fields: ticker, company_name, industry, report_year, data_year, section, line_item, value, source_pdf
├── Records: 83,471
└── Issue: line_item is original text, not canonical
```

---

## Next Steps

1. **Stage 4 Publish** - Upload statements + compensation to Cloudflare D1
2. Skip multi-year until canonical mapping is created
3. Consider creating multi-year mapping in future iteration (LLM-based or manual)
