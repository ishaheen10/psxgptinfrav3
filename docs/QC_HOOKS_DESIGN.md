# QC Hooks Design for P&L Extraction Pipeline

## Overview

This document provides context for creating Claude Code hooks that help automate QC investigation and fixes in the P&L extraction pipeline. The hooks should provide Claude with enough context about the data flow and QC results to investigate and resolve issues autonomously.

---

## Data Flow

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
│ STAGE 3: data/extracted_pl/{TICKER}_{filing_type}_{date}_{cons}.md          │
│                                                                             │
│ What: Structured P&L extraction with canonical field mapping                │
│ Format: Markdown table with columns:                                        │
│   - Source Item (original text from PDF)                                    │
│   - Canonical (standardized field name)                                     │
│   - Ref (row reference for arithmetic, e.g., "A", "B", "C=A+B")             │
│   - Period columns (e.g., "9M Mar 2023", "3M Mar 2022")                     │
│                                                                             │
│ Header: UNIT_TYPE indicates scale (thousands, millions, etc.)               │
│ Source: SOURCE_PAGES shows which markdown_pages were used                   │
│                                                                             │
│ Quality: Depends on OCR quality + LLM interpretation                        │
│   - Period labels inferred from PDF headers (may be wrong)                  │
│   - Values copied from OCR (may have errors)                                │
│   - Canonical mapping may have collisions                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step4_QCPL.py - Arithmetic QC)
┌─────────────────────────────────────────────────────────────────────────────┐
│ QC CHECKPOINT: artifacts/stage3/step4_qc_results.json                       │
│                                                                             │
│ What: Validates arithmetic within each extraction file                      │
│ Checks:                                                                     │
│   - Ref formulas evaluate correctly (e.g., C=A+B means row C = row A + B)   │
│   - Subtotals match their components                                        │
│   - Cross-period consistency where applicable                               │
│                                                                             │
│ Result: PASS/FAIL per file with specific formula failures                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step5_JSONify.py)
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: data/json_pl/{TICKER}.json                                         │
│                                                                             │
│ What: All periods for a ticker in structured JSON                           │
│ Structure:                                                                  │
│   {                                                                         │
│     "ticker": "ABC",                                                        │
│     "periods": [                                                            │
│       {                                                                     │
│         "period_end": "2023-03-31",                                         │
│         "duration": "9M",           // 3M, 6M, 9M, or 12M                   │
│         "consolidation": "consolidated",                                    │
│         "source_filing": "ABC_quarterly_2023-03-31_consolidated.md",        │
│         "values": {                                                         │
│           "revenue_net": 1234567,                                           │
│           "gross_profit": 456789,                                           │
│           ...                                                               │
│         }                                                                   │
│       },                                                                    │
│       ...                                                                   │
│     ]                                                                       │
│   }                                                                         │
│                                                                             │
│ Note: Same period_end may appear multiple times with different durations    │
│       (e.g., 3M and 9M both ending Mar 31)                                  │
│ Note: Values from later filings (comparatives) may duplicate/conflict with  │
│       values from original filings                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step6_DeriveQuarters.py)
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: data/quarterly_pl/{TICKER}.json                                    │
│                                                                             │
│ What: Quarterly (3M standalone) values for each fiscal year                 │
│ Derivation methods:                                                         │
│   - direct_3M: Value came directly from a 3M period                         │
│   - 6M-Q1: Q2 = 6M cumulative - Q1                                          │
│   - 9M-6M: Q3 = 9M cumulative - 6M cumulative                               │
│   - 9M-Q1-Q2: Q3 = 9M - Q1 - Q2                                             │
│   - 12M-9M: Q4 = Annual - 9M cumulative                                     │
│   - 12M-Q1-Q2-Q3: Q4 = Annual - Q1 - Q2 - Q3                                │
│                                                                             │
│ Structure:                                                                  │
│   {                                                                         │
│     "ticker": "ABC",                                                        │
│     "fiscal_year_end_month": 6,    // June year-end                         │
│     "quarters": [                                                           │
│       {                                                                     │
│         "quarter": "Q1",                                                    │
│         "period_end": "2023-09-30",                                         │
│         "fiscal_year": 2024,                                                │
│         "method": "direct_3M",                                              │
│         "source": "ABC_quarterly_2023-09-30_consolidated.md",               │
│         "values": { ... }                                                   │
│       },                                                                    │
│       ...                                                                   │
│     ]                                                                       │
│   }                                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (Step6 also runs QC)
┌─────────────────────────────────────────────────────────────────────────────┐
│ QC CHECKPOINT: artifacts/stage3/step6_qc_issues.json                        │
│                                                                             │
│ What: Validates derived quarters against annual totals                      │
│ Checks:                                                                     │
│   - Q1 + Q2 + Q3 + Q4 = Annual (within 1% tolerance)                        │
│   - Checked for: revenue (industry-specific), gross_profit, net_profit      │
│   - Negative income flags (unexpected for most companies)                   │
│                                                                             │
│ Structure:                                                                  │
│   {                                                                         │
│     "total_issues": 49,                                                     │
│     "issues": [                                                             │
│       {                                                                     │
│         "ticker": "ABC",                                                    │
│         "quarter": "FY",           // "FY" = fiscal year arithmetic check   │
│         "fiscal_year": 2022,                                                │
│         "consolidation": "consolidated",                                    │
│         "method": "arithmetic_check",                                       │
│         "issues": [                                                         │
│           "revenue_net: Q1+Q2+Q3+Q4=6,358,965 vs Annual=3,795,200 (67.6%)"  │
│         ]                                                                   │
│       },                                                                    │
│       ...                                                                   │
│     ]                                                                       │
│   }                                                                         │
│                                                                             │
│ Failure indicates: Something is wrong upstream (OCR, extraction, or         │
│                    period labeling)                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Files Reference

| Path | What It Contains |
|------|------------------|
| `markdown_pages/{TICKER}/{YEAR}/{FILING}/page_NNN.md` | Raw OCR from PDF page |
| `data/extracted_pl/{TICKER}_{type}_{date}_{cons}.md` | Structured P&L extraction |
| `data/json_pl/{TICKER}.json` | All periods merged into JSON |
| `data/quarterly_pl/{TICKER}.json` | Derived quarterly values |
| `artifacts/stage3/step4_qc_results.json` | Per-file arithmetic QC results |
| `artifacts/stage3/step6_qc_issues.json` | Quarter derivation QC failures |
| `artifacts/stage3/step2_statement_pages.json` | Source page locations for each ticker/filing |
| `tickers100.json` | Ticker metadata (fiscal year end, industry) |

---

## QC Failure Types

### Step4 Failures (Arithmetic within extraction)

**What it means**: The Ref formulas in the extraction don't compute correctly.

**Example**: Row C claims `C=A+B` but the values don't add up.

**Possible causes**:
- OCR error in the source markdown
- LLM copied wrong values during extraction
- Rounding differences

### Step6 Failures (Quarter derivation)

**What it means**: Q1 + Q2 + Q3 + Q4 ≠ Annual (beyond 1% tolerance)

**Example**: `revenue_net: Q1+Q2+Q3+Q4=6,358,965 vs Annual=3,795,200 (67.6%)`

**Possible causes**:
- Period mislabeling in extraction (e.g., 9M cumulative labeled as 3M standalone)
- Wrong values extracted from OCR
- Unit mismatch between periods
- Field collision (multiple source items mapped to same canonical field)
- Fiscal year boundary confusion

---

## Investigation Workflow

When a Step6 QC failure occurs, Claude should:

1. **Identify the failing ticker and fiscal year** from step6_qc_issues.json

2. **Load the json_pl data** to see all available periods:
   - What periods exist for that fiscal year?
   - What are the values for the failing field?
   - What source_filing did each period come from?

3. **Check for conflicts**:
   - Does the same (period_end, field) appear with different values?
   - Does the same value appear with different period durations?

4. **Trace back to extraction files**:
   - Read the relevant extracted_pl/*.md files
   - Check the period column headers
   - Verify against SOURCE_PAGES reference

5. **ALWAYS validate with source markdown**:
   - Find the source page location from: `artifacts/stage3/step2_statement_pages.json`
     (lookup by ticker and filing to get the page numbers)
   - Read the original markdown_pages to see what the PDF actually said
   - Compare extracted values against the original OCR to identify:
     - OCR errors (garbled numbers)
     - Wrong period labels in extraction
     - Missing or misaligned data

6. **Determine the fix**:
   - If period label is wrong in extraction → edit the extraction file
   - If OCR error → may need re-OCR or manual correction
   - If extraction missed data → may need re-extraction

7. **Apply fix and re-run**:
   - Edit the extraction file
   - Run Step5_JSONify.py --ticker X
   - Run Step6_DeriveQuarters.py --ticker X
   - Verify QC passes

---

## Hook Design

### Purpose

The hook should trigger after Step6 runs and provide Claude with:
1. Summary of QC failures
2. Relevant context to investigate
3. Clear instructions on what to do next

### Hook Output Template

When QC failures are detected, the hook should output something like:

```
=== STEP6 QC FAILURES DETECTED ===

Found {N} arithmetic failures across {M} tickers.

FAILING TICKERS:
  - {TICKER1}: FY{YEAR} - {field}: {Q_sum} vs Annual {annual} ({diff}%)
  - {TICKER2}: ...

TO INVESTIGATE:

1. Load the json_pl data:
   Read data/json_pl/{TICKER}.json

2. Find periods for the failing fiscal year and check:
   - What durations (3M, 6M, 9M, 12M) exist for each period_end?
   - What source_filing provided each period?
   - Do any values appear with conflicting durations?

3. Trace to extraction files:
   Read data/extracted_pl/{source_filing}
   Check the period column headers match the actual data

4. ALWAYS validate with source markdown:
   - Find page location from: artifacts/stage3/step2_statement_pages.json
   - Read markdown_pages/{TICKER}/{YEAR}/{FILING}/page_NNN.md
   - Compare extracted values against original OCR

5. Fix the issue and re-run:
   - Edit the extraction file if period labels are wrong
   - uv run python pipeline/stage3_extract/Step5_JSONify.py --ticker {TICKER}
   - uv run python pipeline/stage3_extract/Step6_DeriveQuarters.py --ticker {TICKER}

Full details: artifacts/stage3/step6_qc_issues.json
```

### Exit Behavior

- **Exit 0**: No QC failures, continue normally
- **Exit 2**: QC failures found, block and show investigation instructions to Claude

---

## Hook Configuration

Add to `.claude/settings.local.json`:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/post-step6-qc.sh",
            "timeout": 60000
          }
        ]
      }
    ]
  }
}
```

### Shell Hook

`.claude/hooks/post-step6-qc.sh`:

```bash
#!/bin/bash
# PostToolUse hook: After Step6, check for QC failures and provide investigation context

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // empty')

# Only trigger for Step6_DeriveQuarters
if [[ "$command" != *"Step6_DeriveQuarters"* ]]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

QC_FILE="artifacts/stage3/step6_qc_issues.json"

if [ ! -f "$QC_FILE" ]; then
  exit 0
fi

# Count arithmetic failures
fail_count=$(jq '[.issues[] | select(.method == "arithmetic_check")] | length' "$QC_FILE")

if [ "$fail_count" -eq 0 ]; then
  exit 0
fi

# Get failing tickers
failing_tickers=$(jq -r '[.issues[] | select(.method == "arithmetic_check") | .ticker] | unique | join(", ")' "$QC_FILE")

# Get sample failures
sample_failures=$(jq -r '.issues[] | select(.method == "arithmetic_check") | "\(.ticker) FY\(.fiscal_year): \(.issues[0])"' "$QC_FILE" | head -10)

cat >&2 << EOF
=== STEP6 QC FAILURES DETECTED ===

Found $fail_count arithmetic failures.

FAILING TICKERS: $failing_tickers

SAMPLE FAILURES:
$sample_failures

TO INVESTIGATE:

1. Read the json_pl data for failing ticker:
   data/json_pl/{TICKER}.json

2. For the failing fiscal year, check all periods:
   - What durations (3M, 6M, 9M, 12M) exist?
   - What source_filing provided each?
   - Do values conflict across sources?

3. Trace to extraction files:
   data/extracted_pl/{source_filing}
   - Check period column headers
   - Check SOURCE_PAGES reference

4. If needed, check source markdown:
   markdown_pages/{path}

5. Fix and re-run:
   - Edit extraction if period labels wrong
   - uv run python pipeline/stage3_extract/Step5_JSONify.py --ticker {TICKER}
   - uv run python pipeline/stage3_extract/Step6_DeriveQuarters.py --ticker {TICKER}

Full details: $QC_FILE
EOF

exit 2
```

---

## Summary

The hook provides **context, not solutions**. It tells Claude:

1. **What failed**: Which tickers, which fiscal years, what the mismatch is
2. **Where to look**: The data files in the pipeline
3. **How to investigate**: Read json_pl → trace to extraction → trace to source
4. **What to do after fixing**: Re-run Step5 and Step6

Claude then uses its understanding of the data to investigate, identify root causes, and apply appropriate fixes.
