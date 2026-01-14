#!/bin/bash
# Stage 3: Extract
# Extracts financial statements from OCR'd markdown pages
#
# Prerequisites:
#   - Stage 2 completed (extraction_manifest.json exists)
#   - Review any corrupted/repaired pages
#
# After completion, review artifacts/stage3/ for QC reports
# before running Stage 4 (publish)

set -e  # Exit on error

cd "$(dirname "$0")/.."  # Go to project root

echo "=========================================="
echo "STAGE 3: EXTRACT"
echo "=========================================="

# Verify prerequisite
if [ ! -f "artifacts/stage2/extraction_manifest.json" ]; then
    echo "ERROR: artifacts/stage2/extraction_manifest.json not found!"
    echo "Run Stage 2 first: ./scripts/run_stage2.sh"
    exit 1
fi

# Step 1: Build Manifests
echo ""
echo "[Step 1] Building extraction manifests..."
python pipeline/stage3_extract/Step1_BuildManifests.py

# Step 2: Extract Statements (LLM - costs money)
echo ""
echo "[Step 2] Extracting statements with LLM..."
echo "WARNING: This step makes API calls and costs money!"
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted. Run Step 2 manually when ready:"
    echo "  python pipeline/stage3_extract/Step2_ExtractStatements.py"
    exit 0
fi
python pipeline/stage3_extract/Step2_ExtractStatements.py

# Step 3: QC Extraction
echo ""
echo "[Step 3] Running extraction QC..."
python pipeline/stage3_extract/Step3_QCExtraction.py

# Check for extraction errors
if [ -f "artifacts/stage3/extraction_errors.jsonl" ]; then
    ERROR_COUNT=$(wc -l < artifacts/stage3/extraction_errors.jsonl 2>/dev/null | tr -d ' ' || echo "0")
    if [ "$ERROR_COUNT" != "0" ]; then
        echo ""
        echo "WARNING: Found $ERROR_COUNT extraction errors!"
        echo "Review: artifacts/stage3/extraction_errors.jsonl"
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Stopped. Fix errors and re-run."
            exit 1
        fi
    fi
fi

# Step 4: Convert to JSON
echo ""
echo "[Step 4] Converting to JSON format..."
python pipeline/stage3_extract/Step4_ConvertToJSON.py

# Step 5: QC Arithmetic
echo ""
echo "[Step 5] Running arithmetic QC..."
python pipeline/stage3_extract/Step5_QCArithmetic.py

# Step 6: Calculate Periods & Build Source Manifest
echo ""
echo "[Step 6] Calculating periods and building source manifest..."
python pipeline/stage3_extract/Step6_CalculatePeriods.py
python pipeline/stage3_extract/Step6_BuildSourceManifest.py

# Step 6a: Merge Statement JSONs
echo ""
echo "[Step 6a] Merging statement JSONs..."
python pipeline/stage3_extract/Step6a_MergeStatementJSONs.py

# Step 6b: QC Period Ordering
echo ""
echo "[Step 6b] QC period ordering..."
python pipeline/stage3_extract/Step6b_QCPeriodOrdering.py

# Step 7: Calculate and Validate
echo ""
echo "[Step 7] Calculating derived values and validating..."
python pipeline/stage3_extract/Step7_CalculateAndValidate.py

# Step 8: Diagnose Errors
echo ""
echo "[Step 8] Diagnosing remaining errors..."
python pipeline/stage3_extract/Step8_DiagnoseErrors.py

echo ""
echo "=========================================="
echo "STAGE 3 COMPLETE"
echo "=========================================="
echo ""
echo "Output directories:"
echo "  - extracted_statements/     (raw extracted)"
echo "  - statements_json/          (JSON format)"
echo ""
echo "QC reports:"
echo "  - artifacts/stage3/"
echo ""
echo "Next steps:"
echo "  1. Review QC reports in artifacts/stage3/"
echo "  2. Run: ./scripts/run_stage4.sh"
echo ""
echo "Optional additional steps:"
echo "  - Compensation: python pipeline/stage3_extract/Step9_CompileCompensation.py"
echo "  - Multi-year:   python pipeline/stage3_extract/Step11_CompileMultiYear.py"
