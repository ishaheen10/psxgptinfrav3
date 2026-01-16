#!/bin/bash
# Stage 4: Extract Other Data (Compensation + Multi-Year)
#
# Prerequisites:
#   - Stage 2 completed (extraction_manifest.json exists)
#   - markdown_pages/ available
#
# Steps:
#   1. Extract compensation data from annual reports
#   2. QC compensation extraction (source match)
#   3. JSONify compensation markdown
#   4. Extract multi-year data from annual reports
#   5. QC multi-year extraction (source match)
#   6. JSONify multi-year markdown
#   7. Clean multi-year data

set -e  # Exit on error

cd "$(dirname "$0")/.."  # Go to project root

echo "=========================================="
echo "STAGE 4: EXTRACT OTHER DATA"
echo "=========================================="

# Step 1: Extract Compensation
echo ""
echo "[Step 1] Extracting compensation data..."
python pipeline/stage4_extract_other/Step1_ExtractCompensation.py

# Step 2: QC Compensation
echo ""
echo "[Step 2] QC compensation extraction..."
python pipeline/stage4_extract_other/Step2_QCCompensation.py

# Step 3: JSONify Compensation
echo ""
echo "[Step 3] JSONifying compensation data..."
python pipeline/stage4_extract_other/Step3_JSONifyCompensation.py

# Step 4: Extract Multi-Year
echo ""
echo "[Step 4] Extracting multi-year data..."
python pipeline/stage4_extract_other/Step4_ExtractMultiYear.py

# Step 5: QC Multi-Year
echo ""
echo "[Step 5] QC multi-year extraction..."
python pipeline/stage4_extract_other/Step5_QCMultiYear.py

# Step 6: JSONify Multi-Year
echo ""
echo "[Step 6] JSONifying multi-year data..."
python pipeline/stage4_extract_other/Step6_JSONifyMultiYear.py

# Step 7: Clean Multi-Year
echo ""
echo "[Step 7] Cleaning multi-year data..."
python pipeline/stage4_extract_other/Step7_CleanMultiYear.py

echo ""
echo "=========================================="
echo "STAGE 4 COMPLETE"
echo "=========================================="
