#!/bin/bash
# Stage 5: Publish Data to D1
#
# Prerequisites:
#   - Stage 3 completed (quarterly statements exist with QC passed)
#   - Stage 4 completed (compensation + multi-year extracted)
#
# Steps:
#   1. Flatten P&L data
#   2. QC pre-upload validation
#   3. Upload P&L to D1

set -e  # Exit on error

cd "$(dirname "$0")/.."  # Go to project root

echo "=========================================="
echo "STAGE 5: PUBLISH DATA TO D1"
echo "=========================================="

# Step 1: Flatten P&L
echo ""
echo "[Step 1] Flattening P&L data..."
python pipeline/stage5_publish/Step1_FlattenPL.py

# Step 2: QC Pre-Upload
echo ""
echo "[Step 2] Running pre-upload QC..."
python pipeline/stage5_publish/Step2_QCPreUpload.py

# Check QC results
if [ -f "artifacts/stage5/qc_pre_upload.json" ]; then
    ISSUES=$(python3 -c "import json; d=json.load(open('artifacts/stage5/qc_pre_upload.json')); print(d.get('total_issues', 0))" 2>/dev/null || echo "0")
    if [ "$ISSUES" != "0" ]; then
        echo ""
        echo "WARNING: Found $ISSUES QC issues!"
        echo "Review: artifacts/stage5/qc_pre_upload.json"
        read -p "Continue with upload? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Aborted. Fix issues and re-run."
            exit 1
        fi
    fi
fi

# Step 3: Upload to D1
echo ""
echo "[Step 3] Uploading to D1..."
python pipeline/stage5_publish/Step3_UploadPL.py

echo ""
echo "=========================================="
echo "STAGE 5 COMPLETE"
echo "=========================================="
