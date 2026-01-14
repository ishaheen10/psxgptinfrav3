#!/bin/bash
# Stage 1: Ingest
# Converts PDFs to markdown via OCR
#
# For most runs, markdown_pages/ already exists (symlinked from main repo)
# and Steps 1-5 are skipped. Only QC steps typically run.

set -e
cd "$(dirname "$0")/.."

echo "========================================"
echo "STAGE 1: INGEST"
echo "========================================"
echo ""

# Steps 1-5: Download, Split, JPG, Upload, OCR
# These are typically already done - markdown_pages/ exists
if [ ! -L "markdown_pages" ] && [ ! -d "markdown_pages" ]; then
    echo "Running full OCR pipeline..."
    python pipeline/stage1_ingest/Step1_DownloadPDFs.py
    python pipeline/stage1_ingest/Step2_SplitPages.py
    python pipeline/stage1_ingest/Step3_ConvertToJPG.py
    python pipeline/stage1_ingest/Step4_UploadToR2.py
    python pipeline/stage1_ingest/Step5_RunOCR.py
else
    echo "markdown_pages/ exists - skipping download/OCR steps"
fi

# Step 6: QC Coverage (always run)
echo ""
echo "Running QC Coverage..."
python pipeline/stage1_ingest/Step6_QCCoverage.py

# Step 7: QC Corruption (always run)
echo ""
echo "Running QC Corruption Detection..."
python pipeline/stage1_ingest/Step7_QCCorruption.py

# Step 8: Re-OCR if needed
if [ -f "artifacts/stage1/corrupted_pages.json" ]; then
    corrupted=$(python -c "import json; print(json.load(open('artifacts/stage1/corrupted_pages.json'))['total_corrupted'])")
    if [ "$corrupted" -gt 0 ]; then
        echo ""
        echo "Found $corrupted corrupted pages - running Re-OCR..."
        python pipeline/stage1_ingest/Step8_ReOCR.py
    fi
fi

echo ""
echo "========================================"
echo "STAGE 1 COMPLETE"
echo "========================================"
