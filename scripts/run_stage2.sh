#!/bin/bash
# Stage 2: Review
# Classifies pages and repairs OCR issues

set -e
cd "$(dirname "$0")/.."

echo "========================================"
echo "STAGE 2: REVIEW"
echo "========================================"
echo ""

# Step 1: Build Skip Manifest (deterministic)
echo "Step 1: Building skip manifest..."
python pipeline/stage2_review/Step1_BuildSkipManifest.py

# Step 2: Classify Pages (DeepSeek - costs money)
echo ""
echo "Step 2: Classifying pages with DeepSeek..."
python pipeline/stage2_review/Step2_ClassifyPages.py

# Step 3: Classify Repairs
echo ""
echo "Step 3: Routing pages by repair type..."
python pipeline/stage2_review/Step3_ClassifyRepairs.py

# Step 4: Repair Fix (DeepSeek - costs money)
if [ -f "artifacts/stage2/repair_manifest.json" ]; then
    fix_count=$(python -c "import json; print(json.load(open('artifacts/stage2/repair_manifest.json'))['counts']['Fix'])")
    if [ "$fix_count" -gt 0 ]; then
        echo ""
        echo "Step 4: Repairing $fix_count pages with DeepSeek..."
        python pipeline/stage2_review/Step4_RepairFix.py
    fi
fi

# Step 5: Repair ReOCR (Gemini - costs money)
if [ -f "artifacts/stage2/repair_manifest.json" ]; then
    reocr_count=$(python -c "import json; print(json.load(open('artifacts/stage2/repair_manifest.json'))['counts']['ReOCR'])")
    if [ "$reocr_count" -gt 0 ]; then
        echo ""
        echo "Step 5: Re-OCR $reocr_count pages with Gemini..."
        python pipeline/stage2_review/Step5_RepairReOCR.py
    fi
fi

# Step 6: Final Corruption Check
echo ""
echo "Step 6: Final corruption check..."
python pipeline/stage2_review/Step6_FinalCorruptionCheck.py

# Step 7: Build Extraction Manifest
echo ""
echo "Step 7: Building extraction manifest..."
python pipeline/stage2_review/Step7_BuildExtractionManifest.py

echo ""
echo "========================================"
echo "STAGE 2 COMPLETE"
echo "========================================"
echo ""
echo "Output: artifacts/stage2/extraction_manifest.json"
echo "Ready for Stage 3: Extract"
