#!/bin/bash
# Stage 4: Publish
# Flattens, unifies, and uploads data to Cloudflare D1
#
# Prerequisites:
#   - Stage 3 completed (review QC reports first)
#
# This script handles two parallel data flows:
#   A. Financial Statements (Steps 1-4)
#   B. Document Database for search (Steps 5-8)

set -e  # Exit on error

cd "$(dirname "$0")/.."  # Go to project root

# Parse arguments
SKIP_STATEMENTS=false
SKIP_DOCUMENTS=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --statements-only) SKIP_DOCUMENTS=true; shift ;;
        --documents-only) SKIP_STATEMENTS=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=========================================="
echo "STAGE 4: PUBLISH"
echo "=========================================="

# ==========================================
# FLOW A: Financial Statements
# ==========================================
if [ "$SKIP_STATEMENTS" = false ]; then
    echo ""
    echo "--- FLOW A: Financial Statements ---"

    # Step 0: Validate Mappings
    echo ""
    echo "[Step 0] Validating field mappings..."
    python pipeline/stage4_publish/Step0_ValidateMapping.py

    # Step 1: Flatten Statements
    echo ""
    echo "[Step 1] Flattening statements..."
    python pipeline/stage4_publish/Step1_FlattenStatements.py

    # Step 2: Unify Statements
    echo ""
    echo "[Step 2] Unifying with canonical mappings..."
    python pipeline/stage4_publish/Step2_UnifyStatements.py

    # Step 2b: Normalize Units
    echo ""
    echo "[Step 2b] Normalizing units to thousands..."
    python pipeline/stage4_publish/Step2b_NormalizeUnits.py

    # Step 3: QC Pre-Upload
    echo ""
    echo "[Step 3] Running pre-upload QC..."
    python pipeline/stage4_publish/Step3_QCPreUpload.py

    # Check QC results
    if [ -f "artifacts/stage4/qc_pre_upload.json" ]; then
        ISSUES=$(python -c "import json; d=json.load(open('artifacts/stage4/qc_pre_upload.json')); print(d.get('total_issues', 0))" 2>/dev/null || echo "0")
        if [ "$ISSUES" != "0" ]; then
            echo ""
            echo "WARNING: Found $ISSUES QC issues!"
            echo "Review: artifacts/stage4/qc_pre_upload.json"
            read -p "Continue with upload? (y/N) " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                echo "Aborted. Fix issues and re-run."
                exit 1
            fi
        fi
    fi

    # Step 4: Upload Statements
    echo ""
    echo "[Step 4] Uploading statements to D1..."
    read -p "Upload statements to Cloudflare D1? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        python pipeline/stage4_publish/Step4_UploadStatements.py
    else
        echo "Skipped statement upload."
    fi
fi

# ==========================================
# FLOW B: Document Database (for search)
# ==========================================
if [ "$SKIP_DOCUMENTS" = false ]; then
    echo ""
    echo "--- FLOW B: Document Database ---"

    # Check for existing snapshot for delta computation
    LATEST_SNAPSHOT=$(ls -d artifacts/stage4/database_snapshot_* 2>/dev/null | sort | tail -1 || echo "")

    # Create backup snapshot before compiling
    TIMESTAMP=$(date +%Y%m%d)
    if [ -d "database_jsonl_compiled" ]; then
        echo ""
        echo "Creating backup snapshot: artifacts/stage4/database_snapshot_$TIMESTAMP"
        mkdir -p "artifacts/stage4"
        cp -r database_jsonl_compiled "artifacts/stage4/database_snapshot_$TIMESTAMP"
        LATEST_SNAPSHOT="artifacts/stage4/database_snapshot_$TIMESTAMP"
    fi

    # Step 5: Compile Documents
    echo ""
    echo "[Step 5] Compiling document database..."
    python pipeline/stage4_publish/Step5_CompileDocuments.py

    # Step 6: Compute Delta
    echo ""
    echo "[Step 6] Computing delta..."
    if [ -n "$LATEST_SNAPSHOT" ] && [ -d "$LATEST_SNAPSHOT" ]; then
        echo "Using previous snapshot: $LATEST_SNAPSHOT"
        python pipeline/stage4_publish/Step6_ComputeDelta.py \
            --previous "$LATEST_SNAPSHOT" \
            --current database_jsonl_compiled \
            --output database_jsonl_upload \
            --overwrite
    else
        echo "No previous snapshot found - uploading full dataset"
        rm -rf database_jsonl_upload
        cp -r database_jsonl_compiled database_jsonl_upload
    fi

    # Count records to upload
    UPLOAD_COUNT=$(find database_jsonl_upload -name "*.jsonl" -exec cat {} \; 2>/dev/null | wc -l | tr -d ' ' || echo "0")
    echo ""
    echo "Records to upload: $UPLOAD_COUNT"

    if [ "$UPLOAD_COUNT" = "0" ]; then
        echo "Nothing new to upload for documents."
    else
        read -p "Upload $UPLOAD_COUNT document records to D1? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Step 7: Upload Documents
            echo ""
            echo "[Step 7] Uploading documents to D1..."
            python pipeline/stage4_publish/Step7_UploadDocuments.py --input database_jsonl_upload

            # Step 8: Update Filing Periods
            echo ""
            echo "[Step 8] Updating filing periods..."
            python pipeline/stage4_publish/Step8_UpdatePeriods.py
        else
            echo "Skipped document upload. Delta ready in database_jsonl_upload/"
        fi
    fi
fi

echo ""
echo "=========================================="
echo "STAGE 4 COMPLETE"
echo "=========================================="
echo ""
echo "Options for next run:"
echo "  --statements-only   Skip document flow"
echo "  --documents-only    Skip statements flow"
