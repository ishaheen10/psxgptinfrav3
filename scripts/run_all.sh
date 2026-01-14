#!/bin/bash
# Run full pipeline: Stage 1 + Stage 2
# Stage 3 and 4 are typically run separately

set -e
cd "$(dirname "$0")/.."

echo "========================================"
echo "PSX PIPELINE V3"
echo "========================================"
echo ""
echo "This script runs Stage 1 (Ingest) and Stage 2 (Review)"
echo "Stage 3 (Extract) and Stage 4 (Publish) should be run separately"
echo ""

# Stage 1
./scripts/run_stage1.sh

echo ""
echo ""

# Stage 2
./scripts/run_stage2.sh

echo ""
echo "========================================"
echo "STAGES 1-2 COMPLETE"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Review artifacts/stage2/extraction_manifest.json"
echo "  2. Run Stage 3: ./scripts/run_stage3.sh (when implemented)"
echo "  3. Run Stage 4: ./scripts/run_stage4.sh (when implemented)"
