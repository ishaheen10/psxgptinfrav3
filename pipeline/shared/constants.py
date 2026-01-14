"""Pipeline constants and paths."""

from pathlib import Path

# Project root (psxgptinfrav3/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Key directories
ARTIFACTS = PROJECT_ROOT / "artifacts"
MARKDOWN_ROOT = PROJECT_ROOT / "markdown_pages"

# Stage-specific artifact paths
STAGE1_ARTIFACTS = ARTIFACTS / "stage1"
STAGE2_ARTIFACTS = ARTIFACTS / "stage2"
STAGE3_ARTIFACTS = ARTIFACTS / "stage3"
STAGE4_ARTIFACTS = ARTIFACTS / "stage4"
UTILITIES_ARTIFACTS = ARTIFACTS / "utilities"

# Output directories
STATEMENTS_FINAL = PROJECT_ROOT / "statements_final"
DATABASE_ROWS = PROJECT_ROOT / "database_rows"
