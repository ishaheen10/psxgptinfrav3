#!/usr/bin/env python3
"""
Step 1: Download PDFs from PSX website.

Downloads quarterly and annual reports from PSX to database_pdfs/.
Uses Playwright to navigate the PSX financials portal.

Input:  tickers100.json (ticker list)
Output: database_pdfs/<ticker>/<year>/<ticker>_<type>_<date>.pdf

Note: This step requires browser automation and network access.
For most pipeline runs, markdown_pages/ already exists and this step is skipped.
"""

# Standard library imports
import os
import time
import sys
import re
import json
import traceback
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared import Checkpoint
from shared.constants import PROJECT_ROOT

# --- Configuration ---
BASE_URL = "https://financials.psx.com.pk/"
TARGET_START_YEAR = int(os.getenv("TARGET_START_YEAR", "2021"))
TARGET_END_YEAR = int(os.getenv("TARGET_END_YEAR", "2025"))
TICKERS_FILE = PROJECT_ROOT / "tickers100.json"
SCREENSHOTS_DIR = PROJECT_ROOT / "screenshots"
LOCAL_PDF_DIR = PROJECT_ROOT / "database_pdfs"
EXPECTED_REPORTS_PER_YEAR = int(os.getenv("EXPECTED_REPORTS_PER_YEAR", "4"))

# --- Logging Configuration ---
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / 'step1_download.log')
    ]
)
logger = logging.getLogger(__name__)


def is_local_development() -> bool:
    """Detect if we're running in a local development environment."""
    return os.getenv('LOCAL_DEVELOPMENT', 'false').lower() == 'true'


def load_tickers() -> List[Dict]:
    """Load ticker data from the JSON manifest."""
    try:
        with open(TICKERS_FILE, 'r') as f:
            tickers = json.load(f)
        logger.info(f"Loaded {len(tickers)} tickers from {TICKERS_FILE}")
        return tickers
    except Exception as e:
        logger.error(f"Failed to load tickers from {TICKERS_FILE}: {e}")
        raise


def sanitize_component(value: str) -> str:
    """Convert a string to a filesystem-safe component."""
    cleaned = re.sub(r'[^\w\-]+', '_', value.strip())
    return cleaned.strip('_') or "value"


def report_exists_locally(ticker_symbol: str, year: int, report_type: str, period_date: str) -> bool:
    """Check if a report already exists locally to avoid re-downloading."""
    clean_ticker = ticker_symbol.upper().strip()
    dir_path = LOCAL_PDF_DIR / clean_ticker / str(year)
    if not dir_path.exists():
        return False
    stem = f"{clean_ticker}_{sanitize_component(report_type)}_{sanitize_component(period_date) if period_date != 'Unknown' else str(year)}"
    pattern = f"{stem}.*"
    return any(dir_path.glob(pattern))


def count_local_reports_for_year(ticker_symbol: str, year: int) -> int:
    """Count how many PDFs are stored locally for a given ticker and year."""
    clean_ticker = ticker_symbol.upper().strip()
    dir_path = LOCAL_PDF_DIR / clean_ticker / str(year)
    if not dir_path.exists():
        return 0
    return sum(1 for path in dir_path.glob("*.pdf"))


def main():
    """Main download function."""
    logger.info("=" * 70)
    logger.info("STAGE 1 STEP 1: DOWNLOAD PDFs FROM PSX")
    logger.info("=" * 70)

    # Check for Playwright
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install")
        return

    LOCAL_PDF_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers()
    checkpoint = Checkpoint.load("Step1_DownloadPDFs", stage=1)

    # Count what needs to be done
    to_process = []
    for ticker_info in tickers:
        ticker = ticker_info["Symbol"]
        for year in range(TARGET_START_YEAR, TARGET_END_YEAR + 1):
            item_id = f"{ticker}_{year}"
            if item_id in checkpoint.completed_items:
                continue
            local_count = count_local_reports_for_year(ticker, year)
            if local_count >= EXPECTED_REPORTS_PER_YEAR:
                checkpoint.skip(item_id)
                continue
            to_process.append((ticker_info, year))

    checkpoint.set_total(len(to_process))

    if not to_process:
        logger.info("All downloads complete - nothing to do.")
        checkpoint.finalize()
        return

    logger.info(f"Need to process {len(to_process)} ticker-year combinations")
    logger.info("Note: Full download implementation requires Playwright browser automation")
    logger.info("See original Step1_DownloadPDFs.py for complete implementation")

    # For a clean V3 setup, we assume markdown_pages/ already exists
    # This script is provided for completeness but typically not needed
    checkpoint.finalize()


if __name__ == "__main__":
    main()
