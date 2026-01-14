#!/usr/bin/env python3
"""
Step 2: Split PDFs into individual pages.

Takes PDFs from database_pdfs/ and creates page-separated versions in pdf_pages/.

Input:  database_pdfs/<ticker>/<year>/<filing>.pdf
Output: pdf_pages/<ticker>/<year>/<filing>/page_001.pdf, page_002.pdf, ...

Note: For most pipeline runs, markdown_pages/ already exists and this step is skipped.
"""

import sys
from pathlib import Path
from tqdm import tqdm
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared import Checkpoint
from shared.constants import PROJECT_ROOT

# Directories
INPUT_DIR = PROJECT_ROOT / "database_pdfs"
OUTPUT_DIR = PROJECT_ROOT / "pdf_pages"


def split_pdf_to_pages(pdf_path: Path, output_base_dir: Path) -> tuple:
    """
    Split a single PDF into individual pages.

    Structure:
    Input: database_pdfs/AABS/2021/AABS_Annual_2021.pdf
    Output: pdf_pages/AABS/2021/AABS_Annual_2021/page_001.pdf
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        logger.error("pypdf not installed. Run: pip install pypdf")
        return False, 0

    try:
        reader = PdfReader(pdf_path)
        num_pages = len(reader.pages)

        # Create output directory for this PDF
        relative_path = pdf_path.relative_to(INPUT_DIR)
        pdf_folder_name = relative_path.stem
        pdf_output_dir = output_base_dir / relative_path.parent / pdf_folder_name

        pdf_output_dir.mkdir(parents=True, exist_ok=True)

        pages_created = 0
        for page_num in range(num_pages):
            page_filename = f"page_{page_num + 1:03d}.pdf"
            page_output_path = pdf_output_dir / page_filename

            # Skip if already exists
            if page_output_path.exists():
                pages_created += 1
                continue

            writer = PdfWriter()
            writer.add_page(reader.pages[page_num])

            with open(page_output_path, 'wb') as output_file:
                writer.write(output_file)

            pages_created += 1

        return True, num_pages

    except Exception as e:
        logger.error(f"Error splitting {pdf_path}: {e}")
        return False, 0


def find_all_pdfs(directory: Path) -> list:
    """Recursively find all PDF files in a directory."""
    return list(directory.rglob("*.pdf"))


def main():
    logger.info("=" * 70)
    logger.info("STAGE 1 STEP 2: SPLIT PDFs INTO PAGES")
    logger.info("=" * 70)

    if not INPUT_DIR.exists():
        logger.error(f"Input directory not found: {INPUT_DIR}")
        logger.error("Run Step1_DownloadPDFs.py first or ensure database_pdfs/ exists")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdf_files = find_all_pdfs(INPUT_DIR)
    logger.info(f"Found {len(pdf_files)} PDFs to process")

    if not pdf_files:
        logger.warning("No PDFs found in input directory!")
        return

    checkpoint = Checkpoint.load("Step2_SplitPages", stage=1)
    checkpoint.set_total(len(pdf_files))

    successful = failed = 0
    total_pages = 0

    for pdf_path in tqdm(pdf_files, desc="Splitting PDFs"):
        item_id = str(pdf_path.relative_to(INPUT_DIR))

        if item_id in checkpoint.completed_items:
            checkpoint.skip(item_id)
            continue

        checkpoint.mark_in_progress(item_id)
        success, num_pages = split_pdf_to_pages(pdf_path, OUTPUT_DIR)

        if success:
            successful += 1
            total_pages += num_pages
            checkpoint.complete(item_id)
        else:
            failed += 1
            checkpoint.fail(item_id, "Split failed")

    checkpoint.finalize()

    logger.info("=" * 70)
    logger.info(f"Split complete!")
    logger.info(f"Successful: {successful} PDFs")
    logger.info(f"Failed: {failed} PDFs")
    logger.info(f"Total pages: {total_pages}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
