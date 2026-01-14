#!/usr/bin/env python3
"""
Step 3: Convert PDF pages to JPG thumbnails for UI preview.

Reads from pdf_pages/ and creates thumbnails in pdf_thumbnails/.

Input:  pdf_pages/<ticker>/<year>/<filing>/page_###.pdf
Output: pdf_thumbnails/<ticker>/<year>/<filing>/page_###.jpg

Specs:
- Output width: 360px
- JPEG quality: 70%
- DPI: 110
- Format: JPEG/RGB
"""

import sys
from pathlib import Path
from tqdm import tqdm
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import PROJECT_ROOT

# Directories
INPUT_DIR = PROJECT_ROOT / "pdf_pages"
OUTPUT_DIR = PROJECT_ROOT / "pdf_thumbnails"

# Thumbnail specifications
TARGET_WIDTH = 360
JPEG_QUALITY = 70
DPI = 110
MAX_WORKERS = max(1, cpu_count() - 1)


def convert_pdf_to_jpg(args: tuple) -> tuple:
    """Convert a single-page PDF to JPG thumbnail."""
    from pdf2image import convert_from_path
    from PIL import Image

    pdf_path, output_dir = args

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        jpg_filename = pdf_path.stem + ".jpg"
        jpg_path = output_dir / jpg_filename

        if jpg_path.exists():
            return ('skipped', pdf_path)

        images = convert_from_path(
            pdf_path,
            dpi=DPI,
            first_page=1,
            last_page=1,
            fmt='jpeg'
        )

        if not images:
            return ('failed', pdf_path)

        image = images[0]
        if image.mode != 'RGB':
            image = image.convert('RGB')

        original_width, original_height = image.size
        aspect_ratio = original_height / original_width
        target_height = int(TARGET_WIDTH * aspect_ratio)

        resized_image = image.resize(
            (TARGET_WIDTH, target_height),
            Image.Resampling.LANCZOS
        )

        resized_image.save(
            jpg_path,
            'JPEG',
            quality=JPEG_QUALITY,
            optimize=True
        )

        return ('success', pdf_path)

    except Exception as e:
        logger.error(f"Error converting {pdf_path}: {e}")
        return ('failed', pdf_path)


def find_all_pdfs(directory: Path) -> list:
    """Recursively find all PDF files."""
    return list(directory.rglob("*.pdf"))


def main():
    logger.info("=" * 70)
    logger.info("STAGE 1 STEP 3: CONVERT PDFs TO JPG THUMBNAILS")
    logger.info("=" * 70)

    if not INPUT_DIR.exists():
        logger.error(f"Input directory not found: {INPUT_DIR}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdf_files = find_all_pdfs(INPUT_DIR)
    logger.info(f"Found {len(pdf_files)} PDFs to convert")
    logger.info(f"Thumbnail specs: {TARGET_WIDTH}px width, {JPEG_QUALITY}% quality, {DPI} DPI")
    logger.info(f"Using {MAX_WORKERS} CPU workers")

    if not pdf_files:
        logger.warning("No PDFs found!")
        return

    # Prepare tasks
    tasks = []
    for pdf_path in pdf_files:
        relative_path = pdf_path.relative_to(INPUT_DIR)
        output_subdir = OUTPUT_DIR / relative_path.parent
        tasks.append((pdf_path, output_subdir))

    successful = failed = skipped = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(convert_pdf_to_jpg, task) for task in tasks]

        with tqdm(total=len(tasks), desc="Converting to JPG") as pbar:
            for future in as_completed(futures):
                status, _ = future.result()
                if status == 'success':
                    successful += 1
                elif status == 'skipped':
                    skipped += 1
                else:
                    failed += 1
                pbar.update(1)

    logger.info("=" * 70)
    logger.info(f"Conversion complete!")
    logger.info(f"Converted: {successful}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Failed: {failed}")
    logger.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        if "poppler" in str(e).lower():
            logger.error("POPPLER NOT FOUND")
            logger.error("Install with: brew install poppler (macOS) or apt-get install poppler-utils (Linux)")
        else:
            raise
