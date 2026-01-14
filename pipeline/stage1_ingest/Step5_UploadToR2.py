#!/usr/bin/env python3
"""
Step 4: Upload files to Cloudflare R2.

Uploads:
- pdf_pages/ → PDF_PAGES/
- pdf_thumbnails/ → PDF_THUMBNAILS/
- database_pdfs/ → PDF/

Uses local manifest to track uploads - no R2 API calls to check existence.

Input:  pdf_pages/, pdf_thumbnails/, database_pdfs/
Output: Files uploaded to R2, manifest at artifacts/stage1/r2_upload_manifest.json
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent to path for shared imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import PROJECT_ROOT, STAGE1_ARTIFACTS

# R2 Configuration
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET_NAME")
R2_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY")
R2_ENDPOINT = os.getenv("CLOUDFLARE_R2_ENDPOINT")

MANIFEST_FILE = STAGE1_ARTIFACTS / "r2_upload_manifest.json"
MAX_WORKERS = 150


def setup_r2_client():
    """Setup R2 client with boto3."""
    import boto3
    from botocore.config import Config

    session = boto3.Session(
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )

    client = session.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        region_name='auto',
        config=Config(
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'},
            s3={'addressing_style': 'path'},
            max_pool_connections=50
        )
    )

    client.head_bucket(Bucket=R2_BUCKET_NAME)
    logger.info(f"Connected to R2 bucket: {R2_BUCKET_NAME}")
    return client


def load_manifest() -> set:
    """Load set of already-uploaded R2 keys from local manifest."""
    if not MANIFEST_FILE.exists():
        return set()
    try:
        with open(MANIFEST_FILE) as f:
            data = json.load(f)
            return set(data.get("uploaded_keys", []))
    except Exception as e:
        logger.warning(f"Could not load manifest: {e}")
        return set()


def save_manifest(uploaded_keys: set):
    """Save uploaded keys to local manifest."""
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now().isoformat(),
        "count": len(uploaded_keys),
        "uploaded_keys": sorted(uploaded_keys)
    }
    with open(MANIFEST_FILE, 'w') as f:
        json.dump(data, f)
    logger.info(f"Manifest saved: {len(uploaded_keys)} keys")


def upload_single_file(r2_client, local_path: Path, r2_key: str, content_type: str, existing_keys: set) -> tuple:
    """Upload a single file."""
    try:
        if r2_key in existing_keys:
            return ('skipped', r2_key)

        r2_client.upload_file(
            str(local_path),
            R2_BUCKET_NAME,
            r2_key,
            ExtraArgs={'ContentType': content_type}
        )
        return ('success', r2_key)

    except Exception as e:
        logger.error(f"Error uploading {r2_key}: {e}")
        return ('failed', r2_key)


def find_all_files(directory: Path, extensions: tuple) -> list:
    """Find all files with given extensions."""
    files = []
    for p in directory.rglob("*"):
        if p.is_file() and p.suffix.lower() in extensions:
            files.append(p)
    return files


def prepare_upload_tasks(local_dir: Path, r2_prefix: str, allowed_exts: tuple) -> list:
    """Prepare upload tasks for a directory."""
    if not local_dir.exists():
        logger.warning(f"Directory not found, skipping: {local_dir}")
        return []

    local_files = find_all_files(local_dir, allowed_exts)
    tasks = []

    for local_path in local_files:
        relative = local_path.relative_to(local_dir)
        r2_key = r2_prefix + str(relative)
        tasks.append((local_path, r2_key))

    logger.info(f"{local_dir} → {r2_prefix} | {len(tasks)} files queued")
    return tasks


def main():
    parser = argparse.ArgumentParser(description="Upload files to R2")
    parser.add_argument("--sync", action="store_true",
                        help="Rebuild manifest from R2 before uploading")
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("STAGE 1 STEP 4: UPLOAD TO R2")
    logger.info("=" * 70)

    try:
        r2_client = setup_r2_client()
    except Exception as e:
        logger.error(f"Failed to setup R2 client: {e}")
        logger.error("Check your CLOUDFLARE_R2_* environment variables")
        return

    existing_keys = load_manifest()
    logger.info(f"Loaded manifest: {len(existing_keys)} keys already uploaded")

    # Define upload sets
    UPLOAD_SETS = [
        (PROJECT_ROOT / "pdf_pages", "PDF_PAGES/", (".pdf",), "application/pdf"),
        (PROJECT_ROOT / "pdf_thumbnails", "PDF_THUMBNAILS/", (".jpg", ".jpeg", ".png"), "image/jpeg"),
        (PROJECT_ROOT / "database_pdfs", "PDF/", (".pdf",), "application/pdf"),
    ]

    # Gather tasks
    all_tasks = []
    for local_dir, prefix, exts, ctype in UPLOAD_SETS:
        tasks = prepare_upload_tasks(local_dir, prefix, exts)
        tasks = [(p, key, ctype) for (p, key) in tasks]
        all_tasks.extend(tasks)

    logger.info(f"Total files to check/upload: {len(all_tasks)}")

    successful = failed = skipped = 0
    newly_uploaded = []
    lock = Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(upload_single_file, r2_client, p, key, ctype, existing_keys): (p, key)
            for (p, key, ctype) in all_tasks
        }

        with tqdm(total=len(futures), desc="Uploading to R2") as pbar:
            for future in as_completed(futures):
                status, r2_key = future.result()

                with lock:
                    if status == 'success':
                        successful += 1
                        newly_uploaded.append(r2_key)
                    elif status == 'skipped':
                        skipped += 1
                    else:
                        failed += 1

                pbar.update(1)

    # Update manifest
    if newly_uploaded:
        existing_keys.update(newly_uploaded)
        save_manifest(existing_keys)

    logger.info("=" * 70)
    logger.info(f"Upload complete!")
    logger.info(f"Uploaded: {successful}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Failed: {failed}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
