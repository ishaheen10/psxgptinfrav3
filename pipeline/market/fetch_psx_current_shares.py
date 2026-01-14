#!/usr/bin/env python3
"""
Fetch current shares outstanding from PSX for all tickers.
Outputs: psx_current_shares.json
"""

import json
import re
import time
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = Path(__file__).parent.parent.parent
TICKERS_FILE = BASE_DIR / "market" / "tickers100_full.json"
OUTPUT_FILE = BASE_DIR / "psx_current_shares.json"

PSX_URL = "https://dps.psx.com.pk/company/{ticker}"


def fetch_shares(ticker: str) -> dict:
    """Fetch shares data from PSX for a single ticker."""
    try:
        resp = requests.get(PSX_URL.format(ticker=ticker), timeout=15)
        text = resp.text

        result = {"ticker": ticker, "success": False}

        # Primary pattern: stats_label">Shares</div><div class="stats_value">1,465,000,000</div>
        match = re.search(
            r'stats_label">\s*Shares\s*</div>\s*<div[^>]*class="stats_value"[^>]*>\s*([\d,]+)\s*</div>',
            text, re.I
        )
        if match:
            value_str = match.group(1).replace(',', '')
            result["total_shares"] = int(value_str)
            result["source"] = "stats_value"
            result["success"] = True
            return result

        # Fallback: look for any "Shares" followed by a large number
        match = re.search(r'>Shares<[^>]*>[^<]*<[^>]*>([\d,]{7,})<', text, re.I)
        if match:
            value_str = match.group(1).replace(',', '')
            result["total_shares"] = int(value_str)
            result["source"] = "shares_pattern"
            result["success"] = True
            return result

        # Last resort: find free float and calculate
        ff_match = re.search(r'Free\s*Float[^<]*<[^>]*>([\d,]+)<', text, re.I)
        pct_match = re.search(r'([\d.]+)\s*%\s*</div>', text)
        if ff_match and pct_match:
            free_float = int(ff_match.group(1).replace(',', ''))
            pct = float(pct_match.group(1))
            if pct > 0:
                result["total_shares"] = int(free_float / (pct / 100))
                result["source"] = "calculated"
                result["success"] = True

        return result

    except Exception as e:
        return {"ticker": ticker, "success": False, "error": str(e)}


def main():
    # Load tickers
    with open(TICKERS_FILE) as f:
        tickers_data = json.load(f)
    tickers = [t["Symbol"] for t in tickers_data]

    print(f"Fetching shares for {len(tickers)} tickers from PSX...")

    results = {}
    success = 0
    failed = 0

    # Use thread pool for parallel fetching
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_shares, t): t for t in tickers}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            ticker = result["ticker"]

            if result["success"]:
                results[ticker] = result["total_shares"]
                success += 1
                status = f"{result['total_shares']:,}"
            else:
                failed += 1
                status = f"FAILED: {result.get('error', 'no data')[:30]}"

            if i % 20 == 0 or not result["success"]:
                print(f"  [{i}/{len(tickers)}] {ticker}: {status}")

            time.sleep(0.1)  # Small delay to be nice to server

    # Write output
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nDone!")
    print(f"  Success: {success}")
    print(f"  Failed: {failed}")
    print(f"  Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
