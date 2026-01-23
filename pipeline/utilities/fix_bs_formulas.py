#!/usr/bin/env python3
"""
Fix BS formula coverage issues.

This script fixes cases where total_assets formula doesn't include all asset rows.
For example: R=G+H+I+J+K+L+M+N+O+P+Q  should be  R=A+B+C+D+E+F+G+H+I+J+K+L+M+N+O+P+Q

Only fixes files where:
1. Accounting equation passes (total_assets ≈ total_eq_liab within 5%)
2. Formula is incomplete (starts after row A/B/C/D/E)
"""

import re
from pathlib import Path
from typing import Optional
import argparse


def parse_bs_file(filepath: Path) -> dict:
    """Parse a BS markdown file and return structured data."""
    with open(filepath) as f:
        lines = f.readlines()

    result = {
        'header_lines': [],
        'table_lines': [],
        'footer_lines': [],
        'rows': [],
        'total_assets_line_idx': None,
        'total_assets_ref': None,
        'total_assets_value': 0,
        'total_eq_liab_value': 0,
        'asset_refs': [],  # All asset refs (A, B, C, ...) before total_assets
    }

    in_table = False
    table_started = False

    for i, line in enumerate(lines):
        # Detect table header separator
        if '|:---|:---|' in line or '| :--- | :--- |' in line:
            in_table = True
            table_started = True
            result['table_lines'].append((i, line))
            continue

        if not table_started:
            result['header_lines'].append((i, line))
            continue

        if in_table and line.startswith('|'):
            result['table_lines'].append((i, line))

            # Parse the row
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 5:
                canonical = parts[2].replace('**', '').lower()
                ref = parts[3]
                val_str = parts[4].replace(',', '').replace('**', '').replace('-', '0')
                val_str = val_str.replace('(', '-').replace(')', '')
                try:
                    val = int(val_str) if val_str.strip() else 0
                except:
                    val = 0

                row_data = {
                    'line_idx': i,
                    'canonical': canonical,
                    'ref': ref,
                    'value': val,
                    'original_line': line
                }
                result['rows'].append(row_data)

                # Check for total_assets
                if 'total_assets' in canonical:
                    result['total_assets_line_idx'] = i
                    result['total_assets_ref'] = ref
                    result['total_assets_value'] = val

                # Check for total_equity_and_liabilities
                if 'total_equity_and_liabilities' in canonical:
                    result['total_eq_liab_value'] = val

                # Track asset refs (simple refs without = before total_assets)
                # Also track refs that are formulas (to exclude from asset_refs)
                if result['total_assets_line_idx'] is None:
                    if ref and ref.isalpha() and '=' not in ref:
                        result['asset_refs'].append(ref)
                    elif ref and '=' in ref:
                        # This is a subtotal - track the result ref to exclude it
                        result_ref = ref.split('=')[0]
                        if 'subtotal_refs' not in result:
                            result['subtotal_refs'] = []
                        result['subtotal_refs'].append(result_ref)
        else:
            in_table = False
            result['footer_lines'].append((i, line))

    return result


def needs_formula_fix(parsed: dict) -> tuple[bool, str]:
    """
    Check if file needs formula fix. Returns (needs_fix, reason).

    Fix logic:
    - If accounting equation PASSES: Don't fix (internally consistent even if formula looks incomplete)
    - If accounting equation FAILS: Check if sum of all assets matches eq+liab
      - If sum matches: Fix formula and value
      - If sum doesn't match: Unfixable (needs re-extraction)
    """

    if not parsed['total_assets_ref']:
        return False, "No total_assets row found"

    if not parsed['asset_refs']:
        return False, "No asset refs found"

    eq_liab = parsed['total_eq_liab_value']
    stated_assets = parsed['total_assets_value']

    if eq_liab <= 0:
        return False, "No total_eq_liab value"

    # Calculate sum of all asset refs (excluding subtotals)
    subtotal_refs = set(parsed.get('subtotal_refs', []))
    sum_all_assets = 0
    for row in parsed['rows']:
        if row['ref'] in parsed['asset_refs'] and row['ref'] not in subtotal_refs:
            sum_all_assets += row['value']

    # Check accounting equation
    eq_diff_pct = 100 * abs(stated_assets - eq_liab) / eq_liab

    if eq_diff_pct <= 5:
        # Accounting equation passes - file is internally consistent
        # Don't fix even if formula looks incomplete (changing formula would break arithmetic)
        return False, f"Accounting equation passes ({eq_diff_pct:.1f}% diff), internally consistent"

    # Accounting equation fails - check if sum of assets would fix it
    sum_diff_pct = 100 * abs(sum_all_assets - eq_liab) / eq_liab

    if sum_diff_pct <= 5:
        # Sum of all assets matches eq+liab - can fix by updating formula and value
        return True, f"Eq fails ({eq_diff_pct:.1f}%) but sum matches ({sum_diff_pct:.1f}%), fixable"
    else:
        # Neither stated value nor sum matches - needs re-extraction
        return False, f"Eq fails ({eq_diff_pct:.1f}%) and sum mismatch ({sum_diff_pct:.1f}%), needs re-extract"


def fix_formula(parsed: dict) -> str:
    """
    Generate the fixed formula line.
    Updates both the formula AND the value to match the sum of all assets.
    """

    if parsed['total_assets_line_idx'] is None:
        return None

    # Get subtotal refs to exclude (these are intermediate sums like total_current_assets)
    subtotal_refs = set(parsed.get('subtotal_refs', []))

    # Filter out subtotal refs from asset refs
    clean_asset_refs = [r for r in parsed['asset_refs'] if r not in subtotal_refs]

    # Calculate the new value (sum of all asset refs)
    new_values = []
    num_periods = 0
    for row in parsed['rows']:
        if row['ref'] in clean_asset_refs:
            if num_periods == 0:
                # Determine number of periods from first asset row
                # Read the full row to get all values
                pass

    # Re-parse to get all period values
    ref_values = {}  # ref -> list of values for each period
    for row in parsed['rows']:
        if row['ref'] in clean_asset_refs:
            ref_values[row['ref']] = []
            # Parse all values from original line
            parts = row['original_line'].split('|')
            for part in parts[4:]:  # Values start at column 4
                val_str = part.strip().replace(',', '').replace('**', '').replace('-', '0')
                val_str = val_str.replace('(', '-').replace(')', '')
                try:
                    val = int(val_str) if val_str.strip() else 0
                except:
                    val = 0
                if val_str.strip():  # Only add if there was a value
                    ref_values[row['ref']].append(val)

    # Calculate sums for each period
    if not ref_values:
        return None

    num_periods = max(len(vals) for vals in ref_values.values())
    period_sums = [0] * num_periods
    for ref in clean_asset_refs:
        vals = ref_values.get(ref, [])
        for i, v in enumerate(vals):
            if i < num_periods:
                period_sums[i] += v

    # Find the total_assets row and build new line
    for row in parsed['rows']:
        if row['line_idx'] == parsed['total_assets_line_idx']:
            original_line = row['original_line']
            old_ref = row['ref']

            # Get the result ref (the part before =)
            if '=' in old_ref:
                result_ref = old_ref.split('=')[0]
            else:
                result_ref = old_ref

            # Build new formula with all asset refs (excluding subtotals)
            new_formula = f"{result_ref}={'+'.join(clean_asset_refs)}"

            # Replace both formula and values in the line
            parts = original_line.split('|')
            if len(parts) >= 5:
                parts[3] = f" {new_formula} "
                # Update values
                for i, new_sum in enumerate(period_sums):
                    if 4 + i < len(parts):
                        parts[4 + i] = f" {new_sum:,} "
                return '|'.join(parts)

    return None


def fix_file(filepath: Path, dry_run: bool = False) -> tuple[bool, str]:
    """Fix a single BS file. Returns (fixed, message)."""

    parsed = parse_bs_file(filepath)
    needs_fix, reason = needs_formula_fix(parsed)

    if not needs_fix:
        return False, reason

    fixed_line = fix_formula(parsed)
    if not fixed_line:
        return False, "Could not generate fixed line"

    if dry_run:
        return True, f"Would fix: {reason}"

    # Read original file
    with open(filepath) as f:
        lines = f.readlines()

    # Replace the line
    lines[parsed['total_assets_line_idx']] = fixed_line

    # Write back
    with open(filepath, 'w') as f:
        f.writelines(lines)

    return True, f"Fixed: {reason}"


def main():
    parser = argparse.ArgumentParser(description="Fix BS formula coverage issues")
    parser.add_argument("--dry-run", action="store_true", help="Don't modify files, just report")
    parser.add_argument("--file", type=str, help="Fix a single file")
    args = parser.parse_args()

    bs_dir = Path("data/extracted_bs")

    if args.file:
        files = [Path(args.file)]
    else:
        files = sorted(bs_dir.glob("*.md"))

    stats = {
        'total': 0,
        'fixed': 0,
        'internally_consistent': 0,  # Eq passes, no fix needed
        'need_reextract': 0,  # Eq fails, sum doesn't match
        'no_data': 0,  # Missing total_assets or refs
    }

    for filepath in files:
        stats['total'] += 1
        fixed, message = fix_file(filepath, dry_run=args.dry_run)

        if fixed:
            stats['fixed'] += 1
            print(f"{'[DRY-RUN] ' if args.dry_run else ''}Fixed: {filepath.name}")
            print(f"  {message}")
        elif "passes" in message.lower() or "internally consistent" in message.lower():
            stats['internally_consistent'] += 1
        elif "re-extract" in message.lower() or "mismatch" in message.lower():
            stats['need_reextract'] += 1
            if stats['need_reextract'] <= 5:
                print(f"Need re-extract: {filepath.name}")
                print(f"  {message}")
        else:
            stats['no_data'] += 1

    print("\n" + "=" * 70)
    print(f"Total files:              {stats['total']}")
    print(f"Fixed (post-process):     {stats['fixed']}")
    print(f"Internally consistent:    {stats['internally_consistent']} (eq passes, no fix needed)")
    print(f"Need LLM re-extraction:   {stats['need_reextract']} (eq fails, sum mismatch)")
    print(f"Missing data:             {stats['no_data']} (no total_assets or refs)")
    print("=" * 70)


if __name__ == "__main__":
    main()
