#!/usr/bin/env python3
"""
Step 7: Derive LTM (Trailing Twelve Months) P&L

Derives LTM periods from REPORTED cumulative periods only:
- Q1 LTM = current 3M + prior 12M - prior 3M
- Q2 LTM = current 6M + prior 12M - prior 6M
- Q3 LTM = current 9M + prior 12M - prior 9M

This intentionally avoids using derived quarterly 3M periods as LTM inputs.

Input:  data/json_pl/*.json
Output: data/ltm_pl/*.json
        artifacts/stage3/step7_ltm_pl_qc.json
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "json_pl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "ltm_pl"
QC_OUTPUT = PROJECT_ROOT / "artifacts" / "stage3" / "step7_ltm_pl_qc.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_value(value: float, unit_type: str, canonical: str | None = None) -> float | None:
    if value is None:
        return None
    if canonical and "eps" in canonical.lower():
        return value
    unit_lower = unit_type.lower().strip() if unit_type else "thousands"
    if unit_lower in ("rupees", "rupee", "full_rupees"):
        return value / 1000.0
    if unit_lower == "millions":
        return value * 1000.0
    return value


def normalize_period(period: dict) -> dict:
    unit_type = period.get("unit_type", "thousands")
    values = period.get("values", {})
    normalized = {}
    for canonical, entry in values.items():
        if isinstance(entry, dict):
            raw_value = entry.get("value")
        else:
            raw_value = entry
        normalized[canonical] = normalize_value(raw_value, unit_type, canonical)
    out = dict(period)
    out["unit_type"] = "thousands"
    out["values"] = normalized
    return out


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def previous_year_same_date(date_str: str) -> str:
    dt = parse_date(date_str)
    try:
        return dt.replace(year=dt.year - 1).strftime("%Y-%m-%d")
    except ValueError:
        # Handle leap-day edge cases conservatively
        return dt.replace(year=dt.year - 1, day=28).strftime("%Y-%m-%d")


def load_periods(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    periods = [normalize_period(p) for p in data.get("periods", [])]
    return periods


def build_indexes(periods: list[dict]) -> tuple[dict, dict]:
    by_key = {}
    annuals_by_scope = {}
    for period in periods:
        key = (
            period.get("consolidation"),
            period.get("period_end"),
            period.get("duration"),
        )
        by_key[key] = period
        if period.get("duration") == "12M":
            annuals_by_scope.setdefault(period.get("consolidation"), []).append(period)
    for scope_periods in annuals_by_scope.values():
        scope_periods.sort(key=lambda p: p["period_end"])
    return by_key, annuals_by_scope


def latest_annual_before(annuals: list[dict], current_end: str) -> dict | None:
    candidate = None
    for period in annuals:
        if period["period_end"] < current_end:
            candidate = period
        else:
            break
    return candidate


def has_direct_provenance(period: dict) -> bool:
    return bool(period.get("source_file")) and bool(period.get("source_pages")) and bool(period.get("source_url"))


def get_period_source_labels(period: dict | None) -> dict:
    if not period:
        return {}
    return period.get("source_items") or period.get("source_labels") or {}


def derive_ltm_period(current: dict, prior_annual: dict, prior_same: dict) -> tuple[dict | None, list[str]]:
    issues = []
    if not (has_direct_provenance(current) and has_direct_provenance(prior_annual) and has_direct_provenance(prior_same)):
        issues.append("missing_direct_provenance_component")
        return None, issues

    current_values = current.get("values", {})
    annual_values = prior_annual.get("values", {})
    prior_values = prior_same.get("values", {})

    common_fields = set(current_values) & set(annual_values) & set(prior_values)
    derived_values = {}
    for field in sorted(common_fields):
        c = current_values.get(field)
        a = annual_values.get(field)
        p = prior_values.get(field)
        if c is None or a is None or p is None:
            continue
        derived_values[field] = c + a - p

    if not derived_values:
        issues.append("no_common_numeric_fields")
        return None, issues

    duration = current["duration"]
    method = f"LTM_{duration}+prior12M-prior{duration}"
    result = {
        "period_end": current["period_end"],
        "duration": "LTM",
        "base_duration": duration,
        "fiscal_year": current.get("fiscal_year"),
        "consolidation": current.get("consolidation"),
        "unit_type": "thousands",
        "method": method,
        "values": derived_values,
        "source_labels": get_period_source_labels(current),
        "source_components": {
            "current": {
                "period_end": current.get("period_end"),
                "duration": current.get("duration"),
                "source_file": current.get("source_file"),
                "source_pages": current.get("source_pages"),
                "source_url": current.get("source_url"),
            },
            "prior_annual": {
                "period_end": prior_annual.get("period_end"),
                "duration": prior_annual.get("duration"),
                "source_file": prior_annual.get("source_file"),
                "source_pages": prior_annual.get("source_pages"),
                "source_url": prior_annual.get("source_url"),
            },
            "prior_same_duration": {
                "period_end": prior_same.get("period_end"),
                "duration": prior_same.get("duration"),
                "source_file": prior_same.get("source_file"),
                "source_pages": prior_same.get("source_pages"),
                "source_url": prior_same.get("source_url"),
            },
        },
        "qc_flag": "",
    }
    return result, issues


def process_ticker(path: Path) -> tuple[dict, dict]:
    raw = json.loads(path.read_text())
    ticker = raw["ticker"]
    periods = [normalize_period(p) for p in raw.get("periods", [])]
    by_key, annuals_by_scope = build_indexes(periods)

    ltm_periods = []
    qc_issues = []
    counters = {
        "candidates": 0,
        "generated": 0,
        "missing_prior_annual": 0,
        "missing_prior_same_duration": 0,
        "qc_failures": 0,
    }

    for current in periods:
        duration = current.get("duration")
        if duration not in ("3M", "6M", "9M"):
            continue
        counters["candidates"] += 1
        scope = current.get("consolidation")
        current_end = current.get("period_end")
        prior_annual = latest_annual_before(annuals_by_scope.get(scope, []), current_end)
        if not prior_annual:
            counters["missing_prior_annual"] += 1
            qc_issues.append({
                "ticker": ticker,
                "period_end": current_end,
                "consolidation": scope,
                "base_duration": duration,
                "issue": "missing_prior_annual",
            })
            continue

        prior_same_end = previous_year_same_date(current_end)
        prior_same = by_key.get((scope, prior_same_end, duration))
        if not prior_same:
            counters["missing_prior_same_duration"] += 1
            qc_issues.append({
                "ticker": ticker,
                "period_end": current_end,
                "consolidation": scope,
                "base_duration": duration,
                "issue": "missing_prior_same_duration",
                "expected_period_end": prior_same_end,
            })
            continue

        ltm_period, issues = derive_ltm_period(current, prior_annual, prior_same)
        if issues:
            counters["qc_failures"] += 1
            qc_issues.append({
                "ticker": ticker,
                "period_end": current_end,
                "consolidation": scope,
                "base_duration": duration,
                "issue": "derive_ltm_failed",
                "details": issues,
            })
            continue

        ltm_periods.append(ltm_period)
        counters["generated"] += 1

    output = {
        "ticker": ticker,
        "generated_at": utc_now_iso(),
        "ltm_periods": sorted(ltm_periods, key=lambda p: (p["consolidation"], p["period_end"])),
    }
    qc = {
        "ticker": ticker,
        "summary": counters,
        "issues": qc_issues,
    }
    return output, qc


def main():
    parser = argparse.ArgumentParser(description="Derive LTM P&L periods from reported cumulative periods")
    parser.add_argument("--ticker", help="Process a single ticker")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    QC_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(INPUT_DIR.glob("*.json"))
    if args.ticker:
        files = [p for p in files if p.stem == args.ticker]

    qc_results = []
    total_generated = 0
    for path in files:
        output, qc = process_ticker(path)
        (OUTPUT_DIR / path.name).write_text(json.dumps(output, indent=2))
        qc_results.append(qc)
        total_generated += qc["summary"]["generated"]

    QC_OUTPUT.write_text(json.dumps({
        "generated_at": utc_now_iso(),
        "results": qc_results,
        "summary": {
            "tickers": len(qc_results),
            "generated_ltm_periods": total_generated,
        },
    }, indent=2))

    print(f"Generated LTM P&L for {len(qc_results)} tickers")
    print(f"Total LTM periods: {total_generated}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"QC: {QC_OUTPUT}")


if __name__ == "__main__":
    main()
