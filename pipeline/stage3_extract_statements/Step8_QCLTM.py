#!/usr/bin/env python3
"""
Step 8: QC derived LTM outputs

Summarizes the quality and coverage of:
- data/ltm_pl/*.json
- data/ltm_cf/*.json

This is a post-derivation QC pass. It does not create new values; it
checks that generated LTM periods:
- contain values
- keep source_components for all three required inputs
- retain direct provenance on those source components
- line up with the per-ticker issue summaries from Step7

Output:
- artifacts/stage3/step8_ltm_qc_summary.json
"""

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LTM_PL_DIR = PROJECT_ROOT / "data" / "ltm_pl"
LTM_CF_DIR = PROJECT_ROOT / "data" / "ltm_cf"
PL_ISSUES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_ltm_pl_qc.json"
CF_ISSUES_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step7_ltm_cf_qc.json"
OUTPUT_FILE = PROJECT_ROOT / "artifacts" / "stage3" / "step8_ltm_qc_summary.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def has_direct_provenance(component: dict) -> bool:
    return bool(component.get("source_file")) and bool(component.get("source_pages")) and bool(component.get("source_url"))


def load_issue_counts(path: Path) -> tuple[dict[str, dict], Counter]:
    if not path.exists():
        return {}, Counter()
    data = json.loads(path.read_text())
    by_ticker = {}
    totals = Counter()
    for result in data.get("results", []):
        ticker = result.get("ticker")
        by_ticker[ticker] = result.get("summary", {})
        for issue in result.get("issues", []):
            totals[issue.get("issue", "unknown_issue")] += 1
    return by_ticker, totals


def summarize_family(name: str, data_dir: Path, issues_file: Path) -> dict:
    issue_summaries, issue_totals = load_issue_counts(issues_file)

    files = sorted(data_dir.glob("*.json"))
    base_duration_counts = Counter()
    method_counts = Counter()
    coverage = Counter()
    issue_counter = Counter(issue_totals)
    tickers_with_output = 0
    generated_periods = 0

    for path in files:
        data = json.loads(path.read_text())
        periods = data.get("ltm_periods", [])
        if periods:
            tickers_with_output += 1
        generated_periods += len(periods)

        for period in periods:
            base_duration = period.get("base_duration", "unknown")
            method = period.get("method", "unknown")
            values = period.get("values", {})
            source_components = period.get("source_components", {})

            base_duration_counts[base_duration] += 1
            method_counts[method] += 1

            if values:
                coverage["nonempty_value_periods"] += 1
            else:
                issue_counter["empty_values"] += 1

            required_components = ("current", "prior_annual", "prior_same_duration")
            for key in required_components:
                component = source_components.get(key)
                if component:
                    coverage["component_present"] += 1
                    if has_direct_provenance(component):
                        coverage["component_with_direct_provenance"] += 1
                    else:
                        issue_counter["component_missing_direct_provenance"] += 1
                else:
                    issue_counter["missing_source_component"] += 1

    return {
        "family": name,
        "tickers_scanned": len(files),
        "tickers_with_output": tickers_with_output,
        "generated_ltm_periods": generated_periods,
        "base_duration_counts": dict(base_duration_counts),
        "method_counts": dict(method_counts),
        "coverage": dict(coverage),
        "step7_issue_counts": dict(issue_totals),
        "issue_counts": dict(issue_counter),
        "ticker_summaries": issue_summaries,
    }


def main() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    summary = {
        "generated_at": utc_now_iso(),
        "pl": summarize_family("profit_loss", LTM_PL_DIR, PL_ISSUES_FILE),
        "cf": summarize_family("cash_flow", LTM_CF_DIR, CF_ISSUES_FILE),
    }
    OUTPUT_FILE.write_text(json.dumps(summary, indent=2))
    print(f"Wrote LTM QC summary: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
