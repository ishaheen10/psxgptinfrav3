#!/usr/bin/env python3
"""
Step 4: Rebuild universal ratios on Cloudflare D1.

Drops and recreates the `ratios` table, then repopulates it from the
already-uploaded `financial_statements` table.

Usage:
    python3 Step4_RebuildRatios.py
    python3 Step4_RebuildRatios.py --sql-only
"""

import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "stage5"
D1_DATABASE = "psx"


def build_sql() -> str:
    return """
DROP TABLE IF EXISTS ratios;

CREATE TABLE ratios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT NOT NULL,
  period_end TEXT NOT NULL,
  period_duration TEXT NOT NULL,
  section TEXT NOT NULL,
  ratio_name TEXT NOT NULL,
  value REAL,
  computed_at TEXT DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  p.ticker,
  p.period_end,
  p.period_duration,
  p.section,
  'roe' AS ratio_name,
  p.value * 1.0 / NULLIF(b.value, 0) AS value
FROM financial_statements p
JOIN financial_statements b
  ON b.ticker = p.ticker
 AND b.period_end = p.period_end
 AND b.section = p.section
WHERE p.section IN ('consolidated', 'unconsolidated')
  AND p.statement_type = 'profit_loss'
  AND p.canonical_name = 'net_profit'
  AND p.period_duration IN ('12M', 'LTM')
  AND p.value IS NOT NULL
  AND b.statement_type = 'balance_sheet'
  AND b.canonical_name = 'total_equity'
  AND b.period_duration = 'PIT'
  AND b.value IS NOT NULL;

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  p.ticker,
  p.period_end,
  p.period_duration,
  p.section,
  'roa' AS ratio_name,
  p.value * 1.0 / NULLIF(b.value, 0) AS value
FROM financial_statements p
JOIN financial_statements b
  ON b.ticker = p.ticker
 AND b.period_end = p.period_end
 AND b.section = p.section
WHERE p.section IN ('consolidated', 'unconsolidated')
  AND p.statement_type = 'profit_loss'
  AND p.canonical_name = 'net_profit'
  AND p.period_duration IN ('12M', 'LTM')
  AND p.value IS NOT NULL
  AND b.statement_type = 'balance_sheet'
  AND b.canonical_name = 'total_assets'
  AND b.period_duration = 'PIT'
  AND b.value IS NOT NULL;

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  ticker,
  period_end,
  period_duration,
  section,
  'net_margin' AS ratio_name,
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'net_profit'
    THEN value END) * 1.0
  / NULLIF(MAX(CASE
      WHEN statement_type = 'profit_loss'
        AND canonical_name = 'revenue_net'
      THEN value END), 0) AS value
FROM financial_statements
WHERE section IN ('consolidated', 'unconsolidated')
  AND statement_type = 'profit_loss'
  AND period_duration IN ('3M', '6M', '9M', '12M', 'LTM')
  AND canonical_name IN ('net_profit', 'revenue_net')
GROUP BY ticker, period_end, section, period_duration
HAVING
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'net_profit'
    THEN value END) IS NOT NULL
  AND
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'revenue_net'
    THEN value END) IS NOT NULL
  AND
  NULLIF(MAX(CASE
      WHEN statement_type = 'profit_loss'
        AND canonical_name = 'revenue_net'
      THEN value END), 0) IS NOT NULL;

-- net_margin for bank tickers: use total_income instead of revenue_net
-- Bank tickers (Industry = 'Banking' in tickers100.json):
-- ABL, AKBL, BAFL, BAHL, BIPL, BOP, FABL, HBL, HMB, MCB, MEBL, NBP, SCBPL, UBL
INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  ticker,
  period_end,
  period_duration,
  section,
  'net_margin' AS ratio_name,
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'net_profit'
    THEN value END) * 1.0
  / NULLIF(MAX(CASE
      WHEN statement_type = 'profit_loss'
        AND canonical_name = 'total_income'
      THEN value END), 0) AS value
FROM financial_statements
WHERE section IN ('consolidated', 'unconsolidated')
  AND statement_type = 'profit_loss'
  AND period_duration IN ('3M', '6M', '9M', '12M', 'LTM')
  AND canonical_name IN ('net_profit', 'total_income')
  AND ticker IN ('ABL', 'AKBL', 'BAFL', 'BAHL', 'BIPL', 'BOP', 'FABL', 'HBL', 'HMB', 'MCB', 'MEBL', 'NBP', 'SCBPL', 'UBL')
GROUP BY ticker, period_end, section, period_duration
HAVING
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'net_profit'
    THEN value END) IS NOT NULL
  AND
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'total_income'
    THEN value END) IS NOT NULL
  AND
  NULLIF(MAX(CASE
      WHEN statement_type = 'profit_loss'
        AND canonical_name = 'total_income'
      THEN value END), 0) IS NOT NULL;

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  ticker,
  period_end,
  period_duration,
  section,
  'gross_margin' AS ratio_name,
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'gross_profit'
    THEN value END) * 1.0
  / NULLIF(MAX(CASE
      WHEN statement_type = 'profit_loss'
        AND canonical_name = 'revenue_net'
      THEN value END), 0) AS value
FROM financial_statements
WHERE section IN ('consolidated', 'unconsolidated')
  AND statement_type = 'profit_loss'
  AND period_duration IN ('3M', '6M', '9M', '12M', 'LTM')
  AND canonical_name IN ('gross_profit', 'revenue_net')
GROUP BY ticker, period_end, section, period_duration
HAVING
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'gross_profit'
    THEN value END) IS NOT NULL
  AND
  MAX(CASE
    WHEN statement_type = 'profit_loss'
      AND canonical_name = 'revenue_net'
    THEN value END) IS NOT NULL
  AND
  NULLIF(MAX(CASE
      WHEN statement_type = 'profit_loss'
        AND canonical_name = 'revenue_net'
      THEN value END), 0) IS NOT NULL;

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  ticker,
  period_end,
  period_duration,
  section,
  'current_ratio' AS ratio_name,
  MAX(CASE
    WHEN statement_type = 'balance_sheet'
      AND canonical_name = 'total_current_assets'
      AND period_duration = 'PIT'
    THEN value END) * 1.0
  / NULLIF(MAX(CASE
      WHEN statement_type = 'balance_sheet'
        AND canonical_name = 'total_current_liabilities'
        AND period_duration = 'PIT'
      THEN value END), 0) AS value
FROM financial_statements
WHERE section IN ('consolidated', 'unconsolidated')
  AND statement_type = 'balance_sheet'
  AND period_duration = 'PIT'
  AND canonical_name IN ('total_current_assets', 'total_current_liabilities')
GROUP BY ticker, period_end, section, period_duration
HAVING
  MAX(CASE
    WHEN statement_type = 'balance_sheet'
      AND canonical_name = 'total_current_assets'
      AND period_duration = 'PIT'
    THEN value END) IS NOT NULL
  AND
  MAX(CASE
    WHEN statement_type = 'balance_sheet'
      AND canonical_name = 'total_current_liabilities'
      AND period_duration = 'PIT'
    THEN value END) IS NOT NULL
  AND
  NULLIF(MAX(CASE
      WHEN statement_type = 'balance_sheet'
        AND canonical_name = 'total_current_liabilities'
        AND period_duration = 'PIT'
      THEN value END), 0) IS NOT NULL;

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  ticker,
  period_end,
  period_duration,
  section,
  'debt_to_equity' AS ratio_name,
  MAX(CASE
    WHEN statement_type = 'balance_sheet'
      AND canonical_name = 'total_liabilities'
      AND period_duration = 'PIT'
    THEN value END) * 1.0
  / NULLIF(MAX(CASE
      WHEN statement_type = 'balance_sheet'
        AND canonical_name = 'total_equity'
        AND period_duration = 'PIT'
      THEN value END), 0) AS value
FROM financial_statements
WHERE section IN ('consolidated', 'unconsolidated')
  AND statement_type = 'balance_sheet'
  AND period_duration = 'PIT'
  AND canonical_name IN ('total_liabilities', 'total_equity')
GROUP BY ticker, period_end, section, period_duration
HAVING
  MAX(CASE
    WHEN statement_type = 'balance_sheet'
      AND canonical_name = 'total_liabilities'
      AND period_duration = 'PIT'
    THEN value END) IS NOT NULL
  AND
  MAX(CASE
    WHEN statement_type = 'balance_sheet'
      AND canonical_name = 'total_equity'
      AND period_duration = 'PIT'
    THEN value END) IS NOT NULL
  AND
  NULLIF(MAX(CASE
      WHEN statement_type = 'balance_sheet'
        AND canonical_name = 'total_equity'
        AND period_duration = 'PIT'
      THEN value END), 0) IS NOT NULL;

INSERT INTO ratios (ticker, period_end, period_duration, section, ratio_name, value)
SELECT
  p.ticker,
  p.period_end,
  p.period_duration,
  p.section,
  'asset_turnover' AS ratio_name,
  p.value * 1.0 / NULLIF(b.value, 0) AS value
FROM financial_statements p
JOIN financial_statements b
  ON b.ticker = p.ticker
 AND b.period_end = p.period_end
 AND b.section = p.section
WHERE p.section IN ('consolidated', 'unconsolidated')
  AND p.statement_type = 'profit_loss'
  AND p.canonical_name = 'revenue_net'
  AND p.period_duration IN ('12M', 'LTM')
  AND p.value IS NOT NULL
  AND b.statement_type = 'balance_sheet'
  AND b.canonical_name = 'total_assets'
  AND b.period_duration = 'PIT'
  AND b.value IS NOT NULL;
""".strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild universal ratios on D1")
    parser.add_argument("--sql-only", action="store_true", help="Write SQL file only")
    args = parser.parse_args()

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    sql = build_sql()
    sql_path = ARTIFACTS_DIR / "step4_rebuild_ratios.sql"
    sql_path.write_text(sql)
    print(f"SQL written to: {sql_path}")

    if args.sql_only:
        return 0

    result = subprocess.run(
        [
            "npx",
            "wrangler",
            "d1",
            "execute",
            D1_DATABASE,
            "--remote",
            f"--file={sql_path}",
        ],
        cwd=PROJECT_ROOT,
        text=True,
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
