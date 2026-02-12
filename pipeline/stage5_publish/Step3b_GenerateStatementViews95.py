#!/usr/bin/env python3
"""
Step3b_GenerateStatementViews95.py - Generate statement CSVs with 95% canonical coverage

Dynamic approach: displays all non-null canonicals from 95% coverage list,
organized by section, with "Other" capturing the residual.

Usage:
    python pipeline/stage5_publish/Step3b_GenerateStatementViews95.py ABL
    python pipeline/stage5_publish/Step3b_GenerateStatementViews95.py --all
    python pipeline/stage5_publish/Step3b_GenerateStatementViews95.py --type BANK
"""

import argparse
import json
import csv
from pathlib import Path
from collections import OrderedDict

PROJECT_ROOT = Path(__file__).parent.parent.parent
JSON_PL_DIR = PROJECT_ROOT / "data" / "json_pl"
JSON_CF_DIR = PROJECT_ROOT / "data" / "quarterly_cf"
JSON_BS_DIR = PROJECT_ROOT / "data" / "json_bs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "excel"

# ============================================================
# 95% COVERAGE CANONICAL LISTS (organized by section)
# ============================================================

# P&L sections with canonicals and display labels
# Each section is independent - items listed are ONLY those that should sum to the subtotal
PL_SECTIONS = OrderedDict([
    ("NET_INTEREST", {
        "subtotal": "net_interest_income",
        "label": "Net Interest Income",
        "items": OrderedDict([
            ("bank_interest_income", "Interest Income"),
            ("bank_interest_expense", "Interest Expense"),
        ])
    }),
    ("GROSS_PROFIT", {
        "subtotal": "gross_profit",
        "label": "Gross Profit",
        "items": OrderedDict([
            ("revenue_net", "Revenue"),
            ("cost_of_revenue", "Cost of Sales"),
        ])
    }),
    ("UNDERWRITING", {
        "subtotal": "underwriting_profit",
        "label": "Underwriting Profit",
        "items": OrderedDict([
            ("net_premium", "Net Premium"),
            ("claims_expense", "Claims Expense"),
            ("commission_expense", "Commission Expense"),
            ("other_underwriting", "Reinsurance & Reserves"),
            ("other_insurance_adjustments", "Insurance Adjustments"),
            ("operating_expenses", "Management Expenses"),
        ])
    }),
    ("PBT_BANK", {
        "subtotal": "profit_before_tax",
        "label": "Profit Before Tax",
        "items": OrderedDict([
            ("net_interest_income", "Net Interest Income"),
            ("fee_income", "Fee & Commission"),
            ("dividend_income", "Dividend Income"),
            ("fx_income", "FX Income"),
            ("trading_gains", "Trading Gains"),
            ("other_non_interest_income", "Other Non-Interest Income"),
            ("operating_expenses", "Operating Expenses"),
            ("provisions", "Provisions"),
            ("workers_welfare_fund", "Workers Welfare Fund"),
            ("other_charges", "Other Charges"),
            ("other_bank_adjustments", "Other Bank Adjustments"),
        ])
    }),
    ("PBT_INSURANCE", {
        "subtotal": "profit_before_tax",
        "label": "Profit Before Tax",
        "items": OrderedDict([
            ("underwriting_profit", "Underwriting Profit"),
            ("investment_income", "Investment Income"),
            ("other_income", "Other Income"),
            ("finance_cost", "Finance Costs"),
        ])
    }),
    ("PBT_CORPORATE", {
        "subtotal": "profit_before_tax",
        "label": "Profit Before Tax",
        "items": OrderedDict([
            ("gross_profit", "Gross Profit"),
            ("operating_expenses", "Operating Expenses"),
            ("administrative_expenses", "Admin Expenses"),
            ("other_income", "Other Income"),
            ("other_operating", "Other Operating"),
            ("other_expenses", "Other Expenses"),
            ("other_non_operating", "Other Non-Operating"),
            ("finance_cost", "Finance Costs"),
            ("share_of_associates", "Share of Associates"),
            ("provisions", "Provisions"),
            ("workers_welfare_fund", "Workers Welfare Fund"),
            ("other_charges", "Other Charges"),
            ("levy", "Levy"),
            ("exploration_cost", "Exploration Cost"),
            ("royalties", "Royalties"),
        ])
    }),
])

PL_FOOTER = OrderedDict([
    ("taxation", "Taxation"),
    ("taxation_current", "Taxation (Current)"),
    ("taxation_deferred", "Taxation (Deferred)"),
    ("taxation_prior", "Taxation (Prior)"),
    ("net_profit", "Net Profit"),
    ("net_profit_parent", "Net Profit (Parent)"),
    ("nci_income", "NCI Income"),
    ("net_profit_continuing", "Net Profit (Continuing)"),
    ("net_profit_discontinued", "Net Profit (Discontinued)"),
    ("other_comprehensive_income", "Other Comprehensive Income"),
    ("total_comprehensive_income", "Total Comprehensive Income"),
    ("eps", "EPS"),
    ("eps_diluted", "EPS (Diluted)"),
    ("eps_continuing", "EPS (Continuing)"),
])

# CF sections
CF_SECTIONS = OrderedDict([
    ("CFO", {
        "subtotal": "cfo",
        "label": "Cash from Operations",
        "items": OrderedDict([
            ("net_profit_cf_start", "Net Profit (Start)"),
            ("profit_before_tax", "Profit Before Tax"),
            ("depreciation_amortization", "Depreciation & Amortization"),
            ("amortization", "Amortization"),
            ("depreciation_right_of_use_assets", "Depreciation (RoU Assets)"),
            ("provisions_cf", "Provisions"),
            ("gain_on_sale_fixed_assets", "Gain on Asset Sales"),
            ("share_of_profit_associates", "Share of Associates"),
            ("unrealized_gain_on_investments", "Unrealized Gains"),
            ("unrealized_loss_trading", "Unrealized Losses"),
            ("gain_on_securities", "Gain on Securities"),
            ("fair_value_gains_losses", "Fair Value Changes"),
            ("defined_benefits_paid", "Defined Benefits Paid"),
            ("interest_expensed_lease", "Interest on Leases"),
            ("workers_welfare_fund", "Workers Welfare Fund"),
            ("dividend_income", "Dividend Income Adj"),
            ("fx_adjustment", "FX Adjustment"),
            ("fx_effect_on_cash", "FX Effect on Cash"),
            ("finance_cost_paid", "Finance Cost Paid"),
            ("interest_paid", "Interest Paid"),
            ("other_operating_adjustments", "Other Operating Adj"),
            ("adjustments_subtotal", "Adjustments Subtotal"),
            ("change_in_operating_assets", "Δ Operating Assets"),
            ("change_in_operating_liabilities", "Δ Operating Liabilities"),
            ("change_in_receivables", "Δ Receivables"),
            ("change_in_payables", "Δ Payables"),
            ("change_in_inventory", "Δ Inventory"),
            ("change_in_other_working_capital", "Δ Other Working Capital"),
            ("cash_generated_from_operations", "Cash Generated from Ops"),
            ("taxes_paid", "Taxes Paid"),
        ])
    }),
    ("CFI", {
        "subtotal": "cfi",
        "label": "Cash from Investing",
        "items": OrderedDict([
            ("capex", "Capex"),
            ("asset_sales", "Asset Sales"),
            ("investment_purchases", "Investment Purchases"),
            ("investment_sales", "Investment Sales"),
            ("acquisitions", "Acquisitions"),
            ("dividend_received", "Dividends Received"),
            ("interest_received", "Interest Received"),
            ("other_investing", "Other Investing"),
        ])
    }),
    ("CFF", {
        "subtotal": "cff",
        "label": "Cash from Financing",
        "items": OrderedDict([
            ("dividends_paid", "Dividends Paid"),
            ("borrowings_raised", "Borrowings Raised"),
            ("borrowings_repaid", "Borrowings Repaid"),
            ("borrowings_raised_repaid", "Net Borrowings"),
            ("change_in_borrowings", "Δ Borrowings"),
            ("equity_issued", "Equity Issued"),
            ("equity_repurchased", "Equity Repurchased"),
            ("lease_principal_paid", "Lease Payments"),
            ("other_financing", "Other Financing"),
        ])
    }),
])

CF_FOOTER = OrderedDict([
    ("net_cash_change", "Net Change in Cash"),
    ("cash_start", "Cash at Beginning"),
    ("cash_end", "Cash at End"),
])

# BS sections - structured as Current/Non-Current to avoid double-counting
BS_SECTIONS = OrderedDict([
    ("CURRENT_ASSETS", {
        "subtotal": "total_current_assets",
        "label": "Total Current Assets",
        "items": OrderedDict([
            ("cash_and_equivalents", "Cash & Equivalents"),
            ("cash_and_bank_balances", "Cash & Bank Balances"),
            ("cash", "Cash"),
            ("bank_balances", "Bank Balances"),
            ("cash_and_balances_with_treasury_banks", "Cash with Treasury"),
            ("balances_with_other_banks", "Balances with Banks"),
            ("short_term_investments", "Short-term Investments"),
            ("lending_to_fis", "Lending to FIs"),
            ("advances", "Advances"),
            ("receivables", "Receivables"),
            ("other_receivables", "Other Receivables"),
            ("inventory", "Inventory"),
            ("prepayments", "Prepayments"),
            ("contract_assets", "Contract Assets"),
            ("other_current_assets", "Other Current Assets"),
        ])
    }),
    ("NON_CURRENT_ASSETS", {
        "subtotal": "total_non_current_assets",
        "label": "Total Non-Current Assets",
        "items": OrderedDict([
            ("long_term_investments", "Long-term Investments"),
            ("investments", "Investments"),
            ("property_equipment", "Property & Equipment"),
            ("intangibles", "Intangibles"),
            ("right_of_use_assets", "Right of Use Assets"),
            ("investment_property", "Investment Property"),
            ("deferred_tax_assets", "Deferred Tax Assets"),
            ("long_term_deposits", "Long-term Deposits"),
            ("long_term_loans", "Long-term Loans"),
            ("biological_assets", "Biological Assets"),
            ("other_non_current_assets", "Other Non-Current Assets"),
        ])
    }),
    ("CURRENT_LIABILITIES", {
        "subtotal": "total_current_liabilities",
        "label": "Total Current Liabilities",
        "items": OrderedDict([
            ("bills_payable", "Bills Payable"),
            ("short_term_debt", "Short-term Debt"),
            ("borrowings", "Borrowings"),
            ("payables", "Payables"),
            ("lease_liabilities_current", "Lease Liabilities (Current)"),
            ("contract_liabilities", "Contract Liabilities"),
            ("unclaimed_dividend", "Unclaimed Dividend"),
            ("provisions", "Provisions"),
            ("other_current_liabilities", "Other Current Liabilities"),
        ])
    }),
    ("NON_CURRENT_LIABILITIES", {
        "subtotal": "total_non_current_liabilities",
        "label": "Total Non-Current Liabilities",
        "items": OrderedDict([
            ("long_term_debt", "Long-term Debt"),
            ("deposits", "Deposits"),
            ("deferred_tax_liability", "Deferred Tax Liability"),
            ("lease_liabilities", "Lease Liabilities"),
            ("lease_liabilities_non_current", "Lease Liabilities (Non-Current)"),
            ("deferred_liabilities", "Deferred Liabilities"),
            ("employee_benefit_obligations", "Employee Benefits"),
            ("other_non_current_liabilities", "Other Non-Current Liabilities"),
        ])
    }),
    ("EQUITY", {
        "subtotal": "total_equity",
        "label": "TOTAL EQUITY",
        "items": OrderedDict([
            ("share_capital", "Share Capital"),
            ("reserves", "Reserves"),
            ("capital_reserves", "Capital Reserves"),
            ("revenue_reserves", "Revenue Reserves"),
            ("revaluation_surplus", "Revaluation Surplus"),
            ("retained_earnings", "Retained Earnings"),
            ("share_premium", "Share Premium"),
            ("nci", "Non-Controlling Interest"),
        ])
    }),
])

BS_FOOTER = OrderedDict([
    ("total_assets", "TOTAL ASSETS"),
    ("total_liabilities", "TOTAL LIABILITIES"),
    ("total_equity", "TOTAL EQUITY"),
    ("total_equity_and_liabilities", "Total Equity & Liabilities"),
])


# ============================================================
# COMPANY TYPE DETECTION
# ============================================================

def detect_company_type(ticker: str) -> str:
    """Detect company type based on P&L canonicals."""
    pl_path = JSON_PL_DIR / f"{ticker}.json"
    if not pl_path.exists():
        return "CORPORATE"

    with open(pl_path) as f:
        data = json.load(f)

    for p in data.get("periods", [])[:5]:
        values = p.get("values", {})
        if "net_interest_income" in values or "bank_interest_income" in values:
            return "BANK"
        if any(k in values for k in ["net_premium", "gross_premium", "claims_expense"]):
            return "INSURANCE"

    return "CORPORATE"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def format_val(val) -> str:
    """Format value for CSV."""
    if val is None:
        return ""
    if val == 0:
        return "0"
    if isinstance(val, float) and abs(val) < 100:
        return f"{val:.2f}"
    return f"{val:,.0f}"


def quarter_label_with_source(period: dict) -> str:
    """Generate column header: Q1 2024 (1Q2024)."""
    end = period.get("period_end", "")
    year = end[:4]
    month = end[5:7]
    q_map = {"03": ("Q1", "1Q"), "06": ("Q2", "2Q"), "09": ("Q3", "3Q"), "12": ("Q4", "AR")}
    q_label, filing_prefix = q_map.get(month, (month, month))
    source = f"AR{year}" if filing_prefix == "AR" else f"{filing_prefix}{year}"
    return f"{q_label} {year} ({source})"


def get_periods(data: dict, key: str = "periods", duration_filter: str = None,
                consolidation: str = "consolidated", start_year: str = "2021") -> list:
    """Get filtered periods from data, deduplicating by period_end."""
    seen = {}
    items = data.get(key, [])
    for p in items:
        end = p.get("period_end", "")
        dur = p.get("duration", "")
        if end >= start_year and p.get("consolidation") == consolidation:
            if duration_filter is None or dur == duration_filter:
                source = p.get("source_file", "")
                is_quarterly = "quarterly" in source
                if end not in seen:
                    seen[end] = p
                elif is_quarterly and "quarterly" not in seen[end].get("source_file", ""):
                    seen[end] = p
    return sorted(seen.values(), key=lambda x: x["period_end"])


def get_relevant_section(company_type: str, statement: str) -> list:
    """Get relevant sections based on company type."""
    if statement == "PL":
        if company_type == "BANK":
            return ["NET_INTEREST", "PBT_BANK"]
        elif company_type == "INSURANCE":
            return ["UNDERWRITING", "PBT_INSURANCE"]
        else:
            return ["GROSS_PROFIT", "PBT_CORPORATE"]
    elif statement == "CF":
        return ["CFO", "CFI", "CFF"]
    else:  # BS
        return ["CURRENT_ASSETS", "NON_CURRENT_ASSETS", "CURRENT_LIABILITIES", "NON_CURRENT_LIABILITIES", "EQUITY"]


def build_dynamic_statement(periods: list, sections_def: OrderedDict, footer_def: OrderedDict,
                           relevant_sections: list, company_type: str) -> list:
    """Build statement rows dynamically - show all non-null items."""
    rows = []

    for section_key in relevant_sections:
        if section_key not in sections_def:
            continue

        section = sections_def[section_key]
        items = section["items"]
        subtotal_key = section["subtotal"]
        subtotal_label = section["label"]

        # Track which items have any non-null values across all periods
        items_with_data = []
        for canonical, label in items.items():
            has_data = any(p.get("values", {}).get(canonical) is not None for p in periods)
            if has_data:
                items_with_data.append((canonical, label))

        # Add rows for items with data
        for canonical, label in items_with_data:
            row = {
                "label": label,
                "values": [p.get("values", {}).get(canonical) for p in periods]
            }
            rows.append(row)

        # Calculate "Other" row
        other_values = []
        for p in periods:
            v = p.get("values", {})
            subtotal = v.get(subtotal_key)
            if subtotal is not None:
                component_sum = sum(v.get(c, 0) or 0 for c, _ in items_with_data)
                other_values.append(subtotal - component_sum)
            else:
                other_values.append(None)

        # Only add "Other" if it has meaningful values
        if any(o is not None and abs(o) > 0.01 for o in other_values):
            rows.append({"label": f"Other {subtotal_label.split()[-1]}", "values": other_values})

        # Subtotal row
        rows.append({
            "label": subtotal_label,
            "values": [p.get("values", {}).get(subtotal_key) for p in periods]
        })

        # Spacer
        rows.append({"label": "", "values": [None] * len(periods)})

    # Footer items (show if non-null)
    for canonical, label in footer_def.items():
        has_data = any(p.get("values", {}).get(canonical) is not None for p in periods)
        if has_data:
            rows.append({
                "label": label,
                "values": [p.get("values", {}).get(canonical) for p in periods]
            })

    return rows


def write_csv(filepath: Path, periods: list, rows: list) -> int:
    """Write statement to CSV file."""
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        header = ["Line Item"] + [quarter_label_with_source(p) for p in periods]
        writer.writerow(header)

        for row in rows:
            csv_row = [row["label"]] + [format_val(v) for v in row["values"]]
            writer.writerow(csv_row)

    return len(periods)


def generate_views(ticker: str, consolidation: str = "consolidated") -> dict:
    """Generate all statement views for a ticker."""
    results = {}
    consol_suffix = "cons" if consolidation == "consolidated" else "uncons"
    company_type = detect_company_type(ticker)

    # P&L
    pl_path = JSON_PL_DIR / f"{ticker}.json"
    if pl_path.exists():
        with open(pl_path) as f:
            pl_data = json.load(f)
        periods = get_periods(pl_data, duration_filter="3M", consolidation=consolidation)
        if periods:
            relevant = get_relevant_section(company_type, "PL")
            rows = build_dynamic_statement(periods, PL_SECTIONS, PL_FOOTER, relevant, company_type)
            output = OUTPUT_DIR / f"{ticker}_pl_view_{consol_suffix}.csv"
            n = write_csv(output, periods, rows)
            results["PL"] = {"quarters": n, "file": str(output), "type": company_type}

    # CF
    cf_path = JSON_CF_DIR / f"{ticker}.json"
    if cf_path.exists():
        with open(cf_path) as f:
            cf_data = json.load(f)
        periods = get_periods(cf_data, key="quarters", consolidation=consolidation)
        if periods:
            relevant = get_relevant_section(company_type, "CF")
            rows = build_dynamic_statement(periods, CF_SECTIONS, CF_FOOTER, relevant, company_type)
            output = OUTPUT_DIR / f"{ticker}_cf_view_{consol_suffix}.csv"
            n = write_csv(output, periods, rows)
            results["CF"] = {"quarters": n, "file": str(output), "type": company_type}

    # BS
    bs_path = JSON_BS_DIR / f"{ticker}.json"
    if bs_path.exists():
        with open(bs_path) as f:
            bs_data = json.load(f)
        periods = get_periods(bs_data, consolidation=consolidation)
        if periods:
            relevant = get_relevant_section(company_type, "BS")
            rows = build_dynamic_statement(periods, BS_SECTIONS, BS_FOOTER, relevant, company_type)
            output = OUTPUT_DIR / f"{ticker}_bs_view_{consol_suffix}.csv"
            n = write_csv(output, periods, rows)
            results["BS"] = {"quarters": n, "file": str(output), "type": company_type}

    return results


def main():
    parser = argparse.ArgumentParser(description="Generate statement view CSVs (95% coverage)")
    parser.add_argument("ticker", nargs="?", help="Ticker symbol (or --all)")
    parser.add_argument("--all", action="store_true", help="Generate for all tickers")
    parser.add_argument("--type", choices=["BANK", "INSURANCE", "CORPORATE"],
                        help="Filter by company type")
    parser.add_argument("--consolidation", default="consolidated",
                        choices=["consolidated", "unconsolidated"])
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.all or args.type:
        tickers = sorted(set(p.stem for p in JSON_PL_DIR.glob("*.json")))
        if args.type:
            tickers = [t for t in tickers if detect_company_type(t) == args.type]

        print(f"Generating views for {len(tickers)} tickers...")
        by_type = {"BANK": 0, "INSURANCE": 0, "CORPORATE": 0}

        for ticker in tickers:
            results = generate_views(ticker, args.consolidation)
            if results:
                ctype = results.get("PL", results.get("CF", {})).get("type", "CORPORATE")
                by_type[ctype] += 1
                print(f"  {ticker} [{ctype}]: " + ", ".join(f"{k}={v['quarters']}q" for k, v in results.items()))

        print(f"\nSummary: {by_type}")
    else:
        if not args.ticker:
            parser.print_help()
            return
        results = generate_views(args.ticker, args.consolidation)
        if results:
            ctype = results.get("PL", results.get("CF", {})).get("type", "CORPORATE")
            print(f"Generated views for {args.ticker} [{ctype}]:")
            for k, v in results.items():
                print(f"  {k}: {v['quarters']} quarters -> {v['file']}")


if __name__ == "__main__":
    main()
