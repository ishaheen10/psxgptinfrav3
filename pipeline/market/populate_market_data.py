#!/usr/bin/env python3
"""
Populate market_data table - simplified:
- Shares: PSX current
- Prices: PSX historical (split-adjusted)  
- P/E: Market Cap / LTM Net Income
- Dividend Yield: LTM Dividends / Market Cap
"""

import json
import argparse
import requests
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import time
import re

BASE_DIR = Path(__file__).parent.parent.parent
PSX_CURRENT_FILE = BASE_DIR / "psx_current_shares.json"
STATEMENTS_FILE = BASE_DIR / "financial_statements_cleaned.jsonl"
TICKERS_FILE = BASE_DIR / "market" / "tickers100_full.json"
OUTPUT_FILE = BASE_DIR / "market_data_inserts.sql"

PSX_API_URL = "https://dps.psx.com.pk/timeseries/eod/{symbol}"


def load_psx_shares() -> dict:
    with open(PSX_CURRENT_FILE) as f:
        return json.load(f)


def load_tickers() -> list[str]:
    with open(TICKERS_FILE) as f:
        return [t["Symbol"] for t in json.load(f)]


def parse_period_to_date(period_end: str) -> datetime | None:
    if not period_end:
        return None
    if re.match(r"^\d{4}$", period_end):
        return datetime(int(period_end), 12, 31)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", period_end):
        return datetime.strptime(period_end, "%Y-%m-%d")
    match = re.match(r"^(\d{4})-(\w{3})$", period_end)
    if match:
        month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                     "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        year, mon = int(match.group(1)), month_map.get(match.group(2), 12)
        return datetime(year, mon, 28)  # Approximate end of month
    return None


def load_ltm_data() -> tuple[dict, dict]:
    """Load LTM (12-month) net income and dividends using key_metric field."""
    net_income = defaultdict(list)
    dividends = defaultdict(list)

    with open(STATEMENTS_FILE) as f:
        for line in f:
            d = json.loads(line)

            # Only use LTM or annual records (12 months)
            period_type = d.get("period_type", "")
            if period_type not in ["ltm", "annual"]:
                continue

            ticker = d["ticker"]
            section = d.get("section", "")
            period_end = d.get("period_end", "")
            key_metric = d.get("key_metric", "")
            value = d.get("value")
            unit_type = d.get("unit_type", "thousands")

            if value is None or section != "consolidated" or not key_metric:
                continue

            period_date = parse_period_to_date(period_end)
            if not period_date:
                continue

            # Convert to Rs (unit_type can be "thousands", "000s", or "rupees")
            value_rs = value * 1000 if unit_type in ["thousands", "000s"] else value

            # Net income - use key_metric for reliable matching
            if key_metric == "net_profit":
                net_income[ticker].append({"date": period_date, "value": value_rs})

            # Dividends - use key_metric (excludes NCI dividends)
            if key_metric == "dividend_paid":
                dividends[ticker].append({"date": period_date, "value": abs(value_rs)})

    # Sort by date desc, dedupe
    for ticker in net_income:
        net_income[ticker].sort(key=lambda x: x["date"], reverse=True)
        seen = set()
        net_income[ticker] = [r for r in net_income[ticker] if not (r["date"] in seen or seen.add(r["date"]))]

    for ticker in dividends:
        dividends[ticker].sort(key=lambda x: x["date"], reverse=True)
        seen = set()
        dividends[ticker] = [r for r in dividends[ticker] if not (r["date"] in seen or seen.add(r["date"]))]

    return dict(net_income), dict(dividends)


def get_value_as_of(ticker: str, as_of_date: datetime, data: dict) -> float | None:
    """Get most recent LTM value as of date."""
    if ticker not in data:
        return None
    for r in data[ticker]:
        if r["date"] <= as_of_date:
            return r["value"]
    return None


def fetch_psx_prices(symbol: str) -> list[dict]:
    url = PSX_API_URL.format(symbol=symbol)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != 1:
            return []
        return [{"date": datetime.fromtimestamp(item[0]).strftime("%Y-%m-%d"), 
                 "close": item[3]} for item in data.get("data", []) if len(item) >= 4]
    except Exception as e:
        print(f"  Error: {e}")
        return []


def generate_sql(ticker: str, shares: int, prices: list, net_income: dict, dividends: dict) -> list[str]:
    stmts = []
    for p in prices:
        close = p["close"]
        if not close or close <= 0:
            continue
        
        as_of = datetime.strptime(p["date"], "%Y-%m-%d")
        mcap = close * shares
        
        ni = get_value_as_of(ticker, as_of, net_income)
        pe = mcap / ni if ni and ni > 0 else None
        
        div = get_value_as_of(ticker, as_of, dividends)
        div_yield = (div / mcap * 100) if div and mcap > 0 else None
        
        pe_sql = f"{pe:.4f}" if pe else "NULL"
        yield_sql = f"{div_yield:.4f}" if div_yield else "NULL"
        
        stmts.append(f"""INSERT INTO market_data (symbol, date, closing_price, total_shares, market_cap, pe_ratio, dividend_yield, source)
VALUES ('{ticker}', '{p["date"]}', {close:.2f}, {shares}, {mcap:.2f}, {pe_sql}, {yield_sql}, 'psx')
ON CONFLICT(symbol, date) DO UPDATE SET closing_price=excluded.closing_price, total_shares=excluded.total_shares, market_cap=excluded.market_cap, pe_ratio=excluded.pe_ratio, dividend_yield=excluded.dividend_yield;""")
    return stmts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    print("Loading data...")
    psx_shares = load_psx_shares()
    net_income, dividends = load_ltm_data()
    print(f"  Shares: {len(psx_shares)}, Net Income: {len(net_income)}, Dividends: {len(dividends)}")

    tickers = [args.ticker.upper()] if args.ticker else load_tickers()
    if args.limit:
        tickers = tickers[:args.limit]

    print(f"Processing {len(tickers)} tickers...")
    all_stmts = []
    
    for i, ticker in enumerate(tickers):
        print(f"  [{i+1}/{len(tickers)}] {ticker}...", end=" ", flush=True)
        shares = psx_shares.get(ticker)
        if not shares:
            print("no shares")
            continue
        prices = fetch_psx_prices(ticker)
        if not prices:
            print("no prices")
            continue
        stmts = generate_sql(ticker, shares, prices, net_income, dividends)
        all_stmts.extend(stmts)
        print(f"{len(prices)} prices")
        time.sleep(0.1)

    with open(OUTPUT_FILE, "w") as f:
        f.write(f"-- Market data: P/E = MCap/NI, DivYield = Div/MCap\n")
        f.write(f"-- Generated: {datetime.now().isoformat()}\n")
        f.write(f"-- Records: {len(all_stmts)}\n\n")
        f.write("\n".join(all_stmts))

    print(f"\nDone! {len(all_stmts)} records -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
