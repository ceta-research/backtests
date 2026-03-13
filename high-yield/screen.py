#!/usr/bin/env python3
"""
High Dividend Yield Quality Screen - Current qualifying stocks.

Screens the current universe for high-yield stocks passing quality filters.

Usage:
    python3 high-yield/screen.py
    python3 high-yield/screen.py --preset india
    python3 high-yield/screen.py --exchange BSE,NSE
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

import argparse


def main():
    parser = argparse.ArgumentParser(description="High Dividend Yield Quality screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    sql = f"""
        SELECT r.symbol, p.companyName, p.exchange, p.sector,
            ROUND(r.dividendYield * 100, 2) AS yield_pct,
            ROUND(r.dividendPayoutRatio * 100, 1) AS payout_pct,
            ROUND(k.returnOnEquity * 100, 1) AS roe_pct,
            ROUND(r.debtToEquityRatio, 2) AS debt_equity,
            ROUND(c.freeCashFlow / 1e6, 1) AS fcf_mm,
            ROUND(k.marketCap / 1e9, 1) AS mktcap_bn
        FROM financial_ratios r
        JOIN key_metrics k ON r.symbol = k.symbol AND r.date = k.date AND r.period = k.period
        JOIN cash_flow_statement c ON r.symbol = c.symbol AND r.date = c.date AND r.period = c.period
        JOIN profile p ON r.symbol = p.symbol
        WHERE r.period = 'FY'
          AND r.dividendYield BETWEEN 0.04 AND 0.15
          AND r.dividendPayoutRatio BETWEEN 0 AND 0.80
          AND c.freeCashFlow > 0
          AND k.returnOnEquity > 0.08
          AND (r.debtToEquityRatio < 2.0 OR r.debtToEquityRatio IS NULL)
          AND k.marketCap > {mktcap_threshold}
          {exchange_where}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY r.symbol ORDER BY r.date DESC) = 1
        ORDER BY r.dividendYield DESC
        LIMIT 50
    """

    print(f"High Dividend Yield Quality Screen - {universe_name}")
    print(f"Signal: Yield 4-15%, Payout < 80%, FCF > 0, ROE > 8%, D/E < 2.0")
    print(f"Market cap > {mktcap_threshold/1e9:.0f}B local currency")
    print("=" * 120)

    results = cr.query(sql, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    print(f"\n{'Symbol':<12} {'Company':<30} {'Exchange':<8} {'Yield%':>7} {'Payout%':>8} "
          f"{'ROE%':>6} {'D/E':>6} {'FCF($M)':>8} {'MCap($B)':>9}")
    print("-" * 120)
    for r in results:
        print(f"{r['symbol']:<12} {str(r.get('companyName', ''))[:29]:<30} "
              f"{r.get('exchange', ''):<8} {r['yield_pct']:>7} {r['payout_pct']:>8} "
              f"{r['roe_pct']:>6} {r.get('debt_equity', 'N/A'):>6} "
              f"{r['fcf_mm']:>8} {r['mktcap_bn']:>9}")

    print(f"\nTotal: {len(results)} qualifying stocks")


if __name__ == "__main__":
    main()
