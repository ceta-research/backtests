#!/usr/bin/env python3
"""
Income Quality Current Stock Screen

Finds stocks currently qualifying as high income quality
(Operating Cash Flow / Net Income > 1.2 with positive net income).

Usage:
    python3 income-quality/screen.py
    python3 income-quality/screen.py --preset india
    python3 income-quality/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import (add_common_args, resolve_exchanges,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

IQ_HIGH_THRESHOLD = 1.2


def build_screen_sql(exchanges, mktcap_min):
    """Build SQL for current income quality screen."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
    SELECT
        k.symbol,
        p.companyName,
        p.sector,
        p.exchange,
        ROUND(p.marketCap / 1e9, 2) AS marketCap_B,
        ROUND(k.incomeQualityTTM, 2) AS income_quality,
        ROUND(f.netProfitMarginTTM * 100, 1) AS net_margin_pct,
        ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE k.incomeQualityTTM > {IQ_HIGH_THRESHOLD}
        AND f.netProfitMarginTTM > 0.05
        AND k.returnOnEquityTTM > 0.08
        AND p.marketCap > {mktcap_min}
        AND p.isActivelyTrading = true
        {exchange_clause}
    ORDER BY k.incomeQualityTTM DESC
    LIMIT 100
    """


def main():
    parser = argparse.ArgumentParser(
        description="Income Quality current stock screen"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(
        args,
        default_exchanges=["NYSE", "NASDAQ", "AMEX"],
        default_name="US_MAJOR"
    )
    mktcap_threshold = get_mktcap_threshold(exchanges)

    print(f"Income Quality Screen: {universe_name}")
    print(f"Signal: OCF/NI > {IQ_HIGH_THRESHOLD} + NI margin > 5% + ROE > 8%")
    print(f"Market cap min: {mktcap_threshold:,.0f}")
    print()

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if args.cloud:
        sql = build_screen_sql(exchanges, mktcap_threshold)
        result = cr.execute_code(f"""
import duckdb
con = duckdb.connect()
result = con.execute('''{sql}''').fetchdf()
print(result.to_string(index=False))
print(f"\\nTotal: {{len(result)}} stocks")
""")
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
    else:
        sql = build_screen_sql(exchanges, mktcap_threshold)
        results = cr.query(sql, verbose=args.verbose, timeout=120)

        if not results:
            print("No qualifying stocks found.")
            return

        print(f"{'Symbol':<10} {'Company':<30} {'Sector':<22} "
              f"{'MCap($B)':>8} {'IQ':>6} {'NIM%':>6} {'ROE%':>6}")
        print("-" * 95)

        for r in results:
            print(f"{r['symbol']:<10} "
                  f"{r['companyName'][:28]:<30} "
                  f"{r.get('sector', 'N/A')[:20]:<22} "
                  f"{r['marketCap_B']:>8.1f} "
                  f"{r['income_quality']:>6.2f} "
                  f"{r['net_margin_pct']:>5.1f}% "
                  f"{r['roe_pct']:>5.1f}%")

        print(f"\nTotal: {len(results)} stocks with high income quality")


if __name__ == "__main__":
    main()
