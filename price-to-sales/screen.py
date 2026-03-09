#!/usr/bin/env python3
"""
Price-to-Sales (P/S) Value - Current Stock Screen

Screens for low P/S stocks using TTM data: P/S < 1.0,
Gross Margin > 20%, Operating Margin > 5%, ROE > 10%,
market cap > local currency threshold.

Usage:
    python3 price-to-sales/screen.py
    python3 price-to-sales/screen.py --preset india
    python3 price-to-sales/screen.py --exchange XETRA
    python3 price-to-sales/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
PS_MAX = 1.0
PS_MIN = 0.0
GROSS_MARGIN_MIN = 0.20
OP_MARGIN_MIN = 0.05
ROE_MIN = 0.10
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        SELECT
            f.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(f.priceToSalesRatioTTM, 3) AS ps_ratio,
            ROUND(f.grossProfitMarginTTM * 100, 1) AS gross_margin_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 1) AS op_margin_pct,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM financial_ratios_ttm f
        JOIN key_metrics_ttm k ON f.symbol = k.symbol
        JOIN profile p ON f.symbol = p.symbol
        WHERE f.priceToSalesRatioTTM > {PS_MIN}
          AND f.priceToSalesRatioTTM < {PS_MAX}
          AND f.grossProfitMarginTTM > {GROSS_MARGIN_MIN}
          AND f.operatingProfitMarginTTM > {OP_MARGIN_MIN}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.symbol ORDER BY f.priceToSalesRatioTTM ASC) = 1
        ORDER BY f.priceToSalesRatioTTM ASC
        LIMIT {MAX_STOCKS}
    """

    results = client.query(sql, verbose=verbose)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="Price-to-Sales value live screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", dest="output_json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch as CR
        cr = CR(api_key=args.api_key, base_url=args.base_url)
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = cr.execute_code(
            f"python3 price-to-sales/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Price-to-Sales Screen | Universe: {universe_name}")
    print(f"Filters: P/S {PS_MIN}-{PS_MAX}, GrossMargin > {GROSS_MARGIN_MIN*100:.0f}%, "
          f"OpMargin > {OP_MARGIN_MIN*100:.0f}%, ROE > {ROE_MIN*100:.0f}%, "
          f"MCap > {mktcap_min/1e9:.1f}B local")
    print("=" * 90)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<28} {'P/S':>6} {'GrMgn':>7} "
          f"{'OpMgn':>7} {'ROE%':>6} {'MCap$B':>8}")
    print("-" * 90)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:26]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<28} "
              f"{r.get('ps_ratio', 'N/A'):>6} "
              f"{r.get('gross_margin_pct', 'N/A'):>6}% "
              f"{r.get('op_margin_pct', 'N/A'):>6}% "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('mktcap_b', 'N/A'):>8}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), TTM metrics.")


if __name__ == "__main__":
    main()
