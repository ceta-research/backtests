#!/usr/bin/env python3
"""
R&D Efficiency - Current Stock Screen

Screens for stocks with high R&D spending efficiency: companies that invest
heavily in R&D and generate strong gross profit per dollar spent.

Uses TTM data for current screening.

Usage:
    python3 rd-efficiency/screen.py
    python3 rd-efficiency/screen.py --preset germany
    python3 rd-efficiency/screen.py --exchange JPX
    python3 rd-efficiency/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
RD_RATIO_MIN = 0.02
RD_RATIO_MAX = 0.30
GROSS_MARGIN_MIN = 0.40
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
        WITH inc AS (
            SELECT symbol, revenue, grossProfit, researchAndDevelopmentExpenses,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY'
              AND revenue > 0
              AND grossProfit > 0
              AND researchAndDevelopmentExpenses > 0
        )
        SELECT
            inc.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(inc.researchAndDevelopmentExpenses / inc.revenue * 100, 1) AS rd_ratio_pct,
            ROUND(inc.grossProfit / inc.revenue * 100, 1) AS gross_margin_pct,
            ROUND(inc.grossProfit / inc.researchAndDevelopmentExpenses, 2) AS rd_efficiency,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM inc
        JOIN profile p ON inc.symbol = p.symbol
        JOIN key_metrics_ttm k ON inc.symbol = k.symbol
        WHERE inc.rn = 1
          AND inc.researchAndDevelopmentExpenses / inc.revenue > {RD_RATIO_MIN}
          AND inc.researchAndDevelopmentExpenses / inc.revenue < {RD_RATIO_MAX}
          AND inc.grossProfit / inc.revenue > {GROSS_MARGIN_MIN}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND k.marketCap > {mktcap_min}
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Asset Management%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Shell Companies%')
          {exchange_filter}
        ORDER BY rd_efficiency DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="R&D Efficiency - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("rd-efficiency", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"R&D Efficiency Screen - {universe_name}")
    print(f"Signal: R&D/Rev {RD_RATIO_MIN*100:.0f}%-{RD_RATIO_MAX*100:.0f}%, "
          f"GrossMargin > {GROSS_MARGIN_MIN*100:.0f}%, "
          f"ROE > {ROE_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print(f"Ranked by: GrossProfit / R&D Expenses (highest first)")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'Sector':<22} "
          f"{'R&D%':>6} {'GM%':>6} {'Effic':>8} {'ROE%':>6} {'MCap$B':>8}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:26]:<28} "
              f"{r.get('sector', '')[:20]:<22} "
              f"{r.get('rd_ratio_pct', ''):>6} {r.get('gross_margin_pct', ''):>6} "
              f"{r.get('rd_efficiency', ''):>8} {r.get('roe_pct', ''):>6} "
              f"{r.get('mktcap_b', ''):>8}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
