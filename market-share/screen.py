#!/usr/bin/env python3
"""
Market Share Gain - Current Stock Screen

Screens for stocks gaining market share (sector-relative revenue growth),
using most recent FY income statement data + TTM quality metrics.

Signal: YoY revenue growth exceeds sector median by >= 10 percentage points,
        ROE > 8%, operating profit margin > 5%, MCap > local-currency threshold.

Usage:
    python3 market-share/screen.py
    python3 market-share/screen.py --preset india
    python3 market-share/screen.py --exchange XETRA
    python3 market-share/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
EXCESS_GROWTH_MIN = 0.10   # 10pp above sector median
ROE_MIN = 0.08             # Return on equity > 8%
OPM_MIN = 0.05             # Operating profit margin > 5%
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using most recent FY + TTM data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH profile_data AS (
            SELECT DISTINCT symbol, sector, exchange, companyName
            FROM profile
            WHERE sector IS NOT NULL AND sector != ''
            {exchange_filter}
        ),
        rev_current AS (
            SELECT symbol, revenue,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
        ),
        rev_prior AS (
            SELECT symbol, revenue AS prior_revenue,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
        ),
        growth AS (
            SELECT rc.symbol, pd.sector, pd.companyName, pd.exchange,
                (rc.revenue - rp.prior_revenue) / rp.prior_revenue AS rev_growth,
                k.returnOnEquityTTM, k.marketCap, f.operatingProfitMarginTTM
            FROM rev_current rc
            JOIN rev_prior rp ON rc.symbol = rp.symbol AND rp.rn = 2
            JOIN key_metrics_ttm k ON rc.symbol = k.symbol
            JOIN financial_ratios_ttm f ON rc.symbol = f.symbol
            JOIN profile_data pd ON rc.symbol = pd.symbol
            WHERE rc.rn = 1
              AND rp.prior_revenue > 0
              AND k.returnOnEquityTTM > {ROE_MIN}
              AND f.operatingProfitMarginTTM > {OPM_MIN}
              AND k.marketCap > {mktcap_min}
        ),
        sector_stats AS (
            SELECT sector,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rev_growth) AS median_growth
            FROM growth
            GROUP BY sector
            HAVING COUNT(*) >= 3
        )
        SELECT g.symbol, g.companyName, g.exchange, g.sector,
            ROUND(g.rev_growth * 100, 2) AS rev_growth_pct,
            ROUND((g.rev_growth - ss.median_growth) * 100, 2) AS excess_growth_pct,
            ROUND(g.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(g.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(g.marketCap / 1e9, 2) AS mktcap_b
        FROM growth g
        JOIN sector_stats ss ON g.sector = ss.sector
        WHERE (g.rev_growth - ss.median_growth) >= {EXCESS_GROWTH_MIN}
        ORDER BY excess_growth_pct DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Market Share Gain - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("market-share", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Market Share Gain Screen - {universe_name}")
    print(f"Signal: Excess Rev Growth >= {EXCESS_GROWTH_MIN*100:.0f}pp above sector median, "
          f"ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print("-" * 110)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<30} {'Sector':<22} {'ExGrowth%':>10} "
          f"{'RevGrw%':>8} {'ROE%':>6} {'OPM%':>6} {'MCap$B':>8}")
    print("-" * 110)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:28]:<30} "
              f"{r.get('sector', '')[:20]:<22} {r.get('excess_growth_pct', ''):>10} "
              f"{r.get('rev_growth_pct', ''):>8} {r.get('roe_pct', ''):>6} "
              f"{r.get('opm_pct', ''):>6} {r.get('mktcap_b', ''):>8}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
