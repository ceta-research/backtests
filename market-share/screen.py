#!/usr/bin/env python3
"""
Market Share Gain - Current Stock Screen

Screens for stocks currently gaining market share (sector-relative revenue growth).
Uses most recent FY data for revenue growth + TTM quality filters.

Usage:
    python3 market-share/screen.py
    python3 market-share/screen.py --preset india
    python3 market-share/screen.py --exchange XETRA
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
EXCESS_GROWTH_MIN = 0.10  # 10pp above sector median
ROE_MIN = 0.08            # Return on equity > 8%
OPM_MIN = 0.05            # Operating profit margin > 5%
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using most recent FY revenue + TTM quality data.

    Computes sector-relative YoY revenue growth dynamically, then applies
    quality filters using TTM ROE and operating margin.
    Returns list of dicts.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_subquery = f"(SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        sym_subquery = "(SELECT DISTINCT symbol FROM profile)"
        exchange_filter = ""

    sql = f"""
        WITH rev_curr AS (
            SELECT symbol, revenue, dateEpoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL AND revenue > 0
              AND symbol IN {sym_subquery}
        ),
        rev_prior AS (
            SELECT symbol, revenue AS prior_revenue, dateEpoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL AND revenue > 0
              AND symbol IN {sym_subquery}
        ),
        growth AS (
            SELECT rc.symbol, p.sector,
                (rc.revenue - rp.prior_revenue) / rp.prior_revenue AS rev_growth
            FROM rev_curr rc
            JOIN rev_prior rp ON rc.symbol = rp.symbol AND rp.rn = 2
            JOIN profile p ON rc.symbol = p.symbol
            WHERE rc.rn = 1
              AND rp.prior_revenue > 0
              AND p.sector IS NOT NULL AND p.sector != ''
        ),
        sector_stats AS (
            SELECT sector,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rev_growth) AS median_growth
            FROM growth
            GROUP BY sector
            HAVING COUNT(*) >= 3
        ),
        ranked AS (
            SELECT g.symbol, g.sector, g.rev_growth,
                (g.rev_growth - ss.median_growth) AS excess_growth
            FROM growth g
            JOIN sector_stats ss ON g.sector = ss.sector
            WHERE (g.rev_growth - ss.median_growth) >= {EXCESS_GROWTH_MIN}
        )
        SELECT r.symbol, p.companyName, p.exchange, p.sector,
            ROUND(r.rev_growth * 100, 2) AS rev_growth_pct,
            ROUND(r.excess_growth * 100, 2) AS excess_growth_pct,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM ranked r
        JOIN profile p ON r.symbol = p.symbol
        JOIN key_metrics_ttm k ON r.symbol = k.symbol
        JOIN financial_ratios_ttm f ON r.symbol = f.symbol
        WHERE k.returnOnEquityTTM > {ROE_MIN}
          AND f.operatingProfitMarginTTM > {OPM_MIN}
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        ORDER BY r.excess_growth DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=180)
    return results


def main():
    parser = argparse.ArgumentParser(description="Market Share Gain - current screen")
    add_common_args(parser)
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B"
                    if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M")
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Market Share Gain Screen - {universe_name}")
    print(f"Signal: Excess Rev Growth >= {EXCESS_GROWTH_MIN*100:.0f}pp vs sector median, "
          f"ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'Sector':<22} "
          f"{'RevG%':>6} {'ExcG%':>6} {'ROE%':>6} {'OPM%':>6} {'MCap$B':>8}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {str(r.get('companyName', ''))[:26]:<28} "
              f"{str(r.get('sector', ''))[:20]:<22} "
              f"{r.get('rev_growth_pct', ''):>6} {r.get('excess_growth_pct', ''):>6} "
              f"{r.get('roe_pct', ''):>6} {r.get('opm_pct', ''):>6} "
              f"{r.get('mktcap_b', ''):>8}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
