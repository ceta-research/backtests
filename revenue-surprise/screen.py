#!/usr/bin/env python3
"""
Revenue Surprise Momentum - Current Stock Screen

Screens for stocks with the highest recent FY revenue surprise vs analyst estimates.
Uses TTM / most recent FY data for current screening.

Usage:
    python3 revenue-surprise/screen.py
    python3 revenue-surprise/screen.py --preset india
    python3 revenue-surprise/screen.py --exchange XETRA
    python3 revenue-surprise/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
REV_SURPRISE_MIN = 0.00
REV_SURPRISE_MAX = 0.50
ROE_MIN = 0.08
DE_MAX = 2.5
MAX_STOCKS = 30
DATE_MATCH_WINDOW = 7776000  # 90 days in seconds


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using most recent FY data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH rev_surprise AS (
            SELECT i.symbol,
                   i.revenue AS actual_revenue,
                   a.revenueAvg AS est_revenue,
                   (i.revenue - a.revenueAvg) / ABS(a.revenueAvg) AS rev_surprise_pct,
                   CAST(i.date AS DATE) AS filing_date,
                   ROW_NUMBER() OVER (PARTITION BY i.symbol ORDER BY i.dateEpoch DESC) AS rn
            FROM income_statement i
            JOIN analyst_estimates a ON i.symbol = a.symbol
                AND a.period = 'annual'
                AND ABS(CAST(i.dateEpoch AS BIGINT) - CAST(a.dateEpoch AS BIGINT)) <= {DATE_MATCH_WINDOW}
            WHERE i.period = 'FY'
              AND i.revenue IS NOT NULL AND i.revenue > 0
              AND a.revenueAvg IS NOT NULL AND a.revenueAvg > 0
        )
        SELECT rs.symbol, p.companyName, p.exchange, p.sector,
            ROUND(rs.rev_surprise_pct * 100, 2) AS rev_surprise_pct,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(f.debtToEquityRatioTTM, 2) AS de_ratio,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b,
            ROUND(rs.actual_revenue / 1e9, 2) AS actual_rev_b,
            ROUND(rs.est_revenue / 1e9, 2) AS est_rev_b,
            rs.filing_date
        FROM rev_surprise rs
        JOIN profile p ON rs.symbol = p.symbol
        JOIN key_metrics_ttm k ON rs.symbol = k.symbol
        JOIN financial_ratios_ttm f ON rs.symbol = f.symbol
        WHERE rs.rn = 1
          AND rs.rev_surprise_pct > {REV_SURPRISE_MIN}
          AND rs.rev_surprise_pct < {REV_SURPRISE_MAX}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND (f.debtToEquityRatioTTM IS NULL OR f.debtToEquityRatioTTM < {DE_MAX})
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        ORDER BY rs.rev_surprise_pct DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Revenue Surprise Momentum - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("revenue-surprise", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Revenue Surprise Momentum Screen - {universe_name}")
    print(f"Signal: Rev surprise {REV_SURPRISE_MIN*100:.0f}%-{REV_SURPRISE_MAX*100:.0f}%, "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_label} local")
    print("-" * 105)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'Surp%':>7} {'ROE%':>6} "
          f"{'D/E':>5} {'MCap$B':>8} {'ActRev$B':>10} {'EstRev$B':>10} {'Filed':>12}")
    print("-" * 105)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:26]:<28} "
              f"{r.get('rev_surprise_pct', ''):>7} {r.get('roe_pct', ''):>6} "
              f"{r.get('de_ratio', ''):>5} {r.get('mktcap_b', ''):>8} "
              f"{r.get('actual_rev_b', ''):>10} {r.get('est_rev_b', ''):>10} "
              f"{str(r.get('filing_date', '')):>12}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
