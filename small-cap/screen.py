#!/usr/bin/env python3
"""
Small-Cap Growth - Current Stock Screen

Screens for small-cap stocks with strong revenue growth and profitability.
Uses FY (annual) data since the strategy rebalances on annual filings.

Signal: Revenue growth > 15% YoY (FY), net income > 0, D/E < 2.0,
        market cap within small-cap range (5%–200% of exchange standard threshold)
Selection: Top 30 by revenue growth, equal weight.

Usage:
    python3 small-cap/screen.py
    python3 small-cap/screen.py --preset india
    python3 small-cap/screen.py --exchange XETRA
    python3 small-cap/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, MKTCAP_THRESHOLD_MAP

# Signal parameters (match backtest.py)
REV_GROWTH_MIN = 0.15
REV_GROWTH_MAX = 5.0
DE_MAX = 2.0
MAX_STOCKS = 30

SMALL_CAP_MIN_FACTOR = 0.05
SMALL_CAP_MAX_FACTOR = 2.0


def get_small_cap_bounds(exchanges):
    """Return (min_cap, max_cap) in local currency for small-cap filtering."""
    default = 1_000_000_000
    if not exchanges:
        standard = default
    else:
        thresholds = [MKTCAP_THRESHOLD_MAP.get(ex, default) for ex in exchanges]
        standard = min(thresholds)
    return int(standard * SMALL_CAP_MIN_FACTOR), int(standard * SMALL_CAP_MAX_FACTOR)


def run_screen(client, exchanges, small_cap_min, small_cap_max, verbose=False):
    """Run live screen using FY data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH inc_curr AS (
            SELECT symbol, revenue, netIncome, dateEpoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue > 0 AND revenue IS NOT NULL
        ),
        inc_prev AS (
            SELECT symbol, revenue, dateEpoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue > 0 AND revenue IS NOT NULL
        ),
        rev_growth AS (
            SELECT c.symbol,
                (c.revenue - p.revenue) / ABS(p.revenue) AS rev_growth,
                c.netIncome,
                c.revenue AS revenue_current,
                p.revenue AS revenue_prior
            FROM inc_curr c
            JOIN inc_prev p ON c.symbol = p.symbol AND c.rn = 1 AND p.rn = 2
            WHERE p.revenue > 0
        ),
        km AS (
            SELECT symbol, marketCap,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM key_metrics
            WHERE period = 'FY' AND marketCap > 0
        ),
        fr AS (
            SELECT symbol, debtToEquityRatio,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM financial_ratios
            WHERE period = 'FY'
        )
        SELECT rg.symbol, p.companyName, p.exchange, p.sector, p.industry,
            ROUND(rg.rev_growth * 100, 1) AS rev_growth_pct,
            ROUND(rg.netIncome / 1e6, 1) AS net_income_m,
            ROUND(km.marketCap / 1e6, 0) AS mktcap_m,
            ROUND(fr.debtToEquityRatio, 2) AS de_ratio
        FROM rev_growth rg
        JOIN km ON rg.symbol = km.symbol AND km.rn = 1
        JOIN fr ON rg.symbol = fr.symbol AND fr.rn = 1
        JOIN profile p ON rg.symbol = p.symbol
        WHERE rg.rev_growth > {REV_GROWTH_MIN}
          AND rg.rev_growth < {REV_GROWTH_MAX}
          AND rg.netIncome > 0
          AND km.marketCap > {small_cap_min}
          AND km.marketCap < {small_cap_max}
          AND fr.debtToEquityRatio >= 0
          AND fr.debtToEquityRatio < {DE_MAX}
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Asset Management%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Shell Companies%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Closed-End Fund%')
          {exchange_filter}
        ORDER BY rg.rev_growth DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Small-Cap Growth - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("small-cap", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    small_cap_min, small_cap_max = get_small_cap_bounds(exchanges)
    min_label = f"{small_cap_min/1e9:.0f}B" if small_cap_min >= 1e9 else f"{small_cap_min/1e6:.0f}M"
    max_label = f"{small_cap_max/1e9:.0f}B" if small_cap_max >= 1e9 else f"{small_cap_max/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Small-Cap Growth Screen - {universe_name}")
    print(f"Signal: MCap {min_label}–{max_label} local, "
          f"rev growth > {REV_GROWTH_MIN*100:.0f}%, netIncome > 0, D/E < {DE_MAX:.0f}")
    print("-" * 95)

    results = run_screen(cr, exchanges, small_cap_min, small_cap_max, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'Exchange':<8} "
          f"{'RevGrowth%':>10} {'NI $M':>8} {'MCap $M':>8} {'D/E':>6}")
    print("-" * 95)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {str(r.get('companyName', ''))[:26]:<28} "
              f"{r.get('exchange', ''):<8} "
              f"{r.get('rev_growth_pct', ''):>10} {r.get('net_income_m', ''):>8} "
              f"{r.get('mktcap_m', ''):>8} {r.get('de_ratio', ''):>6}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
