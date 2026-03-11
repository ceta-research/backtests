#!/usr/bin/env python3
"""
Free Cash Flow Growth - Current Stock Screen

Screens for stocks with high YoY FCF growth backed by growing operating cash flow.
Uses TTM (trailing twelve months) data for current screening.

Usage:
    python3 fcf-growth/screen.py
    python3 fcf-growth/screen.py --preset india
    python3 fcf-growth/screen.py --exchange XETRA
    python3 fcf-growth/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
FCF_GROWTH_MIN = 0.15
OCF_GROWTH_MIN = 0.0
ROE_MIN = 0.10
DE_MAX = 1.5
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using FY data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    # Use FY data for consistency with backtest (TTM FCF data may be sparse)
    sql = f"""
        WITH cf_current AS (
            SELECT symbol, freeCashFlow, operatingCashFlow,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM cash_flow_statement
            WHERE period = 'FY'
              AND freeCashFlow IS NOT NULL
        ),
        cf_prior AS (
            SELECT symbol,
                freeCashFlow AS fcf_prior,
                operatingCashFlow AS ocf_prior,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM cash_flow_statement
            WHERE period = 'FY'
              AND freeCashFlow IS NOT NULL
        ),
        growth AS (
            SELECT cc.symbol,
                cc.freeCashFlow AS fcf_current,
                cp.fcf_prior,
                (cc.freeCashFlow - cp.fcf_prior) / NULLIF(ABS(cp.fcf_prior), 0) AS fcf_growth,
                (cc.operatingCashFlow - cp.ocf_prior) / NULLIF(ABS(cp.ocf_prior), 0) AS ocf_growth
            FROM cf_current cc
            JOIN cf_prior cp ON cc.symbol = cp.symbol AND cp.rn = 2
            WHERE cc.rn = 1
        )
        SELECT g.symbol, p.companyName, p.exchange, p.sector,
            ROUND(g.fcf_growth * 100, 2) AS fcf_growth_pct,
            ROUND(g.ocf_growth * 100, 2) AS ocf_growth_pct,
            ROUND(g.fcf_current / 1e9, 2) AS fcf_bn,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(f.debtToEquityRatioTTM, 2) AS de_ratio,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM growth g
        JOIN profile p ON g.symbol = p.symbol
        JOIN key_metrics_ttm k ON g.symbol = k.symbol
        JOIN financial_ratios_ttm f ON g.symbol = f.symbol
        WHERE g.fcf_current > 0
          AND g.fcf_growth > {FCF_GROWTH_MIN}
          AND g.ocf_growth > {OCF_GROWTH_MIN}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND f.debtToEquityRatioTTM < {DE_MAX}
          AND k.marketCap > {mktcap_min}
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Asset Management%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Shell Companies%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Closed-End Fund%')
          {exchange_filter}
        ORDER BY g.fcf_growth DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="FCF Growth - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("fcf-growth", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"FCF Growth Screen - {universe_name}")
    print(f"Signal: FCF growth > {FCF_GROWTH_MIN*100:.0f}%, OCF growth > {OCF_GROWTH_MIN*100:.0f}%, "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<30} {'FCFGrow%':>9} {'OCFGrow%':>9} "
          f"{'ROE%':>6} {'D/E':>5} {'MCap$B':>8} {'Sector':<20}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:28]:<30} "
              f"{r.get('fcf_growth_pct', ''):>9} {r.get('ocf_growth_pct', ''):>9} "
              f"{r.get('roe_pct', ''):>6} {r.get('de_ratio', ''):>5} "
              f"{r.get('mktcap_b', ''):>8} {r.get('sector', '')[:18]:<20}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
