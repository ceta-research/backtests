#!/usr/bin/env python3
"""
DuPont ROE Decomposition - Current Stock Screen

Screens for stocks with high ROE driven by profitability, not leverage.
Uses TTM (trailing twelve months) data for live analysis.

Simple Screen: ROE > 15%, show DuPont components
Advanced Screen: ROE > 15% + net margin > 8% + equity multiplier < 3.0

Usage:
    python3 roe-dupont/screen.py
    python3 roe-dupont/screen.py --preset india
    python3 roe-dupont/screen.py --exchange BSE,NSE
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS


def run_screen(args, advanced=True):
    exchanges, universe_name = resolve_exchanges(
        args,
        default_exchanges=["NYSE", "NASDAQ", "AMEX"],
        default_name="US_MAJOR"
    )

    mktcap_threshold = get_mktcap_threshold(exchanges)

    # Build exchange filter
    if exchanges:
        ex_filter = " AND p.exchange IN (" + ", ".join(f"'{e}'" for e in exchanges) + ")"
    else:
        ex_filter = ""

    # Build SQL
    where_clauses = [
        "k.returnOnEquityTTM > 0.15",
        f"k.marketCap > {mktcap_threshold}",
        "COALESCE(p.sector, '') NOT IN ('Financial Services', 'Utilities')",
    ]

    if advanced:
        where_clauses.extend([
            "(1 + f.debtToEquityRatioTTM) < 3.0",
            "f.netProfitMarginTTM > 0.08",
            "f.debtToEquityRatioTTM >= 0",
        ])

    where = " AND ".join(where_clauses)

    sql = f"""
    SELECT
        k.symbol,
        p.companyName,
        p.sector,
        p.exchange,
        ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
        ROUND(f.netProfitMarginTTM * 100, 1) AS net_margin_pct,
        ROUND(f.assetTurnoverTTM, 2) AS asset_turnover,
        ROUND(1 + f.debtToEquityRatioTTM, 2) AS equity_multiplier,
        ROUND(k.marketCap / 1e9, 1) AS market_cap_b
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE {where}
      {ex_filter}
    ORDER BY f.netProfitMarginTTM DESC
    LIMIT 50
    """

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    screen_type = "Advanced (Quality ROE)" if advanced else "Simple (All High ROE)"
    print(f"\nDuPont ROE Screen: {screen_type}")
    print(f"Universe: {universe_name}")
    print(f"Filters: ROE > 15%{', margin > 8%, eq_mult < 3.0' if advanced else ''}")
    print("-" * 100)

    results = cr.query(sql, verbose=args.verbose, timeout=120)

    if not results:
        print("No qualifying stocks found.")
        return

    # Print results
    header = (f"{'Symbol':<10} {'Company':<30} {'Sector':<22} "
              f"{'ROE%':>6} {'Margin%':>8} {'AT':>6} {'EqMult':>7} {'MCap$B':>8}")
    print(header)
    print("-" * 100)

    for r in results:
        sym = r.get("symbol", "")[:9]
        name = r.get("companyName", "")[:29]
        sector = r.get("sector", "")[:21]
        roe = r.get("roe_pct", 0)
        margin = r.get("net_margin_pct", 0)
        at = r.get("asset_turnover", 0)
        em = r.get("equity_multiplier", 0)
        mcap = r.get("market_cap_b", 0)
        print(f"{sym:<10} {name:<30} {sector:<22} "
              f"{roe:>6.1f} {margin:>8.1f} {at:>6.2f} {em:>7.2f} {mcap:>8.1f}")

    print(f"\n{len(results)} stocks found.")


def main():
    parser = argparse.ArgumentParser(
        description="DuPont ROE Decomposition stock screen"
    )
    add_common_args(parser)
    parser.add_argument("--simple", action="store_true",
                        help="Run simple screen (ROE > 15% only, no quality filter)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "roe-dupont", script="screen.py", args_str=" ".join(cloud_args),
            api_key=args.api_key, base_url=args.base_url, verbose=True
        )
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    run_screen(args, advanced=not args.simple)


if __name__ == "__main__":
    main()
