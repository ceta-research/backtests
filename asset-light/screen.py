#!/usr/bin/env python3
"""
Asset-Light Business Models - Current Stock Screen

Screens for asset-light companies using TTM metrics.
Composite score: asset turnover + inverse capex intensity + gross margin.

Usage:
    python3 asset-light/screen.py
    python3 asset-light/screen.py --preset india
    python3 asset-light/screen.py --cloud --preset us
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

MAX_RESULTS = 50


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run asset-light screen using TTM data."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH base AS (
            SELECT
                k.symbol,
                p.companyName,
                p.exchange,
                p.sector,
                f.assetTurnoverTTM AS asset_turnover,
                ABS(k.capexToRevenueTTM) AS capex_intensity,
                f.grossProfitMarginTTM AS gross_margin,
                k.returnOnInvestedCapitalTTM AS roic,
                k.marketCap
            FROM key_metrics_ttm k
            JOIN financial_ratios_ttm f ON k.symbol = f.symbol
            JOIN profile p ON k.symbol = p.symbol
            WHERE f.assetTurnoverTTM IS NOT NULL
              AND k.capexToRevenueTTM IS NOT NULL
              AND f.grossProfitMarginTTM IS NOT NULL
              AND k.marketCap > {mktcap_min}
              AND p.sector NOT IN ('Financial Services', 'Utilities')
              {exchange_filter}
        ),
        ranked AS (
            SELECT *,
                PERCENT_RANK() OVER (ORDER BY asset_turnover ASC) AS turnover_rank,
                PERCENT_RANK() OVER (ORDER BY capex_intensity DESC) AS capex_rank,
                PERCENT_RANK() OVER (ORDER BY gross_margin ASC) AS margin_rank
            FROM base
        )
        SELECT
            symbol,
            companyName,
            exchange,
            sector,
            ROUND(asset_turnover, 2) AS asset_turnover,
            ROUND(capex_intensity * 100, 1) AS capex_to_rev_pct,
            ROUND(gross_margin * 100, 1) AS gross_margin_pct,
            ROUND(COALESCE(roic, 0) * 100, 1) AS roic_pct,
            ROUND((turnover_rank + capex_rank + margin_rank) / 3.0, 3) AS composite_score,
            ROUND(marketCap / 1e9, 1) AS mktcap_b
        FROM ranked
        WHERE (turnover_rank + capex_rank + margin_rank) / 3.0 >= 0.80
        ORDER BY composite_score DESC
        LIMIT {MAX_RESULTS}
    """

    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Asset-Light stock screen (TTM)")
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

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        # Read this file and execute on cloud
        with open(__file__) as f:
            code = f.read()
        result = cr.execute_code(code)
        print(result.get("stdout", ""))
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    print(f"Asset-Light Screen: {universe_name}")
    print(f"Market cap threshold: {mktcap_threshold:,.0f}")
    print("-" * 80)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    print(f"\n{'Symbol':<10} {'Company':<30} {'Turnover':>10} {'Capex%':>8} "
          f"{'Margin%':>9} {'ROIC%':>8} {'Score':>8} {'MCap B':>8}")
    print("-" * 95)
    for r in results:
        print(f"{r['symbol']:<10} {r['companyName'][:28]:<30} "
              f"{r['asset_turnover']:>10} {r['capex_to_rev_pct']:>7.1f}% "
              f"{r['gross_margin_pct']:>8.1f}% {r['roic_pct']:>7.1f}% "
              f"{r['composite_score']:>8.3f} {r['mktcap_b']:>7.1f}")

    print(f"\n{len(results)} asset-light stocks found")


if __name__ == "__main__":
    main()
