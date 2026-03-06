#!/usr/bin/env python3
"""
Asset Growth Anomaly - Current Stock Screen

Screens for stocks with low total asset growth, high quality metrics.
Uses TTM (trailing twelve months) data for current screening.

Usage:
    python3 asset-growth/screen.py
    python3 asset-growth/screen.py --preset india
    python3 asset-growth/screen.py --exchange XETRA
    python3 asset-growth/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
ASSET_GROWTH_MAX = 0.10
ASSET_GROWTH_MIN = -0.20
ROE_MIN = 0.08
ROA_MIN = 0.05
OPM_MIN = 0.10
MKTCAP_MIN = 500_000_000
MAX_STOCKS = 30


def run_screen(client, exchanges, verbose=False):
    """Run live screen using TTM data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH bs_current AS (
            SELECT symbol, totalAssets, dateEpoch as filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM balance_sheet
            WHERE period = 'FY' AND totalAssets IS NOT NULL AND totalAssets > 0
        ),
        bs_prior AS (
            SELECT symbol, totalAssets AS totalAssets_prior, dateEpoch as filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM balance_sheet
            WHERE period = 'FY' AND totalAssets IS NOT NULL AND totalAssets > 0
        ),
        growth AS (
            SELECT bc.symbol,
                bc.totalAssets AS current_assets,
                bp.totalAssets_prior AS prior_assets,
                (bc.totalAssets - bp.totalAssets_prior) / bp.totalAssets_prior AS asset_growth
            FROM bs_current bc
            JOIN bs_prior bp ON bc.symbol = bp.symbol AND bp.rn = 2
            WHERE bc.rn = 1
              AND bp.totalAssets_prior > 0
        )
        SELECT g.symbol, p.companyName, p.exchange, p.sector,
            ROUND(g.asset_growth * 100, 2) AS asset_growth_pct,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(k.returnOnAssetsTTM * 100, 2) AS roa_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b,
            ROUND(g.current_assets / 1e9, 2) AS total_assets_b
        FROM growth g
        JOIN profile p ON g.symbol = p.symbol
        JOIN key_metrics_ttm k ON g.symbol = k.symbol
        JOIN financial_ratios_ttm f ON g.symbol = f.symbol
        WHERE g.asset_growth < {ASSET_GROWTH_MAX}
          AND g.asset_growth > {ASSET_GROWTH_MIN}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND k.returnOnAssetsTTM > {ROA_MIN}
          AND f.operatingProfitMarginTTM > {OPM_MIN}
          AND k.marketCap > {MKTCAP_MIN}
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Asset Management%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Shell Companies%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Closed-End Fund%')
          {exchange_filter}
        ORDER BY g.asset_growth ASC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Asset Growth Anomaly - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("asset-growth", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Asset Growth Anomaly Screen - {universe_name}")
    print(f"Signal: AG {ASSET_GROWTH_MIN*100:.0f}% to {ASSET_GROWTH_MAX*100:.0f}%, "
          f"ROE > {ROE_MIN*100:.0f}%, ROA > {ROA_MIN*100:.0f}%, "
          f"OPM > {OPM_MIN*100:.0f}%, MCap > ${MKTCAP_MIN/1e6:.0f}M")
    print("-" * 90)

    results = run_screen(cr, exchanges, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<30} {'AG%':>6} {'ROE%':>6} "
          f"{'ROA%':>6} {'OPM%':>6} {'MCap$B':>8}")
    print("-" * 90)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:28]:<30} "
              f"{r.get('asset_growth_pct', ''):>6} {r.get('roe_pct', ''):>6} "
              f"{r.get('roa_pct', ''):>6} {r.get('opm_pct', ''):>6} "
              f"{r.get('mktcap_b', ''):>8}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
