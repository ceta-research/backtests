#!/usr/bin/env python3
"""
Price-to-Tangible-Book - Current Stock Screen

Screens for stocks with low P/TBV ratio using TTM quality metrics
and the latest FY balance sheet for tangible book computation.

P/TBV = marketCap / (totalStockholdersEquity - goodwill - intangibleAssets)

Usage:
    python3 tangible-book/screen.py
    python3 tangible-book/screen.py --preset india
    python3 tangible-book/screen.py --exchange XETRA
    python3 tangible-book/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
ROE_MIN = 0.08
ROA_MIN = 0.03
OPM_MIN = 0.10
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data + latest FY balance sheet. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH bs AS (
            SELECT symbol,
                   totalStockholdersEquity,
                   COALESCE(goodwill, 0) AS goodwill,
                   COALESCE(intangibleAssets, 0) AS intangibleAssets,
                   (totalStockholdersEquity
                    - COALESCE(goodwill, 0)
                    - COALESCE(intangibleAssets, 0)) AS tangible_equity,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalStockholdersEquity IS NOT NULL
              AND totalStockholdersEquity > 0
        )
        SELECT
            bs.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(k.marketCap / NULLIF(bs.tangible_equity, 0), 2) AS p_tbv,
            ROUND(k.bookValuePerShareTTM, 2) AS bvps,
            ROUND(k.tangibleBookValuePerShareTTM, 2) AS tbvps,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(k.returnOnAssetsTTM * 100, 2) AS roa_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b,
            ROUND(bs.goodwill / 1e9, 2) AS goodwill_b,
            ROUND(bs.tangible_equity / 1e9, 2) AS tangible_equity_b
        FROM bs
        JOIN profile p ON bs.symbol = p.symbol
        JOIN key_metrics_ttm k ON bs.symbol = k.symbol
        JOIN financial_ratios_ttm f ON bs.symbol = f.symbol
        WHERE bs.rn = 1
          AND bs.tangible_equity > 0
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND k.returnOnAssetsTTM > {ROA_MIN}
          AND f.operatingProfitMarginTTM > {OPM_MIN}
          AND k.marketCap > {mktcap_min}
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Asset Management%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Shell Companies%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Closed-End Fund%')
          {exchange_filter}
        ORDER BY p_tbv ASC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Price-to-Tangible-Book - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("tangible-book", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Price-to-Tangible-Book Screen - {universe_name}")
    print(f"Signal: P/TBV ASC, ROE > {ROE_MIN*100:.0f}%, ROA > {ROA_MIN*100:.0f}%, "
          f"OPM > {OPM_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'P/TBV':>6} {'BVPS':>7} "
          f"{'TBVPS':>7} {'ROE%':>6} {'ROA%':>6} {'OPM%':>6} {'MCap$B':>8}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:26]:<28} "
              f"{r.get('p_tbv', ''):>6} {r.get('bvps', ''):>7} "
              f"{r.get('tbvps', ''):>7} {r.get('roe_pct', ''):>6} "
              f"{r.get('roa_pct', ''):>6} {r.get('opm_pct', ''):>6} "
              f"{r.get('mktcap_b', ''):>8}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
