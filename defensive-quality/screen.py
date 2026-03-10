#!/usr/bin/env python3
"""
Defensive Sector Quality - Current Stock Screen

Screens for quality stocks in defensive sectors (Consumer Defensive, Utilities, Healthcare).
Uses TTM data for current screening.

Usage:
    python3 defensive-quality/screen.py
    python3 defensive-quality/screen.py --preset india
    python3 defensive-quality/screen.py --exchange XETRA
    python3 defensive-quality/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
DEFENSIVE_SECTORS = ("Consumer Defensive", "Utilities", "Healthcare")
ROE_MIN = 0.06
OPM_MIN = 0.08
DE_MAX = 2.5
DIV_YIELD_MIN = 0.005
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data. Returns list of dicts."""
    sectors_sql = "', '".join(DEFENSIVE_SECTORS)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        SELECT
            k.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            p.industry,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(f.debtToEquityRatioTTM, 2) AS de_ratio,
            ROUND(f.dividendYieldTTM * 100, 2) AS div_yield_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM key_metrics_ttm k
        JOIN financial_ratios_ttm f ON k.symbol = f.symbol
        JOIN profile p ON k.symbol = p.symbol
        WHERE p.sector IN ('{sectors_sql}')
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND f.operatingProfitMarginTTM > {OPM_MIN}
          AND (f.debtToEquityRatioTTM IS NULL OR f.debtToEquityRatioTTM < {DE_MAX})
          AND f.dividendYieldTTM > {DIV_YIELD_MIN}
          AND k.marketCap > {mktcap_min}
          AND p.isActivelyTrading = true
          {exchange_filter}
        ORDER BY f.dividendYieldTTM DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Defensive Sector Quality - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("defensive-quality", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9
                    else f"{mktcap_threshold/1e6:.0f}M")
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    sectors_str = " | ".join(DEFENSIVE_SECTORS)
    print(f"Defensive Sector Quality Screen - {universe_name}")
    print(f"Sectors: {sectors_str}")
    print(f"Signal: ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, "
          f"D/E < {DE_MAX}, DivYield > {DIV_YIELD_MIN*100:.1f}%, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'Sector':<22} {'ROE%':>6} "
          f"{'OPM%':>6} {'D/E':>6} {'DivY%':>6} {'MCap$B':>8}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {str(r.get('companyName', ''))[:26]:<28} "
              f"{str(r.get('sector', ''))[:20]:<22} "
              f"{r.get('roe_pct', ''):>6} {r.get('opm_pct', ''):>6} "
              f"{r.get('de_ratio', ''):>6} {r.get('div_yield_pct', ''):>6} "
              f"{r.get('mktcap_b', ''):>8}")

    print(f"\n{len(results)} stocks qualify (ranked by dividend yield, descending).")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
