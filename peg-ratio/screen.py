#!/usr/bin/env python3
"""
PEG Ratio (GARP) - Current Stock Screen

Screens for GARP stocks using TTM data: PEG < 1.0, P/E 8-30,
ROE > 12%, D/E < 1.5, market cap > $1B.

Usage:
    python3 peg-ratio/screen.py
    python3 peg-ratio/screen.py --preset india
    python3 peg-ratio/screen.py --exchange XETRA
    python3 peg-ratio/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
PEG_MAX = 1.0
PEG_MIN = 0.0
PE_MIN = 8
PE_MAX = 30
ROE_MIN = 0.12
DE_MAX = 1.5
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold()
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        SELECT
            f.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(f.priceToEarningsGrowthRatioTTM, 3) AS peg_ratio,
            ROUND(f.priceToEarningsRatioTTM, 2) AS pe_ratio,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(f.debtToEquityRatioTTM, 2) AS debt_to_equity,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM financial_ratios_ttm f
        JOIN key_metrics_ttm k ON f.symbol = k.symbol
        JOIN profile p ON f.symbol = p.symbol
        WHERE f.priceToEarningsGrowthRatioTTM > {PEG_MIN}
          AND f.priceToEarningsGrowthRatioTTM < {PEG_MAX}
          AND f.priceToEarningsRatioTTM > {PE_MIN}
          AND f.priceToEarningsRatioTTM < {PE_MAX}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND f.debtToEquityRatioTTM >= 0
          AND f.debtToEquityRatioTTM < {DE_MAX}
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.symbol ORDER BY f.priceToEarningsGrowthRatioTTM ASC) = 1
        ORDER BY f.priceToEarningsGrowthRatioTTM ASC
        LIMIT {MAX_STOCKS}
    """

    results = client.query(sql, verbose=verbose)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="PEG Ratio (GARP) live screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", dest="output_json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch as CR
        cr = CR(api_key=args.api_key, base_url=args.base_url)
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = cr.execute_code(
            f"python3 peg-ratio/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"PEG Ratio (GARP) Screen | Universe: {universe_name}")
    print(f"Filters: PEG {PEG_MIN}-{PEG_MAX}, P/E {PE_MIN}-{PE_MAX}, "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_min/1e9:.1f}B local")
    print("=" * 80)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<30} {'PEG':>6} {'P/E':>6} "
          f"{'ROE%':>6} {'D/E':>6} {'MCap$B':>8}")
    print("-" * 80)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:28]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<30} "
              f"{r.get('peg_ratio', 'N/A'):>6} "
              f"{r.get('pe_ratio', 'N/A'):>6} "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('debt_to_equity', 'N/A'):>6} "
              f"{r.get('mktcap_b', 'N/A'):>8}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), TTM metrics.")


if __name__ == "__main__":
    main()
