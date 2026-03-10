#!/usr/bin/env python3
"""
GARP (Growth at a Reasonable Price) - Current Stock Screen

Screens for GARP stocks using TTM valuation + latest FY revenue growth:
PEG < 1.5, P/E 5-50, ROE > 10%, D/E < 2.0, revenue growth > 15% YoY,
market cap > exchange threshold.

Usage:
    python3 garp/screen.py
    python3 garp/screen.py --preset india
    python3 garp/screen.py --exchange XETRA
    python3 garp/screen.py --cloud
    python3 garp/screen.py --json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
PEG_MAX = 1.5
PEG_MIN = 0.0
PE_MIN = 5
PE_MAX = 50
ROE_MIN = 0.10
DE_MAX = 2.0
REV_GROWTH_MIN = 0.15
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM valuation + latest FY revenue growth. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH latest_income AS (
            SELECT symbol, revenue, dateEpoch,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue > 0
        ),
        rev_growth AS (
            SELECT c.symbol,
                   ROUND((c.revenue - p.revenue) / ABS(p.revenue) * 100, 1) AS rev_growth_pct
            FROM latest_income c
            JOIN latest_income p ON c.symbol = p.symbol AND c.rn = 1 AND p.rn = 2
            WHERE p.revenue > 0
              AND (c.revenue - p.revenue) / ABS(p.revenue) > {REV_GROWTH_MIN}
        )
        SELECT
            f.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(f.priceToEarningsGrowthRatioTTM, 3) AS peg_ratio,
            ROUND(f.priceToEarningsRatioTTM, 2) AS pe_ratio,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(f.debtToEquityRatioTTM, 2) AS debt_to_equity,
            g.rev_growth_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM financial_ratios_ttm f
        JOIN key_metrics_ttm k ON f.symbol = k.symbol
        JOIN profile p ON f.symbol = p.symbol
        JOIN rev_growth g ON f.symbol = g.symbol
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
    parser = argparse.ArgumentParser(description="GARP (Growth at a Reasonable Price) live screen")
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
            f"python3 garp/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"GARP Screen | Universe: {universe_name}")
    print(f"Filters: PEG {PEG_MIN}-{PEG_MAX}, P/E {PE_MIN}-{PE_MAX}, "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, "
          f"RevGrowth > {REV_GROWTH_MIN*100:.0f}%, MCap > {mktcap_min/1e9:.1f}B local")
    print("=" * 95)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<26} {'PEG':>6} {'P/E':>6} "
          f"{'ROE%':>6} {'D/E':>6} {'RevGrw%':>8} {'MCap$B':>8}")
    print("-" * 95)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:24]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<26} "
              f"{r.get('peg_ratio', 'N/A'):>6} "
              f"{r.get('pe_ratio', 'N/A'):>6} "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('debt_to_equity', 'N/A'):>6} "
              f"{str(r.get('rev_growth_pct', 'N/A'))+'%':>8} "
              f"{r.get('mktcap_b', 'N/A'):>8}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), TTM valuation + latest FY revenue growth.")


if __name__ == "__main__":
    main()
