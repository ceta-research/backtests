#!/usr/bin/env python3
"""
P/E Mean Reversion (Sector-Relative) - Current Stock Screen

Screens for stocks where the current P/E is < 60% of their sector's median P/E,
with stable quality fundamentals. Uses TTM data for live screening.

Usage:
    python3 pe-mean-revert/screen.py
    python3 pe-mean-revert/screen.py --preset india
    python3 pe-mean-revert/screen.py --exchange XETRA
    python3 pe-mean-revert/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
PE_MIN = 3.0
PE_MAX = 50.0
SECTOR_RATIO_MAX = 0.60    # Stock P/E < 60% of sector median
ROE_MIN = 0.08
DE_MAX = 2.0
MAX_STOCKS = 30
SECTOR_MIN_STOCKS = 5


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data.

    Computes sector median P/E from TTM data across all stocks in each sector,
    then finds stocks trading at deep discount to sector peers.
    Returns list of dicts.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH universe AS (
            -- All stocks with TTM P/E and sector data
            SELECT
                k.symbol,
                p.companyName,
                p.exchange,
                p.sector,
                fr.priceToEarningsRatioTTM AS pe_ttm,
                k.returnOnEquityTTM AS roe,
                fr.debtToEquityRatioTTM AS de,
                k.marketCap
            FROM key_metrics_ttm k
            JOIN financial_ratios_ttm fr ON k.symbol = fr.symbol
            JOIN profile p ON k.symbol = p.symbol
            WHERE fr.priceToEarningsRatioTTM BETWEEN {PE_MIN} AND {PE_MAX}
              AND k.returnOnEquityTTM > {ROE_MIN}
              AND (fr.debtToEquityRatioTTM IS NULL
                   OR (fr.debtToEquityRatioTTM >= 0 AND fr.debtToEquityRatioTTM < {DE_MAX}))
              AND k.marketCap > {mktcap_min}
              AND p.sector IS NOT NULL
              {exchange_filter}
        ),
        sector_medians AS (
            -- Sector median P/E (requires at least {SECTOR_MIN_STOCKS} stocks)
            SELECT
                exchange,
                sector,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ttm) AS median_pe,
                COUNT(*) AS n_sector_stocks
            FROM universe
            GROUP BY exchange, sector
            HAVING COUNT(*) >= {SECTOR_MIN_STOCKS}
        )
        SELECT
            u.symbol,
            u.companyName,
            u.exchange,
            u.sector,
            ROUND(u.pe_ttm, 2) AS pe_ttm,
            ROUND(sm.median_pe, 2) AS sector_median_pe,
            ROUND(u.pe_ttm / sm.median_pe, 3) AS pe_ratio_to_sector,
            ROUND((1 - u.pe_ttm / sm.median_pe) * 100, 1) AS discount_pct,
            ROUND(u.roe * 100, 1) AS roe_pct,
            ROUND(u.de, 2) AS debt_to_equity,
            ROUND(u.marketCap / 1e9, 2) AS mktcap_b
        FROM universe u
        JOIN sector_medians sm ON u.exchange = sm.exchange AND u.sector = sm.sector
        WHERE u.pe_ttm / sm.median_pe < {SECTOR_RATIO_MAX}
        ORDER BY u.pe_ttm / sm.median_pe ASC
        LIMIT {MAX_STOCKS}
    """

    results = client.query(sql, verbose=verbose)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="P/E Mean Reversion (Sector-Relative) live screen")
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
            f"python3 pe-mean-revert/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"P/E Mean Reversion Screen (Sector-Relative) | Universe: {universe_name}")
    print(f"Filters: PE {PE_MIN}-{PE_MAX}, sector discount >= 40% (ratio < {SECTOR_RATIO_MAX}), "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_min/1e9:.1f}B local")
    print("=" * 110)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<26} {'Sector':<22} {'PE TTM':>7} "
          f"{'Sect Med':>8} {'Ratio':>7} {'Disc%':>7} {'ROE%':>6} {'D/E':>5} {'MCap$B':>7}")
    print("-" * 110)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:24]
        sector = (r.get("sector") or "")[:20]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<26} {sector:<22} "
              f"{r.get('pe_ttm', 'N/A'):>7} "
              f"{r.get('sector_median_pe', 'N/A'):>8} "
              f"{r.get('pe_ratio_to_sector', 'N/A'):>7} "
              f"{r.get('discount_pct', 'N/A'):>6}% "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('debt_to_equity', 'N/A'):>5} "
              f"{r.get('mktcap_b', 'N/A'):>7}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), TTM metrics.")


if __name__ == "__main__":
    main()
