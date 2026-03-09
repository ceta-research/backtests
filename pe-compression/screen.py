#!/usr/bin/env python3
"""
P/E Compression (Mean Reversion) - Current Stock Screen

Screens for stocks where the current P/E has compressed >= 15% below the
5-year historical average, with stable fundamentals. Uses FY annual + TTM data.

Usage:
    python3 pe-compression/screen.py
    python3 pe-compression/screen.py --preset india
    python3 pe-compression/screen.py --exchange XETRA
    python3 pe-compression/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
PE_MIN = 5.0
PE_MAX = 40.0
COMPRESSION_MAX = 0.85     # Current P/E < 85% of 5yr average
N_PRIOR_MIN = 3
ROE_MIN = 0.10
DE_MAX = 2.0
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using FY historical + TTM current data.

    Computes 5-year average P/E from FY data, compares to current TTM P/E.
    Returns list of dicts.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH fy_pe_history AS (
            -- Historical annual P/E ratios per symbol (last 6 FY filings)
            SELECT
                r.symbol,
                r.priceToEarningsRatio AS pe,
                r.dateEpoch AS filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY r.symbol ORDER BY r.dateEpoch DESC) AS rn,
                AVG(r.priceToEarningsRatio) OVER (
                    PARTITION BY r.symbol
                    ORDER BY r.dateEpoch
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ) AS avg_pe_5yr,
                COUNT(r.priceToEarningsRatio) OVER (
                    PARTITION BY r.symbol
                    ORDER BY r.dateEpoch
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ) AS n_prior_years
            FROM financial_ratios r
            JOIN profile p ON r.symbol = p.symbol
            WHERE r.period = 'FY'
              AND r.priceToEarningsRatio > 0
              AND r.priceToEarningsRatio < 200
              {exchange_filter}
        ),
        latest_fy AS (
            -- Most recent FY P/E + historical average
            SELECT symbol, pe AS fy_pe, avg_pe_5yr, n_prior_years
            FROM fy_pe_history
            WHERE rn = 1
              AND avg_pe_5yr IS NOT NULL
              AND n_prior_years >= {N_PRIOR_MIN}
              AND pe > {PE_MIN}
              AND pe < {PE_MAX}
        )
        SELECT
            f.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(f.fy_pe, 2) AS fy_pe_current,
            ROUND(f.avg_pe_5yr, 2) AS pe_5yr_avg,
            ROUND(f.fy_pe / f.avg_pe_5yr, 3) AS pe_ratio_to_avg,
            ROUND((1 - f.fy_pe / f.avg_pe_5yr) * 100, 1) AS compression_pct,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(fr.debtToEquityRatioTTM, 2) AS debt_to_equity,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM latest_fy f
        JOIN key_metrics_ttm k ON f.symbol = k.symbol
        JOIN financial_ratios_ttm fr ON f.symbol = fr.symbol
        JOIN profile p ON f.symbol = p.symbol
        WHERE f.fy_pe / f.avg_pe_5yr < {COMPRESSION_MAX}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND (fr.debtToEquityRatioTTM IS NULL
               OR (fr.debtToEquityRatioTTM >= 0 AND fr.debtToEquityRatioTTM < {DE_MAX}))
          AND k.marketCap > {mktcap_min}
        ORDER BY f.fy_pe / f.avg_pe_5yr ASC
        LIMIT {MAX_STOCKS}
    """

    results = client.query(sql, verbose=verbose)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="P/E Compression live screen")
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
            f"python3 pe-compression/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"P/E Compression Screen | Universe: {universe_name}")
    print(f"Filters: PE {PE_MIN}-{PE_MAX}, compression >= 15% (ratio < {COMPRESSION_MAX}), "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_min/1e9:.1f}B local")
    print("=" * 100)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<28} {'FY PE':>7} {'5yr Avg':>7} "
          f"{'Ratio':>7} {'Compress%':>10} {'ROE%':>6} {'D/E':>6} {'MCap$B':>8}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:26]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<28} "
              f"{r.get('fy_pe_current', 'N/A'):>7} "
              f"{r.get('pe_5yr_avg', 'N/A'):>7} "
              f"{r.get('pe_ratio_to_avg', 'N/A'):>7} "
              f"{r.get('compression_pct', 'N/A'):>9}% "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('debt_to_equity', 'N/A'):>6} "
              f"{r.get('mktcap_b', 'N/A'):>8}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), FY annual metrics.")


if __name__ == "__main__":
    main()
