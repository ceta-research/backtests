#!/usr/bin/env python3
"""
ETF Concentration Screen

Current stock screen: find quality stocks with the lowest average ETF weight.
These stocks are held by institutional-grade ETFs but not heavily weighted,
suggesting they are less distorted by passive flow mechanics.

Usage:
    python3 etf-concentration/screen.py
    python3 etf-concentration/screen.py --preset us --top 50
    python3 etf-concentration/screen.py --preset india
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

ROE_MIN = 0.10
PE_MIN = 0.0
PE_MAX = 40.0
MIN_ETF_COUNT = 5


def main():
    parser = argparse.ArgumentParser(description="ETF Concentration screen")
    add_common_args(parser)
    parser.add_argument("--top", type=int, default=30, help="Number of stocks to show")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    ex_filter = ", ".join(f"'{e}'" for e in exchanges) if exchanges else "'NYSE','NASDAQ'"

    sql = f"""
    WITH concentration AS (
        SELECT
            eh.asset AS symbol,
            COUNT(DISTINCT eh.symbol) AS etf_count,
            ROUND(SUM(eh.weightPercentage), 4) AS total_weight,
            ROUND(AVG(eh.weightPercentage), 4) AS avg_weight,
            ROUND(MAX(eh.weightPercentage), 4) AS max_weight
        FROM etf_holder eh
        JOIN profile p ON eh.asset = p.symbol
        WHERE p.exchange IN ({ex_filter})
          AND eh.weightPercentage BETWEEN 0 AND 100
        GROUP BY eh.asset
        HAVING COUNT(DISTINCT eh.symbol) >= {MIN_ETF_COUNT}
    ),
    quality AS (
        SELECT
            k.symbol,
            k.returnOnEquityTTM AS roe,
            f.priceToEarningsRatioTTM AS pe,
            k.marketCapTTM AS market_cap,
            p.companyName,
            p.sector,
            p.exchange
        FROM key_metrics_ttm k
        JOIN financial_ratios_ttm f ON k.symbol = f.symbol
        JOIN profile p ON k.symbol = p.symbol
        WHERE p.exchange IN ({ex_filter})
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND f.priceToEarningsRatioTTM > {PE_MIN}
          AND f.priceToEarningsRatioTTM < {PE_MAX}
          AND k.marketCapTTM > {mktcap_threshold}
    )
    SELECT
        q.symbol,
        q.companyName,
        q.sector,
        q.exchange,
        ROUND(q.roe * 100, 1) AS roe_pct,
        ROUND(q.pe, 1) AS pe,
        ROUND(q.market_cap / 1e9, 2) AS mcap_b,
        c.etf_count,
        c.avg_weight AS avg_wt_pct,
        c.total_weight AS total_wt_pct,
        c.max_weight AS max_wt_pct
    FROM quality q
    JOIN concentration c ON q.symbol = c.symbol
    ORDER BY c.avg_weight ASC
    LIMIT {args.top}
    """

    print(f"ETF Concentration Screen: {universe_name}")
    print(f"Signal: Lowest avg ETF weight, ROE > {ROE_MIN*100:.0f}%, "
          f"P/E {PE_MIN:.0f}-{PE_MAX:.0f}, >= {MIN_ETF_COUNT} ETFs")
    print("=" * 120)

    rows = cr.query(sql, verbose=args.verbose, timeout=120, memory_mb=4096, threads=2)

    if not rows:
        print("No qualifying stocks found.")
        return

    print(f"{'#':<4} {'Symbol':<10} {'Company':<30} {'Sector':<22} "
          f"{'ROE%':>6} {'P/E':>6} {'MCap$B':>8} {'ETFs':>5} "
          f"{'AvgWt%':>8} {'TotWt%':>8} {'MaxWt%':>8}")
    print("-" * 120)

    for i, r in enumerate(rows, 1):
        print(f"{i:<4} {r['symbol']:<10} {str(r['companyName'])[:29]:<30} "
              f"{str(r['sector'])[:21]:<22} "
              f"{r['roe_pct']:>6.1f} {r['pe']:>6.1f} {r['mcap_b']:>8.2f} "
              f"{r['etf_count']:>5} {r['avg_wt_pct']:>8.4f} "
              f"{r['total_wt_pct']:>8.2f} {r['max_wt_pct']:>8.4f}")

    print(f"\n{len(rows)} stocks found. Lowest avg weight = least concentrated in ETFs.")


if __name__ == "__main__":
    main()
