#!/usr/bin/env python3
"""
ETF Rebalancing Drag - Current Stock Screen

Shows quality stocks with the lowest ETF ownership ratio (least rebalancing exposure).
Uses live production data via Ceta Research API.

Usage:
    python3 etf-rebalancing/screen.py
    python3 etf-rebalancing/screen.py --preset india
    python3 etf-rebalancing/screen.py --exchange XETRA --verbose
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
ROE_MIN = 0.10
PE_MIN = 0.0
PE_MAX = 40.0
MIN_ETF_OWNERS = 1
MAX_STOCKS = 30


def main():
    parser = argparse.ArgumentParser(description="ETF Rebalancing Drag - current screen")
    add_common_args(parser)
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap = get_mktcap_threshold(exchanges)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    sql = f"""
    WITH etf_ownership AS (
        SELECT
            eh.asset as symbol,
            COUNT(DISTINCT eh.symbol) as etf_count,
            ROUND(SUM(eh.marketValue) / 1e6, 1) as total_etf_value_mn,
            ROUND(SUM(eh.marketValue) / NULLIF(k.marketCap, 0), 6) as ownership_ratio
        FROM etf_holder eh
        JOIN profile p ON eh.asset = p.symbol
        JOIN key_metrics_ttm k ON eh.asset = k.symbol
        WHERE k.returnOnEquityTTM > {ROE_MIN}
          AND k.marketCap > {mktcap}
          {exchange_where}
        GROUP BY eh.asset, k.marketCap
        HAVING COUNT(DISTINCT eh.symbol) >= {MIN_ETF_OWNERS}
    )
    SELECT
        eo.symbol,
        p.companyName,
        p.exchange,
        p.sector,
        ROUND(k.marketCap / 1e9, 2) as market_cap_bn,
        ROUND(k.returnOnEquityTTM * 100, 1) as roe_pct,
        ROUND(f.priceToEarningsRatioTTM, 1) as pe_ratio,
        eo.etf_count,
        eo.total_etf_value_mn,
        eo.ownership_ratio
    FROM etf_ownership eo
    JOIN profile p ON eo.symbol = p.symbol
    JOIN key_metrics_ttm k ON eo.symbol = k.symbol
    JOIN financial_ratios_ttm f ON eo.symbol = f.symbol
    WHERE f.priceToEarningsRatioTTM > {PE_MIN}
      AND f.priceToEarningsRatioTTM < {PE_MAX}
    ORDER BY eo.ownership_ratio ASC
    LIMIT {MAX_STOCKS}
    """

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    print(f"\nETF Rebalancing Drag Screen: {universe_name}")
    print(f"Signal: Lowest ETF ownership ratio (quality stocks with least passive exposure)")
    print(f"Filters: ROE > {ROE_MIN*100:.0f}%, P/E {PE_MIN:.0f}-{PE_MAX:.0f}, "
          f"MCap > {mktcap/1e9:.0f}B local, >= {MIN_ETF_OWNERS} ETF")
    print("=" * 110)

    rows = cr.query(sql, verbose=args.verbose, timeout=120, memory_mb=4096, threads=2)

    if not rows:
        print("No qualifying stocks found.")
        return

    print(f"\n{'#':>3} {'Symbol':<12} {'Name':<30} {'Exchange':<8} {'Sector':<20} "
          f"{'MCap(B)':>8} {'ROE%':>6} {'P/E':>6} {'ETFs':>5} {'ETF$M':>8} {'OwnRatio':>9}")
    print("-" * 110)

    for i, r in enumerate(rows, 1):
        name = (r.get('companyName') or '')[:28]
        sector = (r.get('sector') or '')[:18]
        print(f"{i:>3} {r['symbol']:<12} {name:<30} {r.get('exchange',''):<8} {sector:<20} "
              f"{r.get('market_cap_bn', 0):>8.1f} {r.get('roe_pct', 0):>5.1f}% "
              f"{r.get('pe_ratio', 0):>5.1f} {r.get('etf_count', 0):>5} "
              f"{r.get('total_etf_value_mn', 0):>7.1f} {r.get('ownership_ratio', 0):>9.6f}")

    print(f"\nTotal: {len(rows)} qualifying stocks")


if __name__ == "__main__":
    main()
