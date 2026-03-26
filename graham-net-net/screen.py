#!/usr/bin/env python3
"""
Graham Net-Net Screen - Current qualifying stocks

Finds stocks currently trading below their NCAV per share.
NCAV = Net Current Asset Value = Current Assets - All Liabilities - Preferred Stock
Source: key_metrics_ttm.grahamNetNetTTM (FMP pre-computed)

Usage:
    python3 graham-net-net/screen.py
    python3 graham-net-net/screen.py --preset india
    python3 graham-net-net/screen.py --exchange JPX
    python3 graham-net-net/screen.py --cloud  (runs via Ceta Research Code Execution API)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, EXCHANGE_PRESETS
from backtest import NETNET_MKTCAP_THRESHOLDS, DEFAULT_MKTCAP, get_netnet_mktcap_threshold


SIMPLE_SCREEN_SQL = """
-- Graham Net-Net Screen: stocks trading below liquidation value
-- NCAV per share = Current Assets - All Liabilities - Preferred Stock (all divided by shares)
-- Net-net = price < NCAV per share
SELECT
    k.symbol,
    p.exchange,
    p.sector,
    ROUND(s.adjClose, 2) AS price,
    ROUND(k.grahamNetNetTTM, 2) AS ncav_per_share,
    ROUND(s.adjClose / k.grahamNetNetTTM, 3) AS price_to_ncav,
    ROUND((k.grahamNetNetTTM - s.adjClose) / k.grahamNetNetTTM * 100, 1) AS discount_pct,
    ROUND(k.marketCap / 1e6, 1) AS mktcap_m_local
FROM key_metrics_ttm k
JOIN (
    SELECT symbol, adjClose
    FROM stock_eod
    WHERE date >= '2025-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
) s ON k.symbol = s.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE k.grahamNetNetTTM > 0
    AND s.adjClose > 0.50
    AND s.adjClose < k.grahamNetNetTTM
    AND k.marketCap > {mktcap_min}
    {exchange_filter}
ORDER BY price_to_ncav ASC
LIMIT 50
"""

QUALITY_SCREEN_SQL = """
-- Quality-filtered Graham Net-Net Screen
-- Adds: current ratio > 1.5 (liquidity), ROE > 0 (profitable)
SELECT
    k.symbol,
    p.exchange,
    p.sector,
    ROUND(s.adjClose, 2) AS price,
    ROUND(k.grahamNetNetTTM, 2) AS ncav_per_share,
    ROUND(s.adjClose / k.grahamNetNetTTM, 3) AS price_to_ncav,
    ROUND((k.grahamNetNetTTM - s.adjClose) / k.grahamNetNetTTM * 100, 1) AS discount_pct,
    ROUND(k.currentRatioTTM, 2) AS current_ratio,
    ROUND(r.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(k.marketCap / 1e6, 1) AS mktcap_m_local
FROM key_metrics_ttm k
JOIN financial_ratios_ttm r ON k.symbol = r.symbol
JOIN (
    SELECT symbol, adjClose
    FROM stock_eod
    WHERE date >= '2025-01-01'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
) s ON k.symbol = s.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE k.grahamNetNetTTM > 0
    AND s.adjClose > 0.50
    AND s.adjClose < k.grahamNetNetTTM
    AND k.currentRatioTTM >= 1.5
    AND r.returnOnEquityTTM > 0
    AND k.marketCap > {mktcap_min}
    {exchange_filter}
ORDER BY price_to_ncav ASC
LIMIT 50
"""


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run current net-net screen and print results."""
    if exchanges:
        ex_list = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_list})"
    else:
        exchange_filter = ""

    print("\n--- Simple Net-Net Screen (price < NCAV per share) ---")
    sql = SIMPLE_SCREEN_SQL.format(mktcap_min=mktcap_min, exchange_filter=exchange_filter)
    results = client.query(sql, verbose=verbose)
    if results:
        print(f"{'Symbol':<12} {'Exch':<6} {'Price':>8} {'NCAV/sh':>8} {'P/NCAV':>7} {'Disc%':>7} {'Sector'}")
        print("-" * 75)
        for r in results:
            print(f"{r['symbol']:<12} {r['exchange']:<6} {r['price']:>8.2f} "
                  f"{r['ncav_per_share']:>8.2f} {r['price_to_ncav']:>7.3f} "
                  f"{r['discount_pct']:>6.1f}% {(r['sector'] or '')[:25]}")
        print(f"\n  {len(results)} qualifying stocks")
    else:
        print("  No qualifying stocks found.")

    print("\n--- Quality-Filtered Screen (+ current ratio > 1.5, ROE > 0) ---")
    sql2 = QUALITY_SCREEN_SQL.format(mktcap_min=mktcap_min, exchange_filter=exchange_filter)
    results2 = client.query(sql2, verbose=verbose)
    if results2:
        print(f"{'Symbol':<12} {'Exch':<6} {'Price':>8} {'NCAV/sh':>8} {'P/NCAV':>7} "
              f"{'CR':>5} {'ROE%':>6} {'Sector'}")
        print("-" * 80)
        for r in results2:
            print(f"{r['symbol']:<12} {r['exchange']:<6} {r['price']:>8.2f} "
                  f"{r['ncav_per_share']:>8.2f} {r['price_to_ncav']:>7.3f} "
                  f"{r['current_ratio']:>5.2f} {r['roe_pct']:>5.1f}% {(r['sector'] or '')[:25]}")
        print(f"\n  {len(results2)} qualifying stocks (quality-filtered)")
    else:
        print("  No qualifying stocks (quality-filtered).")

    return results


def main():
    parser = argparse.ArgumentParser(description="Graham Net-Net current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run via Ceta Research Code Execution API")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch as CR
        cr = CR(api_key=args.api_key, base_url=args.base_url)
        with open(__file__) as f:
            code = f.read()
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = cr.execute_code(code, args=" ".join(cloud_args))
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_netnet_mktcap_threshold(exchanges)

    print(f"Graham Net-Net Screen — {universe_name}")
    print(f"Filter: price < NCAV/share, mktcap > {mktcap_threshold/1e6:.0f}M local")

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)


if __name__ == "__main__":
    main()
