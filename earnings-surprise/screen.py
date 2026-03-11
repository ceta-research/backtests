#!/usr/bin/env python3
"""
Earnings Surprise Screen

Shows stocks with the largest recent earnings surprises (last 90 days).
Useful for finding potential PEAD trades or current market sentiment.

Usage:
    python3 earnings-surprise/screen.py
    python3 earnings-surprise/screen.py --preset india
    python3 earnings-surprise/screen.py --direction both --lookback 30
    python3 earnings-surprise/screen.py --direction negative

Columns:
    symbol       - Ticker symbol
    exchange     - Exchange code
    event_date   - Earnings announcement date
    surprise_pct - Surprise magnitude % = (actual - est) / |est| * 100
    epsActual    - Reported EPS
    epsEstimated - Consensus estimate
    mktcap_bn    - Market cap in billions (local currency)
    direction    - BEAT or MISS
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

DEFAULT_LOOKBACK_DAYS = 90
DEFAULT_LIMIT = 30
MIN_ESTIMATE = 0.01


def run_screen(cr, exchanges, mktcap_min, direction="positive",
               lookback_days=DEFAULT_LOOKBACK_DAYS, limit=DEFAULT_LIMIT,
               verbose=False):
    """Screen for recent earnings surprises."""

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_join = f"JOIN profile p ON es.symbol = p.symbol AND p.exchange IN ({ex_filter})"
        exchange_select_raw = "p.exchange AS exchange,"
        exchange_select_deduped = "exchange,"
    else:
        exchange_join = "LEFT JOIN profile p ON es.symbol = p.symbol"
        exchange_select_raw = "p.exchange AS exchange,"
        exchange_select_deduped = "exchange,"

    if direction == "positive":
        dir_filter = "AND es.epsActual > es.epsEstimated"
        order_col = "surprise_pct DESC"
    elif direction == "negative":
        dir_filter = "AND es.epsActual < es.epsEstimated"
        order_col = "surprise_pct ASC"
    else:  # both
        dir_filter = ""
        order_col = "ABS(surprise_pct) DESC"

    sql = f"""
    WITH raw_surprises AS (
        SELECT es.symbol,
            {exchange_select_raw}
            CAST(es.date AS DATE) AS event_date,
            es.epsActual,
            es.epsEstimated,
            ROUND((es.epsActual - es.epsEstimated) / ABS(es.epsEstimated) * 100, 2) AS surprise_pct,
            CASE WHEN es.epsActual > es.epsEstimated THEN 'BEAT'
                 WHEN es.epsActual < es.epsEstimated THEN 'MISS'
                 ELSE 'INLINE' END AS direction,
            ROW_NUMBER() OVER (PARTITION BY es.symbol, CAST(es.date AS DATE)
                               ORDER BY es.epsActual DESC) AS rn
        FROM earnings_surprises es
        {exchange_join}
        WHERE CAST(es.date AS DATE) >= CURRENT_DATE - INTERVAL '{lookback_days}' DAY
          AND es.epsEstimated IS NOT NULL
          AND ABS(es.epsEstimated) > {MIN_ESTIMATE}
          AND es.epsActual IS NOT NULL
          {dir_filter}
    ),
    deduped AS (
        SELECT symbol, {exchange_select_deduped} event_date, epsActual, epsEstimated,
            surprise_pct, direction
        FROM raw_surprises
        WHERE rn = 1
    ),
    with_mcap AS (
        SELECT d.symbol, d.exchange, d.event_date, d.epsActual, d.epsEstimated,
            d.surprise_pct, d.direction,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_bn
        FROM deduped d
        LEFT JOIN key_metrics_ttm k ON d.symbol = k.symbol
        WHERE k.marketCap IS NULL OR k.marketCap > {mktcap_min}
    )
    SELECT symbol, exchange, event_date, surprise_pct, epsActual, epsEstimated,
        mktcap_bn, direction
    FROM with_mcap
    ORDER BY {order_col}
    LIMIT {limit}
    """

    if verbose:
        print(f"Running earnings surprise screen...")
        print(f"  Direction: {direction}")
        print(f"  Lookback: {lookback_days} days")
        print(f"  Min market cap: ${mktcap_min/1e9:.1f}B local")
        print(f"  Min |estimate|: ${MIN_ESTIMATE}")

    results = cr.query(sql, format="json", limit=limit + 10, timeout=300,
                       memory_mb=4096, threads=2)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="Earnings Surprise live screen")
    add_common_args(parser)
    parser.add_argument("--direction", choices=["positive", "negative", "both"],
                        default="positive",
                        help="Surprise direction to screen (default: positive)")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f"Days to look back for earnings events (default: {DEFAULT_LOOKBACK_DAYS})")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max results to return (default: {DEFAULT_LIMIT})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("earnings-surprise/screen", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    results = run_screen(cr, exchanges, mktcap_threshold,
                          direction=args.direction, lookback_days=args.lookback,
                          limit=args.limit, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    dir_label = {"positive": "Biggest Beats", "negative": "Biggest Misses",
                 "both": "All Surprises"}[args.direction]
    print(f"\n{'=' * 90}")
    print(f"  EARNINGS SURPRISE SCREEN: {dir_label} ({universe_name}, last {args.lookback} days)")
    print(f"{'=' * 90}")
    print(f"  {'Symbol':<10} {'Exchange':<8} {'Date':>12} {'Surprise%':>10} "
          f"{'Actual':>8} {'Est':>8} {'MCap(B)':>10} {'Dir':>6}")
    print(f"  {'-' * 78}")

    for r in results:
        print(f"  {str(r.get('symbol', '')):<10} "
              f"{str(r.get('exchange', '')):<8} "
              f"{str(r.get('event_date', ''))[:10]:>12} "
              f"{r.get('surprise_pct', 0):>+9.1f}% "
              f"{r.get('epsActual', 0):>8.2f} "
              f"{r.get('epsEstimated', 0):>8.2f} "
              f"{str(r.get('mktcap_bn', 'N/A')):>10} "
              f"{str(r.get('direction', '')):>6}")

    print(f"\n  {len(results)} stocks found (lookback: {args.lookback}d, |est| > ${MIN_ESTIMATE})")
    print(f"{'=' * 90}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Saved to {args.output}")


if __name__ == "__main__":
    main()
