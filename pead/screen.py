#!/usr/bin/env python3
"""
PEAD Screen: Recent Earnings Surprises

Shows stocks with recent earnings surprises (last 30 days).
Use to find potential PEAD trades.

Usage:
    # Default: positive surprises, US stocks
    python3 pead/screen.py

    # Negative surprises
    python3 pead/screen.py --direction negative

    # Both directions
    python3 pead/screen.py --direction both

    # Filter by market cap
    python3 pead/screen.py --min-mcap 10000000000

    # Different exchange
    python3 pead/screen.py --preset india

    # Cloud execution
    python3 pead/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_MCAP = 500_000_000
DEFAULT_MIN_SURPRISE = 0.05  # 5% surprise minimum
MIN_ESTIMATE = 0.01


def run_screen(client, exchanges=None, direction="positive", lookback_days=DEFAULT_LOOKBACK_DAYS,
               min_mcap=DEFAULT_MIN_MCAP, min_surprise=DEFAULT_MIN_SURPRISE, limit=50, verbose=False):
    """Screen for recent earnings surprises."""

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_join = f"JOIN profile p ON s.symbol = p.symbol AND p.exchange IN ({ex_filter})"
    else:
        exchange_join = ""

    if direction == "positive":
        dir_filter = "AND s.epsActual > s.epsEstimated"
        surprise_filter = f"AND (s.epsActual - s.epsEstimated) / ABS(s.epsEstimated) > {min_surprise}"
    elif direction == "negative":
        dir_filter = "AND s.epsActual < s.epsEstimated"
        surprise_filter = f"AND (s.epsEstimated - s.epsActual) / ABS(s.epsEstimated) > {min_surprise}"
    else:  # both
        dir_filter = ""
        surprise_filter = f"AND ABS(s.epsActual - s.epsEstimated) / ABS(s.epsEstimated) > {min_surprise}"

    sql = f"""
        WITH surprises AS (
            SELECT s.symbol,
                CAST(s.date AS DATE) AS event_date,
                s.epsActual,
                s.epsEstimated,
                ROUND((s.epsActual - s.epsEstimated) / ABS(s.epsEstimated) * 100, 1) AS surprise_pct,
                CASE WHEN s.epsActual > s.epsEstimated THEN 'BEAT'
                     WHEN s.epsActual < s.epsEstimated THEN 'MISS'
                     ELSE 'INLINE' END AS direction
            FROM earnings_surprises s
            {exchange_join}
            WHERE CAST(s.date AS DATE) >= CURRENT_DATE - INTERVAL '{lookback_days}' DAY
              AND s.epsEstimated IS NOT NULL
              AND ABS(s.epsEstimated) > {MIN_ESTIMATE}
              AND s.epsActual IS NOT NULL
              {dir_filter}
              {surprise_filter}
        ),
        with_mcap AS (
            SELECT su.*,
                ROUND(k.marketCap / 1e9, 1) AS mktcap_bn
            FROM surprises su
            LEFT JOIN key_metrics_ttm k ON su.symbol = k.symbol
            WHERE k.marketCap IS NULL OR k.marketCap > {min_mcap}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY su.symbol ORDER BY k.date DESC) = 1
        )
        SELECT symbol, event_date, epsActual, epsEstimated,
            surprise_pct, direction, mktcap_bn
        FROM with_mcap
        ORDER BY ABS(surprise_pct) DESC
        LIMIT {limit}
    """

    if verbose:
        print(f"Running screen query...")
        print(f"  Direction: {direction}")
        print(f"  Lookback: {lookback_days} days")
        print(f"  Min market cap: ${min_mcap/1e9:.1f}B")
        print(f"  Min surprise: {min_surprise*100:.0f}%")

    results = client.query(sql, verbose=verbose)
    return results


def main():
    parser = argparse.ArgumentParser(description="PEAD screen: recent earnings surprises")
    add_common_args(parser)
    parser.add_argument("--direction", choices=["positive", "negative", "both"],
                        default="positive", help="Surprise direction (default: positive)")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f"Days to look back (default: {DEFAULT_LOOKBACK_DAYS})")
    parser.add_argument("--min-mcap", type=float, default=DEFAULT_MIN_MCAP,
                        help=f"Minimum market cap (default: ${DEFAULT_MIN_MCAP/1e9:.1f}B)")
    parser.add_argument("--min-surprise", type=float, default=DEFAULT_MIN_SURPRISE,
                        help=f"Minimum surprise ratio (default: {DEFAULT_MIN_SURPRISE})")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max results (default: 50)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("pead", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    results = run_screen(cr, exchanges=exchanges, direction=args.direction,
                          lookback_days=args.lookback, min_mcap=args.min_mcap,
                          min_surprise=args.min_surprise, limit=args.limit,
                          verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    # Print results
    dir_label = {"positive": "Beats", "negative": "Misses", "both": "All"}[args.direction]
    print(f"\n{'=' * 80}")
    print(f"  PEAD SCREEN: {dir_label} ({universe_name}, last {args.lookback} days)")
    print(f"{'=' * 80}")
    print(f"  {'Symbol':<8} {'Date':>12} {'Actual':>8} {'Est':>8} {'Surprise':>10} {'Dir':>6} {'MCap($B)':>10}")
    print(f"  {'-' * 68}")

    for r in results:
        print(f"  {r['symbol']:<8} {r['event_date']:>12} {r['epsActual']:>8.2f} {r['epsEstimated']:>8.2f} "
              f"{r['surprise_pct']:>+9.1f}% {r['direction']:>6} "
              f"{r.get('mktcap_bn', 'N/A'):>10}")

    print(f"\n  {len(results)} stocks found")
    print(f"{'=' * 80}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Saved to {args.output}")


if __name__ == "__main__":
    main()
