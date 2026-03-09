#!/usr/bin/env python3
"""
Earnings Beat Streaks - Live Screen

Shows companies currently on active earnings beat streaks (3+ consecutive beats).
Uses the most recent earnings data to find companies beating analyst estimates
quarter after quarter.

Usage:
    python3 beat-streaks/screen.py
    python3 beat-streaks/screen.py --preset us --min-streak 4
    python3 beat-streaks/screen.py --cloud

Columns:
    symbol        - Ticker symbol
    current_streak - Number of consecutive beats
    avg_surprise  - Average surprise percentage during streak
    streak_start  - Date of first beat in current streak
    latest_beat   - Most recent beat date
    mktcap_bn     - Market cap in billions (local currency)
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

DEFAULT_MIN_STREAK = 3
DEFAULT_LIMIT = 30


def run_screen(cr, exchanges, mktcap_min, min_streak=DEFAULT_MIN_STREAK,
               limit=DEFAULT_LIMIT, verbose=False):
    """Run live beat streak screen."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    query = f"""
    WITH ordered_earnings AS (
        SELECT symbol,
            CAST(date AS DATE) AS event_date,
            epsActual AS actual,
            epsEstimated AS estimated,
            CASE WHEN epsActual > epsEstimated THEN 1 ELSE 0 END AS is_beat,
            ROUND((epsActual - epsEstimated) / ABS(NULLIF(epsEstimated, 0)) * 100, 1) AS surprise_pct,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS recency_rank
        FROM earnings_surprises
        WHERE epsEstimated IS NOT NULL
          AND ABS(epsEstimated) > 0.01
          AND epsActual IS NOT NULL
          AND {sym_filter}
    ),
    streak_calc AS (
        SELECT *, SUM(CASE WHEN is_beat = 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY symbol ORDER BY recency_rank
                  ROWS UNBOUNDED PRECEDING) AS streak_breaker
        FROM ordered_earnings
    ),
    streaks AS (
        SELECT symbol,
            COUNT(*) AS current_streak,
            ROUND(AVG(surprise_pct), 1) AS avg_surprise_pct,
            ROUND(MIN(surprise_pct), 1) AS min_surprise_pct,
            MIN(event_date) AS streak_start,
            MAX(event_date) AS latest_beat
        FROM streak_calc
        WHERE streak_breaker = 0 AND is_beat = 1
        GROUP BY symbol
        HAVING COUNT(*) >= {min_streak}
    )
    SELECT s.symbol, s.current_streak, s.avg_surprise_pct, s.min_surprise_pct,
        s.streak_start, s.latest_beat,
        ROUND(k.marketCap / 1e9, 1) AS mktcap_bn
    FROM streaks s
    JOIN key_metrics k ON s.symbol = k.symbol AND k.period = 'FY'
    WHERE k.marketCap > {mktcap_min}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY k.date DESC) = 1
    ORDER BY s.current_streak DESC, s.avg_surprise_pct DESC
    LIMIT {limit}
    """

    print(f"Running beat streak screen (min streak: {min_streak}, limit: {limit})...")
    if verbose:
        print(f"Query:\n{query}\n")

    rows = cr.query(query, format="json", limit=limit + 10, timeout=300,
                    memory_mb=4096, threads=2)

    if not rows:
        print("No results returned.")
        return []

    # Print results
    print(f"\n{'Symbol':<12} {'Streak':>7} {'Avg%':>7} {'Min%':>7} {'Start':>12} {'Latest':>12} {'MCap(B)':>10}")
    print("-" * 75)
    for row in rows:
        print(f"{row['symbol']:<12} {row['current_streak']:>7} "
              f"{row.get('avg_surprise_pct', 0):>+6.1f}% "
              f"{row.get('min_surprise_pct', 0):>+6.1f}% "
              f"{str(row.get('streak_start', ''))[:10]:>12} "
              f"{str(row.get('latest_beat', ''))[:10]:>12} "
              f"{row.get('mktcap_bn', 0):>10.1f}")

    print(f"\n{len(rows)} companies with {min_streak}+ consecutive beat streaks")
    return rows


def main():
    parser = argparse.ArgumentParser(description="Beat Streaks live screen")
    add_common_args(parser)
    parser.add_argument("--min-streak", type=int, default=DEFAULT_MIN_STREAK,
                        help=f"Minimum streak length (default: {DEFAULT_MIN_STREAK})")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max rows to return (default: {DEFAULT_LIMIT})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("beat-streaks/screen", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, mktcap_threshold,
               min_streak=args.min_streak, limit=args.limit, verbose=args.verbose)


if __name__ == "__main__":
    main()
