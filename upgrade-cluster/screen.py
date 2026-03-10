#!/usr/bin/env python3
"""
Analyst Upgrade Clusters - Live Screen

Shows stocks with recent analyst upgrade clusters: stocks where the aggregate
bullish analyst count (StrongBuy + Buy) jumped by 2 or more in the past 30 days.

Usage:
    python3 upgrade-cluster/screen.py
    python3 upgrade-cluster/screen.py --preset us --min-delta 3
    python3 upgrade-cluster/screen.py --exchange LSE --days 60
    python3 upgrade-cluster/screen.py --cloud

Columns:
    symbol        - Ticker symbol
    obs_date      - Date of the upgrade cluster observation
    upgrade_delta - Number of new bullish ratings (StrongBuy + Buy increase)
    bullish_count - Current total bullish analyst count
    bearish_count - Current total bearish analyst count
    mktcap_bn     - Market cap in billions (local currency)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

DEFAULT_MIN_DELTA = 2
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 30


def run_screen(cr, exchanges, mktcap_min, min_delta=DEFAULT_MIN_DELTA,
               days=DEFAULT_DAYS, limit=DEFAULT_LIMIT, verbose=False):
    """Run live upgrade cluster screen."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"p.exchange IN ({ex_filter})"
    else:
        sym_filter = "1=1"

    # CRITICAL: cast UINT16 to INTEGER before delta computation
    query = f"""
    WITH lagged AS (
        SELECT
            symbol,
            CAST(date AS DATE) AS obs_date,
            CAST(analystRatingsStrongBuy AS INTEGER) + CAST(analystRatingsBuy AS INTEGER)
                AS bullish_count,
            CAST(analystRatingsSell AS INTEGER) + CAST(analystRatingsStrongSell AS INTEGER)
                AS bearish_count,
            LAG(CAST(analystRatingsStrongBuy AS INTEGER) + CAST(analystRatingsBuy AS INTEGER))
                OVER (PARTITION BY symbol ORDER BY date) AS prev_bullish,
            LAG(CAST(date AS DATE))
                OVER (PARTITION BY symbol ORDER BY date) AS prev_date
        FROM grades_historical
        WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
    ),
    clusters AS (
        SELECT
            symbol,
            obs_date,
            bullish_count,
            bearish_count,
            bullish_count - prev_bullish AS upgrade_delta
        FROM lagged
        WHERE prev_bullish IS NOT NULL
          AND (obs_date - prev_date) <= 30
          AND bullish_count - prev_bullish >= {min_delta}
    )
    SELECT
        c.symbol,
        c.obs_date,
        c.upgrade_delta,
        c.bullish_count,
        c.bearish_count,
        ROUND(k.marketCap / 1e9, 1) AS mktcap_bn
    FROM clusters c
    JOIN profile p ON c.symbol = p.symbol
    JOIN key_metrics k ON c.symbol = k.symbol AND k.period = 'FY'
    WHERE {sym_filter}
      AND k.marketCap > {mktcap_min}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY k.date DESC, c.obs_date DESC) = 1
    ORDER BY c.upgrade_delta DESC, c.obs_date DESC
    LIMIT {limit}
    """

    print(f"Running upgrade cluster screen (min delta: {min_delta}, last {days} days)...")
    if verbose:
        print(f"Query:\n{query}\n")

    rows = cr.query(query, format="json", limit=limit + 10, timeout=300,
                    memory_mb=4096, threads=2)

    if not rows:
        print("No upgrade clusters found in the specified window.")
        return []

    print(f"\n{'Symbol':<12} {'Date':>12} {'Delta':>7} {'Bullish':>9} {'Bearish':>9} {'MCap(B)':>10}")
    print("-" * 65)
    for row in rows:
        print(f"{row['symbol']:<12} {str(row.get('obs_date', ''))[:10]:>12} "
              f"{row.get('upgrade_delta', 0):>7} "
              f"{row.get('bullish_count', 0):>9} "
              f"{row.get('bearish_count', 0):>9} "
              f"{row.get('mktcap_bn', 0):>10.1f}")

    print(f"\n{len(rows)} stocks with {min_delta}+ analyst upgrade cluster in past {days} days")
    return rows


def main():
    parser = argparse.ArgumentParser(description="Analyst Upgrade Clusters live screen")
    add_common_args(parser)
    parser.add_argument("--min-delta", type=int, default=DEFAULT_MIN_DELTA,
                        help=f"Minimum bullish count increase (default: {DEFAULT_MIN_DELTA})")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"Lookback window in days (default: {DEFAULT_DAYS})")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max rows to return (default: {DEFAULT_LIMIT})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("upgrade-cluster/screen", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, mktcap_threshold,
               min_delta=args.min_delta, days=args.days,
               limit=args.limit, verbose=args.verbose)


if __name__ == "__main__":
    main()
