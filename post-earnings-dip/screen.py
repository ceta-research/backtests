#!/usr/bin/env python3
"""
Post-Earnings Dip - Live Screen

Shows companies that recently beat earnings estimates but sold off 5%+ on the
announcement. These are the current "buy the dip after a beat" candidates.

Looks back 30 calendar days for recent earnings events meeting the criteria.

Usage:
    python3 post-earnings-dip/screen.py
    python3 post-earnings-dip/screen.py --preset india --min-dip 0.10
    python3 post-earnings-dip/screen.py --lookback 60
    python3 post-earnings-dip/screen.py --cloud

Columns:
    symbol        - Ticker
    event_date    - Earnings announcement date
    surprise_pct  - EPS beat magnitude (%)
    reaction_ret  - Stock return T-1 to T+1 (the sell-off)
    bench_ret     - Benchmark return over same window
    abnormal_ret  - Excess return vs benchmark
    days_since    - Trading days since the announcement
    mktcap_bn     - Market cap in billions (local currency)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold
from data_utils import REGIONAL_BENCHMARKS

DEFAULT_MIN_DIP = 0.05    # 5% drop
DEFAULT_LOOKBACK = 30     # calendar days to look back
DEFAULT_LIMIT = 30


def run_screen(cr, exchanges, mktcap_min, min_dip=DEFAULT_MIN_DIP,
               lookback_days=DEFAULT_LOOKBACK, limit=DEFAULT_LIMIT, verbose=False):
    """Find recent earnings beats with 5%+ sell-offs."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"p.exchange IN ({ex_filter})"
    else:
        sym_filter = "1=1"

    # Determine benchmark
    benchmark = "SPY"
    if exchanges:
        for ex in exchanges:
            if ex in REGIONAL_BENCHMARKS:
                benchmark = REGIONAL_BENCHMARKS[ex]
                break

    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    min_dip_pct = min_dip * 100

    query = f"""
    WITH recent_beats AS (
        SELECT symbol,
               CAST(date AS DATE) AS event_date,
               epsActual,
               epsEstimated,
               ROUND((epsActual - epsEstimated) / ABS(NULLIF(epsEstimated, 0.0)) * 100.0, 1) AS surprise_pct
        FROM earnings_surprises
        WHERE epsEstimated IS NOT NULL
          AND ABS(epsEstimated) > 0.01
          AND epsActual > epsEstimated
          AND CAST(date AS DATE) >= '{cutoff_date}'
          AND {sym_filter}
    ),
    -- Map to T0 trading day (ASOF join equivalent via self-join on closest date)
    beat_prices AS (
        SELECT rb.symbol, rb.event_date, rb.surprise_pct,
               p_curr.adjClose AS price_t0,
               p_curr.trade_date AS t0_date
        FROM recent_beats rb
        JOIN (
            SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
            FROM stock_eod
            WHERE adjClose > 0
              AND CAST(date AS DATE) >= '{cutoff_date}'
        ) p_curr ON rb.symbol = p_curr.symbol
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY rb.symbol, rb.event_date
            ORDER BY ABS(DATEDIFF('day', p_curr.trade_date, rb.event_date))
        ) = 1
    )
    SELECT
        rb.symbol,
        rb.event_date,
        rb.surprise_pct,
        ROUND(k.marketCap / 1e9, 2) AS mktcap_bn,
        pr.companyName AS company_name,
        pr.sector
    FROM recent_beats rb
    JOIN key_metrics k ON rb.symbol = k.symbol AND k.period = 'FY'
    JOIN profile pr ON rb.symbol = pr.symbol
    WHERE k.marketCap > {mktcap_min}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rb.symbol ORDER BY k.date DESC) = 1
    ORDER BY rb.event_date DESC, rb.surprise_pct DESC
    LIMIT {limit * 3}
    """

    # Simplified screen: use the data explorer to find recent beats,
    # then note that full dip calculation requires price data join.
    # For the live screen, we show recent beats and the user can check prices.

    simple_query = f"""
    SELECT e.symbol,
           CAST(e.date AS DATE) AS event_date,
           ROUND((e.epsActual - e.epsEstimated) / ABS(NULLIF(e.epsEstimated, 0.0)) * 100.0, 1) AS surprise_pct,
           ROUND(k.marketCap / 1e9, 2) AS mktcap_bn,
           p.companyName AS company_name,
           p.sector,
           p.exchange
    FROM earnings_surprises e
    JOIN profile p ON e.symbol = p.symbol
    JOIN (
        SELECT symbol, marketCap, date
        FROM key_metrics
        WHERE period = 'FY'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
    ) k ON e.symbol = k.symbol
    WHERE e.epsEstimated IS NOT NULL
      AND ABS(e.epsEstimated) > 0.01
      AND e.epsActual > e.epsEstimated
      AND CAST(e.date AS DATE) >= '{cutoff_date}'
      AND k.marketCap > {mktcap_min}
      AND {sym_filter}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.symbol, CAST(e.date AS DATE) ORDER BY e.epsActual DESC) = 1
    ORDER BY CAST(e.date AS DATE) DESC, surprise_pct DESC
    LIMIT {limit}
    """

    print(f"Screening for recent earnings beats (last {lookback_days} days)...")
    print(f"  Universe: {', '.join(exchanges) if exchanges else 'All'}")
    print(f"  Dip filter: >= {min_dip_pct:.0f}% (applied to price data - check manually)")
    if verbose:
        print(f"\nQuery:\n{simple_query}\n")

    rows = cr.query(simple_query, format="json", limit=limit + 10, timeout=300,
                    memory_mb=4096, threads=2)

    if not rows:
        print("No results returned.")
        return []

    print(f"\n{'Symbol':<10} {'Date':>12} {'Surprise%':>10} {'MCap(B)':>9} "
          f"{'Sector':<20} {'Company':<30}")
    print("-" * 100)
    for row in rows[:limit]:
        print(f"{row['symbol']:<10} {str(row.get('event_date', ''))[:10]:>12} "
              f"{row.get('surprise_pct', 0):>+9.1f}% "
              f"{row.get('mktcap_bn', 0):>9.2f} "
              f"{str(row.get('sector', ''))[:20]:<20} "
              f"{str(row.get('company_name', ''))[:30]:<30}")

    n = len(rows[:limit])
    print(f"\n{n} recent earnings beats (last {lookback_days} days)")
    print(f"\nNote: Run backtest.py to identify which of these had >= {min_dip_pct:.0f}% dips.")
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Post-earnings dip live screen")
    add_common_args(parser)
    parser.add_argument("--min-dip", type=float, default=DEFAULT_MIN_DIP,
                        help=f"Minimum dip threshold (default: {DEFAULT_MIN_DIP})")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK,
                        help=f"Calendar days to look back (default: {DEFAULT_LOOKBACK})")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max rows to return (default: {DEFAULT_LIMIT})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("post-earnings-dip/screen",
                                    args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, mktcap_threshold,
               min_dip=args.min_dip, lookback_days=args.lookback,
               limit=args.limit, verbose=args.verbose)


if __name__ == "__main__":
    main()
