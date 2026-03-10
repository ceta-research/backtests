#!/usr/bin/env python3
"""
Spinoff Current Screen

Shows recent spinoffs from the curated list and their current performance vs SPY.
Highlights spinoffs in the first 12 months (forced-selling window still active).

Usage:
    python3 spinoff/screen.py
    python3 spinoff/screen.py --months 36   # Show last 36 months of spinoffs
    python3 spinoff/screen.py --verbose

Data source: Ceta Research SQL API
Requires: CR_API_KEY environment variable
"""

import argparse
import duckdb
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet

# Import spinoff list from backtest
from spinoff.backtest import SPINOFFS, build_event_list


def screen_recent_spinoffs(client, months=24, verbose=False):
    """Fetch current performance for recent spinoffs."""
    cutoff = date.today() - timedelta(days=months * 30)
    recent_events = [
        e for e in build_event_list()
        if date.fromisoformat(e["event_date"]) >= cutoff
    ]

    if not recent_events:
        print(f"  No spinoffs in the last {months} months.")
        return

    print(f"  Screening {len(recent_events)} events from {cutoff} onward\n")

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='2GB'")

    all_syms = list({e["symbol"] for e in recent_events} | {"SPY"})
    sym_in = ", ".join(f"'{s}'" for s in all_syms)

    # Fetch recent prices (last 3 years)
    min_date = str(cutoff - timedelta(days=30))
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{min_date}'
          AND adjClose IS NOT NULL AND adjClose > 0
    """
    n = query_parquet(client, price_sql, con, "prices",
                      verbose=verbose, limit=2000000, timeout=300,
                      memory_mb=2048, threads=2)
    if n == 0:
        print("  No price data returned.")
        con.close()
        return

    # Build trading days from SPY
    con.execute("""
        CREATE TABLE trading_days AS
        SELECT trade_date, ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM prices WHERE symbol = 'SPY' ORDER BY trade_date
    """)

    today_row = con.execute("SELECT trade_date, day_num FROM trading_days ORDER BY trade_date DESC LIMIT 1").fetchone()
    if not today_row:
        con.close()
        return
    latest_date, latest_day_num = today_row

    print(f"  {'Symbol':<8} {'Type':<8} {'Spinoff Date':<14} {'Days':<6} {'Stock%':>8} {'SPY%':>8} {'CAR%':>8}  Description")
    print(f"  {'-' * 80}")

    for ev in sorted(recent_events, key=lambda x: x["event_date"]):
        symbol = ev["symbol"]
        event_date = ev["event_date"]
        category = ev["category"]
        description = ev["description"]

        # Find T0
        t0_row = con.execute(f"""
            SELECT day_num, trade_date FROM trading_days
            WHERE trade_date >= '{event_date}' ORDER BY trade_date LIMIT 1
        """).fetchone()
        if not t0_row:
            continue
        t0_num, t0_date = t0_row

        # T0 prices
        t0_prices = con.execute(f"""
            SELECT
                (SELECT adjClose FROM prices WHERE symbol = '{symbol}' AND trade_date = '{t0_date}'),
                (SELECT adjClose FROM prices WHERE symbol = 'SPY' AND trade_date = '{t0_date}')
        """).fetchone()
        if not t0_prices or t0_prices[0] is None or t0_prices[1] is None:
            continue

        stock_t0, spy_t0 = t0_prices

        # Latest prices
        lat_prices = con.execute(f"""
            SELECT
                (SELECT adjClose FROM prices WHERE symbol = '{symbol}' AND trade_date = '{latest_date}'),
                (SELECT adjClose FROM prices WHERE symbol = 'SPY' AND trade_date = '{latest_date}')
        """).fetchone()
        if not lat_prices or lat_prices[0] is None or lat_prices[1] is None:
            continue

        stock_lat, spy_lat = lat_prices
        stock_ret = (stock_lat - stock_t0) / stock_t0 * 100
        spy_ret = (spy_lat - spy_t0) / spy_t0 * 100
        car = stock_ret - spy_ret
        days_since = latest_day_num - t0_num

        flag = " <-- active window" if days_since <= 252 else ""
        print(
            f"  {symbol:<8} {category:<8} {event_date:<14} {days_since:<6} "
            f"{stock_ret:>+7.1f}% {spy_ret:>+7.1f}% {car:>+7.1f}%  {description[:30]}{flag}"
        )

    print(f"\n  Latest market date: {latest_date}")
    print(f"\n  Note: '<-- active window' = within T+252 (still in forced-selling recovery phase)")
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Recent spinoff performance screen")
    parser.add_argument("--api-key", type=str, help="Ceta Research API key")
    parser.add_argument("--base-url", type=str, help="API base URL (optional)")
    parser.add_argument("--months", type=int, default=24,
                        help="Show spinoffs from last N months (default: 24)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    print("=" * 65)
    print("  SPINOFF SCREEN: Recent Corporate Spinoffs")
    print(f"  Last {args.months} months | Benchmark: SPY")
    print("=" * 65)

    client = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    screen_recent_spinoffs(client, months=args.months, verbose=args.verbose)


if __name__ == "__main__":
    main()
