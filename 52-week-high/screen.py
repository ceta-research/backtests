#!/usr/bin/env python3
"""52-Week High Proximity screen: the book a rebalance on the screen's day would hold.

It runs the backtest's own fetch and screen on that one date (backtest.fetch_data_via_api, screen_stocks), with
data checks dc-3 on, so it ranks as the backtest does:
  - Universe: ordinary shares under universe rules ur-4 (load_universe, then the screen's semi-join on its
    universe table), less the lines its line starts and line ends leave out on the day.
  - Size: market cap over the leg's threshold, read as check M's cap_used (the latest FY row filed 45 days before
    the day, over check M's divisor). The floor is in the reporting currency, as in the backtest.
  - Signal: close / MAX(high) over the 252 bars ending at the last traded bar in the 10 days to the day
    [George & Hwang 2004], 100 bars at least; one line per security.
  - Book: the top 30 by proximity ratio, exact ties to the larger cap_used, then the symbol.
One leg at a time. The fetch's progress goes to stderr, the book to stdout.

On the day itself no later bar exists, so check T's closed-day test, which compares a date with three or more other
dates in its window, is skipped (a vendor row on a market holiday that day can be the signal bar), and the dedup's
preference for the line buyable at entry has no entry bar to read.

Usage:
    python3 52-week-high/screen.py                     # US (default)
    python3 52-week-high/screen.py --preset india
    python3 52-week-high/screen.py --exchange XETRA
    python3 52-week-high/screen.py --preset us --csv
"""

import argparse
import contextlib
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold
from data_utils import DataChecks, UniverseRules
from backtest import DATA_CHECKS, UNIVERSE_RULES, fetch_data_via_api, screen_stocks


def screen(client, exchanges, day=None, verbose=False):
    """The book on day (today in UTC when None) on these exchanges: [(symbol, cap_used, proximity_ratio)] in rank
    order, [] when the leg has no data."""
    day = day or datetime.now(timezone.utc).date()
    mktcap_min = get_mktcap_threshold(exchanges)
    dc, ur = DataChecks("on", DATA_CHECKS), UniverseRules(UNIVERSE_RULES)
    con = fetch_data_via_api(client, exchanges, [day], verbose=verbose, mktcap_min=mktcap_min, dc=dc, ur=ur)
    if con is None:
        return []
    try:
        return screen_stocks(con, day, mktcap_min, verbose=verbose, dc=dc, ur=ur)
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description="52-Week High Proximity screen (live): the backtest's book today")
    parser.add_argument("--exchange", type=str,
                        help="Exchange code(s), comma-separated (e.g. SHZ,SHH)")
    parser.add_argument("--preset", type=str, choices=sorted(EXCHANGE_PRESETS.keys()),
                        help="Exchange preset")
    parser.add_argument("--api-key", type=str,
                        help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str)
    parser.add_argument("--csv", action="store_true", help="Output as CSV")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.preset:
        exchanges = EXCHANGE_PRESETS[args.preset]["exchanges"]
    elif args.exchange:
        exchanges = [e.strip().upper() for e in args.exchange.split(",")]
    else:
        exchanges = ["NYSE", "NASDAQ", "AMEX"]

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    day = datetime.now(timezone.utc).date()
    with contextlib.redirect_stdout(sys.stderr):
        book = screen(cr, exchanges, day, verbose=args.verbose)

    rows = [(i, s, cap, ratio, (1 - ratio) * 100) for i, (s, cap, ratio) in enumerate(book, 1)]
    if args.csv:
        print("rank,symbol,market_cap,proximity_ratio,pct_below_high")
        for r in rows:
            print(",".join(str(v) for v in r))
        return
    print(f"52-Week High Proximity Screen: {', '.join(exchanges)} on {day}")
    print(f"Universe: ordinary shares (universe rules ur-4), market cap over "
          f"{get_mktcap_threshold(exchanges) / 1e9:g}B in the reporting currency (data checks dc-3)")
    print("Signal: close / MAX(high) over the 252 bars ending at the last traded bar in the 10 days to the day\n")
    if not rows:
        print("No qualifying stocks found.")
        return
    print(f"{'Rank':>4}  {'Symbol':<14} {'MCap (B)':>12} {'Proximity':>10} {'% below':>8}")
    for i, s, cap, ratio, below in rows:
        print(f"{i:>4}  {s:<14} {cap / 1e9:>12.2f} {ratio:>10.4f} {below:>8.1f}")
    print("\nData: Ceta Research (FMP financial data warehouse)")
    print("Proximity ratio: 1.00 = on its 52-week high | 0.95 = 5% below it")


if __name__ == "__main__":
    main()
