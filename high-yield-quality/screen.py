#!/usr/bin/env python3
"""
High Yield + Quality Stock Screen

Current stock screen using TTM data. Returns qualifying stocks ranked by dividend yield.

Signal: DivYield > 2%, ROA > 5%, CR > 1.0, D/E < 1.5, Payout < 80%
Selection: Top 30 by dividend yield DESC

Usage:
    python3 high-yield-quality/screen.py
    python3 high-yield-quality/screen.py --preset india
    python3 high-yield-quality/screen.py --exchange XETRA
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

DIVIDEND_YIELD_MIN = 0.02
ROA_MIN = 0.05
CR_MIN = 1.0
DE_MAX = 1.5
PAYOUT_MAX = 0.80
MAX_STOCKS = 30


def main():
    parser = argparse.ArgumentParser(description="High Yield + Quality stock screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Code Execution API)")
    parser.add_argument("--limit", type=int, default=MAX_STOCKS,
                        help=f"Max stocks to return (default {MAX_STOCKS})")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_join = f"JOIN profile p ON k.symbol = p.symbol"
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_join = ""
        exchange_where = ""

    sql = f"""
    SELECT
      k.symbol,
      k.dividendYieldTTM AS dividend_yield,
      r.dividendPayoutRatioTTM AS payout_ratio,
      r.debtToEquityRatioTTM AS debt_to_equity,
      r.returnOnAssetsTTM AS roa,
      r.currentRatioTTM AS current_ratio,
      k.marketCap
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm r ON k.symbol = r.symbol
    {exchange_join}
    WHERE k.dividendYieldTTM > {DIVIDEND_YIELD_MIN}
      AND r.returnOnAssetsTTM > {ROA_MIN}
      AND r.currentRatioTTM > {CR_MIN}
      AND r.debtToEquityRatioTTM >= 0
      AND r.debtToEquityRatioTTM < {DE_MAX}
      AND r.dividendPayoutRatioTTM > 0
      AND r.dividendPayoutRatioTTM < {PAYOUT_MAX}
      AND k.marketCap > {mktcap_threshold}
      {exchange_where}
    ORDER BY k.dividendYieldTTM DESC
    LIMIT {args.limit}
    """

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if args.cloud:
        code = f"""
from cr_client import CetaResearch
cr = CetaResearch()
results = cr.query('''{sql}''', verbose=True)
print(f"Found {{len(results)}} qualifying stocks")
header = f"{{'Symbol':<8} {{'Yield':>8} {{'Payout':>8} {{'D/E':>6} {{'ROA':>7} {{'CR':>6} {{'MktCap':>14}}"
print(header)
print("-" * 60)
for r in results:
    print(f"{{r['symbol']:<8}} {{r['dividend_yield']*100:>7.2f}}% {{r['payout_ratio']*100:>7.1f}}% "
          f"{{r['debt_to_equity']:>5.2f}} {{r['roa']*100:>6.2f}}% {{r['current_ratio']:>5.2f}} "
          f"{{r['marketCap']/1e9:>13.1f}}B")
"""
        result = cr.execute_code(code, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    print(f"High Yield + Quality Screen: {universe_name}")
    print(f"Signal: DivYield > {DIVIDEND_YIELD_MIN*100:.0f}%, ROA > {ROA_MIN*100:.0f}%, "
          f"CR > {CR_MIN}, D/E < {DE_MAX}, Payout < {PAYOUT_MAX*100:.0f}%")
    print()

    results = cr.query(sql, verbose=args.verbose, memory_mb=4096, threads=2)

    if not results:
        print("No qualifying stocks found.")
        return

    print(f"Found {len(results)} qualifying stocks\n")
    print(f"{'Symbol':<8} {'Yield':>8} {'Payout':>8} {'D/E':>6} {'ROA':>7} {'CR':>6} {'MktCap':>14}")
    print("-" * 60)
    for r in results:
        print(f"{r['symbol']:<8} {r['dividend_yield']*100:>7.2f}% {r['payout_ratio']*100:>7.1f}% "
              f"{r['debt_to_equity']:>5.2f} {r['roa']*100:>6.2f}% {r['current_ratio']:>5.2f} "
              f"{r['marketCap']/1e9:>13.1f}B")


if __name__ == "__main__":
    main()
