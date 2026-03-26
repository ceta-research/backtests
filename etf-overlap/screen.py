#!/usr/bin/env python3
"""
ETF Overlap Screen

Check overlap between ETFs in your portfolio. Accepts any number of ETF
tickers and shows pairwise overlaps plus shared holdings.

Usage:
    # Two ETFs
    python3 etf-overlap/screen.py SPY QQQ

    # Your portfolio
    python3 etf-overlap/screen.py SPY QQQ VTI VXUS BND

    # With details (show shared holdings)
    python3 etf-overlap/screen.py SPY QQQ --details

    # Cloud execution
    python3 etf-overlap/screen.py SPY QQQ --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch


def main():
    parser = argparse.ArgumentParser(description="Check ETF overlap")
    parser.add_argument("etfs", nargs="+", help="ETF tickers to compare")
    parser.add_argument("--details", "-d", action="store_true",
                        help="Show shared holdings for each pair")
    parser.add_argument("--top", type=int, default=20,
                        help="Number of shared holdings to show (default: 20)")
    parser.add_argument("--api-key", type=str, help="API key")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("etf-overlap", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    etfs = [e.upper() for e in args.etfs]
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Fetch holdings
    etf_filter = ",".join(f"'{e}'" for e in etfs)
    data = cr.query(f"""
        SELECT symbol, asset, weightPercentage
        FROM etf_holder
        WHERE symbol IN ({etf_filter})
          AND asset IS NOT NULL AND asset != ''
          AND asset NOT LIKE '%.NE'
        ORDER BY symbol, weightPercentage DESC
    """, memory_mb=4096, threads=2)

    holdings = {}
    for row in data:
        sym = row['symbol']
        if sym not in holdings:
            holdings[sym] = {}
        holdings[sym][row['asset']] = row.get('weightPercentage', 0)

    missing = [e for e in etfs if e not in holdings]
    if missing:
        print(f"Not found: {', '.join(missing)}")

    found = [e for e in etfs if e in holdings]
    if len(found) < 2:
        print("Need at least 2 ETFs to compare.")
        return

    # Print holdings summary
    print(f"\n{'ETF':<8} {'Holdings':>10}")
    print("-" * 20)
    for e in found:
        print(f"{e:<8} {len(holdings[e]):>10}")

    # Pairwise overlaps
    print(f"\n{'ETF A':<8} {'ETF B':<8} {'Shared':>8} {'Overlap%':>10}")
    print("-" * 38)
    for i, a in enumerate(found):
        for b in found[i+1:]:
            shared = set(holdings[a].keys()) & set(holdings[b].keys())
            smaller = min(len(holdings[a]), len(holdings[b]))
            pct = round(len(shared) * 100 / smaller, 1) if smaller > 0 else 0
            print(f"{a:<8} {b:<8} {len(shared):>8} {pct:>9.1f}%")

            if args.details:
                # Show top shared holdings by combined weight
                shared_list = []
                for stock in shared:
                    wa = holdings[a].get(stock, 0)
                    wb = holdings[b].get(stock, 0)
                    shared_list.append((stock, wa, wb))
                shared_list.sort(key=lambda x: x[1] + x[2], reverse=True)

                print(f"  {'Stock':<10} {'in ' + a:>10} {'in ' + b:>10}")
                print(f"  {'-' * 32}")
                for stock, wa, wb in shared_list[:args.top]:
                    print(f"  {stock:<10} {wa:>9.2f}% {wb:>9.2f}%")
                print()

    # Portfolio summary
    if len(found) > 2:
        all_stocks = set()
        for e in found:
            all_stocks.update(holdings[e].keys())
        stock_counts = {}
        for stock in all_stocks:
            stock_counts[stock] = sum(1 for e in found if stock in holdings[e])
        unique = len(all_stocks)
        redundant = sum(1 for c in stock_counts.values() if c > 1)
        print(f"\nPortfolio summary ({len(found)} ETFs):")
        print(f"  Unique stocks: {unique}")
        print(f"  Redundant (in 2+ ETFs): {redundant} ({round(redundant*100/unique, 1) if unique else 0}%)")
        max_count = max(stock_counts.values())
        most_held = [s for s, c in stock_counts.items() if c == max_count]
        print(f"  Most duplicated ({max_count} ETFs): {', '.join(most_held[:10])}")


if __name__ == "__main__":
    main()
