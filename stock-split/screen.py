#!/usr/bin/env python3
"""
Stock Split Live Screen

Finds recent forward stock splits using real-time data.
Optional: filter by market cap and minimum split ratio.

Usage:
    python3 stock-split/screen.py                         # splits in last 90 days
    python3 stock-split/screen.py --days 180              # last 6 months
    python3 stock-split/screen.py --min-mktcap 1000000000 # $1B+ only
    python3 stock-split/screen.py --min-ratio 2.0         # 2-for-1 and above
    python3 stock-split/screen.py --cloud                 # run on cloud compute

Data source: Ceta Research SQL API (FMP financial data warehouse)
Requires: CR_API_KEY environment variable (get key at cetaresearch.com)
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch, ExecutionError
from cloud_runner import run_screen_on_cloud


SIMPLE_SCREEN_SQL = """
SELECT
    symbol,
    CAST(date AS DATE) AS split_date,
    numerator,
    denominator,
    ROUND(CAST(numerator AS FLOAT) / denominator, 1) AS split_ratio
FROM splits_calendar
WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
  AND numerator IS NOT NULL
  AND denominator IS NOT NULL
  AND denominator > 0
  AND numerator > denominator
ORDER BY split_date DESC
"""

ADVANCED_SCREEN_SQL = """
WITH splits AS (
    SELECT symbol,
           CAST(date AS DATE) AS split_date,
           numerator, denominator,
           ROUND(CAST(numerator AS FLOAT) / denominator, 1) AS split_ratio
    FROM splits_calendar
    WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
      AND numerator IS NOT NULL AND denominator IS NOT NULL AND denominator > 0
      AND numerator > denominator
      AND CAST(numerator AS FLOAT) / denominator >= {min_ratio}
)
SELECT s.symbol, s.split_date,
       CONCAT(CAST(s.numerator AS VARCHAR), '-for-', CAST(s.denominator AS VARCHAR)) AS split_desc,
       s.split_ratio,
       ROUND(k.marketCap / 1e9, 2) AS mktcap_bn
FROM splits s
JOIN key_metrics k ON s.symbol = k.symbol AND k.period = 'FY'
WHERE k.marketCap > {min_mktcap}
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY k.date DESC) = 1
ORDER BY s.split_date DESC, s.split_ratio DESC
"""


def run_screen(args):
    client = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if args.min_mktcap or args.min_ratio > 1.0:
        sql = ADVANCED_SCREEN_SQL.format(
            days=args.days,
            min_ratio=args.min_ratio,
            min_mktcap=args.min_mktcap or 0,
        )
        print(f"Running advanced screen: last {args.days} days, "
              f"ratio >= {args.min_ratio}x, mktcap >= ${(args.min_mktcap or 0)/1e6:.0f}M")
    else:
        sql = SIMPLE_SCREEN_SQL.format(days=args.days)
        print(f"Running simple screen: forward splits in last {args.days} days")

    results = client.query(sql, format="json", timeout=120, verbose=args.verbose)

    if not results:
        print("No results returned.")
        return

    # Print table
    if args.min_mktcap:
        headers = ["symbol", "split_date", "split_desc", "split_ratio", "mktcap_bn"]
        print(f"\n{'Symbol':<8}  {'Split Date':<12}  {'Split':<10}  {'Ratio':<6}  {'Mktcap ($B)'}")
        print(f"{'-'*8}  {'-'*12}  {'-'*10}  {'-'*6}  {'-'*12}")
        for r in results:
            print(f"{r['symbol']:<8}  {str(r['split_date']):<12}  "
                  f"{r.get('split_desc', ''):<10}  {r['split_ratio']:<6.1f}  "
                  f"${r.get('mktcap_bn', 'N/A')}")
    else:
        print(f"\n{'Symbol':<8}  {'Split Date':<12}  {'Numerator':<10}  {'Denominator':<12}  {'Ratio'}")
        print(f"{'-'*8}  {'-'*12}  {'-'*10}  {'-'*12}  {'-'*6}")
        for r in results:
            print(f"{r['symbol']:<8}  {str(r['split_date']):<12}  "
                  f"{r['numerator']:<10}  {r['denominator']:<12}  {r['split_ratio']:.1f}x")

    print(f"\n{len(results)} split event(s) found")
    print("Data: Ceta Research (FMP splits data)")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to {args.output}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Stock Split Live Screen")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--days", type=int, default=90,
                        help="Look-back window in calendar days (default: 90)")
    parser.add_argument("--min-mktcap", type=float, default=None,
                        help="Minimum market cap in USD (e.g. 1000000000 for $1B)")
    parser.add_argument("--min-ratio", type=float, default=1.0,
                        help="Minimum split ratio (default: 1.0, no filter)")
    parser.add_argument("--output", type=str, help="Save results to JSON file")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        run_screen_on_cloud(
            script_path=__file__,
            args=[f"--days={args.days}"] +
                 ([f"--min-mktcap={args.min_mktcap}"] if args.min_mktcap else []) +
                 [f"--min-ratio={args.min_ratio}"],
            api_key=args.api_key,
        )
    else:
        run_screen(args)


if __name__ == "__main__":
    main()
