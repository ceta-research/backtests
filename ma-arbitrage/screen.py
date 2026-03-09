#!/usr/bin/env python3
"""
M&A Current Activity Screen

Shows recent M&A deal announcements from the last N days,
with available market cap data for context.

Data source: mergers_acquisitions_latest (FMP / SEC filings)
Note: Coverage is selective; not all M&A deals appear in this dataset.

Usage:
    python3 ma-arbitrage/screen.py                    # last 90 days
    python3 ma-arbitrage/screen.py --days 180         # last 180 days
    python3 ma-arbitrage/screen.py --min-mktcap 1e9   # large-cap only
    python3 ma-arbitrage/screen.py --cloud             # run on Ceta Research cloud
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch


def run_screen(client, days=90, min_mktcap=None, verbose=False):
    """Fetch recent M&A deals with optional market cap filter."""

    mktcap_clause = ""
    if min_mktcap:
        mktcap_clause = f"AND COALESCE(k.marketCap, 0) > {min_mktcap}"

    sql = f"""
WITH recent_deals AS (
    SELECT
        symbol AS acquirer,
        targetedSymbol AS target,
        companyName AS acquirer_name,
        targetedCompanyName AS target_name,
        CAST(transactionDate AS DATE) AS deal_date,
        ROW_NUMBER() OVER (
            PARTITION BY targetedSymbol, CAST(transactionDate AS DATE)
            ORDER BY acceptedDate DESC
        ) AS rn
    FROM mergers_acquisitions_latest
    WHERE CAST(transactionDate AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
      AND targetedSymbol IS NOT NULL
      AND TRIM(targetedSymbol) != ''
),
deduped AS (
    SELECT acquirer, target, acquirer_name, target_name, deal_date
    FROM recent_deals
    WHERE rn = 1
)
SELECT
    d.deal_date,
    d.acquirer,
    d.acquirer_name,
    d.target,
    d.target_name,
    ROUND(COALESCE(k.marketCap, 0) / 1e9, 2) AS target_mktcap_bn
FROM deduped d
LEFT JOIN key_metrics k ON d.target = k.symbol
    AND k.period = 'FY'
    AND CAST(k.date AS DATE) <= d.deal_date
    AND QUALIFY ROW_NUMBER() OVER (
        PARTITION BY d.target, d.deal_date
        ORDER BY k.date DESC
    ) = 1
WHERE 1=1
  {mktcap_clause}
ORDER BY d.deal_date DESC
LIMIT 50
"""

    # Simplify the query — the QUALIFY inside LEFT JOIN won't work
    # Use a CTE for market cap lookup instead
    sql = f"""
WITH recent_deals AS (
    SELECT
        symbol AS acquirer,
        targetedSymbol AS target,
        companyName AS acquirer_name,
        targetedCompanyName AS target_name,
        CAST(transactionDate AS DATE) AS deal_date,
        ROW_NUMBER() OVER (
            PARTITION BY targetedSymbol, CAST(transactionDate AS DATE)
            ORDER BY acceptedDate DESC
        ) AS rn
    FROM mergers_acquisitions_latest
    WHERE CAST(transactionDate AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
      AND targetedSymbol IS NOT NULL
      AND TRIM(targetedSymbol) != ''
),
deduped AS (
    SELECT acquirer, target, acquirer_name, target_name, deal_date
    FROM recent_deals
    WHERE rn = 1
),
latest_mcap AS (
    SELECT symbol, marketCap
    FROM key_metrics
    WHERE period = 'FY'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
)
SELECT
    d.deal_date,
    d.acquirer,
    LEFT(d.acquirer_name, 30) AS acquirer_name,
    d.target,
    LEFT(d.target_name, 35) AS target_name,
    ROUND(COALESCE(m.marketCap, 0) / 1e9, 2) AS target_mktcap_bn
FROM deduped d
LEFT JOIN latest_mcap m ON d.target = m.symbol
{"WHERE m.marketCap > " + str(min_mktcap) if min_mktcap else ""}
ORDER BY d.deal_date DESC
LIMIT 50
"""

    if verbose:
        print(f"Running screen: last {days} days")

    result = client.query(sql, format="json", timeout=120, limit=100,
                          memory_mb=4096, threads=2)

    if isinstance(result, dict) and "error" in result:
        print(f"Error: {result['error']}")
        return []

    return result if isinstance(result, list) else []


def main():
    parser = argparse.ArgumentParser(description="M&A Current Activity Screen")
    parser.add_argument("--api-key", default=os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY"))
    parser.add_argument("--base-url", default="https://api.cetaresearch.com/api/v1")
    parser.add_argument("--days", type=int, default=90, help="Days to look back (default: 90)")
    parser.add_argument("--min-mktcap", type=float, default=None,
                        help="Minimum target market cap (e.g. 1e9 for $1B)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if not args.api_key:
        print("Error: Set CR_API_KEY env var or use --api-key.")
        sys.exit(1)

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("ma-arbitrage/screen", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    deals = run_screen(cr, days=args.days, min_mktcap=args.min_mktcap, verbose=args.verbose)

    if not deals:
        print("No deals found.")
        return

    print(f"\nM&A Activity — Last {args.days} Days")
    print(f"{'Date':<12} {'Acquirer':<8} {'Acquirer Name':<32} {'Target':<8} {'Target Name':<37} {'MktCap ($B)':>12}")
    print("-" * 112)
    for d in deals:
        print(f"{str(d.get('deal_date','')):<12} "
              f"{str(d.get('acquirer','')):<8} "
              f"{str(d.get('acquirer_name','')):<32} "
              f"{str(d.get('target','')):<8} "
              f"{str(d.get('target_name','')):<37} "
              f"{d.get('target_mktcap_bn', 0):>12.2f}")
    print(f"\n{len(deals)} deals found")
    print(f"\nNote: Data from FMP mergers_acquisitions_latest (SEC filings).")
    print(f"Coverage is selective — not all M&A deals appear in this dataset.")


if __name__ == "__main__":
    main()
