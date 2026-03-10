#!/usr/bin/env python3
"""
Sector Momentum Rotation — Current Screen

Shows current sector rankings by trailing 12-month return and the stocks
currently in the best-performing sectors. Run this to see what the strategy
would buy today.

Usage:
    python3 sector-momentum/screen.py                    # US (NYSE+NASDAQ+AMEX)
    python3 sector-momentum/screen.py --preset india     # India
    python3 sector-momentum/screen.py --n-best 3         # Top 3 sectors

Output:
    - Sector ranking table (best to worst by 12-month return)
    - Stocks in the selected top N sectors
    - Shareable query links for Ceta Research data explorer
"""

import argparse
import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

N_BEST_SECTORS = 2
MIN_SECTOR_STOCKS = 5


def run_screen(cr, exchanges, universe_name, n_best=N_BEST_SECTORS, verbose=False):
    """Run the current sector screen and print results."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
        mktcap_min = get_mktcap_threshold(exchanges)
    else:
        exchange_clause = ""
        mktcap_min = 1_000_000_000

    print(f"\n{'=' * 60}")
    print(f"  SECTOR MOMENTUM ROTATION — CURRENT SCREEN")
    print(f"  Universe: {universe_name}")
    print(f"  Date: {date.today().isoformat()}")
    print(f"  Signal: Top {n_best} sectors by 12-month trailing return")
    print(f"  Market cap filter: >{mktcap_min:,.0f} local currency")
    print(f"{'=' * 60}\n")

    # Query: sector rankings (best to worst)
    sector_sql = f"""
WITH prices AS (
    SELECT e.symbol, e.adjClose, CAST(e.date AS DATE) AS trade_date
    FROM stock_eod e
    JOIN profile p ON e.symbol = p.symbol
    WHERE p.sector IS NOT NULL AND p.sector != ''
      AND p.marketCap > {mktcap_min}
      {exchange_clause}
      AND CAST(e.date AS DATE) >= CURRENT_DATE - INTERVAL '400' DAY
      AND e.adjClose IS NOT NULL AND e.adjClose > 0
),
recent AS (
    SELECT symbol, adjClose AS recent_price
    FROM prices
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) = 1
),
year_ago AS (
    SELECT symbol, adjClose AS old_price
    FROM prices
    WHERE trade_date <= CURRENT_DATE - INTERVAL '252' DAY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) = 1
),
stock_returns AS (
    SELECT r.symbol, pr.sector,
           (r.recent_price / ya.old_price - 1) * 100 AS return_12m
    FROM recent r
    JOIN year_ago ya ON r.symbol = ya.symbol
    JOIN profile pr ON r.symbol = pr.symbol
    WHERE ya.old_price > 0 AND r.recent_price > 0
      AND (r.recent_price / ya.old_price - 1) BETWEEN -0.99 AND 5.0
)
SELECT
    pr.sector,
    ROUND(AVG(sr.return_12m), 2) AS avg_return_12m_pct,
    COUNT(DISTINCT sr.symbol) AS n_stocks,
    ROW_NUMBER() OVER (ORDER BY AVG(sr.return_12m) DESC) AS rank_best
FROM stock_returns sr
JOIN profile pr ON sr.symbol = pr.symbol
GROUP BY pr.sector
HAVING COUNT(DISTINCT sr.symbol) >= {MIN_SECTOR_STOCKS}
ORDER BY avg_return_12m_pct DESC
    """

    print("Fetching sector rankings...")
    sector_rows = cr.query(sector_sql, verbose=verbose)

    if not sector_rows:
        print("No data returned.")
        return

    print(f"\n{'Rank':<6} {'Sector':<30} {'12M Return':>12} {'# Stocks':>10}")
    print("-" * 62)
    best_sectors = []
    for row in sector_rows:
        rank = row.get("rank_best", "?")
        sector = row.get("sector", "?")
        ret = row.get("avg_return_12m_pct", 0)
        n = row.get("n_stocks", 0)
        marker = " <<" if rank <= n_best else ""
        print(f"{rank:<6} {sector:<30} {ret:>+11.1f}% {n:>10}{marker}")
        if rank <= n_best:
            best_sectors.append(sector)

    if not best_sectors:
        print("\nNot enough sectors qualify.")
        return

    print(f"\n  Selected (top {n_best}): {', '.join(best_sectors)}")

    # Query: stocks in best sectors
    sectors_str = ", ".join(f"'{s}'" for s in best_sectors)
    stocks_sql = f"""
WITH prices AS (
    SELECT e.symbol, e.adjClose, CAST(e.date AS DATE) AS trade_date
    FROM stock_eod e
    JOIN profile p ON e.symbol = p.symbol
    WHERE p.sector IN ({sectors_str})
      AND p.marketCap > {mktcap_min}
      {exchange_clause}
      AND CAST(e.date AS DATE) >= CURRENT_DATE - INTERVAL '400' DAY
      AND e.adjClose IS NOT NULL AND e.adjClose > 0
),
recent AS (
    SELECT symbol, adjClose AS recent_price
    FROM prices
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) = 1
),
year_ago AS (
    SELECT symbol, adjClose AS old_price
    FROM prices
    WHERE trade_date <= CURRENT_DATE - INTERVAL '252' DAY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) = 1
)
SELECT
    pr.symbol,
    pr.companyName AS company,
    pr.sector,
    ROUND((r.recent_price / ya.old_price - 1) * 100, 2) AS return_12m_pct,
    ROUND(r.recent_price, 2) AS current_price,
    ROUND(pr.marketCap / 1e9, 2) AS mktcap_bn_local
FROM recent r
JOIN year_ago ya ON r.symbol = ya.symbol
JOIN profile pr ON r.symbol = pr.symbol
WHERE ya.old_price > 0 AND r.recent_price > 0
  AND (r.recent_price / ya.old_price - 1) BETWEEN -0.99 AND 5.0
ORDER BY pr.sector ASC, return_12m_pct DESC
LIMIT 100
    """

    print("\n\nFetching stocks in selected sectors...")
    stock_rows = cr.query(stocks_sql, verbose=verbose)

    if not stock_rows:
        print("No stocks found.")
        return

    print(f"\n{'Symbol':<10} {'Company':<35} {'Sector':<25} {'12M Ret':>9} {'Price':>8} {'MCap':>10}")
    print("-" * 100)
    for row in stock_rows:
        sym = row.get("symbol", "?")
        company = (row.get("company") or "")[:33]
        sector = (row.get("sector") or "")[:23]
        ret = row.get("return_12m_pct", 0)
        price = row.get("current_price", 0)
        mcap = row.get("mktcap_bn_local", 0)
        print(f"{sym:<10} {company:<35} {sector:<25} {ret:>+8.1f}% {price:>8.2f} {mcap:>8.1f}B")

    print(f"\n  Total stocks: {len(stock_rows)}")
    print(f"\n  Data: Ceta Research (FMP warehouse, {date.today().isoformat()})")
    print(f"  Note: Strategy buys equal-weight exposure to ALL stocks above.")


def main():
    parser = argparse.ArgumentParser(description="Sector Momentum Rotation current screen")
    add_common_args(parser)
    parser.add_argument("--n-best", type=int, default=N_BEST_SECTORS,
                        help=f"Number of best sectors to show (default {N_BEST_SECTORS})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("sector-momentum/screen",
                                    args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, universe_name, n_best=args.n_best, verbose=args.verbose)


if __name__ == "__main__":
    main()
