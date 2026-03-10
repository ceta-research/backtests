#!/usr/bin/env python3
"""
Graham Number Timing Screen (Current Data)

Screen for stocks currently trading below their Graham Number.

Graham Number = sqrt(22.5 × EPS × BVPS)
Signal: Price < Graham Number (ratio < 1.0)
Quality filters: ROE > 10%, positive earnings, positive equity

Usage:
    # Screen US stocks
    python3 graham-timing/screen.py

    # Screen Indian stocks
    python3 graham-timing/screen.py --exchange BSE,NSE

    # Top 50 results
    python3 graham-timing/screen.py --limit 50

    # Cloud execution
    python3 graham-timing/screen.py --cloud
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

ROE_MIN = 0.10
DEFAULT_LIMIT = 30


def screen(exchanges, limit=DEFAULT_LIMIT, verbose=False):
    """Screen for stocks trading below Graham Number using TTM data."""

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    mktcap_min = get_mktcap_threshold(exchanges) if exchanges else 1000000000

    sql = f"""
    WITH latest_filings AS (
        -- Get most recent FY filing per symbol
        SELECT
            symbol,
            netIncome,
            totalStockholdersEquity,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) as rn
        FROM (
            SELECT i.symbol, i.netIncome, b.totalStockholdersEquity, i.dateEpoch
            FROM income_statement i
            JOIN balance_sheet b ON i.symbol = b.symbol AND i.dateEpoch = b.dateEpoch
            WHERE i.period = 'FY'
              AND b.period = 'FY'
              AND i.netIncome > 0
              AND b.totalStockholdersEquity > 0
        ) combined
    ),
    current_data AS (
        SELECT
            p.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            f.netIncome,
            f.totalStockholdersEquity,
            k.marketCap,
            k.returnOnEquity as roe,
            s.adjClose as price
        FROM profile p
        JOIN latest_filings f ON p.symbol = f.symbol
        JOIN key_metrics k ON p.symbol = k.symbol
        JOIN (
            SELECT symbol, adjClose,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) as rn
            FROM stock_eod
            WHERE dateEpoch >= CAST(EXTRACT(EPOCH FROM CURRENT_DATE - INTERVAL '10 days') AS BIGINT)
        ) s ON p.symbol = s.symbol
        WHERE f.rn = 1
          AND s.rn = 1
          AND k.period = 'FY'
          AND k.returnOnEquity > {ROE_MIN}
          AND k.marketCap > {mktcap_min}
          AND s.adjClose > 0
          {exchange_where}
    ),
    graham_calc AS (
        SELECT
            symbol,
            companyName,
            exchange,
            sector,
            roe,
            marketCap,
            price,
            -- Compute shares outstanding from market cap and price
            marketCap / price as shares,
            -- Compute EPS and BVPS
            netIncome / (marketCap / price) as eps,
            totalStockholdersEquity / (marketCap / price) as bvps
        FROM current_data
        WHERE marketCap / price > 0
    ),
    graham_screen AS (
        SELECT
            symbol,
            companyName,
            exchange,
            sector,
            roe,
            marketCap,
            price,
            eps,
            bvps,
            -- Graham Number = sqrt(22.5 × EPS × BVPS)
            POWER(22.5 * eps * bvps, 0.5) as graham_number
        FROM graham_calc
        WHERE eps > 0 AND bvps > 0
    )
    SELECT
        symbol,
        companyName,
        exchange,
        sector,
        ROUND(roe * 100, 1) as roe_pct,
        ROUND(marketCap / 1000000, 0) as mcap_millions,
        ROUND(price, 2) as price,
        ROUND(graham_number, 2) as graham_number,
        ROUND(price / graham_number, 3) as price_to_graham,
        ROUND((1 - price / graham_number) * 100, 1) as discount_pct
    FROM graham_screen
    WHERE price < graham_number  -- Only undervalued stocks
    ORDER BY price_to_graham ASC
    LIMIT {limit}
    """

    client = CetaResearch()
    print(f"\nScreening for stocks below Graham Number...")
    print(f"Exchanges: {', '.join(exchanges) if exchanges else 'All'}")
    print(f"Market cap threshold: {mktcap_min:,.0f} (local currency)")
    print(f"ROE threshold: {ROE_MIN*100:.0f}%")
    print(f"Limit: {limit}\n")

    results = client.query(sql, format='json', verbose=verbose,
                           memory_mb=8192, threads=4)

    if not results or len(results) == 0:
        print("No stocks found matching criteria.")
        return

    print(f"Found {len(results)} stocks trading below Graham Number:\n")
    print(f"{'Symbol':<10} {'Company':<30} {'Ex':<6} {'Sector':<20} {'ROE%':>6} {'MCap':>8} {'Price':>8} {'Graham':>8} {'P/G':>6} {'Disc%':>6}")
    print("-" * 120)

    for r in results:
        print(f"{r['symbol']:<10} {r['companyName'][:29]:<30} {r['exchange']:<6} {r['sector'][:19]:<20} "
              f"{r['roe_pct']:>6.1f} {r['mcap_millions']:>8,.0f}M {r['price']:>8.2f} {r['graham_number']:>8.2f} "
              f"{r['price_to_graham']:>6.3f} {r['discount_pct']:>6.1f}%")

    print(f"\n{'='*120}")
    print(f"Graham Number = sqrt(22.5 × EPS × BVPS)")
    print(f"P/G < 1.0 means stock trades below Graham's fair value estimate")
    print(f"Discount% = (1 - Price/Graham) × 100")
    print(f"{'='*120}\n")


def main():
    parser = argparse.ArgumentParser(description="Graham Number Timing Screen")
    add_common_args(parser)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help="Max stocks to return (default: 30)")
    args = parser.parse_args()

    exchanges, _ = resolve_exchanges(args)
    screen(exchanges, limit=args.limit, verbose=args.verbose)


if __name__ == "__main__":
    main()
