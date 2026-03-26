#!/usr/bin/env python3
"""
Dividend Growth Screen - Current qualifying stocks

Screens for companies with 5+ consecutive years of annual dividend increases,
plus quality filters (payout < 80%, FCF > 0, market cap > threshold).

Usage:
    python3 dividend-growth/screen.py
    python3 dividend-growth/screen.py --preset india
    python3 dividend-growth/screen.py --exchange XETRA
    python3 dividend-growth/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

MIN_STREAK = 5


def run_screen(cr, exchanges, universe_name, verbose=False):
    """Screen for current dividend growth stocks."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_join = f"JOIN profile p ON s.symbol = p.symbol AND p.exchange IN ({ex_filter})"
    else:
        exchange_join = ""

    mktcap_threshold = get_mktcap_threshold(exchanges)

    sql = f"""
    WITH annual_div AS (
        SELECT symbol,
            EXTRACT(YEAR FROM CAST(date AS DATE)) AS yr,
            SUM(adjDividend) AS total_div
        FROM dividend_calendar
        WHERE adjDividend > 0
            {"AND symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN (" + ex_filter + "))" if exchanges else ""}
        GROUP BY symbol, EXTRACT(YEAR FROM CAST(date AS DATE))
    ),
    growth AS (
        SELECT symbol, yr, total_div,
            LAG(total_div) OVER (PARTITION BY symbol ORDER BY yr) AS prev_div
        FROM annual_div
    ),
    last_break AS (
        SELECT symbol, MAX(yr) AS break_yr
        FROM growth
        WHERE prev_div IS NOT NULL AND total_div <= prev_div
        GROUP BY symbol
    ),
    streak AS (
        SELECT g.symbol, COUNT(*) AS consecutive_years,
            MIN(g.yr) AS streak_from,
            MAX(g.yr) AS streak_to
        FROM growth g
        LEFT JOIN last_break lb ON g.symbol = lb.symbol
        WHERE g.prev_div IS NOT NULL
          AND g.total_div > g.prev_div
          AND (lb.break_yr IS NULL OR g.yr > lb.break_yr)
        GROUP BY g.symbol
        HAVING COUNT(*) >= {MIN_STREAK}
    )
    SELECT s.symbol,
        s.consecutive_years,
        s.streak_from,
        s.streak_to,
        ROUND(r.dividendPayoutRatio * 100, 1) AS payout_pct,
        ROUND(k.marketCap / 1e9, 2) AS mktcap_bn,
        ROUND(c.freeCashFlow / 1e6, 0) AS fcf_mm
    FROM streak s
    {exchange_join}
    JOIN financial_ratios_ttm r ON s.symbol = r.symbol
    JOIN key_metrics_ttm k ON s.symbol = k.symbol
    JOIN cash_flow_statement_ttm c ON s.symbol = c.symbol
    WHERE r.dividendPayoutRatio BETWEEN 0 AND 0.80
      AND c.freeCashFlow > 0
      AND k.marketCap > {mktcap_threshold}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY s.consecutive_years DESC) = 1
    ORDER BY s.consecutive_years DESC, k.marketCap DESC
    LIMIT 50
    """

    results = cr.query(sql, verbose=verbose, timeout=120)

    if not results:
        print(f"No qualifying stocks for {universe_name}")
        return

    print(f"\nDividend Growth Screen: {universe_name}")
    print(f"Signal: {MIN_STREAK}+ consecutive years of dividend increases, Payout < 80%, FCF > 0")
    print(f"{'='*85}")
    print(f"{'Symbol':<10} {'Streak':>6} {'From':>6} {'To':>6} {'Payout%':>8} {'MCap($B)':>9} {'FCF($M)':>8}")
    print("-" * 85)
    for r in results:
        print(f"{r['symbol']:<10} {r['consecutive_years']:>6} {int(r['streak_from']):>6} "
              f"{int(r['streak_to']):>6} {r['payout_pct']:>7.1f}% "
              f"{r['mktcap_bn']:>9.2f} {r['fcf_mm']:>8.0f}")
    print(f"\n{len(results)} stocks found")


def main():
    parser = argparse.ArgumentParser(description="Dividend Growth screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("dividend-growth", script="screen.py",
                                    args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, universe_name, verbose=args.verbose)


if __name__ == "__main__":
    main()
