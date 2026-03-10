#!/usr/bin/env python3
"""
Cyclical Sector Timing - Current Screen

Runs the strategy signal on current data to identify:
1. Whether the expansion signal is active (majority of cyclicals growing revenue)
2. Which stocks qualify under the current screen

Usage:
    python3 cyclical-timing/screen.py                    # US default
    python3 cyclical-timing/screen.py --preset india     # India
    python3 cyclical-timing/screen.py --cloud            # Cloud execution
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

EXPANSION_THRESHOLD = 0.50
ROE_MIN = 0.05
TOP_N = 30


def run_screen(api_key=None, base_url=None, exchanges=None, universe_name="US_MAJOR",
               verbose=False):
    """Run current cyclical timing screen. Returns (signal_dict, stocks_list)."""
    cr = CetaResearch(api_key=api_key, base_url=base_url)
    mktcap_min = get_mktcap_threshold(exchanges)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        ex_where = f"AND p.exchange IN ({ex_filter})"
    else:
        ex_where = ""

    # Step 1: Expansion signal
    signal_sql = f"""
        WITH is_current AS (
            SELECT symbol, revenue AS rev_current, dateEpoch,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
        ),
        is_prior AS (
            SELECT symbol, revenue AS rev_prior, dateEpoch AS de_prior,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
              AND dateEpoch < (
                SELECT MAX(dateEpoch) - 86400*300
                FROM income_statement
                WHERE symbol = is_current.symbol AND period = 'FY'
              )
        ),
        growth AS (
            SELECT c.symbol,
                   (c.rev_current - p.rev_prior) / p.rev_prior AS rev_growth
            FROM is_current c
            JOIN is_prior p ON c.symbol = p.symbol AND p.rn = 1
            WHERE c.rn = 1
              AND (c.rev_current - p.rev_prior) / p.rev_prior BETWEEN -0.99 AND 10.0
        )
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN g.rev_growth > 0 THEN 1 ELSE 0 END) AS n_growing,
            AVG(g.rev_growth) AS avg_growth,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY g.rev_growth) AS median_growth
        FROM growth g
        JOIN profile p ON g.symbol = p.symbol
        WHERE p.sector IN ('Basic Materials', 'Industrials', 'Energy', 'Consumer Cyclical')
          AND p.marketCap > {mktcap_min}
          {ex_where}
    """

    print(f"\n--- Cyclical Expansion Signal ({universe_name}) ---")
    signal_result = cr.query(signal_sql, verbose=verbose)

    if not signal_result or not signal_result[0].get('n_total'):
        print("No signal data available.")
        return None, []

    s = signal_result[0]
    n_total = s['n_total'] or 0
    n_growing = s['n_growing'] or 0
    expansion_ratio = n_growing / n_total if n_total > 0 else 0
    is_expanding = expansion_ratio >= EXPANSION_THRESHOLD

    print(f"Stocks with revenue data: {n_total}")
    print(f"Growing YoY: {n_growing} ({expansion_ratio:.1%})")
    print(f"Median growth: {(s.get('median_growth') or 0)*100:.1f}%")
    print(f"Signal: {'EXPANSION ACTIVE ✓' if is_expanding else 'CONTRACTION - CASH'}")

    if not is_expanding:
        return {
            "universe": universe_name,
            "expansion_ratio": round(expansion_ratio, 3),
            "n_stocks_measured": n_total,
            "signal": "CONTRACTION",
            "action": "HOLD CASH - cyclical revenue expansion below threshold"
        }, []

    # Step 2: Screen qualifying stocks
    screen_sql = f"""
        WITH is_current AS (
            SELECT symbol, revenue AS rev_current, dateEpoch,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
        ),
        is_prior AS (
            SELECT symbol, revenue AS rev_prior, dateEpoch AS de_prior,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
        ),
        km AS (
            SELECT symbol, returnOnEquity, marketCap,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM key_metrics
            WHERE period = 'FY' AND returnOnEquity IS NOT NULL
        )
        SELECT
            p.symbol,
            p.sector,
            p.exchange,
            ROUND((c.rev_current - pr.rev_prior) / pr.rev_prior * 100, 1) AS rev_growth_pct,
            ROUND(km.returnOnEquity * 100, 1) AS roe_pct,
            ROUND(p.marketCap / 1e9, 2) AS market_cap_b
        FROM is_current c
        JOIN is_prior pr ON c.symbol = pr.symbol AND pr.rn = 1
        JOIN km ON c.symbol = km.symbol AND km.rn = 1
        JOIN profile p ON c.symbol = p.symbol
        WHERE c.rn = 1
          AND c.dateEpoch != pr.de_prior
          AND (c.rev_current - pr.rev_prior) / pr.rev_prior > 0
          AND km.returnOnEquity >= {ROE_MIN}
          AND p.marketCap > {mktcap_min}
          AND p.sector IN ('Basic Materials', 'Industrials', 'Energy', 'Consumer Cyclical')
          {ex_where}
        ORDER BY km.returnOnEquity DESC
        LIMIT {TOP_N}
    """

    print(f"\n--- Top {TOP_N} Qualifying Cyclical Stocks (by ROE) ---")
    stocks = cr.query(screen_sql, verbose=verbose)

    if stocks:
        print(f"\n{'Symbol':<12} {'Sector':<22} {'RevGrowth':>10} {'ROE':>8} {'MCap($B)':>10}")
        print("-" * 68)
        for s in stocks:
            print(f"{s['symbol']:<12} {s['sector']:<22} "
                  f"{s.get('rev_growth_pct', 0) or 0:>9.1f}% "
                  f"{s.get('roe_pct', 0) or 0:>7.1f}% "
                  f"{s.get('market_cap_b', 0) or 0:>9.1f}B")
    else:
        print("No qualifying stocks found.")

    return {
        "universe": universe_name,
        "expansion_ratio": round(expansion_ratio, 3),
        "n_stocks_measured": n_total,
        "n_growing": n_growing,
        "signal": "EXPANSION",
        "n_qualifying": len(stocks),
        "action": f"INVEST - {len(stocks)} qualifying cyclical stocks"
    }, stocks


def main():
    parser = argparse.ArgumentParser(description="Cyclical Sector Timing screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        screen_code = open(__file__).read()
        result = cr.execute_code(screen_code, verbose=True)
        print(result.get("output", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    signal, stocks = run_screen(
        api_key=args.api_key,
        base_url=args.base_url,
        exchanges=exchanges,
        universe_name=universe_name,
        verbose=args.verbose
    )

    if signal:
        print(f"\n--- Summary ---")
        print(json.dumps(signal, indent=2))


if __name__ == "__main__":
    main()
