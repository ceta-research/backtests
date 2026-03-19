#!/usr/bin/env python3
"""
Sustained ROIC Current Stock Screen

Finds stocks currently qualifying as "sustained ROIC" (ROIC > 12%
in 3+ of last 5 fiscal years).

Usage:
    python3 sustained-roic/screen.py
    python3 sustained-roic/screen.py --preset india
    python3 sustained-roic/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import (add_common_args, resolve_exchanges,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

ROIC_THRESHOLD = 0.12
SUSTAINED_MIN_YEARS = 3
LOOKBACK_YEARS = 5


def build_screen_sql(exchanges, mktcap_min):
    """Build SQL for current sustained ROIC screen."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
    WITH yearly_roic AS (
        SELECT
            ic.symbol,
            ic.fiscalYear,
            ic.operatingIncome * (1.0 - CASE
                WHEN ic.incomeTaxExpense > 0 AND ic.incomeBeforeTax > 0
                THEN CAST(ic.incomeTaxExpense AS DOUBLE) / ic.incomeBeforeTax
                ELSE 0.25
            END) AS nopat,
            bs.totalAssets - bs.totalCurrentLiabilities
                - COALESCE(bs.cashAndCashEquivalents, 0) AS invested_capital
        FROM income_statement ic
        JOIN balance_sheet bs ON ic.symbol = bs.symbol
            AND ic.fiscalYear = bs.fiscalYear
            AND ic.period = bs.period
        WHERE ic.period = 'FY'
            AND ic.fiscalYear BETWEEN YEAR(CURRENT_DATE) - {LOOKBACK_YEARS}
                AND YEAR(CURRENT_DATE) - 1
            AND ic.operatingIncome IS NOT NULL
            AND ic.incomeBeforeTax IS NOT NULL
            AND bs.totalAssets IS NOT NULL
            AND bs.totalCurrentLiabilities IS NOT NULL
    ),
    roic_calc AS (
        SELECT
            symbol,
            fiscalYear,
            CASE WHEN invested_capital > 0
                THEN nopat / invested_capital
                ELSE NULL
            END AS roic
        FROM yearly_roic
        WHERE invested_capital > 0
    ),
    roic_summary AS (
        SELECT
            symbol,
            COUNT(*) AS years_with_data,
            SUM(CASE WHEN roic > {ROIC_THRESHOLD} THEN 1 ELSE 0 END) AS years_above,
            ROUND(AVG(roic) * 100, 1) AS avg_roic_pct,
            ROUND(MAX(CASE WHEN fiscalYear = (
                SELECT MAX(fiscalYear) FROM roic_calc rc2
                WHERE rc2.symbol = roic_calc.symbol
            ) THEN roic ELSE NULL END) * 100, 1) AS latest_roic_pct
        FROM roic_calc
        WHERE roic IS NOT NULL
        GROUP BY symbol
        HAVING COUNT(*) >= 3
            AND SUM(CASE WHEN roic > {ROIC_THRESHOLD} THEN 1 ELSE 0 END) >= {SUSTAINED_MIN_YEARS}
    )
    SELECT
        rs.symbol,
        p.companyName,
        p.sector,
        p.exchange,
        ROUND(p.marketCap / 1e9, 2) AS marketCap_B,
        rs.years_above AS yrs_above_12pct,
        rs.years_with_data,
        rs.avg_roic_pct,
        rs.latest_roic_pct
    FROM roic_summary rs
    JOIN profile p ON rs.symbol = p.symbol
    WHERE p.isActivelyTrading = true
        AND p.marketCap > {mktcap_min}
        {exchange_clause}
    ORDER BY rs.avg_roic_pct DESC
    LIMIT 100
    """


def main():
    parser = argparse.ArgumentParser(
        description="Sustained ROIC current stock screen"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(
        args,
        default_exchanges=["NYSE", "NASDAQ", "AMEX"],
        default_name="US_MAJOR"
    )
    mktcap_threshold = get_mktcap_threshold(exchanges)

    print(f"Sustained ROIC Screen: {universe_name}")
    print(f"Signal: ROIC > {ROIC_THRESHOLD*100:.0f}% in "
          f"{SUSTAINED_MIN_YEARS}+/{LOOKBACK_YEARS} FY")
    print(f"Market cap min: {mktcap_threshold:,.0f}")
    print()

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if args.cloud:
        sql = build_screen_sql(exchanges, mktcap_threshold)
        result = cr.execute_code(f"""
import duckdb
con = duckdb.connect()
result = con.execute('''{sql}''').fetchdf()
print(result.to_string(index=False))
print(f"\\nTotal: {{len(result)}} stocks")
""")
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
    else:
        sql = build_screen_sql(exchanges, mktcap_threshold)
        results = cr.query(sql, verbose=args.verbose, timeout=120)

        if not results:
            print("No qualifying stocks found.")
            return

        print(f"{'Symbol':<10} {'Company':<30} {'Sector':<25} "
              f"{'MCap($B)':>8} {'Yrs>12%':>8} {'AvgROIC':>8} {'LatestROIC':>10}")
        print("-" * 105)

        for r in results:
            print(f"{r['symbol']:<10} "
                  f"{r['companyName'][:28]:<30} "
                  f"{r.get('sector', 'N/A')[:23]:<25} "
                  f"{r['marketCap_B']:>8.1f} "
                  f"{r['yrs_above_12pct']:>8} "
                  f"{r['avg_roic_pct']:>7.1f}% "
                  f"{r['latest_roic_pct']:>9.1f}%")

        print(f"\nTotal: {len(results)} stocks qualifying as sustained ROIC")


if __name__ == "__main__":
    main()
