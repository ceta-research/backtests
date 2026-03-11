#!/usr/bin/env python3
"""Earnings Growth Consistency screen — current qualifying stocks.

Uses TTM (trailing twelve months) data for quality filters plus the most
recent 4 FY income_statement records to compute the 3-year NI growth streak.

Filters (point-in-time FY data):
  - Net income grew YoY for 3 consecutive years (4 FY data points)
  - All 4 periods profitable (NI > 0)
  - ROE > 8% (quality floor)
  - Debt-to-Equity < 2.0
  - Market cap > exchange threshold

Ranking: Top 30 by ROE descending.

Usage:
    python3 earnings-consistency/screen.py                   # US (default)
    python3 earnings-consistency/screen.py --preset india
    python3 earnings-consistency/screen.py --exchange XETRA
    python3 earnings-consistency/screen.py --global
    python3 earnings-consistency/screen.py --preset us --csv
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

# TTM-based screen for live signal.
# The earnings streak is computed from income_statement FY rows (not TTM),
# because EPS growth requires annual comparison points.
SCREEN_SQL_TEMPLATE = """
WITH
-- Rank all FY netIncome filings per symbol (most recent first)
fy_income AS (
    SELECT symbol, netIncome, dateEpoch,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM income_statement
    WHERE period = 'FY'
      AND netIncome IS NOT NULL
      {exchange_income_filter}
),
-- Symbols with 3 consecutive years of net income growth, all profitable
streak AS (
    SELECT y1.symbol,
        ROUND(y1.netIncome / 1e6, 1) AS ni_yr0_m,
        ROUND(y2.netIncome / 1e6, 1) AS ni_yr1_m,
        ROUND(y3.netIncome / 1e6, 1) AS ni_yr2_m,
        ROUND(y4.netIncome / 1e6, 1) AS ni_yr3_m,
        ROUND((y1.netIncome - y4.netIncome) / NULLIF(y4.netIncome, 0) * 100, 1) AS ni_3yr_growth_pct
    FROM fy_income y1
    JOIN fy_income y2 ON y1.symbol = y2.symbol AND y2.rn = 2
    JOIN fy_income y3 ON y1.symbol = y3.symbol AND y3.rn = 3
    JOIN fy_income y4 ON y1.symbol = y4.symbol AND y4.rn = 4
    WHERE y1.rn = 1
      AND y1.netIncome > y2.netIncome
      AND y2.netIncome > y3.netIncome
      AND y3.netIncome > y4.netIncome
      AND y4.netIncome > 0
),
-- Quality filters from TTM tables
quality AS (
    SELECT
        k.symbol,
        p.companyName,
        p.exchange,
        k.returnOnEquityTTM * 100                   AS roe_pct,
        f.debtToEquityRatioTTM                      AS debt_to_equity,
        k.marketCap / 1e9                           AS market_cap_billions
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE k.returnOnEquityTTM > 0.08
      AND f.debtToEquityRatioTTM >= 0
      AND f.debtToEquityRatioTTM < 2.0
      AND k.marketCap > {mktcap_min}
      {exchange_filter}
)
SELECT
    q.symbol,
    q.companyName,
    q.exchange,
    ROUND(q.roe_pct, 1)              AS roe_pct,
    ROUND(q.debt_to_equity, 2)       AS debt_to_equity,
    ROUND(q.market_cap_billions, 2)  AS market_cap_billions,
    ROUND(s.ni_3yr_growth_pct, 1)    AS ni_3yr_growth_pct,
    s.ni_yr0_m,
    s.ni_yr1_m,
    s.ni_yr2_m,
    s.ni_yr3_m
FROM quality q
JOIN streak s ON q.symbol = s.symbol
ORDER BY q.roe_pct DESC
"""


def format_table(rows, columns):
    if not rows:
        print("No qualifying stocks found.")
        return
    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            val = row.get(c, "")
            widths[c] = max(widths[c], len(format_value(c, val)))
    header = " | ".join(c.ljust(widths[c]) for c in columns)
    separator = "-+-".join("-" * widths[c] for c in columns)
    print(header)
    print(separator)
    for row in rows:
        line = " | ".join(format_value(c, row.get(c, "")).ljust(widths[c]) for c in columns)
        print(line)


def format_value(col, value):
    if value is None:
        return "-"
    if col in ("roe_pct", "debt_to_equity", "market_cap_billions",
               "ni_3yr_growth_pct", "ni_yr0_m", "ni_yr1_m", "ni_yr2_m", "ni_yr3_m"):
        return f"{float(value):.1f}"
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="Earnings Consistency stock screen (live)")
    parser.add_argument("--exchange", type=str,
                        help="Exchange code(s), comma-separated (e.g. BSE,NSE)")
    parser.add_argument("--preset", type=str, choices=sorted(EXCHANGE_PRESETS.keys()),
                        help="Exchange preset")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges (no exchange filter)")
    parser.add_argument("--api-key", type=str,
                        help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str)
    parser.add_argument("--csv", action="store_true", help="Output as CSV")
    parser.add_argument("--top", type=int, default=30,
                        help="Show top N by ROE (default: 30)")
    args = parser.parse_args()

    # Resolve exchange
    if args.global_screen:
        exchanges = None
        label = "Global (all exchanges)"
        mktcap_min = 1_000_000_000
        exchange_filter = ""
        exchange_income_filter = ""
    elif args.preset:
        preset = EXCHANGE_PRESETS[args.preset]
        exchanges = preset["exchanges"]
        label = f"{args.preset.title()} ({', '.join(exchanges)})"
        mktcap_min = get_mktcap_threshold(exchanges)
        quoted = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({quoted})"
        exchange_income_filter = (
            f"AND symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({quoted}))"
        )
    elif args.exchange:
        exchanges = [e.strip().upper() for e in args.exchange.split(",")]
        label = ", ".join(exchanges)
        mktcap_min = get_mktcap_threshold(exchanges)
        quoted = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({quoted})"
        exchange_income_filter = (
            f"AND symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({quoted}))"
        )
    else:
        exchanges = ["NYSE", "NASDAQ", "AMEX"]
        label = "US (NYSE, NASDAQ, AMEX)"
        mktcap_min = 1_000_000_000
        exchange_filter = "AND p.exchange IN ('NYSE', 'NASDAQ', 'AMEX')"
        exchange_income_filter = (
            "AND symbol IN (SELECT DISTINCT symbol FROM profile "
            "WHERE exchange IN ('NYSE', 'NASDAQ', 'AMEX'))"
        )

    sql = SCREEN_SQL_TEMPLATE.format(
        mktcap_min=mktcap_min,
        exchange_filter=exchange_filter,
        exchange_income_filter=exchange_income_filter,
    )

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Earnings Consistency Screen: {label}")
    print(f"Filters: 3-year NI growth streak, NI>0 all 4 years, ROE>8%, D/E<2.0, MCap>threshold")
    print(f"Ranking: Top {args.top} by ROE")
    print()

    if args.csv:
        result = cr.query(sql, format="csv")
        print(result)
        return

    results = cr.query(sql)
    if not results:
        print("No qualifying stocks found.")
        return

    shown = results[:args.top]

    columns = ["symbol", "companyName", "exchange", "roe_pct", "debt_to_equity",
               "market_cap_billions", "ni_3yr_growth_pct"]
    print(f"\n{len(results)} stocks with 3-year NI growth streak. Showing top {len(shown)} by ROE:\n")
    format_table(shown, columns)
    print(f"\nTotal qualifying: {len(results)} stocks")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nData: Ceta Research (FMP financial data warehouse)")


if __name__ == "__main__":
    main()
