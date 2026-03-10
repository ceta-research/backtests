#!/usr/bin/env python3
"""Quality Momentum screen — current qualifying stocks.

Uses TTM (trailing twelve months) tables for quality filters, then ranks
by 12-month price return. Run against production data for a live signal.

Quality filters (TTM):
  - ROE > 15%
  - Debt-to-Equity < 1.0 (and >= 0, i.e., not negative equity)
  - Gross margin > 20%
  - Market cap > exchange threshold

Momentum: sorted by 12M price return descending (top 30 = portfolio picks).
Note: Screen shows all qualifying stocks ranked by momentum. The backtest
      selects top 30 equal-weight; your own screen can adjust the cutoff.

Usage:
    python3 quality-momentum/screen.py                     # US (default)
    python3 quality-momentum/screen.py --preset india
    python3 quality-momentum/screen.py --exchange XETRA
    python3 quality-momentum/screen.py --global
    python3 quality-momentum/screen.py --preset us --csv
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

# Current-data screen using TTM tables.
# Momentum is computed from stock_eod within the same SQL query (last available
# price vs. price ~365 days ago). The backtest uses point-in-time FY data.
SCREEN_SQL_TEMPLATE = """
WITH quality AS (
    SELECT
        k.symbol,
        p.companyName,
        p.exchange,
        k.returnOnEquityTTM * 100                          AS roe_pct,
        f.debtToEquityRatioTTM                             AS debt_to_equity,
        CASE
            WHEN f.grossProfitMarginTTM IS NOT NULL THEN f.grossProfitMarginTTM * 100
            ELSE NULL
        END                                                AS gross_margin_pct,
        k.marketCap / 1e9                                  AS market_cap_billions
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE k.returnOnEquityTTM > 0.15
      AND f.debtToEquityRatioTTM >= 0
      AND f.debtToEquityRatioTTM < 1.0
      AND (
          f.grossProfitMarginTTM IS NULL
          OR f.grossProfitMarginTTM > 0.20
      )
      AND k.marketCap > {mktcap_min}
      {exchange_filter}
),
current_price AS (
    SELECT symbol, adjClose AS price_now, dateEpoch AS epoch_now,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM stock_eod
    WHERE date >= CURRENT_DATE - INTERVAL '15' DAY
      AND adjClose > 0
),
price_12m_ago AS (
    SELECT symbol, adjClose AS price_12m, dateEpoch AS epoch_12m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) - CAST(
                   EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '365' DAY))::BIGINT AS BIGINT
               ))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '395' DAY AND CURRENT_DATE - INTERVAL '335' DAY
      AND adjClose > 0
),
momentum AS (
    SELECT c.symbol,
           ROUND((c.price_now - p12.price_12m) / p12.price_12m * 100, 1) AS return_12m_pct
    FROM current_price c
    JOIN price_12m_ago p12 ON c.symbol = p12.symbol
    WHERE c.rn = 1 AND p12.rn = 1
      AND p12.price_12m > 0
)
SELECT
    q.symbol,
    q.companyName,
    q.exchange,
    ROUND(q.roe_pct, 1)           AS roe_pct,
    ROUND(q.debt_to_equity, 2)    AS debt_to_equity,
    ROUND(q.gross_margin_pct, 1)  AS gross_margin_pct,
    ROUND(q.market_cap_billions, 2) AS market_cap_billions,
    m.return_12m_pct
FROM quality q
LEFT JOIN momentum m ON q.symbol = m.symbol
ORDER BY m.return_12m_pct DESC NULLS LAST
"""

EXCHANGE_MKTCAP = {
    "us": 1_000_000_000,        # $1B USD
    "india": 20_000_000_000,    # ₹20B
    "germany": 500_000_000,     # €500M
    "china": 2_000_000_000,     # ¥2B
    "hongkong": 2_000_000_000,  # HK$2B
    "uk": 500_000_000,          # £500M
    "japan": 100_000_000_000,   # ¥100B
    "korea": 500_000_000_000,   # ₩500B
    "taiwan": 10_000_000_000,   # NT$10B
    "canada": 500_000_000,      # C$500M
    "switzerland": 500_000_000, # CHF 500M
    "sweden": 5_000_000_000,    # SEK 5B
    "thailand": 10_000_000_000, # ฿10B
}


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
    if col in ("roe_pct", "debt_to_equity", "gross_margin_pct",
               "market_cap_billions", "return_12m_pct"):
        return f"{float(value):.1f}"
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="Quality Momentum stock screen (live)")
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
                        help="Show top N by momentum (default: 30)")
    args = parser.parse_args()

    # Resolve exchange
    if args.global_screen:
        exchanges = None
        label = "Global (all exchanges)"
        mktcap_min = 1_000_000_000
    elif args.preset:
        preset = EXCHANGE_PRESETS[args.preset]
        exchanges = preset["exchanges"]
        label = f"{args.preset.title()} ({', '.join(exchanges)})"
        mktcap_min = get_mktcap_threshold(exchanges)
    elif args.exchange:
        exchanges = [e.strip().upper() for e in args.exchange.split(",")]
        label = ", ".join(exchanges)
        mktcap_min = get_mktcap_threshold(exchanges)
    else:
        exchanges = ["NYSE", "NASDAQ", "AMEX"]
        label = "US (NYSE, NASDAQ, AMEX)"
        mktcap_min = 1_000_000_000

    # Build exchange filter
    if exchanges:
        quoted = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({quoted})"
    else:
        exchange_filter = ""

    sql = SCREEN_SQL_TEMPLATE.format(
        mktcap_min=mktcap_min,
        exchange_filter=exchange_filter,
    )

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Quality Momentum Screen: {label}")
    print(f"Filters: ROE>15%, D/E<1.0, Gross Margin>20%, MCap>threshold")
    print(f"Ranking: Top {args.top} by 12-month price return")
    print()

    if args.csv:
        result = cr.query(sql, format="csv")
        print(result)
        return

    results = cr.query(sql)
    if not results:
        print("No qualifying stocks found.")
        return

    # Limit to top N
    shown = results[:args.top]

    columns = ["symbol", "companyName", "exchange", "roe_pct", "debt_to_equity",
               "gross_margin_pct", "market_cap_billions", "return_12m_pct"]
    print(f"\n{len(results)} total quality stocks. Showing top {len(shown)} by 12M momentum:\n")
    format_table(shown, columns)
    print(f"\nTotal qualifying: {len(results)} stocks")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nData: Ceta Research (FMP financial data warehouse)")


if __name__ == "__main__":
    main()
