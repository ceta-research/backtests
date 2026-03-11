#!/usr/bin/env python3
"""12-Month Price Momentum screen — current qualifying stocks.

Pure price momentum screen using stock_eod for live 12-1M return calculation.
No financial quality filters — this is the raw momentum signal.

Momentum signal: 12M-1M return (price from ~12M ago to ~1M ago, skip last month).
Universe: Market cap > exchange threshold (from key_metrics_ttm).

Usage:
    python3 price-momentum/screen.py                     # US (default)
    python3 price-momentum/screen.py --preset india
    python3 price-momentum/screen.py --exchange XETRA
    python3 price-momentum/screen.py --preset us --top 50
    python3 price-momentum/screen.py --preset us --csv
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

SCREEN_SQL_TEMPLATE = """
WITH universe AS (
    SELECT
        p.symbol,
        p.companyName,
        p.exchange,
        k.marketCap / 1e9 AS market_cap_billions
    FROM profile p
    JOIN key_metrics_ttm k ON p.symbol = k.symbol
    WHERE k.marketCap > {mktcap_min}
      AND p.isActivelyTrading = true
      {exchange_filter}
),
price_now AS (
    SELECT symbol, adjClose AS price_now,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM stock_eod
    WHERE date >= CURRENT_DATE - INTERVAL '15' DAY
      AND adjClose > 0
),
price_12m_ago AS (
    SELECT symbol, adjClose AS price_12m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) - CAST(
                   EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '365' DAY))::BIGINT AS BIGINT
               ))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '395' DAY
                   AND CURRENT_DATE - INTERVAL '335' DAY
      AND adjClose > 0
),
price_1m_ago AS (
    SELECT symbol, adjClose AS price_1m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) - CAST(
                   EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '30' DAY))::BIGINT AS BIGINT
               ))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '45' DAY
                   AND CURRENT_DATE - INTERVAL '15' DAY
      AND adjClose > 0
),
momentum AS (
    SELECT
        p12.symbol,
        ROUND((p1m.price_1m - p12.price_12m) / p12.price_12m * 100, 1) AS return_12m_1m_pct,
        ROUND((pn.price_now - p1m.price_1m) / p1m.price_1m * 100, 1)   AS return_1m_pct
    FROM price_12m_ago p12
    JOIN price_1m_ago p1m ON p12.symbol = p1m.symbol
    JOIN price_now pn ON p12.symbol = pn.symbol
    WHERE p12.rn = 1
      AND p1m.rn = 1
      AND pn.rn = 1
      AND p12.price_12m > 0
      AND p1m.price_1m > 1.0
)
SELECT
    u.symbol,
    u.companyName,
    u.exchange,
    ROUND(u.market_cap_billions, 2)  AS market_cap_billions,
    m.return_12m_1m_pct,
    m.return_1m_pct
FROM universe u
JOIN momentum m ON u.symbol = m.symbol
ORDER BY m.return_12m_1m_pct DESC NULLS LAST
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
    if col in ("market_cap_billions", "return_12m_1m_pct", "return_1m_pct"):
        return f"{float(value):.1f}"
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="12-Month Price Momentum stock screen (live)")
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
                        help="Show top N by 12M-1M momentum (default: 30)")
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

    print(f"12-Month Price Momentum Screen: {label}")
    print(f"Universe: MCap > threshold, Price > $1")
    print(f"Signal: 12M-1M return (skip last month, per Jegadeesh & Titman 1993)")
    print(f"Showing top {args.top} by 12M-1M return\n")

    if args.csv:
        result = cr.query(sql, format="csv")
        print(result)
        return

    results = cr.query(sql)
    if not results:
        print("No qualifying stocks found.")
        return

    shown = results[:args.top]

    columns = ["symbol", "companyName", "exchange", "market_cap_billions",
               "return_12m_1m_pct", "return_1m_pct"]
    print(f"{len(results)} stocks in universe. Top {len(shown)} by 12M-1M momentum:\n")
    format_table(shown, columns)
    print(f"\nTotal universe: {len(results)} stocks")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nData: Ceta Research (FMP financial data warehouse)")
    print("Note: 1M return shown for reference — not part of the entry signal")


if __name__ == "__main__":
    main()
