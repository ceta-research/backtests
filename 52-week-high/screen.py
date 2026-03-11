#!/usr/bin/env python3
"""52-Week High Proximity screen — current qualifying stocks.

Finds stocks closest to their 52-week high using live stock_eod data.
Signal: adjClose / MAX(high over 252 trading days) per George & Hwang (2004).

Universe: Market cap > exchange threshold (from key_metrics_ttm).

Usage:
    python3 52-week-high/screen.py                     # US (default)
    python3 52-week-high/screen.py --preset india
    python3 52-week-high/screen.py --exchange XETRA
    python3 52-week-high/screen.py --preset us --top 50
    python3 52-week-high/screen.py --preset us --csv
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
price_window AS (
    SELECT
        symbol,
        date,
        adjClose,
        high,
        dateEpoch,
        MAX(high) OVER (
            PARTITION BY symbol ORDER BY dateEpoch
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS high_52w,
        COUNT(*) OVER (
            PARTITION BY symbol ORDER BY dateEpoch
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS row_count
    FROM stock_eod
    WHERE date >= CURRENT_DATE - INTERVAL '14' MONTH
      AND adjClose > 0
      AND high > 0
),
latest AS (
    SELECT symbol, adjClose, high_52w, row_count
    FROM price_window
    WHERE date >= CURRENT_DATE - INTERVAL '10' DAY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) = 1
),
proximity AS (
    SELECT
        symbol,
        ROUND(adjClose / high_52w, 4)                  AS proximity_ratio,
        ROUND((1 - adjClose / high_52w) * 100, 1)      AS pct_below_high,
        ROUND(adjClose, 2)                              AS price,
        ROUND(high_52w, 2)                              AS high_52w
    FROM latest
    WHERE high_52w > 0
      AND row_count >= 100
      AND adjClose / high_52w <= 1.05
)
SELECT
    u.symbol,
    u.companyName,
    u.exchange,
    ROUND(u.market_cap_billions, 2)  AS market_cap_billions,
    pr.proximity_ratio,
    pr.pct_below_high,
    pr.price,
    pr.high_52w
FROM universe u
JOIN proximity pr ON u.symbol = pr.symbol
ORDER BY pr.proximity_ratio DESC NULLS LAST
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
    if col in ("market_cap_billions", "proximity_ratio", "pct_below_high", "price", "high_52w"):
        return f"{float(value):.2f}" if col == "proximity_ratio" else f"{float(value):.1f}"
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="52-Week High Proximity stock screen (live)")
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
                        help="Show top N by proximity ratio (default: 30)")
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

    print(f"52-Week High Proximity Screen: {label}")
    print(f"Universe: MCap > threshold, isActivelyTrading = true")
    print(f"Signal: adjClose / MAX(high over 252 trading days) [George & Hwang 2004]")
    print(f"Showing top {args.top} by proximity ratio\n")

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
               "proximity_ratio", "pct_below_high", "price", "high_52w"]
    print(f"{len(results)} stocks in universe. Top {len(shown)} by proximity ratio:\n")
    format_table(shown, columns)
    print(f"\nTotal universe: {len(results)} stocks")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nData: Ceta Research (FMP financial data warehouse)")
    print("Proximity ratio: 1.00 = exactly at 52-week high | 0.95 = 5% below high")


if __name__ == "__main__":
    main()
