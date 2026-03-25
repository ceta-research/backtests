#!/usr/bin/env python3
"""Magic Formula (Greenblatt) screen on current TTM data.

Ranks stocks by combined Earnings Yield + ROCE rank.
Excludes Financial Services and Utilities (Greenblatt's methodology).

Usage:
    # Screen US stocks (default)
    python3 magic-formula/screen.py

    # Screen Indian stocks
    python3 magic-formula/screen.py --exchange BSE,NSE

    # Screen all exchanges
    python3 magic-formula/screen.py --global

    # Include financials and utilities
    python3 magic-formula/screen.py --no-sector-filter

See README.md for methodology.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

MAGIC_FORMULA_SQL_TEMPLATE = """
WITH base AS (
    SELECT
        k.symbol,
        p.companyName,
        p.exchange,
        p.sector,
        k.earningsYieldTTM as earnings_yield,
        k.returnOnCapitalEmployedTTM as roce,
        k.marketCap / 1e9 as market_cap_billions
    FROM key_metrics_ttm k
    JOIN profile p ON k.symbol = p.symbol
    WHERE k.earningsYieldTTM > 0
      AND k.returnOnCapitalEmployedTTM > 0
      AND k.marketCap > 1000000000
      {exchange_filter}
      {sector_filter}
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY earnings_yield DESC) AS ey_rank,
        RANK() OVER (ORDER BY roce DESC) AS roce_rank
    FROM base
)
SELECT symbol, companyName, exchange, sector,
       ROUND(earnings_yield * 100, 2) as ey_pct,
       ROUND(roce * 100, 2) as roce_pct,
       ey_rank, roce_rank,
       (ey_rank + roce_rank) as combined_rank,
       ROUND(market_cap_billions, 1) as mcap_bn
FROM ranked
ORDER BY combined_rank ASC
LIMIT 30
"""


def build_exchange_filter(exchanges):
    if not exchanges:
        return ""
    quoted = ", ".join(f"'{e}'" for e in exchanges)
    return f"AND p.exchange IN ({quoted})"


def format_value(column, value):
    if value is None:
        return "-"
    if column in ("ey_pct", "roce_pct", "mcap_bn"):
        return f"{float(value):.1f}"
    if column in ("ey_rank", "roce_rank", "combined_rank"):
        return str(int(float(value)))
    return str(value)


def format_table(rows, columns):
    if not rows:
        print("No qualifying stocks found.")
        return

    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            widths[c] = max(widths[c], len(format_value(c, row.get(c, ""))))

    header = " | ".join(c.ljust(widths[c]) for c in columns)
    separator = "-+-".join("-" * widths[c] for c in columns)
    print(header)
    print(separator)
    for row in rows:
        line = " | ".join(format_value(c, row.get(c, "")).ljust(widths[c]) for c in columns)
        print(line)


def main():
    parser = argparse.ArgumentParser(description="Magic Formula stock screen (current TTM data)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--exchange", type=str, help="Exchange(s), comma-separated")
    parser.add_argument("--preset", type=str, choices=["us", "india", "germany", "china", "hongkong",
                                                        "japan", "korea", "australia", "uk"],
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--csv", action="store_true", help="Output as CSV")
    parser.add_argument("--no-sector-filter", action="store_true",
                        help="Include Financial Services and Utilities")
    args = parser.parse_args()

    preset_map = {
        "us": ("NYSE", "NASDAQ", "AMEX"),
        "india": ("NSE",),
        "germany": ("XETRA",),
        "china": ("SHZ", "SHH"),
        "hongkong": ("HKSE",),
        "japan": ("JPX",),
        "korea": ("KSC",),
        "australia": ("ASX",),
        "uk": ("LSE",),
    }

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("magic-formula", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    if args.global_screen:
        exchanges = None
        label = "Global (all exchanges)"
    elif args.preset:
        exchanges = preset_map[args.preset]
        label = f"{args.preset.title()} ({', '.join(exchanges)})"
    elif args.exchange:
        exchanges = tuple(e.strip().upper() for e in args.exchange.split(","))
        label = ", ".join(exchanges)
    else:
        exchanges = ("NYSE", "NASDAQ", "AMEX")
        label = "US (NYSE, NASDAQ, AMEX)"

    exchange_filter = build_exchange_filter(exchanges)
    sector_filter = "" if args.no_sector_filter else "AND p.sector NOT IN ('Financial Services', 'Utilities')"
    sql = MAGIC_FORMULA_SQL_TEMPLATE.format(
        exchange_filter=exchange_filter,
        sector_filter=sector_filter,
    )

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    sector_note = "" if args.no_sector_filter else " (ex. Financials/Utilities)"
    print(f"Magic Formula Screen: {label}")
    print(f"Signal: Rank(EY) + Rank(ROCE), top 30{sector_note}")
    print(f"Filters: EY > 0, ROCE > 0, MCap > $1B")
    print()

    if args.csv:
        results = cr.query(sql, format="csv", verbose=True)
        print(results)
    else:
        results = cr.query(sql, verbose=True)
        if not results:
            print("No qualifying stocks found.")
            return

        columns = ["symbol", "companyName", "exchange",
                    "ey_pct", "roce_pct", "combined_rank", "mcap_bn"]
        print(f"\nTop {len(results)} by Magic Formula rank:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
