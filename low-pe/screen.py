#!/usr/bin/env python3
"""Low P/E value screen on current data.

Finds stocks that pass all 4 filters using pre-computed TTM tables:
  1. P/E ratio between 0 and 15
  2. ROE > 10%
  3. Debt-to-Equity < 1.0
  4. Market Cap > $1B

Returns top 30 by lowest P/E.

Usage:
    # Screen US stocks (default)
    python3 low-pe/screen.py

    # Screen Indian stocks
    python3 low-pe/screen.py --exchange BSE,NSE

    # Screen all exchanges
    python3 low-pe/screen.py --global

See README.md for data source setup.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

LOW_PE_SQL_TEMPLATE = """
SELECT
    k.symbol,
    p.companyName,
    p.exchange,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.returnOnEquityTTM * 100 as roe_pct,
    f.debtToEquityRatioTTM as debt_to_equity,
    k.marketCap / 1e9 as market_cap_billions
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE f.priceToEarningsRatioTTM > 0
  AND f.priceToEarningsRatioTTM < 15
  AND k.returnOnEquityTTM > 0.10
  AND f.debtToEquityRatioTTM >= 0
  AND f.debtToEquityRatioTTM < 1.0
  AND k.marketCap > 1000000000
  {exchange_filter}
ORDER BY f.priceToEarningsRatioTTM ASC
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
    if column in ("pe_ratio", "roe_pct", "debt_to_equity", "market_cap_billions"):
        return f"{float(value):.2f}"
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
    parser = argparse.ArgumentParser(description="Low P/E value stock screen")
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
        result = run_screen_cloud("low-pe", args_str=" ".join(cloud_args),
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
    sql = LOW_PE_SQL_TEMPLATE.format(exchange_filter=exchange_filter)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Low P/E Screen: {label}")
    print(f"Filters: P/E 0-15, ROE > 10%, D/E < 1.0, MCap > $1B")
    print(f"Selection: Top 30 by lowest P/E")
    print()

    if args.csv:
        results = cr.query(sql, format="csv", verbose=True)
        print(results)
    else:
        results = cr.query(sql, verbose=True)
        if not results:
            print("No qualifying stocks found.")
            return

        columns = ["symbol", "companyName", "exchange", "pe_ratio",
                    "roe_pct", "debt_to_equity", "market_cap_billions"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
