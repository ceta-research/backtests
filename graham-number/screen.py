#!/usr/bin/env python3
"""Graham Number stock screen on current (TTM) data.

Finds stocks trading below their Graham Number using pre-computed TTM tables.
Graham Number = sqrt(22.5 * EPS * BVPS). Equivalent filter: P/E * P/B < 22.5.

Returns top 30 stocks by deepest discount (lowest P/E * P/B product).

Usage:
    # Screen US stocks (default)
    python3 graham-number/screen.py

    # Screen Indian stocks
    python3 graham-number/screen.py --exchange BSE,NSE

    # Screen all exchanges
    python3 graham-number/screen.py --global

See README.md for data source setup.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import MKTCAP_THRESHOLD_MAP, get_mktcap_threshold, EXCHANGE_PRESETS


GRAHAM_SQL_TEMPLATE = """
SELECT
    k.symbol,
    p.companyName,
    p.exchange,
    ROUND(f.priceToEarningsRatioTTM, 2) as pe_ratio,
    ROUND(f.priceToBookRatioTTM, 2) as pb_ratio,
    ROUND(f.priceToEarningsRatioTTM * f.priceToBookRatioTTM, 2) as pe_pb_product,
    ROUND(k.grahamNumberTTM, 2) as graham_number,
    ROUND(k.marketCap / 1e9, 3) as market_cap_billions,
    ROUND(f.bookValuePerShareTTM, 2) as book_value_per_share
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE f.priceToEarningsRatioTTM > 0
  AND f.priceToBookRatioTTM > 0
  AND k.grahamNumberTTM > 0
  AND f.priceToEarningsRatioTTM * f.priceToBookRatioTTM < 22.5
  AND k.marketCap > {mktcap_min}
  {exchange_filter}
ORDER BY f.priceToEarningsRatioTTM * f.priceToBookRatioTTM ASC
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
    if column in ("pe_ratio", "pb_ratio", "pe_pb_product", "book_value_per_share"):
        return f"{float(value):.2f}"
    if column == "market_cap_billions":
        return f"{float(value):.3f}"
    if column == "graham_number":
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
    parser = argparse.ArgumentParser(description="Graham Number stock screen")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--exchange", type=str, help="Exchange(s), comma-separated")
    parser.add_argument("--preset", type=str, choices=sorted(EXCHANGE_PRESETS.keys()),
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--csv", action="store_true", help="Output as CSV")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("graham-number", args_str=" ".join(cloud_args),
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
        preset = EXCHANGE_PRESETS[args.preset]
        exchanges = preset["exchanges"]
        label = f"{args.preset.title()} ({', '.join(exchanges)})"
    elif args.exchange:
        exchanges = tuple(e.strip().upper() for e in args.exchange.split(","))
        label = ", ".join(exchanges)
    else:
        exchanges = ("NYSE", "NASDAQ", "AMEX")
        label = "US (NYSE, NASDAQ, AMEX)"

    mktcap_min = get_mktcap_threshold(list(exchanges) if exchanges else None)
    exchange_filter = build_exchange_filter(exchanges)
    sql = GRAHAM_SQL_TEMPLATE.format(exchange_filter=exchange_filter, mktcap_min=mktcap_min)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Graham Number Screen: {label}")
    print(f"Filter: P/E * P/B < 22.5 (price below Graham Number = sqrt(22.5 * EPS * BVPS))")
    print(f"Market cap threshold: {mktcap_min:,.0f} (local currency)")
    print(f"Selection: Top 30 by lowest P/E * P/B product (deepest discount)")
    print()

    if args.csv:
        results = cr.query(sql, format="csv", verbose=True)
        print(results)
    else:
        results = cr.query(sql, verbose=True)
        if not results:
            print("No qualifying stocks found.")
            return

        columns = ["symbol", "companyName", "exchange", "pe_ratio", "pb_ratio",
                   "pe_pb_product", "graham_number", "market_cap_billions"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
