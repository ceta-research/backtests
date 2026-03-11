#!/usr/bin/env python3
"""FCF Yield value screen on current data.

Finds stocks that pass all filters using pre-computed TTM tables:
  1. FCF Yield (FCF/Market Cap) > 8%
  2. Return on Equity > 10%
  3. Interest Coverage Ratio > 3x
  4. Operating Profit Margin > 10%
  5. Market Cap > exchange-specific threshold

Returns top 30 by highest FCF yield.

Usage:
    # Screen US stocks (default)
    python3 fcf-yield/screen.py

    # Screen German stocks
    python3 fcf-yield/screen.py --preset germany

    # Screen all exchanges
    python3 fcf-yield/screen.py --global

See README.md for data source setup.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

FCF_YIELD_SQL_TEMPLATE = """
SELECT
    k.symbol,
    p.companyName,
    p.exchange,
    p.sector,
    k.freeCashFlowYieldTTM * 100 as fcf_yield_pct,
    k.returnOnEquityTTM * 100 as roe_pct,
    f.interestCoverageRatioTTM as interest_coverage,
    f.operatingProfitMarginTTM * 100 as op_margin_pct,
    k.marketCap / 1e9 as market_cap_billions
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE k.freeCashFlowYieldTTM > 0.08
  AND k.freeCashFlowYieldTTM < 0.50
  AND k.returnOnEquityTTM > 0.10
  AND f.interestCoverageRatioTTM > 3
  AND f.operatingProfitMarginTTM > 0.10
  AND k.marketCap > {mktcap_min}
  {exchange_filter}
ORDER BY k.freeCashFlowYieldTTM DESC
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
    if column in ("fcf_yield_pct", "roe_pct", "op_margin_pct"):
        return f"{float(value):.1f}%"
    if column == "interest_coverage":
        return f"{float(value):.1f}x"
    if column == "market_cap_billions":
        return f"${float(value):.1f}B"
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
    parser = argparse.ArgumentParser(description="FCF Yield value stock screen")
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
        result = run_screen_cloud("fcf-yield", args_str=" ".join(cloud_args),
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
        exchanges = tuple(preset["exchanges"])
        label = f"{preset['name']} ({', '.join(exchanges)})"
    elif args.exchange:
        exchanges = tuple(e.strip().upper() for e in args.exchange.split(","))
        label = ", ".join(exchanges)
    else:
        exchanges = ("NYSE", "NASDAQ", "AMEX")
        label = "US (NYSE, NASDAQ, AMEX)"

    exchange_filter = build_exchange_filter(exchanges)
    mktcap_min = get_mktcap_threshold(list(exchanges) if exchanges else None)
    sql = FCF_YIELD_SQL_TEMPLATE.format(
        mktcap_min=mktcap_min,
        exchange_filter=exchange_filter
    )

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"FCF Yield Screen: {label}")
    print(f"Filters: FCF Yield > 8%, ROE > 10%, IC > 3x, OPM > 10%, MCap > {mktcap_min/1e9:.0f}B local")
    print(f"Selection: Top 30 by highest FCF yield")
    print()

    if args.csv:
        results = cr.query(sql, format="csv", verbose=True)
        print(results)
    else:
        results = cr.query(sql, verbose=True)
        if not results:
            print("No qualifying stocks found.")
            return

        columns = ["symbol", "companyName", "exchange", "sector",
                   "fcf_yield_pct", "roe_pct", "interest_coverage",
                   "op_margin_pct", "market_cap_billions"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
