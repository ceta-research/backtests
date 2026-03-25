#!/usr/bin/env python3
"""Piotroski F-Score screen on current data using pre-computed TTM tables.

Two screen modes:
  1. Simple: High F-Score stocks (score >= 8), any sector
  2. Value: High F-Score + low P/B + reasonable P/E (Piotroski's original value focus)

Usage:
    # Simple screen: US stocks with F-Score >= 8
    python3 piotroski/screen.py

    # Value screen: F-Score >= 7, P/B < 1.5, P/E < 20
    python3 piotroski/screen.py --value

    # Screen Indian stocks
    python3 piotroski/screen.py --exchange BSE,NSE

    # Screen all exchanges
    python3 piotroski/screen.py --global

    # Lower the score threshold
    python3 piotroski/screen.py --min-score 7

See README.md for data source setup.
"""

import argparse
import os
import sys

# --- Data source (default: Ceta Research) ---
# Screens use pre-computed TTM tables via a single SQL query.
# See README.md for alternatives.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch


SIMPLE_SCREEN_SQL = """
SELECT
    s.symbol,
    p.companyName,
    p.exchange,
    s.piotroskiScore,
    f.priceToBookRatioTTM as price_to_book,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.returnOnEquityTTM * 100 as roe_pct,
    k.marketCap / 1e9 as market_cap_billions
FROM scores s
JOIN key_metrics_ttm k ON s.symbol = k.symbol
JOIN financial_ratios_ttm f ON s.symbol = f.symbol
JOIN profile p ON s.symbol = p.symbol
WHERE
    s.piotroskiScore >= {min_score}
    AND k.marketCap > {min_mcap}
    {exchange_filter}
ORDER BY s.piotroskiScore DESC, f.priceToBookRatioTTM ASC
LIMIT 50
"""

VALUE_SCREEN_SQL = """
SELECT
    s.symbol,
    p.companyName,
    p.exchange,
    p.sector,
    s.piotroskiScore,
    f.priceToBookRatioTTM as price_to_book,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.currentRatioTTM as current_ratio,
    f.debtToEquityRatioTTM as debt_to_equity,
    k.returnOnEquityTTM * 100 as roe_pct,
    k.marketCap / 1e9 as market_cap_billions
FROM scores s
JOIN key_metrics_ttm k ON s.symbol = k.symbol
JOIN financial_ratios_ttm f ON s.symbol = f.symbol
JOIN profile p ON s.symbol = p.symbol
WHERE
    s.piotroskiScore >= {min_score}
    AND f.priceToBookRatioTTM > 0
    AND f.priceToBookRatioTTM < 1.5
    AND f.priceToEarningsRatioTTM > 0
    AND f.priceToEarningsRatioTTM < 20
    AND k.marketCap > {min_mcap}
    AND p.sector NOT IN ('Financial Services')
    {exchange_filter}
ORDER BY s.piotroskiScore DESC, f.priceToBookRatioTTM ASC
LIMIT 30
"""

EXCHANGE_PRESETS = {
    "us": ("NYSE", "NASDAQ", "AMEX"),
    "india": ("NSE",),
    "germany": ("XETRA",),
    "china": ("SHZ", "SHH"),
    "hongkong": ("HKSE",),
}


def build_exchange_filter(exchanges):
    if not exchanges:
        return ""
    quoted = ", ".join(f"'{e}'" for e in exchanges)
    return f"AND p.exchange IN ({quoted})"


def format_table(rows, columns):
    """Simple table formatter."""
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


def format_value(column, value):
    if value is None:
        return "-"
    if column in ("roe_pct", "pe_ratio", "price_to_book", "debt_to_equity",
                   "current_ratio", "market_cap_billions"):
        return f"{float(value):.2f}"
    if column == "piotroskiScore":
        return str(int(value))
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="Piotroski F-Score stock screen")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--value", action="store_true",
                        help="Value screen: add P/B < 1.5, P/E < 20, exclude financials")
    parser.add_argument("--min-score", type=int, default=8,
                        help="Minimum F-Score (default: 8)")
    parser.add_argument("--min-mcap", type=float, default=500_000_000,
                        help="Minimum market cap in dollars (default: 500M)")
    parser.add_argument("--exchange", type=str,
                        help="Exchange(s), comma-separated (e.g., NYSE,NASDAQ,AMEX)")
    parser.add_argument("--preset", type=str, choices=EXCHANGE_PRESETS.keys(),
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges (no filter)")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL (for local dev)")
    parser.add_argument("--csv", action="store_true", help="Output as CSV instead of table")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("piotroski", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    # Determine exchanges
    if args.global_screen:
        exchanges = None
        label = "Global (all exchanges)"
    elif args.preset:
        exchanges = EXCHANGE_PRESETS[args.preset]
        label = f"{args.preset.title()} ({', '.join(exchanges)})"
    elif args.exchange:
        exchanges = tuple(e.strip().upper() for e in args.exchange.split(","))
        label = ", ".join(exchanges)
    else:
        exchanges = ("NYSE", "NASDAQ", "AMEX")
        label = "US (NYSE, NASDAQ, AMEX)"

    exchange_filter = build_exchange_filter(exchanges)

    # Choose screen type
    # Value screen uses lower defaults to match blog's advanced screen
    if args.value:
        min_score = args.min_score if args.min_score != 8 else 7
        min_mcap = args.min_mcap if args.min_mcap != 500_000_000 else 300_000_000
        screen_name = "Piotroski Value Screen"
        sql = VALUE_SCREEN_SQL.format(
            min_score=min_score,
            min_mcap=int(min_mcap),
            exchange_filter=exchange_filter,
        )
        columns = ["symbol", "companyName", "exchange", "sector", "piotroskiScore",
                    "price_to_book", "pe_ratio", "current_ratio", "debt_to_equity",
                    "roe_pct", "market_cap_billions"]
        filters = (f"F-Score >= {min_score}, P/B < 1.5, P/E < 20, "
                   f"MCap > ${min_mcap/1e9:.1f}B, ex-financials")
    else:
        screen_name = "Piotroski F-Score Screen"
        sql = SIMPLE_SCREEN_SQL.format(
            min_score=args.min_score,
            min_mcap=int(args.min_mcap),
            exchange_filter=exchange_filter,
        )
        columns = ["symbol", "companyName", "exchange", "piotroskiScore",
                    "price_to_book", "pe_ratio", "roe_pct", "market_cap_billions"]
        filters = f"F-Score >= {args.min_score}, MCap > ${args.min_mcap/1e9:.1f}B"

    # Connect
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"{screen_name}: {label}")
    print(f"Filters: {filters}")
    print()

    # Execute
    if args.csv:
        results = cr.query(sql, format="csv", verbose=True)
        print(results)
    else:
        results = cr.query(sql, verbose=True)
        if not results:
            print("No qualifying stocks found.")
            return

        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)

        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
