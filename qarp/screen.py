#!/usr/bin/env python3
"""QARP quality-value screen on current data.

Finds stocks that pass all 7 filters using pre-computed TTM tables:
  1. Piotroski F-Score >= 7
  2. ROE > 15%
  3. Debt-to-Equity < 0.5
  4. Current Ratio > 1.5
  5. Income Quality (OCF/NI) > 1.0
  6. P/E between 5 and 25
  7. Market Cap > $1B

Usage:
    # Screen US stocks (default)
    python3 qarp/screen.py

    # Screen Indian stocks
    python3 qarp/screen.py --exchange BSE,NSE

    # Screen German stocks
    python3 qarp/screen.py --exchange XETRA

    # Screen all exchanges (global)
    python3 qarp/screen.py --global

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


QARP_SQL_TEMPLATE = """
SELECT
    k.symbol,
    p.companyName,
    p.exchange,
    k.returnOnEquityTTM * 100 as roe_pct,
    f.debtToEquityRatioTTM as debt_to_equity,
    k.currentRatioTTM as current_ratio,
    k.incomeQualityTTM as income_quality,
    s.piotroskiScore,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.marketCap / 1e9 as market_cap_billions
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN scores s ON k.symbol = s.symbol
JOIN profile p ON k.symbol = p.symbol
WHERE
    k.returnOnEquityTTM > 0.15
    AND f.debtToEquityRatioTTM >= 0
    AND f.debtToEquityRatioTTM < 0.5
    AND k.currentRatioTTM > 1.5
    AND k.incomeQualityTTM > 1
    AND s.piotroskiScore >= 7
    AND f.priceToEarningsRatioTTM > 5
    AND f.priceToEarningsRatioTTM < 25
    AND k.marketCap > 1000000000
    {exchange_filter}
ORDER BY s.piotroskiScore DESC, k.returnOnEquityTTM DESC
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

    # Compute column widths
    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            val = row.get(c, "")
            widths[c] = max(widths[c], len(format_value(c, val)))

    # Header
    header = " | ".join(c.ljust(widths[c]) for c in columns)
    separator = "-+-".join("-" * widths[c] for c in columns)
    print(header)
    print(separator)

    # Rows
    for row in rows:
        line = " | ".join(format_value(c, row.get(c, "")).ljust(widths[c]) for c in columns)
        print(line)


def format_value(column, value):
    if value is None:
        return "-"
    if column in ("roe_pct", "pe_ratio", "debt_to_equity", "current_ratio", "income_quality", "market_cap_billions"):
        return f"{float(value):.2f}"
    if column == "piotroskiScore":
        return str(int(value))
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="QARP quality-value stock screen")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--exchange", type=str, help="Exchange(s), comma-separated (e.g., NYSE,NASDAQ,AMEX)")
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
        result = run_screen_cloud("qarp", args_str=" ".join(cloud_args),
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

    # Build SQL
    exchange_filter = build_exchange_filter(exchanges)
    sql = QARP_SQL_TEMPLATE.format(exchange_filter=exchange_filter)

    # Connect
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"QARP Screen: {label}")
    print(f"Filters: Piotroski >= 7, ROE > 15%, D/E < 0.5, CR > 1.5, IQ > 1.0, P/E 5-25, MCap > $1B")
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

        columns = ["symbol", "companyName", "exchange", "piotroskiScore",
                    "roe_pct", "pe_ratio", "debt_to_equity", "current_ratio",
                    "income_quality", "market_cap_billions"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)

        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
