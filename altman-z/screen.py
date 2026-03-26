#!/usr/bin/env python3
"""Altman Z-Score stock screen on current data.

Uses pre-computed altmanZScore from the scores table.

Two modes:
  1. Simple: Z-Score > 3.0 (safe zone), sorted by Z-Score descending
  2. Advanced: Z > 3.0 + Piotroski >= 5 + ROE > 0 + D/E < 1.5

Both modes exclude Financial Services and Utilities.

Usage:
    # Simple screen: US safe-zone stocks
    python3 altman-z/screen.py

    # Advanced screen with quality filters
    python3 altman-z/screen.py --advanced

    # Screen Indian stocks
    python3 altman-z/screen.py --exchange BSE,NSE

    # Screen using preset
    python3 altman-z/screen.py --preset india

    # Screen all exchanges
    python3 altman-z/screen.py --global

See README.md for data source setup.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

SIMPLE_SQL_TEMPLATE = """
SELECT
    s.symbol,
    p.companyName,
    p.exchange,
    p.sector,
    s.altmanZScore as z_score,
    k.marketCap / 1e9 as market_cap_billions
FROM scores s
JOIN key_metrics_ttm k ON s.symbol = k.symbol
JOIN profile p ON s.symbol = p.symbol
WHERE
    s.altmanZScore > 3.0
    AND k.marketCap > {min_mcap}
    AND p.sector NOT IN ('Financial Services', 'Utilities')
    {exchange_filter}
ORDER BY s.altmanZScore DESC
LIMIT 50
"""

ADVANCED_SQL_TEMPLATE = """
SELECT
    s.symbol,
    p.companyName,
    p.exchange,
    p.sector,
    s.altmanZScore as z_score,
    s.piotroskiScore,
    k.returnOnEquityTTM * 100 as roe_pct,
    f.debtToEquityRatioTTM as debt_to_equity,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.marketCap / 1e9 as market_cap_billions
FROM scores s
JOIN key_metrics_ttm k ON s.symbol = k.symbol
JOIN financial_ratios_ttm f ON s.symbol = f.symbol
JOIN profile p ON s.symbol = p.symbol
WHERE
    s.altmanZScore > 3.0
    AND s.piotroskiScore >= 5
    AND k.returnOnEquityTTM > 0
    AND f.debtToEquityRatioTTM >= 0
    AND f.debtToEquityRatioTTM < 1.5
    AND k.marketCap > {min_mcap}
    AND p.sector NOT IN ('Financial Services', 'Utilities')
    {exchange_filter}
ORDER BY s.altmanZScore DESC
LIMIT 50
"""

EXCHANGE_PRESETS = {
    "us": ("NYSE", "NASDAQ", "AMEX"),
    "india": ("NSE",),
    "germany": ("XETRA",),
    "china": ("SHZ", "SHH"),
    "hongkong": ("HKSE",),
    "korea": ("KSC",),
    "brazil": ("SAO",),
    "taiwan": ("TAI", "TWO"),
    "singapore": ("SES",),
    "southafrica": ("JNB",),
    "canada": ("TSX",),
    "switzerland": ("SIX",),
    "sweden": ("STO",),
    "france": ("PAR",),
    "uk": ("LSE",),
}


def build_exchange_filter(exchanges):
    if not exchanges:
        return ""
    quoted = ", ".join(f"'{e}'" for e in exchanges)
    return f"AND p.exchange IN ({quoted})"


def format_value(column, value):
    if value is None:
        return "-"
    if column in ("z_score", "roe_pct", "pe_ratio", "debt_to_equity",
                   "market_cap_billions"):
        return f"{float(value):.2f}"
    if column == "piotroskiScore":
        return str(int(value))
    return str(value)


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
        line = " | ".join(
            format_value(c, row.get(c, "")).ljust(widths[c]) for c in columns)
        print(line)


def main():
    parser = argparse.ArgumentParser(
        description="Altman Z-Score stock screen")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--advanced", action="store_true",
                        help="Advanced screen: Z>3 + Piotroski>=5 + ROE>0 + D/E<1.5")
    parser.add_argument("--min-mcap", type=float, default=500_000_000,
                        help="Minimum market cap in dollars (default: 500M)")
    parser.add_argument("--exchange", type=str,
                        help="Exchange(s), comma-separated (e.g., NYSE,NASDAQ,AMEX)")
    parser.add_argument("--preset", type=str, choices=EXCHANGE_PRESETS.keys(),
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges (no filter)")
    parser.add_argument("--api-key", type=str,
                        help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str,
                        help="API base URL (for local dev)")
    parser.add_argument("--csv", action="store_true",
                        help="Output as CSV instead of table")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud(
            "altman-z", args_str=" ".join(cloud_args),
            api_key=args.api_key, base_url=args.base_url, verbose=True)
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
    if args.advanced:
        screen_name = "Altman Z-Score Advanced Screen"
        sql = ADVANCED_SQL_TEMPLATE.format(
            min_mcap=int(args.min_mcap),
            exchange_filter=exchange_filter,
        )
        columns = ["symbol", "companyName", "exchange", "sector", "z_score",
                    "piotroskiScore", "roe_pct", "debt_to_equity", "pe_ratio",
                    "market_cap_billions"]
        filters = ("Z-Score > 3.0, Piotroski >= 5, ROE > 0%, D/E < 1.5, "
                   f"MCap > ${args.min_mcap/1e9:.1f}B, excl. financials/utilities")
    else:
        screen_name = "Altman Z-Score Screen"
        sql = SIMPLE_SQL_TEMPLATE.format(
            min_mcap=int(args.min_mcap),
            exchange_filter=exchange_filter,
        )
        columns = ["symbol", "companyName", "exchange", "sector", "z_score",
                    "market_cap_billions"]
        filters = (f"Z-Score > 3.0, MCap > ${args.min_mcap/1e9:.1f}B, "
                   "excl. financials/utilities")

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
