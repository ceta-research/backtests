#!/usr/bin/env python3
"""S&P 500 survivorship bias screen - biased vs unbiased comparison.

Shows current low P/E stocks from two universes:
  1. Biased: Current S&P 500 members (what most screeners use)
  2. Unbiased: Reconstructed point-in-time S&P 500 (includes recently removed members)

The difference highlights stocks that survivorship-biased screens miss entirely.

Usage:
    # Default comparison
    python3 sp500-survivorship/screen.py

    # With explicit API key
    python3 sp500-survivorship/screen.py --api-key YOUR_KEY

    # CSV output
    python3 sp500-survivorship/screen.py --csv

See README.md for data source setup.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch


# Biased universe: current S&P 500 members with low P/E
BIASED_SQL = """
SELECT
    k.symbol,
    p.companyName,
    p.sector,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.returnOnEquityTTM * 100 as roe_pct,
    f.debtToEquityRatioTTM as debt_to_equity,
    k.marketCap / 1e9 as market_cap_billions
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN profile p ON k.symbol = p.symbol
JOIN sp500_constituent s ON k.symbol = s.symbol
WHERE f.priceToEarningsRatioTTM > 0
  AND f.priceToEarningsRatioTTM < 15
ORDER BY f.priceToEarningsRatioTTM ASC
LIMIT 100
"""

# Unbiased universe: current + recently removed S&P 500 members with low P/E.
# Includes stocks removed in the last 5 years that a point-in-time screen would
# have considered (approximation of PIT membership for a live screen).
UNBIASED_SQL = """
WITH pit_universe AS (
    -- Current members
    SELECT DISTINCT symbol FROM sp500_constituent
    UNION
    -- Recently removed members (last 5 years of removals)
    SELECT DISTINCT "removedTicker" as symbol
    FROM historical_sp500_constituent
    WHERE "removedTicker" IS NOT NULL
      AND dateAddedEpoch > (EXTRACT(EPOCH FROM CURRENT_DATE) - 5 * 365.25 * 86400)
)
SELECT
    k.symbol,
    p.companyName,
    p.sector,
    f.priceToEarningsRatioTTM as pe_ratio,
    k.returnOnEquityTTM * 100 as roe_pct,
    f.debtToEquityRatioTTM as debt_to_equity,
    k.marketCap / 1e9 as market_cap_billions
FROM key_metrics_ttm k
JOIN financial_ratios_ttm f ON k.symbol = f.symbol
JOIN profile p ON k.symbol = p.symbol
JOIN pit_universe u ON k.symbol = u.symbol
WHERE f.priceToEarningsRatioTTM > 0
  AND f.priceToEarningsRatioTTM < 15
ORDER BY f.priceToEarningsRatioTTM ASC
LIMIT 100
"""


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
        line = " | ".join(
            format_value(c, row.get(c, "")).ljust(widths[c]) for c in columns
        )
        print(line)


def main():
    parser = argparse.ArgumentParser(
        description="S&P 500 survivorship bias stock screen"
    )
    parser.add_argument("--api-key", type=str,
                        help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--csv", action="store_true", help="Output as CSV")
    args = parser.parse_args()

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print("=" * 80)
    print("S&P 500 SURVIVORSHIP BIAS SCREEN")
    print("=" * 80)
    print("Filter: 0 < P/E < 15 (TTM), top 100 by lowest P/E")
    print()

    columns = ["symbol", "companyName", "sector", "pe_ratio",
                "roe_pct", "debt_to_equity", "market_cap_billions"]

    # --- Biased screen ---
    print("BIASED UNIVERSE (current S&P 500 members only)")
    print("-" * 80)

    if args.csv:
        biased = cr.query(BIASED_SQL, format="csv", verbose=True)
        print(biased)
        biased_symbols = set()
    else:
        biased = cr.query(BIASED_SQL, verbose=True)
        if not biased:
            print("No qualifying stocks found in biased universe.")
            biased_symbols = set()
        else:
            biased_symbols = set(r["symbol"] for r in biased)
            print(f"\n{len(biased)} stocks qualify:\n")
            format_table(biased, columns)

    print()

    # --- Unbiased screen ---
    print("UNBIASED UNIVERSE (current + recently removed S&P 500 members)")
    print("-" * 80)

    if args.csv:
        unbiased = cr.query(UNBIASED_SQL, format="csv", verbose=True)
        print(unbiased)
        return
    else:
        unbiased = cr.query(UNBIASED_SQL, verbose=True)
        if not unbiased:
            print("No qualifying stocks found in unbiased universe.")
            return

        unbiased_symbols = set(r["symbol"] for r in unbiased)
        print(f"\n{len(unbiased)} stocks qualify:\n")
        format_table(unbiased, columns)

    # --- Difference ---
    print()
    print("=" * 80)
    print("SURVIVORSHIP BIAS DIFFERENCE")
    print("=" * 80)

    only_unbiased = unbiased_symbols - biased_symbols
    only_biased = biased_symbols - unbiased_symbols

    if only_unbiased:
        print(f"\nStocks in UNBIASED screen but NOT in biased ({len(only_unbiased)}):")
        print("These are former S&P 500 members that a biased screen misses.\n")
        diff_rows = [r for r in unbiased if r["symbol"] in only_unbiased]
        diff_rows.sort(key=lambda r: float(r.get("pe_ratio", 999)))
        format_table(diff_rows, columns)
    else:
        print("\nNo difference: both universes produced identical screens.")

    if only_biased:
        print(f"\nStocks in BIASED screen but NOT in unbiased ({len(only_biased)}):")
        print("These are newer S&P 500 additions that displaced removed members.\n")
        diff_rows = [r for r in biased if r["symbol"] in only_biased]
        diff_rows.sort(key=lambda r: float(r.get("pe_ratio", 999)))
        format_table(diff_rows, columns)

    print(f"\nBiased: {len(biased_symbols)} stocks | "
          f"Unbiased: {len(unbiased_symbols)} stocks | "
          f"Overlap: {len(biased_symbols & unbiased_symbols)} stocks")
    print(f"Only in unbiased (missed by biased): {len(only_unbiased)}")
    print(f"Only in biased (new additions): {len(only_biased)}")


if __name__ == "__main__":
    main()
