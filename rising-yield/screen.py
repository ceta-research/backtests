#!/usr/bin/env python3
"""Rising Dividend Yield screen on current data.

Finds stocks with 3 consecutive fiscal years of rising dividend yield,
driven by dividend growth (not price decline), with quality filters.

Signal:
  - dividendYield increased across 3 consecutive FY periods
  - DPS also increased (bullish driver)
  - ROE > 10%, Payout < 75%, Market Cap > exchange threshold

Usage:
    python3 rising-yield/screen.py
    python3 rising-yield/screen.py --preset india
    python3 rising-yield/screen.py --exchange XETRA
    python3 rising-yield/screen.py --global
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import get_mktcap_threshold

SCREEN_SQL_TEMPLATE = """
WITH yearly_yield AS (
    SELECT r.symbol, r.date,
        CAST(r.fiscalYear AS INTEGER) AS yr,
        r.dividendYield,
        r.dividendPerShare,
        r.dividendPayoutRatio,
        LAG(r.dividendYield, 1) OVER (PARTITION BY r.symbol ORDER BY r.fiscalYear) AS yield_1yr,
        LAG(r.dividendYield, 2) OVER (PARTITION BY r.symbol ORDER BY r.fiscalYear) AS yield_2yr,
        LAG(r.dividendPerShare, 1) OVER (PARTITION BY r.symbol ORDER BY r.fiscalYear) AS dps_1yr,
        LAG(r.dividendPerShare, 2) OVER (PARTITION BY r.symbol ORDER BY r.fiscalYear) AS dps_2yr
    FROM financial_ratios r
    WHERE r.period = 'FY'
      AND r.dividendYield IS NOT NULL
      AND r.dividendYield > 0
),
screened AS (
    SELECT y.symbol, y.date, y.yr,
        y.dividendYield,
        y.dividendPerShare,
        y.dividendPayoutRatio,
        (y.dividendYield - y.yield_2yr) AS yield_change_2yr,
        CASE
            WHEN y.dividendPerShare > y.dps_1yr AND y.dps_1yr > y.dps_2yr
                THEN 'Consecutive DPS Growth'
            WHEN y.dividendPerShare > y.dps_1yr
                THEN 'Partial DPS Growth'
            ELSE 'Price Decline Driver'
        END AS yield_driver
    FROM yearly_yield y
    WHERE y.yield_2yr IS NOT NULL
      AND y.dividendYield > y.yield_1yr
      AND y.yield_1yr > y.yield_2yr
      AND y.dividendPerShare > y.dps_1yr
      AND y.dividendPayoutRatio > 0
      AND y.dividendPayoutRatio < 0.75
    QUALIFY ROW_NUMBER() OVER (PARTITION BY y.symbol ORDER BY y.date DESC) = 1
)
SELECT
    s.symbol,
    p.companyName,
    p.exchange,
    ROUND(s.dividendYield * 100, 2) AS current_yield_pct,
    ROUND(s.yield_change_2yr * 100, 2) AS yield_expansion_2yr_pct,
    s.yield_driver,
    ROUND(s.dividendPayoutRatio * 100, 1) AS payout_pct,
    ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(k.marketCap / 1e9, 1) AS market_cap_b
FROM screened s
JOIN key_metrics_ttm k ON s.symbol = k.symbol
JOIN profile p ON s.symbol = p.symbol
WHERE k.returnOnEquityTTM > 0.10
  AND k.marketCap > {mktcap_threshold}
  {exchange_filter}
ORDER BY s.yield_change_2yr DESC
LIMIT 30
"""

EXCHANGE_PRESETS = {
    "us": ("NYSE", "NASDAQ", "AMEX"),
    "india": ("NSE",),
    "germany": ("XETRA",),
    "canada": ("TSX",),
    "japan": ("JPX",),
    "uk": ("LSE",),
    "china": ("SHZ", "SHH"),
    "hongkong": ("HKSE",),
    "korea": ("KSC",),
    "australia": ("ASX",),
    "switzerland": ("SIX",),
    "singapore": ("SES",),
    "sweden": ("STO",),
    "taiwan": ("TAI",),
    "brazil": ("SAO",),
    "southafrica": ("JNB",),
}


def build_exchange_filter(exchanges):
    if not exchanges:
        return ""
    quoted = ", ".join(f"'{e}'" for e in exchanges)
    return f"AND p.exchange IN ({quoted})"


def format_table(rows, columns):
    if not rows:
        print("No qualifying stocks found.")
        return

    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            val = row.get(c, "")
            widths[c] = max(widths[c], len(str(val) if val is not None else "-"))

    header = " | ".join(c.ljust(widths[c]) for c in columns)
    separator = "-+-".join("-" * widths[c] for c in columns)
    print(header)
    print(separator)

    for row in rows:
        vals = []
        for c in columns:
            v = row.get(c, "")
            vals.append(str(v if v is not None else "-").ljust(widths[c]))
        print(" | ".join(vals))


def main():
    parser = argparse.ArgumentParser(description="Rising Dividend Yield stock screen")
    parser.add_argument("--exchange", type=str, help="Exchange(s), comma-separated")
    parser.add_argument("--preset", type=str, choices=EXCHANGE_PRESETS.keys(),
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges")
    parser.add_argument("--api-key", type=str, help="API key")
    parser.add_argument("--csv", action="store_true", help="Output as CSV")
    args = parser.parse_args()

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

    mktcap = get_mktcap_threshold(list(exchanges) if exchanges else ["NYSE"])
    exchange_filter = build_exchange_filter(exchanges)
    sql = SCREEN_SQL_TEMPLATE.format(
        mktcap_threshold=mktcap,
        exchange_filter=exchange_filter
    )

    cr = CetaResearch(api_key=args.api_key)

    print(f"Rising Dividend Yield Screen: {label}")
    print(f"Signal: 3yr rising yield, DPS growth driver, ROE > 10%, Payout < 75%")
    print()

    if args.csv:
        results = cr.query(sql, format="csv", verbose=True)
        print(results)
    else:
        results = cr.query(sql, verbose=True)
        if not results:
            print("No qualifying stocks found.")
            return

        columns = ["symbol", "companyName", "exchange", "current_yield_pct",
                    "yield_expansion_2yr_pct", "yield_driver", "payout_pct",
                    "roe_pct", "market_cap_b"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")


if __name__ == "__main__":
    main()
