#!/usr/bin/env python3
"""Sector-Adjusted Momentum (Relative Strength) screen — current qualifying stocks.

Computes each stock's 12-1 month return minus its sector's equal-weighted average
12-1 month return. Isolates stock-level momentum from sector-wide trends.

Signal: (stock 12M-1M return) - (sector avg 12M-1M return)
Universe: Market cap > exchange threshold (from key_metrics_ttm), actively trading.
Sector: From profile.sector (GICS).

Usage:
    python3 relative-strength/screen.py                     # US (default)
    python3 relative-strength/screen.py --preset india
    python3 relative-strength/screen.py --exchange XETRA
    python3 relative-strength/screen.py --preset us --top 50
    python3 relative-strength/screen.py --preset us --csv
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
        p.sector,
        k.marketCap / 1e9 AS market_cap_billions
    FROM profile p
    JOIN key_metrics_ttm k ON p.symbol = k.symbol
    WHERE k.marketCap > {mktcap_min}
      AND p.isActivelyTrading = true
      AND p.sector IS NOT NULL AND p.sector != ''
      {exchange_filter}
),
price_now AS (
    SELECT symbol, adjClose AS price_now,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM stock_eod
    WHERE date >= CURRENT_DATE - INTERVAL '15' DAY
      AND adjClose > 0
),
price_12m_ago AS (
    SELECT symbol, adjClose AS price_12m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) - CAST(
                   EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '365' DAY))::BIGINT AS BIGINT
               ))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '395' DAY
                   AND CURRENT_DATE - INTERVAL '335' DAY
      AND adjClose > 0
),
price_1m_ago AS (
    SELECT symbol, adjClose AS price_1m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) - CAST(
                   EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '30' DAY))::BIGINT AS BIGINT
               ))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '45' DAY
                   AND CURRENT_DATE - INTERVAL '15' DAY
      AND adjClose > 0
),
raw_momentum AS (
    SELECT
        u.symbol,
        u.companyName,
        u.exchange,
        u.sector,
        u.market_cap_billions,
        ROUND((p1m.price_1m - p12.price_12m) / p12.price_12m * 100, 1) AS raw_mom_12m_1m_pct,
        ROUND((pn.price_now - p1m.price_1m) / p1m.price_1m * 100, 1)   AS return_1m_pct
    FROM universe u
    JOIN price_12m_ago p12 ON u.symbol = p12.symbol AND p12.rn = 1
    JOIN price_1m_ago p1m  ON u.symbol = p1m.symbol  AND p1m.rn = 1
    JOIN price_now pn       ON u.symbol = pn.symbol   AND pn.rn = 1
    WHERE p12.price_12m > 1.0
      AND p1m.price_1m > 1.0
),
sector_avg AS (
    SELECT
        sector,
        COUNT(*) AS sector_count,
        AVG(raw_mom_12m_1m_pct) AS sector_avg_mom
    FROM raw_momentum
    GROUP BY sector
    HAVING COUNT(*) >= 5
)
SELECT
    m.symbol,
    m.companyName,
    m.exchange,
    m.sector,
    ROUND(m.market_cap_billions, 2)  AS market_cap_billions,
    m.raw_mom_12m_1m_pct,
    ROUND(s.sector_avg_mom, 1)       AS sector_avg_pct,
    ROUND(m.raw_mom_12m_1m_pct - s.sector_avg_mom, 1) AS relative_strength_pct,
    m.return_1m_pct,
    s.sector_count
FROM raw_momentum m
JOIN sector_avg s ON m.sector = s.sector
ORDER BY relative_strength_pct DESC NULLS LAST
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
    if col in ("market_cap_billions", "raw_mom_12m_1m_pct", "sector_avg_pct",
               "relative_strength_pct", "return_1m_pct"):
        return f"{float(value):.1f}"
    if col == "sector_count":
        return str(int(float(value)))
    return str(value)


def main():
    parser = argparse.ArgumentParser(
        description="Sector-Adjusted Momentum (Relative Strength) stock screen (live)"
    )
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
                        help="Show top N by relative strength (default: 30)")
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

    print(f"Sector-Adjusted Momentum (Relative Strength) Screen: {label}")
    print(f"Universe: MCap > threshold, Price > $1, Sector known")
    print(f"Signal: 12M-1M return minus sector average (min 5 stocks per sector)")
    print(f"Showing top {args.top} by relative strength\n")

    if args.csv:
        result = cr.query(sql, format="csv")
        print(result)
        return

    results = cr.query(sql)
    if not results:
        print("No qualifying stocks found.")
        return

    shown = results[:args.top]

    columns = ["symbol", "companyName", "sector", "market_cap_billions",
               "raw_mom_12m_1m_pct", "sector_avg_pct", "relative_strength_pct", "return_1m_pct"]
    print(f"{len(results)} stocks in universe. Top {len(shown)} by relative strength:\n")
    format_table(shown, columns)

    # Sector summary
    sector_counts = {}
    for r in shown:
        s = r.get("sector", "Unknown")
        sector_counts[s] = sector_counts.get(s, 0) + 1
    print(f"\nSector breakdown (top {len(shown)}):")
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        pct = count * 100 / len(shown)
        print(f"  {sector:<35} {count:>3} ({pct:.0f}%)")

    print(f"\nTotal universe: {len(results)} stocks")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nData: Ceta Research (FMP financial data warehouse)")
    print("Note: Relative strength = stock 12M-1M return minus sector equal-weighted average")


if __name__ == "__main__":
    main()
