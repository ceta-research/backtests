#!/usr/bin/env python3
"""Volume-Confirmed Momentum screen — current qualifying stocks.

Screens for stocks showing 11-month price momentum (skip last month)
confirmed by rising 3-month vs 12-month average daily volume.

Quality filters (TTM):
  - Net profit margin > 0 (positive earnings)
  - Market cap > exchange threshold

Momentum: 11-month return (T-1month to T-12months), top stocks ranked descending.
Volume: 3-month avg daily volume > 12-month avg (rising trend, vol_ratio > 1.0).

The skip-last-month approach avoids the well-documented short-term reversal effect.
Volume confirmation filters out low-conviction momentum (thin-volume drift).

Usage:
    python3 volume-confirmed-momentum/screen.py                     # US (default)
    python3 volume-confirmed-momentum/screen.py --preset india
    python3 volume-confirmed-momentum/screen.py --exchange XETRA
    python3 volume-confirmed-momentum/screen.py --preset us --csv
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

# Screen SQL: skip-last-month momentum + volume ratio confirmation
# Uses TTM tables for quality filter, stock_eod for momentum and volume computation.
SCREEN_SQL_TEMPLATE = """
WITH quality AS (
    SELECT
        k.symbol,
        p.companyName,
        p.exchange,
        COALESCE(f.netProfitMarginTTM, 0) * 100    AS net_margin_pct,
        k.marketCap / 1e9                           AS market_cap_billions
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE f.netProfitMarginTTM > 0
      AND k.marketCap > {mktcap_min}
      {exchange_filter}
),
-- Skip-last-month: price ~1M ago as momentum start (avoids short-term reversal)
price_1m_ago AS (
    SELECT symbol, adjClose AS price_1m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) -
                   CAST(EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '30' DAY)) AS BIGINT))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '42' DAY
                   AND CURRENT_DATE - INTERVAL '18' DAY
      AND adjClose > 0
),
-- 12M lookback price: momentum denominator
price_12m_ago AS (
    SELECT symbol, adjClose AS price_12m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) -
                   CAST(EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '365' DAY)) AS BIGINT))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '395' DAY
                   AND CURRENT_DATE - INTERVAL '335' DAY
      AND adjClose > 0
),
-- Skip-last-month momentum = (price_1m - price_12m) / price_12m
momentum AS (
    SELECT p1.symbol,
           ROUND((p1.price_1m - p12.price_12m) / NULLIF(p12.price_12m, 0) * 100, 1)
               AS return_11m_pct
    FROM price_1m_ago p1
    JOIN price_12m_ago p12 ON p1.symbol = p12.symbol
    WHERE p1.rn = 1 AND p12.rn = 1 AND p12.price_12m > 0
),
-- Volume trend: avg daily volume over last 3M vs last 12M
volume_trend AS (
    SELECT symbol,
           AVG(CASE WHEN date >= CURRENT_DATE - INTERVAL '95' DAY
                     AND volume IS NOT NULL AND volume > 0
                    THEN volume END) AS avg_vol_3m,
           AVG(CASE WHEN volume IS NOT NULL AND volume > 0
                    THEN volume END)                      AS avg_vol_12m,
           COUNT(CASE WHEN volume IS NOT NULL AND volume > 0 THEN 1 END) AS n_days
    FROM stock_eod
    WHERE date >= CURRENT_DATE - INTERVAL '400' DAY
      AND date <= CURRENT_DATE
    GROUP BY symbol
    HAVING COUNT(CASE WHEN volume IS NOT NULL AND volume > 0 THEN 1 END) >= 60
       AND AVG(CASE WHEN volume IS NOT NULL AND volume > 0 THEN volume END) > 0
       AND AVG(CASE WHEN date >= CURRENT_DATE - INTERVAL '95' DAY
                     AND volume IS NOT NULL AND volume > 0
                    THEN volume END) IS NOT NULL
),
vol_confirmed AS (
    SELECT symbol,
           ROUND(avg_vol_3m / NULLIF(avg_vol_12m, 0), 2) AS vol_ratio
    FROM volume_trend
    WHERE avg_vol_3m > avg_vol_12m
)
SELECT
    q.symbol,
    q.companyName,
    q.exchange,
    ROUND(q.net_margin_pct, 1)        AS net_margin_pct,
    ROUND(q.market_cap_billions, 2)   AS market_cap_billions,
    m.return_11m_pct,
    v.vol_ratio
FROM quality q
JOIN momentum m ON q.symbol = m.symbol
JOIN vol_confirmed v ON q.symbol = v.symbol
WHERE m.return_11m_pct > 0
ORDER BY m.return_11m_pct DESC
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
    if col in ("net_margin_pct", "market_cap_billions", "return_11m_pct", "vol_ratio"):
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def main():
    parser = argparse.ArgumentParser(
        description="Volume-Confirmed Momentum stock screen (live)"
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
                        help="Show top N by momentum (default: 30)")
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

    print(f"Volume-Confirmed Momentum Screen: {label}")
    print(f"Filters: NetMargin>0, MCap>threshold, 11M return>0, 3M volume > 12M avg")
    print(f"Ranking: Top {args.top} by 11-month price return (skip last month)")
    print()

    if args.csv:
        result = cr.query(sql, format="csv")
        print(result)
        return

    results = cr.query(sql)
    if not results:
        print("No qualifying stocks found.")
        return

    shown = results[:args.top]

    columns = ["symbol", "companyName", "exchange", "net_margin_pct",
               "market_cap_billions", "return_11m_pct", "vol_ratio"]
    print(f"\n{len(results)} qualifying stocks. Showing top {len(shown)}:\n")
    format_table(shown, columns)
    print(f"\nTotal qualifying: {len(results)} stocks")
    print(f"Columns: net_margin_pct (%), market_cap_billions ($B), "
          f"return_11m_pct (%, skip-last-month), vol_ratio (3M/12M avg volume)")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nData: Ceta Research (FMP financial data warehouse)")


if __name__ == "__main__":
    main()
