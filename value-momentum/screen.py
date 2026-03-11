#!/usr/bin/env python3
"""Value-Momentum screen — current qualifying stocks.

Uses TTM (trailing twelve months) tables for value filters, then ranks
by composite value+momentum score. Run against production data for a live signal.

Value filters (TTM):
  - P/E > 0 AND P/E < 20    (positive earnings, reasonable valuation)
  - ROE > 10%               (basic profitability)
  - Debt-to-Equity < 1.0    (not over-levered)
  - Market cap > exchange threshold

Composite ranking: average of P/E percentile rank (ASC) and 12M momentum (DESC).
The screen shows all qualifying stocks ranked by composite score. Top 30 =
portfolio picks in the backtest; adjust for your own strategy.

Academic basis:
  Asness, Moskowitz, Pedersen (2013). "Value and Momentum Everywhere."
  Journal of Finance, 68(3), 929-985.

Usage:
    python3 value-momentum/screen.py                     # US (default)
    python3 value-momentum/screen.py --preset india
    python3 value-momentum/screen.py --exchange XETRA
    python3 value-momentum/screen.py --global
    python3 value-momentum/screen.py --preset us --csv
    python3 value-momentum/screen.py --preset us --top 50
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

# Composite value+momentum screen using TTM tables.
# Momentum computed inline from stock_eod (current price vs ~365 days ago).
# The backtest uses point-in-time FY data with 45-day filing lag.
SCREEN_SQL_TEMPLATE = """
WITH value_stocks AS (
    SELECT
        k.symbol,
        p.companyName,
        p.exchange,
        f.priceToEarningsRatioTTM                         AS pe_ratio,
        k.returnOnEquityTTM * 100                         AS roe_pct,
        f.debtToEquityRatioTTM                            AS debt_to_equity,
        k.marketCap / 1e9                                 AS market_cap_billions
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE f.priceToEarningsRatioTTM > 0
      AND f.priceToEarningsRatioTTM < 20
      AND k.returnOnEquityTTM > 0.10
      AND f.debtToEquityRatioTTM >= 0
      AND f.debtToEquityRatioTTM < 1.0
      AND k.marketCap > {mktcap_min}
      {exchange_filter}
),
current_price AS (
    SELECT symbol, adjClose AS price_now, dateEpoch AS epoch_now,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM stock_eod
    WHERE date >= CURRENT_DATE - INTERVAL '15' DAY
      AND adjClose > 0
),
price_12m_ago AS (
    SELECT symbol, adjClose AS price_12m, dateEpoch AS epoch_12m,
           ROW_NUMBER() OVER (
               PARTITION BY symbol
               ORDER BY ABS(CAST(dateEpoch AS BIGINT) - CAST(
                   EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '365' DAY))::BIGINT AS BIGINT
               ))
           ) AS rn
    FROM stock_eod
    WHERE date BETWEEN CURRENT_DATE - INTERVAL '395' DAY AND CURRENT_DATE - INTERVAL '335' DAY
      AND adjClose > 0
),
momentum AS (
    SELECT c.symbol,
           ROUND((c.price_now - p12.price_12m) / p12.price_12m * 100, 1) AS return_12m_pct
    FROM current_price c
    JOIN price_12m_ago p12 ON c.symbol = p12.symbol
    WHERE c.rn = 1 AND p12.rn = 1
      AND p12.price_12m > 0
),
with_scores AS (
    SELECT
        v.symbol,
        v.companyName,
        v.exchange,
        v.pe_ratio,
        v.roe_pct,
        v.debt_to_equity,
        v.market_cap_billions,
        m.return_12m_pct,
        RANK() OVER (ORDER BY v.pe_ratio ASC) AS pe_rank,
        RANK() OVER (ORDER BY m.return_12m_pct DESC NULLS LAST) AS mom_rank,
        COUNT(*) OVER () AS total_n
    FROM value_stocks v
    LEFT JOIN momentum m ON v.symbol = m.symbol
    WHERE m.return_12m_pct IS NOT NULL
),
scored AS (
    SELECT *,
        ROUND(
            (
                (1.0 - CAST(pe_rank - 1 AS DOUBLE) / NULLIF(total_n - 1, 0)) +
                (1.0 - CAST(mom_rank - 1 AS DOUBLE) / NULLIF(total_n - 1, 0))
            ) / 2.0 * 100,
            1
        ) AS composite_score
    FROM with_scores
)
SELECT
    symbol,
    companyName,
    exchange,
    ROUND(pe_ratio, 1)            AS pe_ratio,
    ROUND(roe_pct, 1)             AS roe_pct,
    ROUND(debt_to_equity, 2)      AS debt_to_equity,
    ROUND(market_cap_billions, 2) AS market_cap_billions,
    return_12m_pct,
    composite_score
FROM scored
ORDER BY composite_score DESC NULLS LAST
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
    if col in ("pe_ratio", "roe_pct", "debt_to_equity",
               "market_cap_billions", "return_12m_pct", "composite_score"):
        return f"{float(value):.1f}"
    return str(value)


def main():
    parser = argparse.ArgumentParser(description="Value-Momentum stock screen (live)")
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
                        help="Show top N by composite score (default: 30)")
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

    print(f"Value-Momentum Screen: {label}")
    print(f"Filters: P/E 0-20, ROE>10%, D/E<1.0, MCap>threshold")
    print(f"Ranking: Top {args.top} by composite value+momentum score")
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

    columns = ["symbol", "companyName", "exchange", "pe_ratio", "roe_pct",
               "debt_to_equity", "market_cap_billions", "return_12m_pct", "composite_score"]
    print(f"\n{len(results)} total value stocks with momentum. "
          f"Showing top {len(shown)} by composite score:\n")
    format_table(shown, columns)
    print(f"\nTotal qualifying: {len(results)} stocks")
    if exchanges:
        print(f"Exchange filter: {', '.join(exchanges)}")
    print("\nComposite score: 0-100 (higher = better value + stronger momentum)")
    print("Data: Ceta Research (FMP financial data warehouse)")


if __name__ == "__main__":
    main()
