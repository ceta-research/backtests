#!/usr/bin/env python3
"""FCF Compounders screen on current data.

Finds stocks that pass all filters using FY and TTM tables:
  1. FCF grew in >= 4 of last 5 FY years (positive YoY growth)
  2. All FCF positive (no negative FCF years)
  3. ROIC > 15% (TTM)
  4. Operating Margin > 15% (TTM)
  5. Market Cap > exchange-specific threshold

Returns top 30 by highest ROIC.

Usage:
    python3 fcf-compounders/screen.py
    python3 fcf-compounders/screen.py --preset india
    python3 fcf-compounders/screen.py --global

See README.md for strategy details.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

FCF_COMPOUNDERS_SQL_TEMPLATE = """
WITH yearly_fcf AS (
  SELECT
    symbol,
    freeCashFlow,
    date,
    LAG(freeCashFlow) OVER (PARTITION BY symbol ORDER BY date) AS prev_fcf
  FROM cash_flow_statement
  WHERE period = 'FY'
    AND freeCashFlow IS NOT NULL
),
fcf_stats AS (
  SELECT
    symbol,
    COUNT(*) AS total_pairs,
    SUM(CASE WHEN freeCashFlow > prev_fcf AND prev_fcf > 0 THEN 1 ELSE 0 END) AS growth_years,
    MIN(freeCashFlow) AS min_fcf,
    MIN(prev_fcf) AS min_prev_fcf
  FROM yearly_fcf
  WHERE prev_fcf IS NOT NULL
    AND date >= '2019-01-01'
  GROUP BY symbol
  HAVING COUNT(*) >= 4
)
SELECT
  fs.symbol,
  p.companyName,
  p.exchange,
  p.sector,
  fs.growth_years,
  fs.total_pairs AS total_years,
  ROUND(k.returnOnInvestedCapitalTTM * 100, 1) AS roic_pct,
  ROUND(r.operatingProfitMarginTTM * 100, 1) AS op_margin_pct,
  ROUND(k.freeCashFlowPerShareTTM, 2) AS fcf_per_share,
  ROUND(k.marketCap / 1e9, 1) AS market_cap_billions
FROM fcf_stats fs
JOIN key_metrics_ttm k ON fs.symbol = k.symbol
JOIN financial_ratios_ttm r ON fs.symbol = r.symbol
JOIN profile p ON fs.symbol = p.symbol
WHERE fs.growth_years >= 4
  AND fs.min_fcf > 0
  AND fs.min_prev_fcf > 0
  AND k.returnOnInvestedCapitalTTM > 0.15
  AND r.operatingProfitMarginTTM > 0.15
  AND k.marketCap > {mktcap_min}
  {exchange_filter}
ORDER BY k.returnOnInvestedCapitalTTM DESC
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
    if column in ("roic_pct", "op_margin_pct"):
        return f"{float(value):.1f}%"
    if column == "fcf_per_share":
        return f"${float(value):.2f}"
    if column == "market_cap_billions":
        return f"${float(value):.1f}B"
    if column in ("growth_years", "total_years"):
        return str(int(value))
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
    parser = argparse.ArgumentParser(description="FCF Compounders stock screen")
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
        result = run_screen_cloud("fcf-compounders", args_str=" ".join(cloud_args),
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
    sql = FCF_COMPOUNDERS_SQL_TEMPLATE.format(
        mktcap_min=mktcap_min,
        exchange_filter=exchange_filter
    )

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"FCF Compounders Screen: {label}")
    print(f"Filters: FCF grew 4+/5yr, all FCF>0, ROIC >15%, OPM >15%, MCap >{mktcap_min/1e9:.0f}B local")
    print(f"Selection: Top 30 by highest ROIC")
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
                   "growth_years", "total_years", "roic_pct",
                   "op_margin_pct", "fcf_per_share", "market_cap_billions"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
