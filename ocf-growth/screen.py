#!/usr/bin/env python3
"""OCF Growth / Cash Flow Momentum screen on current data.

Finds stocks with strong operating cash flow momentum and positive
earnings-cash flow divergence using pre-computed growth + TTM tables:
  1. OCF Growth > 10% (latest FY)
  2. OCF Growth > NI Growth (positive divergence)
  3. Return on Equity > 10% (TTM)
  4. Operating Profit Margin > 5% (TTM)
  5. Market Cap > exchange-specific threshold

Returns top 30 by highest divergence (OCF growth - NI growth).

Usage:
    python3 ocf-growth/screen.py
    python3 ocf-growth/screen.py --preset india
    python3 ocf-growth/screen.py --global

See README.md for strategy details.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold

OCF_SCREEN_SQL_TEMPLATE = """
WITH latest_cg AS (
    SELECT symbol, growthOperatingCashFlow, growthFreeCashFlow, dateEpoch,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM cash_flow_statement_growth
    WHERE period = 'FY' AND growthOperatingCashFlow IS NOT NULL
),
latest_ig AS (
    SELECT symbol, growthNetIncome, dateEpoch,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM income_statement_growth
    WHERE period = 'FY' AND growthNetIncome IS NOT NULL
)
SELECT
    cg.symbol,
    p.companyName,
    p.exchange,
    p.sector,
    ROUND(cg.growthOperatingCashFlow * 100, 1) AS ocf_growth_pct,
    ROUND(ig.growthNetIncome * 100, 1) AS ni_growth_pct,
    ROUND((cg.growthOperatingCashFlow - ig.growthNetIncome) * 100, 1) AS divergence_pct,
    ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(f.operatingProfitMarginTTM * 100, 1) AS op_margin_pct,
    ROUND(k.marketCap / 1e9, 1) AS market_cap_billions
FROM latest_cg cg
JOIN latest_ig ig ON cg.symbol = ig.symbol AND ig.rn = 1
JOIN key_metrics_ttm k ON cg.symbol = k.symbol
JOIN financial_ratios_ttm f ON cg.symbol = f.symbol
JOIN profile p ON cg.symbol = p.symbol
WHERE cg.rn = 1
  AND cg.growthOperatingCashFlow > 0.10
  AND cg.growthOperatingCashFlow < 5.0
  AND cg.growthOperatingCashFlow > ig.growthNetIncome
  AND k.returnOnEquityTTM > 0.10
  AND f.operatingProfitMarginTTM > 0.05
  AND k.marketCap > {mktcap_min}
  {exchange_filter}
ORDER BY (cg.growthOperatingCashFlow - ig.growthNetIncome) DESC
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
    if column in ("ocf_growth_pct", "ni_growth_pct", "divergence_pct", "roe_pct", "op_margin_pct"):
        return f"{float(value):.1f}%"
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
    parser = argparse.ArgumentParser(description="OCF Growth / Cash Flow Momentum stock screen")
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
        result = run_screen_cloud("ocf-growth", args_str=" ".join(cloud_args),
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
    sql = OCF_SCREEN_SQL_TEMPLATE.format(
        mktcap_min=mktcap_min,
        exchange_filter=exchange_filter
    )

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"OCF Growth Screen: {label}")
    print(f"Filters: OCF Growth > 10%, Divergence > 0, ROE > 10%, OPM > 5%, MCap > {mktcap_min/1e9:.0f}B local")
    print(f"Selection: Top 30 by highest divergence (OCF growth - NI growth)")
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
                   "ocf_growth_pct", "ni_growth_pct", "divergence_pct",
                   "roe_pct", "op_margin_pct", "market_cap_billions"]
        print(f"\n{len(results)} stocks qualify:\n")
        format_table(results, columns)
        print(f"\nTotal: {len(results)} stocks")
        if exchanges:
            print(f"Exchange filter: {', '.join(exchanges)}")


if __name__ == "__main__":
    main()
