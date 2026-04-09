#!/usr/bin/env python3
"""
Deleveraging Screen - Current FY Data

Screens for companies actively reducing debt year-over-year using the most recent
annual filings. Shows companies where D/E ratio dropped 10%+ compared to prior year.

Uses annual filing data (not TTM) because deleveraging is measured YoY, not trailing.

Usage:
    python3 deleveraging/screen.py                    # US (default)
    python3 deleveraging/screen.py --preset india     # India
    python3 deleveraging/screen.py --preset germany   # Germany
    python3 deleveraging/screen.py --global           # All exchanges
    python3 deleveraging/screen.py --cloud            # Cloud execution
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (same as backtest)
DE_CHANGE_THRESHOLD = -0.10   # D/E must drop at least 10% YoY
DE_PRIOR_MIN = 0.1            # Prior D/E must be meaningful
DE_CURRENT_MIN = 0.01         # Excludes zero D/E (FMP FY2012 data errors)
ROE_MIN = 0.08                # ROE > 8%
LIMIT = 30


def build_screen_sql(exchanges=None, mktcap_min=1_000_000_000):
    """Build the FY screening SQL query (YoY D/E comparison)."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
WITH current_fy AS (
    SELECT symbol, debtToEquityRatio AS de_current, date AS current_date
    FROM financial_ratios
    WHERE period = 'FY'
      AND debtToEquityRatio IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 18 MONTH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),
prior_fy AS (
    SELECT symbol, debtToEquityRatio AS de_prior, date AS prior_date
    FROM financial_ratios
    WHERE period = 'FY'
      AND debtToEquityRatio IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 30 MONTH
      AND date < CURRENT_DATE - INTERVAL 12 MONTH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),
km AS (
    SELECT symbol, returnOnEquityTTM AS roe, marketCap
    FROM key_metrics_ttm
)
SELECT
    c.symbol,
    p.companyName,
    p.exchange,
    p.sector,
    ROUND(c.de_current, 2) AS de_current,
    ROUND(pr.de_prior, 2) AS de_prior,
    ROUND((c.de_current - pr.de_prior) / pr.de_prior * 100, 1) AS de_change_pct,
    ROUND(k.roe * 100, 1) AS roe_pct,
    ROUND(k.marketCap / 1e9, 2) AS market_cap_bn,
    c.current_date
FROM current_fy c
JOIN prior_fy pr ON c.symbol = pr.symbol
JOIN km k ON c.symbol = k.symbol
JOIN profile p ON c.symbol = p.symbol
WHERE pr.de_prior > {DE_PRIOR_MIN}
  AND c.de_current > {DE_CURRENT_MIN}
  AND (c.de_current - pr.de_prior) / pr.de_prior < {DE_CHANGE_THRESHOLD}
  AND k.roe > {ROE_MIN}
  AND k.marketCap > {mktcap_min}
  {exchange_clause}
ORDER BY (c.de_current - pr.de_prior) / pr.de_prior ASC
LIMIT {LIMIT}
"""


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Deleveraging screen (current FY data)")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON instead of table")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"

    if args.cloud:
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        sql = build_screen_sql(exchanges, mktcap_threshold)
        code = f"""
from cr_client import CetaResearch
cr = CetaResearch()
results = cr.query('''{sql}''')
for r in results:
    print(f"{{r['symbol']:<8}} {{str(r.get('companyName',''))[:28]:<30}} "
          f"{{r['de_change_pct']:>7.1f}%  {{r['de_prior']:>5}}→{{r['de_current']:>5}} "
          f"ROE={{r['roe_pct']:>5}}% MCap={{r['market_cap_bn']:>7}}B")
"""
        result = cr.execute_code(code, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    sql = build_screen_sql(exchanges, mktcap_threshold)

    print(f"Deleveraging Screen: {universe_name}")
    print(f"Signal: D/E YoY change < {DE_CHANGE_THRESHOLD*100:.0f}%, prior D/E > {DE_PRIOR_MIN}, "
          f"ROE > {ROE_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print(f"Top {LIMIT} by largest D/E reduction\n")

    results = cr.query(sql, timeout=120)

    if args.json:
        print(json.dumps(results, indent=2))
        return

    if not results:
        print("No stocks passed the screen.")
        return

    print(f"{'Symbol':<8} {'Company':<30} {'D/E Chg':>9} {'Prior→Curr':>12} "
          f"{'ROE':>7} {'MCap($B)':>10}")
    print("-" * 80)
    for r in results:
        name = (r.get("companyName") or "")[:28]
        de_prior = r.get("de_prior", 0)
        de_curr = r.get("de_current", 0)
        chg = r.get("de_change_pct", 0)
        print(f"{r['symbol']:<8} {name:<30} {chg:>8.1f}%  "
              f"{de_prior:>5.2f}→{de_curr:>5.2f}  "
              f"{r['roe_pct']:>5.1f}%  {r['market_cap_bn']:>8.2f}B")

    print(f"\n{len(results)} stocks passed ({universe_name})")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
