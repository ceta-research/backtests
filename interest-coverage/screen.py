#!/usr/bin/env python3
"""
Interest Coverage Screen - Current TTM Data

Screens for companies with strong debt-servicing ability using current data.
Uses TTM tables (trailing twelve months) for real-time screening.

Usage:
    python3 interest-coverage/screen.py                    # US (default)
    python3 interest-coverage/screen.py --preset india     # India
    python3 interest-coverage/screen.py --preset germany   # Germany
    python3 interest-coverage/screen.py --global           # All exchanges
    python3 interest-coverage/screen.py --cloud            # Cloud execution
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (same as backtest)
COVERAGE_MIN = 5.0
DE_MIN = 0.0
DE_MAX = 1.5
ROE_MIN = 0.08
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold()
LIMIT = 30


def build_screen_sql(exchanges=None, mktcap_min=1_000_000_000):
    """Build the TTM screening SQL query."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
SELECT
    r.symbol,
    p.companyName,
    p.exchange,
    ROUND(r.interestCoverageRatioTTM, 1) AS coverage,
    ROUND(r.debtToEquityRatioTTM, 2) AS debt_to_equity,
    ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(k.marketCap / 1e9, 1) AS market_cap_bn
FROM financial_ratios_ttm r
JOIN key_metrics_ttm k ON r.symbol = k.symbol
JOIN profile p ON r.symbol = p.symbol
WHERE r.interestCoverageRatioTTM > {COVERAGE_MIN}
  AND r.debtToEquityRatioTTM >= {DE_MIN}
  AND r.debtToEquityRatioTTM < {DE_MAX}
  AND k.returnOnEquityTTM > {ROE_MIN}
  AND k.marketCap > {mktcap_min}
  {exchange_clause}
ORDER BY r.interestCoverageRatioTTM DESC
LIMIT {LIMIT}
"""


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Interest Coverage screen (current TTM data)")
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
    print(f"{{r['symbol']:<8}} {{r.get('companyName','')[:30]:<32}} "
          f"{{r['coverage']:>8}} {{r['debt_to_equity']:>6}} "
          f"{{r['roe_pct']:>6}}% {{r['market_cap_bn']:>8}}B")
"""
        result = cr.execute_code(code, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    sql = build_screen_sql(exchanges, mktcap_threshold)

    print(f"Interest Coverage Screen: {universe_name}")
    print(f"Signal: Coverage > {COVERAGE_MIN}, D/E {DE_MIN}-{DE_MAX}, "
          f"ROE > {ROE_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print(f"Top {LIMIT} by highest coverage\n")

    results = cr.query(sql, timeout=120)

    if args.json:
        print(json.dumps(results, indent=2))
        return

    if not results:
        print("No stocks passed the screen.")
        return

    print(f"{'Symbol':<8} {'Company':<32} {'Cov':>8} {'D/E':>6} {'ROE':>6} {'MCap($B)':>10}")
    print("-" * 72)
    for r in results:
        name = (r.get("companyName") or "")[:30]
        print(f"{r['symbol']:<8} {name:<32} {r['coverage']:>8} {r['debt_to_equity']:>6} "
              f"{r['roe_pct']:>5}% {r['market_cap_bn']:>9}B")

    print(f"\n{len(results)} stocks passed ({universe_name})")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
