#!/usr/bin/env python3
"""
Net Debt to EBITDA Screen - Current TTM Data

Screens for companies with low net leverage and strong profitability using current data.
Uses TTM tables (trailing twelve months) for real-time screening.

Usage:
    python3 net-debt-ebitda/screen.py                    # US (default)
    python3 net-debt-ebitda/screen.py --preset india     # India
    python3 net-debt-ebitda/screen.py --preset germany   # Germany
    python3 net-debt-ebitda/screen.py --global           # All exchanges
    python3 net-debt-ebitda/screen.py --cloud            # Cloud execution
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges

# Signal parameters (same as backtest)
NET_DEBT_EBITDA_MAX = 2.0
NET_DEBT_EBITDA_MIN = -5.0
ROE_MIN = 0.10
MKTCAP_MIN = 1_000_000_000
LIMIT = 30


def build_screen_sql(exchanges=None):
    """Build the TTM screening SQL query."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
SELECT
    k.symbol,
    p.companyName,
    p.exchange,
    ROUND(k.netDebtToEBITDATTM, 2) AS net_debt_ebitda,
    ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(k.marketCap / 1e9, 1) AS market_cap_bn
FROM key_metrics_ttm k
JOIN profile p ON k.symbol = p.symbol
WHERE k.netDebtToEBITDATTM < {NET_DEBT_EBITDA_MAX}
  AND k.netDebtToEBITDATTM > {NET_DEBT_EBITDA_MIN}
  AND k.returnOnEquityTTM > {ROE_MIN}
  AND k.marketCap > {MKTCAP_MIN}
  {exchange_clause}
ORDER BY k.netDebtToEBITDATTM ASC
LIMIT {LIMIT}
"""


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Net Debt/EBITDA screen (current TTM data)")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON instead of table")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)

    if args.cloud:
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        sql = build_screen_sql(exchanges)
        code = f"""
from cr_client import CetaResearch
cr = CetaResearch()
results = cr.query('''{sql}''')
for r in results:
    print(f"{{r['symbol']:<8}} {{(r.get('companyName') or '')[:30]:<32}} "
          f"{{r['net_debt_ebitda']:>8}} {{r['roe_pct']:>6}}% {{r['market_cap_bn']:>8}}B")
"""
        result = cr.execute_code(code, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    sql = build_screen_sql(exchanges)

    print(f"Net Debt/EBITDA Screen: {universe_name}")
    print(f"Signal: Net Debt/EBITDA < {NET_DEBT_EBITDA_MAX}x and > {NET_DEBT_EBITDA_MIN}x, "
          f"ROE > {ROE_MIN*100:.0f}%, MCap > ${MKTCAP_MIN/1e9:.0f}B")
    print(f"Top {LIMIT} by lowest Net Debt/EBITDA\n")

    results = cr.query(sql, timeout=120)

    if args.json:
        print(json.dumps(results, indent=2))
        return

    if not results:
        print("No stocks passed the screen.")
        return

    print(f"{'Symbol':<8} {'Company':<32} {'NetDebt/EBITDA':>14} {'ROE':>6} {'MCap($B)':>10}")
    print("-" * 76)
    for r in results:
        name = (r.get("companyName") or "")[:30]
        print(f"{r['symbol']:<8} {name:<32} {r['net_debt_ebitda']:>14} "
              f"{r['roe_pct']:>5}% {r['market_cap_bn']:>9}B")

    print(f"\n{len(results)} stocks passed ({universe_name})")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
