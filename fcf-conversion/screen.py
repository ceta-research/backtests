#!/usr/bin/env python3
"""
FCF Conversion Quality Screen

Shows current stocks passing the FCF conversion quality filter using TTM data.

Signal: FCF/NI > 100%, FCF/NI < 300%, FCF margin > 10%, ROE > 10%, OPM > 10%

Usage:
    # Screen US stocks (default)
    python3 fcf-conversion/screen.py

    # Screen Indian stocks
    python3 fcf-conversion/screen.py --preset india

    # Run on cloud
    python3 fcf-conversion/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

SQL_TEMPLATE = """
SELECT
  c.symbol,
  p.companyName,
  p.exchange,
  ROUND(c.freeCashFlow / NULLIF(i.netIncome, 0) * 100, 1) AS fcf_conversion_pct,
  ROUND(c.freeCashFlow / NULLIF(i.revenue, 0) * 100, 1) AS fcf_margin_pct,
  ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
  ROUND(r.operatingProfitMarginTTM * 100, 1) AS op_margin_pct,
  ROUND(k.marketCap / 1e9, 1) AS mktcap_bn
FROM cash_flow_statement_ttm c
JOIN income_statement_ttm i ON c.symbol = i.symbol
JOIN key_metrics_ttm k ON c.symbol = k.symbol
JOIN financial_ratios_ttm r ON c.symbol = r.symbol
JOIN profile p ON c.symbol = p.symbol
WHERE i.netIncome > 0
  AND c.freeCashFlow > 0
  AND i.revenue > 0
  AND c.freeCashFlow / NULLIF(i.netIncome, 0) > 1.0
  AND c.freeCashFlow / NULLIF(i.netIncome, 0) < 3.0
  AND c.freeCashFlow / NULLIF(i.revenue, 0) > 0.10
  AND k.returnOnEquityTTM > 0.10
  AND r.operatingProfitMarginTTM > 0.10
  AND k.marketCap > {mktcap_min}
  {exchange_filter}
ORDER BY c.freeCashFlow / NULLIF(i.netIncome, 0) DESC
LIMIT 30
"""


def main():
    parser = argparse.ArgumentParser(description="FCF Conversion Quality screen (TTM)")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)

    # Build exchange filter
    if exchanges:
        ex_list = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_list})"
    else:
        exchange_filter = ""

    sql = SQL_TEMPLATE.format(mktcap_min=mktcap_min, exchange_filter=exchange_filter)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if args.cloud:
        code = f"""
from cr_client import CetaResearch
cr = CetaResearch()
results = cr.query('''{sql}''')
if results:
    print(f"{{len(results)}} stocks pass FCF Conversion screen")
    print(f"{{'':<8}} {{'Company':<30}} {{'Exch':<8}} {{'Conv%':>8}} {{'FCFMgn%':>8}} {{'ROE%':>8}} {{'OPM%':>8}} {{'MCap$B':>8}}")
    print("-" * 100)
    for r in results:
        print(f"{{r['symbol']:<8}} {{r['companyName'][:28]:<30}} {{r['exchange']:<8}} "
              f"{{r['fcf_conversion_pct']:>8}} {{r['fcf_margin_pct']:>8}} "
              f"{{r['roe_pct']:>8}} {{r['op_margin_pct']:>8}} {{r['mktcap_bn']:>8}}")
else:
    print("No results")
"""
        result = cr.execute_code(code)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    print(f"FCF Conversion Quality Screen: {universe_name}")
    print(f"Signal: FCF/NI 100-300%, FCF margin > 10%, ROE > 10%, OPM > 10%")
    print(f"Market cap > {mktcap_min/1e9:.0f}B local currency")
    print("=" * 100)

    results = cr.query(sql, verbose=args.verbose)
    if not results:
        print("No stocks pass the screen.")
        return

    print(f"\n{len(results)} stocks pass the screen:\n")
    print(f"{'Symbol':<8} {'Company':<30} {'Exch':<8} {'Conv%':>8} {'FCFMgn%':>8} {'ROE%':>8} {'OPM%':>8} {'MCap$B':>8}")
    print("-" * 100)
    for r in results:
        print(f"{r['symbol']:<8} {str(r.get('companyName', ''))[:28]:<30} {r.get('exchange', ''):<8} "
              f"{r.get('fcf_conversion_pct', ''):>8} {r.get('fcf_margin_pct', ''):>8} "
              f"{r.get('roe_pct', ''):>8} {r.get('op_margin_pct', ''):>8} {r.get('mktcap_bn', ''):>8}")


if __name__ == "__main__":
    main()
