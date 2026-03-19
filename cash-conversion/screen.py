#!/usr/bin/env python3
"""Current CCC stock screen using live TTM data."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold
import argparse

def main():
    parser = argparse.ArgumentParser(description="CCC stock screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Build exchange filter
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    sql = f"""
    SELECT k.symbol, p.companyName, p.exchange,
        ROUND(k.cashConversionCycleTTM, 1) AS ccc_days,
        ROUND(k.daysOfSalesOutstandingTTM, 1) AS dso,
        ROUND(k.daysOfInventoryOutstandingTTM, 1) AS dio,
        ROUND(k.daysOfPayablesOutstandingTTM, 1) AS dpo,
        ROUND(k.marketCap / 1e9, 2) AS market_cap_b
    FROM key_metrics_ttm k
    JOIN profile p ON k.symbol = p.symbol
    WHERE k.cashConversionCycleTTM < 30
      AND k.marketCap > {mktcap_min}
      AND COALESCE(p.sector, '') NOT IN ('Financial Services')
      {exchange_where}
    ORDER BY k.cashConversionCycleTTM ASC
    LIMIT 50
    """

    print(f"CCC Screen: {universe_name}")
    print(f"Filter: CCC < 30 days, MCap > {mktcap_min:,.0f}")
    print("=" * 80)

    results = cr.query(sql, verbose=args.verbose, timeout=120)
    if not results:
        print("No results.")
        return

    print(f"\n{'Symbol':<12} {'Company':<30} {'CCC':>8} {'DSO':>8} {'DIO':>8} {'DPO':>8} {'MCap($B)':>10}")
    print("-" * 94)
    for r in results:
        print(f"{r['symbol']:<12} {r['companyName'][:28]:<30} {r['ccc_days']:>8} {r['dso']:>8} {r['dio']:>8} {r['dpo']:>8} {r['market_cap_b']:>10}")

    print(f"\n{len(results)} stocks found.")

if __name__ == "__main__":
    main()
