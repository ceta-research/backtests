#!/usr/bin/env python3
"""Current margin expansion stock screen using live TTM data.

Shows top stocks by operating profit margin with quality filters.
Note: True expansion signal requires multi-year FY data (see backtest.py).
TTM screen shows current margin levels only.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold
import argparse

def main():
    parser = argparse.ArgumentParser(description="Margin expansion stock screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    sql = f"""
    SELECT k.symbol, p.companyName, p.exchange,
        ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
        ROUND(f.grossProfitMarginTTM * 100, 2) AS gross_margin_pct,
        ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
        ROUND(k.marketCap / 1e9, 2) AS market_cap_b,
        p.sector
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm f ON k.symbol = f.symbol
    JOIN profile p ON k.symbol = p.symbol
    WHERE f.operatingProfitMarginTTM > 0.10
      AND f.grossProfitMarginTTM > 0.30
      AND k.returnOnEquityTTM > 0.10
      AND k.marketCap > {mktcap_min}
      AND COALESCE(p.sector, '') NOT IN ('Financial Services')
      {exchange_where}
    ORDER BY f.operatingProfitMarginTTM DESC
    LIMIT 50
    """

    print(f"Margin Expansion Screen: {universe_name}")
    print(f"Filter: OPM > 10%, Gross > 30%, ROE > 10%, MCap > {mktcap_min:,.0f}")
    print("=" * 110)

    results = cr.query(sql, verbose=args.verbose, timeout=120)
    if not results:
        print("No results.")
        return

    print(f"\n{'Symbol':<12} {'Company':<28} {'OPM%':>8} {'Gross%':>8} {'ROE%':>8} {'MCap($B)':>10} {'Sector':<20}")
    print("-" * 110)
    for r in results:
        print(f"{r['symbol']:<12} {r['companyName'][:26]:<28} {r['opm_pct']:>8} "
              f"{r['gross_margin_pct']:>8} {r['roe_pct']:>8} {r['market_cap_b']:>10} "
              f"{r.get('sector', '')[:18]:<20}")

    print(f"\n{len(results)} stocks found.")

if __name__ == "__main__":
    main()
