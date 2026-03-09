#!/usr/bin/env python3
"""
EV/EBITDA Value Screen - Current Stock Screen

Screens for undervalued stocks using TTM data: EV/EBITDA < 10, ROE > 10%,
market cap > exchange-specific threshold. Ranks by lowest EV/EBITDA.

Usage:
    python3 ev-ebitda/screen.py
    python3 ev-ebitda/screen.py --preset india
    python3 ev-ebitda/screen.py --exchange XETRA
    python3 ev-ebitda/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
EV_EBITDA_MAX = 10.0
EV_EBITDA_MIN = 0.0
ROE_MIN = 0.10
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        SELECT
            k.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(k.evToEBITDATTM, 2) AS ev_ebitda,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(k.enterpriseValueTTM / 1e9, 2) AS ev_bn,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM key_metrics_ttm k
        JOIN profile p ON k.symbol = p.symbol
        WHERE k.evToEBITDATTM > {EV_EBITDA_MIN}
          AND k.evToEBITDATTM < {EV_EBITDA_MAX}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.evToEBITDATTM ASC) = 1
        ORDER BY k.evToEBITDATTM ASC
        LIMIT {MAX_STOCKS}
    """

    results = client.query(sql, verbose=verbose)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="EV/EBITDA value live screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", dest="output_json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch as CR
        cr = CR(api_key=args.api_key, base_url=args.base_url)
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = cr.execute_code(
            f"python3 ev-ebitda/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"EV/EBITDA Value Screen | Universe: {universe_name}")
    print(f"Filters: EV/EBITDA {EV_EBITDA_MIN}-{EV_EBITDA_MAX}x, "
          f"ROE > {ROE_MIN*100:.0f}%, MCap > {mktcap_min/1e9:.1f}B local")
    print("=" * 90)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<30} {'EV/EBITDA':>10} {'ROE%':>6} "
          f"{'EV($B)':>8} {'MCap($B)':>10}")
    print("-" * 90)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:28]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<30} "
              f"{r.get('ev_ebitda', 'N/A'):>10} "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('ev_bn', 'N/A'):>8} "
              f"{r.get('mktcap_b', 'N/A'):>10}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), TTM metrics.")


if __name__ == "__main__":
    main()
