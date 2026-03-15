#!/usr/bin/env python3
"""
Capex Efficiency Screen - Current Qualifying Stocks

Screens for capital-efficient companies with high ROIC using TTM data.
Returns companies spending < 8% of revenue on capex, < 40% of OCF on capex,
while earning > 15% ROIC and > 15% operating margin.

Usage:
    python3 capex-efficiency/screen.py
    python3 capex-efficiency/screen.py --preset india
    python3 capex-efficiency/screen.py --exchange BSE,NSE --limit 50
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (same as backtest)
CAPEX_TO_REV_MAX = 0.08
CAPEX_TO_OCF_MAX = 0.40
ROIC_MIN = 0.15
OPM_MIN = 0.15


def screen(client, exchanges, mktcap_min, limit=30, verbose=False):
    """Run current capex efficiency screen using TTM data."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    sql = f"""
        SELECT
            p.symbol,
            p.companyName,
            p.exchange,
            ROUND(k.capexToRevenueTTM * 100, 1) AS capex_to_rev_pct,
            ROUND(k.capexToOperatingCashFlowTTM * 100, 1) AS capex_to_ocf_pct,
            ROUND(k.capexToDepreciationTTM, 2) AS capex_to_depr,
            ROUND(k.returnOnInvestedCapitalTTM * 100, 1) AS roic_pct,
            ROUND(r.operatingProfitMarginTTM * 100, 1) AS op_margin_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_bn
        FROM key_metrics_ttm k
        JOIN financial_ratios_ttm r ON k.symbol = r.symbol
        JOIN profile p ON k.symbol = p.symbol
        WHERE k.capexToRevenueTTM > 0
          AND k.capexToRevenueTTM < {CAPEX_TO_REV_MAX}
          AND k.capexToOperatingCashFlowTTM > 0
          AND k.capexToOperatingCashFlowTTM < {CAPEX_TO_OCF_MAX}
          AND k.returnOnInvestedCapitalTTM > {ROIC_MIN}
          AND r.operatingProfitMarginTTM > {OPM_MIN}
          AND k.marketCap > {mktcap_min}
          {exchange_where}
        ORDER BY k.returnOnInvestedCapitalTTM DESC
        LIMIT {limit}
    """

    print(f"Screening for capex-efficient stocks...")
    print(f"  Capex/Revenue < {CAPEX_TO_REV_MAX*100}%")
    print(f"  Capex/OCF < {CAPEX_TO_OCF_MAX*100}%")
    print(f"  ROIC > {ROIC_MIN*100}%")
    print(f"  Operating Margin > {OPM_MIN*100}%")
    print(f"  Market Cap > {mktcap_min:,.0f}\n")

    results = client.query(sql, verbose=verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    # Print results
    print(f"{'Symbol':<10} {'Company':<30} {'Exch':<6} {'Cpx/Rev':<8} {'Cpx/OCF':<8} {'Cpx/Dep':<8} {'ROIC%':<7} {'OPM%':<7} {'MktCap':<10}")
    print("-" * 120)

    for row in results:
        print(f"{row['symbol']:<10} {row['companyName'][:29]:<30} {row['exchange']:<6} "
              f"{row['capex_to_rev_pct']:>7.1f}% {row['capex_to_ocf_pct']:>7.1f}% "
              f"{row['capex_to_depr']:>7.2f} {row['roic_pct']:>6.1f}% {row['op_margin_pct']:>6.1f}% "
              f"${row['mktcap_bn']:>8.2f}B")

    print(f"\n{len(results)} stocks qualify")


def main():
    parser = argparse.ArgumentParser(description="Capex Efficiency Screen (Current)")
    add_common_args(parser)
    parser.add_argument('--limit', type=int, default=30, help='Max stocks to return')
    args = parser.parse_args()

    client = CetaResearch()
    exchanges, exchange_label = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)

    print(f"\nCapex Efficiency Screen - {exchange_label}")
    print("=" * 60 + "\n")

    screen(client, exchanges, mktcap_min, args.limit, args.verbose)


if __name__ == "__main__":
    main()
