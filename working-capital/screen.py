#!/usr/bin/env python3
"""
Working Capital Efficiency - Current Stock Screen

Screens for stocks with low WC/Revenue, positive revenue growth, and quality metrics.
Uses TTM (trailing twelve months) and latest annual data for current screening.

Usage:
    python3 working-capital/screen.py
    python3 working-capital/screen.py --preset india
    python3 working-capital/screen.py --exchange XETRA
    python3 working-capital/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
WC_RATIO_MAX = 0.50
ROE_MIN = 0.08
OPM_MIN = 0.10
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold()
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using latest annual + TTM data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH bs_latest AS (
            SELECT symbol,
                (totalCurrentAssets - totalCurrentLiabilities) AS workingCapital,
                dateEpoch AS filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalCurrentAssets IS NOT NULL
              AND totalCurrentLiabilities IS NOT NULL
              AND totalCurrentAssets > totalCurrentLiabilities
        ),
        inc_current AS (
            SELECT symbol, revenue, dateEpoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue > 0
        ),
        inc_prior AS (
            SELECT symbol, revenue AS revenue_prior,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND revenue > 0
        )
        SELECT b.symbol, p.companyName, p.exchange, p.sector,
            ROUND(b.workingCapital / ic.revenue, 3) AS wc_to_revenue,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b,
            ROUND(ic.revenue / 1e9, 2) AS revenue_b,
            ROUND(b.workingCapital / 1e9, 3) AS working_capital_b
        FROM bs_latest b
        JOIN inc_current ic ON b.symbol = ic.symbol AND ic.rn = 1
        JOIN (SELECT symbol, revenue AS revenue_prior FROM inc_prior WHERE rn = 2) ip
            ON b.symbol = ip.symbol
        JOIN profile p ON b.symbol = p.symbol
        JOIN key_metrics_ttm k ON b.symbol = k.symbol
        JOIN financial_ratios_ttm f ON b.symbol = f.symbol
        WHERE b.rn = 1
          AND b.workingCapital > 0
          AND ic.revenue > 0
          AND b.workingCapital / ic.revenue < {WC_RATIO_MAX}
          AND ic.revenue > ip.revenue_prior
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND f.operatingProfitMarginTTM > {OPM_MIN}
          AND k.marketCap > {mktcap_min}
          AND (p.sector IS NULL OR p.sector NOT IN ('Financial Services', 'Financials'))
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Banks%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Insurance%')
          {exchange_filter}
        ORDER BY wc_to_revenue ASC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results


def main():
    parser = argparse.ArgumentParser(description="Working Capital Efficiency - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("working-capital", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Working Capital Efficiency Screen - {universe_name}")
    print(f"Signal: WC/Rev < {WC_RATIO_MAX*100:.0f}%, rev growth > 0, "
          f"ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'WC/Rev':>7} {'ROE%':>6} "
          f"{'OPM%':>6} {'MCap$B':>7} {'Rev$B':>7}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:26]:<28} "
              f"{r.get('wc_to_revenue', ''):>7} {r.get('roe_pct', ''):>6} "
              f"{r.get('opm_pct', ''):>6} {r.get('mktcap_b', ''):>7} "
              f"{r.get('revenue_b', ''):>7}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
