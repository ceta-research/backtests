#!/usr/bin/env python3
"""
Compounding Equity Screen - Current Stock Screen

Screens for companies with 5-year shareholders' equity CAGR > 10%,
combined with ROE and operating margin quality overlays.

Uses the most recent available FY data for live screening.

Usage:
    python3 equity-growth/screen.py
    python3 equity-growth/screen.py --preset india
    python3 equity-growth/screen.py --exchange XETRA
    python3 equity-growth/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
EQUITY_CAGR_MIN = 0.10
EQUITY_CAGR_MAX = 1.00
EQUITY_YEARS_MIN = 3.5
EQUITY_YEARS_MAX = 7.0
ROE_MIN = 0.08
OPM_MIN = 0.08
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using most recent FY data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    # Use the most recent FY filing as t=0 and the one closest to 5 years prior
    sql = f"""
        WITH curr_eq AS (
            SELECT symbol, totalStockholdersEquity AS eq_curr, dateEpoch AS epoch_curr,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM balance_sheet
            WHERE period = 'FY' AND totalStockholdersEquity > 0
        ),
        prior_5yr AS (
            SELECT c.symbol,
                b.totalStockholdersEquity AS eq_prior,
                b.dateEpoch AS epoch_prior,
                c.eq_curr,
                c.epoch_curr,
                (c.epoch_curr - b.dateEpoch) / 31536000.0 AS years_gap,
                POWER(
                    c.eq_curr / b.totalStockholdersEquity,
                    1.0 / ((c.epoch_curr - b.dateEpoch) / 31536000.0)
                ) - 1 AS eq_cagr,
                ROW_NUMBER() OVER (
                    PARTITION BY c.symbol
                    ORDER BY ABS((c.epoch_curr - b.dateEpoch) / 31536000.0 - 5) ASC
                ) AS best_match
            FROM curr_eq c
            JOIN balance_sheet b ON c.symbol = b.symbol
                AND c.rn = 1
                AND b.period = 'FY'
                AND b.totalStockholdersEquity > 0
                AND b.dateEpoch < c.epoch_curr - 4 * 31536000
                AND b.dateEpoch > c.epoch_curr - 7 * 31536000
        )
        SELECT
            pr.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            p.industry,
            ROUND(pr.eq_cagr * 100, 2) AS eq_cagr_pct,
            ROUND(pr.years_gap, 1) AS years_measured,
            ROUND(pr.eq_curr / 1e9, 2) AS equity_curr_b,
            ROUND(pr.eq_prior / 1e9, 2) AS equity_prior_b,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(f.operatingProfitMarginTTM * 100, 2) AS opm_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM prior_5yr pr
        JOIN profile p ON pr.symbol = p.symbol
        JOIN key_metrics_ttm k ON pr.symbol = k.symbol
        JOIN financial_ratios_ttm f ON pr.symbol = f.symbol
        WHERE pr.best_match = 1
          AND pr.years_gap BETWEEN {EQUITY_YEARS_MIN} AND {EQUITY_YEARS_MAX}
          AND pr.eq_cagr > {EQUITY_CAGR_MIN}
          AND pr.eq_cagr < {EQUITY_CAGR_MAX}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND f.operatingProfitMarginTTM > {OPM_MIN}
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        ORDER BY pr.eq_cagr DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120, memory_mb=8192, threads=4)
    return results


def main():
    parser = argparse.ArgumentParser(description="Compounding Equity Screen - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("equity-growth", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Compounding Equity Screen - {universe_name}")
    print(f"Signal: 5yr equity CAGR > {EQUITY_CAGR_MIN*100:.0f}%, "
          f"ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, "
          f"MCap > {mktcap_label} local")
    print("-" * 105)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'EqCAGR%':>8} {'Yrs':>4} "
          f"{'ROE%':>6} {'OPM%':>6} {'MCap$B':>8} {'Sector':<20}")
    print("-" * 105)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {r.get('companyName', '')[:26]:<28} "
              f"{r.get('eq_cagr_pct', ''):>8} {r.get('years_measured', ''):>4} "
              f"{r.get('roe_pct', ''):>6} {r.get('opm_pct', ''):>6} "
              f"{r.get('mktcap_b', ''):>8} {r.get('sector', '')[:18]:<20}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
