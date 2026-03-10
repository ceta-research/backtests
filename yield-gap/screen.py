#!/usr/bin/env python3
"""
Yield Gap Screen - Current Stock Screen

Screens for stocks with earnings yields significantly above the regional
risk-free rate, with quality filters to avoid value traps.

The "yield gap" is the spread between a stock's earnings yield (1/PE)
and the prevailing risk-free rate. When this gap is wide, equities are
priced cheaply relative to bonds.

Usage:
    python3 yield-gap/screen.py
    python3 yield-gap/screen.py --preset india
    python3 yield-gap/screen.py --exchange XETRA
    python3 yield-gap/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, get_risk_free_rate

# Signal parameters (match backtest.py)
EARNINGS_YIELD_MIN = 0.06
SPREAD_ABOVE_RFR = 0.03
EARNINGS_YIELD_MAX = 0.50
ROE_MIN = 0.08
DE_MAX = 2.0
MAX_STOCKS = 30


def get_effective_ey_threshold(risk_free_rate):
    return max(EARNINGS_YIELD_MIN, risk_free_rate + SPREAD_ABOVE_RFR)


def run_screen(client, exchanges, mktcap_min, ey_threshold, verbose=False):
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
            ROUND(k.earningsYieldTTM * 100, 2) AS earnings_yield_pct,
            ROUND(1.0 / NULLIF(k.earningsYieldTTM, 0), 1) AS implied_pe,
            ROUND(k.returnOnEquityTTM * 100, 2) AS roe_pct,
            ROUND(fr.debtToEquityRatioTTM, 2) AS debt_to_equity,
            ROUND(k.freeCashFlowYieldTTM * 100, 2) AS fcf_yield_pct,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM key_metrics_ttm k
        JOIN profile p ON k.symbol = p.symbol
        JOIN financial_ratios_ttm fr ON k.symbol = fr.symbol
        WHERE k.earningsYieldTTM > {ey_threshold}
          AND k.earningsYieldTTM < {EARNINGS_YIELD_MAX}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND (fr.debtToEquityRatioTTM IS NULL
               OR (fr.debtToEquityRatioTTM >= 0 AND fr.debtToEquityRatioTTM < {DE_MAX}))
          AND k.marketCap > {mktcap_min}
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Asset Management%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Shell Companies%')
          AND (p.industry IS NULL OR p.industry NOT LIKE 'Closed-End Fund%')
          {exchange_filter}
        ORDER BY k.earningsYieldTTM DESC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=120)
    return results or []


def main():
    parser = argparse.ArgumentParser(description="Yield Gap Screen - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("yield-gap", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    ey_threshold = get_effective_ey_threshold(risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Yield Gap Screen - {universe_name}")
    print(f"Signal: EY > {ey_threshold*100:.1f}% (rfr={risk_free_rate*100:.1f}%+{SPREAD_ABOVE_RFR*100:.0f}%), "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, ey_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'EY%':>6} {'PE':>6} {'ROE%':>6} "
          f"{'D/E':>6} {'FCF%':>7} {'MCap$B':>8}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:26]
        print(f"{i:<4} {r.get('symbol', ''):<10} {company:<28} "
              f"{r.get('earnings_yield_pct', 'N/A'):>6} "
              f"{r.get('implied_pe', 'N/A'):>6} "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('debt_to_equity', 'N/A'):>6} "
              f"{r.get('fcf_yield_pct', 'N/A'):>7} "
              f"{r.get('mktcap_b', 'N/A'):>8}")

    print(f"\n{len(results)} stocks qualify. Data: Ceta Research (FMP), TTM metrics.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
