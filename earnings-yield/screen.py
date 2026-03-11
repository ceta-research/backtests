#!/usr/bin/env python3
"""
Earnings Yield Screen — Current Qualifying Stocks

Screens the current universe for high earnings yield stocks using TTM data.
Returns top 50 by earnings yield with quality filters.

Signal: earningsYieldTTM > 8%, returnOnEquityTTM > 12%, D/E < 1.5, IC > 3
        Optionally: piotroskiScore >= 6 (--advanced flag)

Usage:
    python3 earnings-yield/screen.py                    # US stocks
    python3 earnings-yield/screen.py --preset india     # India
    python3 earnings-yield/screen.py --exchange LSE     # UK
    python3 earnings-yield/screen.py --advanced         # Add Piotroski filter
    python3 earnings-yield/screen.py --cloud            # Run on cloud
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold


EY_TTM_MIN = 0.08      # 8% earnings yield minimum for screen
ROE_TTM_MIN = 0.12     # 12% ROE minimum
DE_TTM_MAX = 1.5       # D/E < 1.5
IC_TTM_MIN = 3.0       # Interest coverage > 3x
PIOTROSKI_MIN = 6      # Advanced screen: Piotroski >= 6


def run_screen(cr, exchanges, mktcap_min, advanced=False, verbose=False, top_n=50):
    """Run current TTM earnings yield screen. Returns list of qualifying stocks."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    if advanced:
        sql = f"""
            SELECT
                k.symbol,
                ROUND(k.earningsYieldTTM * 100, 2) AS ey_pct,
                ROUND(1.0 / NULLIF(r.priceToEarningsRatioTTM, 0) * 100, 2) AS pe_implied,
                ROUND(r.priceToEarningsRatioTTM, 1) AS pe,
                ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
                ROUND(r.debtToEquityRatioTTM, 2) AS de,
                ROUND(r.interestCoverageRatioTTM, 1) AS ic,
                s.piotroskiScore,
                ROUND(k.marketCap / 1e9, 1) AS mktcap_bn,
                p.exchange,
                p.sector
            FROM key_metrics_ttm k
            JOIN financial_ratios_ttm r ON k.symbol = r.symbol
            JOIN scores s ON k.symbol = s.symbol
            JOIN profile p ON k.symbol = p.symbol
            WHERE k.earningsYieldTTM > {EY_TTM_MIN}
              AND r.priceToEarningsRatioTTM > 0
              AND r.priceToEarningsRatioTTM < 100
              AND k.returnOnEquityTTM > {ROE_TTM_MIN}
              AND r.debtToEquityRatioTTM >= 0
              AND r.debtToEquityRatioTTM < {DE_TTM_MAX}
              AND r.interestCoverageRatioTTM > {IC_TTM_MIN}
              AND s.piotroskiScore >= {PIOTROSKI_MIN}
              AND k.marketCap > {mktcap_min}
              {exchange_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.earningsYieldTTM DESC) = 1
            ORDER BY k.earningsYieldTTM DESC
            LIMIT {top_n}
        """
    else:
        sql = f"""
            SELECT
                k.symbol,
                ROUND(k.earningsYieldTTM * 100, 2) AS ey_pct,
                ROUND(r.priceToEarningsRatioTTM, 1) AS pe,
                ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
                ROUND(r.debtToEquityRatioTTM, 2) AS de,
                ROUND(r.interestCoverageRatioTTM, 1) AS ic,
                ROUND(k.marketCap / 1e9, 1) AS mktcap_bn,
                p.exchange,
                p.sector
            FROM key_metrics_ttm k
            JOIN financial_ratios_ttm r ON k.symbol = r.symbol
            JOIN profile p ON k.symbol = p.symbol
            WHERE k.earningsYieldTTM > {EY_TTM_MIN}
              AND r.priceToEarningsRatioTTM > 0
              AND r.priceToEarningsRatioTTM < 100
              AND k.returnOnEquityTTM > {ROE_TTM_MIN}
              AND r.debtToEquityRatioTTM >= 0
              AND r.debtToEquityRatioTTM < {DE_TTM_MAX}
              AND r.interestCoverageRatioTTM > {IC_TTM_MIN}
              AND k.marketCap > {mktcap_min}
              {exchange_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.earningsYieldTTM DESC) = 1
            ORDER BY k.earningsYieldTTM DESC
            LIMIT {top_n}
        """

    rows = cr.query(sql, verbose=verbose)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Earnings Yield current stock screen")
    add_common_args(parser)
    parser.add_argument("--advanced", action="store_true",
                        help="Add Piotroski score filter (>= 6)")
    parser.add_argument("--top", type=int, default=50,
                        help="Number of stocks to return (default 50)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run via Ceta Research Code Execution API")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("earnings-yield", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    screen_type = "Advanced (with Piotroski)" if args.advanced else "Simple"
    print(f"\nEarnings Yield Screen — {screen_type}")
    print(f"Universe: {universe_name}")
    print(f"Filters: EY > {EY_TTM_MIN*100:.0f}%, ROE > {ROE_TTM_MIN*100:.0f}%, "
          f"D/E < {DE_TTM_MAX}, IC > {IC_TTM_MIN:.0f}x, MCap > {mktcap_threshold/1e9:.0f}B local")
    if args.advanced:
        print(f"         Piotroski >= {PIOTROSKI_MIN}")
    print("=" * 65)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    rows = run_screen(cr, exchanges, mktcap_threshold, advanced=args.advanced,
                      verbose=args.verbose, top_n=args.top)

    if not rows:
        print("No qualifying stocks found.")
        return

    print(f"\nTop {len(rows)} stocks by earnings yield:\n")
    keys = list(rows[0].keys())
    widths = {k: max(len(k), max(len(str(r.get(k, ""))) for r in rows)) for k in keys}
    header = "  ".join(f"{k:<{widths[k]}}" for k in keys)
    print(header)
    print("-" * len(header))
    for r in rows:
        print("  ".join(f"{str(r.get(k, '')):<{widths[k]}}" for k in keys))

    print(f"\nTotal qualifying: {len(rows)} stocks")


if __name__ == "__main__":
    main()
