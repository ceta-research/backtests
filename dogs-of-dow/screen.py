#!/usr/bin/env python3
"""
Dogs of the Dow Current Screen

Shows the current top 10 highest-yielding stocks from the Dow 30 (US)
or top 30 by market cap (other exchanges).

Usage:
    # Current US Dogs
    python3 dogs-of-dow/screen.py

    # Current Indian "Dogs" (high yield blue chips)
    python3 dogs-of-dow/screen.py --preset india

    # Run on cloud
    python3 dogs-of-dow/screen.py --cloud --preset us
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, print_header

DOGS_COUNT = 10
BLUECHIP_COUNT = 30


def run_screen(client, exchanges, verbose=False):
    """Run current Dogs screen using TTM data."""
    use_dow = exchanges and set(exchanges).issubset({"NYSE", "NASDAQ", "AMEX"})

    if use_dow:
        sql = f"""
            WITH dow AS (
                SELECT DISTINCT symbol
                FROM dowjones_constituent
                WHERE symbol IS NOT NULL
            ),
            ttm AS (
                SELECT symbol, dividendYieldTTM, priceToEarningsRatioTTM,
                       dividendPayoutRatioTTM,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dividendYieldTTM DESC) AS rn
                FROM financial_ratios_ttm
                WHERE dividendYieldTTM IS NOT NULL AND dividendYieldTTM > 0
            ),
            met AS (
                SELECT symbol, marketCap,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY marketCap DESC) AS rn
                FROM key_metrics_ttm
                WHERE marketCap IS NOT NULL
            ),
            prof AS (
                SELECT symbol, companyName,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY symbol) AS rn
                FROM profile WHERE companyName IS NOT NULL
            )
            SELECT
                d.symbol,
                COALESCE(p.companyName, d.symbol) AS company,
                ROUND(t.dividendYieldTTM * 100, 2) AS yield_pct,
                ROUND(t.priceToEarningsRatioTTM, 1) AS pe_ratio,
                ROUND(t.dividendPayoutRatioTTM * 100, 1) AS payout_pct,
                ROUND(m.marketCap / 1e9, 1) AS mktcap_bn,
                ROW_NUMBER() OVER (ORDER BY t.dividendYieldTTM DESC) AS rank
            FROM dow d
            JOIN ttm t ON d.symbol = t.symbol AND t.rn = 1
            LEFT JOIN met m ON d.symbol = m.symbol AND m.rn = 1
            LEFT JOIN prof p ON d.symbol = p.symbol AND p.rn = 1
            ORDER BY t.dividendYieldTTM DESC
            LIMIT {DOGS_COUNT}
        """
        title = "DOGS OF THE DOW (Current)"
    else:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges) if exchanges else "'NYSE'"
        sql = f"""
            WITH met AS (
                SELECT k.symbol, k.marketCap,
                       ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.marketCap DESC) AS rn
                FROM key_metrics_ttm k
                JOIN profile p ON k.symbol = p.symbol
                WHERE k.marketCap IS NOT NULL AND k.marketCap > 1000000000
                  AND p.exchange IN ({ex_filter})
            ),
            bluechips AS (
                SELECT symbol, marketCap FROM met WHERE rn = 1
                ORDER BY marketCap DESC
                LIMIT {BLUECHIP_COUNT}
            ),
            ttm AS (
                SELECT symbol, dividendYieldTTM, priceToEarningsRatioTTM,
                       dividendPayoutRatioTTM,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dividendYieldTTM DESC) AS rn
                FROM financial_ratios_ttm
                WHERE dividendYieldTTM IS NOT NULL AND dividendYieldTTM > 0
            ),
            prof AS (
                SELECT symbol, companyName,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY symbol) AS rn
                FROM profile WHERE companyName IS NOT NULL
            )
            SELECT
                b.symbol,
                COALESCE(p.companyName, b.symbol) AS company,
                ROUND(t.dividendYieldTTM * 100, 2) AS yield_pct,
                ROUND(t.priceToEarningsRatioTTM, 1) AS pe_ratio,
                ROUND(t.dividendPayoutRatioTTM * 100, 1) AS payout_pct,
                ROUND(b.marketCap / 1e9, 1) AS mktcap_bn,
                ROW_NUMBER() OVER (ORDER BY t.dividendYieldTTM DESC) AS rank
            FROM bluechips b
            JOIN ttm t ON b.symbol = t.symbol AND t.rn = 1
            LEFT JOIN prof p ON b.symbol = p.symbol AND p.rn = 1
            ORDER BY t.dividendYieldTTM DESC
            LIMIT {DOGS_COUNT}
        """
        ex_label = ", ".join(exchanges) if exchanges else "Global"
        title = f"HIGH YIELD BLUE CHIPS ({ex_label})"

    results = client.query(sql, verbose=verbose)
    if not results:
        print(f"No results for screen.")
        return

    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")
    print(f"  {'#':<4} {'Symbol':<8} {'Company':<30} {'Yield':>7} {'P/E':>7} {'Payout':>8} {'MCap($B)':>9}")
    print("  " + "-" * 75)

    for r in results:
        rank = r.get("rank", "")
        sym = r.get("symbol", "")
        company = (r.get("company", "") or "")[:28]
        yld = r.get("yield_pct", "")
        pe = r.get("pe_ratio", "")
        payout = r.get("payout_pct", "")
        mcap = r.get("mktcap_bn", "")

        yld_str = f"{yld:>6.2f}%" if yld else "   N/A"
        pe_str = f"{pe:>7.1f}" if pe else "    N/A"
        payout_str = f"{payout:>7.1f}%" if payout else "    N/A"
        mcap_str = f"{mcap:>8.1f}" if mcap else "     N/A"

        print(f"  {rank:<4} {sym:<8} {company:<30} {yld_str} {pe_str} {payout_str} {mcap_str}")

    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(description="Dogs of the Dow current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("dogs-of-dow", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, verbose=args.verbose)


if __name__ == "__main__":
    main()
