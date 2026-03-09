#!/usr/bin/env python3
"""
Pre-Earnings Screen: Upcoming earnings with beat rate classification

Shows stocks reporting earnings in the next N days, classified by
historical beat rate. Habitual beaters are the most actionable candidates
for the pre-earnings runup strategy.

Usage:
    # Default: all stocks reporting in next 14 days, US
    python3 pre-earnings/screen.py

    # Only habitual beaters (>75% beat rate)
    python3 pre-earnings/screen.py --category habitual_beater

    # Longer window
    python3 pre-earnings/screen.py --days 21

    # Different exchange
    python3 pre-earnings/screen.py --preset india

    # Cloud execution
    python3 pre-earnings/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_MIN_PRIOR_REPORTS = 8
HABITUAL_BEATER_THRESHOLD = 0.75
HABITUAL_MISSER_THRESHOLD = 0.25


def run_screen(client, exchanges=None, category="all", lookback_days=DEFAULT_LOOKBACK_DAYS,
               mktcap_min=None, min_prior_reports=DEFAULT_MIN_PRIOR_REPORTS,
               limit=50, verbose=False):
    """Screen for upcoming earnings with beat rate classification."""

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
        profile_join = "JOIN profile p ON ec.symbol = p.symbol"
    else:
        exchange_filter = ""
        profile_join = ""

    mcap_threshold = mktcap_min or 1_000_000_000

    if category == "habitual_beater":
        cat_filter = f"AND beat_rate > {HABITUAL_BEATER_THRESHOLD}"
    elif category == "habitual_misser":
        cat_filter = f"AND beat_rate < {HABITUAL_MISSER_THRESHOLD}"
    elif category == "mixed":
        cat_filter = f"AND beat_rate BETWEEN {HABITUAL_MISSER_THRESHOLD} AND {HABITUAL_BEATER_THRESHOLD}"
    else:
        cat_filter = ""

    sql = f"""
        WITH beat_history AS (
            SELECT
                symbol,
                COUNT(*) AS total_reports,
                SUM(CASE WHEN epsActual > epsEstimated THEN 1 ELSE 0 END) AS beats,
                ROUND(
                    SUM(CASE WHEN epsActual > epsEstimated THEN 1 ELSE 0 END)
                    * 100.0 / COUNT(*), 1
                ) AS beat_rate_pct,
                CAST(
                    SUM(CASE WHEN epsActual > epsEstimated THEN 1 ELSE 0 END) AS DOUBLE
                ) / COUNT(*) AS beat_rate,
                MAX(CAST(date AS DATE)) AS last_report_date
            FROM earnings_surprises
            WHERE epsEstimated IS NOT NULL
              AND ABS(epsEstimated) > 0.01
              AND epsActual IS NOT NULL
            GROUP BY symbol
            HAVING COUNT(*) >= {min_prior_reports}
        ),
        upcoming AS (
            SELECT ec.symbol,
                CAST(ec.date AS DATE) AS earnings_date,
                ec.epsEstimated
            FROM earnings_calendar ec
            {profile_join}
            WHERE CAST(ec.date AS DATE) BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '{lookback_days}' DAY
              AND ec.epsEstimated IS NOT NULL
              {exchange_filter}
        ),
        with_mcap AS (
            SELECT u.*,
                bh.beat_rate_pct,
                bh.beat_rate,
                bh.total_reports,
                bh.beats,
                bh.last_report_date,
                CASE
                    WHEN bh.beat_rate > {HABITUAL_BEATER_THRESHOLD} THEN 'habitual_beater'
                    WHEN bh.beat_rate < {HABITUAL_MISSER_THRESHOLD} THEN 'habitual_misser'
                    ELSE 'mixed'
                END AS category,
                ROUND(k.marketCap / 1e9, 1) AS mktcap_bn
            FROM upcoming u
            JOIN beat_history bh ON u.symbol = bh.symbol
            LEFT JOIN key_metrics_ttm k ON u.symbol = k.symbol
            WHERE k.marketCap IS NULL OR k.marketCap > {mcap_threshold}
            {cat_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY u.symbol ORDER BY k.date DESC) = 1
        )
        SELECT
            symbol, earnings_date, epsEstimated,
            beat_rate_pct, beats, total_reports,
            category, mktcap_bn, last_report_date
        FROM with_mcap
        ORDER BY
            CASE category WHEN 'habitual_beater' THEN 1 WHEN 'mixed' THEN 2 ELSE 3 END,
            beat_rate_pct DESC,
            mktcap_bn DESC NULLS LAST
        LIMIT {limit}
    """

    if verbose:
        print("Running pre-earnings screen...")
        print(f"  Category: {category}")
        print(f"  Lookback: {lookback_days} days")
        print(f"  Min prior reports: {min_prior_reports}")

    results = client.query(sql, verbose=verbose)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Pre-earnings screen: upcoming earnings with beat rate classification")
    add_common_args(parser)
    parser.add_argument("--category",
                        choices=["all", "habitual_beater", "habitual_misser", "mixed"],
                        default="all",
                        help="Beat rate category filter (default: all)")
    parser.add_argument("--days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f"Days to look ahead (default: {DEFAULT_LOOKBACK_DAYS})")
    parser.add_argument("--min-reports", type=int, default=DEFAULT_MIN_PRIOR_REPORTS,
                        help=f"Min prior quarters for classification (default: {DEFAULT_MIN_PRIOR_REPORTS})")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max results (default: 50)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("pre-earnings", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    results = run_screen(cr, exchanges=exchanges, category=args.category,
                          lookback_days=args.days, mktcap_min=mktcap_min,
                          min_prior_reports=args.min_reports,
                          limit=args.limit, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    cat_label = {
        "all": "All Categories",
        "habitual_beater": "Habitual Beaters (>75%)",
        "habitual_misser": "Habitual Missers (<25%)",
        "mixed": "Mixed (25-75%)",
    }[args.category]

    print(f"\n{'=' * 95}")
    print(f"  PRE-EARNINGS SCREEN: {cat_label} ({universe_name}, next {args.days} days)")
    print(f"{'=' * 95}")
    print(f"  {'Symbol':<8} {'Earnings':>12} {'Beat%':>8} {'Reports':>8} {'Category':>20} {'MCap($B)':>10}")
    print(f"  {'-' * 80}")

    for r in results:
        print(f"  {r['symbol']:<8} {str(r['earnings_date']):>12} "
              f"{r.get('beat_rate_pct', 0):>7.1f}% {r.get('total_reports', 0):>8} "
              f"{r.get('category', ''):>20} {r.get('mktcap_bn', 'N/A'):>10}")

    print(f"\n  {len(results)} stocks found")
    print(f"{'=' * 95}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Saved to {args.output}")


if __name__ == "__main__":
    main()
