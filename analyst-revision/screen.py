#!/usr/bin/env python3
"""
Analyst Rating Revision Screen — Live Signal

Shows current analyst upgrades from the last 30 days. Optionally filters to
upgrade clusters (2+ independent analysts within 30 days).

Data source: stock_grade (FMP individual analyst grade changes)

Usage:
    python3 analyst-revision/screen.py                      # US recent upgrades
    python3 analyst-revision/screen.py --preset uk
    python3 analyst-revision/screen.py --clusters           # Upgrade clusters only
    python3 analyst-revision/screen.py --days 60 --clusters
    python3 analyst-revision/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold


GRADE_MAP_SQL = """
        WITH grade_map AS (
            SELECT grade, score FROM (VALUES
                ('Strong Buy', 5), ('Buy', 5), ('Outperform', 5), ('Overweight', 5),
                ('Market Outperform', 5), ('Positive', 5), ('Accumulate', 5),
                ('Top Pick', 5), ('Conviction Buy', 5), ('Add', 5),
                ('Hold', 3), ('Neutral', 3), ('Equal-Weight', 3), ('Market Perform', 3),
                ('Sector Perform', 3), ('In-Line', 3), ('Peer Perform', 3), ('Mixed', 3),
                ('Sector Weight', 3), ('Market Weight', 3),
                ('Sell', 1), ('Underperform', 1), ('Underweight', 1),
                ('Market Underperform', 1), ('Reduce', 1), ('Strong Sell', 1), ('Negative', 1)
            ) AS t(grade, score)
        )"""


def screen_recent_upgrades(cr, exchanges, mktcap_min, days=30, verbose=False):
    """Show individual analyst upgrades in the last N days."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"AND sg.symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        mcap_join = f"""
            LEFT JOIN (
                SELECT symbol, marketCap, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
                FROM key_metrics WHERE period = 'FY' AND marketCap IS NOT NULL
                  AND symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))
            ) mc ON sg.symbol = mc.symbol AND mc.rn = 1"""
        mcap_filter = f"AND (mc.marketCap IS NULL OR mc.marketCap > {mktcap_min})"
    else:
        sym_filter = ""
        mcap_join = ""
        mcap_filter = ""

    sql = f"""
        {GRADE_MAP_SQL},
        recent AS (
            SELECT
                sg.symbol,
                CAST(sg.date AS DATE) AS revision_date,
                sg.gradingCompany,
                sg.previousGrade,
                sg.newGrade,
                gn.score - gp.score AS grade_change,
                ROW_NUMBER() OVER (
                    PARTITION BY sg.symbol, CAST(sg.date AS DATE), sg.gradingCompany
                    ORDER BY sg.dateEpoch DESC
                ) AS rn
            FROM stock_grade sg
            LEFT JOIN grade_map gn ON LOWER(sg.newGrade) = LOWER(gn.grade)
            LEFT JOIN grade_map gp ON LOWER(sg.previousGrade) = LOWER(gp.grade)
            WHERE CAST(sg.date AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
              AND sg.action = 'upgrade'
              AND gn.score IS NOT NULL AND gp.score IS NOT NULL
              {sym_filter}
        )
        SELECT r.symbol, r.revision_date, r.gradingCompany,
               r.previousGrade, r.newGrade, r.grade_change
        FROM recent r
        {mcap_join}
        WHERE r.rn = 1
          {mcap_filter}
        ORDER BY r.revision_date DESC, r.grade_change DESC
        LIMIT 100
    """

    if verbose:
        print(f"Querying recent upgrades (last {days} days)...")

    results = cr.query(sql, format="json", verbose=verbose)
    return results


def screen_upgrade_clusters(cr, exchanges, mktcap_min, days=30, min_analysts=2, verbose=False):
    """Show stocks with 2+ independent analyst upgrades within the last N days."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"AND sg.symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        mcap_join = f"""
            LEFT JOIN (
                SELECT symbol, marketCap, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
                FROM key_metrics WHERE period = 'FY' AND marketCap IS NOT NULL
                  AND symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))
            ) mc ON clusters.symbol = mc.symbol AND mc.rn = 1"""
        mcap_filter = f"AND (mc.marketCap IS NULL OR mc.marketCap > {mktcap_min})"
    else:
        sym_filter = ""
        mcap_join = ""
        mcap_filter = ""

    sql = f"""
        WITH deduped AS (
            SELECT
                symbol, CAST(date AS DATE) AS revision_date, gradingCompany,
                previousGrade, newGrade,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, CAST(date AS DATE), gradingCompany
                    ORDER BY dateEpoch DESC
                ) AS rn
            FROM stock_grade
            WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
              AND action = 'upgrade'
              {sym_filter}
        ),
        upgrades AS (
            SELECT symbol, revision_date, gradingCompany, previousGrade, newGrade
            FROM deduped WHERE rn = 1
        ),
        clusters AS (
            SELECT
                symbol,
                COUNT(DISTINCT gradingCompany) AS distinct_analysts,
                COUNT(*) AS upgrade_count,
                MIN(revision_date) AS first_upgrade,
                MAX(revision_date) AS last_upgrade,
                STRING_AGG(DISTINCT gradingCompany, ', ' ORDER BY gradingCompany) AS analyst_firms,
                STRING_AGG(DISTINCT newGrade, ', ' ORDER BY newGrade) AS new_grades
            FROM upgrades
            GROUP BY symbol
            HAVING COUNT(DISTINCT gradingCompany) >= {min_analysts}
        )
        SELECT clusters.symbol, clusters.distinct_analysts, clusters.upgrade_count,
               clusters.first_upgrade, clusters.last_upgrade,
               clusters.analyst_firms, clusters.new_grades
        FROM clusters
        {mcap_join}
        WHERE 1=1 {mcap_filter}
        ORDER BY clusters.distinct_analysts DESC, clusters.upgrade_count DESC
        LIMIT 50
    """

    if verbose:
        print(f"Querying upgrade clusters (last {days} days, min {min_analysts} analysts)...")

    results = cr.query(sql, format="json", verbose=verbose)
    return results


def main():
    parser = argparse.ArgumentParser(description="Analyst Rating Revision live screen")
    add_common_args(parser)
    parser.add_argument("--clusters", action="store_true",
                        help="Show only upgrade clusters (2+ analysts)")
    parser.add_argument("--days", type=int, default=30,
                        help="Lookback window in days (default: 30)")
    parser.add_argument("--min-analysts", type=int, default=2,
                        help="Minimum distinct analysts for cluster filter (default: 2)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("analyst-revision/screen", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url, verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    if args.clusters:
        print(f"\nUpgrade Clusters — {universe_name} (last {args.days} days, {args.min_analysts}+ analysts)")
        print("=" * 70)
        results = screen_upgrade_clusters(cr, exchanges, mktcap_threshold,
                                          days=args.days, min_analysts=args.min_analysts,
                                          verbose=args.verbose)
        if not results:
            print("No upgrade clusters found.")
            return

        print(f"{'Symbol':<12} {'Analysts':>9} {'Count':>7} {'First':>12} {'Last':>12} {'Firms'}")
        print("-" * 80)
        for r in results:
            firms = r.get("analyst_firms", "")[:35]
            print(f"{r['symbol']:<12} {r['distinct_analysts']:>9} {r['upgrade_count']:>7} "
                  f"{r['first_upgrade']:>12} {r['last_upgrade']:>12}  {firms}")
    else:
        print(f"\nRecent Upgrades — {universe_name} (last {args.days} days)")
        print("=" * 70)
        results = screen_recent_upgrades(cr, exchanges, mktcap_threshold,
                                         days=args.days, verbose=args.verbose)
        if not results:
            print("No upgrades found.")
            return

        print(f"{'Symbol':<12} {'Date':>12} {'From':>18} {'To':>18} {'Change':>8} {'Firm'}")
        print("-" * 90)
        for r in results:
            print(f"{r['symbol']:<12} {r['revision_date']:>12} "
                  f"{r.get('previousGrade',''):>18} {r.get('newGrade',''):>18} "
                  f"{r.get('grade_change',0):>+7}  {r.get('gradingCompany','')[:25]}")

    print(f"\nTotal: {len(results)} results")


if __name__ == "__main__":
    main()
