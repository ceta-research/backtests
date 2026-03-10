#!/usr/bin/env python3
"""
Industry Leader - Current Stock Screen

Screens for the top 3 companies by revenue in each industry showing
positive YoY revenue growth. Uses FY data for consistency with backtest.

Usage:
    python3 industry-leader/screen.py
    python3 industry-leader/screen.py --preset india
    python3 industry-leader/screen.py --exchange XETRA
    python3 industry-leader/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
INDUSTRY_GROWTH_MIN = 0.05
MIN_INDUSTRY_SIZE = 3
LEADERS_PER_INDUSTRY = 3
MAX_STOCKS = 300


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using FY data. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH latest_rev AS (
            SELECT i.symbol, p.industry, p.exchange, p.companyName,
                i.revenue, i.date AS filing_date,
                ROW_NUMBER() OVER (PARTITION BY i.symbol ORDER BY i.dateEpoch DESC) AS rn
            FROM income_statement i
            JOIN profile p ON i.symbol = p.symbol
            WHERE i.period = 'FY'
              AND i.revenue > 0
              AND p.industry IS NOT NULL AND p.industry != ''
              {exchange_filter}
        ),
        prior_rev AS (
            SELECT i.symbol, i.revenue AS prior_revenue,
                ROW_NUMBER() OVER (PARTITION BY i.symbol ORDER BY i.dateEpoch DESC) AS rn
            FROM income_statement i
            WHERE i.period = 'FY' AND i.revenue > 0
        ),
        current AS (SELECT * FROM latest_rev WHERE rn = 1),
        prior AS (SELECT * FROM prior_rev WHERE rn = 2),
        company_growth AS (
            SELECT c.symbol, c.companyName, c.exchange, c.industry,
                c.revenue, p.prior_revenue, c.filing_date,
                (c.revenue - p.prior_revenue) / p.prior_revenue AS rev_growth
            FROM current c
            JOIN prior p ON c.symbol = p.symbol
            WHERE p.prior_revenue > 0
        ),
        key_mc AS (
            SELECT symbol, marketCapTTM AS marketCap
            FROM key_metrics_ttm
            WHERE marketCapTTM IS NOT NULL AND marketCapTTM > 0
        ),
        company_filtered AS (
            SELECT cg.*, mc.marketCap
            FROM company_growth cg
            JOIN key_mc mc ON cg.symbol = mc.symbol
            WHERE mc.marketCap >= {mktcap_min}
        ),
        industry_agg AS (
            SELECT industry,
                COUNT(*) AS n_companies,
                ROUND(AVG(rev_growth) * 100, 2) AS avg_growth_pct
            FROM company_filtered
            GROUP BY industry
            HAVING COUNT(*) >= {MIN_INDUSTRY_SIZE}
               AND AVG(rev_growth) >= {INDUSTRY_GROWTH_MIN}
        ),
        leaders AS (
            SELECT cf.symbol, cf.companyName, cf.exchange, cf.industry,
                cf.revenue, cf.rev_growth, cf.marketCap, cf.filing_date,
                ia.avg_growth_pct AS industry_growth_pct,
                ROUND(cf.revenue / 1e9, 2) AS revenue_b,
                ROUND(cf.marketCap / 1e9, 2) AS mktcap_b,
                ROUND(cf.rev_growth * 100, 2) AS rev_growth_pct,
                ROW_NUMBER() OVER (PARTITION BY cf.industry ORDER BY cf.revenue DESC) AS rev_rank
            FROM company_filtered cf
            JOIN industry_agg ia ON cf.industry = ia.industry
        )
        SELECT symbol, companyName, exchange, industry, revenue_b,
            rev_growth_pct, industry_growth_pct, mktcap_b, filing_date, rev_rank
        FROM leaders
        WHERE rev_rank <= {LEADERS_PER_INDUSTRY}
        ORDER BY industry, rev_rank
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=300,
                           memory_mb=8192, threads=4)
    return results


def main():
    parser = argparse.ArgumentParser(description="Industry Leader - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("industry-leader", args_str=" ".join(cloud_args),
                                  api_key=args.api_key, base_url=args.base_url,
                                  verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges, use_low_threshold=False)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Industry Leader Screen - {universe_name}")
    print(f"Signal: Industry avg rev growth >= {INDUSTRY_GROWTH_MIN*100:.0f}%, "
          f"top {LEADERS_PER_INDUSTRY} by revenue/industry, MCap > {mktcap_label} local")
    print("-" * 110)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    current_industry = None
    count = 0
    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<30} {'Rev$B':>7} {'RevGr%':>7} "
          f"{'IndGr%':>8} {'MCap$B':>8} {'Date':<12}")
    print("-" * 110)

    for r in results:
        ind = r.get('industry', '')
        if ind != current_industry:
            print(f"\n  === {ind} (ind avg growth: {r.get('industry_growth_pct', '')}%) ===")
            current_industry = ind
        count += 1
        print(f"{r.get('rev_rank', ''):<4} {r['symbol']:<10} {r.get('companyName', '')[:28]:<30} "
              f"{r.get('revenue_b', ''):>7} {r.get('rev_growth_pct', ''):>7} "
              f"{r.get('industry_growth_pct', ''):>8} {r.get('mktcap_b', ''):>8} "
              f"{r.get('filing_date', ''):<12}")

    industries_count = len(set(r.get('industry') for r in results))
    print(f"\n{count} stocks qualify across {industries_count} growing industries.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
