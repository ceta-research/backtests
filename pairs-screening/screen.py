#!/usr/bin/env python3
"""Pairs trading candidate screen.

Finds US large-cap stock pairs with high return correlation (>= 0.80)
within the same sector. Reduces the ~4M possible pairs among US stocks
to a manageable set of candidates for cointegration testing.

The screen runs per-sector to keep memory and compute within API limits.
For the full multi-sector run, use --global which runs each sector
sequentially and aggregates results.

Usage:
    # Screen a single sector (fast, 1-2 min)
    python3 pairs-screening/screen.py --sector Energy

    # Screen all sectors and save results (10-15 min)
    python3 pairs-screening/screen.py --global --output results/candidate_pairs.csv

    # Show current universe size by sector
    python3 pairs-screening/screen.py --universe

See README.md for methodology and data source details.
"""

import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

# Minimum correlation threshold (Gatev et al. 2006: 0.80 is industry standard)
MIN_CORRELATION = 0.80

# Minimum overlapping trading days (252 = 1 full year)
MIN_COMMON_DAYS = 252

# Maximum market cap ratio (avoid pairing $200B vs $2B)
MAX_MKTCAP_RATIO = 5.0

# US minimum market cap (large-cap: $1B USD)
MIN_MKTCAP_USD = 1_000_000_000

# Lookback: most recent ~252 trading days
LOOKBACK_DATE = "2024-01-01"

SECTORS = [
    "Financial Services",
    "Real Estate",
    "Energy",
    "Consumer Cyclical",
    "Utilities",
    "Communication Services",
    "Technology",
    "Healthcare",
    "Industrials",
    "Basic Materials",
    "Consumer Defensive",
]

UNIVERSE_SQL = f"""
WITH mktcap AS (
    SELECT symbol, marketCap,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM key_metrics
    WHERE period = 'FY'
      AND marketCap IS NOT NULL
      AND marketCap > {MIN_MKTCAP_USD}
),
universe AS (
    SELECT p.symbol, p.sector, p.industry
    FROM profile p JOIN mktcap m ON p.symbol = m.symbol AND m.rn = 1
    WHERE p.country = 'US'
      AND p.sector IS NOT NULL
      AND p.symbol NOT LIKE '%.%'
      AND p.symbol NOT LIKE '%-%'
      AND LENGTH(p.symbol) <= 5
)
SELECT sector, COUNT(*) AS stocks
FROM universe
GROUP BY sector
ORDER BY stocks DESC
"""

SECTOR_PAIRS_SQL = """
WITH mktcap AS (
    SELECT symbol, marketCap,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM key_metrics
    WHERE period = 'FY'
      AND marketCap IS NOT NULL
      AND marketCap > {min_mktcap}
),
sector_stocks AS (
    SELECT p.symbol, p.sector, p.industry, m.marketCap
    FROM profile p JOIN mktcap m ON p.symbol = m.symbol AND m.rn = 1
    WHERE p.country = 'US'
      AND p.sector = '{sector}'
      AND p.sector IS NOT NULL
      AND p.symbol NOT LIKE '%.%'
      AND p.symbol NOT LIKE '%-%'
      AND LENGTH(p.symbol) <= 5
),
daily_ret AS (
    SELECT symbol, CAST(date AS DATE) AS trade_date,
        (adjClose - LAG(adjClose) OVER (PARTITION BY symbol ORDER BY date))
            / NULLIF(LAG(adjClose) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret
    FROM stock_eod
    WHERE symbol IN (SELECT symbol FROM sector_stocks)
      AND date >= '{lookback_date}'
)
SELECT
    a.symbol AS symbol_a,
    b.symbol AS symbol_b,
    sa.sector,
    sa.industry AS industry_a,
    sb.industry AS industry_b,
    (sa.industry = sb.industry) AS same_industry,
    ROUND(CORR(a.ret, b.ret), 4) AS correlation,
    COUNT(*) AS common_days,
    ROUND(sa.marketCap / 1e9, 2) AS mktcap_a_bn,
    ROUND(sb.marketCap / 1e9, 2) AS mktcap_b_bn,
    ROUND(
        GREATEST(sa.marketCap, sb.marketCap)
        / NULLIF(LEAST(sa.marketCap, sb.marketCap), 0), 2
    ) AS mktcap_ratio
FROM daily_ret a
JOIN daily_ret b ON a.trade_date = b.trade_date AND a.symbol < b.symbol
JOIN sector_stocks sa ON a.symbol = sa.symbol
JOIN sector_stocks sb ON b.symbol = sb.symbol
WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
GROUP BY a.symbol, b.symbol, sa.sector, sa.industry, sb.industry,
    sa.marketCap, sb.marketCap
HAVING COUNT(*) >= {min_common_days}
  AND CORR(a.ret, b.ret) >= {min_corr}
  AND GREATEST(sa.marketCap, sb.marketCap)
      / NULLIF(LEAST(sa.marketCap, sb.marketCap), 0) <= {max_mktcap_ratio}
ORDER BY correlation DESC
"""


def run_universe(cr, verbose=False):
    """Show current universe size by sector."""
    if verbose:
        print("Fetching universe stats...\n")
    rows = cr.query(UNIVERSE_SQL, verbose=verbose)
    total = sum(r["stocks"] for r in rows)
    print(f"{'Sector':<30} {'Stocks':>6}")
    print("-" * 38)
    for r in rows:
        print(f"{r['sector']:<30} {r['stocks']:>6,}")
    print("-" * 38)
    print(f"{'TOTAL':<30} {total:>6,}")
    print(f"\nMin market cap: ${MIN_MKTCAP_USD/1e9:.0f}B | Universe: US only")


def run_sector(cr, sector, verbose=False):
    """Run pairwise correlation screen for one sector."""
    sql = SECTOR_PAIRS_SQL.format(
        sector=sector,
        min_mktcap=MIN_MKTCAP_USD,
        lookback_date=LOOKBACK_DATE,
        min_common_days=MIN_COMMON_DAYS,
        min_corr=MIN_CORRELATION,
        max_mktcap_ratio=MAX_MKTCAP_RATIO,
    )
    if verbose:
        print(f"Screening sector: {sector}")
    rows = cr.query(sql, verbose=verbose)
    return rows or []


def print_pairs(rows, sector=None, limit=20):
    """Print a summary of candidate pairs."""
    if not rows:
        print("  No candidate pairs found.")
        return
    header = f"{'Symbol A':<10} {'Symbol B':<10} {'Corr':>6} {'Industry':<35} {'MCap Ratio':>10}"
    print(header)
    print("-" * len(header))
    for r in rows[:limit]:
        industry = r.get("industry_a", "")[:33]
        if not r.get("same_industry"):
            industry += "*"
        print(
            f"{r['symbol_a']:<10} {r['symbol_b']:<10} "
            f"{float(r['correlation']):>6.4f} {industry:<35} {float(r['mktcap_ratio']):>10.1f}x"
        )
    if len(rows) > limit:
        print(f"  ... and {len(rows) - limit} more pairs")
    print(f"\n  Total: {len(rows)} candidate pairs")
    if any(not r.get("same_industry") for r in rows[:limit]):
        print("  * = cross-industry pair (same sector, different industry)")


def save_csv(rows, output_path):
    """Save candidate pairs to CSV."""
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} pairs to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Pairs trading candidate screen (US large-cap)"
    )
    parser.add_argument(
        "--sector",
        type=str,
        help=f"Screen a specific sector. Options: {', '.join(SECTORS)}",
    )
    parser.add_argument(
        "--global",
        dest="global_screen",
        action="store_true",
        help="Screen all sectors and aggregate results",
    )
    parser.add_argument(
        "--universe",
        action="store_true",
        help="Show current universe size by sector (no correlation computation)",
    )
    parser.add_argument("--output", type=str, help="Save results to CSV")
    parser.add_argument(
        "--min-corr",
        type=float,
        default=MIN_CORRELATION,
        help=f"Minimum correlation threshold (default: {MIN_CORRELATION})",
    )
    parser.add_argument("--verbose", action="store_true", help="Show API progress")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    args = parser.parse_args()

    cr = CetaResearch(api_key=args.api_key)

    if args.universe:
        run_universe(cr, verbose=args.verbose)
        return

    print(f"Pairs Screening: US stocks > ${MIN_MKTCAP_USD/1e9:.0f}B market cap")
    print(f"Filters: correlation >= {args.min_corr}, mktcap ratio < {MAX_MKTCAP_RATIO}x, >= {MIN_COMMON_DAYS} common days")
    print()

    if args.global_screen:
        all_pairs = []
        for sector in SECTORS:
            rows = run_sector(cr, sector, verbose=args.verbose)
            same_ind = sum(1 for r in rows if r.get("same_industry"))
            print(f"  {sector:<30} {len(rows):>5} pairs ({same_ind} same-industry)")
            all_pairs.extend(rows)
        print(f"\nTotal candidate pairs: {len(all_pairs)}")
        same_ind_total = sum(1 for r in all_pairs if r.get("same_industry"))
        print(f"Same-industry: {same_ind_total} ({100*same_ind_total/len(all_pairs):.1f}%)")
        avg_corr = sum(float(r["correlation"]) for r in all_pairs) / len(all_pairs)
        print(f"Avg correlation: {avg_corr:.4f}")
        if args.output:
            save_csv(all_pairs, args.output)

    elif args.sector:
        rows = run_sector(cr, args.sector, verbose=args.verbose)
        print(f"Sector: {args.sector}")
        print()
        print_pairs(rows)
        if args.output:
            save_csv(rows, args.output)

    else:
        # Default: run Energy sector as a quick demo
        sector = "Energy"
        print(f"Running demo sector: {sector}")
        print("(Use --sector <name> for a specific sector, or --global for all)\n")
        rows = run_sector(cr, sector, verbose=args.verbose)
        print_pairs(rows)


if __name__ == "__main__":
    main()
