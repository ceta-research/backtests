#!/usr/bin/env python3
"""
ETF Overlap Analysis

Cross-sectional analysis of ETF holding overlaps. Not a return-based backtest;
this is a portfolio construction tool that measures how much overlap exists
between ETFs in an investor's portfolio.

Outputs:
  - Pairwise overlap (count-based and weight-based)
  - Most widely held stocks across all ETFs
  - Common portfolio template analysis (3-fund, etc.)
  - All-pairs overlap for large ETFs
  - Structured JSON results

Usage:
    # Full analysis with default popular ETFs
    python3 etf-overlap/analysis.py --output results/etf_overlap.json --verbose

    # Specific ETF pair
    python3 etf-overlap/analysis.py --pair SPY,QQQ

    # Custom ETF list
    python3 etf-overlap/analysis.py --etfs SPY,QQQ,VOO,VTI,VXUS

    # All-pairs mode (ETFs with 50+ holdings, slow)
    python3 etf-overlap/analysis.py --all-pairs --output results/all_pairs.json
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

# --- Popular ETFs for default analysis ---
POPULAR_ETFS = [
    'SPY', 'QQQ', 'VOO', 'IVV', 'VTI', 'ITOT', 'VXUS', 'VGT', 'DIA',
    'IWM', 'SCHD', 'VYM', 'QQQM', 'ARKK', 'RSP', 'QUAL', 'MTUM',
    'VLUE', 'SMH', 'VEA', 'EFA', 'VWO', 'XLK', 'XLF',
    'XLE', 'XLV', 'IWF', 'IWD', 'VIG', 'DGRO', 'JEPI', 'JEPQ',
    'VTV', 'VUG', 'HDV', 'SPYG', 'SPYV',
]
# NOTE: Bond ETFs (BND, AGG) excluded. FMP stores bond issuer tickers
# identically to stock tickers, causing false overlap with equity ETFs.

INTERESTING_PAIRS = [
    # Same index, different provider
    ('SPY', 'VOO'), ('SPY', 'IVV'), ('QQQ', 'QQQM'),
    ('VTI', 'ITOT'), ('IVV', 'VOO'),
    # Overlapping universes
    ('SPY', 'QQQ'), ('SPY', 'VTI'), ('VOO', 'QQQ'), ('VTI', 'QQQ'),
    ('SPY', 'DIA'), ('SPY', 'RSP'),
    # Sector subsets
    ('SPY', 'VGT'), ('SPY', 'XLK'), ('SPY', 'XLF'),
    ('SPY', 'XLE'), ('SPY', 'XLV'), ('QQQ', 'VGT'), ('QQQ', 'SMH'),
    # Factor ETFs
    ('SPY', 'QUAL'), ('SPY', 'MTUM'), ('SPY', 'VLUE'),
    ('IWF', 'IWD'), ('VTV', 'VUG'), ('SPYG', 'SPYV'),
    # Dividend / income
    ('SPY', 'SCHD'), ('SCHD', 'VYM'), ('HDV', 'VYM'), ('HDV', 'SCHD'),
    ('VIG', 'DGRO'), ('JEPI', 'JEPQ'),
    # Growth vs value
    ('VTI', 'VOO'), ('SPY', 'SPYG'), ('SPY', 'SPYV'),
    # Genuine diversification (low overlap expected)
    ('SPY', 'VXUS'), ('SPY', 'VEA'), ('SPY', 'VWO'), ('SPY', 'EFA'),
    ('VTI', 'VXUS'), ('VTI', 'VEA'), ('VTI', 'VWO'),
    ('SPY', 'IWM'), ('VOO', 'VGT'),
    # NOTE: SPY/BND excluded - FMP data stores bond issuer tickers identically
    # to stock tickers, causing false overlap (BND holds corporate bonds issued
    # by SPY constituents, not the same equity shares).
]

# Three-fund and popular portfolio templates
PORTFOLIO_TEMPLATES = {
    "Classic Three-Fund (equity only)": ["VTI", "VXUS"],
    "Simplified Two-Fund": ["VTI", "VXUS"],
    "S&P 500 + Nasdaq": ["SPY", "QQQ"],
    "S&P 500 Triple": ["SPY", "VOO", "IVV"],
    "Growth + Value": ["VUG", "VTV"],
    "US + Dividend": ["SPY", "SCHD"],
    "Tech Heavy": ["QQQ", "VGT", "SMH"],
    "Broad + Sector": ["SPY", "XLK", "XLF", "XLV", "XLE"],
    "All-in-One Equity": ["VTI", "VXUS", "VWO"],
}


def fetch_holdings(cr, etf_list, verbose=False):
    """Fetch holdings for a list of ETFs. Returns dict of {etf: {asset: weight}}."""
    etf_filter = ",".join(f"'{e}'" for e in etf_list)
    sql = f"""
        SELECT symbol, asset, weightPercentage
        FROM etf_holder
        WHERE symbol IN ({etf_filter})
          AND asset IS NOT NULL AND asset != ''
          AND asset NOT LIKE '%.NE'
    """
    if verbose:
        print(f"  Fetching holdings for {len(etf_list)} ETFs...")

    data = cr.query(sql, verbose=verbose, memory_mb=16384, threads=6, timeout=300)

    holdings = {}
    for row in data:
        sym = row['symbol']
        if sym not in holdings:
            holdings[sym] = {}
        asset = row['asset']
        weight = row.get('weightPercentage') or 0
        holdings[sym][asset] = weight

    if verbose:
        found = len(holdings)
        missing = [e for e in etf_list if e not in holdings]
        print(f"  Found {found}/{len(etf_list)} ETFs")
        if missing:
            print(f"  Missing: {', '.join(missing)}")
    return holdings


def fetch_etf_info(cr, etf_list, verbose=False):
    """Fetch ETF metadata (name, AUM, expense ratio)."""
    etf_filter = ",".join(f"'{e}'" for e in etf_list)
    sql = f"""
        SELECT symbol, name, expenseRatio,
               assetsUnderManagement, holdingsCount, etfCompany
        FROM etf_info
        WHERE symbol IN ({etf_filter})
    """
    data = cr.query(sql, verbose=verbose, memory_mb=4096, threads=2)
    return {r['symbol']: r for r in data}


def compute_pair_overlap(holdings, etf_a, etf_b):
    """Compute count-based and weight-based overlap between two ETFs."""
    if etf_a not in holdings or etf_b not in holdings:
        return None

    set_a = set(holdings[etf_a].keys())
    set_b = set(holdings[etf_b].keys())
    shared = set_a & set_b

    n_a = len(set_a)
    n_b = len(set_b)
    n_shared = len(shared)
    smaller = min(n_a, n_b)

    count_overlap_pct = round(n_shared * 100 / smaller, 1) if smaller > 0 else 0

    # Weight-based overlap: sum of min(weight_a, weight_b) for shared stocks
    weight_overlap = 0
    weight_a_shared = 0
    weight_b_shared = 0
    for stock in shared:
        wa = holdings[etf_a].get(stock, 0)
        wb = holdings[etf_b].get(stock, 0)
        weight_overlap += min(wa, wb)
        weight_a_shared += wa
        weight_b_shared += wb

    return {
        "etf_a": etf_a,
        "etf_b": etf_b,
        "shared_count": n_shared,
        "holdings_a": n_a,
        "holdings_b": n_b,
        "count_overlap_pct": count_overlap_pct,
        "weight_overlap_pct": round(weight_overlap, 2),
        "weight_a_in_shared": round(weight_a_shared, 2),
        "weight_b_in_shared": round(weight_b_shared, 2),
    }


def analyze_portfolio_template(holdings, name, etfs):
    """Analyze overlap within a portfolio template."""
    available = [e for e in etfs if e in holdings]
    if len(available) < 2:
        return None

    # Union of all holdings
    all_stocks = set()
    for etf in available:
        all_stocks.update(holdings[etf].keys())

    # Count how many ETFs hold each stock
    stock_counts = {}
    for stock in all_stocks:
        count = sum(1 for etf in available if stock in holdings[etf])
        stock_counts[stock] = count

    unique_stocks = len(all_stocks)
    redundant = sum(1 for s, c in stock_counts.items() if c > 1)
    max_overlap = max(stock_counts.values()) if stock_counts else 0

    # Pairwise overlaps
    pairs = []
    for i, a in enumerate(available):
        for b in available[i+1:]:
            r = compute_pair_overlap(holdings, a, b)
            if r:
                pairs.append(r)

    return {
        "name": name,
        "etfs": available,
        "unique_stocks": unique_stocks,
        "redundant_stocks": redundant,
        "redundancy_pct": round(redundant * 100 / unique_stocks, 1) if unique_stocks else 0,
        "max_etf_overlap": max_overlap,
        "pair_overlaps": pairs,
    }


def compute_most_widely_held(cr, top_n=30, verbose=False):
    """Find stocks held by the most ETFs."""
    sql = f"""
        SELECT asset,
               COUNT(DISTINCT symbol) as etf_count,
               ROUND(SUM(marketValue) / 1e9, 2) as total_value_bn
        FROM etf_holder
        WHERE asset IS NOT NULL AND asset != ''
          AND asset NOT LIKE '%.NE'
          AND asset NOT IN ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF')
        GROUP BY asset
        ORDER BY etf_count DESC
        LIMIT {top_n}
    """
    if verbose:
        print("  Fetching most widely held stocks...")
    return cr.query(sql, verbose=verbose, memory_mb=16384, threads=6)


def compute_universe_stats(cr, verbose=False):
    """Get overall ETF universe statistics."""
    sql = """
        SELECT
            COUNT(DISTINCT symbol) as total_etfs,
            COUNT(*) as total_holdings,
            COUNT(DISTINCT asset) as unique_stocks,
            COUNT(DISTINCT CASE WHEN weightPercentage IS NOT NULL THEN symbol END) as etfs_with_weights
        FROM etf_holder
        WHERE asset IS NOT NULL AND asset != ''
          AND asset NOT LIKE '%.NE'
          AND asset NOT IN ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF')
    """
    if verbose:
        print("  Fetching universe stats...")
    r = cr.query(sql, verbose=verbose, memory_mb=16384, threads=6)
    return r[0] if r else {}


def compute_all_pairs_overlap(holdings, min_holdings=50, min_shared=5, verbose=False):
    """Compute overlap for all ETF pairs. Only includes ETFs with min_holdings."""
    large_etfs = {e: h for e, h in holdings.items() if len(h) >= min_holdings}
    if verbose:
        print(f"  Computing all-pairs for {len(large_etfs)} ETFs with {min_holdings}+ holdings...")

    results = []
    etf_names = sorted(large_etfs.keys())
    total_pairs = len(etf_names) * (len(etf_names) - 1) // 2
    checked = 0

    for i, a in enumerate(etf_names):
        for b in etf_names[i+1:]:
            shared = set(large_etfs[a].keys()) & set(large_etfs[b].keys())
            checked += 1
            if len(shared) < min_shared:
                continue

            n_a = len(large_etfs[a])
            n_b = len(large_etfs[b])
            smaller = min(n_a, n_b)
            overlap_pct = round(len(shared) * 100 / smaller, 1) if smaller > 0 else 0

            results.append({
                "etf_a": a, "etf_b": b,
                "shared_count": len(shared),
                "holdings_a": n_a, "holdings_b": n_b,
                "overlap_pct": overlap_pct,
            })

        if verbose and (i + 1) % 100 == 0:
            print(f"    Processed {i+1}/{len(etf_names)} ETFs ({checked}/{total_pairs} pairs)...")

    results.sort(key=lambda x: x['overlap_pct'], reverse=True)
    if verbose:
        print(f"  Found {len(results)} pairs with {min_shared}+ shared stocks")
    return results


def print_results(pair_results, etf_info, portfolio_results, widely_held, universe_stats):
    """Print formatted results to stdout."""
    print("\n" + "=" * 80)
    print("ETF OVERLAP ANALYSIS")
    print("=" * 80)

    # Universe
    if universe_stats:
        print(f"\nUniverse: {universe_stats.get('total_etfs', '?'):,} ETFs, "
              f"{universe_stats.get('total_holdings', '?'):,} holdings, "
              f"{universe_stats.get('unique_stocks', '?'):,} unique stocks")

    # Pair overlaps
    print(f"\n{'─' * 80}")
    print("PAIRWISE OVERLAP")
    print(f"{'─' * 80}")
    print(f"{'ETF A':<8} {'ETF B':<8} {'Shared':>7} {'of A':>6} {'of B':>6} "
          f"{'Count%':>8} {'Wt Overlap':>11}")
    print("-" * 62)

    for r in pair_results:
        if r is None:
            continue
        wo = f"{r['weight_overlap_pct']:.1f}%" if r.get('weight_overlap_pct') else "N/A"
        print(f"{r['etf_a']:<8} {r['etf_b']:<8} {r['shared_count']:>7} "
              f"{r['holdings_a']:>6} {r['holdings_b']:>6} "
              f"{r['count_overlap_pct']:>7.1f}% {wo:>11}")

    # Portfolio templates
    if portfolio_results:
        print(f"\n{'─' * 80}")
        print("PORTFOLIO TEMPLATE ANALYSIS")
        print(f"{'─' * 80}")
        for pr in portfolio_results:
            if pr is None:
                continue
            etfs_str = " + ".join(pr['etfs'])
            print(f"\n  {pr['name']}: {etfs_str}")
            print(f"    Unique stocks: {pr['unique_stocks']}")
            print(f"    Redundant (in 2+ ETFs): {pr['redundant_stocks']} ({pr['redundancy_pct']}%)")
            for p in pr.get('pair_overlaps', []):
                print(f"      {p['etf_a']}/{p['etf_b']}: {p['count_overlap_pct']:.1f}% "
                      f"({p['shared_count']} shared)")

    # Most widely held
    if widely_held:
        print(f"\n{'─' * 80}")
        print("MOST WIDELY HELD STOCKS")
        print(f"{'─' * 80}")
        print(f"{'Stock':<10} {'ETFs':>8} {'Total Value ($B)':>16}")
        print("-" * 36)
        for r in widely_held[:20]:
            print(f"{r['asset']:<10} {r['etf_count']:>8} {r.get('total_value_bn', 0):>16}")

    print("\n" + "=" * 80)


def build_output(pair_results, etf_info_map, portfolio_results, widely_held,
                 universe_stats, all_pairs=None):
    """Build structured JSON output."""
    # Categorize pairs
    categories = {
        "same_index": [],
        "overlapping_universe": [],
        "sector_subset": [],
        "factor": [],
        "dividend": [],
        "genuine_diversification": [],
        "other": [],
    }

    same_index_pairs = {
        ('SPY', 'VOO'), ('SPY', 'IVV'), ('QQQ', 'QQQM'),
        ('VTI', 'ITOT'), ('IVV', 'VOO'),
    }
    overlap_pairs = {
        ('SPY', 'QQQ'), ('SPY', 'VTI'), ('VOO', 'QQQ'), ('VTI', 'QQQ'),
        ('SPY', 'DIA'), ('SPY', 'RSP'), ('VTI', 'VOO'),
    }
    sector_pairs = {
        ('SPY', 'VGT'), ('SPY', 'XLK'), ('SPY', 'XLF'),
        ('SPY', 'XLE'), ('SPY', 'XLV'), ('QQQ', 'VGT'),
        ('QQQ', 'SMH'), ('VOO', 'VGT'),
    }
    factor_pairs = {
        ('SPY', 'QUAL'), ('SPY', 'MTUM'), ('SPY', 'VLUE'),
        ('IWF', 'IWD'), ('VTV', 'VUG'), ('SPYG', 'SPYV'),
    }
    dividend_pairs = {
        ('SPY', 'SCHD'), ('SCHD', 'VYM'), ('HDV', 'VYM'), ('HDV', 'SCHD'),
        ('VIG', 'DGRO'), ('JEPI', 'JEPQ'),
    }
    diversification_pairs = {
        ('SPY', 'VXUS'), ('SPY', 'VEA'), ('SPY', 'VWO'), ('SPY', 'EFA'),
        ('VTI', 'VXUS'), ('VTI', 'VEA'), ('VTI', 'VWO'),
        ('SPY', 'IWM'),
    }

    for r in pair_results:
        if r is None:
            continue
        pair = (r['etf_a'], r['etf_b'])
        pair_r = (r['etf_b'], r['etf_a'])
        if pair in same_index_pairs or pair_r in same_index_pairs:
            categories["same_index"].append(r)
        elif pair in overlap_pairs or pair_r in overlap_pairs:
            categories["overlapping_universe"].append(r)
        elif pair in sector_pairs or pair_r in sector_pairs:
            categories["sector_subset"].append(r)
        elif pair in factor_pairs or pair_r in factor_pairs:
            categories["factor"].append(r)
        elif pair in dividend_pairs or pair_r in dividend_pairs:
            categories["dividend"].append(r)
        elif pair in diversification_pairs or pair_r in diversification_pairs:
            categories["genuine_diversification"].append(r)
        else:
            categories["other"].append(r)

    output = {
        "analysis": "ETF Overlap",
        "data_source": "FMP ETF Holdings via Ceta Research",
        "universe": universe_stats,
        "pair_overlaps": {
            "all": pair_results,
            "by_category": categories,
        },
        "portfolio_templates": portfolio_results,
        "most_widely_held": widely_held,
        "etf_info": {sym: info for sym, info in etf_info_map.items()},
    }

    if all_pairs:
        output["all_pairs_top100"] = all_pairs[:100]

    return output


def main():
    parser = argparse.ArgumentParser(description="ETF Overlap Analysis")
    parser.add_argument("--pair", type=str,
                        help="Analyze a specific pair (e.g., SPY,QQQ)")
    parser.add_argument("--etfs", type=str,
                        help="Comma-separated list of ETFs to analyze")
    parser.add_argument("--all-pairs", action="store_true",
                        help="Compute all-pairs overlap (50+ holdings ETFs)")
    parser.add_argument("--min-holdings", type=int, default=50,
                        help="Min holdings for all-pairs mode (default: 50)")
    parser.add_argument("--output", "-o", type=str,
                        help="Output JSON file path")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose output")
    parser.add_argument("--api-key", type=str, help="API key")
    parser.add_argument("--base-url", type=str, help="API base URL")
    args = parser.parse_args()

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    t0 = time.time()

    # Determine which ETFs to analyze
    if args.pair:
        etf_list = [e.strip().upper() for e in args.pair.split(",")]
        pairs_to_check = [(etf_list[0], etf_list[1])] if len(etf_list) >= 2 else []
    elif args.etfs:
        etf_list = [e.strip().upper() for e in args.etfs.split(",")]
        pairs_to_check = [(etf_list[i], etf_list[j])
                          for i in range(len(etf_list))
                          for j in range(i+1, len(etf_list))]
    else:
        etf_list = POPULAR_ETFS
        pairs_to_check = INTERESTING_PAIRS

    # Phase 1: Fetch data
    print("Phase 1: Fetching data...")
    holdings = fetch_holdings(cr, etf_list, verbose=args.verbose)
    etf_info_map = fetch_etf_info(cr, list(holdings.keys()), verbose=args.verbose)
    universe_stats = compute_universe_stats(cr, verbose=args.verbose)

    # Phase 2: Compute pairwise overlaps
    print("\nPhase 2: Computing pairwise overlaps...")
    pair_results = []
    for a, b in pairs_to_check:
        r = compute_pair_overlap(holdings, a, b)
        if r:
            pair_results.append(r)

    pair_results.sort(key=lambda x: x['count_overlap_pct'], reverse=True)

    # Phase 3: Portfolio template analysis
    print("\nPhase 3: Analyzing portfolio templates...")
    portfolio_results = []
    for name, etfs in PORTFOLIO_TEMPLATES.items():
        r = analyze_portfolio_template(holdings, name, etfs)
        if r:
            portfolio_results.append(r)

    # Phase 4: Most widely held stocks
    print("\nPhase 4: Finding most widely held stocks...")
    widely_held = compute_most_widely_held(cr, top_n=30, verbose=args.verbose)

    # Phase 5: All-pairs (optional)
    all_pairs = None
    if args.all_pairs:
        print("\nPhase 5: Computing all-pairs overlap...")
        # Fetch all ETFs with 50+ holdings
        print("  Fetching all large ETFs...")
        large_etf_sql = f"""
            SELECT symbol FROM (
                SELECT symbol, COUNT(DISTINCT asset) as cnt
                FROM etf_holder
                WHERE asset IS NOT NULL AND asset != ''
                  AND asset NOT LIKE '%.NE'
                GROUP BY symbol
                HAVING COUNT(DISTINCT asset) >= {args.min_holdings}
            ) sub
        """
        large_etfs_data = cr.query(large_etf_sql, verbose=args.verbose, memory_mb=4096, threads=2)
        large_etf_names = [r['symbol'] for r in large_etfs_data]
        print(f"  Found {len(large_etf_names)} ETFs with {args.min_holdings}+ holdings")

        # Fetch all their holdings
        all_holdings = fetch_holdings(cr, large_etf_names, verbose=args.verbose)

        # Compute overlaps locally
        all_pairs = compute_all_pairs_overlap(all_holdings, min_holdings=args.min_holdings,
                                               verbose=args.verbose)

    total_time = time.time() - t0

    # Print results
    print_results(pair_results, etf_info_map, portfolio_results, widely_held, universe_stats)

    if all_pairs:
        print(f"\n{'─' * 80}")
        print(f"ALL-PAIRS TOP 50 (ETFs with {args.min_holdings}+ holdings)")
        print(f"{'─' * 80}")
        print(f"{'ETF A':<10} {'ETF B':<10} {'Shared':>7} {'of A':>6} {'of B':>6} {'Overlap%':>10}")
        print("-" * 52)
        for r in all_pairs[:50]:
            print(f"{r['etf_a']:<10} {r['etf_b']:<10} {r['shared_count']:>7} "
                  f"{r['holdings_a']:>6} {r['holdings_b']:>6} {r['overlap_pct']:>9.1f}%")

    print(f"\nTotal time: {total_time:.0f}s")

    # Save output
    if args.output:
        output = build_output(pair_results, etf_info_map, portfolio_results,
                              widely_held, universe_stats, all_pairs)
        os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
