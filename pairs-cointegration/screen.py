#!/usr/bin/env python3
"""Current cointegration status screen for pairs trading.

Reads the cointegrated_pairs.csv output from backtest.py, fetches fresh price
data for the top pairs, and shows which pairs currently have extended spreads.
Pairs with |z-score| > threshold are active trading candidates: the spread is
stretched beyond its historical mean and historically tends to revert.

Signal interpretation:
    z-score > 0  → A overvalued vs B → SHORT A / LONG B (bet on spread contraction)
    z-score < 0  → A undervalued vs B → LONG A / SHORT B

Usage:
    # Show top 20 pairs by current z-score magnitude
    python3 pairs-cointegration/screen.py

    # Show top 50 Energy pairs
    python3 pairs-cointegration/screen.py --top 50 --sector Energy

    # Custom z-score threshold and input file
    python3 pairs-cointegration/screen.py --min-zscore 2.0 --input path/to/cointegrated_pairs.csv
"""

import argparse
import csv
import math
import os
import sys
import tempfile
from datetime import datetime, timedelta

import duckdb
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

# ─── Default paths ────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INPUT = os.path.join(
    _ROOT, "..", "ts-content-creator", "content", "_current",
    "pairs-03-cointegration", "results", "cointegrated_pairs.csv"
)

# ─── Screen parameters ────────────────────────────────────────────────────────
DEFAULT_TOP_N        = 20
DEFAULT_MIN_ZSCORE   = 1.5
FRESH_LOOKBACK_DAYS  = 90     # Days of fresh price data to fetch
RESOURCES = {"memoryMb": 4096, "threads": 2}


def load_cointegrated_pairs(input_path, sector_filter=None, top_n=None):
    """Load cointegrated pairs CSV. Optionally filter by sector."""
    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        print("Run backtest.py first to generate cointegrated_pairs.csv")
        sys.exit(1)

    pairs = []
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if sector_filter and row.get("sector", "") != sector_filter:
                continue
            pairs.append(row)

    if not pairs:
        msg = f" (sector: {sector_filter})" if sector_filter else ""
        print(f"No cointegrated pairs found{msg} in {input_path}")
        sys.exit(0)

    # Sort by ADF p-value (most significant first) as default ranking
    pairs.sort(key=lambda x: float(x.get("adf_pvalue", 1.0)))

    if top_n:
        pairs = pairs[:top_n]

    return pairs


def get_fresh_price_lookback():
    """Get start date for fresh price fetch (FRESH_LOOKBACK_DAYS ago)."""
    start = datetime.today() - timedelta(days=FRESH_LOOKBACK_DAYS)
    return start.strftime("%Y-%m-%d")


def fetch_fresh_prices(cr, symbols, lookback_date, verbose=False):
    """Fetch recent price data for all symbols. Returns DuckDB connection."""
    sym_list = ", ".join(f"'{s}'" for s in sorted(symbols))
    sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_list})
          AND date >= '{lookback_date}'
          AND adjClose IS NOT NULL
          AND adjClose > 0
        ORDER BY symbol, trade_date
    """

    if verbose:
        print(f"  Fetching {FRESH_LOOKBACK_DAYS}-day prices for {len(symbols)} symbols...")

    for attempt in range(3):
        try:
            parquet_bytes = cr.query(
                sql,
                format="parquet",
                limit=500_000,
                timeout=120,
                verbose=verbose,
                memory_mb=RESOURCES["memoryMb"],
                threads=RESOURCES["threads"],
            )
            break
        except Exception as e:
            if attempt < 2:
                import time
                time.sleep(5)
            else:
                print(f"  ERROR fetching prices: {e}")
                return None
    else:
        return None

    if not parquet_bytes:
        print("  No price data returned.")
        return None

    con = duckdb.connect(":memory:")
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        f.write(parquet_bytes)
        tmp_path = f.name
    try:
        con.execute(f"CREATE TABLE prices AS SELECT * FROM read_parquet('{tmp_path}')")
    finally:
        os.unlink(tmp_path)

    row_count = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    if verbose:
        sym_count = con.execute("SELECT COUNT(DISTINCT symbol) FROM prices").fetchone()[0]
        print(f"  Loaded {row_count:,} rows for {sym_count} symbols")

    return con


def compute_current_zscore(con, pair):
    """Compute current spread z-score for a pair using historical parameters.

    Uses the hedge_ratio and spread statistics from cointegration analysis
    as the baseline. Computes the current spread and normalizes by historical std.

    Returns (zscore, current_spread, n_obs) or None if insufficient data.
    """
    sym_a       = pair["symbol_a"]
    sym_b       = pair["symbol_b"]
    beta        = float(pair["hedge_ratio"])
    spread_mean = float(pair["spread_mean"])
    spread_std  = float(pair["spread_std"])

    if spread_std < 1e-10:
        return None

    # Get aligned prices for the fresh lookback window
    rows = con.execute(f"""
        SELECT a.trade_date, a.adjClose AS price_a, b.adjClose AS price_b
        FROM prices a
        JOIN prices b ON a.trade_date = b.trade_date
        WHERE a.symbol = '{sym_a}'
          AND b.symbol = '{sym_b}'
          AND a.adjClose > 0
          AND b.adjClose > 0
        ORDER BY a.trade_date DESC
    """).fetchall()

    if len(rows) < 5:
        return None

    # Most recent spread
    latest = rows[0]
    current_spread = float(latest[1]) - beta * float(latest[2])

    # Z-score relative to cointegration-period parameters
    zscore = (current_spread - spread_mean) / spread_std

    return zscore, current_spread, len(rows)


def format_signal(zscore):
    """Format the trading signal from a z-score."""
    if zscore > 0:
        return "SHORT A / LONG B"
    else:
        return "LONG A  / SHORT B"


def print_screen_results(screened_pairs, min_zscore, sector_filter=None):
    """Print the pairs screen results table."""
    # Filter to pairs with |z-score| >= threshold
    active = [(p, z, s, n) for p, z, s, n in screened_pairs if abs(z) >= min_zscore]

    # Sort by |z-score| descending
    active.sort(key=lambda x: abs(x[1]), reverse=True)

    sector_str = f" | Sector: {sector_filter}" if sector_filter else ""
    print()
    print(f"Current Pairs with Extended Spreads (|z-score| >= {min_zscore}){sector_str}")
    print("=" * 80)

    if not active:
        print(f"  No pairs currently have |z-score| >= {min_zscore}.")
        print(f"  {len(screened_pairs)} pairs checked, none have extended spreads right now.")
        return

    header = (
        f"{'Symbol A':<10} {'Symbol B':<10} {'Sector':<22} "
        f"{'Half-Life':>9}  {'Z-Score':>8}  {'Signal':<20} {'Days':>5}"
    )
    print(header)
    print("-" * 80)

    for pair, zscore, current_spread, n_days in active:
        hl = float(pair.get("half_life_days", 0))
        signal = format_signal(zscore)
        sector = pair.get("sector", "Unknown")[:20]
        print(
            f"{pair['symbol_a']:<10} {pair['symbol_b']:<10} {sector:<22} "
            f"{hl:>8.1f}d  {zscore:>+8.2f}  {signal:<20} {n_days:>5}"
        )

    print()
    print(f"Active: {len(active)} pairs | Checked: {len(screened_pairs)} pairs")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Show z-score distribution of checked pairs
    all_zscores = [abs(z) for _, z, _, _ in screened_pairs]
    if all_zscores:
        print()
        print("Z-score distribution (all checked pairs):")
        thresholds = [1.0, 1.5, 2.0, 2.5]
        for t in thresholds:
            cnt = sum(1 for z in all_zscores if z >= t)
            pct = 100.0 * cnt / len(all_zscores)
            print(f"  |z| >= {t:.1f}: {cnt:>4} pairs ({pct:.1f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="Current pairs spread z-score screen"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help=f"Path to cointegrated_pairs.csv (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP_N,
        dest="top_n",
        help=f"Number of top pairs to check (ranked by ADF significance, default: {DEFAULT_TOP_N})",
    )
    parser.add_argument(
        "--sector",
        type=str,
        default=None,
        help="Filter by sector (e.g. 'Energy', 'Financial Services')",
    )
    parser.add_argument(
        "--min-zscore",
        type=float,
        default=DEFAULT_MIN_ZSCORE,
        help=f"Minimum |z-score| to show in output (default: {DEFAULT_MIN_ZSCORE})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show API progress",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="API key (or set CR_API_KEY env var)",
    )
    args = parser.parse_args()

    print("Pairs Trading Current Z-Score Screen")
    print(f"Input: {args.input}")
    print(f"Top {args.top_n} pairs | min |z-score|: {args.min_zscore}")
    if args.sector:
        print(f"Sector filter: {args.sector}")
    print()

    # ── Load cointegrated pairs ────────────────────────────────────────────────
    pairs = load_cointegrated_pairs(args.input, sector_filter=args.sector, top_n=args.top_n)
    print(f"Loaded {len(pairs)} pairs to check")

    # ── Fetch fresh prices ─────────────────────────────────────────────────────
    unique_symbols = set()
    for p in pairs:
        unique_symbols.add(p["symbol_a"])
        unique_symbols.add(p["symbol_b"])

    lookback_date = get_fresh_price_lookback()
    print(f"Fetching {FRESH_LOOKBACK_DAYS}-day price history from {lookback_date}...")

    cr = CetaResearch(api_key=args.api_key)
    con = fetch_fresh_prices(cr, unique_symbols, lookback_date, verbose=args.verbose)
    if con is None:
        print("ERROR: Could not fetch price data.")
        sys.exit(1)

    # ── Compute z-scores ───────────────────────────────────────────────────────
    screened = []
    n_skipped = 0

    for pair in pairs:
        result = compute_current_zscore(con, pair)
        if result is None:
            n_skipped += 1
            continue
        zscore, current_spread, n_days = result
        screened.append((pair, zscore, current_spread, n_days))

    con.close()

    if n_skipped > 0:
        print(f"Skipped {n_skipped} pairs (insufficient recent price data)")

    # ── Print results ──────────────────────────────────────────────────────────
    print_screen_results(screened, args.min_zscore, sector_filter=args.sector)


if __name__ == "__main__":
    main()
