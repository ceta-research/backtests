#!/usr/bin/env python3
"""Pairs Trading Cointegration Analysis.

Tests statistical cointegration on candidate pairs from the pairs-screening step.
Uses the Engle-Granger two-step method: OLS regression on price levels, then ADF
test on the residual spread. Filters to pairs with statistically significant
mean reversion (ADF p < 0.05) and a practical half-life (5-120 trading days).

This is a statistical analysis pipeline, not a trading backtest. It produces
cointegrated_pairs.csv for use in downstream z-score signal generation and
portfolio backtesting.

Methodology:
    1. OLS: P_A = beta * P_B + intercept (price levels, not log)
    2. Spread = P_A - beta * P_B
    3. ADF test on spread (maxlag=20, autolag='AIC')
    4. Half-life from AR(1) fit: delta_spread(t) = alpha * spread(t-1) + epsilon
       half_life = -log(2) / log(ar1_beta) where ar1_beta is AR(1) coefficient

Filters:
    ADF p-value < 0.05 AND 5 <= half_life <= 120 trading days

Academic references:
    Engle, R. & Granger, C. (1987). "Co-integration and Error Correction."
        Econometrica, 55(2), 251-276.
    Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading."
        Review of Financial Studies, 19(3), 797-827.

Usage:
    # Run with default paths (reads from pairs-screening output)
    python3 pairs-cointegration/backtest.py

    # Specify input/output paths
    python3 pairs-cointegration/backtest.py \\
        --input path/to/candidate_pairs.csv \\
        --output path/to/cointegrated_pairs.csv

    # Verbose output
    python3 pairs-cointegration/backtest.py --verbose

See README.md for full methodology and data source details.
"""

import argparse
import csv
import io
import math
import os
import sys
import time
import tempfile
from collections import defaultdict
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import linregress
from statsmodels.tsa.stattools import adfuller

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

# ─── Default paths ────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INPUT = os.path.join(
    _ROOT, "..", "ts-content-creator", "content", "_current",
    "pairs-02-screening", "results", "candidate_pairs.csv"
)
DEFAULT_OUTPUT = os.path.join(
    _ROOT, "..", "ts-content-creator", "content", "_current",
    "pairs-03-cointegration", "results", "cointegrated_pairs.csv"
)

# ─── Analysis parameters ──────────────────────────────────────────────────────
ADF_PVALUE_THRESHOLD = 0.05     # ADF test significance level
MIN_HALF_LIFE_DAYS   = 5        # Too fast to trade practically
MAX_HALF_LIFE_DAYS   = 120      # Too slow for capital efficiency
MIN_OVERLAP_DAYS     = 150      # Minimum common trading days required
ADF_MAXLAG           = 20       # ADF test max lag (higher than default for daily data)
PRICE_BATCH_SIZE     = 100      # Symbols per API batch to avoid payload limits
DEFAULT_LOOKBACK_DATE = "2024-01-01"

# API resource allocation (conservative — data fetch only, not heavy backtest)
RESOURCES = {"memoryMb": 4096, "threads": 2}


def load_candidate_pairs(input_path):
    """Load candidate pairs CSV. Returns list of dicts."""
    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        print("Run pairs-screening/screen.py --global first to generate candidate_pairs.csv")
        sys.exit(1)

    pairs = []
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pairs.append(row)

    print(f"Loaded {len(pairs):,} candidate pairs from {input_path}")
    return pairs


def get_unique_symbols(pairs):
    """Extract unique symbols from pairs list."""
    symbols = set()
    for p in pairs:
        symbols.add(p["symbol_a"])
        symbols.add(p["symbol_b"])
    return sorted(symbols)


def fetch_price_batch(cr, symbols, lookback_date, verbose=False):
    """Fetch price data for a batch of symbols. Returns parquet bytes or None."""
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_list})
          AND date >= '{lookback_date}'
          AND adjClose IS NOT NULL
          AND adjClose > 0
        ORDER BY symbol, trade_date
    """
    for attempt in range(3):
        try:
            parquet_bytes = cr.query(
                sql,
                format="parquet",
                limit=2_000_000,
                timeout=300,
                verbose=verbose,
                memory_mb=RESOURCES["memoryMb"],
                threads=RESOURCES["threads"],
            )
            return parquet_bytes
        except Exception as e:
            err_str = str(e)
            if "Rate limited" in err_str and attempt < 2:
                wait = 65
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif attempt < 2:
                print(f"    Batch fetch error (attempt {attempt+1}/3): {err_str[:100]}")
                time.sleep(5)
            else:
                print(f"    Batch fetch failed after 3 attempts: {err_str[:200]}")
                return None
    return None


def fetch_all_prices(cr, symbols, lookback_date, verbose=False):
    """Fetch prices for all symbols in batches of PRICE_BATCH_SIZE.

    Loads everything into an in-memory DuckDB connection for fast pair lookups.
    Returns DuckDB connection with table 'prices' (symbol, trade_date, adjClose).
    """
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='2GB'")
    con.execute("""
        CREATE TABLE prices (
            symbol   VARCHAR,
            trade_date DATE,
            adjClose DOUBLE
        )
    """)

    total_rows = 0
    n_batches = math.ceil(len(symbols) / PRICE_BATCH_SIZE)

    print(f"\nFetching price data for {len(symbols):,} symbols "
          f"in {n_batches} batches of {PRICE_BATCH_SIZE}...")

    for i in range(0, len(symbols), PRICE_BATCH_SIZE):
        batch = symbols[i:i + PRICE_BATCH_SIZE]
        batch_num = i // PRICE_BATCH_SIZE + 1

        if verbose:
            print(f"  Batch {batch_num}/{n_batches}: {batch[0]} ... {batch[-1]}")
        else:
            # Show progress every 5 batches
            if batch_num == 1 or batch_num % 5 == 0 or batch_num == n_batches:
                print(f"  Batch {batch_num}/{n_batches} ({i+1}-{min(i+PRICE_BATCH_SIZE, len(symbols))} of {len(symbols)} symbols)")

        parquet_bytes = fetch_price_batch(cr, batch, lookback_date, verbose=verbose)
        if not parquet_bytes:
            print(f"    WARNING: No data for batch {batch_num}, skipping")
            continue

        # Write parquet to temp file and load into DuckDB
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            f.write(parquet_bytes)
            tmp_path = f.name

        try:
            count = con.execute(f"""
                INSERT INTO prices
                SELECT symbol, trade_date, adjClose
                FROM read_parquet('{tmp_path}')
            """).fetchone()
            rows_inserted = con.execute(
                f"SELECT COUNT(*) FROM prices WHERE symbol IN ({', '.join(repr(s) for s in batch)})"
            ).fetchone()[0]
            total_rows += rows_inserted
        finally:
            os.unlink(tmp_path)

    print(f"Total price rows loaded: {total_rows:,}")

    # Create index for fast lookups
    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")

    # Show coverage summary
    coverage = con.execute("""
        SELECT COUNT(DISTINCT symbol) AS n_symbols,
               MIN(trade_date) AS date_start,
               MAX(trade_date) AS date_end,
               COUNT(*) AS total_rows
        FROM prices
    """).fetchone()
    if coverage:
        print(f"Coverage: {coverage[0]:,} symbols | {coverage[1]} to {coverage[2]} | {coverage[3]:,} rows")

    return con


def get_pair_prices(con, symbol_a, symbol_b, min_days=MIN_OVERLAP_DAYS):
    """Get aligned price series for a pair.

    Returns (prices_a, prices_b, date_start, date_end) or None if insufficient overlap.
    prices_a and prices_b are numpy arrays aligned by date.
    """
    rows = con.execute(f"""
        SELECT a.trade_date, a.adjClose AS price_a, b.adjClose AS price_b
        FROM prices a
        JOIN prices b ON a.trade_date = b.trade_date
        WHERE a.symbol = '{symbol_a}'
          AND b.symbol = '{symbol_b}'
          AND a.adjClose > 0
          AND b.adjClose > 0
        ORDER BY a.trade_date
    """).fetchall()

    if len(rows) < min_days:
        return None

    dates    = [r[0] for r in rows]
    prices_a = np.array([float(r[1]) for r in rows])
    prices_b = np.array([float(r[2]) for r in rows])

    return prices_a, prices_b, dates[0], dates[-1]


def compute_half_life(spread):
    """Compute mean-reversion half-life from AR(1) fit on spread differences.

    Model: delta_spread(t) = alpha + beta * spread(t-1) + epsilon
    Half-life = -log(2) / log(1 + beta) if beta is negative (mean-reverting)

    Returns half_life in days, or None if spread is not mean-reverting.
    """
    spread_lag  = spread[:-1]
    spread_diff = np.diff(spread)

    if len(spread_lag) < 10:
        return None

    # Fit: delta_spread = alpha + beta * spread_lag
    slope, intercept, r_value, p_value, std_err = linregress(spread_lag, spread_diff)

    # AR(1) coefficient of the spread level: spread(t) = (1 + beta) * spread(t-1) + ...
    ar1_beta = 1.0 + slope

    # Mean-reverting if ar1_beta < 1 (slope < 0)
    if ar1_beta >= 1.0 or ar1_beta <= 0.0:
        return None, ar1_beta

    half_life = -math.log(2.0) / math.log(ar1_beta)

    if not math.isfinite(half_life) or half_life <= 0:
        return None, ar1_beta

    return half_life, ar1_beta


def test_cointegration(prices_a, prices_b):
    """Run Engle-Granger cointegration test on a price pair.

    Step 1: OLS regression P_A = beta * P_B + intercept (price levels)
    Step 2: Construct spread = P_A - beta * P_B
    Step 3: ADF test on spread to test for stationarity
    Step 4: Half-life from AR(1) fit on spread differences

    Returns dict with all stats, or None on error.
    """
    if len(prices_a) < 30 or len(prices_b) < 30:
        return None

    try:
        # OLS on price levels (not log)
        slope, intercept, r_value, p_value, std_err = linregress(prices_b, prices_a)
        beta       = slope
        r_squared  = r_value ** 2

        # Construct spread
        spread = prices_a - beta * prices_b

        # Spread descriptive stats
        spread_mean = float(np.mean(spread))
        spread_std  = float(np.std(spread, ddof=1))
        spread_skew = float(pd.Series(spread).skew())
        spread_kurt = float(pd.Series(spread).kurt())

        if spread_std < 1e-10:
            # Degenerate spread — essentially identical prices
            return None

        # ADF test (Augmented Dickey-Fuller) on the spread
        adf_result = adfuller(spread, maxlag=ADF_MAXLAG, autolag="AIC")
        adf_stat   = float(adf_result[0])
        adf_pvalue = float(adf_result[1])

        # Half-life from AR(1)
        hl_result = compute_half_life(spread)
        if hl_result is None:
            return None
        half_life, ar1_beta = hl_result

        return {
            "hedge_ratio":   round(beta, 6),
            "intercept":     round(float(intercept), 6),
            "adf_stat":      round(adf_stat, 6),
            "adf_pvalue":    round(adf_pvalue, 6),
            "r_squared":     round(r_squared, 6),
            "half_life_days": round(half_life, 2) if half_life is not None else None,
            "ar1_beta":      round(ar1_beta, 6),
            "spread_mean":   round(spread_mean, 6),
            "spread_std":    round(spread_std, 6),
            "spread_skew":   round(spread_skew, 4),
            "spread_kurt":   round(spread_kurt, 4),
            "n_observations": len(prices_a),
        }

    except Exception as e:
        return None


def passes_filters(stats):
    """Check if a pair passes all cointegration filters."""
    if stats is None:
        return False
    if stats["adf_pvalue"] is None or stats["adf_pvalue"] >= ADF_PVALUE_THRESHOLD:
        return False
    if stats["half_life_days"] is None:
        return False
    if not (MIN_HALF_LIFE_DAYS <= stats["half_life_days"] <= MAX_HALF_LIFE_DAYS):
        return False
    return True


def print_summary(total_candidates, results, sector_counts):
    """Print analysis summary with sector breakdown."""
    passed = [r for r in results if passes_filters(r["stats"])]
    n_passed = len(passed)
    pass_rate = 100.0 * n_passed / total_candidates if total_candidates > 0 else 0

    half_lives = [r["stats"]["half_life_days"] for r in passed if r["stats"]["half_life_days"]]
    avg_hl    = float(np.mean(half_lives)) if half_lives else 0
    median_hl = float(np.median(half_lives)) if half_lives else 0
    min_hl    = min(half_lives) if half_lives else 0
    max_hl    = max(half_lives) if half_lives else 0

    print()
    print("Cointegration Analysis Complete")
    print("================================")
    print(f"Candidates tested:    {total_candidates:,}")
    print(f"Passed (ADF p<0.05):    {n_passed:,}")
    print(f"Pass rate:            {pass_rate:.1f}%")
    print(f"Avg half-life:        {avg_hl:.1f} days")
    print(f"Median half-life:     {median_hl:.1f} days")
    print(f"Half-life range:       {min_hl:.1f} - {max_hl:.1f} days")

    # Sector breakdown from sector_counts
    if sector_counts:
        print()
        print("By sector:")
        sorted_sectors = sorted(
            sector_counts.items(),
            key=lambda x: x[1]["passed"] / x[1]["total"] if x[1]["total"] > 0 else 0,
            reverse=True,
        )
        for sector, counts in sorted_sectors:
            t = counts["total"]
            p = counts["passed"]
            pct = 100.0 * p / t if t > 0 else 0
            print(f"  {sector:<25}: {p}/{t} = {pct:.1f}%")


def save_results(results, output_path):
    """Save cointegrated pairs to CSV."""
    passed = [r for r in results if passes_filters(r["stats"])]

    # Sort by ADF p-value (most significant first), then half-life
    passed.sort(key=lambda x: (x["stats"]["adf_pvalue"], x["stats"]["half_life_days"] or 999))

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    fieldnames = [
        "symbol_a", "symbol_b", "sector",
        "hedge_ratio", "intercept",
        "adf_stat", "adf_pvalue",
        "r_squared",
        "half_life_days", "ar1_beta",
        "spread_mean", "spread_std", "spread_skew", "spread_kurt",
        "n_observations",
        "date_start", "date_end",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in passed:
            row = {
                "symbol_a":      r["symbol_a"],
                "symbol_b":      r["symbol_b"],
                "sector":        r["sector"],
                "date_start":    r["date_start"],
                "date_end":      r["date_end"],
            }
            row.update(r["stats"])
            writer.writerow(row)

    print(f"\nSaved {len(passed)} cointegrated pairs to {output_path}")
    return len(passed)


def main():
    parser = argparse.ArgumentParser(
        description="Pairs Trading Cointegration Analysis (Engle-Granger two-step)"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help=f"Path to candidate_pairs.csv (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Path for cointegrated_pairs.csv (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--lookback-date",
        type=str,
        default=DEFAULT_LOOKBACK_DATE,
        help=f"Start date for price data (default: {DEFAULT_LOOKBACK_DATE})",
    )
    parser.add_argument(
        "--min-days",
        type=int,
        default=MIN_OVERLAP_DAYS,
        help=f"Minimum overlapping trading days required (default: {MIN_OVERLAP_DAYS})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show API progress and per-pair stats",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="API key (or set CR_API_KEY env var)",
    )
    args = parser.parse_args()

    print("Pairs Cointegration Analysis")
    print(f"ADF threshold: p < {ADF_PVALUE_THRESHOLD}  |  Half-life: {MIN_HALF_LIFE_DAYS}-{MAX_HALF_LIFE_DAYS} days")
    print(f"Lookback start: {args.lookback_date}  |  Min overlap: {args.min_days} days")
    print()

    # ── Phase 1: Load candidate pairs ─────────────────────────────────────────
    pairs = load_candidate_pairs(args.input)
    if not pairs:
        print("No candidate pairs to test.")
        sys.exit(0)

    # ── Phase 2: Fetch price data ──────────────────────────────────────────────
    cr = CetaResearch(api_key=args.api_key)
    unique_symbols = get_unique_symbols(pairs)

    t_start = time.time()
    con = fetch_all_prices(cr, unique_symbols, args.lookback_date, verbose=args.verbose)

    # ── Phase 3: Test cointegration for each pair ──────────────────────────────
    print(f"\nTesting cointegration for {len(pairs):,} candidate pairs...")

    results      = []
    sector_counts = defaultdict(lambda: {"total": 0, "passed": 0})

    n_no_data     = 0
    n_insufficient = 0
    n_adf_fail    = 0
    n_hl_fail     = 0
    n_passed      = 0

    report_interval = max(100, len(pairs) // 20)  # report ~20 times

    for i, pair in enumerate(pairs):
        sym_a  = pair["symbol_a"]
        sym_b  = pair["symbol_b"]
        sector = pair.get("sector", "Unknown")

        if (i + 1) % report_interval == 0 or (i + 1) == len(pairs):
            elapsed = time.time() - t_start
            print(f"  Progress: {i+1:,}/{len(pairs):,} pairs "
                  f"({100*(i+1)/len(pairs):.0f}%) | "
                  f"passed: {n_passed} | elapsed: {elapsed:.0f}s")

        # Get aligned price series
        price_data = get_pair_prices(con, sym_a, sym_b, min_days=args.min_days)
        if price_data is None:
            n_no_data += 1
            if args.verbose:
                print(f"    SKIP {sym_a}/{sym_b}: insufficient overlap (<{args.min_days} days)")
            continue

        prices_a, prices_b, date_start, date_end = price_data

        # Run cointegration tests
        stats = test_cointegration(prices_a, prices_b)

        if stats is None:
            n_insufficient += 1
            continue

        sector_counts[sector]["total"] += 1

        # Check filters
        if stats["adf_pvalue"] >= ADF_PVALUE_THRESHOLD:
            n_adf_fail += 1
            if args.verbose:
                print(f"    FAIL {sym_a}/{sym_b}: ADF p={stats['adf_pvalue']:.3f} >= {ADF_PVALUE_THRESHOLD}")
            continue

        if stats["half_life_days"] is None or not (MIN_HALF_LIFE_DAYS <= stats["half_life_days"] <= MAX_HALF_LIFE_DAYS):
            n_hl_fail += 1
            hl_str = f"{stats['half_life_days']:.1f}" if stats["half_life_days"] else "None"
            if args.verbose:
                print(f"    FAIL {sym_a}/{sym_b}: half_life={hl_str} outside [{MIN_HALF_LIFE_DAYS},{MAX_HALF_LIFE_DAYS}]")
            continue

        # Passed all filters
        n_passed += 1
        sector_counts[sector]["passed"] += 1

        results.append({
            "symbol_a":  sym_a,
            "symbol_b":  sym_b,
            "sector":    sector,
            "date_start": str(date_start),
            "date_end":   str(date_end),
            "stats":     stats,
        })

        if args.verbose:
            print(f"    PASS {sym_a}/{sym_b} ({sector}): "
                  f"ADF p={stats['adf_pvalue']:.4f}, "
                  f"half_life={stats['half_life_days']:.1f}d, "
                  f"beta={stats['hedge_ratio']:.3f}")

    elapsed_total = time.time() - t_start
    con.close()

    # ── Phase 4: Save results ──────────────────────────────────────────────────
    if results:
        save_results(results, args.output)
    else:
        print("\nNo pairs passed cointegration filters.")

    # ── Phase 5: Summary ───────────────────────────────────────────────────────
    print_summary(len(pairs), results, sector_counts)
    print(f"\nFilter breakdown:")
    print(f"  No price overlap (<{args.min_days} days): {n_no_data:,}")
    print(f"  Computation errors:               {n_insufficient:,}")
    print(f"  Failed ADF test:                  {n_adf_fail:,}")
    print(f"  Failed half-life filter:          {n_hl_fail:,}")
    print(f"  Passed:                           {n_passed:,}")
    print(f"\nTotal time: {elapsed_total:.0f}s")


if __name__ == "__main__":
    main()
