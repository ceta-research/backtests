#!/usr/bin/env python3
"""
Post-Stock Split Performance Event Study

Measures cumulative abnormal returns (CAR) around forward stock splits, 2000-2025.
Tests the "post-split drift" hypothesis from Fama et al. (1969) and Ikenberry et al. (1996).

Key finding: Positive pre-split CAR (+3.22% at T-5). Negative post-split drift (-2.98% at T+252).
The traditional long-side signal does NOT hold in 2000-2025 US data.

Usage:
    python3 stock-split/backtest.py                                      # default settings
    python3 stock-split/backtest.py --output results/ --verbose
    python3 stock-split/backtest.py --min-mktcap 1000000000 --verbose   # $1B+ only
    python3 stock-split/backtest.py --start-year 2010 --end-year 2025   # recent period only
    python3 stock-split/backtest.py --exchange NYSE,NASDAQ,AMEX --verbose  # US only

Data source: Ceta Research SQL API (FMP financial data warehouse)
Requires: CR_API_KEY environment variable (get key at cetaresearch.com)
"""

import argparse
import duckdb
import json
import math
import os
import sys
import csv
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet
from cli_utils import get_mktcap_threshold

# ─── Parameters ───────────────────────────────────────────────────────────────
STRATEGY_NAME = "Post-Stock Split Performance"
# DEFAULT_MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold(use_low_threshold=True)
DEFAULT_MIN_RATIO = 1.5             # Minimum forward split ratio
DEFAULT_START_YEAR = 2000
DEFAULT_END_YEAR = 2025
WINDOWS = [1, 5, 21, 63, 126, 252]  # Post-event windows (trading days)
PRE_WINDOWS = [-5]                   # Pre-event window
MAX_RETURN_CAP = 2.0                 # Cap extreme single-window returns (data artifacts)


def classify_split_ratio(ratio: float) -> str:
    if abs(ratio - 2.0) < 0.01:
        return "2-for-1"
    elif abs(ratio - 3.0) < 0.01:
        return "3-for-1"
    elif abs(ratio - 4.0) < 0.01:
        return "4-for-1"
    elif ratio >= 5.0:
        return "5-for-1+"
    return "other"


# ─── Data Fetching ────────────────────────────────────────────────────────────

def fetch_splits_and_prices(client, con, args, verbose):
    """Load split events, market cap filter, SPY prices, and stock prices into DuckDB."""

    # 1. Load splits events
    exchange_filter = ""
    if args.exchanges:
        ex_list = ", ".join(f"'{e}'" for e in args.exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_list})"

    print("  Loading split events...")
    splits_sql = f"""
        SELECT s.symbol,
               CAST(s.date AS DATE) AS event_date,
               CAST(s.numerator AS FLOAT) / NULLIF(s.denominator, 0) AS split_ratio
        FROM splits_calendar s
        {f"JOIN profile p ON s.symbol = p.symbol" if args.exchanges else ""}
        WHERE s.numerator IS NOT NULL
          AND s.denominator IS NOT NULL
          AND s.denominator > 0
          AND s.numerator > s.denominator
          AND CAST(s.numerator AS FLOAT) / s.denominator >= {args.min_ratio}
          AND CAST(s.date AS DATE) >= '{args.start_year}-01-01'
          AND CAST(s.date AS DATE) <= '{args.end_year}-12-31'
          {exchange_filter}
    """
    n_splits = query_parquet(client, splits_sql, con, "raw_splits",
                             verbose=verbose, memory_mb=4096, threads=2)
    print(f"    -> {n_splits} raw forward split events")

    # 2. Market cap filter (most recent FY key_metrics before event)
    print("  Applying market cap filter...")
    km_sql = """
        SELECT symbol, CAST(date AS DATE) AS filing_date, marketCap
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL AND marketCap > 0
    """
    query_parquet(client, km_sql, con, "km_cache", verbose=verbose, memory_mb=4096, threads=2)

    con.execute(f"""
        CREATE TABLE events AS
        WITH ranked AS (
            SELECT s.symbol, s.event_date, s.split_ratio, km.marketCap,
                   ROW_NUMBER() OVER (PARTITION BY s.symbol, s.event_date
                                      ORDER BY km.filing_date DESC) AS rn
            FROM raw_splits s
            LEFT JOIN km_cache km ON s.symbol = km.symbol AND km.filing_date <= s.event_date
        )
        SELECT symbol, event_date, split_ratio
        FROM ranked
        WHERE rn = 1 AND (marketCap IS NULL OR marketCap > {args.min_mktcap})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, event_date ORDER BY split_ratio DESC) = 1
    """)

    n_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    cat_rows = con.execute("""
        SELECT CASE
                 WHEN ABS(split_ratio - 2.0) < 0.01 THEN '2-for-1'
                 WHEN ABS(split_ratio - 3.0) < 0.01 THEN '3-for-1'
                 WHEN ABS(split_ratio - 4.0) < 0.01 THEN '4-for-1'
                 WHEN split_ratio >= 5.0 THEN '5-for-1+'
                 ELSE 'other'
               END AS cat, COUNT(*) AS n
        FROM events GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    cat_str = ", ".join(f"{cat}: {n}" for cat, n in cat_rows)
    print(f"    -> {n_events} events after market cap filter ({cat_str})")

    # 3. SPY prices
    print("  Loading SPY benchmark prices...")
    spy_sql = f"""
        SELECT CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol = 'SPY'
          AND CAST(date AS DATE) >= '{args.start_year - 1}-01-01'
          AND CAST(date AS DATE) <= '{args.end_year + 1}-12-31'
          AND adjClose IS NOT NULL AND adjClose > 0
    """
    n_spy = query_parquet(client, spy_sql, con, "spy_prices",
                          verbose=verbose, memory_mb=4096, threads=2)
    print(f"    -> {n_spy} SPY price records")

    # 4. Stock prices in batches
    unique_syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM events").fetchall()]
    print(f"  Loading prices for {len(unique_syms)} unique symbols (may take a few minutes)...")
    batch_size = 500
    batches = [unique_syms[i:i + batch_size] for i in range(0, len(unique_syms), batch_size)]

    for i, batch in enumerate(batches):
        sym_list = ", ".join(f"'{s}'" for s in batch)
        price_sql = f"""
            SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
            FROM stock_eod
            WHERE symbol IN ({sym_list})
              AND CAST(date AS DATE) >= '{args.start_year - 1}-01-01'
              AND CAST(date AS DATE) <= '{args.end_year + 1}-12-31'
              AND adjClose IS NOT NULL AND adjClose > 0
        """
        query_parquet(client, price_sql, con, f"prices_batch_{i}",
                      verbose=False, memory_mb=4096, threads=2)
        if verbose:
            print(f"    Batch {i + 1}/{len(batches)} loaded")

    # Merge price batches
    if batches:
        union_sql = " UNION ALL ".join(f"SELECT * FROM prices_batch_{i}" for i in range(len(batches)))
        con.execute(f"CREATE TABLE stock_prices AS {union_sql}")
    else:
        con.execute("CREATE TABLE stock_prices (symbol VARCHAR, trade_date DATE, adjClose DOUBLE)")

    n_prices = con.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
    print(f"    -> {n_prices} stock price records loaded")


# ─── Event Study (DuckDB SQL) ─────────────────────────────────────────────────

def run_event_study(con, all_windows, verbose):
    """Compute CAR for all events at all windows using DuckDB SQL."""
    print("  Building trading day index...")
    con.execute("""
        CREATE TABLE trading_days AS
        SELECT trade_date, ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM spy_prices
        ORDER BY trade_date
    """)

    print("  Mapping events to T0 trading days...")
    # For each event, find nearest trading day at or after event_date (up to +5 calendar days)
    con.execute("""
        CREATE TABLE event_t0 AS
        WITH nearest AS (
            SELECT e.symbol, e.event_date, e.split_ratio,
                   td.day_num AS t0_num, td.trade_date AS t0_date,
                   ROW_NUMBER() OVER (PARTITION BY e.symbol, e.event_date ORDER BY td.trade_date) AS rn
            FROM events e
            JOIN trading_days td
                ON td.trade_date >= e.event_date
                AND td.trade_date <= e.event_date + INTERVAL '5' DAY
        )
        SELECT symbol, event_date, split_ratio, t0_num, t0_date
        FROM nearest WHERE rn = 1
    """)

    print("  Getting T0 prices...")
    con.execute("""
        CREATE TABLE event_t0_prices AS
        SELECT t.symbol, t.event_date, t.split_ratio, t.t0_num, t.t0_date,
               sp.adjClose AS t0_stock, spy.adjClose AS t0_spy
        FROM event_t0 t
        JOIN stock_prices sp ON sp.symbol = t.symbol AND sp.trade_date = t.t0_date
        JOIN spy_prices spy ON spy.trade_date = t.t0_date
        WHERE sp.adjClose > 0 AND spy.adjClose > 0
    """)

    n_with_t0 = con.execute("SELECT COUNT(*) FROM event_t0_prices").fetchone()[0]
    print(f"    -> {n_with_t0} events with T0 prices")

    # Compute CAR for each event at each window
    windows_values = ", ".join(f"({w})" for w in all_windows)
    print(f"  Computing CAR for windows {all_windows}...")
    con.execute(f"""
        CREATE TABLE car_results AS
        WITH windows AS (
            SELECT * FROM (VALUES {windows_values}) t(window_offset)
        ),
        event_windows AS (
            SELECT t.symbol, t.event_date, t.split_ratio, t.t0_num, t.t0_stock, t.t0_spy,
                   w.window_offset
            FROM event_t0_prices t CROSS JOIN windows w
        ),
        window_dates AS (
            SELECT ew.*, td.trade_date AS window_date
            FROM event_windows ew
            JOIN trading_days td ON td.day_num = ew.t0_num + ew.window_offset
        )
        SELECT
            wd.symbol, wd.event_date, wd.split_ratio, wd.window_offset,
            (sp.adjClose - wd.t0_stock) / wd.t0_stock * 100 AS stock_ret,
            (spy.adjClose - wd.t0_spy) / wd.t0_spy * 100 AS spy_ret,
            ((sp.adjClose - wd.t0_stock) / wd.t0_stock
             - (spy.adjClose - wd.t0_spy) / wd.t0_spy) * 100 AS car,
            CASE
                WHEN ABS(wd.split_ratio - 2.0) < 0.01 THEN '2-for-1'
                WHEN ABS(wd.split_ratio - 3.0) < 0.01 THEN '3-for-1'
                WHEN ABS(wd.split_ratio - 4.0) < 0.01 THEN '4-for-1'
                WHEN wd.split_ratio >= 5.0 THEN '5-for-1+'
                ELSE 'other'
            END AS category
        FROM window_dates wd
        JOIN stock_prices sp ON sp.symbol = wd.symbol AND sp.trade_date = wd.window_date
        JOIN spy_prices spy ON spy.trade_date = wd.window_date
        WHERE ABS((sp.adjClose - wd.t0_stock) / wd.t0_stock) <= {MAX_RETURN_CAP}
          AND ABS((spy.adjClose - wd.t0_spy) / wd.t0_spy) <= {MAX_RETURN_CAP}
          AND sp.adjClose > 0 AND spy.adjClose > 0
    """)

    n_results = con.execute("SELECT COUNT(*) FROM car_results").fetchone()[0]
    print(f"    -> {n_results} event-window CAR observations")
    return n_results


# ─── Aggregation ─────────────────────────────────────────────────────────────

def aggregate(con, all_windows):
    """Compute summary statistics by window and by category using DuckDB SQL."""

    def window_label(w):
        return f"T{'+' if w >= 0 else ''}{w}"

    # Overall CAR by window
    overall = {}
    for w in all_windows:
        lbl = window_label(w)
        row = con.execute(f"""
            SELECT COUNT(*) as n,
                   ROUND(AVG(car), 3) as mean_car,
                   ROUND(MEDIAN(car), 3) as median_car,
                   ROUND(AVG(car) / NULLIF(STDDEV(car) / SQRT(COUNT(*)), 0), 2) as t_stat,
                   ROUND(SUM(CASE WHEN car > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as hit_rate
            FROM car_results
            WHERE window_offset = {w}
        """).fetchone()
        n, mean, median, t, hit = row
        overall[lbl] = {
            "mean_car": mean,
            "median_car": median,
            "t_stat": t,
            "n": n,
            "hit_rate": hit,
            "significant_5pct": abs(t or 0) > 1.96,
            "significant_1pct": abs(t or 0) > 2.576,
        }

    # By category
    categories = ["2-for-1", "3-for-1", "4-for-1", "5-for-1+", "other"]
    by_cat = {}
    for cat in categories:
        n_total = con.execute(f"""
            SELECT COUNT(DISTINCT symbol || '_' || CAST(event_date AS VARCHAR))
            FROM car_results WHERE category = '{cat}'
        """).fetchone()[0]
        cat_windows = {}
        for w in all_windows:
            lbl = window_label(w)
            row = con.execute(f"""
                SELECT COUNT(*) as n,
                       ROUND(AVG(car), 3) as mean_car,
                       ROUND(MEDIAN(car), 3) as median_car,
                       ROUND(AVG(car) / NULLIF(STDDEV(car) / SQRT(COUNT(*)), 0), 2) as t_stat,
                       ROUND(SUM(CASE WHEN car > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as hit_rate
                FROM car_results
                WHERE window_offset = {w} AND category = '{cat}'
            """).fetchone()
            n, mean, median, t, hit = row
            if n and n > 0:
                cat_windows[lbl] = {
                    "mean_car": mean,
                    "median_car": median,
                    "t_stat": t,
                    "n": n,
                    "hit_rate": hit,
                    "significant_5pct": abs(t or 0) > 1.96,
                    "significant_1pct": abs(t or 0) > 2.576,
                }
        by_cat[cat] = {"n": n_total, "windows": cat_windows}

    return overall, by_cat


# ─── Output ──────────────────────────────────────────────────────────────────

def save_results(con, overall, by_cat, all_windows, output_dir, args):
    """Save summary_metrics.json and event_returns.csv."""
    os.makedirs(output_dir, exist_ok=True)

    # Date range
    date_row = con.execute("""
        SELECT MIN(CAST(event_date AS VARCHAR)), MAX(CAST(event_date AS VARCHAR))
        FROM car_results
    """).fetchone()
    total_events = con.execute("""
        SELECT COUNT(DISTINCT symbol || '_' || CAST(event_date AS VARCHAR))
        FROM car_results
    """).fetchone()[0]

    summary = {
        "strategy": STRATEGY_NAME,
        "total_events": total_events,
        "date_range": {"first": date_row[0], "last": date_row[1]},
        "parameters": {
            "min_mktcap": args.min_mktcap,
            "min_ratio": args.min_ratio,
            "start_year": args.start_year,
            "end_year": args.end_year,
            "windows": all_windows,
        },
        "cumulative_abnormal_returns": overall,
        "by_category": by_cat,
    }

    json_path = os.path.join(output_dir, "summary_metrics.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"  Saved {json_path}")

    # event_returns.csv (sample - T+63 for all events)
    csv_path = os.path.join(output_dir, "event_returns.csv")
    rows = con.execute("""
        SELECT symbol, CAST(event_date AS VARCHAR) as event_date, split_ratio, category,
               window_offset, ROUND(stock_ret, 4) as stock_ret,
               ROUND(spy_ret, 4) as spy_ret, ROUND(car, 4) as car
        FROM car_results
        ORDER BY event_date, symbol, window_offset
    """).fetchall()
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "event_date", "split_ratio", "category",
                    "window", "stock_ret_pct", "spy_ret_pct", "car_pct"])
        w.writerows(rows)
    print(f"  Saved {csv_path}")

    # event_frequency.csv
    freq_path = os.path.join(output_dir, "event_frequency.csv")
    freq_rows = con.execute("""
        SELECT YEAR(event_date) as year, COUNT(DISTINCT symbol || '_' || CAST(event_date AS VARCHAR)) as total,
               COUNT(DISTINCT CASE WHEN ABS(split_ratio - 2.0) < 0.01 THEN symbol END) as "2-for-1",
               COUNT(DISTINCT CASE WHEN ABS(split_ratio - 3.0) < 0.01 THEN symbol END) as "3-for-1",
               COUNT(DISTINCT CASE WHEN ABS(split_ratio - 4.0) < 0.01 THEN symbol END) as "4-for-1",
               COUNT(DISTINCT CASE WHEN split_ratio >= 5.0 THEN symbol END) as "5-for-1+",
               COUNT(DISTINCT CASE WHEN ABS(split_ratio - 2.0) >= 0.01
                                    AND ABS(split_ratio - 3.0) >= 0.01
                                    AND ABS(split_ratio - 4.0) >= 0.01
                                    AND split_ratio < 5.0 THEN symbol END) as other
        FROM car_results
        WHERE window_offset = 1
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    with open(freq_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["year", "total", "2-for-1", "3-for-1", "4-for-1", "5-for-1+", "other"])
        w.writerows(freq_rows)
    print(f"  Saved {freq_path}")


def print_summary(overall, all_windows):
    """Print results table to console."""
    print("\n" + "=" * 65)
    print("  RESULTS: Post-Split CAR")
    print("=" * 65)
    print(f"  {'Window':<8}  {'Mean CAR':>9}  {'t-stat':>7}  {'N':>7}  {'Hit%':>6}  Sig")
    print(f"  {'-'*8}  {'-'*9}  {'-'*7}  {'-'*7}  {'-'*6}  ---")
    for w in all_windows:
        lbl = f"T{'+' if w >= 0 else ''}{w}"
        s = overall.get(lbl, {})
        if not s:
            continue
        sig = "**" if s["significant_1pct"] else ("*" if s["significant_5pct"] else "  ")
        print(f"  {lbl:<8}  {s['mean_car']:>+9.3f}%  {s['t_stat']:>+7.2f}  {s['n']:>7,}  "
              f"{s['hit_rate']:>5.1f}%  {sig}")
    print(f"\n  * p<0.05  ** p<0.01")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Post-Stock Split Performance Event Study")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--exchange", type=str,
                        help="Exchange filter, comma-separated (e.g. NYSE,NASDAQ,AMEX). "
                             "Default: all exchanges with splits data")
    parser.add_argument("--output", type=str, default="stock-split/results",
                        help="Output directory (default: stock-split/results)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--min-mktcap", type=float, default=None,
                        help="Minimum market cap in local currency (default: auto per exchange)")
    parser.add_argument("--min-ratio", type=float, default=DEFAULT_MIN_RATIO,
                        help=f"Minimum split ratio (default: {DEFAULT_MIN_RATIO})")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    args = parser.parse_args()
    args.exchanges = [e.strip().upper() for e in args.exchange.split(",")] if args.exchange else None
    if args.min_mktcap is None:
        args.min_mktcap = get_mktcap_threshold(args.exchanges, use_low_threshold=True)

    client = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    con = duckdb.connect(":memory:")

    print("=" * 65)
    print(f"  {STRATEGY_NAME}")
    period = f"{args.start_year}-{args.end_year}"
    exch = " | ".join(args.exchanges) if args.exchanges else "All exchanges"
    print(f"  Period: {period} | Exchange: {exch}")
    mktcap_label = f"{args.min_mktcap/1e9:.0f}B" if args.min_mktcap >= 1e9 else f"{args.min_mktcap/1e6:.0f}M"
    print(f"  Min mktcap: {mktcap_label} local | Min split ratio: {args.min_ratio}x")
    print("=" * 65)

    all_windows = PRE_WINDOWS + WINDOWS

    print("\nPhase 1: Loading data...")
    fetch_splits_and_prices(client, con, args, args.verbose)

    print("\nPhase 2: Running event study...")
    run_event_study(con, all_windows, args.verbose)

    print("\nPhase 3: Aggregating results...")
    overall, by_cat = aggregate(con, all_windows)

    print_summary(overall, all_windows)

    print("\nPhase 4: Saving results...")
    save_results(con, overall, by_cat, all_windows, args.output, args)
    print("\nDone.")


if __name__ == "__main__":
    main()
