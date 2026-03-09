#!/usr/bin/env python3
"""
Earnings Beat Streaks Event Study

Event study measuring abnormal returns after consecutive earnings beats.
Fetches data via API, caches in DuckDB, runs locally.

Signal: Consecutive quarters of epsActual > epsEstimated (streak_length >= 2)
Categories: streak_2 (2nd beat), streak_3 (3rd), streak_4 (4th), streak_5plus (5th+)
Event windows: T+1, T+5, T+21, T+63 trading days
Benchmark: SPY (US) or regional ETF

Academic reference: Loh & Warachka (2012) "Streaks in Earnings Surprises
and the Cross-Section of Stock Returns", Management Science, 58(7), 1305-1321.
Myers, Myers & Skinner (2007) "Earnings Momentum and Earnings Management",
Journal of Accounting, Auditing & Finance, 22(2), 249-284.

Usage:
    # US event study (default)
    python3 beat-streaks/backtest.py

    # With specific exchange
    python3 beat-streaks/backtest.py --preset india

    # All exchanges
    python3 beat-streaks/backtest.py --global --output results/exchange_comparison.json --verbose

See README.md for strategy details.
"""

import argparse
import duckdb
import json
import math
import os
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, REGIONAL_BENCHMARKS
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Parameters ---
MIN_ESTIMATE = 0.01           # |epsEstimated| > $0.01 (avoid extreme ratios)
MIN_STREAK = 2                # Minimum streak length to include as event
MAX_RETURN = 2.0              # Cap individual event returns at 200%
WINSORIZE_PCT = 1.0           # Winsorize at 1st/99th percentile
WINDOWS = [1, 5, 21, 63]      # Trading day windows post-event
START_YEAR = 2000
END_YEAR = 2025


def fetch_data(client, exchanges, mktcap_min, verbose=False):
    """Fetch earnings surprises with streak computation, prices, and market cap."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    # 1. Fetch earnings data and compute streak lengths
    print("  Fetching earnings surprises and computing streaks...")
    surprise_sql = f"""
        SELECT symbol, date, epsActual, epsEstimated
        FROM earnings_surprises
        WHERE epsEstimated IS NOT NULL
          AND ABS(epsEstimated) > {MIN_ESTIMATE}
          AND epsActual IS NOT NULL
          AND CAST(date AS DATE) >= '{START_YEAR}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR}-12-31'
          AND {sym_filter}
    """
    count = query_parquet(client, surprise_sql, con, "raw_surprises",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} earnings records")
    if count == 0:
        print("  No earnings data. Skipping.")
        con.close()
        return None

    # Compute streak lengths via DuckDB window functions
    con.execute("""
        CREATE TABLE surprises AS
        SELECT symbol,
            CAST(date AS DATE) AS event_date,
            epsActual,
            epsEstimated,
            CASE WHEN epsActual > epsEstimated THEN 1 ELSE 0 END AS is_beat
        FROM raw_surprises
    """)
    # Deduplicate: keep one record per symbol/date (some FMP data has duplicates)
    con.execute("""
        CREATE TABLE surprises_dedup AS
        SELECT symbol, event_date, epsActual, epsEstimated, is_beat
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY symbol, event_date ORDER BY epsActual DESC) AS rn
            FROM surprises
        ) WHERE rn = 1
    """)

    # Compute streak_length: for each beat, how many consecutive beats preceded it
    con.execute("""
        CREATE TABLE streak_events AS
        WITH miss_groups AS (
            SELECT symbol, event_date, epsActual, epsEstimated, is_beat,
                SUM(CASE WHEN is_beat = 0 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY symbol ORDER BY event_date
                          ROWS UNBOUNDED PRECEDING) AS miss_count
            FROM surprises_dedup
        ),
        with_streak AS (
            SELECT symbol, event_date, epsActual, epsEstimated, is_beat, miss_count,
                ROW_NUMBER() OVER (PARTITION BY symbol, miss_count ORDER BY event_date) AS streak_length
            FROM miss_groups
            WHERE is_beat = 1
        )
        SELECT symbol, event_date, streak_length,
            CASE
                WHEN streak_length = 2 THEN 'streak_2'
                WHEN streak_length = 3 THEN 'streak_3'
                WHEN streak_length = 4 THEN 'streak_4'
                ELSE 'streak_5plus'
            END AS category
        FROM with_streak
        WHERE streak_length >= 2
    """)

    n_streak = con.execute("SELECT COUNT(*) FROM streak_events").fetchone()[0]
    cat_counts = con.execute("""
        SELECT category, COUNT(*) as n
        FROM streak_events
        GROUP BY category ORDER BY category
    """).fetchall()
    print(f"    -> {n_streak} streak events (streak >= 2)")
    for cat, n in cat_counts:
        print(f"       {cat}: {n:,}")

    if n_streak < 50:
        print("  Too few streak events for meaningful analysis. Skipping.")
        con.close()
        return None

    # 2. Fetch market cap for filtering (FY key_metrics)
    print("  Fetching market cap data...")
    mcap_sql = f"""
        SELECT symbol, dateEpoch AS filing_epoch, marketCap
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND {sym_filter}
    """
    mcap_count = query_parquet(client, mcap_sql, con, "mcap_cache",
                               verbose=verbose, limit=5000000, timeout=600,
                               memory_mb=4096, threads=2)
    print(f"    -> {mcap_count} market cap rows")

    # 3. Filter by market cap (most recent FY before event)
    print("  Filtering by market cap...")
    con.execute(f"""
        CREATE TABLE events AS
        WITH matched AS (
            SELECT s.symbol, s.event_date, s.streak_length, s.category,
                m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY s.symbol, s.event_date
                                   ORDER BY m.filing_epoch DESC) AS rn
            FROM streak_events s
            LEFT JOIN mcap_cache m ON s.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(s.event_date)
        )
        SELECT symbol, event_date, streak_length, category
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)
    # Deduplicate (one event per symbol per date - take highest streak)
    con.execute("""
        CREATE TABLE unique_events AS
        SELECT symbol, event_date, streak_length, category,
            ROW_NUMBER() OVER (PARTITION BY symbol, event_date ORDER BY streak_length DESC) AS rn
        FROM events
    """)
    con.execute("DELETE FROM unique_events WHERE rn > 1")
    con.execute("ALTER TABLE unique_events DROP COLUMN rn")

    n_events = con.execute("SELECT COUNT(*) FROM unique_events").fetchone()[0]
    cat_final = con.execute("""
        SELECT category, COUNT(*) as n
        FROM unique_events GROUP BY category ORDER BY category
    """).fetchall()
    print(f"    -> {n_events} unique events after market cap filter")
    for cat, n in cat_final:
        print(f"       {cat}: {n:,}")

    if n_events < 50:
        print("  Too few events after market cap filter. Skipping.")
        con.close()
        return None

    # 4. Get all unique event symbols
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unique_events").fetchall()]

    # 5. Determine benchmark
    benchmark = "SPY"
    if exchanges:
        for ex in exchanges:
            if ex in REGIONAL_BENCHMARKS:
                benchmark = REGIONAL_BENCHMARKS[ex]
                break

    # 6. Fetch price data
    print(f"  Fetching prices for {len(event_symbols)} symbols + {benchmark}...")
    sym_list = event_symbols + [benchmark]
    sym_in = ", ".join(f"'{s}'" for s in sym_list)
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{START_YEAR-1}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR+1}-12-31'
          AND adjClose > 0
    """
    count = query_parquet(client, price_sql, con, "prices",
                          verbose=verbose, limit=10000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")
    print(f"    -> {count} price rows")

    # 7. Build trading day calendar from benchmark
    con.execute(f"""
        CREATE TABLE trading_days AS
        SELECT trade_date,
            ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM prices
        WHERE symbol = '{benchmark}'
        ORDER BY trade_date
    """)
    n_days = con.execute("SELECT COUNT(*) FROM trading_days").fetchone()[0]
    print(f"    -> {n_days} trading days from {benchmark}")

    con.execute(f"CREATE TABLE config AS SELECT '{benchmark}' AS benchmark")
    return con


def compute_event_returns(con, windows=WINDOWS, verbose=False):
    """Compute abnormal returns at each window for all streak events."""
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Map each event to its T+0 trading day
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT e.symbol, e.event_date, e.streak_length, e.category,
            td.day_num AS t0_num, td.trade_date AS t0_date
        FROM unique_events e
        ASOF JOIN trading_days td ON td.trade_date >= e.event_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # T+0 prices for events
    print("    Getting T+0 prices...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.event_date, et.streak_length, et.category,
            et.t0_num, et.t0_date,
            sp.adjClose AS stock_t0, bp.adjClose AS bench_t0
        FROM event_t0 et
        JOIN prices sp ON et.symbol = sp.symbol AND et.t0_date = sp.trade_date
        JOIN prices bp ON bp.symbol = '{benchmark}' AND et.t0_date = bp.trade_date
        WHERE sp.adjClose > 0 AND bp.adjClose > 0
    """)
    n_priced = con.execute("SELECT COUNT(*) FROM event_base").fetchone()[0]
    print(f"    -> {n_priced} events with T+0 prices")
    con.execute("DROP TABLE event_t0")

    # Compute returns at each window
    for w in windows:
        print(f"    Computing T+{w} returns...")
        con.execute(f"""
            CREATE OR REPLACE TABLE window_{w}_returns AS
            SELECT eb.symbol, eb.event_date,
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0, 8) AS stock_ret,
                ROUND((bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS bench_ret,
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0
                     - (bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS abnormal_ret
            FROM event_base eb
            JOIN trading_days td ON td.day_num = eb.t0_num + {w}
            JOIN prices sp ON eb.symbol = sp.symbol AND td.trade_date = sp.trade_date
            JOIN prices bp ON bp.symbol = '{benchmark}' AND td.trade_date = bp.trade_date
        """)
        n_computed = con.execute(f"SELECT COUNT(*) FROM window_{w}_returns").fetchone()[0]
        print(f"      -> {n_computed} events with T+{w} returns")

    # Join all windows
    print("    Joining window results...")
    select_cols = ["eb.symbol", "eb.event_date", "eb.streak_length", "eb.category"]
    join_clauses = []
    for w in windows:
        select_cols.extend([
            f"w{w}.stock_ret AS stock_ret_{w}d",
            f"w{w}.bench_ret AS bench_ret_{w}d",
            f"w{w}.abnormal_ret AS abnormal_ret_{w}d",
        ])
        join_clauses.append(
            f"LEFT JOIN window_{w}_returns w{w} ON eb.symbol = w{w}.symbol AND eb.event_date = w{w}.event_date"
        )

    result_sql = f"""
        SELECT {', '.join(select_cols)}
        FROM event_base eb
        {' '.join(join_clauses)}
        WHERE w1.abnormal_ret IS NOT NULL
        ORDER BY eb.event_date
    """
    rows = con.execute(result_sql).fetchall()

    col_names = ["symbol", "event_date", "streak_length", "category"]
    for w in windows:
        col_names.extend([f"stock_ret_{w}d", f"bench_ret_{w}d", f"abnormal_ret_{w}d"])

    results = []
    for row in rows:
        r = {}
        for i, col in enumerate(col_names):
            val = row[i]
            if col == "event_date":
                r[col] = val.isoformat() if isinstance(val, date) else str(val)
            elif isinstance(val, float):
                r[col] = round(val, 6)
            else:
                r[col] = val
        results.append(r)

    for w in windows:
        con.execute(f"DROP TABLE IF EXISTS window_{w}_returns")

    print(f"    -> {len(results)} events with returns")
    return results


def winsorize(values, pct=WINSORIZE_PCT):
    """Winsorize values at pct/100-pct percentiles."""
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    n = len(sorted_v)
    lo_idx = max(0, int(n * pct / 100))
    hi_idx = min(n - 1, int(n * (100 - pct) / 100))
    lo_val = sorted_v[lo_idx]
    hi_val = sorted_v[hi_idx]
    return [max(lo_val, min(hi_val, v)) for v in values]


def compute_car_stats(values_raw, windows_key):
    """Compute CAR stats for a set of values."""
    if not values_raw:
        return None
    values = winsorize(values_raw)
    n = len(values)
    mean_car = sum(values) / n
    if n > 1:
        var = sum((v - mean_car) ** 2 for v in values) / (n - 1)
        std = math.sqrt(var) if var > 0 else 0
        se = std / math.sqrt(n)
        t_stat = mean_car / se if se > 0 else 0
    else:
        t_stat = 0

    hit_rate = sum(1 for v in values_raw if v > 0) / len(values_raw)
    sorted_vals = sorted(values_raw)
    mid = len(sorted_vals) // 2
    median = (sorted_vals[mid] if len(sorted_vals) % 2 == 1
              else (sorted_vals[mid-1] + sorted_vals[mid]) / 2)

    return {
        "mean_car": round(mean_car * 100, 4),
        "median_car": round(median * 100, 4),
        "t_stat": round(t_stat, 3),
        "n": len(values_raw),
        "hit_rate": round(hit_rate * 100, 2),
        "significant_5pct": abs(t_stat) > 1.96,
        "significant_1pct": abs(t_stat) > 2.576,
    }


def compute_metrics(results, windows=WINDOWS):
    """Compute CAR metrics overall and by streak category."""
    categories = ["overall", "streak_2", "streak_3", "streak_4", "streak_5plus"]
    metrics = {}

    for cat in categories:
        if cat == "overall":
            subset = results
        else:
            subset = [r for r in results if r.get("category") == cat]

        n = len(subset)
        if n == 0:
            continue

        metrics[cat] = {"n": n}

        for w in windows:
            key = f"abnormal_ret_{w}d"
            raw_values = [r[key] for r in subset if r.get(key) is not None]
            if raw_values:
                stats = compute_car_stats(raw_values, key)
                if stats:
                    metrics[cat][f"T+{w}"] = stats

    return metrics


def compute_yearly_stats(results):
    """Compute event counts by year and streak category."""
    by_year = {}
    for r in results:
        year = r["event_date"][:4]
        cat = r.get("category", "unknown")
        if year not in by_year:
            by_year[year] = {"total": 0, "streak_2": 0, "streak_3": 0,
                             "streak_4": 0, "streak_5plus": 0}
        by_year[year]["total"] += 1
        by_year[year][cat] = by_year[year].get(cat, 0) + 1

    yearly = []
    for year in sorted(by_year.keys()):
        d = by_year[year]
        yearly.append({
            "year": int(year),
            "total": d["total"],
            "streak_2": d.get("streak_2", 0),
            "streak_3": d.get("streak_3", 0),
            "streak_4": d.get("streak_4", 0),
            "streak_5plus": d.get("streak_5plus", 0),
        })
    return yearly


def print_results(metrics, universe_name):
    """Print formatted results."""
    print(f"\n{'=' * 70}")
    print(f"  BEAT STREAKS EVENT STUDY RESULTS: {universe_name}")
    print(f"{'=' * 70}")

    categories = ["overall", "streak_2", "streak_3", "streak_4", "streak_5plus"]
    labels = {
        "overall": "All Streak Events (streak >= 2)",
        "streak_2": "Streak 2 (2nd consecutive beat)",
        "streak_3": "Streak 3 (3rd consecutive beat)",
        "streak_4": "Streak 4 (4th consecutive beat)",
        "streak_5plus": "Streak 5+ (5th or longer beat)",
    }

    for cat in categories:
        section = metrics.get(cat, {})
        n = section.get("n", 0)
        if n == 0:
            continue

        print(f"\n  {labels[cat]} (n={n:,})")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>8}")
        print(f"  {'-' * 55}")

        for w in WINDOWS:
            d = section.get(f"T+{w}")
            if d is None:
                continue
            sig = " **" if d.get("significant_1pct") else (" *" if d.get("significant_5pct") else "")
            print(f"  T+{w:<7} {d['mean_car']:>+9.3f}% {d['median_car']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

    print(f"{'=' * 70}")


def run_single(cr, exchanges, universe_name, mktcap_min, verbose=False, output_path=None):
    """Run beat streaks event study for a single exchange set."""
    mktcap_label = (f"{mktcap_min/1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min/1e6:.0f}M")
    signal_desc = (f"Consecutive EPS beats (streak >= {MIN_STREAK}), "
                   f"MCap > {mktcap_label} local, |est| > ${MIN_ESTIMATE}")
    print_header("BEAT STREAKS EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print(f"  Categories: streak_2 / streak_3 / streak_4 / streak_5plus")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    con = fetch_data(cr, exchanges, mktcap_min, verbose=verbose)
    if con is None:
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    print(f"\nPhase 2: Computing event-window returns...")
    t1 = time.time()
    results = compute_event_returns(con, windows=WINDOWS, verbose=verbose)
    compute_time = time.time() - t1
    print(f"Returns computed in {compute_time:.0f}s")

    if not results:
        print("No valid event returns. Skipping.")
        con.close()
        return None

    print("\nPhase 3: Computing CAR metrics...")
    metrics = compute_metrics(results, windows=WINDOWS)
    yearly = compute_yearly_stats(results)

    print_results(metrics, universe_name)

    if yearly:
        print(f"\n  Yearly Event Counts (total streak events >= 2):")
        print(f"  {'Year':>6} {'Total':>8} {'Str2':>6} {'Str3':>6} {'Str4':>6} {'Str5+':>6}")
        print(f"  {'-' * 42}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total']:>8} {y['streak_2']:>6} "
                  f"{y['streak_3']:>6} {y['streak_4']:>6} {y['streak_5plus']:>6}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = {
        "strategy": "Earnings Beat Streaks",
        "universe": universe_name,
        "benchmark": benchmark,
        "study_type": "event_study",
        "period": f"{START_YEAR}-{END_YEAR}",
        "filters": {
            "min_market_cap": mktcap_min,
            "min_estimate": MIN_ESTIMATE,
            "min_streak": MIN_STREAK,
        },
        "windows": WINDOWS,
        "car_metrics": metrics,
        "yearly_stats": yearly,
        "n_total_events": len(results),
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

        # Also save event-level CSV
        csv_path = output_path.replace(".json", "_events.csv")
        if results:
            headers = list(results[0].keys())
            with open(csv_path, "w") as f:
                f.write(",".join(headers) + "\n")
                for row in results:
                    f.write(",".join(str(row.get(h, "")) for h in headers) + "\n")
            print(f"  Events saved to {csv_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="Earnings Beat Streaks event study")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("beat-streaks", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)

    # --global mode: run all exchange presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        presets_to_run = [
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("canada", ["TSX"]),
            ("uk", ["LSE"]),
            ("japan", ["JPX"]),
            ("india", ["BSE", "NSE"]),
            ("germany", ["XETRA"]),
            ("china", ["SHZ", "SHH"]),
            ("korea", ["KSC"]),
            ("sweden", ["STO"]),
            ("hongkong", ["HKSE"]),
            ("australia", ["ASX"]),
            ("brazil", ["SAO"]),
            ("switzerland", ["SIX"]),
            ("taiwan", ["TAI", "TWO"]),
            ("thailand", ["SET"]),
            ("norway", ["OSL"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"beat_streaks_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            mktcap_threshold = get_mktcap_threshold(preset_exchanges)

            try:
                result = run_single(cr, preset_exchanges, uni_name, mktcap_threshold,
                                    verbose=args.verbose, output_path=output_path)
                if result:
                    all_results[uni_name] = result
            except Exception as e:
                print(f"\n  ERROR on {uni_name}: {e}")
                import traceback
                traceback.print_exc()
                all_results[uni_name] = {"error": str(e)}

        # Save comparison
        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Print summary table
        print(f"\n\n{'=' * 90}")
        print("BEAT STREAKS EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 90}")
        print(f"{'Exchange':<20} {'Events':>8} {'S2 CAR+1':>10} {'S3 CAR+1':>10} "
              f"{'ALL T+1':>9} {'ALL T+21':>9} {'t(21)':>7}")
        print("-" * 90)

        for uni, r in sorted(all_results.items(),
                              key=lambda x: abs(x[1].get("car_metrics", {}).get("overall", {}).get("T+1", {}).get("mean_car", 0)
                                                if isinstance(x[1], dict) else 0),
                              reverse=True):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<20} {'ERROR / NO DATA'}")
                continue

            overall = r["car_metrics"].get("overall", {})
            s2 = r["car_metrics"].get("streak_2", {})
            s3 = r["car_metrics"].get("streak_3", {})
            n = overall.get("n", 0)

            c_all_1 = overall.get("T+1", {}).get("mean_car", 0)
            c_all_21 = overall.get("T+21", {}).get("mean_car", 0)
            t_21 = overall.get("T+21", {}).get("t_stat", 0)
            c_s2_1 = s2.get("T+1", {}).get("mean_car", 0) if s2 else 0
            c_s3_1 = s3.get("T+1", {}).get("mean_car", 0) if s3 else 0

            print(f"{uni:<20} {n:>8} {c_s2_1:>+9.3f}% {c_s3_1:>+9.3f}% "
                  f"{c_all_1:>+8.3f}% {c_all_21:>+8.3f}% {t_21:>+6.2f}")

        print("=" * 90)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold,
               verbose=args.verbose, output_path=args.output)


if __name__ == "__main__":
    main()
