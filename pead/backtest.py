#!/usr/bin/env python3
"""
Post-Earnings Announcement Drift (PEAD) Event Study

Event study measuring abnormal returns after earnings surprises.
Fetches data via API, caches in DuckDB, runs locally.

Signal: Earnings surprise = (epsActual - epsEstimated) / |epsEstimated|
Categories: positive (beat), negative (miss)
Event windows: T+1, T+5, T+21, T+63 trading days
Benchmark: SPY (US) or regional ETF
Universe: Market cap > $500M, |epsEstimated| > $0.01

Academic reference: Ball & Brown (1968) "An Empirical Evaluation of
Accounting Income Numbers", Journal of Accounting Research 6(2), 159-178.
Bernard & Thomas (1989) "Post-Earnings-Announcement Drift: Delayed Price
Response or Risk Premium?", Journal of Accounting Research 27, 1-36.

Usage:
    # US event study (default)
    python3 pead/backtest.py

    # With specific exchange
    python3 pead/backtest.py --preset india

    # All exchanges
    python3 pead/backtest.py --global --output results/exchange_comparison.json --verbose

    # Cap surprise magnitude to reduce outlier noise
    python3 pead/backtest.py --max-surprise 5.0

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
from data_utils import query_parquet, LOCAL_INDEX_BENCHMARKS, get_local_benchmark
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Parameters ---
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold()
MIN_ESTIMATE = 0.01           # |epsEstimated| > $0.01 (avoid extreme ratios)
MAX_SURPRISE = 2.0            # Cap surprise at 200% to reduce outlier noise
MAX_RETURN = 2.0              # Cap individual event returns at 200%
WINSORIZE_PCT = 1.0           # Winsorize at 1st/99th percentile
WINDOWS = [1, 5, 21, 63]      # Trading day windows post-event
START_YEAR = 2000
END_YEAR = 2025


def fetch_data(client, exchanges, mktcap_min, benchmark_symbol="SPY", verbose=False):
    """Fetch earnings surprises, prices, and market cap data into DuckDB."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    # 1. Fetch earnings surprises
    print("  Fetching earnings surprises...")
    surprise_sql = f"""
        SELECT symbol, date, epsActual, epsEstimated, dateEpoch
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
    print(f"    -> {count} surprise events")
    if count == 0:
        print("  No earnings surprise data. Skipping.")
        con.close()
        return None

    # Compute surprise percentage and filter
    con.execute(f"""
        CREATE TABLE surprises AS
        SELECT symbol,
            CAST(date AS DATE) AS event_date,
            epsActual,
            epsEstimated,
            (epsActual - epsEstimated) / ABS(epsEstimated) AS surprise_pct,
            CASE WHEN epsActual > epsEstimated THEN 'positive' ELSE 'negative' END AS category
        FROM raw_surprises
        WHERE ABS((epsActual - epsEstimated) / ABS(epsEstimated)) <= {MAX_SURPRISE}
    """)
    n_filtered = con.execute("SELECT COUNT(*) FROM surprises").fetchone()[0]
    print(f"    -> {n_filtered} events after surprise cap ({MAX_SURPRISE*100:.0f}%)")

    # 2. Fetch market cap for filtering (FY key_metrics)
    print("  Fetching market cap data...")
    mcap_sql = f"""
        SELECT symbol, dateEpoch AS filing_epoch, marketCap
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND {sym_filter}
    """
    count = query_parquet(client, mcap_sql, con, "mcap_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} market cap rows")

    # 3. Filter by market cap (most recent FY before event)
    print("  Filtering by market cap...")
    con.execute(f"""
        CREATE TABLE events AS
        WITH matched AS (
            SELECT s.symbol, s.event_date, s.surprise_pct, s.category,
                m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY s.symbol, s.event_date
                                   ORDER BY m.filing_epoch DESC) AS rn
            FROM surprises s
            LEFT JOIN mcap_cache m ON s.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(s.event_date)
        )
        SELECT symbol, event_date, surprise_pct, category
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)
    # Deduplicate (one event per symbol per date)
    con.execute("""
        CREATE TABLE unique_events AS
        SELECT symbol, event_date, surprise_pct, category,
            ROW_NUMBER() OVER (PARTITION BY symbol, event_date ORDER BY surprise_pct DESC) AS rn
        FROM events
    """)
    con.execute("DELETE FROM unique_events WHERE rn > 1")
    con.execute("ALTER TABLE unique_events DROP COLUMN rn")

    n_events = con.execute("SELECT COUNT(*) FROM unique_events").fetchone()[0]
    pos = con.execute("SELECT COUNT(*) FROM unique_events WHERE category = 'positive'").fetchone()[0]
    neg = n_events - pos
    print(f"    -> {n_events} unique events ({pos} positive, {neg} negative)")

    if n_events < 50:
        print("  Too few events for meaningful analysis. Skipping.")
        con.close()
        return None

    # 4. Get all unique event symbols
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unique_events").fetchall()]

    # 5. Fetch price data for event symbols + benchmark
    benchmark = benchmark_symbol
    print(f"  Fetching prices for {len(event_symbols)} symbols + {benchmark}...")

    # Fetch prices only for event symbols + benchmark (not entire exchange)
    sym_list = event_symbols + [benchmark]
    sym_in = ", ".join(f"'{s}'" for s in sym_list)
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose, volume
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

    # 6. Build trading day calendar from benchmark
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

    # Store benchmark ticker for later use
    con.execute(f"CREATE TABLE config AS SELECT '{benchmark}' AS benchmark")

    return con


def compute_event_returns(con, windows=WINDOWS, offset_days=1, verbose=False):
    """Compute abnormal returns at each window for all events.

    Uses vectorized DuckDB SQL for performance (handles 200K+ events).

    Args:
        offset_days: int - 1 = MOC execution (entry at T+1 close), 0 = same-day (legacy)
    Returns list of event dicts with returns at each window.
    """
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Step 1: Map each event to its T+0 trading day using ASOF JOIN
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT e.symbol, e.event_date, e.surprise_pct, e.category,
            td.day_num AS t0_num, td.trade_date AS t0_date
        FROM unique_events e
        ASOF JOIN trading_days td ON td.trade_date >= e.event_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # Step 2: Get entry prices (T+offset for MOC execution)
    entry_label = f"T+{offset_days}" if offset_days > 0 else "T+0"
    print(f"    Getting {entry_label} prices (entry)...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.event_date, et.surprise_pct, et.category,
            et.t0_num, et.t0_date,
            td_entry.day_num AS entry_num, td_entry.trade_date AS entry_date,
            sp.adjClose AS stock_t0, bp.adjClose AS bench_t0
        FROM event_t0 et
        JOIN trading_days td_entry ON td_entry.day_num = et.t0_num + {offset_days}
        JOIN prices sp ON et.symbol = sp.symbol AND td_entry.trade_date = sp.trade_date
        JOIN prices bp ON bp.symbol = '{benchmark}' AND td_entry.trade_date = bp.trade_date
        WHERE sp.adjClose > 0 AND bp.adjClose > 0
    """)
    n_priced = con.execute("SELECT COUNT(*) FROM event_base").fetchone()[0]
    print(f"    -> {n_priced} events with T+0 prices")

    # Drop intermediate table to free memory
    con.execute("DROP TABLE event_t0")

    # Step 3: Compute returns at each window using a single query per window
    for w in windows:
        print(f"    Computing T+{w} returns...")

        # Join event_base -> trading_days (target day_num) -> prices
        # Windows measured from entry (T+offset), so target = entry_num + w
        con.execute(f"""
            CREATE OR REPLACE TABLE window_{w}_returns AS
            SELECT eb.symbol, eb.event_date,
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0, 8) AS stock_ret,
                ROUND((bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS bench_ret,
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0
                     - (bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS abnormal_ret
            FROM event_base eb
            JOIN trading_days td ON td.day_num = eb.entry_num + {w}
            JOIN prices sp ON eb.symbol = sp.symbol AND td.trade_date = sp.trade_date
            JOIN prices bp ON bp.symbol = '{benchmark}' AND td.trade_date = bp.trade_date
        """)

        n_computed = con.execute(f"SELECT COUNT(*) FROM window_{w}_returns").fetchone()[0]
        print(f"      -> {n_computed} events with T+{w} returns")

    # Step 4: Join all windows and extract results
    print("    Joining window results...")

    # Build join query - left join each window table to event_base
    select_cols = ["eb.symbol", "eb.event_date", "eb.surprise_pct", "eb.category"]
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

    # Must have at least T+1 return
    result_sql = f"""
        SELECT {', '.join(select_cols)}
        FROM event_base eb
        {' '.join(join_clauses)}
        WHERE w1.abnormal_ret IS NOT NULL
        ORDER BY eb.event_date
    """
    rows = con.execute(result_sql).fetchall()

    col_names = ["symbol", "event_date", "surprise_pct", "category"]
    for w in windows:
        col_names.extend([f"stock_ret_{w}d", f"bench_ret_{w}d", f"abnormal_ret_{w}d"])

    results = []
    for row in rows:
        r = {}
        for i, col in enumerate(col_names):
            val = row[i]
            if col == "event_date":
                r[col] = val.isoformat() if isinstance(val, date) else str(val)
            elif col == "surprise_pct":
                r[col] = round(float(val) * 100, 2) if val is not None else 0
            elif isinstance(val, float):
                r[col] = round(val, 6)
            else:
                r[col] = val
        results.append(r)

    skipped = n_mapped - len(results)
    print(f"    -> {len(results)} events with returns, {skipped} skipped (no price data)")

    # Clean up window tables
    for w in windows:
        con.execute(f"DROP TABLE IF EXISTS window_{w}_returns")

    return results


def winsorize(values, pct=WINSORIZE_PCT):
    """Winsorize values at pct/100-pct percentiles.

    Clips extreme values to reduce outlier influence on mean/std.
    """
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    n = len(sorted_v)
    lo_idx = max(0, int(n * pct / 100))
    hi_idx = min(n - 1, int(n * (100 - pct) / 100))
    lo_val = sorted_v[lo_idx]
    hi_val = sorted_v[hi_idx]
    return [max(lo_val, min(hi_val, v)) for v in values]


def compute_car_metrics(results, windows=WINDOWS):
    """Compute Cumulative Abnormal Return (CAR) metrics.

    Uses winsorized means and raw medians. Returns dict with
    overall and by-category CAR stats.
    """
    metrics = {"overall": {}, "positive": {}, "negative": {}}

    for label, filter_fn in [
        ("overall", lambda r: True),
        ("positive", lambda r: r["category"] == "positive"),
        ("negative", lambda r: r["category"] == "negative"),
    ]:
        subset = [r for r in results if filter_fn(r)]
        n = len(subset)
        if n == 0:
            continue

        metrics[label]["n_events"] = n

        for w in windows:
            key = f"abnormal_ret_{w}d"
            raw_values = [r[key] for r in subset if r.get(key) is not None]
            if not raw_values:
                continue

            # Winsorize for mean/std/t-stat computation
            values = winsorize(raw_values)

            mean_car = sum(values) / len(values)
            # Compute t-statistic on winsorized data
            if len(values) > 1:
                var = sum((v - mean_car) ** 2 for v in values) / (len(values) - 1)
                std = math.sqrt(var) if var > 0 else 0
                se = std / math.sqrt(len(values))
                t_stat = mean_car / se if se > 0 else 0
            else:
                std = 0
                t_stat = 0

            # Hit rate (% of events with positive abnormal return) - use raw
            hit_rate = sum(1 for v in raw_values if v > 0) / len(raw_values)

            # Median - use raw (robust to outliers)
            sorted_vals = sorted(raw_values)
            mid = len(sorted_vals) // 2
            median = sorted_vals[mid] if len(sorted_vals) % 2 == 1 else (sorted_vals[mid-1] + sorted_vals[mid]) / 2

            metrics[label][f"car_{w}d"] = {
                "mean": round(mean_car * 100, 4),
                "median": round(median * 100, 4),
                "std": round(std * 100, 4),
                "t_stat": round(t_stat, 3),
                "significant": abs(t_stat) > 1.96,
                "hit_rate": round(hit_rate * 100, 2),
                "n_obs": len(raw_values),
            }

    return metrics


def compute_quintile_metrics(results, windows=WINDOWS):
    """Stratify results by surprise quintile and compute CAR."""
    # Sort by surprise magnitude
    valid = [r for r in results if r.get("surprise_pct") is not None]
    valid.sort(key=lambda r: r["surprise_pct"])

    n = len(valid)
    if n < 50:
        return {}

    q_size = n // 5
    quintiles = {}

    for q in range(5):
        start = q * q_size
        end = start + q_size if q < 4 else n
        subset = valid[start:end]

        q_label = f"Q{q+1}"
        surprise_vals = [r["surprise_pct"] for r in subset]
        quintiles[q_label] = {
            "n_events": len(subset),
            "surprise_range": f"{min(surprise_vals):.1f}% to {max(surprise_vals):.1f}%",
            "mean_surprise": round(sum(surprise_vals) / len(surprise_vals), 2),
        }

        for w in windows:
            key = f"abnormal_ret_{w}d"
            raw_values = [r[key] for r in subset if r.get(key) is not None]
            if raw_values:
                values = winsorize(raw_values)
                mean_car = sum(values) / len(values)
                quintiles[q_label][f"car_{w}d"] = round(mean_car * 100, 4)

    return quintiles


def compute_yearly_stats(results):
    """Compute event counts and beat rates by year."""
    by_year = {}
    for r in results:
        year = r["event_date"][:4]
        if year not in by_year:
            by_year[year] = {"total": 0, "positive": 0, "negative": 0}
        by_year[year]["total"] += 1
        by_year[year][r["category"]] += 1

    yearly = []
    for year in sorted(by_year.keys()):
        d = by_year[year]
        yearly.append({
            "year": int(year),
            "total_events": d["total"],
            "beats": d["positive"],
            "misses": d["negative"],
            "beat_rate": round(d["positive"] / d["total"] * 100, 1) if d["total"] > 0 else 0,
        })
    return yearly


def build_output(car_metrics, quintiles, yearly, results, universe_name, benchmark, mktcap_min):
    """Build JSON output."""
    return {
        "strategy": "PEAD (Post-Earnings Announcement Drift)",
        "universe": universe_name,
        "benchmark": benchmark,
        "study_type": "event_study",
        "period": f"{START_YEAR}-{END_YEAR}",
        "filters": {
            "min_market_cap": mktcap_min,
            "min_estimate": MIN_ESTIMATE,
            "max_surprise": MAX_SURPRISE,
        },
        "windows": WINDOWS,
        "car_metrics": car_metrics,
        "quintile_analysis": quintiles,
        "yearly_stats": yearly,
        "n_total_events": len(results),
        "n_positive": sum(1 for r in results if r["category"] == "positive"),
        "n_negative": sum(1 for r in results if r["category"] == "negative"),
    }


def print_results(car_metrics, quintiles, universe_name):
    """Print formatted results."""
    print(f"\n{'=' * 70}")
    print(f"  PEAD EVENT STUDY RESULTS: {universe_name}")
    print(f"{'=' * 70}")

    for label in ["overall", "positive", "negative"]:
        section = car_metrics.get(label, {})
        n = section.get("n_events", 0)
        if n == 0:
            continue

        title = {"overall": "All Events", "positive": "Positive Surprises (Beats)",
                 "negative": "Negative Surprises (Misses)"}[label]
        print(f"\n  {title} (n={n:,})")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>8}")
        print(f"  {'-' * 53}")

        for w in WINDOWS:
            d = section.get(f"car_{w}d")
            if d is None:
                continue
            sig = " **" if d["significant"] else ""
            print(f"  T+{w:<7} {d['mean']:>+9.3f}% {d['median']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

    if quintiles:
        print(f"\n  Quintile Analysis (Q1=worst misses, Q5=biggest beats)")
        print(f"  {'Q':<5} {'Events':>8} {'Surprise':>20} {'CAR T+1':>10} {'CAR T+21':>10} {'CAR T+63':>10}")
        print(f"  {'-' * 65}")
        for q in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
            d = quintiles.get(q, {})
            if not d:
                continue
            c1 = d.get("car_1d", 0)
            c21 = d.get("car_21d", 0)
            c63 = d.get("car_63d", 0)
            print(f"  {q:<5} {d['n_events']:>8} {d['surprise_range']:>20} "
                  f"{c1:>+9.3f}% {c21:>+9.3f}% {c63:>+9.3f}%")

        # Spread Q5-Q1
        if "Q5" in quintiles and "Q1" in quintiles:
            for w in [1, 21, 63]:
                q5 = quintiles["Q5"].get(f"car_{w}d", 0)
                q1 = quintiles["Q1"].get(f"car_{w}d", 0)
                if w == 63:
                    print(f"\n  Q5-Q1 spread at T+63: {q5 - q1:+.3f}%")

    print(f"{'=' * 70}")


def run_single(cr, exchanges, universe_name, mktcap_min, verbose=False, output_path=None,
               max_surprise=None, offset_days=1, benchmark_symbol=None, benchmark_name=None):
    """Run PEAD event study for a single exchange set."""
    global MAX_SURPRISE
    if max_surprise is not None:
        MAX_SURPRISE = max_surprise

    # Resolve benchmark if not passed
    if benchmark_symbol is None:
        benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    if benchmark_name is None:
        benchmark_name = benchmark_symbol

    exec_model = "Next-day close (MOC)" if offset_days > 0 else "Same-day close (legacy)"
    mktcap_label = f"{mktcap_min/1e9:.0f}B" if mktcap_min >= 1e9 else f"{mktcap_min/1e6:.0f}M"
    signal_desc = (f"Surprise = (actual - est) / |est|, "
                   f"MCap > {mktcap_label} local, |est| > ${MIN_ESTIMATE}")
    print_header("PEAD EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Max surprise: {MAX_SURPRISE*100:.0f}%")
    print("=" * 65)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    con = fetch_data(cr, exchanges, mktcap_min, benchmark_symbol=benchmark_symbol, verbose=verbose)
    if con is None:
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Phase 2: Compute event returns
    print(f"\nPhase 2: Computing event-window returns...")
    t1 = time.time()
    results = compute_event_returns(con, windows=WINDOWS, offset_days=offset_days, verbose=verbose)
    compute_time = time.time() - t1
    print(f"Returns computed in {compute_time:.0f}s")

    if not results:
        print("No valid event returns. Skipping.")
        con.close()
        return None

    # Phase 3: Compute metrics
    print("\nPhase 3: Computing CAR metrics...")
    car_metrics = compute_car_metrics(results, windows=WINDOWS)
    quintiles = compute_quintile_metrics(results, windows=WINDOWS)
    yearly = compute_yearly_stats(results)

    # Print results
    print_results(car_metrics, quintiles, universe_name)

    # Yearly summary
    if yearly:
        print(f"\n  Yearly Event Counts:")
        print(f"  {'Year':>6} {'Events':>8} {'Beats':>8} {'Misses':>8} {'Beat%':>8}")
        print(f"  {'-' * 40}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total_events']:>8} {y['beats']:>8} "
                  f"{y['misses']:>8} {y['beat_rate']:>7.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = build_output(car_metrics, quintiles, yearly, results, universe_name, benchmark, mktcap_min)

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
    parser = argparse.ArgumentParser(description="PEAD (Post-Earnings Announcement Drift) event study")
    add_common_args(parser)
    parser.add_argument("--max-surprise", type=float, default=MAX_SURPRISE,
                        help=f"Max absolute surprise ratio to include (default {MAX_SURPRISE})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("pead", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    offset_days = 0 if args.no_next_day else 1

    # --global mode
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
            ("india", ["NSE"]),
            ("germany", ["XETRA"]),
            ("china", ["SHZ", "SHH"]),
            ("korea", ["KSC"]),
            ("sweden", ["STO"]),
            ("hongkong", ["HKSE"]),
            ("australia", ["ASX"]),
            ("brazil", ["SAO"]),
            ("switzerland", ["SIX"]),
            ("taiwan", ["TAI"]),
            ("thailand", ["SET"]),
            ("norway", ["OSL"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"pead_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            mktcap_threshold = get_mktcap_threshold(preset_exchanges)
            bench_sym, bench_name = get_local_benchmark(preset_exchanges)

            try:
                result = run_single(cr, preset_exchanges, uni_name, mktcap_threshold,
                                    verbose=args.verbose, output_path=output_path,
                                    max_surprise=args.max_surprise,
                                    offset_days=offset_days,
                                    benchmark_symbol=bench_sym,
                                    benchmark_name=bench_name)
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

        # Print summary
        print(f"\n\n{'=' * 100}")
        print("PEAD EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 100}")
        print(f"{'Exchange':<15} {'Events':>8} {'Pos':>6} {'Neg':>6} "
              f"{'CAR+1d':>9} {'CAR+21d':>9} {'CAR+63d':>9} "
              f"{'t(63)':>7} {'Q5-Q1':>8}")
        print("-" * 100)

        for uni, r in sorted(all_results.items(),
                              key=lambda x: abs(x[1].get("car_metrics", {}).get("overall", {}).get("car_63d", {}).get("mean", 0) if isinstance(x[1].get("car_metrics", {}).get("overall", {}).get("car_63d"), dict) else 0),
                              reverse=True):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<15} {'ERROR / NO DATA':>8}")
                continue

            overall = r["car_metrics"]["overall"]
            n = overall.get("n_events", 0)
            pos_n = r.get("n_positive", 0)
            neg_n = r.get("n_negative", 0)

            c1 = overall.get("car_1d", {}).get("mean", 0) if isinstance(overall.get("car_1d"), dict) else 0
            c21 = overall.get("car_21d", {}).get("mean", 0) if isinstance(overall.get("car_21d"), dict) else 0
            c63 = overall.get("car_63d", {}).get("mean", 0) if isinstance(overall.get("car_63d"), dict) else 0
            t63 = overall.get("car_63d", {}).get("t_stat", 0) if isinstance(overall.get("car_63d"), dict) else 0

            # Q5-Q1 spread
            q = r.get("quintile_analysis", {})
            spread = ""
            if "Q5" in q and "Q1" in q:
                q5 = q["Q5"].get("car_63d", 0)
                q1 = q["Q1"].get("car_63d", 0)
                spread = f"{q5-q1:+.3f}%"

            print(f"{uni:<15} {n:>8} {pos_n:>6} {neg_n:>6} "
                  f"{c1:>+8.3f}% {c21:>+8.3f}% {c63:>+8.3f}% "
                  f"{t63:>+6.2f} {spread:>8}")

        print("=" * 100)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    bench_sym, bench_name = get_local_benchmark(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold, verbose=args.verbose,
               output_path=args.output, max_surprise=args.max_surprise,
               offset_days=offset_days,
               benchmark_symbol=bench_sym, benchmark_name=bench_name)


if __name__ == "__main__":
    main()
