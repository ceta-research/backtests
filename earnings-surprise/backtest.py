#!/usr/bin/env python3
"""
Earnings Surprise Event Study (PEAD with Quintile Focus)

Event study measuring post-earnings announcement drift (PEAD) with full
quintile stratification of the surprise magnitude.

Signal: surprise = (epsActual - epsEstimated) / ABS(epsEstimated)
Categories: positive / negative + Q1 (worst miss) through Q5 (biggest beat)
Event windows: T+1, T+5, T+21, T+63 trading days
Benchmark: SPY (US) or regional ETF

Academic references:
  Ball & Brown (1968) "An Empirical Evaluation of Accounting Income Numbers"
  Bernard & Thomas (1989) "Post-Earnings-Announcement Drift: Delayed Price Response or Risk Premium?"
  Foster, Olsen & Shevlin (1984) "Earnings Releases, Anomalies, and the Behavior of Security Returns"

Usage:
    # US event study (default)
    python3 earnings-surprise/backtest.py

    # Specific exchange
    python3 earnings-surprise/backtest.py --preset india

    # Save results
    python3 earnings-surprise/backtest.py --preset us --output earnings-surprise/results/us.json --verbose

    # All exchanges
    python3 earnings-surprise/backtest.py --global --output earnings-surprise/results/exchange_comparison.json

See README.md for strategy details.
"""

import argparse
import duckdb
import json
import math
import os
import sys
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, LOCAL_INDEX_BENCHMARKS
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Parameters ---
MIN_ESTIMATE = 0.01       # |epsEstimated| > $0.01 (avoid near-zero denominator distortions)
MAX_SURPRISE = 5.0        # Cap surprise at 500% to reduce extreme outlier noise
WINSORIZE_PCT = 1.0       # Winsorize abnormal returns at 1st/99th percentile
WINDOWS = [1, 5, 21, 63]  # Trading day windows post-event
START_YEAR = 2000
END_YEAR = 2025


def fetch_data(client, exchanges, mktcap_min, verbose=False):
    """Fetch earnings surprises with quintile classification, prices, and market cap."""
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

    # Deduplicate (FMP has duplicate records per symbol/date), compute surprise
    con.execute(f"""
        CREATE TABLE surprises AS
        SELECT symbol,
            CAST(date AS DATE) AS event_date,
            epsActual,
            epsEstimated,
            (epsActual - epsEstimated) / ABS(epsEstimated) AS surprise_raw,
            CASE WHEN epsActual > epsEstimated THEN 'positive' ELSE 'negative' END AS category
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY symbol, CAST(date AS DATE)
                                   ORDER BY epsActual DESC) AS rn
            FROM raw_surprises
        ) WHERE rn = 1
          AND ABS((epsActual - epsEstimated) / ABS(epsEstimated)) <= {MAX_SURPRISE}
    """)

    n_deduped = con.execute("SELECT COUNT(*) FROM surprises").fetchone()[0]
    pos = con.execute("SELECT COUNT(*) FROM surprises WHERE category = 'positive'").fetchone()[0]
    neg = n_deduped - pos
    print(f"    -> {n_deduped} unique events after dedup+cap ({pos} positive, {neg} negative)")

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

    # 3. Filter by market cap (most recent FY filing before event)
    print("  Filtering by market cap...")
    con.execute(f"""
        CREATE TABLE events AS
        WITH matched AS (
            SELECT s.symbol, s.event_date, s.surprise_raw, s.category,
                m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY s.symbol, s.event_date
                                   ORDER BY m.filing_epoch DESC) AS rn
            FROM surprises s
            LEFT JOIN mcap_cache m ON s.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(s.event_date)
        )
        SELECT symbol, event_date, surprise_raw, category
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)
    # One event per symbol/date (take highest surprise_raw if still any dupes)
    con.execute("""
        CREATE TABLE pre_events AS
        SELECT symbol, event_date, surprise_raw, category,
            ROW_NUMBER() OVER (PARTITION BY symbol, event_date ORDER BY surprise_raw DESC) AS rn
        FROM events
    """)
    con.execute("DELETE FROM pre_events WHERE rn > 1")
    con.execute("ALTER TABLE pre_events DROP COLUMN rn")

    n_events = con.execute("SELECT COUNT(*) FROM pre_events").fetchone()[0]
    pos_f = con.execute("SELECT COUNT(*) FROM pre_events WHERE category = 'positive'").fetchone()[0]
    neg_f = n_events - pos_f
    print(f"    -> {n_events} unique events after market cap filter ({pos_f} positive, {neg_f} negative)")

    if n_events < 50:
        print("  Too few events for meaningful analysis. Skipping.")
        con.close()
        return None

    # 4. Assign quintiles globally (Q1 = biggest misses, Q5 = biggest beats)
    #    NTILE(5) over ALL events ordered by surprise_raw ascending
    con.execute("""
        CREATE TABLE unique_events AS
        SELECT symbol, event_date, surprise_raw, category,
            'Q' || CAST(NTILE(5) OVER (ORDER BY surprise_raw ASC) AS VARCHAR) AS quintile
        FROM pre_events
    """)

    quintile_counts = con.execute("""
        SELECT quintile, COUNT(*) as n
        FROM unique_events
        GROUP BY quintile ORDER BY quintile
    """).fetchall()
    print(f"    Quintile distribution:")
    for q, n in quintile_counts:
        print(f"       {q}: {n:,}")

    # 5. Get all unique event symbols
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unique_events").fetchall()]

    # 6. Determine benchmark (local currency index — e.g. Sensex for India, Nikkei for Japan)
    benchmark = "SPY"
    if exchanges:
        for ex in exchanges:
            if ex in LOCAL_INDEX_BENCHMARKS:
                benchmark = LOCAL_INDEX_BENCHMARKS[ex]
                break

    # 7. Fetch price data for event symbols + benchmark
    print(f"  Fetching prices for {len(event_symbols)} symbols + {benchmark}...")
    sym_list = event_symbols + [benchmark]
    sym_in = ", ".join(f"'{s}'" for s in sym_list)
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{START_YEAR - 1}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR + 1}-12-31'
          AND adjClose > 0
    """
    count = query_parquet(client, price_sql, con, "prices",
                          verbose=verbose, limit=10000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")
    print(f"    -> {count} price rows")

    # 8. Build trading day calendar from benchmark
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
    """Compute abnormal returns at each window for all events."""
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Map each event to its T+0 trading day using ASOF JOIN
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT e.symbol, e.event_date, e.surprise_raw, e.category, e.quintile,
            td.day_num AS t0_num, td.trade_date AS t0_date
        FROM unique_events e
        ASOF JOIN trading_days td ON td.trade_date >= e.event_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # T+0 prices for events (stock + benchmark)
    print("    Getting T+0 prices...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.event_date, et.surprise_raw, et.category, et.quintile,
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
    select_cols = ["eb.symbol", "eb.event_date", "eb.surprise_raw", "eb.category", "eb.quintile"]
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

    col_names = ["symbol", "event_date", "surprise_raw", "category", "quintile"]
    for w in windows:
        col_names.extend([f"stock_ret_{w}d", f"bench_ret_{w}d", f"abnormal_ret_{w}d"])

    results = []
    for row in rows:
        r = {}
        for i, col in enumerate(col_names):
            val = row[i]
            if col == "event_date":
                r[col] = val.isoformat() if isinstance(val, date) else str(val)
            elif col == "surprise_raw":
                r[col] = round(float(val) * 100, 2) if val is not None else 0.0
            elif isinstance(val, float):
                r[col] = round(val, 6)
            else:
                r[col] = val
        results.append(r)

    for w in windows:
        con.execute(f"DROP TABLE IF EXISTS window_{w}_returns")

    print(f"    -> {len(results)} events with full returns")
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


def compute_car_stats(raw_values):
    """Compute CAR stats (mean, median, t-stat, hit_rate) for a list of abnormal returns."""
    if not raw_values:
        return None
    values = winsorize(raw_values)
    n = len(values)
    mean_car = sum(values) / n
    if n > 1:
        var = sum((v - mean_car) ** 2 for v in values) / (n - 1)
        std = math.sqrt(var) if var > 0 else 0
        se = std / math.sqrt(n)
        t_stat = mean_car / se if se > 0 else 0
    else:
        std = 0
        t_stat = 0

    hit_rate = sum(1 for v in raw_values if v > 0) / len(raw_values)
    sorted_vals = sorted(raw_values)
    mid = len(sorted_vals) // 2
    median = (sorted_vals[mid] if len(sorted_vals) % 2 == 1
              else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2)

    return {
        "mean": round(mean_car * 100, 4),
        "median": round(median * 100, 4),
        "std": round(std * 100, 4),
        "t_stat": round(t_stat, 3),
        "n": len(raw_values),
        "hit_rate": round(hit_rate * 100, 2),
        "significant_5pct": abs(t_stat) > 1.96,
        "significant_1pct": abs(t_stat) > 2.576,
    }


def compute_metrics(results, windows=WINDOWS):
    """Compute CAR metrics for overall, positive, negative, and Q1-Q5 categories."""
    categories = {
        "overall": lambda r: True,
        "positive": lambda r: r.get("category") == "positive",
        "negative": lambda r: r.get("category") == "negative",
        "Q1": lambda r: r.get("quintile") == "Q1",
        "Q2": lambda r: r.get("quintile") == "Q2",
        "Q3": lambda r: r.get("quintile") == "Q3",
        "Q4": lambda r: r.get("quintile") == "Q4",
        "Q5": lambda r: r.get("quintile") == "Q5",
    }

    metrics = {}
    for cat, filter_fn in categories.items():
        subset = [r for r in results if filter_fn(r)]
        n = len(subset)
        if n == 0:
            continue

        metrics[cat] = {"n_events": n}

        # Surprise stats for this subset (using surprise_raw which is already in %)
        surprises = [r["surprise_raw"] for r in subset if r.get("surprise_raw") is not None]
        if surprises:
            metrics[cat]["mean_surprise_pct"] = round(sum(surprises) / len(surprises), 2)
            metrics[cat]["median_surprise_pct"] = round(
                sorted(surprises)[len(surprises) // 2], 2)

        for w in windows:
            key = f"abnormal_ret_{w}d"
            raw_values = [r[key] for r in subset if r.get(key) is not None]
            if raw_values:
                stats = compute_car_stats(raw_values)
                if stats:
                    metrics[cat][f"car_{w}d"] = stats

    return metrics


def compute_yearly_stats(results):
    """Compute event counts, beat rates, and surprise magnitudes by year."""
    by_year = {}
    for r in results:
        year = r["event_date"][:4]
        cat = r.get("category", "unknown")
        if year not in by_year:
            by_year[year] = {"total": 0, "positive": 0, "negative": 0}
        by_year[year]["total"] += 1
        by_year[year][cat] = by_year[year].get(cat, 0) + 1

    yearly = []
    for year in sorted(by_year.keys()):
        d = by_year[year]
        total = d["total"]
        yearly.append({
            "year": int(year),
            "total_events": total,
            "beats": d.get("positive", 0),
            "misses": d.get("negative", 0),
            "beat_rate": round(d.get("positive", 0) / total * 100, 1) if total > 0 else 0,
        })
    return yearly


def print_results(metrics, universe_name, windows=WINDOWS):
    """Print formatted results."""
    print(f"\n{'=' * 75}")
    print(f"  EARNINGS SURPRISE EVENT STUDY RESULTS: {universe_name}")
    print(f"{'=' * 75}")

    section_order = ["overall", "positive", "negative", "Q1", "Q2", "Q3", "Q4", "Q5"]
    section_labels = {
        "overall": "All Events",
        "positive": "Positive Surprises (Beats)",
        "negative": "Negative Surprises (Misses)",
        "Q1": "Q1 — Largest Misses",
        "Q2": "Q2",
        "Q3": "Q3 (Near-zero surprises)",
        "Q4": "Q4",
        "Q5": "Q5 — Largest Beats",
    }

    for cat in section_order:
        section = metrics.get(cat)
        if not section:
            continue
        n = section.get("n_events", 0)
        if n == 0:
            continue

        mean_surp = section.get("mean_surprise_pct", 0)
        print(f"\n  {section_labels[cat]} (n={n:,}, mean surprise={mean_surp:+.1f}%)")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>8}")
        print(f"  {'-' * 55}")

        for w in windows:
            d = section.get(f"car_{w}d")
            if d is None:
                continue
            sig = " **" if d.get("significant_1pct") else (" *" if d.get("significant_5pct") else "")
            print(f"  T+{w:<7} {d['mean']:>+9.3f}% {d['median']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

    # Q5-Q1 spread
    q5 = metrics.get("Q5", {})
    q1 = metrics.get("Q1", {})
    if q5 and q1:
        print(f"\n  Q5-Q1 Spread (monotonic drift evidence):")
        for w in windows:
            c5 = q5.get(f"car_{w}d", {}).get("mean", 0)
            c1 = q1.get(f"car_{w}d", {}).get("mean", 0)
            print(f"    T+{w}: {c5 - c1:+.3f}%")

    print(f"{'=' * 75}")


def run_single(cr, exchanges, universe_name, mktcap_min, verbose=False, output_path=None):
    """Run earnings surprise event study for a single exchange set."""
    mktcap_label = (f"{mktcap_min/1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min/1e6:.0f}M")
    signal_desc = (f"Surprise = (epsActual - epsEstimated) / |epsEstimated|, "
                   f"MCap > {mktcap_label} local, |est| > ${MIN_ESTIMATE}, "
                   f"cap at {MAX_SURPRISE*100:.0f}%")
    print_header("EARNINGS SURPRISE EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print(f"  Categories: positive / negative / Q1-Q5 (quintile by surprise magnitude)")
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
        print(f"\n  Yearly Event Counts:")
        print(f"  {'Year':>6} {'Events':>8} {'Beats':>8} {'Misses':>8} {'Beat%':>8}")
        print(f"  {'-' * 40}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total_events']:>8} {y['beats']:>8} "
                  f"{y['misses']:>8} {y['beat_rate']:>7.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = {
        "strategy": "Earnings Surprise (PEAD with Quintile Stratification)",
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
        "car_metrics": metrics,
        "yearly_stats": yearly,
        "n_total_events": len(results),
        "n_positive": sum(1 for r in results if r.get("category") == "positive"),
        "n_negative": sum(1 for r in results if r.get("category") == "negative"),
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

        # Event-level CSV
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
    parser = argparse.ArgumentParser(description="Earnings Surprise (PEAD) event study with quintile stratification")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("earnings-surprise", args_str=" ".join(cloud_args),
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
        # Note: ASX excluded (adjClose split adjustment artifacts). SAO included (ok for event studies).
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
            ("brazil", ["SAO"]),   # SAO ok for event studies (no adjClose needed)
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
                output_path = os.path.join(out_dir, f"earnings_surprise_{uni_name}.json")

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
        print(f"\n\n{'=' * 110}")
        print("EARNINGS SURPRISE EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 110}")
        print(f"{'Exchange':<20} {'Events':>8} {'Beats':>7} {'Misses':>7} "
              f"{'Beat T+1':>9} {'Beat T+63':>10} {'Miss T+1':>9} {'Miss T+63':>10} "
              f"{'Q5-Q1 T+63':>12}")
        print("-" * 110)

        for uni, r in sorted(all_results.items(),
                              key=lambda x: (
                                  x[1].get("car_metrics", {}).get("positive", {}).get("car_63d", {}).get("mean", 0)
                                  if isinstance(x[1], dict) else 0
                              ),
                              reverse=True):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<20} {'ERROR / NO DATA'}")
                continue

            cm = r["car_metrics"]
            n = r.get("n_total_events", 0)
            n_pos = r.get("n_positive", 0)
            n_neg = r.get("n_negative", 0)

            pos = cm.get("positive", {})
            neg = cm.get("negative", {})
            q5 = cm.get("Q5", {})
            q1 = cm.get("Q1", {})

            b1 = pos.get("car_1d", {}).get("mean", 0) if isinstance(pos.get("car_1d"), dict) else 0
            b63 = pos.get("car_63d", {}).get("mean", 0) if isinstance(pos.get("car_63d"), dict) else 0
            m1 = neg.get("car_1d", {}).get("mean", 0) if isinstance(neg.get("car_1d"), dict) else 0
            m63 = neg.get("car_63d", {}).get("mean", 0) if isinstance(neg.get("car_63d"), dict) else 0

            spread = ""
            if q5 and q1:
                q5_63 = q5.get("car_63d", {}).get("mean", 0) if isinstance(q5.get("car_63d"), dict) else 0
                q1_63 = q1.get("car_63d", {}).get("mean", 0) if isinstance(q1.get("car_63d"), dict) else 0
                spread = f"{q5_63 - q1_63:+.3f}%"

            print(f"{uni:<20} {n:>8} {n_pos:>7} {n_neg:>7} "
                  f"{b1:>+8.3f}% {b63:>+9.3f}% {m1:>+8.3f}% {m63:>+9.3f}% "
                  f"{spread:>12}")

        print("=" * 110)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold,
               verbose=args.verbose, output_path=args.output)


if __name__ == "__main__":
    main()
