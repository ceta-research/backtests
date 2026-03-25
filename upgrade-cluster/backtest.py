#!/usr/bin/env python3
"""
Analyst Upgrade Clusters Event Study

Event study measuring abnormal returns after analyst rating upgrade clusters.
An "upgrade cluster" fires when the aggregate bullish count (StrongBuy + Buy)
increases by 2 or more between consecutive observations of the same symbol,
with 14–30 days between observations (eliminates high-frequency noise).

Data source: grades_historical (FMP aggregate analyst rating counts)
IMPORTANT: analystRatings* columns are UINT16 in parquet. Must cast to INTEGER
before computing deltas to avoid underflow on decreases.

Categories:
  upgrade_small  — bullish delta = 2 (minimum cluster)
  upgrade_medium — bullish delta = 3–4
  upgrade_large  — bullish delta >= 5 (strongest signal)
  downgrade_cluster — bearish delta >= 2

Event windows: T+1, T+5, T+21, T+63 trading days
Benchmark: SPY (US) or regional ETF

Academic reference:
  Womack, K. (1996). "Do Brokerage Analysts' Recommendations Have Investment Value?"
  Journal of Finance, 51(1), 137-167.
  Barber, B., Lehavy, R., McNichols, M. & Trueman, B. (2001). "Can Investors Profit
  from the Prophets?" Journal of Finance, 56(2), 531-563.

Usage:
    # US market (default)
    python3 upgrade-cluster/backtest.py

    # Specific preset
    python3 upgrade-cluster/backtest.py --preset india --verbose

    # Single exchange
    python3 upgrade-cluster/backtest.py --exchange JPX --output results/upgrade_cluster_JPX.json

    # All exchanges (global run)
    python3 upgrade-cluster/backtest.py --global --output upgrade-cluster/results/exchange_comparison.json

    # Live screen: current upgrade clusters
    python3 upgrade-cluster/screen.py

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
MIN_DELTA = 2                 # Minimum bullish count increase to flag as cluster
WINSORIZE_PCT = 1.0           # Winsorize at 1st/99th percentile
WINDOWS = [1, 5, 21, 63]     # Trading day windows post-event
START_YEAR = 2019             # grades_historical data sparse before 2019
END_YEAR = 2025
MIN_GAP_DAYS = 14             # Minimum days between observations (filter noisy daily updates)
MAX_GAP_DAYS = 30             # Maximum days (skip stale observations)
MIN_EVENTS = 50               # Minimum events to proceed with analysis


def fetch_data(client, exchanges, mktcap_min, verbose=False):
    """Fetch upgrade cluster events, market caps, and prices via API."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    # 1. Fetch grades_historical and detect upgrade clusters
    # CRITICAL: cast UINT16 columns to INTEGER to avoid overflow when counts decrease
    print("  Fetching analyst rating data and detecting clusters...")
    grades_sql = f"""
        SELECT
            symbol,
            CAST(date AS DATE) AS obs_date,
            CAST(analystRatingsStrongBuy AS INTEGER) + CAST(analystRatingsBuy AS INTEGER)
                AS bullish_count,
            CAST(analystRatingsSell AS INTEGER) + CAST(analystRatingsStrongSell AS INTEGER)
                AS bearish_count
        FROM grades_historical
        WHERE CAST(date AS DATE) >= '{START_YEAR}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR}-12-31'
          AND {sym_filter}
    """
    count = query_parquet(client, grades_sql, con, "raw_grades",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} rating observations")
    if count == 0:
        print("  No grades data. Skipping.")
        con.close()
        return None

    # Detect clusters using LAG() with gap filter
    con.execute(f"""
        CREATE TABLE cluster_events AS
        WITH lagged AS (
            SELECT
                symbol,
                obs_date,
                bullish_count,
                bearish_count,
                LAG(bullish_count) OVER (PARTITION BY symbol ORDER BY obs_date) AS prev_bullish,
                LAG(bearish_count) OVER (PARTITION BY symbol ORDER BY obs_date) AS prev_bearish,
                LAG(obs_date) OVER (PARTITION BY symbol ORDER BY obs_date) AS prev_date
            FROM raw_grades
        ),
        filtered AS (
            SELECT
                symbol,
                obs_date AS event_date,
                bullish_count - prev_bullish AS upgrade_delta,
                bearish_count - prev_bearish AS downgrade_delta,
                obs_date - prev_date AS gap_days
            FROM lagged
            WHERE prev_bullish IS NOT NULL
              AND (obs_date - prev_date) >= {MIN_GAP_DAYS}
              AND (obs_date - prev_date) <= {MAX_GAP_DAYS}
              AND (
                  bullish_count - prev_bullish >= {MIN_DELTA}
                  OR bearish_count - prev_bearish >= {MIN_DELTA}
              )
        )
        SELECT
            symbol,
            event_date,
            upgrade_delta,
            downgrade_delta,
            gap_days,
            CASE
                WHEN upgrade_delta >= 5 THEN 'upgrade_large'
                WHEN upgrade_delta >= 3 THEN 'upgrade_medium'
                WHEN upgrade_delta >= 2 THEN 'upgrade_small'
                WHEN downgrade_delta >= 2 THEN 'downgrade_cluster'
                ELSE NULL
            END AS category
        FROM filtered
        WHERE upgrade_delta >= {MIN_DELTA} OR downgrade_delta >= {MIN_DELTA}
    """)

    n_raw = con.execute("SELECT COUNT(*) FROM cluster_events").fetchone()[0]
    cat_counts = con.execute("""
        SELECT category, COUNT(*) as n
        FROM cluster_events
        GROUP BY category ORDER BY category
    """).fetchall()
    print(f"    -> {n_raw} cluster events detected")
    for cat, n in cat_counts:
        print(f"       {cat}: {n:,}")

    if n_raw < MIN_EVENTS:
        print(f"  Too few events ({n_raw} < {MIN_EVENTS}). Skipping.")
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
            SELECT c.symbol, c.event_date, c.upgrade_delta, c.downgrade_delta,
                c.gap_days, c.category, m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY c.symbol, c.event_date
                                   ORDER BY m.filing_epoch DESC) AS rn
            FROM cluster_events c
            LEFT JOIN mcap_cache m ON c.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(c.event_date)
        )
        SELECT symbol, event_date, upgrade_delta, downgrade_delta, gap_days, category
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)

    # Deduplicate: one event per symbol/date (keep highest upgrade delta)
    con.execute("""
        CREATE TABLE unique_events AS
        SELECT symbol, event_date, upgrade_delta, downgrade_delta, gap_days, category,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, event_date
                ORDER BY CASE
                    WHEN upgrade_delta >= 5 THEN 0
                    WHEN upgrade_delta >= 3 THEN 1
                    WHEN upgrade_delta >= 2 THEN 2
                    ELSE 3
                END
            ) AS rn
        FROM events
    """)
    con.execute("DELETE FROM unique_events WHERE rn > 1")
    con.execute("ALTER TABLE unique_events DROP COLUMN rn")

    n_events = con.execute("SELECT COUNT(*) FROM unique_events").fetchone()[0]
    cat_final = con.execute("""
        SELECT category, COUNT(*) as n
        FROM unique_events GROUP BY category ORDER BY n DESC
    """).fetchall()
    print(f"    -> {n_events} events after market cap filter")
    for cat, n in cat_final:
        print(f"       {cat}: {n:,}")

    if n_events < MIN_EVENTS:
        print(f"  Too few events after market cap filter ({n_events}). Skipping.")
        con.close()
        return None

    # 4. Get unique event symbols
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unique_events").fetchall()]

    # 5. Determine benchmark
    benchmark = "SPY"
    if exchanges:
        for ex in exchanges:
            if ex in REGIONAL_BENCHMARKS:
                benchmark = REGIONAL_BENCHMARKS[ex]
                break

    # 6. Fetch prices
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
    """Compute abnormal returns at each window for all cluster events."""
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Map each event to its T+0 trading day
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT e.symbol, e.event_date, e.upgrade_delta, e.downgrade_delta, e.category,
            td.day_num AS t0_num, td.trade_date AS t0_date
        FROM unique_events e
        ASOF JOIN trading_days td ON td.trade_date >= e.event_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # T+0 prices
    print("    Getting T+0 prices...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.event_date, et.upgrade_delta, et.downgrade_delta, et.category,
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
    select_cols = ["eb.symbol", "eb.event_date", "eb.upgrade_delta",
                   "eb.downgrade_delta", "eb.category"]
    join_clauses = []
    for w in windows:
        select_cols.extend([
            f"w{w}.stock_ret AS stock_ret_{w}d",
            f"w{w}.bench_ret AS bench_ret_{w}d",
            f"w{w}.abnormal_ret AS abnormal_ret_{w}d",
        ])
        join_clauses.append(
            f"LEFT JOIN window_{w}_returns w{w} "
            f"ON eb.symbol = w{w}.symbol AND eb.event_date = w{w}.event_date"
        )

    result_sql = f"""
        SELECT {', '.join(select_cols)}
        FROM event_base eb
        {' '.join(join_clauses)}
        WHERE w1.abnormal_ret IS NOT NULL
        ORDER BY eb.event_date
    """
    rows = con.execute(result_sql).fetchall()

    col_names = ["symbol", "event_date", "upgrade_delta", "downgrade_delta", "category"]
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
    """Winsorize values at pct/100–pct percentiles."""
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    n = len(sorted_v)
    lo_idx = max(0, int(n * pct / 100))
    hi_idx = min(n - 1, int(n * (100 - pct) / 100))
    lo_val = sorted_v[lo_idx]
    hi_val = sorted_v[hi_idx]
    return [max(lo_val, min(hi_val, v)) for v in values]


def compute_car_stats(values_raw):
    """Compute CAR stats: mean, median, t-stat, n, hit rate, significance."""
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
              else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2)

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
    """Compute CAR metrics overall and by cluster category."""
    categories = ["overall", "upgrade_small", "upgrade_medium", "upgrade_large",
                  "downgrade_cluster"]
    metrics = {}

    for cat in categories:
        if cat == "overall":
            # All upgrade clusters combined (exclude downgrade_cluster from overall)
            subset = [r for r in results if r.get("category", "").startswith("upgrade")]
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
                stats = compute_car_stats(raw_values)
                if stats:
                    metrics[cat][f"T+{w}"] = stats

    return metrics


def compute_yearly_stats(results):
    """Compute event counts by year and category."""
    by_year = {}
    for r in results:
        year = r["event_date"][:4]
        cat = r.get("category", "unknown")
        if year not in by_year:
            by_year[year] = {"total_upgrade": 0, "upgrade_small": 0,
                             "upgrade_medium": 0, "upgrade_large": 0,
                             "downgrade_cluster": 0}
        if cat.startswith("upgrade"):
            by_year[year]["total_upgrade"] += 1
        by_year[year][cat] = by_year[year].get(cat, 0) + 1

    yearly = []
    for year in sorted(by_year.keys()):
        d = by_year[year]
        yearly.append({
            "year": int(year),
            "total_upgrade": d.get("total_upgrade", 0),
            "upgrade_small": d.get("upgrade_small", 0),
            "upgrade_medium": d.get("upgrade_medium", 0),
            "upgrade_large": d.get("upgrade_large", 0),
            "downgrade_cluster": d.get("downgrade_cluster", 0),
        })
    return yearly


def print_results(metrics, universe_name):
    """Print formatted results table."""
    print(f"\n{'=' * 70}")
    print(f"  ANALYST UPGRADE CLUSTERS EVENT STUDY: {universe_name}")
    print(f"{'=' * 70}")

    categories = ["overall", "upgrade_small", "upgrade_medium", "upgrade_large",
                  "downgrade_cluster"]
    labels = {
        "overall": "All Upgrade Clusters (delta >= 2)",
        "upgrade_small": "Small Cluster (delta = 2)",
        "upgrade_medium": "Medium Cluster (delta = 3-4)",
        "upgrade_large": "Large Cluster (delta >= 5)",
        "downgrade_cluster": "Downgrade Cluster (bearish delta >= 2)",
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
    """Run upgrade cluster event study for a single exchange set."""
    mktcap_label = (f"{mktcap_min/1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min/1e6:.0f}M")
    signal_desc = (f"Bullish rating delta >= {MIN_DELTA} in {MIN_GAP_DAYS}–{MAX_GAP_DAYS}d window, "
                   f"MCap > {mktcap_label} local")
    print_header("ANALYST UPGRADE CLUSTERS EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print(f"  Period: {START_YEAR}–{END_YEAR} | Gap filter: {MIN_GAP_DAYS}–{MAX_GAP_DAYS} days")
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
        print(f"  {'Year':>6} {'Up-Total':>9} {'Small':>7} {'Medium':>7} {'Large':>7} {'Down':>7}")
        print(f"  {'-' * 50}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total_upgrade']:>9} {y['upgrade_small']:>7} "
                  f"{y['upgrade_medium']:>7} {y['upgrade_large']:>7} "
                  f"{y['downgrade_cluster']:>7}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = {
        "strategy": "Analyst Upgrade Clusters",
        "universe": universe_name,
        "benchmark": benchmark,
        "study_type": "event_study",
        "period": f"{START_YEAR}-{END_YEAR}",
        "filters": {
            "min_delta": MIN_DELTA,
            "min_gap_days": MIN_GAP_DAYS,
            "max_gap_days": MAX_GAP_DAYS,
            "min_market_cap": mktcap_min,
        },
        "windows": WINDOWS,
        "car_metrics": metrics,
        "yearly_stats": yearly,
        "n_total_upgrade_events": len([r for r in results if r.get("category", "").startswith("upgrade")]),
        "n_total_downgrade_events": len([r for r in results if r.get("category") == "downgrade_cluster"]),
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
    parser = argparse.ArgumentParser(description="Analyst Upgrade Clusters event study")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("upgrade-cluster", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)

    # --global mode: run all eligible exchange presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        presets_to_run = [
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("india", ["NSE"]),
            ("uk", ["LSE"]),
            ("japan", ["JPX"]),
            ("germany", ["XETRA"]),
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("korea", ["KSC"]),
            ("taiwan", ["TAI", "TWO"]),
            ("canada", ["TSX"]),
            ("australia", ["ASX"]),
            ("switzerland", ["SIX"]),
            ("sweden", ["STO"]),
            ("brazil", ["SAO"]),
            ("southafrica", ["JNB"]),
            ("thailand", ["SET"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"upgrade_cluster_{uni_name}.json")

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
        print("ANALYST UPGRADE CLUSTERS EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 90}")
        print(f"{'Exchange':<20} {'N(up)':>8} {'T+1 CAR':>10} {'T+21 CAR':>10} "
              f"{'t(T+1)':>8} {'t(T+21)':>8}")
        print("-" * 90)

        for uni, r in sorted(all_results.items(),
                              key=lambda x: abs(x[1].get("car_metrics", {}).get("overall", {}).get(
                                  "T+1", {}).get("mean_car", 0) if isinstance(x[1], dict) else 0),
                              reverse=True):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<20} {'ERROR / NO DATA'}")
                continue

            overall = r["car_metrics"].get("overall", {})
            n = overall.get("n", 0)
            c1 = overall.get("T+1", {}).get("mean_car", 0)
            c21 = overall.get("T+21", {}).get("mean_car", 0)
            t1 = overall.get("T+1", {}).get("t_stat", 0)
            t21 = overall.get("T+21", {}).get("t_stat", 0)

            print(f"{uni:<20} {n:>8} {c1:>+9.3f}% {c21:>+9.3f}% {t1:>+7.2f} {t21:>+7.2f}")

        print("=" * 90)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold,
               verbose=args.verbose, output_path=args.output)


if __name__ == "__main__":
    main()
