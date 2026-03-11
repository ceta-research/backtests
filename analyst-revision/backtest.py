#!/usr/bin/env python3
"""
Analyst Rating Revision Event Study

Event study measuring abnormal returns after individual analyst rating upgrades
and downgrades. Uses FMP stock_grade data (individual grade changes per analyst
firm, not aggregate count snapshots).

Signal: Individual analyst upgrades (action='upgrade') or downgrades ('downgrade')
        from stock_grade table. Stratified by:
          - Category: upgrade vs downgrade
          - Cluster status: single upgrade vs clustered (2+ analysts, 30-day window)
          - Magnitude: small (Hold→Buy, +2) vs large (Sell→Buy, +4)

Event windows: T+1, T+5, T+21, T+63 trading days
Benchmark: SPY (US) or regional ETF

Key difference from event-08-upgrade-cluster:
  upgrade-cluster uses grades_historical (aggregate count changes: +2 analysts added to Buy).
  analyst-revision uses stock_grade (individual grade changes: one analyst raises from Hold to Buy).
  These capture related but distinct market signals.

Academic basis:
  - Stickel, S. (1995). "The Anatomy of the Performance of Buy and Sell Recommendations."
    Financial Analysts Journal, 51(5), 25-39.
  - Womack, K. (1996). "Do Brokerage Analysts' Recommendations Have Investment Value?"
    Journal of Finance, 51(1), 137-167.
  - Barber, B., Lehavy, R., McNichols, M. & Trueman, B. (2001). "Can Investors Profit
    from the Prophets?" Journal of Finance, 56(2), 531-563.

Usage:
    python3 analyst-revision/backtest.py                       # US default
    python3 analyst-revision/backtest.py --preset uk
    python3 analyst-revision/backtest.py --preset germany
    python3 analyst-revision/backtest.py --global --output analyst-revision/results/exchange_comparison.json
    python3 analyst-revision/backtest.py --preset us --verbose
"""

import argparse
import duckdb
import json
import math
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, REGIONAL_BENCHMARKS
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Parameters ---
WINDOWS = [1, 5, 21, 63]       # Trading day windows post-event
START_YEAR = 2012               # stock_grade sparse before 2012
END_YEAR = 2025
CLUSTER_WINDOW_DAYS = 30        # Days to look for additional analysts (cluster detection)
MIN_CLUSTER_ANALYSTS = 2        # Minimum distinct analysts to qualify as clustered
MIN_EVENTS = 50                 # Minimum events to report results
WINSORIZE_PCT = 1.0             # Winsorize at 1st/99th percentile
MIN_ENTRY_PRICE = 1.0           # Skip sub-$1 entry prices

# Grade score mapping for magnitude calculation
GRADE_SCORES = {
    # Bullish (score=5)
    "strong buy": 5, "buy": 5, "outperform": 5, "overweight": 5,
    "market outperform": 5, "positive": 5, "accumulate": 5,
    "top pick": 5, "conviction buy": 5, "add": 5, "long-term buy": 5,
    # Neutral (score=3)
    "hold": 3, "neutral": 3, "equal-weight": 3, "market perform": 3,
    "sector perform": 3, "in-line": 3, "peer perform": 3, "mixed": 3,
    "sector weight": 3, "market weight": 3, "equal weight": 3,
    # Bearish (score=1)
    "sell": 1, "underperform": 1, "underweight": 1, "market underperform": 1,
    "reduce": 1, "strong sell": 1, "negative": 1,
}


def grade_to_score(grade_str):
    if not grade_str:
        return None
    return GRADE_SCORES.get(grade_str.lower().strip())


def fetch_data(client, exchanges, mktcap_min, verbose=False):
    """Fetch analyst revision events, market caps, and prices via API."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    # 1. Fetch stock_grade events (individual analyst grade changes)
    # Deduplicate by (symbol, date, gradingCompany) keeping most recent fetch.
    print("  Fetching analyst grade revision events...")
    grades_sql = f"""
        WITH raw AS (
            SELECT
                symbol,
                CAST(date AS DATE) AS event_date,
                gradingCompany,
                previousGrade,
                newGrade,
                action,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, CAST(date AS DATE), gradingCompany
                    ORDER BY dateEpoch DESC
                ) AS rn
            FROM stock_grade
            WHERE CAST(date AS DATE) >= '{START_YEAR}-01-01'
              AND CAST(date AS DATE) <= '{END_YEAR}-12-31'
              AND action IN ('upgrade', 'downgrade')
              AND {sym_filter}
        )
        SELECT symbol, event_date, gradingCompany, previousGrade, newGrade, action
        FROM raw
        WHERE rn = 1
    """
    count = query_parquet(client, grades_sql, con, "raw_events",
                          verbose=verbose, limit=2000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} grade revision events (after dedup)")
    if count == 0:
        print("  No events found. Skipping.")
        con.close()
        return None

    cat_counts = con.execute("""
        SELECT action, COUNT(*) as n
        FROM raw_events
        GROUP BY action ORDER BY action
    """).fetchall()
    for cat, n in cat_counts:
        print(f"       {cat}: {n:,}")

    # 2. Add magnitude (grade score delta) locally
    print("  Computing grade magnitudes...")
    all_events = con.execute("""
        SELECT symbol, event_date, gradingCompany, previousGrade, newGrade, action
        FROM raw_events
    """).fetchall()

    event_rows = []
    for sym, evt_date, company, prev_g, new_g, action in all_events:
        new_score = grade_to_score(new_g)
        prev_score = grade_to_score(prev_g)
        if new_score is not None and prev_score is not None:
            magnitude = abs(new_score - prev_score)
        else:
            magnitude = 2  # Default: small change (can't compute without scores)

        mag_label = "large" if magnitude >= 4 else "small"
        event_rows.append((sym, evt_date.isoformat(), company, action, magnitude, mag_label))

    if not event_rows:
        print("  No events with valid grades. Skipping.")
        con.close()
        return None

    con.execute("""
        CREATE TABLE events_with_mag(
            symbol VARCHAR, event_date DATE, gradingCompany VARCHAR,
            action VARCHAR, magnitude INTEGER, mag_label VARCHAR
        )
    """)
    con.executemany("INSERT INTO events_with_mag VALUES (?, ?, ?, ?, ?, ?)", event_rows)
    print(f"    -> {len(event_rows)} events with magnitude computed")

    # 3. Cluster detection: tag each upgrade as clustered if 2+ distinct analysts
    #    upgraded the same stock within +/- CLUSTER_WINDOW_DAYS
    print("  Detecting upgrade clusters...")
    con.execute(f"""
        CREATE TABLE upgrade_cluster_tags AS
        WITH upgrades AS (
            SELECT symbol, event_date, gradingCompany
            FROM events_with_mag
            WHERE action = 'upgrade'
        )
        SELECT
            a.symbol, a.event_date, a.gradingCompany,
            COUNT(DISTINCT b.gradingCompany) AS other_upgrade_analysts
        FROM upgrades a
        LEFT JOIN upgrades b
            ON a.symbol = b.symbol
            AND b.event_date BETWEEN a.event_date - {CLUSTER_WINDOW_DAYS} AND a.event_date + {CLUSTER_WINDOW_DAYS}
            AND b.gradingCompany != a.gradingCompany
        GROUP BY a.symbol, a.event_date, a.gradingCompany
    """)

    # Final events table with cluster status
    con.execute(f"""
        CREATE TABLE final_events AS
        SELECT
            e.symbol, e.event_date, e.action, e.magnitude, e.mag_label,
            CASE
                WHEN e.action = 'upgrade' AND ct.other_upgrade_analysts >= {MIN_CLUSTER_ANALYSTS - 1}
                THEN 'clustered'
                ELSE 'single'
            END AS cluster_status,
            CASE
                WHEN e.action = 'upgrade' AND ct.other_upgrade_analysts >= {MIN_CLUSTER_ANALYSTS - 1}
                THEN 'upgrade_clustered'
                WHEN e.action = 'upgrade'
                THEN 'upgrade_single'
                ELSE 'downgrade'
            END AS category
        FROM events_with_mag e
        LEFT JOIN upgrade_cluster_tags ct
            ON e.symbol = ct.symbol
            AND e.event_date = ct.event_date
            AND e.gradingCompany = ct.gradingCompany
    """)
    n_final = con.execute("SELECT COUNT(*) FROM final_events").fetchone()[0]
    cat_final = con.execute("""
        SELECT category, COUNT(*) as n
        FROM final_events GROUP BY category ORDER BY n DESC
    """).fetchall()
    print(f"    -> {n_final} events with cluster tags")
    for cat, n in cat_final:
        print(f"       {cat}: {n:,}")

    if n_final < MIN_EVENTS:
        print(f"  Too few events. Skipping.")
        con.close()
        return None

    # 4. Market cap filter (FY key_metrics, most recent before event)
    print("  Fetching market cap data for filter...")
    mcap_sql = f"""
        SELECT symbol, dateEpoch AS filing_epoch, marketCap
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL AND {sym_filter}
    """
    mcap_count = query_parquet(client, mcap_sql, con, "mcap_cache",
                               verbose=verbose, limit=5000000, timeout=600,
                               memory_mb=4096, threads=2)
    print(f"    -> {mcap_count} market cap rows")

    # Apply market cap filter: keep events where most recent FY mcap > threshold
    con.execute(f"""
        CREATE TABLE events_filtered AS
        WITH matched AS (
            SELECT e.symbol, e.event_date, e.action, e.magnitude, e.mag_label,
                   e.cluster_status, e.category, m.marketCap,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.symbol, e.event_date
                       ORDER BY m.filing_epoch DESC
                   ) AS rn
            FROM final_events e
            LEFT JOIN mcap_cache m ON e.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(e.event_date)
        )
        SELECT symbol, event_date, action, magnitude, mag_label, cluster_status, category
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)

    n_filtered = con.execute("SELECT COUNT(*) FROM events_filtered").fetchone()[0]
    cat_filtered = con.execute("""
        SELECT category, COUNT(*) as n
        FROM events_filtered GROUP BY category ORDER BY n DESC
    """).fetchall()
    print(f"    -> {n_filtered} events after market cap filter")
    for cat, n in cat_filtered:
        print(f"       {cat}: {n:,}")

    if n_filtered < MIN_EVENTS:
        print(f"  Too few events after market cap filter. Skipping.")
        con.close()
        return None

    # 5. Determine benchmark
    benchmark = "SPY"
    if exchanges:
        for ex in exchanges:
            if ex in REGIONAL_BENCHMARKS:
                benchmark = REGIONAL_BENCHMARKS[ex]
                break

    # 6. Fetch prices for event symbols + benchmark
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM events_filtered").fetchall()]
    sym_list = event_symbols + [benchmark]
    sym_in = ", ".join(f"'{s}'" for s in sym_list)

    print(f"  Fetching prices for {len(event_symbols)} symbols + {benchmark}...")
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{START_YEAR - 1}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR + 1}-12-31'
          AND adjClose > {MIN_ENTRY_PRICE}
    """
    price_count = query_parquet(client, price_sql, con, "prices",
                                verbose=verbose, limit=10000000, timeout=600,
                                memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")
    print(f"    -> {price_count} price rows")

    # 7. Trading day calendar from benchmark
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
    if n_days < 100:
        print(f"  Too few trading days for {benchmark}. Skipping.")
        con.close()
        return None

    con.execute(f"CREATE TABLE config AS SELECT '{benchmark}' AS benchmark")
    return con


def compute_event_returns(con, windows=WINDOWS, verbose=False):
    """Compute abnormal returns at each window for all events."""
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Map each event to its T+0 trading day (first trading day >= event_date)
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT e.symbol, e.event_date, e.action, e.magnitude, e.mag_label,
               e.cluster_status, e.category,
               td.day_num AS t0_num, td.trade_date AS t0_date
        FROM events_filtered e
        ASOF JOIN trading_days td ON td.trade_date >= e.event_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # T+0 prices
    print("    Getting T+0 prices...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.event_date, et.action, et.magnitude, et.mag_label,
               et.cluster_status, et.category, et.t0_num, et.t0_date,
               sp.adjClose AS stock_t0, bp.adjClose AS bench_t0
        FROM event_t0 et
        JOIN prices sp ON et.symbol = sp.symbol AND et.t0_date = sp.trade_date
        JOIN prices bp ON bp.symbol = '{benchmark}' AND et.t0_date = bp.trade_date
        WHERE sp.adjClose > {MIN_ENTRY_PRICE} AND bp.adjClose > 0
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
        n_w = con.execute(f"SELECT COUNT(*) FROM window_{w}_returns").fetchone()[0]
        print(f"      -> {n_w} events with T+{w} returns")

    # Join all windows
    print("    Joining window results...")
    select_cols = ["eb.symbol", "eb.event_date", "eb.action", "eb.magnitude",
                   "eb.mag_label", "eb.cluster_status", "eb.category"]
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

    col_names = ["symbol", "event_date", "action", "magnitude", "mag_label",
                 "cluster_status", "category"]
    for w in windows:
        col_names.extend([f"stock_ret_{w}d", f"bench_ret_{w}d", f"abnormal_ret_{w}d"])

    results = []
    for row in rows:
        r = {}
        for i, col in enumerate(col_names):
            val = row[i]
            if col == "event_date":
                r[col] = val.isoformat() if hasattr(val, 'isoformat') else str(val)
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
    """Winsorize values at pct/100 and (100-pct)/100 percentiles."""
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    n = len(sorted_v)
    lo_idx = max(0, int(n * pct / 100))
    hi_idx = min(n - 1, int(n * (100 - pct) / 100))
    lo_val = sorted_v[lo_idx]
    hi_val = sorted_v[hi_idx]
    return [max(lo_val, min(hi_val, v)) for v in values]


def compute_car_stats(values_raw, is_upgrade=True):
    """Compute mean CAR, t-stat, hit rate for a set of abnormal returns."""
    if not values_raw or len(values_raw) < 5:
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
        t_stat = 0.0

    sorted_raw = sorted(values_raw)
    mid = len(sorted_raw) // 2
    median = (sorted_raw[mid] if len(sorted_raw) % 2 == 1
              else (sorted_raw[mid - 1] + sorted_raw[mid]) / 2)

    # Hit rate: upgrades should have abnormal_ret > 0, downgrades < 0
    if is_upgrade:
        hit_rate = sum(1 for v in values_raw if v > 0) / len(values_raw)
    else:
        hit_rate = sum(1 for v in values_raw if v < 0) / len(values_raw)

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
    """Compute CAR metrics overall, by category, and by magnitude."""
    metrics = {}

    # --- Upgrade (all) ---
    upgrades = [r for r in results if r["action"] == "upgrade"]
    if len(upgrades) >= MIN_EVENTS:
        metrics["upgrade_all"] = {"n": len(upgrades)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in upgrades if r.get(key) is not None]
            if vals:
                metrics["upgrade_all"][f"T+{w}"] = compute_car_stats(vals, is_upgrade=True)

    # --- Upgrade single ---
    up_single = [r for r in results if r["category"] == "upgrade_single"]
    if len(up_single) >= MIN_EVENTS:
        metrics["upgrade_single"] = {"n": len(up_single)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in up_single if r.get(key) is not None]
            if vals:
                metrics["upgrade_single"][f"T+{w}"] = compute_car_stats(vals, is_upgrade=True)

    # --- Upgrade clustered ---
    up_clustered = [r for r in results if r["category"] == "upgrade_clustered"]
    if len(up_clustered) >= MIN_EVENTS:
        metrics["upgrade_clustered"] = {"n": len(up_clustered)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in up_clustered if r.get(key) is not None]
            if vals:
                metrics["upgrade_clustered"][f"T+{w}"] = compute_car_stats(vals, is_upgrade=True)

    # --- Upgrade small magnitude ---
    up_small = [r for r in results if r["action"] == "upgrade" and r.get("mag_label") == "small"]
    if len(up_small) >= MIN_EVENTS:
        metrics["upgrade_small"] = {"n": len(up_small)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in up_small if r.get(key) is not None]
            if vals:
                metrics["upgrade_small"][f"T+{w}"] = compute_car_stats(vals, is_upgrade=True)

    # --- Upgrade large magnitude ---
    up_large = [r for r in results if r["action"] == "upgrade" and r.get("mag_label") == "large"]
    if len(up_large) >= MIN_EVENTS:
        metrics["upgrade_large"] = {"n": len(up_large)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in up_large if r.get(key) is not None]
            if vals:
                metrics["upgrade_large"][f"T+{w}"] = compute_car_stats(vals, is_upgrade=True)

    # --- Downgrade (all) ---
    downgrades = [r for r in results if r["action"] == "downgrade"]
    if len(downgrades) >= MIN_EVENTS:
        metrics["downgrade_all"] = {"n": len(downgrades)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in downgrades if r.get(key) is not None]
            if vals:
                metrics["downgrade_all"][f"T+{w}"] = compute_car_stats(vals, is_upgrade=False)

    return metrics


def compute_yearly_stats(results):
    """Event counts by year and category."""
    by_year = {}
    for r in results:
        year = str(r["event_date"])[:4]
        action = r.get("action", "unknown")
        cat = r.get("category", "unknown")
        if year not in by_year:
            by_year[year] = {"upgrades": 0, "downgrades": 0,
                             "upgrade_clustered": 0, "upgrade_single": 0}
        if action == "upgrade":
            by_year[year]["upgrades"] += 1
        elif action == "downgrade":
            by_year[year]["downgrades"] += 1
        if cat == "upgrade_clustered":
            by_year[year]["upgrade_clustered"] += 1
        elif cat == "upgrade_single":
            by_year[year]["upgrade_single"] += 1

    yearly = []
    for year in sorted(by_year.keys()):
        d = by_year[year]
        yearly.append({
            "year": int(year),
            "upgrades": d["upgrades"],
            "downgrades": d["downgrades"],
            "upgrade_clustered": d["upgrade_clustered"],
            "upgrade_single": d["upgrade_single"],
        })
    return yearly


def print_results(metrics, universe_name):
    """Print formatted results table."""
    print(f"\n{'=' * 72}")
    print(f"  ANALYST REVISION EVENT STUDY: {universe_name}")
    print(f"{'=' * 72}")

    cat_labels = {
        "upgrade_all": "All Upgrades",
        "upgrade_single": "Single Analyst Upgrades",
        "upgrade_clustered": f"Clustered Upgrades (2+ analysts / {CLUSTER_WINDOW_DAYS}d)",
        "upgrade_small": "Small Upgrades (Hold→Buy, +2)",
        "upgrade_large": "Large Upgrades (Sell→Buy, +4)",
        "downgrade_all": "All Downgrades",
    }

    for cat, label in cat_labels.items():
        section = metrics.get(cat)
        if not section:
            continue
        n = section.get("n", 0)
        print(f"\n  {label} (n={n:,})")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>4} {'Hit%':>8}")
        print(f"  {'-' * 55}")
        for w in WINDOWS:
            d = section.get(f"T+{w}")
            if not d:
                continue
            sig = " **" if d.get("significant_1pct") else (" *" if d.get("significant_5pct") else "")
            print(f"  T+{w:<7} {d['mean_car']:>+9.3f}% {d['median_car']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>4} {d['hit_rate']:>7.1f}%")

    print(f"\n{'=' * 72}")


def run_single(cr, exchanges, universe_name, mktcap_min, verbose=False, output_path=None):
    """Run analyst revision event study for one exchange set."""
    mktcap_label = (f"{mktcap_min/1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min/1e6:.0f}M")
    signal_desc = (
        f"Individual analyst upgrades/downgrades (stock_grade), MCap > {mktcap_label} local, "
        f"cluster = 2+ analysts / {CLUSTER_WINDOW_DAYS}d"
    )
    print_header("ANALYST REVISION EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print(f"  Period: {START_YEAR}-{END_YEAR}")
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
        print(f"  {'Year':>6} {'Upgrades':>10} {'Single':>8} {'Clustered':>10} {'Downgrades':>12}")
        print(f"  {'-' * 55}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['upgrades']:>10} {y['upgrade_single']:>8} "
                  f"{y['upgrade_clustered']:>10} {y['downgrades']:>12}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = {
        "strategy": "Analyst Rating Revision Momentum",
        "universe": universe_name,
        "benchmark": benchmark,
        "study_type": "event_study",
        "period": f"{START_YEAR}-{END_YEAR}",
        "filters": {
            "cluster_window_days": CLUSTER_WINDOW_DAYS,
            "min_cluster_analysts": MIN_CLUSTER_ANALYSTS,
            "min_market_cap": mktcap_min,
            "start_year": START_YEAR,
            "end_year": END_YEAR,
        },
        "windows": WINDOWS,
        "car_metrics": metrics,
        "yearly_stats": yearly,
        "n_total_upgrades": len([r for r in results if r["action"] == "upgrade"]),
        "n_total_downgrades": len([r for r in results if r["action"] == "downgrade"]),
        "n_clustered_upgrades": len([r for r in results if r["category"] == "upgrade_clustered"]),
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

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
    parser = argparse.ArgumentParser(description="Analyst Rating Revision event study")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("analyst-revision", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)

    # --global mode
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running eligible exchange presets")
        print("  Note: Only exchanges with substantial analyst grade coverage")
        print("  (Western markets). Asian markets excluded (insufficient data).")
        print("=" * 65)

        all_results = {}
        # Exchanges with sufficient analyst grade data in FMP stock_grade
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("uk",          ["LSE"]),
            ("germany",     ["XETRA"]),
            ("switzerland", ["SIX"]),
            ("canada",      ["TSX"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"analyst_revision_{uni_name}.json")

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

        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Summary table
        print(f"\n\n{'=' * 85}")
        print("ANALYST REVISION EVENT STUDY: EXCHANGE COMPARISON")
        print(f"{'=' * 85}")
        print(f"{'Exchange':<20} {'N(up)':>8} {'T+1 CAR':>10} {'T+21 CAR':>10} "
              f"{'t(T+1)':>8} {'t(T+21)':>8} {'Hit%@T+1':>9}")
        print("-" * 85)

        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("car_metrics", {}).get("upgrade_all", {}).get(
                                  "T+1", {}).get("mean_car", 0) if isinstance(x[1], dict) else 0),
                              reverse=True):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<20} {'ERROR / NO DATA'}")
                continue

            up = r["car_metrics"].get("upgrade_all", {})
            n = up.get("n", 0)
            c1 = up.get("T+1", {}).get("mean_car", 0) or 0
            c21 = up.get("T+21", {}).get("mean_car", 0) or 0
            t1 = up.get("T+1", {}).get("t_stat", 0) or 0
            t21 = up.get("T+21", {}).get("t_stat", 0) or 0
            h1 = up.get("T+1", {}).get("hit_rate", 0) or 0

            print(f"{uni:<20} {n:>8} {c1:>+9.3f}% {c21:>+9.3f}% "
                  f"{t1:>+7.2f} {t21:>+7.2f} {h1:>8.1f}%")

        print("=" * 85)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold,
               verbose=args.verbose, output_path=args.output)


if __name__ == "__main__":
    main()
