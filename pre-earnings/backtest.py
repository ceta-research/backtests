#!/usr/bin/env python3
"""
Pre-Earnings Announcement Runup Event Study

Event study measuring abnormal returns in the days BEFORE earnings announcements.
Unlike PEAD (which measures drift after), this captures anticipatory price movement
leading up to the announcement.

Signal: Pre-event windows measured backward from T=0 (announcement date)
  T-10: 10 trading days before announcement
  T-5:  5 trading days before announcement
  T-1:  1 trading day before announcement
  T+1:  1 trading day after (announcement day reaction, for comparison)

Stratification: Beat rate computed point-in-time (only prior quarters counted)
  habitual_beater:  > 75% prior beats, >= 8 prior reports
  habitual_misser: < 25% prior beats, >= 8 prior reports
  mixed:           25-75% prior beats, >= 4 prior reports

Abnormal return (CAR) = stock window return - benchmark window return

Academic reference:
  Barber, De George, Lehavy & Trueman (2013) "The Earnings Announcement Premium
  and Trading Volume", Journal of Accounting Research 51(1), 53-99.
  So & Wang (2014) "News-Driven Return Reversals: Liquidity Provision Ahead of
  Earnings Announcements", Journal of Financial Economics 114(1), 20-35.

Usage:
    # US (default)
    python3 pre-earnings/backtest.py

    # Specific exchange
    python3 pre-earnings/backtest.py --preset india

    # All exchanges
    python3 pre-earnings/backtest.py --global --output results/exchange_comparison.json --verbose

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
from data_utils import query_parquet, get_local_benchmark, LOCAL_INDEX_BENCHMARKS
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Parameters ---
MIN_ESTIMATE = 0.01          # |epsEstimated| > $0.01 (avoid extreme ratios)
MIN_PRIOR_REPORTS = 4        # Min prior quarters to classify beat rate
MIN_PRIOR_FOR_HABITUAL = 8   # Min prior quarters for habitual_beater/misser label
HABITUAL_BEATER_THRESHOLD = 0.75   # Beat rate > 75% = habitual beater
HABITUAL_MISSER_THRESHOLD = 0.25   # Beat rate < 25% = habitual misser
WINSORIZE_PCT = 1.0          # Winsorize at 1st/99th percentile

PRE_WINDOWS = [10, 5, 1]     # Trading days before event (T-10, T-5, T-1)
POST_WINDOWS = [1]           # Trading days after event (T+1, announcement day)
ALL_WINDOWS = [("pre", w) for w in PRE_WINDOWS] + [("post", w) for w in POST_WINDOWS]

START_YEAR = 2000
END_YEAR = 2025


def fetch_data(client, exchanges, mktcap_min, verbose=False):
    """Fetch earnings surprise events, prices, and market cap data into DuckDB."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    # 1. Fetch all earnings surprise events (used for both events and beat rate computation)
    print("  Fetching earnings events...")
    events_sql = f"""
        SELECT symbol,
               CAST(date AS DATE) AS event_date,
               epsActual,
               epsEstimated,
               dateEpoch
        FROM earnings_surprises
        WHERE epsEstimated IS NOT NULL
          AND ABS(epsEstimated) > {MIN_ESTIMATE}
          AND epsActual IS NOT NULL
          AND {sym_filter}
    """
    count = query_parquet(client, events_sql, con, "all_events",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} total earnings events")
    if count == 0:
        print("  No earnings data. Skipping.")
        con.close()
        return None

    # Deduplicate by symbol + date (keep most recent if duplicates)
    con.execute("""
        CREATE TABLE events_deduped AS
        SELECT symbol, event_date, epsActual, epsEstimated,
            ROW_NUMBER() OVER (PARTITION BY symbol, event_date ORDER BY event_date DESC) AS rn
        FROM all_events
    """)
    con.execute("DELETE FROM events_deduped WHERE rn > 1")
    con.execute("ALTER TABLE events_deduped DROP COLUMN rn")
    n_deduped = con.execute("SELECT COUNT(*) FROM events_deduped").fetchone()[0]
    print(f"    -> {n_deduped} events after dedup")

    # 2. Compute rolling (point-in-time) beat rates for each event
    # For each event, count only PRIOR events for the same symbol
    print("  Computing point-in-time beat rates...")
    con.execute(f"""
        CREATE TABLE event_with_beatrate AS
        WITH prior_stats AS (
            SELECT
                e.symbol,
                e.event_date,
                e.epsActual,
                e.epsEstimated,
                -- Count prior events (strictly before this event)
                COUNT(CASE WHEN p.event_date < e.event_date THEN 1 END) AS prior_reports,
                SUM(CASE WHEN p.event_date < e.event_date
                              AND p.epsActual > p.epsEstimated THEN 1 ELSE 0 END) AS prior_beats
            FROM events_deduped e
            LEFT JOIN events_deduped p ON e.symbol = p.symbol
                AND p.event_date < e.event_date
            GROUP BY e.symbol, e.event_date, e.epsActual, e.epsEstimated
        )
        SELECT
            symbol, event_date, epsActual, epsEstimated,
            prior_reports,
            prior_beats,
            CASE WHEN prior_reports > 0
                 THEN CAST(prior_beats AS DOUBLE) / prior_reports
                 ELSE NULL END AS beat_rate,
            CASE
                WHEN prior_reports >= {MIN_PRIOR_FOR_HABITUAL}
                     AND CAST(prior_beats AS DOUBLE) / prior_reports > {HABITUAL_BEATER_THRESHOLD}
                     THEN 'habitual_beater'
                WHEN prior_reports >= {MIN_PRIOR_FOR_HABITUAL}
                     AND CAST(prior_beats AS DOUBLE) / prior_reports < {HABITUAL_MISSER_THRESHOLD}
                     THEN 'habitual_misser'
                WHEN prior_reports >= {MIN_PRIOR_REPORTS}
                     THEN 'mixed'
                ELSE 'insufficient_history'
            END AS beat_category
        FROM prior_stats
    """)
    n_categorized = con.execute("SELECT COUNT(*) FROM event_with_beatrate WHERE beat_category != 'insufficient_history'").fetchone()[0]
    print(f"    -> {n_categorized} events with sufficient history for categorization")

    if n_categorized == 0:
        print("  No events with sufficient history. Skipping.")
        con.close()
        return None

    # 3. Filter to backtest period and events with categorization
    con.execute(f"""
        CREATE TABLE backtest_events AS
        SELECT * FROM event_with_beatrate
        WHERE beat_category != 'insufficient_history'
          AND CAST(event_date AS DATE) >= '{START_YEAR}-01-01'
          AND CAST(event_date AS DATE) <= '{END_YEAR}-12-31'
    """)
    n_in_period = con.execute("SELECT COUNT(*) FROM backtest_events").fetchone()[0]
    print(f"    -> {n_in_period} events in {START_YEAR}-{END_YEAR}")

    # 4. Market cap filtering using FY key_metrics
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

    print("  Applying market cap filter...")
    con.execute(f"""
        CREATE TABLE filtered_events AS
        WITH matched AS (
            SELECT be.symbol, be.event_date, be.beat_rate, be.beat_category,
                m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY be.symbol, be.event_date
                                   ORDER BY m.filing_epoch DESC) AS rn
            FROM backtest_events be
            LEFT JOIN mcap_cache m ON be.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(be.event_date)
        )
        SELECT symbol, event_date, beat_rate, beat_category
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)
    n_filtered = con.execute("SELECT COUNT(*) FROM filtered_events").fetchone()[0]
    n_habitual_beater = con.execute("SELECT COUNT(*) FROM filtered_events WHERE beat_category = 'habitual_beater'").fetchone()[0]
    n_habitual_misser = con.execute("SELECT COUNT(*) FROM filtered_events WHERE beat_category = 'habitual_misser'").fetchone()[0]
    n_mixed = con.execute("SELECT COUNT(*) FROM filtered_events WHERE beat_category = 'mixed'").fetchone()[0]
    print(f"    -> {n_filtered} events after market cap filter")
    print(f"       habitual_beater={n_habitual_beater}, mixed={n_mixed}, habitual_misser={n_habitual_misser}")

    if n_filtered < 50:
        print("  Too few events for meaningful analysis. Skipping.")
        con.close()
        return None

    # 5. Get unique event symbols for price fetching
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM filtered_events").fetchall()]
    print(f"  Fetching prices for {len(event_symbols)} symbols + benchmark...")

    # Determine benchmark — use local currency index (same calendar + currency as stocks)
    benchmark, benchmark_name = get_local_benchmark(exchanges)

    # 6. Fetch prices for event symbols + benchmark
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
    price_count = query_parquet(client, price_sql, con, "prices",
                                verbose=verbose, limit=10000000, timeout=600,
                                memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")
    print(f"    -> {price_count} price rows")

    # 7. Build trading day calendar from benchmark
    con.execute(f"""
        CREATE TABLE trading_days AS
        SELECT trade_date,
            ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM prices
        WHERE symbol = '{benchmark}'
        ORDER BY trade_date
    """)
    n_trading_days = con.execute("SELECT COUNT(*) FROM trading_days").fetchone()[0]
    print(f"    -> {n_trading_days} trading days from {benchmark}")

    con.execute(f"CREATE TABLE config AS SELECT '{benchmark}' AS benchmark, '{benchmark_name}' AS benchmark_name")

    return con


def compute_event_returns(con, verbose=False):
    """Compute pre-event and post-event abnormal returns for all events.

    Pre-event: return from T-N to T0 (how much did stock rise before announcement)
    Post-event: return from T0 to T+1 (announcement day reaction)

    Returns list of event dicts with CARs at each window.
    """
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]

    # Step 1: Map each event to its T0 trading day using ASOF JOIN
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT fe.symbol, fe.event_date, fe.beat_rate, fe.beat_category,
            td.day_num AS t0_num, td.trade_date AS t0_date
        FROM filtered_events fe
        ASOF JOIN trading_days td ON td.trade_date >= fe.event_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # Step 2: Get T0 prices for all events (stock + benchmark)
    print("    Getting T0 prices...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.event_date, et.beat_rate, et.beat_category,
            et.t0_num, et.t0_date,
            sp.adjClose AS stock_t0, bp.adjClose AS bench_t0
        FROM event_t0 et
        JOIN prices sp ON et.symbol = sp.symbol AND et.t0_date = sp.trade_date
        JOIN prices bp ON bp.symbol = '{benchmark}' AND et.t0_date = bp.trade_date
        WHERE sp.adjClose > 0 AND bp.adjClose > 0
    """)
    n_priced = con.execute("SELECT COUNT(*) FROM event_base").fetchone()[0]
    print(f"    -> {n_priced} events with T0 prices")
    con.execute("DROP TABLE event_t0")

    # Step 3: Compute PRE-event returns (T-W to T0)
    # Return = (price_T0 - price_T-W) / price_T-W (stock went up before earnings)
    # Abnormal = stock_window_return - benchmark_window_return
    for w in PRE_WINDOWS:
        print(f"    Computing T-{w} pre-event returns...")
        con.execute(f"""
            CREATE OR REPLACE TABLE pre_window_{w}_returns AS
            SELECT eb.symbol, eb.event_date,
                -- Stock: went from T-w price to T0 price
                ROUND((eb.stock_t0 - sp.adjClose) / sp.adjClose, 8) AS stock_ret,
                -- Benchmark: same window
                ROUND((eb.bench_t0 - bp.adjClose) / bp.adjClose, 8) AS bench_ret,
                -- Abnormal return
                ROUND((eb.stock_t0 - sp.adjClose) / sp.adjClose
                     - (eb.bench_t0 - bp.adjClose) / bp.adjClose, 8) AS abnormal_ret
            FROM event_base eb
            JOIN trading_days td ON td.day_num = eb.t0_num - {w}
            JOIN prices sp ON eb.symbol = sp.symbol AND td.trade_date = sp.trade_date
            JOIN prices bp ON bp.symbol = '{benchmark}' AND td.trade_date = bp.trade_date
        """)
        n = con.execute(f"SELECT COUNT(*) FROM pre_window_{w}_returns").fetchone()[0]
        print(f"      -> {n} events with T-{w} returns")

    # Step 4: Compute POST-event returns (T0 to T+W)
    for w in POST_WINDOWS:
        print(f"    Computing T+{w} post-event returns...")
        con.execute(f"""
            CREATE OR REPLACE TABLE post_window_{w}_returns AS
            SELECT eb.symbol, eb.event_date,
                -- Stock: went from T0 price to T+w price
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0, 8) AS stock_ret,
                -- Benchmark: same window
                ROUND((bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS bench_ret,
                -- Abnormal return
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0
                     - (bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS abnormal_ret
            FROM event_base eb
            JOIN trading_days td ON td.day_num = eb.t0_num + {w}
            JOIN prices sp ON eb.symbol = sp.symbol AND td.trade_date = sp.trade_date
            JOIN prices bp ON bp.symbol = '{benchmark}' AND td.trade_date = bp.trade_date
        """)
        n = con.execute(f"SELECT COUNT(*) FROM post_window_{w}_returns").fetchone()[0]
        print(f"      -> {n} events with T+{w} returns")

    # Step 5: Join all windows to event_base
    print("    Joining window results...")
    select_cols = ["eb.symbol", "eb.event_date", "eb.beat_rate", "eb.beat_category"]
    join_clauses = []

    for w in PRE_WINDOWS:
        alias = f"pre{w}"
        select_cols.extend([
            f"{alias}.stock_ret AS pre_stock_ret_{w}d",
            f"{alias}.bench_ret AS pre_bench_ret_{w}d",
            f"{alias}.abnormal_ret AS pre_abnormal_ret_{w}d",
        ])
        join_clauses.append(
            f"LEFT JOIN pre_window_{w}_returns {alias} "
            f"ON eb.symbol = {alias}.symbol AND eb.event_date = {alias}.event_date"
        )

    for w in POST_WINDOWS:
        alias = f"post{w}"
        select_cols.extend([
            f"{alias}.stock_ret AS post_stock_ret_{w}d",
            f"{alias}.bench_ret AS post_bench_ret_{w}d",
            f"{alias}.abnormal_ret AS post_abnormal_ret_{w}d",
        ])
        join_clauses.append(
            f"LEFT JOIN post_window_{w}_returns {alias} "
            f"ON eb.symbol = {alias}.symbol AND eb.event_date = {alias}.event_date"
        )

    # Require T-10 return (widest pre-window) for event inclusion
    result_sql = f"""
        SELECT {', '.join(select_cols)}
        FROM event_base eb
        {' '.join(join_clauses)}
        WHERE pre10.abnormal_ret IS NOT NULL
        ORDER BY eb.event_date
    """
    rows = con.execute(result_sql).fetchall()

    col_names = ["symbol", "event_date", "beat_rate", "beat_category"]
    for w in PRE_WINDOWS:
        col_names.extend([f"pre_stock_ret_{w}d", f"pre_bench_ret_{w}d", f"pre_abnormal_ret_{w}d"])
    for w in POST_WINDOWS:
        col_names.extend([f"post_stock_ret_{w}d", f"post_bench_ret_{w}d", f"post_abnormal_ret_{w}d"])

    results = []
    for row in rows:
        r = {}
        for i, col in enumerate(col_names):
            val = row[i]
            if col == "event_date":
                r[col] = val.isoformat() if isinstance(val, date) else str(val)
            elif col == "beat_rate":
                r[col] = round(float(val) * 100, 2) if val is not None else None
            elif isinstance(val, float):
                r[col] = round(val, 6)
            else:
                r[col] = val
        results.append(r)

    skipped = n_priced - len(results)
    print(f"    -> {len(results)} events with complete returns, {skipped} skipped (missing prices)")

    # Cleanup window tables
    for w in PRE_WINDOWS:
        con.execute(f"DROP TABLE IF EXISTS pre_window_{w}_returns")
    for w in POST_WINDOWS:
        con.execute(f"DROP TABLE IF EXISTS post_window_{w}_returns")

    return results


def winsorize(values, pct=WINSORIZE_PCT):
    """Winsorize at pct/100-pct percentiles."""
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
    """Compute CAR statistics for a set of abnormal returns."""
    if not values_raw:
        return None
    values = winsorize(values_raw)
    mean_car = sum(values) / len(values)

    if len(values) > 1:
        var = sum((v - mean_car) ** 2 for v in values) / (len(values) - 1)
        std = math.sqrt(var) if var > 0 else 0
        se = std / math.sqrt(len(values))
        t_stat = mean_car / se if se > 0 else 0
    else:
        std = 0
        t_stat = 0

    # Median on raw (more robust)
    sorted_vals = sorted(values_raw)
    mid = len(sorted_vals) // 2
    median = (sorted_vals[mid] if len(sorted_vals) % 2 == 1
              else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2)

    hit_rate = sum(1 for v in values_raw if v > 0) / len(values_raw)

    return {
        "mean": round(mean_car * 100, 4),
        "median": round(median * 100, 4),
        "std": round(std * 100, 4),
        "t_stat": round(t_stat, 3),
        "significant": abs(t_stat) > 1.96,
        "hit_rate": round(hit_rate * 100, 2),
        "n_obs": len(values_raw),
    }


def compute_metrics(results):
    """Compute CAR metrics by window and beat rate category."""
    categories = ["overall", "habitual_beater", "mixed", "habitual_misser"]
    metrics = {}

    for label in categories:
        if label == "overall":
            subset = results
        else:
            subset = [r for r in results if r.get("beat_category") == label]

        if not subset:
            continue

        cat_metrics = {"n_events": len(subset)}

        # Pre-event windows
        for w in PRE_WINDOWS:
            key = f"pre_abnormal_ret_{w}d"
            raw = [r[key] for r in subset if r.get(key) is not None]
            stats = compute_car_stats(raw)
            if stats:
                cat_metrics[f"car_pre_{w}d"] = stats

        # Post-event windows
        for w in POST_WINDOWS:
            key = f"post_abnormal_ret_{w}d"
            raw = [r[key] for r in subset if r.get(key) is not None]
            stats = compute_car_stats(raw)
            if stats:
                cat_metrics[f"car_post_{w}d"] = stats

        metrics[label] = cat_metrics

    return metrics


def compute_beat_rate_quintiles(results):
    """Stratify events by beat rate quintile and compute CARs."""
    with_rate = [r for r in results if r.get("beat_rate") is not None]
    with_rate.sort(key=lambda r: r["beat_rate"])
    n = len(with_rate)
    if n < 50:
        return {}

    q_size = n // 5
    quintiles = {}
    for q in range(5):
        start = q * q_size
        end = start + q_size if q < 4 else n
        subset = with_rate[start:end]
        q_label = f"Q{q+1}"
        rates = [r["beat_rate"] for r in subset]
        quintiles[q_label] = {
            "n_events": len(subset),
            "beat_rate_range": f"{min(rates):.1f}% to {max(rates):.1f}%",
            "mean_beat_rate": round(sum(rates) / len(rates), 2),
        }
        for w in PRE_WINDOWS:
            key = f"pre_abnormal_ret_{w}d"
            raw = [r[key] for r in subset if r.get(key) is not None]
            if raw:
                vals = winsorize(raw)
                quintiles[q_label][f"car_pre_{w}d"] = round(sum(vals) / len(vals) * 100, 4)
        for w in POST_WINDOWS:
            key = f"post_abnormal_ret_{w}d"
            raw = [r[key] for r in subset if r.get(key) is not None]
            if raw:
                vals = winsorize(raw)
                quintiles[q_label][f"car_post_{w}d"] = round(sum(vals) / len(vals) * 100, 4)

    return quintiles


def compute_yearly_stats(results):
    """Event counts and category breakdown by year."""
    by_year = {}
    for r in results:
        year = r["event_date"][:4]
        if year not in by_year:
            by_year[year] = {"total": 0, "habitual_beater": 0, "mixed": 0, "habitual_misser": 0}
        by_year[year]["total"] += 1
        cat = r.get("beat_category", "mixed")
        if cat in by_year[year]:
            by_year[year][cat] += 1

    return [
        {
            "year": int(y),
            "total_events": by_year[y]["total"],
            "habitual_beater": by_year[y]["habitual_beater"],
            "mixed": by_year[y]["mixed"],
            "habitual_misser": by_year[y]["habitual_misser"],
        }
        for y in sorted(by_year.keys())
    ]


def build_output(metrics, quintiles, yearly, results, universe_name, benchmark, benchmark_name, mktcap_min):
    """Build JSON output."""
    return {
        "strategy": "Pre-Earnings Announcement Runup",
        "universe": universe_name,
        "benchmark": benchmark,
        "benchmark_name": benchmark_name,
        "study_type": "event_study",
        "period": f"{START_YEAR}-{END_YEAR}",
        "filters": {
            "min_market_cap": mktcap_min,
            "min_estimate": MIN_ESTIMATE,
            "min_prior_reports": MIN_PRIOR_REPORTS,
            "min_prior_for_habitual": MIN_PRIOR_FOR_HABITUAL,
            "habitual_beater_threshold": HABITUAL_BEATER_THRESHOLD,
            "habitual_misser_threshold": HABITUAL_MISSER_THRESHOLD,
        },
        "pre_windows": PRE_WINDOWS,
        "post_windows": POST_WINDOWS,
        "car_metrics": metrics,
        "quintile_analysis": quintiles,
        "yearly_stats": yearly,
        "n_total_events": len(results),
        "n_habitual_beater": sum(1 for r in results if r.get("beat_category") == "habitual_beater"),
        "n_habitual_misser": sum(1 for r in results if r.get("beat_category") == "habitual_misser"),
        "n_mixed": sum(1 for r in results if r.get("beat_category") == "mixed"),
    }


def print_results(metrics, quintiles, universe_name):
    """Print formatted results."""
    print(f"\n{'=' * 80}")
    print(f"  PRE-EARNINGS RUNUP EVENT STUDY: {universe_name}")
    print(f"{'=' * 80}")

    for label in ["overall", "habitual_beater", "mixed", "habitual_misser"]:
        section = metrics.get(label, {})
        n = section.get("n_events", 0)
        if n == 0:
            continue

        title = {
            "overall": "All Events",
            "habitual_beater": "Habitual Beaters (>75% beat rate, 8+ quarters)",
            "habitual_misser": "Habitual Missers (<25% beat rate, 8+ quarters)",
            "mixed": "Mixed (25-75% beat rate)",
        }[label]
        print(f"\n  {title} (n={n:,})")
        print(f"  {'Window':<12} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>8}")
        print(f"  {'-' * 55}")

        for w in PRE_WINDOWS:
            d = section.get(f"car_pre_{w}d")
            if d is None:
                continue
            sig = " **" if d["significant"] else ""
            print(f"  T-{w:<9} {d['mean']:>+9.3f}% {d['median']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

        for w in POST_WINDOWS:
            d = section.get(f"car_post_{w}d")
            if d is None:
                continue
            sig = " **" if d["significant"] else ""
            print(f"  T+{w:<9} {d['mean']:>+9.3f}% {d['median']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

    if quintiles:
        print(f"\n  Beat Rate Quintile Analysis (Q1=lowest, Q5=highest beat rate)")
        print(f"  {'Q':<5} {'Events':>8} {'Beat%':>20} {'T-10 CAR':>10} {'T-5 CAR':>10} {'T+1 CAR':>10}")
        print(f"  {'-' * 65}")
        for q in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
            d = quintiles.get(q, {})
            if not d:
                continue
            c10 = d.get("car_pre_10d", 0)
            c5 = d.get("car_pre_5d", 0)
            c1post = d.get("car_post_1d", 0)
            print(f"  {q:<5} {d['n_events']:>8} {d['beat_rate_range']:>20} "
                  f"{c10:>+9.3f}% {c5:>+9.3f}% {c1post:>+9.3f}%")

    print(f"{'=' * 80}")


def run_single(cr, exchanges, universe_name, mktcap_min, verbose=False, output_path=None):
    """Run pre-earnings event study for a single exchange set."""
    mktcap_label = (f"{mktcap_min/1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min/1e6:.0f}M")
    signal_desc = (f"Pre-event CAR at T-10/T-5/T-1, "
                   f"MCap>{mktcap_label} local, habitual/mixed/misser classification")
    print_header("PRE-EARNINGS RUNUP EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Pre-event windows: {', '.join(f'T-{w}' for w in PRE_WINDOWS)}")
    print(f"  Post-event windows: {', '.join(f'T+{w}' for w in POST_WINDOWS)}")
    print(f"  Habitual beater: >{HABITUAL_BEATER_THRESHOLD*100:.0f}% beat rate, "
          f">={MIN_PRIOR_FOR_HABITUAL} quarters")
    print("=" * 65)

    t0 = time.time()
    print("\nPhase 1: Fetching data...")
    con = fetch_data(cr, exchanges, mktcap_min, verbose=verbose)
    if con is None:
        return None
    fetch_time = time.time() - t0

    row = con.execute("SELECT benchmark, benchmark_name FROM config").fetchone()
    benchmark, benchmark_name = row[0], row[1]

    print(f"\nPhase 2: Computing returns...")
    t1 = time.time()
    results = compute_event_returns(con, verbose=verbose)
    compute_time = time.time() - t1

    if not results:
        print("No valid event returns. Skipping.")
        con.close()
        return None

    print(f"\nPhase 3: Computing metrics...")
    metrics = compute_metrics(results)
    quintiles = compute_beat_rate_quintiles(results)
    yearly = compute_yearly_stats(results)

    print_results(metrics, quintiles, universe_name)

    # Yearly summary
    if yearly:
        print(f"\n  Yearly Event Counts:")
        print(f"  {'Year':>6} {'Total':>8} {'Beaters':>8} {'Mixed':>8} {'Missers':>8}")
        print(f"  {'-' * 45}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total_events']:>8} {y['habitual_beater']:>8} "
                  f"{y['mixed']:>8} {y['habitual_misser']:>8}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = build_output(metrics, quintiles, yearly, results, universe_name, benchmark, benchmark_name, mktcap_min)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

        # Save event-level CSV
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
    parser = argparse.ArgumentParser(
        description="Pre-Earnings Announcement Runup event study")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("pre-earnings", args_str=" ".join(cloud_args),
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
            ("taiwan", ["TAI", "TWO"]),
            ("korea", ["KSC"]),
            ("sweden", ["STO"]),
            ("thailand", ["SET"]),
            ("norway", ["OSL"]),
            ("brazil", ["SAO"]),
            ("hongkong", ["HKSE"]),
            ("switzerland", ["SIX"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"pre_earnings_{uni_name}.json")

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

        # Print summary
        print(f"\n\n{'=' * 110}")
        print("PRE-EARNINGS RUNUP: EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 110}")
        print(f"{'Exchange':<18} {'Bench':>10} {'Events':>8} {'Beat%':>6} "
              f"{'All T-10':>10} {'Beat T-10':>10} {'All T-5':>10} "
              f"{'t(T-10)':>8} {'Sig':>4}")
        print("-" * 120)

        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("car_metrics", {}).get("habitual_beater", {}).get("car_pre_10d", {}).get("mean", -99)
                                             if isinstance(x[1].get("car_metrics", {}).get("habitual_beater", {}).get("car_pre_10d"), dict)
                                             else -99),
                              reverse=True):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<18} {'ERROR / NO DATA':>8}")
                continue

            overall = r["car_metrics"].get("overall", {})
            beater = r["car_metrics"].get("habitual_beater", {})
            n = overall.get("n_events", 0)
            n_beaters = beater.get("n_events", 0)
            beater_pct = round(n_beaters / n * 100, 1) if n > 0 else 0
            bench_name = r.get("benchmark_name", r.get("benchmark", "?"))

            all_t10 = overall.get("car_pre_10d", {}).get("mean", 0) if isinstance(overall.get("car_pre_10d"), dict) else 0
            beat_t10 = beater.get("car_pre_10d", {}).get("mean", 0) if isinstance(beater.get("car_pre_10d"), dict) else 0
            all_t5 = overall.get("car_pre_5d", {}).get("mean", 0) if isinstance(overall.get("car_pre_5d"), dict) else 0
            t_stat = overall.get("car_pre_10d", {}).get("t_stat", 0) if isinstance(overall.get("car_pre_10d"), dict) else 0
            sig = "**" if abs(t_stat) > 1.96 else ""

            print(f"{uni:<18} {bench_name:>10} {n:>8} {beater_pct:>5.1f}% "
                  f"{all_t10:>+9.3f}% {beat_t10:>+9.3f}% {all_t5:>+9.3f}% "
                  f"{t_stat:>+7.2f} {sig:>4}")

        print("=" * 110)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold,
               verbose=args.verbose, output_path=args.output)


if __name__ == "__main__":
    main()
