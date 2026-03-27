#!/usr/bin/env python3
"""
Post-Earnings Dip Event Study

Event study measuring mean reversion after "sell the news" reactions to earnings beats.
Fetches data via API, caches in DuckDB, runs locally.

Signal:
  1. Company beats earnings estimates (epsActual > epsEstimated)
  2. Stock drops >= 5% from T-1 close to T+1 close (sell-the-news reaction)
  Categories: dip_5 (5-10%), dip_10 (10-20%), dip_20 (20%+)

Measurement:
  Cumulative Abnormal Returns (CAR) at T+5, T+10, T+21, T+63 from T+1 (dip bottom)
  vs. matched benchmark returns over same windows

Academic reference: Bartov, Radhakrishnan & Krinsky (2000) "Investor Sophistication
and Patterns in Stock Returns after Earnings Announcements", The Accounting Review,
75(1), 43-63. Also: Livnat & Mendenhall (2006) "Comparing the Post-Earnings
Announcement Drift for Surprises Calculated from Analyst and Time Series Forecasts",
Journal of Accounting Research, 44(1), 177-205.

Usage:
    # US event study (default)
    python3 post-earnings-dip/backtest.py

    # Specific exchange
    python3 post-earnings-dip/backtest.py --preset india

    # All exchanges
    python3 post-earnings-dip/backtest.py --global --output results/exchange_comparison.json --verbose

    # Custom dip threshold
    python3 post-earnings-dip/backtest.py --min-dip 0.10  # 10%+ drops only

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
from data_utils import query_parquet, get_local_benchmark
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Parameters ---
MIN_ESTIMATE = 0.01       # |epsEstimated| > $0.01 (avoid extreme ratios)
MIN_DIP = 0.05            # Minimum dip: 5% drop from T-1 to T+1 (default)
WINSORIZE_PCT = 1.0       # Winsorize at 1st/99th percentile
WINDOWS = [5, 10, 21, 63] # Trading day windows post-dip (from T+1)
START_YEAR = 2000
END_YEAR = 2025


def fetch_data(client, exchanges, mktcap_min, min_dip=MIN_DIP, verbose=False):
    """Fetch earnings, prices, and compute dip events into DuckDB."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter = "1=1"

    # 1. Fetch earnings beats
    print("  Fetching earnings beats...")
    beat_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS event_date, epsActual, epsEstimated,
               ROUND((epsActual - epsEstimated) / ABS(NULLIF(epsEstimated, 0.0)) * 100.0, 2) AS surprise_pct
        FROM earnings_surprises
        WHERE epsEstimated IS NOT NULL
          AND ABS(epsEstimated) > {MIN_ESTIMATE}
          AND epsActual IS NOT NULL
          AND epsActual > epsEstimated
          AND CAST(date AS DATE) >= '{START_YEAR}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR}-12-31'
          AND {sym_filter}
    """
    count = query_parquet(client, beat_sql, con, "beats_raw",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} earnings beats")
    if count < 100:
        print("  Too few beats. Skipping.")
        con.close()
        return None

    # Deduplicate: one beat per symbol/date
    con.execute("""
        CREATE TABLE beats AS
        SELECT symbol, event_date, epsActual, epsEstimated, surprise_pct
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, event_date
                                         ORDER BY surprise_pct DESC) AS rn
            FROM beats_raw
        ) WHERE rn = 1
    """)

    # 2. Fetch market cap for size filter (FY key_metrics)
    print("  Fetching market cap data...")
    mcap_sql = f"""
        SELECT symbol, dateEpoch AS filing_epoch, marketCap
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND {sym_filter}
    """
    query_parquet(client, mcap_sql, con, "mcap_cache",
                  verbose=verbose, limit=5000000, timeout=600,
                  memory_mb=4096, threads=2)

    # Filter beats by market cap
    con.execute(f"""
        CREATE TABLE beats_mcap AS
        WITH matched AS (
            SELECT b.symbol, b.event_date, b.surprise_pct,
                m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY b.symbol, b.event_date
                                   ORDER BY m.filing_epoch DESC) AS rn
            FROM beats b
            LEFT JOIN mcap_cache m ON b.symbol = m.symbol
                AND m.filing_epoch <= EPOCH(b.event_date)
        )
        SELECT symbol, event_date, surprise_pct
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)

    n_filtered = con.execute("SELECT COUNT(*) FROM beats_mcap").fetchone()[0]
    print(f"    -> {n_filtered} beats after market cap filter")
    if n_filtered < 50:
        print("  Too few events after market cap filter. Skipping.")
        con.close()
        return None

    # 3. Determine benchmark (local currency index for accurate alpha measurement)
    benchmark, benchmark_name = get_local_benchmark(exchanges)

    # 4. Fetch price data for all beat symbols + benchmark
    beat_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM beats_mcap").fetchall()]
    print(f"  Fetching prices for {len(beat_symbols)} symbols + {benchmark_name} ({benchmark})...")
    sym_list = beat_symbols + [benchmark]
    sym_in = ", ".join(f"'{s}'" for s in sym_list)
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{START_YEAR - 1}-01-01'
          AND CAST(date AS DATE) <= '{END_YEAR + 1}-12-31'
          AND adjClose > 0
    """
    p_count = query_parquet(client, price_sql, con, "prices",
                            verbose=verbose, limit=10000000, timeout=600,
                            memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")
    print(f"    -> {p_count} price rows")

    # 5. Build trading day calendar from benchmark
    con.execute(f"""
        CREATE TABLE trading_days AS
        SELECT trade_date,
               ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM prices
        WHERE symbol = '{benchmark}'
        ORDER BY trade_date
    """)
    n_days = con.execute("SELECT COUNT(*) FROM trading_days").fetchone()[0]
    print(f"    -> {n_days} trading days from {benchmark_name} ({benchmark})")

    # 6. Map each beat event to its announcement T0 trading day (ASOF join)
    con.execute("""
        CREATE TABLE events_t0 AS
        SELECT b.symbol, b.event_date, b.surprise_pct,
               td.day_num AS t0_num, td.trade_date AS t0_date
        FROM beats_mcap b
        ASOF JOIN trading_days td ON td.trade_date >= b.event_date
    """)

    # 7. Get T-1 and T+1 prices to measure announcement reaction
    print("  Computing announcement-window returns (T-1 to T+1)...")
    con.execute(f"""
        CREATE TABLE events_with_reaction AS
        SELECT e.symbol, e.event_date, e.surprise_pct,
               e.t0_num, e.t0_date,
               -- T-1: previous trading day before announcement
               p_tm1.adjClose AS price_tm1,
               -- T+1: day after announcement
               p_tp1.adjClose AS price_tp1,
               -- benchmark at same dates
               b_tm1.adjClose AS bench_tm1,
               b_tp1.adjClose AS bench_tp1
        FROM events_t0 e
        -- T-1 price: trading day before T0
        JOIN trading_days td_tm1 ON td_tm1.day_num = e.t0_num - 1
        JOIN prices p_tm1 ON e.symbol = p_tm1.symbol AND td_tm1.trade_date = p_tm1.trade_date
        -- T+1 price: trading day after T0
        JOIN trading_days td_tp1 ON td_tp1.day_num = e.t0_num + 1
        JOIN prices p_tp1 ON e.symbol = p_tp1.symbol AND td_tp1.trade_date = p_tp1.trade_date
        -- Benchmark at same dates
        JOIN prices b_tm1 ON b_tm1.symbol = '{benchmark}' AND td_tm1.trade_date = b_tm1.trade_date
        JOIN prices b_tp1 ON b_tp1.symbol = '{benchmark}' AND td_tp1.trade_date = b_tp1.trade_date
        WHERE p_tm1.adjClose > 0 AND p_tp1.adjClose > 0
    """)

    # Compute announcement return and filter for dips
    print(f"  Filtering for dip >= {min_dip * 100:.0f}%...")
    con.execute(f"""
        CREATE TABLE dip_events AS
        SELECT symbol, event_date, surprise_pct,
               t0_num, t0_date,
               price_tm1, price_tp1, bench_tm1, bench_tp1,
               -- Announcement window return: T-1 to T+1
               ROUND((price_tp1 - price_tm1) / price_tm1, 6) AS reaction_ret,
               ROUND((bench_tp1 - bench_tm1) / bench_tm1, 6) AS bench_reaction_ret,
               -- Abnormal reaction (excess over benchmark)
               ROUND((price_tp1 - price_tm1) / price_tm1
                     - (bench_tp1 - bench_tm1) / bench_tm1, 6) AS abnormal_reaction,
               -- Categorize by dip size
               CASE
                   WHEN (price_tp1 - price_tm1) / price_tm1 <= -0.20 THEN 'dip_20'
                   WHEN (price_tp1 - price_tm1) / price_tm1 <= -0.10 THEN 'dip_10'
                   ELSE 'dip_5'
               END AS category
        FROM events_with_reaction
        WHERE (price_tp1 - price_tm1) / price_tm1 <= -{min_dip}
    """)

    n_dips = con.execute("SELECT COUNT(*) FROM dip_events").fetchone()[0]
    cat_counts = con.execute("""
        SELECT category, COUNT(*) AS n
        FROM dip_events
        GROUP BY category ORDER BY category
    """).fetchall()
    print(f"    -> {n_dips} beat-and-dip events")
    for cat, n in cat_counts:
        label = {"dip_5": "5-10%", "dip_10": "10-20%", "dip_20": "20%+"}.get(cat, cat)
        print(f"       {cat} ({label} drop): {n:,}")

    if n_dips < 30:
        print("  Too few dip events for meaningful analysis. Skipping.")
        con.close()
        return None

    con.execute(f"CREATE TABLE config AS SELECT '{benchmark}' AS benchmark, '{benchmark_name}' AS benchmark_name, {min_dip} AS min_dip")
    return con


def compute_event_returns(con, windows=WINDOWS, verbose=False):
    """Compute CAR at each window for all dip events. Returns start from T+1 (dip bottom)."""
    benchmark = con.execute("SELECT benchmark FROM config").fetchone()[0]
    print("    Mapping events to post-dip trading days (measuring from T+1)...")

    # Get T+1 day_num for each event (dip bottom)
    con.execute("""
        CREATE TABLE event_base AS
        SELECT d.symbol, d.event_date, d.surprise_pct, d.category,
               d.reaction_ret, d.abnormal_reaction,
               -- T+1 is the dip bottom; we measure recovery from here
               td.day_num + 1 AS tp1_num,
               -- Use T+1 prices as base for forward returns
               d.price_tp1 AS stock_base,
               d.bench_tp1 AS bench_base
        FROM dip_events d
        JOIN trading_days td ON td.day_num = d.t0_num
    """)
    n_base = con.execute("SELECT COUNT(*) FROM event_base").fetchone()[0]
    print(f"    -> {n_base} events with T+1 base prices")

    # Compute returns at each forward window from T+1
    for w in windows:
        print(f"    Computing T+{w} returns (from dip bottom)...")
        con.execute(f"""
            CREATE OR REPLACE TABLE window_{w}_returns AS
            SELECT eb.symbol, eb.event_date,
                   ROUND((sp.adjClose - eb.stock_base) / eb.stock_base, 8) AS stock_ret,
                   ROUND((bp.adjClose - eb.bench_base) / eb.bench_base, 8) AS bench_ret,
                   ROUND((sp.adjClose - eb.stock_base) / eb.stock_base
                        - (bp.adjClose - eb.bench_base) / eb.bench_base, 8) AS abnormal_ret
            FROM event_base eb
            JOIN trading_days td ON td.day_num = eb.tp1_num + {w}
            JOIN prices sp ON eb.symbol = sp.symbol AND td.trade_date = sp.trade_date
            JOIN prices bp ON bp.symbol = '{benchmark}' AND td.trade_date = bp.trade_date
        """)
        n_comp = con.execute(f"SELECT COUNT(*) FROM window_{w}_returns").fetchone()[0]
        print(f"      -> {n_comp} events with T+{w} returns")

    # Join all windows
    print("    Joining window results...")
    select_cols = [
        "eb.symbol", "eb.event_date", "eb.surprise_pct", "eb.category",
        "eb.reaction_ret", "eb.abnormal_reaction"
    ]
    join_clauses = []
    for w in windows:
        select_cols.extend([
            f"w{w}.stock_ret AS stock_ret_{w}d",
            f"w{w}.bench_ret AS bench_ret_{w}d",
            f"w{w}.abnormal_ret AS abnormal_ret_{w}d",
        ])
        join_clauses.append(
            f"LEFT JOIN window_{w}_returns w{w} ON eb.symbol = w{w}.symbol "
            f"AND eb.event_date = w{w}.event_date"
        )

    result_sql = f"""
        SELECT {', '.join(select_cols)}
        FROM event_base eb
        {' '.join(join_clauses)}
        WHERE w{windows[0]}.abnormal_ret IS NOT NULL
        ORDER BY eb.event_date
    """
    rows = con.execute(result_sql).fetchall()

    col_names = ["symbol", "event_date", "surprise_pct", "category",
                 "reaction_ret", "abnormal_reaction"]
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

    print(f"    -> {len(results)} events with forward returns")
    return results


def winsorize(values, pct=WINSORIZE_PCT):
    """Winsorize at pct/100-pct percentiles."""
    if len(values) < 10:
        return values
    sv = sorted(values)
    n = len(sv)
    lo = max(0, int(n * pct / 100))
    hi = min(n - 1, int(n * (100 - pct) / 100))
    lo_val, hi_val = sv[lo], sv[hi]
    return [max(lo_val, min(hi_val, v)) for v in values]


def compute_car_stats(values_raw):
    """Compute CAR mean, median, t-stat, hit rate."""
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

    sorted_v = sorted(values_raw)
    mid = len(sorted_v) // 2
    median = (sorted_v[mid] if len(sorted_v) % 2 == 1
              else (sorted_v[mid - 1] + sorted_v[mid]) / 2)
    hit_rate = sum(1 for v in values_raw if v > 0) / len(values_raw)

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
    """Compute CAR metrics by category and overall."""
    categories = ["overall", "dip_5", "dip_10", "dip_20"]
    metrics = {}

    # Also compute reaction return stats
    all_reactions = [r["abnormal_reaction"] for r in results if r.get("abnormal_reaction") is not None]
    if all_reactions:
        metrics["reaction_stats"] = {
            "mean_pct": round(sum(all_reactions) * 100 / len(all_reactions), 4),
            "n": len(all_reactions),
        }

    for cat in categories:
        subset = results if cat == "overall" else [r for r in results if r.get("category") == cat]
        if not subset:
            continue

        cat_data = {"n": len(subset)}
        for w in windows:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in subset if r.get(key) is not None]
            if vals:
                stats = compute_car_stats(vals)
                if stats:
                    cat_data[f"T+{w}"] = stats

        metrics[cat] = cat_data

    return metrics


def compute_yearly_stats(results):
    """Event counts by year and category."""
    by_year = {}
    for r in results:
        year = r["event_date"][:4]
        cat = r.get("category", "unknown")
        if year not in by_year:
            by_year[year] = {"total": 0, "dip_5": 0, "dip_10": 0, "dip_20": 0}
        by_year[year]["total"] += 1
        by_year[year][cat] = by_year[year].get(cat, 0) + 1

    return [
        {
            "year": int(yr),
            "total": by_year[yr]["total"],
            "dip_5": by_year[yr].get("dip_5", 0),
            "dip_10": by_year[yr].get("dip_10", 0),
            "dip_20": by_year[yr].get("dip_20", 0),
        }
        for yr in sorted(by_year.keys())
    ]


def print_results(metrics, universe_name, windows=WINDOWS):
    """Print formatted results."""
    print(f"\n{'=' * 75}")
    print(f"  POST-EARNINGS DIP EVENT STUDY RESULTS: {universe_name}")
    print(f"{'=' * 75}")

    if "reaction_stats" in metrics:
        rs = metrics["reaction_stats"]
        print(f"\n  Announcement reaction (abnormal T-1 to T+1): "
              f"mean {rs['mean_pct']:+.2f}% (n={rs['n']:,})")

    categories = ["overall", "dip_5", "dip_10", "dip_20"]
    labels = {
        "overall": "All Beat+Dip Events",
        "dip_5":   "Dip 5-10%  (moderate sell-off)",
        "dip_10":  "Dip 10-20% (sharp sell-off)",
        "dip_20":  "Dip 20%+   (severe sell-off)",
    }

    for cat in categories:
        section = metrics.get(cat, {})
        n = section.get("n", 0)
        if n == 0:
            continue

        print(f"\n  {labels[cat]} (n={n:,})")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>8}")
        print(f"  {'-' * 55}")

        for w in windows:
            d = section.get(f"T+{w}")
            if d is None:
                continue
            sig = " **" if d.get("significant_1pct") else (" *" if d.get("significant_5pct") else "  ")
            print(f"  T+{w:<7} {d['mean_car']:>+9.3f}% {d['median_car']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

    print(f"{'=' * 75}")


def run_single(cr, exchanges, universe_name, mktcap_min, min_dip=MIN_DIP,
               verbose=False, output_path=None):
    """Run post-earnings dip event study for a single exchange set."""
    mktcap_label = (f"{mktcap_min / 1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min / 1e6:.0f}M")
    signal_desc = (f"EPS beat + stock dip >= {min_dip * 100:.0f}% (T-1 to T+1), "
                   f"MCap > {mktcap_label} local")
    print_header("POST-EARNINGS DIP EVENT STUDY", universe_name, exchanges, signal_desc)
    print(f"  Recovery windows: {', '.join(f'T+{w}' for w in WINDOWS)} from dip bottom (T+1)")
    print(f"  Categories: dip_5 (5-10%), dip_10 (10-20%), dip_20 (20%+)")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    con = fetch_data(cr, exchanges, mktcap_min, min_dip=min_dip, verbose=verbose)
    if con is None:
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    row = con.execute("SELECT benchmark, benchmark_name FROM config").fetchone()
    benchmark = row[0]
    benchmark_name = row[1]
    print(f"  Benchmark: {benchmark_name} ({benchmark})")

    print(f"\nPhase 2: Computing post-dip recovery returns...")
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
        print(f"\n  Yearly Event Counts (beat + dip >= {min_dip * 100:.0f}%):")
        print(f"  {'Year':>6} {'Total':>8} {'Dip5':>8} {'Dip10':>8} {'Dip20':>8}")
        print(f"  {'-' * 44}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total']:>8} {y['dip_5']:>8} "
                  f"{y['dip_10']:>8} {y['dip_20']:>8}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, compute: {compute_time:.0f}s)")

    output = {
        "strategy": "Post-Earnings Dip Mean Reversion",
        "universe": universe_name,
        "benchmark": benchmark,
        "benchmark_name": benchmark_name,
        "study_type": "event_study",
        "period": f"{START_YEAR}-{END_YEAR}",
        "filters": {
            "min_market_cap": mktcap_min,
            "min_estimate": MIN_ESTIMATE,
            "min_dip": min_dip,
        },
        "windows": WINDOWS,
        "car_metrics": metrics,
        "yearly_stats": yearly,
        "n_total_events": len(results),
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
                    exist_ok=True)
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
        description="Post-earnings dip mean reversion event study")
    add_common_args(parser)
    parser.add_argument("--min-dip", type=float, default=MIN_DIP,
                        help=f"Minimum dip threshold (default: {MIN_DIP}, i.e. 5%%)")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("post-earnings-dip", args_str=" ".join(cloud_args),
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
        # Exchanges included: sufficient events (>=200) and data history (>=10yr).
        # Excluded: ASX (136 events, 8yr history), SET (94 events), OSL (85 events),
        #           SIX (81 events) — all too thin for statistically meaningful results.
        # Note on LSE: data sparse pre-2022 (data pipeline issue). Included but content
        #   should caveat limited history.
        # Note on SHZ_SHH (China): beat rate 32-36% vs 41-60% elsewhere. Include but
        #   flag in content.
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("canada",      ["TSX"]),
            ("uk",          ["LSE"]),
            ("japan",       ["JPX"]),
            ("india",       ["NSE"]),
            ("germany",     ["XETRA"]),
            ("taiwan",      ["TAI", "TWO"]),
            ("sweden",      ["STO"]),
            ("korea",       ["KSC"]),
            ("brazil",      ["SAO"]),
            ("hongkong",    ["HKSE"]),
            ("china",       ["SHZ", "SHH"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"post_dip_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            mktcap_threshold = get_mktcap_threshold(preset_exchanges)

            try:
                result = run_single(cr, preset_exchanges, uni_name, mktcap_threshold,
                                    min_dip=args.min_dip, verbose=args.verbose,
                                    output_path=output_path)
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
        print(f"\n\n{'=' * 95}")
        print("POST-EARNINGS DIP: EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 95}")
        print(f"{'Exchange':<22} {'Events':>8} {'T+5 CAR':>9} {'T+21 CAR':>10} "
              f"{'t(21)':>8} {'T+63 CAR':>10} {'t(63)':>8}")
        print("-" * 95)

        for uni, r in sorted(
            all_results.items(),
            key=lambda x: (x[1].get("car_metrics", {}).get("overall", {})
                           .get("T+21", {}).get("mean_car", 0)
                           if isinstance(x[1], dict) and "car_metrics" in x[1] else 0),
            reverse=True
        ):
            if "error" in r or not r.get("car_metrics"):
                print(f"{uni:<22} {'ERROR / NO DATA'}")
                continue

            overall = r["car_metrics"].get("overall", {})
            n = overall.get("n", 0)
            c5 = overall.get("T+5", {}).get("mean_car", 0)
            c21 = overall.get("T+21", {}).get("mean_car", 0)
            t21 = overall.get("T+21", {}).get("t_stat", 0)
            c63 = overall.get("T+63", {}).get("mean_car", 0)
            t63 = overall.get("T+63", {}).get("t_stat", 0)

            sig21 = "**" if abs(t21) > 2.576 else ("*" if abs(t21) > 1.96 else "")
            sig63 = "**" if abs(t63) > 2.576 else ("*" if abs(t63) > 1.96 else "")

            print(f"{uni:<22} {n:>8} {c5:>+8.3f}% {c21:>+9.3f}%{sig21:<2} "
                  f"{t21:>+7.2f} {c63:>+9.3f}%{sig63:<2} {t63:>+7.2f}")

        print("=" * 95)
        return

    # Single exchange mode
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, mktcap_threshold,
               min_dip=args.min_dip, verbose=args.verbose, output_path=args.output)


if __name__ == "__main__":
    main()
