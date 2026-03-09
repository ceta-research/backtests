#!/usr/bin/env python3
"""
M&A Announcement Return Patterns Event Study

Measures cumulative abnormal returns (CAR) for both target and acquirer stocks
following M&A deal announcements. Data sourced from FMP's mergers_acquisitions_latest
table (SEC-sourced deal filings, primarily US companies).

Two event pools analyzed separately:
  - "target" events: target company (targetedSymbol) after deal announced
  - "acquirer" events: acquirer company (symbol) after deal announced

Key data notes:
  - Data contains ~2.9 filings per deal (multiple share classes). Deduplicated by
    (symbol/targetedSymbol, transactionDate) before analysis.
  - No deal price, premium, or terms in source data.
  - transactionDate = SEC filing date (may differ from press announcement date).
  - Coverage selective: not all M&A deals appear; coverage improves post-2015.

Academic reference:
  Mitchell & Pulvino (2001) "Characteristics of Risk and Return in Risk Arbitrage",
  Journal of Finance 56(6), 2135-2175.
  Baker & Savasoglu (2002) "Limited Arbitrage in Mergers and Acquisitions",
  Journal of Financial Economics 64(1), 91-115.

Usage:
    # US event study (default)
    python3 ma-arbitrage/backtest.py

    # With custom market cap filter
    python3 ma-arbitrage/backtest.py --min-mktcap 500000000

    # With output files
    python3 ma-arbitrage/backtest.py --output results/ --verbose

    # Custom date range
    python3 ma-arbitrage/backtest.py --start-year 2010 --end-year 2025

See README.md for strategy details.
"""

import argparse
import duckdb
import json
import math
import os
import sys
import csv
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet
from cli_utils import get_mktcap_threshold

# --- Parameters ---
STRATEGY_NAME = "M&A Announcement Return Patterns"
BENCHMARK = "SPY"
WINDOWS = [1, 5, 21, 63]          # Post-announcement windows (trading days)
WINSORIZE_PCT = 1.0                # Clip at 1st/99th percentile
MAX_RETURN_CAP = 3.0               # Cap extreme returns (data artifacts)
DEFAULT_START_YEAR = 2000
DEFAULT_END_YEAR = 2025
DEFAULT_MKTCAP_MIN = 1_000_000_000  # $1B USD (US-only strategy)


def fetch_data(client, args, verbose=False):
    """Fetch M&A events, market cap data, and prices into DuckDB."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")
    con.execute("SET preserve_insertion_order=false")

    start_year = args.start_year
    end_year = args.end_year
    mktcap_min = args.min_mktcap

    # 1. Fetch M&A events — two event pools (targets and acquirers)
    #    Deduplicate: one event per (targetedSymbol, date) for targets
    #                 one event per (symbol, date) for acquirers
    print("  Fetching M&A events...")
    # Exclude warrants (symbols ending in W/WS/-WT/-WS) — these are leveraged
    # instruments with extreme price swings unrelated to the underlying M&A event.
    # Also excludes SPAC-related warrant instruments that appear in the 2020-21 boom.
    ma_sql = f"""
        SELECT
            symbol,
            targetedSymbol,
            CAST(transactionDate AS DATE) AS deal_date
        FROM mergers_acquisitions_latest
        WHERE CAST(transactionDate AS DATE) >= '{start_year}-01-01'
          AND CAST(transactionDate AS DATE) <= '{end_year}-12-31'
          AND symbol IS NOT NULL AND TRIM(symbol) != ''
          AND NOT (symbol LIKE '%-WT' OR symbol LIKE '%-WS'
                   OR (symbol LIKE '%W' AND LENGTH(symbol) > 5))
    """
    n_rows = query_parquet(client, ma_sql, con, "raw_ma",
                           verbose=verbose, limit=50000, timeout=300,
                           memory_mb=4096, threads=2)
    print(f"    -> {n_rows} raw M&A rows ({start_year}-{end_year})")
    if n_rows == 0:
        print("  No M&A data. Exiting.")
        con.close()
        return None

    # Build deduplicated target and acquirer event lists
    # Targets: targetedSymbol IS NOT NULL, deduplicate by (targetedSymbol, deal_date)
    con.execute("""
        CREATE TABLE target_events AS
        SELECT targetedSymbol AS symbol, deal_date, 'target' AS role
        FROM raw_ma
        WHERE targetedSymbol IS NOT NULL AND TRIM(targetedSymbol) != ''
        GROUP BY targetedSymbol, deal_date
    """)
    n_targets_raw = con.execute("SELECT COUNT(*) FROM target_events").fetchone()[0]

    # Acquirers: deduplicate by (symbol, deal_date)
    con.execute("""
        CREATE TABLE acquirer_events AS
        SELECT symbol, deal_date, 'acquirer' AS role
        FROM raw_ma
        GROUP BY symbol, deal_date
    """)
    n_acquirers_raw = con.execute("SELECT COUNT(*) FROM acquirer_events").fetchone()[0]
    print(f"    -> {n_targets_raw} deduplicated target events, {n_acquirers_raw} acquirer events")

    # Combine into single events table
    con.execute("""
        CREATE TABLE all_events AS
        SELECT symbol, deal_date, role FROM target_events
        UNION ALL
        SELECT symbol, deal_date, role FROM acquirer_events
    """)
    con.execute("DROP TABLE target_events")
    con.execute("DROP TABLE acquirer_events")

    n_events_total = con.execute("SELECT COUNT(*) FROM all_events").fetchone()[0]
    print(f"    -> {n_events_total} total events before market cap filter")

    # 2. Fetch market cap data for filtering (most recent FY before event)
    print("  Fetching market cap data...")
    mcap_sql = """
        SELECT symbol, CAST(date AS DATE) AS filing_date, marketCap
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL AND marketCap > 0
    """
    n_mcap = query_parquet(client, mcap_sql, con, "mcap_cache",
                           verbose=verbose, limit=5000000, timeout=300,
                           memory_mb=4096, threads=2)
    print(f"    -> {n_mcap} market cap rows")

    # 3. Apply market cap filter
    print("  Applying market cap filter...")
    con.execute(f"""
        CREATE TABLE events AS
        WITH matched AS (
            SELECT e.symbol, e.deal_date, e.role, m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY e.symbol, e.deal_date
                                   ORDER BY m.filing_date DESC) AS rn
            FROM all_events e
            LEFT JOIN mcap_cache m ON e.symbol = m.symbol
                AND m.filing_date <= e.deal_date
        )
        SELECT symbol, deal_date, role
        FROM matched
        WHERE rn = 1
          AND (marketCap IS NULL OR marketCap > {mktcap_min})
    """)
    con.execute("DROP TABLE all_events")

    n_filtered = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    n_tgt = con.execute("SELECT COUNT(*) FROM events WHERE role = 'target'").fetchone()[0]
    n_acq = con.execute("SELECT COUNT(*) FROM events WHERE role = 'acquirer'").fetchone()[0]
    print(f"    -> {n_filtered} events after mktcap > ${mktcap_min/1e9:.1f}B filter "
          f"({n_tgt} targets, {n_acq} acquirers)")

    if n_filtered < 20:
        print("  Too few events for meaningful analysis. Exiting.")
        con.close()
        return None

    # 4. Fetch price data for all event symbols + benchmark (SPY)
    event_symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM events").fetchall()]
    print(f"  Fetching prices for {len(event_symbols)} symbols + {BENCHMARK}...")

    sym_list = event_symbols + [BENCHMARK]
    sym_in = ", ".join(f"'{s}'" for s in sym_list)
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{start_year - 1}-01-01'
          AND CAST(date AS DATE) <= '{end_year + 1}-12-31'
          AND adjClose > 0
    """
    n_prices = query_parquet(client, price_sql, con, "prices",
                             verbose=verbose, limit=20000000, timeout=600,
                             memory_mb=4096, threads=2)
    print(f"    -> {n_prices} price rows")

    if n_prices == 0:
        print("  No price data. Exiting.")
        con.close()
        return None

    con.execute("CREATE INDEX idx_prices ON prices(symbol, trade_date)")

    # 5. Build trading day calendar from SPY
    con.execute(f"""
        CREATE TABLE trading_days AS
        SELECT trade_date,
            ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM prices
        WHERE symbol = '{BENCHMARK}'
        ORDER BY trade_date
    """)
    n_days = con.execute("SELECT COUNT(*) FROM trading_days").fetchone()[0]
    print(f"    -> {n_days} trading days from {BENCHMARK}")

    if n_days < 100:
        print(f"  Insufficient price data for {BENCHMARK}. Exiting.")
        con.close()
        return None

    return con


def compute_event_returns(con, verbose=False):
    """Compute abnormal returns at each window for all events."""
    windows = WINDOWS

    # Map each event to T+0 trading day (first trading day on or after deal_date)
    print("    Mapping events to trading days...")
    con.execute("""
        CREATE TABLE event_t0 AS
        SELECT e.symbol, e.deal_date, e.role,
            td.day_num AS t0_num, td.trade_date AS t0_date
        FROM events e
        ASOF JOIN trading_days td ON td.trade_date >= e.deal_date
    """)
    n_mapped = con.execute("SELECT COUNT(*) FROM event_t0").fetchone()[0]
    print(f"    -> {n_mapped} events mapped to trading days")

    # Get T+0 prices for stock and benchmark
    print("    Getting T+0 prices...")
    con.execute(f"""
        CREATE TABLE event_base AS
        SELECT et.symbol, et.deal_date, et.role,
            et.t0_num, et.t0_date,
            sp.adjClose AS stock_t0, bp.adjClose AS bench_t0
        FROM event_t0 et
        JOIN prices sp ON et.symbol = sp.symbol AND et.t0_date = sp.trade_date
        JOIN prices bp ON bp.symbol = '{BENCHMARK}' AND et.t0_date = bp.trade_date
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
            SELECT eb.symbol, eb.deal_date,
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0, 8) AS stock_ret,
                ROUND((bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS bench_ret,
                ROUND((sp.adjClose - eb.stock_t0) / eb.stock_t0
                     - (bp.adjClose - eb.bench_t0) / eb.bench_t0, 8) AS abnormal_ret
            FROM event_base eb
            JOIN trading_days td ON td.day_num = eb.t0_num + {w}
            JOIN prices sp ON eb.symbol = sp.symbol AND td.trade_date = sp.trade_date
            JOIN prices bp ON bp.symbol = '{BENCHMARK}' AND td.trade_date = bp.trade_date
        """)
        n_w = con.execute(f"SELECT COUNT(*) FROM window_{w}_returns").fetchone()[0]
        print(f"      -> {n_w} events with T+{w} returns")

    # Join all windows
    print("    Joining window results...")
    select_cols = ["eb.symbol", "eb.deal_date", "eb.role"]
    join_clauses = []
    for w in windows:
        select_cols.extend([
            f"w{w}.stock_ret AS stock_ret_{w}d",
            f"w{w}.bench_ret AS bench_ret_{w}d",
            f"w{w}.abnormal_ret AS abnormal_ret_{w}d",
        ])
        join_clauses.append(
            f"LEFT JOIN window_{w}_returns w{w} ON eb.symbol = w{w}.symbol "
            f"AND eb.deal_date = w{w}.deal_date"
        )

    result_sql = f"""
        SELECT {', '.join(select_cols)}
        FROM event_base eb
        {' '.join(join_clauses)}
        WHERE w1.abnormal_ret IS NOT NULL
        ORDER BY eb.deal_date
    """
    rows = con.execute(result_sql).fetchall()
    col_names = ["symbol", "deal_date", "role"]
    for w in windows:
        col_names.extend([f"stock_ret_{w}d", f"bench_ret_{w}d", f"abnormal_ret_{w}d"])

    results = []
    for row in rows:
        r = {}
        for i, col in enumerate(col_names):
            val = row[i]
            if col == "deal_date":
                r[col] = val.isoformat() if isinstance(val, date) else str(val)
            elif isinstance(val, float):
                r[col] = round(val, 6)
            else:
                r[col] = val
        results.append(r)

    skipped = n_mapped - len(results)
    print(f"    -> {len(results)} events with returns, {skipped} skipped (no price data)")

    for w in windows:
        con.execute(f"DROP TABLE IF EXISTS window_{w}_returns")

    return results


def winsorize(values, pct=WINSORIZE_PCT):
    """Clip extreme values at pct/100-pct percentiles."""
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    n = len(sorted_v)
    lo_idx = max(0, int(n * pct / 100))
    hi_idx = min(n - 1, int(n * (100 - pct) / 100))
    lo_val, hi_val = sorted_v[lo_idx], sorted_v[hi_idx]
    return [max(lo_val, min(hi_val, v)) for v in values]


def compute_car_stats(raw_values):
    """Compute CAR statistics: mean, median, t-stat, hit rate."""
    if not raw_values:
        return None
    values = winsorize(raw_values)
    mean_car = sum(values) / len(values)
    if len(values) > 1:
        var = sum((v - mean_car) ** 2 for v in values) / (len(values) - 1)
        std = math.sqrt(var) if var > 0 else 0
        se = std / math.sqrt(len(values))
        t_stat = mean_car / se if se > 0 else 0
    else:
        t_stat = 0
    hit_rate = sum(1 for v in raw_values if v > 0) / len(raw_values)
    sorted_vals = sorted(raw_values)
    mid = len(sorted_vals) // 2
    median = (sorted_vals[mid] if len(sorted_vals) % 2 == 1
              else (sorted_vals[mid-1] + sorted_vals[mid]) / 2)
    return {
        "mean": round(mean_car * 100, 4),
        "median": round(median * 100, 4),
        "t_stat": round(t_stat, 3),
        "significant": abs(t_stat) > 1.96,
        "hit_rate": round(hit_rate * 100, 2),
        "n_obs": len(raw_values),
    }


def compute_metrics(results):
    """Compute CAR metrics by role (target/acquirer) and overall."""
    metrics = {}
    for label, filter_fn in [
        ("overall", lambda r: True),
        ("target", lambda r: r["role"] == "target"),
        ("acquirer", lambda r: r["role"] == "acquirer"),
    ]:
        subset = [r for r in results if filter_fn(r)]
        if not subset:
            continue
        metrics[label] = {"n_events": len(subset)}
        for w in WINDOWS:
            key = f"abnormal_ret_{w}d"
            vals = [r[key] for r in subset if r.get(key) is not None]
            stats = compute_car_stats(vals)
            if stats:
                metrics[label][f"car_{w}d"] = stats
    return metrics


def compute_yearly_stats(results):
    """Count events and summarize by year."""
    by_year = {}
    for r in results:
        year = r["deal_date"][:4]
        role = r["role"]
        if year not in by_year:
            by_year[year] = {"total": 0, "target": 0, "acquirer": 0}
        by_year[year]["total"] += 1
        by_year[year][role] = by_year[year].get(role, 0) + 1

    return [
        {"year": int(y), **by_year[y]}
        for y in sorted(by_year)
    ]


def print_results(metrics, universe_name):
    """Print formatted summary to stdout."""
    print(f"\n{'=' * 70}")
    print(f"  M&A RETURN PATTERNS: {universe_name}")
    print(f"{'=' * 70}")

    label_map = {
        "overall": "All Events (Combined)",
        "target": "Target Companies (targetedSymbol)",
        "acquirer": "Acquirer Companies (symbol)",
    }

    for label in ["overall", "target", "acquirer"]:
        section = metrics.get(label, {})
        n = section.get("n_events", 0)
        if n == 0:
            continue
        print(f"\n  {label_map[label]} (n={n:,})")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>8}")
        print(f"  {'-' * 53}")
        for w in WINDOWS:
            d = section.get(f"car_{w}d")
            if d is None:
                continue
            sig = " **" if d["significant"] else ""
            print(f"  T+{w:<7} {d['mean']:>+9.3f}% {d['median']:>+9.3f}% "
                  f"{d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>7.1f}%")

    print(f"{'=' * 70}")


def build_output(metrics, yearly, results, args):
    """Build structured JSON output."""
    n_target = sum(1 for r in results if r["role"] == "target")
    n_acquirer = sum(1 for r in results if r["role"] == "acquirer")
    return {
        "strategy": STRATEGY_NAME,
        "data_source": "mergers_acquisitions_latest (FMP / SEC filings)",
        "universe": "US (NYSE/NASDAQ/AMEX)",
        "benchmark": BENCHMARK,
        "study_type": "event_study",
        "period": f"{args.start_year}-{args.end_year}",
        "filters": {
            "min_market_cap_usd": args.min_mktcap,
            "deduplication": "one event per (symbol, transactionDate)",
        },
        "windows": WINDOWS,
        "car_metrics": metrics,
        "yearly_stats": yearly,
        "n_total_events": len(results),
        "n_target_events": n_target,
        "n_acquirer_events": n_acquirer,
        "data_notes": [
            "transactionDate = SEC filing date, may differ from press announcement date",
            "No deal price, premium, or deal type in source data",
            "Coverage selective: not all public M&A deals are captured",
            "Targets with no US price data (private/foreign) excluded by default",
        ],
    }


def run(args):
    """Run the full M&A event study."""
    print(f"\n{'=' * 65}")
    print(f"  {STRATEGY_NAME}")
    print(f"  Period: {args.start_year}-{args.end_year}")
    print(f"  Market cap filter: > ${args.min_mktcap/1e9:.1f}B USD")
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print(f"{'=' * 65}")

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print("\nPhase 1: Fetching data...")
    t0 = time.time()
    con = fetch_data(cr, args, verbose=args.verbose)
    if con is None:
        return None
    print(f"\nData fetched in {time.time() - t0:.0f}s")

    print("\nPhase 2: Computing event-window returns...")
    t1 = time.time()
    results = compute_event_returns(con, verbose=args.verbose)
    print(f"Returns computed in {time.time() - t1:.0f}s")

    if not results:
        print("No valid event returns. Exiting.")
        con.close()
        return None

    print("\nPhase 3: Computing CAR metrics...")
    metrics = compute_metrics(results)
    yearly = compute_yearly_stats(results)

    print_results(metrics, "US (NYSE/NASDAQ/AMEX)")

    if args.verbose:
        print(f"\n  Yearly Event Counts:")
        print(f"  {'Year':>6} {'Total':>8} {'Target':>8} {'Acquirer':>8}")
        print(f"  {'-' * 34}")
        for y in yearly:
            print(f"  {y['year']:>6} {y['total']:>8} {y.get('target',0):>8} {y.get('acquirer',0):>8}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s")

    output = build_output(metrics, yearly, results, args)

    if args.output:
        os.makedirs(args.output, exist_ok=True)
        json_path = os.path.join(args.output, "summary_metrics.json")
        with open(json_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {json_path}")

        # Save event-level CSV
        csv_path = os.path.join(args.output, "event_returns.csv")
        if results:
            headers = list(results[0].keys())
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(results)
            print(f"  Events saved to {csv_path}")

        # Save yearly CSV
        yearly_path = os.path.join(args.output, "event_frequency.csv")
        if yearly:
            with open(yearly_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=yearly[0].keys())
                writer.writeheader()
                writer.writerows(yearly)
            print(f"  Yearly stats saved to {yearly_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description=f"{STRATEGY_NAME} event study")
    parser.add_argument("--api-key", default=os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY"),
                        help="Ceta Research API key")
    parser.add_argument("--base-url", default="https://api.cetaresearch.com/api/v1",
                        help="API base URL")
    parser.add_argument("--output", default="ma-arbitrage/results",
                        help="Output directory for results")
    parser.add_argument("--min-mktcap", type=float, default=DEFAULT_MKTCAP_MIN,
                        help=f"Minimum market cap (default: ${DEFAULT_MKTCAP_MIN/1e9:.0f}B)")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR,
                        help=f"Start year (default: {DEFAULT_START_YEAR})")
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR,
                        help=f"End year (default: {DEFAULT_END_YEAR})")
    parser.add_argument("--verbose", action="store_true",
                        help="Print detailed progress")
    args = parser.parse_args()

    if not args.api_key:
        print("Error: API key required. Set CR_API_KEY env var or use --api-key.")
        sys.exit(1)

    run(args)


if __name__ == "__main__":
    main()
