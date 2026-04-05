#!/usr/bin/env python3
"""
Corporate Spinoff Event Study

Measures cumulative abnormal returns (CAR) for both spinoff parents and children
across multiple time horizons post-spinoff.

Event windows: T+1, T+5, T+21, T+63, T+126, T+252 trading days
Categories: parent, child, all
Benchmark: SPY

Academic basis:
- Cusatis, Miles & Woolridge (1993): Both parent and child outperform ~25% over 3 years
- McConnell & Ovtchinnikov (2004): Effect concentrated in first 2 years
- Greenblatt (1997): "You Can Be a Stock Market Genius"

Mechanism: Index funds receive spinoff shares too small for their benchmark,
creating forced selling. Both parent and child benefit from improved focus
(conglomerate discount removal).

Data note: No spinoff table exists in FMP. Events use a curated list of
confirmed US corporate spinoffs from public filings and SEC records.

Usage:
    python3 spinoff/backtest.py
    python3 spinoff/backtest.py --output spinoff/results/ --verbose
    python3 spinoff/backtest.py --start-year 2015 --verbose

Data source: Ceta Research SQL API (FMP price warehouse)
Requires: CR_API_KEY environment variable
"""

import argparse
import csv
import duckdb
import json
import math
import os
import sys
import time
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet

# --- Event windows (trading days post-spinoff) ---
WINDOWS = [1, 5, 21, 63, 126, 252]

# --- Curated spinoff list ---
# (parent_symbol, child_symbol, spinoff_date, description)
# spinoff_date = first trading day of child (or effective date for parent-only entries)
# Sources: SEC Form 10-12 filings, press releases, Bloomberg, S&P Capital IQ
SPINOFFS = [
    # parent    child    date          description
    ("MSI",  "MMI",   "2011-01-04", "Motorola Solutions / Motorola Mobility"),
    ("EXPE", "TRIP",  "2011-12-21", "Expedia / TripAdvisor"),
    ("COP",  "PSX",   "2012-05-01", "ConocoPhillips / Phillips 66"),
    ("MPC",  "MPLX",  "2012-10-31", "Marathon Petroleum / MPLX LP"),
    ("ABT",  "ABBV",  "2013-01-02", "Abbott Labs / AbbVie"),
    ("PFE",  "ZTS",   "2013-02-01", "Pfizer / Zoetis"),
    ("GE",   "SYF",   "2014-07-31", "GE Capital / Synchrony Financial"),
    ("ADP",  "CDK",   "2014-10-01", "ADP / CDK Global"),
    ("EBAY", "PYPL",  "2015-07-20", "eBay / PayPal"),
    ("HPQ",  "HPE",   "2015-11-02", "HP Inc / HP Enterprise"),
    ("YUM",  "YUMC",  "2016-11-01", "Yum Brands / Yum China"),
    ("XRX",  "CNDT",  "2017-01-03", "Xerox / Conduent"),
    ("MET",  "BHF",   "2017-08-07", "MetLife / Brighthouse Financial"),
    ("PNR",  "NVT",   "2018-04-30", "Pentair / nVent Electric"),
    ("TNL",  "WH",    "2018-06-01", "Wyndham Worldwide (now TNL) / Wyndham Hotels"),
    ("HON",  "GTX",   "2018-10-04", "Honeywell / Garrett Motion"),
    ("HON",  "REZI",  "2018-11-01", "Honeywell / Resideo Technologies"),
    ("DD",   "DOW",   "2019-04-01", "DowDuPont / Dow Inc"),
    ("DD",   "CTVA",  "2019-06-03", "DowDuPont / Corteva Agriscience"),
    ("DHR",  "NVST",  "2019-09-20", "Danaher / Envista Holdings"),
    ("RTX",  "OTIS",  "2020-04-03", "Raytheon Technologies / Otis Worldwide"),
    ("RTX",  "CARR",  "2020-04-03", "Raytheon Technologies / Carrier Global"),
    ("FTV",  "VNT",   "2020-10-09", "Fortive / Vontier"),
    ("PFE",  "VTRS",  "2020-11-16", "Pfizer / Viatris"),
    ("IBM",  "KD",    "2021-11-04", "IBM / Kyndryl"),
    ("GSK",  "HLN",   "2022-07-18", "GSK / Haleon"),
    ("GE",   "GEHC",  "2023-01-04", "GE / GE Healthcare"),
    ("JNJ",  "KVUE",  "2023-05-04", "J&J / Kenvue"),
    ("MMM",  "SOLV",  "2024-04-01", "3M / Solventum"),
    ("GE",   "GEV",   "2024-04-02", "GE / GE Vernova"),
]

# Winsorize at this percentile to handle extreme single-event returns
WINSORIZE_PCT = 1.0


def build_event_list(start_year=None, end_year=None):
    """Convert SPINOFFS list to flat event rows, deduplicating parent events on same date."""
    events = []
    seen_parent_dates = set()  # Deduplicate parent events (e.g. RTX on 2020-04-03)

    for parent, child, spinoff_date, description in SPINOFFS:
        yr = int(spinoff_date[:4])
        if start_year and yr < start_year:
            continue
        if end_year and yr > end_year:
            continue

        # Parent event (deduplicated by symbol+date)
        parent_key = (parent, spinoff_date)
        if parent_key not in seen_parent_dates:
            events.append({
                "symbol": parent,
                "event_date": spinoff_date,
                "category": "parent",
                "description": description,
            })
            seen_parent_dates.add(parent_key)

        # Child event
        if child:
            events.append({
                "symbol": child,
                "event_date": spinoff_date,
                "category": "child",
                "description": description,
            })

    return events


def fetch_data(client, events, verbose=False):
    """Fetch price data for all event symbols + SPY. Returns DuckDB connection."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads TO 2")

    # Collect all unique symbols
    all_symbols = list({e["symbol"] for e in events} | {"SPY"})
    sym_in = ", ".join(f"'{s}'" for s in all_symbols)

    # Determine date range
    min_date = min(e["event_date"] for e in events)
    min_year = int(min_date[:4]) - 1  # Fetch 1 year before earliest event

    print(f"  Fetching prices for {len(all_symbols)} symbols (2010 onward)...")
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose, volume
        FROM stock_eod
        WHERE symbol IN ({sym_in})
          AND CAST(date AS DATE) >= '{min_year}-01-01'
          AND CAST(date AS DATE) <= '2026-12-31'
          AND adjClose IS NOT NULL
          AND adjClose > 0
    """
    n = query_parquet(client, price_sql, con, "prices",
                      verbose=verbose, limit=5000000, timeout=600,
                      memory_mb=4096, threads=2)
    print(f"    -> {n} price records for {len(all_symbols)} symbols")

    if n == 0:
        print("  No price data returned. Check API key and network.")
        con.close()
        return None

    # Build trading day calendar from SPY
    spy_count = con.execute("SELECT COUNT(*) FROM prices WHERE symbol = 'SPY'").fetchone()[0]
    if spy_count == 0:
        print("  ERROR: No SPY data found. Cannot build trading day calendar.")
        con.close()
        return None

    con.execute("""
        CREATE TABLE trading_days AS
        SELECT trade_date,
               ROW_NUMBER() OVER (ORDER BY trade_date) AS day_num
        FROM prices
        WHERE symbol = 'SPY'
        ORDER BY trade_date
    """)
    n_days = con.execute("SELECT COUNT(*) FROM trading_days").fetchone()[0]
    print(f"    -> {n_days} trading days from SPY")

    # Check symbol coverage
    found_syms = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM prices").fetchall()}
    missing = [s for s in all_symbols if s not in found_syms]
    if missing:
        print(f"    -> Missing symbols (no price data): {', '.join(missing)}")

    return con, found_syms


def compute_event_returns(con, events, found_symbols, windows=WINDOWS,
                          entry_offset=0, verbose=False):
    """Compute CAR vs SPY for each event at each window.

    Args:
        entry_offset: Trading days after event date to use as entry price.
            0 = event-date close (traditional event study).
            1 = next-day close (MOC execution, more realistic for trading).
    """

    results = []

    for ev in events:
        symbol = ev["symbol"]
        event_date = ev["event_date"]
        category = ev["category"]
        description = ev["description"]

        if symbol not in found_symbols:
            if verbose:
                print(f"    Skipping {symbol} ({event_date}): no price data")
            continue

        # Find T0: first trading day on or after event_date
        t0_row = con.execute(f"""
            SELECT day_num, trade_date
            FROM trading_days
            WHERE trade_date >= '{event_date}'
            ORDER BY trade_date
            LIMIT 1
        """).fetchone()

        if not t0_row:
            if verbose:
                print(f"    Skipping {symbol} ({event_date}): event date after all trading days")
            continue

        t0_num, t0_date = t0_row

        # Entry price: T0 + entry_offset trading days
        entry_num = t0_num + entry_offset
        if entry_offset > 0:
            entry_row = con.execute(f"""
                SELECT trade_date FROM trading_days WHERE day_num = {entry_num}
            """).fetchone()
            if not entry_row:
                if verbose:
                    print(f"    Skipping {symbol} ({event_date}): no entry date at T+{entry_offset}")
                continue
            entry_date = entry_row[0]
        else:
            entry_date = t0_date

        # Get entry prices for stock and SPY
        entry_prices = con.execute(f"""
            SELECT
                (SELECT adjClose FROM prices WHERE symbol = '{symbol}' AND trade_date = '{entry_date}') AS stock_entry,
                (SELECT adjClose FROM prices WHERE symbol = 'SPY' AND trade_date = '{entry_date}') AS spy_entry
        """).fetchone()

        if not entry_prices or entry_prices[0] is None or entry_prices[1] is None:
            if verbose:
                print(f"    Skipping {symbol} ({event_date}): no entry price on {entry_date}")
            continue

        stock_entry, spy_entry = entry_prices

        result = {
            "symbol": symbol,
            "event_date": event_date,
            "t0_date": str(t0_date),
            "entry_date": str(entry_date),
            "category": category,
            "description": description,
        }

        for w in windows:
            # Find T+W trading day (from event date T0, not entry)
            tw_row = con.execute(f"""
                SELECT trade_date
                FROM trading_days
                WHERE day_num = {t0_num} + {w}
            """).fetchone()

            if not tw_row:
                result[f"ar_{w}"] = None
                result[f"stock_ret_{w}"] = None
                result[f"spy_ret_{w}"] = None
                continue

            tw_date = tw_row[0]

            # Get prices at T+W
            tw_prices = con.execute(f"""
                SELECT
                    (SELECT adjClose FROM prices WHERE symbol = '{symbol}' AND trade_date = '{tw_date}') AS stock_tw,
                    (SELECT adjClose FROM prices WHERE symbol = 'SPY' AND trade_date = '{tw_date}') AS spy_tw
            """).fetchone()

            if not tw_prices or tw_prices[0] is None or tw_prices[1] is None:
                result[f"ar_{w}"] = None
                result[f"stock_ret_{w}"] = None
                result[f"spy_ret_{w}"] = None
                continue

            stock_tw, spy_tw = tw_prices
            stock_ret = (stock_tw - stock_entry) / stock_entry * 100
            spy_ret = (spy_tw - spy_entry) / spy_entry * 100
            ar = stock_ret - spy_ret

            result[f"ar_{w}"] = round(ar, 4)
            result[f"stock_ret_{w}"] = round(stock_ret, 4)
            result[f"spy_ret_{w}"] = round(spy_ret, 4)

        results.append(result)

    return results


def winsorize(values, pct=WINSORIZE_PCT):
    """Winsorize at pct/100 and (1-pct/100) percentiles."""
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    n = len(sorted_v)
    lo = max(0, int(n * pct / 100))
    hi = min(n - 1, int(n * (100 - pct) / 100))
    lo_val, hi_val = sorted_v[lo], sorted_v[hi]
    return [max(lo_val, min(hi_val, v)) for v in values]


def car_stats(values_raw):
    """Compute CAR statistics for a set of abnormal return values."""
    if not values_raw:
        return None
    values = winsorize(values_raw)
    n = len(values)
    mean_v = sum(values) / n
    sorted_raw = sorted(values_raw)
    mid = n // 2
    median_v = sorted_raw[mid] if n % 2 == 1 else (sorted_raw[mid - 1] + sorted_raw[mid]) / 2

    t_stat = 0.0
    if n > 1:
        var = sum((v - mean_v) ** 2 for v in values) / (n - 1)
        std = math.sqrt(var) if var > 0 else 0.0
        se = std / math.sqrt(n) if n > 0 else 0.0
        t_stat = mean_v / se if se > 0 else 0.0

    hit_rate = sum(1 for v in values_raw if v > 0) / len(values_raw) * 100

    return {
        "mean_car": round(mean_v, 3),
        "median_car": round(sorted_raw[mid] if n % 2 == 1 else (sorted_raw[mid - 1] + sorted_raw[mid]) / 2, 3),
        "t_stat": round(t_stat, 2),
        "n": n,
        "hit_rate": round(hit_rate, 1),
        "significant_5pct": abs(t_stat) > 1.96,
        "significant_1pct": abs(t_stat) > 2.576,
    }


def compute_metrics(results, windows=WINDOWS):
    """Aggregate CAR metrics by category (parent, child, all) and window."""
    categories = ["all", "parent", "child"]
    metrics = {}

    for cat in categories:
        subset = results if cat == "all" else [r for r in results if r["category"] == cat]
        n = len(subset)
        if n == 0:
            continue

        metrics[cat] = {"n": n, "windows": {}}
        for w in windows:
            key = f"ar_{w}"
            raw = [r[key] for r in subset if r.get(key) is not None]
            if raw:
                stats = car_stats(raw)
                if stats:
                    metrics[cat]["windows"][f"T+{w}"] = stats

    return metrics


def print_results(metrics):
    """Print formatted CAR results table."""
    cats = ["parent", "child", "all"]
    labels = {
        "parent": "PARENT (retained company)",
        "child": "CHILD (spun-off entity)",
        "all": "ALL EVENTS (combined)",
    }

    for cat in cats:
        section = metrics.get(cat)
        if not section:
            continue
        n = section["n"]
        print(f"\n  {labels[cat]}  (n={n})")
        print(f"  {'Window':<10} {'Mean CAR':>10} {'Median':>10} {'t-stat':>8} {'Sig':>5} {'Hit%':>7} {'N':>5}")
        print(f"  {'-' * 57}")
        for w in WINDOWS:
            d = section["windows"].get(f"T+{w}")
            if not d:
                continue
            sig = "**" if d["significant_1pct"] else ("*" if d["significant_5pct"] else "")
            print(
                f"  T+{w:<7} {d['mean_car']:>+9.2f}% {d['median_car']:>+9.2f}%"
                f" {d['t_stat']:>+7.2f} {sig:>5} {d['hit_rate']:>6.1f}% {d['n']:>5}"
            )

    print(f"\n  * p<0.05  ** p<0.01")


def save_results(results, metrics, output_dir, execution_model="Event-date close (T+0)"):
    """Save results to output_dir."""
    os.makedirs(output_dir, exist_ok=True)

    # --- summary_metrics.json ---
    summary = {
        "strategy": "Corporate Spinoff Event Study",
        "benchmark": "SPY",
        "execution": execution_model,
        "n_spinoffs": len(SPINOFFS),
        "windows": WINDOWS,
        "methodology": "Curated list of major US corporate spinoffs, 2011-2024",
        "categories": metrics,
    }
    summary_path = os.path.join(output_dir, "summary_metrics.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved {summary_path}")

    # --- individual_spinoffs.csv ---
    csv_path = os.path.join(output_dir, "individual_spinoffs.csv")
    fieldnames = ["symbol", "event_date", "t0_date", "entry_date", "category", "description"]
    for w in WINDOWS:
        fieldnames.extend([f"ar_{w}", f"stock_ret_{w}", f"spy_ret_{w}"])

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in sorted(results, key=lambda x: (x["event_date"], x["category"])):
            writer.writerow(r)
    print(f"  Saved {csv_path}")

    # --- parent_vs_child.json (legacy format for content compatibility) ---
    pvc = {}
    for w in WINDOWS:
        pvc[f"T+{w}"] = {}
        for cat in ["parent", "child"]:
            section = metrics.get(cat, {})
            win_data = section.get("windows", {}).get(f"T+{w}", {})
            if win_data:
                pvc[f"T+{w}"][cat] = {
                    "mean": win_data["mean_car"],
                    "median": win_data["median_car"],
                    "t_stat": win_data["t_stat"],
                    "n": win_data["n"],
                    "hit_rate": win_data["hit_rate"],
                }
    pvc_path = os.path.join(output_dir, "parent_vs_child.json")
    with open(pvc_path, "w") as f:
        json.dump(pvc, f, indent=2)
    print(f"  Saved {pvc_path}")

    return summary_path, csv_path


def print_top_spinoffs(results):
    """Print best and worst spinoff children at T+252."""
    child_results = [r for r in results if r["category"] == "child" and r.get("ar_252") is not None]
    if not child_results:
        return

    child_results.sort(key=lambda x: x["ar_252"], reverse=True)
    print(f"\n  Top 5 spinoff children (T+252 CAR):")
    print(f"  {'Symbol':<8} {'CAR+252':>10}  Description")
    print(f"  {'-' * 55}")
    for r in child_results[:5]:
        print(f"  {r['symbol']:<8} {r['ar_252']:>+9.1f}%  {r['description'][:40]}")

    print(f"\n  Bottom 5 spinoff children (T+252 CAR):")
    print(f"  {'Symbol':<8} {'CAR+252':>10}  Description")
    print(f"  {'-' * 55}")
    for r in child_results[-5:]:
        print(f"  {r['symbol']:<8} {r['ar_252']:>+9.1f}%  {r['description'][:40]}")


def main():
    parser = argparse.ArgumentParser(description="Corporate Spinoff Event Study")
    parser.add_argument("--api-key", type=str, help="Ceta Research API key (or set CR_API_KEY)")
    parser.add_argument("--base-url", type=str, help="API base URL (optional)")
    parser.add_argument("--output", type=str, default="spinoff/results",
                        help="Output directory (default: spinoff/results)")
    parser.add_argument("--start-year", type=int, default=None, help="Include spinoffs from this year onward")
    parser.add_argument("--end-year", type=int, default=None, help="Include spinoffs up to this year")
    parser.add_argument("--no-next-day", action="store_true",
                        help="Use event-date close as entry (T+0). Default: next-day close (T+1)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    entry_offset = 0 if args.no_next_day else 1
    exec_model = "Event-date close (T+0)" if entry_offset == 0 else "Next-day close (MOC, T+1)"

    print("=" * 65)
    print("  CORPORATE SPINOFF EVENT STUDY")
    print("  Signal: Curated list of major US corporate spinoffs")
    print("  Benchmark: SPY")
    print(f"  Execution: {exec_model}")
    print(f"  Windows: {', '.join(f'T+{w}' for w in WINDOWS)}")
    print("=" * 65)

    # Build event list
    events = build_event_list(start_year=args.start_year, end_year=args.end_year)
    n_parent = sum(1 for e in events if e["category"] == "parent")
    n_child = sum(1 for e in events if e["category"] == "child")
    print(f"\n  Events: {len(events)} total ({n_parent} parent, {n_child} child)")
    print(f"  Spinoffs: {len(SPINOFFS)}, Period: {min(e['event_date'] for e in events)[:4]}–{max(e['event_date'] for e in events)[:4]}")

    # Fetch data
    print("\nPhase 1: Fetching price data...")
    t0 = time.time()
    client = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    result = fetch_data(client, events, verbose=args.verbose)
    if result is None:
        print("ERROR: Failed to fetch data.")
        sys.exit(1)
    con, found_symbols = result
    fetch_time = time.time() - t0
    print(f"  Data fetched in {fetch_time:.0f}s")

    # Compute returns
    print("\nPhase 2: Computing event-window returns...")
    t1 = time.time()
    results = compute_event_returns(con, events, found_symbols, windows=WINDOWS,
                                    entry_offset=entry_offset, verbose=args.verbose)
    compute_time = time.time() - t1
    print(f"  {len(results)} events with returns computed in {compute_time:.0f}s")

    if not results:
        print("ERROR: No event returns computed.")
        con.close()
        sys.exit(1)

    # Aggregate metrics
    print("\nPhase 3: Aggregating metrics...")
    metrics = compute_metrics(results, windows=WINDOWS)

    # Print results
    print(f"\n{'=' * 65}")
    print(f"  RESULTS: Spinoff Event Study (2011–2024)")
    print(f"{'=' * 65}")
    print_results(metrics)
    print_top_spinoffs(results)

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s")

    # Save results
    print("\nPhase 4: Saving results...")
    save_results(results, metrics, args.output, execution_model=exec_model)
    print("\nDone.")

    con.close()


if __name__ == "__main__":
    main()
