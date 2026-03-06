#!/usr/bin/env python3
"""
Index Reconstitution Event Study
S&P 500 and NASDAQ-100 addition/removal effects, 2000-2025.

Measures cumulative abnormal returns (CAR) around index changes and simulates
a "long removals at T+21" portfolio strategy to produce CAGR/Sharpe/MaxDD metrics.

Key data note: All three constituent history tables (historical_sp500_constituent,
historical_nasdaq_constituent, historical_dowjones_constituent) store a snapshot
'date' field that reflects when FMP last updated the record (e.g., 2025-11-30),
NOT the historical change date. The actual event date for BOTH additions and
removals is derived from 'dateAdded' (text field parsed via TRY_STRPTIME).

For removals: the stock that was removed (removedTicker) was removed on the same
date its replacement was added. So removal_event_date = dateAdded of the pairing row.

Usage:
    # S&P 500 (default)
    python3 event-index-recon/backtest.py

    # NASDAQ-100
    python3 event-index-recon/backtest.py --index nasdaq100

    # Both indices
    python3 event-index-recon/backtest.py --global

    # Save results
    python3 event-index-recon/backtest.py --output results/sp500_results.json --verbose
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from metrics import compute_metrics, compute_annual_returns, format_metrics

STRATEGY_NAME = "Index Reconstitution Event Study"
START_YEAR = 2000
END_DATE = "2026-01-31"
WINDOWS = [1, 5, 21, 63]      # Trading-day windows for CAR measurement
HOLD_WINDOW = 21               # Primary hold window for portfolio simulation
RISK_FREE_RATE = 0.02          # US 10Y Treasury proxy

INDEX_CONFIGS = {
    "sp500": {
        "name": "S&P 500",
        "table": "historical_sp500_constituent",
        "benchmark_etf": "SPY",
        "slug": "SP500",
    },
    "nasdaq100": {
        "name": "NASDAQ-100",
        "table": "historical_nasdaq_constituent",
        "benchmark_etf": "QQQ",
        "slug": "NDX",
    },
}

ALL_INDICES = ["sp500", "nasdaq100"]


# ---------------------------------------------------------------------------
# Phase 1: Fetch events from API
# ---------------------------------------------------------------------------

def fetch_events(client, table, verbose=False):
    """Fetch addition and removal events from a constituent history table.

    Returns list of dicts with keys: symbol, event_date, category, reason.
    Both additions and removals use dateAdded as the event date (see module docstring).
    """
    sql = f"""
        SELECT DISTINCT
            symbol,
            removedTicker,
            TRY_STRPTIME(dateAdded, '%B %d, %Y') AS event_date,
            reason
        FROM {table}
        WHERE TRY_STRPTIME(dateAdded, '%B %d, %Y') IS NOT NULL
          AND TRY_STRPTIME(dateAdded, '%B %d, %Y') >= '{START_YEAR}-01-01'
          AND TRY_STRPTIME(dateAdded, '%B %d, %Y') <= '{END_DATE}'
        ORDER BY event_date
    """
    rows = client.query(sql, verbose=verbose)
    if not rows:
        return []

    events = []
    seen = set()
    for row in rows:
        sym = row.get("symbol")
        removed = row.get("removedTicker")
        raw_dt = row.get("event_date")
        reason = row.get("reason", "")

        if raw_dt is None:
            continue

        # Parse event date (may come back as string or datetime)
        if isinstance(raw_dt, str):
            raw_dt = raw_dt[:10]
            evt_date = date.fromisoformat(raw_dt)
        elif isinstance(raw_dt, datetime):
            evt_date = raw_dt.date()
        elif isinstance(raw_dt, date):
            evt_date = raw_dt
        else:
            continue

        # Addition event
        if sym and sym.strip():
            key = (sym.strip(), evt_date, "addition")
            if key not in seen:
                seen.add(key)
                events.append({
                    "symbol": sym.strip(),
                    "event_date": evt_date,
                    "category": "addition",
                    "reason": reason,
                })

        # Removal event (use same dateAdded as event date)
        if removed and removed.strip():
            key = (removed.strip(), evt_date, "removal")
            if key not in seen:
                seen.add(key)
                events.append({
                    "symbol": removed.strip(),
                    "event_date": evt_date,
                    "category": "removal",
                    "reason": reason,
                })

    events.sort(key=lambda e: e["event_date"])
    return events


# ---------------------------------------------------------------------------
# Phase 2: Fetch prices
# ---------------------------------------------------------------------------

def fetch_prices(client, symbols, benchmark_etf, verbose=False):
    """Fetch daily adjusted close prices for event symbols + benchmark.

    Returns dict: {symbol -> {date_obj -> price}}
    """
    all_syms = list(set(symbols) | {benchmark_etf})
    sym_list = ", ".join(f"'{s}'" for s in all_syms)

    sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_list})
          AND CAST(date AS DATE) >= '{START_YEAR}-01-01'
          AND CAST(date AS DATE) <= '{END_DATE}'
          AND adjClose > 0
        ORDER BY symbol, date
    """
    if verbose:
        print(f"  Fetching prices for {len(all_syms)} symbols...")
    rows = client.query(sql, format="json", verbose=verbose, timeout=600, limit=10_000_000)
    if not rows:
        return {}

    prices = {}
    for row in rows:
        sym = row["symbol"]
        raw_dt = row["trade_date"]
        px = row["adjClose"]
        if raw_dt is None or px is None:
            continue
        if isinstance(raw_dt, str):
            trade_date = date.fromisoformat(raw_dt[:10])
        elif isinstance(raw_dt, datetime):
            trade_date = raw_dt.date()
        else:
            trade_date = raw_dt
        if sym not in prices:
            prices[sym] = {}
        prices[sym][trade_date] = float(px)

    return prices


def build_trading_calendar(prices, benchmark_etf):
    """Extract sorted list of trading dates from benchmark price data."""
    bench_prices = prices.get(benchmark_etf, {})
    return sorted(bench_prices.keys())


def get_price_at_offset(prices, symbol, event_date, offset_days, calendar):
    """Get the price at T+offset_days trading days from event_date.

    Returns (price, actual_date) or (None, None) if not found.
    """
    # Find the calendar index for event_date (or next trading day)
    for i, d in enumerate(calendar):
        if d >= event_date:
            target_idx = i + offset_days
            if 0 <= target_idx < len(calendar):
                target_date = calendar[target_idx]
                px = prices.get(symbol, {}).get(target_date)
                if px is not None:
                    return px, target_date
            break
    return None, None


# ---------------------------------------------------------------------------
# Phase 3: Event study analysis
# ---------------------------------------------------------------------------

def compute_event_returns(events, prices, benchmark_etf, calendar, windows, verbose=False):
    """Compute CAR for each event at each window.

    Returns list of dicts with event details + returns at each window.
    """
    results = []
    missing_prices = 0
    total = len(events)

    for i, evt in enumerate(events):
        sym = evt["symbol"]
        event_date = evt["event_date"]
        cat = evt["category"]

        if verbose and i % 100 == 0:
            print(f"  Processing event {i+1}/{total}...")

        # T+0 price (entry)
        p0, t0_date = get_price_at_offset(prices, sym, event_date, 0, calendar)
        bench_p0, _ = get_price_at_offset(prices, benchmark_etf, event_date, 0, calendar)

        if p0 is None or bench_p0 is None:
            missing_prices += 1
            continue

        row = {
            "symbol": sym,
            "event_date": event_date.isoformat(),
            "t0_date": t0_date.isoformat() if t0_date else None,
            "category": cat,
            "reason": evt.get("reason", ""),
            "p0": p0,
        }

        for w in windows:
            pN, tN_date = get_price_at_offset(prices, sym, event_date, w, calendar)
            bench_pN, _ = get_price_at_offset(prices, benchmark_etf, event_date, w, calendar)

            if pN is not None and bench_pN is not None and p0 > 0 and bench_p0 > 0:
                stock_ret = (pN - p0) / p0
                bench_ret = (bench_pN - bench_p0) / bench_p0
                car = stock_ret - bench_ret
                row[f"ret_T{w}"] = round(stock_ret * 100, 4)
                row[f"bench_T{w}"] = round(bench_ret * 100, 4)
                row[f"car_T{w}"] = round(car * 100, 4)
            else:
                row[f"ret_T{w}"] = None
                row[f"bench_T{w}"] = None
                row[f"car_T{w}"] = None

        results.append(row)

    if verbose:
        print(f"  Computed returns for {len(results)}/{total} events ({missing_prices} missing prices)")
    return results


def compute_car_summary(results, windows):
    """Compute mean CAR, t-stat, hit rate by category and window."""
    import math

    categories = ["addition", "removal", "all"]
    summary = {}

    for cat in categories:
        if cat == "all":
            subset = results
        else:
            subset = [r for r in results if r["category"] == cat]

        if not subset:
            continue

        summary[cat] = {"n_events": len(subset), "windows": {}}

        for w in windows:
            key = f"car_T{w}"
            cars = [r[key] for r in subset if r.get(key) is not None]
            n = len(cars)
            if n < 2:
                continue

            mean_car = sum(cars) / n
            median_car = sorted(cars)[n // 2]
            variance = sum((x - mean_car) ** 2 for x in cars) / (n - 1)
            std = math.sqrt(variance)
            t_stat = (mean_car / (std / math.sqrt(n))) if std > 0 else 0
            hit_rate = sum(1 for c in cars if c > 0) / n * 100

            summary[cat]["windows"][f"T+{w}"] = {
                "mean_car_pct": round(mean_car, 4),
                "median_car_pct": round(median_car, 4),
                "t_stat": round(t_stat, 3),
                "n": n,
                "hit_rate_pct": round(hit_rate, 1),
                "sig_5pct": abs(t_stat) > 1.96,
                "sig_1pct": abs(t_stat) > 2.576,
            }

    return summary


# ---------------------------------------------------------------------------
# Phase 4: Portfolio simulation (long removals at T+21)
# ---------------------------------------------------------------------------

def simulate_removal_portfolio(results, benchmark_etf, prices, calendar, hold_window=21):
    """Simulate a monthly portfolio that goes long on every removal event.

    For each calendar month:
    - Enter all removal events starting in that month at T+0
    - Exit at T+hold_window (default 21 trading days)
    - Portfolio return = equal-weight average of all exits in that month's T+hold_window window
    - Benchmark return = same-month SPY return (first to last trading day of the calendar month)

    Returns list of dicts: {month, portfolio_return, benchmark_return, n_events}
    """
    # Filter to removals only
    removals = [r for r in results if r["category"] == "removal"
                and r.get(f"car_T{hold_window}") is not None
                and r.get(f"ret_T{hold_window}") is not None]

    if not removals:
        return []

    # Group by year-month (event start month)
    by_month = {}
    for r in removals:
        ym = r["event_date"][:7]  # "YYYY-MM"
        if ym not in by_month:
            by_month[ym] = []
        by_month[ym].append(r)

    # Compute SPY monthly returns from calendar
    bench_prices = prices.get(benchmark_etf, {})
    spy_monthly = {}
    for d in calendar:
        ym = d.strftime("%Y-%m")
        if ym not in spy_monthly:
            spy_monthly[ym] = {"start": None, "end": None}
        spy_monthly[ym]["end"] = bench_prices.get(d)
        if spy_monthly[ym]["start"] is None:
            spy_monthly[ym]["start"] = bench_prices.get(d)

    # Build period results for months that had removal events
    period_results = []
    for ym in sorted(by_month.keys()):
        events_this_month = by_month[ym]
        # Portfolio return = avg T+21 raw stock return (not CAR, since we're building full portfolio)
        # Using raw stock return because we're simulating actual long positions
        rets = [r[f"ret_T{hold_window}"] / 100.0 for r in events_this_month
                if r.get(f"ret_T{hold_window}") is not None]
        if not rets:
            continue
        port_ret = sum(rets) / len(rets)

        # Benchmark: SPY return over the same T+21 window as the average event
        # Use the month's SPY monthly return as proxy
        spy_info = spy_monthly.get(ym, {})
        spy_start = spy_info.get("start")
        spy_end = spy_info.get("end")
        if spy_start and spy_end and spy_start > 0:
            spy_ret = (spy_end - spy_start) / spy_start
        else:
            spy_ret = 0.0

        period_results.append({
            "month": ym,
            "portfolio_return": round(port_ret, 6),
            "benchmark_return": round(spy_ret, 6),
            "n_events": len(rets),
        })

    return period_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_index(client, index_key, verbose=False, output_dir=None):
    """Run the full event study for one index. Returns summary dict."""
    config = INDEX_CONFIGS[index_key]
    table = config["table"]
    name = config["name"]
    benchmark_etf = config["benchmark_etf"]
    slug = config["slug"]

    print(f"\n{'=' * 65}")
    print(f"  INDEX RECONSTITUTION EVENT STUDY: {name}")
    print(f"  Table: {table}")
    print(f"  Benchmark: {benchmark_etf}")
    print(f"  Period: {START_YEAR}-2025")
    print(f"  Hold window: T+{HOLD_WINDOW} trading days")
    print(f"{'=' * 65}")

    t0 = time.time()

    # 1. Fetch events
    print(f"\nPhase 1: Fetching events from {table}...")
    events = fetch_events(client, table, verbose=verbose)
    if not events:
        print("  ERROR: No events returned.")
        return None

    additions = [e for e in events if e["category"] == "addition"]
    removals = [e for e in events if e["category"] == "removal"]
    print(f"  {len(events)} total events: {len(additions)} additions, {len(removals)} removals")
    print(f"  Date range: {events[0]['event_date']} to {events[-1]['event_date']}")

    # 2. Fetch prices
    print(f"\nPhase 2: Fetching prices...")
    event_symbols = list(set(e["symbol"] for e in events))
    prices = fetch_prices(client, event_symbols, benchmark_etf, verbose=verbose)
    calendar = build_trading_calendar(prices, benchmark_etf)
    print(f"  {len(prices)} symbols loaded, {len(calendar)} trading days")

    # 3. Compute event returns
    print(f"\nPhase 3: Computing event returns...")
    event_results = compute_event_returns(events, prices, benchmark_etf, calendar,
                                          WINDOWS, verbose=verbose)
    car_summary = compute_car_summary(event_results, WINDOWS)

    # Print CAR table
    print(f"\n  CAR Summary ({name}):")
    print(f"  {'Window':<10} {'Category':<12} {'Mean CAR':>10} {'t-stat':>8} {'N':>7} {'Hit Rate':>10} {'Sig?':>6}")
    print(f"  {'-' * 64}")
    for cat in ["addition", "removal"]:
        cat_data = car_summary.get(cat, {})
        for w in WINDOWS:
            w_key = f"T+{w}"
            w_data = cat_data.get("windows", {}).get(w_key)
            if w_data:
                sig = "**" if w_data["sig_1pct"] else ("*" if w_data["sig_5pct"] else "")
                print(f"  {w_key:<10} {cat:<12} {w_data['mean_car_pct']:>9.3f}% "
                      f"{w_data['t_stat']:>8.2f} {w_data['n']:>7} "
                      f"{w_data['hit_rate_pct']:>9.1f}% {sig:>6}")

    # 4. Portfolio simulation
    print(f"\nPhase 4: Simulating 'long removals T+{HOLD_WINDOW}' portfolio...")
    period_results = simulate_removal_portfolio(event_results, benchmark_etf, prices, calendar, HOLD_WINDOW)

    if len(period_results) < 12:
        print(f"  WARNING: Only {len(period_results)} monthly periods — not enough for metrics.")
        port_metrics = None
    else:
        port_returns = [p["portfolio_return"] for p in period_results]
        bench_returns = [p["benchmark_return"] for p in period_results]
        port_metrics = compute_metrics(port_returns, bench_returns,
                                       periods_per_year=12, risk_free_rate=RISK_FREE_RATE)

        print(format_metrics(port_metrics, f"{slug} Removals", benchmark_etf))

        # Annual returns
        period_dates = [p["month"] + "-01" for p in period_results]
        annual = compute_annual_returns(port_returns, bench_returns, period_dates, 12)
        if annual:
            print(f"\n  {'Year':<8} {'Long Removals':>14} {benchmark_etf:>10} {'Excess':>10} {'Events':>8}")
            print(f"  {'-' * 52}")
            for ar in annual:
                yr = ar["year"]
                yr_events = sum(p["n_events"] for p in period_results if p["month"].startswith(str(yr)))
                print(f"  {yr:<8} {ar['portfolio']*100:>13.1f}% {ar['benchmark']*100:>9.1f}% "
                      f"{ar['excess']*100:>+9.1f}% {yr_events:>8}")

    elapsed = time.time() - t0
    print(f"\n  Completed in {elapsed:.0f}s")

    # Build output
    result = {
        "strategy": STRATEGY_NAME,
        "index": name,
        "table": table,
        "benchmark": benchmark_etf,
        "period": f"{START_YEAR}-2025",
        "n_events": len(event_results),
        "n_additions": len([r for r in event_results if r["category"] == "addition"]),
        "n_removals": len([r for r in event_results if r["category"] == "removal"]),
        "hold_window_days": HOLD_WINDOW,
        "car_summary": car_summary,
        "portfolio_simulation": {
            "strategy": f"Long removals at T+{HOLD_WINDOW}",
            "n_months": len(period_results),
            "avg_events_per_month": (sum(p["n_events"] for p in period_results) / len(period_results)
                                      if period_results else 0),
        },
    }

    if port_metrics:
        p = port_metrics["portfolio"]
        b = port_metrics["benchmark"]
        c = port_metrics["comparison"]
        result["portfolio_simulation"].update({
            "cagr_pct": round(p["cagr"] * 100, 2) if p["cagr"] is not None else None,
            "spy_cagr_pct": round(b["cagr"] * 100, 2) if b["cagr"] is not None else None,
            "excess_cagr_pct": round(c["excess_cagr"] * 100, 2) if c["excess_cagr"] is not None else None,
            "max_drawdown_pct": round(p["max_drawdown"] * 100, 2) if p["max_drawdown"] is not None else None,
            "sharpe_ratio": round(p["sharpe_ratio"], 3) if p["sharpe_ratio"] is not None else None,
            "sortino_ratio": round(p["sortino_ratio"], 3) if p["sortino_ratio"] is not None else None,
            "win_rate_pct": round(c["win_rate"] * 100, 1) if c["win_rate"] is not None else None,
        })
        period_dates = [p["month"] + "-01" for p in period_results]
        annual = compute_annual_returns(port_returns, bench_returns, period_dates, 12)
        result["annual_returns"] = [
            {"year": ar["year"], "portfolio_pct": round(ar["portfolio"] * 100, 2),
             "benchmark_pct": round(ar["benchmark"] * 100, 2),
             "excess_pct": round(ar["excess"] * 100, 2)}
            for ar in annual
        ]

    # Save to disk
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        fname = os.path.join(output_dir, f"results_{slug}.json")
        with open(fname, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n  Results saved to {fname}")

        # Save event-level CSV
        csv_path = os.path.join(output_dir, f"event_returns_{slug}.csv")
        if event_results:
            headers = list(event_results[0].keys())
            with open(csv_path, "w") as f:
                f.write(",".join(headers) + "\n")
                for row in event_results:
                    f.write(",".join(str(row.get(h, "")) for h in headers) + "\n")
            print(f"  Event returns saved to {csv_path}")

    return result


def main():
    parser = argparse.ArgumentParser(description=STRATEGY_NAME)
    parser.add_argument("--index", choices=list(INDEX_CONFIGS.keys()),
                        default="sp500", help="Index to analyze (default: sp500)")
    parser.add_argument("--global", dest="global_bt", action="store_true",
                        help="Run all indices (S&P 500 + NASDAQ-100)")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory for results (default: event-index-recon/results/)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    output_dir = args.output or os.path.join(os.path.dirname(__file__), "results")
    indices = ALL_INDICES if args.global_bt else [args.index]

    client = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    all_results = {}
    for idx in indices:
        r = run_index(client, idx, verbose=args.verbose, output_dir=output_dir)
        if r:
            all_results[idx] = r

    # Save comparison summary if running multiple indices
    if len(all_results) > 1:
        comparison = {}
        for idx_key, r in all_results.items():
            sim = r.get("portfolio_simulation", {})
            comparison[r["index"]] = {
                "n_removals": r["n_removals"],
                "cagr_pct": sim.get("cagr_pct"),
                "benchmark_cagr_pct": sim.get("spy_cagr_pct"),
                "excess_cagr_pct": sim.get("excess_cagr_pct"),
                "max_drawdown_pct": sim.get("max_drawdown_pct"),
                "sharpe_ratio": sim.get("sharpe_ratio"),
                "removal_T21_car_pct": (
                    r.get("car_summary", {}).get("removal", {})
                    .get("windows", {}).get("T+21", {}).get("mean_car_pct")
                ),
            }

        comp_path = os.path.join(output_dir, "index_comparison.json")
        with open(comp_path, "w") as f:
            json.dump(comparison, f, indent=2)
        print(f"\nComparison saved to {comp_path}")

        print("\n=== CROSS-INDEX COMPARISON ===")
        print(f"  {'Index':<16} {'Removals':>10} {'CAGR':>8} {'Excess':>8} {'MaxDD':>8} {'Sharpe':>8} {'T+21 CAR':>10}")
        print(f"  {'-' * 72}")
        for idx_name, v in comparison.items():
            cagr = f"{v['cagr_pct']:.1f}%" if v['cagr_pct'] is not None else "N/A"
            exc = f"{v['excess_cagr_pct']:+.1f}%" if v['excess_cagr_pct'] is not None else "N/A"
            dd = f"{v['max_drawdown_pct']:.1f}%" if v['max_drawdown_pct'] is not None else "N/A"
            sh = f"{v['sharpe_ratio']:.2f}" if v['sharpe_ratio'] is not None else "N/A"
            t21 = f"{v['removal_T21_car_pct']:+.3f}%" if v['removal_T21_car_pct'] is not None else "N/A"
            print(f"  {idx_name:<16} {v['n_removals']:>10} {cagr:>8} {exc:>8} {dd:>8} {sh:>8} {t21:>10}")


if __name__ == "__main__":
    main()
