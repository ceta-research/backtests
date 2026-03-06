#!/usr/bin/env python3
"""
Index Reconstitution Screen — recent S&P 500 / NASDAQ-100 changes.

Shows stocks recently added to or removed from major US indices, with
return vs benchmark since the event date. Use this to monitor active
event windows (T+5, T+21, T+63).

Expected effects (from 2000-2025 backtest, N=679 SP500 / N=406 NDX):
  S&P 500 additions:  -0.98% T+5 (t=-3.68**), -1.06% T+21 (t=-2.11*)
  S&P 500 removals:   +7.22% T+21 mean (median +0.73%, outlier-driven)
  NDX additions:      -0.76% T+5 (t=-1.99*)
  NDX removals:       +5.13% T+21 (t=3.29**), median +2.61%

Usage:
    # S&P 500 (default), last 90 days
    python3 event-index-recon/screen.py

    # NASDAQ-100
    python3 event-index-recon/screen.py --index nasdaq100

    # Both indices
    python3 event-index-recon/screen.py --global

    # Last 180 calendar days
    python3 event-index-recon/screen.py --days 180
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

LOOKBACK_DAYS = 90

INDEX_CONFIGS = {
    "sp500": {
        "name": "S&P 500",
        "table": "historical_sp500_constituent",
        "benchmark_etf": "SPY",
    },
    "nasdaq100": {
        "name": "NASDAQ-100",
        "table": "historical_nasdaq_constituent",
        "benchmark_etf": "QQQ",
    },
}

EXPECTED_EFFECTS = {
    "sp500": {
        "addition": "-0.98% T+5 (t=-3.68**), -1.06% T+21 (t=-2.11*)",
        "removal":  "+7.22% T+21 mean (median +0.73%, outlier-driven)",
    },
    "nasdaq100": {
        "addition": "-0.76% T+5 (t=-1.99*)",
        "removal":  "+5.13% T+21 (t=3.29**), median +2.61%",
    },
}


def fetch_recent_events(client, table, since_date, verbose=False):
    """Fetch addition/removal events since since_date."""
    sql = f"""
        SELECT DISTINCT
            symbol,
            removedTicker,
            TRY_STRPTIME(dateAdded, '%B %d, %Y') AS event_date,
            reason
        FROM {table}
        WHERE TRY_STRPTIME(dateAdded, '%B %d, %Y') IS NOT NULL
          AND TRY_STRPTIME(dateAdded, '%B %d, %Y') >= '{since_date}'
        ORDER BY event_date DESC
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
        if isinstance(raw_dt, str):
            evt_date = date.fromisoformat(raw_dt[:10])
        elif isinstance(raw_dt, datetime):
            evt_date = raw_dt.date()
        elif isinstance(raw_dt, date):
            evt_date = raw_dt
        else:
            continue

        if sym and sym.strip():
            key = (sym.strip(), evt_date, "addition")
            if key not in seen:
                seen.add(key)
                events.append({"symbol": sym.strip(), "event_date": evt_date,
                               "category": "addition", "reason": reason})

        if removed and removed.strip():
            key = (removed.strip(), evt_date, "removal")
            if key not in seen:
                seen.add(key)
                events.append({"symbol": removed.strip(), "event_date": evt_date,
                               "category": "removal", "reason": reason})

    events.sort(key=lambda e: e["event_date"], reverse=True)
    return events


def fetch_prices_since(client, symbols, benchmark_etf, since_date, verbose=False):
    """Fetch daily adjusted close prices since since_date."""
    all_syms = list(set(symbols) | {benchmark_etf})
    sym_list = ", ".join(f"'{s}'" for s in all_syms)
    sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_list})
          AND CAST(date AS DATE) >= '{since_date}'
          AND adjClose > 0
        ORDER BY symbol, date
    """
    if verbose:
        print(f"  Fetching prices for {len(all_syms)} symbols since {since_date}...")
    rows = client.query(sql, format="json", verbose=verbose, timeout=300)
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
            td = date.fromisoformat(raw_dt[:10])
        elif isinstance(raw_dt, datetime):
            td = raw_dt.date()
        else:
            td = raw_dt
        if sym not in prices:
            prices[sym] = {}
        prices[sym][td] = float(px)
    return prices


def build_calendar(prices, benchmark_etf):
    """Build sorted list of trading days from benchmark price data."""
    return sorted(prices.get(benchmark_etf, {}).keys())


def get_price_on_or_after(prices, symbol, target_date, calendar):
    """Get the first available price on or after target_date."""
    sym_prices = prices.get(symbol, {})
    for d in calendar:
        if d >= target_date and d in sym_prices:
            return sym_prices[d], d
    return None, None


def count_trading_days(event_date, calendar):
    """Count trading days from event_date to last calendar day."""
    start_idx = None
    for i, d in enumerate(calendar):
        if d >= event_date:
            start_idx = i
            break
    if start_idx is None:
        return 0
    return len(calendar) - 1 - start_idx


def classify_window(days):
    if days < 5:
        return "< T+5"
    elif days < 21:
        return "T+5 zone"
    elif days < 63:
        return "T+21 zone"
    else:
        return "T+63+"


def fmt_pct(val, width=8):
    if val is None:
        return "N/A".rjust(width)
    return f"{val:+.2f}%".rjust(width)


def print_table(events, index_key):
    """Print screen results grouped by category."""
    effects = EXPECTED_EFFECTS[index_key]
    for cat in ["addition", "removal"]:
        subset = [e for e in events if e["category"] == cat]
        if not subset:
            continue
        label = "Additions" if cat == "addition" else "Removals"
        expected = effects[cat]
        print(f"\n  {label} (N={len(subset)}) — Backtest: {expected}")
        hdr = f"  {'Symbol':<8} {'Date':<12} {'Days':>5} {'Window':<12} {'Stock':>9} {'Bench':>9} {'CAR':>9}  Reason"
        print(f"  {'-' * 78}")
        print(hdr)
        print(f"  {'-' * 78}")
        for e in sorted(subset, key=lambda x: x["event_date"], reverse=True):
            days = e.get("days_elapsed")
            window = classify_window(days) if isinstance(days, int) else "?"
            reason = (e.get("reason") or "")[:20]
            print(
                f"  {e['symbol']:<8} "
                f"{e['event_date'].strftime('%Y-%m-%d'):<12} "
                f"{str(days) if days is not None else '?':>5} "
                f"{window:<12}"
                f"{fmt_pct(e.get('stock_return_pct'))}"
                f"{fmt_pct(e.get('bench_return_pct'))}"
                f"{fmt_pct(e.get('car_pct'))}  "
                f"{reason}"
            )


def screen_index(client, index_key, lookback_days=LOOKBACK_DAYS, verbose=False):
    """Run the screen for one index."""
    config = INDEX_CONFIGS[index_key]
    name = config["name"]
    table = config["table"]
    benchmark_etf = config["benchmark_etf"]
    since_date = date.today() - timedelta(days=lookback_days)

    print(f"\n{'=' * 65}")
    print(f"  INDEX RECON SCREEN: {name}")
    print(f"  Events since: {since_date}  ({lookback_days} calendar days)")
    print(f"  Benchmark: {benchmark_etf}")
    print(f"{'=' * 65}")

    events = fetch_recent_events(client, table, since_date.isoformat(), verbose=verbose)
    if not events:
        print("  No recent events found.")
        return

    print(f"  {len(events)} events found")

    symbols = list(set(e["symbol"] for e in events))
    prices = fetch_prices_since(client, symbols, benchmark_etf, since_date.isoformat(), verbose=verbose)
    calendar = build_calendar(prices, benchmark_etf)

    enriched = []
    for evt in events:
        sym = evt["symbol"]
        event_date = evt["event_date"]

        p0, _ = get_price_on_or_after(prices, sym, event_date, calendar)
        bench_p0, _ = get_price_on_or_after(prices, benchmark_etf, event_date, calendar)

        sym_prices = prices.get(sym, {})
        bench_prices = prices.get(benchmark_etf, {})
        p_current = sym_prices.get(max(sym_prices)) if sym_prices else None
        bench_current = bench_prices.get(max(bench_prices)) if bench_prices else None

        days_elapsed = count_trading_days(event_date, calendar)

        stock_ret = bench_ret = car = None
        if p0 and bench_p0 and p_current and bench_current and p0 > 0 and bench_p0 > 0:
            stock_ret = round((p_current - p0) / p0 * 100, 2)
            bench_ret = round((bench_current - bench_p0) / bench_p0 * 100, 2)
            car = round(stock_ret - bench_ret, 2)

        enriched.append({
            **evt,
            "days_elapsed": days_elapsed,
            "stock_return_pct": stock_ret,
            "bench_return_pct": bench_ret,
            "car_pct": car,
        })

    print_table(enriched, index_key)


def main():
    parser = argparse.ArgumentParser(description="Index Reconstitution Screen")
    parser.add_argument("--index", choices=list(INDEX_CONFIGS.keys()),
                        default="sp500", help="Index (default: sp500)")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen both S&P 500 and NASDAQ-100")
    parser.add_argument("--days", type=int, default=LOOKBACK_DAYS,
                        help=f"Lookback window in calendar days (default: {LOOKBACK_DAYS})")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    parser.add_argument("--base-url", type=str, help="API base URL")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    indices = ["sp500", "nasdaq100"] if args.global_screen else [args.index]
    client = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    for idx in indices:
        screen_index(client, idx, lookback_days=args.days, verbose=args.verbose)

    print()


if __name__ == "__main__":
    main()
