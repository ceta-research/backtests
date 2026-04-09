#!/usr/bin/env python3
"""
S&P 500 Survivorship Bias Analysis

Measures how much survivorship bias inflates backtest returns by comparing:
1. Biased: Screen current S&P 500 members across all historical dates
2. Unbiased: Screen point-in-time S&P 500 members at each rebalance date

Strategy: Low P/E screen (0 < P/E < 15), top 100 by lowest P/E, equal weight.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2000-2025.
Transaction costs: Size-tiered (0.1-0.5% per trade).

Usage:
    python3 sp500-survivorship/backtest.py
    python3 sp500-survivorship/backtest.py --verbose
    python3 sp500-survivorship/backtest.py --output results/summary.json
"""

import argparse
import duckdb
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, generate_rebalance_dates, filter_returns, remove_price_oscillations
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs

# --- Config ---
PE_MAX = 15.0
PE_MIN = 0.0
TOP_N = 100
FILING_LAG_DAYS = 45
START_YEAR = 2000
END_YEAR = 2025
RISK_FREE_RATE = 0.02  # US 10-year average


def fetch_data(client, rebalance_dates, verbose=False):
    """Fetch S&P 500 constituent + financial data into DuckDB."""
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Historical S&P 500 constituent changes
    print("  Fetching historical S&P 500 constituent changes...")
    hist_sql = """
        SELECT symbol, removedTicker, dateAddedEpoch,
               addedSecurity, removedSecurity
        FROM historical_sp500_constituent
        WHERE dateAddedEpoch IS NOT NULL
    """
    count = query_parquet(client, hist_sql, con, "hist_constituents",
                          verbose=verbose, limit=100000, timeout=300,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} change events")

    # 2. Current S&P 500 members
    print("  Fetching current S&P 500 members...")
    curr_sql = "SELECT DISTINCT symbol, sector FROM sp500_constituent"
    curr_data = client.query(curr_sql, verbose=verbose, timeout=120)
    if not curr_data:
        print("  ERROR: No current S&P 500 members found.")
        return None

    vals = ",".join(
        f"('{r['symbol']}', '{r.get('sector', '').replace(chr(39), '')}')"
        for r in curr_data
    )
    con.execute(f"CREATE TABLE current_sp500(symbol VARCHAR, sector VARCHAR)")
    con.execute(f"INSERT INTO current_sp500 VALUES {vals}")
    print(f"    -> {len(curr_data)} current members")

    # 3. Build symbol filter for warehouse queries (all S&P 500 related symbols)
    sp500_sym_filter = """(
        symbol IN (SELECT DISTINCT symbol FROM sp500_constituent)
        OR symbol IN (SELECT DISTINCT symbol FROM historical_sp500_constituent WHERE symbol IS NOT NULL)
        OR symbol IN (SELECT DISTINCT "removedTicker" FROM historical_sp500_constituent WHERE "removedTicker" IS NOT NULL)
    )"""

    # 4. Financial ratios (P/E)
    print("  Fetching financial ratios (P/E)...")
    ratios_sql = f"""
        SELECT symbol, "priceToEarningsRatio", "dateEpoch" as filing_epoch
        FROM financial_ratios
        WHERE period = 'FY'
          AND "priceToEarningsRatio" IS NOT NULL
          AND {sp500_sym_filter}
    """
    count = query_parquet(client, ratios_sql, con, "ratios_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} P/E rows")

    # 5. Key metrics (market cap)
    print("  Fetching key metrics (market cap)...")
    metrics_sql = f"""
        SELECT symbol, "marketCap", "dateEpoch" as filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND "marketCap" IS NOT NULL
          AND {sp500_sym_filter}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} market cap rows")

    # 6. Prices at rebalance windows
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=11)  # +1 for offset_days=1 (MOC execution)
        date_conditions.append(
            f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')"
        )
    date_filter = " OR ".join(date_conditions)

    price_sql = f"""
        SELECT symbol, "dateEpoch" as trade_epoch, "adjClose"
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR {sp500_sym_filter}
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=10000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX IF NOT EXISTS idx_prices ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count:,} price rows")

    return con


def reconstruct_sp500_at_date(con, target_date):
    """Reconstruct S&P 500 membership at a specific date.

    Logic: Track all addition/removal events up to target_date.
    For each symbol, the latest event determines membership.
    Current members with no historical events assumed "always in" (epoch 0).
    """
    target_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())

    members = con.execute("""
        WITH events AS (
            -- Historical additions
            SELECT symbol, dateAddedEpoch AS event_epoch, 1 AS event_type
            FROM hist_constituents
            WHERE dateAddedEpoch <= ? AND symbol IS NOT NULL
            UNION ALL
            -- Historical removals
            SELECT removedTicker AS symbol, dateAddedEpoch AS event_epoch, 0 AS event_type
            FROM hist_constituents
            WHERE dateAddedEpoch <= ? AND removedTicker IS NOT NULL
            UNION ALL
            -- Current members as fallback (catches members with no change events)
            SELECT symbol, 0 AS event_epoch, 1 AS event_type
            FROM current_sp500
        ),
        latest AS (
            SELECT symbol, event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY event_epoch DESC, event_type DESC
                ) AS rn
            FROM events
        )
        SELECT symbol FROM latest WHERE rn = 1 AND event_type = 1
    """, [target_epoch, target_epoch]).fetchall()

    return set(r[0] for r in members)


def screen_low_pe(con, target_date, universe_symbols):
    """Screen for low P/E stocks within a given universe.

    Returns list of (symbol, pe_ratio, market_cap) sorted by P/E ascending.
    """
    if not universe_symbols:
        return []

    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    # Insert universe into temp table for efficient joining
    con.execute("DROP TABLE IF EXISTS _screen_universe")
    if len(universe_symbols) > 0:
        vals = ",".join(f"('{s}')" for s in universe_symbols)
        con.execute(f"CREATE TEMP TABLE _screen_universe(symbol VARCHAR)")
        con.execute(f"INSERT INTO _screen_universe VALUES {vals}")

    rows = con.execute(f"""
        WITH latest_pe AS (
            SELECT r.symbol, r.priceToEarningsRatio AS pe,
                   r.filing_epoch,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.symbol ORDER BY r.filing_epoch DESC
                   ) AS rn
            FROM ratios_cache r
            JOIN _screen_universe u ON r.symbol = u.symbol
            WHERE r.filing_epoch <= ?
        ),
        latest_mcap AS (
            SELECT m.symbol, m.marketCap,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.symbol ORDER BY m.filing_epoch DESC
                   ) AS rn
            FROM metrics_cache m
            JOIN _screen_universe u ON m.symbol = u.symbol
            WHERE m.filing_epoch <= ?
        )
        SELECT p.symbol, p.pe, COALESCE(c.marketCap, 1e9) AS mcap
        FROM latest_pe p
        LEFT JOIN latest_mcap c ON p.symbol = c.symbol AND c.rn = 1
        WHERE p.rn = 1
          AND p.pe > {PE_MIN}
          AND p.pe < {PE_MAX}
        ORDER BY p.pe ASC
        LIMIT {TOP_N}
    """, [cutoff_epoch, cutoff_epoch]).fetchall()

    con.execute("DROP TABLE IF EXISTS _screen_universe")
    return [(r[0], r[1], r[2]) for r in rows]


def get_price(con, symbol, target_date, offset_days=1):
    """Get adjusted close offset_days after target_date.

    offset_days=1: next-day close (MOC execution, default)
    offset_days=0: same-day close (legacy)
    """
    start = target_date + timedelta(days=offset_days)
    target_epoch = int(datetime.combine(start, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(
        start + timedelta(days=10), datetime.min.time()
    ).timestamp())
    row = con.execute("""
        SELECT adjClose FROM prices_cache
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, target_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def compute_portfolio_return(con, stock_list, entry_date, exit_date,
                              use_costs=True, offset_days=1):
    """Compute equal-weighted return for a list of (symbol, pe, mcap)."""
    if not stock_list:
        return 0.0, 0, 0

    symbol_returns = []
    for sym, pe, mcap in stock_list:
        ep = get_price(con, sym, entry_date, offset_days=offset_days)
        xp = get_price(con, sym, exit_date, offset_days=offset_days)
        symbol_returns.append((sym, ep, xp, mcap or 1e9))

    clean, skipped = filter_returns(symbol_returns)

    if not clean:
        return 0.0, 0, len(symbol_returns)

    returns = []
    for sym, raw_ret, mcap in clean:
        if use_costs:
            cost = tiered_cost(mcap)
            net_ret = apply_costs(raw_ret, cost)
        else:
            net_ret = raw_ret
        returns.append(net_ret)

    mean_ret = sum(returns) / len(returns)
    return mean_ret, len(returns), len(symbol_returns) - len(clean)


def run_backtest(con, rebalance_dates, use_costs=True, verbose=False, offset_days=1):
    """Run the biased vs unbiased backtest."""
    print(f"Phase 2: Running backtest "
          f"({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")

    # Get current S&P 500 members (biased universe - same for all dates)
    current_members = set(
        r[0] for r in con.execute("SELECT symbol FROM current_sp500").fetchall()
    )
    print(f"  Current S&P 500: {len(current_members)} members (biased universe)")

    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        # Reconstruct point-in-time S&P 500 (unbiased)
        pit_members = reconstruct_sp500_at_date(con, entry_date)

        # Screen both universes
        biased_stocks = screen_low_pe(con, entry_date, current_members)
        unbiased_stocks = screen_low_pe(con, entry_date, pit_members)

        # Compute returns
        b_ret, b_cnt, b_skip = compute_portfolio_return(
            con, biased_stocks, entry_date, exit_date, use_costs,
            offset_days=offset_days)
        u_ret, u_cnt, u_skip = compute_portfolio_return(
            con, unbiased_stocks, entry_date, exit_date, use_costs,
            offset_days=offset_days)

        # SPY benchmark
        spy_ep = get_price(con, "SPY", entry_date, offset_days=offset_days)
        spy_xp = get_price(con, "SPY", exit_date, offset_days=offset_days)
        spy_ret = ((spy_xp - spy_ep) / spy_ep
                   if spy_ep and spy_xp and spy_ep > 0 else None)

        # Survivorship victims: in unbiased screen but not biased screen
        biased_syms = set(s[0] for s in biased_stocks)
        unbiased_syms = set(s[0] for s in unbiased_stocks)
        victims = unbiased_syms - biased_syms

        periods.append({
            "year": entry_date.year,
            "quarter": (entry_date.month - 1) // 3 + 1,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "biased_return": b_ret,
            "unbiased_return": u_ret,
            "spy_return": spy_ret,
            "biased_count": b_cnt,
            "unbiased_count": u_cnt,
            "pit_member_count": len(pit_members),
            "survivorship_victims": len(victims),
        })

        if verbose:
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date}: "
                  f"B={b_ret*100:+.1f}% ({b_cnt}), "
                  f"U={u_ret*100:+.1f}% ({u_cnt}), "
                  f"SPY={spy_pct:+.1f}%, "
                  f"PIT={len(pit_members)}, victims={len(victims)}")

    print(f"Phase 2 complete: {len(periods)} periods.\n")
    return periods


def build_output(periods):
    """Build output dict with metrics and analysis."""
    valid = [p for p in periods if p["spy_return"] is not None]
    n = len(valid)
    if n == 0:
        return {"error": "No valid periods"}

    periods_per_year = 4  # quarterly

    biased_rets = [p["biased_return"] for p in valid]
    unbiased_rets = [p["unbiased_return"] for p in valid]
    spy_rets = [p["spy_return"] for p in valid]

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    results = {}
    for name, rets in [("biased", biased_rets),
                        ("unbiased", unbiased_rets),
                        ("spy", spy_rets)]:
        m = _compute_metrics(rets, spy_rets, periods_per_year,
                              risk_free_rate=RISK_FREE_RATE)
        pm = m["portfolio"]

        results[name] = {
            "cagr": round(pm["cagr"] * 100, 2),
            "total_return": round(pm["total_return"] * 100, 2),
            "volatility": round(pm["annualized_volatility"] * 100, 2),
            "sharpe": rnd(pm["sharpe_ratio"]),
            "sortino": rnd(pm["sortino_ratio"]),
            "calmar": rnd(pm["calmar_ratio"]),
            "max_drawdown": round(pm["max_drawdown"] * 100, 1),
            "var_95": round(pm["var_95"] * 100, 1) if pm["var_95"] is not None else None,
        }

        if name in ("biased", "unbiased"):
            c = m["comparison"]
            results[f"{name}_vs_spy"] = {
                "excess_cagr": round(c["excess_cagr"] * 100, 2),
                "information_ratio": rnd(c["information_ratio"]),
                "up_capture": (round(c["up_capture"] * 100, 1)
                               if c["up_capture"] is not None else None),
                "down_capture": (round(c["down_capture"] * 100, 1)
                                 if c["down_capture"] is not None else None),
                "beta": rnd(c["beta"]),
                "alpha": (round(c["alpha"] * 100, 2)
                          if c["alpha"] is not None else None),
            }

    # Survivorship bias gap
    bias_gap = {
        "cagr_gap": round(
            results["biased"]["cagr"] - results["unbiased"]["cagr"], 2
        ),
        "sharpe_gap": rnd(
            (results["biased"]["sharpe"] or 0)
            - (results["unbiased"]["sharpe"] or 0)
        ),
        "drawdown_gap": round(
            results["biased"]["max_drawdown"]
            - results["unbiased"]["max_drawdown"], 1
        ),
        "volatility_gap": round(
            results["biased"]["volatility"]
            - results["unbiased"]["volatility"], 2
        ),
    }

    # Decade breakdown
    decades = {}
    for p in valid:
        yr = p["year"]
        if yr < 2005:
            d = "2000-04"
        elif yr < 2010:
            d = "2005-09"
        elif yr < 2015:
            d = "2010-14"
        elif yr < 2020:
            d = "2015-19"
        else:
            d = "2020-25"

        if d not in decades:
            decades[d] = {"biased": [], "unbiased": [], "spy": []}
        decades[d]["biased"].append(p["biased_return"])
        decades[d]["unbiased"].append(p["unbiased_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            b_avg = sum(decades[d]["biased"]) / len(decades[d]["biased"]) * 100
            u_avg = sum(decades[d]["unbiased"]) / len(decades[d]["unbiased"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "biased": round(b_avg, 1),
                "unbiased": round(u_avg, 1),
                "gap": round(b_avg - u_avg, 1),
                "spy": round(spy_avg, 1),
            })

    # Stock count stats
    avg_biased = sum(p["biased_count"] for p in valid) / n
    avg_unbiased = sum(p["unbiased_count"] for p in valid) / n
    avg_pit = sum(p["pit_member_count"] for p in valid) / n
    avg_victims = sum(p["survivorship_victims"] for p in valid) / n

    output = {
        "strategy": "S&P 500 Survivorship Bias Analysis",
        "signal": f"Low P/E screen (0 < P/E < {PE_MAX}), top {TOP_N} by lowest P/E",
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} quarters)",
        "rebalancing": "quarterly (Jan/Apr/Jul/Oct)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "filing_lag": f"{FILING_LAG_DAYS} days",
        "portfolios": {
            "biased": results["biased"],
            "unbiased": results["unbiased"],
            "spy": results["spy"],
        },
        "survivorship_bias": bias_gap,
        "biased_vs_spy": results.get("biased_vs_spy"),
        "unbiased_vs_spy": results.get("unbiased_vs_spy"),
        "avg_counts": {
            "biased_portfolio": round(avg_biased, 0),
            "unbiased_portfolio": round(avg_unbiased, 0),
            "pit_sp500_members": round(avg_pit, 0),
            "survivorship_victims_per_period": round(avg_victims, 1),
        },
        "decade_breakdown": decade_results,
        "quarterly_returns": [
            {
                "year": p["year"],
                "quarter": p["quarter"],
                "biased": round(p["biased_return"] * 100, 2),
                "unbiased": round(p["unbiased_return"] * 100, 2),
                "spy": (round(p["spy_return"] * 100, 2)
                        if p["spy_return"] else None),
                "biased_count": p["biased_count"],
                "unbiased_count": p["unbiased_count"],
                "pit_members": p["pit_member_count"],
            }
            for p in valid
        ],
    }

    return output


def print_summary(m):
    """Print formatted results."""
    if "error" in m:
        print(f"\nERROR: {m['error']}")
        return

    p = m["portfolios"]
    bias = m["survivorship_bias"]

    print("\n" + "=" * 85)
    print("S&P 500 SURVIVORSHIP BIAS ANALYSIS")
    print("=" * 85)
    print(f"Period: {m['period']}")
    print(f"Signal: {m['signal']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print(f"Filing lag: {m['filing_lag']}")

    counts = m["avg_counts"]
    print(f"Avg portfolio: Biased={counts['biased_portfolio']:.0f}, "
          f"Unbiased={counts['unbiased_portfolio']:.0f}")
    print(f"Avg PIT S&P 500 members: {counts['pit_sp500_members']:.0f}")
    print(f"Avg survivorship victims/period: "
          f"{counts['survivorship_victims_per_period']:.1f}")
    print("-" * 85)

    header = (f"{'Portfolio':<20} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8}")
    print(header)
    print("-" * 85)

    for name, label in [("biased", "Biased (current)"),
                        ("unbiased", "Unbiased (PIT)"),
                        ("spy", "S&P 500 (SPY)")]:
        d = p[name]
        sh = f"{d['sharpe']:.3f}" if d['sharpe'] is not None else "N/A"
        so = f"{d['sortino']:.3f}" if d['sortino'] is not None else "N/A"
        ca = f"{d['calmar']:.3f}" if d['calmar'] is not None else "N/A"
        print(f"{label:<20} {d['cagr']:>7.1f}% {d['volatility']:>7.1f}% "
              f"{sh:>8} {so:>8} {ca:>8} {d['max_drawdown']:>7.1f}%")

    print(f"\nSURVIVORSHIP BIAS GAP:")
    print(f"  CAGR gap:       {bias['cagr_gap']:+.2f}%")
    print(f"  Sharpe gap:     {bias['sharpe_gap']:+.3f}")
    print(f"  MaxDD gap:      {bias['drawdown_gap']:+.1f}%")
    print(f"  Volatility gap: {bias['volatility_gap']:+.2f}%")

    for label, key in [("Biased vs SPY", "biased_vs_spy"),
                       ("Unbiased vs SPY", "unbiased_vs_spy")]:
        vs = m.get(key)
        if vs:
            print(f"\n{label}:")
            print(f"  Excess CAGR: {vs['excess_cagr']:+.2f}%")
            if vs.get('beta') is not None:
                print(f"  Beta: {vs['beta']:.3f}  |  "
                      f"Alpha: {vs['alpha']:+.2f}%")
            if vs.get('up_capture') is not None:
                print(f"  Up Capture: {vs['up_capture']:.1f}%  |  "
                      f"Down Capture: {vs['down_capture']:.1f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Biased':>10} {'Unbiased':>10} "
              f"{'Gap':>10} {'SPY':>10}")
        print("-" * 55)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['biased']:>9.1f}% "
                  f"{d['unbiased']:>9.1f}% "
                  f"{d['gap']:>+9.1f}% {d['spy']:>9.1f}%")

    print("=" * 85)


def main():
    parser = argparse.ArgumentParser(
        description="S&P 500 Survivorship Bias Backtest"
    )
    parser.add_argument("--output", help="Save JSON results to file")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--no-costs", action="store_true",
                        help="Disable transaction costs")
    parser.add_argument("--no-next-day", action="store_true",
                        help="Use same-day close instead of next-day (legacy mode)")
    parser.add_argument("--api-key", help="CR API key")
    parser.add_argument("--base-url", help="Override API base URL")
    args = parser.parse_args()

    offset_days = 0 if args.no_next_day else 1
    exec_model = "next-day close (MOC)" if offset_days == 1 else "same-day close (legacy)"

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print("\n" + "=" * 75)
    print("S&P 500 SURVIVORSHIP BIAS BACKTEST")
    print("=" * 75)
    print(f"  Signal: Low P/E (0 < P/E < {PE_MAX}), top {TOP_N}")
    print(f"  Rebalancing: Quarterly, {START_YEAR}-{END_YEAR}")
    print(f"  Costs: {'size-tiered' if not args.no_costs else 'none'}")
    print(f"  Filing lag: {FILING_LAG_DAYS} days")
    print(f"  Execution: {exec_model}")
    print("=" * 75)

    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(
        START_YEAR, END_YEAR, "quarterly"
    )
    t0 = time.time()
    con = fetch_data(cr, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available.")
        return
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    t1 = time.time()
    use_costs = not args.no_costs
    periods = run_backtest(con, rebalance_dates, use_costs=use_costs,
                            verbose=args.verbose, offset_days=offset_days)
    bt_time = time.time() - t1

    output = build_output(periods)
    print_summary(output)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s "
          f"(fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    if args.output:
        out_dir = os.path.dirname(args.output) or "."
        os.makedirs(out_dir, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
