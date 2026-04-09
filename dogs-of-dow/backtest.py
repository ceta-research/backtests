#!/usr/bin/env python3
"""
Dogs of the Dow Backtest

Annual rebalancing, equal weight, top 10 by highest dividend yield.
US: Dow 30 membership (dowjones_constituent table).
Other exchanges: Top 30 by market cap as "blue chip" universe, pick top 10 by yield.

Signal: Highest dividend yield within blue-chip universe
Portfolio: 10 stocks, equal weight. Cash if < 5 qualify.
Rebalancing: Annual (January), 2000-2025.

Usage:
    # Backtest US (true Dogs of the Dow)
    python3 dogs-of-dow/backtest.py

    # Backtest Indian stocks (high yield blue chips)
    python3 dogs-of-dow/backtest.py --preset india

    # Backtest all exchanges
    python3 dogs-of-dow/backtest.py --global --output results/exchange_comparison.json --verbose

See README.md for strategy details.
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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns, get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS, remove_price_oscillations
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import add_common_args, resolve_exchanges, print_header, get_mktcap_threshold

# --- Signal parameters ---
DOGS_COUNT = 10          # Top 10 by yield
BLUECHIP_COUNT = 30      # Blue-chip universe size (non-US)
MIN_STOCKS = 5           # Hold cash if fewer qualify
DEFAULT_FREQUENCY = "annual"
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold()


def is_us_exchange(exchanges):
    """Check if running on US exchanges (use true Dow 30)."""
    if exchanges is None:
        return False
    us_codes = {"NYSE", "NASDAQ", "AMEX"}
    return set(exchanges).issubset(us_codes) and len(set(exchanges) & us_codes) > 0


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    For US: also fetches Dow 30 membership.
    For non-US: fetches market cap data for blue-chip selection.

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    use_dow = is_us_exchange(exchanges)

    # 1. Universe
    if use_dow:
        print("  Fetching Dow 30 membership...")
        dow_sql = "SELECT DISTINCT symbol FROM dowjones_constituent WHERE symbol IS NOT NULL"
        dow_rows = client.query(dow_sql, verbose=verbose)
        if not dow_rows:
            print("  No Dow 30 data found.")
            return None
        sym_values = ",".join(f"('{r['symbol']}')" for r in dow_rows)
        con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")
        print(f"  Universe: Dow 30 ({len(dow_rows)} members)")
    else:
        print("  Fetching exchange membership...")
        profile_sql = f"SELECT DISTINCT symbol, exchange FROM profile {exchange_where}"
        profiles = client.query(profile_sql, verbose=verbose)
        if not profiles:
            print("  No symbols found for these exchanges.")
            return None
        print(f"  Universe: {len(profiles)} symbols")
        sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
        con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter_sql = "1=1"

    if use_dow:
        sym_filter_sql = "symbol IN (SELECT symbol FROM dowjones_constituent)"

    # 2. Market cap data (for blue-chip selection on non-US, and for cost tiers)
    print("  Fetching key metrics (market cap)...")
    metrics_sql = f"""
        SELECT symbol, marketCap, dateEpoch as filing_epoch, period
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache", verbose=verbose)
    print(f"    -> {count} rows")

    # 3. Dividend yield data (FY for historical screening)
    print("  Fetching financial ratios (dividend yield)...")
    ratios_sql = f"""
        SELECT symbol, dividendYield, dividendPerShare, dateEpoch as filing_epoch, period
        FROM financial_ratios
        WHERE period = 'FY' AND {sym_filter_sql}
    """
    count = query_parquet(client, ratios_sql, con, "ratios_cache", verbose=verbose)
    print(f"    -> {count} rows")

    # 4. Prices (at rebalance dates)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    if use_dow:
        symbol_source = "symbol IN (SELECT DISTINCT symbol FROM dowjones_constituent WHERE symbol IS NOT NULL)"
    elif exchanges:
        symbol_source = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        symbol_source = "symbol IN (SELECT DISTINCT symbol FROM key_metrics WHERE period = 'FY')"

    # Build benchmark symbol list (SPY + local index if applicable)
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(bench_symbols)

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR {symbol_source}
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def screen_dogs(con, target_date, use_dow=True, mktcap_min=1_000_000_000):
    """Screen for Dogs (top yielders in blue-chip universe).

    For US (use_dow=True): Rank all Dow 30 members by yield, pick top 10.
    For non-US (use_dow=False): Take top 30 by market cap, rank by yield, pick top 10.

    Returns list of (symbol, market_cap) tuples.
    """
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())

    if use_dow:
        # Dogs of the Dow: rank Dow 30 by yield, pick top 10
        rows = con.execute("""
            WITH latest_ratios AS (
                SELECT symbol, dividendYield, filing_epoch,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
                FROM ratios_cache
                WHERE filing_epoch <= ?
                  AND dividendYield IS NOT NULL AND dividendYield > 0
            ),
            latest_metrics AS (
                SELECT symbol, marketCap,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
                FROM metrics_cache WHERE filing_epoch <= ?
            )
            SELECT r.symbol, COALESCE(m.marketCap, 100000000000) as marketCap
            FROM latest_ratios r
            JOIN universe u ON r.symbol = u.symbol
            LEFT JOIN latest_metrics m ON r.symbol = m.symbol AND m.rn = 1
            WHERE r.rn = 1
            ORDER BY r.dividendYield DESC
            LIMIT ?
        """, [cutoff_epoch, cutoff_epoch, DOGS_COUNT]).fetchall()
    else:
        # High Yield Blue Chips: top 30 by market cap, then top 10 by yield
        rows = con.execute("""
            WITH latest_metrics AS (
                SELECT symbol, marketCap,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
                FROM metrics_cache
                WHERE filing_epoch <= ? AND marketCap IS NOT NULL
            ),
            bluechips AS (
                SELECT m.symbol, m.marketCap
                FROM latest_metrics m
                JOIN universe u ON m.symbol = u.symbol
                WHERE m.rn = 1 AND m.marketCap >= ?
                ORDER BY m.marketCap DESC
                LIMIT ?
            ),
            latest_ratios AS (
                SELECT symbol, dividendYield,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
                FROM ratios_cache
                WHERE filing_epoch <= ?
                  AND dividendYield IS NOT NULL AND dividendYield > 0
            )
            SELECT b.symbol, b.marketCap
            FROM bluechips b
            JOIN latest_ratios r ON b.symbol = r.symbol AND r.rn = 1
            ORDER BY r.dividendYield DESC
            LIMIT ?
        """, [cutoff_epoch, mktcap_min, BLUECHIP_COUNT, cutoff_epoch, DOGS_COUNT]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, use_dow=True, use_costs=True, verbose=False,
                 mktcap_min=1_000_000_000, offset_days=1, benchmark_symbol="SPY"):
    """Run Dogs backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_dogs(con, entry_date, use_dow=use_dow, mktcap_min=mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            bench_return = get_benchmark_return(
                con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": bench_return,
                "stocks_held": 0,
                "holdings": f"CASH ({len(portfolio)} passed)",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _ in portfolio]
        mcaps = {s: mc for s, mc in portfolio}

        entry_prices = get_prices(con, symbols, entry_date, offset_days=offset_days)
        exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)

        # Compute returns with data quality filtering
        symbol_returns = []
        for sym in symbols:
            ep = entry_prices.get(sym)
            xp = exit_prices.get(sym)
            symbol_returns.append((sym, ep, xp, mcaps.get(sym)))

        clean_returns, skipped = filter_returns(symbol_returns, verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean_returns:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        bench_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(bench_return, 6) if bench_return is not None else None,
            "stocks_held": len(returns),
            "holdings": ",".join(symbols),
        })

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Dogs of the Dow backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("dogs-of-dow", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    use_dow = is_us_exchange(exchanges)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    from cli_utils import get_risk_free_rate
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)

    offset_days = 0 if args.no_next_day else 1
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "Same-day close (legacy)" if offset_days == 0 else "Next-day close (MOC)"

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    if use_dow:
        signal_desc = f"Dow 30, top {DOGS_COUNT} by dividend yield"
    else:
        signal_desc = f"Top {BLUECHIP_COUNT} by MCap > {mktcap_threshold/1e9:.0f}B local, top {DOGS_COUNT} by dividend yield"
    print_header("DOGS OF THE DOW BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available. Exiting.")
        sys.exit(1)
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, use_dow=use_dow,
                           use_costs=use_costs, verbose=args.verbose,
                           mktcap_min=mktcap_threshold,
                           offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    # Phase 3: Compute metrics
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)

    label = "Dogs" if use_dow else "HY BluChp"
    print(format_metrics(metrics, label, benchmark_name))

    # Portfolio metadata
    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    # Annual returns
    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        bench_col = benchmark_name[:10]
        print(f"\n  {'Year':<8} {label:>10} {bench_col:>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    # Save results
    if args.output:
        p = metrics["portfolio"]
        b = metrics["benchmark"]
        c = metrics["comparison"]

        def pct(v):
            return round(v * 100, 2) if v is not None else None

        def rnd(v, d=3):
            return round(v, d) if v is not None else None

        def format_series(s):
            return {
                "total_return": pct(s.get("total_return")),
                "cagr": pct(s.get("cagr")),
                "max_drawdown": pct(s.get("max_drawdown")),
                "annualized_volatility": pct(s.get("annualized_volatility")),
                "sharpe_ratio": rnd(s.get("sharpe_ratio")),
                "sortino_ratio": rnd(s.get("sortino_ratio")),
                "calmar_ratio": rnd(s.get("calmar_ratio")),
                "var_95": pct(s.get("var_95")),
                "max_consecutive_losses": s.get("max_consecutive_losses"),
                "pct_negative_periods": pct(s.get("pct_negative_periods")),
            }

        output = {
            "universe": universe_name,
            "strategy": "Dogs of the Dow" if use_dow else "High Yield Blue Chips",
            "benchmark_symbol": benchmark_symbol,
            "benchmark_name": benchmark_name,
            "execution": exec_model,
            "n_periods": len(valid),
            "years": round(len(valid) / periods_per_year, 1),
            "frequency": frequency,
            "cash_periods": cash_periods,
            "invested_periods": len(valid) - cash_periods,
            "avg_stocks_when_invested": round(avg_stocks, 1),
            "portfolio": format_series(p),
            "spy": format_series(b),
            "comparison": {
                "excess_cagr": pct(c.get("excess_cagr")),
                "win_rate": pct(c.get("win_rate")),
                "information_ratio": rnd(c.get("information_ratio")),
                "tracking_error": pct(c.get("tracking_error")),
                "up_capture": pct(c.get("up_capture")),
                "down_capture": pct(c.get("down_capture")),
                "beta": rnd(c.get("beta")),
                "alpha": pct(c.get("alpha")),
            },
            "excess_cagr": pct(c.get("excess_cagr")),
            "win_rate_vs_spy": pct(c.get("win_rate")),
            "annual_returns": [
                {"year": ar["year"],
                 "portfolio": round(ar["portfolio"] * 100, 2),
                 "spy": round(ar["benchmark"] * 100, 2),
                 "excess": round(ar["excess"] * 100, 2)}
                for ar in annual
            ],
        }
        os.makedirs(os.path.dirname(args.output), exist_ok=True) if os.path.dirname(args.output) else None
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
