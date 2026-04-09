#!/usr/bin/env python3
"""
Revenue Surprise Momentum Backtest

Quarterly rebalancing (Jan/Apr/Jul/Oct), equal weight, top 30 by highest revenue surprise.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Actual quarterly revenue > analyst consensus estimate (positive surprise),
        0% < surprise < 50%, ROE > 8%, D/E < 2.5, MCap > local-currency threshold
Portfolio: Top 30 by highest revenue surprise %, equal weight. Cash if < 10 qualify.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2000-2024.

Revenue surprise momentum has short-term persistence: quarterly rebalancing captures
the effect before it fades. Annual rebalancing misses the drift because surprises
decay within months, not years.

Academic reference: Jegadeesh & Livnat (2006) "Revenue Surprises and Stock Returns",
Journal of Accounting and Economics 41(1-2), 147-166.

Usage:
    # Backtest US stocks (default)
    python3 revenue-surprise/backtest.py

    # Backtest Indian stocks
    python3 revenue-surprise/backtest.py --preset india

    # Backtest all exchanges (loop)
    python3 revenue-surprise/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 revenue-surprise/backtest.py --no-costs

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
from data_utils import (query_parquet, get_prices, generate_rebalance_dates, filter_returns,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
REV_SURPRISE_MIN = 0.00    # Revenue surprise > 0% (beat estimates)
REV_SURPRISE_MAX = 0.50    # Revenue surprise < 50% (exclude data artifacts)
ROE_MIN = 0.08             # Return on equity > 8%
DE_MAX = 2.5               # Debt-to-equity < 2.5
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]  # Quarterly: Jan/Apr/Jul/Oct
MAX_SINGLE_RETURN = 2.0    # Cap individual stock returns at 200% (data quality guard)
MIN_ENTRY_PRICE = 1.0      # Skip stocks with entry price < $1 (price data artifact)
DATE_MATCH_WINDOW = 7776000  # 90 days in seconds - tolerance for matching quarter date to estimate date


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        revenue_cache(symbol, actual_revenue, est_revenue, filing_epoch)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, debtToEquityRatio, filing_epoch)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
        sym_in_filter = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        sym_in_filter_i = f"i.symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        exchange_where = ""
        sym_in_filter = "1=1"
        sym_in_filter_i = "1=1"

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange FROM profile {exchange_where}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2. Revenue surprise: join quarterly income_statement with analyst_estimates
    # Match quarterly periods (Q1/Q2/Q3/Q4) with analyst quarter estimates.
    print("  Fetching quarterly revenue data (income_statement + analyst_estimates)...")
    revenue_sql = f"""
        SELECT i.symbol,
               i.revenue AS actual_revenue,
               a.revenueAvg AS est_revenue,
               CAST(i.dateEpoch AS BIGINT) AS filing_epoch
        FROM income_statement i
        JOIN analyst_estimates a ON i.symbol = a.symbol
            AND a.period = 'quarter'
            AND ABS(CAST(i.dateEpoch AS BIGINT) - CAST(a.dateEpoch AS BIGINT)) <= {DATE_MATCH_WINDOW}
        WHERE i.period IN ('Q1', 'Q2', 'Q3', 'Q4')
          AND i.revenue IS NOT NULL AND i.revenue > 0
          AND a.revenueAvg IS NOT NULL AND a.revenueAvg > 0
          AND (i.revenue - a.revenueAvg) / ABS(a.revenueAvg) > {REV_SURPRISE_MIN}
          AND (i.revenue - a.revenueAvg) / ABS(a.revenueAvg) < {REV_SURPRISE_MAX}
          AND {sym_in_filter_i}
    """
    count = query_parquet(client, revenue_sql, con, "revenue_cache",
                          verbose=verbose, memory_mb=4096, threads=2)
    print(f"    -> {count} rows with valid quarterly revenue surprise data")
    if count == 0:
        print("  No revenue surprise data found. Skipping.")
        con.close()
        return None

    # 3. Key metrics (ROE, market cap) - use FY data (more stable than TTM for point-in-time)
    print("  Fetching key metrics (ROE, market cap)...")
    metrics_sql = f"""
        SELECT symbol, returnOnEquity, marketCap,
               CAST(dateEpoch AS BIGINT) AS filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND returnOnEquity IS NOT NULL
          AND {sym_in_filter}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache",
                          verbose=verbose, memory_mb=4096, threads=2)
    print(f"    -> {count} rows")

    # 4. Financial ratios (D/E) - use FY data
    print("  Fetching financial ratios (D/E)...")
    ratios_sql = f"""
        SELECT symbol, debtToEquityRatio,
               CAST(dateEpoch AS BIGINT) AS filing_epoch
        FROM financial_ratios
        WHERE period = 'FY'
          AND debtToEquityRatio IS NOT NULL
          AND {sym_in_filter}
    """
    count = query_parquet(client, ratios_sql, con, "ratios_cache",
                          verbose=verbose, memory_mb=4096, threads=2)
    print(f"    -> {count} rows")

    # 5. Prices at rebalance dates
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

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
            OR symbol IN (
                SELECT DISTINCT symbol FROM income_statement WHERE period IN ('Q1','Q2','Q3','Q4')
                    {f"AND {sym_in_filter}" if sym_in_filter != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for top revenue surprise stocks.

    Uses point-in-time approach: only data filed before target_date - 45 days.
    Picks the most recent quarterly filing per symbol within that window.
    Returns list of (symbol, market_cap) tuples sorted by revenue_surprise DESC.
    """
    # 45-day lag for point-in-time (quarterly reports typically available 6 weeks after quarter end)
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH rev AS (
            SELECT symbol, actual_revenue, est_revenue,
                   (actual_revenue - est_revenue) / ABS(est_revenue) AS rev_surprise,
                   filing_epoch,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM revenue_cache
            WHERE filing_epoch <= ?
        ),
        km AS (
            SELECT symbol, returnOnEquity, marketCap, filing_epoch,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        fr AS (
            SELECT symbol, debtToEquityRatio, filing_epoch,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ?
        )
        SELECT r.symbol, km.marketCap, r.rev_surprise
        FROM rev r
        JOIN km ON r.symbol = km.symbol AND km.rn = 1
        JOIN fr ON r.symbol = fr.symbol AND fr.rn = 1
        WHERE r.rn = 1
          AND km.returnOnEquity > ?
          AND (fr.debtToEquityRatio IS NULL OR fr.debtToEquityRatio < ?)
          AND km.marketCap > ?
        ORDER BY r.rev_surprise DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch,
          ROE_MIN, DE_MAX, mktcap_min, MAX_STOCKS]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run Revenue Surprise backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            spy_return = get_benchmark_return(
                con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
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

        symbol_data = [(sym, entry_prices.get(sym), exit_prices.get(sym), mcaps.get(sym))
                       for sym in symbols]
        clean, skipped = filter_returns(symbol_data,
                                        min_entry_price=MIN_ENTRY_PRICE,
                                        max_single_return=MAX_SINGLE_RETURN,
                                        verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        spy_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "stocks_held": len(returns),
            "holdings": ",".join(symbols),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, bench={spy_return * 100 if spy_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency,
                 periods_per_year, cash_periods, avg_stocks):
    """Build JSON output in standard format."""
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

    return {
        "universe": universe_name,
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


def run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, verbose, output_path=None,
               offset_days=1, benchmark_symbol="SPY", benchmark_name="S&P 500"):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    signal_desc = (f"Qtly rev surprise {REV_SURPRISE_MIN*100:.0f}%-{REV_SURPRISE_MAX*100:.0f}%, "
                   f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, "
                   f"MCap > {mktcap_label} local, top {MAX_STOCKS}")
    print_header("REVENUE SURPRISE MOMENTUM BACKTEST", universe_name, exchanges, signal_desc)
    exec_model = "same-bar" if offset_days == 0 else "MOC (next-day)"
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency,
                                               months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2000-2024)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    # Phase 3: Compute metrics
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "Revenue Surprise", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'RevSurprise':>13} {benchmark_name[:9]:>10} {'Excess':>10}")
        print("  " + "-" * 45)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>12.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="Revenue Surprise Momentum backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("revenue-surprise", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs

    # --global mode: loop all eligible exchange presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        # Exchanges with quarterly analyst_estimates coverage >= 10 years
        # Excluded: ASX (adjClose), SAO (adjClose), KSC (9yr), STO (9yr), JNB (thin)
        presets_to_run = [
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("india", ["NSE"]),
            ("uk", ["LSE"]),
            ("germany", ["XETRA"]),
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("canada", ["TSX"]),
            ("japan", ["JPX"]),
            ("taiwan", ["TAI", "TWO"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"returns_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            try:
                b_sym, b_name = get_local_benchmark(preset_exchanges)
                b_offset = 0 if args.no_next_day else 1
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, args.verbose, output_path,
                                    offset_days=b_offset, benchmark_symbol=b_sym,
                                    benchmark_name=b_name)
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
        print(f"\n\n{'=' * 80}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 80}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} {'MaxDD':>8} {'Cash%':>8} {'AvgStk':>8}")
        print("-" * 80)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR / NO DATA':>8}")
                continue
            p = r.get("portfolio", {})
            c = r.get("comparison", {})
            n = r.get("n_periods", 0)
            cp = r.get("cash_periods", 0)
            cash_pct = round(cp * 100 / n, 0) if n > 0 else 0
            cagr = p.get("cagr")
            excess = c.get("excess_cagr")
            sharpe = p.get("sharpe_ratio")
            maxdd = p.get("max_drawdown")
            avg = r.get("avg_stocks_when_invested")
            print(f"{uni:<20} {cagr if cagr is not None else 'N/A':>7}% "
                  f"{f'{excess:+.2f}' if excess is not None else 'N/A':>7}% "
                  f"{sharpe if sharpe is not None else 'N/A':>8} "
                  f"{maxdd if maxdd is not None else 'N/A':>7}% "
                  f"{cash_pct:>7.0f}% {avg if avg is not None else 'N/A':>8}")
        print("=" * 80)
        return

    # Single exchange mode
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    offset_days = 0 if args.no_next_day else 1
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, args.verbose, args.output,
               offset_days=offset_days, benchmark_symbol=benchmark_symbol,
               benchmark_name=benchmark_name)


if __name__ == "__main__":
    main()
