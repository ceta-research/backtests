#!/usr/bin/env python3
"""
OCF Growth / Cash Flow Momentum Strategy Backtest

Annual rebalancing (July), equal weight, top 30 by highest OCF-earnings divergence.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: OCF Growth > 10%, OCF Growth > NI Growth (positive divergence),
        ROE > 10%, Operating Margin > 5%, Market Cap > local-currency threshold
Portfolio: Top 30 by divergence (OCF growth - NI growth), equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), 2000-2025.

Academic reference:
  Chan, K. et al. (2006) "Earnings Quality and Stock Returns" Journal of Business 79(3).
  Sloan, R. (1996) "Do Stock Prices Fully Reflect Information in Accruals and Cash Flows?"
  Dechow, P. (1994) "Accounting Earnings and Cash Flows as Measures of Firm Performance"

Usage:
    python3 ocf-growth/backtest.py
    python3 ocf-growth/backtest.py --preset india
    python3 ocf-growth/backtest.py --global --output results/exchange_comparison.json --verbose
    python3 ocf-growth/backtest.py --no-costs

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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns, get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, save_results, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
OCF_GROWTH_MIN = 0.10      # OCF growth > 10% YoY
OCF_GROWTH_MAX = 5.0       # Cap at 500% to exclude recovery-from-zero artifacts
ROE_MIN = 0.10             # Return on equity > 10%
OPM_MIN = 0.05             # Operating profit margin > 5%
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (annual FY filings available, 45-day lag)

# Presets to skip in global mode (data quality or covered by broader preset)
EXCLUDED_PRESETS = {
    "brazil",       # SAO: adjClose split artifacts
    "australia",    # ASX: adjClose split artifacts
    "nyse",         # Covered by "us" preset
    "nasdaq",       # Covered by "us" preset
}


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        ocf_growth_cache(symbol, growthOCF, growthFCF, filing_epoch, period)
        ni_growth_cache(symbol, growthNI, filing_epoch, period)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, operatingProfitMargin, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
    else:
        exchange_where = ""

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

    if exchanges:
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter_sql = "1=1"

    # 2. Cash flow growth (FY)
    queries = [
        ("ocf_growth_cache", f"""
            SELECT symbol, growthOperatingCashFlow as growthOCF,
                   growthFreeCashFlow as growthFCF,
                   dateEpoch as filing_epoch, period
            FROM cash_flow_statement_growth
            WHERE period = 'FY'
              AND growthOperatingCashFlow IS NOT NULL
              AND {sym_filter_sql}
        """, "cash flow growth (OCF, FCF)"),
        # 3. Income statement growth (FY)
        ("ni_growth_cache", f"""
            SELECT symbol, growthNetIncome as growthNI,
                   dateEpoch as filing_epoch, period
            FROM income_statement_growth
            WHERE period = 'FY'
              AND growthNetIncome IS NOT NULL
              AND {sym_filter_sql}
        """, "income statement growth (NI)"),
        # 4. Key metrics (ROE, market cap)
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap,
                   dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND returnOnEquity IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (ROE, market cap)"),
        # 5. Financial ratios (operating margin)
        ("ratios_cache", f"""
            SELECT symbol, operatingProfitMargin,
                   dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND operatingProfitMargin IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (operating margin)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 6. Prices (only at rebalance dates + 10-day window)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    # Build benchmark symbol list (SPY + local index)
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
                SELECT DISTINCT symbol FROM cash_flow_statement_growth WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for OCF momentum stocks with positive divergence.
    Returns list of (symbol, market_cap, divergence) tuples."""
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH ocf AS (
            SELECT symbol, growthOCF, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ocf_growth_cache WHERE filing_epoch <= ?
        ),
        ni AS (
            SELECT symbol, growthNI, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ni_growth_cache WHERE filing_epoch <= ?
        ),
        m AS (
            SELECT symbol, returnOnEquity, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        r AS (
            SELECT symbol, operatingProfitMargin, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        )
        SELECT ocf.symbol, m.marketCap, (ocf.growthOCF - ni.growthNI) as divergence
        FROM ocf
        JOIN ni ON ocf.symbol = ni.symbol AND ni.rn = 1
        JOIN m ON ocf.symbol = m.symbol AND m.rn = 1
        JOIN r ON ocf.symbol = r.symbol AND r.rn = 1
        WHERE ocf.rn = 1
          AND ocf.growthOCF > ?
          AND ocf.growthOCF < ?
          AND ocf.growthOCF > ni.growthNI
          AND m.returnOnEquity > ?
          AND r.operatingProfitMargin > ?
          AND m.marketCap > ?
        ORDER BY (ocf.growthOCF - ni.growthNI) DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, cutoff_epoch,
          OCF_GROWTH_MIN, OCF_GROWTH_MAX, ROE_MIN, OPM_MIN, mktcap_min, MAX_STOCKS]).fetchall()

    return [(r[0], r[1], r[2]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run OCF growth backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

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

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _ in portfolio}

        entry_prices = get_prices(con, symbols, entry_date, offset_days=offset_days)
        exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)

        # Collect raw data for filter_returns (caps artifacts at 200%)
        raw_data = []
        for sym in symbols:
            ep = entry_prices.get(sym)
            xp = exit_prices.get(sym)
            if ep and xp and ep > 0:
                raw_data.append((sym, ep, xp, mcaps.get(sym)))

        clean, skipped = filter_returns(raw_data, verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean:
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
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def main():
    parser = argparse.ArgumentParser(description="OCF Growth / Cash Flow Momentum backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("ocf-growth", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    offset_days = 0 if args.no_next_day else 1
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)

    signal_desc = (f"OCF Growth {OCF_GROWTH_MIN*100:.0f}-{OCF_GROWTH_MAX*100:.0f}%, "
                   f"Divergence > 0, ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, "
                   f"MCap > {mktcap_threshold/1e9:.0f}B local, top {MAX_STOCKS}")
    exec_model = "same-day close" if args.no_next_day else "next-day open (MOC)"
    print_header("OCF GROWTH BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    if exchanges is None:
        all_results = {}
        for preset_name, preset in EXCHANGE_PRESETS.items():
            ex_list = preset["exchanges"]
            key = preset["name"]
            if preset_name in EXCLUDED_PRESETS:
                print(f"\nSkipping {key} ({', '.join(ex_list)}) - excluded")
                continue
            if key in all_results:
                continue

            print(f"\n{'='*65}")
            print(f"  Running: {key} ({', '.join(ex_list)})")
            print(f"{'='*65}")
            single_result = _run_single(args, ex_list, key, frequency, use_costs,
                                        verbose=args.verbose, offset_days=offset_days)
            if single_result:
                all_results[key] = single_result

        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n  Global results saved to {args.output}")
        return

    result = _run_single(args, exchanges, universe_name, frequency, use_costs,
                         verbose=args.verbose, output_path=args.output, offset_days=offset_days)


def _run_single(args, exchanges, universe_name, frequency, use_costs,
                verbose=False, output_path=None, offset_days=1):
    """Run backtest for a single exchange set. Returns result dict or None."""
    risk_free_rate = get_risk_free_rate(exchanges, getattr(args, 'risk_free_rate', None))
    mktcap_threshold = get_mktcap_threshold(exchanges)
    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)

    cr = CetaResearch(api_key=getattr(args, 'api_key', None),
                      base_url=getattr(args, 'base_url', None))

    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency,
                                               months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose)
    if con is None:
        print("  No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\n  Data fetched in {fetch_time:.0f}s")

    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs,
                           verbose=verbose, offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1
    print(f"  Backtest completed in {bt_time:.0f}s")

    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("  No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    bench_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, bench_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)

    print(format_metrics(metrics, "OCF Growth", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, bench_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'OCF Growth':>10} {benchmark_name:>12} {'Excess':>10}")
        print("  " + "-" * 42)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>11.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

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
        "benchmark_symbol": benchmark_symbol,
        "benchmark_name": benchmark_name,
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

    if output_path:
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


if __name__ == "__main__":
    main()
