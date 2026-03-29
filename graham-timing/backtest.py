#!/usr/bin/env python3
"""
Graham Number Timing Multi-Exchange Backtest

Benjamin Graham's intrinsic value formula as a timing signal. Backtested 2000-2025.

Signal: Price < Graham Number, where Graham Number = sqrt(22.5 × EPS × BVPS)
        Plus quality filters: ROE > 10%, positive earnings, positive equity
Portfolio: Top 30 by deepest discount to Graham Number. Equal weight. Cash if < 10.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2000-2025.

Usage:
    # Backtest US stocks (default)
    python3 graham-timing/backtest.py

    # Backtest Indian stocks
    python3 graham-timing/backtest.py --exchange BSE,NSE

    # Backtest all exchanges
    python3 graham-timing/backtest.py --global

    # Custom parameters
    python3 graham-timing/backtest.py --frequency semi-annual --verbose

See README.md for details.
"""

import argparse
import duckdb
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns, get_local_benchmark, get_benchmark_return
from metrics import compute_metrics as _compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost
from cli_utils import add_common_args, resolve_exchanges, print_header, get_mktcap_threshold

# --- Signal parameters ---
ROE_MIN = 0.10
MIN_STOCKS = 10
MAX_STOCKS = 30  # Top 30 most undervalued
DEFAULT_FREQUENCY = "quarterly"

# Presets to run for --global
PRESETS_TO_RUN = [
    ("us", ["NYSE", "NASDAQ", "AMEX"]),
    ("india", ["NSE"]),
    ("germany", ["XETRA"]),
    ("china", ["SHZ", "SHH"]),
    ("hongkong", ["HKSE"]),
    ("korea", ["KSC"]),
    ("canada", ["TSX"]),
    ("thailand", ["SET"]),
    ("taiwan", ["TAI"]),
    ("japan", ["JPX"]),
    ("uk", ["LSE"]),
    ("switzerland", ["SIX"]),
    ("sweden", ["STO"]),
    ("indonesia", ["JKT"]),
]

def fetch_data_via_api(client, exchanges, rebalance_dates, benchmark_symbol="SPY", verbose=False):
    """Fetch historical financial data and load into DuckDB.

    Populates tables:
        universe, metrics_cache, ratios_cache, income_cache, balance_cache, prices_cache
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where_direct = f"WHERE exchange IN ({ex_filter})"
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        exchange_where_direct = ""
        sym_filter_sql = "1=1"

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange FROM profile {exchange_where_direct}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found.")
        return None
    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2-5: Financial data
    queries = [
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY' AND returnOnEquity IS NOT NULL AND {sym_filter_sql}
        """, "key metrics"),
        ("income_cache", f"""
            SELECT symbol, netIncome, revenue, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "income statements"),
        ("balance_cache", f"""
            SELECT symbol, totalStockholdersEquity, dateEpoch as filing_epoch, period
            FROM balance_sheet
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "balance sheets"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              memory_mb=4096, threads=2)
        print(f"    -> {count} rows")

    # 6. Prices (at rebalance dates + 10-day window)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    from data_utils import LOCAL_INDEX_BENCHMARKS
    bench_symbols = {"'SPY'", f"'{benchmark_symbol}'"}
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
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
                INTERSECT
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache", verbose=verbose,
                          limit=5000000, timeout=600, memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for stocks trading below Graham Number.

    Graham Number = sqrt(22.5 × EPS × BVPS)
    Where:
        EPS = Net Income / Shares Outstanding
        BVPS = Total Equity / Shares Outstanding
        Shares Outstanding ≈ Market Cap / Current Price

    Returns list of (symbol, price_to_graham_ratio, market_cap) sorted by ratio ASC.
    """
    # Convert date to datetime for timestamp() method
    target_dt = datetime.combine(target_date, datetime.min.time())
    cutoff_epoch = int((target_dt - timedelta(days=45)).timestamp())
    prev_year_epoch = int((target_dt - timedelta(days=445)).timestamp())

    # Price window epochs
    price_start_epoch = int(target_dt.timestamp())
    price_end_epoch = int((target_dt + timedelta(days=10)).timestamp())

    # CRITICAL: Each table uses its own ROW_NUMBER() independently.
    # Do NOT join by filing_epoch across tables — income_statement uses
    # SEC filing dates while key_metrics uses fiscal year-end dates.
    # Join by symbol only, taking the latest valid row per table.
    rows = con.execute("""
        WITH inc AS (
            SELECT symbol, netIncome, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        bal AS (
            SELECT symbol, totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        met AS (
            SELECT symbol, marketCap, returnOnEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        prices AS (
            SELECT symbol, adjClose AS price,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch DESC) AS rn
            FROM prices_cache
            WHERE trade_epoch BETWEEN ? AND ?
        )
        SELECT
            i.symbol,
            i.netIncome,
            b.totalStockholdersEquity,
            m.marketCap,
            m.returnOnEquity,
            p.price
        FROM inc i
        JOIN bal b ON i.symbol = b.symbol AND b.rn = 1
        JOIN met m ON i.symbol = m.symbol AND m.rn = 1
        JOIN prices p ON i.symbol = p.symbol AND p.rn = 1
        WHERE i.rn = 1
          AND i.netIncome > 0
          AND b.totalStockholdersEquity > 0
          AND m.returnOnEquity > ?
          AND m.marketCap > ?
          AND p.price > 0
    """, [cutoff_epoch, prev_year_epoch,
          cutoff_epoch, prev_year_epoch,
          cutoff_epoch,
          price_start_epoch, price_end_epoch,
          ROE_MIN, mktcap_min]).fetchall()

    results = []
    for symbol, net_income, equity, mkt_cap, roe, price in rows:
        # Compute shares outstanding from market cap and price
        shares = mkt_cap / price if price > 0 else 0
        if shares <= 0:
            continue

        # Compute EPS and BVPS
        eps = net_income / shares
        bvps = equity / shares

        if eps <= 0 or bvps <= 0:
            continue

        # Graham Number = sqrt(22.5 × EPS × BVPS)
        graham_number = (22.5 * eps * bvps) ** 0.5

        if graham_number <= 0:
            continue

        # Price-to-Graham ratio (< 1.0 means undervalued)
        price_to_graham = price / graham_number

        if price_to_graham < 1.0:  # Only stocks below Graham Number
            results.append((symbol, price_to_graham, mkt_cap))

    # Sort by price-to-graham ASC (most undervalued first)
    results.sort(key=lambda x: x[1])
    return results[:MAX_STOCKS]  # Top 30


def run_backtest(exchanges, start_year=2000, end_year=2025, frequency=DEFAULT_FREQUENCY,
                 apply_costs=True, risk_free_rate=None, verbose=False,
                 offset_days=1, benchmark_symbol="SPY", benchmark_name="S&P 500"):
    """Run Graham Number timing backtest on specified exchanges."""

    exec_model = "same-day close" if offset_days == 0 else "next-day close (MOC)"
    print(f"\n{'='*70}")
    print(f"Graham Number Timing Backtest")
    print(f"Exchanges: {', '.join(exchanges)}")
    print(f"Period: {start_year}-{end_year} ({frequency} rebalancing)")
    print(f"Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"{'='*70}\n")

    client = CetaResearch()
    rebalance_dates = generate_rebalance_dates(start_year, end_year, frequency)

    # Get exchange-specific market cap threshold and risk-free rate
    mktcap_min = get_mktcap_threshold(exchanges)
    if risk_free_rate is None:
        risk_free_rate = 0.02  # Default 2%

    print(f"Market cap threshold: {mktcap_min:,.0f} (local currency)")
    print(f"Risk-free rate: {risk_free_rate*100:.1f}%\n")

    con = fetch_data_via_api(client, exchanges, rebalance_dates, benchmark_symbol=benchmark_symbol, verbose=verbose)
    if not con:
        return None

    print(f"\n{'='*70}")
    print(f"Running backtest ({len(rebalance_dates)} periods)...")
    print(f"{'='*70}\n")

    portfolio_returns = []
    spy_returns = []
    period_data = []

    for i, target_date in enumerate(rebalance_dates[:-1]):
        exit_date = rebalance_dates[i + 1]

        if verbose or (i % 10 == 0):
            print(f"  Period {i+1}/{len(rebalance_dates)-1}: {target_date.isoformat()} -> {exit_date.isoformat()}")

        # Screen stocks
        stocks = screen_stocks(con, target_date, mktcap_min)

        if len(stocks) < MIN_STOCKS:
            # Cash period
            portfolio_returns.append(0.0)
            period_data.append({
                "rebalance_date": target_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "stocks_held": 0,
                "holdings": "CASH",
                "portfolio_return": 0.0,
            })
            if verbose:
                print(f"    -> CASH (only {len(stocks)} stocks qualify, need {MIN_STOCKS})")
        else:
            # Get prices
            symbols = [s[0] for s in stocks]
            entry_prices = get_prices(con, symbols, target_date, offset_days=offset_days)
            exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)

            # Compute raw returns: (symbol, entry_price, exit_price, market_cap)
            raw_returns = []
            for symbol, _, mkt_cap in stocks:
                entry = entry_prices.get(symbol)
                exit_ = exit_prices.get(symbol)
                if entry and exit_ and entry > 0:
                    raw_returns.append((symbol, entry, exit_, mkt_cap))

            # filter_returns returns (clean_list, skipped_list)
            # clean_list contains (symbol, raw_return_fraction, market_cap)
            clean_returns, _ = filter_returns(raw_returns, verbose=verbose)

            if len(clean_returns) >= MIN_STOCKS:
                # Apply transaction costs and average
                total_return = 0.0
                for symbol, raw_return, mkt_cap in clean_returns:
                    if apply_costs:
                        cost_rate = tiered_cost(mkt_cap)
                        net_return = raw_return - (2 * cost_rate)  # Round-trip
                    else:
                        net_return = raw_return
                    total_return += net_return

                period_return = total_return / len(clean_returns)
                portfolio_returns.append(period_return)

                period_data.append({
                    "rebalance_date": target_date.isoformat(),
                    "exit_date": exit_date.isoformat(),
                    "stocks_held": len(clean_returns),
                    "holdings": ",".join([s[0] for s in clean_returns[:10]]) + ("..." if len(clean_returns) > 10 else ""),
                    "portfolio_return": round(period_return * 100, 2),
                })

                if verbose:
                    print(f"    -> {len(clean_returns)} stocks, return: {period_return*100:+.2f}%")
            else:
                # Not enough clean data after quality filter
                portfolio_returns.append(0.0)
                period_data.append({
                    "rebalance_date": target_date.isoformat(),
                    "exit_date": exit_date.isoformat(),
                    "stocks_held": 0,
                    "holdings": "CASH (data quality filter)",
                    "portfolio_return": 0.0,
                })

        # Local benchmark
        bench_return = get_benchmark_return(
            con, benchmark_symbol, target_date, exit_date, offset_days=offset_days)
        spy_returns.append(bench_return if bench_return is not None else 0.0)
        if bench_return is None and verbose:
            print(f"    {benchmark_symbol}: Missing price data")

    con.close()

    # Compute metrics
    n_periods = len(portfolio_returns)
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    cash_periods = sum(1 for pd in period_data if pd["stocks_held"] == 0)
    invested_periods = n_periods - cash_periods
    avg_stocks = sum(pd["stocks_held"] for pd in period_data if pd["stocks_held"] > 0)
    avg_stocks_when_invested = avg_stocks / invested_periods if invested_periods > 0 else 0

    years = n_periods / periods_per_year

    metrics = _compute_metrics(
        portfolio_returns, spy_returns,
        periods_per_year=periods_per_year,
        risk_free_rate=risk_free_rate
    )

    # Convert dates to ISO strings for compute_annual_returns
    period_dates = [d.isoformat() for d in rebalance_dates[:-1]]
    annual_returns = compute_annual_returns(
        portfolio_returns, spy_returns, period_dates,
        periods_per_year
    )

    result = {
        "universe": "+".join(sorted(set(exchanges))),
        "frequency": frequency,
        "n_periods": n_periods,
        "years": round(years, 1),
        "cash_periods": cash_periods,
        "invested_periods": invested_periods,
        "avg_stocks_when_invested": round(avg_stocks_when_invested, 1),
        "portfolio": metrics["portfolio"],
        "spy": metrics["benchmark"],
        "comparison": metrics["comparison"],
        "annual_returns": annual_returns,
        "period_data": period_data,
    }

    return result


def main():
    parser = argparse.ArgumentParser(description="Graham Number Timing Backtest")
    add_common_args(parser)
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=2025)
    args = parser.parse_args()

    offset_days = 0 if args.no_next_day else 1

    if args.global_bt:
        # Run all presets
        all_results = {}
        for preset_name, preset_exchanges in PRESETS_TO_RUN:
            print(f"\n{'#'*70}")
            print(f"# Running preset: {preset_name.upper()}")
            print(f"{'#'*70}")

            bsym, bname = get_local_benchmark(preset_exchanges)
            result = run_backtest(
                preset_exchanges,
                start_year=args.start_year,
                end_year=args.end_year,
                frequency=args.frequency or DEFAULT_FREQUENCY,
                apply_costs=not args.no_costs,
                risk_free_rate=args.risk_free_rate,
                verbose=args.verbose,
                offset_days=offset_days,
                benchmark_symbol=bsym,
                benchmark_name=bname,
            )

            if result:
                all_results[result["universe"]] = result
                print(f"\nCompleted {preset_name}: {result['portfolio']['cagr']:.2f}% CAGR")
            else:
                print(f"\nSkipped {preset_name}: No data")

        # Save results
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nSaved results to: {args.output}")

            # Print summary
            print(f"\n{'='*70}")
            print(f"SUMMARY: {len(all_results)} exchanges tested")
            print(f"{'='*70}\n")
            print(format_metrics(all_results))
    else:
        # Single exchange run
        exchanges, universe_name = resolve_exchanges(args)
        bsym, bname = get_local_benchmark(exchanges)
        result = run_backtest(
            exchanges,
            start_year=args.start_year,
            end_year=args.end_year,
            frequency=args.frequency or DEFAULT_FREQUENCY,
            apply_costs=not args.no_costs,
            risk_free_rate=args.risk_free_rate,
            verbose=args.verbose,
            offset_days=offset_days,
            benchmark_symbol=bsym,
            benchmark_name=bname,
        )

        if result:
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump({result["universe"]: result}, f, indent=2)
                p = result['portfolio']
                s = result['spy']
                c = result['comparison']
                print(f"\n{'='*70}")
                print(f"✅ BACKTEST COMPLETE: {result['universe']}")
                print(f"{'='*70}")
                print(f"  CAGR:      {p['cagr']*100:>7.2f}%  ({bname}: {s['cagr']*100:.2f}%)")
                print(f"  Excess:    {c['excess_cagr']*100:>+7.2f}%")
                print(f"  Sharpe:    {(p['sharpe_ratio'] or 0):>7.3f}")
                print(f"  Max DD:    {p['max_drawdown']*100:>7.2f}%")
                print(f"  Down Cap:  {(c['down_capture'] or 0)*100:>7.1f}%")
                print(f"  Cash:      {result['cash_periods']}/{result['n_periods']} periods")
                print(f"  Avg Stks:  {result['avg_stocks_when_invested']:.1f}")
                print(f"\nResults saved to: {args.output}")
            else:
                print(f"\n✅ Backtest complete. Use --output to save results.")


if __name__ == "__main__":
    main()
