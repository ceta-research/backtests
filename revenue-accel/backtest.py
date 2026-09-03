#!/usr/bin/env python3
"""
Revenue Acceleration Growth Backtest

Annual rebalancing (April), equal weight, top 30 by acceleration score.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Revenue YoY growth accelerating (growth_current > growth_prior),
        current growth > 5%, ROE > 10%, D/E < 1.5, MCap > local threshold
Portfolio: Top 30 by acceleration magnitude, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (April), 2000-2025.

Academic references:
  - Chan, Karceski & Lakonishok (1996) "Momentum Strategies", Journal of Finance 51(5).
    Documented that earnings and revenue growth momentum predicts future returns.
  - Lakonishok, Shleifer & Vishny (1994) "Contrarian Investment, Extrapolation, and Risk",
    Journal of Finance 49(5). Analysts underextrapolate fundamental momentum.

Usage:
    # Backtest US stocks (default)
    python3 revenue-accel/backtest.py

    # Backtest Indian stocks
    python3 revenue-accel/backtest.py --preset india

    # Backtest all exchanges
    python3 revenue-accel/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 revenue-accel/backtest.py --no-costs

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
                        entry_buyable,
                        remove_price_oscillations, get_local_benchmark, get_benchmark_return,
                        LOCAL_INDEX_BENCHMARKS)
from metrics import (compute_metrics, compute_annual_returns, format_metrics,
                     period_accounting)
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
REV_GROWTH_MIN = 0.05      # Minimum current revenue growth (5%)
ROE_MIN = 0.10             # Return on equity > 10% (quality filter)
DE_MAX = 1.5               # Debt-to-equity < 1.5
MAX_STOCKS = 30            # Top 30 by acceleration magnitude, equal weight
MIN_STOCKS = 10            # Hold cash if fewer qualify
MAX_SINGLE_RETURN = 2.0    # Cap individual stock returns at 200% (data quality guard)
MIN_ENTRY_PRICE = 1.0      # Skip stocks with entry price < $1 (price data artifact)
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [4]  # April — covers Dec/Mar FY-end companies with 45-day lag


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, exclude_funds=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        income_cache(symbol, revenue, filing_epoch, period)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, debtToEquityRatio, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    # Optional universe guard. Closed-end funds and ETFs report investment income as
    # "revenue", which swings with mark-to-market and dominates an acceleration ranking.
    # Off by default so the published signal is unchanged. isActivelyTrading is
    # deliberately NOT used here: it would drop delisted names retroactively and add
    # survivorship bias to a historical run.
    fund_guard = " AND isFund = false AND isEtf = false" if exclude_funds else ""

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter}){fund_guard}"
        sym_filter_sql = (f"symbol IN (SELECT DISTINCT symbol FROM profile "
                          f"WHERE exchange IN ({ex_filter}){fund_guard})")
    elif exclude_funds:
        exchange_where = "WHERE isFund = false AND isEtf = false"
        sym_filter_sql = ("symbol IN (SELECT DISTINCT symbol FROM profile "
                          "WHERE isFund = false AND isEtf = false)")
    else:
        exchange_where = ""
        sym_filter_sql = "1=1"

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

    # 2-4: Financial data
    queries = [
        ("income_cache", f"""
            SELECT symbol, revenue, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL
              AND revenue > 0
              AND {sym_filter_sql}
        """, "income statements (revenue)"),
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap,
                   dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND returnOnEquity IS NOT NULL
              AND marketCap IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (ROE, market cap)"),
        ("ratios_cache", f"""
            SELECT symbol, debtToEquityRatio,
                   dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND debtToEquityRatio IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (D/E)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 5. Prices (only at rebalance windows)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        # +1 day of slack so the MOC (next-day) entry window is fully covered
        end = d + timedelta(days=11)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    # Benchmark symbols: SPY always, plus the local index for these exchanges
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(sorted(bench_symbols))

    # Filter price symbols using income_statement coverage (cheaper than full universe)
    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR symbol IN (
                SELECT DISTINCT symbol FROM income_statement
                WHERE period = 'FY' AND revenue > 0
                  {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
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
    """Screen for revenue-accelerating stocks.

    Requires 3 consecutive FY revenue filings to compute:
      - growth_current = (rev_t - rev_t1) / rev_t1
      - growth_prior   = (rev_t1 - rev_t2) / rev_t2
      - acceleration   = growth_current - growth_prior

    Selects top MAX_STOCKS by acceleration magnitude with quality filters.
    Returns list of (symbol, market_cap) tuples.
    """
    # 45-day lag for point-in-time accuracy
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())
    # Max 5-year lookback to avoid very stale data
    stale_cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45 + 5 * 365), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        -- All FY revenue filings in the valid window
        inc AS (
            SELECT symbol, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        -- Compute growth rates using 3 consecutive filings
        rev_calc AS (
            SELECT r1.symbol,
                (r1.revenue - r2.revenue) / NULLIF(r2.revenue, 0) AS growth_current,
                (r2.revenue - r3.revenue) / NULLIF(r3.revenue, 0) AS growth_prior,
                (r1.revenue - r2.revenue) / NULLIF(r2.revenue, 0)
                  - (r2.revenue - r3.revenue) / NULLIF(r3.revenue, 0) AS acceleration
            FROM inc r1
            JOIN inc r2 ON r1.symbol = r2.symbol AND r2.rn = 2
            JOIN inc r3 ON r1.symbol = r3.symbol AND r3.rn = 3
            WHERE r1.rn = 1
        ),
        -- Most recent quality metrics (no prior-year requirement)
        met AS (
            SELECT symbol, returnOnEquity, marketCap,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        rat AS (
            SELECT symbol, debtToEquityRatio,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        )
        SELECT rc.symbol, met.marketCap, rc.acceleration
        FROM rev_calc rc
        JOIN met ON rc.symbol = met.symbol AND met.rn = 1
        JOIN rat ON rc.symbol = rat.symbol AND rat.rn = 1
        WHERE rc.growth_current > rc.growth_prior       -- Accelerating
          AND rc.growth_current > ?                      -- Min current growth %
          AND met.returnOnEquity > ?                     -- Quality: ROE filter
          AND rat.debtToEquityRatio >= 0                 -- Must have positive equity
          AND rat.debtToEquityRatio < ?                  -- Quality: D/E filter
          AND met.marketCap > ?                          -- MCap filter
        ORDER BY rc.acceleration DESC
        LIMIT ?
    """, [
        cutoff_epoch, stale_cutoff_epoch,   # inc
        cutoff_epoch,                        # met
        cutoff_epoch, stale_cutoff_epoch,   # rat
        REV_GROWTH_MIN,
        ROE_MIN,
        DE_MAX,
        mktcap_min,
        MAX_STOCKS,
    ]).fetchall()

    return [(r[0], r[1]) for r in rows]  # (symbol, marketCap)


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run Revenue Acceleration backtest. Returns list of period result dicts.

    offset_days=1 executes at the next session's close (market-on-close), so the
    screen is never filled at a price observed on the same bar it was computed from.
    """
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        # Primary benchmark = local index (SPY for US). SPY is also always kept as a
        # secondary, cross-market comparable since it is total-return in USD.
        spy_return = get_benchmark_return(con, benchmark_symbol, entry_date, exit_date,
                                          offset_days=offset_days)
        if benchmark_symbol == "SPY":
            spy_usd_return = spy_return
        else:
            spy_usd_return = get_benchmark_return(con, "SPY", entry_date, exit_date,
                                                  offset_days=offset_days)

        if len(portfolio) < MIN_STOCKS:
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
                "spy_usd_return": spy_usd_return,
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

        # The cash rule has to be re-checked HERE, not just on the screen count.
        # Screening can pass 30 names while only a handful of them have a usable
        # ENTRY price, and averaging 1-4 survivors reports a single stock's year
        # as a diversified portfolio return.
        #
        # Checked on `buyable`, NOT on len(clean). filter_returns also drops a
        # name for a missing EXIT price and for a realised return over
        # MAX_SINGLE_RETURN, neither of which is knowable on the rebalance date.
        # Deciding cash from those is look-ahead and it flatters: a period the
        # strategy really held and really lost gets rewritten as a flat 0.0%. If
        # the book cleared the floor at entry the strategy ENTERED, so the
        # exit-side survivors are averaged below exactly as they were before this
        # guard existed. That survivor-averaging has its own known weakness; it
        # is pre-existing, logged in DATA_QUALITY_ISSUES.md, and deliberately
        # not changed here.
        buyable = entry_buyable(symbol_data, min_entry_price=MIN_ENTRY_PRICE)
        if buyable < MIN_STOCKS:
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
                "spy_usd_return": spy_usd_return,
                "stocks_held": 0,
                "holdings": f"CASH ({buyable} buyable at entry of {len(portfolio)} screened)",
            })
            if verbose:
                print(f"    {entry_date}: only {buyable} of {len(portfolio)} screened "
                      f"names were buyable at entry (< {MIN_STOCKS}), CASH")
            continue

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "spy_usd_return": round(spy_usd_return, 6) if spy_usd_return is not None else None,
            "stocks_held": len(returns),
            "holdings": ",".join(sym for sym, _, _ in clean[:20]),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, bench={spy_return * 100 if spy_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency,
                 periods_per_year, cash_periods, avg_stocks,
                 benchmark_symbol="SPY", benchmark_name="S&P 500", execution="next-day close",
                 spy_metrics=None, spy_annual=None):
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

    # Secondary comparison: SPY in USD. Identical to the primary block for US runs,
    # so it is only emitted when the primary benchmark is a local index.
    spy_block = {}
    if spy_metrics is not None and benchmark_symbol != "SPY":
        sc = spy_metrics["comparison"]
        spy_block = {
            "spy_usd": format_series(spy_metrics["benchmark"]),
            "comparison_vs_spy": {
                "excess_cagr": pct(sc.get("excess_cagr")),
                "win_rate": pct(sc.get("win_rate")),
                "information_ratio": rnd(sc.get("information_ratio")),
                "up_capture": pct(sc.get("up_capture")),
                "down_capture": pct(sc.get("down_capture")),
                "beta": rnd(sc.get("beta")),
                "alpha": pct(sc.get("alpha")),
            },
            "excess_cagr_vs_spy": pct(sc.get("excess_cagr")),
            "annual_returns_vs_spy": [
                {"year": ar["year"],
                 "portfolio": round(ar["portfolio"] * 100, 2),
                 "spy": round(ar["benchmark"] * 100, 2),
                 "excess": round(ar["excess"] * 100, 2)}
                for ar in (spy_annual or [])
            ],
        }

    return {
        "universe": universe_name,
        # B006: n_periods, total_rebalances, cash_periods, invested_periods and
        # the window-provenance block. `executed` is every rebalance the
        # strategy ran; `valid` is the benchmark-priced subset metrics use. No
        # key below may re-declare one of these -- gate 8 enforces that.
        **period_accounting(
            [r for r in results if r["portfolio_return"] is not None],
            valid, cash_periods, universe_name=universe_name,
            benchmark_symbol=benchmark_symbol),
        "years": round(len(valid) / periods_per_year, 1),
        "frequency": frequency,
        "benchmark_name": benchmark_name,
        "execution": execution,
        "avg_stocks_when_invested": round(avg_stocks, 1),
        "period_data": results,
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
        **spy_block,
    }


def run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, verbose, output_path=None, offset_days=1,
               exclude_funds=False):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "next-day close (MOC)" if offset_days else "same-day close"

    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B"
                    if mktcap_threshold >= 1e9
                    else f"{mktcap_threshold/1e6:.0f}M")
    signal_desc = (f"Rev accel > 0, growth > {REV_GROWTH_MIN*100:.0f}%, "
                   f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, "
                   f"MCap > {mktcap_label} local, Top {MAX_STOCKS}")
    print_header("REVENUE ACCELERATION BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    if exclude_funds:
        print("  Universe guard: closed-end funds and ETFs excluded (isFund/isEtf)")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency,
                                               months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose,
                             exclude_funds=exclude_funds)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "Revenue Acceleration", benchmark_name))

    # Secondary: same portfolio measured against SPY (USD, total return) so the
    # cross-market comparison has one consistent yardstick.
    spy_metrics = None
    spy_annual = None
    if benchmark_symbol != "SPY":
        spy_valid = [r for r in results
                     if r["portfolio_return"] is not None and r.get("spy_usd_return") is not None]
        if spy_valid:
            spy_metrics = compute_metrics(
                [r["portfolio_return"] for r in spy_valid],
                [r["spy_usd_return"] for r in spy_valid],
                periods_per_year, risk_free_rate=risk_free_rate)
            spy_annual = compute_annual_returns(
                [r["portfolio_return"] for r in spy_valid],
                [r["spy_usd_return"] for r in spy_valid],
                [r["rebalance_date"] for r in spy_valid], periods_per_year)
            sx = spy_metrics["comparison"].get("excess_cagr")
            sb = spy_metrics["benchmark"].get("cagr")
            print(f"\n  Secondary benchmark S&P 500 (SPY, USD): "
                  f"{sb * 100:.2f}% CAGR, excess {sx * 100:+.2f}%"
                  if sx is not None and sb is not None else
                  "\n  Secondary benchmark S&P 500 (SPY, USD): unavailable")

    # B006: count over `executed` (every rebalance the strategy actually ran),
    # not `valid` (only those the benchmark can also price). Keeps the honest
    # full-window cash rate: an OSL leg that sat in cash before ^OSEAX starts
    # still sat in cash. invested_periods is derived in period_accounting.
    executed = [r for r in results if r["portfolio_return"] is not None]
    cash_periods = sum(1 for r in executed if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in executed if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(executed)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'RevAccel':>12} {benchmark_name:>10} {'Excess':>10}")
        print("  " + "-" * 42)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks,
                          benchmark_symbol=benchmark_symbol,
                          benchmark_name=benchmark_name,
                          execution=exec_model,
                          spy_metrics=spy_metrics,
                          spy_annual=spy_annual)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="Revenue Acceleration Growth backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    parser.add_argument("--exclude-funds", action="store_true",
                        help="Exclude closed-end funds and ETFs from the universe "
                             "(diagnostic: their investment income is reported as revenue)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("revenue-accel", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    offset_days = 0 if args.no_next_day else 1

    # --global mode: loop all presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        presets_to_run = [
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("india", ["NSE"]),
            ("japan", ["JPX"]),
            ("uk", ["LSE"]),
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("korea", ["KSC"]),
            ("taiwan", ["TAI", "TWO"]),
            ("germany", ["XETRA"]),
            ("canada", ["TSX"]),
            ("thailand", ["SET"]),
            ("sweden", ["STO"]),
            ("switzerland", ["SIX"]),
            ("singapore", ["SES"]),
            ("southafrica", ["JNB"]),
            # ASX excluded: adjClose split/adjustment issues
            # SAO excluded: adjClose split/adjustment issues
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
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, args.verbose, output_path,
                                    offset_days=offset_days,
                                    exclude_funds=args.exclude_funds)
                if result:
                    all_results[uni_name] = result
            except Exception as e:
                print(f"\n  ERROR on {uni_name}: {e}")
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
        print(f"{'Exchange':<20} {'Benchmark':<18} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} {'MaxDD':>8} {'Cash%':>7} {'AvgStk':>7}")
        print("-" * 96)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR / NO DATA':<20}")
                continue
            p = r.get("portfolio", {})
            c = r.get("comparison", {})
            n = r.get("n_periods", 0)
            # B006: divide by total_rebalances, not n_periods. Cash is counted
            # over every rebalance the strategy ran; n_periods counts only the
            # benchmark-priced ones. Fallback keeps pre-B006 JSONs renderable.
            tr = r.get("total_rebalances", n)
            cp = r.get("cash_periods", 0)
            cash_pct = round(cp * 100 / tr, 0) if tr > 0 else 0
            cagr = p.get("cagr")
            excess = c.get("excess_cagr")
            sharpe = p.get("sharpe_ratio")
            maxdd = p.get("max_drawdown")
            avg = r.get("avg_stocks_when_invested")
            bench = r.get("benchmark_name", "S&P 500")
            print(f"{uni:<20} {bench:<18} {cagr if cagr is not None else 'N/A':>7}% "
                  f"{f'{excess:+.2f}' if excess is not None else 'N/A':>7}% "
                  f"{sharpe if sharpe is not None else 'N/A':>8} "
                  f"{maxdd if maxdd is not None else 'N/A':>7}% "
                  f"{cash_pct:>6.0f}% {avg if avg is not None else 'N/A':>7}")
        print("=" * 96)
        return

    # Single exchange mode
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, args.verbose, args.output, offset_days=offset_days,
               exclude_funds=args.exclude_funds)


if __name__ == "__main__":
    main()
