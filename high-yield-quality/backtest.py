#!/usr/bin/env python3
"""
High Yield + Quality Backtest

Dividend yield screen with quality filters (ROE, FCF, payout, debt).
Annual rebalancing (July), top 30 by yield, equal weight.

Signal: DivYield 4-15%, ROE > 8%, FCF > 0, D/E < 2.0, Payout < 80%
Portfolio: Top 30 by dividend yield DESC, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), 2000-2025.

Usage:
    python3 high-yield-quality/backtest.py
    python3 high-yield-quality/backtest.py --preset india
    python3 high-yield-quality/backtest.py --global
    python3 high-yield-quality/backtest.py --frequency semi-annual --no-costs
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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns, remove_price_oscillations
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import add_common_args, resolve_exchanges, print_header, get_mktcap_threshold

# --- Signal parameters ---
DIVIDEND_YIELD_MIN = 0.04   # 4% floor - target meaningful high yield
DIVIDEND_YIELD_MAX = 0.15   # 15% cap - exclude likely distressed situations
ROE_MIN = 0.08              # Return on equity > 8%
DE_MAX = 2.0                # Debt/equity < 2.0
PAYOUT_MAX = 0.80           # Payout ratio < 80%
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_MONTHS = [7]        # July


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        ratios_cache(symbol, dividendYield, dividendPayoutRatio, debtToEquityRatio,
                     filing_epoch, period)
        metrics_cache(symbol, marketCap, returnOnEquity, filing_epoch, period)
        fcf_cache(symbol, freeCashFlow, filing_epoch, period)
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

    # 2. Financial ratios + key metrics
    queries = [
        ("ratios_cache", f"""
            SELECT symbol, dividendYield, dividendPayoutRatio, debtToEquityRatio,
                   dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY' AND dividendYield IS NOT NULL AND {sym_filter_sql}
        """, "financial ratios (yield, payout, D/E)"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap, returnOnEquity, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY' AND marketCap IS NOT NULL AND {sym_filter_sql}
        """, "key metrics (market cap, ROE)"),
        ("fcf_cache", f"""
            SELECT symbol, freeCashFlow, dateEpoch as filing_epoch, period
            FROM cash_flow_statement
            WHERE period = 'FY' AND freeCashFlow IS NOT NULL AND {sym_filter_sql}
        """, "free cash flow"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 3. Prices (only at rebalance dates)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR symbol IN (
                SELECT DISTINCT symbol FROM financial_ratios WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for High Yield + Quality stocks.

    Returns list of (symbol, dividend_yield, market_cap) tuples,
    sorted by dividend yield DESC, limited to MAX_STOCKS.
    """
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH r AS (
            SELECT symbol, dividendYield, dividendPayoutRatio, debtToEquityRatio,
                   filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        ),
        m AS (
            SELECT symbol, marketCap, returnOnEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        f AS (
            SELECT symbol, freeCashFlow, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM fcf_cache WHERE filing_epoch <= ?
        )
        SELECT r.symbol, r.dividendYield, m.marketCap
        FROM r
        JOIN m ON r.symbol = m.symbol AND m.rn = 1
        JOIN f ON r.symbol = f.symbol AND f.rn = 1
        WHERE r.rn = 1
          AND r.dividendYield >= ?
          AND r.dividendYield <= ?
          AND m.returnOnEquity > ?
          AND r.debtToEquityRatio >= 0
          AND r.debtToEquityRatio < ?
          AND r.dividendPayoutRatio > 0
          AND r.dividendPayoutRatio < ?
          AND f.freeCashFlow > 0
          AND m.marketCap > ?
        ORDER BY r.dividendYield DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch,
          DIVIDEND_YIELD_MIN, DIVIDEND_YIELD_MAX, ROE_MIN, DE_MAX, PAYOUT_MAX,
          mktcap_min, MAX_STOCKS]).fetchall()

    return [(r[0], r[1], r[2]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run High Yield + Quality backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            spy_entry = get_prices(con, ["SPY"], entry_date)
            spy_exit = get_prices(con, ["SPY"], exit_date)
            spy_return = None
            if "SPY" in spy_entry and "SPY" in spy_exit and spy_entry["SPY"] > 0:
                spy_return = (spy_exit["SPY"] - spy_entry["SPY"]) / spy_entry["SPY"]

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": round(spy_return, 6) if spy_return is not None else None,
                "stocks_held": 0,
                "holdings": f"CASH ({len(portfolio)} passed)",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, _, mc in portfolio}

        entry_prices = get_prices(con, symbols, entry_date)
        exit_prices = get_prices(con, symbols, exit_date)

        # Build symbol returns for filter_returns
        symbol_returns = []
        for sym in symbols:
            ep = entry_prices.get(sym)
            xp = exit_prices.get(sym)
            symbol_returns.append((sym, ep, xp, mcaps.get(sym)))

        clean, skipped = filter_returns(symbol_returns, verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        spy_entry = get_prices(con, ["SPY"], entry_date)
        spy_exit = get_prices(con, ["SPY"], exit_date)
        spy_return = None
        if "SPY" in spy_entry and "SPY" in spy_exit and spy_entry["SPY"] > 0:
            spy_return = (spy_exit["SPY"] - spy_entry["SPY"]) / spy_entry["SPY"]

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "stocks_held": len(returns),
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, spy={spy_return * 100 if spy_return else 0:.1f}%{excess}")

    return results


def main():
    parser = argparse.ArgumentParser(description="High Yield + Quality backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("high-yield-quality", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    from cli_utils import get_risk_free_rate
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    # Use custom months for semi-annual (April/October)
    months = DEFAULT_MONTHS if frequency == "semi-annual" and not args.frequency else None

    signal_desc = (f"DivYield {DIVIDEND_YIELD_MIN*100:.0f}-{DIVIDEND_YIELD_MAX*100:.0f}%, "
                   f"ROE > {ROE_MIN*100:.0f}%, FCF > 0, D/E < {DE_MAX}, "
                   f"Payout < {PAYOUT_MAX*100:.0f}%, MCap > {mktcap_threshold/1e9:.0f}B local, top {MAX_STOCKS}")
    print_header("HIGH YIELD + QUALITY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency, months=months)
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
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=args.verbose)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    # Phase 3: Compute metrics
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    raw_metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                                  risk_free_rate=risk_free_rate)

    # Display
    print(format_metrics(raw_metrics, "High Yield + Quality", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    # Annual returns
    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'HYQ':>10} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    # Save results
    if args.output:
        p = raw_metrics["portfolio"]
        b = raw_metrics["benchmark"]
        c = raw_metrics["comparison"]

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
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
