#!/usr/bin/env python3
"""
QARP Multi-Exchange Backtest

7-factor quality-value signal backtested across global exchanges, 2000-2025.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Piotroski >= 7, ROE > 15%, D/E < 0.5, CR > 1.5, IQ > 1.0, P/E 5-25, MCap > $1B
Portfolio: Equal weight all qualifying. Cash if < 10 qualify.
Rebalancing: Semi-annual (Jan/Jul), 2000-2025.

Usage:
    # Backtest US stocks (default)
    python3 qarp/backtest.py

    # Backtest Indian stocks
    python3 qarp/backtest.py --exchange BSE,NSE

    # Backtest German stocks
    python3 qarp/backtest.py --exchange XETRA

    # Backtest all exchanges
    python3 qarp/backtest.py --global

    # Custom parameters
    python3 qarp/backtest.py --frequency quarterly --risk-free-rate 0.0

See README.md for data source setup.
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
from data_utils import query_parquet, get_prices, generate_rebalance_dates
from metrics import compute_metrics as _compute_metrics, compute_annual_returns, format_metrics
from cli_utils import add_common_args, resolve_exchanges, print_header

# --- Signal parameters ---
PIOTROSKI_MIN = 7
ROE_MIN = 0.15
DE_MAX = 0.5
PE_MIN = 5
PE_MAX = 25
CR_MIN = 1.5
IQ_MIN = 1.0
MKTCAP_MIN = 1_000_000_000
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "semi-annual"


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all historical financial data and load into DuckDB.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, priceToEarningsRatio, debtToEquityRatio, filing_epoch, period)
        income_cache(symbol, netIncome, grossProfit, revenue, filing_epoch, period)
        balance_cache(symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                      longTermDebt, totalStockholdersEquity, filing_epoch, period)
        cashflow_cache(symbol, operatingCashFlow, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose) + index on (symbol, trade_epoch)

    Returns a DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where_direct = f"WHERE exchange IN ({ex_filter})"
    else:
        exchange_where_direct = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange FROM profile {exchange_where_direct}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    if exchanges:
        sym_filter_sql = f"""
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE exchange IN ({ex_filter})
            )
        """
    else:
        sym_filter_sql = "1=1"

    # 2-6: Financial data (all via parquet)
    queries = [
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY' AND returnOnEquity IS NOT NULL AND {sym_filter_sql}
        """, "key metrics"),
        ("ratios_cache", f"""
            SELECT symbol, priceToEarningsRatio, debtToEquityRatio, dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY' AND priceToEarningsRatio IS NOT NULL AND {sym_filter_sql}
        """, "financial ratios"),
        ("income_cache", f"""
            SELECT symbol, netIncome, grossProfit, revenue, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "income statements"),
        ("balance_cache", f"""
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                longTermDebt, totalStockholdersEquity, dateEpoch as filing_epoch, period
            FROM balance_sheet
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "balance sheets"),
        ("cashflow_cache", f"""
            SELECT symbol, operatingCashFlow, dateEpoch as filing_epoch, period
            FROM cash_flow_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "cash flow statements"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 7. Prices (only at rebalance dates)
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
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
                INTERSECT
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows (at {len(rebalance_dates)} rebalance dates)")

    return con


def screen_stocks(con, target_date):
    """Screen for QARP using cached data. Same logic as original backtest."""
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())
    prev_year_epoch = int(datetime.combine(target_date - timedelta(days=445), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH inc_curr AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        inc_prev AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        bal_curr AS (
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                longTermDebt, totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        bal_prev AS (
            SELECT symbol, totalAssets, longTermDebt, totalCurrentAssets, totalCurrentLiabilities,
                totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ?
        ),
        cf_curr AS (
            SELECT symbol, operatingCashFlow, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM cashflow_cache WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        m AS (
            SELECT symbol, returnOnEquity, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        r AS (
            SELECT symbol, priceToEarningsRatio, debtToEquityRatio, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        ),
        piotroski AS (
            SELECT ic.symbol,
                CASE WHEN ic.netIncome > 0 THEN 1 ELSE 0 END AS f1_ni,
                CASE WHEN cf.operatingCashFlow > 0 THEN 1 ELSE 0 END AS f2_ocf,
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.netIncome / bc.totalAssets) > (ip.netIncome / bp.totalAssets) THEN 1 ELSE 0 END AS f3_roa,
                CASE WHEN cf.operatingCashFlow > ic.netIncome THEN 1 ELSE 0 END AS f4_accrual,
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (COALESCE(bc.longTermDebt,0) / bc.totalAssets) < (COALESCE(bp.longTermDebt,0) / bp.totalAssets) THEN 1 ELSE 0 END AS f5_leverage,
                CASE WHEN bc.totalCurrentAssets > 0 AND bc.totalCurrentLiabilities > 0
                     AND bp.totalCurrentAssets > 0 AND bp.totalCurrentLiabilities > 0
                     AND (bc.totalCurrentAssets / bc.totalCurrentLiabilities) > (bp.totalCurrentAssets / bp.totalCurrentLiabilities) THEN 1 ELSE 0 END AS f6_liquidity,
                CASE WHEN bc.totalStockholdersEquity >= bp.totalStockholdersEquity THEN 1 ELSE 0 END AS f7_no_dilution,
                CASE WHEN ic.revenue > 0 AND ip.revenue > 0 AND bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.revenue / bc.totalAssets) > (ip.revenue / bp.totalAssets) THEN 1 ELSE 0 END AS f8_turnover,
                CASE WHEN ic.grossProfit > 0 AND ip.grossProfit > 0 AND ic.revenue > 0 AND ip.revenue > 0
                     AND (ic.grossProfit / ic.revenue) > (ip.grossProfit / ip.revenue) THEN 1 ELSE 0 END AS f9_margin,
                CASE WHEN bc.totalCurrentLiabilities > 0
                     THEN bc.totalCurrentAssets * 1.0 / bc.totalCurrentLiabilities
                     ELSE NULL END AS current_ratio,
                CASE WHEN ic.netIncome > 0
                     THEN cf.operatingCashFlow * 1.0 / ic.netIncome
                     ELSE NULL END AS income_quality
            FROM inc_curr ic
            JOIN inc_prev ip ON ic.symbol = ip.symbol AND ip.rn = 1
            JOIN bal_curr bc ON ic.symbol = bc.symbol AND bc.rn = 1
            JOIN bal_prev bp ON ic.symbol = bp.symbol AND bp.rn = 1
            JOIN cf_curr cf ON ic.symbol = cf.symbol AND cf.rn = 1
            WHERE ic.rn = 1
        ),
        scored AS (
            SELECT symbol,
                (f1_ni + f2_ocf + f3_roa + f4_accrual + f5_leverage + f6_liquidity
                 + f7_no_dilution + f8_turnover + f9_margin) AS f_score,
                current_ratio,
                income_quality
            FROM piotroski
        )
        SELECT s.symbol
        FROM scored s
        JOIN m ON s.symbol = m.symbol AND m.rn = 1
        JOIN r ON s.symbol = r.symbol AND r.rn = 1
        WHERE s.f_score >= ?
          AND m.returnOnEquity > ?
          AND r.debtToEquityRatio >= 0
          AND r.debtToEquityRatio < ?
          AND s.current_ratio IS NOT NULL
          AND s.current_ratio > ?
          AND s.income_quality IS NOT NULL
          AND s.income_quality > ?
          AND r.priceToEarningsRatio > ?
          AND r.priceToEarningsRatio < ?
          AND m.marketCap > ?
        ORDER BY s.f_score DESC, m.returnOnEquity DESC
    """, [cutoff_epoch, prev_year_epoch,
          prev_year_epoch,
          cutoff_epoch, prev_year_epoch,
          prev_year_epoch,
          cutoff_epoch, prev_year_epoch,
          cutoff_epoch,
          cutoff_epoch,
          PIOTROSKI_MIN, ROE_MIN, DE_MAX, CR_MIN, IQ_MIN,
          PE_MIN, PE_MAX, MKTCAP_MIN]).fetchall()
    return [r[0] for r in rows]


def run_backtest(con, rebalance_dates, verbose=False):
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date)

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

        entry_prices = get_prices(con, portfolio, entry_date)
        exit_prices = get_prices(con, portfolio, exit_date)

        returns = []
        for sym in portfolio:
            ep = entry_prices.get(sym)
            xp = exit_prices.get(sym)
            if ep and xp and ep > 0:
                returns.append((xp - ep) / ep)

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
            "holdings": ",".join(portfolio),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, spy={spy_return * 100 if spy_return else 0:.1f}%{excess}")

    return results


def build_output(raw_metrics, results, universe_name, periods_per_year):
    """Build backward-compatible JSON output with new metrics added.

    Keeps all existing field names (cagr, total_return, etc.) as percentages
    (e.g. 9.96 for 9.96%) for backward compatibility with existing charts and blogs.
    Adds new fields (sortino_ratio, calmar_ratio, etc.) in the same format.
    """
    p = raw_metrics["portfolio"]
    b = raw_metrics["benchmark"]
    c = raw_metrics["comparison"]

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0

    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    period_dates = [r["rebalance_date"] for r in valid]
    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)

    def pct(v):
        return round(v * 100, 2) if v is not None else None

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    return {
        "universe": universe_name,
        "n_periods": len(valid),
        "years": round(len(valid) / periods_per_year, 1),
        "cash_periods": cash_periods,
        "invested_periods": len(valid) - cash_periods,
        "avg_stocks_when_invested": round(avg_stocks, 1),
        "portfolio": {
            "total_return": pct(p["total_return"]),
            "cagr": pct(p["cagr"]),
            "max_drawdown": pct(p["max_drawdown"]),
            "annualized_volatility": pct(p["annualized_volatility"]),
            "sharpe_ratio": rnd(p["sharpe_ratio"]),
            "sortino_ratio": rnd(p["sortino_ratio"]),
            "calmar_ratio": rnd(p["calmar_ratio"]),
            "var_95": pct(p["var_95"]),
            "max_consecutive_losses": p["max_consecutive_losses"],
            "pct_negative_periods": pct(p["pct_negative_periods"]),
        },
        "spy": {
            "total_return": pct(b["total_return"]),
            "cagr": pct(b["cagr"]),
            "max_drawdown": pct(b["max_drawdown"]),
            "annualized_volatility": pct(b["annualized_volatility"]),
            "sharpe_ratio": rnd(b["sharpe_ratio"]),
            "sortino_ratio": rnd(b["sortino_ratio"]),
            "calmar_ratio": rnd(b["calmar_ratio"]),
            "var_95": pct(b["var_95"]),
            "max_consecutive_losses": b["max_consecutive_losses"],
            "pct_negative_periods": pct(b["pct_negative_periods"]),
        },
        "comparison": {
            "excess_cagr": pct(c["excess_cagr"]),
            "win_rate": pct(c["win_rate"]),
            "information_ratio": rnd(c["information_ratio"]),
            "tracking_error": pct(c["tracking_error"]),
            "up_capture": pct(c["up_capture"]),
            "down_capture": pct(c["down_capture"]),
            "beta": rnd(c["beta"]),
            "alpha": pct(c["alpha"]),
        },
        # Backward compat: keep flat fields
        "excess_cagr": pct(c["excess_cagr"]),
        "win_rate_vs_spy": pct(c["win_rate"]),
        "annual_returns": [
            {"year": ar["year"],
             "portfolio": round(ar["portfolio"] * 100, 2),
             "spy": round(ar["benchmark"] * 100, 2),
             "excess": round(ar["excess"] * 100, 2)}
            for ar in annual
        ],
    }


def main():
    parser = argparse.ArgumentParser(description="QARP multi-exchange backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        # Rebuild args string without --cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("qarp", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY

    # Auto-detect risk-free rate from exchanges (or use user override)
    from cli_utils import get_risk_free_rate
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    signal_desc = (f"Piotroski >= {PIOTROSKI_MIN}, ROE > {ROE_MIN*100:.0f}%, "
                   f"D/E < {DE_MAX}, CR > {CR_MIN}, IQ > {IQ_MIN}, "
                   f"P/E {PE_MIN}-{PE_MAX}, MCap > ${MKTCAP_MIN/1e9:.0f}B")
    print_header("QARP BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Risk-free rate: {risk_free_rate*100:.1f}%")
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

    # Phase 2: Run backtest locally
    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, verbose=args.verbose)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    # Phase 3: Compute metrics using shared module
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    raw_metrics = _compute_metrics(port_returns, spy_returns, periods_per_year,
                                   risk_free_rate=risk_free_rate)

    # Display
    print(format_metrics(raw_metrics, "QARP", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'QARP':>10} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    # Save results (backward-compatible format)
    if args.output:
        output = build_output(raw_metrics, results, universe_name, periods_per_year)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
