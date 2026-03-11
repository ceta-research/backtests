#!/usr/bin/env python3
"""
Earnings Growth Consistency Backtest

Annual rebalancing (July), equal weight, top 30 by ROE.
Fetches data via Ceta Research API, caches in DuckDB, runs locally.

Signal: Net income grew YoY for 3 consecutive years (4 FY data points required),
        all 4 periods profitable, ROE > 8%, D/E < 2.0, MCap > local threshold.
Portfolio: Top 30 by ROE, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), 2000-2025.

Academic references:
  - Dichev, I.D. and Tang, V.W. (2009). "Earnings Volatility and Earnings Predictability."
    Journal of Accounting and Economics, 47(1-2). Consistent earners have lower cost of
    capital and attract stable institutional ownership.
  - Sloan, R.G. (1996). "Do Stock Prices Fully Reflect Information in Accruals and Cash
    Flows About Future Earnings?" The Accounting Review, 71(3). Earnings quality predicts
    future returns — consistent earnings are associated with lower accruals and better
    subsequent performance.

Usage:
    python3 earnings-consistency/backtest.py                         # US default
    python3 earnings-consistency/backtest.py --preset india
    python3 earnings-consistency/backtest.py --global --output results/exchange_comparison.json --verbose
    python3 earnings-consistency/backtest.py --preset us --no-costs --verbose
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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
ROE_MIN = 0.08          # Return on equity > 8% (modest quality bar)
DE_MAX = 2.0            # Debt-to-equity < 2.0 (manageable leverage)
MIN_STREAK = 3          # Require 3 consecutive years of NI growth (4 data points)
MAX_STOCKS = 30         # Top N by ROE, equal weight
MIN_STOCKS = 10         # Hold cash if fewer qualify
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July — covers Dec/Mar FY-end companies with 45-day lag
MAX_SINGLE_RETURN = 2.0  # Cap at 200% per stock (data quality guard)
MIN_ENTRY_PRICE = 1.0    # Skip sub-$1 entry prices (price data artifacts)
STALE_YEARS = 5          # Ignore FY filings older than 5 years


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all data needed for earnings-consistency backtest.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, filing_epoch, period)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, debtToEquityRatio, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
        sym_filter_sql = (
            f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        )
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

    # 2-4: Financial data — fetch all FY records for each table
    queries = [
        ("income_cache", f"""
            SELECT symbol, netIncome, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY'
              AND netIncome IS NOT NULL
              AND {sym_filter_sql}
        """, "income statements (netIncome)"),
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND returnOnEquity IS NOT NULL
              AND marketCap IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (ROE, MCap)"),
        ("ratios_cache", f"""
            SELECT symbol, debtToEquityRatio, dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND debtToEquityRatio IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (D/E)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              memory_mb=4096, threads=2)
        print(f"    -> {count} rows")

    # 5. Prices — only at rebalance windows (entry + exit)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    # Symbol filter: restrict to symbols with positive netIncome in income_statement
    # to avoid fetching prices for shells and pre-revenue companies
    if sym_filter_sql != "1=1":
        exchange_sym_filter = f"AND {sym_filter_sql}"
    else:
        exchange_sym_filter = ""

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR symbol IN (
                SELECT DISTINCT symbol FROM income_statement
                WHERE period = 'FY' AND netIncome > 0
                  {exchange_sym_filter}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min, verbose=False):
    """Screen for earnings-consistent stocks at target_date.

    Requires 4 consecutive FY netIncome filings (3 growth years):
      - NI[rn=1] > NI[rn=2] > NI[rn=3] > NI[rn=4] > 0
    Plus quality filters: ROE > 8%, D/E < 2.0, MCap > threshold.
    Returns top MAX_STOCKS by ROE descending.

    Uses 45-day filing lag for point-in-time integrity.
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())
    stale_cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45 + STALE_YEARS * 365), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        -- All FY netIncome filings within the valid point-in-time window
        inc AS (
            SELECT symbol, netIncome, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        -- Consistent earners: 3 consecutive years of NI growth, all profitable
        streak AS (
            SELECT y1.symbol
            FROM inc y1
            JOIN inc y2 ON y1.symbol = y2.symbol AND y2.rn = 2
            JOIN inc y3 ON y1.symbol = y3.symbol AND y3.rn = 3
            JOIN inc y4 ON y1.symbol = y4.symbol AND y4.rn = 4
            WHERE y1.rn = 1
              AND y1.netIncome > y2.netIncome
              AND y2.netIncome > y3.netIncome
              AND y3.netIncome > y4.netIncome
              AND y4.netIncome > 0
        ),
        -- Most recent ROE and MCap
        met AS (
            SELECT symbol, returnOnEquity, marketCap,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        -- Most recent D/E
        rat AS (
            SELECT symbol, debtToEquityRatio,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        )
        SELECT s.symbol, met.returnOnEquity, met.marketCap
        FROM streak s
        JOIN met ON s.symbol = met.symbol AND met.rn = 1
        JOIN rat ON s.symbol = rat.symbol AND rat.rn = 1
        WHERE met.returnOnEquity > ?
          AND rat.debtToEquityRatio >= 0
          AND rat.debtToEquityRatio < ?
          AND met.marketCap > ?
        ORDER BY met.returnOnEquity DESC
        LIMIT ?
    """, [
        cutoff_epoch, stale_cutoff_epoch,   # inc window
        cutoff_epoch,                        # met
        cutoff_epoch, stale_cutoff_epoch,   # rat window
        ROE_MIN, DE_MAX, mktcap_min,
        MAX_STOCKS,
    ]).fetchall()

    if verbose and rows:
        top_roe = rows[0][1] * 100 if rows else 0
        bot_roe = rows[-1][1] * 100 if rows else 0
        print(f"    Qualifying: {len(rows)} stocks "
              f"(ROE range: {bot_roe:.0f}%–{top_roe:.0f}%)")

    return [(r[0], r[2]) for r in rows]  # (symbol, market_cap)


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run earnings-consistency backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min, verbose=verbose)

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
                "spy_return": spy_return,
                "stocks_held": 0,
                "holdings": f"CASH ({len(portfolio)} passed, need {MIN_STOCKS})",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _ in portfolio]
        mcaps = {s: mc for s, mc in portfolio}

        entry_prices = get_prices(con, symbols, entry_date)
        exit_prices = get_prices(con, symbols, exit_date)

        symbol_data = [
            (sym, entry_prices.get(sym), exit_prices.get(sym), mcaps.get(sym))
            for sym in symbols
        ]
        clean, skipped = filter_returns(symbol_data,
                                        min_entry_price=MIN_ENTRY_PRICE,
                                        max_single_return=MAX_SINGLE_RETURN,
                                        verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs and mcap:
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
                  f"port={port_return * 100:.1f}%, "
                  f"spy={spy_return * 100 if spy_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency, periods_per_year,
                 cash_periods, avg_stocks):
    """Build JSON output in standard format."""
    p = metrics["portfolio"]
    b = metrics["benchmark"]
    c = metrics["comparison"]

    def pct(v):
        return round(v * 100, 2) if v is not None else None

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    def fmt(s):
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
        "portfolio": fmt(p),
        "spy": fmt(b),
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
               risk_free_rate, mktcap_threshold, verbose, output_path=None):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    signal_desc = (
        f"NI grew 3 consecutive years, NI>0 all 4 periods, "
        f"ROE>{ROE_MIN*100:.0f}%, D/E<{DE_MAX}, "
        f"MCap>{mktcap_threshold/1e9:.0f}B local → top {MAX_STOCKS} by ROE"
    )
    print_header("EARNINGS CONSISTENCY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

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

    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=verbose)
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
    print(format_metrics(metrics, "Earnings Consistency", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'EarnCons':>10} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
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
    parser = argparse.ArgumentParser(description="Earnings Growth Consistency multi-exchange backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("earnings-consistency", args_str=" ".join(cloud_args),
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

        # Exchange list: broad coverage
        # ASX/SAO excluded (adjClose artifacts per DATA_QUALITY_ISSUES.md)
        # OSL excluded: 100% cash — oil-dominated market has no consistent earners passing the screen
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["BSE", "NSE"]),
            ("uk",          ["LSE"]),
            ("germany",     ["XETRA"]),
            ("japan",       ["JPX"]),
            ("china",       ["SHZ", "SHH"]),
            ("hongkong",    ["HKSE"]),
            ("korea",       ["KSC"]),
            ("taiwan",      ["TAI", "TWO"]),
            ("canada",      ["TSX"]),
            ("switzerland", ["SIX"]),
            ("sweden",      ["STO"]),
            ("thailand",    ["SET"]),
            ("southafrica", ["JNB"]),
            ("italy",       ["MIL"]),
            ("malaysia",    ["KLS"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            mktcap_threshold = get_mktcap_threshold(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"returns_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            try:
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, mktcap_threshold, args.verbose, output_path)
                if result:
                    all_results[uni_name] = result
            except Exception as e:
                print(f"\n  ERROR on {uni_name}: {e}")
                all_results[uni_name] = {"error": str(e)}

        # Save comparison JSON
        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Print summary table
        print(f"\n\n{'=' * 80}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 80}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>8} {'AvgStk':>8}")
        print("-" * 80)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR / NO DATA':>40}")
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
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, mktcap_threshold, args.verbose, args.output)


if __name__ == "__main__":
    main()
