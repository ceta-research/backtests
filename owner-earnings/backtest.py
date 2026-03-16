#!/usr/bin/env python3
"""
Owner Earnings Yield Strategy Backtest

Annual rebalancing (July), equal weight, top 30 by highest OE yield.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: OE Yield > 5%, OE Yield < 50%, ROE > 10%, Operating Margin > 10%,
        Market Cap > local-currency threshold (standard)

Owner Earnings = Net Income + D&A - min(|Capex|, D&A)
   where min(|Capex|, D&A) proxies maintenance capex.
   If capex < D&A, all capex is maintenance. If capex > D&A, excess is growth.

OE Yield = Owner Earnings / Market Cap

Portfolio: Top 30 by highest OE yield, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), 2000-2025.

Academic reference: Buffett, W. (1986). "Berkshire Hathaway Annual Letter to
Shareholders." Owner Earnings concept. Also: Greenwald, B. et al. (2001)
"Value Investing: From Graham to Buffett and Beyond." Earnings power valuation.

Usage:
    # Backtest US stocks (default)
    python3 owner-earnings/backtest.py

    # Backtest German stocks
    python3 owner-earnings/backtest.py --preset germany

    # Backtest all exchanges (loop)
    python3 owner-earnings/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs (academic baseline)
    python3 owner-earnings/backtest.py --no-costs

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
from data_utils import query_parquet, get_prices, generate_rebalance_dates
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, save_results, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
OE_YIELD_MIN = 0.05        # OE yield > 5% (less than 20x owner earnings)
OE_YIELD_MAX = 0.50        # OE yield < 50% (removes data artifacts)
ROE_MIN = 0.10             # Return on equity > 10%
OPM_MIN = 0.10             # Operating profit margin > 10% (genuine pricing power)
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (annual FY filings available, 45-day lag)

# Exchanges with known data quality issues - excluded from global run
EXCLUDED_PRESETS = {
    "brazil",       # SAO: adjClose split artifacts
    "australia",    # ASX: adjClose split artifacts
    "france",       # PAR: pipeline gap (1 FY symbol in warehouse)
    "nyse",         # Covered by "us" preset
    "nasdaq",       # Covered by "us" preset
    "uk",           # LSE: 651% return in 2010 (price data artifact)
    "norway",       # OSL: 44% cash periods (11/25), universe too thin
    "germany",      # XETRA: adjClose split artifacts (DAP.DE, EL45.DE)
    "canada",       # TSX: adjClose split artifacts (SGU.V)
}


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, depreciationAndAmortization, filing_epoch)
        cashflow_cache(symbol, capitalExpenditure, filing_epoch)
        metrics_cache(symbol, marketCap, filing_epoch)
        ratios_cache(symbol, returnOnEquity, operatingProfitMargin, filing_epoch)
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

    # 2-5. Financial data (FY, annual filings)
    # NOTE: returnOnEquity is in key_metrics, operatingProfitMargin is in financial_ratios
    queries = [
        ("income_cache", f"""
            SELECT symbol, netIncome, depreciationAndAmortization,
                   dateEpoch as filing_epoch
            FROM income_statement
            WHERE period = 'FY'
              AND netIncome IS NOT NULL
              AND depreciationAndAmortization IS NOT NULL
              AND {sym_filter_sql}
        """, "income statement (net income, D&A)"),
        ("cashflow_cache", f"""
            SELECT symbol, capitalExpenditure,
                   dateEpoch as filing_epoch
            FROM cash_flow_statement
            WHERE period = 'FY'
              AND capitalExpenditure IS NOT NULL
              AND {sym_filter_sql}
        """, "cash flow statement (capex)"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap, returnOnEquity,
                   dateEpoch as filing_epoch
            FROM key_metrics
            WHERE period = 'FY'
              AND marketCap IS NOT NULL
              AND marketCap > 0
              AND {sym_filter_sql}
        """, "key metrics (market cap, ROE)"),
        ("ratios_cache", f"""
            SELECT symbol, operatingProfitMargin,
                   dateEpoch as filing_epoch
            FROM financial_ratios
            WHERE period = 'FY'
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

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR symbol IN (
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
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
    """Screen for high Owner Earnings yield stocks.

    Owner Earnings = Net Income + D&A - min(|Capex|, D&A)
    OE Yield = Owner Earnings / Market Cap

    Returns list of (symbol, market_cap) tuples.
    """
    # 45-day lag: use filings available at least 45 days before rebalance date
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH inc AS (
            SELECT symbol, netIncome, depreciationAndAmortization, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        cf AS (
            SELECT symbol, capitalExpenditure, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM cashflow_cache WHERE filing_epoch <= ?
        ),
        m AS (
            SELECT symbol, marketCap, returnOnEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        r AS (
            SELECT symbol, operatingProfitMargin, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        ),
        calculated AS (
            SELECT
                inc.symbol,
                m.marketCap,
                inc.netIncome
                    + inc.depreciationAndAmortization
                    - LEAST(ABS(cf.capitalExpenditure), inc.depreciationAndAmortization)
                    AS owner_earnings
            FROM inc
            JOIN cf ON inc.symbol = cf.symbol AND cf.rn = 1
            JOIN m ON inc.symbol = m.symbol AND m.rn = 1
            JOIN r ON inc.symbol = r.symbol AND r.rn = 1
            WHERE inc.rn = 1
              AND m.returnOnEquity > ?
              AND r.operatingProfitMargin > ?
              AND m.marketCap > ?
        )
        SELECT symbol, marketCap
        FROM calculated
        WHERE owner_earnings > 0
          AND owner_earnings / marketCap > ?
          AND owner_earnings / marketCap < ?
        ORDER BY owner_earnings / marketCap DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, cutoff_epoch,
          ROE_MIN, OPM_MIN, mktcap_min,
          OE_YIELD_MIN, OE_YIELD_MAX, MAX_STOCKS]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run Owner Earnings yield backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            spy_prices_entry = get_prices(con, ["SPY"], entry_date)
            spy_prices_exit = get_prices(con, ["SPY"], exit_date)
            spy_return = None
            if "SPY" in spy_prices_entry and "SPY" in spy_prices_exit and spy_prices_entry["SPY"] > 0:
                spy_return = (spy_prices_exit["SPY"] - spy_prices_entry["SPY"]) / spy_prices_entry["SPY"]

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

        entry_prices = get_prices(con, symbols, entry_date)
        exit_prices = get_prices(con, symbols, exit_date)

        returns = []
        for sym in symbols:
            ep = entry_prices.get(sym)
            xp = exit_prices.get(sym)
            if ep and xp and ep > 0:
                raw_ret = (xp - ep) / ep
                if use_costs:
                    cost = tiered_cost(mcaps.get(sym))
                    net_ret = apply_costs(raw_ret, cost)
                else:
                    net_ret = raw_ret
                returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        spy_prices_entry = get_prices(con, ["SPY"], entry_date)
        spy_prices_exit = get_prices(con, ["SPY"], exit_date)
        spy_return = None
        if "SPY" in spy_prices_entry and "SPY" in spy_prices_exit and spy_prices_entry["SPY"] > 0:
            spy_return = (spy_prices_exit["SPY"] - spy_prices_entry["SPY"]) / spy_prices_entry["SPY"]

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
    parser = argparse.ArgumentParser(description="Owner Earnings Yield Strategy backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("owner-earnings", args_str=" ".join(cloud_args),
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

    signal_desc = (f"OE Yield {OE_YIELD_MIN*100:.0f}-{OE_YIELD_MAX*100:.0f}%, ROE > {ROE_MIN*100:.0f}%, "
                   f"OPM > {OPM_MIN*100:.0f}%, "
                   f"MCap > {mktcap_threshold/1e9:.0f}B local, top {MAX_STOCKS}")
    print_header("OWNER EARNINGS YIELD BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    # Handle global mode: loop over all eligible exchanges individually
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
            single_result = _run_single(args, ex_list, key, frequency, use_costs, verbose=args.verbose)
            if single_result:
                all_results[key] = single_result

        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n  Global results saved to {args.output}")
        return

    result = _run_single(args, exchanges, universe_name, frequency, use_costs,
                         verbose=args.verbose, output_path=args.output)


def _run_single(args, exchanges, universe_name, frequency, use_costs,
                verbose=False, output_path=None):
    """Run backtest for a single exchange set. Returns result dict or None."""
    risk_free_rate = get_risk_free_rate(exchanges, getattr(args, 'risk_free_rate', None))
    mktcap_threshold = get_mktcap_threshold(exchanges)
    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

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
                           verbose=verbose)
    bt_time = time.time() - t1
    print(f"  Backtest completed in {bt_time:.0f}s")

    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("  No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)

    print(format_metrics(metrics, "Owner Earnings", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'OE Yield':>10} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    # Build output dict
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
