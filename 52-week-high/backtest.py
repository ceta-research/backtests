#!/usr/bin/env python3
"""
52-Week High Proximity Backtest

Buys the top 30 stocks nearest to their 52-week high (highest proximity ratio)
within each exchange. Quarterly rebalancing.

Signal: adjClose / MAX(high over 252 trading days) per George & Hwang (2004).
        Higher ratio = stock closer to its 52-week high = stronger momentum signal.
        No skip-month needed (unlike classic 12-1 momentum).

Universe: MCap > exchange threshold (from key_metrics FY). Price > $1.
Portfolio: Equal weight top 30. Cash if < 10 qualify.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2000-2025.
Costs: Size-tiered (see costs.py). Excludes: ASX, SAO (adjClose artifacts).

Academic basis:
  George, T. & Hwang, C. (2004). "The 52-Week High and Momentum Investing."
  Journal of Finance, 59(5), 2145-2176.

Usage:
    python3 52-week-high/backtest.py                           # US default
    python3 52-week-high/backtest.py --preset india
    python3 52-week-high/backtest.py --global --output results/exchange_comparison.json
    python3 52-week-high/backtest.py --preset us --no-costs --verbose
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
HIGH_WINDOW_DAYS = 365       # Calendar days for 252 trading day window
HIGH_WINDOW_FETCH = 390      # Days before rebalance to fetch prices (252td + buffer)
MIN_HISTORY_ROWS = 100       # Minimum trading days required to compute valid 52w high
MAX_STOCKS = 30              # Top N by proximity, equal weight
MIN_STOCKS = 10              # Hold cash if fewer qualify
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]   # Jan / Apr / Jul / Oct
MAX_SINGLE_RETURN = 2.0      # Cap at 200% per stock (data quality guard)
MIN_ENTRY_PRICE = 1.0        # Skip sub-$1 entry prices (price data artifacts)
MAX_PROXIMITY = 1.1          # Proximity > 1.1 likely a data error (adjClose > 52w high)

# Development compute (max resources)
FETCH_MEMORY_MB = 4096
FETCH_THREADS = 2


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all data needed for 52-week high proximity backtest.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        metrics_cache(symbol, marketCap, filing_epoch, period)
            -- used for point-in-time market cap filter only
        prices_cache(symbol, trade_epoch, adjClose, high)
            -- extended window: 52W lookback + entry date per rebalance date

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='8GB'")

    # 1. Universe (exchange membership from profile)
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
        sym_filter_sql = (
            f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        )
    else:
        sym_filter_sql = "1=1"

    # 2. Historical market cap (from key_metrics FY filings, point-in-time)
    print("  Fetching historical market cap...")
    metrics_sql = f"""
        SELECT symbol, marketCap, dateEpoch as filing_epoch, period
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND marketCap > 0
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache", verbose=verbose,
                          memory_mb=FETCH_MEMORY_MB, threads=FETCH_THREADS)
    print(f"    -> {count} market cap rows")

    # 3. Prices — HIGH_WINDOW_FETCH days before each rebalance through entry end (R+10)
    # Need both adjClose (for entry/exit returns) AND high (for 52w high computation).
    # Window per rebalance date: [R - HIGH_WINDOW_FETCH, R + 10]
    # This covers: 252 trading days of high data + entry prices.
    # Exit prices for period i are covered by period i+1's entry window.
    print("  Fetching prices (adjClose + high for 52w window)...")
    date_conditions = []
    for d in rebalance_dates:
        fetch_start = d - timedelta(days=HIGH_WINDOW_FETCH)
        entry_end = d + timedelta(days=10)
        date_conditions.append(
            f"(date >= '{fetch_start.isoformat()}' AND date <= '{entry_end.isoformat()}')"
        )
    date_filter = " OR ".join(date_conditions)

    # Symbol pre-filter: only symbols with FY key_metrics (MCap data).
    exchange_fy_filter = f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""
    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, high
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR symbol = 'MTUM'
            OR symbol IN (
                SELECT DISTINCT symbol FROM key_metrics
                WHERE period = 'FY'
                  AND marketCap IS NOT NULL
                  AND marketCap > 0
                  {exchange_fy_filter}
            )
          )
          AND adjClose > 0
          AND high > 0
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=15000000, timeout=600,
                          memory_mb=FETCH_MEMORY_MB, threads=FETCH_THREADS)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows (adjClose + high, {HIGH_WINDOW_FETCH}-day window per date)")

    return con


def get_eligible_symbols(con, target_date, mktcap_min):
    """Get symbols passing market cap filter at target_date.

    Uses 45-day filing lag for point-in-time integrity.
    Returns dict: {symbol: market_cap}
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        )
        SELECT symbol, marketCap
        FROM met
        WHERE rn = 1
          AND marketCap > ?
    """, [cutoff_epoch, mktcap_min]).fetchall()

    return {r[0]: r[1] for r in rows}


def compute_proximity(con, symbols, target_date):
    """Compute 52-week high proximity ratio for a set of symbols at target_date.

    proximity_ratio = adjClose(T) / MAX(high) over past 252 trading days

    Higher ratio = stock is closer to its 52-week high.
    Ratio of 1.0 means the stock is exactly at its 52-week high.

    Returns list of (symbol, proximity_ratio) sorted descending.
    """
    if not symbols:
        return []

    start_epoch = int(datetime.combine(
        target_date - timedelta(days=HIGH_WINDOW_FETCH), datetime.min.time()
    ).timestamp())
    target_epoch = int(datetime.combine(
        target_date + timedelta(days=5), datetime.min.time()
    ).timestamp())

    sym_list = ",".join(f"'{s}'" for s in symbols)

    rows = con.execute(f"""
        WITH windowed AS (
            SELECT symbol, trade_epoch, adjClose, high,
                MAX(high) OVER (
                    PARTITION BY symbol ORDER BY trade_epoch
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS high_52w,
                COUNT(*) OVER (
                    PARTITION BY symbol ORDER BY trade_epoch
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS row_count
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_epoch >= {start_epoch}
              AND trade_epoch <= {target_epoch}
        ),
        latest AS (
            SELECT symbol, adjClose, high_52w, row_count
            FROM windowed
            WHERE trade_epoch <= {target_epoch}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch DESC) = 1
        )
        SELECT
            symbol,
            adjClose / high_52w AS proximity_ratio,
            adjClose,
            high_52w,
            row_count
        FROM latest
        WHERE high_52w > 0
          AND adjClose > 0
          AND row_count >= {MIN_HISTORY_ROWS}
          AND adjClose / high_52w <= {MAX_PROXIMITY}
        ORDER BY proximity_ratio DESC
        LIMIT {MAX_STOCKS}
    """).fetchall()

    return [(r[0], r[1]) for r in rows]  # list of (symbol, proximity_ratio)


def screen_stocks(con, target_date, mktcap_min, verbose=False):
    """Screen for 52-week high proximity stocks at target_date.

    1. Get universe passing market cap filter (point-in-time)
    2. Compute proximity ratio for universe
    3. Select top MAX_STOCKS by proximity ratio

    Returns list of (symbol, market_cap, proximity_ratio).
    """
    eligible = get_eligible_symbols(con, target_date, mktcap_min)
    if not eligible:
        return []

    proximity_ranked = compute_proximity(con, list(eligible.keys()), target_date)

    result = [
        (sym, eligible.get(sym, 0), prox)
        for sym, prox in proximity_ranked
        if sym in eligible
    ]

    if verbose and result:
        top_prox = result[0][2]
        bot_prox = result[-1][2]
        print(f"    MCap-eligible: {len(eligible)}, "
              f"with proximity: {len(proximity_ranked)}, "
              f"Selected: {len(result)} "
              f"(prox range: {bot_prox:.3f}–{top_prox:.3f})")

    return result


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run 52-week high proximity backtest. Returns list of period result dicts."""
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

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _ in portfolio}

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

        avg_prox = sum(p for _, _, p in portfolio) / len(portfolio)

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "stocks_held": len(returns),
            "avg_proximity_ratio": round(avg_prox, 4),
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg prox={avg_prox:.3f}), "
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
        f"Top {MAX_STOCKS} by proximity ratio (adjClose/52w-high) | MCap>{mktcap_threshold/1e9:.0f}B local"
    )
    print_header("52-WEEK HIGH PROXIMITY BACKTEST", universe_name, exchanges, signal_desc)
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
    print(format_metrics(metrics, "52W-High Proximity", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'52W-High':>10} {'SPY':>10} {'Excess':>10}")
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
    parser = argparse.ArgumentParser(description="52-Week High Proximity multi-exchange backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("52-week-high", args_str=" ".join(cloud_args),
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

        # ASX excluded (adjClose split artifacts affect high column too).
        # SAO excluded (adjClose artifacts).
        # PAR excluded (only 1 symbol with FY key_metrics MCap data — pipeline gap).
        # TWO: no FY key_metrics data → run TAI only, TWO stocks auto-excluded by MCap filter.
        # SES: using FMP code SES (not SGX — wrong in cli_utils preset).
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),
            ("uk",          ["LSE"]),
            ("germany",     ["XETRA"]),
            ("japan",       ["JPX"]),
            ("china",       ["SHZ", "SHH"]),
            ("hongkong",    ["HKSE"]),
            ("korea",       ["KSC"]),
            ("taiwan",      ["TAI"]),
            ("canada",      ["TSX"]),
            ("switzerland", ["SIX"]),
            ("sweden",      ["STO"]),
            ("thailand",    ["SET"]),
            ("southafrica", ["JNB"]),
            ("norway",      ["OSL"]),
            ("italy",       ["MIL"]),
            ("malaysia",    ["KLS"]),
            ("singapore",   ["SES"]),
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
