#!/usr/bin/env python3
"""
Small-Cap Growth Backtest

Annual rebalancing (July), equal weight, top 30 by revenue growth.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Revenue growth > 15% YoY (FY), net income > 0, D/E < 2.0,
        market cap within small-cap range (5%–200% of exchange standard threshold)
Portfolio: Top 30 by revenue growth DESC, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), 2000–2025.

Academic references:
  - Banz (1981) "The Relationship Between Return and Market Value of Common Stocks",
    Journal of Financial Economics 9(1), 3–18. First formal documentation of size premium.
  - Fama & French (1992) "The Cross-Section of Expected Stock Returns", Journal of Finance
    47(2), 427–465. Confirmed size as a systematic risk factor.
  - Fama & French (1993) "Common Risk Factors in the Returns on Stocks and Bonds",
    Journal of Financial Economics 33(1), 3–56. Small-minus-big (SMB) factor.

Usage:
    # Backtest US stocks (default)
    python3 small-cap/backtest.py

    # Backtest Indian stocks
    python3 small-cap/backtest.py --preset india

    # Backtest all exchanges
    python3 small-cap/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 small-cap/backtest.py --no-costs

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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns, get_local_benchmark, get_benchmark_return
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, MKTCAP_THRESHOLD_MAP)

# --- Signal parameters ---
REV_GROWTH_MIN = 0.15      # Revenue growth > 15% YoY (FY)
REV_GROWTH_MAX = 5.0       # Cap at 500% to exclude data artifacts
DE_MAX = 2.0               # Debt-to-equity < 2.0 (not over-leveraged)

MAX_STOCKS = 30            # Top 30 by revenue growth, equal weight
MIN_STOCKS = 10            # Hold cash if fewer qualify
MAX_SINGLE_RETURN = 3.0    # Cap individual stock returns at 300% (small-cap volatility guard)
MIN_ENTRY_PRICE = 0.50     # Skip stocks with entry price < $0.50

# Small-cap market cap bounds (relative to exchange standard threshold)
SMALL_CAP_MIN_FACTOR = 0.05   # Lower: 5% of standard threshold (micro-cap floor)
SMALL_CAP_MAX_FACTOR = 2.0    # Upper: 200% of standard threshold (small-cap ceiling)

DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July — covers Dec/Mar FY-end companies with 45-day lag


def get_small_cap_bounds(exchanges):
    """Return (min_cap, max_cap) in local currency for small-cap filtering.

    Uses exchange-specific thresholds from MKTCAP_THRESHOLD_MAP, scaled by
    SMALL_CAP_MIN_FACTOR and SMALL_CAP_MAX_FACTOR.

    For multi-exchange presets (e.g. BSE+NSE), uses min() of thresholds
    (conservative: avoids filtering differently per exchange in same DuckDB query).

    Examples:
        US (NYSE): standard=$1B → min=$50M, max=$2B
        India (BSE): standard=₹20B → min=₹1B, max=₹40B
        Japan (JPX): standard=¥100B → min=¥5B, max=¥200B
    """
    default = 1_000_000_000

    if not exchanges:
        standard = default
    else:
        thresholds = [MKTCAP_THRESHOLD_MAP.get(ex, default) for ex in exchanges]
        standard = min(thresholds)

    return int(standard * SMALL_CAP_MIN_FACTOR), int(standard * SMALL_CAP_MAX_FACTOR)


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        income_cache(symbol, revenue, netIncome, filing_epoch, period)
        metrics_cache(symbol, marketCap, filing_epoch, period)
        ratios_cache(symbol, debtToEquityRatio, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
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

    # 2–4: Financial data
    queries = [
        ("income_cache", f"""
            SELECT symbol, revenue, netIncome, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL
              AND revenue > 0
              AND {sym_filter_sql}
        """, "income statements (revenue, net income)"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND marketCap IS NOT NULL
              AND marketCap > 0
              AND {sym_filter_sql}
        """, "key metrics (market cap)"),
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
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 5. Prices (only at rebalance windows)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    from data_utils import LOCAL_INDEX_BENCHMARKS
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
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, small_cap_min, small_cap_max):
    """Screen for small-cap growth stocks.

    Computes YoY revenue growth from two consecutive FY income statements.
    Returns list of (symbol, market_cap) tuples sorted by revenue growth DESC.
    Uses 45-day lag for point-in-time data integrity.
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH -- Current FY income (most recent before cutoff)
        inc_curr AS (
            SELECT symbol, revenue, netIncome, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        -- Prior FY income (second-most-recent before cutoff)
        inc_prev AS (
            SELECT symbol, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        rev_growth AS (
            SELECT c.symbol,
                   (c.revenue - p.revenue) / ABS(p.revenue) AS rev_growth,
                   c.netIncome
            FROM inc_curr c
            JOIN inc_prev p ON c.symbol = p.symbol AND c.rn = 1 AND p.rn = 2
            WHERE p.revenue > 0
        ),
        km AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        fr AS (
            SELECT symbol, debtToEquityRatio, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        )
        SELECT rg.symbol, km.marketCap
        FROM rev_growth rg
        JOIN km ON rg.symbol = km.symbol AND km.rn = 1
        JOIN fr ON rg.symbol = fr.symbol AND fr.rn = 1
        WHERE rg.rev_growth > ?
          AND rg.rev_growth < ?
          AND rg.netIncome > 0
          AND km.marketCap > ?
          AND km.marketCap < ?
          AND fr.debtToEquityRatio >= 0
          AND fr.debtToEquityRatio < ?
        ORDER BY rg.rev_growth DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, cutoff_epoch,
          REV_GROWTH_MIN, REV_GROWTH_MAX,
          small_cap_min, small_cap_max,
          DE_MAX, MAX_STOCKS]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, small_cap_min, small_cap_max,
                 use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run Small-Cap Growth backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, small_cap_min, small_cap_max)

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

    small_cap_min, small_cap_max = get_small_cap_bounds(exchanges)
    min_label = f"{small_cap_min/1e9:.0f}B" if small_cap_min >= 1e9 else f"{small_cap_min/1e6:.0f}M"
    max_label = f"{small_cap_max/1e9:.0f}B" if small_cap_max >= 1e9 else f"{small_cap_max/1e6:.0f}M"

    signal_desc = (f"MCap {min_label}–{max_label} local, rev growth > {REV_GROWTH_MIN*100:.0f}%, "
                   f"netIncome > 0, D/E < {DE_MAX:.0f}, top {MAX_STOCKS}")
    print_header("SMALL-CAP GROWTH BACKTEST", universe_name, exchanges, signal_desc)
    exec_model = "next-day close (MOC)" if offset_days == 1 else "same-day close (legacy)"
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
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
    print(f"\nPhase 2: Running {frequency} backtest (2000–2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, small_cap_min, small_cap_max,
                           use_costs=use_costs, verbose=verbose,
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
    print(format_metrics(metrics, "Small-Cap Growth", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        bench_label = benchmark_name[:10]
        print(f"\n  {'Year':<8} {'SmallCap':>12} {bench_label:>10} {'Excess':>10}")
        print("  " + "-" * 42)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% {ar['benchmark']*100:>9.1f}% "
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
    parser = argparse.ArgumentParser(description="Small-Cap Growth backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("small-cap", args_str=" ".join(cloud_args),
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
            ("germany", ["XETRA"]),
            ("sweden", ["STO"]),
            ("canada", ["TSX"]),
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("japan", ["JPX"]),
            ("uk", ["LSE"]),
            ("korea", ["KSC"]),
            ("switzerland", ["SIX"]),
            ("taiwan", ["TAI"]),
            ("thailand", ["SET"]),
            ("southafrica", ["JNB"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            bench_sym, bench_name = get_local_benchmark(preset_exchanges)
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
                                    benchmark_symbol=bench_sym,
                                    benchmark_name=bench_name)
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
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    output_path = args.output

    result = run_single(cr, exchanges, universe_name, frequency, use_costs,
                        risk_free_rate, args.verbose, output_path,
                        offset_days=offset_days,
                        benchmark_symbol=benchmark_symbol,
                        benchmark_name=benchmark_name)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
