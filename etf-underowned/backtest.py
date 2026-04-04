#!/usr/bin/env python3
"""
ETF Underowned Quality Backtest

Annual rebalancing (July), equal weight, top 30 quality stocks with low ETF ownership.
Uses current ETF holdings snapshot as ownership signal + point-in-time financials.

Signal: Quality stocks (ROE > 12%, D/E < 1, CR > 1.5, margin > 5%, P/E 0-40)
        held by fewer than 10 ETFs. Ranked by ROE DESC (highest quality first).
Portfolio: Top 30 quality stocks among underowned names, equal weight.
           Cash if < 10 qualify.
Rebalancing: Annual (July), 2005-2025.

Academic basis: Piotroski (2000) showed quality F-Score returns were 13.4% among
small-caps vs 5-6% among large-caps (2x more effective with less coverage).
Stambaugh et al. (2015) confirmed anomalies are stronger in hard-to-arbitrage stocks.
Merton (1987) predicts higher expected returns for stocks with limited investor awareness.

IMPORTANT CAVEAT: ETF holdings data (etf_holder) is a current snapshot, not historical.
Ownership classifications are applied retrospectively across all backtest periods.
The quality filters use point-in-time FY data. This means the ownership signal has
look-ahead bias while the quality filters do not. Results should be interpreted as
"how would a portfolio of currently under-owned quality stocks have performed?"
rather than a fully point-in-time strategy.

Usage:
    python3 etf-underowned/backtest.py
    python3 etf-underowned/backtest.py --preset india
    python3 etf-underowned/backtest.py --global --output results/exchange_comparison.json --verbose
    python3 etf-underowned/backtest.py --no-costs
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
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
ROE_MIN = 0.12             # Return on equity > 12%
DE_MAX = 1.0               # Debt/Equity < 1.0
CR_MIN = 1.5               # Current ratio > 1.5
MARGIN_MIN = 0.05          # Net profit margin > 5%
PE_MIN = 0.0               # P/E > 0 (profitable companies only)
PE_MAX = 40.0              # P/E < 40 (exclude extreme growth valuations)
MAX_ETF_COUNT = 10         # Stocks held by fewer than 10 ETFs
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (annual filings available by then)
MAX_SINGLE_RETURN = 2.0    # Cap individual stock returns at 200%
MIN_ENTRY_PRICE = 1.0      # Skip penny stocks


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch ETF ownership, financial data, and prices into DuckDB.

    Tables created:
        ownership_cache(symbol VARCHAR, etf_count INTEGER)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, priceToEarningsRatio, debtToEquityRatio,
                     currentRatio, netProfitMargin, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. ETF ownership counts (current snapshot - ALL stocks, no minimum)
    print("  Fetching ETF ownership counts...")
    ownership_sql = f"""
        SELECT
            eh.asset as symbol,
            COUNT(DISTINCT eh.symbol) as etf_count
        FROM etf_holder eh
        JOIN profile p ON eh.asset = p.symbol
        {exchange_where}
        GROUP BY eh.asset
    """
    ownership_rows = client.query(ownership_sql, verbose=verbose, timeout=300,
                                  memory_mb=4096, threads=2)
    con.execute("CREATE TABLE ownership_cache(symbol VARCHAR, etf_count INTEGER)")
    if ownership_rows:
        vals = ",".join(f"('{r['symbol']}', {r['etf_count']})" for r in ownership_rows)
        con.execute(f"INSERT INTO ownership_cache VALUES {vals}")
        print(f"    -> {len(ownership_rows)} stocks with ETF ownership data")
    else:
        print("  No ETF ownership data for these exchanges (all stocks treated as underowned)")

    if exchanges:
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter_sql = "1=1"

    # 2. Financial data (FY for point-in-time quality filters)
    queries = [
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap,
                   dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND returnOnEquity IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (ROE, market cap)"),
        ("ratios_cache", f"""
            SELECT symbol, priceToEarningsRatio, debtToEquityRatio,
                   currentRatio, netProfitMargin,
                   dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND priceToEarningsRatio IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (P/E, D/E, CR, margin)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 3. Prices at rebalance dates
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
                SELECT DISTINCT symbol FROM key_metrics WHERE period = 'FY'
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


def screen_stocks(con, target_date, mktcap_min):
    """Screen for highest-quality stocks with low ETF ownership.

    Quality filters use point-in-time FY data (45-day lag).
    Ownership uses current snapshot (static across all periods).

    Returns list of (symbol, market_cap, etf_count) sorted by ROE DESC.
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH km AS (
            SELECT symbol, returnOnEquity, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        fr AS (
            SELECT symbol, priceToEarningsRatio, debtToEquityRatio,
                   currentRatio, netProfitMargin, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ?
        )
        SELECT km.symbol, km.marketCap, COALESCE(oc.etf_count, 0) as etf_count
        FROM km
        JOIN fr ON km.symbol = fr.symbol AND fr.rn = 1
        LEFT JOIN ownership_cache oc ON km.symbol = oc.symbol
        WHERE km.rn = 1
          AND km.returnOnEquity > ?
          AND fr.priceToEarningsRatio > ?
          AND fr.priceToEarningsRatio < ?
          AND fr.debtToEquityRatio >= 0
          AND fr.debtToEquityRatio < ?
          AND fr.currentRatio > ?
          AND fr.netProfitMargin > ?
          AND km.marketCap > ?
          AND COALESCE(oc.etf_count, 0) < ?
        ORDER BY km.returnOnEquity DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch,
          ROE_MIN, PE_MIN, PE_MAX,
          DE_MAX, CR_MIN, MARGIN_MIN,
          mktcap_min, MAX_ETF_COUNT, MAX_STOCKS]).fetchall()

    return [(r[0], r[1], r[2]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run ETF Underowned Quality backtest. Returns list of period result dicts."""
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
                "avg_etf_count": 0,
                "holdings": f"CASH ({len(portfolio)} passed)",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _ in portfolio}
        avg_etf = sum(ec for _, _, ec in portfolio) / len(portfolio)

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
            "avg_etf_count": round(avg_etf, 1),
            "holdings": ",".join(symbols),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg {avg_etf:.0f} ETFs), "
                  f"port={port_return * 100:.1f}%, spy={spy_return * 100 if spy_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency,
                 periods_per_year, cash_periods, avg_stocks, avg_etf_count):
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
        "strategy": "ETF Underowned Quality",
        "universe": universe_name,
        "n_periods": len(valid),
        "years": round(len(valid) / periods_per_year, 1),
        "frequency": frequency,
        "cash_periods": cash_periods,
        "invested_periods": len(valid) - cash_periods,
        "avg_stocks_when_invested": round(avg_stocks, 1),
        "avg_etf_count": round(avg_etf_count, 1),
        "signal": {
            "roe_min": ROE_MIN,
            "de_max": DE_MAX,
            "cr_min": CR_MIN,
            "margin_min": MARGIN_MIN,
            "pe_range": [PE_MIN, PE_MAX],
            "max_etf_count": MAX_ETF_COUNT,
            "max_stocks": MAX_STOCKS,
        },
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
        "caveat": "ETF ownership signal uses current snapshot data (look-ahead bias). "
                   "Quality filters are point-in-time correct.",
    }


def run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, verbose, output_path=None,
               offset_days=1, benchmark_symbol="SPY", benchmark_name="S&P 500"):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    signal_desc = (f"Quality (ROE>{ROE_MIN*100:.0f}%, D/E<{DE_MAX}, CR>{CR_MIN}, "
                   f"margin>{MARGIN_MIN*100:.0f}%), < {MAX_ETF_COUNT} ETFs, "
                   f"MCap > {mktcap_label} local, top {MAX_STOCKS} by ROE")
    print_header("ETF UNDEROWNED QUALITY BACKTEST", universe_name, exchanges, signal_desc)
    exec_model = "next-day close (MOC)" if offset_days == 1 else "same-day close"
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2005, 2025, frequency,
                                                months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2005-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
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
    print(format_metrics(metrics, "ETF Underowned Quality", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(r["stocks_held"] for r in invested) / len(invested) if invested else 0
    avg_etf = sum(r["avg_etf_count"] for r in invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")
    print(f"  Avg ETF count (portfolio): {avg_etf:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'Underowned':>12} {benchmark_name[:10]:>10} {'Excess':>10}")
        print("  " + "-" * 42)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks, avg_etf)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="ETF Underowned Quality backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("etf-underowned", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs

    # --global mode: loop all presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        presets_to_run = [
            ("us", ["NYSE", "NASDAQ"]),     # Exclude AMEX (low FY data coverage)
            ("india", ["NSE"]),
            ("germany", ["XETRA"]),
            ("japan", ["JPX"]),
            ("uk", ["LSE"]),
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("korea", ["KSC"]),
            ("taiwan", ["TAI"]),
            ("canada", ["TSX"]),
            ("australia", ["ASX"]),
            ("sweden", ["STO"]),
            ("switzerland", ["SIX"]),
            ("brazil", ["SAO"]),
            ("thailand", ["SET"]),
            ("singapore", ["SES"]),         # SES not SGX (FMP code)
            ("southafrica", ["JNB"]),
            ("norway", ["OSL"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        offset_days = 0 if args.no_next_day else 1

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

            bsym, bname = get_local_benchmark(preset_exchanges)
            try:
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, args.verbose, output_path,
                                    offset_days=offset_days, benchmark_symbol=bsym,
                                    benchmark_name=bname)
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
        print(f"\n\n{'=' * 90}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 90}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} {'MaxDD':>8} "
              f"{'Cash%':>8} {'AvgStk':>8} {'AvgETF':>8}")
        print("-" * 90)
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
            avg_e = r.get("avg_etf_count")
            print(f"{uni:<20} {cagr if cagr is not None else 'N/A':>7}% "
                  f"{f'{excess:+.2f}' if excess is not None else 'N/A':>7}% "
                  f"{sharpe if sharpe is not None else 'N/A':>8} "
                  f"{maxdd if maxdd is not None else 'N/A':>7}% "
                  f"{cash_pct:>7.0f}% {avg if avg is not None else 'N/A':>8} "
                  f"{avg_e if avg_e is not None else 'N/A':>8}")
        print("=" * 90)
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
