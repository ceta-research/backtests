#!/usr/bin/env python3
"""
Sector-Adjusted Momentum (Relative Strength) Backtest

Buys the top 30 stocks per exchange by 12-1 month sector-adjusted return.
Signal = (stock 12M-1M return) - (equal-weighted sector average 12M-1M return).
Strips the sector effect from momentum, isolating stock-level outperformance.

No financial quality filters — pure price signal with sector adjustment.
Universe filter: Market cap > exchange threshold, Price > $1, Sector known.

Signal: Top 30 by (12M-1M return − sector-avg 12M-1M return)
        Universe: MCap > exchange threshold, Price > $1, sector != 'Unknown'
        Min 5 stocks in sector to compute meaningful average (else excluded)
Portfolio: Equal weight top 30. Cash if < 10 qualify.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2000-2025.
Costs: Size-tiered (see costs.py). Excludes: ASX, SAO (adjClose artifacts).

Academic basis:
  Moskowitz, T. & Grinblatt, M. (1999). "Do Industries Explain Momentum?"
  Journal of Finance, 54(4), 1249-1290.

  Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling Losers:
  Implications for Stock Market Efficiency." Journal of Finance, 48(1), 65-91.

Usage:
    python3 relative-strength/backtest.py                            # US default
    python3 relative-strength/backtest.py --preset india
    python3 relative-strength/backtest.py --global --output results/exchange_comparison.json
    python3 relative-strength/backtest.py --preset us --no-costs --verbose
"""

import duckdb
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import (query_parquet, get_prices, generate_rebalance_dates, filter_returns,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
MOMENTUM_DAYS = 365     # 12-month lookback start
SKIP_DAYS = 30          # Skip last month (avoid short-term reversal, per J&T 1993)
MOMENTUM_WINDOW = 30    # Days to search for price near the lookback start/end
MOMENTUM_LOOKBACK_FETCH = 410  # Days before rebalance to fetch prices (365 + 30 skip + 15 buffer)
MAX_STOCKS = 30         # Top N by relative strength, equal weight
MIN_STOCKS = 10         # Hold cash if fewer qualify
MIN_SECTOR_SIZE = 5     # Min stocks in sector to compute meaningful average (else excluded)
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]  # Jan / Apr / Jul / Oct
MAX_SINGLE_RETURN = 2.0  # Cap at 200% per stock for portfolio returns (data quality guard)
MIN_ENTRY_PRICE = 1.0    # Skip sub-$1 entry prices (price data artifacts)
MIN_MOMENTUM_PRICE = 1.0 # Skip stocks where momentum-start OR momentum-end price < $1
MAX_MOMENTUM = 5.0       # Cap raw 12M-1M signal at 500% (data quality guard)

# Development compute (max resources)
FETCH_MEMORY_MB = 4096
FETCH_THREADS = 2


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, benchmark_symbol="SPY"):
    """Fetch all data needed for sector-adjusted momentum backtest.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        sector_cache(symbol VARCHAR, sector VARCHAR)
            -- GICS sector from profile table (relatively static classification)
        metrics_cache(symbol, marketCap, filing_epoch, period)
            -- used for point-in-time market cap filter only
        prices_cache(symbol, trade_epoch, adjClose)
            -- extended window: 12M lookback + skip window + entry date per rebalance date

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
    con.execute("SET memory_limit='8GB'")

    # 1. Universe + sector data (exchange membership + GICS sector from profile)
    print("  Fetching exchange membership + sector data...")
    profile_sql = f"""
        SELECT DISTINCT symbol, exchange, sector
        FROM profile
        {exchange_where}
        AND sector IS NOT NULL AND sector != ''
    """
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    print(f"  Universe: {len(profiles)} symbols with sector data")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    sector_values = ",".join(
        f"('{r['symbol']}', '{r['sector'].replace(chr(39), chr(39)+chr(39))}')"
        for r in profiles
    )

    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")
    con.execute(
        "CREATE TABLE sector_cache(symbol VARCHAR, sector VARCHAR);"
        f" INSERT INTO sector_cache VALUES {sector_values}"
    )

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

    # 3. Prices — extended window covering all rebalance dates
    print("  Fetching prices (extended momentum + skip + entry window)...")
    date_conditions = []
    for d in rebalance_dates:
        momentum_start = d - timedelta(days=MOMENTUM_LOOKBACK_FETCH)
        entry_end = d + timedelta(days=12)  # +12 for MOC offset_days=1 with window_days=10
        date_conditions.append(
            f"(date >= '{momentum_start.isoformat()}' AND date <= '{entry_end.isoformat()}')"
        )
    date_filter = " OR ".join(date_conditions)

    # Build benchmark symbol list: SPY + local index for all exchanges
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    if benchmark_symbol:
        bench_symbols.add(f"'{benchmark_symbol}'")
    bench_list = ", ".join(bench_symbols)

    exchange_fy_filter = f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""
    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
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
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=15000000, timeout=600,
                          memory_mb=FETCH_MEMORY_MB, threads=FETCH_THREADS)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def get_eligible_symbols(con, target_date, mktcap_min):
    """Get symbols passing market cap filter at target_date (45-day filing lag).

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


def compute_relative_strength(con, symbols, target_date):
    """Compute sector-adjusted 12-1 month momentum for a set of symbols.

    Algorithm:
    1. Compute 12M-1M raw momentum for each eligible symbol
    2. Group by sector, compute equal-weighted sector averages (min MIN_SECTOR_SIZE stocks)
    3. Relative strength = raw momentum - sector average
    4. Return only symbols where sector average could be computed

    Returns dict: {symbol: (relative_strength, raw_momentum, sector, sector_avg)}
    """
    if not symbols:
        return {}

    lookback_start = target_date - timedelta(days=MOMENTUM_DAYS)
    lookback_end = target_date - timedelta(days=SKIP_DAYS)

    start_prices = get_prices(con, symbols, lookback_start, window_days=MOMENTUM_WINDOW)
    end_prices = get_prices(con, symbols, lookback_end, window_days=MOMENTUM_WINDOW)

    # Get sector mapping for eligible symbols
    if symbols:
        sym_list = ", ".join(f"'{s}'" for s in symbols)
        sector_rows = con.execute(
            f"SELECT symbol, sector FROM sector_cache WHERE symbol IN ({sym_list})"
        ).fetchall()
        sector_map = {r[0]: r[1] for r in sector_rows}
    else:
        sector_map = {}

    # Compute raw momentum for each symbol
    raw_momentum = {}
    for sym in symbols:
        start = start_prices.get(sym)
        end = end_prices.get(sym)
        if not (start and end):
            continue
        if start < MIN_MOMENTUM_PRICE or end < MIN_MOMENTUM_PRICE:
            continue
        mom = (end - start) / start
        if mom > MAX_MOMENTUM:
            continue
        sector = sector_map.get(sym)
        if not sector:
            continue  # No sector data for this symbol
        raw_momentum[sym] = (mom, sector)

    # Group raw momentum by sector (only eligible symbols)
    sector_returns = {}
    sector_counts = {}
    for sym, (mom, sector) in raw_momentum.items():
        if sector not in sector_returns:
            sector_returns[sector] = []
        sector_returns[sector].append(mom)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    # Compute sector averages (only for sectors with enough stocks)
    sector_avg = {
        sector: sum(returns) / len(returns)
        for sector, returns in sector_returns.items()
        if len(returns) >= MIN_SECTOR_SIZE
    }

    # Relative strength = raw momentum - sector average
    result = {}
    for sym, (mom, sector) in raw_momentum.items():
        if sector not in sector_avg:
            continue  # Skip symbols in under-represented sectors
        avg = sector_avg[sector]
        rs = mom - avg
        result[sym] = (rs, mom, sector, avg)

    return result


def screen_stocks(con, target_date, mktcap_min, verbose=False):
    """Screen for relative strength stocks at target_date.

    1. Get universe passing market cap filter (point-in-time)
    2. Compute sector-adjusted momentum for universe
    3. Select top MAX_STOCKS by relative strength

    Returns list of (symbol, market_cap, relative_strength, raw_mom, sector, sector_avg)
    sorted by relative_strength descending.
    """
    eligible = get_eligible_symbols(con, target_date, mktcap_min)
    if not eligible:
        return []

    rs_data = compute_relative_strength(con, list(eligible.keys()), target_date)

    candidates = [
        (sym, eligible[sym], rs, mom, sector, sec_avg)
        for sym, (rs, mom, sector, sec_avg) in rs_data.items()
    ]

    candidates.sort(key=lambda x: x[2], reverse=True)
    result = candidates[:MAX_STOCKS]

    if verbose and result:
        top_rs = result[0][2] * 100
        bot_rs = result[-1][2] * 100
        sectors_held = len(set(r[4] for r in result))
        print(f"    MCap-eligible: {len(eligible)}, "
              f"with RS signal: {len(rs_data)}, "
              f"Selected: {len(result)} "
              f"(RS range: {bot_rs:.0f}%–{top_rs:.0f}%, "
              f"{sectors_held} sectors)")

    return result


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run sector-adjusted momentum backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min, verbose=verbose)

        if len(portfolio) < MIN_STOCKS:
            bench_return = get_benchmark_return(
                con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": bench_return,
                "stocks_held": 0,
                "holdings": f"CASH ({len(portfolio)} passed, need {MIN_STOCKS})",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _, _, _, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _, _, _, _ in portfolio}

        entry_prices = get_prices(con, symbols, entry_date, offset_days=offset_days)
        exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)

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

        bench_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

        avg_rs = (sum(r[2] for r in portfolio) / len(portfolio)) * 100
        n_sectors = len(set(r[4] for r in portfolio))

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(bench_return, 6) if bench_return is not None else None,
            "stocks_held": len(returns),
            "avg_relative_strength": round(avg_rs, 1),
            "n_sectors": n_sectors,
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg RS={avg_rs:.0f}%, "
                  f"{n_sectors}sec), "
                  f"port={port_return * 100:.1f}%, "
                  f"bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency, periods_per_year,
                 cash_periods, avg_stocks, avg_sectors, benchmark_name="S&P 500",
                 benchmark_symbol="SPY"):
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
        "avg_sectors_when_invested": round(avg_sectors, 1),
        "benchmark_name": benchmark_name,
        "benchmark_symbol": benchmark_symbol,
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
               risk_free_rate, mktcap_threshold, verbose, output_path=None, offset_days=1):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "MOC (next-day close)" if offset_days == 1 else "same-day close (legacy)"

    signal_desc = (
        f"Top {MAX_STOCKS} by sector-adj 12M-1M return | MCap>{mktcap_threshold/1e9:.0f}B local"
    )
    print_header("SECTOR-ADJUSTED MOMENTUM (RELATIVE STRENGTH) BACKTEST",
                 universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency,
                                               months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose,
                             benchmark_symbol=benchmark_symbol)
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
    print(format_metrics(metrics, "Relative Strength", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    avg_sectors_list = [r.get("n_sectors", 0) for r in results if r.get("n_sectors", 0) > 0]
    avg_sectors = sum(avg_sectors_list) / len(avg_sectors_list) if avg_sectors_list else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")
    print(f"  Avg sectors (invested): {avg_sectors:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'RS Strat':>10} {benchmark_name:>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks, avg_sectors,
                          benchmark_name=benchmark_name, benchmark_symbol=benchmark_symbol)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Sector-Adjusted Momentum (Relative Strength) multi-exchange backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("relative-strength", args_str=" ".join(cloud_args),
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

    # --global mode: loop all eligible exchange presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}

        # ASX excluded (adjClose split artifacts).
        # SAO excluded (adjClose artifacts; fine for event studies, not price strategies).
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
                                    use_costs, rfr, mktcap_threshold, args.verbose, output_path,
                                    offset_days=offset_days)
                if result:
                    all_results[uni_name] = result
            except Exception as e:
                print(f"\n  ERROR on {uni_name}: {e}")
                import traceback
                traceback.print_exc()
                all_results[uni_name] = {"error": str(e)}

        # Save comparison JSON
        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Print summary table
        print(f"\n\n{'=' * 85}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 85}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>8} {'AvgStk':>8} {'AvgSec':>7}")
        print("-" * 85)
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
            avgsec = r.get("avg_sectors_when_invested")
            print(f"{uni:<20} {cagr if cagr is not None else 'N/A':>7}% "
                  f"{f'{excess:+.2f}' if excess is not None else 'N/A':>7}% "
                  f"{sharpe if sharpe is not None else 'N/A':>8} "
                  f"{maxdd if maxdd is not None else 'N/A':>7}% "
                  f"{cash_pct:>7.0f}% {avg if avg is not None else 'N/A':>8} "
                  f"{avgsec if avgsec is not None else 'N/A':>7}")
        print("=" * 85)
        return

    # Single exchange mode
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, mktcap_threshold, args.verbose, args.output,
               offset_days=offset_days)


if __name__ == "__main__":
    main()
