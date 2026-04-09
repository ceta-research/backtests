#!/usr/bin/env python3
"""
Volume-Confirmed Momentum Backtest

Semi-annual rebalancing (Jan/Jul), equal weight, top 30 by skip-last-month 12M return,
confirmed by rising 3-month vs 12-month average daily volume.

Signal:
  - 12M price return (skip last month): (price at T-30d) / (price at T-365d) - 1
    Skip-last-month avoids the well-documented short-term reversal in momentum stocks.
  - Volume confirmation: 3-month avg daily volume > 12-month avg daily volume (vol_ratio > 1.0)
    Rising volume confirms institutional buying interest behind the price trend.
  - Minimal quality gate: netIncome > 0 AND operatingCashFlow > 0 (FY, 45-day lag)
    Eliminates speculative momentum in money-losing stocks.
  - Market cap > exchange threshold (standard mid-to-large cap filter)

Portfolio: Top 30 by momentum score, equal weight. Cash if < 10 qualify.
Rebalancing: Semi-annual (Jan 1, Jul 1), 2001-2025.

Academic basis:
  Lee, C.M.C. & Swaminathan, B. (2000). "Price Momentum and Trading Volume."
  Journal of Finance, 55(5), 2017-2069.
  Key finding: High-volume momentum stocks sustain returns longer and exhibit
  stronger momentum. Low-volume momentum reverses faster.

Usage:
    python3 volume-confirmed-momentum/backtest.py                          # US default
    python3 volume-confirmed-momentum/backtest.py --preset india
    python3 volume-confirmed-momentum/backtest.py --global --output results/exchange_comparison.json
    python3 volume-confirmed-momentum/backtest.py --preset us --no-costs --verbose

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
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
MOMENTUM_LOOKBACK_DAYS = 365    # 12M momentum lookback period
SKIP_DAYS = 30                  # Skip-last-month: compute momentum from T-30d (not T)
SKIP_WINDOW = 15                # Search window for skip-month price (handles weekends/holidays)
MOMENTUM_LOOKBACK_FETCH = 400   # Days before rebalance to start price fetch (includes buffer)
VOLUME_LOOKBACK_3M_DAYS = 95   # Calendar days for 3-month volume average (~63 trading days)
MIN_VOLUME_DAYS = 60            # Minimum days of non-null, non-zero volume required
VOLUME_RATIO_MIN = 1.0          # 3M avg volume must exceed 12M avg (rising volume trend)
MAX_STOCKS = 30                 # Top N by momentum, equal weight
MIN_STOCKS = 10                 # Hold cash if fewer qualify
DEFAULT_FREQUENCY = "semi-annual"
DEFAULT_REBALANCE_MONTHS = [1, 7]   # Jan / Jul
MAX_SINGLE_RETURN = 2.0         # Cap at 200% per stock (data quality guard)
MIN_ENTRY_PRICE = 1.0           # Skip sub-$1 entry prices (adjClose artifacts)


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, benchmark_symbols=None):
    """Fetch all data for volume-confirmed momentum backtest.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, filing_epoch, period)
        cashflow_cache(symbol, operatingCashFlow, filing_epoch, period)
        metrics_cache(symbol, marketCap, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose, volume)
            -- extended window: 12M momentum lookback + volume trend + entry/exit prices

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
        sym_filter_sql = (
            f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
        )
    else:
        sym_filter_sql = "1=1"

    # 2-4: Financial data (FY only, minimal quality filter: NI > 0, OCF > 0, MCap)
    queries = [
        ("income_cache", f"""
            SELECT symbol, netIncome, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "income statements (NI)"),
        ("cashflow_cache", f"""
            SELECT symbol, operatingCashFlow, dateEpoch as filing_epoch, period
            FROM cash_flow_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "cash flow statements (OCF)"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY' AND marketCap IS NOT NULL AND {sym_filter_sql}
        """, "key metrics (market cap)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              memory_mb=4096, threads=2)
        print(f"    -> {count} rows")

    # 5. Prices + Volume — extended window per rebalance date
    # Window per date: [T - 400 days, T + 10 days]
    #   - Covers 12M momentum lookback (T-365d), skip-month price (T-30d)
    #   - Covers 12M volume history for trend computation
    #   - Covers entry price at T and exit price at next rebalance T
    # Symbol filter: ever-profitable symbols only (reduces price data volume ~50%)
    print("  Fetching prices + volume (extended momentum + volume window)...")
    date_conditions = []
    for d in rebalance_dates:
        momentum_start = d - timedelta(days=MOMENTUM_LOOKBACK_FETCH)
        entry_end = d + timedelta(days=10)
        date_conditions.append(
            f"(date >= '{momentum_start.isoformat()}' AND date <= '{entry_end.isoformat()}')"
        )
    date_filter = " OR ".join(date_conditions)

    if sym_filter_sql != "1=1":
        exchange_fy_filter = f"AND {sym_filter_sql}"
    else:
        exchange_fy_filter = ""

    bench_set = {"'SPY'"}
    if benchmark_symbols:
        for sym in benchmark_symbols:
            bench_set.add(f"'{sym}'")
    bench_list = ", ".join(bench_set)

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR symbol IN (
                SELECT DISTINCT i.symbol FROM income_statement i
                WHERE i.period = 'FY' AND i.netIncome > 0 {exchange_fy_filter}
                INTERSECT
                SELECT DISTINCT c.symbol FROM cash_flow_statement c
                WHERE c.period = 'FY' AND c.operatingCashFlow > 0 {exchange_fy_filter}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=10000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows ({MOMENTUM_LOOKBACK_FETCH}-day window per date)")

    # Verify volume column exists in prices_cache
    cols = [r[0] for r in con.execute("DESCRIBE prices_cache").fetchall()]
    if "volume" not in cols:
        print("  WARNING: 'volume' column not found in stock_eod data. Volume ratio filter disabled.")
        con.execute("ALTER TABLE prices_cache ADD COLUMN volume DOUBLE DEFAULT NULL")

    return con


def screen_quality(con, target_date, mktcap_min):
    """Apply minimal quality filter at target_date. Returns dict: {symbol: market_cap}.

    Quality criteria (FY annual data, 45-day filing lag for point-in-time integrity):
      - netIncome > 0          (profitable, prevents speculative momentum in money-losers)
      - operatingCashFlow > 0  (real cash generation, not just accounting earnings)
      - marketCap > threshold  (exchange-specific mid-to-large cap filter)
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        inc AS (
            SELECT symbol, netIncome, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ?
        ),
        cf AS (
            SELECT symbol, operatingCashFlow, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM cashflow_cache
            WHERE filing_epoch <= ?
        ),
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        )
        SELECT met.symbol, met.marketCap
        FROM met
        JOIN inc ON met.symbol = inc.symbol AND inc.rn = 1
        JOIN cf  ON met.symbol = cf.symbol  AND cf.rn  = 1
        WHERE met.rn = 1
          AND inc.netIncome > 0
          AND cf.operatingCashFlow > 0
          AND met.marketCap > ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, mktcap_min]).fetchall()

    return {r[0]: r[1] for r in rows}


def compute_momentum_skip1m(con, symbols, target_date):
    """Compute skip-last-month 12M price return for quality-passing symbols.

    Returns dict: {symbol: momentum_return}
    Momentum = (price at T-30d) / (price at T-365d) - 1

    Skip-last-month is standard practice in momentum strategies to avoid the
    well-documented short-term reversal effect (Jegadeesh 1990, Lehmann 1990).
    """
    if not symbols:
        return {}

    # Skip-month price: ~1 month before rebalance (first trading day in [T-30d, T-15d])
    skip_date = target_date - timedelta(days=SKIP_DAYS)
    skip_prices = get_prices(con, symbols, skip_date, window_days=SKIP_WINDOW)

    # 12M lookback price: ~12 months before (first trading day in [T-365d, T-335d])
    lookback_date = target_date - timedelta(days=MOMENTUM_LOOKBACK_DAYS)
    lookback_prices = get_prices(con, symbols, lookback_date, window_days=30)

    momentum = {}
    for sym in symbols:
        skip = skip_prices.get(sym)
        past = lookback_prices.get(sym)
        if skip and past and past > 0 and skip > 0:
            momentum[sym] = (skip - past) / past

    return momentum


def compute_volume_ratio(con, symbols, target_date):
    """Compute 3M vs 12M average daily volume ratio for symbols.

    Returns dict: {symbol: vol_ratio}
    vol_ratio = avg_daily_volume_3m / avg_daily_volume_12m

    Ratio > 1.0 signals recent volume above trailing average (rising buying activity).
    Symbols with fewer than MIN_VOLUME_DAYS of non-null volume are excluded.
    """
    if not symbols:
        return {}

    cutoff_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    start_epoch_12m = int(datetime.combine(
        target_date - timedelta(days=MOMENTUM_LOOKBACK_FETCH), datetime.min.time()
    ).timestamp())
    cutoff_3m_epoch = int(datetime.combine(
        target_date - timedelta(days=VOLUME_LOOKBACK_3M_DAYS), datetime.min.time()
    ).timestamp())

    sym_list = ", ".join(f"'{s}'" for s in symbols)

    try:
        rows = con.execute(f"""
            WITH vol_data AS (
                SELECT symbol,
                       AVG(CASE WHEN trade_epoch >= {cutoff_3m_epoch}
                                 AND volume IS NOT NULL AND volume > 0
                                THEN volume END) as avg_vol_3m,
                       AVG(CASE WHEN volume IS NOT NULL AND volume > 0
                                THEN volume END) as avg_vol_12m,
                       COUNT(CASE WHEN volume IS NOT NULL AND volume > 0
                                  THEN 1 END) as n_days_with_volume
                FROM prices_cache
                WHERE symbol IN ({sym_list})
                  AND trade_epoch <= {cutoff_epoch}
                  AND trade_epoch >= {start_epoch_12m}
                GROUP BY symbol
                HAVING COUNT(CASE WHEN volume IS NOT NULL AND volume > 0
                                  THEN 1 END) >= {MIN_VOLUME_DAYS}
                   AND AVG(CASE WHEN volume IS NOT NULL AND volume > 0
                                THEN volume END) > 0
                   AND AVG(CASE WHEN trade_epoch >= {cutoff_3m_epoch}
                                 AND volume IS NOT NULL AND volume > 0
                                THEN volume END) IS NOT NULL
            )
            SELECT symbol,
                   avg_vol_3m / NULLIF(avg_vol_12m, 0) as vol_ratio
            FROM vol_data
            WHERE avg_vol_3m IS NOT NULL
        """).fetchall()
    except Exception:
        return {}

    return {r[0]: r[1] for r in rows}


def screen_stocks(con, target_date, mktcap_min, verbose=False):
    """Screen for volume-confirmed momentum stocks at target_date.

    Steps:
    1. Quality filter: NI > 0, OCF > 0, MCap > threshold
    2. Skip-last-month momentum: price return from T-365d to T-30d
    3. Volume confirmation: 3M avg volume > 12M avg volume
    4. Combined filter: momentum > 0 AND vol_ratio > 1.0
    5. Select top MAX_STOCKS by momentum score, equal weight

    Returns list of (symbol, market_cap, momentum, vol_ratio) tuples, sorted by momentum desc.
    """
    # Step 1: Quality filter
    quality_stocks = screen_quality(con, target_date, mktcap_min)
    if not quality_stocks:
        return []

    quality_symbols = list(quality_stocks.keys())

    # Step 2: Momentum (skip-last-month)
    momentum = compute_momentum_skip1m(con, quality_symbols, target_date)
    if not momentum:
        return []

    # Step 3: Volume ratio
    # Only compute for symbols that passed quality and have positive momentum (reduces compute)
    positive_mom_symbols = [s for s, m in momentum.items() if m > 0]
    vol_ratios = compute_volume_ratio(con, positive_mom_symbols, target_date)

    # Step 4: Combined filter - positive momentum AND rising volume
    candidates = []
    for sym, mom in momentum.items():
        if mom <= 0:
            continue
        vol_ratio = vol_ratios.get(sym)
        if vol_ratio is None or vol_ratio < VOLUME_RATIO_MIN:
            continue
        candidates.append((sym, quality_stocks[sym], mom, vol_ratio))

    # Step 5: Sort by momentum descending, take top MAX_STOCKS
    candidates.sort(key=lambda x: x[2], reverse=True)
    result = candidates[:MAX_STOCKS]

    if verbose and result:
        top_mom = result[0][2] * 100
        bot_mom = result[-1][2] * 100
        vol_confirmed = len(vol_ratios)
        print(f"    Quality: {len(quality_stocks)}, "
              f"with momentum: {len(momentum)}, "
              f"positive+vol_confirmed: {len(candidates)}, "
              f"selected: {len(result)} "
              f"(mom range: {bot_mom:.0f}%–{top_mom:.0f}%)")

    return result


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run volume-confirmed momentum backtest. Returns list of period result dicts."""
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

        symbols = [s for s, _, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _, _ in portfolio}
        moms = {s: m for s, _, m, _ in portfolio}

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

        avg_mom = (sum(moms[s] for s in [s for s, _, _, _ in portfolio]) / len(portfolio)) * 100

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(bench_return, 6) if bench_return is not None else None,
            "stocks_held": len(returns),
            "avg_momentum_12m": round(avg_mom, 1),
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg 12M mom={avg_mom:.0f}%), "
                  f"port={port_return * 100:.1f}%, "
                  f"bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency, periods_per_year,
                 cash_periods, avg_stocks, benchmark_name="S&P 500", benchmark_symbol="SPY"):
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
        "benchmark_name": benchmark_name,
        "benchmark_symbol": benchmark_symbol,
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
               risk_free_rate, mktcap_threshold, verbose, output_path=None,
               offset_days=1, benchmark_symbol="SPY", benchmark_name="S&P 500"):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B"
                    if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M")
    signal_desc = (
        f"NI>0, OCF>0, MCap>{mktcap_label} local → "
        f"12M mom (skip 1M) > 0 + vol_ratio > {VOLUME_RATIO_MIN} → "
        f"top {MAX_STOCKS} by momentum"
    )
    exec_model = "next-day close (MOC)" if offset_days == 1 else "same-bar (legacy)"
    print_header("VOLUME-CONFIRMED MOMENTUM BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    # Start from 2001 (need 12M price history from Jan 2000 for first Jul 2001 rebalance)
    rebalance_dates = generate_rebalance_dates(2001, 2025, frequency,
                                               months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    bench_symbols = {benchmark_symbol, "SPY"}
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose,
                             benchmark_symbols=bench_symbols)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    print(f"\nPhase 2: Running {frequency} backtest (2001-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    valid = [r for r in results
             if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "Volume-Confirmed Momentum", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    bench_abbr = benchmark_name[:8]
    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'VolMom':>10} {bench_abbr:>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks,
                          benchmark_name=benchmark_name, benchmark_symbol=benchmark_symbol)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
                    exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="Volume-Confirmed Momentum backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("volume-confirmed-momentum", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    offset_days = 0 if getattr(args, "no_next_day", False) else 1

    # --global mode: loop all eligible exchange presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}

        # Exchange list: broad coverage
        # ASX/SAO excluded (adjClose artifacts per METHODOLOGY.md)
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),
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
            ("norway",      ["OSL"]),
            ("italy",       ["MIL"]),
            ("malaysia",    ["KLS"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            mktcap_threshold = get_mktcap_threshold(preset_exchanges)
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
                                    use_costs, rfr, mktcap_threshold, args.verbose, output_path,
                                    offset_days=offset_days,
                                    benchmark_symbol=bench_sym, benchmark_name=bench_name)
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
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, mktcap_threshold, args.verbose, args.output,
               offset_days=offset_days,
               benchmark_symbol=benchmark_symbol, benchmark_name=benchmark_name)


if __name__ == "__main__":
    main()
