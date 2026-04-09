#!/usr/bin/env python3
"""
52-Week Low Quality Backtest

Quarterly rebalancing (Jan/Apr/Jul/Oct), equal weight, top 30 by price proximity.
Fetches full price history for 52-week low computation. Caches in DuckDB, runs locally.

Signal: Price within 15% of 52-week low + Piotroski F-score >= 7
Portfolio: Top 30 sorted by proximity to 52-week low ASC, equal weight. Cash if < 5 qualify.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2002-2025.

Academic reference:
- De Bondt & Thaler (1985) "Does the Stock Market Overreact?", Journal of Finance.
  Mean reversion: past losers outperform past winners over 3-5 year horizons.
- Piotroski (2000) "Value Investing: The Use of Historical Financial Statement
  Information to Separate Winners from Losers", Journal of Accounting Research.
  F-score >= 7 distinguishes financially strong from weak companies.

Usage:
    # Backtest US stocks (default)
    python3 52-week-low/backtest.py

    # Backtest Indian stocks
    python3 52-week-low/backtest.py --preset india

    # Backtest all exchanges (loop)
    python3 52-week-low/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 52-week-low/backtest.py --no-costs

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
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
PROXIMITY_THRESHOLD = 0.15   # Within 15% of 52-week low: (price - low) / low <= 0.15
PIOTROSKI_MIN = 7            # Minimum Piotroski F-score (0-9 scale)
MAX_STOCKS = 30
MIN_STOCKS = 5               # Lower threshold: strategy has sparse universe by design
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]  # Jan, Apr, Jul, Oct
MAX_SINGLE_RETURN = 2.0      # Cap individual stock returns at 200% (data quality guard)
MIN_ENTRY_PRICE = 1.0        # Skip stocks with entry price < $1 (price data artifact)
FILING_LAG_DAYS = 45         # Days to lag annual filings (point-in-time safety)
LOOKBACK_DAYS = 365          # 52-week lookback for price low


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and full price history, load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, grossProfit, revenue, filing_epoch, period)
        balance_cache(symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                      longTermDebt, totalStockholdersEquity, filing_epoch, period)
        cashflow_cache(symbol, operatingCashFlow, filing_epoch, period)
        metrics_cache(symbol, marketCap, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Note: Full price history (not just at rebalance dates) is fetched to support
    52-week low computation at every quarterly rebalance.

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

    # 2. Annual financial data (for Piotroski score)
    queries = [
        ("income_cache", f"""
            SELECT symbol, netIncome, grossProfit, revenue, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY'
              AND netIncome IS NOT NULL
              AND {sym_filter_sql}
        """, "income statements (net income, gross profit, revenue)"),
        ("balance_cache", f"""
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                   longTermDebt, totalStockholdersEquity, dateEpoch as filing_epoch, period
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalAssets IS NOT NULL
              AND totalAssets > 0
              AND {sym_filter_sql}
        """, "balance sheets (assets, liabilities, equity)"),
        ("cashflow_cache", f"""
            SELECT symbol, operatingCashFlow, dateEpoch as filing_epoch, period
            FROM cash_flow_statement
            WHERE period = 'FY'
              AND operatingCashFlow IS NOT NULL
              AND {sym_filter_sql}
        """, "cash flow statements (operating CF)"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND marketCap IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (market cap)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              memory_mb=16384, threads=6)
        print(f"    -> {count} rows")

    # 3. Full price history for 52-week low computation
    # Need: (first rebalance - LOOKBACK_DAYS) to (last rebalance + 10 days)
    first_date = rebalance_dates[0] - timedelta(days=LOOKBACK_DAYS + 5)
    last_date = rebalance_dates[-1] + timedelta(days=15)

    # Build benchmark symbol list (SPY + local index if applicable)
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(bench_symbols)

    print(f"  Fetching full price history ({first_date.year}-{last_date.year})...")
    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE date >= '{first_date.isoformat()}'
          AND date <= '{last_date.isoformat()}'
          AND adjClose IS NOT NULL
          AND adjClose > 0
          AND (
            symbol IN ({bench_list})
            OR {sym_filter_sql}
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=50000000, timeout=600,
                          memory_mb=16384, threads=6)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def compute_piotroski_scores(con, cutoff_epoch, prev_year_epoch):
    """Compute Piotroski F-scores for all symbols with data.

    Returns dict: {symbol: (score, market_cap)}

    Components (0 or 1 each, sum 0-9):
    F1: Net income > 0
    F2: Operating cash flow > 0
    F3: ROA improving YoY
    F4: Accruals: OCF / assets > NI / assets (cash quality)
    F5: Leverage decreasing (LT debt ratio falling)
    F6: Liquidity improving (current ratio rising)
    F7: No dilution (equity not shrinking - proxy for no new shares issued)
    F8: Asset turnover improving
    F9: Gross margin improving
    """
    rows = con.execute("""
        WITH
        inc_curr AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        inc_prev AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ?
        ),
        bal_curr AS (
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                longTermDebt, totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        bal_prev AS (
            SELECT symbol, totalAssets, longTermDebt, totalCurrentAssets,
                totalCurrentLiabilities, totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache
            WHERE filing_epoch <= ?
        ),
        cf_curr AS (
            SELECT symbol, operatingCashFlow, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM cashflow_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        scored AS (
            SELECT ic.symbol,
                -- Profitability signals
                CASE WHEN ic.netIncome > 0 THEN 1 ELSE 0 END AS f1_ni,
                CASE WHEN cf.operatingCashFlow > 0 THEN 1 ELSE 0 END AS f2_ocf,
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.netIncome / bc.totalAssets) > (ip.netIncome / bp.totalAssets)
                     THEN 1 ELSE 0 END AS f3_roa,
                CASE WHEN bc.totalAssets > 0
                     AND cf.operatingCashFlow / bc.totalAssets > ic.netIncome / bc.totalAssets
                     THEN 1 ELSE 0 END AS f4_accrual,
                -- Leverage / liquidity signals
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (COALESCE(bc.longTermDebt, 0) / bc.totalAssets)
                       < (COALESCE(bp.longTermDebt, 0) / bp.totalAssets)
                     THEN 1 ELSE 0 END AS f5_leverage,
                CASE WHEN bc.totalCurrentLiabilities > 0 AND bp.totalCurrentLiabilities > 0
                     AND (bc.totalCurrentAssets / bc.totalCurrentLiabilities)
                       > (bp.totalCurrentAssets / bp.totalCurrentLiabilities)
                     THEN 1 ELSE 0 END AS f6_liquidity,
                -- Operating efficiency signals
                CASE WHEN bc.totalStockholdersEquity >= bp.totalStockholdersEquity
                     THEN 1 ELSE 0 END AS f7_no_dilution,
                CASE WHEN ic.revenue > 0 AND ip.revenue > 0 AND bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.revenue / bc.totalAssets) > (ip.revenue / bp.totalAssets)
                     THEN 1 ELSE 0 END AS f8_turnover,
                CASE WHEN ic.grossProfit > 0 AND ip.grossProfit > 0
                     AND ic.revenue > 0 AND ip.revenue > 0
                     AND (ic.grossProfit / ic.revenue) > (ip.grossProfit / ip.revenue)
                     THEN 1 ELSE 0 END AS f9_margin,
                m.marketCap
            FROM inc_curr ic
            JOIN inc_prev ip ON ic.symbol = ip.symbol AND ip.rn = 1
            JOIN bal_curr bc ON ic.symbol = bc.symbol AND bc.rn = 1
            JOIN bal_prev bp ON ic.symbol = bp.symbol AND bp.rn = 1
            JOIN cf_curr cf ON ic.symbol = cf.symbol AND cf.rn = 1
            JOIN met m ON ic.symbol = m.symbol AND m.rn = 1
            WHERE ic.rn = 1
        )
        SELECT symbol,
            (f1_ni + f2_ocf + f3_roa + f4_accrual + f5_leverage + f6_liquidity
             + f7_no_dilution + f8_turnover + f9_margin) AS f_score,
            marketCap
        FROM scored
    """, [
        cutoff_epoch, prev_year_epoch,   # inc_curr window
        prev_year_epoch,                  # inc_prev upper bound
        cutoff_epoch, prev_year_epoch,   # bal_curr window
        prev_year_epoch,                  # bal_prev upper bound
        cutoff_epoch, prev_year_epoch,   # cf_curr window
        cutoff_epoch,                     # met upper bound
    ]).fetchall()

    return {r[0]: (r[1], r[2]) for r in rows}


def screen_stocks(con, target_date, mktcap_min):
    """Screen for quality stocks near 52-week lows.

    Steps:
    1. Get current price and 52-week low per stock
    2. Filter: (current_price - low_52w) / low_52w <= PROXIMITY_THRESHOLD
    3. Filter: Piotroski F-score >= PIOTROSKI_MIN
    4. Filter: market cap >= mktcap_min
    5. Sort by proximity to 52-week low (most depressed first)

    Returns list of (symbol, market_cap, pct_above_low) sorted by pct_above_low ASC.
    """
    # 45-day filing lag for annual data
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS), datetime.min.time()
    ).timestamp())
    prev_year_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS + 400), datetime.min.time()
    ).timestamp())

    # Entry date range: first available price at/near target_date
    entry_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    entry_end_epoch = int(datetime.combine(
        target_date + timedelta(days=10), datetime.min.time()
    ).timestamp())

    # 52-week lookback: prices from LOOKBACK_DAYS before target to day before entry
    lookback_start_epoch = int(datetime.combine(
        target_date - timedelta(days=LOOKBACK_DAYS), datetime.min.time()
    ).timestamp())
    lookback_end_epoch = int(datetime.combine(
        target_date - timedelta(days=1), datetime.min.time()
    ).timestamp())

    # 1. Compute Piotroski scores for all symbols with data
    piotroski = compute_piotroski_scores(con, cutoff_epoch, prev_year_epoch)

    # 2. Get high-quality symbols (F-score >= threshold, market cap filter)
    quality_syms = {
        sym: (score, mcap)
        for sym, (score, mcap) in piotroski.items()
        if score >= PIOTROSKI_MIN and mcap is not None and mcap >= mktcap_min
    }

    if not quality_syms:
        return []

    sym_list = ", ".join(f"'{s}'" for s in quality_syms.keys())

    # 3. Compute current price and 52-week low
    rows = con.execute(f"""
        WITH current_prices AS (
            SELECT symbol,
                   adjClose AS current_price,
                   trade_epoch
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_epoch >= {entry_epoch}
              AND trade_epoch <= {entry_end_epoch}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC) = 1
        ),
        low_52w AS (
            SELECT symbol,
                   MIN(adjClose) AS low_52w
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_epoch >= {lookback_start_epoch}
              AND trade_epoch <= {lookback_end_epoch}
              AND adjClose > 0
            GROUP BY symbol
        )
        SELECT cp.symbol,
               cp.current_price,
               l.low_52w,
               (cp.current_price - l.low_52w) / l.low_52w AS pct_above_low
        FROM current_prices cp
        JOIN low_52w l ON cp.symbol = l.symbol
        WHERE l.low_52w > 0
          AND (cp.current_price - l.low_52w) / l.low_52w <= {PROXIMITY_THRESHOLD}
          AND cp.current_price >= {MIN_ENTRY_PRICE}
        ORDER BY pct_above_low ASC
        LIMIT {MAX_STOCKS}
    """).fetchall()

    result = []
    for sym, cur_price, low_52w, pct_above in rows:
        if sym in quality_syms:
            mcap = quality_syms[sym][1]
            result.append((sym, mcap, pct_above))

    return result


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run 52-Week Low Quality backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            bench_return = get_benchmark_return(
                con, benchmark_symbol, entry_date, exit_date,
                offset_days=offset_days)

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

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _ in portfolio}

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
            con, benchmark_symbol, entry_date, exit_date,
            offset_days=offset_days)

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
               risk_free_rate, verbose, output_path=None, offset_days=1):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "Same-day close" if offset_days == 0 else "Next-day close (MOC)"

    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    signal_desc = (f"Price within {PROXIMITY_THRESHOLD*100:.0f}% of 52w low, "
                   f"Piotroski >= {PIOTROSKI_MIN}, MCap > {mktcap_label} local, top {MAX_STOCKS}")
    print_header("52-WEEK LOW QUALITY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2002, 2025, frequency,
                                                months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2002-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol)
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
    print(format_metrics(metrics, "52-Week Low Quality", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        bench_label = benchmark_name[:10]
        print(f"\n  {'Year':<8} {'52wLow':>12} {bench_label:>10} {'Excess':>10}")
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
    parser = argparse.ArgumentParser(description="52-Week Low Quality backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("52-week-low", args_str=" ".join(cloud_args),
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
            # Confirmed clean exchanges (see DATA_QUALITY_ISSUES.md)
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("india", ["NSE"]),
            ("germany", ["XETRA"]),
            ("sweden", ["STO"]),
            ("canada", ["TSX"]),
            ("korea", ["KSC"]),
            ("taiwan", ["TAI", "TWO"]),
            ("singapore", ["SES"]),   # SES = Singapore, not SGX (SGX has 0 profile rows)
            ("hongkong", ["HKSE"]),
            ("switzerland", ["SIX"]),
            ("france", ["PAR"]),
            ("norway", ["OSL"]),
            ("china", ["SHZ", "SHH"]),
            ("southafrica", ["JNB"]),
            ("thailand", ["SET"]),
            # Excluded: ASX (adjClose split issues), SAO/Brazil (adjClose split issues),
            #           JPX/Japan (no FY data), LSE/UK (no FY data)
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
                                    offset_days=offset_days)
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
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, args.verbose, args.output,
               offset_days=offset_days)


if __name__ == "__main__":
    main()
