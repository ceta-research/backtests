#!/usr/bin/env python3
"""
Oversold Quality Backtest

Quarterly rebalancing, equal weight, top 30 by lowest RSI.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: RSI-14 < 30 (technically oversold) AND Piotroski F-Score >= 7 (fundamentally strong)
Portfolio: Up to 30 stocks with lowest RSI (most oversold), equal weight. Cash if < 5 qualify.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), 2000-2025.

Strategy thesis: Quality companies that are technically oversold tend to mean revert.
The Piotroski filter ensures we're not buying fundamentally weak companies in freefall.
RSI < 30 identifies temporary selling pressure, not structural deterioration.

No P/B filter: Unlike the Piotroski standalone strategy, we target all quality stocks
(Piotroski >= 7), not just cheap value stocks. The RSI filter is our entry timing.

Academic reference: Piotroski, J.D. (2000). "Value Investing: The Use of Historical
Financial Statement Information to Separate Winners from Losers". Journal of Accounting
Research. RSI: Wilder, J.W. (1978). "New Concepts in Technical Trading Systems."

Usage:
    # Backtest US stocks (default)
    python3 oversold-quality/backtest.py

    # Backtest Indian stocks
    python3 oversold-quality/backtest.py --preset india

    # Backtest all exchanges
    python3 oversold-quality/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 oversold-quality/backtest.py --no-costs

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
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
PIOTROSKI_MIN = 7          # F-Score threshold (0-9 scale, 7+ = high quality)
RSI_MAX = 30               # RSI below this = oversold
RSI_LOOKBACK = 14          # Standard RSI-14 period
RSI_MIN_PERIODS = 10       # Minimum data points for RSI computation
RSI_WINDOW_DAYS = 22       # Calendar days of price history to fetch for RSI (14 trading days ≈ 20-22 cal days)
MAX_STOCKS = 30            # Top 30 by lowest RSI (most oversold)
MIN_STOCKS = 5             # Hold cash if fewer qualify (RSI filter is restrictive)
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]  # Jan/Apr/Jul/Oct
MAX_SINGLE_RETURN = 2.0    # Cap individual stock returns at 200% (data quality guard)
MIN_ENTRY_PRICE = 1.0      # Skip stocks with entry price < $1 (price data artifact)
MIN_PROFITABLE_YEARS = 2   # Symbol must have netIncome > 0 in at least N years (price pre-filter)


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, benchmark_symbol="SPY"):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, grossProfit, revenue, filing_epoch, period)
        balance_cache(symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                      longTermDebt, totalStockholdersEquity, filing_epoch, period)
        cashflow_cache(symbol, operatingCashFlow, filing_epoch, period)
        metrics_cache(symbol, marketCap, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose) -- extended window for RSI

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

    # 2-4: Financial data for Piotroski (income, balance, cashflow, key_metrics)
    queries = [
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

    # 5. Prices — extended window: RSI lookback (22 cal days before) + entry price (10 days after)
    # 22 calendar days ≈ 14-16 trading days (enough for RSI-14 computation).
    # Symbol filter: only symbols with at least one profitable FY (netIncome > 0 AND OCF > 0)
    # This uses warehouse subqueries (not inline symbol lists) to keep the SQL compact.
    print("  Fetching prices (extended RSI window)...")
    date_conditions = []
    for d in rebalance_dates:
        rsi_start = d - timedelta(days=RSI_WINDOW_DAYS)
        entry_end = d + timedelta(days=10)
        date_conditions.append(
            f"(date >= '{rsi_start.isoformat()}' AND date <= '{entry_end.isoformat()}')"
        )
    date_filter = " OR ".join(date_conditions)

    # Use warehouse subquery to filter to ever-profitable symbols (reduces price volume ~50%)
    # Avoids embedding a long symbol IN (...) list which hits query length limits.
    if sym_filter_sql != "1=1":
        exchange_fy_filter = f"AND {sym_filter_sql}"
    else:
        exchange_fy_filter = ""

    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    if benchmark_symbol:
        bench_symbols.add(f"'{benchmark_symbol}'")
    bench_list = ", ".join(bench_symbols)

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
    print(f"    -> {count} price rows ({RSI_WINDOW_DAYS}-day RSI lookback per rebalance date)")

    return con


def compute_piotroski(con, target_date, mktcap_min):
    """Compute full 9-factor Piotroski F-Score for all stocks at target_date.

    No P/B filter — we want all quality stocks, not just value stocks.
    RSI < 30 is our entry timing, so we don't restrict by valuation.

    Returns dict: {symbol: (f_score, market_cap)}
    Uses 45-day filing lag for point-in-time integrity.
    Uses previous-year comparison window of 445 days (vs 45 for point-in-time).
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())
    prev_year_epoch = int(datetime.combine(
        target_date - timedelta(days=445), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        inc_curr AS (
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
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        scored AS (
            SELECT ic.symbol,
                -- F1: Positive net income
                CASE WHEN ic.netIncome > 0 THEN 1 ELSE 0 END AS f1_ni,
                -- F2: Positive operating cash flow
                CASE WHEN cf.operatingCashFlow > 0 THEN 1 ELSE 0 END AS f2_ocf,
                -- F3: Increasing ROA year-over-year
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.netIncome / bc.totalAssets) > (ip.netIncome / bp.totalAssets)
                     THEN 1 ELSE 0 END AS f3_roa,
                -- F4: Cash flow quality (OCF > net income = low accruals)
                CASE WHEN cf.operatingCashFlow > ic.netIncome THEN 1 ELSE 0 END AS f4_accrual,
                -- F5: Decreasing leverage
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (COALESCE(bc.longTermDebt,0) / bc.totalAssets) < (COALESCE(bp.longTermDebt,0) / bp.totalAssets)
                     THEN 1 ELSE 0 END AS f5_leverage,
                -- F6: Improving liquidity (current ratio)
                CASE WHEN bc.totalCurrentAssets > 0 AND bc.totalCurrentLiabilities > 0
                     AND bp.totalCurrentAssets > 0 AND bp.totalCurrentLiabilities > 0
                     AND (bc.totalCurrentAssets / bc.totalCurrentLiabilities) > (bp.totalCurrentAssets / bp.totalCurrentLiabilities)
                     THEN 1 ELSE 0 END AS f6_liquidity,
                -- F7: No dilution (shares not increasing)
                CASE WHEN bc.totalStockholdersEquity >= bp.totalStockholdersEquity THEN 1 ELSE 0 END AS f7_no_dilution,
                -- F8: Improving asset turnover
                CASE WHEN ic.revenue > 0 AND ip.revenue > 0 AND bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.revenue / bc.totalAssets) > (ip.revenue / bp.totalAssets)
                     THEN 1 ELSE 0 END AS f8_turnover,
                -- F9: Improving gross margin
                CASE WHEN ic.grossProfit > 0 AND ip.grossProfit > 0 AND ic.revenue > 0 AND ip.revenue > 0
                     AND (ic.grossProfit / ic.revenue) > (ip.grossProfit / ip.revenue)
                     THEN 1 ELSE 0 END AS f9_margin,
                met.marketCap
            FROM inc_curr ic
            JOIN inc_prev ip ON ic.symbol = ip.symbol AND ip.rn = 1
            JOIN bal_curr bc ON ic.symbol = bc.symbol AND bc.rn = 1
            JOIN bal_prev bp ON ic.symbol = bp.symbol AND bp.rn = 1
            JOIN cf_curr cf ON ic.symbol = cf.symbol AND cf.rn = 1
            JOIN met ON ic.symbol = met.symbol AND met.rn = 1
            WHERE ic.rn = 1
              AND met.marketCap > ?
        )
        SELECT symbol,
            (f1_ni + f2_ocf + f3_roa + f4_accrual + f5_leverage + f6_liquidity
             + f7_no_dilution + f8_turnover + f9_margin) AS f_score,
            marketCap
        FROM scored
        WHERE (f1_ni + f2_ocf + f3_roa + f4_accrual + f5_leverage + f6_liquidity
               + f7_no_dilution + f8_turnover + f9_margin) >= ?
    """, [
        cutoff_epoch, prev_year_epoch,   # inc_curr
        prev_year_epoch,                  # inc_prev
        cutoff_epoch, prev_year_epoch,   # bal_curr
        prev_year_epoch,                  # bal_prev
        cutoff_epoch, prev_year_epoch,   # cf_curr
        cutoff_epoch,                     # met
        mktcap_min,
        PIOTROSKI_MIN,
    ]).fetchall()

    return {r[0]: (r[1], r[2]) for r in rows}


def compute_rsi(con, target_date, quality_symbols):
    """Compute RSI-14 for quality-passing symbols at target_date.

    Uses simple RSI (average of last 14 daily returns, not Wilder's smoothed EMA).
    Returns dict: {symbol: rsi_value} — only includes symbols with sufficient price history.
    """
    if not quality_symbols:
        return {}

    cutoff_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    start_epoch = int(datetime.combine(
        target_date - timedelta(days=RSI_WINDOW_DAYS), datetime.min.time()
    ).timestamp())

    sym_list = ", ".join(f"'{s}'" for s in quality_symbols)

    rows = con.execute(f"""
        WITH recent_prices AS (
            SELECT symbol, trade_epoch, adjClose,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch DESC) as rn
            FROM prices_cache
            WHERE trade_epoch <= ?
              AND trade_epoch >= ?
              AND adjClose > 0
              AND symbol IN ({sym_list})
        ),
        with_changes AS (
            SELECT symbol,
                   adjClose - LEAD(adjClose) OVER (PARTITION BY symbol ORDER BY rn) as change
            FROM recent_prices
            WHERE rn <= 15
        ),
        gain_loss AS (
            SELECT symbol,
                   CASE WHEN change > 0 THEN change ELSE 0 END as gain,
                   CASE WHEN change < 0 THEN -change ELSE 0 END as loss
            FROM with_changes
            WHERE change IS NOT NULL
        ),
        avg_gl AS (
            SELECT symbol,
                   AVG(gain) as avg_gain,
                   AVG(loss) as avg_loss,
                   COUNT(*) as n_periods
            FROM gain_loss
            GROUP BY symbol
            HAVING COUNT(*) >= ?
        )
        SELECT symbol,
               CASE
                   WHEN avg_loss = 0 THEN 100.0
                   WHEN avg_gain = 0 THEN 0.0
                   ELSE 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
               END as rsi
        FROM avg_gl
        ORDER BY symbol
    """, [cutoff_epoch, start_epoch, RSI_MIN_PERIODS]).fetchall()

    return {r[0]: r[1] for r in rows}


def screen_stocks(con, target_date, mktcap_min, verbose=False):
    """Screen for oversold quality stocks at target_date.

    1. Compute Piotroski F-Score >= 7 (no P/B filter)
    2. Compute RSI-14 for quality-passing stocks
    3. Select RSI < 30, ranked by RSI ascending (most oversold first)

    Returns list of (symbol, market_cap, rsi) tuples.
    """
    # Step 1: Piotroski scoring
    quality_stocks = compute_piotroski(con, target_date, mktcap_min)

    if not quality_stocks:
        return []

    # Step 2: RSI computation for quality-passing stocks
    quality_symbols = list(quality_stocks.keys())
    rsi_data = compute_rsi(con, target_date, quality_symbols)

    # Step 3: Filter RSI < RSI_MAX and combine
    candidates = []
    for symbol, (f_score, market_cap) in quality_stocks.items():
        rsi = rsi_data.get(symbol)
        if rsi is not None and rsi < RSI_MAX:
            candidates.append((symbol, market_cap, rsi))

    # Sort by RSI ascending (most oversold first), take top MAX_STOCKS
    candidates.sort(key=lambda x: x[2])
    result = candidates[:MAX_STOCKS]

    if verbose and result:
        print(f"    Quality (Pio>={PIOTROSKI_MIN}): {len(quality_stocks)}, "
              f"RSI<{RSI_MAX}: {len(candidates)}, Selected: {len(result)}")

    return result


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run oversold quality backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min, verbose=verbose)

        bench_return = get_benchmark_return(con, benchmark_symbol, entry_date, exit_date,
                                            offset_days=offset_days)

        if len(portfolio) < MIN_STOCKS:
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

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _ in portfolio}
        rsis = {s: rsi for s, _, rsi in portfolio}

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

        avg_rsi = sum(rsis[s] for s in symbols if s in rsis) / len(symbols)

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(bench_return, 6) if bench_return is not None else None,
            "stocks_held": len(returns),
            "avg_rsi": round(avg_rsi, 1),
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg RSI={avg_rsi:.0f}), "
                  f"port={port_return * 100:.1f}%, bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

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
               risk_free_rate, mktcap_threshold, verbose, output_path=None,
               offset_days=1, benchmark_symbol="SPY", benchmark_name="S&P 500"):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    exec_model = "same-day (signal)" if offset_days == 0 else "next-day close (MOC)"
    signal_desc = (f"Piotroski>={PIOTROSKI_MIN} (9-factor, no P/B filter) + "
                   f"RSI-{RSI_LOOKBACK}<{RSI_MAX}, "
                   f"MCap>{mktcap_threshold/1e9:.0f}B local, top {MAX_STOCKS}")
    print_header("OVERSOLD QUALITY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    # Phase 1: Fetch data
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

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs, verbose=verbose,
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
    print(format_metrics(metrics, "Oversold Quality", benchmark_name))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        bench_label = benchmark_name[:10] if len(benchmark_name) > 10 else benchmark_name
        print(f"\n  {'Year':<8} {'OversoldQual':>14} {bench_label:>10} {'Excess':>10}")
        print("  " + "-" * 44)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>13.1f}% {ar['benchmark']*100:>9.1f}% "
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
    parser = argparse.ArgumentParser(description="Oversold Quality backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("oversold-quality", args_str=" ".join(cloud_args),
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
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("taiwan", ["TAI"]),
            ("thailand", ["SET"]),
            ("germany", ["XETRA"]),
            ("korea", ["KSC"]),
            ("canada", ["TSX"]),
            ("sweden", ["STO"]),
            ("switzerland", ["SIX"]),
            ("indonesia", ["JKT"]),
            ("southafrica", ["JNB"]),
            ("norway", ["OSL"]),
            ("italy", ["MIL"]),
            ("malaysia", ["KLS"]),
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

            bench_sym, bench_name = get_local_benchmark(preset_exchanges)
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

        # Save comparison
        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Print summary
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
