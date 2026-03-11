#!/usr/bin/env python3
"""
Value-Momentum Multi-Exchange Backtest

Combines two return factors with negative correlation: value (cheap P/E) and
price momentum (12-1 month trend). Stocks must pass a value quality screen,
then are ranked by a composite value+momentum score.

Signal:
  Value filter:   P/E > 0 AND P/E < 20, ROE > 10%, D/E < 1.0, MCap > threshold
  Momentum:       12-1 month price return (trailing 12M, excluding last month)
  Composite rank: Average of percentile rank on value (P/E ASC) and momentum (return DESC)
  Selection:      Top 30 by composite score, equal weight

Academic basis:
  Asness, Moskowitz, Pedersen (2013). "Value and Momentum Everywhere."
  Journal of Finance, 68(3), 929-985.

Rebalancing: Semi-annual (Jan/Jul), 2000-2025.

Usage:
    python3 value-momentum/backtest.py                          # US default
    python3 value-momentum/backtest.py --preset india
    python3 value-momentum/backtest.py --preset japan
    python3 value-momentum/backtest.py --global --output results/exchange_comparison.json
    python3 value-momentum/backtest.py --preset us --no-costs --verbose
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
PE_MIN = 0              # Positive earnings (no money-losers)
PE_MAX = 20             # Cheap by P/E (value screen)
ROE_MIN = 0.10          # Return on equity > 10% (basic profitability quality bar)
DE_MAX = 1.0            # Debt-to-equity < 1.0 (avoid highly levered value traps)
MOMENTUM_DAYS = 365     # 12-month lookback for momentum
MOMENTUM_WINDOW = 30    # Days to search for a price near the lookback start
MOMENTUM_LOOKBACK_FETCH = 395  # Days before rebalance date to fetch prices (momentum buffer)
MAX_STOCKS = 30         # Top N by composite score, equal weight
MIN_STOCKS = 10         # Hold cash if fewer qualify
DEFAULT_FREQUENCY = "semi-annual"
DEFAULT_REBALANCE_MONTHS = [1, 7]   # Jan / Jul
MAX_SINGLE_RETURN = 2.0  # Cap at 200% per stock (data quality guard)
MIN_ENTRY_PRICE = 1.0    # Skip sub-$1 entry prices (price data artifacts)


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all data needed for value-momentum backtest.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, priceToEarningsRatio, debtToEquityRatio, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)
            -- extended window: entry date AND 12M momentum lookback per rebalance date

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

    # 2-3. Financial data (FY only, for point-in-time signal construction)
    queries = [
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY' AND returnOnEquity IS NOT NULL AND {sym_filter_sql}
        """, "key metrics (ROE, MCap)"),
        ("ratios_cache", f"""
            SELECT symbol, priceToEarningsRatio, debtToEquityRatio, dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND priceToEarningsRatio IS NOT NULL
              AND debtToEquityRatio IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (P/E, D/E)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              memory_mb=4096, threads=2)
        print(f"    -> {count} rows")

    # 4. Prices — extended window: momentum lookback start (R-395 days) through entry/exit (R+10 days)
    # For each rebalance date D, fetches [D-395, D+10].
    # This single window per rebalance covers:
    #   - 12M momentum start price: found in [D-395, D-335] via get_prices(..., window_days=30)
    #   - Entry price: found in [D, D+10] via get_prices(...)
    # Exit prices for period i are covered by the next rebalance date's entry window.
    # Symbol filter: stocks that have P/E and D/E data (value candidates only).
    print("  Fetching prices (extended momentum + entry window)...")
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

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR symbol IN (
                SELECT DISTINCT r.symbol FROM financial_ratios r
                WHERE r.period = 'FY'
                  AND r.priceToEarningsRatio > 0
                  AND r.priceToEarningsRatio < {PE_MAX}
                  {exchange_fy_filter}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=10000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows ({MOMENTUM_LOOKBACK_FETCH}-day momentum window per date)")

    return con


def screen_value(con, target_date, mktcap_min):
    """Apply value filter at target_date. Returns dict: {symbol: (pe, market_cap)}.

    Uses 45-day filing lag for point-in-time integrity.

    Value criteria (FY annual data):
      - P/E > 0 AND P/E < 20   (profitable and cheap)
      - ROE > 10%              (basic profitability threshold)
      - D/E >= 0 AND D/E < 1.0 (not over-levered)
      - Market cap > exchange threshold
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        met AS (
            SELECT symbol, returnOnEquity, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        rat AS (
            SELECT symbol, priceToEarningsRatio, debtToEquityRatio, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ?
        )
        SELECT met.symbol, rat.priceToEarningsRatio, met.marketCap
        FROM met
        JOIN rat ON met.symbol = rat.symbol AND rat.rn = 1
        WHERE met.rn = 1
          AND rat.priceToEarningsRatio > ?
          AND rat.priceToEarningsRatio < ?
          AND met.returnOnEquity > ?
          AND rat.debtToEquityRatio >= 0
          AND rat.debtToEquityRatio < ?
          AND met.marketCap > ?
    """, [
        cutoff_epoch,   # met
        cutoff_epoch,   # rat
        PE_MIN, PE_MAX, ROE_MIN, DE_MAX, mktcap_min,
    ]).fetchall()

    # {symbol: (pe, market_cap)}
    return {r[0]: (r[1], r[2]) for r in rows}


def compute_momentum(con, symbols, target_date):
    """Compute 12-month price return for value-passing symbols.

    Returns dict: {symbol: 12m_return}. Symbols without sufficient price
    history (no price found ~12M ago) are excluded.
    """
    if not symbols:
        return {}

    # 12M lookback: search for price in [target_date - 365d, target_date - 335d]
    lookback_date = target_date - timedelta(days=MOMENTUM_DAYS)
    current_prices = get_prices(con, symbols, target_date, window_days=10)
    lookback_prices = get_prices(con, symbols, lookback_date, window_days=MOMENTUM_WINDOW)

    momentum = {}
    for sym in symbols:
        cur = current_prices.get(sym)
        past = lookback_prices.get(sym)
        if cur and past and past > 0 and cur > 0:
            momentum[sym] = (cur - past) / past

    return momentum


def rank_composite(value_stocks, momentum, n_select=MAX_STOCKS):
    """Rank stocks by composite value+momentum score. Returns top n_select.

    Composite score: average of P/E percentile rank (ASC = cheaper = better)
    and momentum percentile rank (DESC = stronger = better).

    Both ranks are normalized to [0, 1] where 1.0 = best rank.
    Composite = (value_rank_pct + momentum_rank_pct) / 2

    Returns list of (symbol, pe, market_cap, momentum_12m, composite_score), sorted desc.
    """
    # Build candidate list: stocks with both value signal and momentum
    candidates = []
    for sym, (pe, mcap) in value_stocks.items():
        if sym in momentum:
            candidates.append((sym, pe, mcap, momentum[sym]))

    n = len(candidates)
    if n == 0:
        return []

    # Value rank: lower P/E → higher rank (rank 1 = lowest P/E = best value)
    sorted_by_pe = sorted(candidates, key=lambda x: x[1])
    value_rank = {sym: (i + 1) for i, (sym, pe, mcap, mom) in enumerate(sorted_by_pe)}

    # Momentum rank: higher momentum → higher rank (rank 1 = highest momentum = best)
    sorted_by_mom = sorted(candidates, key=lambda x: x[3], reverse=True)
    mom_rank = {sym: (i + 1) for i, (sym, pe, mcap, mom) in enumerate(sorted_by_mom)}

    # Composite percentile (1.0 = best rank, 0.0 = worst)
    scored = []
    for sym, pe, mcap, mom in candidates:
        value_pct = (n - value_rank[sym]) / (n - 1) if n > 1 else 1.0
        mom_pct = (n - mom_rank[sym]) / (n - 1) if n > 1 else 1.0
        composite = (value_pct + mom_pct) / 2.0
        scored.append((sym, pe, mcap, mom, composite))

    # Sort by composite descending, take top n_select
    scored.sort(key=lambda x: x[4], reverse=True)
    return scored[:n_select]


def screen_stocks(con, target_date, mktcap_min, verbose=False):
    """Screen for value-momentum stocks at target_date.

    1. Apply value filter (P/E, ROE, D/E, MCap)
    2. Compute 12-month momentum for value-passing stocks
    3. Rank by composite value+momentum score, select top MAX_STOCKS

    Returns list of (symbol, pe, market_cap, momentum_12m, composite_score).
    """
    value_stocks = screen_value(con, target_date, mktcap_min)

    if not value_stocks:
        return []

    value_symbols = list(value_stocks.keys())
    momentum = compute_momentum(con, value_symbols, target_date)

    result = rank_composite(value_stocks, momentum, n_select=MAX_STOCKS)

    if verbose and result:
        top_comp = result[0][4]
        avg_pe = sum(r[1] for r in result) / len(result)
        avg_mom = sum(r[3] for r in result) / len(result) * 100
        print(f"    Value: {len(value_stocks)}, "
              f"with momentum: {len(momentum)}, "
              f"Selected: {len(result)} "
              f"(avg P/E: {avg_pe:.1f}, avg 12M: {avg_mom:.0f}%, "
              f"top composite: {top_comp:.2f})")

    return result


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run value-momentum backtest. Returns list of period result dicts."""
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

        symbols = [s for s, _, _, _, _ in portfolio]
        mcaps = {s: mc for s, pe, mc, mom, comp in portfolio}

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

        avg_pe = sum(r[1] for r in portfolio) / len(portfolio)
        avg_mom = sum(r[3] for r in portfolio) / len(portfolio) * 100

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "stocks_held": len(returns),
            "avg_pe": round(avg_pe, 1),
            "avg_momentum_12m": round(avg_mom, 1),
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg P/E={avg_pe:.1f}, "
                  f"12M={avg_mom:.0f}%), "
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
        f"P/E {PE_MIN}-{PE_MAX}, ROE>{ROE_MIN*100:.0f}%, D/E<{DE_MAX}, "
        f"MCap>{mktcap_threshold/1e9:.0f}B local → top {MAX_STOCKS} by composite value+momentum"
    )
    print_header("VALUE-MOMENTUM BACKTEST", universe_name, exchanges, signal_desc)
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
    print(format_metrics(metrics, "Value-Momentum", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'ValMom':>10} {'SPY':>10} {'Excess':>10}")
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
    parser = argparse.ArgumentParser(description="Value-Momentum multi-exchange backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("value-momentum", args_str=" ".join(cloud_args),
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

        # Exchange list: broad coverage. ASX/SAO excluded (adjClose artifacts).
        # SES used for Singapore (not SGX — cli_utils preset uses SGX which returns 0).
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),          # NSE only (BSE+NSE = duplicates)
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
