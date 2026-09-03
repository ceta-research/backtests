#!/usr/bin/env python3
"""
Capital Expenditure Efficiency Strategy Backtest

Annual rebalancing (July), equal weight, top 30 by highest ROIC with low capex intensity.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Capex-to-Revenue < 8%, Capex-to-OCF < 40%, ROIC > 15%, Operating Margin > 15%,
        Market Cap > local-currency threshold
Portfolio: Top 30 by highest ROIC, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), 2000-2025.

Academic reference:
  Cooper, M., Gulen, H. & Schill, M. (2008) "Asset Growth and the Cross-Section of Stock Returns"
    Journal of Finance, 63(4), 1609-1651.
  Titman, S., Wei, K. & Xie, F. (2004) "Capital Investments and Stock Returns"
    Journal of Financial and Quantitative Analysis, 39(4), 677-700.
  Novy-Marx, R. (2013) "The Other Side of Value: The Gross Profitability Premium"
    Journal of Financial Economics, 108(1), 1-28.

Usage:
    python3 capex-efficiency/backtest.py
    python3 capex-efficiency/backtest.py --preset india
    python3 capex-efficiency/backtest.py --global --output results/exchange_comparison.json --verbose
    python3 capex-efficiency/backtest.py --no-costs

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
                        entry_buyable_prices,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, save_results,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
CAPEX_TO_REV_MAX = 0.08        # Capex-to-Revenue < 8% (asset-light)
CAPEX_TO_OCF_MAX = 0.40        # Capex-to-OCF < 40% (retains 60%+ of OCF as FCF)
ROIC_MIN = 0.15                # ROIC > 15% (earns well above cost of capital)
OPM_MIN = 0.15                 # Operating profit margin > 15% (pricing power)
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (annual FY filings available, 45-day lag)

# Presets to skip in global mode (data quality or covered by broader preset)
EXCLUDED_PRESETS = {
    "brazil",       # SAO: adjClose split artifacts
    "australia",    # ASX: adjClose split artifacts
    "nyse",         # Covered by "us" preset
    "nasdaq",       # Covered by "us" preset
}


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        metrics_cache(symbol, capexToRevenue, capexToOperatingCashFlow, capexToDepreciation,
                      returnOnInvestedCapital, marketCap, filing_epoch, period)
        ratios_cache(symbol, operatingProfitMargin, filing_epoch, period)
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

    # 2. Key metrics (capex ratios, ROIC, market cap) - FY only
    queries = [
        ("metrics_cache", f"""
            SELECT symbol,
                   capexToRevenue,
                   capexToOperatingCashFlow,
                   capexToDepreciation,
                   returnOnInvestedCapital,
                   marketCap,
                   dateEpoch as filing_epoch,
                   period
            FROM key_metrics
            WHERE period = 'FY'
              AND capexToRevenue IS NOT NULL
              AND returnOnInvestedCapital IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (capex ratios, ROIC, market cap)"),
        # 3. Financial ratios (operating margin)
        ("ratios_cache", f"""
            SELECT symbol,
                   operatingProfitMargin,
                   dateEpoch as filing_epoch,
                   period
            FROM financial_ratios
            WHERE period = 'FY'
              AND operatingProfitMargin IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (operating margin)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 4. Prices (only at rebalance dates + 10-day window)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    # Build benchmark symbol list (SPY + local index)
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
                          verbose=verbose, limit=5000000, timeout=600)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for capital-efficient stocks with high ROIC.
    Returns list of (symbol, market_cap, roic) tuples."""
    # 45-day lag: use filings available at least 45 days before rebalance date
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH m AS (
            SELECT symbol, capexToRevenue, capexToOperatingCashFlow, capexToDepreciation,
                   returnOnInvestedCapital, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        r AS (
            SELECT symbol, operatingProfitMargin, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        )
        SELECT m.symbol, m.marketCap, m.returnOnInvestedCapital
        FROM m
        JOIN r ON m.symbol = r.symbol AND r.rn = 1
        WHERE m.rn = 1
          AND m.capexToRevenue > 0
          AND m.capexToRevenue < ?
          AND m.capexToOperatingCashFlow > 0
          AND m.capexToOperatingCashFlow < ?
          AND m.returnOnInvestedCapital > ?
          AND r.operatingProfitMargin > ?
          AND m.marketCap > ?
        ORDER BY m.returnOnInvestedCapital DESC
        LIMIT ?
    """, [cutoff_epoch, cutoff_epoch,
          CAPEX_TO_REV_MAX, CAPEX_TO_OCF_MAX, ROIC_MIN, OPM_MIN, mktcap_min, MAX_STOCKS]).fetchall()

    return [(r[0], r[1], r[2]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run capex efficiency backtest. Returns list of period result dicts."""
    results = []

    for i, rdate in enumerate(rebalance_dates[:-1]):
        next_rdate = rebalance_dates[i + 1]
        print(f"\n  Period {i+1}/{len(rebalance_dates)-1}: {rdate} → {next_rdate}")

        # Benchmark first, so a cash period records the real benchmark instead of
        # a fabricated 0.0. Cash periods now enter the metric series, so a fake
        # benchmark observation there would corrupt alpha/beta and excess return.
        bench_return = get_benchmark_return(
            con, benchmark_symbol, rdate, next_rdate, offset_days=offset_days)

        # Screen
        qualifying = screen_stocks(con, rdate, mktcap_min)
        n_stocks = len(qualifying)
        print(f"    Qualifying stocks: {n_stocks}")

        if n_stocks < MIN_STOCKS:
            print(f"    → Cash (< {MIN_STOCKS} stocks)")
            results.append({
                "start_date": rdate.isoformat(),
                "end_date": next_rdate.isoformat(),
                "n_stocks": 0,
                "stocks_held": 0,
                "screened": n_stocks,
                "entry_buyable": None,
                "min_stocks": MIN_STOCKS,
                "return": 0.0,
                "spy_return": bench_return if bench_return is not None else 0.0,
                "avg_roic": None,
                "msg": f"cash (< {MIN_STOCKS} stocks)"
            })
            continue

        # Get symbols and metadata
        symbols = [sym for sym, _, _ in qualifying]
        mcaps = {sym: mcap for sym, mcap, _ in qualifying}
        roics = {sym: roic for sym, _, roic in qualifying}

        # Get prices (MOC execution: offset_days=1 means next-day close)
        px_start = get_prices(con, symbols, rdate, offset_days=offset_days)
        px_end = get_prices(con, symbols, next_rdate, offset_days=offset_days)

        if not px_start or not px_end:
            # Same defect class as the floor re-check below: a bare continue
            # deleted the period from the series rather than recording it, so a
            # rebalance with no prices at all left no trace in the output.
            print("    → Cash (no prices for either leg of the period)")
            results.append({
                "start_date": rdate.isoformat(),
                "end_date": next_rdate.isoformat(),
                "n_stocks": 0,
                "stocks_held": 0,
                "screened": n_stocks,
                # Counted here rather than left null: the prices WERE fetched,
                # so the entry-side count is knowable. `not px_end` alone
                # reaches this branch with a fully buyable book, and writing
                # null (or 0) there would hide a period that cleared the floor
                # at entry behind a branch that reads like it never priced.
                "entry_buyable": entry_buyable_prices(symbols, px_start),
                "min_stocks": MIN_STOCKS,
                "return": 0.0,
                "spy_return": bench_return if bench_return is not None else 0.0,
                "avg_roic": None,
                "msg": "cash (no prices for the period)"
            })
            continue

        # Collect raw returns for filtering
        raw_data = []
        for sym in symbols:
            ep = px_start.get(sym)
            xp = px_end.get(sym)
            if ep and xp and ep > 0:
                raw_data.append((sym, ep, xp, mcaps.get(sym)))

        # Filter out artifacts (>200% single-period returns, penny stocks)
        clean, skipped = filter_returns(raw_data, verbose=verbose)

        # The cash rule has to be re-checked HERE, not just on the screen count.
        # Screening can pass 30 names while only a handful of them have a usable
        # ENTRY price, and averaging 1-4 survivors reports a single stock's year
        # as a diversified portfolio return. Recording cash also keeps the period
        # in the series: the old `continue` deleted it.
        #
        # Checked on `buyable`, NOT on len(clean). filter_returns also drops a
        # name for a missing EXIT price and for a realised return over its
        # max_single_return, neither of which is knowable on the rebalance date.
        # Deciding cash from those is look-ahead and it flatters: a period the
        # strategy really held and really lost gets rewritten as a flat 0.0%. If
        # the book cleared the floor at entry the strategy ENTERED, so the
        # exit-side survivors are averaged below exactly as they were before this
        # guard existed. That survivor-averaging has its own known weakness; it
        # is pre-existing, logged in DATA_QUALITY_ISSUES.md, and deliberately
        # not changed here.
        #
        # `raw_data` above is built with the EXIT price already filtered out, so
        # entry_buyable() over it would still be exit-conditioned. Count from the
        # screened symbols and the entry-price map instead. No min_entry_price,
        # matching this topic's own filter_returns call, which takes the $1.00
        # default.
        buyable = entry_buyable_prices(symbols, px_start)
        if buyable < MIN_STOCKS:
            print(f"    → Cash (only {buyable} of {n_stocks} screened names "
                  "buyable at entry)")
            results.append({
                "start_date": rdate.isoformat(),
                "end_date": next_rdate.isoformat(),
                "n_stocks": 0,
                "stocks_held": 0,
                "screened": n_stocks,
                "entry_buyable": buyable,
                "min_stocks": MIN_STOCKS,
                "return": 0.0,
                "spy_return": bench_return if bench_return is not None else 0.0,
                "avg_roic": None,
                "msg": f"cash ({buyable} buyable at entry of {n_stocks} screened)"
            })
            continue

        # Apply costs to individual returns, then calculate portfolio return
        net_returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            net_returns.append(net_ret)

        # Calculate equal-weight portfolio return
        #
        # Both fallbacks are load-bearing, not defensive noise. The cash rule
        # above is decided on `buyable`, so this branch is now reachable with an
        # EMPTY `clean`: every name buyable at entry, none surviving to the exit
        # side. The pre-guard higher up only catches "no prices for EITHER leg";
        # it does not fire when a name priced at exit and was then dropped for a
        # return over max_single_return, which empties `clean` just as well.
        # Under the old exit-conditioned guard that case cashed out before
        # reaching here. avg_roic uses None, matching what the cash record above
        # already writes for this field.
        port_return = sum(net_returns) / len(net_returns) if net_returns else 0.0

        avg_roic = (sum(roics.get(sym, 0) for sym, _, _ in clean) / len(clean)
                    if clean else None)

        results.append({
            "start_date": rdate.isoformat(),
            "end_date": next_rdate.isoformat(),
            "n_stocks": len(clean),
            "stocks_held": len(clean),
            "screened": n_stocks,
            "entry_buyable": buyable,
            "min_stocks": MIN_STOCKS,
            "return": port_return,
            "spy_return": bench_return if bench_return is not None else 0.0,
            "avg_roic": avg_roic,
            "msg": "invested"
        })
        bench_str = f"{bench_return*100:.2f}%" if bench_return is not None else "N/A"
        print(f"    → Return: {port_return*100:.2f}% ({len(clean)} stocks, bench: {bench_str})")

    return results


def main():
    parser = argparse.ArgumentParser(description="Capex Efficiency Strategy Backtest")
    add_common_args(parser)
    args = parser.parse_args()

    client = CetaResearch()
    use_costs = not args.no_costs

    # Execution model
    offset_days = 0 if args.no_next_day else 1
    exec_model = "Same-day close (legacy)" if offset_days == 0 else "Next-day close (MOC)"

    # Global mode: loop all eligible exchanges
    if args.global_bt:
        print("Running global mode (all eligible exchanges)...")
        print(f"  Execution: {exec_model}")
        all_results = {}

        for preset_name, preset_data in EXCHANGE_PRESETS.items():
            if preset_name in EXCLUDED_PRESETS:
                continue

            exchanges = preset_data["exchanges"]

            print(f"\n{'='*70}")
            print(f"  Backtest: {preset_name.upper()} ({', '.join(exchanges)})")

            benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
            print(f"  Benchmark: {benchmark_name} ({benchmark_symbol})")
            print(f"{'='*70}")

            try:
                mktcap_min = get_mktcap_threshold(exchanges)
                rebalance_dates = generate_rebalance_dates(2000, 2025, DEFAULT_FREQUENCY, DEFAULT_REBALANCE_MONTHS)

                con = fetch_data_via_api(client, exchanges, rebalance_dates, verbose=args.verbose)
                if not con:
                    print(f"  Skipping {preset_name}: no data")
                    continue

                period_results = run_backtest(con, rebalance_dates, mktcap_min, use_costs, args.verbose,
                                              offset_days=offset_days, benchmark_symbol=benchmark_symbol)
                if not period_results:
                    print(f"  Skipping {preset_name}: no valid periods")
                    continue

                # Cash periods compound at 0%, they are not deleted. Filtering to
                # msg == "invested" dropped them from the series, and with
                # periods_per_year=1 metrics.py derives years from len(series), so
                # a dropped bad period shortened the horizon and inflated CAGR.
                # Every other floor topic compounds cash at 0%; match them.
                returns = [r["return"] for r in period_results]
                bench_returns = [r["spy_return"] for r in period_results]

                if not returns:
                    print(f"  Skipping {preset_name}: no valid returns")
                    continue

                rfr = get_risk_free_rate(exchanges)
                metrics = compute_metrics(returns, bench_returns, periods_per_year=1, risk_free_rate=rfr)
                period_dates = [r["start_date"] for r in period_results]
                annual_returns = compute_annual_returns(returns, bench_returns, period_dates, periods_per_year=1)

                all_results[preset_name.upper()] = {
                    "portfolio": metrics,
                    "annual_returns": annual_returns,
                    "period_results": period_results
                }

                print("\n  Summary:")
                print(format_metrics(metrics))

            except Exception as e:
                print(f"  Error in {preset_name}: {e}")
                import traceback
                traceback.print_exc()
                continue

        # Save aggregated results
        if all_results and args.output:
            with open(args.output, 'w') as f:
                json.dump(all_results, f, indent=2)
            print(f"\n✓ Global results saved to {args.output}")

        return

    # Single exchange mode
    exchanges, exchange_label = resolve_exchanges(args)
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)

    print("=" * 70)
    print(f"  CAPEX EFFICIENCY BACKTEST - {exchange_label}")
    if exchanges:
        print(f"  Exchanges: {', '.join(exchanges)}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 70)

    mktcap_min = get_mktcap_threshold(exchanges)
    print(f"Market cap filter: {mktcap_min:,.0f} (local currency)")

    rebalance_dates = generate_rebalance_dates(2000, 2025, DEFAULT_FREQUENCY, DEFAULT_REBALANCE_MONTHS)
    print(f"Rebalance dates: {len(rebalance_dates)} ({rebalance_dates[0]} to {rebalance_dates[-1]})")

    con = fetch_data_via_api(client, exchanges, rebalance_dates, verbose=args.verbose)
    if not con:
        print("No data fetched. Exiting.")
        return

    print("\nRunning backtest...")
    period_results = run_backtest(con, rebalance_dates, mktcap_min, use_costs, args.verbose,
                                  offset_days=offset_days, benchmark_symbol=benchmark_symbol)

    if not period_results:
        print("No valid periods. Exiting.")
        return

    # Every period counts, cash included at 0%. See the note in the global path:
    # filtering cash out shortened the horizon and inflated CAGR.
    returns = [r["return"] for r in period_results]
    bench_returns = [r["spy_return"] for r in period_results]

    if not returns:
        print("No valid returns. Exiting.")
        return

    # Compute metrics
    rfr = get_risk_free_rate(exchanges)
    metrics = compute_metrics(returns, bench_returns, periods_per_year=1, risk_free_rate=rfr)
    period_dates = [r["start_date"] for r in period_results]
    annual_returns = compute_annual_returns(returns, bench_returns, period_dates, periods_per_year=1)

    # Print results
    print("\n" + "="*60)
    print("BACKTEST RESULTS")
    print("="*60)
    print(format_metrics(metrics))

    # Save if requested
    if args.output:
        output_dir = os.path.dirname(args.output) or "."
        save_results(metrics, period_results, output_dir, exchange_label, strategy_name="capex-efficiency")
        print(f"\n✓ Results saved to {output_dir}/")


if __name__ == "__main__":
    main()
