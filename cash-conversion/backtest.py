#!/usr/bin/env python3
"""
Cash Conversion Cycle (CCC) Backtest

Annual rebalancing (April), equal weight, four portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: Cash Conversion Cycle from FY key_metrics.
  CCC = DSO + DIO - DPO (days, pre-computed in key_metrics)

Portfolios:
  Low CCC (< 30 days): Capital-efficient companies
  Mid CCC (30-90 days): Average efficiency
  High CCC (> 90 days): Capital-intensive companies
  Low + Decreasing: Low CCC with YoY improvement (strongest signal)

Universe: Non-financial stocks above exchange-specific market cap threshold.
Benchmark: S&P 500 (SPY).

Usage:
    python3 cash-conversion/backtest.py
    python3 cash-conversion/backtest.py --preset india --verbose
    python3 cash-conversion/backtest.py --global --output results/exchange_comparison.json
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
from data_utils import (query_parquet, generate_rebalance_dates, filter_returns,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations)
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, get_risk_free_rate,
                       EXCHANGE_PRESETS)

# --- Config ---
CCC_LOW = 30       # Below this = "low CCC" (capital-efficient)
CCC_HIGH = 90      # Above this = "high CCC" (capital-intensive)
FILING_LAG_DAYS = 45
EXCLUDED_SECTORS = ("Financial Services",)  # CCC meaningless for banks/insurance
START_YEAR = 2000
END_YEAR = 2025

GLOBAL_PRESETS = [
    "us", "india", "germany", "china", "hongkong", "canada",
    "uk", "switzerland", "sweden", "korea", "brazil",
    "taiwan", "singapore", "southafrica", "france",
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch CCC data and prices into DuckDB."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter_sql = f"""
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE exchange IN ({ex_filter})
                  AND COALESCE(sector, '') NOT IN ('Financial Services')
            )
        """
        profile_where = f"WHERE exchange IN ({ex_filter})"
    else:
        sym_filter_sql = """
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE COALESCE(sector, '') NOT IN ('Financial Services')
            )
        """
        profile_where = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange, sector FROM profile {profile_where}"
    profiles = client.query(profile_sql, verbose=verbose, timeout=120)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None

    filtered = [p for p in profiles if p.get("sector") not in EXCLUDED_SECTORS]
    excluded_count = len(profiles) - len(filtered)
    print(f"  Universe: {len(profiles)} symbols, {excluded_count} financials excluded, "
          f"{len(filtered)} remaining")

    if not filtered:
        return None

    sym_values = ",".join(f"('{r['symbol']}')" for r in filtered)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2. Key metrics (CCC + market cap)
    print("  Fetching key metrics (CCC)...")
    metrics_sql = f"""
        SELECT symbol,
            cashConversionCycle,
            daysOfSalesOutstanding,
            daysOfInventoryOutstanding,
            daysOfPayablesOutstanding,
            marketCap,
            dateEpoch as filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND cashConversionCycle IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache", verbose=verbose,
                          limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} rows")

    # 3. Prices (only at rebalance dates + 10-day windows)
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
                SELECT DISTINCT symbol FROM key_metrics
                WHERE period = 'FY'
                  AND cashConversionCycle IS NOT NULL
                  AND {sym_filter_sql}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count:,} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen stocks by CCC at a rebalance date.

    Returns dict: {symbol: {"ccc": float, "ccc_prior": float|None,
                             "ccc_change": float|None, "bucket": str,
                             "direction": str, "mcap": float}}
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH ranked AS (
            SELECT symbol,
                cashConversionCycle AS ccc,
                marketCap,
                filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        ),
        current_and_prior AS (
            SELECT
                r1.symbol,
                r1.ccc AS ccc,
                r2.ccc AS ccc_prior,
                r1.marketCap,
                r1.ccc - COALESCE(r2.ccc, r1.ccc) AS ccc_change
            FROM ranked r1
            LEFT JOIN ranked r2 ON r1.symbol = r2.symbol AND r2.rn = 2
            JOIN universe u ON r1.symbol = u.symbol
            WHERE r1.rn = 1
              AND r1.marketCap > ?
        )
        SELECT symbol, ccc, ccc_prior, ccc_change, marketCap
        FROM current_and_prior
        WHERE ccc IS NOT NULL
    """, [cutoff_epoch, mktcap_min]).fetchall()

    result = {}
    for sym, ccc, ccc_prior, ccc_change, mcap in rows:
        if ccc < CCC_LOW:
            bucket = "low"
        elif ccc <= CCC_HIGH:
            bucket = "mid"
        else:
            bucket = "high"

        if ccc_prior is not None and ccc_change is not None:
            direction = "decreasing" if ccc_change < 0 else "increasing"
        else:
            direction = "unknown"

        result[sym] = {
            "ccc": ccc,
            "ccc_prior": ccc_prior,
            "ccc_change": ccc_change,
            "bucket": bucket,
            "direction": direction,
            "mcap": mcap,
        }

    return result


def get_price(con, symbol, target_date, offset_days=0):
    """Get adjusted close on or just after target_date + offset_days."""
    shifted = target_date + timedelta(days=offset_days)
    target_epoch = int(datetime.combine(shifted, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(shifted + timedelta(days=10), datetime.min.time()).timestamp())
    row = con.execute("""
        SELECT adjClose FROM prices_cache
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, target_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def compute_portfolio_return(con, portfolio, entry_date, exit_date,
                             use_costs=True, verbose=False, offset_days=1):
    """Compute equal-weighted return for a portfolio."""
    if not portfolio:
        return 0.0, 0, 0

    symbol_returns = []
    for sym, info in portfolio.items():
        ep = get_price(con, sym, entry_date, offset_days=offset_days)
        xp = get_price(con, sym, exit_date, offset_days=offset_days)
        symbol_returns.append((sym, ep, xp, info["mcap"]))

    clean, skipped = filter_returns(symbol_returns, verbose=verbose)

    if not clean:
        return 0.0, 0, len(skipped)

    returns = []
    for sym, raw_ret, mcap in clean:
        if use_costs:
            cost = tiered_cost(mcap)
            net_ret = apply_costs(raw_ret, cost)
        else:
            net_ret = raw_ret
        returns.append(net_ret)

    mean_ret = sum(returns) / len(returns)
    return mean_ret, len(returns), len(skipped)


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run the full CCC backtest with four portfolio tracks."""
    print(f"Phase 2: Running annual backtest "
          f"({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")
    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        scored = screen_stocks(con, entry_date, mktcap_min)
        if not scored:
            if verbose:
                print(f"  {entry_date.year}: No scored stocks found, skipping")
            continue

        # Split into portfolios
        low = {s: v for s, v in scored.items() if v["bucket"] == "low"}
        mid = {s: v for s, v in scored.items() if v["bucket"] == "mid"}
        high = {s: v for s, v in scored.items() if v["bucket"] == "high"}
        low_decreasing = {s: v for s, v in scored.items()
                          if v["bucket"] == "low" and v["direction"] == "decreasing"}

        # Compute returns for each track
        track_data = {}
        for name, portfolio in [("low", low), ("mid", mid), ("high", high),
                                ("low_decreasing", low_decreasing)]:
            ret, cnt, skip = compute_portfolio_return(
                con, portfolio, entry_date, exit_date,
                use_costs=use_costs, verbose=verbose, offset_days=offset_days
            )
            track_data[name] = {"return": ret, "count": cnt, "skipped": skip}

        # Benchmark (local index or SPY)
        spy_ret = get_benchmark_return(con, benchmark_symbol, entry_date, exit_date,
                                       offset_days=offset_days)

        periods.append({
            "year": entry_date.year,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "low_return": track_data["low"]["return"],
            "mid_return": track_data["mid"]["return"],
            "high_return": track_data["high"]["return"],
            "low_decreasing_return": track_data["low_decreasing"]["return"],
            "spy_return": spy_ret,
            "low_count": track_data["low"]["count"],
            "mid_count": track_data["mid"]["count"],
            "high_count": track_data["high"]["count"],
            "low_decreasing_count": track_data["low_decreasing"]["count"],
            "total_count": sum(
                track_data[t]["count"] for t in ["low", "mid", "high"]
            ),
        })

        if verbose:
            s = track_data
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"Low={s['low']['return']*100:+.1f}% ({s['low']['count']}), "
                  f"Mid={s['mid']['return']*100:+.1f}% ({s['mid']['count']}), "
                  f"High={s['high']['return']*100:+.1f}% ({s['high']['count']}), "
                  f"Low+Dec={s['low_decreasing']['return']*100:+.1f}% ({s['low_decreasing']['count']}), "
                  f"SPY={spy_pct:+.1f}%")

    print(f"Phase 2 complete: {len(periods)} annual periods.\n")
    return periods


def build_output(periods, universe_name, risk_free_rate, periods_per_year):
    """Build output dict with all tracks + analysis."""
    valid = [p for p in periods if p["spy_return"] is not None]
    n = len(valid)
    if n == 0:
        return {"universe": universe_name, "error": "No valid periods"}

    spy_returns = [p["spy_return"] for p in valid]

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    results = {}
    for track, key in [("low", "low_return"), ("mid", "mid_return"),
                       ("high", "high_return"),
                       ("low_decreasing", "low_decreasing_return"),
                       ("spy", "spy_return")]:
        rets = [p[key] for p in valid]
        m = _compute_metrics(rets, spy_returns, periods_per_year,
                             risk_free_rate=risk_free_rate)
        pm = m["portfolio"]

        results[track] = {
            "cagr": round(pm["cagr"] * 100, 2),
            "total_return": round(pm["total_return"] * 100, 2),
            "volatility": round(pm["annualized_volatility"] * 100, 2),
            "sharpe": rnd(pm["sharpe_ratio"]),
            "sortino": rnd(pm["sortino_ratio"]),
            "calmar": rnd(pm["calmar_ratio"]),
            "max_drawdown": round(pm["max_drawdown"] * 100, 1),
            "var_95": round(pm["var_95"] * 100, 1) if pm["var_95"] is not None else None,
            "pct_negative_years": round(pm["pct_negative_periods"] * 100, 0),
            "max_consecutive_losses": pm["max_consecutive_losses"],
        }

        # Comparison metrics for key tracks
        if track in ("low", "low_decreasing"):
            c = m["comparison"]
            results[f"{track}_vs_spy"] = {
                "excess_cagr": round(c["excess_cagr"] * 100, 2),
                "information_ratio": rnd(c["information_ratio"]),
                "tracking_error": (round(c["tracking_error"] * 100, 2)
                                   if c["tracking_error"] is not None else None),
                "up_capture": (round(c["up_capture"] * 100, 1)
                               if c["up_capture"] is not None else None),
                "down_capture": (round(c["down_capture"] * 100, 1)
                                 if c["down_capture"] is not None else None),
                "beta": rnd(c["beta"]),
                "alpha": (round(c["alpha"] * 100, 2)
                          if c["alpha"] is not None else None),
            }

    # Decade breakdown
    decades = {}
    for p in valid:
        yr = p["year"]
        if yr < 2005:
            d = "2000-04"
        elif yr < 2010:
            d = "2005-09"
        elif yr < 2015:
            d = "2010-14"
        elif yr < 2020:
            d = "2015-19"
        else:
            d = "2020-25"

        if d not in decades:
            decades[d] = {"low": [], "high": [], "spy": []}
        decades[d]["low"].append(p["low_return"])
        decades[d]["high"].append(p["high_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            l_avg = sum(decades[d]["low"]) / len(decades[d]["low"]) * 100
            h_avg = sum(decades[d]["high"]) / len(decades[d]["high"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "low_return": round(l_avg, 1),
                "high_return": round(h_avg, 1),
                "spread": round(l_avg - h_avg, 1),
                "spy_return": round(spy_avg, 1),
            })

    # Aggregate
    low_high_spread = results["low"]["cagr"] - results["high"]["cagr"]
    low_vs_spy_cagr = results["low"]["cagr"] - results["spy"]["cagr"]

    avg_low = sum(p["low_count"] for p in valid) / n
    avg_mid = sum(p["mid_count"] for p in valid) / n
    avg_high = sum(p["high_count"] for p in valid) / n
    avg_low_dec = sum(p["low_decreasing_count"] for p in valid) / n
    avg_total = sum(p["total_count"] for p in valid) / n

    low_cash = sum(1 for p in valid if p["low_count"] == 0)
    high_cash = sum(1 for p in valid if p["high_count"] == 0)
    low_dec_cash = sum(1 for p in valid if p["low_decreasing_count"] == 0)

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "sector_exclusions": "Financial Services",
        "ccc_thresholds": {"low": f"< {CCC_LOW} days", "high": f"> {CCC_HIGH} days"},
        "portfolios": {
            "low_ccc": results["low"],
            "mid_ccc": results["mid"],
            "high_ccc": results["high"],
            "low_decreasing": results["low_decreasing"],
            "sp500": results["spy"],
        },
        "spread_cagr": round(low_high_spread, 2),
        "low_vs_spy": round(low_vs_spy_cagr, 2),
        "avg_stock_counts": {
            "low": round(avg_low, 0),
            "mid": round(avg_mid, 0),
            "high": round(avg_high, 0),
            "low_decreasing": round(avg_low_dec, 0),
            "total": round(avg_total, 0),
        },
        "cash_periods": {
            "low": low_cash,
            "high": high_cash,
            "low_decreasing": low_dec_cash,
        },
        "decade_breakdown": decade_results,
    }

    if "low_vs_spy" in results:
        output["low_vs_spy_detail"] = results["low_vs_spy"]
    if "low_decreasing_vs_spy" in results:
        output["low_decreasing_vs_spy"] = results["low_decreasing_vs_spy"]

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "low": round(p["low_return"] * 100, 2),
            "mid": round(p["mid_return"] * 100, 2),
            "high": round(p["high_return"] * 100, 2),
            "low_decreasing": round(p["low_decreasing_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "low_count": p["low_count"],
            "mid_count": p["mid_count"],
            "high_count": p["high_count"],
            "low_decreasing_count": p["low_decreasing_count"],
        }
        for p in valid
    ]

    return output


def print_summary(m):
    """Print formatted backtest results."""
    if "error" in m:
        print(f"\nERROR: {m['error']}")
        return

    p = m["portfolios"]
    print("\n" + "=" * 95)
    print(f"CASH CONVERSION CYCLE BACKTEST: {m['universe']}")
    print("=" * 95)
    print(f"Period: {m['period']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print(f"Excluded: {m['sector_exclusions']}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: Low={counts['low']:.0f}, Mid={counts['mid']:.0f}, "
          f"High={counts['high']:.0f}, Low+Dec={counts['low_decreasing']:.0f}, "
          f"Total={counts['total']:.0f}")
    cash = m["cash_periods"]
    if cash["low"] > 0 or cash["high"] > 0 or cash["low_decreasing"] > 0:
        print(f"Cash periods: Low={cash['low']}, High={cash['high']}, "
              f"Low+Dec={cash['low_decreasing']}")
    print("-" * 95)

    header = (f"{'Portfolio':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 95)

    bench_label = m.get("benchmark", {}).get("name", "S&P 500")
    for name, label in [("low_ccc", "Low CCC (<30d)"),
                         ("low_decreasing", "Low + Decreasing"),
                         ("mid_ccc", "Mid CCC (30-90d)"),
                         ("high_ccc", "High CCC (>90d)"),
                         ("sp500", bench_label)]:
        d = p[name]
        sortino = d.get('sortino')
        calmar = d.get('calmar')
        var95 = d.get('var_95')
        s_str = f"{sortino:>8.3f}" if sortino is not None else f"{'N/A':>8}"
        c_str = f"{calmar:>8.3f}" if calmar is not None else f"{'N/A':>8}"
        v_str = f"{var95:>7.1f}%" if var95 is not None else f"{'N/A':>8}"
        sharpe = d.get('sharpe')
        sh_str = f"{sharpe:>8.3f}" if sharpe is not None else f"{'N/A':>8}"
        print(f"{label:<22} {d['cagr']:>7.1f}% {d['volatility']:>7.1f}% "
              f"{sh_str} {s_str} {c_str} "
              f"{d['max_drawdown']:>7.1f}% {v_str}")

    bench_label = m.get("benchmark", {}).get("name", "S&P 500")
    print(f"\nLow-High CCC spread: {m['spread_cagr']:.1f}% per year")
    print(f"Low CCC vs {bench_label}: {m['low_vs_spy']:+.1f}%")

    lvs = m.get("low_vs_spy_detail")
    if lvs:
        print(f"\nLow CCC vs {bench_label}:")
        print(f"  Excess CAGR: {lvs['excess_cagr']:+.2f}%")
        if lvs.get('information_ratio') is not None:
            print(f"  Information Ratio: {lvs['information_ratio']:.3f}")
        if lvs.get('up_capture') is not None:
            print(f"  Up Capture: {lvs['up_capture']:.1f}%  |  "
                  f"Down Capture: {lvs['down_capture']:.1f}%")
        if lvs.get('beta') is not None:
            print(f"  Beta: {lvs['beta']:.3f}  |  Alpha: {lvs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Low CCC':>10} {'High CCC':>10} "
              f"{'Spread':>10} {bench_label:>10}")
        print("-" * 55)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['low_return']:>9.1f}% "
                  f"{d['high_return']:>9.1f}% "
                  f"{d['spread']:>+9.1f}% {d['spy_return']:>9.1f}%")

    print("=" * 95)


def run_single_exchange(args, preset_name=None, preset_data=None):
    """Run backtest for a single exchange/preset."""
    if preset_data:
        exchanges = preset_data["exchanges"]
        universe_name = preset_data["name"]
    else:
        exchanges, universe_name = resolve_exchanges(
            args,
            default_exchanges=["NYSE", "NASDAQ", "AMEX"],
            default_name="US_MAJOR"
        )

    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9
                    else f"{mktcap_threshold/1e6:.0f}M")
    use_costs = not args.no_costs
    periods_per_year = 1

    offset_days = 0 if args.no_next_day else 1
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "same-day close (legacy)" if args.no_next_day else "next-day close (MOC)"

    signal_desc = (f"CCC buckets (<{CCC_LOW}d / {CCC_LOW}-{CCC_HIGH}d / >{CCC_HIGH}d), "
                   f"MCap > {mktcap_label} local, excl. financials")
    print_header("CASH CONVERSION CYCLE BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  Portfolios: Low (<{CCC_LOW}d), Mid ({CCC_LOW}-{CCC_HIGH}d), "
          f"High (>{CCC_HIGH}d), Low+Decreasing")
    print(f"  Rebalancing: Annual (April 1), {START_YEAR}-{END_YEAR}")
    print(f"  Costs: {'size-tiered' if use_costs else 'none'}, "
          f"Rf: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 75)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(
        START_YEAR, END_YEAR, "annual", months=[4]
    )
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available.")
        return {"universe": universe_name, "error": "No data"}
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    t1 = time.time()
    periods = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=args.verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1

    output = build_output(periods, universe_name, risk_free_rate, periods_per_year)
    output["benchmark"] = {"symbol": benchmark_symbol, "name": benchmark_name}
    output["execution"] = exec_model
    print_summary(output)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s "
          f"(fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Cash Conversion Cycle backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "cash-conversion", args_str=" ".join(cloud_args),
            api_key=args.api_key, base_url=args.base_url, verbose=True
        )
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    if args.global_bt:
        all_results = {}
        for preset_name in GLOBAL_PRESETS:
            preset = EXCHANGE_PRESETS[preset_name]
            print(f"\n{'#' * 80}")
            print(f"# Exchange: {preset['name']} ({preset_name})")
            print(f"{'#' * 80}")
            try:
                output = run_single_exchange(args, preset_name, preset)
                all_results[preset["name"]] = output
            except Exception as e:
                print(f"  ERROR: {e}")
                import traceback
                traceback.print_exc()
                all_results[preset["name"]] = {
                    "universe": preset["name"],
                    "error": str(e),
                }

        # Print comparison summary
        print("\n" + "=" * 100)
        print("EXCHANGE COMPARISON SUMMARY")
        print("=" * 100)
        print(f"{'Exchange':<16} {'Low CAGR':>10} {'High CAGR':>10} "
              f"{'Spread':>10} {'Low+Dec':>10} {'SPY CAGR':>10} "
              f"{'Avg Low':>10} {'Avg Hi':>10}")
        print("-" * 100)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>10}  {r.get('error', '')[:50]}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['low_ccc']['cagr']:>9.1f}% "
                  f"{p['high_ccc']['cagr']:>9.1f}% "
                  f"{r['spread_cagr']:>+9.1f}% "
                  f"{p['low_decreasing']['cagr']:>9.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{avg['low']:>10.0f} "
                  f"{avg['high']:>10.0f}")
        print("=" * 100)

        if args.output:
            out_dir = os.path.dirname(args.output) or "."
            os.makedirs(out_dir, exist_ok=True)
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\nResults saved to {args.output}")
    else:
        output = run_single_exchange(args)

        if args.output:
            out_dir = os.path.dirname(args.output) or "."
            os.makedirs(out_dir, exist_ok=True)
            with open(args.output, "w") as f:
                json.dump(output, f, indent=2)
            print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
