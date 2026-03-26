#!/usr/bin/env python3
"""
Income Quality Backtest

Annual rebalancing (April), equal weight, three portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: Income Quality = Operating Cash Flow / Net Income (from key_metrics FY)
  - Pre-computed by FMP as incomeQuality
  - Filter: netIncome > 0 (avoid misleading ratios from negative NI)

Portfolios:
  High:   IQ > 1.2 (cash-backed earnings, 20%+ more cash than reported profit)
  Medium: 0.5 <= IQ <= 1.2 (moderate cash backing)
  Low:    IQ < 0.5 (accrual-heavy, earnings not backed by cash)

Based on Sloan (1996): high-accrual firms underperform by ~10% annually.

Benchmark: S&P 500 (SPY).

Usage:
    python3 income-quality/backtest.py
    python3 income-quality/backtest.py --preset india --verbose
    python3 income-quality/backtest.py --global --output results/exchange_comparison.json
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
from data_utils import query_parquet, generate_rebalance_dates, filter_returns
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, get_risk_free_rate,
                       EXCHANGE_PRESETS)

# --- Config ---
IQ_HIGH_THRESHOLD = 1.2     # OCF/NI > 1.2 = high quality
IQ_LOW_THRESHOLD = 0.5      # OCF/NI < 0.5 = low quality (accrual-heavy)
FILING_LAG_DAYS = 45        # Point-in-time data lag
START_YEAR = 2000
END_YEAR = 2025

# Exchanges to test in --global mode
GLOBAL_PRESETS = [
    "us", "india", "germany", "china", "hongkong", "canada",
    "uk", "switzerland", "sweden", "korea", "brazil",
    "taiwan", "singapore", "southafrica",
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all historical financial data and load into DuckDB."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter_sql = f"""
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE exchange IN ({ex_filter})
            )
        """
        profile_where = f"WHERE exchange IN ({ex_filter})"
    else:
        sym_filter_sql = "1=1"
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

    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2. Key metrics (incomeQuality + marketCap)
    print("  Fetching key metrics (incomeQuality)...")
    metrics_sql = f"""
        SELECT symbol, incomeQuality, marketCap,
            dateEpoch as filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND incomeQuality IS NOT NULL
          AND marketCap IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} rows")

    # 3. Income statement (netIncome for positive NI filter)
    print("  Fetching income statements (netIncome filter)...")
    income_sql = f"""
        SELECT symbol, netIncome,
            dateEpoch as filing_epoch
        FROM income_statement
        WHERE period = 'FY'
          AND netIncome IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, income_sql, con, "income_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} rows")

    # 4. Prices (only at rebalance dates + 10-day windows)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol = 'SPY'
            OR symbol IN (
                SELECT DISTINCT symbol FROM key_metrics WHERE period = 'FY'
                    AND incomeQuality IS NOT NULL
                    AND {sym_filter_sql}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count:,} price rows")

    return con


def classify_stocks(con, target_date, mktcap_min):
    """Classify stocks by income quality at a rebalance date.

    Returns dict: {symbol: (income_quality, group, market_cap)}
    where group is 'high', 'medium', or 'low'
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        -- Most recent FY key_metrics per symbol
        latest_metrics AS (
            SELECT symbol, incomeQuality, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol, filing_epoch ORDER BY filing_epoch DESC
                ) AS dedup_rn
                FROM metrics_cache WHERE filing_epoch <= ?
            ) WHERE dedup_rn = 1
        ),
        -- Most recent FY income statement per symbol (for NI > 0 filter)
        latest_income AS (
            SELECT symbol, netIncome,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol, filing_epoch ORDER BY filing_epoch DESC
                ) AS dedup_rn
                FROM income_cache WHERE filing_epoch <= ?
            ) WHERE dedup_rn = 1
        )
        SELECT
            m.symbol,
            m.incomeQuality,
            m.marketCap
        FROM latest_metrics m
        JOIN latest_income i ON m.symbol = i.symbol AND i.rn = 1
        JOIN universe u ON m.symbol = u.symbol
        WHERE m.rn = 1
          AND m.marketCap > ?
          AND i.netIncome > 0  -- Only positive net income (avoid misleading IQ ratios)
    """, [cutoff_epoch, cutoff_epoch, mktcap_min]).fetchall()

    result = {}
    for sym, iq, mcap in rows:
        if iq > IQ_HIGH_THRESHOLD:
            group = "high"
        elif iq < IQ_LOW_THRESHOLD:
            group = "low"
        else:
            group = "medium"
        result[sym] = (iq, group, mcap)
    return result


def get_price(con, symbol, target_date):
    """Get adjusted close price on or just after target_date."""
    target_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(target_date + timedelta(days=10), datetime.min.time()).timestamp())
    row = con.execute("""
        SELECT adjClose FROM prices_cache
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, target_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def compute_portfolio_return(con, portfolio, entry_date, exit_date,
                             use_costs=True, verbose=False):
    """Compute equal-weighted return for a portfolio of stocks."""
    if not portfolio:
        return 0.0, 0, 0

    symbol_returns = []
    for sym, (iq, group, mcap) in portfolio.items():
        ep = get_price(con, sym, entry_date)
        xp = get_price(con, sym, exit_date)
        symbol_returns.append((sym, ep, xp, mcap))

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


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run the full Income Quality backtest with three portfolio tracks."""
    print(f"Phase 2: Running annual backtest "
          f"({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")
    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        classified = classify_stocks(con, entry_date, mktcap_min)
        if not classified:
            if verbose:
                print(f"  {entry_date.year}: No classified stocks found, skipping")
            continue

        # Split into groups
        high = {s: v for s, v in classified.items() if v[1] == "high"}
        medium = {s: v for s, v in classified.items() if v[1] == "medium"}
        low = {s: v for s, v in classified.items() if v[1] == "low"}

        # Compute returns for each track
        track_data = {}
        for name, portfolio in [("high", high), ("medium", medium), ("low", low)]:
            ret, cnt, skip = compute_portfolio_return(
                con, portfolio, entry_date, exit_date,
                use_costs=use_costs, verbose=verbose
            )
            track_data[name] = {"return": ret, "count": cnt, "skipped": skip}

        # Benchmark
        spy_ep = get_price(con, "SPY", entry_date)
        spy_xp = get_price(con, "SPY", exit_date)
        spy_ret = ((spy_xp - spy_ep) / spy_ep
                   if spy_ep and spy_xp and spy_ep > 0 else None)

        periods.append({
            "year": entry_date.year,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "high_return": track_data["high"]["return"],
            "medium_return": track_data["medium"]["return"],
            "low_return": track_data["low"]["return"],
            "spy_return": spy_ret,
            "high_count": track_data["high"]["count"],
            "medium_count": track_data["medium"]["count"],
            "low_count": track_data["low"]["count"],
            "total_count": sum(
                track_data[t]["count"] for t in ["high", "medium", "low"]
            ),
        })

        if verbose:
            s = track_data
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"High={s['high']['return']*100:+.1f}% ({s['high']['count']}), "
                  f"Med={s['medium']['return']*100:+.1f}% ({s['medium']['count']}), "
                  f"Low={s['low']['return']*100:+.1f}% ({s['low']['count']}), "
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
    for track, key in [("high", "high_return"),
                       ("medium", "medium_return"),
                       ("low", "low_return"),
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

        # Comparison metrics for high track
        if track == "high":
            c = m["comparison"]
            results["high_vs_spy"] = {
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
                "win_rate": (round(c["win_rate"] * 100, 1)
                             if c["win_rate"] is not None else None),
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
            decades[d] = {"high": [], "medium": [], "low": [], "spy": []}
        decades[d]["high"].append(p["high_return"])
        decades[d]["medium"].append(p["medium_return"])
        decades[d]["low"].append(p["low_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            h_avg = sum(decades[d]["high"]) / len(decades[d]["high"]) * 100
            m_avg = sum(decades[d]["medium"]) / len(decades[d]["medium"]) * 100
            l_avg = sum(decades[d]["low"]) / len(decades[d]["low"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "high_return": round(h_avg, 1),
                "medium_return": round(m_avg, 1),
                "low_return": round(l_avg, 1),
                "spy_return": round(spy_avg, 1),
                "high_vs_spy": round(h_avg - spy_avg, 1),
                "spread": round(h_avg - l_avg, 1),
            })

    # Aggregate metrics
    high_excess = results["high"]["cagr"] - results["spy"]["cagr"]
    spread = results["high"]["cagr"] - results["low"]["cagr"]

    avg_high = sum(p["high_count"] for p in valid) / n
    avg_med = sum(p["medium_count"] for p in valid) / n
    avg_low = sum(p["low_count"] for p in valid) / n
    avg_total = sum(p["total_count"] for p in valid) / n

    high_cash = sum(1 for p in valid if p["high_count"] == 0)

    # High IQ percentage
    high_pcts = []
    for p in valid:
        if p["total_count"] > 0:
            high_pcts.append(p["high_count"] / p["total_count"] * 100)

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "signal": f"Income Quality (OCF/NI): High > {IQ_HIGH_THRESHOLD}, Low < {IQ_LOW_THRESHOLD}",
        "signal_formula": "Operating Cash Flow / Net Income",
        "portfolios": {
            "high": results["high"],
            "medium": results["medium"],
            "low": results["low"],
            "sp500": results["spy"],
        },
        "high_excess_cagr": round(high_excess, 2),
        "high_low_spread": round(spread, 2),
        "avg_stock_counts": {
            "high": round(avg_high, 0),
            "medium": round(avg_med, 0),
            "low": round(avg_low, 0),
            "total": round(avg_total, 0),
        },
        "cash_periods": {"high": high_cash},
        "avg_high_pct": round(
            sum(high_pcts) / len(high_pcts), 1
        ) if high_pcts else 0,
        "decade_breakdown": decade_results,
    }

    if "high_vs_spy" in results:
        output["high_vs_spy"] = results["high_vs_spy"]

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "high": round(p["high_return"] * 100, 2),
            "medium": round(p["medium_return"] * 100, 2),
            "low": round(p["low_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "high_count": p["high_count"],
            "medium_count": p["medium_count"],
            "low_count": p["low_count"],
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
    print(f"INCOME QUALITY BACKTEST: {m['universe']}")
    print("=" * 95)
    print(f"Period: {m['period']}")
    print(f"Signal: {m['signal']}")
    print(f"Formula: {m['signal_formula']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: High={counts['high']:.0f}, "
          f"Medium={counts['medium']:.0f}, "
          f"Low={counts['low']:.0f}, Total={counts['total']:.0f}")
    print(f"High IQ pool: ~{m['avg_high_pct']:.0f}% of universe")
    cash = m["cash_periods"]
    if cash["high"] > 0:
        print(f"Cash periods (high): {cash['high']}")
    print("-" * 95)

    header = (f"{'Portfolio':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 95)

    for name, label in [("high", "High IQ (>1.2)"),
                         ("medium", "Medium IQ (0.5-1.2)"),
                         ("low", "Low IQ (<0.5)"),
                         ("sp500", "S&P 500")]:
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

    print(f"\nHigh IQ vs SPY excess: {m['high_excess_cagr']:+.1f}% per year")
    print(f"High-Low spread: {m['high_low_spread']:+.1f}% per year")

    # High vs SPY comparison
    hvs = m.get("high_vs_spy")
    if hvs:
        print(f"\nHigh IQ vs S&P 500:")
        print(f"  Excess CAGR: {hvs['excess_cagr']:+.2f}%")
        if hvs.get('win_rate') is not None:
            print(f"  Win Rate: {hvs['win_rate']:.1f}%")
        if hvs.get('information_ratio') is not None:
            print(f"  Information Ratio: {hvs['information_ratio']:.3f}")
        if hvs.get('up_capture') is not None:
            print(f"  Up Capture: {hvs['up_capture']:.1f}%  |  "
                  f"Down Capture: {hvs['down_capture']:.1f}%")
        if hvs.get('beta') is not None:
            print(f"  Beta: {hvs['beta']:.3f}  |  Alpha: {hvs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'High':>10} {'Medium':>10} "
              f"{'Low':>10} {'SPY':>10} {'vs SPY':>10} {'Spread':>10}")
        print("-" * 75)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['high_return']:>9.1f}% "
                  f"{d['medium_return']:>9.1f}% "
                  f"{d['low_return']:>9.1f}% {d['spy_return']:>9.1f}% "
                  f"{d['high_vs_spy']:>+9.1f}% {d['spread']:>+9.1f}%")

    print("=" * 95)


def run_single_exchange(args, preset_name=None, preset_data=None):
    """Run backtest for a single exchange/preset. Returns output dict."""
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

    signal_desc = (f"Income Quality (OCF/NI): High > {IQ_HIGH_THRESHOLD}, "
                   f"Low < {IQ_LOW_THRESHOLD}, MCap > {mktcap_label} local")
    print_header("INCOME QUALITY BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  Signal: Operating Cash Flow / Net Income (FMP incomeQuality)")
    print(f"  Filter: netIncome > 0 (exclude negative earners)")
    print(f"  Rebalancing: Annual (April 1), {START_YEAR}-{END_YEAR}")
    print(f"  Costs: {'size-tiered' if use_costs else 'none'}, "
          f"Rf: {risk_free_rate*100:.1f}%")
    print("=" * 75)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Phase 1: Fetch data
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

    # Phase 2: Run backtest
    t1 = time.time()
    periods = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=args.verbose)
    bt_time = time.time() - t1

    # Phase 3: Compute and display metrics
    output = build_output(periods, universe_name, risk_free_rate, periods_per_year)
    print_summary(output)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s "
          f"(fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Income Quality (OCF/NI) backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "income-quality", args_str=" ".join(cloud_args),
            api_key=args.api_key, base_url=args.base_url, verbose=True
        )
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    if args.global_bt:
        # Global mode: iterate over all exchange presets
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
        print(f"{'Exchange':<16} {'High CAGR':>10} {'Medium':>10} "
              f"{'Low':>10} {'SPY CAGR':>10} {'Excess':>10} "
              f"{'Spread':>10} {'Avg High':>10}")
        print("-" * 100)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>10}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['high']['cagr']:>9.1f}% "
                  f"{p['medium']['cagr']:>9.1f}% "
                  f"{p['low']['cagr']:>9.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{r['high_excess_cagr']:>+9.1f}% "
                  f"{r['high_low_spread']:>+9.1f}% "
                  f"{avg['high']:>10.0f}")
        print("=" * 100)

        # Save results
        if args.output:
            out_dir = os.path.dirname(args.output) or "."
            os.makedirs(out_dir, exist_ok=True)
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\nResults saved to {args.output}")
    else:
        # Single exchange mode
        output = run_single_exchange(args)

        if args.output:
            out_dir = os.path.dirname(args.output) or "."
            os.makedirs(out_dir, exist_ok=True)
            with open(args.output, "w") as f:
                json.dump(output, f, indent=2)
            print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
