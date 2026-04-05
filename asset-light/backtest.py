#!/usr/bin/env python3
"""
Asset-Light Business Models Backtest

Annual rebalancing (April), equal weight, three portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: Composite asset-light score from three FY financial metrics:
  1. Asset Turnover = revenue / totalAssets (higher = more asset-light)
  2. Capex Intensity = ABS(capitalExpenditure) / revenue (lower = more asset-light)
  3. Gross Margin = grossProfit / revenue (higher = more asset-light)

Each metric is PERCENT_RANK'd across the universe, then averaged.
  Top quintile (>= 0.8) = "Asset-Light" portfolio
  Bottom quintile (<= 0.2) = "Asset-Heavy" portfolio

Academic basis: Eisfeldt & Papanikolaou (2013) - intangible capital-intensive
firms earn higher risk-adjusted returns.

Universe: Non-financial, non-utility stocks above exchange-specific market cap threshold.
Benchmark: S&P 500 (SPY).

Usage:
    python3 asset-light/backtest.py
    python3 asset-light/backtest.py --preset india --verbose
    python3 asset-light/backtest.py --global --output results/exchange_comparison.json
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
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS)
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, get_risk_free_rate,
                       EXCHANGE_PRESETS)

# --- Config ---
QUINTILE_TOP = 0.80     # Asset-Light threshold (top 20%)
QUINTILE_BOTTOM = 0.20  # Asset-Heavy threshold (bottom 20%)
FILING_LAG_DAYS = 45
EXCLUDED_SECTORS = ("Financial Services", "Utilities")
START_YEAR = 2000
END_YEAR = 2025
MAX_STOCKS = 50         # Cap portfolio size per quintile

GLOBAL_PRESETS = [
    "us", "india", "germany", "china", "hongkong", "canada",
    "uk", "switzerland", "sweden", "korea", "brazil",
    "taiwan", "singapore", "southafrica", "france",
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all historical financial data and load into DuckDB."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter_sql = f"""
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE exchange IN ({ex_filter})
                  AND COALESCE(sector, '') NOT IN ('Financial Services', 'Utilities')
            )
        """
        profile_where = f"WHERE exchange IN ({ex_filter})"
    else:
        sym_filter_sql = """
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE COALESCE(sector, '') NOT IN ('Financial Services', 'Utilities')
            )
        """
        profile_where = ""

    # Build benchmark symbols list (SPY + local index)
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(bench_symbols)

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
    print(f"  Universe: {len(profiles)} symbols, {excluded_count} financials/utilities excluded, "
          f"{len(filtered)} remaining")

    if not filtered:
        print("  No non-financial/utility symbols found.")
        return None

    sym_values = ",".join(f"('{r['symbol']}')" for r in filtered)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2-4: Financial data
    queries = [
        ("income_cache", f"""
            SELECT symbol, revenue, grossProfit,
                dateEpoch as filing_epoch
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL AND revenue > 0
              AND {sym_filter_sql}
        """, "income statements"),
        ("balance_cache", f"""
            SELECT symbol, totalAssets,
                dateEpoch as filing_epoch
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalAssets IS NOT NULL AND totalAssets > 0
              AND {sym_filter_sql}
        """, "balance sheets"),
        ("cashflow_cache", f"""
            SELECT symbol, capitalExpenditure,
                dateEpoch as filing_epoch
            FROM cash_flow_statement
            WHERE period = 'FY'
              AND {sym_filter_sql}
        """, "cash flow statements"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap,
                dateEpoch as filing_epoch
            FROM key_metrics
            WHERE period = 'FY'
              AND marketCap IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (market cap)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              limit=5000000, timeout=600,
                              memory_mb=4096, threads=2)
        print(f"    -> {count:,} rows")

    # 5. Prices
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=10)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR symbol IN (
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
                    AND {sym_filter_sql}
                INTERSECT
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
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


def compute_asset_light_scores(con, target_date, mktcap_min):
    """Compute composite asset-light scores for all qualifying stocks.

    Returns dict: {symbol: (composite_score, classification, market_cap)}
    where classification is "asset_light", "asset_heavy", or "middle".
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        inc AS (
            SELECT symbol, revenue, grossProfit, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        bs AS (
            SELECT symbol, totalAssets, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ?
        ),
        cf AS (
            SELECT symbol, capitalExpenditure, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM cashflow_cache WHERE filing_epoch <= ?
        ),
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        base AS (
            SELECT
                inc.symbol,
                inc.revenue / NULLIF(bs.totalAssets, 0) AS asset_turnover,
                ABS(cf.capitalExpenditure) / NULLIF(inc.revenue, 0) AS capex_intensity,
                inc.grossProfit / NULLIF(inc.revenue, 0) AS gross_margin,
                met.marketCap
            FROM inc
            JOIN bs ON inc.symbol = bs.symbol AND bs.rn = 1
            JOIN cf ON inc.symbol = cf.symbol AND cf.rn = 1
            JOIN met ON inc.symbol = met.symbol AND met.rn = 1
            JOIN universe u ON inc.symbol = u.symbol
            WHERE inc.rn = 1
              AND met.marketCap > ?
              AND inc.revenue / NULLIF(bs.totalAssets, 0) IS NOT NULL
              AND ABS(cf.capitalExpenditure) / NULLIF(inc.revenue, 0) IS NOT NULL
              AND inc.grossProfit / NULLIF(inc.revenue, 0) IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                asset_turnover,
                capex_intensity,
                gross_margin,
                marketCap,
                PERCENT_RANK() OVER (ORDER BY asset_turnover ASC) AS turnover_rank,
                PERCENT_RANK() OVER (ORDER BY capex_intensity DESC) AS capex_rank,
                PERCENT_RANK() OVER (ORDER BY gross_margin ASC) AS margin_rank
            FROM base
        )
        SELECT
            symbol,
            (turnover_rank + capex_rank + margin_rank) / 3.0 AS composite_score,
            marketCap
        FROM ranked
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, cutoff_epoch, mktcap_min]).fetchall()

    result = {}
    for sym, score, mcap in rows:
        if score >= QUINTILE_TOP:
            classification = "asset_light"
        elif score <= QUINTILE_BOTTOM:
            classification = "asset_heavy"
        else:
            classification = "middle"
        result[sym] = (score, classification, mcap)
    return result


def get_price(con, symbol, target_date, offset_days=1):
    """Get adjusted close price on or just after target_date + offset_days.

    offset_days=1 implements MOC execution: signal on close of rebalance date,
    execute at next trading day's close. offset_days=0 = same-day (biased).
    """
    effective_date = target_date + timedelta(days=offset_days)
    target_epoch = int(datetime.combine(effective_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(effective_date + timedelta(days=10), datetime.min.time()).timestamp())
    row = con.execute("""
        SELECT adjClose FROM prices_cache
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, target_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def compute_portfolio_return(con, portfolio, entry_date, exit_date,
                             use_costs=True, verbose=False, offset_days=1):
    """Compute equal-weighted return for a portfolio of stocks.

    Args:
        portfolio: dict {symbol: (score, classification, market_cap)}
        offset_days: days to shift for MOC execution (1=next-day close, 0=same-day)

    Returns:
        tuple (mean_return, count, skipped_count)
    """
    if not portfolio:
        return 0.0, 0, 0

    symbol_returns = []
    for sym, (score, cls, mcap) in portfolio.items():
        ep = get_price(con, sym, entry_date, offset_days=offset_days)
        xp = get_price(con, sym, exit_date, offset_days=offset_days)
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


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run the full asset-light backtest with three portfolio tracks."""
    print(f"Phase 2: Running annual backtest "
          f"({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")
    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        scored = compute_asset_light_scores(con, entry_date, mktcap_min)
        if not scored:
            if verbose:
                print(f"  {entry_date.year}: No scored stocks found, skipping")
            continue

        # Split into quintiles
        light = {s: v for s, v in scored.items() if v[1] == "asset_light"}
        heavy = {s: v for s, v in scored.items() if v[1] == "asset_heavy"}

        # Cap portfolio size (take top/bottom by score)
        if len(light) > MAX_STOCKS:
            sorted_light = sorted(light.items(), key=lambda x: x[1][0], reverse=True)
            light = dict(sorted_light[:MAX_STOCKS])
        if len(heavy) > MAX_STOCKS:
            sorted_heavy = sorted(heavy.items(), key=lambda x: x[1][0])
            heavy = dict(sorted_heavy[:MAX_STOCKS])

        # Compute returns for each track
        track_data = {}
        for name, portfolio in [("asset_light", light), ("asset_heavy", heavy)]:
            ret, cnt, skip = compute_portfolio_return(
                con, portfolio, entry_date, exit_date,
                use_costs=use_costs, verbose=verbose, offset_days=offset_days
            )
            track_data[name] = {"return": ret, "count": cnt, "skipped": skip}

        # Benchmark
        spy_ret = get_benchmark_return(con, benchmark_symbol, entry_date, exit_date,
                                       offset_days=offset_days)

        periods.append({
            "year": entry_date.year,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "light_return": track_data["asset_light"]["return"],
            "heavy_return": track_data["asset_heavy"]["return"],
            "spy_return": spy_ret,
            "light_count": track_data["asset_light"]["count"],
            "heavy_count": track_data["asset_heavy"]["count"],
            "total_scored": len(scored),
        })

        if verbose:
            s = track_data
            bench_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"Light={s['asset_light']['return']*100:+.1f}% ({s['asset_light']['count']}), "
                  f"Heavy={s['asset_heavy']['return']*100:+.1f}% ({s['asset_heavy']['count']}), "
                  f"Bench={bench_pct:+.1f}%, Universe={len(scored)}")

    print(f"Phase 2 complete: {len(periods)} annual periods.\n")
    return periods


def build_output(periods, universe_name, risk_free_rate, periods_per_year,
                 benchmark_name="S&P 500"):
    """Build output dict with all tracks + analysis."""
    valid = [p for p in periods if p["spy_return"] is not None]
    n = len(valid)
    if n == 0:
        return {"universe": universe_name, "error": "No valid periods"}

    spy_returns = [p["spy_return"] for p in valid]

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    results = {}
    for track, key in [("asset_light", "light_return"),
                       ("asset_heavy", "heavy_return"),
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

        # Comparison metrics for asset-light
        if track == "asset_light":
            c = m["comparison"]
            results["light_vs_spy"] = {
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
                "win_rate": round(c["win_rate"] * 100, 1),
            }

    # Light-Heavy spread
    light_heavy_spread = results["asset_light"]["cagr"] - results["asset_heavy"]["cagr"]

    avg_light = sum(p["light_count"] for p in valid) / n
    avg_heavy = sum(p["heavy_count"] for p in valid) / n
    avg_total = sum(p["total_scored"] for p in valid) / n

    light_cash = sum(1 for p in valid if p["light_count"] == 0)
    heavy_cash = sum(1 for p in valid if p["heavy_count"] == 0)

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
            decades[d] = {"light": [], "heavy": [], "spy": []}
        decades[d]["light"].append(p["light_return"])
        decades[d]["heavy"].append(p["heavy_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            l_avg = sum(decades[d]["light"]) / len(decades[d]["light"]) * 100
            h_avg = sum(decades[d]["heavy"]) / len(decades[d]["heavy"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "light_return": round(l_avg, 1),
                "heavy_return": round(h_avg, 1),
                "spread": round(l_avg - h_avg, 1),
                "spy_return": round(spy_avg, 1),
            })

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "sector_exclusions": "Financial Services, Utilities",
        "signal": "Composite: PERCENT_RANK(asset_turnover) + PERCENT_RANK(1/capex_intensity) + PERCENT_RANK(gross_margin), averaged",
        "quintile_thresholds": {"asset_light": f">= {QUINTILE_TOP}", "asset_heavy": f"<= {QUINTILE_BOTTOM}"},
        "benchmark_name": benchmark_name,
        "portfolios": {
            "asset_light": results["asset_light"],
            "asset_heavy": results["asset_heavy"],
            "sp500": results["spy"],
        },
        "light_heavy_spread_cagr": round(light_heavy_spread, 2),
        "light_vs_spy": results.get("light_vs_spy", {}),
        "avg_stock_counts": {
            "light": round(avg_light, 0),
            "heavy": round(avg_heavy, 0),
            "total_universe": round(avg_total, 0),
        },
        "cash_periods": {
            "light": light_cash,
            "heavy": heavy_cash,
        },
        "decade_breakdown": decade_results,
    }

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "light": round(p["light_return"] * 100, 2),
            "heavy": round(p["heavy_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "light_count": p["light_count"],
            "heavy_count": p["heavy_count"],
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
    print(f"ASSET-LIGHT BACKTEST: {m['universe']}")
    print("=" * 95)
    print(f"Period: {m['period']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print(f"Excluded: {m['sector_exclusions']}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: Light={counts['light']:.0f}, Heavy={counts['heavy']:.0f}, "
          f"Universe={counts['total_universe']:.0f}")
    cash = m["cash_periods"]
    if cash["light"] > 0 or cash["heavy"] > 0:
        print(f"Cash periods: Light={cash['light']}, Heavy={cash['heavy']}")
    print("-" * 95)

    header = (f"{'Portfolio':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 95)

    bench_label = m.get("benchmark_name", "S&P 500")
    for name, label in [("asset_light", "Asset-Light (top 20%)"),
                         ("asset_heavy", "Asset-Heavy (bot 20%)"),
                         ("sp500", f"{bench_label}")]:
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

    print(f"\nLight-Heavy spread: {m['light_heavy_spread_cagr']:+.1f}% per year")

    lvs = m.get("light_vs_spy")
    if lvs:
        print(f"\nAsset-Light vs {bench_label}:")
        print(f"  Excess CAGR: {lvs['excess_cagr']:+.2f}%")
        if lvs.get('win_rate') is not None:
            print(f"  Win Rate: {lvs['win_rate']:.1f}%")
        if lvs.get('information_ratio') is not None:
            print(f"  Information Ratio: {lvs['information_ratio']:.3f}")
        if lvs.get('up_capture') is not None:
            print(f"  Up Capture: {lvs['up_capture']:.1f}%  |  "
                  f"Down Capture: {lvs['down_capture']:.1f}%")
        if lvs.get('beta') is not None:
            print(f"  Beta: {lvs['beta']:.3f}  |  Alpha: {lvs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Light':>10} {'Heavy':>10} "
              f"{'Spread':>10} {'SPY':>10}")
        print("-" * 55)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['light_return']:>9.1f}% "
                  f"{d['heavy_return']:>9.1f}% "
                  f"{d['spread']:>+9.1f}% {d['spy_return']:>9.1f}%")

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
    offset_days = 0 if args.no_next_day else 1
    exec_model = "same-day (biased)" if offset_days == 0 else "next-day close (MOC)"
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)

    signal_desc = (f"Composite asset-light score (turnover + 1/capex + margin), "
                   f"MCap > {mktcap_label} local, excl. financials/utilities")
    print_header("ASSET-LIGHT BUSINESS MODELS BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  Portfolios: Light (top 20%), Heavy (bottom 20%)")
    print(f"  Rebalancing: Annual (April 1), {START_YEAR}-{END_YEAR}")
    print(f"  Costs: {'size-tiered' if use_costs else 'none'}, "
          f"Rf: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
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
                           use_costs=use_costs, verbose=args.verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1

    # Phase 3: Compute and display metrics
    output = build_output(periods, universe_name, risk_free_rate, periods_per_year,
                          benchmark_name=benchmark_name)
    print_summary(output)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s "
          f"(fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Asset-Light Business Models backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "asset-light", args_str=" ".join(cloud_args),
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
        print("\n" + "=" * 95)
        print("EXCHANGE COMPARISON SUMMARY")
        print("=" * 95)
        print(f"{'Exchange':<16} {'Light CAGR':>11} {'Heavy CAGR':>11} "
              f"{'Spread':>10} {'SPY CAGR':>10} "
              f"{'Avg Light':>10} {'Avg Heavy':>10}")
        print("-" * 80)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>11}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['asset_light']['cagr']:>10.1f}% "
                  f"{p['asset_heavy']['cagr']:>10.1f}% "
                  f"{r['light_heavy_spread_cagr']:>+9.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{avg['light']:>10.0f} {avg['heavy']:>10.0f}")
        print("=" * 95)

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
