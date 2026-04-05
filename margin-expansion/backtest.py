#!/usr/bin/env python3
"""
Margin Expansion Backtest

Annual rebalancing (April), equal weight, three portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: Operating Profit Margin expansion over prior 3 fiscal years.
  Expansion = currentOPM - avg(OPM_1, OPM_2, OPM_3)
  OPM = operatingIncome / revenue (from income_statement FY)

Portfolios:
  Expanding (>+1pp): Companies improving operating margins
  Stable (-1pp to +1pp): Flat margins
  Contracting (<-1pp): Deteriorating margins
  Consecutive Expanding: 2+ years of YoY margin improvement (strongest signal)

Academic basis:
  Novy-Marx (2013) "The Other Side of Value: Gross Profitability Premium"
  Haugen & Baker (1996) "Commonality in Determinants of Expected Stock Returns"

Universe: Non-financial stocks above exchange-specific market cap threshold.
Benchmark: S&P 500 (SPY).

Usage:
    python3 margin-expansion/backtest.py
    python3 margin-expansion/backtest.py --preset india --verbose
    python3 margin-expansion/backtest.py --global --output results/exchange_comparison.json
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
                        get_local_benchmark, get_benchmark_return,
                        LOCAL_INDEX_BENCHMARKS)
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, get_risk_free_rate,
                       EXCHANGE_PRESETS)

# --- Config ---
EXPANSION_THRESHOLD = 0.01   # +1pp = "expanding"
CONTRACTION_THRESHOLD = -0.01  # -1pp = "contracting"
FILING_LAG_DAYS = 45
EXCLUDED_SECTORS = ("Financial Services",)
START_YEAR = 2000
END_YEAR = 2025
MAX_STOCKS = 30  # Top 30 expanders for the "expanding" portfolio
MAX_SINGLE_RETURN = 2.0  # Cap single-stock return at 200%
MIN_ENTRY_PRICE = 1.0

GLOBAL_PRESETS = [
    "us", "india", "germany", "china", "hongkong", "canada",
    "uk", "switzerland", "sweden", "korea", "brazil",
    "taiwan", "singapore", "southafrica", "france",
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch margin data and prices into DuckDB."""
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

    # 2. Income statement (OPM = operatingIncome / revenue)
    print("  Fetching income statement (FY)...")
    income_sql = f"""
        SELECT symbol,
            operatingIncome,
            revenue,
            dateEpoch as filing_epoch
        FROM income_statement
        WHERE period = 'FY'
          AND revenue IS NOT NULL AND revenue > 0
          AND operatingIncome IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, income_sql, con, "income_cache", verbose=verbose,
                          limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} rows")

    # 3. Key metrics (market cap for size filter + cost tiering)
    print("  Fetching key metrics (market cap)...")
    metrics_sql = f"""
        SELECT symbol,
            marketCap,
            dateEpoch as filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache", verbose=verbose,
                          limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    print(f"    -> {count:,} rows")

    # 4. Prices (only at rebalance dates + 10-day windows)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=11)  # +1 extra day for offset_days=1 MOC
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
                SELECT DISTINCT symbol FROM income_statement
                WHERE period = 'FY'
                  AND revenue > 0
                  AND operatingIncome IS NOT NULL
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


def screen_stocks(con, target_date, mktcap_min):
    """Screen stocks by margin expansion at a rebalance date.

    Computes OPM = operatingIncome/revenue for each FY filing,
    then expansion = current OPM - avg(prior 3 FY OPMs).

    Returns dict: {symbol: {"expansion": float, "current_opm": float,
                             "avg_prior_3yr": float, "bucket": str,
                             "yoy_improving": bool, "mcap": float}}
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH income_ranked AS (
            SELECT symbol,
                CAST(operatingIncome AS DOUBLE) / CAST(revenue AS DOUBLE) AS opm,
                filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache
            WHERE filing_epoch <= ?
              AND revenue > 0
        ),
        with_lags AS (
            SELECT
                symbol,
                opm AS current_opm,
                LAG(opm, 1) OVER (PARTITION BY symbol ORDER BY filing_epoch) AS opm_1,
                LAG(opm, 2) OVER (PARTITION BY symbol ORDER BY filing_epoch) AS opm_2,
                LAG(opm, 3) OVER (PARTITION BY symbol ORDER BY filing_epoch) AS opm_3,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn2
            FROM income_ranked
            WHERE rn <= 10
        ),
        expansion_calc AS (
            SELECT
                symbol,
                current_opm,
                opm_1,
                (opm_1 + opm_2 + opm_3) / 3.0 AS avg_prior_3yr,
                current_opm - (opm_1 + opm_2 + opm_3) / 3.0 AS expansion,
                CASE WHEN current_opm > opm_1 THEN 1 ELSE 0 END AS yoy_improving
            FROM with_lags
            WHERE rn2 = 1
              AND opm_1 IS NOT NULL
              AND opm_2 IS NOT NULL
              AND opm_3 IS NOT NULL
        ),
        km AS (
            SELECT symbol, marketCap,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
        )
        SELECT e.symbol, e.expansion, e.current_opm, e.avg_prior_3yr,
               e.yoy_improving, km.marketCap
        FROM expansion_calc e
        JOIN km ON e.symbol = km.symbol AND km.rn = 1
        JOIN universe u ON e.symbol = u.symbol
        WHERE km.marketCap > ?
    """, [cutoff_epoch, cutoff_epoch, mktcap_min]).fetchall()

    result = {}
    for sym, expansion, current_opm, avg_prior, yoy_improving, mcap in rows:
        if expansion > EXPANSION_THRESHOLD:
            bucket = "expanding"
        elif expansion < CONTRACTION_THRESHOLD:
            bucket = "contracting"
        else:
            bucket = "stable"

        result[sym] = {
            "expansion": expansion,
            "current_opm": current_opm,
            "avg_prior_3yr": avg_prior,
            "bucket": bucket,
            "yoy_improving": bool(yoy_improving),
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

    clean, skipped = filter_returns(symbol_returns, min_entry_price=MIN_ENTRY_PRICE,
                                     max_single_return=MAX_SINGLE_RETURN, verbose=verbose)

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
    """Run the full margin expansion backtest with portfolio tracks."""
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

        # Split into portfolios - ALL stocks in each bucket (equal weight)
        expanding = {s: v for s, v in scored.items() if v["bucket"] == "expanding"}
        stable = {s: v for s, v in scored.items() if v["bucket"] == "stable"}
        contracting = {s: v for s, v in scored.items() if v["bucket"] == "contracting"}

        # Consecutive expanders: expanding AND yoy_improving (strongest signal)
        consecutive = {s: v for s, v in expanding.items() if v["yoy_improving"]}

        # Compute returns for each track
        track_data = {}
        for name, portfolio in [("expanding", expanding),
                                ("stable", stable),
                                ("contracting", contracting),
                                ("consecutive", consecutive)]:
            ret, cnt, skip = compute_portfolio_return(
                con, portfolio, entry_date, exit_date,
                use_costs=use_costs, verbose=verbose, offset_days=offset_days
            )
            track_data[name] = {"return": ret, "count": cnt, "skipped": skip}

        # Benchmark (local index, MOC-adjusted)
        spy_ret = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

        periods.append({
            "year": entry_date.year,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "expanding_return": track_data["expanding"]["return"],
            "stable_return": track_data["stable"]["return"],
            "contracting_return": track_data["contracting"]["return"],
            "consecutive_return": track_data["consecutive"]["return"],
            "spy_return": spy_ret,
            "expanding_count": track_data["expanding"]["count"],
            "stable_count": track_data["stable"]["count"],
            "contracting_count": track_data["contracting"]["count"],
            "consecutive_count": track_data["consecutive"]["count"],
            "total_expanding": len(expanding),
            "total_scored": len(scored),
        })

        if verbose:
            s = track_data
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"Exp={s['expanding']['return']*100:+.1f}% ({s['expanding']['count']}), "
                  f"Stb={s['stable']['return']*100:+.1f}% ({s['stable']['count']}), "
                  f"Con={s['contracting']['return']*100:+.1f}% ({s['contracting']['count']}), "
                  f"Consec={s['consecutive']['return']*100:+.1f}% ({s['consecutive']['count']}), "
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
    for track, key in [("expanding", "expanding_return"),
                       ("stable", "stable_return"),
                       ("contracting", "contracting_return"),
                       ("consecutive", "consecutive_return"),
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
        if track in ("expanding", "consecutive"):
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
            decades[d] = {"expanding": [], "contracting": [], "spy": []}
        decades[d]["expanding"].append(p["expanding_return"])
        decades[d]["contracting"].append(p["contracting_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            e_avg = sum(decades[d]["expanding"]) / len(decades[d]["expanding"]) * 100
            c_avg = sum(decades[d]["contracting"]) / len(decades[d]["contracting"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "expanding_return": round(e_avg, 1),
                "contracting_return": round(c_avg, 1),
                "spread": round(e_avg - c_avg, 1),
                "spy_return": round(spy_avg, 1),
            })

    # Aggregate
    spread = results["expanding"]["cagr"] - results["contracting"]["cagr"]
    exp_vs_spy = results["expanding"]["cagr"] - results["spy"]["cagr"]

    avg_exp = sum(p["expanding_count"] for p in valid) / n
    avg_stb = sum(p["stable_count"] for p in valid) / n
    avg_con = sum(p["contracting_count"] for p in valid) / n
    avg_consec = sum(p["consecutive_count"] for p in valid) / n
    avg_total = sum(p["total_scored"] for p in valid) / n

    exp_cash = sum(1 for p in valid if p["expanding_count"] == 0)
    con_cash = sum(1 for p in valid if p["contracting_count"] == 0)
    consec_cash = sum(1 for p in valid if p["consecutive_count"] == 0)

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight (top 30 expanders)",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "sector_exclusions": "Financial Services",
        "filing_lag": f"{FILING_LAG_DAYS} days",
        "signal": "OPM expansion = current FY OPM - avg(prior 3 FY OPMs)",
        "expansion_threshold": f"+/-{EXPANSION_THRESHOLD*100:.0f}pp",
        "portfolios": {
            "expanding": results["expanding"],
            "consecutive": results["consecutive"],
            "stable": results["stable"],
            "contracting": results["contracting"],
            "sp500": results["spy"],
        },
        "spread_cagr": round(spread, 2),
        "expanding_vs_spy": round(exp_vs_spy, 2),
        "avg_stock_counts": {
            "expanding": round(avg_exp, 0),
            "stable": round(avg_stb, 0),
            "contracting": round(avg_con, 0),
            "consecutive": round(avg_consec, 0),
            "total_scored": round(avg_total, 0),
        },
        "cash_periods": {
            "expanding": exp_cash,
            "contracting": con_cash,
            "consecutive": consec_cash,
        },
        "decade_breakdown": decade_results,
    }

    if "expanding_vs_spy" in results:
        output["expanding_vs_spy_detail"] = results["expanding_vs_spy"]
    if "consecutive_vs_spy" in results:
        output["consecutive_vs_spy_detail"] = results["consecutive_vs_spy"]

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "expanding": round(p["expanding_return"] * 100, 2),
            "stable": round(p["stable_return"] * 100, 2),
            "contracting": round(p["contracting_return"] * 100, 2),
            "consecutive": round(p["consecutive_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "expanding_count": p["expanding_count"],
            "stable_count": p["stable_count"],
            "contracting_count": p["contracting_count"],
            "consecutive_count": p["consecutive_count"],
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
    print(f"MARGIN EXPANSION BACKTEST: {m['universe']}")
    print("=" * 95)
    print(f"Period: {m['period']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print(f"Excluded: {m['sector_exclusions']}")
    print(f"Signal: {m['signal']}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: Exp={counts['expanding']:.0f}, Stb={counts['stable']:.0f}, "
          f"Con={counts['contracting']:.0f}, Consec={counts['consecutive']:.0f}, "
          f"Total={counts['total_scored']:.0f}")
    cash = m["cash_periods"]
    if cash["expanding"] > 0 or cash["contracting"] > 0 or cash["consecutive"] > 0:
        print(f"Cash periods: Exp={cash['expanding']}, Con={cash['contracting']}, "
              f"Consec={cash['consecutive']}")
    print("-" * 95)

    header = (f"{'Portfolio':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 95)

    for name, label in [("expanding", "Expanding (>+1pp)"),
                         ("consecutive", "Consecutive Exp"),
                         ("stable", "Stable (-1 to +1pp)"),
                         ("contracting", "Contracting (<-1pp)"),
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

    print(f"\nExpanding-Contracting spread: {m['spread_cagr']:.1f}% per year")
    print(f"Expanding vs SPY: {m['expanding_vs_spy']:+.1f}%")

    evs = m.get("expanding_vs_spy_detail")
    if evs:
        print(f"\nExpanding vs S&P 500:")
        print(f"  Excess CAGR: {evs['excess_cagr']:+.2f}%")
        if evs.get('information_ratio') is not None:
            print(f"  Information Ratio: {evs['information_ratio']:.3f}")
        if evs.get('up_capture') is not None:
            print(f"  Up Capture: {evs['up_capture']:.1f}%  |  "
                  f"Down Capture: {evs['down_capture']:.1f}%")
        if evs.get('beta') is not None:
            print(f"  Beta: {evs['beta']:.3f}  |  Alpha: {evs['alpha']:+.2f}%")

    cvs = m.get("consecutive_vs_spy_detail")
    if cvs:
        print(f"\nConsecutive Expanding vs S&P 500:")
        print(f"  Excess CAGR: {cvs['excess_cagr']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Expanding':>12} {'Contracting':>12} "
              f"{'Spread':>10} {'SPY':>10}")
        print("-" * 60)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['expanding_return']:>11.1f}% "
                  f"{d['contracting_return']:>11.1f}% "
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
    exec_model = "same-day close" if args.no_next_day else "next-day open (MOC)"
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)

    signal_desc = (f"OPM expansion vs 3yr avg (>{EXPANSION_THRESHOLD*100:.0f}pp / "
                   f"<{CONTRACTION_THRESHOLD*100:.0f}pp), "
                   f"MCap > {mktcap_label} local, excl. financials")
    print_header("MARGIN EXPANSION BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  Portfolios: Expanding (>{EXPANSION_THRESHOLD*100:.0f}pp), "
          f"Stable, Contracting (<{CONTRACTION_THRESHOLD*100:.0f}pp), "
          f"Consecutive (2+ yr improving)")
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
    output["benchmark"] = f"{benchmark_name} ({benchmark_symbol})"
    output["execution"] = exec_model
    print_summary(output)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s "
          f"(fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Margin Expansion backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "margin-expansion", args_str=" ".join(cloud_args),
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
        print("\n" + "=" * 110)
        print("EXCHANGE COMPARISON SUMMARY")
        print("=" * 110)
        print(f"{'Exchange':<16} {'Exp CAGR':>10} {'Con CAGR':>10} "
              f"{'Spread':>10} {'Consec':>10} {'SPY CAGR':>10} "
              f"{'Avg Exp':>10} {'Avg Con':>10}")
        print("-" * 110)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>10}  {r.get('error', '')[:50]}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['expanding']['cagr']:>9.1f}% "
                  f"{p['contracting']['cagr']:>9.1f}% "
                  f"{r['spread_cagr']:>+9.1f}% "
                  f"{p['consecutive']['cagr']:>9.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{avg['expanding']:>10.0f} "
                  f"{avg['contracting']:>10.0f}")
        print("=" * 110)

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
