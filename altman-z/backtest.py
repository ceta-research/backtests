#!/usr/bin/env python3
"""
Altman Z-Score Safety Backtest

Annual rebalancing (April), equal weight, four portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: Altman Z-Score computed from raw FY financial statements.
  Z = 1.2*(WC/TA) + 1.4*(RE/TA) + 3.3*(EBITDA/TA) + 0.6*(MktCap/TL) + 1.0*(Rev/TA)

Zones:
  Safe (Z > 2.99): Low bankruptcy probability
  Gray (1.81-2.99): Uncertain
  Distress (Z < 1.81): High bankruptcy probability

Universe: Non-financial, non-utility stocks above exchange-specific market cap threshold.
Portfolios: Safe, Gray, Distress, All-ex-Distress.
Benchmark: S&P 500 (SPY).

Usage:
    python3 altman-z/backtest.py
    python3 altman-z/backtest.py --preset india --verbose
    python3 altman-z/backtest.py --global --output results/exchange_comparison.json
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
Z_SAFE = 2.99
Z_DISTRESS = 1.81
FILING_LAG_DAYS = 45
EXCLUDED_SECTORS = ("Financial Services", "Utilities")
START_YEAR = 2000
END_YEAR = 2025

# Exchanges to test in --global mode (tier 1 + tier 2)
GLOBAL_PRESETS = [
    "us", "india", "germany", "china", "hongkong", "canada",
    "uk", "switzerland", "sweden", "korea", "brazil",
    "taiwan", "singapore", "southafrica", "france",
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all historical financial data and load into DuckDB.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        balance_cache(symbol, totalCurrentAssets, totalCurrentLiabilities,
                      totalAssets, retainedEarnings, totalLiabilities, filing_epoch)
        income_cache(symbol, ebitda, revenue, filing_epoch)
        metrics_cache(symbol, marketCap, filing_epoch)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns a DuckDB connection or None.
    """
    # Build exchange and sector filter for API queries
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

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe (with sector info for filtering)
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange, sector FROM profile {profile_where}"
    profiles = client.query(profile_sql, verbose=verbose, timeout=120)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None

    # Filter out financials and utilities locally
    filtered = [p for p in profiles if p.get("sector") not in EXCLUDED_SECTORS]
    excluded_count = len(profiles) - len(filtered)
    print(f"  Universe: {len(profiles)} symbols, {excluded_count} financials/utilities excluded, "
          f"{len(filtered)} remaining")

    if not filtered:
        print("  No non-financial/utility symbols found.")
        return None

    # Create universe table (sector-filtered symbols only)
    sym_values = ",".join(f"('{r['symbol']}')" for r in filtered)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2-4: Financial data (all via parquet for speed)
    queries = [
        ("balance_cache", f"""
            SELECT symbol,
                totalCurrentAssets, totalCurrentLiabilities,
                totalAssets, retainedEarnings, totalLiabilities,
                dateEpoch as filing_epoch
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalAssets IS NOT NULL AND totalAssets > 0
              AND totalLiabilities IS NOT NULL AND totalLiabilities > 0
              AND {sym_filter_sql}
        """, "balance sheets"),
        ("income_cache", f"""
            SELECT symbol, ebitda, revenue,
                dateEpoch as filing_epoch
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL
              AND {sym_filter_sql}
        """, "income statements"),
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
                              memory_mb=16384, threads=6)
        print(f"    -> {count:,} rows")

    # 5. Prices (only at rebalance dates + 10-day windows)
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
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
                    AND {sym_filter_sql}
                INTERSECT
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
                    AND {sym_filter_sql}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=16384, threads=6)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count:,} price rows")

    return con


def compute_z_scores(con, target_date, mktcap_min):
    """Compute Altman Z-Scores for all qualifying stocks at a rebalance date.

    Returns dict: {symbol: (z_score, zone, market_cap)}
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        bs AS (
            SELECT symbol, totalCurrentAssets, totalCurrentLiabilities,
                   totalAssets, retainedEarnings, totalLiabilities, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ?
        ),
        inc AS (
            SELECT symbol, ebitda, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        z_raw AS (
            SELECT
                bs.symbol,
                1.2 * (bs.totalCurrentAssets - bs.totalCurrentLiabilities)
                    / NULLIF(bs.totalAssets, 0)
              + 1.4 * COALESCE(bs.retainedEarnings, 0)
                    / NULLIF(bs.totalAssets, 0)
              + 3.3 * COALESCE(inc.ebitda, 0)
                    / NULLIF(bs.totalAssets, 0)
              + 0.6 * met.marketCap
                    / NULLIF(bs.totalLiabilities, 0)
              + 1.0 * inc.revenue
                    / NULLIF(bs.totalAssets, 0)
                AS z_score,
                met.marketCap
            FROM bs
            JOIN inc ON bs.symbol = inc.symbol AND inc.rn = 1
            JOIN met ON bs.symbol = met.symbol AND met.rn = 1
            JOIN universe u ON bs.symbol = u.symbol
            WHERE bs.rn = 1
              AND met.marketCap > ?
        )
        SELECT symbol, z_score, marketCap
        FROM z_raw
        WHERE z_score IS NOT NULL
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, mktcap_min]).fetchall()

    result = {}
    for sym, z, mcap in rows:
        if z > Z_SAFE:
            zone = "safe"
        elif z >= Z_DISTRESS:
            zone = "gray"
        else:
            zone = "distress"
        result[sym] = (z, zone, mcap)
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
    """Compute equal-weighted return for a portfolio of stocks.

    Args:
        portfolio: dict {symbol: (z_score, zone, market_cap)}

    Returns:
        tuple (mean_return, count, skipped_count)
    """
    if not portfolio:
        return 0.0, 0, 0

    symbol_returns = []
    for sym, (z, zone, mcap) in portfolio.items():
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
    """Run the full Altman Z-Score backtest with four portfolio tracks."""
    print(f"Phase 2: Running annual backtest "
          f"({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")
    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        scored = compute_z_scores(con, entry_date, mktcap_min)
        if not scored:
            if verbose:
                print(f"  {entry_date.year}: No scored stocks found, skipping")
            continue

        # Split into zones
        safe = {s: v for s, v in scored.items() if v[1] == "safe"}
        gray = {s: v for s, v in scored.items() if v[1] == "gray"}
        distress = {s: v for s, v in scored.items() if v[1] == "distress"}
        all_ex_distress = {s: v for s, v in scored.items() if v[1] != "distress"}

        # Compute returns for each track
        track_data = {}
        for name, portfolio in [("safe", safe), ("gray", gray),
                                ("distress", distress),
                                ("all_ex_distress", all_ex_distress)]:
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
            "safe_return": track_data["safe"]["return"],
            "gray_return": track_data["gray"]["return"],
            "distress_return": track_data["distress"]["return"],
            "all_ex_distress_return": track_data["all_ex_distress"]["return"],
            "spy_return": spy_ret,
            "safe_count": track_data["safe"]["count"],
            "gray_count": track_data["gray"]["count"],
            "distress_count": track_data["distress"]["count"],
            "total_count": sum(
                track_data[t]["count"] for t in ["safe", "gray", "distress"]
            ),
        })

        if verbose:
            s = track_data
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"Safe={s['safe']['return']*100:+.1f}% ({s['safe']['count']}), "
                  f"Gray={s['gray']['return']*100:+.1f}% ({s['gray']['count']}), "
                  f"Distress={s['distress']['return']*100:+.1f}% ({s['distress']['count']}), "
                  f"ExDist={s['all_ex_distress']['return']*100:+.1f}%, "
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
    for track, key in [("safe", "safe_return"), ("gray", "gray_return"),
                       ("distress", "distress_return"),
                       ("all_ex_distress", "all_ex_distress_return"),
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

        # Comparison metrics for safe-zone and all-ex-distress
        if track in ("safe", "all_ex_distress"):
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
            decades[d] = {"safe": [], "distress": [], "spy": []}
        decades[d]["safe"].append(p["safe_return"])
        decades[d]["distress"].append(p["distress_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            s_avg = sum(decades[d]["safe"]) / len(decades[d]["safe"]) * 100
            d_avg = sum(decades[d]["distress"]) / len(decades[d]["distress"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "safe_return": round(s_avg, 1),
                "distress_return": round(d_avg, 1),
                "spread": round(s_avg - d_avg, 1),
                "spy_return": round(spy_avg, 1),
            })

    # Aggregate metrics
    safe_distress_spread = results["safe"]["cagr"] - results["distress"]["cagr"]
    avoidance_alpha = results["all_ex_distress"]["cagr"] - results["spy"]["cagr"]

    avg_safe = sum(p["safe_count"] for p in valid) / n
    avg_gray = sum(p["gray_count"] for p in valid) / n
    avg_distress = sum(p["distress_count"] for p in valid) / n
    avg_total = sum(p["total_count"] for p in valid) / n

    safe_cash = sum(1 for p in valid if p["safe_count"] == 0)
    distress_cash = sum(1 for p in valid if p["distress_count"] == 0)

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "sector_exclusions": "Financial Services, Utilities",
        "z_score_thresholds": {"safe": f"> {Z_SAFE}", "distress": f"< {Z_DISTRESS}"},
        "portfolios": {
            "safe_zone": results["safe"],
            "gray_zone": results["gray"],
            "distress_zone": results["distress"],
            "all_ex_distress": results["all_ex_distress"],
            "sp500": results["spy"],
        },
        "spread_cagr": round(safe_distress_spread, 2),
        "avoidance_alpha": round(avoidance_alpha, 2),
        "avg_stock_counts": {
            "safe": round(avg_safe, 0),
            "gray": round(avg_gray, 0),
            "distress": round(avg_distress, 0),
            "total": round(avg_total, 0),
        },
        "cash_periods": {
            "safe": safe_cash,
            "distress": distress_cash,
        },
        "decade_breakdown": decade_results,
    }

    if "safe_vs_spy" in results:
        output["safe_vs_spy"] = results["safe_vs_spy"]
    if "all_ex_distress_vs_spy" in results:
        output["all_ex_distress_vs_spy"] = results["all_ex_distress_vs_spy"]

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "safe": round(p["safe_return"] * 100, 2),
            "gray": round(p["gray_return"] * 100, 2),
            "distress": round(p["distress_return"] * 100, 2),
            "all_ex_distress": round(p["all_ex_distress_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "safe_count": p["safe_count"],
            "gray_count": p["gray_count"],
            "distress_count": p["distress_count"],
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
    print(f"ALTMAN Z-SCORE BACKTEST: {m['universe']}")
    print("=" * 95)
    print(f"Period: {m['period']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print(f"Excluded: {m['sector_exclusions']}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: Safe={counts['safe']:.0f}, Gray={counts['gray']:.0f}, "
          f"Distress={counts['distress']:.0f}, Total={counts['total']:.0f}")
    cash = m["cash_periods"]
    if cash["safe"] > 0 or cash["distress"] > 0:
        print(f"Cash periods: Safe={cash['safe']}, Distress={cash['distress']}")
    print("-" * 95)

    header = (f"{'Portfolio':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 95)

    for name, label in [("safe_zone", "Safe (Z>2.99)"),
                         ("all_ex_distress", "All ex-Distress"),
                         ("gray_zone", "Gray (1.81-2.99)"),
                         ("distress_zone", "Distress (Z<1.81)"),
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

    print(f"\nSafe-Distress spread: {m['spread_cagr']:.1f}% per year")
    print(f"Avoidance alpha (all-ex-distress vs SPY): {m['avoidance_alpha']:+.1f}%")

    # Safe vs SPY comparison
    svs = m.get("safe_vs_spy")
    if svs:
        print(f"\nSafe Zone vs S&P 500:")
        print(f"  Excess CAGR: {svs['excess_cagr']:+.2f}%")
        if svs.get('information_ratio') is not None:
            print(f"  Information Ratio: {svs['information_ratio']:.3f}")
        if svs.get('up_capture') is not None:
            print(f"  Up Capture: {svs['up_capture']:.1f}%  |  "
                  f"Down Capture: {svs['down_capture']:.1f}%")
        if svs.get('beta') is not None:
            print(f"  Beta: {svs['beta']:.3f}  |  Alpha: {svs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Safe':>10} {'Distress':>10} "
              f"{'Spread':>10} {'SPY':>10}")
        print("-" * 55)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['safe_return']:>9.1f}% "
                  f"{d['distress_return']:>9.1f}% "
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

    signal_desc = (f"Z-Score zones, MCap > {mktcap_label} local, "
                   f"excl. financials/utilities")
    print_header("ALTMAN Z-SCORE SAFETY BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  Portfolios: Safe (Z>{Z_SAFE}), Gray ({Z_DISTRESS}-{Z_SAFE}), "
          f"Distress (Z<{Z_DISTRESS})")
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
        description="Altman Z-Score Safety backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "altman-z", args_str=" ".join(cloud_args),
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
                all_results[preset["name"]] = {
                    "universe": preset["name"],
                    "error": str(e),
                }

        # Print comparison summary
        print("\n" + "=" * 95)
        print("EXCHANGE COMPARISON SUMMARY")
        print("=" * 95)
        print(f"{'Exchange':<16} {'Safe CAGR':>10} {'Distress':>10} "
              f"{'Spread':>10} {'ExDist CAGR':>12} {'SPY CAGR':>10} "
              f"{'Avg Safe':>10}")
        print("-" * 80)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>10}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['safe_zone']['cagr']:>9.1f}% "
                  f"{p['distress_zone']['cagr']:>9.1f}% "
                  f"{r['spread_cagr']:>+9.1f}% "
                  f"{p['all_ex_distress']['cagr']:>11.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{avg['safe']:>10.0f}")
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
