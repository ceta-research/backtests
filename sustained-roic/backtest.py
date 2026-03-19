#!/usr/bin/env python3
"""
Sustained High ROIC Backtest

Annual rebalancing (April), equal weight, three portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: ROIC computed from raw FY financial statements over 5-year lookback.
  ROIC = NOPAT / Invested Capital
  NOPAT = Operating Income * (1 - effective tax rate)
  Invested Capital = Total Assets - Current Liabilities - Cash

Portfolios:
  Sustained: ROIC > 12% in 3+ of last 5 FY periods
  Single-year: Current ROIC > 12% but < 3 qualifying years
  Low: Current ROIC <= 12%

Benchmark: S&P 500 (SPY).

Usage:
    python3 sustained-roic/backtest.py
    python3 sustained-roic/backtest.py --preset india --verbose
    python3 sustained-roic/backtest.py --global --output results/exchange_comparison.json
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
ROIC_THRESHOLD = 0.12          # 12% ROIC threshold
SUSTAINED_MIN_YEARS = 3        # Minimum years above threshold out of lookback window
LOOKBACK_YEARS = 5             # Number of FY periods to look back
FILING_LAG_DAYS = 45           # Point-in-time data lag
START_YEAR = 2000
END_YEAR = 2025

# Exchanges to test in --global mode
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

    # 2-4: Financial data
    queries = [
        ("income_cache", f"""
            SELECT symbol,
                operatingIncome, incomeTaxExpense, incomeBeforeTax,
                dateEpoch as filing_epoch
            FROM income_statement
            WHERE period = 'FY'
              AND operatingIncome IS NOT NULL
              AND incomeBeforeTax IS NOT NULL
              AND {sym_filter_sql}
        """, "income statements"),
        ("balance_cache", f"""
            SELECT symbol,
                totalAssets, totalCurrentLiabilities,
                cashAndCashEquivalents,
                dateEpoch as filing_epoch
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalAssets IS NOT NULL
              AND totalCurrentLiabilities IS NOT NULL
              AND {sym_filter_sql}
        """, "balance sheets"),
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


def classify_stocks(con, target_date, mktcap_min):
    """Classify stocks by sustained ROIC at a rebalance date.

    Returns dict: {symbol: (current_roic, years_above, group, market_cap)}
    where group is 'sustained', 'single_year', or 'low'
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        -- Get most recent FY filings per symbol, up to 5 years back
        inc AS (
            SELECT symbol, operatingIncome, incomeTaxExpense, incomeBeforeTax,
                   filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS yr_rank
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol, filing_epoch ORDER BY filing_epoch DESC
                ) AS dedup_rn
                FROM income_cache WHERE filing_epoch <= ?
            ) WHERE dedup_rn = 1
        ),
        bs AS (
            SELECT symbol, totalAssets, totalCurrentLiabilities,
                   cashAndCashEquivalents, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS yr_rank
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol, filing_epoch ORDER BY filing_epoch DESC
                ) AS dedup_rn
                FROM balance_cache WHERE filing_epoch <= ?
            ) WHERE dedup_rn = 1
        ),
        met AS (
            SELECT symbol, marketCap,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol, filing_epoch ORDER BY filing_epoch DESC
                ) AS dedup_rn
                FROM metrics_cache WHERE filing_epoch <= ?
            ) WHERE dedup_rn = 1
        ),
        -- Compute ROIC for each of the last N years
        roic_by_year AS (
            SELECT
                inc.symbol,
                inc.yr_rank,
                CASE
                    WHEN bs.totalAssets - bs.totalCurrentLiabilities
                         - COALESCE(bs.cashAndCashEquivalents, 0) > 0
                    THEN (inc.operatingIncome * (1.0 - COALESCE(
                            inc.incomeTaxExpense / NULLIF(inc.incomeBeforeTax, 0),
                            0.25)))
                         / (bs.totalAssets - bs.totalCurrentLiabilities
                            - COALESCE(bs.cashAndCashEquivalents, 0))
                    ELSE NULL
                END AS roic
            FROM inc
            JOIN bs ON inc.symbol = bs.symbol AND inc.yr_rank = bs.yr_rank
            WHERE inc.yr_rank <= ?
        ),
        -- Summarize ROIC history per symbol
        roic_summary AS (
            SELECT
                symbol,
                COUNT(*) AS years_available,
                SUM(CASE WHEN roic > ? THEN 1 ELSE 0 END) AS years_above,
                MAX(CASE WHEN yr_rank = 1 THEN roic ELSE NULL END) AS current_roic
            FROM roic_by_year
            WHERE roic IS NOT NULL
            GROUP BY symbol
            HAVING COUNT(*) >= 3  -- Need at least 3 years of data
        )
        SELECT
            rs.symbol,
            rs.current_roic,
            rs.years_above,
            rs.years_available,
            m.marketCap
        FROM roic_summary rs
        JOIN met m ON rs.symbol = m.symbol AND m.rn = 1
        JOIN universe u ON rs.symbol = u.symbol
        WHERE m.marketCap > ?
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch,
          LOOKBACK_YEARS, ROIC_THRESHOLD, mktcap_min]).fetchall()

    result = {}
    for sym, current_roic, years_above, years_available, mcap in rows:
        if years_above >= SUSTAINED_MIN_YEARS:
            group = "sustained"
        elif current_roic is not None and current_roic > ROIC_THRESHOLD:
            group = "single_year"
        else:
            group = "low"
        result[sym] = (current_roic, years_above, group, mcap)
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
        portfolio: dict {symbol: (current_roic, years_above, group, market_cap)}

    Returns:
        tuple (mean_return, count, skipped_count)
    """
    if not portfolio:
        return 0.0, 0, 0

    symbol_returns = []
    for sym, (roic, yrs, group, mcap) in portfolio.items():
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
    """Run the full Sustained ROIC backtest with three portfolio tracks."""
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
        sustained = {s: v for s, v in classified.items() if v[2] == "sustained"}
        single_year = {s: v for s, v in classified.items() if v[2] == "single_year"}
        low = {s: v for s, v in classified.items() if v[2] == "low"}

        # Compute returns for each track
        track_data = {}
        for name, portfolio in [("sustained", sustained),
                                ("single_year", single_year),
                                ("low", low)]:
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
            "sustained_return": track_data["sustained"]["return"],
            "single_year_return": track_data["single_year"]["return"],
            "low_return": track_data["low"]["return"],
            "spy_return": spy_ret,
            "sustained_count": track_data["sustained"]["count"],
            "single_year_count": track_data["single_year"]["count"],
            "low_count": track_data["low"]["count"],
            "total_count": sum(
                track_data[t]["count"] for t in ["sustained", "single_year", "low"]
            ),
        })

        if verbose:
            s = track_data
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"Sustained={s['sustained']['return']*100:+.1f}% ({s['sustained']['count']}), "
                  f"Single={s['single_year']['return']*100:+.1f}% ({s['single_year']['count']}), "
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
    for track, key in [("sustained", "sustained_return"),
                       ("single_year", "single_year_return"),
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

        # Comparison metrics for sustained track
        if track == "sustained":
            c = m["comparison"]
            results["sustained_vs_spy"] = {
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
            decades[d] = {"sustained": [], "single_year": [], "low": [], "spy": []}
        decades[d]["sustained"].append(p["sustained_return"])
        decades[d]["single_year"].append(p["single_year_return"])
        decades[d]["low"].append(p["low_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            s_avg = sum(decades[d]["sustained"]) / len(decades[d]["sustained"]) * 100
            sy_avg = sum(decades[d]["single_year"]) / len(decades[d]["single_year"]) * 100
            l_avg = sum(decades[d]["low"]) / len(decades[d]["low"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "sustained_return": round(s_avg, 1),
                "single_year_return": round(sy_avg, 1),
                "low_return": round(l_avg, 1),
                "spy_return": round(spy_avg, 1),
                "sustained_vs_spy": round(s_avg - spy_avg, 1),
            })

    # Aggregate metrics
    sustained_excess = results["sustained"]["cagr"] - results["spy"]["cagr"]
    persistence_alpha = results["sustained"]["cagr"] - results["single_year"]["cagr"]

    avg_sustained = sum(p["sustained_count"] for p in valid) / n
    avg_single = sum(p["single_year_count"] for p in valid) / n
    avg_low = sum(p["low_count"] for p in valid) / n
    avg_total = sum(p["total_count"] for p in valid) / n

    sustained_cash = sum(1 for p in valid if p["sustained_count"] == 0)

    # Sustained pool percentage (what % of universe qualifies)
    sustained_pcts = []
    for p in valid:
        if p["total_count"] > 0:
            sustained_pcts.append(p["sustained_count"] / p["total_count"] * 100)

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "signal": f"ROIC > {ROIC_THRESHOLD*100:.0f}% in {SUSTAINED_MIN_YEARS}+ of last {LOOKBACK_YEARS} FY",
        "roic_formula": "NOPAT / Invested Capital",
        "portfolios": {
            "sustained": results["sustained"],
            "single_year": results["single_year"],
            "low": results["low"],
            "sp500": results["spy"],
        },
        "sustained_excess_cagr": round(sustained_excess, 2),
        "persistence_alpha": round(persistence_alpha, 2),
        "avg_stock_counts": {
            "sustained": round(avg_sustained, 0),
            "single_year": round(avg_single, 0),
            "low": round(avg_low, 0),
            "total": round(avg_total, 0),
        },
        "cash_periods": {"sustained": sustained_cash},
        "avg_sustained_pct": round(
            sum(sustained_pcts) / len(sustained_pcts), 1
        ) if sustained_pcts else 0,
        "decade_breakdown": decade_results,
    }

    if "sustained_vs_spy" in results:
        output["sustained_vs_spy"] = results["sustained_vs_spy"]

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "sustained": round(p["sustained_return"] * 100, 2),
            "single_year": round(p["single_year_return"] * 100, 2),
            "low": round(p["low_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "sustained_count": p["sustained_count"],
            "single_year_count": p["single_year_count"],
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
    print(f"SUSTAINED ROIC BACKTEST: {m['universe']}")
    print("=" * 95)
    print(f"Period: {m['period']}")
    print(f"Signal: {m['signal']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: Sustained={counts['sustained']:.0f}, "
          f"Single-year={counts['single_year']:.0f}, "
          f"Low={counts['low']:.0f}, Total={counts['total']:.0f}")
    print(f"Sustained pool: ~{m['avg_sustained_pct']:.0f}% of universe")
    cash = m["cash_periods"]
    if cash["sustained"] > 0:
        print(f"Cash periods (sustained): {cash['sustained']}")
    print("-" * 95)

    header = (f"{'Portfolio':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 95)

    for name, label in [("sustained", "Sustained (3+/5yr)"),
                         ("single_year", "Single-year ROIC"),
                         ("low", "Low ROIC"),
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

    print(f"\nSustained vs SPY excess: {m['sustained_excess_cagr']:+.1f}% per year")
    print(f"Persistence alpha (sustained vs single-year): "
          f"{m['persistence_alpha']:+.1f}% per year")

    # Sustained vs SPY comparison
    svs = m.get("sustained_vs_spy")
    if svs:
        print(f"\nSustained vs S&P 500:")
        print(f"  Excess CAGR: {svs['excess_cagr']:+.2f}%")
        if svs.get('win_rate') is not None:
            print(f"  Win Rate: {svs['win_rate']:.1f}%")
        if svs.get('information_ratio') is not None:
            print(f"  Information Ratio: {svs['information_ratio']:.3f}")
        if svs.get('up_capture') is not None:
            print(f"  Up Capture: {svs['up_capture']:.1f}%  |  "
                  f"Down Capture: {svs['down_capture']:.1f}%")
        if svs.get('beta') is not None:
            print(f"  Beta: {svs['beta']:.3f}  |  Alpha: {svs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Sustained':>10} {'Single-yr':>10} "
              f"{'Low':>10} {'SPY':>10} {'vs SPY':>10}")
        print("-" * 65)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['sustained_return']:>9.1f}% "
                  f"{d['single_year_return']:>9.1f}% "
                  f"{d['low_return']:>9.1f}% {d['spy_return']:>9.1f}% "
                  f"{d['sustained_vs_spy']:>+9.1f}%")

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

    signal_desc = (f"ROIC > {ROIC_THRESHOLD*100:.0f}% in {SUSTAINED_MIN_YEARS}+/"
                   f"{LOOKBACK_YEARS}yr, MCap > {mktcap_label} local")
    print_header("SUSTAINED HIGH ROIC BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  ROIC = NOPAT / Invested Capital")
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
        description="Sustained High ROIC backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "sustained-roic", args_str=" ".join(cloud_args),
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
        print("\n" + "=" * 95)
        print("EXCHANGE COMPARISON SUMMARY")
        print("=" * 95)
        print(f"{'Exchange':<16} {'Sust CAGR':>10} {'Single':>10} "
              f"{'Low':>10} {'SPY CAGR':>10} {'Excess':>10} "
              f"{'Avg Sust':>10}")
        print("-" * 80)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>10}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['sustained']['cagr']:>9.1f}% "
                  f"{p['single_year']['cagr']:>9.1f}% "
                  f"{p['low']['cagr']:>9.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{r['sustained_excess_cagr']:>+9.1f}% "
                  f"{avg['sustained']:>10.0f}")
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
