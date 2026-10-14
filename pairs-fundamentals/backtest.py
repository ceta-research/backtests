#!/usr/bin/env python3
"""
Pairs Trading Backtest (Fundamental Pairs)

Annual pair formation using 252-day returns correlation within sectors.
Top 20 pairs per exchange (same sector, corr > 0.70). Spread z-score entry
filter: only trade pairs where spread is extended (|z| > 1.5) at formation end.
Equal-dollar weighting across active pairs. Annual rebalance.

Universe (pairs_universe.py): for each trading year, the lines listed and traded
through the formation year before it, one per security and per company, the top
30 of each sector by that year's median daily traded value in USD. No size
look-ahead. A line needs an FY filing of any date, a later one included: that
test is not point-in-time. Survivorship is reduced, not removed: the price data
holds almost no name that stopped trading before 2015 (US) or 2020 (elsewhere).

Return model:
    - Spread = log(P_A) - beta * log(P_B), beta estimated by OLS on log-prices
    - z = the spread on the pair's last common bar of the formation year against
      that year's mean and std; the trade is entered on the next bar
    - If z > 0: A overvalued vs B → Short A, Long B
    - Pair return = -sign(z) * (Return_A - Return_B) / 2  (equal-dollar), each
      leg's gain capped at 200%, less one size-tiered cost per leg for the round trip
    - Portfolio return = mean(pair returns), 0% if < MIN_PAIRS_ACTIVE pairs entered
    - Exits (--exit-method): a pair without a year-end price on both legs is
      dropped (drop) or closes at the last bar both legs traded (ltp)

Academic reference:
    Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading: Performance
    of a Relative-Value Arbitrage Rule." Review of Financial Studies, 19(3), 797-827.
    GGR found ~11% annual excess return (1962-2002). Do & Faff (2010, 2012) showed
    declining profitability post-2002 as strategy became crowded.

Usage:
    python3 pairs-fundamentals/backtest.py                              # US default
    python3 pairs-fundamentals/backtest.py --preset india
    python3 pairs-fundamentals/backtest.py --global \\
        --output results/exchange_comparison.json --verbose
"""

import argparse
import duckdb
import json
import math
import os
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import (get_local_benchmark, get_prices, LOCAL_INDEX_BENCHMARKS,
                        add_exit_method_arg, campaign_listing_label)
from metrics import (compute_metrics, compute_annual_returns, format_metrics,
                     period_accounting)
from costs import tiered_cost
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate)
from pairs_universe import (UNIVERSE_NOTES, PairLtp, build_candidates, check_pairs,
                            fetch_prices, fetch_universe, load_delist_bounds)

# ─── Strategy Parameters ──────────────────────────────────────────────────────
TOP_N_PER_SECTOR = 30      # Candidate stocks per sector (ranked by formation-year traded value)
MIN_CORR = 0.70            # Minimum 252-day returns-based correlation
MIN_CORR_DAYS = 200        # Minimum common trading days for pair eligibility (and formation-year bars)
MIN_CANDIDATES = 20        # Leg skipped when no trading year has this many candidates
MAX_PAIRS = 20             # Maximum pairs to hold per year
Z_ENTRY = 1.5              # Minimum |z-score| at formation end to enter a trade
MIN_PAIRS_ACTIVE = 3       # Hold cash if fewer pairs meet the entry condition
MAX_SINGLE_RETURN = 2.0    # Cap on a leg's return (data quality guard)
MIN_LEG_PRICE = 1.0        # Skip pairs where either leg price < $1 (artifacts)
START_YEAR = 2005
END_YEAR = 2024


def fetch_data_via_api(cr, exchanges, verbose=False, offset_days=1):
    """Build the per-year candidate universe (pairs_universe) and fetch daily prices into DuckDB.

    The connection holds pairs_universe's tables, among them:
        candidates(yr, symbol, sector, company, size_usd, cap_usd)   one list per trading year
        prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)

    Returns (connection, {year: build_candidates' counts}) or None.
    """
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # ── Step 1: Per-year candidates (top N per sector by formation-year traded value) ──
    years = range(START_YEAR, END_YEAR + 1)
    if fetch_universe(cr, con, exchanges, years, verbose=verbose) is None:
        return None
    per_year = build_candidates(con, years, offset_days, MIN_LEG_PRICE,
                                TOP_N_PER_SECTOR, MIN_CORR_DAYS)
    sizes = [c["candidates"] for c in per_year.values()]
    print(f"  Candidates: {min(sizes)}-{max(sizes)} stocks a year "
          f"(top {TOP_N_PER_SECTOR}/sector by formation-year traded value)")

    if max(sizes) < MIN_CANDIDATES:
        print("  Too few candidates for pairs trading.")
        return None

    # ── Step 2: Fetch daily prices (2004 onward) for candidates + benchmarks ──
    print("  Fetching daily prices (2004-present)...")
    bench_symbols = {"SPY"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(sym)
    count = fetch_prices(cr, con, None, bench_symbols, verbose=verbose)
    if count == 0:
        print("  No price data found.")
        return None

    print(f"  Price rows: {count:,}")
    return con, per_year


def compute_correlations(con, formation_start, formation_end):
    """Compute same-sector pairwise 252-day returns correlations via DuckDB.

    Uses a self-join on daily returns within the formation period. Both legs are
    candidates of the trading year after it.
    Filters to MIN_CORR_DAYS common trading days and MIN_CORR threshold.

    Returns list of (sym_a, sym_b, sector, correlation, common_days).
    """
    fs = formation_start.isoformat()
    fe = formation_end.isoformat()
    year = formation_end.year + 1

    rows = con.execute(f"""
        WITH daily_ret AS (
            SELECT p.symbol, c.sector, p.trade_date,
                (p.adjClose - LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date, p.adjClose DESC NULLS LAST))
                    / NULLIF(LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date, p.adjClose DESC NULLS LAST), 0)
                AS ret
            FROM prices_cache p
            JOIN candidates c ON c.symbol = p.symbol AND c.yr = {year}
            WHERE p.trade_date >= '{fs}' AND p.trade_date <= '{fe}'
        ),
        pair_corr AS (
            SELECT
                a.symbol AS sym_a,
                b.symbol AS sym_b,
                a.sector AS sector,
                ROUND(CORR(a.ret, b.ret), 4) AS correlation,
                COUNT(*) AS common_days
            FROM daily_ret a
            JOIN daily_ret b
                ON a.trade_date = b.trade_date AND a.symbol < b.symbol
               AND a.sector = b.sector
            WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
            GROUP BY a.symbol, b.symbol, a.sector
            HAVING COUNT(*) >= {MIN_CORR_DAYS}
        )
        SELECT sym_a, sym_b, sector, correlation, common_days
        FROM pair_corr
        WHERE correlation >= {MIN_CORR}
          AND NOT isnan(correlation)
        ORDER BY correlation DESC, sym_a, sym_b
        LIMIT {MAX_PAIRS * 5}
    """).fetchall()

    return rows  # list of tuples: (sym_a, sym_b, sector, corr, common_days)


def compute_spread_params(con, sym_a, sym_b, formation_start, formation_end):
    """Estimate OLS beta (log-price) and spread mean/std for a pair.

    Spread = log(P_A) - beta * log(P_B)
    Beta = Cov(log_A, log_B) / Var(log_B) = CORR * STD(log_A) / STD(log_B)

    Returns (beta, spread_mean, spread_std, spread_end) or None if insufficient data.
    spread_end is the spread on the pair's last common bar of the formation period.
    """
    fs = formation_start.isoformat()
    fe = formation_end.isoformat()

    row = con.execute(f"""
        WITH log_prices AS (
            SELECT symbol, trade_date, LN(adjClose) AS lp
            FROM prices_cache
            WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'
              AND symbol IN ('{sym_a}', '{sym_b}')
              AND adjClose > 0
        ),
        paired AS (
            SELECT a.lp AS la, b.lp AS lb
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        ),
        stats AS (
            SELECT
                CORR(la, lb)  AS corr,
                STDDEV(la)    AS std_a,
                STDDEV(lb)    AS std_b,
                AVG(la)       AS mean_a,
                AVG(lb)       AS mean_b,
                COUNT(*)      AS n_obs
            FROM paired
        )
        SELECT
            corr * std_a / NULLIF(std_b, 0)              AS beta,
            mean_a - (corr * std_a / NULLIF(std_b, 0)) * mean_b AS intercept,
            n_obs
        FROM stats
    """).fetchone()

    if not row or row[0] is None or row[2] is None or row[2] < MIN_CORR_DAYS:
        return None

    beta = float(row[0])
    if not math.isfinite(beta):
        return None

    # Compute spread mean and std using the estimated beta
    spread_row = con.execute(f"""
        WITH log_prices AS (
            SELECT symbol, trade_date, LN(adjClose) AS lp
            FROM prices_cache
            WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'
              AND symbol IN ('{sym_a}', '{sym_b}')
              AND adjClose > 0
        ),
        spread AS (
            SELECT a.lp - {beta} * b.lp AS s
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        )
        SELECT AVG(s) AS mean_s, STDDEV(s) AS std_s FROM spread
    """).fetchone()

    if (not spread_row or spread_row[0] is None
            or spread_row[1] is None or float(spread_row[1]) < 1e-10):
        return None

    # Entry signal: the spread on the last bar both legs traded in the formation period
    end_row = con.execute(f"""
        SELECT LN(a.adjClose) - {beta} * LN(b.adjClose) AS s
        FROM prices_cache a
        JOIN prices_cache b
            ON a.trade_date = b.trade_date
           AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        WHERE a.trade_date >= '{fs}' AND a.trade_date <= '{fe}'
          AND a.adjClose > 0 AND b.adjClose > 0
        ORDER BY a.trade_date DESC, a.adjClose DESC NULLS LAST, b.adjClose DESC NULLS LAST
        LIMIT 1
    """).fetchone()

    return beta, float(spread_row[0]), float(spread_row[1]), float(end_row[0])


def get_price_at_date(con, symbol, target_date, window_days=10, offset_days=0):
    """First available price in [target_date+offset, target_date+offset+window]."""
    shifted = target_date + timedelta(days=offset_days)
    end_date = shifted + timedelta(days=window_days)
    row = con.execute(f"""
        SELECT adjClose FROM prices_cache
        WHERE symbol = '{symbol}'
          AND trade_date >= '{shifted.isoformat()}'
          AND trade_date <= '{end_date.isoformat()}'
          AND adjClose > 0
        ORDER BY trade_date ASC, adjClose DESC NULLS LAST LIMIT 1
    """).fetchone()
    return float(row[0]) if row else None


def run_backtest(con, use_costs=True, verbose=False, offset_days=1,
                 benchmark_symbol="SPY", ltp=None, exit_method="drop"):
    """Run annual pairs backtest (START_YEAR to END_YEAR).

    Reads con's candidates (one list per trading year) and prices_cache.
    ltp: pairs_universe.PairLtp when exit_method is not "drop".

    Returns list of annual result dicts.
    """
    results = []

    for year in range(START_YEAR, END_YEAR + 1):
        formation_start = date(year - 1, 1, 1)
        formation_end   = date(year - 1, 12, 31)
        trading_start   = date(year, 1, 1)
        trading_end     = date(year, 12, 31)

        t0 = time.time()
        corr_rows = compute_correlations(con, formation_start, formation_end)
        selected  = corr_rows[:MAX_PAIRS]
        # Two lines of one company, or a NaN correlation, stop the run.
        same_company = check_pairs(con, year, [(p[0], p[1], p[3]) for p in selected])

        if verbose:
            print(f"  {year}: {len(corr_rows)} eligible pairs → "
                  f"{len(selected)} selected  [{time.time()-t0:.1f}s corr]")

        # Benchmark annual return
        bench_start = get_price_at_date(con, benchmark_symbol, trading_start,
                                        offset_days=offset_days)
        bench_end   = get_price_at_date(con, benchmark_symbol, trading_end,
                                        window_days=15, offset_days=offset_days)
        spy_ret = ((bench_end - bench_start) / bench_start
                   if bench_start and bench_end else None)

        # Prices at trading year start (MOC: offset_days forward), with each entry bar's epoch
        entry = get_prices(con, sorted({s for p in selected for s in p[:2]}), trading_start,
                           offset_days=offset_days, with_epochs=True)

        # Pairs entered: (sym_a, sym_b, direction)
        entered = []

        for (sym_a, sym_b, sector, corr, _) in selected:
            params = compute_spread_params(con, sym_a, sym_b, formation_start, formation_end)
            if params is None:
                continue
            beta, spread_mean, spread_std, spread_end = params

            p_a0 = entry.get(sym_a, (None, None))[0]
            p_b0 = entry.get(sym_b, (None, None))[0]
            if not p_a0 or not p_b0 or p_a0 < MIN_LEG_PRICE or p_b0 < MIN_LEG_PRICE:
                continue

            # Z-score at formation end: the signal is known before the entry bar trades
            z_start = (spread_end - spread_mean) / spread_std

            # Only enter trades where spread is sufficiently extended
            if abs(z_start) < Z_ENTRY:
                continue

            # z_start > 0 → A expensive vs B → Short A, Long B
            # Convergence (A falls, B rises relative) → positive return
            entered.append((sym_a, sym_b, -1 if z_start > 0 else 1))

        # Cash is decided here, on the entry-side count, the same under every exit method.
        n_entered = len(entered)
        invested = n_entered >= MIN_PAIRS_ACTIVE

        exits, fills = {}, {}
        if invested:
            # Prices at trading year end (MOC: offset_days forward); None = no bar in the window
            exits = {s: get_price_at_date(con, s, trading_end, window_days=15,
                                          offset_days=offset_days)
                     for s in sorted({s for p in entered for s in p[:2]})}
            if ltp is not None:
                # A pair without both exit prices closes at the last bar both legs traded.
                fills = ltp.fill(year, [(p[0], p[1]) for p in entered], entry, exits,
                                 trading_end, MIN_PAIRS_ACTIVE)
        # Cost tier: each leg's USD market cap for this trading year (None: the smallest-cap tier).
        caps = dict(con.execute("SELECT symbol, cap_usd FROM candidates WHERE yr = ?",
                                [year]).fetchall())

        def book(pair_exits):
            """(pair returns, legs capped, pairs priced by a fill) of the entered pairs with both exit prices."""
            returns, capped, filled = [], 0, 0
            for (sym_a, sym_b, direction) in entered:
                # A filled pair takes the fill on both legs, never one leg's year-end price.
                p_a1, p_b1 = pair_exits.get((sym_a, sym_b)) or (exits.get(sym_a), exits.get(sym_b))
                if not p_a1 or not p_b1:
                    continue
                p_a0, p_b0 = entry[sym_a][0], entry[sym_b][0]

                ret_a = (p_a1 - p_a0) / p_a0
                ret_b = (p_b1 - p_b0) / p_b0

                # Data quality guard: a leg's gain is capped and counted (a leg cannot lose 200%)
                capped += (ret_a > MAX_SINGLE_RETURN) + (ret_b > MAX_SINGLE_RETURN)
                ret_a = min(ret_a, MAX_SINGLE_RETURN)
                ret_b = min(ret_b, MAX_SINGLE_RETURN)

                # Equal-dollar pairs return
                pair_return = direction * (ret_a - ret_b) / 2.0

                # Transaction costs: half the pair's capital sits in each leg, so a round
                # trip costs each leg's one-way rate once
                if use_costs:
                    pair_return -= tiered_cost(caps.get(sym_a)) + tiered_cost(caps.get(sym_b))

                returns.append(pair_return)
                filled += (sym_a, sym_b) in pair_exits
            return returns, capped, filled

        active_returns, legs_capped, _ = book(fills if exit_method == "ltp" else {})
        n_active = len(active_returns)
        port_return = sum(active_returns) / n_active if n_active else 0.0

        results.append({
            "year": year,
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_ret, 6) if spy_ret is not None else None,
            "pairs_formed": len(selected),
            "pairs_entered": n_entered,
            "pairs_active": n_active,
            "legs_capped": legs_capped,
            "same_company_pairs": same_company,
            "pairs": [{"a": p[0], "b": p[1], "sector": p[2], "corr": p[3]} for p in selected],
        })
        if exit_method == "both" and invested:
            r_ltp, _, n_filled = book(fills)
            results[-1].update({"portfolio_return_ltp": round(sum(r_ltp) / len(r_ltp), 6) if r_ltp else 0.0,
                                "pairs_active_ltp": len(r_ltp), "ltp_filled": n_filled})

        if verbose:
            spy_s = f"{spy_ret*100:.1f}%" if spy_ret is not None else "N/A"
            cash_marker = "" if invested else " [CASH]"
            print(f"    → {n_active}/{n_entered} entered pairs priced ({len(selected)} formed), "
                  f"port={port_return*100:.1f}%, spy={spy_s}{cash_marker}")

    return results


def build_output(m, annual, valid, results, universe_name,
                 benchmark_name="S&P 500", benchmark_symbol="SPY",
                 per_year=None, listing_filter=None, ltp_block=None):
    """Build JSON output in standard exchange_comparison format.

    per_year: build_candidates' {year: counts}, merged into each year's record.
    listing_filter: campaign_listing_label() of the leg. ltp_block: PairLtp.results_block().
    """
    p = m["portfolio"]
    b = m["benchmark"]
    c = m["comparison"]

    def pct(v):  return round(v * 100, 2) if v is not None else None
    def rnd(v, d=3): return round(v, d) if v is not None else None

    def fmt(s):
        return {
            "cagr":                  pct(s.get("cagr")),
            "max_drawdown":          pct(s.get("max_drawdown")),
            "annualized_volatility": pct(s.get("annualized_volatility")),
            "sharpe_ratio":          rnd(s.get("sharpe_ratio")),
            "sortino_ratio":         rnd(s.get("sortino_ratio")),
            "calmar_ratio":          rnd(s.get("calmar_ratio")),
            "total_return":          pct(s.get("total_return")),
        }

    # B006: the counting SCOPE here was already right -- cash over every year
    # the strategy ran, not just the years the benchmark can price. What changes
    # is that invested_periods is now DERIVED from total_rebalances instead of
    # counted independently, and the measured window is published. Every
    # results.append site writes a float portfolio_return, so executed ==
    # results; the filter is kept so the shape matches every other topic.
    executed = [r for r in results if r["portfolio_return"] is not None]
    cash_periods = sum(1 for r in executed if r["pairs_entered"] < MIN_PAIRS_ACTIVE)
    invested = [r["pairs_active"] for r in executed if r["pairs_entered"] >= MIN_PAIRS_ACTIVE]
    avg_pairs = round(sum(invested) / len(invested), 1) if invested else 0

    out = {
        "universe": universe_name,
        # B006: n_periods, total_rebalances, cash_periods, invested_periods and
        # the window-provenance block. These are yearly records, so the period
        # keys are "year". `n_years` below is kept for backward compatibility
        # and equals n_periods. No key may re-declare an accounting key --
        # gate 8 enforces that.
        **period_accounting(executed, valid, cash_periods,
                            universe_name=universe_name,
                            benchmark_symbol=benchmark_symbol,
                            date_key="year", end_key="year"),
        "n_years": len(valid),
        "years": f"{START_YEAR}-{END_YEAR}",
        "frequency": "annual",
        "execution": "next-day close (MOC)",
        "benchmark_name": benchmark_name,
        "avg_pairs_when_invested": avg_pairs,
        "legs_capped": sum(r["legs_capped"] for r in executed),
        "portfolio": fmt(p),
        "spy": fmt(b),
        "comparison": {
            "excess_cagr":      pct(c.get("excess_cagr")),
            "win_rate":         pct(c.get("win_rate")),
            "information_ratio": rnd(c.get("information_ratio")),
            "up_capture":       pct(c.get("up_capture")),
            "down_capture":     pct(c.get("down_capture")),
            "beta":             rnd(c.get("beta")),
            "alpha":            pct(c.get("alpha")),
        },
        "excess_cagr": pct(c.get("excess_cagr")),
        "win_rate_vs_spy": pct(c.get("win_rate")),
        "annual_returns": [
            {
                "year": r["year"],
                "portfolio": round(r["portfolio_return"] * 100, 2),
                "spy": round(r["spy_return"] * 100, 2) if r["spy_return"] is not None else None,
                "excess": round((r["portfolio_return"] - (r["spy_return"] or 0)) * 100, 2),
                "pairs_active": r["pairs_active"],
                "pairs_formed": r["pairs_formed"],
                "pairs_entered": r["pairs_entered"],
                "legs_capped": r["legs_capped"],
                "same_company_pairs": r["same_company_pairs"],
                # both mode: the LTP book of an invested year, next to the published drop book
                **{k: v for k, v in r.items() if "ltp" in k},
                # the year's universe: candidates, sectors and the other build_candidates counts
                **(per_year or {}).get(r["year"], {}),
                "pairs": r["pairs"],
            }
            for r in results
        ],
        # Size look-ahead removed; survivorship reduced, not removed (see pairs_universe.py).
        "universe_rule": {**UNIVERSE_NOTES, "top_n_per_sector": TOP_N_PER_SECTOR,
                          "min_formation_bars": MIN_CORR_DAYS},
        "listing_filter": listing_filter,
        "isin_dedup": True,
        "company_dedup": True,
    }
    if ltp_block is not None:
        out.update({"exit_method": ltp_block["exit_method"], "ltp": ltp_block})
    return out


def run_single(cr, exchanges, universe_name, use_costs, risk_free_rate, verbose,
               output_path=None, offset_days=1, exit_method="drop"):
    """Run backtest for a single exchange set. Returns output dict or None."""
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "same-day close (legacy)" if offset_days == 0 else "next-day close (MOC)"

    signal_desc = (f"Same-sector corr > {MIN_CORR}, top {MAX_PAIRS} pairs, "
                   f"|z| > {Z_ENTRY} entry, annual rebalance {START_YEAR}-{END_YEAR}")
    print_header("PAIRS TRADING BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Costs: {'size-tiered, one per leg and round trip' if use_costs else 'none'}  "
          f"RFR: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    fetched = fetch_data_via_api(cr, exchanges, verbose=verbose, offset_days=offset_days)
    if fetched is None:
        print("No data available. Skipping.")
        return None
    con, per_year = fetched
    ltp = PairLtp(con, load_delist_bounds(cr), verbose=verbose) if exit_method != "drop" else None
    print(f"Data fetched in {time.time()-t0:.0f}s")

    # Check if local benchmark has price data; fall back to SPY if not
    bench_check = con.execute(f"""
        SELECT COUNT(*) FROM prices_cache WHERE symbol = '{benchmark_symbol}'
    """).fetchone()[0]
    if bench_check == 0 and benchmark_symbol != "SPY":
        print(f"  WARNING: No price data for {benchmark_symbol}, falling back to SPY")
        benchmark_symbol = "SPY"
        benchmark_name = "S&P 500"

    print(f"\nPhase 2: Running annual backtest ({START_YEAR}-{END_YEAR})...")
    t1 = time.time()
    results = run_backtest(con, use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol,
                           ltp=ltp, exit_method=exit_method)
    print(f"Backtest completed in {time.time()-t1:.0f}s")

    # B006: filter on BOTH keys, as the other 66 topics do. period_accounting
    # raises if `valid` is not a subset of `executed`, and with only the
    # spy_return test that containment held by accident: no results.append site
    # in this file currently writes a None portfolio_return. Add one trailing-
    # stub or skip path that records a benchmark return without a portfolio
    # return -- which sector-momentum's grid already does for its own reasons --
    # and this topic starts hard-crashing on real data. The invariant would be
    # firing correctly; the caller would be wrong. Behaviour is unchanged today.
    valid = [r for r in results
             if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns  = [r["spy_return"] for r in valid]

    m = compute_metrics(port_returns, spy_returns, 1, risk_free_rate=risk_free_rate)
    print(format_metrics(m, "Pairs", benchmark_name))
    ltp_block = ltp.results_block(exit_method, valid, 1, risk_free_rate) if ltp else None

    # B006: scope unchanged (every year the strategy ran). `executed` drops any
    # record the strategy never actually ran; this topic writes none.
    executed = [r for r in results if r["portfolio_return"] is not None]
    cash_periods = sum(1 for r in executed if r["pairs_entered"] < MIN_PAIRS_ACTIVE)
    avg_pairs = sum(r["pairs_active"] for r in valid) / len(valid)
    print(f"\n  Cash periods: {cash_periods}/{len(executed)}")
    print(f"  Avg active pairs: {avg_pairs:.1f}")

    annual = compute_annual_returns(port_returns, spy_returns,
                                    [str(r["year"]) for r in valid], 1)
    if annual:
        bname = benchmark_name[:8]
        print(f"\n  {'Year':<8} {'Pairs':>10} {bname:>8} {'Excess':>8} {'Active':>8}")
        print("  " + "-" * 50)
        for i, ar in enumerate(annual):
            n_act = valid[i]["pairs_active"] if i < len(valid) else "-"
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}%"
                  f" {ar['benchmark']*100:>7.1f}%"
                  f" {ar['excess']*100:>+7.1f}% {n_act:>8}")

    print(f"\n  Total time: {time.time()-t0:.0f}s")

    out = build_output(m, annual, valid, results, universe_name,
                       benchmark_name=benchmark_name,
                       benchmark_symbol=benchmark_symbol,
                       per_year=per_year,
                       listing_filter=campaign_listing_label(exchanges),
                       ltp_block=ltp_block)
    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
                    exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(out, f, indent=2)
        print(f"  Results saved to {output_path}")

    con.close()
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Pairs Trading Backtest (Annual, Same-Sector, Correlation-Based)")
    add_common_args(parser)
    add_exit_method_arg(parser)
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    use_costs = not args.no_costs
    offset_days = 0 if args.no_next_day else 1

    # ── Global mode ────────────────────────────────────────────────────────────
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        # Exchanges eligible for pairs trading:
        # - Sufficient universe size (200+ large-cap stocks)
        # - 10+ years of price history
        # - Short-selling broadly available (note: restricted in China/India)
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),
            ("japan",       ["JPX"]),
            ("uk",          ["LSE"]),
            ("china",       ["SHZ", "SHH"]),
            ("hongkong",    ["HKSE"]),
            ("korea",       ["KSC"]),
            ("taiwan",      ["TAI", "TWO"]),
            ("germany",     ["XETRA"]),
            ("canada",      ["TSX"]),
            ("sweden",      ["STO"]),
            ("switzerland", ["SIX"]),
            ("thailand",    ["SET"]),
            ("southafrica", ["JNB"]),
            ("singapore",   ["SES"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        all_results = {}

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"returns_{uni_name}.json")

            print(f"\n{'#'*65}\n# {preset_name.upper()} ({uni_name})\n{'#'*65}")
            try:
                result = run_single(cr, preset_exchanges, uni_name, use_costs, rfr,
                                    args.verbose, output_path,
                                    offset_days=offset_days,
                                    exit_method=args.exit_method)
                if result:
                    all_results[uni_name] = result
            except Exception as e:
                print(f"\n  ERROR on {uni_name}: {e}")
                import traceback
                traceback.print_exc()
                all_results[uni_name] = {"error": str(e)}

        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Summary table
        print(f"\n\n{'='*80}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'='*80}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>7} {'AvgPairs':>9}")
        print("-" * 75)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR':>40}")
                continue
            p = r.get("portfolio", {})
            c = r.get("comparison", {})
            n = r.get("n_years", 0)
            # B006: divide by total_rebalances, not n_years. Cash is counted
            # over every year the strategy ran; n_years counts only the
            # benchmark-priced ones. Fallback keeps pre-B006 JSONs renderable.
            tr = r.get("total_rebalances", n)
            cp = r.get("cash_periods", 0)
            cash_pct = round(cp * 100 / tr, 0) if tr > 0 else 0
            # B006: this table prints to STDOUT; period_accounting's truncation
            # warning goes to STDERR. A `> log.txt` re-run therefore kept the
            # numbers and lost the caveat, which is the shape defect (b) takes
            # downstream. Names the exchange so the note stands alone, and
            # prints nothing at all on an untruncated leg.
            if r.get("window_truncated"):
                print(f"  {uni}: MEASURED {r.get('window_label')}")
            cagr = p.get("cagr")
            exc  = c.get("excess_cagr")
            shp  = p.get("sharpe_ratio")
            mdd  = p.get("max_drawdown")
            avg  = r.get("avg_pairs_when_invested")
            exc_s = f"{exc:+.2f}" if exc is not None else "N/A"
            print(f"{uni:<20} {str(cagr)+('%' if cagr else ''):>8}"
                  f" {exc_s:>7}%"
                  f" {str(shp) if shp else 'N/A':>8}"
                  f" {str(mdd)+('%' if mdd else ''):>8}"
                  f" {cash_pct:>6.0f}%"
                  f" {str(avg) if avg else 'N/A':>9}")
        print("=" * 80)
        return

    # ── Single exchange mode ──────────────────────────────────────────────────
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, use_costs, risk_free_rate,
               args.verbose, args.output, offset_days=offset_days,
               exit_method=args.exit_method)


if __name__ == "__main__":
    main()
