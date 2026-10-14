#!/usr/bin/env python3
"""
Z-Score Pairs Trading Backtest

Active daily z-score monitoring on mean-reverting stock pairs. Annual pair
formation (correlation + half-life filter), then daily z-score signals during
the trading year: enter at |z| > 2.0, exit at |z| < 0.5, with 60-day and
loss-stop exits. Equal-dollar weight across all active trades.

Universe (pairs_universe.py): for each trading year, the lines listed and traded
through the formation year before it, one per security and per company, the top
30 of each sector by that year's median daily traded value in USD. No size
look-ahead. A line needs an FY filing of any date, a later one included: that
test is not point-in-time. Survivorship is reduced, not removed: the price data
holds almost no name that stopped trading before 2015 (US) or 2020 (elsewhere).

Key output beyond CAGR/Sharpe: convergence_rate and avg_holding_days expose
the disconnect between "convergence" (spread returns to mean) and actual P&L
after transaction costs.

Return model:
    - Log-spread: s_t = log(P_A_t) - beta * log(P_B_t)
    - Rolling z-score: z_t = (s_t - mean(s_{t-N..t})) / std(s_{t-N..t}), N=40,
      on a full window of N common bars
    - If z_entry > 0: A expensive vs B → direction = -1 (short A, long B)
    - Pair return per trade = direction * (Return_A - Return_B) / 2, less one
      size-tiered cost per leg for the round trip
    - Entered pair: each leg's entry bar, its first bar in the ten days from
      1 January + the execution offset, prints MIN_LEG_PRICE or more inside the
      pair's delisting bound. Only entered pairs trade that year.
    - Annual portfolio return = sum(trade_returns) / n_pairs_formed
    - Cash year: fewer than MIN_PAIRS_ACTIVE entered pairs, decided on the
      entry bars alone; nothing trades
    - Exits: a position still open at the pair's last common bar closes there
      (--exit-method ltp, the only mode)

Academic reference:
    Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading:
    Performance of a Relative-Value Arbitrage Rule." Review of Financial
    Studies, 19(3), 797-827.

Usage:
    python3 pairs-zscore/backtest.py                              # US default
    python3 pairs-zscore/backtest.py --preset japan
    python3 pairs-zscore/backtest.py --global \\
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
from data_utils import (get_local_benchmark, add_exit_method_arg, campaign_listing_label,
                        get_prices, utc_date)
from metrics import (compute_metrics, compute_annual_returns, format_metrics,
                     period_accounting)
from costs import tiered_cost
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate)
from pairs_universe import (ENTRY_WINDOW_DAYS, UNIVERSE_NOTES, PairLtp, build_candidates,
                            check_pairs, fetch_prices, fetch_universe, load_delist_bounds)

# ── Strategy Parameters ────────────────────────────────────────────────────────
TOP_N_PER_SECTOR  = 30       # Candidate stocks per sector (ranked by formation-year traded value)
MIN_CORR          = 0.70     # Minimum 252-day returns correlation
MIN_CORR_DAYS     = 200      # Minimum common trading days for pair eligibility (and formation-year bars)
MIN_CANDIDATES    = 20       # Leg skipped when no trading year has this many candidates
MAX_PAIRS         = 20       # Maximum pairs to hold per year
HALF_LIFE_MIN     = 5        # Minimum half-life (days) — filter out noise pairs
HALF_LIFE_MAX     = 60       # Maximum half-life (days) — filter out barely-mean-reverting pairs
Z_LOOKBACK        = 40       # Rolling window for z-score mean/std (trading days)
Z_ENTRY           = 2.0      # |z| threshold to enter a trade
Z_EXIT            = 0.5      # |z| threshold for convergence exit
MAX_HOLD_DAYS     = 60       # Time stop: maximum trading days to hold a position
LOSS_STOP         = -0.05    # Loss stop: exit if pair P&L < -5%
MIN_PAIRS_ACTIVE  = 3        # Annual portfolio held cash if fewer formed pairs are entered
MAX_SINGLE_RETURN = 2.0      # Cap absolute pair return (data quality guard)
MIN_LEG_PRICE     = 1.0      # Skip pairs where either leg price < 1.0 (artifacts)
START_YEAR        = 2005
END_YEAR          = 2024


def fetch_data_via_api(cr, exchanges, verbose=False, benchmark_symbol="SPY", offset_days=1):
    """Build the per-year candidate universe (pairs_universe) and fetch daily prices into DuckDB.

    The connection holds pairs_universe's tables, among them:
        candidates(yr, symbol, sector, company, size_usd, cap_usd)   one list per trading year
        prices_cache(symbol, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)

    Returns (connection, {year: build_candidates' counts}) or None.
    """
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

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

    # ── Fetch daily prices (2004 onward) ──────────────────────────────────────
    # Extra year (2004) provides warmup for z-score lookback on first trading year
    print("  Fetching daily prices (2004-present)...")
    count = fetch_prices(cr, con, None, [benchmark_symbol], verbose=verbose)
    if count == 0:
        print("  No price data found.")
        return None

    print(f"  Price rows: {count:,}")
    return con, per_year


def compute_pair_candidates(con, formation_start, formation_end):
    """Find same-sector pairs by returns correlation during the formation period.

    Returns list of (sym_a, sym_b, sector, correlation, common_days).
    Both legs are candidates of the trading year after the formation period.
    Filters: same sector, corr >= MIN_CORR, >= MIN_CORR_DAYS common days.
    Ranked by correlation (desc).
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
                a.sector,
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
        LIMIT {MAX_PAIRS * 10}
    """).fetchall()

    return rows


def estimate_spread_params(con, sym_a, sym_b, formation_start, formation_end):
    """Estimate OLS beta (log-price), spread mean/std, and half-life.

    Spread = log(P_A) - beta * log(P_B)
    Beta = CORR(log_A, log_B) * STD(log_A) / STD(log_B)
    Half-life = -log(2) / log(1 + AR1_coefficient)

    Returns (beta, spread_mean, spread_std, half_life) or None if:
        - Insufficient data
        - Half-life outside [HALF_LIFE_MIN, HALF_LIFE_MAX]
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
            SELECT a.trade_date, a.lp AS la, b.lp AS lb
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        ),
        beta_est AS (
            SELECT
                CORR(la, lb) * STDDEV(la) / NULLIF(STDDEV(lb), 0) AS beta,
                AVG(la) AS mean_la,
                AVG(lb) AS mean_lb,
                COUNT(*) AS n_obs
            FROM paired
        )
        SELECT beta, mean_la - beta * mean_lb AS intercept, n_obs FROM beta_est
    """).fetchone()

    if not row or row[0] is None or row[2] < MIN_CORR_DAYS:
        return None

    beta = float(row[0])
    if not math.isfinite(beta) or beta <= 0:
        return None

    # Compute spread time series and AR(1) for half-life
    spread_rows = con.execute(f"""
        WITH log_prices AS (
            SELECT symbol, trade_date, LN(adjClose) AS lp
            FROM prices_cache
            WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'
              AND symbol IN ('{sym_a}', '{sym_b}')
              AND adjClose > 0
        ),
        spread AS (
            SELECT a.trade_date,
                   a.lp - {beta} * b.lp AS s
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
            ORDER BY a.trade_date
        ),
        ar1 AS (
            SELECT
                s,
                LAG(s) OVER (ORDER BY trade_date) AS s_lag
            FROM spread
        )
        SELECT
            AVG(s)     AS mean_s,
            STDDEV(s)  AS std_s,
            -- AR(1) via OLS: ds_t = phi * s_lag + eps
            CORR(s - s_lag, s_lag) * STDDEV(s - s_lag) / NULLIF(STDDEV(s_lag), 0) AS phi
        FROM ar1
        WHERE s IS NOT NULL AND s_lag IS NOT NULL
    """).fetchone()

    if (not spread_rows or spread_rows[0] is None
            or spread_rows[1] is None
            or float(spread_rows[1]) < 1e-10):
        return None

    mean_s = float(spread_rows[0])
    std_s  = float(spread_rows[1])
    phi    = float(spread_rows[2]) if spread_rows[2] is not None else None

    if phi is None or not math.isfinite(phi):
        return None

    # phi is the AR(1) coefficient. Half-life = -log(2)/log(1+phi)
    # For mean reversion: phi must be in (-1, 0)
    if phi >= 0 or phi <= -1:
        return None

    half_life = -math.log(2) / math.log(1 + phi)
    if not (HALF_LIFE_MIN <= half_life <= HALF_LIFE_MAX):
        return None

    return beta, mean_s, std_s, half_life


def entered_pairs(con, selected, trading_start, trading_end, offset_days=1, ltp=None):
    """The pairs of `selected` entered for the year that starts at trading_start.

    A pair is entered when each leg's entry bar, its first bar in [trading_start + offset_days,
    that day + ENTRY_WINDOW_DAYS] (data_utils.get_prices), prints MIN_LEG_PRICE or more and is
    dated inside the pair's delisting bound (ltp.bound, as in simulate_pair_trades). No later
    bar is read: the year's cash decision rests on this list, and only these pairs trade.
    """
    entry = get_prices(con, sorted({s for pair in selected for s in pair[:2]}), trading_start,
                       window_days=ENTRY_WINDOW_DAYS, offset_days=offset_days, with_epochs=True)
    entered = []
    for pair in selected:
        sym_a, sym_b = pair[0], pair[1]
        pa_row, pb_row = entry.get(sym_a), entry.get(sym_b)   # (adjClose, bar epoch)
        if not pa_row or not pb_row:
            continue
        if float(pa_row[0]) < MIN_LEG_PRICE or float(pb_row[0]) < MIN_LEG_PRICE:
            continue
        bound = ltp.bound(sym_a, sym_b, trading_start, trading_end) if ltp is not None else trading_end
        if utc_date(max(pa_row[1], pb_row[1])) > bound:   # a bar past a leg's delisting bound is a repeat
            continue
        entered.append(pair)
    return entered


def simulate_pair_trades(con, sym_a, sym_b, beta,
                          trading_start, trading_end,
                          mc_a, mc_b, use_costs=True, offset_days=1, ltp=None):
    """Simulate active z-score trading for one pair during the trading year.

    Fetches daily z-scores for the pair, then iterates day by day tracking
    entry/exit events. One position open per pair at a time.

    Entry: |z| crosses Z_ENTRY (2.0) from below
    Exit: |z| < Z_EXIT (0.5)  →  convergence
          holding_days >= MAX_HOLD_DAYS  →  time stop
          pair P&L <= LOSS_STOP (-5%)    →  loss stop
          last common bar                →  forced close

    mc_a, mc_b: each leg's market cap in USD (None: the smallest-cap cost tier).
    ltp: pairs_universe.PairLtp. It bounds the bars by a leg's delisting and labels
    the forced close 'last_bar' or 'year_end'.

    Returns list of trade dicts, or None if no prices available.
    Each trade dict:
        entry_date, exit_date, entry_z, exit_z,
        exit_type ('convergence'|'time_stop'|'loss_stop'|'last_bar'|'year_end'),
        holding_days, pair_return (after costs)
    """
    # Warmup: twice Z_LOOKBACK in calendar days, so weekday bars fill the window by trading_start
    warmup_start = trading_start - timedelta(days=Z_LOOKBACK * 2)
    last_day = ltp.bound(sym_a, sym_b, trading_start, trading_end) if ltp is not None else trading_end
    # Half the pair's capital sits in each leg, so a round trip costs each leg's one-way rate once.
    cost = (tiered_cost(mc_a) + tiered_cost(mc_b)) if use_costs else 0.0

    rows = con.execute(f"""
        WITH log_prices AS (
            SELECT symbol, trade_date, LN(adjClose) AS lp, adjClose
            FROM prices_cache
            WHERE trade_date >= '{warmup_start.isoformat()}'
              AND trade_date <= '{last_day.isoformat()}'
              AND symbol IN ('{sym_a}', '{sym_b}')
              AND adjClose > 0
        ),
        paired AS (
            SELECT a.trade_date,
                   a.adjClose AS pa,
                   b.adjClose AS pb,
                   a.lp - {beta} * b.lp AS spread
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        ),
        with_z AS (
            SELECT
                trade_date,
                pa, pb, spread,
                (spread - AVG(spread) OVER w)
                    / NULLIF(STDDEV(spread) OVER w, 0) AS z_score,
                COUNT(*) OVER w AS window_bars
            FROM paired
            WINDOW w AS (ORDER BY trade_date
                         ROWS BETWEEN {Z_LOOKBACK - 1} PRECEDING AND CURRENT ROW)
        )
        SELECT trade_date, pa, pb, z_score
        FROM with_z
        WHERE trade_date >= '{trading_start.isoformat()}'
          AND z_score IS NOT NULL
          AND window_bars = {Z_LOOKBACK}
        ORDER BY trade_date
    """).fetchall()

    if not rows:
        return None

    trades = []
    in_position       = False
    entry_idx         = None
    entry_z           = None
    entry_pa          = None
    entry_pb          = None
    direction         = None   # +1 = long_spread (buy A, short B); -1 = short_spread
    trading_day       = 0      # trading days since entry
    pending_entry_z   = None   # MOC: entry signal z-score, enter at next row's prices
    pending_exit_type = None   # MOC: exit type flagged, execute at next row's prices

    def _append_trade(ed, xd, ez, xz, xt, hd, pr):
        trades.append({
            "entry_date":   ed.isoformat() if hasattr(ed, "isoformat") else str(ed),
            "exit_date":    xd.isoformat() if hasattr(xd, "isoformat") else str(xd),
            "entry_z":      round(ez, 3),
            "exit_z":       round(float(xz), 3) if xz is not None else 0.0,
            "exit_type":    xt,
            "holding_days": hd,
            "pair_return":  round(pr, 6),
        })

    for i, row in enumerate(rows):
        trade_date, pa, pb = row[0], float(row[1]), float(row[2])
        z = row[3]
        if z is None or math.isnan(z):
            continue

        # ── Execute pending exit at today's prices (signal fired yesterday) ────
        if pending_exit_type is not None and in_position:
            exit_type         = pending_exit_type
            pending_exit_type = None
            ret_a      = (pa - entry_pa) / entry_pa
            ret_b      = (pb - entry_pb) / entry_pb
            pair_return = direction * (ret_a - ret_b) / 2.0
            pair_return -= cost
            pair_return = max(pair_return, -MAX_SINGLE_RETURN)
            _append_trade(rows[entry_idx][0], trade_date,
                          entry_z, z, exit_type, trading_day, pair_return)
            in_position = False
            entry_idx   = None
            trading_day = 0

        # ── Execute pending entry at today's prices (signal fired yesterday) ───
        if pending_entry_z is not None and not in_position:
            sig_z           = pending_entry_z
            pending_entry_z = None
            in_position = True
            entry_idx   = i
            entry_z     = sig_z
            entry_pa    = pa
            entry_pb    = pb
            direction   = -1 if sig_z > 0 else 1
            trading_day = 0
            continue  # don't evaluate exits on entry bar

        if not in_position:
            # Entry condition: |z| first crosses Z_ENTRY
            if abs(z) >= Z_ENTRY:
                if offset_days == 0:
                    in_position = True
                    entry_idx   = i
                    entry_z     = z
                    entry_pa    = pa
                    entry_pb    = pb
                    direction   = -1 if z > 0 else 1
                    trading_day = 0
                else:
                    pending_entry_z = z   # enter tomorrow
        else:
            trading_day += 1

            # Current pair P&L (equal-dollar, market-neutral)
            ret_a = (pa - entry_pa) / entry_pa
            ret_b = (pb - entry_pb) / entry_pb
            pair_return = direction * (ret_a - ret_b) / 2.0

            exit_type = None
            if abs(z) <= Z_EXIT:
                exit_type = "convergence"
            elif trading_day >= MAX_HOLD_DAYS:
                exit_type = "time_stop"
            elif pair_return <= LOSS_STOP:
                exit_type = "loss_stop"

            if exit_type:
                if offset_days == 0:
                    pair_return -= cost
                    pair_return = max(pair_return, -MAX_SINGLE_RETURN)
                    _append_trade(rows[entry_idx][0], trade_date,
                                  entry_z, z, exit_type, trading_day, pair_return)
                    in_position = False
                    entry_idx   = None
                    trading_day = 0
                else:
                    pending_exit_type = exit_type   # exit tomorrow

    # Force-close: a position still open at the pair's last common bar closes there
    if in_position and rows:
        last = rows[-1]
        last_date, last_pa, last_pb, last_z = last[0], float(last[1]), float(last[2]), last[3]
        if last_pa and last_pb and entry_pa and entry_pb:
            if ltp is not None:   # an exit queued for a bar that never came is a forced close too
                final_exit = ltp.close(sym_a, sym_b, rows[entry_idx][0], last_date,
                                       trading_start, trading_end)
            else:
                final_exit = pending_exit_type if pending_exit_type else "year_end"
            ret_a = (last_pa - entry_pa) / entry_pa
            ret_b = (last_pb - entry_pb) / entry_pb
            pair_return = direction * (ret_a - ret_b) / 2.0
            pair_return -= cost
            pair_return = max(pair_return, -MAX_SINGLE_RETURN)
            trading_day = len(rows) - 1 - entry_idx
            _append_trade(rows[entry_idx][0], last_date,
                          entry_z, last_z, final_exit, trading_day, pair_return)

    return trades if trades else None


def run_backtest(con, use_costs=True, verbose=False, offset_days=1,
                 benchmark_symbol="SPY", ltp=None):
    """Run annual z-score pairs backtest (START_YEAR to END_YEAR).

    Reads con's candidates (one list per trading year) and prices_cache.
    ltp: pairs_universe.PairLtp for the forced closes (see simulate_pair_trades) and the entry
    bound (see entered_pairs). A formed pair that is not entered sits the year out.

    Returns (annual_results, trade_stats) where:
        annual_results: list of {year, portfolio_return, spy_return, pairs_formed,
                                  pairs_entered, pairs_with_trades, total_trades,
                                  same_company_pairs, pairs}
        trade_stats: aggregate trade statistics across all years
    """
    annual_results = []
    all_trades = []

    for year in range(START_YEAR, END_YEAR + 1):
        formation_start = date(year - 1, 1, 1)
        formation_end   = date(year - 1, 12, 31)
        trading_start   = date(year, 1, 1)
        trading_end     = date(year, 12, 31)

        t0 = time.time()

        # ── Step 1: Find correlated same-sector pairs ─────────────────────────
        candidates = compute_pair_candidates(con, formation_start, formation_end)

        # ── Step 2: Apply half-life filter (cointegration proxy) ──────────────
        filtered_pairs = []
        for (sym_a, sym_b, sector, corr, common_days) in candidates:
            params = estimate_spread_params(con, sym_a, sym_b,
                                            formation_start, formation_end)
            if params is not None:
                beta, spread_mean, spread_std, half_life = params
                filtered_pairs.append((sym_a, sym_b, sector, corr, beta,
                                       spread_mean, spread_std, half_life))
            if len(filtered_pairs) >= MAX_PAIRS:
                break

        selected = filtered_pairs[:MAX_PAIRS]
        # Two lines of one company, or a NaN correlation, stop the run.
        same_company = check_pairs(con, year, [(p[0], p[1], p[3]) for p in selected])

        if verbose:
            print(f"  {year}: {len(candidates)} corr candidates → "
                  f"{len(selected)} with valid half-life  [{time.time()-t0:.1f}s]")

        # ── Step 3: Benchmark annual return ───────────────────────────────────
        spy_start_row = con.execute(f"""
            SELECT adjClose FROM prices_cache
            WHERE symbol = '{benchmark_symbol}'
              AND trade_date >= '{trading_start.isoformat()}'
            ORDER BY trade_date ASC, adjClose DESC NULLS LAST LIMIT 1
        """).fetchone()
        spy_end_row = con.execute(f"""
            SELECT adjClose FROM prices_cache
            WHERE symbol = '{benchmark_symbol}'
              AND trade_date <= '{trading_end.isoformat()}'
            ORDER BY trade_date DESC, adjClose DESC NULLS LAST LIMIT 1
        """).fetchone()
        spy_ret = None
        if spy_start_row and spy_end_row:
            spy_s, spy_e = float(spy_start_row[0]), float(spy_end_row[0])
            if spy_s > 0:
                spy_ret = (spy_e - spy_s) / spy_s

        # ── Step 4: Pairs entered, on their entry bars alone ──────────────────
        entered = entered_pairs(con, selected, trading_start, trading_end, offset_days, ltp)

        # ── Step 5: Simulate z-score trades for each entered pair ─────────────
        # Cash is decided here, on the entry-side count: a cash year trades nothing.
        n_formed, n_entered = len(selected), len(entered)
        invested = n_entered >= MIN_PAIRS_ACTIVE
        # Cost tier: each leg's USD market cap for this trading year (None: the smallest-cap tier).
        caps = dict(con.execute("SELECT symbol, cap_usd FROM candidates WHERE yr = ?",
                                [year]).fetchall())
        year_trade_returns = []
        pairs_with_trades = 0
        total_year_trades = 0

        for (sym_a, sym_b, sector, corr, beta, _mean, _std, half_life) in (entered if invested else []):
            trades = simulate_pair_trades(
                con, sym_a, sym_b, beta,
                trading_start, trading_end,
                caps.get(sym_a), caps.get(sym_b), use_costs=use_costs,
                offset_days=offset_days, ltp=ltp
            )

            if trades:
                pairs_with_trades += 1
                total_year_trades += len(trades)
                pair_year_return = sum(t["pair_return"] for t in trades)
                year_trade_returns.append(pair_year_return)
                for t in trades:
                    t["year"] = year
                    t["symbol_a"] = sym_a
                    t["symbol_b"] = sym_b
                    all_trades.append(t)
            else:
                year_trade_returns.append(0.0)  # Pair formed but no signals fired

        # ── Step 6: Annual portfolio return ───────────────────────────────────
        if invested:
            port_return = sum(year_trade_returns) / n_formed
        else:
            port_return = 0.0  # Cash period

        annual_results.append({
            "year":             year,
            "portfolio_return": round(port_return, 6),
            "spy_return":       round(spy_ret, 6) if spy_ret is not None else None,
            "pairs_formed":     n_formed,
            "pairs_entered":    n_entered,
            "pairs_with_trades": pairs_with_trades,
            "total_trades":     total_year_trades,
            "same_company_pairs": same_company,
            "pairs":            [{"a": p[0], "b": p[1], "sector": p[2], "corr": p[3]}
                                 for p in selected],
        })

        if verbose:
            spy_s = f"{spy_ret*100:.1f}%" if spy_ret is not None else "N/A"
            cash_marker = "" if invested else " [CASH]"
            print(f"    → {pairs_with_trades}/{n_entered} entered pairs traded "
                  f"({n_formed} formed), {total_year_trades} trades, "
                  f"port={port_return*100:.1f}%, spy={spy_s}{cash_marker}")

    # ── Aggregate trade statistics ─────────────────────────────────────────────
    trade_stats = {}
    if all_trades:
        n = len(all_trades)
        n_conv    = sum(1 for t in all_trades if t["exit_type"] == "convergence")
        n_time    = sum(1 for t in all_trades if t["exit_type"] == "time_stop")
        n_loss    = sum(1 for t in all_trades if t["exit_type"] == "loss_stop")
        n_end     = sum(1 for t in all_trades if t["exit_type"] == "year_end")
        n_last    = sum(1 for t in all_trades if t["exit_type"] == "last_bar")
        avg_hold  = sum(t["holding_days"] for t in all_trades) / n
        avg_pl    = sum(t["pair_return"] for t in all_trades) / n
        trade_stats = {
            "total_trades":        n,
            "convergence_rate":    round(n_conv * 100.0 / n, 1),
            "time_stop_rate":      round(n_time * 100.0 / n, 1),
            "loss_stop_rate":      round(n_loss * 100.0 / n, 1),
            "year_end_rate":       round(n_end  * 100.0 / n, 1),
            "last_bar_rate":       round(n_last * 100.0 / n, 1),
            "avg_holding_days":    round(avg_hold, 1),
            "avg_trade_return_pct":round(avg_pl * 100, 3),
        }

    return annual_results, trade_stats


def build_output(m, annual, valid, results, trade_stats, universe_name,
                 per_year=None, listing_filter=None, ltp_block=None):
    """Build JSON output in standard exchange_comparison format plus trade stats.

    per_year: build_candidates' {year: counts}, merged into each year's record.
    listing_filter: campaign_listing_label() of the leg. ltp_block: PairLtp.closes_block().
    """
    p = m["portfolio"]
    b = m["benchmark"]
    c = m["comparison"]

    def pct(v):       return round(v * 100, 2) if v is not None else None
    def rnd(v, d=3):  return round(v, d) if v is not None else None

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
    executed      = [r for r in results if r["portfolio_return"] is not None]
    cash_periods  = sum(1 for r in executed if r["pairs_entered"] < MIN_PAIRS_ACTIVE)
    invested_yrs  = [r for r in executed if r["pairs_entered"] >= MIN_PAIRS_ACTIVE]
    avg_pairs     = (round(sum(r["pairs_with_trades"] for r in invested_yrs) / len(invested_yrs), 1)
                     if invested_yrs else 0)
    total_trades  = sum(r["total_trades"] for r in executed)
    trades_per_yr = round(total_trades / len(executed), 1) if executed else 0

    out = {
        "universe":             universe_name,
        # B006: n_periods, total_rebalances, cash_periods, invested_periods and
        # the window-provenance block. These are yearly records, so the period
        # keys are "year". `n_years` below is kept for backward compatibility
        # and equals n_periods. No key may re-declare an accounting key --
        # gate 8 enforces that.
        **period_accounting(executed, valid, cash_periods,
                            universe_name=universe_name,
                            date_key="year", end_key="year"),
        "n_years":              len(valid),
        "years":                f"{START_YEAR}-{END_YEAR}",
        "frequency":            "daily_zscore",
        "avg_pairs_with_trades": avg_pairs,
        "total_trades":         total_trades,
        "trades_per_year":      trades_per_yr,
        "trade_stats":          trade_stats,
        "portfolio":            fmt(p),
        "spy":                  fmt(b),
        "comparison": {
            "excess_cagr":       pct(c.get("excess_cagr")),
            "win_rate":          pct(c.get("win_rate")),
            "information_ratio": rnd(c.get("information_ratio")),
            "up_capture":        pct(c.get("up_capture")),
            "down_capture":      pct(c.get("down_capture")),
            "beta":              rnd(c.get("beta")),
            "alpha":             pct(c.get("alpha")),
        },
        "excess_cagr":         pct(c.get("excess_cagr")),
        "win_rate_vs_spy":     pct(c.get("win_rate")),
        "annual_returns": [
            {
                "year":              r["year"],
                "portfolio":         round(r["portfolio_return"] * 100, 2),
                "spy":               round(r["spy_return"] * 100, 2)
                                     if r["spy_return"] is not None else None,
                "excess":            round((r["portfolio_return"] - (r["spy_return"] or 0)) * 100, 2),
                "pairs_formed":      r["pairs_formed"],
                "pairs_entered":     r["pairs_entered"],
                "pairs_with_trades": r["pairs_with_trades"],
                "total_trades":      r["total_trades"],
                "same_company_pairs": r["same_company_pairs"],
                # the year's universe: candidates, sectors and the other build_candidates counts
                **(per_year or {}).get(r["year"], {}),
                "pairs":             r["pairs"],
            }
            for r in results
        ],
        # Size look-ahead removed; survivorship reduced, not removed (see pairs_universe.py).
        "universe_rule":       {**UNIVERSE_NOTES, "top_n_per_sector": TOP_N_PER_SECTOR,
                                "min_formation_bars": MIN_CORR_DAYS},
        "listing_filter":      listing_filter,
        "isin_dedup":          True,
        "company_dedup":       True,
    }
    if ltp_block is not None:
        out.update({"exit_method": ltp_block["exit_method"], "ltp": ltp_block})
    return out


def run_single(cr, exchanges, universe_name, use_costs, risk_free_rate,
               verbose, output_path=None, offset_days=1, *, exit_method):
    """Run backtest for a single exchange set. Returns output dict or None."""
    if exit_method != "ltp":   # a pair has no drop book: both legs close at their last common bar
        raise ValueError("pairs-zscore runs with --exit-method ltp only")
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "next-day close (MOC)" if offset_days else "same-bar"
    signal_desc = (
        f"Same-sector corr > {MIN_CORR}, half-life {HALF_LIFE_MIN}-{HALF_LIFE_MAX}d, "
        f"top {MAX_PAIRS} pairs, z-entry > {Z_ENTRY}, z-exit < {Z_EXIT}, "
        f"max hold {MAX_HOLD_DAYS}d, loss stop {LOSS_STOP*100:.0f}%"
    )
    print_header("Z-SCORE PAIRS TRADING BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Costs: {'size-tiered, one per leg and round trip' if use_costs else 'none'}  "
          f"RFR: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    fetched = fetch_data_via_api(cr, exchanges, verbose=verbose,
                                 benchmark_symbol=benchmark_symbol, offset_days=offset_days)
    if fetched is None:
        print("No data available. Skipping.")
        return None
    con, per_year = fetched
    ltp = PairLtp(con, load_delist_bounds(cr), verbose=verbose)
    print(f"Data fetched in {time.time()-t0:.0f}s")

    print(f"\nPhase 2: Running daily z-score simulation ({START_YEAR}-{END_YEAR})...")
    t1 = time.time()
    results, trade_stats = run_backtest(con, use_costs=use_costs, verbose=verbose,
                                        offset_days=offset_days,
                                        benchmark_symbol=benchmark_symbol, ltp=ltp)
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
    spy_returns  = [r["spy_return"]       for r in valid]

    m = compute_metrics(port_returns, spy_returns, 1, risk_free_rate=risk_free_rate)
    print(format_metrics(m, "Pairs (z-score)", benchmark_name))

    # Trade statistics summary
    # B006: scope unchanged (every year the strategy ran). `executed` drops any
    # record the strategy never actually ran; this topic writes none.
    executed = [r for r in results if r["portfolio_return"] is not None]
    cash_periods = sum(1 for r in executed if r["pairs_entered"] < MIN_PAIRS_ACTIVE)
    print(f"\n  Cash periods: {cash_periods}/{len(executed)}")
    if trade_stats:
        print(f"  Total trades: {trade_stats['total_trades']}")
        print(f"  Convergence rate: {trade_stats['convergence_rate']}%")
        print(f"  Time stop rate:   {trade_stats['time_stop_rate']}%")
        print(f"  Loss stop rate:   {trade_stats['loss_stop_rate']}%")
        print(f"  Last-bar closes:  {trade_stats['last_bar_rate']}%")
        print(f"  Avg hold:         {trade_stats['avg_holding_days']} days")
        print(f"  Avg trade return: {trade_stats['avg_trade_return_pct']}%")

    annual = compute_annual_returns(port_returns, spy_returns,
                                    [str(r["year"]) for r in valid], 1)
    if annual:
        bench_hdr = benchmark_name[:6] if len(benchmark_name) > 6 else benchmark_name
        print(f"\n  {'Year':<8} {'Port':>8} {bench_hdr:>8} {'Excess':>8} "
              f"{'Trades':>8}")
        print("  " + "-" * 52)
        for i, ar in enumerate(annual):
            r = valid[i] if i < len(valid) else {}
            n_tr = r.get("total_trades", "-")
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>7.1f}%"
                  f" {ar['benchmark']*100:>7.1f}%"
                  f" {ar['excess']*100:>+7.1f}% {str(n_tr):>8}")

    print(f"\n  Total time: {time.time()-t0:.0f}s")

    out = build_output(m, annual, valid, results, trade_stats, universe_name,
                       per_year=per_year, listing_filter=campaign_listing_label(exchanges),
                       ltp_block=ltp.closes_block(exit_method))
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
        description="Z-Score Pairs Trading Backtest (Daily Signal, Annual Formation)")
    add_common_args(parser)
    add_exit_method_arg(parser)
    args = parser.parse_args()
    if args.exit_method != "ltp":   # no drop book exists to publish or compare against
        parser.error("pairs-zscore closes a pair at the last bar both legs traded: "
                     "--exit-method ltp is the only mode")

    exchanges, universe_name = resolve_exchanges(args)
    use_costs = not args.no_costs
    offset_days = 0 if args.no_next_day else 1

    # ── Global mode ────────────────────────────────────────────────────────────
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all eligible exchange presets")
        print("=" * 65)
        print("\n  Eligibility notes:")
        print("  - China/India: short-selling restrictions apply")
        print("  - Results are theoretical for restricted markets")
        print()

        # The 12 markets of the comparison. SET and SES are not run.
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("japan",       ["JPX"]),
            ("canada",      ["TSX"]),
            ("hongkong",    ["HKSE"]),
            ("china",       ["SHZ", "SHH"]),
            ("korea",       ["KSC"]),
            ("taiwan",      ["TAI", "TWO"]),
            ("sweden",      ["STO"]),
            ("southafrica", ["JNB"]),
            ("india",       ["NSE"]),
            ("uk",          ["LSE"]),
            ("germany",     ["XETRA"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        all_results = {}

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr      = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            out_path = None
            if args.output:
                out_dir  = os.path.dirname(args.output) or "."
                out_path = os.path.join(out_dir, f"returns_{uni_name}.json")

            print(f"\n{'#'*65}\n# {preset_name.upper()} ({uni_name})\n{'#'*65}")
            try:
                result = run_single(cr, preset_exchanges, uni_name,
                                    use_costs, rfr, args.verbose, out_path,
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
            print(f"\nExchange comparison saved to {args.output}")

        # Summary table
        print(f"\n\n{'='*90}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'='*90}")
        print(f"{'Exchange':<22} {'CAGR':>7} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>7} {'Conv%':>7} {'AvgHold':>8}")
        print("-" * 85)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<22} {'ERROR':>50}")
                continue
            p  = r.get("portfolio", {})
            c  = r.get("comparison", {})
            ts = r.get("trade_stats", {})
            n  = r.get("n_years", 0)
            # B006: divide by total_rebalances, not n_years. Cash is counted
            # over every year the strategy ran; n_years counts only the
            # benchmark-priced ones. Fallback keeps pre-B006 JSONs renderable.
            tr = r.get("total_rebalances", n)
            cp = r.get("cash_periods", 0)
            cash_pct  = round(cp * 100 / tr, 0) if tr > 0 else 0
            conv_pct  = ts.get("convergence_rate", "N/A")
            avg_hold  = ts.get("avg_holding_days", "N/A")
            cagr      = p.get("cagr")
            exc       = c.get("excess_cagr")
            shp       = p.get("sharpe_ratio")
            mdd       = p.get("max_drawdown")
            exc_s     = f"{exc:+.2f}" if exc is not None else "N/A"
            print(f"{uni:<22} {str(cagr)+'%' if cagr is not None else 'N/A':>7}"
                  f" {exc_s:>7}%"
                  f" {str(shp) if shp is not None else 'N/A':>8}"
                  f" {str(mdd)+'%' if mdd is not None else 'N/A':>8}"
                  f" {cash_pct:>6.0f}%"
                  f" {str(conv_pct)+'%' if isinstance(conv_pct, (int,float)) else 'N/A':>7}"
                  f" {str(avg_hold)+'d' if isinstance(avg_hold, (int,float)) else 'N/A':>8}")
        print("=" * 90)
        return

    # ── Single exchange mode ───────────────────────────────────────────────────
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, use_costs, risk_free_rate,
               args.verbose, args.output, offset_days=offset_days,
               exit_method=args.exit_method)


if __name__ == "__main__":
    main()
