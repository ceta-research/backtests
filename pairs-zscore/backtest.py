#!/usr/bin/env python3
"""
Z-Score Pairs Trading Backtest

Active daily z-score monitoring on mean-reverting stock pairs. Annual pair
formation (correlation + half-life filter), then daily z-score signals during
the trading year: enter at |z| > 2.0, exit at |z| < 0.5, with 60-day and
loss-stop exits. Equal-dollar weight across all active trades.

Key output beyond CAGR/Sharpe: convergence_rate and avg_holding_days expose
the disconnect between "convergence" (spread returns to mean) and actual P&L
after transaction costs.

Return model:
    - Log-spread: s_t = log(P_A_t) - beta * log(P_B_t)
    - Rolling z-score: z_t = (s_t - mean(s_{t-N..t})) / std(s_{t-N..t}), N=40
    - If z_entry > 0: A expensive vs B → direction = -1 (short A, long B)
    - Pair return per trade = direction * (Return_A - Return_B) / 2
    - Annual portfolio return = sum(trade_returns) / n_pairs_formed

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
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# ── Strategy Parameters ────────────────────────────────────────────────────────
TOP_N_PER_SECTOR  = 30       # Candidate stocks per sector (ranked by market cap)
MIN_CORR          = 0.70     # Minimum 252-day returns correlation
MIN_CORR_DAYS     = 200      # Minimum common trading days for pair eligibility
MAX_PAIRS         = 20       # Maximum pairs to hold per year
HALF_LIFE_MIN     = 5        # Minimum half-life (days) — filter out noise pairs
HALF_LIFE_MAX     = 60       # Maximum half-life (days) — filter out barely-mean-reverting pairs
Z_LOOKBACK        = 40       # Rolling window for z-score mean/std (trading days)
Z_ENTRY           = 2.0      # |z| threshold to enter a trade
Z_EXIT            = 0.5      # |z| threshold for convergence exit
MAX_HOLD_DAYS     = 60       # Time stop: maximum trading days to hold a position
LOSS_STOP         = -0.05    # Loss stop: exit if pair P&L < -5%
MIN_PAIRS_ACTIVE  = 3        # Annual portfolio held cash if fewer pairs have any trades
MAX_SINGLE_RETURN = 2.0      # Cap absolute pair return (data quality guard)
MIN_LEG_PRICE     = 1.0      # Skip pairs where either leg price < 1.0 (artifacts)
START_YEAR        = 2005
END_YEAR          = 2024


def fetch_data_via_api(cr, exchanges, verbose=False):
    """Fetch sector mapping, market caps, and full daily price history into DuckDB.

    Populates DuckDB tables:
        sector_map(symbol VARCHAR, sector VARCHAR)
        mcap_map(symbol VARCHAR, marketCap DOUBLE)
        prices_cache(symbol VARCHAR, trade_date DATE, adjClose DOUBLE)

    Returns DuckDB connection or None.
    """
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    ex_filter = ", ".join(f"'{e}'" for e in exchanges) if exchanges else None
    ex_where = f"exchange IN ({ex_filter})" if ex_filter else "1=1"

    # ── Sector map ────────────────────────────────────────────────────────────
    print("  Fetching sector mapping...")
    sector_sql = f"""
        WITH dedup AS (
            SELECT symbol, sector,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetchedAtEpoch DESC) AS rn
            FROM profile
            WHERE {ex_where}
              AND sector IS NOT NULL AND sector != ''
              AND isActivelyTrading = true
        )
        SELECT symbol, sector FROM dedup WHERE rn = 1
    """
    sector_data = cr.query(sector_sql, verbose=verbose)
    if not sector_data:
        print("  No sector data found.")
        return None

    sector_map = {r["symbol"]: r["sector"] for r in sector_data}
    all_symbols = list(sector_map.keys())
    n_sectors = len(set(sector_map.values()))
    print(f"  {len(all_symbols)} symbols across {n_sectors} sectors")

    # ── Latest market cap for candidate ranking ───────────────────────────────
    print("  Fetching market caps...")
    mcap_sql = f"""
        WITH prof AS (
            SELECT DISTINCT symbol FROM profile
            WHERE {ex_where}
              AND sector IS NOT NULL AND sector != ''
              AND isActivelyTrading = true
        ),
        dedup AS (
            SELECT km.symbol, km.marketCap,
                ROW_NUMBER() OVER (PARTITION BY km.symbol ORDER BY km.dateEpoch DESC) AS rn
            FROM key_metrics km
            JOIN prof p ON km.symbol = p.symbol
            WHERE km.period = 'FY'
              AND km.marketCap IS NOT NULL AND km.marketCap > 0
        )
        SELECT symbol, marketCap FROM dedup WHERE rn = 1
    """
    mcap_data = []
    for attempt in range(3):
        try:
            mcap_data = cr.query(mcap_sql, verbose=verbose) or []
            break
        except Exception as e:
            if "Rate limited" in str(e) and attempt < 2:
                wait = 65
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
    if not mcap_data:
        print("  No market cap data.")
        return None

    mcap_map = {r["symbol"]: r["marketCap"] for r in mcap_data}

    # ── Select top N per sector by market cap ─────────────────────────────────
    sector_buckets = defaultdict(list)
    for sym in all_symbols:
        if sym in mcap_map and mcap_map[sym] > 0:
            sector_buckets[sector_map[sym]].append((sym, mcap_map[sym]))

    candidates = []
    for sec, stocks in sector_buckets.items():
        top = sorted(stocks, key=lambda x: x[1], reverse=True)[:TOP_N_PER_SECTOR]
        candidates.extend(s for s, _ in top)
    candidates = list(set(candidates))

    print(f"  Candidates: {len(candidates)} stocks "
          f"(top {TOP_N_PER_SECTOR}/sector × {len(sector_buckets)} sectors)")

    if len(candidates) < 20:
        print("  Too few candidates for pairs trading.")
        return None

    # ── Load sector/mcap into DuckDB ──────────────────────────────────────────
    sec_vals = ", ".join(f"('{s}', '{sector_map[s].replace(chr(39), chr(39)+chr(39))}')"
                         for s in candidates)
    con.execute("CREATE TABLE sector_map(symbol VARCHAR, sector VARCHAR)")
    con.execute(f"INSERT INTO sector_map VALUES {sec_vals}")

    mcap_vals = ", ".join(f"('{s}', {mcap_map.get(s, 0)})" for s in candidates)
    con.execute("CREATE TABLE mcap_map(symbol VARCHAR, marketCap DOUBLE)")
    con.execute(f"INSERT INTO mcap_map VALUES {mcap_vals}")

    # ── Fetch daily prices (2004 onward) ──────────────────────────────────────
    # Extra year (2004) provides warmup for z-score lookback on first trading year
    print("  Fetching daily prices (2004-present)...")
    price_syms = candidates + ["SPY"]
    price_sym_filter = ", ".join(f"'{s}'" for s in price_syms)

    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({price_sym_filter})
          AND date >= '2004-01-01'
          AND adjClose IS NOT NULL AND adjClose > 0
    """
    count = query_parquet(cr, price_sql, con, "prices_cache",
                          verbose=verbose, limit=10_000_000, timeout=600,
                          memory_mb=4096, threads=2)
    if count == 0:
        print("  No price data found.")
        return None

    print(f"  Price rows: {count:,}")
    con.execute("CREATE INDEX idx_pc_sym_date ON prices_cache(symbol, trade_date)")
    return con


def compute_pair_candidates(con, formation_start, formation_end):
    """Find same-sector pairs by returns correlation during the formation period.

    Returns list of (sym_a, sym_b, sector, correlation, common_days).
    Filters: same sector, corr >= MIN_CORR, >= MIN_CORR_DAYS common days.
    Ranked by correlation (desc).
    """
    fs = formation_start.isoformat()
    fe = formation_end.isoformat()

    rows = con.execute(f"""
        WITH daily_ret AS (
            SELECT p.symbol, p.trade_date,
                (p.adjClose - LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date))
                    / NULLIF(LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date), 0)
                AS ret
            FROM prices_cache p
            WHERE p.trade_date >= '{fs}' AND p.trade_date <= '{fe}'
              AND p.symbol != 'SPY'
              AND p.symbol IN (SELECT symbol FROM sector_map)
        ),
        pair_corr AS (
            SELECT
                a.symbol AS sym_a,
                b.symbol AS sym_b,
                sm_a.sector,
                ROUND(CORR(a.ret, b.ret), 4) AS correlation,
                COUNT(*) AS common_days
            FROM daily_ret a
            JOIN daily_ret b
                ON a.trade_date = b.trade_date AND a.symbol < b.symbol
            JOIN sector_map sm_a ON a.symbol = sm_a.symbol
            JOIN sector_map sm_b ON b.symbol = sm_b.symbol
            WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
              AND sm_a.sector = sm_b.sector
            GROUP BY a.symbol, b.symbol, sm_a.sector
            HAVING COUNT(*) >= {MIN_CORR_DAYS}
        )
        SELECT sym_a, sym_b, sector, correlation, common_days
        FROM pair_corr
        WHERE correlation >= {MIN_CORR}
        ORDER BY correlation DESC
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


def simulate_pair_trades(con, sym_a, sym_b, beta,
                          trading_start, trading_end,
                          mc_a, mc_b, use_costs=True):
    """Simulate active z-score trading for one pair during the trading year.

    Fetches daily z-scores for the pair, then iterates day by day tracking
    entry/exit events. One position open per pair at a time.

    Entry: |z| crosses Z_ENTRY (2.0) from below
    Exit: |z| < Z_EXIT (0.5)  →  convergence
          holding_days >= MAX_HOLD_DAYS  →  time stop
          pair P&L <= LOSS_STOP (-5%)    →  loss stop
          year end                       →  forced close

    Returns list of trade dicts, or None if no prices available.
    Each trade dict:
        entry_date, exit_date, entry_z, exit_z,
        exit_type ('convergence'|'time_stop'|'loss_stop'|'year_end'),
        holding_days, pair_return (after costs)
    """
    # Warmup: need Z_LOOKBACK calendar days before trading_start
    warmup_start = trading_start - timedelta(days=Z_LOOKBACK * 2)

    rows = con.execute(f"""
        WITH log_prices AS (
            SELECT symbol, trade_date, LN(adjClose) AS lp, adjClose
            FROM prices_cache
            WHERE trade_date >= '{warmup_start.isoformat()}'
              AND trade_date <= '{trading_end.isoformat()}'
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
                    / NULLIF(STDDEV(spread) OVER w, 0) AS z_score
            FROM paired
            WINDOW w AS (ORDER BY trade_date
                         ROWS BETWEEN {Z_LOOKBACK - 1} PRECEDING AND CURRENT ROW)
        )
        SELECT trade_date, pa, pb, z_score
        FROM with_z
        WHERE trade_date >= '{trading_start.isoformat()}'
          AND z_score IS NOT NULL
        ORDER BY trade_date
    """).fetchall()

    if not rows:
        return None

    trades = []
    in_position   = False
    entry_idx     = None
    entry_z       = None
    entry_pa      = None
    entry_pb      = None
    direction     = None   # +1 = long_spread (buy A, short B); -1 = short_spread
    trading_day   = 0      # trading days since entry

    for i, row in enumerate(rows):
        trade_date, pa, pb = row[0], float(row[1]), float(row[2])
        z = row[3]
        if z is None or math.isnan(z):
            continue

        if not in_position:
            # Entry condition: |z| first crosses Z_ENTRY
            if abs(z) >= Z_ENTRY:
                in_position = True
                entry_idx   = i
                entry_z     = z
                entry_pa    = pa
                entry_pb    = pb
                direction   = -1 if z > 0 else 1   # short spread if z>0
                trading_day = 0
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
                if use_costs:
                    cost = 2 * tiered_cost(mc_a) + 2 * tiered_cost(mc_b)
                    pair_return -= cost
                pair_return = max(pair_return, -MAX_SINGLE_RETURN)

                trades.append({
                    "entry_date":    rows[entry_idx][0].isoformat()
                                     if hasattr(rows[entry_idx][0], "isoformat")
                                     else str(rows[entry_idx][0]),
                    "exit_date":     trade_date.isoformat()
                                     if hasattr(trade_date, "isoformat")
                                     else str(trade_date),
                    "entry_z":       round(entry_z, 3),
                    "exit_z":        round(z, 3),
                    "exit_type":     exit_type,
                    "holding_days":  trading_day,
                    "pair_return":   round(pair_return, 6),
                })
                in_position = False
                entry_idx   = None
                trading_day = 0

    # Force-close open position at year end
    if in_position and rows:
        last = rows[-1]
        last_date, last_pa, last_pb, last_z = last[0], float(last[1]), float(last[2]), last[3]
        if last_pa and last_pb and entry_pa and entry_pb:
            ret_a = (last_pa - entry_pa) / entry_pa
            ret_b = (last_pb - entry_pb) / entry_pb
            pair_return = direction * (ret_a - ret_b) / 2.0
            if use_costs:
                cost = 2 * tiered_cost(mc_a) + 2 * tiered_cost(mc_b)
                pair_return -= cost
            pair_return = max(pair_return, -MAX_SINGLE_RETURN)
            trading_day = len(rows) - 1 - entry_idx

            trades.append({
                "entry_date":    rows[entry_idx][0].isoformat()
                                 if hasattr(rows[entry_idx][0], "isoformat")
                                 else str(rows[entry_idx][0]),
                "exit_date":     last_date.isoformat()
                                 if hasattr(last_date, "isoformat")
                                 else str(last_date),
                "entry_z":       round(entry_z, 3),
                "exit_z":        round(float(last_z), 3) if last_z else 0.0,
                "exit_type":     "year_end",
                "holding_days":  trading_day,
                "pair_return":   round(pair_return, 6),
            })

    return trades if trades else None


def run_backtest(con, use_costs=True, verbose=False):
    """Run annual z-score pairs backtest (START_YEAR to END_YEAR).

    Returns (annual_results, trade_stats) where:
        annual_results: list of {year, portfolio_return, spy_return, pairs_formed,
                                  pairs_with_trades, total_trades}
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

        if verbose:
            print(f"  {year}: {len(candidates)} corr candidates → "
                  f"{len(selected)} with valid half-life  [{time.time()-t0:.1f}s]")

        # ── Step 3: SPY benchmark return ──────────────────────────────────────
        spy_start_row = con.execute(f"""
            SELECT adjClose FROM prices_cache
            WHERE symbol = 'SPY'
              AND trade_date >= '{trading_start.isoformat()}'
            ORDER BY trade_date ASC LIMIT 1
        """).fetchone()
        spy_end_row = con.execute(f"""
            SELECT adjClose FROM prices_cache
            WHERE symbol = 'SPY'
              AND trade_date <= '{trading_end.isoformat()}'
            ORDER BY trade_date DESC LIMIT 1
        """).fetchone()
        spy_ret = None
        if spy_start_row and spy_end_row:
            spy_s, spy_e = float(spy_start_row[0]), float(spy_end_row[0])
            if spy_s > 0:
                spy_ret = (spy_e - spy_s) / spy_s

        # ── Step 4: Simulate z-score trades for each pair ─────────────────────
        year_trade_returns = []
        pairs_with_trades = 0
        total_year_trades = 0

        for (sym_a, sym_b, sector, corr, beta, _mean, _std, half_life) in selected:
            # Prices must exist at trading start
            pa_row = con.execute(f"""
                SELECT adjClose FROM prices_cache
                WHERE symbol = '{sym_a}'
                  AND trade_date >= '{trading_start.isoformat()}'
                ORDER BY trade_date ASC LIMIT 1
            """).fetchone()
            pb_row = con.execute(f"""
                SELECT adjClose FROM prices_cache
                WHERE symbol = '{sym_b}'
                  AND trade_date >= '{trading_start.isoformat()}'
                ORDER BY trade_date ASC LIMIT 1
            """).fetchone()
            if not pa_row or not pb_row:
                continue
            if float(pa_row[0]) < MIN_LEG_PRICE or float(pb_row[0]) < MIN_LEG_PRICE:
                continue

            mc_a_row = con.execute(
                f"SELECT marketCap FROM mcap_map WHERE symbol = '{sym_a}'"
            ).fetchone()
            mc_b_row = con.execute(
                f"SELECT marketCap FROM mcap_map WHERE symbol = '{sym_b}'"
            ).fetchone()
            mc_a = float(mc_a_row[0]) if mc_a_row else 1e9
            mc_b = float(mc_b_row[0]) if mc_b_row else 1e9

            trades = simulate_pair_trades(
                con, sym_a, sym_b, beta,
                trading_start, trading_end,
                mc_a, mc_b, use_costs=use_costs
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

        # ── Step 5: Annual portfolio return ───────────────────────────────────
        n_formed = len(selected)
        if pairs_with_trades >= MIN_PAIRS_ACTIVE and year_trade_returns:
            port_return = sum(year_trade_returns) / n_formed
        else:
            port_return = 0.0  # Cash period

        annual_results.append({
            "year":             year,
            "portfolio_return": round(port_return, 6),
            "spy_return":       round(spy_ret, 6) if spy_ret is not None else None,
            "pairs_formed":     n_formed,
            "pairs_with_trades": pairs_with_trades,
            "total_trades":     total_year_trades,
        })

        if verbose:
            spy_s = f"{spy_ret*100:.1f}%" if spy_ret is not None else "N/A"
            cash_marker = " [CASH]" if pairs_with_trades < MIN_PAIRS_ACTIVE else ""
            print(f"    → {pairs_with_trades}/{n_formed} pairs traded, "
                  f"{total_year_trades} trades, "
                  f"port={port_return*100:.1f}%, spy={spy_s}{cash_marker}")

    # ── Aggregate trade statistics ─────────────────────────────────────────────
    trade_stats = {}
    if all_trades:
        n = len(all_trades)
        n_conv    = sum(1 for t in all_trades if t["exit_type"] == "convergence")
        n_time    = sum(1 for t in all_trades if t["exit_type"] == "time_stop")
        n_loss    = sum(1 for t in all_trades if t["exit_type"] == "loss_stop")
        n_end     = sum(1 for t in all_trades if t["exit_type"] == "year_end")
        avg_hold  = sum(t["holding_days"] for t in all_trades) / n
        avg_pl    = sum(t["pair_return"] for t in all_trades) / n
        trade_stats = {
            "total_trades":        n,
            "convergence_rate":    round(n_conv * 100.0 / n, 1),
            "time_stop_rate":      round(n_time * 100.0 / n, 1),
            "loss_stop_rate":      round(n_loss * 100.0 / n, 1),
            "year_end_rate":       round(n_end  * 100.0 / n, 1),
            "avg_holding_days":    round(avg_hold, 1),
            "avg_trade_return_pct":round(avg_pl * 100, 3),
        }

    return annual_results, trade_stats


def build_output(m, annual, valid, results, trade_stats, universe_name):
    """Build JSON output in standard exchange_comparison format plus trade stats."""
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

    cash_periods  = sum(1 for r in results if r["pairs_with_trades"] < MIN_PAIRS_ACTIVE)
    invested_yrs  = [r for r in results if r["pairs_with_trades"] >= MIN_PAIRS_ACTIVE]
    avg_pairs     = (round(sum(r["pairs_with_trades"] for r in invested_yrs) / len(invested_yrs), 1)
                     if invested_yrs else 0)
    total_trades  = sum(r["total_trades"] for r in results)
    trades_per_yr = round(total_trades / len(results), 1) if results else 0

    return {
        "universe":             universe_name,
        "n_years":              len(valid),
        "years":                f"{START_YEAR}-{END_YEAR}",
        "frequency":            "daily_zscore",
        "cash_periods":         cash_periods,
        "invested_periods":     len(invested_yrs),
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
                "pairs_with_trades": r["pairs_with_trades"],
                "total_trades":      r["total_trades"],
            }
            for r in results
        ],
    }


def run_single(cr, exchanges, universe_name, use_costs, risk_free_rate,
               verbose, output_path=None):
    """Run backtest for a single exchange set. Returns output dict or None."""
    signal_desc = (
        f"Same-sector corr > {MIN_CORR}, half-life {HALF_LIFE_MIN}-{HALF_LIFE_MAX}d, "
        f"top {MAX_PAIRS} pairs, z-entry > {Z_ENTRY}, z-exit < {Z_EXIT}, "
        f"max hold {MAX_HOLD_DAYS}d, loss stop {LOSS_STOP*100:.0f}%"
    )
    print_header("Z-SCORE PAIRS TRADING BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Costs: {'size-tiered×4 legs' if use_costs else 'none'}  "
          f"RFR: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, verbose=verbose)
    if con is None:
        print("No data available. Skipping.")
        return None
    print(f"Data fetched in {time.time()-t0:.0f}s")

    print(f"\nPhase 2: Running daily z-score simulation ({START_YEAR}-{END_YEAR})...")
    t1 = time.time()
    results, trade_stats = run_backtest(con, use_costs=use_costs, verbose=verbose)
    print(f"Backtest completed in {time.time()-t1:.0f}s")

    valid = [r for r in results if r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns  = [r["spy_return"]       for r in valid]

    m = compute_metrics(port_returns, spy_returns, 1, risk_free_rate=risk_free_rate)
    print(format_metrics(m, "Pairs (z-score)", "S&P 500"))

    # Trade statistics summary
    cash_periods = sum(1 for r in results if r["pairs_with_trades"] < MIN_PAIRS_ACTIVE)
    print(f"\n  Cash periods: {cash_periods}/{len(results)}")
    if trade_stats:
        print(f"  Total trades: {trade_stats['total_trades']}")
        print(f"  Convergence rate: {trade_stats['convergence_rate']}%")
        print(f"  Time stop rate:   {trade_stats['time_stop_rate']}%")
        print(f"  Loss stop rate:   {trade_stats['loss_stop_rate']}%")
        print(f"  Avg hold:         {trade_stats['avg_holding_days']} days")
        print(f"  Avg trade return: {trade_stats['avg_trade_return_pct']}%")

    annual = compute_annual_returns(port_returns, spy_returns,
                                    [str(r["year"]) for r in valid], 1)
    if annual:
        print(f"\n  {'Year':<8} {'Port':>8} {'SPY':>8} {'Excess':>8} "
              f"{'Trades':>8} {'Conv%':>7}")
        print("  " + "-" * 55)
        for i, ar in enumerate(annual):
            r = valid[i] if i < len(valid) else {}
            n_tr = r.get("total_trades", "-")
            conv = (round(sum(1 for t in (trade_stats or {}) if False) * 100, 1)
                    if False else "-")
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>7.1f}%"
                  f" {ar['benchmark']*100:>7.1f}%"
                  f" {ar['excess']*100:>+7.1f}% {str(n_tr):>8}")

    print(f"\n  Total time: {time.time()-t0:.0f}s")

    out = build_output(m, annual, valid, results, trade_stats, universe_name)
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
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    use_costs = not args.no_costs

    # ── Global mode ────────────────────────────────────────────────────────────
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all eligible exchange presets")
        print("=" * 65)
        print("\n  Eligibility notes:")
        print("  - China/India: short-selling restrictions apply")
        print("  - Results are theoretical for restricted markets")
        print()

        # Eligible exchanges (from pairs-fundamentals baseline + daily signal improvement):
        # Excluded: SET (90% cash), SES (90% cash) — insufficient correlated pairs
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
                                    use_costs, rfr, args.verbose, out_path)
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
            cp = r.get("cash_periods", 0)
            cash_pct  = round(cp * 100 / n, 0) if n > 0 else 0
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
               args.verbose, args.output)


if __name__ == "__main__":
    main()
