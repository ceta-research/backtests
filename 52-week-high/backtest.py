#!/usr/bin/env python3
"""
52-Week High Proximity Backtest

Buys the top 30 stocks nearest to their 52-week high (highest proximity ratio)
within each exchange. Quarterly rebalancing.

Signal: close / MAX(high over 252 trading days) per George & Hwang (2004).
        Both split-adjusted, neither dividend-adjusted; adjClose prices returns only.
        Signal bar: the latest bar in [R - 10d, R] with volume > 0 (none: not ranked);
        the high spans the 252 bars ending there, untraded (zero or NULL volume) bars included.
        Higher ratio = stock closer to its 52-week high = stronger momentum signal.
        Exact ties rank by larger market cap, then symbol.
        No skip-month needed (unlike classic 12-1 momentum).

Universe: ordinary shares only (data_utils universe rules ur-4, METHODOLOGY.md): preferreds, notes, units,
        funds, US ADRs, US trusts and LPs, JPX REITs and the hand list's lines are out before the screen; on SAO a
        company holds one line; a line whose earlier history copies another line's enters on its first own day
        (ur-4 line_starts), its 252-bar frame starting there too when those bars are another company's or
        instrument's (lookback_clip); a line whose later rows are not its listing's leaves after its last own day
        (line_ends). MCap > exchange threshold (key_metrics FY, check M's cap_used). Price > $1.
Portfolio: Equal weight top 30. Cash if < 10 qualify.
Rebalancing: Quarterly (Jan/Apr/Jul/Oct), from 2000 to the latest close (METHODOLOGY.md, "Run Window").
Costs: costs.tiered_cost on each booked name's cap_used, its USD tiers converted at the leg's
        costs.get_fx_per_usd (cost_model in the results). Excludes: ASX, SAO (adjClose artifacts).

Academic basis:
  George, T. & Hwang, C. (2004). "The 52-Week High and Momentum Investing."
  Journal of Finance, 59(5), 2145-2176.

Usage:
    python3 52-week-high/backtest.py                           # US default, to the latest close
    python3 52-week-high/backtest.py --preset india --end-date YYYY-MM-DD --output 52-week-high/results/returns_NSE.json
    python3 52-week-high/backtest.py --global --end-year 2025 --output 52-week-high/results/exchange_comparison.json
    python3 52-week-high/backtest.py --preset us --no-costs --verbose

    # --end-year YYYY ends the grid in that year with no partial period. Flags: --data-checks on|uncapped
    # (default on), --full-holdings, --exit-method drop|ltp|both
"""

import argparse
import duckdb
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from functools import partial

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import (query_parquet, get_prices, RunWindow, with_partial_year, filter_returns,
                        entry_buyable, entry_usable, LtpExits, add_exit_method_arg,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations, utc_epoch,
                        campaign_listing_condition, campaign_listing_label, isin_dedup_sql, load_isin_map,
                        DATA_CHECKS_V3, DataChecks, clean_price_cache, mark_nvdr_twins, load_cap_check,
                        load_splits, empty_cap_check, cap_used_sql, CAP_FAIL_TEST,
                        UniverseRules, load_universe, UNIVERSE_RULES_V4, without_lines_sql)
from metrics import (compute_metrics, format_metrics,
                     period_accounting)
from costs import tiered_cost, apply_costs, get_fx_per_usd
from cli_utils import (add_common_args, add_end_date_arg, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
HIGH_WINDOW_DAYS = 365       # Calendar days for 252 trading day window
HIGH_WINDOW_FETCH = 390      # Days before rebalance to fetch prices (252td + buffer)
MIN_HISTORY_ROWS = 100       # Minimum trading days required to compute valid 52w high
SIGNAL_TOLERANCE_DAYS = 10   # Signal bar = latest bar in [R - 10d, R] with volume > 0; covers weekends + year-end holidays
MAX_STOCKS = 30              # Top N by proximity, equal weight
MIN_STOCKS = 10              # Hold cash if fewer qualify
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]   # Jan / Apr / Jul / Oct
MIN_ENTRY_PRICE = 1.0        # Skip sub-$1 entry prices (price data artifacts)
MAX_PROXIMITY = 1.1          # Proximity > 1.1 likely a data error (close > 52w high)
# No return cap: data checks dc-3 (traded bars, NVDR twins, market-cap check at the floor, the reviewed
# hand list) clean the inputs, and every kept move over +100% or under -50% is listed in the results.
DATA_CHECKS = DATA_CHECKS_V3
UNIVERSE_RULES = UNIVERSE_RULES_V4

# Development compute (max resources)
FETCH_MEMORY_MB = 4096
FETCH_THREADS = 2


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, mktcap_min=None, dc=None, ur=None,
                       win=None):
    """Fetch all data needed for 52-week high proximity backtest.

    Populates DuckDB tables:
        universe(symbol VARCHAR): ordinary lines only
        metrics_cache(symbol, marketCap, filing_epoch, period)
            -- used for point-in-time market cap filter only
        prices_cache(symbol, trade_epoch, adjClose, close, high, volume)
            -- extended window: 52W lookback + entry date per rebalance date
        cap_check (empty unless dc is on), and with dc on: splits, prices_dropped

    dc: a data_utils.DataChecks; when on, the fetch runs checks T, N and M (mktcap_min is the floor). Check T
    cleans the grid's windows [d, d + 11] only: a lookback keeps the rows outside them (untraded rows among them).
    ur: a data_utils.UniverseRules (UNIVERSE_RULES), which records the lines the ordinary-shares rule removes.
    win: the leg's data_utils.RunWindow; its fetch_end cuts the price windows at the run's end date.
    Returns DuckDB connection or None.
    """
    checks = dc is not None and dc.on
    ur = ur if ur is not None else UniverseRules(UNIVERSE_RULES)
    win = win if win is not None else RunWindow.typed()
    # Campaign listing rule; sym_filter_sql carries it into the market-cap and price fetches.
    listing_cond = campaign_listing_condition(exchanges)
    listing_and = f" AND {listing_cond}" if listing_cond else ""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter}){listing_and}"
    else:
        exchange_where = ""
    if listing_cond:
        print(f"  Listing filter: {listing_cond}")

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='8GB'")

    # 1. Universe: ordinary shares only (the CASE names why a line is not one; those lines stay out)
    if load_universe(client, con, exchange_where, ur, UNIVERSE_RULES, verbose=verbose) is None:
        return None

    if exchanges:
        sym_filter_sql = (
            f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}){listing_and})"
        )
    else:
        sym_filter_sql = "1=1"
    # symbol -> ISIN (+ live, home-currency flags) for screen_stocks' same-security dedup, every leg.
    load_isin_map(client, con, exchanges, sym_filter_sql, verbose=verbose)
    if checks:   # check N: SET NVDR lines share their ordinary line's security key
        dc.record_twins_linked(mark_nvdr_twins(con))
    ur.mark_twins(con)   # ur-4: an SAO preferred or unit line stands in for an unpriced ordinary line

    # 2. Historical market cap (from key_metrics FY filings, point-in-time)
    print("  Fetching historical market cap...")
    metrics_sql = f"""
        SELECT symbol, marketCap, dateEpoch as filing_epoch, period
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND marketCap > 0
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache", verbose=verbose,
                          memory_mb=FETCH_MEMORY_MB, threads=FETCH_THREADS)
    print(f"    -> {count} market cap rows")

    # Check M (the floor reads cap_used) and the splits check F flags; cap_check stays empty when off.
    if checks:
        print("  Fetching market-cap check inputs and splits...")
        dc.record_cap(load_cap_check(client, con, sym_filter_sql, mktcap_min, dc.cfg, verbose=verbose))
        load_splits(client, con, sym_filter_sql, verbose=verbose)
    else:
        empty_cap_check(con)

    # 3. Prices — HIGH_WINDOW_FETCH days before each rebalance through entry end (R+11)
    # adjClose prices entry/exit returns; close and high (split-adjusted, dividends left in) make the signal.
    # Window per rebalance date: [R - HIGH_WINDOW_FETCH, R + 11]
    # This covers: 252 trading days of high data + entry prices.
    # Exit prices for period i are covered by period i+1's entry window.
    print("  Fetching prices (adjClose + close/high for 52w window)...")
    date_conditions = []
    for d in rebalance_dates:
        fetch_start = d - timedelta(days=HIGH_WINDOW_FETCH)
        entry_end = win.fetch_end(d, 11)  # covers get_prices' [d+1, d+11] with offset_days=1
        date_conditions.append(
            f"(date >= '{fetch_start.isoformat()}' AND date <= '{entry_end.isoformat()}')"
        )
    date_filter = " OR ".join(date_conditions)

    # Build benchmark symbol list (local index + SPY fallback)
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(bench_symbols)

    # Symbol pre-filter: only symbols with FY key_metrics (MCap data).
    exchange_fy_filter = f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""
    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, close, high, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR symbol = 'MTUM'
            OR symbol IN (
                SELECT DISTINCT symbol FROM key_metrics
                WHERE period = 'FY'
                  AND marketCap IS NOT NULL
                  AND marketCap > 0
                  {exchange_fy_filter}
            )
          )
          AND adjClose > 0
          AND high > 0
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=15000000, timeout=600,
                          memory_mb=FETCH_MEMORY_MB, threads=FETCH_THREADS)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    if checks:   # check T: closed days, zero-volume and flat lines, non-positive prices, sibling-class gaps
        dc.record_clean(clean_price_cache(con, bench_symbols, rebalance_dates, dc.cfg, siblings="cap_inputs"))
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows (adjClose + close/high, {HIGH_WINDOW_FETCH}-day window per date)")

    return con


def _frame_start_sql(clips):
    """clips: {symbol: first own day} of the leg's lookback_clip lines (ur-4), whose earlier bars are another
    company's or instrument's. A condition on a prices_cache row: such a line's frame starts on its first own day."""
    if not clips:
        return "TRUE"
    whens = " ".join("WHEN '{}' THEN {}".format(s.replace("'", "''"), utc_epoch(date.fromisoformat(d)))
                     for s, d in sorted(clips.items()))
    return f"trade_epoch >= CASE symbol {whens} ELSE 0 END"


def _candidates_sql(target_date, cap_test, events=False, clips=None):
    """The screen's candidates on target_date among the lines in `universe`, each with its proximity ratio and
    market cap.

    The floor reads cap_used (check M): each line's latest FY row filed by the 45-day cutoff (the first `?`), its
    cap over check M's divisor, NULL for an ineligible row. cap_test is the floor clause ("km.cap_used > ?", or
    CAP_FAIL_TEST for M's events, which also read km_epoch and raw_cap). The candidates carry cap_used as
    marketCap, so the tie-break and the cost tier read it. clips: _frame_start_sql's.

    proximity_ratio = close(S) / MAX(high) over the 252 bars ending at S, untraded (zero or NULL volume)
    bars included, S being the latest bar in [R - 10d, R] with volume > 0; a line with no such bar is not ranked.

    Price basis on both sides: split-adjusted, dividends left in. adjClose is back-adjusted for
    every later dividend, so it would rank a name by what it paid out after the rebalance date.

    Higher ratio = stock is closer to its 52-week high.
    Ratio of 1.0 means the stock closed on its 52-week high.
    """
    extra = ", km.filing_epoch AS km_epoch, km.marketCap AS raw_cap" if events else ""
    start_epoch = utc_epoch(target_date - timedelta(days=HIGH_WINDOW_FETCH))
    # Signal reads only bars on/before R; entry is the first bar from R+1, so no bar after entry leaks in.
    target_epoch = utc_epoch(target_date)
    signal_floor_epoch = utc_epoch(target_date - timedelta(days=SIGNAL_TOLERANCE_DAYS))

    # Duplicates sort by close, high, volume DESC NULLS LAST: fixed frames, untraded twin outside the traded bar's frame.
    return f"""
        WITH km AS (
            SELECT m.symbol, m.marketCap, m.filing_epoch,
                {cap_used_sql()} AS cap_used,
                ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.filing_epoch DESC, m.marketCap DESC NULLS LAST) AS rn
            FROM metrics_cache m
            LEFT JOIN cap_check cc ON cc.symbol = m.symbol AND cc.filing_epoch = m.filing_epoch
            WHERE m.filing_epoch <= ?
        ),
        eligible AS (
            SELECT km.symbol, km.cap_used AS marketCap{extra}
            FROM km
            WHERE km.rn = 1
              AND km.symbol IN (SELECT symbol FROM universe)
              AND {cap_test}
        ),
        windowed AS (
            SELECT symbol, trade_epoch, close, high, volume,
                MAX(high) OVER (
                    PARTITION BY symbol ORDER BY trade_epoch, close, high, volume DESC NULLS LAST
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS high_52w,
                COUNT(*) OVER (
                    PARTITION BY symbol ORDER BY trade_epoch, close, high, volume DESC NULLS LAST
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS row_count
            FROM prices_cache
            WHERE symbol IN (SELECT symbol FROM eligible)
              AND trade_epoch >= {start_epoch}
              AND trade_epoch <= {target_epoch}
              AND {_frame_start_sql(clips)}
        ),
        latest AS (
            SELECT symbol, close, high_52w, row_count
            FROM windowed
            WHERE trade_epoch <= {target_epoch}
              AND trade_epoch >= {signal_floor_epoch}
              AND volume > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch DESC, close DESC NULLS LAST,
                                       high_52w DESC NULLS LAST, row_count DESC) = 1
        )
        SELECT
            l.symbol,
            l.close / l.high_52w AS proximity_ratio,
            e.* EXCLUDE (symbol),
            l.close,
            l.high_52w,
            l.row_count
        FROM latest l
        JOIN eligible e ON e.symbol = l.symbol
        WHERE l.high_52w > 0
          AND l.close > 0
          AND l.row_count >= {MIN_HISTORY_ROWS}
          AND l.close / l.high_52w <= {MAX_PROXIMITY}
    """


def screen_stocks(con, target_date, mktcap_min, verbose=False, offset_days=1, dc=None, ur=None):
    """Screen for 52-week high proximity stocks at target_date (_candidates_sql), the top MAX_STOCKS by
    proximity ratio after the security dedup. Exact ties rank by larger cap_used, then symbol.

    dc (data_utils.DataChecks): when on, NVDR twins share a security (check N); it also records the
    screen's twin and market-cap events. ur-4's class twins share a security in every mode, so an SAO
    company holds one line. ur: the leg's data_utils.UniverseRules (its removals act at the fetch, its
    line_starts, line_ends and lookback_clip here; it records the class twins each screen picks).
    Returns list of (symbol, market_cap, proximity_ratio), the market cap being cap_used.
    """
    cutoff_epoch = utc_epoch(target_date - timedelta(days=45))
    late = ur.not_started(target_date) if ur is not None else []
    gone = ur.ended(target_date) if ur is not None else []
    left_out = late + gone
    clips = {s: ur.starts[s][0] for s in ur.clips} if ur is not None else {}
    sql = partial(_candidates_sql, target_date, clips=clips)
    candidates = without_lines_sql(sql("km.cap_used > ?"), left_out)
    params = [cutoff_epoch, mktcap_min]
    # One row per security BEFORE the top-N cut, so a collapsed twin frees its slot for the next name.
    rows = con.execute(f"""
        SELECT symbol, marketCap, proximity_ratio
        FROM ({isin_dedup_sql(candidates, target_date, offset_days, MIN_ENTRY_PRICE,
                              twin_key=(dc is not None and dc.on) or bool(UNIVERSE_RULES.get("class_twins")))})
        ORDER BY proximity_ratio DESC, marketCap DESC NULLS LAST, symbol
        LIMIT ?
    """, params + [MAX_STOCKS]).fetchall()

    picked = [(r[0], r[1], r[2]) for r in rows]
    if ur is not None:
        ur.stand_ins([s for s, _, _ in picked], target_date)
    if dc is not None:
        dc.twins(con, candidates, params, target_date, offset_days, MIN_ENTRY_PRICE, [s for s, _, _ in picked])
        dc.caps(con, without_lines_sql(sql(CAP_FAIL_TEST, events=True), left_out), params + [mktcap_min],
                target_date, mktcap_min)
    if verbose and picked:
        print(f"    Selected: {len(picked)} (prox range: {picked[-1][2]:.3f}-{picked[0][2]:.3f})")
    return picked


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY", ltp=None, ur=None, win=None, fx=1.0, exit_method="drop",
                 dc=None):
    """Run 52-week high proximity backtest. Returns list of period result dicts.

    dc: the leg's data_utils.DataChecks. Without one, no check runs (the "uncapped" mode); either way there is
    no return cap. ur: the leg's data_utils.UniverseRules, passed to the screen. win: the leg's
    data_utils.RunWindow (its grid made rebalance_dates): the partial exit is priced at the close on the end date
    (the caller marks the rows: RunWindow.mark). fx: the leg's costs.get_fx_per_usd, which converts the USD cost
    tiers; each booked name pays its cap_used's tier, recorded as cost_rate in holdings_full."""
    results = []
    dc = dc if dc is not None else DataChecks("uncapped", DATA_CHECKS)
    win = win if win is not None else RunWindow.typed()

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]
        x_off = win.exit_offset(exit_date, offset_days)

        portfolio = screen_stocks(con, entry_date, mktcap_min, verbose=verbose, offset_days=offset_days, dc=dc, ur=ur)

        bench_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days, exit_offset_days=x_off)

        if len(portfolio) < MIN_STOCKS:
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": bench_return,
                "stocks_held": 0,
                "screened": len(portfolio),
                "entry_buyable": None,
                "entry_unpriced": None,
                "min_stocks": MIN_STOCKS,
                **dc.row(entry_date, screened=True, priced=False),
                "holdings": f"CASH ({len(portfolio)} passed, need {MIN_STOCKS})",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _, _ in portfolio]
        mcaps = {s: mc for s, mc, _ in portfolio}

        entry_prices = get_prices(con, symbols, entry_date, offset_days=offset_days)
        exit_prices = get_prices(con, symbols, exit_date, offset_days=x_off)

        symbol_data = [(sym, entry_prices.get(sym), exit_prices.get(sym), mcaps.get(sym))
                       for sym in symbols]
        # screened names the strategy can't buy at entry (no bar, or under the floor): the book is the rest
        unpriced = sorted(s for s in symbols if not entry_usable(entry_prices.get(s), MIN_ENTRY_PRICE))
        # LTP fills only for periods that clear the entry-side cash guard below.
        ltp_fill = (ltp.fill(symbols, entry_prices, exit_prices, entry_date, exit_date,
                             offset_days=offset_days, min_entry_price=MIN_ENTRY_PRICE)
                    if ltp and entry_buyable(symbol_data, min_entry_price=MIN_ENTRY_PRICE) >= MIN_STOCKS else {})
        exit_ltp = {**exit_prices, **ltp_fill}

        def book(xmap, verbose=verbose, record=True):
            # symbol_data with the exit slot taken from xmap; each name's one-way rate, None on a no-costs run
            clean, skipped = filter_returns([(sym, ep, xmap.get(sym), mcap) for sym, ep, _, mcap in symbol_data],
                                            min_entry_price=MIN_ENTRY_PRICE,
                                            max_single_return=None,   # no return cap (data checks dc-3)
                                            verbose=verbose)
            clean = dc.hand(clean, entry_date, exit_date, filled=ltp_fill, record=record)   # dc-3's hand list
            rates = {s: tiered_cost(mc, fx_per_usd=fx) if use_costs else None for s, _, mc in clean}
            return clean, [rr if rates[s] is None else apply_costs(rr, rates[s]) for s, rr, _ in clean], rates

        booked = exit_ltp if exit_method == "ltp" else exit_prices
        clean, returns, rates = book(booked)

        # The cash rule has to be re-checked HERE, not just on the screen count.
        # Screening can pass 30 names while only a handful of them have a usable
        # ENTRY price, and averaging 1-4 survivors reports a single stock's year
        # as a diversified portfolio return.
        #
        # Checked on `buyable`, NOT on len(clean). filter_returns also drops a
        # name for a missing EXIT price, which is not knowable on the rebalance
        # date (this topic sets no return cap; a cap would be a second such drop).
        # Deciding cash from those is look-ahead and it flatters: a period the
        # strategy really held and really lost gets rewritten as a flat 0.0%. If
        # the book cleared the floor at entry the strategy ENTERED, so the
        # exit-side survivors are averaged below exactly as they were before this
        # guard existed. That survivor-averaging has its own known weakness; it
        # is pre-existing, logged in DATA_QUALITY_ISSUES.md, and deliberately
        # not changed here.
        buyable = entry_buyable(symbol_data, min_entry_price=MIN_ENTRY_PRICE)
        if buyable < MIN_STOCKS:
            # window bars only: T's bar events, no book to flag
            dc.prices(con, entry_date, exit_date, symbol_data, offset_days, MIN_ENTRY_PRICE, held=False,
                      exit_offset_days=x_off)
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": bench_return,
                "stocks_held": 0,
                "screened": len(portfolio),
                "entry_buyable": buyable,
                "entry_unpriced": unpriced,
                "min_stocks": MIN_STOCKS,
                **dc.row(entry_date, screened=True, priced=True),
                "holdings": f"CASH ({buyable} buyable at entry of {len(portfolio)} screened)",
            })
            if verbose:
                print(f"    {entry_date}: only {buyable} of {len(portfolio)} screened "
                      f"names were buyable at entry (< {MIN_STOCKS}), CASH")
            continue

        port_return = sum(returns) / len(returns) if returns else 0.0
        avg_prox = sum(p for _, _, p in portfolio) / len(portfolio)
        # window bars only (never the LTP fills): T's bar events, L and F's flags
        dc.prices(con, entry_date, exit_date, symbol_data, offset_days, MIN_ENTRY_PRICE, held=True,
                  exit_offset_days=x_off)

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(bench_return, 6) if bench_return is not None else None,
            "stocks_held": len(returns),
            "screened": len(portfolio),
            "entry_buyable": buyable,
            "entry_unpriced": unpriced,
            "min_stocks": MIN_STOCKS,
            **dc.row(entry_date, screened=True, priced=True),
            "avg_proximity_ratio": round(avg_prox, 4),
            "holdings": ",".join(sym for sym, _, _ in clean),
            **dc.holdings_full(con, clean, entry_prices, booked, ltp_fill if exit_method == "ltp" else {},
                               entry_date, exit_date, offset_days, ltp, exit_offset_days=x_off, cost_rates=rates),
        })
        if exit_method == "both":
            c_ltp, r_ltp, _ = book(exit_ltp, verbose=False, record=False)
            results[-1].update({"portfolio_return_ltp": round(sum(r_ltp) / len(r_ltp), 6) if r_ltp else 0.0,
                                "stocks_held_ltp": len(r_ltp), "ltp_filled": sum(1 for s, _, _ in c_ltp if s in ltp_fill)})

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks (avg prox={avg_prox:.3f}), "
                  f"port={port_return * 100:.1f}%, "
                  f"bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency, periods_per_year,
                 cash_periods, avg_stocks):
    """Build JSON output in standard format."""
    p = metrics["portfolio"]
    b = metrics["benchmark"]
    c = metrics["comparison"]

    def pct(v):
        return round(v * 100, 2) if v is not None else None

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    def fmt(s):
        return {
            "total_return": pct(s.get("total_return")),
            "cagr": pct(s.get("cagr")),
            "max_drawdown": pct(s.get("max_drawdown")),
            "annualized_volatility": pct(s.get("annualized_volatility")),
            "sharpe_ratio": rnd(s.get("sharpe_ratio")),
            "sortino_ratio": rnd(s.get("sortino_ratio")),
            "calmar_ratio": rnd(s.get("calmar_ratio")),
            "var_95": pct(s.get("var_95")),
            "max_consecutive_losses": s.get("max_consecutive_losses"),
            "pct_negative_periods": pct(s.get("pct_negative_periods")),
        }

    return {
        "universe": universe_name,
        # B006: n_periods, total_rebalances, cash_periods, invested_periods and
        # the window-provenance block. `executed` is every rebalance the
        # strategy ran; `valid` is the benchmark-priced subset metrics use. No
        # key below may re-declare one of these -- gate 8 enforces that.
        **period_accounting(
            [r for r in results if r["portfolio_return"] is not None],
            valid, cash_periods, universe_name=universe_name),
        "years": round(len(valid) / periods_per_year, 1),
        "frequency": frequency,
        "avg_stocks_when_invested": round(avg_stocks, 1),
        "period_data": results,
        "portfolio": fmt(p),
        "spy": fmt(b),
        "comparison": {
            "excess_cagr": pct(c.get("excess_cagr")),
            "win_rate": pct(c.get("win_rate")),
            "information_ratio": rnd(c.get("information_ratio")),
            "tracking_error": pct(c.get("tracking_error")),
            "up_capture": pct(c.get("up_capture")),
            "down_capture": pct(c.get("down_capture")),
            "beta": rnd(c.get("beta")),
            "alpha": pct(c.get("alpha")),
        },
        "excess_cagr": pct(c.get("excess_cagr")),
        "win_rate_vs_spy": pct(c.get("win_rate")),
        "annual_returns": [
            {"year": ar["year"],
             "portfolio": round(ar["portfolio"] * 100, 2),
             "spy": round(ar["benchmark"] * 100, 2),
             "excess": round(ar["excess"] * 100, 2)}
            for ar in annual
        ],
    }


def run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, verbose, output_path=None, offset_days=1, exit_method="drop",
               data_checks="on", full_holdings=False, *, win):
    """Run backtest for a single exchange set. Returns output dict or None.

    data_checks: "on" (data checks dc-3), or "uncapped" (no check and no cap: attribution only).
    full_holdings: every invested period row also lists each booked name (holdings_full) with the one-way
    rate it paid (cost_rate). win: the run's data_utils.RunWindow (main builds it from --end-date or --end-year)."""
    win = win.for_leg(universe_name)
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]
    dc = DataChecks(data_checks, DATA_CHECKS, full_holdings=full_holdings)
    ur = UniverseRules(UNIVERSE_RULES)
    fx = get_fx_per_usd(exchanges)   # local currency per USD: the cost tiers' breakpoints in the leg's currency
    mktcap_threshold = get_mktcap_threshold(exchanges)

    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "next-day close (MOC)" if offset_days == 1 else "same-day close"

    signal_desc = (
        f"Top {MAX_STOCKS} by proximity ratio (close/52w-high) | MCap>{mktcap_threshold/1e9:.0f}B local"
    )
    print_header("52-WEEK HIGH PROXIMITY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {f'size-tiered (tiers x{fx:g} local/USD)' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = win.grid(2000, frequency, months=DEFAULT_REBALANCE_MONTHS, offset_days=offset_days)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose,
                             mktcap_min=mktcap_threshold, dc=dc, ur=ur, win=win)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    print(f"\nPhase 2: Running {frequency} backtest ({rebalance_dates[0]} to {rebalance_dates[-1]})...")
    t1 = time.time()
    # LTP reads remote stock_eod: check T's rules (dc-3's rule S among them) clean a fill on the remote path only.
    ltp = (LtpExits(cr, con, verbose=verbose, traded_only=dc.on, end_date=win.end_date, cfg=dc.cfg)
           if exit_method != "drop" else None)
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol,
                           ltp=ltp, ur=ur, win=win, fx=fx, exit_method=exit_method, dc=dc)
    win.mark(results)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")
    print(f"  {dc.summary()}")
    ub = ur.block()
    print(f"  Universe rules: {ub['ordinary_shares']['removed_count']} lines not ordinary shares "
          f"{ub['ordinary_shares']['by_reason']}")

    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    fractions = win.fractions(valid)
    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate, fractions=fractions)
    print(format_metrics(metrics, "52W-High Proximity", benchmark_name))
    ltp_block = (ltp.results_block(exit_method, valid, periods_per_year, risk_free_rate, fractions=fractions)
                 if ltp else None)
    if ltp_block:
        dc.annotate_ltp(con, ltp, ltp_block)   # F's flags on the fills, inside the ltp block

    # B006: count over `executed` (every rebalance the strategy actually ran),
    # not `valid` (only those the benchmark can also price). Keeps the honest
    # full-window cash rate: an OSL leg that sat in cash before ^OSEAX starts
    # still sat in cash. invested_periods is derived in period_accounting.
    executed = [r for r in results if r["portfolio_return"] is not None]
    cash_periods = sum(1 for r in executed if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in executed if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(executed)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    annual, partial_year = win.annual(valid, periods_per_year)
    if annual:
        bench_label = benchmark_name[:10]
        print(f"\n  {'Year':<8} {'52W-High':>10} {bench_label:>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in with_partial_year(annual, partial_year):
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks)
    output.update({**win.fields(con, benchmark_symbol, valid, periods_per_year, partial_year, offset_days),
                   "listing_filter": campaign_listing_label(exchanges), "isin_dedup": True})
    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both
    output.update({
        "universe_rules": ub,            # the fetch's removals and the screen's stand-ins: equal in drop, ltp and both
        "cost_model": {"function": "costs.tiered_cost", "fx_per_usd": fx} if use_costs else None,
    })
    if ltp_block:
        output.update({"exit_method": exit_method, "ltp": ltp_block})

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="52-Week High Proximity multi-exchange backtest")
    add_common_args(parser)
    add_end_date_arg(parser)
    add_exit_method_arg(parser)
    parser.add_argument("--data-checks", choices=DataChecks.MODES, default="on",
                        help="Data checks dc-3 (default on). 'uncapped' turns every check off and sets no "
                             "return cap: the same-day attribution arm only, never published")
    parser.add_argument("--full-holdings", action="store_true",
                        help="Also list every booked name per invested period (holdings_full: entry and exit "
                             "bars and prices, return before costs, the one-way rate it paid), for review")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("52-week-high", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    offset_days = 0 if args.no_next_day else 1

    # --global mode: loop all eligible exchange presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}

        # ASX excluded (adjClose split artifacts affect high column too).
        # SAO excluded (adjClose artifacts).
        # PAR excluded (only 1 symbol with FY key_metrics MCap data — pipeline gap).
        # TWO: no FY key_metrics data → run TAI only, TWO stocks auto-excluded by MCap filter.
        # SES: using FMP code SES (not SGX — wrong in cli_utils preset).
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),
            ("uk",          ["LSE"]),
            ("germany",     ["XETRA"]),
            ("japan",       ["JPX"]),
            ("china",       ["SHZ", "SHH"]),
            ("hongkong",    ["HKSE"]),
            ("korea",       ["KSC"]),
            ("taiwan",      ["TAI"]),
            ("canada",      ["TSX"]),
            ("switzerland", ["SIX"]),
            ("sweden",      ["STO"]),
            ("thailand",    ["SET"]),
            ("southafrica", ["JNB"]),
            ("norway",      ["OSL"]),
            ("italy",       ["MIL"]),
            ("malaysia",    ["KLS"]),
            ("singapore",   ["SES"]),
        ]

        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        # one end date for every leg: the latest day every leg traded, or the pin
        win = RunWindow.from_args(args, cr, {"_".join(ex): ex for _, ex in presets_to_run})

        for preset_name, preset_exchanges in presets_to_run:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"returns_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            try:
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, args.verbose, output_path,
                                    offset_days=offset_days, exit_method=args.exit_method,
                                    data_checks=args.data_checks, full_holdings=args.full_holdings, win=win)
                if result:
                    all_results[uni_name] = result
            except Exception as e:
                print(f"\n  ERROR on {uni_name}: {e}")
                all_results[uni_name] = {"error": str(e)}

        # Save comparison JSON
        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Print summary table
        print(f"\n\n{'=' * 80}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 80}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>8} {'AvgStk':>8}")
        print("-" * 80)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR / NO DATA':>40}")
                continue
            p = r.get("portfolio", {})
            c = r.get("comparison", {})
            n = r.get("n_periods", 0)
            # B006: cash is counted over every rebalance the strategy ran, so
            # the percentage must divide by total_rebalances, NOT n_periods
            # (which counts only benchmark-priced periods). Dividing by
            # n_periods gives 84/50 = 168% on the OSL leg. The fallback keeps
            # pre-B006 JSONs renderable until they are re-run.
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
            excess = c.get("excess_cagr")
            sharpe = p.get("sharpe_ratio")
            maxdd = p.get("max_drawdown")
            avg = r.get("avg_stocks_when_invested")
            print(f"{uni:<20} {cagr if cagr is not None else 'N/A':>7}% "
                  f"{f'{excess:+.2f}' if excess is not None else 'N/A':>7}% "
                  f"{sharpe if sharpe is not None else 'N/A':>8} "
                  f"{maxdd if maxdd is not None else 'N/A':>7}% "
                  f"{cash_pct:>7.0f}% {avg if avg is not None else 'N/A':>8}")
        print("=" * 80)
        return

    # Single exchange mode
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    win = RunWindow.from_args(args, cr, {universe_name: exchanges})
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, args.verbose, args.output, offset_days=offset_days,
               exit_method=args.exit_method, data_checks=args.data_checks, full_holdings=args.full_holdings,
               win=win)


if __name__ == "__main__":
    main()
