#!/usr/bin/env python3
"""
Asset Growth Anomaly Backtest

Annual rebalancing (July), equal weight, top 30 by lowest asset growth.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Asset growth -20% to +10%, ROE > 8%, ROA > 5%, OPM > 10%, MCap > local-currency threshold (low)
Growth: the current FY balance sheet (period end in (T-410, T-45] days, as are the key-metrics and ratios
        rows) against the same company's prior FY, 300-430 days before it in the same reporting currency.
Universe: ordinary shares only (data_utils universe rules ur-2, METHODOLOGY.md): preferreds, notes, units,
        funds, US ADRs and US trusts and LPs are out before the screen; on SAO a company holds one line, a
        preferred or unit line standing in when the ordinary line has no entry price; a line whose earlier
        history copies another line's enters on its first own day (ur-2 line_starts: AZZA3.SA, 2024-08-27).
Portfolio: Top 30 by lowest asset growth, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), from 2000 to the latest close (METHODOLOGY.md, "Run Window"); the published
        window runs to 2026-11-09 (--end-date 2026-11-09). Data checks dc-1 in place of a return cap.

Academic reference: Cooper, Gulen & Schill (2008) "Asset Growth and the
Cross-Section of Stock Returns", Journal of Finance 63(4), 1609-1651.

Usage:
    # Backtest US stocks (default)
    python3 asset-growth/backtest.py

    # Backtest Indian stocks
    python3 asset-growth/backtest.py --preset india

    # Backtest all exchanges (loop)
    python3 asset-growth/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 asset-growth/backtest.py --no-costs

See README.md for strategy details.
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
from data_utils import (query_parquet, get_prices, RunWindow, with_partial_year, filter_returns,
                        entry_buyable, entry_usable, LtpExits, add_exit_method_arg,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations, utc_epoch,
                        campaign_listing_condition, campaign_listing_label, isin_dedup_sql, load_isin_map,
                        DATA_CHECKS_V1, DataChecks, clean_price_cache, mark_nvdr_twins, load_cap_check,
                        load_splits, empty_cap_check, cap_used_sql, CAP_FAIL_TEST,
                        UniverseRules, non_ordinary_sql, UNIVERSE_RULES_V2)
from metrics import (compute_metrics, format_metrics,
                     period_accounting)
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, add_end_date_arg, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
ASSET_GROWTH_MAX = 0.10    # Buy stocks with asset growth < 10%
ASSET_GROWTH_MIN = -0.20   # Exclude shrinking companies (> -20%)
ROE_MIN = 0.08             # Return on equity > 8%
ROA_MIN = 0.05             # Return on assets > 5%
OPM_MIN = 0.10             # Operating profit margin > 10%
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold(use_low_threshold=True)
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (annual filings available by then)
MIN_ENTRY_PRICE = 1.0      # Skip stocks with entry price < $1 (price data artifact)
FILING_LAG_DAYS = 45          # an FY row counts from 45 days after its period end
CURRENT_ROW_MAX_DAYS = 410    # current FY rows: a period end after T-410 days (45 + 365)
PRIOR_FY_MIN_DAYS = 300       # Prior FY row: 300-430 days before the company's own current row
PRIOR_FY_MAX_DAYS = 430       # (52/53-week years and a moved year-end fit; a skipped year does not)
# No return cap: data checks dc-1 (traded bars, NVDR twins, market-cap check at the floor) clean the
# inputs, and every kept move over +100% or under -50% is listed in the results.
DATA_CHECKS = DATA_CHECKS_V1
UNIVERSE_RULES = UNIVERSE_RULES_V2


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, mktcap_min=None, dc=None, ur=None,
                       win=None):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR): ordinary lines only
        balance_cache(symbol, totalAssets, filing_epoch, period, reportedCurrency)
        metrics_cache(symbol, returnOnEquity, returnOnAssets, marketCap, filing_epoch, period)
        ratios_cache(symbol, operatingProfitMargin, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose, close, volume)
        cap_check (empty unless dc is on), and with dc on: splits, prices_dropped

    dc: a data_utils.DataChecks; when on, the fetch runs checks T, N and M (mktcap_min is the floor).
    ur: a data_utils.UniverseRules (UNIVERSE_RULES), which records the lines the ordinary-shares rule removes.
    win: the leg's data_utils.RunWindow; its fetch_end cuts the price windows at the run's end date.
    Returns DuckDB connection or None.
    """
    checks = dc is not None and dc.on
    ur = ur if ur is not None else UniverseRules(UNIVERSE_RULES)
    win = win if win is not None else RunWindow.typed()
    # Campaign listing rule; sym_filter_sql and the profile fetch carry it.
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
    con.execute("SET memory_limit='4GB'")

    # 1. Universe: ordinary shares only (the CASE names why a line is not one; those lines stay out)
    print("  Fetching exchange membership...")
    profile_sql = f"""
        SELECT DISTINCT symbol, exchange, cik, companyName,
               {non_ordinary_sql("p", cfg=UNIVERSE_RULES)} AS non_ordinary
        FROM profile p
        {exchange_where}
    """
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    kept = ur.sector_rows(profiles)
    print(f"  Universe: {ur.lines} symbols, {len(ur.removed)} not ordinary shares")
    if not kept:
        print("  No ordinary shares in this universe.")
        return None
    con.execute("CREATE TABLE universe AS SELECT DISTINCT unnest(?::VARCHAR[]) AS symbol",
                [sorted({r["symbol"] for r in kept})])

    if exchanges:
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}){listing_and})"
    else:
        sym_filter_sql = "1=1"
    # symbol -> ISIN (+ live, home-currency flags) for screen_stocks' same-security dedup, every leg.
    load_isin_map(client, con, exchanges, sym_filter_sql, verbose=verbose)
    if checks:   # check N: SET NVDR lines share their ordinary line's security key
        dc.record_twins_linked(mark_nvdr_twins(con))
    ur.mark_twins(con)   # ur-2: an SAO preferred or unit line stands in for an unpriced ordinary line

    # 2. Balance sheet (two FY rows in one currency for YoY growth), key metrics, ratios
    queries = [
        ("balance_cache", f"""
            SELECT symbol, totalAssets, dateEpoch as filing_epoch, period, reportedCurrency
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalAssets IS NOT NULL
              AND totalAssets > 0
              AND {sym_filter_sql}
        """, "balance sheet (total assets)"),
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, returnOnAssets, marketCap,
                   dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND returnOnEquity IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (ROE, ROA, market cap)"),
        ("ratios_cache", f"""
            SELECT symbol, operatingProfitMargin,
                   dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND operatingProfitMargin IS NOT NULL
              AND {sym_filter_sql}
        """, "financial ratios (operating profit margin)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # Check M (the floor reads cap_used) and the splits check F flags; cap_check stays empty when off.
    if checks:
        print("  Fetching market-cap check inputs and splits...")
        dc.record_cap(load_cap_check(client, con, sym_filter_sql, mktcap_min, dc.cfg, verbose=verbose))
        load_splits(client, con, sym_filter_sql, verbose=verbose)
    else:
        empty_cap_check(con)

    # 3. Prices (only at rebalance dates, with next-day offset window)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = win.fetch_end(d, 11)  # covers get_prices' [d+1, d+11] with offset_days=1
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    # Build benchmark symbol list (SPY + local index)
    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(bench_symbols)

    price_sql = f"""
        SELECT symbol, dateEpoch as trade_epoch, adjClose, close, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR symbol IN (
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    if checks:   # check T: closed days, zero-volume and flat lines, non-positive prices, sibling-class gaps
        dc.record_clean(clean_price_cache(con, bench_symbols, rebalance_dates, dc.cfg, siblings="cap_inputs"))
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


PRIOR_GUARD_REASONS = ("currency_switch", "no_prior_in_window")


class PriorYearGuard:
    """One leg's prior-year guard record: per rebalance date, the screen's candidates left unranked because
    no prior FY row pairs with the current one (screen inputs only: equal in drop, ltp and both)."""

    RULE = (f"the current FY balance-sheet row has a period end in (T-{CURRENT_ROW_MAX_DAYS}, T-{FILING_LAG_DAYS}] "
            f"days, as do the key-metrics and ratios rows; the prior row is the latest {PRIOR_FY_MIN_DAYS} to "
            f"{PRIOR_FY_MAX_DAYS} days before it in the same reportedCurrency (NULL matches NULL); a line with none "
            "is not ranked: currency_switch when that window holds rows only in another currency, "
            "no_prior_in_window when it holds none; counted per line before the security dedup, over the lines "
            "in the universe that pass the ROE, ROA, margin and market-cap tests")

    def __init__(self):
        self.periods = {}

    def record(self, rows, target_date):
        """rows: (symbol, reason) of one screen's unranked candidates."""
        self.periods[target_date.isoformat()] = {k: sum(1 for _, why in rows if why == k)
                                                 for k in PRIOR_GUARD_REASONS}

    def block(self):
        by_period = [{"rebalance_date": d, **n} for d, n in sorted(self.periods.items())]
        by_reason = {k: sum(p[k] for p in by_period) for k in PRIOR_GUARD_REASONS}
        return {"rule": self.RULE, "dropped": sum(by_reason.values()), "by_reason": by_reason,
                "by_period": by_period}


def _candidates_sql(cap_test, events=False):
    """The screen's candidates in `universe`, each with prior_guard (PriorYearGuard.RULE; NULL when paired). The
    floor reads cap_used (check M); cap_test is the floor clause ("km.cap_used > ?", or CAP_FAIL_TEST for M's
    events, which also read km_epoch and raw_cap)."""
    extra = ", km.filing_epoch AS km_epoch, km.marketCap AS raw_cap" if events else ""
    growth = "(bc.totalAssets - bp.totalAssets_prior) / bp.totalAssets_prior"
    return f"""
        WITH bs_current AS (
            SELECT symbol, totalAssets, filing_epoch, reportedCurrency,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC, totalAssets DESC NULLS LAST,
                    reportedCurrency DESC NULLS LAST) AS rn
            FROM balance_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        bs_window AS (
            SELECT b.symbol, b.totalAssets, b.filing_epoch, b.reportedCurrency, c.curr_ccy
            FROM balance_cache b
            JOIN (SELECT symbol, filing_epoch AS curr_epoch, reportedCurrency AS curr_ccy
                  FROM bs_current WHERE rn = 1) c ON c.symbol = b.symbol
            WHERE b.filing_epoch <= c.curr_epoch - ? AND b.filing_epoch >= c.curr_epoch - ?
        ),
        bs_prior AS (
            SELECT symbol, totalAssets AS totalAssets_prior, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC, totalAssets DESC NULLS LAST,
                    reportedCurrency DESC NULLS LAST) AS rn
            FROM bs_window
            WHERE reportedCurrency IS NOT DISTINCT FROM curr_ccy
        ),
        km AS (
            SELECT m.symbol, m.returnOnEquity, m.returnOnAssets, m.marketCap, m.filing_epoch,
                {cap_used_sql()} AS cap_used,
                ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.filing_epoch DESC, m.marketCap DESC NULLS LAST,
                    m.returnOnEquity DESC NULLS LAST, m.returnOnAssets DESC NULLS LAST) AS rn
            FROM metrics_cache m
            LEFT JOIN cap_check cc ON cc.symbol = m.symbol AND cc.filing_epoch = m.filing_epoch
            WHERE m.filing_epoch <= ? AND m.filing_epoch > ?
        ),
        fr AS (
            SELECT symbol, operatingProfitMargin, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC, operatingProfitMargin DESC NULLS LAST) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        )
        SELECT bc.symbol, km.cap_used AS marketCap, {growth} AS asset_growth,
            CASE WHEN bp.symbol IS NOT NULL THEN NULL
                 WHEN bw.symbol IS NOT NULL THEN 'currency_switch' ELSE 'no_prior_in_window' END AS prior_guard{extra}
        FROM bs_current bc
        LEFT JOIN bs_prior bp ON bc.symbol = bp.symbol AND bp.rn = 1
        LEFT JOIN (SELECT DISTINCT symbol FROM bs_window) bw ON bw.symbol = bc.symbol
        JOIN km ON bc.symbol = km.symbol AND km.rn = 1
        JOIN fr ON bc.symbol = fr.symbol AND fr.rn = 1
        WHERE bc.rn = 1
          AND bc.symbol IN (SELECT symbol FROM universe)
          AND (bp.symbol IS NULL OR (bp.totalAssets_prior > 0 AND {growth} < ? AND {growth} > ?))
          AND km.returnOnEquity > ?
          AND km.returnOnAssets > ?
          AND fr.operatingProfitMargin > ?
          AND {cap_test}
    """


def _started(candidates_sql, late):
    """The candidates without the lines a screen leaves out before their ur-2 line_starts day (unchanged if none)."""
    if not late:
        return candidates_sql
    names = ", ".join("'" + s.replace("'", "''") + "'" for s in late)
    return f"SELECT * FROM ({candidates_sql}) _s WHERE _s.symbol NOT IN ({names})"


def _paired(candidates_sql, rule="IS NULL"):
    """The candidates the prior-year guard pairs ("IS NULL") or leaves unranked ("IS NOT NULL")."""
    return f"SELECT * FROM ({candidates_sql}) _g WHERE _g.prior_guard {rule}"


def screen_stocks(con, target_date, mktcap_min, offset_days=1, dc=None, ur=None, guard=None):
    """Screen for low asset growth quality stocks.

    Computes YoY asset growth from each company's current FY balance sheet and its own prior FY row.
    Returns list of (symbol, market_cap) tuples sorted by asset growth ASC, top MAX_STOCKS after the
    prior-year guard and the security dedup.
    dc (data_utils.DataChecks): when on, NVDR twins share a security (check N); it also records the
    screen's twin and market-cap events. ur-2's class twins share a security in every mode, so an SAO
    company holds one line. ur: the leg's data_utils.UniverseRules (its removals act at the fetch, its
    line_starts here; it records the class twins each screen picks). guard (PriorYearGuard) records the
    unranked candidates.
    """
    # 45-day lag for point-in-time (July rebalance = filings through ~May 17)
    cutoff_epoch = utc_epoch(target_date - timedelta(days=FILING_LAG_DAYS))
    # Staleness: a period end within 410 days (45 + 365), else a delisted name's last pair keeps a slot
    stale_epoch = utc_epoch(target_date - timedelta(days=CURRENT_ROW_MAX_DAYS))
    # Prior FY is measured from each company's OWN current row, not from the rebalance date.
    prior_gap_min, prior_gap_max = PRIOR_FY_MIN_DAYS * 86400, PRIOR_FY_MAX_DAYS * 86400

    late = ur.not_started(target_date) if ur is not None else []
    candidates = _started(_candidates_sql("km.cap_used > ?"), late)
    params = [cutoff_epoch, stale_epoch,       # bs_current
              prior_gap_min, prior_gap_max,   # bs_window
              cutoff_epoch, stale_epoch,       # km
              cutoff_epoch, stale_epoch,       # fr
              ASSET_GROWTH_MAX, ASSET_GROWTH_MIN, ROE_MIN, ROA_MIN, OPM_MIN, mktcap_min]
    paired = _paired(candidates)
    # One row per security BEFORE the top-N cut, so a collapsed twin or an unpaired line frees its slot.
    rows = con.execute(f"""
        SELECT symbol, marketCap, asset_growth
        FROM ({isin_dedup_sql(paired, target_date, offset_days, MIN_ENTRY_PRICE,
                              twin_key=(dc is not None and dc.on) or bool(UNIVERSE_RULES.get("class_twins")))})
        ORDER BY asset_growth ASC, symbol
        LIMIT ?
    """, params + [MAX_STOCKS]).fetchall()

    picked = [(r[0], r[1]) for r in rows]
    if ur is not None:
        ur.stand_ins([s for s, _ in picked], target_date)
    if guard is not None:
        guard.record(con.execute(f"SELECT DISTINCT symbol, prior_guard FROM ({_paired(candidates, 'IS NOT NULL')})",
                                 params).fetchall(), target_date)
    if dc is not None:
        dc.twins(con, paired, params, target_date, offset_days, MIN_ENTRY_PRICE, [s for s, _ in picked])
        dc.caps(con, _paired(_started(_candidates_sql(CAP_FAIL_TEST, events=True), late)), params + [mktcap_min],
                target_date, mktcap_min)
    return picked


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY", ltp=None, ur=None, guard=None, win=None,
                 exit_method="drop", dc=None):
    """Run Asset Growth backtest. Returns list of period result dicts.

    dc: the leg's data_utils.DataChecks. Without one, no check runs (the "uncapped" mode); either
    way there is no return cap. ur, guard: the leg's data_utils.UniverseRules and PriorYearGuard, passed
    to the screen. win: the leg's data_utils.RunWindow (its grid made rebalance_dates): the partial exit
    is priced at the close on the end date (the caller marks the rows: RunWindow.mark)."""
    results = []
    dc = dc if dc is not None else DataChecks("uncapped", DATA_CHECKS)
    win = win if win is not None else RunWindow.typed()

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]
        x_off = win.exit_offset(exit_date, offset_days)

        portfolio = screen_stocks(con, entry_date, mktcap_min, offset_days=offset_days, dc=dc, ur=ur, guard=guard)

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
                "holdings": f"CASH ({len(portfolio)} passed)",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _ in portfolio]
        mcaps = {s: mc for s, mc in portfolio}

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
            # symbol_data with the exit slot taken from xmap
            clean, skipped = filter_returns([(sym, ep, xmap.get(sym), mcap) for sym, ep, _, mcap in symbol_data],
                                            min_entry_price=MIN_ENTRY_PRICE,
                                            max_single_return=None,   # no return cap (data checks dc-1)
                                            verbose=verbose)
            clean = dc.hand(clean, entry_date, exit_date, filled=ltp_fill, record=record)   # dc-3's hand list
            return clean, [apply_costs(rr, tiered_cost(mc)) if use_costs else rr for _, rr, mc in clean]

        booked = exit_ltp if exit_method == "ltp" else exit_prices
        clean, returns = book(booked)

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
            "holdings": ",".join(sym for sym, _, _ in clean),
            **dc.holdings_full(con, clean, entry_prices, booked, ltp_fill if exit_method == "ltp" else {},
                               entry_date, exit_date, offset_days, ltp, exit_offset_days=x_off),
        })
        if exit_method == "both":
            c_ltp, r_ltp = book(exit_ltp, verbose=False, record=False)
            results[-1].update({"portfolio_return_ltp": round(sum(r_ltp) / len(r_ltp), 6) if r_ltp else 0.0,
                                "stocks_held_ltp": len(r_ltp), "ltp_filled": sum(1 for s, _, _ in c_ltp if s in ltp_fill)})

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency,
                 periods_per_year, cash_periods, avg_stocks):
    """Build JSON output in standard format."""
    p = metrics["portfolio"]
    b = metrics["benchmark"]
    c = metrics["comparison"]

    def pct(v):
        return round(v * 100, 2) if v is not None else None

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    def format_series(s):
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
        "portfolio": format_series(p),
        "spy": format_series(b),
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

    data_checks: "on" (data checks dc-1), or "uncapped" (no check and no cap: attribution only). The
    universe rules ur-2 and the prior-year guard apply in both. full_holdings: every invested period row
    also lists each booked name (holdings_full). win: the run's data_utils.RunWindow (main builds it from
    --end-date or --end-year)."""
    win = win.for_leg(universe_name)
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]
    dc = DataChecks(data_checks, DATA_CHECKS, full_holdings=full_holdings)
    ur = UniverseRules(UNIVERSE_RULES)
    guard = PriorYearGuard()

    mktcap_threshold = get_mktcap_threshold(exchanges, use_low_threshold=True)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "Same-day close (legacy)" if offset_days == 0 else "Next-day close (MOC)"
    signal_desc = (f"AG {ASSET_GROWTH_MIN*100:.0f}% to {ASSET_GROWTH_MAX*100:.0f}%, "
                   f"ROE > {ROE_MIN*100:.0f}%, ROA > {ROA_MIN*100:.0f}%, "
                   f"OPM > {OPM_MIN*100:.0f}%, MCap > {mktcap_label} local, top {MAX_STOCKS}")
    print_header("ASSET GROWTH ANOMALY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    # Phase 1: Fetch data
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

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest ({rebalance_dates[0]} to {rebalance_dates[-1]})...")
    t1 = time.time()
    # prices_cache holds only [R, R+11d] windows at rebalance dates, so LTP reads remote stock_eod.
    ltp = (LtpExits(cr, con, verbose=verbose, traded_only=dc.on, end_date=win.end_date, cfg=dc.cfg)
           if exit_method != "drop" else None)
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs,
                           verbose=verbose, offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol, ltp=ltp, ur=ur, guard=guard, win=win,
                           exit_method=exit_method, dc=dc)
    win.mark(results)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")
    print(f"  {dc.summary()}")
    ub, gb = ur.block(), guard.block()
    print(f"  Universe rules: {ub['ordinary_shares']['removed_count']} lines not ordinary shares "
          f"{ub['ordinary_shares']['by_reason']}; prior-year guard: {gb['dropped']} unranked {gb['by_reason']}")

    # Phase 3: Compute metrics
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
    print(format_metrics(metrics, "Asset Growth", benchmark_name))
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
        print(f"\n  {'Year':<8} {'AssetGrowth':>12} {bench_label:>10} {'Excess':>10}")
        print("  " + "-" * 42)
        for ar in with_partial_year(annual, partial_year):
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks)
    output.update({**win.fields(con, benchmark_symbol, valid, periods_per_year, partial_year, offset_days),
                   "listing_filter": campaign_listing_label(exchanges), "isin_dedup": True})
    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both
    output["universe_rules"] = ub        # the fetch's removals: equal in drop, ltp and both
    output["prior_year_guard"] = gb      # the screen only: equal in drop, ltp and both
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
    parser = argparse.ArgumentParser(description="Asset Growth Anomaly backtest")
    add_common_args(parser)
    add_end_date_arg(parser)
    add_exit_method_arg(parser)
    parser.add_argument("--data-checks", choices=DataChecks.MODES, default="on",
                        help="Data checks dc-1 (default on). 'uncapped' turns every check off and sets no "
                             "return cap: the same-day attribution arm only, never published")
    parser.add_argument("--full-holdings", action="store_true",
                        help="Also list every booked name per invested period (holdings_full: entry and exit "
                             "bars and prices, return before costs), for review")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("asset-growth", args_str=" ".join(cloud_args),
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

    # --global mode: loop all presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        presets_to_run = [
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("india", ["NSE"]),
            ("germany", ["XETRA"]),
            ("sweden", ["STO"]),
            ("canada", ["TSX"]),
            ("china", ["SHZ", "SHH"]),
            ("hongkong", ["HKSE"]),
            ("japan", ["JPX"]),
            ("uk", ["LSE"]),
            ("australia", ["ASX"]),
            ("korea", ["KSC"]),
            ("brazil", ["SAO"]),
            ("switzerland", ["SIX"]),
            ("taiwan", ["TAI"]),
            ("thailand", ["SET"]),
            ("saudi", ["SAU"]),
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

        # Save comparison
        if args.output:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n\nExchange comparison saved to {args.output}")

        # Print summary
        print(f"\n\n{'=' * 80}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 80}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} {'MaxDD':>8} {'Cash%':>8} {'AvgStk':>8}")
        print("-" * 80)
        for uni, r in sorted(all_results.items(), key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999, reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR / NO DATA':>8}")
                continue
            p = r.get("portfolio", {})
            c = r.get("comparison", {})
            n = r.get("n_periods", 0)
            # B006: divide by total_rebalances, not n_periods. Cash is counted
            # over every rebalance the strategy ran; n_periods counts only the
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
