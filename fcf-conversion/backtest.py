#!/usr/bin/env python3
"""
FCF Conversion Quality Backtest

Annual rebalancing (July), equal weight, top 30 by highest FCF conversion.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: FCF/NI > 100%, FCF/NI < 300%, FCF margin > 10%, ROE > 10%, OPM > 10%,
        NI > 0, FCF > 0, Market Cap > local currency threshold
Rows: the FY cash-flow, income, key-metrics and ratios rows with a period end in (T-410, T-45] days; the
        income row pairs with the cash-flow row (the same reportedCurrency, period ends within 31 days either
        way), else the line is not ranked (statement_pair_guard in the results).
Universe: ordinary shares only (data_utils universe rules ur-3, METHODOLOGY.md): preferreds, notes, units,
        funds, US ADRs, US trusts and LPs and the hand list's lines, depositary receipts listed abroad among
        them, are out before the screen; on SAO a company holds one line; a line whose earlier history copies
        another line's enters on its first own day (ur-3 line_starts).
Portfolio: Top 30 by highest FCF conversion, equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), from 2000 to the latest close (METHODOLOGY.md, "Run Window"). Data checks dc-3 in
        place of a return cap. Costs: costs.tiered_cost, its USD tiers converted at the leg's
        costs.get_fx_per_usd (cost_model in the results).

Academic reference: Sloan, R. (1996) "Do Stock Prices Fully Reflect Information
in Accruals and Cash Flows about Future Earnings?" The Accounting Review 71(3).

Usage:
    # Backtest US stocks (default), to the latest close
    python3 fcf-conversion/backtest.py

    # One market to a pinned end date (--end-year YYYY ends the grid in that year with no partial period)
    python3 fcf-conversion/backtest.py --exchange NSE --end-date YYYY-MM-DD --output fcf-conversion/results/returns_NSE.json

    # Flags: --data-checks on|uncapped (default on), --full-holdings, --exit-method drop|ltp|both, and
    # --exclude-sector "Financial Services" (that sector's lines leave before the top-30 cut)

    # Backtest all exchanges (loop)
    python3 fcf-conversion/backtest.py --global --output fcf-conversion/results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 fcf-conversion/backtest.py --no-costs

See README.md for strategy details.
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
                        UniverseRules, non_ordinary_sql, UNIVERSE_RULES_V3, RowGuard, started_sql as _started,
                        guarded_sql)
from metrics import (compute_metrics, format_metrics,
                     period_accounting)
from costs import tiered_cost, apply_costs, get_fx_per_usd
from cli_utils import (add_common_args, add_end_date_arg, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
FCF_CONVERSION_MIN = 1.0   # FCF/NI > 100%
FCF_CONVERSION_MAX = 3.0   # FCF/NI < 300% (filter noise from tiny NI denominators)
FCF_MARGIN_MIN = 0.10      # FCF/Revenue > 10%
ROE_MIN = 0.10             # Return on equity > 10%
OPM_MIN = 0.10             # Operating profit margin > 10%
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (annual filings available by then)
MIN_ENTRY_PRICE = 1.0      # Skip penny stocks
FILING_LAG_DAYS = 45          # an FY row counts from 45 days after its period end
CURRENT_ROW_MAX_DAYS = 410    # the FY rows: a period end after T-410 days (45 + 365)
PAIR_MAX_GAP_DAYS = 31        # the income row's period end within 31 days of the cash-flow row's, either way
# No return cap: data checks dc-3 (traded bars, NVDR twins, market-cap check at the floor, the reviewed
# hand list) clean the inputs, and every kept move over +100% or under -50% is listed in the results.
DATA_CHECKS = DATA_CHECKS_V3
UNIVERSE_RULES = UNIVERSE_RULES_V3


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, mktcap_min=None, dc=None, ur=None,
                       win=None, sectors=None):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR): ordinary lines only
        cashflow_cache(symbol, freeCashFlow, filing_epoch, period, reportedCurrency)
        income_cache(symbol, netIncome, revenue, filing_epoch, period, reportedCurrency)
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, operatingProfitMargin, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose, close, volume)
        cap_check (empty unless dc is on), and with dc on: splits, prices_dropped
        sector_out, with sectors: the kept lines whose profile names an excluded sector

    dc: a data_utils.DataChecks; when on, the fetch runs checks T, N and M (mktcap_min is the floor).
    ur: a data_utils.UniverseRules (UNIVERSE_RULES), which records the lines the ordinary-shares rule removes.
    win: the leg's data_utils.RunWindow; its fetch_end cuts the price windows at the run's end date.
    sectors: the leg's SectorExclusion (--exclude-sector), or None.
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
        SELECT DISTINCT symbol, exchange, cik, companyName, sector,
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
    if sectors is not None:
        sectors.load(con, kept)
        print(f"  Excluded sectors {sectors.sectors}: {len(sectors.symbols)} lines leave the book")

    if exchanges:
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}){listing_and})"
    else:
        sym_filter_sql = "1=1"
    # symbol -> ISIN (+ live, home-currency flags) for screen_stocks' same-security dedup, every leg.
    load_isin_map(client, con, exchanges, sym_filter_sql, verbose=verbose)
    if checks:   # check N: SET NVDR lines share their ordinary line's security key
        dc.record_twins_linked(mark_nvdr_twins(con))
    ur.mark_twins(con)   # ur-3: an SAO preferred or unit line stands in for an unpriced ordinary line

    # 2-5. Financial data (the cash-flow and income rows carry their currency for the pair test)
    queries = [
        ("cashflow_cache", f"""
            SELECT symbol, freeCashFlow, dateEpoch as filing_epoch, period, reportedCurrency
            FROM cash_flow_statement
            WHERE period = 'FY'
              AND freeCashFlow IS NOT NULL
              AND {sym_filter_sql}
        """, "cash flow statement (FCF)"),
        ("income_cache", f"""
            SELECT symbol, netIncome, revenue, dateEpoch as filing_epoch, period, reportedCurrency
            FROM income_statement
            WHERE period = 'FY'
              AND netIncome IS NOT NULL
              AND {sym_filter_sql}
        """, "income statement (NI, revenue)"),
        ("metrics_cache", f"""
            SELECT symbol, returnOnEquity, marketCap,
                   dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND returnOnEquity IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (ROE, market cap)"),
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

    # 6. Prices (only at rebalance dates)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = win.fetch_end(d, 11)  # covers get_prices' [d+1, d+11] with offset_days=1
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    # Include local benchmark symbols alongside SPY
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
                SELECT DISTINCT symbol FROM cash_flow_statement WHERE period = 'FY'
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


PAIR_GUARD_REASONS = ("currency_mismatch", "no_income_in_window")


class StatementPairGuard(RowGuard):
    """One leg's cash-flow and income pair record: per rebalance date, the screen's candidates left unranked
    because no income row pairs with the cash-flow row (screen inputs only: equal in drop, ltp and both)."""

    RULE = (f"the FY cash-flow, income, key-metrics and ratios rows have a period end in "
            f"(T-{CURRENT_ROW_MAX_DAYS}, T-{FILING_LAG_DAYS}] days; the income row is the latest in that window "
            f"whose period end is within {PAIR_MAX_GAP_DAYS} days of the cash-flow row's, either way, in the same "
            "reportedCurrency (NULL matches NULL); a line with none is not ranked: currency_mismatch when such "
            "rows exist only in another currency, no_income_in_window when there are none; counted per line "
            "before the security dedup, over the lines in the universe with positive free cash flow that pass "
            "the ROE, operating-margin and market-cap tests")

    def __init__(self):
        super().__init__(self.RULE, PAIR_GUARD_REASONS)


class SectorExclusion:
    """--exclude-sector: a line any of whose profile rows names one of `sectors` leaves the book after the
    screen's other tests (the pair guard and check M still count it), before the security dedup and the
    top-30 cut. removed: per rebalance date, every such line that passed the screen (screen inputs only:
    equal in drop, ltp and both)."""

    FIELD = "profile.sector"

    def __init__(self, sectors):
        sectors = [sectors] if isinstance(sectors, str) else sectors   # one name, never its characters
        self.sectors, self.symbols, self.removed = list(dict.fromkeys(sectors)), set(), {}

    def load(self, con, rows):
        """rows: the leg's kept profile rows (symbol, sector, ...). Writes the sector_out table the screen reads."""
        self.symbols = {r["symbol"] for r in rows if r.get("sector") in self.sectors}
        con.execute("CREATE OR REPLACE TABLE sector_out AS SELECT DISTINCT unnest(?::VARCHAR[]) AS symbol",
                    [sorted(self.symbols)])

    def keep(self, sql):
        """sql's rows without the excluded lines."""
        return f"SELECT * FROM ({sql}) _x WHERE _x.symbol NOT IN (SELECT symbol FROM sector_out)"

    def record(self, con, sql, params, target_date):
        """The excluded lines among sql's rows on target_date (a date screened again replaces its list)."""
        rows = con.execute(f"SELECT DISTINCT symbol FROM ({sql}) _x WHERE _x.symbol IN (SELECT symbol FROM sector_out)",
                           params).fetchall()
        self.removed[target_date.isoformat()] = sorted(r[0] for r in rows)

    def block(self):
        return {"sectors": list(self.sectors), "field": self.FIELD, "removed": dict(sorted(self.removed.items()))}


def _candidates_sql(cap_test, events=False):
    """The screen's candidates in `universe`, each with pair_guard (StatementPairGuard.RULE; NULL when paired).
    The floor reads cap_used (check M); cap_test is the floor clause ("km.cap_used > ?", or CAP_FAIL_TEST for
    M's events, which also read km_epoch and raw_cap)."""
    extra = ", km.filing_epoch AS km_epoch, km.marketCap AS raw_cap" if events else ""
    conv = "cf.freeCashFlow / inc.netIncome"
    return f"""
        WITH cf AS (
            SELECT symbol, freeCashFlow, filing_epoch, reportedCurrency,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC, freeCashFlow DESC NULLS LAST,
                    reportedCurrency DESC NULLS LAST) AS rn
            FROM cashflow_cache
            WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        inc_window AS (
            SELECT i.symbol, i.netIncome, i.revenue, i.filing_epoch, i.reportedCurrency, c.cf_ccy
            FROM income_cache i
            JOIN (SELECT symbol, filing_epoch AS cf_epoch, reportedCurrency AS cf_ccy FROM cf WHERE rn = 1) c
                ON c.symbol = i.symbol
            WHERE i.filing_epoch <= ? AND i.filing_epoch > ?
              AND i.filing_epoch >= c.cf_epoch - ? AND i.filing_epoch <= c.cf_epoch + ?
        ),
        inc AS (
            SELECT symbol, netIncome, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC, netIncome DESC NULLS LAST,
                    revenue DESC NULLS LAST) AS rn
            FROM inc_window
            WHERE reportedCurrency IS NOT DISTINCT FROM cf_ccy
        ),
        km AS (
            SELECT m.symbol, m.returnOnEquity, m.marketCap, m.filing_epoch,
                {cap_used_sql()} AS cap_used,
                ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.filing_epoch DESC, m.marketCap DESC NULLS LAST,
                    m.returnOnEquity DESC NULLS LAST) AS rn
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
        SELECT cf.symbol, km.cap_used AS marketCap, {conv} AS fcf_conversion,
            CASE WHEN inc.symbol IS NOT NULL THEN NULL
                 WHEN iw.symbol IS NOT NULL THEN 'currency_mismatch' ELSE 'no_income_in_window' END AS pair_guard{extra}
        FROM cf
        LEFT JOIN inc ON cf.symbol = inc.symbol AND inc.rn = 1
        LEFT JOIN (SELECT DISTINCT symbol FROM inc_window) iw ON iw.symbol = cf.symbol
        JOIN km ON cf.symbol = km.symbol AND km.rn = 1
        JOIN fr ON cf.symbol = fr.symbol AND fr.rn = 1
        WHERE cf.rn = 1
          AND cf.symbol IN (SELECT symbol FROM universe)
          AND cf.freeCashFlow > 0
          AND (inc.symbol IS NULL OR (inc.netIncome > 0 AND inc.revenue > 0
               AND {conv} > ? AND {conv} < ?
               AND cf.freeCashFlow / inc.revenue > ?))
          AND km.returnOnEquity > ?
          AND fr.operatingProfitMargin > ?
          AND {cap_test}
    """


# The candidates the pair guard pairs ("IS NULL") or leaves unranked ("IS NOT NULL").
_paired = partial(guarded_sql, "pair_guard")


def screen_stocks(con, target_date, mktcap_min, offset_days=1, dc=None, ur=None, guard=None, sectors=None):
    """Screen for high FCF conversion quality stocks.

    FCF Conversion = freeCashFlow / netIncome, from the cash-flow row and the income row that pairs with it.
    Requires both NI > 0 and FCF > 0, plus quality filters.
    Returns list of (symbol, market_cap) tuples sorted by FCF conversion DESC, top MAX_STOCKS after the pair
    guard, the sector switch and the security dedup.
    dc (data_utils.DataChecks): when on, NVDR twins share a security (check N); it also records the
    screen's twin and market-cap events. ur-3's class twins share a security in every mode, so an SAO
    company holds one line. ur: the leg's data_utils.UniverseRules (its removals act at the fetch, its
    line_starts here; it records the class twins each screen picks). guard (StatementPairGuard) records the
    unranked candidates. sectors (SectorExclusion) leaves its lines out and records them.
    """
    # 45-day lag for point-in-time (July rebalance = filings through ~May 17)
    cutoff_epoch = utc_epoch(target_date - timedelta(days=FILING_LAG_DAYS))
    # Staleness: a period end within 410 days (45 + 365), else a delisted name's last rows keep a slot
    stale_epoch = utc_epoch(target_date - timedelta(days=CURRENT_ROW_MAX_DAYS))
    pair_gap = PAIR_MAX_GAP_DAYS * 86400

    late = ur.not_started(target_date) if ur is not None else []
    candidates = _started(_candidates_sql("km.cap_used > ?"), late)
    params = [cutoff_epoch, stale_epoch,                       # cf
              cutoff_epoch, stale_epoch, pair_gap, pair_gap,   # inc_window
              cutoff_epoch, stale_epoch,                       # km
              cutoff_epoch, stale_epoch,                       # fr
              FCF_CONVERSION_MIN, FCF_CONVERSION_MAX, FCF_MARGIN_MIN, ROE_MIN, OPM_MIN, mktcap_min]
    paired = _paired(candidates)
    pool = sectors.keep(paired) if sectors is not None else paired
    # One row per security BEFORE the top-N cut, so a collapsed twin or an unpaired line frees its slot.
    rows = con.execute(f"""
        SELECT symbol, marketCap, fcf_conversion
        FROM ({isin_dedup_sql(pool, target_date, offset_days, MIN_ENTRY_PRICE,
                              twin_key=(dc is not None and dc.on) or bool(UNIVERSE_RULES.get("class_twins")))})
        ORDER BY fcf_conversion DESC, symbol
        LIMIT ?
    """, params + [MAX_STOCKS]).fetchall()

    picked = [(r[0], r[1]) for r in rows]
    if ur is not None:
        ur.stand_ins([s for s, _ in picked], target_date)
    if guard is not None:
        guard.record(con.execute(f"SELECT DISTINCT symbol, pair_guard FROM ({_paired(candidates, 'IS NOT NULL')})",
                                 params).fetchall(), target_date)
    if sectors is not None:
        sectors.record(con, paired, params, target_date)
    if dc is not None:
        dc.twins(con, pool, params, target_date, offset_days, MIN_ENTRY_PRICE, [s for s, _ in picked])
        dc.caps(con, _paired(_started(_candidates_sql(CAP_FAIL_TEST, events=True), late)), params + [mktcap_min],
                target_date, mktcap_min)
    return picked


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY", ltp=None, ur=None, guard=None, win=None, fx=1.0,
                 sectors=None, exit_method="drop", dc=None):
    """Run FCF Conversion backtest. Returns list of period result dicts.

    dc: the leg's data_utils.DataChecks. Without one, no check runs (the "uncapped" mode); either
    way there is no return cap. ur, guard, sectors: the leg's data_utils.UniverseRules, StatementPairGuard
    and SectorExclusion (or None), passed to the screen. win: the leg's data_utils.RunWindow (its grid made
    rebalance_dates): the partial exit is priced at the close on the end date (the caller marks the rows:
    RunWindow.mark). fx: the leg's costs.get_fx_per_usd, which converts the USD cost tiers."""
    results = []
    dc = dc if dc is not None else DataChecks("uncapped", DATA_CHECKS)
    win = win if win is not None else RunWindow.typed()

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]
        x_off = win.exit_offset(exit_date, offset_days)

        portfolio = screen_stocks(con, entry_date, mktcap_min, offset_days=offset_days, dc=dc, ur=ur, guard=guard,
                                  sectors=sectors)

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
                                            max_single_return=None,   # no return cap (data checks dc-3)
                                            verbose=verbose)
            clean = dc.hand(clean, entry_date, exit_date, filled=ltp_fill, record=record)   # dc-3's hand list
            return clean, [apply_costs(rr, tiered_cost(mc, fx_per_usd=fx)) if use_costs else rr for _, rr, mc in clean]

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
               data_checks="on", full_holdings=False, exclude_sector=None, *, win):
    """Run backtest for a single exchange set. Returns output dict or None.

    data_checks: "on" (data checks dc-3), or "uncapped" (no check and no cap: attribution only). The
    universe rules ur-3 and the pair guard apply in both. full_holdings: every invested period row also
    lists each booked name (holdings_full). exclude_sector: profile sectors whose lines leave the book
    (--exclude-sector; sector_exclusion in the results), or None. win: the run's data_utils.RunWindow (main
    builds it from --end-date or --end-year)."""
    win = win.for_leg(universe_name)
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]
    dc = DataChecks(data_checks, DATA_CHECKS, full_holdings=full_holdings)
    ur = UniverseRules(UNIVERSE_RULES)
    guard = StatementPairGuard()
    sectors = SectorExclusion(exclude_sector) if exclude_sector else None
    fx = get_fx_per_usd(exchanges)   # local currency per USD: the cost tiers' breakpoints in the leg's currency

    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "Same-day close (legacy)" if offset_days == 0 else "Next-day close (MOC)"
    signal_desc = (f"FCF/NI {FCF_CONVERSION_MIN*100:.0f}-{FCF_CONVERSION_MAX*100:.0f}%, "
                   f"FCF margin > {FCF_MARGIN_MIN*100:.0f}%, "
                   f"ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, "
                   f"MCap > {mktcap_label} local, top {MAX_STOCKS}")
    print_header("FCF CONVERSION QUALITY BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {f'size-tiered (tiers x{fx:g} local/USD)' if use_costs else 'none'}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    if sectors is not None:
        print(f"  Excluding sectors: {', '.join(sectors.sectors)} ({SectorExclusion.FIELD})")
    print("=" * 65)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = win.grid(2000, frequency, months=DEFAULT_REBALANCE_MONTHS, offset_days=offset_days)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose,
                             mktcap_min=mktcap_threshold, dc=dc, ur=ur, win=win, sectors=sectors)
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
                           benchmark_symbol=benchmark_symbol, ltp=ltp, ur=ur, guard=guard, win=win, fx=fx,
                           sectors=sectors, exit_method=exit_method, dc=dc)
    win.mark(results)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")
    print(f"  {dc.summary()}")
    ub, gb = ur.block(), guard.block()
    print(f"  Universe rules: {ub['ordinary_shares']['removed_count']} lines not ordinary shares "
          f"{ub['ordinary_shares']['by_reason']}; pair guard: {gb['dropped']} unranked {gb['by_reason']}")
    if sectors is not None:
        print(f"  Sector exclusion: {sum(len(s) for s in sectors.removed.values())} line-periods removed")

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
    print(format_metrics(metrics, "FCF Conversion", benchmark_name))
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
        bench_label = benchmark_name[:12]
        print(f"\n  {'Year':<8} {'FCF Conv':>12} {bench_label:>12} {'Excess':>10}")
        print("  " + "-" * 44)
        for ar in with_partial_year(annual, partial_year):
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% {ar['benchmark']*100:>11.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks)
    output.update({**win.fields(con, benchmark_symbol, valid, periods_per_year, partial_year, offset_days),
                   "listing_filter": campaign_listing_label(exchanges), "isin_dedup": True})
    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both
    output.update({
        "universe_rules": ub,            # the fetch's removals: equal in drop, ltp and both
        "statement_pair_guard": gb,      # the screen only: equal in drop, ltp and both
        "cost_model": {"function": "costs.tiered_cost", "fx_per_usd": fx} if use_costs else None,
        **({"sector_exclusion": sectors.block()} if sectors is not None else {}),   # the screen only
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
    parser = argparse.ArgumentParser(description="FCF Conversion Quality backtest")
    add_common_args(parser)
    add_end_date_arg(parser)
    add_exit_method_arg(parser)
    parser.add_argument("--data-checks", choices=DataChecks.MODES, default="on",
                        help="Data checks dc-3 (default on). 'uncapped' turns every check off and sets no "
                             "return cap: the same-day attribution arm only, never published")
    parser.add_argument("--full-holdings", action="store_true",
                        help="Also list every booked name per invested period (holdings_full: entry and exit "
                             "bars and prices, return before costs), for review")
    parser.add_argument("--exclude-sector", action="append", default=None, metavar="SECTOR",
                        help="Leave out the lines whose profile sector is SECTOR (any of a line's profile rows) "
                             "after the screen's other tests and before the top-30 cut, listing them per "
                             "rebalance (sector_exclusion in the results). Repeat it for more sectors; off by "
                             "default")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("fcf-conversion", args_str=" ".join(cloud_args),
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
                                    data_checks=args.data_checks, full_holdings=args.full_holdings,
                                    exclude_sector=args.exclude_sector, win=win)
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
               exclude_sector=args.exclude_sector, win=win)


if __name__ == "__main__":
    main()
