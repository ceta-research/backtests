#!/usr/bin/env python3
"""
Defensive Sector Quality Backtest

Annual rebalancing (July), equal weight, top 30 by dividend yield.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Stocks in Consumer Defensive, Utilities, or Healthcare sectors with
        ROE > 6%, OPM > 8%, D/E < 2.5, Dividend Yield > 0.5%, MCap > local threshold.
Universe: ordinary shares only, and the dividend-yield rule (data_utils universe rules ur-1,
        METHODOLOGY.md): preferreds, notes, bonds, units, warrants, rights and funds are off the
        sector map; a vendor yield above 25%, or one computed across two currencies that disagrees
        with the company's own dividends paid, drops the line before the top-30 cut.
Portfolio: Top 30 by dividend yield (income-ranked), equal weight. Cash if < 10 qualify.
Rebalancing: Annual (July), from 2000 to the latest close (METHODOLOGY.md, "Run window"); the published
        window is 2000-2025 (--end-year 2025). Data checks dc-1 in place of a return cap.

Academic reference: Novy-Marx, R. (2013). "The Other Side of Value: The Gross
Profitability Premium." Journal of Financial Economics 108(1), 1-28.
Defensive sectors show persistent risk-adjusted outperformance during market stress.

Usage:
    # Backtest US stocks (default)
    python3 defensive-quality/backtest.py

    # Backtest Indian stocks
    python3 defensive-quality/backtest.py --preset india

    # Backtest all exchanges (loop)
    python3 defensive-quality/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 defensive-quality/backtest.py --no-costs

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
from data_utils import (query_parquet, get_prices, RunWindow, filter_returns,
                        entry_buyable, entry_usable, LtpExits, add_exit_method_arg,
                        get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS,
                         remove_price_oscillations, utc_epoch,
                        campaign_listing_condition, campaign_listing_label, isin_dedup_sql, load_isin_map,
                        DATA_CHECKS_V1, DataChecks, clean_price_cache, mark_nvdr_twins, load_cap_check,
                        load_splits, empty_cap_check, cap_used_sql, CAP_FAIL_TEST,
                        UniverseRules, non_ordinary_sql, yield_rule_sql)
from metrics import (compute_metrics, format_metrics,
                     period_accounting)
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, add_end_date_arg, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
DEFENSIVE_SECTORS = ("Consumer Defensive", "Utilities", "Healthcare")
ROE_MIN = 0.06              # Return on equity > 6% (lower bar: utilities are capital-intensive)
OPM_MIN = 0.08              # Operating profit margin > 8%
DE_MAX = 2.5                # Debt/equity < 2.5 (utilities carry structural debt)
DIV_YIELD_MIN = 0.005       # Dividend yield > 0.5%
MAX_STOCKS = 30             # Top 30 by dividend yield
MIN_STOCKS = 10             # Hold cash if fewer than 10 qualify
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [7]  # July (FY filings available, market settled from Q1 earnings)
MIN_ENTRY_PRICE = 1.0       # Skip stocks with entry price < $1 (price data artifact)
# No return cap: data checks dc-1 (traded bars, NVDR twins, market-cap check at the floor) clean the
# inputs, and every kept move over +100% or under -50% is listed in the results.
DATA_CHECKS = DATA_CHECKS_V1
PAID_CACHE_DDL = "CREATE TABLE paid_cache(symbol VARCHAR, filing_epoch BIGINT, paid DOUBLE, reportedCurrency VARCHAR)"


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False, mktcap_min=None, dc=None, ur=None,
                       win=None):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        sector_map(symbol VARCHAR, sector VARCHAR, exchange VARCHAR): ordinary lines only
        line_ccy(symbol, currency): each kept line's trading currency
        metrics_cache(symbol, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, operatingProfitMargin, debtToEquityRatio, dividendYield,
                     reportedCurrency, filing_epoch, period)
        paid_cache(symbol, filing_epoch, paid, reportedCurrency): FY dividends paid
        prices_cache(symbol, trade_epoch, adjClose, close, volume)
        cap_check (empty unless dc is on), and with dc on: splits, prices_dropped

    dc: a data_utils.DataChecks; when on, the fetch runs checks T, N and M (mktcap_min is the floor).
    ur: a data_utils.UniverseRules, which records the lines the ordinary-shares rule removes.
    win: the leg's data_utils.RunWindow; its fetch_end cuts the price windows at the run's end date.
    Returns DuckDB connection or None.
    """
    checks = dc is not None and dc.on
    ur = ur if ur is not None else UniverseRules()
    win = win if win is not None else RunWindow.typed()
    sectors_sql = "', '".join(DEFENSIVE_SECTORS)

    bench_symbols = {"'SPY'"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(f"'{sym}'")
    bench_list = ", ".join(bench_symbols)

    # Campaign listing rule; the sector map, sym_filter_sql and the price universe all carry it.
    listing_cond = campaign_listing_condition(exchanges)
    listing_and = f" AND {listing_cond}" if listing_cond else ""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter}){listing_and}"
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}){listing_and})"
        sector_sym_sql = (f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}){listing_and} "
                          f"AND sector IN ('{sectors_sql}'))")
    else:
        exchange_where = ""
        sym_filter_sql = "1=1"
        sector_sym_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE sector IN ('{sectors_sql}'))"
    price_universe_filter = f"(symbol IN ({bench_list}) OR {sector_sym_sql})"
    if listing_cond:
        print(f"  Listing filter: {listing_cond}")

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Sector map — current snapshot (sectors rarely change for established companies)
    # No isActivelyTrading filter: today's snapshot would drop names delisted since (B041).
    # The sector filter is the fund guard: FMP files nearly all funds/ETFs under Financial Services.
    # Ordinary shares only (universe rules ur-1): the CASE names why a line is not one, and those lines
    # stay out of the sector map.
    print("  Fetching sector & exchange mappings...")
    sector_sql = f"""
        SELECT DISTINCT symbol, sector, exchange, currency, cik, companyName,
               {non_ordinary_sql("p")} AS non_ordinary
        FROM profile p
        {exchange_where}
        AND sector IN ('{sectors_sql}')
    """
    sector_rows = client.query(sector_sql, verbose=verbose, timeout=120)
    if not sector_rows:
        print("  No symbols found for these exchanges.")
        return None
    kept = ur.sector_rows(sector_rows)
    print(f"  Defensive universe: {ur.lines} symbols, {len(ur.removed)} not ordinary shares")
    if not kept:
        print("  No ordinary shares in this universe.")
        return None

    con.execute("CREATE TABLE sector_map(symbol VARCHAR, sector VARCHAR, exchange VARCHAR)")
    con.executemany("INSERT INTO sector_map VALUES (?, ?, ?)",
                    sorted({(r["symbol"], r["sector"], r["exchange"]) for r in kept}))
    ccy = {}   # one trading currency per line: its first non-null profile currency
    for r in sorted(kept, key=lambda r: (r["symbol"], r["currency"] is None, str(r["currency"]))):
        ccy.setdefault(r["symbol"], r["currency"])
    con.execute("CREATE TABLE line_ccy(symbol VARCHAR, currency VARCHAR)")
    con.executemany("INSERT INTO line_ccy VALUES (?, ?)", sorted(ccy.items()))
    # symbol -> ISIN (+ live, home-currency flags) for screen_stocks' same-security dedup, every leg.
    load_isin_map(client, con, exchanges, sym_filter_sql, verbose=verbose)
    if checks:   # check N: SET NVDR lines share their ordinary line's security key
        dc.record_twins_linked(mark_nvdr_twins(con))

    # 2. Key metrics FY (ROE, market cap)
    print("  Fetching key metrics (ROE, market cap)...")
    km_sql = f"""
        SELECT symbol, returnOnEquity, marketCap, dateEpoch AS filing_epoch, period
        FROM key_metrics
        WHERE period = 'FY'
          AND returnOnEquity IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, km_sql, con, "metrics_cache", verbose=verbose)
    print(f"    -> {count} rows")

    # Check M (the floor reads cap_used) and the splits check F flags; cap_check stays empty when off.
    if checks:
        print("  Fetching market-cap check inputs and splits...")
        dc.record_cap(load_cap_check(client, con, sym_filter_sql, mktcap_min, dc.cfg, verbose=verbose))
        load_splits(client, con, sym_filter_sql, verbose=verbose)
    else:
        empty_cap_check(con)

    # 3. Financial ratios FY (OPM, D/E, dividend yield and its reporting currency)
    print("  Fetching financial ratios (OPM, D/E, dividend yield)...")
    fr_sql = f"""
        SELECT symbol, operatingProfitMargin, debtToEquityRatio, dividendYield, reportedCurrency,
               dateEpoch AS filing_epoch, period
        FROM financial_ratios
        WHERE period = 'FY'
          AND operatingProfitMargin IS NOT NULL
          AND {sym_filter_sql}
    """
    count = query_parquet(client, fr_sql, con, "ratios_cache", verbose=verbose)
    print(f"    -> {count} rows")

    # 3b. FY dividends paid, for the yield rule's cross-currency check
    print("  Fetching dividends paid (cash flow statement)...")
    cf_sql = f"""
        SELECT symbol, dateEpoch AS filing_epoch,
               COALESCE(NULLIF(commonDividendsPaid, 0), netDividendsPaid) AS paid, reportedCurrency
        FROM cash_flow_statement
        WHERE period = 'FY'
          AND {sector_sym_sql}
    """
    count = query_parquet(client, cf_sql, con, "paid_cache", verbose=verbose)
    if not count:   # query_parquet's empty-result placeholder has none of the columns
        con.execute("DROP TABLE paid_cache")
        con.execute(PAID_CACHE_DDL)
    print(f"    -> {count} rows")

    # 4. Prices (at rebalance dates + SPY)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = win.fetch_end(d, 11)  # covers get_prices' [d+1, d+11] with offset_days=1
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    price_sql = f"""
        SELECT symbol, dateEpoch AS trade_epoch, adjClose, close, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND {price_universe_filter}
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5_000_000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    if checks:   # check T: closed days, zero-volume and flat lines, non-positive prices, sibling-class gaps
        dc.record_clean(clean_price_cache(con, bench_symbols, rebalance_dates, dc.cfg, siblings="cap_inputs"))
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def _candidates_sql(cap_test):
    """The screen's candidate SELECT, every line that passes the quality filters and the market-cap floor,
    with the yield rule's reason (yield_rule, NULL when the line stays) and the inputs it read. The floor
    reads cap_used (check M), the screen reports cap_used as the market cap, and the km tie-break stays on
    the raw cap. cap_test is the floor clause ("km.cap_used > ?", or CAP_FAIL_TEST for M's events, which
    also read km_epoch and raw_cap). The yield rule's paid yield reads the same FY's dividends paid and its
    key_metrics cap; the reporting currency is that FY's cash-flow row's, else the ratios row's."""
    report_ccy = "COALESCE(cf.reportedCurrency, fr.reportedCurrency)"
    return f"""
        WITH km AS (
            SELECT m.symbol, m.returnOnEquity, m.marketCap, m.filing_epoch,
                {cap_used_sql()} AS cap_used,
                ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.filing_epoch DESC,
                    m.marketCap DESC NULLS LAST, m.returnOnEquity DESC NULLS LAST) AS rn
            FROM metrics_cache m
            LEFT JOIN cap_check cc ON cc.symbol = m.symbol AND cc.filing_epoch = m.filing_epoch
            WHERE m.filing_epoch <= ? AND m.filing_epoch >= ?
        ),
        fr AS (
            SELECT symbol, operatingProfitMargin, debtToEquityRatio, dividendYield, reportedCurrency,
                   filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC,
                    dividendYield DESC NULLS LAST, operatingProfitMargin DESC NULLS LAST,
                    debtToEquityRatio DESC NULLS LAST) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ? AND filing_epoch >= ?
        ),
        cf AS (
            SELECT symbol, filing_epoch, paid, reportedCurrency,
                ROW_NUMBER() OVER (PARTITION BY symbol, filing_epoch ORDER BY ABS(paid) DESC NULLS LAST,
                    reportedCurrency) AS rn
            FROM paid_cache
        ),
        mc AS (SELECT symbol, filing_epoch, MAX(marketCap) AS marketCap FROM metrics_cache GROUP BY ALL)
        SELECT sm.symbol, km.cap_used AS marketCap, fr.dividendYield,
               km.filing_epoch AS km_epoch, km.marketCap AS raw_cap,
               fr.filing_epoch AS fy_epoch, cf.paid AS fy_paid, mc.marketCap AS fy_cap,
               lc.currency AS trade_ccy, {report_ccy} AS report_ccy,
               {yield_rule_sql("fr.dividendYield", "cf.paid", "mc.marketCap", "lc.currency", report_ccy)} AS yield_rule
        FROM sector_map sm
        JOIN km ON sm.symbol = km.symbol AND km.rn = 1
        JOIN fr ON sm.symbol = fr.symbol AND fr.rn = 1
        LEFT JOIN cf ON cf.symbol = sm.symbol AND cf.filing_epoch = fr.filing_epoch AND cf.rn = 1
        LEFT JOIN mc ON mc.symbol = sm.symbol AND mc.filing_epoch = fr.filing_epoch
        LEFT JOIN line_ccy lc ON lc.symbol = sm.symbol
        WHERE km.returnOnEquity > ?
          AND fr.operatingProfitMargin > ?
          AND (fr.debtToEquityRatio IS NULL OR fr.debtToEquityRatio < ?)
          AND (fr.dividendYield IS NULL OR fr.dividendYield > ?)
          AND {cap_test}
    """


def _kept(candidates_sql, rule="IS NULL"):
    """The candidates the yield rule keeps ("IS NULL") or drops ("IS NOT NULL")."""
    return f"SELECT * FROM ({candidates_sql}) _y WHERE _y.yield_rule {rule}"


def screen_stocks(con, target_date, mktcap_min, offset_days=1, dc=None, ur=None):
    """Screen for quality stocks in defensive sectors.

    Returns list of (symbol, market_cap, dividend_yield) sorted by dividend_yield DESC.
    Top MAX_STOCKS are selected, after the yield rule and the security dedup.
    dc (data_utils.DataChecks): when on, NVDR twins share a security (check N); it also records the
    screen's twin and market-cap events. ur (data_utils.UniverseRules) records the yield rule's drops.
    """
    # 45-day lag for point-in-time (July rebalance → filings through ~May 17)
    cutoff_epoch = utc_epoch(target_date - timedelta(days=45))
    # Staleness: last 18 months only, else a delisted name's last FY row keeps a slot forever (B041)
    earliest_epoch = utc_epoch(target_date - timedelta(days=540))

    candidates = _candidates_sql("km.cap_used > ?")
    params = [cutoff_epoch, earliest_epoch, cutoff_epoch, earliest_epoch,
              ROE_MIN, OPM_MIN, DE_MAX, DIV_YIELD_MIN, mktcap_min]
    kept = _kept(candidates)
    # One row per security BEFORE the top-N cut, so a collapsed twin (or a symbol the sector_map
    # join fans out) or a line the yield rule drops frees its slot.
    rows = con.execute(f"""
        SELECT symbol, marketCap, dividendYield
        FROM ({isin_dedup_sql(kept, target_date, offset_days, MIN_ENTRY_PRICE,
                              twin_key=dc is not None and dc.on)})
        ORDER BY dividendYield DESC, symbol
        LIMIT ?
    """, params + [MAX_STOCKS]).fetchall()

    picked = [(r[0], r[1], r[2]) for r in rows]
    if ur is not None:
        ur.yield_drops(con.execute(f"""
            SELECT DISTINCT symbol, yield_rule, fy_epoch, dividendYield, fy_paid, fy_cap, trade_ccy, report_ccy
            FROM ({_kept(candidates, "IS NOT NULL")})""", params).fetchall(), target_date)
    if dc is not None:
        dc.twins(con, kept, params, target_date, offset_days, MIN_ENTRY_PRICE, [s for s, _, _ in picked])
        dc.caps(con, _kept(_candidates_sql(CAP_FAIL_TEST)), params + [mktcap_min], target_date, mktcap_min)
    return picked


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY", ltp=None, ur=None, exit_method="drop", dc=None, win=None):
    """Run Defensive Quality backtest. Returns list of period result dicts.

    dc: the leg's data_utils.DataChecks. Without one, no check runs (the "uncapped" mode); either
    way there is no return cap. ur: the leg's data_utils.UniverseRules (records the yield drops).
    win: the leg's data_utils.RunWindow (its grid made rebalance_dates): the partial exit is priced at
    the close on the end date (the caller marks the rows: RunWindow.mark)."""
    results = []
    dc = dc if dc is not None else DataChecks("uncapped", DATA_CHECKS)
    win = win if win is not None else RunWindow.typed()

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]
        x_off = win.exit_offset(exit_date, offset_days)

        portfolio = screen_stocks(con, entry_date, mktcap_min, offset_days=offset_days, dc=dc, ur=ur)

        # Benchmark return
        spy_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days, exit_offset_days=x_off)

        if len(portfolio) < MIN_STOCKS:
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
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

        def book(xmap, verbose=verbose):
            # symbol_data with the exit slot taken from xmap
            clean, skipped = filter_returns([(sym, ep, xmap.get(sym), mcap) for sym, ep, _, mcap in symbol_data],
                                            min_entry_price=MIN_ENTRY_PRICE,
                                            max_single_return=None,   # no return cap (data checks dc-1)
                                            verbose=verbose)
            returns = []
            for sym, raw_ret, mcap in clean:
                if use_costs:
                    cost = tiered_cost(mcap)
                    net_ret = apply_costs(raw_ret, cost)
                else:
                    net_ret = raw_ret
                returns.append(net_ret)
            return clean, returns

        clean, returns = book(exit_ltp if exit_method == "ltp" else exit_prices)

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
                "spy_return": spy_return,
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
        booked = exit_ltp if exit_method == "ltp" else exit_prices

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
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
            c_ltp, r_ltp = book(exit_ltp, verbose=False)
            results[-1].update({"portfolio_return_ltp": round(sum(r_ltp) / len(r_ltp), 6) if r_ltp else 0.0,
                                "stocks_held_ltp": len(r_ltp), "ltp_filled": sum(1 for s, _, _ in c_ltp if s in ltp_fill)})

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, spy={spy_return * 100 if spy_return else 0:.1f}%{excess}")

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
               data_checks="on", full_holdings=False, win=None):
    """Run backtest for a single exchange set. Returns output dict or None.

    data_checks: "on" (data checks dc-1), or "uncapped" (no check and no cap: attribution only). The
    universe rules ur-1 apply in both. full_holdings: every invested period row also lists each booked
    name (holdings_full). win: the run's data_utils.RunWindow (main builds it from --end-date or
    --end-year; required)."""
    if win is None:
        raise TypeError("run_single needs win: a data_utils.RunWindow (--end-date or --end-year)")
    win = win.for_leg(universe_name)
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]
    dc = DataChecks(data_checks, DATA_CHECKS, full_holdings=full_holdings)
    ur = UniverseRules()
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9
                    else f"{mktcap_threshold/1e6:.0f}M")
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "same-day close" if offset_days == 0 else "next-day close (MOC)"
    signal_desc = (f"Sectors: {', '.join(DEFENSIVE_SECTORS)}, "
                   f"ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%, "
                   f"D/E < {DE_MAX}, DivYield > {DIV_YIELD_MIN*100:.1f}%, "
                   f"MCap > {mktcap_label} local, top {MAX_STOCKS} by yield")
    print_header("DEFENSIVE SECTOR QUALITY BACKTEST", universe_name, exchanges, signal_desc)
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
    ltp = (LtpExits(cr, con, verbose=verbose, traded_only=dc.on, end_date=win.end_date)
           if exit_method != "drop" else None)
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs,
                           verbose=verbose, offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol, ltp=ltp, ur=ur, win=win,
                           exit_method=exit_method, dc=dc)
    win.mark(results)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")
    print(f"  {dc.summary()}")
    ub = ur.block()
    print(f"  Universe rules: {ub['ordinary_shares']['removed_count']} lines not ordinary shares "
          f"{ub['ordinary_shares']['by_reason']}, yield drops {ub['dividend_yield']['by_reason']}")

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
    print(format_metrics(metrics, "Defensive Quality", benchmark_name))
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
        bench_col = benchmark_name[:10]
        print(f"\n  {'Year':<8} {'DefQuality':>12} {bench_col:>10} {'Excess':>10}")
        print("  " + "-" * 44)
        for ar in annual + ([{**partial_year, "year": f"{partial_year['year']}p"}] if partial_year else []):
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}%"
                  f" {ar['benchmark']*100:>9.1f}% {ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks)
    output.update({"years": win.years(valid, periods_per_year),
                   **win.fields(valid, periods_per_year, partial_year,
                                win.priced_to(con, benchmark_symbol, valid, offset_days)),
                   "listing_filter": campaign_listing_label(exchanges), "isin_dedup": True})
    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both
    output["universe_rules"] = ub        # equal in drop, ltp and both (uncapped, yield drops read the raw-cap floor)
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
    parser = argparse.ArgumentParser(description="Defensive Sector Quality backtest")
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
        result = run_backtest_cloud("defensive-quality", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs

    # --global mode: loop all presets
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        all_results = {}
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),
            ("china",       ["SHZ", "SHH"]),
            ("hongkong",    ["HKSE"]),
            ("thailand",    ["SET"]),
            ("japan",       ["JPX"]),
            ("uk",          ["LSE"]),
            ("germany",     ["XETRA"]),
            ("taiwan",      ["TAI", "TWO"]),
            ("korea",       ["KSC"]),
            ("canada",      ["TSX"]),
            ("switzerland", ["SIX"]),
            ("sweden",      ["STO"]),
            # JNB excluded: thin defensive universe, never reaches MIN_STOCKS=10
            ("malaysia",    ["KLS"]),
            ("indonesia",   ["JKT"]),
        ]

        offset_days = 0 if args.no_next_day else 1
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        # one end date for every leg: the earliest leg's latest close, or the pin
        win = RunWindow.from_args(args, cr, {"_".join(ex): (ex, get_local_benchmark(ex)[0])
                                             for _, ex in presets_to_run})

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
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR / NO DATA':}")
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
    offset_days = 0 if args.no_next_day else 1
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    win = RunWindow.from_args(args, cr, {universe_name: (exchanges, get_local_benchmark(exchanges)[0])})
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, args.verbose, args.output, offset_days=offset_days,
               exit_method=args.exit_method, data_checks=args.data_checks, full_holdings=args.full_holdings,
               win=win)


if __name__ == "__main__":
    main()
