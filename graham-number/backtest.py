#!/usr/bin/env python3
"""
Graham Number Backtest

Annual rebalancing, equal weight, top 30 by deepest discount to Graham Number.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: price < Graham Number (sqrt(22.5 × EPS × BVPS)), market cap filter
  Each company's latest FY filing (45-day lag, at most 540 days old), Graham Number restated
  in the price's unit (pence x100, point-in-time USDxxx FX across currencies; no currency, no
  fresh rate or a depositary receipt across currencies drops the line)
Portfolio: Top 30 by deepest discount (price/GN ratio), equal weight. Cash if < 10 qualify.
Rebalancing: Annual (January), 2000-2025.

Graham Number = sqrt(22.5 × EPS × BVPS)
Equivalent filter: P/E × P/B < 22.5

Usage:
    # Backtest US stocks (default)
    python3 graham-number/backtest.py

    # Backtest Indian stocks
    python3 graham-number/backtest.py --exchange BSE,NSE

    # Backtest all exchanges
    python3 graham-number/backtest.py --global

    # Specific exchange preset
    python3 graham-number/backtest.py --preset india --output results/india.json

See README.md for data source setup.
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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns, entry_buyable, get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS, remove_price_oscillations, entry_buyable_prices, LtpExits, add_exit_method_arg, utc_epoch
from data_utils import campaign_listing_condition, campaign_listing_label, isin_dedup_sql, load_isin_map
from metrics import (compute_metrics, compute_annual_returns, format_metrics,
                     period_accounting)
from costs import tiered_cost, apply_costs
from cli_utils import add_common_args, resolve_exchanges, print_header, get_mktcap_threshold

# --- Signal parameters ---
# Graham Number is sqrt(22.5 * EPS * BVPS), pre-computed by FMP as key_metrics.grahamNumber
MAX_STOCKS = 30
MIN_STOCKS = 10
DEFAULT_FREQUENCY = "annual"
MAX_FILING_AGE_DAYS = 540  # a company whose latest FY filing is older drops out (as in graham-net-net)

# Price/GN units: adjClose is in profile.currency, grahamNumber in key_metrics.reportedCurrency.
SUBUNIT_CCY = {"GBp": "GBP", "ZAc": "ZAR", "ILA": "ILS"}  # quoted in 1/100 of the unit (data_utils.QUOTE_CURRENCY)
FX_MAX_STALE_DAYS = 7  # newest USDxxx close on/before the price bar, at most this old


def major_ccy_sql(col):
    """SQL: `col` with sub-unit quotes mapped to their unit (GBp -> GBP)."""
    return f"CASE {col} " + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in SUBUNIT_CCY.items()) + f" ELSE {col} END"


def fetch_fx(client, con, rebalance_dates, verbose=False):
    """fx_cache(ccy, fx_epoch, per_usd): USDxxx closes for every currency a mismatched line needs."""
    ccys = [r[0] for r in con.execute(f"""
        SELECT DISTINCT c FROM (
            SELECT UNNEST([t, r]) AS c FROM (
                SELECT {major_ccy_sql('u.currency')} AS t, m.reportedCurrency AS r
                FROM metrics_cache m JOIN ccy_map u ON u.symbol = m.symbol) WHERE t <> r)
        WHERE c <> 'USD' AND regexp_full_match(c, '[A-Z]{{3}}') ORDER BY c""").fetchall()]
    n = 0
    if ccys:
        wins = " OR ".join(f"(dateEpoch >= {utc_epoch(d - timedelta(days=FX_MAX_STALE_DAYS))}"
                           f" AND dateEpoch <= {utc_epoch(d + timedelta(days=11))})" for d in rebalance_dates)
        # USDxxx (units per USD), not xxxUSD: EURUSD stops in 2026-02 while USDEUR runs on.
        n = query_parquet(client, f"""
            SELECT SUBSTR(symbol, 4, 3) AS ccy, dateEpoch AS fx_epoch, close AS per_usd
            FROM forex_prices_daily
            WHERE symbol IN ({", ".join(f"'USD{c}'" for c in ccys)}) AND close > 0 AND ({wins})
        """, con, "fx_cache", verbose=verbose)
    if not n:  # no mismatched line, or query_parquet's column-less empty placeholder
        con.execute("DROP TABLE IF EXISTS fx_cache")
        con.execute("CREATE TABLE fx_cache(ccy VARCHAR, fx_epoch BIGINT, per_usd DOUBLE)")
    print(f"    -> FX for {len(ccys)} currencies: {n} rows")
    return n


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        ccy_map(symbol, currency, is_adr)  -- profile.currency (adjClose's unit), profile.isAdr
        metrics_cache(symbol, grahamNumber, marketCap, reportedCurrency, filing_epoch, period)
        fx_cache(ccy, fx_epoch, per_usd)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    # Campaign listing rule; sym_filter_sql carries it (the screen skips `universe`).
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

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange, currency, isAdr FROM profile {exchange_where}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")
    # Trading currency per line (the price unit), set against reportedCurrency in screen_stocks.
    con.execute("CREATE TABLE ccy_map AS SELECT UNNEST(?::VARCHAR[]) AS symbol, UNNEST(?::VARCHAR[]) AS currency,"
                " UNNEST(?::BOOLEAN[]) AS is_adr",
                [[r["symbol"] for r in profiles], [r.get("currency") for r in profiles],
                 [r.get("isAdr") for r in profiles]])

    if exchanges:
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}){listing_and})"
    else:
        sym_filter_sql = "1=1"
    # symbol -> ISIN (+ live, home-currency flags) for screen_stocks' same-security dedup, every leg.
    load_isin_map(client, con, exchanges, sym_filter_sql, verbose=verbose)

    # 2. Key metrics: Graham Number, market cap
    # Every FY filing, NULL (undefined) Graham Numbers included: screen_stocks tests each company's LATEST one.
    print("  Fetching key metrics (Graham Number, market cap)...")
    metrics_sql = f"""
        SELECT symbol, grahamNumber, marketCap, reportedCurrency, dateEpoch as filing_epoch, period
        FROM key_metrics
        WHERE period = 'FY'
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache",
                          verbose=verbose, memory_mb=16384, threads=6)
    print(f"    -> {count} rows")

    if count == 0:
        print("  No qualifying data found.")
        return None
    fetch_fx(client, con, rebalance_dates, verbose=verbose)

    # 3. Prices at rebalance dates
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=11)  # covers get_prices' [d+1, d+11] with offset_days=1
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
        SELECT symbol, dateEpoch as trade_epoch, adjClose, volume
        FROM stock_eod
        WHERE ({date_filter})
          AND (
            symbol IN ({bench_list})
            OR symbol IN (
                SELECT DISTINCT symbol FROM key_metrics
                WHERE period = 'FY' AND grahamNumber > 0
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=16384, threads=6)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    remove_price_oscillations(con, verbose=verbose)
    print(f"    -> {count} price rows")

    return con


def gn_lines_sql(target_date, mktcap_min):
    """(sql, params): the screen's CTEs, ending in `conv` with one row per line.

    conv.gn_px is the Graham Number in the price's unit: x100 on sub-unit quotes, USDxxx FX when
    the trading and reporting currencies differ. It is NULL (line dropped) without both
    currencies or a rate at most FX_MAX_STALE_DAYS old on/before the price bar, and on a
    depositary receipt (profile.isAdr) across currencies (conv.dr): FMP's per-share basis for
    receipts is mixed (see graham-net-net), so FX alone can't put the figure per receipt.
    """
    cutoff_epoch = utc_epoch(target_date - timedelta(days=45))
    earliest_epoch = utc_epoch(target_date - timedelta(days=MAX_FILING_AGE_DAYS))
    target_epoch = utc_epoch(target_date)
    end_epoch = utc_epoch(target_date + timedelta(days=10))
    stale = FX_MAX_STALE_DAYS * 86400
    subunits = ", ".join(f"'{k}'" for k in SUBUNIT_CCY)

    # Latest FY filing first, THEN the Graham Number/size tests: testing first kept a company whose
    # EPS or book value turned negative (Graham Number NULL) on its last positive filing, however old.
    sql = f"""
        WITH latest AS (
            SELECT symbol, grahamNumber, marketCap, reportedCurrency,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC,
                    marketCap DESC NULLS LAST, grahamNumber DESC NULLS LAST,
                    reportedCurrency DESC NULLS LAST) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ? AND filing_epoch >= ?
        ),
        km AS (
            SELECT * FROM latest
            WHERE rn = 1 AND grahamNumber > 0 AND marketCap > ?
        ),
        prices AS (
            SELECT symbol, adjClose, trade_epoch
            FROM prices_cache
            WHERE trade_epoch >= ? AND trade_epoch <= ?
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST) = 1
        ),
        base AS (
            SELECT km.symbol, km.marketCap, km.grahamNumber, km.reportedCurrency AS rccy,
                   prices.adjClose, prices.trade_epoch, u.currency AS tccy, {major_ccy_sql('u.currency')} AS tmaj,
                   u.adr
            FROM km
            JOIN prices ON km.symbol = prices.symbol
            LEFT JOIN (SELECT symbol, MAX(currency) AS currency, BOOL_OR(is_adr) AS adr FROM ccy_map GROUP BY symbol) u
                ON u.symbol = km.symbol
            WHERE prices.adjClose > 0
        ),
        bt AS (SELECT b.*, f.per_usd AS t_usd, f.fx_epoch AS t_ep FROM base b
               ASOF LEFT JOIN fx_cache f ON f.ccy = b.tmaj AND b.trade_epoch >= f.fx_epoch),
        br AS (SELECT b.*, f.per_usd AS r_usd, f.fx_epoch AS r_ep FROM bt b
               ASOF LEFT JOIN fx_cache f ON f.ccy = b.rccy AND b.trade_epoch >= f.fx_epoch),
        conv AS (
            SELECT symbol, marketCap, adjClose, adr AND tmaj <> rccy AS dr,
                   grahamNumber * CASE WHEN tccy IN ({subunits}) THEN 100.0 ELSE 1.0 END
                   * CASE WHEN tmaj = rccy THEN 1.0
                          WHEN adr THEN NULL
                          ELSE CASE WHEN tmaj = 'USD' THEN 1.0 WHEN trade_epoch - t_ep <= {stale} THEN t_usd END
                             / CASE WHEN rccy = 'USD' THEN 1.0 WHEN trade_epoch - r_ep <= {stale} THEN r_usd END
                     END AS gn_px
            FROM br
        )
    """
    return sql, [cutoff_epoch, earliest_epoch, mktcap_min, target_epoch, end_epoch]


def ccy_excluded(con, target_date, mktcap_min, receipts=False):
    """Screen lines dropped at target_date: Graham Number can't be put in the price's unit (receipts: DRs only)."""
    ctes, params = gn_lines_sql(target_date, mktcap_min)
    dr = " AND dr" if receipts else ""
    return con.execute(ctes + f" SELECT COUNT(*) FROM conv WHERE gn_px IS NULL{dr}", params).fetchone()[0]


def screen_stocks(con, target_date, mktcap_min, offset_days=1):
    """Screen for stocks trading below Graham Number (latest FY filing, restated in the
    price's unit by gn_lines_sql()).

    Returns list of (symbol, market_cap) tuples, sorted by deepest discount.
    """
    ctes, params = gn_lines_sql(target_date, mktcap_min)
    # Filter: price < Graham Number; rank by discount (price/GN, lower = deeper).
    # Converted before the dedup, so a twin that fails conversion frees its slot.
    candidates = ctes + """
        SELECT symbol, marketCap, adjClose / gn_px AS price_to_gn
        FROM conv
        WHERE adjClose < gn_px
    """
    # One row per ISIN BEFORE the top-N cut, so a collapsed twin frees its slot (1.0 = filter_returns' default floor).
    rows = con.execute(f"""
        SELECT symbol, marketCap
        FROM ({isin_dedup_sql(candidates, target_date, offset_days, 1.0)})
        ORDER BY price_to_gn ASC, symbol
        LIMIT ?
    """, params + [MAX_STOCKS]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY", ltp=None, exit_method="drop"):
    """Run Graham Number backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min, offset_days=offset_days)

        if len(portfolio) < MIN_STOCKS:
            bench_return = get_benchmark_return(
                con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": bench_return,
                "stocks_held": 0,
                "screened": len(portfolio),
                "entry_buyable": None,
                "min_stocks": MIN_STOCKS,
                "holdings": f"CASH ({len(portfolio)} passed)",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _ in portfolio]
        mcaps = {s: mc for s, mc in portfolio}

        entry_prices = get_prices(con, symbols, entry_date, offset_days=offset_days)
        exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)
        # LTP fills only for periods that clear the entry-side cash guard below (1.0 = filter_returns' default floor).
        ltp_fill = (ltp.fill(symbols, entry_prices, exit_prices, entry_date, exit_date,
                             offset_days=offset_days, min_entry_price=1.0)
                    if ltp and entry_buyable_prices(symbols, entry_prices, min_entry_price=1.0) >= MIN_STOCKS else {})
        exit_ltp = {**exit_prices, **ltp_fill}

        def book(xmap):
            # Collect (symbol, entry_price, exit_price, mcap) for quality filtering
            return [(sym, entry_prices.get(sym), xmap.get(sym), mcaps.get(sym))
                    for sym in symbols]

        # raw keeps the DROP exits so the entry-only cash guard below reads it unchanged.
        raw = book(exit_prices)
        clean, _ = filter_returns(book(exit_ltp) if exit_method == "ltp" else raw, verbose=verbose)

        # The cash rule has to be re-checked HERE, not just on the screen count.
        # Screening can pass 30 names while only a handful of them have a usable
        # ENTRY price, and averaging 1-4 survivors reports a single stock's year
        # as a diversified portfolio return.
        #
        # Checked on `buyable`, NOT on len(clean). filter_returns also drops a
        # name for a missing EXIT price and for a realised return over its
        # max_single_return, neither of which is knowable on the rebalance date.
        # Deciding cash from those is look-ahead and it flatters: a period the
        # strategy really held and really lost gets rewritten as a flat 0.0%. If
        # the book cleared the floor at entry the strategy ENTERED, so the
        # exit-side survivors are averaged below exactly as they were before this
        # guard existed. That survivor-averaging has its own known weakness; it
        # is pre-existing, logged in DATA_QUALITY_ISSUES.md, and deliberately
        # not changed here.
        #
        # No min_entry_price here, matching this topic's own filter_returns call,
        # which takes the $1.00 default.
        buyable = entry_buyable(raw)
        if buyable < MIN_STOCKS:
            bench_return = get_benchmark_return(
                con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": bench_return,
                "stocks_held": 0,
                "screened": len(portfolio),
                "entry_buyable": buyable,
                "min_stocks": MIN_STOCKS,
                "holdings": f"CASH ({buyable} buyable at entry of {len(portfolio)} screened)",
            })
            if verbose:
                print(f"    {entry_date}: only {buyable} of {len(portfolio)} screened "
                      f"names were buyable at entry (< {MIN_STOCKS}), CASH")
            continue

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        bench_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(bench_return, 6) if bench_return is not None else None,
            "stocks_held": len(returns),
            "screened": len(portfolio),
            "entry_buyable": buyable,
            "min_stocks": MIN_STOCKS,
            "holdings": ",".join(sym for sym, _, _ in clean[:10]) + ("..." if len(clean) > 10 else ""),
        })
        if exit_method == "both":
            c_ltp, _ = filter_returns(book(exit_ltp))
            r_ltp = [apply_costs(rr, tiered_cost(mc)) if use_costs else rr for _, rr, mc in c_ltp]
            results[-1].update({"portfolio_return_ltp": round(sum(r_ltp) / len(r_ltp), 6) if r_ltp else 0.0,
                                "stocks_held_ltp": len(r_ltp), "ltp_filled": sum(1 for s, _, _ in c_ltp if s in ltp_fill)})

        if verbose:
            excess = ""
            if bench_return is not None:
                excess = f"  ex={((port_return - bench_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, bench={bench_return * 100 if bench_return else 0:.1f}%{excess}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Graham Number backtest")
    add_common_args(parser)
    add_exit_method_arg(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("graham-number", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs
    from cli_utils import get_risk_free_rate
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    offset_days = 0 if args.no_next_day else 1
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "same-day close (legacy)" if args.no_next_day else "next-day close (MOC)"

    signal_desc = (f"price < Graham Number (sqrt(22.5*EPS*BVPS)), "
                   f"MCap > {mktcap_threshold/1e9:.0f}B local, top {MAX_STOCKS} by discount")
    print_header("GRAHAM NUMBER BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available. Exiting.")
        sys.exit(1)
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")
    # Screen lines per rebalance with no currency, no fresh FX or a cross-currency DR (dropped, not compared raw).
    ccy_excl = {d.isoformat(): ccy_excluded(con, d, mktcap_threshold) for d in rebalance_dates[:-1]}
    dr_excl = sum(ccy_excluded(con, d, mktcap_threshold, receipts=True) for d in rebalance_dates[:-1])

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    # prices_cache only holds [d, d+11] windows, so LTP reads remote stock_eod.
    ltp = LtpExits(cr, con, verbose=args.verbose) if args.exit_method != "drop" else None
    results = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=args.verbose,
                           offset_days=offset_days, benchmark_symbol=benchmark_symbol,
                           ltp=ltp, exit_method=args.exit_method)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")
    print(f"  Graham Number currency: {sum(ccy_excl.values())} candidate lines excluded"
          f" (no currency or fresh FX; {dr_excl} cross-currency receipts)")

    # Phase 3: Compute metrics
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)

    print(format_metrics(metrics, "Graham Number", benchmark_name))
    ltp_block = ltp.results_block(args.exit_method, valid, periods_per_year, risk_free_rate) if ltp else None

    # Portfolio metadata
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

    # Annual returns
    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'Graham No.':>12} {benchmark_name:>10} {'Excess':>10}")
        print("  " + "-" * 44)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    # Save results
    if args.output:
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

        output = {
            "universe": universe_name,
            # B006: n_periods, total_rebalances, cash_periods, invested_periods
            # and the window-provenance block. `executed` is every rebalance the
            # strategy ran; `valid` is the benchmark-priced subset metrics use.
            # No key below may re-declare one of these -- gate 8 enforces that.
            **period_accounting(
                [r for r in results if r["portfolio_return"] is not None],
                valid, cash_periods, universe_name=universe_name),
            "years": round(len(valid) / periods_per_year, 1),
            "frequency": frequency,
            "listing_filter": campaign_listing_label(exchanges),
            "isin_dedup": True,
            "gn_ccy_excluded": {"total": sum(ccy_excl.values()), "receipts": dr_excl, "by_period": ccy_excl},
            "avg_stocks_when_invested": round(avg_stocks, 1),
            "period_data": results,
            **({"exit_method": args.exit_method, "ltp": ltp_block} if ltp_block else {}),
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
            # Backward-compat top-level fields
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
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
