#!/usr/bin/env python3
"""
EV/EBITDA Sector-Relative Timing Backtest

Annual rebalancing, equal weight, top 30 by deepest EV/EBITDA discount to sector peers.
Fetches data via configurable provider, caches in DuckDB, runs locally.

Signal: Stock's EV/EBITDA < 70% of its sector's median EV/EBITDA (same exchange),
        with quality fundamentals intact (ROE > 8%, D/E < 2.0, EV/EBITDA 0.5-25).
Portfolio: Top 30 by lowest (stock EV/EBITDA / sector median EV/EBITDA), equal weight.
           Cash if < 10 stocks qualify.
Rebalancing: Annual (January), 2000-2025.

Distinct from EV/EBITDA Value Screen (value-03-ev-ebitda):
  - Value screen uses absolute threshold (EV/EBITDA < 10x, universal)
  - This uses SECTOR MEDIAN as baseline (relative sector timing)
  - Captures: "stock is cheap vs sector peers now" rather than "cheap vs the market"
  - Works in high-valuation environments where sector-level P/E is elevated
  - More resilient to sector-specific valuation regimes (tech vs utilities vs energy)

Advantage of EV/EBITDA over P/E for sector-relative timing:
  - Comparable across different capital structures (debt level does not distort)
  - Not affected by tax rate differences across geographies
  - Works for companies with high interest expenses that depress EPS

Academic reference:
  Loughran, T. & Wellman, J.W. (2011). New Evidence on the Relation between the
  Enterprise Multiple and Average Stock Returns. Journal of Financial and Quantitative
  Analysis, 46(6), 1629-1650.
  (EV/EBITDA as a value signal predicts returns across international markets.)

Usage:
    # Backtest US stocks (default)
    python3 ev-ebitda-relative/backtest.py

    # Backtest Indian stocks
    python3 ev-ebitda-relative/backtest.py --preset india

    # Backtest all exchanges
    python3 ev-ebitda-relative/backtest.py --global --output results/exchange_comparison.json --verbose

    # Without transaction costs
    python3 ev-ebitda-relative/backtest.py --no-costs

See README.md for strategy details.
"""

import argparse
import duckdb
import json
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# --- Signal parameters ---
EV_EBITDA_MIN = 0.5            # Exclude negative / near-zero EBITDA
EV_EBITDA_MAX = 25.0           # Exclude speculative / distressed extremes
SECTOR_RATIO_MAX = 0.70        # Stock EV/EBITDA must be < 70% of sector median (30%+ discount)
ROE_MIN = 0.08                 # Return on equity > 8% (fundamental quality)
DE_MAX = 2.0                   # Debt-to-equity < 2.0 (not dangerously leveraged)
MAX_STOCKS = 30                # Top 30 by sector-relative compression, equal weight
MIN_STOCKS = 10                # Hold cash if fewer qualify
SECTOR_MIN_STOCKS = 5          # Need at least 5 stocks in sector to compute meaningful median
DEFAULT_FREQUENCY = "annual"
DEFAULT_REBALANCE_MONTHS = [1]  # January
MAX_SINGLE_RETURN = 2.0        # Cap individual stock returns at 200% (data quality guard)
MIN_ENTRY_PRICE = 1.0          # Skip stocks with entry price < $1 (price data artifact)

# Exchanges confirmed to have FY key_metrics + financial_ratios data.
# Excluded: ASX/SAO (adjClose artifacts).
GLOBAL_PRESETS = [
    ("us",           ["NYSE", "NASDAQ", "AMEX"]),
    ("india",        ["BSE", "NSE"]),
    ("japan",        ["JPX"]),
    ("uk",           ["LSE"]),
    ("china",        ["SHZ", "SHH"]),
    ("hongkong",     ["HKSE"]),
    ("taiwan",       ["TAI", "TWO"]),
    ("thailand",     ["SET"]),
    ("germany",      ["XETRA"]),
    ("korea",        ["KSC"]),
    ("canada",       ["TSX"]),
    ("sweden",       ["STO"]),
    ("switzerland",  ["SIX"]),
    ("norway",       ["OSL"]),
    ("southafrica",  ["JNB"]),
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        profile_cache(symbol VARCHAR, sector VARCHAR, exchange VARCHAR)
        metrics_cache(symbol, evToEBITDA, returnOnEquity, marketCap, filing_epoch, period)
        ratios_cache(symbol, debtToEquityRatio, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
        exchange_and = f"AND exchange IN ({ex_filter})"
    else:
        exchange_where = ""
        exchange_and = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe with sector classification
    print("  Fetching exchange membership and sectors...")
    profile_sql = f"SELECT DISTINCT symbol, exchange, sector FROM profile {exchange_where}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    profiles = [p for p in profiles if p.get("sector")]  # require sector
    print(f"  Universe: {len(profiles)} symbols with sector data")

    sym_values = ",".join(
        f"('{r['symbol']}','{r['exchange']}','{r['sector'].replace(chr(39), chr(39)+chr(39))}')"
        for r in profiles
    )
    con.execute("CREATE TABLE profile_cache(symbol VARCHAR, exchange VARCHAR, sector VARCHAR)")
    con.execute(f"INSERT INTO profile_cache VALUES {sym_values}")

    if exchanges:
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        sym_filter_sql = "1=1"

    # 2. Key metrics — EV/EBITDA, ROE, market cap (all FY)
    queries = [
        ("metrics_cache", f"""
            SELECT symbol, evToEBITDA, returnOnEquity, marketCap,
                   dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY'
              AND evToEBITDA IS NOT NULL
              AND evToEBITDA > 0
              AND returnOnEquity IS NOT NULL
              AND marketCap IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (EV/EBITDA, ROE, market cap)"),
        ("ratios_cache", f"""
            SELECT symbol, debtToEquityRatio,
                   dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY'
              AND {sym_filter_sql}
        """, "financial ratios (D/E)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              memory_mb=4096, threads=2)
        print(f"    -> {count} rows")

    if con.execute("SELECT COUNT(*) FROM metrics_cache").fetchone()[0] == 0:
        print("  No FY key_metrics data found. This exchange may not have EV/EBITDA data.")
        con.close()
        return None

    # 3. Prices (only at rebalance dates)
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
                SELECT DISTINCT symbol FROM key_metrics WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for sector-relatively cheap stocks with quality fundamentals.

    Returns list of (symbol, market_cap) tuples sorted by EV/EBITDA ratio ASC.

    Logic:
    - For each symbol, get the most recent FY EV/EBITDA filing within 45-day lag
    - Compute the sector median EV/EBITDA across all stocks in that exchange/sector at this date
    - Screen for stocks where (stock EV/EBITDA / sector median EV/EBITDA) < SECTOR_RATIO_MAX
    - Apply quality filters: ROE > 8%, D/E < 2.0, market cap threshold
    - Uses 45-day lag for point-in-time data integrity
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=45), datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH latest_metrics AS (
            -- Most recent FY EV/EBITDA + ROE + mktcap per symbol as of cutoff
            SELECT
                m.symbol,
                m.evToEBITDA AS ev_ebitda,
                m.returnOnEquity,
                m.marketCap,
                ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.filing_epoch DESC) AS rn
            FROM metrics_cache m
            WHERE m.filing_epoch <= ?
              AND m.evToEBITDA BETWEEN ? AND ?
        ),
        latest_ratios AS (
            -- Most recent FY D/E per symbol as of cutoff
            SELECT
                symbol, debtToEquityRatio,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ?
        ),
        current_stocks AS (
            -- Join metrics, ratios, sector info
            SELECT
                lm.symbol,
                pc.sector,
                lm.ev_ebitda,
                lm.returnOnEquity,
                lm.marketCap,
                lr.debtToEquityRatio
            FROM latest_metrics lm
            LEFT JOIN latest_ratios lr ON lm.symbol = lr.symbol AND lr.rn = 1
            JOIN profile_cache pc ON lm.symbol = pc.symbol
            WHERE lm.rn = 1
              AND lm.returnOnEquity > ?
              AND (lr.debtToEquityRatio IS NULL
                   OR (lr.debtToEquityRatio >= 0 AND lr.debtToEquityRatio < ?))
              AND lm.marketCap > ?
        ),
        sector_medians AS (
            -- Sector median EV/EBITDA (requires at least SECTOR_MIN_STOCKS stocks per sector)
            SELECT
                sector,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ev_ebitda) AS median_ev_ebitda,
                COUNT(*) AS n_sector_stocks
            FROM current_stocks
            GROUP BY sector
            HAVING COUNT(*) >= ?
        )
        SELECT
            cs.symbol,
            cs.marketCap,
            ROUND(cs.ev_ebitda / sm.median_ev_ebitda, 4) AS ev_ratio_to_sector
        FROM current_stocks cs
        JOIN sector_medians sm ON cs.sector = sm.sector
        WHERE cs.ev_ebitda / sm.median_ev_ebitda < ?
        ORDER BY cs.ev_ebitda / sm.median_ev_ebitda ASC
        LIMIT ?
    """, [cutoff_epoch,
          EV_EBITDA_MIN, EV_EBITDA_MAX,
          cutoff_epoch,
          ROE_MIN, DE_MAX, mktcap_min,
          SECTOR_MIN_STOCKS,
          SECTOR_RATIO_MAX,
          MAX_STOCKS]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run EV/EBITDA sector-relative backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            spy_prices_entry = get_prices(con, ["SPY"], entry_date)
            spy_prices_exit = get_prices(con, ["SPY"], exit_date)
            spy_return = None
            if "SPY" in spy_prices_entry and "SPY" in spy_prices_exit and spy_prices_entry["SPY"] > 0:
                spy_return = (spy_prices_exit["SPY"] - spy_prices_entry["SPY"]) / spy_prices_entry["SPY"]

            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
                "stocks_held": 0,
                "holdings": f"CASH ({len(portfolio)} passed)",
            })
            if verbose:
                print(f"    {entry_date}: {len(portfolio)} passed (< {MIN_STOCKS}), CASH")
            continue

        symbols = [s for s, _ in portfolio]
        mcaps = {s: mc for s, mc in portfolio}

        entry_prices = get_prices(con, symbols, entry_date)
        exit_prices = get_prices(con, symbols, exit_date)

        symbol_data = [(sym, entry_prices.get(sym), exit_prices.get(sym), mcaps.get(sym))
                       for sym in symbols]
        clean, skipped = filter_returns(symbol_data,
                                        min_entry_price=MIN_ENTRY_PRICE,
                                        max_single_return=MAX_SINGLE_RETURN,
                                        verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        spy_prices_entry = get_prices(con, ["SPY"], entry_date)
        spy_prices_exit = get_prices(con, ["SPY"], exit_date)
        spy_return = None
        if "SPY" in spy_prices_entry and "SPY" in spy_prices_exit and spy_prices_entry["SPY"] > 0:
            spy_return = (spy_prices_exit["SPY"] - spy_prices_entry["SPY"]) / spy_prices_entry["SPY"]

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "stocks_held": len(returns),
            "holdings": ",".join(symbols[:10]) + ("..." if len(symbols) > 10 else ""),
        })

        if verbose:
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(returns)} stocks, "
                  f"port={port_return * 100:.1f}%, spy={spy_return * 100 if spy_return else 0:.1f}%{excess}")

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
        "n_periods": len(valid),
        "years": round(len(valid) / periods_per_year, 1),
        "frequency": frequency,
        "cash_periods": cash_periods,
        "invested_periods": len(valid) - cash_periods,
        "avg_stocks_when_invested": round(avg_stocks, 1),
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
               risk_free_rate, mktcap_threshold, verbose, output_path=None):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]

    signal_desc = (f"EV/EBITDA {EV_EBITDA_MIN}-{EV_EBITDA_MAX}, "
                   f"sector-ratio < {SECTOR_RATIO_MAX*100:.0f}% of sector median, "
                   f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, "
                   f"MCap > {mktcap_threshold/1e9:.0f}B local, top {MAX_STOCKS}")
    print_header("EV/EBITDA SECTOR-RELATIVE TIMING BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(2000, 2025, frequency,
                                               months=DEFAULT_REBALANCE_MONTHS)
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=verbose)
    if con is None:
        print("No data available. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest (2000-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs, verbose=verbose)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    # Phase 3: Compute metrics
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "EV/EBITDA Sector-Relative", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'EV/EBITDA Rel':>15} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 46)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>14.1f}% {ar['benchmark']*100:>9.1f}% "
                  f"{ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks)

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="EV/EBITDA Sector-Relative Timing backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("ev-ebitda-relative", args_str=" ".join(cloud_args),
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
        print("  Excluded: ASX/SAO (adjClose artifacts)")
        print("=" * 65)

        all_results = {}
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

        for preset_name, preset_exchanges in GLOBAL_PRESETS:
            uni_name = "_".join(preset_exchanges)
            rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)
            mktcap_threshold = get_mktcap_threshold(preset_exchanges)
            output_path = None
            if args.output:
                out_dir = os.path.dirname(args.output) or "."
                output_path = os.path.join(out_dir, f"returns_{uni_name}.json")

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            try:
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, mktcap_threshold, args.verbose, output_path)
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
            cp = r.get("cash_periods", 0)
            cash_pct = round(cp * 100 / n, 0) if n > 0 else 0
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
    mktcap_threshold = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, mktcap_threshold, args.verbose, args.output)


if __name__ == "__main__":
    main()
