#!/usr/bin/env python3
"""
Sector Mean Reversion Backtest

Quarterly rebalancing, equal weight. Buys stocks in the worst-performing sectors
by trailing 12-month return. Pure price signal — no fundamental data required.

Signal: Rank sectors by equal-weighted 12-month trailing return.
        Buy all qualifying stocks in the bottom N_WORST_SECTORS sectors.
        Min MIN_SECTOR_STOCKS stocks with valid returns for a sector to count.
        Min MIN_QUALIFYING_SECTORS sectors needed to run (else cash).
Portfolio: Equal-weight all selected stocks. Cash if < MIN_PORTFOLIO_STOCKS qualify.
Rebalancing: Quarterly (Jan, Apr, Jul, Oct), 2000-2025.

Academic basis: Moskowitz & Grinblatt (1999) document industry momentum; extreme
sector underperformance tends to mean-revert (contrarian extension of the finding).

Usage:
    python3 sector-rotation/backtest.py                                   # US default
    python3 sector-rotation/backtest.py --preset india                    # India
    python3 sector-rotation/backtest.py --preset us --n-worst 3           # Bottom 3 sectors
    python3 sector-rotation/backtest.py --global --output results/exchange_comparison.json

See README.md for strategy details and academic references.
"""

import argparse
import duckdb
import json
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, filter_returns
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold, EXCHANGE_PRESETS)

# --- Signal parameters ---
N_WORST_SECTORS = 2           # Bottom N sectors by trailing 12-month return
MIN_SECTOR_STOCKS = 5         # Min stocks with valid 12-month returns for a sector to qualify
MIN_QUALIFYING_SECTORS = 5    # Min qualifying sectors to run strategy (else cash)
MIN_PORTFOLIO_STOCKS = 10     # Hold cash if fewer stocks pass all filters
DEFAULT_FREQUENCY = "quarterly"
DEFAULT_REBALANCE_MONTHS = [1, 4, 7, 10]  # Quarter-start months
MAX_SINGLE_RETURN = 2.0       # Cap individual portfolio stock return at 200% (data artifacts)
MIN_ENTRY_PRICE = 0.50        # Skip stocks below this price at entry (data artifacts)
SECTOR_RETURN_MAX = 5.0       # Sanity cap on 12-month stock returns for sector scoring
BACKTEST_START = 2000
BACKTEST_END = 2025


def generate_rebalance_dates(start_year, end_year, months):
    """Generate quarter-start rebalance dates."""
    dates = []
    for year in range(start_year, end_year + 1):
        for month in months:
            dates.append(date(year, month, 1))
    return sorted(dates)


def fetch_data(client, exchanges, mktcap_min, verbose=False):
    """Fetch sector mappings and prices for sector rotation backtest.

    Prices are fetched ONLY at quarter-start dates (Jan/Apr/Jul/Oct, days 1-15)
    from 1999 onwards. This captures both:
      - Entry/exit prices at each quarterly rebalance date
      - Year-ago prices for 12-month return computation (same month, prior year)

    Returns DuckDB connection with tables:
      sector_map(symbol VARCHAR, sector VARCHAR, market_cap DOUBLE)
      prices_cache(symbol VARCHAR, trade_date DATE, adjClose DOUBLE)
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        mktcap_filter = f"exchange IN ({ex_filter}) AND marketCap > {mktcap_min}"
        price_sym_filter = (
            f"(symbol IN (SELECT DISTINCT symbol FROM profile "
            f"WHERE exchange IN ({ex_filter}) AND marketCap > {mktcap_min}) "
            f"OR symbol = 'SPY')"
        )
    else:
        mktcap_filter = f"marketCap > {mktcap_min}"
        price_sym_filter = (
            f"(symbol IN (SELECT DISTINCT symbol FROM profile "
            f"WHERE marketCap > {mktcap_min}) OR symbol = 'SPY')"
        )

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=2")

    # 1. Sector map (current snapshot with market cap for cost tiers)
    print("  Fetching sector mappings...")
    sector_sql = f"""
        SELECT DISTINCT symbol, sector, marketCap AS market_cap
        FROM profile
        WHERE {mktcap_filter}
          AND sector IS NOT NULL AND sector != ''
    """
    n_sectors = query_parquet(client, sector_sql, con, "sector_map",
                              verbose=verbose, timeout=120)
    if n_sectors == 0:
        print("  No symbols found for this exchange.")
        return None
    print(f"  Universe: {n_sectors} symbols with sector data")

    # 2. Prices at quarter-start dates (Jan/Apr/Jul/Oct, days 1-15, 1999-2026)
    #    Includes SPY for benchmark. Limit 20M rows covers all global exchanges.
    print("  Fetching prices (quarter-start windows, 1999-2026)...")
    price_sql = f"""
        SELECT symbol,
               CAST(date AS DATE) AS trade_date,
               adjClose
        FROM stock_eod
        WHERE {price_sym_filter}
          AND EXTRACT(MONTH FROM CAST(date AS DATE)) IN (1, 4, 7, 10)
          AND EXTRACT(DAY FROM CAST(date AS DATE)) <= 15
          AND CAST(date AS DATE) >= '1999-01-01'
          AND CAST(date AS DATE) <= '2026-03-01'
          AND adjClose IS NOT NULL
          AND adjClose > 0
    """
    n_prices = query_parquet(client, price_sql, con, "prices_cache",
                             verbose=verbose, limit=20_000_000, timeout=600,
                             memory_mb=4096, threads=2)
    if n_prices == 0:
        print("  No price data found.")
        con.close()
        return None
    print(f"  Price rows: {n_prices:,}")

    # Index for fast per-period lookups
    con.execute("CREATE INDEX idx_pc_sym_date ON prices_cache(symbol, trade_date)")

    return con


def screen_sectors(con, target_date, n_worst=N_WORST_SECTORS):
    """Identify worst N sectors and their stocks at target_date.

    12-month return per stock = (price at target_date quarter) / (price at same quarter
    one year prior) - 1. Sectors are ranked by equal-weighted average of constituent
    returns (min MIN_SECTOR_STOCKS stocks with valid data required).

    Returns:
        list of (symbol, sector, recent_price, market_cap) for stocks in worst N sectors
        int: number of qualifying sectors (for cash-period detection)
    """
    yr = target_date.year
    mo = target_date.month
    yr_ago = yr - 1

    rows = con.execute(f"""
        WITH recent AS (
            -- First available price in the target quarter-month
            SELECT sm.symbol, sm.sector, sm.market_cap, pc.adjClose AS recent_price
            FROM sector_map sm
            JOIN prices_cache pc ON sm.symbol = pc.symbol
            WHERE EXTRACT(YEAR FROM pc.trade_date) = {yr}
              AND EXTRACT(MONTH FROM pc.trade_date) = {mo}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY sm.symbol ORDER BY pc.trade_date ASC) = 1
        ),
        year_ago AS (
            -- First available price in the same month one year prior
            SELECT symbol, adjClose AS old_price
            FROM prices_cache
            WHERE EXTRACT(YEAR FROM trade_date) = {yr_ago}
              AND EXTRACT(MONTH FROM trade_date) = {mo}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date ASC) = 1
        ),
        stock_returns AS (
            SELECT r.symbol, r.sector, r.recent_price, r.market_cap,
                   (r.recent_price / ya.old_price - 1) AS return_12m
            FROM recent r
            JOIN year_ago ya ON r.symbol = ya.symbol
            WHERE ya.old_price > 0
              AND r.recent_price >= {MIN_ENTRY_PRICE}
        ),
        sector_stats AS (
            SELECT sector,
                   AVG(return_12m) AS avg_sector_return,
                   COUNT(*) AS n_stocks
            FROM stock_returns
            WHERE return_12m BETWEEN -0.99 AND {SECTOR_RETURN_MAX}
            GROUP BY sector
            HAVING COUNT(*) >= {MIN_SECTOR_STOCKS}
        ),
        ranked AS (
            SELECT sector, avg_sector_return,
                   ROW_NUMBER() OVER (ORDER BY avg_sector_return ASC) AS rank_worst,
                   COUNT(*) OVER () AS n_qualifying
            FROM sector_stats
        )
        SELECT sr.symbol, sr.sector, sr.recent_price, sr.market_cap,
               rs.avg_sector_return, rs.n_qualifying
        FROM stock_returns sr
        JOIN ranked rs ON sr.sector = rs.sector
        WHERE rs.rank_worst <= {n_worst}
          AND sr.return_12m BETWEEN -0.99 AND {SECTOR_RETURN_MAX}
        ORDER BY rs.rank_worst ASC, sr.symbol ASC
    """).fetchall()

    n_qualifying = rows[0][5] if rows else 0
    return rows, n_qualifying


def get_prices_at(con, symbols, year, month):
    """Get first available prices in the given month/year for a list of symbols."""
    if not symbols:
        return {}
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    result = con.execute(f"""
        SELECT symbol, adjClose
        FROM prices_cache
        WHERE symbol IN ({sym_list})
          AND EXTRACT(YEAR FROM trade_date) = {year}
          AND EXTRACT(MONTH FROM trade_date) = {month}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date ASC) = 1
    """).fetchall()
    return {r[0]: r[1] for r in result}


def run_backtest(con, rebalance_dates, use_costs=True, verbose=False, n_worst=N_WORST_SECTORS):
    """Run sector rotation backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        # Screen: identify worst sectors and their stocks at entry date
        portfolio_rows, n_qualifying = screen_sectors(con, entry_date, n_worst=n_worst)

        # SPY benchmark return for this period
        spy_entry_prices = get_prices_at(con, ["SPY"], entry_date.year, entry_date.month)
        spy_exit_prices = get_prices_at(con, ["SPY"], exit_date.year, exit_date.month)
        spy_return = None
        if "SPY" in spy_entry_prices and "SPY" in spy_exit_prices and spy_entry_prices["SPY"] > 0:
            spy_return = (spy_exit_prices["SPY"] - spy_entry_prices["SPY"]) / spy_entry_prices["SPY"]

        # Cash period: insufficient qualifying sectors or stocks
        if n_qualifying < MIN_QUALIFYING_SECTORS or len(portfolio_rows) < MIN_PORTFOLIO_STOCKS:
            reason = (f"sectors={n_qualifying}" if n_qualifying < MIN_QUALIFYING_SECTORS
                      else f"stocks={len(portfolio_rows)}")
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
                "stocks_held": 0,
                "sectors_selected": "",
                "n_qualifying_sectors": n_qualifying,
                "holdings": f"CASH ({reason})",
            })
            if verbose:
                sectors_txt = ", ".join(dict.fromkeys(r[1] for r in portfolio_rows))
                print(f"    {entry_date}: CASH ({reason}) | sectors: {sectors_txt or 'none'}")
            continue

        # Get exit prices for portfolio
        symbols = [r[0] for r in portfolio_rows]
        mcaps = {r[0]: r[3] for r in portfolio_rows}  # current profile market cap
        entry_prices_map = {r[0]: r[2] for r in portfolio_rows}  # from screen (at entry)
        exit_prices_map = get_prices_at(con, symbols, exit_date.year, exit_date.month)

        # Build (symbol, entry_price, exit_price, market_cap) for filter_returns
        symbol_data = [
            (sym, entry_prices_map.get(sym), exit_prices_map.get(sym), mcaps.get(sym))
            for sym in symbols
            if entry_prices_map.get(sym) and exit_prices_map.get(sym)
        ]
        clean, skipped = filter_returns(symbol_data,
                                        min_entry_price=MIN_ENTRY_PRICE,
                                        max_single_return=MAX_SINGLE_RETURN,
                                        verbose=verbose)

        if len(clean) < MIN_PORTFOLIO_STOCKS:
            results.append({
                "rebalance_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
                "portfolio_return": 0.0,
                "spy_return": spy_return,
                "stocks_held": 0,
                "sectors_selected": "",
                "n_qualifying_sectors": n_qualifying,
                "holdings": f"CASH (only {len(clean)} clean stocks)",
            })
            if verbose:
                print(f"    {entry_date}: CASH (only {len(clean)} clean stocks after filters)")
            continue

        # Equal-weight returns with transaction costs
        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns)
        sectors_selected = ", ".join(sorted(set(r[1] for r in portfolio_rows)))

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "stocks_held": len(clean),
            "sectors_selected": sectors_selected,
            "n_qualifying_sectors": n_qualifying,
            "holdings": ",".join(sym for sym, _, _ in clean),
        })

        if verbose:
            spy_str = f"{spy_return * 100:.1f}%" if spy_return is not None else "N/A"
            excess_str = ""
            if spy_return is not None:
                excess_str = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_date}: {len(clean)} stocks [{sectors_selected}] "
                  f"port={port_return * 100:.1f}%, spy={spy_str}{excess_str}")

    return results


def build_output(metrics, annual, valid, results, universe_name, frequency,
                 periods_per_year, cash_periods, avg_stocks, n_worst=N_WORST_SECTORS):
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
        "n_worst_sectors": n_worst,
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
            {
                "year": ar["year"],
                "portfolio": round(ar["portfolio"] * 100, 2),
                "spy": round(ar["benchmark"] * 100, 2),
                "excess": round(ar["excess"] * 100, 2),
            }
            for ar in annual
        ],
    }


def run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, verbose, output_path=None, n_worst=N_WORST_SECTORS):
    """Run backtest for a single exchange set. Returns output dict or None."""
    periods_per_year = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}[frequency]
    mktcap_min = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_min / 1e9:.0f}B" if mktcap_min >= 1e9
                    else f"{mktcap_min / 1e6:.0f}M")

    signal_desc = (f"Bottom {n_worst} sectors by 12M trailing return, "
                   f"MCap > {mktcap_label} local, equal weight")
    print_header("SECTOR MEAN REVERSION BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency}, Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate * 100:.1f}%")
    print("=" * 65)

    # Generate rebalance dates
    rebalance_dates = generate_rebalance_dates(BACKTEST_START, BACKTEST_END + 1,
                                               DEFAULT_REBALANCE_MONTHS)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data...")
    t0 = time.time()
    con = fetch_data(cr, exchanges, mktcap_min, verbose=verbose)
    if con is None:
        print("  No data. Skipping.")
        return None
    fetch_time = time.time() - t0
    print(f"  Data fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running {frequency} backtest ({BACKTEST_START}-{BACKTEST_END})...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, use_costs=use_costs, verbose=verbose,
                           n_worst=n_worst)
    bt_time = time.time() - t1
    print(f"  Backtest complete in {bt_time:.0f}s")

    # Phase 3: Compute metrics
    valid = [r for r in results
             if r["portfolio_return"] is not None and r["spy_return"] is not None]
    if not valid:
        print("  No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "Sector Rotation", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    # Sector frequency (how often each sector was in the bottom 2)
    sector_freq = {}
    for r in results:
        if r["sectors_selected"]:
            for s in r["sectors_selected"].split(", "):
                sector_freq[s] = sector_freq.get(s, 0) + 1
    if sector_freq:
        print("\n  Sector frequency (quarters in bottom 2):")
        for s, cnt in sorted(sector_freq.items(), key=lambda x: -x[1])[:6]:
            print(f"    {s}: {cnt}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'SectorRot':>12} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 44)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio'] * 100:>11.1f}% "
                  f"{ar['benchmark'] * 100:>9.1f}% {ar['excess'] * 100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s")

    output = build_output(metrics, annual, valid, results, universe_name,
                          frequency, periods_per_year, cash_periods, avg_stocks,
                          n_worst=n_worst)
    output["sector_frequency"] = sector_freq

    if output_path:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {output_path}")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(description="Sector Mean Reversion backtest")
    add_common_args(parser)
    parser.add_argument("--n-worst", type=int, default=N_WORST_SECTORS,
                        help=f"Number of worst sectors to buy (default {N_WORST_SECTORS})")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("sector-rotation", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    n_worst = args.n_worst

    exchanges, universe_name = resolve_exchanges(args)
    frequency = args.frequency or DEFAULT_FREQUENCY
    use_costs = not args.no_costs

    # --global mode: loop all presets
    if exchanges is None:
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        presets_to_run = [
            ("us", ["NYSE", "NASDAQ", "AMEX"]),
            ("india", ["BSE", "NSE"]),
            ("germany", ["XETRA"]),
            ("uk", ["LSE"]),
            ("canada", ["TSX"]),
            ("korea", ["KSC"]),
            ("australia", ["ASX"]),
            ("taiwan", ["TAI", "TWO"]),
            ("brazil", ["SAO"]),
            ("hongkong", ["HKSE"]),
            ("switzerland", ["SIX"]),
            ("sweden", ["STO"]),
            ("thailand", ["SET"]),
            ("southafrica", ["JNB"]),
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

            print(f"\n{'#' * 65}")
            print(f"# {preset_name.upper()} ({uni_name})")
            print(f"{'#' * 65}")

            try:
                result = run_single(cr, preset_exchanges, uni_name, frequency,
                                    use_costs, rfr, args.verbose, output_path,
                                    n_worst=n_worst)
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

        # Summary table
        print(f"\n\n{'=' * 90}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'=' * 90}")
        print(f"{'Exchange':<22} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>8} {'AvgStk':>8}")
        print("-" * 90)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<22} {'ERROR / NO DATA':}")
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
            print(f"{uni:<22} {f'{cagr:.2f}%' if cagr is not None else 'N/A':>8} "
                  f"{f'{excess:+.2f}%' if excess is not None else 'N/A':>8} "
                  f"{f'{sharpe:.3f}' if sharpe is not None else 'N/A':>8} "
                  f"{f'{maxdd:.1f}%' if maxdd is not None else 'N/A':>8} "
                  f"{f'{cash_pct:.0f}%':>8} {f'{avg:.1f}' if avg is not None else 'N/A':>8}")
        print("=" * 90)
        return

    # Single exchange mode
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, frequency, use_costs,
               risk_free_rate, args.verbose, args.output, n_worst=n_worst)


if __name__ == "__main__":
    main()
