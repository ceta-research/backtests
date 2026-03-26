#!/usr/bin/env python3
"""
Graham Net-Net Backtest

Annual rebalancing, equal weight, all qualifying stocks (max 30).

Signal: stock price < NCAV per share (Net Current Asset Value)
  NCAV per share = key_metrics.grahamNetNet (FMP pre-computed)
  = (Current Assets - Total Liabilities - Preferred Stock) / Shares Outstanding

Screen: adjClose at rebalance date < grahamNetNet from most recent FY filing
  (45-day filing lag applied)

Portfolio: All qualifying stocks, equal weight, max 30. Cash if < 5 qualify.
Rebalancing: Annual (April), 2001-2025.

Net-nets are inherently small-cap/micro-cap. Market cap thresholds are set much
lower than standard strategies — approximately $30-100M USD equivalent.

Usage:
    # US (default)
    python3 graham-net-net/backtest.py

    # Japan
    python3 graham-net-net/backtest.py --preset japan

    # Hong Kong
    python3 graham-net-net/backtest.py --preset hongkong

    # All exchanges
    python3 graham-net-net/backtest.py --global --output results/exchange_comparison.json

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
from data_utils import query_parquet, get_prices, generate_rebalance_dates, filter_returns
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost, apply_costs
from cli_utils import add_common_args, resolve_exchanges, print_header

# --- Signal parameters ---
MIN_STOCKS = 5       # Hold cash if fewer qualify (net-nets are rare)
MAX_STOCKS = 30      # Portfolio cap
DEFAULT_FREQUENCY = "annual"
DATA_LAG_DAYS = 45   # Filing lag for annual data

# Net-net specific market cap thresholds (local currency).
# Much lower than standard strategies — net-nets are inherently small/micro-cap.
# Target: ~$30-100M USD equivalent (vs $200-500M for standard strategies).
NETNET_MKTCAP_THRESHOLDS = {
    # North America (USD)
    "NYSE": 50_000_000,         # $50M USD
    "NASDAQ": 50_000_000,       # $50M USD
    "AMEX": 50_000_000,         # $50M USD
    "TSX": 20_000_000,          # C$20M ≈ $15M USD
    "TSXV": 10_000_000,         # C$10M ≈ $7M USD
    # Europe
    "LSE": 15_000_000,          # £15M ≈ $19M USD
    "XETRA": 20_000_000,        # €20M ≈ $22M USD
    "STO": 200_000_000,         # SEK 200M ≈ $18M USD
    "OSL": 200_000_000,         # NOK 200M ≈ $19M USD
    "SIX": 20_000_000,          # CHF 20M ≈ $23M USD
    "PAR": 20_000_000,          # €20M ≈ $22M USD
    "WSE": 50_000_000,          # PLN 50M ≈ $12M USD
    # Asia-Pacific
    "JPX": 5_000_000_000,       # ¥5B ≈ $33M USD
    "BSE": 500_000_000,         # ₹500M ≈ $6M USD (small but India net-nets are tiny)
    "NSE": 500_000_000,         # ₹500M ≈ $6M USD
    "HKSE": 200_000_000,        # HK$200M ≈ $26M USD
    "KSC": 50_000_000_000,      # ₩50B ≈ $37M USD
    "TAI": 500_000_000,         # NT$500M ≈ $16M USD
    "TWO": 500_000_000,         # NT$500M ≈ $16M USD
    "SET": 1_000_000_000,       # ฿1B ≈ $29M USD
    "JKT": 500_000_000_000,     # IDR 500B ≈ $31M USD
    "SES": 50_000_000,          # S$50M ≈ $37M USD
    # Other
    "SAU": 100_000_000,         # SAR 100M ≈ $27M USD
    "TLV": 50_000_000,          # ₪50M ≈ $14M USD
    "JNB": 200_000_000,         # R200M ≈ $11M USD
    "SAO": 100_000_000,         # R$100M ≈ $20M USD
}

DEFAULT_MKTCAP = 50_000_000  # $50M USD for unlisted exchanges


def get_netnet_mktcap_threshold(exchanges):
    """Get net-net specific market cap threshold for exchanges."""
    if not exchanges:
        return DEFAULT_MKTCAP
    thresholds = [NETNET_MKTCAP_THRESHOLDS.get(ex, DEFAULT_MKTCAP) for ex in exchanges]
    return min(thresholds)


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch financial data and load into DuckDB.

    Populates tables:
        universe(symbol VARCHAR)
        metrics_cache(symbol, grahamNetNet, marketCap, currentRatio, filing_epoch)
        prices_cache(symbol, trade_epoch, adjClose) + index

    Returns DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"WHERE exchange IN ({ex_filter})"
        sym_filter_sql = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({ex_filter}))"
    else:
        exchange_where = ""
        sym_filter_sql = "1=1"

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='6GB'")

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange FROM profile {exchange_where}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2. key_metrics FY: grahamNetNet, marketCap, currentRatio
    print("  Fetching key metrics (grahamNetNet, marketCap, currentRatio)...")
    metrics_sql = f"""
        SELECT symbol, grahamNetNet, marketCap, currentRatio,
               dateEpoch as filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND grahamNetNet IS NOT NULL
          AND grahamNetNet > 0
          AND {sym_filter_sql}
    """
    count = query_parquet(client, metrics_sql, con, "metrics_cache", verbose=verbose,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} rows")

    if count == 0:
        print("  No metrics data found.")
        return None

    # 3. Stock prices at rebalance date windows
    # Need prices at entry AND exit dates (+ SPY for benchmark)
    print("  Fetching prices at rebalance dates...")
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
                          verbose=verbose, limit=5_000_000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows")

    return con


def screen_stocks(con, target_date, mktcap_min):
    """Screen for Graham net-net stocks at target_date.

    Finds stocks where current price < grahamNetNet per share
    (NCAV per share from most recent FY filing, with 45-day lag).

    Returns list of (symbol, market_cap) tuples.
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=DATA_LAG_DAYS), datetime.min.time()
    ).timestamp())
    target_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(target_date + timedelta(days=10), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH km AS (
            SELECT symbol, grahamNetNet, marketCap,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache
            WHERE filing_epoch <= ?
              AND grahamNetNet > 0
              AND marketCap > ?
        ),
        prices AS (
            SELECT symbol, adjClose
            FROM prices_cache
            WHERE trade_epoch >= ? AND trade_epoch <= ?
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC) = 1
        )
        SELECT km.symbol, km.marketCap
        FROM km
        JOIN prices ON km.symbol = prices.symbol
        WHERE km.rn = 1
          AND prices.adjClose > 0.50
          AND prices.adjClose < km.grahamNetNet
        ORDER BY prices.adjClose / km.grahamNetNet ASC
        LIMIT ?
    """, [cutoff_epoch, mktcap_min, target_epoch, end_epoch, MAX_STOCKS]).fetchall()

    return [(r[0], r[1]) for r in rows]


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run Graham net-net backtest. Returns list of period result dicts."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        portfolio = screen_stocks(con, entry_date, mktcap_min)

        if len(portfolio) < MIN_STOCKS:
            spy_entry = get_prices(con, ["SPY"], entry_date)
            spy_exit = get_prices(con, ["SPY"], exit_date)
            spy_return = None
            if "SPY" in spy_entry and "SPY" in spy_exit and spy_entry["SPY"] > 0:
                spy_return = (spy_exit["SPY"] - spy_entry["SPY"]) / spy_entry["SPY"]

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

        # Build raw returns list for filtering
        raw_returns = []
        for sym in symbols:
            ep = entry_prices.get(sym)
            xp = exit_prices.get(sym)
            mc = mcaps.get(sym)
            if ep and xp and ep > 0:
                raw_returns.append((sym, ep, xp, mc))

        # Filter data artifacts: cap >300% annual return (net-nets can legitimately
        # return 100-200% in recovery years, but >300% is almost always a data error)
        clean, skipped = filter_returns(raw_returns, min_entry_price=0.50,
                                        max_single_return=3.0, verbose=verbose)

        returns = []
        for sym, raw_ret, mcap in clean:
            if use_costs:
                cost = tiered_cost(mcap)
                net_ret = apply_costs(raw_ret, cost)
            else:
                net_ret = raw_ret
            returns.append(net_ret)

        port_return = sum(returns) / len(returns) if returns else 0.0

        spy_entry = get_prices(con, ["SPY"], entry_date)
        spy_exit = get_prices(con, ["SPY"], exit_date)
        spy_return = None
        if "SPY" in spy_entry and "SPY" in spy_exit and spy_entry["SPY"] > 0:
            spy_return = (spy_exit["SPY"] - spy_entry["SPY"]) / spy_entry["SPY"]

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


def main():
    parser = argparse.ArgumentParser(description="Graham Net-Net backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("graham-net-net", args_str=" ".join(cloud_args),
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
    mktcap_threshold = get_netnet_mktcap_threshold(exchanges)

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[frequency]

    signal_desc = (f"price < grahamNetNet (NCAV/share), MCap > {mktcap_threshold/1e6:.0f}M local, "
                   f"all qualifying (max {MAX_STOCKS})")
    print_header("GRAHAM NET-NET BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Frequency: {frequency} (April), Costs: {'size-tiered' if use_costs else 'none'}")
    print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print("\nPhase 1: Fetching data via API...")
    # Annual: April each year (after December FY filings + 45-day lag)
    rebalance_dates = generate_rebalance_dates(2001, 2025, frequency, months=[4])
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available. Exiting.")
        sys.exit(1)
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    print(f"\nPhase 2: Running {frequency} backtest (2001-2025)...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs, verbose=args.verbose)
    bt_time = time.time() - t1
    print(f"Backtest completed in {bt_time:.0f}s")

    # Compute metrics
    valid = [r for r in results if r["spy_return"] is not None]
    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)

    print(format_metrics(metrics, "Net-Net", "S&P 500"))

    cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    print(f"\n  Cash periods: {cash_periods} / {len(results)}")
    print(f"  Avg stocks (invested): {avg_stocks:.1f}")

    # Annual returns
    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'Net-Net':>10} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 40)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}% {ar['benchmark']*100:>9.1f}% "
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
            "n_periods": len(valid),
            "years": round(len(valid) / periods_per_year, 1),
            "frequency": frequency,
            "cash_periods": cash_periods,
            "invested_periods": len(valid) - cash_periods,
            "avg_stocks_when_invested": round(avg_stocks, 1),
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
        os.makedirs(os.path.dirname(args.output), exist_ok=True) if os.path.dirname(args.output) else None
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
