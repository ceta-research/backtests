#!/usr/bin/env python3
"""
Sector P/E Compression Backtest

Quarterly rebalancing, equal weight across compressed sectors.
Signal: Sector aggregate P/E falls 1+ std dev below its 5-year rolling average (z-score < -1).
Execution: Buy compressed sector ETFs; hold SPY when no sectors are compressed.

Academic reference:
  Campbell, J.Y. & Shiller, R.J. (1985). Valuation ratios and the long-run stock market
  outlook. Journal of Finance, 43(3), 661-676.

Usage:
    python3 sector-pe-compression/backtest.py
    python3 sector-pe-compression/backtest.py --output results/backtest.json --verbose
    python3 sector-pe-compression/backtest.py --start-year 2005 --end-year 2025

See README.md for strategy details.
"""

import argparse
import duckdb
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from math import isnan, sqrt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet, generate_rebalance_dates
from metrics import compute_metrics, compute_annual_returns, format_metrics
from cli_utils import get_risk_free_rate

# --- Signal parameters ---
Z_THRESHOLD = -1.0          # Buy sectors with z-score below this
LOOKBACK_YEARS = 5          # Years of history to compute avg / std
DATA_LAG_DAYS = 45          # Days after period end before data is available
START_YEAR = 2000           # Backtest start (note: 5yr lookback means first signal in 2005)
END_YEAR = 2025             # Backtest end
ETF_TRANSACTION_COST = 0.001  # 0.1% per trade (buy + sell = 0.2% round trip)
MAX_ETF_DIVERGENCE = 0.45  # Max |ETF_return - SPY_return| per quarter (split artifact guard)

# Sector ETF mapping: FMP sector name → ETF ticker
SECTOR_TO_ETF = {
    "Technology": "XLK",
    "Energy": "XLE",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Industrials": "XLI",
    "Basic Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",       # Available from 2015-10-08
    "Communication Services": "XLC",  # Available from 2018-06-19
}

# Sector ETF inception dates — used to skip sectors before ETF existed
ETF_INCEPTION = {
    "XLK": date(1998, 12, 22),
    "XLE": date(1998, 12, 22),
    "XLF": date(1998, 12, 22),
    "XLV": date(1998, 12, 22),
    "XLY": date(1998, 12, 22),
    "XLP": date(1998, 12, 22),
    "XLI": date(1998, 12, 22),
    "XLB": date(1998, 12, 22),
    "XLU": date(1998, 12, 22),
    "XLRE": date(2015, 10, 8),
    "XLC": date(2018, 6, 19),
    "SPY": date(1993, 1, 29),
}

ALL_ETFS = list(SECTOR_TO_ETF.values()) + ["SPY"]


def fetch_data_via_api(client, rebalance_dates, start_year, verbose=False):
    """Fetch all required data and load into DuckDB.

    Populates tables:
        sp500_sectors(symbol, sector)
        ratios_cache(symbol, sector, priceToEarningsRatio, marketCap, filing_epoch)
        etf_prices(symbol, trade_epoch, adjClose)
    """
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Sector membership
    print("  Fetching S&P 500 sector membership...")
    sector_sql = "SELECT DISTINCT symbol, sector FROM sp500_constituent WHERE sector IS NOT NULL AND sector != ''"
    sectors = client.query(sector_sql, verbose=verbose)
    if not sectors:
        print("  ERROR: No sector data.")
        return None
    print(f"  Sectors: {len(set(r['sector'] for r in sectors))} ({len(sectors)} symbols)")

    # Load into DuckDB
    sym_sector = [(r['symbol'], r['sector']) for r in sectors]
    con.execute("CREATE TABLE sp500_sectors(symbol VARCHAR, sector VARCHAR)")
    for sym, sec in sym_sector:
        con.execute("INSERT INTO sp500_sectors VALUES (?, ?)", [sym, sec])

    sector_syms = list(set(r['symbol'] for r in sectors))
    sym_filter = ", ".join(f"'{s}'" for s in sector_syms)

    # 2. FY P/E ratios + market cap (combined from financial_ratios and key_metrics)
    print("  Fetching FY P/E ratios...")
    ratios_sql = f"""
        SELECT r.symbol, r.priceToEarningsRatio, r.dateEpoch AS filing_epoch
        FROM financial_ratios r
        WHERE r.period = 'FY'
          AND r.priceToEarningsRatio IS NOT NULL
          AND r.priceToEarningsRatio > 0
          AND r.priceToEarningsRatio < 200
          AND r.symbol IN ({sym_filter})
    """
    count = query_parquet(client, ratios_sql, con, "ratios_raw", verbose=verbose,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} P/E rows")

    print("  Fetching FY market caps...")
    mcap_sql = f"""
        SELECT symbol, marketCap, dateEpoch AS filing_epoch
        FROM key_metrics
        WHERE period = 'FY'
          AND marketCap IS NOT NULL
          AND marketCap > 0
          AND symbol IN ({sym_filter})
    """
    count = query_parquet(client, mcap_sql, con, "mcap_raw", verbose=verbose,
                          memory_mb=4096, threads=2)
    print(f"    -> {count} market cap rows")

    # Join P/E and market cap on symbol + filing date (within 60 days)
    # Cast to BIGINT to avoid UINT32 overflow in subtraction
    con.execute("""
        CREATE TABLE ratios_cache AS
        SELECT
            s.sector,
            r.symbol,
            r.priceToEarningsRatio,
            m.marketCap,
            r.filing_epoch
        FROM ratios_raw r
        JOIN mcap_raw m ON r.symbol = m.symbol
            AND ABS(CAST(r.filing_epoch AS BIGINT) - CAST(m.filing_epoch AS BIGINT)) < 86400 * 60
        JOIN sp500_sectors s ON r.symbol = s.symbol
        WHERE m.marketCap > 0
          AND r.priceToEarningsRatio > 0
    """)
    total = con.execute("SELECT COUNT(*) FROM ratios_cache").fetchone()[0]
    print(f"    -> {total} joined P/E+MCap rows")

    # 3. ETF + SPY prices at rebalance windows
    print("  Fetching ETF prices...")
    etf_list = ", ".join(f"'{e}'" for e in ALL_ETFS)
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=12)
        date_conditions.append(f"(date >= '{d.isoformat()}' AND date <= '{end.isoformat()}')")
    date_filter = " OR ".join(date_conditions)

    etf_sql = f"""
        SELECT symbol, dateEpoch AS trade_epoch, adjClose
        FROM stock_eod
        WHERE symbol IN ({etf_list})
          AND ({date_filter})
    """
    count = query_parquet(client, etf_sql, con, "etf_prices", verbose=verbose,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_etf ON etf_prices(symbol, trade_epoch)")
    print(f"    -> {count} ETF price rows")

    return con


def get_etf_price(con, symbol, target_date, offset_days=0):
    """Get ETF price closest to (but not before) target_date + offset within 10 trading days.

    offset_days=0: same-day close (old behavior)
    offset_days=1: next-day close (MOC execution)
    """
    base = target_date + timedelta(days=offset_days)
    start_epoch = int(datetime.combine(base, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(base + timedelta(days=12), datetime.min.time()).timestamp())

    row = con.execute("""
        SELECT adjClose FROM etf_prices
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, start_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def compute_sector_pe(con, as_of_epoch, lookback_years=5, data_lag_days=45):
    """Compute market-cap weighted sector P/E and z-scores as of a given epoch.

    Uses FY filings available with a data_lag_days cutoff.
    5-year rolling average and std computed from prior years' annual P/E.

    Returns dict: {sector: {'current_pe': float, 'avg_pe': float, 'std_pe': float,
                             'z_score': float, 'n_prior': int, 'n_stocks': int}}
    """
    # Cutoff: data must be filed at least data_lag_days before the rebalance date
    cutoff = as_of_epoch - data_lag_days * 86400
    # Window for 5-year lookback: 5yr * 365.25 days * 86400 sec
    window_start = cutoff - int(lookback_years * 365.25 * 86400)

    rows = con.execute("""
        WITH latest_by_symbol AS (
            -- Most recent FY filing per symbol, before cutoff
            SELECT
                sector, symbol, priceToEarningsRatio, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache
            WHERE filing_epoch <= ?
        ),
        current_pe_inputs AS (
            SELECT sector, symbol, priceToEarningsRatio, marketCap
            FROM latest_by_symbol WHERE rn = 1
        ),
        current_sector_pe AS (
            SELECT
                sector,
                COUNT(*) AS n_stocks,
                SUM(marketCap) / NULLIF(SUM(marketCap / priceToEarningsRatio), 0) AS sector_pe
            FROM current_pe_inputs
            WHERE priceToEarningsRatio > 0
            GROUP BY sector
            HAVING COUNT(*) >= 3
        ),
        annual_pe_by_year AS (
            -- Annual sector P/E snapshots for the 5-year window
            SELECT
                sector,
                CAST(ROUND(filing_epoch / (365.25 * 86400) + 1970) AS INT) AS approx_year,
                SUM(marketCap) / NULLIF(SUM(marketCap / priceToEarningsRatio), 0) AS sector_pe
            FROM (
                SELECT sector, symbol, priceToEarningsRatio, marketCap, filing_epoch,
                    ROW_NUMBER() OVER (PARTITION BY symbol, CAST(ROUND(filing_epoch / (365.25 * 86400) + 1970) AS INT)
                                       ORDER BY filing_epoch DESC) AS rn_yr
                FROM ratios_cache
                WHERE filing_epoch > ? AND filing_epoch < ?
                  AND priceToEarningsRatio > 0
            ) yearly
            WHERE rn_yr = 1
            GROUP BY sector, approx_year
            HAVING COUNT(*) >= 3
        ),
        sector_stats AS (
            SELECT
                sector,
                AVG(sector_pe) AS avg_pe,
                STDDEV(sector_pe) AS std_pe,
                COUNT(*) AS n_prior
            FROM annual_pe_by_year
            GROUP BY sector
            HAVING COUNT(*) >= 3
        )
        SELECT
            c.sector,
            c.n_stocks,
            ROUND(c.sector_pe, 3) AS current_pe,
            ROUND(s.avg_pe, 3) AS avg_pe,
            ROUND(COALESCE(s.std_pe, 0), 3) AS std_pe,
            s.n_prior,
            CASE
                WHEN s.std_pe > 0 THEN ROUND((c.sector_pe - s.avg_pe) / s.std_pe, 3)
                ELSE NULL
            END AS z_score
        FROM current_sector_pe c
        JOIN sector_stats s ON c.sector = s.sector
    """, [cutoff, window_start, cutoff]).fetchall()

    result = {}
    for row in rows:
        sector, n_stocks, cur_pe, avg_pe, std_pe, n_prior, z_score = row
        result[sector] = {
            "current_pe": cur_pe,
            "avg_pe": avg_pe,
            "std_pe": std_pe,
            "z_score": z_score,
            "n_prior": n_prior,
            "n_stocks": n_stocks,
        }
    return result


def run_backtest(con, rebalance_dates, use_costs=True, verbose=False, offset_days=1):
    """Run quarterly sector P/E compression backtest."""
    results = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        entry_epoch = int(datetime.combine(entry_date, datetime.min.time()).timestamp())

        # Compute sector P/E z-scores as of entry_date
        sector_data = compute_sector_pe(con, entry_epoch)

        # Identify compressed sectors
        compressed = []
        for sector, data in sector_data.items():
            z = data.get("z_score")
            if z is not None and z < Z_THRESHOLD:
                etf = SECTOR_TO_ETF.get(sector)
                if etf and ETF_INCEPTION.get(etf, date(1900, 1, 1)) <= entry_date:
                    compressed.append((sector, etf, z))

        # SPY return for this period
        spy_entry = get_etf_price(con, "SPY", entry_date, offset_days=offset_days)
        spy_exit = get_etf_price(con, "SPY", exit_date, offset_days=offset_days)
        spy_return = None
        if spy_entry and spy_exit and spy_entry > 0:
            spy_return = (spy_exit - spy_entry) / spy_entry

        if not compressed:
            # No compressed sectors — hold SPY
            port_return = spy_return if spy_return is not None else 0.0
            holdings = "SPY (no compressed sectors)"
            etfs_held = []
        else:
            # Equal weight compressed sector ETFs
            etf_returns = []
            etfs_held = []
            for sector, etf, z in compressed:
                entry_price = get_etf_price(con, etf, entry_date, offset_days=offset_days)
                exit_price = get_etf_price(con, etf, exit_date, offset_days=offset_days)
                if entry_price and exit_price and entry_price > 0:
                    raw_return = (exit_price - entry_price) / entry_price
                    # Guard against split artifacts: skip if ETF diverges >45% from SPY in one quarter
                    spy_ret = spy_return if spy_return is not None else 0.0
                    if abs(raw_return - spy_ret) > MAX_ETF_DIVERGENCE:
                        if verbose:
                            print(f"    [SKIP] {etf} return {raw_return*100:.1f}% vs SPY {spy_ret*100:.1f}% "
                                  f"(divergence {abs(raw_return-spy_ret)*100:.1f}% > {MAX_ETF_DIVERGENCE*100:.0f}%) — split artifact")
                        continue
                    if use_costs:
                        # ETF: 0.1% buy + 0.1% sell = 0.2% round trip
                        raw_return -= ETF_TRANSACTION_COST * 2
                    etf_returns.append(raw_return)
                    etfs_held.append(etf)

            if etf_returns:
                port_return = sum(etf_returns) / len(etf_returns)
            else:
                port_return = spy_return if spy_return is not None else 0.0
                etfs_held = []

            holdings = ",".join(etfs_held)

        if verbose:
            compressed_str = ", ".join(f"{sec}({z:.2f})" for sec, _, z in compressed) or "none"
            excess = ""
            if spy_return is not None:
                excess = f"  ex={((port_return - spy_return)*100):+.1f}%"
            print(f"  {entry_date}: [{compressed_str}] → port={port_return*100:.1f}%, "
                  f"spy={spy_return*100 if spy_return else 0:.1f}%{excess}")

        results.append({
            "rebalance_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "n_compressed_sectors": len(compressed),
            "etfs_held": etfs_held,
            "holdings": holdings,
            "sector_data": {s: {"z": d["z_score"], "pe": d["current_pe"],
                                "avg_pe": d["avg_pe"], "n_prior": d["n_prior"]}
                            for s, d in sector_data.items()},
        })

    return results


def build_output(metrics, annual, valid, results, periods_per_year, cash_periods, avg_sectors):
    p = metrics["portfolio"]
    b = metrics["benchmark"]
    c = metrics["comparison"]

    def pct(v): return round(v * 100, 2) if v is not None else None
    def rnd(v, d=3): return round(v, d) if v is not None else None

    def fmt(s):
        return {
            "cagr": pct(s.get("cagr")),
            "total_return": pct(s.get("total_return")),
            "max_drawdown": pct(s.get("max_drawdown")),
            "annualized_volatility": pct(s.get("annualized_volatility")),
            "sharpe_ratio": rnd(s.get("sharpe_ratio")),
            "sortino_ratio": rnd(s.get("sortino_ratio")),
            "calmar_ratio": rnd(s.get("calmar_ratio")),
            "max_consecutive_losses": s.get("max_consecutive_losses"),
            "pct_negative_periods": pct(s.get("pct_negative_periods")),
        }

    return {
        "universe": "S&P 500 Sectors (11 GICS sectors)",
        "strategy": "Sector P/E Compression",
        "n_periods": len(valid),
        "years": round(len(valid) / periods_per_year, 1),
        "frequency": "quarterly",
        "cash_periods": cash_periods,
        "avg_sectors_when_invested": round(avg_sectors, 1),
        "portfolio": fmt(p),
        "spy": fmt(b),
        "comparison": {
            "excess_cagr": pct(c.get("excess_cagr")),
            "win_rate": pct(c.get("win_rate")),
            "information_ratio": rnd(c.get("information_ratio")),
            "up_capture": pct(c.get("up_capture")),
            "down_capture": pct(c.get("down_capture")),
            "beta": rnd(c.get("beta")),
            "alpha": pct(c.get("alpha")),
        },
        "annual_returns": [
            {"year": ar["year"],
             "portfolio": round(ar["portfolio"] * 100, 2),
             "spy": round(ar["benchmark"] * 100, 2),
             "excess": round(ar["excess"] * 100, 2)}
            for ar in annual
        ],
    }


def main():
    parser = argparse.ArgumentParser(description="Sector P/E Compression backtest")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument("--no-costs", action="store_true", help="Skip transaction costs")
    parser.add_argument("--no-next-day", action="store_true",
                        help="Use same-day close instead of next-day (MOC) execution")
    parser.add_argument("--output", default=None, help="JSON output path")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--api-key", default=os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY"))
    parser.add_argument("--base-url", default="https://api.cetaresearch.com/api/v1")
    args = parser.parse_args()

    use_costs = not args.no_costs
    offset_days = 0 if args.no_next_day else 1
    risk_free_rate = 0.020  # US 10Y Treasury (approx over period)
    periods_per_year = 4    # Quarterly

    exec_model = "next-day close (MOC)" if offset_days == 1 else "same-day close"
    print("=" * 65)
    print("  SECTOR P/E COMPRESSION BACKTEST")
    print(f"  Universe: S&P 500 sectors via FMP constituents")
    print(f"  Signal: Sector P/E z-score < {Z_THRESHOLD} (vs 5yr avg)")
    print(f"  Execution: {exec_model}, Compressed sector ETFs, equal weight")
    print(f"  Period: {args.start_year}-{args.end_year}, quarterly rebalance")
    print(f"  Costs: {'0.2% round trip (ETFs)' if use_costs else 'none'}")
    print("=" * 65)

    rebalance_dates = generate_rebalance_dates(
        args.start_year, args.end_year, "quarterly", months=[1, 4, 7, 10]
    )
    print(f"\n  Rebalance dates: {len(rebalance_dates)} quarters")

    print("\nPhase 1: Fetching data...")
    t0 = time.time()
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    con = fetch_data_via_api(cr, rebalance_dates, args.start_year, verbose=args.verbose)
    if con is None:
        print("ERROR: Failed to fetch data.")
        return
    print(f"  Data loaded in {time.time()-t0:.0f}s")

    print("\nPhase 2: Running backtest...")
    t1 = time.time()
    results = run_backtest(con, rebalance_dates, use_costs=use_costs, verbose=args.verbose,
                           offset_days=offset_days)
    print(f"  Backtest done in {time.time()-t1:.0f}s")

    # Filter to periods with valid spy return
    valid = [r for r in results if r["spy_return"] is not None]

    # Count "cash" periods (held SPY because nothing was compressed)
    cash_periods = sum(1 for r in valid if r["n_compressed_sectors"] == 0)
    invested = [r["n_compressed_sectors"] for r in valid if r["n_compressed_sectors"] > 0]
    avg_sectors = sum(invested) / len(invested) if invested else 0

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    print("\nPhase 3: Computing metrics...")
    metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "Sector P/E Compression", "S&P 500 (SPY)"))
    print(f"\n  SPY periods (no compression): {cash_periods} / {len(valid)}")
    print(f"  Avg compressed sectors (invested): {avg_sectors:.1f}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)
    if annual:
        print(f"\n  {'Year':<8} {'Strategy':>12} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 42)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% "
                  f"{ar['benchmark']*100:>9.1f}% {ar['excess']*100:>+9.1f}%")

    output = build_output(metrics, annual, valid, results, periods_per_year,
                          cash_periods, avg_sectors)

    if args.output:
        os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n  Results saved to {args.output}")

    con.close()
    print(f"\n  Total time: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
