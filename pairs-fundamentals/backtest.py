#!/usr/bin/env python3
"""
Pairs Trading Backtest (Fundamental Pairs)

Annual pair formation using 252-day returns correlation within sectors.
Top 20 pairs per exchange (same sector, corr > 0.70). Spread z-score entry
filter: only trade pairs where spread is extended (|z| > 1.5) at formation end.
Equal-dollar weighting across active pairs. Annual rebalance.

Return model:
    - Spread = log(P_A) - beta * log(P_B), beta estimated by OLS on log-prices
    - If z_start > 0: A overvalued vs B → Short A, Long B
    - Pair return = -sign(z) * (Return_A - Return_B) / 2  (equal-dollar)
    - Portfolio return = mean(active pair returns), 0% if < MIN_PAIRS_ACTIVE

Academic reference:
    Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading: Performance
    of a Relative-Value Arbitrage Rule." Review of Financial Studies, 19(3), 797-827.
    GGR found ~11% annual excess return (1962-2002). Do & Faff (2010, 2012) showed
    declining profitability post-2002 as strategy became crowded.

Usage:
    python3 pairs-fundamentals/backtest.py                              # US default
    python3 pairs-fundamentals/backtest.py --preset india
    python3 pairs-fundamentals/backtest.py --global \\
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
from data_utils import (query_parquet, get_local_benchmark, get_benchmark_return,
                        LOCAL_INDEX_BENCHMARKS)
from metrics import compute_metrics, compute_annual_returns, format_metrics
from costs import tiered_cost
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_risk_free_rate, get_mktcap_threshold)

# ─── Strategy Parameters ──────────────────────────────────────────────────────
TOP_N_PER_SECTOR = 30      # Candidate stocks per sector (ranked by latest market cap)
MIN_CORR = 0.70            # Minimum 252-day returns-based correlation
MIN_CORR_DAYS = 200        # Minimum common trading days for pair eligibility
MAX_PAIRS = 20             # Maximum pairs to hold per year
Z_ENTRY = 1.5              # Minimum |z-score| at formation end to enter a trade
MIN_PAIRS_ACTIVE = 3       # Hold cash if fewer pairs meet the entry condition
MAX_SINGLE_RETURN = 2.0    # Cap absolute pair return (data quality guard)
MIN_LEG_PRICE = 1.0        # Skip pairs where either leg price < $1 (artifacts)
START_YEAR = 2005
END_YEAR = 2024


def fetch_data_via_api(cr, exchanges, verbose=False):
    """Fetch sector mapping, market caps, and full daily price history into DuckDB.

    Populates:
        sector_map(symbol VARCHAR, sector VARCHAR)
        mcap_map(symbol VARCHAR, marketCap DOUBLE)
        prices_cache(symbol VARCHAR, trade_date DATE, adjClose DOUBLE)

    Returns DuckDB connection or None.
    """
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    ex_filter = ", ".join(f"'{e}'" for e in exchanges) if exchanges else None
    ex_where = f"exchange IN ({ex_filter})" if ex_filter else "1=1"

    # ── Step 1: Sector map ────────────────────────────────────────────────────
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

    # ── Step 2: Latest market cap for candidate ranking ───────────────────────
    print("  Fetching market caps...")
    # Use exchange subquery to avoid hitting 50k char SQL limit with inline symbol list
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

    # ── Step 3: Select top N per sector by market cap ─────────────────────────
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

    # ── Step 4: Load sector map and mcap into DuckDB ──────────────────────────
    sec_vals = ", ".join(f"('{s}', '{sector_map[s]}')" for s in candidates)
    con.execute("CREATE TABLE sector_map(symbol VARCHAR, sector VARCHAR)")
    con.execute(f"INSERT INTO sector_map VALUES {sec_vals}")

    mcap_vals = ", ".join(f"('{s}', {mcap_map.get(s, 0)})" for s in candidates)
    con.execute("CREATE TABLE mcap_map(symbol VARCHAR, marketCap DOUBLE)")
    con.execute(f"INSERT INTO mcap_map VALUES {mcap_vals}")

    # ── Step 5: Fetch daily prices (2004 onward) for candidates + benchmarks ──
    print("  Fetching daily prices (2004-present)...")
    bench_symbols = {"SPY"}
    if exchanges:
        for ex in exchanges:
            sym = LOCAL_INDEX_BENCHMARKS.get(ex)
            if sym:
                bench_symbols.add(sym)
    price_syms = candidates + list(bench_symbols)
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
                          memory_mb=16384, threads=6)
    if count == 0:
        print("  No price data found.")
        return None

    print(f"  Price rows: {count:,}")
    con.execute("CREATE INDEX idx_pc_sym_date ON prices_cache(symbol, trade_date)")

    return con


def compute_correlations(con, formation_start, formation_end):
    """Compute same-sector pairwise 252-day returns correlations via DuckDB.

    Uses a self-join on daily returns within the formation period.
    Filters to MIN_CORR_DAYS common trading days and MIN_CORR threshold.

    Returns list of (sym_a, sym_b, sector, correlation, common_days).
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
                sm_a.sector AS sector,
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
        LIMIT {MAX_PAIRS * 5}
    """).fetchall()

    return rows  # list of tuples: (sym_a, sym_b, sector, corr, common_days)


def compute_spread_params(con, sym_a, sym_b, formation_start, formation_end):
    """Estimate OLS beta (log-price) and spread mean/std for a pair.

    Spread = log(P_A) - beta * log(P_B)
    Beta = Cov(log_A, log_B) / Var(log_B) = CORR * STD(log_A) / STD(log_B)

    Returns (beta, spread_mean, spread_std) or None if insufficient data.
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
            SELECT a.lp AS la, b.lp AS lb
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        ),
        stats AS (
            SELECT
                CORR(la, lb)  AS corr,
                STDDEV(la)    AS std_a,
                STDDEV(lb)    AS std_b,
                AVG(la)       AS mean_a,
                AVG(lb)       AS mean_b,
                COUNT(*)      AS n_obs
            FROM paired
        )
        SELECT
            corr * std_a / NULLIF(std_b, 0)              AS beta,
            mean_a - (corr * std_a / NULLIF(std_b, 0)) * mean_b AS intercept,
            n_obs
        FROM stats
    """).fetchone()

    if not row or row[0] is None or row[2] is None or row[2] < MIN_CORR_DAYS:
        return None

    beta = float(row[0])
    if not math.isfinite(beta):
        return None

    # Compute spread mean and std using the estimated beta
    spread_row = con.execute(f"""
        WITH log_prices AS (
            SELECT symbol, trade_date, LN(adjClose) AS lp
            FROM prices_cache
            WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'
              AND symbol IN ('{sym_a}', '{sym_b}')
              AND adjClose > 0
        ),
        spread AS (
            SELECT a.lp - {beta} * b.lp AS s
            FROM log_prices a
            JOIN log_prices b
                ON a.trade_date = b.trade_date
               AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'
        )
        SELECT AVG(s) AS mean_s, STDDEV(s) AS std_s FROM spread
    """).fetchone()

    if (not spread_row or spread_row[0] is None
            or spread_row[1] is None or float(spread_row[1]) < 1e-10):
        return None

    return beta, float(spread_row[0]), float(spread_row[1])


def get_price_at_date(con, symbol, target_date, window_days=10, offset_days=0):
    """First available price in [target_date+offset, target_date+offset+window]."""
    shifted = target_date + timedelta(days=offset_days)
    end_date = shifted + timedelta(days=window_days)
    row = con.execute(f"""
        SELECT adjClose FROM prices_cache
        WHERE symbol = '{symbol}'
          AND trade_date >= '{shifted.isoformat()}'
          AND trade_date <= '{end_date.isoformat()}'
          AND adjClose > 0
        ORDER BY trade_date ASC LIMIT 1
    """).fetchone()
    return float(row[0]) if row else None


def run_backtest(con, use_costs=True, verbose=False, offset_days=1,
                 benchmark_symbol="SPY"):
    """Run annual pairs backtest (START_YEAR to END_YEAR).

    Returns list of annual result dicts.
    """
    results = []

    for year in range(START_YEAR, END_YEAR + 1):
        formation_start = date(year - 1, 1, 1)
        formation_end   = date(year - 1, 12, 31)
        trading_start   = date(year, 1, 1)
        trading_end     = date(year, 12, 31)

        t0 = time.time()
        corr_rows = compute_correlations(con, formation_start, formation_end)
        selected  = corr_rows[:MAX_PAIRS]

        if verbose:
            print(f"  {year}: {len(corr_rows)} eligible pairs → "
                  f"{len(selected)} selected  [{time.time()-t0:.1f}s corr]")

        # Benchmark annual return
        bench_start = get_price_at_date(con, benchmark_symbol, trading_start,
                                        offset_days=offset_days)
        bench_end   = get_price_at_date(con, benchmark_symbol, trading_end,
                                        window_days=15, offset_days=offset_days)
        spy_ret = ((bench_end - bench_start) / bench_start
                   if bench_start and bench_end else None)

        # Per-pair trading
        active_returns = []

        for (sym_a, sym_b, sector, corr, _) in selected:
            params = compute_spread_params(con, sym_a, sym_b, formation_start, formation_end)
            if params is None:
                continue
            beta, spread_mean, spread_std = params

            # Prices at trading year start (MOC: offset_days forward)
            p_a0 = get_price_at_date(con, sym_a, trading_start,
                                     offset_days=offset_days)
            p_b0 = get_price_at_date(con, sym_b, trading_start,
                                     offset_days=offset_days)
            if not p_a0 or not p_b0 or p_a0 < MIN_LEG_PRICE or p_b0 < MIN_LEG_PRICE:
                continue

            # Z-score at formation end / trading start
            spread0 = math.log(p_a0) - beta * math.log(p_b0)
            z_start = (spread0 - spread_mean) / spread_std

            # Only enter trades where spread is sufficiently extended
            if abs(z_start) < Z_ENTRY:
                continue

            # Prices at trading year end (MOC: offset_days forward)
            p_a1 = get_price_at_date(con, sym_a, trading_end, window_days=15,
                                     offset_days=offset_days)
            p_b1 = get_price_at_date(con, sym_b, trading_end, window_days=15,
                                     offset_days=offset_days)
            if not p_a1 or not p_b1:
                continue

            ret_a = (p_a1 - p_a0) / p_a0
            ret_b = (p_b1 - p_b0) / p_b0

            # Data quality guard
            if abs(ret_a) > MAX_SINGLE_RETURN or abs(ret_b) > MAX_SINGLE_RETURN:
                continue

            # Equal-dollar pairs return
            # z_start > 0 → A expensive vs B → Short A, Long B
            # Convergence (A falls, B rises relative) → positive return
            direction = -1 if z_start > 0 else 1
            pair_return = direction * (ret_a - ret_b) / 2.0

            # Transaction costs: 4 one-way legs (open/close × 2 stocks)
            if use_costs:
                mc_a_row = con.execute(
                    f"SELECT marketCap FROM mcap_map WHERE symbol = '{sym_a}'"
                ).fetchone()
                mc_b_row = con.execute(
                    f"SELECT marketCap FROM mcap_map WHERE symbol = '{sym_b}'"
                ).fetchone()
                mc_a = mc_a_row[0] if mc_a_row else 1e9
                mc_b = mc_b_row[0] if mc_b_row else 1e9
                cost = 2 * tiered_cost(mc_a) + 2 * tiered_cost(mc_b)  # 4 one-way legs
                pair_return -= cost

            active_returns.append(pair_return)

        n_active = len(active_returns)
        port_return = (sum(active_returns) / n_active
                       if n_active >= MIN_PAIRS_ACTIVE else 0.0)

        results.append({
            "year": year,
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_ret, 6) if spy_ret is not None else None,
            "pairs_formed": len(selected),
            "pairs_active": n_active,
        })

        if verbose:
            spy_s = f"{spy_ret*100:.1f}%" if spy_ret is not None else "N/A"
            print(f"    → {n_active}/{len(selected)} active, "
                  f"port={port_return*100:.1f}%, spy={spy_s}")

    return results


def build_output(m, annual, valid, results, universe_name,
                 benchmark_name="S&P 500", benchmark_symbol="SPY"):
    """Build JSON output in standard exchange_comparison format."""
    p = m["portfolio"]
    b = m["benchmark"]
    c = m["comparison"]

    def pct(v):  return round(v * 100, 2) if v is not None else None
    def rnd(v, d=3): return round(v, d) if v is not None else None

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

    cash_periods = sum(1 for r in results if r["pairs_active"] < MIN_PAIRS_ACTIVE)
    invested = [r["pairs_active"] for r in results if r["pairs_active"] >= MIN_PAIRS_ACTIVE]
    avg_pairs = round(sum(invested) / len(invested), 1) if invested else 0

    return {
        "universe": universe_name,
        "n_years": len(valid),
        "years": f"{START_YEAR}-{END_YEAR}",
        "frequency": "annual",
        "execution": "next-day close (MOC)",
        "benchmark_symbol": benchmark_symbol,
        "benchmark_name": benchmark_name,
        "cash_periods": cash_periods,
        "invested_periods": len(invested),
        "avg_pairs_when_invested": avg_pairs,
        "portfolio": fmt(p),
        "spy": fmt(b),
        "comparison": {
            "excess_cagr":      pct(c.get("excess_cagr")),
            "win_rate":         pct(c.get("win_rate")),
            "information_ratio": rnd(c.get("information_ratio")),
            "up_capture":       pct(c.get("up_capture")),
            "down_capture":     pct(c.get("down_capture")),
            "beta":             rnd(c.get("beta")),
            "alpha":            pct(c.get("alpha")),
        },
        "excess_cagr": pct(c.get("excess_cagr")),
        "win_rate_vs_spy": pct(c.get("win_rate")),
        "annual_returns": [
            {
                "year": r["year"],
                "portfolio": round(r["portfolio_return"] * 100, 2),
                "spy": round(r["spy_return"] * 100, 2) if r["spy_return"] is not None else None,
                "excess": round((r["portfolio_return"] - (r["spy_return"] or 0)) * 100, 2),
                "pairs_active": r["pairs_active"],
                "pairs_formed": r["pairs_formed"],
            }
            for r in results
        ],
    }


def run_single(cr, exchanges, universe_name, use_costs, risk_free_rate, verbose,
               output_path=None, offset_days=1):
    """Run backtest for a single exchange set. Returns output dict or None."""
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "same-day close (legacy)" if offset_days == 0 else "next-day close (MOC)"

    signal_desc = (f"Same-sector corr > {MIN_CORR}, top {MAX_PAIRS} pairs, "
                   f"|z| > {Z_ENTRY} entry, annual rebalance {START_YEAR}-{END_YEAR}")
    print_header("PAIRS TRADING BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Costs: {'size-tiered×4 legs' if use_costs else 'none'}  "
          f"RFR: {risk_free_rate*100:.1f}%")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print("=" * 65)

    print("\nPhase 1: Fetching data via API...")
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, verbose=verbose)
    if con is None:
        print("No data available. Skipping.")
        return None
    print(f"Data fetched in {time.time()-t0:.0f}s")

    # Check if local benchmark has price data; fall back to SPY if not
    bench_check = con.execute(f"""
        SELECT COUNT(*) FROM prices_cache WHERE symbol = '{benchmark_symbol}'
    """).fetchone()[0]
    if bench_check == 0 and benchmark_symbol != "SPY":
        print(f"  WARNING: No price data for {benchmark_symbol}, falling back to SPY")
        benchmark_symbol = "SPY"
        benchmark_name = "S&P 500"

    print(f"\nPhase 2: Running annual backtest ({START_YEAR}-{END_YEAR})...")
    t1 = time.time()
    results = run_backtest(con, use_costs=use_costs, verbose=verbose,
                           offset_days=offset_days,
                           benchmark_symbol=benchmark_symbol)
    print(f"Backtest completed in {time.time()-t1:.0f}s")

    valid = [r for r in results if r["spy_return"] is not None]
    if not valid:
        print("No valid periods. Skipping.")
        con.close()
        return None

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns  = [r["spy_return"] for r in valid]

    m = compute_metrics(port_returns, spy_returns, 1, risk_free_rate=risk_free_rate)
    print(format_metrics(m, "Pairs", benchmark_name))

    cash_periods = sum(1 for r in results if r["pairs_active"] < MIN_PAIRS_ACTIVE)
    avg_pairs = sum(r["pairs_active"] for r in valid) / len(valid)
    print(f"\n  Cash periods: {cash_periods}/{len(results)}")
    print(f"  Avg active pairs: {avg_pairs:.1f}")

    annual = compute_annual_returns(port_returns, spy_returns,
                                    [str(r["year"]) for r in valid], 1)
    if annual:
        bname = benchmark_name[:8]
        print(f"\n  {'Year':<8} {'Pairs':>10} {bname:>8} {'Excess':>8} {'Active':>8}")
        print("  " + "-" * 50)
        for i, ar in enumerate(annual):
            n_act = valid[i]["pairs_active"] if i < len(valid) else "-"
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>9.1f}%"
                  f" {ar['benchmark']*100:>7.1f}%"
                  f" {ar['excess']*100:>+7.1f}% {n_act:>8}")

    print(f"\n  Total time: {time.time()-t0:.0f}s")

    out = build_output(m, annual, valid, results, universe_name,
                       benchmark_name=benchmark_name,
                       benchmark_symbol=benchmark_symbol)
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
        description="Pairs Trading Backtest (Annual, Same-Sector, Correlation-Based)")
    add_common_args(parser)
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    use_costs = not args.no_costs
    offset_days = 0 if args.no_next_day else 1

    # ── Global mode ────────────────────────────────────────────────────────────
    if exchanges is None and universe_name in ("Global", "GLOBAL"):
        print("=" * 65)
        print("  GLOBAL MODE: Running all exchange presets")
        print("=" * 65)

        # Exchanges eligible for pairs trading:
        # - Sufficient universe size (200+ large-cap stocks)
        # - 10+ years of price history
        # - Short-selling broadly available (note: restricted in China/India)
        presets_to_run = [
            ("us",          ["NYSE", "NASDAQ", "AMEX"]),
            ("india",       ["NSE"]),
            ("japan",       ["JPX"]),
            ("uk",          ["LSE"]),
            ("china",       ["SHZ", "SHH"]),
            ("hongkong",    ["HKSE"]),
            ("korea",       ["KSC"]),
            ("taiwan",      ["TAI", "TWO"]),
            ("germany",     ["XETRA"]),
            ("canada",      ["TSX"]),
            ("sweden",      ["STO"]),
            ("switzerland", ["SIX"]),
            ("thailand",    ["SET"]),
            ("southafrica", ["JNB"]),
            ("singapore",   ["SES"]),
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

            print(f"\n{'#'*65}\n# {preset_name.upper()} ({uni_name})\n{'#'*65}")
            try:
                result = run_single(cr, preset_exchanges, uni_name, use_costs, rfr,
                                    args.verbose, output_path,
                                    offset_days=offset_days)
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
            print(f"\n\nExchange comparison saved to {args.output}")

        # Summary table
        print(f"\n\n{'='*80}")
        print("EXCHANGE COMPARISON SUMMARY")
        print(f"{'='*80}")
        print(f"{'Exchange':<20} {'CAGR':>8} {'Excess':>8} {'Sharpe':>8} "
              f"{'MaxDD':>8} {'Cash%':>7} {'AvgPairs':>9}")
        print("-" * 75)
        for uni, r in sorted(all_results.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
            if "error" in r or not r.get("portfolio"):
                print(f"{uni:<20} {'ERROR':>40}")
                continue
            p = r.get("portfolio", {})
            c = r.get("comparison", {})
            n = r.get("n_years", 0)
            cp = r.get("cash_periods", 0)
            cash_pct = round(cp * 100 / n, 0) if n > 0 else 0
            cagr = p.get("cagr")
            exc  = c.get("excess_cagr")
            shp  = p.get("sharpe_ratio")
            mdd  = p.get("max_drawdown")
            avg  = r.get("avg_pairs_when_invested")
            exc_s = f"{exc:+.2f}" if exc is not None else "N/A"
            print(f"{uni:<20} {str(cagr)+('%' if cagr else ''):>8}"
                  f" {exc_s:>7}%"
                  f" {str(shp) if shp else 'N/A':>8}"
                  f" {str(mdd)+('%' if mdd else ''):>8}"
                  f" {cash_pct:>6.0f}%"
                  f" {str(avg) if avg else 'N/A':>9}")
        print("=" * 80)
        return

    # ── Single exchange mode ──────────────────────────────────────────────────
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_single(cr, exchanges, universe_name, use_costs, risk_free_rate,
               args.verbose, args.output, offset_days=offset_days)


if __name__ == "__main__":
    main()
