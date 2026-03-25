#!/usr/bin/env python3
"""
Piotroski F-Score Backtest

Annual rebalancing (April), equal weight, three portfolio tracks.
Fetches data via configurable provider, caches in local DuckDB, runs locally.

Signal: Piotroski F-Score computed from raw financial statements.
Universe: Bottom 20% by P/B, market cap > $100M.
Portfolios: Score 8-9 (long), Score 0-2 (avoid), All Value (baseline).
Benchmark: S&P 500 (SPY).

Usage:
    # Backtest US stocks (default)
    python3 piotroski/backtest.py

    # Backtest Indian stocks
    python3 piotroski/backtest.py --exchange BSE,NSE

    # Backtest with verbose output, save results
    python3 piotroski/backtest.py --verbose --output results.json

    # Backtest all exchanges
    python3 piotroski/backtest.py --global

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
from data_utils import query_parquet, generate_rebalance_dates, get_local_benchmark, get_benchmark_return, LOCAL_INDEX_BENCHMARKS
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs
from cli_utils import add_common_args, resolve_exchanges, print_header, get_mktcap_threshold

# --- Config ---
# MKTCAP_MIN removed - now computed per-exchange via get_mktcap_threshold()
PB_QUINTILE = 0.20  # Bottom 20% by P/B
DEFAULT_FREQUENCY = "annual"


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all historical financial data and load into DuckDB.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, grossProfit, revenue, filing_epoch, period)
        balance_cache(symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                      longTermDebt, totalStockholdersEquity, filing_epoch, period)
        cashflow_cache(symbol, operatingCashFlow, filing_epoch, period)
        metrics_cache(symbol, marketCap, filing_epoch, period)
        ratios_cache(symbol, priceToBookRatio, filing_epoch, period)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns a DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where_direct = f"WHERE exchange IN ({ex_filter})"
    else:
        exchange_where_direct = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange FROM profile {exchange_where_direct}"
    profiles = client.query(profile_sql, verbose=verbose)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None
    print(f"  Universe: {len(profiles)} symbols")

    sym_values = ",".join(f"('{r['symbol']}')" for r in profiles)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    if exchanges:
        sym_filter_sql = f"""
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE exchange IN ({ex_filter})
            )
        """
    else:
        sym_filter_sql = "1=1"

    # 2-6: Financial data (all via parquet)
    queries = [
        ("income_cache", f"""
            SELECT symbol, netIncome, grossProfit, revenue, dateEpoch as filing_epoch, period
            FROM income_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "income statements"),
        ("balance_cache", f"""
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                longTermDebt, totalStockholdersEquity, dateEpoch as filing_epoch, period
            FROM balance_sheet
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "balance sheets"),
        ("cashflow_cache", f"""
            SELECT symbol, operatingCashFlow, dateEpoch as filing_epoch, period
            FROM cash_flow_statement
            WHERE period = 'FY' AND {sym_filter_sql}
        """, "cash flow statements"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap, dateEpoch as filing_epoch, period
            FROM key_metrics
            WHERE period = 'FY' AND marketCap IS NOT NULL AND {sym_filter_sql}
        """, "key metrics"),
        ("ratios_cache", f"""
            SELECT symbol, priceToBookRatio, dateEpoch as filing_epoch, period
            FROM financial_ratios
            WHERE period = 'FY' AND priceToBookRatio IS NOT NULL AND {sym_filter_sql}
        """, "financial ratios (P/B)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose)
        print(f"    -> {count} rows")

    # 7. Prices (only at rebalance dates)
    print("  Fetching prices...")
    date_conditions = []
    for d in rebalance_dates:
        end = d + timedelta(days=14)  # Extra margin for offset_days=1 + weekends
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
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
                INTERSECT
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
                    {f"AND {sym_filter_sql}" if sym_filter_sql != "1=1" else ""}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count} price rows (at {len(rebalance_dates)} rebalance dates)")

    return con


def screen_and_score(con, target_date, mktcap_min):
    """Compute Piotroski scores for value universe at a given rebalance date.

    Returns dict: {symbol: (score, market_cap)}
    """
    cutoff_epoch = int(datetime.combine(target_date - timedelta(days=45), datetime.min.time()).timestamp())
    prev_year_epoch = int(datetime.combine(target_date - timedelta(days=445), datetime.min.time()).timestamp())

    rows = con.execute("""
        WITH
        inc_curr AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        inc_prev AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        bal_curr AS (
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                longTermDebt, totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        bal_prev AS (
            SELECT symbol, totalAssets, longTermDebt, totalCurrentAssets, totalCurrentLiabilities,
                totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ?
        ),
        cf_curr AS (
            SELECT symbol, operatingCashFlow, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM cashflow_cache WHERE filing_epoch <= ? AND filing_epoch > ?
        ),
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        rat AS (
            SELECT symbol, priceToBookRatio, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM ratios_cache WHERE filing_epoch <= ?
        ),
        pb_universe AS (
            SELECT rat.symbol, rat.priceToBookRatio, met.marketCap,
                PERCENT_RANK() OVER (ORDER BY rat.priceToBookRatio ASC) AS pb_pctile
            FROM rat
            JOIN met ON rat.symbol = met.symbol AND met.rn = 1
            WHERE rat.rn = 1
              AND rat.priceToBookRatio > 0
              AND met.marketCap > ?
        ),
        value_stocks AS (
            SELECT symbol, priceToBookRatio, marketCap
            FROM pb_universe
            WHERE pb_pctile <= ?
        ),
        scored AS (
            SELECT ic.symbol,
                CASE WHEN ic.netIncome > 0 THEN 1 ELSE 0 END AS f1_ni,
                CASE WHEN cf.operatingCashFlow > 0 THEN 1 ELSE 0 END AS f2_ocf,
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.netIncome / bc.totalAssets) > (ip.netIncome / bp.totalAssets) THEN 1 ELSE 0 END AS f3_roa,
                CASE WHEN cf.operatingCashFlow > ic.netIncome THEN 1 ELSE 0 END AS f4_accrual,
                CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (COALESCE(bc.longTermDebt,0) / bc.totalAssets) < (COALESCE(bp.longTermDebt,0) / bp.totalAssets) THEN 1 ELSE 0 END AS f5_leverage,
                CASE WHEN bc.totalCurrentAssets > 0 AND bc.totalCurrentLiabilities > 0
                     AND bp.totalCurrentAssets > 0 AND bp.totalCurrentLiabilities > 0
                     AND (bc.totalCurrentAssets / bc.totalCurrentLiabilities) > (bp.totalCurrentAssets / bp.totalCurrentLiabilities) THEN 1 ELSE 0 END AS f6_liquidity,
                CASE WHEN bc.totalStockholdersEquity >= bp.totalStockholdersEquity THEN 1 ELSE 0 END AS f7_no_dilution,
                CASE WHEN ic.revenue > 0 AND ip.revenue > 0 AND bc.totalAssets > 0 AND bp.totalAssets > 0
                     AND (ic.revenue / bc.totalAssets) > (ip.revenue / bp.totalAssets) THEN 1 ELSE 0 END AS f8_turnover,
                CASE WHEN ic.grossProfit > 0 AND ip.grossProfit > 0 AND ic.revenue > 0 AND ip.revenue > 0
                     AND (ic.grossProfit / ic.revenue) > (ip.grossProfit / ip.revenue) THEN 1 ELSE 0 END AS f9_margin,
                vs.marketCap
            FROM value_stocks vs
            JOIN inc_curr ic ON vs.symbol = ic.symbol AND ic.rn = 1
            JOIN inc_prev ip ON vs.symbol = ip.symbol AND ip.rn = 1
            JOIN bal_curr bc ON vs.symbol = bc.symbol AND bc.rn = 1
            JOIN bal_prev bp ON vs.symbol = bp.symbol AND bp.rn = 1
            JOIN cf_curr cf ON vs.symbol = cf.symbol AND cf.rn = 1
        )
        SELECT symbol,
            (f1_ni + f2_ocf + f3_roa + f4_accrual + f5_leverage + f6_liquidity
             + f7_no_dilution + f8_turnover + f9_margin) AS f_score,
            marketCap
        FROM scored
    """, [
        cutoff_epoch, prev_year_epoch,
        prev_year_epoch,
        cutoff_epoch, prev_year_epoch,
        prev_year_epoch,
        cutoff_epoch, prev_year_epoch,
        cutoff_epoch,
        cutoff_epoch,
        mktcap_min,
        PB_QUINTILE,
    ]).fetchall()

    return {r[0]: (r[1], r[2]) for r in rows}


def get_price(con, symbol, target_date, offset_days=0):
    """Get adjusted close price on or just after target_date + offset_days."""
    shifted_date = target_date + timedelta(days=offset_days)
    target_epoch = int(datetime.combine(shifted_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(shifted_date + timedelta(days=10), datetime.min.time()).timestamp())
    row = con.execute("""
        SELECT adjClose FROM prices_cache
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, target_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False,
                 offset_days=1, benchmark_symbol="SPY"):
    """Run the full Piotroski backtest with three portfolio tracks."""
    print(f"Phase 2: Running annual backtest ({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")
    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        scored = screen_and_score(con, entry_date, mktcap_min)
        if not scored:
            if verbose:
                print(f"  {entry_date.year}: No scored stocks found, skipping")
            continue

        high = {s: v for s, v in scored.items() if v[0] >= 8}
        low = {s: v for s, v in scored.items() if v[0] <= 2}

        track_returns = {}
        for name, portfolio in [("high", high), ("low", low), ("all", scored)]:
            returns = []
            for sym, (score, mcap) in portfolio.items():
                ep = get_price(con, sym, entry_date, offset_days=offset_days)
                xp = get_price(con, sym, exit_date, offset_days=offset_days)
                if ep and xp and ep > 0:
                    raw_ret = (xp - ep) / ep
                    if use_costs:
                        cost = tiered_cost(mcap)
                        net_ret = apply_costs(raw_ret, cost)
                    else:
                        net_ret = raw_ret
                    returns.append(net_ret)
            track_returns[name] = sum(returns) / len(returns) if returns else 0.0

        bench_return = get_benchmark_return(
            con, benchmark_symbol, entry_date, exit_date, offset_days=offset_days)

        periods.append({
            "year": entry_date.year,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "high_return": track_returns["high"],
            "low_return": track_returns["low"],
            "all_return": track_returns["all"],
            "spy_return": bench_return,
            "high_count": len(high),
            "low_count": len(low),
            "all_count": len(scored),
        })

        if verbose:
            h_pct = track_returns["high"] * 100
            l_pct = track_returns["low"] * 100
            b_pct = bench_return * 100 if bench_return else 0
            print(f"  {entry_date.year}: Score 8-9={h_pct:+.1f}% ({len(high)}), "
                  f"Score 0-2={l_pct:+.1f}% ({len(low)}), "
                  f"All={track_returns['all']*100:+.1f}% ({len(scored)}), "
                  f"Bench={b_pct:+.1f}%")

    print(f"Phase 2 complete: {len(periods)} annual periods.\n")
    return periods


def compute_track_metrics(periods, track_key, risk_free_rate, periods_per_year):
    """Compute metrics for one portfolio track using shared metrics module."""
    valid = [p for p in periods if p["spy_return"] is not None]
    if not valid:
        return None

    track_returns = [p[track_key] for p in valid]
    spy_returns = [p["spy_return"] for p in valid]

    return _compute_metrics(track_returns, spy_returns, periods_per_year,
                            risk_free_rate=risk_free_rate)


def build_output(periods, universe_name, risk_free_rate, periods_per_year):
    """Build Piotroski output with all three tracks + strategy-specific analysis."""
    valid = [p for p in periods if p["spy_return"] is not None]
    n = len(valid)
    if n == 0:
        return {"universe": universe_name, "error": "No valid periods"}

    # Compute metrics for each track using shared metrics module
    spy_returns = [p["spy_return"] for p in valid]
    results = {}
    for track, key in [("high", "high_return"), ("low", "low_return"),
                       ("all", "all_return"), ("spy", "spy_return")]:
        rets = [p[key] for p in valid]
        m = _compute_metrics(rets, spy_returns, periods_per_year,
                             risk_free_rate=risk_free_rate)
        pm = m["portfolio"]

        def rnd(v, d=3):
            return round(v, d) if v is not None else None

        results[track] = {
            "cagr": round(pm["cagr"] * 100, 2),
            "total_return": round(pm["total_return"] * 100, 2),
            "volatility": round(pm["annualized_volatility"] * 100, 2),
            "sharpe": rnd(pm["sharpe_ratio"]),
            "sortino": rnd(pm["sortino_ratio"]),
            "calmar": rnd(pm["calmar_ratio"]),
            "max_drawdown": round(pm["max_drawdown"] * 100, 1),
            "var_95": round(pm["var_95"] * 100, 1) if pm["var_95"] is not None else None,
            "pct_negative_years": round(pm["pct_negative_periods"] * 100, 0),
            "max_consecutive_losses": pm["max_consecutive_losses"],
        }

        # Add comparison metrics for high-score track
        if track == "high":
            c = m["comparison"]
            results["high_vs_spy"] = {
                "excess_cagr": round(c["excess_cagr"] * 100, 2),
                "information_ratio": rnd(c["information_ratio"]),
                "tracking_error": round(c["tracking_error"] * 100, 2) if c["tracking_error"] is not None else None,
                "up_capture": round(c["up_capture"] * 100, 1) if c["up_capture"] is not None else None,
                "down_capture": round(c["down_capture"] * 100, 1) if c["down_capture"] is not None else None,
                "beta": rnd(c["beta"]),
                "alpha": round(c["alpha"] * 100, 2) if c["alpha"] is not None else None,
            }

    # Decade breakdown
    decades = {}
    for p in valid:
        yr = p["year"]
        if yr < 1990:
            d = "1985-89"
        elif yr < 2000:
            d = "1990s"
        elif yr < 2010:
            d = "2000s"
        elif yr < 2020:
            d = "2010s"
        else:
            d = "2020-25"

        if d not in decades:
            decades[d] = {"high": [], "low": [], "all": []}
        decades[d]["high"].append(p["high_return"])
        decades[d]["low"].append(p["low_return"])
        decades[d]["all"].append(p["all_return"])

    decade_results = []
    for d in ["1985-89", "1990s", "2000s", "2010s", "2020-25"]:
        if d in decades:
            h_avg = sum(decades[d]["high"]) / len(decades[d]["high"]) * 100
            l_avg = sum(decades[d]["low"]) / len(decades[d]["low"]) * 100
            decade_results.append({
                "decade": d,
                "high_return": round(h_avg, 1),
                "low_return": round(l_avg, 1),
                "spread": round(h_avg - l_avg, 1),
            })

    # Alpha decomposition
    high_alpha = results["high"]["cagr"] - results["all"]["cagr"]
    avoid_alpha = results["all"]["cagr"] - results["low"]["cagr"]

    # Pre/post publication
    pre = [p for p in valid if p["year"] < 2000]
    post = [p for p in valid if p["year"] >= 2000]
    pre_spread = None
    post_spread = None
    if pre:
        pre_h = sum(p["high_return"] for p in pre) / len(pre) * 100
        pre_l = sum(p["low_return"] for p in pre) / len(pre) * 100
        pre_spread = round(pre_h - pre_l, 1)
    if post:
        post_h = sum(p["high_return"] for p in post) / len(post) * 100
        post_l = sum(p["low_return"] for p in post) / len(post) * 100
        post_spread = round(post_h - post_l, 1)

    output = {
        "universe": universe_name,
        "period": f"1985-2025 ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "portfolios": {
            "score_8_9": results["high"],
            "score_0_2": results["low"],
            "all_value": results["all"],
            "sp500": results["spy"],
        },
        "spread_cagr": round(results["high"]["cagr"] - results["low"]["cagr"], 2),
        "alpha_decomposition": {
            "selection_alpha": round(high_alpha, 1),
            "avoidance_alpha": round(avoid_alpha, 1),
        },
        "decade_breakdown": decade_results,
        "publication_effect": {
            "pre_2000_spread": pre_spread,
            "post_2000_spread": post_spread,
        },
    }

    if "high_vs_spy" in results:
        output["high_vs_spy"] = results["high_vs_spy"]

    # Include period-level data for chart generation
    output["annual_returns"] = [
        {
            "year": p["year"],
            "high": round(p["high_return"] * 100, 2),
            "low": round(p["low_return"] * 100, 2),
            "all": round(p["all_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2) if p["spy_return"] is not None else None,
            "high_count": p["high_count"],
            "low_count": p["low_count"],
            "all_count": p["all_count"],
        }
        for p in valid
    ]

    return output


def print_summary(m, benchmark_name="S&P 500"):
    p = m["portfolios"]
    print("\n" + "=" * 85)
    print(f"PIOTROSKI F-SCORE BACKTEST: {m['universe']}")
    print("=" * 85)
    print(f"Period: {m['period']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print("-" * 85)

    print(f"{'Portfolio':<20} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print("-" * 85)
    for name, label in [("score_8_9", "Score 8-9"), ("all_value", "All Value"),
                         ("score_0_2", "Score 0-2"), ("sp500", benchmark_name)]:
        d = p[name]
        sortino = d.get('sortino')
        calmar = d.get('calmar')
        var95 = d.get('var_95')
        s_str = f"{sortino:>8.3f}" if sortino is not None else f"{'N/A':>8}"
        c_str = f"{calmar:>8.3f}" if calmar is not None else f"{'N/A':>8}"
        v_str = f"{var95:>7.1f}%" if var95 is not None else f"{'N/A':>8}"
        print(f"{label:<20} {d['cagr']:>7.1f}% {d['volatility']:>7.1f}% {d['sharpe']:>8.3f} "
              f"{s_str} {c_str} {d['max_drawdown']:>7.1f}% {v_str}")

    print(f"\nSpread (8-9 minus 0-2): {m['spread_cagr']:.1f}% per year")
    print(f"Selection alpha (8-9 vs all): +{m['alpha_decomposition']['selection_alpha']:.1f}%")
    print(f"Avoidance alpha (all vs 0-2): +{m['alpha_decomposition']['avoidance_alpha']:.1f}%")

    # Score 8-9 vs benchmark comparison
    hvs = m.get("high_vs_spy")
    if hvs:
        print(f"\nScore 8-9 vs {benchmark_name}:")
        print(f"  Excess CAGR: {hvs['excess_cagr']:+.2f}%")
        if hvs.get('information_ratio') is not None:
            print(f"  Information Ratio: {hvs['information_ratio']:.3f}")
        if hvs.get('up_capture') is not None:
            print(f"  Up Capture: {hvs['up_capture']:.1f}%  |  Down Capture: {hvs['down_capture']:.1f}%")
        if hvs.get('beta') is not None:
            print(f"  Beta: {hvs['beta']:.3f}  |  Alpha: {hvs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Decade':<12} {'Score 8-9':>10} {'Score 0-2':>10} {'Spread':>10}")
        print("-" * 45)
        for d in m["decade_breakdown"]:
            print(f"{d['decade']:<12} {d['high_return']:>9.1f}% {d['low_return']:>9.1f}% {d['spread']:>+9.1f}%")

    pub = m.get("publication_effect", {})
    if pub.get("pre_2000_spread") and pub.get("post_2000_spread"):
        print(f"\nPre-publication (1985-1999): {pub['pre_2000_spread']:.1f}% spread")
        print(f"Post-publication (2000-2025): {pub['post_2000_spread']:.1f}% spread")

    print("=" * 85)


def main():
    parser = argparse.ArgumentParser(description="Piotroski F-Score backtest")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute (Projects API)")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("piotroski", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(
        args,
        default_exchanges=["NYSE", "NASDAQ", "AMEX"],
        default_name="US Value (bottom 20% P/B, >$100M)"
    )
    # Auto-detect risk-free rate from exchanges (or use user override)
    from cli_utils import get_risk_free_rate
    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    use_costs = not args.no_costs
    periods_per_year = 1  # Annual only for Piotroski

    offset_days = 0 if args.no_next_day else 1
    benchmark_symbol, benchmark_name = get_local_benchmark(exchanges)
    exec_model = "Same-day close (legacy)" if offset_days == 0 else "Next-day close (MOC)"

    signal_desc = f"Bottom {int(PB_QUINTILE*100)}% P/B, MCap > {mktcap_label} local"
    print_header("PIOTROSKI F-SCORE BACKTEST", universe_name, exchanges, signal_desc)
    print(f"  Portfolios: Score 8-9 (long), Score 0-2 (avoid), All Value (baseline)")
    print(f"  Rebalancing: Annual (April 1), 1985-2025")
    print(f"  Execution: {exec_model}, Benchmark: {benchmark_name} ({benchmark_symbol})")
    print(f"  Costs: {'size-tiered' if use_costs else 'none'}, Rf: {risk_free_rate*100:.1f}%")
    print("=" * 75)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(1985, 2025, "annual", months=[4])
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available. Exiting.")
        sys.exit(1)
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest locally
    t1 = time.time()
    periods = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs,
                          verbose=args.verbose, offset_days=offset_days,
                          benchmark_symbol=benchmark_symbol)
    bt_time = time.time() - t1

    # Phase 3: Compute and display metrics
    output = build_output(periods, universe_name, risk_free_rate, periods_per_year)
    print_summary(output, benchmark_name=benchmark_name)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s (fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    # Save results
    if args.output:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Results saved to {args.output}")

    con.close()


if __name__ == "__main__":
    main()
