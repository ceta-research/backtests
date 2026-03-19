#!/usr/bin/env python3
"""
DuPont ROE Decomposition Backtest

Annual rebalancing (April), equal weight, four portfolio tracks.
Fetches data via API, caches in local DuckDB, runs locally.

Signal: DuPont decomposition of ROE into three components:
  ROE = Net Profit Margin x Asset Turnover x Equity Multiplier

Portfolios:
  Quality ROE:     ROE > 15%, net margin > 8%, equity multiplier < 3.0
  Margin-Driven:   Top quartile net margin within ROE > 15%
  Leverage-Driven: Top quartile equity multiplier within ROE > 15%
  All High ROE:    All stocks with ROE > 15%

Universe: Non-financial, non-utility stocks above exchange-specific market cap threshold.
Benchmark: S&P 500 (SPY).

Academic basis:
  Soliman (2008): "The Use of DuPont Analysis by Market Participants"
  Fairfield & Yohn (2001): "Using Asset Turnover and Profit Margin to Forecast
  Changes in Profitability"

Usage:
    python3 roe-dupont/backtest.py
    python3 roe-dupont/backtest.py --preset india --verbose
    python3 roe-dupont/backtest.py --global --output results/exchange_comparison.json
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
from data_utils import query_parquet, generate_rebalance_dates, filter_returns
from metrics import compute_metrics as _compute_metrics
from costs import tiered_cost, apply_costs
from cli_utils import (add_common_args, resolve_exchanges, print_header,
                       get_mktcap_threshold, get_risk_free_rate,
                       EXCHANGE_PRESETS)

# --- Config ---
ROE_MIN = 0.15              # Minimum ROE for inclusion
NET_MARGIN_MIN = 0.08       # Quality ROE: minimum net margin
EQUITY_MULT_MAX = 3.0       # Quality ROE: maximum equity multiplier
FILING_LAG_DAYS = 45        # Days after FY end before data is available
EXCLUDED_SECTORS = ("Financial Services", "Utilities")
START_YEAR = 2000
END_YEAR = 2025
MAX_SINGLE_RETURN = 2.0     # Data quality guard: cap single-period returns

GLOBAL_PRESETS = [
    "us", "india", "germany", "china", "hongkong", "canada",
    "uk", "switzerland", "sweden", "korea", "brazil",
    "taiwan", "singapore", "southafrica", "france",
]


def fetch_data_via_api(client, exchanges, rebalance_dates, verbose=False):
    """Fetch all historical financial data and load into DuckDB.

    Populates DuckDB tables:
        universe(symbol VARCHAR)
        income_cache(symbol, netIncome, revenue, filing_epoch)
        balance_cache(symbol, totalAssets, totalStockholdersEquity, filing_epoch)
        metrics_cache(symbol, marketCap, filing_epoch)
        prices_cache(symbol, trade_epoch, adjClose)

    Returns a DuckDB connection or None.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        sym_filter_sql = f"""
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE exchange IN ({ex_filter})
                  AND COALESCE(sector, '') NOT IN ('Financial Services', 'Utilities')
            )
        """
        profile_where = f"WHERE exchange IN ({ex_filter})"
    else:
        sym_filter_sql = """
            symbol IN (
                SELECT DISTINCT symbol FROM profile
                WHERE COALESCE(sector, '') NOT IN ('Financial Services', 'Utilities')
            )
        """
        profile_where = ""

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit='4GB'")

    # 1. Universe (with sector info for filtering)
    print("  Fetching exchange membership...")
    profile_sql = f"SELECT DISTINCT symbol, exchange, sector FROM profile {profile_where}"
    profiles = client.query(profile_sql, verbose=verbose, timeout=120)
    if not profiles:
        print("  No symbols found for these exchanges.")
        return None

    filtered = [p for p in profiles if p.get("sector") not in EXCLUDED_SECTORS]
    excluded_count = len(profiles) - len(filtered)
    print(f"  Universe: {len(profiles)} symbols, {excluded_count} financials/utilities excluded, "
          f"{len(filtered)} remaining")

    if not filtered:
        print("  No non-financial/utility symbols found.")
        return None

    sym_values = ",".join(f"('{r['symbol']}')" for r in filtered)
    con.execute(f"CREATE TABLE universe(symbol VARCHAR); INSERT INTO universe VALUES {sym_values}")

    # 2-4: Financial data
    queries = [
        ("income_cache", f"""
            SELECT symbol, netIncome, revenue,
                dateEpoch as filing_epoch
            FROM income_statement
            WHERE period = 'FY'
              AND revenue IS NOT NULL AND revenue > 0
              AND netIncome IS NOT NULL
              AND {sym_filter_sql}
        """, "income statements"),
        ("balance_cache", f"""
            SELECT symbol, totalAssets, totalStockholdersEquity,
                dateEpoch as filing_epoch
            FROM balance_sheet
            WHERE period = 'FY'
              AND totalAssets IS NOT NULL AND totalAssets > 0
              AND {sym_filter_sql}
        """, "balance sheets"),
        ("metrics_cache", f"""
            SELECT symbol, marketCap,
                dateEpoch as filing_epoch
            FROM key_metrics
            WHERE period = 'FY'
              AND marketCap IS NOT NULL
              AND {sym_filter_sql}
        """, "key metrics (market cap)"),
    ]

    for table_name, sql, label in queries:
        print(f"  Fetching {label}...")
        count = query_parquet(client, sql, con, table_name, verbose=verbose,
                              limit=5000000, timeout=600,
                              memory_mb=4096, threads=2)
        print(f"    -> {count:,} rows")

    # 5. Prices (only at rebalance dates + 10-day windows)
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
                SELECT DISTINCT symbol FROM income_statement WHERE period = 'FY'
                    AND {sym_filter_sql}
                INTERSECT
                SELECT DISTINCT symbol FROM balance_sheet WHERE period = 'FY'
                    AND {sym_filter_sql}
            )
          )
    """
    count = query_parquet(client, price_sql, con, "prices_cache",
                          verbose=verbose, limit=5000000, timeout=600,
                          memory_mb=4096, threads=2)
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    print(f"    -> {count:,} price rows")

    return con


def compute_dupont(con, target_date, mktcap_min):
    """Compute DuPont decomposition for all qualifying stocks at a rebalance date.

    Returns dict: {symbol: {net_margin, asset_turnover, equity_multiplier,
                            roe_dupont, market_cap, classification}}

    Classification:
      quality_roe:     ROE > 15%, margin > 8%, eq_mult < 3.0
      margin_driven:   Top quartile net margin (within ROE > 15%)
      leverage_driven: Top quartile equity multiplier (within ROE > 15%)
      mixed:           Everything else with ROE > 15%
    """
    cutoff_epoch = int(datetime.combine(
        target_date - timedelta(days=FILING_LAG_DAYS),
        datetime.min.time()
    ).timestamp())

    rows = con.execute("""
        WITH
        inc AS (
            SELECT symbol, netIncome, revenue, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM income_cache WHERE filing_epoch <= ?
        ),
        bs AS (
            SELECT symbol, totalAssets, totalStockholdersEquity, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM balance_cache WHERE filing_epoch <= ?
              AND totalStockholdersEquity > 0
        ),
        met AS (
            SELECT symbol, marketCap, filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC) AS rn
            FROM metrics_cache WHERE filing_epoch <= ?
        ),
        dupont_raw AS (
            SELECT
                inc.symbol,
                inc.netIncome * 1.0 / NULLIF(inc.revenue, 0) AS net_margin,
                inc.revenue * 1.0 / NULLIF(bs.totalAssets, 0) AS asset_turnover,
                bs.totalAssets * 1.0 / NULLIF(bs.totalStockholdersEquity, 0) AS equity_multiplier,
                (inc.netIncome * 1.0 / NULLIF(inc.revenue, 0))
                    * (inc.revenue * 1.0 / NULLIF(bs.totalAssets, 0))
                    * (bs.totalAssets * 1.0 / NULLIF(bs.totalStockholdersEquity, 0)) AS roe_dupont,
                met.marketCap
            FROM inc
            JOIN bs ON inc.symbol = bs.symbol AND bs.rn = 1
            JOIN met ON inc.symbol = met.symbol AND met.rn = 1
            JOIN universe u ON inc.symbol = u.symbol
            WHERE inc.rn = 1
              AND met.marketCap > ?
        ),
        roe_filtered AS (
            SELECT *
            FROM dupont_raw
            WHERE roe_dupont > ?
              AND net_margin IS NOT NULL
              AND asset_turnover IS NOT NULL
              AND equity_multiplier IS NOT NULL
              AND equity_multiplier > 0
        ),
        ranked AS (
            SELECT *,
                NTILE(4) OVER (ORDER BY net_margin DESC) AS margin_quartile,
                NTILE(4) OVER (ORDER BY equity_multiplier DESC) AS leverage_quartile
            FROM roe_filtered
        )
        SELECT symbol, net_margin, asset_turnover, equity_multiplier,
               roe_dupont, marketCap, margin_quartile, leverage_quartile
        FROM ranked
    """, [cutoff_epoch, cutoff_epoch, cutoff_epoch, mktcap_min, ROE_MIN]).fetchall()

    result = {}
    for (sym, nm, at, em, roe, mcap, mq, lq) in rows:
        # Determine classification
        is_quality = (nm > NET_MARGIN_MIN and em < EQUITY_MULT_MAX)
        is_margin_driven = (mq == 1)
        is_leverage_driven = (lq == 1)

        result[sym] = {
            "net_margin": nm,
            "asset_turnover": at,
            "equity_multiplier": em,
            "roe_dupont": roe,
            "market_cap": mcap,
            "is_quality": is_quality,
            "is_margin_driven": is_margin_driven,
            "is_leverage_driven": is_leverage_driven,
        }
    return result


def get_price(con, symbol, target_date):
    """Get adjusted close price on or just after target_date."""
    target_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(target_date + timedelta(days=10), datetime.min.time()).timestamp())
    row = con.execute("""
        SELECT adjClose FROM prices_cache
        WHERE symbol = ? AND trade_epoch >= ? AND trade_epoch <= ?
        ORDER BY trade_epoch ASC LIMIT 1
    """, [symbol, target_epoch, end_epoch]).fetchone()
    return row[0] if row else None


def compute_portfolio_return(con, portfolio_symbols, scored, entry_date, exit_date,
                             use_costs=True, verbose=False):
    """Compute equal-weighted return for a portfolio of stocks.

    Args:
        portfolio_symbols: list of symbol strings
        scored: dict from compute_dupont()

    Returns:
        tuple (mean_return, count, skipped_count)
    """
    if not portfolio_symbols:
        return 0.0, 0, 0

    symbol_returns = []
    for sym in portfolio_symbols:
        ep = get_price(con, sym, entry_date)
        xp = get_price(con, sym, exit_date)
        mcap = scored[sym]["market_cap"]
        symbol_returns.append((sym, ep, xp, mcap))

    clean, skipped = filter_returns(symbol_returns, max_single_return=MAX_SINGLE_RETURN,
                                    verbose=verbose)

    if not clean:
        return 0.0, 0, len(skipped)

    returns = []
    for sym, raw_ret, mcap in clean:
        if use_costs:
            cost = tiered_cost(mcap)
            net_ret = apply_costs(raw_ret, cost)
        else:
            net_ret = raw_ret
        returns.append(net_ret)

    mean_ret = sum(returns) / len(returns)
    return mean_ret, len(returns), len(skipped)


def run_backtest(con, rebalance_dates, mktcap_min, use_costs=True, verbose=False):
    """Run the full DuPont ROE backtest with four portfolio tracks."""
    print(f"Phase 2: Running annual backtest "
          f"({rebalance_dates[0].year}-{rebalance_dates[-1].year})...")
    periods = []

    for i in range(len(rebalance_dates) - 1):
        entry_date = rebalance_dates[i]
        exit_date = rebalance_dates[i + 1]

        scored = compute_dupont(con, entry_date, mktcap_min)
        if not scored:
            if verbose:
                print(f"  {entry_date.year}: No scored stocks found, skipping")
            continue

        # Build portfolio lists
        quality = [s for s, v in scored.items() if v["is_quality"]]
        margin = [s for s, v in scored.items() if v["is_margin_driven"]]
        leverage = [s for s, v in scored.items() if v["is_leverage_driven"]]
        all_roe = list(scored.keys())

        # Compute returns for each track
        track_data = {}
        for name, syms in [("quality_roe", quality),
                           ("margin_driven", margin),
                           ("leverage_driven", leverage),
                           ("all_high_roe", all_roe)]:
            ret, cnt, skip = compute_portfolio_return(
                con, syms, scored, entry_date, exit_date,
                use_costs=use_costs, verbose=verbose
            )
            track_data[name] = {"return": ret, "count": cnt, "skipped": skip}

        # Benchmark
        spy_ep = get_price(con, "SPY", entry_date)
        spy_xp = get_price(con, "SPY", exit_date)
        spy_ret = ((spy_xp - spy_ep) / spy_ep
                   if spy_ep and spy_xp and spy_ep > 0 else None)

        periods.append({
            "year": entry_date.year,
            "entry": entry_date.isoformat(),
            "exit": exit_date.isoformat(),
            "quality_roe_return": track_data["quality_roe"]["return"],
            "margin_driven_return": track_data["margin_driven"]["return"],
            "leverage_driven_return": track_data["leverage_driven"]["return"],
            "all_high_roe_return": track_data["all_high_roe"]["return"],
            "spy_return": spy_ret,
            "quality_roe_count": track_data["quality_roe"]["count"],
            "margin_driven_count": track_data["margin_driven"]["count"],
            "leverage_driven_count": track_data["leverage_driven"]["count"],
            "all_high_roe_count": track_data["all_high_roe"]["count"],
        })

        if verbose:
            s = track_data
            spy_pct = spy_ret * 100 if spy_ret else 0
            print(f"  {entry_date.year}: "
                  f"Quality={s['quality_roe']['return']*100:+.1f}% ({s['quality_roe']['count']}), "
                  f"Margin={s['margin_driven']['return']*100:+.1f}% ({s['margin_driven']['count']}), "
                  f"Lever={s['leverage_driven']['return']*100:+.1f}% ({s['leverage_driven']['count']}), "
                  f"AllROE={s['all_high_roe']['return']*100:+.1f}% ({s['all_high_roe']['count']}), "
                  f"SPY={spy_pct:+.1f}%")

    print(f"Phase 2 complete: {len(periods)} annual periods.\n")
    return periods


def build_output(periods, universe_name, risk_free_rate, periods_per_year):
    """Build output dict with all tracks + analysis."""
    valid = [p for p in periods if p["spy_return"] is not None]
    n = len(valid)
    if n == 0:
        return {"universe": universe_name, "error": "No valid periods"}

    spy_returns = [p["spy_return"] for p in valid]

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    results = {}
    for track, key in [("quality_roe", "quality_roe_return"),
                       ("margin_driven", "margin_driven_return"),
                       ("leverage_driven", "leverage_driven_return"),
                       ("all_high_roe", "all_high_roe_return"),
                       ("spy", "spy_return")]:
        rets = [p[key] for p in valid]
        m = _compute_metrics(rets, spy_returns, periods_per_year,
                             risk_free_rate=risk_free_rate)
        pm = m["portfolio"]

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

        # Comparison metrics for quality_roe and margin_driven
        if track in ("quality_roe", "margin_driven"):
            c = m["comparison"]
            results[f"{track}_vs_spy"] = {
                "excess_cagr": round(c["excess_cagr"] * 100, 2),
                "information_ratio": rnd(c["information_ratio"]),
                "tracking_error": (round(c["tracking_error"] * 100, 2)
                                   if c["tracking_error"] is not None else None),
                "up_capture": (round(c["up_capture"] * 100, 1)
                               if c["up_capture"] is not None else None),
                "down_capture": (round(c["down_capture"] * 100, 1)
                                 if c["down_capture"] is not None else None),
                "beta": rnd(c["beta"]),
                "alpha": (round(c["alpha"] * 100, 2)
                          if c["alpha"] is not None else None),
                "win_rate": (round(c["win_rate"] * 100, 1)
                             if c["win_rate"] is not None else None),
            }

    # Decade breakdown
    decades = {}
    for p in valid:
        yr = p["year"]
        if yr < 2005:
            d = "2000-04"
        elif yr < 2010:
            d = "2005-09"
        elif yr < 2015:
            d = "2010-14"
        elif yr < 2020:
            d = "2015-19"
        else:
            d = "2020-25"

        if d not in decades:
            decades[d] = {"quality": [], "margin": [], "leverage": [], "spy": []}
        decades[d]["quality"].append(p["quality_roe_return"])
        decades[d]["margin"].append(p["margin_driven_return"])
        decades[d]["leverage"].append(p["leverage_driven_return"])
        decades[d]["spy"].append(p["spy_return"])

    decade_results = []
    for d in ["2000-04", "2005-09", "2010-14", "2015-19", "2020-25"]:
        if d in decades:
            q_avg = sum(decades[d]["quality"]) / len(decades[d]["quality"]) * 100
            m_avg = sum(decades[d]["margin"]) / len(decades[d]["margin"]) * 100
            l_avg = sum(decades[d]["leverage"]) / len(decades[d]["leverage"]) * 100
            spy_avg = sum(decades[d]["spy"]) / len(decades[d]["spy"]) * 100
            decade_results.append({
                "period": d,
                "quality_roe_return": round(q_avg, 1),
                "margin_driven_return": round(m_avg, 1),
                "leverage_driven_return": round(l_avg, 1),
                "margin_leverage_spread": round(m_avg - l_avg, 1),
                "spy_return": round(spy_avg, 1),
            })

    # Aggregate metrics
    margin_leverage_spread = results["margin_driven"]["cagr"] - results["leverage_driven"]["cagr"]
    quality_excess = results["quality_roe"]["cagr"] - results["spy"]["cagr"]

    avg_quality = sum(p["quality_roe_count"] for p in valid) / n
    avg_margin = sum(p["margin_driven_count"] for p in valid) / n
    avg_leverage = sum(p["leverage_driven_count"] for p in valid) / n
    avg_all = sum(p["all_high_roe_count"] for p in valid) / n

    quality_cash = sum(1 for p in valid if p["quality_roe_count"] == 0)
    margin_cash = sum(1 for p in valid if p["margin_driven_count"] == 0)

    output = {
        "universe": universe_name,
        "period": f"{valid[0]['year']}-{valid[-1]['year'] + 1} ({n} years)",
        "rebalancing": "annual (April 1)",
        "weighting": "equal weight",
        "transaction_costs": "0.1-0.5% per trade (size-tiered)",
        "sector_exclusions": "Financial Services, Utilities",
        "signal": {
            "roe_min": ROE_MIN,
            "quality_filters": {
                "net_margin_min": NET_MARGIN_MIN,
                "equity_multiplier_max": EQUITY_MULT_MAX,
            },
        },
        "portfolios": {
            "quality_roe": results["quality_roe"],
            "margin_driven": results["margin_driven"],
            "leverage_driven": results["leverage_driven"],
            "all_high_roe": results["all_high_roe"],
            "sp500": results["spy"],
        },
        "margin_leverage_spread": round(margin_leverage_spread, 2),
        "quality_excess_cagr": round(quality_excess, 2),
        "avg_stock_counts": {
            "quality_roe": round(avg_quality, 0),
            "margin_driven": round(avg_margin, 0),
            "leverage_driven": round(avg_leverage, 0),
            "all_high_roe": round(avg_all, 0),
        },
        "cash_periods": {
            "quality_roe": quality_cash,
            "margin_driven": margin_cash,
        },
        "decade_breakdown": decade_results,
    }

    if "quality_roe_vs_spy" in results:
        output["quality_roe_vs_spy"] = results["quality_roe_vs_spy"]
    if "margin_driven_vs_spy" in results:
        output["margin_driven_vs_spy"] = results["margin_driven_vs_spy"]

    # Period-level data for charts
    output["annual_returns"] = [
        {
            "year": p["year"],
            "quality_roe": round(p["quality_roe_return"] * 100, 2),
            "margin_driven": round(p["margin_driven_return"] * 100, 2),
            "leverage_driven": round(p["leverage_driven_return"] * 100, 2),
            "all_high_roe": round(p["all_high_roe_return"] * 100, 2),
            "spy": round(p["spy_return"] * 100, 2),
            "quality_roe_count": p["quality_roe_count"],
            "margin_driven_count": p["margin_driven_count"],
            "leverage_driven_count": p["leverage_driven_count"],
        }
        for p in valid
    ]

    return output


def print_summary(m):
    """Print formatted backtest results."""
    if "error" in m:
        print(f"\nERROR: {m['error']}")
        return

    p = m["portfolios"]
    print("\n" + "=" * 100)
    print(f"DUPONT ROE DECOMPOSITION BACKTEST: {m['universe']}")
    print("=" * 100)
    print(f"Period: {m['period']}")
    print(f"Rebalancing: {m['rebalancing']}")
    print(f"Costs: {m['transaction_costs']}")
    print(f"Excluded: {m['sector_exclusions']}")
    print(f"Signal: ROE > {ROE_MIN*100:.0f}%, Quality = margin > {NET_MARGIN_MIN*100:.0f}% "
          f"& eq_mult < {EQUITY_MULT_MAX:.1f}")
    counts = m["avg_stock_counts"]
    print(f"Avg stocks: Quality={counts['quality_roe']:.0f}, "
          f"Margin-Q1={counts['margin_driven']:.0f}, "
          f"Lever-Q1={counts['leverage_driven']:.0f}, "
          f"All ROE>15%={counts['all_high_roe']:.0f}")
    cash = m["cash_periods"]
    if cash["quality_roe"] > 0 or cash["margin_driven"] > 0:
        print(f"Cash periods: Quality={cash['quality_roe']}, "
              f"Margin-Q1={cash['margin_driven']}")
    print("-" * 100)

    header = (f"{'Portfolio':<24} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} "
              f"{'Sortino':>8} {'Calmar':>8} {'MaxDD':>8} {'VaR95':>8}")
    print(header)
    print("-" * 100)

    for name, label in [("quality_roe", "Quality ROE"),
                        ("margin_driven", "Margin-Driven (Q1)"),
                        ("all_high_roe", "All ROE > 15%"),
                        ("leverage_driven", "Leverage-Driven (Q1)"),
                        ("sp500", "S&P 500")]:
        d = p[name]
        sortino = d.get('sortino')
        calmar = d.get('calmar')
        var95 = d.get('var_95')
        s_str = f"{sortino:>8.3f}" if sortino is not None else f"{'N/A':>8}"
        c_str = f"{calmar:>8.3f}" if calmar is not None else f"{'N/A':>8}"
        v_str = f"{var95:>7.1f}%" if var95 is not None else f"{'N/A':>8}"
        sharpe = d.get('sharpe')
        sh_str = f"{sharpe:>8.3f}" if sharpe is not None else f"{'N/A':>8}"
        print(f"{label:<24} {d['cagr']:>7.1f}% {d['volatility']:>7.1f}% "
              f"{sh_str} {s_str} {c_str} "
              f"{d['max_drawdown']:>7.1f}% {v_str}")

    print(f"\nMargin-Leverage spread: {m['margin_leverage_spread']:+.1f}% per year")
    print(f"Quality ROE excess CAGR: {m['quality_excess_cagr']:+.1f}%")

    # Quality ROE vs SPY comparison
    qvs = m.get("quality_roe_vs_spy")
    if qvs:
        print(f"\nQuality ROE vs S&P 500:")
        print(f"  Excess CAGR: {qvs['excess_cagr']:+.2f}%")
        if qvs.get('win_rate') is not None:
            print(f"  Win Rate: {qvs['win_rate']:.1f}%")
        if qvs.get('information_ratio') is not None:
            print(f"  Information Ratio: {qvs['information_ratio']:.3f}")
        if qvs.get('up_capture') is not None:
            print(f"  Up Capture: {qvs['up_capture']:.1f}%  |  "
                  f"Down Capture: {qvs['down_capture']:.1f}%")
        if qvs.get('beta') is not None:
            print(f"  Beta: {qvs['beta']:.3f}  |  Alpha: {qvs['alpha']:+.2f}%")

    if m.get("decade_breakdown"):
        print(f"\n{'Period':<12} {'Quality':>10} {'Margin':>10} {'Leverage':>10} "
              f"{'M-L Spread':>12} {'SPY':>10}")
        print("-" * 68)
        for d in m["decade_breakdown"]:
            print(f"{d['period']:<12} {d['quality_roe_return']:>9.1f}% "
                  f"{d['margin_driven_return']:>9.1f}% "
                  f"{d['leverage_driven_return']:>9.1f}% "
                  f"{d['margin_leverage_spread']:>+11.1f}% "
                  f"{d['spy_return']:>9.1f}%")

    print("=" * 100)


def run_single_exchange(args, preset_name=None, preset_data=None):
    """Run backtest for a single exchange/preset. Returns output dict."""
    if preset_data:
        exchanges = preset_data["exchanges"]
        universe_name = preset_data["name"]
    else:
        exchanges, universe_name = resolve_exchanges(
            args,
            default_exchanges=["NYSE", "NASDAQ", "AMEX"],
            default_name="US_MAJOR"
        )

    risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9
                    else f"{mktcap_threshold/1e6:.0f}M")
    use_costs = not args.no_costs
    periods_per_year = 1

    signal_desc = (f"DuPont ROE decomposition, ROE > {ROE_MIN*100:.0f}%, "
                   f"MCap > {mktcap_label} local, excl. financials/utilities")
    print_header("DUPONT ROE DECOMPOSITION BACKTEST", universe_name,
                 exchanges, signal_desc)
    print(f"  Quality: margin > {NET_MARGIN_MIN*100:.0f}%, "
          f"eq_mult < {EQUITY_MULT_MAX:.1f}")
    print(f"  Rebalancing: Annual (April 1), {START_YEAR}-{END_YEAR}")
    print(f"  Costs: {'size-tiered' if use_costs else 'none'}, "
          f"Rf: {risk_free_rate*100:.1f}%")
    print("=" * 65)

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data via API...")
    rebalance_dates = generate_rebalance_dates(
        START_YEAR, END_YEAR, "annual", months=[4]
    )
    t0 = time.time()
    con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)
    if con is None:
        print("No data available.")
        return {"universe": universe_name, "error": "No data"}
    fetch_time = time.time() - t0
    print(f"\nData fetched in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    t1 = time.time()
    periods = run_backtest(con, rebalance_dates, mktcap_threshold,
                           use_costs=use_costs, verbose=args.verbose)
    bt_time = time.time() - t1

    # Phase 3: Compute and display metrics
    output = build_output(periods, universe_name, risk_free_rate, periods_per_year)
    print_summary(output)

    total_time = time.time() - t0
    print(f"\nTotal time: {total_time:.0f}s "
          f"(fetch: {fetch_time:.0f}s, backtest: {bt_time:.0f}s)")

    con.close()
    return output


def main():
    parser = argparse.ArgumentParser(
        description="DuPont ROE Decomposition backtest"
    )
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud(
            "roe-dupont", args_str=" ".join(cloud_args),
            api_key=args.api_key, base_url=args.base_url, verbose=True
        )
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    if args.global_bt:
        all_results = {}
        for preset_name in GLOBAL_PRESETS:
            preset = EXCHANGE_PRESETS[preset_name]
            print(f"\n{'#' * 80}")
            print(f"# Exchange: {preset['name']} ({preset_name})")
            print(f"{'#' * 80}")
            try:
                output = run_single_exchange(args, preset_name, preset)
                all_results[preset["name"]] = output
            except Exception as e:
                print(f"  ERROR: {e}")
                import traceback
                traceback.print_exc()
                all_results[preset["name"]] = {
                    "universe": preset["name"],
                    "error": str(e),
                }

        # Print comparison summary
        print("\n" + "=" * 110)
        print("EXCHANGE COMPARISON SUMMARY")
        print("=" * 110)
        print(f"{'Exchange':<16} {'QualROE':>10} {'MarginQ1':>10} "
              f"{'LeverQ1':>10} {'M-L Sprd':>10} {'AllROE':>10} {'SPY':>10} "
              f"{'AvgQual':>10}")
        print("-" * 100)
        for name, r in all_results.items():
            if "error" in r:
                print(f"{name:<16} {'ERROR':>10}  {r.get('error', '')[:40]}")
                continue
            p = r["portfolios"]
            avg = r["avg_stock_counts"]
            print(f"{name:<16} {p['quality_roe']['cagr']:>9.1f}% "
                  f"{p['margin_driven']['cagr']:>9.1f}% "
                  f"{p['leverage_driven']['cagr']:>9.1f}% "
                  f"{r['margin_leverage_spread']:>+9.1f}% "
                  f"{p['all_high_roe']['cagr']:>9.1f}% "
                  f"{p['sp500']['cagr']:>9.1f}% "
                  f"{avg['quality_roe']:>10.0f}")
        print("=" * 110)

        # Save results
        if args.output:
            out_dir = os.path.dirname(args.output) or "."
            os.makedirs(out_dir, exist_ok=True)
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\nResults saved to {args.output}")
    else:
        output = run_single_exchange(args)

        if args.output:
            out_dir = os.path.dirname(args.output) or "."
            os.makedirs(out_dir, exist_ok=True)
            with open(args.output, "w") as f:
                json.dump(output, f, indent=2)
            print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
