#!/usr/bin/env python3
"""
Sector Correlation Regime Screen

Shows the current market correlation regime based on the last 60 trading days
of S&P 500 sector ETF prices.

Usage:
    python3 sector-correlation/screen.py
    python3 sector-correlation/screen.py --verbose
    python3 sector-correlation/screen.py --cloud
"""

import argparse
import json
import os
import sys
import time
import tempfile
import duckdb
from datetime import date, timedelta
from itertools import combinations

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from data_utils import query_parquet

SECTOR_ETFS = ['XLK', 'XLE', 'XLF', 'XLV', 'XLY', 'XLP', 'XLI', 'XLB', 'XLU']
DEFENSIVE_ETFS = ['XLU', 'XLV', 'XLP']
BENCHMARK = 'SPY'

CORR_WINDOW = 60
HIGH_THRESHOLD = 0.7
LOW_THRESHOLD = 0.4

SECTOR_NAMES = {
    'XLK': 'Technology',
    'XLE': 'Energy',
    'XLF': 'Financials',
    'XLV': 'Healthcare',
    'XLY': 'Consumer Disc.',
    'XLP': 'Consumer Staples',
    'XLI': 'Industrials',
    'XLB': 'Materials',
    'XLU': 'Utilities',
}

SECTOR_PAIRS = list(combinations(SECTOR_ETFS, 2))


def compute_pairwise_matrix(by_symbol):
    """Compute pairwise correlation for all sector pairs. Returns dict and avg."""
    matrix = {}
    corr_values = []

    for s1, s2 in SECTOR_PAIRS:
        if s1 not in by_symbol or s2 not in by_symbol:
            continue
        common_dates = set(by_symbol[s1].keys()) & set(by_symbol[s2].keys())
        if len(common_dates) < 20:
            continue
        dates_sorted = sorted(common_dates)
        r1 = [by_symbol[s1][d] for d in dates_sorted]
        r2 = [by_symbol[s2][d] for d in dates_sorted]

        n = len(r1)
        mean1 = sum(r1) / n
        mean2 = sum(r2) / n
        var1 = sum((x - mean1) ** 2 for x in r1)
        var2 = sum((x - mean2) ** 2 for x in r2)
        cov = sum((r1[i] - mean1) * (r2[i] - mean2) for i in range(n))
        denom = (var1 * var2) ** 0.5
        if denom > 0:
            corr = cov / denom
            matrix[(s1, s2)] = corr
            corr_values.append(corr)

    avg = sum(corr_values) / len(corr_values) if corr_values else None
    return matrix, avg


def main():
    parser = argparse.ArgumentParser(description="Sector Correlation Regime Screen")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--cloud", action="store_true",
                        help="Run via Ceta Research Code Execution API")
    parser.add_argument("--api-key", type=str, default=None)
    parser.add_argument("--base-url", type=str,
                        default="https://api.cetaresearch.com/api/v1")
    args = parser.parse_args()

    if args.cloud:
        api_key = args.api_key or os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY")
        cr = CetaResearch(api_key=api_key, base_url=args.base_url)
        with open(__file__, "r") as f:
            code = f.read()
        code_no_cloud = code.replace("args.cloud", "False")
        result = cr.execute_code(code_no_cloud, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    api_key = args.api_key or os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY")
    cr = CetaResearch(api_key=api_key, base_url=args.base_url)

    # Fetch last 90 calendar days of prices to ensure 60+ trading days
    today = date.today()
    start_date = today - timedelta(days=130)

    all_symbols = SECTOR_ETFS + [BENCHMARK]
    sym_list = ", ".join(f"'{s}'" for s in all_symbols)

    print(f"Fetching sector ETF prices ({start_date} to {today})...")
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_list})
          AND CAST(date AS DATE) >= '{start_date.isoformat()}'
          AND CAST(date AS DATE) <= '{today.isoformat()}'
          AND adjClose IS NOT NULL
          AND adjClose > 0
        ORDER BY symbol, trade_date
    """

    con = duckdb.connect(":memory:")
    n = query_parquet(cr, price_sql, con, "prices_cache",
                      verbose=args.verbose, limit=5000, timeout=60)

    if n == 0:
        print("ERROR: No price data returned.")
        sys.exit(1)

    # Build returns dict
    rows = con.execute("""
        WITH r AS (
            SELECT symbol, trade_date, adjClose,
                   LAG(adjClose) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close
            FROM prices_cache
        )
        SELECT symbol, trade_date, (adjClose - prev_close) / prev_close AS daily_return
        FROM r WHERE prev_close IS NOT NULL AND prev_close > 0
        ORDER BY symbol, trade_date
    """).fetchall()

    by_symbol = {}
    for sym, dt, ret in rows:
        if sym not in by_symbol:
            by_symbol[sym] = {}
        by_symbol[sym][dt] = ret

    # Use last 60 trading days
    all_dates = sorted(set(dt for sym in SECTOR_ETFS if sym in by_symbol
                           for dt in by_symbol[sym].keys()))
    if len(all_dates) < 20:
        print(f"ERROR: Only {len(all_dates)} trading days available. Need at least 20.")
        sys.exit(1)

    cutoff_date = all_dates[-CORR_WINDOW] if len(all_dates) >= CORR_WINDOW else all_dates[0]
    by_symbol_window = {
        sym: {dt: ret for dt, ret in sym_data.items() if dt >= cutoff_date}
        for sym, sym_data in by_symbol.items()
        if sym in SECTOR_ETFS
    }

    matrix, avg_corr = compute_pairwise_matrix(by_symbol_window)

    # Classify regime
    if avg_corr is None:
        regime = "UNKNOWN"
        allocation = [BENCHMARK]
    elif avg_corr > HIGH_THRESHOLD:
        regime = "HIGH"
        allocation = DEFENSIVE_ETFS
    elif avg_corr < LOW_THRESHOLD:
        regime = "LOW"
        allocation = SECTOR_ETFS
    else:
        regime = "MEDIUM"
        allocation = [BENCHMARK]

    window_days = len(all_dates[-CORR_WINDOW:]) if len(all_dates) >= CORR_WINDOW else len(all_dates)
    as_of = all_dates[-1] if all_dates else today

    print(f"\n{'='*60}")
    print(f"  SECTOR CORRELATION REGIME SCREEN")
    print(f"  As of: {as_of}  |  Window: {window_days} trading days")
    print(f"{'='*60}")
    print(f"\n  Average pairwise correlation: {avg_corr:.3f}" if avg_corr else "  Correlation: N/A")
    print(f"  Regime: {regime}")
    print(f"  Threshold: High >0.7, Low <0.4")

    regime_desc = {
        "HIGH": "DEFENSIVE — diversification failing, sectors moving together",
        "MEDIUM": "NEUTRAL — normal market conditions",
        "LOW": "DIVERSIFIED — strong diversification premium available",
    }
    print(f"  Interpretation: {regime_desc.get(regime, 'N/A')}")

    print(f"\n  Allocation: {', '.join(allocation)}")
    if regime == "HIGH":
        print(f"  -> Defensive sectors: Utilities + Healthcare + Staples (equal weight)")
    elif regime == "LOW":
        print(f"  -> All 9 sector ETFs (equal weight)")
    else:
        print(f"  -> SPY (buy and hold)")

    # Print correlation matrix (compact)
    if matrix and args.verbose:
        print(f"\n  Pairwise Correlations (last {window_days} trading days):")
        print(f"  {'':8}", end="")
        for s in SECTOR_ETFS:
            print(f"  {s:4}", end="")
        print()
        for i, s1 in enumerate(SECTOR_ETFS):
            print(f"  {s1:8}", end="")
            for j, s2 in enumerate(SECTOR_ETFS):
                if i == j:
                    print(f"  1.00", end="")
                elif (s1, s2) in matrix:
                    print(f" {matrix[(s1, s2)]:+.2f}", end="")
                elif (s2, s1) in matrix:
                    print(f" {matrix[(s2, s1)]:+.2f}", end="")
                else:
                    print(f"   N/A", end="")
            print()

    # Per-sector recent returns
    spy_data = {dt: ret for dt, ret in by_symbol.get(BENCHMARK, {}).items()
                if dt >= cutoff_date}
    spy_30d = list(spy_data.values())[-22:] if spy_data else []
    spy_30d_cum = ((1 + sum(spy_30d) / len(spy_30d)) ** 22 - 1) * 100 if spy_30d else None

    print(f"\n  ETF Recent Performance (~30d):")
    for sym in SECTOR_ETFS:
        sym_data = {dt: ret for dt, ret in by_symbol.get(sym, {}).items()
                    if dt >= cutoff_date}
        recent = list(sym_data.values())[-22:]
        if recent:
            cum = ((1 + sum(recent) / len(recent)) ** 22 - 1) * 100
            marker = " ***" if sym in DEFENSIVE_ETFS else ""
            name = SECTOR_NAMES.get(sym, sym)
            print(f"    {sym:4} ({name:18}): {cum:+5.1f}%{marker}")

    if spy_30d_cum is not None:
        print(f"    {'SPY':4} ({'S&P 500':18}): {spy_30d_cum:+5.1f}%")

    print(f"\n{'='*60}")
    print(f"  *** = defensive sectors (active in HIGH regime)")
    con.close()


if __name__ == "__main__":
    main()
