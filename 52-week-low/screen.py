#!/usr/bin/env python3
"""
52-Week Low Quality - Current Stock Screen

Screens for quality stocks near their 52-week lows using live data.
Uses TTM (trailing twelve months) financial data + recent price history.

Signal:
  - Price within 15% of 52-week low: (price - low_52w) / low_52w <= 0.15
  - Piotroski F-score >= 7 (from TTM/latest annual data)
  - Market cap > exchange-specific threshold

Usage:
    python3 52-week-low/screen.py
    python3 52-week-low/screen.py --preset india
    python3 52-week-low/screen.py --exchange XETRA
    python3 52-week-low/screen.py --cloud
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Signal parameters (match backtest.py)
PROXIMITY_THRESHOLD = 0.15   # Within 15% of 52-week low
PIOTROSKI_MIN = 7            # Minimum Piotroski F-score
MAX_STOCKS = 30


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data + recent prices. Returns list of dicts.

    Note: Piotroski uses TTM/latest annual data; price uses last 252 trading days
    from stock_eod. This matches the backtest logic but uses most recent data.
    """
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
        WITH
        -- Latest annual financial data per stock
        inc AS (
            SELECT symbol, netIncome, grossProfit, revenue, dateEpoch as filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn_curr,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM income_statement
            WHERE period = 'FY' AND netIncome IS NOT NULL
        ),
        inc_prev AS (
            SELECT symbol, netIncome, grossProfit, revenue
            FROM inc WHERE rn = 2
        ),
        inc_curr AS (
            SELECT symbol, netIncome, grossProfit, revenue, filing_epoch
            FROM inc WHERE rn = 1
        ),
        bal AS (
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                   longTermDebt, totalStockholdersEquity, dateEpoch as filing_epoch,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM balance_sheet
            WHERE period = 'FY' AND totalAssets IS NOT NULL AND totalAssets > 0
        ),
        bal_prev AS (
            SELECT symbol, totalAssets, longTermDebt, totalCurrentAssets,
                   totalCurrentLiabilities, totalStockholdersEquity
            FROM bal WHERE rn = 2
        ),
        bal_curr AS (
            SELECT symbol, totalAssets, totalCurrentAssets, totalCurrentLiabilities,
                   longTermDebt, totalStockholdersEquity
            FROM bal WHERE rn = 1
        ),
        cf AS (
            SELECT symbol, operatingCashFlow,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
            FROM cash_flow_statement
            WHERE period = 'FY' AND operatingCashFlow IS NOT NULL
        ),
        cf_curr AS (
            SELECT symbol, operatingCashFlow FROM cf WHERE rn = 1
        ),
        -- Piotroski F-score (0-9)
        piotroski AS (
            SELECT ic.symbol,
                CASE WHEN ic.netIncome > 0 THEN 1 ELSE 0 END
                + CASE WHEN cfc.operatingCashFlow > 0 THEN 1 ELSE 0 END
                + CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                       AND (ic.netIncome / bc.totalAssets) > (ip.netIncome / bp.totalAssets)
                       THEN 1 ELSE 0 END
                + CASE WHEN bc.totalAssets > 0
                       AND cfc.operatingCashFlow / bc.totalAssets > ic.netIncome / bc.totalAssets
                       THEN 1 ELSE 0 END
                + CASE WHEN bc.totalAssets > 0 AND bp.totalAssets > 0
                       AND (COALESCE(bc.longTermDebt,0) / bc.totalAssets)
                         < (COALESCE(bp.longTermDebt,0) / bp.totalAssets)
                       THEN 1 ELSE 0 END
                + CASE WHEN bc.totalCurrentLiabilities > 0 AND bp.totalCurrentLiabilities > 0
                       AND (bc.totalCurrentAssets / bc.totalCurrentLiabilities)
                         > (bp.totalCurrentAssets / bp.totalCurrentLiabilities)
                       THEN 1 ELSE 0 END
                + CASE WHEN bc.totalStockholdersEquity >= bp.totalStockholdersEquity
                       THEN 1 ELSE 0 END
                + CASE WHEN ic.revenue > 0 AND ip.revenue > 0
                       AND bc.totalAssets > 0 AND bp.totalAssets > 0
                       AND (ic.revenue / bc.totalAssets) > (ip.revenue / bp.totalAssets)
                       THEN 1 ELSE 0 END
                + CASE WHEN ic.grossProfit > 0 AND ip.grossProfit > 0
                       AND ic.revenue > 0 AND ip.revenue > 0
                       AND (ic.grossProfit / ic.revenue) > (ip.grossProfit / ip.revenue)
                       THEN 1 ELSE 0 END
                AS f_score
            FROM inc_curr ic
            JOIN inc_prev ip ON ic.symbol = ip.symbol
            JOIN bal_curr bc ON ic.symbol = bc.symbol
            JOIN bal_prev bp ON ic.symbol = bp.symbol
            JOIN cf_curr cfc ON ic.symbol = cfc.symbol
        ),
        -- Current price and 52-week low from recent EOD data
        recent_prices AS (
            SELECT symbol,
                   LAST_VALUE(adjClose) OVER (
                       PARTITION BY symbol ORDER BY dateEpoch
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   ) AS current_price,
                   MIN(adjClose) OVER (PARTITION BY symbol) AS low_52w
            FROM stock_eod
            WHERE dateEpoch >= (EPOCH(CURRENT_DATE) - 365*86400)
              AND adjClose IS NOT NULL AND adjClose > 0
        ),
        price_summary AS (
            SELECT symbol,
                   MAX(current_price) AS current_price,
                   MIN(low_52w) AS low_52w
            FROM recent_prices
            GROUP BY symbol
        )
        SELECT
            pio.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            p.industry,
            ROUND(pio.f_score, 0) AS piotroski_score,
            ROUND(ps.current_price, 2) AS current_price,
            ROUND(ps.low_52w, 2) AS low_52w,
            ROUND((ps.current_price - ps.low_52w) / ps.low_52w * 100, 1) AS pct_above_low,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_ttm,
            ROUND(k.priceToEarningsTTM, 1) AS pe_ttm
        FROM piotroski pio
        JOIN profile p ON pio.symbol = p.symbol
        JOIN key_metrics_ttm k ON pio.symbol = k.symbol
        JOIN price_summary ps ON pio.symbol = ps.symbol
        WHERE pio.f_score >= {PIOTROSKI_MIN}
          AND ps.low_52w > 0
          AND (ps.current_price - ps.low_52w) / ps.low_52w <= {PROXIMITY_THRESHOLD}
          AND ps.current_price >= 1.0
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        ORDER BY pct_above_low ASC
        LIMIT {MAX_STOCKS}
    """

    if verbose:
        print("Running screen query...")
    results = client.query(sql, verbose=verbose, timeout=300)
    return results


def main():
    parser = argparse.ArgumentParser(description="52-Week Low Quality - current screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_screen_cloud("52-week-low", args_str=" ".join(cloud_args),
                                   api_key=args.api_key, base_url=args.base_url,
                                   verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = f"{mktcap_threshold/1e9:.0f}B" if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M"
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"52-Week Low Quality Screen - {universe_name}")
    print(f"Signal: Price within {PROXIMITY_THRESHOLD*100:.0f}% of 52w low, "
          f"Piotroski >= {PIOTROSKI_MIN}, MCap > {mktcap_label} local")
    print("-" * 100)

    results = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not results:
        print("No stocks qualify.")
        return

    print(f"\n{'#':<4} {'Symbol':<10} {'Company':<28} {'Exch':<6} {'F':<4} {'Cur$':>7} "
          f"{'52wLo$':>7} {'Abv%':>5} {'MCap$B':>8} {'ROE%':>6}")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(f"{i:<4} {r['symbol']:<10} {str(r.get('companyName',''))[:26]:<28} "
              f"{r.get('exchange',''):<6} {r.get('piotroski_score',''):>3}  "
              f"{r.get('current_price',''):>7} {r.get('low_52w',''):>7} "
              f"{r.get('pct_above_low',''):>5} {r.get('mktcap_b',''):>8} "
              f"{r.get('roe_ttm',''):>6}")

    print(f"\n{len(results)} stocks qualify.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
