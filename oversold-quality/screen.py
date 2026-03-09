#!/usr/bin/env python3
"""
Oversold Quality - Current Stock Screen

Screens for fundamentally strong companies that are technically oversold.
Uses TTM data for a quality proxy (5-factor) + computes RSI-14 from recent daily prices.

Quality proxy (5-factor TTM, approximating Piotroski >= 7):
    - ROA TTM > 0 (profitable)
    - Income quality TTM > 1 (OCF exceeds net income = low accruals)
    - Current ratio > 1 (liquid)
    - ROE > 10% (shareholder returns)
    - Net Debt/EBITDA < 3 (manageable leverage)

RSI < 30: Computed from last 14 available daily closing prices.

Note: The full backtest uses the 9-factor Piotroski score on annual filings.
This screen uses TTM data as a practical proxy for real-time screening.

Usage:
    python3 oversold-quality/screen.py
    python3 oversold-quality/screen.py --preset india
    python3 oversold-quality/screen.py --exchange XETRA
    python3 oversold-quality/screen.py --cloud
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Signal parameters (match backtest.py)
RSI_MAX = 30
RSI_LOOKBACK = 14
MIN_PERIODS_RSI = 10
MAX_STOCKS = 30

# TTM quality proxy thresholds
ROA_MIN = 0.0       # ROA > 0 (profitable)
ROE_MIN = 0.10      # ROE > 10% (quality return)
INCOME_QUALITY_MIN = 1.0   # OCF > net income (cash quality)
CURRENT_RATIO_MIN = 1.0    # Current ratio > 1 (liquid)
NET_DEBT_EBITDA_MAX = 3.0  # Net Debt/EBITDA < 3 (not over-leveraged)


def run_screen(client, exchanges, mktcap_min, verbose=False):
    """Run live screen using TTM data + recent RSI. Returns list of dicts."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    # Step 1: Get quality-passing stocks (TTM proxy for Piotroski >= 7)
    quality_sql = f"""
        SELECT
            k.symbol,
            p.companyName,
            p.exchange,
            p.sector,
            ROUND(k.returnOnAssetsTTM * 100, 1) AS roa_pct,
            ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
            ROUND(k.incomeQualityTTM, 2) AS income_quality,
            ROUND(k.currentRatioTTM, 2) AS current_ratio,
            ROUND(k.netDebtToEBITDATTM, 2) AS net_debt_ebitda,
            ROUND(k.marketCap / 1e9, 2) AS mktcap_b
        FROM key_metrics_ttm k
        JOIN profile p ON k.symbol = p.symbol
        WHERE k.returnOnAssetsTTM > {ROA_MIN}
          AND k.returnOnEquityTTM > {ROE_MIN}
          AND k.incomeQualityTTM > {INCOME_QUALITY_MIN}
          AND k.currentRatioTTM > {CURRENT_RATIO_MIN}
          AND k.netDebtToEBITDATTM < {NET_DEBT_EBITDA_MAX}
          AND k.netDebtToEBITDATTM > -10
          AND k.marketCap > {mktcap_min}
          {exchange_filter}
        ORDER BY k.symbol
    """

    quality_stocks = client.query(quality_sql, verbose=verbose)
    if not quality_stocks:
        return []

    if verbose:
        print(f"  Quality proxy: {len(quality_stocks)} stocks pass TTM filters")

    symbols = [r["symbol"] for r in quality_stocks]
    sym_list = ", ".join(f"'{s}'" for s in symbols)

    # Step 2: Compute RSI from recent 30 days of daily prices
    # Fetch last 35 calendar days of closes, compute RSI-14 in SQL
    rsi_sql = f"""
        WITH recent_prices AS (
            SELECT symbol, dateEpoch as trade_epoch, adjClose,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) as rn
            FROM stock_eod
            WHERE symbol IN ({sym_list})
              AND adjClose > 0
              AND date >= (CURRENT_DATE - INTERVAL 35 DAY)
        ),
        with_changes AS (
            SELECT symbol,
                   adjClose - LEAD(adjClose) OVER (PARTITION BY symbol ORDER BY rn) as change
            FROM recent_prices
            WHERE rn <= 15
        ),
        gain_loss AS (
            SELECT symbol,
                   CASE WHEN change > 0 THEN change ELSE 0 END as gain,
                   CASE WHEN change < 0 THEN -change ELSE 0 END as loss
            FROM with_changes
            WHERE change IS NOT NULL
        ),
        avg_gl AS (
            SELECT symbol,
                   AVG(gain) as avg_gain,
                   AVG(loss) as avg_loss,
                   COUNT(*) as n_periods
            FROM gain_loss
            GROUP BY symbol
            HAVING COUNT(*) >= {MIN_PERIODS_RSI}
        )
        SELECT symbol,
               ROUND(
                   CASE
                       WHEN avg_loss = 0 THEN 100.0
                       WHEN avg_gain = 0 THEN 0.0
                       ELSE 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
                   END, 1
               ) as rsi_14
        FROM avg_gl
        WHERE CASE
                  WHEN avg_loss = 0 THEN 100.0
                  WHEN avg_gain = 0 THEN 0.0
                  ELSE 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
              END < {RSI_MAX}
        ORDER BY rsi_14 ASC
        LIMIT {MAX_STOCKS}
    """

    rsi_results = client.query(rsi_sql, verbose=verbose)
    if not rsi_results:
        return []

    oversold_symbols = {r["symbol"]: r["rsi_14"] for r in rsi_results}

    # Combine quality + RSI
    combined = []
    quality_lookup = {r["symbol"]: r for r in quality_stocks}
    for symbol, rsi in sorted(oversold_symbols.items(), key=lambda x: x[1]):
        if symbol in quality_lookup:
            rec = quality_lookup[symbol].copy()
            rec["rsi_14"] = rsi
            combined.append(rec)

    return combined[:MAX_STOCKS]


def main():
    parser = argparse.ArgumentParser(description="Oversold Quality live screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", dest="output_json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch as CR
        cr = CR(api_key=args.api_key, base_url=args.base_url)
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = cr.execute_code(
            f"python3 oversold-quality/screen.py {' '.join(cloud_args)}",
            verbose=True
        )
        print(result)
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_min = get_mktcap_threshold(exchanges)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"Oversold Quality Screen | Universe: {universe_name}")
    print(f"Quality proxy: ROA>0, ROE>{ROE_MIN*100:.0f}%, IncomeQuality>{INCOME_QUALITY_MIN}, "
          f"CurrentRatio>{CURRENT_RATIO_MIN}, NetDebt/EBITDA<{NET_DEBT_EBITDA_MAX}")
    print(f"RSI filter: RSI-14 < {RSI_MAX} (technically oversold)")
    print(f"MCap > {mktcap_min/1e9:.1f}B local currency")
    print("=" * 90)

    results = run_screen(cr, exchanges, mktcap_min, verbose=args.verbose)

    if not results:
        print("No qualifying stocks found.")
        return

    if args.output_json:
        print(json.dumps(results, indent=2))
        return

    print(f"\n{'#':<5} {'Symbol':<12} {'Company':<30} {'RSI':>5} {'ROE%':>6} "
          f"{'ROA%':>6} {'IQ':>5} {'CR':>5} {'ND/E':>6} {'MCap$B':>8}")
    print("-" * 90)
    for i, r in enumerate(results, 1):
        company = (r.get("companyName") or "")[:28]
        print(f"{i:<5} {r.get('symbol', ''):<12} {company:<30} "
              f"{r.get('rsi_14', 'N/A'):>5} "
              f"{r.get('roe_pct', 'N/A'):>6} "
              f"{r.get('roa_pct', 'N/A'):>6} "
              f"{r.get('income_quality', 'N/A'):>5} "
              f"{r.get('current_ratio', 'N/A'):>5} "
              f"{r.get('net_debt_ebitda', 'N/A'):>6} "
              f"{r.get('mktcap_b', 'N/A'):>8}")

    print(f"\n{len(results)} stocks qualify.")
    print(f"RSI = 14-period RSI from last 35 calendar days. IQ = Income Quality (OCF/NI). "
          f"ND/E = Net Debt/EBITDA.")
    print("Note: Quality proxy uses TTM data. Backtest uses full 9-factor annual Piotroski score.")
    print("Data: Ceta Research (FMP financial data warehouse).")


if __name__ == "__main__":
    main()
