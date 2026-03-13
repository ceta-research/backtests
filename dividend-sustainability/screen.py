#!/usr/bin/env python3
"""
Dividend Sustainability Score Screen

Current stock screen using the 5-component sustainability composite.
Returns stocks with score >= 7 out of 10, yield >= 2%.

Components (each 0-2 points, total 0-10):
  1. Payout ratio: <50%=2, 50-80%=1, >80%=0
  2. Debt/Equity: <0.5=2, 0.5-1.5=1, >1.5=0
  3. FCF Coverage: >2x=2, 1-2x=1, <1x=0
  4. ROE: >15%=2, 8-15%=1, <8%=0
  5. Piotroski F-Score: >=7=2, 5-6=1, <5=0

Usage:
    python3 dividend-sustainability/screen.py
    python3 dividend-sustainability/screen.py --preset india
    python3 dividend-sustainability/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

MIN_SCORE = 7
YIELD_MIN = 0.02
MAX_RESULTS = 50


def run_screen(cr, exchanges, universe_name, verbose=False):
    """Run sustainability screen. Returns list of qualifying stocks."""
    mktcap_threshold = get_mktcap_threshold(exchanges)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
    WITH latest_ratios AS (
        SELECT r.symbol, r.dividendPayoutRatio, r.debtToEquityRatio,
               r.dividendYield, r.date,
            ROW_NUMBER() OVER (PARTITION BY r.symbol ORDER BY r.date DESC) AS rn
        FROM financial_ratios r
        JOIN profile p ON r.symbol = p.symbol
        WHERE r.period = 'FY'
          AND r.dividendPayoutRatio > 0
          AND r.dividendYield IS NOT NULL
          {exchange_filter}
    ),
    latest_cf AS (
        SELECT c.symbol, c.freeCashFlow, c.commonDividendsPaid, c.date,
            ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY c.date DESC) AS rn
        FROM cash_flow_statement c
        JOIN profile p ON c.symbol = p.symbol
        WHERE c.period = 'FY'
          AND c.commonDividendsPaid < 0
          {exchange_filter}
    ),
    latest_metrics AS (
        SELECT k.symbol, k.returnOnEquity, k.marketCap, k.date,
            ROW_NUMBER() OVER (PARTITION BY k.symbol ORDER BY k.date DESC) AS rn
        FROM key_metrics k
        JOIN profile p ON k.symbol = p.symbol
        WHERE k.period = 'FY'
          AND k.marketCap IS NOT NULL
          {exchange_filter}
    ),
    latest_scores AS (
        SELECT symbol, piotroskiScore
        FROM scores
    ),
    scored AS (
        SELECT r.symbol, r.date,
            ROUND(r.dividendPayoutRatio * 100, 1) AS payout_pct,
            ROUND(r.debtToEquityRatio, 2) AS debt_equity,
            ROUND(c.freeCashFlow / NULLIF(ABS(c.commonDividendsPaid), 0), 2) AS fcf_coverage,
            ROUND(k.returnOnEquity * 100, 1) AS roe_pct,
            s.piotroskiScore AS piotroski,
            ROUND(r.dividendYield * 100, 2) AS yield_pct,
            ROUND(k.marketCap / 1e9, 1) AS mktcap_bn,
            -- Component scores
            CASE WHEN r.dividendPayoutRatio < 0.5 THEN 2
                 WHEN r.dividendPayoutRatio < 0.8 THEN 1 ELSE 0 END AS c_payout,
            CASE WHEN r.debtToEquityRatio >= 0 AND r.debtToEquityRatio < 0.5 THEN 2
                 WHEN r.debtToEquityRatio >= 0 AND r.debtToEquityRatio < 1.5 THEN 1
                 ELSE 0 END AS c_debt,
            CASE WHEN c.freeCashFlow > 0 AND c.commonDividendsPaid < 0
                      AND c.freeCashFlow / ABS(c.commonDividendsPaid) > 2 THEN 2
                 WHEN c.freeCashFlow > 0 AND c.commonDividendsPaid < 0
                      AND c.freeCashFlow / ABS(c.commonDividendsPaid) > 1 THEN 1
                 ELSE 0 END AS c_fcf,
            CASE WHEN k.returnOnEquity > 0.15 THEN 2
                 WHEN k.returnOnEquity > 0.08 THEN 1 ELSE 0 END AS c_roe,
            CASE WHEN s.piotroskiScore >= 7 THEN 2
                 WHEN s.piotroskiScore >= 5 THEN 1 ELSE 0 END AS c_piotroski
        FROM latest_ratios r
        JOIN latest_cf c ON r.symbol = c.symbol AND c.rn = 1
        JOIN latest_metrics k ON r.symbol = k.symbol AND k.rn = 1
        LEFT JOIN latest_scores s ON r.symbol = s.symbol
        WHERE r.rn = 1
          AND r.dividendYield > {YIELD_MIN}
          AND k.marketCap > {mktcap_threshold}
    )
    SELECT symbol, date, payout_pct, debt_equity, fcf_coverage, roe_pct, piotroski,
           yield_pct, mktcap_bn,
           c_payout + c_debt + c_fcf + c_roe + COALESCE(c_piotroski, 0) AS sustainability_score,
           c_payout, c_debt, c_fcf, c_roe, COALESCE(c_piotroski, 0) AS c_piotroski
    FROM scored
    WHERE c_payout + c_debt + c_fcf + c_roe + COALESCE(c_piotroski, 0) >= {MIN_SCORE}
    ORDER BY sustainability_score DESC, yield_pct DESC
    LIMIT {MAX_RESULTS}
    """

    print(f"\n{'=' * 90}")
    print(f"  DIVIDEND SUSTAINABILITY SCREEN: {universe_name}")
    print(f"  Score >= {MIN_SCORE}/10, Yield > {YIELD_MIN*100:.0f}%, MCap > {mktcap_threshold/1e9:.0f}B local")
    print(f"{'=' * 90}")

    results = cr.query(sql, verbose=verbose, timeout=120)

    if not results:
        print("  No qualifying stocks found.")
        return []

    print(f"\n  {'Symbol':<10} {'Score':>6} {'Yield':>7} {'Payout':>8} {'D/E':>7} "
          f"{'FCFCov':>8} {'ROE':>7} {'Piotr':>6} {'MCap':>8} {'Date':>12}")
    print("  " + "-" * 90)
    for r in results:
        print(f"  {r['symbol']:<10} {r['sustainability_score']:>5}/10 "
              f"{r['yield_pct']:>6.1f}% {r['payout_pct']:>7.1f}% "
              f"{r['debt_equity']:>7.2f} {r['fcf_coverage']:>7.1f}x "
              f"{r['roe_pct']:>6.1f}% {r.get('piotroski', 'N/A'):>6} "
              f"{r['mktcap_bn']:>7.1f}B {r['date'][:10]:>12}")

    print(f"\n  {len(results)} stocks qualify (score >= {MIN_SCORE}/10)")
    return results


def main():
    parser = argparse.ArgumentParser(description="Dividend Sustainability Score screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_screen_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
        # Read this file and execute on cloud
        with open(__file__, "r") as f:
            code = f.read()
        result = cr.execute_code(code, verbose=True)
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, universe_name, verbose=args.verbose)


if __name__ == "__main__":
    main()
