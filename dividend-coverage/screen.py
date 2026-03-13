#!/usr/bin/env python3
"""
Dividend Coverage Screen

Current stock screen for the dividend coverage strategy.
Returns stocks with strong FCF coverage of dividend payments.

Signal: FCF / ABS(commonDividendsPaid) between 1.5x and 20x
Filters: dividendYield > 2%, marketCap > exchange threshold

Usage:
    python3 dividend-coverage/screen.py
    python3 dividend-coverage/screen.py --preset india
    python3 dividend-coverage/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

COVERAGE_MIN = 1.5
COVERAGE_MAX = 20.0
YIELD_MIN = 0.02
MAX_RESULTS = 50


def run_screen(cr, exchanges, universe_name, verbose=False):
    """Run dividend coverage screen. Returns list of qualifying stocks."""
    mktcap_threshold = get_mktcap_threshold(exchanges)

    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_filter = ""

    sql = f"""
    WITH latest_cf AS (
        SELECT c.symbol, c.freeCashFlow, c.commonDividendsPaid, c.date,
            ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY c.date DESC) AS rn
        FROM cash_flow_statement c
        JOIN profile p ON c.symbol = p.symbol
        WHERE c.period = 'FY'
          AND c.commonDividendsPaid < 0
          AND c.freeCashFlow IS NOT NULL
          AND c.freeCashFlow > 0
          {exchange_filter}
    ),
    latest_ratios AS (
        SELECT symbol, dividendYield, date,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM financial_ratios
        WHERE period = 'FY' AND dividendYield IS NOT NULL
    ),
    latest_metrics AS (
        SELECT symbol, marketCap, date,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM key_metrics
        WHERE period = 'FY' AND marketCap IS NOT NULL
    )
    SELECT
        cf.symbol,
        p.exchange,
        p.companyName,
        ROUND(cf.freeCashFlow / NULLIF(ABS(cf.commonDividendsPaid), 0), 2) AS coverage,
        ROUND(r.dividendYield * 100, 2) AS yield_pct,
        ROUND(cf.freeCashFlow / 1e9, 2) AS fcf_bn,
        ROUND(ABS(cf.commonDividendsPaid) / 1e9, 2) AS div_paid_bn,
        ROUND(km.marketCap / 1e9, 1) AS mktcap_bn,
        cf.date AS filing_date
    FROM latest_cf cf
    JOIN latest_ratios r ON cf.symbol = r.symbol AND r.rn = 1
    JOIN latest_metrics km ON cf.symbol = km.symbol AND km.rn = 1
    JOIN profile p ON cf.symbol = p.symbol
    WHERE cf.rn = 1
      AND cf.freeCashFlow / NULLIF(ABS(cf.commonDividendsPaid), 0) >= {COVERAGE_MIN}
      AND cf.freeCashFlow / NULLIF(ABS(cf.commonDividendsPaid), 0) <= {COVERAGE_MAX}
      AND r.dividendYield > {YIELD_MIN}
      AND km.marketCap > {mktcap_threshold}
    ORDER BY coverage DESC
    LIMIT {MAX_RESULTS}
    """

    print(f"Screening {universe_name} for dividend coverage stocks...")
    print(f"  Coverage: {COVERAGE_MIN}-{COVERAGE_MAX}x, Yield > {YIELD_MIN*100:.0f}%")
    print(f"  Market cap > {mktcap_threshold/1e9:.0f}B local currency\n")

    results = cr.query(sql, verbose=verbose, timeout=120)

    if not results:
        print("No qualifying stocks found.")
        return []

    print(f"{'Symbol':<10} {'Exchange':<8} {'Coverage':>10} {'Yield%':>8} "
          f"{'FCF($B)':>8} {'DivPd($B)':>10} {'MCap($B)':>10} {'Filed':>12}")
    print("-" * 86)
    for r in results:
        print(f"{r['symbol']:<10} {r['exchange']:<8} {r['coverage']:>10.1f}x "
              f"{r['yield_pct']:>7.1f}% {r['fcf_bn']:>8.1f} {r['div_paid_bn']:>10.1f} "
              f"{r['mktcap_bn']:>10.1f} {r['filing_date']:>12}")

    print(f"\n{len(results)} stocks qualify")
    return results


def main():
    parser = argparse.ArgumentParser(description="Dividend Coverage screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run via Code Execution API")
    args = parser.parse_args()

    if args.cloud:
        from cr_client import CetaResearch as CR
        cr = CR(api_key=args.api_key, base_url=args.base_url)
        with open(__file__) as f:
            code = f.read()
        result = cr.execute_code(code, args=["--preset", args.preset or "us"])
        print(result.get("stdout", ""))
        if result.get("stderr"):
            print(result["stderr"], file=sys.stderr)
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, universe_name, verbose=args.verbose)


if __name__ == "__main__":
    main()
