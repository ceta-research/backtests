#!/usr/bin/env python3
"""
Low Debt Quality Screen (Live)

Screens for current qualifying stocks using TTM data.
Uses D/E < 0.5 + quality filters as proxy for Piotroski >= 7 on live data.

Note: Full Piotroski computation requires YoY FY comparisons. The live screen uses
TTM metrics as a proxy: positive ROE, OCF > 0, positive net income, improving margins.
For precise Piotroski scoring on live data, use the backtest with latest-period FY data.

Usage:
    python3 low-debt/screen.py
    python3 low-debt/screen.py --preset india
    python3 low-debt/screen.py --exchange XETRA
    python3 low-debt/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold, EXCHANGE_PRESETS

# Screen thresholds (TTM)
DE_MAX = 0.50         # D/E < 0.5
ROE_MIN = 0.08        # Return on equity > 8%
OPM_MIN = 0.08        # Operating profit margin > 8%
ICR_MIN = 5.0         # Interest coverage > 5x
DE_DISPLAY = 30       # Display top N stocks


def build_screen_sql(exchanges, mktcap_min):
    """Build TTM screening SQL."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
SELECT
    p.exchange,
    r.symbol,
    p.companyName,
    p.sector,
    ROUND(r.debtToEquityRatioTTM, 3) AS de_ratio,
    ROUND(k.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(r.operatingProfitMarginTTM * 100, 1) AS opm_pct,
    ROUND(r.interestCoverageRatioTTM, 1) AS interest_coverage,
    ROUND(k.freeCashFlowYieldTTM * 100, 2) AS fcf_yield_pct,
    ROUND(k.marketCap / 1e9, 2) AS mktcap_b
FROM financial_ratios_ttm r
JOIN key_metrics_ttm k ON r.symbol = k.symbol
JOIN profile p ON r.symbol = p.symbol
WHERE r.debtToEquityRatioTTM >= 0
  AND r.debtToEquityRatioTTM < {DE_MAX}
  AND k.returnOnEquityTTM > {ROE_MIN}
  AND r.operatingProfitMarginTTM > {OPM_MIN}
  AND r.interestCoverageRatioTTM > {ICR_MIN}
  AND p.isActivelyTrading = true
  AND k.marketCap > {mktcap_min}
  {exchange_clause}
ORDER BY de_ratio ASC
LIMIT {DE_DISPLAY}
"""


def run_screen(cr, exchanges, universe_name, verbose=False):
    """Run the live screen and print results."""
    mktcap_min = get_mktcap_threshold(exchanges)
    sql = build_screen_sql(exchanges, mktcap_min)

    if verbose:
        print(f"\nSQL:\n{sql}\n")

    print(f"Screening {universe_name} for Low Debt Quality stocks...")
    print(f"  Filters: D/E < {DE_MAX}, ROE > {ROE_MIN*100:.0f}%, OPM > {OPM_MIN*100:.0f}%,"
          f" ICR > {ICR_MIN}x, MCap > {mktcap_min/1e9:.0f}B local")
    print()

    rows = cr.query(sql, timeout=120, verbose=verbose)

    if not rows:
        print("No qualifying stocks found.")
        return

    print(f"{'#':<4} {'Symbol':<12} {'Company':<30} {'Sector':<22} "
          f"{'D/E':>6} {'ROE%':>7} {'OPM%':>7} {'ICR':>6} {'MktCap':>9}")
    print("-" * 110)

    for i, row in enumerate(rows, 1):
        name = (row.get('companyName') or '')[:29]
        sector = (row.get('sector') or '')[:21]
        print(f"{i:<4} {row.get('symbol',''):<12} {name:<30} {sector:<22} "
              f"{row.get('de_ratio', 0):>6.3f} "
              f"{row.get('roe_pct', 0):>6.1f}% "
              f"{row.get('opm_pct', 0):>6.1f}% "
              f"{row.get('interest_coverage', 0):>6.1f} "
              f"{row.get('mktcap_b', 0):>8.2f}B")

    print(f"\nTotal: {len(rows)} qualifying stocks")
    print("\nNote: Live screen uses TTM quality filters as Piotroski proxy.")
    print("      Run backtest.py for full Piotroski scoring with FY point-in-time data.")


def main():
    parser = argparse.ArgumentParser(description="Low Debt Quality screen (live TTM)")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)

    if args.cloud:
        mktcap_min = get_mktcap_threshold(exchanges)
        sql = build_screen_sql(exchanges, mktcap_min).strip()

        from cr_client import CetaResearch as _CR
        cr = _CR(api_key=args.api_key, base_url=args.base_url)
        result = cr.execute_code(
            f"""
import sys
sys.path.insert(0, '.')
from cr_client import CetaResearch
cr = CetaResearch()
rows = cr.query('''{sql}''', timeout=120)
print(f'Found {{len(rows)}} qualifying stocks')
for r in rows[:20]:
    print(f"{{r.get('symbol',''):<12}} D/E={{r.get('de_ratio',0):.3f}} ROE={{r.get('roe_pct',0):.1f}%")
""",
            verbose=True
        )
        print(result.get("stdout", ""))
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, universe_name, verbose=args.verbose)


if __name__ == "__main__":
    main()
