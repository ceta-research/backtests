#!/usr/bin/env python3
"""
Revenue Acceleration Growth Screen (Live)

Screens for current qualifying stocks with accelerating revenue growth.
Uses TTM revenue data to compute growth acceleration + quality filters.

Signal (live):
  - Revenue growth acceleration: TTM growth > prior-year TTM growth (approximated)
  - Current growth > 5%
  - ROE > 10%, D/E < 1.5, isActivelyTrading = true
  - Market cap > exchange-specific threshold

Note: Full acceleration requires 3 consecutive annual filings. This live screen
uses FY data directly from the warehouse for the most recent period.

Usage:
    python3 revenue-accel/screen.py
    python3 revenue-accel/screen.py --preset india
    python3 revenue-accel/screen.py --exchange XETRA
    python3 revenue-accel/screen.py --cloud
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Screen thresholds
REV_GROWTH_MIN = 0.05   # Current growth > 5%
ROE_MIN = 0.10          # ROE > 10%
DE_MAX = 1.5            # D/E < 1.5
DISPLAY_TOP = 30        # Display top N by acceleration


def build_screen_sql(exchanges, mktcap_min):
    """Build live screening SQL using FY income statement data."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_clause = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_clause = ""

    return f"""
WITH
-- 3 most recent FY revenue filings per symbol
inc AS (
    SELECT symbol, revenue, dateEpoch as filing_epoch,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
    FROM income_statement
    WHERE period = 'FY' AND revenue IS NOT NULL AND revenue > 0
),
-- Compute growth rates
rev_calc AS (
    SELECT r1.symbol,
        (r1.revenue - r2.revenue) / NULLIF(r2.revenue, 0) AS growth_current,
        (r2.revenue - r3.revenue) / NULLIF(r3.revenue, 0) AS growth_prior,
        (r1.revenue - r2.revenue) / NULLIF(r2.revenue, 0)
          - (r2.revenue - r3.revenue) / NULLIF(r3.revenue, 0) AS acceleration,
        r1.revenue AS rev_latest
    FROM inc r1
    JOIN inc r2 ON r1.symbol = r2.symbol AND r2.rn = 2
    JOIN inc r3 ON r1.symbol = r3.symbol AND r3.rn = 3
    WHERE r1.rn = 1
),
-- Quality metrics (TTM)
qual AS (
    SELECT k.symbol,
        k.returnOnEquityTTM,
        k.marketCap,
        r.debtToEquityRatioTTM
    FROM key_metrics_ttm k
    JOIN financial_ratios_ttm r ON k.symbol = r.symbol
)
SELECT
    p.exchange,
    rc.symbol,
    p.companyName,
    p.sector,
    ROUND(rc.growth_current * 100, 1) AS growth_pct,
    ROUND(rc.growth_prior * 100, 1) AS prior_growth_pct,
    ROUND(rc.acceleration * 100, 1) AS acceleration_ppt,
    ROUND(q.returnOnEquityTTM * 100, 1) AS roe_pct,
    ROUND(q.debtToEquityRatioTTM, 2) AS de_ratio,
    ROUND(q.marketCap / 1e9, 2) AS mktcap_b
FROM rev_calc rc
JOIN qual q ON rc.symbol = q.symbol
JOIN profile p ON rc.symbol = p.symbol
WHERE rc.growth_current > rc.growth_prior
  AND rc.growth_current > {REV_GROWTH_MIN}
  AND q.returnOnEquityTTM > {ROE_MIN}
  AND q.debtToEquityRatioTTM >= 0
  AND q.debtToEquityRatioTTM < {DE_MAX}
  AND q.marketCap > {mktcap_min}
  AND p.isActivelyTrading = true
  {exchange_clause}
ORDER BY rc.acceleration DESC
LIMIT {DISPLAY_TOP}
"""


def run_screen(cr, exchanges, universe_name, verbose=False):
    """Run the live screen and print results."""
    mktcap_min = get_mktcap_threshold(exchanges)
    sql = build_screen_sql(exchanges, mktcap_min)

    if verbose:
        print(f"\nSQL:\n{sql}\n")

    print(f"Screening {universe_name} for Revenue Acceleration stocks...")
    print(f"  Filters: Rev growth accel > 0, current growth > {REV_GROWTH_MIN*100:.0f}%, "
          f"ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, MCap > {mktcap_min/1e9:.0f}B local")
    print()

    rows = cr.query(sql, timeout=180, verbose=verbose)

    if not rows:
        print("No qualifying stocks found.")
        return

    print(f"{'#':<4} {'Symbol':<12} {'Company':<28} {'Sector':<22} "
          f"{'Cur%':>7} {'Prv%':>7} {'Accel':>7} {'ROE%':>7} {'D/E':>6} {'MCap':>9}")
    print("-" * 115)

    for i, row in enumerate(rows, 1):
        name = (row.get('companyName') or '')[:27]
        sector = (row.get('sector') or '')[:21]
        print(f"{i:<4} {row.get('symbol',''):<12} {name:<28} {sector:<22} "
              f"{row.get('growth_pct', 0):>6.1f}% "
              f"{row.get('prior_growth_pct', 0):>6.1f}% "
              f"{row.get('acceleration_ppt', 0):>+6.1f}pp "
              f"{row.get('roe_pct', 0):>6.1f}% "
              f"{row.get('de_ratio', 0):>6.2f} "
              f"{row.get('mktcap_b', 0):>8.2f}B")

    print(f"\nTotal: {len(rows)} qualifying stocks (top {DISPLAY_TOP} by acceleration)")
    print("\nNote: Acceleration = (current YoY growth) - (prior YoY growth), in percentage points.")
    print("      Run backtest.py for full historical analysis with point-in-time data.")


def main():
    parser = argparse.ArgumentParser(description="Revenue Acceleration screen (live)")
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
rows = cr.query('''{sql}''', timeout=180)
print(f'Found {{len(rows)}} qualifying stocks')
for r in rows[:20]:
    print(f"{{r.get('symbol',''):<12}} accel={{r.get('acceleration_ppt',0):+.1f}}pp "
          f"growth={{r.get('growth_pct',0):.1f}%")
""",
            verbose=True
        )
        print(result.get("stdout", ""))
        return

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)
    run_screen(cr, exchanges, universe_name, verbose=args.verbose)


if __name__ == "__main__":
    main()
