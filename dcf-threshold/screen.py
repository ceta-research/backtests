#!/usr/bin/env python3
"""
DCF Threshold Live Screen

Shows stocks currently passing the DCF Threshold signal:
  - FCF/MarketCap >= 8.78% (= 20% discount to Gordon Growth DCF intrinsic value)
  - Operating Cash Flow > 0
  - ROE > 8%
  - D/E < 1.5 (or null)

Usage:
    python3 dcf-threshold/screen.py                   # US default (top 30)
    python3 dcf-threshold/screen.py --preset india    # India
    python3 dcf-threshold/screen.py --preset germany  # Germany
    python3 dcf-threshold/screen.py --cloud           # Run on cloud
    python3 dcf-threshold/screen.py --json            # JSON output
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Gordon Growth Model
GROWTH_RATE = 0.025
DISCOUNT_RATE = 0.10
DCF_MULTIPLE = (1 + GROWTH_RATE) / (DISCOUNT_RATE - GROWTH_RATE)  # 13.67
DISCOUNT_THRESHOLD = 0.20
FCF_YIELD_MIN = (1 + DISCOUNT_THRESHOLD) / DCF_MULTIPLE  # ~8.78%

ROE_MIN = 0.08
DE_MAX = 1.5
TOP_N = 30


def run_screen(cr, exchanges, mktcap_min, verbose=False):
    """Run live DCF threshold screen. Returns list of matching stocks."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    sql = f"""
    WITH latest_cf AS (
        SELECT symbol, freeCashFlow, operatingCashFlow,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM cash_flow_statement
        WHERE period = 'FY' AND freeCashFlow > 0 AND operatingCashFlow > 0
    ),
    latest_km AS (
        SELECT symbol, marketCap, returnOnEquity,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM key_metrics
        WHERE period = 'FY' AND marketCap > {mktcap_min}
    ),
    latest_ra AS (
        SELECT symbol, debtToEquityRatio,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM financial_ratios
        WHERE period = 'FY'
    )
    SELECT
        cf.symbol,
        p.companyName,
        p.exchange,
        p.sector,
        ROUND(cf.freeCashFlow / 1e6, 0)              AS fcf_mm,
        ROUND(km.marketCap / 1e9, 2)                 AS mktcap_bn,
        ROUND(cf.freeCashFlow / km.marketCap * 100, 2) AS fcf_yield_pct,
        ROUND((1 - km.marketCap / (cf.freeCashFlow * {DCF_MULTIPLE:.2f})) * 100, 1) AS discount_pct,
        ROUND(km.returnOnEquity * 100, 1)            AS roe_pct,
        ROUND(COALESCE(ra.debtToEquityRatio, -1), 2) AS debt_to_equity
    FROM latest_cf cf
    JOIN latest_km km ON cf.symbol = km.symbol AND km.rn = 1
    JOIN profile p ON cf.symbol = p.symbol
    LEFT JOIN latest_ra ra ON cf.symbol = ra.symbol AND ra.rn = 1
    WHERE cf.rn = 1
      AND cf.freeCashFlow / km.marketCap >= {FCF_YIELD_MIN:.6f}
      AND km.returnOnEquity >= {ROE_MIN}
      AND (ra.debtToEquityRatio IS NULL
           OR (ra.debtToEquityRatio >= 0 AND ra.debtToEquityRatio < {DE_MAX}))
      {exchange_where}
    ORDER BY fcf_yield_pct DESC
    LIMIT {TOP_N}
    """

    if verbose:
        print(f"SQL:\n{sql}\n")

    return cr.query(sql, verbose=verbose)


def main():
    parser = argparse.ArgumentParser(description="DCF Threshold live stock screen")
    add_common_args(parser)
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a not in ("--cloud",)]
        result = run_backtest_cloud("dcf-threshold/screen", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    mktcap_threshold = get_mktcap_threshold(exchanges)
    mktcap_label = (f"{mktcap_threshold/1e9:.0f}B"
                    if mktcap_threshold >= 1e9 else f"{mktcap_threshold/1e6:.0f}M")
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    print(f"\nDCF Threshold Screen: {universe_name}")
    print(f"Signal: FCF yield >= {FCF_YIELD_MIN*100:.2f}% (= {DISCOUNT_THRESHOLD*100:.0f}% DCF discount)")
    print(f"Quality: ROE > {ROE_MIN*100:.0f}%, D/E < {DE_MAX}, OCF > 0")
    print(f"Universe: MCap >= {mktcap_label} local")
    print(f"Gordon Growth Model: g={GROWTH_RATE*100:.1f}%, r={DISCOUNT_RATE*100:.0f}%, "
          f"multiple={DCF_MULTIPLE:.2f}x\n")

    rows = run_screen(cr, exchanges, mktcap_threshold, verbose=args.verbose)

    if not rows:
        print("No results returned.")
        return

    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return

    header = (f"{'Symbol':<12} {'Company':<30} {'Exch':<6} "
              f"{'FCF_Yld':>8} {'Disc%':>7} {'ROE%':>6} {'D/E':>6} {'MCap_Bn':>8}")
    print(header)
    print("-" * len(header))

    for r in rows:
        de = r.get("debt_to_equity", -1)
        de_str = f"{de:.2f}" if de >= 0 else "N/A"
        print(
            f"{r.get('symbol',''):<12} "
            f"{str(r.get('companyName',''))[:30]:<30} "
            f"{r.get('exchange',''):<6} "
            f"{r.get('fcf_yield_pct',0):>7.2f}% "
            f"{r.get('discount_pct',0):>6.1f}% "
            f"{r.get('roe_pct',0):>5.1f}% "
            f"{de_str:>6} "
            f"{r.get('mktcap_bn',0):>8.2f}B"
        )

    print(f"\n{len(rows)} stocks returned. Data: Ceta Research (FMP financial data warehouse).")


if __name__ == "__main__":
    main()
