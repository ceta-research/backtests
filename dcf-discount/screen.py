#!/usr/bin/env python3
"""
DCF Discount Live Screen

Shows stocks currently trading below their intrinsic value (computed DCF).
Uses Gordon Growth Model: DCF = FCF * 13.67 * price / marketCap
20% discount ≡ FCF/MarketCap >= 8.78%

Usage:
    python3 dcf-discount/screen.py                  # US default (top 30)
    python3 dcf-discount/screen.py --preset india   # India
    python3 dcf-discount/screen.py --cloud          # Run on cloud
    python3 dcf-discount/screen.py --preset us --quality   # Add quality filters
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges

# Gordon Growth Model
GROWTH_RATE = 0.025
DISCOUNT_RATE = 0.10
DCF_MULTIPLE = (1 + GROWTH_RATE) / (DISCOUNT_RATE - GROWTH_RATE)  # 13.67
DISCOUNT_THRESHOLD = 0.20
FCF_YIELD_MIN = (1 + DISCOUNT_THRESHOLD) / DCF_MULTIPLE  # ~8.78%

MKTCAP_MIN = 1_000_000_000
TOP_N = 30


def run_screen(cr, exchanges, quality=False, verbose=False):
    """Run live DCF discount screen. Returns list of matching stocks."""
    if exchanges:
        ex_filter = ", ".join(f"'{e}'" for e in exchanges)
        exchange_where = f"AND p.exchange IN ({ex_filter})"
    else:
        exchange_where = ""

    quality_join = ""
    quality_where = ""
    quality_cols = ""
    if quality:
        quality_join = """
        JOIN financial_ratios fr ON cfs.symbol = fr.symbol AND fr.period = 'FY'"""
        quality_where = """
          AND fr.returnOnEquity > 0.10
          AND fr.debtToEquityRatio < 1.5"""
        quality_cols = """
        ROUND(fr.returnOnEquity * 100, 1) AS roe_pct,
        ROUND(fr.debtToEquityRatio, 2) AS debt_to_equity,"""

    sql = f"""
    WITH latest_fcf AS (
        SELECT symbol, freeCashFlow,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM cash_flow_statement
        WHERE period = 'FY' AND freeCashFlow > 0
    ),
    latest_km AS (
        SELECT symbol, marketCap,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY dateEpoch DESC) AS rn
        FROM key_metrics
        WHERE period = 'FY' AND marketCap > {MKTCAP_MIN}
    )
    SELECT
        cfs.symbol,
        p.companyName,
        p.exchange,
        p.sector,
        ROUND(cfs.freeCashFlow / 1e6, 0) AS fcf_mm,
        ROUND(km.marketCap / 1e9, 1) AS mktcap_bn,
        ROUND(cfs.freeCashFlow / km.marketCap * 100, 2) AS fcf_yield_pct,
        ROUND((1 - km.marketCap / (cfs.freeCashFlow * {DCF_MULTIPLE:.2f})) * 100, 1) AS discount_pct,
        {quality_cols}
        ROUND(km.marketCap / 1e9, 1) AS mktcap_bn_check
    FROM latest_fcf cfs
    JOIN latest_km km ON cfs.symbol = km.symbol AND km.rn = 1
    JOIN profile p ON cfs.symbol = p.symbol
    {quality_join}
    WHERE cfs.rn = 1
      AND cfs.freeCashFlow / km.marketCap >= {FCF_YIELD_MIN:.6f}
      {exchange_where}
      {quality_where}
    ORDER BY fcf_yield_pct DESC
    LIMIT {TOP_N}
    """

    if verbose:
        print(f"SQL:\n{sql}\n")

    return cr.query(sql, verbose=verbose)


def main():
    parser = argparse.ArgumentParser(description="DCF Discount live stock screen")
    add_common_args(parser)
    parser.add_argument("--quality", action="store_true",
                        help="Add quality filters: ROE > 10%, D/E < 1.5")
    parser.add_argument("--cloud", action="store_true",
                        help="Run on Ceta Research cloud compute")
    args = parser.parse_args()

    if args.cloud:
        from cloud_runner import run_backtest_cloud
        cloud_args = [a for a in sys.argv[1:] if a != "--cloud"]
        result = run_backtest_cloud("dcf-discount/screen", args_str=" ".join(cloud_args),
                                    api_key=args.api_key, base_url=args.base_url,
                                    verbose=True)
        print(result.get("stdout", ""))
        return

    exchanges, universe_name = resolve_exchanges(args)
    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    quality_label = " (quality-filtered)" if args.quality else ""
    print(f"\nDCF Discount Screen: {universe_name}{quality_label}")
    print(f"Signal: FCF yield >= {FCF_YIELD_MIN*100:.2f}% (= {DISCOUNT_THRESHOLD*100:.0f}% DCF discount)")
    print(f"Universe: MCap >= ${MKTCAP_MIN/1e9:.0f}B")
    print(f"Gordon Growth Model: g={GROWTH_RATE*100:.1f}%, r={DISCOUNT_RATE*100:.0f}%, "
          f"multiple={DCF_MULTIPLE:.2f}x\n")

    rows = run_screen(cr, exchanges, quality=args.quality, verbose=args.verbose)

    if not rows:
        print("No results returned.")
        return

    # Display results
    header = f"{'Symbol':<12} {'Company':<30} {'Exch':<6} {'FCF_Yld':>8} {'Disc%':>7} {'MCap_Bn':>8}"
    if args.quality:
        header += f" {'ROE%':>6} {'D/E':>6}"
    print(header)
    print("-" * len(header))

    for r in rows:
        line = (f"{r.get('symbol',''):<12} {str(r.get('companyName',''))[:30]:<30} "
                f"{r.get('exchange',''):<6} {r.get('fcf_yield_pct',0):>7.2f}% "
                f"{r.get('discount_pct',0):>6.1f}% {r.get('mktcap_bn',0):>8.1f}B")
        if args.quality:
            line += f" {r.get('roe_pct',0):>5.1f}% {r.get('debt_to_equity',0):>6.2f}"
        print(line)

    print(f"\n{len(rows)} stocks returned. Data: Ceta Research (FMP financial data warehouse).")


if __name__ == "__main__":
    main()
