#!/usr/bin/env python3
"""Z-Score Pairs Trading: Current Signal Screen

Finds same-sector cointegrated pairs with active z-score signals using the
most recent 252 trading days for formation and the most recent 40 days for
z-score computation.

Usage:
    # Screen US stocks (default)
    python3 pairs-zscore/screen.py

    # Screen Japanese stocks
    python3 pairs-zscore/screen.py --preset japan

    # Screen German stocks
    python3 pairs-zscore/screen.py --exchange XETRA

    # Screen all exchanges
    python3 pairs-zscore/screen.py --global
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# ── Exchange-specific market cap thresholds (local currency) ──────────────────
MKTCAP_DEFAULTS = {
    "NYSE": 1_000_000_000, "NASDAQ": 1_000_000_000, "AMEX": 1_000_000_000,
    "JPX":  10_000_000_000,
    "BSE":  20_000_000_000, "NSE": 20_000_000_000,
    "LSE":  500_000_000,
    "XETRA": 500_000_000,
    "SIX":  500_000_000,
    "STO":  5_000_000_000,
    "TSX":  500_000_000,
    "SHZ":  2_000_000_000, "SHH": 2_000_000_000,
    "HKSE": 2_000_000_000,
    "KSC":  500_000_000_000,
    "TAI":  10_000_000_000, "TWO": 10_000_000_000,
    "JNB":  10_000_000_000,
}

PAIRS_ZSCORE_SCREEN_SQL = """
WITH sector_map AS (
    SELECT DISTINCT symbol, sector
    FROM profile
    WHERE sector IS NOT NULL AND sector != ''
      AND isActivelyTrading = true
      {exchange_filter}
),
recent_mktcap AS (
    SELECT km.symbol, km.marketCap,
        ROW_NUMBER() OVER (PARTITION BY km.symbol ORDER BY km.dateEpoch DESC) AS rn
    FROM key_metrics km
    JOIN sector_map sm ON km.symbol = sm.symbol
    WHERE km.period = 'FY' AND km.marketCap IS NOT NULL AND km.marketCap > {mktcap_threshold}
),
large_caps AS (
    SELECT rm.symbol, sm.sector
    FROM recent_mktcap rm
    JOIN sector_map sm ON rm.symbol = sm.symbol
    WHERE rm.rn = 1
),
-- 252-day daily returns for correlation
daily_ret AS (
    SELECT eod.symbol,
           CAST(eod.date AS DATE) AS trade_date,
           (eod.adjClose - LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date))
               / NULLIF(LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date), 0) AS ret
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE eod.date >= (CURRENT_DATE - INTERVAL '370 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
-- Correlation pairs (same sector, corr >= 0.70, >= 200 common days)
pair_corr AS (
    SELECT
        a.symbol AS sym_a, b.symbol AS sym_b,
        la.sector,
        ROUND(CORR(a.ret, b.ret), 4) AS correlation,
        COUNT(*) AS common_days
    FROM daily_ret a
    JOIN daily_ret b
        ON a.trade_date = b.trade_date AND a.symbol < b.symbol
    JOIN large_caps la ON a.symbol = la.symbol
    JOIN large_caps lb ON b.symbol = lb.symbol
    WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL
      AND la.sector = lb.sector
    GROUP BY a.symbol, b.symbol, la.sector
    HAVING COUNT(*) >= 200
),
-- Log-price spread for OLS beta
log_prices AS (
    SELECT eod.symbol,
           CAST(eod.date AS DATE) AS trade_date,
           LN(eod.adjClose) AS lp,
           eod.adjClose
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE eod.date >= (CURRENT_DATE - INTERVAL '100 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
-- OLS beta + current z-score for top correlated pairs
pair_params AS (
    SELECT
        pc.sym_a, pc.sym_b, pc.sector, pc.correlation,
        CORR(la.lp, lb.lp) * STDDEV(la.lp) / NULLIF(STDDEV(lb.lp), 0) AS beta,
        COUNT(*) AS price_overlap_days
    FROM pair_corr pc
    JOIN log_prices la ON pc.sym_a = la.symbol
    JOIN log_prices lb ON pc.sym_b = lb.symbol
      AND la.trade_date = lb.trade_date
    WHERE pc.correlation >= 0.70
    GROUP BY pc.sym_a, pc.sym_b, pc.sector, pc.correlation
    HAVING COUNT(*) >= 30
    ORDER BY pc.correlation DESC
    LIMIT 100
),
-- 40-day rolling z-score for each pair (most recent date only)
spread_z AS (
    SELECT
        pp.sym_a, pp.sym_b, pp.sector, pp.correlation, pp.beta,
        (la.lp - pp.beta * lb.lp) AS spread_today,
        AVG(la.lp - pp.beta * lb.lp)
            OVER (PARTITION BY pp.sym_a, pp.sym_b
                  ORDER BY la.trade_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
            AS spread_mean,
        STDDEV(la.lp - pp.beta * lb.lp)
            OVER (PARTITION BY pp.sym_a, pp.sym_b
                  ORDER BY la.trade_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
            AS spread_std,
        la.trade_date,
        ROW_NUMBER() OVER (PARTITION BY pp.sym_a, pp.sym_b ORDER BY la.trade_date DESC) AS rn
    FROM pair_params pp
    JOIN log_prices la ON pp.sym_a = la.symbol
    JOIN log_prices lb ON pp.sym_b = lb.symbol AND la.trade_date = lb.trade_date
)
SELECT
    sym_a, sym_b, sector,
    ROUND(correlation, 3) AS corr,
    ROUND(beta, 3) AS beta,
    ROUND(spread_today, 4) AS spread,
    ROUND(spread_mean, 4) AS spread_mean,
    ROUND(spread_std, 4) AS spread_std,
    ROUND((spread_today - spread_mean) / NULLIF(spread_std, 0), 3) AS z_score,
    trade_date AS last_date
FROM spread_z
WHERE rn = 1
  AND spread_std > 0
  AND ABS((spread_today - spread_mean) / NULLIF(spread_std, 0)) >= 0.5
ORDER BY ABS((spread_today - spread_mean) / NULLIF(spread_std, 0)) DESC
LIMIT {top_n}
"""


def get_mktcap(exchanges):
    if not exchanges:
        return 1_000_000_000
    return min(MKTCAP_DEFAULTS.get(ex, 1_000_000_000) for ex in exchanges)


def main():
    parser = argparse.ArgumentParser(description="Z-Score Pairs Trading Screen")
    add_common_args(parser)
    parser.add_argument("--min-corr", type=float, default=0.70,
                        help="Minimum correlation threshold (default: 0.70)")
    parser.add_argument("--top-n", type=int, default=25,
                        help="Number of top pairs to return (default: 25)")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)

    if exchanges:
        quoted = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND exchange IN ({quoted})"
        mktcap = get_mktcap(exchanges)
    else:
        exchange_filter = ""
        mktcap = 1_000_000_000

    sql = PAIRS_ZSCORE_SCREEN_SQL.format(
        exchange_filter=exchange_filter,
        mktcap_threshold=mktcap,
        min_corr=args.min_corr,
        top_n=args.top_n,
    )

    cr = CetaResearch(api_key=args.api_key)

    print(f"Z-Score Pairs Screen: {universe_name}")
    print(f"Lookback: 252d formation, 40d z-score | Corr >= {args.min_corr}")
    print(f"MCap > {mktcap:,} | Showing pairs with |z| >= 0.5")
    print()

    results = cr.query(sql, verbose=True)
    if not results:
        print("No pairs found.")
        return

    # Bucket by signal strength
    strong  = [r for r in results if abs(float(r["z_score"])) >= 2.0]
    moderate = [r for r in results if 1.0 <= abs(float(r["z_score"])) < 2.0]
    weak    = [r for r in results if abs(float(r["z_score"])) < 1.0]

    print(f"{'Rank':<5} {'Sym A':<8} {'Sym B':<8} {'Sector':<28} "
          f"{'Corr':>6} {'Beta':>6} {'Z-Score':>8} {'Signal'}")
    print("-" * 85)

    for i, row in enumerate(results, 1):
        z = float(row["z_score"])
        sig = ""
        if z < -2.0:
            sig = "LONG A / SHORT B"
        elif z > 2.0:
            sig = "SHORT A / LONG B"
        elif z < -1.0:
            sig = "watch (weak long)"
        elif z > 1.0:
            sig = "watch (weak short)"
        print(f"{i:<5} {row['sym_a']:<8} {row['sym_b']:<8} "
              f"{str(row['sector'])[:28]:<28} "
              f"{float(row['corr']):>6.3f} {float(row['beta']):>6.3f} "
              f"{z:>+8.3f}  {sig}")

    print(f"\nTotal: {len(results)} pairs  |  "
          f"Actionable (|z|>2.0): {len(strong)}  |  "
          f"Watching (1<|z|<2): {len(moderate)}  |  "
          f"Quiet (|z|<1): {len(weak)}")
    print(f"\nUniverse: {universe_name}")
    print(f"Data: Ceta Research (FMP warehouse, formation period: last 252 days)")
    print(f"Entry signal: |z| > 2.0 | Exit signal: |z| < 0.5")


if __name__ == "__main__":
    main()
