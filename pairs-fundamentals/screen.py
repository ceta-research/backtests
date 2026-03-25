#!/usr/bin/env python3
"""Pairs trading screen on current data.

Finds the top correlated same-sector pairs among large-cap stocks using
the most recent 252 trading days of daily returns.

Usage:
    # Screen US stocks (default)
    python3 pairs-fundamentals/screen.py

    # Screen Japanese stocks
    python3 pairs-fundamentals/screen.py --preset japan

    # Screen German stocks
    python3 pairs-fundamentals/screen.py --exchange XETRA

    # Screen all exchanges (global)
    python3 pairs-fundamentals/screen.py --global

See README.md for data source setup.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

PAIRS_SCREEN_SQL = """
WITH sector_map AS (
    SELECT DISTINCT symbol, sector
    FROM profile
    WHERE sector IS NOT NULL
      AND isActivelyTrading = true
      {exchange_filter}
),
recent_mktcap AS (
    SELECT km.symbol, km.marketCap,
        ROW_NUMBER() OVER (PARTITION BY km.symbol ORDER BY km.dateEpoch DESC) AS rn
    FROM key_metrics km
    JOIN sector_map sm ON km.symbol = sm.symbol
    WHERE km.period = 'FY' AND km.marketCap IS NOT NULL
),
large_caps AS (
    SELECT rm.symbol, sm.sector
    FROM recent_mktcap rm
    JOIN sector_map sm ON rm.symbol = sm.symbol
    WHERE rm.rn = 1 AND rm.marketCap > {mktcap_threshold}
),
daily_ret AS (
    SELECT
        eod.symbol,
        CAST(eod.date AS DATE) AS trade_date,
        (eod.adjClose - LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date))
            / NULLIF(LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date), 0) AS ret
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE eod.date >= (CURRENT_DATE - INTERVAL '365 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
pair_corr AS (
    SELECT
        a.symbol AS symbol_a,
        b.symbol AS symbol_b,
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
)
SELECT
    symbol_a,
    symbol_b,
    sector,
    correlation,
    common_days
FROM pair_corr
WHERE correlation >= {min_corr}
ORDER BY correlation DESC
LIMIT {top_n}
"""

# Exchange preset defaults
EXCHANGE_PRESETS = {
    "us":        (["NYSE", "NASDAQ", "AMEX"], 1_000_000_000),
    "japan":     (["JPX"], 10_000_000_000),
    "india":     (["NSE"], 20_000_000_000),
    "germany":   (["XETRA"], 500_000_000),
    "uk":        (["LSE"], 500_000_000),
    "china":     (["SHZ", "SHH"], 2_000_000_000),
    "hongkong":  (["HKSE"], 2_000_000_000),
    "korea":     (["KSC"], 500_000_000_000),
    "taiwan":    (["TAI", "TWO"], 10_000_000_000),
    "canada":    (["TSX"], 1_000_000_000),
    "sweden":    (["STO"], 1_000_000_000),
}


def main():
    parser = argparse.ArgumentParser(description="Pairs trading correlation screen")
    parser.add_argument("--exchange", type=str,
                        help="Exchange(s), comma-separated (e.g., NYSE,NASDAQ,AMEX)")
    parser.add_argument("--preset", type=str, choices=EXCHANGE_PRESETS.keys(),
                        help="Use a preset exchange group")
    parser.add_argument("--global", dest="global_screen", action="store_true",
                        help="Screen all exchanges (no filter)")
    parser.add_argument("--min-corr", type=float, default=0.70,
                        help="Minimum correlation threshold (default: 0.70)")
    parser.add_argument("--top-n", type=int, default=20,
                        help="Number of top pairs to return (default: 20)")
    parser.add_argument("--mktcap", type=int, default=None,
                        help="Market cap threshold in local currency units (default: per preset)")
    parser.add_argument("--api-key", type=str, help="API key (or set CR_API_KEY env var)")
    args = parser.parse_args()

    # Determine exchanges and market cap threshold
    if args.global_screen:
        exchanges = None
        mktcap = args.mktcap or 1_000_000_000
        label = "Global (all exchanges)"
    elif args.preset:
        preset_exchanges, preset_mktcap = EXCHANGE_PRESETS[args.preset]
        exchanges = preset_exchanges
        mktcap = args.mktcap or preset_mktcap
        label = f"{args.preset.title()} ({', '.join(exchanges)})"
    elif args.exchange:
        exchanges = [e.strip().upper() for e in args.exchange.split(",")]
        mktcap = args.mktcap or 1_000_000_000
        label = ", ".join(exchanges)
    else:
        exchanges = ["NYSE", "NASDAQ", "AMEX"]
        mktcap = 1_000_000_000
        label = "US (NYSE, NASDAQ, AMEX)"

    # Build exchange filter
    if exchanges:
        quoted = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND exchange IN ({quoted})"
    else:
        exchange_filter = ""

    sql = PAIRS_SCREEN_SQL.format(
        exchange_filter=exchange_filter,
        mktcap_threshold=mktcap,
        min_corr=args.min_corr,
        top_n=args.top_n,
    )

    cr = CetaResearch(api_key=args.api_key)

    print(f"Pairs Screen: {label}")
    print(f"Filters: corr >= {args.min_corr}, min 200 common days, same sector, MCap > {mktcap:,}")
    print()

    results = cr.query(sql, verbose=True)
    if not results:
        print("No pairs found meeting criteria.")
        return

    print(f"\n{'#':<4} {'Symbol A':<10} {'Symbol B':<10} {'Sector':<30} {'Corr':>7} {'Days':>6}")
    print("-" * 72)
    for i, row in enumerate(results, 1):
        print(f"{i:<4} {row['symbol_a']:<10} {row['symbol_b']:<10} "
              f"{str(row['sector'])[:30]:<30} {float(row['correlation']):>7.4f} "
              f"{int(row['common_days']):>6}")

    print(f"\nTotal: {len(results)} pairs")
    print(f"Exchange: {label}")
    print(f"Data: Ceta Research (FMP warehouse, last 252 trading days)")


if __name__ == "__main__":
    main()
