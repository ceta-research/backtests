#!/usr/bin/env python3
"""Multi-Pair Portfolio: Current Signal Screen

Finds same-sector cointegrated pairs using the most recent 252 trading days
for formation and the most recent 40 days for z-score computation. Shows the
current portfolio composition for N = 5, 10, 15, and 20 simultaneous pairs,
with inverse-volatility weights based on trailing 60-day spread volatility.

Sector cap enforcement: marks pairs that exceed the soft cap of 3 per sector
(informational only — the backtest does not hard-cap, but over-concentration
in one sector is worth flagging).

Usage:
    # Screen US stocks (default)
    python3 pairs-multi-pair/screen.py

    # Screen Japanese stocks
    python3 pairs-multi-pair/screen.py --preset japan

    # Screen German stocks
    python3 pairs-multi-pair/screen.py --exchange XETRA

    # Show more pairs
    python3 pairs-multi-pair/screen.py --top-n 30
"""

import argparse
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from cli_utils import add_common_args, resolve_exchanges, get_mktcap_threshold

# Soft cap: mark pairs if the sector already has this many pairs in the portfolio
SECTOR_CAP_SOFT = 3

# ── Exchange-specific market cap thresholds (local currency, mirrors backtest) ──
MKTCAP_DEFAULTS = {
    "NYSE": 1_000_000_000, "NASDAQ": 1_000_000_000, "AMEX": 1_000_000_000,
    "JPX":  100_000_000_000,
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

# ── SQL: pair formation + z-score + trailing spread vol ──────────────────────
# Formation window: 252 trading days (≈ 1 year)
# Z-score window: 40 trading days
# Spread vol window: 60 trading days (for inverse-vol weight display)
SCREEN_SQL = """
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
-- 252-day daily returns for correlation (formation period)
daily_ret AS (
    SELECT eod.symbol,
           CAST(eod.date AS DATE) AS trade_date,
           (eod.adjClose - LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date))
               / NULLIF(LAG(eod.adjClose) OVER (PARTITION BY eod.symbol ORDER BY eod.date), 0) AS ret
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE eod.date >= (CURRENT_DATE - INTERVAL '380 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
-- Same-sector correlation pairs (corr >= 0.70, >= 200 common days)
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
      AND CORR(a.ret, b.ret) >= 0.70
),
-- Log-price data: full formation window for OLS beta + recent 100d for z-score/vol
log_prices AS (
    SELECT eod.symbol,
           CAST(eod.date AS DATE) AS trade_date,
           LN(eod.adjClose) AS lp,
           eod.adjClose
    FROM stock_eod eod
    JOIN large_caps lc ON eod.symbol = lc.symbol
    WHERE eod.date >= (CURRENT_DATE - INTERVAL '110 days')
      AND eod.adjClose IS NOT NULL AND eod.adjClose > 0
),
-- OLS beta from formation window
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
    LIMIT 200
),
-- 40-day rolling z-score + 60-day spread return vol (most recent day only)
spread_metrics AS (
    SELECT
        pp.sym_a, pp.sym_b, pp.sector, pp.correlation, pp.beta,
        -- Current spread and z-score
        (la.lp - pp.beta * lb.lp) AS spread_today,
        AVG(la.lp - pp.beta * lb.lp)
            OVER (PARTITION BY pp.sym_a, pp.sym_b
                  ORDER BY la.trade_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
            AS spread_mean,
        STDDEV(la.lp - pp.beta * lb.lp)
            OVER (PARTITION BY pp.sym_a, pp.sym_b
                  ORDER BY la.trade_date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
            AS spread_std,
        -- 60-day spread RETURN vol (annualised): STDDEV of daily spread changes * sqrt(252)
        -- spread_change_t = spread_t - spread_{t-1}
        STDDEV(
            (la.lp - pp.beta * lb.lp)
            - LAG(la.lp - pp.beta * lb.lp) OVER (
                PARTITION BY pp.sym_a, pp.sym_b ORDER BY la.trade_date
            )
        ) OVER (PARTITION BY pp.sym_a, pp.sym_b
                ORDER BY la.trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
        * SQRT(252) AS spread_vol_ann,
        la.trade_date,
        la.adjClose AS price_a,
        lb.adjClose AS price_b,
        ROW_NUMBER() OVER (PARTITION BY pp.sym_a, pp.sym_b ORDER BY la.trade_date DESC) AS rn
    FROM pair_params pp
    JOIN log_prices la ON pp.sym_a = la.symbol
    JOIN log_prices lb ON pp.sym_b = lb.symbol AND la.trade_date = lb.trade_date
)
SELECT
    sym_a, sym_b, sector,
    ROUND(correlation, 3) AS corr,
    ROUND(beta, 3) AS beta,
    ROUND((spread_today - spread_mean) / NULLIF(spread_std, 0), 3) AS z_score,
    ROUND(spread_vol_ann, 4) AS spread_vol_ann,
    trade_date AS last_date,
    ROUND(price_a, 2) AS price_a,
    ROUND(price_b, 2) AS price_b
FROM spread_metrics
WHERE rn = 1
  AND spread_std > 0
  AND price_a >= 1.0
  AND price_b >= 1.0
ORDER BY correlation DESC
LIMIT {top_n}
"""


def get_mktcap(exchanges):
    if not exchanges:
        return 1_000_000_000
    return min(MKTCAP_DEFAULTS.get(ex, 1_000_000_000) for ex in exchanges)


def compute_inv_vol_weights(pairs_subset):
    """Compute inverse-volatility weights for a list of pair dicts.

    Pairs with missing spread_vol_ann get equal-weight fallback.
    Returns list of floats (weights, sum = 1.0).
    """
    vols = [p.get("spread_vol_ann") for p in pairs_subset]
    valid = [(i, v) for i, v in enumerate(vols) if v and float(v) > 0]

    if not valid:
        # All missing — equal weight
        return [1.0 / len(pairs_subset)] * len(pairs_subset)

    inv_vols = [1.0 / float(v) for _, v in valid]
    total_iv = sum(inv_vols)
    n_missing = len(pairs_subset) - len(valid)

    weights = [0.0] * len(pairs_subset)
    valid_share = 1.0 - (n_missing / len(pairs_subset)) if n_missing > 0 else 1.0

    for (i, _), iv in zip(valid, inv_vols):
        weights[i] = (iv / total_iv) * valid_share

    if n_missing > 0:
        equal_share = (1.0 - valid_share) / n_missing
        for i, v in enumerate(vols):
            if not v or float(v) <= 0:
                weights[i] = equal_share

    return weights


def label_sector_concentration(pairs, n):
    """Return dict mapping (sym_a, sym_b) → True if sector exceeds SECTOR_CAP_SOFT
    within the top-n pairs.
    """
    subset = pairs[:n]
    sector_counts = {}
    flagged = {}
    for p in subset:
        sec = p["sector"]
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if sector_counts[sec] > SECTOR_CAP_SOFT:
            flagged[(p["sym_a"], p["sym_b"])] = True
    return flagged


def print_portfolio_view(pairs, n, label, flagged):
    """Print portfolio composition for a given N with inv-vol weights."""
    subset = pairs[:n]
    weights = compute_inv_vol_weights(subset)

    print(f"\n  Portfolio N={n} — {label}")
    print(f"  {'Rank':<5} {'Sym A':<8} {'Sym B':<8} {'Sector':<24} "
          f"{'Corr':>6} {'Z':>7} {'SpVol':>7} {'IVWt':>6} {'Note'}")
    print("  " + "-" * 88)

    for i, (p, w) in enumerate(zip(subset, weights), 1):
        z       = float(p["z_score"]) if p.get("z_score") else 0.0
        sv      = float(p["spread_vol_ann"]) if p.get("spread_vol_ann") else None
        sv_s    = f"{sv:.3f}" if sv else " N/A "
        w_s     = f"{w*100:.1f}%"
        note    = ""
        if (p["sym_a"], p["sym_b"]) in flagged:
            note = "!SECTOR"
        elif abs(z) >= 2.0:
            note = "SIGNAL"
        elif abs(z) >= 1.0:
            note = "watch"

        z_dir = ""
        if z > 2.0:
            z_dir = "↓A"   # short A, long B
        elif z < -2.0:
            z_dir = "↑A"   # long A, short B

        print(f"  {i:<5} {p['sym_a']:<8} {p['sym_b']:<8} "
              f"{str(p['sector'])[:24]:<24} "
              f"{float(p['corr']):>6.3f} {z:>+7.3f} {sv_s:>7} {w_s:>6}  {note} {z_dir}")

    active_signals = sum(1 for p in subset if abs(float(p.get("z_score") or 0)) >= 2.0)
    sector_counts = {}
    for p in subset:
        sector_counts[p["sector"]] = sector_counts.get(p["sector"], 0) + 1
    overweight = {s: c for s, c in sector_counts.items() if c > SECTOR_CAP_SOFT}

    print(f"\n  Active signals (|z|>2.0): {active_signals}/{n}")
    if overweight:
        ow_str = ", ".join(f"{s}={c}" for s, c in overweight.items())
        print(f"  ! Sector concentration (>{SECTOR_CAP_SOFT}): {ow_str}")


def main():
    parser = argparse.ArgumentParser(description="Multi-Pair Portfolio Screen")
    add_common_args(parser)
    parser.add_argument("--top-n", type=int, default=25,
                        help="Max pairs to fetch (default: 25, must be >= 20)")
    args = parser.parse_args()

    exchanges, universe_name = resolve_exchanges(args)
    top_n = max(args.top_n, 20)   # need at least 20 for all portfolio views

    if exchanges:
        quoted = ", ".join(f"'{e}'" for e in exchanges)
        exchange_filter = f"AND exchange IN ({quoted})"
        mktcap = get_mktcap(exchanges)
    else:
        exchange_filter = ""
        mktcap = 1_000_000_000

    sql = SCREEN_SQL.format(
        exchange_filter=exchange_filter,
        mktcap_threshold=mktcap,
        top_n=top_n,
    )

    cr = CetaResearch(api_key=args.api_key)

    print(f"Multi-Pair Portfolio Screen: {universe_name}")
    print(f"Formation: 252d returns corr | Z-score: 40d rolling | Spread vol: 60d trailing")
    print(f"MCap > {mktcap:,} | Entry threshold: |z| > 2.0")
    print()

    results = cr.query(sql, verbose=getattr(args, "verbose", False))
    if not results:
        print("No pairs found.")
        return

    print(f"Found {len(results)} pairs passing corr >= 0.70 filter (ordered by correlation)")

    # Summary of all pairs
    strong   = [r for r in results if abs(float(r.get("z_score") or 0)) >= 2.0]
    moderate = [r for r in results if 1.0 <= abs(float(r.get("z_score") or 0)) < 2.0]

    print(f"\nAll pairs summary:")
    print(f"  Actionable (|z|>2.0): {len(strong)}")
    print(f"  Watching   (1<|z|<2): {len(moderate)}")
    print(f"  Universe: {universe_name}")

    # Full pair list
    print(f"\n  {'Rank':<5} {'Sym A':<8} {'Sym B':<8} {'Sector':<24} "
          f"{'Corr':>6} {'Beta':>6} {'Z-Score':>8} {'SpVol':>7} {'Signal'}")
    print("  " + "-" * 85)

    for i, row in enumerate(results, 1):
        z    = float(row.get("z_score") or 0)
        sv   = float(row["spread_vol_ann"]) if row.get("spread_vol_ann") else None
        sv_s = f"{sv:.3f}" if sv else " N/A "

        if z < -2.0:
            sig = "LONG A / SHORT B"
        elif z > 2.0:
            sig = "SHORT A / LONG B"
        elif z < -1.0:
            sig = "watch (weak long)"
        elif z > 1.0:
            sig = "watch (weak short)"
        else:
            sig = ""

        print(f"  {i:<5} {row['sym_a']:<8} {row['sym_b']:<8} "
              f"{str(row['sector'])[:24]:<24} "
              f"{float(row['corr']):>6.3f} {float(row['beta']):>6.3f} "
              f"{z:>+8.3f} {sv_s:>7}  {sig}")

    # Portfolio views for each N with sector concentration flags
    print(f"\n{'='*70}")
    print(f"PORTFOLIO VIEWS (top N by correlation, inverse-vol weights)")
    print(f"{'='*70}")
    print(f"  !SECTOR = exceeds {SECTOR_CAP_SOFT}-pair sector soft cap")
    print(f"  SIGNAL  = currently actionable (|z| > 2.0)")

    for n in [5, 10, 15, 20]:
        if len(results) < n:
            print(f"\n  [N={n}: only {len(results)} pairs available, skipping]")
            continue
        flagged = label_sector_concentration(results, n)
        print_portfolio_view(results, n, "inverse-vol weighted", flagged)

    print(f"\nData: Ceta Research (FMP warehouse, formation: last 252 days)")
    print(f"Entry signal: |z| > 2.0 | Exit signal: |z| < 0.5")
    print(f"Inv-vol weight: 1 / spread_vol_ann (60-day trailing)")


if __name__ == "__main__":
    main()
