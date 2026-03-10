#!/usr/bin/env python3
"""
Sector Correlation Regime Backtest

Monthly rebalancing. Uses 60-day rolling average pairwise correlation across 9 S&P 500
sector SPDR ETFs to classify market regimes. Shifts allocation based on regime.

Signal: Average pairwise correlation of XLK, XLE, XLF, XLV, XLY, XLP, XLI, XLB, XLU
        - High (>0.7):        Defensive sectors only (XLU, XLV, XLP), equal weight
        - Medium (0.4-0.7):   SPY buy-and-hold (100%)
        - Low (<0.4):         All 9 sector ETFs, equal weight

Transaction costs: 0.1% of portfolio when regime changes (ETF trading cost).

Academic basis:
    Longin & Solnik (2001) "Extreme Correlation of International Equity Markets"
    Journal of Finance, 56(2), 649-676.
    Kritzman et al. (2012) "Regime Shifts: Implications for Dynamic Strategies"
    Financial Analysts Journal, 68(3), 22-39.

Usage:
    python3 sector-correlation/backtest.py
    python3 sector-correlation/backtest.py --output results/backtest_results.json --verbose
    python3 sector-correlation/backtest.py --no-costs

See README.md for strategy details.
"""

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from itertools import combinations

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch
from metrics import compute_metrics, compute_annual_returns, format_metrics

# --- Strategy parameters ---
SECTOR_ETFS = ['XLK', 'XLE', 'XLF', 'XLV', 'XLY', 'XLP', 'XLI', 'XLB', 'XLU']
DEFENSIVE_ETFS = ['XLU', 'XLV', 'XLP']
BENCHMARK = 'SPY'

CORR_WINDOW = 60        # Trading days for rolling correlation
HIGH_THRESHOLD = 0.70   # avg corr > this → high regime (defensive)
LOW_THRESHOLD = 0.40    # avg corr < this → low regime (diversified)

ETF_COST = 0.001        # 0.1% one-way ETF cost on regime change

BACKTEST_START = 2000
BACKTEST_END = 2025

SECTOR_PAIRS = list(combinations(SECTOR_ETFS, 2))  # 36 pairs


def fetch_prices(cr, verbose=False):
    """Fetch all daily prices for sector ETFs + SPY.

    Returns a dict: symbol -> sorted list of (date, adjClose) tuples.
    """
    all_symbols = SECTOR_ETFS + [BENCHMARK]
    sym_list = ", ".join(f"'{s}'" for s in all_symbols)

    print("  Fetching sector ETF + SPY prices (1999-2026)...")
    price_sql = f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, adjClose
        FROM stock_eod
        WHERE symbol IN ({sym_list})
          AND CAST(date AS DATE) >= '1999-01-01'
          AND CAST(date AS DATE) <= '2026-03-01'
          AND adjClose IS NOT NULL
          AND adjClose > 0
        ORDER BY symbol, trade_date
    """

    rows = cr.query(price_sql, format="json", limit=200_000, timeout=120,
                    verbose=verbose, memory_mb=4096, threads=2)

    if not rows:
        print("  ERROR: No price data returned.")
        return None

    # Build dict: symbol -> sorted list of (date_obj, price)
    prices = {}
    for row in rows:
        sym = row["symbol"]
        dt = row["trade_date"]
        if isinstance(dt, str):
            dt = date.fromisoformat(dt[:10])
        price = float(row["adjClose"])
        if sym not in prices:
            prices[sym] = []
        prices[sym].append((dt, price))

    for sym in prices:
        prices[sym].sort(key=lambda x: x[0])

    n_symbols = len(prices)
    total_rows = sum(len(v) for v in prices.values())
    print(f"    -> {total_rows:,} price rows ({n_symbols} symbols)")
    return prices


def build_returns(prices):
    """Compute daily returns from prices.

    Returns dict: symbol -> sorted list of (date_obj, daily_return).
    """
    returns = {}
    for sym, price_list in prices.items():
        sym_returns = []
        for i in range(1, len(price_list)):
            dt = price_list[i][0]
            prev = price_list[i - 1][1]
            curr = price_list[i][1]
            if prev > 0:
                sym_returns.append((dt, (curr - prev) / prev))
        returns[sym] = sym_returns
    return returns


def build_return_dict(returns):
    """Convert returns lists to date-keyed dicts for O(1) lookup.

    Returns dict: symbol -> {date_obj: daily_return}
    """
    return {sym: dict(sym_rets) for sym, sym_rets in returns.items()}


def get_all_trading_dates(returns_list):
    """Get sorted list of all unique trading dates across sector ETFs."""
    date_set = set()
    for sym in SECTOR_ETFS:
        if sym in returns_list:
            date_set.update(dt for dt, _ in returns_list[sym])
    return sorted(date_set)


def compute_avg_correlation(return_dict, trading_dates, as_of_date):
    """Compute average pairwise correlation for the 60 trading days ending on as_of_date.

    Args:
        return_dict: symbol -> {date: return}
        trading_dates: sorted list of all trading dates
        as_of_date: date object (last date to include in window)

    Returns float or None if insufficient data.
    """
    # Find trading dates <= as_of_date
    eligible = [d for d in trading_dates if d <= as_of_date]
    if len(eligible) < CORR_WINDOW + 5:
        return None

    window_dates = set(eligible[-CORR_WINDOW:])

    correlations = []
    for s1, s2 in SECTOR_PAIRS:
        rd1 = return_dict.get(s1, {})
        rd2 = return_dict.get(s2, {})
        common = [d for d in window_dates if d in rd1 and d in rd2]
        if len(common) < 20:
            continue

        common.sort()
        r1 = [rd1[d] for d in common]
        r2 = [rd2[d] for d in common]

        n = len(r1)
        mean1 = sum(r1) / n
        mean2 = sum(r2) / n
        ss1 = sum((x - mean1) ** 2 for x in r1)
        ss2 = sum((x - mean2) ** 2 for x in r2)
        cov = sum((r1[i] - mean1) * (r2[i] - mean2) for i in range(n))
        denom = (ss1 * ss2) ** 0.5
        if denom > 0:
            correlations.append(cov / denom)

    if len(correlations) < 10:
        return None
    return sum(correlations) / len(correlations)


def classify_regime(avg_corr):
    if avg_corr is None:
        return "medium"
    if avg_corr > HIGH_THRESHOLD:
        return "high"
    if avg_corr < LOW_THRESHOLD:
        return "low"
    return "medium"


def get_regime_allocation(regime):
    if regime == "high":
        return DEFENSIVE_ETFS
    if regime == "low":
        return SECTOR_ETFS
    return [BENCHMARK]


def get_price_on_or_after(prices, symbol, target_date, lookahead=10):
    """Get the first price for symbol on or after target_date."""
    end = target_date + timedelta(days=lookahead)
    sym_prices = prices.get(symbol, [])
    for dt, px in sym_prices:
        if target_date <= dt <= end:
            return px
    return None


def generate_month_starts(start_year, end_year):
    """Return list of first-of-month dates from Jan start_year to Jan (end_year+1)."""
    dates = []
    for year in range(start_year, end_year + 2):
        for month in range(1, 13):
            dates.append(date(year, month, 1))
            if year == end_year + 1 and month == 1:
                return dates
    return dates


def run_backtest(prices, return_dict, trading_dates, use_costs=True, verbose=False):
    """Run the correlation regime backtest. Returns list of period dicts."""
    month_starts = generate_month_starts(BACKTEST_START, BACKTEST_END)

    results = []
    prev_regime = None

    for i in range(len(month_starts) - 1):
        entry_month = month_starts[i]
        exit_month = month_starts[i + 1]

        # Signal: last trading day before month start
        signal_date = entry_month - timedelta(days=1)

        avg_corr = compute_avg_correlation(return_dict, trading_dates, signal_date)
        regime = classify_regime(avg_corr)
        allocation = get_regime_allocation(regime)

        # Prices at entry and exit
        period_returns = []
        for sym in allocation:
            ep = get_price_on_or_after(prices, sym, entry_month)
            xp = get_price_on_or_after(prices, sym, exit_month)
            if ep and xp and ep > 0:
                period_returns.append((xp - ep) / ep)

        port_return = sum(period_returns) / len(period_returns) if period_returns else 0.0

        # Transaction cost on regime change
        if use_costs and prev_regime is not None and regime != prev_regime:
            port_return -= ETF_COST

        # SPY benchmark
        spy_ep = get_price_on_or_after(prices, BENCHMARK, entry_month)
        spy_xp = get_price_on_or_after(prices, BENCHMARK, exit_month)
        spy_return = None
        if spy_ep and spy_xp and spy_ep > 0:
            spy_return = (spy_xp - spy_ep) / spy_ep

        results.append({
            "rebalance_date": entry_month.isoformat(),
            "exit_date": exit_month.isoformat(),
            "avg_correlation": round(avg_corr, 4) if avg_corr is not None else None,
            "regime": regime,
            "allocation": ",".join(allocation),
            "portfolio_return": round(port_return, 6),
            "spy_return": round(spy_return, 6) if spy_return is not None else None,
            "regime_changed": prev_regime is not None and regime != prev_regime,
        })

        if verbose:
            corr_str = f"{avg_corr:.3f}" if avg_corr is not None else "  N/A"
            excess_str = ""
            if spy_return is not None:
                excess_str = f"  ex={((port_return - spy_return) * 100):+.1f}%"
            print(f"    {entry_month.strftime('%Y-%m')}: corr={corr_str} [{regime:6s}] "
                  f"port={port_return*100:+5.1f}% spy={spy_return*100 if spy_return else 0:+5.1f}%{excess_str}")

        prev_regime = regime

    return results


def compute_regime_stats(results):
    """Compute per-regime counts and average annualized return."""
    regimes = {"high": [], "medium": [], "low": []}
    for r in results:
        regime = r.get("regime", "medium")
        ret = r.get("portfolio_return")
        if ret is not None:
            regimes[regime].append(ret)

    n_total = len(results)
    stats = {}
    for regime, rets in regimes.items():
        n = len(rets)
        avg_monthly = sum(rets) / n if rets else 0
        avg_annual = ((1 + avg_monthly) ** 12 - 1) * 100
        stats[regime] = {
            "months": n,
            "pct_time": round(n * 100 / n_total, 1) if n_total > 0 else 0,
            "avg_annual_return": round(avg_annual, 2),
        }
    return stats


def build_output(metrics, annual, results, regime_stats, use_costs):
    p = metrics["portfolio"]
    b = metrics["benchmark"]
    c = metrics["comparison"]

    def pct(v):
        return round(v * 100, 2) if v is not None else None

    def rnd(v, d=3):
        return round(v, d) if v is not None else None

    def fmt(s):
        return {
            "total_return": pct(s.get("total_return")),
            "cagr": pct(s.get("cagr")),
            "max_drawdown": pct(s.get("max_drawdown")),
            "annualized_volatility": pct(s.get("annualized_volatility")),
            "sharpe_ratio": rnd(s.get("sharpe_ratio")),
            "sortino_ratio": rnd(s.get("sortino_ratio")),
            "calmar_ratio": rnd(s.get("calmar_ratio")),
            "var_95": pct(s.get("var_95")),
        }

    n_changes = sum(1 for r in results if r.get("regime_changed"))

    return {
        "universe": "US Sector ETFs (SPDR)",
        "n_periods": len(results),
        "years": round(len(results) / 12, 1),
        "frequency": "monthly",
        "correlation_window_days": CORR_WINDOW,
        "high_threshold": HIGH_THRESHOLD,
        "low_threshold": LOW_THRESHOLD,
        "transaction_costs": use_costs,
        "n_regime_changes": n_changes,
        "portfolio": fmt(p),
        "spy": fmt(b),
        "comparison": {
            "excess_cagr": pct(c.get("excess_cagr")),
            "win_rate": pct(c.get("win_rate")),
            "information_ratio": rnd(c.get("information_ratio")),
            "tracking_error": pct(c.get("tracking_error")),
            "up_capture": pct(c.get("up_capture")),
            "down_capture": pct(c.get("down_capture")),
            "beta": rnd(c.get("beta")),
            "alpha": pct(c.get("alpha")),
        },
        "excess_cagr": pct(c.get("excess_cagr")),
        "win_rate_vs_spy": pct(c.get("win_rate")),
        "regime_stats": regime_stats,
        "annual_returns": [
            {
                "year": ar["year"],
                "portfolio": round(ar["portfolio"] * 100, 2),
                "spy": round(ar["benchmark"] * 100, 2),
                "excess": round(ar["excess"] * 100, 2),
            }
            for ar in annual
        ],
        "monthly_returns": [
            {
                "date": r["rebalance_date"],
                "regime": r["regime"],
                "avg_correlation": r["avg_correlation"],
                "portfolio_return": r["portfolio_return"],
                "spy_return": r["spy_return"],
            }
            for r in results
        ],
    }


def main():
    parser = argparse.ArgumentParser(description="Sector Correlation Regime Backtest")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--no-costs", action="store_true")
    parser.add_argument("--api-key", type=str, default=None)
    parser.add_argument("--base-url", type=str,
                        default="https://api.cetaresearch.com/api/v1")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY")
    use_costs = not args.no_costs
    risk_free_rate = 0.020  # US 10Y Treasury

    print("=" * 65)
    print("  SECTOR CORRELATION REGIME BACKTEST")
    print("  Universe: 9 S&P 500 Sector SPDR ETFs")
    print(f"  Signal: {CORR_WINDOW}-day avg pairwise correlation")
    print(f"  High >0.7: defensive | Low <0.4: all 9 ETFs | Medium: SPY")
    print(f"  Period: {BACKTEST_START}-{BACKTEST_END} | Monthly | Costs: {'yes' if use_costs else 'no'}")
    print("=" * 65)

    cr = CetaResearch(api_key=api_key, base_url=args.base_url)

    # Phase 1: Fetch data
    print("\nPhase 1: Fetching data...")
    t0 = time.time()
    prices = fetch_prices(cr, verbose=args.verbose)
    if prices is None:
        sys.exit(1)

    # Build returns in memory
    returns_list = build_returns(prices)
    return_dict = build_return_dict(returns_list)
    trading_dates = get_all_trading_dates(returns_list)
    print(f"  {len(trading_dates)} trading dates from {trading_dates[0]} to {trading_dates[-1]}")
    fetch_time = time.time() - t0
    print(f"  Data ready in {fetch_time:.0f}s")

    # Phase 2: Run backtest
    print(f"\nPhase 2: Running monthly backtest ({BACKTEST_START}-{BACKTEST_END})...")
    t1 = time.time()
    results = run_backtest(prices, return_dict, trading_dates,
                           use_costs=use_costs, verbose=args.verbose)
    bt_time = time.time() - t1
    print(f"  Backtest completed in {bt_time:.0f}s ({len(results)} periods)")

    # Phase 3: Compute metrics
    valid = [r for r in results
             if r["portfolio_return"] is not None and r["spy_return"] is not None]

    if not valid:
        print("ERROR: No valid periods.")
        sys.exit(1)

    port_returns = [r["portfolio_return"] for r in valid]
    spy_returns = [r["spy_return"] for r in valid]

    metrics = compute_metrics(port_returns, spy_returns, periods_per_year=12,
                              risk_free_rate=risk_free_rate)
    print(format_metrics(metrics, "Correlation Regime", "SPY"))

    regime_stats = compute_regime_stats(valid)
    print(f"\n  Regime breakdown:")
    for regime in ["high", "medium", "low"]:
        s = regime_stats[regime]
        print(f"    {regime:8s}: {s['months']:3d} months ({s['pct_time']:.1f}%), "
              f"avg {s['avg_annual_return']:+.1f}%/yr")

    n_changes = sum(1 for r in results if r.get("regime_changed"))
    print(f"  Regime changes: {n_changes}")

    period_dates = [r["rebalance_date"] for r in valid]
    annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year=12)
    if annual:
        print(f"\n  {'Year':<8} {'CorrRegime':>12} {'SPY':>10} {'Excess':>10}")
        print("  " + "-" * 44)
        for ar in annual:
            print(f"  {ar['year']:<8} {ar['portfolio']*100:>11.1f}% "
                  f"{ar['benchmark']*100:>9.1f}% {ar['excess']*100:>+9.1f}%")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s")

    output = build_output(metrics, annual, valid, regime_stats, use_costs)

    if args.output:
        os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to {args.output}")
    else:
        print("\n  (Use --output results/backtest_results.json to save)")


if __name__ == "__main__":
    main()
