#!/usr/bin/env python3
"""
Run Interest Coverage backtest across all eligible exchanges.
Aggregates results into a single exchange_comparison.json file.

Usage:
    python3 interest-coverage/run_all_exchanges.py --verbose
    python3 interest-coverage/run_all_exchanges.py --output results/exchange_comparison.json
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cr_client import CetaResearch
from data_utils import query_parquet, get_prices, generate_rebalance_dates
from metrics import compute_metrics, compute_annual_returns
from costs import tiered_cost, apply_costs
from cli_utils import get_risk_free_rate, get_mktcap_threshold, REGIONAL_RISK_FREE_RATES

# Import backtest functions from our module
from backtest import (
    fetch_data_via_api, run_backtest,
    COVERAGE_MIN, DE_MIN, DE_MAX, ROE_MIN, MAX_STOCKS, MIN_STOCKS,
    DEFAULT_FREQUENCY,
)

# Exchanges to test (sorted by expected qualifying stock count)
# Excluded exchanges (see DATA_QUALITY_ISSUES.md):
#   ASX, SAO — broken adjClose (unadjusted stock splits, >1000x price ratios)
#   JPX, LSE — no FY fundamental data in warehouse
EXCHANGE_CONFIGS = [
    {"name": "US_MAJOR", "exchanges": ["NYSE", "NASDAQ", "AMEX"]},
    {"name": "BSE", "exchanges": ["BSE"]},
    {"name": "NSE", "exchanges": ["NSE"]},
    {"name": "XETRA", "exchanges": ["XETRA"]},
    {"name": "HKSE", "exchanges": ["HKSE"]},
    {"name": "SHZ", "exchanges": ["SHZ"]},
    {"name": "SHH", "exchanges": ["SHH"]},
    {"name": "KSC", "exchanges": ["KSC"]},
    {"name": "TSX", "exchanges": ["TSX"]},
    {"name": "SET", "exchanges": ["SET"]},
    {"name": "TAI", "exchanges": ["TAI"]},
    {"name": "STO", "exchanges": ["STO"]},
    {"name": "SIX", "exchanges": ["SIX"]},
]


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run Interest Coverage backtest on all exchanges")
    parser.add_argument("--output", type=str, default="interest-coverage/results/exchange_comparison.json")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--api-key", type=str)
    parser.add_argument("--base-url", type=str)
    parser.add_argument("--no-costs", action="store_true")
    parser.add_argument("--frequency", type=str, default=DEFAULT_FREQUENCY)
    parser.add_argument("--resume", action="store_true",
                        help="Skip exchanges already completed in output file")
    args = parser.parse_args()

    freq_map = {"monthly": 12, "quarterly": 4, "semi-annual": 2, "annual": 1}
    periods_per_year = freq_map[args.frequency]
    use_costs = not args.no_costs

    cr = CetaResearch(api_key=args.api_key, base_url=args.base_url)

    # Load existing results if resuming
    all_results = {}
    if args.resume and os.path.exists(args.output):
        with open(args.output) as f:
            all_results = json.load(f)
        completed = [k for k, v in all_results.items() if v.get("status") == "completed"]
        print(f"Resuming: {len(completed)} already completed: {completed}")

    total_start = time.time()

    for config in EXCHANGE_CONFIGS:
        name = config["name"]
        exchanges = config["exchanges"]
        risk_free_rate = get_risk_free_rate(exchanges)
        mktcap_threshold = get_mktcap_threshold(exchanges)

        # Skip if already completed in resume mode
        if args.resume and all_results.get(name, {}).get("status") == "completed":
            print(f"\n  SKIPPING {name} (already completed)")
            continue

        print(f"\n{'='*65}")
        print(f"  {name} ({', '.join(exchanges)})")
        print(f"  Risk-free rate: {risk_free_rate*100:.1f}%")
        print(f"{'='*65}")

        try:
            t0 = time.time()
            rebalance_dates = generate_rebalance_dates(2000, 2025, args.frequency)
            con = fetch_data_via_api(cr, exchanges, rebalance_dates, verbose=args.verbose)

            if con is None:
                print(f"  SKIPPED: No data for {name}")
                all_results[name] = {"status": "no_data"}
                continue

            results = run_backtest(con, rebalance_dates, mktcap_threshold, use_costs=use_costs, verbose=args.verbose)

            valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
            if not valid:
                print(f"  SKIPPED: No valid periods for {name}")
                all_results[name] = {"status": "no_valid_periods"}
                con.close()
                continue

            port_returns = [r["portfolio_return"] for r in valid]
            spy_returns = [r["spy_return"] for r in valid]

            metrics = compute_metrics(port_returns, spy_returns, periods_per_year,
                                      risk_free_rate=risk_free_rate)

            p = metrics["portfolio"]
            b = metrics["benchmark"]
            c = metrics["comparison"]

            cash_periods = sum(1 for r in results if r["stocks_held"] == 0)
            invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
            avg_stocks = sum(invested) / len(invested) if invested else 0

            period_dates = [r["rebalance_date"] for r in valid]
            annual = compute_annual_returns(port_returns, spy_returns, period_dates, periods_per_year)

            def pct(v):
                return round(v * 100, 2) if v is not None else None

            def rnd(v, d=3):
                return round(v, d) if v is not None else None

            def format_series(s):
                return {
                    "total_return": pct(s.get("total_return")),
                    "cagr": pct(s.get("cagr")),
                    "max_drawdown": pct(s.get("max_drawdown")),
                    "annualized_volatility": pct(s.get("annualized_volatility")),
                    "sharpe_ratio": rnd(s.get("sharpe_ratio")),
                    "sortino_ratio": rnd(s.get("sortino_ratio")),
                    "calmar_ratio": rnd(s.get("calmar_ratio")),
                    "var_95": pct(s.get("var_95")),
                    "max_consecutive_losses": s.get("max_consecutive_losses"),
                    "pct_negative_periods": pct(s.get("pct_negative_periods")),
                }

            exchange_result = {
                "universe": name,
                "exchanges": exchanges,
                "n_periods": len(valid),
                "years": round(len(valid) / periods_per_year, 1),
                "frequency": args.frequency,
                "cash_periods": cash_periods,
                "invested_periods": len(valid) - cash_periods,
                "avg_stocks_when_invested": round(avg_stocks, 1),
                "portfolio": format_series(p),
                "spy": format_series(b),
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
                "annual_returns": [
                    {"year": ar["year"],
                     "portfolio": round(ar["portfolio"] * 100, 2),
                     "spy": round(ar["benchmark"] * 100, 2),
                     "excess": round(ar["excess"] * 100, 2)}
                    for ar in annual
                ],
                "status": "completed",
                "elapsed_seconds": round(time.time() - t0, 1),
            }

            all_results[name] = exchange_result
            con.close()

            # Print summary
            cagr = exchange_result["portfolio"]["cagr"]
            spy_cagr = exchange_result["spy"]["cagr"]
            sharpe = exchange_result["portfolio"]["sharpe_ratio"]
            max_dd = exchange_result["portfolio"]["max_drawdown"]
            print(f"\n  CAGR: {cagr}% (SPY: {spy_cagr}%), Sharpe: {sharpe}, "
                  f"MaxDD: {max_dd}%, Cash: {cash_periods}/{len(results)}, "
                  f"Avg stocks: {avg_stocks:.0f}")

        except Exception as e:
            print(f"  ERROR: {name} failed: {e}")
            all_results[name] = {"status": "error", "error": str(e)}

    # Save aggregated results
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(all_results, f, indent=2)

    total_time = time.time() - total_start
    print(f"\n{'='*65}")
    print(f"  ALL EXCHANGES COMPLETE ({total_time:.0f}s)")
    print(f"  Results saved to {args.output}")
    print(f"{'='*65}")

    # Print comparison table
    print(f"\n{'Exchange':<12} {'CAGR':>8} {'SPY':>8} {'Excess':>8} {'Sharpe':>8} {'MaxDD':>8} {'Cash':>6} {'Stocks':>7}")
    print("-" * 75)
    for name, r in sorted(all_results.items(), key=lambda x: x[1].get("portfolio", {}).get("cagr", 0) or 0, reverse=True):
        if r.get("status") != "completed":
            print(f"{name:<12} {'SKIPPED':>8} ({r.get('status', 'unknown')})")
            continue
        p = r["portfolio"]
        sharpe_str = f"{p['sharpe_ratio']:>8.3f}" if p.get('sharpe_ratio') is not None else f"{'N/A':>8}"
        print(f"{name:<12} {p['cagr']:>7.1f}% {r['spy']['cagr']:>7.1f}% "
              f"{r['excess_cagr']:>+7.1f}% {sharpe_str} "
              f"{p['max_drawdown']:>7.1f}% {r['cash_periods']:>5}/{r['n_periods']:<1} "
              f"{r['avg_stocks_when_invested']:>5.0f}")


if __name__ == "__main__":
    main()
