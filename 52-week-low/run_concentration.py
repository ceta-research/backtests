#!/usr/bin/env python3
"""Ad-hoc: 52-week-low quality, US, quarterly. Compare portfolio sizes 10/20/30.

Fetches data once, re-screens at each size for an apples-to-apples comparison.
Answers the YouTube comment: "can you do this on 10 stocks instead of 30."
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest as bt
from cr_client import CetaResearch

EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]
SIZES = [10, 15, 20, 30]
PPY = 4  # quarterly

cr = CetaResearch()
rebalance_dates = bt.generate_rebalance_dates(2002, 2025, "quarterly",
                                              months=bt.DEFAULT_REBALANCE_MONTHS)
print("Fetching US data once...")
con = bt.fetch_data_via_api(cr, EXCHANGES, rebalance_dates, verbose=False)
mktcap = bt.get_mktcap_threshold(EXCHANGES)
rfr = bt.get_risk_free_rate(EXCHANGES, None)
print(f"mktcap_min={mktcap:,.0f}  rfr={rfr:.3f}")

summary = {}
for n in SIZES:
    bt.MAX_STOCKS = n
    results = bt.run_backtest(con, rebalance_dates, mktcap, use_costs=True,
                              verbose=False, offset_days=1, benchmark_symbol="SPY")
    valid = [r for r in results if r["portfolio_return"] is not None and r["spy_return"] is not None]
    port = [r["portfolio_return"] for r in valid]
    spy = [r["spy_return"] for r in valid]
    m = bt.compute_metrics(port, spy, PPY, risk_free_rate=rfr)
    p, b, c = m["portfolio"], m["benchmark"], m["comparison"]
    invested = [r["stocks_held"] for r in results if r["stocks_held"] > 0]
    avg_stocks = sum(invested) / len(invested) if invested else 0
    final_strat = 10000 * (1 + p["total_return"])
    final_spy = 10000 * (1 + b["total_return"])
    summary[n] = {
        "n_periods": len(valid),
        "avg_stocks": round(avg_stocks, 1),
        "cagr": round(p["cagr"] * 100, 2),
        "final_value": round(final_strat),
        "spy_cagr": round(b["cagr"] * 100, 2),
        "spy_final_value": round(final_spy),
        "max_drawdown": round(p["max_drawdown"] * 100, 2),
        "annualized_volatility": round(p["annualized_volatility"] * 100, 2),
        "sharpe": round(p["sharpe_ratio"], 3),
        "sortino": round(p["sortino_ratio"], 3),
        "calmar": round(p["calmar_ratio"], 3),
        "excess_cagr": round(c["excess_cagr"] * 100, 2),
        "win_rate": round(c["win_rate"] * 100, 1),
        "down_capture": round(c["down_capture"] * 100, 1),
        "up_capture": round(c["up_capture"] * 100, 1),
        "beta": round(c["beta"], 3),
        "best_period": round(max(port) * 100, 1),
        "worst_period": round(min(port) * 100, 1),
    }
    print(f"\n=== TOP {n} (avg held {summary[n]['avg_stocks']}) ===")
    print(f"  CAGR {summary[n]['cagr']}%  final ${summary[n]['final_value']:,}  "
          f"vs SPY {summary[n]['spy_cagr']}% ${summary[n]['spy_final_value']:,}")
    print(f"  MaxDD {summary[n]['max_drawdown']}%  Vol {summary[n]['annualized_volatility']}%  "
          f"Sharpe {summary[n]['sharpe']}  Sortino {summary[n]['sortino']}")
    print(f"  Excess {summary[n]['excess_cagr']}%  Win {summary[n]['win_rate']}%  "
          f"DownCap {summary[n]['down_capture']}%  Beta {summary[n]['beta']}")
    print(f"  Best qtr {summary[n]['best_period']}%  Worst qtr {summary[n]['worst_period']}%")

con.close()
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "concentration_us.json")
with open(out, "w") as f:
    json.dump(summary, f, indent=2)
print(f"\nSaved {out}")
print("\n\nFINAL TABLE")
print(f"{'Size':>6} {'AvgHeld':>8} {'CAGR':>7} {'Final$':>10} {'MaxDD':>8} {'Vol':>7} {'Sharpe':>7} {'Excess':>8}")
for n in SIZES:
    s = summary[n]
    print(f"{n:>6} {s['avg_stocks']:>8} {s['cagr']:>6}% ${s['final_value']:>9,} "
          f"{s['max_drawdown']:>7}% {s['annualized_volatility']:>6}% {s['sharpe']:>7} {s['excess_cagr']:>7}%")
