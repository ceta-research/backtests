#!/usr/bin/env python3
"""Prove capex-efficiency now compounds cash at 0% instead of deleting the period.

Builds a synthetic 25-period result list (no DB, no API, no backtest run) and
pushes it through the EXACT comprehensions now in capex-efficiency/backtest.py,
versus the ones that were there before, versus the honest reference.
"""
import ast
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from metrics import compute_metrics, compute_annual_returns  # noqa: E402

# 25 annual periods. 20 good years at +10%, 5 years where the priced book is
# thin (the guard now fires) and the surviving handful was down 40%.
records = []
for y in range(2000, 2025):
    thin = y in (2003, 2008, 2011, 2015, 2020)
    records.append({
        "start_date": f"{y}-01-02",
        "end_date": f"{y+1}-01-02",
        "n_stocks": 0 if thin else 20,
        "stocks_held": 0 if thin else 20,
        "return": 0.0 if thin else 0.10,
        "spy_return": 0.05,
        "avg_roic": None,
        # Kept in lockstep with the literal backtest.py writes. Only the
        # `== "invested"` test below is load-bearing, but a stale cash string
        # here would misdescribe what a real record looks like.
        "msg": "cash (3 buyable at entry of 30 screened)" if thin else "invested",
    })

# What the pre-fix branch computed: cash rows filtered OUT of the series.
old_ret = [r["return"] for r in records if r.get("msg") == "invested"]
old_bench = [r["spy_return"] for r in records if r.get("msg") == "invested"]
old_dates = [r["start_date"] for r in records if r.get("msg") == "invested"]

# What HEAD-of-main computed: the thin periods were still "invested" at their
# real (bad) return, so they counted.
main_ret = [0.10 if r["msg"] == "invested" else -0.40 for r in records]
main_bench = [r["spy_return"] for r in records]

# What the file computes NOW.
src = (REPO / "capex-efficiency" / "backtest.py").read_text()
tree = ast.parse(src)
found = [n for n in ast.walk(tree) if isinstance(n, ast.ListComp)]
exprs = {ast.unparse(n) for n in found}
new_ret = [r["return"] for r in records]
new_bench = [r["spy_return"] for r in records]
new_dates = [r["start_date"] for r in records]

print("=== source gate ===")
# ast.unparse normalises string quotes to single quotes.
must_have = {
    "[r['return'] for r in period_results]",
    "[r['spy_return'] for r in period_results]",
    "[r['start_date'] for r in period_results]",
}
missing = must_have - exprs
leftover = {e for e in exprs if "invested" in e}
print("required comprehensions present:", not missing, "" if not missing else missing)
print("residual msg==invested filters:", len(leftover), leftover or "(none)")
assert not missing, missing
assert not leftover, leftover
assert src.count('[r["return"] for r in period_results]') == 2, "both paths must be widened"
assert src.count('[r["spy_return"] for r in period_results]') == 2
assert src.count('[r["start_date"] for r in period_results]') == 2

print("\n=== metric effect (periods_per_year=1) ===")
for label, rets, bench in (
    ("main   (thin book counted at its real -40%)", main_ret, main_bench),
    ("branch before this fix (thin period DELETED)", old_ret, old_bench),
    ("branch now  (thin period = cash at 0%)      ", new_ret, new_bench),
):
    p = compute_metrics(rets, bench, periods_per_year=1, risk_free_rate=0.02)["portfolio"]
    print(f"{label}  n={len(rets):>2}  cagr={p['cagr']*100:>7.2f}%  "
          f"max_dd={p['max_drawdown']*100:>7.2f}%")

print("\n=== alignment gate ===")
print("returns/bench/dates lengths:", len(new_ret), len(new_bench), len(new_dates))
assert len(new_ret) == len(new_bench) == len(new_dates) == 25
ann = compute_annual_returns(new_ret, new_bench, new_dates, periods_per_year=1)
years = [a["year"] for a in ann]
print("annual rows:", len(ann), "first/last year:", years[0], years[-1])
assert len(ann) == 25, "a cash year must still appear in the annual table"
cash_years = [a for a in ann if a["portfolio"] == 0.0]
print("cash years present in annual table:", [a["year"] for a in cash_years])
assert len(cash_years) == 5

print("\n=== benchmark honesty gate ===")
bench_lines = [ln.strip() for ln in src.splitlines() if '"spy_return":' in ln]
print(f"{len(bench_lines)} spy_return writes, all carrying the real benchmark:",
      all("bench_return" in ln for ln in bench_lines))
assert len(bench_lines) == 4 and all("bench_return" in ln for ln in bench_lines)

print("\nALL CAPEX SEMANTICS GATES PASS")
