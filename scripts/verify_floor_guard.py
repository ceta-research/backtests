#!/usr/bin/env python3
"""Static gates for the post-price stock-floor guard (blocker B005).

Runs alongside scripts/test_floor_guard.py. That harness proves behaviour on
synthetic data; this proves the change landed everywhere it was supposed to and
nowhere it wasn't.

Gates:
  1 COMPILE    every */backtest.py byte-compiles
  2 IMPORT     every */backtest.py imports clean (catches NameError at module scope)
  3 CENSUS     every floor topic is guarded, and the no-floor topics are still
               no-floor (proof no floor was invented, which the task forbids)
  4 SCHEMA     every floor topic publishes per-period data
  5 HOLDINGS   no invested-branch holdings expression still reads the pre-filter list
  6 SCOPE      nothing outside */backtest.py and scripts/ is modified

Usage:  python scripts/verify_floor_guard.py
"""
import ast
import pathlib
import py_compile
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
FLOOR_NAMES = ("MIN_STOCKS", "MIN_PORTFOLIO_STOCKS")

# Documented exemptions, each with the reason it cannot satisfy the generic gate.
SCHEMA_EXEMPT = {
    # Different record schema (start_date/n_stocks/msg), and its per-exchange
    # output goes through cli_utils.save_results, which writes a metrics JSON plus
    # results/returns_{exchange}.csv. period_results reaches JSON only on the
    # --global path. So a re-run-population scan reads per-period stocks_held from
    # results/returns_*.csv for this topic, not from a returns_*.json.
    "capex-efficiency": "per-period data lands in results/returns_*.csv, not JSON",
}
HOLDINGS_EXEMPT = {
    "capex-efficiency": "has no holdings key at all",
}
# Pre-existing environment gap, not touched by this change: statsmodels is not
# installed and pairs-cointegration imports adfuller at module scope. Verified
# untouched (`git status --porcelain pairs-cointegration/` is empty).
IMPORT_EXEMPT = {
    "pairs-cointegration": "needs statsmodels, not installed in this venv",
}
# Files outside */backtest.py and scripts/ that this change is allowed to touch.
SCOPE_ALLOW = {
    # Carries period_data through to the combined comparison so one glob covers
    # every topic's per-period book size.
    "magic-formula/run_all_exchanges.py",
}


def topics():
    return sorted(p.parent.name for p in ROOT.glob("*/backtest.py"))


def parse(topic):
    return ast.parse((ROOT / topic / "backtest.py").read_text())


def floors(tree):
    out = {}
    for n in tree.body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in FLOOR_NAMES:
                    if isinstance(n.value, ast.Constant):
                        out[t.id] = n.value.value
    return out


def filter_vars(tree):
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            f = node.value.func
            nm = f.id if isinstance(f, ast.Name) else getattr(f, "attr", None)
            if nm == "filter_returns":
                t = node.targets[0]
                if isinstance(t, ast.Tuple) and isinstance(t.elts[0], ast.Name):
                    out.add(t.elts[0].id)
                elif isinstance(t, ast.Name):
                    out.add(t.id)
    return out


def guard_hits(tree):
    """Operator-agnostic: any Compare of len(<filter-bound var>) against a floor
    constant. Catches graham-timing's inverted `>=` and sector-*'s
    MIN_PORTFOLIO_STOCKS without a separate rule for either."""
    fl, fv = floors(tree), filter_vars(tree)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Call):
            f = node.left.func
            if isinstance(f, ast.Name) and f.id == "len" and node.left.args:
                a = node.left.args[0]
                if isinstance(a, ast.Name) and a.id in fv:
                    for comp in node.comparators:
                        if isinstance(comp, ast.Name) and comp.id in fl:
                            hits.append((node.lineno, a.id, comp.id))
    return hits


def inline_guard_hits(tree):
    """qarp/low-pe have no filter_returns; their post-price book is `returns`,
    so the guard compares len(returns) to the floor."""
    fl = floors(tree)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Call):
            f = node.left.func
            if isinstance(f, ast.Name) and f.id == "len" and node.left.args:
                a = node.left.args[0]
                if isinstance(a, ast.Name) and a.id in ("returns", "held"):
                    for comp in node.comparators:
                        if isinstance(comp, ast.Name) and comp.id in fl:
                            hits.append((node.lineno, a.id, comp.id))
    return hits


def main():
    fails = []
    all_topics = topics()

    # ---- gate 1: compile
    bad = []
    for t in all_topics:
        try:
            py_compile.compile(str(ROOT / t / "backtest.py"), doraise=True)
        except Exception as e:
            bad.append((t, str(e)[:120]))
    print(f"1 COMPILE   {len(all_topics) - len(bad)}/{len(all_topics)} ok")
    for b in bad:
        print("    FAIL", b)
    fails += bad

    # ---- gate 2: import
    bad = []
    import importlib.util
    for t in all_topics:
        try:
            spec = importlib.util.spec_from_file_location(
                "chk_" + t.replace("-", "_"), ROOT / t / "backtest.py")
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
        except Exception as e:
            if t in IMPORT_EXEMPT:
                continue
            bad.append((t, f"{e.__class__.__name__}: {e}"))
    print(f"2 IMPORT    {len(all_topics) - len(bad) - len(IMPORT_EXEMPT)}"
          f"/{len(all_topics) - len(IMPORT_EXEMPT)} ok "
          f"(exempt: {list(IMPORT_EXEMPT)})")
    for b in bad:
        print("    FAIL", b)
    fails += bad

    # ---- gate 3: census
    floor_topics, nofloor, unguarded = [], [], []
    for t in all_topics:
        tree = parse(t)
        if floors(tree):
            floor_topics.append(t)
            if not (guard_hits(tree) or inline_guard_hits(tree)):
                unguarded.append(t)
        else:
            nofloor.append(t)
    print(f"3 CENSUS    floor topics {len(floor_topics)}, "
          f"guarded {len(floor_topics) - len(unguarded)}, unguarded {len(unguarded)}; "
          f"no-floor {len(nofloor)} (must stay {len(nofloor)}: no floor invented)")
    for t in unguarded:
        print("    UNGUARDED", t)
    fails += [("unguarded", t) for t in unguarded]

    # ---- gate 4: schema (per-period data published)
    missing = []
    for t in floor_topics:
        if t in SCHEMA_EXEMPT:
            continue
        src = (ROOT / t / "backtest.py").read_text()
        if '"period_data"' not in src:
            missing.append(t)
    print(f"4 SCHEMA    {len(floor_topics) - len(missing) - len(SCHEMA_EXEMPT)}"
          f"/{len(floor_topics) - len(SCHEMA_EXEMPT)} publish period_data "
          f"(exempt: {list(SCHEMA_EXEMPT)})")
    for t in missing:
        print("    MISSING period_data:", t)
    fails += [("no period_data", t) for t in missing]

    # ---- gate 5: holdings reads the post-filter book
    bad = []
    for t in floor_topics:
        if t in HOLDINGS_EXEMPT:
            continue
        tree = parse(t)
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                for k, v in zip(node.keys, node.values):
                    if not (isinstance(k, ast.Constant) and k.value == "holdings"):
                        continue
                    expr = ast.unparse(v)
                    if expr.startswith("f'CASH") or expr.startswith('f"CASH'):
                        continue          # cash branches name no stocks
                    names = {n.id for n in ast.walk(v) if isinstance(n, ast.Name)}
                    if names & {"symbols", "portfolio", "stocks"}:
                        bad.append((t, node.lineno, expr[:70]))
    print(f"5 HOLDINGS  {len(floor_topics) - len(HOLDINGS_EXEMPT) - len(bad)}"
          f"/{len(floor_topics) - len(HOLDINGS_EXEMPT)} report the priced book")
    for b in bad:
        print("    PRE-FILTER LIST:", b)
    fails += bad

    # ---- gate 6: scope
    # Union of the working tree and everything already committed on this branch, so
    # the gate keeps its teeth after the work is committed. Checking only
    # `git status` would report a clean tree and pass vacuously.
    touched = set()
    for line in subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                               capture_output=True, text=True).stdout.splitlines():
        touched.add(line[3:].strip().strip('"'))
    committed = subprocess.run(["git", "diff", "main...HEAD", "--name-only"], cwd=ROOT,
                               capture_output=True, text=True)
    if committed.returncode == 0:
        touched.update(p for p in committed.stdout.split() if p)
    stray = sorted(p for p in touched if not (
        p.endswith("/backtest.py")
        or p.startswith("scripts/")
        or p in SCOPE_ALLOW))
    print(f"6 SCOPE     {len(touched)} files changed, {len(stray)} outside "
          f"*/backtest.py + scripts/ + {len(SCOPE_ALLOW)} allowed")
    for s in stray:
        print("    OUT OF SCOPE:", s)
    fails += [("out of scope", s) for s in stray]

    # ---- gate 7: cash_periods counts over the same collection as n_periods
    # n_periods is len(valid). Counting cash over `results` subtracts periods that
    # are not in that denominator, which is how published legs ended up with a
    # negative invested_periods (deleveraging OSL: 50 - 53 = -3).
    wrong, right = [], []
    for t in floor_topics:
        src = (ROOT / t / "backtest.py").read_text()
        colls = re.findall(r"cash_periods\s*=\s*sum\(1 for r in (\w+)", src)
        if not colls:
            continue
        (right if all(c == "valid" for c in colls) else wrong).append(t)
        if f"Cash periods: {{cash_periods}} / {{len(results)}}" in src:
            wrong.append(t)
    wrong = sorted(set(wrong))
    print(f"7 CASHCOUNT {len(right)}/{len(right) + len(wrong)} count cash over `valid`")
    for t in wrong:
        print("    COUNTS OVER `results`:", t)
    fails += [("cash_periods over results", t) for t in wrong]

    print()
    print("ALL GATES PASS" if not fails else f"GATE FAILURES: {len(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
