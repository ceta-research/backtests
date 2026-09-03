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
    # B005 follow-up: hosts entry_usable/entry_buyable/entry_buyable_prices, the
    # entry-knowable count the cash rule is now decided on. Root .py files match
    # none of the allow rules below, so without this entry the scope gate
    # rejects the fix's own dependency.
    "data_utils.py",
    # B005 follow-up: records the exit-side survivorship question that change
    # deliberately does NOT fix, so the decision is written down where the 66
    # guard comments point rather than living only in a commit message. B006
    # appends to it too (graham-timing's 0.0 benchmark substitution,
    # capex-efficiency's invented invested_periods).
    "DATA_QUALITY_ISSUES.md",
    # B006: hosts the shared period_accounting/warn_if_truncated helper. Root
    # .py files match none of the allow rules below, so without this entry the
    # scope gate rejects its own dependency.
    "metrics.py",
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


BUYABLE_FNS = ("entry_buyable", "entry_buyable_prices")


def buyable_vars(tree):
    """Names bound from an entry_buyable*() call -- the entry-knowable count."""
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            f = node.value.func
            nm = f.id if isinstance(f, ast.Name) else getattr(f, "attr", None)
            if nm in BUYABLE_FNS:
                out.update(t.id for t in node.targets if isinstance(t, ast.Name))
    return out


def guard_hits(tree):
    """Operator-agnostic: any Compare of an entry-buyable count against a floor
    constant. Catches graham-timing's inverted `>=` and sector-*'s
    MIN_PORTFOLIO_STOCKS without a separate rule for either. One rule now covers
    all 66 topics, including qarp/low-pe, which used to need their own.

    Anchored on the BOUND NAME, not on `len(...)`: len() of the post-filter book
    is now the DEFECT, not the fix -- see lookahead_hits."""
    fl, bv = floors(tree), buyable_vars(tree)
    return [(n.lineno, n.left.id, c.id)
            for n in ast.walk(tree)
            if isinstance(n, ast.Compare) and isinstance(n.left, ast.Name)
            and n.left.id in bv
            for c in n.comparators
            if isinstance(c, ast.Name) and c.id in fl]


def lookahead_hits(tree):
    """The DEFECT form: a floor compared against len() of the POST-FILTER book.

    filter_returns drops names for a missing EXIT price and for a realised
    return above max_single_return. A cash decision read off that count uses
    exit-date information at the rebalance date, and it resolves the flattering
    way: a period the strategy really held and really lost is rewritten as a
    flat 0.0%. Any such compare is a regression -- including in qarp/low-pe,
    where the post-price book is the inline `returns`/`held` list rather than a
    filter_returns binding."""
    fl, fv = floors(tree), filter_vars(tree)
    names = fv | {"returns", "held", "clean", "clean_returns"}
    return [(n.lineno, n.left.args[0].id, c.id)
            for n in ast.walk(tree)
            if isinstance(n, ast.Compare) and isinstance(n.left, ast.Call)
            and isinstance(n.left.func, ast.Name) and n.left.func.id == "len"
            and n.left.args and isinstance(n.left.args[0], ast.Name)
            and n.left.args[0].id in names
            for c in n.comparators
            if isinstance(c, ast.Name) and c.id in fl]


# The collections that stand for "every rebalance the strategy actually ran".
EXECUTED_COLLS = {"executed", "results"}


def derived_denominator(src):
    """(allowed cash-count collections, human label) for one topic's source.

    Reads which population the topic derives invested_periods from, so gate 7
    can require the cash count to come from the same one.
    """
    if re.search(r'"invested_periods":\s*total_rebalances\s*-\s*cash_periods', src) \
            and re.search(r"total_rebalances\s*=\s*len\(results\)", src):
        return EXECUTED_COLLS, "total_rebalances = len(results)"
    return {"valid"}, "len(valid)"


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
    #
    # Two questions, not one. Every floor topic must HAVE a post-price guard,
    # and no guard may be decided on the post-filter book. The second half is
    # the B005 blocker: filter_returns also drops a name for a missing EXIT
    # price and for a realised return above MAX_SINGLE_RETURN, neither of which
    # is knowable on the rebalance date, so `len(clean) < MIN_STOCKS` sends a
    # period to cash off exit-date information.
    floor_topics, nofloor, unguarded, lookahead = [], [], [], []
    for t in all_topics:
        tree = parse(t)
        if floors(tree):
            floor_topics.append(t)
            if not guard_hits(tree):
                unguarded.append(t)
            for lineno, var, fl in lookahead_hits(tree):
                lookahead.append((t, lineno, f"floor {fl} compared against "
                                             f"len({var})"))
        else:
            nofloor.append(t)
    print(f"3 CENSUS    floor topics {len(floor_topics)}, "
          f"guarded {len(floor_topics) - len(unguarded)}, unguarded {len(unguarded)}; "
          f"no-floor {len(nofloor)} (must stay {len(nofloor)}: no floor invented); "
          f"exit-conditioned {len(lookahead)}")
    for t in unguarded:
        print("    UNGUARDED", t)
    for h in lookahead:
        print("    EXIT-CONDITIONED CASH:", h)
    fails += [("unguarded", t) for t in unguarded]
    fails += [("exit-conditioned cash", h) for h in lookahead]
    # SELF-TEST 1, teeth against a topic with NO guard at all. Rewriting the
    # detector to anchor on a bound name rather than on `len(...)` could easily
    # leave it matching nothing, and a detector that matches nothing looks
    # exactly like a corpus with nothing wrong in it.
    p3_none = ast.parse("MIN_STOCKS = 10\ndef f(d):\n    return d\n")
    if guard_hits(p3_none) or floors(p3_none) != {"MIN_STOCKS": 10}:
        print("    SELF-TEST FAIL: gate 3 no longer recognises an UNGUARDED "
              "floor topic; a topic with no post-price guard would pass")
        fails.append(("gate 3 unguarded-probe", "unguarded probe not detected"))
    # SELF-TEST 2, teeth against the B005 form this change removes.
    p3_bad = ast.parse("MIN_STOCKS = 10\ndef f(d):\n"
                       "    clean, skipped = filter_returns(d)\n"
                       "    if len(clean) < MIN_STOCKS:\n        return 0\n")
    if not lookahead_hits(p3_bad) or guard_hits(p3_bad):
        print("    SELF-TEST FAIL: gate 3 no longer flags the exit-conditioned "
              "`len(clean) < MIN_STOCKS` guard it was rewritten to catch")
        fails.append(("gate 3 lookahead-probe", "probe not flagged"))
    # SELF-TEST 3, negative control: the corrected form must be ACCEPTED, or the
    # gate cannot be satisfied by fixing the code and gets disabled instead.
    p3_ok = ast.parse("MIN_STOCKS = 10\ndef f(d):\n"
                      "    buyable = entry_buyable(d, min_entry_price=1.0)\n"
                      "    if buyable < MIN_STOCKS:\n        return 0\n")
    if not guard_hits(p3_ok) or lookahead_hits(p3_ok):
        print("    SELF-TEST FAIL: gate 3 rejects the corrected form")
        fails.append(("gate 3 false positive", "clean probe flagged"))

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

    # ---- gate 7: cash_periods and invested_periods share ONE denominator
    #
    # Almost every topic derives invested_periods from len(valid), and n_periods
    # is len(valid), so counting cash over `results` subtracts periods that are
    # not in its own denominator. That is how published legs ended up with a
    # negative invested_periods (deleveraging OSL: 50 - 53 = -3).
    #
    # 52-week-low is the exception, and it is prior art rather than a bug
    # (commit 3c39bcb): its build_output publishes total_rebalances = len(results)
    # and derives invested_periods from THAT. Forcing its cash count onto
    # `valid` gives the record two different denominators and overstates
    # invested periods 4-5x -- the next OSL re-run would publish ~45-56 against
    # a true 11. So the gate checks COHERENCE between the two numbers, not a
    # fixed collection name.
    wrong, right = [], []
    for t in floor_topics:
        src = (ROOT / t / "backtest.py").read_text()
        colls = re.findall(r"cash_periods\s*=\s*sum\(1 for r in (\w+)", src)
        if not colls:
            continue
        want, denom = derived_denominator(src)
        if all(c in want for c in colls):
            right.append(t)
        else:
            wrong.append((t, f"counts cash over {sorted(set(colls))} but derives "
                             f"invested_periods from {denom}"))
        if want == {"valid"} and \
                f"Cash periods: {{cash_periods}} / {{len(results)}}" in src:
            wrong.append((t, "prints the cash rate over len(results)"))
    wrong = sorted(set(wrong))
    print(f"7 CASHCOUNT {len(right)}/{len(right) + len(wrong)} count cash over "
          "the same population invested_periods is derived from")
    for t, why in wrong:
        print(f"    MISMATCHED DENOMINATOR: {t} -- {why}")
    fails += [("cash denominator mismatch", f"{t}: {why}") for t, why in wrong]
    # SELF-TEST. The rule is now conditional, so either branch can go vacuous on
    # its own and the clean-tree output looks the same either way. Feed it one
    # source of each shape and require the right verdict.
    p7_valid = 'x = {"invested_periods": len(valid) - cash_periods}'
    p7_total = ('total_rebalances = len(results)\n'
                'x = {"invested_periods": total_rebalances - cash_periods}')
    if derived_denominator(p7_valid)[0] != {"valid"} or \
            derived_denominator(p7_total)[0] != EXECUTED_COLLS:
        print("    SELF-TEST FAIL: gate 7 can no longer tell the two "
              "invested_periods derivations apart; one branch is dead")
        fails.append(("gate 7 vacuous", "denominator probe misclassified"))

    print()
    print("ALL GATES PASS" if not fails else f"GATE FAILURES: {len(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
