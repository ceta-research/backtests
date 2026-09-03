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
    # B006: both carry their own copy of the per-exchange accounting block
    # rather than importing the topic's build_output, so the counting fix has to
    # land in them directly.
    "dogs-of-dow/run_all_exchanges.py",
    "interest-coverage/run_all_exchanges.py",
    # B006: records two defects found while reworking period counting that are
    # NOT counting bugs and that need their own re-run (graham-timing's 0.0
    # benchmark substitution, capex-efficiency's invented invested_periods).
    "DATA_QUALITY_ISSUES.md",
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

    # ---- gate 7: cash is counted over the EXECUTED collection, never `valid`
    #
    # B006. This gate previously asserted the opposite. That was convention (A):
    # make cash_periods share a denominator with n_periods by counting both over
    # `valid`. It bought coherence by discarding real cash periods, and it broke
    # the one topic that had already solved this correctly (52-week-low, commit
    # 3c39bcb), which counts cash over the full run and publishes
    # total_rebalances alongside a smaller n_periods.
    #
    # The convention now is that they are DIFFERENT populations:
    #   cash_periods   counted over `executed` -- every rebalance the strategy
    #                  ran (portfolio_return is not None, so trailing stubs drop)
    #   n_periods      len(valid) -- executed AND benchmark-priced
    # They are never compared. That is what preserves the honest full-window
    # cash rate: Norway sat in cash for 84 of 95 rebalances, not 84 of the 50
    # that ^OSEAX (which starts 2013-03-05) happened to price.
    py_files = sorted(ROOT.glob("*/*.py"))
    wrong, checked = [], 0
    for f in py_files:
        src = f.read_text()
        rel = str(f.relative_to(ROOT))
        colls = re.findall(r"cash_periods\s*=\s*sum\(1 for \w+ in (\w+)", src)
        if colls:
            checked += 1
        for coll in colls:
            if coll == "valid":
                wrong.append((rel, f"counts cash over `{coll}`, not the "
                                   "executed collection"))
        if re.search(r'"invested_periods":\s*len\(valid\)\s*-\s*cash_periods', src):
            wrong.append((rel, "derives invested_periods from len(valid)"))
        if re.search(r"Cash periods: \{cash_periods\} / \{len\(valid\)\}", src):
            wrong.append((rel, "prints the cash rate over len(valid)"))
        # The pe-compression-style assert bound a full-window count to the
        # truncated metrics window. Anchored so the explanatory COMMENT that
        # replaced it does not match.
        if re.search(r"^\s*assert\s+0\s*<=\s*len\(valid\)\s*-\s*cash_periods",
                     src, re.M):
            wrong.append((rel, "pe-compression-style assert reintroduced"))
    wrong = sorted(set(wrong))
    print(f"7 CASHCOUNT {checked - len(set(w[0] for w in wrong))}/{checked} "
          "count cash over the executed collection")
    for rel, why in wrong:
        print(f"    WRONG SCOPE: {rel} -- {why}")
    fails += [("cash scope", f"{rel}: {why}") for rel, why in wrong]

    # ---- gate 8: universal adoption of the shared helper
    #
    # Scoped, not "every build_output". A build_output is IN SCOPE if the dict it
    # returns names one of the accounting keys; those with no period-accounting
    # concept at all are listed as exempt rather than forced to carry
    # meaningless fields.
    #
    # ***TRAP, hit for real while writing this***: ast.unparse renders string
    # literals with SINGLE quotes, so a substring test for '"cash_periods"'
    # against unparsed source matches NOTHING and the gate passes vacuously on
    # every file. This gate inspects ast.Dict key NODES, never unparsed text,
    # and the coverage self-test below catches a regression back to that.
    OWNED = {"n_periods", "cash_periods", "invested_periods", "total_rebalances"}

    def dict_literals(tree):
        return [n for n in ast.walk(tree) if isinstance(n, ast.Dict)]

    def owned_keys(d):
        """Owned keys this dict actually COMPUTES.

        Two shapes are deliberately not period accounting and are excluded by
        structure rather than by an exemption list, so a new topic in either
        shape needs no maintenance here:

        COHORT DICTS. The quintile-style topics (altman-z, asset-light,
        cash-conversion, income-quality, margin-expansion, roe-dupont,
        sustained-roic) emit `"cash_periods": {"high": n, "low": m}` -- a
        mapping keyed by cohort, not a count of rebalances. There is no single
        invested_periods to derive and no single measured window, so the helper
        does not apply.

        PASSTHROUGH READS. Merge and audit scripts copy a value straight out of
        a record already on disk (`v.get("invested_periods")`). They report what
        a run produced; they do not compute it. Rewriting them to call the
        helper would invent numbers rather than carry them.
        """
        out = set()
        for k, v in zip(d.keys, d.values):
            if not (isinstance(k, ast.Constant) and k.value in OWNED):
                continue
            if isinstance(v, ast.Dict):
                continue                       # cohort mapping, not a count
            src = ast.unparse(v)
            if re.search(r'\.get\(\s*[\'"]' + re.escape(k.value) + r'[\'"]', src):
                continue                       # passthrough read
            if re.search(r'\[\s*[\'"]' + re.escape(k.value) + r'[\'"]\s*\]', src):
                continue                       # passthrough subscript
            out.add(k.value)
        return out

    def has_acct_splat(d):
        """A None key in an ast.Dict is a ** splat. Accept it when its value is a
        period_accounting() call or a name bound from one."""
        for k, v in zip(d.keys, d.values):
            if k is not None:
                continue
            src = ast.unparse(v)
            if "period_accounting" in src or src.strip().lstrip("_") == "acct":
                return True
        return False

    # A dict is doing PERIOD ACCOUNTING, and so must use the helper, when it
    # computes a relationship between the counts: an invested_periods or a
    # total_rebalances, or both an n_periods and a cash_periods. The B006 defect
    # is precisely that relationship going wrong. A dict computing only ONE
    # count states a fact with nothing to contradict it, so there is nothing for
    # the helper to own -- e.g. sector-correlation's regime study and
    # 52-week-low/run_concentration.py publish n_periods alone, and
    # pairs-multi-pair's per-pair diversification table publishes cash_periods
    # alone.
    def is_accounting(keys):
        return bool(keys & {"invested_periods", "total_rebalances"}) or \
            {"n_periods", "cash_periods"} <= keys

    ACCOUNTING_EXEMPT = {
        # Merges per-exchange CSVs into a comparison JSON and publishes
        # "invested_periods": len(period_returns) with no cash count anywhere.
        # That number is really the count of periods that produced a return,
        # i.e. executed periods, mislabelled as invested. Fixing the label needs
        # a cash count this script's CSV inputs do not carry, so it needs a
        # re-run to fix properly and is logged in DATA_QUALITY_ISSUES.md rather
        # than papered over with an invented number here.
        "capex-efficiency/merge_results.py":
            "publishes invested_periods with no cash count; see "
            "DATA_QUALITY_ISSUES.md",
        # Declares the expected record shape as a key -> type map so it can
        # CHECK records. It does not produce one, so there is nothing here for
        # the helper to compute.
        "scripts/test_build_output_synthetic.py":
            "declares the expected schema as a key -> type map, not a record",
    }

    in_scope, bad8 = [], []
    for f in py_files:
        try:
            tree = ast.parse(f.read_text())
        except SyntaxError:
            continue
        rel = str(f.relative_to(ROOT))
        if rel in ACCOUNTING_EXEMPT:
            continue
        for d in dict_literals(tree):
            ok = owned_keys(d)
            splat = has_acct_splat(d)
            if not splat and not is_accounting(ok):
                continue
            if not ok and not splat:
                continue
            in_scope.append((rel, d.lineno))
            if ok and splat:
                bad8.append((rel, d.lineno,
                             f"splats the helper AND hard-codes {sorted(ok)}"))
            elif ok and not splat:
                bad8.append((rel, d.lineno,
                             f"hard-codes {sorted(ok)} instead of splatting "
                             "period_accounting()"))
    print(f"8 ACCOUNTING {len(in_scope) - len(bad8)}/{len(in_scope)} "
          "accounting dicts take their counts from period_accounting() "
          f"(exempt: {list(ACCOUNTING_EXEMPT)})")
    for b in bad8:
        print("    HAND-ROLLED:", b)
    fails += [("hand-rolled accounting", b) for b in bad8]
    # SELF-TEST, positive control. Coverage alone does NOT protect this gate:
    # every conforming dict is in scope via its ** splat, so an owned_keys()
    # that silently matches nothing keeps the count at 71 while quietly failing
    # to flag a single hand-rolled dict. (Verified: with owned_keys() gated on
    # an ast.unparse substring -- which renders string literals with SINGLE
    # quotes, so a '"cash_periods"' test matches nothing -- a hand-rolled
    # build_output goes completely undetected.) So feed the detector a dict that
    # MUST be flagged and check that it is.
    probe = ast.parse(
        'x = {"universe": u, "n_periods": len(valid), '
        '"cash_periods": c, "invested_periods": len(valid) - c}')
    probe_dict = next(n for n in ast.walk(probe) if isinstance(n, ast.Dict))
    probe_keys = owned_keys(probe_dict)
    if not is_accounting(probe_keys) or has_acct_splat(probe_dict):
        print("    SELF-TEST FAIL: the detector no longer flags a hand-rolled "
              f"accounting dict (owned_keys returned {sorted(probe_keys)}); "
              "gate 8 is passing vacuously")
        fails.append(("gate 8 vacuous", sorted(probe_keys)))
    if len(in_scope) < 60:
        print(f"    SELF-TEST FAIL: only {len(in_scope)} accounting dicts found; "
              "the AST walk has probably stopped matching")
        fails.append(("gate 8 coverage", len(in_scope)))

    # ---- gate 9: no cash percentage still divides by the metrics window
    # Two distinct shapes. A single pattern anchored on the literal
    # "cash_periods" catches only the second and leaves all 55 printers unguarded.
    PRINTER = re.compile(
        r'cp\s*=\s*r\.get\("cash_periods".*?\n.*?cp\s*\*\s*100\s*/\s*'
        r'(?:n|n_periods|n_years)\b', re.S)
    INLINE = re.compile(
        r'cash_periods.{0,80}?/\s*(?:max\()?\s*(?:\w+\.get\(")?'
        r'(?:n_periods|n_years)\b', re.S)
    bad9 = []
    for f in py_files:
        src = f.read_text()
        rel = str(f.relative_to(ROOT))
        if PRINTER.search(src):
            bad9.append((rel, "summary printer divides cash by the metrics window"))
        if INLINE.search(src):
            bad9.append((rel, "inline cash pct divides by the metrics window"))
    print(f"9 DENOMINATOR {len(py_files) - len(set(b[0] for b in bad9))}"
          f"/{len(py_files)} files free of metrics-window cash denominators")
    for b in bad9:
        print("    WRONG DENOMINATOR:", b)
    fails += [("cash denominator", b) for b in bad9]

    print()
    print("ALL GATES PASS" if not fails else f"GATE FAILURES: {len(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
