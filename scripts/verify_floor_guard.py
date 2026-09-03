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
  7 CASHCOUNT  cash is counted over `executed`, never over the metrics window
  8 ACCOUNTING every accounting dict takes its counts from period_accounting()
  9 DENOMINATOR no cash percentage divides by the metrics window
 10 BINDING    every period_accounting() call site actually imports the callee
 11 PROVENANCE the measured window has a reader, not just a writer
 12 CHARTS     chart inclusion gates on window_truncated too
 13 FUNNEL     every period record carries an entry-knowable, decodable funnel

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
    # B006: its CAGR chart gated on `cash_periods < total_periods`, comparing a
    # full-run cash count against the truncated metrics window, so a truncated
    # leg was read as 100%-cash and dropped from the chart with no trace. The
    # gate now compares against total_rebalances.
    "magic-formula/generate_charts.py",
    # THE ONE RESULTS FILE THIS CHANGE ADDS, and the only one it may touch. It
    # is a positive control for scripts/scan_results_invariant.py that was
    # gitignored and present only in the author's tree, so the scan reported
    # "the scanner lost its teeth" on every other checkout. Force-added, copied
    # byte-for-byte and verified with cmp; no backtest produced it and no other
    # results file is modified. Gate 6 flagged it correctly before this entry
    # existed -- that is the gate working, and the exemption is deliberate.
    "volume-confirmed-momentum/results/vcm_osl.json",
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
} | {
    # B006 presentation layer: each renders a cash percentage read from a
    # results JSON, so each needs total_rebalances as its denominator or it
    # shows 168% cash on a truncated leg. oversold-quality additionally drops a
    # workaround that filtered out rows with negative invested_periods.
    f"{t}/generate_charts.py"
    for t in ("52-week-high", "quality-momentum", "earnings-consistency",
              "price-momentum", "value-momentum", "cyclical-timing",
              "pairs-zscore", "relative-strength", "oversold-quality",
              "pairs-fundamentals", "pairs-multi-pair")
} | {
    # Same denominator fix, plus it now carries window_label through to the
    # arena summary.
    "nse_arena/framework.py",
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


def _slot_names(elt, i):
    """Names appearing in slot `i` of a built (sym, entry, exit, mcap) tuple."""
    if isinstance(elt, ast.Tuple) and len(elt.elts) > i:
        return {n.id for n in ast.walk(elt.elts[i]) if isinstance(n, ast.Name)}
    return set()


def _one_level_sources(tree, names):
    """Expand `x` to the names on the right of a local `x = <expr>`.

    One level only. It is enough to see through `xp = exit_prices.get(sym)`,
    and it stops well short of dragging in half the function.
    """
    out = set(names)
    for n in ast.walk(tree):
        if (isinstance(n, ast.Assign) and len(n.targets) == 1
                and isinstance(n.targets[0], ast.Name)
                and n.targets[0].id in names):
            out |= {m.id for m in ast.walk(n.value) if isinstance(m, ast.Name)}
    return out


def _builders(tree, name):
    """Every construction site of `name`, as (element expr, [gating conditions])."""
    sites = []
    for node in ast.walk(tree):
        # form 1:  name = [ <elt> for <t> in ... if <cond> ]
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets):
            v = node.value
            if isinstance(v, (ast.ListComp, ast.GeneratorExp, ast.SetComp)):
                sites.append((v.elt, [c for g in v.generators for c in g.ifs]))
        # form 2:  if <cond>: name.append(<elt>)
        if isinstance(node, ast.If):
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == "append"
                        and isinstance(inner.func.value, ast.Name)
                        and inner.func.value.id == name and inner.args):
                    sites.append((inner.args[0], [node.test]))
    return sites


def provenance_hits(tree):
    """The look-ahead the BOUND NAME cannot see.

    guard_hits anchors on `buyable < MIN_STOCKS`, so it accepts the guard
    whatever `buyable` was counted from. But sector-rotation, sector-momentum
    and graham-timing build their symbol_data EXIT-FILTERED:

        symbol_data = [(sym, entry.get(sym), exit.get(sym), mcap.get(sym))
                       for sym in symbols
                       if entry.get(sym) and exit.get(sym)]     # <-- exit slot

    Counting THAT with entry_buyable() restores the exit-conditioned cash rule
    in full, with `buyable` still the bound name and every other gate green.
    That is why entry_buyable_prices(symbols, entry_map) exists as a separate
    helper for those three: it takes the entry map explicitly and cannot be fed
    an exit-filtered collection by accident.

    So: resolve the collection handed to entry_buyable(), and flag it if any
    condition gating its elements reads a name that feeds the tuple's EXIT slot
    and not its ENTRY slot. Subtracting the entry-slot names is what keeps the
    shared loop variable `sym` from flagging every comprehension ever written.
    """
    hits = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in BUYABLE_FNS and node.args):
            continue
        if node.func.id == "entry_buyable_prices":
            continue          # takes the entry price map directly; nothing to resolve
        arg = node.args[0]
        if not isinstance(arg, ast.Name):
            continue
        for elt, conds in _builders(tree, arg.id):
            entry_side = _one_level_sources(tree, _slot_names(elt, 1))
            exit_only = _one_level_sources(tree, _slot_names(elt, 2)) - entry_side
            if not exit_only:
                continue
            for c in conds:
                used = {n.id for n in ast.walk(c) if isinstance(n, ast.Name)}
                bad = sorted(used & exit_only)
                if bad:
                    hits.append((node.lineno, arg.id, bad))
    return hits


# ---------------------------------------------------------------------------
# The entry funnel (gate 13). See the block comment above period_state() in
# data_utils.py for what the three fields mean and why min_stocks is one of them.
FUNNEL_KEYS = ("screened", "entry_buyable", "min_stocks")
# Names that hold the POST-FILTER book. Reading any of them into an
# entry-knowable field reintroduces B005 INSIDE the field built to audit it,
# which is strictly worse than not having the field: the artifact would then
# assert an exit-conditioned count as entry-knowable. filter_vars() adds each
# topic's own filter_returns binding on top of these.
EXIT_CONDITIONED = {"clean", "clean_returns", "returns", "held", "net_returns",
                    "skipped"}


def period_dicts(tree):
    """Every appended dict literal that is a per-period record.

    Anchored on `stocks_held`, the one key all 66 topics' records share --
    including capex-efficiency, whose records land in a CSV rather than JSON and
    whose other keys (start_date/n_stocks/msg) match nothing else.
    """
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "append" and node.args
                and isinstance(node.args[0], ast.Dict)):
            d = node.args[0]
            if any(isinstance(k, ast.Constant) and k.value == "stocks_held"
                   for k in d.keys):
                yield d


def funnel_hits(tree):
    """Every way a period record's entry-funnel fields can be wrong.

    Four separate checks, because they fail independently:

    PRESENCE.    All three keys, in EVERY record of a floor topic -- cash
                 branches included. Not optional-when-unknown: cli_utils'
                 save_results derives its CSV header from period_results[0],
                 so a key missing from the first record is missing from the
                 whole file. Null carries "not measured"; absence carries
                 nothing.
    PROVENANCE.  `entry_buyable` must be None, a name bound from an
                 entry_buyable*() call, or a direct call to one. Anything else
                 is a hand-rolled count that no other gate inspects.
    FLOOR.       `min_stocks` must be the topic's floor CONSTANT, never a
                 literal. A literal 10 sitting beside a MIN_STOCKS that someone
                 later changes to 12 is the classic stale duplicate.
    TAINT.       Neither entry-knowable field may read the post-filter book.
                 This is what stops `"entry_buyable": len(clean)`.

    Plus null monotonicity: a record that never screened cannot know how many
    of the names it never screened were buyable.

    The ORDERING invariant (stocks_held <= entry_buyable <= screened) is a
    property of values, not of syntax, so it is not checkable here. It is
    asserted on real records by S7 in scripts/test_floor_guard.py and on the
    committed corpus by scripts/scan_results_invariant.py.
    """
    fl, bv = floors(tree), buyable_vars(tree)
    tainted = filter_vars(tree) | EXIT_CONDITIONED
    out = []
    for d in period_dicts(tree):
        vals = {k.value: v for k, v in zip(d.keys, d.values)
                if isinstance(k, ast.Constant)}
        missing = [k for k in FUNNEL_KEYS if k not in vals]
        if missing:
            out.append((d.lineno, f"period record is missing {missing}"))
            continue
        ms = vals["min_stocks"]
        if not (isinstance(ms, ast.Name) and ms.id in fl):
            out.append((d.lineno, f"min_stocks is {ast.unparse(ms)}, not this "
                                  f"topic's floor constant {sorted(fl)}"))
        eb = vals["entry_buyable"]
        if not ((isinstance(eb, ast.Constant) and eb.value is None)
                or (isinstance(eb, ast.Name) and eb.id in bv)
                or (isinstance(eb, ast.Call) and isinstance(eb.func, ast.Name)
                    and eb.func.id in BUYABLE_FNS)):
            out.append((d.lineno, f"entry_buyable is {ast.unparse(eb)}, not None "
                                  "and not an entry_buyable*() count"))
        for key in ("screened", "entry_buyable"):
            used = {n.id for n in ast.walk(vals[key]) if isinstance(n, ast.Name)}
            bad = sorted(used & tainted)
            if bad:
                out.append((d.lineno, f"{key} reads the POST-FILTER book {bad}; "
                                      "that count is exit-conditioned (B005)"))
        sc = vals["screened"]
        if (isinstance(sc, ast.Constant) and sc.value is None
                and not (isinstance(eb, ast.Constant) and eb.value is None)):
            out.append((d.lineno, "screened is None but entry_buyable is not; "
                                  "a period that never screened cannot know "
                                  "how many names were buyable"))
    return out


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
    floor_topics, nofloor, unguarded, lookahead, provenance = [], [], [], [], []
    for t in all_topics:
        tree = parse(t)
        if floors(tree):
            floor_topics.append(t)
            if not guard_hits(tree):
                unguarded.append(t)
            for lineno, var, fl in lookahead_hits(tree):
                lookahead.append((t, lineno, f"floor {fl} compared against "
                                             f"len({var})"))
            for lineno, var, bad in provenance_hits(tree):
                provenance.append((t, lineno, f"entry_buyable({var}) where {var} "
                                              f"is built exit-filtered on {bad}"))
        else:
            nofloor.append(t)
    print(f"3 CENSUS    floor topics {len(floor_topics)}, "
          f"guarded {len(floor_topics) - len(unguarded)}, unguarded {len(unguarded)}; "
          f"no-floor {len(nofloor)} (must stay {len(nofloor)}: no floor invented); "
          f"exit-conditioned {len(lookahead)}; exit-filtered source {len(provenance)}")
    for t in unguarded:
        print("    UNGUARDED", t)
    for h in lookahead:
        print("    EXIT-CONDITIONED CASH:", h)
    for h in provenance:
        print("    EXIT-FILTERED SOURCE:", h)
    fails += [("unguarded", t) for t in unguarded]
    fails += [("exit-conditioned cash", h) for h in lookahead]
    fails += [("exit-filtered source", h) for h in provenance]
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
    # SELF-TEST 4, teeth against the regression the bound name cannot see: the
    # guard reads `buyable`, but `buyable` was counted over an EXIT-FILTERED
    # collection. Both other probes stay green on this, which is the point.
    p3_prov = ast.parse(
        "MIN_STOCKS = 10\ndef f(symbols, e, x, m):\n"
        "    symbol_data = [(s, e.get(s), x.get(s), m.get(s)) for s in symbols\n"
        "                   if e.get(s) and x.get(s)]\n"
        "    buyable = entry_buyable(symbol_data)\n"
        "    if buyable < MIN_STOCKS:\n        return 0\n")
    if not provenance_hits(p3_prov):
        print("    SELF-TEST FAIL: gate 3 no longer flags entry_buyable() over an "
              "EXIT-FILTERED collection -- the look-ahead can be reintroduced "
              "under the corrected bound name")
        fails.append(("gate 3 provenance-probe", "exit-filtered source not flagged"))
    # SELF-TEST 5, negative control for the same detector. The two-argument
    # helper and an unfiltered comprehension must both stay clean, or the gate
    # is unsatisfiable and gets deleted rather than fixed.
    p3_prov_ok = ast.parse(
        "MIN_STOCKS = 10\ndef f(symbols, e, x, m):\n"
        "    symbol_data = [(s, e.get(s), x.get(s), m.get(s)) for s in symbols\n"
        "                   if e.get(s)]\n"
        "    a = entry_buyable(symbol_data)\n"
        "    b = entry_buyable_prices(symbols, e)\n"
        "    return a + b\n")
    if provenance_hits(p3_prov_ok):
        print("    SELF-TEST FAIL: gate 3 provenance check flags the CORRECT form")
        fails.append(("gate 3 provenance false positive", "clean probe flagged"))

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
        # Chart scripts are in scope as a CLASS, not one at a time. Fixing
        # invested_periods removes the accidental filter that kept
        # benchmark-truncated legs out of ranked comparisons, so every file
        # gating on it has to read window_truncated too -- 42 of them. An
        # enumerated allow-list here would have to be edited in lockstep with
        # gate 12 and would say less than gate 12 already says: gate 12 checks
        # the PREDICATE in every one of these files, which is a tighter
        # constraint than naming the files.
        or p.endswith("/generate_charts.py")
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
        # Anchored on the RATIO, not on the label. It used to require the
        # literal phrase "Cash periods: ", which sector-pe-compression spells
        # "SPY periods (no compression): " -- so the last surviving instance of
        # the shape sat in the corpus with this gate green over it. A label is
        # free to be reworded; the denominator is what this gate is about.
        if re.search(r"\{cash_periods\} / \{len\(valid\)\}", src):
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
        # Read-only scanner. Its only accounting-shaped dicts are the synthetic
        # in-memory fixtures the truncation detector's self-test is built from,
        # which exist precisely so that detector is not tested solely against
        # the corpus it scans. It never writes a results record.
        "scripts/scan_results_invariant.py":
            "synthetic self-test fixtures; the scanner writes nothing",
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
    # Shape 1, the run_all summary printers: `cash_periods` is bound to `cp` on
    # an earlier line, so a pattern anchored on the literal never reaches the
    # division. 52 of the 68 denominators are this shape.
    PRINTER = re.compile(
        r'cp\s*=\s*r\.get\("cash_periods".*?\n.*?cp\s*\*\s*100\s*/\s*'
        r'(?:n|n_periods|n_years)\b', re.S)
    # Shape 2, chart files that divide inline by a named metrics-window key.
    INLINE = re.compile(
        r'cash_periods.{0,80}?/\s*(?:max\()?\s*(?:\w+\.get\(|\w+\[)?\s*'
        r'["\']?(?:n_periods|n_years)\b', re.S)
    # Shape 3, chart files that bind the metrics window to a local `n` first and
    # then divide by it. Missed by both patterns above, and it is how eight of
    # the generate_charts.py files were written.
    LOCAL_N = re.compile(
        r'n\s*=\s*\w+\.get\(\s*["\'](?:n_periods|n_years)["\'].*?\n.*?'
        r'cash_periods.{0,60}?/\s*n\b', re.S)
    # Shape 4, an f-string that prints "cash/window" as a RATIO rather than
    # dividing. Missed by all three patterns above: there is no arithmetic to
    # anchor on, and the `{r['` between the slash and n_periods defeats INLINE's
    # bounded gap. Both run_all_exchanges.py comparison tables were written this
    # way and stayed wrong while gate 9 reported 324/324 clean.
    # The negative lookahead lets the correct form through: a divisor that reads
    # `total_rebalances` first and falls back to n_periods for pre-B006 files is
    # right, and flagging it would make the gate unfixable-by-fixing.
    FSTRING_RATIO = re.compile(
        r"cash_periods.{0,40}?\}\s*/\s*\{(?![^}]*total_rebalances)"
        r"[^}]{0,60}?n_periods\b")
    bad9 = []
    for f in py_files:
        src = f.read_text()
        rel = str(f.relative_to(ROOT))
        # This file carries the shape-4 probe literals below, so without the
        # exclusion the gate flags its own self-test. (Gate 11 hit the same
        # trap on its anchor constant.) Everything else in scripts/ is still
        # scanned.
        if FSTRING_RATIO.search(src) and rel != "scripts/verify_floor_guard.py":
            bad9.append((rel, "f-string pairs the cash count with the metrics "
                              "window as a ratio"))
        if PRINTER.search(src):
            bad9.append((rel, "summary printer divides cash by the metrics window"))
        if INLINE.search(src):
            bad9.append((rel, "inline cash pct divides by the metrics window"))
        if LOCAL_N.search(src):
            bad9.append((rel, "cash pct divides by a local bound to the "
                              "metrics window"))
    print(f"9 DENOMINATOR {len(py_files) - len(set(b[0] for b in bad9))}"
          f"/{len(py_files)} files free of metrics-window cash denominators")
    for b in bad9:
        print("    WRONG DENOMINATOR:", b)
    fails += [("cash denominator", b) for b in bad9]
    # SELF-TEST for shape 4. It is the newest and the most regex-fragile of the
    # four (a negative lookahead plus two bounded gaps), and a pattern that
    # matches nothing is indistinguishable from a pattern that finds nothing.
    # Positive: the exact line both run_all comparison tables shipped with.
    # Negative: the fixed form, which must be allowed through or the gate could
    # not be satisfied by fixing the code.
    p9_bad = ("f\"{p['max_drawdown']:>7.1f}% "
              "{r['cash_periods']:>5}/{r['n_periods']:<1} \"")
    p9_ok = ("f\"{r['cash_periods']:>5}"
             "/{r.get('total_rebalances') or r['n_periods']:<1} \"")
    if not FSTRING_RATIO.search(p9_bad):
        print("    SELF-TEST FAIL: gate 9 shape 4 no longer flags the "
              "cash/n_periods f-string ratio it was added for")
        fails.append(("gate 9 shape 4 vacuous", "probe not flagged"))
    if FSTRING_RATIO.search(p9_ok):
        print("    SELF-TEST FAIL: gate 9 shape 4 flags the corrected form; "
              "the gate cannot be satisfied by fixing the code")
        fails.append(("gate 9 shape 4 false positive", "clean probe flagged"))

    # ---- gate 10: every call to the shared helper actually BINDS the callee
    #
    # This gate exists because the class it catches shipped. qarp/backtest.py
    # called period_accounting() while importing only
    # `compute_metrics as _compute_metrics, compute_annual_returns,
    # format_metrics` -- qarp is the one topic that ALIASES the metrics import,
    # so a mechanical edit keyed on the common `from metrics import
    # compute_metrics, ...` shape skipped it. Every gate above passed with that
    # defect live: the module IMPORTS clean (gate 2) because the call sits
    # inside build_output, which main() reaches only under `if args.output:`.
    # So the backtest ran to completion and then died with NameError while
    # writing its record -- after the work, discarding the results.
    #
    # Why the existing dynamic harnesses could not have caught it:
    # test_build_output_synthetic.py only drives the canonical 9-arg signature,
    # and test_noncanonical_build_output.py's binding check collects names from
    # a Call's `.args` and `.keywords` -- the ARGUMENTS -- and never inspects
    # the callee itself. 13 topics are executed by neither. A static gate over
    # every call site is the only check that reaches all of them.
    # entry_buyable/entry_buyable_prices are in here for the same reason: the
    # B005 correction added one call and one import to each of 66 files, and a
    # missed import is invisible to every other gate until a real run dies. Gate
    # 2 does not catch it (the call is inside run_backtest), and the dynamic
    # harness cannot reach graham-timing, sector-momentum or sector-rotation at
    # all. This static walk is the only check that covers all 66.
    HELPERS = {"period_accounting", "warn_if_truncated",
               "entry_buyable", "entry_buyable_prices"}

    def bound_names(tree):
        """Names bound at any scope in the file: imports, defs, assignments.

        Deliberately scope-blind. A name bound anywhere is good enough to prove
        the callee is not simply absent, which is the defect class here; a
        genuine scoping error would surface as a NameError under the dynamic
        harnesses instead.
        """
        out = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.ImportFrom):
                out.update(a.asname or a.name for a in n.names)
            elif isinstance(n, ast.Import):
                out.update((a.asname or a.name).split(".")[0] for a in n.names)
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef,
                                ast.ClassDef)):
                out.add(n.name)
            elif isinstance(n, ast.Assign):
                out.update(t.id for t in n.targets if isinstance(t, ast.Name))
            elif isinstance(n, (ast.AnnAssign, ast.NamedExpr)):
                if isinstance(n.target, ast.Name):
                    out.add(n.target.id)
            elif isinstance(n, ast.arg):
                out.add(n.arg)
        return out

    def unbound_helper_calls(tree):
        """(callee, lineno) for bare-name helper calls whose name is unbound.

        Attribute calls (`metrics.period_accounting(...)`) are not bare Names
        and are correctly ignored: their binding is the module import, which
        gate 2 already proves.
        """
        names = bound_names(tree)
        return [(n.func.id, n.lineno) for n in ast.walk(tree)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id in HELPERS and n.func.id not in names]

    # Root .py files host the helper itself, so scan them too. Pre-filter on
    # text: four pre-existing screen.py files (deleveraging, high-yield-quality,
    # low-debt, revenue-accel) do not parse under this Python's ast, are
    # untouched by this branch, and contain no helper call -- verified. Without
    # the pre-filter their SyntaxError would have to be swallowed, which is how
    # a gate goes quietly blind.
    bind_files = [f for f in sorted(ROOT.glob("*/*.py")) + sorted(ROOT.glob("*.py"))
                  if any(h in f.read_text() for h in HELPERS)]
    bad10, call_sites = [], 0
    for f in bind_files:
        try:
            tree = ast.parse(f.read_text())
        except SyntaxError as e:
            bad10.append((str(f.relative_to(ROOT)), 0, f"does not parse: {e}"))
            continue
        call_sites += sum(1 for n in ast.walk(tree)
                          if isinstance(n, ast.Call)
                          and isinstance(n.func, ast.Name)
                          and n.func.id in HELPERS)
        for name, lineno in unbound_helper_calls(tree):
            bad10.append((str(f.relative_to(ROOT)), lineno,
                          f"calls {name}() but never binds the name -- "
                          "NameError at write time"))
    print(f"10 BINDING {call_sites - len(bad10)}/{call_sites} helper call sites "
          f"bind their callee ({len(bind_files)} files scanned)")
    for b in bad10:
        print("    UNBOUND CALLEE:", b)
    fails += [("unbound callee", b) for b in bad10]
    # SELF-TEST, positive control. A binding check that collects the wrong node
    # set passes vacuously on a clean tree and looks identical to a working one.
    # That is exactly how test_noncanonical_build_output.py's version went
    # blind. So feed the detector the qarp defect verbatim and require a flag.
    probe10 = ast.parse(
        "from metrics import compute_metrics as _compute_metrics, "
        "format_metrics\ndef build_output(results, valid, c):\n"
        "    return {**period_accounting(results, valid, c)}\n")
    if not unbound_helper_calls(probe10):
        print("    SELF-TEST FAIL: the detector no longer flags an unbound "
              "period_accounting() call; gate 10 is passing vacuously")
        fails.append(("gate 10 vacuous", "probe not flagged"))
    # And the negative control: a correctly-imported call must NOT be flagged,
    # or the gate is a tripwire that fires on everything and gets disabled.
    probe10_ok = ast.parse(
        "from metrics import period_accounting, warn_if_truncated\n"
        "x = period_accounting(a, b, c)\nwarn_if_truncated(x)\n")
    if unbound_helper_calls(probe10_ok):
        print("    SELF-TEST FAIL: gate 10 flags a correctly-imported call")
        fails.append(("gate 10 false positive", "clean probe flagged"))
    # MEASURED, not guessed: 170 call sites at this tip (101 period-accounting
    # plus the 66 entry_buyable* guards and the helper definitions themselves).
    # The floor sits a little under that so a topic being retired does not fail
    # the gate, while a walk that stops matching still does.
    if call_sites < 150:
        print(f"    SELF-TEST FAIL: only {call_sites} helper call sites found; "
              "the AST walk has probably stopped matching")
        fails.append(("gate 10 coverage", call_sites))

    # ---- gate 11: the measured window has a READER, not just a writer
    #
    # Defect (b) is a labelling failure, so writing window_label into the JSON
    # fixes nothing on its own -- a field nothing reads changes no heading. The
    # provenance block shipped with exactly one consumer in the whole repo, and
    # that one was a DEAD STORE (nse_arena/framework.py collected window_label
    # into its row dict and print_comparison never emitted it). Meanwhile the
    # stderr warning, the only live channel, was captured and discarded by
    # scripts/batch_rerun_diff.py -- the tool the re-run campaign uses to build
    # its log. Both are now wired, and this gate keeps them wired.
    #
    # Anchored on the summary printers because that table is where a blog or
    # README window actually gets copied from. It prints to STDOUT while the
    # warning goes to STDERR, so a `> log.txt` re-run keeps the numbers and
    # loses the caveat unless the note is in the table itself.
    PRINTER_ANCHOR = "cash_pct = round(cp * 100 / tr, 0) if tr > 0 else 0"
    bad11, printers = [], 0
    for f in py_files:
        src = f.read_text()
        rel = str(f.relative_to(ROOT))
        # Skip this file: it names the anchor as a literal, so without the
        # exclusion the gate counts itself as a 55th printer and passes on its
        # own comment text. Found by the count reading 55 against a known 54.
        if PRINTER_ANCHOR not in src or rel.startswith("scripts/"):
            continue
        printers += 1
        if "window_truncated" not in src or "window_label" not in src:
            bad11.append((rel, "summary printer does not surface the measured "
                               "window; a truncated leg prints its cash rate "
                               "with nothing to say the window was cut"))
    print(f"11 PROVENANCE {printers - len(bad11)}/{printers} summary printers "
          "surface the measured window")
    for b in bad11:
        print("    NO WINDOW NOTE:", b)
    fails += [("window note missing", b) for b in bad11]
    # The two single-file readers, checked by name because each is a distinct
    # channel and losing either is silent.
    READERS = {
        "scripts/batch_rerun_diff.py": (
            'ln.startswith("WARNING ")',
            "the re-run campaign's log would swallow every truncation warning"),
        "nse_arena/framework.py": (
            "MEASURED {r['window_label']}",
            "window_label would go back to being a dead store"),
    }
    for rel, (needle, why) in READERS.items():
        p = ROOT / rel
        if not p.exists() or needle not in p.read_text():
            print(f"    READER LOST: {rel} no longer contains {needle!r} -- {why}")
            fails.append(("provenance reader lost", rel))
    if printers < 50:
        print(f"    SELF-TEST FAIL: only {printers} summary printers found; "
              "the anchor has probably drifted")
        fails.append(("gate 11 coverage", printers))

    # ---- gate 12: chart inclusion must read the measured window
    #
    # Fixing invested_periods removes an ACCIDENTAL filter. `invested_periods
    # > 0` is the standard "did this leg produce data" gate in 42 chart files.
    # Under the old arithmetic a benchmark-truncated leg produced a NEGATIVE
    # invested_periods and silently failed it. Under
    # `invested_periods = total_rebalances - cash_periods` it is always >= 0, so
    # the leg re-enters -- measured over roughly half the window its siblings are
    # measured over, under a footer still claiming the full span. Seven legs flip
    # on the committed corpus (deleveraging, ev-ebitda, oversold-quality,
    # relative-strength, value-momentum, volume-confirmed-momentum, yield-gap),
    # all OSL, and the direction is flattering: max drawdown roughly half the
    # sibling median in every one, CAGR above the sibling median in six.
    #
    # So chart inclusion now reads window_truncated as well. That is not a new
    # editorial choice -- it reproduces deliberately what the old arithmetic did
    # by accident. Whether a truncated leg should instead appear WITH its
    # measured window annotated on the bar is a real editorial question and it
    # stays open for a human; excluding is the conservative default.
    #
    # These files json.load at module scope, so this gate is static. Importing
    # them would execute a file read on a clean checkout.
    def _mentions(node, name):
        return any(isinstance(n, ast.Constant) and n.value == name
                   for n in ast.walk(node))

    def chart_gate_hits(tree):
        """Gating expressions that read invested_periods but not window_truncated.

        Descends tracking the NEAREST enclosing function rather than walking the
        module and every function separately: the latter collects each condition
        twice, once with no function in scope, and the helper exemption below
        then never applies.
        """
        out = []

        def visit(node, fn):
            if isinstance(node, ast.FunctionDef):
                fn = node
            conds = []
            if isinstance(node, ast.comprehension):
                conds += node.ifs
            elif isinstance(node, ast.If):
                conds.append(node.test)
            elif isinstance(node, ast.Return) and node.value is not None:
                conds.append(node.value)
            for c in conds:
                if _mentions(c, "invested_periods") and not _mentions(c, "window_truncated"):
                    # A predicate helper (deleveraging's is_clean,
                    # interest-coverage's) may reject the truncated leg on an
                    # earlier line of its own body instead of inline.
                    if fn is not None and _mentions(fn, "window_truncated"):
                        continue
                    out.append(c.lineno)
            for ch in ast.iter_child_nodes(node):
                visit(ch, fn)

        visit(tree, None)
        return sorted(set(out))

    bad12, charts = [], 0
    for p in sorted(ROOT.glob("*/generate_charts.py")):
        src = p.read_text()
        if "invested_periods" not in src:
            continue
        charts += 1
        for lineno in chart_gate_hits(ast.parse(src)):
            bad12.append((f"{p.parent.name}/generate_charts.py", lineno))
    print(f"12 CHARTS     {charts - len({f for f, _ in bad12})}/{charts} chart files "
          "gate inclusion on window_truncated as well as invested_periods")
    for b in bad12:
        print("    TRUNCATED LEG COULD RE-ENTER:", b)
    fails += [("chart gate ignores window_truncated", b) for b in bad12]
    # SELF-TEST, both directions. A detector that matches nothing looks exactly
    # like a corpus with nothing wrong in it.
    p12_bad = ast.parse('x = [k for k, v in d.items() if v["invested_periods"] > 0]\n')
    if not chart_gate_hits(p12_bad):
        print("    SELF-TEST FAIL: gate 12 no longer flags a bare invested_periods gate")
        fails.append(("gate 12 probe", "bare gate not flagged"))
    p12_ok = ast.parse('x = [k for k, v in d.items() if v["invested_periods"] > 0\n'
                       '     and not v.get("window_truncated", False)]\n')
    if chart_gate_hits(p12_ok):
        print("    SELF-TEST FAIL: gate 12 flags the corrected chart gate")
        fails.append(("gate 12 false positive", "clean probe flagged"))
    if charts < 40:
        print(f"    SELF-TEST FAIL: only {charts} chart files found; anchor drifted")
        fails.append(("gate 12 coverage", charts))

    # ---- gate 13: the entry funnel is present, entry-knowable and decodable
    #
    # B005 corrected which count the floor is compared against. It left the
    # decision UNAUDITABLE: a committed cash period says `stocks_held: 0` and
    # nothing else, so pre-screen cash and post-price cash are the same record.
    # Five of the six topics where the guard actually fires serialize only the
    # aggregate cash_periods, which lumps the two together, so the class could
    # not be measured from published artifacts at all -- only by git forensics
    # or a re-run. Three keys per period record close that at zero compute cost.
    #
    # `min_stocks` is in the record because the funnel alone cannot separate
    # guard-fired cash from a ZERO-SURVIVOR invested period. Correcting the
    # guard made the latter reachable (S6 in scripts/test_floor_guard.py) and it
    # also records stocks_held 0. Measured on the harness: S1 writes
    # buyable=3 floor=10, S6 writes buyable=15 floor=10, and only the floor
    # tells them apart. The floor also VARIES -- 5 for dogs-of-dow,
    # 52-week-low, graham-net-net and oversold-quality, 8 for trending-value,
    # 10 elsewhere -- so no scanner can supply one itself.
    bad13, records = [], 0
    for t in floor_topics:
        tree = parse(t)
        records += sum(1 for _ in period_dicts(tree))
        for lineno, why in funnel_hits(tree):
            bad13.append((t, lineno, why))
    print(f"13 FUNNEL     {len(floor_topics) - len({t for t, _, _ in bad13})}"
          f"/{len(floor_topics)} floor topics carry a valid entry funnel "
          f"({records} period records)")
    for b in bad13:
        print("    FUNNEL:", b)
    fails += [("entry funnel", b) for b in bad13]

    # SELF-TESTS. Each probe is a mistake someone could actually make, and each
    # must be caught by a DIFFERENT check inside funnel_hits -- a gate with one
    # live check and four dead ones passes this block just as well.
    _P = ("MIN_STOCKS = 10\ndef f(d, symbols, e):\n"
          "    clean, skipped = filter_returns(d)\n"
          "    buyable = entry_buyable(d)\n"
          "    r = []\n    r.append({%s})\n")

    def probe(body):
        return funnel_hits(ast.parse(_P % body))

    # positive controls
    for label, body in (
        ("no funnel keys at all",
         '"stocks_held": 0'),
        ("entry_buyable counted off the POST-FILTER book (B005 inside the "
         "audit field)",
         '"stocks_held": 0, "screened": 30, "entry_buyable": len(clean), '
         '"min_stocks": MIN_STOCKS'),
        ("min_stocks written as a literal, free to drift from the constant",
         '"stocks_held": 0, "screened": 30, "entry_buyable": buyable, '
         '"min_stocks": 10'),
        ("screened counted off the post-filter book",
         '"stocks_held": 0, "screened": len(clean), "entry_buyable": buyable, '
         '"min_stocks": MIN_STOCKS'),
        ("screened null while entry_buyable claims a count",
         '"stocks_held": 0, "screened": None, "entry_buyable": buyable, '
         '"min_stocks": MIN_STOCKS'),
    ):
        if not probe(body):
            print(f"    SELF-TEST FAIL: gate 13 no longer flags {label}")
            fails.append(("gate 13 probe", label))
    # negative controls: both accepted spellings must pass, or the gate cannot
    # be satisfied by writing the field correctly and gets deleted rather than
    # fixed. The second is capex-efficiency's, which counts inline because its
    # no-prices branch runs before `buyable` is bound.
    for label, body in (
        ("the bound-name form",
         '"stocks_held": 0, "screened": 30, "entry_buyable": buyable, '
         '"min_stocks": MIN_STOCKS'),
        ("the inline entry_buyable_prices() form",
         '"stocks_held": 0, "screened": 30, '
         '"entry_buyable": entry_buyable_prices(symbols, e), '
         '"min_stocks": MIN_STOCKS'),
        ("the never-screened form, both counts null",
         '"stocks_held": 0, "screened": None, "entry_buyable": None, '
         '"min_stocks": MIN_STOCKS'),
    ):
        if probe(body):
            print(f"    SELF-TEST FAIL: gate 13 flags {label}, which is CORRECT")
            fails.append(("gate 13 false positive", label))
    # Coverage. 64 topics record 3 period dicts and 2 record 4
    # (capex-efficiency's no-prices branch, cyclical-timing's signal-off
    # branch), so the anchor should find 200. A walk that stops matching
    # reports a clean corpus, which is what this catches.
    if records < 190:
        print(f"    SELF-TEST FAIL: only {records} period records found; the "
              "`stocks_held` anchor has drifted and the gate is near-vacuous")
        fails.append(("gate 13 coverage", records))

    print()
    print("ALL GATES PASS" if not fails else f"GATE FAILURES: {len(fails)}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
