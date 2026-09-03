#!/usr/bin/env python3
"""Scan the committed results corpus for arithmetically impossible period counts.

Blocker B006, verification LEG 3. Reads only; writes nothing. The results corpus
is gitignored-but-committed data and this task must not modify a byte of it, so an
UNCHANGED scan result is also the regression test for "no results file touched".

TWO REGIMES, because the corpus predates the convention:

  LEGACY (no `total_rebalances` key). Convention-free check ONLY: flag a negative
    invested_periods. Deliberately does NOT relate cash_periods to n_periods.
    That relation is the trap this whole blocker turns on -- see M5 below.

  B006 (`total_rebalances` present). Full invariant:
    cash + invested == total_rebalances,  and each of cash/invested/n_periods
    inside [0, total_rebalances].  n_periods is NEVER compared to cash_periods.

TWO DEFECTS, because the invariant check only sees one of them:

  (a) IMPOSSIBLE ARITHMETIC -- a negative invested_periods. The regimes above.
  (b) SILENT WINDOW TRUNCATION -- a leg measured over a fraction of its
      siblings' window. It leaves NO arithmetic trace whenever
      cash_periods <= n_periods, so (a)'s check is structurally blind to it and
      the topic renders in a ranked comparison under a footer claiming the full
      span. Detected separately by truncated_legs(): inside one
      exchange_comparison.json every leg shares a rebalance grid, so the file's
      maximum n_periods IS total_rebalances. Needs no new fields; works on
      legacy records. The two populations OVERLAP but neither contains the
      other -- 9 legs carry both, 9 carry only (b).

SELF-TEST, baked in so the scanner cannot rot into a no-op: the pass condition is
that it flags EXACTLY the 22 known-bad nodes and clears every 52-week-low node.
Teeth and false-positive control in one assertion. The truncation detector has
its own pair, against SYNTHETIC in-memory fixtures rather than corpus files, so
that it cannot silently degrade into agreeing with whatever the corpus says.

M5, THE FALSE-POSITIVE CONTROL (reproduced on real data during planning): swap the
legacy branch for the naive `i < 0 or c + i != n or c > n` and the scan flags 24
nodes -- the 22 true bads PLUS both 52-week-low OSL nodes (n=50, cash=84,
invested=11), which are CORRECT prior art from commit 3c39bcb. 84 > 50 fires
because cash is a full-window count and n_periods is a metrics-window count. They
are different populations on purpose.

Usage:  python scripts/scan_results_invariant.py [--naive]
        --naive runs mutation M5 and is expected to FAIL the self-test.
"""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# The 22 known-bad nodes at 037ad42, as (topic-relative path, json pointer).
# 13 distinct topic/universe pairs; exchange_comparison.json carries a duplicate
# of each topic's returns_OSL node, which is why nodes (22) exceed pairs (13).
EXPECT_FLAGGED = {
    ("deleveraging/results/exchange_comparison.json", "/OSL"),
    ("deleveraging/results/returns_OSL.json", ""),
    ("earnings-consistency/results/exchange_comparison.json", "/OSL"),
    ("earnings-consistency/results/returns_OSL.json", ""),
    ("etf-underowned/results/returns_OSL.json", ""),
    ("ev-ebitda-relative/results/exchange_comparison.json", "/OSL"),
    ("ev-ebitda-relative/results/returns_OSL.json", ""),
    ("ev-ebitda/results/exchange_comparison.json", "/OSL"),
    ("ev-ebitda/results/returns_OSL.json", ""),
    ("fcf-compounders/results/exchange_comparison.json", "/OSL"),
    ("fcf-compounders/results/returns_OSL.json", ""),
    ("fcf-yield/results/value-04-fcf-yield_norway.json", ""),
    ("garp/results/returns_OSL.json", ""),
    ("oversold-quality/results/exchange_comparison.json", "/OSL"),
    ("oversold-quality/results/oversold_quality_osl.json", ""),
    ("qarp/results/jnb_results.json", ""),
    ("relative-strength/results/exchange_comparison.json", "/OSL"),
    ("relative-strength/results/returns_OSL.json", ""),
    ("value-momentum/results/exchange_comparison.json", "/OSL"),
    ("value-momentum/results/returns_OSL.json", ""),
    ("volume-confirmed-momentum/results/exchange_comparison.json", "/OSL"),
    ("volume-confirmed-momentum/results/vcm_osl.json", ""),
}
# 13 topic/universe pairs, for the human-readable summary.
EXPECT_PAIRS = 13
# Measured at 037ad42: 20 nodes already carry total_rebalances (all 52-week-low:
# 17 universes inside exchange_comparison.json plus 3 returns_*.json). They all
# satisfy the B006 regime, so real data exercises it on day one.
EXPECT_B006_REGIME = 20


def check(rec, naive=False):
    n = rec.get("n_periods")
    c = rec.get("cash_periods")
    i = rec.get("invested_periods")
    tr = rec.get("total_rebalances")
    if not all(isinstance(v, int) and not isinstance(v, bool) for v in (n, c, i)):
        return None, None
    if naive:
        # MUTATION M5, and it must bypass regime detection to be honest. The
        # realistic mistake is a scanner written WITHOUT knowing that cash and
        # n_periods live in different populations -- it applies one naive rule
        # to every node. Gating the naive rule behind "no total_rebalances"
        # would make the mutation toothless here, because the prior art
        # (52-week-low) is precisely the data that already carries
        # total_rebalances. Verified: regime-gated M5 flags 22 and passes; this
        # ungated form flags 24 and correctly fails the self-test.
        regime = "b006" if isinstance(tr, int) else "legacy"
        if i < 0 or c + i != n or c > n:
            return regime, f"impossible (naive: n={n} c={c} i={i})"
        return regime, None
    if tr is None or not isinstance(tr, int):
        if i < 0:
            return "legacy", f"negative invested_periods ({i})"
        return "legacy", None
    if c + i != tr:
        return "b006", f"cash({c}) + invested({i}) != total_rebalances({tr})"
    if not 0 <= c <= tr:
        return "b006", f"cash_periods({c}) outside [0,{tr}]"
    if not 0 <= i <= tr:
        return "b006", f"invested_periods({i}) outside [0,{tr}]"
    if not 0 <= n <= tr:
        return "b006", f"n_periods({n}) > total_rebalances({tr})"
    return "b006", None


def walk(obj, ptr, rel, hits, counts, naive):
    if isinstance(obj, dict):
        if all(k in obj for k in ("n_periods", "cash_periods", "invested_periods")):
            regime, problem = check(obj, naive)
            if regime:
                counts[regime] = counts.get(regime, 0) + 1
                counts["nodes"] = counts.get("nodes", 0) + 1
                if problem:
                    hits.append((rel, ptr, problem, obj.get("n_periods"),
                                 obj.get("cash_periods"), obj.get("invested_periods"),
                                 obj.get("total_rebalances")))
        for k, v in obj.items():
            walk(v, f"{ptr}/{k}", rel, hits, counts, naive)


# ---------------------------------------------------------------------------
# DEFECT (b): silent window truncation.
#
# The invariant check above is structurally BLIND to this. It flags a negative
# invested_periods, which catches defect (a); truncation leaves no arithmetic
# trace at all whenever cash_periods <= n_periods, so a leg measured over half
# its siblings' window passes every check and renders in a ranked comparison
# under a footer claiming the full span.
#
# The detector needs no new fields and works on legacy records: inside one
# exchange_comparison.json every leg shares a rebalance grid, so the file's
# maximum n_periods IS total_rebalances. A leg materially below it was cut.
#
# 0.75, and >= 3 siblings: a two-leg file has no majority to compare against,
# and small differences are ordinary (a market that opened late, a delisting at
# the end). 0.75 is a blunt threshold chosen to surface HALF-window legs, which
# is the shape that actually misleads. A solo returns_*.json cannot express the
# comparison at all -- yield-gap is the live example, its returns_OSL.json says
# n=25 while its exchange_comparison node says n=11, and only the latter (the
# one the charts read) is truncated.
TRUNCATION_RATIO = 0.75
MIN_SIBLINGS = 3

# Every materially truncated leg in the corpus, as (path, pointer). All 18 are
# Norway (OSL), whose ^OSEAX benchmark has no price data before 2013.
#   9 also carry invested_periods < 0 and are already in EXPECT_FLAGGED above.
#   9 do not, and were invisible to every check in this repo until now. Of
#     those, 52-week-low is the correct prior art (it publishes
#     total_rebalances) and garp is already in the re-run population via its
#     returns_OSL.json, leaving SEVEN topics whose live comparison charts and
#     posts still show a 2013-2025 Norway bar among 2000-2025 siblings.
EXPECT_TRUNCATED = {
    # --- also flagged by the invariant check (defect (a) and (b) together) ---
    ("deleveraging/results/exchange_comparison.json", "/OSL"),
    ("earnings-consistency/results/exchange_comparison.json", "/OSL"),
    ("ev-ebitda-relative/results/exchange_comparison.json", "/OSL"),
    ("ev-ebitda/results/exchange_comparison.json", "/OSL"),
    ("fcf-compounders/results/exchange_comparison.json", "/OSL"),
    ("oversold-quality/results/exchange_comparison.json", "/OSL"),
    ("relative-strength/results/exchange_comparison.json", "/OSL"),
    ("value-momentum/results/exchange_comparison.json", "/OSL"),
    ("volume-confirmed-momentum/results/exchange_comparison.json", "/OSL"),
    # --- defect (b) ONLY: no arithmetic trace, nothing ever suppressed them ---
    ("52-week-high/results/exchange_comparison.json", "/OSL"),
    ("52-week-low/results/exchange_comparison.json", "/OSL"),   # correct prior art
    ("etf-rebalancing/results/exchange_comparison.json", "/OSL"),
    ("garp/results/exchange_comparison.json", "/OSL"),          # already in re-run set
    ("pe-compression/results/exchange_comparison.json", "/OSL"),
    ("peg-ratio/results/exchange_comparison.json", "/OSL"),
    ("price-momentum/results/exchange_comparison.json", "/OSL"),
    ("price-to-book/results/exchange_comparison.json", "/OSL"),
    ("yield-gap/results/exchange_comparison.json", "/OSL"),
}


def truncated_legs(data):
    """(pointer, n_periods, total) for every materially truncated leg in ONE file."""
    nodes = {k: v for k, v in data.items()
             if isinstance(v, dict) and "n_periods" in v and "error" not in v
             and isinstance(v.get("n_periods"), int)}
    if len(nodes) < MIN_SIBLINGS:
        return []
    mx = max(v["n_periods"] for v in nodes.values())
    if mx <= 0:
        return []
    return sorted((f"/{k}", v["n_periods"], mx) for k, v in nodes.items()
                  if v["n_periods"] < TRUNCATION_RATIO * mx)


# ---------------------------------------------------------------------------
# DEFECT (c): a per-period entry funnel that contradicts itself.
#
# B005's guard is now recorded per period: `screened`, `entry_buyable` and
# `min_stocks` sit beside `stocks_held` in every period record of every floor
# topic, so a committed cash period can finally be attributed to the screen or
# to the guard. See the block above period_state() in data_utils.py.
#
# CONDITIONAL, and that is deliberate. The committed corpus predates the fields
# entirely -- nothing here was re-run -- so REQUIRING them would fail all 1712
# results files and say nothing about anything. Records that carry the funnel
# get checked; records that do not are counted and skipped. The count is the
# honest progress meter for the re-run: it reads 0 today and rises as legs are
# regenerated.
#
# Consequence worth naming: this detector is VACUOUS against the corpus right
# now. Its self-test is therefore not a nicety, it is the only thing separating
# "no funnel record is broken" from "there are no funnel records". Same reason
# truncation_selftest() exists.
FUNNEL_KEYS = ("screened", "entry_buyable", "min_stocks")


def funnel_problem(rec):
    """The first way one period record's funnel contradicts itself, or None.

    Checks ORDER and NULL MONOTONICITY only. It deliberately does NOT assert
    "a cash record implies entry_buyable < min_stocks": capex-efficiency's
    no-prices branch reaches cash with a fully buyable book when only the EXIT
    leg is missing, and a zero-survivor period is invested with stocks_held 0.
    Both are correct records that a cash-semantics rule would flag.
    """
    if not all(k in rec for k in FUNNEL_KEYS):
        return None                                   # pre-funnel record
    sc, eb, fl = rec["screened"], rec["entry_buyable"], rec["min_stocks"]
    held = rec.get("stocks_held", rec.get("n_stocks"))
    if not isinstance(fl, int) or isinstance(fl, bool) or fl <= 0:
        return f"min_stocks={fl!r} is not a positive int"
    for name, v in (("screened", sc), ("entry_buyable", eb)):
        if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v < 0):
            return f"{name}={v!r} is neither null nor a non-negative int"
    if sc is None and eb is not None:
        return (f"screened is null but entry_buyable={eb}; a period that never "
                "screened cannot know how many names were buyable")
    if sc is not None and eb is not None and eb > sc:
        return f"entry_buyable({eb}) > screened({sc})"
    if eb is not None and isinstance(held, int) and held > eb:
        return f"stocks_held({held}) > entry_buyable({eb})"
    return None


def funnel_scan(obj, ptr, rel, hits, counts):
    """Walk any nested structure, checking every list under a `period_data` key."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "period_data" and isinstance(v, list):
                for i, rec in enumerate(v):
                    if not isinstance(rec, dict):
                        continue
                    if all(f in rec for f in FUNNEL_KEYS):
                        counts["funnel"] = counts.get("funnel", 0) + 1
                        problem = funnel_problem(rec)
                        if problem:
                            hits.append((rel, f"{ptr}/period_data/{i}", problem))
                    else:
                        counts["prefunnel"] = counts.get("prefunnel", 0) + 1
            funnel_scan(v, f"{ptr}/{k}", rel, hits, counts)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            funnel_scan(v, f"{ptr}/{i}", rel, hits, counts)


def funnel_selftest():
    """Teeth and false-positive control, on SYNTHETIC records.

    The corpus carries no funnel records at all today, so without this the
    detector reports a clean scan whether it works or not.
    """
    bad = []
    ok = {"stocks_held": 12, "screened": 30, "entry_buyable": 12, "min_stocks": 10}
    cases_bad = {
        "entry_buyable above screened":
            {**ok, "entry_buyable": 40},
        "stocks_held above entry_buyable":
            {**ok, "stocks_held": 20, "entry_buyable": 12},
        "screened null while entry_buyable claims a count":
            {**ok, "screened": None},
        "min_stocks not a positive int":
            {**ok, "min_stocks": "10"},
        "entry_buyable a bool rather than a count":
            {**ok, "entry_buyable": True},
    }
    for label, rec in cases_bad.items():
        if not funnel_problem(rec):
            bad.append(f"NOT detected: {label}")
    cases_ok = {
        "the ordinary invested record": ok,
        # The two records a cash-semantics rule would wrongly flag.
        "guard-fired cash (buyable below the floor)":
            {"stocks_held": 0, "screened": 30, "entry_buyable": 4, "min_stocks": 10},
        "entered, zero survivors (buyable above the floor, held 0)":
            {"stocks_held": 0, "screened": 30, "entry_buyable": 30, "min_stocks": 10},
        "pre-screen cash (never priced)":
            {"stocks_held": 0, "screened": 3, "entry_buyable": None, "min_stocks": 10},
        "never screened (signal off)":
            {"stocks_held": 0, "screened": None, "entry_buyable": None, "min_stocks": 10},
        "capex-efficiency's n_stocks spelling":
            {"n_stocks": 12, "screened": 30, "entry_buyable": 12, "min_stocks": 10},
    }
    for label, rec in cases_ok.items():
        problem = funnel_problem(rec)
        if problem:
            bad.append(f"FALSELY flagged {label}: {problem}")
    # A record without the fields must be SKIPPED, never flagged -- that is what
    # keeps the whole legacy corpus out of this check.
    if funnel_problem({"stocks_held": 0, "portfolio_return": 0.0}) is not None:
        bad.append("a pre-funnel record was flagged instead of skipped")
    return bad


def truncation_selftest():
    """Teeth and false-positive control, on SYNTHETIC fixtures.

    In-memory, not corpus files: the one results file this change was permitted
    to add is spent on the invariant check's positive control, and a detector
    whose only test is the corpus it scans cannot tell "nothing is wrong" from
    "I stopped working".
    """
    bad = []
    full = {"n_periods": 100, "cash_periods": 0, "invested_periods": 100}
    cut = {"n_periods": 40, "cash_periods": 0, "invested_periods": 40}
    if not truncated_legs({"A": full, "B": full, "OSL": cut}):
        bad.append("half-window leg among 3 siblings NOT detected")
    if truncated_legs({"A": full, "B": full, "C": full}):
        bad.append("uniform file falsely flagged")
    if truncated_legs({"A": full, "OSL": cut}):
        bad.append("2-leg file flagged despite no majority to compare against")
    if truncated_legs({"A": full, "B": full,
                       "OSL": {"n_periods": 90, "cash_periods": 0,
                               "invested_periods": 90}}):
        bad.append("a leg within the ratio was flagged")
    return bad


def main(argv):
    naive = "--naive" in argv
    hits, counts, truncated, fhits = [], {}, [], []
    files = sorted(ROOT.glob("*/results/*.json"))
    unreadable = []
    for f in files:
        try:
            data = json.loads(f.read_text())
        except Exception as e:
            unreadable.append((str(f.relative_to(ROOT)), str(e)[:60]))
            continue
        walk(data, "", str(f.relative_to(ROOT)), hits, counts, naive)
        funnel_scan(data, "", str(f.relative_to(ROOT)), fhits, counts)
        if f.name == "exchange_comparison.json" and isinstance(data, dict):
            rel = str(f.relative_to(ROOT))
            for ptr, n, mx in truncated_legs(data):
                truncated.append((rel, ptr, n, mx))

    print(f"files scanned        {len(files)}"
          + (f"  ({len(unreadable)} unreadable)" if unreadable else ""))
    print(f"accounting nodes     {counts.get('nodes', 0)}"
          f"   (legacy {counts.get('legacy', 0)}, "
          f"B006-regime {counts.get('b006', 0)})")
    passed = counts.get("nodes", 0) - len(hits)
    print(f"invariant PASS       {passed}")
    print(f"invariant FAIL       {len(hits)}")
    if naive:
        print("MODE                 --naive (mutation M5, expected to false-fire)")
    print()
    for rel, ptr, problem, n, c, i, tr in sorted(hits):
        print(f"  FAIL {rel}{ptr}")
        print(f"       n_periods={n} cash={c} invested={i} total_rebalances={tr}"
              f"  -- {problem}")

    print()
    print(f"truncated legs       {len(truncated)}"
          "   (defect (b): measured over <75% of the file's rebalance grid)")
    inv_flagged = {(rel, ptr) for rel, ptr, *_ in hits}
    for rel, ptr, n, mx in sorted(truncated):
        also = " [also fails the invariant]" if (rel, ptr) in inv_flagged else ""
        print(f"  TRUNC {rel}{ptr}  n_periods={n} of {mx}{also}")

    print()
    flagged = {(rel, ptr) for rel, ptr, *_ in hits}
    ok = True

    # An EXPECT_FLAGGED path that is absent from disk is a CORPUS problem, not a
    # detector problem, and the two must never print the same message. This fired
    # for real: volume-confirmed-momentum/results/vcm_osl.json is gitignored
    # (.gitignore `*/results/`) and existed only in the author's working tree, so
    # every clean checkout reported "the scanner lost its teeth" while the
    # detector was working perfectly and the count silently dropped from 22 to
    # 21. It is committed now (`git add -f`, the established pattern here -- 1712
    # results files are tracked the same way), so absence below means someone
    # deleted it or the checkout is partial.
    absent = sorted(p for p, _ in EXPECT_FLAGGED if not (ROOT / p).exists())
    if absent:
        ok = False
        print(f"CORPUS INCOMPLETE: {len(absent)} EXPECT_FLAGGED file(s) absent "
              "from this checkout. The scan population is not the one the "
              "self-test was calibrated against, so neither a PASS nor a FAIL "
              "above means anything. This is NOT a lost-teeth result.")
        for p in absent:
            print("    ", p)

    missing = {(p, ptr) for p, ptr in (EXPECT_FLAGGED - flagged) if p not in absent}
    extra = flagged - EXPECT_FLAGGED
    if missing:
        ok = False
        print(f"SELF-TEST FAIL: {len(missing)} known-bad node(s) NOT detected "
              "(the scanner lost its teeth):")
        for m in sorted(missing):
            print("    ", m)
    if extra:
        ok = False
        print(f"SELF-TEST FAIL: {len(extra)} node(s) flagged that are not known-bad "
              "(false positives):")
        for m in sorted(extra):
            print("    ", m)

    fw = sorted(r for r, _ in flagged if r.startswith("52-week-low/"))
    if fw:
        ok = False
        print("SELF-TEST FAIL: 52-week-low flagged. Its OSL leg (n=50, "
              "total=95, cash=84, invested=11) is CORRECT prior art from 3c39bcb.")
        for r in fw:
            print("    ", r)
    else:
        print("SELF-TEST ok: no 52-week-low node flagged "
              "(false-positive control holds)")

    b006 = counts.get("b006", 0)
    if b006 != EXPECT_B006_REGIME:
        print(f"SELF-TEST NOTE: {b006} nodes carry total_rebalances, "
              f"expected {EXPECT_B006_REGIME}. That is expected to RISE after a "
              "re-run; update EXPECT_B006_REGIME then.")
    else:
        print(f"SELF-TEST ok: {b006} nodes exercise the B006 regime, all pass")

    pairs = {r.split("/")[0] for r, _ in flagged}
    print(f"SELF-TEST {'ok' if len(pairs) == EXPECT_PAIRS else 'FAIL'}: "
          f"{len(pairs)} distinct topics flagged, expected {EXPECT_PAIRS}")
    if len(pairs) != EXPECT_PAIRS:
        ok = False

    # ---- defect (b) detector: teeth first, then the corpus expectation.
    tbad = truncation_selftest()
    if tbad:
        ok = False
        print(f"SELF-TEST FAIL: the truncation detector is broken ({len(tbad)}):")
        for b in tbad:
            print("    ", b)
    else:
        print("SELF-TEST ok: truncation detector passes 4 synthetic controls")

    tflagged = {(rel, ptr) for rel, ptr, *_ in truncated}
    t_missing = {(p, ptr) for p, ptr in (EXPECT_TRUNCATED - tflagged)
                 if p not in absent and (ROOT / p).exists()}
    t_extra = tflagged - EXPECT_TRUNCATED
    if t_missing:
        ok = False
        print(f"SELF-TEST FAIL: {len(t_missing)} known-truncated leg(s) NOT "
              "detected (the truncation detector lost its teeth):")
        for m in sorted(t_missing):
            print("    ", m)
    if t_extra:
        ok = False
        print(f"SELF-TEST FAIL: {len(t_extra)} newly truncated leg(s). These are "
              "NOT in the documented re-run population -- add them, or explain "
              "why the window shrank:")
        for m in sorted(t_extra):
            print("    ", m)
    if not (t_missing or t_extra):
        only_b = len(EXPECT_TRUNCATED - EXPECT_FLAGGED)
        print(f"SELF-TEST ok: {len(tflagged)} truncated legs, exactly the "
              f"documented set ({only_b} carry defect (b) ONLY and are invisible "
              "to the invariant check)")

    # ---- defect (c) detector: the per-period entry funnel.
    print()
    nf, npf = counts.get("funnel", 0), counts.get("prefunnel", 0)
    print(f"funnel records       {nf} checked, {npf} pre-funnel (skipped)")
    print(f"funnel FAIL          {len(fhits)}")
    for rel, ptr, problem in sorted(fhits)[:20]:
        print(f"  FUNNEL {rel}{ptr}  -- {problem}")
    fbad = funnel_selftest()
    if fbad:
        ok = False
        print(f"SELF-TEST FAIL: the funnel detector is broken ({len(fbad)}):")
        for b in fbad:
            print("    ", b)
    else:
        print("SELF-TEST ok: funnel detector passes 12 synthetic controls "
              "(5 teeth, 6 false-positive, 1 skip-the-legacy-corpus)")
    if fhits:
        ok = False
    if nf == 0:
        # A statement of fact, not a failure. It becomes one the moment a re-run
        # lands and this line does not move.
        print("NOTE: no committed record carries the entry funnel yet, so the "
              "corpus half of this check is vacuous. It stops being vacuous "
              "with the first re-run; the self-test above is what holds until "
              "then.")

    print()
    print("SELF-TEST PASS" if ok else "SELF-TEST FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
