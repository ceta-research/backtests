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

SELF-TEST, baked in so the scanner cannot rot into a no-op: the pass condition is
that it flags EXACTLY the 22 known-bad nodes and clears every 52-week-low node.
Teeth and false-positive control in one assertion.

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


def main(argv):
    naive = "--naive" in argv
    hits, counts = [], {}
    files = sorted(ROOT.glob("*/results/*.json"))
    unreadable = []
    for f in files:
        try:
            data = json.loads(f.read_text())
        except Exception as e:
            unreadable.append((str(f.relative_to(ROOT)), str(e)[:60]))
            continue
        walk(data, "", str(f.relative_to(ROOT)), hits, counts, naive)

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
    flagged = {(rel, ptr) for rel, ptr, *_ in hits}
    ok = True

    missing = EXPECT_FLAGGED - flagged
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

    print()
    print("SELF-TEST PASS" if ok else "SELF-TEST FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
