"""Classify chart benchmark bugs by severity across all backtest topics.

Two severities:
  WRONG_SERIES - a regional chart plots a benchmark series taken from a
                 hardcoded US exchange key, so the line itself is the wrong
                 data (readers of the India blog see a US index).
  MISLABEL     - the series is the charted exchange's own benchmark, but the
                 legend/title calls it "S&P 500".

Affected = the topic's results prove at least one non-US exchange was measured
against a non-SPY benchmark (its stored benchmark series differs from the US
one, or it carries an explicit non-SPY benchmark_name).
"""
import ast
import glob
import json
import os
import re
import sys

ROOT = "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests"
US_KEYS = {"US_MAJOR", "NYSE_NASDAQ_AMEX", "US", "NYSE", "NASDAQ", "NYSE_NASDAQ"}
# Benchmark-awareness is spelled at least eight different ways across topics.
# Every spelling missing from this list becomes a FALSE POSITIVE: the topic is
# already correct but gets reported MISLABEL, and "fixing" it wastes a rerun.
# graham-net-net and earnings-consistency were both misreported for exactly
# this reason (BENCHMARK_LABELS / bench_label were absent).
AWARE = re.compile(
    r"benchmark_name|benchmark_label|BENCHMARK_LABELS|bench_label"
    r"|BENCH_NAMES|bench_name|BENCHMARK_NAMES|local_bench|benchmark_from")
HARDCODED_LABEL = re.compile(r'["\']S&P 500|["\']SPY["\']|label\s*=\s*["\']SPY')


# Confirmed legitimate by hand: both branch deliberately between a local
# benchmark for single-exchange charts and SPY as a cross-market reference.
# Listed here so the classifier can reach a clean zero instead of a standing
# count that everyone has to remember to ignore.
REVIEW_ALLOWLIST = {"fcf-compounders", "high-yield-quality"}


def bench_series(entry):
    ar = entry.get("annual_returns") or []
    return tuple(round(a.get("spy", 0), 2) for a in ar)


def comparison_is_stale(topic):
    """Is the file generate_charts.py reads a pre-local-benchmark run?

    Returns (stale, n_local, n_identical). A topic can carry benchmark_name
    stamps that were backfilled by hand onto results whose series were never
    re-run (commit 9f4303f did exactly that to income-quality and
    sustained-roic). On those, "S&P 500" is the TRUTHFUL label and relabelling
    prints a local index over a genuine S&P 500 line, which is worse than
    leaving it alone. So the series decides, never the stamp.

    One identical series is normal and not evidence of staleness: JNB has no
    entry in LOCAL_INDEX_BENCHMARKS and genuinely ran against SPY. The test is
    whether US-identical series OUTNUMBER genuinely local ones.
    """
    f = f"{ROOT}/{topic}/results/exchange_comparison.json"
    if not os.path.exists(f):
        return False, 0, 0
    try:
        d = json.load(open(f))
    except Exception:
        return False, 0, 0
    if not isinstance(d, dict):
        return False, 0, 0
    us = next((bench_series(v) for k, v in d.items()
               if isinstance(v, dict) and k in US_KEYS), None)
    if not us:
        return False, 0, 0
    local = ident = 0
    for k, v in d.items():
        if not isinstance(v, dict) or k in US_KEYS:
            continue
        s = bench_series(v)
        if not s:
            continue
        n = min(len(s), len(us))
        if s[:n] == us[:n]:
            ident += 1
        else:
            local += 1
    return (ident >= max(local, 1) and ident > 0), local, ident


def topic_is_affected(topic):
    """True if some non-US exchange was measured against a non-SPY benchmark."""
    names, differs = set(), set()
    for rf in glob.glob(f"{ROOT}/{topic}/results/*.json"):
        try:
            d = json.load(open(rf))
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        entries = [d] if "universe" in d else [v for v in d.values() if isinstance(v, dict)]
        for v in entries:
            bn = v.get("benchmark_name") or v.get("benchmark")
            if isinstance(bn, str) and bn not in ("S&P 500", "SPY"):
                names.add(bn)
        if "universe" in d:
            continue
        us = next((bench_series(v) for k, v in d.items()
                   if isinstance(v, dict) and k in US_KEYS), None)
        if us:
            for k, v in d.items():
                if isinstance(v, dict) and k not in US_KEYS:
                    s = bench_series(v)
                    if s and len(s) == len(us) and s != us:
                        differs.add(k)
    return names or differs, sorted(names)[:4]


def chart_functions(tree, src):
    """Yield (name, source) for functions that draw a per-exchange chart."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("chart_"):
            if "comparison" in node.name:      # comparison charts legitimately show SPY
                continue
            yield node.name, ast.get_source_segment(src, node) or ""


def classify(topic):
    # Data first. A stale comparison file means the labels are currently
    # honest and the fix is a rerun, not an edit, whatever the code shape says.
    stale, n_local, n_ident = comparison_is_stale(topic)
    if stale:
        return "STALE_RESULTS", [f"{n_ident} of {n_local + n_ident} non-US series are "
                                 f"identical to the US one: rerun --global BEFORE relabelling"]
    path = f"{ROOT}/{topic}/generate_charts.py"
    src = open(path).read()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return "PARSE_ERROR", []
    # A per-charted-exchange benchmark lookup, e.g. data[exchanges[0]]["spy"]
    # or get_benchmark_cumulative(exchange_key). If a function has one of these
    # AND a hardcoded US ref, it is probably branching single vs multi exchange
    # deliberately (legitimate), so it needs eyeballing rather than condemning.
    # Must be a per-exchange BENCHMARK lookup. A bare data[ex_key] does not
    # count: that is how every script fetches the PORTFOLIO line.
    VAR = r"(exchanges\s*\[\s*0\s*\]|ex_key|exchange_key|ref_key|bench_key)"
    PER_EX = re.compile(
        r"data\s*\[\s*" + VAR + r"\s*\]\s*\[\s*[\"']spy[\"']\s*\]"
        r"|get_benchmark_cumulative\s*\(\s*\w"
        r"|get_spy_cumulative\s*\(\s*" + VAR
    )
    reasons, mixed = [], []
    for name, fsrc in chart_functions(tree, src):
        hits = []
        for uk in US_KEYS:
            if re.search(r'data\s*\[\s*["\']' + uk + r'["\']\s*\]', fsrc):
                hits.append(f"{name}: data['{uk}'] hardcoded")
        if re.search(r"get_spy_cumulative\(\s*\)", fsrc):
            hits.append(f"{name}: get_spy_cumulative() uses US default")
        if not hits:
            continue
        (mixed if PER_EX.search(fsrc) else reasons).extend(hits)
    if reasons:
        return "WRONG_SERIES", reasons
    if mixed:
        if topic in REVIEW_ALLOWLIST:
            return "OK", []
        return "REVIEW_BRANCHED", mixed
    if HARDCODED_LABEL.search(src) and not AWARE.search(src):
        return "MISLABEL", []
    if AWARE.search(src):
        return "OK", []
    return "NO_BENCH_LABEL", []


def main():
    out = {"WRONG_SERIES": [], "STALE_RESULTS": [], "REVIEW_BRANCHED": [], "MISLABEL": [],
           "OK": [], "NO_BENCH_LABEL": [], "PARSE_ERROR": []}
    for cg in sorted(glob.glob(f"{ROOT}/*/generate_charts.py")):
        topic = os.path.basename(os.path.dirname(cg))
        affected, names = topic_is_affected(topic)
        if not affected:
            continue
        verdict, reasons = classify(topic)
        out[verdict].append((topic, names, reasons))

    print(f"{'VERDICT':<16}{'TOPIC':<30}{'BENCHMARKS':<40}REASON")
    print("-" * 120)
    for verdict in ["WRONG_SERIES", "STALE_RESULTS", "REVIEW_BRANCHED", "MISLABEL",
                    "PARSE_ERROR", "NO_BENCH_LABEL", "OK"]:
        for topic, names, reasons in out[verdict]:
            r = reasons[0] if reasons else ""
            print(f"{verdict:<16}{topic:<30}{str(names)[:38]:<40}{r}")
    print()
    for k in out:
        print(f"{k}: {len(out[k])}")
    print(f"\nTOTAL AFFECTED: {sum(len(v) for v in out.values())}")
    print("\nFIX FIRST (wrong data, not just a label):")
    for topic, _, reasons in out["WRONG_SERIES"]:
        print(f"  {topic}")
        for r in reasons[:3]:
            print(f"      {r}")
    if out["STALE_RESULTS"]:
        print("\nDO NOT RELABEL THESE. Their charts read a pre-local-benchmark file, so")
        print("'S&P 500' is currently TRUE. Rerun --global first; a label edit here lies:")
        for topic, _, reasons in out["STALE_RESULTS"]:
            print(f"  {topic:<24}{reasons[0] if reasons else ''}")


if __name__ == "__main__":
    main()
