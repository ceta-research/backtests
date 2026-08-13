"""Drop the stale "benchmark in USD" clause from chart footers.

After the local-benchmark migration most exchanges are measured against their
own index, denominated in the same currency as the returns, so
"(returns in GBP, benchmark in USD)" is wrong twice over: the benchmark is the
FTSE 100, and it is in GBP.

NOT a blanket regex. Some exchanges have no local index in
LOCAL_INDEX_BENCHMARKS and genuinely ran against SPY (JNB, MIL, WSE, SAU, KLS,
JKT, TLV, PAR, AMS, BME). For those the footer is CORRECT and must stay, so
each footer is resolved against what that topic's own results actually recorded.

Dry-run by default, --apply to write.
"""
import glob
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from chart_utils import benchmark_label, RESULT_KEY_TO_EXCHANGE  # noqa: E402
from data_utils import LOCAL_INDEX_BENCHMARKS, LOCAL_INDEX_NAMES  # noqa: E402

FOOTER = re.compile(r'"([^"]*?)\s*\(returns in ([A-Z]{3}), benchmark in USD\)"')


def results_for(topic):
    """Best available keyed results dict for a topic."""
    f = f"{ROOT}/{topic}/results/exchange_comparison.json"
    if os.path.exists(f):
        try:
            d = json.load(open(f))
            if isinstance(d, dict):
                return d
        except Exception:
            pass
    # fall back to merging per-exchange files on their `universe` key
    merged = {}
    for p in glob.glob(f"{ROOT}/{topic}/results/*.json"):
        try:
            d = json.load(open(p))
        except Exception:
            continue
        if isinstance(d, dict) and "universe" in d:
            merged[str(d["universe"])] = d
    return merged


def resolve(data, label):
    """Benchmark name for a footer's exchange label, or None if unknown.

    Must mirror data_utils.get_local_benchmark, which is what the backtest
    actually used: collect the index symbol for every exchange in the run and
    use it only if all of them agree on exactly one. A component with no local
    index contributes nothing rather than forcing a fallback.

    That distinction is load-bearing for multi-exchange footers. "SHZ + SHH"
    looks USD-benchmarked if you resolve SHZ alone (Shenzhen has no entry in
    LOCAL_INDEX_BENCHMARKS), but the combined run is measured against the SSE
    Composite because SHH supplies the only symbol. "TAI + TWO" is the same
    shape, with both components agreeing on TAIEX.
    """
    # An exact key in the results wins: its recorded benchmark_name is ground truth.
    for c in (label, label.replace(" + ", "_"), label.replace(" + ", "+")):
        if c in data:
            return benchmark_label(data, c)

    parts = [p.strip() for p in label.split("+")] if "+" in label else [label]
    symbols, names = set(), {}
    for p in parts:
        ex = RESULT_KEY_TO_EXCHANGE.get(p, p)
        sym = LOCAL_INDEX_BENCHMARKS.get(ex)
        if sym:
            symbols.add(sym)
            names[sym] = LOCAL_INDEX_NAMES.get(sym, sym)
    if len(symbols) == 1:
        return names[symbols.pop()]
    return None                            # none, or genuinely mixed -> SPY


def patch(topic, apply=False):
    path = f"{ROOT}/{topic}/generate_charts.py"
    if not os.path.exists(path):
        return topic, "no generate_charts.py", 0, 0
    src = open(path).read()
    data = results_for(topic)
    changed = kept = 0

    def sub(m):
        nonlocal changed, kept
        prefix, ccy = m.group(1), m.group(2)
        bench = resolve(data, prefix)
        if bench is None or bench == "S&P 500":
            kept += 1                      # genuinely USD-benchmarked, leave it
            return m.group(0)
        changed += 1
        return f'"{prefix} (returns in {ccy})"'

    new = FOOTER.sub(sub, src)
    if apply and new != src:
        open(path, "w").write(new)
    return topic, ("patched" if changed else "no change"), changed, kept


TOPICS = ["ev-ebitda", "oversold-quality", "ev-ebitda-relative", "pe-mean-revert",
          "qarp", "price-to-book", "pe-compression", "price-to-sales",
          "working-capital"]

if __name__ == "__main__":
    apply = "--apply" in sys.argv
    tc = tk = 0
    print(f"{'TOPIC':<24}{'STATUS':<12}{'FIXED':>7}{'KEPT (really USD)':>20}")
    print("-" * 64)
    for t in TOPICS:
        n, s, c, k = patch(t, apply=apply)
        tc += c
        tk += k
        print(f"{n:<24}{s:<12}{c:>7}{k:>20}")
    print(f"\n{tc} footers {'rewritten' if apply else 'would be rewritten'}, "
          f"{tk} left alone because the benchmark really is in USD")
    if not apply:
        print("DRY RUN, nothing written")
