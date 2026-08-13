"""Point per-exchange charts at the charted exchange's own benchmark.

For each target topic's generate_charts.py:
  * import chart_utils
  * parameterise get_spy_cumulative by exchange key
  * inside per-exchange chart functions, replace hardcoded US benchmark
    lookups and "S&P 500" labels with the charted exchange's own values

Comparison charts are left alone: their SPY reference line is legitimate.
"""
import ast
import re
import sys

ROOT = "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests"
US_KEYS = ["US_MAJOR", "NYSE_NASDAQ_AMEX", "NYSE_NASDAQ", "US"]

IMPORT_BLOCK = (
    "import os as _os, sys as _sys\n"
    "_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))\n"
    "from chart_utils import benchmark_label, benchmark_cumulative\n"
)


def key_expr(fsrc):
    """The variable naming the exchange being charted in this function."""
    for cand in ["exchanges[0]", "exchange_key", "ex_key", "ref_key", "bench_key"]:
        base = cand.split("[")[0]
        if re.search(r"\bdef \w+\([^)]*\b" + re.escape(base) + r"\b", fsrc):
            return cand
    return None


def patch_function(fsrc, key):
    out = fsrc
    # benchmark series must come from the charted exchange
    out = re.sub(r"get_spy_cumulative\(\s*\)", f"get_spy_cumulative({key})", out)
    for uk in US_KEYS:
        out = out.replace(f'data["{uk}"]', f"data[{key}]")
        out = out.replace(f"data['{uk}']", f"data[{key}]")
    # legend labels
    out = re.sub(r'label\s*=\s*f?"S&P 500 \(\{([^}]+)\}% CAGR\)"',
                 lambda m: f'label=f"{{benchmark_label(data, {key})}} ({{{m.group(1)}}}% CAGR)"', out)
    out = re.sub(r'label\s*=\s*"S&P 500"', f"label=benchmark_label(data, {key})", out)
    out = re.sub(r'label\s*=\s*\'S&P 500\'', f"label=benchmark_label(data, {key})", out)
    return out


def _rewrite_in_function(src, fname, us_keys):
    """Replace data["<US_KEY>"] with data[ref_key], scoped to one function.

    Uses the AST to find the function's exact source span so the edit cannot
    leak into a neighbouring function, which is what a regex over the whole
    file does.
    """
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return src
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and node.name == fname):
            continue
        seg = ast.get_source_segment(src, node)
        if not seg:
            continue
        new = seg
        for uk in us_keys:
            new = new.replace(f'data["{uk}"]', "data[ref_key]")
            new = new.replace(f"data['{uk}']", "data[ref_key]")
        if new != seg:
            src = src.replace(seg, new, 1)
    return src


def patch_topic(topic, apply=False):
    path = f"{ROOT}/{topic}/generate_charts.py"
    src = open(path).read()
    original = src
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return topic, f"PARSE ERROR {e}", 0

    # patch per-exchange chart functions, longest span first so offsets hold
    targets = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("chart_") \
                and "comparison" not in node.name:
            seg = ast.get_source_segment(src, node)
            if seg:
                targets.append(seg)
    changed = 0
    for seg in sorted(set(targets), key=len, reverse=True):
        k = key_expr(seg)
        if not k:
            continue
        new = patch_function(seg, k)
        if new != seg:
            src = src.replace(seg, new, 1)
            changed += 1

    # parameterise get_spy_cumulative itself
    src = re.sub(r"def get_spy_cumulative\(\s*\)", "def get_spy_cumulative(ref_key)", src)
    src = re.sub(r"def get_spy_cumulative\(\s*ref_key\s*=\s*[\"'][A-Z_]+[\"']\s*,",
                 "def get_spy_cumulative(ref_key,", src)
    # Rewrite the US lookup ONLY inside get_spy_cumulative's own body.
    #
    # This used to be a re.S regex spanning from the def to the first US-key
    # lookup anywhere after it. Because `.*?` crosses function boundaries, it
    # reached into chart_comparison_cagr and rewrote a legitimate cross-market
    # data["NYSE_NASDAQ_AMEX"] into data[ref_key], where ref_key is not in
    # scope. Three topics died with NameError. Bound the edit to the function.
    src = _rewrite_in_function(src, "get_spy_cumulative", US_KEYS)

    if changed and "from chart_utils import" not in src:
        lines = src.split("\n")
        insert = 0
        for i, l in enumerate(lines[:40]):
            if l.startswith("import ") or l.startswith("from "):
                insert = i + 1
        lines.insert(insert, IMPORT_BLOCK.rstrip("\n"))
        src = "\n".join(lines)

    if src == original:
        return topic, "no change", 0
    try:
        ast.parse(src)
    except SyntaxError as e:
        return topic, f"WOULD BREAK SYNTAX: {e}", 0
    if apply:
        open(path, "w").write(src)
    return topic, "patched", changed


# Catch-up batch 2026-08-13: MISLABEL topics BEHIND the bias-fix sweep cursor
# (index <= 51), which the sweep will never revisit. Topics ahead of the cursor
# are handled by the runbook itself and are deliberately absent.
#
# These four are the subset this tool can actually rewrite. The other behind-
# cursor topics (low-debt, value-momentum, high-yield, price-momentum) use
# plot_* function names or pass no exchange key, so the AST filter skips them
# and they need hand edits. A "no change" here is not a clean bill of health.
#
# NEVER add a STALE_RESULTS topic to this list. 52-week-low, income-quality,
# margin-expansion and sustained-roic read a pre-local-benchmark file where
# "S&P 500" is the TRUE label; relabelling them creates a falsehood.
TOPICS = ["small-value", "price-to-sales", "ev-ebitda", "equity-growth",
          "working-capital", "price-to-book", "altman-z"]

PREVIOUS_BATCH = ["asset-growth", "deleveraging", "dividend-sustainability", "fcf-growth",
                  "graham-number", "industry-leader", "interest-coverage", "low-pe",
                  "market-share", "net-debt-ebitda", "owner-earnings", "rising-yield",
                  "sector-rotation", "small-cap", "yield-gap"]  # done 2026-08-13

if __name__ == "__main__":
    apply = "--apply" in sys.argv
    for t in TOPICS:
        name, status, n = patch_topic(t, apply=apply)
        print(f"{name:<28}{status:<22}{n} function(s)")
    print("\nAPPLIED" if apply else "\nDRY RUN, nothing written")
