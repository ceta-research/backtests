"""Make regional chart titles name the benchmark they actually plot.

Finds calls to chart_cumulative / chart_annual_bars whose first argument is a
literal exchange key (or a one-element list of one) and whose title contains
"vs S&P 500", and rewrites the title into an f-string using benchmark_label.

US exchanges are skipped: "vs S&P 500" is correct there.
"""
import ast
import re
import sys

ROOT = "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests"
US_KEYS = {"US_MAJOR", "NYSE_NASDAQ_AMEX", "NYSE_NASDAQ", "US", "NYSE", "NASDAQ", "AMEX", "us"}
FUNCS = {"chart_cumulative", "chart_annual_bars", "chart_cumulative_single"}


def first_key(node):
    """Exchange key from the call's first positional arg, if literal."""
    if not node.args:
        return None
    a = node.args[0]
    if isinstance(a, ast.Constant) and isinstance(a.value, str):
        return a.value
    if isinstance(a, (ast.List, ast.Tuple)) and len(a.elts) == 1:
        e = a.elts[0]
        if isinstance(e, ast.Constant) and isinstance(e.value, str):
            return e.value
    return None


def patch(topic, apply=False):
    path = f"{ROOT}/{topic}/generate_charts.py"
    src = open(path).read()
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return topic, f"PARSE ERROR {e}", 0

    repls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if fn not in FUNCS:
            continue
        key = first_key(node)
        if not key or key in US_KEYS:
            continue
        for arg in node.args[1:]:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str) \
                    and "vs S&P 500" in arg.value:
                old = ast.get_source_segment(src, arg)
                if not old or '"' in arg.value:
                    continue
                body = arg.value.replace("{", "{{").replace("}", "}}")
                # inner single quotes so the outer f-string can use doubles
                body = body.replace("vs S&P 500",
                                    "vs {benchmark_label(data, '%s')}" % key)
                repls.append((old, 'f"%s"' % body))

    if not repls:
        return topic, "no regional titles", 0
    for old, new in repls:
        src = src.replace(old, new, 1)
    try:
        ast.parse(src)
    except SyntaxError as e:
        return topic, f"WOULD BREAK SYNTAX: {e}", 0
    if apply:
        open(path, "w").write(src)
    return topic, "patched", len(repls)


# Catch-up batch 2026-08-13. See the note in patch_chart_benchmarks.py:
# behind-cursor MISLABEL topics only, never a STALE_RESULTS topic.
TOPICS = ["small-value", "price-to-sales", "ev-ebitda", "equity-growth",
          "working-capital", "price-to-book", "altman-z"]

PREVIOUS_BATCH = ["asset-growth", "deleveraging", "dividend-sustainability", "fcf-growth",
                  "graham-number", "industry-leader", "interest-coverage", "low-pe",
                  "market-share", "net-debt-ebitda", "owner-earnings", "rising-yield",
                  "small-cap", "yield-gap"]  # done 2026-08-13

if __name__ == "__main__":
    apply = "--apply" in sys.argv
    total = 0
    for t in TOPICS:
        n, s, c = patch(t, apply=apply)
        total += c
        print(f"{n:<28}{s:<26}{c} title(s)")
    print(f"\n{total} titles {'rewritten' if apply else 'would be rewritten'}")
