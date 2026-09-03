#!/usr/bin/env python3
"""Smoke-call the hand-edited non-canonical build_outputs on fabricated data.

Blocker B006. scripts/test_build_output_synthetic.py covers the 53 topics whose
build_output takes the canonical positional signature. Six sites were edited by
hand because their shapes differ, and those are exactly the ones a template
sweep cannot vouch for. Compiling and importing a module does NOT execute a
function body, so a NameError inside one of these ships silently and only
surfaces mid re-run.

Covered here by execution (their build_output is callable in isolation):
    pairs-fundamentals, pairs-zscore, pairs-multi-pair

NOT covered by execution, and deliberately named rather than quietly omitted:
    graham-timing/backtest.py:398          inside run_backtest(), after a DuckDB
                                           fetch; cannot be reached without
                                           either a live query or heavy mocking.
    dogs-of-dow/run_all_exchanges.py:167   inside main()'s per-exchange loop.
    interest-coverage/run_all_exchanges.py:168   same.
For those three, coverage is: byte-compile, module import, and the static
name-binding check in check_call_site_bindings() below, which confirms every
name each new call references is bound in its enclosing function. That is
weaker than execution and is stated as such.

These records are YEARLY, so the period key is "year" (an int), which is the
degrade path in period_accounting's date handling.

Usage:  python scripts/test_noncanonical_build_output.py
"""
import ast
import contextlib
import importlib.util
import io
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from metrics import compute_metrics  # noqa: E402

PASS, FAIL = [], []

# Every site that calls the shared helper outside the canonical template.
CALL_SITES = [
    "dogs-of-dow/run_all_exchanges.py",
    "interest-coverage/run_all_exchanges.py",
    "graham-timing/backtest.py",
    "pairs-fundamentals/backtest.py",
    "pairs-zscore/backtest.py",
    "pairs-multi-pair/backtest.py",
]


def load(rel):
    spec = importlib.util.spec_from_file_location(
        "nc_" + rel.replace("/", "_").replace("-", "_").replace(".py", ""),
        ROOT / rel)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def check(name, fn):
    try:
        fn()
        PASS.append(name)
        print(f"  PASS  {name}")
    except Exception as e:
        FAIL.append((name, f"{e.__class__.__name__}: {e}"))
        print(f"  FAIL  {name}: {e.__class__.__name__}: {e}")


def assert_counts(out, total, n, cash, invested, who):
    got = (out.get("total_rebalances"), out.get("n_periods"),
           out.get("cash_periods"), out.get("invested_periods"))
    assert got == (total, n, cash, invested), f"{who}: counts {got}, want " \
        f"{(total, n, cash, invested)}"
    assert out["cash_periods"] + out["invested_periods"] == out["total_rebalances"]
    assert out.get("window_truncated") is True, f"{who}: not marked truncated"
    assert "unmeasured" in out.get("window_label", ""), \
        f"{who}: label hides truncation: {out.get('window_label')!r}"
    assert "None" not in out.get("window_label", ""), \
        f"{who}: label leaks None: {out.get('window_label')!r}"


def quiet(fn, *a, **k):
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(buf):
        out = fn(*a, **k)
    return out, buf.getvalue()


def stub_metrics(port, spy):
    return compute_metrics(port, spy, 1)


# --------------------------------------------------------------- pairs-fundamentals
def t_pairs_fundamentals():
    m = load("pairs-fundamentals/backtest.py")
    results = ([{"year": 2002 + i, "portfolio_return": 0.05, "spy_return": None,
                 "pairs_formed": 10, "pairs_active": 0} for i in range(4)]
               + [{"year": 2006 + i, "portfolio_return": 0.05, "spy_return": 0.03,
                   "pairs_formed": 10, "pairs_active": 0} for i in range(3)]
               + [{"year": 2009 + i, "portfolio_return": 0.05, "spy_return": 0.03,
                   "pairs_formed": 10, "pairs_active": 8} for i in range(13)])
    valid = [r for r in results if r["spy_return"] is not None]
    port = [r["portfolio_return"] for r in valid]
    spy = [r["spy_return"] for r in valid]
    out, log = quiet(m.build_output, stub_metrics(port, spy), [], valid, results, "OSL")
    # executed == results (20); cash = pairs_active < MIN_PAIRS_ACTIVE
    cash = sum(1 for r in results if r["pairs_active"] < m.MIN_PAIRS_ACTIVE)
    assert_counts(out, 20, 16, cash, 20 - cash, "pairs-fundamentals")
    assert out["measured_start"] == "2006", out["measured_start"]
    assert out["requested_start"] == "2002", out["requested_start"]
    assert "WARNING" in log, "no truncation warning"
    print(f"        total=20 n=16 cash={cash} invested={20-cash} | {out['window_label']}")


# --------------------------------------------------------------- pairs-zscore
def t_pairs_zscore():
    m = load("pairs-zscore/backtest.py")
    results = ([{"year": 2002 + i, "portfolio_return": 0.05, "spy_return": None,
                 "pairs_formed": 10, "pairs_with_trades": 0, "total_trades": 0}
                for i in range(4)]
               + [{"year": 2006 + i, "portfolio_return": 0.05, "spy_return": 0.03,
                   "pairs_formed": 10, "pairs_with_trades": 8, "total_trades": 12}
                  for i in range(16)])
    valid = [r for r in results if r["spy_return"] is not None]
    port = [r["portfolio_return"] for r in valid]
    spy = [r["spy_return"] for r in valid]
    out, log = quiet(m.build_output, stub_metrics(port, spy), [], valid, results,
                     {"total_trades": 192}, "OSL")
    cash = sum(1 for r in results if r["pairs_with_trades"] < m.MIN_PAIRS_ACTIVE)
    assert_counts(out, 20, 16, cash, 20 - cash, "pairs-zscore")
    assert "WARNING" in log, "no truncation warning"
    print(f"        total=20 n=16 cash={cash} invested={20-cash} | {out['window_label']}")


# --------------------------------------------------------------- pairs-multi-pair
def t_pairs_multi_pair():
    m = load("pairs-multi-pair/backtest.py")
    primary = ([{"year": 2002 + i, "portfolio_return": 0.05, "spy_return": None,
                 "n_active": 0, "is_cash": True} for i in range(4)]
               + [{"year": 2006 + i, "portfolio_return": 0.05, "spy_return": 0.03,
                   "n_active": 20, "is_cash": False} for i in range(16)])
    per_year = [{"year": y["year"], "pairs": [1] * 20} for y in primary]
    valid = [y for y in primary if y["spy_return"] is not None]
    port = [y["portfolio_return"] for y in valid]
    spy = [y["spy_return"] for y in valid]
    out, log = quiet(m.build_output, stub_metrics(port, spy), per_year, primary,
                     [], {"total_trades": 100}, "OSL")
    assert_counts(out, 20, 16, 4, 16, "pairs-multi-pair")
    assert "WARNING" in log, "no truncation warning"
    print(f"        total=20 n=16 cash=4 invested=16 | {out['window_label']}")


# --------------------------------------------------------------- static binding
def check_call_site_bindings():
    """Every name each new period_accounting call references must be bound in
    its enclosing function. This is the ONLY automated coverage the three
    non-executable sites get, and it cannot see a wrong VALUE, only a missing
    name."""
    problems = []
    for rel in CALL_SITES:
        tree = ast.parse((ROOT / rel).read_text())
        mod = {n.id for x in tree.body if isinstance(x, ast.Assign)
               for n in ast.walk(x)
               if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
        mod |= {f.name for f in ast.walk(tree) if isinstance(f, ast.FunctionDef)}
        mod |= {a.asname or a.name.split(".")[0] for i in ast.walk(tree)
                if isinstance(i, (ast.Import, ast.ImportFrom)) for a in i.names}
        found = 0
        for f in ast.walk(tree):
            if not isinstance(f, ast.FunctionDef):
                continue
            calls = [c for c in ast.walk(f) if isinstance(c, ast.Call)
                     and isinstance(c.func, ast.Name)
                     and c.func.id == "period_accounting"]
            if not calls:
                continue
            bound = {a.arg for a in f.args.args} | {a.arg for a in f.args.kwonlyargs}
            for n in ast.walk(f):
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
                    bound.add(n.id)
                if isinstance(n, ast.comprehension):
                    for t in ast.walk(n.target):
                        if isinstance(t, ast.Name):
                            bound.add(t.id)
            for c in calls:
                found += 1
                refs = {n.id for a in list(c.args) + [k.value for k in c.keywords]
                        for n in ast.walk(a) if isinstance(n, ast.Name)}
                unbound = sorted(refs - bound - mod - set(dir(__builtins__)))
                if unbound:
                    problems.append(f"{rel}:{c.lineno} unbound {unbound}")
        if not found:
            problems.append(f"{rel}: no period_accounting call found")
    assert not problems, "; ".join(problems)
    print(f"        {len(CALL_SITES)} call sites, every referenced name bound")


def main():
    print("test_noncanonical_build_output (B006)")
    print("  EXECUTED:")
    check("pairs-fundamentals build_output", t_pairs_fundamentals)
    check("pairs-zscore build_output", t_pairs_zscore)
    check("pairs-multi-pair build_output", t_pairs_multi_pair)
    print("  STATIC ONLY (graham-timing, both run_all_exchanges.py):")
    check("call-site name bindings", check_call_site_bindings)
    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
