#!/usr/bin/env python3
"""Check an installed pairs results JSON before anything is published (offline, no network).

For pairs-fundamentals, pairs-zscore and pairs-multi-pair, whose rerun trades the per-year
universe of pairs_universe.py. A per-market file is one node; exchange_comparison.json is
{key: node}. A node fails when:
  - it is an error node, or has no universe_rule block saying top 30 per sector
  - exit_method is not ltp, at the top or in the ltp block (never install a both or drop run)
  - a year has no candidates/sectors count, or more than 30 candidates per sector
  - a year's same_company_pairs is not 0
  - a year's pairs are not recorded, or a recorded pair has no finite correlation

usage: check_pairs_results.py FILE.json|TOPIC ...   (TOPIC = every results/returns_*.json and
       the comparison file of that topic directory)   exit 1 on any failure
       check_pairs_results.py --self-test
"""
import contextlib
import copy
import glob
import importlib.util
import io
import json
import math
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOPICS = ("pairs-fundamentals", "pairs-zscore", "pairs-multi-pair")
TOP_N = 30   # names per sector: TOP_N_PER_SECTOR in the three topics


def exit_methods(x):
    """Every exit_method value anywhere in a node."""
    if isinstance(x, dict):
        return [v for k, v in x.items() if k == "exit_method"] + [m for v in x.values() for m in exit_methods(v)]
    if isinstance(x, list):
        return [m for v in x for m in exit_methods(v)]
    return []


def count(v):
    return type(v) is int and v >= 0


def problems(node):
    """Why a node is not an installable ltp run of the per-year universe; [] when it is."""
    if not isinstance(node, dict):
        return ["not a results object"]
    if "error" in node:
        return [f"error node: {str(node['error'])[:80]}"]
    out = []
    rule = node.get("universe_rule")
    if not isinstance(rule, dict) or rule.get("top_n_per_sector") != TOP_N:
        out.append(f"no universe_rule block with top_n_per_sector {TOP_N}")
    modes = exit_methods(node)
    if node.get("exit_method") != "ltp" or any(m != "ltp" for m in modes):
        out.append(f"exit_method is {sorted({str(m) for m in modes}) or 'missing'}, not ltp")
    years = node.get("annual_returns")
    if not isinstance(years, list) or not years:
        return out + ["no annual_returns"]
    for r in years:
        y, c, s = r.get("year"), r.get("candidates"), r.get("sectors")
        if not count(c) or not count(s):
            out.append(f"{y}: no candidates/sectors count")
        elif c > TOP_N * s:
            out.append(f"{y}: {c} candidates in {s} sectors, over {TOP_N} per sector")
        if not count(r.get("same_company_pairs")) or r["same_company_pairs"] != 0:
            out.append(f"{y}: same_company_pairs is {r.get('same_company_pairs')!r}, not 0")
        pairs = r.get("pairs")
        if not isinstance(pairs, list):
            out.append(f"{y}: pairs not recorded")
            continue
        bad = [p for p in pairs if not isinstance(p, dict) or not isinstance(p.get("corr"), (int, float))
               or not math.isfinite(p["corr"])]
        if bad:
            out.append(f"{y}: {len(bad)} recorded pair(s) without a finite correlation")
    return out


def nodes(path):
    """(label, node) for a per-market file, or one per key of a comparison file."""
    with open(path) as fh:
        d = json.load(fh)
    if (isinstance(d, dict) and d and "annual_returns" not in d and "error" not in d
            and all(isinstance(v, dict) for v in d.values())):
        return [(f"{path} [{k}]", v) for k, v in d.items()]
    return [(path, d)]


def expand(arg, root=ROOT):
    """A topic name stands for its per-market files and its comparison file."""
    if arg not in TOPICS:
        return [arg]
    d = os.path.join(root, arg, "results")
    comp = os.path.join(d, "exchange_comparison.json")
    return sorted(glob.glob(os.path.join(d, "returns_*.json"))) + ([comp] if os.path.exists(comp) else [])


def run(args, root=ROOT):
    bad = n = 0
    for arg in args:
        paths = expand(arg, root)
        if not paths:
            print(f"FAIL {arg}: no results files")
            bad += 1
        for path in paths:
            for label, node in nodes(path):
                n += 1
                probs = problems(node)
                bad += bool(probs)
                print(f"{'FAIL' if probs else 'ok  '} {label}")
                for p in probs:
                    print(f"       - {p}")
    print(f"\n{n} nodes, {bad} with problems")
    return 1 if bad else 0


# ---------------------------------------------------------------- self-test

def check(cond, what):
    if not cond:
        raise SystemExit(f"SELF-TEST FAIL: {what}")


def built_nodes():
    """A node from each topic's own build_output on made-up years: 4 cash, 16 invested, 330 names in 11 sectors."""
    sys.path.insert(0, ROOT)
    from metrics import compute_metrics
    made = {}
    for topic in TOPICS:
        spec = importlib.util.spec_from_file_location("cpr_" + topic.replace("-", "_"),
                                                      os.path.join(ROOT, topic, "backtest.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        years = range(2005, 2025)
        form = {y: {"pairs_formed": 20, "pairs_entered": 0 if y < 2009 else 8, "same_company_pairs": 0,
                    "pairs": [{"a": "AAA", "b": "BBB", "sector": "Energy", "corr": 0.7}]} for y in years}
        per_year = {y: {"candidates": TOP_N * 11, "sectors": 11} for y in years}
        rows = [{"year": y, "portfolio_return": 0.01, "spy_return": 0.03, "pairs_active": form[y]["pairs_entered"],
                 "pairs_with_trades": form[y]["pairs_entered"], "total_trades": 5, "legs_capped": 0,
                 "n_active": form[y]["pairs_entered"], "is_cash": y < 2009, **form[y]} for y in years]
        m = compute_metrics([r["portfolio_return"] for r in rows], [r["spy_return"] for r in rows], 1)
        extra = {"per_year": per_year, "listing_filter": None, "ltp_block": {"exit_method": "ltp", "fills": []}}
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            if topic == "pairs-fundamentals":
                made[topic] = mod.build_output(m, [], rows, rows, "X", **extra)
            elif topic == "pairs-zscore":
                made[topic] = mod.build_output(m, [], rows, rows, {"total_trades": 100}, "X", **extra)
            else:
                data = [{"year": y, "formation": form[y]} for y in years]
                made[topic] = mod.build_output(m, data, rows, [], {"total_trades": 100}, "X", **extra)
    return made


def edit(node, fn):
    n = copy.deepcopy(node)
    fn(n)
    return n


def year(key, value):
    return lambda n: n["annual_returns"][3].__setitem__(key, value)


def corr(value):
    return lambda n: n["annual_returns"][3]["pairs"][0].__setitem__("corr", value)


def self_test():
    made = built_nodes()
    for topic, node in made.items():
        check(problems(node) == [], f"{topic}: build_output's own node fails: {problems(node)}")
    cap = TOP_N * 11
    # (name, change, text its problem must carry)
    bad = [
        ("no universe_rule", lambda n: n.pop("universe_rule"), "universe_rule"),
        ("rule under the old key", lambda n: n.__setitem__("universe", n.pop("universe_rule")), "universe_rule"),
        ("another top N", lambda n: n["universe_rule"].__setitem__("top_n_per_sector", TOP_N + 1), "universe_rule"),
        ("one candidate over the cap", year("candidates", cap + 1), "candidates in 11 sectors"),
        ("no candidates count", lambda n: n["annual_returns"][3].pop("candidates"), "no candidates/sectors"),
        ("no sectors count", lambda n: n["annual_returns"][3].pop("sectors"), "no candidates/sectors"),
        ("a same-company pair", year("same_company_pairs", 1), "same_company_pairs is 1"),
        ("same_company_pairs absent", lambda n: n["annual_returns"][3].pop("same_company_pairs"), "same_company_pairs"),
        ("same_company_pairs false", year("same_company_pairs", False), "same_company_pairs"),
        ("NaN correlation", corr(float("nan")), "finite"),
        ("infinite correlation", corr(float("inf")), "finite"),
        ("null correlation", corr(None), "finite"),
        ("pairs not recorded", lambda n: n["annual_returns"][3].pop("pairs"), "pairs not recorded"),
        ("a both run", lambda n: n.__setitem__("exit_method", "both"), "not ltp"),
        ("a drop run", lambda n: (n.pop("exit_method"), n.pop("ltp")), "not ltp"),
        ("ltp block of a both run", lambda n: n["ltp"].__setitem__("exit_method", "both"), "not ltp"),
        ("no years", lambda n: n.__setitem__("annual_returns", []), "no annual_returns"),
        ("error node", lambda n: n.__setitem__("error", "boom"), "error node"),
    ]
    for topic, node in made.items():
        for name, fn, text in bad:
            probs = problems(edit(node, fn))
            check(len(probs) == 1 and text in probs[0], f"{topic}: '{name}' gives {probs}")
        # the cap is inclusive, a sector-less cash year is fine, an empty pairs list is fine
        check(problems(edit(node, year("candidates", cap))) == [], f"{topic}: exactly {cap} candidates fails")
        ok = edit(node, lambda n: n["annual_returns"][0].update(candidates=0, sectors=0, pairs=[]))
        check(problems(ok) == [], f"{topic}: an empty year fails: {problems(ok)}")
    # files: NaN survives the JSON round trip, a comparison file is read per key, a topic name expands
    good = made["pairs-zscore"]
    nan = edit(good, corr(float("nan")))
    with tempfile.TemporaryDirectory() as tmp:
        paths = {}
        for name, d in (("good", good), ("nan", nan), ("comp", {"JPX": good, "LSE": {"error": "boom"}}),
                        ("comp_ok", {"JPX": good, "LSE": good})):
            paths[name] = os.path.join(tmp, f"{name}.json")
            with open(paths[name], "w") as fh:
                json.dump(d, fh)
        check([problems(n) for _, n in nodes(paths["nan"])] != [[]], "a NaN in a file is not seen")
        check([bool(problems(n)) for _, n in nodes(paths["comp"])] == [False, True], "comparison file not read per key")
        with contextlib.redirect_stdout(io.StringIO()) as buf:
            rcs = [run([paths["good"]]), run([paths["good"], paths["nan"]]), run([paths["comp"]]),
                   run([paths["comp_ok"]])]
        check(rcs == [0, 1, 1, 0], f"exit codes {rcs}")
        check(f"FAIL {paths['comp']} [LSE]\n       - error node: boom" in buf.getvalue(),
              "a failure is not printed with its node and reason")
        # a topic name: its returns_*.json and its comparison file, nothing else in results/
        res = os.path.join(tmp, TOPICS[1], "results")
        os.makedirs(res)
        for name, d in (("returns_JPX.json", good), ("exchange_comparison.json", {"JPX": good}),
                        ("diversification_analysis.json", [])):
            with open(os.path.join(res, name), "w") as fh:
                json.dump(d, fh)
        want = [os.path.join(res, "returns_JPX.json"), os.path.join(res, "exchange_comparison.json")]
        check(expand(TOPICS[1], tmp) == want, f"topic expands to {expand(TOPICS[1], tmp)}")
        with contextlib.redirect_stdout(io.StringIO()) as buf:
            rcs = [run([TOPICS[1]], tmp), run([TOPICS[0]], tmp)]
        check(rcs == [0, 1] and "2 nodes, 0 with problems" in buf.getvalue() and "no results files" in buf.getvalue(),
              f"topic runs: {rcs}")
    check(expand("x.json") == ["x.json"], "a path is not passed through")
    print(f"check_pairs_results: SELF-TEST PASS ({len(made)} topics' build_output, {len(bad)} failing shapes each)")
    return 0


if __name__ == "__main__":
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0 if argv else 2)
    sys.exit(self_test() if argv == ["--self-test"] else run(argv))
