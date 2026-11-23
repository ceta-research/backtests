#!/usr/bin/env python3
"""Offline check of scripts/cost_conventions.py (no network): a leg's rows recomputed from its full-holdings file
under the file's own cost convention, and its CAGR and excess under both.

The trades arm runs on the files of test_52wh_selection's typed seven-quarter fixture (run_backtest's own rows, in
drop, ltp and both): every row reproduces, the cash rows, the row that books nothing and the row with no benchmark
return among them; a row stored without its drift trade doesn't; and the same books under the house rule give the
rows typed here.

The round-trip arm runs on ROUND_TRIP, a results dict typed here: items on two tiers, a cash row, an invested row
with no benchmark return ahead of the measured ones, a two-key cost_model, and each row stored as the rounded
mean of (return less twice the rate). Its fifth row stores 0.098 where its rounded items give 0.09800075, so a
check that rounds and then compares fails a file the tolerance passes. The same dict under the trades convention
gives TRADES_ROWS, derived beside it, and both conventions' CAGR and excess come from the measured rows: the
file's own from its stored rows, the other from the recompute rounded to 6 places.

Each refusal has its reason (a null cost_model, an unknown turnover, an invested row without holdings_full, a
zero-survivor row among them, an item without cost_rate, a top level that isn't one leg), main() exits 0, 1 or 2,
says a both file's _ltp series isn't checked, and writes --json's fields. Every mutant of the script must fail a
check on a wrong answer, not an error.

Usage:
    python scripts/test_cost_conventions.py [repo root]
"""
import contextlib
import copy
import io
import json
import os
import sys
import tempfile
import types

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "scripts", "cost_conventions.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
import test_52wh_selection as wh   # noqa: E402  (the typed seven-quarter fixture: walk_run, WALK_ROWS)

HI, MID, LO = 0.005, 0.003, 0.001
TRADES_MODEL = {"function": "costs.tiered_cost", "fx_per_usd": 1.0, "turnover": "trades"}


def load(src):
    mod = types.ModuleType("cost_conventions_under_test")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def leg(rows, **over):
    """What the script reads of a leg file, around run_backtest's rows."""
    return {"universe": "TEST", "frequency": "quarterly", "period_data": rows, "cost_model": dict(TRADES_MODEL), **over}


_FIXTURE = {}


def fixture(mode):
    """The typed fixture's leg file in one exit mode, built once."""
    if mode not in _FIXTURE:
        _FIXTURE[mode] = leg(wh.walk_run(wh.load(wh.SRC), mode))
    return copy.deepcopy(_FIXTURE[mode])


# The fixture's books under the house rule, each row its mean return less twice its mean rate: quarter 0
# 0.10 - 2 x 0.003, quarter 1 0.1999996 - 2 x 0.004 = 0.1919996, quarter 3 0.10 - 2 x 0.003, quarter 4 (ltp) G's
# -0.2 - 2 x 0.003, quarter 6 0.10 - 2 x 0.003.
HOUSE_ROWS = [0.094, 0.192, 0.0, 0.094, -0.206, 0.0, 0.094]
DRIFT_ROW, DRIFT_TRADE = 1, 0.003 / 22   # quarter 1's carried name trades 1/22 of the book at 0.3%


def item(symbol, ret, rate):
    return {"symbol": symbol, "return": ret, "cost_rate": rate}


def row(reb, exit_, stored, items, bench=0.01):
    """A period row as a leg file holds it: invested with its items, or (items None) cash before the screen."""
    out = {"rebalance_date": reb, "exit_date": exit_, "portfolio_return": stored, "spy_return": bench,
           "stocks_held": len(items or []), "screened": len(items) if items else 1,
           "entry_buyable": len(items) if items else None, "min_stocks": 2}
    return {**out, "holdings_full": items} if items is not None else out


# A leg on the house rule: each stored row is round(mean(return - 2 x rate), 6).
ROUND_TRIP = {
    "universe": "TEST", "frequency": "quarterly",
    "cost_model": {"function": "costs.tiered_cost", "fx_per_usd": 1.0},
    "period_data": [
        # 0 no benchmark return yet. (0.10 - 0.010 + 0.04 - 0.002)/2 = 0.064.
        row("2020-01-01", "2020-04-01", 0.064, [item("A", 0.10, HI), item("B", 0.04, LO)], bench=None),
        # 1 (0.02 - 0.010 + 0.06 - 0.006)/2 = 0.032.
        row("2020-04-01", "2020-07-01", 0.032, [item("A", 0.02, HI), item("C", 0.06, MID)]),
        # 2 cash.
        row("2020-07-01", "2020-10-01", 0.0, None),
        # 3 (-0.05 - 0.010 + 0.03 - 0.002)/2 = -0.016.
        row("2020-10-01", "2021-01-01", -0.016, [item("A", -0.05, HI), item("B", 0.03, LO)]),
        # 4 the run booked 0.1000006 three times and 0.0999996 once: mean 0.10000035, less 0.002, stored 0.098.
        #   Its items, rounded to 6 places, read 0.100001 three times and 0.1: 0.10000075 less 0.002 = 0.09800075,
        #   which rounds to 0.098001.
        row("2021-01-01", "2021-04-01", 0.098, [item("W", 0.100001, LO), item("X", 0.100001, LO),
                                                 item("Y", 0.100001, LO), item("Z", 0.1, LO)]),
        # 5 (0.08 - 0.010 + 0.00 - 0.002)/2 = 0.034.
        row("2021-04-01", "2021-07-01", 0.034, [item("A", 0.08, HI), item("B", 0.0, LO)]),
    ],
}
ROUND_GAP = 7.5e-7   # row 4: 0.09800075 against the stored 0.098
# The same books under the trades convention:
#  0 entries 1/2 x 0.005 + 1/2 x 0.001 = 0.003: 0.07 - 0.003 = 0.067. End weights A 1.10/2.14, B 1.04/2.14.
#  1 A trades |1/2 - 1.10/2.14| = 0.014019 at 0.005 (0.000070), C enters at 1/2 x 0.003 (0.0015), B sells
#    1.04/2.14 = 0.485981 at 0.001 (0.000486): 0.002056. Row 2 is cash, so the sale lands here:
#    1.02/2.08 x 0.005 + 1.06/2.08 x 0.003 = 0.002452 + 0.001529 = 0.003981. 0.04 - 0.006037 = 0.033963.
#  3 entries 0.003: -0.01 - 0.003 = -0.013. End weights A 0.95/1.98, B 1.03/1.98.
#  4 four entries at 1/4 x 0.001 (0.001), A sells 0.95/1.98 x 0.005 (0.002399), B 1.03/1.98 x 0.001 (0.000520):
#    0.003919. 0.10000075 - 0.003919 = 0.096082.
#  5 A and B enter (0.003) and the four leave, their weights adding to 1, at 0.001: 0.004. 0.04 - 0.004 = 0.036.
TRADES_ROWS = [0.067, 0.033963, 0.0, -0.013, 0.096082, 0.036]
MEASURED = slice(1, None)   # row 0 has no benchmark return


def cagr(rets, ppy=4):
    total = 1.0
    for r in rets:
        total *= 1 + r
    return (total ** (ppy / len(rets)) - 1) * 100


def refused(mod, res, word):
    """The refusal's reason names `word`; '' when the file is refused that way."""
    try:
        mod.check(res)
    except mod.Refusal as e:
        return "" if word in str(e) else f"refused for {e}, want a reason naming {word}"
    return f"not refused (want a reason naming {word})"


def without(res, i, key="holdings_full"):
    out = copy.deepcopy(res)
    del out["period_data"][i][key]
    return out


def run_main(mod, paths, *args):
    """main() on paths: (exit code, stdout)."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            rc = mod.main([*paths, *args])
        except SystemExit as e:
            rc = e.code
    return rc or 0, out.getvalue()


def failures(src):
    out = []
    try:
        mod = load(src)
        # the trades arm on the typed fixture's files
        for mode in ("drop", "ltp", "both"):
            got = mod.check(fixture(mode))
            if (got["convention"], got["rows_checked"], got["bad"]) != ("trades", 7, []) or got["worst_gap"] > 2e-6:
                out.append(f"the fixture's {mode} file: {got}")
        ltp = fixture("ltp")
        house = mod.recompute(ltp, "round_trip")
        if len(house) != 7 or any(abs(a - b) > 2e-6 for a, b in zip(house, HOUSE_ROWS)):
            out.append(f"the fixture's ltp books under the house rule {house}, want {HOUSE_ROWS}")
        nodrift = fixture("ltp")
        nodrift["period_data"][DRIFT_ROW]["portfolio_return"] = round(
            nodrift["period_data"][DRIFT_ROW]["portfolio_return"] + DRIFT_TRADE, 6)
        bad = [b[0] for b in mod.check(nodrift)["bad"]]
        if bad != [ltp["period_data"][DRIFT_ROW]["rebalance_date"]]:
            out.append(f"a row stored without its drift trade: rows faulted {bad}")
        # the round-trip arm on the typed dict, and the other convention's rows and figures from it
        got = mod.check(ROUND_TRIP)
        if (got["convention"], got["rows_checked"], got["bad"]) != ("round_trip", 6, []) or \
                abs(got["worst_gap"] - ROUND_GAP) > 1e-9:
            out.append(f"the round-trip dict: {got}")
        trades = [round(x, 6) for x in mod.recompute(ROUND_TRIP, "trades")]
        if trades != TRADES_ROWS:
            out.append(f"the round-trip dict under trades {trades}, want {TRADES_ROWS}")
        rows = ROUND_TRIP["period_data"][MEASURED]
        bench = cagr([r["spy_return"] for r in rows])
        want = {"round_trip": cagr([r["portfolio_return"] for r in rows]), "trades": cagr(TRADES_ROWS[MEASURED])}
        figures = mod.figures(ROUND_TRIP)
        for how, c in want.items():
            f = figures.get(how) or {}
            if abs((f.get("cagr") or 0) - c) > 1e-9 or abs((f.get("excess_cagr") or 0) - (c - bench)) > 1e-9:
                out.append(f"{how} figures {f}, want CAGR {c:.6f} and excess {c - bench:.6f}")
        off = copy.deepcopy(ROUND_TRIP)
        off["period_data"][3]["portfolio_return"] = -0.01601
        if [b[0] for b in mod.check(off)["bad"]] != ["2020-10-01"]:
            out.append(f"a round-trip row off by 0.00001: {mod.check(off)['bad']}")
        # refusals
        zero = fixture("drop")
        cases = [("a null cost_model", {**ROUND_TRIP, "cost_model": None}, "cost_model"),
                 ("a reserved turnover", {**ROUND_TRIP, "cost_model": {**TRADES_MODEL, "turnover": "traded"}}, "turnover"),
                 ("an invested row without holdings_full", without(ROUND_TRIP, 1), "holdings_full"),
                 ("a row that books nothing, without holdings_full", without(zero, 4), "holdings_full"),
                 ("an item without cost_rate", {**ROUND_TRIP, "period_data": [
                     {**r, "holdings_full": [{k: v for k, v in h.items() if k != "cost_rate"}
                                             for h in r["holdings_full"]]} if "holdings_full" in r else r
                     for r in ROUND_TRIP["period_data"]]}, "cost_rate"),
                 ("an item with a null cost_rate", {**ROUND_TRIP, "period_data": [
                     {**r, "holdings_full": [{**h, "cost_rate": None} for h in r["holdings_full"]]}
                     if "holdings_full" in r else r for r in ROUND_TRIP["period_data"]]}, "cost_rate"),
                 ("a comparison file", {"SET": ROUND_TRIP, "NSE": ROUND_TRIP}, "one leg")]
        if zero["period_data"][4].get("holdings_full") != [] or zero["period_data"][4]["stocks_held"] != 0:
            out.append("the fixture's drop file has no row that books nothing")
        out += [f"{name}: {why}" for name, res, word in cases for why in [refused(mod, res, word)] if why]
        # gate 15's reading of a file
        if mod.problem(fixture("ltp")) or mod.problem(ROUND_TRIP):
            out.append(f"problem() faults a good file: {mod.problem(fixture('ltp'))} {mod.problem(ROUND_TRIP)}")
        for name, res in (("a row off", nodrift), ("no holdings_full", without(ROUND_TRIP, 1))):
            if not mod.problem(res):
                out.append(f"problem() passes {name}")
        # main(): exit codes, the both note, --json
        with tempfile.TemporaryDirectory() as tmp:
            paths = {}
            for name, res in (("ltp", ltp), ("both", fixture("both")), ("rt", ROUND_TRIP), ("nodrift", nodrift),
                              ("null", {**ROUND_TRIP, "cost_model": None})):
                paths[name] = os.path.join(tmp, f"{name}.json")
                with open(paths[name], "w") as fh:
                    json.dump(res, fh)
            js = os.path.join(tmp, "out.json")
            rc, text = run_main(mod, [paths["ltp"], paths["both"], paths["rt"]], "--json", js)
            if rc != 0:
                out.append(f"main() on three good files exits {rc}: {text}")
            if text.count("_ltp series") != 1:
                out.append(f"main() names the unchecked _ltp series {text.count('_ltp series')} times, want once "
                           "(the both file)")
            with open(js) as fh:
                recs = json.load(fh)
            keys = {"file", "convention", "rows_checked", "worst_gap", "round_trip", "trades"}
            if [set(r) for r in recs] != [keys] * 3 or [r["convention"] for r in recs] != ["trades", "trades", "round_trip"] \
                    or [r["file"] for r in recs] != [paths[k] for k in ("ltp", "both", "rt")] \
                    or any(set(r[how]) != {"cagr", "excess_cagr"} for r in recs for how in ("round_trip", "trades")) \
                    or abs(recs[2]["trades"]["cagr"] - want["trades"]) > 1e-9:
                out.append(f"--json wrote {recs}")
            for name, files, code in (("a row that doesn't reproduce", ["ltp", "nodrift"], 1),
                                      ("a refused file", ["ltp", "null"], 2),
                                      ("a refused file beside a faulted one", ["nodrift", "null"], 2)):
                rc, text = run_main(mod, [paths[k] for k in files])
                if rc != code:
                    out.append(f"main() on {name} exits {rc}, want {code}: {text}")
    except Exception as e:   # the checks before it keep their findings
        return out + [f"error: {type(e).__name__}: {e}"]
    return out


MUTANTS = {
    "an absent turnover read as trades": ('cm.get("turnover", "round_trip")', 'cm.get("turnover", "trades")'),
    "a cash row that doesn't sell": (
        "        costs = trade_costs(period_books)\n",
        "        walked = iter(trade_costs([b for b in period_books if b]))\n"
        "        costs = [next(walked) if b else 0.0 for b in period_books]\n"),
    "a walk over measured rows only": ('    for r in res["period_data"]:\n        if "holdings_full" not in r:',
                                       '    for r in _rows(res):\n        if "holdings_full" not in r:'),
    "a tolerance wide enough to hide the drift term": ("TOLERANCE = 2e-6", "TOLERANCE = 1e-3"),
    "rounded, then compared": ("abs(got - r[\"portfolio_return\"]) > TOLERANCE",
                               "round(got, 6) != r[\"portfolio_return\"]"),
    "the house rule charged one way": ("apply_costs(x, rate)", "x - rate"),
    "the other convention's figures from unrounded rows": ("round(by[how][at[id(r)]], 6)", "by[how][at[id(r)]]"),
    "the file's own figures from the recompute": ('r["portfolio_return"] if how == own else',
                                                  'r["portfolio_return"] if False else'),
    "a null cost_model read as the house rule": (
        "    if not isinstance(cm, dict):\n        raise Refusal(",
        "    if not isinstance(cm, dict):\n        cm = {}\n    if False:\n        raise Refusal("),
    "a reserved turnover read as trades": (
        "    if how not in CONVENTIONS:\n",
        '    if how == "traded":\n        how = "trades"\n    if how not in CONVENTIONS:\n'),
    "rows told apart by the names held": ("period_state(r) == INVESTED or", "False or"),
    "an invested row without holdings_full read as cash": (
        'raise Refusal(f"{r.get(\'rebalance_date\')}: an invested row without holdings_full',
        'Refusal(f"{r.get(\'rebalance_date\')}: an invested row without holdings_full'),
    "an item without cost_rate charged nothing": (
        '        if any(h.get("cost_rate") is None for h in r["holdings_full"]):\n',
        '        r = {**r, "holdings_full": [{**h, "cost_rate": h.get("cost_rate") or 0.0} for h in r["holdings_full"]]}\n'
        '        if False:\n'),
    "a comparison file read as a leg": ('if not isinstance(res, dict) or "period_data" not in res:',
                                        'if not isinstance(res, dict):'),
    "a faulted row exits 0": ("            worst = max(worst, 1)\n", "            pass\n"),
    "a refused file exits 0": ("            worst = 2\n", "            pass\n"),
    "no word on a both file's _ltp series": ('if any("portfolio_return_ltp" in r for r in res["period_data"]):',
                                             "if False:"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_cost_conventions: FAIL", base)
        sys.exit(1)
    survived, errored = [], []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_cost_conventions: FAIL (mutant anchor found {SRC.count(old)} times: {name})")
            sys.exit(1)
        got = failures(SRC.replace(old, new))
        if not got:
            survived.append(name)
        elif all(f.startswith("error:") for f in got):
            errored.append(name)
    if survived or errored:
        print("test_cost_conventions: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    print(f"test_cost_conventions: PASS (the typed fixture's three files and a typed round-trip leg of "
          f"{len(ROUND_TRIP['period_data'])} rows; {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
