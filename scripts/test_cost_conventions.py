#!/usr/bin/env python3
"""Offline check of scripts/cost_conventions.py (no network): a leg's rows recomputed from its full-holdings file
under the file's own cost convention, and its CAGR and excess under both.

The trades arm runs on the files of test_52wh_selection's typed nine-quarter fixture (run_backtest's own rows, in
drop, ltp and both): every row reproduces, the cash rows, the rows that book nothing (one with an invested row
straight after it) and the row with no benchmark return among them; a row stored without its drift trade doesn't;
and the same books under the house rule give the rows typed here.

The round-trip arm runs on ROUND_TRIP, a results dict typed here: items on two tiers, a cash row, an invested row
with no benchmark return ahead of the measured ones, a two-key cost_model, and each row stored as the rounded
mean of (return less twice the rate). Its fifth row stores 0.098 where its rounded items give 0.09800075, so a
check that rounds and then compares fails a file the tolerance passes. The same dict under the trades convention
gives TRADES_ROWS, derived beside it, and both conventions' CAGR and excess come from the measured rows: the
file's own from its stored rows, the other from the recompute rounded to 6 places.

A row with no benchmark return is compared like any other, and a row 0.000003 off is faulted where the one
0.00000075 off passes. Each refusal has its reason (no cost_model, null or from before the key; an unknown
turnover; an invested row without holdings_full, a zero-survivor row among them; an item without cost_rate; a
null portfolio_return; a return under -100%; a top level that isn't one leg), and problem() answers with it.

main()'s printed lines are checked on a good trades file, on the round-trip dict (typed in ROUND_TRIP_LINES) and
on a faulted file: the header with its worst gap, each convention's CAGR and excess under its own name, the
difference with its sign, and the row that doesn't reproduce. It exits 0, 1 or 2 whatever the files' order, says
a both file's _ltp series isn't checked, and writes --json's fields, on a failing run too. Every mutant of the
script must fail a check on a wrong answer, not an error; a crash where a refusal is due is a wrong answer.

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
import test_52wh_selection as wh   # noqa: E402  (the typed nine-quarter fixture: walk_run)

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
# -0.2 - 2 x 0.003, quarter 6 0.10 - 2 x 0.003, quarter 8 0.10 - 2 x 0.002.
HOUSE_ROWS = [0.094, 0.192, 0.0, 0.094, -0.206, 0.0, 0.094, 0.0, 0.096]
QUARTERS = len(HOUSE_ROWS)
BENCH = [0.02] * (QUARTERS - 1)   # walk_run's benchmark: none on quarter 0, 2% on each quarter after it
# Each file's worst gap: the sale of quarter 3's book is 0.111/33 = 0.00336363..., so the row it lands on (drop's
# quarter 3, ltp's quarter 4) is stored 0.00000036363... from its recompute.
WALK_GAP = 4e-6 / 11
# Quarter 1's carried name trades 1/22 of the book at 0.3%. Stored without that trade the row reads 0.192825 +
# 0.000136 = 0.192961, which is 0.0001356 from its recompute, 0.1928254.
DRIFT_ROW, NODRIFT = 1, 0.192961


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


def figure_lines(own, rets, bench):
    """The figure lines main() prints for one file, from this test's own arithmetic: rets is {convention: its
    measured rows}, bench the benchmark's, own the file's convention."""
    c, b = {how: cagr(rets[how]) for how in ("round_trip", "trades")}, cagr(bench)
    return [f"  {how:<10} CAGR {c[how]:.2f}%, excess {c[how] - b:+.2f}pp" + ("  (the file's)" if how == own else "")
            for how in c] + [f"  trades less round_trip: CAGR {c['trades'] - c['round_trip']:+.3f}pp"]


# ROUND_TRIP's figure lines, typed. Its measured rows compound to 1.032 x 0.984 x 1.098 x 1.034 = 1.152916 over
# five quarters, 12.06% a year; the benchmark's 1.01^4 is 4.06%; TRADES_ROWS compound to 12.52% a year.
ROUND_TRIP_LINES = ["  round_trip CAGR 12.06%, excess +8.00pp  (the file's)",
                    "  trades     CAGR 12.52%, excess +8.46pp",
                    "  trades less round_trip: CAGR +0.461pp"]


def header(path, own, rows, gap):
    return f"{path}: {own}, {rows} rows checked, worst gap {gap} (tolerance 2e-06)"


def refused(mod, res, word):
    """The refusal's reason names `word`; '' when the file is refused that way. A file the script crashes on is
    not refused: that is a wrong answer too."""
    try:
        mod.check(res)
    except mod.Refusal as e:
        return "" if word in str(e) else f"refused for {e}, want a reason naming {word}"
    except Exception as e:
        return f"raised {type(e).__name__} ({e}), want a refusal naming {word}"
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
            try:
                got = mod.check(fixture(mode))
            except mod.Refusal as e:   # a good file refused is a wrong answer, not an error
                out.append(f"the fixture's {mode} file is refused: {e}")
                continue
            if (got["convention"], got["rows_checked"], got["bad"]) != ("trades", QUARTERS, []) or \
                    abs(got["worst_gap"] - WALK_GAP) > 1e-9:
                out.append(f"the fixture's {mode} file: {got}")
        ltp = fixture("ltp")
        house = mod.recompute(ltp, "round_trip")
        if len(house) != QUARTERS or any(abs(a - b) > 2e-6 for a, b in zip(house, HOUSE_ROWS)):
            out.append(f"the fixture's ltp books under the house rule {house}, want {HOUSE_ROWS}")
        nodrift = fixture("ltp")
        nodrift["period_data"][DRIFT_ROW]["portfolio_return"] = NODRIFT
        bad = [b[0] for b in mod.check(nodrift)["bad"]]
        if bad != [ltp["period_data"][DRIFT_ROW]["rebalance_date"]]:
            out.append(f"a row stored without its drift trade: rows faulted {bad}")
        unmeasured = fixture("ltp")   # its first row has no benchmark return, and is compared like any other
        if unmeasured["period_data"][0]["spy_return"] is not None or unmeasured["period_data"][0]["portfolio_return"] != 0.097:
            out.append(f"the fixture's first row is not the unmeasured 0.097: {unmeasured['period_data'][0]}")
        unmeasured["period_data"][0]["portfolio_return"] = 0.094
        bad = [b[0] for b in mod.check(unmeasured)["bad"]]
        if bad != [ltp["period_data"][0]["rebalance_date"]]:
            out.append(f"a row with no benchmark return stored 0.003 off: rows faulted {bad}")
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
        off = copy.deepcopy(ROUND_TRIP)   # over the tolerance, where row 4's 0.00000075 is under it
        off["period_data"][3]["portfolio_return"] = -0.016003
        if [b[0] for b in mod.check(off)["bad"]] != ["2020-10-01"]:
            out.append(f"a round-trip row off by 0.000003: {mod.check(off)['bad']}")
        # refusals
        zero, null_row, low = fixture("drop"), fixture("ltp"), fixture("ltp")
        null_row["period_data"][3]["portfolio_return"] = None
        low["period_data"][3]["holdings_full"][0]["return"] = -1.5
        cases = [("a null cost_model", {**ROUND_TRIP, "cost_model": None}, "cost_model"),
                 ("a costed file from before cost_model", {k: v for k, v in ROUND_TRIP.items() if k != "cost_model"},
                  "older than cost_model"),
                 ("a row with a null portfolio_return", null_row, "portfolio_return is null"),
                 ("an item with a return under -100%", low, "under -100%"),
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
        for name, res in (("a row off", nodrift), ("a row with no benchmark return off", unmeasured),
                          ("no holdings_full", without(ROUND_TRIP, 1)), ("a null portfolio_return", null_row),
                          ("a return under -100%", low)):
            try:
                if not mod.problem(res):
                    out.append(f"problem() passes {name}")
            except Exception as e:   # gate 15 calls problem(): it has to answer, not raise
                out.append(f"problem() raises {type(e).__name__} on {name}")
        # main(): what it prints, its exit codes, the both note, --json
        if figure_lines("round_trip", {"round_trip": [r["portfolio_return"] for r in rows], "trades": TRADES_ROWS[MEASURED]},
                        [r["spy_return"] for r in rows]) != ROUND_TRIP_LINES:
            out.append("ROUND_TRIP_LINES is not what this test's own arithmetic gives for ROUND_TRIP")
        with tempfile.TemporaryDirectory() as tmp:
            paths = {}
            for name, res in (("ltp", ltp), ("both", fixture("both")), ("rt", ROUND_TRIP), ("nodrift", nodrift),
                              ("null", {**ROUND_TRIP, "cost_model": None}), ("null_row", null_row)):
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
            stored = wh.WALK_ROWS["ltp"][1:]
            says = ([header(paths["ltp"], "trades", QUARTERS, "3.6e-07")]
                    + figure_lines("trades", {"round_trip": HOUSE_ROWS[1:], "trades": stored}, BENCH)
                    + [header(paths["rt"], "round_trip", 6, "7.5e-07")] + ROUND_TRIP_LINES)
            if [x for x in says if x not in text.splitlines()] or "DOES NOT REPRODUCE" in text:
                out.append(f"main() on three good files doesn't print {[x for x in says if x not in text.splitlines()]}"
                           f" or prints a fault: {text}")
            with open(js) as fh:
                recs = json.load(fh)
            keys = {"file", "convention", "rows_checked", "worst_gap", "round_trip", "trades"}
            if [set(r) for r in recs] != [keys] * 3 or [r["convention"] for r in recs] != ["trades", "trades", "round_trip"] \
                    or [r["file"] for r in recs] != [paths[k] for k in ("ltp", "both", "rt")] \
                    or [r["rows_checked"] for r in recs] != [QUARTERS, QUARTERS, 6] \
                    or any(abs(r["worst_gap"] - gap) > 1e-9 for r, gap in zip(recs, (WALK_GAP, WALK_GAP, ROUND_GAP))) \
                    or any(set(r[how]) != {"cagr", "excess_cagr"} for r in recs for how in ("round_trip", "trades")) \
                    or any(abs(recs[2][how]["cagr"] - c) > 1e-9 or abs(recs[2][how]["excess_cagr"] - (c - bench)) > 1e-9
                           for how, c in want.items()):
                out.append(f"--json wrote {recs}")
            # a faulted file: the row is printed, and the file's own figures are its stored rows'
            rc, text = run_main(mod, [paths["nodrift"]])
            says = ([header(paths["nodrift"], "trades", QUARTERS, "1.4e-04"),
                     f"  DOES NOT REPRODUCE 2020-04-01: stored {NODRIFT}, recomputed 0.192825"]
                    + figure_lines("trades", {"round_trip": HOUSE_ROWS[1:], "trades": [NODRIFT] + stored[1:]}, BENCH))
            if rc != 1 or [x for x in says if x not in text.splitlines()]:
                out.append(f"main() on a faulted file exits {rc} and doesn't print "
                           f"{[x for x in says if x not in text.splitlines()]}: {text}")
            for name, files, code in (("a row that doesn't reproduce", ["ltp", "nodrift"], 1),
                                      ("a refused file", ["ltp", "null"], 2),
                                      ("a row with a null portfolio_return", ["ltp", "null_row"], 2),
                                      ("a refused file after a faulted one", ["nodrift", "null"], 2),
                                      ("a faulted file after a refused one", ["null", "nodrift"], 2)):
                rc, text = run_main(mod, [paths[k] for k in files])
                if rc != code:
                    out.append(f"main() on {name} exits {rc}, want {code}: {text}")
            # a failing run still writes --json, a record per checked file, and prints the refusal's reason
            js = os.path.join(tmp, "failing.json")
            rc, text = run_main(mod, [paths["ltp"], paths["nodrift"], paths["null"]], "--json", js)
            recs = json.load(open(js)) if os.path.exists(js) else None
            if recs is None or [r["file"] for r in recs] != [paths["ltp"], paths["nodrift"]]:
                out.append(f"--json on a run with a faulted and a refused file wrote {recs}")
            if not any(x.startswith(f"{paths['null']}: REFUSED: ") and "cost_model" in x for x in text.splitlines()):
                out.append(f"main() doesn't print why it refuses a file: {text}")
    except Exception as e:   # the checks before it keep their findings
        return out + [f"error: {type(e).__name__}: {e}"]
    return out


WALK = "            costs = trade_costs(period_books)\n"
MUTANTS = {
    "an absent turnover read as trades": ('cm.get("turnover", "round_trip")', 'cm.get("turnover", "trades")'),
    "a cash row that doesn't sell": (
        WALK,
        "            walked = iter(trade_costs([b for b in period_books if b]))\n"
        "            costs = [next(walked) if b else 0.0 for b in period_books]\n"),
    "a row that books nothing left out of the walk": (
        WALK,
        "            walked = iter(trade_costs([b for b in period_books if b != {}]))\n"
        "            costs = [next(walked) if b != {} else 0.0 for b in period_books]\n"),
    # every row keeps its place and its book; only the rows with a benchmark return are walked
    "a walk over measured rows only": (
        WALK,
        '            seen = [i for i, r in enumerate(res["period_data"]) if r.get("spy_return") is not None]\n'
        "            part = dict(zip(seen, trade_costs([period_books[i] for i in seen])))\n"
        "            costs = [part.get(i, 0.0) for i in range(len(period_books))]\n"),
    "a return the walk refuses raised, not refused": ("            raise Refusal(str(e)) from None\n",
                                                      "            raise\n"),
    "a null portfolio_return not refused": ('        if r.get("portfolio_return") is None:\n', "        if False:\n"),
    "a row with no benchmark return not compared": (
        'if abs(got - r["portfolio_return"]) > TOLERANCE]}',
        'if abs(got - r["portfolio_return"]) > TOLERANCE and r.get("spy_return", 0) is not None]}'),
    "a tolerance wide enough to hide the drift term": ("TOLERANCE = 2e-6", "TOLERANCE = 1e-3"),
    "a tolerance 4.5 times too wide": ("TOLERANCE = 2e-6", "TOLERANCE = 9e-6"),
    "rounded, then compared": ("abs(got - r[\"portfolio_return\"]) > TOLERANCE",
                               "round(got, 6) != r[\"portfolio_return\"]"),
    "a row that books nothing read as a row without its list": ('        if "holdings_full" not in r:\n',
                                                                '        if not r.get("holdings_full"):\n'),
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
    "a faulted file after a refused one exits 1": ("            worst = max(worst, 1)\n",
                                                   "            worst = max(worst, 1)\n            worst = min(worst, 1)\n"),
    "no word on a both file's _ltp series": ('if any("portfolio_return_ltp" in r for r in res["period_data"]):',
                                             "if False:"),
    # what main() prints and writes
    "the difference printed with its sign flipped": ("fig['trades']['cagr'] - fig['round_trip']['cagr']:+.3f",
                                                     "fig['round_trip']['cagr'] - fig['trades']['cagr']:+.3f"),
    "each convention's figures printed under the other's name": (
        'c, x = fig[how]["cagr"], fig[how]["excess_cagr"]',
        'c, x = [fig[h] for h in CONVENTIONS if h != how][0].values()'),
    "CAGR and excess swapped in the printed line": ('f"CAGR {c:.2f}%, excess {x:+.2f}pp"',
                                                    'f"CAGR {x:.2f}%, excess {c:+.2f}pp"'),
    "a row that doesn't reproduce not printed": ('    for day, stored, new in got["bad"]:\n',
                                                 "    for day, stored, new in []:\n"),
    "a trades file's worst gap reported as 0": (
        '"worst_gap": max((abs(got - r["portfolio_return"]) for r, got in pairs), default=0.0),',
        '"worst_gap": max((abs(got - r["portfolio_return"]) for r, got in pairs), default=0.0) * (own != "trades"),'),
    "the worst gap printed as 0": ("worst gap {got['worst_gap']:.1e}", "worst gap {0.0:.1e}"),
    "--json's rows_checked and worst_gap zeroed": (
        '"rows_checked": got["rows_checked"],\n                 "worst_gap": got["worst_gap"], **fig}',
        '"rows_checked": 0,\n                 "worst_gap": 0.0, **fig}'),
    "--json not written when a file fails": ("    if args.json:\n", "    if args.json and not worst:\n"),
    "REFUSED printed without its reason": ('print(f"{path}: REFUSED: {e}")', 'print(f"{path}: REFUSED")'),
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
