#!/usr/bin/env python3
"""Offline checks for scripts/gate_ltp.py (crafted dicts, no backtests, no network): every gate check
passes a good run and fails the crafted bad one with its own message. Checks survive python -O."""
import contextlib
import copy
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gate_ltp as gate  # noqa: E402


def check(cond, what):
    if not cond:
        raise SystemExit(f"FAIL: {what}")


def has(probs, text, what):
    check(any(text in p for p in probs), f"{what}: expected a problem containing {text!r}, got {probs}")


PAIRS = [{"published_cagr_path": "portfolio.cagr", "ltp_block_path": "ltp"},
         {"published_cagr_path": "portfolios.low.cagr", "ltp_block_path": "ltp.low_track"},
         {"published_cagr_path": "portfolios.mid.cagr", "ltp_block_path": "ltp.mid_track"}]
G = {"topic": "some-topic", "leg": "uk", "leg_args": ["--exchange", "LSE"], "pairs": PAIRS}

# both run: published = drop books; run-level headline block, a per-track block with counts only
# (piotroski style) and a per-track block with full provenance (cash-conversion style)
BOTH = {
    "universe": "LSE",
    "portfolio": {"cagr": 4.46, "sharpe_ratio": 0.31},
    "portfolios": {"low": {"cagr": 5.42}, "mid": {"cagr": 8.83}},
    "period_data": [
        {"year": 2001, "portfolio_return": 1.2, "stocks_held": 20,
         "portfolio_return_ltp": 1.1, "stocks_held_ltp": 21, "ltp_filled": 1},
        {"year": 2002, "portfolio_return": 0.0, "stocks_held": 0, "msg": "cash"},
    ],
    "exit_method": "both",
    "ltp": {"exit_method": "both", "missing_exits": 5, "ltp_filled": 4, "still_missing": 1,
            "fills": [{"symbol": s} for s in "ABCD"],
            "drop_portfolio": {"cagr": 4.46}, "ltp_portfolio": {"cagr": 4.35},
            "low_track": {"drop_portfolio": {"cagr": 5.42}, "ltp_portfolio": {"cagr": 5.38}},
            "mid_track": {"missing_exits": 3, "ltp_filled": 3, "still_missing": 0,
                          "fills": [{"symbol": s} for s in "XYZ"],
                          "drop_portfolio": {"cagr": 8.83}, "ltp_portfolio": {"cagr": 8.64}}},
}
DROP = gate.strip_ltp(BOTH)
LTP = {**copy.deepcopy(DROP), "portfolio": {"cagr": 4.35, "sharpe_ratio": 0.29},
       "portfolios": {"low": {"cagr": 5.38}, "mid": {"cagr": 8.64}},
       "exit_method": "ltp", "ltp": {"exit_method": "ltp", "missing_exits": 5, "ltp_filled": 4,
                                     "still_missing": 1, "fills": []}}


def mutate(d, fn):
    d = copy.deepcopy(d)
    fn(d)
    return d


def test_stage_a():
    deltas, probs = gate.check_stage_a(G, BOTH, DROP, LTP, with_ltp=True)
    check(probs == [], f"good leg flagged: {probs}")
    check([x[3] for x in deltas] == [-0.11, -0.04, -0.19], f"deltas {deltas}")
    check(deltas[1][4:] == (None, None), "per-track block without counts reports None counts")
    audited = mutate(BOTH, lambda x: x.update(period_ltp_audit=[{"year": 2001}]) or x["period_data"][0].update(ltp_audit={}))
    _, probs = gate.check_stage_a(G, audited, DROP)
    check(probs == [], f"both-run audit keys counted as a drop-vs-both difference: {probs}")

    cases = [
        ("both missing", dict(b=None), "both run missing"),
        ("drop missing", dict(d=None), "drop run missing"),
        ("ltp missing", dict(l=None, with_ltp=True), "ltp run missing"),
        ("both error", dict(b={**BOTH, "error": "No data"}), "both run error: No data"),
        ("drop book", dict(b=mutate(BOTH, lambda x: x["ltp"]["mid_track"]["drop_portfolio"].update(cagr=8.9))),
         "drop book != published: portfolios.mid.cagr"),
        ("m != f + s", dict(b=mutate(BOTH, lambda x: x["ltp"].update(still_missing=2))), "counts inconsistent"),
        ("f != len(fills)", dict(b=mutate(BOTH, lambda x: x["ltp"]["mid_track"]["fills"].pop())),
         "counts inconsistent"),
        ("run-level block lost a count", dict(b=mutate(BOTH, lambda x: x["ltp"].pop("still_missing"))),
         "counts inconsistent"),
        ("track f > m", dict(b=mutate(BOTH, lambda x: x["ltp"]["low_track"].update(missing_exits=2, ltp_filled=3))),
         "track counts inconsistent"),
        ("block gone", dict(b=mutate(BOTH, lambda x: x["ltp"].pop("mid_track"))), "path missing"),
        ("drop has _ltp", dict(d=mutate(DROP, lambda x: x["period_data"][0].update(portfolio_return_ltp=1.1))),
         "drop run carries ltp keys"),
        ("drop has ltp_filled", dict(d=mutate(DROP, lambda x: x["period_data"][1].update(ltp_filled=0))),
         "drop run carries ltp keys"),
        ("drop has exit_method", dict(d={**DROP, "exit_method": "drop"}), "drop run carries ltp keys"),
        ("drop has period_ltp_audit", dict(d={**DROP, "period_ltp_audit": [{"year": 2001}]}),
         "drop run carries ltp keys"),
        ("drop has ltp_audit", dict(d=mutate(DROP, lambda x: x["period_data"][0].update(ltp_audit={}))),
         "drop run carries ltp keys"),
        ("drop != both", dict(d=mutate(DROP, lambda x: x["period_data"][0].update(stocks_held=19))),
         "drop run != both run (first difference) .period_data[0].stocks_held"),
        ("drop pub differs", dict(d=mutate(DROP, lambda x: x["portfolio"].update(cagr=4.5))),
         "drop-run vs both-run published CAGR differ"),
        ("ltp run book", dict(l=mutate(LTP, lambda x: x["portfolios"]["low"].update(cagr=5.40))),
         "ltp run's published portfolios.low.cagr=5.4 != both run's LTP book 5.38"),
    ]
    for what, kw, text in cases:
        args = dict(b=BOTH, d=DROP, l=LTP, with_ltp=False)
        args.update(kw)
        _, probs = gate.check_stage_a(G, args["b"], args["d"], args["l"], args["with_ltp"])
        has(probs, text, what)


def test_stage_a_adapters():
    # graham-timing: output keyed by universe, published CAGR an unrounded fraction
    g = {**G, "topic": "graham-timing", "pairs": [{"published_cagr_path": "LSE.portfolio.cagr", "ltp_block_path": "LSE.ltp"}]}
    blk = {"missing_exits": 2, "ltp_filled": 2, "still_missing": 0, "fills": [{}, {}],
           "drop_portfolio": {"cagr": 8.76}, "ltp_portfolio": {"cagr": 8.81}}
    b = {"LSE": {"portfolio": {"cagr": 0.08761234}, "exit_method": "both", "ltp": blk}}
    d = {"LSE": {"portfolio": {"cagr": 0.08761234}}}
    _, probs = gate.check_stage_a(g, b, d)
    check(probs == [], f"fraction CAGR flagged: {probs}")
    _, probs = gate.check_stage_a(g, mutate(b, lambda x: x["LSE"]["portfolio"].update(cagr=0.0901)), d)
    has(probs, "drop book != published", "fraction CAGR off")
    _, probs = gate.check_stage_a(g, b, {"LSE": {"portfolio": {"cagr": 0.0877}}})
    has(probs, "drop-run vs both-run published CAGR differ", "fraction drop vs both")

    # capex-efficiency: save_results output synthesized by adapt()
    with tempfile.TemporaryDirectory() as tmp:
        files = {}
        for mode, extra, cols in (("both", {"exit_method": "both", "ltp": {
                "missing_exits": 1, "ltp_filled": 1, "still_missing": 0, "fills": [{}],
                "drop_portfolio": {"cagr": 4.01}, "ltp_portfolio": {"cagr": 3.44}}},
                "year,portfolio_return,portfolio_return_ltp"), ("drop", {}, "year,portfolio_return")):
            final = os.path.join(tmp, f"uk.{mode}.json")
            os.makedirs(final[:-5] + ".d")
            json.dump({"portfolio": {"cagr": 0.0401, "sharpe_ratio": 0.2}, **extra},
                      open(os.path.join(final[:-5] + ".d", "capex-efficiency_metrics_LSE.json"), "w"))
            open(os.path.join(final[:-5] + ".d", "returns_LSE.csv"), "w").write(cols + "\n2001,1.0" + (",0.9" if extra else "") + "\n")
            check(gate.adapt("capex-efficiency", ["--exchange", "LSE"], final), f"adapt {mode}")
            files[mode] = json.load(open(final))
        check(files["both"]["portfolio"]["cagr"] == 4.01, "adapt converts the fraction to percent")
        check("portfolio_return_ltp" in files["both"]["csv_header"], "adapt keeps the CSV header")
        check(files["drop"]["metrics"]["portfolio"]["sharpe_ratio"] == 0.2 and "returns_LSE.csv" in files["drop"]["csv"],
              "adapt keeps the whole run for det/flip")
        g = {**G, "topic": "capex-efficiency", "pairs": [PAIRS[0]]}
        _, probs = gate.check_stage_a(g, files["both"], files["drop"])
        check(probs == [], f"capex adapter flagged: {probs}")
        bad = mutate(files["drop"], lambda x: x["csv_header"].update(portfolio_return_ltp=0))
        _, probs = gate.check_stage_a(g, files["both"], bad)
        has(probs, "drop run carries ltp keys", "capex drop CSV with an _ltp column")
        check(not gate.adapt("capex-efficiency", ["--exchange", "JPX"], os.path.join(tmp, "uk.drop.json")),
              "adapt reports a missing metrics file")


def test_det():
    rep = copy.deepcopy(BOTH)
    check(gate.check_det(BOTH, rep, "both") == [], "identical runs flagged")
    has(gate.check_det(BOTH, mutate(rep, lambda x: x["ltp"]["mid_track"]["ltp_portfolio"].update(cagr=8.65)), "both"),
        "det: both run != its repeat (first difference) .ltp.mid_track.ltp_portfolio.cagr", "one float")
    has(gate.check_det(BOTH, mutate(rep, lambda x: x["ltp"]["fills"].reverse()), "both"),
        ".ltp.fills[0].symbol", "fill order")
    has(gate.check_det(BOTH, mutate(rep, lambda x: x["period_data"].pop()), "both"), "length 2 vs 1", "a period lost")
    has(gate.check_det(BOTH, None, "drop"), "det: drop run or its repeat missing", "repeat missing")
    saved = gate.VOLATILE
    try:
        gate.VOLATILE = ("elapsed_seconds",)
        check(gate.check_det({**BOTH, "elapsed_seconds": 1}, {**rep, "elapsed_seconds": 2}, "both") == [],
              "volatile key not stripped")
    finally:
        gate.VOLATILE = saved


def test_flip():
    check(gate.check_flip(DROP, copy.deepcopy(DROP), "drop") == [], "default == explicit drop flagged")
    has(gate.check_flip(DROP, mutate(DROP, lambda x: x["portfolio"].update(cagr=4.47)), "drop"),
        "flip: default run != --exit-method drop run (first difference) .portfolio.cagr", "differ")
    has(gate.check_flip(BOTH, copy.deepcopy(BOTH), "drop"), "default is drop but the default run carries ltp keys",
        "drop default running a non-drop mode")
    check(gate.check_flip(LTP, copy.deepcopy(LTP), "ltp") == [], "default == explicit ltp flagged")
    has(gate.check_flip(DROP, copy.deepcopy(DROP), "ltp"), "flip: default run's exit_method [] != ltp",
        "ltp default running drop")
    mixed = mutate(LTP, lambda x: x["ltp"].update(exit_method="both"))
    has(gate.check_flip(mixed, copy.deepcopy(mixed), "ltp"), "exit_method ['both', 'ltp'] != ltp", "mixed modes")
    has(gate.check_flip(None, DROP, "drop"), "flip: default run or --exit-method drop run missing", "missing")


def test_runs_and_commands():
    check(gate.runs_for(["stage-a"]) == ["both", "drop"], "stage-a runs")
    check(gate.runs_for(["stage-a"], with_ltp=True) == ["both", "drop", "ltp"], "stage-a --with-ltp runs")
    check(gate.runs_for(["det"], det_mode="both") == ["both", "both.rep"], "det runs")
    saved = gate.DEFAULT_EXIT_METHOD
    try:
        gate.DEFAULT_EXIT_METHOD = "drop"
        check(gate.runs_for(["stage-a", "flip", "det"], det_mode="drop") == ["both", "drop", "default", "drop.rep"],
              "all checks reuse the drop run")
        gate.DEFAULT_EXIT_METHOD = "ltp"
        check(gate.runs_for(["flip"]) == ["ltp", "default"], "flip follows DEFAULT_EXIT_METHOD")
    finally:
        gate.DEFAULT_EXIT_METHOD = saved
    c = gate.command(G, "default", "o.json", "py")
    check(c == ["py", "some-topic/backtest.py", "--exchange", "LSE", "--output", "o.json"], f"default command {c}")
    c = gate.command(G, "drop.rep", "o.json", "py")
    check(c[4:6] == ["--exit-method", "drop"], f"repeat command {c}")
    check(gate.classify(0, True, "fine", 1) == "ok", "ok run")
    check(gate.classify(0, True, "Parquet magic bytes not found", 1) == "retry", "magic bytes fails an rc=0 run")
    check(gate.classify(0, False, "fine", 1) == "retry", "rc=0 without output")
    check(gate.classify(1, False, "Traceback\nKeyError", 1) == "retry", "first crash retried once")
    check(gate.classify(1, False, "Traceback\nKeyError", 2) == "crash", "second crash stops")
    check(gate.classify(1, False, "Traceback\nCetaResearchError: Rate limited. Retry after 60s", 3) == "retry",
          "transient traceback keeps retrying")
    check(gate.classify(1, False, "Traceback\nSubmit failed (503): busy", 3) == "retry", "5xx is transient")
    check(gate.classify(-9, False, "Phase 1", 3) == "retry", "killed run (timeout) retried")
    check(gate.classify(1, False, "No data available. Exiting.", 1) == "crash", "empty market stops at once")
    check(gate.classify(0, False, "No data available. Skipping.", 1) == "crash", "empty market, rc 0, no output")
    check(gate.classify(1, False, "Rate limited. Retry after 60s\nNo data available.", 1) == "retry",
          "a transient behind the empty result still retries")


def test_load_legs():
    w = os.path.join(gate.REPO, "scripts", "gate_legs", "wave1.json")
    legs = gate.load_legs(w)
    check(len({(g["topic"], g["leg"]) for g in legs}) == len(legs), "wave1 legs unique")
    check({g["topic"] for g in legs} == {"cyclical-timing", "asset-growth", "fcf-conversion", "cash-conversion",
                                         "graham-net-net"}, "wave1 topics")
    sel = gate.load_legs(w, ["cash-conversion"], ["japan"])
    check(len(sel) == 1 and sel[0]["leg_args"] == ["--preset", "japan"] and len(sel[0]["pairs"]) == 4, "filter")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "flat.json")
        json.dump([{"topic": "low-pe", "leg_args": ["--exchange", "NYSE,NASDAQ"], "pairs": [PAIRS[0]]},
                   {"topic": "sp500-survivorship", "leg_args": [], "pairs": [PAIRS[0]]}], open(p, "w"))
        check([g["leg"] for g in gate.load_legs(p)] == ["NYSE_NASDAQ", "default"], "stage A flat format")
        for what, legs_json in (
                ("bad name", {"low-pe": {"pairs": [PAIRS[0]], "legs": {"u k": ["--exchange", "LSE"]}}}),
                ("no topic", {"no-such-topic": {"pairs": [PAIRS[0]], "legs": {"uk": ["--exchange", "LSE"]}}}),
                ("no pairs", {"low-pe": {"pairs": [], "legs": {"uk": ["--exchange", "LSE"]}}}),
                ("duplicate", [{"topic": "low-pe", "leg_args": ["--exchange", "LSE"], "pairs": [PAIRS[0]]}] * 2)):
            json.dump(legs_json, open(p, "w"))
            try:
                gate.load_legs(p)
                check(False, f"load_legs accepted {what}")
            except SystemExit as e:
                check(not str(e).startswith("FAIL"), f"load_legs accepted {what}")


def test_report():
    with tempfile.TemporaryDirectory() as tmp:
        g = {**G, "topic": "low-pe"}
        os.makedirs(os.path.join(tmp, "low-pe"))
        for run, d in (("both", BOTH), ("drop", DROP), ("default", DROP), ("drop.rep", DROP)):
            json.dump(d, open(gate.run_path(tmp, g, run), "w"))
        json.dump({"seconds": 38}, open(gate.run_path(tmp, g, "drop")[:-5] + ".meta.json", "w"))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            bad = gate.report([g], tmp, ["stage-a", "det", "flip"], det_mode="drop")
        check(bad == 0, f"good leg reported bad:\n{out.getvalue()}")
        rows = json.load(open(os.path.join(tmp, "gate_report.json")))
        check(rows[0]["seconds"]["drop"] == 38 and rows[0]["problems"] == [], "gate_report.json row")
        json.dump(mutate(DROP, lambda x: x["portfolio"].update(sharpe_ratio=0.3)),
                  open(gate.run_path(tmp, g, "drop.rep"), "w"))
        with contextlib.redirect_stdout(out):
            bad = gate.report([g], tmp, ["stage-a", "det", "flip"], det_mode="drop")
        check(bad == 1 and "det: drop run != its repeat" in out.getvalue(), "report catches a det failure")


if __name__ == "__main__":
    for t in (test_stage_a, test_stage_a_adapters, test_det, test_flip, test_runs_and_commands, test_load_legs,
              test_report):
        t()
        print(f"ok {t.__name__}")
    print("all gate_ltp checks passed")
