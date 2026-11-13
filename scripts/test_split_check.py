#!/usr/bin/env python3
"""Offline check of scripts/split_check.py on synthetic bars (no network).

One semi-annual leg of four periods (a cash period, two full ones and a partial one at half a period) holds:
  A   a +100% step undone by a -45% step five trading days later: both net out, nothing is rebuilt;
  B   a 2x step and a 0.5x step six trading days apart, past the reversal window: two round-ratio artifacts whose
      rebuilds cancel inside the one holding;
  H   a round-ratio one-way step (adjClose and close both double, no split row): an artifact, its step divided out;
  C   an 11:10 bonus whose split row adjClose didn't absorb (adjClose and close both step by 10/11): rebuilt with
      the bonus shares restored;
  D   a 2:1 split row adjClose absorbed (the close halves, adjClose flat): no rebuild;
  E   a 2.5x one-way step with a split row three days before it: an artifact;
  S   an unadjusted 2:1 split (both halve on the split row's day): one artifact, rebuilt once and not again as a
      split row;
  E2  a 2.5x one-way step with a split row four days before it: no split signature, no rebuild;
  F   a 4-for-5 consolidation as a factor step on a flat close (adjClose up 25%): rebuilt on the close's move;
  G   a 0.5x step on the day trading resumes after five zero-volume days: a repricing, no rebuild.
A second leg on the same periods pins the edges and the thresholds:
  X     a 0.5x step on the exit bar, undone the bar after: the undoing is outside the holding, so the step doesn't
        net out and is a round-ratio artifact;
  T     a 0.5x step and a 2:1 split row on the bar after the exit bar: neither is read;
  M     a 2x step that gives back 0.64x two bars later: outside the reversal tolerance, an artifact;
  DUP   a second, half-price bar on the entry date and a second bar with a flat close on its 2x step's day: the
        highest adjClose, then the highest close, is kept, so the step is a round-ratio artifact;
  N     a 1041131:1000000 split row with adjClose up 0.5%: under the near-one factor floor, absorbed;
  R     a 0.525x step with the close moving the same: inside the 6% round-ratio tolerance, an artifact;
  FL    a 2x adjClose step on a flat close with no split row: no split signature;
  Z     a 0.5x step on a zero-volume day after another: not a reopening, an artifact;
  AF    a 2.5x one-way step with a split row two days after it: an artifact;
  LATE  a 2x step in a holding whose first bar is 12 days after the rebalance date: unpriced, nothing read;
  ON11  a holding whose first bar is 11 days after it: priced;
  SUB   a 2x spike undone by two moves under the step threshold, then an unadjusted 2:1 split later in the
        holding: the spike nets out alone and the split is rebuilt (alone in a leg, it puts the leg over the bar);
  TP    a 0.5x step and a 2:1 split row on the first bar after the partial period's exit date, which the backtest
        prices at that day's close: neither is read;
  XP    a 0.5x step on the partial period's exit date, undone the bar after: the undoing is past the priced exit,
        so the step doesn't net out and is a round-ratio artifact.
Every period has bars after its exit date, as the warehouse has once the run's end date has passed. Each leg's
rebuilt excess is checked against metrics.compute_metrics on returns rebuilt here by hand (each
holding's corrections compounded, two periods per year, the partial period's fraction), and so is each rebuild's
own move. A leg with no artifact reads under the bar, and a move equal to the bar is over it (the bar set to a
rebuild's exact move). A person's "no change" on H leaves it out of the rebuilds and
the net and lists it in the leg's not_artifact, beside the every-candidate net; on a leg whose only artifact it is,
the verdict turns under the bar. main() runs the three queries per file on an in-memory warehouse for both legs
with that call and writes the same records, its bars query reading no partial holding's bar past the exit date; a
key that matches no candidate is refused, and so is a symbol holding a quote or a semicolon, before any SQL is
built. The warehouse loads each table with one INSERT. Every mutant of split_check.py (the reversal window, the
split-row window, the absorption test, the rebuild's sign, each threshold above and more) must trip a check.

Usage:
    python scripts/test_split_check.py [repo root]
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import types
from datetime import date, timedelta

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "scripts", "split_check.py")
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from data_utils import utc_epoch   # noqa: E402
from metrics import compute_metrics   # noqa: E402

BASE, VOL = 10.0, 1000.0
P0, P1 = ("2020-01-01", "2020-07-01"), ("2020-07-01", "2021-01-01")
P2, P3 = ("2021-01-01", "2021-07-01"), ("2021-07-01", "2021-10-01")   # P3: the partial period, half a period
SPY = [0.03, 0.05, 0.02, 0.01]
STORED = [0.0, 0.12, 0.05, -0.03]
FRACTIONS = [1.0, 1.0, 1.0, 0.5]


def day(period, k):
    return (date.fromisoformat(period[0]) + timedelta(days=k)).isoformat()


# symbol: (period, events as (day, close factor, adjClose factor), zero-volume days, split rows (day, num, den)
#          [, the first bar's day])
HOLDINGS = {
    "A": (P1, [(40, 2.0, 2.0), (45, 0.55, 0.55)], (), ()),
    "B": (P1, [(60, 2.0, 2.0), (66, 0.5, 0.5)], (), ()),
    "H": (P1, [(100, 2.0, 2.0)], (), ()),
    "C": (P2, [(50, 10 / 11, 10 / 11), (120, 1.2, 1.2)], (), ((50, 11, 10),)),
    "D": (P2, [(70, 0.5, 1.0)], (), ((70, 2, 1),)),
    "E": (P2, [(90, 0.4, 0.4)], (), ((87, 5, 2),)),
    "S": (P2, [(110, 0.5, 0.5)], (), ((110, 2, 1),)),
    "E2": (P3, [(30, 0.4, 0.4)], (), ((26, 5, 2),)),
    "F": (P3, [(40, 1.0, 1.25)], (), ((40, 4, 5),)),
    "G": (P3, [(55, 0.5, 0.5)], range(50, 55), ()),
}
EDGE = {   # P1's exit bar is its day 185 (the first bar after the exit date); P3's is its day 92, the exit date
    "X": (P1, [(185, 0.5, 0.5), (186, 2.0, 2.0)], (), ()),
    "T": (P1, [(186, 0.5, 0.5)], (), ((186, 2, 1),)),
    "M": (P1, [(60, 2.0, 2.0), (62, 0.64, 0.64)], (), ()),
    "DUP": (P1, [(80, 2.0, 2.0)], (), ()),
    # a 2:1 split adjClose follows down with the close, corrected 2 bars later: the split row nets out with its step
    "NS": (P1, [(120, 0.5, 0.5), (122, 1.0, 2.0)], (), ((120, 2, 1),)),
    # an adjClose glitch x2, then a real 2:1 split adjClose takes from the glitched level: nets out the same way
    "NG": (P1, [(140, 1.0, 2.0), (142, 0.5, 0.5)], (), ((142, 2, 1),)),
    "N": (P2, [(50, 0.96, 1.005)], (), ((50, 1041131, 1000000),)),
    "R": (P2, [(70, 0.525, 0.525)], (), ()),
    "FL": (P2, [(90, 1.0, 2.0)], (), ()),
    "Z": (P2, [(110, 0.5, 0.5)], range(108, 112), ()),
    "AF": (P2, [(130, 0.4, 0.4)], (), ((132, 5, 2),)),
    "LATE": (P3, [(40, 2.0, 2.0)], (), (), 12),
    "ON11": (P3, [], (), (), 11),
    "SUB": (P1, [(30, 2.0, 2.0), (31, 0.7, 0.7), (32, 1 / 1.4, 1 / 1.4), (120, 0.5, 0.5)], (), ((120, 2, 1),)),
    "TP": (P3, [(93, 0.5, 0.5)], (), ((93, 2, 1),)),
    "XP": (P3, [(92, 0.5, 0.5), (93, 2.0, 2.0)], (), ()),
}
DUPLICATES = [("DUP", P1, 1, 5.0, 5.0), ("DUP", P1, 80, 10.0, 20.0)]   # (symbol, period, day, close, adjClose)
NETS, ROUND_WHY = "reverses inside the window, nets out", "round ratio with the close moving the same"
REOPEN_WHY, NO_SIGN = "trading resumed after zero-volume days: a repricing", "no split signature"
STEPS = sorted([   # (symbol, day, netted, artifact, why)
    ("A", day(P1, 40), True, False, NETS), ("A", day(P1, 45), True, False, NETS),
    ("B", day(P1, 60), False, True, ROUND_WHY), ("B", day(P1, 66), False, True, ROUND_WHY),
    ("H", day(P1, 100), False, True, ROUND_WHY),
    ("E", day(P2, 90), False, True, "split row"), ("S", day(P2, 110), False, True, "split row"),
    ("E2", day(P3, 30), False, False, NO_SIGN), ("G", day(P3, 55), False, False, REOPEN_WHY)])
EDGE_STEPS = sorted([
    ("X", day(P1, 185), False, True, ROUND_WHY), ("M", day(P1, 60), False, True, ROUND_WHY),
    ("DUP", day(P1, 80), False, True, ROUND_WHY), ("R", day(P2, 70), False, True, ROUND_WHY),
    ("FL", day(P2, 90), False, False, NO_SIGN), ("Z", day(P2, 110), False, True, ROUND_WHY),
    ("AF", day(P2, 130), False, True, "split row"),
    ("NS", day(P1, 120), True, False, NETS), ("NS", day(P1, 122), True, False, NETS),
    ("NG", day(P1, 140), True, False, NETS), ("NG", day(P1, 142), True, False, NETS),
    ("SUB", day(P1, 30), True, False, NETS), ("SUB", day(P1, 120), False, True, "split row"),
    ("XP", day(P3, 92), False, True, ROUND_WHY)])
SPLIT_ROWS = sorted([   # (symbol, row day, unadjusted): every split row inside a holding
    ("C", day(P2, 50), True), ("D", day(P2, 70), False), ("E", day(P2, 87), False), ("S", day(P2, 110), True),
    ("E2", day(P3, 26), False), ("F", day(P3, 40), True)])
EDGE_SPLIT_ROWS = sorted([("N", day(P2, 50), False), ("AF", day(P2, 132), False),
                          ("NS", day(P1, 120), True), ("NG", day(P1, 142), True), ("SUB", day(P1, 120), True)])
# (symbol, day): (booked return, names held, correction, kind); the holding's return times the correction
REBUILDS = {
    ("B", day(P1, 60)): (0.0, 3, 0.5, "step"), ("B", day(P1, 66)): (0.0, 3, 2.0, "step"),
    ("H", day(P1, 100)): (1.0, 3, 0.5, "step"),
    ("C", day(P2, 50)): (10 / 11 * 1.2 - 1, 4, 1.1, "bonus or split not in adjClose"),
    ("E", day(P2, 90)): (-0.6, 4, 2.5, "step"), ("S", day(P2, 110)): (-0.5, 4, 2.0, "step"),
    ("F", day(P3, 40)): (0.25, 3, 0.8, "factor step, close flat"),
}
EDGE_REBUILDS = {
    ("X", day(P1, 185)): (-0.5, 7, 2.0, "step"), ("M", day(P1, 60)): (2 * 0.64 - 1, 7, 0.5, "step"),
    ("DUP", day(P1, 80)): (1.0, 7, 0.5, "step"), ("R", day(P2, 70)): (-0.475, 5, 1 / 0.525, "step"),
    ("Z", day(P2, 110)): (-0.5, 5, 2.0, "step"), ("AF", day(P2, 130)): (-0.6, 5, 2.5, "step"),
    ("SUB", day(P1, 120)): (-0.5, 7, 2.0, "step"), ("XP", day(P3, 92)): (-0.5, 4, 2.0, "step"),
}
# universe: (holdings, extra bar rows, steps, split rows, rebuilds)
CASES = {"TEST": (HOLDINGS, (), STEPS, SPLIT_ROWS, REBUILDS),
         "EDGE": (EDGE, DUPLICATES, EDGE_STEPS, EDGE_SPLIT_ROWS, EDGE_REBUILDS)}
PERIOD_OF = {P1[0]: 1, P2[0]: 2, P3[0]: 3}
RATIO = [{"symbol": "A", "min_price": 1.0, "max_price": 1500.0, "price_ratio": 1500.0},
         {"symbol": "B", "min_price": 1.0, "max_price": 150.0, "price_ratio": 150.0},
         {"symbol": "C", "min_price": 2.0, "max_price": 10.0, "price_ratio": 5.0}]
H_KEY = f"H:{P1[0]}:{day(P1, 100)}"   # a person's "no change" on H's step
SUB_KEY = f"SUB:{P1[0]}:{day(P1, 120)}"
V_LEG, V_KEY = {"Q": (P1, [(30, 1.3, 1.3)], (), ()), "V": (P1, [(50, 2.0, 2.0)], (), ())}, f"V:{P1[0]}:{day(P1, 50)}"


def bars(symbol, period, events=(), quiet=(), first=1):
    """Daily bars from the first bar's day after the rebalance date to 3 days past the exit date, at BASE with
    volume VOL; each event multiplies every bar from its day on."""
    r, x = (date.fromisoformat(d) for d in period)
    out, c, a = [], BASE, BASE
    for k in range(1, (x - r).days + 4):
        for d, fc, fa in events:
            if d == k:
                c, a = c * fc, a * fa
        if k >= first:
            out.append({"symbol": symbol, "date": (r + timedelta(days=k)).isoformat(), "close": c, "adjClose": a,
                        "volume": 0.0 if k in quiet else VOL})
    return out


def fixture(holdings=HOLDINGS, universe="TEST", extra=()):
    """(results dict, bar rows, split rows) for the leg."""
    bar_rows, split_rows, held = [], [], {}
    for s, (p, ev, quiet, sp, *first) in holdings.items():
        bar_rows += bars(s, p, ev, quiet, *first)
        split_rows += [{"symbol": s, "date": day(p, k), "numerator": n, "denominator": d} for k, n, d in sp]
        held.setdefault(p, []).append(s)
    bar_rows += [{"symbol": s, "date": day(p, k), "close": c, "adjClose": a, "volume": VOL} for s, p, k, c, a in extra]
    rows = [{"rebalance_date": P0[0], "exit_date": P0[1], "portfolio_return": STORED[0], "spy_return": SPY[0],
             "stocks_held": 0, "holdings": "CASH (2 passed)"}]
    for i, p in enumerate((P1, P2, P3), 1):
        names = held.get(p, [])
        rows.append({"rebalance_date": p[0], "exit_date": p[1], "portfolio_return": STORED[i], "spy_return": SPY[i],
                     "stocks_held": len(names), "holdings": ",".join(names) or "CASH (0 passed)"})
    rows[-1].update(partial=True, fraction=0.5)
    m = compute_metrics(STORED, SPY, 2, fractions=FRACTIONS)
    res = {"universe": universe, "frequency": "semi-annual", "period_data": rows,
           "portfolio": {"cagr": round(m["portfolio"]["cagr"] * 100, 2)},
           "comparison": {"excess_cagr": round(m["comparison"]["excess_cagr"] * 100, 2)},
           "data_checks": {"large_moves": [{"symbol": "H", "rebalance_date": P1[0], "return": 1.0}]},
           "ltp": {"large_moves": [{"symbol": "E", "rebalance_date": P2[0], "return": -0.6}]}}
    return res, bar_rows, split_rows


def case_fixture(universe):
    holdings, extra = CASES[universe][:2]
    return fixture(holdings, universe, extra)


def excess(rets):
    return compute_metrics(rets, SPY, 2, fractions=FRACTIONS)["comparison"]["excess_cagr"] * 100


def key_of(universe, k):
    return f"{k[0]}:{CASES[universe][0][k[0]][0][0]}:{k[1]}"


def moved(universe, corrections):
    """The stored returns with each (symbol, day) correction applied, compounded within a holding."""
    holdings, rebuilds, total = CASES[universe][0], CASES[universe][4], {}
    for k in corrections:
        r_b, n, f, _ = rebuilds[k]
        s = k[0]
        total[s] = (r_b, n, total.get(s, (r_b, n, 1.0))[2] * f, holdings[s][0][0])
    rets = list(STORED)
    for r_b, n, f, reb in total.values():
        rets[PERIOD_OF[reb]] += (1 + r_b) * (f - 1) / n
    return rets


def near(a, b, tol):
    return a is not None and b is not None and abs(a - b) <= tol


def case_failures(universe, leg, not_artifact=()):
    """The leg's steps, split rows, rebuilds and nets against the case's, with not_artifact a person's calls."""
    _, _, steps, split_rows, rebuilds = CASES[universe]
    out = []
    got = sorted((c["symbol"], c["date"], c["netted"], c["artifact"], c["why"]) for c in leg["steps"])
    if got != steps:
        out.append(f"{universe} steps {got}, want {steps}")
    got = sorted((t["symbol"], t["date"], t["unadjusted"]) for t in leg["split_rows_in_windows"])
    if got != split_rows:
        out.append(f"{universe} split rows {got}, want {split_rows}")
    x0 = excess(STORED)
    kept = [k for k in rebuilds if key_of(universe, k) not in not_artifact]
    marked = [k for k in rebuilds if key_of(universe, k) in not_artifact]
    for field, want_keys in (("rebuilds", kept), ("not_artifact", marked)):
        entries = leg.get(field) or []
        seen = {(e["symbol"], e.get("bar") or e["date"]): e for e in entries}
        if sorted(seen) != sorted(want_keys) or len(entries) != len(want_keys):
            out.append(f"{universe} {field} {sorted(seen)} ({len(entries)}), want {sorted(want_keys)}")
        for k in want_keys:
            e = seen.get(k)
            if e is None:
                continue
            r_b, n, f, kind = rebuilds[k]
            want_move = excess(moved(universe, [k])) - x0
            if not (near(e["r_booked"], r_b, 1e-5) and near(e["r_rebuilt"], (1 + r_b) * f - 1, 1e-5)
                    and e["n"] == n and e["kind"] == kind and near(e["leg_excess_move"], want_move, 1e-3)
                    and e.get("key") == key_of(universe, k)):
                out.append(f"{universe} {field} {k}: booked {e.get('r_booked')} rebuilt {e.get('r_rebuilt')} n "
                           f"{e.get('n')} kind {e.get('kind')} move {e.get('leg_excess_move')} key {e.get('key')}; "
                           f"want {r_b:.6f}, {(1 + r_b) * f - 1:.6f}, {n}, {kind}, {want_move:.4f}, "
                           f"{key_of(universe, k)}")
    x1, x_all = excess(moved(universe, kept)), excess(moved(universe, list(rebuilds)))
    if not (near(leg["net_move"], x1 - x0, 1e-3) and near(leg.get("net_move_every_candidate"), x_all - x0, 1e-3)
            and near(leg["rebuilt"][1], x1, 0.006) and near(leg["recompounded"][1], x0, 0.006)):
        out.append(f"{universe} leg: net {leg['net_move']} (every candidate {leg.get('net_move_every_candidate')}) "
                   f"rebuilt {leg['rebuilt']} recompounded {leg['recompounded']}; want net {x1 - x0:.4f} (every "
                   f"candidate {x_all - x0:.4f}), excess {x0:.2f} to {x1:.2f}")
    return out


def leg_failures(mod):
    res, bar_rows, split_rows = fixture()
    leg = mod.check_leg(res, bar_rows, split_rows, RATIO)
    out = case_failures("TEST", leg)
    want = {"universe": "TEST", "periods_per_year": 2, "held_lines": 10, "held_line_periods": 10,
            "line_periods_priced": 10, "ratio_over_100": 2, "stored": [res["portfolio"]["cagr"],
                                                                        res["comparison"]["excess_cagr"]]}
    got = {k: leg.get(k) for k in want}
    if got != want:
        out.append(f"leg fields {got}, want {want}")
    if [r["symbol"] for r in leg["ratio_over_1000"]] != ["A"]:
        out.append(f"ratio over 1000 {leg['ratio_over_1000']}")
    if leg["check_L"] != {"large_moves": res["data_checks"]["large_moves"],
                          "ltp_large_moves": res["ltp"]["large_moves"]}:
        out.append(f"check L {leg['check_L']}")
    if leg["verdict"] != "over the bar" or not leg["over_bar"]:
        out.append(f"verdict {leg['verdict']} ({leg['over_bar']}), want over the bar")
    flat, fb, fs = fixture({"Q": (P1, [(30, 1.3, 1.3)], (), ())})
    calm = mod.check_leg(flat, fb, fs, [])
    if calm["verdict"] != "under the bar" or calm["over_bar"] or calm["rebuilds"] or calm["net_move"] != 0:
        out.append(f"a leg with no artifact: verdict {calm['verdict']}, over {calm['over_bar']}, "
                   f"rebuilds {calm['rebuilds']}, net {calm['net_move']}")
    return out


def edge_failures(mod):
    """The second leg: the exit edges, the duplicate bars, the thresholds, the entry window and the reversal's
    partner; then SUB alone in a leg, which its split rebuild puts over the bar."""
    res, bar_rows, split_rows = case_fixture("EDGE")
    leg = mod.check_leg(res, bar_rows, split_rows, [])
    out = case_failures("EDGE", leg)
    want = {"held_lines": 16, "held_line_periods": 16, "line_periods_priced": 15}
    got = {k: leg.get(k) for k in want}
    if got != want:
        out.append(f"EDGE leg fields {got}, want {want} (LATE unpriced, ON11 priced)")
    alone = mod.check_leg(*fixture({"SUB": EDGE["SUB"]}), [])
    keys = [e["key"] for e in alone["rebuilds"]]
    if alone["verdict"] != "over the bar" or keys != [SUB_KEY]:
        out.append(f"SUB alone: verdict {alone['verdict']}, rebuilds {keys}; want over the bar with {SUB_KEY}")
    return out


def not_artifact_failures(mod):
    """A person's "no change" on H, and on the only artifact of a two-name leg."""
    res, bar_rows, split_rows = fixture()
    out = case_failures("TEST", mod.check_leg(res, bar_rows, split_rows, RATIO, {H_KEY}), {H_KEY})
    one, ob, os_ = fixture(V_LEG)
    every, judged = mod.check_leg(one, ob, os_, []), mod.check_leg(one, ob, os_, [], {V_KEY})
    if every["verdict"] != "over the bar" or not every["rebuilds"]:
        out.append(f"V's leg with no call: verdict {every['verdict']}, rebuilds {len(every['rebuilds'])}")
    if not (judged["verdict"] == "under the bar" and not judged["over_bar"] and not judged["rebuilds"]
            and judged["net_move"] == 0 and judged.get("net_move_every_candidate") == every["net_move"] != 0
            and [e.get("key") for e in judged.get("not_artifact") or []] == [V_KEY]):
        out.append(f"V called no change: verdict {judged['verdict']}, over {judged['over_bar']}, rebuilds "
                   f"{len(judged['rebuilds'])}, net {judged['net_move']} (every candidate "
                   f"{judged.get('net_move_every_candidate')}), not_artifact {judged.get('not_artifact')}")
    return out


def bar_failures(mod):
    """A move equal to the bar is over it: V's leg with the bar set to V's own move, read before rounding (its one
    rebuild's move is also the leg's net)."""
    one, ob, os_ = fixture(V_LEG)
    rounded, bar = mod._rounded, mod.BAR
    try:
        mod._rounded = lambda x, nd=6: x
        mod.BAR = abs(mod.check_leg(one, ob, os_, [])["rebuilds"][0]["leg_excess_move"])
        over = mod.check_leg(one, ob, os_, [])["over_bar"]
    finally:
        mod._rounded, mod.BAR = rounded, bar
    return [] if len(over) == 2 else [f"a move equal to the bar: over_bar {over}, want the rebuild's and the net's"]


EOD_COLUMNS = (("symbol", "VARCHAR"), ("date", "DATE"), ("dateEpoch", "UINTEGER"), ("close", "DOUBLE"),
               ("adjClose", "DOUBLE"), ("volume", "DOUBLE"))
SPLIT_COLUMNS = (("symbol", "VARCHAR"), ("date", "DATE"), ("numerator", "DOUBLE"), ("denominator", "DOUBLE"))


class Warehouse:
    """Stands in for CetaResearch: stock_eod and splits_calendar in DuckDB, the warehouse's column types, from bar
    and split rows as dicts (test_same_bars_scan's warehouse too)."""

    def __init__(self, bar_rows, split_rows=()):
        self.con, self.sqls = duckdb.connect(), []
        on = lambda r: date.fromisoformat(r["date"])
        self.load("stock_eod", EOD_COLUMNS, [(b["symbol"], on(b), utc_epoch(on(b)), b["close"], b["adjClose"],
                                              b["volume"]) for b in bar_rows])
        self.load("splits_calendar", SPLIT_COLUMNS,
                  [(r["symbol"], on(r), r["numerator"], r["denominator"]) for r in split_rows])

    def load(self, table, columns, rows):
        """The table, its rows in one INSERT (a list per column, unnested side by side)."""
        self.con.execute(f"CREATE TABLE {table}({', '.join(f'{c} {t}' for c, t in columns)})")
        if rows:
            self.con.execute(f"INSERT INTO {table} SELECT {', '.join(f'unnest(?::{t}[])' for _, t in columns)}",
                             [list(col) for col in zip(*rows)])

    def query(self, sql, **_kw):
        self.sqls.append(sql)
        cur = self.con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


@contextlib.contextmanager
def fake_cr_client(wh):
    """cr_client, inside the block, a module whose CetaResearch hands back wh."""
    fake = types.ModuleType("cr_client")
    fake.CetaResearch = lambda *a, **k: wh
    saved = sys.modules.get("cr_client")
    sys.modules["cr_client"] = fake
    try:
        yield
    finally:
        if saved is None:
            sys.modules.pop("cr_client", None)
        else:
            sys.modules["cr_client"] = saved


def run_main(mod, wh, legs, flags=()):
    """main() on the legs' files through wh: (its record, None), or (None, the code it exited with)."""
    with fake_cr_client(wh), tempfile.TemporaryDirectory() as tmp:
        paths, out = [], os.path.join(tmp, "out.json")
        for res in legs:
            paths.append(os.path.join(tmp, f"{res['universe']}.json"))
            with open(paths[-1], "w") as fh:
                json.dump(res, fh)
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                mod.main(paths + ["--json", out] + list(flags))
        except SystemExit as e:
            return None, e.code
        if not os.path.exists(out):
            return None, "no record written"
        with open(out) as fh:
            return json.load(fh), None


def main_failures(mod):
    """main() on both legs' files through the warehouse with a person's call on H: three queries a file, and the
    records check_leg gives on their rows with that call. A key matching no candidate is refused."""
    legs = {u: case_fixture(u) for u in CASES}
    bar_rows = [r for _, rows, _ in legs.values() for r in rows]
    split_rows = [r for _, _, rows in legs.values() for r in rows]
    wh = Warehouse(bar_rows, split_rows)
    report, code = run_main(mod, wh, [res for res, _, _ in legs.values()], ["--not-artifact", H_KEY])
    if report is None:
        return [f"main on both legs with a person's call: exited {code}"]
    out = [] if len(wh.sqls) == 6 else [f"main sent {len(wh.sqls)} queries, want 6"]
    for u, (res, _, _) in legs.items():
        ratio, got_bars, got_splits = (wh.query(q) for q in mod.queries(res))
        want = json.loads(json.dumps(mod.check_leg(res, got_bars, got_splits, ratio, [H_KEY]), default=str))
        leg = (report.get("legs") or {}).get(u)
        if leg is None or leg.get("file") is None or {k: v for k, v in leg.items() if k != "file"} != want:
            out.append(f"main's {u} record differs from check_leg on the same query rows")
        if sorted(s[0] for s in CASES[u][2]) != sorted(c["symbol"] for c in want["steps"]):
            out.append(f"the warehouse's bars give other {u} steps than the fixture's")
        partial = {s for s, h in CASES[u][0].items() if h[0] == P3}
        if any(b["symbol"] in partial and str(b["date"])[:10] > P3[1] for b in got_bars):
            out.append(f"the bars query reads {u}'s partial holdings past their exit date")
    if report.get("bar") != 0.15:
        out.append(f"main's bar {report.get('bar')}, want 0.15")
    if report.get("not_artifact") != [H_KEY]:
        out.append(f"main's record of the person's calls {report.get('not_artifact')}, want {[H_KEY]}")
    unmatched = f"H:{P1[0]}:{day(P1, 101)}"
    rec, code = run_main(mod, Warehouse(bar_rows, split_rows), [legs["TEST"][0]], ["--not-artifact", unmatched])
    if rec is not None or not code:
        out.append(f"main took a --not-artifact that matches no candidate ({unmatched}): exit {code}")
    for bad in ("BAD';X", "BAD;X"):
        try:
            mod.queries(fixture({bad: (P1, [], (), ())})[0])
            out.append(f"a symbol holding a quote or a semicolon reached the SQL: {bad}")
        except ValueError:
            pass
    return out


def failures(src):
    try:
        mod = types.ModuleType("split_check_under_test")
        mod.__file__ = PATH
        exec(compile(src, PATH, "exec"), mod.__dict__)
        out = []
        for check in (leg_failures, edge_failures, not_artifact_failures, bar_failures, main_failures):
            try:
                out += check(mod)
            except Exception as e:   # reported as an error: main() does not count it as a caught mutant
                out.append(f"error: {check.__name__}: {type(e).__name__}: {e}")
        return out
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]


# name: [(old, new), ...]; every `old` occurs once and only once in the source
MUTANTS = {
    "a reversal window of four trading days": [("i + 1 + REVERSAL_BARS", "i + REVERSAL_BARS")],
    "a reversal window of six trading days": [("i + 1 + REVERSAL_BARS", "i + 2 + REVERSAL_BARS")],
    "a reversal looked for past the exit bar": [("min(i + 1 + REVERSAL_BARS, ext + 1)",
                                                 "min(i + 1 + REVERSAL_BARS, len(seq))")],
    "the reversal tolerance doubled": [("abs(b + la) < 0.25 * abs(la)", "abs(b + la) < 0.5 * abs(la)")],
    "split rows within two days": [("<= SPLIT_DAYS]", "< SPLIT_DAYS]")],
    "split rows within four days": [("<= SPLIT_DAYS]", "<= SPLIT_DAYS + 1]")],
    "split rows only before a step": [("if days_apart(x[0], seq[i][0]) <= SPLIT_DAYS]",
                                       "if 0 <= (dt.date.fromisoformat(seq[i][0]) - dt.date.fromisoformat(x[0])).days"
                                       " <= SPLIT_DAYS]")],
    "steps read past the exit bar": [("for i in range(ent + 1, ext + 1):", "for i in range(ent + 1, len(seq)):")],
    "split rows tested past the exit bar": [("range(ent + 1, ext + 1) if seq[i][0] >= d0",
                                             "range(ent + 1, len(seq)) if seq[i][0] >= d0")],
    "the exit bar on the exit date": [("if b[0] > xd), len(seq) - 1)", "if b[0] >= xd), len(seq) - 1)")],
    "a partial exit on the first bar after the end date": [('end = xd if p.get("partial") else plus(xd, WINDOW_DAYS)',
                                                             "end = plus(xd, WINDOW_DAYS)")],
    "the partial span fetched past its exit date": [
        ("{epoch(r)}, {epoch(end)})", "{epoch(r)}, {epoch(plus(end, WINDOW_DAYS))})"),
        ("max(end for *_, end in sp)", "max(plus(end, WINDOW_DAYS) for *_, end in sp)")],
    "a reversal netted against any later opposite step": [(' and cands[j]["date"] <= c["undone_by"]', "")],
    "the bar that undid a reversal not netted with it": [('cands[j]["date"] <= c["undone_by"]',
                                                          'cands[j]["date"] < c["undone_by"]')],
    "an entry bar late by any number of days": [("ent = 0 if seq and seq[0][0] <= plus(reb, WINDOW_DAYS) else None",
                                                 "ent = 0 if seq else None")],
    "an entry bar on the 11th day unpriced": [("seq[0][0] <= plus(reb, WINDOW_DAYS)",
                                               "seq[0][0] < plus(reb, WINDOW_DAYS)")],
    "every bar row kept": [("one_per_day(r for r in bars", "list(r for r in bars")],
    "a duplicate date keeps the lower adjClose": [("rank > best[k][0]", "rank < best[k][0]")],
    "a duplicate date keeps the lower close": [('(r.get("adjClose") or 0, r.get("close") or 0)',
                                                '(r.get("adjClose") or 0, -(r.get("close") or 0))')],
    "absorption read on the close": [("abs(math.log(a_st / pf)) < 0.1", "abs(math.log(c_st / pf)) < 0.1")],
    "absorbed and unadjusted swapped": [("abs(math.log(a_st / pf)) < 0.1", "abs(math.log(a_st / pf)) >= 0.1")],
    "no near-one factor floor": [(" and abs(math.log(pf)) > 0.05", "")],
    "a 3% round-ratio tolerance": [("abs(k / q - 1) < 0.06", "abs(k / q - 1) < 0.03")],
    "a round ratio without the close moving the same": [("(round_ratio and same and not reopen)",
                                                         "(round_ratio and not reopen)")],
    "a reopening read as an artifact": [("(round_ratio and same and not reopen)", "(round_ratio and same)")],
    "a reopening needs only a zero-volume day before": [('reopen = not c["volume"][0] and bool(c["volume"][1])',
                                                         'reopen = not c["volume"][0]')],
    "a step multiplied in, not divided out": [('c["date"], 1 / c["adj_step"])', 'c["date"], c["adj_step"])')],
    "a split rebuild inverted": [('/ t["price_factor"]) / t["adj_step"]', '/ t["price_factor"]) * t["adj_step"]')],
    "a rebuild with its sign flipped": [("(1 + r_b) * (f - 1) / n", "(1 + r_b) * (1 - f) / n")],
    "a split row on an artifact's day rebuilt again": [("if not t[\"unadjusted\"] or key in done:",
                                                        "if not t[\"unadjusted\"]:")],
    "a holding's rebuilds not compounded": [("total[key] *= f", "total[key] = f")],
    "a split row on a netted step's day rebuilt": [('if c["artifact"] or c["netted"]}', 'if c["artifact"]}')],
    "a rebuild's move equal to the bar under it": [('abs(e["leg_excess_move"]) >= BAR]',
                                                    'abs(e["leg_excess_move"]) > BAR]')],
    "a net equal to the bar under it": [("if abs(net) >= BAR:", "if abs(net) > BAR:")],
    "check L drops the fills' list": [('"ltp_large_moves": ltp.get("large_moves", [])', '"ltp_large_moves": []')],
    "sql_list refuses only quotes": [('bad = [s for s in symbols if ";" in s or "\'" in s]',
                                      'bad = [s for s in symbols if "\'" in s]')],
    "a person's no change still in the net": [('every if i["key"] not in not_artifact])', "every])")],
    "a person's no change still among the rebuilds": [('kept = [e for e in effects if e["key"] not in not_artifact]',
                                                       "kept = effects")],
    "the verdict counts a person's no change": [('for e in kept if abs(e["leg_excess_move"]) >= BAR]',
                                                 'for e in effects if abs(e["leg_excess_move"]) >= BAR]'),
                                                ("if abs(net) >= BAR:", "if abs(net_every) >= BAR:")],
    "the every-candidate net is the confirmed one": [('"net_move_every_candidate": round(net_every, 4)',
                                                      '"net_move_every_candidate": round(net, 4)')],
    "a person's no change not recorded": [('"not_artifact": [e for e in effects if e["key"] in not_artifact]',
                                           '"not_artifact": []')],
    "a leg keyed with its file though its name is free": [
        ('return universe if universe not in legs else f"{universe} ({path})"', 'return f"{universe} ({path})"')],
    "main ignores --not-artifact": [("check_leg(res, bars, splits, ratio, args.not_artifact)",
                                     "check_leg(res, bars, splits, ratio)")],
    "a key matching no candidate taken": [("    if unmatched:\n", "    if False:\n")],
}


def main():
    if not os.path.exists(PATH):
        print(f"test_split_check: FAIL ({PATH} does not exist)")
        sys.exit(1)
    with open(PATH) as fh:
        src = fh.read()
    base = failures(src)
    if base:
        print("test_split_check: FAIL")
        for f in base:
            print("  ", f)
        sys.exit(1)
    survived, broken = [], []
    for name, edits in MUTANTS.items():
        m = src
        for old, new in edits:
            if src.count(old) != 1:
                print(f"test_split_check: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            m = m.replace(old, new)
        got = failures(m)
        if not got:
            survived.append(name)
        elif all(f.startswith("error:") for f in got):
            broken.append((name, got))
    if survived or broken:
        print("test_split_check: FAIL (mutants survived:", survived, "; mutants that only raised:", broken, ")")
        sys.exit(1)
    print(f"test_split_check: PASS ({len(HOLDINGS)} + {len(EDGE)} holdings, {len(STEPS)} + {len(EDGE_STEPS)} steps, "
          f"{len(SPLIT_ROWS)} + {len(EDGE_SPLIT_ROWS)} split rows, {len(REBUILDS)} + {len(EDGE_REBUILDS)} rebuilds; "
          f"a person's call; main's queries on both legs; {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
