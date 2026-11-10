#!/usr/bin/env python3
"""Check F's review (data checks dc-3): which band flags in a results file need a hand-list entry.

A band flag needs one when swapping its holding's return to the close basis moves its leg's excess by
review_min_pp or more (data_utils.DATA_CHECKS_V3["factor_hand"]); the entry (data_utils.FACTOR_HAND) records the
holding as rebuilt, real or kept, with its evidence. At a topic's onboarding and at every rerun, run this on the
leg files, review each flag it lists, add the entries, and rerun the leg. Gate 15 of verify_floor_guard.py
refuses a committed dc-3 file with any problem() below.

Usage: python scripts/factor_review.py RESULTS.json [RESULTS.json ...]
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from data_utils import DATA_CHECKS_V3, _sig   # noqa: E402
from metrics import compute_metrics   # noqa: E402

PPY = {"annual": 1, "semi-annual": 2, "quarterly": 4, "monthly": 12}


def _rows(res):
    return [r for r in res["period_data"] if r["portfolio_return"] is not None and r["spy_return"] is not None]


def _cagr_excess(res, rows, rets):
    """(CAGR, excess CAGR) in percent, as the topic computes them from its period returns."""
    fr = [r["fraction"] if r.get("partial") else 1.0 for r in rows] if any("partial" in r for r in rows) else None
    m = compute_metrics(rets, [r["spy_return"] for r in rows], PPY[res["frequency"]], fractions=fr)
    return m["portfolio"]["cagr"] * 100, m["comparison"]["excess_cagr"] * 100


def reproduces(res):
    """The leg's CAGR and excess come back from its period returns (to the 2 decimals it publishes)."""
    rows = _rows(res)
    c, x = _cagr_excess(res, rows, [r["portfolio_return"] for r in rows])
    return abs(c - res["portfolio"]["cagr"]) < 0.006 and abs(x - res["comparison"]["excess_cagr"]) < 0.006


def effects(res):
    """[(span, event, effect)]: each band factor_flag event, its holding span (symbol, rebalance date, exit date) and
    the move in the leg's excess (pp) when its return is swapped to the close basis, which moves its period by
    r_close minus r_adj over the names held."""
    rows = _rows(res)
    base = [r["portfolio_return"] for r in rows]
    x0 = _cagr_excess(res, rows, base)[1]
    at = {r["rebalance_date"]: i for i, r in enumerate(rows)}
    out = []
    for e in res["data_checks"]["events"]:
        if e["check"] != "factor_flag" or "band" not in e["reasons"] or e["rebalance_date"] not in at:
            continue
        i = at[e["rebalance_date"]]
        rets = list(base)
        rets[i] += (e["r_close"] - e["r_adj"]) / rows[i]["stocks_held"]
        out.append(((e["symbol"], e["rebalance_date"], rows[i]["exit_date"]), e,
                    _cagr_excess(res, rows, rets)[1] - x0))
    return out


def _keys(entries):
    return {(e["symbol"], e["rebalance_date"], e["exit_date"]) for e in entries}


def unreviewed(res, entries, min_pp):
    """The band flags at min_pp or more whose span isn't among `entries`: [(span, effect)]."""
    listed = _keys(entries)
    return [(span, eff) for span, _, eff in effects(res) if abs(eff) >= min_pp and span not in listed]


def problems(res, code=DATA_CHECKS_V3["factor_hand"]):
    """Why a committed dc-3 file can't stand, [] when it can (and for any other version):
    the leg doesn't reproduce; a band flag at review_min_pp or more isn't on the file's own list; a hand_stale
    event; or an entry of the code's list whose span the file holds has no matching hand event (so adding an
    entry forces the leg's rerun)."""
    dcb = res.get("data_checks") or {}
    if dcb.get("version") != "dc-3":
        return []
    if not reproduces(res):
        return ["the leg's CAGR and excess don't reproduce from period_data"]
    fh = dcb["rules"]["factor_hand"]
    out = [f"unreviewed band flag {s} {r} (excess {eff:+.3f}pp)"
           for (s, r, _), eff in unreviewed(res, fh["entries"], fh["review_min_pp"])]
    out += [f"hand_stale {e['symbol']} {e['rebalance_date']}" for e in dcb["events"] if e["check"] == "hand_stale"]
    exits = {r["rebalance_date"]: r["exit_date"] for r in res["period_data"]}
    seen = {(e["symbol"], e["rebalance_date"]) for e in dcb["events"]
            if e["check"] in ("factor_flag", "hand", "hand_stale")}
    hands = {(e["symbol"], e["rebalance_date"]): e for e in dcb["events"] if e["check"] == "hand"}
    for c in code["entries"]:
        k = (c["symbol"], c["rebalance_date"])
        if k not in seen or exits.get(c["rebalance_date"]) != c["exit_date"]:
            continue
        h = hands.get(k)
        if h is None or h["kind"] != c["kind"] or (c["kind"] == "rebuilt" and h["booked"] != _sig(c["booked"])):
            out.append(f"hand list entry {c['symbol']} {c['rebalance_date']} not booked as listed: rerun the leg")
    return out


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    fh = DATA_CHECKS_V3["factor_hand"]
    for path in sys.argv[1:]:
        res = json.load(open(path))
        listed = (res["data_checks"]["rules"].get("factor_hand") or {}).get("entries", [])
        print(f"{path}: data checks {res['data_checks']['version']}")
        for (s, r, x), eff in sorted(unreviewed(res, listed, fh["review_min_pp"]), key=lambda t: -abs(t[1])):
            print(f"  review {s} {r} to {x}: excess {eff:+.3f}pp on a close-basis swap")
        for p in problems(res):
            print(f"  PROBLEM {p}")


if __name__ == "__main__":
    main()
