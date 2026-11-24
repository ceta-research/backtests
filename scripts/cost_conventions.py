#!/usr/bin/env python3
"""A leg's cost convention, checked from its --full-holdings results file, and the leg's return under each one.

The conventions (METHODOLOGY.md, "Transaction Costs"). A results file names its own in cost_model.turnover:
  round_trip  every booked name pays its one-way rate twice each period, kept or not (costs.apply_costs).
              A cost_model with no turnover key means this one.
  trades      only real trades are charged, each sized to the trade (costs.trade_costs).
  traded      reserved for entries and exits only. It is not built, and a file that names it is refused.

For each file:
  * The check. Each period_data row's portfolio_return is recomputed from its holdings_full items (return
    before costs, cost_rate) under the file's own convention, and must sit within TOLERANCE of the stored
    value: a row and each of its items are rounded to 6 places. The walk covers every row in order. A cash
    row (no holdings_full), a row that books nothing (an empty holdings_full) and a row with no benchmark
    return all count.
  * The figures. CAGR and excess CAGR under both conventions, over the rows with a benchmark return, as the
    topic computes them. The file's own convention uses its stored rows, the other the recompute rounded to
    6 places.

Only portfolio_return is checked. A --exit-method both file lists its main book, so its _ltp series can't be
recomputed from it, and the script says so.

Exit 0, 1 when a row doesn't reproduce, and 2 when a file is refused, with the reason: a file that can't be
opened or isn't JSON, a top level that isn't one leg (an exchange_comparison.json), no cost_model (a --no-costs
run, or a file older than the key), an unknown turnover, a row or an item that isn't an object, an invested row
without holdings_full (rerun the leg with --full-holdings), a row without holdings_full whose entry_buyable or
min_stocks is not a number, a holdings_full that isn't a list, an item without its symbol or with one that
isn't a string, a symbol listed twice in one row, an item whose return or cost_rate is missing or not a finite
number, a return under -100% or a negative cost_rate (on either convention), returns or costs too large for
the trades walk to add, or a row whose portfolio_return is null or not a finite number.
The figures refuse what only they read: a frequency they aren't built for, a row whose spy_return is missing or
is neither null nor a finite number, a partial row whose fraction is missing or not a finite number, a fraction
or a partial row that metrics.compute_metrics refuses (outside 0 to 1, or not the last measured row), and
values the other convention's walk can't add.
--json writes one record per checked file: file, convention, rows_checked, worst_gap, and each convention's
cagr and excess_cagr. Gate 15 of verify_floor_guard.py runs the check (problem) on every committed data-checks
file whose cost_model carries turnover; it doesn't read what only the figures read. Offline.

Usage: python scripts/cost_conventions.py RESULTS.json [RESULTS.json ...] [--json OUT]
"""
import argparse
import json
import math
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from costs import apply_costs, trade_costs   # noqa: E402
from data_utils import INVESTED, period_state   # noqa: E402
from factor_review import PPY, _cagr_excess, _rows   # noqa: E402

CONVENTIONS = ("round_trip", "trades")
TOLERANCE = 2e-6   # a row and each of its items are rounded to 6 places


class Refusal(Exception):
    """A file the check can't read, with the reason."""


def convention(res):
    """The file's cost convention: cost_model.turnover, and round_trip for a cost_model without the key."""
    if not isinstance(res, dict) or "period_data" not in res:
        raise Refusal("the top level is not one leg (no period_data): pass each leg's own file")
    cm = res.get("cost_model")
    if not isinstance(cm, dict):
        raise Refusal("cost_model is null or missing: a run without costs, or a file older than cost_model, has "
                      "no convention to check")
    how = cm.get("turnover", "round_trip")
    if how not in CONVENTIONS:
        raise Refusal(f"turnover {how!r} is not one of {', '.join(CONVENTIONS)}")
    return how


def _number(x):
    """x is a finite number: not null, a string or NaN."""
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def books(res):
    """Every period row's book, in order: {symbol: (return before costs, cost_rate)} from its holdings_full, {}
    for a row that books nothing, None for a cash row. Raises Refusal for a row or an item it can't read (a row's
    two funnel keys and an item's symbol among them), and for a value no book can hold (a return under -100%, a
    negative cost_rate), on either convention."""
    out = []
    for i, r in enumerate(res["period_data"]):
        if not isinstance(r, dict):
            raise Refusal(f"row {i} is not an object")
        day = r.get("rebalance_date")
        if "holdings_full" not in r:
            # period_state compares the two keys when the row has both and entry_buyable is not null
            if r.get("entry_buyable") is not None and "min_stocks" in r \
                    and not (_number(r["entry_buyable"]) and _number(r["min_stocks"])):
                raise Refusal(f"{day}: entry_buyable or min_stocks is not a number")
            if period_state(r) == INVESTED or r.get("stocks_held"):
                raise Refusal(f"{day}: an invested row without holdings_full (rerun the leg with --full-holdings)")
            out.append(None)
            continue
        if not isinstance(r["holdings_full"], list):
            raise Refusal(f"{day}: holdings_full is not a list")
        book = {}
        for h in r["holdings_full"]:
            if not isinstance(h, dict):
                raise Refusal(f"{day}: an item that is not an object")
            if not h.get("symbol"):
                raise Refusal(f"{day}: an item without symbol")
            if not isinstance(h["symbol"], str):
                raise Refusal(f"{day}: an item whose symbol is not a string")
            for key in ("return", "cost_rate"):
                if not _number(h.get(key)):
                    raise Refusal(f"{day}: {h['symbol']}'s {key} is missing or not a finite number")
            if h["return"] < -1.0:
                raise Refusal(f"{day}: {h['symbol']}'s return is under -100%")
            if h["cost_rate"] < 0:
                raise Refusal(f"{day}: {h['symbol']}'s cost_rate is negative")
            if h["symbol"] in book:
                raise Refusal(f"{day}: {h['symbol']} is listed twice")
            book[h["symbol"]] = (h["return"], h["cost_rate"])
        out.append(book)
    return out


def recompute(res, how):
    """Each row's portfolio_return under one convention, unrounded: the mean of its items' returns less their
    costs, and 0.0 for a cash row or a row that books nothing. Raises Refusal for what books() or the trades walk
    refuses, sums that overflow among them."""
    period_books = books(res)
    if how == "trades":
        try:
            costs = trade_costs(period_books)
        except ValueError as e:   # the walk's own refusal: books() has refused the values it names today
            raise Refusal(str(e)) from None
        except OverflowError:   # math.fsum's: each value is one a book can hold, and their sum is not
            raise Refusal("a row's returns or costs are too large to add") from None
        return [sum(x for x, _ in b.values()) / len(b) - c if b else 0.0 for b, c in zip(period_books, costs)]
    return [sum(apply_costs(x, rate) for x, rate in b.values()) / len(b) if b else 0.0 for b in period_books]


def check(res):
    """The file's own convention on every row: {"convention", "rows_checked", "worst_gap", "bad"}, where bad
    lists (rebalance date, stored, recomputed) for each row not within TOLERANCE of its recompute, one whose
    recompute is not a number among them. Raises Refusal for a file it can't read."""
    own = convention(res)
    new = recompute(res, own)   # reads every row and item first, so a row that isn't an object is refused there
    for r in res["period_data"]:
        if not _number(r.get("portfolio_return")):
            raise Refusal(f"{r.get('rebalance_date')}: portfolio_return is null or not a finite number")
    pairs = list(zip(res["period_data"], new))
    return {"convention": own, "rows_checked": len(res["period_data"]),
            "worst_gap": max((abs(got - r["portfolio_return"]) for r, got in pairs), default=0.0),
            "bad": [(r.get("rebalance_date"), r["portfolio_return"], got) for r, got in pairs
                    if not abs(got - r["portfolio_return"]) <= TOLERANCE]}


def _measured(res):
    """The rows with a benchmark return, after checking what only the figures read of the file: its frequency,
    each row's spy_return and a partial row's fraction. Raises Refusal for one they can't read."""
    freq = res.get("frequency")
    if not isinstance(freq, str) or freq not in PPY:
        raise Refusal(f"frequency {freq!r} is not one of {', '.join(PPY)}")
    for r in res["period_data"]:
        day = r.get("rebalance_date")
        if "spy_return" not in r or not (r["spy_return"] is None or _number(r["spy_return"])):
            raise Refusal(f"{day}: spy_return is missing, or is neither null nor a finite number")
        if r.get("partial") and not _number(r.get("fraction")):
            raise Refusal(f"{day}: a partial row's fraction is missing or not a finite number")
    return _rows(res)


def figures(res):
    """{convention: {"cagr", "excess_cagr"}} in percent, over the rows with a benchmark return: the file's own
    convention from its stored rows, the other from the recompute rounded to 6 places. None where the leg has
    fewer than two full measured periods. Raises Refusal for a file whose figures can't be read."""
    own = convention(res)
    by = {how: recompute(res, how) for how in CONVENTIONS}
    at = {id(r): i for i, r in enumerate(res["period_data"])}
    measured = _measured(res)
    out = {}
    for how in CONVENTIONS:
        rets = [r["portfolio_return"] if how == own else round(by[how][at[id(r)]], 6) for r in measured]
        try:
            c, x = _cagr_excess(res, measured, rets)
        except TypeError:   # compute_metrics gives no figure for fewer than two full periods
            c = x = None
        except ValueError as e:   # compute_metrics' own refusal: a fraction outside 0 to 1, or a partial row not last
            raise Refusal(f"no figure can be computed ({e})") from None
        out[how] = {"cagr": c, "excess_cagr": x}
    return out


def problem(res):
    """Why a committed file that states a convention can't stand, or None (gate 15's results check)."""
    try:
        got = check(res)
    except Refusal as e:
        return f"cost convention: {e}"
    if not got["bad"]:
        return None
    day, stored, new = got["bad"][0]
    return (f"cost convention {got['convention']}: {len(got['bad'])} row(s) don't reproduce from holdings_full "
            f"(first {day}: stored {stored}, recomputed {new:.6f})")


def report(path, res):
    """Print one file's check and figures. Returns the check and the file's --json record."""
    got, fig = check(res), figures(res)
    print(f"{path}: {got['convention']}, {got['rows_checked']} rows checked, worst gap {got['worst_gap']:.1e} "
          f"(tolerance {TOLERANCE:.0e})")
    if any("portfolio_return_ltp" in r for r in res["period_data"]):
        print("  portfolio_return only: holdings_full is the main book, so the _ltp series isn't checked")
    for day, stored, new in got["bad"]:
        print(f"  DOES NOT REPRODUCE {day}: stored {stored}, recomputed {new:.6f}")
    for how in CONVENTIONS:
        c, x = fig[how]["cagr"], fig[how]["excess_cagr"]
        print(f"  {how:<10} " + ("no figure: fewer than two full measured periods" if c is None
                                 else f"CAGR {c:.2f}%, excess {x:+.2f}pp") + ("  (the file's)" if how == got["convention"] else ""))
    if fig["trades"]["cagr"] is not None:
        print(f"  trades less round_trip: CAGR {fig['trades']['cagr'] - fig['round_trip']['cagr']:+.3f}pp")
    return got, {"file": path, "convention": got["convention"], "rows_checked": got["rows_checked"],
                 "worst_gap": got["worst_gap"], **fig}


def load(path):
    """The results file at path. Raises Refusal for one that can't be opened or isn't JSON."""
    try:
        with open(path) as fh:
            return json.load(fh)
    except OSError as e:
        raise Refusal(f"the file can't be opened ({e.strerror or e})") from None
    except ValueError as e:
        raise Refusal(f"the file is not JSON ({e})") from None


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("files", nargs="+", metavar="RESULTS.json")
    ap.add_argument("--json", metavar="OUT", help="write each checked file's record here")
    args = ap.parse_args(argv)
    worst, records = 0, []
    for path in args.files:
        try:
            got, record = report(path, load(path))
        except Refusal as e:
            print(f"{path}: REFUSED: {e}")
            worst = 2
            continue
        records.append(record)
        if got["bad"]:
            worst = max(worst, 1)
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(records, fh, indent=2)
    return worst


if __name__ == "__main__":
    sys.exit(main())
