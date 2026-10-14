#!/usr/bin/env python3
"""Offline check of pairs-multi-pair on the per-year pairs universe (no network).

The made-up market and its stand-in client are scripts/test_pairs_zscore.py's: pairs_universe
builds the candidates from them and the topic's own fetch, pair formation, trade simulation,
books and results run on top, end to end. The code this topic shares with pairs-zscore (the
correlation list, the half-life filter, the trade simulation, the fetch) is checked by that
file's own checks and mutants, applied here to this topic's source. This file adds six legs
and three bars-only pairs to that market.

This topic's own rules, on the same market and on hand-built years:

Formation: the walk down the correlation list (200 rows) stops at 20 pairs with a half-life.
The correlation, the hedge ratio, the half-life and the spread volatility read the formation
year and not a day either side of it; a negative hedge ratio forms no pair. A pair's year is
the sum of its trades; its spread volatility is the annualised deviation of the daily change of
log A - beta * log B over the formation year, on 100 changes or more. On weekday bars the
year's first bar can signal: the z-score window is filled from the months before. A position
that converges on its 60th held day is a convergence, not a time stop.

Entry: T being 1 January, a pair is entered when each leg's first bar in the window from
T + offset to 10 days after that (2 to 12 January at next-bar execution, 1 to 11 January at
same-bar) prints 1.00 or more and is dated on or before the pair's delisting bound, the bound
the trade simulation stops at: a leg's entry bar past the other leg's bound keeps the pair out.
No bar after the window is asked of a pair: one whose leg stops on the window's last day is
entered all the same. A pair that is not entered sits the year out, whatever it does later: it
is in no book and is never simulated.

Books: the N-pair book (5, 10, 15, 20) holds the first N entered pairs by correlation, equal
weight or by 1 / spread vol (a pair without a vol takes an equal share). A book with fewer than
3 entered pairs is cash, however many of them traded; a year with fewer than 3 entered pairs
trades nothing and asks the exit helper nothing, however many pairs formed and whatever prints
after the window. The published book is the 20-pair inverse-vol one. The size x allocation
table needs 3 benchmark-priced years and counts cash over every year.

Results: each year's universe counts, formed and entered pairs, the universe rule, the listing
rule, both dedups and the exit block, next to the keys the charts and posts read, and no key
besides. The standalone table is written beside the results file under that file's own name.
Two lines of one company among the formed pairs stop the run, entered or not. Only
--exit-method ltp runs, on each of the 12 markets, over 2005 to 2024; a leg of two exchanges
sends both to the universe.

Every mutant of the topic's source must trip a check. pairs-zscore's mutants apply when their
anchor is in this source; SHARED_MIN keeps their number from falling. Loading the source cannot
start a run: its entry point is cut off before it executes, a def that would call something
when it is defined is refused, the client is a stand-in that raises, and the process cannot
open a connection.

Usage:
    python scripts/test_pairs_multi_pair.py [repo root]
"""
import ast
import contextlib
import fnmatch
import io
import json
import math
import os
import socket
import sys
import tempfile
import types
from datetime import date, timedelta

import duckdb


def no_network(*args, **kwargs):
    raise RuntimeError("test_pairs_multi_pair: a network call was blocked")


def no_client(*args, **kwargs):
    raise RuntimeError("test_pairs_multi_pair: the topic built a warehouse client")


# Nothing below may reach the warehouse: no key, no name lookup, no connection.
for _key in ("CR_API_KEY", "TS_API_KEY"):
    os.environ.pop(_key, None)
socket.getaddrinfo = socket.create_connection = no_network
socket.socket.connect = socket.socket.connect_ex = no_network

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "pairs-multi-pair", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import pairs_universe  # noqa: E402
import test_pairs_zscore as Z  # noqa: E402  the made-up market and the shared code's checks
from data_utils import get_local_benchmark  # noqa: E402
from metrics import compute_metrics  # noqa: E402

YEARS3 = (2019, 2020, 2021)   # 2021: the leg's lines have no 2020 USD rate, so the year has no candidate
SIZES = (5, 10, 15, 20)
RFR = 0.03                    # not the 0.02 default: the leg's own rate must reach every metric
D = date

# --- lines this file adds to the made-up market ------------------------------------------------------
# Pair k of a leg forms on 2019 at spread amplitude 0.010 + 0.002 k, so k is its correlation rank.
# The entry window of 2020: 2 to 12 January at next-bar execution, 1 to 11 January at same-bar.
MARCH = Z.ev(D(2020, 3, 10), D(2020, 3, 12), Z.HI)        # a round trip in March
APRIL = Z.ev(D(2020, 4, 10), D(2020, 4, 12), Z.HI)        # one in April
JUNE = Z.ev(D(2020, 6, 10), D(2020, 6, 15), Z.HI)         # open when the A leg stops on 15 June
SEPTEMBER = Z.ev(D(2020, 9, 10), D(2020, 9, 12), Z.HI)    # a round trip in September
JAN2, JAN3, JAN5, JAN6, JAN11, JAN12, JAN13 = (D(2020, 1, day) for day in (2, 3, 5, 6, 11, 12, 13))
ADDED = {   # leg: [(pair, its 2020 bars, the leg that prints 0.99 on 2 January only)]
    # the floor on the window's first bar: four formed; two entered at next-bar execution, four at same-bar
    "MPFLOOR": [("MFL1", dict(events=MARCH), None),
                ("MFL2", dict(events=JUNE, a_last=D(2020, 6, 15)), None),
                ("MFL3", dict(a_scale=0.01, a_flat=1.0), "A"),
                ("MFL4", dict(b_scale=0.01, b_flat=1.0), "B")],
    # the window's last day: a three-pair year, invested at next-bar execution
    "MPWIN": [("MIN1", dict(events=MARCH), None),
              ("MIN2", dict(a_gap=(Z.T20, JAN11)), None),       # A's first bar of 2020 is 12 January
              ("MIN3", dict(b_gap=(Z.T20, JAN11)), None)],      # B's is
    # the day after it: two entered pairs and one more a day late on each side, a cash year
    "MPOUT": [("MOUT1", dict(events=MARCH), None),
              ("MOUT2", {}, None),
              ("MOUT3", dict(a_gap=(Z.T20, JAN12), events=APRIL), None),   # A's first bar is 13 January
              ("MOUT4", dict(b_gap=(Z.T20, JAN12), events=APRIL), None)],  # B's is
    # the delisting bound, and two pairs that sit an invested year out with a signal later in it
    "MPBOUND": [("MBD1", dict(events=MARCH), None),
                ("MBD2", dict(a_gap=(Z.T20, JAN5)), None),      # A's first bar is 6 January
                ("MBD3", dict(b_gap=(Z.T20, JAN5)), None),      # B's is
                ("MBD4", dict(b_scale=0.01, b_flat=1.0, events=APRIL), "B"),         # under the floor
                ("MBD5", dict(a_gap=(Z.T20, JAN12), events=SEPTEMBER), None)],       # a day late
    # the formation windows: a bar the day before the formation year and one the day after it, both far off the spread
    "MPEDGE": [("MEDG", {}, None)],
    # a leg whose last bar is the window's last day: entered all the same, a three-pair year stays invested
    "MPSTOP": [("MST1", dict(events=MARCH), None),
               ("MST2", {}, None),
               ("MST3", dict(a_last=JAN12), None)],             # A prints from 1 to 12 January and no further
}
EDGE_DAYS = (Z.Y18_END, Z.T20)
WEEKDAYS = [d for d in Z.span(D(2019, 10, 1), D(2020, 3, 31)) if d.weekday() < 5]
_market, _marks = set(Z.PX), [len(rows) for rows in (Z.PROFILE, Z.METRICS, Z.EOD)]
_exchanges = {row[1] for row in Z.PROFILE}
for _leg, _pairs in ADDED.items():
    for _k, (_name, _kw, _dip) in enumerate(_pairs):
        _legs = dict(zip("AB", Z.formed(2019, 0.010 + 0.002 * _k, **_kw)))
        if _dip:
            _legs[_dip][JAN2] = 0.99
        if _leg == "MPEDGE":
            for _d in EDGE_DAYS:
                _legs["A"][_d], _legs["B"][_d] = 1.2 * Z.wave(_d), Z.wave(_d)
        for _side, _px in _legs.items():
            Z.line(_name + _side, _leg, _name, _px)
_wb = {d: Z.drift(d) for d in WEEKDAYS}   # a signal on the first bar of 2020, a Wednesday
Z.bars("MWKA", {d: p * math.exp(Z.HI if d == Z.T20 else Z.noise(d)) for d, p in _wb.items()})
Z.bars("MWKB", _wb)
MAR1, MAY1 = D(2020, 3, 1), D(2020, 5, 1)   # a signal on 1 March: 1 May is the 60th held day
STOP_EVENTS = {d: Z.HI + 0.001 * (d - MAR1).days for d in Z.span(MAR1, MAY1 - Z.ONE)}   # no convergence, no 5% loss
STOP_EVENTS[MAY1] = STOP_EVENTS[MAY1 + Z.ONE] = Z.spread_at(0.0, MAY1, STOP_EVENTS)      # converged on the 60th held day
_tb = {d: Z.drift(d) for d in Z.span(D(2019, 11, 1), MAY1 + Z.ONE)}
Z.bars("MTSA", {d: p * math.exp(STOP_EVENTS.get(d, Z.noise(d))) for d, p in _tb.items()})
Z.bars("MTSB", _tb)
_year = Z.span(Z.T19, Z.Y19_END)   # log A falls as log B rises: a hedge ratio near -1 on a spread that reverts
Z.bars("MNGA", {d: 1e4 / Z.wave(d) * math.exp(0.02 * math.sin(2 * math.pi * i / 20)) for i, d in enumerate(_year)})
Z.bars("MNGB", {d: Z.wave(d) for d in _year})
BARS_ONLY = ("MWKA", "MWKB", "MTSA", "MTSB", "MNGA", "MNGB")
CLASH = sorted((_market & (set(BARS_ONLY) | {n + s for ps in ADDED.values() for n, _, _ in ps for s in "AB"}))
               | (_exchanges & set(ADDED)))
for _table, _rows, _mark in zip(("profile", "key_metrics", "stock_eod"), (Z.PROFILE, Z.METRICS, Z.EOD), _marks):
    for _i in range(_mark, len(_rows), 2000):   # the stand-in warehouse was loaded before these lines existed
        _part = _rows[_i:_i + 2000]
        Z.WH.con.execute(f"INSERT INTO {_table} VALUES " + ", ".join(["(" + ", ".join("?" * len(_part[0])) + ")"] * len(_part)),
                         [v for r in _part for v in r])


PATH_CALLS = {"sys.path.insert", "os.path.dirname", "os.path.abspath"}


def load(src):
    """The topic as a module, without its entry point: only its imports, constants and definitions run.

    The closing `if __name__ ...: main()` is cut off however it is worded, any other top-level
    statement is refused, so is a def that would call something when it is defined (a decorator,
    a default, an annotation), and the client the topic imported is replaced by one that raises.
    """
    tree = ast.parse(src, PATH)
    body = []
    for node in tree.body:
        if isinstance(node, ast.If):
            continue
        if isinstance(node, ast.FunctionDef):
            header = [node.args] + ([node.returns] if node.returns else [])
            plain = not node.decorator_list and not any(isinstance(n, ast.Call) for part in header for n in ast.walk(part))
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            plain = True
        else:   # a constant, the docstring, or the repo root put on sys.path
            calls = {ast.unparse(n.func) for n in ast.walk(node) if isinstance(n, ast.Call)}
            plain = ((isinstance(node, ast.Assign) and not calls)
                     or (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant))
                     or (isinstance(node, ast.Expr) and "sys.path.insert" in calls and calls <= PATH_CALLS))
        if not plain:
            raise RuntimeError(f"line {node.lineno} of the topic could start a run: {ast.unparse(node)[:60]}")
        body.append(node)
    tree.body = body
    mod = types.ModuleType("pairs_multi_pair_under_test")
    mod.__file__ = PATH
    exec(compile(tree, PATH, "exec"), mod.__dict__)
    mod.CetaResearch = no_client
    mod.print = lambda *a, **k: None
    mod.PUBLISHED_YEARS = (mod.START_YEAR, mod.END_YEAR)
    mod.START_YEAR, mod.END_YEAR = Z.YEARS
    return mod


def guard_checks():
    """Loading the topic cannot run main() or reach the network, whatever its last lines say."""
    tripwire = SRC.replace("def main():\n", "def main():\n    raise SystemExit('main() ran while the topic was loaded')\n", 1)
    tail = 'if __name__ == "__main__":'
    if tripwire == SRC or SRC.count(tail) != 1:
        yield "fixture: the topic has no main() or no entry-point test to guard"
    for name, variant, refused in (("as written", tripwire, False),
                                   ("with its entry-point test inverted", tripwire.replace(tail, 'if __name__ != "__main__":'), False),
                                   ("with its entry-point test always true", tripwire.replace(tail, "if True:"), False),
                                   ("with a bare main() call at the end", tripwire + "\nmain()\n", True),
                                   ("with main() called in an assignment", tripwire + "\nDONE = main()\n", True),
                                   ("with main() called by a decorator", tripwire + "\n@(lambda f: main())\ndef late():\n    pass\n", True),
                                   ("with main() called in a def's default", tripwire + "\ndef late(x=main()):\n    pass\n", True),
                                   ("with main() called in a def's annotation", tripwire + "\ndef late(x: main() = 1):\n    pass\n", True),
                                   ("with main() called in a def's return annotation", tripwire + "\ndef late() -> main():\n    pass\n", True)):
        try:
            mod = load(variant)
            if refused:
                yield f"the topic {name} was loaded"
            try:
                mod.CetaResearch(api_key="x")
                yield f"the topic {name} can build a warehouse client"
            except RuntimeError:
                pass
        except RuntimeError as e:
            if not refused:
                yield f"the topic {name} was refused: {e}"
        except SystemExit as e:
            yield f"the topic {name}: {e}"
    def connect():
        with socket.socket() as s:
            s.connect(("127.0.0.1", 9))

    for name, call in (("name lookup", lambda: socket.getaddrinfo("localhost", 80)),
                       ("connection", lambda: socket.create_connection(("127.0.0.1", 9), timeout=0.2)),
                       ("socket connect", connect)):
        try:
            call()
            yield f"a {name} went through"
        except RuntimeError:
            pass
        except OSError as e:
            yield f"a {name} reached the network stack: {e}"
    if os.environ.get("CR_API_KEY") or os.environ.get("TS_API_KEY"):
        yield "a warehouse key is set inside the test"


@contextlib.contextmanager
def quiet():
    """cli_utils prints the run's header; period accounting warns on an unpriced year."""
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        yield


def close(x, y):
    return x is not None and y is not None and abs(x - y) <= 2e-6 + 1e-9 * abs(y)


def same_record(got, wanted):
    """A record matches: the same keys, floats to 2e-6, everything else exactly."""
    return (isinstance(got, dict) and sorted(got) == sorted(wanted)
            and all(close(got[k], v) if isinstance(v, float) else got[k] == v for k, v in wanted.items()))


def leg3():
    """(con, per-year counts) of the main leg over three trading years, built by the module."""
    con = duckdb.connect()
    pairs_universe.fetch_universe(Z.WH, con, Z.MAIN, YEARS3)
    counts = pairs_universe.build_candidates(con, YEARS3, 1, 1.0, 30, 200)
    pairs_universe.fetch_prices(Z.WH, con, None, [Z.BENCH])
    return con, counts


# --- this file's own arithmetic ---------------------------------------------------------------------

def common_logs(pa, pb, first, last):
    days = [d for d in Z.span(first, last) if d in pa and d in pb]
    return [math.log(pa[d]) for d in days], [math.log(pb[d]) for d in days]


def ols_beta(pa, pb, first, last):
    la, lb = common_logs(pa, pb, first, last)
    return Z.cov(la, lb) / Z.cov(lb, lb)


def spread_vol(pa, pb, first, last, beta=None):
    """Annualised deviation of the daily change of log A - beta * log B; beta: the window's own OLS slope."""
    beta = ols_beta(pa, pb, first, last) if beta is None else beta
    la, lb = common_logs(pa, pb, first, last)
    s = [x - beta * y for x, y in zip(la, lb)]
    ds = [y - x for x, y in zip(s, s[1:])]
    return math.sqrt(Z.cov(ds, ds) / (len(ds) - 1)) * math.sqrt(252)


def book(pairs, n, allocation):
    """(return, pairs that traded, cash) of the n-pair book. pairs: [(year's return, trades, spread vol)] by correlation."""
    held = pairs[:n]
    traded = sum(1 for _, trades, _ in held if trades)
    if len(held) < 3:
        return 0.0, traded, True
    inv = [1.0 / v if v else None for _, _, v in held]
    known = [x for x in inv if x is not None]
    if allocation == "equal" or not known:
        weights = [1.0 / len(held)] * len(held)
    else:   # a pair without a vol takes an equal share; the others split the rest by 1 / vol
        rest = len(known) / len(held)
        weights = [1.0 / len(held) if x is None else x / sum(known) * rest for x in inv]
    return round(sum(w * r for w, (r, _, _) in zip(weights, held)), 6), traded, False


def book_records(years, n, allocation):
    """The n-pair book's yearly records as the topic writes them. years: [(year, benchmark return, pairs)]."""
    out = []
    for year, spy, pairs in years:
        ret, traded, cash = book(pairs, n, allocation)
        out.append({"year": year, "portfolio_return": ret, "spy_return": spy, "n_active": traded, "is_cash": cash})
    return out


def table(years, rfr):
    """The size x allocation table: one row per book with 3 or more benchmark-priced years."""
    rows = []
    for n in SIZES:
        for allocation in ("equal", "inverse_vol"):
            recs = book_records(years, n, allocation)
            priced = [r for r in recs if r["spy_return"] is not None]
            if len(priced) < 3:
                continue
            m = compute_metrics([r["portfolio_return"] for r in priced], [r["spy_return"] for r in priced], 1,
                                risk_free_rate=rfr)
            invested = [r for r in recs if not r["is_cash"]]
            sharpe = m["portfolio"]["sharpe_ratio"]
            rows.append({"n_pairs": n, "allocation": allocation,
                         "cagr_pct": round(m["portfolio"]["cagr"] * 100, 2),
                         "excess_cagr_pct": round(m["comparison"]["excess_cagr"] * 100, 2),
                         "sharpe": round(sharpe, 3) if sharpe is not None else None,
                         "max_drawdown_pct": round(m["portfolio"]["max_drawdown"] * 100, 2),
                         "cash_periods": sum(1 for r in recs if r["is_cash"]),
                         "avg_active_pairs": round(sum(r["n_active"] for r in invested) / len(invested), 1)
                                             if invested else 0})
    return rows


# --- hand-built years: the books -------------------------------------------------------------------

def made(n, no_vol=(), idle=()):
    """n entered pairs by correlation: (year's return, trades, spread vol). Pair i returns 3% less 0.4% x i."""
    return [(0.0 if i in idle else round(0.03 - 0.004 * i, 6), 0 if i in idle else 1 + i % 2,
             None if i in no_vol else 0.10 + 0.02 * i) for i in range(n)]


BOOK_YEARS = [
    (2001, 0.10, made(21, no_vol={6}, idle={2, 11})),   # 21 entered: four different books, the 21st pair in none
    (2002, -0.05, made(3, idle={1, 2})),                # three entered, one traded: invested
    (2003, 0.07, made(2)),                              # two entered, both traded: cash
    (2004, None, made(4, no_vol={0, 1, 2, 3})),         # no benchmark that year; no pair has a vol: equal shares
    (2005, 0.02, []),                                   # nothing formed: cash
    (2006, 0.04, made(12, no_vol={0})),                 # 12 entered: the 15- and 20-pair books hold the same 12
    (2007, None, made(1)),                              # no benchmark, and cash
    (2008, 0.01, made(2, idle={0, 1})),                 # cash
]
BY_YEAR = {y[0]: y for y in BOOK_YEARS}
TABLES = {"five priced years": [BY_YEAR[y] for y in (2001, 2002, 2003, 2004, 2005, 2006, 2007)],
          "three priced years": [BY_YEAR[y] for y in (2001, 2002, 2004, 2003)],
          "two priced years": [BY_YEAR[y] for y in (2001, 2002, 2004, 2007)],
          "three priced years, all cash": [BY_YEAR[y] for y in (2003, 2005, 2008)]}


def as_year(year, spy, pairs):
    """A year as run_backtest_multi records it."""
    return {"year": year, "spy_return": spy,
            "pairs": [{"sym_a": f"A{i}", "sym_b": f"B{i}", "sector": "S", "correlation": 0.9, "spread_vol_ann": v,
                       "annual_return": r, "n_trades": t, "convergence_rate": None}
                      for i, (r, t, v) in enumerate(pairs)],
            "formation": {"pairs_formed": len(pairs) + 1, "pairs_entered": len(pairs), "same_company_pairs": 0,
                          "pairs": [{"a": f"A{i}", "b": f"B{i}", "sector": "S", "corr": 0.9} for i in range(len(pairs) + 1)]}}


def as_pairs(yd):
    return [(p["annual_return"], p["n_trades"], p["spread_vol_ann"]) for p in yd["pairs"]]


def book_checks(mod):
    """compute_portfolio_for_size and build_diversification_analysis on the hand-built years."""
    data = [as_year(*y) for y in BOOK_YEARS]
    for n in (4, 5, 6, 9, 10, 11, 14, 15, 16, 19, 20, 21):
        for allocation in ("equal", "inverse_vol"):
            got, wanted = mod.compute_portfolio_for_size(data, n, allocation), book_records(BOOK_YEARS, n, allocation)
            if got != wanted:
                bad = [g["year"] for g, w in zip(got, wanted) if g != w] or "the list of years"
                yield f"{n}-pair {allocation} book differs in {bad}: {[g for g, w in zip(got, wanted) if g != w][:1]}"
    for name, years in TABLES.items():
        got, wanted = mod.build_diversification_analysis([as_year(*y) for y in years], risk_free_rate=RFR), table(years, RFR)
        if got != wanted:
            yield f"size x allocation table, {name}: {got[:2]} != {wanted[:2]} ({len(got)} rows, {len(wanted)} wanted)"


def path_checks(mod):
    """The source's trading years; the standalone table sits beside the results file, under that file's own name."""
    if mod.PUBLISHED_YEARS != (2005, 2024):
        yield f"the source's trading years are {mod.PUBLISHED_YEARS}, not 2005 to 2024"
    j = os.path.join
    for given, wanted in ((j("results", "returns_NSE.json"), j("results", "diversification_returns_NSE.json")),
                          (j("gate", "pairs-multi-pair", "us.ltp.json"), j("gate", "pairs-multi-pair", "diversification_us.ltp.json")),
                          (j(os.sep, "tmp", "x", "india.ltp.rep.json"), j(os.sep, "tmp", "x", "diversification_india.ltp.rep.json")),
                          ("out.json", "diversification_out.json")):
        got = mod.diversification_path(given)
        if got != wanted:
            yield f"standalone table of {given}: {got} != {wanted}"
        if fnmatch.fnmatch(os.path.basename(got), "returns_*.json"):   # scripts/rebuild_comparison.py folds those in
            yield f"standalone table of {given} is named like a market's results file: {got}"


# --- the made-up market: what it must produce --------------------------------------------------------

def bench(year):
    px = Z.PX[Z.BENCH]
    days = sorted(d for d in px if d.year == year)
    return px[days[-1]] / px[days[0]] - 1.0


def trades_wanted():
    """{(year, A leg): its trades}, each worked out from the fixture's own bars."""
    w = Z.want
    return {
        (2019, "Q1A"): [w("Q1A", "Q1B", D(2019, 3, 11), D(2019, 3, 14), "convergence", 2, -1, 0.002)],
        (2019, "Q2A"): [w("Q2A", "Q2B", D(2019, 4, 11), D(2019, 4, 14), "convergence", 2, -1, 0.002)],
        (2020, "P00A"): [w("P00A", "P00B", D(2020, 3, 11), D(2020, 3, 14), "convergence", 2, -1, 0.002)],
        (2020, "P01A"): [w("P01A", "P01B", D(2020, 4, 11), D(2020, 4, 14), "convergence", 2, -1, 0.008)],   # 0.3% + no cap
        (2020, "P02A"): [w("P02A", "P02B", D(2020, 6, 11), D(2020, 6, 17), "last_bar", 6, -1, 0.002)],
        (2020, "P03A"): [w("P03A", "P03B", D(2020, 12, 27), D(2020, 12, 31), "year_end", 4, -1, 0.002)],
        (2020, "P09A"): [w("P09A", "P09B", D(2020, 12, 25), D(2020, 12, 31), "year_end", 6, -1, 0.002)],
        (2020, "P11A"): [w("P11A", "P11B", D(2020, 3, 21), D(2020, 3, 24), "convergence", 2, -1, 0.002)],
    }


FORMED = {2019: Z.PAIRS_2019, 2020: Z.PAIRS_2020[:20], 2021: []}
NOT_ENTERED = ("P04A", "P05A", "P07A", "P08A")   # a leg under 1.00 or without a bar in the year
ENTERED = {y: [p for p in pairs if p[0] not in NOT_ENTERED] for y, pairs in FORMED.items()}


def year_wanted(year):
    """A year's record on the main leg: the entered pairs' results and the formation block."""
    first, last, trades = D(year - 1, 1, 1), D(year - 1, 12, 31), trades_wanted()
    corr = lambda a, b, sector: round(Z.corr_row(a, b, sector, first, last)[3], 4)
    pairs = []
    for a, b, sector in ENTERED[year]:
        mine = trades.get((year, a), [])
        conv = sum(1 for t in mine if t[2] == "convergence")
        pairs.append({"sym_a": a, "sym_b": b, "sector": sector, "correlation": corr(a, b, sector),
                      "spread_vol_ann": spread_vol(Z.PX[a], Z.PX[b], first, last),
                      "annual_return": sum(t[4] for t in mine), "n_trades": len(mine),
                      "convergence_rate": round(conv * 100.0 / len(mine), 1) if mine else None})
    formed = [{"a": a, "b": b, "sector": sector, "corr": corr(a, b, sector)} for a, b, sector in FORMED[year]]
    return {"year": year, "spy_return": bench(year), "pairs": pairs,
            "formation": {"pairs_formed": len(formed), "pairs_entered": len(pairs), "same_company_pairs": 0,
                          "pairs": formed}}


def year_problems(got, wanted):
    """Differences between a year's record and the wanted one."""
    if not isinstance(got, dict) or sorted(got) != sorted(wanted) or got["year"] != wanted["year"]:
        return [f"{wanted['year']}: record keys {sorted(got) if isinstance(got, dict) else got}"]
    year, out = wanted["year"], []
    if not close(got["spy_return"], wanted["spy_return"]):
        out.append(f"{year} benchmark return: {got['spy_return']} != {wanted['spy_return']}")
    for key, g, w in (("entered pairs", got["pairs"], wanted["pairs"]),
                      ("formed pairs", got["formation"].get("pairs"), wanted["formation"]["pairs"])):
        if not isinstance(g, list) or [x.get("sym_a", x.get("a")) for x in g] != [x.get("sym_a", x.get("a")) for x in w]:
            out.append(f"{year} {key}: {[x.get('sym_a', x.get('a')) for x in g or []]}")
        else:
            out += [f"{year} {key}, {x.get('sym_a', x.get('a'))}: {x} != {y}" for x, y in zip(g, w) if not same_record(x, y)]
    counts = {k: v for k, v in wanted["formation"].items() if k != "pairs"}
    if {k: v for k, v in got["formation"].items() if k != "pairs"} != counts:
        out.append(f"{year} formation counts: {got['formation']} != {counts}")
    return out


STUB_KINDS = ["convergence"] * 4 + ["time_stop"] * 3 + ["loss_stop"] * 3 + ["year_end"] * 2 + ["last_bar"]
STUB = {(2019, "Q1A"): [("convergence", 3, 0.011), ("time_stop", 60, -0.004)],
        (2020, "P00A"): [(kind, 2 + 5 * i, round(0.012 - 0.003 * i, 6)) for i, kind in enumerate(STUB_KINDS)]}


def stub_checks(mod, con):
    """run_backtest_multi around made-up trades: what it asks of the simulation and makes of the answer."""
    asked, marker, real = [], pairs_universe.PairLtp(con, {}), mod.simulate_pair_trades   # the entry bound asks it too

    def simulate(con_, a, b, beta, start, end, cap_a, cap_b, **kw):
        asked.append(((a, b, start, end, cap_a, cap_b), beta, kw))
        return [{"entry_date": "x", "exit_date": "y", "exit_type": kind, "holding_days": held, "pair_return": ret}
                for kind, held, ret in STUB.get((start.year, a), [])]

    mod.simulate_pair_trades = simulate
    try:
        records, stats = mod.run_backtest_multi(con, use_costs=False, verbose=True, benchmark_symbol=Z.BENCH,
                                                offset_days=0, ltp=marker)
    finally:
        mod.simulate_pair_trades = real
    caps = {y: dict(con.execute("SELECT symbol, cap_usd FROM candidates WHERE yr = ?", [y]).fetchall()) for y in YEARS3}
    if not (caps[2020].get("P01B", 0) is None and close(caps[2020]["P01A"], 1.9e9 / 0.9)
            and close(caps[2019]["Q1A"], 8.5e9 / 0.8) and close(caps[2020]["Q1A"], 1.0e9 / 0.9)):
        yield f"fixture: the cost caps of P01 and Q1: {caps[2020].get('P01A')} {caps[2020].get('P01B')} {caps[2019].get('Q1A')}"
    wanted = [((a, b, D(y, 1, 1), D(y, 12, 31), caps[y][a], caps[y][b]),
               ols_beta(Z.PX[a], Z.PX[b], D(y - 1, 1, 1), D(y - 1, 12, 31)))
              for y in YEARS3 for a, b, _ in ENTERED[y]]
    if [x[0] for x in asked] != [w[0] for w in wanted]:
        yield f"simulation asked for {[x[0] for x in asked][:3]}..., wanted {[w[0] for w in wanted][:3]}..."
    elif any(not close(x[1], w[1]) for x, w in zip(asked, wanted)):
        yield f"simulation asked on hedge ratios {[round(x[1], 6) for x in asked]}"
    if any(x[2] != {"use_costs": False, "offset_days": 0, "ltp": marker} for x in asked):
        yield f"simulation asked with {asked[0][2] if asked else None}"
    by_leg = {(r["year"], p["sym_a"]): p for r in records for p in r["pairs"]}
    for key, trades in STUB.items():
        conv = sum(1 for kind, _, _ in trades if kind == "convergence")
        wanted_pair = (round(sum(ret for _, _, ret in trades), 6), len(trades), round(conv * 100.0 / len(trades), 1))
        got = by_leg.get(key, {})
        if (got.get("annual_return"), got.get("n_trades"), got.get("convergence_rate")) != wanted_pair:
            yield f"{key} with {len(trades)} made-up trades: {got} != {wanted_pair}"
    idle = [p for key, p in by_leg.items() if key not in STUB]
    if any((p["annual_return"], p["n_trades"], p["convergence_rate"]) != (0.0, 0, None) for p in idle):
        yield "a pair without a trade does not record 0.0, 0, None"
    trades = [t for ts in STUB.values() for t in ts]
    share = lambda kind: round(sum(1 for t in trades if t[0] == kind) * 100.0 / len(trades), 1)
    wanted_stats = {"total_trades": 15, "convergence_rate": share("convergence"), "time_stop_rate": share("time_stop"),
                    "loss_stop_rate": share("loss_stop"), "year_end_rate": share("year_end"),
                    "last_bar_rate": share("last_bar"),
                    "avg_holding_days": round(sum(t[1] for t in trades) / len(trades), 1),
                    "avg_trade_return_pct": round(sum(t[2] for t in trades) / len(trades) * 100, 3)}
    if len({share(k) for k in set(STUB_KINDS)}) != 5:
        yield "fixture: two exit types share a rate"
    if stats != wanted_stats:
        yield f"trade_stats on made-up trades: {stats} != {wanted_stats}"


def year_checks(mod):
    """run_backtest_multi: the main leg's three years, made-up trades, a cash leg, the same-company guard."""
    con = Z.cached("main3", leg3)[0]
    mod.END_YEAR = YEARS3[-1]
    try:
        ltp = pairs_universe.PairLtp(con, pairs_universe.load_delist_bounds(Z.WH))
        records, stats = mod.run_backtest_multi(con, benchmark_symbol=Z.BENCH, ltp=ltp)
        if [r.get("year") for r in records] != list(YEARS3):
            yield f"years run: {[r.get('year') for r in records]}"
            return
        for got, year in zip(records, YEARS3):
            for problem in year_problems(got, year_wanted(year)):
                yield problem
        trades = [t for ts in trades_wanted().values() for t in ts]
        share = lambda kind: round(sum(1 for t in trades if t[2] == kind) * 100.0 / len(trades), 1)
        wanted = {"total_trades": len(trades), "convergence_rate": share("convergence"), "time_stop_rate": 0.0,
                  "loss_stop_rate": 0.0, "year_end_rate": share("year_end"), "last_bar_rate": share("last_bar"),
                  "avg_holding_days": round(sum(t[3] for t in trades) / len(trades), 1),
                  "avg_trade_return_pct": sum(t[4] for t in trades) / len(trades) * 100}
        if sorted(stats) != sorted(wanted) or any(not Z.near(stats[k], v, 6e-4) for k, v in wanted.items()):
            yield f"trade_stats: {stats} != {wanted}"
        if ltp.fills != [Z.CLOSE_2020]:
            yield f"early closes recorded: {ltp.fills}"
        for problem in stub_checks(mod, con):
            yield problem
    finally:
        mod.END_YEAR = Z.YEARS[-1]
    # two entered pairs, both with a signal: every book is cash and the year trades nothing
    cash = Z.cached("cash", lambda: Z.leg(["ZCASH"]))[0]
    ltp = pairs_universe.PairLtp(cash, {})
    records, stats = mod.run_backtest_multi(cash, benchmark_symbol="SPY", ltp=ltp)
    got = [(r["year"], r["formation"]["pairs_formed"], r["formation"]["pairs_entered"],
            [(p["sym_a"], p["annual_return"], p["n_trades"], p["convergence_rate"]) for p in r["pairs"]]) for r in records]
    if got != [(2019, 0, 0, []), (2020, 2, 2, [("K1A", 0.0, 0, None), ("K2A", 0.0, 0, None)])] or stats or ltp.fills:
        yield f"two entered pairs are a year that trades nothing: {got}, stats {stats}, closes {ltp.fills}"
    books = [b for n in SIZES for a in ("equal", "inverse_vol") for b in mod.compute_portfolio_for_size(records, n, a)]
    if any((b["portfolio_return"], b["n_active"], b["is_cash"]) != (0.0, 0, True) for b in books):
        yield f"two entered pairs are not cash at every size: {books[:2]}"
    cash.execute("UPDATE candidates SET company = 'K1A' WHERE symbol = 'K1B'")
    try:
        mod.run_backtest_multi(cash, benchmark_symbol="SPY", ltp=pairs_universe.PairLtp(cash, {}))
        yield "two lines of one company among the pairs did not stop the run"
    except AssertionError:
        pass
    finally:
        cash.execute("UPDATE candidates SET company = 'K1B' WHERE symbol = 'K1B'")


def year_rows(records):
    """[(year, formed A legs, pairs formed, pairs entered, [(A leg, return, trades, convergence rate)])]."""
    return [(r["year"], [p["a"] for p in r["formation"]["pairs"]], r["formation"]["pairs_formed"],
             r["formation"]["pairs_entered"],
             [(p["sym_a"], p["annual_return"], p["n_trades"], p["convergence_rate"]) for p in r["pairs"]]) for r in records]


def trip(name, month, offset):
    """The round trip on a pair's events of the 10th to the 12th: in on the 11th and out on the 14th at next-bar execution."""
    return Z.want(name + "A", name + "B", D(2020, month, 10 + offset), D(2020, month, 13 + offset), "convergence",
                  3 - offset, -1, 0.006)   # two 0.3% legs


def open_in_june(name, offset):
    """(the trade, its record) of the position open when the A leg stops on 15 June: a last-bar close."""
    entry = D(2020, 6, 10 + offset)
    trade = Z.want(name + "A", name + "B", entry, D(2020, 6, 15), "last_bar", 5 - offset, -1, 0.006)
    return trade, {"pair": [name + "A", name + "B"], "year": 2020, "entry_date": entry.isoformat(),
                   "last_common_bar": "2020-06-15", "delisted_date": [None, None], "delisting_bound": ["none", "none"]}


DEC29, DEC30 = D(2019, 12, 29), D(2019, 12, 30)   # a leg delisted then: its bars count to 5 and to 6 January
DEC2 = D(2019, 12, 2)                             # delisted then: its bars count to 9 December
GATE_RUNS = (   # (what the run shows, leg, delisting dates, offset, {entered pair: its trade's month, if the year is invested})
    ("0.99 on the window's first bar, on either leg: two entered pairs", "MPFLOOR", {}, 1, {"MFL1": None, "MFL2": None}),
    ("same-bar execution, the window opens on 1 January at 1.00: four entered pairs", "MPFLOOR", {}, 0,
     {"MFL1": 3, "MFL2": 6, "MFL3": None, "MFL4": None}),
    ("a first bar on the window's last day, on either leg: three entered pairs", "MPWIN", {}, 1,
     {"MIN1": 3, "MIN2": None, "MIN3": None}),
    ("the same without an exit helper", "MPWIN", None, 1, {"MIN1": 3, "MIN2": None, "MIN3": None}),
    ("a leg delisted 30 days before the year, its bars repeating into it: two entered pairs", "MPWIN", {"MIN1B": DEC2}, 1,
     {"MIN2": None, "MIN3": None}),
    ("a delisting 31 days before the year is an earlier holder's: three entered pairs", "MPWIN", {"MIN1B": DEC2 - Z.ONE}, 1,
     {"MIN1": 3, "MIN2": None, "MIN3": None}),
    ("same-bar execution, the window closes on 11 January: one entered pair", "MPWIN", {}, 0, {"MIN1": None}),
    ("a first bar the day after the window, on either leg: two entered pairs", "MPOUT", {}, 1, {"MOUT1": None, "MOUT2": None}),
    ("no delisting: three entered pairs, and two that sit the year out with a signal", "MPBOUND", {}, 1,
     {"MBD1": 3, "MBD2": None, "MBD3": None}),
    ("a first bar on the delisting bound, on either leg: three entered pairs", "MPBOUND", {"MBD2A": DEC30, "MBD3B": DEC30}, 1,
     {"MBD1": 3, "MBD2": None, "MBD3": None}),
    ("A's first bar the day after its delisting bound: two entered pairs", "MPBOUND", {"MBD2A": DEC29}, 1,
     {"MBD1": None, "MBD3": None}),
    ("B's first bar the day after its delisting bound: two entered pairs", "MPBOUND", {"MBD3B": DEC29}, 1,
     {"MBD1": None, "MBD2": None}),
    # the bound is the pair's: a leg inside its own bound, its entry bar the day after the other leg's
    ("A's first bar the day after B's delisting bound: two entered pairs", "MPBOUND", {"MBD2B": DEC29}, 1,
     {"MBD1": None, "MBD3": None}),
    ("B's first bar the day after A's delisting bound: two entered pairs", "MPBOUND", {"MBD3A": DEC29}, 1,
     {"MBD1": None, "MBD2": None}),
    ("a leg whose last bar is the window's last day: three entered pairs", "MPSTOP", {}, 1,
     {"MST1": 3, "MST2": None, "MST3": None}),
)


def gate_checks(mod):
    """run_backtest_multi on the added legs: the entry window, the floor, the delisting bound, the cash gate, the company guard."""
    for what, leg, delisted, offset, entered in GATE_RUNS:
        con = Z.cached("multi-pair " + leg, lambda: Z.leg([leg]))[0]
        ltp = None if delisted is None else pairs_universe.PairLtp(con, delisted)
        records, stats = mod.run_backtest_multi(con, benchmark_symbol="SPY", offset_days=offset, ltp=ltp)
        fills = ltp.fills if ltp else []
        formed = [name + "A" for name, _, _ in ADDED[leg]]
        pairs, closes = [], []
        for name, month in entered.items():
            trade, record = (None, None) if month is None else open_in_june(name, offset) if month == 6 else (trip(name, month, offset), None)
            pairs.append((name + "A", trade))
            closes += [record] if record else []
        trades = [t for _, t in pairs if t]
        share = lambda kind: round(sum(1 for t in trades if t[2] == kind) * 100.0 / len(trades), 1)
        wanted = {"total_trades": len(trades), "convergence_rate": share("convergence"), "time_stop_rate": 0.0,
                  "loss_stop_rate": 0.0, "year_end_rate": 0.0, "last_bar_rate": share("last_bar"),
                  "avg_holding_days": round(sum(t[3] for t in trades) / len(trades), 1),
                  "avg_trade_return_pct": sum(t[4] for t in trades) / len(trades) * 100} if trades else {}
        got = year_rows(records)
        last = got[-1] if got else (None, None, None, None, [])
        if (got[:-1] != [(2019, [], 0, 0, [])] or last[:4] != (2020, formed, len(formed), len(entered))
                or [p[0] for p in last[4]] != [a for a, _ in pairs]
                or any(p[1:] != (0.0, 0, None) if t is None else
                       not close(p[1], t[4]) or p[2:] != (1, 100.0 if t[2] == "convergence" else 0.0)
                       for p, (_, t) in zip(last[4], pairs))
                or sorted(stats) != sorted(wanted) or any(not Z.near(stats[k], v, 6e-4) for k, v in wanted.items())
                or fills != closes):
            yield f"{leg}, {what}: {got}, stats {stats}, closes {fills}"
        books = [(a, b) for n in SIZES for a in ("equal", "inverse_vol") for b in mod.compute_portfolio_for_size(records, n, a)
                 if b["year"] == 2020]
        if len(entered) < 3:   # a cash year at every size: nothing was simulated, whatever prints after the window
            if any((b["portfolio_return"], b["n_active"], b["is_cash"]) != (0.0, 0, True) for _, b in books):
                yield f"{leg}, {what}: not cash at every size: {books[:2]}"
        elif any(b["is_cash"] or b["n_active"] != len(trades) for _, b in books) or any(
                not close(b["portfolio_return"], round(sum(t[4] for t in trades) / len(pairs), 6)) for a, b in books if a == "equal"):
            yield f"{leg}, {what}: the books of an invested year: {books[:2]}"
    # two lines of one company in a formed pair that is not entered
    form = Z.cached("multi-pair MPFLOOR", lambda: Z.leg(["MPFLOOR"]))[0]
    form.execute("UPDATE candidates SET company = 'MFL3A' WHERE symbol = 'MFL3B'")
    try:
        mod.run_backtest_multi(form, benchmark_symbol="SPY", ltp=pairs_universe.PairLtp(form, {}))
        yield "two lines of one company in a formed pair that was not entered did not stop the run"
    except AssertionError:
        pass
    finally:
        form.execute("UPDATE candidates SET company = 'MFL3B' WHERE symbol = 'MFL3B'")


def two_exchange_checks(mod):
    """fetch_data_via_api on a leg of two exchanges, 19 candidates and 6: both reach the universe."""
    got = mod.fetch_data_via_api(Z.WH, ["ZG19", "MPWIN"], benchmark_symbol="SPY")
    if got is None or got[1][2020].get("candidates") != 25:
        yield f"a leg of two exchanges: {got and got[1][2020]}"
    if got:
        got[0].close()


def edge_wanted(first=Z.T19, last=Z.Y19_END):
    """(beta, spread mean, spread deviation, half-life, spread vol) of the edge pair over a window, as the topic estimates them."""
    la, lb = common_logs(Z.PX["MEDGA"], Z.PX["MEDGB"], first, last)
    beta = Z.cov(la, lb) / Z.cov(lb, lb)
    s = [x - beta * y for x, y in zip(la, lb)]
    tail = s[1:]   # the half-life query keeps the days that have a day before them
    return (beta, Z.mean(tail), math.sqrt(Z.cov(tail, tail) / (len(tail) - 1)),
            Z.half_life(Z.PX["MEDGA"], Z.PX["MEDGB"], first, last), spread_vol(Z.PX["MEDGA"], Z.PX["MEDGB"], first, last, beta))


def edge_checks(mod):
    """The formation windows on a pair with a bar the day before the formation year and one the day after it."""
    con = Z.cached("multi-pair MPEDGE", lambda: Z.leg(["MPEDGE"]))[0]
    got, wanted = mod.compute_pair_candidates(con, Z.T19, Z.Y19_END), Z.ranked([("MEDGA", "MEDGB", "MEDG")], Z.T19, Z.Y19_END)
    if not Z.rows_match(got, wanted):
        yield f"correlation list, bars either side of the formation year: {got} != {wanted}"
    wanted = edge_wanted()
    got = mod.estimate_spread_params(con, "MEDGA", "MEDGB", Z.T19, Z.Y19_END)
    if got is None or any(not close(g, w) for g, w in zip(got, wanted[:4])):
        yield f"hedge ratio, spread and half-life, bars either side of the formation year: {got} != {wanted[:4]}"
    got = mod.compute_spread_vol_during_formation(con, "MEDGA", "MEDGB", wanted[0], Z.T19, Z.Y19_END)
    if not close(got, wanted[4]):
        yield f"spread vol, bars either side of the formation year: {got} != {wanted[4]}"


def warm_con():
    con = duckdb.connect()
    pairs_universe.fetch_prices(Z.WH, con, BARS_ONLY, [])
    return con


def warm_checks(mod):
    """Bars-only pairs: a signal on the year's first weekday bar, a convergence on the 60th held day, a negative hedge ratio."""
    con = Z.cached("multi-pair bars", warm_con)
    for name, a, b, wanted in (
            ("a signal on the year's first bar, weekday bars", "MWKA", "MWKB",   # the window is filled from the months before
             [Z.want("MWKA", "MWKB", D(2020, 1, 2), D(2020, 1, 6), "convergence", 1, -1, 0.004)]),   # exits on the Monday
            ("converged on the 60th held day", "MTSA", "MTSB",   # a convergence, not a time stop
             [Z.want("MTSA", "MTSB", MAR1 + Z.ONE, MAY1 + Z.ONE, "convergence", 60, -1, 0.004)])):
        out = mod.simulate_pair_trades(con, a, b, 1.0, Z.T20, Z.Y20_END, 2e10, 5e9)
        got = [(t["entry_date"], t["exit_date"], t["exit_type"], t["holding_days"], t["pair_return"]) for t in out or []]
        if not Z.same(got, wanted):
            yield f"{name}: {got} != {wanted}"
    got = mod.estimate_spread_params(con, "MNGA", "MNGB", Z.T19, Z.Y19_END)
    if got is not None:
        yield f"a pair with a negative hedge ratio has spread parameters: {got}"


VOL_PX = {}
for _name, _days in (("V", 101), ("W", 100)):   # 100 daily changes, and 99
    for _i, _d in enumerate(Z.span(Z.T19, Z.T19 + timedelta(days=_days - 1))):
        VOL_PX.setdefault(_name + "A", {})[_d] = Z.wave(_d) * math.exp(0.01 * math.sin(_i))
        VOL_PX.setdefault(_name + "B", {})[_d] = Z.wave(_d)


def vol_con():
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)",
                    [(s, d, Z.ep(d), p) for s, px in VOL_PX.items() for d, p in px.items()])
    return con


def vol_checks(mod):
    """compute_spread_vol_during_formation: the window, the hedge ratio, 100 daily changes."""
    con = Z.cached("main3", leg3)[0]
    qa, qb = Z.PX["Q1A"], Z.PX["Q1B"]
    # Q1 has bars either side of 2019 (B is flat that year); in 2018 B moves, so the hedge ratio counts
    for name, first, last, beta in (("2019, bars either side of the window", Z.T19, Z.Y19_END, 1.0),
                                    ("2018, a hedge ratio of 1.3", Z.ORIGIN, Z.Y18_END, 1.3)):
        got, wanted = mod.compute_spread_vol_during_formation(con, "Q1A", "Q1B", beta, first, last), spread_vol(qa, qb, first, last, beta)
        if not close(got, wanted):
            yield f"spread vol, {name}: {got} != {wanted}"
    small = Z.cached("vol", vol_con)
    span = (Z.T19, Z.T19 + timedelta(days=200))
    got = mod.compute_spread_vol_during_formation(small, "VA", "VB", 1.0, *span)
    if not close(got, spread_vol(VOL_PX["VA"], VOL_PX["VB"], *span, 1.0)):
        yield f"spread vol on 100 daily changes: {got}"
    got = mod.compute_spread_vol_during_formation(small, "WA", "WB", 1.0, *span)
    if got is not None:
        yield f"spread vol on 99 daily changes: {got}"


SERIES = (("cagr", 2), ("max_drawdown", 2), ("annualized_volatility", 2), ("sharpe_ratio", 3), ("sortino_ratio", 3),
          ("calmar_ratio", 3), ("total_return", 2))   # 2: a percentage to 2 places; 3: a ratio to 3
COMPARED = (("excess_cagr", 2), ("win_rate", 2), ("information_ratio", 3), ("up_capture", 2), ("down_capture", 2),
            ("beta", 3), ("alpha", 2))
RESULT_KEYS = (   # every top-level key of a results file: the leg, the period accounting, the book, the universe, the exits
    "universe", "n_periods", "total_rebalances", "cash_periods", "invested_periods", "measured_start", "measured_end",
    "requested_start", "requested_end", "benchmark_symbol", "benchmark_first_date", "window_truncated",
    "unmeasured_periods", "window_label", "n_years", "years", "frequency", "portfolio_config", "benchmark_name",
    "avg_active_pairs", "total_trades", "trade_stats", "portfolio", "spy", "comparison", "excess_cagr", "win_rate_vs_spy",
    "annual_returns", "diversification_analysis", "universe_rule", "listing_filter", "isin_dedup", "company_dedup",
    "exit_method", "ltp")


def published(values, keys):
    """Metrics as a results file carries them: percentages to 2 places, ratios to 3, a missing one None."""
    return {k: None if values.get(k) is None else round(values[k] * 100, 2) if places == 2 else round(values[k], 3)
            for k, places in keys}


def metric_problems(out, m, name):
    """The published series, the comparison and its two top-level copies against compute_metrics' values; no key besides."""
    wanted = {"portfolio": published(m["portfolio"], SERIES), "spy": published(m["benchmark"], SERIES),
              "comparison": published(m["comparison"], COMPARED)}
    wanted["excess_cagr"], wanted["win_rate_vs_spy"] = wanted["comparison"]["excess_cagr"], wanted["comparison"]["win_rate"]
    for key, value in wanted.items():
        if out.get(key) != value:
            yield f"{name}, results {key}: {out.get(key)} != {value}"
    if sorted(out) != sorted(RESULT_KEYS):
        yield f"{name}, results keys missing or new: {sorted(set(out) ^ set(RESULT_KEYS))}"


def primary_checks(mod):
    """run_single on the hand-built years (fetch and simulation stood in): the published book and its record."""
    data = [as_year(*y) for y in BOOK_YEARS]
    counts = {y[0]: {"candidates": 1000 + y[0]} for y in BOOK_YEARS}
    stats = {"total_trades": 77, "convergence_rate": 50.0, "last_bar_rate": 1.0, "avg_holding_days": 9.0,
             "avg_trade_return_pct": 0.1}
    calls, real = [], (mod.fetch_data_via_api, mod.run_backtest_multi)
    blank = duckdb.connect()
    mod.fetch_data_via_api = lambda *a, **k: calls.append(("fetch", a, k)) or (blank, counts)
    mod.run_backtest_multi = lambda *a, **k: calls.append(("run", a, k)) or (data, stats)
    try:
        with quiet():
            out = mod.run_single(Z.WH, ["ZNONE"], "ZNONE", False, RFR, False, None, offset_days=0,
                                 benchmark_symbol="^X", benchmark_name="Bench", exit_method="ltp")
    finally:
        mod.fetch_data_via_api, mod.run_backtest_multi = real
    if out is None or [c[0] for c in calls] != ["fetch", "run"]:
        yield f"run_single on hand-built years: {[c[0] for c in calls]}"
        return
    (_, f_args, f_kw), (_, r_args, r_kw) = calls
    if f_args != (Z.WH, ["ZNONE"]) or f_kw != {"verbose": False, "benchmark_symbol": "^X", "offset_days": 0}:
        yield f"fetch called with {f_args[1:]} {f_kw}"
    helper = r_kw.pop("ltp", None)
    if (r_args != (blank,) or r_kw != {"use_costs": False, "verbose": False, "benchmark_symbol": "^X", "offset_days": 0}
            or not isinstance(helper, pairs_universe.PairLtp) or helper.con is not blank):
        yield f"run_backtest_multi called with {r_kw}, helper {helper}"
    recs = book_records(BOOK_YEARS, 20, "inverse_vol")
    if recs == book_records(BOOK_YEARS, 20, "equal") or recs in (book_records(BOOK_YEARS, 19, "inverse_vol"),
                                                               book_records(BOOK_YEARS, 21, "inverse_vol")):
        yield "fixture: the 20-pair inverse-vol book is not its own"
    priced = [r for r in recs if r["spy_return"] is not None]
    invested = [r for r in recs if not r["is_cash"]]
    m = compute_metrics([r["portfolio_return"] for r in priced], [r["spy_return"] for r in priced], 1, risk_free_rate=RFR)
    rows = [{"year": r["year"], "portfolio": round(r["portfolio_return"] * 100, 2), "spy": round(r["spy_return"] * 100, 2),
             "excess": round((r["portfolio_return"] - r["spy_return"]) * 100, 2), "n_active": r["n_active"],
             "is_cash": r["is_cash"], **counts[r["year"]], **yd["formation"]}
            for r, yd in zip(recs, data) if r["spy_return"] is not None]
    for key, value in (
            ("universe", "ZNONE"), ("annual_returns", rows), ("diversification_analysis", table(BOOK_YEARS, RFR)),
            ("total_rebalances", len(recs)), ("n_periods", len(priced)), ("cash_periods", sum(1 for r in recs if r["is_cash"])),
            ("invested_periods", len(invested)), ("benchmark_symbol", "^X"), ("benchmark_name", "Bench"),
            ("avg_active_pairs", round(sum(r["n_active"] for r in invested) / len(invested), 1)),
            ("total_trades", 77), ("trade_stats", stats), ("portfolio_config", "20-pair inverse_vol"),
            ("listing_filter", None), ("exit_method", "ltp"),
            ("ltp", {"exit_method": "ltp", "source": "last common bar of the pair", "delist_grace_days": 7,
                     "early_closes": 0, "fills": []})):
        if out.get(key) != value:
            got = out.get(key)
            if key == "annual_returns" and isinstance(got, list) and len(got) == len(value):
                got = [(g.get("year"), sorted(k for k in set(g) | set(w) if g.get(k) != w.get(k)))
                       for g, w in zip(got, value) if g != w]
            yield f"hand-built years, results {key}: {got}"
    for problem in metric_problems(out, m, "hand-built years"):
        yield problem


CHART_KEYS = ("universe", "years", "n_years", "total_rebalances", "cash_periods", "portfolio_config", "benchmark_name",
              "benchmark_symbol", "avg_active_pairs", "total_trades", "trade_stats", "portfolio", "spy", "comparison",
              "excess_cagr", "win_rate_vs_spy", "annual_returns", "diversification_analysis")
TRADE_KEYS = ("total_trades", "convergence_rate", "time_stop_rate", "loss_stop_rate", "year_end_rate",
              "avg_holding_days", "avg_trade_return_pct")
SERIES_KEYS = ("cagr", "total_return", "max_drawdown", "sharpe_ratio", "annualized_volatility")


def main_checks(mod):
    """run_single on the XETRA leg over three years: fetch, simulation, books, the results files."""
    leg_con, counts = Z.cached("main3", leg3)
    seen, real = {}, mod.run_backtest_multi

    def spy(con, **kw):
        seen["books"] = {y: Z.book(con, y) for y in YEARS3}
        seen["kw"], seen["out"] = kw, real(con, **kw)
        return seen["out"]

    mod.run_backtest_multi, mod.END_YEAR = spy, YEARS3[-1]
    args = (Z.WH, Z.MAIN, "XETRA", True, RFR, False)
    kw = dict(offset_days=1, benchmark_symbol=Z.BENCH, benchmark_name="DAX")
    try:
        sent = len(Z.WH.sent)
        for method in ("drop", "both"):
            try:
                with quiet():
                    mod.run_single(*args, None, exit_method=method, **kw)
                yield f"run_single ran with exit_method={method}"
            except ValueError:
                pass
        if len(Z.WH.sent) != sent:
            yield "run_single queried the warehouse before refusing an exit method"
        try:
            with quiet():
                mod.run_single(*args, None, **kw)
            yield "run_single ran without an exit method"
        except TypeError:
            pass
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "results", "returns_XETRA.json")
            with quiet():
                out = mod.run_single(*args, path, exit_method="ltp", **kw)
            if out is None or "out" not in seen:
                yield "run_single returned nothing"
                return
            files = sorted(os.listdir(os.path.dirname(path)))
            if files != ["diversification_returns_XETRA.json", "returns_XETRA.json"]:
                yield f"files written: {files}"
            else:
                with open(path) as fh:
                    if json.load(fh) != json.loads(json.dumps(out)):
                        yield "the results file is not the returned results"
                with open(os.path.join(os.path.dirname(path), files[0])) as fh:
                    if json.load(fh) != json.loads(json.dumps(out.get("diversification_analysis"))):
                        yield "the standalone table is not the results' size x allocation table"
    finally:
        mod.run_backtest_multi, mod.END_YEAR = real, Z.YEARS[-1]
    if seen["books"] != {y: Z.book(leg_con, y) for y in YEARS3}:
        yield f"the main leg's candidates are not the module's: {seen['books']}"
    helper = seen["kw"].pop("ltp", None)
    if (seen["kw"] != {"use_costs": True, "verbose": False, "benchmark_symbol": Z.BENCH, "offset_days": 1}
            or not isinstance(helper, pairs_universe.PairLtp)):
        yield f"run_backtest_multi was called with {seen['kw']}, helper {helper}"
    records, stats = seen["out"]
    for got, year in zip(records, YEARS3):
        problems = year_problems(got, year_wanted(year))
        if problems:
            yield f"run_single's own run: {problems[0]}"
    years = [(r["year"], r["spy_return"], as_pairs(r)) for r in records]
    recs = book_records(years, 20, "inverse_vol")
    equal = book_records(years, 20, "equal")
    if [(r["n_active"], r["is_cash"]) for r in recs] != [(2, False), (6, False), (0, True)] or any(
            round(a["portfolio_return"] * 100, 2) == round(b["portfolio_return"] * 100, 2) for a, b in zip(recs[:2], equal[:2])):
        yield f"fixture: the main leg's published book: {recs} against equal weight {equal}"
    m = compute_metrics([r["portfolio_return"] for r in recs], [r["spy_return"] for r in recs], 1, risk_free_rate=RFR)
    rows = [{"year": r["year"], "portfolio": round(r["portfolio_return"] * 100, 2), "spy": round(r["spy_return"] * 100, 2),
             "excess": round((r["portfolio_return"] - r["spy_return"]) * 100, 2), "n_active": r["n_active"],
             "is_cash": r["is_cash"], **counts[r["year"]], **yd["formation"]} for r, yd in zip(recs, records)]

    # the results JSON
    if any(k not in out for k in CHART_KEYS) or any(k not in out["trade_stats"] for k in TRADE_KEYS) or any(
            k not in out[node] for node in ("portfolio", "spy") for k in SERIES_KEYS):
        yield f"a key the charts or posts read is gone: {sorted(out)}"
    rule = {**pairs_universe.UNIVERSE_NOTES, "top_n_per_sector": 30, "min_formation_bars": 200}
    block = {"exit_method": "ltp", "source": "last common bar of the pair", "delist_grace_days": 7,
             "early_closes": 1, "fills": [Z.CLOSE_2020]}
    for key, value in (("universe", "XETRA"), ("universe_rule", rule), ("listing_filter", "country DE"),
                       ("isin_dedup", True), ("company_dedup", True), ("exit_method", "ltp"), ("ltp", block),
                       ("years", "2019-2021"), ("n_years", 3), ("total_rebalances", 3), ("n_periods", 3),
                       ("cash_periods", 1), ("invested_periods", 2), ("avg_active_pairs", 4.0),
                       ("portfolio_config", "20-pair inverse_vol"), ("benchmark_name", "DAX"),
                       ("benchmark_symbol", Z.BENCH), ("total_trades", 8), ("trade_stats", stats),
                       ("diversification_analysis", table(years, RFR)), ("annual_returns", rows)):
        if out.get(key) != value:
            got = out.get(key)
            if key == "annual_returns" and isinstance(got, list) and len(got) == len(value):
                got = [(g.get("year"), sorted(k for k in set(g) | set(w) if g.get(k) != w.get(k)))
                       for g, w in zip(got, value) if g != w]
            yield f"results {key}: {got}"
    for problem in metric_problems(out, m, "the main leg"):
        yield problem
    if (counts[2019]["candidates"], counts[2020]["same_company"], counts[2020]["no_cost_cap"], counts[2021]["candidates"],
            counts[2021]["no_fx"] > 0) != (6, 1, 1, 0, True):
        yield f"fixture: the main leg's universe counts: {counts}"
    if Z.OVERCLAIM in json.dumps(out).lower():
        yield "the results claim survivorship is removed: it is only reduced"
    for problem in Z.WH.problems:
        yield problem


def cli_checks(mod):
    """main(): only ltp runs, and every market of the all-market loop gets its benchmark and the exit method."""
    calls = []

    def run_single(cr, exchanges, name, use_costs, rfr, verbose, output_path=None, **kw):
        calls.append((exchanges, name, use_costs, output_path and os.path.basename(output_path), kw))
        return {"portfolio": {"cagr": 1.0}, "universe": name}

    def passed(exchanges, offset=1):
        symbol, name = get_local_benchmark(exchanges)
        return {"offset_days": offset, "benchmark_symbol": symbol, "benchmark_name": name, "exit_method": "ltp"}

    mod.CetaResearch = lambda *a, **k: calls.append("client") or "client"
    mod.run_single = run_single
    us, argv = ["NYSE", "NASDAQ", "AMEX"], sys.argv
    if len({get_local_benchmark(ex)[0] for _, ex in Z.LEGS}) < 10:
        yield "fixture: the markets do not have their own benchmarks"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "exchange_comparison.json")
            runs = (
                (["--exit-method", "drop"], "refused"), (["--exit-method", "both"], "refused"),
                (["--global", "--exit-method", "both"], "refused"),
                (["--exchange", "NYSE,NASDAQ,AMEX", "--output", target],
                 [(us, "NYSE_NASDAQ_AMEX", True, "exchange_comparison.json", passed(us))]),
                (["--preset", "japan", "--exit-method", "ltp", "--no-next-day", "--no-costs"],
                 [(["JPX"], "JPX", False, None, passed(["JPX"], 0))]),
                (["--global", "--output", target],
                 [(ex, "_".join(ex), True, f"returns_{'_'.join(ex)}.json", passed(ex)) for _, ex in Z.LEGS]),
            )
            for flags, wanted in runs:
                del calls[:]
                sys.argv = ["backtest.py"] + flags
                try:
                    with contextlib.redirect_stderr(io.StringIO()):   # parser.error prints the usage
                        mod.main()
                    got = calls[1:] if calls[:1] == ["client"] else f"no client first: {calls}"
                except SystemExit as e:
                    got = "refused" if e.code not in (0, None) and not calls else f"exit {e.code} after {calls}"
                if got != wanted:
                    yield f"main() with {flags}: {got} != {wanted}"
            with open(target) as fh:
                if list(json.load(fh)) != ["_".join(ex) for _, ex in Z.LEGS]:
                    yield "the all-market file does not hold the 12 markets"
    finally:
        sys.argv = argv


def fixture_problems():
    """The hand-built years are what the checks above assume, by this file's own arithmetic."""
    out = list(Z.fixture_problems())
    b = lambda year, n, allocation: book(BY_YEAR[year][2], n, allocation)
    sizes = [b(2001, n, a)[0] for a in ("equal", "inverse_vol") for n in (4, 5, 6, 9, 10, 11, 14, 15, 16, 19, 20, 21)]
    if len(set(sizes)) != len(sizes):
        out.append(f"two 2001 books return the same: {sizes}")
    if b(2002, 20, "equal") != (round(0.03 / 3, 6), 1, False) or b(2003, 20, "equal") != (0.0, 2, True):
        out.append(f"the 3-pair and 2-pair years: {b(2002, 20, 'equal')} {b(2003, 20, 'equal')}")
    if b(2004, 5, "inverse_vol") != b(2004, 5, "equal") or b(2004, 5, "equal")[0] != round(sum(r for r, _, _ in BY_YEAR[2004][2]) / 4, 6):
        out.append("the year without a spread vol is not an equal-weight book of 4")
    if b(2006, 15, "inverse_vol") != b(2006, 20, "inverse_vol") or b(2006, 10, "inverse_vol") == b(2006, 15, "inverse_vol"):
        out.append("the 12-pair year: the 15- and 20-pair books differ, or the 10-pair one does not")
    rows = {name: table(years, RFR) for name, years in TABLES.items()}
    if [len(r) for r in rows.values()] != [8, 8, 0, 8] or len({(r["cagr_pct"], r["sharpe"]) for r in rows["five priced years"]}) != 8:
        out.append(f"the tables' rows: {[len(r) for r in rows.values()]}, {rows['five priced years']}")
    if {r["sharpe"] for r in rows["three priced years, all cash"]} != {None} or table(TABLES["five priced years"], 0.02) == rows["five priced years"]:
        out.append("the all-cash table has a Sharpe ratio, or the risk-free rate does not move the table")
    if [r["cash_periods"] for r in rows["five priced years"]] != [3] * 8:   # 2003, 2005 and the unpriced 2007
        out.append(f"the table's cash years, one of them unpriced: {[r['cash_periods'] for r in rows['five priced years']]}")
    recs = [r for r in book_records(BOOK_YEARS, 20, "inverse_vol") if r["spy_return"] is not None]
    m = compute_metrics([r["portfolio_return"] for r in recs], [r["spy_return"] for r in recs], 1, risk_free_rate=RFR)
    book_m, cmp_m = published(m["portfolio"], SERIES), published(m["comparison"], COMPARED)
    if (None in (book_m["max_drawdown"], book_m["annualized_volatility"], cmp_m["win_rate"], cmp_m["up_capture"], cmp_m["beta"])
            or book_m["max_drawdown"] == book_m["annualized_volatility"] or cmp_m["win_rate"] == cmp_m["up_capture"]):
        out.append(f"the published book's metrics do not tell one key from another: {book_m} {cmp_m}")
    return out + added_problems()


def added_problems():
    """The lines added here are what gate_checks, edge_checks and warm_checks assume, by the fixture's own bars."""
    out = [f"added lines or legs already in the market: {CLASH}"] if CLASH else []
    for leg, pairs in ADDED.items():
        names = [(n + "A", n + "B", n) for n, _, _ in pairs]
        rows = Z.ranked(names, Z.T19, Z.Y19_END)
        if [r[0] for r in rows] != [a for a, _, _ in names] or len({round(r[3], 4) for r in rows}) != len(rows) or rows[-1][3] < 0.71:
            out.append(f"{leg}: pair k is not the k-th by correlation: {[(r[0], round(r[3], 4)) for r in rows]}")
        hl = [Z.half_life(Z.PX[a], Z.PX[b]) for a, b, _ in names]
        if any(h is None or not 5 < h < 60 for h in hl):
            out.append(f"{leg}: a pair without a usable half-life: {hl}")
    # the dates the legs stand on: the module's entry window and the exit helper's delisting bound
    window = [Z.T20 + timedelta(days=offset + extra) for offset in (1, 0) for extra in (0, pairs_universe.ENTRY_WINDOW_DAYS)]
    if window != [JAN2, JAN12, Z.T20, JAN11]:
        out.append(f"the entry window is not 2 to 12 January, 1 to 11 January at same-bar execution: {window}")
    bound = lambda day: pairs_universe.PairLtp(None, {"A": day}).bound("A", "B", Z.T20, Z.Y20_END)
    if ((bound(DEC29), bound(DEC30), bound(DEC2), bound(DEC2 - Z.ONE)) != (JAN5, JAN6, D(2019, 12, 9), Z.Y20_END)
            or any(len(e) < 3 and any(e.values()) for _, _, _, _, e in GATE_RUNS)):
        out.append(f"the delisting bounds are not 5 and 6 January, 9 December and none, or a cash run lists a trade:"
                   f" {bound(DEC29)} {bound(DEC30)} {bound(DEC2)} {bound(DEC2 - Z.ONE)}")
    in_2020 = lambda s: sorted(d for d in Z.PX[s] if d.year == 2020)
    two, eve = JAN2, Z.Y19_END
    for dipped, other in (("MFL3A", "MFL3B"), ("MFL4B", "MFL4A"), ("MBD4B", "MBD4A")):   # 0.99 on 2 January only
        px = Z.PX[dipped]
        if ([d for d in in_2020(dipped) if px[d] < 1.0] != [JAN2] or px[JAN2] != 0.99 or (px[Z.T20], px[JAN3], px[JAN12]) != (1.0, 1.0, 1.0)
                or px[eve] < 1.0 or min(Z.PX[other][d] for d in in_2020(other)) < 1.0):
            out.append(f"{dipped} is not under 1.00 on 2 January only: {px[eve]} {px[Z.T20]} {px[JAN2]} {px[JAN3]}")
    late = {"MIN2A": JAN12, "MIN3B": JAN12, "MOUT3A": JAN13, "MOUT4B": JAN13, "MBD2A": JAN6, "MBD3B": JAN6, "MBD5A": JAN13}
    for gone, day in late.items():
        if in_2020(gone)[0] != day or Z.PX[gone][day] < 1.0 or eve not in Z.PX[gone]:
            out.append(f"{gone}'s first bar of 2020 is not {day}: {in_2020(gone)[:2]}")
    for leg, pairs in ADDED.items():   # every other leg prints from 1 January
        for s in (n + side for n, _, _ in pairs for side in "AB"):
            if s not in late and in_2020(s)[:2] != [Z.T20, JAN2]:
                out.append(f"{s} does not print from 1 January: {in_2020(s)[:2]}")
    if max(Z.PX["MST3A"]) != JAN12 or max(Z.PX["MST3B"]) != Z.END:
        out.append(f"MST3A's last bar is not the window's last day, or MST3B stops too: {max(Z.PX['MST3A'])}")
    # the edge pair: its bar the day before the formation year, and the one the day after, move every estimate
    base, row = edge_wanted(), Z.corr_row("MEDGA", "MEDGB", "MEDG", Z.T19, Z.Y19_END)
    for name, days in (("before", (Z.Y18_END, Z.Y19_END)), ("after", (Z.T19, Z.T20))):
        moved, wider = edge_wanted(*days), Z.corr_row("MEDGA", "MEDGB", "MEDG", *days)
        still = [i for i in (0, 1, 3, 4) if moved[i] is not None and abs(moved[i] - base[i]) < 1e-4]   # beta, mean, half-life, vol
        if still or wider[4] != row[4] + 1 or abs(wider[3] - row[3]) < 1e-3:
            out.append(f"the edge pair's bar the day {name} the formation year does not move the estimates: {moved} against {base}")
    if min(Z.PX["MEDGA"]) != Z.Y18_END or min(Z.PX["MEDGB"]) != Z.Y18_END or row[4] != 364:
        out.append(f"the edge pair's bars do not start the day before the formation year: {min(Z.PX['MEDGA'])}")
    # the weekday pair: a z-score over 2.0 on the first bar of 2020, converged two bars later, exit on the Monday
    at = WEEKDAYS.index(Z.T20)
    spread = [math.log(Z.PX["MWKA"][d] / Z.PX["MWKB"][d]) for d in WEEKDAYS]
    z = lambda i: Z.zscore(spread[i - 39:i + 1])
    held = lambda days: sum(1 for d in WEEKDAYS if Z.T20 - timedelta(days=days) <= d < Z.T20)
    if (WEEKDAYS[at + 1:at + 4] != [two, D(2020, 1, 3), D(2020, 1, 6)] or z(at) < 2.0 or abs(z(at + 2)) > 0.5
            or any(abs(z(i)) >= 2.0 for i in range(at + 1, len(WEEKDAYS)))):
        out.append(f"the weekday pair's signal: z {z(at)} on {WEEKDAYS[at]}, {z(at + 2)} two bars later")
    if not held(40) < 39 <= held(80):   # 39 bars before the year fill the window on its first bar
        out.append(f"weekday bars before the year: {held(40)} in 40 calendar days, {held(80)} in 80")
    # the time-stop pair converges on its 60th held day and not before; the negative-ratio pair would have a half-life
    ramp = [STOP_EVENTS.get(d, Z.noise(d)) for d in Z.span(MAR1 - timedelta(days=39), MAY1)]
    zs = [Z.zscore(ramp[i - 39:i + 1]) for i in range(39, len(ramp))]   # 1 March to 1 May
    if (MAY1 - MAR1).days != 61 or zs[0] < 2.0 or abs(zs[-1]) > 0.5 or min(abs(z) for z in zs[:-1]) <= 0.5:
        out.append(f"the time-stop pair: z {zs[0]} on its signal, {zs[-1]} on the 60th held day, {min(abs(z) for z in zs[:-1])} at least before")
    beta, hl = ols_beta(Z.PX["MNGA"], Z.PX["MNGB"], Z.T19, Z.Y19_END), Z.half_life(Z.PX["MNGA"], Z.PX["MNGB"])
    if not beta < -0.5 or hl is None or not 5 < hl < 60:
        out.append(f"the negative-ratio pair: a hedge ratio of {beta}, a half-life of {hl}")
    return out


def failures(src, first_only=False):
    out = []
    try:
        mod = load(src)
        # path_checks first: a mutant of the standalone file's name must fail before anything is written
        for check in (path_checks, book_checks, Z.unit_checks, warm_checks, vol_checks, edge_checks, Z.formation_checks,
                      year_checks, gate_checks, primary_checks, Z.fetch_checks, two_exchange_checks, main_checks, cli_checks):
            for f in check(mod):
                out.append(f)
                if first_only:
                    return out
    except Exception as e:  # a mutant that breaks the topic fails loudly
        out.append(f"error: {type(e).__name__}: {e}")
    finally:
        Z.WH.problems = []
    return out


def within(start, stop, old, new):
    """A mutant inside one function: (the function's source, the same with `old` replaced)."""
    seg = SRC[SRC.index(start):SRC.index(stop)]
    return (seg, seg.replace(old, new)) if seg.count(old) == 1 else (f"<{old!r} is not unique in {start}>", new)


def nth(start, stop, old, new, n):
    """A mutant of the n-th of the two `old` inside one function: (the function's source, the same with it replaced)."""
    seg = SRC[SRC.index(start):SRC.index(stop)]
    parts = seg.split(old)
    if len(parts) != 3:
        return f"<{old!r} is not in {start} twice>", new
    return seg, old.join(parts[:n + 1]) + new + old.join(parts[n + 1:])


def check_moved():
    """A mutant: the company check runs on the entered pairs, after the entry step, not on the year's selected pairs."""
    seg = SRC[SRC.index(CHECK):SRC.index(STEP_5)]
    return seg, seg.replace(CHECK, "same_company = 0") + CHECK.replace("in selected", "in entered") + "\n\n        "


def shared_mutants():
    """(pairs-zscore's mutants that fit this topic, the names of the others).

    One fits when its anchor is in this source exactly once and OWN has no mutant of that name:
    this topic words that rule differently, or has its own mutant for it. SHARED_MIN keeps the
    count from falling: a shared rule that stops being mutated here fails the run.
    """
    fit = {name: pair for name, pair in Z.MUTANTS.items() if name not in OWN and SRC.count(pair[0]) == 1}
    return fit, sorted(set(Z.MUTANTS) - set(fit) - set(OWN))


SHARED_MIN = 102   # pairs-zscore's mutants that fit this source today
SIZES_LINE = "PORTFOLIO_SIZES   = [5, 10, 15, 20]"
ROWS = "_MAX_CANDIDATES   = 20 * 10 "
MAX_N = "max_n = max(PORTFOLIO_SIZES)"
CORR = ("def compute_pair_candidates", "def estimate_spread_params")
EST = ("def estimate_spread_params", "def entered_pairs")
FS, FE = "fs = formation_start.isoformat()", "fe = formation_end.isoformat()"
DAY_EARLY = "'{(formation_start - timedelta(days=1)).isoformat()}'"
DAY_LATE = "'{(formation_end + timedelta(days=1)).isoformat()}'"
CHECK = "same_company = check_pairs(con, year, [(p[0], p[1], p[3]) for p in selected])"
ENTRY_WINDOW = "window_days=ENTRY_WINDOW_DAYS, offset_days=offset_days, with_epochs=True)"
ENTRY_BARS = "pa_row, pb_row = entry.get(sym_a), entry.get(sym_b)"
FLOOR_TEST = "if float(pa_row[0]) < MIN_LEG_PRICE or float(pb_row[0]) < MIN_LEG_PRICE:"
ENTRY_BOUND = "bound = ltp.bound(sym_a, sym_b, trading_start, trading_end) if ltp is not None else trading_end"
PAST_BOUND = "if utc_date(max(pa_row[1], pb_row[1])) > bound:"
ENTRY_CALL = "entered = entered_pairs(con, selected, trading_start, trading_end, offset_days, ltp)"
YEAR_CASH = "invested = n_entered >= MIN_PAIRS_ACTIVE"
TRADED = "for (sym_a, sym_b, sector, corr, beta, _mean, _std, half_life) in entered:"
STEP_5 = "# ── Step 5: Compute spread vols"
WARM_UP = "warmup_start = trading_start - timedelta(days=Z_LOOKBACK * 2)"
EXIT_ORDER = ('if abs(z) <= Z_EXIT:\n                exit_type = "convergence"\n'
              '            elif trading_day >= MAX_HOLD_DAYS:\n                exit_type = "time_stop"')
EXIT_SWAPPED = ('if trading_day >= MAX_HOLD_DAYS:\n                exit_type = "time_stop"\n'
                '            elif abs(z) <= Z_EXIT:\n                exit_type = "convergence"')
BENCH_FROM = "WHERE symbol = '{benchmark_symbol}'\n              AND trade_date >= '{trading_start.isoformat()}'"
SIM_TAIL = "offset_days=offset_days, ltp=ltp\n            ) if invested else None"
CASH = "if n_entered < MIN_PAIRS_ACTIVE or not pairs:"
SLICE = 'pairs   = yd["pairs"][:n_pairs]'
VOL = ("def compute_spread_vol_during_formation", "def simulate_pair_trades")
VOL_WINDOW = "WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'"
VOL_MIN = "if not row or row[0] is None or row[1] < 100:"
TABLE_MIN = "if len(port_returns) < 3:"
TABLE_RATE = "m = compute_metrics(port_returns, spy_returns, 1,\n                                risk_free_rate=risk_free_rate)"
OUT = ("def build_output", "def diversification_path")
OUT_CASH = 'cash_periods = sum(1 for y in executed_primary if y["is_cash"])'
OUT_ACTIVE = 'invested_yrs = [y for y in executed_primary if not y["is_cash"]]'
COUNTS = '**(per_year or {}).get(y["year"], {}),'
RULE = ('        "universe_rule":          {**UNIVERSE_NOTES, "top_n_per_sector": TOP_N_PER_SECTOR,\n'
        '                                   "min_formation_bars": MIN_CORR_DAYS},\n')
PRIMARY = 'primary_yearly = compute_portfolio_for_size(per_year_data, 20, "inverse_vol")'
FETCH = ("fetched = fetch_data_via_api(cr, exchanges, verbose=verbose,\n"
         "                                 benchmark_symbol=benchmark_symbol, offset_days=offset_days)")
RUN = ("con, use_costs=use_costs, verbose=verbose,\n"
       "        benchmark_symbol=benchmark_symbol, offset_days=offset_days, ltp=ltp\n    )")
SIDE = "div_path = diversification_path(output_path)"
SIDE_NAME = '"diversification_" + os.path.basename(output_path)'
ALL_MARKETS = "benchmark_name=bench_name,\n                                    exit_method=args.exit_method)"
ONE_MARKET = "benchmark_name=bench_name,\n               exit_method=args.exit_method)"
OWN = {
    # formation: 200 correlation rows, the first 20 pairs with a half-life
    "201 correlation rows": (ROWS, "_MAX_CANDIDATES   = 20 * 10 + 1 "),
    "199 correlation rows": (ROWS, "_MAX_CANDIDATES   = 20 * 10 - 1 "),
    "19 pairs formed": (MAX_N, MAX_N + " - 1"),
    "21 pairs formed": (MAX_N, MAX_N + " + 1"),
    "half-life filter on the first 20 correlation rows only": (
        "for (sym_a, sym_b, sector, corr, common_days) in candidates:",
        "for (sym_a, sym_b, sector, corr, common_days) in candidates[:max_n]:"),
    # the formation windows, a day either side
    "correlation list reads the day before the formation year": within(*CORR, FS, "fs = " + DAY_EARLY[2:-2]),
    "correlation list reads the day after the formation year": within(*CORR, FE, "fe = " + DAY_LATE[2:-2]),
    "hedge ratio reads the day before the formation year": nth(*EST, "'{fs}'", DAY_EARLY, 0),
    "hedge ratio reads the day after the formation year": nth(*EST, "'{fe}'", DAY_LATE, 0),
    "half-life reads the day before the formation year": nth(*EST, "'{fs}'", DAY_EARLY, 1),
    "half-life reads the day after the formation year": nth(*EST, "'{fe}'", DAY_LATE, 1),
    # entry: each leg's first bar in the 10-day window from T + offset, 1.00 or more, on or before the delisting bound
    "entry window of 9 days": (ENTRY_WINDOW, ENTRY_WINDOW.replace("ENTRY_WINDOW_DAYS", "ENTRY_WINDOW_DAYS - 1")),
    "entry window of 11 days": (ENTRY_WINDOW, ENTRY_WINDOW.replace("ENTRY_WINDOW_DAYS", "ENTRY_WINDOW_DAYS + 1")),
    "entry window runs to the year's end": (ENTRY_WINDOW, ENTRY_WINDOW.replace("ENTRY_WINDOW_DAYS", "(trading_end - trading_start).days")),
    "entry window opens on 1 January whatever the offset": (ENTRY_WINDOW, ENTRY_WINDOW.replace("offset_days=offset_days", "offset_days=0")),
    "entry window opens on 2 January whatever the offset": (ENTRY_WINDOW, ENTRY_WINDOW.replace("offset_days=offset_days", "offset_days=1")),
    "entry window opens a day late": (ENTRY_WINDOW, ENTRY_WINDOW.replace("offset_days=offset_days", "offset_days=offset_days + 1")),
    "entry window opens a day early": (ENTRY_WINDOW, ENTRY_WINDOW.replace("offset_days=offset_days", "offset_days=offset_days - 1")),
    "entry offset not passed to the entry test": (ENTRY_CALL, ENTRY_CALL.replace("offset_days, ltp)", "ltp=ltp)")),
    "A leg's entry bar not read": (ENTRY_BARS, ENTRY_BARS.replace("entry.get(sym_a), entry.get(sym_b)", "entry.get(sym_b), entry.get(sym_b)")),
    "B leg's entry bar not read": (ENTRY_BARS, ENTRY_BARS.replace("entry.get(sym_a), entry.get(sym_b)", "entry.get(sym_a), entry.get(sym_a)")),
    "entry price of exactly 1.00 refused in the year loop": (FLOOR_TEST, FLOOR_TEST.replace("<", "<=")),
    "A leg's entry price not tested": (FLOOR_TEST, "if float(pb_row[0]) < MIN_LEG_PRICE:"),
    "B leg's entry price not tested": (FLOOR_TEST, "if float(pa_row[0]) < MIN_LEG_PRICE:"),
    "delisting bound ignored at entry": (PAST_BOUND, "if False:"),
    "exit helper not passed to the entry test": (ENTRY_CALL, ENTRY_CALL.replace(", ltp)", ")")),
    "a bar on the delisting bound refused at entry": (PAST_BOUND, PAST_BOUND.replace("> bound", ">= bound")),
    "a bar the day after the delisting bound entered": (PAST_BOUND, PAST_BOUND.replace("> bound", "> bound + timedelta(days=1)")),
    "only the A leg's entry bar held to the bound": (PAST_BOUND, PAST_BOUND.replace("max(pa_row[1], pb_row[1])", "pa_row[1]")),
    "only the B leg's entry bar held to the bound": (PAST_BOUND, PAST_BOUND.replace("max(pa_row[1], pb_row[1])", "pb_row[1]")),
    "the earlier of the two entry bars held to the bound": (PAST_BOUND, PAST_BOUND.replace("max(", "min(")),
    "each leg held to its own delisting bound, not the pair's": (
        ENTRY_BOUND + "\n        " + PAST_BOUND,
        "bound_a = ltp.bound(sym_a, sym_a, trading_start, trading_end) if ltp is not None else trading_end\n"
        "        bound_b = ltp.bound(sym_b, sym_b, trading_start, trading_end) if ltp is not None else trading_end\n"
        "        if utc_date(pa_row[1]) > bound_a or utc_date(pb_row[1]) > bound_b:"),
    "entry bound from the A leg alone": (ENTRY_BOUND, ENTRY_BOUND.replace("ltp.bound(sym_a, sym_b,", "ltp.bound(sym_a, sym_a,")),
    "entry bound from the B leg alone": (ENTRY_BOUND, ENTRY_BOUND.replace("ltp.bound(sym_a, sym_b,", "ltp.bound(sym_b, sym_b,")),
    "entry bound anchored at the year's end": (ENTRY_BOUND, ENTRY_BOUND.replace("trading_start, trading_end)", "trading_end, trading_end)")),
    "entry bound anchored a day before the year": (
        ENTRY_BOUND, ENTRY_BOUND.replace("trading_start, trading_end)", "trading_start - timedelta(days=1), trading_end)")),
    "entry bound anchored a day into the year": (
        ENTRY_BOUND, ENTRY_BOUND.replace("trading_start, trading_end)", "trading_start + timedelta(days=1), trading_end)")),
    # the year: its cash gate, the benchmark inside it, what the simulation is asked, what is recorded
    "benchmark year starts a day late": (BENCH_FROM, BENCH_FROM.replace(">=", ">")),
    "a year's cash decided on the pairs formed": (YEAR_CASH, YEAR_CASH.replace("n_entered", "n_formed")),
    "cash with exactly 3 entered pairs": (YEAR_CASH, YEAR_CASH.replace(">=", ">")),
    "pairs that are not entered still trade": (TRADED, TRADED.replace("in entered:", "in selected:")),
    "same-company pairs checked among the entered only": check_moved(),
    "z-score warm-up of 40 calendar days": (WARM_UP, WARM_UP.replace("Z_LOOKBACK * 2", "Z_LOOKBACK")),
    "z-score warm-up of 39 calendar days": (WARM_UP, WARM_UP.replace("Z_LOOKBACK * 2", "Z_LOOKBACK - 1")),
    "time stop tested before convergence": (EXIT_ORDER, EXIT_SWAPPED),
    "a negative hedge ratio accepted": ("if not math.isfinite(beta) or beta <= 0:", "if not math.isfinite(beta):"),
    "trading years from 2006": ("START_YEAR        = 2005\n", "START_YEAR        = 2006\n"),
    "trading years to 2023": ("END_YEAR          = 2024\n", "END_YEAR          = 2023\n"),
    "run_single defaults to an exit method": ('benchmark_name="S&P 500", *, exit_method):',
                                              'benchmark_name="S&P 500", *, exit_method="ltp"):'),
    "one exchange of a two-exchange leg": ("fetch_universe(cr, con, exchanges, years,", "fetch_universe(cr, con, exchanges[:1], years,"),
    "a year of two entered pairs still trades": (SIM_TAIL, SIM_TAIL.replace("if invested", "if True")),
    "exit helper not passed to the simulation": (SIM_TAIL, SIM_TAIL.replace(", ltp=ltp", "")),
    "entry offset not passed to the simulation": (SIM_TAIL, SIM_TAIL.replace("offset_days=offset_days, ", "")),
    "costs switch not passed to the simulation": ("caps.get(sym_a), caps.get(sym_b), use_costs=use_costs,",
                                                  "caps.get(sym_a), caps.get(sym_b),"),
    "B leg's cap for both legs": ("caps.get(sym_a), caps.get(sym_b), use_costs", "caps.get(sym_b), caps.get(sym_b), use_costs"),
    "spread vol on a hedge ratio of 1": ("con, sym_a, sym_b, beta, formation_start, formation_end",
                                        "con, sym_a, sym_b, 1.0, formation_start, formation_end"),
    "a pair's year is its best trade": ('annual_return = sum(t["pair_return"] for t in trades)',
                                        'annual_return = max(t["pair_return"] for t in trades)'),
    "a pair's convergence rate counts its time stops": ('for t in trades if t["exit_type"] == "convergence")',
                                                        'for t in trades if t["exit_type"] == "time_stop")'),
    "convergences counted as time stops": ('for t in all_trades if t["exit_type"] == "convergence")',
                                           'for t in all_trades if t["exit_type"] == "time_stop")'),
    "time stops counted as loss stops": ('n_time   = sum(1 for t in all_trades if t["exit_type"] == "time_stop")',
                                         'n_time   = sum(1 for t in all_trades if t["exit_type"] == "loss_stop")'),
    "loss stops counted as time stops": ('n_loss   = sum(1 for t in all_trades if t["exit_type"] == "loss_stop")',
                                         'n_loss   = sum(1 for t in all_trades if t["exit_type"] == "time_stop")'),
    "year-end closes counted as last-bar closes": ('n_end    = sum(1 for t in all_trades if t["exit_type"] == "year_end")',
                                                   'n_end    = sum(1 for t in all_trades if t["exit_type"] == "last_bar")'),
    "entered pairs recorded as formed": ('"pairs_formed":       n_formed,', '"pairs_formed":       n_entered,'),
    "formed pairs recorded as entered": ('"pairs_entered":      n_entered,', '"pairs_entered":      n_formed,'),
    # the spread volatility: the formation window, the hedge ratio, 100 daily changes
    "spread vol on bars after the formation year": within(*VOL, VOL_WINDOW, "WHERE trade_date >= '{fs}'"),
    "spread vol on bars before the formation year": within(*VOL, VOL_WINDOW, "WHERE trade_date <= '{fe}'"),
    "spread vol without the year's last day": within(*VOL, VOL_WINDOW, VOL_WINDOW.replace("<=", "<")),
    "spread vol without the year's first day": within(*VOL, VOL_WINDOW, VOL_WINDOW.replace(">=", ">")),
    "spread vol reads the day before the formation year": within(*VOL, VOL_WINDOW, VOL_WINDOW.replace("'{fs}'", DAY_EARLY)),
    "spread vol reads the day after the formation year": within(*VOL, VOL_WINDOW, VOL_WINDOW.replace("'{fe}'", DAY_LATE)),
    "spread vol of the unhedged spread": within(*VOL, "a.lp - {beta} * b.lp AS s", "a.lp - b.lp AS s"),
    "spread vol not annualised": ("STDDEV(ds) * SQRT(252) AS ann_vol", "STDDEV(ds) AS ann_vol"),
    "spread vol on 99 daily changes": (VOL_MIN, VOL_MIN.replace("< 100", "< 99")),
    "spread vol on 100 daily changes refused": (VOL_MIN, VOL_MIN.replace("< 100", "< 101")),
    # the books: the first N entered pairs, cash under 3 of them, the two weightings
    "5-pair book of 4": (SIZES_LINE, "PORTFOLIO_SIZES   = [4, 10, 15, 20]"),
    "5-pair book of 6": (SIZES_LINE, "PORTFOLIO_SIZES   = [6, 10, 15, 20]"),
    "10-pair book of 9": (SIZES_LINE, "PORTFOLIO_SIZES   = [5, 9, 15, 20]"),
    "10-pair book of 11": (SIZES_LINE, "PORTFOLIO_SIZES   = [5, 11, 15, 20]"),
    "15-pair book of 14": (SIZES_LINE, "PORTFOLIO_SIZES   = [5, 10, 14, 20]"),
    "15-pair book of 16": (SIZES_LINE, "PORTFOLIO_SIZES   = [5, 10, 16, 20]"),
    "20-pair book of 19": (SIZES_LINE, "PORTFOLIO_SIZES   = [5, 10, 15, 19]"),
    "20-pair book of 21": (SIZES_LINE, "PORTFOLIO_SIZES   = [5, 10, 15, 21]"),
    "a book holds one pair more": (SLICE, SLICE.replace("[:n_pairs]", "[:n_pairs + 1]")),
    "a book holds one pair fewer": (SLICE, SLICE.replace("[:n_pairs]", "[:n_pairs - 1]")),
    "a book skips the most correlated pair": (SLICE, SLICE.replace("[:n_pairs]", "[1:n_pairs + 1]")),
    "cash decided on the pairs that traded": (CASH, CASH.replace("n_entered <", "n_active <")),
    "a book's cash decided on the pairs formed": (CASH, CASH.replace("n_entered <", 'yd["formation"]["pairs_formed"] <')),
    "a book of exactly 3 entered pairs is cash": (CASH, CASH.replace("n_entered <", "n_entered <=")),
    "pairs that did not trade counted as active": ('n_active = sum(1 for p in pairs if p["n_trades"] > 0)',
                                                   'n_active = sum(1 for p in pairs if p["n_trades"] >= 0)'),
    "equal weight over the book's nominal size": ("weight = 1.0 / len(pairs) if pairs else 1.0",
                                                  "weight = 1.0 / n_pairs if pairs else 1.0"),
    "the equal book weighted by 1 / vol": ('if allocation == "equal":', 'if allocation != "equal":'),
    "weights by vol, not its inverse": ("inv_vols.append(1.0 / v if (v and v > 0) else None)",
                                        "inv_vols.append(v if (v and v > 0) else None)"),
    "a pair without a vol gets no weight": ("w = missing_share / n_missing", "w = 0.0"),
    "pairs with a vol share the whole book": ("valid_share   = 1.0 - missing_share", "valid_share   = 1.0"),
    # the size x allocation table
    "table on 2 priced years": (TABLE_MIN, TABLE_MIN.replace("< 3", "< 2")),
    "table refused on 3 priced years": (TABLE_MIN, TABLE_MIN.replace("< 3", "< 4")),
    "table without the leg's risk-free rate": (TABLE_RATE, "m = compute_metrics(port_returns, spy_returns, 1)"),
    "table's cash years counted on priced years only": (
        'cash_periods  = sum(1 for y in yearly if y["is_cash"])',
        'cash_periods  = sum(1 for y in yearly if y["is_cash"] and y["spy_return"] is not None)'),
    "table's active pairs averaged over cash years too": ('invested      = [y for y in yearly if not y["is_cash"]]',
                                                          "invested      = list(yearly)"),
    # the published book and its record
    "published book of 19 pairs": (PRIMARY, PRIMARY.replace("20", "19")),
    "published book of 21 pairs": (PRIMARY, PRIMARY.replace("20", "21")),
    "published book equal-weighted": (PRIMARY, PRIMARY.replace('"inverse_vol"', '"equal"')),
    "published metrics without the leg's risk-free rate": (
        "m = compute_metrics(port_returns, spy_returns, 1, risk_free_rate=risk_free_rate)",
        "m = compute_metrics(port_returns, spy_returns, 1)"),
    "table computed without the leg's risk-free rate": ("per_year_data, risk_free_rate=risk_free_rate\n    )",
                                                        "per_year_data\n    )"),
    "published cash years counted on priced years only": within(
        *OUT, OUT_CASH, OUT_CASH.replace('y["is_cash"])', 'y["is_cash"] and y["spy_return"] is not None)')),
    "published active pairs averaged over cash years too": (OUT_ACTIVE, "invested_yrs = list(executed_primary)"),
    "universe counts of the year before": (COUNTS, COUNTS.replace('y["year"]', 'y["year"] - 1')),
    "universe counts not recorded": (COUNTS, "**{},"),
    "formed pairs not recorded": ('**yd["formation"],', "**{},"),
    "max drawdown published from the volatility": ('"max_drawdown":          pct(s.get("max_drawdown")),',
                                                   '"max_drawdown":          pct(s.get("annualized_volatility")),'),
    "win rate published from the up capture": ('"win_rate":           pct(c.get("win_rate")),',
                                               '"win_rate":           pct(c.get("up_capture")),'),
    "win rate against the benchmark published from the up capture": ('"win_rate_vs_spy":        pct(c.get("win_rate")),',
                                                                     '"win_rate_vs_spy":        pct(c.get("up_capture")),'),
    "beta against the benchmark not published": ('            "beta":               rnd(c.get("beta")),\n', ""),
    "a run-time field in the results": ('"frequency":              "daily_zscore",',
                                        '"frequency":              "daily_zscore", "run_seconds": time.time(),'),
    "the first year's formed pairs on every year": ('yd = [d for d in per_year_data if d["year"] == y["year"]][0]',
                                                    "yd = per_year_data[0]"),
    "universe rule not recorded": (RULE, ""),
    "universe rule in place of the leg's name": ('"universe":               universe_name,',
                                                 '"universe":               dict(UNIVERSE_NOTES),'),
    "top N recorded from another constant": ('"top_n_per_sector": TOP_N_PER_SECTOR,', '"top_n_per_sector": MIN_CANDIDATES,'),
    "formation bars recorded from another constant": ('"min_formation_bars": MIN_CORR_DAYS}', '"min_formation_bars": Z_LOOKBACK}'),
    "listing rule not recorded": ('"listing_filter":         listing_filter,', '"listing_filter":         None,'),
    "ISIN dedup not recorded": ('"isin_dedup":             True,', '"isin_dedup":             False,'),
    "company dedup not recorded": ('"company_dedup":          True,', '"company_dedup":          False,'),
    "benchmark name not recorded": ("benchmark_name=benchmark_name,\n                       benchmark_symbol=benchmark_symbol,\n",
                                    "benchmark_symbol=benchmark_symbol,\n"),
    "benchmark symbol not recorded": ("benchmark_name=benchmark_name,\n                       benchmark_symbol=benchmark_symbol,\n",
                                      "benchmark_name=benchmark_name,\n"),
    # run_single: what it asks of the fetch and of the year loop
    "entry offset not passed to the fetch": (FETCH, FETCH.replace(", offset_days=offset_days)", ")")),
    "benchmark not passed to the fetch": (FETCH, FETCH.replace("benchmark_symbol=benchmark_symbol, ", "")),
    "exit helper not passed to the year loop": (RUN, RUN.replace(", ltp=ltp", "")),
    "costs switch not passed to the year loop": (RUN, RUN.replace("use_costs=use_costs, ", "")),
    "entry offset not passed to the year loop": (RUN, RUN.replace(", offset_days=offset_days", "")),
    "benchmark not passed to the year loop": (RUN, RUN.replace("benchmark_symbol=benchmark_symbol, ", "")),
    # the standalone table: beside the results file, under that file's name
    "standalone table under one fixed name": (SIDE, 'div_path = os.path.join(out_dir or ".", "diversification_analysis.json")'),
    "standalone table named like a market's results": (SIDE_NAME, 'os.path.basename(output_path)[:-5] + "_diversification.json"'),
    "standalone table in the working directory": ("return os.path.join(os.path.dirname(output_path),", "return os.path.join("),
    "standalone table holds the whole results": ("json.dump(diversification_analysis, f, indent=2)", "json.dump(out, f, indent=2)"),
    # only ltp exits exist, on every market, each against its own benchmark
    "exit method not passed in the all-market loop": (ALL_MARKETS, "benchmark_name=bench_name)"),
    "exit method not passed for one market": (ONE_MARKET, "benchmark_name=bench_name)"),
    "every market against SPY in the all-market loop": ("get_local_benchmark(preset_exchanges)", "get_local_benchmark(None)"),
    "one market against SPY": ("bench_sym, bench_name = get_local_benchmark(exchanges)",
                               "bench_sym, bench_name = get_local_benchmark(None)"),
}


def main():
    shared, foreign = shared_mutants()
    mutants = {**shared, **OWN}
    base = list(guard_checks()) + fixture_problems()
    if len(shared) < SHARED_MIN:
        base.append(f"{len(shared)} of pairs-zscore's mutants fit this source, {SHARED_MIN} did; the others: {foreign}")
    if Z.OVERCLAIM in SRC.lower():
        base.append("the topic's source claims survivorship is removed: it is only reduced")
    base = base or failures(SRC)
    if base:
        print("test_pairs_multi_pair: FAIL", base)
        sys.exit(1)
    loose = [name for name, (old, _) in OWN.items() if SRC.count(old) != 1]
    if loose:
        print("test_pairs_multi_pair: FAIL (mutant anchors not unique:", loose, ")")
        sys.exit(1)
    survived = []
    for name, (old, new) in mutants.items():
        if not failures(SRC.replace(old, new), first_only=True):
            survived.append(name)
    if survived:
        print("test_pairs_multi_pair: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_pairs_multi_pair: PASS ({len(Z.PROFILE)} lines, {len(mutants)} mutants caught, {len(shared)} of them"
          f" shared with pairs-zscore; {len(foreign)} of that topic's do not fit this source)")


if __name__ == "__main__":
    main()
