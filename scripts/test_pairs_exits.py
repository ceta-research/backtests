#!/usr/bin/env python3
"""Offline check of the pair-level exits in pairs_universe.py (no network).

A pair is one position: when a leg stops trading, both legs close at the last bar on which both
traded. Checked on made-up bars: last_common_bar takes the last common day after the entry bars
and on or before the exit date, never a later one; a delisting bounds the window at the delisting
plus the grace days, unless it is more than 30 days before the leg's entry bar; PairLtp.fill counts
in pairs, once per pair and whichever leg has no exit price, judges a delisting from that leg's own
entry bar, takes no bar outside (later entry bar, exit date], asks nothing on a cash year and writes
a results block scripts/gate_ltp.py accepts unchanged; PairLtp.close labels a position closed more
than 10 days before year end. Every mutant of the module's source must trip a check.

The three pairs topics are checked once they import pairs_universe; until then each is reported as
not wired. pairs-fundamentals: with three entered pairs and one missing exit (the A leg's, then the
B leg's) the year is invested in drop, ltp and both mode alike (cash is decided on the entry-side
count), the drop run carries no ltp key, the both run publishes the drop book next to the LTP book,
the pair is filled once, two entered pairs are a cash year that asks for no fill, and a
same-company pair stops the run. pairs-zscore and pairs-multi-pair: an exit signal executes on the
next bar, a z-score needs Z_LOOKBACK bars, a position open at the pair's last bar is labelled by
PairLtp.close inside PairLtp.bound, and --exit-method drop or both is refused. All three: a pair's
round trip costs one tiered cost per leg.

Usage:
    python scripts/test_pairs_exits.py [repo root]
"""
import contextlib
import io
import math
import os
import re
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "pairs_universe.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

ENTRY, EXIT = date(2020, 1, 2), date(2020, 12, 31)   # entry bar of every leg; the year's exit date
JUN15, MAR31 = date(2020, 6, 15), date(2020, 3, 31)
BASE = {s: 10.0 * (i + 1) for i, s in enumerate(
    ["A", "B", "C", "D", "E", "G", "H", "I", "R", "S", "X", "Y", "N1", "N2", "K1", "K2", "DUPA", "U", "W"])}
# symbol: bar ranges; every leg has its entry bar
RANGES = {
    "A": [(ENTRY, JUN15)], "B": [(ENTRY, date(2021, 1, 15))],            # A stops in June
    "C": [(ENTRY, date(2021, 1, 15))], "D": [(ENTRY, date(2021, 1, 15))],
    "E": [(ENTRY, MAR31), (date(2020, 4, 10), date(2020, 4, 10))],        # last common day is not either
    "G": [(ENTRY, MAR31), (date(2020, 4, 20), date(2020, 4, 20))],        # leg's own last bar
    "H": [(ENTRY, MAR31)], "I": [(ENTRY, EXIT)],                          # H prints 0.0 on 1 April
    "R": [(ENTRY, date(2020, 7, 10))], "S": [(ENTRY, EXIT)],              # R delisted 10 June, repeats after
    "X": [(ENTRY, EXIT)], "Y": [(ENTRY, EXIT)],                           # delisting rows 31 and 30 days before entry
    "N1": [(ENTRY, ENTRY)], "N2": [(ENTRY, EXIT)],                        # no common bar after entry
    "K1": [(ENTRY, date(2020, 1, 6))], "K2": [(date(2020, 1, 6), date(2020, 1, 6))],   # K2 enters on K1's last bar
    "DUPA": [(ENTRY, date(2020, 2, 10))],                                 # two rows on its last day
    "U": [(ENTRY, date(2020, 12, 20)), (date(2021, 1, 5), date(2021, 1, 15))],   # no bar at the exit, back after it
    "W": [(date(2020, 1, 3), JUN15)],                                     # enters a day after Y: 31 days after Y's delisting
}
DELISTED = [("R", "2015-01-01"), ("R", "2020-06-10"), ("X", "2019-12-02"), ("Y", "2019-12-03"), ("Z", None)]
BOUNDS = {"R": date(2020, 6, 10), "X": date(2019, 12, 2), "Y": date(2019, 12, 3)}
TOPICS = ("pairs-zscore", "pairs-multi-pair")


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def px(symbol, d):
    """A price no other (symbol, day) has."""
    return BASE[symbol] + (d - date(2020, 1, 1)).days / 1000.0


def prices(rows):
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)")
    if rows:
        con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)", [(s, d, ep(d), p) for s, d, p in rows])
    return con


_CONS = {}


def cached(key, build):
    """Fixture connections are only read: one build serves the base run and every mutant."""
    if key not in _CONS:
        _CONS[key] = build()
    return _CONS[key]


def module_con():
    rows = []
    for s, spans in RANGES.items():
        for first, last in spans:
            rows += [(s, first + timedelta(days=i), px(s, first + timedelta(days=i)))
                     for i in range((last - first).days + 1)]
    rows += [("H", date(2020, 4, 1), 0.0), ("DUPA", date(2020, 2, 10), px("DUPA", date(2020, 2, 10)) + 1.0)]
    return prices(rows)


class _Client:
    """Stands in for CetaResearch: answers the delisted_companies query from made-up rows."""

    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate VARCHAR)")
        self.con.executemany("INSERT INTO delisted_companies VALUES (?, ?)", DELISTED)

    def query(self, sql, **kw):
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def load(src, path=PATH, name="pairs_universe_under_test"):
    mod = types.ModuleType(name)
    mod.__file__ = path
    exec(compile(src, path, "exec"), mod.__dict__)
    mod.print = lambda *a, **k: None
    return mod


def bar(symbol_a, symbol_b, d):
    return (d, px(symbol_a, d), px(symbol_b, d))


def module_checks(mod):
    con = cached("module", module_con)
    bounds = mod.load_delist_bounds(_Client())
    if bounds != BOUNDS:
        yield f"load_delist_bounds: {bounds} != {BOUNDS}"
    lcb = lambda a, b, after=ENTRY, upto=EXIT, bnd=bounds, **kw: mod.last_common_bar(con, a, b, after, upto, bnd, **kw)
    cases = [
        ("a leg ends in June: both close on its last bar", lcb("A", "B"), bar("A", "B", JUN15)),
        ("nothing dated after the exit date", lcb("C", "D"), bar("C", "D", EXIT)),
        ("the exit date itself counts", lcb("A", "B", upto=date(2020, 6, 14)), bar("A", "B", date(2020, 6, 14))),
        ("the day after the entry bar counts", lcb("A", "B", after=date(2020, 6, 14)), bar("A", "B", JUN15)),
        ("the entry bar itself does not", lcb("A", "B", after=JUN15), None),
        ("the last COMMON day, not a leg's own last bar", lcb("E", "G"), bar("E", "G", MAR31)),
        ("a bar at 0.0 is not a traded bar", lcb("H", "I"), bar("H", "I", MAR31)),
        ("two rows on a day: the higher price", lcb("DUPA", "B", upto=date(2020, 2, 10)),
         (date(2020, 2, 10), px("DUPA", date(2020, 2, 10)) + 1.0, px("B", date(2020, 2, 10)))),
        ("the delisting bound cuts repeat bars", lcb("R", "S"), bar("R", "S", date(2020, 6, 17))),
        ("the second leg's delisting bounds too", lcb("S", "R"), bar("S", "R", date(2020, 6, 17))),
        ("no delisting row: the leg's last bar", lcb("R", "S", bnd={}), bar("R", "S", date(2020, 7, 10))),
        ("a delisting 31 days before entry is ignored", lcb("X", "B"), bar("X", "B", EXIT)),
        ("a delisting 30 days before entry bounds", lcb("Y", "B"), None),
        ("the leg's own entry bar is its anchor", lcb("X", "B", anchors={"X": date(2020, 1, 1)}), None),
        ("no common bar after entry", lcb("N1", "N2"), None),
    ]
    for name, got, want in cases:
        if got != want:
            yield f"last_common_bar, {name}: {got} != {want}"

    # PairLtp.fill: counted in pairs, in the caller's order, whichever leg has no exit price
    entry = {s: (px(s, spans[0][0]), ep(spans[0][0])) for s, spans in RANGES.items()}   # each leg's first bar
    exits = {s: px(s, date(2021, 1, 4)) for s in ("B", "C", "D", "Y")}
    exits.update({"A": None, "N2": px("N2", EXIT), "K1": None, "U": None})
    # (D, A): only the second leg is missing. (U, B): U is back after the exit date. (Y, W): Y's
    # delisting is 30 days before its own entry bar, 31 before the pair's later one: Y's bar decides
    pairs = [("A", "B"), ("C", "D"), ("E", "G"), ("N1", "N2"), ("K1", "K2"), ("D", "A"), ("U", "B"), ("Y", "W")]
    ltp = mod.PairLtp(con, bounds)
    got = ltp.fill(2020, pairs, entry, exits, EXIT, 3)
    want = {("A", "B"): bar("A", "B", JUN15)[1:], ("E", "G"): bar("E", "G", MAR31)[1:],
            ("D", "A"): bar("D", "A", JUN15)[1:], ("U", "B"): bar("U", "B", date(2020, 12, 20))[1:]}
    if got != want or list(got) != list(want):
        yield f"fill: {got} != {want}"
    prov = ltp.provenance()
    counts = {k: prov.get(k) for k in ("missing_exits", "ltp_filled", "still_missing")}
    if counts != {"missing_exits": 7, "ltp_filled": 4, "still_missing": 3} or len(prov["fills"]) != 4:
        yield f"fill counts, one per pair: {counts}, {len(prov['fills'])} fills"
    want_fill = {"pair": ["E", "G"], "year": 2020, "entry_bars": ["2020-01-02", "2020-01-02"],
                 "entry_prices": [px("E", ENTRY), px("G", ENTRY)], "last_common_bar": "2020-03-31",
                 "prices": list(bar("E", "G", MAR31)[1:]), "missing_leg": ["E", "G"],
                 "delisted_date": [None, None], "delisting_bound": ["none", "none"], "exit_date": "2020-12-31"}
    if len(prov["fills"]) == 4 and prov["fills"][1] != want_fill:
        yield f"fill record: {prov['fills'][1]} != {want_fill}"
    legs = [(f["pair"], f["missing_leg"], f["last_common_bar"]) for f in prov["fills"]]
    want_legs = [(["A", "B"], ["A"], "2020-06-15"), (["E", "G"], ["E", "G"], "2020-03-31"),
                 (["D", "A"], ["A"], "2020-06-15"), (["U", "B"], ["U"], "2020-12-20")]
    if legs != want_legs:
        yield f"fill records, the missing leg and the bar of each: {legs} != {want_legs}"
    if prov.get("source") != "last common bar of the pair" or prov.get("delist_grace_days") != 7:
        yield f"provenance: {prov.get('source')!r}, {prov.get('delist_grace_days')}"
    # both legs trade past the exit date: the exit date's bar, nothing later
    late = mod.PairLtp(con, bounds)
    got = late.fill(2020, [("C", "D")], entry, {"C": exits["C"]}, EXIT, 1)
    if got != {("C", "D"): bar("C", "D", EXIT)[1:]} or [f["missing_leg"] for f in late.fills] != [["D"]]:
        yield f"fill of a pair that trades past the exit date: {got}, {late.fills}"
    # whatever last_common_bar returns, a fill outside (later entry bar, exit date] is refused
    real, k2_entry, one_day = mod.last_common_bar, date(2020, 1, 6), timedelta(days=1)
    try:
        for name, day, ok in (("on the later entry bar", k2_entry, False), ("the day after it", k2_entry + one_day, True),
                              ("on the exit date", EXIT, True), ("the day after the exit date", EXIT + one_day, False)):
            mod.last_common_bar = lambda *a, _day=day, **k: (_day, 1.0, 2.0)
            try:
                took = mod.PairLtp(con, bounds).fill(2020, [("K1", "K2")], entry, {}, EXIT, 1) == {("K1", "K2"): (1.0, 2.0)}
            except AssertionError:
                took = False
            if took != ok:
                yield f"fill {'refused' if ok else 'took'} a last common bar {name}"
    finally:
        mod.last_common_bar = real
    bound_ltp = mod.PairLtp(con, bounds)
    bound_ltp.fill(2020, [("R", "S"), ("C", "D"), ("A", "B")], entry, {"C": 1.0, "D": 1.0, "S": 1.0, "B": 1.0}, EXIT, 3)
    record = (bound_ltp.fills or [{}])[0]
    if (record.get("delisted_date"), record.get("delisting_bound")) != (["2020-06-10", None], ["applied", "none"]):
        yield f"fill record of a delisting-bounded pair: {record}"
    # a cash year asks nothing; exactly the minimum is invested
    cash = mod.PairLtp(con, bounds)
    if cash.fill(2020, pairs[:2], entry, exits, EXIT, 3) != {} or cash.missing or cash.fills:
        yield "fill counted or filled a pair on a cash year"
    if set(cash.fill(2020, pairs[:3], entry, exits, EXIT, 3)) != {("A", "B"), ("E", "G")} or cash.missing != 2:
        yield "fill skipped a year with exactly the minimum of entered pairs"

    # the results block: LtpExits' shape, accepted by the gate's own check
    import gate_ltp
    from metrics import compute_metrics
    valid = [{"year": 2019, "portfolio_return": 0.05, "spy_return": 0.10},
             {"year": 2020, "portfolio_return": 0.02, "portfolio_return_ltp": 0.03, "spy_return": 0.08}]
    cagr = lambda rets: round(compute_metrics(rets, [0.10, 0.08], 1, risk_free_rate=0.02)["portfolio"]["cagr"] * 100, 2)
    modes = {m: {"portfolio": {"cagr": cagr([0.05, 0.03 if m == "ltp" else 0.02])}} for m in gate_ltp.EXIT_METHODS}
    for m in ("ltp", "both"):
        with contextlib.redirect_stdout(io.StringIO()):   # the both block prints its two books
            modes[m].update({"exit_method": m, "ltp": ltp.results_block(m, valid, 1, 0.02)})
    leg = {"topic": "pairs-fundamentals", "pairs": [{"published_cagr_path": "portfolio.cagr", "ltp_block_path": "ltp"}]}
    problems = gate_ltp.check_stage_a(leg, modes["both"], modes["drop"], modes["ltp"], with_ltp=True)[1]
    if problems or set(modes["ltp"]["ltp"]) != {"exit_method", "source", "delist_grace_days", "missing_exits",
                                                "ltp_filled", "still_missing", "fills"}:
        yield f"results block refused by gate_ltp.check_stage_a: {problems} {sorted(modes['ltp']['ltp'])}"

    # bound() and close(): the two z-score topics' side
    for name, got, want in (
            ("delisting plus grace", ltp.bound("R", "S", date(2020, 1, 1), EXIT), date(2020, 6, 17)),
            ("second leg", ltp.bound("S", "R", date(2020, 1, 1), EXIT), date(2020, 6, 17)),
            ("no delisting", ltp.bound("A", "B", date(2020, 1, 1), EXIT), EXIT),
            ("delisting 31 days before the anchor", ltp.bound("X", "B", ENTRY, EXIT), EXIT),
            ("delisting 30 days before the anchor", ltp.bound("Y", "B", ENTRY, EXIT), date(2019, 12, 10))):
        if got != want:
            yield f"bound, {name}: {got} != {want}"
    z = mod.PairLtp(con, bounds)
    labels = [z.close("R", "S", date(2020, 6, 1), last, date(2020, 1, 1), EXIT)
              for last in (EXIT, date(2020, 12, 21), date(2020, 12, 20), date(2020, 6, 17))]
    block = z.closes_block("ltp")
    want_close = {"pair": ["R", "S"], "year": 2020, "entry_date": "2020-06-01", "last_common_bar": "2020-06-17",
                  "delisted_date": ["2020-06-10", None], "delisting_bound": ["applied", "none"]}
    if labels != ["year_end", "year_end", "last_bar", "last_bar"]:
        yield f"close labels at 0, 10, 11 days and June: {labels}"
    if (set(block) != {"exit_method", "source", "delist_grace_days", "early_closes", "fills"}
            or block["early_closes"] != 2 or block["fills"][1:] != [want_close] or block["exit_method"] != "ltp"):
        yield f"closes_block: {block}"


def failures(src, first_only=False):
    out = []
    try:
        for f in module_checks(load(src)):
            out.append(f)
            if first_only:
                break
    except Exception as e:  # a mutant that breaks the module fails loudly
        out.append(f"error: {type(e).__name__}: {e}")
    return out


# --- pairs-zscore and pairs-multi-pair, once wired -------------------------------------------------

def spread_con(days, high, a_last=None):
    """PA/PB bars on `days`: PB flat at 100, the log spread +-0.001 except 0.05 on the `high` indexes."""
    rows = []
    for i, d in enumerate(days):
        s = 0.05 if i in high else (0.001 if i % 2 else -0.001)
        rows.append(("PB", d, 100.0))
        if a_last is None or d <= a_last:
            rows.append(("PA", d, 100.0 * math.exp(s)))
    return cached(("spread", days[0], days[-1], tuple(sorted(high)), a_last), lambda: prices(rows))


def span(first, last):
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def simulate(mod, con, ltp=None, use_costs=True):
    trades = mod.simulate_pair_trades(con, "PA", "PB", 1.0, date(2020, 1, 1), EXIT, 2e10, 2e10,
                                      use_costs=use_costs, offset_days=1, ltp=ltp)
    return [(str(t["entry_date"])[:10], str(t["exit_date"])[:10], t["exit_type"]) for t in trades or []], trades or []


def topic_checks(topic, src):
    path = os.path.join(ROOT, topic, "backtest.py")
    mod = load(src, path, topic.replace("-", "_") + "_under_test")
    import pairs_universe
    warm = span(date(2019, 11, 1), EXIT)                  # 61 bars before the trading year
    k = warm.index(date(2020, 1, 10))
    con = spread_con(warm, {k, k + 1, k + 2})
    # the signal of 10 January enters on the 11th; the exit signal of the 13th executes on the 14th
    got, with_costs = simulate(mod, con)
    if got != [("2020-01-11", "2020-01-14", "convergence")]:
        yield f"entry and exit one bar after their signals: {got}"
    _, free = simulate(mod, con, use_costs=False)
    if len(with_costs) == 1 and len(free) == 1:
        cost = free[0]["pair_return"] - with_costs[0]["pair_return"]
        if abs(cost - 0.002) > 2e-6:
            yield f"round trip of two 0.1% legs costs {cost:.6f}, not 0.002"
    # a z-score needs Z_LOOKBACK bars: a spike on the 39th bar gives no signal, one on the 40th does
    year = span(date(2020, 1, 2), EXIT)
    got, _ = simulate(mod, spread_con(year, {38}))
    if got:
        yield f"a z-score on 39 bars traded: {got}"
    got, _ = simulate(mod, spread_con(year, {39, 40, 41}))
    if got != [(year[40].isoformat(), year[43].isoformat(), "convergence")]:
        yield f"a z-score on 40 bars did not trade: {got}"
    # a position open at the pair's last bar: labelled and recorded by PairLtp.close
    for name, high_from, a_last, bounds, want, n_fills in (
            ("a leg stops in June", date(2020, 6, 10), JUN15, {}, ("2020-06-11", "2020-06-15", "last_bar"), 1),
            ("both trade to year end", date(2020, 12, 26), None, {}, ("2020-12-27", "2020-12-31", "year_end"), 0),
            ("a delisted leg's repeat bars", date(2020, 6, 12), date(2020, 7, 10), {"PA": date(2020, 6, 10)},
             ("2020-06-13", "2020-06-17", "last_bar"), 1)):
        ltp = pairs_universe.PairLtp(None, bounds)
        got, _ = simulate(mod, spread_con(warm, {i for i, d in enumerate(warm) if d >= high_from}, a_last), ltp=ltp)
        if got != [want] or len(ltp.fills) != n_fills:
            yield f"{name}: {got} != {[want]}, {len(ltp.fills)} closes recorded"
    # only ltp exits exist: drop and both are refused before any client is built
    calls = []
    mod.CetaResearch = lambda *a, **k: calls.append("client")
    mod.run_single = lambda *a, **k: calls.append(k.get("exit_method"))
    argv = sys.argv
    try:
        for flags, want in ((["--exit-method", "drop"], "refused"), (["--exit-method", "both"], "refused"),
                            (["--exit-method", "ltp"], "ltp"), ([], "ltp")):
            del calls[:]
            sys.argv = ["backtest.py", "--preset", "us"] + flags
            try:
                with contextlib.redirect_stderr(io.StringIO()):   # parser.error prints the usage
                    mod.main()
                got = calls[-1] if calls else None
            except SystemExit as e:
                got = "refused" if e.code not in (0, None) and not calls else f"exit {e.code} after {calls}"
            if got != want:
                yield f"main() with {flags or 'no flag'}: {got} != {want}"
    finally:
        sys.argv = argv


# --- pairs-fundamentals, once wired -----------------------------------------------------------------

def fund_con(n_pairs, stops="A"):
    """n same-sector pairs for trading year 2020, each A leg rich at formation end; the last pair's
    `stops` leg stops on 15 June. A is flat through 2020, B gains GROWTH[k] by the exit bar."""
    con = prices([])
    con.execute("CREATE TABLE candidates(yr INTEGER, symbol VARCHAR, sector VARCHAR, company VARCHAR,"
                " size_usd DOUBLE, cap_usd DOUBLE)")
    rows, y_end, entry = [], date(2019, 12, 31), date(2020, 1, 2)
    for k, growth in enumerate(GROWTH[:n_pairs], 1):
        a, b, a_end, b_end = f"P{k}A", f"P{k}B", None, None
        for i, d in enumerate(span(date(2019, 1, 1), date(2021, 1, 10))):
            if d <= y_end:
                left = (y_end - d).days
                b_end = 100.0 + 10.0 * ((i * 7) % 13) / 13.0
                a_end = b_end * math.exp(0.03 * (15 - left) / 15 if left < 15 else (0.0005 if i % 2 else -0.0005))
                rows += [(a, d, a_end), (b, d, b_end)]
            else:
                trades = k < n_pairs or d <= JUN15
                if trades or stops != "B":
                    rows.append((b, d, b_end * (1.0 + growth * (d - entry).days / 365.0)))
                if trades or stops != "A":
                    rows.append((a, d, a_end))
            if not k - 1:
                rows.append(("SPY", d, 100.0))
        con.executemany("INSERT INTO candidates VALUES (2020, ?, ?, ?, 1000.0, 2e10)",
                        [(a, f"Sector{k}", a), (b, f"Sector{k}", b)])
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)", [(s, d, ep(d), p) for s, d, p in rows])
    return con


GROWTH = (0.10, 0.06, 0.04)
COST = 0.002                                                  # two legs at 0.1%
PRICED = [g / 2 - COST for g in GROWTH[:2]]                   # short the flat A, long B: half of B's gain
AT_LAST_BAR = GROWTH[2] * 165 / 365 / 2 - COST                # 2 January to 15 June is 165 days
BOOKS = {"drop": (sum(PRICED) / 2, 2), "ltp": ((sum(PRICED) + AT_LAST_BAR) / 3, 3)}


def fundamentals_checks(src):
    """Three entered pairs, one without an exit price on either leg: invested in every mode, on the entry-side count."""
    from data_utils import EXIT_METHODS
    import pairs_universe
    mod = load(src, os.path.join(ROOT, "pairs-fundamentals", "backtest.py"), "pairs_fundamentals_under_test")
    mod.START_YEAR = mod.END_YEAR = 2020
    near = lambda x, y: x is not None and abs(x - y) <= 2e-6
    for n_pairs, stops in ((3, "A"), (3, "B"), (2, "A"), (2, "B")):
        con, runs = cached(("fund", n_pairs, stops), lambda: fund_con(n_pairs, stops)), {}
        for mode in EXIT_METHODS:
            ltp = pairs_universe.PairLtp(con, {}) if mode != "drop" else None
            rec = mod.run_backtest(con, use_costs=True, verbose=False, offset_days=1, benchmark_symbol="SPY",
                                   ltp=ltp, exit_method=mode)[0]
            runs[mode] = rec
            if rec.get("pairs_entered") != n_pairs:
                yield f"{n_pairs} pairs, {stops} stops, {mode}: pairs_entered {rec.get('pairs_entered')}"
            if n_pairs == 2:   # a cash year: nothing published, nothing asked of the helper
                if rec["portfolio_return"] != 0.0 or any("ltp" in k for k in rec) or (ltp and (ltp.missing or ltp.fills)):
                    yield f"two entered pairs are a cash year in {mode} mode, {stops} stops: {rec}"
                continue
            want, held = BOOKS["ltp" if mode == "ltp" else "drop"]
            if not near(rec["portfolio_return"], want) or rec.get("pairs_active") != held:
                yield (f"{mode} book, {stops} stops: {rec['portfolio_return']} on {rec.get('pairs_active')} pairs"
                       f" != {want:.6f} on {held}")
            if ltp and ((ltp.missing, ltp.filled) != (1, 1)
                        or [f["missing_leg"] for f in ltp.fills] != [[f"P3{stops}"]]):
                yield (f"{mode}, {stops} stops: one pair without an exit, counted {ltp.missing},"
                       f" filled {ltp.filled}: {ltp.fills}")
        if n_pairs == 2:
            continue
        if any("ltp" in k for k in runs["drop"]):
            yield f"the drop run carries an ltp key: {sorted(runs['drop'])}"
        both = runs["both"]
        if {k: v for k, v in both.items() if "ltp" not in k} != runs["drop"]:
            yield f"the both run's published record is not the drop run's: {both} vs {runs['drop']}"
        if (not near(both.get("portfolio_return_ltp"), BOOKS["ltp"][0])
                or (both.get("pairs_active_ltp"), both.get("ltp_filled")) != (3, 1)):
            yield f"the both run's LTP book: {both}"
    # two lines of one company among the candidates stop the run (check_pairs)
    twin = cached(("fund", "twin"), lambda: fund_con(3))
    twin.execute("UPDATE candidates SET company = 'P1A' WHERE symbol = 'P1B'")
    try:
        mod.run_backtest(twin, use_costs=True, verbose=False, offset_days=1, benchmark_symbol="SPY",
                         ltp=None, exit_method=EXIT_METHODS[0])
        yield "a same-company pair among the candidates did not stop the run"
    except AssertionError:
        pass


def topic_failures(topic, src):
    out = []
    try:
        out += list(fundamentals_checks(src) if topic == FUNDAMENTALS else topic_checks(topic, src))
    except Exception as e:
        out.append(f"error: {type(e).__name__}: {e}")
    return out


FUNDAMENTALS = "pairs-fundamentals"
CASH_RULE = re.compile(r"\bn_entered(\s*(?:>=|<)\s*MIN_PAIRS_ACTIVE)")
COST_CALL = re.compile(r"(?<![\w.])tiered_cost\(")
LEG = " AND adjClose > 0 AND trade_epoch >= ? AND trade_epoch < ? GROUP BY trade_epoch)"
HOLDER = "timedelta(days=LTP_EARLIER_HOLDER_DAYS) >= anchor"
GRACE = "cap = dd + timedelta(days=LTP_DELIST_GRACE_DAYS)"
COUNT = "            self.missing += 1\n"
SKIP = "            if exits.get({}) is not None:\n                continue\n"
FILL_BAR = "after, exit_date, self.delisted, anchors=anchors)"
WINDOW = "if not after < bar <= exit_date:"
MUTANTS = {
    # the last common bar
    "per-leg fill: each leg's own last bar": ("JOIN {leg} y ON y.trade_epoch = x.trade_epoch", "JOIN {leg} y ON TRUE"),
    "first common bar": ("ORDER BY x.trade_epoch DESC, y.trade_epoch DESC LIMIT 1",
                         "ORDER BY x.trade_epoch ASC, y.trade_epoch ASC LIMIT 1"),
    "exit date excluded": ("utc_epoch(hi + timedelta(days=1))", "utc_epoch(hi)"),
    "bars after the exit date used": ("utc_epoch(hi + timedelta(days=1))", "utc_epoch(hi + timedelta(days=30))"),
    "entry bar itself counts": ("utc_epoch(after + timedelta(days=1)), ", "utc_epoch(after), "),
    "zero-price bar counts": (LEG, LEG.replace(" AND adjClose > 0", "")),
    "duplicate bar takes the lower price": ("MAX(adjClose) AS px", "MIN(adjClose) AS px"),
    # the delisting bound
    "no delisting bound": ("return min(on_or_before, cap), ", "return on_or_before, "),
    "grace a day longer": (GRACE, GRACE[:-1] + " + 1)"),
    "grace a day shorter": (GRACE, GRACE[:-1] + " - 1)"),
    "delisting 30 days before entry ignored": (HOLDER, HOLDER.replace(">=", ">")),
    "delisting 31 days before entry bounds": (HOLDER, HOLDER.replace("DAYS)", "DAYS + 1)")),
    "leg's own anchor ignored": ("anchors.get(s, after)", "after"),
    "only the first leg's delisting": ("on_or_before, bounds)[0] for s in (a, b))", "on_or_before, bounds)[0] for s in (a,))"),
    "delisting table not loaded": ("return LtpExits(cr, None).delisted", "return {}"),
    # PairLtp.fill
    "fills requested on a cash year": ("if len(pairs) < min_pairs:", "if False:"),
    "the minimum of entered pairs is cash": ("if len(pairs) < min_pairs:", "if len(pairs) <= min_pairs:"),
    "counted per leg, not per pair": ("self.missing += 1", "self.missing += len(gone)"),
    "a priced pair is filled too": ("            if not gone:\n                continue\n", ""),
    "a None exit taken as a price": ("if exits.get(s) is None]", "if s not in exits]"),
    "a pair is skipped when its first leg has an exit": (COUNT, SKIP.format("a") + COUNT),
    "a pair is skipped when its second leg has an exit": (COUNT, SKIP.format("b") + COUNT),
    "window opens at the first leg's entry bar": ("after = max(anchors.values())", "after = min(anchors.values())"),
    "delisting judged from the later entry bar, not the leg's own": (FILL_BAR, FILL_BAR.replace(", anchors=anchors", "")),
    "bars up to 15 days after the exit date": (FILL_BAR, FILL_BAR.replace("exit_date", "exit_date + timedelta(days=15)")),
    "the bar of the day after the exit date": (FILL_BAR, FILL_BAR.replace("exit_date", "exit_date + timedelta(days=1)")),
    "the exit date's own bar excluded": (FILL_BAR, FILL_BAR.replace("exit_date", "exit_date - timedelta(days=1)")),
    "no window check on a fill": (WINDOW, "if False:"),
    "window check lets the entry bar through": (WINDOW, "if not after <= bar <= exit_date:"),
    "window check refuses the exit date": (WINDOW, "if not after < bar < exit_date:"),
    "window check has no upper bound": (WINDOW, "if not after < bar:"),
    "window check has no lower bound": (WINDOW, "if not bar <= exit_date:"),
    "filled pairs not counted": ("self.filled += 1", "self.filled += 0"),
    "fill record names no delisting": ("[_iso(self.delisted[s]) if s in self.delisted else None for s in legs]",
                                       "[None for s in legs]"),
    # bound() and close()
    "bound ignores the second leg": ("self.delisted)[0] for s in (a, b))", "self.delisted)[0] for s in (a,))"),
    "close: 11 days before year end is a year-end close": ("YEAR_END_DAYS = 10 ", "YEAR_END_DAYS = 11 "),
    "close: 10 days before year end is an early close": ("YEAR_END_DAYS = 10 ", "YEAR_END_DAYS = 9 "),
    "close: every close recorded": (
        '        if (trading_end - last_bar).days <= YEAR_END_DAYS:\n            return "year_end"\n', ""),
}


def main():
    base = failures(SRC)
    if base:
        print("test_pairs_exits: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_pairs_exits: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new), first_only=True):
            survived.append(name)
    if survived:
        print("test_pairs_exits: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    wired, pending = 0, []
    for topic in (FUNDAMENTALS,) + TOPICS:
        with open(os.path.join(ROOT, topic, "backtest.py")) as fh:
            src = fh.read()
        if "pairs_universe" not in src:
            pending.append(topic)
            continue
        wired += 1
        # regex mutants of the topic's own source: the cost of a leg doubled, cash decided on the exit-side count
        mutants = {"each leg's cost charged twice": (COST_CALL, "2 * tiered_cost(")}
        if topic == FUNDAMENTALS:
            mutants["cash from n_active"] = (CASH_RULE, r"n_active\1")
        problems = topic_failures(topic, src)
        for name, (pattern, new) in mutants.items():
            if not problems and not pattern.search(src):
                problems = [f"nothing to mutate for '{name}': the source has no match for {pattern.pattern}"]
            if not problems and not topic_failures(topic, pattern.sub(new, src)):
                problems = [f"mutant survived: {name}"]
        if problems:
            print(f"test_pairs_exits: FAIL ({topic}:", problems, ")")
            sys.exit(1)
    for topic in pending:
        print(f"test_pairs_exits: SKIP {topic} checks (not wired to pairs_universe yet)")
    print(f"test_pairs_exits: PASS ({len(RANGES)} legs, {len(MUTANTS)} mutants caught,"
          f" {wired}/{len(TOPICS) + 1} topics wired)")


if __name__ == "__main__":
    main()
