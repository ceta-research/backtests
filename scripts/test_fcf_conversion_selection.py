#!/usr/bin/env python3
"""Offline check of fcf-conversion's screen: its signal, its row window and the cash-flow and income pair (no network).

screen_stocks takes the latest FY cash-flow, income, key-metrics and ratios rows whose period end is in
(T-410, T-45] days, among the lines in `universe`. The income row must pair with the cash-flow row: the latest
one with a period end within 31 days of the cash-flow row's, either way, in the same reportedCurrency (NULL
matches NULL). The old screen took each table's latest row on its own, so a stale row, a misaligned year or
a currency mix still ranked. A line left without a pair is unranked, and the screen's statement_pair_guard
record counts it (currency_mismatch, no_income_in_window). A paired line ranks on free cash flow over net
income in (1, 3), with free cash flow over revenue, ROE and the operating margin each over 10% and cap_used
over the floor; one line per security, then the top 30 by the ratio.

Also covered: ur-3's Brazil class twins with data checks off and on, each ur-3 line start (before and on its
first own day, check M's events, a freed slot), and the --exclude-sector switch (it removes a line any of
whose profile rows names the sector, after the screen's other tests and before the top-30 cut, and records
it per rebalance; off, the book doesn't change). Every mutant of the topic's source must trip a check, by
changing a book or a record and not by raising.

The live screen (screen.py, today's TTM rows) takes the backtest's universe rules: a line ur-3 alone removes
stays out, and --cloud sends the same SQL text, its regex backslashes intact. Its mutants must trip that check.

Usage:
    python scripts/test_fcf_conversion_selection.py [repo root]
"""
import ast
import contextlib
import io
import itertools
import os
import sys
import types
import warnings
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "fcf-conversion", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)
from data_utils import (DataChecks, UniverseRules, UNIVERSE_RULES_V3, empty_cap_check,   # noqa: E402
                        mark_nvdr_twins)

D = date(2020, 7, 1)
D2 = date(2019, 7, 1)                   # a second screen, for the records' order
FLOOR = 1e9
FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)
NI = 100.0
FS = "Financial Services"
SAME = object()                         # the income rows report in the cash-flow rows' currency
_conv = itertools.count(1)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def T(n, d=D):
    """n days before d (the screen date)."""
    return d - timedelta(days=n)


def load(src):
    mod = types.ModuleType("fcf_conversion_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def co(cf=(FY19,), inc=None, km=None, fr=None, conv=None, ni=NI, rev=None, ccy="USD", inc_ccy=SAME, extra_inc=(),
       roe=0.2, opm=0.2, mcap=5e9, px=5.0, isin=None, universe=True, rule=None, cap_div=None):
    """One company: a cash-flow row on each `cf` day in `ccy`, an income row on each `inc` day (default the cf
    days) in `inc_ccy`, key-metrics and ratios rows on each `km` and `fr` day (default the cf days).

    conv is free cash flow over net income (default a distinct 1.5 + n/1000, so a book ranks on it alone);
    rev defaults to 5 x |ni|, a margin of conv/5. extra_inc adds income rows (day, netIncome, revenue,
    currency). rule and cap_div write check M's cap_check row on each key-metrics day.
    """
    conv = 1.5 + next(_conv) / 1000 if conv is None else conv
    rev = 5 * abs(ni) if rev is None else rev
    ic = ccy if inc_ccy is SAME else inc_ccy
    days = tuple(cf)
    return {"cf": [(d, conv * ni, ccy) for d in days],
            "inc": [(d, ni, rev, ic) for d in (days if inc is None else inc)] + list(extra_inc),
            "km": tuple(days if km is None else km), "fr": tuple(days if fr is None else fr),
            "roe": roe, "opm": opm, "mcap": mcap, "px": px, "isin": isin, "universe": universe, "rule": rule,
            "cap_div": cap_div, "conv": conv, "used": mcap / cap_div if cap_div else mcap}


ON_FY19 = {"km": (FY19,), "fr": (FY19,)}    # key metrics and ratios inside the window, whatever the other rows
COMPANIES = {
    "BASE": co(),
    # free cash flow over net income in (1, 3), open at both ends
    "CONV_1": co(conv=1.0), "CONV_1_IN": co(conv=1.001), "CONV_3": co(conv=3.0), "CONV_3_IN": co(conv=2.999),
    # every sign negative: the ratios pass, the sign tests don't
    "NEG": co(ni=-NI, rev=-5 * NI),
    # free cash flow over revenue, ROE and the operating margin: over 10%, open
    "MARGIN": co(conv=2.0, rev=2000.0), "MARGIN_IN": co(conv=2.0, rev=1999.0),
    "ROE": co(roe=0.10), "ROE_IN": co(roe=0.1001), "OPM": co(opm=0.10), "OPM_IN": co(opm=0.1001),
    # the floor reads cap_used (check M), open, and the book carries cap_used
    "SMALL": co(mcap=FLOOR), "SMALL_IN": co(mcap=FLOOR * 1.001),
    "CAP_DIV": co(rule="cap_over_k", cap_div=10.0), "CAP_DIV_IN": co(rule="cap_over_k", cap_div=2.0),
    "INELIG": co(rule="ineligible"),
    # the row window (T-410, T-45], one statement at a time, the others where they pair
    "CF_409": co(cf=(T(409),), inc=(T(404),), **ON_FY19), "CF_410": co(cf=(T(410),), inc=(T(404),), **ON_FY19),
    "CF_45": co(cf=(T(45),), inc=(T(50),), **ON_FY19), "CF_44": co(cf=(T(44),), inc=(T(50),), **ON_FY19),
    "INC_409": co(cf=(T(404),), inc=(T(409),), **ON_FY19), "INC_410": co(cf=(T(404),), inc=(T(410),), **ON_FY19),
    "INC_45": co(cf=(T(50),), inc=(T(45),), **ON_FY19), "INC_44": co(cf=(T(50),), inc=(T(44),), **ON_FY19),
    "KM_409": co(km=(T(409),)), "KM_410": co(km=(T(410),)), "KM_45": co(km=(T(45),)), "KM_44": co(km=(T(44),)),
    "FR_409": co(fr=(T(409),)), "FR_410": co(fr=(T(410),)), "FR_45": co(fr=(T(45),)), "FR_44": co(fr=(T(44),)),
    # the pair: period ends within 31 days either way, inclusive
    "PAIR_P31": co(inc=(FY19 + timedelta(days=31),)), "PAIR_M31": co(inc=(FY19 - timedelta(days=31),)),
    "PAIR_P32": co(inc=(FY19 + timedelta(days=32),)), "PAIR_M32": co(inc=(FY19 - timedelta(days=32),)),
    # the pair: the same reportedCurrency, NULL matching NULL (USD_AUD is BXB.AX's 2011 shape)
    "NULL_NULL": co(ccy=None), "NULL_AUD": co(ccy=None, inc_ccy="AUD"), "AUD_NULL": co(ccy="AUD", inc_ccy=None),
    "USD_AUD": co(inc_ccy="AUD"), "AUD_AUD": co(ccy="AUD"),
    "FAR_AUD": co(inc=(FY19 + timedelta(days=32),), inc_ccy="AUD"),   # another currency outside the 31 days
    # the same period end also holds a USD row with ten times the income, first on the income tie-break
    "TWO_CCY": co(ccy="AUD", extra_inc=[(FY19, 10 * NI, 50 * NI, "USD")]),
    # two income rows within the 31 days: the later one pairs
    "TWO_INC": co(extra_inc=[(FY19 - timedelta(days=20), 10 * NI, 50 * NI, "USD")]),
    # unpaired lines the guard doesn't count, since another test fails too
    "NEG_UNPAIRED": co(ni=-NI, rev=-5 * NI, inc_ccy="AUD"), "LOW_ROE_UNPAIRED": co(roe=0.05, inc_ccy="AUD"),
    "LOW_OPM_UNPAIRED": co(opm=0.05, inc_ccy="AUD"), "SMALL_UNPAIRED": co(mcap=1e6, inc_ccy="AUD"),
    "OUTSIDE_UNPAIRED": co(universe=False, inc_ccy="AUD"),
    # the universe rules removed it: never ranked
    "OUTSIDE": co(universe=False),
    # same ISIN: the line with an entry price is kept
    "TWIN_A": co(px=None, isin="US0000000001"), "TWIN_B": co(isin="US0000000001"),
    # an equal signal ranks by symbol
    "TIE_A": co(conv=1.75), "TIE_B": co(conv=1.75),
    # FY2018 rows: only the D2 screen reads them, and finds no income row in the cash-flow row's currency
    "D2_ONLY": co(cf=(FY18,), inc_ccy="AUD"),
}
NO_INCOME = {"INC_410", "INC_44", "PAIR_P32", "PAIR_M32", "FAR_AUD"}
MISMATCH = {"NULL_AUD", "AUD_NULL", "USD_AUD"}
OUT = NO_INCOME | MISMATCH | {
    "CONV_1", "CONV_3", "NEG", "MARGIN", "ROE", "OPM", "SMALL", "CAP_DIV", "INELIG", "CF_410", "CF_44", "KM_410",
    "KM_44", "FR_410", "FR_44", "NEG_UNPAIRED", "LOW_ROE_UNPAIRED", "LOW_OPM_UNPAIRED", "SMALL_UNPAIRED",
    "OUTSIDE_UNPAIRED", "OUTSIDE", "TWIN_A", "D2_ONLY"}
REASONS = ("currency_mismatch", "no_income_in_window")


def ranked(companies, out, n=30):
    """The expected book: (symbol, cap_used) by the ratio descending, then symbol, top n."""
    keep = sorted((s for s in companies if s not in out), key=lambda s: (-companies[s]["conv"], s))
    return [(s, companies[s]["used"]) for s in keep[:n]]


EXPECT = ranked(COMPANIES, OUT)
GUARD_D = {"rebalance_date": D.isoformat(), "currency_mismatch": len(MISMATCH), "no_income_in_window": len(NO_INCOME)}
GUARD_D2 = {"rebalance_date": D2.isoformat(), "currency_mismatch": 1, "no_income_in_window": 0}
CAP_EVENTS = [("CAP_DIV", "cap_over_k"), ("INELIG", "ineligible")]

# More than 30 qualify: the unpaired line would rank first if paired, so its slot goes to the 31st filler.
FILLERS = {f"F{i:02d}": co(conv=1.1 + 0.01 * i) for i in range(31)}
CROWD = {**FILLERS, "MIX": co(conv=2.9, inc_ccy="AUD")}
CROWD_EXPECT = ranked(CROWD, {"MIX"})

# ur-3's Brazil class twins: one slot per company, the ordinary line when priced, else a priced sibling.
BRAZIL = {
    "BRAA3.SA": co(isin="BRBRAAACNOR1"), "BRAA4.SA": co(isin="BRBRAAACNPR2"),     # ordinary priced
    "BRBB3.SA": co(px=None), "BRBB4.SA": co(),                                    # a stand-in
    "BRCC3.SA": co(px=None), "BRCC4.SA": co(), "BRCC11.SA": co(),                 # two siblings
    "BREE3.SA": co(px=None), "BREE4.SA": co(px=None),                             # neither priced
    "BRFF3.SA": co(), "BRFF11.SA": co(),       # a priced unit sorts before its priced ordinary line
}
BRAZIL_BOOK = {"BRAA3.SA", "BRBB4.SA", "BRCC11.SA", "BREE3.SA", "BRFF3.SA"}
BRAZIL_STAND_INS = [{"rebalance_date": D.isoformat(), "symbols": ["BRBB4.SA", "BRCC11.SA"]}]

# ur-3's line starts: a listed line's rows and bars before its first own day copy another line's history.
LINE_STARTS = [(s, date.fromisoformat(d)) for s, d, _ in UNIVERSE_RULES_V3["line_starts"]]

# The sector switch. FIN2 has two profile rows and only one names the sector; FIN_LOW fails ROE and
# FIN_UNPAIRED the pair, so neither passed the screen; FIN_OUT is outside the universe.
FIN = {"FIN1": co(conv=2.5), "FIN2": co(conv=2.4), "IND1": co(), "IND2": co(), "FIN_LOW": co(roe=0.05),
       "FIN_UNPAIRED": co(inc_ccy="AUD"), "FIN_OUT": co(universe=False)}
FIN_PROFILE = [("FIN1", FS), ("FIN2", FS), ("FIN2", "Industrials"), ("IND1", "Industrials"), ("IND2", None),
               ("FIN_LOW", FS), ("FIN_UNPAIRED", FS), ("FIN_OUT", FS)]
FIN_UNRANKED = {"FIN_LOW", "FIN_UNPAIRED", "FIN_OUT"}
FIN_CROWD = {**FILLERS, "FINX": co(conv=2.9)}


def db(companies, epoch_type="UINTEGER", on=(D,)):
    """The caches screen_stocks reads. The warehouse's dateEpoch is unsigned 32-bit."""
    con = duckdb.connect()
    e = f"filing_epoch {epoch_type}, period VARCHAR"
    con.execute(f"CREATE TABLE cashflow_cache(symbol VARCHAR, freeCashFlow DOUBLE, {e}, reportedCurrency VARCHAR)")
    con.execute(f"CREATE TABLE income_cache(symbol VARCHAR, netIncome DOUBLE, revenue DOUBLE, {e},"
                " reportedCurrency VARCHAR)")
    con.execute(f"CREATE TABLE metrics_cache(symbol VARCHAR, returnOnEquity DOUBLE, marketCap DOUBLE, {e})")
    con.execute(f"CREATE TABLE ratios_cache(symbol VARCHAR, operatingProfitMargin DOUBLE, {e})")
    con.execute("CREATE TABLE universe(symbol VARCHAR)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    empty_cap_check(con)
    for s, c in companies.items():
        for d, fcf, ccy in c["cf"]:
            con.execute("INSERT INTO cashflow_cache VALUES (?, ?, ?, 'FY', ?)", [s, fcf, ep(d), ccy])
        for d, ni, rev, ccy in c["inc"]:
            con.execute("INSERT INTO income_cache VALUES (?, ?, ?, ?, 'FY', ?)", [s, ni, rev, ep(d), ccy])
        for d in c["km"]:
            con.execute("INSERT INTO metrics_cache VALUES (?, ?, ?, ?, 'FY')", [s, c["roe"], c["mcap"], ep(d)])
            if c["rule"]:
                con.execute("INSERT INTO cap_check (symbol, filing_epoch, rule, cap_div) VALUES (?, ?, ?, ?)",
                            [s, ep(d), c["rule"], c["cap_div"]])
        for d in c["fr"]:
            con.execute("INSERT INTO ratios_cache VALUES (?, ?, ?, 'FY')", [s, c["opm"], ep(d)])
        if c["universe"]:
            con.execute("INSERT INTO universe VALUES (?)", [s])
        for d in on if c["px"] else ():
            con.execute("INSERT INTO prices_cache VALUES (?, ?, ?, ?, 1e6)", [s, ep(d + timedelta(days=1)), c["px"], c["px"]])
        con.execute("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", [s, c["isin"]])
    mark_nvdr_twins(con)     # the twin key's columns, as a data-checks run has them
    return con


def diff(label, got, want):
    if got == want:
        return []
    names, wnames = [r[0] for r in got], [r[0] for r in want]
    if set(names) != set(wnames):
        return [f"{label}: missing {sorted(set(wnames) - set(names))}, extra {sorted(set(names) - set(wnames))}"]
    return [f"{label}: {got} != {want}"]


def screen(mod, con, d, **kw):
    return [(r[0], r[1]) for r in mod.screen_stocks(con, d, FLOOR, offset_days=1, **kw)]


def guard_failures(mod):
    """The screen with data checks on and the guard recording: the same book, check M's events, and the
    record's keys and counts per date, sorted by date and equal whichever date is screened first; then a
    crowd where the unpaired line's slot goes to the 31st name."""
    out = []
    con, g, dc = db(COMPANIES), mod.StatementPairGuard(), DataChecks("on", mod.DATA_CHECKS)
    out += diff("book (data checks on)", screen(mod, con, D, dc=dc, guard=g), EXPECT)
    caps = sorted((e["symbol"], e["rule"]) for e in dc.events if e["check"] == "cap")
    if caps != CAP_EVENTS:
        out.append(f"check M events {caps}, want {CAP_EVENTS}")
    screen(mod, con, D2, guard=g)
    b = g.block()
    if list(b) != ["rule", "dropped", "by_reason", "by_period"] or list(b.get("by_reason", {})) != list(REASONS):
        out.append(f"statement_pair_guard keys {list(b)} {list(b.get('by_reason', {}))}")
    per = b.get("by_period", [])
    if per != [GUARD_D2, GUARD_D]:
        out.append(f"statement_pair_guard by_period {per}, want {[GUARD_D2, GUARD_D]}")
    sums = {k: sum(p.get(k, 0) for p in per) for k in REASONS}
    if b.get("by_reason") != sums or b.get("dropped") != sum(sums.values()):
        out.append(f"statement_pair_guard totals {b.get('dropped')} {b.get('by_reason')}, by_period sums {sums}")
    g2 = mod.StatementPairGuard()
    for d in (D2, D):
        screen(mod, con, d, guard=g2)
    if g2.block() != b:
        out.append("statement_pair_guard depends on the order the dates are screened")
    con, g = db(CROWD), mod.StatementPairGuard()
    out += diff("crowd book", screen(mod, con, D, guard=g), CROWD_EXPECT)
    if g.block()["by_period"] != [{"rebalance_date": D.isoformat(), "currency_mismatch": 1, "no_income_in_window": 0}]:
        out.append(f"crowd statement_pair_guard {g.block()['by_period']}")
    return out


def brazil_failures(mod):
    """Each SAO company holds one line, with data checks on and off: the ordinary line when it has an entry
    price; else its first priced sibling, which the leg's UniverseRules records as a stand-in; the ordinary
    line again when no line is priced."""
    out = []
    for label, dc in (("data checks off", None), ("data checks on", DataChecks("on", mod.DATA_CHECKS))):
        con, ur = db(BRAZIL), UniverseRules(mod.UNIVERSE_RULES)
        ur.mark_twins(con)
        got = {s for s, _ in screen(mod, con, D, dc=dc, ur=ur)}
        if got != BRAZIL_BOOK:
            out.append(f"Brazil book ({label}): {sorted(got)}, want {sorted(BRAZIL_BOOK)}")
        stand = ur.block()["class_stand_ins"]["by_period"]
        if stand != BRAZIL_STAND_INS:
            out.append(f"Brazil stand-ins ({label}): {stand}, want {BRAZIL_STAND_INS}")
    return out


def leg_rules(mod, companies):
    """The leg's rules (the topic's own UNIVERSE_RULES) after its profile rows: every fixture line kept."""
    ur = UniverseRules(mod.UNIVERSE_RULES)
    ur.sector_rows([{"symbol": s, "exchange": "X", "cik": None, "companyName": s, "non_ordinary": None}
                    for s in companies])
    return ur


def line_start_failures(mod):
    """Each ur-3 line start: a screen the day before its first own day leaves the line out, with data checks
    on and off, and check M reads no event for it; a screen on that day ranks it; the block lists it. A
    line left out frees its slot."""
    out = []
    if UniverseRules(mod.UNIVERSE_RULES).block()["version"] != "ur-3":
        out.append("the topic's universe rules are not ur-3")
    for sym, first in LINE_STARTS:
        rows = {sym: co(cf=(T(100, first),))}
        for label, dc in (("data checks off", None), ("data checks on", DataChecks("on", mod.DATA_CHECKS))):
            ur = leg_rules(mod, rows)
            got = {s for s, _ in screen(mod, db(rows), first - timedelta(days=1), dc=dc, ur=ur)}
            if got:
                out.append(f"{sym} the day before {first} ({label}): {sorted(got)}, want none")
            got = {s for s, _ in screen(mod, db(rows), first, dc=dc, ur=ur)}
            if got != {sym}:
                out.append(f"{sym} on {first} ({label}): {sorted(got)}, want ['{sym}']")
        lines = [(r["symbol"], r["first_day"]) for r in ur.block().get("line_starts", {}).get("lines", [])]
        if lines != [(sym, first.isoformat())]:
            out.append(f"{sym}: line_starts block {lines}")
        inelig = {sym: co(cf=(T(100, first),), rule="ineligible")}
        dc = DataChecks("on", mod.DATA_CHECKS)
        screen(mod, db(inelig), first - timedelta(days=1), dc=dc, ur=leg_rules(mod, inelig))
        caps = [e["symbol"] for e in dc.events if e["check"] == "cap"]
        if caps:
            out.append(f"check M events for {sym} before its first own day: {caps}")
    sym = LINE_STARTS[0][0]     # 30 qualify besides it, and it would rank first: its slot goes to the 30th filler
    crowd = {**{f"F{i:02d}": co(conv=1.1 + 0.01 * i) for i in range(30)}, sym: co(conv=2.9)}
    got = {s for s, _ in screen(mod, db(crowd), D, ur=leg_rules(mod, crowd))}
    if got != set(crowd) - {sym}:
        out.append(f"the left-out line's slot: missing {sorted(set(crowd) - {sym} - got)}, extra {sorted(got - set(crowd))}")
    if mod._started("SELECT 1", []) != "SELECT 1":
        out.append("the candidates SQL changes when no line is left out")
    return out


def sector_failures(mod):
    """--exclude-sector: off, the book is the screen's; on, a line any of whose profile rows names the
    sector leaves before the top-30 cut (its slot goes to the next name) and the switch lists it per
    rebalance, among the lines that passed the screen only; a sector no line names changes nothing."""
    out = []
    con = db(FIN)
    want_all = ranked(FIN, FIN_UNRANKED)
    out += diff("financials, switch off", screen(mod, con, D), want_all)
    profile = [{"symbol": s, "sector": x} for s, x in FIN_PROFILE]
    sw = mod.SectorExclusion([FS])
    sw.load(con, profile)
    out += diff("financials, switch on", screen(mod, con, D, sectors=sw), ranked(FIN, FIN_UNRANKED | {"FIN1", "FIN2"}))
    out += diff("financials, switch on, data checks on",
                screen(mod, con, D, sectors=sw, dc=DataChecks("on", mod.DATA_CHECKS)),
                ranked(FIN, FIN_UNRANKED | {"FIN1", "FIN2"}))
    screen(mod, con, D2, sectors=sw)
    want = {"sectors": [FS], "field": "profile.sector",
            "removed": {D2.isoformat(): [], D.isoformat(): ["FIN1", "FIN2"]}}
    b = sw.block()
    if b != want or list(b) != list(want) or list(b["removed"]) != list(want["removed"]):
        out.append(f"sector_exclusion block {b}, want {want}")
    if mod.SectorExclusion(FS).sectors != [FS]:
        out.append(f"one sector name passed as a string reads {mod.SectorExclusion(FS).sectors}")
    other = mod.SectorExclusion(["Utilities"])
    other.load(con, profile)
    out += diff("financials, a sector no line names", screen(mod, con, D, sectors=other), want_all)
    if other.block()["removed"] != {D.isoformat(): []}:
        out.append(f"a sector no line names: removed {other.block()['removed']}")
    con, sw = db(FIN_CROWD), mod.SectorExclusion([FS])
    sw.load(con, [{"symbol": "FINX", "sector": FS}])
    out += diff("financials crowd", screen(mod, con, D, sectors=sw), ranked(FIN_CROWD, {"FINX"}))
    if sw.block()["removed"] != {D.isoformat(): ["FINX"]}:
        out.append(f"financials crowd: removed {sw.block()['removed']}")
    return out


def failures(src, epoch_types=("UINTEGER",)):
    out = []
    try:
        mod = load(src)
        for t in epoch_types:   # the book first: it reads only what any version of the screen takes
            out += diff(f"book ({t})", screen(mod, db(COMPANIES, t), D), EXPECT)
        return (out + guard_failures(mod) + brazil_failures(mod) + line_start_failures(mod)
                + sector_failures(mod))
    except Exception as e:  # reported as an error: main() does not count it as a caught mutant
        return out + [f"error: {type(e).__name__}: {e}"]


CF_WHERE = "FROM cashflow_cache\n            WHERE filing_epoch <= ? AND filing_epoch > ?\n"
INC_WHERE = "WHERE i.filing_epoch <= ? AND i.filing_epoch > ?\n"
KM_WHERE = "WHERE m.filing_epoch <= ? AND m.filing_epoch > ?\n"
FR_WHERE = "FROM ratios_cache\n            WHERE filing_epoch <= ? AND filing_epoch > ?\n"
GAP_WHERE = "AND i.filing_epoch >= c.cf_epoch - ? AND i.filing_epoch <= c.cf_epoch + ?\n"
CCY_WHERE = "WHERE reportedCurrency IS NOT DISTINCT FROM cf_ccy\n"
INC_ORDER = "ORDER BY filing_epoch DESC, netIncome DESC NULLS LAST,"
BAND = "{conv} > ? AND {conv} < ?"
SIGNS = ("          AND cf.freeCashFlow > 0\n", "inc.netIncome > 0 AND inc.revenue > 0\n               AND ")
ROE_OUTER = "inc.revenue > ?))\n          AND km.returnOnEquity > ?\n"
SEMI = "          AND cf.symbol IN (SELECT symbol FROM universe)\n"
LAG = "cutoff_epoch = utc_epoch(target_date - timedelta(days=FILING_LAG_DAYS))"
CANDIDATES = 'candidates = _started(_candidates_sql("km.cap_used > ?"), late)'
PICKED = "picked = [(r[0], r[1]) for r in rows]"
TWIN_KEY = "twin_key=(dc is not None and dc.on) or bool(UNIVERSE_RULES.get(\"class_twins\")))"
DEDUP = ("FROM ({isin_dedup_sql(pool, target_date, offset_days, MIN_ENTRY_PRICE,\n"
         "                              " + TWIN_KEY + "})")
POOL = "pool = sectors.keep(paired) if sectors is not None else paired"
RECORD = "sectors.record(con, paired, params, target_date)"
ANY_ROW = 'self.symbols = {r["symbol"] for r in rows if r.get("sector") in self.sectors}'
# name: [(old, new), ...]; every `old` occurs once and only once in the source
MUTANTS = {
    # the signal
    "band closed at the bottom": [(BAND, "{conv} >= ? AND {conv} < ?")],
    "band closed at the top": [(BAND, "{conv} > ? AND {conv} <= ?")],
    "band from 1.01": [("FCF_CONVERSION_MIN = 1.0 ", "FCF_CONVERSION_MIN = 1.01 ")],
    "band to 2.99": [("FCF_CONVERSION_MAX = 3.0 ", "FCF_CONVERSION_MAX = 2.99 ")],
    "no sign tests": [(SIGNS[0], ""), (SIGNS[1], "")],
    "margin closed": [("cf.freeCashFlow / inc.revenue > ?", "cf.freeCashFlow / inc.revenue >= ?")],
    "ROE closed": [("AND km.returnOnEquity > ?", "AND km.returnOnEquity >= ?")],
    "operating margin closed": [("AND fr.operatingProfitMargin > ?", "AND fr.operatingProfitMargin >= ?")],
    # the floor
    "no size floor": [('sql("km.cap_used > ?")', 'sql("? IS NOT NULL")')],
    "the floor on the raw cap": [('sql("km.cap_used > ?")', 'sql("km.marketCap > ?")')],
    "the floor closed": [('sql("km.cap_used > ?")', 'sql("km.cap_used >= ?")')],
    "the book carries the raw cap": [("SELECT cf.symbol, km.cap_used AS marketCap",
                                      "SELECT cf.symbol, km.marketCap AS marketCap")],
    # the row window
    "no lag": [(LAG, "cutoff_epoch = utc_epoch(target_date)")],
    "lag 44 days": [("FILING_LAG_DAYS = 45 ", "FILING_LAG_DAYS = 44 ")],
    "lag 46 days": [("FILING_LAG_DAYS = 45 ", "FILING_LAG_DAYS = 46 ")],
    "rows to 411 days": [("CURRENT_ROW_MAX_DAYS = 410 ", "CURRENT_ROW_MAX_DAYS = 411 ")],
    "rows to 409 days": [("CURRENT_ROW_MAX_DAYS = 410 ", "CURRENT_ROW_MAX_DAYS = 409 ")],
    "cash-flow bound closed": [(CF_WHERE, CF_WHERE.replace("filing_epoch > ?", "filing_epoch >= ?"))],
    "no age bound on cash flow": [(CF_WHERE, CF_WHERE.replace("AND filing_epoch > ?", "AND ? IS NOT NULL"))],
    "cash flow read in the lag": [(CF_WHERE, CF_WHERE.replace("filing_epoch <= ? AND", "? IS NOT NULL AND"))],
    "income bound closed": [(INC_WHERE, INC_WHERE.replace("i.filing_epoch > ?", "i.filing_epoch >= ?"))],
    "no age bound on income": [(INC_WHERE, "WHERE i.filing_epoch <= ? AND ? IS NOT NULL\n")],
    "income read in the lag": [(INC_WHERE, "WHERE ? IS NOT NULL AND i.filing_epoch > ?\n")],
    "key-metrics bound closed": [(KM_WHERE, KM_WHERE.replace("m.filing_epoch > ?", "m.filing_epoch >= ?"))],
    "no age bound on key metrics": [(KM_WHERE, "WHERE m.filing_epoch <= ? AND ? IS NOT NULL\n")],
    "key metrics read in the lag": [(KM_WHERE, "WHERE ? IS NOT NULL AND m.filing_epoch > ?\n")],
    "ratios bound closed": [(FR_WHERE, FR_WHERE.replace("filing_epoch > ?", "filing_epoch >= ?"))],
    "no age bound on ratios": [(FR_WHERE, FR_WHERE.replace("AND filing_epoch > ?", "AND ? IS NOT NULL"))],
    "ratios read in the lag": [(FR_WHERE, FR_WHERE.replace("filing_epoch <= ? AND", "? IS NOT NULL AND"))],
    # the pair
    "no pair window (the old screen)": [(GAP_WHERE, "AND ? IS NOT NULL AND ? IS NOT NULL\n")],
    "no lower pair bound": [(GAP_WHERE, "AND ? IS NOT NULL AND i.filing_epoch <= c.cf_epoch + ?\n")],
    "no upper pair bound": [(GAP_WHERE, "AND i.filing_epoch >= c.cf_epoch - ? AND ? IS NOT NULL\n")],
    "pair gap 30 days": [("PAIR_MAX_GAP_DAYS = 31 ", "PAIR_MAX_GAP_DAYS = 30 ")],
    "pair gap 32 days": [("PAIR_MAX_GAP_DAYS = 31 ", "PAIR_MAX_GAP_DAYS = 32 ")],
    "no currency test": [(CCY_WHERE, "WHERE TRUE\n")],
    "currency test with =": [(CCY_WHERE, "WHERE reportedCurrency = cf_ccy\n")],
    "earliest income row": [(INC_ORDER, INC_ORDER.replace("filing_epoch DESC", "filing_epoch ASC"))],
    "a mismatch recorded as no income": [("WHEN iw.symbol IS NOT NULL THEN 'currency_mismatch'",
                                          "WHEN FALSE THEN 'currency_mismatch'")],
    # the guard's record
    "guard not recorded": [("guard.record(con.execute(", "(lambda *a: None)(con.execute(")],
    "the guard counts negative free cash flow": [(SIGNS[0], ""), ("inc.netIncome > 0 AND inc.revenue > 0",
                                                                  "inc.netIncome > 0 AND inc.revenue > 0 AND cf.freeCashFlow > 0")],
    "the guard counts low ROE": [(ROE_OUTER, "inc.revenue > ? AND km.returnOnEquity > ?))\n")],
    # the universe, check M and the dedup
    "no universe semi-join": [(SEMI, "")],
    "check M events not recorded": [("dc.caps(con, ", "(lambda *a: None)(con, ")],
    "same-ISIN dedup removed": [(DEDUP, "FROM ({pool})")],
    "ranking reversed": [("ORDER BY fcf_conversion DESC, symbol", "ORDER BY fcf_conversion ASC, symbol")],
    "tie-break reversed": [("ORDER BY fcf_conversion DESC, symbol", "ORDER BY fcf_conversion DESC, symbol DESC")],
    # ur-3: Brazil's class twins and the line starts
    "ur-2 in place of ur-3": [("UNIVERSE_RULES = UNIVERSE_RULES_V3",
                               "UNIVERSE_RULES = __import__('data_utils').UNIVERSE_RULES_V2")],
    "class twins not keyed in the screen": [(TWIN_KEY, "twin_key=dc is not None and dc.on)")],
    "stand-ins not recorded": [("ur.stand_ins([s for s, _ in picked], target_date)", "pass")],
    "no line starts": [("late = ur.not_started(target_date) if ur is not None else []", "late = []")],
    "a line left out after the top-30 cut": [
        (CANDIDATES, 'candidates = _candidates_sql("km.cap_used > ?")'),
        (PICKED, "picked = [(r[0], r[1]) for r in rows if r[0] not in late]")],
    "check M reads a line before its first own day": [
        ("_paired(_started(_candidates_sql(CAP_FAIL_TEST, events=True), late))",
         "_paired(_candidates_sql(CAP_FAIL_TEST, events=True))")],
    # the sector switch
    "the switch keeps every line": [("WHERE _x.symbol NOT IN (SELECT symbol FROM sector_out)", "WHERE TRUE")],
    "the switch after the top-30 cut": [
        (POOL, "pool = paired"),
        (PICKED, "picked = [(r[0], r[1]) for r in rows if sectors is None or r[0] not in sectors.symbols]")],
    "removed not recorded": [(RECORD, "pass")],
    "removed read before the pair test": [(RECORD, "sectors.record(con, candidates, params, target_date)")],
    "one sector name read as its characters": [
        ("sectors = [sectors] if isinstance(sectors, str) else sectors", "sectors = sectors")],
    "every profile row must name the sector": [
        (ANY_ROW, ANY_ROW[:-1] + ' and all(x.get("sector") in self.sectors for x in rows if x["symbol"] == r["symbol"])}')],
}


# ---------------------------------------------------------------- the live screen (screen.py)

SCREEN_PATH = os.path.join(ROOT, "fcf-conversion", "screen.py")
with open(SCREEN_PATH) as fh:
    SCREEN_SRC = fh.read()
# (exchange, ISIN, fund flag, FCF over net income, market cap): TTM rows that pass the signal. 6288.HK is a
# depositary receipt on ur-3's hand list only, FUND.HK a fund-flagged line, OTHER.L on another exchange and
# SMALL.HK under HKSE's floor.
SCREEN_LINES = {"ORD1.HK": ("HKSE", "HK0000000011", False, 1.5, 5e9),
                "ORD2.HK": ("HKSE", "HK0000000012", False, 1.4, 5e9),
                "6288.HK": ("HKSE", "US0000000013", False, 2.0, 5e9),
                "FUND.HK": ("HKSE", "HK0000000014", True, 1.8, 5e9),
                "OTHER.L": ("LSE", "GB0000000015", False, 1.6, 5e9),
                "SMALL.HK": ("HKSE", "HK0000000016", False, 1.7, 1e8)}
SCREEN_BOOK = ["ORD1.HK", "ORD2.HK"]


class ScreenClient:
    """Stands in for CetaResearch: query runs the SQL on the fixture, execute_code records the code."""

    def __init__(self, con):
        self.con, self.sqls, self.codes = con, [], []

    def query(self, sql, **_kw):
        self.sqls.append(sql)
        cur = self.con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def execute_code(self, code, **_kw):
        self.codes.append(code)
        return {"stdout": ""}


def screen_db():
    con = duckdb.connect()
    con.execute("CREATE TABLE profile(symbol VARCHAR, companyName VARCHAR, exchange VARCHAR, isin VARCHAR,"
                " isFund BOOLEAN, isEtf BOOLEAN, isAdr BOOLEAN)")
    con.execute("CREATE TABLE cash_flow_statement_ttm(symbol VARCHAR, freeCashFlow DOUBLE)")
    con.execute("CREATE TABLE income_statement_ttm(symbol VARCHAR, netIncome DOUBLE, revenue DOUBLE)")
    con.execute("CREATE TABLE key_metrics_ttm(symbol VARCHAR, returnOnEquityTTM DOUBLE, marketCap DOUBLE)")
    con.execute("CREATE TABLE financial_ratios_ttm(symbol VARCHAR, operatingProfitMarginTTM DOUBLE)")
    for s, (ex, isin, fund, conv, cap) in SCREEN_LINES.items():
        con.execute("INSERT INTO profile VALUES (?, ?, ?, ?, ?, FALSE, FALSE)", [s, f"{s} Holdings Ltd", ex, isin, fund])
        con.execute("INSERT INTO cash_flow_statement_ttm VALUES (?, ?)", [s, conv * NI])
        con.execute("INSERT INTO income_statement_ttm VALUES (?, ?, ?)", [s, NI, 5 * NI])
        con.execute("INSERT INTO key_metrics_ttm VALUES (?, 0.2, ?)", [s, cap])
        con.execute("INSERT INTO financial_ratios_ttm VALUES (?, 0.2)", [s])
    return con


def load_screen(src):
    """screen.py from src, binding `backtest` to fcf-conversion's (the sibling a run from the root imports)."""
    saved = sys.modules.get("backtest")
    sys.modules["backtest"] = load(SRC)
    try:
        mod = types.ModuleType("fcf_conversion_screen")
        mod.__file__ = SCREEN_PATH
        exec(compile(src, SCREEN_PATH, "exec"), mod.__dict__)
        return mod
    finally:
        if saved is None:
            sys.modules.pop("backtest", None)
        else:
            sys.modules["backtest"] = saved


def run_screen_main(mod, client, args):
    argv, sys.argv = sys.argv, ["screen.py"] + args
    mod.CetaResearch = lambda **_kw: client
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            mod.main()
    finally:
        sys.argv = argv


def screen_failures(src):
    """main() on HKSE, then with --cloud: the book is the ordinary lines, and the cloud code queries the same SQL."""
    try:
        mod = load_screen(src)
        con = screen_db()
        client = ScreenClient(con)
        run_screen_main(mod, client, ["--exchange", "HKSE"])
        run_screen_main(mod, client, ["--exchange", "HKSE", "--cloud"])
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    if len(client.sqls) != 1 or len(client.codes) != 1:
        return [f"screen: {len(client.sqls)} queries and {len(client.codes)} cloud runs, not one of each"]
    book = [r[0] for r in con.execute(client.sqls[0]).fetchall()]
    out = [] if book == SCREEN_BOOK else [f"screen: book {book}, want {SCREEN_BOOK}"]
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)   # a mutant's '''...''' reads \s as an invalid escape
            tree = ast.parse(client.codes[0])
        sent = [ast.literal_eval(n.args[0]) for n in ast.walk(tree)
                if isinstance(n, ast.Call) and getattr(n.func, "attr", None) == "query" and n.args]
    except (SyntaxError, ValueError) as e:
        return out + [f"screen: the --cloud code does not parse: {e}"]
    if sent != client.sqls:
        out.append("screen: --cloud queries other SQL than the local run"
                   + (": the regex backslashes changed" if "\\b" in client.sqls[0] else ""))
    return out


SCREEN_MUTANTS = {
    "the screen keeps every line": [("  AND ({non_ordinary}) IS NULL\n", "")],
    "the screen on ur-2": [('non_ordinary_sql("p", cfg=UNIVERSE_RULES)',
                            'non_ordinary_sql("p", cfg=__import__("data_utils").UNIVERSE_RULES_V2)')],
    "--cloud sends the SQL in a triple-quoted string": [("cr.query({sql!r})", "cr.query('''{sql}''')")],
}


def mutant_results(source, mutants, check):
    """(survived, raised): the mutants no check catches, and those caught only by raising."""
    survived, broken = [], []
    for name, edits in mutants.items():
        src = source
        for old, new in edits:
            if source.count(old) != 1:
                print(f"test_fcf_conversion_selection: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            src = src.replace(old, new)
        got = check(src)
        if not got:
            survived.append(name)
        elif any(f.startswith("error:") for f in got):
            broken.append((name, got))
    return survived, broken


def main():
    base = failures(SRC, epoch_types=("UINTEGER", "BIGINT")) + screen_failures(SCREEN_SRC)
    if base:
        print("test_fcf_conversion_selection: FAIL")
        for f in base:
            print("  ", f)
        sys.exit(1)
    survived, broken = mutant_results(SRC, MUTANTS, failures)
    s2, b2 = mutant_results(SCREEN_SRC, SCREEN_MUTANTS, screen_failures)
    survived, broken = survived + s2, broken + b2
    if survived or broken:
        print("test_fcf_conversion_selection: FAIL (mutants survived:", survived, "; mutants that raised:", broken, ")")
        sys.exit(1)
    print(f"test_fcf_conversion_selection: PASS ({len(COMPANIES)} companies, {len(CROWD)} in the crowd,"
          f" {len(LINE_STARTS)} line starts, {len(MUTANTS)} mutants caught; the live screen,"
          f" {len(SCREEN_MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
