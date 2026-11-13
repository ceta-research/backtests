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
first own day, check M's events, a freed slot), each ur-3 line end (before, on and after its last own day,
check M's events, the line_ends block, and July books from 2021 to 2023 where COG and CLR each hold a slot
until their last own day and the next name takes it after), and the --exclude-sector switch (it removes a
line any of whose profile rows names the sector, after the screen's other tests and before the top-30 cut,
and records it per rebalance; off, the book doesn't change). Every mutant of the topic's source must trip a
check, by changing a book or a record and not by raising, and so must each mutant of data_utils' line-end
date test and block.

The live screen (screen.py, today's TTM rows) takes the backtest's universe rules: a line ur-3 alone removes
stays out, and --cloud sends the same SQL text, its regex backslashes intact. ur-3's line ends and starts hold on
the screen's day before the top-30 cut: today's US book leaves COG and CLR out and the next fillers take their
slots, a book on a line's last own day keeps it, and L1G.AX stays out the day before its first own day. The book
ranks on the unrounded FCF over net income, then by symbol, at the 30th slot. Its mutants must trip these checks.

Costs and the results file: run_backtest, its screen and prices stubbed, books a JPX holding at 1 trillion yen
in the 0.3% tier and an LSE holding at 8 billion pounds in the 0.1% tier (the USD tiers at the leg's FX rate).
run_single on test_data_checks' synthetic SET leg writes data_checks right after isin_dedup, then
universe_rules, statement_pair_guard and cost_model (the tiers at 35 baht a dollar, the rate run_backtest
gets; null on a no-costs run); a sector switch that names no line writes the same file, its sector_exclusion block apart, and one that
removes a line still pins cost_model; the universe fetch carries the profile's sector; main() hands
--exclude-sector to run_single as one sector name. Its mutants must trip these checks.

Usage:
    python scripts/test_fcf_conversion_selection.py [repo root]
"""
import ast
import contextlib
import io
import itertools
import json
import os
import sys
import tempfile
import types
import warnings
from datetime import date, timedelta

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "fcf-conversion", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from data_utils import (DataChecks, UniverseRules, UNIVERSE_RULES_V3, empty_cap_check,   # noqa: E402
                        mark_nvdr_twins, utc_epoch as ep)
from costs import apply_costs, get_fx_per_usd   # noqa: E402
import test_data_checks as tdc   # noqa: E402
# asset growth's selection test: its screen dates and floor, and the checks both topics' screens share
from test_asset_growth_selection import D, D2, FLOOR, brazil_failures, diff, screen   # noqa: E402

FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)
NI = 100.0
FS = "Financial Services"
SAME = object()                         # the income rows report in the cash-flow rows' currency
_conv = itertools.count(1)


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

# ur-3's line starts: a listed line's rows and bars before its first own day copy another line's history.
LINE_STARTS = [(s, date.fromisoformat(d)) for s, d, _ in UNIVERSE_RULES_V3["line_starts"]]
# ur-3's line ends: a listed line's rows after its last own day are not its listing's.
LINE_ENDS = [(s, date.fromisoformat(d)) for s, d, _ in UNIVERSE_RULES_V3.get("line_ends", ())]
JULY = (date(2021, 7, 1), date(2022, 7, 1), date(2023, 7, 1))     # COG ends between the first two, CLR the last two

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


def leg_rules(mod, companies, rules=UniverseRules):
    """The leg's rules (the topic's own UNIVERSE_RULES) after its profile rows: every fixture line kept."""
    ur = rules(mod.UNIVERSE_RULES)
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
    if mod.without_lines_sql("SELECT 1", []) != "SELECT 1":
        out.append("the candidates SQL changes when no line is left out")
    return out


def line_end_failures(mod, rules=UniverseRules):
    """Each ur-3 line end: a screen the day before or on its last own day ranks the line, with data checks on
    and off, and check M reads its event; a screen the day after leaves it out and check M reads none; the
    block lists it. In a crowd of 31 fillers screened each July from 2021 to 2023, COG and CLR each hold a slot
    in the books before their last own day, and the next filler takes it in the books after. rules: the
    UniverseRules class the leg runs (a data_utils mutant's)."""
    out = [] if {s for s, _ in LINE_ENDS} == {"COG", "CLR"} else [f"ur-3's line ends {LINE_ENDS}, want COG and CLR"]
    for sym, last in LINE_ENDS:
        rows = {sym: co(cf=(T(100, last),))}
        for label, dc in (("data checks off", None), ("data checks on", DataChecks("on", mod.DATA_CHECKS))):
            ur = leg_rules(mod, rows, rules)
            for d, want in ((last - timedelta(days=1), {sym}), (last, {sym}), (last + timedelta(days=1), set())):
                got = {s for s, _ in screen(mod, db(rows), d, dc=dc, ur=ur)}
                if got != want:
                    out.append(f"{sym} on {d} ({label}): {sorted(got)}, want {sorted(want)}")
        lines = [(r["symbol"], r["last_day"]) for r in ur.block().get("line_ends", {}).get("lines", [])]
        if lines != [(sym, last.isoformat())]:
            out.append(f"{sym}: line_ends block {lines}")
        inelig = {sym: co(cf=(T(100, last),), rule="ineligible")}
        for d, want in ((last, [sym]), (last + timedelta(days=1), [])):
            dc = DataChecks("on", mod.DATA_CHECKS)
            screen(mod, db(inelig), d, dc=dc, ur=leg_rules(mod, inelig, rules))
            caps = [e["symbol"] for e in dc.events if e["check"] == "cap"]
            if caps != want:
                out.append(f"check M events for {sym} on {d}: {caps}, want {want}")
    days = [T(100, d) for d in JULY]
    crowd = {**{f"F{i:02d}": co(cf=days, conv=1.1 + 0.01 * i) for i in range(31)},
             **{s: co(cf=days, conv=2.9) for s, _ in LINE_ENDS}}
    con, ur = db(crowd, on=JULY), leg_rules(mod, crowd, rules)
    for d in JULY:
        out += diff(f"line-end crowd {d}", screen(mod, con, d, ur=ur),
                    ranked(crowd, {s for s, last in LINE_ENDS if last < d}))
    if [r["symbol"] for r in ur.block().get("line_ends", {}).get("lines", [])] != sorted(s for s, _ in LINE_ENDS):
        out.append(f"line-end crowd: line_ends block {ur.block().get('line_ends')}")
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
        return (out + guard_failures(mod) + brazil_failures(mod, db, BRAZIL) + line_start_failures(mod)
                + line_end_failures(mod) + sector_failures(mod))
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
CANDIDATES = 'candidates = without_lines_sql(_candidates_sql("km.cap_used > ?"), left_out)'
CAP_LINES = "_paired(without_lines_sql(_candidates_sql(CAP_FAIL_TEST, events=True), left_out))"
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
        (PICKED, "picked = [(r[0], r[1]) for r in rows if r[0] not in left_out]")],
    "check M reads a line before its first own day": [(CAP_LINES, CAP_LINES.replace("left_out", "gone"))],
    # ur-3's line ends
    "no line ends": [("gone = ur.ended(target_date) if ur is not None else []", "gone = []")],
    "an ended line left out after the top-30 cut": [
        (CANDIDATES, 'candidates = without_lines_sql(_candidates_sql("km.cap_used > ?"), late)'),
        (PICKED, "picked = [(r[0], r[1]) for r in rows if r[0] not in gone]")],
    "check M reads a line after its last own day": [(CAP_LINES, CAP_LINES.replace("left_out", "late"))],
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


# data_utils' line-end test and block, each mutant run through line_end_failures
DU_PATH = os.path.join(ROOT, "data_utils.py")
with open(DU_PATH) as fh:
    DU_SRC = fh.read()
ENDED = "if d < target_date.isoformat())"
DU_MUTANTS = {
    "a line left out on its last own day too": [(ENDED, "if d <= target_date.isoformat())")],
    "a line left out before its last own day, not after": [(ENDED, "if d > target_date.isoformat())")],
    "a line kept the day after its last own day": [(ENDED, "if d < (target_date - timedelta(days=1)).isoformat())")],
    "line_ends not written to the block": [('("line_ends", LINE_END_RULE, "last_day", self.ends)',
                                            '("line_ends", LINE_END_RULE, "last_day", {})')],
}


def du_line_end_failures(src):
    """line_end_failures with the UniverseRules of data_utils source src."""
    try:
        du_mod = types.ModuleType("data_utils_line_end_mutant")
        du_mod.__file__ = DU_PATH
        exec(compile(src, DU_PATH, "exec"), du_mod.__dict__)
        return line_end_failures(load(SRC), du_mod.UniverseRules)
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]


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
# ur-3's line ends COG and CLR rank ahead of 31 NYSE fillers, so a US book that keeps them, or leaves them out
# after the top-30 cut, is not the fillers' first 30. The fillers go in last first. U28 and U29 tie on FCF over net
# income, so the book of 2021-10-01 keeps U28 at the 30th slot by the symbol tie-break alone. T31 rounds to U30's
# fcf_conversion_pct below its unrounded value and sorts before it, so a book ranked on the rounded column keeps
# T31 at the 30th slot. L1G.AX is ur-3's line start on ASX (first own day 2025-10-04).
US_FILLERS = [f"U{i:02d}" for i in range(1, 31)] + ["T31"]
FILLER_CONV = {**{s: round(2.4 - 0.03 * i, 2) for i, s in enumerate(US_FILLERS[:27], 1)},
               "U28": 1.56, "U29": 1.56, "U30": 1.5004, "T31": 1.5001}
LINE_SCREEN_LINES = {"COG": ("NYSE", "US0000000021", False, 2.6, 5e9),
                     "CLR": ("NYSE", "US0000000022", False, 2.5, 5e9),
                     **{s: ("NYSE", f"US00000001{i:02d}", False, FILLER_CONV[s], 5e9)
                        for i, s in reversed(list(enumerate(US_FILLERS, 1)))},
                     "L1G.AX": ("ASX", "AU0000000031", False, 1.5, 5e9),
                     "ASX1.AX": ("ASX", "AU0000000032", False, 1.4, 5e9)}


class ScreenClient(tdc.FakeClient):
    """Stands in for CetaResearch: query runs the SQL on the fixture (test_data_checks' FakeClient), execute_code
    records the code."""

    def __init__(self, con):
        super().__init__(con)
        self.codes = []

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
    for s, (ex, isin, fund, conv, cap) in {**SCREEN_LINES, **LINE_SCREEN_LINES}.items():
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


def screen_line_failures(src):
    """ur-3's line ends and starts in the live screen: main() on NYSE (today) leaves COG and CLR out and ranks the
    fillers' first 30; screen_sql on a line's last own day keeps it and the day after leaves it out, the next
    filler taking its slot; on ASX, L1G.AX is out the day before its first own day and ranks on it."""
    us = [(date(2021, 10, 1), ["COG", "CLR"] + US_FILLERS[:28]), (date(2021, 10, 2), ["CLR"] + US_FILLERS[:29]),
          (date(2022, 11, 23), US_FILLERS[:30])]
    asx = [(date(2025, 10, 3), ["ASX1.AX"]), (date(2025, 10, 4), ["L1G.AX", "ASX1.AX"])]
    try:
        mod = load_screen(src)
        con = screen_db()
        client = ScreenClient(con)
        run_screen_main(mod, client, ["--exchange", "NYSE"])
        cases = [("NYSE today", client.sqls[-1], US_FILLERS[:30])]
        for ex, days in ((["NYSE"], us), (["ASX"], asx)):
            cases += [(f"{ex[0]} {d}", mod.screen_sql(ex, mod.get_mktcap_threshold(ex), d), want) for d, want in days]
        books = [(label, [r[0] for r in con.execute(sql).fetchall()], want) for label, sql, want in cases]
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return [f"screen {label}: book {book}, want {want}" for label, book, want in books if book != want]


def live_screen_failures(src):
    return screen_failures(src) + screen_line_failures(src)


SCREEN_MUTANTS = {
    "the screen keeps every line": [("  AND ({non_ordinary}) IS NULL\n", "")],
    "the screen on ur-2": [('non_ordinary_sql("p", cfg=UNIVERSE_RULES)',
                            'non_ordinary_sql("p", cfg=__import__("data_utils").UNIVERSE_RULES_V2)')],
    "--cloud sends the SQL in a triple-quoted string": [("cr.query({sql!r})", "cr.query('''{sql}''')")],
    "the screen keeps ended lines": [("ur.not_started(day) + ur.ended(day)", "ur.not_started(day)")],
    "the screen ranks a line before its first own day": [("ur.not_started(day) + ur.ended(day)", "ur.ended(day)")],
    "the screen leaves its lines out after the top-30 cut": [
        ('return f"SELECT * FROM ({without_lines_sql(candidates, left_out)}) _r ORDER BY fcf_conversion DESC, symbol '
         'LIMIT 30"',
         'return without_lines_sql(f"SELECT * FROM ({candidates}) _r ORDER BY fcf_conversion DESC, symbol LIMIT 30", '
         'left_out)')],
    "the screen with no symbol tie-break": [("ORDER BY fcf_conversion DESC, symbol LIMIT 30\"",
                                             "ORDER BY fcf_conversion DESC LIMIT 30\"")],
    "the screen ranked on the rounded column": [("ORDER BY fcf_conversion DESC, symbol LIMIT 30\"",
                                                 "ORDER BY fcf_conversion_pct DESC, symbol LIMIT 30\"")],
}


# ---------------------------------------------------------------- costs and the results file

# run_backtest on one leg, the screen and prices stubbed: ten holdings at one market cap in the leg's currency,
# each up 10% before costs. The cost tiers are in USD (over 10B 0.1%, over 2B 0.3%), converted at the leg's rate.
COST_ENTRY, COST_EXIT, COST_RAW = date(2020, 7, 1), date(2021, 7, 1), 0.10
COST_CASES = [   # (leg, market cap in its currency, one-way rate): JPX at 150 yen, LSE at 0.79 pounds a dollar
    ("JPX", 1e12, 0.003),   # 1 trillion yen is about 6.7B dollars: the 0.3% tier, where 1e12 > 10B would read 0.1%
    ("LSE", 8e9, 0.001),    # 8B pounds is about 10.1B dollars: the 0.1% tier, where 8e9 < 10B would read 0.3%
]
COST_MODEL = {"function": "costs.tiered_cost", "fx_per_usd": 35.0}   # the SET leg: 35 baht a dollar
SET_FINANCIAL = "A01.BK"   # the synthetic leg's line whose profile names FS for the switch's run
_LEGS = {}


def set_leg(financial=False):
    """test_data_checks' synthetic SET leg for fcf-conversion, built once (runs only read it); financial: with
    SET_FINANCIAL's profile row in FS."""
    if financial not in _LEGS:
        src = tdc.fcf_source()
        if financial:
            src.execute("UPDATE profile SET sector = ? WHERE symbol = ?", [FS, SET_FINANCIAL])
        _LEGS[financial] = src
    return _LEGS[financial]


class FakeCon:
    """A connection run_backtest must not read: the screen, prices and benchmark are stubbed."""

    def __getattr__(self, name):
        raise AssertionError(f"run_backtest read the connection ({name})")


def cost_return(mod, exchange, cap):
    """The period return run_backtest books for ten holdings at `cap`, costs on, at the leg's FX rate."""
    names = [f"C{i:02d}" for i in range(10)]
    mod.screen_stocks = lambda *a, **k: [(s, cap) for s in names]
    mod.get_prices = lambda con, symbols, d, **k: {s: 10.0 * (1 + COST_RAW * (d == COST_EXIT)) for s in symbols}
    mod.get_benchmark_return = lambda *a, **k: 0.05
    rows = mod.run_backtest(FakeCon(), [COST_ENTRY, COST_EXIT], 0.0, use_costs=True, fx=get_fx_per_usd([exchange]))
    return rows[0]["portfolio_return"]


def cost_failures(src):
    """A holding's cost tier reads its market cap against the USD tiers converted at the leg's FX rate."""
    out = []
    for exchange, cap, rate in COST_CASES:
        try:
            got, want = cost_return(load(src), exchange, cap), round(apply_costs(COST_RAW, rate), 6)
        except Exception as e:
            return out + [f"error: {type(e).__name__}: {e}"]
        if got != want:
            out.append(f"costs: {exchange} at {cap:g} books {got}, want {want} (the {rate:.1%} tier)")
    return out


def results_failures(src):
    """run_single on test_data_checks' synthetic SET leg (fcf_source): data_checks sits right after isin_dedup, then
    universe_rules, statement_pair_guard and cost_model, then the LTP keys; cost_model names the tiers and the
    leg's FX rate, which run_single passes to run_backtest, and a no-costs run writes it null; a switch on a
    sector no line names writes the same file as no switch, its sector_exclusion block apart, and a switch that
    removes a line still pins cost_model."""
    out = []
    try:
        mod = load(src)
        fx = []
        run_bt = mod.run_backtest
        mod.run_backtest = lambda *a, **k: fx.append(k.get("fx")) or run_bt(*a, **k)
        runs = {}
        with tempfile.TemporaryDirectory() as tmp:
            for name, mode, sw, s, costs in (("drop", "drop", None, set_leg(), True),
                                             ("ltp", "ltp", None, set_leg(), True),
                                             ("none", "ltp", "Utilities", set_leg(), True),
                                             ("fin", "ltp", FS, set_leg(True), True),
                                             ("free", "ltp", None, set_leg(), False)):
                runs[name] = tdc.run_topic("fcf-conversion", s, mode, "on", tmp, tag=f".{name}", mod=mod,
                                           use_costs=costs, **({"exclude_sector": sw} if sw else {}))[0]
    except Exception as e:
        return out + [f"error: {type(e).__name__}: {e}"]
    if fx != [35.0] * len(runs):
        out.append(f"run_single passes fx {fx} to run_backtest, want 35.0 on every run")
    for name, res in runs.items():
        keys = list(res)
        tail = keys[keys.index("isin_dedup") + 1:] if "isin_dedup" in keys else keys
        want = (["data_checks", "universe_rules", "statement_pair_guard", "cost_model"]
                + (["sector_exclusion"] if name in ("none", "fin") else [])
                + (["exit_method", "ltp"] if name != "drop" else []))
        if tail != want:
            out.append(f"{name}: keys after isin_dedup {tail}, want {want}")
        want_model = None if name == "free" else COST_MODEL
        if res.get("cost_model") != want_model:
            out.append(f"{name}: cost_model {res.get('cost_model')}, want {want_model}")
    none, plain = runs["none"], runs["ltp"]
    dates = [r["rebalance_date"] for r in none["period_data"]]
    if none.get("sector_exclusion") != {"sectors": ["Utilities"], "field": "profile.sector",
                                        "removed": {d: [] for d in dates}}:
        out.append(f"a sector no line names: sector_exclusion {none.get('sector_exclusion')}")
    # byte for byte: each file is json.dump(indent=2) of what it loads back as
    if json.dumps({k: v for k, v in none.items() if k != "sector_exclusion"}, indent=2) != json.dumps(plain, indent=2):
        out.append("a switch on a sector no line names changes the file beyond its sector_exclusion block")
    fin = runs["fin"]
    removed = {s for v in (fin.get("sector_exclusion") or {}).get("removed", {}).values() for s in v}
    held = {s for r in fin["period_data"] for s in r["holdings"].split(",")}
    if removed != {SET_FINANCIAL} or held & removed:
        out.append(f"the switch on {FS}: removed {sorted(removed)}, held {sorted(held & removed)}")
    return out


RESULT_MUTANTS = {
    "costs ignore the leg's FX rate": [("tiered_cost(mc, fx_per_usd=fx)", "tiered_cost(mc)")],
    "costs at one baht, yen or pound a dollar": [("fx = get_fx_per_usd(exchanges)", "fx = 1.0")],
    "run_single doesn't pass the FX rate": [("ur=ur, guard=guard, win=win, fx=fx,", "ur=ur, guard=guard, win=win,")],
    "cost_model not written": [('"cost_model": {"function": "costs.tiered_cost", "fx_per_usd": fx} if use_costs else None',
                                '"cost_model": None')],
    "cost_model written on a no-costs run": [('"fx_per_usd": fx} if use_costs else None', '"fx_per_usd": fx}')],
    "data_checks after the other blocks": [
        ('    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both\n', ""),
        ("    if ltp_block:\n        output.update(", '    output["data_checks"] = dc.block()\n    if ltp_block:\n'
                                                      "        output.update(")],
    "the switch always on": [("sectors = SectorExclusion(exclude_sector) if exclude_sector else None",
                              "sectors = SectorExclusion(exclude_sector or [])")],
    "the universe fetch without the sector column": [('extra_cols=("sector",), ', "")],
    "--exclude-sector repeatable": [('parser.add_argument("--exclude-sector", default=None,',
                                     'parser.add_argument("--exclude-sector", action="append", default=None,')],
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


def flag_failures(src):
    """main() on one leg hands --exclude-sector to run_single as one sector name, as the arm's legs-file line
    passes it (CetaResearch and run_single stubbed, a typed end year so no query runs)."""
    try:
        mod, got = load(src), []
        mod.CetaResearch = lambda **_kw: None
        mod.run_single = lambda *a, **k: got.append(k.get("exclude_sector"))
        argv, sys.argv = sys.argv, ["backtest.py", "--exchange", "NYSE,NASDAQ,AMEX", "--end-year", "2025",
                                    "--exclude-sector", FS]
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                mod.main()
        finally:
            sys.argv = argv
    except (Exception, SystemExit) as e:   # argparse exits on a flag it doesn't take
        return [f"error: {type(e).__name__}: {e}"]
    return [] if got == [FS] else [f"main hands run_single exclude_sector {got}, want [{FS!r}]"]


def costs_and_results_failures(src):
    return cost_failures(src) + results_failures(src) + flag_failures(src)


def main():
    base = (failures(SRC, epoch_types=("UINTEGER", "BIGINT")) + live_screen_failures(SCREEN_SRC)
            + costs_and_results_failures(SRC))
    if base:
        print("test_fcf_conversion_selection: FAIL")
        for f in base:
            print("  ", f)
        sys.exit(1)
    survived, broken = mutant_results(SRC, MUTANTS, failures)
    s2, b2 = mutant_results(SCREEN_SRC, SCREEN_MUTANTS, live_screen_failures)
    s3, b3 = mutant_results(SRC, RESULT_MUTANTS, costs_and_results_failures)
    s4, b4 = mutant_results(DU_SRC, DU_MUTANTS, du_line_end_failures)
    survived, broken = survived + s2 + s3 + s4, broken + b2 + b3 + b4
    if survived or broken:
        print("test_fcf_conversion_selection: FAIL (mutants survived:", survived, "; mutants that raised:", broken, ")")
        sys.exit(1)
    print(f"test_fcf_conversion_selection: PASS ({len(COMPANIES)} companies, {len(CROWD)} in the crowd,"
          f" {len(LINE_STARTS)} line starts, {len(LINE_ENDS)} line ends, {len(MUTANTS)} mutants caught;"
          f" data_utils' line-end test and block, {len(DU_MUTANTS)} mutants caught; the live screen,"
          f" {len(SCREEN_MUTANTS)} mutants caught; costs and the results file on the SET leg,"
          f" {len(RESULT_MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
