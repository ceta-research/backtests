#!/usr/bin/env python3
"""Offline check that asset-growth pairs each company's current FY balance row with ITS OWN prior year (no network).

screen_stocks takes the latest FY balance-sheet, key-metrics and ratios rows whose period end is in
(T-410, T-45] days, then the latest balance row 300-430 days before the current one in the same
reportedCurrency (NULL matches NULL), among the lines in `universe`. The old screen paired the current
row with any row on or before T-410, so a skipped year, a stub or a currency switch still ranked. A line
left without a prior row is unranked, and the screen's prior_year_guard record counts it
(currency_switch, no_prior_in_window). Every mutant of the topic's source must trip a check, by changing
the book or the record and not by raising.

Usage:
    python scripts/test_asset_growth_selection.py [repo root]
"""
import calendar
import itertools
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "asset-growth", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)
from data_utils import DataChecks, empty_cap_check, mark_nvdr_twins   # noqa: E402

D = date(2020, 7, 1)
D2 = date(2019, 7, 1)                   # a second screen, for the record's order
FLOOR = 1e9
CUTOFF = D - timedelta(days=45)         # 2020-05-17, inclusive
STALE = D - timedelta(days=410)         # 2019-05-18, exclusive
FY17, FY18, FY19 = date(2017, 12, 31), date(2018, 12, 31), date(2019, 12, 31)
STUB = date(2019, 6, 30)                # 184 days before FY19
IN_LAG = date(2020, 6, 15)
PRIOR_TA = 1000.0
DECOY_TA = 2000.0                       # paired with any current row: growth under -45%, out of the band
_curr = itertools.count(1)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def back(d, days):
    return d - timedelta(days=days)


def load(src):
    mod = types.ModuleType("asset_growth_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def co(*days, curr=None, ta=None, ccy="USD", ccys=None, extra=(), km=None, fr=None, bad=(), roe=0.2,
       roa=0.1, opm=0.2, mcap=5e9, px=5.0, isin=None, universe=True, ineligible=False):
    """One FY row per day in each cache: the last day carries `curr` (default a distinct 1000 + n, so every
    book ranks on growth alone), earlier days PRIOR_TA, all in `ccy`.

    ta/ccys override a day's totalAssets (None drops the balance row) or reportedCurrency; extra adds balance
    rows (day, totalAssets, currency); km/fr give the days of the key-metrics and ratios rows (default every
    day); a `bad` day's key-metrics and ratios rows fail ROE, ROA and margin; ineligible writes check M's
    'ineligible' rule on the last key-metrics row.
    """
    curr = PRIOR_TA + next(_curr) if curr is None else curr
    t = {d: (curr if d == days[-1] else PRIOR_TA) for d in days}
    t.update(ta or {})
    c = {d: ccy for d in days}
    c.update(ccys or {})
    return {"bal": [(d, t[d], c[d]) for d in days if t[d] is not None] + list(extra),
            "km": tuple(days if km is None else km), "fr": tuple(days if fr is None else fr), "bad": set(bad),
            "roe": roe, "roa": roa, "opm": opm, "mcap": mcap, "px": px, "isin": isin, "universe": universe,
            "ineligible": ineligible, "growth": (curr - PRIOR_TA) / PRIOR_TA}


def year(curr_day, gap=365, **kw):
    return co(back(curr_day, gap), curr_day, **kw)


ON_FY19 = {"km": (FY19,), "fr": (FY19,)}    # key metrics and ratios inside the window, whatever the balance rows
COMPANIES = {
    # fiscal calendars
    "DEC": year(FY19),
    "MAR": co(date(2019, 3, 31), date(2020, 3, 31)),
    "JUN": year(date(2019, 6, 30)),
    "SEP": year(date(2019, 9, 30)),
    "WK53": co(date(2018, 12, 29), date(2020, 1, 4)),            # 53-week year, 371 days
    "WK52": co(date(2018, 9, 29), date(2019, 9, 28)),            # 52-week year, 364 days
    # a skipped year: no prior row in the window, out
    "GAP": co(FY17, FY19),
    "GAP_JUN": co(date(2017, 6, 30), date(2019, 6, 30)),
    # a stub 184 days before the current row is not the prior year: skipped past, or out when it is alone
    "STUB": co(FY18, STUB, FY19, ta={STUB: DECOY_TA}),
    "STUB_ONLY": co(STUB, FY19),
    # two rows inside the window (396 and 334 days back): the later one is the prior year
    "TWO": co(date(2018, 11, 30), date(2019, 1, 31), FY19, ta={date(2018, 11, 30): DECOY_TA}),
    # the gap bounds are inclusive
    "EDGE_MIN": year(FY19, gap=300),
    "EDGE_MAX": year(FY19, gap=430),
    "UNDER_MIN": year(FY19, gap=299),
    "OVER_MAX": year(FY19, gap=431),
    # the current balance row: a period end after T-410 (exclusive) and on or before T-45
    "CUR_409": year(STALE + timedelta(days=1), **ON_FY19),
    "CUR_410": year(STALE, **ON_FY19),
    "CUR_411": year(STALE - timedelta(days=1), **ON_FY19),
    "CUR_45": year(CUTOFF),
    "CUR_44": year(CUTOFF + timedelta(days=1)),                  # its T-409 row is current, with no prior
    "LAGGED": co(FY18, FY19, IN_LAG, ta={FY19: PRIOR_TA + next(_curr), IN_LAG: DECOY_TA}, bad=(IN_LAG,)),
    # key metrics and ratios: the same window
    "KM_409": year(FY19, km=(FY18, STALE + timedelta(days=1))),
    "KM_410": year(FY19, km=(FY18, STALE)),
    "KM_OLD": year(FY19, km=(FY18,)),
    "FR_409": year(FY19, fr=(FY18, STALE + timedelta(days=1))),
    "FR_410": year(FY19, fr=(FY18, STALE)),
    "FR_OLD": year(FY19, fr=(FY18,)),
    # the reporting currency: prior then current
    "CCY_SWITCH": year(FY19, ccys={FY18: "AUD"}),
    "CCY_SAME": year(FY19, ccy="AUD"),
    "NULL_NULL": year(FY19, ccy=None),
    "NULL_AUD": year(FY19, ccy="AUD", ccys={FY18: None}),
    "AUD_NULL": year(FY19, ccy=None, ccys={FY18: "AUD"}),
    # the prior date also holds a larger USD row, first on the tie-break: AUD still pairs with AUD
    "TWO_CCY": year(FY19, ccy="AUD", extra=[(FY18, DECOY_TA, "USD")]),
    # the growth band (-20%, +10%) is open at both ends
    "G_MAX": year(FY19, curr=1100.0),
    "G_MAX_IN": year(FY19, curr=1099.9),
    "G_MIN": year(FY19, curr=800.0),
    "G_MIN_IN": year(FY19, curr=800.1),
    # the other tests: out, and a line they fail is not counted by the guard either
    "LOW_ROE": year(FY19, roe=0.05),
    "LOW_ROA": year(FY19, roa=0.04),
    "LOW_OPM": year(FY19, opm=0.09),
    "LOW_ROE_GAP": co(FY17, FY19, roe=0.05),
    "SMALL": year(FY19, mcap=1e6),
    "SMALL_GAP": co(FY17, FY19, mcap=1e6),
    "INELIG": year(FY19, ineligible=True),                       # check M: no cap_used, under the floor
    # the universe rules removed these lines: never ranked, never counted
    "OUTSIDE": year(FY19, universe=False),
    "OUTSIDE_GAP": co(FY17, FY19, universe=False),
    # same ISIN: the line with an entry price is kept
    "TWIN_A": year(FY19, px=None, isin="US0000000001"),
    "TWIN_B": year(FY19, isin="US0000000001"),
}
NO_PRIOR = {"GAP", "GAP_JUN", "STUB_ONLY", "UNDER_MIN", "OVER_MAX", "CUR_44"}
SWITCHED = {"CCY_SWITCH", "NULL_AUD", "AUD_NULL"}
OUT = NO_PRIOR | SWITCHED | {
    "CUR_410", "CUR_411", "KM_410", "KM_OLD", "FR_410", "FR_OLD", "G_MAX", "G_MIN", "LOW_ROE", "LOW_ROA",
    "LOW_OPM", "LOW_ROE_GAP", "SMALL", "SMALL_GAP", "INELIG", "OUTSIDE", "OUTSIDE_GAP", "TWIN_A"}


def ranked(companies, out, n=30):
    """The expected book: (symbol, marketCap) by growth ascending, then symbol, top n."""
    keep = sorted((s for s in companies if s not in out), key=lambda s: (companies[s]["growth"], s))
    return [(s, companies[s]["mcap"]) for s in keep[:n]]


EXPECT = ranked(COMPANIES, OUT)
GUARD_D = {"rebalance_date": D.isoformat(), "currency_switch": len(SWITCHED), "no_prior_in_window": len(NO_PRIOR)}

# More than 30 qualify: the switched line would rank first if paired, so its slot goes to the 31st filler.
CROWD = {f"F{i:02d}": year(FY19, curr=900.0 + 5 * i) for i in range(31)}
CROWD["SWITCH"] = year(FY19, curr=850.0, ccys={FY18: "AUD"})
CROWD_EXPECT = ranked(CROWD, {"SWITCH"})


def db(companies, epoch_type="UINTEGER"):
    """The caches screen_stocks reads. The warehouse's dateEpoch is unsigned 32-bit."""
    con = duckdb.connect()
    e = f"filing_epoch {epoch_type}, period VARCHAR"
    con.execute(f"CREATE TABLE balance_cache(symbol VARCHAR, totalAssets DOUBLE, {e}, reportedCurrency VARCHAR)")
    con.execute(f"CREATE TABLE metrics_cache(symbol VARCHAR, returnOnEquity DOUBLE, returnOnAssets DOUBLE,"
                f" marketCap DOUBLE, {e})")
    con.execute(f"CREATE TABLE ratios_cache(symbol VARCHAR, operatingProfitMargin DOUBLE, {e})")
    con.execute("CREATE TABLE universe(symbol VARCHAR)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    empty_cap_check(con)
    for s, c in companies.items():
        for d, ta, ccy in c["bal"]:
            con.execute("INSERT INTO balance_cache VALUES (?, ?, ?, 'FY', ?)", [s, ta, ep(d), ccy])
        for d in c["km"]:
            q = (-1.0, -1.0) if d in c["bad"] else (c["roe"], c["roa"])
            con.execute("INSERT INTO metrics_cache VALUES (?, ?, ?, ?, ?, 'FY')", [s, *q, c["mcap"], ep(d)])
        for d in c["fr"]:
            con.execute("INSERT INTO ratios_cache VALUES (?, ?, ?, 'FY')", [s, -1.0 if d in c["bad"] else c["opm"], ep(d)])
        if c["ineligible"]:
            con.execute("INSERT INTO cap_check (symbol, filing_epoch, rule) VALUES (?, ?, 'ineligible')",
                        [s, ep(c["km"][-1])])
        if c["universe"]:
            con.execute("INSERT INTO universe VALUES (?)", [s])
        if c["px"]:
            con.execute("INSERT INTO prices_cache VALUES (?, ?, ?, ?, 1e6)", [s, ep(D + timedelta(days=1)), c["px"], c["px"]])
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


def checked_failures(mod):
    """The screen with data checks on and the guard recording: the same book, check M's event, and the
    record's counts at D, sorted by date and equal whichever date is screened first."""
    out = []
    con, g, dc = db(COMPANIES), mod.PriorYearGuard(), DataChecks("on")
    out += diff("book (data checks on)", screen(mod, con, D, dc=dc, guard=g), EXPECT)
    caps = sorted((e["symbol"], e["rule"]) for e in dc.events if e["check"] == "cap")
    if caps != [("INELIG", "ineligible")]:
        out.append(f"check M events {caps}")
    screen(mod, con, D2, guard=g)
    b = g.block()
    if list(b) != ["rule", "dropped", "by_reason", "by_period"]:
        out.append(f"prior_year_guard keys {list(b)}")
    per = b.get("by_period", [])
    if [p.get("rebalance_date") for p in per] != [D2.isoformat(), D.isoformat()] or per[-1:] != [GUARD_D]:
        out.append(f"prior_year_guard by_period {per}, want ... {GUARD_D}")
    sums = {k: sum(p[k] for p in per) for k in ("currency_switch", "no_prior_in_window")}
    if b.get("by_reason") != sums or b.get("dropped") != sum(sums.values()):
        out.append(f"prior_year_guard totals {b.get('dropped')} {b.get('by_reason')}, by_period sums {sums}")
    g2 = mod.PriorYearGuard()
    for d in (D, D2):
        screen(mod, con, d, guard=g2)
    if g2.block() != b:
        out.append("prior_year_guard depends on the order the dates are screened")
    con, g = db(CROWD), mod.PriorYearGuard()
    out += diff("crowd book", screen(mod, con, D, guard=g), CROWD_EXPECT)
    if g.block()["by_period"] != [{"rebalance_date": D.isoformat(), "currency_switch": 1, "no_prior_in_window": 0}]:
        out.append(f"crowd prior_year_guard {g.block()['by_period']}")
    return out


# Sweep: one company per month-end fiscal calendar, FY2009-FY2021 rows growing 5% a year, screened at every
# July rebalance 2012-2021. GAPPY_* never have two consecutive years; SWITCH15 reports in AUD to FY2014 and
# in USD from FY2015, so only the 2016 screen leaves it unranked.
SWEEP_YEARS = range(2012, 2022)
CALENDARS = {calendar.month_abbr[m].upper(): (m, calendar.monthrange(2021, m)[1]) for m in range(1, 13)}


def sweep_companies():
    rows = {name: {date(y, m, dd): (PRIOR_TA * 1.05 ** (y - 2009), "USD") for y in range(2009, 2022)}
            for name, (m, dd) in CALENDARS.items()}
    rows["GAPPY_DEC"] = {d: v for d, v in rows["DEC"].items() if d.year % 2 == 0}
    rows["GAPPY_JUN"] = {d: v for d, v in rows["JUN"].items() if d.year % 2 == 0}
    rows["SWITCH15"] = {d: (ta, "AUD" if d.year < 2015 else "USD") for d, (ta, _) in rows["DEC"].items()}
    out = {}
    for s, r in rows.items():
        days = tuple(sorted(r))
        out[s] = {**co(*days, px=None), "bal": [(d, *r[d]) for d in days]}
    return out


def sweep(mod):
    con, g, out = db(sweep_companies()), mod.PriorYearGuard(), []
    for y in SWEEP_YEARS:
        want = set(CALENDARS) | ({"SWITCH15"} if y != 2016 else set())
        got = {s for s, _ in screen(mod, con, date(y, 7, 1), guard=g)}
        if got != want:
            out.append(f"sweep {y}: missing {sorted(want - got)}, extra {sorted(got - want)}")
    want = [{"rebalance_date": date(y, 7, 1).isoformat(), "currency_switch": int(y == 2016),
             "no_prior_in_window": 2 * (y % 2)} for y in SWEEP_YEARS]
    if g.block()["by_period"] != want:
        out.append(f"sweep prior_year_guard {g.block()['by_period']}")
    return out


def failures(src, epoch_types=("UINTEGER",)):
    out = []
    try:
        mod = load(src)
        for t in epoch_types:   # the book first: it reads only what any version of the screen takes
            out += diff(f"book ({t})", screen(mod, db(COMPANIES, t), D), EXPECT)
        return out + checked_failures(mod) + sweep(mod)
    except Exception as e:  # reported as an error: main() does not count it as a caught mutant
        return out + [f"error: {type(e).__name__}: {e}"]


CUR_WHERE = "FROM balance_cache\n            WHERE filing_epoch <= ? AND filing_epoch > ?\n"
KM_WHERE = "WHERE m.filing_epoch <= ? AND m.filing_epoch > ?\n"
FR_WHERE = "FROM ratios_cache\n            WHERE filing_epoch <= ? AND filing_epoch > ?\n"
GAP_WHERE = "WHERE b.filing_epoch <= c.curr_epoch - ? AND b.filing_epoch >= c.curr_epoch - ?\n"
GAP_ARGS = "prior_gap_min, prior_gap_max,   # bs_window"
CCY_WHERE = "WHERE reportedCurrency IS NOT DISTINCT FROM curr_ccy\n"
PRIOR_ORDER = ("ORDER BY filing_epoch DESC, totalAssets DESC NULLS LAST,\n"
               "                    reportedCurrency DESC NULLS LAST) AS rn\n"
               "            FROM bs_window\n")
SEMI = "          AND bc.symbol IN (SELECT symbol FROM universe)\n"
BAND = "{growth} < ? AND {growth} > ?)"
LAG = "cutoff_epoch = utc_epoch(target_date - timedelta(days=FILING_LAG_DAYS))"
DEDUP = ("FROM ({isin_dedup_sql(paired, target_date, offset_days, MIN_ENTRY_PRICE,\n"
         "                              twin_key=dc is not None and dc.on)})")
# name: [(old, new), ...]; every `old` must occur exactly once in the source
MUTANTS = {
    # the prior row
    "any earlier row (the old screen)": [(GAP_WHERE, "WHERE b.filing_epoch < c.curr_epoch AND ? IS NOT NULL"
                                                     " AND ? IS NOT NULL\n")],
    "no upper gap bound": [(GAP_WHERE, "WHERE b.filing_epoch <= c.curr_epoch - ? AND ? IS NOT NULL\n")],
    "no lower gap bound": [(GAP_WHERE, "WHERE b.filing_epoch < c.curr_epoch AND ? IS NOT NULL"
                                       " AND b.filing_epoch >= c.curr_epoch - ?\n")],
    "gap bounds swapped": [(GAP_ARGS, "prior_gap_max, prior_gap_min,   # bs_window")],
    "earliest row in the window": [(PRIOR_ORDER, PRIOR_ORDER.replace("filing_epoch DESC", "filing_epoch ASC"))],
    "lower gap bound 301 days": [("PRIOR_FY_MIN_DAYS = 300 ", "PRIOR_FY_MIN_DAYS = 301 ")],
    "upper gap bound 429 days": [("PRIOR_FY_MAX_DAYS = 430 ", "PRIOR_FY_MAX_DAYS = 429 ")],
    "upper gap bound admits a skipped year": [("PRIOR_FY_MAX_DAYS = 430 ", "PRIOR_FY_MAX_DAYS = 800 ")],
    # the currency test
    "no currency test": [(CCY_WHERE, "WHERE TRUE\n")],
    "currency test with =": [(CCY_WHERE, "WHERE reportedCurrency = curr_ccy\n")],
    "a switch recorded as no prior": [("WHEN bw.symbol IS NOT NULL THEN 'currency_switch'",
                                       "WHEN FALSE THEN 'currency_switch'")],
    # the current rows
    "no lag": [(LAG, "cutoff_epoch = utc_epoch(target_date)")],
    "lag 44 days": [("FILING_LAG_DAYS = 45 ", "FILING_LAG_DAYS = 44 ")],
    "lag 46 days": [("FILING_LAG_DAYS = 45 ", "FILING_LAG_DAYS = 46 ")],
    "current rows to 411 days": [("CURRENT_ROW_MAX_DAYS = 410 ", "CURRENT_ROW_MAX_DAYS = 411 ")],
    "current rows to 409 days": [("CURRENT_ROW_MAX_DAYS = 410 ", "CURRENT_ROW_MAX_DAYS = 409 ")],
    "balance bound inclusive": [(CUR_WHERE, CUR_WHERE.replace("filing_epoch > ?", "filing_epoch >= ?"))],
    "no age bound on the balance row": [(CUR_WHERE, CUR_WHERE.replace("AND filing_epoch > ?", "AND ? IS NOT NULL"))],
    "key-metrics bound inclusive": [(KM_WHERE, KM_WHERE.replace("m.filing_epoch > ?", "m.filing_epoch >= ?"))],
    "no age bound on key metrics": [(KM_WHERE, "WHERE m.filing_epoch <= ? AND ? IS NOT NULL\n")],
    "key metrics read in the lag": [(KM_WHERE, "WHERE ? IS NOT NULL AND m.filing_epoch > ?\n")],
    "ratios bound inclusive": [(FR_WHERE, FR_WHERE.replace("filing_epoch > ?", "filing_epoch >= ?"))],
    "no age bound on ratios": [(FR_WHERE, FR_WHERE.replace("AND filing_epoch > ?", "AND ? IS NOT NULL"))],
    "ratios read in the lag": [(FR_WHERE, FR_WHERE.replace("filing_epoch <= ? AND", "? IS NOT NULL AND"))],
    # the universe and the tests
    "no universe semi-join": [(SEMI, "")],
    "growth band closed": [(BAND, "{growth} <= ? AND {growth} >= ?)")],
    "no size floor": [('sql("km.cap_used > ?")', 'sql("? IS NOT NULL")')],
    "the floor on the raw cap": [('sql("km.cap_used > ?")', 'sql("km.marketCap > ?")')],
    "same-ISIN dedup removed": [(DEDUP, "FROM ({paired})")],
    "ranking reversed": [("ORDER BY asset_growth ASC, symbol", "ORDER BY asset_growth DESC, symbol")],
}


def main():
    base = failures(SRC, epoch_types=("UINTEGER", "BIGINT"))
    if base:
        print("test_asset_growth_selection: FAIL")
        for f in base:
            print("  ", f)
        sys.exit(1)
    survived, broken = [], []
    for name, edits in MUTANTS.items():
        src = SRC
        for old, new in edits:
            if SRC.count(old) != 1:
                print(f"test_asset_growth_selection: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            src = src.replace(old, new)
        got = failures(src)
        if not got:
            survived.append(name)
        elif any(f.startswith("error:") for f in got):
            broken.append((name, got))
    if survived or broken:
        print("test_asset_growth_selection: FAIL (mutants survived:", survived, "; mutants that raised:", broken, ")")
        sys.exit(1)
    print(f"test_asset_growth_selection: PASS ({len(COMPANIES)} companies, {len(CROWD)} in the crowd,"
          f" {len(CALENDARS)} fiscal calendars x {len(SWEEP_YEARS)} rebalances, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
