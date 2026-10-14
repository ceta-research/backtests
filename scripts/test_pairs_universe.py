#!/usr/bin/env python3
"""Offline check of the pairs candidate universe, pairs_universe.py (no network).

A stand-in client runs the module's own warehouse SQL on made-up rows, so fetch_universe,
build_candidates, fetch_prices and check_pairs are driven end to end. Checked: a line is a
candidate of a trading year only if it was listed through the formation year before it (200 bars
priced above zero, the last one inside the year's final 10 days) and traded; no isActivelyTrading
test, and one list per year; funds, lines with no sector and lines with no FY filing are out; the
FY-filing test reads any date (a line first filing after both years is in both), and UNIVERSE_NOTES
and each wired topic's docstring say so; the campaign listing rule; size is the median daily close x
volume (close, not adjClose) in USD at a rate dated on or before year end and at most 7 days old; one line per ISIN, then per company (ISIN,
cik, name, ticker root), both before the top-30 cut and both on the entry window of offset_days 0
and 1; the cost cap is the latest FY filing 45 to 540 days before the year, in USD; same inputs in
another order give the same lists. Every mutant of the module's source must trip a check.

The three pairs topics' own correlation queries are checked once a topic imports pairs_universe
(a frozen-price leg forms no pair, the result does not depend on row order); until then that
topic is reported as not wired.

Usage:
    python scripts/test_pairs_universe.py [repo root]
"""
import os
import re
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "pairs_universe.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

YEARS = [2019, 2020]
T20 = date(2020, 1, 1)
F18_END, F19_END = date(2018, 12, 31), date(2019, 12, 31)
FY17, FY18 = date(2017, 12, 31), date(2018, 12, 31)   # each 366 days before the next trading year
LATE_FY = date(2021, 12, 31)                           # after both trading years
TOP_N, MIN_DAYS, MIN_PX, OFFSET = 30, 200, 1.0, 1
TABLES = ("profile", "key_metrics", "stock_eod", "forex_prices_daily", "delisted_companies")
TOPICS = {"pairs-fundamentals": "compute_correlations", "pairs-zscore": "compute_pair_candidates",
          "pairs-multi-pair": "compute_pair_candidates"}


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def line(symbol, sector, exchange="NYSE", country="US", currency="USD", isin=None, cik=None, name=None,
         live=True, etf=False, fund=False, caps="default", bars="default", entry=None, tv=1000.0,
         profile_rows=None, adj=1.0, zeros=()):
    """One listed line. bars: [(last day, n days, traded value or None)]; entry: {(month, day) of Jan 2020: price};
    adj: adjClose / close of its bars; zeros: days with a bar at price 0.0."""
    return {"symbol": symbol, "sector": sector, "exchange": exchange, "country": country, "currency": currency,
            "isin": isin or f"ZZ{symbol}", "cik": cik, "name": name or f"{symbol} Corp", "live": live,
            "etf": etf, "fund": fund, "profile_rows": profile_rows, "adj": adj, "zeros": zeros,
            "caps": [(FY18, 5e9, "USD", "FY")] if caps == "default" else caps,
            "bars": [(F19_END, 250, tv)] if bars == "default" else bars,
            "entry": {(1, d): entry for d in range(2, 7)} if isinstance(entry, float) else (entry or {})}


def fixture():
    """Every line of the made-up warehouse, with the FX rows. Sector names label the scenario."""
    L = []
    # a, b: listed through 2019 and traded
    L += [line("L200", "Listed", bars=[(F19_END, 200, 1000.0)]),
          line("L199", "Listed", bars=[(F19_END, 199, 1000.0)]),
          line("LAST10", "Listed", bars=[(date(2019, 12, 21), 250, 1000.0)]),
          line("LAST11", "Listed", bars=[(date(2019, 12, 20), 250, 1000.0)]),
          line("ZEROVOL", "Listed", tv=0.0),
          line("HALFNULL", "Listed", bars=[(date(2019, 9, 22), 150, None), (F19_END, 100, 1000.0)])]
    # a bar at price 0.0 is not a formation bar: not in the count, not the last bar, not in the median
    L += [line("Z199", "Listed", bars=[(F19_END, 199, 1000.0)], zeros=[date(2019, 6, 1)]),
          line("ZLAST11", "Listed", bars=[(date(2019, 12, 20), 250, 1000.0)], zeros=[F19_END]),
          line("ZMED", "Listed", bars=[(date(2019, 9, 22), 100, 1000.0), (F19_END, 100, 3000.0)],
               zeros=[date(2019, 6, 1), date(2019, 6, 2)])]
    # a name that has stopped trading is in its listed years; a name listed later is not in earlier ones
    two = [(FY17, 5e9, "USD", "FY"), (FY18, 5e9, "USD", "FY")]
    L += [line("OLD", "Era", live=False, caps=two, bars=[(F18_END, 250, 1000.0), (date(2019, 6, 30), 181, 1000.0)]),
          line("NEW", "Era"),
          line("STAY", "Era", caps=two, bars=[(F18_END, 250, 1000.0), (F19_END, 250, 1000.0)])]
    # the FY-filing test is not point-in-time: a first filing dated after both years admits the line to both
    L += [line("LATEFY", "Era", caps=[(LATE_FY, 5e9, "USD", "FY")], bars=[(F18_END, 250, 1000.0), (F19_END, 250, 1000.0)])]
    # f: 31 names, the 31st is out
    L += [line(f"D{i:02d}", "Cut31", tv=10000.0 - 100 * i) for i in range(1, 32)]
    # 31 lines, two of them one security: the 31st by size gets the freed slot
    L += [line("T01Z", "CutTwin", isin="XT1", entry=5.0, tv=30000.0),
          line("T01", "CutTwin", isin="XT1", entry=5.0, tv=29000.0)]
    L += [line(f"T{i:02d}", "CutTwin", tv=20000.0 - 100 * i) for i in range(2, 31)]
    # d: same ISIN. The line under the entry floor loses; then the line still trading wins
    L += [line("TWIN_A", "Isin", isin="XI1", entry=0.5), line("TWIN_B", "Isin", isin="XI1", entry=5.0),
          line("RETD", "Isin", isin="XI2", live=False, entry=5.0), line("RETDX", "Isin", isin="XI2", entry=5.0)]
    # d: the entry window follows offset_days. 1 January is inside it at offset 0 only, 12 January at offset 1 only
    L += [line("ISA", "Isin", isin="XI3", entry={(1, 1): 5.0}), line("ISB", "Isin", isin="XI3", entry={(1, 12): 5.0})]
    # e: company links, one per pair
    L += [line("CIKA", "Company", cik="0000111", entry=5.0), line("CIKBB", "Company", cik="111", entry=5.0),
          line("NAMEA", "Company", name="AB Volvo", entry=5.0),
          line("NAMEBB", "Company", name="AB Volvo (publ)", entry=5.0),
          line("PUNA", "Company", name="Samsung Electronics Co., Ltd.", entry=5.0),
          line("PUNBB", "Company", name="Samsung Electronics Co. Ltd.", entry=5.0),
          line("ONECIK", "Company", cik="222", name="Mixed Name plc", entry=5.0),
          line("ONECIKX", "Company", name="Mixed Name plc", entry=5.0),
          line("SAME1", "Company", cik="301", name="Twin Name Corp", entry=5.0),
          line("SAME2", "Company", cik="302", name="Twin Name Corp", entry=5.0),
          line("ROOT", "Company", name="Root Common Inc", entry=5.0),
          line("ROOT-PFA", "Company", name="Root Preferred A", entry=5.0),
          line("005930.KS", "Company", name="Sam Common", entry=5.0, tv=3000.0),
          line("005935.KS", "Company", name="Sam Preferred", entry=5.0, tv=100.0),
          line("005940.KS", "Company", name="Other Korean", entry=5.0)]
    # e: the pick. Buyable at entry, then the shorter symbol, the larger size, the symbol
    L += [line("PK", "Company", cik="401"), line("PKLONG", "Company", cik="401", entry=5.0),
          line("SH", "Company", cik="402", entry=5.0, tv=100.0),
          line("SHLONG", "Company", cik="402", entry=5.0, tv=9000.0),
          line("SZA1", "Company", cik="403", entry=5.0, tv=100.0),
          line("SZB1", "Company", cik="403", entry=5.0, tv=200.0),
          line("EQA1", "Company", cik="404", entry=5.0), line("EQB1", "Company", cik="404", entry=5.0)]
    # e: the entry window [T + 1, T + 11] ([T, T + 10] at offset 0) and the entry floor, one unit either side
    L += [line("WA", "Window", cik="501"), line("WALONG", "Window", cik="501", entry={(1, 12): 5.0}),
          line("WB", "Window", cik="502"), line("WBLONG", "Window", cik="502", entry={(1, 13): 5.0}),
          line("WC", "Window", cik="503"), line("WCLONG", "Window", cik="503", entry={(1, 1): 5.0}),
          line("WD", "Window", cik="505"), line("WDLONG", "Window", cik="505", entry={(1, 11): 5.0}),
          line("FA", "Window", cik="504", entry=0.99), line("FALONG", "Window", cik="504", entry=1.0)]
    # e: a bar at 0.0 is not an entry bar, the line is bought on its next one
    L += [line("WZ", "Window", cik="506"), line("WZLONG", "Window", cik="506", entry={(1, 2): 0.0, (1, 3): 5.0})]
    # no size floor: a five-name sector admits all five
    L += [line(f"F{i}", "Five", tv=v) for i, v in enumerate((500.0, 400.0, 300.0, 200.0, 7.0), 1)]
    # the static universe: funds, no sector, no FY filing; the latest profile row decides
    prof = lambda sector, etf, at: {"sector": sector, "etf": etf, "fetched": at}
    L += [line("ETF1", "Funds", etf=True), line("FUND1", "Funds", fund=True), line("NOSECT", None),
          line("BLANKSECT", ""), line("NOFY", "Funds", caps=[]),
          line("ZEROCAP", "Funds", caps=[(FY18, 0.0, "USD", "FY")]),
          line("QTR", "Funds", caps=[(FY18, 5e9, "USD", "Q4")]),
          line("NULLFLAG", "Funds", etf=None, fund=None),
          line("GOODF", "Funds", caps=[(FY18, 5e9, "USD", "FY"), (None, 9e15, "USD", "FY")]),
          line("DUP", "Funds", profile_rows=[prof("Funds", False, 100), prof("Funds", True, 200)]),
          line("DUP2", "Funds", profile_rows=[prof("Funds", False, 200), prof("Stale", False, 100)])]
    # c: size in USD
    L += [line("GBPENCE", "Fx", currency="GBp", tv=8000.0), line("USDLINE", "Fx", tv=150.0),
          line("NOFX", "Fx", currency="XXX"), line("STALEFX", "Fx", currency="STL"),
          line("EDGEFX", "Fx", currency="EDG", tv=320.0), line("LATEFX", "Fx", currency="LAT", tv=440.0),
          line("NEWEST", "Fx", currency="NEW", tv=1200.0),
          # the rate of 31 December counts, the rate of 1 January does not
          line("EOYFX", "Fx", currency="EOY", tv=380.0), line("JANFX", "Fx", currency="JAN", tv=280.0),
          # size is close x volume: adjClose is half the close after a later 2:1 split
          line("SPLIT", "Fx", tv=260.0, adj=0.5)]
    # cost cap: the latest FY filing dated T - 540 to T - 45 days, in USD
    back = lambda days: T20 - timedelta(days=days)
    L += [line("CAP44", "Cap", caps=[(back(44), 3e9, "USD", "FY")]),
          line("CAP45", "Cap", caps=[(back(45), 3e9, "USD", "FY")]),
          line("CAP540", "Cap", caps=[(back(540), 4e9, "USD", "FY")]),
          line("CAP541", "Cap", caps=[(back(541), 4e9, "USD", "FY")]),
          line("CAPNEW", "Cap", caps=[(FY18, 1e9, "USD", "FY"), (date(2019, 6, 30), 2e9, "USD", "FY"),
                                      (back(10), 9e9, "USD", "FY")]),
          line("CAPKRW", "Cap", caps=[(FY18, 1.2e13, "KRW", "FY")]),
          line("CAPNOFX", "Cap", caps=[(FY18, 5e9, "XXX", "FY")]),
          line("CAPTIE", "Cap", caps=[(FY18, 5e9, "USD", "FY"), (FY18, 7e9, "USD", "FY")])]
    # equal sizes: the symbol decides, whichever line the warehouse returns first
    for k in range(1, 9):
        pair = [line(f"TI{k}A", f"Tie{k}"), line(f"TI{k}B", f"Tie{k}")]
        L += pair if k % 2 else pair[::-1]
    # the campaign listing rule, a leg with no rate at all
    L += [line("DE1.DE", "Autos", exchange="XETRA", country="DE", currency="EUR", tv=900.0),
          line("US1.DE", "Autos", exchange="XETRA", country="US", currency="EUR"),
          line("GBX.L", "Banks", exchange="LSE", country="GB", currency="GBp", tv=8000.0),
          line("USX.L", "Banks", exchange="LSE", country="GB", currency="USD"),
          line("ZZZ1.SI", "Reits", exchange="SES", country="SG", currency="ZZZ")]
    L += [line("SPY", None, exchange="AMEX", etf=True, caps=[],
               bars=[(date(2003, 12, 31), 5, 1000.0), (F19_END, 250, 1000.0)])]
    fx = [("USDGBP", date(2019, 12, 30), 0.8), ("USDGBP", F19_END, 0.0),
          ("USDSTL", date(2019, 12, 23), 3.0),
          ("USDEDG", date(2019, 12, 24), 2.0), ("USDEDG", date(2019, 12, 23), 9.0),
          ("USDLAT", date(2019, 12, 30), 4.0), ("USDLAT", date(2020, 1, 2), 2.0),
          ("USDNEW", date(2019, 12, 27), 5.0), ("USDNEW", date(2019, 12, 30), 10.0),
          ("USDEOY", date(2019, 12, 30), 4.0), ("USDEOY", F19_END, 2.0),
          ("USDJAN", date(2019, 12, 30), 4.0), ("USDJAN", T20, 2.0),
          ("USDKRW", date(2019, 12, 30), 1200.0), ("USDEUR", date(2019, 12, 30), 0.9)]
    return L, fx


# 2020 candidates by sector on the NYSE leg, with size_usd where the scenario is about size
EXPECT_2020 = {
    "Listed": ["L200", "LAST10", "ZMED"],
    "Era": ["LATEFY", "NEW", "STAY"],
    "Cut31": [f"D{i:02d}" for i in range(1, 31)],
    "CutTwin": [f"T{i:02d}" for i in range(1, 31)],
    "Isin": ["ISB", "RETDX", "TWIN_B"],
    "Company": ["005930.KS", "005940.KS", "CIKA", "EQA1", "NAMEA", "ONECIK", "PKLONG", "PUNA", "ROOT",
                "SAME1", "SAME2", "SH", "SZB1"],
    "Window": ["FALONG", "WALONG", "WB", "WC", "WDLONG", "WZLONG"],
    "Five": ["F1", "F2", "F3", "F4", "F5"],
    "Funds": ["DUP2", "GOODF", "NULLFLAG"],
    "Fx": ["EDGEFX", "EOYFX", "GBPENCE", "JANFX", "LATEFX", "NEWEST", "SPLIT", "USDLINE"],
    "Cap": ["CAP44", "CAP45", "CAP540", "CAP541", "CAPKRW", "CAPNEW", "CAPNOFX", "CAPTIE"],
    **{f"Tie{k}": [f"TI{k}A", f"TI{k}B"] for k in range(1, 9)},
}
EXPECT_2019 = {"Era": ["LATEFY", "OLD", "STAY"]}
# the sectors that change at offset_days 0, where the entry window is [1 January, 11 January]
EXPECT_2020_OFFSET0 = {"Isin": ["ISA", "RETDX", "TWIN_B"],
                       "Window": ["FALONG", "WA", "WB", "WCLONG", "WDLONG", "WZLONG"]}
SIZES = {"GBPENCE": 100.0, "USDLINE": 150.0, "EDGEFX": 160.0, "LATEFX": 110.0, "NEWEST": 120.0, "F5": 7.0,
         "EOYFX": 190.0, "JANFX": 70.0, "SPLIT": 260.0, "ZMED": 2000.0}
CAPS = {"CAP44": None, "CAP45": 3e9, "CAP540": 4e9, "CAP541": None, "CAPNEW": 2e9, "CAPKRW": 1e10,
        "CAPNOFX": None, "CAPTIE": 7e9, "L200": 5e9, "LATEFY": None}
COUNTS_2020 = {"listed": 151, "no_fx": 2, "same_isin": 4, "same_company": 16, "eligible": 129,
               "candidates": 128, "sectors": 19, "min_size_usd": 7.0,
               "sector_cut_usd": {"Cut31": 7000.0, "CutTwin": 17000.0}, "no_cost_cap": 4}
COUNTS_2019 = {"listed": 3, "no_fx": 0, "same_isin": 0, "same_company": 0, "eligible": 3, "candidates": 3,
               "sectors": 1, "min_size_usd": 1000.0, "sector_cut_usd": {}, "no_cost_cap": 1}
COMPANY = {"CIKBB": "CIKA", "NAMEBB": "NAMEA", "PUNBB": "PUNA", "ONECIKX": "ONECIK", "SAME2": "SAME2",
           "ROOT-PFA": "ROOT", "005935.KS": "005930.KS", "005940.KS": "005940.KS", "T01Z": "T01",
           "TWIN_B": "TWIN_A", "L200": "L200"}


class Warehouse:
    """Stands in for CetaResearch: runs each query on made-up warehouse tables."""

    def __init__(self, lines, fx):
        c = self.con = duckdb.connect()
        self.sent, self.problems, self.fail_next = [], [], 0
        c.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, sector VARCHAR, country VARCHAR,"
                  " currency VARCHAR, isin VARCHAR, cik VARCHAR, companyName VARCHAR, isActivelyTrading BOOLEAN,"
                  " isEtf BOOLEAN, isFund BOOLEAN, fetchedAtEpoch BIGINT)")
        c.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, marketCap DOUBLE,"
                  " reportedCurrency VARCHAR, dateEpoch UINTEGER)")
        c.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch UINTEGER, close DOUBLE,"
                  " adjClose DOUBLE, volume DOUBLE)")
        c.execute("CREATE TABLE forex_prices_daily(symbol VARCHAR, dateEpoch UINTEGER, close DOUBLE)")
        c.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate VARCHAR)")
        for ln in lines:
            for p in ln["profile_rows"] or [{"sector": ln["sector"], "etf": ln["etf"], "fetched": 100}]:
                c.execute("INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          [ln["symbol"], ln["exchange"], p["sector"], ln["country"], ln["currency"], ln["isin"],
                           ln["cik"], ln["name"], ln["live"], p["etf"], ln["fund"], p["fetched"]])
            for d, cap, ccy, period in ln["caps"]:
                c.execute("INSERT INTO key_metrics VALUES (?, ?, ?, ?, ?)",
                          [ln["symbol"], period, cap, ccy, ep(d) if d else None])
            for last, n, tv in ln["bars"]:   # n consecutive days ending on `last`, close 10
                c.execute("INSERT INTO stock_eod SELECT ?, CAST(CAST(? AS DATE) - CAST(i AS INTEGER) AS VARCHAR),"
                          " ? - i * 86400, 10.0, ?, ? FROM range(?) t(i)",
                          [ln["symbol"], last.isoformat(), ep(last), 10.0 * ln["adj"],
                           None if tv is None else tv / 10.0, n])
            for d in ln["zeros"]:
                c.execute("INSERT INTO stock_eod VALUES (?, ?, ?, 0.0, 0.0, 100.0)", [ln["symbol"], d.isoformat(), ep(d)])
            for (month, day), px in ln["entry"].items():
                d = date(2020, month, day)
                c.execute("INSERT INTO stock_eod VALUES (?, ?, ?, ?, ?, 100.0)", [ln["symbol"], d.isoformat(), ep(d), px, px])
        c.execute("INSERT INTO stock_eod VALUES ('L200', '2020-02-03', ?, 0.0, 0.0, 100.0)", [ep(date(2020, 2, 3))])
        for sym, d, rate in fx:
            c.execute("INSERT INTO forex_prices_daily VALUES (?, ?, ?)", [sym, ep(d), rate])

    def query(self, sql, format="json", **kw):
        self.sent.append(sql)
        literals = re.findall(r"'((?:[^']|'')*)'", sql)
        if ";" in sql or len(sql) > 50000 or any("[" in s or any(t in s for t in TABLES) for s in literals):
            self.problems.append(f"warehouse SQL breaks a platform rule: {sql[:120]!r}")
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("Rate limited. Retry after 1s")
        if format != "parquet":
            cur = self.con.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "result.parquet")
            self.con.execute(f"COPY ({sql}) TO '{out}' (FORMAT PARQUET)")
            if not self.con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]:
                return b""   # the empty result query_parquet turns into its placeholder table
            with open(out, "rb") as fh:
                return fh.read()


LINES, FX = fixture()
WH = Warehouse(LINES, FX)
WH_REVERSED = Warehouse(LINES[::-1], FX[::-1])   # the same rows, inserted in the opposite order


def load(src):
    mod = types.ModuleType("pairs_universe_under_test")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    mod.print = lambda *a, **k: None
    return mod


def run_leg(mod, wh, exchanges, top_n=TOP_N):
    """(con, fetch counts, per-year counts) of one leg, through the module's own fetches."""
    con = duckdb.connect()
    fetched = mod.fetch_universe(wh, con, exchanges, YEARS)
    if fetched is None:
        return con, None, None
    return con, fetched, mod.build_candidates(con, YEARS, OFFSET, MIN_PX, top_n, MIN_DAYS)


def book(con, year):
    """{sector: [symbols]} of a year's candidates."""
    out = {}
    for sector, symbol in con.execute("SELECT sector, symbol FROM candidates WHERE yr = ? ORDER BY 1, 2", [year]).fetchall():
        out.setdefault(sector, []).append(symbol)
    return out


def close(a, b):
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) <= 1e-9 * max(1.0, abs(b)))


def raises(fn, *args):
    try:
        fn(*args)
    except AssertionError:
        return True
    return False


def keys_checks(mod):
    """company_keys on its own: every link, and the same keys whatever the row order."""
    rows = [{"symbol": ln["symbol"], "isin": ln["isin"], "cik": ln["cik"], "companyName": ln["name"]}
            for ln in LINES if ln["exchange"] == "NYSE"]
    keys = mod.company_keys(rows)
    for sym, want in COMPANY.items():
        if keys.get(sym) != want:
            yield f"company key of {sym}: {keys.get(sym)} != {want}"
    if mod.company_keys(rows[::-1]) != keys or list(keys) != sorted(keys):
        yield "company keys depend on the row order"
    if mod.company_keys([{"symbol": "A", "isin": None, "cik": None, "companyName": None},
                         {"symbol": "B", "isin": " ", "cik": "", "companyName": "()"}]) != {"A": "A", "B": "B"}:
        yield "lines with no ISIN, cik or name were merged"


ANY_DATE = "FY filing of any date (not point-in-time)"
ANY_DATE_DOC = (re.compile(r"FY\s+filing\s+of\s+any\s+date"), re.compile(r"not\s+point-in-time"))


def says_any_date(doc):
    return all(p.search(doc or "") for p in ANY_DATE_DOC)


def notes_checks(mod):
    """The universe_rule every results JSON carries says the FY-filing test reads any date (LATEFY)."""
    if ANY_DATE not in mod.UNIVERSE_NOTES.get("rule", ""):
        yield f"UNIVERSE_NOTES rule does not say {ANY_DATE!r}"
    if "after the formation year" not in mod.UNIVERSE_NOTES.get("filing", ""):
        yield "UNIVERSE_NOTES has no filing note saying a later filing admits a line"
    if not says_any_date(mod.__doc__):
        yield "the module docstring does not say the FY-filing test reads any date"


def leg_checks(mod):
    """The NYSE leg end to end, then the other legs."""
    con, fetched, counts = run_leg(mod, WH, ["NYSE"])
    got20, got19 = book(con, 2020), book(con, 2019)
    for name, got, want in (("2020", got20, EXPECT_2020), ("2019", got19, EXPECT_2019)):
        for sector in sorted(set(got) | set(want), key=str):
            if got.get(sector) != want.get(sector):
                yield f"{name} candidates in {sector}: {got.get(sector)} != {want.get(sector)}"
    rows = {r[0]: r for r in con.execute("SELECT symbol, size_usd, cap_usd, company FROM candidates WHERE yr = 2020").fetchall()}
    for sym, want in SIZES.items():
        if sym in rows and not close(rows[sym][1], want):
            yield f"size_usd of {sym}: {rows[sym][1]} != {want}"
    for sym, want in CAPS.items():
        if sym in rows and not close(rows[sym][2], want):
            yield f"cap_usd of {sym}: {rows[sym][2]} != {want}"
    for year, want in ((2020, COUNTS_2020), (2019, COUNTS_2019)):
        for key in sorted(set(want) | set(counts[year])):
            if counts[year].get(key) != want.get(key):
                yield f"{year} count {key}: {counts[year].get(key)} != {want.get(key)}"
    universe = dict(con.execute("SELECT symbol, company FROM universe").fetchall())
    for sym, want in COMPANY.items():
        if universe.get(sym) != want:
            yield f"universe.company of {sym}: {universe.get(sym)} != {want}"
    cols = lambda t: [(r[0], r[1]) for r in con.execute(f"DESCRIBE {t}").fetchall()]
    if cols("candidates") != [("yr", "INTEGER"), ("symbol", "VARCHAR"), ("sector", "VARCHAR"),
                              ("company", "VARCHAR"), ("size_usd", "DOUBLE"), ("cap_usd", "DOUBLE")]:
        yield f"candidates columns: {cols('candidates')}"
    shared = {s for s, k in universe.items() if list(universe.values()).count(k) > 1}
    px = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM entry_px").fetchall()}
    if not px or not px <= shared or fetched["entry_px"] != con.execute("SELECT COUNT(*) FROM entry_px").fetchone()[0]:
        yield f"entry_px holds lines that share no company, or a wrong count: {sorted(px - shared)}"
    if con.execute("SELECT COUNT(*) FROM metrics_cache WHERE symbol = 'GOODF'").fetchone()[0] != 1:
        yield "a filing without a date was loaded, or GOODF's dated one was not"

    # check_pairs: two companies, both candidates of the year, a finite correlation
    if mod.check_pairs(con, 2020, [("L200", "LAST10", 0.9), ("NEW", "STAY", 0.71)]) != 0:
        yield "check_pairs did not pass two distinct companies"
    if not raises(mod.check_pairs, con, 2020, [("L200", "LAST10", float("nan"))]):
        yield "check_pairs let a NaN correlation through"
    if not raises(mod.check_pairs, con, 2020, [("L200", "LAST10", None)]):
        yield "check_pairs let a missing correlation through"
    if not raises(mod.check_pairs, con, 2020, [("L200", "L199", 0.9)]):
        yield "check_pairs let a leg outside the candidates through"
    if not raises(mod.check_pairs, con, 2019, [("L200", "LAST10", 0.9)]):
        yield "check_pairs let another year's candidates through"
    con.execute("INSERT INTO candidates VALUES (2020, 'CIKBB', 'Company', 'CIKA', 1.0, NULL)")
    if not raises(mod.check_pairs, con, 2020, [("L200", "LAST10", 0.9), ("CIKA", "CIKBB", 0.99)]):
        yield "check_pairs let an injected same-company pair through"
    con.execute("DELETE FROM candidates WHERE symbol = 'CIKBB'")

    # offset_days 0: the entry window opens on 1 January and ends on the 11th, nothing else moves
    at0 = mod.build_candidates(con, YEARS, 0, MIN_PX, TOP_N, MIN_DAYS)
    got0, want0 = book(con, 2020), {**EXPECT_2020, **EXPECT_2020_OFFSET0}
    for sector in sorted(set(got0) | set(want0), key=str):
        if got0.get(sector) != want0.get(sector):
            yield f"2020 candidates at offset 0 in {sector}: {got0.get(sector)} != {want0.get(sector)}"
    if at0 != {2020: COUNTS_2020, 2019: COUNTS_2019} or book(con, 2019) != EXPECT_2019:
        yield f"counts or the 2019 candidates at offset 0: {at0}"

    # equal sizes at a cut of one: the symbol decides in every sector
    mod.build_candidates(con, [2020], OFFSET, MIN_PX, 1, MIN_DAYS)
    ties = book(con, 2020)
    wrong = [f"Tie{k}" for k in range(1, 9) if ties.get(f"Tie{k}") != [f"TI{k}A"]]
    if wrong or ties.get("Cut31") != ["D01"] or ties.get("CutTwin") != ["T01"]:
        yield f"cut of one: {wrong} {ties.get('Cut31')} {ties.get('CutTwin')}"
    for bad_offset in (2, -1):
        try:
            mod.build_candidates(con, [2020], bad_offset, MIN_PX, TOP_N, MIN_DAYS)
            yield f"build_candidates accepted offset_days={bad_offset}, which entry_px does not cover"
        except ValueError:
            pass

    # fetch_prices: every candidate and benchmark, both date columns, no bar at or under zero
    counts = mod.build_candidates(con, YEARS, OFFSET, MIN_PX, TOP_N, MIN_DAYS)
    n = mod.fetch_prices(WH, con, None, ["SPY"])
    want = {s for syms in list(EXPECT_2020.values()) + list(EXPECT_2019.values()) for s in syms} | {"SPY"}
    have = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM prices_cache").fetchall()}
    if have != want or n != con.execute("SELECT COUNT(*) FROM prices_cache").fetchone()[0]:
        yield f"prices_cache symbols: extra {sorted(have - want)} missing {sorted(want - have)}, count {n}"
    if cols("prices_cache") != [("symbol", "VARCHAR"), ("trade_date", "DATE"), ("trade_epoch", "BIGINT"),
                                ("adjClose", "DOUBLE")]:
        yield f"prices_cache columns: {cols('prices_cache')}"
    bad = con.execute("SELECT COUNT(*) FROM prices_cache WHERE adjClose <= 0 OR trade_date < DATE '2004-01-01'"
                      " OR trade_epoch <> epoch(CAST(trade_date AS TIMESTAMP))").fetchone()[0]
    if bad:
        yield f"prices_cache: {bad} rows at or under zero, before 2004, or with a wrong epoch"
    from data_utils import get_prices
    if get_prices(con, ["TWIN_A", "TWIN_B"], T20, offset_days=1, with_epochs=True) != {
            "TWIN_B": (5.0, ep(date(2020, 1, 2)))}:
        yield "get_prices cannot read prices_cache on the epoch path"
    first = con.execute("SELECT symbol, trade_epoch, adjClose FROM prices_cache ORDER BY 1, 2, 3").fetchall()
    budget, mod.MAX_IN_CHARS, before = mod.MAX_IN_CHARS, 60, len(WH.sent)
    mod.fetch_prices(WH, con, sorted(want - {"SPY"}), ["SPY"])
    mod.MAX_IN_CHARS = budget
    if (len(WH.sent) - before < 10
            or con.execute("SELECT symbol, trade_epoch, adjClose FROM prices_cache ORDER BY 1, 2, 3").fetchall() != first):
        yield "a symbol list split over several queries loads different bars"

    # the campaign listing rule, sub-unit quotes, a leg with no rate, an empty leg
    for exchanges, want, size in ((["XETRA"], ["DE1.DE"], 1000.0), (["LSE"], ["GBX.L"], 100.0), (["SES"], [], None)):
        leg, _, leg_counts = run_leg(mod, WH, exchanges)
        got = leg.execute("SELECT symbol, size_usd FROM candidates WHERE yr = 2020 ORDER BY 1").fetchall()
        if [r[0] for r in got] != want or (want and not close(got[0][1], size)):
            yield f"{exchanges} leg: {got} != {want} at {size}"
        if exchanges == ["SES"] and leg_counts[2020]["no_fx"] != 1:
            yield f"SES leg: the line without a rate was not counted: {leg_counts[2020]}"
    if run_leg(mod, WH, ["JPX"])[1] is not None:
        yield "a leg with no line did not return None"
    try:
        mod.fetch_universe(WH, duckdb.connect(), None, YEARS)
        yield "fetch_universe accepted a missing exchange list"
    except ValueError:
        pass

    # a rate limit is waited out, not fatal
    slept = []
    mod.time = types.SimpleNamespace(sleep=slept.append)
    WH.fail_next = 1
    if run_leg(mod, WH, ["XETRA"])[1] is None or len(slept) != 1:
        yield f"a rate-limited fetch was not retried once: slept {slept}"

    # determinism: the same rows inserted in the opposite order
    other, _, other_counts = run_leg(mod, WH_REVERSED, ["NYSE"])
    dump = lambda c: c.execute("SELECT * FROM candidates ORDER BY yr, symbol").fetchall()
    if dump(other) != dump(con) or other_counts != counts:
        yield "the same rows in another order give different candidates"
    if dump(con) != con.execute("SELECT * FROM candidates").fetchall():
        yield "candidates are not stored in (yr, symbol) order"
    for problem in WH.problems + WH_REVERSED.problems:
        yield problem


def failures(src, first_only=False):
    out = []
    try:
        mod = load(src)
        for check in (notes_checks, keys_checks, leg_checks):
            for f in check(mod):
                out.append(f)
                if first_only:
                    return out
    except Exception as e:  # a mutant that breaks the module fails loudly
        out.append(f"error: {type(e).__name__}: {e}")
    finally:
        WH.fail_next, WH.problems, WH_REVERSED.problems = 0, [], []
    return out


def corr_fixture(order):
    """con with 2020 candidates and their 2019 bars: A1/A2 move together, FRZ never moves."""
    con = duckdb.connect()
    con.execute("CREATE TABLE candidates(yr INTEGER, symbol VARCHAR, sector VARCHAR, company VARCHAR,"
                " size_usd DOUBLE, cap_usd DOUBLE)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)")
    names = ["A1", "A2", "FRZ", "LONE"]
    bars = []
    for i in range(260):
        d = date(2019, 1, 1) + timedelta(days=i)
        wave = 100.0 + 10.0 * ((i * 7) % 13) / 13.0
        px = {"A1": wave, "A2": 2.0 * wave + (0.5 if i % 2 else 0.0), "FRZ": 50.0, "LONE": 80.0 + (i * 5) % 11}
        bars += [(s, d, ep(d), px[s]) for s in names]
    for s in names[::order]:
        con.execute("INSERT INTO candidates VALUES (2020, ?, 'Tech', ?, 1000.0, 5e9)", [s, s])
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)", bars[::order])
    return con


def topic_failures(topic, src):
    """A wired topic's correlation query: a frozen-price leg forms no pair, row order does not matter."""
    try:
        mod = types.ModuleType(topic.replace("-", "_") + "_under_test")
        mod.__file__ = os.path.join(ROOT, topic, "backtest.py")
        exec(compile(src, mod.__file__, "exec"), mod.__dict__)
        mod.print = lambda *a, **k: None
        pairs = [[(r[0], r[1]) for r in getattr(mod, TOPICS[topic])(corr_fixture(order), date(2019, 1, 1), F19_END)]
                 for order in (1, -1)]
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return [] if pairs == [[("A1", "A2")], [("A1", "A2")]] else [f"{topic} pairs {pairs} != [('A1', 'A2')] twice"]


NAN_GUARD = re.compile(r"\s+AND\s+NOT\s+isnan\(\s*correlation\s*\)", re.I)
HAS_FILING = "has_filing = \"symbol IN (SELECT k.symbol FROM key_metrics k WHERE k.period = 'FY' AND k.marketCap > 0)\""
BUYABLE = ("ORDER BY CASE WHEN px.adjClose > 0 AND px.adjClose >= {float(min_leg_price)} THEN 0 ELSE 1 END,\n"
           "                         LENGTH(d.symbol)")
ISIN_STEP = ('s["isin"] = isin_dedup_sql(s["sized"], t, offset_days, min_leg_price,\n'
             '                               window_days=ENTRY_WINDOW_DAYS, prices_table="entry_px")')
FX_WINDOW = "utc_epoch(end - timedelta(days=FX_MAX_STALE_DAYS)), utc_epoch(end)"
ENTRY_FETCH = "date(y, 1, 1), date(y, 1, 1) + timedelta(days=ENTRY_WINDOW_DAYS + 1)) for y in years)"
ENTRY_END = "utc_epoch(first + timedelta(days=ENTRY_WINDOW_DAYS))"
ISIN_OFFSET = 'isin_dedup_sql(s["sized"], t, offset_days, min_leg_price,'
MUTANTS = {
    # a: listed through the formation year
    "200 bars not enough": ("b.n_days >= ", "b.n_days > "),
    "no last-bar test": ("\n          AND b.last_epoch >= {last_ok}", ""),
    "last bar 11 days before year end allowed": ("LISTED_GRACE_DAYS = 10 ", "LISTED_GRACE_DAYS = 11 "),
    "last bar 10 days before year end refused": ("LISTED_GRACE_DAYS = 10 ", "LISTED_GRACE_DAYS = 9 "),
    "last-bar bound exclusive": ("b.last_epoch >= ", "b.last_epoch > "),
    "formation year is the trading year": ("fy, t = year - 1, date(year, 1, 1)", "fy, t = year, date(year, 1, 1)"),
    "formation year two back": ("fy, t = year - 1, date(year, 1, 1)", "fy, t = year - 2, date(year, 1, 1)"),
    "bars at 0.0 are formation bars": ("-12-31' AND adjClose > 0", "-12-31'"),
    # b: traded
    "no traded test": ("\n          AND b.med_tv > 0", ""),
    "missing volume skipped, not zero": ("MEDIAN(COALESCE(close * volume, 0))", "MEDIAN(close * volume)"),
    # one list per year, no isActivelyTrading
    "isActivelyTrading re-added": ("FROM profile p\n            WHERE {profile_where}",
                                   "FROM profile p\n            WHERE {profile_where} AND p.isActivelyTrading = true"),
    "one list built and reused": ("candidates_sql(y, offset_days", "candidates_sql(years[0], offset_days"),
    # the static universe
    "no FY-filing test": (HAS_FILING, 'has_filing = "1=1"'),
    "FY-filing test point-in-time (a filing after the years refused)": (
        HAS_FILING, HAS_FILING.replace("k.marketCap > 0)", "k.marketCap > 0 AND k.dateEpoch <= 1577836800)")),
    "rule hides the any-date filing test": (
        '"rule": "per formation year: lines with a sector and an FY filing of any date (not point-in-time), "',
        '"rule": "per formation year: lines with a sector and an FY filing, "'),
    "no filing note": ('    "filing": "operating-company test:', '    "_filing": "operating-company test:'),
    "docstring hides the any-date filing test": (
        "The operating-company test is not point-in-time: a line needs an FY filing of any date, so a line\n"
        "whose first filing is dated years after a formation year is still a candidate in it.\n", ""),
    "a quarterly filing counts": ("WHERE k.period = 'FY' AND k.marketCap > 0)", "WHERE k.marketCap > 0)"),
    "a zero cap counts": ("k.period = 'FY' AND k.marketCap > 0)", "k.period = 'FY')"),
    "funds kept": ("\n          AND COALESCE(isEtf, false) = false AND COALESCE(isFund, false) = false", ""),
    "lines with no sector kept": ("\n          AND sector IS NOT NULL AND sector <> ''", ""),
    "oldest profile row": ("ORDER BY p.fetchedAtEpoch DESC NULLS LAST", "ORDER BY p.fetchedAtEpoch ASC NULLS LAST"),
    "undated filing loaded": (" AND dateEpoch IS NOT NULL\n", "\n"),
    "listing rule dropped": ("listing = campaign_listing_condition(exchanges)", 'listing = ""'),
    # f: the cut, after d and e
    "dedup after the cut": ('isin_dedup_sql(s["sized"], t,', 'isin_dedup_sql(_cut_sql(s["sized"], top_n), t,'),
    # stands in for "no tie-break": DuckDB's own order among equals changes from run to run
    "cut ties in hash order": ("ORDER BY c.size_usd DESC, c.symbol) AS rnk", "ORDER BY c.size_usd DESC, hash(c.symbol)) AS rnk"),
    "cut tie-break reversed":("ORDER BY c.size_usd DESC, c.symbol) AS rnk", "ORDER BY c.size_usd DESC, c.symbol DESC) AS rnk"),
    "cut keeps the smallest": ("ORDER BY c.size_usd DESC, c.symbol", "ORDER BY c.size_usd, c.symbol"),
    "cut one line wider": ("WHERE rnk <= {int(top_n)}", "WHERE rnk <= {int(top_n) + 1}"),
    # d: same ISIN
    "no ISIN dedup": (ISIN_STEP, 's["isin"] = s["sized"]'),
    "ISIN dedup always at offset 0": (ISIN_OFFSET, ISIN_OFFSET.replace("offset_days", "0")),
    "ISIN dedup always at offset 1": (ISIN_OFFSET, ISIN_OFFSET.replace("offset_days", "1")),
    # e: company links and the pick
    "ISIN link removed": ("for group in by_isin.values():\n        union(group)", "for group in by_isin.values():\n        pass"),
    "cik link removed": ("for group in by_cik.values():\n        union(group)", "for group in by_cik.values():\n        pass"),
    "name link removed": ("            union([s for s, _ in group])", "            pass"),
    "root link removed": ("for group in by_root.values():\n        union(group)", "for group in by_root.values():\n        pass"),
    "names compared when both carry a cik": ("if not all(has_cik for _, has_cik in group):", "if True:"),
    "cik zero padding kept": ('.strip().lstrip("0")', ".strip()"),
    ".KS class digit kept": ("root = root[:5]", "root = root[:6]"),
    "root not cut at the hyphen": ('root = base.split("-")[0]', "root = base"),
    "parenthesised text kept in names": ('re.sub(r"\\([^)]*\\)", " ", str(name or "").upper())', 'str(name or "").upper()'),
    "punctuation kept in names": ('re.sub(r"[\\W_]+", " ", ', 're.sub(r"\\s+", " ", '),
    "no company dedup": ('FROM ({_cut_sql(s["company"], top_n)}) r', 'FROM ({_cut_sql(s["isin"], top_n)}) r'),
    "pick ignores buyable at entry": (BUYABLE, "ORDER BY LENGTH(d.symbol)"),
    "pick: size before symbol length": ("LENGTH(d.symbol), d.size_usd DESC, d.symbol",
                                        "d.size_usd DESC, LENGTH(d.symbol), d.symbol"),
    "pick: smaller size first": ("LENGTH(d.symbol), d.size_usd DESC,", "LENGTH(d.symbol), d.size_usd,"),
    "pick: symbol tie-break reversed": ("d.size_usd DESC, d.symbol) AS _co_rn", "d.size_usd DESC, d.symbol DESC) AS _co_rn"),
    "pick: entry floor exclusive": ("px.adjClose >= {float(min_leg_price)}", "px.adjClose > {float(min_leg_price)}"),
    "entry window a day longer": ("ENTRY_WINDOW_DAYS = 10 ", "ENTRY_WINDOW_DAYS = 11 "),
    "entry window a day shorter": ("ENTRY_WINDOW_DAYS = 10 ", "ENTRY_WINDOW_DAYS = 9 "),
    "entry window ignores the offset": ("first = date(year, 1, 1) + timedelta(days=offset_days)", "first = date(year, 1, 1)"),
    "pick always at offset 1": ("_entry_bar_sql(year, offset_days)}) px", "_entry_bar_sql(year, 1)}) px"),
    "entry window ends on 12 January at any offset": (ENTRY_END, ENTRY_END.replace("first", "date(year, 1, 2)")),
    "any offset accepted": ("if offset_days not in (0, 1):", "if False:"),
    "entry bars fetched from 2 January": (ENTRY_FETCH, ENTRY_FETCH.replace("date(y, 1, 1), ", "date(y, 1, 2), ")),
    "entry bars fetched to 11 January": (ENTRY_FETCH, ENTRY_FETCH.replace("DAYS + 1)", "DAYS)")),
    "entry bars at 0.0 loaded": ("({in_list}) AND adjClose > 0 AND ({wins})", "({in_list}) AND ({wins})"),
    # c: size in USD
    "size from adjClose": ("COALESCE(close * volume, 0)", "COALESCE(adjClose * volume, 0)"),
    "sub-unit factor dropped": ("rates[major] / unit", "rates[major]"),
    "rate dated after formation end": (FX_WINDOW, FX_WINDOW.replace("utc_epoch(end)", "utc_epoch(end + timedelta(days=30))")),
    "rate of 1 January used": (FX_WINDOW, FX_WINDOW.replace("utc_epoch(end)", "utc_epoch(end + timedelta(days=1))")),
    "rate of 31 December refused": (FX_WINDOW, FX_WINDOW.replace("utc_epoch(end)", "utc_epoch(end - timedelta(days=1))")),
    "rate 8 days old allowed": ("FX_MAX_STALE_DAYS = 7 ", "FX_MAX_STALE_DAYS = 8 "),
    "rate 7 days old refused": ("FX_MAX_STALE_DAYS = 7 ", "FX_MAX_STALE_DAYS = 6 "),
    "oldest rate in the window": ("ORDER BY fx_epoch DESC, per_usd DESC", "ORDER BY fx_epoch ASC, per_usd DESC"),
    "zero FX close accepted": (" AND close > 0 AND ({wins})", " AND ({wins})"),
    "line without a rate kept": ("WHERE size_usd IS NOT NULL", "WHERE TRUE"),
    "size left in the quote currency": ("b.med_tv / f.per_usd AS size_usd", "b.med_tv AS size_usd"),
    # the cost cap
    "cap filing 44 days old used": ("COST_CAP_LAG_DAYS = 45 ", "COST_CAP_LAG_DAYS = 44 "),
    "cap filing 45 days old ignored": ("COST_CAP_LAG_DAYS = 45 ", "COST_CAP_LAG_DAYS = 46 "),
    "cap filing 541 days old used": ("COST_CAP_MAX_AGE_DAYS = 540 ", "COST_CAP_MAX_AGE_DAYS = 541 "),
    "cap filing 540 days old ignored": ("COST_CAP_MAX_AGE_DAYS = 540 ", "COST_CAP_MAX_AGE_DAYS = 539 "),
    "cap not converted": ("marketCap / per_usd AS cap_usd", "marketCap AS cap_usd"),
    "cap from the oldest filing": ("ORDER BY m.filing_epoch DESC, m.marketCap DESC NULLS LAST",
                                   "ORDER BY m.filing_epoch ASC, m.marketCap DESC NULLS LAST"),
    "cap tie takes the smaller": ("m.marketCap DESC NULLS LAST,", "m.marketCap ASC NULLS LAST,"),
    # pair guards and prices
    "same-company pair passes": ("if company[a] == company[b]:", "if False:"),
    "NaN correlation passes": ("if corr is None or not math.isfinite(corr):", "if corr is None:"),
    "leg outside the candidates passes": ("if a not in company or b not in company:",
                                          "if a in company and b in company and company[a] == company[b]:"),
    "any year's candidates pass": ('FROM candidates WHERE yr = ?", [int(year)]).fetchall())',
                                   'FROM candidates WHERE yr >= ?", [int(year) - 9]).fetchall())'),
    "zero-price bars loaded": ("AND date >= '{PRICES_FROM}' AND adjClose > 0", "AND date >= '{PRICES_FROM}'"),
    "prices before 2004 loaded": (" AND date >= '{PRICES_FROM}'", ""),
    "no retry on a rate limit": ("if attempt == 3 or not wait:", "if True:"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_pairs_universe: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_pairs_universe: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new), first_only=True):
            survived.append(name)
    if survived:
        print("test_pairs_universe: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    wired, pending = 0, []
    for topic in TOPICS:
        with open(os.path.join(ROOT, topic, "backtest.py")) as fh:
            src = fh.read()
        if "pairs_universe" not in src:
            pending.append(topic)
            continue
        wired += 1
        problems = topic_failures(topic, src)
        if not problems and not says_any_date(src.split('"""')[1]):
            problems = ["the module docstring does not say the FY-filing test reads any date"]
        if not problems and not NAN_GUARD.search(src):
            problems = ["the correlation query has no 'AND NOT isnan(correlation)' guard to mutate"]
        if not problems and not topic_failures(topic, NAN_GUARD.sub("", src)):
            problems = ["mutant survived: NaN guard removed"]
        if problems:
            print(f"test_pairs_universe: FAIL ({topic}:", problems, ")")
            sys.exit(1)
    for topic in pending:
        print(f"test_pairs_universe: SKIP {topic} correlation checks (not wired to pairs_universe yet)")
    print(f"test_pairs_universe: PASS ({len(LINES)} lines, {len(MUTANTS)} mutants caught,"
          f" {wired}/{len(TOPICS)} topics wired)")


if __name__ == "__main__":
    main()
