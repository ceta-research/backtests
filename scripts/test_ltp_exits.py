#!/usr/bin/env python3
"""Offline checks for data_utils.LtpExits (fake client, no network)."""
import os
import sys
from datetime import date, datetime, timezone

import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import data_utils
from data_utils import LtpExits, LTP_DELIST_GRACE_DAYS

DELISTED = [{"symbol": "DLST", "delistedDate": "2010-02-01"},
            {"symbol": "OLD", "delistedDate": "2001-05-01"}]


def ep(d, hour=0):
    return int(datetime(d.year, d.month, d.day, hour, tzinfo=timezone.utc).timestamp())


class FakeClient:
    """Serves the delisting table, then whatever tail rows the test sets."""

    def __init__(self, tail=None, fail_first=0):
        self.tail, self.fail_first, self.calls = tail or [], fail_first, 0

    def query(self, sql, **kw):
        self.calls += 1
        if self.calls <= self.fail_first:
            raise RuntimeError("transient")
        if "delisted_companies" in sql:
            return DELISTED
        self.last_sql = sql
        return self.tail


def cache(bars):
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)", bars)
    return con


# ---- 1. bounds, local path (the topic's own cache) ----
bars = [
    ("ENTRYONLY", date(2010, 1, 4), 10.0),                       # only the entry bar: must stay dropped
    ("DLST", date(2010, 1, 4), 10.0), ("DLST", date(2010, 2, 1), 6.0),
    ("DLST", date(2010, 2, 1 + LTP_DELIST_GRACE_DAYS), 5.0),       # inside grace
    ("DLST", date(2010, 3, 1), 50.0),                             # after grace (ticker reuse): excluded
    ("OLD", date(2010, 1, 4), 20.0), ("OLD", date(2010, 3, 15), 22.0),  # delisting predates entry: ignored
    ("LATE", date(2010, 1, 4), 30.0), ("LATE", date(2010, 3, 31), 33.0),
    ("LATE", date(2010, 4, 2), 99.0),                             # after exit date: excluded
    ("EDGE", date(2010, 1, 4), 40.0), ("EDGE", date(2010, 4, 1), 44.0),  # bar ON the exit date: included
]
con = cache([(s, ep(d), p) for s, d, p in bars])
ltp = LtpExits(FakeClient(), con, local=True)
syms = ["ENTRYONLY", "DLST", "OLD", "LATE", "EDGE", "NOENTRY"]
entry = {"ENTRYONLY": 10.0, "DLST": 10.0, "OLD": 20.0, "LATE": 30.0, "EDGE": 40.0}
out = ltp.fill(syms, entry, {}, date(2010, 1, 1), date(2010, 4, 1), offset_days=1)

assert out == {"DLST": 5.0, "OLD": 22.0, "LATE": 33.0, "EDGE": 44.0}, out
assert ltp.missing == 5 and ltp.filled == 4, (ltp.missing, ltp.filled)
assert all(f["entry_bar"] < f["last_bar"] <= f["exit_date"] for f in ltp.fills)
kinds = {f["symbol"]: f["delisting_bound"] for f in ltp.fills}
assert kinds == {"DLST": "applied", "OLD": "ignored_predates_entry", "LATE": "none", "EDGE": "none"}, kinds
prov = ltp.provenance()
assert prov["filled_without_delisting_row"] == 2 and prov["filled_ignoring_delisting_row"] == 1, prov
assert ltp.fill(["DLST"], entry, {"DLST": 7.0}, date(2010, 1, 1), date(2010, 4, 1)) == {}  # priced exit untouched

# ---- 2. epochs that are not UTC midnight: the entry bar must still never come back ----
con = cache([("X", ep(date(2010, 1, 4), 14), 10.0),              # entry bar stamped 14:00 UTC
             ("Y", ep(date(2010, 1, 4), 14), 10.0), ("Y", ep(date(2010, 4, 1), 21), 12.0)])  # late on exit day
ltp = LtpExits(FakeClient(), con, local=True)
out = ltp.fill(["X", "Y"], {"X": 10.0, "Y": 10.0}, {}, date(2010, 1, 1), date(2010, 4, 1), offset_days=1)
assert out == {"Y": 12.0}, out

# ---- 3. remote path: the real query against a local stock_eod, then the oscillation filter ----
class SqlClient(FakeClient):
    """Runs the remote SQL against a local fake stock_eod, so the query itself is tested."""

    def __init__(self, rows, delisted=None, date_type="VARCHAR"):
        super().__init__()
        self.delisted = delisted if delisted is not None else DELISTED
        self.db = duckdb.connect()
        self.db.execute(f"CREATE TABLE stock_eod(symbol VARCHAR, date {date_type}, dateEpoch BIGINT, adjClose DOUBLE)")
        self.db.executemany("INSERT INTO stock_eod VALUES (?, ?, ?, ?)", [(s, d.isoformat(), ep(d), p) for s, d, p in rows])

    def query(self, sql, **kw):
        self.calls += 1
        if "delisted_companies" in sql:
            return self.delisted
        cur = self.db.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def bdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d = date.fromordinal(d.toordinal() + 1)


E, X = date(2010, 1, 1), date(2010, 4, 1)
eod = [("SPIKE", date(2010, 1, 4), 10.0), ("SPIKE", date(2010, 3, 30), 10.2),
       ("SPIKE", date(2010, 3, 31), 31.0),                       # phantom, reverts after the exit date
       ("SPIKE", date(2010, 4, 5), 10.1),                        # context only (after hi)
       ("CLEAN", date(2010, 1, 4), 10.0), ("CLEAN", date(2010, 2, 26), 9.0),
       ("CLEAN", date(2010, 4, 6), 77.0),                        # context only, never a price
       ("GONE", date(2010, 1, 4), 10.0)]                         # entry bar only
con = cache([(s, ep(d), p) for s, d, p in eod if d == date(2010, 1, 4)])
for dt in ("VARCHAR", "DATE"):
    ltp = LtpExits(SqlClient(eod, delisted=[], date_type=dt), con)
    out = ltp.fill(["SPIKE", "CLEAN", "GONE"], {"SPIKE": 10.0, "CLEAN": 10.0, "GONE": 10.0}, {}, E, X, offset_days=1)
    assert out == {"SPIKE": 10.2, "CLEAN": 9.0}, (dt, out)
    assert ltp.osc_removed == 1 and ltp.provenance()["oscillation_rows_removed"] == 1
assert not con.execute("SELECT 1 FROM information_schema.tables WHERE table_name = '_ltp_tail'").fetchall()

# ---- 3b. a genuine final crash survives: no bars past a delisting bound reach the filter ----
crash = [("CRASH", d, 10.0) for d in bdays(date(2010, 1, 4), date(2010, 1, 28))] + [("CRASH", date(2010, 1, 29), 3.0)]
reuse = [("CRASH", d, 10.0) for d in bdays(date(2010, 2, 9), date(2010, 2, 12))]   # ticker reused near the old level
con = cache([("CRASH", ep(date(2010, 1, 4)), 10.0)])
ltp = LtpExits(SqlClient(crash + reuse, delisted=[{"symbol": "CRASH", "delistedDate": "2010-02-01"}]), con)
assert ltp.fill(["CRASH"], {"CRASH": 10.0}, {}, E, X, offset_days=1) == {"CRASH": 3.0}

# ---- 3c. tier 2 works on the remote path: it fetches the holding period, not a few bars ----
osc = [("OSC", d, 13.5 if (i % 15 == 7 or d == date(2010, 3, 30)) else 10.0)
       for i, d in enumerate(bdays(date(2009, 6, 1), date(2010, 3, 30)))] + [("OSC", date(2010, 4, 2), 10.0)]
con = cache([("OSC", ep(d), p) for s, d, p in osc if d >= date(2010, 1, 4)])
ltp = LtpExits(SqlClient(osc, delisted=[]), con)
assert ltp.fill(["OSC"], {"OSC": 10.0}, {}, E, X, offset_days=1) == {"OSC": 10.0}   # not the 13.5 phantom

# ---- 3d. delisted a few days before the entry bar: the bound still applies, no reused-ticker exit ----
con = cache([("REUSE", ep(date(2010, 1, 4)), 0.50), ("REUSE", ep(date(2010, 1, 5)), 0.50),
             ("REUSE", ep(date(2010, 3, 15)), 1.20), ("REUSE", ep(date(2010, 3, 31)), 1.20)])
ltp = LtpExits(FakeClient(), con, local=True)
ltp.delisted = {"REUSE": date(2010, 1, 1)}
assert ltp.fill(["REUSE"], {"REUSE": 0.50}, {}, E, X, offset_days=1) == {"REUSE": 0.50}
assert ltp.fills[0]["delisting_bound"] == "applied"

# ---- 3e. a second print on the entry day is never the exit ----
con = cache([("DUP", ep(date(2010, 1, 4)), 10.0), ("DUP", ep(date(2010, 1, 4), 14), 10.4)])
assert LtpExits(FakeClient(), con, local=True).fill(["DUP"], {"DUP": 10.0}, {}, E, X, offset_days=1) == {}

# ---- 4. date-schema cache: get_prices(with_epochs) still yields an entry bar ----
con = duckdb.connect()
con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, adjClose DOUBLE)")
con.execute("INSERT INTO prices_cache VALUES ('D', DATE '2010-01-04', 10.0)")
ltp = LtpExits(FakeClient([{"symbol": "D", "dateEpoch": ep(date(2010, 1, 4)), "adjClose": 10.0},
                           {"symbol": "D", "dateEpoch": ep(date(2010, 2, 10)), "adjClose": 8.0}]), con)
assert ltp.fill(["D"], {"D": 10.0}, {}, date(2010, 1, 1), date(2010, 4, 1), offset_days=1) == {"D": 8.0}

# ---- 5. a transient failure on the delisting load is retried ----
data_utils_sleep = __import__("time").sleep
__import__("time").sleep = lambda s: None
try:
    c = FakeClient(fail_first=2)
    ltp = LtpExits(c, duckdb.connect())
    assert ltp.delisted["DLST"] == date(2010, 2, 1) and c.calls == 3
finally:
    __import__("time").sleep = data_utils_sleep

print("test_ltp_exits: PASS")
