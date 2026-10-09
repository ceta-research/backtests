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

# ---- 3. remote path: tail query + the same oscillation filter the caches get ----
con = cache([("SPIKE", ep(date(2010, 1, 4)), 10.0), ("CLEAN", ep(date(2010, 1, 4)), 10.0),
             ("GONE", ep(date(2010, 1, 4)), 10.0)])
tail = [
    # SPIKE: 10 -> 10.2 -> 31 (phantom, reverts next day, after the exit date) -> 10.1
    {"symbol": "SPIKE", "dateEpoch": ep(date(2010, 1, 4)), "adjClose": 10.0},
    {"symbol": "SPIKE", "dateEpoch": ep(date(2010, 3, 30)), "adjClose": 10.2},
    {"symbol": "SPIKE", "dateEpoch": ep(date(2010, 3, 31)), "adjClose": 31.0},
    {"symbol": "SPIKE", "dateEpoch": ep(date(2010, 4, 5)), "adjClose": 10.1},   # context only (after hi)
    {"symbol": "CLEAN", "dateEpoch": ep(date(2010, 1, 4)), "adjClose": 10.0},
    {"symbol": "CLEAN", "dateEpoch": ep(date(2010, 2, 26)), "adjClose": 9.0},
    {"symbol": "CLEAN", "dateEpoch": ep(date(2010, 4, 6)), "adjClose": 77.0},   # context only, never a price
    {"symbol": "GONE", "dateEpoch": ep(date(2010, 1, 4)), "adjClose": 10.0},    # entry bar only
]
client = FakeClient(tail)
ltp = LtpExits(client, con)
out = ltp.fill(["SPIKE", "CLEAN", "GONE"], {"SPIKE": 10.0, "CLEAN": 10.0, "GONE": 10.0}, {},
               date(2010, 1, 1), date(2010, 4, 1), offset_days=1)
assert out == {"SPIKE": 10.2, "CLEAN": 9.0}, out
assert ltp.osc_removed == 1 and ltp.provenance()["oscillation_rows_removed"] == 1
assert "stock_eod" in client.last_sql and "rn <=" in client.last_sql
assert not con.execute("SELECT 1 FROM information_schema.tables WHERE table_name = '_ltp_tail'").fetchall()

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
