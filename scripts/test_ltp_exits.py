#!/usr/bin/env python3
"""Offline checks for data_utils.LtpExits bounds (local-cache path, fake client)."""
import os
import sys
from datetime import date, datetime, timezone

import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_utils import LtpExits, LTP_DELIST_GRACE_DAYS


class FakeClient:
    def query(self, sql, **kw):
        return [{"symbol": "DLST", "delistedDate": "2010-02-01"},
                {"symbol": "OLD", "delistedDate": "2001-05-01"}]


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


con = duckdb.connect()
con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
bars = [
    ("ENTRYONLY", date(2010, 1, 4), 10.0),                       # only the entry bar: must stay dropped
    ("DLST", date(2010, 1, 4), 10.0), ("DLST", date(2010, 2, 1), 6.0),
    ("DLST", date(2010, 2, 1 + LTP_DELIST_GRACE_DAYS), 5.0),       # inside grace
    ("DLST", date(2010, 3, 1), 50.0),                             # after grace (ticker reuse): excluded
    ("OLD", date(2010, 1, 4), 20.0), ("OLD", date(2010, 3, 15), 22.0),  # delisting predates entry: ignored
    ("LATE", date(2010, 1, 4), 30.0), ("LATE", date(2010, 3, 31), 33.0),
    ("LATE", date(2010, 4, 2), 99.0),                             # after exit date: excluded
]
con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)", [(s, ep(d), p) for s, d, p in bars])

ltp = LtpExits(FakeClient(), con, local=True)
syms = ["ENTRYONLY", "DLST", "OLD", "LATE", "NOENTRY"]
entry = {"ENTRYONLY": 10.0, "DLST": 10.0, "OLD": 20.0, "LATE": 30.0}
out = ltp.fill(syms, entry, {}, date(2010, 1, 1), date(2010, 4, 1), offset_days=1)

assert out == {"DLST": 5.0, "OLD": 22.0, "LATE": 33.0}, out
assert ltp.missing == 4 and ltp.filled == 3, (ltp.missing, ltp.filled)
assert all(f["entry_bar"] < f["last_bar"] <= f["exit_date"] for f in ltp.fills)
assert ltp.fill(["DLST"], entry, {"DLST": 7.0}, date(2010, 1, 1), date(2010, 4, 1)) == {}  # priced exit untouched
print("test_ltp_exits: PASS")
