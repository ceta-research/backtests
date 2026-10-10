#!/usr/bin/env python3
"""Offline checks for data_utils.LtpExits (fake clients, no network). Checks survive python -O."""
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_utils import LtpExits, LTP_DELIST_GRACE_DAYS, remove_price_oscillations


def check(cond, what):
    if not cond:
        raise SystemExit(f"FAIL: {what}")


def ep(d, hour=0):
    return int(datetime(d.year, d.month, d.day, hour, tzinfo=timezone.utc).timestamp())


def bdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


class FakeClient:
    """Serves the delisting table; fails the first `fail_first` calls."""

    def __init__(self, delisted=None, fail_first=0):
        self.delisted = delisted or []
        self.fail_first, self.calls = fail_first, 0

    def query(self, sql, **kw):
        self.calls += 1
        if self.calls <= self.fail_first:
            raise RuntimeError("transient")
        if "delisted_companies" in sql:
            return self.delisted
        return []


class SqlClient(FakeClient):
    """Runs the remote SQL against a local fake stock_eod, so the query itself is tested."""

    def __init__(self, rows, delisted=None, date_type="VARCHAR"):
        super().__init__(delisted)
        self.db = duckdb.connect()
        self.db.execute(f"CREATE TABLE stock_eod(symbol VARCHAR, date {date_type}, dateEpoch BIGINT, adjClose DOUBLE)")
        self.db.executemany("INSERT INTO stock_eod VALUES (?, ?, ?, ?)",
                            [(s, d.isoformat(), ep(d), p) for s, d, p in rows])

    def query(self, sql, **kw):
        self.calls += 1
        if "delisted_companies" in sql:
            return self.delisted
        cur = self.db.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def cache(rows):
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)", rows)
    return con


E, X = date(2010, 1, 1), date(2010, 4, 1)     # entry / exit dates; offset_days=1 -> entry bar 2010-01-04
DLST = [{"symbol": "DLST", "delistedDate": "2010-02-01"}, {"symbol": "OLD", "delistedDate": "2001-05-01"}]

# ---- 1. bounds, local path ----
con = cache([(s, ep(d), p) for s, d, p in [
    ("ENTRYONLY", date(2010, 1, 4), 10.0),                               # only the entry bar: stays dropped
    ("DLST", date(2010, 1, 4), 10.0), ("DLST", date(2010, 2, 1), 6.0),
    ("DLST", date(2010, 2, 1 + LTP_DELIST_GRACE_DAYS), 5.0),               # inside grace (remote: see below)
    ("OLD", date(2010, 1, 4), 20.0), ("OLD", date(2010, 3, 15), 22.0),    # delisting long before entry: ignored
    ("LATE", date(2010, 1, 4), 30.0), ("LATE", date(2010, 3, 31), 33.0),
    ("LATE", date(2010, 4, 2), 99.0),                                     # after the exit date: excluded
    ("EDGE", date(2010, 1, 4), 40.0), ("EDGE", date(2010, 4, 1), 44.0)]])  # ON the exit date: included
ltp = LtpExits(SqlClient([("DLST", date(2010, 2, 1), 6.0), ("DLST", date(2010, 2, 8), 5.0),
                          ("DLST", date(2010, 3, 1), 50.0)], delisted=DLST), con, local=True)
out = ltp.fill(["ENTRYONLY", "DLST", "OLD", "LATE", "EDGE", "NOENTRY"],
               {"ENTRYONLY": 10.0, "DLST": 10.0, "OLD": 20.0, "LATE": 30.0, "EDGE": 40.0}, {}, E, X, offset_days=1)
check(out == {"DLST": 5.0, "OLD": 22.0, "LATE": 33.0, "EDGE": 44.0}, f"local bounds {out}")
check(ltp.missing == 5 and ltp.filled == 4, "local counts")
kinds = {f["symbol"]: f["delisting_bound"] for f in ltp.fills}
check(kinds == {"DLST": "applied", "OLD": "ignored_predates_entry", "LATE": "none", "EDGE": "none"}, f"kinds {kinds}")
prov = ltp.provenance()
check(prov["filled_without_delisting_row"] == 2 and prov["filled_ignoring_delisting_row"] == 1, "provenance")
check([f["symbol"] for f in ltp.fills] == ["DLST", "OLD", "LATE", "EDGE"], "fills in caller order")
check(ltp.fill(["DLST"], {"DLST": 10.0}, {"DLST": 7.0}, E, X) == {}, "priced exit untouched")

# ---- 2. epochs off UTC midnight: no print from the entry day comes back; the exit day counts ----
con = cache([("X", ep(date(2010, 1, 4), 14), 10.0),
             ("Y", ep(date(2010, 1, 4), 14), 10.0), ("Y", ep(date(2010, 4, 1), 21), 12.0),
             ("DUP", ep(date(2010, 1, 4)), 10.0), ("DUP", ep(date(2010, 1, 4), 14), 10.4)])
out = LtpExits(FakeClient(), con, local=True).fill(["X", "Y", "DUP"], {"X": 10.0, "Y": 10.0, "DUP": 10.0}, {},
                                                   E, X, offset_days=1)
check(out == {"Y": 12.0}, f"entry-day prints / late exit-day bar {out}")

# ---- 3. remote path: the real query on a local stock_eod, whatever the date column type ----
eod = [("A", date(2010, 1, 4), 10.0), ("A", date(2010, 1, 4), 10.0),     # duplicate entry-day bar
       ("A", date(2010, 3, 31), 9.0),
       ("EDGE", date(2010, 1, 4), 10.0), ("EDGE", date(2010, 4, 1), 11.0),   # bar ON the exit date
       ("LATE", date(2010, 1, 4), 10.0), ("LATE", date(2010, 4, 2), 99.0),   # only after the exit date
       ("GONE", date(2010, 1, 4), 10.0),                                    # entry bar only
       ("L8", date(2010, 1, 5), 10.0)]          # entry bar a day later than the others, nothing after it:
                                                # only the per-name epoch bound (not the date prefilter) drops it
for dt in ("VARCHAR", "DATE", "TIMESTAMP"):
    con = cache([(s, ep(d), p) for s, d, p in eod if d <= date(2010, 1, 5)])
    out = LtpExits(SqlClient(eod, date_type=dt), con).fill(
        ["A", "EDGE", "LATE", "GONE", "L8"], {s: 10.0 for s in ("A", "EDGE", "LATE", "GONE", "L8")}, {}, E, X,
        offset_days=1)
    check(out == {"A": 9.0, "EDGE": 11.0}, f"remote picks, date {dt}: {out}")

# ---- 3b. a genuine final crash survives a reused ticker after the delisting (remote, and local=True) ----
crash = [("CRASH", d, 10.0) for d in bdays(date(2010, 1, 4), date(2010, 1, 28))] + [("CRASH", date(2010, 1, 29), 3.0)]
reuse = [("CRASH", d, 10.0) for d in bdays(date(2010, 2, 9), date(2010, 3, 31))]
dl = [{"symbol": "CRASH", "delistedDate": "2010-02-01"}]
out = LtpExits(SqlClient(crash + reuse, delisted=dl), cache([("CRASH", ep(date(2010, 1, 4)), 10.0)])).fill(
    ["CRASH"], {"CRASH": 10.0}, {}, E, X, offset_days=1)
check(out == {"CRASH": 3.0}, f"remote crash {out}")
# local=True: the topic's own filter over its whole cache deletes the crash, so applied names go remote
full = cache([(s, ep(d), p) for s, d, p in crash + reuse])
remove_price_oscillations(full, verbose=False)
filtered_last = full.execute(f"SELECT adjClose FROM prices_cache WHERE trade_epoch <= {ep(date(2010, 2, 8))} "
                             "ORDER BY trade_epoch DESC LIMIT 1").fetchone()[0]
out = LtpExits(SqlClient(crash + reuse, delisted=dl), full, local=True).fill(["CRASH"], {"CRASH": 10.0}, {}, E, X,
                                                                              offset_days=1)
check(filtered_last == 10.0 and out == {"CRASH": 3.0}, f"local crash: cache says {filtered_last}, fill {out}")

# ---- 3c. delisting dates around the entry bar ----
for dd, expect, kind in [(date(2010, 1, 1), {"R": 0.5}, "applied"),         # 3 days before: bound applies
                         (date(2009, 12, 15), {}, None),                   # 20 days before: window closed
                         (date(2009, 12, 3), {"R": 1.2}, "ignored_predates_entry")]:   # 32 days: earlier holder
    con = cache([("R", ep(date(2010, 1, 4)), 0.5), ("R", ep(date(2010, 1, 5)), 0.5),
                 ("R", ep(date(2010, 3, 15)), 1.2), ("R", ep(date(2010, 3, 31)), 1.2)])
    ltp = LtpExits(SqlClient([("R", date(2010, 1, 5), 0.5), ("R", date(2010, 3, 31), 1.2)],
                             delisted=[{"symbol": "R", "delistedDate": dd.isoformat()}]), con, local=True)
    out = ltp.fill(["R"], {"R": 0.5}, {}, E, X, offset_days=1)
    check(out == expect, f"delisted {dd}: {out}")
    check((ltp.fills[0]["delisting_bound"] if ltp.fills else None) == kind, f"delisted {dd}: kind")
    check(ltp.closed == (1 if not expect else 0), f"delisted {dd}: closed count {ltp.closed}")

# ---- 4. date-schema cache: get_prices(with_epochs) still yields an entry bar ----
con = duckdb.connect()
con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, adjClose DOUBLE)")
con.execute("INSERT INTO prices_cache VALUES ('D', DATE '2010-01-04', 10.0)")
out = LtpExits(SqlClient([("D", date(2010, 1, 4), 10.0), ("D", date(2010, 2, 10), 8.0)]), con).fill(
    ["D"], {"D": 10.0}, {}, E, X, offset_days=1)
check(out == {"D": 8.0}, f"date schema {out}")

# ---- 5. retries: a transient failure is retried, a bad query is not ----
sleep = time.sleep
time.sleep = lambda s: None
try:
    c = FakeClient(DLST, fail_first=2)
    check(LtpExits(c, duckdb.connect()).delisted["DLST"] == date(2010, 2, 1) and c.calls == 3, "transient retried")

    class BadSql(FakeClient):
        def query(self, sql, **kw):
            self.calls += 1
            raise RuntimeError("[SYNTAX_ERROR] SQL syntax error")
    b = BadSql()
    try:
        LtpExits(b, duckdb.connect())
    except RuntimeError:
        pass
    check(b.calls == 1, f"bad query retried {b.calls} times")
finally:
    time.sleep = sleep

print("test_ltp_exits: PASS")
