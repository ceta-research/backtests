#!/usr/bin/env python3
"""Offline tests for data checks dc-1 (no network, no backtest leg).

T (traded bars), N (NVDR twins), M (market-cap check), F (factor flags, detection only) and L
(large-move list) on crafted rows shaped like the measured cases (EASTW-R.BK, TRUE.BK 2006, the
JNB 2012-01-02 holiday, NSE/SAU/SET day counts, ^FTSE, RAM.BK, the SET twins, SOS, ZEST, HAO,
GRPN, CIM, 3003.SR, TSG.JO, 2603.TW, SALEE.BK), plus an exit lost to a closed day, a name halted
after entry (keeps its LTP fill), large moves on LTP fills, and duplicate stock_eod rows under
check M. The review of the first same-day runs added: M on 7211.T (k first), 4541.T (a delisting
record left out of the split products), 0653.HK FY2011 to FY2013 and 1124.HK (lowered, not
excluded), BBD-A.TO, ERIC-A.ST and 02826K.KS (turnover over the share classes), Bonjour FY2007 to
FY2009 (the consolidations' gate), CETX, TOPS and PIXY (no turnover), with ZEST, HAO and 8038.T
staying out; T on flat no-volume lines (1114.HK, BC88.L, GBGLOBAL.NS, 76ID.L, BR.BK against
TRUE.BK and ADVANC-R.BK), 600694.SS's halted exit and ELUX-A.ST's 2006 entry off ELUX-B's scale.
A second review added: share classes need two shared FY caps (1789.HK and 1935.HK share one); M
lowers, not excludes, when only the consolidations open the gate or a sibling has no bars (ERIC-A.ST
and ERIC-B.ST FY1999, 02826K.KS FY2008, 0760.HK FY2006); BC88.L's stale quote that steps once; and
T's sibling step on ELUX's real rows (2007 keeps its rows), on lines that must not move (LG Chem's
pref, Samsung Heavy's pref, BP-B.L, 1935.HK), three classes, the liquid line, the leading run of off
rows and the trigger. A third review added: Thai NVDR feeds that tick between two prices keep their
rows (BH-R.BK, RATCH-R.BK, MCS-R.BK), and a failing row whose divisor is 1 is 'unchecked', not a
silent 'stands' (HAO FY2025). A fourth review added: a date with no close is skipped, not read as a
change, and changes are counted within one window (NUL.L, XW.L, GAP.BK); M's divisor edges (1.25
lowers, unknown turnover with a divisor of 1 is 'unchecked', a k-only divisor lowers); and an
'unchecked' row on the synthetic leg (A07.BK FY2010) keeps FMP's cap at the floors. Then both
opted-in topics run end to end on a synthetic SET leg through a fake client, in drop, ltp, both and
uncapped modes, and the results pass the LTP gate's stage-a comparison and the results scanners,
with and without --full-holdings.

Shared code: against the base commit (BASE, the last one before data checks), isin_dedup_sql
(twin_key=False) returns the same SQL, LtpExits(traded_only=False) sends the same SQL and builds
the same temp table, filter_returns' default gives the same output, and every other function,
class and constant of data_utils is unchanged (ast). Each "must fail" check is also run on a
mutant and has to fail: clean_price_cache stubbed (KIO books +299.5%), cap_check empty (SOS and
ZEST pass the floor), F's detection stubbed, and gate 15 on mutated topic sources (opted-in
topics with their wiring cut, capped topics with a None cap in any form, an uncapped results file).

Usage: python scripts/test_data_checks.py [--base REF]
"""
import argparse
import ast
import contextlib
import copy
import importlib.util
import io
import json
import math
import os
import random
import statistics
import subprocess
import sys
import tempfile
from datetime import date, timedelta

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import data_utils as du   # noqa: E402
from data_utils import (DATA_CHECKS_V1, DataChecks, clean_price_cache, mark_nvdr_twins,  # noqa: E402
                        isin_dedup_sql, nvdr_twins_sql, apply_cap_rule, cap_check_sql, empty_cap_check,
                        filter_returns, get_prices, get_benchmark_return, remove_price_oscillations,
                        utc_epoch as ep, CAP_INPUTS_DDL)
from gate_ltp import check_stage_a, has_ltp_keys, strip_ltp, first_diff   # noqa: E402
import scan_results_invariant as sri   # noqa: E402
from verify_floor_guard import dc_wiring, dc_uses, dc_results_problem, DATA_CHECKS_TOPICS   # noqa: E402
from test_floor_guard import NO_RETURN_CAP   # noqa: E402

BASE = "e1ecb7c"   # backtests main before data checks


def check(cond, what):
    if not cond:
        raise SystemExit(f"FAIL: {what}")


def weekdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def cache(rows):
    """prices_cache as the topics build it: (symbol, date, adjClose, close, volume) rows."""
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?, ?)", [(s, ep(d), a, c, v) for s, d, a, c, v in rows])
    con.execute("CREATE INDEX idx_prices_sym_epoch ON prices_cache(symbol, trade_epoch)")
    return con


def no_clean(con, *_a, **_k):
    """Mutant: check T removed (the cache stays as fetched)."""
    con.execute("CREATE OR REPLACE TABLE prices_dropped(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE,"
                " close DOUBLE, volume DOUBLE, reason VARCHAR, window_start BIGINT)")
    return {"rows_moved": {"closed_day": 0, "zero_volume": 0, "nonpositive_price": 0},
            "symbol_windows_without_volume": 0, "closed_day_test_skipped": 0, "closed_dates": []}


def book(con, sym, d, x, offset=1):
    """Entry, exit and the return filter_returns books with the cap off (the topics' call)."""
    e = get_prices(con, [sym], d, offset_days=offset).get(sym)
    xp = get_prices(con, [sym], x, offset_days=offset).get(sym)
    clean, _ = filter_returns([(sym, e, xp, 1e10)], min_entry_price=1.0, max_single_return=None)
    return e, xp, (clean[0][1] if clean else None)


def fillers(n, days, traded=lambda i, d: True, px=50.0, prefix="F"):
    return [(f"{prefix}{i:03d}", d, px, px, 1000.0 if traded(i, d) else 0.0) for i in range(n) for d in days]


# ---------------------------------------------------------------- T: traded bars

def eastw_failures(clean=clean_price_cache):
    """EASTW-R.BK, SET 2013 Q1: two zero-volume stale rows open the window (2.74/2.73 against 8.25).
    T moves 01-01 (a closed day: 3 of 40 traded) and 01-02 (zero volume of a line that traded)."""
    d, x, s = date(2013, 1, 1), date(2013, 4, 1), "EASTW-R.BK"
    days_e, days_x = list(weekdays(d, d + timedelta(days=11))), list(weekdays(x, x + timedelta(days=11)))
    rows = fillers(40, days_e + days_x, traded=lambda i, day: day != d or i < 3)
    rows += [(s, date(2013, 1, 1), 2.74, 3.7, 0.0), (s, date(2013, 1, 2), 2.73, 3.7, 0.0)]
    rows += [(s, day, 8.25, 13.0, 413900.0) for day in days_e if day >= date(2013, 1, 3)]
    rows += [(s, day, 9.51 if day == x else 9.58, 15.1, 150000.0) for day in days_x]
    con = cache(rows)
    st = clean(con, {"'^SET.BK'"}, [d, x])
    remove_price_oscillations(con, verbose=False)
    e, xp, r = book(con, s, d, x)
    f = []
    if r is None or abs(r - (9.58 / 8.25 - 1)) > 1e-9:
        f.append(f"EASTW books {r} (entry {e}), want +16.1% on the 01-03 entry")
    if st["closed_dates"] != [{"date": "2013-01-01", "traded": 3, "ref": 41.0}]:
        f.append(f"closed dates {st['closed_dates']}")
    dc = DataChecks("on")
    con.execute("CREATE OR REPLACE TABLE splits(symbol VARCHAR, split_epoch BIGINT, numerator DOUBLE, denominator DOUBLE)")
    dc.prices(con, d, x, [(s, e, xp, 1e10)], 1, 1.0)
    ev = [v for v in dc.events if v["check"] == "entry_bar"]
    want = {"date": "2013-01-02", "adjClose": 2.73, "close": 3.7, "volume": 0.0, "reason": "zero_volume"}
    if not (len(ev) == 1 and ev[0]["first_bar"] == want and ev[0]["used_bar"]["date"] == "2013-01-03"
            and ev[0]["used_bar"]["adjClose"] == 8.25):
        f.append(f"entry_bar event {ev}")
    if dc.row(d)["entry_bars_moved"] != 1:
        f.append(f"period row {dc.row(d)}")
    return f


def feed_failures():
    """TRUE.BK 2006: a line with volume 0 on every row and moving prices keeps every row (counted)."""
    d = date(2006, 1, 1)
    days = list(weekdays(d, d + timedelta(days=11)))
    pxs = [5.98, 6.16, 6.40, 6.28, 6.75, 6.80, 6.70, 6.90, 7.00]
    con = cache(fillers(30, days) + [("TRUE.BK", day, p, p, 0.0) for day, p in zip(days, pxs)])
    st = clean_price_cache(con, set(), [d])
    n = con.execute("SELECT COUNT(*) FROM prices_cache WHERE symbol = 'TRUE.BK'").fetchone()[0]
    f = []
    if n != len(days):
        f.append(f"TRUE.BK keeps {n} of {len(days)} rows")
    if st["symbol_windows_without_volume"] != 1 or sum(st["rows_moved"].values()):
        f.append(f"stats {st}")
    if get_prices(con, ["TRUE.BK"], d, offset_days=1).get("TRUE.BK") != 5.98:
        f.append("TRUE.BK entry is its first row in the window")
    return f


def kio_failures(clean=clean_price_cache):
    """JNB 2011 Q4: on 2012-01-02 8 of 100 lines traded (neighbours 95). KIO.JO's row that day reads
    adjClose 49,422.39 at volume 0 against 14,826.95 the day after. T closes the day; KIO exits on
    01-03 (+19.9%). Without T it books +299.5%. ABG.JO traded that day (one of the 8) and moves too;
    ONLYD.JO has rows only on that day, so its entry is lost."""
    d, x = date(2011, 10, 1), date(2012, 1, 1)
    D = date(2012, 1, 2)
    days_e, days_x = list(weekdays(d, d + timedelta(days=11))), list(weekdays(x, x + timedelta(days=11)))
    rows = fillers(98, days_e + days_x, traded=lambda i, day: (i < 7) if day == D else (i < 93))
    kio_e = [12370.08, 11888.7, 12255.8, 12384.36, 12532.35, 13014.29, 12962.58, 13134.28]
    rows += [("KIO.JO", day, p, p * 3.5, 500000.0) for day, p in zip(days_e, kio_e)]
    rows += [("KIO.JO", D, 49422.39, 50000.0, 0.0)]
    kio_x = [14826.95, 14984.08, 14855.52, 15040.93, 15100.0, 15200.0, 15150.0, 15300.0]
    rows += [("KIO.JO", day, p, p * 3.5, 300000.0) for day, p in zip([v for v in days_x if v > D], kio_x)]
    rows += [("ABG.JO", day, 13300.0, 13400.0, 900000.0) for day in days_e]
    rows += [("ABG.JO", D, 13977.42, 14100.0, 406612.0)]
    rows += [("ABG.JO", day, 14176.65, 14300.0, 666751.0) for day in days_x if day > D]
    rows += [("ONLYD.JO", D, 20.0, 20.0, 0.0), ("ONLYD.JO", D + timedelta(days=-90), 20.0, 20.0, 1.0)]
    con = cache(rows)
    st = clean(con, set(), [d, x])
    remove_price_oscillations(con, verbose=False)
    e, xp, r = book(con, "KIO.JO", d, x)
    f = []
    if r is None or abs(r - (14826.95 / 12370.08 - 1)) > 1e-9:
        f.append(f"KIO books {None if r is None else round(r * 100, 1)}%, want +19.9%")
    if st["closed_dates"] != [{"date": "2012-01-02", "traded": 8, "ref": 95.0}]:
        f.append(f"closed dates {st['closed_dates']}")
    if get_prices(con, ["ABG.JO"], x, offset_days=1).get("ABG.JO") != 14176.65:
        f.append("ABG.JO's traded 2012-01-02 row moves with the closed day")
    dc = DataChecks("on")
    con.execute("CREATE OR REPLACE TABLE splits(symbol VARCHAR, split_epoch BIGINT, numerator DOUBLE, denominator DOUBLE)")
    sd = [("ONLYD.JO", *[get_prices(con, ["ONLYD.JO"], t, offset_days=1).get("ONLYD.JO") for t in (x, date(2012, 4, 1))], 1e10)]
    dc.prices(con, x, date(2012, 4, 1), sd, 1, 1.0, held=False)
    if [v["check"] for v in dc.events] != ["entry_lost"] or dc.row(x)["entries_lost"] != 1:
        f.append(f"ONLYD.JO entry_lost: {dc.events}")
    return f


def exit_lost_failures():
    """GONE's only exit-window row is a traded row on a closed day (4 of 44 lines trade on 2013-07-02).
    T empties its exit window: the event and the counts say the exit was lost, not moved."""
    d, x, D = date(2013, 4, 1), date(2013, 7, 1), date(2013, 7, 2)
    days_e, days_x = list(weekdays(d, d + timedelta(days=11))), list(weekdays(x, x + timedelta(days=11)))
    rows = fillers(43, days_e + days_x, traded=lambda i, day: day != D or i < 3)
    rows += [("GONE", day, 10.0, 10.0, 100.0) for day in days_e] + [("GONE", D, 12.0, 12.0, 100.0)]
    con = cache(rows)
    st = clean_price_cache(con, set(), [d, x])
    con.execute("CREATE TABLE splits(symbol VARCHAR, split_epoch BIGINT, numerator DOUBLE, denominator DOUBLE)")
    e, xp = (get_prices(con, ["GONE"], t, offset_days=1).get("GONE") for t in (d, x))
    f = []
    if [c["date"] for c in st["closed_dates"]] != ["2013-07-02"] or e != 10.0 or xp is not None:
        f.append(f"GONE setup: closed {st['closed_dates']}, entry {e}, exit {xp}")
    dc = DataChecks("on")
    dc.prices(con, d, x, [("GONE", e, xp, 1e10)], 1, 1.0)
    got = [(v["check"], v["side"], v.get("first_bar", {}).get("reason")) for v in dc.events]
    if got != [("exit_lost", "exit", "closed_day")]:
        f.append(f"GONE events {got}, want one exit_lost")
    row, c = dc.row(d), dc.block()["counts"]
    if (row.get("exits_lost"), row["exit_bars_moved"], c.get("exits_lost"), c["exit_bars_moved"]) != (1, 0, 1, 0):
        f.append(f"GONE counts: row {row}, block exits_lost {c.get('exits_lost')} exit_bars_moved {c['exit_bars_moved']}")
    return f


def closed_shape_failures():
    """One statistic for every day shape: closed when traded < 0.5 x the median of the other days."""
    f = []

    def window(n_syms, day_traded, d=date(2019, 10, 1), n_days=None):
        days = list(weekdays(d, d + timedelta(days=11)))[:n_days]
        rows = []
        for day in days:
            k = day_traded.get(day, n_syms)
            rows += [(f"S{i:04d}", day, 10.0, 10.0, 1.0 if i < k else 0.0) for i in range(n_syms)]
        return days, cache(rows)

    # NSE 2019-10-02: a copied-volume holiday set, 25% of the rows, all of them traded
    days, con = window(160, {})
    con.execute("DELETE FROM prices_cache WHERE trade_epoch = ? AND symbol >= 'S0040'", [ep(date(2019, 10, 2))])
    st = clean_price_cache(con, set(), [date(2019, 10, 1)])
    if [c["date"] for c in st["closed_dates"]] != ["2019-10-02"]:
        f.append(f"NSE holiday set: {st['closed_dates']}")
    for share, label in ((0.65, "SAU Sunday"), (0.83, "SET 2013-01-02")):
        days, con = window(186, {date(2019, 10, 2): int(186 * share)})
        st = clean_price_cache(con, set(), [date(2019, 10, 1)])
        if st["closed_dates"] or st["rows_moved"]["closed_day"]:
            f.append(f"{label} ({share} of the reference) must stay open: {st['closed_dates']}")
    days, con = window(15, {date(2019, 10, 2): 1})
    st = clean_price_cache(con, set(), [date(2019, 10, 1)])
    if st["closed_dates"] or st["closed_day_test_skipped"] != 1:
        f.append(f"ref under 20: skipped and counted, got {st}")
    days, con = window(50, {date(2019, 10, 2): 1}, n_days=3)
    st = clean_price_cache(con, set(), [date(2019, 10, 1)])
    if st["closed_dates"] or st["closed_day_test_skipped"] != 1:
        f.append(f"fewer than 3 other dates: skipped and counted, got {st}")
    return f


def bench_failures():
    """^FTSE prints volume 0 on most January 2013 rows: benchmark rows are never touched."""
    d, x = date(2013, 1, 1), date(2013, 4, 1)
    days = list(weekdays(d, d + timedelta(days=11))) + list(weekdays(x, x + timedelta(days=11)))
    rows = fillers(40, days, traded=lambda i, day: day != date(2013, 1, 2) or i < 2)
    rows += [("^FTSE", day, 6000.0 + 10 * k, 6000.0 + 10 * k, 0.0) for k, day in enumerate(days)]
    raw = cache(rows)
    con = cache(rows)
    st = clean_price_cache(con, {"'^FTSE'", "'SPY'"}, [d, x])
    f = []
    n = con.execute("SELECT COUNT(*) FROM prices_cache WHERE symbol = '^FTSE'").fetchone()[0]
    if n != len(days) or con.execute("SELECT COUNT(*) FROM prices_dropped WHERE symbol = '^FTSE'").fetchone()[0]:
        f.append(f"^FTSE keeps {n} of {len(days)} rows")
    if not st["closed_dates"]:
        f.append("the closed day still closes for the other lines")
    if get_benchmark_return(con, "^FTSE", d, x, offset_days=1) != get_benchmark_return(raw, "^FTSE", d, x, offset_days=1):
        f.append("benchmark return changed")
    return f


def ram_failures():
    """RAM.BK, SET 2013 Q3: zero-volume rows at factor 0.1822 between traded rows at 0.8332. With T
    the oscillation filter deletes no traded row and the entry is the first traded row."""
    d = date(2013, 7, 1)
    days = list(weekdays(d, d + timedelta(days=11)))
    traded = {date(2013, 7, 1), date(2013, 7, 4), date(2013, 7, 9), date(2013, 7, 12)}
    rows = fillers(30, days)
    rows += [("RAM.BK", day, 83.32 if day in traded else 18.22, 100.0,
              5000.0 if day in traded else 0.0) for day in days]
    f = []
    con = cache(rows)
    clean_price_cache(con, set(), [d])
    out = remove_price_oscillations(con, verbose=False)
    left = con.execute("SELECT trade_epoch FROM prices_cache WHERE symbol = 'RAM.BK' ORDER BY 1").fetchall()
    if [r[0] for r in left] != sorted(ep(t) for t in traded) or out["rows_removed"]:
        f.append(f"RAM.BK after T: {len(left)} rows, filter removed {out['rows_removed']}")
    if get_prices(con, ["RAM.BK"], d, offset_days=1).get("RAM.BK") != 83.32:
        f.append("RAM.BK entry is its first traded row (07-04)")
    con = cache(rows)
    remove_price_oscillations(con, verbose=False)
    if get_prices(con, ["RAM.BK"], d, offset_days=1).get("RAM.BK") == 83.32:
        f.append("without T the entry should differ: the case would test nothing")
    return f


# ---------------------------------------------------------------- N: NVDR twins

TWIN_MAP = [("KBANK.BK", "TH0016010009"), ("KBANK-R.BK", "TH0016010R14"),     # both traded: ordinary wins
            ("BBL.BK", "TH0001010006"), ("BBL-R.BK", "TH0001010R14"),         # BBL.BK has no bar: stand-in
            ("SF.BK", "TH0123450002"), ("SF-R.BK", "TH0123451X15"),           # character 10 is not R
            ("U-R.BK", "TH9999990R10"), ("RABBIT.BK", "TH9999990009"),        # U.BK not in the leg
            ("XYZ.BK", "TH5555550001"), ("XYZ-R.BK", "TH7777770R12")]         # ISIN prefix differs


def twin_failures():
    con = duckdb.connect()
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    con.executemany("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", TWIN_MAP)
    con.execute("INSERT INTO isin_map VALUES ('KBANK.BK', 'TH0016010009', TRUE, TRUE)")   # duplicated row
    ranked = ["KBANK-R.BK", "KBANK.BK", "BBL-R.BK", "BBL.BK", "SF-R.BK", "SF.BK", "U-R.BK", "RABBIT.BK",
              "XYZ.BK", "XYZ-R.BK"] + [f"Z{i:02d}.BK" for i in range(11, 32)]
    con.execute("CREATE TABLE cands(symbol VARCHAR, score DOUBLE)")
    con.executemany("INSERT INTO cands VALUES (?, ?)", [(s, 100.0 - i) for i, s in enumerate(ranked)])
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE, volume DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, 50, 50, 100)",
                    [(s, ep(date(2013, 1, 3))) for s in ranked if s != "BBL.BK"])
    f = []
    linked = mark_nvdr_twins(con)
    if linked != 2:
        f.append(f"{linked} NVDR lines linked, want 2 (KBANK-R, BBL-R)")
    d = date(2013, 1, 1)
    top = lambda twin: [r[0] for r in con.execute(
        f"SELECT symbol FROM ({isin_dedup_sql('SELECT symbol, score FROM cands', d, 1, 1.0, twin_key=twin)})"
        " ORDER BY score DESC, symbol LIMIT 30").fetchall()]
    on, off = top(True), top(False)
    if "KBANK-R.BK" in on or "KBANK.BK" not in on:
        f.append("KBANK.BK (ordinary) wins over its NVDR")
    if "BBL-R.BK" not in on or "BBL.BK" in on:
        f.append("BBL-R.BK stands in when BBL.BK has no bar")
    for pair in (("SF.BK", "SF-R.BK"), ("U-R.BK", "RABBIT.BK"), ("XYZ.BK", "XYZ-R.BK")):
        if not set(pair) <= set(on):
            f.append(f"{pair} keep their own keys")
    if "Z31.BK" not in on or "Z31.BK" in off:
        f.append("the collapsed twins free their slots for ranks 31 and 32")
    if not {"KBANK.BK", "KBANK-R.BK", "BBL-R.BK"} <= set(off):
        f.append("twin_key=False keeps both lines (the old key)")
    pairs = sorted(con.execute(nvdr_twins_sql("SELECT symbol, score FROM cands", d, 1, 1.0)).fetchall())
    if pairs != [("BBL-R.BK", "BBL.BK", True), ("KBANK.BK", "KBANK-R.BK", False)]:
        f.append(f"twin pairs {pairs}")
    dc = DataChecks("on")
    dc.twins(con, "SELECT symbol, score FROM cands WHERE score > ?", [0], d, 1, 1.0, on)
    got = sorted((e["symbol"], e["merged"], e["reason"]) for e in dc.events)
    if got != [("BBL-R.BK", "BBL.BK", "stand_in"), ("KBANK.BK", "KBANK-R.BK", "ordinary_first")]:
        f.append(f"twin events {got}")
    return f


# ---------------------------------------------------------------- M: market-cap check

# symbol, pf_raw, pf_splits, n_traded, u, eps, net income, shares -> want rule, divisor
CAP_CASES = [
    ("SOS", 750.0, None, 20, 1.9e-4, 124.0, 1.47e6, 593233.0, "cap_over_k", 50.0),   # k 50 disagrees with pf 750
    ("ZEST", 150.0, None, 20, 2.0e-5, -1.12, -11.407e6, 10.202e6, "cap_over_pf", 150.0),   # lowered, still out
    ("HAO", 25.0, None, 20, 1.6e-5, 10.0, 1e8, 1e8, "ineligible", None),   # k 10 disagrees with pf 25, u fails
    ("GRPN", 20.0, None, 20, 0.015, 1.0, 1e8, 1e8, "stands", None),
    ("CIM", 3.0, None, 20, 0.01, 0.275, 1e8, 1e8, "stands", None),
    ("THIN1", 30.0, None, 9, 1e-6, 1.0, 1e8, 1e8, "cap_over_pf", 30.0),    # under 10 traded bars: u unknown, lowered
    ("THIN8", 30.0, None, 9, 1e-6, 8.0, 1e8, 1e8, "cap_over_pf", 30.0),
    ("SPLIT", None, 40.0, 20, 1e-5, 1.0, 1e8, 1e8, "ineligible", None),    # no raw price: no divisor; strict, u fails
    ("NOPF", None, None, 20, 1e-6, 50.0, 1e8, 1e8, "stands", None),        # no pf: the cap stands
    ("LOWPF", 2.99, None, 20, 1e-6, 50.0, 1e8, 1e8, "stands", None),
    ("SMALLEPS", 30.0, None, 20, 0.01, 0.04, 1e8, 1e10, "stands", None),   # |eps| < 0.05: no k
    ("SMALLNI", 30.0, None, 20, 0.01, 5.0, 9e5, 1e8, "stands", None),      # |net income| < 1M: no k
    ("SIGNS", 30.0, None, 20, 0.01, -5.0, 1e8, 1e8, "stands", None),       # signs disagree: no k
    ("STILL", 10.0, None, 20, 1e-8, 1.0, 1e8, 1e8, "ineligible", None),    # 8038.T shape: fails on cap / 10 too
]


def cap_insert(con, rows, table="cap_inputs"):
    """Insert dict rows by column name; a column the table lacks (an older schema) is left out."""
    cols = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}
    for r in rows:
        keys = [k for k in r if k in cols]
        con.execute(f"INSERT INTO {table} ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
                    [r[k] for k in keys])


def cap_rows(con):
    """cap_check as {symbol: row dict}."""
    cur = con.execute("SELECT * FROM cap_check")
    names = [c[0] for c in cur.description]
    return {r[0]: dict(zip(names, r)) for r in cur.fetchall()}


def cap_used_of(r):
    """The cap a floor reads for one cap_check row (cap_used_sql)."""
    div = r.get("cap_div", r.get("k_div"))
    return None if r["rule"] == "ineligible" else r["market_cap"] / (div or 1.0)


def cap_rule_failures():
    con = duckdb.connect()
    con.execute(CAP_INPUTS_DDL)
    cap = 2e9
    cap_insert(con, [{"symbol": s, "filing_epoch": 1000 + i, "market_cap": cap, "net_income": ni, "eps": eps,
                      "shares": sh, "n_bars": 20, "n_traded": nt, "turnover": u * cap, "pf_raw": pfr, "pf_splits": pfs,
                      "pf_cons": pfr or pfs, "pf_fwd": 1.0 if (pfr or pfs) else None}
                     for i, (s, pfr, pfs, nt, u, eps, ni, sh, _, _) in enumerate(CAP_CASES)])
    stats = apply_cap_rule(con)
    got = cap_rows(con)
    f = []
    for s, *_, rule, kd in CAP_CASES:
        r = got.get(s)
        div = r and r["cap_div"]
        if r is None or r["rule"] != rule or (kd is None) != (div is None) or (kd and abs(div - kd) > 0.1):
            f.append(f"{s}: {r and (r['rule'], div)}, want {rule} {kd}")
    if got["SPLIT"]["pf_source"] != "splits" or got["SOS"]["pf_source"] != "raw":
        f.append("pf_source")
    if stats != {"cap_rows_checked": len(CAP_CASES), "cap_rows_flagged": 7, "cap_unchecked": []}:
        f.append(f"stats {stats}")
    return f


def cap_source():
    """FMP tables shaped like the server's, for SOS's FY2019 inputs as measured on the warehouse."""
    src = duckdb.connect()
    src.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, date VARCHAR, dateEpoch BIGINT,"
                " returnOnEquity DOUBLE, returnOnAssets DOUBLE, marketCap DOUBLE)")
    src.execute("CREATE TABLE income_statement(symbol VARCHAR, period VARCHAR, date VARCHAR, dateEpoch BIGINT,"
                " revenue DOUBLE, netIncome DOUBLE, eps DOUBLE, weightedAverageShsOut DOUBLE, filingDate VARCHAR)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    src.execute("CREATE TABLE stock_prices_non_split_adjusted_daily(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT,"
                " adjClose DOUBLE, volume DOUBLE)")
    src.execute("CREATE TABLE splits_calendar(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT, numerator DOUBLE,"
                " denominator DOUBLE)")
    return src


def cap_sql_failures():
    """cap_check_sql on a local copy: SOS FY2019 gives pf 750 and k 50; 20 of 30 bars; median of traded."""
    src = cap_source()
    fy = date(2019, 12, 31)
    src.execute("INSERT INTO key_metrics VALUES ('SOS', 'FY', '2019-12-31', ?, 0.29, 0.1, 1641773434.5)", [ep(fy)])
    src.execute("INSERT INTO key_metrics VALUES ('SOS', 'FY', '2019-12-31', ?, 0.29, 0.1, 1.0e9)", [ep(fy)])  # dup row
    src.execute("INSERT INTO income_statement VALUES ('SOS', 'FY', '2019-12-31', ?, 1, 1470000, 124, 593233,"
                " '2020-06-15')", [ep(fy)])
    days = [fy - timedelta(days=k) for k in range(30)]
    vols = [0.0 if k in (1, 3) else (329.8 if k % 2 else 112.0) for k in range(30)]
    for day, vol in zip(days, vols):
        src.execute("INSERT INTO stock_eod VALUES ('SOS', ?, ?, 2767.5, 2767.5, ?)", [day.isoformat(), ep(day), vol])
        src.execute("INSERT INTO stock_prices_non_split_adjusted_daily VALUES ('SOS', ?, ?, 3.69, 1)",
                    [day.isoformat(), ep(day)])
    src.execute("INSERT INTO splits_calendar VALUES ('SOS', '2020-08-01', ?, 1, 15)", [ep(date(2020, 8, 1))])
    # a period end on a lookback-block boundary still sees the 20 bars before it
    look = DATA_CHECKS_V1["cap"]["lookback_days"] * 86400
    fe2 = (ep(date(2015, 6, 1)) // look) * look
    src.execute("INSERT INTO key_metrics VALUES ('EDGE', 'FY', 'x', ?, 0.2, 0.1, 5e9)", [fe2])
    for k in range(25):
        e2 = fe2 - 86400 * k
        src.execute("INSERT INTO stock_eod VALUES ('EDGE', ?, ?, 10, 10, 1e6)", [du.utc_date(e2).isoformat(), e2])
    rows = src.execute(cap_check_sql("symbol IN ('SOS', 'EDGE')", 1e9, [("2015-02-01", "2015-06-01"),
                                                                          ("2019-11-16", "2019-12-31")])).fetchall()
    got = {r[0]: r for r in rows}
    f = []
    s = got.get("SOS")
    if s is None or s[2] != 1641773434.5 or s[6] != 20 or s[7] != 18 or s[9] != 750.0 or s[10] != 15.0:
        f.append(f"SOS inputs {s}")
    elif abs(s[5] * s[4] / s[3] - 50.04) > 0.01 or \
            abs(s[8] - statistics.median(v * 2767.5 for v in vols[:20] if v > 0)) > 1e-6:
        f.append(f"SOS k or median turnover {s}")
    if got.get("EDGE") is None or got["EDGE"][6] != 20:
        f.append(f"EDGE bars {got.get('EDGE')}")
    # duplicate (symbol, date) rows count once: an exact copy, and an added zero-volume copy
    for s in ("CLEAN", "DUPT", "DUPZ"):
        src.execute("INSERT INTO key_metrics VALUES (?, 'FY', '2019-12-31', ?, 0.2, 0.1, 1e10)", [s, ep(fy)])
    for k, day in enumerate(days):
        vol = 1e4 * (k + 1)   # turnover grows with age, so a window cut short lowers the median
        for s, extra in (("CLEAN", []), ("DUPT", [vol]), ("DUPZ", [0.0])):
            for v in [vol] + extra:
                src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, 10, 10, ?)", [s, day.isoformat(), ep(day), v])
    rows = src.execute(cap_check_sql("symbol IN ('CLEAN', 'DUPT', 'DUPZ')", 1e9,
                                     [("2019-11-16", "2019-12-31")])).fetchall()
    got = {r[0]: (r[6], r[7], r[8]) for r in rows}
    want = (20, 20, statistics.median(1e5 * (k + 1) for k in range(20)))
    for s in ("CLEAN", "DUPT", "DUPZ"):
        if got.get(s) != want:
            f.append(f"{s} (n_bars, n_traded, turnover) {got.get(s)}, want {want}")
    return f


def delev_cap_con(with_check=True):
    """deleveraging's screen inputs: 12 clean names, SOS (k branch) and ZEST (turnover branch)."""
    mod = load_topic("deleveraging")
    con = duckdb.connect()
    con.execute("CREATE TABLE ratios_cache(symbol VARCHAR, debtToEquityRatio DOUBLE, filing_epoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE metrics_cache(symbol VARCHAR, returnOnEquity DOUBLE, marketCap DOUBLE, filing_epoch BIGINT,"
                " period VARCHAR)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE, volume DOUBLE)")
    names = [f"N{i:02d}" for i in range(12)] + ["SOS", "ZEST"]
    for s in names:
        for y, de in ((2018, 0.5), (2019, 0.3)):
            con.execute("INSERT INTO ratios_cache VALUES (?, ?, ?, 'FY')", [s, de, ep(date(y, 12, 31))])
            con.execute("INSERT INTO metrics_cache VALUES (?, 0.2, 5e9, ?, 'FY')", [s, ep(date(y, 12, 31))])
        con.execute("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", [s, f"US00000{s:0>5}"])
        con.execute("INSERT INTO prices_cache VALUES (?, ?, 20, 20, 1000)", [s, ep(date(2020, 4, 2))])
    mark_nvdr_twins(con)
    con.execute(CAP_INPUTS_DDL)
    if with_check:   # distinct caps: lines with the same FY cap pool their turnover (share classes)
        cap_insert(con, [{"symbol": s, "filing_epoch": ep(date(2019, 12, 31)), "market_cap": cap, "net_income": ni,
                          "eps": eps, "shares": sh, "n_bars": 20, "n_traded": nt, "turnover": u * cap, "pf_raw": pfr,
                          "pf_cons": pfr, "pf_fwd": 1.0}
                         for s, cap, pfr, nt, u, eps, ni, sh in (
                             ("SOS", 5e9, 750.0, 20, 1.9e-4, 124.0, 1.47e6, 593233.0),
                             ("ZEST", 5.1e9, 150.0, 20, 2.0e-5, -1.12, -11.407e6, 10.202e6))])
    apply_cap_rule(con)
    return mod, con


def cyc_cap_con(with_check=True):
    """cyclical-timing's signal and screen inputs, the same 14 names."""
    mod = load_topic("cyclical-timing")
    _, con = delev_cap_con(with_check)
    con.execute("CREATE TABLE revenue_cache(symbol VARCHAR, revenue DOUBLE, filing_epoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE profile_cache(symbol VARCHAR, sector VARCHAR, exchange VARCHAR, market_cap DOUBLE)")
    for s in [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ratios_cache").fetchall()]:
        for y, rev in ((2018, 100.0), (2019, 110.0)):
            con.execute("INSERT INTO revenue_cache VALUES (?, ?, ?, 'FY')", [s, rev, ep(date(y, 12, 31))])
        con.execute("INSERT INTO profile_cache VALUES (?, 'Industrials', 'NYSE', 5e9)", [s])
    con.execute("UPDATE prices_cache SET trade_epoch = ?", [ep(date(2020, 7, 2))])
    return mod, con


def cap_screen_failures(with_check=True):
    """Check M at every floor: SOS (cap / 50 = 100M) and ZEST (ineligible) leave deleveraging's screen,
    cyclical-timing's signal and its screen. With cap_check empty they pass: the mutant."""
    f = []
    mod, con = delev_cap_con(with_check)
    dc = DataChecks("on")
    got = {s for s, _ in mod.screen_stocks(con, date(2020, 4, 1), 1e9, offset_days=1, dc=dc)}
    if got & {"SOS", "ZEST"} or len(got) != 12:
        f.append(f"deleveraging screen holds {sorted(got)}")
    caps = sorted((e["symbol"], e["rule"]) for e in dc.events if e["check"] == "cap")
    if caps != [("SOS", "cap_over_k"), ("ZEST", "cap_over_pf")]:
        f.append(f"deleveraging cap events {caps}")
    mod, con = cyc_cap_con(with_check)
    dc = DataChecks("on")
    _, _, n_sig = mod.compute_expansion_signal(con, date(2020, 7, 1), 1e9, dc=dc)
    if n_sig != 12:
        f.append(f"cyclical signal counts {n_sig} names, want 12")
    got = {s for s, _, _ in mod.screen_stocks(con, date(2020, 7, 1), 1e9, offset_days=1, dc=dc)}
    if got & {"SOS", "ZEST"} or len(got) != 12:
        f.append(f"cyclical screen holds {sorted(got)}")
    kinds = sorted((e["check"], e["symbol"]) for e in dc.events if e["check"] in ("cap", "signal_cap"))
    if kinds != [("cap", "SOS"), ("cap", "ZEST"), ("signal_cap", "SOS"), ("signal_cap", "ZEST")]:
        f.append(f"cyclical cap events {kinds}")
    return f


# ---------------------------------------------------------------- cases from the same-day reviews

def cap_case(rows, by_fy=False):
    """apply_cap_rule on dict rows (period end defaults to 1000, n_bars to 20); {symbol: cap_check row},
    or {(symbol, filing_epoch): row} with by_fy."""
    con = duckdb.connect()
    con.execute(CAP_INPUTS_DDL)
    cap_insert(con, [{"filing_epoch": 1000, "n_bars": 20, **r} for r in rows])
    apply_cap_rule(con)
    if not by_fy:
        return cap_rows(con)
    cur = con.execute("SELECT * FROM cap_check")
    names = [c[0] for c in cur.description]
    return {(r[0], r[1]): dict(zip(names, r)) for r in cur.fetchall()}


def m1_failures():
    """7211.T Mitsubishi Motors FY2003: k 10.03 (the 2013 1:10 consolidation, EPS restated, shares not)
    agrees with pf 10, so the cap lowers to cap / k first and u reads 7.80e-4 on 404.9bn, not 7.77e-5
    on FMP's 4.06T: cap_over_k, over the 100bn floor. HAO FY2024 (k 10 against pf 25) and SOS FY2019
    (k 50 against pf 750) disagree with pf and keep turnover-then-k: both stay out."""
    cap = 4.0623e12
    got = cap_case([
        {"symbol": "7211.T", "market_cap": cap, "shares": 1.488016e9, "eps": 251.85, "net_income": 37.361e9,
         "n_traded": 20, "turnover": 7.77e-5 * cap, "pf_splits": 10.0, "pf_cons": 10.0, "pf_fwd": 1.0},
        {"symbol": "HAO", "market_cap": 2.01e11, "shares": 1e8, "eps": 10.04, "net_income": 1e8, "n_traded": 20,
         "turnover": 1.56e-5 * 2.01e11, "pf_raw": 25.0, "pf_splits": 6.4e4, "pf_cons": 6.4e4, "pf_fwd": 1.0},
        {"symbol": "SOS", "market_cap": 1.64e9, "shares": 593233.0, "eps": 124.0, "net_income": 1.47e6, "n_traded": 20,
         "turnover": 1.08e-4 * 1.64e9, "pf_raw": 750.0, "pf_splits": 50.0, "pf_cons": 50.0, "pf_fwd": 1.0}])
    f = []
    r = got["7211.T"]
    used = cap_used_of(r)
    if r["rule"] != "cap_over_k" or used is None or abs(used / 1e9 - 404.9) > 0.5:
        f.append(f"7211.T FY2003: {r['rule']}, cap_used {used}, want cap_over_k 404.9bn (over the 100bn floor)")
    for s, floor in (("HAO", 1e9), ("SOS", 1e9)):
        u = cap_used_of(got[s])
        if u is not None and u > floor:
            f.append(f"{s} must stay out: {got[s]['rule']} cap_used {u}")
    return f


def k_guard_failures():
    """4541.T Nichi-Iko FY2011 (k 132 from 5.09bn fixed shares against EPS 101.54): its only later split
    is the 1:70,384,700 delisting record, which FMP's prices do not carry. cap_check_sql leaves an event
    past split_max out, so no pf opens the gate, k is never read and FMP's 161bn stands (true about
    104bn, over the 100bn floor). Before, the record gave pf 7.0e7 and u 8.96e-5 made it ineligible.
    The control, 7211.T, keeps its 1:10 (pf 10)."""
    src = cap_source()
    fy, cap = date(2011, 11, 30), 161.064e9
    src.execute("INSERT INTO key_metrics VALUES ('4541.T', 'FY', ?, ?, 0.1, 0.05, ?)", [fy.isoformat(), ep(fy), cap])
    src.execute("INSERT INTO income_statement VALUES ('4541.T', 'FY', ?, ?, 1, 3908009000, 101.54, 5088921000, ?)",
                [fy.isoformat(), ep(fy), (fy + timedelta(days=60)).isoformat()])
    for k in range(25):
        day = fy - timedelta(days=k)
        src.execute("INSERT INTO stock_eod VALUES ('4541.T', ?, ?, 31.65, 31.65, ?)",
                    [day.isoformat(), ep(day), 8.96e-5 * cap / 31.65])
    src.execute("INSERT INTO splits_calendar VALUES ('4541.T', '2023-03-29', ?, 1, 70384700)", [ep(date(2023, 3, 29))])
    fy2 = date(2003, 3, 31)
    src.execute("INSERT INTO key_metrics VALUES ('7211.T', 'FY', ?, ?, 0.1, 0.05, 4.0623e12)", [fy2.isoformat(), ep(fy2)])
    src.execute("INSERT INTO splits_calendar VALUES ('7211.T', '2013-07-29', ?, 1, 10)", [ep(date(2013, 7, 29))])
    cur = src.execute(cap_check_sql("symbol IN ('4541.T', '7211.T')", 1e11, [("2011-10-01", "2011-11-30")]))
    names = [c[0] for c in cur.description]
    rows = [dict(zip(names, r)) for r in cur.fetchall()]
    by = {r["symbol"]: r for r in rows}
    f = []
    if by["4541.T"]["pf_splits"] is not None:
        f.append(f"4541.T pf_splits {by['4541.T']['pf_splits']}: the delisting record must not count")
    if abs((by["7211.T"]["pf_splits"] or 0) - 10.0) > 1e-9:
        f.append(f"7211.T pf_splits {by['7211.T']['pf_splits']}, want 10")
    got = cap_case(rows)
    used = cap_used_of(got["4541.T"])
    if used is None or used < 1e11:
        f.append(f"4541.T FY2011: {got['4541.T']['rule']}, cap_used {used}, want FMP's 161bn to stand")
    return f


def m2_failures():
    """Turnover over the lines that carry one FY cap, and a failing cap lowered instead of excluded.
    0653.HK Bonjour FY2011 to FY2013 (pf_raw 16.5, u 2.9e-5 to 6.9e-5) lower to their as-traded caps
    4.53bn, 3.68bn, 6.58bn, over the HK$2bn floor; 1124.HK FY2008 (pf_raw 9.0) to 3.43bn. BBD-A.TO
    FY2001, ERIC-A.ST FY2006 and 02826K.KS FY2014 are thin classes on a real cap: their sibling's
    turnover clears the test and the cap stands (028260.KS listed in December 2014, 9 traded bars). The
    exclusions stay out: ZEST FY2018 lowers to 12m, HAO FY2024 stays ineligible, and the 8038.T shape
    (u 1e-8, which fails on the lowered cap as well) stays ineligible."""
    rows = []
    for s, fe, cap, raw, u in (("0653.HK", 2011, 74757082700.12, 1.31, 2.93e-5),
                               ("0653.HK", 2012, 60703067050.92, 1.07, 6.87e-5),
                               ("0653.HK", 2013, 108693025442.12, 1.86, 6.02e-5)):
        shares = cap / (raw * 16.5)   # a cap of shares x close, close = 16.5 x raw
        rows.append({"symbol": f"{s}/{fe}", "filing_epoch": fe, "market_cap": cap, "shares": shares,
                     "eps": 0.0685, "net_income": 0.0685 * shares, "n_traded": 20, "turnover": u * cap,
                     "pf_raw": 16.5, "pf_splits": 545.45, "pf_cons": 600.0, "pf_fwd": 1.0})
    cap = 30901836139.2
    rows.append({"symbol": "1124.HK", "filing_epoch": 2008, "market_cap": cap, "shares": 2641182576.0, "eps": 0.0442,
                 "net_income": 364694000.0, "n_traded": 20, "turnover": 4.57e-5 * cap, "pf_raw": 9.0, "pf_splits": 8.985,
                 "pf_cons": 10.0, "pf_fwd": 1.0})
    for a, b, fe, cap, ua, ub, nb, pf in (("BBD-A.TO", "BBD-B.TO", 2001, 22815626902.7175, 4.68e-5, 3.04e-3, 20, 25.0),
                                          ("ERIC-A.ST", "ERIC-B.ST", 2006, 447.562e9, 1.17e-5, 7.62e-3, 18, 5.0),
                                          ("02826K.KS", "028260.KS", 2014, 16552833502000.0, 1.78e-5, 0.0356, 9, 3.17)):
        # each pair shares its FY cap in a later year too: one shared FY alone does not make siblings
        for f, c in ((fe, cap), (fe + 1, cap * 1.1)):
            rows.append({"symbol": a, "filing_epoch": f, "market_cap": c, "n_traded": 20, "turnover": ua * c,
                         "pf_splits": pf, "pf_cons": pf, "pf_fwd": 1.0})
            rows.append({"symbol": b, "filing_epoch": f, "market_cap": c, "n_traded": nb, "turnover": ub * c,
                         "pf_raw": 1.0 if b == "028260.KS" else pf, "pf_splits": pf, "pf_cons": pf, "pf_fwd": 1.0})
    rows.append({"symbol": "ZEST", "filing_epoch": 2018, "market_cap": 1.82e9, "shares": 10.202e6, "eps": -1.12,
                 "net_income": -11.407e6, "n_traded": 20, "turnover": 7.49e-5 * 1.82e9, "pf_raw": 150.0,
                 "pf_splits": 150.0, "pf_cons": 150.0, "pf_fwd": 1.0})
    rows.append({"symbol": "HAO", "filing_epoch": 2024, "market_cap": 2.01e11, "shares": 1e8, "eps": 10.04,
                 "net_income": 1e8, "n_traded": 20, "turnover": 1.56e-5 * 2.01e11, "pf_raw": 25.0, "pf_splits": 6.4e4,
                 "pf_cons": 6.4e4, "pf_fwd": 1.0})
    rows.append({"symbol": "8038.T", "filing_epoch": 2006, "market_cap": 1.8e16, "shares": 1e9, "eps": 1.0,
                 "net_income": 1e9, "n_traded": 19, "turnover": 3.92e-9 * 1.8e16, "pf_raw": 10.0, "pf_splits": 10.0,
                 "pf_cons": 10.0, "pf_fwd": 1.0})
    by = cap_case(rows, by_fy=True)
    got = {s: r for (s, _), r in by.items()}
    f = []
    for s, want, floor in (("0653.HK/2011", 4.53e9, 2e9), ("0653.HK/2012", 3.68e9, 2e9), ("0653.HK/2013", 6.58e9, 2e9),
                           ("1124.HK", 3.43e9, 2e9)):
        used = cap_used_of(got[s])
        if used is None or abs(used / want - 1) > 0.01 or used <= floor:
            f.append(f"{s}: {got[s]['rule']} cap_used {used}, want about {want / 1e9:.2f}bn (over {floor / 1e9:.0f}bn)")
    for s, fe in (("BBD-A.TO", 2001), ("ERIC-A.ST", 2006), ("02826K.KS", 2014)):
        r = by[(s, fe)]
        if r["rule"] != "stands" or r.get("lines") != 2:
            f.append(f"{s}: {r['rule']} over {r.get('lines')} lines, want the cap to stand on its sibling's turnover")
    for s, floor in (("ZEST", 1e9), ("HAO", 1e9), ("8038.T", 1e11)):
        used = cap_used_of(got[s])
        if used is not None and used > floor:
            f.append(f"{s} must stay out: {got[s]['rule']} cap_used {used}")
    if got["8038.T"]["rule"] != "ineligible" or got["HAO"]["rule"] != "ineligible":
        f.append(f"8038.T {got['8038.T']['rule']}, HAO {got['HAO']['rule']}: both stay ineligible")
    return f


def m3_failures():
    """Inflations the pf gate could not see. Bonjour FY2007, FY2008: forward and reverse splits net out
    to pf_raw 1.375 while the later consolidations multiply to 600, so the gate opens, u fails and
    the cap lowers by pf x the forward splits FMP's FY share count was restated for (the 2010 6:1 and
    2:1: 1.375 x 12 = 16.5) to 0.39bn and 0.63bn, under the floor (the 2008 and 2009 holdings leave).
    FY2009 lowers the same way to 2.32bn and stays (the 2010 holding): the divisor is not the 600
    product. CETX FY2016 (3 traded bars) and PIXY FY2018 (none) have no u: with pf >= 3 the cap lowers
    by pf. TOPS FY2004 has bars but no raw close; its FY2006 row has one (pf_raw 4.54e13) with no split
    record between the two period ends, so FY2004 reads that price factor ('raw_fy') and lowers too."""
    rows = []
    for fe, cap, u in ((2007, 6468811877.0, 6.12e-5), (2008, 10321654150.5, 3.68e-5), (2009, 38230217471.74, 6.02e-5)):
        rows.append({"symbol": "0653.HK", "filing_epoch": fe, "market_cap": cap, "shares": 3.1e9, "eps": 0.0424,
                     "net_income": 0.0424 * 3.1e9, "n_traded": 20, "turnover": u * cap, "pf_raw": 1.375,
                     "pf_splits": 45.45, "pf_cons": 600.0, "pf_fwd": 12.0, "sh_restated": 12.0, "sh_step": 1.0})
    for s, fe, cap, nt, pfr, pfs in (("CETX", 2016, 59.4e9, 3, 2.52e5, 2.06e3),
                                     ("TOPS", 2004, 6.49e16, 0, None, 4.54e13),
                                     ("TOPS", 2006, 2.15e16, 0, 4.54e13, 4.54e13),
                                     ("PIXY", 2016, 1.44e9, 0, 3.46e7, 1.44e6)):
        rows.append({"symbol": s, "filing_epoch": fe, "market_cap": cap, "shares": 1e6, "eps": 1.0, "net_income": 1e6,
                     "n_traded": nt, "turnover": 0.0 if nt else None, "pf_raw": pfr, "pf_splits": pfs,
                     "pf_cons": pfs, "pf_fwd": 1.0, "next_split": 3000})
    by = cap_case(rows, by_fy=True)
    got = {f"0653.HK/{fe}": by[("0653.HK", fe)] for fe in (2007, 2008, 2009)}
    got.update({s: by[(s, fe)] for s, fe in (("CETX", 2016), ("TOPS", 2004), ("PIXY", 2016))})
    f = []
    if got["TOPS"]["pf_source"] != "raw_fy" or got["TOPS"]["pf"] != 4.54e13:
        f.append(f"TOPS FY2004 price factor {got['TOPS']['pf']} from {got['TOPS']['pf_source']}, want FY2006's 4.54e13")
    for s, want_in in (("0653.HK/2007", False), ("0653.HK/2008", False), ("0653.HK/2009", True)):
        used = cap_used_of(got[s])
        if (used is not None and used > 2e9) != want_in:
            f.append(f"Bonjour {s[-4:]}: {got[s]['rule']} cap_used {used}, want {'in' if want_in else 'out'} at the 2bn floor")
    if abs(cap_used_of(got["0653.HK/2008"]) - 10321654150.5 / 16.5) > 1e3:
        f.append(f"Bonjour 2008 divisor {got['0653.HK/2008'].get('cap_div')}, want 16.5")
    for s in ("CETX", "TOPS", "PIXY"):
        used = cap_used_of(got[s])
        if used is None or used > 1e9 or got[s]["rule"] != "cap_over_pf":
            f.append(f"{s}: {got[s]['rule']} cap_used {used}, want cap_over_pf under the $1bn floor")
    return f


def m_class_fy_failures():
    """Two unrelated companies can carry one identical FY cap: 1789.HK (AK Medical) and 1935.HK (JH
    Educational) share only FY2016's 2,362,800,000. Lines are share classes only when they share an
    identical cap in min_shared_fy (2) FYs or more, so 1935.HK's turnover is its own (crafted here: pf
    4, u 2e-5 against 1789.HK's 5e-3). Before, the pair pooled and 1935.HK stood on 1789.HK's trading;
    now it is read alone, fails on cap / 4 as well and is ineligible. ELUX-A/B, sharing two FYs, pool."""
    cap = 2362800000.0
    rows = [{"symbol": "1935.HK", "filing_epoch": 2016, "market_cap": cap, "n_traded": 20, "turnover": 2e-5 * cap,
             "pf_raw": 4.0, "pf_splits": 4.0, "pf_cons": 4.0, "pf_fwd": 1.0},
            {"symbol": "1789.HK", "filing_epoch": 2016, "market_cap": cap, "n_traded": 20, "turnover": 5e-3 * cap,
             "pf_raw": 1.0},
            {"symbol": "1789.HK", "filing_epoch": 2017, "market_cap": 5.1e9, "n_traded": 20, "turnover": 5e-3 * 5.1e9,
             "pf_raw": 1.0}]
    for fe, cap2 in ((2005, 27.529e9), (2006, 36.204e9)):
        rows += [{"symbol": "ELUX-A.ST", "filing_epoch": fe, "market_cap": cap2, "n_traded": 20,
                  "turnover": 2e-5 * cap2, "pf_raw": 4.0, "pf_splits": 4.0, "pf_cons": 4.0, "pf_fwd": 1.0},
                 {"symbol": "ELUX-B.ST", "filing_epoch": fe, "market_cap": cap2, "n_traded": 20,
                  "turnover": 5e-3 * cap2, "pf_raw": 4.0, "pf_splits": 4.0, "pf_cons": 4.0, "pf_fwd": 1.0}]
    by = cap_case(rows, by_fy=True)
    f = []
    r = by[("1935.HK", 2016)]
    if r["lines"] != 1 or r["rule"] != "ineligible":
        f.append(f"1935.HK FY2016: {r['rule']} over {r['lines']} lines, want its own turnover (1 line): ineligible")
    r = by[("ELUX-A.ST", 2006)]
    if r["lines"] != 2 or r["rule"] != "stands":
        f.append(f"ELUX-A.ST FY2006: {r['rule']} over {r['lines']} lines, want 2 lines (two shared FYs): stands")
    return f


def m_strict_failures():
    """A row is made ineligible only when pf itself opened the gate and every line of its group has
    bars; otherwise a failing cap lowers by the divisor, and where no evidence gives a divisor it is
    'unchecked' at FMP's cap (the server's inputs, as measured, with FMP's FY share counts).
    ERIC-A.ST FY1999 (SEK 786bn): pf_raw 1.25, so only the 2008 1:5 consolidation (pf_cons 5) opens
    the gate, and ERIC-B.ST has no stock_eod rows before 2000-01-03, so u 1.58e-5 is the thin A class
    alone. The FY share count shows no step at the 2000 4:1 or the 2008 1:5 (sh_restated 0.8), so D =
    1.25 x 0.8 is under 1: unchecked at 786bn for both lines, over the SEK 5bn floor (round 3 lowered
    them by 1.25 x 4 to 157bn and B to 218bn, with pf_fwd multiplying a price factor near 1). 02826K.KS
    FY2008: pf 3.17 from the split records only (no raw close) and its common 028260.KS listed in
    December 2014, so no bars; no record steps the share count, so it is unchecked at 16.67T. 0760.HK
    FY2006 (pf_raw 2, u 1.5e-8, records restated in the share count to 0.5): unchecked at 6.91bn.
    Before any of this, all four were ineligible; none is now. HAO and the 8038.T shape (pf >= 3,
    bars) stay ineligible in m2_failures."""
    rows = []
    for s, pfs in (("ERIC-A.ST", 1.25), ("ERIC-B.ST", 0.9027777777777777)):
        rows.append({"symbol": s, "filing_epoch": 1998, "market_cap": 800437396000.0, "net_income": 13049054360.0,
                     "eps": 8.52, "shares": 1597520000.0, "n_bars": None, "n_traded": None, "turnover": None,
                     "pf_splits": pfs, "pf_cons": 5.0, "pf_fwd": 4.0, "sh_restated": 0.8, "sh_step": 1.0})
    rows.append({"symbol": "ERIC-A.ST", "filing_epoch": 1999, "market_cap": 785954286206.1, "net_income": 12119249870.0,
                 "eps": 7.73, "shares": 1568614482.0, "n_bars": 20, "n_traded": 20, "turnover": 12404403.0,
                 "pf_raw": 1.25, "pf_splits": 1.25, "pf_cons": 5.0, "pf_fwd": 4.0, "sh_restated": 0.8, "sh_step": 1.0})
    rows.append({"symbol": "ERIC-B.ST", "filing_epoch": 1999, "market_cap": 785954286206.1, "net_income": 12119249870.0,
                 "eps": 7.73, "shares": 1568614482.0, "n_bars": None, "n_traded": None, "turnover": None,
                 "pf_splits": 0.9027777777777777, "pf_cons": 5.0, "pf_fwd": 4.0, "sh_restated": 0.8, "sh_step": 1.0})
    for s, fe, cap, ni, eps, sh, nb, to, pfr, pfs, shr in (
            ("02826K.KS", 2008, 16670267781299.49, 345649895000.0, 2378.68, 145311629.0, 20, 325253717.22, None,
             3.1667588816, 0.350089),
            ("028260.KS", 2008, 16670267781299.49, 345649895000.0, 2378.68, 145311629.0, None, None, None, 1.0254832590,
             1.0),
            ("02826K.KS", 2014, 16552833502000.0, 455646975000.0, 4349.24, 104764769.0, 20, 293912633.53, None,
             3.1667588816, 0.350089),
            ("028260.KS", 2014, 16552833502000.0, 455646975000.0, 4349.24, 104764769.0, 9, 589469496000.0, 1.0254832590,
             1.0254832590, 1.0)):
        rows.append({"symbol": s, "filing_epoch": fe, "market_cap": cap, "net_income": ni, "eps": eps, "shares": sh,
                     "n_bars": nb, "n_traded": nb, "turnover": to, "pf_raw": pfr, "pf_splits": pfs, "pf_cons": pfs,
                     "pf_fwd": 1.0, "sh_restated": shr, "sh_step": 1.0})
    rows.append({"symbol": "0760.HK", "filing_epoch": 2006, "market_cap": 6912949072.77, "net_income": -16268814.698129,
                 "eps": -0.0106, "shares": 1533910040.0, "n_bars": 20, "n_traded": 17, "turnover": 104.0, "pf_raw": 2.0,
                 "pf_splits": 2.0, "pf_cons": 20.0, "pf_fwd": 10.0, "sh_restated": 0.5, "sh_step": 1.0})
    by = cap_case(rows, by_fy=True)
    f = []
    for key, want, floor in ((("ERIC-A.ST", 1999), 785954286206.1, 5e9), (("ERIC-B.ST", 1999), 785954286206.1, 5e9),
                             (("02826K.KS", 2008), 16670267781299.49, 5e11), (("0760.HK", 2006), 6912949072.77, 2e9)):
        r = by[key]
        used = cap_used_of(r)
        if r["rule"] != "unchecked" or used is None or abs(used / want - 1) > 1e-9:
            f.append(f"{key[0]} FY{key[1]}: {r['rule']} cap_used {used}, want unchecked at FMP's {want / 1e9:.4g}bn")
        elif used <= floor:
            f.append(f"{key[0]} FY{key[1]}: cap_used {used} under the floor {floor}")
    r = by[("02826K.KS", 2014)]
    if r["rule"] != "stands" or r["lines"] != 2:
        f.append(f"02826K.KS FY2014: {r['rule']} over {r['lines']} lines, want stands on 028260.KS's 9 bars")
    return f


def m_unchecked_failures():
    """A failing row whose divisor is 1 is not silent. HAO FY2025 (the server's inputs): pf_raw 1.0, the
    later consolidations 2560 open the gate, no forward split, k 0.9995 (no K), u 9.13e-5 under the
    1e-4 test, not strict. Lowering by 1 changes nothing, so the row is 'unchecked' at FMP's 388m (no
    divisor), counted in cap_rows_flagged and listed in the block's cap_unchecked with its own price
    factor, split products, turnover ratio, lines and k; before, it was written 'stands' and counted
    nowhere. GRPN-like CLEAR (gate closed) stands and is not flagged."""
    con = duckdb.connect()
    con.execute(CAP_INPUTS_DDL)
    fe = ep(date(2025, 6, 30))
    cap_insert(con, [{"symbol": "HAO", "filing_epoch": fe, "market_cap": 388400896.0, "net_income": 3876680.0,
                      "eps": 33.2, "shares": 116707.0, "n_bars": 20, "n_traded": 20, "turnover": 35460.94,
                      "pf_raw": 1.0, "pf_splits": 2560.0, "pf_cons": 2560.0, "pf_fwd": 1.0},
                     {"symbol": "CLEAR", "filing_epoch": fe, "market_cap": 2e9, "n_bars": 20, "n_traded": 20,
                      "turnover": 0.01 * 2e9, "pf_raw": 1.0, "pf_splits": 1.0, "pf_cons": 1.0, "pf_fwd": 1.0}])
    stats = apply_cap_rule(con)
    got = cap_rows(con)
    f = []
    r = got["HAO"]
    if r["rule"] != "unchecked" or r["cap_div"] is not None or cap_used_of(r) != 388400896.0:
        f.append(f"HAO FY2025: {r['rule']} cap_div {r['cap_div']} cap_used {cap_used_of(r)}, "
                 "want unchecked at FMP's 388m")
    if got["CLEAR"]["rule"] != "stands":
        f.append(f"CLEAR: {got['CLEAR']['rule']}, want stands")
    if (stats["cap_rows_checked"], stats["cap_rows_flagged"]) != (2, 1):
        f.append(f"stats {stats}, want 2 checked and 1 flagged (HAO)")
    listed = stats.get("cap_unchecked", [])
    want = {"symbol": "HAO", "fy": "2025-06-30", "cap": 388400896.0, "pf": 1.0, "pf_source": "raw", "pf_cons": 2560.0,
            "pf_fwd": 1.0, "sh_restated": None, "sh_step": None, "u": 35460.94 / 388400896.0, "lines": 1,
            "k": 116707.0 * 33.2 / 3876680.0}
    got_u = listed[0] if len(listed) == 1 else {}
    off = [k for k, v in want.items() if not (got_u.get(k) == v if v is None or isinstance(v, (str, int)) else
                                              isinstance(got_u.get(k), float) and abs(got_u[k] / v - 1) < 1e-5)]
    if len(listed) != 1 or set(got_u) != set(want) or off:
        f.append(f"cap_unchecked {listed}, want one HAO FY2025 entry {want} (off: {off})")
    dc = DataChecks("on")
    dc.record_cap(stats)
    blk = dc.block()
    if blk["counts"]["cap_rows_flagged"] != 1 or [u["symbol"] for u in blk.get("cap_unchecked", [])] != ["HAO"]:
        f.append(f"block: cap_rows_flagged {blk['counts']['cap_rows_flagged']}, cap_unchecked {blk.get('cap_unchecked')}")
    return f


def m_divisor_failures():
    """The divisor's edges, on crafted rows whose gate the later consolidations alone open (pf_cons 5), so
    none is strict and a failing cap lowers. LOW_D: price factor 1.25, no forward split, no k, u 1e-6:
    lowered by 1.25 ('cap_over_pf'), since only a divisor of 1 leaves the cap as it is. NO_U: price
    factor 1 and 5 traded bars, so u is unknown and the divisor is 1: 'unchecked' and listed, not a
    silent 'stands'. K_ONLY: price factor 1 with k 10 (disagreeing): the divisor is max(k, D) = 10, so
    the row lowers by 10 and is not 'unchecked', though D alone is 1."""
    con = duckdb.connect()
    con.execute(CAP_INPUTS_DDL)
    cap = 2e9
    base = {"filing_epoch": 1000, "market_cap": cap, "n_bars": 20, "n_traded": 20, "turnover": 1e-6 * cap,
            "pf_splits": 5.0, "pf_cons": 5.0, "pf_fwd": 1.0}
    cap_insert(con, [{**base, "symbol": "LOW_D", "pf_raw": 1.25},
                     {**base, "symbol": "NO_U", "pf_raw": 1.0, "n_traded": 5},
                     {**base, "symbol": "K_ONLY", "pf_raw": 1.0, "eps": 10.0, "net_income": 1e8, "shares": 1e8}])
    stats = apply_cap_rule(con)
    got = cap_rows(con)
    f = []
    for s, rule, div in (("LOW_D", "cap_over_pf", 1.25), ("NO_U", "unchecked", None), ("K_ONLY", "cap_over_pf", 10.0)):
        r = got[s]
        if r["rule"] != rule or (div is None) != (r["cap_div"] is None) or (div and abs(r["cap_div"] - div) > 1e-9):
            f.append(f"{s}: {r['rule']} cap_div {r['cap_div']}, want {rule} {div}")
    listed = [(u["symbol"], u["u"]) for u in stats["cap_unchecked"]]
    if listed != [("NO_U", None)] or (stats["cap_rows_checked"], stats["cap_rows_flagged"]) != (3, 3):
        f.append(f"stats {stats['cap_rows_checked']} checked, {stats['cap_rows_flagged']} flagged, cap_unchecked "
                 f"{listed}, want 3, 3 and NO_U alone (u unknown)")
    return f


def m6_evidence_failures():
    """The divisor needs evidence beyond the split records (rows shaped like the measured ones). WEST3:
    no bars before its FY end (it listed after), pf 10 from a later 1:10 the FY share count was
    restated for: unchecked at FMP's real 1.30bn. GOL: no bars, records netting to 1 (pf_cons 10,
    pf_fwd 10): unchecked at 4.31bn, where pf x pf_fwd gave 10. S0568: pf_raw 1.0 and the 5:1, 1:10
    and 2:1 all restated in the share count: unchecked at 3.64bn. INPEX: pf_raw 1.786, pf_fwd 400 and
    a 7:5000 record on its listing day, all restated: unchecked at 3.6T. DCC: bars but no raw close and
    a 2:137 record the share count never shows: unchecked at 0.86bn. BONJ: pf_raw 1.375 and pf_fwd 12
    with no share evidence: pf_fwd does not multiply a price factor near 1, so it lowers by 1.375 alone.
    PHANT: no bars, pf 25 from the split records, no share step: unchecked. NOBAR (SAGA.L's shape): no
    bars at FY2004, while its FY2006 row has bars and pf_raw 10: a row with no bars never reads another
    FY's price factor (FMP's cap there may sit on a price the feed does not hold), so FY2004 is
    unchecked. XREC: bars but no raw close at FY2004 and a split record before FY2006, whose pf_raw is
    3: a record between the period ends stops the borrowing, so unchecked. Must stay lowered: CETX
    (pf_raw 2.52e5, 3 traded bars, the share count restated for the 35:1) by 2.52e5 x 35; P1124
    (1124.HK FY2008: pf_raw 9, the share count restated for the later 1:10) by 9, price evidence alone;
    HAOS (no bars, the FY share count steps at the 1:25 and the 1:128) by 3200, the steps, not the
    6.4e4 product."""
    base = {"n_bars": 20, "n_traded": 20, "eps": 1.0, "pf_fwd": 1.0, "sh_step": 1.0}
    rows = [
        {**base, "symbol": "WEST3", "market_cap": 1.30e9, "shares": 1.09e7, "net_income": 1.06e7, "n_bars": None,
         "n_traded": None, "pf_splits": 10.0, "pf_cons": 10.0, "sh_restated": 0.1},
        {**base, "symbol": "GOL", "market_cap": 4.31e9, "shares": 1.64e8, "net_income": 1.64e8, "n_bars": None,
         "n_traded": None, "pf_splits": 1.0, "pf_cons": 10.0, "pf_fwd": 10.0, "sh_restated": 0.5},
        {**base, "symbol": "S0568", "market_cap": 3.64e9, "n_traded": 19, "turnover": 1.81e-7 * 3.64e9, "pf_raw": 1.0,
         "pf_splits": 1.0, "pf_cons": 10.0, "pf_fwd": 10.0, "sh_restated": 0.5},
        {**base, "symbol": "INPEX", "market_cap": 3.6e12, "n_traded": 16, "turnover": 7.06e-6 * 3.6e12,
         "pf_raw": 1.785708598437964, "pf_splits": 1.7857142857142858, "pf_cons": 714.2857142857141,
         "pf_fwd": 400.0, "sh_restated": 0.5599999999999999},
        {**base, "symbol": "DCC", "market_cap": 8.57e8, "n_traded": 5, "turnover": 2e6, "pf_splits": 68.5,
         "pf_cons": 68.5, "sh_restated": 0.0146},
        {**base, "symbol": "BONJ", "market_cap": 1.03e10, "turnover": 3.68e-5 * 1.03e10, "pf_raw": 1.375,
         "pf_splits": 45.45, "pf_cons": 600.0, "pf_fwd": 12.0, "sh_restated": None, "sh_step": None},
        {**base, "symbol": "PHANT", "market_cap": 1.64e10, "n_bars": None, "n_traded": None, "pf_splits": 25.0,
         "pf_cons": 25.0, "sh_restated": 1.0},
        {**base, "symbol": "CETX", "market_cap": 4.08e10, "n_traded": 3, "turnover": 0.0, "pf_raw": 2.52e5,
         "pf_splits": 2.06e3, "pf_cons": 7.2e4, "pf_fwd": 35.0, "sh_restated": 35.0},
        {**base, "symbol": "HAOS", "market_cap": 1.87e11, "n_bars": None, "n_traded": None, "pf_splits": 6.4e4,
         "pf_cons": 6.4e4, "sh_restated": 1.0, "sh_step": 3200.0},
        {**base, "symbol": "P1124", "market_cap": 30901836139.2, "turnover": 4.57e-5 * 30901836139.2, "pf_raw": 9.0,
         "pf_splits": 8.985, "pf_cons": 10.0, "sh_restated": 0.1},
        {**base, "symbol": "NOBAR", "market_cap": 2.05e9, "n_bars": None, "n_traded": None, "pf_splits": 13.7,
         "pf_cons": 15.0, "sh_restated": 1.0, "next_split": 3000},
        {**base, "symbol": "NOBAR", "filing_epoch": 2006, "market_cap": 1.84e9, "turnover": 0.02 * 1.84e9,
         "pf_raw": 13.7, "pf_splits": 13.7, "pf_cons": 15.0, "sh_restated": 1.0, "next_split": 3000},
        {**base, "symbol": "XREC", "market_cap": 3e9, "n_traded": 5, "turnover": 1e6, "pf_splits": 50.0,
         "pf_cons": 50.0, "sh_restated": 1.0, "next_split": 1500},
        {**base, "symbol": "XREC", "filing_epoch": 2006, "market_cap": 3e9, "turnover": 1e7, "pf_raw": 3.0,
         "pf_splits": 3.0, "pf_cons": 3.0, "sh_restated": 1.0, "next_split": 2500}]
    con = duckdb.connect()
    con.execute(CAP_INPUTS_DDL)
    cap_insert(con, [{"filing_epoch": 1000, **r} for r in rows])
    stats = apply_cap_rule(con)
    cur = con.execute("SELECT * FROM cap_check WHERE filing_epoch = 1000")
    names = [c[0] for c in cur.description]
    got = {r[0]: dict(zip(names, r)) for r in cur.fetchall()}
    f = []
    for s, rule, div in (("WEST3", "unchecked", None), ("GOL", "unchecked", None), ("S0568", "unchecked", None),
                         ("INPEX", "unchecked", None), ("DCC", "unchecked", None), ("BONJ", "cap_over_pf", 1.375),
                         ("PHANT", "unchecked", None), ("NOBAR", "unchecked", None), ("XREC", "unchecked", None),
                         ("CETX", "cap_over_pf", 2.52e5 * 35.0), ("P1124", "cap_over_pf", 9.0),
                         ("HAOS", "cap_over_pf", 3200.0)):
        r = got[s]
        if r["rule"] != rule or (div is None) != (r["cap_div"] is None) or (div and abs(r["cap_div"] / div - 1) > 1e-9):
            f.append(f"{s}: {r['rule']} cap_div {r['cap_div']}, want {rule} {div}")
    if got["NOBAR"]["pf_source"] != "splits" or got["XREC"]["pf_source"] != "splits":
        f.append(f"pf_source NOBAR {got['NOBAR']['pf_source']}, XREC {got['XREC']['pf_source']}: neither borrows")
    listed = sorted(u["symbol"] for u in stats["cap_unchecked"])
    if listed != ["DCC", "GOL", "INPEX", "NOBAR", "PHANT", "S0568", "WEST3", "XREC"]:
        f.append(f"cap_unchecked {listed}")
    return f


def m6_sql_failures():
    """cap_check_sql on a local copy: split records and FMP's FY share counts. DUP (AMCR's shape): the
    same 1:5 twice on one day counts once, pf_cons 5. K71 (071970.KS's shape): 1:8 twice 38 days apart,
    1:20 twice 129 days apart, then a third 1:8 400 days later and a 1:4 30 days after the first 1:8:
    pf_cons 8 x 20 x 8 x 4 = 5120, where every record counted gave 819,200. SHR: FY share counts 1e8 to
    FY2012, then 3.63e6, 4e6, 2.56e6, 2.6e6: a 1:10 in 2012 the count was restated for (no step), a
    1:10 in 2013 at which it falls 27.5x (HAO's shape: the log of the change 1.44 times the record's,
    inside share_step), an 11:10 bonus too small to read, a 1:20 that moves it to 0.64x (neither) and
    a 1:4 after the last FY (untestable); FY2010 reads sh_restated 0.1, sh_step 10 and next_split the
    2012 record, FY2013 reads 1 and 1."""
    src = cap_source()
    rec = []
    for y in range(2010, 2017):
        fy = date(y, 12, 31)
        sh = 1e8 if y <= 2012 else {2013: 3.63e6, 2014: 4e6, 2015: 2.56e6, 2016: 2.6e6}[y]
        src.execute("INSERT INTO key_metrics VALUES ('SHR', 'FY', ?, ?, 0.1, 0.05, 5e10)", [fy.isoformat(), ep(fy)])
        src.execute("INSERT INTO income_statement VALUES ('SHR', 'FY', ?, ?, 1, 1e8, 1.0, ?, ?)",
                    [fy.isoformat(), ep(fy), sh, (fy + timedelta(days=60)).isoformat()])
    rec += [("SHR", date(2012, 6, 1), 1, 10), ("SHR", date(2013, 6, 3), 1, 10), ("SHR", date(2014, 6, 2), 11, 10),
            ("SHR", date(2015, 6, 1), 1, 20), ("SHR", date(2017, 3, 1), 1, 4)]
    fy = date(2020, 12, 31)
    for s in ("DUP", "K71"):
        src.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, 0.1, 0.05, 1.64e10)", [s, fy.isoformat(), ep(fy)])
    rec += [("DUP", date(2026, 1, 15), 1, 5), ("DUP", date(2026, 1, 15), 1, 5)]
    x, y = date(2021, 3, 30), date(2022, 1, 17)
    rec += [("K71", x, 1, 8), ("K71", x + timedelta(days=38), 1, 8), ("K71", x + timedelta(days=30), 1, 4),
            ("K71", y, 1, 20), ("K71", y + timedelta(days=129), 1, 20), ("K71", y + timedelta(days=400), 1, 8)]
    for s, d, n, dn in rec:
        src.execute("INSERT INTO splits_calendar VALUES (?, ?, ?, ?, ?)", [s, d.isoformat(), ep(d), n, dn])
    cur = src.execute(cap_check_sql("symbol IN ('SHR', 'DUP', 'K71')", 1e9, [("2009-11-01", "2020-12-31")]))
    names = [c[0] for c in cur.description]
    rows = {(r[0], du.utc_date(r[1]).year): dict(zip(names, r)) for r in cur.fetchall()}
    f = []
    close = lambda a, b: a is not None and abs(a / b - 1) < 1e-9
    if not close(rows[("DUP", 2020)]["pf_cons"], 5.0):
        f.append(f"DUP pf_cons {rows[('DUP', 2020)]['pf_cons']}, want 5 (one record)")
    if not close(rows[("K71", 2020)]["pf_cons"], 5120.0):
        f.append(f"K71 pf_cons {rows[('K71', 2020)]['pf_cons']}, want 5120 (duplicates within 150 days once)")
    want_cols = {"next_split", "sh_restated", "sh_step"}
    if want_cols - set(names):
        return f + [f"cap_check_sql has no {sorted(want_cols - set(names))}"]
    r10, r13 = rows[("SHR", 2010)], rows[("SHR", 2013)]
    if not (close(r10["sh_restated"], 0.1) and close(r10["sh_step"], 10.0)
            and r10["next_split"] == ep(date(2012, 6, 1))):
        f.append(f"SHR FY2010 sh_restated {r10['sh_restated']} sh_step {r10['sh_step']} next {r10['next_split']}, "
                 "want 0.1, 10 and the 2012-06-01 record")
    if not (close(r13["sh_restated"], 1.0) and close(r13["sh_step"], 1.0)):
        f.append(f"SHR FY2013 sh_restated {r13['sh_restated']} sh_step {r13['sh_step']}, want 1 and 1")
    return f


T1_FLAT =(("1114.HK", 7.30, 1.01), ("BC88.L", 95.0, 15.22), ("GBGLOBAL.NS", 930.0, 930.0), ("76ID.L", 165.0, 33.71),
           ("BR.BK", 35.0, 28.88))


def t1_failures():
    """A line with no traded row in the window keeps its rows only when its close changes on two days or
    more there, day over day in date order: 1114.HK at a suspended 7.30, BC88.L, GBGLOBAL.NS, 76ID.L and
    BR.BK at one flat close lose every row and are not bought; so does BC88.L's real 2009-10 window, a
    stale quote that steps once (95 on 10-01 and 10-02, then 70 for six days, volume 0 throughout: one
    change). NUL.L is flat at 95 with one row whose close is NULL (adjClose 18.61): a date with no close
    is skipped, so it has no change and leaves, where reading adjClose there saw two. XW.L is flat at 60
    in 2009-10 and steps once in 2022-07 (70, then 75): changes are counted within a window, so both
    windows leave. TRUE.BK 2006-04 (7 closes) and ADVANC-R.BK 2005-01 (4 changes) keep theirs. The
    two-price NVDR feeds are in t1_feed_failures. Without the flat-line rule, all eleven windows keep
    their rows and the stale lines are bought."""
    d, d2 = date(2022, 7, 1), date(2009, 10, 1)
    days, days2 = list(weekdays(d, d + timedelta(days=11))), list(weekdays(d2, d2 + timedelta(days=11)))
    rows = fillers(30, days + days2)
    for s, c, a in T1_FLAT:
        rows += [(s, day, a, c, 0.0) for day in days]
    true = [6.4, 6.52, 6.22, 6.238918, 6.22, 6.28, 5.92, 5.63, 5.63]
    rows += [("TRUE.BK", day, round(c * 0.9, 4), c, 0.0) for day, c in zip(days, true)]
    adv = [111.0, 107.0, 109.0, 112.0, 111.0, 111.0, 111.0, 111.0, 111.0]
    rows += [("ADVANC-R.BK", day, round(c * 0.29, 4), c, 0.0) for day, c in zip(days, adv)]
    bc88 = [(day, 18.61, 95.0) if day < date(2009, 10, 5) else (day, 13.71, 70.0) for day in days2]
    rows += [("BC88.L", day, a, c, 0.0) for day, a, c in bc88]
    rows += [("NUL.L", day, 18.61, None if i == 3 else 95.0, 0.0) for i, day in enumerate(days2)]
    rows += [("XW.L", day, 60.0, 60.0, 0.0) for day in days2]
    rows += [("XW.L", day, 70.0 if i < 3 else 75.0, 70.0 if i < 3 else 75.0, 0.0) for i, day in enumerate(days)]
    con = cache(rows)
    st = clean_price_cache(con, set(), [d2, d])
    f = []
    for s, _, _ in T1_FLAT:
        if get_prices(con, [s], d, offset_days=1).get(s) is not None:
            f.append(f"{s}: a flat line with no volume is still buyable")
    if get_prices(con, ["BC88.L"], d2, offset_days=1).get("BC88.L") is not None:
        f.append("BC88.L 2009-10: a stale quote that steps once (95, then 70) with no volume is still buyable at 18.61")
    if get_prices(con, ["NUL.L"], d2, offset_days=1).get("NUL.L") is not None:
        f.append("NUL.L 2009-10: a flat line with one NULL close is still buyable at 18.61 (a missing close read as a change)")
    for x in (d2, d):
        if get_prices(con, ["XW.L"], x, offset_days=1).get("XW.L") is not None:
            f.append(f"XW.L {x:%Y-%m}: under two changes in the window, still buyable (changes counted across windows)")
    for s, want in (("TRUE.BK", round(6.52 * 0.9, 4)), ("ADVANC-R.BK", round(107.0 * 0.29, 4))):
        if get_prices(con, [s], d, offset_days=1).get(s) != want:
            f.append(f"{s}: a moving feed with no volume keeps its rows (entry {want})")
    if (st["symbol_windows_without_volume"], st.get("flat_windows_moved")) != (11, 9):
        f.append(f"counts: {st['symbol_windows_without_volume']} windows without volume, "
                 f"{st.get('flat_windows_moved')} flat windows moved, want 11 and 9")
    want_rows = 6 * len(days) + 3 * len(days2)
    if st["rows_moved"]["zero_volume"] != want_rows:
        f.append(f"rows_moved.zero_volume {st['rows_moved']['zero_volume']}, want {want_rows}")
    reasons = {r[0] for r in con.execute("SELECT DISTINCT reason FROM prices_dropped").fetchall()}
    if reasons != {"flat_no_volume"}:
        f.append(f"prices_dropped reasons {reasons}")
    return f


# real stock_eod rows, volume 0 throughout: (date, close, adjClose)
T1_FEEDS = {
    # ticks between 48.0 and 48.5: changes on 5 days (held in deleveraging Thailand, a stand-in for BH.BK)
    "BH-R.BK": (date(2007, 7, 1), [("2007-07-02", 48.0, 32.53), ("2007-07-03", 48.0, 32.53),
                                   ("2007-07-04", 48.5, 32.87), ("2007-07-05", 48.0, 32.53),
                                   ("2007-07-06", 48.5, 32.87), ("2007-07-09", 48.5, 32.87),
                                   ("2007-07-10", 48.5, 32.87), ("2007-07-11", 48.0, 32.53),
                                   ("2007-07-12", 48.5, 32.87)]),
    # 37.75 / 37.98 while RATCH.BK trades 38.68 / 38.91 on 0.6m to 2.3m shares: 4 changes
    "RATCH-R.BK": (date(2005, 7, 1), [("2005-07-04", 37.75, 13.94), ("2005-07-05", 37.98, 14.02),
                                      ("2005-07-06", 37.75, 13.94), ("2005-07-07", 37.98, 14.02),
                                      ("2005-07-08", 37.98, 14.02), ("2005-07-11", 37.98, 14.02),
                                      ("2005-07-12", 37.75, 13.94)]),
    # 3.40 / 3.42: changes on two days, the least a no-volume line needs
    "MCS-R.BK": (date(2007, 4, 1), [("2007-04-02", 3.4, 0.94246), ("2007-04-03", 3.42, 0.948),
                                    ("2007-04-04", 3.42, 0.948), ("2007-04-05", 3.42, 0.948),
                                    ("2007-04-09", 3.4, 0.94246), ("2007-04-10", 3.4, 0.94246),
                                    ("2007-04-11", 3.4, 0.94246), ("2007-04-12", 3.4, 0.94246)]),
    # 1.55 for six rows, then 1.61: one change, out (as under the 3-close rule)
    "BCH-R.BK": (date(2006, 4, 1), [("2006-04-03", 1.55, 0.74744), ("2006-04-04", 1.55, 0.788),
                                    ("2006-04-05", 1.55, 0.78414), ("2006-04-07", 1.55, 0.788),
                                    ("2006-04-10", 1.55, 0.78414), ("2006-04-11", 1.55, 0.78414),
                                    ("2006-04-12", 1.61, 0.81504)]),
}


def t1_feed_failures():
    """Pre-2009 Thai NVDR feeds print real prices with no volume, some ticking between two prices. The
    3-close rule moved them; the rule now counts the days the close changes. BH-R.BK 2007-07 (5
    changes), RATCH-R.BK 2005-07 (4) and MCS-R.BK 2007-04 (2) keep their rows and their entries;
    BCH-R.BK 2006-04 (1.55, then 1.61 once) stays out. DUP.BK is flat at 95 with a duplicate row at 70
    on one date: one close per date (the row get_prices reads, adjClose first), so it has no change and
    stays out, where counting rows in date order would see two. GAP.BK reads 95, 95, a date with no
    close, then 70, 70 and 80: the missing close is skipped, so it changes on two days and keeps its
    rows; a missing close read as a break would lose the 95 to 70 change."""
    rows, dates = [], []
    for s, (d, real) in T1_FEEDS.items():
        dates.append(d)
        rows += fillers(30, list(weekdays(d, d + timedelta(days=11))), prefix=f"F{d.year}{d.month:02d}")
        rows += [(s, date.fromisoformat(day), a, c, 0.0) for day, c, a in real]
    d0 = date(2009, 1, 1)
    dates.append(d0)
    days0 = list(weekdays(d0, d0 + timedelta(days=11)))
    rows += fillers(30, days0, prefix="G")
    rows += [("DUP.BK", day, 18.61, 95.0, 0.0) for day in days0]
    rows.append(("DUP.BK", days0[1], 13.71, 70.0, 0.0))
    gap = [95.0, 95.0, None, 70.0, 70.0, 80.0, 80.0, 80.0]
    rows += [("GAP.BK", day, 0.5 * (c or 95.0), c, 0.0) for day, c in zip(days0, gap)]
    con = cache(rows)
    st = clean_price_cache(con, set(), dates)
    f = []
    for s, want in (("BH-R.BK", 32.53), ("RATCH-R.BK", 13.94), ("MCS-R.BK", 0.94246), ("BCH-R.BK", None)):
        got = get_prices(con, [s], T1_FEEDS[s][0], offset_days=1).get(s)
        if got != want:
            f.append(f"{s}: entry {got}, want {want}" + (" (a feed whose close changes on 2 days or more keeps"
                                                         " its rows)" if want else " (one change: out)"))
    if get_prices(con, ["DUP.BK"], d0, offset_days=1).get("DUP.BK") is not None:
        f.append("DUP.BK: a flat line with a duplicate row on one date is buyable (rows counted, not dates)")
    if get_prices(con, ["GAP.BK"], d0, offset_days=1).get("GAP.BK") != 47.5:
        f.append("GAP.BK: two changes across a date with no close, entry "
                 f"{get_prices(con, ['GAP.BK'], d0, offset_days=1).get('GAP.BK')}, want 47.5 (a missing close skipped)")
    if (st["symbol_windows_without_volume"], st.get("flat_windows_moved")) != (6, 2):
        f.append(f"counts: {st['symbol_windows_without_volume']} windows without volume, "
                 f"{st.get('flat_windows_moved')} flat windows moved, want 6 and 2")
    return f


def t1_exit_failures():
    """600694.SS deleveraging China 2006-04: halted from 07-03, its exit window holds flat zero-volume rows
    (close 27.78, adjClose 17.06 on a halt-row factor step). T moves them, the window exit is gone, and
    the LTP fill (traded_only) reads the last traded bar, adjClose 13.65 on 06-30: +66.7% against the
    entry's 8.19, where the halt rows booked +108.3%."""
    d, x = date(2006, 4, 1), date(2006, 7, 1)
    days_e, days_x = list(weekdays(d, d + timedelta(days=11))), list(weekdays(x, x + timedelta(days=11)))
    rows = fillers(30, days_e + days_x)
    rows += [("600694.SS", day, 8.19, 13.34, 5e5) for day in days_e]
    rows += [("600694.SS", day, 17.06, 27.78, 0.0) for day in days_x if day >= date(2006, 7, 3)]
    con = cache(rows)
    clean_price_cache(con, set(), [d, x])
    e, xp = (get_prices(con, ["600694.SS"], t, offset_days=1).get("600694.SS") for t in (d, x))
    f = []
    if e != 8.19 or xp is not None:
        f.append(f"600694.SS window bars: entry {e}, exit {xp}, want 8.19 and none (the halt rows move)")
    remote = [("600694.SS", day, 8.19 + (13.65 - 8.19) * k / 60, 13.34, 4e5)
              for k, day in enumerate(weekdays(date(2006, 3, 3), date(2006, 6, 30)))]
    remote[-1] = ("600694.SS", date(2006, 6, 30), 13.65, 22.23, 4e5)
    remote += [("600694.SS", day, 17.06, 27.78, 0.0) for day in weekdays(date(2006, 7, 3), date(2006, 7, 31))]
    ltp = du.LtpExits(LtpClient(ltp_src(remote)), con, traded_only=True)
    fill = ltp.fill(["600694.SS"], {"600694.SS": e}, {"600694.SS": xp}, d, x, offset_days=1, min_entry_price=1.0)
    if fill.get("600694.SS") != 13.65:
        f.append(f"600694.SS LTP fill {fill}, want the last traded bar 13.65")
    elif abs(fill["600694.SS"] / e - 1 - 0.6667) > 1e-3:
        f.append(f"600694.SS books {fill['600694.SS'] / e - 1:+.1%}")
    return f


def elux_con(extra=()):
    """ELUX-A.ST and ELUX-B.ST around 2006-07 (cyclical-timing STO, annual windows), with the FY caps they
    share in cap_inputs. A's traded rows from 2006-06-08 sit near 52 while B trades 84 to 100 on millions
    of shares; A's zero-volume rows open the 2006 window (84.18 on 07-03); both meet again by 2007."""
    rows, wins = [], (date(2005, 7, 1), date(2006, 7, 1), date(2007, 7, 1))
    for w in wins:
        rows += fillers(30, list(weekdays(w, w + timedelta(days=11))))
    for day in weekdays(date(2005, 7, 1), date(2005, 7, 12)):
        rows += [("ELUX-A.ST", day, 80.0 * 1.04, 80.0 * 1.04, 3000.0), ("ELUX-B.ST", day, 80.0, 80.0, 2e6)]
    b06 = {date(2006, 7, 3): 92.0, date(2006, 7, 4): 93.0, date(2006, 7, 5): 94.5, date(2006, 7, 6): 95.0,
           date(2006, 7, 7): 95.62, date(2006, 7, 10): 96.0, date(2006, 7, 11): 95.2, date(2006, 7, 12): 94.8}
    for day, b in b06.items():
        rows.append(("ELUX-B.ST", day, b * 0.55, b, 1.48e6))
        if day < date(2006, 7, 7):
            rows.append(("ELUX-A.ST", day, 48.25, 84.18, 0.0))
        else:
            rows.append(("ELUX-A.ST", day, 30.05 if day == date(2006, 7, 7) else 30.2,
                         52.43 if day == date(2006, 7, 7) else 52.7, 950.0 if day == date(2006, 7, 7) else 213.0))
    for day in weekdays(date(2007, 7, 2), date(2007, 7, 12)):
        rows += [("ELUX-A.ST", day, 92.4, 156.37, 4000.0), ("ELUX-B.ST", day, 86.4, 153.72, 3e6)]
    rows += list(extra)
    con = cache(rows)
    con.execute(CAP_INPUTS_DDL)
    for s in ("ELUX-A.ST", "ELUX-B.ST"):
        cap_insert(con, [{"symbol": s, "filing_epoch": ep(date(y, 12, 31)), "market_cap": cap}
                         for y, cap in ((2005, 27.529e9), (2006, 36.204e9))])
    return con, wins


def t2_failures():
    """ELUX-A.ST 2006 (cyclical-timing Sweden): T moved the entry off the zero-volume 07-03 row (84.18,
    0.915 of B, on scale) onto 07-07, 52.43 on 950 shares, while ELUX-B traded 95.62 on 1.48m: A over
    B 0.548 against a reference 1.03 (the median of the 2005 and 2007 window medians). Every traded A
    row of the window sits that far off, so they move (sibling_gap) and the entry is lost, where
    before it booked from 30.05 (+207.5% to 2007). B is untouched, and the 2007 window, where A traded
    on B's scale, too. Controls: a print 24% above B's scale stays; a pair whose daily ratio swings
    60% (V.ST, W.ST) is not tested."""
    con, (w05, w06, w07) = elux_con()
    clean_price_cache(con, set(), [w05, w06, w07])
    f = []
    if get_prices(con, ["ELUX-A.ST"], w06, offset_days=1).get("ELUX-A.ST") != 30.05:
        f.append("without the sibling test the 2006 entry is the 07-07 print (30.05): the case would test nothing")
    con, _ = elux_con()
    try:
        st = clean_price_cache(con, set(), [w05, w06, w07], siblings="cap_inputs")
    except TypeError as e:
        return f + [f"clean_price_cache has no sibling test ({e})"]
    ref = math.sqrt(1.04 * 156.37 / 153.72)   # the median of the 2005 and 2007 window medians
    a = get_prices(con, ["ELUX-A.ST", "ELUX-B.ST"], w06, offset_days=1)
    if "ELUX-A.ST" in a:
        f.append(f"ELUX-A.ST 2006 entry {a['ELUX-A.ST']}, want none (every traded row off B's scale)")
    if abs(a.get("ELUX-B.ST", 0) - 92.0 * 0.55) > 1e-9:
        f.append(f"ELUX-B.ST 2006 entry {a.get('ELUX-B.ST')}")
    if get_prices(con, ["ELUX-A.ST"], w07, offset_days=1).get("ELUX-A.ST") != 92.4:
        f.append("ELUX-A.ST 2007 (on B's scale) keeps its rows")
    if st.get("sibling_gap_rows") != 4 or {g["sibling"] for g in st.get("sibling_gaps", [])} != {"ELUX-B.ST"}:
        f.append(f"sibling gaps: {st.get('sibling_gap_rows')} rows {st.get('sibling_gaps')}")
    elif abs(st["sibling_gaps"][0]["ref"] - ref) > 1e-4 or abs(st["sibling_gaps"][0]["ratio"] - 52.43 / 95.62) > 1e-4:
        f.append(f"gap {st['sibling_gaps'][0]}, want ratio 0.548 against a reference {ref:.4f}")
    # controls: a print 24% above B's scale (ELUX-A 2008 shape) stays; a pair that trades apart is not tested
    d08 = date(2006, 7, 10)
    con, wins = elux_con()
    con.execute("UPDATE prices_cache SET close = ?, adjClose = 50.0 WHERE symbol = 'ELUX-A.ST' AND trade_epoch = ?",
                [96.0 * ref * 1.24, ep(d08)])
    con.execute("DELETE FROM prices_cache WHERE symbol = 'ELUX-A.ST' AND trade_epoch = ?", [ep(date(2006, 7, 7))])
    st = clean_price_cache(con, set(), list(wins), siblings="cap_inputs")
    if get_prices(con, ["ELUX-A.ST"], w06, offset_days=1).get("ELUX-A.ST") != 50.0:
        f.append(f"a print 24% above B's scale is kept: {st.get('sibling_gaps')}")
    # V.ST (thin) beside W.ST, whose close swings 1.0 / 1.6 day to day: the window medians agree, the days
    # do not, so the pair is not tested. V's 2006 window opens on a zero-volume row on W's scale.
    wild = [("W.ST", day, 1.0, 1.0 + 0.6 * (k % 2), 2e5) for w in wins
            for k, day in enumerate(weekdays(w, w + timedelta(days=11)))]
    wild += [("V.ST", day, 1.0, 1.0, 1e5) for w in wins for day in weekdays(w, w + timedelta(days=11))]
    con, _ = elux_con(wild)
    cap_insert(con, [{"symbol": s, "filing_epoch": fe, "market_cap": 1e9} for s in ("V.ST", "W.ST") for fe in (1, 2)])
    con.execute("UPDATE prices_cache SET volume = 0 WHERE symbol = 'V.ST' AND trade_epoch = ?", [ep(date(2006, 7, 3))])
    con.execute("UPDATE prices_cache SET close = 3.0 WHERE symbol = 'V.ST' AND trade_epoch = ?", [ep(date(2006, 7, 4))])
    st = clean_price_cache(con, set(), list(wins), siblings="cap_inputs")
    if any(g["symbol"] == "V.ST" for g in st["sibling_gaps"]):
        f.append(f"a pair that trades apart is not tested: {st['sibling_gaps']}")
    return f


# ELUX-A.ST and ELUX-B.ST in cyclical-timing's July windows, as stock_eod prints them.
# yy-mm-dd: (A close, A adjClose, A volume, B close, B adjClose, B volume)
ELUX_JULY = {
    "05-07-01": (79.59, 42.5, 852, 76.17, 38.75, 3.29216e+06), "05-07-04": (79.59, 42.5, 427, 75.72, 38.52, 2.02548e+06),
    "05-07-05": (78.18, 41.75, 747, 75.72, 38.52, 3.13486e+06), "05-07-06": (120.66, 64.43, 0, 75.94, 38.63, 1.49074e+06),
    "05-07-07": (78.18, 41.75, 1708, 74.8, 38.05, 5.19908e+06), "05-07-08": (79.59, 42.5, 427, 75.94, 38.63, 1.63104e+06),
    "05-07-11": (81.23, 43.38, 854, 77.09, 39.22, 2.64461e+06), "05-07-12": (81.23, 43.38, 1097, 77.55, 39.45, 2.46516e+06),
    "06-07-03": (84.18, 48.25, 0, 96.53, 52.97, 1.65008e+06), "06-07-04": (84.18, 48.25, 0, 96.53, 52.97, 1.51486e+06),
    "06-07-05": (84.18, 48.25, 0, 93.79, 51.47, 2.43748e+06), "06-07-06": (84.18, 48.25, 0, 96.53, 52.97, 2.47574e+06),
    "06-07-07": (52.43, 30.05, 950, 95.62, 52.47, 1.48376e+06), "06-07-10": (52.43, 30.05, 213, 96.99, 53.22, 1.92146e+06),
    "06-07-11": (52.43, 30.05, 213, 94.25, 51.72, 1.58166e+06), "06-07-12": (52.43, 30.05, 213, 91.96, 50.46, 5.40956e+06),
    "07-07-02": (163.86, 96.82, 0, 149.6, 84.09, 2.16132e+06), "07-07-03": (156.37, 92.4, 152, 153.72, 86.4, 2.77659e+06),
    "07-07-04": (156.84, 92.68, 1244, 150.98, 84.86, 1.7064e+06), "07-07-05": (159.18, 94.06, 106, 149.6, 84.09, 2.09242e+06),
    "07-07-06": (165.26, 97.65, 961, 151.43, 85.12, 1.2018e+06), "07-07-09": (165.26, 97.65, 213, 153.26, 86.15, 2.90351e+06),
    "07-07-10": (165.26, 97.65, 106, 149.15, 83.84, 2.28266e+06), "07-07-11": (127.53, 75.36, 0, 146.4, 82.29, 4.37829e+06),
    "07-07-12": (164.79, 97.37, 427, 147.32, 82.81, 3.26446e+06), "08-07-01": (62.14, 38.38, 0, 66.34, 39.07, 4.75955e+06),
    "08-07-02": (62.14, 38.38, 0, 67.25, 39.61, 3.31558e+06), "08-07-03": (62.14, 38.38, 0, 65.88, 38.8, 3.66181e+06),
    "08-07-04": (80.52, 49.73, 352, 64.74, 38.13, 1.31169e+06), "08-07-07": (62.14, 38.38, 0, 64.74, 38.13, 2.08662e+06),
    "08-07-08": (62.14, 38.38, 0, 64.97, 38.26, 2.18244e+06), "08-07-09": (62.14, 38.38, 0, 65.42, 38.53, 2.68216e+06),
    "08-07-10": (62.14, 38.38, 0, 63.82, 37.59, 2.81879e+06), "08-07-11": (62.14, 38.38, 0, 61.53, 36.24, 2.01289e+06),
}


def pair_con(rows, wins, pairs, closed=()):
    """A cache of `rows` plus 30 filler lines trading every weekday of each window but the `closed`
    days, and cap_inputs where each pair of lines shares an identical cap in two FYs."""
    rows = list(rows)
    for w in wins:
        rows += fillers(30, list(weekdays(w, w + timedelta(days=11))), traded=lambda i, d: d not in closed)
    con = cache(rows)
    con.execute(CAP_INPUTS_DDL)
    for i, group in enumerate(pairs):
        cap_insert(con, [{"symbol": s, "filing_epoch": fe, "market_cap": 1e10 * (i + 1) + fe}
                         for s in group for fe in (1, 2)])
    return con


def elux_real_con(years=(2005, 2006, 2007, 2008), keep_one_traded=()):
    """ELUX_JULY's rows for the given July windows; in a window listed in keep_one_traded, A keeps only
    its first traded row's volume."""
    wins = [date(y, 7, 1) for y in years]
    rows, seen = [], set()
    for k, (ac, aa, av, bc, ba, bv) in sorted(ELUX_JULY.items()):
        d = date.fromisoformat("20" + k)
        if d.year not in years:
            continue
        if d.year in keep_one_traded and av > 0:
            av = av if d.year not in seen else 0
            seen.add(d.year)
        rows += [("ELUX-A.ST", d, aa, ac, float(av)), ("ELUX-B.ST", d, ba, bc, float(bv))]
    return pair_con(rows, wins, [("ELUX-A.ST", "ELUX-B.ST")]), wins


def t2_real_failures():
    """ELUX-A/B's real July rows. 2006: the 52.43 prints (0.55 of B) leave and the entry is lost, as in
    t2_failures. 2007: A's 07-02 and 07-11 rows have no volume (163.86 and 127.53), step 2 moves them, so
    T moved the window's first bar; the reference against B is the 2006 window (0.55, the anomaly) and
    one 2008 day (1.24), two windows that disagree, so the pair is not tested and A's 7 traded rows on
    B's scale stay (entry 92.4 on 07-03). Before, the reference pooled those 5 days (median 0.556, 4 of 5
    in the band), all 7 rows moved and the 2007 entry was lost. 2008: one reference window (2007) is
    not enough, so A's 07-04 print stays. A reference needs 2 windows and 5 days: with 2005 beside 2006
    alone, or 2005 and 2007 holding one traded A day each, 2006 is not tested."""
    con, (w05, w06, w07, w08) = elux_real_con()
    st = clean_price_cache(con, set(), [w05, w06, w07, w08], siblings="cap_inputs")
    f = []
    a = {w: get_prices(con, ["ELUX-A.ST"], w, offset_days=1).get("ELUX-A.ST") for w in (w05, w06, w07, w08)}
    if a != {w05: 42.5, w06: None, w07: 92.4, w08: 49.73}:
        f.append(f"ELUX-A.ST entries {a}, want 42.5, none (2006), 92.4 (2007, on B's scale), 49.73")
    kept = con.execute("SELECT COUNT(*) FROM prices_cache WHERE symbol = 'ELUX-A.ST' AND volume > 0 AND trade_epoch"
                       " BETWEEN ? AND ?", [ep(w07), ep(w07 + timedelta(days=11))]).fetchone()[0]
    if kept != 7:
        f.append(f"ELUX-A.ST 2007 keeps {kept} traded rows, want 7")
    if sorted(g["date"] for g in st.get("sibling_gaps", [])) != ["2006-07-07", "2006-07-10", "2006-07-11", "2006-07-12"]:
        f.append(f"sibling gaps {st.get('sibling_gaps')}, want A's four 2006 prints")
    for years, one, why in (((2005, 2006), (), "one reference window"),
                            ((2005, 2006, 2007), (2005, 2007), "two reference days")):
        con, wins = elux_real_con(years, one)
        clean_price_cache(con, set(), wins, siblings="cap_inputs")
        got = get_prices(con, ["ELUX-A.ST"], w06, offset_days=1).get("ELUX-A.ST")
        if got != 30.05:
            f.append(f"ELUX-A.ST 2006 with {why}: entry {got}, want 30.05 (not tested)")
    return f


def t2_pref_failures():
    """Lines that trade apart from their sibling for real must not move.
    (1) LG Chem, deleveraging Korea 2004-04: common 051910.KS (55,000 on 5e5 shares) and pref 051915.KS
    (2.5e4), common over pref 2.24, 2.30, 2.13, 1.70, 1.66, 1.67, 1.44 by window, and a holiday on
    2004-04-05: nothing moves, entries 40,700 and 19,108. Before, 14 rows of both lines moved and both
    entries were lost. (2) Samsung Heavy's pref 010145.KS against 010140.KS (stock_eod rows of 2023-01
    and 2023-07; the other windows at their real median ratios, 32 to 42): in 2023-01 the zero-volume
    01-02 row (24.3 times the common) moves, in 2023-07 the zero-volume rows at 98,600 (14.5 times). The
    bar T moved was already off the reference (about 36), so the pair is not tested: entries 109,500
    and 53,500 on 74,434 shares. Before, the 2023-01 entry moved to 184,500 and the 2023-07 one was
    lost. (3) BP-B.L, a fixed-rate preference, against BP.L, 2016-01 rows: the zero-volume rows at 161
    (0.463 of BP.L, inside the band of the 0.373 reference) move; 01-07's 160.5 (0.475) sits within the
    band of that moved bar, so T's move opened no gap (BP.L fell): entry 160.5. Before, it was lost.
    (4) Lines that share one FY cap are not siblings (1789.HK and 1935.HK, crafted prices): 1935.HK's
    2025-04 entry stays at its print 27% under the pair's usual ratio. Before, it moved."""
    f = []
    # (1)
    wins = [date(y, m, 1) for y, m in ((2003, 10), (2004, 1), (2004, 4), (2004, 7), (2004, 10), (2005, 1), (2005, 4))]
    ratio = dict(zip(wins, (2.24, 2.30, 2.13, 1.70, 1.66, 1.67, 1.44)))
    rows = []
    for w in wins:
        for k, d in enumerate(weekdays(w, w + timedelta(days=11))):
            hol = d == date(2004, 4, 5)
            thin = hol or (w == wins[0] and k > 0) or (w == wins[1] and k > 4)
            rows += [("051910.KS", d, 55000 * 0.74, 55000.0, 0.0 if hol else 5e5),
                     ("051915.KS", d, 55000 / ratio[w] * 0.74, 55000 / ratio[w], 0.0 if thin else 2.5e4)]
    con = pair_con(rows, wins, [("051910.KS", "051915.KS")], closed=(date(2004, 4, 5),))
    st = clean_price_cache(con, set(), wins, siblings="cap_inputs")
    e = get_prices(con, ["051910.KS", "051915.KS"], date(2004, 4, 1), offset_days=1)
    if st.get("sibling_gap_rows") or abs(e.get("051910.KS", 0) - 40700) > 1e-6 or \
            abs(e.get("051915.KS", 0) - 55000 / 2.13 * 0.74) > 1e-6:
        f.append(f"LG Chem: {st.get('sibling_gap_rows')} rows moved, 2004-04 entries {e}, want none and 40,700 / 19,108")
    # (2)
    real = {date(2023, 1, 2): (123500, 0, 5080), date(2023, 1, 3): (109500, 7377, 4965),
            date(2023, 1, 4): (142000, 3750, 5050), date(2023, 1, 5): (184500, 7507, 4935),
            date(2023, 1, 6): (206500, 20863, 5000), date(2023, 1, 9): (190000, 8888, 5110),
            date(2023, 1, 10): (170500, 5944, 5060), date(2023, 1, 11): (211500, 15057, 5120),
            date(2023, 1, 12): (196500, 9613, 5090), date(2023, 7, 3): (98600, 0, 6810),
            date(2023, 7, 4): (98600, 0, 6940), date(2023, 7, 5): (98600, 0, 7060),
            date(2023, 7, 6): (53500, 74434, 7050), date(2023, 7, 7): (77400, 124284, 7100),
            date(2023, 7, 10): (42500, 58020, 7200), date(2023, 7, 11): (36350, 61315, 7540),
            date(2023, 7, 12): (32000, 61841, 8170)}
    med = {date(2022, 1, 1): 35.80, date(2022, 4, 1): 40.81, date(2022, 7, 1): 41.59, date(2022, 10, 1): 36.16,
           date(2023, 4, 1): 31.99}
    wins = sorted(list(med) + [date(2023, 1, 1), date(2023, 7, 1)])
    rows = [r for d, (p, v, c) in real.items() for r in (("010145.KS", d, float(p), float(p), float(v)),
                                                          ("010140.KS", d, float(c), float(c), 5e6))]
    for w, m in med.items():
        rows += [r for d in weekdays(w, w + timedelta(days=11))
                 for r in (("010145.KS", d, 5700 * m, 5700 * m, 500.0), ("010140.KS", d, 5700.0, 5700.0, 3e6))]
    con = pair_con(rows, wins, [("010140.KS", "010145.KS")])
    st = clean_price_cache(con, set(), wins, siblings="cap_inputs")
    e = {w: get_prices(con, ["010145.KS"], w, offset_days=1).get("010145.KS") for w in (date(2023, 1, 1), date(2023, 7, 1))}
    if st.get("sibling_gap_rows") or e != {date(2023, 1, 1): 109500.0, date(2023, 7, 1): 53500.0}:
        f.append(f"010145.KS: {st.get('sibling_gaps')}, entries {e}, want none moved and 109,500 / 53,500")
    # (3)
    real = {date(2016, 1, 4): (161.0, 0, 347.55), date(2016, 1, 5): (161.0, 0, 348.15),
            date(2016, 1, 6): (161.0, 0, 343.45), date(2016, 1, 7): (160.5, 4200, 337.7),
            date(2016, 1, 8): (160.5, 520, 328.7), date(2016, 1, 11): (160.5, 24, 328.05),
            date(2016, 1, 12): (160.5, 0, 323.1)}
    med = {date(2015, 1, 1): 0.4089, date(2015, 4, 1): 0.3586, date(2015, 7, 1): 0.3874, date(2015, 10, 1): 0.4238,
           date(2016, 4, 1): 0.4479, date(2016, 7, 1): 0.3285, date(2016, 10, 1): 0.3419, date(2017, 1, 1): 0.3205}
    wins = sorted(list(med) + [date(2016, 1, 1)])
    rows = [r for d, (p, v, c) in real.items() for r in (("BP-B.L", d, p, p, float(v)), ("BP.L", d, c, c, 3e7))]
    for w, m in med.items():
        rows += [r for d in weekdays(w, w + timedelta(days=11))
                 for r in (("BP-B.L", d, 400 * m, 400 * m, 1000.0), ("BP.L", d, 400.0, 400.0, 4e7))]
    con = pair_con(rows, wins, [("BP.L", "BP-B.L")])
    st = clean_price_cache(con, set(), wins, siblings="cap_inputs")
    e = get_prices(con, ["BP-B.L"], date(2016, 1, 1), offset_days=1).get("BP-B.L")
    if st.get("sibling_gap_rows") or e != 160.5:
        f.append(f"BP-B.L 2016-01: {st.get('sibling_gaps')}, entry {e}, want none moved and 160.5")
    # (4) unrelated companies with one coincidental FY cap (1789.HK and 1935.HK, FY2016 2,362,800,000) are
    # not siblings: 1935.HK's 2025-04 window opens on a zero-volume row and it then trades 27% lower
    wins = [date(y, m, 1) for y, m in ((2024, 1), (2024, 4), (2024, 7), (2024, 10), (2025, 1), (2025, 4), (2025, 7))]
    rows = []
    for w in wins:
        for k, d in enumerate(weekdays(w, w + timedelta(days=11))):
            low = w == date(2025, 4, 1)
            rows += [("1789.HK", d, 7.0, 7.0, 1e7),
                     ("1935.HK", d, 0.58 if low and k else 0.80, 0.58 if low and k else 0.80, 0.0 if low and not k else 1e5)]
    con = pair_con(rows, wins, [])
    cap_insert(con, [{"symbol": s, "filing_epoch": ep(date(y, 12, 31)), "market_cap": cap}
                     for s, y, cap in (("1789.HK", 2016, 2362800000.0), ("1935.HK", 2016, 2362800000.0),
                                       ("1789.HK", 2017, 5.1e9), ("1935.HK", 2017, 2.0e9))])
    st = clean_price_cache(con, set(), wins, siblings="cap_inputs")
    e = get_prices(con, ["1935.HK"], date(2025, 4, 1), offset_days=1).get("1935.HK")
    if st.get("sibling_gap_rows") or e != 0.58:
        f.append(f"1935.HK 2025-04: {st.get('sibling_gaps')}, entry {e}, want none moved and 0.58 (one shared FY)")
    return f


def t2_three_failures():
    """Annual windows 2005 to 2007, every pair at par outside 2006; 2006-07-05 is a market holiday.
    (1) Three classes: A (thin) beside B and C (liquid). A's 2006 window opens on a zero-volume row at
    par, which T moves; on 07-04 B prints 160 while A and C print 100. A's 07-04 row is off B's scale
    and on C's, so it stays: every sibling that can test a row must flag it. (2) The liquid line is
    never moved: L (1e6 shares) beside T (500); L's window opens on a zero-volume row and T prints at
    0.55 of L from 07-04. L keeps its rows (entry 07-04's). Before, L's rows off T's prints moved and
    L's entry was lost. (3) Only the leading run of off rows moves: D's window opens on a zero-volume
    row; D prints 55 on 07-04 (off E's scale), 100 on 07-06, 55 again on 07-07. 07-04 moves, 07-06 is
    the entry and stops the test, 07-07 stays. (4) A window where T moved only a later row is not
    tested: G opens on a real 130 print on 07-03 (H at 100), and only the 07-05 holiday row moves, so
    G's entry stays 07-03's."""
    wins, hol = [date(2005, 7, 1), date(2006, 7, 1), date(2007, 7, 1)], date(2006, 7, 5)
    rows = []
    for w in wins:
        for k, d in enumerate(weekdays(w, w + timedelta(days=11))):
            first, odd, mid = (w == wins[1] and k == 0), (w == wins[1] and k == 1), w == wins[1]
            rows += [("A.ST", d, 100.0 + k, 100.0, 0.0 if first else 500.0),
                     ("B.ST", d, 100.0, 160.0 if odd else 100.0, 1e6), ("C.ST", d, 100.0, 100.0, 2e6),
                     ("L.ST", d, 100.0 + k, 100.0, 0.0 if first else 1e6),
                     ("T.ST", d, 100.0, 55.0 if mid and k > 0 else 100.0, 500.0),
                     ("D.ST", d, 100.0 + k, 55.0 if mid and d in (date(2006, 7, 4), date(2006, 7, 7)) else 100.0,
                      0.0 if first else 500.0), ("E.ST", d, 100.0, 100.0, 1e6),
                     ("G.ST", d, 100.0 + k, 130.0 if first else 100.0, 500.0), ("H.ST", d, 100.0, 100.0, 1e6)]
    con = pair_con(rows, wins, [("A.ST", "B.ST", "C.ST"), ("L.ST", "T.ST"), ("D.ST", "E.ST"), ("G.ST", "H.ST")],
                   closed=(hol,))
    st = clean_price_cache(con, set(), wins, siblings="cap_inputs")
    e = get_prices(con, ["A.ST", "L.ST", "D.ST", "G.ST"], wins[1], offset_days=1)
    f = []
    if st.get("closed_dates", [{}])[0].get("date") != hol.isoformat():
        f.append(f"closed dates {st.get('closed_dates')}, want the 2006-07-05 holiday")
    for s, want, why in (("A.ST", 101.0, "07-04: off B's scale, on C's"), ("L.ST", 101.0, "the liquid line keeps its rows"),
                         ("D.ST", 103.0, "07-04 moves, 07-06 stops the test"), ("G.ST", 100.0, "T moved no first bar")):
        if e.get(s) != want:
            f.append(f"{s} 2006 entry {e.get(s)}, want {want} ({why})")
    if [(g["symbol"], g["date"]) for g in st.get("sibling_gaps", [])] != [("D.ST", "2006-07-04")]:
        f.append(f"sibling gaps {st.get('sibling_gaps')}, want D.ST's 07-04 alone")
    return f


# ---------------------------------------------------------------- F (flags only) and L

F_CASES = {
    # symbol: (entry adj, entry close, exit adj, exit close, calendar splits, want reasons)
    "3003.SR": (9.0, 24.0, 27.477, 32.69, [], ["band", "large_move"]),     # step 2.1x the close does not mirror
    "TSG.JO": (1392.0, 1401.0, 840.8, 1499.1, [], ["band"]),               # adjClose -39.6%, close +7.0%
    "2603.TW": (70.0, 155.0, 115.8, 140.0, [], ["band"]),                  # real dividend, still flagged (no repair)
    "SPLIT2": (100.0, 100.0, 60.0, 60.0, [], []),                          # 2:1 in both columns, no calendar row
    "SALEE.BK": (4.88, 5.92, 1.17, 1.41, [(date(2015, 5, 21), 4, 1)], ["large_move", "split"]),
    "BACKWARD": (10.0, 10.0, 11.0, 11.0, [(date(2015, 6, 1), 1, 5)], ["split"]),   # calendar only, smooth prices
    "DAVE": (10.0, 10.0, 31.74, 31.74, [], ["large_move"]),               # a real +217% move: listed, unchanged
    "PLAIN": (10.0, 10.0, 11.0, 11.0, [(date(2015, 6, 1), 2, 3)], []),     # ratio 1.5: under split_min
}


def f_failures(dc_cls=DataChecks, cfg=DATA_CHECKS_V1):
    d, x = date(2015, 4, 1), date(2015, 7, 1)
    rows, splits = [], []
    for s, (ea, ec, xa, xc, sp, _) in F_CASES.items():
        rows += [(s, date(2015, 4, 2), ea, ec, 1000.0), (s, date(2015, 7, 2), xa, xc, 1000.0)]
        splits += [(s, ep(day), num, den) for day, num, den in sp]
    con = cache(rows)
    con.execute("CREATE TABLE prices_dropped(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE, reason VARCHAR, window_start BIGINT)")
    con.execute("CREATE TABLE splits(symbol VARCHAR, split_epoch BIGINT, numerator DOUBLE, denominator DOUBLE)")
    con.executemany("INSERT INTO splits VALUES (?, ?, ?, ?)", splits)
    before = con.execute("SELECT * FROM prices_cache ORDER BY ALL").fetchall()
    sd = [(s, *[get_prices(con, [s], t, offset_days=1).get(s) for t in (d, x)], 1e10) for s in F_CASES]
    dc = dc_cls("on", cfg)
    dc.prices(con, d, x, sd, 1, 1.0)
    f = []
    got = {e["symbol"]: e for e in dc.events if e["check"] == "factor_flag"}
    for s, (ea, ec, xa, xc, sp, want) in F_CASES.items():
        e = got.get(s)
        if sorted(e["reasons"] if e else []) != want:
            f.append(f"{s}: reasons {e['reasons'] if e else []}, want {want}")
        elif e and abs(e["r_adj"] - (xa / ea - 1)) > 1e-5:
            f.append(f"{s}: r_adj {e['r_adj']}")
    if con.execute("SELECT * FROM prices_cache ORDER BY ALL").fetchall() != before:
        f.append("F changed a price")
    lm = sorted(m["symbol"] for m in dc.large_moves)
    if lm != ["3003.SR", "DAVE", "SALEE.BK"]:
        f.append(f"large moves {lm}")
    if dc.factor_checked != len(F_CASES) or dc.row(d)["factor_flags"] != 6:
        f.append(f"counts: checked {dc.factor_checked}, flagged {dc.row(d)['factor_flags']}")
    return f


class NoFlags(DataChecks):
    """Mutant: F's detection stubbed (no split lookup, a band that holds everything)."""

    def _splits(self, con, spans):
        return {}


def f_mutant_cfg():
    cfg = copy.deepcopy(DATA_CHECKS_V1)
    cfg["factor"]["band"] = [0.0, float("inf")]
    return cfg


# ---------------------------------------------------------------- shared code against BASE

def load_base(tmp, ref):
    try:
        src = subprocess.run(["git", "show", f"{ref}:data_utils.py"], cwd=ROOT, capture_output=True, text=True,
                             check=True).stdout
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"FAIL: cannot read data_utils.py at {ref} ({e.stderr.strip()}); fetch the full history")
    path = os.path.join(tmp, "du_base_for_identity.py")
    with open(path, "w") as fh:
        fh.write(src)
    spec = importlib.util.spec_from_file_location("du_base_for_identity", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, src


CHANGED = {"isin_dedup_sql", "filter_returns", "LtpExits"}
LTP_CHANGED = {"__init__", "fill", "_last_bars", "provenance"}


def ast_failures(base_src):
    """Every function, class and constant of BASE's data_utils is unchanged, except the three that
    gained a default-off kwarg (and in LtpExits only the methods that carry it)."""
    old, new = ast.parse(base_src), ast.parse(open(os.path.join(ROOT, "data_utils.py")).read())

    def names(tree):
        out = {}
        for n in tree.body:
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)):
                out[n.name] = n
            elif isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                out[n.targets[0].id] = n
        return out

    a, b = names(old), names(new)
    f = []
    for k, node in a.items():
        if k not in b:
            f.append(f"{k} removed")
        elif k not in CHANGED and ast.dump(node) != ast.dump(b[k]):
            f.append(f"{k} changed")
    ma = {n.name: n for n in a["LtpExits"].body if isinstance(n, ast.FunctionDef)}
    mb = {n.name: n for n in b["LtpExits"].body if isinstance(n, ast.FunctionDef)}
    for k, node in ma.items():
        if k not in LTP_CHANGED and (k not in mb or ast.dump(node) != ast.dump(mb[k])):
            f.append(f"LtpExits.{k} changed")
    imp = lambda t: {ast.dump(n) for n in t.body if isinstance(n, (ast.Import, ast.ImportFrom))}
    if not imp(old) <= imp(new):
        f.append("an import was removed")
    return f


def dedup_identity_failures(base):
    f = []
    for d in (date(2013, 1, 1), date(2024, 2, 29)):
        for off in (0, 1):
            for floor in (0.5, 1.0):
                for kw in ({}, {"window_days": 30}, {"isin_table": "m2", "prices_table": "p2"}):
                    args = ("SELECT symbol FROM c WHERE x > ?", d, off, floor)
                    if du.isin_dedup_sql(*args, **kw) != base.isin_dedup_sql(*args, **kw):
                        f.append(f"isin_dedup_sql{args[1:]} {kw}")
                    if du.isin_dedup_sql(*args, twin_key=False, **kw) != base.isin_dedup_sql(*args, **kw):
                        f.append(f"isin_dedup_sql(twin_key=False){args[1:]} {kw}")
    return f


def filter_identity_failures(base):
    rnd = random.Random(20261030)
    pool = [None, 0.0, -1.0, 0.4, 0.99, 1.0, 1.5, 3.0, 10.0, 25.0, 100.0, 1e4]
    f = []
    for i in range(10000):
        rows = [(f"S{j}", rnd.choice(pool) if rnd.random() < 0.3 else rnd.uniform(0.01, 200),
                 rnd.choice(pool) if rnd.random() < 0.3 else rnd.uniform(0.01, 900), rnd.uniform(1e6, 1e12))
                for j in range(rnd.randint(0, 6))]
        kw = rnd.choice([{}, {"min_entry_price": 0.5}, {"max_single_return": 3.0}, {"min_entry_price": 0.0}])
        if filter_returns(rows, **kw) != base.filter_returns(rows, **kw):
            f.append(f"filter_returns {rows} {kw}")
            break
    return f


class LtpClient:
    """Serves delisted_companies and runs the remote stock_eod SQL locally; records every SQL."""

    def __init__(self, src):
        self.src, self.sqls = src, []

    def query(self, sql, **_kw):
        self.sqls.append(sql)
        cur = self.src.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def ltp_identity_failures(base):
    """LtpExits(traded_only=False) sends BASE's SQL, builds BASE's temp table and returns BASE's fills."""
    src = duckdb.connect()
    src.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate DATE)")
    src.execute("INSERT INTO delisted_companies VALUES ('DL', '2018-05-20'), ('OLD', '2001-01-01')")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    d, x = date(2018, 4, 1), date(2018, 7, 1)
    for s, last in (("DL", date(2018, 5, 15)), ("OLD", date(2018, 6, 20)), ("GAP", date(2018, 6, 1))):
        for k, day in enumerate(weekdays(date(2018, 3, 1), last)):
            src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, ?, ?, ?)",
                        [s, day.isoformat(), ep(day), 10.0 + 0.1 * k, 11.0, 0.0 if k % 7 == 6 else 100.0])
    out = []
    for mod in (base, du):
        con = duckdb.connect()
        con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
        for s in ("DL", "OLD", "GAP"):
            con.execute("INSERT INTO prices_cache VALUES (?, ?, 10)", [s, ep(date(2018, 4, 2))])
        schemas, orig = [], mod.remove_price_oscillations

        def spy(c, *a, **k):
            schemas.append(c.execute("DESCRIBE prices_cache").fetchall())
            return orig(c, *a, **k)

        mod.remove_price_oscillations = spy
        try:
            client = LtpClient(src)
            ltp = mod.LtpExits(client, con) if mod is base else mod.LtpExits(client, con, traded_only=False)
            fill = ltp.fill(["DL", "OLD", "GAP"], {s: 10.0 for s in ("DL", "OLD", "GAP")}, {}, d, x, offset_days=1)
        finally:
            mod.remove_price_oscillations = orig
        out.append((client.sqls, schemas, fill, ltp.provenance()))
    f = []
    for i, label in enumerate(("SQL sent", "temp table schema", "fills", "provenance")):
        if out[0][i] != out[1][i]:
            f.append(f"LtpExits {label}: {first_diff(out[0][i], out[1][i])}")
    if not out[0][2]:
        f.append("the LTP identity case filled nothing: it would test nothing")
    return f


def ltp_src(rows):
    """The remote side of an LTP fill: no delisting rows, stock_eod (symbol, date, adjClose, close, volume)."""
    src = duckdb.connect()
    src.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate DATE)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    src.executemany("INSERT INTO stock_eod VALUES (?, ?, ?, ?, ?, ?)",
                    [(s, day.isoformat(), ep(day), a, c, v) for s, day, a, c, v in rows])
    return src


def ltp_traded_failures():
    """traded_only=True: a line with a traded row in its LTP window (entry bar, exit date] loses its
    zero-volume rows over the fetched span; fills carry ltp_close and ltp_volume. HALT trades only on
    its entry bar and then prints zero-volume repeats: like a T window with no traded row, it keeps
    them, and fills at the last one."""
    rows = [("Z", day, 10.0, 12.0, 100.0) for day in weekdays(date(2018, 3, 1), date(2018, 5, 15))]
    rows += [("Z", date(2018, 3, 17), 10.0, 12.0, 0.0),    # zero-volume context row before the entry bar
             ("Z", date(2018, 5, 16), 15.0, 18.0, 0.0)]
    rows += [("HALT", date(2018, 4, 2), 10.0, 12.0, 900.0)]
    rows += [("HALT", day, 10.0 if day < date(2018, 6, 28) else 9.5, 12.0, 0.0)
             for day in (date(2018, 4, 3), date(2018, 5, 1), date(2018, 6, 28))]
    src = ltp_src(rows)
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE, volume DOUBLE)")
    for s in ("Z", "HALT"):
        con.execute("INSERT INTO prices_cache VALUES (?, ?, 10, 12, 100)", [s, ep(date(2018, 4, 2))])
    f = []
    for traded, want in ((False, {"Z": 15.0, "HALT": 9.5}), (True, {"Z": 10.0, "HALT": 9.5})):
        ltp = du.LtpExits(LtpClient(src), con, traded_only=traded)
        got = ltp.fill(["Z", "HALT"], {"Z": 10.0, "HALT": 10.0}, {}, date(2018, 4, 1), date(2018, 7, 1), offset_days=1)
        if got != want:
            f.append(f"traded_only={traded}: fill {got}, want {want}")
        p = ltp.provenance()
        z = next((x for x in ltp.fills if x["symbol"] == "Z"), {})
        if traded and not (p["traded_only"] and p["zero_volume_rows_moved"] == 2
                           and z.get("ltp_close") == 12.0 and z.get("ltp_volume") == 100.0):
            f.append(f"traded_only provenance {p}")
        if not traded and ("traded_only" in p or "ltp_close" in z):
            f.append("traded_only=False adds nothing")
    return f


def ltp_large_move_failures():
    """L on LTP fills: JUMP fills at +300%, DROP at -70%, FLAT at +10%. The ltp block lists the two
    large moves and flags them on their fills; the window-bar block stays empty (strip_ltp equality)."""
    d, x = date(2013, 4, 1), date(2013, 7, 1)
    path = {"JUMP": (10.0, 20.0, 40.0), "DROP": (10.0, 6.0, 3.0), "FLAT": (10.0, 10.5, 11.0)}
    rows = [(s, day, p, p, 500.0) for s, ps in path.items()
            for day, p in zip((date(2013, 4, 2), date(2013, 5, 2), date(2013, 6, 20)), ps)]
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE splits(symbol VARCHAR, split_epoch BIGINT, numerator DOUBLE, denominator DOUBLE)")
    for s in path:
        con.execute("INSERT INTO prices_cache VALUES (?, ?, 10, 10, 1000)", [s, ep(date(2013, 4, 2))])
    ltp = du.LtpExits(LtpClient(ltp_src(rows)), con, traded_only=True)
    fill = ltp.fill(list(path), {s: 10.0 for s in path}, {}, d, x, offset_days=1, min_entry_price=1.0)
    f = []
    if fill != {"JUMP": 40.0, "DROP": 3.0, "FLAT": 11.0}:
        f.append(f"fills {fill}")
    block = ltp.results_block("ltp", [], 4, 0.02)
    dc = DataChecks("on")
    dc.annotate_ltp(con, ltp, block)
    lm = [(m.get("symbol"), m.get("return")) for m in block.get("large_moves", [])]
    if lm != [("DROP", -0.7), ("JUMP", 3.0)] or block.get("large_move_fills") != 2:
        f.append(f"ltp large moves {lm}, count {block.get('large_move_fills')}")
    fc = {x["symbol"]: x.get("factor_check", {}) for x in block["fills"]}
    for s, want in (("JUMP", ["large_move"]), ("DROP", ["large_move"]), ("FLAT", [])):
        if fc[s].get("reasons") != want or fc[s].get("flagged") != bool(want):
            f.append(f"{s} factor_check {fc[s]}, want reasons {want}")
    if block.get("factor_flagged") != 2:
        f.append(f"factor_flagged {block.get('factor_flagged')}")
    if dc.large_moves or dc.events:
        f.append("an LTP fill reached the window-bar block")
    return f


# ---------------------------------------------------------------- both topics end to end

def load_topic(topic):
    spec = importlib.util.spec_from_file_location("dc_" + topic.replace("-", "_"),
                                                  os.path.join(ROOT, topic, "backtest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeClient:
    """Stands in for CetaResearch: runs the topic's own SQL on the fixture, parquet like the API."""

    def __init__(self, src):
        self.src, self.sqls = src, []

    def query(self, sql, format="json", **_kw):
        self.sqls.append(sql)
        check(";" not in sql, "no ';' in any SQL sent to the API")
        if format != "parquet":
            cur = self.src.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.parquet")
            self.src.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
            with open(path, "rb") as fh:
                return fh.read()


FILLERS = [f"A{i:02d}.BK" for i in range(20)]
SPECIAL = ["TWN.BK", "TWN-R.BK", "STD.BK", "STD-R.BK", "BAD.BK", "ZV.BK", "INFL.BK", "BIG.BK", "DLST.BK"]
ISIN = {**{s: f"TH1{i:03d}000000" for i, s in enumerate(FILLERS)},
        "TWN.BK": "TH2000010009", "TWN-R.BK": "TH2000010R18", "STD.BK": "TH2000020001", "STD-R.BK": "TH2000020R11",
        "BAD.BK": "TH3000010001", "ZV.BK": "TH3000020001", "INFL.BK": "TH3000030001", "BIG.BK": "TH3000040001",
        "DLST.BK": "TH3000050001"}
# Leg-wide holiday rows (3 lines trade), each the first row of its window like KIO's 2012-01-02, so the
# oscillation filter has no earlier neighbour and keeps BAD.BK's bad print unless T moves it.
CLOSED = (date(2012, 1, 2), date(2012, 7, 2))
STALE = (date(2013, 1, 1), date(2013, 1, 2), date(2016, 7, 4))   # ZV.BK's zero-volume rows at a third
DELISTED = date(2018, 5, 20)


def price(s, d):
    q = (d.year - 2000) * 4 + (d.month - 1) // 3
    p = 50.0 * 1.02 ** q
    if s == "BIG.BK" and d >= date(2016, 7, 1):
        p *= 2.5
    return round(p, 4)


def synthetic_source():
    """A SET leg: 20 clean names, an ordinary/NVDR pair, an NVDR standing in, a KIO-like closed-day
    exit, an EASTW-like stale entry, an SOS-like inflated cap, a real +150% move, a delisting, and a
    clean name whose FY2010 row check M writes 'unchecked' (A07.BK: a later 1:5 consolidation opens the
    gate, and 20 thin December bars whose raw close is the close give a price factor of 1, k 1 and
    turnover 1e-8 of the cap, so the divisor is 1. Its other FY rows have no bars, and its FY share
    count does not step at the 1:5, so no evidence gives a divisor: unchecked at FMP's cap as well)."""
    src = cap_source()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR, sector VARCHAR,"
                " marketCap DOUBLE, isActivelyTrading BOOLEAN, currency VARCHAR)")
    src.execute("CREATE TABLE financial_ratios(symbol VARCHAR, period VARCHAR, date VARCHAR, dateEpoch BIGINT,"
                " debtToEquityRatio DOUBLE)")
    src.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate DATE)")
    src.execute("INSERT INTO delisted_companies VALUES ('DLST.BK', ?)", [DELISTED])
    names = FILLERS + SPECIAL
    # one FY cap per company: an NVDR shares its ordinary line's, every other line has its own
    cap_of = {s: 5e10 + 1e7 * names.index(s.replace("-R.BK", ".BK")) for s in names}
    for s in names:
        src.execute("INSERT INTO profile VALUES (?, 'SET', 'TH', ?, 'Industrials', 5e10, TRUE, 'THB')", [s, ISIN[s]])
        for y in range(1998, 2025):
            fy = date(y, 12, 31)
            k = y - 1998
            src.execute("INSERT INTO financial_ratios VALUES (?, 'FY', ?, ?, ?)", [s, fy.isoformat(), ep(fy), 20.0 * 0.85 ** k])
            src.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, 0.2, 0.1, ?)", [s, fy.isoformat(), ep(fy), cap_of[s]])
            ni, eps, sh = (1.47e6, 124.0, 593233.0) if s == "INFL.BK" else (2e9, 2.0, 1e9)
            src.execute("INSERT INTO income_statement VALUES (?, 'FY', ?, ?, ?, ?, ?, ?, ?)",
                        [s, fy.isoformat(), ep(fy), 1000.0 * 1.05 ** k, ni, eps, sh, (fy + timedelta(days=60)).isoformat()])
            if s == "INFL.BK":   # the 20 bars before each period end: pf 750, u 1.9e-4, k 50
                for day in weekdays(fy - timedelta(days=29), fy):
                    src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, 2767.5, 2767.5, ?)",
                                [s, day.isoformat(), ep(day), 1.9e-4 * 5e10 / 2767.5])
                    src.execute("INSERT INTO stock_prices_non_split_adjusted_daily VALUES (?, ?, ?, 3.69, 1)",
                                [s, day.isoformat(), ep(day)])
    src.execute("INSERT INTO splits_calendar VALUES ('A05.BK', '2019-03-01', ?, 2, 1)", [ep(date(2019, 3, 1))])
    src.execute("INSERT INTO splits_calendar VALUES ('A07.BK', '2019-06-03', ?, 1, 5)", [ep(date(2019, 6, 3))])
    fy10 = date(2010, 12, 31)
    for day in weekdays(fy10 - timedelta(days=29), fy10):
        src.execute("INSERT INTO stock_eod VALUES ('A07.BK', ?, ?, 50.0, 50.0, 10.0)", [day.isoformat(), ep(day)])
        src.execute("INSERT INTO stock_prices_non_split_adjusted_daily VALUES ('A07.BK', ?, ?, 50.0, 10)",
                    [day.isoformat(), ep(day)])
    rows = []
    for d in du.generate_rebalance_dates(2000, 2025, "quarterly"):
        for day in weekdays(d, d + timedelta(days=11)):
            rows += [("^SET.BK", day, 1000.0 + day.toordinal() % 97, 0.0), ("SPY", day, 300.0 + day.toordinal() % 89, 1e7)]
            for i, s in enumerate(names):
                if s == "STD.BK" or (s == "DLST.BK" and day > DELISTED):
                    continue
                p, v = price(s, day), 1e5
                if day in CLOSED:
                    v = 1e5 if i < 3 else 0.0
                    if s == "BAD.BK":
                        p, v = p * 3.46, 0.0
                if s == "ZV.BK" and day in STALE:
                    p, v = p / 3, 0.0
                rows.append((s, day, p, v))
    for day in weekdays(date(2018, 4, 13), date(2018, 5, 15)):    # DLST.BK trades until its delisting
        rows.append(("DLST.BK", day, price("DLST.BK", day) * 0.8, 1e5))
    rows.append(("DLST.BK", date(2018, 5, 16), price("DLST.BK", date(2018, 4, 2)) * 1.5, 0.0))   # a stale print
    src.executemany("INSERT INTO stock_eod VALUES (?, ?, ?, ?, ?, ?)",
                    [(s, day.isoformat(), ep(day), p, p, v) for s, day, p, v in rows])
    return src


def run_topic(topic, src, run_mode, data_checks, tmp, full_holdings=False):
    """One synthetic run. run_mode is the exit method this test fixes on purpose (drop, ltp, both)."""
    mod = load_topic(topic)
    out = os.path.join(tmp, f"{topic}.{run_mode}.{data_checks}{'.full' if full_holdings else ''}.json")
    client = FakeClient(src)
    freq = "quarterly" if topic == "deleveraging" else "annual"
    kw = {"full_holdings": True} if full_holdings else {}
    with contextlib.redirect_stdout(io.StringIO()):
        mod.run_single(client, ["SET"], "SET", freq, True, 0.02, False, out, offset_days=1,
                       exit_method=run_mode, data_checks=data_checks, **kw)
    with open(out) as fh:
        return json.load(fh), client.sqls


def holdings_full_failures(topic, src, tmp):
    """--full-holdings: each invested period row lists every booked name (holdings_full) with its entry and
    exit bars, prices, exit source and return; nothing else in the file changes, cash rows carry no list,
    LTP fills show their last bar, and the LTP gate and scanners still pass. Off by default."""
    f = []
    try:
        runs = {m: run_topic(topic, src, m, "on", tmp, full_holdings=True)[0] for m in ("drop", "ltp", "both")}
    except TypeError as e:
        return [f"{topic}: run_single has no full_holdings ({e})"]
    plain = run_topic(topic, src, "ltp", "on", tmp)[0]
    ltp = runs["ltp"]
    strip = lambda res: {**res, "period_data": [{k: v for k, v in r.items() if k != "holdings_full"}
                                                for r in res["period_data"]]}
    if strip(ltp) != plain:
        f.append(f"{topic}: full holdings changed something else: {first_diff(strip(ltp), plain)}")
    if any("holdings_full" in r for r in plain["period_data"]):
        f.append(f"{topic}: holdings_full written without the flag")
    g = {"topic": topic, "pairs": [{"published_cagr_path": "portfolio.cagr", "ltp_block_path": "ltp"}]}
    _, probs = check_stage_a(g, runs["both"], runs["drop"], ltp, with_ltp=True)
    f += [f"{topic} full-holdings stage-a: {p}" for p in probs]
    hits, counts = [], {}
    sri.walk(ltp, "", topic, hits, counts, False)
    sri.funnel_scan(ltp, "", topic, hits, {})
    if hits:
        f.append(f"{topic}: scanners {hits}")
    fills = {(x["entry_date"], x["symbol"]): x for x in ltp["ltp"]["fills"]}
    n_ltp = n_named = 0
    for r in ltp["period_data"]:
        hf = r.get("holdings_full")
        if not r["stocks_held"]:
            if hf is not None:
                f.append(f"{topic} {r['rebalance_date']}: a cash row carries holdings_full")
            continue
        names = [h["symbol"] for h in hf or []]
        shown = r["holdings"].split(",")
        if len(names) != r["stocks_held"] or names[:len(shown)] != shown:
            f.append(f"{topic} {r['rebalance_date']}: holdings_full {names} against {r['stocks_held']} held, {shown}")
            break
        n_named += len(names) - len(shown)
        for h in hf:
            if set(h) != {"symbol", "entry_bar", "entry_price", "exit_bar", "exit_price", "exit", "return"} or \
                    abs(h["return"] - round(h["exit_price"] / h["entry_price"] - 1, 6)) > 1e-9 or \
                    not h["entry_bar"] < h["exit_bar"] or h["exit"] not in ("window", "last_traded"):
                f.append(f"{topic} {r['rebalance_date']}: bad item {h}")
            if h["exit"] == "last_traded":
                n_ltp += 1
                fill = fills.get((r["rebalance_date"], h["symbol"]))
                if not fill or (fill["last_bar"], fill["ltp"]) != (h["exit_bar"], h["exit_price"]):
                    f.append(f"{topic} {r['rebalance_date']}: {h} does not match its fill {fill}")
    if topic == "deleveraging" and not n_ltp:
        f.append(f"{topic}: no LTP-filled name listed (DLST.BK)")
    if topic == "cyclical-timing" and not n_named:
        f.append(f"{topic}: the synthetic books never pass the 10-name holdings string: the case tests nothing")
    if any(h["exit"] != "window" for r in runs["drop"]["period_data"] for h in r.get("holdings_full", [])):
        f.append(f"{topic}: a drop run lists an LTP exit")
    return f


def topic_failures(topic, src, tmp):
    f = []
    runs = {m: run_topic(topic, src, m, "on", tmp) for m in ("drop", "ltp", "both")}
    unc, _ = run_topic(topic, src, "ltp", "uncapped", tmp)
    drop, both, ltp = runs["drop"][0], runs["both"][0], runs["ltp"][0]
    g = {"topic": topic, "pairs": [{"published_cagr_path": "portfolio.cagr", "ltp_block_path": "ltp"}]}
    _, probs = check_stage_a(g, both, drop, ltp, with_ltp=True)
    f += [f"{topic} stage-a: {p}" for p in probs]
    dcb = drop.get("data_checks")
    if dcb is None or list(drop).index("data_checks") != list(drop).index("isin_dedup") + 1:
        f.append(f"{topic}: data_checks sits right after isin_dedup")
        return f
    if has_ltp_keys(dcb) or "exit_method" in json.dumps(dcb):
        f.append(f"{topic}: a data_checks key names LTP or exit_method")
    if dcb != both["data_checks"] or dcb != ltp["data_checks"]:
        f.append(f"{topic}: data_checks differs across exit methods")
    hits, counts = [], {}
    sri.walk(drop, "", topic, hits, counts, False)
    if hits or counts.get("nodes") != 1:
        f.append(f"{topic}: accounting nodes {counts} {hits}")
    hits = []
    sri.funnel_scan(drop, "", topic, hits, {})
    if hits:
        f.append(f"{topic}: funnel {hits}")
    ev, c = dcb["events"], dcb["counts"]
    if ev != sorted(ev, key=du._event_key):
        f.append(f"{topic}: events not sorted")
    shuffled = DataChecks("on")
    shuffled.events, shuffled.large_moves = random.Random(7).sample(ev, len(ev)), list(reversed(dcb["large_moves"]))
    blk = shuffled.block()
    if json.dumps(blk["events"]) != json.dumps(ev) or json.dumps(blk["large_moves"]) != json.dumps(dcb["large_moves"]):
        f.append(f"{topic}: shuffled events give different JSON")
    if dcb["mode"] != "on" or dcb["max_single_return"] is not None or dcb["version"] != "dc-1":
        f.append(f"{topic}: header {dcb['mode']} {dcb['max_single_return']}")
    if not all(isinstance(v, int) for k, v in c.items() if k != "rows_moved") or \
            not all(isinstance(v, int) for v in c["rows_moved"].values()):
        f.append(f"{topic}: counts are ints")
    kinds = {(e["check"], e["symbol"]) for e in ev}
    want_dates = ["2012-01-02", "2012-07-02"] if topic == "deleveraging" else ["2012-07-02"]
    if [x["date"] for x in dcb["closed_dates"]] != want_dates:
        f.append(f"{topic}: closed dates {dcb['closed_dates']}")
    want = {("exit_bar", "BAD.BK"), ("twin", "TWN.BK"), ("twin", "STD-R.BK"), ("cap", "INFL.BK"),
            ("factor_flag", "BIG.BK"), ("exit_bar", "ZV.BK"), ("entry_bar", "ZV.BK")}
    if topic == "deleveraging":
        want |= {("entry_bar", "BAD.BK"), ("factor_flag", "A05.BK")}
    else:
        want |= {("signal_cap", "INFL.BK")}
    if want - kinds:
        f.append(f"{topic}: missing events {sorted(want - kinds)}")
    if [m["symbol"] for m in dcb["large_moves"]] != ["BIG.BK"]:
        f.append(f"{topic}: large moves {[(m['rebalance_date'], m['symbol']) for m in dcb['large_moves']]}")
    reasons = {e["reason"] for e in ev if e["check"] == "twin"}
    if reasons != {"ordinary_first", "stand_in"} or c["twins_linked"] != 2:
        f.append(f"{topic}: twins {reasons} linked {c['twins_linked']}")
    # A07.BK FY2010 is 'unchecked' through cap_check_sql and the floors: listed, no cap event, and held
    # in the books that read that row, as in the uncapped arm (FMP's cap clears the floor). Its FY rows
    # before the 1:5 with no bars are unchecked too: the split record alone gives no divisor.
    un = [(u["symbol"], u["fy"], u["cap"]) for u in dcb.get("cap_unchecked", [])]
    if un != [("A07.BK", f"{y}-12-31", 5.007e10) for y in range(1998, 2019)]:
        f.append(f"{topic}: cap_unchecked {un}, want A07.BK FY1998 to FY2018 at 5.007e10")
    if any(e["symbol"] == "A07.BK" and e["check"] in ("cap", "signal_cap") for e in ev):
        f.append(f"{topic}: A07.BK has a cap event, but an unchecked row keeps FMP's cap")
    held = lambda res: {r["rebalance_date"] for r in res["period_data"] if "A07.BK" in r["holdings"].split(",")}
    fy10 = {"2011-04-01", "2011-07-01", "2011-10-01", "2012-01-01"} if topic == "deleveraging" else {"2011-07-01"}
    if not fy10 <= held(unc) or held(ltp) != held(unc):
        f.append(f"{topic}: A07.BK not held at {sorted(fy10 - held(ltp))} (books on its FY2010 row), or held "
                 f"apart from the uncapped arm: {sorted(held(ltp) ^ held(unc))}")
    keys = ["entry_bars_moved", "exit_bars_moved", "entries_lost", "exits_lost", "twins_collapsed", "cap_excluded",
            "factor_flags"]
    keys += ["signal_cap_excluded"] if topic == "cyclical-timing" else []
    for r in drop["period_data"]:
        if [k for k in keys if k not in r] or any(r[k] is not None and not isinstance(r[k], int) for k in keys):
            f.append(f"{topic}: period row {r['rebalance_date']} keys")
            break
        held = [h for h in r["holdings"].split(",") if h and not h.startswith("CASH")]
        if {"INFL.BK", "TWN-R.BK"} & set(held):
            f.append(f"{topic}: {r['rebalance_date']} holds a name the checks remove: {held}")
            break
    n_cap = sum(r["cap_excluded"] or 0 for r in drop["period_data"])
    if n_cap != c["cap_excluded"]:
        f.append(f"{topic}: per-period cap_excluded {n_cap} != counts {c['cap_excluded']}")
    fl = ltp["ltp"]
    if not (fl["traded_only"] and all("ltp_close" in x and "factor_check" in x for x in fl["fills"])
            and "factor_flagged" in fl and fl.get("large_move_fills") == len(fl.get("large_moves", [None]))):
        f.append(f"{topic}: ltp block lacks traded_only provenance")
    if dc_results_problem(ltp) or not dc_results_problem(unc):
        f.append(f"{topic}: the results check must pass mode on and refuse the uncapped arm")
    if topic == "deleveraging" and not (fl["fills"] and fl["zero_volume_rows_moved"] >= 1):
        f.append(f"{topic}: DLST.BK's LTP fill skips its stale zero-volume print ({fl['fills']})")
    ub = unc["data_checks"]
    if ub["mode"] != "uncapped" or ub["events"] or ub["large_moves"] or ub["closed_dates"] or ub["cap_unchecked"] or \
            any(v for k, v in ub["counts"].items() if k != "rows_moved") or any(ub["counts"]["rows_moved"].values()):
        f.append(f"{topic}: the uncapped arm records nothing, got {ub['counts']}")
    if "traded_only" in unc["ltp"]:
        f.append(f"{topic}: the uncapped arm's LTP is the plain one")
    # what the checks did, visible against the uncapped arm: BAD.BK's closed-day exit
    bad = "2011-10-01" if topic == "deleveraging" else "2011-07-01"
    pr = lambda res: next(r["portfolio_return"] for r in res["period_data"] if r["rebalance_date"] == bad)
    if not pr(unc) > pr(ltp) + 0.05:
        f.append(f"{topic}: {bad} uncapped {pr(unc)} vs on {pr(ltp)}: BAD.BK's +246% should only be in the uncapped arm")
    return f


# ---------------------------------------------------------------- gate 15 must fail

def gate15_failures():
    f = []
    for t, n in DATA_CHECKS_TOPICS.items():
        src = open(os.path.join(ROOT, t, "backtest.py")).read()
        if dc_wiring(ast.parse(src), n):
            f.append(f"gate 15 flags the real {t}: {dc_wiring(ast.parse(src), n)}")
        mutants = {"cap restored": src.replace("max_single_return=None", "max_single_return=2.0", 1),
                   "a floor on the raw cap": src.replace('sql("km.cap_used > ?")', 'sql("km.marketCap > ?")', 1),
                   "M not loaded": src.replace("load_cap_check(client", "print(client", 1),
                   "dc dropped": src.replace("exit_method=exit_method, dc=dc)", "exit_method=exit_method)", 1)}
        for label, m in mutants.items():
            check(m != src, f"gate 15 mutant {label} found nothing to change in {t}")
            if not dc_wiring(ast.parse(m), n):
                f.append(f"gate 15 misses {label} in {t}")
    # a capped topic that drops its cap without opting in: every form of a None cap is a use
    ld = open(os.path.join(ROOT, "low-debt", "backtest.py")).read()
    gn = open(os.path.join(ROOT, "graham-net-net", "backtest.py")).read()
    call = "filter_returns(raw_returns, min_entry_price=0.50,\n                                  max_single_return=3.0,"
    for label, s in (("low-debt as is", ld), ("graham-net-net as is", gn)):
        if dc_uses(ast.parse(s)):
            f.append(f"gate 15 flags {label}: {dc_uses(ast.parse(s))}")
    for label, s, m in (("MAX_SINGLE_RETURN = None", ld, ld.replace("MAX_SINGLE_RETURN = 2.0", "MAX_SINGLE_RETURN = None", 1)),
                        ("MAX_SINGLE_RETURN = float('inf')", ld,
                         ld.replace("MAX_SINGLE_RETURN = 2.0", "MAX_SINGLE_RETURN = float('inf')", 1)),
                        ("positional None", gn, gn.replace(call, "filter_returns(raw_returns, 0.50, None,", 1))):
        check(m != s, f"gate 15 use mutant {label} found nothing to change")
        if not dc_uses(ast.parse(m)):
            f.append(f"gate 15 misses {label} in a capped topic")
    # the floor-guard harness's no-cap exemption is the data-checks list plus the two topics that never had a cap
    if NO_RETURN_CAP - {"qarp", "low-pe"} != set(DATA_CHECKS_TOPICS):
        f.append(f"test_floor_guard NO_RETURN_CAP {sorted(NO_RETURN_CAP)} is not qarp, low-pe and DATA_CHECKS_TOPICS")
    # only a mode-on block may sit in a committed results file
    for res, bad in (({}, False), ({"data_checks": {"mode": "on"}}, False),
                     ({"data_checks": {"mode": "uncapped"}}, True), ({"data_checks": None}, True)):
        if bool(dc_results_problem(res)) != bad:
            f.append(f"dc_results_problem({res}) {dc_results_problem(res)}")
    return f


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base", default=BASE, help=f"commit the shared code is compared against (default {BASE})")
    args = ap.parse_args()
    ran = []

    def expect(name, failures):
        check(failures == [], f"{name}: {failures}")
        ran.append(name)

    def expect_fail(name, failures):
        check(failures != [], f"must-fail mutant passed: {name}")
        ran.append(f"mutant {name}")

    # T
    expect("T EASTW-R entry", eastw_failures())
    expect_fail("T stubbed (EASTW-R books +250.9%)", eastw_failures(no_clean))
    expect("T zero-volume feed", feed_failures())
    expect("T JNB closed day", kio_failures())
    stub = kio_failures(no_clean)
    expect_fail("T stubbed (KIO)", stub)
    check(any("299.5%" in s for s in stub), f"the stubbed KIO case fails for the +299.5% booking: {stub}")
    expect("T exit lost on a closed day", exit_lost_failures())
    expect("T closed-day shapes", closed_shape_failures())
    expect("T benchmarks exempt", bench_failures())
    expect("T RAM shape", ram_failures())
    expect("T LTP traded_only (HALT keeps its fill)", ltp_traded_failures())
    expect("L on LTP fills", ltp_large_move_failures())
    # N
    expect("N twins", twin_failures())
    # M
    expect("M rule", cap_rule_failures())
    expect("M server SQL", cap_sql_failures())
    expect("M at every floor", cap_screen_failures())
    expect_fail("M with cap_check empty (SOS, ZEST pass)", cap_screen_failures(with_check=False))
    # cases from the same-day reviews
    expect("M1 k first: 7211.T FY2003", m1_failures())
    expect("M k guard: 4541.T delisting record", k_guard_failures())
    expect("M2 sibling turnover, lowered not excluded", m2_failures())
    expect("M3 consolidation gate, unknown turnover", m3_failures())
    expect("M siblings share two FY caps: 1789.HK / 1935.HK", m_class_fy_failures())
    expect("M lowers, not excludes, unless strict: ERIC-A.ST FY1999", m_strict_failures())
    expect("M a divisor of 1 is unchecked, not stands: HAO FY2025", m_unchecked_failures())
    expect("M divisor edges: 1.25 lowers, unknown turnover unchecked, k alone lowers", m_divisor_failures())
    expect("M divisor needs evidence: WEST3, GOL, 0568.HK, INPEX, DCC back; CETX, HAO stay", m6_evidence_failures())
    expect("M split records once, FY share steps: AMCR, 071970.KS, restated and stepping counts", m6_sql_failures())
    expect("T1 flat lines with no volume", t1_failures())
    expect("T1 two-price NVDR feeds: BH-R.BK, RATCH-R.BK, MCS-R.BK", t1_feed_failures())
    expect("T1 halted exit reads the last traded bar", t1_exit_failures())
    expect("T2 sibling gap: ELUX-A.ST 2006", t2_failures())
    expect("T2 real ELUX rows: 2007 keeps its rows", t2_real_failures())
    expect("T2 must not move: LG Chem, Samsung Heavy pref, BP-B.L, 1935.HK", t2_pref_failures())
    expect("T2 three classes, liquid line, leading run, trigger", t2_three_failures())
    # F and L
    expect("F flags, L list", f_failures())
    expect_fail("F detection stubbed", f_failures(NoFlags, f_mutant_cfg()))
    # filter_returns
    clean, _ = filter_returns([("A", 10.0, 35.0, 1e9), ("B", 10.0, 56.2, 1e9)])
    check([s for s, _, _ in clean] == [], "default cap drops +250% and +462%")
    clean, _ = filter_returns([("A", 10.0, 35.0, 1e9), ("B", 10.0, 56.2, 1e9), ("C", 0.5, 9.0, 1e9),
                               ("D", 10.0, None, 1e9)], max_single_return=None)
    check([s for s, _, _ in clean] == ["A", "B"], "None keeps +250% and +462%, entry and exit rules still apply")
    ran.append("filter_returns None")
    # shared code against BASE
    stale = (f"data_utils differs from the base {args.base} outside the data-checks surface. If that change "
             "is intended and has had its own review, pass --base <the commit that made it> or move BASE there")
    with tempfile.TemporaryDirectory() as tmp:
        base, base_src = load_base(tmp, args.base)
        for name, failures in (("ast identity", ast_failures(base_src)),
                               ("isin_dedup_sql(twin_key=False) identity", dedup_identity_failures(base)),
                               ("filter_returns default identity (10,000 random books)", filter_identity_failures(base)),
                               ("LtpExits(traded_only=False) identity", ltp_identity_failures(base))):
            expect(name, failures + ([stale] if failures else []))
    # both topics end to end, and gate 15
    src = synthetic_source()
    with tempfile.TemporaryDirectory() as tmp:
        for t in DATA_CHECKS_TOPICS:
            expect(f"{t} end to end", topic_failures(t, src, tmp))
            expect(f"{t} full holdings", holdings_full_failures(t, src, tmp))
    expect("gate 15 on the real topics and mutants", gate15_failures())
    print("\n".join(f"ok {r}" for r in ran))
    print(f"test_data_checks: PASS ({len(ran)} checks)")


if __name__ == "__main__":
    main()
