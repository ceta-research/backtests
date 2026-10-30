#!/usr/bin/env python3
"""Offline tests for data checks dc-1 (no network, no backtest leg).

T (traded bars), N (NVDR twins), M (market-cap check), F (factor flags, detection only) and L
(large-move list) on crafted rows shaped like the measured cases (EASTW-R.BK, TRUE.BK 2006, the
JNB 2012-01-02 holiday, NSE/SAU/SET day counts, ^FTSE, RAM.BK, the SET twins, SOS, ZEST, HAO,
GRPN, CIM, 3003.SR, TSG.JO, 2603.TW, SALEE.BK). Then both opted-in topics run end to end on a
synthetic SET leg through a fake client, in drop, ltp, both and uncapped modes, and the results
pass the LTP gate's stage-a comparison and the results scanners.

Shared code: against the base commit (BASE, the last one before data checks), isin_dedup_sql
(twin_key=False) returns the same SQL, LtpExits(traded_only=False) sends the same SQL and builds
the same temp table, filter_returns' default gives the same output, and every other function,
class and constant of data_utils is unchanged (ast). Each "must fail" check is also run on a
mutant and has to fail: clean_price_cache stubbed (KIO books +299.5%), cap_check empty (SOS and
ZEST pass the floor), F's detection stubbed, and gate 15 on mutated topic sources.

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
from verify_floor_guard import dc_wiring, DATA_CHECKS_TOPICS   # noqa: E402

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

# symbol, pf_raw, pf_splits, n_traded, u, eps, net income, shares -> want rule, k_div
CAP_CASES = [
    ("SOS", 750.0, None, 20, 1.9e-4, 124.0, 1.47e6, 593233.0, "cap_over_k", 50.0),
    ("ZEST", 150.0, None, 20, 2.0e-5, -1.12, -11.407e6, 10.202e6, "ineligible", None),
    ("HAO", 25.0, None, 20, 1.6e-5, 10.0, 1e8, 1e8, "ineligible", None),
    ("GRPN", 20.0, None, 20, 0.015, 1.0, 1e8, 1e8, "stands", None),
    ("CIM", 3.0, None, 20, 0.01, 0.275, 1e8, 1e8, "stands", None),
    ("THIN1", 30.0, None, 9, 1e-6, 1.0, 1e8, 1e8, "stands", None),         # under 10 traded bars: u unknown
    ("THIN8", 30.0, None, 9, 1e-6, 8.0, 1e8, 1e8, "cap_over_k", 8.0),
    ("SPLIT", None, 40.0, 20, 1e-5, 1.0, 1e8, 1e8, "ineligible", None),    # no raw price: splits fallback
    ("NOPF", None, None, 20, 1e-6, 50.0, 1e8, 1e8, "stands", None),        # no pf: the cap stands
    ("LOWPF", 2.99, None, 20, 1e-6, 50.0, 1e8, 1e8, "stands", None),
    ("SMALLEPS", 30.0, None, 20, 0.01, 0.04, 1e8, 1e10, "stands", None),   # |eps| < 0.05: no k
    ("SMALLNI", 30.0, None, 20, 0.01, 5.0, 9e5, 1e8, "stands", None),      # |net income| < 1M: no k
    ("SIGNS", 30.0, None, 20, 0.01, -5.0, 1e8, 1e8, "stands", None),       # signs disagree: no k
]


def cap_rule_failures():
    con = duckdb.connect()
    con.execute(CAP_INPUTS_DDL)
    for i, (s, pfr, pfs, nt, u, eps, ni, sh, _, _) in enumerate(CAP_CASES):
        cap = 2e9
        con.execute("INSERT INTO cap_inputs VALUES (?, ?, ?, ?, ?, ?, 20, ?, ?, ?, ?)",
                    [s, 1000 + i, cap, ni, eps, sh, nt, u * cap, pfr, pfs])
    stats = apply_cap_rule(con)
    got = {r[0]: r[1:] for r in con.execute("SELECT symbol, rule, k_div, pf_source FROM cap_check").fetchall()}
    f = []
    for s, *_, rule, kd in CAP_CASES:
        r = got.get(s)
        if r is None or r[0] != rule or (kd is None) != (r[1] is None) or (kd and abs(r[1] - kd) > 0.1):
            f.append(f"{s}: {r}, want {rule} {kd}")
    if got["SPLIT"][2] != "splits" or got["SOS"][2] != "raw":
        f.append("pf_source")
    if stats != {"cap_rows_checked": len(CAP_CASES), "cap_rows_flagged": 5}:
        f.append(f"stats {stats}")
    return f


def cap_source():
    """FMP tables shaped like the server's, holding SOS's FY2019 inputs (out/cap_px.json)."""
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
    if with_check:
        for s, pfr, nt, u, eps, ni, sh in (("SOS", 750.0, 20, 1.9e-4, 124.0, 1.47e6, 593233.0),
                                           ("ZEST", 150.0, 20, 2.0e-5, -1.12, -11.407e6, 10.202e6)):
            con.execute("INSERT INTO cap_inputs VALUES (?, ?, 5e9, ?, ?, ?, 20, ?, ?, ?, NULL)",
                        [s, ep(date(2019, 12, 31)), ni, eps, sh, nt, u * 5e9, pfr])
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
    if caps != [("SOS", "cap_over_k"), ("ZEST", "ineligible")]:
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


def ltp_traded_failures():
    """traded_only=True: the zero-volume rows of a line that traded leave the fill's span; fills carry
    ltp_close and ltp_volume."""
    src = duckdb.connect()
    src.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate DATE)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " volume DOUBLE)")
    for day in weekdays(date(2018, 3, 1), date(2018, 5, 15)):
        src.execute("INSERT INTO stock_eod VALUES ('Z', ?, ?, 10, 12, 100)", [day.isoformat(), ep(day)])
    src.execute("INSERT INTO stock_eod VALUES ('Z', '2018-05-16', ?, 15, 18, 0)", [ep(date(2018, 5, 16))])
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE, volume DOUBLE)")
    con.execute("INSERT INTO prices_cache VALUES ('Z', ?, 10, 12, 100)", [ep(date(2018, 4, 2))])
    f = []
    for traded, want in ((False, 15.0), (True, 10.0)):
        ltp = du.LtpExits(LtpClient(src), con, traded_only=traded)
        got = ltp.fill(["Z"], {"Z": 10.0}, {}, date(2018, 4, 1), date(2018, 7, 1), offset_days=1)
        if got.get("Z") != want:
            f.append(f"traded_only={traded}: fill {got}, want {want}")
        p = ltp.provenance()
        if traded and not (p["traded_only"] and p["zero_volume_rows_moved"] == 1
                           and ltp.fills[0]["ltp_close"] == 12.0 and ltp.fills[0]["ltp_volume"] == 100.0):
            f.append(f"traded_only provenance {p}")
        if not traded and ("traded_only" in p or "ltp_close" in ltp.fills[0]):
            f.append("traded_only=False adds nothing")
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
    exit, an EASTW-like stale entry, an SOS-like inflated cap, a real +150% move, a delisting."""
    src = cap_source()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR, sector VARCHAR,"
                " marketCap DOUBLE, isActivelyTrading BOOLEAN, currency VARCHAR)")
    src.execute("CREATE TABLE financial_ratios(symbol VARCHAR, period VARCHAR, date VARCHAR, dateEpoch BIGINT,"
                " debtToEquityRatio DOUBLE)")
    src.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate DATE)")
    src.execute("INSERT INTO delisted_companies VALUES ('DLST.BK', ?)", [DELISTED])
    names = FILLERS + SPECIAL
    for s in names:
        src.execute("INSERT INTO profile VALUES (?, 'SET', 'TH', ?, 'Industrials', 5e10, TRUE, 'THB')", [s, ISIN[s]])
        for y in range(1998, 2025):
            fy = date(y, 12, 31)
            k = y - 1998
            src.execute("INSERT INTO financial_ratios VALUES (?, 'FY', ?, ?, ?)", [s, fy.isoformat(), ep(fy), 20.0 * 0.85 ** k])
            src.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, 0.2, 0.1, 5e10)", [s, fy.isoformat(), ep(fy)])
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


def run_topic(topic, src, exit_method, data_checks, tmp):
    mod = load_topic(topic)
    out = os.path.join(tmp, f"{topic}.{exit_method}.{data_checks}.json")
    client = FakeClient(src)
    freq = "quarterly" if topic == "deleveraging" else "annual"
    with contextlib.redirect_stdout(io.StringIO()):
        mod.run_single(client, ["SET"], "SET", freq, True, 0.02, False, out, offset_days=1,
                       exit_method=exit_method, data_checks=data_checks)
    with open(out) as fh:
        return json.load(fh), client.sqls


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
    keys = ["entry_bars_moved", "exit_bars_moved", "entries_lost", "twins_collapsed", "cap_excluded", "factor_flags"]
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
            and "factor_flagged" in fl):
        f.append(f"{topic}: ltp block lacks traded_only provenance")
    if topic == "deleveraging" and not (fl["fills"] and fl["zero_volume_rows_moved"] >= 1):
        f.append(f"{topic}: DLST.BK's LTP fill skips its stale zero-volume print ({fl['fills']})")
    ub = unc["data_checks"]
    if ub["mode"] != "uncapped" or ub["events"] or ub["large_moves"] or ub["closed_dates"] or \
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
    expect("T closed-day shapes", closed_shape_failures())
    expect("T benchmarks exempt", bench_failures())
    expect("T RAM shape", ram_failures())
    expect("T LTP traded_only", ltp_traded_failures())
    # N
    expect("N twins", twin_failures())
    # M
    expect("M rule", cap_rule_failures())
    expect("M server SQL", cap_sql_failures())
    expect("M at every floor", cap_screen_failures())
    expect_fail("M with cap_check empty (SOS, ZEST pass)", cap_screen_failures(with_check=False))
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
    with tempfile.TemporaryDirectory() as tmp:
        base, base_src = load_base(tmp, args.base)
        expect("ast identity", ast_failures(base_src))
        expect("isin_dedup_sql(twin_key=False) identity", dedup_identity_failures(base))
        expect("filter_returns default identity (10,000 random books)", filter_identity_failures(base))
        expect("LtpExits(traded_only=False) identity", ltp_identity_failures(base))
    # both topics end to end, and gate 15
    src = synthetic_source()
    with tempfile.TemporaryDirectory() as tmp:
        for t in DATA_CHECKS_TOPICS:
            expect(f"{t} end to end", topic_failures(t, src, tmp))
    expect("gate 15 on the real topics and mutants", gate15_failures())
    print("\n".join(f"ok {r}" for r in ran))
    print(f"test_data_checks: PASS ({len(ran)} checks)")


if __name__ == "__main__":
    main()
