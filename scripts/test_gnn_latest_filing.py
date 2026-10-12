#!/usr/bin/env python3
"""Offline check that graham-net-net screens on each company's LATEST FY filing (no network).

The screen must pick the latest filing on or before the 45-day cutoff first and only then require
NCAV > 0 and the size floor; a filing older than MAX_FILING_AGE_DAYS drops the company. Filtering
first let a company whose NCAV turned negative qualify on its last positive filing for years.
Every mutant of the topic's source must trip a check.

Usage:
    python scripts/test_gnn_latest_filing.py
"""
import os
import sys
import types
from datetime import date, datetime, timezone

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = os.path.join(ROOT, "graham-net-net", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 4, 1)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("gnn_latest")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


# symbol, [(filing date, grahamNetNet, marketCap)], price: every NCAV is 10, price 5 (ratio 0.5)
FILINGS = {
    "OK": [(date(2018, 12, 31), 10.0, 1e9), (date(2019, 12, 31), 10.0, 1e9)],   # latest positive: in
    "TURNED": [(date(2018, 12, 31), 10.0, 1e9), (date(2019, 12, 31), -3.0, 1e9)],  # latest negative: out
    "SHRANK": [(date(2018, 12, 31), 10.0, 1e9), (date(2019, 12, 31), 10.0, 1e5)],  # latest below floor: out
    "OLD": [(date(2018, 6, 30), 10.0, 1e9)],                                       # 641 days old: out
    "LAGGED": [(date(2019, 12, 31), 10.0, 1e9), (date(2020, 3, 1), -3.0, 1e9)],   # newest is inside the lag: in
}
EXPECT = {"OK", "LAGGED"}


def db():
    con = duckdb.connect()
    con.execute("CREATE TABLE metrics_cache(symbol VARCHAR, grahamNetNet DOUBLE, marketCap DOUBLE,"
                " currentRatio DOUBLE, reportedCurrency VARCHAR, filing_epoch BIGINT)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE ccy_map(symbol VARCHAR, currency VARCHAR, is_adr BOOLEAN)")
    con.execute("CREATE TABLE fx_cache(ccy VARCHAR, fx_epoch BIGINT, per_usd DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    for s, rows in FILINGS.items():
        for d, g, m in rows:
            con.execute("INSERT INTO metrics_cache VALUES (?, ?, ?, 2.0, 'USD', ?)", [s, g, m, ep(d)])
        con.execute("INSERT INTO prices_cache VALUES (?, ?, 5.0, 1e6)", [s, ep(date(2020, 4, 2))])
        con.execute("INSERT INTO ccy_map VALUES (?, 'USD', FALSE)", [s])
    return con


def failures(src):
    try:
        mod = load(src)
        got = {r[0] for r in mod.screen_stocks(db(), D, 1e6, offset_days=1)}
    except Exception as e:  # a mutant that breaks the module fails loudly
        return [f"error: {type(e).__name__}: {e}"]
    return [] if got == EXPECT else [f"book {sorted(got)} != {sorted(EXPECT)}"]


MUTANTS = {
    # the old bug: NCAV / size tests applied before picking the latest filing
    "NCAV test before latest": ("WHERE filing_epoch <= ? AND filing_epoch >= ?",
                                "WHERE filing_epoch <= ? AND filing_epoch >= ? AND grahamNetNet > 0"),
    "size test before latest": ("WHERE filing_epoch <= ? AND filing_epoch >= ?",
                                "WHERE filing_epoch <= ? AND filing_epoch >= ? AND marketCap > 1000000"),
    "no staleness bound": ("WHERE filing_epoch <= ? AND filing_epoch >= ?", "WHERE filing_epoch <= ? AND ? IS NOT NULL"),
    "no size floor": ("WHERE rn = 1 AND grahamNetNet > 0 AND marketCap > ?", "WHERE rn = 1 AND grahamNetNet > 0 AND ? IS NOT NULL"),
    "no lag": ("cutoff_epoch = utc_epoch(target_date - timedelta(days=DATA_LAG_DAYS))",
               "cutoff_epoch = utc_epoch(target_date)"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_gnn_latest_filing: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_gnn_latest_filing: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new)):
            survived.append(name)
    if survived:
        print("test_gnn_latest_filing: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_gnn_latest_filing: PASS ({len(FILINGS)} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
