#!/usr/bin/env python3
"""Offline check that price-to-sales screens on each company's LATEST FY filing (no network).

The screen must pick the latest key_metrics and financial_ratios filing on or before the 45-day
cutoff first and only then apply the six tests (P/S band, gross margin, operating margin, ROE,
size); a filing older than MAX_FILING_AGE_DAYS drops the company. Testing first (pass-only
fetches, then rn = 1) let a company whose P/S or ROE had turned qualify on its last passing
filing for years: ORLY held in 2024 on its FY2004 P/S. Every mutant of the topic's source must
trip a check.

Usage:
    python scripts/test_ps_latest_filing.py [repo root]
"""
import os
import re
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "price-to-sales", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 4, 1)
FLOOR = 5e8
FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)
OLD_DAY = date(2018, 6, 30)             # 641 days before D
IN_LAG = date(2020, 3, 1)               # inside the 45-day lag
AT_CUTOFF = D - timedelta(days=45)
AT_MAX_AGE = D - timedelta(days=540)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("ps_latest")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


GOOD_M = (0.20, 1e9)            # returnOnEquity, marketCap
GOOD_R = (0.50, 0.30, 0.10)     # priceToSalesRatio, grossProfitMargin, operatingProfitMargin
# symbol: (key_metrics filings [(date, roe, mcap)], financial_ratios filings [(date, ps, gm, om)])
FILINGS = {
    "OK": ([(FY18, *GOOD_M), (FY19, *GOOD_M)], [(FY18, *GOOD_R), (FY19, *GOOD_R)]),
    # latest P/S above 1 (the ORLY shape): out
    "PS_TURNED": ([(FY18, *GOOD_M), (FY19, *GOOD_M)], [(FY18, 0.096, 0.30, 0.10), (FY19, 3.77, 0.30, 0.10)]),
    # latest ROE negative: out
    "ROE_TURNED": ([(FY18, *GOOD_M), (FY19, -2.05, 1e9)], [(FY18, *GOOD_R), (FY19, *GOOD_R)]),
    # latest size below the floor: out
    "SHRANK": ([(FY18, *GOOD_M), (FY19, 0.20, 1e5)], [(FY18, *GOOD_R), (FY19, *GOOD_R)]),
    # latest gross margin below 20%: out
    "GM_TURNED": ([(FY18, *GOOD_M), (FY19, *GOOD_M)], [(FY18, *GOOD_R), (FY19, 0.50, 0.10, 0.10)]),
    # only filing is 641 days old, in one table or both: out
    "OLD": ([(OLD_DAY, *GOOD_M)], [(OLD_DAY, *GOOD_R)]),
    "OLD_M": ([(OLD_DAY, *GOOD_M)], [(FY19, *GOOD_R)]),
    "OLD_R": ([(FY19, *GOOD_M)], [(OLD_DAY, *GOOD_R)]),
    # newest filing is inside the lag and fails; the one before passes: in
    "LAGGED": ([(FY19, *GOOD_M), (IN_LAG, -1.0, 1e9)], [(FY19, 0.40, 0.30, 0.10), (IN_LAG, 3.0, 0.30, 0.10)]),
    # filings exactly on the cutoff and exactly MAX_FILING_AGE_DAYS old: both bounds are inclusive
    "EDGE_LAG": ([(AT_CUTOFF, *GOOD_M)], [(AT_CUTOFF, 0.60, 0.30, 0.10)]),
    "EDGE_AGE": ([(AT_MAX_AGE, *GOOD_M)], [(AT_MAX_AGE, 0.70, 0.30, 0.10)]),
}
EXPECT = [("LAGGED", 1e9), ("OK", 1e9), ("EDGE_LAG", 1e9), ("EDGE_AGE", 1e9)]   # P/S ascending


def db():
    con = duckdb.connect()
    con.execute("CREATE TABLE metrics_cache(symbol VARCHAR, returnOnEquity DOUBLE, marketCap DOUBLE,"
                " filing_epoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE ratios_cache(symbol VARCHAR, priceToSalesRatio DOUBLE, grossProfitMargin DOUBLE,"
                " operatingProfitMargin DOUBLE, filing_epoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    for s, (metrics, ratios) in FILINGS.items():
        for d, roe, mcap in metrics:
            con.execute("INSERT INTO metrics_cache VALUES (?, ?, ?, ?, 'FY')", [s, roe, mcap, ep(d)])
        for d, ps, gm, om in ratios:
            con.execute("INSERT INTO ratios_cache VALUES (?, ?, ?, ?, ?, 'FY')", [s, ps, gm, om, ep(d)])
        con.execute("INSERT INTO prices_cache VALUES (?, ?, 5.0, 1e6)", [s, ep(date(2020, 4, 2))])
    return con


class _Client:
    """Stands in for CetaResearch in fetch_data_via_api: one profile row, nothing else."""

    def query(self, sql, **kw):
        return [{"symbol": "OK", "exchange": "NYSE"}]


def fetch_sql(mod):
    """{table: SQL} the topic's fetch sends, with query_parquet and the other loaders stubbed."""
    sent = {}

    def fake_query_parquet(client, sql, con, table_name, **kw):
        sent[table_name] = sql
        con.execute(f"CREATE TABLE {table_name}(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
        return 0

    mod.query_parquet = fake_query_parquet
    mod.load_isin_map = lambda *a, **k: 0
    mod.remove_price_oscillations = lambda *a, **k: None
    mod.print = lambda *a, **k: None
    mod.fetch_data_via_api(_Client(), ["NYSE"], [D]).close()
    return sent


def failures(src):
    try:
        mod = load(src)
        got = [(r[0], r[1]) for r in mod.screen_stocks(db(), D, FLOOR, offset_days=1)]
        sent = fetch_sql(load(src))
    except Exception as e:  # a mutant that breaks the module fails loudly
        return [f"error: {type(e).__name__}: {e}"]
    out = [] if got == EXPECT else [f"book {got} != {EXPECT}"]
    # the old bug's other half: pass-only fetches, so the latest-filing pick never sees a failing filing
    if re.search(r"returnOnEquity\s*[<>]", sent.get("metrics_cache", "returnOnEquity >")):
        out.append("key_metrics fetch filters on returnOnEquity")
    if re.search(r"priceToSalesRatio\s*[<>]", sent.get("ratios_cache", "priceToSalesRatio >")):
        out.append("financial_ratios fetch filters on priceToSalesRatio")
    return out


M_WHERE = "FROM metrics_cache WHERE filing_epoch <= ? AND filing_epoch >= ?"
R_WHERE = "FROM ratios_cache WHERE filing_epoch <= ? AND filing_epoch >= ?"
MUTANTS = {
    # the old bug: a test applied before picking the latest filing
    "ROE test before latest": (M_WHERE, M_WHERE + " AND returnOnEquity > 0.10"),
    "size test before latest": (M_WHERE, M_WHERE + " AND marketCap > 500000000"),
    "P/S test before latest": (R_WHERE, R_WHERE + " AND priceToSalesRatio < 1.0"),
    "no staleness bound (metrics)": (M_WHERE, "FROM metrics_cache WHERE filing_epoch <= ? AND ? IS NOT NULL"),
    "no staleness bound (ratios)": (R_WHERE, "FROM ratios_cache WHERE filing_epoch <= ? AND ? IS NOT NULL"),
    "no lag": ("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
               "cutoff_epoch = utc_epoch(target_date)"),
    "bounds swapped": ("[cutoff_epoch, earliest_epoch, cutoff_epoch, earliest_epoch,",
                       "[earliest_epoch, cutoff_epoch, cutoff_epoch, earliest_epoch,"),
    "no P/S ceiling": ("          AND r.priceToSalesRatio < ?", "          AND ? IS NOT NULL"),
    "no ROE test": ("          AND m.returnOnEquity > ?", "          AND ? IS NOT NULL"),
    "no size floor": ("          AND m.marketCap > ?", "          AND ? IS NOT NULL"),
    "no margin test": ("          AND r.grossProfitMargin > ?", "          AND ? IS NOT NULL"),
    # the old bug's other half: only passing filings loaded into the caches
    "ROE-passing-only fetch": ("              AND returnOnEquity IS NOT NULL\n",
                               "              AND returnOnEquity IS NOT NULL\n"
                               "              AND returnOnEquity > {ROE_MIN}\n"),
    "P/S-passing-only fetch": ("              AND priceToSalesRatio IS NOT NULL\n",
                               "              AND priceToSalesRatio IS NOT NULL\n"
                               "              AND priceToSalesRatio < {PS_MAX}\n"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_ps_latest_filing: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_ps_latest_filing: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new)):
            survived.append(name)
    if survived:
        print("test_ps_latest_filing: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_ps_latest_filing: PASS ({len(FILINGS)} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
