#!/usr/bin/env python3
"""Offline check of dogs-of-dow's selection: yield from each company's LATEST FY filing (no network).

screen_dogs must take the latest financial_ratios filing on or before the 45-day cutoff and no older
than MAX_FILING_AGE_DAYS first, and only then require dividendYield > 0. Testing first (yield > 0
inside the window, then rn = 1) kept a company that cut its dividend to zero ranking on its last
paying filing, however old: UBSG.SW stayed a top-10 yielder on SIX at the 2010-2012 rebalances on
its FY2007 filing. A latest filing with no yield drops the company too. The market-cap filing has
the same age bound: without one in range a Dow name keeps the default cap, a non-US name leaves the
blue chips. The same-security dedup still runs before each top-N cut. Every mutant of the topic's
source must trip a check.

Usage:
    python scripts/test_dogs_selection.py [repo root]
"""
import os
import re
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "dogs-of-dow", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 4, 1)
FLOOR = 5e8
FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)   # 457 and 92 days before D
OLD_DAY = date(2018, 6, 30)             # 641 days before D
IN_LAG = date(2020, 3, 1)               # inside the 45-day lag
AT_CUTOFF = D - timedelta(days=45)
AT_MAX_AGE = D - timedelta(days=540)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("dogs_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


CAPS = [(FY18, 2e11), (FY19, 2e11)]
# symbol: (key_metrics filings [(date, marketCap)], financial_ratios filings [(date, dividendYield)])
FILINGS = {
    "OK": (CAPS, [(FY18, 0.030), (FY19, 0.031)]),
    # latest yield is zero after a cut (the Alibaba shape), or missing: out
    "CUT": (CAPS, [(FY18, 0.090), (FY19, 0.0)]),
    "CUT_NULL": (CAPS, [(FY18, 0.080), (FY19, None)]),
    # only ratios filing is 641 days old: out
    "OLD_R": (CAPS, [(OLD_DAY, 0.070)]),
    # only cap filing is 641 days old: a Dow name keeps the default cap, a non-US name is out
    "OLD_M": ([(OLD_DAY, 7e10)], [(FY19, 0.060)]),
    # newest filing is inside the lag and has cut to zero; the one before pays: in, on the older cap
    "LAGGED": ([(FY19, 5e10), (IN_LAG, 9e10)], [(FY19, 0.020), (IN_LAG, 0.0)]),
    # filings exactly on the cutoff and exactly MAX_FILING_AGE_DAYS old: both bounds are inclusive
    "EDGE_LAG": ([(AT_CUTOFF, 2e11)], [(AT_CUTOFF, 0.050)]),
    "EDGE_AGE": ([(AT_MAX_AGE, 2e11)], [(AT_MAX_AGE, 0.050)]),
    # one security, two lines: the line priced under 1.0 loses
    "TWIN_A": (CAPS, [(FY19, 0.040)]),
    "TWIN_B": (CAPS, [(FY19, 0.040)]),
}
# 12 Dow names, U01 the top yielder with a twin line: the twin must not take a top-10 slot
US_CUT = {f"U{i:02d}": (CAPS, [(FY19, 0.13 - i / 100)]) for i in range(1, 13)}
US_CUT["U01Z"] = US_CUT["U01"]
# 31 names, C01 the largest with a twin line, yield rising as size falls: C31 is outside the top 30
XUS_CUT = {f"C{i:02d}": ([(FY19, (40 - i) * 1e9)], [(FY19, i / 1000)]) for i in range(1, 32)}
XUS_CUT["C01Z"] = XUS_CUT["C01"]
ISIN = {"TWIN_A": "X1", "TWIN_B": "X1", "U01": "X2", "U01Z": "X2", "C01": "X3", "C01Z": "X3"}
PRICE = {"TWIN_A": 0.5}

EXPECT = {   # yield descending, then symbol
    "Dow": [("OLD_M", 1e11), ("EDGE_AGE", 2e11), ("EDGE_LAG", 2e11), ("TWIN_B", 2e11), ("OK", 2e11),
            ("LAGGED", 5e10)],
    "non-US": [("EDGE_AGE", 2e11), ("EDGE_LAG", 2e11), ("TWIN_B", 2e11), ("OK", 2e11), ("LAGGED", 5e10)],
    "Dow top 10": [(f"U{i:02d}", 2e11) for i in range(1, 11)],
    "non-US top 10 of 30": [(f"C{i:02d}", (40 - i) * 1e9) for i in range(30, 20, -1)],
}


def db(filings):
    con = duckdb.connect()
    con.execute("CREATE TABLE universe(symbol VARCHAR)")
    con.execute("CREATE TABLE metrics_cache(symbol VARCHAR, marketCap DOUBLE, filing_epoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE ratios_cache(symbol VARCHAR, dividendYield DOUBLE, dividendPerShare DOUBLE,"
                " filing_epoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    for s, (metrics, ratios) in filings.items():
        con.execute("INSERT INTO universe VALUES (?)", [s])
        for d, mcap in metrics:
            con.execute("INSERT INTO metrics_cache VALUES (?, ?, ?, 'FY')", [s, mcap, ep(d)])
        for d, yld in ratios:
            con.execute("INSERT INTO ratios_cache VALUES (?, ?, 1.0, ?, 'FY')", [s, yld, ep(d)])
        con.execute("INSERT INTO prices_cache VALUES (?, ?, ?, 1e6)", [s, ep(date(2020, 4, 2)), PRICE.get(s, 5.0)])
        con.execute("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", [s, ISIN.get(s)])
    return con


class _Client:
    """Stands in for CetaResearch in fetch_data_via_api: one profile row, nothing else."""

    def query(self, sql, **kw):
        return [{"symbol": "OK", "exchange": "BSE"}]


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
    mod.fetch_data_via_api(_Client(), ["BSE"], [D]).close()
    return sent


def failures(src):
    try:
        mod = load(src)
        got = {
            "Dow": mod.screen_dogs(db(FILINGS), D, use_dow=True, offset_days=1),
            "non-US": mod.screen_dogs(db(FILINGS), D, use_dow=False, mktcap_min=FLOOR, offset_days=1),
            "Dow top 10": mod.screen_dogs(db(US_CUT), D, use_dow=True, offset_days=1),
            "non-US top 10 of 30": mod.screen_dogs(db(XUS_CUT), D, use_dow=False, mktcap_min=FLOOR, offset_days=1),
        }
        sent = fetch_sql(load(src))
    except Exception as e:  # a mutant that breaks the module fails loudly
        return [f"error: {type(e).__name__}: {e}"]
    out = [f"{k}: book {got[k]} != {want}" for k, want in EXPECT.items() if got[k] != want]
    # the cache must hold zero and missing yields, or the latest-filing pick never sees a cut
    if re.search(r"dividendYield\s*([<>]|IS\s+NOT\s+NULL)", sent.get("ratios_cache", "dividendYield >")):
        out.append("financial_ratios fetch filters on dividendYield")
    return out


ROWNUM = ("                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY filing_epoch DESC,"
          " dividendYield DESC NULLS LAST) AS rn\n                FROM ratios_cache\n"
          "                WHERE filing_epoch <= ? AND ")
R_DOW = "SELECT symbol, dividendYield, filing_epoch,\n" + ROWNUM      # the Dow branch's latest_ratios
R_XUS = "SELECT symbol, dividendYield,\n" + ROWNUM                    # the non-US branch's
M_DOW = "FROM metrics_cache WHERE filing_epoch <= ? AND "
M_XUS = "FROM metrics_cache\n                WHERE filing_epoch <= ? AND "
AGE, NO_AGE = "filing_epoch >= ?", "? IS NOT NULL"
P_DOW = "[cutoff_epoch, earliest_epoch, cutoff_epoch, earliest_epoch, DOGS_COUNT]"
P_XUS = ("[cutoff_epoch, earliest_epoch, mktcap_min, BLUECHIP_COUNT,\n"
         "              cutoff_epoch, earliest_epoch, DOGS_COUNT]")
WRAP = "FROM ({isin_dedup_sql(candidates, target_date, offset_days, 1.0)})\n"
CUT_DOW, CUT_XUS = "ORDER BY dividendYield DESC, symbol", "ORDER BY marketCap DESC, symbol"
FETCH = "WHERE period = 'FY' AND {sym_filter_sql}"
MUTANTS = {
    # the old bug: yield > 0 applied before picking the latest filing, with and without the age bound
    "yield test before latest (Dow)": (R_DOW + AGE, R_DOW + AGE + " AND dividendYield > 0"),
    "yield test before latest (non-US)": (R_XUS + AGE, R_XUS + AGE + " AND dividendYield > 0"),
    "the old screen (Dow)": (R_DOW + AGE, R_DOW + NO_AGE + " AND dividendYield IS NOT NULL AND dividendYield > 0"),
    "the old screen (non-US)": (R_XUS + AGE, R_XUS + NO_AGE + " AND dividendYield IS NOT NULL AND dividendYield > 0"),
    "missing yield skipped (Dow)": (R_DOW + AGE, R_DOW + AGE + " AND dividendYield IS NOT NULL"),
    "missing yield skipped (non-US)": (R_XUS + AGE, R_XUS + AGE + " AND dividendYield IS NOT NULL"),
    "no yield test (Dow)": ("WHERE r.rn = 1 AND r.dividendYield > 0", "WHERE r.rn = 1"),
    "no yield test (non-US)": ("            WHERE r.dividendYield > 0\n", ""),
    "no staleness bound (ratios, Dow)": (R_DOW + AGE, R_DOW + NO_AGE),
    "no staleness bound (ratios, non-US)": (R_XUS + AGE, R_XUS + NO_AGE),
    "no staleness bound (metrics, Dow)": (M_DOW + AGE, M_DOW + NO_AGE),
    "no staleness bound (metrics, non-US)": (M_XUS + AGE, M_XUS + NO_AGE),
    "staleness bound ten times wider": ("MAX_FILING_AGE_DAYS = 540", "MAX_FILING_AGE_DAYS = 5400"),
    "no lag": ("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
               "cutoff_epoch = utc_epoch(target_date)"),
    "ratios bounds swapped (Dow)": (P_DOW, P_DOW.replace("[cutoff_epoch, earliest_epoch", "[earliest_epoch, cutoff_epoch")),
    "metrics bounds swapped (Dow)": (P_DOW, P_DOW.replace("cutoff_epoch, earliest_epoch, DOGS", "earliest_epoch, cutoff_epoch, DOGS")),
    "ratios bounds swapped (non-US)": (P_XUS, P_XUS.replace("cutoff_epoch, earliest_epoch, DOGS", "earliest_epoch, cutoff_epoch, DOGS")),
    "metrics bounds swapped (non-US)": (P_XUS, P_XUS.replace("[cutoff_epoch, earliest_epoch", "[earliest_epoch, cutoff_epoch")),
    # the same-security dedup: gone, or after the top-N cut
    "no dedup (Dow)": ("\n            " + WRAP, "\n            FROM ({candidates})\n"),
    "no dedup (non-US)": ("\n                " + WRAP, "\n                FROM ({candidates})\n"),
    "cut before dedup (Dow)": (WRAP + "            " + CUT_DOW, WRAP.replace(
        "candidates,", f"'SELECT * FROM (' + candidates + ') {CUT_DOW} LIMIT 10',") + "            " + CUT_DOW),
    "cut before dedup (non-US)": (WRAP + "                " + CUT_XUS, WRAP.replace(
        "candidates,", f"'SELECT * FROM (' + candidates + ') {CUT_XUS} LIMIT 30',") + "                " + CUT_XUS),
    # the old bug's other half, were it added: only paying (or non-missing) filings loaded into the cache
    "yield-passing-only fetch": (FETCH, FETCH.replace("AND", "AND dividendYield > 0 AND")),
    "non-missing-only fetch": (FETCH, FETCH.replace("AND", "AND dividendYield IS NOT NULL AND")),
}


def main():
    base = failures(SRC)
    if base:
        print("test_dogs_selection: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_dogs_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new)):
            survived.append(name)
    if survived:
        print("test_dogs_selection: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    n = len(FILINGS) + len(US_CUT) + len(XUS_CUT)
    print(f"test_dogs_selection: PASS ({n} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
