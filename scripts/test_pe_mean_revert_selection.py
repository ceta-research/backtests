#!/usr/bin/env python3
"""Offline check that pe-mean-revert screens on each company's LATEST FY filing (no network).

The screen must pick the latest financial_ratios and key_metrics filing on or before the 45-day
cutoff first and only then apply the P/E band and the quality tests; a filing older than
MAX_FILING_AGE_DAYS drops the company. Testing first (a pass-only fetch, then the band inside
the latest-row pick) let a company whose P/E had left the band qualify on its last in-band
filing for years. The sector median and its 5-name minimum stay over in-band latest P/Es only.
The topic's real fetch_data_via_api and screen_stocks run on one in-memory DuckDB holding
made-up warehouse tables. Every mutant of the topic's source must change the book.

Usage:
    python scripts/test_pe_mean_revert_selection.py [repo root]
"""
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "pe-mean-revert", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 4, 1)
BAR = date(2020, 4, 2)
FLOOR = 5e8
FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)
OLD_DAY = date(2018, 6, 30)             # 641 days before D
IN_LAG = date(2020, 3, 1)               # inside the 45-day lag
AT_CUTOFF = D - timedelta(days=45)
AT_MAX_AGE = D - timedelta(days=540)
TECH, ENERGY, UTIL = "Technology", "Energy", "Utilities"


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("pmr_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


GOOD_M = (0.20, 1e9)                                # returnOnEquity, marketCap
M1, M2 = [(FY19, *GOOD_M)], [(FY18, *GOOD_M), (FY19, *GOOD_M)]
R2 = [(FY18, 8.0, 0.5), (FY19, 8.0, 0.5)]           # priceToEarningsRatio, debtToEquityRatio
# symbol: (sector, financial_ratios filings [(date, pe, de)], key_metrics filings [(date, roe, mcap)])
COMPANIES = {
    "OK": (TECH, R2, M2),
    # FY18 P/E in band, latest above it / a loss / above the old fetch's 200 cap / below it: out
    "PE_HIGH": (TECH, [(FY18, 8.0, 0.5), (FY19, 80.0, 0.5)], M2),
    "PE_LOSS": (TECH, [(FY18, 8.0, 0.5), (FY19, -5.0, 0.5)], M2),
    "PE_200": (TECH, [(FY18, 8.0, 0.5), (FY19, 250.0, 0.5)], M2),
    "PE_LOW": (TECH, [(FY18, 8.0, 0.5), (FY19, 2.0, 0.5)], M2),
    # latest D/E, ROE or size fails: out
    "DE_TURNED": (TECH, [(FY18, 8.0, 0.5), (FY19, 8.0, 3.0)], M2),
    "ROE_TURNED": (TECH, R2, [(FY18, *GOOD_M), (FY19, 0.01, 1e9)]),
    "SHRANK": (TECH, R2, [(FY18, *GOOD_M), (FY19, 0.20, 1e5)]),
    # only filing is 641 days old, in both tables or one: out
    "OLD": (TECH, [(OLD_DAY, 8.0, 0.5)], [(OLD_DAY, *GOOD_M)]),
    "OLD_R": (TECH, [(OLD_DAY, 8.0, 0.5)], M1),
    "OLD_M": (TECH, [(FY19, 8.0, 0.5)], [(OLD_DAY, *GOOD_M)]),
    # newest filing is inside the lag and fails; the one before passes: in
    "LAGGED": (TECH, [(FY19, 6.0, 0.5), (IN_LAG, 80.0, 0.5)], [(FY19, *GOOD_M), (IN_LAG, -1.0, 1e9)]),
    # filings exactly on the cutoff and exactly MAX_FILING_AGE_DAYS old: both bounds are inclusive
    "EDGE_LAG": (TECH, [(AT_CUTOFF, 9.0, 0.5)], [(AT_CUTOFF, *GOOD_M)]),
    "EDGE_AGE": (TECH, [(AT_MAX_AGE, 10.0, 0.5)], [(AT_MAX_AGE, *GOOD_M)]),
    # one security, two lines: TWIN_A has no entry bar, so TWIN_B is the line kept
    "TWIN_A": (TECH, [(FY19, 7.0, 0.5)], M1),
    "TWIN_B": (TECH, [(FY19, 7.0, 0.5)], M1),
    # Energy: 5 in-band names, median 20, CAND at 0.55 of it; three latest P/Es under the band
    # would pull the median to 15.5 and push CAND out if they counted
    "CAND": (ENERGY, [(FY19, 11.0, 0.5)], M1),
    "E_LOW1": (ENERGY, [(FY19, 1.0, 0.5)], M1),
    "E_LOW2": (ENERGY, [(FY19, 1.5, 0.5)], M1),
    "E_LOW3": (ENERGY, [(FY19, 2.0, 0.5)], M1),
    # Utilities: 4 in-band names, so no median and THIN is out; two above the band would make 6
    "THIN": (UTIL, [(FY19, 6.0, 0.5)], M1),
    "U_DEAR1": (UTIL, [(FY19, 60.0, 0.5)], M1),
    "U_DEAR2": (UTIL, [(FY19, 70.0, 0.5)], M1),
}
# dear names that set each sector's median at 20 and never pass the 60% cut (Technology keeps
# a majority of them even when every stale name above is let back in)
for _sector, _n in ((TECH, 16), (ENERGY, 4), (UTIL, 3)):
    for _i in range(_n):
        COMPANIES[f"Z{_sector[:2].upper()}{_i:02d}"] = (_sector, [(FY19, 20.0, 0.5)], M1)
ISIN = {"TWIN_A": "US0000000017", "TWIN_B": "US0000000017"}
UNPRICED = {"TWIN_A"}
EXPECT = [("LAGGED", 1e9), ("TWIN_B", 1e9), ("OK", 1e9), ("EDGE_LAG", 1e9), ("EDGE_AGE", 1e9),
          ("CAND", 1e9)]                            # P/E over sector median, ascending


def warehouse():
    """The made-up warehouse tables; the topic's fetch builds its caches beside them."""
    con = duckdb.connect()
    con.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, sector VARCHAR, isin VARCHAR,"
                " isActivelyTrading BOOLEAN)")
    con.execute("CREATE TABLE financial_ratios(symbol VARCHAR, period VARCHAR, dateEpoch BIGINT,"
                " priceToEarningsRatio DOUBLE, debtToEquityRatio DOUBLE)")
    con.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, dateEpoch BIGINT,"
                " returnOnEquity DOUBLE, marketCap DOUBLE)")
    con.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE,"
                " volume DOUBLE)")
    for s, (sector, ratios, metrics) in COMPANIES.items():
        con.execute("INSERT INTO profile VALUES (?, 'NYSE', ?, ?, TRUE)", [s, sector, ISIN.get(s)])
        for d, pe, de in ratios:
            con.execute("INSERT INTO financial_ratios VALUES (?, 'FY', ?, ?, ?)", [s, ep(d), pe, de])
        for d, roe, mcap in metrics:
            con.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, ?)", [s, ep(d), roe, mcap])
        if s not in UNPRICED:
            con.execute("INSERT INTO stock_eod VALUES (?, ?, ?, 5.0, 1e6)", [s, BAR, ep(BAR)])
    return con


class _Client:
    """Stands in for CetaResearch: the profile query runs on the made-up warehouse."""

    def __init__(self, con):
        self.con = con

    def query(self, sql, **kw):
        cur = self.con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetched(mod):
    """The topic's own fetch, with its loaders pointed at the made-up warehouse."""
    con = warehouse()

    def fake_query_parquet(client, sql, c, table_name, **kw):
        c.execute(f"CREATE TABLE {table_name} AS {sql}")
        return c.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    def fake_load_isin_map(client, c, exchanges, sym_filter_sql, **kw):
        c.execute("CREATE TABLE isin_map AS SELECT symbol, isin, isActivelyTrading, TRUE AS home_ccy"
                  f" FROM profile WHERE {sym_filter_sql}")

    mod.duckdb = types.SimpleNamespace(connect=lambda *a, **k: con)
    mod.query_parquet = fake_query_parquet
    mod.load_isin_map = fake_load_isin_map
    mod.remove_price_oscillations = lambda *a, **k: None
    mod.print = lambda *a, **k: None
    return mod.fetch_data_via_api(_Client(con), ["NYSE"], [D])


def failures(src):
    """(book mismatches, error): a mutant has to change the book, not just break the module."""
    try:
        mod = load(src)
        con = fetched(mod)
        got = [(r[0], r[1]) for r in mod.screen_stocks(con, D, FLOOR, offset_days=1)]
        mod.MAX_STOCKS = 3      # the twin is collapsed BEFORE the cut, so the third slot is refilled
        cut = [r[0] for r in mod.screen_stocks(con, D, FLOOR, offset_days=1)]
        con.close()
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"
    out = [] if got == EXPECT else [f"book {got} != {EXPECT}"]
    if cut != [s for s, _ in EXPECT[:3]]:
        out.append(f"top 3 {cut} != {[s for s, _ in EXPECT[:3]]}")
    return out, None


R_WHERE = "WHERE r.filing_epoch <= ? AND r.filing_epoch >= ?"
M_WHERE = "WHERE filing_epoch <= ? AND filing_epoch >= ?"
FETCH_PE = "              AND priceToEarningsRatio IS NOT NULL\n"
BAND = "              AND lr.pe BETWEEN ? AND ?\n"
RATIO_CUT = "        WHERE cs.pe / sm.median_pe < ?\n"
PARAMS = ("          PE_MIN, PE_MAX,\n          ROE_MIN, DE_MAX, mktcap_min,\n"
          "          SECTOR_MIN_STOCKS,\n          SECTOR_RATIO_MAX,\n")
DEDUP = ("        FROM ({isin_dedup_sql(candidates, target_date, offset_days, MIN_ENTRY_PRICE)})\n"
         "        ORDER BY pe_ratio ASC, symbol\n        LIMIT ?\n")
# name: [(old, new), ...], every `old` exactly once in the source
MUTANTS = {
    # the old bug: the band (or another test) applied before picking the latest filing
    "P/E band before latest": [(R_WHERE, R_WHERE + " AND r.priceToEarningsRatio BETWEEN 3.0 AND 50.0")],
    "D/E test before latest": [(R_WHERE, R_WHERE + " AND r.debtToEquityRatio < 2.0")],
    "ROE test before latest": [(M_WHERE, M_WHERE + " AND returnOnEquity > 0.08")],
    "size test before latest": [(M_WHERE, M_WHERE + " AND marketCap > 500000000")],
    # the old bug's other half: only positive, under-200 P/Es loaded into ratios_cache
    "positive-P/E-only fetch": [(FETCH_PE, FETCH_PE + "              AND priceToEarningsRatio > 0\n")],
    "P/E-under-200-only fetch": [(FETCH_PE, FETCH_PE + "              AND priceToEarningsRatio < 200\n")],
    "no staleness bound (ratios)": [(R_WHERE, "WHERE r.filing_epoch <= ? AND ? IS NOT NULL")],
    "no staleness bound (metrics)": [(M_WHERE, "WHERE filing_epoch <= ? AND ? IS NOT NULL")],
    "staleness bound 10x": [("MAX_FILING_AGE_DAYS = 540 ", "MAX_FILING_AGE_DAYS = 5400 ")],
    "no lag": [("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
                "cutoff_epoch = utc_epoch(target_date)")],
    "bounds swapped": [("[cutoff_epoch, earliest_epoch,\n", "[earliest_epoch, cutoff_epoch,\n")],
    # the band moved below the sector median: out-of-band P/Es would count in it and in the 5-name minimum
    "band after the median": [(BAND, ""), (RATIO_CUT, RATIO_CUT.rstrip("\n") + " AND cs.pe BETWEEN ? AND ?\n"),
                              (PARAMS, PARAMS[len("          PE_MIN, PE_MAX,\n"):] + "          PE_MIN, PE_MAX,\n")],
    "no P/E band": [(BAND, "              AND ? IS NOT NULL AND ? IS NOT NULL\n")],
    "no sector-ratio cut": [(RATIO_CUT, "        WHERE ? IS NOT NULL\n")],
    "no sector minimum": [("HAVING COUNT(*) >= ?", "HAVING ? IS NOT NULL")],
    "no ROE test": [("              AND lm.returnOnEquity > ?", "              AND ? IS NOT NULL")],
    "no D/E test": [("lr.debtToEquityRatio < ?))", "? IS NOT NULL))")],
    "no size floor": [("              AND lm.marketCap > ?", "              AND ? IS NOT NULL")],
    # the campaign dedup must stay around the candidates, before the top-N cut
    "no same-security dedup": [(DEDUP, "        FROM ({candidates})\n        ORDER BY pe_ratio ASC, symbol\n"
                                       "        LIMIT ?\n")],
    "dedup after the cut": [(DEDUP, "        FROM ({isin_dedup_sql('SELECT * FROM (' + candidates + ') ORDER BY pe_ratio ASC,"
                                    " symbol LIMIT ?', target_date, offset_days, MIN_ENTRY_PRICE)})\n"
                                    "        ORDER BY pe_ratio ASC, symbol\n")],
}


def main():
    base, err = failures(SRC)
    if base or err:
        print("test_pe_mean_revert_selection: FAIL", err or base)
        sys.exit(1)
    survived, broken = [], []
    for name, pairs in MUTANTS.items():
        src = SRC
        for old, new in pairs:
            if SRC.count(old) != 1:
                print(f"test_pe_mean_revert_selection: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            src = src.replace(old, new)
        got, err = failures(src)
        if err:
            broken.append(f"{name}: {err}")
        elif not got:
            survived.append(name)
    if survived or broken:
        print("test_pe_mean_revert_selection: FAIL (mutants survived:", survived, "broken:", broken, ")")
        sys.exit(1)
    print(f"test_pe_mean_revert_selection: PASS ({len(COMPANIES)} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
