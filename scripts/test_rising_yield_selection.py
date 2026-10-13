#!/usr/bin/env python3
"""Offline check that rising-yield tests each company's LATEST complete FY row (no network).

The screen must first pick the newest FY row on or before the 45-day cutoff and no older than
MAX_FILING_AGE_DAYS, whatever the year end, and only then require three consecutive fiscal
years of rising, positive dividendYield plus the DPS-driver and payout tests on that row. Testing
first (rising rows only, then the newest of them, no age limit) kept a company on its last rising
year for decades: AJRD ranked in the 2024 top 30 on its FY1989 yield. Stopping at the calendar year before the
rebalance did the same at one year's depth for January to mid-May year ends (March: most Indian
and Japanese companies). A fiscal year with a NULL or non-positive yield, or with no row, breaks
the run; the fetch used to drop those rows, so LAG skipped them. A row dated after the cutoff must
not change the book.
The topic's real fetch and screen run against a made-up warehouse, and every mutant of the
topic's source must trip a check.

Usage:
    python scripts/test_rising_yield_selection.py [repo root]
"""
import os
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "rising-yield", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 4, 1)          # FY18 and FY19 rows (Dec 31) both sit inside [D-540, D-45]
D_JAN = date(2020, 1, 20)     # FY19 rows (Dec 31) are still inside the 45-day lag
D_JUL = date(2020, 7, 1)      # the production rebalance date: March year ends are past the lag
FLOOR = 5e8
AT_MAX_AGE = D - timedelta(days=540)
TOO_OLD = AT_MAX_AGE - timedelta(days=1)
AT_CUTOFF = D - timedelta(days=45)
IN_LAG = D - timedelta(days=44)          # one day inside the 45-day lag


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("rising_yield_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def run(end, *rows):
    """FY rows (dividendYield, dividendPerShare, dividendPayoutRatio), oldest first, the last one ending `end`."""
    first = end.year - len(rows) + 1
    return [(end.replace(year=first + i), *r) for i, r in enumerate(rows)]


FY19 = date(2019, 12, 31)
RISING = run(FY19, (0.01, 1.0, 0.5), (0.02, 1.1, 0.5), (0.03, 1.2, 0.5), (0.04, 1.3, 0.5))
GOOD_KM = run(FY19, (0.20, 1e9), (0.20, 1e9), (0.20, 1e9))
# symbol: (financial_ratios FY rows, key_metrics FY rows [(period end, returnOnEquity, marketCap)])
COMPANIES = {
    "OK": (RISING, GOOD_KM),
    "OKB": (RISING, GOOD_KM),                # second line of OK (same ISIN): collapses into it
    # the AJRD shape: rising through FY18, FY19 falls. In at D_JAN (FY19 not out yet), out at D
    "STALE": (run(FY19, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.06, 1.2, 0.5), (0.01, 1.3, 0.5)), GOOD_KM),
    # dividend cut to zero in FY19: the zero row is the latest one, not a row to skip
    "CUT": (run(FY19, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.055, 1.2, 0.5), (0.0, 0.0, 0.0)), GOOD_KM),
    # FY17 has a NULL yield, a zero yield, or no row: FY16 is not "the year before FY18"
    "NULL_YEAR": (run(FY19, (0.01, 0.9, 0.5), (0.02, 1.0, 0.5), (None, None, None), (0.03, 1.2, 0.5),
                      (0.04, 1.3, 0.5)), GOOD_KM),
    "ZERO_YEAR": (run(FY19, (0.02, 1.0, 0.5), (0.0, 0.0, 0.0), (0.03, 1.2, 0.5), (0.04, 1.3, 0.5)), GOOD_KM),
    "MISSING_YEAR": ([r for r in RISING if r[0].year != 2017], GOOD_KM),
    # FY19 yield still rises but DPS fell (price-decline driver), or payout left the band
    "DPS_TURNED": (run(FY19, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.042, 1.2, 0.5), (0.07, 1.1, 0.5)), GOOD_KM),
    "PAYOUT_TURNED": (run(FY19, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.044, 1.2, 0.5), (0.05, 1.3, 0.9)), GOOD_KM),
    # newest row is 541 days old at D, in financial_ratios or in key_metrics
    "OLD_R": (run(TOO_OLD, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.046, 1.2, 0.5)), GOOD_KM),
    "OLD_M": (RISING, run(date(2017, 12, 31), (0.20, 1e9), (0.20, 1e9))),
    # newest row exactly MAX_FILING_AGE_DAYS old at D: the bound is inclusive
    "EDGE_AGE": (run(AT_MAX_AGE, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.045, 1.2, 0.5)), GOOD_KM),
    # newest row exactly on the 45-day cutoff at D, and the run only completes there: the bound is inclusive
    "EDGE_LAG": (run(AT_CUTOFF, (0.05, 1.0, 0.5), (0.02, 1.1, 0.5), (0.03, 1.2, 0.5), (0.056, 1.3, 0.5)), GOOD_KM),
    # newest row is one day inside the lag at D and cuts to zero; the row before completes a run: in at D
    # and D_JAN, out on 1 July
    "LAG44": (run(IN_LAG, (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.048, 1.2, 0.5), (0.0, 0.0, 0.0)), GOOD_KM),
    # January year end: the row ending 2020-01-31 is past the lag at D, so it is the one tested.
    # FY_JAN's yield falls there (out at D); FY_JAN_UP's run only completes there (in at D)
    "FY_JAN": (run(date(2020, 1, 31), (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.05, 1.2, 0.5), (0.01, 1.3, 0.5)),
               GOOD_KM),
    "FY_JAN_UP": (run(date(2020, 1, 31), (0.05, 1.0, 0.5), (0.02, 1.1, 0.5), (0.03, 1.2, 0.5), (0.055, 1.3, 0.5)),
                  GOOD_KM),
    # March year end, dividend cut to zero in the year to 2020-03-31: inside the lag at D, out on 1 July
    "MAR_CUT": (run(date(2020, 3, 31), (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.052, 1.2, 0.5), (0.0, 0.0, 0.0)),
                GOOD_KM),
    # year end moved June -> December in 2019: at D_JAN the December stub is not known yet (the June row
    # is tested), afterwards the stub is the latest row
    "STUB": (run(date(2019, 6, 30), (0.05, 1.0, 0.5), (0.02, 1.1, 0.5), (0.03, 1.2, 0.5), (0.041, 1.3, 0.5))
             + [(date(2019, 12, 31), 0.0, 0.0, 0.0)], GOOD_KM),
    # two known rows in calendar 2019: the later one (dividend cut) is the latest, not the higher-yield one
    "FY_CHANGE": (run(date(2019, 3, 31), (0.02, 1.0, 0.5), (0.03, 1.1, 0.5), (0.05, 1.2, 0.5))
                  + [(date(2019, 9, 30), 0.0, 0.0, 0.0)], GOOD_KM),
    # latest key_metrics row fails ROE or size
    "ROE_TURNED": (RISING, run(FY19, (0.20, 1e9), (0.20, 1e9), (-0.05, 1e9))),
    "SHRANK": (RISING, run(FY19, (0.20, 1e9), (0.20, 1e9), (0.20, 1e5))),
}
# yield_change_2yr descending, then symbol
EXPECT = {
    D: [("EDGE_LAG", 1e9), ("FY_JAN_UP", 1e9), ("MAR_CUT", 1e9), ("LAG44", 1e9), ("EDGE_AGE", 1e9), ("OK", 1e9)],
    D_JAN: [("STALE", 1e9), ("CUT", 1e9), ("MAR_CUT", 1e9), ("FY_JAN", 1e9), ("LAG44", 1e9), ("OLD_R", 1e9),
            ("EDGE_AGE", 1e9), ("PAYOUT_TURNED", 1e9), ("DPS_TURNED", 1e9), ("STUB", 1e9), ("OK", 1e9),
            ("ROE_TURNED", 1e9), ("SHRANK", 1e9)],
    D_JUL: [("EDGE_LAG", 1e9), ("FY_JAN_UP", 1e9), ("OK", 1e9)],
}


def warehouse():
    wh = duckdb.connect()
    wh.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR,"
               " isActivelyTrading BOOLEAN, currency VARCHAR)")
    wh.execute("CREATE TABLE financial_ratios(symbol VARCHAR, period VARCHAR, date DATE, dateEpoch BIGINT,"
               " dividendYield DOUBLE, dividendPerShare DOUBLE, dividendPayoutRatio DOUBLE)")
    wh.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, dateEpoch BIGINT,"
               " returnOnEquity DOUBLE, marketCap DOUBLE)")
    wh.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    for s, (ratios, metrics) in COMPANIES.items():
        isin = f"US{'OK' if s == 'OKB' else s:0>10}"
        wh.execute("INSERT INTO profile VALUES (?, 'NYSE', 'US', ?, TRUE, 'USD')", [s, isin])
        for d, dy, dps, payout in ratios:
            wh.execute("INSERT INTO financial_ratios VALUES (?, 'FY', ?, ?, ?, ?, ?)", [s, d, ep(d), dy, dps, payout])
        for d, roe, mcap in metrics:
            wh.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, ?)", [s, ep(d), roe, mcap])
        for d in EXPECT:
            bar = d + timedelta(days=1)
            wh.execute("INSERT INTO stock_eod VALUES (?, ?, ?, 5.0, 1e6)", [s, bar, ep(bar)])
    return wh


class _Client:
    """Stands in for CetaResearch: runs the topic's own SQL on the made-up warehouse."""

    def __init__(self, wh):
        self.wh = wh

    def query(self, sql, format="json", **kw):
        if format != "parquet":
            cur = self.wh.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.parquet")
            self.wh.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
            with open(path, "rb") as fh:
                return fh.read()


WH = warehouse()


def failures(src):
    try:
        mod = load(src)
        mod.print = lambda *a, **k: None
        con = mod.fetch_data_via_api(_Client(WH), ["NYSE"], sorted(EXPECT))
        got = {d: [(r[0], r[1]) for r in mod.screen_stocks(con, d, FLOOR, offset_days=1)] for d in EXPECT}
        unpaid = con.execute("SELECT COUNT(*) FROM ratios_cache"
                             " WHERE dividendYield IS NULL OR dividendYield <= 0").fetchone()[0]
        con.close()
    except Exception as e:  # a mutant that breaks the module fails loudly
        return [f"error: {type(e).__name__}: {e}"]
    out = [f"{d} book {got[d]} != {want}" for d, want in EXPECT.items() if got[d] != want]
    # the old bug's other half: non-paying years dropped at fetch, so LAG never sees them
    if not unpaid:
        out.append("financial_ratios fetch drops NULL and non-positive dividendYield rows")
    return out


WINDOW = "WHERE filing_epoch <= ? AND filing_epoch >= ?"
NO_AGE = "WHERE filing_epoch <= ? AND ? IS NOT NULL"
R_WHERE = "FROM yield_trajectory\n            " + WINDOW    # yield_trajectory rows
M_WHERE = "FROM metrics_cache\n            " + WINDOW       # metrics_cache rows
ALL_ROWS = "FROM deduped JOIN yearly USING (symbol, fiscal_year)"
YEARS = "              AND fy_1yr = fiscal_year - 1 AND fy_2yr = fiscal_year - 2\n"
MUTANTS = {
    # the old bug: a test applied before picking the latest row
    "rising test before latest": (ALL_ROWS + '\n    """)',
                                  ALL_ROWS + "\n        WHERE yield_2yr IS NOT NULL\n"
                                  '          AND dividendYield > yield_1yr\n          AND yield_1yr > yield_2yr\n    """)'),
    "driver test before latest": (R_WHERE, R_WHERE + " AND yield_driver IN ('Consecutive DPS Growth',"
                                                     " 'Partial DPS Growth')"),
    "payout test before latest": (R_WHERE, R_WHERE + " AND dividendPayoutRatio <= 0.75"),
    "no staleness bound (ratios)": (R_WHERE, R_WHERE.replace(WINDOW, NO_AGE)),
    "no staleness bound (metrics)": (M_WHERE, M_WHERE.replace(WINDOW, NO_AGE)),
    # the old bug at one year's depth: rows of the rebalance year ignored (every test date is in 2020)
    "calendar-year bound": (R_WHERE, R_WHERE + " AND fiscal_year <= 2019"),
    # look-ahead: a later row of the same calendar year, not known yet, replaces the known one
    "year's newest row only": (ALL_ROWS, ALL_ROWS + " WHERE rn = 1"),
    "latest by year, not date": ("PARTITION BY symbol ORDER BY filing_epoch DESC, dividendYield",
                                 "PARTITION BY symbol ORDER BY fiscal_year DESC, dividendYield"),
    "no lag": ("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
               "cutoff_epoch = utc_epoch(target_date)"),
    # each window bound one day off
    "filing a day too old allowed": ("MAX_FILING_AGE_DAYS = 540 ", "MAX_FILING_AGE_DAYS = 541 "),
    "filing age measured from the cutoff": (
        "earliest_epoch = utc_epoch(target_date - timedelta(days=MAX_FILING_AGE_DAYS))",
        "earliest_epoch = cutoff_epoch - MAX_FILING_AGE_DAYS * 86400"),
    "lag a day short": ("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
                        "cutoff_epoch = utc_epoch(target_date - timedelta(days=44))"),
    "lag a day long": ("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
                       "cutoff_epoch = utc_epoch(target_date - timedelta(days=46))"),
    "bounds swapped": ("[cutoff_epoch, earliest_epoch, cutoff_epoch, earliest_epoch,",
                       "[earliest_epoch, cutoff_epoch, cutoff_epoch, earliest_epoch,"),
    # the old bug's other half: years skipped instead of breaking the run
    "positive-yield-only fetch": ("        WHERE period = 'FY'\n          AND {sym_filter_sql}\n",
                                  "        WHERE period = 'FY'\n          AND dividendYield IS NOT NULL\n"
                                  "          AND dividendYield > 0\n          AND {sym_filter_sql}\n"),
    "skipped year allowed": (YEARS, ""),
    "only the year before checked": (YEARS, "              AND fy_1yr = fiscal_year - 1\n"),
    "zero-yield year allowed": (" AND yield_2yr > 0\n", "\n"),
    "no driver test": ("              AND yield_driver IN ('Consecutive DPS Growth', 'Partial DPS Growth')\n", ""),
    "no payout cap": ("lr.dividendPayoutRatio <= ?)", "? IS NOT NULL)"),
    "no ROE test": ("          AND km.returnOnEquity > ?", "          AND ? IS NOT NULL"),
    "no size floor": ("          AND km.marketCap > ?", "          AND ? IS NOT NULL"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_rising_yield_selection: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_rising_yield_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new)):
            survived.append(name)
    if survived:
        print("test_rising_yield_selection: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_rising_yield_selection: PASS ({len(COMPANIES)} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
