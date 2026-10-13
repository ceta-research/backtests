#!/usr/bin/env python3
"""Offline check that low-debt compares each company's current FY row with ITS OWN prior year (no network).

screen_stocks takes the latest FY row on or before the 45-day cutoff and no older than 595 days,
then pairs it with the same company's previous fiscal year: the latest row 300-430 days before
THAT row. The prior row used to come from a window off the rebalance date (410-595 days back,
185 days wide), which no fiscal year ending between May 18 and Nov 13 can fall in at a July
rebalance, so June, September and October year-ends were never screened on any market. A company
that skipped a year has no prior row and stays out. Every mutant of the topic's source must trip
a check, by changing the book and not by raising.

Usage:
    python scripts/test_low_debt_selection.py [repo root]
"""
import calendar
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "low-debt", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 7, 1)
FLOOR = 1e9
CUTOFF = D - timedelta(days=45)         # 2020-05-17, inclusive
TOO_OLD = D - timedelta(days=595)       # 2018-11-14, exclusive
FY17, FY18, FY19 = date(2017, 12, 31), date(2018, 12, 31), date(2019, 12, 31)
STUB = date(2019, 6, 30)                # 184 days before FY19
IN_LAG = date(2020, 6, 15)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def back(d, days):
    return d - timedelta(days=days)


def load(src):
    mod = types.ModuleType("low_debt_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


# income (netIncome, grossProfit, revenue); balance (totalAssets, totalCurrentAssets,
# totalCurrentLiabilities, longTermDebt, totalStockholdersEquity). CURR against PRIOR scores F = 9.
PRIOR_I, CURR_I = (80, 300, 900), (100, 400, 1000)
PRIOR_B, CURR_B = (1000, 400, 200, 100, 500), (1050, 500, 200, 50, 600)
# rows BETTER than CURR: taken as the prior year, either one alone drops F to 6
DECOY_I, DECOY_B = (300, 900, 2000), (1000, 900, 200, 10, 900)
BAD_I, BAD_B = (-50, 100, 500), (2000, 100, 400, 1500, 100)
OCF = 150.0


def co(*days, inc=None, bal=None, de=None, mcap=5e9, px=5.0, isin=None):
    """One FY row per day in every table: the last day gets the CURR values, earlier days PRIOR.

    inc/bal override a day's income/balance values (None drops that statement's row), de its D/E.
    """
    i = {d: (CURR_I if d == days[-1] else PRIOR_I) for d in days}
    b = {d: (CURR_B if d == days[-1] else PRIOR_B) for d in days}
    i.update(inc or {})
    b.update(bal or {})
    return {"days": days, "inc": i, "bal": b, "de": de or {}, "mcap": mcap, "px": px, "isin": isin}


def year(curr, gap=365, **kw):
    return co(back(curr, gap), curr, **kw)


COMPANIES = {
    # fiscal calendars: the last five have year-ends in the old dead zone (May 18 - Nov 13)
    "DEC": year(FY19, mcap=6e9),
    "MAR": co(date(2019, 3, 31), date(2020, 3, 31), mcap=6e9),
    "WK53": co(date(2018, 12, 29), date(2020, 1, 4)),            # 53-week year, 371 days
    "JUN": year(date(2019, 6, 30), mcap=9e9),
    "SEP": year(date(2019, 9, 30), mcap=8e9),
    "OCT": year(date(2019, 10, 31), mcap=7e9),
    "MAY31": year(date(2019, 5, 31)),
    "NOV13": year(date(2019, 11, 13)),
    "WK52": co(date(2018, 9, 29), date(2019, 9, 28)),            # 52-week year, 364 days
    # a skipped year (in both statements, or in one): no prior row, out
    "GAP": co(FY17, FY19),
    "GAP_JUN": co(date(2017, 6, 30), date(2019, 6, 30)),
    "GAP_INC": co(FY17, FY18, FY19, inc={FY18: None}),
    "GAP_BAL": co(FY17, FY18, FY19, bal={FY18: None}),
    # an extra FY row 184 days before the current one (a stub period) is not the prior year: in
    "STUB_INC": co(FY18, STUB, FY19, inc={STUB: DECOY_I}, bal={STUB: None}),
    "STUB_BAL": co(FY18, STUB, FY19, bal={STUB: DECOY_B}, inc={STUB: None}),
    # two rows inside the window (396 and 334 days back): the later one is the prior year: in
    "TWO_INC": co(date(2018, 11, 30), date(2019, 1, 31), FY19,
                  inc={date(2018, 11, 30): DECOY_I}, bal={date(2018, 11, 30): None}),
    "TWO_BAL": co(date(2018, 11, 30), date(2019, 1, 31), FY19,
                  bal={date(2018, 11, 30): DECOY_B}, inc={date(2018, 11, 30): None}),
    # the gap bounds are inclusive
    "EDGE_MIN": year(FY19, gap=300),
    "EDGE_MAX": year(FY19, gap=430),
    "UNDER_MIN": year(FY19, gap=299),
    "OVER_MAX": year(FY19, gap=431),
    # current-row rule, unchanged: on the cutoff is in, 595 days old is out, a row in the lag is ignored
    "EDGE_LAG": year(CUTOFF),
    "EDGE_FRESH": year(TOO_OLD + timedelta(days=1)),
    "EDGE_STALE": year(TOO_OLD),
    "STALE": year(date(2018, 10, 31)),
    "LAGGED": co(FY18, FY19, IN_LAG, inc={FY19: CURR_I, IN_LAG: BAD_I},
                 bal={FY19: CURR_B, IN_LAG: BAD_B}, de={IN_LAG: 0.9}),
    # the tests themselves, unchanged: F-score, D/E on the latest row, size
    "WORSE": co(FY18, FY19, inc={FY18: CURR_I, FY19: PRIOR_I}, bal={FY18: CURR_B, FY19: PRIOR_B}),
    "LEVERED": year(FY19, de={FY19: 0.9}),
    "SMALL": year(FY19, mcap=1e6),
    # same ISIN: the line with an entry price is kept
    "TWIN_A": year(FY19, px=None, isin="US0000000001"),
    "TWIN_B": year(FY19, isin="US0000000001"),
}
OUT = {"GAP", "GAP_JUN", "GAP_INC", "GAP_BAL", "UNDER_MIN", "OVER_MAX", "EDGE_STALE", "STALE",
       "WORSE", "LEVERED", "SMALL", "TWIN_A"}
EXPECT = sorted(((s, c["mcap"]) for s, c in COMPANIES.items() if s not in OUT),
                key=lambda r: (-r[1], r[0]))            # marketCap DESC, symbol


def db(companies, epoch_type="UINTEGER"):
    """The caches screen_stocks reads. The warehouse's dateEpoch is unsigned 32-bit."""
    con = duckdb.connect()
    e = f"filing_epoch {epoch_type}, period VARCHAR"
    con.execute(f"CREATE TABLE income_cache(symbol VARCHAR, netIncome DOUBLE, grossProfit DOUBLE, revenue DOUBLE, {e})")
    con.execute("CREATE TABLE balance_cache(symbol VARCHAR, totalAssets DOUBLE, totalCurrentAssets DOUBLE,"
                " totalCurrentLiabilities DOUBLE, longTermDebt DOUBLE, totalStockholdersEquity DOUBLE,"
                f" {e})")
    con.execute(f"CREATE TABLE cashflow_cache(symbol VARCHAR, operatingCashFlow DOUBLE, {e})")
    con.execute(f"CREATE TABLE ratios_cache(symbol VARCHAR, debtToEquityRatio DOUBLE, {e})")
    con.execute(f"CREATE TABLE metrics_cache(symbol VARCHAR, marketCap DOUBLE, {e})")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    for s, c in companies.items():
        for d in c["days"]:
            if c["inc"][d]:
                con.execute("INSERT INTO income_cache VALUES (?, ?, ?, ?, ?, 'FY')", [s, *c["inc"][d], ep(d)])
            if c["bal"][d]:
                con.execute("INSERT INTO balance_cache VALUES (?, ?, ?, ?, ?, ?, ?, 'FY')", [s, *c["bal"][d], ep(d)])
            con.execute("INSERT INTO cashflow_cache VALUES (?, ?, ?, 'FY')", [s, c.get("ocf", {}).get(d, OCF), ep(d)])
            con.execute("INSERT INTO ratios_cache VALUES (?, ?, ?, 'FY')", [s, c["de"].get(d, 0.2), ep(d)])
            con.execute("INSERT INTO metrics_cache VALUES (?, ?, ?, 'FY')", [s, c["mcap"], ep(d)])
        if c["px"]:
            con.execute("INSERT INTO prices_cache VALUES (?, ?, ?, 1e6)", [s, ep(D + timedelta(days=1)), c["px"]])
        con.execute("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", [s, c["isin"]])
    return con


# Sweep: one company per fiscal calendar (12 month-ends and the old window's four edge days),
# FY2009-FY2021 rows that improve every year, screened at every July rebalance 2012-2021.
SWEEP_YEARS = range(2012, 2022)
CALENDARS = {calendar.month_abbr[m].upper(): (m, calendar.monthrange(2021, m)[1]) for m in range(1, 13)}
CALENDARS.update({"MAY17": (5, 17), "MAY18": (5, 18), "NOV13": (11, 13), "NOV14": (11, 14)})


def improving(k):
    """(income, balance, operatingCashFlow) for year k: all nine F-score terms beat year k-1 and k-2."""
    ta = 1000.0 + 50 * k
    rev = ta * (0.90 + 0.02 * k)
    ni = ta * (0.08 + 0.005 * k)
    return ((ni, rev * (0.30 + 0.01 * k), rev),
            (ta, 400.0 + 40 * k, 200.0, ta * (0.10 - 0.005 * k), 500.0 + 20 * k), ni * 1.5)


def sweep_companies():
    out = {}
    for name, (m, dd) in CALENDARS.items():
        out[name] = {date(y, m, dd): improving(y - 2009) for y in range(2009, 2022)}
    # every second year missing: never two consecutive years on file
    out["GAPPY_DEC"] = {d: v for d, v in out["DEC"].items() if d.year % 2 == 0}
    out["GAPPY_JUN"] = {d: v for d, v in out["JUN"].items() if d.year % 2 == 0}
    return {s: {"days": tuple(rows), "inc": {d: v[0] for d, v in rows.items()},
                "bal": {d: v[1] for d, v in rows.items()}, "ocf": {d: v[2] for d, v in rows.items()},
                "de": {}, "mcap": 5e9, "px": None, "isin": None}
            for s, rows in out.items()}


def sweep(mod):
    con, out = db(sweep_companies()), []
    for y in SWEEP_YEARS:
        got = {r[0] for r in mod.screen_stocks(con, date(y, 7, 1), FLOOR, offset_days=1)}
        if got != set(CALENDARS):
            out.append(f"{y}: missing {sorted(set(CALENDARS) - got)}, extra {sorted(got - set(CALENDARS))}")
    return out


def failures(src, epoch_types=("UINTEGER",)):
    try:
        mod = load(src)
        out = []
        for t in epoch_types:
            got = [(r[0], r[1]) for r in mod.screen_stocks(db(COMPANIES, t), D, FLOOR, offset_days=1)]
            if got != EXPECT:
                names, want = [r[0] for r in got], [r[0] for r in EXPECT]
                out.append(f"book ({t}): missing {sorted(set(want) - set(names))},"
                           f" extra {sorted(set(names) - set(want))}" if set(names) != set(want)
                           else f"book ({t}): order {names} != {want}")
        return out + sweep(mod)
    except Exception as e:  # reported as an error: main() does not count it as a caught mutant
        return [f"error: {type(e).__name__}: {e}"]


INC_JOIN = "            JOIN (SELECT symbol AS sym, filing_epoch AS curr_epoch FROM inc_curr WHERE rn = 1) c ON c.sym = symbol\n"
BAL_JOIN = "            JOIN (SELECT symbol AS sym, filing_epoch AS curr_epoch FROM bal_curr WHERE rn = 1) c ON c.sym = symbol\n"
GAP_WHERE = "            WHERE filing_epoch <= curr_epoch - ? AND filing_epoch >= curr_epoch - ?\n"
NO_MAX = "            WHERE filing_epoch <= curr_epoch - ? AND ? IS NOT NULL\n"
NO_MIN = "            WHERE filing_epoch < curr_epoch AND ? IS NOT NULL AND filing_epoch >= curr_epoch - ?\n"
INC_ARGS = "        prior_gap_min, prior_gap_max,          # inc_prev\n"
BAL_ARGS = "        prior_gap_min, prior_gap_max,            # bal_prev\n"
# the old bug: the prior row taken from a window off the rebalance date
DATE_WHERE = "            WHERE filing_epoch <= ? AND filing_epoch > ?\n"
DATE_ARGS = "        utc_epoch(target_date - timedelta(days=410)), stale_cutoff_epoch,\n"
WIDE_ARGS = "        utc_epoch(target_date - timedelta(days=410)), utc_epoch(target_date - timedelta(days=5 * 365)),\n"
INC_LATEST = ("ORDER BY filing_epoch DESC,\n"
              "                    netIncome DESC NULLS LAST, grossProfit DESC NULLS LAST, revenue DESC NULLS LAST) AS rn\n"
              "            FROM income_cache\n" + INC_JOIN)
BAL_LATEST = ("ORDER BY filing_epoch DESC,\n"
              "                    totalAssets DESC NULLS LAST, totalCurrentAssets DESC NULLS LAST,\n"
              "                    totalCurrentLiabilities DESC NULLS LAST, longTermDebt DESC NULLS LAST,\n"
              "                    totalStockholdersEquity DESC NULLS LAST) AS rn\n"
              "            FROM balance_cache\n" + BAL_JOIN)
# name: [(old, new), ...]; every `old` must occur exactly once in the source
MUTANTS = {
    "old date window (income)": [(INC_JOIN + GAP_WHERE, DATE_WHERE), (INC_ARGS, DATE_ARGS)],
    "old date window (balance)": [(BAL_JOIN + GAP_WHERE, DATE_WHERE), (BAL_ARGS, DATE_ARGS)],
    "old date window (both)": [(INC_JOIN + GAP_WHERE, DATE_WHERE), (INC_ARGS, DATE_ARGS),
                               (BAL_JOIN + GAP_WHERE, DATE_WHERE), (BAL_ARGS, DATE_ARGS)],
    # the old comment's "5 years max lookback": every calendar is screened, and so is a skipped year
    "date window, 5-year lookback": [(INC_JOIN + GAP_WHERE, DATE_WHERE), (INC_ARGS, WIDE_ARGS),
                                     (BAL_JOIN + GAP_WHERE, DATE_WHERE), (BAL_ARGS, WIDE_ARGS)],
    "no upper gap bound (income)": [(INC_JOIN + GAP_WHERE, INC_JOIN + NO_MAX)],
    "no upper gap bound (balance)": [(BAL_JOIN + GAP_WHERE, BAL_JOIN + NO_MAX)],
    "no lower gap bound (income)": [(INC_JOIN + GAP_WHERE, INC_JOIN + NO_MIN)],
    "no lower gap bound (balance)": [(BAL_JOIN + GAP_WHERE, BAL_JOIN + NO_MIN)],
    "earliest row in the window (income)": [(INC_LATEST, INC_LATEST.replace("filing_epoch DESC", "filing_epoch ASC"))],
    "earliest row in the window (balance)": [(BAL_LATEST, BAL_LATEST.replace("filing_epoch DESC", "filing_epoch ASC"))],
    "gap bounds swapped": [(INC_ARGS, "        prior_gap_max, prior_gap_min,\n")],
    "lower gap bound 301 days": [("PRIOR_FY_MIN_DAYS = 300 ", "PRIOR_FY_MIN_DAYS = 301 ")],
    "upper gap bound 429 days": [("PRIOR_FY_MAX_DAYS = 430 ", "PRIOR_FY_MAX_DAYS = 429 ")],
    "upper gap bound admits a skipped year": [("PRIOR_FY_MAX_DAYS = 430 ", "PRIOR_FY_MAX_DAYS = 800 ")],
    "no maximum age for current rows": [("stale_cutoff_epoch = utc_epoch(target_date - timedelta(days=45 + 550))",
                                         "stale_cutoff_epoch = 0")],
    "no lag": [("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))", "cutoff_epoch = utc_epoch(target_date)")],
    "same-ISIN dedup removed": [("FROM ({isin_dedup_sql(candidates, target_date, offset_days, MIN_ENTRY_PRICE)})",
                                 "FROM ({candidates})")],
    "no D/E ceiling": [("          AND debtToEquityRatio < ?\n", "          AND ? IS NOT NULL\n")],
    "no size floor": [("              AND met.marketCap > ?\n", "              AND ? IS NOT NULL\n")],
    "no F-score floor": [("+ f7_no_dilution + f8_turnover + f9_margin) >= ?",
                          "+ f7_no_dilution + f8_turnover + f9_margin) >= 0 AND ? IS NOT NULL")],
}


def main():
    base = failures(SRC, epoch_types=("UINTEGER", "BIGINT"))
    if base:
        print("test_low_debt_selection: FAIL", base)
        sys.exit(1)
    survived, broken = [], []
    for name, edits in MUTANTS.items():
        src = SRC
        for old, new in edits:
            if SRC.count(old) != 1:
                print(f"test_low_debt_selection: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            src = src.replace(old, new)
        got = failures(src)
        if not got:
            survived.append(name)
        elif any(f.startswith("error:") for f in got):
            broken.append((name, got))
    if survived or broken:
        print("test_low_debt_selection: FAIL (mutants survived:", survived, "; mutants that raised:", broken, ")")
        sys.exit(1)
    print(f"test_low_debt_selection: PASS ({len(COMPANIES)} companies, {len(CALENDARS)} fiscal calendars"
          f" x {len(SWEEP_YEARS)} rebalances, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
