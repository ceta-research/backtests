#!/usr/bin/env python3
"""Offline check that dividend-growth's streak ENDS at the last complete year (no network).

The streak is the number of consecutive calendar years, ending at the year before the rebalance,
in which the year's adjDividend sum (by ex-date) rose over the prior year's. A year with no
dividend, a cut or a flat year breaks it, and the first paid year after a gap is not an increase.
A July 2021 rebalance reads 2020 as the last complete year; 2021's dividends are not read at all.
Counting the increase-years after the last recorded decrease let a company that stopped paying
keep its streak for good and a skipped year count as an increase: 13 of Sweden's top 30 in
July 2021 had paid nothing in 2020. Every mutant of the topic's source must trip a check.

Usage:
    python scripts/test_dividend_growth_selection.py [repo root]
"""
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "dividend-growth", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D21, D20 = date(2021, 7, 1), date(2020, 7, 1)   # last complete years: 2020 and 2019
FLOOR = 1e9
CUT = 5                                         # a top-N small enough to bite on the made-up book


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("dg_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def up(first, last, skip=()):
    """{year: dividend}, one a year from `first` to `last`, each above the one before; `skip` years pay nothing."""
    return {y: 1.0 + 0.1 * (y - first) for y in range(first, last + 1) if y not in skip}


OK = (0.40, 100.0)      # dividendPayoutRatio, freeCashFlow
# symbol: ({year: dividend}, marketCap, payout, FCF). Streaks are as of year-end 2020 / 2019.
COMPANIES = {
    "CLEAN": (up(2009, 2021), 5e9, *OK),                            # 11 / 10; 2021's rise is not read
    "FUTURE_CUT": ({**up(2009, 2020), 2021: 0.2}, 4e9, *OK),        # 11 / 10; the cut comes after both year-ends
    "RESTART": (up(2000, 2020, skip=(2010,)), 9e9, *OK),            # 9 / 8: 2011 follows a gap, the run starts 2012
    "TWIN_A": (up(2012, 2020), 3e9, *OK),                           # 8 / 7; one ISIN with TWIN_B
    "TWIN_B": (up(2012, 2020), 3.5e9, *OK),
    "TIE_A": (up(2013, 2020), 2e9, *OK),                            # 7 / 6; same size, symbol decides
    "TIE_B": (up(2013, 2020), 2e9, *OK),
    "CUT_2015": ({**up(2008, 2014), **{y: 0.1 * (y - 2010) for y in range(2015, 2021)}}, 2.5e9, *OK),   # 5 / 4
    "EDGE": (up(2015, 2020), 1.5e9, *OK),                           # 5 / 4: first paid 2015
    "STOPPED_2020": (up(2009, 2019), 8e9, *OK),                     # 0 / 10: nothing in 2020 (the Sweden shape)
    "STOPPED_LONG": (up(2005, 2015), 8e9, *OK),                     # 0 / 0: silent since 2015
    "GAP_SKIP": (up(2010, 2020, skip=(2018,)), 8e9, *OK),           # 1 / 0: 2019 beats 2017 but follows a gap
    "GAP_4": (up(2005, 2020, skip=(2015,)), 8e9, *OK),              # 4 / 3: the run restarts after the gap
    "CUT_2018": ({**up(2008, 2017), 2018: 0.5, 2019: 0.6, 2020: 0.7}, 8e9, *OK),   # 2 / 1
    "FLAT_2020": ({**up(2009, 2019), 2020: up(2009, 2019)[2019]}, 7e9, *OK),       # 0 / 10
    "CUT_2020": ({**up(2009, 2019), 2020: 0.3}, 6e9, *OK),          # 0 / 10
    "INCOMPLETE": (up(2016, 2021), 8e9, *OK),                       # 4 / 3: the fifth rise is in 2021
    "HIGH_PAYOUT": (up(2009, 2020), 8e9, 0.95, 100.0),              # 11, payout over the cap
    "NEG_FCF": (up(2009, 2020), 8e9, 0.40, -5.0),                   # 11, negative FCF
    "SMALL": (up(2009, 2020), 5e8, *OK),                            # 11, under the size floor
}
# streak DESC, marketCap DESC, symbol; one line per ISIN
BOOK_2021 = [("CLEAN", 5e9), ("FUTURE_CUT", 4e9), ("RESTART", 9e9), ("TWIN_A", 3e9),
             ("TIE_A", 2e9), ("TIE_B", 2e9), ("CUT_2015", 2.5e9), ("EDGE", 1.5e9)]
# a year earlier the 2020 stoppers are still in: 2020 was not complete in July 2020
BOOK_2020 = [("STOPPED_2020", 8e9), ("FLAT_2020", 7e9), ("CUT_2020", 6e9), ("CLEAN", 5e9), ("FUTURE_CUT", 4e9),
             ("RESTART", 9e9), ("TWIN_A", 3e9), ("TIE_A", 2e9), ("TIE_B", 2e9)]


def tables():
    """{cache table: [columns, rows]} of what the topic's fetch loads."""
    fin = "filing_epoch BIGINT, period VARCHAR"
    t = {"div_calendar": ["symbol VARCHAR, date VARCHAR, adjDividend DOUBLE", []],
         "ratios_cache": [f"symbol VARCHAR, dividendPayoutRatio DOUBLE, {fin}", []],
         "metrics_cache": [f"symbol VARCHAR, marketCap DOUBLE, {fin}", []],
         "cashflow_cache": [f"symbol VARCHAR, freeCashFlow DOUBLE, {fin}", []],
         "prices_cache": ["symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, volume DOUBLE", []],
         "isin_map": ["symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN", []]}
    for s, (divs, mcap, payout, fcf) in COMPANIES.items():
        t["div_calendar"][1] += [(s, f"{y}-05-15", v) for y, v in divs.items()]
        for fy in (2018, 2019, 2020):
            e = ep(date(fy, 12, 31))
            t["ratios_cache"][1].append((s, payout, e, "FY"))
            t["metrics_cache"][1].append((s, mcap, e, "FY"))
            t["cashflow_cache"][1].append((s, fcf, e, "FY"))
        t["prices_cache"][1] += [(s, ep(d + timedelta(days=1)), 50.0, 1e6) for d in (D20, D21)]
        t["isin_map"][1].append((s, "XX000TWIN" if s.startswith("TWIN") else f"XX000{s}", True, True))
    return t


class _Client:
    """Stands in for CetaResearch in fetch_data_via_api: the profile rows, nothing else."""

    def query(self, sql, **kw):
        return [{"symbol": s, "exchange": "NYSE"} for s in COMPANIES]


def fetch(mod):
    """The topic's own fetch_data_via_api over the made-up rows: div_annual and div_yoy are built by its SQL."""
    t = tables()

    def fill(con, name):
        cols, rows = t[name]
        con.execute(f"CREATE TABLE {name}({cols})")
        con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(rows[0]))})", rows)
        return len(rows)

    mod.query_parquet = lambda client, sql, con, table_name, **kw: fill(con, table_name)
    mod.load_isin_map = lambda client, con, *a, **kw: fill(con, "isin_map")
    mod.remove_price_oscillations = lambda *a, **k: None
    mod.print = lambda *a, **k: None
    return mod.fetch_data_via_api(_Client(), ["NYSE"], [D20, D21])


def failures(src):
    try:
        mod = load(src)
        con = fetch(mod)
        got21 = mod.screen_stocks(con, D21, FLOOR, offset_days=1)
        got20 = mod.screen_stocks(con, D20, FLOOR, offset_days=1)
        mod.MAX_STOCKS = CUT
        cut21 = mod.screen_stocks(con, D21, FLOOR, offset_days=1)
    except Exception as e:  # a mutant that breaks the module fails loudly
        return [f"error: {type(e).__name__}: {e}"]
    out = []
    for label, got, want in (("July 2021 book", got21, BOOK_2021), ("July 2020 book", got20, BOOK_2020),
                             (f"July 2021 top {CUT}", cut21, BOOK_2021[:CUT])):
        if got != want:
            out.append(f"{label} {[s for s, _ in got]} != {[s for s, _ in want]}")
    return out


HAVING = "HAVING COUNT(*) >= ? AND MAX(d.yr) = ?"
ORDER = "ORDER BY consecutive_years DESC, marketCap DESC, symbol"
MAX_YEAR = "max_year = target_date.year - 1"
MUTANTS = {
    # the old bug, half one: LAG over paid years only, so a skipped year reads as an increase
    "gap year counted as an increase": (
        "                  AND LAG(yr) OVER (PARTITION BY symbol ORDER BY yr) = yr - 1\n", ""),
    "year after a gap dropped, not a break": ('"DELETE FROM div_yoy WHERE prev_div IS NULL"',
                                              '"DELETE FROM div_yoy WHERE prev_div IS NULL'
                                              ' OR (increased = 0 AND total_div > prev_div)"'),
    # the old bug, half two: the run need not end at the last complete year
    "streak not anchored": (HAVING, "HAVING COUNT(*) >= ? AND ? IS NOT NULL"),
    "anchored a year early": (HAVING, "HAVING COUNT(*) >= ? AND MAX(d.yr) >= ? - 1"),
    "reads the rebalance year": (MAX_YEAR, "max_year = target_date.year"),
    "last complete year a year stale": (MAX_YEAR, "max_year = target_date.year - 2"),
    "a later cut hides the streak": ("WHERE increased = 0 AND yr <= ?", "WHERE increased = 0 AND ? IS NOT NULL"),
    "later increases counted": ("              AND d.yr <= ?\n", "              AND ? IS NOT NULL\n"),
    # threshold, ranking, tie-break and the wiring around them stay as they were
    "flat year counted as an increase": ("CASE WHEN total_div > LAG(total_div)",
                                         "CASE WHEN total_div >= LAG(total_div)"),
    "no minimum streak": (HAVING, "HAVING ? IS NOT NULL AND MAX(d.yr) = ?"),
    "minimum streak exclusive": (HAVING, "HAVING COUNT(*) > ? AND MAX(d.yr) = ?"),
    "floor and anchor parameters swapped": ("[max_year, max_year, MIN_STREAK, max_year,",
                                            "[max_year, max_year, max_year, MIN_STREAK,"),
    "ranked by size first": (ORDER, "ORDER BY marketCap DESC, consecutive_years DESC, symbol"),
    "shortest streak first": (ORDER, "ORDER BY consecutive_years, marketCap DESC, symbol"),
    "tie-break reversed": (ORDER, ORDER + " DESC"),
    "no same-security dedup": ("FROM ({isin_dedup_sql(candidates, target_date, offset_days, MIN_ENTRY_PRICE)})",
                               "FROM ({candidates})"),
    "cut before the dedup": ('          AND km.marketCap > ?\n    """',
                             "          AND km.marketCap > ?\n        ORDER BY s.consecutive_years DESC,"
                             f' km.marketCap DESC, s.symbol LIMIT {CUT}\n    """'),
    "no payout cap": ("AND fr.dividendPayoutRatio <= ?", "AND ? IS NOT NULL"),
    "no FCF test": ("AND cf.freeCashFlow > ?", "AND ? IS NOT NULL"),
    "no size floor": ("AND km.marketCap > ?", "AND ? IS NOT NULL"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_dividend_growth_selection: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_dividend_growth_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new)):
            survived.append(name)
    if survived:
        print("test_dividend_growth_selection: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_dividend_growth_selection: PASS ({len(COMPANIES)} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
