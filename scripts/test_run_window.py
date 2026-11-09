#!/usr/bin/env python3
"""The run window: a run ends on the latest close (data_utils.RunWindow, METHODOLOGY.md "Run window").

  1 FINDER   a leg's good day has a benchmark bar, and every exchange of the leg traded (volume > 0)
             at least half its median daily line count over the last 21 days: a half-loaded day, a
             holiday of zero-volume rows, a day without the benchmark and a day one exchange of the leg
             missed don't count. data_end is a leg's latest good day; end_date is the latest day that
             is a good day for every leg; a spread over 5 days warns and names the late legs; a leg with
             no good day fails; no ';' reaches the API; two queries for the whole run
  2 PIN      --end-date pins the date: it must be a good day for every leg, and the data_end keys are null
  3 GRID     the grid stops at the last grid date whose execution day (date + offset) is on or before
             end_date; that date is the last rebalance when end_date is 7 days or more after it, and
             the final exit otherwise; annual, semi-annual and quarterly grids; --no-next-day
  4 PRICING  the partial exit is priced at the close on end_date (offset 0), every other exit as
             today; fetch windows stop at end_date
  5 LEGACY   --end-year builds today's grid, prices every exit as today and adds no key
  6 ROWS     mark() flags every row and gives the partial one its fraction, from the execution day;
             fractions() refuses an unmarked row; fields() writes the leg's keys, and an unmeasured
             partial period writes no partial_period
  7 MUTANTS  each rule above, broken in a copy of data_utils.py, fails a check

Usage:  python scripts/test_run_window.py
"""
import contextlib
import importlib.util
import io
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FAIL = []
TODAY = date(2026, 11, 9)   # a Monday
START = TODAY - timedelta(days=40)


def check(cond, what):
    if not cond:
        FAIL.append(what)
        print(f"  FAIL {what}")
    return cond


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def weekdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


class FakeClient:
    """Runs the SQL on an in-memory warehouse, as the API would; refuses a ';'."""

    def __init__(self, src):
        self.src, self.sqls = src, []

    def query(self, sql, format="json", **_kw):
        self.sqls.append(sql)
        if ";" in sql:
            raise AssertionError("a ';' in SQL sent to the API")
        cur = self.src.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


US = (["NYSE", "NASDAQ"], "SPY")
IN = (["NSE"], "^NSEI")


_WAREHOUSES = {}


def warehouse(**kw):
    """One read-only warehouse per spec (the fake client only reads), shared across checks and mutants."""
    key = repr(sorted(kw.items()))
    if key not in _WAREHOUSES:
        _WAREHOUSES[key] = build_warehouse(**kw)
    return _WAREHOUSES[key]


def build_warehouse(us_last=date(2026, 11, 5), us_thin=date(2026, 11, 6), spy_last=date(2026, 11, 6),
                    in_last=date(2026, 11, 6), nsei_last=date(2026, 11, 4), in_closed=(), us_holiday=(),
                    nyse_out=()):
    """US: 40 lines (20 NYSE, 20 NASDAQ) trading on weekdays to us_last, 10 more on us_thin (half-loaded).
    IN: 30 lines to in_last, none on in_closed. us_holiday: every US line has a row with no volume.
    nyse_out: days the NYSE lines miss (NASDAQ trades). Benchmarks end on their own dates."""
    src = duckdb.connect()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch BIGINT, adjClose DOUBLE,"
                " close DOUBLE, volume DOUBLE)")
    rows, prof = [], []
    for i in range(40):
        s, ex = f"U{i:02d}", ("NYSE" if i % 2 else "NASDAQ")
        prof.append((s, ex))
        for d in weekdays(START, us_last):
            if ex == "NYSE" and d in nyse_out:
                continue
            rows.append((s, d, 0.0 if d in us_holiday else 1000.0))
        if i < 10 and us_thin:
            rows.append((s, us_thin, 1000.0))
    for i in range(30):
        s = f"I{i:02d}.NS"
        prof.append((s, "NSE"))
        rows += [(s, d, 1000.0) for d in weekdays(START, in_last) if d not in in_closed]
    rows += [("SPY", d, 1e6) for d in weekdays(START, spy_last)]
    rows += [("^NSEI", d, 0.0) for d in weekdays(START, nsei_last) if d not in in_closed]   # an index: no volume
    prof += [("SPY", "AMEX"), ("^NSEI", "INDEX")]
    src.executemany("INSERT INTO profile VALUES (?, ?)", prof)
    src.executemany("INSERT INTO stock_eod VALUES (?, ?, ?, 100.0, 100.0, ?)",
                    [(s, d.isoformat(), ep(d), v) for s, d, v in rows])
    return src


def quiet(fn, *a, **kw):
    err = io.StringIO()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
        out = fn(*a, **kw)
    return out, err.getvalue()


# ---------------------------------------------------------------- 1 finder

def finder_failures(du):
    f = []

    def latest(legs=None, **kw):
        client = FakeClient(warehouse(**kw))
        win, err = quiet(du.RunWindow.latest, client, legs or {"US": US, "IN": IN}, today=TODAY)
        return win, err, client

    win, err, client = latest()
    want = {"US": "2026-11-05", "IN": "2026-11-04"}   # US's 11-06 is a quarter loaded; ^NSEI stops on 11-04
    if win.data_end_by_market != want:
        f.append(f"data_end_by_market {win.data_end_by_market}, want {want}")
    if win.end_date != date(2026, 11, 4):
        f.append(f"end_date {win.end_date}, want 2026-11-04")
    if win.requested != "latest" or not win.checked_at:
        f.append(f"requested {win.requested!r}, checked_at {win.checked_at!r}")
    if "WARNING" in err:
        f.append(f"a 1-day spread warned: {err.strip()}")
    if len(client.sqls) != 2:
        f.append(f"{len(client.sqls)} queries for the whole run, want 2")
    # a holiday of zero-volume rows with a benchmark bar is not a good day
    win, _, _ = latest(us_holiday={date(2026, 11, 5)}, us_thin=None)
    if win.data_end_by_market["US"] != "2026-11-04":
        f.append(f"a zero-volume holiday counted: US data_end {win.data_end_by_market['US']}")
    # one exchange of the leg missing the day: NASDAQ alone traded half the leg's lines on 11-05
    win, _, _ = latest(nyse_out={date(2026, 11, 5)}, us_thin=None)
    if win.data_end_by_market["US"] != "2026-11-04":
        f.append(f"a day NYSE missed counted for the US leg: {win.data_end_by_market['US']}")
    # every leg must have traded on end_date: IN closed on US's latest close, with later bars
    win, _, _ = latest(in_closed={date(2026, 11, 5)}, nsei_last=date(2026, 11, 6))
    if (win.end_date, win.data_end_by_market) != (date(2026, 11, 4), {"US": "2026-11-05", "IN": "2026-11-06"}):
        f.append(f"a day IN was closed: end_date {win.end_date}, data_end {win.data_end_by_market}")
    # a lagging index: 6 days behind the latest leg warns and names it
    win, err, _ = latest(nsei_last=date(2026, 10, 30))
    if win.end_date != date(2026, 10, 30):
        f.append(f"lagging index: end_date {win.end_date}, want 2026-10-30")
    if "WARNING" not in err or "IN (2026-10-30)" not in err:
        f.append(f"a 6-day spread did not warn naming IN: {err.strip()!r}")
    # a leg with no good day at all fails, naming it
    try:
        latest(nsei_last=date(2026, 9, 1))
        f.append("a leg whose benchmark has no bar in 21 days did not fail")
    except ValueError as e:
        if "IN" not in str(e):
            f.append(f"the failure doesn't name the leg: {e}")
    # the run day itself never counts: a market still trading has a bar for it before its close
    win, _, _ = latest({"US": US}, us_last=TODAY, spy_last=TODAY, us_thin=None)
    if win.data_end_by_market["US"] != "2026-11-06":
        f.append(f"the run day counted: US data_end {win.data_end_by_market['US']}, want Friday 2026-11-06")
    # one leg alone
    win, _, _ = latest({"US": US})
    if win.end_date != date(2026, 11, 5) or win.data_end_by_market != {"US": "2026-11-05"}:
        f.append(f"single leg: {win.end_date} {win.data_end_by_market}")
    if win.for_leg("US").data_end != "2026-11-05":
        f.append("for_leg does not carry the leg's data_end")
    return f


# ---------------------------------------------------------------- 2 pin

def pin_failures(du):
    f = []
    client = FakeClient(warehouse())
    win, _ = quiet(du.RunWindow.pinned, client, {"US": US, "IN": IN}, date(2026, 11, 3), today=TODAY)
    if win.end_date != date(2026, 11, 3) or win.requested != "2026-11-03":
        f.append(f"pinned: end_date {win.end_date}, requested {win.requested!r}")
    if (win.data_end_by_market, win.checked_at, win.for_leg("US").data_end) != (None, None, None):
        f.append("pinned: the data_end keys are not null")
    for d, why, kw in ((date(2026, 11, 7), "a Saturday", {}),
                       (date(2026, 11, 6), "a day ^NSEI has no bar", {}),
                       (date(2026, 11, 3), "a zero-volume US holiday", {"us_holiday": {date(2026, 11, 3)}}),
                       (TODAY, "the run day", {"us_last": TODAY, "spy_last": TODAY, "in_last": TODAY,
                                               "nsei_last": TODAY, "us_thin": None})):
        try:
            quiet(du.RunWindow.pinned, FakeClient(warehouse(**kw)), {"US": US, "IN": IN}, d, today=TODAY)
            f.append(f"a pin on {why} did not fail")
        except ValueError:
            pass
    return f


# ---------------------------------------------------------------- 3 grid

def at(du, end):
    return du.RunWindow(end_date=end, requested=end.isoformat())


def grid_failures(du):
    f = []
    g = du.generate_rebalance_dates
    cases = [
        # name, end_date, frequency, months, offset, want tail, want partial exit
        ("annual to 2026-11-06", date(2026, 11, 6), "annual", [7], 1,
         [date(2025, 7, 1), date(2026, 7, 1), date(2026, 11, 6)], True),
        ("annual, end 4 days after the grid date", date(2026, 7, 5), "annual", [7], 1,
         [date(2025, 7, 1), date(2026, 7, 1)], False),
        ("annual, end 7 days after the grid date", date(2026, 7, 8), "annual", [7], 1,
         [date(2026, 7, 1), date(2026, 7, 8)], True),
        ("annual, end on the grid date with next-day execution", date(2026, 7, 1), "annual", [7], 1,
         [date(2024, 7, 1), date(2025, 7, 1), date(2026, 7, 1)], True),
        ("annual, end on the grid date with same-day execution", date(2026, 7, 1), "annual", [7], 0,
         [date(2025, 7, 1), date(2026, 7, 1)], False),
        ("quarterly to 2026-11-06", date(2026, 11, 6), "quarterly", None, 1,
         [date(2026, 7, 1), date(2026, 10, 1), date(2026, 11, 6)], True),
        ("semi-annual to 2026-03-13", date(2026, 3, 13), "semi-annual", None, 1,
         [date(2025, 7, 1), date(2026, 1, 1), date(2026, 3, 13)], True),
    ]
    for name, end, freq, months, off, tail, partial in cases:
        win = at(du, end)
        dates = win.grid(2000, freq, months=months, offset_days=off)
        if dates[-len(tail):] != tail:
            f.append(f"{name}: grid ends {dates[-len(tail):]}, want {tail}")
        start = g(2000, 2000, freq, months)
        if dates[:len(start)] != start:
            f.append(f"{name}: grid starts {dates[:len(start)]}, want {start}")
        if sorted(set(dates)) != dates:
            f.append(f"{name}: grid not strictly increasing")
        if win.partial_exit != (end if partial else None):
            f.append(f"{name}: partial_exit {win.partial_exit}, want {end if partial else None}")
    return f


# ---------------------------------------------------------------- 4 pricing

def pricing_failures(du):
    f = []
    win = at(du, date(2026, 11, 6))
    if win.exit_offset(date(2026, 11, 6), 1) != 0:
        f.append("the partial exit is not priced at the close on end_date")
    if win.exit_offset(date(2026, 7, 1), 1) != 1 or win.exit_offset(date(2026, 7, 1), 0) != 0:
        f.append("a grid exit's offset changed")
    if win.fetch_end(date(2026, 11, 6), 11) != date(2026, 11, 6):
        f.append("the end date's fetch window runs past end_date")
    if win.fetch_end(date(2026, 10, 30), 11) != date(2026, 11, 6):
        f.append("a fetch window that crosses end_date isn't cut at it")
    if win.fetch_end(date(2025, 7, 1), 11) != date(2025, 7, 12):
        f.append("an earlier fetch window changed")
    # get_prices at the end date: the close on that day, no later bar even when one is cached
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)",
                    [("A", ep(date(2026, 11, 5)), 9.0), ("A", ep(date(2026, 11, 6)), 10.0),
                     ("A", ep(date(2026, 11, 9)), 11.0), ("SPY", ep(date(2026, 7, 2)), 100.0),
                     ("SPY", ep(date(2026, 11, 6)), 105.0)])
    px = du.get_prices(con, ["A"], date(2026, 11, 6), offset_days=win.exit_offset(date(2026, 11, 6), 1))
    if px != {"A": 10.0}:
        f.append(f"exit price at end_date {px}, want the close on that day")
    r = du.get_benchmark_return(con, "SPY", date(2026, 7, 1), date(2026, 11, 6), offset_days=1,
                                exit_offset_days=win.exit_offset(date(2026, 11, 6), 1))
    if r is None or abs(r - 0.05) > 1e-12:
        f.append(f"benchmark return to end_date {r}, want 0.05")
    if du.get_benchmark_return(con, "SPY", date(2026, 7, 1), date(2026, 11, 6), offset_days=1) is not None:
        f.append("get_benchmark_return without exit_offset_days changed (want today's: no bar after 11-07)")
    return f


# ---------------------------------------------------------------- 5 legacy

def legacy_failures(du):
    f = []
    for freq, months in (("annual", [7]), ("quarterly", [1, 4, 7, 10]), ("semi-annual", None)):
        win = du.RunWindow.typed(2025)
        dates = win.grid(2000, freq, months=months, offset_days=1)
        if dates != du.generate_rebalance_dates(2000, 2025, freq, months):
            f.append(f"legacy {freq} grid differs from generate_rebalance_dates(2000, 2025)")
        if win.partial_exit is not None or win.exit_offset(dates[-1], 1) != 1:
            f.append(f"legacy {freq}: a partial exit or a changed offset")
        if win.fetch_end(date(2025, 7, 1), 11) != date(2025, 7, 12):
            f.append(f"legacy {freq}: fetch windows changed")
        rows = [{"rebalance_date": a.isoformat(), "exit_date": b.isoformat()} for a, b in zip(dates, dates[1:])]
        if win.mark(rows) != [{"rebalance_date": a.isoformat(), "exit_date": b.isoformat()}
                              for a, b in zip(dates, dates[1:])] or win.fields(rows, 1, None) != {} \
                or win.fractions(rows) is not None:
            f.append(f"legacy {freq}: adds keys")
    return f


# ---------------------------------------------------------------- 6 rows

def rows_failures(du):
    f = []
    win = at(du, date(2026, 11, 6))
    try:
        win.fields([], 1, None)
        f.append("fields() before grid() did not fail")
    except RuntimeError:
        pass
    dates = win.grid(2000, "annual", months=[7], offset_days=1)
    rows = win.mark([{"rebalance_date": a.isoformat(), "exit_date": b.isoformat()} for a, b in zip(dates, dates[1:])])
    # held from the execution day 2026-07-02 to 2026-11-06: 127 days of the 365 to 2027-07-01
    if rows[-1].get("partial") is not True or rows[-1].get("fraction") != 0.347945:
        f.append(f"partial row {rows[-1]}")
    if any(r.get("partial") is not False or "fraction" in r for r in rows[:-1]):
        f.append("a full row is not marked partial: false, or carries a fraction")
    if win.fractions(rows) != [1.0] * 26 + [0.347945]:
        f.append(f"fractions {win.fractions(rows)[-3:]}")
    try:
        win.fractions(rows + [{"rebalance_date": "x", "exit_date": "y"}])
        f.append("fractions() took an unmarked row")
    except ValueError:
        pass
    py = {"year": 2026, "start": "2026-07-01", "end": "2026-11-06", "periods": 1, "year_fraction": 0.347945,
          "portfolio": 0.032149, "benchmark": 0.051, "excess": 0.032149 - 0.051}
    got = win.fields(rows, 1, py)
    want = {"end_date_requested": "2026-11-06", "end_date": "2026-11-06", "data_end": None,
            "data_end_by_market": None, "data_end_checked_at": None, "full_periods": 26,
            "span_years": 26.347945,
            "partial_period": {"rebalance_date": "2026-07-01", "entry_day": "2026-07-02",
                               "exit_date": "2026-11-06", "days": 127, "period_days": 365, "fraction": 0.347945},
            "partial_year": {"year": 2026, "start": "2026-07-01", "end": "2026-11-06", "periods": 1,
                             "year_fraction": 0.347945, "portfolio": 3.21, "spy": 5.1, "excess": -1.89}}
    if got != want:
        f.append("fields " + str({k: (got.get(k), want[k]) for k in want if got.get(k) != want[k]}))
    # the partial period unmeasured (its benchmark can't price it): no partial_period, every measured period full
    got = win.fields(rows[:-1], 1, None)
    if (got["full_periods"], got["span_years"], got["partial_year"], got["partial_period"]) != (26, 26.0, None, None):
        f.append(f"unmeasured partial period: {got['full_periods']} {got['span_years']} {got['partial_period']}")
    # no partial period: every row full
    win = at(du, date(2026, 7, 5))
    dates = win.grid(2000, "annual", months=[7], offset_days=1)
    rows = win.mark([{"rebalance_date": a.isoformat(), "exit_date": b.isoformat()} for a, b in zip(dates, dates[1:])])
    got = win.fields(rows, 1, None)
    if got["partial_period"] is not None or got["full_periods"] != 26 or any(r["partial"] for r in rows):
        f.append(f"stub under 7 days: {got['partial_period']} {got['full_periods']}")
    return f


# ---------------------------------------------------------------- 7 mutants

MUTANTS = [
    # (name, old text, new text) in data_utils.py: each must make a check above fail
    ("rows counted, not traded lines", "COUNT(DISTINCT e.symbol) FILTER (WHERE e.volume > 0) AS n",
     "COUNT(DISTINCT e.symbol) AS n"),
    ("coverage rule off",
     "if ratios[name] and all(r.get(d, 0.0) >= DATA_END_MIN_COVERAGE for r in ratios[name].values())}",
     "if ratios[name]}"),
    ("one exchange of the leg enough",
     "if ratios[name] and all(r.get(d, 0.0) >= DATA_END_MIN_COVERAGE for r in ratios[name].values())}",
     "if ratios[name] and any(r.get(d, 0.0) >= DATA_END_MIN_COVERAGE for r in ratios[name].values())}"),
    ("benchmark rule off", "for d in sorted(bench_days[name])",
     "for d in sorted({d for r in ratios[name].values() for d in r})"),
    ("the earliest data_end, not a day every leg traded", "end, newest = max(common), max(by.values())",
     "end, newest = min(by.values()), max(by.values())"),
    ("no lag warning", "date.fromisoformat(d)).days > END_DATE_LAG_WARN_DAYS)", "date.fromisoformat(d)).days > 99)"),
    ("the run day counts", "(today or datetime.now(timezone.utc).date()) - timedelta(days=1))",
     "(today or datetime.now(timezone.utc).date()))"),
    ("a pin on the run day", "        if end_date >= (today or datetime.now(timezone.utc).date()):",
     "        if False:"),
    ("a pin unchecked", "        if bad:\n            raise ValueError(f\"--end-date",
     "        if False:\n            raise ValueError(f\"--end-date"),
    ("any stub held", "if (end - last).days < END_DATE_MIN_STUB_DAYS:", "if (end - last).days < 0:"),
    ("a 7-day stub dropped", "if (end - last).days < END_DATE_MIN_STUB_DAYS:",
     "if (end - last).days <= END_DATE_MIN_STUB_DAYS:"),
    ("grid ignores the execution day", "if d + timedelta(days=offset_days) <= end]", "if d <= end]"),
    ("partial exit priced next day",
     "return 0 if self.end_date is not None and exit_date == self.end_date else offset_days", "return offset_days"),
    ("fetch runs past the end", "return e if self.end_date is None else min(e, self.end_date)", "return e"),
    ("benchmark exit offset ignored", "offset_days=offset_days if exit_offset_days is None else exit_offset_days)",
     "offset_days=offset_days)"),
    ("legacy grid moved", "return generate_rebalance_dates(start_year, self.end_year, frequency, months)",
     "return generate_rebalance_dates(start_year, self.end_year + 1, frequency, months)"),
    ("fraction from the grid date",
     "held = (self.partial_exit - self.last_rebalance - timedelta(days=self.offset_days)).days",
     "held = (self.partial_exit - self.last_rebalance).days"),
    ("full periods count the partial one", "\"full_periods\": sum(1 for f in fr if f == 1)", "\"full_periods\": len(fr)"),
    ("an unmarked row taken", "if any(\"partial\" not in r for r in rows):", "if False:"),
    ("an unmeasured partial period described", "if any(r[\"partial\"] for r in valid):", "if self.partial_exit is not None:"),
]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_all(du):
    out = []
    for name, fn in (("1 FINDER", finder_failures), ("2 PIN", pin_failures), ("3 GRID", grid_failures),
                     ("4 PRICING", pricing_failures), ("5 LEGACY", legacy_failures), ("6 ROWS", rows_failures)):
        try:
            out += [f"{name}: {x}" for x in fn(du)]
        except Exception as e:   # a missing helper or a crash is a failure, not a pass
            out.append(f"{name}: {type(e).__name__}: {e}")
    return out


def main():
    import data_utils as du
    for x in run_all(du):
        check(False, x)
    print("7 MUTANTS")
    src = open(os.path.join(ROOT, "data_utils.py")).read()
    with tempfile.TemporaryDirectory() as tmp:
        for i, (name, old, new) in enumerate(MUTANTS):
            if src.count(old) != 1:
                check(False, f"mutant {name}: anchor found {src.count(old)} times")
                continue
            path = os.path.join(tmp, f"du_mut_{i}.py")
            with open(path, "w") as fh:
                fh.write(src.replace(old, new))
            check(bool(run_all(load(path, f"du_mut_{i}"))), f"mutant {name} passes every check")
    print("FAIL" if FAIL else "PASS", f"({len(FAIL)} failures)")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
