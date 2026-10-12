#!/usr/bin/env python3
"""Offline checks for cash-conversion's per-bucket floor (no network, no backtest).

A CCC bucket with fewer than MIN_BUCKET_STOCKS names buyable at ENTRY (a usable entry price,
filter_returns' entry test) holds cash that year: 0.0%, counted in cash_periods. The count
reads entry prices only, so lost exits, the return cap and LTP fills never decide cash, and
LTP is asked only for invested buckets' names. run_backtest runs on stubbed screen, price and
benchmark functions; every mutant of the topic's source must trip its own named check.

Not driven by test_floor_guard.py: its harness assumes one book per period, this topic has four.

Usage:
    python scripts/test_cash_conversion_floor.py
"""
import contextlib
import io
import os
import sys
import types
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from data_utils import entry_usable  # noqa: E402

PATH = os.path.join(ROOT, "cash-conversion", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()

D0, D1, D2 = date(2020, 4, 1), date(2021, 4, 1), date(2022, 4, 1)
BENCH = 0.05


def load(src):
    """The topic module built from `src` (the real file, or a mutant of it)."""
    mod = types.ModuleType("cc_floor")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def names(prefix, n, start=0):
    return [f"{prefix}{i:02d}" for i in range(start, start + n)]


class FakeLtp:
    """Records what it was asked for; answers a fill for EVERY scored name with a missing exit,
    asked or not, so a cash bucket's names have fills sitting in xps_ltp."""

    def __init__(self, universe):
        self.universe, self.asked = universe, []

    def fill(self, symbols, eps, xps, entry_date, exit_date, offset_days=0, min_entry_price=0.0):
        self.asked += list(symbols)
        return {s: 120.0 for s in self.universe
                if xps.get(s) is None and entry_usable(eps.get(s), min_entry_price)}


def run(mod, spec, dates=(D0, D1), ltp=None, method="drop"):
    """spec: {symbol: (bucket, direction, entry_px, exit_px)}. Returns the one period record.
    Drives each exit method on purpose, so `method`, not `exit_method` (gate 14 is for callers)."""
    scored = {s: {"ccc": 0.0, "ccc_prior": None, "ccc_change": None, "bucket": b,
                  "direction": d, "mcap": 5e9} for s, (b, d, _, _) in spec.items()}
    mod.screen_stocks = lambda con, t, m, offset_days=1: dict(scored)
    mod.get_price = lambda con, s, t, offset_days=0: spec[s][2] if t == dates[0] else spec[s][3]
    mod.get_benchmark_return = lambda *a, **k: BENCH
    with contextlib.redirect_stdout(io.StringIO()):
        periods = mod.run_backtest(None, list(dates), 0.0, use_costs=False,
                                   ltp=ltp, exit_method=method)
    assert len(periods) == 1, periods
    return periods[0]


def book(prefix, n, bucket, direction="increasing", entry=100.0, exit=110.0, start=0):
    return {s: (bucket, direction, entry, exit) for s in names(prefix, n, start)}


def near(a, b):
    return a is not None and abs(a - b) < 1e-9


def failures(mod):
    f = []

    def want(label, ok, got=""):
        if not ok:
            f.append(f"{label}: {got}")

    floor = mod.MIN_BUCKET_STOCKS
    want("floor is 10", floor == 10, floor)

    # S1: 9 buyable -> cash (low, and low_decreasing inside it); 10 -> invested.
    s1 = {**book("L", 9, "low", "decreasing"), **book("M", 10, "mid"), **book("H", 10, "high")}
    p1 = run(mod, s1)
    want("S1 9 buyable -> cash", p1["low_return"] == 0.0 and p1["low_count"] == 0
         and p1["low_buyable"] == 9 and p1["low_screened"] == 9
         and p1["low_decreasing_return"] == 0.0 and p1["low_decreasing_buyable"] == 9, p1)
    want("S1 10 buyable -> invested", near(p1["mid_return"], 0.10) and p1["mid_count"] == 10
         and p1["mid_buyable"] == 10 and near(p1["high_return"], 0.10), p1)

    # S2: names without a usable entry price don't count; buckets decide independently.
    s2 = {**book("H", 9, "high"),
          "H09": ("high", "increasing", None, 110.0), "H10": ("high", "increasing", 0.5, 110.0),
          "H11": ("high", "increasing", 0.0, 110.0),
          **book("M", 10, "mid"),
          "M10": ("mid", "increasing", None, 110.0), "M11": ("mid", "increasing", 0.99, 110.0),
          **book("L", 9, "low", "decreasing"), **book("L", 1, "low", "increasing", start=9)}
    p2 = run(mod, s2)
    want("S2 unpriced names don't count", p2["high_return"] == 0.0 and p2["high_count"] == 0
         and p2["high_screened"] == 12 and p2["high_buyable"] == 9, p2)
    want("S2 10 of 12 -> invested", near(p2["mid_return"], 0.10) and p2["mid_count"] == 10
         and p2["mid_screened"] == 12 and p2["mid_buyable"] == 10, p2)
    want("S2 low invested, low_decreasing cash", p2["low_count"] == 10
         and p2["low_decreasing_count"] == 0 and p2["low_decreasing_buyable"] == 9, p2)

    # S3: exit-side drops never decide cash: lost exits, zero survivors, the return cap.
    s3 = {**book("M", 3, "mid"), **book("M", 7, "mid", exit=None, start=3),
          **book("H", 10, "high", exit=None),
          **book("L", 5, "low", exit=1100.0), **book("L", 5, "low", start=5)}
    p3 = run(mod, s3, dates=(D1, D2))
    want("S3 lost exits don't decide cash", near(p3["mid_return"], 0.10) and p3["mid_count"] == 3
         and p3["mid_buyable"] == 10, p3)
    want("S3 zero survivors invested", p3["high_return"] == 0.0 and p3["high_count"] == 0
         and p3["high_buyable"] == 10, p3)
    want("S3 return cap doesn't decide cash", near(p3["low_return"], 0.10) and p3["low_count"] == 5
         and p3["low_buyable"] == 10, p3)

    # S4: LTP asked only for invested buckets; fills never invest a cash bucket.
    s4 = {**book("L", 9, "low", exit=None),
          **book("M", 6, "mid"), **book("M", 4, "mid", exit=None, start=6),
          **book("H", 10, "high")}
    fake = FakeLtp(list(s4))
    p4 = run(mod, s4, ltp=fake, method="ltp")
    want("S4 LTP only for invested buckets",
         sorted(fake.asked) == sorted(names("M", 10) + names("H", 10)), fake.asked)
    want("S4 LTP fills never invest a cash bucket", p4["low_return"] == 0.0 and p4["low_count"] == 0
         and p4["low_buyable"] == 9, p4)
    want("S4 LTP fills invested bucket", near(p4["mid_return"], 0.14) and p4["mid_count"] == 10, p4)
    a = p4.get("ltp_audit", {})
    want("S4 LTP audit", a.get("low") == {"missing": 0, "filled": []}
         and a.get("mid") == {"missing": 4, "filled": names("M", 4, start=6)}, a)
    fake = FakeLtp(list(s4))
    pb = run(mod, s4, ltp=fake, method="both")
    want("S4 both: cash track has no LTP book", "low_return_ltp" not in pb
         and pb["low_return"] == 0.0 and near(pb.get("mid_return_ltp"), 0.14)
         and near(pb["mid_return"], 0.10) and pb["mid_count"] == 6, pb)

    # S5: results JSON: cash from the floor (not count == 0), cash + invested == n per bucket,
    # avg_stock_counts over invested periods only.
    with contextlib.redirect_stdout(io.StringIO()):
        out = mod.build_output([p1, p3], "TEST", 0.0, 1)
    n = len(out.get("annual_returns", []))
    want("S5 cash_periods", out.get("cash_periods") ==
         {"low": 1, "mid": 0, "high": 0, "low_decreasing": 2}, out.get("cash_periods"))
    want("S5 invested_periods", out.get("invested_periods") ==
         {"low": 1, "mid": 2, "high": 2, "low_decreasing": 0}, out.get("invested_periods"))
    want("S5 cash + invested == n", n == 2 and all(
        out["cash_periods"][t] + out["invested_periods"][t] == n for t in mod.TRACKS), n)
    want("S5 min_stocks", out.get("min_stocks") == floor, out.get("min_stocks"))
    # Over invested periods only: low 5 (p3; p1 cash), high (10 + 0)/2 (p3 invested, 0 survivors).
    # mid is (10 + 3)/2 = 6.5 -> 6 either way, so it is not asserted.
    avg = out.get("avg_stock_counts", {})
    want("S5 avg_stock_counts when invested", avg.get("low") == 5 and avg.get("high") == 5
         and avg.get("low_decreasing") == 0, avg)
    ar = out.get("annual_returns", [{}])[0]
    want("S5 annual_returns funnel", all(ar.get(f"{t}_{k}") == p1[f"{t}_{k}"]
                                         for t in mod.TRACKS for k in ("screened", "buyable")), ar)
    return f


MUTANTS = {
    # (edits, the check that must catch it)
    "floor 9": ([("MIN_BUCKET_STOCKS = 10", "MIN_BUCKET_STOCKS = 9")], "S1 9 buyable -> cash"),
    "floor 11": ([("MIN_BUCKET_STOCKS = 10", "MIN_BUCKET_STOCKS = 11")], "S1 10 buyable -> invested"),
    "screened count": ([("entry_buyable_prices(list(p), eps, 1.0)", "len(p)")],
                       "S2 unpriced names don't count"),
    "count reads exits": ([("entry_buyable_prices(list(p), eps, 1.0)",
                            "entry_buyable_prices([s for s in p if xps[s] is not None], eps, 1.0)")],
                          "S3 lost exits don't decide cash"),
    "cash on the post-filter book": ([("            track_data[name] = {\"return\": ret, ",
                                       "            if cnt < MIN_BUCKET_STOCKS:\n"
                                       "                ret, cnt = 0.0, 0\n"
                                       "            track_data[name] = {\"return\": ret, "),
                                      ("cash = {name for name, _ in tracks if buyable[name] < MIN_BUCKET_STOCKS}",
                                       "cash = set()")],
                                     "S3 lost exits don't decide cash"),
    "LTP asked for every name": ([("held = [s for s, v in scored.items() if v[\"bucket\"] not in cash]",
                                   "held = list(scored)")], "S4 LTP only for invested buckets"),
    "LTP mode skips the floor": ([("            if name in cash:\n",
                                   "            if name in cash and not ltp:\n")],
                                 "S4 LTP fills never invest a cash bucket"),
    "cash from count == 0": ([("p[f\"{t}_buyable\"] < MIN_BUCKET_STOCKS", "p[f\"{t}_count\"] == 0")],
                             "S5 cash_periods"),
    # Dropping the predicate averages over all n periods: the pre-fix behaviour.
    "avg over every year": ([(" if p[f\"{t}_buyable\"] >= MIN_BUCKET_STOCKS]", "]")],
                            "S5 avg_stock_counts when invested"),
}

if __name__ == "__main__":
    bad = failures(load(SRC))
    if bad:
        raise SystemExit("FAIL:\n  " + "\n  ".join(bad))
    for name, (edits, expect) in MUTANTS.items():
        src = SRC
        for old, new in edits:
            if old not in src:
                raise SystemExit(f"FAIL: mutant '{name}' anchor missing: {old!r}")
            src = src.replace(old, new)
        try:
            fails = failures(load(src))
        except Exception as e:     # a crash is not the named check firing
            fails = [f"raised {e.__class__.__name__}: {e}"]
        if not any(x.startswith(expect + ":") for x in fails):
            raise SystemExit(f"FAIL: mutant '{name}' not caught by '{expect}': {fails}")
    print(f"test_cash_conversion_floor: PASS (5 scenarios; {len(MUTANTS)} mutants caught)")
