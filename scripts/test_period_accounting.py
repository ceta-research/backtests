#!/usr/bin/env python3
"""Unit tests for metrics.period_accounting (blocker B006).

Pure arithmetic and string formatting, no I/O, no network. Runs in milliseconds.

WHAT THIS LEG CAN AND CANNOT PROVE -- stated up front, because the obvious
framing overclaims. Under the total_rebalances convention the invariant is
arithmetically TRUE BY CONSTRUCTION for any input data: invested is derived, both
terms count over one list, and valid is a subset of executed. So these checks can
only ever catch a WIRING regression, and specifically they CANNOT catch someone
passing `valid` where `executed` belongs -- the exact bug B005 shipped into
52-week-low. That is why the static gates (verify_floor_guard.py gates 7-9) carry
the teeth for scope, and scan_results_invariant.py carries them for data.

Usage:  python scripts/test_period_accounting.py
"""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from metrics import period_accounting, warn_if_truncated  # noqa: E402

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
        print(f"  PASS  {name}")
    except Exception as e:
        FAIL.append((name, f"{e.__class__.__name__}: {e}"))
        print(f"  FAIL  {name}: {e.__class__.__name__}: {e}")


def rec(pr=0.05, sr=0.03, held=5, d="2002-01-03", x="2002-04-03"):
    return {"portfolio_return": pr, "spy_return": sr, "stocks_held": held,
            "rebalance_date": d, "exit_date": x}


def raises(fn):
    try:
        fn()
    except ValueError:
        return
    raise AssertionError("expected ValueError, none raised")


# ---------------------------------------------------------------- T1
def t1_52_week_low_osl():
    """THE MUST-PASS CONTROL. 52-week-low OSL is correct prior art (commit
    3c39bcb) and the helper must reproduce its committed numbers exactly."""
    executed = ([rec(held=0, d="2002-01-03", x="2002-04-03")] * 84
                + [rec(held=5, d="2013-04-01", x="2025-10-01")] * 11)
    valid = executed[45:]                                    # 50 measured
    a = period_accounting(executed, valid, 84,
                          benchmark_symbol="^OSEAX",
                          benchmark_first_date="2013-03-05",
                          universe_name="OSL", warn=False)
    assert a["n_periods"] == 50, a["n_periods"]
    assert a["total_rebalances"] == 95, a["total_rebalances"]
    assert a["cash_periods"] == 84, a["cash_periods"]
    assert a["invested_periods"] == 11, a["invested_periods"]
    assert a["cash_periods"] + a["invested_periods"] == a["total_rebalances"]
    assert a["window_truncated"] is True
    assert a["unmeasured_periods"] == 45, a["unmeasured_periods"]
    assert "^OSEAX starts 2013-03-05" in a["window_label"], a["window_label"]
    assert "45 of 95 rebalances unmeasured" in a["window_label"]

    # Pinned against the ACTUAL committed record, read at test time so it cannot
    # drift silently. This is the direct answer to "prove 52-week-low still
    # passes": the helper reproduces 3c39bcb's numbers and the four bounds do not
    # fire on them.
    j = json.load(open(ROOT / "52-week-low/results/returns_OSL.json"))
    got = (j["n_periods"], j["total_rebalances"],
           j["cash_periods"], j["invested_periods"])
    assert got == (50, 95, 84, 11), got
    assert j["cash_periods"] + j["invested_periods"] == j["total_rebalances"]
    assert 0 <= j["n_periods"] <= j["total_rebalances"]


# ---------------------------------------------------------------- T2
def t2_qarp_jnb_all_cash():
    """The 100%-cash degenerate class. Under convention (A) this record would
    collapse to cash=0/invested=0/n=0, indistinguishable from a run that never
    happened. It must stay legible as what it is."""
    a = period_accounting([rec(held=0)] * 51, [], 51,
                          benchmark_symbol="SPY", universe_name="JNB",
                          warn=False)
    assert (a["n_periods"], a["total_rebalances"],
            a["cash_periods"], a["invested_periods"]) == (0, 51, 51, 0)
    assert a["window_label"].startswith("NO MEASURED PERIODS"), a["window_label"]
    assert "all 51 rebalances unmeasured" in a["window_label"]

    j = json.load(open(ROOT / "qarp/results/jnb_results.json"))
    assert (j["n_periods"], j["cash_periods"], j["invested_periods"]) == (0, 51, -51)


# ---------------------------------------------------------------- T3
def t3_wiring_bugs_raise():
    """The mis-wiring that GENERATED the bad records: cash counted over a
    collection larger than the one passed as `executed`.

    Be precise about the limit. Feed the TRUE historical shape of deleveraging
    OSL (executed=95, valid=50, cash=53) and the helper correctly does NOT raise
    -- invested=42 is a perfectly valid record. The committed bad records are
    detected by scan_results_invariant.py's legacy regime, not here.
    """
    # deleveraging OSL shape as it was mis-wired: 53 cash counted over 95
    # results, but `valid` (50) handed in as the executed population.
    raises(lambda: period_accounting([rec()] * 50, [rec()] * 50, 53, warn=False))
    # oversold-quality shape: 101 cash over a 95-long executed population
    raises(lambda: period_accounting([rec()] * 95, [rec()] * 50, 101, warn=False))
    # garp OSL shape: 73 cash, executed handed in as valid (50)
    raises(lambda: period_accounting([rec()] * 50, [rec()] * 50, 73, warn=False))

    # ...and the correctly-wired version of the same leg does NOT raise.
    ok = period_accounting([rec()] * 95, [rec()] * 50, 53, warn=False)
    assert ok["invested_periods"] == 42, ok["invested_periods"]


# ---------------------------------------------------------------- T4-T8
def t4_valid_must_be_subset():
    raises(lambda: period_accounting([rec()] * 10, [rec()] * 11, 0, warn=False))


def t5_bad_cash_values():
    raises(lambda: period_accounting([rec()] * 10, [rec()] * 5, -1, warn=False))
    raises(lambda: period_accounting([rec()] * 10, [rec()] * 5, 11, warn=False))
    raises(lambda: period_accounting([rec()] * 10, [rec()] * 5, 3.0, warn=False))
    raises(lambda: period_accounting([rec()] * 10, [rec()] * 5, True, warn=False))


def t6_trailing_stubs_excluded():
    """sector-momentum's class: the rebalance grid runs past the price-fetch cap,
    leaving records with no exit price. Those are not rebalances the strategy
    ran, and must inflate neither cash_periods nor total_rebalances."""
    results = [rec(held=0)] * 30 + [rec(held=5)] * 65 + [rec(pr=None, sr=None)] * 5
    executed = [r for r in results if r["portfolio_return"] is not None]
    valid = [r for r in results
             if r["portfolio_return"] is not None and r["spy_return"] is not None]
    assert len(executed) == 95 and len(valid) == 95
    a = period_accounting(executed, valid, 30, warn=False)
    assert a["total_rebalances"] == 95, a["total_rebalances"]
    assert a["window_truncated"] is False
    assert a["window_label"] == "2002-2002", a["window_label"]


def t7_degenerate_empty():
    a = period_accounting([], [], 0, warn=False)
    assert (a["n_periods"], a["total_rebalances"],
            a["cash_periods"], a["invested_periods"]) == (0, 0, 0, 0)
    assert a["measured_start"] is None and a["requested_start"] is None
    assert a["window_truncated"] is False


def t8_missing_date_keys():
    """Pairs and yearly records key on `year` (sometimes an int), not
    rebalance_date. A provenance function that crashes a valid run is worse than
    one that degrades."""
    yearly = [{"year": 2002, "is_cash": True}, {"year": 2003, "is_cash": False}]
    a = period_accounting(yearly, yearly[1:], 1,
                          date_key="year", end_key="year", warn=False)
    assert a["measured_start"] == "2003", a["measured_start"]
    assert a["requested_start"] == "2002", a["requested_start"]
    assert a["window_truncated"] is True

    odd = [{"nothing": 1}] * 4
    b = period_accounting(odd, odd[2:], 0, warn=False)
    assert b["measured_start"] is None
    assert "?" in b["window_label"], b["window_label"]


def t9_no_none_strings_in_output():
    """The benchmark_first_date deviation: symbol and date are both optional, and
    the label/warning must never render 'benchmark None starts None'."""
    import io
    a = period_accounting([rec()] * 95, [rec()] * 50, 10, warn=False)
    assert "None" not in a["window_label"], a["window_label"]
    b = period_accounting([rec()] * 95, [rec()] * 50, 10,
                          benchmark_symbol="^OSEAX", warn=False)
    assert "None" not in b["window_label"], b["window_label"]
    assert "benchmark ^OSEAX;" in b["window_label"], b["window_label"]
    c = period_accounting([rec(held=0)] * 51, [], 51, warn=False)
    assert "None" not in c["window_label"], c["window_label"]

    buf = io.StringIO()
    warn_if_truncated(a, None, stream=buf)
    assert "None" not in buf.getvalue(), buf.getvalue()
    assert "WARNING" in buf.getvalue()


def t10_warn_is_never_fatal():
    """Truncation warns; it must not raise, or the re-run campaign stalls on
    every empty-book OSL leg."""
    import io
    buf = io.StringIO()
    a = period_accounting([rec()] * 95, [rec()] * 50, 84,
                          benchmark_symbol="^OSEAX",
                          benchmark_first_date="2013-03-05",
                          universe_name="OSL", warn=False)
    warn_if_truncated(a, "OSL", stream=buf)
    out = buf.getvalue()
    assert "WARNING OSL" in out, out
    assert "45 of 95 rebalances unmeasured" in out, out
    assert "NOT 2002-2002" in out, out
    # untruncated leg is silent
    buf2 = io.StringIO()
    warn_if_truncated(period_accounting([rec()] * 10, [rec()] * 10, 0, warn=False),
                      "US", stream=buf2)
    assert buf2.getvalue() == "", buf2.getvalue()


def main():
    print("test_period_accounting (B006)")
    check("T1  52-week-low OSL prior art reproduced + pinned to committed JSON",
          t1_52_week_low_osl)
    check("T2  qarp JNB 100%-cash degenerate class stays legible", t2_qarp_jnb_all_cash)
    check("T3  wiring bugs raise; correctly-wired truncated leg does not",
          t3_wiring_bugs_raise)
    check("T4  valid must be a subset of executed", t4_valid_must_be_subset)
    check("T5  non-int / out-of-range cash raises", t5_bad_cash_values)
    check("T6  trailing stubs excluded from executed", t6_trailing_stubs_excluded)
    check("T7  degenerate empty inputs", t7_degenerate_empty)
    check("T8  missing / int date keys degrade, never raise", t8_missing_date_keys)
    check("T9  no 'None' leaks into label or warning", t9_no_none_strings_in_output)
    check("T10 truncation warns and is never fatal", t10_warn_is_never_fatal)
    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
