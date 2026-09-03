#!/usr/bin/env python3
"""Call every canonical build_output on fabricated data and check the record.

Blocker B006, verification LEG 4. This is the only leg that exercises real topic
code, and it runs NO backtest: no network, no DuckDB query, no API call. It
imports each */backtest.py the same way verify_floor_guard.py gate 2 already
does, builds a fixture in memory, and asserts on the returned dict.

WHY THIS LEG EXISTS. The committed results corpus cannot serve as an oracle for
the new fields: `period_data` is present in only 29 of 2,176 accounting nodes, so
there is almost nothing to recompute cash and invested FROM. The static gates
prove the helper is CALLED; only this proves the record that comes out has the
right shape and the right numbers.

THE FIXTURE exercises all three populations at once:
    20 cash periods,   benchmark-priced     -> executed, valid
    60 invested,       benchmark-priced     -> executed, valid
    10 cash periods,   benchmark CANNOT price -> executed, NOT valid
     5 trailing stubs, never ran            -> NOT executed
  => total_rebalances 90, n_periods 80, cash 30, invested 60, unmeasured 10

Usage:  python scripts/test_build_output_synthetic.py [-v]
"""
import importlib.util
import io
import pathlib
import sys
import contextlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from metrics import compute_metrics  # noqa: E402

VERBOSE = "-v" in sys.argv

CANON = ("metrics", "annual", "valid", "results", "universe_name", "frequency",
         "periods_per_year", "cash_periods", "avg_stocks")

SCHEMA = {
    "n_periods": int, "total_rebalances": int, "cash_periods": int,
    "invested_periods": int, "measured_start": str, "measured_end": str,
    "requested_start": str, "requested_end": str, "window_truncated": bool,
    "unmeasured_periods": int, "window_label": str,
}


def rec(pr=0.05, sr=0.03, held=5, d="2002-01-03", x="2002-04-03"):
    return {"portfolio_return": pr, "spy_return": sr, "stocks_held": held,
            "rebalance_date": d, "exit_date": x,
            # keys various topics read off a period record
            "avg_weight": 1.0, "avg_etf_count": 3.0, "avg_ownership_ratio": 0.5,
            "n_sectors": 4, "n_compressed_sectors": held,
            "sectors_selected": ["Tech"], "signal_active": True,
            "holdings": "AAPL", "signal_value": 1.0, "regime_changed": False}


def fixture():
    results = ([rec(held=0, d="2002-01-03", x="2002-04-03")] * 20
               + [rec(held=5, d="2013-04-01", x="2025-10-01")] * 60
               + [rec(held=0, sr=None, d="2003-01-03", x="2003-04-03")] * 10
               + [rec(pr=None, sr=None, d="2026-04-01", x="2026-07-01")] * 5)
    valid = [r for r in results
             if r["portfolio_return"] is not None and r["spy_return"] is not None]
    executed = [r for r in results if r["portfolio_return"] is not None]
    cash = sum(1 for r in executed if r["stocks_held"] == 0)
    return results, valid, executed, cash


def load(topic):
    spec = importlib.util.spec_from_file_location(
        "bo_" + topic.replace("-", "_"), ROOT / topic / "backtest.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def canonical_topics():
    """Topics whose build_output takes the canonical positional signature."""
    import inspect
    out = []
    for p in sorted(ROOT.glob("*/backtest.py")):
        t = p.parent.name
        try:
            m = load(t)
        except Exception:
            continue
        fn = getattr(m, "build_output", None)
        if fn is None:
            continue
        try:
            params = list(inspect.signature(fn).parameters)
        except (TypeError, ValueError):
            continue
        if tuple(params[:len(CANON)]) == CANON:
            out.append((t, m, fn))
    return out


def main():
    results, valid, executed, cash = fixture()
    assert (len(results), len(executed), len(valid), cash) == (95, 90, 80, 30)
    print(f"fixture: results={len(results)} executed={len(executed)} "
          f"valid={len(valid)} cash_over_executed={cash}")

    port = [r["portfolio_return"] for r in valid]
    spy = [r["spy_return"] for r in valid]
    mt = compute_metrics(port, spy, 4)
    annual = []

    topics = canonical_topics()
    print(f"topics with the canonical build_output signature: {len(topics)}\n")

    ok, bad, skipped = [], [], []
    for t, m, fn in topics:
        # Some topics take extra REQUIRED positionals after avg_stocks
        # (avg_etf_count, avg_sectors, ey_threshold, ...). They are all plain
        # scalars used for reporting, so feed 1.0 until the call binds rather
        # than skipping the topic: those extra-arg topics include four of the
        # ETF family and relative-strength, which are exactly the shapes most
        # likely to drift from the canonical record.
        buf, out, err = io.StringIO(), None, None
        for extra in range(0, 4):
            buf = io.StringIO()
            try:
                with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(buf):
                    out = fn(mt, annual, valid, results, "OSL", "quarterly", 4,
                             cash, 5.0, *([1.0] * extra))
                err = None
                break
            except TypeError as e:
                if "positional argument" in str(e):
                    err = ("signature", str(e))
                    continue
                err = ("error", f"TypeError: {e}")
                break
            except Exception as e:
                err = ("error", f"{e.__class__.__name__}: {e}")
                break
        if err and err[0] == "signature":
            skipped.append((t, f"signature mismatch: {err[1]}"))
            continue
        if err:
            bad.append((t, err[1]))
            continue

        problems = []
        got = (out.get("total_rebalances"), out.get("n_periods"),
               out.get("cash_periods"), out.get("invested_periods"))
        if got != (90, 80, 30, 60):
            problems.append(f"counts {got}, expected (90, 80, 30, 60)")
        if out.get("cash_periods", 0) + out.get("invested_periods", 0) \
                != out.get("total_rebalances"):
            problems.append("cash + invested != total_rebalances")
        if out.get("window_truncated") is not True:
            problems.append("window_truncated not True")
        if out.get("unmeasured_periods") != 10:
            problems.append(f"unmeasured_periods {out.get('unmeasured_periods')}")
        for k, ty in SCHEMA.items():
            if k not in out:
                problems.append(f"missing {k}")
            elif out[k] is not None and not isinstance(out[k], ty):
                problems.append(f"{k} is {type(out[k]).__name__}, want {ty.__name__}")
        label = out.get("window_label", "")
        if "unmeasured" not in label:
            problems.append(f"window_label does not flag truncation: {label!r}")
        if "None" in label:
            problems.append(f"window_label leaks None: {label!r}")
        if out.get("measured_start") != "2002-01-03":
            problems.append(f"measured_start {out.get('measured_start')!r}")
        # the truncation warning must have reached stderr, not raised
        if "WARNING" not in buf.getvalue():
            problems.append("no truncation warning emitted")

        if problems:
            bad.append((t, "; ".join(problems)))
        else:
            ok.append(t)
            if VERBOSE:
                print(f"  PASS {t:<28} {label}")

    print(f"\nPASS {len(ok)}   FAIL {len(bad)}   SKIPPED {len(skipped)}")
    for t, why in bad:
        print(f"  FAIL {t}: {why}")
    for t, why in skipped:
        print(f"  SKIP {t}: {why}")
    if ok:
        sample = next(t for t in ok)
        m = dict(load(sample).__dict__)
        print(f"\nsample record from {sample}:")
        with contextlib.redirect_stderr(io.StringIO()), \
                contextlib.redirect_stdout(io.StringIO()):
            out = load(sample).build_output(mt, annual, valid, results, "OSL",
                                            "quarterly", 4, cash, 5.0)
        for k in ("n_periods", "total_rebalances", "cash_periods",
                  "invested_periods", "years", "measured_start", "measured_end",
                  "requested_start", "requested_end", "window_truncated",
                  "unmeasured_periods", "window_label"):
            print(f"  {k:20} {out.get(k)!r}")

    if len(ok) < 30:
        print(f"\nSELF-TEST FAIL: only {len(ok)} topics exercised; this harness "
              "has stopped reaching real code")
        return 1
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
