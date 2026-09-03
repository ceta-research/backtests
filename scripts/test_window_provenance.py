#!/usr/bin/env python3
"""Runtime proof that the measured window reaches a human (blocker B006, defect b).

scripts/verify_floor_guard.py gate 11 proves the READERS EXIST statically. This
proves they WORK, which is a different claim: a provenance field is only a fix
if something downstream actually emits it, and every channel here had failed
that test at least once.

  1 LABEL       period_accounting builds a label that cannot be copied into a
                heading and come out as the requested span.
  2 WARNING     warn_if_truncated emits the loud line, including the degenerate
                n_periods == 0 case that used to render as the unreadable
                "Measured window is NO MEASURED PERIODS, NOT 2002-2027".
  3 CAMPAIGN    scripts/batch_rerun_diff.run_backtest passes WARNING lines
                through to stdout instead of capturing and discarding them.
                This is the end-to-end one: it drives a real subprocess.
  4 TABLE       the summary-printer note renders for a truncated leg and stays
                silent for an untruncated one.
  5 SILENCE     an untruncated leg produces no warning and no note, so the
                signal keeps meaning something.

Usage:  python scripts/test_window_provenance.py
"""
import io
import os
import pathlib
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from metrics import period_accounting, warn_if_truncated  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'ok  ' if cond else 'FAIL'} {name}" + (f"  -- {detail}" if detail else ""))


def osl_shaped():
    """52-week-low's real OSL leg: 95 rebalances, ^OSEAX prices only the last 50.

    Numbers taken from the committed record (n_periods=50, total_rebalances=95,
    cash_periods=84, invested_periods=11), so a change that breaks the prior art
    breaks this test.
    """
    executed = [{"rebalance_date": f"{2002 + i // 4}-01-02",
                 "exit_date": f"{2002 + (i + 1) // 4}-04-02"} for i in range(95)]
    valid = executed[45:]                      # first benchmark-priced period
    valid[0]["rebalance_date"] = "2013-04-02"
    return executed, valid


def main():
    print("1 LABEL")
    executed, valid = osl_shaped()
    acct = period_accounting(executed, valid, 84, benchmark_symbol="^OSEAX",
                             benchmark_first_date="2013-03-05",
                             universe_name="OSL", warn=False)
    check("counts match the committed 3c39bcb record",
          (acct["n_periods"], acct["total_rebalances"], acct["cash_periods"],
           acct["invested_periods"]) == (50, 95, 84, 11),
          f"{acct['n_periods']}/{acct['total_rebalances']} "
          f"cash={acct['cash_periods']} inv={acct['invested_periods']}")
    lbl = acct["window_label"]
    check("label leads with the MEASURED span, not the requested one",
          lbl.startswith("2013-"), lbl)
    check("label names the benchmark and its first date",
          "^OSEAX" in lbl and "2013-03-05" in lbl)
    check("label states how much went unmeasured",
          "45 of 95 rebalances unmeasured" in lbl)
    check("label still carries the requested span, so nothing is hidden",
          "requested 2002-" in lbl)
    # The point of the pre-formatted string: pasting it whole cannot produce the
    # wrong window. Shortening it to the wrong window takes a deliberate delete.
    check("label cannot be copied verbatim into a 2002-2025 heading",
          "2002-2025" not in lbl.split("(")[0])

    print("\n2 WARNING")
    buf = io.StringIO()
    warn_if_truncated(acct, "OSL", stream=buf)
    w = buf.getvalue()
    check("truncated leg warns", w.startswith("WARNING OSL:"), w.strip())
    check("warning says which window is real", "NOT 2002-" in w and "2013-2025" in w)

    # Degenerate leg: qarp JNB, 51 rebalances, zero benchmark-priced.
    jnb = period_accounting([{"rebalance_date": f"{2002 + i // 2}-01-02",
                              "exit_date": f"{2002 + i // 2}-07-02"}
                             for i in range(51)], [], 51,
                            universe_name="JNB", warn=False)
    check("100%-cash leg is a reportable record, not an absent one",
          (jnb["total_rebalances"], jnb["cash_periods"],
           jnb["invested_periods"], jnb["n_periods"]) == (51, 51, 0, 0))
    buf = io.StringIO()
    warn_if_truncated(jnb, "JNB", stream=buf)
    w0 = buf.getvalue()
    check("zero-period warning reads as a sentence",
          "NOTHING was measured" in w0 and "NO MEASURED PERIODS, NOT" not in w0,
          w0.strip())

    print("\n3 CAMPAIGN  (drives a real subprocess)")
    import batch_rerun_diff as brd
    with tempfile.TemporaryDirectory() as td:
        topic = pathlib.Path(td) / "fake-topic"
        topic.mkdir()
        # Stands in for a backtest: prints a normal metrics block to stdout and
        # a truncation warning to stderr, exactly as a real OSL leg does.
        (topic / "backtest.py").write_text(
            "import sys\n"
            "print('CAGR                              8.28%      8.02%')\n"
            "print('Sharpe Ratio                      0.299      0.361')\n"
            "print('WARNING OSL: benchmark ^OSEAX starts 2013-03-05; 45 of 95 "
            "rebalances unmeasured. Measured window is 2013-2025, NOT 2002-2025.',"
            " file=sys.stderr)\n")
        old_dir, old_stdout = brd.BACKTESTS_DIR, sys.stdout
        brd.BACKTESTS_DIR = td
        cap = io.StringIO()
        try:
            sys.stdout = cap
            res = brd.run_backtest("fake-topic", preset="osl", timeout=60)
        finally:
            sys.stdout = old_stdout
            brd.BACKTESTS_DIR = old_dir
    printed = cap.getvalue()
    check("run_backtest still parses the metrics it always did",
          res and res.get("cagr") == 8.28, str(res and res.get("cagr")))
    check("WARNING reaches STDOUT, so a redirected campaign log keeps it",
          "WARNING OSL:" in printed and "NOT 2002-2025" in printed,
          printed.strip().splitlines()[:1])
    check("the log line says which topic and preset it came from",
          "fake-topic [osl]" in printed)
    check("the warning is also carried on the result for a report writer",
          len(res.get("window_warnings") or []) == 1)

    print("\n4 TABLE")
    # The exact expression the 54 summary printers now run.
    def note(r):
        return (f"  {r['universe']}: MEASURED {r.get('window_label')}"
                if r.get("window_truncated") else None)
    row = dict(acct, universe="OSL")
    check("truncated leg gets a table note", note(row) is not None)
    check("the note names the exchange, so it stands alone",
          note(row).strip().startswith("OSL:"), note(row))
    check("the note carries the same label as the JSON",
          acct["window_label"] in note(row))

    print("\n5 SILENCE")
    ex2 = [{"rebalance_date": f"{2002 + i}-01-02", "exit_date": f"{2003 + i}-01-02"}
           for i in range(24)]
    clean = period_accounting(ex2, ex2, 3, benchmark_symbol="SPY",
                              universe_name="US", warn=False)
    buf = io.StringIO()
    warn_if_truncated(clean, "US", stream=buf)
    check("untruncated leg warns about nothing", buf.getvalue() == "")
    check("untruncated leg gets no table note",
          note(dict(clean, universe="US")) is None)
    check("untruncated label is a bare span",
          clean["window_label"] == "2002-2026", clean["window_label"])

    print(f"\nPASS {len(PASS)}  FAIL {len(FAIL)}")
    if FAIL:
        for f in FAIL:
            print("  FAILED:", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
