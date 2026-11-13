#!/usr/bin/env python3
"""Offline check of scripts/same_bars_scan.py on synthetic legs (no network).

Ten annual rebalance windows. Every line's first window bar (the day after the rebalance date) has its own close
and volume, and every line carries the same bar on each rebalance date, which is outside the window. Leg one
(ur-3's blocks: class_stand_ins, two known lines, four line starts, two line ends) holds these copies:
  AAA, BBB            the same first bar in 3 windows, no rule covers them: listed. BBB has a second bar on its
                      first window bar's date, with a lower adjClose and close: the higher one is kept;
  NEAR1, NEAR2        the same in 2 windows: under the 3, not listed;
  XYZ.BK, XYZ-R.BK    check N's NVDR twins, 4 windows: exempt;
  PPP.BK, QQQ-R.BK    an NVDR line beside another issuer's line, 3 windows: listed;
  PETR3.SA, PETR4.SA  class twins (one issuer), 3 windows: exempt on a leg that keys class twins;
  ABCE3.SA, ABCF3.SA  two SAO lines sharing three characters, not four, 3 windows: listed;
  ABCD, ABCDE         two lines off SAO sharing four characters, 3 windows: listed;
  NATU3.SA, NTCO3.SA  NTCO3.SA removed as a known line, 5 windows: exempt;
  AKN1, ZKN1          AKN1 removed as a known line, first in the pair, 3 windows: exempt;
  ARZZ3.SA, AZZA3.SA  9 windows, all before AZZA3.SA's first own day: exempt;
  OLDL, NEWL          2 windows before NEWL's first own day and 3 after: listed;
  PRE1, PRE2          2 windows before PRE2's first own day and 2 after: exempt;
  STA0, STA1          3 windows, the first on STA1's first own day: listed;
  COG, CTRA           3 windows, all after COG's last own day: exempt;
  END0, END1          3 windows, the last on END1's last own day: listed;
  VOL1, VOL2          the same close in 3 windows on different volumes: not a match;
  GAP1, GAP2          the same bar in 3 windows, 12 days after each rebalance date: outside the window, no match;
  DEAD1, DEAD2        the same close in 4 windows with no volume: not a match.
Leg two (no universe_rules block) holds PETR3.SA and PETR4.SA's 3 windows, listed there, and LEG2A, which copies
leg one's AAA: lines of two legs are never paired. Leg three holds only the NVDR twins. main() exits 1 on legs one
and two and 0 on leg three, and --fetch builds the same bars file from one stock_eod query on an in-memory
warehouse (test_split_check's). Every mutant of same_bars_scan.py (each exemption and its scope, the one bar a
day, the zero-volume rule, the volume match, the 3, the window's first and last day, the exit code) must trip a
check.

Usage:
    python scripts/test_same_bars_scan.py [repo root]
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import types

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "scripts", "same_bars_scan.py")
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from split_check import plus   # noqa: E402
from test_split_check import Warehouse, fake_cr_client   # noqa: E402

DATES = [f"{y}-07-01" for y in range(2015, 2025)]
LEG1 = ["AAA", "BBB", "NEAR1", "NEAR2", "XYZ.BK", "XYZ-R.BK", "PPP.BK", "QQQ-R.BK", "PETR3.SA", "PETR4.SA",
        "ABCE3.SA", "ABCF3.SA", "ABCD", "ABCDE", "NATU3.SA", "NTCO3.SA", "AKN1", "ZKN1", "ARZZ3.SA", "AZZA3.SA",
        "OLDL", "NEWL", "PRE1", "PRE2", "STA0", "STA1", "COG", "CTRA", "END0", "END1", "VOL1", "VOL2", "GAP1", "GAP2",
        "DEAD1", "DEAD2"]
LEG2 = ["PETR3.SA", "PETR4.SA", "LEG2A"]
LEG3 = ["XYZ.BK", "XYZ-R.BK"]
# line: (the line it copies, the windows it copies, as indexes into DATES)
COPIES = {"BBB": ("AAA", (0, 1, 2)), "NEAR2": ("NEAR1", (3, 4)), "XYZ-R.BK": ("XYZ.BK", (0, 1, 2, 3)),
          "QQQ-R.BK": ("PPP.BK", (0, 1, 2)), "PETR4.SA": ("PETR3.SA", (5, 6, 7)),
          "ABCF3.SA": ("ABCE3.SA", (6, 7, 8)), "ABCDE": ("ABCD", (3, 4, 5)),
          "NTCO3.SA": ("NATU3.SA", (0, 1, 2, 3, 4)), "ZKN1": ("AKN1", (0, 1, 2)),
          "AZZA3.SA": ("ARZZ3.SA", tuple(range(9))), "NEWL": ("OLDL", (1, 2, 5, 6, 7)),
          "PRE2": ("PRE1", (3, 4, 5, 6)), "STA1": ("STA0", (4, 5, 6)), "CTRA": ("COG", (7, 8, 9)),
          "END1": ("END0", (4, 5, 6)), "VOL2": ("VOL1", (0, 1, 2)), "GAP2": ("GAP1", (0, 1, 2)),
          "DEAD2": ("DEAD1", (0, 1, 2, 3)), "LEG2A": ("AAA", (0, 1, 2))}
CLOSE_ONLY = {"VOL2"}   # copies its source's close on its own volume
LATE_BARS = {"GAP1", "GAP2"}   # every bar after a rebalance date is 12 days or more after it
QUIET = {("DEAD1", w) for w in range(4)}   # no volume on DEAD1's first bar in windows 0 to 3, so DEAD2's either
DUPLICATES = [("BBB", ["2015-07-02", 0.5, 77, 0.4])]   # a second bar on BBB's first window bar's date
UR1 = {"version": "ur-3",
       "ordinary_shares": {"removed": [{"symbol": "NTCO3.SA", "reason": "known", "name": "a renamed line"},
                                       {"symbol": "AKN1", "reason": "known", "name": "another renamed line"},
                                       {"symbol": "OLDL", "reason": "name_term", "name": "another reason"}]},
       "class_stand_ins": {"by_period": [{"rebalance_date": "2020-07-01", "symbols": ["PETR4.SA"]}]},
       "line_starts": {"lines": [{"symbol": "AZZA3.SA", "first_day": "2024-08-27"},
                                 {"symbol": "NEWL", "first_day": "2020-01-01"},
                                 {"symbol": "PRE2", "first_day": "2020-01-01"},
                                 {"symbol": "STA1", "first_day": "2019-07-02"}]},   # window 4's first bar
       "line_ends": {"lines": [{"symbol": "COG", "last_day": "2021-10-01"},
                               {"symbol": "END1", "last_day": "2021-07-02"}]}}   # window 6's first bar
WANT = {   # leg: (pairs listed, {exempt pair: reason prefix})
    "LEG1": (sorted([("AAA", "BBB"), ("NEWL", "OLDL"), ("PPP.BK", "QQQ-R.BK"), ("ABCE3.SA", "ABCF3.SA"),
                     ("ABCD", "ABCDE"), ("STA0", "STA1"), ("END0", "END1")]),
             {("XYZ-R.BK", "XYZ.BK"): "check N twin", ("PETR3.SA", "PETR4.SA"): "class twin",
              ("NATU3.SA", "NTCO3.SA"): "known list", ("AKN1", "ZKN1"): "known list",
              ("ARZZ3.SA", "AZZA3.SA"): "line start or end", ("PRE1", "PRE2"): "line start or end",
              ("COG", "CTRA"): "line start or end"}),
    "LEG2": ([("PETR3.SA", "PETR4.SA")], {}),
    "LEG3": ([], {("XYZ-R.BK", "XYZ.BK"): "check N twin"}),
}


def own_bar(line, w):
    i = (LEG1 + ["LEG2A"]).index(line)
    return round(1 + i * 1.13 + w * 0.07, 2), 1000 + 7 * i + w


def bars():
    """{symbol: [[date, close, volume, adjClose], ...]} by row: per window the shared bar on the rebalance date, the
    first bar (a copy's equals its source's) and a later one, adjClose equal to the close, and the duplicates."""
    out = {}
    for line in LEG1 + ["LEG2A"]:
        src, wins = COPIES.get(line, (line, ()))
        first = 12 if line in LATE_BARS else 1
        rows = []
        for w, d in enumerate(DATES):
            c, v = own_bar(src if w in wins else line, w)
            if line in CLOSE_ONLY:
                v = own_bar(line, w)[1]
            if (line, w) in QUIET or (src, w) in QUIET and w in wins:
                v = 0
            rows += [[d, 99.0, 5, 99.0], [plus(d, first), c, v, c], [plus(d, first + 2), c + 0.5, v + 1, c + 0.5]]
        out[line] = sorted(rows + [list(r) for s, r in DUPLICATES if s == line])
    return out


def leg(universe, lines, ur=None):
    rows = [{"rebalance_date": d, "exit_date": plus(d, 365), "portfolio_return": 0.0, "spy_return": 0.0,
             "stocks_held": len(lines), "holdings": ",".join(lines)} for d in DATES]
    res = {"universe": universe, "frequency": "annual", "period_data": rows}
    if ur is not None:
        res["universe_rules"] = ur
    return res


LEGS = {"LEG1": leg("LEG1", LEG1, UR1), "LEG2": leg("LEG2", LEG2), "LEG3": leg("LEG3", LEG3)}


def pair_failures(mod, bar_file):
    out = []
    for name, res in LEGS.items():
        flagged, exempt = mod.pairs(res, bar_file)
        got = sorted(tuple(p["pair"]) for p in flagged)
        want_flag, want_ex = WANT[name]
        if got != want_flag:
            out.append(f"{name}: pairs listed {got}, want {want_flag}")
        got_ex = {tuple(p["pair"]): p["reason"] for p in exempt}
        if sorted(got_ex) != sorted(want_ex) or any(not got_ex[k].startswith(r) for k, r in want_ex.items()
                                                    if k in got_ex):
            out.append(f"{name}: exempt {got_ex}, want {want_ex}")
    return out


def run_main(mod, names, bar_path, fetch=None):
    """main() on the named legs' files, with --fetch through the fetch warehouse when one is given: (exit code, its
    JSON record)."""
    with fake_cr_client(fetch), tempfile.TemporaryDirectory() as tmp:
        files = []
        for n in names:
            files.append(os.path.join(tmp, f"{n}.json"))
            with open(files[-1], "w") as fh:
                json.dump(LEGS[n], fh)
        out = os.path.join(tmp, "out.json")
        args = files + ["--bars", bar_path, "--json", out] + (["--fetch"] if fetch is not None else [])
        with contextlib.redirect_stdout(io.StringIO()):
            code = mod.main(args)
        with open(out) as fh:
            return code, json.load(fh)


def main_failures(mod, bar_file):
    out = []
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "bars.json")
        with open(path, "w") as fh:
            json.dump(bar_file, fh)
        code, rec = run_main(mod, ["LEG1", "LEG2"], path)
        if code != 1 or sorted(rec.get("legs", {})) != ["LEG1", "LEG2"]:
            out.append(f"main on legs one and two: exit {code}, legs {sorted(rec.get('legs', {}))}; want 1")
        code, _ = run_main(mod, ["LEG3"], path)
        if code != 0:
            out.append(f"main on leg three: exit {code}, want 0")
        wh = Warehouse([{"symbol": s, "date": d, "close": c, "adjClose": a, "volume": v}
                        for s, rows in bar_file.items() for d, c, v, a in rows])
        fetched_path = os.path.join(tmp, "fetched.json")
        code, rec2 = run_main(mod, ["LEG1", "LEG2"], fetched_path, fetch=wh)
        if len(wh.sqls) != 1:
            out.append(f"--fetch sent {len(wh.sqls)} queries, want 1")
        unfiled = lambda r: {k: {f: v for f, v in x.items() if f != "file"} for k, x in r.get("legs", {}).items()}
        if code != 1 or not unfiled(rec) or unfiled(rec2) != unfiled(rec):
            out.append("--fetch's bars file gives another result than the bars it was built from")
        with open(fetched_path) as fh:
            fetched = json.load(fh)
        if any(d in DATES for rows in fetched.values() for d, *_ in rows):
            out.append("--fetch read bars on a rebalance date, outside the window")
        on = lambda rows: sorted(r for r in rows if r[0] == DUPLICATES[0][1][0])
        if on(fetched.get("BBB", [])) != on(bar_file["BBB"]):
            out.append("--fetch's bars file holds other BBB rows, or other adjClose, on its duplicated date")
    try:
        mod.fetch_sql([leg("BAD", ["BAD;X"])])
        out.append("a symbol holding a semicolon reached the SQL")
    except ValueError:
        pass
    return out


def failures(src):
    try:
        mod = types.ModuleType("same_bars_scan_under_test")
        mod.__file__ = PATH
        exec(compile(src, PATH, "exec"), mod.__dict__)
        bar_file = bars()
        return pair_failures(mod, bar_file) + main_failures(mod, bar_file)
    except Exception as e:   # reported as an error: main() does not count it as a caught mutant
        return [f"error: {type(e).__name__}: {e}"]


# name: [(old, new), ...]; every `old` occurs once and only once in the source
MUTANTS = {
    "no check N exemption": [('return "check N twin"', "return None")],
    "an NVDR line exempt beside any .BK line": [('y == x[:-len("-R.BK")] + ".BK"', 'y.endswith(".BK")')],
    "no class-twin exemption": [("if class_twins and", "if False and")],
    "class twins exempt on a leg that doesn't key them": [("if class_twins and", "if True and")],
    "class twins on any exchange": [('return symbol[:4] if symbol.endswith(".SA") else None', "return symbol[:4]")],
    "a three-character issuer key": [('return symbol[:4] if symbol.endswith(".SA") else None',
                                      'return symbol[:3] if symbol.endswith(".SA") else None')],
    "no known-list exemption": [('"known list" if a in known or b in known else None', "None")],
    "the known list read on a pair's second line only": [('"known list" if a in known or b in known else None',
                                                          '"known list" if b in known else None')],
    "line starts ignored": [('starts.get(x, "") <= dx', '"" <= dx')],
    "a line start exempts every window": [('starts.get(x, "") <= dx', 'x not in starts and "" <= dx')],
    "a match on a line start's first own day not counted": [('starts.get(x, "") <= dx', 'starts.get(x, "") < dx')],
    "line ends ignored": [('dx <= ends.get(x, "9999-12-31")', 'dx <= "9999-12-31"')],
    "a match on a line end's last own day not counted": [('dx <= ends.get(x, "9999-12-31")',
                                                          'dx < ends.get(x, "9999-12-31")')],
    "one own window lists a pair": [("elif len(own) < MIN_WINDOWS:", "elif not own:")],
    "every bar row kept": [("in one_per_day(", "in list(")],
    "zero-volume windows compared": [("if b is not None and b[2]:", "if b is not None:")],
    "volume not compared": [("same[(cents, vol)].append(s)", "same[cents].append(s)")],
    "two windows list a pair": [("MIN_WINDOWS = 3", "MIN_WINDOWS = 2")],
    "four windows needed": [("MIN_WINDOWS = 3", "MIN_WINDOWS = 4")],
    "the rebalance date's bar read as the window's first": [("d < b[0] <= hi", "d <= b[0] <= hi")],
    "the first window bar with no 11-day bound": [("if d < b[0] <= hi), None)", "if d < b[0]), None)")],
    "exit 0 with a pair left": [("return 1 if left else 0", "return 0")],
}


def main():
    if not os.path.exists(PATH):
        print(f"test_same_bars_scan: FAIL ({PATH} does not exist)")
        sys.exit(1)
    with open(PATH) as fh:
        src = fh.read()
    base = failures(src)
    if base:
        print("test_same_bars_scan: FAIL")
        for f in base:
            print("  ", f)
        sys.exit(1)
    survived, broken = [], []
    for name, edits in MUTANTS.items():
        m = src
        for old, new in edits:
            if src.count(old) != 1:
                print(f"test_same_bars_scan: FAIL (mutant anchor not unique: {name})")
                sys.exit(1)
            m = m.replace(old, new)
        got = failures(m)
        if not got:
            survived.append(name)
        elif all(f.startswith("error:") for f in got):
            broken.append((name, got))
    if survived or broken:
        print("test_same_bars_scan: FAIL (mutants survived:", survived, "; mutants that only raised:", broken, ")")
        sys.exit(1)
    print(f"test_same_bars_scan: PASS ({len(LEGS)} legs, {len(COPIES)} copied lines, {len(DATES)} windows; "
          f"main's exit codes and --fetch; {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
