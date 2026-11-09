#!/usr/bin/env python3
"""The three topics on the run window, end to end on test_data_checks.py's synthetic SET leg.

  1 BYTES    each topic at the base ref and at HEAD with --end-year 2025 writes the same results files,
             byte for byte: drop, ltp, both and ltp with full holdings through run_single, and ltp
             through main() (--exchange SET, CetaResearch patched to the fake client). The base runs
             in a subprocess on its own tree (its .py files archived to a temp dir) and its own
             test_data_checks.py, with the shared modules checked to load from there
  2 WINDOW   with FY2025 rows and 2026 bars to 2026-11-10 added, each topic runs pinned to 2026-11-06
             and through RunWindow.latest (run day 2026-11-09):
               the leg's keys and values, every row marked and only the last partial
               the partial row's window exits on the end date, and the benchmark's too
               a held name that stops trading before the end date (and trades again after it) fills
               under ltp from its entry bar to its last bar before the end date, and drop drops it
               a zero-volume line on the end date takes the missing-exit path
               annual_returns hold full years, partial_year closes the identity, CAGR uses span_years
               drop, ltp and both pass gate_ltp's stage-a and the results scanners
               latest equals the pin apart from end_date_requested and the data_end keys
               main() takes --end-date; a run held in cash (too few screened, or too few buyable at
               entry) marks its partial row too; the data checks read the end date's bars
  3 SCRIPTS  the moved topics call no generate_rebalance_dates
  4 MUTANTS  each wiring of the run window in defensive-quality, broken in a copy, fails a check

Usage:  python scripts/test_latest_window_topics.py [--base REF]   (REF defaults to the merge base with origin/main)
"""
import argparse
import ast
import contextlib
import importlib.util
import io
import json
import math
import os
import subprocess
import sys
import tarfile
import tempfile
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import data_utils as du   # noqa: E402
import scan_results_invariant as sri   # noqa: E402
import test_data_checks as tdc   # noqa: E402
from gate_ltp import check_stage_a, first_diff   # noqa: E402

TOPICS = ("defensive-quality", "cyclical-timing", "deleveraging")
FREQ = {"deleveraging": "quarterly"}
END, TODAY = date(2026, 11, 6), date(2026, 11, 9)
HALT, ZEROV = "A03.BK", "A04.BK"   # stops trading after 2026-10-30 (back on 11-09); no volume on END
LEGS = {"SET": (["SET"], "^SET.BK")}
PARTIAL = {   # the partial period each topic's grid gives on END
    "annual": {"rebalance_date": "2026-07-01", "entry_day": "2026-07-02", "exit_date": "2026-11-06",
               "days": 127, "period_days": 365, "fraction": 0.347945},
    "quarterly": {"rebalance_date": "2026-10-01", "entry_day": "2026-10-02", "exit_date": "2026-11-06",
                  "days": 35, "period_days": 92, "fraction": 0.380435},
}
PROVENANCE = ("end_date_requested", "data_end", "data_end_by_market", "data_end_checked_at")
FAIL = []


def check(cond, what):
    if not cond:
        FAIL.append(what)
        print(f"  FAIL {what}")
    return cond


# ---------------------------------------------------------------- 1 bytes

RUNNER = r'''
import contextlib, io, os, sys
tree, out = sys.argv[1], sys.argv[2]
sys.path[:0] = [tree, os.path.join(tree, "scripts")]
import data_utils, metrics
import test_data_checks as tdc
for m in (data_utils, metrics, tdc):
    assert os.path.realpath(m.__file__).startswith(os.path.realpath(tree)), m.__file__
head = hasattr(data_utils, "RunWindow")
synthetic = tdc.synthetic_source()
for topic, src in (("defensive-quality", tdc.defensive_source()), ("cyclical-timing", synthetic),
                   ("deleveraging", synthetic)):
    for run_mode, full in (("drop", False), ("ltp", False), ("both", False), ("ltp", True)):
        tdc.run_topic(topic, src, run_mode, "on", out, full_holdings=full)
    mod = tdc.load_topic(topic)
    mod.CetaResearch = lambda src=src, **kw: tdc.FakeClient(src)
    sys.argv = ["main", "--exchange", "SET", "--output", os.path.join(out, topic + ".main.json"),
                "--exit-method", "ltp"] + (["--end-year", "2025"] if head else [])
    with contextlib.redirect_stdout(io.StringIO()):
        mod.main()
'''


def bytes_failures(ref, tmp):
    """Base and HEAD write the same files, byte for byte."""
    f = []
    base = os.path.join(tmp, "base")
    os.makedirs(base)
    tar = subprocess.run(["git", "archive", "--format=tar", ref, "--", "*.py"], cwd=ROOT, capture_output=True,
                         check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(tar)) as t:
        t.extractall(base, filter="data")
    runner = os.path.join(tmp, "runner.py")
    with open(runner, "w") as fh:
        fh.write(RUNNER)
    env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", ""), "PYTHONHASHSEED": "0"}
    outs, procs = {}, {}
    for side, tree in (("base", base), ("head", ROOT)):
        outs[side] = os.path.join(tmp, f"out_{side}")
        os.makedirs(outs[side])
        procs[side] = subprocess.Popen([sys.executable, runner, tree, outs[side]], cwd=tmp, env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for side, p in procs.items():
        log, _ = p.communicate(timeout=1800)
        if p.returncode:
            return [f"the {side} runs failed: {log[-2000:]}"]
    names = sorted(os.listdir(outs["base"]))
    if names != sorted(os.listdir(outs["head"])) or len(names) != 15:
        return [f"files differ: base {names}, head {sorted(os.listdir(outs['head']))}"]
    for n in names:
        a, b = (open(os.path.join(outs[s], n), "rb").read() for s in ("base", "head"))
        if a != b:
            f.append(f"{n} differs: {first_diff(json.loads(a), json.loads(b))}")
    return f


# ---------------------------------------------------------------- 2 window

def extend(src):
    """FY2025 rows (copies of FY2024 with revenue up 5% and D/E down 15%, so every signal still fires) and
    2026 bars: the quarterly grid windows, then every weekday from 2026-10-13 to 2026-11-10."""
    e25 = tdc.ep(date(2025, 12, 31))
    tweak = {"income_statement": {"revenue": "revenue * 1.05", "filingDate": "'2026-03-01'"},
             "financial_ratios": {"debtToEquityRatio": "debtToEquityRatio * 0.85"}}
    tables = {r[0] for r in src.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    for t in ("key_metrics", "income_statement", "financial_ratios", "cash_flow_statement"):
        if t in tables:
            rep = {"date": "'2025-12-31'", "dateEpoch": str(e25), **tweak.get(t, {})}
            src.execute(f"INSERT INTO {t} SELECT * REPLACE ({', '.join(f'{v} AS {k}' for k, v in rep.items())})"
                        f" FROM {t} WHERE date = '2024-12-31'")
    syms = [r[0] for r in src.execute("SELECT DISTINCT symbol FROM stock_eod WHERE date >= '2025-10-01'"
                                      " ORDER BY 1").fetchall()]
    days = sorted({d for g in du.generate_rebalance_dates(2026, 2026, "quarterly")
                   for d in tdc.weekdays(g, g + timedelta(days=11))}
                  | set(tdc.weekdays(date(2026, 10, 13), date(2026, 11, 10))))
    rows = []
    for day in days:
        for s in syms:
            if s == "^SET.BK":
                p, v = 1000.0 + day.toordinal() % 97, 0.0
            elif s == "SPY":
                p, v = 300.0 + day.toordinal() % 89, 1e7
            elif s == HALT and date(2026, 10, 30) < day <= END:
                continue
            else:
                p, v = tdc.price(s, day), (0.0 if s == ZEROV and day == END else 1e5)
            rows.append((s, day.isoformat(), tdc.ep(day), p, p, v))
    src.executemany("INSERT INTO stock_eod VALUES (?, ?, ?, ?, ?, ?)", rows)
    return src


def sources():
    synthetic = extend(tdc.synthetic_source())
    return {"defensive-quality": extend(tdc.defensive_source()), "cyclical-timing": synthetic, "deleveraging": synthetic}


_WINDOWS = {}   # one window per fixture and kind: run_single copies it (for_leg) before grid() changes it


def window(src, kind):
    key = (id(src), kind)
    if key not in _WINDOWS:
        client = tdc.FakeClient(src)
        with contextlib.redirect_stderr(io.StringIO()):
            _WINDOWS[key] = (du.RunWindow.pinned(client, LEGS, END, today=TODAY) if kind == "pin"
                             else du.RunWindow.latest(client, LEGS, today=TODAY))
    return _WINDOWS[key]


def run(mod, topic, src, run_mode, win, tmp, name, full=True):
    return tdc.run_topic(topic, src, run_mode, "on", tmp, full_holdings=full, win=win, tag=f".{name}", mod=mod)[0]


def bench_close(src, d):
    return src.execute("SELECT adjClose FROM stock_eod WHERE symbol = '^SET.BK' AND date = ?",
                       [d.isoformat()]).fetchone()[0]


def leg_failures(topic, r, src):
    """The interface's keys and the partial row on a pinned file r (ltp, full holdings)."""
    f = []
    ppy = {"annual": 1, "quarterly": 4}[FREQ.get(topic, "annual")]
    part = PARTIAL[FREQ.get(topic, "annual")]
    n = r["n_periods"]
    want = {"end_date_requested": "2026-11-06", "end_date": "2026-11-06", "priced_to": "2026-11-06", "data_end": None,
            "data_end_by_market": None, "data_end_checked_at": None, "full_periods": n - 1,
            "span_years": round((n - 1 + part["fraction"]) / ppy, 6), "partial_period": part,
            "measured_end": "2026-11-06", "requested_end": "2026-11-06"}
    for k, v in want.items():
        if r.get(k) != v:
            f.append(f"{topic}: {k} {r.get(k)!r}, want {v!r}")
    if r.get("years") != round(want["span_years"], 1):
        f.append(f"{topic}: years {r.get('years')}, want {round(want['span_years'], 1)}")
    rows = r["period_data"]
    if [x.get("partial") for x in rows] != [False] * (len(rows) - 1) + [True]:
        f.append(f"{topic}: partial flags {[x.get('partial') for x in rows][-3:]}, not every row marked")
    last = rows[-1]
    if last.get("fraction") != part["fraction"] or last["rebalance_date"] != part["rebalance_date"]:
        f.append(f"{topic}: last row {last['rebalance_date']} fraction {last.get('fraction')}")
    entry = date.fromisoformat(part["entry_day"])
    want_b = round(bench_close(src, END) / bench_close(src, entry) - 1, 6)
    if last["spy_return"] is None or abs(last["spy_return"] - want_b) > 1e-6:
        f.append(f"{topic}: partial benchmark return {last['spy_return']}, want the close on the end date {want_b}")
    held = {h["symbol"]: h for h in last.get("holdings_full") or []}
    if HALT not in held or ZEROV not in held:
        return f + [f"{topic}: {HALT} or {ZEROV} not held in the partial period: the fixture's premise"]
    for s, h in held.items():
        if s in (HALT, ZEROV):
            continue
        if (h["exit"], h["exit_bar"]) != ("window", "2026-11-06"):
            f.append(f"{topic}: {s} exits {h['exit']} on {h['exit_bar']}, want the window bar on the end date")
    # the data checks read the end date's bar too: ZEROV's no-volume bar there is a lost exit
    if (last.get("exits_lost") or 0) < 1:
        f.append(f"{topic}: the partial row's exits_lost {last.get('exits_lost')}: T's end-date window not read")
    fills = {x["symbol"]: x for x in r["ltp"]["fills"] if x["entry_date"] == part["rebalance_date"]}
    for s, last_bar in ((HALT, "2026-10-30"), (ZEROV, "2026-11-05")):
        x = fills.get(s)
        if not x or (x["entry_bar"], x["last_bar"]) != (part["entry_day"], last_bar):
            f.append(f"{topic}: {s} fill {x and (x['entry_bar'], x['last_bar'])}, want ({part['entry_day']}, {last_bar})")
        if held[s]["exit"] != "last_traded":
            f.append(f"{topic}: {s} exits from the {held[s]['exit']}, want its last traded bar")
    ann, py = r["annual_returns"], r["partial_year"]
    if py is None or py["year"] != 2026 or any(a["year"] >= 2026 for a in ann) or py["end"] != "2026-11-06":
        return f + [f"{topic}: partial_year {py}, annual_returns to {ann[-1]['year'] if ann else None}"]
    for side, key in (("portfolio", "portfolio"), ("spy", "spy")):
        g = math.prod(1 + a[key] / 100 for a in ann) * (1 + py[key] / 100)
        if not math.isclose(g, 1 + r[side]["total_return"] / 100, rel_tol=1e-3):
            f.append(f"{topic}: {side} annual rows x partial_year {(g - 1) * 100:.2f}, total {r[side]['total_return']}")
        cagr = ((1 + r[side]["total_return"] / 100) ** (1 / r["span_years"]) - 1) * 100
        if abs(cagr - r[side]["cagr"]) > 0.011:
            f.append(f"{topic}: {side} CAGR {r[side]['cagr']}, want {cagr:.2f} over span_years")
    return f


def window_failures(topic, mod_of, src, tmp, tag="", full_check=True):
    """mod_of(): a fresh topic module. The pinned runs' checks, and (full_check) latest, main() and cash.
    Without full_check (a mutant) it stops at the ltp run's first failure."""
    f = []
    runs = {"ltp": run(mod_of(), topic, src, "ltp", window(src, "pin"), tmp, f"pin.ltp{tag}")}
    f += leg_failures(topic, runs["ltp"], src)
    if f and not full_check:
        return f
    runs.update({m: run(mod_of(), topic, src, m, window(src, "pin"), tmp, f"pin.{m}{tag}") for m in ("drop", "both")})
    held = {h["symbol"] for h in runs["drop"]["period_data"][-1].get("holdings_full") or []}
    if held & {HALT, ZEROV}:
        f.append(f"{topic}: drop books {sorted(held & {HALT, ZEROV})} in the partial period")
    g = {"topic": topic, "pairs": [{"published_cagr_path": "portfolio.cagr", "ltp_block_path": "ltp"}]}
    _, probs = check_stage_a(g, runs["both"], runs["drop"], runs["ltp"], with_ltp=True)
    f += [f"{topic} stage-a: {p}" for p in probs]
    hits, counts = [], {}
    sri.walk(runs["drop"], "", topic, hits, counts, False)
    sri.funnel_scan(runs["drop"], "", topic, hits, {})
    if hits or counts.get("nodes") != 1:
        f.append(f"{topic}: scanners {counts} {hits}")
    if not full_check:
        return f
    latest = run(mod_of(), topic, src, "ltp", window(src, "latest"), tmp, "latest")
    if (latest["data_end"], latest["data_end_by_market"], latest["end_date_requested"]) != \
            ("2026-11-06", {"SET": "2026-11-06"}, "latest") or not latest["data_end_checked_at"]:
        f.append(f"{topic}: latest provenance {[latest.get(k) for k in PROVENANCE]}")
    strip = lambda x: {k: v for k, v in x.items() if k not in PROVENANCE}
    if strip(latest) != strip(runs["ltp"]):
        f.append(f"{topic}: latest differs from the pin: {first_diff(strip(latest), strip(runs['ltp']))}")
    mod = mod_of()
    out = os.path.join(tmp, f"{topic}.main.json")
    mod.CetaResearch = lambda **kw: tdc.FakeClient(src)
    argv = sys.argv
    sys.argv = ["main", "--exchange", "SET", "--end-date", "2026-11-06", "--output", out, "--exit-method", "ltp"]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            mod.main()
    finally:
        sys.argv = argv
    m = json.load(open(out))
    if (m.get("end_date"), m.get("end_date_requested"), m.get("partial_period")) != \
            ("2026-11-06", "2026-11-06", PARTIAL[FREQ.get(topic, "annual")]):
        f.append(f"{topic}: main() --end-date: {m.get('end_date')} {m.get('partial_period')}")
    return f + cash_failures(topic, mod_of, src, tmp)


def cash_failures(topic, mod_of, src, tmp, tag=""):
    """A run held in cash marks its partial row too, through both cash branches: too few names screened
    (MIN_STOCKS raised), and too few buyable at entry (entry_buyable replaced), whose data checks read the
    end date's bars."""
    f = []
    for name, patch in (("cash.screen", {"MIN_STOCKS": 10 ** 6}),
                        ("cash.buyable", {"entry_buyable": lambda *a, **k: 0})):
        mod = mod_of()
        for k, v in patch.items():
            setattr(mod, k, v)
        c = run(mod, topic, src, "ltp", window(src, "pin"), tmp, name + tag, full=False)
        last = c["period_data"][-1]
        if (last.get("partial"), last.get("fraction"), last["stocks_held"], c["full_periods"]) != \
                (True, PARTIAL[FREQ.get(topic, "annual")]["fraction"], 0, c["n_periods"] - 1):
            f.append(f"{topic} {name}: partial row {last.get('partial')} {last.get('fraction')} {last['stocks_held']}")
    return f


# ---------------------------------------------------------------- 3 scripts

def scripts_failures():
    f = []
    for t in TOPICS:
        tree = ast.parse(open(tdc.load_topic(t).__file__).read())
        calls = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", getattr(n.func, "attr", None)) == "generate_rebalance_dates"]
        if calls:
            f.append(f"{t} calls generate_rebalance_dates at lines {calls}")
    return f


# ---------------------------------------------------------------- 4 mutants

MUTANTS = [   # (name, old text, new text) in defensive-quality's script: each must fail a window check
    ("benchmark exit priced next day", "offset_days=offset_days, exit_offset_days=x_off)", "offset_days=offset_days)"),
    ("stock exits priced next day", "exit_prices = get_prices(con, symbols, exit_date, offset_days=x_off)",
     "exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)"),
    ("rows unmarked", "    win.mark(results)\n", "\n"),
    ("fetch past the end date", "end = win.fetch_end(d, 11)", "end = d + timedelta(days=11)"),
    ("data checks read the next day", "held=True,\n                  exit_offset_days=x_off)", "held=True)"),
    ("the cash branch's data checks read the next day", "held=False,\n                      exit_offset_days=x_off)",
     "held=False)"),
    ("CAGR over the period count", "risk_free_rate=risk_free_rate, fractions=fractions)",
     "risk_free_rate=risk_free_rate)"),
    ("keys not written", "**win.fields(con, benchmark_symbol, valid, periods_per_year, partial_year, offset_days),", ""),
]


def mutant_failures(src, tmp):
    f = []
    path = tdc.load_topic("defensive-quality").__file__
    text = open(path).read()
    for i, (name, old, new) in enumerate(MUTANTS):
        if text.count(old) != 1:
            f.append(f"mutant {name}: anchor found {text.count(old)} times")
            continue
        d = os.path.join(tmp, f"mut{i}", "defensive-quality")
        os.makedirs(d)
        mpath = os.path.join(d, os.path.basename(path))
        with open(mpath, "w") as fh:
            fh.write(text.replace(old, new))

        def mod_of(mpath=mpath, i=i):
            spec = importlib.util.spec_from_file_location(f"dq_mut{i}", mpath)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
        try:
            caught = (window_failures("defensive-quality", mod_of, src, tmp, tag=f".m{i}", full_check=False)
                      or cash_failures("defensive-quality", mod_of, src, tmp, tag=f".m{i}"))
        except Exception:   # a crash is a caught mutant
            caught = ["crashed"]
        if not caught:
            f.append(f"mutant {name} passes every window check")
    return f


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None)
    a = ap.parse_args()
    ref = a.base or tdc.merge_base()
    print(f"base {ref[:12]}")
    with tempfile.TemporaryDirectory() as tmp:
        print("1 BYTES")
        for x in bytes_failures(ref, tmp):
            check(False, x)
        print("2 WINDOW")
        srcs = sources()
        for t in TOPICS:
            try:
                for x in window_failures(t, lambda t=t: tdc.load_topic(t), srcs[t], tmp):
                    check(False, x)
            except Exception as e:
                check(False, f"{t}: {type(e).__name__}: {e}")
        print("3 SCRIPTS")
        for x in scripts_failures():
            check(False, x)
        print("4 MUTANTS")
        for x in mutant_failures(srcs["defensive-quality"], tmp):
            check(False, x)
    print("FAIL" if FAIL else "PASS", f"({len(FAIL)} failures)")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
