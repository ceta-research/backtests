#!/usr/bin/env python3
"""Gate for the --exit-method wiring (B005 last-traded-price exits).

Runs topic backtests one market leg at a time and checks the JSON they write. --check picks the
checks (comma list, default stage-a):
  stage-a  both run: every published CAGR equals its LTP block's drop_portfolio.cagr, fill counts
           are consistent. drop run: no LTP key anywhere, equals the both run minus LTP keys.
           Whenever the runs include ltp (--with-ltp, --det-mode ltp, flip once the default is ltp),
           its published CAGR must equal the both run's ltp_portfolio.cagr.
  det      the same leg run twice in --det-mode gives identical JSON.
  flip     a run without --exit-method equals an explicit --exit-method DEFAULT_EXIT_METHOD run
           (labels and equality only: only the stage-a ltp-vs-both check proves the LTP book).
Every mode except `default` passes --exit-method explicitly. At most two backtests run on the
machine at once, counting other people's. How to run, file layout, invariants: scripts/LTP_GATE.md.
Offline tests of the comparison functions: scripts/test_gate_ltp.py.

usage: gate_ltp.py plan|run|report LEGS.json [--check stage-a,det,flip] [--topic T,..] [--leg L,..]
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from data_utils import DEFAULT_EXIT_METHOD, EXIT_METHODS  # noqa: E402

OUT_DIR = os.path.join(REPO, "results", "ltp_gate")   # gitignored (results/)
MACHINE_LIMIT = 2   # backtests on this machine at once, ours or anyone else's
PAT = r"^[^ ]*[Pp]ython[0-9.]*( -[^ ]+)* [^ ]*backtest\.py"
CHECKS = ("stage-a", "det", "flip")
VOLATILE = ()   # keys stripped before the det/flip compare; no gated backtest.py writes a run-time field
TRANSIENT = re.compile(r"magic bytes|Rate limited|timed out|failed \((429|5\d\d)\)|File still preparing"
                       r"|ConnectionError|ConnectionResetError|RemoteDisconnected|ReadTimeout|IncompleteRead")
lock = threading.Lock()


def running():
    return len(subprocess.run(["pgrep", "-f", PAT], capture_output=True, text=True).stdout.split())


# Topics whose single-market mode ignores the --output file name and writes
# <dirname>/<fixed name> with CAGR as a fraction (save_results helper).
ADAPT = {"capex-efficiency": "capex-efficiency_metrics_{leg}.json"}


def adapt(topic, leg_args, final):
    """Synthesize the standard gate file from a save_results-style output dir (legs use --exchange)."""
    d = final[:-5] + ".d"
    src = os.path.join(d, ADAPT[topic].format(leg=leg_args[-1].replace(",", "_")))
    if not os.path.exists(src):
        return False
    m = json.load(open(src))
    out = {"portfolio": {"cagr": round(m["portfolio"]["cagr"] * 100, 2)}}
    out.update({k: m[k] for k in ("exit_method", "ltp") if k in m})
    csvs = sorted(f for f in os.listdir(d) if f.endswith(".csv"))
    out["csv_header"] = {h: 0 for f in csvs for h in open(os.path.join(d, f)).readline().strip().split(",")}
    # the whole run, for the det/flip compare (stage A skips the full drop-vs-both compare here)
    out["metrics"] = {k: v for k, v in m.items() if k not in ("exit_method", "ltp")}
    out["csv"] = {f: open(os.path.join(d, f)).read() for f in csvs}
    json.dump(out, open(final, "w"))
    return True


# ---------------------------------------------------------------- legs and runs

def load_legs(path, topics=None, legs=None):
    """Flat [{topic, leg, leg_args, pairs}]. LEGS.json is {topic: {pairs, legs: {name: args}}}
    (keys starting with _ are notes) or the stage A flat list [{topic, leg_args, pairs, leg?}]."""
    d = json.load(open(path))
    if isinstance(d, dict):
        d = [{"topic": t, "leg": name, "leg_args": args, "pairs": spec["pairs"]}
             for t, spec in d.items() if not t.startswith("_") for name, args in spec["legs"].items()]
    seen = set()
    for g in d:
        g.setdefault("leg", g["leg_args"][-1].replace(",", "_") if g["leg_args"] else "default")
        if not re.fullmatch(r"[A-Za-z0-9_+-]+", g["leg"]) or (g["topic"], g["leg"]) in seen:
            raise SystemExit(f"{path}: bad or duplicate leg name {g['topic']}:{g['leg']}")
        if not os.path.exists(os.path.join(REPO, g["topic"], "backtest.py")) or not g["pairs"]:
            raise SystemExit(f"{path}: {g['topic']} has no backtest.py or no pairs")
        seen.add((g["topic"], g["leg"]))
    return [g for g in d if (not topics or g["topic"] in topics) and (not legs or g["leg"] in legs)]


def runs_for(checks, with_ltp=False, det_mode=DEFAULT_EXIT_METHOD):
    """Run names a leg needs. A run name is a mode (default = no flag), plus .rep for the repeat."""
    runs = []
    if "stage-a" in checks:
        runs += ["both", "drop"] + (["ltp"] if with_ltp else [])
    if "flip" in checks:
        runs += [DEFAULT_EXIT_METHOD, "default"]
    if "det" in checks:
        runs += [det_mode, det_mode + ".rep"]
    return list(dict.fromkeys(runs))


def run_path(out_dir, g, run):
    return os.path.join(out_dir, g["topic"], f"{g['leg']}.{run}.json")


def command(g, run, out, py=sys.executable):
    mode = run.split(".")[0]
    return [py, f"{g['topic']}/backtest.py", *g["leg_args"],
            *([] if mode == "default" else ["--exit-method", mode]), "--output", out]


def classify(rc, out_exists, log_text, attempt):
    """ok | retry | crash. A parquet 'magic bytes' error fails even an rc=0 run (data went missing);
    an empty market stops at once; a traceback that isn't a known transient gets one retry."""
    if rc == 0 and out_exists and "magic bytes" not in log_text:
        return "ok"
    if TRANSIENT.search(log_text):
        return "retry"
    if "No data available" in log_text:   # the topic found nothing to run on; retrying won't help
        return "crash"
    if "Traceback" not in log_text:
        return "retry"
    return "crash" if attempt >= 2 else "retry"


def run_one(g, run, out_dir, py=sys.executable, attempts=4, timeout=7200):
    final = run_path(out_dir, g, run)
    if os.path.exists(final):
        return
    os.makedirs(os.path.dirname(final), exist_ok=True)
    out = final
    if g["topic"] in ADAPT:
        os.makedirs(final[:-5] + ".d", exist_ok=True)
        out = os.path.join(final[:-5] + ".d", "x.json")
    log = final[:-5] + ".log"
    cmd = command(g, run, out, py)
    for attempt in range(1, attempts + 1):
        if g["topic"] in ADAPT:   # no stale save_results files from a failed attempt
            for f in os.listdir(final[:-5] + ".d"):
                os.remove(os.path.join(final[:-5] + ".d", f))
        while True:
            with lock:
                if running() < MACHINE_LIMIT:
                    t0 = time.time()
                    p = subprocess.Popen(cmd, cwd=REPO, stdout=open(log, "w"), stderr=subprocess.STDOUT,
                                         env={**os.environ, "PYTHONUNBUFFERED": "1"})   # live logs
                    time.sleep(3)   # let pgrep see it before the other worker looks
                    break
            time.sleep(10)
        try:
            rc = p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            rc = -9
        secs = int(time.time() - t0)
        txt = open(log, errors="replace").read()
        res = final if g["topic"] in ADAPT and rc == 0 and adapt(g["topic"], g["leg_args"], final) else out
        verdict = classify(rc, os.path.exists(res), txt, attempt)
        print(f"{g['topic']} {g['leg']} {run} attempt {attempt} rc={rc} {secs}s {verdict}", flush=True)
        if verdict == "ok":
            json.dump({"cmd": cmd[1:], "rc": rc, "seconds": secs, "attempts": attempt},
                      open(final[:-5] + ".meta.json", "w"))
            return
        for f in (res, log):   # keep the evidence of every failed attempt
            if os.path.exists(f):
                os.rename(f, f + f".fail{attempt}")
        if verdict == "crash":
            return
        time.sleep(6 * attempt)


# ---------------------------------------------------------------- comparisons

def dig(d, path):
    for k in path.split("."):
        if isinstance(d, list):
            d = d[int(k)]
        else:
            d = d.get(k) if isinstance(d, dict) else None
        if d is None:
            return None
    return d


def is_ltp_key(k):
    """A key the LTP wiring adds: exit_method or any key containing ltp (ltp_filled, *_ltp, period_ltp_audit, ...)."""
    return k == "exit_method" or "ltp" in str(k)


def has_ltp_keys(x):
    if isinstance(x, dict):
        return any(is_ltp_key(k) or has_ltp_keys(v) for k, v in x.items())
    if isinstance(x, list):
        return any(has_ltp_keys(v) for v in x)
    return False


def strip_ltp(x):
    """The structure without anything the LTP wiring adds, for drop-run vs both-run equality."""
    if isinstance(x, dict):
        return {k: strip_ltp(v) for k, v in x.items() if not is_ltp_key(k) and k != "csv_header"}
    if isinstance(x, list):
        return [strip_ltp(v) for v in x]
    return x


def drop_keys(x, keys):
    if isinstance(x, dict):
        return {k: drop_keys(v, keys) for k, v in x.items() if k not in keys}
    if isinstance(x, list):
        return [drop_keys(v, keys) for v in x]
    return x


def exit_methods(x):
    """Every exit_method value anywhere in a results JSON."""
    if isinstance(x, dict):
        return [v for k, v in x.items() if k == "exit_method"] + [m for v in x.values() for m in exit_methods(v)]
    if isinstance(x, list):
        return [m for v in x for m in exit_methods(v)]
    return []


def first_diff(a, b, path=""):
    if type(a) != type(b):
        return f"{path}: {str(a)[:60]} vs {str(b)[:60]}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}.{k}: only in one run"
            d = first_diff(a[k], b[k], f"{path}.{k}")
            if d:
                return d
    elif isinstance(a, list):
        if len(a) != len(b):
            return f"{path}: length {len(a)} vs {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_diff(x, y, f"{path}[{i}]")
            if d:
                return d
    elif a != b:
        return f"{path}: {a} vs {b}"
    return None


def same_cagr(pub, pct):
    """pub (percent, or an unrounded fraction in some topics, e.g. graham-timing) == pct (percent, 2dp)."""
    return abs(float(pub) - float(pct)) <= 0.0051 or abs(round(float(pub) * 100, 2) - float(pct)) <= 0.0051


def check_stage_a(g, b, d, l=None, with_ltp=False):
    """Stage A on one leg's both (b), drop (d) and ltp (l) runs. Returns (deltas, problems)."""
    probs, deltas = [], []
    if b is None:
        probs.append("both run missing")
    if d is None:
        probs.append("drop run missing")
    if with_ltp and l is None:
        probs.append("ltp run missing")
    if b is not None:
        if isinstance(b, dict) and b.get("error"):
            probs.append(f"both run error: {b['error']}")
        for pr in g["pairs"]:
            pub = dig(b, pr["published_cagr_path"])
            blk = dig(b, pr["ltp_block_path"]) or {}
            dp, lp = (blk.get("drop_portfolio") or {}).get("cagr"), (blk.get("ltp_portfolio") or {}).get("cagr")
            if pub is None or dp is None or lp is None:
                probs.append(f"path missing: {pr} pub={pub} drop={dp} ltp={lp}")
                continue
            if not same_cagr(pub, dp):
                probs.append(f"drop book != published: {pr['published_cagr_path']} {pub} vs {dp}")
            m, f, s = blk.get("missing_exits"), blk.get("ltp_filled"), blk.get("still_missing")
            if "fills" in blk:   # run-level block: full provenance
                if None in (m, f, s) or m != f + s or f != len(blk["fills"]):
                    probs.append(f"counts inconsistent: missing={m} filled={f} still={s} fills={len(blk['fills'])}")
            elif m is not None and f is not None and f > m:   # per-track block: counts only
                probs.append(f"track counts inconsistent: missing={m} filled={f}")
            deltas.append((pr["published_cagr_path"], dp, lp, round(float(lp) - float(dp), 2), m, f))
    if d is not None:
        if has_ltp_keys(d):
            probs.append("drop run carries ltp keys")
        if b is not None and g["topic"] not in ADAPT:
            fd = first_diff(strip_ltp(d), strip_ltp(b))
            if fd:
                probs.append(f"drop run != both run (first difference) {fd}")
        if b is not None:
            for pr in g["pairs"]:
                a, c = dig(d, pr["published_cagr_path"]), dig(b, pr["published_cagr_path"])
                if a is None or c is None or abs(float(a) - float(c)) > (0.0051 if abs(float(c)) > 1.5 else 0.000051):
                    probs.append(f"drop-run vs both-run published CAGR differ: {a} vs {c} "
                                 "(not deterministic, or the flag leaks into the drop path)")
    # ltp mode: the published books ARE the LTP books, and must equal the both run's ltp_portfolio
    if l is not None and b is not None:
        for pr in g["pairs"]:
            pub_l = dig(l, pr["published_cagr_path"])
            lp = ((dig(b, pr["ltp_block_path"]) or {}).get("ltp_portfolio") or {}).get("cagr")
            if pub_l is None or lp is None or not same_cagr(pub_l, lp):
                probs.append(f"ltp run's published {pr['published_cagr_path']}={pub_l} != both run's LTP book {lp}")
    return deltas, probs


def check_det(a, r, mode):
    """The same leg and mode run twice must write identical JSON."""
    if a is None or r is None:
        return [f"det: {mode} run or its repeat missing"]
    fd = first_diff(drop_keys(a, VOLATILE), drop_keys(r, VOLATILE))
    return [f"det: {mode} run != its repeat (first difference) {fd}"] if fd else []


def check_flip(dflt, expl, default_method=DEFAULT_EXIT_METHOD):
    """A run without --exit-method must equal --exit-method <default_method>, and be in that mode."""
    if dflt is None or expl is None:
        return [f"flip: default run or --exit-method {default_method} run missing"]
    probs = []
    fd = first_diff(drop_keys(dflt, VOLATILE), drop_keys(expl, VOLATILE))
    if fd:
        probs.append(f"flip: default run != --exit-method {default_method} run (first difference) {fd}")
    ems = sorted(set(exit_methods(dflt)))
    if default_method == "drop":
        if has_ltp_keys(dflt):
            probs.append("flip: default is drop but the default run carries ltp keys")
    elif ems != [default_method]:
        probs.append(f"flip: default run's exit_method {ems} != {default_method}")
    return probs


# ---------------------------------------------------------------- report and CLI

def load(path):
    return json.load(open(path)) if os.path.exists(path) else None


def report(legs, out_dir, checks, with_ltp=False, det_mode=DEFAULT_EXIT_METHOD):
    rows = []
    for g in legs:
        r = {run: load(run_path(out_dir, g, run)) for run in runs_for(checks, with_ltp, det_mode)}
        deltas, probs = [], []
        if "stage-a" in checks:
            deltas, probs = check_stage_a(g, r["both"], r["drop"], r.get("ltp"), with_ltp)
        if "det" in checks:
            probs += check_det(r[det_mode], r[det_mode + ".rep"], det_mode)
        if "flip" in checks:
            probs += check_flip(r["default"], r[DEFAULT_EXIT_METHOD])
        secs = {run: (load(run_path(out_dir, g, run)[:-5] + ".meta.json") or {}).get("seconds") for run in r}
        rows.append({"topic": g["topic"], "leg": g["leg"], "leg_args": g["leg_args"], "deltas": deltas,
                     "problems": probs, "seconds": secs})
    for x in rows:
        ds = "; ".join(f"{p.rsplit('.', 2)[-2] if '.' in p else p} {dp}->{lp} ({dl:+.2f}) miss {m} fill {f}"
                       for p, dp, lp, dl, m, f in x["deltas"])
        t = " ".join(f"{k} {v}s" for k, v in x["seconds"].items() if v is not None)
        print(f"{'FAIL' if x['problems'] else 'ok  '} {x['topic'] + ':' + x['leg']:<36} {ds}  [{t}]")
        for p in x["problems"]:
            print(f"       - {p}")
    bad = sum(bool(x["problems"]) for x in rows)
    print(f"\n{len(rows)} legs, {bad} with problems (checks: {','.join(checks)}; default exit method "
          f"{DEFAULT_EXIT_METHOD}; out-dir {out_dir})")
    os.makedirs(out_dir, exist_ok=True)
    json.dump(rows, open(os.path.join(out_dir, "gate_report.json"), "w"), indent=1)
    return bad


def main(argv=None):
    ap = argparse.ArgumentParser(description="Gate for the --exit-method wiring. See scripts/LTP_GATE.md.")
    ap.add_argument("cmd", choices=("plan", "run", "report"), help="plan = print the commands, run nothing")
    ap.add_argument("legs_file", help="e.g. scripts/gate_legs/wave1.json")
    ap.add_argument("--check", default="stage-a", help=f"comma list of {','.join(CHECKS)} (default stage-a)")
    ap.add_argument("--with-ltp", action="store_true", help="stage-a also runs and checks --exit-method ltp")
    ap.add_argument("--det-mode", default=DEFAULT_EXIT_METHOD, choices=EXIT_METHODS + ("default",),
                    help=f"mode the det check repeats (default {DEFAULT_EXIT_METHOD})")
    ap.add_argument("--topic", help="comma list of topics to keep")
    ap.add_argument("--leg", help="comma list of leg names to keep")
    ap.add_argument("--out-dir", default=OUT_DIR, help="run outputs, logs, gate_report.json (default results/ltp_gate)")
    ap.add_argument("--jobs", type=int, default=2, choices=(1, 2), help="backtests this gate runs at once")
    ap.add_argument("--attempts", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=7200, help="seconds per backtest attempt")
    ap.add_argument("--python", default=sys.executable)
    a = ap.parse_args(argv)
    checks = [c for c in a.check.split(",") if c]
    if not checks or set(checks) - set(CHECKS):
        ap.error(f"--check takes {','.join(CHECKS)}")
    legs = load_legs(a.legs_file, a.topic and a.topic.split(","), a.leg and a.leg.split(","))
    if not legs:
        ap.error("no legs selected")
    out_dir = os.path.abspath(a.out_dir)
    runs = runs_for(checks, a.with_ltp, a.det_mode)
    if a.cmd == "plan":
        for g in legs:
            for run in runs:
                p = run_path(out_dir, g, run)
                print(f"{'have' if os.path.exists(p) else 'todo'} {' '.join(command(g, run, p, a.python)[1:])}")
        print(f"\n{len(legs)} legs x {len(runs)} runs ({', '.join(runs)})")
        return 0
    if a.cmd == "run":
        def worker(items):
            for g in items:
                for run in runs:
                    try:
                        run_one(g, run, out_dir, a.python, a.attempts, a.timeout)
                    except Exception as e:   # one broken run must not stop this worker's other legs
                        print(f"{g['topic']} {g['leg']} {run} GATE ERROR {type(e).__name__}: {e}", flush=True)
        ts = [threading.Thread(target=worker, args=(legs[i::a.jobs],)) for i in range(a.jobs)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        print("ALL DONE\n", flush=True)
    return 1 if report(legs, out_dir, checks, a.with_ltp, a.det_mode) else 0


if __name__ == "__main__":
    sys.exit(main())
