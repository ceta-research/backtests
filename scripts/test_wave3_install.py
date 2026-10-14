#!/usr/bin/env python3
"""Offline check of the wave 3 install block in scripts/LTP_GATE.md (temporary directories, no network).

For each topic of scripts/gate_legs/wave3.json, a copy of its returns_*.json and comparison file gets a
stand-in ltp run under every _outputs name, then the wave 3 section's `git rm` and
`rebuild_comparison.py` lines are applied as written. A topic passes when each rebuild and its `--check`
twin exit 0, every comparison node is an installed run, the keys are the live comparison's (BSE_NSE
read as NSE, the india legs' key), and no returns_*.json from before the rerun is left. A topic with
no comparison file must have no rebuild line. Every doc mutant below must fail.

Usage:
    python scripts/test_wave3_install.py
"""
import contextlib
import glob
import io
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOC = os.path.join(ROOT, "scripts", "LTP_GATE.md")
LEGS = os.path.join(ROOT, "scripts", "gate_legs", "wave3.json")
REBUILD = os.path.join(ROOT, "scripts", "rebuild_comparison.py")
RENAMED = {"BSE_NSE": "NSE"}   # the india legs run NSE alone where the live key was BSE_NSE


def section(text):
    start = text.index("Wave 3 has two legs files")
    return text[start:text.index("\n## ", start)]


def commands(sec):
    """(paths `git rm` removes, argv of each rebuild_comparison.py line) in the section's bash blocks."""
    rm, rebuild = [], []
    for block in re.findall(r"```bash\n(.*?)```", sec, re.S):
        for ln in block.replace("\\\n", " ").splitlines():
            words = shlex.split(ln, comments=True)
            if words[:2] == ["git", "rm"]:
                rm += [w for w in words[2:] if not w.startswith("-")]
            elif words[:2] == ["python3", "scripts/rebuild_comparison.py"]:
                rebuild.append(words[2:])
    return rm, rebuild


def rebuild_module(root):
    mod = types.ModuleType("rebuild_under_test")
    mod.__file__ = REBUILD
    with open(REBUILD) as fh:
        exec(compile(fh.read(), REBUILD, "exec"), mod.__dict__)
    mod.ROOT = root
    return mod


def run(mod, argv):
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        try:
            return mod.main(argv)
        except SystemExit as e:
            return e.code if isinstance(e.code, int) else 1


def topic_failures(topic, outputs, rm, rebuild, tmp):
    src, dst = os.path.join(ROOT, topic, "results"), os.path.join(tmp, topic, "results")
    os.makedirs(dst)
    for path in glob.glob(os.path.join(src, "returns_*.json")) + glob.glob(os.path.join(src, "exchange_comparison.json")):
        shutil.copy(path, dst)
    comp_path = os.path.join(dst, "exchange_comparison.json")
    live = None
    if os.path.exists(comp_path):
        with open(comp_path) as fh:
            live = {RENAMED.get(k, k) for k in json.load(fh)}
    for leg, name in outputs.items():
        with open(os.path.join(dst, name), "w") as fh:
            json.dump({"universe": name, "exit_method": "ltp", "installed_leg": leg,
                       "portfolio": {"cagr": 1.0}, "annual_returns": []}, fh)
    out = []
    for path in rm:
        if path.startswith(f"{topic}/results/"):
            name = path[len(f"{topic}/results/"):]
            if name in outputs.values():
                out.append(f"git rm removes the installed {name}")
            elif os.path.exists(os.path.join(dst, name)):
                os.remove(os.path.join(dst, name))
    mine = [a[1:] for a in rebuild if a and a[0] == topic]
    if live is None:
        return out + [f"rebuild line {a} for a topic with no comparison file" for a in mine]
    builds = [a for a in mine if "--check" not in a]
    if not builds:
        return out + ["no rebuild line"]
    mod = rebuild_module(tmp)
    for a in builds:
        if not any("--check" in c and [w for w in c if w != "--check"] == a for c in mine):
            out.append(f"rebuild {a} has no --check twin")
        for argv in ([topic, *a], [topic, *a, "--check"]):
            if run(mod, argv) != 0:
                out.append(f"{' '.join(argv)} exits non-zero")
    with open(comp_path) as fh:
        comp = json.load(fh)
    if set(comp) != live:
        out.append(f"comparison keys: extra {sorted(set(comp) - live)}, missing {sorted(live - set(comp))}")
    stale = sorted(k for k, v in comp.items() if not isinstance(v, dict) or "installed_leg" not in v)
    if stale:
        out.append(f"nodes not from an installed run: {stale}")
    left = sorted(set(map(os.path.basename, glob.glob(os.path.join(dst, "returns_*.json")))) - set(outputs.values()))
    if left:
        out.append(f"pre-LTP per-market files left: {left}")
    return out


def failures(doc_text):
    rm, rebuild = commands(section(doc_text))
    with open(LEGS) as fh:
        legs = json.load(fh)
    out = []
    with tempfile.TemporaryDirectory() as tmp:
        for topic, spec in legs.items():
            if not topic.startswith("_"):
                out += [f"{topic}: {p}" for p in topic_failures(topic, spec["_outputs"], rm, rebuild, tmp)]
    return out


DQ = "defensive-quality --keep NYSE_NASDAQ_AMEX NSE SHZ_SHH HKSE SET JPX LSE XETRA TAI_TWO KSC TSX SIX STO JNB KLS JKT"
# name: (text, its replacement, how many times the text appears in the section)
MUTANTS = {
    "a topic's rebuild line missing": ("python3 scripts/rebuild_comparison.py 52-week-high\n", "", 1),
    "defensive-quality plain rebuild (adds SHH)": (DQ, "defensive-quality", 2),
    "defensive-quality keeps BSE_NSE": (DQ, DQ.replace(" NSE ", " BSE_NSE "), 2),
    "defensive-quality pre-LTP files kept": ("git rm defensive-quality/", "true defensive-quality/", 1),
    "pairs-fundamentals pre-LTP files kept": ("git rm pairs-fundamentals/", "true pairs-fundamentals/", 1),
    "pairs-fundamentals keeps SES": ("STO XETRA JNB NSE", "STO XETRA JNB NSE SES", 2),
    "pairs-zscore BSE_NSE file kept": ("git rm pairs-zscore/", "true pairs-zscore/", 1),
    "pairs-multi-pair BSE_NSE file kept": ("git rm pairs-multi-pair/", "true pairs-multi-pair/", 1),
    "pairs-zscore without --check": ("JNB NSE LSE XETRA --check\n", "JNB NSE LSE XETRA\n", 1),
    "dcf-discount rebuilt": ("rebuild_comparison.py pairs-multi-pair --check\n",
                             "rebuild_comparison.py pairs-multi-pair --check\n"
                             "python3 scripts/rebuild_comparison.py dcf-discount\n", 1),
}


def main():
    with open(DOC) as fh:
        text = fh.read()
    base = failures(text)
    if base:
        print("test_wave3_install: FAIL")
        for f in base:
            print("  -", f)
        sys.exit(1)
    sec = section(text)
    block = re.search(r"```bash\ngit rm .*?```\n", sec, re.S)
    if block is None:
        print("test_wave3_install: FAIL (no install block to mutate)")
        sys.exit(1)
    mutants = {**MUTANTS, "the install block missing": (block.group(0), "", 1)}
    survived = []
    for name, (old, new, n) in mutants.items():
        if sec.count(old) != n:
            print(f"test_wave3_install: FAIL (mutant anchor found {sec.count(old)} times, not {n}: {name})")
            sys.exit(1)
        if not failures(text.replace(sec, sec.replace(old, new))):
            survived.append(name)
    if survived:
        print("test_wave3_install: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    with open(LEGS) as fh:
        n = sum(1 for k in json.load(fh) if not k.startswith("_"))
    print(f"test_wave3_install: PASS ({n} topics, {len(mutants)} mutants caught)")


if __name__ == "__main__":
    main()
