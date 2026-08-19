#!/usr/bin/env python3
"""Rebuild a strategy's exchange_comparison.json from its per-exchange results.

Why this exists: `--global` writes both the per-exchange `returns_*.json` files and
the combined `exchange_comparison.json` in one pass, so a market that dies mid-run
on a transient parquet error leaves an `{"error": ...}` entry in the combined file
while the per-exchange file from the retry sits next to it, fresh and correct.
`generate_charts.py` reads the combined file and nothing else does, so the stale or
errored entry silently reaches the charts.

Re-run the failed market with `--exchange <CODE> --output <dir>/returns_<KEY>.json`,
then run this to fold every per-exchange file back into the combined one.

    python3 scripts/rebuild_comparison.py sector-momentum
    python3 scripts/rebuild_comparison.py sector-momentum --check

--check exits non-zero if the combined file disagrees with the per-exchange files,
which makes it usable as a pre-publish gate.
"""
import argparse
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_per_exchange(results_dir):
    out = {}
    for path in sorted(glob.glob(os.path.join(results_dir, "returns_*.json"))):
        key = os.path.basename(path)[len("returns_"):-len(".json")]
        try:
            out[key] = json.load(open(path))
        except json.JSONDecodeError as e:
            print(f"  SKIP {key}: unreadable ({e})")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("strategy", help="strategy directory name, e.g. sector-momentum")
    ap.add_argument("--check", action="store_true",
                    help="report disagreement and exit non-zero, write nothing")
    ap.add_argument("--drop", nargs="*", default=[],
                    help="exchange keys to exclude (e.g. JNB for a dropped market)")
    args = ap.parse_args()

    results_dir = os.path.join(ROOT, args.strategy, "results")
    combined_path = os.path.join(results_dir, "exchange_comparison.json")
    if not os.path.isdir(results_dir):
        sys.exit(f"no results dir at {results_dir}")

    per = load_per_exchange(results_dir)
    for k in args.drop:
        per.pop(k, None)
    if not per:
        sys.exit("no returns_*.json files found")

    old = {}
    if os.path.exists(combined_path):
        old = json.load(open(combined_path))

    problems = []
    for key, entry in per.items():
        prev = old.get(key)
        if prev is None:
            problems.append(f"{key}: missing from exchange_comparison.json")
        elif "error" in prev:
            problems.append(f"{key}: comparison file holds an ERROR entry "
                            f"({prev['error'][:60]})")
        elif (prev.get("portfolio") or {}).get("cagr") != entry["portfolio"]["cagr"]:
            problems.append(f"{key}: comparison CAGR {(prev.get('portfolio') or {}).get('cagr')} "
                            f"!= per-exchange {entry['portfolio']['cagr']}")
    for key in old:
        if key not in per:
            problems.append(f"{key}: in comparison file but has no returns_{key}.json")

    for p in problems:
        print(f"  {p}")

    if args.check:
        if problems:
            print(f"\nFAIL: {len(problems)} disagreement(s). "
                  f"Run without --check to rebuild.")
            return 1
        print(f"\nOK: {len(per)} exchanges agree.")
        return 0

    with open(combined_path, "w") as f:
        json.dump(per, f, indent=2)
    print(f"\nWrote {combined_path} with {len(per)} exchanges: {', '.join(sorted(per))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
