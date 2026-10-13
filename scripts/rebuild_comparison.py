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
    python3 scripts/rebuild_comparison.py fcf-yield --keep US_MAJOR LSE JPX Canada HKSE China SIX Taiwan SET NSE
    python3 scripts/rebuild_comparison.py sector-momentum --only JNB

--keep writes a curated file: those keys, in that order. --only replaces the named nodes
and leaves every other node byte-identical, for a one-market repair where the sibling
per-exchange files are older than the published nodes. A rebuild keeps the combined
file's key order and puts new keys after it. ADAPTERS lists the topics whose
per-exchange files are not named returns_<KEY>.json.

A per-exchange file that does not parse stops the run (exit 1, nothing written), and so
does one whose key differs only by case from a key of the combined file: returns_nse.json
for the NSE node, a case-only rename that did not take on a case-insensitive disk.

--check exits non-zero if the combined file disagrees with the per-exchange files,
which makes it usable as a pre-publish gate. It takes the same --drop, --keep or --only
and reports everything that rebuild would change, plus any ERROR entry.
"""
import argparse
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _suffix(name, node):
    return name[len("returns_"):-len(".json")]


# graham-number/run_all_exchanges.py writes <market name, lowercased>_results.json and keys
# the combined file by the name, which the file name alone does not give back.
GRAHAM_NUMBER_KEYS = {n.lower(): n for n in (
    "US_MAJOR", "India", "Germany", "Canada", "Sweden", "Brazil", "Korea", "Australia", "HKSE",
    "Taiwan", "Switzerland", "Norway", "UK", "France", "Japan", "SouthAfrica", "Singapore")}

# Per-exchange files as (glob, key of (file name, its JSON)). Topics not listed use DEFAULT.
DEFAULT = [("returns_*.json", _suffix)]
ADAPTERS = {
    # us.json and sweden.json are single runs like the rest; india.json is a stale duplicate.
    "dogs-of-dow": DEFAULT + [("us.json", lambda name, node: "US_MAJOR"),
                              ("sweden.json", lambda name, node: "STO")],
    "fcf-yield": [("value-04-fcf-yield_*.json", lambda name, node: node.get("universe"))],
    "graham-number": [("*_results.json",
                       lambda name, node: GRAHAM_NUMBER_KEYS.get(name[:-len("_results.json")]))],
}


def load_per_exchange(results_dir, strategy=None):
    """({key: node}, {key: file name}) of a strategy's per-exchange files, nodes as on disk."""
    out, files = {}, {}
    for pattern, key_of in ADAPTERS.get(strategy, DEFAULT):
        for path in sorted(glob.glob(os.path.join(results_dir, pattern))):
            name = os.path.basename(path)
            try:
                node = json.load(open(path))
            except json.JSONDecodeError as e:   # skipping it would write a file without its market
                sys.exit(f"{name}: unreadable ({e})")
            key = key_of(name, node)
            if key is None or key in out:
                sys.exit(f"{name}: " + ("no comparison key known for this file" if key is None
                                         else f"same key {key} as {files[key]}"))
            out[key], files[key] = node, name
    return out, files


def first_diff(a, b, path=""):
    """Where a comparison node (a) first differs from its per-exchange file (b), else None."""
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}.{k}: only in the {'comparison' if k in a else 'per-exchange'} file"
            d = first_diff(a[k], b[k], f"{path}.{k}")
            if d:
                return d
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return f"{path}: length {len(a)} vs {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_diff(x, y, f"{path}[{i}]")
            if d:
                return d
    elif type(a) != type(b) or a != b:
        return f"{path}: {str(a)[:60]} vs {str(b)[:60]}"
    return None


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("strategy", help="strategy directory name, e.g. sector-momentum")
    ap.add_argument("--check", action="store_true",
                    help="report disagreement and exit non-zero, write nothing")
    ap.add_argument("--drop", nargs="*", default=[],
                    help="exchange keys to exclude (e.g. JNB for a dropped market)")
    ap.add_argument("--keep", nargs="+", default=[], metavar="KEY",
                    help="write only these keys, in this order (a curated comparison file)")
    ap.add_argument("--only", nargs="+", default=[], metavar="KEY",
                    help="replace only these nodes; every other node stays byte-identical")
    args = ap.parse_args(argv)
    if sum(map(bool, (args.drop, args.keep, args.only))) > 1:
        ap.error("--drop, --keep and --only are alternatives")

    results_dir = os.path.join(ROOT, args.strategy, "results")
    combined_path = os.path.join(results_dir, "exchange_comparison.json")
    if not os.path.isdir(results_dir):
        sys.exit(f"no results dir at {results_dir}")

    per, files = load_per_exchange(results_dir, args.strategy)
    if not per:
        sys.exit("no per-exchange files found")

    old, old_text = {}, None
    if os.path.exists(combined_path):
        old_text = open(combined_path).read()
        old = json.loads(old_text)

    # returns_nse.json for the NSE node would drop NSE and add nse, which no chart knows.
    folded = {k.lower(): k for k in old}
    for k in per:
        if k not in old and k.lower() in folded:
            sys.exit(f"{files[k]}: key {k} differs only by case from {folded[k.lower()]} "
                     f"in exchange_comparison.json")

    # `new` is the file this run writes; the problems are where the current file differs.
    asked = args.only or args.keep
    if args.only:
        new = {k: per.get(k, v) if k in args.only else v for k, v in old.items()}
    elif args.keep:
        new = {k: per[k] for k in args.keep if k in per}
    else:   # the combined file's own key order, new keys after it
        new = {k: per[k] for k in [*old, *per] if k in per and k not in args.drop}

    # What no rebuild can fix: an asked-for key with nothing to build it from, an errored source.
    blockers = [f"{k}: no per-exchange file" for k in asked if k not in per]
    blockers += [f"{k}: not in exchange_comparison.json (--only replaces existing nodes)"
                 for k in args.only if k not in old]
    blockers += [f"{k}: {files[k]} holds an ERROR entry"
                 for k in (args.only or new) if "error" in per.get(k, {})]

    problems = []
    for key, prev in old.items():
        if "error" in prev:
            problems.append(f"{key}: comparison file holds an ERROR entry "
                            f"({str(prev['error'])[:60]})")
        if key not in new:
            if key not in asked:   # an asked-for key without a file is a blocker above
                problems.append(f"{key}: in comparison file but " + (
                    f"excluded ({files[key]} exists)" if key in per
                    else "has no per-exchange file"))
        elif "error" not in prev and "error" not in new[key]:
            d = first_diff(prev, new[key])
            if d:
                problems.append(f"{key}: differs from {files[key]} at {d}")
    for key in new:
        if key not in old:
            problems.append(f"{key}: missing from exchange_comparison.json ({files[key]})")
    if args.keep and [k for k in old if k in new] != [k for k in new if k in old]:
        problems.append("key order differs from --keep")
    problems = blockers + problems

    for p in problems:
        print(f"  {p}")

    if args.check:
        if problems:
            print(f"\nFAIL: {len(problems)} disagreement(s). "
                  f"Run without --check to rebuild.")
            return 1
        print(f"\nOK: {len(args.only or new)} exchanges agree.")
        return 0

    if blockers:
        print(f"\nFAIL: nothing written, {len(blockers)} key(s) cannot be rebuilt.")
        return 1
    if args.only and json.dumps(old, indent=2) != old_text:
        sys.exit("--only needs exchange_comparison.json as json.dump(indent=2) wrote it: "
                 "re-serializing this one would change its other nodes")
    with open(combined_path, "w") as f:
        json.dump(new, f, indent=2)
    print(f"\nWrote {combined_path} with {len(new)} exchanges: {', '.join(new)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
