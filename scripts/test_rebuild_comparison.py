#!/usr/bin/env python3
"""Offline checks for scripts/rebuild_comparison.py (temporary directories only, no network).

Covers the default returns_<KEY>.json naming, the three naming adapters (dogs-of-dow, fcf-yield,
graham-number), --keep order, --only leaving every other node byte-identical, --drop, --check
failing on each kind of disagreement, and the two refusals (a file that does not parse, a key that
differs from a comparison key only by case). The script's source is run with ROOT pointed at a temporary
directory, so no results file of the repo is read or written. Every mutant of the source must trip
a check, and every check must be tripped by a mutant.

Usage:
    python scripts/test_rebuild_comparison.py        # -v lists the checks each mutant trips
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = os.path.join(ROOT, "scripts", "rebuild_comparison.py")
with open(PATH) as fh:
    SRC = fh.read()

# graham-number/run_all_exchanges.py EXCHANGES names: the comparison keys, files are <lowercased>_results.json
GN_KEYS = ["US_MAJOR", "India", "Germany", "Canada", "Sweden", "Brazil", "Korea", "Australia", "HKSE",
           "Taiwan", "Switzerland", "Norway", "UK", "France", "Japan", "SouthAfrica", "Singapore"]
FY = "value-04-fcf-yield_"


def need(cond, what):
    if not cond:
        raise AssertionError(what)


def node(universe, cagr, **extra):
    """A results JSON small enough to read, with fields below and beside the CAGR."""
    return {"universe": universe, "portfolio": {"cagr": cagr, "sharpe_ratio": 0.5},
            "annual_returns": [{"year": 2001, "portfolio": 1.5}, {"year": 2002, "portfolio": -2.5}],
            **extra}


def returns(*pairs):
    return {f"returns_{k}.json": node(k, c) for k, c in pairs}


def chunk(n):
    """A top-level node as json.dump(indent=2) writes it inside the combined file."""
    return json.dumps(n, indent=2).replace("\n", "\n  ")


class Topic:
    """One topic's results directory under its own temporary root, driven through main()."""

    def __init__(self, mod, name, files, comparison=None, indent=2):
        self.mod, self.name = mod, name
        self.root = tempfile.mkdtemp(dir=mod.BASE)
        self.dir = os.path.join(self.root, name, "results")
        for fname, content in files.items():
            self.write(fname, content)
        if comparison is not None:
            self.write("exchange_comparison.json", comparison, indent)

    def write(self, fname, content, indent=2):
        path = os.path.join(self.dir, fname)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(content, f, indent=indent)

    def text(self):
        with open(os.path.join(self.dir, "exchange_comparison.json")) as f:
            return f.read()

    def data(self):
        return json.loads(self.text())

    def run(self, *argv):
        """(exit code, output) of main([topic, *argv]); sys.exit(message) is exit 1 plus the message."""
        self.mod.ROOT = self.root
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                rc = self.mod.main([self.name, *argv])
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) else 1
            buf.write(str(e.code))
        return rc, buf.getvalue()

    def refused(self, *argv, says, rc=1):
        """The run exits rc, prints every `says` line and leaves the comparison file as it was."""
        before = self.text() if os.path.exists(os.path.join(self.dir, "exchange_comparison.json")) else None
        got, out = self.run(*argv)
        need(got == rc, f"{argv}: exit {got}, expected {rc}: {out}")
        for s in [says] if isinstance(says, str) else says:
            need(s in out, f"{argv}: expected {s!r} in: {out}")
        after = self.text() if os.path.exists(os.path.join(self.dir, "exchange_comparison.json")) else None
        need(after == before, f"{argv}: the comparison file was written")
        return out

    def passes(self, *argv, says=""):
        got, out = self.run(*argv)
        need(got == 0 and says in out, f"{argv}: exit {got}, expected 0 with {says!r}: {out}")
        return out


# ---------------------------------------------------------------- naming

def default_naming(mod):
    files = {**returns(("NSE", 13.65), ("TAI_TWO", 3.31)),
             # none of these is a per-exchange file of a topic without an adapter
             "us.json": node("US_MAJOR", 10.22), "uk_results.json": node("LSE", 4.54),
             FY + "us.json": node("US_MAJOR", 11.12), "MASTER.json": {"note": 1},
             "diagnostics/returns_nofunds_LSE.json": node("LSE", 7.0)}
    t = Topic(mod, "some-topic", files)
    t.passes()
    need(t.data() == {"NSE": files["returns_NSE.json"], "TAI_TWO": files["returns_TAI_TWO.json"]},
         f"default naming built {list(t.data())}")
    t.passes("--check", says="OK: 2 exchanges agree")


def rebuild_keeps_key_order(mod):
    files = returns(("ASX", 1.0), ("JPX", 2.0), ("NSE", 3.0), ("XETRA", 4.0))
    t = Topic(mod, "some-topic", files, {"XETRA": node("XETRA", 9.9), "GONE": node("GONE", 9.9),
                                         "JPX": node("JPX", 9.9)})
    t.passes()
    need(list(t.data()) == ["XETRA", "JPX", "ASX", "NSE"], f"key order {list(t.data())}")
    need(all(t.data()[k] == files[f"returns_{k}.json"] for k in t.data()), "nodes are not the files")
    need(not t.text().endswith("\n"), "trailing newline: every rebuilt file would differ by it")


def adapter_dogs_of_dow(mod):
    files = {**returns(("BSE", 14.6)), "returns_TSX.json": node("Canada", 5.97),
             "us.json": node("US_MAJOR", 10.22), "sweden.json": node("STO", 9.74),
             "india.json": node("India", 12.4)}
    want = {"BSE": files["returns_BSE.json"], "TSX": files["returns_TSX.json"],
            "US_MAJOR": files["us.json"], "STO": files["sweden.json"]}
    t = Topic(mod, "dogs-of-dow", files)
    t.passes()
    # equal to the files: no status, exchanges or elapsed_seconds added, india.json not read
    need(t.data() == want, f"dogs-of-dow built {t.data()}")
    t.passes("--check", says="OK: 4 exchanges agree")


def dogs_run_all_node_disagrees(mod):
    files = {"us.json": node("US_MAJOR", 10.22), "sweden.json": node("STO", 9.74)}
    t = Topic(mod, "dogs-of-dow", files, {"US_MAJOR": {**files["us.json"], "status": "completed"},
                                          "STO": files["sweden.json"]})
    t.refused("--check", says="US_MAJOR: differs from us.json at .status: only in the comparison file")


def adapter_fcf_yield(mod):
    files = {FY + "us.json": node("US_MAJOR", 11.12), FY + "canada.json": node("Canada", 6.03),
             FY + "uk.json": node("LSE", 9.98), "returns_NSE.json": node("NSE", 1.0)}
    t = Topic(mod, "fcf-yield", files)
    t.passes()
    need(t.data() == {"US_MAJOR": files[FY + "us.json"], "Canada": files[FY + "canada.json"],
                      "LSE": files[FY + "uk.json"]}, f"fcf-yield built {list(t.data())}")


def one_key_in_two_files(mod):
    files = {FY + "us.json": node("US_MAJOR", 11.12), FY + "us_domicile.json": node("US_MAJOR", 9.0)}
    t = Topic(mod, "fcf-yield", files, {"US_MAJOR": files[FY + "us.json"]})
    for argv in ((), ("--check",)):
        t.refused(*argv, says=f"{FY}us_domicile.json: same key US_MAJOR as {FY}us.json")
    t = Topic(mod, "dogs-of-dow", {"sweden.json": node("STO", 9.74), **returns(("STO", 9.59))},
              {"STO": node("STO", 9.74)})
    t.refused(says="sweden.json: same key STO as returns_STO.json")


def file_without_a_key(mod):
    t = Topic(mod, "fcf-yield", {FY + "us.json": {"error": "No data"}}, {})
    t.refused(says=f"{FY}us.json: no comparison key known for this file")
    files = {f"{k.lower()}_results.json": node("x", 1.0) for k in ("UK", "Mexico")}
    t = Topic(mod, "graham-number", files, {})
    t.refused(says="mexico_results.json: no comparison key known for this file")


def unreadable_per_exchange_file(mod):
    t = agreeing(mod)
    with open(os.path.join(t.dir, "returns_B.json"), "w") as f:
        f.write('{"universe": "B", "portfolio"')   # a run killed mid-write
    for argv in ((), ("--check",)):
        t.refused(*argv, says="returns_B.json: unreadable")


def key_differs_only_by_case(mod):
    # a case-only rename that did not take on a case-insensitive disk: returns_nse.json for the NSE node
    files = returns(("A", 1.0), ("nse", 2.0))
    t = Topic(mod, "some-topic", files, {"NSE": node("NSE", 2.0), "A": files["returns_A.json"]})
    for argv in ((), ("--check",), ("--keep", "A", "NSE")):
        t.refused(*argv, says="returns_nse.json: key nse differs only by case from NSE in exchange_comparison.json")


def adapter_graham_number(mod):
    # the universe field never equals the key, as in the real files (India: NSE, UK: LSE)
    files = {f"{k.lower()}_results.json": node("u" + k, float(i)) for i, k in enumerate(GN_KEYS)}
    t = Topic(mod, "graham-number", {**files, "returns_NSE.json": node("NSE", 1.0)})
    t.passes()
    need(t.data() == {k: files[f"{k.lower()}_results.json"] for k in GN_KEYS},
         f"graham-number built {list(t.data())}")


# ---------------------------------------------------------------- --keep, --only, --drop

def keep_order(mod):
    files = returns(("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", 4.0))
    t = Topic(mod, "some-topic", files, {k: node(k, 0.0) for k in "ABCD"})
    t.passes("--keep", "C", "A")
    need(list(t.data()) == ["C", "A"], f"--keep C A wrote {list(t.data())}")
    need(t.data() == {"C": files["returns_C.json"], "A": files["returns_A.json"]}, "nodes are not the files")


def keep_needs_every_file(mod):
    files = returns(("A", 1.0), ("C", 3.0))
    t = Topic(mod, "some-topic", files, {"A": files["returns_A.json"], "C": files["returns_C.json"]})
    for argv in (("--keep", "C", "Z"), ("--check", "--keep", "C", "Z")):
        t.refused(*argv, says="Z: no per-exchange file")
    need("1 disagreement(s)" in t.refused("--check", "--keep", "A", "C", "Z", says="Z: no per-exchange file"),
         "a kept key without a file is reported more than once")


def check_keep(mod):
    files = returns(("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", 4.0))
    t = Topic(mod, "some-topic", files, {"C": files["returns_C.json"], "A": files["returns_A.json"]})
    t.passes("--check", "--keep", "C", "A", says="OK: 2 exchanges agree")   # B and D are not kept
    t.refused("--check", "--keep", "A", "C", says="key order differs from --keep")
    t.refused("--check", "--keep", "C", says="A: in comparison file but excluded (returns_A.json exists)")
    t.refused("--check", says=["B: missing from exchange_comparison.json (returns_B.json)",
                               "D: missing from exchange_comparison.json (returns_D.json)"])


def only_leaves_other_nodes_byte_identical(mod):
    # A and C are published from newer runs than their per-exchange files; D has no node
    files = returns(("A", 1.11), ("B", 2.22), ("C", 3.33), ("D", 4.44))
    published = {"C": node("C", 3.0), "B": {"status": "error", "error": "Rate limited"}, "A": node("A", 1.0)}
    t = Topic(mod, "some-topic", files, published)
    before = t.text()
    need(before.count(chunk(published["B"])) == 1, "fixture: B's text must appear once")
    t.passes("--only", "B")
    need(t.text() == before.replace(chunk(published["B"]), chunk(files["returns_B.json"])),
         "--only B changed more than B's node")


def only_refuses_a_reserialized_file(mod):
    files = returns(("A", 1.11), ("B", 2.22))
    t = Topic(mod, "some-topic", files, {"A": node("A", 1.0), "B": node("B", 2.0)}, indent=4)
    t.refused("--only", "B", says="--only needs exchange_comparison.json as json.dump(indent=2) wrote it")


def only_needs_a_node_and_a_file(mod):
    files = returns(("A", 1.11), ("B", 2.22))
    t = Topic(mod, "some-topic", files, {"A": node("A", 1.0), "Z": node("Z", 9.0)})
    for argv in (("--only", "B"), ("--check", "--only", "B")):
        t.refused(*argv, says="B: not in exchange_comparison.json (--only replaces existing nodes)")
    for argv in (("--only", "Z"), ("--check", "--only", "Z")):
        t.refused(*argv, says="Z: no per-exchange file")


def flags_are_alternatives(mod):
    files = returns(("A", 1.11), ("B", 2.22))
    t = Topic(mod, "some-topic", files, {"A": node("A", 1.0), "B": node("B", 2.0)})
    for argv in (("--only", "A", "--keep", "A"), ("--only", "A", "--drop", "B"), ("--keep", "A", "--drop", "B")):
        t.refused(*argv, rc=2, says="--drop, --keep and --only are alternatives")


def check_only_is_scoped(mod):
    files = returns(("A", 1.11), ("B", 2.22), ("C", 3.33))
    t = Topic(mod, "some-topic", files, {"A": node("A", 1.0), "B": node("B", 2.0)})
    out = t.refused("--check", "--only", "B", says="B: differs from returns_B.json at .portfolio.cagr: 2.0 vs 2.22")
    need("A:" not in out and "C:" not in out, f"--check --only B reported another key: {out}")
    t.passes("--only", "B")
    t.passes("--check", "--only", "B", says="OK: 1 exchanges agree")
    out = t.refused("--check", says=["A: differs from returns_A.json", "C: missing from exchange_comparison.json"])
    need("B:" not in out, f"B still reported after --only B: {out}")


def drop(mod):
    files = returns(("A", 1.0), ("B", 2.0), ("C", 3.0))
    t = Topic(mod, "some-topic", files)
    t.passes("--drop", "B")
    need(list(t.data()) == ["A", "C"], f"--drop B wrote {list(t.data())}")
    t.passes("--check", "--drop", "B", says="OK: 2 exchanges agree")   # B's file has no node, and is dropped
    t.refused("--check", says="B: missing from exchange_comparison.json (returns_B.json)")
    t.refused("--check", "--drop", "B", "C", says="C: in comparison file but excluded (returns_C.json exists)")


# ---------------------------------------------------------------- --check, one kind of disagreement each

def agreeing(mod, node_b=None, **more_nodes):
    """Files A and B with a comparison file that agrees, but for the B node and nodes given."""
    files = returns(("A", 1.0), ("B", 2.0))
    comparison = {"B": files["returns_B.json"] if node_b is None else node_b, "A": files["returns_A.json"],
                  **more_nodes}
    return Topic(mod, "some-topic", files, comparison)


def check_agrees(mod):
    t = agreeing(mod)
    before = t.text()
    t.passes("--check", says="OK: 2 exchanges agree")
    need(t.text() == before, "--check wrote the comparison file")


def check_field_below_cagr(mod):
    b = node("B", 2.0)
    b["portfolio"]["sharpe_ratio"] = 0.6
    out = agreeing(mod, b).refused("--check", says=["B: differs from returns_B.json at .portfolio.sharpe_ratio: 0.6 vs 0.5",
                                                    "FAIL: 1 disagreement(s)"])
    need("A:" not in out, f"the agreeing node A was reported: {out}")


def check_field_in_one_side_only(mod):
    agreeing(mod, node("B", 2.0, status="completed")).refused(
        "--check", says="B: differs from returns_B.json at .status: only in the comparison file")
    b = node("B", 2.0)
    del b["universe"]
    agreeing(mod, b).refused("--check", says="B: differs from returns_B.json at .universe: only in the per-exchange file")


def check_list_field(mod):
    b = node("B", 2.0)
    b["annual_returns"][1]["portfolio"] = -2.4
    agreeing(mod, b).refused("--check", says="B: differs from returns_B.json at .annual_returns[1].portfolio: -2.4 vs -2.5")
    b = node("B", 2.0)
    b["annual_returns"].pop()
    agreeing(mod, b).refused("--check", says="B: differs from returns_B.json at .annual_returns: length 1 vs 2")


def check_value_type(mod):
    # true is not 1 in JSON, though True == 1 in Python
    t = Topic(mod, "some-topic", {"returns_A.json": node("A", 1.0, window_truncated=1)},
              {"A": node("A", 1.0, window_truncated=True)})
    t.refused("--check", says="A: differs from returns_A.json at .window_truncated: True vs 1")


def check_error_node(mod):
    err = {"status": "error", "error": "Rate limited. Retry after 60s"}
    agreeing(mod, err).refused("--check", says="B: comparison file holds an ERROR entry (Rate limited. Retry after 60s)")
    # an ERROR entry with nothing to rebuild it from is both things
    agreeing(mod, C=err).refused("--check", says=["C: comparison file holds an ERROR entry",
                                                  "C: in comparison file but has no per-exchange file"])
    # and it is reported when --only repairs another node
    agreeing(mod, err).refused("--check", "--only", "A", says="B: comparison file holds an ERROR entry")


def check_node_without_file(mod):
    agreeing(mod, C=node("C", 3.0)).refused("--check", says="C: in comparison file but has no per-exchange file")


def check_file_without_node(mod):
    t = agreeing(mod)
    t.write("returns_C.json", node("C", 3.0))
    t.refused("--check", says="C: missing from exchange_comparison.json (returns_C.json)")
    t = Topic(mod, "some-topic", returns(("A", 1.0)))   # no comparison file at all
    t.refused("--check", says="A: missing from exchange_comparison.json (returns_A.json)")


def errored_per_exchange_file(mod):
    t = agreeing(mod)
    t.write("returns_B.json", {"error": "No data"})
    for argv in ((), ("--check",), ("--only", "B")):
        t.refused(*argv, says="B: returns_B.json holds an ERROR entry")


CHECKS = [default_naming, rebuild_keeps_key_order, adapter_dogs_of_dow, dogs_run_all_node_disagrees,
          adapter_fcf_yield, one_key_in_two_files, file_without_a_key, unreadable_per_exchange_file,
          key_differs_only_by_case, adapter_graham_number,
          keep_order, keep_needs_every_file, check_keep, only_leaves_other_nodes_byte_identical,
          only_refuses_a_reserialized_file, only_needs_a_node_and_a_file, flags_are_alternatives,
          check_only_is_scoped, drop, check_agrees, check_field_below_cagr, check_field_in_one_side_only,
          check_list_field, check_value_type, check_error_node, check_node_without_file,
          check_file_without_node, errored_per_exchange_file]


def failures(src):
    """{check name: why} for the checks this source of the script fails."""
    mod = types.ModuleType("rebuild_comparison_under_test")
    mod.__file__ = PATH
    try:
        exec(compile(src, PATH, "exec"), mod.__dict__)
    except Exception as e:
        return {"load": f"{type(e).__name__}: {e}"}
    out = {}
    with tempfile.TemporaryDirectory() as base:
        mod.BASE = base
        for fn in CHECKS:
            try:
                fn(mod)
            except Exception as e:  # a mutant that breaks the script fails loudly
                out[fn.__name__] = f"{type(e).__name__}: {e}"
    return out


US_ENTRY = '("us.json", lambda name, node: "US_MAJOR")'
STO_ENTRY = '("sweden.json", lambda name, node: "STO")'
DOGS = f"DEFAULT + [{US_ENTRY},\n                              {STO_ENTRY}]"
MUTANTS = {
    # naming
    "dogs-of-dow: us.json not read": (DOGS, f"DEFAULT + [{STO_ENTRY}]"),
    "dogs-of-dow: sweden.json not read": (DOGS, f"DEFAULT + [{US_ENTRY}]"),
    "dogs-of-dow: india.json read": (DOGS, DOGS[:-1] + ', ("india.json", lambda name, node: "India")]'),
    "dogs-of-dow: keyed by the universe field": (DOGS, DOGS.replace(
        "DEFAULT + [", '[("returns_*.json", lambda name, node: node.get("universe")), ')),
    "a run-time field invented": ("out[key], files[key] = node, name",
                                  'out[key], files[key] = {**node, "status": "completed"}, name'),
    "fcf-yield: keyed by the file name": ('lambda name, node: node.get("universe"))],',
                                          'lambda name, node: name[len("value-04-fcf-yield_"):-len(".json")])],'),
    "fcf-yield: no adapter": ('    "fcf-yield": [', '    "fcf-yield-off": ['),
    "graham-number: key capitalized from the file name": (
        'GRAHAM_NUMBER_KEYS.get(name[:-len("_results.json")]))],', 'name[:-len("_results.json")].capitalize())],'),
    "graham-number: a name in the wrong case": ('"SouthAfrica"', '"Southafrica"'),
    "graham-number: no adapter": ('    "graham-number": [', '    "graham-number-off": ['),
    "two files with one key: the last wins": ("if key is None or key in out:", "if key is None:"),
    "a file without a key is taken": ("if key is None or key in out:", "if key in out:"),
    "an unreadable file is skipped": ('sys.exit(f"{name}: unreadable ({e})")',
                                      'print(f"  SKIP {name}: unreadable ({e})")\n                continue'),
    "a key differing only by case is taken": ("if k not in old and k.lower() in folded:", "if False:"),
    "sub-directories read": ("glob.glob(os.path.join(results_dir, pattern))",
                             'glob.glob(os.path.join(results_dir, "**", pattern), recursive=True)'),
    # rebuild
    "rebuild sorts the keys": ("for k in [*old, *per] if", "for k in sorted(per) if"),
    "rebuild writes a trailing newline": ("        json.dump(new, f, indent=2)\n",
                                          '        json.dump(new, f, indent=2)\n        f.write("\\n")\n'),
    "--drop ignored": (" if k in per and k not in args.drop}", " if k in per}"),
    "--keep sorted": ("for k in args.keep if k in per}", "for k in sorted(args.keep) if k in per}"),
    "--keep ignored": ("    elif args.keep:\n", "    elif False:\n"),
    "--keep writes a shorter file when a key has no file": (
        'blockers = [f"{k}: no per-exchange file" for k in asked if k not in per]', "blockers = []"),
    "--only rebuilds every node": ("per.get(k, v) if k in args.only else v for", "per.get(k, v) for"),
    "--only re-serializes a file written another way": (
        "    if args.only and json.dumps(old, indent=2) != old_text:", "    if False:"),
    "--only accepts a key that is not a node": ("for k in args.only if k not in old]", "for k in args.only if False]"),
    "--only accepted with --keep or --drop": (
        "    if sum(map(bool, (args.drop, args.keep, args.only))) > 1:", "    if False:"),
    "an errored per-exchange file is written": ('if "error" in per.get(k, {})]', "if False]"),
    # --check
    "--check compares CAGR only": (
        "            d = first_diff(prev, new[key])\n",
        '            d = first_diff((prev.get("portfolio") or {}).get("cagr"), new[key]["portfolio"]["cagr"])\n'),
    "--check skips list elements": ("enumerate(zip(a, b)):", "enumerate(zip(a[:0], b[:0])):"),
    "--check skips list lengths": ("        if len(a) != len(b):\n", "        if False:\n"),
    "--check skips fields only in the node": ("for k in sorted(set(a) | set(b)):", "for k in sorted(set(b)):"),
    "--check skips fields only in the file": ("for k in sorted(set(a) | set(b)):", "for k in sorted(set(a)):"),
    "--check takes true for 1": ("elif type(a) != type(b) or a != b:", "elif a != b:"),
    "--check passes an ERROR entry": ('        if "error" in prev:\n', "        if False:\n"),
    "--check passes a node without a file": ("            if key not in asked:", "            if False:"),
    "--check passes a file without a node": ("        if key not in old:\n", "        if False:\n"),
    "--check passes another key order": (
        "    if args.keep and [k for k in old if k in new] != [k for k in new if k in old]:", "    if False:"),
    "--check exits 0 on a disagreement": ('Run without --check to rebuild.")\n            return 1',
                                          'Run without --check to rebuild.")\n            return 0'),
    "--check rebuilds": ("    if args.check:\n", "    if False:\n"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_rebuild_comparison: FAIL", base)
        sys.exit(1)
    survived, tripped = [], set()
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_rebuild_comparison: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        failed = failures(SRC.replace(old, new))
        tripped.update(failed)
        if "-v" in sys.argv:
            print(f"  {name}: {', '.join(failed) or 'SURVIVED'}")
        if not failed:
            survived.append(name)
    never = [fn.__name__ for fn in CHECKS if fn.__name__ not in tripped]
    if survived or never:
        print("test_rebuild_comparison: FAIL (mutants survived:", survived, "; checks no mutant trips:", never, ")")
        sys.exit(1)
    print(f"test_rebuild_comparison: PASS ({len(CHECKS)} checks, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
