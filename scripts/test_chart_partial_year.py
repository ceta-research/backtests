#!/usr/bin/env python3
"""Charts of a run that ends part way through a year (chart_utils' run-window helpers).

A run on the run window keeps full years in annual_returns and a short final year in partial_year
(METHODOLOGY.md, "Run Window"). The helpers draw that year and take the window text from the file:

  1 PARTIAL   the cumulative series has one more point than the full years, its share of a year past
              the last one, compounded with the partial year's growth, and ends at the end date; the
              year-by-year bars end with a labelled partial bar; titles and footers read
              "<start> to <end date>"
  2 FULL      a run-window file whose final year is full (a stub under 7 days): the full-year series,
              and the close its last exit used in the window text
  3 LEGACY    a file with no end_date (asset-growth's results at LEGACY_REF, before the run window)
              gives the same series and the same window text as before
  4 CHARTS    asset-growth/generate_charts.py draws every chart of that file as the base ref did (same
              files, titles, footers, lines and bars), draws a partial leg with the helpers' series and
              window text, and types no window in its source
  5 MUTANTS   each rule above, broken in a copy of chart_utils.py or generate_charts.py, fails a check

Nothing is written under asset-growth/: savefig is replaced by a recorder, and the chart script runs
from a copy in a temp dir.

Usage:  python scripts/test_chart_partial_year.py [--base REF]   (REF defaults to the merge base with origin/main)
"""
import argparse
import contextlib
import copy
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import matplotlib   # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt   # noqa: E402
from test_data_checks import merge_base   # noqa: E402

TOPIC = os.path.join(ROOT, "asset-growth")
CHARTS = os.path.join(TOPIC, "charts")
LEGACY_REF = "9d706a5"   # asset-growth's typed-year results, before its run-window rerun
US = "NYSE_NASDAQ_AMEX"
END = "2026-11-06"
FULL_END, FULL_PRICED = "2026-07-06", "2026-07-02"
SLUGS = {"us": US, "india": "NSE", "uk": "LSE", "japan": "JPX", "germany": "XETRA", "brazil": "SAO",
         "canada": "TSX", "sweden": "STO", "switzerland": "SIX"}
FAIL = []


def check(cond, what):
    if not cond:
        FAIL.append(what)
        print(f"  FAIL {what}")
    return cond


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def git_show(spec):
    r = subprocess.run(["git", "show", spec], cwd=ROOT, capture_output=True, text=True)
    if r.returncode:
        raise SystemExit(f"FAIL: cannot read {spec} ({r.stderr.strip()}); fetch the full history")
    return r.stdout


LEGACY_JSON = git_show(f"{LEGACY_REF}:asset-growth/results/exchange_comparison.json")


def legacy_data():
    return json.loads(LEGACY_JSON)


def run_window_leg(entry, partial=True):
    """A run-window leg made from a legacy one, shaped as RunWindow.fields writes it (percent, key spy)."""
    e = copy.deepcopy(entry)
    e["annual_returns"].append({"year": 2025, "portfolio": 7.5, "spy": 12.25, "excess": -4.75})
    e.update({"end_date_requested": "latest", "window_label": "2000-2026", "full_periods": 26})
    if partial:
        e.update({"end_date": END, "priced_to": END, "measured_end": END,
                  "partial_period": {"rebalance_date": "2026-07-01", "entry_day": "2026-07-02", "exit_date": END,
                                     "days": 127, "period_days": 365, "fraction": 0.347945},
                  "partial_year": {"year": 2026, "start": "2026-07-01", "end": END, "periods": 1,
                                   "year_fraction": 0.347945, "portfolio": 4.2, "spy": -1.5, "excess": 5.7}})
    else:
        e.update({"end_date": FULL_END, "priced_to": FULL_PRICED, "measured_end": "2026-07-01",
                  "partial_period": None, "partial_year": None})
    return e


def ref_growth(entry, field, initial=10000):
    """The series generate_charts drew before: initial at the first start year, each value at its period's end."""
    ar = entry["annual_returns"]
    xs, vals = [ar[0]["year"]], [initial]
    for r in ar:
        vals.append(vals[-1] * (1 + r[field] / 100))
        xs.append(r["year"] + 1)
    return xs, vals


def full_years(entry, field):
    return [r["year"] for r in entry["annual_returns"]], [r[field] for r in entry["annual_returns"]]


# ---------------------------------------------------------------- 1 partial

def partial_failures(cu, data):
    f = []
    e = run_window_leg(data[US])
    py = e["partial_year"]
    for field in ("portfolio", "spy"):
        xs, vals = cu.growth_series(e, field)
        if len(xs) != len(e["annual_returns"]) + 2 or len(vals) != len(xs):
            f.append(f"{field}: {len(xs)} points, not the {len(e['annual_returns']) + 1} full-year ones plus one")
            continue
        if (xs[:-1], vals[:-1]) != ref_growth(e, field):
            f.append(f"{field}: the full-year points differ from the full-year series")
        if abs(xs[-1] - (xs[-2] + py["year_fraction"])) > 1e-9:
            f.append(f"{field}: partial point at x={xs[-1]}, not {xs[-2]} + {py['year_fraction']}")
        if abs(vals[-1] - vals[-2] * (1 + py[field] / 100)) > 1e-6:
            f.append(f"{field}: partial point {vals[-1]} not compounded with {py[field]}%")
        labels, bars = cu.annual_bars(e, field)
        years, values = full_years(e, field)
        if labels[:-1] != years or bars[:-1] != values:
            f.append(f"{field}: the full-year bars changed")
        if bars[-1] != py[field] or not (isinstance(labels[-1], str) and "2026" in labels[-1]
                                         and "partial" in labels[-1]):
            f.append(f"{field}: last bar {labels[-1]!r}={bars[-1]}, not a labelled 2026 partial bar of {py[field]}")
    if cu.window_end(e) != END:
        f.append(f"window_end {cu.window_end(e)!r}, not {END}")
    for bars in (False, True):
        text = cu.window_text(e, bars=bars)
        if text != f"2000 to {END}":
            f.append(f"window_text(bars={bars}) {text!r}, not '2000 to {END}'")
    note = cu.partial_note(e)
    if py["start"] not in note or END not in note or "partial" not in note:
        f.append(f"partial_note {note!r} lacks the partial year's dates")
    if cu.windows_text({US: e, "NSE": run_window_leg(data["NSE"])}, [US, "NSE"]) != f"2000 to {END}":
        f.append("windows_text over two run-window legs")
    short = run_window_leg(data[US])
    short["annual_returns"] = []
    if cu.growth_series(short, "portfolio") != ([2026, 2026 + py["year_fraction"]], [10000, 10000 * 1.042]):
        f.append(f"a run shorter than a year: {cu.growth_series(short, 'portfolio')}")
    return f


# ---------------------------------------------------------------- 2 full final year

def full_failures(cu, data):
    f = []
    e = run_window_leg(data[US], partial=False)
    for field in ("portfolio", "spy"):
        if cu.growth_series(e, field) != ref_growth(e, field):
            f.append(f"{field}: a full final year changed the series")
        if cu.annual_bars(e, field) != full_years(e, field):
            f.append(f"{field}: a full final year changed the bars")
    for bars in (False, True):
        if cu.window_text(e, bars=bars) != f"2000 to {FULL_PRICED}":
            f.append(f"window_text(bars={bars}) {cu.window_text(e, bars=bars)!r}, not the last exit's close")
    no_close = dict(e, priced_to=None)
    if cu.window_text(no_close) != f"2000 to {FULL_END}":
        f.append(f"window_text without priced_to {cu.window_text(no_close)!r}, not end_date")
    if cu.partial_note(e) != "":
        f.append("partial_note on a full final year")
    return f


# ---------------------------------------------------------------- 3 legacy

def legacy_failures(cu, data):
    f = []
    keys = [k for k, v in data.items() if v.get("annual_returns")]
    for k in keys:
        e = data[k]
        if cu.growth_series(e, "portfolio") != ref_growth(e, "portfolio"):
            f.append(f"{k}: portfolio series changed")
        if cu.growth_series(e, "spy") != cu.benchmark_cumulative(data, k, period_end=True):
            f.append(f"{k}: benchmark series differs from benchmark_cumulative(period_end=True)")
        for field in ("portfolio", "spy"):
            if cu.annual_bars(e, field) != full_years(e, field):
                f.append(f"{k}: {field} bars changed")
        if cu.window_end(e) is not None or cu.partial_note(e) != "":
            f.append(f"{k}: a legacy leg read as a run-window leg")
        if cu.window_text(e) != "2000-2025" or cu.window_text(e, bars=True) != "2000-2024":
            f.append(f"{k}: window text {cu.window_text(e)!r} / {cu.window_text(e, bars=True)!r}, "
                     "not the file's 2000-2025 / bar years 2000-2024")
    if cu.windows_text(data, keys) != "2000-2025":
        f.append(f"windows_text {cu.windows_text(data, keys)!r}")
    bare = {k: v for k, v in copy.deepcopy(data[US]).items() if k != "window_label"}
    if cu.window_text(bare) != "2000-2025":
        f.append(f"a leg without window_label: {cu.window_text(bare)!r}, not its annual span")
    return f


# ---------------------------------------------------------------- 4 charts

def snapshot(fig):
    """What a chart shows, as data: per axes the title, lines, bars, texts, ticks and legend; the footers."""
    axes = []
    for ax in fig.axes:
        legend = ax.get_legend()
        axes.append({
            "title": ax.get_title(),
            "lines": [[tuple(map(float, p)) for p in ln.get_xydata()] for ln in ax.lines],
            "bars": [(float(p.get_x()), float(p.get_y()), float(p.get_width()), float(p.get_height()),
                      p.get_hatch()) for p in ax.patches],
            "points": [[tuple(map(float, p)) for p in c.get_offsets()] for c in ax.collections],
            "texts": [(t.get_text(), tuple(map(float, getattr(t, "xy", t.get_position())))) for t in ax.texts],
            "xticks": [t.get_text() for t in ax.get_xticklabels()],
            "legend": [t.get_text() for t in legend.get_texts()] if legend else [],
        })
    return {"axes": axes, "footers": [t.get_text() for t in fig.texts]}


@contextlib.contextmanager
def recorded():
    """plt.savefig records the current figure by file name and writes nothing."""
    figs, orig = {}, plt.savefig
    plt.savefig = lambda out, *a, **k: figs.__setitem__(os.path.basename(str(out)), snapshot(plt.gcf()))
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            yield figs
    finally:
        plt.savefig = orig
        plt.close("all")


def topic_copy(tmp, name, src):
    """generate_charts.py source in tmp/<name>/asset-growth/, beside the legacy results."""
    d = os.path.join(tmp, name, "asset-growth")
    os.makedirs(os.path.join(d, "results"), exist_ok=True)
    with open(os.path.join(d, "results", "exchange_comparison.json"), "w") as fh:
        fh.write(LEGACY_JSON)
    path = os.path.join(d, "generate_charts.py")
    with open(path, "w") as fh:
        fh.write(src)
    return path


def base_figures(tmp, ref):
    path = topic_copy(tmp, "base", git_show(f"{ref}:asset-growth/generate_charts.py"))
    with recorded() as figs:
        mod = load(path, "asset_growth_charts_base")   # an older base draws every chart on import
        if not figs and hasattr(mod, "main"):
            mod.main()
    return figs


def charts_failures(cu, path, base, data):
    f = []
    with recorded() as figs:
        mod = load(path, f"asset_growth_charts_{abs(hash(path))}")
    if figs:
        f.append(f"importing generate_charts drew {len(figs)} charts: its charts belong in main()")
    with recorded() as figs:
        mod.main()
    if sorted(figs) != sorted(base):
        f.append(f"chart set {sorted(figs)} differs from the base's {sorted(base)}")
    for name in sorted(set(figs) & set(base)):
        if figs[name] != base[name]:
            f.append(f"legacy {name} differs from the base")
    mod.data = {k: run_window_leg(v) for k, v in data.items()}
    with recorded() as figs:
        mod.main()
    window = f"2000 to {END}"
    for name, fig in sorted(figs.items()):
        ax = fig["axes"][0]
        if not ax["title"].endswith(f"({window})") or "2000-2025" in ax["title"]:
            f.append(f"partial {name}: title {ax['title']!r} lacks the file's window")
        leg = mod.data.get(SLUGS.get(name.split("_")[0]))
        if leg is None:
            continue
        if not any(window in t for t in fig["footers"]):
            f.append(f"partial {name}: footer {fig['footers']} lacks the file's window")
        if name.endswith("_cumulative_growth.png"):
            for field in ("portfolio", "spy"):
                xs, vals = cu.growth_series(leg, field)
                if [(float(x), float(v)) for x, v in zip(xs, vals)] not in ax["lines"]:
                    f.append(f"partial {name}: no line draws growth_series({field})")
            if not any(END in t for t, _ in ax["texts"]):
                f.append(f"partial {name}: no end label {END}")
        elif name.endswith("_annual_returns.png"):
            labels, spy = cu.annual_bars(leg, "spy")
            port = cu.annual_bars(leg, "portfolio")[1]
            n = len(labels)
            if ax["xticks"] != [str(x) for x in labels]:
                f.append(f"partial {name}: ticks end {ax['xticks'][-2:]}, not {labels[-2:]}")
            if [b[3] for b in ax["bars"]] != spy + port:
                f.append(f"partial {name}: bar heights are not annual_bars' values")
            hatched = [i for i, b in enumerate(ax["bars"]) if b[4]]
            if hatched != [n - 1, 2 * n - 1]:
                f.append(f"partial {name}: hatched bars {hatched}, not the two partial ones")
            if not any(cu.partial_note(leg) in t for t in fig["footers"]):
                f.append(f"partial {name}: footer lacks the partial note")
    src = open(path).read()
    typed = re.findall(r"\b(?:19|20)\d\d-(?:19|20)\d\d\b", src)
    if typed:
        f.append(f"generate_charts.py types a window: {sorted(set(typed))}")
    return f


# ---------------------------------------------------------------- 5 mutants

CU_MUTANTS = [
    # (name, old text, new text): each must make a check above fail
    ("partial point dropped", "    if py:\n        xs.append", "    if False:\n        xs.append"),
    ("partial growth not compounded", "_compound(annual + ([py] if py else []), field, initial)",
     "_compound(annual, field, initial) + [_compound(annual, field, initial)[-1]] * bool(py)"),
    ("partial point at its start", 'xs.append(py["year"] + py["year_fraction"])', 'xs.append(py["year"])'),
    ("partial bar unlabelled", 'labels.append(f"{py[\'year\']} (partial)")', 'labels.append(py["year"])'),
    ("partial bar dropped", "    if py:\n        labels.append", "    if False:\n        labels.append"),
    ("window text ignores the end", "    if end:\n", "    if False:\n"),
    ("window end ignores priced_to", 'return entry.get("priced_to") or entry["end_date"]',
     'return entry["end_date"]'),
    ("legacy bars read window_label", "    if bars and annual:\n", "    if False:\n"),
]

GC_MUTANTS = [
    ("cumulative title typed", 'f"{title} ({window_text(ex)})"', 'f"{title} (2000-2025)"'),
    ("cumulative series without the partial point", "return growth_series(data[exchange_key], field, initial)",
     'return growth_series({**data[exchange_key], "partial_year": None}, field, initial)'),
    ("partial bars not hatched", '    if ex.get("partial_year"):\n        for g in groups:',
     "    if False:\n        for g in groups:"),
    ("annual footer without the partial note", '+ (f". {note}" if note else "")', ""),
]


def helper_failures(cu, data):
    out = []
    for name, fn in (("1 PARTIAL", partial_failures), ("2 FULL", full_failures), ("3 LEGACY", legacy_failures)):
        try:
            out += [f"{name}: {x}" for x in fn(cu, data)]
        except Exception as e:   # a missing helper or a crash is a failure, not a pass
            out.append(f"{name}: {type(e).__name__}: {e}")
    return out


def chart_failures(cu, path, base, data):
    try:
        return [f"4 CHARTS: {x}" for x in charts_failures(cu, path, base, data)]
    except Exception as e:
        return [f"4 CHARTS: {type(e).__name__}: {e}"]


def charts_state():
    return sorted((n, os.stat(os.path.join(CHARTS, n)).st_mtime_ns, os.path.getsize(os.path.join(CHARTS, n)))
                  for n in os.listdir(CHARTS))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None)
    a = ap.parse_args()
    ref = a.base or merge_base()
    before = charts_state()
    data = legacy_data()
    import chart_utils as cu
    with tempfile.TemporaryDirectory() as tmp:
        print(f"base {ref[:12]}")
        base = base_figures(tmp, ref)
        check(len(base) == 20, f"the base drew {len(base)} charts, not 20")
        gc_src = open(os.path.join(TOPIC, "generate_charts.py")).read()
        for x in helper_failures(cu, data) + chart_failures(cu, topic_copy(tmp, "new", gc_src), base, data):
            check(False, x)
        print("5 MUTANTS")
        cu_src = open(os.path.join(ROOT, "chart_utils.py")).read()
        for name, old, new in CU_MUTANTS:
            if cu_src.count(old) != 1:
                check(False, f"mutant {name}: anchor found {cu_src.count(old)} times")
                continue
            path = os.path.join(tmp, f"chart_utils_mut_{len(FAIL)}_{abs(hash(name))}.py")
            with open(path, "w") as fh:
                fh.write(cu_src.replace(old, new))
            check(bool(helper_failures(load(path, "chart_utils_mut"), data)), f"mutant {name} passes every check")
        for name, old, new in GC_MUTANTS:
            if gc_src.count(old) != 1:
                check(False, f"mutant {name}: anchor found {gc_src.count(old)} times")
                continue
            path = topic_copy(tmp, f"mut_{abs(hash(name))}", gc_src.replace(old, new))
            check(bool(chart_failures(cu, path, base, data)), f"mutant {name} passes every check")
    check(charts_state() == before, "asset-growth/charts/ changed during the test")
    print("FAIL" if FAIL else "PASS", f"({len(FAIL)} failures)")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
