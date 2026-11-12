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
  4 CHARTS    for each topic in CHART_TOPICS (asset-growth, fcf-conversion), its generate_charts.py draws
              every chart of its typed-year comparison file (pinned at the topic's legacy ref) as the base
              ref did (same files, titles, footers, lines and bars), draws a partial leg with the helpers'
              series and window text, and types no window in its source
  5 MUTANTS   each rule above, broken in a copy of chart_utils.py or a topic's generate_charts.py, fails a check

Nothing is written under a topic's folder: savefig (pyplot's and Figure's) is replaced by a recorder, and
each chart script runs from a copy in a temp dir.

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
import matplotlib.figure   # noqa: E402
import matplotlib.pyplot as plt   # noqa: E402
from test_data_checks import merge_base   # noqa: E402

LEGACY_REF = "9d706a5"   # asset-growth's typed-year results, before its run-window rerun
US = "NYSE_NASDAQ_AMEX"
END = "2026-11-06"
FULL_END, FULL_PRICED = "2026-07-06", "2026-07-02"
SLUGS = {"us": US, "india": "NSE", "uk": "LSE", "japan": "JPX", "germany": "XETRA", "brazil": "SAO",
         "canada": "TSX", "sweden": "STO", "switzerland": "SIX"}
# fcf-conversion's file regions (1_<region>_cumulative_growth.png, 2_<region>_annual_returns.png) and their legs
FCF_SLUGS = {"us": US, "india": "NSE", "china": "SHZ_SHH", "taiwan": "TAI",
             **{k.lower(): k for k in ("ASX", "HKSE", "JPX", "KSC", "LSE", "SAO", "SAU", "SET", "SIX", "STO", "TSX",
                                       "XETRA")}}
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
_LEGACY = {"asset-growth": LEGACY_JSON}


def legacy_data():
    return json.loads(LEGACY_JSON)


def legacy_json(topic):
    """A chart topic's typed-year exchange_comparison.json at its legacy ref (CHART_TOPICS)."""
    if topic not in _LEGACY:
        _LEGACY[topic] = git_show(f"{CHART_TOPICS[topic]['legacy_ref']}:{topic}/results/exchange_comparison.json")
    return _LEGACY[topic]


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
    """plt.savefig (asset-growth's) and Figure.savefig (fcf-conversion's) record the figure by file name and
    write nothing."""
    figs, orig, orig_fig = {}, plt.savefig, matplotlib.figure.Figure.savefig
    plt.savefig = lambda out, *a, **k: figs.__setitem__(os.path.basename(str(out)), snapshot(plt.gcf()))
    matplotlib.figure.Figure.savefig = lambda self, out, *a, **k: figs.__setitem__(os.path.basename(str(out)),
                                                                                  snapshot(self))
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            yield figs
    finally:
        plt.savefig, matplotlib.figure.Figure.savefig = orig, orig_fig
        plt.close("all")


def topic_copy(tmp, name, src, topic="asset-growth", results=None):
    """generate_charts.py source in tmp/<name>/<topic>/, beside results/exchange_comparison.json: the topic's
    legacy file, or `results` (JSON text)."""
    d = os.path.join(tmp, name, topic)
    os.makedirs(os.path.join(d, "results"), exist_ok=True)
    with open(os.path.join(d, "results", "exchange_comparison.json"), "w") as fh:
        fh.write(legacy_json(topic) if results is None else results)
    path = os.path.join(d, "generate_charts.py")
    with open(path, "w") as fh:
        fh.write(src)
    return path


def run_main(mod):
    """mod.main() under the script's own argv, so a script that parses flags (fcf-conversion's) takes its
    defaults: results/ and charts/ beside its copy."""
    argv, sys.argv = sys.argv, [mod.__file__]
    try:
        mod.main()
    finally:
        sys.argv = argv


def base_figures(tmp, ref, topic="asset-growth"):
    path = topic_copy(tmp, "base", git_show(f"{ref}:{topic}/generate_charts.py"), topic)
    with recorded() as figs:
        mod = load(path, f"{topic.replace('-', '_')}_charts_base")   # an older base draws every chart on import
        if not figs and hasattr(mod, "main"):
            run_main(mod)
    return figs


def charts_failures(cu, path, base, data, topic="asset-growth"):
    t = CHART_TOPICS[topic]
    f = []
    with recorded() as figs:
        mod = load(path, f"{topic.replace('-', '_')}_charts_{abs(hash(path))}")
    if figs:
        f.append(f"importing generate_charts drew {len(figs)} charts: its charts belong in main()")
    with recorded() as figs:
        run_main(mod)
    if sorted(figs) != sorted(base):
        f.append(f"chart set {sorted(figs)} differs from the base's {sorted(base)}")
    for name in sorted(set(figs) & set(base)):
        if figs[name] != base[name]:
            f.append(f"legacy {name} differs from the base")
    legs = {k: run_window_leg(v) for k, v in data.items()}
    if t["partial"] == "data":   # the script read its results at import
        mod.data = legs
    else:                        # main() reads its results when it runs
        with open(os.path.join(os.path.dirname(path), "results", "exchange_comparison.json"), "w") as fh:
            json.dump(legs, fh)
    with recorded() as figs:
        run_main(mod)
    window = f"2000 to {END}"
    for name, fig in sorted(figs.items()):
        ax = fig["axes"][0]
        if not ax["title"].endswith(t["title_end"].format(window)) or "2000-2025" in ax["title"]:
            f.append(f"partial {name}: title {ax['title']!r} lacks the file's window")
        leg = legs.get(t["slugs"].get(name.split("_")[t["slug_at"]]))
        if leg is None:
            continue
        if t["leg_footer"] and not any(window in x for x in fig["footers"]):
            f.append(f"partial {name}: footer {fig['footers']} lacks the file's window")
        if name.endswith("_cumulative_growth.png"):
            for field in ("portfolio", "spy"):
                xs, vals = cu.growth_series(leg, field)
                if [(float(x), float(v)) for x, v in zip(xs, vals)] not in ax["lines"]:
                    f.append(f"partial {name}: no line draws growth_series({field})")
            if t["end_label"] and not any(END in x for x, _ in ax["texts"]):
                f.append(f"partial {name}: no end label {END}")
        elif name.endswith("_annual_returns.png"):
            labels = cu.annual_bars(leg, "portfolio")[0]
            heights = [h for field in t["bar_order"] for h in cu.annual_bars(leg, field)[1]]
            n = len(labels)
            if ax["xticks"] != [str(x) for x in labels]:
                f.append(f"partial {name}: ticks end {ax['xticks'][-2:]}, not {labels[-2:]}")
            if [b[3] for b in ax["bars"]] != heights:
                f.append(f"partial {name}: bar heights are not annual_bars' values")
            hatched = [i for i, b in enumerate(ax["bars"]) if b[4]]
            if hatched != [n - 1, 2 * n - 1]:
                f.append(f"partial {name}: hatched bars {hatched}, not the two partial ones")
            if not any(cu.partial_note(leg) in x for x in fig["footers"]):
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

FCF_GC_MUTANTS = [
    ("comparison title typed", "by Exchange ({windows_text(results, exchanges)})", "by Exchange (2000-2025)"),
    ("cumulative series without the partial point",
     'x, port_vals = growth_series(entry, "portfolio")\n    spy_vals = growth_series(entry, "spy")[1]',
     'x, port_vals = growth_series({**entry, "partial_year": None}, "portfolio")\n'
     '    spy_vals = growth_series({**entry, "partial_year": None}, "spy")[1]'),
    ("annual bars without the partial year",
     'years, port = annual_bars(entry, "portfolio")\n    spy = annual_bars(entry, "spy")[1]',
     'years, port = annual_bars({**entry, "partial_year": None}, "portfolio")\n'
     '    spy = annual_bars({**entry, "partial_year": None}, "spy")[1]'),
    ("partial bars not hatched", '    if entry.get("partial_year"):\n        for g in groups:',
     "    if False:\n        for g in groups:"),
    ("annual footer without the partial note", "    note = partial_note(entry)\n", '    note = ""\n'),
    ("per-leg titles without the window", "def leg_window(entry, bars=False):\n",
     'def leg_window(entry, bars=False):\n    return ""\n'),
    ("per-leg window on typed-year files too", 'if window_end(entry) else ""', 'if True else ""'),
    ("drawdown title without the window", "Max Drawdown by Exchange{run_window(results, exchanges)}",
     "Max Drawdown by Exchange"),
    ("drawdown window on typed-year files too", 'if any(window_end(results[k]) for k in keys) else ""',
     'if True else ""'),
]

# What differs by topic: the legacy comparison file (ref) and the chart count it draws, how a file name names its
# leg (slugs, the name's part at slug_at), how the partial legs reach the script (module data set after import, or
# its results/ file, which main() reads), how a title ends, the per-leg footer and end label (asset-growth's), the
# annual bars' order, the script's mutants and its charts folder.
CHART_TOPICS = {
    "asset-growth": {"legacy_ref": LEGACY_REF, "charts": 20, "slugs": SLUGS, "slug_at": 0, "partial": "data",
                     "title_end": "({})", "leg_footer": True, "end_label": True, "bar_order": ("spy", "portfolio"),
                     "mutants": GC_MUTANTS},
    "fcf-conversion": {"legacy_ref": "2a741b7", "charts": 34, "slugs": FCF_SLUGS, "slug_at": 1, "partial": "results",
                       "title_end": "{})", "leg_footer": False, "end_label": False,
                       "bar_order": ("portfolio", "spy"), "mutants": FCF_GC_MUTANTS},
}


def helper_failures(cu, data):
    out = []
    for name, fn in (("1 PARTIAL", partial_failures), ("2 FULL", full_failures), ("3 LEGACY", legacy_failures)):
        try:
            out += [f"{name}: {x}" for x in fn(cu, data)]
        except Exception as e:   # a missing helper or a crash is a failure, not a pass
            out.append(f"{name}: {type(e).__name__}: {e}")
    return out


def chart_failures(cu, path, base, data, topic="asset-growth"):
    try:
        return [f"4 CHARTS: {x}" for x in charts_failures(cu, path, base, data, topic)]
    except Exception as e:
        return [f"4 CHARTS: {type(e).__name__}: {e}"]


def charts_state(topic):
    """The topic's charts/ files with their times and sizes; None when it has no charts/."""
    d = os.path.join(ROOT, topic, "charts")
    if not os.path.isdir(d):
        return None
    return sorted((n, os.stat(os.path.join(d, n)).st_mtime_ns, os.path.getsize(os.path.join(d, n)))
                  for n in os.listdir(d))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None)
    a = ap.parse_args()
    ref = a.base or merge_base()
    before = {t: charts_state(t) for t in CHART_TOPICS}
    data = legacy_data()
    import chart_utils as cu
    with tempfile.TemporaryDirectory() as tmp:
        print(f"base {ref[:12]}")
        for x in helper_failures(cu, data):
            check(False, x)
        bases, srcs, tdata = {}, {}, {}
        for t, spec in CHART_TOPICS.items():
            bases[t] = base_figures(tmp, ref, t)
            check(len(bases[t]) == spec["charts"], f"{t}: the base drew {len(bases[t])} charts, not {spec['charts']}")
            srcs[t] = open(os.path.join(ROOT, t, "generate_charts.py")).read()
            tdata[t] = json.loads(legacy_json(t))
            for x in chart_failures(cu, topic_copy(tmp, "new", srcs[t], t), bases[t], tdata[t], t):
                check(False, f"{t}: {x}")
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
        for t, spec in CHART_TOPICS.items():
            for name, old, new in spec["mutants"]:
                if srcs[t].count(old) != 1:
                    check(False, f"{t} mutant {name}: anchor found {srcs[t].count(old)} times")
                    continue
                path = topic_copy(tmp, f"mut_{abs(hash((t, name)))}", srcs[t].replace(old, new), t)
                check(bool(chart_failures(cu, path, bases[t], tdata[t], t)), f"{t} mutant {name} passes every check")
    for t in CHART_TOPICS:
        check(charts_state(t) == before[t], f"{t}/charts/ changed during the test")
    print("FAIL" if FAIL else "PASS", f"({len(FAIL)} failures)")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
