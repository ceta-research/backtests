#!/usr/bin/env python3
"""
Generate charts for Analyst Rating Revision event study.

Produces:
  1. CAR progression line chart (upgrades: all vs single vs clustered)
  2. Upgrade vs downgrade comparison bar chart (T+1, T+5, T+21, T+63)
  3. Exchange comparison bar chart (T+21 CAR across exchanges)

Usage:
    python3 analyst-revision/generate_charts.py
    python3 analyst-revision/generate_charts.py --exchange LSE --label UK
    python3 analyst-revision/generate_charts.py --all-exchanges
    python3 analyst-revision/generate_charts.py --output analyst-revision/charts/
"""

import json
import os
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "charts")

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "US",
    "LSE": "UK",
    "XETRA": "Germany",
    "SIX": "Switzerland",
    "TSX": "Canada",
}

EXCHANGE_COVERAGE = {
    "NYSE_NASDAQ_AMEX": "NYSE+NASDAQ+AMEX, 2012–2025",
    "LSE": "LSE, 2012–2025",
    "XETRA": "XETRA, 2012–2025",
    "SIX": "SIX, 2012–2025",
    "TSX": "TSX, 2012–2025",
}

COL_UP_ALL = "#16a34a"        # green - all upgrades
COL_UP_SINGLE = "#2563eb"     # blue - single analyst
COL_UP_CLUSTER = "#7c3aed"    # purple - clustered
COL_DOWN = "#dc2626"          # red - downgrades
COL_POS = "#16a34a"
COL_NEG = "#dc2626"
COL_NEUTRAL = "#374151"

WINDOWS = [1, 5, 21, 63]
W_LABELS = ["T+1", "T+5", "T+21", "T+63"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_exchange_data(exchange_key):
    path = os.path.join(RESULTS_DIR, f"analyst_revision_{exchange_key}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No results for {exchange_key} at {path}")
    with open(path) as f:
        return json.load(f)


def load_comparison():
    path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    with open(path) as f:
        return json.load(f)


def ensure_output(output_dir):
    os.makedirs(output_dir, exist_ok=True)


def save(fig, output_dir, filename):
    path = os.path.join(output_dir, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")
    return path


def get_car(metrics, category, window):
    return (metrics.get(category, {}).get(f"T+{window}") or {}).get("mean_car", None)


def get_n(metrics, category):
    return metrics.get(category, {}).get("n", 0)


def benchmark_label(data):
    """Name the index this exchange's CARs were actually measured against.

    Each result file carries its own benchmark, so a chart must read it rather
    than assume the S&P 500. Labelling a DAX line "S&P 500" would overstate the
    result to anyone reading the chart alone.
    """
    return data.get("benchmark_name") or data.get("benchmark") or "benchmark"


def load_domicile():
    path = os.path.join(RESULTS_DIR, "domicile_decomposition.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Chart 1: CAR Progression — upgrade categories line chart
# ---------------------------------------------------------------------------
def chart_car_progression(data, output_dir, exchange_label="US",
                          exchange_key="NYSE_NASDAQ_AMEX"):
    """Line chart showing CAR at each window: all upgrades, single, clustered, downgrades."""
    metrics = data.get("car_metrics", {})
    n_up = get_n(metrics, "upgrade_all")
    n_dn = get_n(metrics, "downgrade_all")
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)
    bench = benchmark_label(data)

    fig, ax = plt.subplots(figsize=(9, 5))

    categories = [
        ("upgrade_all",      COL_UP_ALL,     "All upgrades",            "o", 2.5),
        ("upgrade_clustered", COL_UP_CLUSTER, "Clustered (2+ analysts)", "s", 2.0),
        ("upgrade_single",   COL_UP_SINGLE,  "Single analyst",           "^", 1.8),
        ("downgrade_all",    COL_DOWN,       "All downgrades",           "D", 2.0),
    ]

    for cat, color, label, marker, lw in categories:
        cars = [get_car(metrics, cat, w) for w in WINDOWS]
        if all(c is None for c in cars):
            continue
        # Replace None with 0 for plotting
        cars_plot = [c if c is not None else 0 for c in cars]
        n = get_n(metrics, cat)
        ax.plot(range(len(WINDOWS)), cars_plot, color=color, linewidth=lw,
                linestyle="--" if cat == "upgrade_all" else "-",
                marker=marker, label=f"{label} (n={n:,})", alpha=0.9)

    ax.axhline(0, color="black", linewidth=0.8, linestyle=":", alpha=0.5)
    ax.set_xticks(range(len(WINDOWS)))
    ax.set_xticklabels(W_LABELS, fontsize=10)
    ax.set_ylabel(f"Mean CAR vs {bench} (%)", fontsize=9)
    ax.set_title(
        f"Analyst Rating Revisions: Post-Event Drift — {exchange_label}\n"
        f"({coverage}, vs {bench}, MCap threshold applied, winsorized mean)",
        fontsize=11, fontweight="bold"
    )
    ax.legend(fontsize=8, loc="upper right" if exchange_label != "Germany" else "upper left")
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"1_{prefix}_car_progression.png")


# ---------------------------------------------------------------------------
# Chart 2: Upgrade vs downgrade comparison — grouped bar
# ---------------------------------------------------------------------------
def chart_upgrade_vs_downgrade(data, output_dir, exchange_label="US",
                                exchange_key="NYSE_NASDAQ_AMEX"):
    """Grouped bar chart: upgrades vs downgrades at each window."""
    metrics = data.get("car_metrics", {})
    n_up = get_n(metrics, "upgrade_all")
    n_dn = get_n(metrics, "downgrade_all")
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)
    bench = benchmark_label(data)

    up_cars = [get_car(metrics, "upgrade_all", w) or 0 for w in WINDOWS]
    dn_cars = [get_car(metrics, "downgrade_all", w) or 0 for w in WINDOWS]

    x = np.arange(len(WINDOWS))
    width = 0.38

    fig, ax = plt.subplots(figsize=(10, 5.5))

    bars_up = ax.bar(x - width / 2, up_cars, width, color=COL_UP_ALL,
                     label=f"Upgrades (n={n_up:,})", alpha=0.85)
    bars_dn = ax.bar(x + width / 2, dn_cars, width, color=COL_DOWN,
                     label=f"Downgrades (n={n_dn:,})", alpha=0.85)

    for bar, v in zip(list(bars_up) + list(bars_dn), up_cars + dn_cars):
        if abs(v) > 0.03:
            offset = 0.03 if v >= 0 else -0.05
            ax.text(bar.get_x() + bar.get_width() / 2, v + offset,
                    f"{v:+.3f}%", ha="center",
                    va="bottom" if v >= 0 else "top",
                    fontsize=7.5, fontweight="bold",
                    color=COL_UP_ALL if v >= 0 else COL_DOWN)

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(W_LABELS, fontsize=10)
    ax.set_ylabel(f"Mean CAR vs {bench} (%)", fontsize=9)
    ax.set_title(
        f"Analyst Revisions: Upgrade vs Downgrade Drift — {exchange_label}\n"
        f"({coverage}, vs {bench}, MCap threshold applied)",
        fontsize=11, fontweight="bold"
    )
    ax.legend(fontsize=9, loc="lower left")
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"2_{prefix}_upgrade_vs_downgrade.png")


# ---------------------------------------------------------------------------
# Chart 3: Exchange comparison — T+21 CAR for upgrades
# ---------------------------------------------------------------------------
def chart_exchange_comparison(comparison_data, output_dir):
    """Horizontal bar chart: T+21 upgrade CAR by exchange, sorted."""
    rows = []
    for ex_key, d in comparison_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        up = d["car_metrics"].get("upgrade_all", {})
        c21 = (up.get("T+21") or {}).get("mean_car", None)
        n = up.get("n", 0)
        sig = (up.get("T+21") or {}).get("significant_5pct", False)
        if c21 is not None and n >= 50:
            rows.append((label, c21, n, sig, ex_key))

    if not rows:
        print("  Skipping exchange comparison — no data")
        return

    rows.sort(key=lambda x: x[1], reverse=True)
    labels = [r[0] for r in rows]
    vals = [r[1] for r in rows]
    ns = [r[2] for r in rows]
    sigs = [r[3] for r in rows]
    colours = [COL_POS if v > 0 else COL_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(10, max(5, len(rows) * 0.6 + 2)))
    y = np.arange(len(labels))

    bars = ax.barh(y, vals, color=colours, height=0.55, alpha=0.85)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v, n, sig in zip(bars, vals, ns, sigs):
        sig_marker = " **" if sig else ""
        x_pos = v + 0.02 if v >= 0 else v - 0.02
        ha = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{v:+.3f}%{sig_marker}  (n={n:,})", va="center", ha=ha,
                fontsize=8, fontweight="bold",
                color=COL_POS if v > 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Mean Upgrade CAR at T+21, vs each market's own local index (%)", fontsize=9)
    ax.set_title(
        "Analyst Upgrade Drift at T+21: Exchange Comparison\n"
        "(2012–2025, each market vs its own local index, MCap threshold per exchange. ** = p<0.05)",
        fontsize=11, fontweight="bold"
    )
    ax.grid(axis="x", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    pos_patch = mpatches.Patch(color=COL_POS, label="Positive drift")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Negative drift")
    ax.legend(handles=[pos_patch, neg_patch], loc="lower right", fontsize=8)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    fig.tight_layout()
    return save(fig, output_dir, "3_exchange_comparison_t21.png")


# ---------------------------------------------------------------------------
# Chart 4: Domicile split — who actually generated the "local" drift
# ---------------------------------------------------------------------------
GROUP_ORDER = ["domestic", "us_domiciled", "other_foreign"]
GROUP_LABELS = {"domestic": "Domiciled locally", "us_domiciled": "US-domiciled",
                "other_foreign": "Other foreign"}


def chart_domicile_split(domicile, output_dir, exchange_key, exchange_label, window=63):
    """Upgrade vs downgrade CAR by where the company is domiciled.

    The point of this chart is direction, not size. A real revision signal
    cannot push upgrades and downgrades the same way. Where both bars sit
    above zero, the abnormal return is measuring the gap between that
    company's home market and the local index it is being scored against.
    """
    ex = (domicile or {}).get(exchange_key)
    if not ex:
        print(f"  Skipping domicile split for {exchange_key} — no decomposition data")
        return
    bench = ex.get("benchmark_name", "local index")

    labels, ups, dns, notes = [], [], [], []
    for g in GROUP_ORDER:
        gd = ex["groups"].get(g) or {}
        u = gd.get(f"upgrade_T+{window}")
        d = gd.get(f"downgrade_T+{window}")
        if not u and not d:
            continue
        labels.append(f"{GROUP_LABELS[g]}\n{gd.get('share_pct', 0):.1f}% of events")
        ups.append(u["mean_car"] if u else 0)
        dns.append(d["mean_car"] if d else 0)
        notes.append((u, d))

    if not labels:
        print(f"  Skipping domicile split for {exchange_key} — no group met the size floor")
        return

    x = np.arange(len(labels))
    width = 0.38
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    b_up = ax.bar(x - width / 2, ups, width, color=COL_UP_ALL, alpha=0.85, label="After upgrades")
    b_dn = ax.bar(x + width / 2, dns, width, color=COL_DOWN, alpha=0.85, label="After downgrades")

    span = max(abs(v) for v in ups + dns) or 1
    for bars, vals, idx in ((b_up, ups, 0), (b_dn, dns, 1)):
        for bar, v, note in zip(bars, vals, notes):
            stat = note[idx]
            txt = f"{v:+.2f}%" + ("*" if stat and stat.get("significant_5pct") else "")
            off = span * 0.03 if v >= 0 else -span * 0.03
            ax.text(bar.get_x() + bar.get_width() / 2, v + off, txt, ha="center",
                    va="bottom" if v >= 0 else "top", fontsize=8, fontweight="bold",
                    color=COL_UP_ALL if idx == 0 else COL_DOWN)

    ax.axhline(0, color="black", linewidth=0.9, linestyle="-", alpha=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel(f"Mean CAR at T+{window} vs {bench} (%)", fontsize=9)
    ax.set_title(
        f"Who Produced the {exchange_label} Drift? CAR at T+{window} by Company Domicile\n"
        f"(2012–2025, vs {bench}. Both bars above zero means the benchmark is mismatched, "
        f"not that analysts are right. * = p<0.05)",
        fontsize=10.5, fontweight="bold"
    )
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")
    fig.tight_layout()
    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"4_{prefix}_domicile_split.png")


# ---------------------------------------------------------------------------
# Chart 5: cross-market view of the same artifact
# ---------------------------------------------------------------------------
def chart_domicile_comparison(domicile, output_dir, window=63):
    """Foreign-listing share per market, next to the upgrade/downgrade tell."""
    if not domicile:
        print("  Skipping domicile comparison — no decomposition data")
        return

    order = [k for k in ["XETRA", "LSE", "SIX", "TSX"] if k in domicile]
    if not order:
        return
    names = {"XETRA": "Germany\n(XETRA)", "LSE": "UK\n(LSE)",
             "SIX": "Switzerland\n(SIX)", "TSX": "Canada\n(TSX)"}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.4))

    # Panel A: share of events by domicile
    dom = [domicile[k]["groups"]["domestic"]["share_pct"] for k in order]
    usd = [domicile[k]["groups"]["us_domiciled"]["share_pct"] for k in order]
    oth = [domicile[k]["groups"]["other_foreign"]["share_pct"] for k in order]
    y = np.arange(len(order))
    ax1.barh(y, dom, color=COL_UP_ALL, alpha=0.85, label="Domiciled locally")
    ax1.barh(y, usd, left=dom, color=COL_DOWN, alpha=0.85, label="US-domiciled")
    ax1.barh(y, oth, left=[a + b for a, b in zip(dom, usd)], color=COL_NEUTRAL,
             alpha=0.7, label="Other foreign")
    # Inline labels only where the segment is wide enough to hold one. The
    # local share is the number that matters and is often tiny, so it is
    # always written in the clear margin to the right of the bar.
    for i, (d, u) in enumerate(zip(dom, usd)):
        if d >= 8:
            ax1.text(d / 2, i, f"{d:.0f}%", ha="center", va="center", fontsize=8.5,
                     fontweight="bold", color="white")
        if u >= 8:
            ax1.text(d + u / 2, i, f"{u:.0f}%", ha="center", va="center", fontsize=8.5,
                     fontweight="bold", color="white")
        ax1.text(103, i, f"local {d:.1f}%", va="center", ha="left", fontsize=8.5,
                 fontweight="bold", color=COL_UP_ALL)
    ax1.set_yticks(y)
    ax1.set_yticklabels([names.get(k, k) for k in order], fontsize=9)
    ax1.set_xlabel("Share of analyst revision events (%)", fontsize=9)
    ax1.set_xlim(0, 124)
    ax1.set_xticks([0, 20, 40, 60, 80, 100])
    ax1.set_title("Who is listed on each exchange\n(share of graded events, 2012–2025)",
                  fontsize=10.5, fontweight="bold")
    ax1.legend(fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=3,
               frameon=False)
    ax1.grid(axis="x", alpha=0.3)
    for sp in ("top", "right"):
        ax1.spines[sp].set_visible(False)

    # Panel B: the tell, for the US-domiciled slice of each market
    ups, dns, keep = [], [], []
    for k in order:
        g = domicile[k]["groups"]["us_domiciled"]
        u, d = g.get(f"upgrade_T+{window}"), g.get(f"downgrade_T+{window}")
        if not u or not d:
            continue
        keep.append(k)
        ups.append(u["mean_car"])
        dns.append(d["mean_car"])
    x = np.arange(len(keep))
    width = 0.38
    ax2.bar(x - width / 2, ups, width, color=COL_UP_ALL, alpha=0.85, label="After upgrades")
    ax2.bar(x + width / 2, dns, width, color=COL_DOWN, alpha=0.85, label="After downgrades")
    span = max([abs(v) for v in ups + dns] or [1])
    for xi, v in list(zip(x - width / 2, ups)) + list(zip(x + width / 2, dns)):
        ax2.text(xi, v + (span * 0.03 if v >= 0 else -span * 0.03), f"{v:+.2f}%",
                 ha="center", va="bottom" if v >= 0 else "top", fontsize=8, fontweight="bold")
    ax2.axhline(0, color="black", linewidth=0.9, alpha=0.6)
    ax2.set_xticks(x)
    ax2.set_xticklabels([names.get(k, k) for k in keep], fontsize=9)
    ax2.set_ylabel(f"Mean CAR at T+{window} vs local index (%)", fontsize=9)
    ax2.set_title("US-domiciled listings, scored against the local index\n"
                  "Upgrades beat the index in every market, downgrades in three of four. "
                  "That is the benchmark, not the analyst.",
                  fontsize=10, fontweight="bold")
    ax2.legend(fontsize=8)
    ax2.grid(axis="y", alpha=0.3)
    for sp in ("top", "right"):
        ax2.spines[sp].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")
    fig.tight_layout()
    return save(fig, output_dir, "5_domicile_contamination.png")


# Exchanges with blog posts
ALL_BLOG_EXCHANGES = [
    ("NYSE_NASDAQ_AMEX", "US"),
    ("LSE", "UK"),
    ("XETRA", "Germany"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate Analyst Revision charts")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--exchange", default="NYSE_NASDAQ_AMEX")
    parser.add_argument("--label", default=None)
    parser.add_argument("--all-exchanges", action="store_true",
                        help="Generate per-exchange charts for all blog exchanges")
    parser.add_argument("--no-comparison", action="store_true")
    args = parser.parse_args()

    ensure_output(args.output)

    exchanges_to_run = ALL_BLOG_EXCHANGES if args.all_exchanges else [
        (args.exchange, args.label or EXCHANGE_LABELS.get(args.exchange, args.exchange))
    ]

    domicile = load_domicile()
    if domicile is None:
        print("  No domicile_decomposition.json found. "
              "Run domicile_analysis.py for the domicile charts.")

    for ex_key, ex_label in exchanges_to_run:
        print(f"\nGenerating charts for {ex_label} ({ex_key})...")
        try:
            ex_data = load_exchange_data(ex_key)
        except FileNotFoundError as e:
            print(f"  Error: {e}")
            continue
        chart_car_progression(ex_data, args.output, ex_label, ex_key)
        chart_upgrade_vs_downgrade(ex_data, args.output, ex_label, ex_key)
        if domicile and ex_key in domicile:
            chart_domicile_split(domicile, args.output, ex_key, ex_label)

    if not args.no_comparison:
        comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
        if os.path.exists(comparison_path):
            print("\nGenerating exchange comparison chart...")
            comparison = load_comparison()
            chart_exchange_comparison(comparison, args.output)
            chart_domicile_comparison(domicile, args.output)
        else:
            print("  No exchange_comparison.json found. Run --global first.")

    print(f"\nDone. Charts saved to: {args.output}/")


if __name__ == "__main__":
    main()
