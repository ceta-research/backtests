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
    ax.set_ylabel("Mean CAR vs benchmark (%)", fontsize=9)
    ax.set_title(
        f"Analyst Rating Revisions: Post-Event Drift — {exchange_label}\n"
        f"({coverage}, MCap threshold applied, winsorized mean)",
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
    ax.set_ylabel("Mean CAR vs benchmark (%)", fontsize=9)
    ax.set_title(
        f"Analyst Revisions: Upgrade vs Downgrade Drift — {exchange_label}\n"
        f"({coverage}, MCap threshold applied)",
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
    ax.set_xlabel("Mean Upgrade CAR at T+21 (%)", fontsize=9)
    ax.set_title(
        "Analyst Upgrade Drift at T+21: Exchange Comparison\n"
        "(2012–2025, individual grade changes, MCap threshold per exchange. ** = p<0.05)",
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

    for ex_key, ex_label in exchanges_to_run:
        print(f"\nGenerating charts for {ex_label} ({ex_key})...")
        try:
            ex_data = load_exchange_data(ex_key)
        except FileNotFoundError as e:
            print(f"  Error: {e}")
            continue
        chart_car_progression(ex_data, args.output, ex_label, ex_key)
        chart_upgrade_vs_downgrade(ex_data, args.output, ex_label, ex_key)

    if not args.no_comparison:
        comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
        if os.path.exists(comparison_path):
            print("\nGenerating exchange comparison chart...")
            comparison = load_comparison()
            chart_exchange_comparison(comparison, args.output)
        else:
            print("  No exchange_comparison.json found. Run --global first.")

    print(f"\nDone. Charts saved to: {args.output}/")


if __name__ == "__main__":
    main()
