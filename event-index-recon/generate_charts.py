#!/usr/bin/env python3
"""
Generate charts for the Index Reconstitution event study.

Reads from results/ directory (produced by backtest.py) and generates
publication-ready PNG charts for use in blog posts.

Charts produced:
  1. car_by_window.png        - Mean CAR at T+1/T+5/T+21/T+63 for both indices
  2. car_additions.png        - Addition drift: S&P 500 vs NASDAQ-100
  3. car_removals.png         - Removal recovery: S&P 500 vs NASDAQ-100
  4. removal_distribution.png - Box plot of T+21 CAR distribution (shows outlier issue)

Usage:
    cd backtests
    python3 event-index-recon/generate_charts.py

    # Custom directories
    python3 event-index-recon/generate_charts.py \\
        --results-dir event-index-recon/results \\
        --output-dir event-index-recon/charts
"""

import argparse
import csv
import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
except ImportError:
    print("matplotlib and numpy required: pip install matplotlib numpy")
    sys.exit(1)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "charts")

WINDOWS = ["T+1", "T+5", "T+21", "T+63"]

COLORS = {
    "sp500_addition": "#c0392b",
    "sp500_removal": "#27ae60",
    "ndx_addition": "#e67e22",
    "ndx_removal": "#2980b9",
    "zero": "#bdc3c7",
    "sig_marker": "#f39c12",
}

STYLE = {
    "fig_width": 10,
    "fig_height": 6,
    "bar_width": 0.35,
    "fontsize_title": 14,
    "fontsize_axis": 11,
    "fontsize_tick": 10,
    "fontsize_annot": 9,
    "dpi": 150,
}


def load_results(results_dir):
    """Load SP500 and NDX result JSONs."""
    results = {}
    for slug, fname in [("sp500", "results_SP500.json"), ("ndx", "results_NDX.json")]:
        path = os.path.join(results_dir, fname)
        if not os.path.exists(path):
            print(f"  WARNING: {path} not found — skipping")
            continue
        with open(path) as f:
            results[slug] = json.load(f)
    return results


def load_event_returns(results_dir):
    """Load event-level CSV files. Returns {slug: [row_dict, ...]}."""
    data = {}
    for slug, fname in [("sp500", "event_returns_SP500.csv"), ("ndx", "event_returns_NDX.csv")]:
        path = os.path.join(results_dir, fname)
        if not os.path.exists(path):
            continue
        rows = []
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        data[slug] = rows
    return data


def get_car_values(results, slug, category, window):
    """Extract mean CAR, t-stat, significance for one cell."""
    car_data = (results.get(slug, {})
                .get("car_summary", {})
                .get(category, {})
                .get("windows", {})
                .get(window, {}))
    return (
        car_data.get("mean_car_pct"),
        car_data.get("t_stat"),
        car_data.get("sig_5pct", False),
        car_data.get("sig_1pct", False),
        car_data.get("n", 0),
    )


def add_sig_label(ax, x, y, sig_5, sig_1, offset=0.05):
    """Add *** / ** / * annotation above/below a bar."""
    if sig_1:
        label = "**"
    elif sig_5:
        label = "*"
    else:
        return
    direction = 1 if y >= 0 else -1
    ax.text(x, y + direction * offset, label, ha="center", va="bottom" if direction > 0 else "top",
            fontsize=STYLE["fontsize_annot"] + 1, color=COLORS["sig_marker"], fontweight="bold")


def chart_car_by_window(results, output_dir):
    """Bar chart: mean CAR at each window for additions and removals, both indices."""
    fig, axes = plt.subplots(1, 2, figsize=(STYLE["fig_width"] * 1.4, STYLE["fig_height"]),
                             sharey=False)
    fig.suptitle("Index Reconstitution — Mean Cumulative Abnormal Return",
                 fontsize=STYLE["fontsize_title"], fontweight="bold", y=1.02)

    for ax_idx, (category, ax) in enumerate(zip(["addition", "removal"], axes)):
        x = np.arange(len(WINDOWS))
        bw = STYLE["bar_width"]

        sp500_vals = [get_car_values(results, "sp500", category, w) for w in WINDOWS]
        ndx_vals = [get_car_values(results, "ndx", category, w) for w in WINDOWS]

        sp500_cars = [v[0] if v[0] is not None else 0 for v in sp500_vals]
        ndx_cars = [v[0] if v[0] is not None else 0 for v in ndx_vals]

        col_sp500 = COLORS[f"sp500_{category}"]
        col_ndx = COLORS[f"ndx_{category}"]

        bars1 = ax.bar(x - bw / 2, sp500_cars, bw, color=col_sp500, alpha=0.85,
                       label="S&P 500", edgecolor="white", linewidth=0.5)
        bars2 = ax.bar(x + bw / 2, ndx_cars, bw, color=col_ndx, alpha=0.85,
                       label="NASDAQ-100", edgecolor="white", linewidth=0.5)

        ax.axhline(0, color=COLORS["zero"], linewidth=1.0, linestyle="--")

        for i, (v, bar) in enumerate(zip(sp500_vals, bars1)):
            add_sig_label(ax, bar.get_x() + bar.get_width() / 2, sp500_cars[i],
                          v[2], v[3], offset=0.12)
        for i, (v, bar) in enumerate(zip(ndx_vals, bars2)):
            add_sig_label(ax, bar.get_x() + bar.get_width() / 2, ndx_cars[i],
                          v[2], v[3], offset=0.12)

        ax.set_xticks(x)
        ax.set_xticklabels(WINDOWS, fontsize=STYLE["fontsize_tick"])
        ax.set_xlabel("Window (trading days)", fontsize=STYLE["fontsize_axis"])
        ax.set_ylabel("Mean CAR (%)", fontsize=STYLE["fontsize_axis"])
        label = "Additions" if category == "addition" else "Removals"
        ax.set_title(label, fontsize=STYLE["fontsize_axis"] + 1, fontweight="semibold")
        ax.legend(fontsize=STYLE["fontsize_tick"])
        ax.yaxis.set_tick_params(labelsize=STYLE["fontsize_tick"])
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.figtext(0.5, -0.04, "* p<0.05   ** p<0.01 (two-tailed t-test). 2000-2025. Data: Ceta Research / FMP.",
                ha="center", fontsize=STYLE["fontsize_annot"] - 1, color="gray")

    fig.tight_layout()
    path = os.path.join(output_dir, "car_by_window.png")
    fig.savefig(path, dpi=STYLE["dpi"], bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_removal_distribution(event_data, output_dir):
    """Box plot of T+21 CAR distribution for removals — shows outlier contamination."""
    fig, axes = plt.subplots(1, 2, figsize=(STYLE["fig_width"], STYLE["fig_height"]),
                             sharey=False)
    fig.suptitle("S&P 500 vs NASDAQ-100 Removals: T+21 CAR Distribution",
                 fontsize=STYLE["fontsize_title"], fontweight="bold", y=1.02)

    for ax, (slug, label, color) in zip(axes, [
        ("sp500", "S&P 500", COLORS["sp500_removal"]),
        ("ndx", "NASDAQ-100", COLORS["ndx_removal"]),
    ]):
        rows = event_data.get(slug, [])
        cars = []
        for row in rows:
            if row.get("category") == "removal":
                val = row.get("car_T21")
                if val not in (None, ""):
                    try:
                        cars.append(float(val))
                    except ValueError:
                        pass

        if not cars:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            continue

        bp = ax.boxplot([cars], vert=True, patch_artist=True,
                        boxprops=dict(facecolor=color, alpha=0.6),
                        medianprops=dict(color="black", linewidth=2),
                        whiskerprops=dict(color="gray"),
                        capprops=dict(color="gray"),
                        flierprops=dict(marker="o", color=color, alpha=0.4, markersize=4))

        median = sorted(cars)[len(cars) // 2]
        mean = sum(cars) / len(cars)
        ax.axhline(mean, color="navy", linewidth=1.5, linestyle="--", alpha=0.8, label=f"Mean: {mean:+.1f}%")
        ax.axhline(0, color=COLORS["zero"], linewidth=1.0, linestyle=":")

        ax.set_title(f"{label}\nN={len(cars)}, Median={median:+.1f}%, Mean={mean:+.1f}%",
                     fontsize=STYLE["fontsize_axis"])
        ax.set_ylabel("T+21 CAR (%)", fontsize=STYLE["fontsize_axis"])
        ax.set_xticks([])
        ax.legend(fontsize=STYLE["fontsize_tick"])
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.figtext(0.5, -0.04,
                "Box: IQR. Whiskers: 1.5x IQR. Dots: outliers. 2000-2025. Data: Ceta Research / FMP.",
                ha="center", fontsize=STYLE["fontsize_annot"] - 1, color="gray")

    fig.tight_layout()
    path = os.path.join(output_dir, "removal_distribution.png")
    fig.savefig(path, dpi=STYLE["dpi"], bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_removal_car_comparison(results, output_dir):
    """Side-by-side: S&P 500 vs NDX removal CAR at all windows."""
    fig, ax = plt.subplots(figsize=(STYLE["fig_width"], STYLE["fig_height"]))
    fig.suptitle("Removal Recovery: S&P 500 vs NASDAQ-100",
                 fontsize=STYLE["fontsize_title"], fontweight="bold")

    x = np.arange(len(WINDOWS))
    bw = STYLE["bar_width"]

    sp500_vals = [get_car_values(results, "sp500", "removal", w) for w in WINDOWS]
    ndx_vals = [get_car_values(results, "ndx", "removal", w) for w in WINDOWS]
    sp500_cars = [v[0] if v[0] is not None else 0 for v in sp500_vals]
    ndx_cars = [v[0] if v[0] is not None else 0 for v in ndx_vals]
    sp500_ns = [v[4] for v in sp500_vals]
    ndx_ns = [v[4] for v in ndx_vals]

    bars1 = ax.bar(x - bw / 2, sp500_cars, bw, color=COLORS["sp500_removal"], alpha=0.85,
                   label="S&P 500", edgecolor="white")
    bars2 = ax.bar(x + bw / 2, ndx_cars, bw, color=COLORS["ndx_removal"], alpha=0.85,
                   label="NASDAQ-100", edgecolor="white")

    ax.axhline(0, color=COLORS["zero"], linewidth=1.0, linestyle="--")

    for i, (v, bar) in enumerate(zip(sp500_vals, bars1)):
        add_sig_label(ax, bar.get_x() + bar.get_width() / 2, sp500_cars[i], v[2], v[3], offset=0.15)
        ax.text(bar.get_x() + bar.get_width() / 2, -0.4,
                f"N={sp500_ns[i]}", ha="center", va="top",
                fontsize=STYLE["fontsize_annot"] - 1, color="gray")
    for i, (v, bar) in enumerate(zip(ndx_vals, bars2)):
        add_sig_label(ax, bar.get_x() + bar.get_width() / 2, ndx_cars[i], v[2], v[3], offset=0.15)
        ax.text(bar.get_x() + bar.get_width() / 2, -0.4,
                f"N={ndx_ns[i]}", ha="center", va="top",
                fontsize=STYLE["fontsize_annot"] - 1, color="gray")

    ax.set_xticks(x)
    ax.set_xticklabels(WINDOWS, fontsize=STYLE["fontsize_tick"])
    ax.set_xlabel("Window (trading days)", fontsize=STYLE["fontsize_axis"])
    ax.set_ylabel("Mean CAR (%)", fontsize=STYLE["fontsize_axis"])
    ax.legend(fontsize=STYLE["fontsize_tick"])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.text(0.98, 0.98,
            "S&P 500 T+21: +7.22% mean (+0.73% median)\nNDX T+21: +5.13%** (median +2.61%)",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=STYLE["fontsize_annot"], color="gray",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="lightgray"))

    plt.figtext(0.5, -0.04, "* p<0.05   ** p<0.01 (two-tailed t-test). 2000-2025. Data: Ceta Research / FMP.",
                ha="center", fontsize=STYLE["fontsize_annot"] - 1, color="gray")

    fig.tight_layout()
    path = os.path.join(output_dir, "removal_car_comparison.png")
    fig.savefig(path, dpi=STYLE["dpi"], bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_addition_car_comparison(results, output_dir):
    """Side-by-side: S&P 500 vs NDX addition CAR at all windows."""
    fig, ax = plt.subplots(figsize=(STYLE["fig_width"], STYLE["fig_height"]))
    fig.suptitle("Addition Drift: S&P 500 vs NASDAQ-100",
                 fontsize=STYLE["fontsize_title"], fontweight="bold")

    x = np.arange(len(WINDOWS))
    bw = STYLE["bar_width"]

    sp500_vals = [get_car_values(results, "sp500", "addition", w) for w in WINDOWS]
    ndx_vals = [get_car_values(results, "ndx", "addition", w) for w in WINDOWS]
    sp500_cars = [v[0] if v[0] is not None else 0 for v in sp500_vals]
    ndx_cars = [v[0] if v[0] is not None else 0 for v in ndx_vals]
    sp500_ns = [v[4] for v in sp500_vals]
    ndx_ns = [v[4] for v in ndx_vals]

    bars1 = ax.bar(x - bw / 2, sp500_cars, bw, color=COLORS["sp500_addition"], alpha=0.85,
                   label="S&P 500", edgecolor="white")
    bars2 = ax.bar(x + bw / 2, ndx_cars, bw, color=COLORS["ndx_addition"], alpha=0.85,
                   label="NASDAQ-100", edgecolor="white")

    ax.axhline(0, color=COLORS["zero"], linewidth=1.0, linestyle="--")

    for i, (v, bar) in enumerate(zip(sp500_vals, bars1)):
        add_sig_label(ax, bar.get_x() + bar.get_width() / 2, sp500_cars[i], v[2], v[3], offset=0.05)
        ax.text(bar.get_x() + bar.get_width() / 2, 0.15,
                f"N={sp500_ns[i]}", ha="center", va="bottom",
                fontsize=STYLE["fontsize_annot"] - 1, color="gray")
    for i, (v, bar) in enumerate(zip(ndx_vals, bars2)):
        add_sig_label(ax, bar.get_x() + bar.get_width() / 2, ndx_cars[i], v[2], v[3], offset=0.05)
        ax.text(bar.get_x() + bar.get_width() / 2, 0.15,
                f"N={ndx_ns[i]}", ha="center", va="bottom",
                fontsize=STYLE["fontsize_annot"] - 1, color="gray")

    ax.set_xticks(x)
    ax.set_xticklabels(WINDOWS, fontsize=STYLE["fontsize_tick"])
    ax.set_xlabel("Window (trading days)", fontsize=STYLE["fontsize_axis"])
    ax.set_ylabel("Mean CAR (%)", fontsize=STYLE["fontsize_axis"])
    ax.legend(fontsize=STYLE["fontsize_tick"])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.figtext(0.5, -0.04, "* p<0.05   ** p<0.01 (two-tailed t-test). 2000-2025. Data: Ceta Research / FMP.",
                ha="center", fontsize=STYLE["fontsize_annot"] - 1, color="gray")

    fig.tight_layout()
    path = os.path.join(output_dir, "addition_car_comparison.png")
    fig.savefig(path, dpi=STYLE["dpi"], bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate index reconstitution charts")
    parser.add_argument("--results-dir", default=RESULTS_DIR,
                        help=f"Directory with results JSON files (default: {RESULTS_DIR})")
    parser.add_argument("--output-dir", default=CHARTS_DIR,
                        help=f"Output directory for PNG charts (default: {CHARTS_DIR})")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Loading results from: {args.results_dir}")
    results = load_results(args.results_dir)
    event_data = load_event_returns(args.results_dir)

    if not results:
        print("No results found. Run backtest.py --global first.")
        sys.exit(1)

    print(f"Generating charts to: {args.output_dir}\n")

    chart_car_by_window(results, args.output_dir)
    chart_addition_car_comparison(results, args.output_dir)
    chart_removal_car_comparison(results, args.output_dir)

    if event_data:
        chart_removal_distribution(event_data, args.output_dir)
    else:
        print("  Skipping distribution chart (no event CSV data)")

    print(f"\nDone. {len(os.listdir(args.output_dir))} files in {args.output_dir}")


if __name__ == "__main__":
    main()
