#!/usr/bin/env python3
"""
Generate charts for the Post-Stock Split Performance event study.

Reads from results/summary_metrics.json and results/event_frequency.csv.

Charts:
    1_car_by_window.png       - CAR at each event window (overall + by ratio)
    2_car_by_category.png     - CAR at T+63 and T+252 by split ratio category
    3_split_frequency.png     - Annual split frequency 2000-2025

Usage:
    python3 stock-split/generate_charts.py
    python3 stock-split/generate_charts.py --results stock-split/results --output stock-split/charts
"""

import argparse
import csv
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ─── Style ────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "#ffffff",
    "axes.facecolor": "#f8f9fa",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "font.family": "sans-serif",
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.dpi": 150,
})

POSITIVE_COLOR = "#16a34a"   # Green
NEGATIVE_COLOR = "#dc2626"   # Red
NEUTRAL_COLOR = "#2563eb"    # Blue
GRAY_COLOR = "#94a3b8"       # Gray
RATIO_COLORS = {
    "2-for-1":  "#2563eb",
    "3-for-1":  "#7c3aed",
    "4-for-1":  "#ea580c",
    "5-for-1+": "#dc2626",
    "other":    "#94a3b8",
}


# ─── Chart 1: CAR by Window ──────────────────────────────────────────────────

def chart_car_by_window(metrics, output_path):
    """Bar chart: overall CAR at each event window."""
    windows_raw = list(metrics["cumulative_abnormal_returns"].items())
    labels = [k for k, v in windows_raw if v]
    means = [v["mean_car"] for _, v in windows_raw if v]
    t_stats = [v["t_stat"] for _, v in windows_raw if v]
    ns = [v["n"] for _, v in windows_raw if v]

    colors = [POSITIVE_COLOR if m > 0 else NEGATIVE_COLOR for m in means]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    bars = ax.bar(labels, means, color=colors, alpha=0.82, width=0.6, edgecolor="white")

    ax.axhline(y=0, color="black", linewidth=0.8)
    ax.set_title("Post-Stock Split: Cumulative Abnormal Returns by Event Window")
    ax.set_xlabel("Event Window (trading days from split)")
    ax.set_ylabel("Mean CAR vs SPY (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.1f}%"))

    # Annotate bars with mean and significance
    for bar, mean, t, n in zip(bars, means, t_stats, ns):
        sig = "**" if abs(t) > 2.576 else ("*" if abs(t) > 1.96 else "")
        label = f"{mean:+.2f}%{sig}"
        ypos = bar.get_height() + 0.05 if mean >= 0 else bar.get_height() - 0.15
        ax.text(bar.get_x() + bar.get_width() / 2, ypos, label,
                ha="center", va="bottom" if mean >= 0 else "top",
                fontsize=9, fontweight="bold",
                color=POSITIVE_COLOR if mean > 0 else NEGATIVE_COLOR)

    # Add N below axis
    for bar, n in zip(bars, ns):
        ax.text(bar.get_x() + bar.get_width() / 2, ax.get_ylim()[0] * 0.95,
                f"N={n:,}", ha="center", va="top", fontsize=7.5, color="#6b7280")

    ax.text(0.99, 0.01, "* p<0.05  ** p<0.01", transform=ax.transAxes,
            ha="right", va="bottom", fontsize=8, color="#6b7280")

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()
    print(f"  Saved {os.path.basename(output_path)}")


# ─── Chart 2: CAR by Split Ratio Category ────────────────────────────────────

def chart_car_by_category(metrics, output_path):
    """Grouped bar chart: CAR at T+63 and T+252 by split ratio category."""
    by_cat = metrics["by_category"]
    categories = ["2-for-1", "3-for-1", "4-for-1", "5-for-1+", "other"]
    windows = ["T+63", "T+252"]
    window_labels = ["3 Months (T+63)", "1 Year (T+252)"]

    data = {}
    for w in windows:
        data[w] = []
        for cat in categories:
            cat_data = by_cat.get(cat, {}).get("windows", {}).get(w, {})
            data[w].append(cat_data.get("mean_car", 0) if cat_data else 0)

    x = np.arange(len(categories))
    width = 0.38

    fig, ax = plt.subplots(figsize=(12, 5.5))
    bars1 = ax.bar(x - width / 2, data["T+63"], width, label=window_labels[0],
                   color=NEUTRAL_COLOR, alpha=0.82)
    bars2 = ax.bar(x + width / 2, data["T+252"], width, label=window_labels[1],
                   color=GRAY_COLOR, alpha=0.75)

    ax.axhline(y=0, color="black", linewidth=0.8)
    ax.set_title("Post-Split Underperformance by Split Ratio Category")
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel("Mean CAR vs SPY (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.1f}%"))
    ax.legend(loc="lower left", framealpha=0.9)

    # Annotate bars
    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            if abs(h) > 0.01:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        h - 0.1 if h < 0 else h + 0.05,
                        f"{h:+.1f}%", ha="center",
                        va="top" if h < 0 else "bottom", fontsize=8)

    # Add N counts
    for i, cat in enumerate(categories):
        n = by_cat.get(cat, {}).get("n", 0)
        ax.text(x[i], ax.get_ylim()[0] * 0.97, f"N={n:,}",
                ha="center", va="top", fontsize=7.5, color="#6b7280")

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()
    print(f"  Saved {os.path.basename(output_path)}")


# ─── Chart 3: Split Frequency Over Time ──────────────────────────────────────

def chart_split_frequency(freq_path, output_path):
    """Stacked bar chart: annual split count by ratio category."""
    rows = []
    with open(freq_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k: int(v) for k, v in row.items()})

    if not rows:
        print("  Skipped split frequency chart (no data)")
        return

    years = [r["year"] for r in rows]
    cats = ["2-for-1", "3-for-1", "4-for-1", "5-for-1+", "other"]
    cat_cols = ["2-for-1", "3-for-1", "4-for-1", "5-for-1+", "other"]
    colors = [RATIO_COLORS[c] for c in cats]

    fig, ax = plt.subplots(figsize=(13, 5))
    bottom = np.zeros(len(years))
    x = np.arange(len(years))

    for cat, col, color in zip(cats, cat_cols, colors):
        vals = np.array([r.get(col, 0) for r in rows])
        ax.bar(x, vals, bottom=bottom, label=cat, color=color, alpha=0.80, width=0.8)
        bottom += vals

    ax.set_title("Annual Forward Stock Splits by Ratio Category (2000-2025)")
    ax.set_xticks(x[::2])
    ax.set_xticklabels([str(years[i]) for i in range(0, len(years), 2)],
                       rotation=45, ha="right")
    ax.set_ylabel("Number of Forward Splits")
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()
    print(f"  Saved {os.path.basename(output_path)}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate stock split event study charts")
    parser.add_argument("--results", type=str, default="stock-split/results",
                        help="Results directory (default: stock-split/results)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory (default: same as --results)")
    args = parser.parse_args()

    results_dir = args.results
    output_dir = args.output or results_dir

    metrics_path = os.path.join(results_dir, "summary_metrics.json")
    freq_path = os.path.join(results_dir, "event_frequency.csv")

    if not os.path.exists(metrics_path):
        print(f"ERROR: {metrics_path} not found. Run backtest.py first.")
        return

    with open(metrics_path) as f:
        metrics = json.load(f)

    os.makedirs(output_dir, exist_ok=True)
    print(f"Generating charts from {results_dir}...")

    chart_car_by_window(metrics, os.path.join(output_dir, "1_car_by_window.png"))
    chart_car_by_category(metrics, os.path.join(output_dir, "2_car_by_category.png"))

    if os.path.exists(freq_path):
        chart_split_frequency(freq_path, os.path.join(output_dir, "3_split_frequency.png"))
    else:
        print(f"  Skipped split frequency chart (no {freq_path})")

    print(f"\nDone. Charts saved to {output_dir}/")


if __name__ == "__main__":
    main()
