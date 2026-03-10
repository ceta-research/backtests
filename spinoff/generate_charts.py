#!/usr/bin/env python3
"""
Generate charts for the Corporate Spinoff Event Study.

Charts produced:
  1. 1_spinoff_car_trajectory.png  -- CAR over time (parent vs child lines)
  2. 2_spinoff_individual_t252.png -- Individual spinoff T+252 CARs (bar chart)

Usage:
    cd backtests
    python3 spinoff/generate_charts.py
    python3 spinoff/generate_charts.py --output spinoff/charts/
"""

import csv
import json
import os
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "charts")

# Colors
COL_PARENT = "#2563eb"   # blue
COL_CHILD  = "#16a34a"   # green
COL_POS    = "#16a34a"
COL_NEG    = "#dc2626"
COL_GRID   = "#e5e7eb"

WINDOWS = [1, 5, 21, 63, 126, 252]
WINDOW_LABELS = ["T+1", "T+5", "T+21", "T+63", "T+126", "T+252"]


def load_results(results_dir):
    """Load summary_metrics.json and individual_spinoffs.csv."""
    summary_path = os.path.join(results_dir, "summary_metrics.json")
    csv_path = os.path.join(results_dir, "individual_spinoffs.csv")

    with open(summary_path) as f:
        summary = json.load(f)

    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    return summary, rows


def chart_car_trajectory(summary, output_path):
    """
    Chart 1: CAR trajectory by window — parent vs child lines with confidence bands.
    """
    categories = summary.get("categories", {})

    parent_data = categories.get("parent", {}).get("windows", {})
    child_data  = categories.get("child",  {}).get("windows", {})

    parent_means = []
    child_means  = []
    parent_ns    = []
    child_ns     = []

    for wl in WINDOW_LABELS:
        p = parent_data.get(wl, {})
        c = child_data.get(wl, {})
        parent_means.append(p.get("mean_car", 0))
        child_means.append(c.get("mean_car", 0))
        parent_ns.append(p.get("n", 0))
        child_ns.append(c.get("n", 0))

    x = np.arange(len(WINDOW_LABELS))

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    ax.axhline(0, color="#6b7280", linewidth=0.8, linestyle="--")

    ax.plot(x, parent_means, color=COL_PARENT, linewidth=2.2, marker="o",
            markersize=7, label="Parent company", zorder=3)
    ax.plot(x, child_means, color=COL_CHILD, linewidth=2.2, marker="s",
            markersize=7, label="Spinoff child", zorder=3)

    # Annotate end values
    ax.annotate(f"{parent_means[-1]:+.1f}%",
                xy=(x[-1], parent_means[-1]), xytext=(5, 4),
                textcoords="offset points", color=COL_PARENT, fontsize=9, fontweight="bold")
    ax.annotate(f"{child_means[-1]:+.1f}%",
                xy=(x[-1], child_means[-1]), xytext=(5, -12),
                textcoords="offset points", color=COL_CHILD, fontsize=9, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(WINDOW_LABELS, fontsize=10)
    ax.set_ylabel("Cumulative Abnormal Return vs SPY (%)", fontsize=10)
    ax.set_xlabel("Trading Days After Spinoff", fontsize=10)
    ax.set_title("Corporate Spinoffs: Abnormal Returns Over Time\nParent vs Spinoff Child (Mean CAR)",
                 fontsize=12, fontweight="bold", pad=12)

    ax.yaxis.grid(True, color=COL_GRID, linewidth=0.7)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    legend = ax.legend(fontsize=10, framealpha=0.9, loc="upper left")

    # Add sample size annotation
    note = f"Parent n={parent_ns[0]}  |  Child n={child_ns[0]}  |  Winsorized 1%"
    ax.text(0.01, 0.02, note, transform=ax.transAxes,
            fontsize=8, color="#6b7280", va="bottom")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"  Saved: {output_path}")


def chart_individual_t252(rows, output_path):
    """
    Chart 2: Individual spinoff children by T+252 CAR, sorted, colored by sign.
    Shows all children with T+252 data.
    """
    children = []
    for r in rows:
        if r.get("category") != "child":
            continue
        ar = r.get("ar_252")
        if ar is None or ar == "":
            continue
        try:
            ar_val = float(ar)
        except (ValueError, TypeError):
            continue
        label = r["symbol"]
        children.append((label, ar_val, r.get("description", "")))

    if not children:
        print("  No child T+252 data available for individual chart.")
        return

    # Sort descending
    children.sort(key=lambda x: x[1], reverse=True)
    labels = [c[0] for c in children]
    values = [c[1] for c in children]
    colors = [COL_POS if v >= 0 else COL_NEG for v in values]

    fig, ax = plt.subplots(figsize=(12, max(6, len(children) * 0.45)))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    y = np.arange(len(labels))
    bars = ax.barh(y, values, color=colors, height=0.65, alpha=0.85, zorder=3)

    ax.axvline(0, color="#374151", linewidth=1.0)

    # Value labels on bars
    for bar, val in zip(bars, values):
        x_pos = val + (1 if val >= 0 else -1)
        ha = "left" if val >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{val:+.0f}%", va="center", ha=ha, fontsize=8,
                color="#111827", fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Cumulative Abnormal Return vs SPY (%)", fontsize=10)
    ax.set_title("Spinoff Children: 1-Year Abnormal Return (T+252)\nAll Events with Complete Data",
                 fontsize=12, fontweight="bold", pad=12)

    ax.xaxis.grid(True, color=COL_GRID, linewidth=0.7)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend patches
    pos_patch = mpatches.Patch(color=COL_POS, label="Outperformed SPY")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Underperformed SPY")
    ax.legend(handles=[pos_patch, neg_patch], fontsize=9, loc="lower right")

    n_pos = sum(1 for v in values if v >= 0)
    note = f"n={len(children)} spinoff children  |  {n_pos}/{len(children)} outperformed SPY at T+252"
    ax.text(0.01, 0.99, note, transform=ax.transAxes,
            fontsize=8, color="#6b7280", va="top")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"  Saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate spinoff event study charts")
    parser.add_argument("--results", type=str, default=RESULTS_DIR,
                        help=f"Results directory (default: {RESULTS_DIR})")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT,
                        help=f"Chart output directory (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("Loading results...")
    summary, rows = load_results(args.results)

    n_spinoffs = summary.get("n_spinoffs", "?")
    cats = summary.get("categories", {})
    n_parent = cats.get("parent", {}).get("n", "?")
    n_child  = cats.get("child",  {}).get("n", "?")
    print(f"  {n_spinoffs} spinoffs, {n_parent} parent events, {n_child} child events")

    print("\nGenerating charts...")
    chart_car_trajectory(
        summary,
        os.path.join(args.output, "1_spinoff_car_trajectory.png")
    )
    chart_individual_t252(
        rows,
        os.path.join(args.output, "2_spinoff_individual_t252.png")
    )

    print("\nDone. Charts saved to:", args.output)
    print("\nNext step: Move charts to ts-content-creator/content/_current/event-05-spinoff/blogs/us/")


if __name__ == "__main__":
    main()
