#!/usr/bin/env python3
"""
Generate charts for PEAD event study.

Produces 3 charts:
  1. CAR by window bar chart (US beats vs misses)
  2. Quintile heatmap (Q1-Q5 across windows)
  3. Exchange comparison bar chart (beats T+63 sorted)

Usage:
    python3 pead/generate_charts.py
    python3 pead/generate_charts.py --output pead/results/charts/
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
DEFAULT_OUTPUT = os.path.join(RESULTS_DIR, "charts")

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "US",
    "TSX": "Canada",
    "LSE": "UK",
    "JPX": "Japan",
    "BSE_NSE": "India",
    "XETRA": "Germany",
    "SHZ_SHH": "China",
    "KSC": "Korea",
    "STO": "Sweden",
    "HKSE": "Hong Kong",
    "ASX": "Australia",
    "SAO": "Brazil",
    "SIX": "Switzerland",
    "TAI": "Taiwan",
    "SET": "Thailand",
    "OSL": "Norway",
}

# Colour palette (colour-blind friendly)
COL_BEAT = "#2563eb"   # blue
COL_MISS = "#dc2626"   # red
COL_POS  = "#16a34a"   # green
COL_NEG  = "#dc2626"   # red
COL_NEUTRAL = "#9ca3af"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_us_data():
    path = os.path.join(RESULTS_DIR, "pead_NYSE_NASDAQ_AMEX.json")
    if not os.path.exists(path):
        path = os.path.join(RESULTS_DIR, "pead_US_MAJOR.json")
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


# ---------------------------------------------------------------------------
# Chart 1: CAR by window — beats vs misses (US)
# ---------------------------------------------------------------------------
def chart_car_by_window(us_data, output_dir):
    """Grouped bar chart: beats and misses CAR at each event window."""
    windows = ["T+1", "T+5", "T+21", "T+63"]
    keys    = ["car_1d", "car_5d", "car_21d", "car_63d"]

    beats  = [us_data["car_metrics"]["positive"][k]["mean"] for k in keys]
    misses = [us_data["car_metrics"]["negative"][k]["mean"] for k in keys]

    x = np.arange(len(windows))
    width = 0.35

    fig, ax = plt.subplots(figsize=(9, 5))

    rects1 = ax.bar(x - width / 2, beats,  width, color=COL_BEAT, label="Beats")
    rects2 = ax.bar(x + width / 2, misses, width, color=COL_MISS, label="Misses")

    # Zero line
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    # Value labels
    for rect in rects1:
        v = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, v + 0.03 if v >= 0 else v - 0.1,
                f"{v:+.2f}%", ha="center", va="bottom" if v >= 0 else "top",
                fontsize=8, color=COL_BEAT, fontweight="bold")
    for rect in rects2:
        v = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, v + 0.03 if v >= 0 else v - 0.1,
                f"{v:+.2f}%", ha="center", va="bottom" if v >= 0 else "top",
                fontsize=8, color=COL_MISS, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(windows)
    ax.set_ylabel("Mean CAR vs SPY (%)")
    ax.set_title("Post-Earnings Drift: US Beats vs Misses\n"
                 f"(NYSE+NASDAQ+AMEX, 2000–2025, N={us_data['car_metrics']['overall']['n_events']:,})",
                 fontsize=11, fontweight="bold")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    return save(fig, output_dir, "1_us_car_by_window.png")


# ---------------------------------------------------------------------------
# Chart 2: Quintile heatmap (US, all windows)
# ---------------------------------------------------------------------------
def chart_quintile_heatmap(us_data, output_dir):
    """Heatmap of mean CAR by quintile (rows) and window (columns)."""
    qa = us_data.get("quintile_analysis", {})
    if not qa:
        print("  Skipping quintile heatmap — no quintile_analysis in results")
        return

    quintiles = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    keys_map  = {"T+1": "car_1d", "T+5": "car_5d", "T+21": "car_21d", "T+63": "car_63d"}
    windows   = list(keys_map.keys())

    matrix = []
    for q in quintiles:
        row = []
        for w, k in keys_map.items():
            row.append(qa.get(q, {}).get(k, 0.0))
        matrix.append(row)

    data = np.array(matrix, dtype=float)

    fig, ax = plt.subplots(figsize=(7, 5))

    # Symmetric colour scale
    vmax = max(abs(data.min()), abs(data.max()))
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto",
                   vmin=-vmax, vmax=vmax)

    ax.set_xticks(range(len(windows)))
    ax.set_xticklabels(windows)
    ax.set_yticks(range(len(quintiles)))
    ax.set_yticklabels(["Q1 (worst misses)", "Q2", "Q3", "Q4", "Q5 (biggest beats)"])

    # Annotations
    for i in range(len(quintiles)):
        for j in range(len(windows)):
            v = data[i, j]
            colour = "white" if abs(v) > vmax * 0.6 else "black"
            ax.text(j, i, f"{v:+.2f}%", ha="center", va="center",
                    fontsize=9, color=colour, fontweight="bold")

    plt.colorbar(im, ax=ax, label="Mean CAR (%)")
    ax.set_title("PEAD Quintile Heatmap — US\n"
                 "(NYSE+NASDAQ+AMEX, T+63 CAR by surprise quintile)",
                 fontsize=11, fontweight="bold")

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    return save(fig, output_dir, "2_us_quintile_heatmap.png")


# ---------------------------------------------------------------------------
# Chart 3: Exchange comparison bar chart (beats T+63)
# ---------------------------------------------------------------------------
def chart_exchange_comparison(comparison_data, output_dir):
    """Horizontal bar chart of beats CAR at T+63, sorted by magnitude."""
    rows = []
    for ex_key, d in comparison_data.items():
        if ex_key == "US_MAJOR":
            continue  # de-dup with NYSE_NASDAQ_AMEX
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        val   = d["car_metrics"]["positive"]["car_63d"]["mean"]
        rows.append((label, val))

    # Sort descending
    rows.sort(key=lambda x: x[1], reverse=True)
    labels = [r[0] for r in rows]
    vals   = [r[1] for r in rows]
    colours = [COL_POS if v >= 0 else COL_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(9, 6))
    y = np.arange(len(labels))

    bars = ax.barh(y, vals, color=colours, height=0.6)

    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v in zip(bars, vals):
        x_pos = v + 0.05 if v >= 0 else v - 0.05
        ha    = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{v:+.2f}%", va="center", ha=ha, fontsize=8, fontweight="bold",
                color=COL_POS if v >= 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Beats Mean CAR at T+63 (%)")
    ax.set_title("Post-Earnings Drift — Beats at T+63: 16 Global Exchanges\n"
                 "(Market cap > $500M, 2000–2025, abnormal return vs regional ETF)",
                 fontsize=11, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    pos_patch = mpatches.Patch(color=COL_POS, label="Positive drift")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Negative drift / reversal")
    ax.legend(handles=[pos_patch, neg_patch], loc="lower right", fontsize=8)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    return save(fig, output_dir, "3_global_exchange_comparison.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate PEAD charts")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output directory for chart images")
    args = parser.parse_args()

    ensure_output(args.output)

    print("Loading data...")
    us_data    = load_us_data()
    comparison = load_comparison()

    print("Generating charts...")
    chart_car_by_window(us_data, args.output)
    chart_quintile_heatmap(us_data, args.output)
    chart_exchange_comparison(comparison, args.output)

    print(f"\nDone. Charts saved to: {args.output}/")


if __name__ == "__main__":
    main()
