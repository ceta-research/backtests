#!/usr/bin/env python3
"""
Generate charts for Earnings Surprise (PEAD) event study.

Charts produced:
  1. 1_us_car_by_window.png    - US beats vs misses CAR at T+1/5/21/63 (grouped bar)
  2. 2_us_quintile_drift.png   - T+63 CAR by Q1-Q5 (monotonic drift bar chart)
  3. 3_comparison_car_t1.png   - T+1 CAR across all exchanges (sorted horizontal bar)
  4. 4_comparison_car_t63.png  - T+63 CAR across all exchanges (sorted horizontal bar)

Usage:
    python3 earnings-surprise/generate_charts.py
    python3 earnings-surprise/generate_charts.py --output earnings-surprise/charts/
    python3 earnings-surprise/generate_charts.py --no-comparison  # skip exchange charts
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
    "TSX": "Canada",
    "LSE": "UK",
    "JPX": "Japan",
    "NSE": "India",
    "XETRA": "Germany",
    "SHZ_SHH": "China",
    "KSC": "Korea",
    "STO": "Sweden",
    "HKSE": "Hong Kong",
    "SAO": "Brazil",
    "SIX": "Switzerland",
    "TAI_TWO": "Taiwan",
    "SET": "Thailand",
    "OSL": "Norway",
}

# Colour scheme
COL_BEAT = "#2563eb"    # blue - beats
COL_MISS = "#dc2626"    # red - misses
COL_POS = "#16a34a"     # green - positive
COL_NEG = "#dc2626"     # red - negative

QUINTILE_COLORS = {
    "Q1": "#dc2626",   # dark red - worst misses
    "Q2": "#f97316",   # orange
    "Q3": "#9ca3af",   # gray - near zero
    "Q4": "#60a5fa",   # light blue
    "Q5": "#16a34a",   # green - biggest beats
}

QUINTILE_LABELS = {
    "Q1": "Q1 (largest misses)",
    "Q2": "Q2",
    "Q3": "Q3 (near-zero)",
    "Q4": "Q4",
    "Q5": "Q5 (largest beats)",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_us_data():
    """Load US results from results directory."""
    for fname in ["earnings_surprise_NYSE_NASDAQ_AMEX.json",
                  "earnings_surprise_US_MAJOR.json",
                  "us_test.json"]:
        path = os.path.join(RESULTS_DIR, fname)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    raise FileNotFoundError(
        f"No US results found in {RESULTS_DIR}. "
        "Run: python3 earnings-surprise/backtest.py --preset us "
        "--output earnings-surprise/results/earnings_surprise_NYSE_NASDAQ_AMEX.json"
    )


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


def _get_metric(cm, category, window_key, field="mean"):
    """Safely extract a metric value from car_metrics."""
    cat = cm.get(category, {})
    w = cat.get(window_key)
    if not isinstance(w, dict):
        return 0.0
    return w.get(field, 0.0)


# ---------------------------------------------------------------------------
# Chart 1: US beats vs misses at each window
# ---------------------------------------------------------------------------
def chart_car_by_window(us_data, output_dir):
    """Grouped bar chart: beats and misses CAR at T+1, T+5, T+21, T+63."""
    windows = ["T+1", "T+5", "T+21", "T+63"]
    w_keys = ["car_1d", "car_5d", "car_21d", "car_63d"]

    cm = us_data.get("car_metrics", {})
    n_total = us_data.get("n_total_events", 0)
    n_pos = us_data.get("n_positive", 0)
    n_neg = us_data.get("n_negative", 0)

    beats = [_get_metric(cm, "positive", k) for k in w_keys]
    misses = [_get_metric(cm, "negative", k) for k in w_keys]

    x = np.arange(len(windows))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))

    rects1 = ax.bar(x - width / 2, beats, width, color=COL_BEAT,
                    label=f"Beats (n={n_pos:,})", alpha=0.85)
    rects2 = ax.bar(x + width / 2, misses, width, color=COL_MISS,
                    label=f"Misses (n={n_neg:,})", alpha=0.85)

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for rects, col in [(rects1, COL_BEAT), (rects2, COL_MISS)]:
        for rect in rects:
            v = rect.get_height()
            if abs(v) > 0.02:
                ax.text(rect.get_x() + rect.get_width() / 2,
                        v + 0.04 if v >= 0 else v - 0.12,
                        f"{v:+.2f}%", ha="center",
                        va="bottom" if v >= 0 else "top",
                        fontsize=8, color=col, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(windows, fontsize=10)
    ax.set_ylabel("Mean CAR vs SPY (%)")
    ax.set_title(f"Post-Earnings Drift: US Beats vs Misses\n"
                 f"(NYSE+NASDAQ+AMEX, 2000-2025, N={n_total:,} events, "
                 f"winsorized mean CAR)",
                 fontsize=11, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    return save(fig, output_dir, "1_us_car_by_window.png")


# ---------------------------------------------------------------------------
# Chart 2: Quintile drift at T+63 (Q1-Q5 bar chart)
# ---------------------------------------------------------------------------
def chart_quintile_drift(us_data, output_dir):
    """Bar chart showing CAR at T+63 for each quintile Q1-Q5."""
    cm = us_data.get("car_metrics", {})
    n_total = us_data.get("n_total_events", 0)

    quintiles = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    car_63 = [_get_metric(cm, q, "car_63d") for q in quintiles]
    car_1 = [_get_metric(cm, q, "car_1d") for q in quintiles]
    colors = [QUINTILE_COLORS[q] for q in quintiles]

    x = np.arange(len(quintiles))
    width = 0.35

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))

    # T+1 sub-chart
    bars1 = ax1.bar(x, car_1, width * 2, color=colors, alpha=0.85)
    ax1.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    for bar, v in zip(bars1, car_1):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                 v + 0.03 if v >= 0 else v - 0.08,
                 f"{v:+.3f}%", ha="center",
                 va="bottom" if v >= 0 else "top",
                 fontsize=8.5, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels([QUINTILE_LABELS[q] for q in quintiles], rotation=15, ha="right")
    ax1.set_ylabel("Mean CAR vs SPY (%)")
    ax1.set_title("T+1 CAR by Surprise Quintile", fontsize=11, fontweight="bold")
    ax1.grid(axis="y", alpha=0.3)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # T+63 sub-chart
    bars2 = ax2.bar(x, car_63, width * 2, color=colors, alpha=0.85)
    ax2.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    for bar, v in zip(bars2, car_63):
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 v + 0.03 if v >= 0 else v - 0.08,
                 f"{v:+.3f}%", ha="center",
                 va="bottom" if v >= 0 else "top",
                 fontsize=8.5, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels([QUINTILE_LABELS[q] for q in quintiles], rotation=15, ha="right")
    ax2.set_ylabel("Mean CAR vs SPY (%)")
    ax2.set_title("T+63 CAR by Surprise Quintile", fontsize=11, fontweight="bold")
    ax2.grid(axis="y", alpha=0.3)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    # Q5-Q1 spread annotation
    spread_63 = car_63[-1] - car_63[0] if len(car_63) >= 5 else 0
    spread_1 = car_1[-1] - car_1[0] if len(car_1) >= 5 else 0
    fig.suptitle(
        f"PEAD Quintile Analysis — US (NYSE+NASDAQ+AMEX, 2000-2025, N={n_total:,})\n"
        f"Q5-Q1 spread: T+1 = {spread_1:+.3f}%,  T+63 = {spread_63:+.3f}%",
        fontsize=12, fontweight="bold", y=1.02
    )

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    fig.tight_layout()
    return save(fig, output_dir, "2_us_quintile_drift.png")


# ---------------------------------------------------------------------------
# Chart 3: Exchange comparison at T+1 (beats CAR)
# ---------------------------------------------------------------------------
def chart_comparison_t1(comparison_data, output_dir):
    """Horizontal bar chart of beats CAR at T+1, sorted descending."""
    rows = []
    for ex_key, d in comparison_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        cm = d["car_metrics"]
        val = _get_metric(cm, "positive", "car_1d")
        n = cm.get("positive", {}).get("n_events", 0)
        if n >= 50:
            rows.append((label, val, n))

    if not rows:
        print("  Skipping T+1 comparison - no data")
        return

    rows.sort(key=lambda x: x[1], reverse=True)
    labels = [r[0] for r in rows]
    vals = [r[1] for r in rows]
    ns = [r[2] for r in rows]
    colours = [COL_POS if v >= 0 else COL_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(10, max(6, len(rows) * 0.45 + 2)))
    y = np.arange(len(labels))

    bars = ax.barh(y, vals, color=colours, height=0.6, alpha=0.85)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v, n in zip(bars, vals, ns):
        x_pos = v + 0.02 if v >= 0 else v - 0.02
        ha = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{v:+.3f}%  (n={n:,})", va="center", ha=ha,
                fontsize=7.5, fontweight="bold",
                color=COL_POS if v >= 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Beats Mean CAR at T+1 (%)")
    ax.set_title("PEAD: T+1 Post-Earnings Drift (Beats) by Exchange\n"
                 "(Exchange-specific MCap filters, 2000-2025, abnormal return vs regional ETF)",
                 fontsize=11, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    pos_patch = mpatches.Patch(color=COL_POS, label="Positive drift")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Negative / no drift")
    ax.legend(handles=[pos_patch, neg_patch], loc="lower right", fontsize=8)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    fig.tight_layout()
    return save(fig, output_dir, "3_comparison_car_t1.png")


# ---------------------------------------------------------------------------
# Chart 4: Exchange comparison at T+63 (beats CAR)
# ---------------------------------------------------------------------------
def chart_comparison_t63(comparison_data, output_dir):
    """Horizontal bar chart of beats CAR at T+63, sorted descending."""
    rows = []
    for ex_key, d in comparison_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        cm = d["car_metrics"]
        val = _get_metric(cm, "positive", "car_63d")
        n = cm.get("positive", {}).get("n_events", 0)
        if n >= 50:
            rows.append((label, val, n))

    if not rows:
        print("  Skipping T+63 comparison - no data")
        return

    rows.sort(key=lambda x: x[1], reverse=True)
    labels = [r[0] for r in rows]
    vals = [r[1] for r in rows]
    ns = [r[2] for r in rows]
    colours = [COL_POS if v >= 0 else COL_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(10, max(6, len(rows) * 0.45 + 2)))
    y = np.arange(len(labels))

    bars = ax.barh(y, vals, color=colours, height=0.6, alpha=0.85)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v, n in zip(bars, vals, ns):
        x_pos = v + 0.03 if v >= 0 else v - 0.03
        ha = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{v:+.3f}%  (n={n:,})", va="center", ha=ha,
                fontsize=7.5, fontweight="bold",
                color=COL_POS if v >= 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Beats Mean CAR at T+63 (~3 months post-earnings) (%)")
    ax.set_title("PEAD: T+63 Post-Earnings Drift (Beats) by Exchange\n"
                 "(Exchange-specific MCap filters, 2000-2025, abnormal return vs regional ETF)",
                 fontsize=11, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    pos_patch = mpatches.Patch(color=COL_POS, label="Positive drift")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Negative drift / reversal")
    ax.legend(handles=[pos_patch, neg_patch], loc="lower right", fontsize=8)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    fig.tight_layout()
    return save(fig, output_dir, "4_comparison_car_t63.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate Earnings Surprise charts")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output directory for chart images")
    parser.add_argument("--no-comparison", action="store_true",
                        help="Skip exchange comparison charts (if no exchange_comparison.json)")
    args = parser.parse_args()

    ensure_output(args.output)

    # Load US data
    print("Loading US data...")
    try:
        us_data = load_us_data()
    except FileNotFoundError as e:
        print(f"  ERROR: {e}")
        print("  Run the US backtest first:")
        print("    python3 earnings-surprise/backtest.py --preset us "
              "--output earnings-surprise/results/earnings_surprise_NYSE_NASDAQ_AMEX.json")
        us_data = None

    if us_data:
        print("Generating Chart 1: US beats vs misses by window...")
        chart_car_by_window(us_data, args.output)

        print("Generating Chart 2: US quintile drift (Q1-Q5)...")
        chart_quintile_drift(us_data, args.output)

    if not args.no_comparison:
        comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
        if os.path.exists(comparison_path):
            print("Loading exchange comparison data...")
            comparison = load_comparison()
            print("Generating Chart 3: Exchange comparison T+1...")
            chart_comparison_t1(comparison, args.output)
            print("Generating Chart 4: Exchange comparison T+63...")
            chart_comparison_t63(comparison, args.output)
        else:
            print("  Skipping exchange comparison charts (no exchange_comparison.json)")
            print("  Run global backtest first: python3 earnings-surprise/backtest.py --global")

    print(f"\nDone. Charts saved to: {args.output}/")


if __name__ == "__main__":
    main()
