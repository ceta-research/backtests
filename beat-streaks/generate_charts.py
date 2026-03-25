#!/usr/bin/env python3
"""
Generate charts for Beat Streaks event study.

Produces charts per exchange:
  1. CAR by streak length (all windows) - grouped bar chart
  2. CAR progression T+1 → T+63 by streak category - line chart
  3. Exchange comparison bar chart (overall T+21 CAR, sorted) [comparison only]

Usage:
    python3 beat-streaks/generate_charts.py
    python3 beat-streaks/generate_charts.py --exchange TSX --label Canada
    python3 beat-streaks/generate_charts.py --all-exchanges
    python3 beat-streaks/generate_charts.py --output beat-streaks/charts/
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
    "ASX": "Australia",
    "SAO": "Brazil",
    "SIX": "Switzerland",
    "TAI_TWO": "Taiwan",
    "SET": "Thailand",
    "OSL": "Norway",
}

# Colour scheme
COL_S2 = "#2563eb"   # blue - streak_2
COL_S3 = "#16a34a"   # green - streak_3
COL_S4 = "#d97706"   # amber - streak_4
COL_S5 = "#9333ea"   # purple - streak_5plus
COL_ALL = "#374151"  # dark gray - overall
COL_POS = "#16a34a"
COL_NEG = "#dc2626"

STREAK_COLORS = {
    "streak_2": COL_S2,
    "streak_3": COL_S3,
    "streak_4": COL_S4,
    "streak_5plus": COL_S5,
}
STREAK_LABELS = {
    "streak_2": "Streak 2 (2nd beat)",
    "streak_3": "Streak 3 (3rd beat)",
    "streak_4": "Streak 4 (4th beat)",
    "streak_5plus": "Streak 5+ (5th+ beat)",
}


# Coverage period labels per exchange (for chart subtitles)
EXCHANGE_COVERAGE = {
    "NYSE_NASDAQ_AMEX": "NYSE+NASDAQ+AMEX, 2000–2025",
    "TSX": "TSX, 2000–2025",
    "JPX": "JPX, 2009–2025",
    "TAI_TWO": "TAI+TWO, 2012–2025",
    "NSE": "NSE, 2020–2025",
    "SAO": "SAO, 2015–2025",
    "LSE": "LSE, 2020–2025",
    "XETRA": "XETRA, 2020–2025",
    "SHZ_SHH": "SHZ+SHH, 2015–2025",
    "KSC": "KSC, 2016–2025",
    "HKSE": "HKSE, 2020–2025",
    "SET": "SET, 2017–2025",
    "STO": "STO, 2015–2025",
    "ASX": "ASX, 2020–2025",
    "OSL": "OSL, 2021–2025",
    "SIX": "SIX, 2021–2025",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_exchange_data(exchange_key="NYSE_NASDAQ_AMEX"):
    path = os.path.join(RESULTS_DIR, f"beat_streaks_{exchange_key}.json")
    if not os.path.exists(path) and exchange_key == "NYSE_NASDAQ_AMEX":
        # Fallback to any US result file
        for fname in os.listdir(RESULTS_DIR):
            if "NYSE" in fname or "US" in fname:
                path = os.path.join(RESULTS_DIR, fname)
                break
    with open(path) as f:
        return json.load(f)


def load_us_data():
    return load_exchange_data("NYSE_NASDAQ_AMEX")


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
# Chart 1: CAR by streak length at each window
# ---------------------------------------------------------------------------
def chart_car_by_streak(data, output_dir, exchange_label="US",
                        exchange_key="NYSE_NASDAQ_AMEX"):
    """Grouped bar chart: CAR by streak category at each event window."""
    windows = ["T+1", "T+5", "T+21", "T+63"]
    w_keys = [1, 5, 21, 63]
    streaks = ["streak_2", "streak_3", "streak_4", "streak_5plus"]

    metrics = data.get("car_metrics", {})
    n_total = metrics.get("overall", {}).get("n", 0)
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)

    # Get CAR for each streak at each window
    car_data = {}
    for s in streaks:
        s_data = metrics.get(s, {})
        car_data[s] = [s_data.get(f"T+{w}", {}).get("mean_car", 0) for w in w_keys]

    x = np.arange(len(windows))
    width = 0.2
    offsets = [-1.5 * width, -0.5 * width, 0.5 * width, 1.5 * width]

    fig, ax = plt.subplots(figsize=(11, 6))

    for i, s in enumerate(streaks):
        bars = ax.bar(x + offsets[i], car_data[s], width,
                      color=STREAK_COLORS[s], label=STREAK_LABELS[s], alpha=0.85)
        # Value labels
        for bar in bars:
            v = bar.get_height()
            if abs(v) > 0.05:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        v + 0.03 if v >= 0 else v - 0.08,
                        f"{v:+.2f}%", ha="center",
                        va="bottom" if v >= 0 else "top",
                        fontsize=7, color=STREAK_COLORS[s], fontweight="bold")

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(windows, fontsize=10)
    ax.set_ylabel("Mean CAR vs benchmark (%)")
    ax.set_title(f"Beat Streaks: CAR by Streak Length — {exchange_label}\n"
                 f"({coverage}, N={n_total:,} streak events)",
                 fontsize=11, fontweight="bold")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"1_{prefix}_car_by_streak.png")


# ---------------------------------------------------------------------------
# Chart 2: CAR progression by window (T+1 → T+63)
# ---------------------------------------------------------------------------
def chart_car_progression(data, output_dir, exchange_label="US",
                          exchange_key="NYSE_NASDAQ_AMEX"):
    """Line chart: CAR progression across windows for each streak length."""
    windows = [1, 5, 21, 63]
    w_labels = ["T+1", "T+5", "T+21", "T+63"]
    streaks = ["streak_2", "streak_3", "streak_4", "streak_5plus"]

    metrics = data.get("car_metrics", {})
    overall = metrics.get("overall", {})
    n_total = overall.get("n", 0)
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)

    fig, ax = plt.subplots(figsize=(9, 5))

    # Overall line
    overall_cars = [overall.get(f"T+{w}", {}).get("mean_car", 0) for w in windows]
    ax.plot(range(len(windows)), overall_cars, color=COL_ALL, linewidth=2.5,
            linestyle="--", marker="o", label="All streaks (overall)", zorder=5)

    for s in streaks:
        s_data = metrics.get(s, {})
        cars = [s_data.get(f"T+{w}", {}).get("mean_car", 0) for w in windows]
        ax.plot(range(len(windows)), cars, color=STREAK_COLORS[s], linewidth=1.8,
                marker="s", label=STREAK_LABELS[s], alpha=0.85)

    ax.axhline(0, color="black", linewidth=0.8, linestyle=":", alpha=0.5)
    ax.set_xticks(range(len(windows)))
    ax.set_xticklabels(w_labels)
    ax.set_ylabel("Mean CAR vs benchmark (%)")
    ax.set_title(f"Beat Streak CAR Progression: {exchange_label}\n"
                 f"({coverage}, N={n_total:,} total streak events, winsorized mean)",
                 fontsize=11, fontweight="bold")
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"2_{prefix}_car_progression.png")


# ---------------------------------------------------------------------------
# Chart 3: Exchange comparison - overall T+21 CAR
# ---------------------------------------------------------------------------
def chart_exchange_comparison(comparison_data, output_dir):
    """Horizontal bar chart of overall CAR at T+21, sorted by magnitude."""
    rows = []
    for ex_key, d in comparison_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        overall = d["car_metrics"].get("overall", {})
        val = overall.get("T+21", {}).get("mean_car", None)
        n = overall.get("n", 0)
        if val is not None and n >= 50:
            rows.append((label, val, n))

    if not rows:
        print("  Skipping exchange comparison - no data")
        return

    rows.sort(key=lambda x: x[1], reverse=True)
    labels = [r[0] for r in rows]
    vals = [r[1] for r in rows]
    ns = [r[2] for r in rows]
    colours = [COL_POS if v >= 0 else COL_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(10, max(6, len(rows) * 0.4 + 2)))
    y = np.arange(len(labels))

    bars = ax.barh(y, vals, color=colours, height=0.6)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v, n in zip(bars, vals, ns):
        x_pos = v + 0.02 if v >= 0 else v - 0.02
        ha = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{v:+.2f}%  (n={n:,})", va="center", ha=ha,
                fontsize=7.5, fontweight="bold",
                color=COL_POS if v >= 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Overall Mean CAR at T+21 (%)")
    ax.set_title("Beat Streaks (streak ≥ 2): T+21 CAR by Exchange\n"
                 "(All streak categories combined, abnormal return vs regional ETF)",
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
    return save(fig, output_dir, "3_exchange_comparison.png")


# Exchanges to generate charts for in --all-exchanges mode
ALL_BLOG_EXCHANGES = [
    ("NYSE_NASDAQ_AMEX", "US"),
    ("TSX", "Canada"),
    ("JPX", "Japan"),
    ("TAI_TWO", "Taiwan"),
    ("NSE", "India"),
    ("SAO", "Brazil"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate Beat Streaks charts")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output directory for chart images")
    parser.add_argument("--exchange", default="NYSE_NASDAQ_AMEX",
                        help="Exchange key (e.g. TSX, JPX, NSE)")
    parser.add_argument("--label", default=None,
                        help="Display label for the exchange (e.g. 'Canada'). "
                             "Defaults to EXCHANGE_LABELS lookup.")
    parser.add_argument("--all-exchanges", action="store_true",
                        help="Generate charts for all 6 blog exchanges")
    parser.add_argument("--no-comparison", action="store_true",
                        help="Skip exchange comparison chart (if no comparison data)")
    args = parser.parse_args()

    ensure_output(args.output)

    exchanges_to_run = ALL_BLOG_EXCHANGES if args.all_exchanges else [
        (args.exchange, args.label or EXCHANGE_LABELS.get(args.exchange, args.exchange))
    ]

    for ex_key, ex_label in exchanges_to_run:
        print(f"Loading data for {ex_label} ({ex_key})...")
        try:
            ex_data = load_exchange_data(ex_key)
        except FileNotFoundError as e:
            print(f"  Error: {e}")
            print(f"  Run backtest.py --exchange {ex_key} first")
            continue

        print(f"Generating charts for {ex_label}...")
        chart_car_by_streak(ex_data, args.output, ex_label, ex_key)
        chart_car_progression(ex_data, args.output, ex_label, ex_key)

    if not args.no_comparison:
        comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
        if os.path.exists(comparison_path):
            print("Generating exchange comparison chart...")
            comparison = load_comparison()
            chart_exchange_comparison(comparison, args.output)
        else:
            print("  Skipping exchange comparison (no exchange_comparison.json found)")
            print("  Run backtest.py --global first")

    print(f"\nDone. Charts saved to: {args.output}/")


if __name__ == "__main__":
    main()
