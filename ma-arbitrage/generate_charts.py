#!/usr/bin/env python3
"""
M&A Return Patterns — Chart Generator

Reads results from ma-arbitrage/results/summary_metrics.json
and generates publication-quality charts for the blog.

Charts generated:
  1_car_by_role.png       — CAR over windows for targets vs acquirers (feature image)
  2_event_frequency.png   — Annual deal count over time
  3_car_overall.png       — Overall CAR with confidence band

Usage:
    cd backtests/
    python3 ma-arbitrage/generate_charts.py
    python3 ma-arbitrage/generate_charts.py --results ma-arbitrage/results/summary_metrics.json
"""

import argparse
import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("Warning: matplotlib not installed. Run: pip install matplotlib")


# --- Style constants ---
STYLE = {
    "target_color": "#2196F3",     # Blue for targets
    "acquirer_color": "#FF6B35",   # Orange for acquirers
    "overall_color": "#4CAF50",    # Green for overall
    "spy_color": "#9E9E9E",        # Gray for benchmark
    "bg": "#FFFFFF",
    "grid": "#F0F0F0",
    "text": "#212121",
    "fig_dpi": 150,
    "fig_size": (10, 6),
}
WINDOWS = [1, 5, 21, 63]
WINDOW_LABELS = ["T+1", "T+5", "T+21", "T+63"]


def load_results(results_path):
    """Load summary_metrics.json."""
    with open(results_path) as f:
        return json.load(f)


def chart_car_by_role(data, out_dir):
    """Chart 1: CAR at each window for targets vs acquirers (feature image)."""
    if not HAS_MPL:
        return

    car = data.get("car_metrics", {})
    target = car.get("target", {})
    acquirer = car.get("acquirer", {})
    overall = car.get("overall", {})

    fig, ax = plt.subplots(figsize=STYLE["fig_size"], dpi=STYLE["fig_dpi"])
    fig.patch.set_facecolor(STYLE["bg"])
    ax.set_facecolor(STYLE["bg"])

    x = list(range(len(WINDOWS)))

    def get_means(section):
        return [section.get(f"car_{w}d", {}).get("mean", 0) for w in WINDOWS]

    def get_significant(section):
        return [section.get(f"car_{w}d", {}).get("significant", False) for w in WINDOWS]

    t_means = get_means(target)
    a_means = get_means(acquirer)
    o_means = get_means(overall)

    ax.plot(x, t_means, "o-", color=STYLE["target_color"], linewidth=2.5,
            markersize=8, label=f"Target (n={target.get('n_events',0):,})", zorder=3)
    ax.plot(x, a_means, "s-", color=STYLE["acquirer_color"], linewidth=2.5,
            markersize=8, label=f"Acquirer (n={acquirer.get('n_events',0):,})", zorder=3)
    ax.plot(x, o_means, "^--", color=STYLE["overall_color"], linewidth=1.5,
            markersize=6, alpha=0.7, label=f"Overall (n={overall.get('n_events',0):,})", zorder=2)

    # Mark significant points with stars
    t_sig = get_significant(target)
    a_sig = get_significant(acquirer)
    for i, (sig, mean) in enumerate(zip(t_sig, t_means)):
        if sig:
            ax.annotate("*", (x[i], mean), xytext=(0, 8), textcoords="offset points",
                        ha="center", fontsize=14, color=STYLE["target_color"])
    for i, (sig, mean) in enumerate(zip(a_sig, a_means)):
        if sig:
            ax.annotate("*", (x[i], mean), xytext=(0, 8), textcoords="offset points",
                        ha="center", fontsize=14, color=STYLE["acquirer_color"])

    ax.axhline(0, color=STYLE["spy_color"], linewidth=1, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(WINDOW_LABELS, fontsize=12)
    ax.set_xlabel("Event Window", fontsize=12, color=STYLE["text"])
    ax.set_ylabel("Mean CAR (%)", fontsize=12, color=STYLE["text"])
    ax.set_title("M&A Announcement: CAR by Role (Targets vs Acquirers)",
                 fontsize=14, fontweight="bold", color=STYLE["text"], pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, color=STYLE["grid"], linewidth=0.8, alpha=0.7)
    ax.tick_params(colors=STYLE["text"])
    for spine in ax.spines.values():
        spine.set_color("#E0E0E0")

    period = data.get("period", "2000-2025")
    ax.text(0.99, 0.02, f"Data: Ceta Research | {period} | * = p<0.05",
            transform=ax.transAxes, ha="right", va="bottom",
            fontsize=8, color="#9E9E9E")

    plt.tight_layout()
    out_path = os.path.join(out_dir, "1_car_by_role.png")
    plt.savefig(out_path, bbox_inches="tight", facecolor=STYLE["bg"])
    plt.close()
    print(f"  Saved: {out_path}")


def chart_event_frequency(data, out_dir):
    """Chart 2: Annual deal count over time."""
    if not HAS_MPL:
        return

    yearly = data.get("yearly_stats", [])
    if not yearly:
        print("  No yearly stats data. Skipping chart 2.")
        return

    years = [y["year"] for y in yearly]
    targets = [y.get("target", 0) for y in yearly]
    acquirers = [y.get("acquirer", 0) for y in yearly]

    fig, ax = plt.subplots(figsize=STYLE["fig_size"], dpi=STYLE["fig_dpi"])
    fig.patch.set_facecolor(STYLE["bg"])
    ax.set_facecolor(STYLE["bg"])

    x = list(range(len(years)))
    width = 0.4

    bars1 = ax.bar([xi - width/2 for xi in x], targets, width=width,
                   color=STYLE["target_color"], alpha=0.85, label="Target events")
    bars2 = ax.bar([xi + width/2 for xi in x], acquirers, width=width,
                   color=STYLE["acquirer_color"], alpha=0.85, label="Acquirer events")

    ax.set_xticks(x)
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=9)
    ax.set_xlabel("Year", fontsize=12, color=STYLE["text"])
    ax.set_ylabel("Events with Return Data", fontsize=12, color=STYLE["text"])
    ax.set_title("M&A Annual Event Count (events with price data)",
                 fontsize=14, fontweight="bold", color=STYLE["text"], pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", color=STYLE["grid"], linewidth=0.8, alpha=0.7)
    ax.tick_params(colors=STYLE["text"])
    for spine in ax.spines.values():
        spine.set_color("#E0E0E0")

    ax.text(0.99, 0.98, "Data: Ceta Research (FMP)",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=8, color="#9E9E9E")

    plt.tight_layout()
    out_path = os.path.join(out_dir, "2_event_frequency.png")
    plt.savefig(out_path, bbox_inches="tight", facecolor=STYLE["bg"])
    plt.close()
    print(f"  Saved: {out_path}")


def chart_overall_car(data, out_dir):
    """Chart 3: Overall CAR bar chart across windows with significance markers."""
    if not HAS_MPL:
        return

    overall = data.get("car_metrics", {}).get("overall", {})
    if not overall:
        print("  No overall metrics. Skipping chart 3.")
        return

    means = [overall.get(f"car_{w}d", {}).get("mean", 0) for w in WINDOWS]
    sigs = [overall.get(f"car_{w}d", {}).get("significant", False) for w in WINDOWS]
    n = overall.get("n_events", 0)

    fig, ax = plt.subplots(figsize=(8, 5), dpi=STYLE["fig_dpi"])
    fig.patch.set_facecolor(STYLE["bg"])
    ax.set_facecolor(STYLE["bg"])

    colors = [STYLE["target_color"] if m >= 0 else "#EF5350" for m in means]
    bars = ax.bar(WINDOW_LABELS, means, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)

    # Add significance stars above bars
    for i, (bar, sig, mean) in enumerate(zip(bars, sigs, means)):
        if sig:
            y_pos = mean + (0.05 if mean >= 0 else -0.1)
            ax.text(bar.get_x() + bar.get_width() / 2, y_pos, "*",
                    ha="center", va="bottom", fontsize=16, color="#333333")

    ax.axhline(0, color=STYLE["spy_color"], linewidth=1, linestyle="-", alpha=0.6)
    ax.set_xlabel("Event Window", fontsize=12, color=STYLE["text"])
    ax.set_ylabel("Mean CAR (%)", fontsize=12, color=STYLE["text"])
    ax.set_title(f"M&A Announcements: Overall Mean CAR (n={n:,})",
                 fontsize=14, fontweight="bold", color=STYLE["text"], pad=15)
    ax.grid(True, axis="y", color=STYLE["grid"], linewidth=0.8, alpha=0.7)
    ax.tick_params(colors=STYLE["text"])
    for spine in ax.spines.values():
        spine.set_color("#E0E0E0")

    period = data.get("period", "2000-2025")
    ax.text(0.99, 0.02, f"Data: Ceta Research | {period} | * = p<0.05",
            transform=ax.transAxes, ha="right", va="bottom",
            fontsize=8, color="#9E9E9E")

    plt.tight_layout()
    out_path = os.path.join(out_dir, "3_car_overall.png")
    plt.savefig(out_path, bbox_inches="tight", facecolor=STYLE["bg"])
    plt.close()
    print(f"  Saved: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="M&A Return Patterns Chart Generator")
    parser.add_argument("--results",
                        default=os.path.join(os.path.dirname(__file__), "results", "summary_metrics.json"),
                        help="Path to summary_metrics.json")
    parser.add_argument("--out-dir",
                        default=os.path.join(os.path.dirname(__file__), "charts"),
                        help="Output directory for charts")
    args = parser.parse_args()

    if not os.path.exists(args.results):
        print(f"Error: Results file not found: {args.results}")
        print("Run backtest.py first to generate results.")
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"Loading results from {args.results}")
    data = load_results(args.results)

    print("\nGenerating charts...")
    chart_car_by_role(data, args.out_dir)
    chart_event_frequency(data, args.out_dir)
    chart_overall_car(data, args.out_dir)

    print(f"\nDone. Charts saved to {args.out_dir}/")


if __name__ == "__main__":
    main()
