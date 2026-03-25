#!/usr/bin/env python3
"""
Pre-Earnings Runup: Chart Generation

Generates charts from backtest results:
  - Per-exchange: CAR bar chart by category (habitual_beater, mixed, habitual_misser)
  - Per-exchange: CAR by window (T-10, T-5, T-1, T+1)
  - Comparison: T-10 CAR across exchanges (habitual beater)
  - Comparison: Habitual beater vs overall heatmap

Usage:
    cd backtests/
    python3 pre-earnings/generate_charts.py

    # Specific exchange result file
    python3 pre-earnings/generate_charts.py --results results/pre_earnings_NYSE_NASDAQ_AMEX.json

    # All exchanges from comparison file
    python3 pre-earnings/generate_charts.py --comparison results/exchange_comparison.json
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
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("WARNING: matplotlib not installed. Run: pip install matplotlib")

CHART_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")

# Colors
COLORS = {
    "habitual_beater": "#2ecc71",   # Green - habitual beaters
    "mixed": "#3498db",             # Blue - mixed
    "habitual_misser": "#e74c3c",   # Red - habitual missers
    "overall": "#95a5a6",           # Gray - overall
    "benchmark": "#f39c12",         # Orange - benchmark reference
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "US",
    "TSX": "Canada",
    "LSE": "UK",
    "JPX": "Japan",
    "NSE": "India",
    "XETRA": "Germany",
    "TAI_TWO": "Taiwan",
    "KSC": "Korea",
    "STO": "Sweden",
    "SET": "Thailand",
    "OSL": "Norway",
    "SAO": "Brazil",
    "HKSE": "Hong Kong",
    "SIX": "Switzerland",
}


def get_car(metrics, category, window_key):
    """Safely extract CAR mean from nested metrics dict."""
    cat = metrics.get(category, {})
    w = cat.get(window_key, {})
    if isinstance(w, dict):
        return w.get("mean", 0)
    return 0


def get_sig(metrics, category, window_key):
    """Check if CAR is statistically significant."""
    cat = metrics.get(category, {})
    w = cat.get(window_key, {})
    if isinstance(w, dict):
        return w.get("significant", False)
    return False


def chart_category_comparison(data, exchange_name, output_dir):
    """Bar chart: T-10 CAR by category (habitual_beater, mixed, habitual_misser)."""
    if not HAS_MATPLOTLIB:
        return

    metrics = data.get("car_metrics", {})
    categories = ["habitual_beater", "mixed", "habitual_misser"]
    cat_labels = ["Habitual Beater\n(>75%)", "Mixed\n(25-75%)", "Habitual Misser\n(<25%)"]

    windows = ["car_pre_10d", "car_pre_5d", "car_pre_1d"]
    window_labels = ["T-10", "T-5", "T-1"]

    n_cats = len(categories)
    n_wins = len(windows)
    x = np.arange(n_cats)
    bar_width = 0.25
    offsets = [(i - (n_wins - 1) / 2) * bar_width for i in range(n_wins)]

    fig, ax = plt.subplots(figsize=(10, 6))

    win_colors = ["#2c3e50", "#7f8c8d", "#bdc3c7"]
    for wi, (wkey, wlabel, offset) in enumerate(zip(windows, window_labels, offsets)):
        values = [get_car(metrics, cat, wkey) for cat in categories]
        bars = ax.bar(x + offset, values, bar_width,
                      label=wlabel, color=win_colors[wi], alpha=0.85,
                      edgecolor="white", linewidth=0.5)

        # Add significance markers for T-10 bars only
        if wkey == "car_pre_10d":
            for bi, (cat, bar) in enumerate(zip(categories, bars)):
                if get_sig(metrics, cat, wkey):
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height() + 0.02,
                            "*", ha="center", va="bottom", fontsize=12, color="black")

    ax.axhline(y=0, color="black", linewidth=0.8, linestyle="-")
    ax.set_xlabel("Beat Rate Category", fontsize=12)
    ax.set_ylabel("Cumulative Abnormal Return (%)", fontsize=12)
    label = EXCHANGE_LABELS.get(exchange_name, exchange_name)
    ax.set_title(f"Pre-Earnings CAR by Beat Rate Category: {label}", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(cat_labels, fontsize=10)
    ax.legend(title="Window", fontsize=10)
    ax.grid(axis="y", alpha=0.3)

    # Event count annotations
    for i, cat in enumerate(categories):
        n = metrics.get(cat, {}).get("n_events", 0)
        ax.text(x[i], ax.get_ylim()[0] * 0.95, f"n={n:,}", ha="center", fontsize=8, color="gray")

    plt.tight_layout()
    fname = f"1_{exchange_name.lower()}_category_car.png"
    path = os.path.join(output_dir, fname)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {fname}")
    return fname


def chart_window_progression(data, exchange_name, output_dir):
    """Line chart: CAR by window (T-10, T-5, T-1, T+1) for each category."""
    if not HAS_MATPLOTLIB:
        return

    metrics = data.get("car_metrics", {})
    categories = ["habitual_beater", "overall", "habitual_misser"]
    cat_colors = [COLORS["habitual_beater"], COLORS["overall"], COLORS["habitual_misser"]]
    cat_labels = ["Habitual Beater (>75%)", "Overall", "Habitual Misser (<25%)"]

    # Windows in order: T-10, T-5, T-1, T+1
    windows = [
        ("car_pre_10d", "T-10"),
        ("car_pre_5d", "T-5"),
        ("car_pre_1d", "T-1"),
        ("car_post_1d", "T+1"),
    ]
    x_vals = [-10, -5, -1, 1]

    fig, ax = plt.subplots(figsize=(10, 6))

    for cat, color, label in zip(categories, cat_colors, cat_labels):
        values = [get_car(metrics, cat, wkey) for wkey, _ in windows]
        n = metrics.get(cat, {}).get("n_events", 0)
        if n == 0:
            continue
        ax.plot(x_vals, values, "o-", color=color, linewidth=2,
                markersize=7, label=f"{label} (n={n:,})")

    ax.axhline(y=0, color="black", linewidth=0.8, linestyle="-")
    ax.axvline(x=0, color="gray", linewidth=1.2, linestyle="--", alpha=0.5,
               label="Announcement (T=0)")

    # Shade pre-event region
    ax.axvspan(-10.5, 0, alpha=0.04, color="green", label="Pre-event window")

    ax.set_xlabel("Trading Days Relative to Announcement (T=0)", fontsize=12)
    ax.set_ylabel("Cumulative Abnormal Return (%)", fontsize=12)
    label = EXCHANGE_LABELS.get(exchange_name, exchange_name)
    ax.set_title(f"Pre-Earnings Runup by Category: {label}", fontsize=13, fontweight="bold")
    ax.set_xticks([-10, -5, -1, 0, 1])
    ax.set_xticklabels(["T-10", "T-5", "T-1", "T=0", "T+1"])
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    plt.tight_layout()
    fname = f"2_{exchange_name.lower()}_window_progression.png"
    path = os.path.join(output_dir, fname)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {fname}")
    return fname


def chart_global_comparison(all_data, output_dir):
    """Horizontal bar chart: T-10 CAR for habitual beaters across exchanges."""
    if not HAS_MATPLOTLIB:
        return

    rows = []
    for uni, d in all_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        metrics = d["car_metrics"]
        overall = metrics.get("overall", {})
        beater = metrics.get("habitual_beater", {})
        n_total = overall.get("n_events", 0)
        n_beater = beater.get("n_events", 0)
        if n_total == 0:
            continue

        all_t10 = get_car(metrics, "overall", "car_pre_10d")
        beat_t10 = get_car(metrics, "habitual_beater", "car_pre_10d")
        t_stat = (metrics.get("overall", {}).get("car_pre_10d", {}) or {}).get("t_stat", 0)

        label = EXCHANGE_LABELS.get(uni, uni)
        rows.append({
            "label": label,
            "all_t10": all_t10,
            "beat_t10": beat_t10,
            "t_stat": t_stat,
            "n_total": n_total,
            "n_beater": n_beater,
        })

    if not rows:
        return

    # Sort by habitual beater T-10 CAR
    rows.sort(key=lambda r: r["beat_t10"], reverse=True)

    labels = [r["label"] for r in rows]
    all_vals = [r["all_t10"] for r in rows]
    beat_vals = [r["beat_t10"] for r in rows]

    n = len(rows)
    y = np.arange(n)
    bar_h = 0.35

    fig, ax = plt.subplots(figsize=(11, max(5, n * 0.5 + 1)))

    bars_all = ax.barh(y + bar_h / 2, all_vals, bar_h,
                       label="Overall", color=COLORS["overall"], alpha=0.85, edgecolor="white")
    bars_beat = ax.barh(y - bar_h / 2, beat_vals, bar_h,
                        label="Habitual Beater (>75%)", color=COLORS["habitual_beater"],
                        alpha=0.85, edgecolor="white")

    ax.axvline(x=0, color="black", linewidth=0.8)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Cumulative Abnormal Return at T-10 (%)", fontsize=11)
    ax.set_title("Pre-Earnings Runup (T-10 CAR): Habitual Beaters vs Overall",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(axis="x", alpha=0.3)

    # Add significance markers
    for i, row in enumerate(rows):
        if abs(row["t_stat"]) > 1.96:
            x_pos = max(row["all_t10"], 0) + 0.02
            ax.text(x_pos, y[i] + bar_h / 2, "*", ha="left", va="center",
                    fontsize=10, color="black")

    plt.tight_layout()
    fname = "1_comparison_pre_earnings_t10.png"
    path = os.path.join(output_dir, fname)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {fname}")
    return fname


def chart_comparison_event_counts(all_data, output_dir):
    """Bar chart: event counts and habitual beater % by exchange."""
    if not HAS_MATPLOTLIB:
        return

    rows = []
    for uni, d in all_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        metrics = d["car_metrics"]
        n_total = metrics.get("overall", {}).get("n_events", 0)
        n_beater = metrics.get("habitual_beater", {}).get("n_events", 0)
        n_misser = metrics.get("habitual_misser", {}).get("n_events", 0)
        n_mixed = metrics.get("mixed", {}).get("n_events", 0)
        if n_total == 0:
            continue
        label = EXCHANGE_LABELS.get(uni, uni)
        rows.append({
            "label": label,
            "n_total": n_total,
            "n_beater": n_beater,
            "n_misser": n_misser,
            "n_mixed": n_mixed,
            "beater_pct": round(n_beater / n_total * 100, 1) if n_total > 0 else 0,
        })

    if not rows:
        return

    rows.sort(key=lambda r: r["n_total"], reverse=True)
    labels = [r["label"] for r in rows]
    beater_pcts = [r["beater_pct"] for r in rows]
    totals = [r["n_total"] for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, max(4, len(rows) * 0.4 + 1)))

    y = np.arange(len(rows))

    # Left: event counts stacked
    ax1.barh(y, [r["n_beater"] for r in rows], label="Habitual Beater",
             color=COLORS["habitual_beater"], alpha=0.85)
    ax1.barh(y, [r["n_mixed"] for r in rows],
             left=[r["n_beater"] for r in rows],
             label="Mixed", color=COLORS["mixed"], alpha=0.85)
    ax1.barh(y, [r["n_misser"] for r in rows],
             left=[r["n_beater"] + r["n_mixed"] for r in rows],
             label="Habitual Misser", color=COLORS["habitual_misser"], alpha=0.85)
    ax1.set_yticks(y)
    ax1.set_yticklabels(labels, fontsize=9)
    ax1.set_xlabel("Total Events", fontsize=10)
    ax1.set_title("Event Counts by Category", fontsize=11, fontweight="bold")
    ax1.legend(fontsize=8)
    ax1.grid(axis="x", alpha=0.3)

    # Right: habitual beater %
    ax2.barh(y, beater_pcts, color=COLORS["habitual_beater"], alpha=0.85)
    ax2.axvline(x=sum(beater_pcts) / len(beater_pcts), color="gray", linestyle="--",
                linewidth=1, label="Average")
    ax2.set_yticks(y)
    ax2.set_yticklabels(labels, fontsize=9)
    ax2.set_xlabel("Habitual Beater Events (%)", fontsize=10)
    ax2.set_title("Habitual Beater Share by Exchange", fontsize=11, fontweight="bold")
    ax2.grid(axis="x", alpha=0.3)

    plt.suptitle("Pre-Earnings Runup: Event Distribution", fontsize=12, fontweight="bold")
    plt.tight_layout()
    fname = "2_comparison_event_distribution.png"
    path = os.path.join(output_dir, fname)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {fname}")
    return fname


def main():
    parser = argparse.ArgumentParser(description="Generate pre-earnings runup charts")
    parser.add_argument("--results", type=str,
                        help="Single exchange results JSON (from backtest.py --output)")
    parser.add_argument("--comparison", type=str,
                        help="Global comparison JSON (from --global --output)")
    parser.add_argument("--output-dir", type=str, default=CHART_DIR,
                        help=f"Chart output directory (default: {CHART_DIR})")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    if args.comparison:
        # Generate comparison charts
        print(f"\nLoading comparison results from {args.comparison}...")
        with open(args.comparison) as f:
            all_data = json.load(f)
        print(f"  {len(all_data)} exchanges found")

        comp_dir = os.path.join(args.output_dir, "comparison")
        os.makedirs(comp_dir, exist_ok=True)

        print("\nGenerating comparison charts...")
        chart_global_comparison(all_data, comp_dir)
        chart_comparison_event_counts(all_data, comp_dir)

        # Also generate per-exchange charts
        for uni, d in all_data.items():
            if "error" in d or not d.get("car_metrics"):
                continue
            label = EXCHANGE_LABELS.get(uni, uni).lower().replace(" ", "_")
            ex_dir = os.path.join(args.output_dir, label)
            os.makedirs(ex_dir, exist_ok=True)
            print(f"\nCharts for {uni}...")
            chart_category_comparison(d, uni, ex_dir)
            chart_window_progression(d, uni, ex_dir)

    elif args.results:
        # Single exchange charts
        print(f"\nLoading results from {args.results}...")
        with open(args.results) as f:
            data = json.load(f)
        uni = data.get("universe", "unknown")
        label = EXCHANGE_LABELS.get(uni, uni).lower().replace(" ", "_")
        ex_dir = os.path.join(args.output_dir, label)
        os.makedirs(ex_dir, exist_ok=True)
        print(f"\nGenerating charts for {uni}...")
        chart_category_comparison(data, uni, ex_dir)
        chart_window_progression(data, uni, ex_dir)

    else:
        # Auto-detect: look for exchange_comparison.json or per-exchange files
        results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        comp_file = os.path.join(results_dir, "exchange_comparison.json")
        if os.path.exists(comp_file):
            args.comparison = comp_file
            main()
            return
        else:
            print("No results found. Run backtest.py first:")
            print("  python3 pre-earnings/backtest.py --global --output results/exchange_comparison.json")
            sys.exit(1)

    print(f"\nAll charts saved to {args.output_dir}")


if __name__ == "__main__":
    main()
