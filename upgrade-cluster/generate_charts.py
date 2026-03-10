#!/usr/bin/env python3
"""
Generate charts for Analyst Upgrade Clusters event study.

Produces:
  1. CAR by cluster category at each window (grouped bar chart)
  2. CAR progression T+1 → T+63 by category (line chart)
  3. Exchange comparison bar chart (T+1 CAR sorted) [if exchange_comparison.json exists]

Usage:
    python3 upgrade-cluster/generate_charts.py
    python3 upgrade-cluster/generate_charts.py --exchange JPX --label Japan
    python3 upgrade-cluster/generate_charts.py --all-exchanges
    python3 upgrade-cluster/generate_charts.py --output upgrade-cluster/charts/
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
    "BSE_NSE": "India",
    "LSE": "UK",
    "JPX": "Japan",
    "XETRA": "Germany",
    "SHZ_SHH": "China",
    "HKSE": "Hong Kong",
    "KSC": "Korea",
    "TAI_TWO": "Taiwan",
    "TSX": "Canada",
    "ASX": "Australia",
    "SIX": "Switzerland",
    "STO": "Sweden",
    "SAO": "Brazil",
    "JNB": "South Africa",
    "SET": "Thailand",
}

# Category colours
COL_SMALL = "#2563eb"     # blue - upgrade_small
COL_MEDIUM = "#16a34a"    # green - upgrade_medium
COL_LARGE = "#d97706"     # amber - upgrade_large
COL_DOWN = "#dc2626"      # red - downgrade_cluster
COL_ALL = "#374151"       # dark gray - overall
COL_POS = "#16a34a"
COL_NEG = "#dc2626"

CAT_COLORS = {
    "upgrade_small": COL_SMALL,
    "upgrade_medium": COL_MEDIUM,
    "upgrade_large": COL_LARGE,
    "downgrade_cluster": COL_DOWN,
}
CAT_LABELS = {
    "upgrade_small": "Small (delta = 2)",
    "upgrade_medium": "Medium (delta = 3–4)",
    "upgrade_large": "Large (delta ≥ 5)",
    "downgrade_cluster": "Downgrade (bearish delta ≥ 2)",
}

EXCHANGE_COVERAGE = {
    "NYSE_NASDAQ_AMEX": "NYSE+NASDAQ+AMEX, 2019–2025",
    "BSE_NSE": "BSE+NSE, 2019–2025",
    "LSE": "LSE, 2019–2025",
    "JPX": "JPX, 2019–2025",
    "XETRA": "XETRA, 2019–2025",
    "SHZ_SHH": "SHZ+SHH, 2019–2025",
    "HKSE": "HKSE, 2019–2025",
    "KSC": "KSC, 2019–2025",
    "TAI_TWO": "TAI+TWO, 2019–2025",
    "TSX": "TSX, 2019–2025",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_exchange_data(exchange_key="NYSE_NASDAQ_AMEX"):
    path = os.path.join(RESULTS_DIR, f"upgrade_cluster_{exchange_key}.json")
    if not os.path.exists(path) and exchange_key == "NYSE_NASDAQ_AMEX":
        for fname in os.listdir(RESULTS_DIR):
            if "NYSE" in fname or "US" in fname:
                path = os.path.join(RESULTS_DIR, fname)
                break
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
# Chart 1: CAR by cluster category at each window (grouped bar)
# ---------------------------------------------------------------------------
def chart_car_by_category(data, output_dir, exchange_label="US",
                           exchange_key="NYSE_NASDAQ_AMEX"):
    """Grouped bar chart: CAR by cluster category at T+1, T+5, T+21, T+63."""
    windows = ["T+1", "T+5", "T+21", "T+63"]
    w_keys = [1, 5, 21, 63]
    cats = ["upgrade_small", "upgrade_medium", "upgrade_large"]

    metrics = data.get("car_metrics", {})
    overall = metrics.get("overall", {})
    n_total = overall.get("n", 0)
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)

    car_data = {}
    for c in cats:
        c_data = metrics.get(c, {})
        car_data[c] = [c_data.get(f"T+{w}", {}).get("mean_car", 0) for w in w_keys]

    x = np.arange(len(windows))
    width = 0.25
    offsets = [-width, 0, width]

    fig, ax = plt.subplots(figsize=(11, 6))

    for i, c in enumerate(cats):
        bars = ax.bar(x + offsets[i], car_data[c], width,
                      color=CAT_COLORS[c], label=CAT_LABELS[c], alpha=0.85)
        for bar in bars:
            v = bar.get_height()
            if abs(v) > 0.05:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        v + 0.02 if v >= 0 else v - 0.07,
                        f"{v:+.2f}%", ha="center",
                        va="bottom" if v >= 0 else "top",
                        fontsize=7, color=CAT_COLORS[c], fontweight="bold")

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(windows, fontsize=10)
    ax.set_ylabel("Mean CAR vs benchmark (%)")
    ax.set_title(
        f"Analyst Upgrade Clusters: CAR by Cluster Size — {exchange_label}\n"
        f"({coverage}, N={n_total:,} upgrade events, 14–30 day observation gap)",
        fontsize=11, fontweight="bold"
    )
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"1_{prefix}_car_by_category.png")


# ---------------------------------------------------------------------------
# Chart 2: CAR progression T+1 → T+63 by category (line chart)
# ---------------------------------------------------------------------------
def chart_car_progression(data, output_dir, exchange_label="US",
                           exchange_key="NYSE_NASDAQ_AMEX"):
    """Line chart: CAR progression from T+1 to T+63 for each cluster category."""
    windows = [1, 5, 21, 63]
    w_labels = ["T+1", "T+5", "T+21", "T+63"]
    cats = ["upgrade_small", "upgrade_medium", "upgrade_large", "downgrade_cluster"]

    metrics = data.get("car_metrics", {})
    overall = metrics.get("overall", {})
    n_total = overall.get("n", 0)
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)

    fig, ax = plt.subplots(figsize=(9, 5))

    # Overall upgrade clusters line
    overall_cars = [overall.get(f"T+{w}", {}).get("mean_car", 0) for w in windows]
    ax.plot(range(len(windows)), overall_cars, color=COL_ALL, linewidth=2.5,
            linestyle="--", marker="o", label="All upgrades (overall)", zorder=5)

    for c in cats:
        c_data = metrics.get(c, {})
        if not c_data:
            continue
        cars = [c_data.get(f"T+{w}", {}).get("mean_car", 0) for w in windows]
        ax.plot(range(len(windows)), cars, color=CAT_COLORS[c], linewidth=1.8,
                marker="s", label=CAT_LABELS[c], alpha=0.85)

    ax.axhline(0, color="black", linewidth=0.8, linestyle=":", alpha=0.5)
    ax.set_xticks(range(len(windows)))
    ax.set_xticklabels(w_labels)
    ax.set_ylabel("Mean CAR vs benchmark (%)")
    ax.set_title(
        f"Upgrade Cluster CAR Progression — {exchange_label}\n"
        f"({coverage}, N={n_total:,} upgrade events, winsorized mean)",
        fontsize=11, fontweight="bold"
    )
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"2_{prefix}_car_progression.png")


# ---------------------------------------------------------------------------
# Chart 3: Exchange comparison — T+1 CAR
# ---------------------------------------------------------------------------
def chart_exchange_comparison(comparison_data, output_dir):
    """Horizontal bar chart of overall T+1 CAR across exchanges, sorted."""
    rows = []
    for ex_key, d in comparison_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        overall = d["car_metrics"].get("overall", {})
        val = overall.get("T+1", {}).get("mean_car", None)
        n = overall.get("n", 0)
        sig = overall.get("T+1", {}).get("significant_5pct", False)
        if val is not None and n >= 50:
            rows.append((label, val, n, sig))

    if not rows:
        print("  Skipping exchange comparison - no data")
        return

    rows.sort(key=lambda x: x[1], reverse=True)
    labels = [r[0] for r in rows]
    vals = [r[1] for r in rows]
    ns = [r[2] for r in rows]
    sigs = [r[3] for r in rows]
    colours = [COL_POS if v >= 0 else COL_NEG for v in vals]

    fig, ax = plt.subplots(figsize=(10, max(6, len(rows) * 0.45 + 2)))
    y = np.arange(len(labels))

    bars = ax.barh(y, vals, color=colours, height=0.6, alpha=0.85)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v, n, sig in zip(bars, vals, ns, sigs):
        sig_marker = " *" if sig else ""
        x_pos = v + 0.01 if v >= 0 else v - 0.01
        ha = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{v:+.3f}%{sig_marker}  (n={n:,})", va="center", ha=ha,
                fontsize=7.5, fontweight="bold",
                color=COL_POS if v >= 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Overall Mean CAR at T+1 (%)")
    ax.set_title(
        "Analyst Upgrade Clusters: T+1 Abnormal Return by Exchange\n"
        "(2019–2025, 14–30 day gap filter, MCap threshold per exchange. * = p<0.05)",
        fontsize=11, fontweight="bold"
    )
    ax.grid(axis="x", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    pos_patch = mpatches.Patch(color=COL_POS, label="Positive T+1 drift")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Negative / no drift")
    ax.legend(handles=[pos_patch, neg_patch], loc="lower right", fontsize=8)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    fig.tight_layout()
    return save(fig, output_dir, "3_exchange_comparison.png")


# Exchanges to generate per-exchange charts for in --all-exchanges mode
ALL_BLOG_EXCHANGES = [
    ("NYSE_NASDAQ_AMEX", "US"),
    ("BSE_NSE", "India"),
    ("JPX", "Japan"),
    ("LSE", "UK"),
    ("XETRA", "Germany"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate Upgrade Cluster charts")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output directory for chart images")
    parser.add_argument("--exchange", default="NYSE_NASDAQ_AMEX",
                        help="Exchange key (e.g. BSE_NSE, JPX, LSE)")
    parser.add_argument("--label", default=None,
                        help="Display label for the exchange")
    parser.add_argument("--all-exchanges", action="store_true",
                        help="Generate charts for all main blog exchanges")
    parser.add_argument("--no-comparison", action="store_true",
                        help="Skip exchange comparison chart")
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
        chart_car_by_category(ex_data, args.output, ex_label, ex_key)
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
