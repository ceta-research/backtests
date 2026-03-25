#!/usr/bin/env python3
"""
Generate charts for Post-Earnings Dip event study.

Produces per-exchange:
  1. CAR by dip category (all windows) - grouped bar chart  [feature image]
  2. CAR progression T+5 → T+63 by dip category - line chart

Plus comparison:
  3. Exchange comparison bar chart (overall T+21 CAR, sorted)

Usage:
    python3 post-earnings-dip/generate_charts.py
    python3 post-earnings-dip/generate_charts.py --exchange TAI_TWO --label Taiwan
    python3 post-earnings-dip/generate_charts.py --all-exchanges
    python3 post-earnings-dip/generate_charts.py --output post-earnings-dip/charts/
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
    "TAI_TWO": "Taiwan",
}

EXCHANGE_COVERAGE = {
    "NYSE_NASDAQ_AMEX": "NYSE+NASDAQ+AMEX, 2000–2025",
    "TSX": "TSX, 2000–2025",
    "LSE": "LSE, 2022–2025",
    "JPX": "JPX, 2014–2025",
    "NSE": "NSE, 2022–2025",
    "XETRA": "XETRA, 2015–2025",
    "SHZ_SHH": "SHZ+SHH, 2014–2025",
    "KSC": "KSC, 2014–2025",
    "STO": "STO, 2014–2025",
    "HKSE": "HKSE, 2014–2025",
    "SAO": "SAO, 2014–2025",
    "TAI_TWO": "TAI+TWO, 2014–2025",
}

# Colour scheme
COL_OVERALL = "#374151"  # dark gray
COL_DIP5    = "#2563eb"  # blue   - 5-10% dip
COL_DIP10   = "#d97706"  # amber  - 10-20% dip
COL_DIP20   = "#9333ea"  # purple - 20%+ dip
COL_POS     = "#16a34a"
COL_NEG     = "#dc2626"

DIP_COLORS = {
    "overall": COL_OVERALL,
    "dip_5":   COL_DIP5,
    "dip_10":  COL_DIP10,
    "dip_20":  COL_DIP20,
}
DIP_LABELS = {
    "overall": "All dips (5%+)",
    "dip_5":   "Moderate dip (5–10%)",
    "dip_10":  "Sharp dip (10–20%)",
    "dip_20":  "Severe dip (20%+)",
}

MIN_N_FOR_CHART = 30   # skip a dip category if too few events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_exchange_data(exchange_key="NYSE_NASDAQ_AMEX"):
    path = os.path.join(RESULTS_DIR, f"post_dip_{exchange_key}.json")
    if not os.path.exists(path) and exchange_key == "NYSE_NASDAQ_AMEX":
        for fname in os.listdir(RESULTS_DIR):
            if "NYSE" in fname or "US_MAJOR" in fname:
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


def get_active_dip_categories(metrics):
    """Return dip categories that have enough events to chart."""
    cats = []
    for cat in ["overall", "dip_5", "dip_10", "dip_20"]:
        data = metrics.get(cat, {})
        n = data.get("n", 0) if cat == "overall" else data.get("n", 0)
        if n >= MIN_N_FOR_CHART:
            cats.append(cat)
    return cats


# ---------------------------------------------------------------------------
# Chart 1: CAR by dip category at each window (grouped bar - FEATURE IMAGE)
# ---------------------------------------------------------------------------
def chart_car_by_dip(data, output_dir, exchange_label="US",
                     exchange_key="NYSE_NASDAQ_AMEX"):
    """Grouped bar chart: CAR by dip size at each event window."""
    windows = ["T+5", "T+10", "T+21", "T+63"]
    w_keys = [5, 10, 21, 63]

    metrics = data.get("car_metrics", {})
    n_total = metrics.get("overall", {}).get("n", 0)
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)
    active = get_active_dip_categories(metrics)

    if not active:
        print(f"  Skipping {exchange_label}: no dip categories with n >= {MIN_N_FOR_CHART}")
        return

    # Build CAR arrays for each active category
    car_data = {}
    for cat in active:
        cat_data = metrics.get(cat, {})
        car_data[cat] = [cat_data.get(f"T+{w}", {}).get("mean_car", 0) for w in w_keys]

    n_cats = len(active)
    x = np.arange(len(windows))
    width = 0.7 / n_cats
    offsets = np.linspace(-(n_cats - 1) / 2 * width, (n_cats - 1) / 2 * width, n_cats)

    fig, ax = plt.subplots(figsize=(11, 6))

    for i, cat in enumerate(active):
        color = DIP_COLORS[cat]
        bars = ax.bar(x + offsets[i], car_data[cat], width,
                      color=color, label=DIP_LABELS[cat], alpha=0.85)
        # Value labels on bars with significant height
        for bar in bars:
            v = bar.get_height()
            if abs(v) > 0.1:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        v + 0.04 if v >= 0 else v - 0.1,
                        f"{v:+.2f}%", ha="center",
                        va="bottom" if v >= 0 else "top",
                        fontsize=7, color=color, fontweight="bold")

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(windows, fontsize=10)
    ax.set_ylabel("Mean CAR vs benchmark (%)")
    ax.set_title(
        f"Post-Earnings Dip: CAR by Dip Size — {exchange_label}\n"
        f"({coverage}, N={n_total:,} dip events, measured from T+1 close)",
        fontsize=11, fontweight="bold"
    )
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    prefix = exchange_label.lower().replace(" ", "_")
    return save(fig, output_dir, f"1_{prefix}_car_by_dip.png")


# ---------------------------------------------------------------------------
# Chart 2: CAR progression T+5 → T+63 by dip category (line chart)
# ---------------------------------------------------------------------------
def chart_car_progression(data, output_dir, exchange_label="US",
                          exchange_key="NYSE_NASDAQ_AMEX"):
    """Line chart: CAR progression across windows by dip size."""
    windows = [5, 10, 21, 63]
    w_labels = ["T+5", "T+10", "T+21", "T+63"]

    metrics = data.get("car_metrics", {})
    n_total = metrics.get("overall", {}).get("n", 0)
    coverage = EXCHANGE_COVERAGE.get(exchange_key, exchange_key)
    active = get_active_dip_categories(metrics)

    if not active:
        return

    fig, ax = plt.subplots(figsize=(9, 5))

    for cat in active:
        cat_data = metrics.get(cat, {})
        cars = [cat_data.get(f"T+{w}", {}).get("mean_car", 0) for w in windows]
        n = cat_data.get("n", 0)
        color = DIP_COLORS[cat]
        linestyle = "--" if cat == "overall" else "-"
        lw = 2.5 if cat == "overall" else 1.8
        marker = "o" if cat == "overall" else "s"
        label = f"{DIP_LABELS[cat]} (n={n:,})"
        ax.plot(range(len(windows)), cars, color=color, linewidth=lw,
                linestyle=linestyle, marker=marker, label=label, alpha=0.9)

    ax.axhline(0, color="black", linewidth=0.8, linestyle=":", alpha=0.5)
    ax.set_xticks(range(len(windows)))
    ax.set_xticklabels(w_labels)
    ax.set_ylabel("Mean CAR vs benchmark (%)")
    ax.set_title(
        f"Post-Earnings Dip CAR Progression: {exchange_label}\n"
        f"({coverage}, N={n_total:,} total dip events, winsorized mean)",
        fontsize=11, fontweight="bold"
    )
    ax.legend(fontsize=8, loc="upper left")
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
    """Horizontal bar chart of overall T+21 CAR, sorted by magnitude."""
    rows = []
    for ex_key, d in comparison_data.items():
        if "error" in d or not d.get("car_metrics"):
            continue
        label = EXCHANGE_LABELS.get(ex_key, ex_key)
        overall = d["car_metrics"].get("overall", {})
        t21 = overall.get("T+21", {})
        val = t21.get("mean_car", None)
        n = overall.get("n", 0)
        t_stat = t21.get("t_stat", 0)
        sig = "**" if abs(t_stat) >= 2.576 else ("*" if abs(t_stat) >= 1.96 else "")
        if val is not None and n >= 100:
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

    bars = ax.barh(y, vals, color=colours, height=0.6)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    for bar, v, n, sig in zip(bars, vals, ns, sigs):
        x_pos = v + 0.03 if v >= 0 else v - 0.03
        ha = "left" if v >= 0 else "right"
        label_text = f"{v:+.2f}%{sig}  (n={n:,})"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                label_text, va="center", ha=ha,
                fontsize=7.5, fontweight="bold",
                color=COL_POS if v >= 0 else COL_NEG)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Overall Mean CAR at T+21 (%)")
    ax.set_title(
        "Post-Earnings Dip: T+21 CAR by Exchange\n"
        "(Beat + 5%+ sell-off, measured from dip bottom; ** p<0.01, * p<0.05)",
        fontsize=11, fontweight="bold"
    )
    ax.grid(axis="x", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    pos_patch = mpatches.Patch(color=COL_POS, label="Positive reversion")
    neg_patch = mpatches.Patch(color=COL_NEG, label="Continued sell-off")
    ax.legend(handles=[pos_patch, neg_patch], loc="lower right", fontsize=8)

    fig.text(0.99, 0.01, "Data: Ceta Research (FMP warehouse) · cetaresearch.com",
             ha="right", va="bottom", fontsize=7, color="gray")

    fig.tight_layout()
    return save(fig, output_dir, "3_exchange_comparison.png")


# Exchanges to generate per-exchange charts for in --all-exchanges mode
ALL_BLOG_EXCHANGES = [
    ("NYSE_NASDAQ_AMEX", "US"),
    ("TAI_TWO", "Taiwan"),
    ("NSE", "India"),
    ("TSX", "Canada"),
    ("JPX", "Japan"),
    ("STO", "Sweden"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate Post-Earnings Dip charts")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output directory for chart images")
    parser.add_argument("--exchange", default="NYSE_NASDAQ_AMEX",
                        help="Exchange key (e.g. TAI_TWO, NSE)")
    parser.add_argument("--label", default=None,
                        help="Display label for the exchange (e.g. 'Taiwan'). "
                             "Defaults to EXCHANGE_LABELS lookup.")
    parser.add_argument("--all-exchanges", action="store_true",
                        help="Generate charts for all blog exchanges")
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
            print(f"  Run backtest.py --preset {ex_label.lower()} first")
            continue

        print(f"Generating charts for {ex_label}...")
        chart_car_by_dip(ex_data, args.output, ex_label, ex_key)
        chart_car_progression(ex_data, args.output, ex_label, ex_key)

    if not args.no_comparison:
        comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
        if os.path.exists(comparison_path):
            print("Generating exchange comparison chart...")
            comparison = load_comparison()
            chart_exchange_comparison(comparison, args.output)
        else:
            print("  Skipping comparison (no exchange_comparison.json found)")
            print("  Run backtest.py --global first")

    print(f"\nDone. Charts saved to: {args.output}/")


if __name__ == "__main__":
    main()
