#!/usr/bin/env python3
"""Generate charts for pairs cointegration analysis.

Reads from:
    - pairs-02-screening results: candidate_pairs.csv
    - pairs-03-cointegration results: cointegrated_pairs.csv

Produces 2 charts saved to pairs-cointegration/charts/:
    1. 1_cointegration_pass_rates.png — sector-level candidates vs cointegrated bar chart
    2. 2_halflife_distribution.png   — histogram of half-life among cointegrated pairs

Usage:
    python3 pairs-cointegration/generate_charts.py
"""

import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ─── Input paths ──────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.abspath(__file__))
_BACKTESTS_ROOT = os.path.dirname(_ROOT)
CANDIDATES_CSV = os.path.join(
    _BACKTESTS_ROOT, "..", "ts-content-creator", "content", "_ready",
    "pairs-02-screening", "results", "candidate_pairs.csv"
)
COINTEGRATED_CSV = os.path.join(
    _BACKTESTS_ROOT, "..", "ts-content-creator", "content", "_current",
    "pairs-03-cointegration", "results", "cointegrated_pairs.csv"
)

# ─── Output path ──────────────────────────────────────────────────────────────
CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")

# ─── Chart colors ─────────────────────────────────────────────────────────────
COLOR_CANDIDATES    = "#aed6f1"   # light blue
COLOR_COINTEGRATED  = "#1a5276"   # dark blue
COLOR_MEDIAN_LINE   = "#c0392b"   # red for median line

FOOTER_TEXT = "Data: Ceta Research (FMP warehouse) | US stocks > $1B | Lookback: 252 trading days"


def load_csv(path, label):
    """Load a CSV file. Exit with error if not found."""
    if not os.path.exists(path):
        print(f"ERROR: {label} not found: {path}")
        print("Run the appropriate script first to generate this file.")
        sys.exit(1)
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"Loaded {len(rows):,} rows from {label}")
    return rows


def count_by_sector(rows, field="sector"):
    """Count rows grouped by sector."""
    counts = defaultdict(int)
    for row in rows:
        sector = row.get(field, "Unknown")
        counts[sector] += 1
    return dict(counts)


def chart_pass_rates(candidates, cointegrated, output_path):
    """Chart 1: Side-by-side bar chart of candidates vs cointegrated by sector.

    Sectors ordered by pass rate descending.
    Pass rate percentage shown on top of cointegrated bars.
    """
    cand_counts  = count_by_sector(candidates)
    coint_counts = count_by_sector(cointegrated)

    # Only include sectors that have at least one candidate
    all_sectors = [s for s in cand_counts if cand_counts[s] > 0]

    # Compute pass rates and sort descending
    sector_data = []
    for sector in all_sectors:
        c = cand_counts.get(sector, 0)
        p = coint_counts.get(sector, 0)
        rate = 100.0 * p / c if c > 0 else 0
        sector_data.append((sector, c, p, rate))

    sector_data.sort(key=lambda x: x[3], reverse=True)

    sectors    = [x[0] for x in sector_data]
    cand_vals  = [x[1] for x in sector_data]
    coint_vals = [x[2] for x in sector_data]
    pass_rates = [x[3] for x in sector_data]

    total_cand  = len(candidates)
    total_coint = len(cointegrated)
    overall_rate = 100.0 * total_coint / total_cand if total_cand > 0 else 0

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(13, 6))

    x     = np.arange(len(sectors))
    width = 0.38

    bars_cand  = ax.bar(x - width / 2, cand_vals,  width, color=COLOR_CANDIDATES,
                        label="Candidates", edgecolor="white", linewidth=0.5)
    bars_coint = ax.bar(x + width / 2, coint_vals, width, color=COLOR_COINTEGRATED,
                        label="Cointegrated", edgecolor="white", linewidth=0.5)

    # Pass rate labels on top of cointegrated bars
    for bar, rate, p_count in zip(bars_coint, pass_rates, coint_vals):
        if p_count > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(cand_vals) * 0.01,
                f"{rate:.0f}%",
                ha="center", va="bottom",
                fontsize=8, color=COLOR_COINTEGRATED, fontweight="bold",
            )

    ax.set_xticks(x)
    ax.set_xticklabels(sectors, rotation=35, ha="right", fontsize=9)
    ax.set_ylabel("Pair Count", fontsize=11)
    ax.set_title(
        f"Cointegration Pass Rates by Sector (US)\n"
        f"ADF p<0.05, Half-life 5-120 days | "
        f"{total_cand:,} candidates \u2192 {total_coint:,} passed ({overall_rate:.1f}%)",
        fontsize=12, pad=14,
    )
    ax.legend(fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Footer
    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 1 saved: {output_path}")


def chart_halflife_distribution(cointegrated, output_path):
    """Chart 2: Histogram of half-life distribution among cointegrated pairs."""
    half_lives = []
    for row in cointegrated:
        hl = row.get("half_life_days", "")
        try:
            half_lives.append(float(hl))
        except (ValueError, TypeError):
            continue

    if not half_lives:
        print("WARNING: No half-life data found, skipping chart 2.")
        return

    avg_hl    = float(np.mean(half_lives))
    median_hl = float(np.median(half_lives))
    min_hl    = min(half_lives)
    max_hl    = max(half_lives)

    # Fixed bins: 5-day width
    bin_edges = list(range(5, 65, 5))  # 5,10,15,...,60
    if max_hl > bin_edges[-1]:
        bin_edges.append(int(max_hl) + 5)

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(11, 6))

    ax.hist(
        half_lives,
        bins=bin_edges,
        color=COLOR_COINTEGRATED,
        edgecolor="white",
        linewidth=0.5,
        rwidth=0.88,
    )

    # Median line
    ax.axvline(
        x=median_hl,
        color=COLOR_MEDIAN_LINE,
        linewidth=2,
        linestyle="--",
        label=f"Median: {median_hl:.1f} days",
    )

    ax.set_xlabel("Half-Life (trading days)", fontsize=11)
    ax.set_ylabel("Number of Pairs", fontsize=11)
    ax.set_title(
        f"Half-Life Distribution of Cointegrated Pairs\n"
        f"{len(half_lives):,} pairs | "
        f"Mean: {avg_hl:.1f} days, Median: {median_hl:.1f} days, "
        f"Range: {min_hl:.1f}\u2013{max_hl:.1f} days",
        fontsize=12, pad=14,
    )
    ax.legend(fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Footer
    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 2 saved: {output_path}")


def main():
    print("Generating pairs cointegration charts...")
    print()

    # ── Load data ──────────────────────────────────────────────────────────────
    candidates    = load_csv(CANDIDATES_CSV,   "candidate_pairs.csv")
    cointegrated  = load_csv(COINTEGRATED_CSV, "cointegrated_pairs.csv")

    # ── Create output directory ────────────────────────────────────────────────
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # ── Generate charts ────────────────────────────────────────────────────────
    chart_pass_rates(
        candidates,
        cointegrated,
        os.path.join(CHARTS_DIR, "1_cointegration_pass_rates.png"),
    )
    chart_halflife_distribution(
        cointegrated,
        os.path.join(CHARTS_DIR, "2_halflife_distribution.png"),
    )

    print()
    print("Done. Charts saved to pairs-cointegration/charts/")
    print()
    print("Next: move charts to blog directory:")
    print("  mv pairs-cointegration/charts/1_cointegration_pass_rates.png \\")
    print("     ../ts-content-creator/content/_current/pairs-03-cointegration/blogs/us/")
    print("  mv pairs-cointegration/charts/2_halflife_distribution.png \\")
    print("     ../ts-content-creator/content/_current/pairs-03-cointegration/blogs/us/")


if __name__ == "__main__":
    main()
