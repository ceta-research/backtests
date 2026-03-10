"""Generate charts for the pairs-02-screening blog post.

Reads from results/screening_summary.json and produces:
  1_sector_distribution.png  - Candidate pairs by sector (bar chart)
  2_correlation_distribution.png - Correlation distribution (histogram)

Usage:
    cd backtests
    python3 pairs-screening/generate_charts.py

Charts are saved to pairs-screening/charts/.
Move them to the blog directory after generation:
    mv pairs-screening/charts/1_sector_distribution.png \\
       ../ts-content-creator/content/_current/pairs-02-screening/blogs/us/
    mv pairs-screening/charts/2_correlation_distribution.png \\
       ../ts-content-creator/content/_current/pairs-02-screening/blogs/us/
"""

import json
import csv
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

results_dir = Path(__file__).parent.parent.parent / "ts-content-creator" / "content" / "_current" / "pairs-02-screening" / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "screening_summary.json") as f:
    summary = json.load(f)

SECTOR_COLORS = {
    "Financial Services": "#1a5276",
    "Real Estate": "#2980b9",
    "Energy": "#e67e22",
    "Consumer Cyclical": "#f39c12",
    "Utilities": "#27ae60",
    "Communication Services": "#8e44ad",
    "Technology": "#c0392b",
    "Healthcare": "#e74c3c",
    "Industrials": "#7f8c8d",
    "Basic Materials": "#95a5a6",
    "Consumer Defensive": "#bdc3c7",
}

FOOTER = "Data: Ceta Research (FMP warehouse) | US stocks > $1B market cap | Lookback: 252 trading days"


def chart_sector_distribution():
    """Bar chart: candidate pairs by sector."""
    sectors = summary["sector_breakdown"]
    # Sort by pair_count descending
    sectors = sorted(sectors, key=lambda x: x["pair_count"], reverse=True)

    names = [s["sector"] for s in sectors]
    counts = [s["pair_count"] for s in sectors]
    colors = [SECTOR_COLORS.get(s["sector"], "#95a5a6") for s in sectors]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(range(len(names)), counts, color=colors, alpha=0.85, width=0.6)

    # Value labels
    for bar, count in zip(bars, counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 5,
            f"{count:,}",
            ha="center", va="bottom", fontsize=9, fontweight="bold"
        )

    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, rotation=35, ha="right", fontsize=10)
    ax.set_ylabel("Candidate Pairs", fontsize=12, fontweight="bold")
    ax.set_title(
        f"Pairs Trading Candidates by Sector (US, corr ≥ 0.80)\n"
        f"Total: {summary['total_pairs']:,} pairs | Universe: {summary.get('universe_size', 3767):,} stocks",
        fontsize=13, fontweight="bold", pad=12
    )
    ax.grid(True, alpha=0.3, axis="y", linestyle="--")
    ax.set_axisbelow(True)
    ax.set_ylim(0, max(counts) * 1.15)

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "1_sector_distribution.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_correlation_distribution():
    """Bar chart: candidate pairs by correlation bucket."""
    # Load candidate pairs CSV for distribution
    pairs_path = results_dir / "candidate_pairs.csv"
    if not pairs_path.exists():
        print("  Skipping correlation distribution: candidate_pairs.csv not found")
        return

    correlations = []
    with open(pairs_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                correlations.append(float(row["correlation"]))
            except (ValueError, KeyError):
                pass

    if not correlations:
        print("  Skipping correlation distribution: no correlation data found")
        return

    # Bucket into 0.80-0.85, 0.85-0.90, 0.90-0.95, 0.95-1.00
    buckets = [
        (0.80, 0.85, "0.80-0.85"),
        (0.85, 0.90, "0.85-0.90"),
        (0.90, 0.95, "0.90-0.95"),
        (0.95, 1.01, "0.95-1.00"),
    ]
    counts = []
    labels = []
    for lo, hi, label in buckets:
        count = sum(1 for c in correlations if lo <= c < hi)
        counts.append(count)
        labels.append(label)

    total = sum(counts)
    pcts = [100 * c / total for c in counts]

    fig, ax = plt.subplots(figsize=(9, 5))
    colors = ["#1a5276", "#2980b9", "#7fb3d8", "#aed6f1"]
    bars = ax.bar(labels, counts, color=colors, alpha=0.85, width=0.5)

    for bar, count, pct in zip(bars, counts, pcts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 5,
            f"{count:,}\n({pct:.1f}%)",
            ha="center", va="bottom", fontsize=10, fontweight="bold"
        )

    ax.set_xlabel("Correlation Range", fontsize=12, fontweight="bold")
    ax.set_ylabel("Number of Pairs", fontsize=12, fontweight="bold")
    ax.set_title(
        f"Correlation Distribution of {total:,} Candidate Pairs (US)\n"
        f"Minimum threshold: 0.80 | Average: {sum(correlations)/len(correlations):.3f}",
        fontsize=13, fontweight="bold", pad=12
    )
    ax.grid(True, alpha=0.3, axis="y", linestyle="--")
    ax.set_axisbelow(True)
    ax.set_ylim(0, max(counts) * 1.20)

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "2_correlation_distribution.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


print("Generating pairs-screening charts...")
chart_sector_distribution()
chart_correlation_distribution()
print(f"\nDone. Charts saved to {charts_dir}/")
print("\nNext: move charts to blog directory:")
print("  mv pairs-screening/charts/1_sector_distribution.png \\")
print("     ../ts-content-creator/content/_current/pairs-02-screening/blogs/us/")
print("  mv pairs-screening/charts/2_correlation_distribution.png \\")
print("     ../ts-content-creator/content/_current/pairs-02-screening/blogs/us/")
