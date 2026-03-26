#!/usr/bin/env python3
"""Generate charts for S&P 500 survivorship bias analysis.

Reads results/summary.json and produces two charts:
  1. Cumulative growth: biased vs unbiased vs SPY (2000-2025)
  2. Annual returns: biased vs unbiased side-by-side bars showing the gap

Usage:
    python3 sp500-survivorship/generate_charts.py
"""

import json
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


# Colors: blue = biased, orange = unbiased, gray = SPY
COLORS = {
    "biased": "#2980b9",
    "unbiased": "#e67e22",
    "spy": "#aab7b8",
    "gap_pos": "#27ae60",
    "gap_neg": "#c0392b",
}

FOOTER = ("Data: Ceta Research | S&P 500 low P/E screen (P/E < 15), "
          "top 100, quarterly rebalance, equal weight, 2000-2025")


def load_results():
    path = os.path.join(os.path.dirname(__file__), "results", "summary.json")
    with open(path) as f:
        return json.load(f)


def cumulative_growth_chart(data, out_dir):
    """Chart 1: Growth of $10,000 for biased, unbiased, and SPY."""
    quarters = data["quarterly_returns"]
    portfolios = data["portfolios"]

    # Build cumulative values from quarterly returns
    biased_vals = [10000]
    unbiased_vals = [10000]
    spy_vals = [10000]
    labels = [f"{quarters[0]['year'] - 1}"]

    for q in quarters:
        biased_vals.append(biased_vals[-1] * (1 + q["biased"] / 100))
        unbiased_vals.append(unbiased_vals[-1] * (1 + q["unbiased"] / 100))
        spy_ret = q["spy"] if q["spy"] is not None else 0
        spy_vals.append(spy_vals[-1] * (1 + spy_ret / 100))
        labels.append(f"{q['year']}Q{q['quarter']}")

    x = list(range(len(biased_vals)))

    # Compute year positions for x-axis labels (show only Q1 of each year)
    tick_positions = [0]
    tick_labels = [str(quarters[0]["year"] - 1)]
    for i, q in enumerate(quarters):
        if q["quarter"] == 1:
            tick_positions.append(i + 1)
            tick_labels.append(str(q["year"]))

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(x, spy_vals, color=COLORS["spy"], linewidth=1.8, linestyle="--",
            label=f"S&P 500 (SPY) ({portfolios['spy']['cagr']}% CAGR)")
    ax.plot(x, unbiased_vals, color=COLORS["unbiased"], linewidth=2.2,
            label=f"Unbiased / Point-in-Time ({portfolios['unbiased']['cagr']}% CAGR)")
    ax.plot(x, biased_vals, color=COLORS["biased"], linewidth=2.2,
            label=f"Biased / Current S&P 500 ({portfolios['biased']['cagr']}% CAGR)")

    # Final value annotations
    for vals, color, offset_y in [
        (biased_vals, COLORS["biased"], 10),
        (unbiased_vals, COLORS["unbiased"], -5),
        (spy_vals, COLORS["spy"], -18),
    ]:
        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(x[-1], vals[-1]),
                    xytext=(8, offset_y), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=color)

    # Shade the gap between biased and unbiased
    ax.fill_between(x, unbiased_vals, biased_vals,
                    alpha=0.08, color=COLORS["biased"],
                    label="Survivorship bias gap")

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title("Growth of $10,000: Survivorship Bias in S&P 500 Backtests (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=9, loc="upper left")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, p: f"${v:,.0f}")
    )
    ax.set_ylim(0, None)
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.04, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = os.path.join(out_dir, "1_us_cumulative_growth.png")
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def annual_gap_chart(data, out_dir):
    """Chart 2: Annual returns for biased vs unbiased, with gap highlighted."""
    quarters = data["quarterly_returns"]

    # Aggregate quarterly returns into annual returns
    annual = defaultdict(lambda: {"biased": 1.0, "unbiased": 1.0, "spy": 1.0})
    for q in quarters:
        yr = q["year"]
        annual[yr]["biased"] *= (1 + q["biased"] / 100)
        annual[yr]["unbiased"] *= (1 + q["unbiased"] / 100)
        spy_ret = q["spy"] if q["spy"] is not None else 0
        annual[yr]["spy"] *= (1 + spy_ret / 100)

    years = sorted(annual.keys())
    biased_annual = [(annual[y]["biased"] - 1) * 100 for y in years]
    unbiased_annual = [(annual[y]["unbiased"] - 1) * 100 for y in years]
    spy_annual = [(annual[y]["spy"] - 1) * 100 for y in years]
    gaps = [b - u for b, u in zip(biased_annual, unbiased_annual)]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9),
                                    gridspec_kw={"height_ratios": [3, 1]})

    # Top panel: annual returns
    x = list(range(len(years)))
    width = 0.25

    offsets = [i - width for i in x]
    ax1.bar([o for o in offsets], spy_annual, width,
            label="S&P 500 (SPY)", color=COLORS["spy"], alpha=0.7)
    ax1.bar([o + width for o in offsets], unbiased_annual, width,
            label="Unbiased (PIT)", color=COLORS["unbiased"], alpha=0.85)
    ax1.bar([o + 2 * width for o in offsets], biased_annual, width,
            label="Biased (Current)", color=COLORS["biased"], alpha=0.85)

    ax1.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax1.set_title("Annual Returns: Biased vs Unbiased S&P 500 Low P/E (2000-2025)",
                  fontsize=13, fontweight="bold", pad=15)
    ax1.set_xticks(x)
    ax1.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax1.legend(fontsize=9, loc="upper left", ncol=3)
    ax1.axhline(y=0, color="black", linewidth=0.5)
    ax1.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax1.set_axisbelow(True)

    # Bottom panel: survivorship bias gap
    gap_colors = [COLORS["gap_pos"] if g >= 0 else COLORS["gap_neg"] for g in gaps]
    ax2.bar(x, gaps, width=0.6, color=gap_colors, alpha=0.8)
    ax2.set_ylabel("Bias Gap (pp)", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Year", fontsize=11)
    ax2.set_title("Survivorship Bias Gap (Biased - Unbiased)", fontsize=11, pad=8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax2.axhline(y=0, color="black", linewidth=0.5)
    ax2.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax2.set_axisbelow(True)

    # Average gap annotation
    avg_gap = sum(gaps) / len(gaps) if gaps else 0
    ax2.axhline(y=avg_gap, color=COLORS["biased"], linewidth=1.2,
                linestyle=":", alpha=0.7)
    ax2.annotate(f"Avg gap: {avg_gap:+.1f}pp",
                 xy=(len(years) - 1, avg_gap),
                 xytext=(5, 8), textcoords="offset points",
                 fontsize=9, color=COLORS["biased"], fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = os.path.join(out_dir, "2_us_annual_returns.png")
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


if __name__ == "__main__":
    data = load_results()
    out_dir = os.path.join(os.path.dirname(__file__), "charts")
    os.makedirs(out_dir, exist_ok=True)

    print("Generating charts for S&P 500 survivorship bias analysis...")
    cumulative_growth_chart(data, out_dir)
    annual_gap_chart(data, out_dir)
    print(f"\nDone. Charts saved to {out_dir}/")
