#!/usr/bin/env python3
"""
Generate charts for Sector Correlation Regime Backtest.

Reads from results/backtest_results.json and produces:
  1_us_cumulative_growth.png   - Strategy vs SPY cumulative growth
  2_us_correlation_regimes.png - Monthly avg correlation with regime shading
  3_us_annual_returns.png      - Annual returns bar chart

Charts are saved to sector-correlation/charts/. Move them to the blog dirs
in ts-content-creator before publishing.

Usage:
    cd backtests/
    python3 sector-correlation/generate_charts.py
"""

import json
import os
import sys
from datetime import datetime

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
except ImportError:
    print("ERROR: matplotlib and numpy required. Run: pip install matplotlib numpy")
    sys.exit(1)

RESULTS_FILE = os.path.join(os.path.dirname(__file__), "results", "backtest_results.json")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "charts")

# Style
plt.rcParams.update({
    "font.family": "sans-serif",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.4,
    "figure.dpi": 150,
})

STRATEGY_COLOR = "#2563EB"   # Blue
SPY_COLOR = "#9CA3AF"        # Gray
HIGH_COLOR = "#FCA5A5"       # Light red (high correlation / defensive)
LOW_COLOR = "#BBF7D0"        # Light green (low correlation / diversified)
MEDIUM_COLOR = "#DBEAFE"     # Light blue (medium / SPY)

HIGH_THRESHOLD = 0.70
LOW_THRESHOLD = 0.40


def load_results():
    if not os.path.exists(RESULTS_FILE):
        print(f"ERROR: Results file not found: {RESULTS_FILE}")
        print("Run backtest first: python3 sector-correlation/backtest.py --output results/backtest_results.json")
        sys.exit(1)
    with open(RESULTS_FILE) as f:
        return json.load(f)


def build_cumulative(monthly_returns):
    """Build cumulative return series. Returns (dates, strategy, spy) lists."""
    dates = []
    port_cum = [1.0]
    spy_cum = [1.0]

    for r in monthly_returns:
        port_ret = r.get("portfolio_return")
        spy_ret = r.get("spy_return")
        if port_ret is None or spy_ret is None:
            continue
        dates.append(datetime.strptime(r["date"], "%Y-%m-%d"))
        port_cum.append(port_cum[-1] * (1 + port_ret))
        spy_cum.append(spy_cum[-1] * (1 + spy_ret))

    # dates are month starts; cumulative values are after that month
    # align: dates[0] = Jan 2000 entry, port_cum[1] = end of Jan 2000
    return dates, port_cum[1:], spy_cum[1:]


def chart_1_cumulative(data, charts_dir):
    """Cumulative growth of $10,000 — strategy vs SPY."""
    monthly = data.get("monthly_returns", [])
    dates, port_cum, spy_cum = build_cumulative(monthly)
    if not dates:
        print("  ERROR: No monthly return data.")
        return

    port_final = port_cum[-1] * 10000
    spy_final = spy_cum[-1] * 10000
    port_cagr = data["portfolio"]["cagr"]
    spy_cagr = data["spy"]["cagr"]

    fig, ax = plt.subplots(figsize=(10, 5.5))

    ax.plot(dates, [v * 10000 for v in port_cum], color=STRATEGY_COLOR,
            linewidth=2, label=f"Correlation Regime ({port_cagr:.2f}% CAGR)", zorder=3)
    ax.plot(dates, [v * 10000 for v in spy_cum], color=SPY_COLOR,
            linewidth=2, linestyle="--", label=f"SPY Buy & Hold ({spy_cagr:.2f}% CAGR)", zorder=2)

    # Shade regime periods
    for i, r in enumerate(monthly):
        if i >= len(dates):
            break
        regime = r.get("regime", "medium")
        color = {"high": HIGH_COLOR, "low": LOW_COLOR, "medium": MEDIUM_COLOR}[regime]
        ax.axvspan(dates[i],
                   dates[i + 1] if i + 1 < len(dates) else dates[-1],
                   alpha=0.3, color=color, linewidth=0, zorder=1)

    # Legend patches for regimes
    high_patch = mpatches.Patch(color=HIGH_COLOR, alpha=0.6, label="High Correlation (defensive)")
    low_patch = mpatches.Patch(color=LOW_COLOR, alpha=0.6, label="Low Correlation (diversified)")
    med_patch = mpatches.Patch(color=MEDIUM_COLOR, alpha=0.6, label="Medium (SPY)")

    ax.set_title("Sector Correlation Regime: $10,000 Invested (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel("Portfolio Value ($)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(handles=[
        ax.get_lines()[0], ax.get_lines()[1], high_patch, low_patch, med_patch
    ], fontsize=9, loc="upper left")

    ax.annotate(f"${port_final:,.0f}", xy=(dates[-1], port_final),
                xytext=(10, 0), textcoords="offset points",
                va="center", color=STRATEGY_COLOR, fontsize=9, fontweight="bold")
    ax.annotate(f"${spy_final:,.0f}", xy=(dates[-1], spy_final),
                xytext=(10, 0), textcoords="offset points",
                va="center", color=SPY_COLOR, fontsize=9)

    fig.tight_layout()
    path = os.path.join(charts_dir, "1_us_cumulative_growth.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_2_correlation_series(data, charts_dir):
    """Monthly avg pairwise correlation with regime shading."""
    monthly = data.get("monthly_returns", [])

    dates = []
    corr_vals = []
    regimes = []

    for r in monthly:
        corr = r.get("avg_correlation")
        if corr is None:
            continue
        dates.append(datetime.strptime(r["date"], "%Y-%m-%d"))
        corr_vals.append(corr)
        regimes.append(r.get("regime", "medium"))

    if not dates:
        print("  ERROR: No correlation data.")
        return

    fig, ax = plt.subplots(figsize=(10, 4.5))

    # Regime background
    for i in range(len(dates)):
        end = dates[i + 1] if i + 1 < len(dates) else dates[-1]
        color = {"high": HIGH_COLOR, "low": LOW_COLOR, "medium": MEDIUM_COLOR}[regimes[i]]
        ax.axvspan(dates[i], end, alpha=0.3, color=color, linewidth=0)

    ax.plot(dates, corr_vals, color="#374151", linewidth=1.2, label="Avg pairwise correlation")
    ax.axhline(HIGH_THRESHOLD, color="#EF4444", linewidth=1.2, linestyle="--",
               label=f"High threshold ({HIGH_THRESHOLD})")
    ax.axhline(LOW_THRESHOLD, color="#10B981", linewidth=1.2, linestyle="--",
               label=f"Low threshold ({LOW_THRESHOLD})")

    ax.set_title("60-Day Rolling Average Sector Correlation (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel("Average Pairwise Correlation")
    ax.set_ylim(0, 1.0)

    high_patch = mpatches.Patch(color=HIGH_COLOR, alpha=0.6, label="High regime")
    low_patch = mpatches.Patch(color=LOW_COLOR, alpha=0.6, label="Low regime")
    med_patch = mpatches.Patch(color=MEDIUM_COLOR, alpha=0.6, label="Medium regime")
    handles = ax.get_lines()[:3] + [high_patch, low_patch, med_patch]
    ax.legend(handles=handles, fontsize=9, loc="upper left")

    fig.tight_layout()
    path = os.path.join(charts_dir, "2_us_correlation_regimes.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_3_annual_returns(data, charts_dir):
    """Annual returns bar chart — strategy vs SPY."""
    annual = data.get("annual_returns", [])
    if not annual:
        print("  ERROR: No annual return data.")
        return

    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = range(len(years))
    width = 0.4

    fig, ax = plt.subplots(figsize=(12, 5))

    bars1 = ax.bar([i - width / 2 for i in x], port_rets, width,
                   label="Correlation Regime", color=STRATEGY_COLOR, alpha=0.85)
    bars2 = ax.bar([i + width / 2 for i in x], spy_rets, width,
                   label="SPY", color=SPY_COLOR, alpha=0.85)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Annual Returns: Correlation Regime vs SPY (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(fontsize=9)

    fig.tight_layout()
    path = os.path.join(charts_dir, "3_us_annual_returns.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def main():
    data = load_results()

    os.makedirs(CHARTS_DIR, exist_ok=True)
    print(f"Generating charts to {CHARTS_DIR}/")

    print("  Chart 1: Cumulative growth...")
    chart_1_cumulative(data, CHARTS_DIR)

    print("  Chart 2: Correlation regimes time series...")
    chart_2_correlation_series(data, CHARTS_DIR)

    print("  Chart 3: Annual returns...")
    chart_3_annual_returns(data, CHARTS_DIR)

    print("\nDone. Move charts to ts-content-creator blog dirs before publishing.")
    print("  mv backtests/sector-correlation/charts/*.png ../ts-content-creator/content/_current/sector-02-correlation/blogs/us/")


if __name__ == "__main__":
    main()
