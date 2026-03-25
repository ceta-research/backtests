#!/usr/bin/env python3
"""
Generate charts for Cyclical Sector Timing strategy.

Creates cumulative growth and annual returns charts for each exchange.
Charts are saved to cyclical-timing/charts/ and must be moved to
ts-content-creator/content/_current/sector-05-cyclical-timing/blogs/{region}/

Usage:
    cd backtests
    python3 cyclical-timing/generate_charts.py
"""

import json
import os
import sys

# Require matplotlib
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
    import numpy as np
except ImportError:
    print("Error: matplotlib not installed. Run: pip install matplotlib")
    sys.exit(1)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "charts")

# Chart style
STRATEGY_COLOR = "#2196F3"    # Blue
BENCHMARK_COLOR = "#9E9E9E"   # Gray
POSITIVE_COLOR = "#4CAF50"    # Green
NEGATIVE_COLOR = "#F44336"    # Red


def load_results(exchange_key):
    """Load backtest results for an exchange."""
    path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    with open(path) as f:
        data = json.load(f)
    return data.get(exchange_key)


def cumulative_growth(returns):
    """Compute cumulative growth index (starting at 1.0)."""
    curve = [1.0]
    for r in returns:
        curve.append(curve[-1] * (1 + r / 100))
    return curve


def plot_cumulative(data, label, output_path, title_suffix=""):
    """Plot cumulative growth chart (strategy vs SPY)."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    port_curve = cumulative_growth(port_rets)
    spy_curve = cumulative_growth(spy_rets)

    # x-axis: years with a starting point one year before first year
    x = [years[0] - 1] + years
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x, port_curve, color=STRATEGY_COLOR, linewidth=2.5,
            label=f"Cyclical Timing  (CAGR: {data['portfolio']['cagr']:.1f}%)")
    ax.plot(x, spy_curve, color=BENCHMARK_COLOR, linewidth=1.8, linestyle="--",
            label=f"S&P 500 (SPY)  (CAGR: {data['spy']['cagr']:.1f}%)")

    ax.set_title(f"Cyclical Sector Timing vs S&P 500\n{label}{title_suffix}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Portfolio Value ($1 Start)", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"${x:.1f}"))
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":")
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)

    # Annotate key metrics
    stats = data.get("comparison", {})
    down_capture = stats.get("down_capture")
    excess = stats.get("excess_cagr")
    n = data.get("n_periods", 0)
    cash_pct = round(data.get("cash_periods", 0) * 100 / n, 0) if n > 0 else 0

    info_text = (
        f"Max Drawdown: {data['portfolio']['max_drawdown']:.1f}%\n"
        f"Down Capture: {down_capture:.1f}%\n"
        f"Excess CAGR: {excess:+.2f}%\n"
        f"Cash periods: {cash_pct:.0f}%"
    )
    ax.text(0.02, 0.97, info_text,
            transform=ax.transAxes, fontsize=9,
            verticalalignment="top", fontfamily="monospace",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                      edgecolor="lightgray", alpha=0.9))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_annual_returns(data, label, output_path, title_suffix=""):
    """Plot annual returns bar chart (strategy vs SPY)."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = np.arange(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(12, 6))

    bars1 = ax.bar(x - width / 2, port_rets, width,
                   color=[POSITIVE_COLOR if r >= 0 else NEGATIVE_COLOR for r in port_rets],
                   alpha=0.85, label="Cyclical Timing")
    bars2 = ax.bar(x + width / 2, spy_rets, width,
                   color=BENCHMARK_COLOR, alpha=0.6, label="S&P 500")

    ax.set_title(f"Annual Returns: Cyclical Timing vs S&P 500\n{label}{title_suffix}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Annual Return (%)", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":", axis="y")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_comparison_cagr(all_data, output_path):
    """Plot CAGR comparison across all eligible exchanges."""
    exchanges = []
    cagrs = []
    spy_cagr = None

    # Only include exchanges with positive avg stocks
    for exch, data in sorted(all_data.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                              reverse=True):
        if "error" in data or not data.get("portfolio"):
            continue
        cagr = data["portfolio"].get("cagr")
        if cagr is not None:
            exchanges.append(exch.replace("_", "+"))
            cagrs.append(cagr)
            if spy_cagr is None:
                spy_cagr = data["spy"].get("cagr")

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = [POSITIVE_COLOR if c >= (spy_cagr or 0) else NEGATIVE_COLOR for c in cagrs]
    bars = ax.barh(exchanges, cagrs, color=colors, alpha=0.85)

    if spy_cagr:
        ax.axvline(spy_cagr, color=BENCHMARK_COLOR, linewidth=2, linestyle="--",
                   label=f"S&P 500 ({spy_cagr:.1f}%)")
        ax.legend(fontsize=11)

    # Label bars
    for bar, val in zip(bars, cagrs):
        ax.text(val + 0.1, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", fontsize=9)

    ax.set_title("Cyclical Sector Timing: CAGR by Exchange (2001–2024)",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("CAGR (%)", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.grid(True, alpha=0.3, linestyle=":", axis="x")
    ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_comparison_drawdown(all_data, output_path):
    """Plot max drawdown comparison across exchanges."""
    exchanges = []
    drawdowns = []

    for exch, data in sorted(all_data.items(),
                              key=lambda x: (x[1].get("portfolio") or {}).get("max_drawdown") or 0):
        if "error" in data or not data.get("portfolio"):
            continue
        mdd = data["portfolio"].get("max_drawdown")
        if mdd is not None:
            exchanges.append(exch.replace("_", "+"))
            drawdowns.append(mdd)

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(exchanges, drawdowns, color=NEGATIVE_COLOR, alpha=0.75)

    ax.set_title("Cyclical Sector Timing: Max Drawdown by Exchange (2001–2024)",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.grid(True, alpha=0.3, linestyle=":", axis="x")
    ax.invert_yaxis()

    for bar, val in zip(ax.patches, drawdowns):
        ax.text(val - 0.5, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", ha="right", fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": ("us", "United States (NYSE+NASDAQ+AMEX)"),
    "NSE": ("india", "India (NSE)"),
    "XETRA": ("germany", "Germany (XETRA)"),
    "ASX": ("australia", "Australia (ASX)"),
    "STO": ("sweden", "Sweden (STO)"),
    "LSE": ("uk", "United Kingdom (LSE)"),
    "TSX": ("canada", "Canada (TSX)"),
    "SIX": ("switzerland", "Switzerland (SIX)"),
    "JPX": ("japan", "Japan (JPX)"),
    "SAO": ("brazil", "Brazil (SAO)"),
    "JNB": ("southafrica", "South Africa (JNB)"),
    "HKSE": ("hongkong", "Hong Kong (HKSE)"),
    "KSC": ("korea", "Korea (KSC)"),
    "TAI_TWO": ("taiwan", "Taiwan (TAI+TWO)"),
    "SHZ_SHH": ("china", "China (SHZ+SHH)"),
}


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # Load all results
    comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(comparison_path):
        print(f"Error: {comparison_path} not found. Run backtest first.")
        sys.exit(1)

    with open(comparison_path) as f:
        all_data = json.load(f)

    print("Generating charts...")

    # Per-exchange charts
    for exch_key, (region_slug, label) in EXCHANGE_LABELS.items():
        data = all_data.get(exch_key)
        if not data or "error" in data or not data.get("portfolio"):
            print(f"  Skipping {exch_key} (no results)")
            continue

        print(f"\n  {label}")
        plot_cumulative(
            data, label,
            os.path.join(CHARTS_DIR, f"1_{region_slug}_cumulative_growth.png")
        )
        plot_annual_returns(
            data, label,
            os.path.join(CHARTS_DIR, f"2_{region_slug}_annual_returns.png")
        )

    # Comparison charts
    print("\n  Comparison charts")
    plot_comparison_cagr(
        all_data,
        os.path.join(CHARTS_DIR, "1_comparison_cagr.png")
    )
    plot_comparison_drawdown(
        all_data,
        os.path.join(CHARTS_DIR, "2_comparison_drawdown.png")
    )

    print(f"\nAll charts saved to {CHARTS_DIR}/")
    print("\nNext step: Move charts to ts-content-creator/content/_current/sector-05-cyclical-timing/blogs/")


if __name__ == "__main__":
    main()
