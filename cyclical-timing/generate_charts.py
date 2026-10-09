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
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (money, money_axis_label, money_formatter, currency_prefix, localize_money_title,
                         benchmark_label, benchmark_legend, is_usd_benchmark_proxy)
import os
import sys

# Require matplotlib
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
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


def plot_cumulative(data, label, output_path, exch_key, bench, title_suffix=""):
    """Plot cumulative growth chart (strategy vs the exchange's benchmark)."""
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
            label=f"{bench}  (CAGR: {data['spy']['cagr']:.1f}%)")

    ax.set_title(f"Cyclical Sector Timing vs {bench}\n{label}{title_suffix}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    # exch_key, not data["universe"]: returns files carry "Canada", "JSE" there
    ax.set_ylabel("Portfolio Value (" + currency_prefix(exch_key) + "1 Start)", fontsize=12)
    ax.yaxis.set_major_formatter(money_formatter(exch_key, decimals=1))
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":")
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)

    # Annotate key metrics
    stats = data.get("comparison", {})
    down_capture = stats.get("down_capture")
    excess = stats.get("excess_cagr")
    n = data.get("n_periods", 0)
    # B006: divide by total_rebalances, not n_periods. Cash is counted over
    # every rebalance the strategy ran; n_periods counts only the
    # benchmark-priced ones. Fallback keeps pre-B006 result files renderable.
    tr = data.get("total_rebalances") or n
    cash_pct = round(data.get("cash_periods", 0) * 100 / tr, 0) if tr > 0 else 0

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


def plot_annual_returns(data, label, output_path, bench, title_suffix=""):
    """Plot annual returns bar chart (strategy vs the exchange's benchmark)."""
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
                   color=BENCHMARK_COLOR, alpha=0.6, label=bench)

    ax.set_title(f"Annual Returns: Cyclical Timing vs {bench}\n{label}{title_suffix}",
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


def comparable_legs(all_data, metric):
    """(exch, data) legs fit to compare, plus names dropped for a truncated window or no investing."""
    legs, dropped = [], []
    for exch, data in all_data.items():
        if "error" in data or (data.get("portfolio") or {}).get(metric) is None:
            continue
        if data.get("window_truncated") or not data.get("invested_periods"):
            dropped.append(exch.replace("_", "+"))
        else:
            legs.append((exch, data))
    return legs, dropped


def window_of(legs):
    """Measured window from the results, e.g. '2001-2025'."""
    return " / ".join(sorted({d.get("window_label", "?") for _, d in legs}))


def plot_comparison_cagr(all_data, output_path):
    """Plot CAGR by exchange, each against its own benchmark (marker)."""
    legs, dropped = comparable_legs(all_data, "cagr")
    if not legs:
        return
    legs.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    exchanges = [exch.replace("_", "+") for exch, _ in legs]
    cagrs = [d["portfolio"]["cagr"] for _, d in legs]
    benches = [d["spy"]["cagr"] for _, d in legs]
    excesses = [d.get("comparison", {}).get("excess_cagr", c - b)
                for (_, d), c, b in zip(legs, cagrs, benches)]
    # No local index: benchmark is the S&P 500 in USD, a cross-currency gap
    proxy = [is_usd_benchmark_proxy(all_data, exch) for exch, _ in legs]
    colors = [BENCHMARK_COLOR if p else POSITIVE_COLOR if c > b else NEGATIVE_COLOR
              for p, c, b in zip(proxy, cagrs, benches)]

    fig, ax = plt.subplots(figsize=(12, max(6, len(legs) * 0.6)))
    y = list(range(len(legs)))
    ax.barh(y, cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, y, marker="|", s=500, linewidths=3, color="black", zorder=3)

    # Label bars
    for i, ((exch, _), c, b, e) in enumerate(zip(legs, cagrs, benches, excesses)):
        ax.text(max(c, b, 0) + 0.3, i, f"{c:.2f}% ({e:+.2f} vs {benchmark_legend(all_data, exch)})",
                va="center", fontsize=9)

    ax.set_yticks(y)
    ax.set_yticklabels(exchanges)
    ax.set_title(f"Cyclical Sector Timing: CAGR vs Own Benchmark by Exchange ({window_of(legs)})",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12)
    ax.set_xlim(right=max(cagrs + benches) + 9)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend(handles=[
        Patch(color=POSITIVE_COLOR, alpha=0.85, label="Beat its own benchmark"),
        Patch(color=NEGATIVE_COLOR, alpha=0.85, label="Trailed its own benchmark"),
        Patch(color=BENCHMARK_COLOR, alpha=0.85, label="No local index (vs S&P 500, USD)"),
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark CAGR"),
    ], fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, linestyle=":", axis="x")
    ax.invert_yaxis()

    beat = sum(c > b for p, c, b in zip(proxy, cagrs, benches) if not p)
    note = f"{beat} of {proxy.count(False)} beat their own benchmark."
    grey = [x for x, p in zip(exchanges, proxy) if p]
    if grey:
        note += f" {', '.join(grey)}: no local index, measured against the S&P 500 in USD, not counted."
    if dropped:
        note += f" Excluded (truncated window or never invested): {', '.join(dropped)}."
    fig.text(0.5, -0.02,
             "Data: Ceta Research | Returns in local currency, each against its own market's index\n" + note,
             ha="center", fontsize=8, color="gray")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_comparison_drawdown(all_data, output_path):
    """Plot max drawdown comparison across exchanges (no benchmark line: each has its own)."""
    legs, _ = comparable_legs(all_data, "max_drawdown")
    if not legs:
        return
    legs.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])
    exchanges = [exch.replace("_", "+") for exch, _ in legs]
    drawdowns = [d["portfolio"]["max_drawdown"] for _, d in legs]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(exchanges, drawdowns, color=NEGATIVE_COLOR, alpha=0.75)

    ax.set_title(f"Cyclical Sector Timing: Max Drawdown by Exchange ({window_of(legs)})",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12)
    ax.set_xlim(left=min(drawdowns) - 8)  # room for the deepest bar's label
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

    # Per-exchange charts come from returns_{key}.json; exchange_comparison.json
    # is rebuilt from them, so both carry each market's own benchmark.
    for exch_key, (region_slug, label) in EXCHANGE_LABELS.items():
        path = os.path.join(RESULTS_DIR, f"returns_{exch_key}.json")
        data = None
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
        if not data or "error" in data or not data.get("portfolio"):
            print(f"  Skipping {exch_key} (no results)")
            continue
        bench = benchmark_label({exch_key: data}, exch_key)

        print(f"\n  {label}")
        plot_cumulative(
            data, label,
            os.path.join(CHARTS_DIR, f"1_{region_slug}_cumulative_growth.png"),
            exch_key, bench
        )
        plot_annual_returns(
            data, label,
            os.path.join(CHARTS_DIR, f"2_{region_slug}_annual_returns.png"),
            bench
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
