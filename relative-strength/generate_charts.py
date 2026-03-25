#!/usr/bin/env python3
"""
Generate charts for Sector-Adjusted Momentum (Relative Strength) strategy.

Creates cumulative growth and annual returns charts for each eligible exchange,
plus CAGR and max drawdown comparison charts.

Charts are saved to relative-strength/charts/ and must be moved to:
  ts-content-creator/content/_current/momentum-07-relative-strength/blogs/{region}/

Usage:
    cd backtests
    python3 relative-strength/generate_charts.py
"""

import json
import os
import sys

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

STRATEGY_COLOR = "#1565C0"    # Deep blue (precision, signal clarity)
BENCHMARK_COLOR = "#9E9E9E"   # Gray
POSITIVE_COLOR = "#43A047"    # Green
NEGATIVE_COLOR = "#E53935"    # Red

# Exchange label map (key = JSON key used in exchange_comparison.json)
EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": ("US (NYSE + NASDAQ + AMEX)", "us"),
    "NSE":          ("India (NSE)", "india"),
    "LSE":              ("UK (LSE)", "uk"),
    "XETRA":            ("Germany (XETRA)", "germany"),
    "JPX":              ("Japan (JPX)", "japan"),
    "SHZ_SHH":          ("China (SHZ + SHH)", "china"),
    "HKSE":             ("Hong Kong (HKSE)", "hongkong"),
    "KSC":              ("Korea (KSC)", "korea"),
    "TAI":              ("Taiwan (TAI)", "taiwan"),
    "TSX":              ("Canada (TSX)", "canada"),
    "SIX":              ("Switzerland (SIX)", "switzerland"),
    "STO":              ("Sweden (STO)", "sweden"),
    "SET":              ("Thailand (SET)", "thailand"),
    "JNB":              ("South Africa (JNB)", "southafrica"),
    "OSL":              ("Norway (OSL)", "norway"),
    "MIL":              ("Italy (MIL)", "italy"),
    "SES":              ("Singapore (SES)", "singapore"),
}


def cumulative_growth(returns):
    """Compute cumulative growth index starting at 1.0."""
    curve = [1.0]
    for r in returns:
        curve.append(curve[-1] * (1 + r / 100))
    return curve


def plot_cumulative(data, label, output_path):
    """Cumulative growth chart: Relative Strength vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    port_curve = cumulative_growth(port_rets)
    spy_curve = cumulative_growth(spy_rets)

    x = [years[0] - 1] + years
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x, port_curve, color=STRATEGY_COLOR, linewidth=2.5,
            label=f"Relative Strength  (CAGR: {data['portfolio']['cagr']:.1f}%)")
    ax.plot(x, spy_curve, color=BENCHMARK_COLOR, linewidth=1.8, linestyle="--",
            label=f"S&P 500 (SPY)  (CAGR: {data['spy']['cagr']:.1f}%)")

    ax.set_title(f"Sector-Adjusted Momentum vs S&P 500\n{label}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Portfolio Value ($1 Start)", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:.1f}"))
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":")
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)

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


def plot_annual_returns(data, label, output_path):
    """Annual returns bar chart: Relative Strength vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = np.arange(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width / 2, port_rets, width,
           color=[POSITIVE_COLOR if r >= 0 else NEGATIVE_COLOR for r in port_rets],
           alpha=0.85, label="Relative Strength")
    ax.bar(x + width / 2, spy_rets, width,
           color=BENCHMARK_COLOR, alpha=0.6, label="S&P 500")

    ax.set_title(f"Annual Returns: Sector-Adjusted Momentum vs S&P 500\n{label}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Annual Return (%)", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":", axis="y")
    ax.axhline(y=0, color="black", linewidth=0.8)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_cagr_comparison(all_data, output_path):
    """Horizontal bar chart comparing CAGR across all exchanges."""
    items = []
    for key, data in all_data.items():
        if "error" in data or not data.get("portfolio"):
            continue
        label, _ = EXCHANGE_LABELS.get(key, (key, key.lower()))
        cagr = data["portfolio"].get("cagr")
        spy_cagr = data["spy"].get("cagr")
        excess = data["comparison"].get("excess_cagr")
        if cagr is not None:
            items.append((label, cagr, spy_cagr or 0, excess or 0))

    if not items:
        print("  No data for CAGR comparison chart.")
        return

    items.sort(key=lambda x: x[1], reverse=True)
    labels = [i[0] for i in items]
    strategy_cagrs = [i[1] for i in items]
    spy_cagrs = [i[2] for i in items]
    excesses = [i[3] for i in items]

    y = np.arange(len(labels))
    height = 0.35

    fig, ax = plt.subplots(figsize=(11, max(6, len(labels) * 0.55)))
    bars1 = ax.barh(y + height / 2, strategy_cagrs, height,
                    color=STRATEGY_COLOR, alpha=0.85, label="Relative Strength")
    bars2 = ax.barh(y - height / 2, spy_cagrs, height,
                    color=BENCHMARK_COLOR, alpha=0.65, label="Local Benchmark (SPY)")

    # Annotate excess CAGR
    for i, (bar, exc) in enumerate(zip(bars1, excesses)):
        color = POSITIVE_COLOR if exc >= 0 else NEGATIVE_COLOR
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
                f"{exc:+.1f}%", va="center", ha="left", fontsize=8, color=color)

    ax.set_title("Sector-Adjusted Momentum: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("CAGR (%)", fontsize=12)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":", axis="x")
    ax.axvline(x=0, color="black", linewidth=0.8)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_drawdown_comparison(all_data, output_path):
    """Bar chart comparing max drawdown across all exchanges."""
    items = []
    for key, data in all_data.items():
        if "error" in data or not data.get("portfolio"):
            continue
        label, _ = EXCHANGE_LABELS.get(key, (key, key.lower()))
        maxdd = data["portfolio"].get("max_drawdown")
        spy_dd = data["spy"].get("max_drawdown")
        if maxdd is not None:
            items.append((label, maxdd, spy_dd or 0))

    if not items:
        print("  No data for drawdown comparison chart.")
        return

    items.sort(key=lambda x: x[1])  # Least negative first
    labels = [i[0] for i in items]
    strat_dds = [i[1] for i in items]
    spy_dds = [i[2] for i in items]

    y = np.arange(len(labels))
    height = 0.35

    fig, ax = plt.subplots(figsize=(11, max(6, len(labels) * 0.55)))
    ax.barh(y + height / 2, strat_dds, height,
            color=NEGATIVE_COLOR, alpha=0.75, label="Relative Strength")
    ax.barh(y - height / 2, spy_dds, height,
            color=BENCHMARK_COLOR, alpha=0.5, label="SPY")

    ax.set_title("Sector-Adjusted Momentum: Max Drawdown by Exchange",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":", axis="x")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # Load exchange comparison results
    comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(comparison_path):
        print(f"Error: {comparison_path} not found. Run backtest.py --global first.")
        sys.exit(1)

    with open(comparison_path) as f:
        all_data = json.load(f)

    print(f"Loaded results for {len(all_data)} exchanges.")
    print(f"Charts will be saved to: {CHARTS_DIR}\n")

    # Per-exchange charts
    for key, data in all_data.items():
        if "error" in data or not data.get("annual_returns"):
            print(f"  Skipping {key}: no data or error")
            continue

        label, slug = EXCHANGE_LABELS.get(key, (key, key.lower().replace("_", "")))
        print(f"Generating charts for {key} ({label})...")

        cum_path = os.path.join(CHARTS_DIR, f"1_{slug}_cumulative_growth.png")
        bar_path = os.path.join(CHARTS_DIR, f"2_{slug}_annual_returns.png")

        plot_cumulative(data, label, cum_path)
        plot_annual_returns(data, label, bar_path)

    print("\nGenerating comparison charts...")

    cagr_path = os.path.join(CHARTS_DIR, "1_comparison_cagr.png")
    dd_path = os.path.join(CHARTS_DIR, "2_comparison_drawdown.png")

    plot_cagr_comparison(all_data, cagr_path)
    plot_drawdown_comparison(all_data, dd_path)

    print(f"\nDone. Move charts from {CHARTS_DIR}/ to the matching blog directories:")
    print("  ts-content-creator/content/_current/momentum-07-relative-strength/blogs/{{region}}/")
    print("\nRemember: use 'mv' not 'cp' to avoid orphan files in the backtests repo.")


if __name__ == "__main__":
    main()
