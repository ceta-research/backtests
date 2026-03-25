#!/usr/bin/env python3
"""
Generate charts for Quality Momentum strategy.

Creates cumulative growth and annual returns charts for each eligible exchange,
plus CAGR and max drawdown comparison charts.

Charts are saved to quality-momentum/charts/ and must be moved to:
  ts-content-creator/content/_current/factor-04-quality-momentum/blogs/{region}/

Usage:
    cd backtests
    python3 quality-momentum/generate_charts.py
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

STRATEGY_COLOR = "#1976D2"    # Blue (quality/professional)
BENCHMARK_COLOR = "#9E9E9E"   # Gray
POSITIVE_COLOR = "#43A047"    # Green
NEGATIVE_COLOR = "#E53935"    # Red


def cumulative_growth(returns):
    """Compute cumulative growth index starting at 1.0."""
    curve = [1.0]
    for r in returns:
        curve.append(curve[-1] * (1 + r / 100))
    return curve


def plot_cumulative(data, label, output_path):
    """Cumulative growth chart: Quality Momentum vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    port_curve = cumulative_growth(port_rets)
    spy_curve = cumulative_growth(spy_rets)

    x = [years[0] - 1] + years
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x, port_curve, color=STRATEGY_COLOR, linewidth=2.5,
            label=f"Quality Momentum  (CAGR: {data['portfolio']['cagr']:.1f}%)")
    ax.plot(x, spy_curve, color=BENCHMARK_COLOR, linewidth=1.8, linestyle="--",
            label=f"S&P 500 (SPY)  (CAGR: {data['spy']['cagr']:.1f}%)")

    ax.set_title(f"Quality Momentum vs S&P 500\n{label}",
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
    """Annual returns bar chart: Quality Momentum vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = np.arange(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width / 2, port_rets, width,
           color=[POSITIVE_COLOR if r >= 0 else NEGATIVE_COLOR for r in port_rets],
           alpha=0.85, label="Quality Momentum")
    ax.bar(x + width / 2, spy_rets, width,
           color=BENCHMARK_COLOR, alpha=0.6, label="S&P 500")

    ax.set_title(f"Annual Returns: Quality Momentum vs S&P 500\n{label}",
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
    """CAGR comparison bar chart across all exchanges."""
    exchanges = []
    cagrs = []
    spy_cagr = None

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

    for bar, val in zip(bars, cagrs):
        ax.text(val + 0.1, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", fontsize=9)

    ax.set_title("Quality Momentum: CAGR by Exchange (2001–2024)",
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
    """Max drawdown comparison chart across all exchanges."""
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

    ax.set_title("Quality Momentum: Max Drawdown by Exchange (2001–2024)",
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


# Maps exchange_comparison.json keys → (region_slug, human label)
# Add/remove exchanges here based on backtest results
EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": ("us",          "United States (NYSE+NASDAQ+AMEX)"),
    "NSE":          ("india",       "India (NSE)"),
    "LSE":              ("uk",          "United Kingdom (LSE)"),
    "XETRA":            ("germany",     "Germany (XETRA)"),
    "JPX":              ("japan",       "Japan (JPX)"),
    "SHZ_SHH":          ("china",       "China (SHZ+SHH)"),
    "HKSE":             ("hongkong",    "Hong Kong (HKSE)"),
    "KSC":              ("korea",       "Korea (KSC)"),
    "TAI_TWO":          ("taiwan",      "Taiwan (TAI+TWO)"),
    "TSX":              ("canada",      "Canada (TSX)"),
    "SIX":              ("switzerland", "Switzerland (SIX)"),
    "STO":              ("sweden",      "Sweden (STO)"),
    "SET":              ("thailand",    "Thailand (SET)"),
    "JNB":              ("southafrica", "South Africa (JNB)"),
    "OSL":              ("norway",      "Norway (OSL)"),
    "MIL":              ("italy",       "Italy (MIL)"),
    "KLS":              ("malaysia",    "Malaysia (KLS)"),
}


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(comparison_path):
        print(f"Error: {comparison_path} not found. Run backtest --global first.")
        sys.exit(1)

    with open(comparison_path) as f:
        all_data = json.load(f)

    print("Generating Quality Momentum charts...")

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
    plot_comparison_cagr(all_data, os.path.join(CHARTS_DIR, "1_comparison_cagr.png"))
    plot_comparison_drawdown(all_data, os.path.join(CHARTS_DIR, "2_comparison_drawdown.png"))

    print(f"\nAll charts saved to {CHARTS_DIR}/")
    print("\nNext: Move charts to:")
    print("  ts-content-creator/content/_current/factor-04-quality-momentum/blogs/{region}/")


if __name__ == "__main__":
    main()
