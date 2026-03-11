#!/usr/bin/env python3
"""
Generate charts for 52-Week High Proximity strategy.

Creates cumulative growth and annual returns charts for each eligible exchange,
plus CAGR and max drawdown comparison charts.

Charts are saved to 52-week-high/charts/ and must be moved to:
  ts-content-creator/content/_current/momentum-02-52-week-high/blogs/{region}/

Usage:
    cd backtests
    python3 52-week-high/generate_charts.py
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

STRATEGY_COLOR = "#1565C0"    # Deep blue (momentum / direction)
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
    """Cumulative growth chart: 52W-High Proximity vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    port_curve = cumulative_growth(port_rets)
    spy_curve = cumulative_growth(spy_rets)

    x = [years[0] - 1] + years
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x, port_curve, color=STRATEGY_COLOR, linewidth=2.5,
            label=f"52W-High Proximity  (CAGR: {data['portfolio']['cagr']:.1f}%)")
    ax.plot(x, spy_curve, color=BENCHMARK_COLOR, linewidth=1.8, linestyle="--",
            label=f"S&P 500 (SPY)  (CAGR: {data['spy']['cagr']:.1f}%)")

    ax.set_title(f"52-Week High Proximity vs S&P 500\n{label}",
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
        f"Excess CAGR: {excess:+.1f}%  |  Down Capture: {down_capture:.0f}%  |  Cash: {cash_pct:.0f}%"
        if down_capture is not None and excess is not None else ""
    )
    if info_text:
        ax.text(0.01, 0.01, info_text, transform=ax.transAxes,
                fontsize=9, color="#555555",
                verticalalignment="bottom", horizontalalignment="left")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_annual(data, label, output_path):
    """Annual returns bar chart: 52W-High Proximity vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = np.arange(len(years))
    width = 0.4

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x - width / 2, port_rets, width, label="52W-High Proximity",
           color=STRATEGY_COLOR, alpha=0.85)
    ax.bar(x + width / 2, spy_rets, width, label="SPY", color=BENCHMARK_COLOR, alpha=0.7)

    ax.axhline(0, color="black", linewidth=0.8, linestyle="-")
    ax.set_title(f"Annual Returns: 52-Week High Proximity vs S&P 500\n{label}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Annual Return (%)", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":", axis="y")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_comparison_cagr(all_data, output_path):
    """CAGR comparison bar chart across all exchanges."""
    exchanges = []
    port_cagrs = []
    spy_cagrs = []

    for uni, data in sorted(all_data.items(),
                             key=lambda x: (x[1].get("portfolio") or {}).get("cagr") or -999,
                             reverse=True):
        if "error" in data or not data.get("portfolio"):
            continue
        exchanges.append(uni.replace("_", "/"))
        port_cagrs.append(data["portfolio"].get("cagr") or 0)
        spy_cagrs.append(data["spy"].get("cagr") or 0)

    if not exchanges:
        return

    x = np.arange(len(exchanges))
    width = 0.4

    fig, ax = plt.subplots(figsize=(max(12, len(exchanges) * 0.8), 7))
    bars = ax.bar(x - width / 2, port_cagrs, width, label="52W-High Proximity",
                  color=STRATEGY_COLOR, alpha=0.85)
    ax.bar(x + width / 2, spy_cagrs, width, label="SPY (local benchmark)",
           color=BENCHMARK_COLOR, alpha=0.7)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("52-Week High Proximity: CAGR by Exchange vs SPY",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Exchange", fontsize=12)
    ax.set_ylabel("CAGR (%)", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(exchanges, rotation=30, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":", axis="y")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_comparison_drawdown(all_data, output_path):
    """Max drawdown comparison across all exchanges."""
    exchanges = []
    drawdowns = []

    for uni, data in sorted(all_data.items(),
                             key=lambda x: (x[1].get("portfolio") or {}).get("max_drawdown") or 0,
                             reverse=False):
        if "error" in data or not data.get("portfolio"):
            continue
        dd = data["portfolio"].get("max_drawdown")
        if dd is None:
            continue
        exchanges.append(uni.replace("_", "/"))
        drawdowns.append(dd)

    if not exchanges:
        return

    colors = [NEGATIVE_COLOR if d < -30 else STRATEGY_COLOR for d in drawdowns]

    fig, ax = plt.subplots(figsize=(max(12, len(exchanges) * 0.8), 6))
    ax.bar(range(len(exchanges)), drawdowns, color=colors, alpha=0.85)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("52-Week High Proximity: Max Drawdown by Exchange",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Exchange", fontsize=12)
    ax.set_ylabel("Max Drawdown (%)", fontsize=12)
    ax.set_xticks(range(len(exchanges)))
    ax.set_xticklabels(exchanges, rotation=30, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.grid(True, alpha=0.3, linestyle=":", axis="y")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


# Exchange-to-display mapping
EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": ("us", "US (NYSE/NASDAQ/AMEX)"),
    "BSE_NSE": ("india", "India (BSE/NSE)"),
    "LSE": ("uk", "UK (LSE)"),
    "XETRA": ("germany", "Germany (XETRA)"),
    "JPX": ("japan", "Japan (JPX)"),
    "SHZ_SHH": ("china", "China (SHZ/SHH)"),
    "HKSE": ("hongkong", "Hong Kong (HKSE)"),
    "KSC": ("korea", "Korea (KSC)"),
    "TAI": ("taiwan", "Taiwan (TAI)"),
    "TSX": ("canada", "Canada (TSX)"),
    "SIX": ("switzerland", "Switzerland (SIX)"),
    "STO": ("sweden", "Sweden (STO)"),
    "SET": ("thailand", "Thailand (SET)"),
    "JNB": ("southafrica", "South Africa (JNB)"),
    "OSL": ("norway", "Norway (OSL)"),
    "MIL": ("italy", "Italy (MIL)"),
    "KLS": ("malaysia", "Malaysia (KLS)"),
    "SES": ("singapore", "Singapore (SES)"),
}


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # Load global results
    comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(comparison_path):
        print(f"No exchange_comparison.json found at {comparison_path}")
        print("Run backtest with --global first.")
        return

    with open(comparison_path) as f:
        all_data = json.load(f)

    print(f"Generating charts for {len(all_data)} exchanges...")

    # Per-exchange charts
    for uni_key, (region_slug, region_label) in EXCHANGE_LABELS.items():
        data = all_data.get(uni_key)
        if not data or "error" in data or not data.get("annual_returns"):
            print(f"  Skipping {uni_key}: no data or error")
            continue

        print(f"\n{region_label}")
        plot_cumulative(data, region_label,
                        os.path.join(CHARTS_DIR, f"1_{region_slug}_cumulative_growth.png"))
        plot_annual(data, region_label,
                    os.path.join(CHARTS_DIR, f"2_{region_slug}_annual_returns.png"))

    # Comparison charts
    print("\nComparison charts")
    plot_comparison_cagr(all_data,
                         os.path.join(CHARTS_DIR, "1_comparison_cagr.png"))
    plot_comparison_drawdown(all_data,
                              os.path.join(CHARTS_DIR, "2_comparison_drawdown.png"))

    print(f"\nAll charts saved to {CHARTS_DIR}/")
    print("\nNext step: move charts to ts-content-creator/content/_current/momentum-02-52-week-high/blogs/")
    print("  mv charts/1_us_cumulative_growth.png ../../ts-content-creator/content/_current/momentum-02-52-week-high/blogs/us/")
    print("  (repeat for each region)")


if __name__ == "__main__":
    main()
