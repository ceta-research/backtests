#!/usr/bin/env python3
"""
Generate charts for Z-Score Pairs Trading strategy.

Creates cumulative growth and annual returns charts for the US exchange,
plus CAGR comparison and convergence vs profitability charts.

Charts are saved to pairs-zscore/charts/ and must be moved to:
  ts-content-creator/content/_current/pairs-04-zscore/blogs/{region}/

Usage:
    cd backtests
    python3 pairs-zscore/generate_charts.py
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

STRATEGY_COLOR = "#7B1FA2"    # Purple (pairs/arbitrage)
BENCHMARK_COLOR = "#9E9E9E"   # Gray
POSITIVE_COLOR = "#43A047"    # Green
NEGATIVE_COLOR = "#E53935"    # Red

# Clean exchanges only (excluded: JNB, BSE_NSE, KSC, STO, SHZ_SHH)
CONTENT_EXCHANGES = {
    "NYSE_NASDAQ_AMEX": ("us",       "United States (NYSE+NASDAQ+AMEX)"),
    "JPX":              ("japan",    "Japan (JPX)"),
    "LSE":              ("uk",       "United Kingdom (LSE)"),
    "HKSE":             ("hongkong", "Hong Kong (HKSE)"),
    "TAI_TWO":          ("taiwan",   "Taiwan (TAI+TWO)"),
    "XETRA":            ("germany",  "Germany (XETRA)"),
    "TSX":              ("canada",   "Canada (TSX)"),
}


def cumulative_growth(returns):
    """Compute cumulative growth index starting at 1.0."""
    curve = [1.0]
    for r in returns:
        curve.append(curve[-1] * (1 + r / 100))
    return curve


def plot_cumulative(data, label, output_path):
    """Cumulative growth chart: Z-Score Pairs vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    port_curve = cumulative_growth(port_rets)
    spy_curve = cumulative_growth(spy_rets)

    x = [years[0] - 1] + years
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x, port_curve, color=STRATEGY_COLOR, linewidth=2.5,
            label=f"Z-Score Pairs  (CAGR: {data['portfolio']['cagr']:.2f}%)")
    ax.plot(x, spy_curve, color=BENCHMARK_COLOR, linewidth=1.8, linestyle="--",
            label=f"S&P 500 (SPY)  (CAGR: {data['spy']['cagr']:.1f}%)")

    ax.set_title(f"Z-Score Pairs Trading vs S&P 500\n{label}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Portfolio Value ($1 Start)", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v:.2f}"))
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, linestyle=":")
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)

    ts = data.get("trade_stats", {})
    conv_rate = ts.get("convergence_rate", 0)
    avg_trade = ts.get("avg_trade_return_pct", 0)
    n = data.get("n_years", 20)
    cash_pct = round(data.get("cash_periods", 0) * 100 / n, 0) if n > 0 else 0

    info_text = (
        f"Max Drawdown: {data['portfolio']['max_drawdown']:.1f}%\n"
        f"Convergence Rate: {conv_rate:.1f}%\n"
        f"Avg Trade Return: {avg_trade:+.3f}%\n"
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
    """Annual returns bar chart: Z-Score Pairs vs SPY."""
    annual = data["annual_returns"]
    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = np.arange(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width / 2, port_rets, width,
           color=[POSITIVE_COLOR if r >= 0 else NEGATIVE_COLOR for r in port_rets],
           alpha=0.85, label="Z-Score Pairs")
    ax.bar(x + width / 2, spy_rets, width,
           color=BENCHMARK_COLOR, alpha=0.6, label="S&P 500")

    ax.set_title(f"Annual Returns: Z-Score Pairs vs S&P 500\n{label}",
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
    """CAGR comparison bar chart across clean exchanges."""
    exchanges = []
    cagrs = []
    spy_cagr = None

    # Sort by CAGR descending (best to worst)
    ordered = []
    for exch_key, (_, label) in CONTENT_EXCHANGES.items():
        data = all_data.get(exch_key)
        if not data or "error" in data or not data.get("portfolio"):
            continue
        cagr = data["portfolio"].get("cagr")
        if cagr is not None:
            ordered.append((label.replace("(", "").replace(")", ""), cagr))
            if spy_cagr is None:
                spy_cagr = data["spy"].get("cagr")

    ordered.sort(key=lambda x: x[1], reverse=True)
    exchanges = [o[0] for o in ordered]
    cagrs = [o[1] for o in ordered]

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(12, 5))
    colors = [POSITIVE_COLOR if c >= 0 else NEGATIVE_COLOR for c in cagrs]
    bars = ax.barh(exchanges, cagrs, color=colors, alpha=0.85)

    if spy_cagr:
        ax.axvline(spy_cagr, color=BENCHMARK_COLOR, linewidth=2, linestyle="--",
                   label=f"S&P 500 ({spy_cagr:.1f}%)")
        ax.legend(fontsize=11)

    ax.axvline(0, color="black", linewidth=0.8)

    for bar, val in zip(bars, cagrs):
        offset = 0.05 if val >= 0 else -0.05
        ha = "left" if val >= 0 else "right"
        ax.text(val + offset, bar.get_y() + bar.get_height() / 2,
                f"{val:.2f}%", va="center", ha=ha, fontsize=9)

    ax.set_title("Z-Score Pairs Trading: CAGR by Exchange (2005-2024)",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("CAGR (%)", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.grid(True, alpha=0.3, linestyle=":", axis="x")
    ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def plot_convergence_vs_profit(all_data, output_path):
    """
    Special chart: convergence rate vs avg trade return per exchange.
    Core narrative: high convergence does NOT mean profitable.
    """
    exchanges = []
    conv_rates = []
    avg_trades = []

    for exch_key, (_, label) in CONTENT_EXCHANGES.items():
        data = all_data.get(exch_key)
        if not data or "error" in data or not data.get("trade_stats"):
            continue
        ts = data["trade_stats"]
        exchanges.append(label.split(" ")[0])  # Short name
        conv_rates.append(ts.get("convergence_rate", 0))
        avg_trades.append(ts.get("avg_trade_return_pct", 0))

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(9, 6))

    colors = [POSITIVE_COLOR if t >= 0 else NEGATIVE_COLOR for t in avg_trades]
    scatter = ax.scatter(conv_rates, avg_trades, c=colors, s=120, zorder=5, alpha=0.9)

    for i, exch in enumerate(exchanges):
        ax.annotate(exch, (conv_rates[i], avg_trades[i]),
                    textcoords="offset points", xytext=(6, 4),
                    fontsize=9, color="black")

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)

    ax.set_title("Convergence Rate vs Avg Trade Return by Exchange\n"
                 "High convergence does not equal profit",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel("Convergence Rate (%)", fontsize=12)
    ax.set_ylabel("Avg Trade Return (%)", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.grid(True, alpha=0.3, linestyle=":")

    # Add annotation box explaining the key insight
    ax.text(0.02, 0.05,
            "All exchanges: 77-87% convergence\nAll exchanges: negative or near-zero CAGR",
            transform=ax.transAxes, fontsize=9,
            verticalalignment="bottom", fontfamily="monospace",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="#FFF9C4",
                      edgecolor="orange", alpha=0.9))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    comparison_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(comparison_path):
        print(f"Error: {comparison_path} not found. Run backtest --global first.")
        sys.exit(1)

    with open(comparison_path) as f:
        all_data = json.load(f)

    print("Generating Z-Score Pairs Trading charts...")

    # Per-exchange: US only for individual charts (core exchange for blog)
    us_data = all_data.get("NYSE_NASDAQ_AMEX")
    if us_data and "portfolio" in us_data:
        print("\n  United States (NYSE+NASDAQ+AMEX)")
        plot_cumulative(
            us_data,
            "United States (NYSE+NASDAQ+AMEX), 2005-2024",
            os.path.join(CHARTS_DIR, "1_us_cumulative_growth.png")
        )
        plot_annual_returns(
            us_data,
            "United States (NYSE+NASDAQ+AMEX), 2005-2024",
            os.path.join(CHARTS_DIR, "2_us_annual_returns.png")
        )
    else:
        print("  Skipping US (no results)")

    # Comparison charts
    print("\n  Comparison charts")
    plot_comparison_cagr(
        all_data,
        os.path.join(CHARTS_DIR, "1_comparison_cagr.png")
    )
    plot_convergence_vs_profit(
        all_data,
        os.path.join(CHARTS_DIR, "2_comparison_convergence_vs_profit.png")
    )

    print(f"\nAll charts saved to {CHARTS_DIR}/")
    print("\nNext: Move charts to content directory:")
    print("  cp pairs-zscore/charts/1_us_cumulative_growth.png \\")
    print("     ../ts-content-creator/content/_current/pairs-04-zscore/blogs/us/")
    print("  cp pairs-zscore/charts/2_us_annual_returns.png \\")
    print("     ../ts-content-creator/content/_current/pairs-04-zscore/blogs/us/")
    print("  cp pairs-zscore/charts/1_comparison_cagr.png \\")
    print("     ../ts-content-creator/content/_current/pairs-04-zscore/blogs/comparison/")
    print("  cp pairs-zscore/charts/2_comparison_convergence_vs_profit.png \\")
    print("     ../ts-content-creator/content/_current/pairs-04-zscore/blogs/comparison/")


if __name__ == "__main__":
    main()
