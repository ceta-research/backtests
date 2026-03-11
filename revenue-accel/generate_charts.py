#!/usr/bin/env python3
"""
Revenue Acceleration Growth - Chart Generator

Generates standard charts from backtest results:
  - Cumulative growth (strategy vs SPY) per exchange
  - Annual returns bar chart per exchange
  - CAGR comparison across exchanges
  - Max drawdown comparison

Run after backtest.py --global:
    cd backtests
    python3 revenue-accel/generate_charts.py

Charts saved to revenue-accel/charts/. Move to ts-content-creator after generation.
"""

import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import numpy as np
except ImportError:
    print("matplotlib not installed. Run: pip install matplotlib")
    sys.exit(1)

# ---- Config ----
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "results", "exchange_comparison.json")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "charts")
STRATEGY_NAME = "Revenue Acceleration"

COLORS = {
    "strategy": "#1a73e8",   # Blue
    "spy": "#e8711a",        # Orange
    "positive": "#2e7d32",   # Green
    "negative": "#c62828",   # Red
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "United States",
    "BSE_NSE": "India",
    "JPX": "Japan",
    "LSE": "United Kingdom",
    "SHZ_SHH": "China",
    "HKSE": "Hong Kong",
    "KSC": "South Korea",
    "TAI_TWO": "Taiwan",
    "XETRA": "Germany",
    "TSX": "Canada",
    "SET": "Thailand",
    "STO": "Sweden",
    "SIX": "Switzerland",
    "SES": "Singapore",
    "JNB": "South Africa",
}

REGION_SLUGS = {
    "NYSE_NASDAQ_AMEX": "us",
    "BSE_NSE": "india",
    "JPX": "japan",
    "LSE": "uk",
    "SHZ_SHH": "china",
    "HKSE": "hongkong",
    "KSC": "korea",
    "TAI_TWO": "taiwan",
    "XETRA": "germany",
    "TSX": "canada",
    "SET": "thailand",
    "STO": "sweden",
    "SIX": "switzerland",
    "SES": "singapore",
    "JNB": "southafrica",
}


def load_results():
    if not os.path.exists(RESULTS_FILE):
        print(f"Results file not found: {RESULTS_FILE}")
        print("Run backtest.py --global first.")
        sys.exit(1)
    with open(RESULTS_FILE) as f:
        return json.load(f)


def plot_cumulative_growth(exchange_key, data, label, filename):
    """Plot cumulative growth of strategy vs SPY."""
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [ar["year"] for ar in annual]
    port_cum = [1.0]
    spy_cum = [1.0]
    for ar in annual:
        port_cum.append(port_cum[-1] * (1 + ar["portfolio"] / 100))
        spy_cum.append(spy_cum[-1] * (1 + ar["spy"] / 100))

    x_labels = [str(years[0] - 1)] + [str(y) for y in years]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x_labels, port_cum, color=COLORS["strategy"], linewidth=2.5,
            label=f"{STRATEGY_NAME} ({data['portfolio']['cagr']}% CAGR)")
    ax.plot(x_labels, spy_cum, color=COLORS["spy"], linewidth=2, linestyle="--",
            label=f"S&P 500 ({data['spy']['cagr']}% CAGR)")

    ax.set_title(f"{STRATEGY_NAME} vs S&P 500 — {label}",
                 fontsize=14, fontweight="bold", pad=15)
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Portfolio Value ($1 invested)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.1f}"))
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    p = data.get("portfolio", {})
    c = data.get("comparison", {})
    excess = c.get("excess_cagr", 0)
    excess_str = f"+{excess:.2f}%" if excess and excess > 0 else f"{excess:.2f}%"
    note = (f"Excess CAGR: {excess_str}  |  "
            f"Sharpe: {p.get('sharpe_ratio', 'N/A')}  |  "
            f"Max DD: {p.get('max_drawdown', 'N/A')}%")
    ax.text(0.02, 0.05, note, transform=ax.transAxes, fontsize=9,
            color="#555555", va="bottom")

    tick_positions = [i for i, yr in enumerate(x_labels) if int(yr) % 5 == 0 or i == 0]
    ax.set_xticks([x_labels[i] for i in tick_positions])
    ax.set_xticklabels([x_labels[i] for i in tick_positions], rotation=0)

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def plot_annual_returns(exchange_key, data, label, filename):
    """Plot annual returns bar chart."""
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = range(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width/2 for i in x], port_rets, width,
           color=[COLORS["positive"] if r >= 0 else COLORS["negative"] for r in port_rets],
           alpha=0.85, label=STRATEGY_NAME)
    ax.bar([i + width/2 for i in x], spy_rets, width,
           color=COLORS["spy"], alpha=0.5, label="S&P 500")

    ax.axhline(y=0, color="black", linewidth=0.8, alpha=0.5)
    ax.set_title(f"Annual Returns — {STRATEGY_NAME} vs S&P 500 ({label})",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def plot_comparison_cagr(all_data, filename="1_comparison_cagr.png"):
    """CAGR comparison bar chart across all exchanges."""
    labels = []
    port_cagrs = []
    spy_cagrs = []

    for key, data in all_data.items():
        if "error" in data or not data.get("portfolio"):
            continue
        label = EXCHANGE_LABELS.get(key, key)
        labels.append(label)
        port_cagrs.append(data["portfolio"]["cagr"])
        spy_cagrs.append(data["spy"]["cagr"])

    if not labels:
        return

    sorted_pairs = sorted(zip(labels, port_cagrs, spy_cagrs), key=lambda x: x[1], reverse=True)
    labels, port_cagrs, spy_cagrs = zip(*sorted_pairs)

    x = range(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar([i - width/2 for i in x], port_cagrs, width,
           color=[COLORS["positive"] if c > s else COLORS["negative"]
                  for c, s in zip(port_cagrs, spy_cagrs)],
           alpha=0.85, label=STRATEGY_NAME)
    ax.bar([i + width/2 for i in x], spy_cagrs, width,
           color=COLORS["spy"], alpha=0.5, label="Local Benchmark (SPY)")

    ax.set_title(f"{STRATEGY_NAME} CAGR — All Exchanges (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("CAGR (%)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def plot_comparison_drawdown(all_data, filename="2_comparison_drawdown.png"):
    """Max drawdown comparison across exchanges."""
    labels = []
    port_mdd = []
    spy_mdd = []

    for key, data in all_data.items():
        if "error" in data or not data.get("portfolio"):
            continue
        label = EXCHANGE_LABELS.get(key, key)
        labels.append(label)
        port_mdd.append(abs(data["portfolio"]["max_drawdown"]))
        spy_mdd.append(abs(data["spy"]["max_drawdown"]))

    if not labels:
        return

    sorted_pairs = sorted(zip(labels, port_mdd, spy_mdd), key=lambda x: x[1])
    labels, port_mdd, spy_mdd = zip(*sorted_pairs)

    x = range(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar([i - width/2 for i in x], port_mdd, width, color=COLORS["negative"],
           alpha=0.8, label=STRATEGY_NAME)
    ax.bar([i + width/2 for i in x], spy_mdd, width, color=COLORS["spy"],
           alpha=0.5, label="SPY")

    ax.set_title(f"Max Drawdown — {STRATEGY_NAME} vs SPY (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Max Drawdown (%, lower = better)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def main():
    print("Loading results...")
    all_data = load_results()
    print(f"  {len(all_data)} exchanges found\n")

    for exchange_key, data in all_data.items():
        if "error" in data or not data.get("portfolio"):
            print(f"  Skipping {exchange_key} (no data)")
            continue

        slug = REGION_SLUGS.get(exchange_key, exchange_key.lower())
        label = EXCHANGE_LABELS.get(exchange_key, exchange_key)

        print(f"  Generating charts for {label}...")
        plot_cumulative_growth(exchange_key, data, label,
                               f"1_{slug}_cumulative_growth.png")
        plot_annual_returns(exchange_key, data, label,
                            f"2_{slug}_annual_returns.png")

    print("\n  Generating comparison charts...")
    plot_comparison_cagr(all_data)
    plot_comparison_drawdown(all_data)

    print(f"\nDone. Charts saved to: {CHARTS_DIR}")
    print("\nNext step: move charts to ts-content-creator blog directories:")
    print("  mv charts/1_us_cumulative_growth.png "
          "../ts-content-creator/content/_current/growth-01-revenue-accel/blogs/us/")
    print("  # ... repeat for each exchange")


if __name__ == "__main__":
    main()
