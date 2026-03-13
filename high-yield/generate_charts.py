#!/usr/bin/env python3
"""
Generate charts for High Dividend Yield Quality backtest results.

Reads results/exchange_comparison.json and generates:
- Cumulative growth charts (per exchange)
- Annual returns bar charts (per exchange)
- CAGR comparison across exchanges

Usage:
    cd backtests
    python3 high-yield/generate_charts.py
"""

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
CHARTS_DIR = os.path.join(SCRIPT_DIR, "charts")

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "US", "BSE_NSE": "India", "XETRA": "Germany",
    "STO": "Sweden", "TSX": "Canada", "SHZ_SHH": "China",
    "HKSE": "Hong Kong", "JPX": "Japan", "LSE": "UK", "ASX": "Australia",
    "KSC": "Korea", "SAO": "Brazil", "SIX": "Switzerland",
    "TAI": "Taiwan", "SET": "Thailand", "SGX": "Singapore",
    "SAU": "Saudi Arabia", "JNB": "South Africa",
}


def load_results():
    path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(path):
        print(f"Results not found at {path}")
        print("Run: python3 high-yield/backtest.py --global --output results/exchange_comparison.json")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def plot_cumulative(data, exchange_key, label):
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [a["year"] for a in annual]
    port_vals = [1.0]
    spy_vals = [1.0]
    for a in annual:
        port_vals.append(port_vals[-1] * (1 + a["portfolio"] / 100))
        spy_vals.append(spy_vals[-1] * (1 + a["spy"] / 100))

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(years + [years[-1] + 1], port_vals, "b-", linewidth=2, label="High Yield Quality")
    ax.plot(years + [years[-1] + 1], spy_vals, "r--", linewidth=1.5, label="S&P 500")
    ax.set_title(f"High Dividend Yield Quality: Cumulative Growth ({label})", fontsize=14)
    ax.set_xlabel("Year")
    ax.set_ylabel("Growth of $1")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.1f"))

    os.makedirs(CHARTS_DIR, exist_ok=True)
    slug = label.lower().replace(" ", "_")
    path = os.path.join(CHARTS_DIR, f"1_{slug}_cumulative_growth.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def plot_annual(data, exchange_key, label):
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [str(a["year"]) for a in annual]
    port_ret = [a["portfolio"] for a in annual]
    spy_ret = [a["spy"] for a in annual]

    x = np.arange(len(years))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width/2, port_ret, width, label="High Yield Quality", color="#2196F3")
    ax.bar(x + width/2, spy_ret, width, label="S&P 500", color="#FF5722", alpha=0.7)
    ax.set_title(f"High Dividend Yield Quality: Annual Returns ({label})", fontsize=14)
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    ax.axhline(y=0, color="black", linewidth=0.5)

    os.makedirs(CHARTS_DIR, exist_ok=True)
    slug = label.lower().replace(" ", "_")
    path = os.path.join(CHARTS_DIR, f"2_{slug}_annual_returns.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def plot_cagr_comparison(all_results):
    exchanges = []
    cagrs = []
    colors = []

    for key, data in sorted(all_results.items()):
        if "error" in data or not data.get("portfolio"):
            continue
        cagr = data["portfolio"].get("cagr")
        if cagr is None:
            continue
        label = EXCHANGE_LABELS.get(key, key)
        exchanges.append(label)
        cagrs.append(cagr)
        excess = (data.get("comparison") or {}).get("excess_cagr", 0) or 0
        colors.append("#4CAF50" if excess > 0 else "#F44336")

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(10, max(6, len(exchanges) * 0.4)))
    y_pos = np.arange(len(exchanges))
    ax.barh(y_pos, cagrs, color=colors, height=0.6)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("CAGR (%)")
    ax.set_title("High Dividend Yield Quality: CAGR by Exchange", fontsize=14)
    ax.grid(True, alpha=0.3, axis="x")

    for i, v in enumerate(cagrs):
        ax.text(v + 0.2, i, f"{v:.1f}%", va="center", fontsize=9)

    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, "3_comparison_cagr.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def main():
    print("Generating High Yield Quality charts...")
    all_results = load_results()

    for key, data in all_results.items():
        if "error" in data or not data.get("portfolio"):
            continue
        label = EXCHANGE_LABELS.get(key, key)
        print(f"\n  {label} ({key}):")
        plot_cumulative(data, key, label)
        plot_annual(data, key, label)

    print("\n  Comparison:")
    plot_cagr_comparison(all_results)
    print(f"\nAll charts saved to {CHARTS_DIR}/")


if __name__ == "__main__":
    main()
