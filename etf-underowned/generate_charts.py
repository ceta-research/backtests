#!/usr/bin/env python3
"""
Generate charts for ETF Underowned Quality backtest results.

Reads JSON results from results/ directory and generates:
- Cumulative growth charts (strategy vs SPY) per exchange
- Annual returns bar charts per exchange
- CAGR comparison chart across all exchanges
- Max drawdown comparison chart

Usage:
    python3 etf-underowned/generate_charts.py
"""

import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError:
    print("matplotlib required: pip install matplotlib")
    sys.exit(1)

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")

STRATEGY_NAME = "ETF Underowned Quality"
STRATEGY_COLOR = "#7B1FA2"   # Purple (distinct from crowding's blue)
SPY_COLOR = "#FF9800"
EXCESS_POS_COLOR = "#4CAF50"
EXCESS_NEG_COLOR = "#F44336"


def load_results(filename):
    path = os.path.join(RESULTS_DIR, filename)
    with open(path) as f:
        return json.load(f)


def cumulative_growth_chart(results, exchange_name, output_path):
    """Cumulative growth of $10,000."""
    annual = results.get("annual_returns", [])
    if not annual:
        return

    years = [ar["year"] for ar in annual]
    port_cum = [10000]
    spy_cum = [10000]
    for ar in annual:
        port_cum.append(port_cum[-1] * (1 + ar["portfolio"] / 100))
        spy_cum.append(spy_cum[-1] * (1 + ar["spy"] / 100))

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(range(len(port_cum)), port_cum, color=STRATEGY_COLOR, linewidth=2,
            label=f"{STRATEGY_NAME}")
    ax.plot(range(len(spy_cum)), spy_cum, color=SPY_COLOR, linewidth=2,
            label="S&P 500")

    x_labels = [str(years[0] - 1)] + [str(y) for y in years]
    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title(f"{STRATEGY_NAME}: Cumulative Growth of $10,000 ({exchange_name})",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Portfolio Value")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def annual_returns_chart(results, exchange_name, output_path):
    """Annual returns bar chart (strategy vs SPY)."""
    annual = results.get("annual_returns", [])
    if not annual:
        return

    years = [str(ar["year"]) for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = range(len(years))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar([i - width / 2 for i in x], port_rets, width, color=STRATEGY_COLOR,
           label=STRATEGY_NAME, alpha=0.85)
    ax.bar([i + width / 2 for i in x], spy_rets, width, color=SPY_COLOR,
           label="S&P 500", alpha=0.85)

    ax.set_xticks(list(x))
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.set_title(f"{STRATEGY_NAME}: Annual Returns ({exchange_name})",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Return (%)")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def comparison_cagr_chart(all_results, output_path):
    """CAGR comparison across all exchanges."""
    data = []
    for uni, r in all_results.items():
        if "error" in r or not r.get("portfolio"):
            continue
        cagr = r["portfolio"].get("cagr")
        excess = r["comparison"].get("excess_cagr")
        if cagr is not None:
            data.append((uni, cagr, excess or 0))

    data.sort(key=lambda x: x[1], reverse=True)
    names = [d[0] for d in data]
    cagrs = [d[1] for d in data]
    colors = [EXCESS_POS_COLOR if d[2] > 0 else EXCESS_NEG_COLOR for d in data]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85)

    for i, (name, cagr, excess) in enumerate(data):
        label = f" {cagr:.1f}% (ex: {excess:+.1f}%)"
        ax.text(max(cagr + 0.3, 0.5), i, label, va="center", fontsize=8)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.set_xlabel("CAGR (%)")
    ax.set_title(f"{STRATEGY_NAME}: CAGR by Exchange (2005-2025)",
                 fontsize=13, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="x")
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def comparison_drawdown_chart(all_results, output_path):
    """Max drawdown comparison across exchanges."""
    data = []
    for uni, r in all_results.items():
        if "error" in r or not r.get("portfolio"):
            continue
        maxdd = r["portfolio"].get("max_drawdown")
        if maxdd is not None:
            data.append((uni, maxdd))

    data.sort(key=lambda x: x[1])  # Most negative first
    names = [d[0] for d in data]
    drawdowns = [d[1] for d in data]

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(names)), drawdowns, color="#E53935", alpha=0.75)

    for i, (name, dd) in enumerate(data):
        ax.text(dd - 1, i, f"{dd:.1f}%", va="center", ha="right", fontsize=8, color="white")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.set_xlabel("Max Drawdown (%)")
    ax.set_title(f"{STRATEGY_NAME}: Max Drawdown by Exchange",
                 fontsize=13, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="x")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # Per-exchange charts - will be populated based on results
    exchange_map = {
        "returns_NYSE_NASDAQ.json": ("US (NYSE + NASDAQ)", "us"),
        "returns_BSE_NSE.json": ("India (BSE + NSE)", "india"),
        "returns_XETRA.json": ("Germany (XETRA)", "germany"),
        "returns_SHZ_SHH.json": ("China (SHZ + SHH)", "china"),
        "returns_HKSE.json": ("Hong Kong (HKSE)", "hongkong"),
        "returns_TSX.json": ("Canada (TSX)", "canada"),
        "returns_JPX.json": ("Japan (JPX)", "japan"),
        "returns_LSE.json": ("UK (LSE)", "uk"),
        "returns_KSC.json": ("South Korea (KSC)", "korea"),
        "returns_TAI.json": ("Taiwan (TAI)", "taiwan"),
        "returns_ASX.json": ("Australia (ASX)", "australia"),
        "returns_STO.json": ("Sweden (STO)", "sweden"),
        "returns_SIX.json": ("Switzerland (SIX)", "switzerland"),
        "returns_SAO.json": ("Brazil (SAO)", "brazil"),
        "returns_SET.json": ("Thailand (SET)", "thailand"),
        "returns_SES.json": ("Singapore (SES)", "singapore"),
        "returns_JNB.json": ("South Africa (JNB)", "southafrica"),
        "returns_OSL.json": ("Norway (OSL)", "norway"),
    }

    for filename, (display_name, short_name) in exchange_map.items():
        path = os.path.join(RESULTS_DIR, filename)
        if not os.path.exists(path):
            continue

        results = load_results(filename)
        if not results.get("annual_returns"):
            continue

        print(f"\nGenerating charts for {display_name}...")
        cumulative_growth_chart(
            results, display_name,
            os.path.join(CHARTS_DIR, f"1_{short_name}_cumulative_growth.png"))
        annual_returns_chart(
            results, display_name,
            os.path.join(CHARTS_DIR, f"2_{short_name}_annual_returns.png"))

    # Comparison charts
    comp_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if os.path.exists(comp_path):
        print("\nGenerating comparison charts...")
        all_results = load_results("exchange_comparison.json")
        comparison_cagr_chart(
            all_results,
            os.path.join(CHARTS_DIR, "1_comparison_cagr.png"))
        comparison_drawdown_chart(
            all_results,
            os.path.join(CHARTS_DIR, "2_comparison_drawdown.png"))

    print(f"\nAll charts saved to {CHARTS_DIR}/")


if __name__ == "__main__":
    main()
