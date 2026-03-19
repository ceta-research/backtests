#!/usr/bin/env python3
"""
Generate charts for DuPont ROE Decomposition backtest results.

Reads results JSON and generates:
  1. Cumulative growth chart (Quality ROE vs Margin-Driven vs Leverage-Driven vs SPY)
  2. Annual returns bar chart (Quality ROE vs SPY)
  3. Margin-Leverage spread chart
  4. Comparison charts for global mode

Usage:
    python3 roe-dupont/generate_charts.py
    python3 roe-dupont/generate_charts.py --input results/exchange_comparison.json
"""

import argparse
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


CHART_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
COLORS = {
    "quality_roe": "#2196F3",      # Blue
    "margin_driven": "#4CAF50",    # Green
    "leverage_driven": "#F44336",  # Red
    "all_high_roe": "#FF9800",     # Orange
    "spy": "#9E9E9E",             # Gray
}


def load_results(path):
    with open(path) as f:
        return json.load(f)


def cumulative_growth(annual_returns, track_key):
    """Compute cumulative growth from annual return percentages."""
    cum = 1.0
    values = [cum]
    for yr in annual_returns:
        ret = yr[track_key] / 100.0
        cum *= (1 + ret)
        values.append(cum)
    return values


def chart_cumulative(results, universe_name, output_prefix=""):
    """Cumulative growth: Quality ROE vs Margin vs Leverage vs SPY."""
    ar = results["annual_returns"]
    years = [r["year"] for r in ar]
    x_labels = years + [years[-1] + 1]

    fig, ax = plt.subplots(figsize=(12, 6))

    for track, label, color in [
        ("quality_roe", "Quality ROE", COLORS["quality_roe"]),
        ("margin_driven", "Margin-Driven (Q1)", COLORS["margin_driven"]),
        ("leverage_driven", "Leverage-Driven (Q1)", COLORS["leverage_driven"]),
        ("spy", "S&P 500", COLORS["spy"]),
    ]:
        vals = cumulative_growth(ar, track)
        ax.plot(x_labels, vals, label=label, color=color, linewidth=2)

    ax.set_title(f"DuPont ROE Decomposition: Cumulative Growth ({universe_name})",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Growth of $1")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.1f"))

    plt.tight_layout()
    path = os.path.join(CHART_DIR, f"{output_prefix}1_{universe_name.lower()}_cumulative_growth.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path


def chart_annual_returns(results, universe_name, output_prefix=""):
    """Annual returns: Quality ROE vs SPY bar chart."""
    ar = results["annual_returns"]
    years = [r["year"] for r in ar]
    quality = [r["quality_roe"] for r in ar]
    spy = [r["spy"] for r in ar]

    fig, ax = plt.subplots(figsize=(14, 6))

    x = range(len(years))
    width = 0.35
    ax.bar([i - width/2 for i in x], quality, width,
           label="Quality ROE", color=COLORS["quality_roe"], alpha=0.8)
    ax.bar([i + width/2 for i in x], spy, width,
           label="S&P 500", color=COLORS["spy"], alpha=0.8)

    ax.set_title(f"DuPont Quality ROE: Annual Returns ({universe_name})",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(years, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    ax.axhline(y=0, color="black", linewidth=0.5)

    plt.tight_layout()
    path = os.path.join(CHART_DIR, f"{output_prefix}2_{universe_name.lower()}_annual_returns.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path


def chart_margin_leverage_spread(results, universe_name, output_prefix=""):
    """Margin-Driven vs Leverage-Driven annual spread."""
    ar = results["annual_returns"]
    years = [r["year"] for r in ar]
    spreads = [r["margin_driven"] - r["leverage_driven"] for r in ar]

    fig, ax = plt.subplots(figsize=(14, 6))

    colors = [COLORS["margin_driven"] if s >= 0 else COLORS["leverage_driven"] for s in spreads]
    ax.bar(years, spreads, color=colors, alpha=0.8)

    ax.set_title(f"Margin-Driven vs Leverage-Driven: Annual Spread ({universe_name})",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Spread (percentage points)")
    ax.grid(True, alpha=0.3, axis="y")
    ax.axhline(y=0, color="black", linewidth=0.8)

    avg_spread = sum(spreads) / len(spreads) if spreads else 0
    ax.axhline(y=avg_spread, color="purple", linewidth=1, linestyle="--",
               label=f"Avg: {avg_spread:+.1f}pp")
    ax.legend()

    plt.tight_layout()
    path = os.path.join(CHART_DIR, f"{output_prefix}3_{universe_name.lower()}_margin_leverage_spread.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path


def chart_comparison_cagr(all_results, output_prefix=""):
    """Global comparison: CAGR across exchanges."""
    exchanges = []
    quality_cagrs = []
    margin_cagrs = []
    leverage_cagrs = []
    spy_cagrs = []

    for name, r in sorted(all_results.items()):
        if "error" in r:
            continue
        p = r["portfolios"]
        exchanges.append(name)
        quality_cagrs.append(p["quality_roe"]["cagr"])
        margin_cagrs.append(p["margin_driven"]["cagr"])
        leverage_cagrs.append(p["leverage_driven"]["cagr"])
        spy_cagrs.append(p["sp500"]["cagr"])

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(14, 8))

    x = range(len(exchanges))
    width = 0.2
    ax.barh([i - 1.5*width for i in x], quality_cagrs, width,
            label="Quality ROE", color=COLORS["quality_roe"], alpha=0.8)
    ax.barh([i - 0.5*width for i in x], margin_cagrs, width,
            label="Margin-Driven", color=COLORS["margin_driven"], alpha=0.8)
    ax.barh([i + 0.5*width for i in x], leverage_cagrs, width,
            label="Leverage-Driven", color=COLORS["leverage_driven"], alpha=0.8)
    ax.barh([i + 1.5*width for i in x], spy_cagrs, width,
            label="S&P 500", color=COLORS["spy"], alpha=0.8)

    ax.set_yticks(list(x))
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("CAGR (%)")
    ax.set_title("DuPont ROE: CAGR Comparison Across Exchanges",
                 fontsize=14, fontweight="bold")
    ax.legend(loc="lower right")
    ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()
    path = os.path.join(CHART_DIR, f"{output_prefix}1_comparison_cagr.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")


def chart_comparison_drawdown(all_results, output_prefix=""):
    """Global comparison: Max drawdown across exchanges."""
    exchanges = []
    quality_dd = []
    spy_dd = []

    for name, r in sorted(all_results.items()):
        if "error" in r:
            continue
        p = r["portfolios"]
        exchanges.append(name)
        quality_dd.append(p["quality_roe"]["max_drawdown"])
        spy_dd.append(p["sp500"]["max_drawdown"])

    if not exchanges:
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    x = range(len(exchanges))
    width = 0.35
    ax.barh([i - width/2 for i in x], quality_dd, width,
            label="Quality ROE", color=COLORS["quality_roe"], alpha=0.8)
    ax.barh([i + width/2 for i in x], spy_dd, width,
            label="S&P 500", color=COLORS["spy"], alpha=0.8)

    ax.set_yticks(list(x))
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("Max Drawdown (%)")
    ax.set_title("DuPont Quality ROE: Max Drawdown Comparison",
                 fontsize=14, fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()
    path = os.path.join(CHART_DIR, f"{output_prefix}2_comparison_drawdown.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate DuPont ROE charts")
    parser.add_argument("--input", type=str, help="Input JSON file")
    args = parser.parse_args()

    os.makedirs(CHART_DIR, exist_ok=True)

    # Check for global results
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    global_path = os.path.join(results_dir, "exchange_comparison.json")
    us_path = os.path.join(results_dir, "us_results.json")

    input_path = args.input or (global_path if os.path.exists(global_path) else us_path)

    if not os.path.exists(input_path):
        print(f"No results found at {input_path}")
        print("Run the backtest first: python3 roe-dupont/backtest.py --output roe-dupont/results/us_results.json")
        sys.exit(1)

    data = load_results(input_path)

    # Check if this is global (multi-exchange) or single exchange
    if "universe" in data:
        # Single exchange results
        print(f"Generating charts for {data['universe']}...")
        chart_cumulative(data, data["universe"])
        chart_annual_returns(data, data["universe"])
        chart_margin_leverage_spread(data, data["universe"])
    else:
        # Global results (dict of exchange -> results)
        print("Generating per-exchange and comparison charts...")

        for name, r in data.items():
            if "error" in r or "annual_returns" not in r:
                print(f"  Skipping {name} (error or no data)")
                continue
            prefix = f"{name.lower()}_"
            chart_cumulative(r, name, output_prefix=prefix)
            chart_annual_returns(r, name, output_prefix=prefix)
            chart_margin_leverage_spread(r, name, output_prefix=prefix)

        # Comparison charts
        chart_comparison_cagr(data)
        chart_comparison_drawdown(data)

    print(f"\nAll charts saved to {CHART_DIR}/")


if __name__ == "__main__":
    main()
