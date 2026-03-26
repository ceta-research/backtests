#!/usr/bin/env python3
"""
Generate charts for Asset-Light backtest results.

Reads results JSON and generates:
- Cumulative growth charts (light vs heavy vs SPY)
- Annual returns bar charts
- Exchange comparison charts (if global results)

Usage:
    python3 asset-light/generate_charts.py
    python3 asset-light/generate_charts.py --input results/exchange_comparison.json
"""

import json
import os
import sys
import argparse

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
CHARTS_DIR = os.path.join(SCRIPT_DIR, "charts")


def cumulative_growth(returns):
    """Compute cumulative growth from list of period returns."""
    values = [1.0]
    for r in returns:
        values.append(values[-1] * (1 + r / 100))
    return values


def plot_cumulative(data, universe, output_path):
    """Plot cumulative growth: light vs heavy vs SPY."""
    years = [d["year"] for d in data]
    light_rets = [d["light"] for d in data]
    heavy_rets = [d["heavy"] for d in data]
    spy_rets = [d["spy"] for d in data]

    light_cum = cumulative_growth(light_rets)
    heavy_cum = cumulative_growth(heavy_rets)
    spy_cum = cumulative_growth(spy_rets)

    x_labels = [str(years[0])] + [str(y + 1) for y in years]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(range(len(light_cum)), light_cum, 'b-', linewidth=2, label='Asset-Light (top 20%)')
    ax.plot(range(len(heavy_cum)), heavy_cum, 'r-', linewidth=1.5, label='Asset-Heavy (bottom 20%)')
    ax.plot(range(len(spy_cum)), spy_cum, 'k--', linewidth=1.5, label='S&P 500')

    ax.set_xlabel('Year')
    ax.set_ylabel('Growth of $1')
    ax.set_title(f'Asset-Light vs Asset-Heavy: Cumulative Growth ({universe})')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_xticks(range(0, len(x_labels), max(1, len(x_labels) // 10)))
    ax.set_xticklabels([x_labels[i] for i in range(0, len(x_labels), max(1, len(x_labels) // 10))],
                       rotation=45)

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {output_path}")


def plot_annual_returns(data, universe, output_path):
    """Plot annual returns bar chart: light vs heavy."""
    years = [d["year"] for d in data]
    light_rets = [d["light"] for d in data]
    heavy_rets = [d["heavy"] for d in data]

    fig, ax = plt.subplots(figsize=(14, 6))
    x = range(len(years))
    width = 0.35

    ax.bar([i - width/2 for i in x], light_rets, width, color='#2196F3', label='Asset-Light')
    ax.bar([i + width/2 for i in x], heavy_rets, width, color='#F44336', alpha=0.7, label='Asset-Heavy')

    ax.axhline(y=0, color='black', linewidth=0.5)
    ax.set_xlabel('Year')
    ax.set_ylabel('Annual Return (%)')
    ax.set_title(f'Asset-Light vs Asset-Heavy: Annual Returns ({universe})')
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.0f%%'))

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {output_path}")


def plot_exchange_comparison(all_results, output_path_cagr, output_path_dd):
    """Plot exchange comparison: CAGR and max drawdown."""
    exchanges = []
    light_cagrs = []
    heavy_cagrs = []
    light_dds = []
    heavy_dds = []

    for name, r in all_results.items():
        if "error" in r:
            continue
        p = r["portfolios"]
        exchanges.append(name)
        light_cagrs.append(p["asset_light"]["cagr"])
        heavy_cagrs.append(p["asset_heavy"]["cagr"])
        light_dds.append(abs(p["asset_light"]["max_drawdown"]))
        heavy_dds.append(abs(p["asset_heavy"]["max_drawdown"]))

    if not exchanges:
        return

    # Sort by light CAGR descending
    sorted_idx = sorted(range(len(exchanges)), key=lambda i: light_cagrs[i], reverse=True)
    exchanges = [exchanges[i] for i in sorted_idx]
    light_cagrs = [light_cagrs[i] for i in sorted_idx]
    heavy_cagrs = [heavy_cagrs[i] for i in sorted_idx]
    light_dds = [light_dds[i] for i in sorted_idx]
    heavy_dds = [heavy_dds[i] for i in sorted_idx]

    # CAGR comparison
    fig, ax = plt.subplots(figsize=(14, 6))
    x = range(len(exchanges))
    width = 0.35
    ax.bar([i - width/2 for i in x], light_cagrs, width, color='#2196F3', label='Asset-Light')
    ax.bar([i + width/2 for i in x], heavy_cagrs, width, color='#F44336', alpha=0.7, label='Asset-Heavy')
    ax.set_xlabel('Exchange')
    ax.set_ylabel('CAGR (%)')
    ax.set_title('Asset-Light vs Asset-Heavy: CAGR by Exchange')
    ax.set_xticks(x)
    ax.set_xticklabels(exchanges, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f%%'))
    fig.tight_layout()
    fig.savefig(output_path_cagr, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {output_path_cagr}")

    # Max drawdown comparison
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width/2 for i in x], light_dds, width, color='#2196F3', label='Asset-Light')
    ax.bar([i + width/2 for i in x], heavy_dds, width, color='#F44336', alpha=0.7, label='Asset-Heavy')
    ax.set_xlabel('Exchange')
    ax.set_ylabel('Max Drawdown (%)')
    ax.set_title('Asset-Light vs Asset-Heavy: Max Drawdown by Exchange')
    ax.set_xticks(x)
    ax.set_xticklabels(exchanges, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.0f%%'))
    fig.tight_layout()
    fig.savefig(output_path_dd, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {output_path_dd}")


def main():
    parser = argparse.ArgumentParser(description="Generate Asset-Light charts")
    parser.add_argument("--input", type=str, help="Input JSON file")
    args = parser.parse_args()

    os.makedirs(CHARTS_DIR, exist_ok=True)

    # Try to find results
    input_file = args.input
    if not input_file:
        # Look for exchange_comparison.json first, then single-exchange files
        comp_path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
        if os.path.exists(comp_path):
            input_file = comp_path
        else:
            # Find any JSON in results/
            for f in os.listdir(RESULTS_DIR):
                if f.endswith(".json"):
                    input_file = os.path.join(RESULTS_DIR, f)
                    break

    if not input_file or not os.path.exists(input_file):
        print("No results found. Run backtest first.")
        return

    with open(input_file) as f:
        data = json.load(f)

    # Check if this is a global comparison or single exchange
    if "annual_returns" in data:
        # Single exchange result
        universe = data.get("universe", "Unknown")
        slug = universe.lower().replace(" ", "_")
        print(f"Generating charts for {universe}...")

        plot_cumulative(data["annual_returns"], universe,
                       os.path.join(CHARTS_DIR, f"1_{slug}_cumulative_growth.png"))
        plot_annual_returns(data["annual_returns"], universe,
                          os.path.join(CHARTS_DIR, f"2_{slug}_annual_returns.png"))
    else:
        # Global comparison
        print("Generating comparison charts...")

        # Per-exchange charts
        for name, result in data.items():
            if "error" in result or "annual_returns" not in result:
                continue
            slug = name.lower().replace(" ", "_")
            print(f"\n  {name}:")
            plot_cumulative(result["annual_returns"], name,
                           os.path.join(CHARTS_DIR, f"1_{slug}_cumulative_growth.png"))
            plot_annual_returns(result["annual_returns"], name,
                              os.path.join(CHARTS_DIR, f"2_{slug}_annual_returns.png"))

        # Comparison charts
        plot_exchange_comparison(
            data,
            os.path.join(CHARTS_DIR, "1_comparison_cagr.png"),
            os.path.join(CHARTS_DIR, "2_comparison_drawdown.png")
        )

    print("\nDone! Charts saved to:", CHARTS_DIR)


if __name__ == "__main__":
    main()
