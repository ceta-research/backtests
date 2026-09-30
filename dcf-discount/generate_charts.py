#!/usr/bin/env python3
"""
Generate charts for DCF Discount backtest results.

Reads dcf-discount/results/{region}.json and writes PNG charts to
dcf-discount/charts/.

Usage:
    python3 dcf-discount/generate_charts.py
    python3 dcf-discount/generate_charts.py --exchange canada
    python3 dcf-discount/generate_charts.py --results-dir path/to/results --output-dir path/to/charts
"""

import argparse
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_label, localize_money_title, money, money_axis_label,
                         money_formatter)
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# Region files are the May 2026 re-run the blogs quote. returns_*.json and
# exchange_comparison*.json are the March run; read only when no region file exists.
REGIONS = {
    "us": "US", "canada": "Canada (TSX)", "germany": "Germany (XETRA)",
    "india": "India (NSE)", "korea": "Korea (KSC)", "sweden": "Sweden (STO)",
    "taiwan": "Taiwan (TAI+TWO)", "australia": "Australia (ASX)",
    "brazil": "Brazil (SAO)", "china": "China (SHZ+SHH)",
    "hongkong": "Hong Kong (HKSE)", "japan": "Japan (JPX)", "uk": "UK (LSE)",
    "switzerland": "Switzerland (SIX)", "thailand": "Thailand (SET)",
    "saudi": "Saudi Arabia (SAU)",
}


def load_results(results_dir):
    """Load exchange result JSON files from results dir."""
    results = {}
    for region in REGIONS:
        path = os.path.join(results_dir, f"{region}.json")
        if os.path.exists(path):
            with open(path) as f:
                results[region] = json.load(f)
    if results:
        return results
    for fname in os.listdir(results_dir):
        if fname.startswith("returns_") and fname.endswith(".json"):
            exchange = fname[len("returns_"):-len(".json")]
            with open(os.path.join(results_dir, fname)) as f:
                results[exchange] = json.load(f)
    # Also check exchange_comparison.json
    comp_path = os.path.join(results_dir, "exchange_comparison.json")
    if os.path.exists(comp_path):
        with open(comp_path) as f:
            results.update(json.load(f))
    return results


def generate_cumulative_chart(exchange, result, output_dir, strategy_name="DCF Discount"):
    """Generate cumulative growth chart for one exchange."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("matplotlib not installed. Run: pip install matplotlib")
        return None

    annual = result.get("annual_returns", [])
    if not annual:
        return None

    years = [a["year"] for a in annual]
    port_ret = [a["portfolio"] / 100 for a in annual]
    spy_ret = [a["spy"] / 100 for a in annual]

    # Cumulative
    port_cum = [10000.0]
    spy_cum = [10000.0]
    for pr, sr in zip(port_ret, spy_ret):
        port_cum.append(port_cum[-1] * (1 + pr))
        spy_cum.append(spy_cum[-1] * (1 + sr))
    x_labels = [str(years[0] - 1)] + [str(y) for y in years]

    # "spy" holds whichever benchmark the run used: the local index outside the US.
    bench = benchmark_label({exchange: result}, exchange)
    port_cagr = result.get("portfolio", {}).get("cagr")
    bench_cagr = result.get("spy", {}).get("cagr")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(range(len(port_cum)), port_cum, color="#1f77b4", linewidth=2,
            label=f"{strategy_name} ({port_cagr:.2f}% CAGR)")
    ax.plot(range(len(spy_cum)), spy_cum, color="#ff7f0e", linewidth=2, linestyle="--",
            label=f"{bench} ({bench_cagr:.2f}% CAGR)")
    for vals, color, dy in ((port_cum, "#1f77b4", 0), (spy_cum, "#ff7f0e", -12)):
        ax.annotate(money(vals[-1] / 1000, exchange, suffix="K"), xy=(len(vals) - 1, vals[-1]),
                    xytext=(6, dy), textcoords="offset points", fontsize=9,
                    fontweight="bold", color=color)

    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(money_formatter(exchange))
    ax.set_title(localize_money_title(
                     f"{strategy_name}, {REGIONS.get(exchange, exchange)}: Growth of $10,000",
                     exchange),
                 fontsize=13, pad=12)
    ax.set_ylabel(money_axis_label(exchange))
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    out_path = os.path.join(output_dir, f"1_{exchange.lower()}_cumulative_growth.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def generate_annual_returns_chart(exchange, result, output_dir, strategy_name="DCF Discount"):
    """Generate annual returns bar chart for one exchange."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    annual = result.get("annual_returns", [])
    if not annual:
        return None

    years = [a["year"] for a in annual]
    port_ret = [a["portfolio"] for a in annual]
    spy_ret = [a["spy"] for a in annual]

    x = range(len(years))
    width = 0.4
    fig, ax = plt.subplots(figsize=(12, 5))
    bars1 = ax.bar([i - width/2 for i in x], port_ret, width, label=strategy_name, color="#1f77b4", alpha=0.85)
    bars2 = ax.bar([i + width/2 for i in x], spy_ret, width,
                   label=benchmark_label({exchange: result}, exchange), color="#ff7f0e", alpha=0.85)

    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(lambda x, _: f"{x:.0f}%")
    ax.set_title(f"{strategy_name}, {REGIONS.get(exchange, exchange)}: Annual Returns",
                 fontsize=13, pad=12)
    ax.set_ylabel("Annual Return (%)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    out_path = os.path.join(output_dir, f"2_{exchange.lower()}_annual_returns.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return out_path


def generate_comparison_chart(all_results, output_dir):
    """Generate CAGR and drawdown comparison charts across exchanges."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    valid = {k: v for k, v in all_results.items()
             if v.get("portfolio", {}).get("cagr") is not None
             and "error" not in v}
    if len(valid) < 2:
        return

    # Sort by portfolio CAGR
    sorted_ex = sorted(valid.items(), key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)
    labels = [k for k, _ in sorted_ex]
    port_cagr = [v["portfolio"]["cagr"] for _, v in sorted_ex]
    spy_cagr = [v["spy"]["cagr"] for _, v in sorted_ex]
    excess = [v["comparison"]["excess_cagr"] for _, v in sorted_ex]

    # CAGR comparison
    x = range(len(labels))
    width = 0.3
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width for i in x], port_cagr, width, label="DCF Discount", color="#1f77b4", alpha=0.85)
    ax.bar([i for i in x], spy_cagr, width, label="SPY", color="#ff7f0e", alpha=0.85)
    ax.bar([i + width for i in x], excess, width, label="Excess CAGR", color="#2ca02c", alpha=0.85)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax.set_title("DCF Discount: CAGR Comparison Across Exchanges", fontsize=13, pad=12)
    ax.set_ylabel("CAGR (%)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out1 = os.path.join(output_dir, "1_comparison_cagr.png")
    fig.savefig(out1, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out1}")

    # Max drawdown comparison
    port_dd = [v["portfolio"]["max_drawdown"] for _, v in sorted_ex]
    spy_dd = [v["spy"]["max_drawdown"] for _, v in sorted_ex]
    fig2, ax2 = plt.subplots(figsize=(14, 5))
    ax2.bar([i - width/2 for i in x], port_dd, width, label="DCF Discount", color="#1f77b4", alpha=0.85)
    ax2.bar([i + width/2 for i in x], spy_dd, width, label="SPY", color="#ff7f0e", alpha=0.85)
    ax2.set_xticks(list(x))
    ax2.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax2.set_title("DCF Discount: Max Drawdown Comparison Across Exchanges", fontsize=13, pad=12)
    ax2.set_ylabel("Max Drawdown (%)")
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)
    fig2.tight_layout()
    out2 = os.path.join(output_dir, "2_comparison_drawdown.png")
    fig2.savefig(out2, dpi=150, bbox_inches="tight")
    plt.close(fig2)
    print(f"  Saved: {out2}")


def main():
    parser = argparse.ArgumentParser(description="Generate DCF Discount backtest charts")
    parser.add_argument("--results-dir", default=None,
                        help="Path to results directory (default: dcf-discount/results/)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for charts (default: dcf-discount/charts/)")
    parser.add_argument("--exchange", default=None,
                        help="Generate charts for specific exchange only")
    args = parser.parse_args()

    results_dir = args.results_dir or os.path.join(HERE, "results")
    output_dir = args.output_dir or os.path.join(HERE, "charts")

    if not os.path.exists(results_dir):
        print(f"Results directory not found: {results_dir}")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    print(f"Loading results from: {results_dir}")
    all_results = load_results(results_dir)
    print(f"Found results for: {list(all_results.keys())}")

    if not all_results:
        print("No result files found.")
        return

    if args.exchange:
        target = {args.exchange: all_results.get(args.exchange, {})}
    else:
        target = all_results

    for exchange, result in target.items():
        if "error" in result or not result.get("annual_returns"):
            print(f"  Skipping {exchange} (no annual returns data)")
            continue
        print(f"\nGenerating charts for {exchange}...")
        generate_cumulative_chart(exchange, result, output_dir)
        generate_annual_returns_chart(exchange, result, output_dir)

    if len(all_results) >= 2 and not args.exchange:
        print("\nGenerating comparison charts...")
        generate_comparison_chart(all_results, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
