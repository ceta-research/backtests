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
from chart_utils import (benchmark_label, benchmark_legend, is_usd_benchmark_proxy,
                         localize_money_title, money, money_axis_label, money_formatter)
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

# Broken FMP split adjustments on ASX/SAO; the comparison post excludes both.
COMPARISON_EXCLUDE = {"australia", "brazil"}
# Comparison-only names, matching the comparison post's table.
COMPARISON_LABELS = {"us": "US (NYSE+NASDAQ+AMEX)", "china": "China (SHH+SHZ)",
                     "saudi": "Saudi (SAU)"}


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
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch
    except ImportError:
        return

    valid = {k: v for k, v in all_results.items()
             if k not in COMPARISON_EXCLUDE
             and v.get("portfolio", {}).get("cagr") is not None
             and "error" not in v}
    if len(valid) < 2:
        return

    # Each market against its OWN benchmark ("spy" holds the local index outside
    # the US); S&P 500 proxies (no local index) are USD, so never counted as beats.
    proxy = {k for k in valid if is_usd_benchmark_proxy(valid, k)}

    def name(k):
        return COMPARISON_LABELS.get(k, REGIONS.get(k, k))

    proxy_note = ""
    if proxy:
        names_ = ", ".join(name(k) for k in sorted(proxy))
        proxy_note = f"{names_} has no local index: its marker is the S&P 500 in USD"
        if "saudi" in proxy:  # USD/SAR 3.75 at both ends of Apr 2000-Apr 2025
            proxy_note += " (the riyal held near 3.75 per dollar, Apr 2000-Apr 2025)"
        proxy_note += ". "
    legend_extra = [Patch(color="#9E9E9E", alpha=0.85,
                          label="No local index (vs S&P 500, USD)")] if proxy else []
    bench_note = "local indices are price-only (no dividends), the S&P 500 is total return."
    n_rows = len(valid)

    # CAGR vs own benchmark, best first
    rows = sorted(valid.items(), key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)
    keys = [k for k, _ in rows]
    cagrs = [v["portfolio"]["cagr"] for _, v in rows]
    benches = [v["spy"]["cagr"] for _, v in rows]
    excesses = [v.get("comparison", {}).get("excess_cagr", c - b)
                for (_, v), c, b in zip(rows, cagrs, benches)]
    colors = ["#9E9E9E" if k in proxy else "#27ae60" if c > b else "#c0392b"
              for k, c, b in zip(keys, cagrs, benches)]

    fig, ax = plt.subplots(figsize=(12, max(6, n_rows * 0.6)))
    ax.barh(range(n_rows), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, range(n_rows), marker="|", s=500, linewidths=3, color="black", zorder=3)
    ax.set_yticks(range(n_rows))
    ax.set_yticklabels([name(k) for k in keys], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_xlim(left=min(0, min(cagrs), min(benches)), right=max(max(cagrs), max(benches)) + 9)
    ax.set_title("DCF Discount: CAGR vs Own Benchmark by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(handles=[
        Patch(color="#27ae60", alpha=0.85, label="Beat its own benchmark"),
        Patch(color="#c0392b", alpha=0.85, label="Trailed its own benchmark"),
        *legend_extra,
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark CAGR"),
    ], fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)
    for i, (k, c, b, e) in enumerate(zip(keys, cagrs, benches, excesses)):
        ax.text(max(c, b, 0) + 0.3, i, f"{c:.2f}% ({e:+.2f} vs {benchmark_legend(valid, k)})",
                va="center", fontsize=9)
    beat = sum(c > b for k, c, b in zip(keys, cagrs, benches) if k not in proxy)
    fig.text(0.5, -0.04,
             "Data: Ceta Research | FCF/MarketCap >= 8.78% screen, top 50, annual April "
             "rebalance, equal weight\n"
             f"Returns in local currency, each against its own market's index; {bench_note}\n"
             f"{proxy_note}{beat} of {n_rows} beat their benchmark.",
             ha="center", fontsize=8, color="#7f8c8d")
    fig.tight_layout()
    out1 = os.path.join(output_dir, "1_comparison_cagr.png")
    fig.savefig(out1, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {out1}")

    # Max drawdown vs own benchmark, shallowest first
    rows = sorted(valid.items(), key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)
    keys = [k for k, _ in rows]
    port_dd = [v["portfolio"]["max_drawdown"] for _, v in rows]
    bench_dd = [v["spy"]["max_drawdown"] for _, v in rows]
    colors = ["#9E9E9E" if k in proxy else "#1f77b4" for k in keys]

    fig2, ax2 = plt.subplots(figsize=(12, max(6, n_rows * 0.6)))
    ax2.barh(range(n_rows), port_dd, color=colors, alpha=0.85, height=0.6)
    ax2.scatter(bench_dd, range(n_rows), marker="|", s=500, linewidths=3, color="black", zorder=3)
    ax2.set_yticks(range(n_rows))
    ax2.set_yticklabels([name(k) for k in keys], fontsize=11)
    ax2.invert_yaxis()
    ax2.set_xlabel("Max drawdown (%, local currency)", fontsize=12, fontweight="bold")
    ax2.set_xlim(left=min(min(port_dd), min(bench_dd)) - 32, right=0)
    ax2.set_title("DCF Discount: Max Drawdown vs Own Benchmark by Exchange (2000-2025)",
                  fontsize=14, fontweight="bold", pad=32)
    # above the axes: every row's label sits on the left, every bar on the right
    ax2.legend(handles=[
        Patch(color="#1f77b4", alpha=0.85, label="DCF Discount max drawdown"),
        *legend_extra,
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark max drawdown"),
    ], fontsize=9, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=3, frameon=False)
    ax2.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax2.set_axisbelow(True)
    for i, (k, d, b) in enumerate(zip(keys, port_dd, bench_dd)):
        ax2.text(min(d, b) - 0.6, i, f"{d:.2f}% ({benchmark_legend(valid, k)} {b:.2f}%)",
                 va="center", ha="right", fontsize=9)
    fig2.text(0.5, -0.04,
              "Data: Ceta Research | Max drawdown on annual (April) rebalance returns\n"
              f"Each market against its own index; {bench_note}\n"
              f"{proxy_note}".rstrip(),
              ha="center", fontsize=8, color="#7f8c8d")
    fig2.tight_layout()
    out2 = os.path.join(output_dir, "2_comparison_drawdown.png")
    fig2.savefig(out2, dpi=150, bbox_inches="tight", facecolor="white")
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
