#!/usr/bin/env python3
"""
Generate charts for FCF Conversion Quality backtest results.

Reads results JSON files and produces:
  - Cumulative growth chart (strategy vs benchmark)
  - Annual returns bar chart (strategy vs benchmark)

Usage:
    python3 fcf-conversion/generate_charts.py
    python3 fcf-conversion/generate_charts.py --results-dir fcf-conversion/results
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
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# Style
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 11,
})

STRATEGY_COLOR = "#2563EB"  # Blue
BENCHMARK_COLOR = "#94A3B8"  # Gray


def load_results(results_dir):
    """Load all result JSON files. Returns dict of {exchange: data}."""
    results = {}
    for fname in sorted(os.listdir(results_dir)):
        if fname.startswith("returns_") and fname.endswith(".json"):
            exchange = fname.replace("returns_", "").replace(".json", "")
            with open(os.path.join(results_dir, fname)) as f:
                results[exchange] = json.load(f)
    # Also try exchange_comparison.json
    comp_path = os.path.join(results_dir, "exchange_comparison.json")
    if os.path.exists(comp_path):
        with open(comp_path) as f:
            data = json.load(f)
            for k, v in data.items():
                if k not in results and isinstance(v, dict) and "portfolio" in v:
                    results[k] = v
    return results


def cumulative_growth_chart(annual_returns, exchange, out_path):
    """Cumulative growth of $10,000."""
    years = [ar["year"] for ar in annual_returns]
    port_vals = [10000]
    spy_vals = [10000]

    for ar in annual_returns:
        port_vals.append(port_vals[-1] * (1 + ar["portfolio"] / 100))
        spy_vals.append(spy_vals[-1] * (1 + ar["spy"] / 100))

    fig, ax = plt.subplots(figsize=(10, 6))
    x = [years[0] - 1] + years
    ax.plot(x, port_vals, color=STRATEGY_COLOR, linewidth=2, label="FCF Conversion Quality")
    ax.plot(x, spy_vals, color=BENCHMARK_COLOR, linewidth=2, label="S&P 500")

    ax.set_title(f"Cumulative Growth of $10,000 ({exchange})", fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Portfolio Value ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(loc="upper left")

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def annual_returns_chart(annual_returns, exchange, out_path):
    """Annual returns bar chart."""
    years = [ar["year"] for ar in annual_returns]
    port = [ar["portfolio"] for ar in annual_returns]
    spy = [ar["spy"] for ar in annual_returns]

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.35
    x_pos = range(len(years))

    ax.bar([p - width / 2 for p in x_pos], port, width, color=STRATEGY_COLOR,
           label="FCF Conversion Quality", alpha=0.85)
    ax.bar([p + width / 2 for p in x_pos], spy, width, color=BENCHMARK_COLOR,
           label="S&P 500", alpha=0.85)

    ax.set_title(f"Annual Returns ({exchange})", fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(list(x_pos))
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.legend(loc="upper left")

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def comparison_cagr_chart(results, out_path):
    """CAGR comparison across exchanges."""
    data = []
    for ex, r in results.items():
        if "error" in r or not r.get("portfolio"):
            continue
        cagr = r["portfolio"].get("cagr")
        spy_cagr = r.get("spy", {}).get("cagr")
        if cagr is not None:
            data.append((ex, cagr, spy_cagr or 0))

    if not data:
        return

    data.sort(key=lambda x: x[1], reverse=True)
    exchanges = [d[0] for d in data]
    cagrs = [d[1] for d in data]
    spy_cagrs = [d[2] for d in data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(data) * 0.5)))
    y_pos = range(len(exchanges))
    ax.barh(y_pos, cagrs, 0.4, color=STRATEGY_COLOR, label="FCF Conversion Quality", alpha=0.85)
    ax.barh([p + 0.4 for p in y_pos], spy_cagrs, 0.4, color=BENCHMARK_COLOR, label="S&P 500", alpha=0.85)

    ax.set_yticks([p + 0.2 for p in y_pos])
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("CAGR (%)")
    ax.set_title("FCF Conversion Quality: CAGR by Exchange", fontsize=14, fontweight="bold")
    ax.legend(loc="lower right")
    ax.axvline(x=0, color="black", linewidth=0.5)

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def comparison_drawdown_chart(results, out_path):
    """Max drawdown comparison across exchanges."""
    data = []
    for ex, r in results.items():
        if "error" in r or not r.get("portfolio"):
            continue
        maxdd = r["portfolio"].get("max_drawdown")
        if maxdd is not None:
            data.append((ex, maxdd))

    if not data:
        return

    data.sort(key=lambda x: x[1])  # Worst (most negative) first
    exchanges = [d[0] for d in data]
    drawdowns = [d[1] for d in data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(data) * 0.5)))
    colors = ["#EF4444" if d < -30 else "#F59E0B" if d < -20 else "#22C55E" for d in drawdowns]
    ax.barh(range(len(exchanges)), drawdowns, color=colors, alpha=0.85)

    ax.set_yticks(range(len(exchanges)))
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("Max Drawdown (%)")
    ax.set_title("FCF Conversion Quality: Max Drawdown by Exchange", fontsize=14, fontweight="bold")
    ax.axvline(x=0, color="black", linewidth=0.5)

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate FCF Conversion charts")
    parser.add_argument("--results-dir", default=RESULTS_DIR)
    parser.add_argument("--output-dir", default=CHART_DIR)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    results = load_results(args.results_dir)
    if not results:
        print(f"No results found in {args.results_dir}")
        return

    print(f"Found results for {len(results)} exchanges: {', '.join(sorted(results.keys()))}")

    # Per-exchange charts
    for exchange, data in results.items():
        if "error" in data or not data.get("annual_returns"):
            continue

        annual = data["annual_returns"]

        # Region name mapping for filenames
        region = exchange.lower().replace("_", "")
        if exchange in ("NYSE_NASDAQ_AMEX", "US_MAJOR"):
            region = "us"
        elif exchange in ("BSE_NSE",):
            region = "india"
        elif exchange in ("SHZ_SHH",):
            region = "china"
        elif exchange in ("TAI",):
            region = "taiwan"

        cumulative_growth_chart(annual, exchange,
                                 os.path.join(args.output_dir, f"1_{region}_cumulative_growth.png"))
        annual_returns_chart(annual, exchange,
                              os.path.join(args.output_dir, f"2_{region}_annual_returns.png"))

    # Comparison charts (if multiple exchanges)
    if len(results) >= 3:
        comparison_cagr_chart(results,
                               os.path.join(args.output_dir, "1_comparison_cagr.png"))
        comparison_drawdown_chart(results,
                                   os.path.join(args.output_dir, "2_comparison_drawdown.png"))

    print(f"\nAll charts saved to {args.output_dir}")


if __name__ == "__main__":
    main()
