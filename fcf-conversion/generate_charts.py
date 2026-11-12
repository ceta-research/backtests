#!/usr/bin/env python3
"""
Generate charts for FCF Conversion Quality backtest results.

Reads results JSON files and produces:
  - Cumulative growth chart (strategy vs benchmark)
  - Annual returns bar chart (strategy vs benchmark)

A run-window file's partial final year is the last point and the last, hatched bar, and its titles carry the
window the file records. A typed-year file charts as before.

Usage:
    python3 fcf-conversion/generate_charts.py
    python3 fcf-conversion/generate_charts.py --results-dir fcf-conversion/results
"""

import argparse
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (annual_bars, benchmark_label, benchmark_legend, growth_series, is_usd_benchmark_proxy,
                         localize_money_title, money, money_axis_label, money_formatter, partial_note, window_end,
                         window_text, windows_text)
import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
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


def leg_window(entry, bars=False):
    """', <window>' for a run-window leg's titles; '' for a typed-year file, whose titles stay as they were."""
    return f", {window_text(entry, bars=bars)}" if window_end(entry) else ""


def run_window(results, keys):
    """' (<window>)' for a comparison title when one of these legs is on the run window, else ''."""
    return f" ({windows_text(results, keys)})" if any(window_end(results[k]) for k in keys) else ""


def cumulative_growth_chart(entry, exchange, out_path, bench="S&P 500"):
    """Cumulative growth of $10,000, each value at its period's end, a partial final year last."""
    x, port_vals = growth_series(entry, "portfolio")
    spy_vals = growth_series(entry, "spy")[1]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(x, port_vals, color=STRATEGY_COLOR, linewidth=2, label="FCF Conversion Quality")
    ax.plot(x, spy_vals, color=BENCHMARK_COLOR, linewidth=2, label=bench)

    ax.set_title(localize_money_title(f"Cumulative Growth of $10,000 ({exchange}{leg_window(entry)})", exchange),
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel(money_axis_label(exchange))
    ax.yaxis.set_major_formatter(money_formatter(exchange))
    ax.legend(loc="upper left")

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def annual_returns_chart(entry, exchange, out_path, bench="S&P 500"):
    """Annual returns bar chart; a partial final year is the last, hatched bar."""
    years, port = annual_bars(entry, "portfolio")
    spy = annual_bars(entry, "spy")[1]

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.35
    x_pos = range(len(years))

    groups = [ax.bar([p - width / 2 for p in x_pos], port, width, color=STRATEGY_COLOR,
                     label="FCF Conversion Quality", alpha=0.85),
              ax.bar([p + width / 2 for p in x_pos], spy, width, color=BENCHMARK_COLOR,
                     label=bench, alpha=0.85)]
    if entry.get("partial_year"):
        for g in groups:
            g[-1].set_hatch("//")
            g[-1].set_edgecolor("#2c3e50")

    ax.set_title(f"Annual Returns ({exchange}{leg_window(entry, bars=True)})", fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(list(x_pos))
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.legend(loc="upper left")
    note = partial_note(entry)
    if note:
        fig.text(0.5, -0.04, note, ha="center", fontsize=8, color="#7f8c8d")

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def comparison_cagr_chart(results, out_path):
    """CAGR by exchange, each against its own benchmark (marker)."""
    data, excluded = [], []
    for ex, r in results.items():
        if "error" in r or not r.get("portfolio") or r["portfolio"].get("cagr") is None:
            continue
        if r.get("window_truncated"):
            excluded.append(f"{ex} (window truncated)")
        elif r.get("invested_periods", 0) <= 0:
            excluded.append(f"{ex} (never invested)")
        elif r.get("spy", {}).get("cagr") is None:
            excluded.append(f"{ex} (no benchmark CAGR)")
        else:
            data.append((ex, r["portfolio"]["cagr"], r["spy"]["cagr"]))

    if not data:
        return

    data.sort(key=lambda x: x[1], reverse=True)
    exchanges = [d[0] for d in data]
    cagrs = [d[1] for d in data]
    benches = [d[2] for d in data]
    excesses = [results[k].get("comparison", {}).get("excess_cagr", c - b) for k, c, b in data]
    # No local index: measured against the S&P 500 in USD, a cross-currency gap.
    proxy = {k for k in exchanges if is_usd_benchmark_proxy(results, k)}
    colors = ["#9E9E9E" if k in proxy else "#27ae60" if c > b else "#c0392b" for k, c, b in data]

    fig, ax = plt.subplots(figsize=(11, max(6, len(data) * 0.6)))
    ax.barh(range(len(exchanges)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, range(len(exchanges)), marker="|", s=500, linewidths=3, color="black", zorder=3)

    ax.set_yticks(range(len(exchanges)))
    ax.set_yticklabels(exchanges)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)")
    ax.set_xlim(right=max(max(cagrs), max(benches)) + 7)
    ax.set_title(f"FCF Conversion Quality: CAGR vs Own Benchmark by Exchange ({windows_text(results, exchanges)})",
                 fontsize=14, fontweight="bold")
    ax.legend(handles=[
        Patch(color="#27ae60", alpha=0.85, label="Beat its own benchmark"),
        Patch(color="#c0392b", alpha=0.85, label="Trailed its own benchmark"),
        Patch(color="#9E9E9E", alpha=0.85, label="No local index (vs S&P 500, USD)"),
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark CAGR"),
    ], fontsize=9, loc="lower right")
    ax.axvline(x=0, color="black", linewidth=0.5)

    for i, (k, c, b) in enumerate(data):
        ax.text(max(c, b, 0) + 0.3, i, f"{c:.2f}% ({excesses[i]:+.2f} vs {benchmark_label(results, k)})",
                va="center", fontsize=9)

    counted = [(c, b) for k, c, b in data if k not in proxy]
    note = f"{sum(c > b for c, b in counted)} of {len(counted)} beat their own benchmark."
    if proxy:
        note += f" {', '.join(sorted(proxy))} (grey): S&P 500 in USD, not counted."
    if excluded:
        note += f" Excluded: {', '.join(excluded)}."
    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same FCF conversion screen, annual rebalance, equal weight\n"
             f"Returns in local currency, each against its own market's index. {note}",
             ha="center", fontsize=8, color="#7f8c8d")

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
    ax.set_title(f"FCF Conversion Quality: Max Drawdown by Exchange{run_window(results, exchanges)}",
                 fontsize=14, fontweight="bold")
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

        # local index, or "S&P 500, USD" where the series is the US one (SAU has no local index)
        bench = benchmark_legend(results, exchange)

        # Region name mapping for filenames
        region = exchange.lower().replace("_", "")
        if exchange in ("NYSE_NASDAQ_AMEX", "US_MAJOR"):
            region = "us"
        elif exchange in ("NSE",):
            region = "india"
        elif exchange in ("SHZ_SHH",):
            region = "china"
        elif exchange in ("TAI",):
            region = "taiwan"

        cumulative_growth_chart(data, exchange,
                                 os.path.join(args.output_dir, f"1_{region}_cumulative_growth.png"), bench)
        annual_returns_chart(data, exchange,
                              os.path.join(args.output_dir, f"2_{region}_annual_returns.png"), bench)

    # Comparison charts (if multiple exchanges)
    if len(results) >= 3:
        comparison_cagr_chart(results,
                               os.path.join(args.output_dir, "1_comparison_cagr.png"))
        comparison_drawdown_chart(results,
                                   os.path.join(args.output_dir, "2_comparison_drawdown.png"))

    print(f"\nAll charts saved to {args.output_dir}")


if __name__ == "__main__":
    main()
