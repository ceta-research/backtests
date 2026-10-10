"""Generate Graham Number Timing charts from exchange_comparison.json.

Run after backtest completes to generate blog post charts.

Usage:
    python3 graham-timing/generate_charts.py
"""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import benchmark_label, localize_money_title, money, money_axis_label, money_formatter
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

# Load results
results_file = results_dir / "exchange_comparison.json"
if not results_file.exists():
    print(f"Error: {results_file} not found. Run backtest.py --global first.")
    exit(1)

with open(results_file) as f:
    data = {k: v for k, v in json.load(f).items() if "error" not in v}   # --global records failed markets

print(f"Loaded {len(data)} exchanges from {results_file}\n")

# No local index data: measured against the S&P 500 in USD, a cross-currency gap.
CROSS_CCY = {"JKT"}


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]  # start year
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"]))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(exchange_key, initial=10000):
    """Get SPY cumulative from exchange data."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["benchmark"]))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchange_key, color="#1a5276"):
    """Generate cumulative growth chart for one exchange vs SPY."""
    ex = data[exchange_key]
    fig, ax = plt.subplots(figsize=(12, 6))

    # SPY benchmark
    spy_years, spy_vals = get_spy_cumulative(exchange_key)
    ax.plot(spy_years, spy_vals, color="#95a5a6", linewidth=2,
            label=f"{benchmark_label(data, exchange_key)} ({ex['spy']['cagr']*100:.2f}% CAGR)", linestyle="--")

    # Portfolio
    years, vals = get_cumulative_growth(exchange_key)
    cagr = ex["portfolio"]["cagr"]
    ax.plot(years, vals, color=color, linewidth=2.5,
            label=f"Graham Timing ({cagr*100:.2f}% CAGR)")

    # Final value annotations
    spy_final_k = spy_vals[-1] / 1000
    port_final_k = vals[-1] / 1000

    ax.annotate(money(spy_final_k, exchange_key, suffix="K"),
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color="#95a5a6")

    ax.annotate(money(port_final_k, exchange_key, suffix="K"),
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=color)

    ax.set_ylabel(money_axis_label(exchange_key), fontsize=12, fontweight="bold")
    ax.set_title(f"Graham Number Timing: {exchange_key}",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=11, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(exchange_key))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | Graham Number timing, quarterly rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    filename = f"{exchange_key.lower()}_cumulative_growth.png"
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  ✓ {filename}")
    plt.close()


def chart_annual_bars(exchange_key, color="#1a5276"):
    """Generate annual returns bar chart for one exchange vs SPY."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["benchmark"] * 100 for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] * 100 for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.35
    x = list(range(len(years)))

    ax.bar([i - width/2 for i in x], spy_returns, width,
           label=benchmark_label(data, exchange_key), color="#95a5a6", alpha=0.7)
    ax.bar([i + width/2 for i in x], port_returns, width,
           label="Graham Timing", color=color, alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"Graham Number Timing Annual Returns: {exchange_key}",
                 fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | Graham Number timing, quarterly rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    filename = f"{exchange_key.lower()}_annual_returns.png"
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  ✓ {filename}")
    plt.close()


def chart_comparison_cagr():
    """CAGR by exchange, each against its own benchmark (marker)."""
    exchanges = sorted(data.keys(), key=lambda k: data[k]["portfolio"]["cagr"], reverse=True)
    # Stored as fractions; plot in percent
    cagrs = [data[ex]["portfolio"]["cagr"] * 100 for ex in exchanges]
    benches = [data[ex]["spy"]["cagr"] * 100 for ex in exchanges]
    excesses = [data[ex]["comparison"]["excess_cagr"] * 100 for ex in exchanges]
    colors = ["#9E9E9E" if ex in CROSS_CCY else "#27ae60" if c > b else "#c0392b"
              for ex, c, b in zip(exchanges, cagrs, benches)]

    fig, ax = plt.subplots(figsize=(11, max(6, len(exchanges) * 0.6)))
    ax.barh(range(len(exchanges)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, range(len(exchanges)), marker="|", s=500, linewidths=3, color="black", zorder=3)

    ax.set_yticks(range(len(exchanges)))
    ax.set_yticklabels(exchanges, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_xlim(right=max(max(cagrs), max(benches)) + 8)
    ax.set_title("Graham Number Timing: CAGR vs Own Benchmark by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    # Legend lists only the colours actually drawn
    swatches = [("#27ae60", "Beat its own benchmark"), ("#c0392b", "Trailed its own benchmark"),
                ("#9E9E9E", "No local index (vs S&P 500, USD)")]
    ax.legend(handles=[Patch(color=c, alpha=0.85, label=l) for c, l in swatches if c in colors] + [
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark CAGR"),
    ], fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (ex, c, b, e) in enumerate(zip(exchanges, cagrs, benches, excesses)):
        bench_name = benchmark_label(data, ex) + (" in USD" if ex in CROSS_CCY else "")
        ax.text(max(c, b, 0) + 0.3, i, f"{c:.2f}% ({e:+.2f} vs {bench_name})",
                va="center", fontsize=9)

    beat = sum(c > b for ex, c, b in zip(exchanges, cagrs, benches) if ex not in CROSS_CCY)
    fig.text(0.5, -0.02,
             "Data: Ceta Research | Graham Number timing, quarterly rebalance, 2000-2025\n"
             "Returns in local currency, each against its own market's index; Indonesia against the "
             f"S&P 500 in USD. {beat} of {len(exchanges)} beat their own benchmark.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "comparison_cagr.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  ✓ comparison_cagr.png")
    plt.close()


def chart_comparison_drawdown():
    """Generate max drawdown comparison bar chart across all exchanges."""
    exchanges = sorted(data.keys(), key=lambda k: data[k]["portfolio"]["max_drawdown"])
    drawdowns = [data[ex]["portfolio"]["max_drawdown"] * 100 for ex in exchanges]

    fig, ax = plt.subplots(figsize=(12, 8))

    y_pos = list(range(len(exchanges)))

    # Single colour: every value is below -43%, so severity bands added nothing
    ax.barh(y_pos, drawdowns, height=0.7, color="#c0392b", alpha=0.8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(exchanges, fontsize=10)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Graham Number Timing: Maximum Drawdown by Exchange",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Graham Number timing, quarterly rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "comparison_drawdown.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  ✓ comparison_drawdown.png")
    plt.close()


# Main execution
print("Generating charts...\n")

# Individual exchange charts (cumulative + annual)
colors = {
    "AMEX+NASDAQ+NYSE": "#1a5276",
    "NSE": "#e67e22",
    "XETRA": "#27ae60",
    "SHH+SHZ": "#c0392b",
    "HKSE": "#8e44ad",
    "KSC": "#95a5a6",
    "TSX": "#7f8c8d",
    "SET": "#16a085",
    "TAI": "#d35400",
    "JPX": "#2c3e50",
    "LSE": "#8e44ad",
    "SIX": "#34495e",
    "STO": "#2980b9",
    "JKT": "#d68910",
}

for ex_key in data.keys():
    color = colors.get(ex_key, "#1a5276")
    chart_cumulative(ex_key, color=color)
    chart_annual_bars(ex_key, color=color)

# Comparison charts
print()
chart_comparison_cagr()
chart_comparison_drawdown()

print(f"\n✓ All charts saved to {charts_dir}/\n")
