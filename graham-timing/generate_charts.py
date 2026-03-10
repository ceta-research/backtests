"""Generate Graham Number Timing charts from exchange_comparison.json.

Run after backtest completes to generate blog post charts.

Usage:
    python3 graham-timing/generate_charts.py
"""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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
    data = json.load(f)

print(f"Loaded {len(data)} exchanges from {results_file}\n")


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
            label=f"S&P 500 ({ex['spy']['cagr']*100:.2f}% CAGR)", linestyle="--")

    # Portfolio
    years, vals = get_cumulative_growth(exchange_key)
    cagr = ex["portfolio"]["cagr"]
    ax.plot(years, vals, color=color, linewidth=2.5,
            label=f"Graham Timing ({cagr*100:.2f}% CAGR)")

    # Final value annotations
    spy_final_k = spy_vals[-1] / 1000
    port_final_k = vals[-1] / 1000

    ax.annotate(f"${spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color="#95a5a6")

    ax.annotate(f"${port_final_k:,.0f}K",
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=color)

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(f"Graham Number Timing: {exchange_key}",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=11, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
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
           label="S&P 500", color="#95a5a6", alpha=0.7)
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
    """Generate CAGR comparison bar chart across all exchanges."""
    exchanges = sorted(data.keys(), key=lambda k: data[k]["portfolio"]["cagr"], reverse=True)
    cagrs = [data[ex]["portfolio"]["cagr"] * 100 for ex in exchanges]
    spy_cagrs = [data[ex]["spy"]["cagr"] * 100 for ex in exchanges]

    fig, ax = plt.subplots(figsize=(12, 8))

    y_pos = list(range(len(exchanges)))

    ax.barh(y_pos, cagrs, height=0.7, color="#1a5276", alpha=0.8, label="Graham Timing")

    # SPY reference line (average across exchanges)
    avg_spy_cagr = sum(spy_cagrs) / len(spy_cagrs)
    ax.axvline(x=avg_spy_cagr, color="#95a5a6", linestyle="--", linewidth=2,
               label=f"S&P 500 Avg ({avg_spy_cagr:.2f}%)")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(exchanges, fontsize=10)
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Graham Number Timing: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Graham Number timing, quarterly rebalance, 2000-2025",
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
    colors = ["#27ae60" if dd > -20 else "#e67e22" if dd > -30 else "#c0392b"
              for dd in drawdowns]

    ax.barh(y_pos, drawdowns, height=0.7, color=colors, alpha=0.8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(exchanges, fontsize=10)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Graham Number Timing: Maximum Drawdown by Exchange",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    # Color legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor="#27ae60", alpha=0.8, label="Low (<-20%)"),
        Patch(facecolor="#e67e22", alpha=0.8, label="Moderate (-20% to -30%)"),
        Patch(facecolor="#c0392b", alpha=0.8, label="High (>-30%)")
    ]
    ax.legend(handles=legend_elements, fontsize=9, loc="lower right")

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
    "BSE+NSE": "#e67e22",
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
