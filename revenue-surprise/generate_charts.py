"""Generate all Revenue Surprise charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "TSX": "#7f8c8d",
    "LSE": "#154360",
    "XETRA": "#27ae60",
    "JPX": "#6e2f1a",
    "TAI_TWO": "#1a252f",
    "HKSE": "#8e44ad",
    "SHZ_SHH": "#c0392b",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Revenue Surprise US",
    "NSE": "Revenue Surprise India",
    "TSX": "Revenue Surprise Canada",
    "LSE": "Revenue Surprise UK",
    "XETRA": "Revenue Surprise Germany",
    "JPX": "Revenue Surprise Japan",
    "TAI_TWO": "Revenue Surprise Taiwan",
    "HKSE": "Revenue Surprise Hong Kong",
    "SHZ_SHH": "Revenue Surprise China",
}


US_KEY = "NYSE_NASDAQ_AMEX"


def bench_label(exchange_key):
    """Name of the index this exchange was actually measured against.

    After the local-benchmark change, each entry's `spy` field holds its own
    local index (Sensex, DAX, Hang Seng...), not the S&P 500. The run records
    which in `benchmark_name`, so never hardcode "S&P 500" on a regional chart.
    """
    return (data.get(exchange_key) or {}).get("benchmark_name") or "S&P 500"


def year_range():
    """Charted period, read from the data rather than hardcoded in titles."""
    ar = data[US_KEY]["annual_returns"]
    return ar[0]["year"], ar[-1]["year"]


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_benchmark_cumulative(exchange_key, initial=10000):
    """Cumulative growth of the benchmark THIS exchange was measured against.

    Never hardcode the US entry here. Each result's `spy` field holds its own
    local index, so reading US data onto a regional chart plots the wrong line
    entirely, not merely the wrong label.
    """
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Cumulative growth for the given exchanges against their own benchmark.

    The reference line is the first exchange's benchmark, so call this with one
    market at a time unless they genuinely share an index.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    ref_key = exchanges[0]
    spy_years, spy_values = get_benchmark_cumulative(ref_key)
    ax.plot(spy_years, spy_values, color=COLORS["SPY"], linewidth=2.5,
            label=bench_label(ref_key), alpha=0.6)

    for ex_key in exchanges:
        ex_years, ex_values = get_cumulative_growth(ex_key)
        ax.plot(ex_years, ex_values, color=COLORS[ex_key], linewidth=2,
                label=EXCHANGE_LABELS.get(ex_key, ex_key))

    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Portfolio Value ($)", fontsize=12)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x/1000:.0f}K"))
    ax.legend(loc="upper left", framealpha=0.9)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(spy_years[0], spy_years[-1])

    footer_text = f"$10,000 invested in {spy_years[0]}, quarterly rebalancing, {footer_universe}"
    plt.figtext(0.5, 0.02, footer_text, ha="center", fontsize=9,
                style="italic", color="#555")

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    plt.savefig(charts_dir / filename, dpi=150, bbox_inches="tight")
    print(f"  Saved: {filename}")
    plt.close()


def chart_annual_returns(exchange_key, filename, title):
    """Generate annual returns bar chart for a single exchange."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 6))

    x = range(len(years))
    width = 0.35

    ax.bar([i - width/2 for i in x], port_returns, width,
           label=EXCHANGE_LABELS.get(exchange_key, exchange_key),
           color=COLORS[exchange_key], alpha=0.8)
    ax.bar([i + width/2 for i in x], spy_returns, width,
           label=bench_label(exchange_key), color=COLORS["SPY"], alpha=0.6)

    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Return (%)", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right")
    ax.axhline(y=0, color='black', linewidth=0.8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{x:.0f}%"))
    ax.legend(loc="upper left", framealpha=0.9)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(charts_dir / filename, dpi=150, bbox_inches="tight")
    print(f"  Saved: {filename}")
    plt.close()


def chart_comparison_cagr():
    """Generate CAGR comparison across all exchanges."""
    exchanges = sorted([k for k in data.keys() if "error" not in data[k]],
                       key=lambda x: data[x]["portfolio"]["cagr"], reverse=True)

    cagr_values = [data[e]["portfolio"]["cagr"] for e in exchanges]
    excess_values = [data[e]["comparison"]["excess_cagr"] for e in exchanges]
    labels = [EXCHANGE_LABELS.get(e, e).replace("Revenue Surprise ", "") for e in exchanges]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # CAGR chart
    colors = [COLORS.get(e, "#333") for e in exchanges]
    ax1.barh(range(len(exchanges)), cagr_values, color=colors, alpha=0.8)
    ax1.set_yticks(range(len(exchanges)))
    ax1.set_yticklabels(labels)
    y0, y1 = year_range()
    ax1.set_xlabel("CAGR (%)", fontsize=12)
    ax1.set_title(f"Revenue Surprise CAGR by Exchange ({y0}-{y1})", fontsize=14, fontweight="bold")
    ax1.axvline(x=0, color='black', linewidth=0.8)
    ax1.grid(True, alpha=0.3, axis='x')

    # Excess CAGR chart. Each bar is excess over that exchange's OWN local
    # index (Sensex, DAX, Hang Seng...), not over the S&P 500.
    ax2.barh(range(len(exchanges)), excess_values,
             color=['#27ae60' if v > 0 else '#e74c3c' for v in excess_values], alpha=0.8)
    ax2.set_yticks(range(len(exchanges)))
    ax2.set_yticklabels(labels)
    ax2.set_xlabel("Excess vs local benchmark (%)", fontsize=12)
    ax2.set_title("Revenue Surprise Excess Return vs Local Benchmark",
                  fontsize=14, fontweight="bold")
    ax2.axvline(x=0, color='black', linewidth=0.8)
    ax2.grid(True, alpha=0.3, axis='x')

    plt.tight_layout()
    plt.savefig(charts_dir / "1_comparison_cagr.png", dpi=150, bbox_inches="tight")
    print("  Saved: 1_comparison_cagr.png")
    plt.close()


def chart_comparison_drawdown():
    """Generate max drawdown comparison across all exchanges."""
    exchanges = sorted([k for k in data.keys() if "error" not in data[k]],
                       key=lambda x: data[x]["portfolio"]["max_drawdown"], reverse=False)

    dd_values = [data[e]["portfolio"]["max_drawdown"] for e in exchanges]
    labels = [EXCHANGE_LABELS.get(e, e).replace("Revenue Surprise ", "") for e in exchanges]

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = [COLORS.get(e, "#333") for e in exchanges]
    ax.barh(range(len(exchanges)), dd_values, color=colors, alpha=0.8)
    ax.set_yticks(range(len(exchanges)))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12)
    ax.set_title("Revenue Surprise Max Drawdown by Exchange", fontsize=14, fontweight="bold")
    ax.axvline(x=0, color='black', linewidth=0.8)
    ax.grid(True, alpha=0.3, axis='x')

    plt.tight_layout()
    plt.savefig(charts_dir / "2_comparison_drawdown.png", dpi=150, bbox_inches="tight")
    print("  Saved: 2_comparison_drawdown.png")
    plt.close()


# Generate charts
print("Generating Revenue Surprise charts...")

# 1. US charts
Y0, Y1 = year_range()
print("\n US (NYSE+NASDAQ+AMEX):")
chart_cumulative([US_KEY], "1_us_cumulative_growth.png",
                 f"Revenue Surprise Momentum: US Growth ({Y0}-{Y1})",
                 "NYSE+NASDAQ+AMEX")
chart_annual_returns(US_KEY, "2_us_annual_returns.png",
                     "Revenue Surprise Momentum: US Annual Returns")

# 2. Comparison charts
print("\n Comparison:")
chart_comparison_cagr()
chart_comparison_drawdown()

print("\nAll charts generated in:", charts_dir)
print("\nMove charts to ts-content-creator/content/_current/momentum-04-revenue-surprise/blogs/")
print("  mv charts/1_us_*.png ../ts-content-creator/content/_current/momentum-04-revenue-surprise/blogs/us/")
print("  mv charts/1_comparison_*.png ../ts-content-creator/content/_current/momentum-04-revenue-surprise/blogs/comparison/")
