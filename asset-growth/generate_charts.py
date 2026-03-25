"""Generate all Asset Growth charts for blog posts from exchange_comparison.json."""
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
    "BSE_NSE": "#e67e22",
    "XETRA": "#27ae60",
    "STO": "#2e86c1",
    "TSX": "#7f8c8d",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "JPX": "#6e2f1a",
    "LSE": "#154360",
    "ASX": "#148f77",
    "KSC": "#6c3483",
    "SAO": "#cb4335",
    "SIX": "#d68910",
    "TAI": "#1a252f",
    "SET": "#5b2c6f",
    "SAU": "#1e8449",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Asset Growth US",
    "BSE_NSE": "Asset Growth India",
    "XETRA": "Asset Growth Germany",
    "STO": "Asset Growth Sweden",
    "TSX": "Asset Growth Canada",
    "SHZ_SHH": "Asset Growth China",
    "HKSE": "Asset Growth HK",
    "JPX": "Asset Growth Japan",
    "LSE": "Asset Growth UK",
    "ASX": "Asset Growth Australia",
    "KSC": "Asset Growth Korea",
    "SAO": "Asset Growth Brazil",
    "SIX": "Asset Growth Switzerland",
    "TAI": "Asset Growth Taiwan",
    "SET": "Asset Growth Thailand",
    "SAU": "Asset Growth Saudi",
}


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(initial=10000):
    """Get SPY cumulative from any exchange."""
    ex = data["NYSE_NASDAQ_AMEX"]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def get_local_benchmark_cumulative(exchange_key, initial=10000):
    """Get local benchmark cumulative from an exchange's own spy field."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


# Local benchmark names for non-US exchanges
LOCAL_BENCHMARK_NAMES = {
    "BSE_NSE": "Sensex",
    "XETRA": "DAX",
    "LSE": "FTSE 100",
    "JPX": "Nikkei 225",
    "TSX": "TSX Composite",
    "SHZ_SHH": "Shanghai Composite",
    "HKSE": "Hang Seng",
    "SAO": "Ibovespa",
    "STO": "OMX Stockholm 30",
    "SIX": "SMI",
    "KSC": "KOSPI",
    "TAI": "TAIEX",
    "SET": "SET Index",
    "SAU": "Tadawul",
    "ASX": "ASX 200",
}


def chart_cumulative(exchanges, filename, title, footer_universe,
                     benchmark_exchange=None, benchmark_label=None):
    """Generate cumulative growth chart for given exchanges vs benchmark.

    If benchmark_exchange is set, use that exchange's local benchmark.
    Otherwise use SPY.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    if benchmark_exchange:
        bench_years, bench_vals = get_local_benchmark_cumulative(benchmark_exchange)
        bench_cagr = data[benchmark_exchange]["spy"]["cagr"]
        bench_name = benchmark_label or LOCAL_BENCHMARK_NAMES.get(benchmark_exchange, "Benchmark")
    else:
        bench_years, bench_vals = get_spy_cumulative()
        bench_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
        bench_name = "S&P 500"

    ax.plot(bench_years, bench_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{bench_name} ({bench_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS[ex_key]} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS[ex_key], linewidth=2.2, label=label)

        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS[ex_key])

    bench_final_k = bench_vals[-1] / 1000
    ax.annotate(f"${bench_final_k:,.0f}K",
                xy=(bench_years[-1], bench_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe,
                      benchmark_label=None):
    """Generate annual returns bar chart."""
    ex = data[exchanges[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]
    bench_name = benchmark_label or "S&P 500"

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=bench_name, color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(exchanges):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS[ex_key], color=COLORS[ex_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Asset Growth CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Asset Growth < -10%, semi-annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate all charts
print("Generating charts for Asset Growth blogs...")

print("US charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "us_cumulative_growth.png",
    "Growth of $10,000: Asset Growth US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "us_annual_returns.png",
    "Asset Growth US: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

print("India charts...")
chart_cumulative(
    ["BSE_NSE"], "india_cumulative_growth.png",
    "Growth of $10,000: Asset Growth India vs S&P 500 (2000-2025)",
    "BSE + NSE (returns in INR)"
)
chart_annual_bars(
    ["BSE_NSE"], "india_annual_returns.png",
    "Asset Growth India: Year-by-Year Returns (2000-2024)",
    "BSE + NSE (returns in INR)"
)

print("UK charts...")
chart_cumulative(
    ["LSE"], "uk_cumulative_growth.png",
    "Growth of $10,000: Asset Growth UK vs FTSE 100 (2000-2025)",
    "LSE (London Stock Exchange)",
    benchmark_exchange="LSE",
)
chart_annual_bars(
    ["LSE"], "uk_annual_returns.png",
    "Asset Growth UK: Year-by-Year Returns (2000-2024)",
    "LSE (London Stock Exchange)",
    benchmark_label="FTSE 100",
)

print("Japan charts...")
chart_cumulative(
    ["JPX"], "japan_cumulative_growth.png",
    "Growth of $10,000: Asset Growth Japan vs Nikkei 225 (2000-2025)",
    "JPX (Tokyo Stock Exchange)",
    benchmark_exchange="JPX",
)
chart_annual_bars(
    ["JPX"], "japan_annual_returns.png",
    "Asset Growth Japan: Year-by-Year Returns (2000-2024)",
    "JPX (Tokyo Stock Exchange)",
    benchmark_label="Nikkei 225",
)

print("Germany charts...")
chart_cumulative(
    ["XETRA"], "germany_cumulative_growth.png",
    "Growth of $10,000: Asset Growth Germany vs DAX (2000-2025)",
    "XETRA (Frankfurt)",
    benchmark_exchange="XETRA",
)
chart_annual_bars(
    ["XETRA"], "germany_annual_returns.png",
    "Asset Growth Germany: Year-by-Year Returns (2000-2024)",
    "XETRA (Frankfurt)",
    benchmark_label="DAX",
)

print("Brazil charts...")
chart_cumulative(
    ["SAO"], "brazil_cumulative_growth.png",
    "Growth of $10,000: Asset Growth Brazil vs Ibovespa (2000-2025)",
    "B3 (Sao Paulo)",
    benchmark_exchange="SAO",
)
chart_annual_bars(
    ["SAO"], "brazil_annual_returns.png",
    "Asset Growth Brazil: Year-by-Year Returns (2000-2024)",
    "B3 (Sao Paulo)",
    benchmark_label="Ibovespa",
)

print("Canada charts...")
chart_cumulative(
    ["TSX"], "canada_cumulative_growth.png",
    "Growth of $10,000: Asset Growth Canada vs TSX Composite (2000-2025)",
    "TSX (Toronto)",
    benchmark_exchange="TSX",
)
chart_annual_bars(
    ["TSX"], "canada_annual_returns.png",
    "Asset Growth Canada: Year-by-Year Returns (2000-2024)",
    "TSX (Toronto)",
    benchmark_label="TSX Composite",
)

print("Sweden charts...")
chart_cumulative(
    ["STO"], "sweden_cumulative_growth.png",
    "Growth of $10,000: Asset Growth Sweden vs OMX Stockholm 30 (2000-2025)",
    "Nasdaq Stockholm",
    benchmark_exchange="STO",
)
chart_annual_bars(
    ["STO"], "sweden_annual_returns.png",
    "Asset Growth Sweden: Year-by-Year Returns (2000-2024)",
    "Nasdaq Stockholm",
    benchmark_label="OMX Stockholm 30",
)

print("Switzerland charts...")
chart_cumulative(
    ["SIX"], "switzerland_cumulative_growth.png",
    "Growth of $10,000: Asset Growth Switzerland vs SMI (2000-2025)",
    "SIX (Zurich)",
    benchmark_exchange="SIX",
)
chart_annual_bars(
    ["SIX"], "switzerland_annual_returns.png",
    "Asset Growth Switzerland: Year-by-Year Returns (2000-2024)",
    "SIX (Zurich)",
    benchmark_label="SMI",
)

print("Comparison charts...")
chart_comparison_cagr("comparison_cagr.png")

print(f"\nDone. Charts generated in {charts_dir}/")
