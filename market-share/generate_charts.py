"""Generate all Market Share Gain charts for blog posts from exchange_comparison.json."""
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
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Market Share US",
    "BSE_NSE": "Market Share India",
    "XETRA": "Market Share Germany",
    "STO": "Market Share Sweden",
    "TSX": "Market Share Canada",
    "SHZ_SHH": "Market Share China",
    "HKSE": "Market Share HK",
    "JPX": "Market Share Japan",
    "LSE": "Market Share UK",
    "ASX": "Market Share Australia",
    "KSC": "Market Share Korea",
    "SAO": "Market Share Brazil",
    "SIX": "Market Share Switzerland",
    "TAI": "Market Share Taiwan",
    "SET": "Market Share Thailand",
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


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative()
    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

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

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(f"${spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
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
             f"Data: Ceta Research | {footer_universe}, annual rebalance (July), 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart."""
    ex = data[exchanges[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)

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
             f"Data: Ceta Research | {footer_universe}, annual rebalance (July), 2000-2025",
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
        if v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Market Share Gain CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.15
        color = "red" if cagr < 0 else "black"
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10,
                fontweight="bold", color=color)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Excess Rev Growth >=10pp vs sector median, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_regime(exchange_key, filename, title, footer_universe, split_year=2010):
    """Before/after regime comparison bar chart."""
    ex = data[exchange_key]
    annual = ex["annual_returns"]

    before = [ar for ar in annual if ar["year"] < split_year]
    after = [ar for ar in annual if ar["year"] >= split_year]

    def avg(lst, key):
        vals = [x[key] for x in lst if x.get("portfolio", 0) != 0 or key != "portfolio"]
        return sum(vals) / len(vals) if vals else 0

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    periods = [f"2000-{split_year-1}", f"{split_year}-2024"]
    port_avgs = [avg(before, "portfolio"), avg(after, "portfolio")]
    spy_avgs = [avg(before, "spy"), avg(after, "spy")]

    x = [0, 1]
    width = 0.35
    for ax, period, port_avg, spy_avg in zip(axes, periods, port_avgs, spy_avgs):
        ax.bar([-width/2], [spy_avg], width, label="S&P 500", color=COLORS["SPY"], alpha=0.7)
        ax.bar([width/2], [port_avg], width,
               label=EXCHANGE_LABELS[exchange_key], color=COLORS[exchange_key], alpha=0.85)
        ax.set_title(period, fontsize=12, fontweight="bold")
        ax.set_ylabel("Avg Annual Return (%)")
        ax.axhline(0, color="black", linewidth=0.5)
        ax.legend(fontsize=9)
        ax.set_xticks([])

    fig.suptitle(title, fontsize=14, fontweight="bold")
    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate all charts
print("Generating charts for Market Share Gain blogs...")

print("US charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "us_cumulative_growth.png",
    "Growth of $10,000: Market Share Gainers US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "us_annual_returns.png",
    "Market Share Gainers US: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)
chart_regime(
    "NYSE_NASDAQ_AMEX", "us_regime.png",
    "Market Share US: Alpha Regime Shift (2000-2009 vs 2010-2024)",
    "NYSE + NASDAQ + AMEX"
)

print("India charts...")
chart_cumulative(
    ["BSE_NSE"], "india_cumulative_growth.png",
    "Growth of $10,000: Market Share Gainers India vs S&P 500 (2000-2025)",
    "BSE + NSE (returns in INR)"
)
chart_annual_bars(
    ["BSE_NSE"], "india_annual_returns.png",
    "Market Share Gainers India: Year-by-Year Returns (2000-2024)",
    "BSE + NSE (returns in INR)"
)

print("Canada charts...")
chart_cumulative(
    ["TSX"], "canada_cumulative_growth.png",
    "Growth of $10,000: Market Share Gainers Canada vs S&P 500 (2000-2025)",
    "TSX"
)
chart_annual_bars(
    ["TSX"], "canada_annual_returns.png",
    "Market Share Gainers Canada: Year-by-Year Returns (2000-2024)",
    "TSX"
)

print("UK charts...")
chart_cumulative(
    ["LSE"], "uk_cumulative_growth.png",
    "Growth of $10,000: Market Share Gainers UK vs S&P 500 (2000-2025)",
    "LSE"
)
chart_annual_bars(
    ["LSE"], "uk_annual_returns.png",
    "Market Share Gainers UK: Year-by-Year Returns (2000-2024)",
    "LSE"
)

print("Germany charts...")
chart_cumulative(
    ["XETRA"], "germany_cumulative_growth.png",
    "Growth of $10,000: Market Share Gainers Germany vs S&P 500 (2000-2025)",
    "XETRA"
)
chart_annual_bars(
    ["XETRA"], "germany_annual_returns.png",
    "Market Share Gainers Germany: Year-by-Year Returns (2000-2024)",
    "XETRA"
)

print("Switzerland charts...")
chart_cumulative(
    ["SIX"], "switzerland_cumulative_growth.png",
    "Growth of $10,000: Market Share Gainers Switzerland vs S&P 500 (2000-2025)",
    "SIX Swiss Exchange"
)
chart_annual_bars(
    ["SIX"], "switzerland_annual_returns.png",
    "Market Share Gainers Switzerland: Year-by-Year Returns (2000-2024)",
    "SIX Swiss Exchange"
)

print("Comparison charts...")
chart_comparison_cagr("comparison_cagr.png")

print(f"\nDone. Charts generated in {charts_dir}/")
