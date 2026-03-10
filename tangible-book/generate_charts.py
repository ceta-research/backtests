"""Generate all P/TBV (Tangible Book) charts for blog posts from exchange_comparison.json."""
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
    "JPX": "#6e2f1a",
    "LSE": "#154360",
    "XETRA": "#27ae60",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI_TWO": "#1a252f",
    "SET": "#5b2c6f",
    "TSX": "#7f8c8d",
    "STO": "#2e86c1",
    "SIX": "#d68910",
    "KSC": "#6c3483",
    "JNB": "#1e8449",
    "SES": "#17a589",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "P/TBV US",
    "BSE_NSE": "P/TBV India",
    "JPX": "P/TBV Japan",
    "LSE": "P/TBV UK",
    "XETRA": "P/TBV Germany",
    "SHZ_SHH": "P/TBV China",
    "HKSE": "P/TBV HK",
    "TAI_TWO": "P/TBV Taiwan",
    "SET": "P/TBV Thailand",
    "TSX": "P/TBV Canada",
    "STO": "P/TBV Sweden",
    "SIX": "P/TBV Switzerland",
    "KSC": "P/TBV Korea",
    "JNB": "P/TBV S. Africa",
    "SES": "P/TBV Singapore",
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
    """Get SPY cumulative from US exchange data."""
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
        if ex_key not in data:
            continue
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
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
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
        if ex_key not in data:
            continue
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
        if not v.get("error") and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.5 + 2)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Price-to-Tangible-Book CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | P/TBV < median, ROE > 8%, ROA > 3%, OPM > 10%, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate all charts
print("Generating charts for Price-to-Tangible-Book blogs...")

print("US charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: P/TBV Strategy vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "P/TBV US: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

print("India charts...")
chart_cumulative(
    ["BSE_NSE"], "1_india_cumulative_growth.png",
    "Growth of $10,000: P/TBV India vs S&P 500 (2000-2025)",
    "BSE + NSE"
)
chart_annual_bars(
    ["BSE_NSE"], "2_india_annual_returns.png",
    "P/TBV India: Year-by-Year Returns (2002-2024)",
    "BSE + NSE"
)

print("UK charts...")
chart_cumulative(
    ["LSE"], "1_uk_cumulative_growth.png",
    "Growth of $10,000: P/TBV UK vs S&P 500 (2000-2025)",
    "London Stock Exchange (LSE)"
)
chart_annual_bars(
    ["LSE"], "2_uk_annual_returns.png",
    "P/TBV UK: Year-by-Year Returns (2000-2024)",
    "London Stock Exchange (LSE)"
)

print("Germany charts...")
chart_cumulative(
    ["XETRA"], "1_germany_cumulative_growth.png",
    "Growth of $10,000: P/TBV Germany vs S&P 500 (2000-2025)",
    "XETRA"
)
chart_annual_bars(
    ["XETRA"], "2_germany_annual_returns.png",
    "P/TBV Germany: Year-by-Year Returns (2000-2024)",
    "XETRA"
)

print("China charts...")
chart_cumulative(
    ["SHZ_SHH"], "1_china_cumulative_growth.png",
    "Growth of $10,000: P/TBV China vs S&P 500 (2000-2025)",
    "Shenzhen + Shanghai"
)
chart_annual_bars(
    ["SHZ_SHH"], "2_china_annual_returns.png",
    "P/TBV China: Year-by-Year Returns (2000-2024)",
    "Shenzhen + Shanghai"
)

print("Sweden charts...")
chart_cumulative(
    ["STO"], "1_sweden_cumulative_growth.png",
    "Growth of $10,000: P/TBV Sweden vs S&P 500 (2000-2025)",
    "Stockholm Stock Exchange (STO)"
)
chart_annual_bars(
    ["STO"], "2_sweden_annual_returns.png",
    "P/TBV Sweden: Year-by-Year Returns (2004-2024)",
    "Stockholm Stock Exchange (STO)"
)

print("Canada charts...")
chart_cumulative(
    ["TSX"], "1_canada_cumulative_growth.png",
    "Growth of $10,000: P/TBV Canada vs S&P 500 (2000-2025)",
    "Toronto Stock Exchange (TSX)"
)
chart_annual_bars(
    ["TSX"], "2_canada_annual_returns.png",
    "P/TBV Canada: Year-by-Year Returns (2000-2024)",
    "Toronto Stock Exchange (TSX)"
)

print("Comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")

print(f"\nDone. Charts generated in {charts_dir}/")
