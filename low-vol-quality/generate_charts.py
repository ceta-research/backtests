"""Generate all Low Volatility + Quality charts for blog posts from exchange_comparison.json."""
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
    "STO": "#2e86c1",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI": "#1a252f",
    "LSE": "#154360",
    "KSC": "#6c3483",
    "JPX": "#6e2f1a",
    "SIX": "#d68910",
    "XETRA": "#27ae60",
    "SET": "#5b2c6f",
    "SAU": "#1e8449",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Low Vol + Quality US",
    "NSE": "Low Vol + Quality India",
    "TSX": "Low Vol + Quality Canada",
    "STO": "Low Vol + Quality Sweden",
    "SHZ_SHH": "Low Vol + Quality China",
    "HKSE": "Low Vol + Quality HK",
    "TAI": "Low Vol + Quality Taiwan",
    "LSE": "Low Vol + Quality UK",
    "KSC": "Low Vol + Quality Korea",
    "JPX": "Low Vol + Quality Japan",
    "SIX": "Low Vol + Quality Switzerland",
    "XETRA": "Low Vol + Quality Germany",
    "SET": "Low Vol + Quality Thailand",
    "SAU": "Low Vol + Quality Saudi",
}

EXCHANGE_UNIVERSE_LABELS = {
    "NYSE_NASDAQ_AMEX": "NYSE + NASDAQ + AMEX",
    "NSE": "NSE (returns in INR)",
    "TSX": "TSX (returns in CAD)",
    "STO": "STO (returns in SEK)",
    "SHZ_SHH": "SHZ + SHH (returns in CNY)",
    "HKSE": "HKSE (returns in HKD)",
    "TAI": "TAI (returns in TWD)",
    "LSE": "LSE (returns in GBP)",
    "KSC": "KSC (returns in KRW)",
    "JPX": "JPX (returns in JPY)",
    "SIX": "SIX (returns in CHF)",
    "XETRA": "XETRA (returns in EUR)",
    "SET": "SET (returns in THB)",
    "SAU": "SAU (returns in SAR)",
}

# Comparison chart display names (shorter)
COMPARISON_LABELS = {
    "NYSE_NASDAQ_AMEX": "US",
    "NSE": "India",
    "TSX": "Canada",
    "STO": "Sweden",
    "SHZ_SHH": "China",
    "HKSE": "Hong Kong",
    "TAI": "Taiwan",
    "LSE": "UK",
    "KSC": "Korea",
    "JPX": "Japan",
    "SIX": "Switzerland",
    "XETRA": "Germany",
    "SET": "Thailand",
    "SAU": "Saudi",
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


def get_spy_cumulative(ref_key="NYSE_NASDAQ_AMEX", initial=10000):
    """Get SPY cumulative from the US exchange data."""
    ex = data[ref_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchange_key, filename, title):
    """Generate cumulative growth chart for one exchange vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    ref_key = "NYSE_NASDAQ_AMEX" if "NYSE_NASDAQ_AMEX" in data else list(data.keys())[0]
    spy_years, spy_vals = get_spy_cumulative(ref_key)
    spy_cagr = data[ref_key]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    years, vals = get_cumulative_growth(exchange_key)
    ex = data[exchange_key]
    cagr = ex["portfolio"]["cagr"]
    label = f"{EXCHANGE_LABELS[exchange_key]} ({cagr}% CAGR)"
    ax.plot(years, vals, color=COLORS[exchange_key], linewidth=2.2, label=label)

    final_k = vals[-1] / 1000
    ax.annotate(f"${final_k:,.0f}K",
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS[exchange_key])

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(f"${spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    universe_label = EXCHANGE_UNIVERSE_LABELS.get(exchange_key, exchange_key)
    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {universe_label}, quarterly rebalance (Jan/Apr/Jul/Oct), 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, title):
    """Generate annual returns bar chart for one exchange."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.35
    x = list(range(len(years)))

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_returns, width,
           label=EXCHANGE_LABELS[exchange_key], color=COLORS[exchange_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    universe_label = EXCHANGE_UNIVERSE_LABELS.get(exchange_key, exchange_key)
    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {universe_label}, quarterly rebalance (Jan/Apr/Jul/Oct), 2000-2025",
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
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [COMPARISON_LABELS.get(k, k) for k, v in exchanges_with_data]
    keys = [k for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    ref_key = "NYSE_NASDAQ_AMEX" if "NYSE_NASDAQ_AMEX" in data else list(data.keys())[0]
    spy_cagr = data[ref_key]["spy"]["cagr"]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.55)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Low Vol + Quality: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | ROE > 10%, OPM > 10%, lowest 252d vol, top 30, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [COMPARISON_LABELS.get(k, k) for k, v in exchanges_with_data]
    keys = [k for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    ref_key = "NYSE_NASDAQ_AMEX" if "NYSE_NASDAQ_AMEX" in data else list(data.keys())[0]
    spy_dd = data[ref_key]["spy"]["max_drawdown"]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.55)))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}% MaxDD)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Low Vol + Quality: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, dd in enumerate(drawdowns):
        ax.text(dd + 0.3, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | ROE > 10%, OPM > 10%, lowest 252d vol, top 30, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# --- Generate all charts ---
print("Generating charts for Low Vol + Quality blogs...")

# Regional charts for blog exchanges
BLOG_EXCHANGES = {
    "NYSE_NASDAQ_AMEX": ("us", "US"),
    "NSE": ("india", "India"),
    "TSX": ("canada", "Canada"),
    "LSE": ("uk", "UK"),
    "JPX": ("japan", "Japan"),
    "SIX": ("switzerland", "Switzerland"),
    "TAI": ("taiwan", "Taiwan"),
}

for key, (slug, name) in BLOG_EXCHANGES.items():
    if key in data:
        print(f"\n{name} charts...")
        chart_cumulative(
            key,
            f"1_{slug}_cumulative_growth.png",
            f"Growth of $10,000: Low Vol + Quality {name} vs S&P 500 (2000-2025)"
        )
        chart_annual_bars(
            key,
            f"2_{slug}_annual_returns.png",
            f"Low Vol + Quality {name}: Year-by-Year Returns (2000-2024)"
        )

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
