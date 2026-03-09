"""Generate all 52-Week Low Quality charts for blog posts from exchange_comparison.json."""
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
    "SIX": "#d68910",
    "KSC": "#6c3483",
    "TAI_TWO": "#1a252f",
    "SES": "#148f77",
    "OSL": "#6e2f1a",
    "JNB": "#cb4335",
    "SET": "#5b2c6f",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "52W Low US",
    "BSE_NSE": "52W Low India",
    "XETRA": "52W Low Germany",
    "STO": "52W Low Sweden",
    "TSX": "52W Low Canada",
    "SHZ_SHH": "52W Low China",
    "HKSE": "52W Low Hong Kong",
    "SIX": "52W Low Switzerland",
    "KSC": "52W Low Korea",
    "TAI_TWO": "52W Low Taiwan",
    "SES": "52W Low Singapore",
    "OSL": "52W Low Norway",
    "JNB": "52W Low South Africa",
    "SET": "52W Low Thailand",
}

EXCHANGE_NAMES = {
    "NYSE_NASDAQ_AMEX": "NYSE + NASDAQ + AMEX",
    "BSE_NSE": "BSE + NSE (returns in INR)",
    "XETRA": "XETRA (returns in EUR)",
    "STO": "Stockholm (returns in SEK)",
    "TSX": "TSX (returns in CAD)",
    "SHZ_SHH": "Shenzhen + Shanghai (returns in CNY)",
    "HKSE": "HKSE (returns in HKD)",
    "SIX": "SIX Swiss Exchange (returns in CHF)",
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


def get_spy_cumulative(exchange_key="NYSE_NASDAQ_AMEX", initial=10000):
    """Get SPY cumulative from any exchange (all have same SPY series)."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchange_key, filename, title, footer_universe):
    """Generate cumulative growth chart for one exchange vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative(exchange_key)
    spy_cagr = data[exchange_key]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    ex = data[exchange_key]
    years, vals = get_cumulative_growth(exchange_key)
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

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, 2002-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, title, footer_universe):
    """Generate annual returns bar chart for one exchange vs SPY."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.38
    x = list(range(len(years)))

    ax.bar([i - width / 2 for i in x], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)
    ax.bar([i + width / 2 for i in x], port_returns, width,
           label=EXCHANGE_LABELS[exchange_key], color=COLORS[exchange_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, 2002-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange (excluding PAR)."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and k != "PAR"
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
    ax.axvline(x=0, color="#333333", linewidth=0.8, linestyle="-")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("52-Week Low Quality CAGR by Exchange (2002-2025)", fontsize=14,
                 fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = cagr + 0.2 if cagr >= 0 else cagr - 1.5
        ax.text(max(cagr, 0) + 0.2, i, f"{cagr:.1f}%", va="center", fontsize=10,
                fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Price within 15% of 52w low + Piotroski F-score >= 7, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange (excluding PAR)."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and k != "PAR"
           and v["portfolio"]["sharpe_ratio"] is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = data["NYSE_NASDAQ_AMEX"]["spy"]["sharpe_ratio"]
    ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 Sharpe ({spy_sharpe:.3f})")
    ax.axvline(x=0, color="#333333", linewidth=0.8, linestyle="-")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("52-Week Low Quality: Sharpe Ratio by Exchange (2002-2025)", fontsize=14,
                 fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, s in enumerate(sharpes):
        x_pos = max(s, 0) + 0.01
        ax.text(x_pos, i, f"{s:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Price within 15% of 52w low + Piotroski F-score >= 7, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate all charts
print("Generating charts for 52-Week Low Quality blogs...")

print("\nUS charts...")
chart_cumulative(
    "NYSE_NASDAQ_AMEX", "us_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality US vs S&P 500 (2002-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    "NYSE_NASDAQ_AMEX", "us_annual_returns.png",
    "52-Week Low Quality US: Year-by-Year Returns (2002-2025)",
    "NYSE + NASDAQ + AMEX"
)

print("\nGermany (XETRA) charts...")
chart_cumulative(
    "XETRA", "germany_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality Germany vs S&P 500 (2002-2025)",
    "XETRA"
)
chart_annual_bars(
    "XETRA", "germany_annual_returns.png",
    "52-Week Low Quality Germany: Year-by-Year Returns (2002-2025)",
    "XETRA"
)

print("\nIndia charts...")
chart_cumulative(
    "BSE_NSE", "india_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality India vs S&P 500 (2002-2025)",
    "BSE + NSE (returns in INR)"
)
chart_annual_bars(
    "BSE_NSE", "india_annual_returns.png",
    "52-Week Low Quality India: Year-by-Year Returns (2002-2025)",
    "BSE + NSE (returns in INR)"
)

print("\nCanada charts...")
chart_cumulative(
    "TSX", "canada_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality Canada vs S&P 500 (2002-2025)",
    "TSX (returns in CAD)"
)
chart_annual_bars(
    "TSX", "canada_annual_returns.png",
    "52-Week Low Quality Canada: Year-by-Year Returns (2002-2025)",
    "TSX (returns in CAD)"
)

print("\nChina charts...")
chart_cumulative(
    "SHZ_SHH", "china_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality China vs S&P 500 (2002-2025)",
    "Shenzhen + Shanghai (returns in CNY)"
)
chart_annual_bars(
    "SHZ_SHH", "china_annual_returns.png",
    "52-Week Low Quality China: Year-by-Year Returns (2002-2025)",
    "Shenzhen + Shanghai (returns in CNY)"
)

print("\nSwitzerland charts...")
chart_cumulative(
    "SIX", "switzerland_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality Switzerland vs S&P 500 (2002-2025)",
    "SIX Swiss Exchange (returns in CHF)"
)
chart_annual_bars(
    "SIX", "switzerland_annual_returns.png",
    "52-Week Low Quality Switzerland: Year-by-Year Returns (2002-2025)",
    "SIX Swiss Exchange (returns in CHF)"
)

print("\nHong Kong charts...")
chart_cumulative(
    "HKSE", "hongkong_cumulative_growth.png",
    "Growth of $10,000: 52-Week Low Quality Hong Kong vs S&P 500 (2002-2025)",
    "HKSE (returns in HKD)"
)
chart_annual_bars(
    "HKSE", "hongkong_annual_returns.png",
    "52-Week Low Quality Hong Kong: Year-by-Year Returns (2002-2025)",
    "HKSE (returns in HKD)"
)

print("\nComparison charts...")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_sharpe("comparison_sharpe.png")

print(f"\nDone. {len(list(charts_dir.glob('*.png')))} charts generated in {charts_dir}/")
