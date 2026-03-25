#!/usr/bin/env python3
"""Generate all Net Debt/EBITDA charts for blog posts from exchange_comparison.json."""
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
    "STO": "#3498db",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI": "#d35400",
    "KSC": "#95a5a6",
    "SET": "#2ecc71",
    "XETRA": "#27ae60",
    "SIX": "#1abc9c",
    "TSX": "#7f8c8d",
    "ASX": "#f39c12",
    "SAO": "#e74c3c",
    "OSL": "#17a589",
    "SES": "#a93226",
    "MIL": "#5d6d7e",
    "TLV": "#9b59b6",
    "AMS": "#2e86c1",
    "BME": "#cb4335",
    "SAU": "#d4ac0d",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Net Debt/EBITDA US",
    "NSE": "Net Debt/EBITDA India",
    "STO": "Net Debt/EBITDA Sweden",
    "SHZ_SHH": "Net Debt/EBITDA China",
    "HKSE": "Net Debt/EBITDA Hong Kong",
    "TAI": "Net Debt/EBITDA Taiwan",
    "KSC": "Net Debt/EBITDA Korea",
    "SET": "Net Debt/EBITDA Thailand",
    "XETRA": "Net Debt/EBITDA Germany",
    "SIX": "Net Debt/EBITDA Switzerland",
    "TSX": "Net Debt/EBITDA Canada",
    "ASX": "Net Debt/EBITDA Australia",
    "SAO": "Net Debt/EBITDA Brazil",
    "OSL": "Net Debt/EBITDA Norway",
    "SES": "Net Debt/EBITDA Singapore",
    "MIL": "Net Debt/EBITDA Italy",
    "TLV": "Net Debt/EBITDA Israel",
    "AMS": "Net Debt/EBITDA Netherlands",
    "BME": "Net Debt/EBITDA Spain",
    "SAU": "Net Debt/EBITDA Saudi Arabia",
}

FOOTER = "Data: Ceta Research | Net Debt/EBITDA <2x, ROE >10%, MCap >$1B, top 30 by lowest ratio, quarterly rebalance, equal weight, 2000-2025"


def get_cumulative_growth(exchange_key, initial=10000):
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(initial=10000):
    ex = data["NYSE_NASDAQ_AMEX"]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def format_k(val, pos):
    return f"${val/1000:,.0f}K"


def chart_cumulative_single(exchange_key, filename, title_suffix=""):
    """Cumulative growth chart for a single exchange vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    spy_years, spy_vals = get_spy_cumulative()
    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--", zorder=2)

    years, vals = get_cumulative_growth(exchange_key)
    ex = data[exchange_key]
    cagr = ex["portfolio"]["cagr"]
    label = f"{EXCHANGE_LABELS.get(exchange_key, exchange_key)} ({cagr}% CAGR)"
    color = COLORS.get(exchange_key, "#1a5276")
    ax.plot(years, vals, color=color, linewidth=2.4, label=label, zorder=3)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(format_k))
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Portfolio Value ($10,000 initial)", fontsize=11)
    ax.set_title(f"Net Debt/EBITDA Strategy vs S&P 500{title_suffix}\n$10,000 initial investment, 2000–2025",
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10, loc="upper left")
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def chart_annual_returns(exchange_key, filename, title_suffix=""):
    """Annual returns bar chart for a single exchange vs SPY."""
    ex = data[exchange_key]
    years = [r["year"] for r in ex["annual_returns"]]
    portfolio = [r["portfolio"] for r in ex["annual_returns"]]
    spy = [r["spy"] for r in ex["annual_returns"]]

    x = range(len(years))
    width = 0.38
    color = COLORS.get(exchange_key, "#1a5276")

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    ax.bar([i - width/2 for i in x], portfolio, width, label="Net Debt/EBITDA Strategy",
           color=color, alpha=0.85)
    ax.bar([i + width/2 for i in x], spy, width, label="S&P 500",
           color=COLORS["SPY"], alpha=0.85)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.set_title(f"Annual Returns: Net Debt/EBITDA Strategy vs S&P 500{title_suffix}",
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.04, FOOTER, ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def chart_comparison_cagr():
    """CAGR bar chart across all exchanges."""
    exchange_data = []
    for key, val in data.items():
        exchange_data.append({
            "key": key,
            "cagr": val["portfolio"]["cagr"],
            "excess": val["comparison"]["excess_cagr"],
        })
    exchange_data.sort(key=lambda x: x["cagr"], reverse=True)

    labels = [e["key"].replace("_", "\n") for e in exchange_data]
    cagrs = [e["cagr"] for e in exchange_data]
    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]

    colors = ["#27ae60" if e["excess"] > 0 else "#c0392b" for e in exchange_data]

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    bars = ax.bar(range(len(labels)), cagrs, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.axhline(spy_cagr, color="#1a5276", linewidth=2, linestyle="--",
               label=f"S&P 500 CAGR ({spy_cagr}%)", zorder=5)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                f"{cagr}%", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("CAGR (%)", fontsize=11)
    ax.set_title("Net Debt/EBITDA Strategy CAGR: 20 Exchanges (2000–2025)\nGreen = beats S&P 500 | Red = underperforms",
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.04, FOOTER, ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / "1_comparison_cagr.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: 1_comparison_cagr.png")


def chart_comparison_drawdown():
    """Max drawdown comparison across all exchanges."""
    exchange_data = []
    for key, val in data.items():
        exchange_data.append({
            "key": key,
            "drawdown": val["portfolio"]["max_drawdown"],
            "spy_dd": val["spy"]["max_drawdown"],
        })
    exchange_data.sort(key=lambda x: x["drawdown"])

    labels = [e["key"].replace("_", "\n") for e in exchange_data]
    dds = [e["drawdown"] for e in exchange_data]
    spy_dd = exchange_data[0]["spy_dd"]  # SPY max drawdown (same for all)

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    ax.bar(range(len(labels)), dds, color="#c0392b", alpha=0.75, edgecolor="white", linewidth=0.5)
    ax.axhline(spy_dd, color="#1a5276", linewidth=2, linestyle="--",
               label=f"S&P 500 Max Drawdown ({spy_dd}%)")

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Max Drawdown (%)", fontsize=11)
    ax.set_title("Max Drawdown: Net Debt/EBITDA Strategy vs S&P 500 Across 20 Exchanges",
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.04, FOOTER, ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / "2_comparison_drawdown.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: 2_comparison_drawdown.png")


if __name__ == "__main__":
    print("Generating Net Debt/EBITDA charts...")

    # US charts
    print("\nUS:")
    chart_cumulative_single("NYSE_NASDAQ_AMEX", "1_us_cumulative_growth.png", " (US)")
    chart_annual_returns("NYSE_NASDAQ_AMEX", "2_us_annual_returns.png", " (US)")

    # India charts
    print("\nIndia:")
    chart_cumulative_single("NSE", "1_india_cumulative_growth.png", " (India: NSE)")
    chart_annual_returns("NSE", "2_india_annual_returns.png", " (India: NSE)")

    # Sweden charts
    print("\nSweden:")
    chart_cumulative_single("STO", "1_sweden_cumulative_growth.png", " (Sweden: STO)")
    chart_annual_returns("STO", "2_sweden_annual_returns.png", " (Sweden: STO)")

    # Comparison charts
    print("\nComparison:")
    chart_comparison_cagr()
    chart_comparison_drawdown()

    print(f"\nAll charts saved to: {charts_dir}")
    print("Copy to content directory:")
    print("  cp charts/1_us_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/us/")
    print("  cp charts/2_us_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/us/")
    print("  cp charts/1_india_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/india/")
    print("  cp charts/2_india_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/india/")
    print("  cp charts/1_sweden_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/sweden/")
    print("  cp charts/2_sweden_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/sweden/")
    print("  cp charts/1_comparison* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/comparison/")
    print("  cp charts/2_comparison* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/comparison/")
