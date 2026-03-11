"""Generate all R&D Efficiency charts for blog posts from exchange_comparison.json."""
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
    "US_MAJOR": "#1a5276",
    "India": "#e67e22",
    "XETRA": "#27ae60",
    "STO": "#2e86c1",
    "China": "#c0392b",
    "HKSE": "#8e44ad",
    "JPX": "#6e2f1a",
    "LSE": "#154360",
    "KSC": "#6c3483",
    "SIX": "#d68910",
    "Taiwan": "#1a252f",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "R&D Efficiency US",
    "India": "R&D Efficiency India",
    "XETRA": "R&D Efficiency Germany",
    "STO": "R&D Efficiency Sweden",
    "China": "R&D Efficiency China",
    "HKSE": "R&D Efficiency HK",
    "JPX": "R&D Efficiency Japan",
    "LSE": "R&D Efficiency UK",
    "KSC": "R&D Efficiency Korea",
    "SIX": "R&D Efficiency Switzerland",
    "Taiwan": "R&D Efficiency Taiwan",
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


def get_spy_cumulative(reference_key="US_MAJOR", initial=10000):
    """Get SPY cumulative from reference exchange."""
    ex = data[reference_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, ref_key=None):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    ref = ref_key or exchanges[0]
    spy_years, spy_vals = get_spy_cumulative(ref)
    spy_cagr = data[ref]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)

        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

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


def chart_annual_bars(exchange_key, filename, title, footer_universe):
    """Generate annual returns bar chart for a single exchange."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))
    x = list(range(len(years)))
    width = 0.35

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_returns, width,
           label=EXCHANGE_LABELS.get(exchange_key, exchange_key),
           color=COLORS.get(exchange_key, "#1a5276"), alpha=0.85)

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
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
        and v.get("portfolio", {}).get("cagr") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    ref_key = "US_MAJOR" if "US_MAJOR" in data else names[0]
    spy_cagr = data[ref_key]["spy"]["cagr"]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.6)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([EXCHANGE_LABELS.get(n, n) for n in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("R&D Efficiency CAGR by Exchange (2000-2025)", fontsize=14,
                 fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | R&D/Rev 2-30%, Gross Margin > 40%, ROE > 10%, annual rebalance",
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
        and v.get("portfolio", {}).get("max_drawdown") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])

    names = [k for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    ref_key = "US_MAJOR" if "US_MAJOR" in data else names[0]
    spy_dd = data[ref_key]["spy"]["max_drawdown"]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.6)))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}% max DD)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([EXCHANGE_LABELS.get(n, n) for n in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("R&D Efficiency Max Drawdown by Exchange (2000-2025)", fontsize=14,
                 fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, dd in enumerate(drawdowns):
        x_pos = min(dd, 0) - 1
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", ha="right", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | R&D/Rev 2-30%, Gross Margin > 40%, ROE > 10%, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate all charts
print("Generating charts for R&D Efficiency blogs...")

if "US_MAJOR" in data:
    print("US charts...")
    chart_cumulative(
        ["US_MAJOR"], "us_cumulative_growth.png",
        "Growth of $10,000: R&D Efficiency US vs S&P 500 (2000-2025)",
        "NYSE + NASDAQ + AMEX"
    )
    chart_annual_bars(
        "US_MAJOR", "us_annual_returns.png",
        "R&D Efficiency US: Year-by-Year Returns (2000-2024)",
        "NYSE + NASDAQ + AMEX"
    )

if "XETRA" in data:
    print("Germany charts...")
    chart_cumulative(
        ["XETRA"], "germany_cumulative_growth.png",
        "Growth of $10,000: R&D Efficiency Germany vs S&P 500 (2000-2025)",
        "XETRA"
    )
    chart_annual_bars(
        "XETRA", "germany_annual_returns.png",
        "R&D Efficiency Germany: Year-by-Year Returns (2000-2024)",
        "XETRA"
    )

if "LSE" in data:
    print("UK charts...")
    chart_cumulative(
        ["LSE"], "uk_cumulative_growth.png",
        "Growth of $10,000: R&D Efficiency UK vs S&P 500 (2000-2025)",
        "LSE"
    )
    chart_annual_bars(
        "LSE", "uk_annual_returns.png",
        "R&D Efficiency UK: Year-by-Year Returns (2000-2024)",
        "LSE"
    )

if "India" in data:
    print("India charts...")
    chart_cumulative(
        ["India"], "india_cumulative_growth.png",
        "Growth of $10,000: R&D Efficiency India vs S&P 500 (2000-2025)",
        "BSE + NSE (returns in INR)"
    )
    chart_annual_bars(
        "India", "india_annual_returns.png",
        "R&D Efficiency India: Year-by-Year Returns (2000-2024)",
        "BSE + NSE (returns in INR)"
    )

if "JPX" in data:
    print("Japan charts...")
    chart_cumulative(
        ["JPX"], "japan_cumulative_growth.png",
        "Growth of $10,000: R&D Efficiency Japan vs S&P 500 (2000-2025)",
        "JPX (returns in JPY)"
    )
    chart_annual_bars(
        "JPX", "japan_annual_returns.png",
        "R&D Efficiency Japan: Year-by-Year Returns (2000-2024)",
        "JPX (returns in JPY)"
    )

print("Comparison charts...")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_drawdown("comparison_drawdown.png")

print(f"\nDone. Charts in {charts_dir}/")
