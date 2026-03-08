"""Generate Piotroski charts from results/piotroski_metrics_US.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "piotroski_metrics_US.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "portfolio": "#1a5276",
    "spy": "#aab7b8",
}


def chart_cumulative():
    """Generate cumulative growth chart."""
    annual_returns = data["annual_returns"]

    # Compute cumulative
    port_vals = [10000]
    spy_vals = [10000]
    years = [annual_returns[0]["year"] - 1]

    for ar in annual_returns:
        port_vals.append(port_vals[-1] * (1 + ar["portfolio"] / 100))
        spy_vals.append(spy_vals[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(years, spy_vals, color=COLORS["spy"], linewidth=1.8,
            label=f"S&P 500 ({data['spy']['cagr']}% CAGR)", linestyle="--")

    cagr = data["portfolio"]["cagr"]
    ax.plot(years, port_vals, color=COLORS["portfolio"], linewidth=2.2,
            label=f"Piotroski F-Score >= 7 ({cagr}% CAGR)")

    # Final values
    ax.annotate(f"${port_vals[-1]/1000:,.0f}K",
                xy=(years[-1], port_vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["portfolio"])

    ax.annotate(f"${spy_vals[-1]/1000:,.0f}K",
                xy=(years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["spy"])

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title("Growth of $10,000: Piotroski F-Score >= 7 vs S&P 500 (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | NYSE + NASDAQ + AMEX, semi-annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "us_cumulative_growth.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars():
    """Generate annual returns bar chart."""
    annual_returns = data["annual_returns"]
    years = [ar["year"] for ar in annual_returns]
    port_returns = [ar["portfolio"] for ar in annual_returns]
    spy_returns = [ar["spy"] for ar in annual_returns]

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.35
    x = list(range(len(years)))

    ax.bar([i - width/2 for i in x], spy_returns, width,
           label="S&P 500", color=COLORS["spy"], alpha=0.7)
    ax.bar([i + width/2 for i in x], port_returns, width,
           label="Piotroski F-Score >= 7", color=COLORS["portfolio"], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title("Piotroski F-Score >= 7: Year-by-Year Returns (2000-2024)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             "Data: Ceta Research | NYSE + NASDAQ + AMEX, semi-annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "us_annual_returns.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate charts
print("Generating Piotroski charts...")
chart_cumulative()
chart_annual_bars()

print(f"\nDone. Charts generated in {charts_dir}/")
