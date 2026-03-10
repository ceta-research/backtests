"""Generate charts for Sector P/E Compression blog post.

Run from the backtests/ directory:
    python3 sector-pe-compression/generate_charts.py

Charts are saved to backtests/sector-pe-compression/charts/ then moved to:
    ts-content-creator/content/_current/sector-06-pe-compression/blogs/us/
"""
import json
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "backtest.json") as f:
    data = json.load(f)

STRATEGY_COLOR = "#1a5276"
SPY_COLOR = "#aab7b8"


def get_cumulative_growth(initial=10000):
    values = [initial]
    years = [data["annual_returns"][0]["year"] - 1]
    for ar in data["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(initial=10000):
    values = [initial]
    years = [data["annual_returns"][0]["year"] - 1]
    for ar in data["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative():
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative()
    spy_cagr = data["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=SPY_COLOR, linewidth=1.8,
            label=f"S&P 500 (SPY) ({spy_cagr}% CAGR)", linestyle="--")

    years, vals = get_cumulative_growth()
    cagr = data["portfolio"]["cagr"]
    ax.plot(years, vals, color=STRATEGY_COLOR, linewidth=2.5,
            label=f"Sector P/E Compression ({cagr}% CAGR)")

    final_k = vals[-1] / 1000
    ax.annotate(f"${final_k:,.0f}K",
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=10, fontweight="bold", color=STRATEGY_COLOR)

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(f"${spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -14), textcoords="offset points",
                fontsize=10, fontweight="bold", color="#7f8c8d")

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title("Growth of $10,000: Sector P/E Compression vs S&P 500 (2005-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=11, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | S&P 500 sectors, quarterly rebalance, z-score < -1.0, "
             "equal weight, 0.1% transaction costs",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "1_us_cumulative_growth.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars():
    years = [ar["year"] for ar in data["annual_returns"]]
    portfolio_returns = [ar["portfolio"] for ar in data["annual_returns"]]
    spy_returns = [ar["spy"] for ar in data["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.38
    x = list(range(len(years)))

    ax.bar([i - width / 2 for i in x], spy_returns, width,
           label="S&P 500 (SPY)", color=SPY_COLOR, alpha=0.8)
    ax.bar([i + width / 2 for i in x], portfolio_returns, width,
           label="Sector P/E Compression", color=STRATEGY_COLOR, alpha=0.9)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title("Sector P/E Compression vs S&P 500: Year-by-Year Returns (2005-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             "Data: Ceta Research | S&P 500 sectors, quarterly rebalance, z-score < -1.0, "
             "equal weight, 0.1% transaction costs",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "2_us_annual_returns.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def move_to_content():
    content_dir = (
        Path(__file__).parent.parent.parent
        / "ts-content-creator/content/_current/sector-06-pe-compression/blogs/us"
    )
    if not content_dir.exists():
        print(f"  Content dir not found: {content_dir}")
        return
    for fname in ["1_us_cumulative_growth.png", "2_us_annual_returns.png"]:
        src = charts_dir / fname
        dst = content_dir / fname
        if src.exists():
            shutil.move(str(src), str(dst))
            print(f"  Moved: {fname} → {dst}")
        else:
            print(f"  Not found: {src}")


print("Generating Sector P/E Compression charts...")
chart_cumulative()
chart_annual_bars()
print("\nMoving charts to content directory...")
move_to_content()
print("\nDone.")
