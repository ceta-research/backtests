"""Generate all Dogs of the Dow charts for blog posts from exchange_comparison.json."""
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
    "NSE": "#e67e22",
    "NSE": "#f39c12",
    "STO": "#27ae60",
    "SAO": "#16a085",
    "SET": "#d35400",
    "KSC": "#95a5a6",
    "HKSE": "#8e44ad",
    "TAI": "#2c3e50",
    "ASX": "#bdc3c7",
    "TSX": "#7f8c8d",
    "SHZ": "#c0392b",
    "SHH": "#e74c3c",
    "SIX": "#34495e",
    "XETRA": "#1abc9c",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "Dogs of the Dow (US)",
    "NSE": "High Yield Blue Chips (India NSE)",
    "NSE": "High Yield Blue Chips (India NSE)",
    "STO": "High Yield Blue Chips (Sweden)",
    "SAO": "High Yield Blue Chips (Brazil)",
    "SET": "High Yield Blue Chips (Thailand)",
    "KSC": "High Yield Blue Chips (Korea)",
    "HKSE": "High Yield Blue Chips (Hong Kong)",
    "TAI": "High Yield Blue Chips (Taiwan)",
    "ASX": "High Yield Blue Chips (Australia)",
    "TSX": "High Yield Blue Chips (Canada)",
    "SHZ": "High Yield Blue Chips (Shenzhen)",
    "SHH": "High Yield Blue Chips (Shanghai)",
}

# Exchanges to include in comparison charts (exclude low-quality)
COMPARISON_EXCHANGES = [
    "US_MAJOR", "NSE", "STO", "SAO", "SET", "KSC",
    "HKSE", "TAI", "ASX", "TSX",
]


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
    """Get SPY cumulative from US_MAJOR (all exchanges share same SPY data)."""
    ex = data["US_MAJOR"]
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
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({data['US_MAJOR']['spy']['cagr']}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)

        # Final value annotation
        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    # SPY final value
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
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
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
               label=EXCHANGE_LABELS.get(ex_key, ex_key), color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    items = [
        (k, data[k]) for k in COMPARISON_EXCHANGES
        if k in data and data[k].get("status") == "completed"
    ]
    items.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, v in items]
    cagrs = [v["portfolio"]["cagr"] for k, v in items]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["US_MAJOR"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("High Yield Blue Chips: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 30 by market cap, top 10 by yield, annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    items = [
        (k, data[k]) for k in COMPARISON_EXCHANGES
        if k in data and data[k].get("status") == "completed"
    ]
    items.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, v in items]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in items]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data["US_MAJOR"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("High Yield Blue Chips: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 30 by market cap, top 10 by yield, annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    items = [
        (k, data[k]) for k in COMPARISON_EXCHANGES
        if k in data and data[k].get("status") == "completed"
        and data[k]["portfolio"].get("sharpe_ratio") is not None
    ]
    items.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [k for k, v in items]
    sharpes = [v["portfolio"]["sharpe_ratio"] for k, v in items]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = data["US_MAJOR"]["spy"]["sharpe_ratio"]
    ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("High Yield Blue Chips: Sharpe Ratio by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, val) in enumerate(zip(bars, sharpes)):
        x_pos = max(val, 0) + 0.01
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 30 by market cap, top 10 by yield, annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating charts for blog.md (US)...")
chart_cumulative(
    ["US_MAJOR"], "us_cumulative_growth.png",
    "Growth of $10,000: Dogs of the Dow vs S&P 500 (2000-2025)",
    "Dow 30, top 10 by dividend yield"
)
chart_annual_bars(
    ["US_MAJOR"], "us_annual_returns.png",
    "Dogs of the Dow vs S&P 500: Year-by-Year Returns (2000-2024)",
    "Dow 30, top 10 by dividend yield"
)

print("Generating charts for blog_india.md...")
chart_cumulative(
    ["NSE"], "india_cumulative_growth.png",
    "Growth of $10,000: High Yield Blue Chips India vs S&P 500 (2000-2025)",
    "NSE, top 30 by market cap, top 10 by yield"
)
chart_annual_bars(
    ["NSE"], "india_annual_returns.png",
    "High Yield Blue Chips India vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NSE, top 30 by market cap, top 10 by yield"
)

print("Generating charts for blog_sweden.md...")
chart_cumulative(
    ["STO"], "sweden_cumulative_growth.png",
    "Growth of $10,000: High Yield Blue Chips Sweden vs S&P 500 (2000-2025)",
    "STO, top 30 by market cap, top 10 by yield"
)
chart_annual_bars(
    ["STO"], "sweden_annual_returns.png",
    "High Yield Blue Chips Sweden vs S&P 500: Year-by-Year Returns (2000-2024)",
    "STO, top 30 by market cap, top 10 by yield"
)

print("Generating charts for blog_comparison.md...")
chart_comparison_cagr("global_cagr_comparison.png")
chart_comparison_drawdown("global_drawdown_comparison.png")
chart_comparison_sharpe("global_sharpe_comparison.png")

print(f"\nDone. Charts generated in {charts_dir}/")
