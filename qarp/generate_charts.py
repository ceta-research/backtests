"""Generate all QARP charts for blog posts from exchange_comparison.json."""
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
    "NYSE": "#2980b9",
    "NASDAQ": "#7fb3d8",
    "BSE": "#e67e22",
    "NSE": "#f39c12",
    "XETRA": "#27ae60",
    "SHZ": "#c0392b",
    "SHH": "#e74c3c",
    "HKSE": "#8e44ad",
    "KSC": "#95a5a6",
    "ASX": "#bdc3c7",
    "TSX": "#7f8c8d",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "QARP US (NYSE+NASDAQ+AMEX)",
    "NYSE": "QARP NYSE",
    "NASDAQ": "QARP NASDAQ",
    "BSE": "QARP BSE (India)",
    "NSE": "QARP NSE (India)",
    "XETRA": "QARP XETRA (Germany)",
    "SHZ": "QARP Shenzhen",
    "SHH": "QARP Shanghai",
    "HKSE": "QARP HKSE (Hong Kong)",
    "KSC": "QARP KSC (Korea)",
    "ASX": "QARP ASX (Australia)",
    "TSX": "QARP TSX (Canada)",
}


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]  # start year
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(initial=10000):
    """Get SPY cumulative from any exchange (all have same SPY data)."""
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
        label = f"{EXCHANGE_LABELS[ex_key]} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS[ex_key], linewidth=2.2, label=label)

        # Final value annotation
        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS[ex_key])

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
             f"Data: Ceta Research | {footer_universe}, semi-annual rebalance, equal weight, 2000-2025",
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

    n_series = len(exchanges) + 1  # exchanges + SPY
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    # SPY bars
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
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, semi-annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange (all exchanges with data)."""
    # Sort by CAGR descending, exclude zero-data exchanges
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = []
    cagrs = []
    colors = []
    for k, v in exchanges_with_data:
        cagr = v["portfolio"]["cagr"]
        names.append(k)
        cagrs.append(cagr)
        colors.append(COLORS.get(k, "#95a5a6"))

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    # SPY reference line
    spy_cagr = data["US_MAJOR"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("QARP CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    # Value labels on bars
    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
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
        if v["invested_periods"] > 0
    ]
    # Sort by drawdown (least negative = best at top)
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    # SPY reference line
    spy_dd = data["US_MAJOR"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("QARP Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sortino(filename):
    """Horizontal bar chart: Sortino ratio by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0
        and v["portfolio"].get("sortino_ratio") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no sortino_ratio data in exchange_comparison.json")
        return

    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sortino_ratio"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    sortinos = [v["portfolio"]["sortino_ratio"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), sortinos, color=colors, alpha=0.85, height=0.6)

    # SPY reference line (if available)
    spy_sortino = data.get("US_MAJOR", {}).get("spy", {}).get("sortino_ratio")
    if spy_sortino is not None:
        ax.axvline(x=spy_sortino, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sortino:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sortino Ratio", fontsize=12, fontweight="bold")
    ax.set_title("QARP Sortino Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, val) in enumerate(zip(bars, sortinos)):
        x_pos = max(val, 0) + 0.02
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_capture(filename):
    """Scatter plot: Up capture (x) vs Down capture (y) by exchange.

    Ideal zone: upper-right for up capture, lower values for down capture.
    Points in bottom-right quadrant are best (high upside, low downside).
    """
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0
        and v.get("comparison", {}).get("up_capture") is not None
        and v.get("comparison", {}).get("down_capture") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no up_capture/down_capture data in exchange_comparison.json")
        return

    fig, ax = plt.subplots(figsize=(10, 10))

    for k, v in exchanges_with_data:
        up = v["comparison"]["up_capture"]
        down = v["comparison"]["down_capture"]
        color = COLORS.get(k, "#95a5a6")
        ax.scatter(up, down, color=color, s=120, zorder=5, edgecolors="white", linewidth=1)
        ax.annotate(k, (up, down), textcoords="offset points", xytext=(8, 4),
                    fontsize=10, fontweight="bold", color=color)

    # Reference lines at 100%
    ax.axvline(x=100, color="#bdc3c7", linewidth=1, linestyle="--", alpha=0.7)
    ax.axhline(y=100, color="#bdc3c7", linewidth=1, linestyle="--", alpha=0.7)

    # Ideal zone annotation
    ax.annotate("Ideal zone\n(high up, low down)",
                xy=(0.95, 0.05), xycoords="axes fraction",
                fontsize=9, color="#27ae60", alpha=0.6, ha="right",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#e8f8f5", edgecolor="#27ae60", alpha=0.3))

    ax.set_xlabel("Up Capture (%)", fontsize=12, fontweight="bold")
    ax.set_ylabel("Down Capture (%)", fontsize=12, fontweight="bold")
    ax.set_title("QARP Up/Down Capture by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
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
    "Growth of $10,000: QARP US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["US_MAJOR"], "us_annual_returns.png",
    "QARP US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

print("Generating charts for blog_india.md...")
chart_cumulative(
    ["BSE", "NSE"], "india_cumulative_growth.png",
    "Growth of $10,000: QARP India vs S&P 500 (2000-2025)",
    "BSE + NSE (returns in INR, benchmark in USD)"
)
chart_annual_bars(
    ["BSE", "NSE"], "india_annual_returns.png",
    "QARP India vs S&P 500: Year-by-Year Returns (2000-2024)",
    "BSE + NSE (returns in INR)"
)

print("Generating charts for blog_germany.md...")
chart_cumulative(
    ["XETRA"], "germany_cumulative_growth.png",
    "Growth of $10,000: QARP Germany vs S&P 500 (2000-2025)",
    "XETRA (returns in EUR, benchmark in USD)"
)
chart_annual_bars(
    ["XETRA"], "germany_annual_returns.png",
    "QARP Germany vs S&P 500: Year-by-Year Returns (2000-2024)",
    "XETRA (returns in EUR)"
)

print("Generating charts for blog_china.md...")
chart_cumulative(
    ["SHZ", "SHH"], "china_cumulative_growth.png",
    "Growth of $10,000: QARP China vs S&P 500 (2000-2025)",
    "SHZ + SHH (returns in CNY, benchmark in USD)"
)
chart_annual_bars(
    ["SHZ", "SHH"], "china_annual_returns.png",
    "QARP China vs S&P 500: Year-by-Year Returns (2000-2024)",
    "SHZ + SHH (returns in CNY)"
)

print("Generating charts for blog_hongkong.md...")
chart_cumulative(
    ["HKSE"], "hongkong_cumulative_growth.png",
    "Growth of $10,000: QARP Hong Kong vs S&P 500 (2000-2025)",
    "HKSE (HKD pegged to USD)"
)
chart_annual_bars(
    ["HKSE"], "hongkong_annual_returns.png",
    "QARP Hong Kong vs S&P 500: Year-by-Year Returns (2000-2024)",
    "HKSE (HKD pegged to USD)"
)

print("Generating charts for blog_comparison.md...")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_drawdown("comparison_drawdown.png")
chart_comparison_sortino("comparison_sortino.png")
chart_comparison_capture("comparison_capture.png")

print(f"\nDone. Charts generated in {charts_dir}/")
