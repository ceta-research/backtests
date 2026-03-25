"""Generate all Yield Gap charts for blog posts from exchange_comparison.json."""
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
    "LSE": "#154360",
    "TSX": "#1e8449",
    "STO": "#2e86c1",
    "NSE": "#e67e22",
    "SIX": "#d68910",
    "XETRA": "#27ae60",
    "JPX": "#6e2f1a",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI_TWO": "#1a252f",
    "SET": "#5b2c6f",
    "KSC": "#6c3483",
    "JKT": "#117a65",
    "KLS": "#7f8c8d",
    "SES": "#2e4057",
    "OSL": "#922b21",
    "WSE": "#5d6d7e",
    "SAU": "#1e8449",
    "JNB": "#cb4335",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Yield Gap US",
    "LSE": "Yield Gap UK",
    "TSX": "Yield Gap Canada",
    "STO": "Yield Gap Sweden",
    "NSE": "Yield Gap India",
    "SIX": "Yield Gap Switzerland",
    "XETRA": "Yield Gap Germany",
    "JPX": "Yield Gap Japan",
    "SHZ_SHH": "Yield Gap China",
    "HKSE": "Yield Gap Hong Kong",
    "TAI_TWO": "Yield Gap Taiwan",
    "SET": "Yield Gap Thailand",
    "KSC": "Yield Gap Korea",
    "JKT": "Yield Gap Indonesia",
    "KLS": "Yield Gap Malaysia",
    "SES": "Yield Gap Singapore",
    "OSL": "Yield Gap Norway",
    "WSE": "Yield Gap Poland",
    "SAU": "Yield Gap Saudi",
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
    """Generate annual returns bar chart (single exchange vs SPY)."""
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


def chart_comparison_cagr(filename, exclude=None):
    """Horizontal bar chart: CAGR by exchange."""
    exclude = exclude or set()
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if k not in exclude and v.get("invested_periods", 0) > 5
        and v.get("portfolio", {}).get("cagr") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, 9))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Yield Gap Strategy: CAGR by Market (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.2
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | EY > rfr+3%, ROE > 8%, D/E < 2.0, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename, exclude=None):
    """Horizontal bar chart: max drawdown by exchange."""
    exclude = exclude or set()
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if k not in exclude and v.get("invested_periods", 0) > 5
        and v.get("portfolio", {}).get("max_drawdown") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, 9))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data["NYSE_NASDAQ_AMEX"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd}% MaxDD)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Yield Gap Strategy: Max Drawdown by Market (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, dd in enumerate(drawdowns):
        ax.text(dd - 1, i, f"{dd:.1f}%", va="center", ha="right", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | EY > rfr+3%, ROE > 8%, D/E < 2.0, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# --- Generate charts ---
print("Generating Yield Gap charts...")

# Exclude JNB (84% cash, not enough invested periods for content)
EXCLUDE = {"JNB"}

print("\nUS charts...")
chart_cumulative(["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
                 "Growth of $10,000: Yield Gap US vs S&P 500 (2000-2025)",
                 "NYSE + NASDAQ + AMEX")
chart_annual_bars("NYSE_NASDAQ_AMEX", "2_us_annual_returns.png",
                  "Yield Gap US: Year-by-Year Returns vs S&P 500 (2000-2024)",
                  "NYSE + NASDAQ + AMEX")

print("\nUK charts...")
chart_cumulative(["LSE"], "1_uk_cumulative_growth.png",
                 "Growth of $10,000: Yield Gap UK vs S&P 500 (2000-2025)",
                 "London Stock Exchange (LSE)")
chart_annual_bars("LSE", "2_uk_annual_returns.png",
                  "Yield Gap UK: Year-by-Year Returns vs S&P 500 (2000-2024)",
                  "London Stock Exchange (LSE)")

print("\nCanada charts...")
chart_cumulative(["TSX"], "1_canada_cumulative_growth.png",
                 "Growth of $10,000: Yield Gap Canada vs S&P 500 (2000-2025)",
                 "Toronto Stock Exchange (TSX)")
chart_annual_bars("TSX", "2_canada_annual_returns.png",
                  "Yield Gap Canada: Year-by-Year Returns vs S&P 500 (2000-2024)",
                  "Toronto Stock Exchange (TSX)")

print("\nSweden charts...")
chart_cumulative(["STO"], "1_sweden_cumulative_growth.png",
                 "Growth of $10,000: Yield Gap Sweden vs S&P 500 (2000-2025)",
                 "Stockholm Stock Exchange (STO)")
chart_annual_bars("STO", "2_sweden_annual_returns.png",
                  "Yield Gap Sweden: Year-by-Year Returns vs S&P 500 (2000-2024)",
                  "Stockholm Stock Exchange (STO)")

print("\nIndia charts...")
chart_cumulative(["NSE"], "1_india_cumulative_growth.png",
                 "Growth of $10,000: Yield Gap India vs S&P 500 (2000-2025)",
                 "NSE (returns in INR)")
chart_annual_bars("NSE", "2_india_annual_returns.png",
                  "Yield Gap India: Year-by-Year Returns vs S&P 500 (2000-2024)",
                  "NSE (returns in INR)")

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png", exclude=EXCLUDE)
chart_comparison_drawdown("2_comparison_drawdown.png", exclude=EXCLUDE)

print(f"\nDone. Charts generated in {charts_dir}/")
