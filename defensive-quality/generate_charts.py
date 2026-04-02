"""Generate all Defensive Quality charts for blog posts from exchange_comparison.json."""
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
    "NSE":          "#e67e22",
    "SHZ_SHH":          "#c0392b",
    "HKSE":             "#8e44ad",
    "SET":              "#5b2c6f",
    "JPX":              "#6e2f1a",
    "LSE":              "#154360",
    "XETRA":            "#27ae60",
    "TAI_TWO":          "#1a252f",
    "KSC":              "#6c3483",
    "TSX":              "#7f8c8d",
    "SIX":              "#d68910",
    "STO":              "#2e86c1",
    "JNB":              "#117a65",
    "KLS":              "#a04000",
    "JKT":              "#1e8449",
    "SPY":              "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Defensive US",
    "NSE":          "Defensive India",
    "SHZ_SHH":          "Defensive China",
    "HKSE":             "Defensive Hong Kong",
    "SET":              "Defensive Thailand",
    "JPX":              "Defensive Japan",
    "LSE":              "Defensive UK",
    "XETRA":            "Defensive Germany",
    "TAI_TWO":          "Defensive Taiwan",
    "KSC":              "Defensive Korea",
    "TSX":              "Defensive Canada",
    "SIX":              "Defensive Switzerland",
    "STO":              "Defensive Sweden",
    "JNB":              "Defensive South Africa",
    "KLS":              "Defensive Malaysia",
    "JKT":              "Defensive Indonesia",
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
    """Get SPY cumulative growth from US exchange data."""
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
            print(f"  Warning: {ex_key} not in results, skipping")
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
               label=EXCHANGE_LABELS.get(ex_key, ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)

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
    """Horizontal bar chart: CAGR by exchange vs S&P 500."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
           and v.get("portfolio", {}).get("cagr") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    raw_keys = [k for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in raw_keys]

    fig, ax = plt.subplots(figsize=(11, max(6, len(names) * 0.5)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Defensive Quality CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.2
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Consumer Defensive + Utilities + Healthcare, annual rebalance",
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
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])  # Most negative first

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    raw_keys = [k for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in raw_keys]

    fig, ax = plt.subplots(figsize=(11, max(6, len(names) * 0.5)))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data["NYSE_NASDAQ_AMEX"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Defensive Quality: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, dd in enumerate(drawdowns):
        ax.text(dd - 0.5, i, f"{dd:.1f}%", va="center", ha="right", fontsize=10)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Consumer Defensive + Utilities + Healthcare, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate charts
print("Generating charts for Defensive Quality blogs...")

print("\nUS charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Defensive Quality US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "Defensive Quality US: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

# India charts (if available)
if "NSE" in data:
    print("\nIndia charts...")
    chart_cumulative(
        ["NSE"], "1_india_cumulative_growth.png",
        "Growth of $10,000: Defensive Quality India vs Sensex (2000-2025)",
        "NSE (returns in INR)"
    )
    chart_annual_bars(
        ["NSE"], "2_india_annual_returns.png",
        "Defensive Quality India: Year-by-Year Returns (2000-2024)",
        "NSE (returns in INR)"
    )

# China charts (if available)
if "SHZ_SHH" in data:
    print("\nChina charts...")
    chart_cumulative(
        ["SHZ_SHH"], "1_china_cumulative_growth.png",
        "Growth of $10,000: Defensive Quality China vs S&P 500 (2000-2025)",
        "SHZ + SHH (returns in CNY)"
    )
    chart_annual_bars(
        ["SHZ_SHH"], "2_china_annual_returns.png",
        "Defensive Quality China: Year-by-Year Returns (2000-2024)",
        "SHZ + SHH (returns in CNY)"
    )

# UK charts (if available)
if "LSE" in data:
    print("\nUK charts...")
    chart_cumulative(
        ["LSE"], "1_uk_cumulative_growth.png",
        "Growth of $10,000: Defensive Quality UK vs FTSE 100 (2000-2025)",
        "LSE (returns in GBP)"
    )
    chart_annual_bars(
        ["LSE"], "2_uk_annual_returns.png",
        "Defensive Quality UK: Year-by-Year Returns (2000-2024)",
        "LSE (returns in GBP)"
    )

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts in {charts_dir}/")
