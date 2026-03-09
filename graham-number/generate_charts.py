"""Generate all Graham Number charts for blog posts from exchange_comparison.json."""
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
    "US_MAJOR":    "#1a5276",
    "Japan":       "#c0392b",
    "Sweden":      "#0d6efd",
    "Canada":      "#c75000",
    "Brazil":      "#157347",
    "Germany":     "#27ae60",
    "Switzerland": "#8e44ad",
    "India":       "#e67e22",
    "Taiwan":      "#2980b9",
    "Korea":       "#7f8c8d",
    "Norway":      "#16a085",
    "Australia":   "#bdc3c7",
    "UK":          "#34495e",
    "HKSE":        "#95a5a6",
    "SPY":         "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR":    "Graham Number US (NYSE+NASDAQ+AMEX)",
    "Japan":       "Graham Number Japan (JPX)",
    "Sweden":      "Graham Number Sweden (STO)",
    "Canada":      "Graham Number Canada (TSX+TSXV)",
    "Brazil":      "Graham Number Brazil (SAO)",
    "Germany":     "Graham Number Germany (XETRA)",
    "Switzerland": "Graham Number Switzerland (SIX)",
    "India":       "Graham Number India (BSE+NSE)",
    "Taiwan":      "Graham Number Taiwan (TAI+TWO)",
    "Korea":       "Graham Number Korea (KSC)",
    "Norway":      "Graham Number Norway (OSL)",
    "Australia":   "Graham Number Australia (ASX)",
    "UK":          "Graham Number UK (LSE)",
    "HKSE":        "Graham Number HK (HKSE)",
}

FOOTER = (
    "Data: Ceta Research | Graham Number (price < sqrt(22.5 x EPS x BVPS)), "
    "annual rebalance, equal weight, 2000-2025"
)

# Exchanges with actual results (exclude data-issue exchanges)
CLEAN_EXCHANGES = [k for k, v in data.items() if v["invested_periods"] > 0]


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
    """Get SPY cumulative from US_MAJOR (reference exchange)."""
    ref = "US_MAJOR" if "US_MAJOR" in data else CLEAN_EXCHANGES[0]
    ex = data[ref]
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
    if spy_years:
        spy_cagr = data.get("US_MAJOR", {}).get("spy", {}).get("cagr", "?")
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in data or data[ex_key]["invested_periods"] == 0:
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

    if spy_years:
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

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center",
             fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    active = [e for e in exchanges if e in data and data[e]["invested_periods"] > 0]
    if not active:
        print(f"  Skipping {filename}: no data for {exchanges}")
        return

    ex = data[active[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(active) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))
    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(active):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}", ha="center",
             fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange (clean exchanges only)."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and k in CLEAN_EXCHANGES
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k).replace("Graham Number ", "") for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, _ in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.7)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data.get("US_MAJOR", {}).get("spy", {}).get("cagr")
    if spy_cagr:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Graham Number CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_excess(filename):
    """Horizontal bar chart: Excess CAGR vs SPY by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and k in CLEAN_EXCHANGES
    ]
    exchanges_with_data.sort(
        key=lambda x: x[1]["comparison"]["excess_cagr"] or -999, reverse=True
    )

    names = [EXCHANGE_LABELS.get(k, k).replace("Graham Number ", "") for k, _ in exchanges_with_data]
    excesses = [v["comparison"]["excess_cagr"] or 0 for _, v in exchanges_with_data]
    colors = ["#27ae60" if e >= 0 else "#c0392b" for e in excesses]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.7)))
    bars = ax.barh(range(len(names)), excesses, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=0, color="black", linewidth=1.2)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Excess CAGR vs S&P 500 (%)", fontsize=12, fontweight="bold")
    ax.set_title("Graham Number: Alpha vs S&P 500 by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, e) in enumerate(zip(bars, excesses)):
        x_pos = e + 0.3 if e >= 0 else e - 0.3
        ha = "left" if e >= 0 else "right"
        ax.text(x_pos, i, f"{e:+.1f}%", va="center", fontsize=9, fontweight="bold",
                ha=ha, color="white" if abs(e) > 3 else "black")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate per-exchange charts ----

print("Generating charts for US...")
chart_cumulative(
    ["US_MAJOR"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Graham Number US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"
)
chart_annual_bars(
    ["US_MAJOR"], "2_us_annual_returns.png",
    "Graham Number US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"
)

print("Generating charts for Japan...")
chart_cumulative(
    ["Japan"], "1_japan_cumulative_growth.png",
    "Growth of $10,000: Graham Number Japan vs S&P 500 (2000-2025)",
    "JPX (returns in JPY, benchmark in USD)"
)
chart_annual_bars(
    ["Japan"], "2_japan_annual_returns.png",
    "Graham Number Japan vs S&P 500: Year-by-Year Returns (2000-2024)",
    "JPX (returns in JPY)"
)

print("Generating charts for Sweden...")
chart_cumulative(
    ["Sweden"], "1_sweden_cumulative_growth.png",
    "Growth of $10,000: Graham Number Sweden vs S&P 500 (2000-2025)",
    "STO (returns in SEK, benchmark in USD)"
)
chart_annual_bars(
    ["Sweden"], "2_sweden_annual_returns.png",
    "Graham Number Sweden vs S&P 500: Year-by-Year Returns (2000-2024)",
    "STO (returns in SEK)"
)

print("Generating charts for Canada...")
chart_cumulative(
    ["Canada"], "1_canada_cumulative_growth.png",
    "Growth of $10,000: Graham Number Canada vs S&P 500 (2000-2025)",
    "TSX + TSXV (returns in CAD, benchmark in USD)"
)
chart_annual_bars(
    ["Canada"], "2_canada_annual_returns.png",
    "Graham Number Canada vs S&P 500: Year-by-Year Returns (2000-2024)",
    "TSX + TSXV (returns in CAD)"
)

print("Generating charts for Brazil...")
chart_cumulative(
    ["Brazil"], "1_brazil_cumulative_growth.png",
    "Growth of $10,000: Graham Number Brazil vs S&P 500 (2000-2025)",
    "SAO (returns in BRL, benchmark in USD)"
)
chart_annual_bars(
    ["Brazil"], "2_brazil_annual_returns.png",
    "Graham Number Brazil vs S&P 500: Year-by-Year Returns (2000-2024)",
    "SAO (returns in BRL)"
)

print("Generating charts for Germany...")
chart_cumulative(
    ["Germany"], "1_germany_cumulative_growth.png",
    "Growth of $10,000: Graham Number Germany vs S&P 500 (2000-2025)",
    "XETRA (returns in EUR, benchmark in USD)"
)
chart_annual_bars(
    ["Germany"], "2_germany_annual_returns.png",
    "Graham Number Germany vs S&P 500: Year-by-Year Returns (2000-2024)",
    "XETRA (returns in EUR)"
)

print("Generating charts for India...")
chart_cumulative(
    ["India"], "1_india_cumulative_growth.png",
    "Growth of $10,000: Graham Number India vs S&P 500 (2000-2025)",
    "BSE + NSE (returns in INR, benchmark in USD)"
)
chart_annual_bars(
    ["India"], "2_india_annual_returns.png",
    "Graham Number India vs S&P 500: Year-by-Year Returns (2000-2024)",
    "BSE + NSE (returns in INR)"
)

print("Generating charts for Taiwan...")
chart_cumulative(
    ["Taiwan"], "1_taiwan_cumulative_growth.png",
    "Growth of $10,000: Graham Number Taiwan vs S&P 500 (2000-2025)",
    "TAI + TWO (returns in TWD, benchmark in USD)"
)
chart_annual_bars(
    ["Taiwan"], "2_taiwan_annual_returns.png",
    "Graham Number Taiwan vs S&P 500: Year-by-Year Returns (2000-2024)",
    "TAI + TWO (returns in TWD)"
)

print("Generating charts for Switzerland...")
chart_cumulative(
    ["Switzerland"], "1_switzerland_cumulative_growth.png",
    "Growth of $10,000: Graham Number Switzerland vs S&P 500 (2000-2025)",
    "SIX (returns in CHF, benchmark in USD)"
)
chart_annual_bars(
    ["Switzerland"], "2_switzerland_annual_returns.png",
    "Graham Number Switzerland vs S&P 500: Year-by-Year Returns (2000-2024)",
    "SIX (returns in CHF)"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_excess("2_comparison_excess.png")

print(f"\nDone. Charts generated in {charts_dir}/")
