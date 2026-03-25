"""Generate all Earnings Yield charts for blog posts from exchange_comparison.json."""
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
    "LSE": "#16a085",
    "XETRA": "#27ae60",
    "JPX": "#d35400",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI": "#2ecc71",
    "TSX": "#7f8c8d",
    "SIX": "#f39c12",
    "STO": "#1abc9c",
    "SET": "#e91e63",
    "SAU": "#9c27b0",
    "JNB": "#ff5722",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Earnings Yield US (NYSE+NASDAQ+AMEX)",
    "NSE": "Earnings Yield India (NSE)",
    "LSE": "Earnings Yield UK (LSE)",
    "XETRA": "Earnings Yield Germany (XETRA)",
    "JPX": "Earnings Yield Japan (JPX)",
    "SHZ_SHH": "Earnings Yield China (SHZ+SHH)",
    "HKSE": "Earnings Yield Hong Kong (HKSE)",
    "TAI": "Earnings Yield Taiwan (TAI)",
    "TSX": "Earnings Yield Canada (TSX)",
    "SIX": "Earnings Yield Switzerland (SIX)",
    "STO": "Earnings Yield Sweden (STO)",
    "SET": "Earnings Yield Thailand (SET)",
    "SAU": "Earnings Yield Saudi Arabia (SAU)",
    "JNB": "Earnings Yield South Africa (JNB)",
}

FOOTER = (
    "Data: Ceta Research (FMP) | Earnings Yield (EY>0%, ROE>12%, D/E<1.5, IC>3x), "
    "quarterly rebalance, equal weight, 2000-2025"
)


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
    """Get SPY cumulative from any exchange (all have same SPY data)."""
    for k in ["NYSE_NASDAQ_AMEX", "LSE", "XETRA"]:
        if k in data and data[k].get("annual_returns"):
            ex = data[k]
            break
    else:
        return [], []

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
        spy_cagr = None
        for k in ["NYSE_NASDAQ_AMEX", "LSE", "XETRA"]:
            if k in data:
                spy_cagr = data[k].get("spy", {}).get("cagr", "?")
                break
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in data or data[ex_key].get("invested_periods", 0) == 0:
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

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    active = [e for e in exchanges if e in data and data[e].get("invested_periods", 0) > 0]
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
               label=EXCHANGE_LABELS.get(ex_key, ex_key), color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if not v.get("error") and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = None
    for k in ["NYSE_NASDAQ_AMEX", "LSE"]:
        if k in data:
            spy_cagr = data[k].get("spy", {}).get("cagr")
            break
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([EXCHANGE_LABELS.get(n, n) for n in names], fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Earnings Yield CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    if spy_cagr is not None:
        ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if not v.get("error") and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = None
    for k in ["NYSE_NASDAQ_AMEX", "LSE"]:
        if k in data:
            spy_dd = data[k].get("spy", {}).get("max_drawdown")
            break
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([EXCHANGE_LABELS.get(n, n) for n in names], fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Earnings Yield Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    if spy_dd is not None:
        ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts (run AFTER backtest produces exchange_comparison.json) ----

print("Generating charts for US...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "Earnings Yield US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight"
)

print("Generating charts for India...")
chart_cumulative(
    ["NSE"], "1_india_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield India vs S&P 500 (2000-2025)",
    "NSE (returns in INR, benchmark in USD)"
)
chart_annual_bars(
    ["NSE"], "2_india_annual_returns.png",
    "Earnings Yield India vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NSE (returns in INR)"
)

print("Generating charts for UK...")
chart_cumulative(
    ["LSE"], "1_uk_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield UK vs S&P 500 (2000-2025)",
    "LSE (returns in GBP, benchmark in USD)"
)
chart_annual_bars(
    ["LSE"], "2_uk_annual_returns.png",
    "Earnings Yield UK vs S&P 500: Year-by-Year Returns (2000-2024)",
    "LSE (returns in GBP)"
)

print("Generating charts for Germany...")
chart_cumulative(
    ["XETRA"], "1_germany_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield Germany vs S&P 500 (2000-2025)",
    "XETRA (returns in EUR, benchmark in USD)"
)
chart_annual_bars(
    ["XETRA"], "2_germany_annual_returns.png",
    "Earnings Yield Germany vs S&P 500: Year-by-Year Returns (2000-2024)",
    "XETRA (returns in EUR)"
)

print("Generating charts for Japan...")
chart_cumulative(
    ["JPX"], "1_japan_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield Japan vs S&P 500 (2000-2025)",
    "JPX (returns in JPY, benchmark in USD)"
)
chart_annual_bars(
    ["JPX"], "2_japan_annual_returns.png",
    "Earnings Yield Japan vs S&P 500: Year-by-Year Returns (2000-2024)",
    "JPX (returns in JPY)"
)

print("Generating charts for Hong Kong...")
chart_cumulative(
    ["HKSE"], "1_hongkong_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield Hong Kong vs S&P 500 (2000-2025)",
    "HKSE (HKD pegged to USD)"
)
chart_annual_bars(
    ["HKSE"], "2_hongkong_annual_returns.png",
    "Earnings Yield Hong Kong vs S&P 500: Year-by-Year Returns (2000-2024)",
    "HKSE (HKD pegged to USD)"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
