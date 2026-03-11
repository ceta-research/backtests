"""Generate all Small-Cap Value charts for blog posts from exchange_comparison.json."""
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
    "BSE_NSE": "#e67e22",
    "JPX": "#c0392b",
    "LSE": "#8e44ad",
    "TSX": "#27ae60",
    "STO": "#2980b9",
    "SHZ_SHH": "#e74c3c",
    "XETRA": "#16a085",
    "SIX": "#2c3e50",
    "TAI": "#7f8c8d",
    "KSC": "#f39c12",
    "HKSE": "#a569bd",
    "SET": "#1abc9c",
    "JNB": "#2ecc71",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Small-Cap Value US",
    "BSE_NSE": "Small-Cap Value India (BSE+NSE)",
    "JPX": "Small-Cap Value Japan (JPX)",
    "LSE": "Small-Cap Value UK (LSE)",
    "TSX": "Small-Cap Value Canada (TSX)",
    "STO": "Small-Cap Value Sweden (STO)",
    "SHZ_SHH": "Small-Cap Value China (SHZ+SHH)",
    "XETRA": "Small-Cap Value Germany (XETRA)",
    "SIX": "Small-Cap Value Switzerland (SIX)",
    "TAI": "Small-Cap Value Taiwan (TAI)",
    "KSC": "Small-Cap Value Korea (KSC)",
    "HKSE": "Small-Cap Value Hong Kong (HKSE)",
    "SET": "Small-Cap Value Thailand (SET)",
    "JNB": "Small-Cap Value South Africa (JNB)",
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


def get_spy_cumulative(ref_key="NYSE_NASDAQ_AMEX", initial=10000):
    """Get SPY cumulative from any exchange (all have same SPY data)."""
    ex = data[ref_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, ref_key=None):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    if ref_key is None:
        ref_key = exchanges[0]
    spy_years, spy_vals = get_spy_cumulative(ref_key)
    spy_cagr = data[ref_key]["spy"]["cagr"]
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

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR vs S&P 500 by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, 9))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Small-Cap Value CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same signal (P/B<1.5, small-cap, ROE>5%), annual July rebalance, equal weight. Local currency returns.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"] or -999, reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, 9))
    bars = ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = data["NYSE_NASDAQ_AMEX"]["spy"]["sharpe_ratio"]
    ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 Sharpe ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("Small-Cap Value Sharpe Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, s) in enumerate(zip(bars, sharpes)):
        x_pos = max(s, 0) + 0.01
        ax.text(x_pos, i, f"{s:.3f}", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Sharpe ratio uses regional risk-free rates. Local currency returns.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating charts for blogs/us/...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "us_cumulative_growth.png",
    "Growth of $10,000: Small-Cap Value US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, P/B < 1.5, small-cap ($50M-$2B)"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "us_annual_returns.png",
    "Small-Cap Value US vs S&P 500: Year-by-Year Returns (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)

print("Generating charts for blogs/india/...")
chart_cumulative(
    ["BSE_NSE"], "india_cumulative_growth.png",
    "Growth of ₹10,000: Small-Cap Value India vs S&P 500 (2000-2025)",
    "BSE + NSE, P/B < 1.5, small-cap (₹1B-₹40B)"
)
chart_annual_bars(
    ["BSE_NSE"], "india_annual_returns.png",
    "Small-Cap Value India vs S&P 500: Year-by-Year Returns (2000-2025)",
    "BSE + NSE (returns in INR)"
)

print("Generating charts for blogs/uk/...")
chart_cumulative(
    ["LSE"], "uk_cumulative_growth.png",
    "Growth of £10,000: Small-Cap Value UK vs S&P 500 (2000-2025)",
    "LSE, P/B < 1.5, small-cap (£25M-£1B)"
)
chart_annual_bars(
    ["LSE"], "uk_annual_returns.png",
    "Small-Cap Value UK vs S&P 500: Year-by-Year Returns (2000-2025)",
    "LSE (returns in GBP)"
)

print("Generating charts for blogs/taiwan/...")
chart_cumulative(
    ["TAI"], "taiwan_cumulative_growth.png",
    "Growth of NT$10,000: Small-Cap Value Taiwan vs S&P 500 (2000-2025)",
    "TWSE, P/B < 1.5, small-cap (NT$500M-NT$20B)"
)
chart_annual_bars(
    ["TAI"], "taiwan_annual_returns.png",
    "Small-Cap Value Taiwan vs S&P 500: Year-by-Year Returns (2000-2025)",
    "TWSE (returns in TWD)"
)

print("Generating charts for blogs/germany/...")
chart_cumulative(
    ["XETRA"], "germany_cumulative_growth.png",
    "Growth of €10,000: Small-Cap Value Germany vs S&P 500 (2000-2025)",
    "XETRA, P/B < 1.5, small-cap (€25M-€1B)"
)
chart_annual_bars(
    ["XETRA"], "germany_annual_returns.png",
    "Small-Cap Value Germany vs S&P 500: Year-by-Year Returns (2000-2025)",
    "XETRA (returns in EUR)"
)

print("Generating charts for blogs/canada/...")
chart_cumulative(
    ["TSX"], "canada_cumulative_growth.png",
    "Growth of C$10,000: Small-Cap Value Canada vs S&P 500 (2000-2025)",
    "TSX, P/B < 1.5, small-cap (C$25M-C$1B)"
)
chart_annual_bars(
    ["TSX"], "canada_annual_returns.png",
    "Small-Cap Value Canada vs S&P 500: Year-by-Year Returns (2000-2025)",
    "TSX (returns in CAD)"
)

print("Generating charts for blogs/sweden/...")
chart_cumulative(
    ["STO"], "sweden_cumulative_growth.png",
    "Growth of SEK10,000: Small-Cap Value Sweden vs S&P 500 (2000-2025)",
    "Nasdaq Stockholm, P/B < 1.5, small-cap (SEK250M-SEK10B)"
)
chart_annual_bars(
    ["STO"], "sweden_annual_returns.png",
    "Small-Cap Value Sweden vs S&P 500: Year-by-Year Returns (2000-2025)",
    "Nasdaq Stockholm (returns in SEK)"
)

print("Generating charts for blogs/thailand/...")
chart_cumulative(
    ["SET"], "thailand_cumulative_growth.png",
    "Growth of ฿10,000: Small-Cap Value Thailand vs S&P 500 (2000-2025)",
    "SET, P/B < 1.5, small-cap (฿500M-฿20B)"
)
chart_annual_bars(
    ["SET"], "thailand_annual_returns.png",
    "Small-Cap Value Thailand vs S&P 500: Year-by-Year Returns (2000-2025)",
    "SET (returns in THB)"
)

print("Generating charts for blogs/japan/...")
chart_cumulative(
    ["JPX"], "japan_cumulative_growth.png",
    "Growth of ¥10,000: Small-Cap Value Japan vs S&P 500 (2000-2025)",
    "JPX, P/B < 1.5, small-cap (¥5B-¥200B)"
)
chart_annual_bars(
    ["JPX"], "japan_annual_returns.png",
    "Small-Cap Value Japan vs S&P 500: Year-by-Year Returns (2000-2025)",
    "JPX (returns in JPY)"
)

print("Generating charts for blogs/southafrica/...")
chart_cumulative(
    ["JNB"], "southafrica_cumulative_growth.png",
    "Growth of R10,000: Small-Cap Value South Africa vs S&P 500 (2000-2025)",
    "JSE, P/B < 1.5, small-cap (R500M-R20B)"
)
chart_annual_bars(
    ["JNB"], "southafrica_annual_returns.png",
    "Small-Cap Value South Africa vs S&P 500: Year-by-Year Returns (2000-2025)",
    "JSE (returns in ZAR)"
)

print("Generating charts for blogs/china/...")
chart_cumulative(
    ["SHZ_SHH"], "china_cumulative_growth.png",
    "Growth of ¥10,000: Small-Cap Value China vs S&P 500 (2000-2025)",
    "SHZ + SHH, P/B < 1.5, small-cap (¥100M-¥4B)"
)
chart_annual_bars(
    ["SHZ_SHH"], "china_annual_returns.png",
    "Small-Cap Value China vs S&P 500: Year-by-Year Returns (2000-2025)",
    "SHZ + SHH (returns in CNY)"
)

print("Generating charts for blogs/comparison/...")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_sharpe("comparison_sharpe.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("\nMove charts to the appropriate blogs/ directories:")
print("  us/1_us_cumulative_growth.png, us/2_us_annual_returns.png")
print("  india/1_india_cumulative_growth.png, india/2_india_annual_returns.png")
print("  uk/1_uk_cumulative_growth.png, uk/2_uk_annual_returns.png")
print("  taiwan/1_taiwan_cumulative_growth.png, taiwan/2_taiwan_annual_returns.png")
print("  germany/1_germany_cumulative_growth.png, germany/2_germany_annual_returns.png")
print("  canada/1_canada_cumulative_growth.png, canada/2_canada_annual_returns.png")
print("  sweden/1_sweden_cumulative_growth.png, sweden/2_sweden_annual_returns.png")
print("  comparison/1_comparison_cagr.png, comparison/2_comparison_sharpe.png")
