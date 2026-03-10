"""Generate all pairs trading charts for blog posts from exchange_comparison.json."""
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
    "JPX":              "#e67e22",
    "LSE":              "#27ae60",
    "JNB":              "#8e44ad",
    "BSE_NSE":          "#c0392b",
    "SHZ_SHH":          "#e74c3c",
    "HKSE":             "#7f8c8d",
    "KSC":              "#95a5a6",
    "TAI_TWO":          "#bdc3c7",
    "XETRA":            "#2ecc71",
    "TSX":              "#3498db",
    "STO":              "#9b59b6",
    "SPY":              "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Pairs US (NYSE+NASDAQ+AMEX)",
    "JPX":              "Pairs Japan (JPX)",
    "LSE":              "Pairs UK (LSE)",
    "JNB":              "Pairs South Africa (JNB)",
    "BSE_NSE":          "Pairs India (BSE+NSE)",
    "SHZ_SHH":          "Pairs China (SHZ+SHH)",
    "HKSE":             "Pairs Hong Kong",
    "KSC":              "Pairs Korea (KSC)",
    "TAI_TWO":          "Pairs Taiwan (TAI+TWO)",
    "XETRA":            "Pairs Germany (XETRA)",
    "TSX":              "Pairs Canada (TSX)",
    "STO":              "Pairs Sweden (STO)",
}

VALID_EXCHANGES = [
    "NYSE_NASDAQ_AMEX", "JPX", "LSE", "JNB", "BSE_NSE", "SHZ_SHH",
    "HKSE", "KSC", "TAI_TWO", "XETRA", "TSX", "STO"
]


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns list."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(ref_key="NYSE_NASDAQ_AMEX", initial=10000):
    """Get SPY cumulative from reference exchange."""
    ex = data[ref_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchange_key, filename, title, footer_exchange):
    """Generate cumulative growth chart for one exchange vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative()
    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    ex = data[exchange_key]
    years, vals = get_cumulative_growth(exchange_key)
    cagr = ex["portfolio"]["cagr"]
    label = f"{EXCHANGE_LABELS[exchange_key]} ({cagr}% CAGR)"
    ax.plot(years, vals, color=COLORS[exchange_key], linewidth=2.2, label=label)

    # Final value annotations
    final_k = vals[-1] / 1000
    ax.annotate(f"${final_k:,.0f}K",
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS[exchange_key])

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
             f"Data: Ceta Research | {footer_exchange}, annual rebalance, market-neutral, 2005-2024",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, title, footer_exchange):
    """Generate annual returns bar chart for one exchange vs SPY."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    pairs_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.38
    x = list(range(len(years)))

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], pairs_returns, width,
           label=EXCHANGE_LABELS[exchange_key], color=COLORS[exchange_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_exchange}, annual rebalance, market-neutral, 2005-2024",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange, sorted descending."""
    valid = [(k, data[k]) for k in VALID_EXCHANGES]
    valid.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = []
    cagrs = []
    colors = []
    for k, v in valid:
        cagrs.append(v["portfolio"]["cagr"])
        # Clean label: exchange code + country
        label_map = {
            "NYSE_NASDAQ_AMEX": "US (NYSE+NASDAQ+AMEX)",
            "JPX": "Japan (JPX)",
            "LSE": "UK (LSE)",
            "JNB": "South Africa (JNB)",
            "BSE_NSE": "India (BSE+NSE)",
            "SHZ_SHH": "China (SHZ+SHH)",
            "HKSE": "Hong Kong",
            "KSC": "Korea (KSC)",
            "TAI_TWO": "Taiwan (TAI+TWO)",
            "XETRA": "Germany (XETRA)",
            "TSX": "Canada (TSX)",
            "STO": "Sweden (STO)",
        }
        names.append(label_map.get(k, k))
        colors.append(COLORS.get(k, "#95a5a6"))

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")
    ax.axvline(x=0, color="black", linewidth=0.8)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Pairs Trading CAGR by Exchange (2005-2024)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = cagr + 0.08 if cagr >= 0 else cagr - 0.5
        ha = "left" if cagr >= 0 else "right"
        ax.text(x_pos, i, f"{cagr:.2f}%", va="center", fontsize=10, fontweight="bold", ha=ha)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same strategy parameters, annual rebalance, 12 exchanges",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cash(filename):
    """Horizontal bar chart: Cash% by exchange (how often strategy sits out)."""
    valid = [(k, data[k]) for k in VALID_EXCHANGES]
    valid.sort(key=lambda x: x[1]["cash_periods"] / x[1]["n_years"])

    names = []
    cash_pcts = []
    colors = []
    label_map = {
        "NYSE_NASDAQ_AMEX": "US (NYSE+NASDAQ+AMEX)",
        "JPX": "Japan (JPX)", "LSE": "UK (LSE)", "JNB": "South Africa",
        "BSE_NSE": "India (BSE+NSE)", "SHZ_SHH": "China (SHZ+SHH)",
        "HKSE": "Hong Kong", "KSC": "Korea", "TAI_TWO": "Taiwan",
        "XETRA": "Germany", "TSX": "Canada", "STO": "Sweden",
    }
    for k, v in valid:
        cash_pct = round(v["cash_periods"] * 100 / v["n_years"])
        cash_pcts.append(cash_pct)
        names.append(label_map.get(k, k))
        colors.append(COLORS.get(k, "#95a5a6"))

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cash_pcts, color=colors, alpha=0.85, height=0.6)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Cash Periods (%)", fontsize=12, fontweight="bold")
    ax.set_title("Pairs Trading: % of Years in Cash (2005-2024)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, pct) in enumerate(zip(bars, cash_pcts)):
        ax.text(pct + 0.5, i, f"{pct:.0f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Cash = year with fewer than 3 active pairs",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating US charts...")
chart_cumulative(
    "NYSE_NASDAQ_AMEX",
    "1_us_cumulative_growth.png",
    "Growth of $10,000: Pairs Trading US vs S&P 500 (2005-2024)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    "NYSE_NASDAQ_AMEX",
    "2_us_annual_returns.png",
    "Pairs Trading US vs S&P 500: Year-by-Year Returns (2005-2024)",
    "NYSE + NASDAQ + AMEX"
)

print("Generating Japan charts...")
chart_cumulative(
    "JPX",
    "1_japan_cumulative_growth.png",
    "Growth of $10,000: Pairs Trading Japan vs S&P 500 (2005-2024)",
    "JPX (Tokyo Stock Exchange)"
)
chart_annual_bars(
    "JPX",
    "2_japan_annual_returns.png",
    "Pairs Trading Japan vs S&P 500: Year-by-Year Returns (2005-2024)",
    "JPX (Tokyo Stock Exchange)"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_cash("2_comparison_cash.png")

print(f"\nDone. Charts generated in {charts_dir}/")
