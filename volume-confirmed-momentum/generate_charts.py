"""Generate all Volume-Confirmed Momentum charts from exchange_comparison.json.

Run after: python3 volume-confirmed-momentum/backtest.py --global --output results/exchange_comparison.json
Charts saved to: backtests/volume-confirmed-momentum/charts/
Move charts (mv, not cp) to: ts-content-creator/content/_current/momentum-08-volume-confirmed/blogs/{region}/
"""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette (consistent across all strategy chart files)
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE":          "#e67e22",
    "LSE":              "#8e44ad",
    "XETRA":            "#16a085",
    "JPX":              "#2980b9",
    "SHZ_SHH":          "#e74c3c",
    "HKSE":             "#a569bd",
    "KSC":              "#95a5a6",
    "TAI_TWO":          "#7f8c8d",
    "TSX":              "#8e44ad",
    "SIX":              "#2c3e50",
    "STO":              "#2980b9",
    "SET":              "#f39c12",
    "JNB":              "#27ae60",
    "OSL":              "#d35400",
    "MIL":              "#e91e63",
    "KLS":              "#1abc9c",
    "SPY":              "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "VolMom US (NYSE+NASDAQ+AMEX)",
    "NSE":          "VolMom India (NSE)",
    "LSE":              "VolMom UK (LSE)",
    "XETRA":            "VolMom Germany (XETRA)",
    "JPX":              "VolMom Japan (JPX)",
    "SHZ_SHH":          "VolMom China (SHZ+SHH)",
    "HKSE":             "VolMom Hong Kong (HKSE)",
    "KSC":              "VolMom Korea (KSC)",
    "TAI_TWO":          "VolMom Taiwan (TAI+TWO)",
    "TSX":              "VolMom Canada (TSX)",
    "SIX":              "VolMom Switzerland (SIX)",
    "STO":              "VolMom Sweden (STO)",
    "SET":              "VolMom Thailand (SET)",
    "JNB":              "VolMom South Africa (JNB)",
    "OSL":              "VolMom Norway (OSL)",
    "MIL":              "VolMom Italy (MIL)",
    "KLS":              "VolMom Malaysia (KLS)",
}


def get_cumulative_growth(exchange_key, initial=10000):
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(ref_key, initial=10000):
    ex = data[ref_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, ref_key=None):
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
             f"Data: Ceta Research | {footer_universe}, semi-annual rebalance, equal weight, 2001-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
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
             f"Data: Ceta Research | {footer_universe}, semi-annual rebalance, equal weight, 2001-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5 + 2)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    # Find US SPY CAGR for reference line
    us_key = next((k for k in data if "NYSE" in k or "AMEX" in k), None)
    if us_key:
        spy_cagr = data[us_key]["spy"]["cagr"]
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Volume-Confirmed Momentum CAGR by Exchange (2001-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | NI>0, OCF>0, 12M mom (skip 1M), vol ratio>1, semi-annual, equal weight. Local currency.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5 + 2)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    us_key = next((k for k in data if "NYSE" in k or "AMEX" in k), None)
    if us_key:
        spy_dd = data[us_key]["spy"]["max_drawdown"]
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Volume-Confirmed Momentum Max Drawdown by Exchange (2001-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=9, fontweight="bold",
                ha="right")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Volume-Confirmed Momentum, semi-annual rebalance, equal weight.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts based on available results ----

available = [k for k, v in data.items()
             if isinstance(v, dict) and v.get("invested_periods", 0) > 0]
print(f"Found {len(available)} exchanges with results: {available}")

# US
if "NYSE_NASDAQ_AMEX" in available:
    print("\nGenerating US charts...")
    chart_cumulative(
        ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum US vs S&P 500 (2001-2025)",
        "NYSE + NASDAQ + AMEX"
    )
    chart_annual_bars(
        ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
        "Volume-Confirmed Momentum US vs S&P 500: Year-by-Year Returns (2001-2025)",
        "NYSE + NASDAQ + AMEX"
    )

# India
if "NSE" in available:
    print("\nGenerating India charts...")
    chart_cumulative(
        ["NSE"], "1_india_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum India vs S&P 500 (2001-2025)",
        "NSE (returns in INR, benchmark in USD)"
    )
    chart_annual_bars(
        ["NSE"], "2_india_annual_returns.png",
        "Volume-Confirmed Momentum India vs S&P 500: Year-by-Year Returns (2001-2025)",
        "NSE (returns in INR)"
    )

# UK
if "LSE" in available:
    print("\nGenerating UK charts...")
    chart_cumulative(
        ["LSE"], "1_uk_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum UK vs S&P 500 (2001-2025)",
        "LSE (returns in GBP, benchmark in USD)"
    )
    chart_annual_bars(
        ["LSE"], "2_uk_annual_returns.png",
        "Volume-Confirmed Momentum UK vs S&P 500: Year-by-Year Returns (2001-2025)",
        "LSE (returns in GBP)"
    )

# Germany
if "XETRA" in available:
    print("\nGenerating Germany charts...")
    chart_cumulative(
        ["XETRA"], "1_germany_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum Germany vs S&P 500 (2001-2025)",
        "XETRA (returns in EUR, benchmark in USD)"
    )
    chart_annual_bars(
        ["XETRA"], "2_germany_annual_returns.png",
        "Volume-Confirmed Momentum Germany vs S&P 500: Year-by-Year Returns (2001-2025)",
        "XETRA (returns in EUR)"
    )

# Japan
if "JPX" in available:
    print("\nGenerating Japan charts...")
    chart_cumulative(
        ["JPX"], "1_japan_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum Japan vs S&P 500 (2001-2025)",
        "JPX (returns in JPY, benchmark in USD)"
    )
    chart_annual_bars(
        ["JPX"], "2_japan_annual_returns.png",
        "Volume-Confirmed Momentum Japan vs S&P 500: Year-by-Year Returns (2001-2025)",
        "JPX (returns in JPY)"
    )

# Canada
if "TSX" in available:
    print("\nGenerating Canada charts...")
    chart_cumulative(
        ["TSX"], "1_canada_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum Canada vs S&P 500 (2001-2025)",
        "TSX (returns in CAD, benchmark in USD)"
    )
    chart_annual_bars(
        ["TSX"], "2_canada_annual_returns.png",
        "Volume-Confirmed Momentum Canada vs S&P 500: Year-by-Year Returns (2001-2025)",
        "TSX (returns in CAD)"
    )

# South Korea
if "KSC" in available:
    print("\nGenerating Korea charts...")
    chart_cumulative(
        ["KSC"], "1_korea_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum Korea vs S&P 500 (2001-2025)",
        "KSC (returns in KRW, benchmark in USD)"
    )
    chart_annual_bars(
        ["KSC"], "2_korea_annual_returns.png",
        "Volume-Confirmed Momentum Korea vs S&P 500: Year-by-Year Returns (2001-2025)",
        "KSC (returns in KRW)"
    )

# China
if "SHZ_SHH" in available:
    print("\nGenerating China charts...")
    chart_cumulative(
        ["SHZ_SHH"], "1_china_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum China vs S&P 500 (2001-2025)",
        "SHZ + SHH (returns in CNY, benchmark in USD)"
    )
    chart_annual_bars(
        ["SHZ_SHH"], "2_china_annual_returns.png",
        "Volume-Confirmed Momentum China vs S&P 500: Year-by-Year Returns (2001-2025)",
        "SHZ + SHH (returns in CNY)"
    )

# Sweden
if "STO" in available:
    print("\nGenerating Sweden charts...")
    chart_cumulative(
        ["STO"], "1_sweden_cumulative_growth.png",
        "Growth of $10,000: Volume-Confirmed Momentum Sweden vs S&P 500 (2001-2025)",
        "STO (returns in SEK, benchmark in USD)"
    )
    chart_annual_bars(
        ["STO"], "2_sweden_annual_returns.png",
        "Volume-Confirmed Momentum Sweden vs S&P 500: Year-by-Year Returns (2001-2025)",
        "STO (returns in SEK)"
    )

# Comparison charts (if 8+ exchanges have results)
if len(available) >= 8:
    print("\nGenerating comparison charts...")
    chart_comparison_cagr("1_comparison_cagr.png")
    chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("Move (mv, not cp) charts to the appropriate blogs/ subdirectory before publishing.")
print("Naming convention:")
print("  Exchange:    blogs/{region}/1_{region}_cumulative_growth.png")
print("               blogs/{region}/2_{region}_annual_returns.png")
print("  Comparison:  blogs/comparison/1_comparison_cagr.png")
print("               blogs/comparison/2_comparison_drawdown.png")
