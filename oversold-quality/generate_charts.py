"""Generate all Oversold Quality charts for blog posts from exchange_comparison.json.

Run after backtest.py --global to generate all charts.
Charts are saved to backtests/oversold-quality/charts/.
Manually copy numbered PNGs to ts-content-creator/content/_current/reversion-03-oversold-quality/blogs/{region}/.
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

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "JNB": "#27ae60",
    "TSX": "#8e44ad",
    "STO": "#2980b9",
    "OSL": "#d35400",
    "JKT": "#c0392b",
    "SHZ_SHH": "#e74c3c",
    "XETRA": "#16a085",
    "SIX": "#2c3e50",
    "TAI": "#7f8c8d",
    "KSC": "#95a5a6",
    "SET": "#f39c12",
    "KLS": "#1abc9c",
    "MIL": "#e91e63",
    "HKSE": "#a569bd",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "OQ US (NYSE+NASDAQ+AMEX)",
    "NSE": "OQ India (NSE)",
    "JNB": "OQ South Africa (JSE)",
    "TSX": "OQ Canada (TSX)",
    "STO": "OQ Sweden (STO)",
    "OSL": "OQ Norway (OSL)",
    "JKT": "OQ Indonesia (JKT)",
    "SHZ_SHH": "OQ China (SHZ+SHH)",
    "XETRA": "OQ Germany (XETRA)",
    "SIX": "OQ Switzerland (SIX)",
    "TAI": "OQ Taiwan (TAI)",
    "KSC": "OQ Korea (KSC)",
    "SET": "OQ Thailand (SET)",
    "KLS": "OQ Malaysia (KLS)",
    "MIL": "OQ Italy (MIL)",
    "HKSE": "OQ Hong Kong (HKSE)",
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
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, equal weight, 2000-2025",
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
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange (all exchanges with data)."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if isinstance(v, dict) and v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Oversold Quality CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Piotroski>=7 + RSI-14<30, quarterly rebalance, equal weight. Local currency returns.",
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
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data["NYSE_NASDAQ_AMEX"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Oversold Quality Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=9, fontweight="bold",
                ha="right")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Piotroski>=7 + RSI-14<30, quarterly rebalance, equal weight.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_cash_periods(filename):
    """Bar chart showing cash period percentage by exchange (unique to this strategy)."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if isinstance(v, dict) and v.get("n_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1].get("cash_periods", 0) / max(x[1].get("n_periods", 1), 1))

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cash_pcts = [100 * v.get("cash_periods", 0) / max(v.get("n_periods", 1), 1)
                 for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(range(len(names)), cash_pcts, color=colors, alpha=0.85, height=0.6)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Cash Period Rate (%)", fontsize=12, fontweight="bold")
    ax.set_title("Oversold Quality: Cash Period Rate by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, pct) in enumerate(zip(bars, cash_pcts)):
        ax.text(pct + 0.5, i, f"{pct:.0f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Cash periods = quarters when fewer than 5 stocks pass RSI<30 + Piotroski>=7 simultaneously.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----
# Charts are generated after running backtest --global.
# Update the exchange list below based on actual results.

EXCHANGES_WITH_CLEAN_DATA = []  # Populated after reviewing results

# Try to generate charts based on what's in the results file
available_exchanges = [k for k, v in data.items() if isinstance(v, dict) and v.get("invested_periods", 0) > 0]
print(f"Found {len(available_exchanges)} exchanges with results: {available_exchanges}")

if "NYSE_NASDAQ_AMEX" in available_exchanges:
    print("\nGenerating charts for blogs/us/...")
    chart_cumulative(
        ["NYSE_NASDAQ_AMEX"], "us_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality US vs S&P 500 (2000-2025)",
        "NYSE + NASDAQ + AMEX"
    )
    chart_annual_bars(
        ["NYSE_NASDAQ_AMEX"], "us_annual_returns.png",
        "Oversold Quality US vs S&P 500: Year-by-Year Returns (2000-2025)",
        "NYSE + NASDAQ + AMEX"
    )

if "NSE" in available_exchanges:
    print("\nGenerating charts for blogs/india/...")
    chart_cumulative(
        ["NSE"], "india_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality India vs S&P 500 (2000-2025)",
        "NSE (returns in INR, benchmark in USD)"
    )
    chart_annual_bars(
        ["NSE"], "india_annual_returns.png",
        "Oversold Quality India vs S&P 500: Year-by-Year Returns (2000-2025)",
        "NSE (returns in INR)"
    )

if "XETRA" in available_exchanges:
    print("\nGenerating charts for blogs/germany/...")
    chart_cumulative(
        ["XETRA"], "germany_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality Germany vs S&P 500 (2000-2025)",
        "XETRA (returns in EUR, benchmark in USD)"
    )
    chart_annual_bars(
        ["XETRA"], "germany_annual_returns.png",
        "Oversold Quality Germany vs S&P 500: Year-by-Year Returns (2000-2025)",
        "XETRA (returns in EUR)"
    )

if "TSX" in available_exchanges:
    print("\nGenerating charts for blogs/canada/...")
    chart_cumulative(
        ["TSX"], "canada_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality Canada vs S&P 500 (2000-2025)",
        "TSX (returns in CAD, benchmark in USD)"
    )
    chart_annual_bars(
        ["TSX"], "canada_annual_returns.png",
        "Oversold Quality Canada vs S&P 500: Year-by-Year Returns (2000-2025)",
        "TSX (returns in CAD)"
    )

if "STO" in available_exchanges:
    print("\nGenerating charts for blogs/sweden/...")
    chart_cumulative(
        ["STO"], "sweden_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality Sweden vs S&P 500 (2000-2025)",
        "STO (returns in SEK, benchmark in USD)"
    )
    chart_annual_bars(
        ["STO"], "sweden_annual_returns.png",
        "Oversold Quality Sweden vs S&P 500: Year-by-Year Returns (2000-2025)",
        "STO (returns in SEK)"
    )

if "KSC" in available_exchanges:
    print("\nGenerating charts for blogs/korea/...")
    chart_cumulative(
        ["KSC"], "korea_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality Korea vs S&P 500 (2000-2025)",
        "KSC (returns in KRW, benchmark in USD)"
    )
    chart_annual_bars(
        ["KSC"], "korea_annual_returns.png",
        "Oversold Quality Korea vs S&P 500: Year-by-Year Returns (2000-2025)",
        "KSC (returns in KRW)"
    )

if "SHZ_SHH" in available_exchanges:
    print("\nGenerating charts for blogs/china/...")
    chart_cumulative(
        ["SHZ_SHH"], "china_cumulative_growth.png",
        "Growth of $10,000: Oversold Quality China vs S&P 500 (2000-2025)",
        "SHZ + SHH (returns in CNY, benchmark in USD)"
    )
    chart_annual_bars(
        ["SHZ_SHH"], "china_annual_returns.png",
        "Oversold Quality China vs S&P 500: Year-by-Year Returns (2000-2025)",
        "SHZ + SHH (returns in CNY)"
    )

if len(available_exchanges) >= 8:
    print("\nGenerating comparison charts...")
    chart_comparison_cagr("comparison_cagr.png")
    chart_comparison_drawdown("comparison_drawdown.png")
    chart_cash_periods("comparison_cash_periods.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("Copy charts to the appropriate blogs/ subdirectory before publishing.")
print("Naming: {exchange}/1_{exchange}_cumulative_growth.png, {exchange}/2_{exchange}_annual_returns.png")
print("Comparison: comparison/1_comparison_cagr.png, comparison/2_comparison_drawdown.png")
