"""Generate all P/E Mean Reversion charts for blog posts from exchange_comparison.json."""
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
    "JPX": "#c0392b",
    "LSE": "#8e44ad",
    "SHZ_SHH": "#e74c3c",
    "HKSE": "#a569bd",
    "TAI_TWO": "#7f8c8d",
    "SET": "#f39c12",
    "XETRA": "#16a085",
    "KSC": "#95a5a6",
    "TSX": "#117a65",
    "STO": "#2980b9",
    "SIX": "#2c3e50",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "P/E Mean Reversion US (NYSE+NASDAQ+AMEX)",
    "NSE": "P/E Mean Reversion India (NSE)",
    "JPX": "P/E Mean Reversion Japan (JPX)",
    "LSE": "P/E Mean Reversion UK (LSE)",
    "SHZ_SHH": "P/E Mean Reversion China (SHZ+SHH)",
    "HKSE": "P/E Mean Reversion Hong Kong (HKSE)",
    "TAI_TWO": "P/E Mean Reversion Taiwan (TAI+TWO)",
    "SET": "P/E Mean Reversion Thailand (SET)",
    "XETRA": "P/E Mean Reversion Germany (XETRA)",
    "KSC": "P/E Mean Reversion Korea (KSC)",
    "TSX": "P/E Mean Reversion Canada (TSX)",
    "STO": "P/E Mean Reversion Sweden (STO)",
    "SIX": "P/E Mean Reversion Switzerland (SIX)",
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
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025",
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
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025",
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
           and v.get("portfolio", {}).get("cagr") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    ref_key = "NYSE_NASDAQ_AMEX" if "NYSE_NASDAQ_AMEX" in data else list(data.keys())[0]
    spy_cagr = data[ref_key]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("P/E Mean Reversion CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same sector-relative P/E screen, annual rebalance, equal weight. Local currency returns.",
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
           and v.get("portfolio", {}).get("max_drawdown") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    ref_key = "NYSE_NASDAQ_AMEX" if "NYSE_NASDAQ_AMEX" in data else list(data.keys())[0]
    spy_dd = data[ref_key]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("P/E Mean Reversion Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=9, fontweight="bold", ha="right")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same sector-relative P/E screen, annual rebalance, equal weight.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate charts for each available exchange ----
valid_exchanges = [
    k for k, v in data.items()
    if isinstance(v, dict) and v.get("invested_periods", 0) > 0
       and v.get("portfolio", {}).get("cagr") is not None
]

print(f"Valid exchanges with data: {valid_exchanges}")

EXCHANGE_CHART_CONFIGS = [
    ("NYSE_NASDAQ_AMEX", "us", "NYSE + NASDAQ + AMEX"),
    ("NSE", "india", "NSE (returns in INR, benchmark in USD)"),
    ("JPX", "japan", "JPX (returns in JPY, benchmark in USD)"),
    ("LSE", "uk", "LSE (returns in GBP, benchmark in USD)"),
    ("SHZ_SHH", "china", "SHZ + SHH (returns in CNY, benchmark in USD)"),
    ("HKSE", "hongkong", "HKSE (returns in HKD, benchmark in USD)"),
    ("TAI_TWO", "taiwan", "TAI + TWO (returns in TWD, benchmark in USD)"),
    ("SET", "thailand", "SET (returns in THB, benchmark in USD)"),
    ("XETRA", "germany", "XETRA (returns in EUR, benchmark in USD)"),
    ("KSC", "korea", "KSC (returns in KRW, benchmark in USD)"),
    ("TSX", "canada", "TSX (returns in CAD, benchmark in USD)"),
    ("STO", "sweden", "STO (returns in SEK, benchmark in USD)"),
    ("SIX", "switzerland", "SIX (returns in CHF, benchmark in USD)"),
]

for ex_key, region_slug, footer in EXCHANGE_CHART_CONFIGS:
    if ex_key not in valid_exchanges:
        continue
    print(f"\nGenerating charts for blogs/{region_slug}/...")
    ex = data[ex_key]
    start_year = ex["annual_returns"][0]["year"] if ex["annual_returns"] else 2000
    end_year = ex["annual_returns"][-1]["year"] if ex["annual_returns"] else 2025
    chart_cumulative(
        [ex_key], f"{region_slug}_cumulative_growth.png",
        f"Growth of $10,000: P/E Mean Reversion {region_slug.title()} vs S&P 500 ({start_year}-{end_year})",
        footer
    )
    chart_annual_bars(
        [ex_key], f"{region_slug}_annual_returns.png",
        f"P/E Mean Reversion {region_slug.title()} vs S&P 500: Year-by-Year Returns ({start_year}-{end_year})",
        footer
    )

print("\nGenerating comparison charts...")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_drawdown("comparison_drawdown.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("Move charts to ts-content-creator/content/_current/timing-01-pe-mean-revert/blogs/{region}/")
print("  Prefix with 1_ (cumulative) and 2_ (annual bars) per the runbook convention.")
