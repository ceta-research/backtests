"""Generate all Sector Momentum charts for blog posts from exchange_comparison.json.

Run from backtests/ directory:
    python3 sector-momentum/generate_charts.py

Charts are saved to backtests/sector-momentum/charts/.
Move each chart to the matching ts-content-creator blog directory after generation.
"""
import json
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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
    "XETRA": "#27ae60",
    "LSE": "#154360",
    "TSX": "#7f8c8d",
    "KSC": "#6c3483",
    # ASX excluded: adjClose split artifacts (see DATA_QUALITY_ISSUES.md)
    "TAI_TWO": "#1a252f",
    # SAO excluded: adjClose split artifacts (see DATA_QUALITY_ISSUES.md)
    "HKSE": "#8e44ad",
    "SIX": "#d68910",
    "STO": "#2e86c1",
    "SET": "#5b2c6f",
    "JNB": "#6e2f1a",
    "JPX": "#1f618d",
    "SHH_SHZ": "#c0392b",
    # SES excluded: 61% cash (not enough sector diversity for strategy to run)
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Sector Momentum US",
    "NSE": "Sector Momentum India",
    "XETRA": "Sector Momentum Germany",
    "LSE": "Sector Momentum UK",
    "TSX": "Sector Momentum Canada",
    "KSC": "Sector Momentum Korea",
    "TAI_TWO": "Sector Momentum Taiwan",
    "HKSE": "Sector Momentum HK",
    "SIX": "Sector Momentum Switzerland",
    "STO": "Sector Momentum Sweden",
    "SET": "Sector Momentum Thailand",
    "JNB": "Sector Momentum S.Africa",
    "JPX": "Sector Momentum Japan",
    "SHH_SHZ": "Sector Momentum China",
}

EXCHANGE_UNIVERSE_LABELS = {
    "NYSE_NASDAQ_AMEX": "NYSE + NASDAQ + AMEX",
    "NSE": "NSE (returns in INR)",
    "XETRA": "XETRA (returns in EUR)",
    "LSE": "LSE (returns in GBP)",
    "TSX": "TSX (returns in CAD)",
    "KSC": "KSC (returns in KRW)",
    "TAI_TWO": "TAI + TWO (returns in TWD)",
    "HKSE": "HKSE (returns in HKD)",
    "SIX": "SIX (returns in CHF)",
    "STO": "STO (returns in SEK)",
    "SET": "SET (returns in THB)",
    "JNB": "JNB (returns in ZAR)",
    "JPX": "JPX (returns in JPY)",
    "SHH_SHZ": "SHH + SHZ (returns in CNY)",
}


def get_cumulative(key, initial=10000):
    ex = data[key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(anchor_key="NYSE_NASDAQ_AMEX", initial=10000):
    ex = data[anchor_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(key, filename, footer_universe):
    ex = data[key]
    cagr = ex["portfolio"]["cagr"]
    spy_cagr = ex["spy"]["cagr"]
    title = (f"Growth of $10,000: Sector Momentum {EXCHANGE_LABELS[key].replace('Sector Momentum ', '')} "
             f"vs S&P 500 (2000-2025)")

    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative()
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8, linestyle="--",
            label=f"S&P 500 ({spy_cagr}% CAGR)")

    years, vals = get_cumulative(key)
    ax.plot(years, vals, color=COLORS[key], linewidth=2.2,
            label=f"{EXCHANGE_LABELS[key]} ({cagr}% CAGR)")

    # Annotate endpoints
    ax.annotate(f"${vals[-1] / 1000:,.0f}K",
                xy=(years[-1], vals[-1]), xytext=(8, 0),
                textcoords="offset points", fontsize=9, fontweight="bold",
                color=COLORS[key])
    ax.annotate(f"${spy_vals[-1] / 1000:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]), xytext=(8, -12),
                textcoords="offset points", fontsize=9, fontweight="bold",
                color=COLORS["SPY"])

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)
    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(key, filename, footer_universe):
    ex = data[key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_rets = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_rets = [ar["spy"] for ar in ex["annual_returns"]]
    title = (f"Sector Momentum {EXCHANGE_LABELS[key].replace('Sector Momentum ', '')}: "
             f"Annual Returns vs S&P 500 (2000-2024)")

    fig, ax = plt.subplots(figsize=(14, 5))
    x = list(range(len(years)))
    width = 0.35

    ax.bar([xi - width / 2 for xi in x], spy_rets, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_rets, width,
           label=EXCHANGE_LABELS[key], color=COLORS[key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar: CAGR by exchange with SPY reference line."""
    valid = [(k, v) for k, v in data.items()
             if v.get("invested_periods", 0) > 0 and v.get("portfolio", {}).get("cagr") is not None]
    valid.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in valid]
    cagrs = [v["portfolio"]["cagr"] for _, v in valid]
    colors = [COLORS.get(k, "#95a5a6") for k, _ in valid]

    # SPY CAGR from US result
    spy_cagr = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("cagr", 8.02)

    fig, ax = plt.subplots(figsize=(11, max(6, len(names) * 0.55)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr:.1f}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Sector Momentum Rotation CAGR by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        ax.text(max(cagr, 0) + 0.2, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 2 sectors by 12M trailing return, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar: Max drawdown by exchange."""
    valid = [(k, v) for k, v in data.items()
             if v.get("portfolio", {}).get("max_drawdown") is not None]
    valid.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])  # Most negative first

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in valid]
    drawdowns = [v["portfolio"]["max_drawdown"] for _, v in valid]
    colors = [COLORS.get(k, "#95a5a6") for k, _ in valid]

    spy_dd = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("max_drawdown", -45.53)

    fig, ax = plt.subplots(figsize=(11, max(6, len(names) * 0.55)))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Sector Momentum Rotation: Max Drawdown by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, dd in enumerate(drawdowns):
        ax.text(dd - 0.5, i, f"{dd:.1f}%", va="center", ha="right", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 2 sectors by 12M trailing return, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# --- Generate charts ---

print("Generating Sector Momentum charts...\n")

for ex_key in EXCHANGE_LABELS:
    if ex_key not in data or not data[ex_key].get("annual_returns"):
        continue
    label = EXCHANGE_LABELS[ex_key].replace("Sector Momentum ", "").lower()
    universe_label = EXCHANGE_UNIVERSE_LABELS.get(ex_key, ex_key)

    print(f"{label.capitalize()} charts...")
    chart_cumulative(ex_key, f"1_{label.replace(' ', '_')}_cumulative_growth.png", universe_label)
    chart_annual_bars(ex_key, f"2_{label.replace(' ', '_')}_annual_returns.png", universe_label)

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("\nNext step: Move charts to ts-content-creator/content/_current/sector-01-rotation/blogs/{region}/")
