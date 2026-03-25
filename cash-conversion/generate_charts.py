"""Generate CCC backtest charts from per-exchange result JSON files."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

# Region mapping: JSON universe name -> short region label for filenames
REGION_MAP = {
    "US_MAJOR": "us",
    "India": "india",
    "Canada": "canada",
    "XETRA": "germany",
    "China": "china",
    "HKSE": "hongkong",
    "LSE": "uk",
    "SIX": "switzerland",
    "STO": "sweden",
    "KSC": "korea",
    "SAO": "brazil",
    "Taiwan": "taiwan",
    "JSE": "southafrica",
}

REGION_LABELS = {
    "US_MAJOR": "US (NYSE + NASDAQ + AMEX)",
    "India": "India (NSE)",
    "Canada": "Canada (TSX + TSXV)",
    "XETRA": "Germany (XETRA)",
    "China": "China (SHZ + SHH)",
    "HKSE": "Hong Kong (HKSE)",
    "LSE": "UK (LSE)",
    "SIX": "Switzerland (SIX)",
    "STO": "Sweden (STO)",
    "KSC": "Korea (KSC)",
    "SAO": "Brazil (SAO)",
    "Taiwan": "Taiwan (TAI + TWO)",
    "JSE": "South Africa (JSE)",
}

# Colors
C_LOW = "#2563eb"     # blue - Low CCC
C_MID = "#6b7280"     # gray - Mid CCC
C_HIGH = "#ea580c"    # orange - High CCC
C_SPY = "#111827"     # black - SPY


def load_all_results():
    """Load all per-exchange result JSONs. Skip errored exchanges."""
    data = {}
    for path in sorted(results_dir.glob("ccc_metrics_*.json")):
        with open(path) as f:
            d = json.load(f)
        universe = d.get("universe", "")
        if "error" in d:
            print(f"  Skipping {universe}: {d['error']}")
            continue
        if "annual_returns" not in d:
            print(f"  Skipping {universe}: no annual_returns")
            continue
        data[universe] = d
    return data


def cumulative_growth(returns, initial=10000):
    """Compound a list of annual return percentages into cumulative values."""
    values = [initial]
    for r in returns:
        values.append(values[-1] * (1 + r / 100))
    return values


def chart_cumulative_growth(data, universe, region):
    """Chart 1: Cumulative growth - Low CCC vs Mid CCC vs High CCC vs SPY."""
    d = data[universe]
    ar = d["annual_returns"]
    years = [ar[0]["year"] - 1] + [row["year"] for row in ar]

    low_returns = [row["low"] for row in ar]
    mid_returns = [row["mid"] for row in ar]
    high_returns = [row["high"] for row in ar]
    spy_returns = [row["spy"] for row in ar]

    low_cum = cumulative_growth(low_returns)
    mid_cum = cumulative_growth(mid_returns)
    high_cum = cumulative_growth(high_returns)
    spy_cum = cumulative_growth(spy_returns)

    low_cagr = d["portfolios"]["low_ccc"]["cagr"]
    mid_cagr = d["portfolios"]["mid_ccc"]["cagr"]
    high_cagr = d["portfolios"]["high_ccc"]["cagr"]
    spy_cagr = d["portfolios"]["sp500"]["cagr"]

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(years, low_cum, color=C_LOW, linewidth=2.2,
            label=f"Low CCC <30d ({low_cagr}% CAGR)")
    ax.plot(years, mid_cum, color=C_MID, linewidth=1.6, alpha=0.7,
            label=f"Mid CCC 30-90d ({mid_cagr}% CAGR)")
    ax.plot(years, high_cum, color=C_HIGH, linewidth=1.6, alpha=0.7,
            label=f"High CCC >90d ({high_cagr}% CAGR)")
    ax.plot(years, spy_cum, color=C_SPY, linewidth=1.8, linestyle="--",
            label=f"S&P 500 ({spy_cagr}% CAGR)")

    # Final value annotations
    for vals, color, offset_y in [
        (low_cum, C_LOW, 0),
        (mid_cum, C_MID, -14),
        (high_cum, C_HIGH, 14),
        (spy_cum, C_SPY, -14),
    ]:
        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, offset_y), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=color)

    label = REGION_LABELS.get(universe, universe)
    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(f"Growth of $10,000: CCC Portfolios vs S&P 500 - {label}",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.2, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {label}, annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / f"1_{region}_cumulative_growth.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_returns(data, universe, region):
    """Chart 2: Annual returns bar chart - Low CCC vs SPY."""
    d = data[universe]
    ar = d["annual_returns"]
    years = [row["year"] for row in ar]
    low_returns = [row["low"] for row in ar]
    spy_returns = [row["spy"] for row in ar]

    fig, ax = plt.subplots(figsize=(12, 7))

    x = list(range(len(years)))
    width = 0.35
    offsets = [i - width / 2 for i in x]

    ax.bar([o for o in offsets], low_returns, width,
           label="Low CCC (<30d)", color=C_LOW, alpha=0.85)
    ax.bar([o + width for o in offsets], spy_returns, width,
           label="S&P 500", color=C_SPY, alpha=0.4)

    label = REGION_LABELS.get(universe, universe)
    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"Low CCC vs S&P 500: Year-by-Year Returns - {label}",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.04,
             f"Data: Ceta Research | {label}, annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / f"2_{region}_annual_returns.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(data):
    """Chart 3: CAGR comparison across all exchanges (horizontal bar)."""
    items = []
    for universe, d in data.items():
        cagr = d["portfolios"]["low_ccc"]["cagr"]
        items.append((universe, cagr))
    items.sort(key=lambda x: x[1], reverse=True)

    names = [REGION_LABELS.get(u, u) for u, _ in items]
    cagrs = [c for _, c in items]

    # SPY reference (same across all)
    spy_cagr = list(data.values())[0]["portfolios"]["sp500"]["cagr"]

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = [C_LOW if c > spy_cagr else "#94a3b8" for c in cagrs]
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_cagr, color="#dc2626", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Low CCC Portfolio CAGR by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.2, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.2
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Low CCC (<30 days), annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "1_comparison_cagr.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(data):
    """Chart 4: Max drawdown comparison across all exchanges (horizontal bar)."""
    items = []
    for universe, d in data.items():
        dd = d["portfolios"]["low_ccc"]["max_drawdown"]
        items.append((universe, dd))
    # Sort by drawdown: least negative (best) at top
    items.sort(key=lambda x: x[1], reverse=True)

    names = [REGION_LABELS.get(u, u) for u, _ in items]
    drawdowns = [dd for _, dd in items]

    spy_dd = list(data.values())[0]["portfolios"]["sp500"]["max_drawdown"]

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = [C_LOW if abs(dd) < abs(spy_dd) else "#94a3b8" for dd in drawdowns]
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_dd, color="#dc2626", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Low CCC Portfolio Max Drawdown by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.2, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Low CCC (<30 days), annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "2_comparison_drawdown.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Main ----

if __name__ == "__main__":
    print("Loading results...")
    data = load_all_results()

    if not data:
        print("No valid result files found in results/")
        exit(1)

    print(f"\nFound {len(data)} exchanges with valid data.\n")

    # Per-exchange charts
    for universe in data:
        region = REGION_MAP.get(universe)
        if not region:
            print(f"  Skipping {universe}: no region mapping")
            continue
        print(f"Generating charts for {universe}...")
        chart_cumulative_growth(data, universe, region)
        chart_annual_returns(data, universe, region)

    # Comparison charts
    print("\nGenerating comparison charts...")
    chart_comparison_cagr(data)
    chart_comparison_drawdown(data)

    print(f"\nDone. Charts saved to {charts_dir}/")
