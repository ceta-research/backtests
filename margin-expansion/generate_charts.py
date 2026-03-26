"""Generate margin expansion backtest charts from result JSON files."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

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
    "SGX": "singapore",
    "JSE": "southafrica",
    "PAR": "france",
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
    "SGX": "Singapore (SGX)",
    "JSE": "South Africa (JSE)",
    "PAR": "France (PAR)",
}

# Colors
C_EXP = "#16a34a"     # green - Expanding
C_STB = "#6b7280"     # gray - Stable
C_CON = "#dc2626"     # red - Contracting
C_SPY = "#111827"     # black - SPY


def load_exchange_comparison():
    """Load the exchange_comparison.json if it exists."""
    comp_path = results_dir / "exchange_comparison.json"
    if comp_path.exists():
        with open(comp_path) as f:
            return json.load(f)
    return None


def load_all_results():
    """Load all per-exchange result JSONs or exchange_comparison.json."""
    # Try exchange_comparison.json first
    comp = load_exchange_comparison()
    if comp:
        data = {}
        for universe, d in comp.items():
            if "error" in d:
                print(f"  Skipping {universe}: {d['error']}")
                continue
            if "annual_returns" not in d:
                print(f"  Skipping {universe}: no annual_returns")
                continue
            data[universe] = d
        return data

    # Fallback: per-exchange files
    data = {}
    for path in sorted(results_dir.glob("margin_expansion_*.json")):
        with open(path) as f:
            d = json.load(f)
        universe = d.get("universe", "")
        if "error" in d or "annual_returns" not in d:
            print(f"  Skipping {universe}: {d.get('error', 'no annual data')}")
            continue
        data[universe] = d
    return data


def cumulative_growth(returns, initial=10000):
    values = [initial]
    for r in returns:
        values.append(values[-1] * (1 + r / 100))
    return values


def chart_cumulative_growth(data, universe, region):
    """Chart 1: Cumulative growth - Expanding vs Stable vs Contracting vs SPY."""
    d = data[universe]
    ar = d["annual_returns"]
    years = [ar[0]["year"] - 1] + [row["year"] for row in ar]

    exp_returns = [row["expanding"] for row in ar]
    stb_returns = [row["stable"] for row in ar]
    con_returns = [row["contracting"] for row in ar]
    spy_returns = [row["spy"] for row in ar]

    exp_cum = cumulative_growth(exp_returns)
    stb_cum = cumulative_growth(stb_returns)
    con_cum = cumulative_growth(con_returns)
    spy_cum = cumulative_growth(spy_returns)

    exp_cagr = d["portfolios"]["expanding"]["cagr"]
    stb_cagr = d["portfolios"]["stable"]["cagr"]
    con_cagr = d["portfolios"]["contracting"]["cagr"]
    spy_cagr = d["portfolios"]["sp500"]["cagr"]

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(years, exp_cum, color=C_EXP, linewidth=2.2,
            label=f"Expanding >+1pp ({exp_cagr}% CAGR)")
    ax.plot(years, stb_cum, color=C_STB, linewidth=1.6, alpha=0.7,
            label=f"Stable ({stb_cagr}% CAGR)")
    ax.plot(years, con_cum, color=C_CON, linewidth=1.6, alpha=0.7,
            label=f"Contracting <-1pp ({con_cagr}% CAGR)")
    ax.plot(years, spy_cum, color=C_SPY, linewidth=1.8, linestyle="--",
            label=f"S&P 500 ({spy_cagr}% CAGR)")

    for vals, color, offset_y in [
        (exp_cum, C_EXP, 0),
        (stb_cum, C_STB, -14),
        (con_cum, C_CON, 14),
        (spy_cum, C_SPY, -14),
    ]:
        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, offset_y), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=color)

    label = REGION_LABELS.get(universe, universe)
    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(f"Growth of $10,000: Margin Expansion Portfolios vs S&P 500 - {label}",
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
    """Chart 2: Annual returns bar chart - Expanding vs SPY."""
    d = data[universe]
    ar = d["annual_returns"]
    years = [row["year"] for row in ar]
    exp_returns = [row["expanding"] for row in ar]
    spy_returns = [row["spy"] for row in ar]

    fig, ax = plt.subplots(figsize=(12, 7))

    x = list(range(len(years)))
    width = 0.35
    offsets = [i - width / 2 for i in x]

    ax.bar([o for o in offsets], exp_returns, width,
           label="Expanding (>+1pp)", color=C_EXP, alpha=0.85)
    ax.bar([o + width for o in offsets], spy_returns, width,
           label="S&P 500", color=C_SPY, alpha=0.4)

    label = REGION_LABELS.get(universe, universe)
    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"Margin Expanders vs S&P 500: Year-by-Year Returns - {label}",
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
        cagr = d["portfolios"]["expanding"]["cagr"]
        items.append((universe, cagr))
    items.sort(key=lambda x: x[1], reverse=True)

    names = [REGION_LABELS.get(u, u) for u, _ in items]
    cagrs = [c for _, c in items]

    spy_cagr = list(data.values())[0]["portfolios"]["sp500"]["cagr"]

    fig, ax = plt.subplots(figsize=(12, max(7, len(items) * 0.6)))

    colors = [C_EXP if c > spy_cagr else "#94a3b8" for c in cagrs]
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_cagr, color="#dc2626", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Margin Expansion Portfolio CAGR by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.2, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.2
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Expanding margins (>+1pp vs 3yr avg), annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "1_comparison_cagr.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(data):
    """Chart 4: Max drawdown comparison across all exchanges."""
    items = []
    for universe, d in data.items():
        dd = d["portfolios"]["expanding"]["max_drawdown"]
        items.append((universe, dd))
    items.sort(key=lambda x: x[1], reverse=True)

    names = [REGION_LABELS.get(u, u) for u, _ in items]
    drawdowns = [dd for _, dd in items]

    spy_dd = list(data.values())[0]["portfolios"]["sp500"]["max_drawdown"]

    fig, ax = plt.subplots(figsize=(12, max(7, len(items) * 0.6)))

    colors = [C_EXP if abs(dd) < abs(spy_dd) else "#94a3b8" for dd in drawdowns]
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_dd, color="#dc2626", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Margin Expansion Portfolio Max Drawdown by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.2, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Expanding margins (>+1pp vs 3yr avg), annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "2_comparison_drawdown.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


if __name__ == "__main__":
    print("Loading results...")
    data = load_all_results()

    if not data:
        print("No valid result files found in results/")
        exit(1)

    print(f"\nFound {len(data)} exchanges with valid data.\n")

    for universe in data:
        region = REGION_MAP.get(universe)
        if not region:
            print(f"  Skipping {universe}: no region mapping")
            continue
        print(f"Generating charts for {universe}...")
        chart_cumulative_growth(data, universe, region)
        chart_annual_returns(data, universe, region)

    if len(data) >= 3:
        print("\nGenerating comparison charts...")
        chart_comparison_cagr(data)
        chart_comparison_drawdown(data)

    print(f"\nDone. Charts saved to {charts_dir}/")
