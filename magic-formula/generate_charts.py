"""Generate all Magic Formula charts for blog posts."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
# Charts go to the content directory, not backtests
content_charts_dir = (
    Path(__file__).parent.parent.parent
    / "ts-content-creator" / "content" / "_current"
    / "factor-01-magic-formula" / "charts"
)
content_charts_dir.mkdir(exist_ok=True)

# Load exchange comparison (nested under 'exchanges')
with open(results_dir / "exchange_comparison.json") as f:
    ec_raw = json.load(f)
ec_data = ec_raw["exchanges"]

# Load individual result files (have annual_returns for growth charts)
individual = {}
for p in results_dir.glob("magic_formula_*.json"):
    key = p.stem.replace("magic_formula_", "")
    with open(p) as f:
        individual[key] = json.load(f)

# Color palette
COLORS = {
    "US_MAJOR": "#1a5276",
    "India": "#e67e22",
    "JKT": "#27ae60",
    "SAO": "#c0392b",
    "Canada": "#7f8c8d",
    "China": "#e74c3c",
    "HKSE": "#8e44ad",
    "KSC": "#95a5a6",
    "ASX": "#bdc3c7",
    "XETRA": "#2980b9",
    "STO": "#16a085",
    "SET": "#d35400",
    "TAI": "#f39c12",
    "TLV": "#7fb3d8",
    "SAU": "#2c3e50",
    "PAR": "#ecf0f1",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "Magic Formula US",
    "India": "Magic Formula India (BSE+NSE)",
    "JKT": "Magic Formula Indonesia (JKT)",
    "SAO": "Magic Formula Brazil (SAO)",
    "Canada": "Magic Formula Canada",
    "China": "Magic Formula China",
    "HKSE": "Magic Formula Hong Kong",
    "KSC": "Magic Formula Korea",
    "ASX": "Magic Formula Australia",
    "XETRA": "Magic Formula Germany",
    "STO": "Magic Formula Sweden",
    "SET": "Magic Formula Thailand",
    "TAI": "Magic Formula Taiwan",
    "TLV": "Magic Formula Israel",
    "SAU": "Magic Formula Saudi Arabia",
    "PAR": "Magic Formula France",
}

FOOTER = "Data: Ceta Research | Magic Formula (Rank EY + Rank ROCE, top 30), quarterly rebalance, 2000-2025"


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns in individual result file."""
    if exchange_key not in individual:
        return [], []
    ex = individual[exchange_key]
    if "annual_returns" not in ex or not ex["annual_returns"]:
        return [], []
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(exchange_key="US_MAJOR", initial=10000):
    """Get SPY cumulative from an individual result file."""
    if exchange_key not in individual:
        return [], []
    ex = individual[exchange_key]
    if "annual_returns" not in ex or not ex["annual_returns"]:
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

    # Use the first available exchange for SPY data
    spy_source = exchanges[0] if exchanges else "US_MAJOR"
    spy_years, spy_vals = get_spy_cumulative(spy_source)
    if spy_years:
        spy_cagr = individual.get(spy_source, {}).get("spy", {}).get("cagr", "?")
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in individual:
            continue
        years, vals = get_cumulative_growth(ex_key)
        if not years:
            continue
        cagr = individual[ex_key]["portfolio"]["cagr"]
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
    out = content_charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in ec_data.items()
        if v.get("total_periods", 0) > 0
        and v.get("cash_periods", 0) < v.get("total_periods", 1)
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in exchanges_with_data]
    cagrs = [v["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, _ in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.6)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    # SPY reference line
    spy_cagr = individual.get("US_MAJOR", {}).get("spy", {}).get("cagr")
    if spy_cagr:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Magic Formula CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = content_charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating Magic Formula charts...")
print()

print("US cumulative growth...")
chart_cumulative(
    ["US_MAJOR"], "us_cumulative_growth.png",
    "Growth of $10,000: Magic Formula US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight, top 30"
)

print("India cumulative growth...")
chart_cumulative(
    ["India"], "india_cumulative_growth.png",
    "Growth of $10,000: Magic Formula India vs S&P 500 (2000-2025)",
    "BSE + NSE, quarterly rebalance, equal weight, top 30"
)

print("Indonesia (JKT) cumulative growth...")
chart_cumulative(
    ["JKT"], "jkt_cumulative_growth.png",
    "Growth of $10,000: Magic Formula Indonesia vs S&P 500 (2000-2025)",
    "Jakarta Stock Exchange, quarterly rebalance, equal weight, top 30"
)

print("Brazil (SAO) cumulative growth...")
chart_cumulative(
    ["SAO"], "brazil_cumulative_growth.png",
    "Growth of $10,000: Magic Formula Brazil vs S&P 500 (2000-2025)",
    "B3 (Bovespa), quarterly rebalance, equal weight, top 30"
)

print("Comparison CAGR chart...")
chart_comparison_cagr("comparison_cagr.png")

print(f"\nDone. Charts in {content_charts_dir}/")
