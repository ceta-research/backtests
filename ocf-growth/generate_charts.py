"""Generate all OCF Growth charts for blog posts from exchange_comparison.json."""
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
    "US_MAJOR": "#1a5276",
    "NYSE": "#2980b9",
    "NASDAQ": "#7fb3d8",
    "LSE": "#16a085",
    "XETRA": "#27ae60",
    "JPX": "#d35400",
    "HKSE": "#8e44ad",
    "KSC": "#2c3e50",
    "Taiwan": "#c0392b",
    "Indonesia": "#f39c12",
    "Thailand": "#27ae60",
    "Canada": "#7f8c8d",
    "China": "#e74c3c",
    "Sweden": "#1abc9c",
    "Switzerland": "#e67e22",
    "Norway": "#3498db",
    "India": "#ff6f00",
    "South_Africa": "#6a1b9a",
    "Saudi_Arabia": "#00695c",
    "Singapore": "#4527a0",
    "Mexico": "#ad1457",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "OCF Growth US (NYSE+NASDAQ+AMEX)",
    "LSE": "OCF Growth LSE (UK)",
    "XETRA": "OCF Growth XETRA (Germany)",
    "JPX": "OCF Growth JPX (Japan)",
    "HKSE": "OCF Growth HKSE (Hong Kong)",
    "KSC": "OCF Growth KSC (Korea)",
    "Taiwan": "OCF Growth Taiwan (TAI+TWO)",
    "Indonesia": "OCF Growth JKT (Indonesia)",
    "Thailand": "OCF Growth SET (Thailand)",
    "Canada": "OCF Growth TSX (Canada)",
    "China": "OCF Growth China (SHH+SHZ)",
    "Sweden": "OCF Growth STO (Sweden)",
    "Switzerland": "OCF Growth SIX (Switzerland)",
    "Norway": "OCF Growth OSL (Norway)",
    "India": "OCF Growth India (NSE)",
    "South_Africa": "OCF Growth JNB (South Africa)",
    "Saudi_Arabia": "OCF Growth SAU (Saudi Arabia)",
    "Singapore": "OCF Growth SGX (Singapore)",
    "Mexico": "OCF Growth BMV (Mexico)",
}

FOOTER = "Data: Ceta Research | OCF Growth >10%, Divergence >0, ROE >10%, OPM >5%, annual rebalance, equal weight, 2000-2025"


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
    """Get SPY cumulative from US data."""
    for k in ["US_MAJOR"]:
        if k in data and data[k].get("annual_returns"):
            ex = data[k]
            break
    else:
        for k in data:
            if data[k].get("annual_returns"):
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
        spy_cagr = data.get("US_MAJOR", {}).get("spy", {}).get("cagr", "?")
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
        if v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data.get("US_MAJOR", {}).get("spy", {}).get("cagr")
    if spy_cagr:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("OCF Growth CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
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
        if v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data.get("US_MAJOR", {}).get("spy", {}).get("max_drawdown")
    if spy_dd:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("OCF Growth Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
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


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0
        and v["portfolio"].get("sharpe_ratio") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no sharpe_ratio data")
        return

    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = data.get("US_MAJOR", {}).get("spy", {}).get("sharpe_ratio")
    if spy_sharpe is not None:
        ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("OCF Growth Sharpe Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, val in enumerate(sharpes):
        x_pos = max(val, 0) + 0.01
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

# Individual exchange charts (generate for exchanges with good data)
# These will be generated dynamically based on what's in the results
individual_exchanges = {
    "US_MAJOR": ("us", "US (NYSE+NASDAQ+AMEX)"),
    "India": ("india", "India (NSE)"),
    "LSE": ("uk", "UK (LSE)"),
    "XETRA": ("germany", "Germany (XETRA)"),
    "JPX": ("japan", "Japan (JPX)"),
    "HKSE": ("hongkong", "Hong Kong (HKSE)"),
    "Canada": ("canada", "Canada (TSX+TSXV)"),
    "SIX": ("switzerland", "Switzerland (SIX)"),
    "SET": ("thailand", "Thailand (SET)"),
    "China": ("china", "China (SHH+SHZ)"),
}

for ex_key, (region_slug, region_label) in individual_exchanges.items():
    if ex_key in data and data[ex_key].get("invested_periods", 0) > 0:
        print(f"Generating {region_label} charts...")
        chart_cumulative(
            [ex_key], f"1_{region_slug}_cumulative_growth.png",
            f"Growth of $10,000: OCF Growth {region_label} vs S&P 500 (2000-2025)",
            f"{region_label}, annual rebalance, equal weight"
        )
        chart_annual_bars(
            [ex_key], f"2_{region_slug}_annual_returns.png",
            f"OCF Growth {region_label} vs S&P 500: Year-by-Year Returns (2000-2024)",
            f"{region_label}, annual rebalance, equal weight"
        )

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")
chart_comparison_sharpe("3_comparison_sharpe.png")

print(f"\nDone. Charts generated in {charts_dir}/")
