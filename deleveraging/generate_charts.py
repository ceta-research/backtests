"""Generate all Deleveraging charts for blog posts from exchange_comparison.json."""
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
    "LSE": "#27ae60",
    "XETRA": "#2980b9",
    "TSX": "#7f8c8d",
    "SET": "#2ecc71",
    "STO": "#3498db",
    "HKSE": "#8e44ad",
    "SHZ_SHH": "#e74c3c",
    "SIX": "#1abc9c",
    "JNB": "#d35400",
    "OSL": "#16a085",
    "SES": "#f39c12",
    "TAI_TWO": "#95a5a6",
    "KSC": "#7d6608",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Deleveraging US",
    "NSE": "Deleveraging India",
    "JPX": "Deleveraging Japan",
    "LSE": "Deleveraging UK",
    "XETRA": "Deleveraging Germany",
    "TSX": "Deleveraging Canada",
    "SET": "Deleveraging Thailand",
    "STO": "Deleveraging Sweden",
    "HKSE": "Deleveraging Hong Kong",
    "SHZ_SHH": "Deleveraging China",
    "SIX": "Deleveraging Switzerland",
    "JNB": "Deleveraging South Africa",
    "OSL": "Deleveraging Norway",
    "SES": "Deleveraging Singapore",
    "TAI_TWO": "Deleveraging Taiwan",
    "KSC": "Deleveraging Korea",
}

FOOTER = ("Data: Ceta Research | Deleveraging (D/E YoY <-10%, prior D/E>0.1, ROE>8%), "
          "quarterly rebalance, equal weight, 2001-2025")


def is_clean(key, val):
    """Check if exchange has usable results."""
    if "error" in val:
        return False
    return val.get("invested_periods", 0) > 0


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
    """Get SPY cumulative growth from US data."""
    us_key = next((k for k in data if "NYSE" in k or "MAJOR" in k), None)
    if not us_key:
        return [], []
    ex = data[us_key]
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
        us_key = next((k for k in data if "NYSE" in k or "MAJOR" in k), None)
        spy_cagr = data[us_key]["spy"]["cagr"] if us_key else ""
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"S&P 500 ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in data or not is_clean(ex_key, data[ex_key]):
            print(f"  Skipping {ex_key}: no data")
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

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center",
             fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, title, footer_universe):
    """Generate annual returns bar chart for one exchange vs SPY."""
    if exchange_key not in data or not is_clean(exchange_key, data[exchange_key]):
        print(f"  Skipping {filename}: no data for {exchange_key}")
        return

    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.38
    x = list(range(len(years)))

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_returns, width,
           label=EXCHANGE_LABELS.get(exchange_key, exchange_key),
           color=COLORS.get(exchange_key, "#1a5276"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}", ha="center",
             fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange vs SPY."""
    exchanges_with_data = [(k, v) for k, v in data.items() if is_clean(k, v)]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, _ in exchanges_with_data]

    us_key = next((k for k in data if "NYSE" in k or "MAJOR" in k), None)
    spy_cagr = data[us_key]["spy"]["cagr"] if us_key else 0

    fig, ax = plt.subplots(figsize=(11, max(5, len(names) * 0.65)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Deleveraging Strategy: CAGR by Exchange (2001-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_excess(filename):
    """Horizontal bar chart: Excess CAGR by exchange."""
    exchanges_with_data = [(k, v) for k, v in data.items() if is_clean(k, v)]
    exchanges_with_data.sort(
        key=lambda x: x[1].get("comparison", {}).get("excess_cagr") or 0,
        reverse=True
    )

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in exchanges_with_data]
    excesses = [v.get("comparison", {}).get("excess_cagr") or 0 for _, v in exchanges_with_data]
    colors = ["#27ae60" if e >= 0 else "#e74c3c" for e in excesses]

    fig, ax = plt.subplots(figsize=(11, max(5, len(names) * 0.65)))
    ax.barh(range(len(names)), excesses, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=0, color="black", linewidth=1.0)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Excess CAGR vs S&P 500 (%)", fontsize=12, fontweight="bold")
    ax.set_title("Deleveraging Strategy: Excess Return by Exchange (2001-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, exc in enumerate(excesses):
        x_pos = exc + 0.1 if exc >= 0 else exc - 0.3
        ax.text(x_pos, i, f"{exc:+.2f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts (update after results are available) ----
# The exchange keys here must match the keys in exchange_comparison.json

# --- US ---
print("Generating US charts...")
us_key = next((k for k in data if "NYSE" in k or "MAJOR" in k), None)
if us_key:
    chart_cumulative(
        [us_key], "1_us_cumulative_growth.png",
        "Growth of $10,000: Deleveraging US vs S&P 500 (2001-2025)",
        "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight, size-tiered transaction costs"
    )
    chart_annual_bars(
        us_key, "2_us_annual_returns.png",
        "Deleveraging US vs S&P 500: Year-by-Year Returns (2001-2025)",
        "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight"
    )

# --- India ---
india_key = next((k for k in data if "NSE" in k), None)
if india_key:
    print("Generating India charts...")
    chart_cumulative(
        [india_key], "1_india_cumulative_growth.png",
        "Growth of $10,000: Deleveraging India vs S&P 500 (2001-2025)",
        "NSE, quarterly rebalance, equal weight (returns in INR, benchmark in USD)"
    )
    chart_annual_bars(
        india_key, "2_india_annual_returns.png",
        "Deleveraging India vs S&P 500: Year-by-Year Returns (2001-2025)",
        "NSE (returns in INR)"
    )

# --- Japan ---
jpx_key = next((k for k in data if k == "JPX"), None)
if jpx_key:
    print("Generating Japan charts...")
    chart_cumulative(
        [jpx_key], "1_japan_cumulative_growth.png",
        "Growth of $10,000: Deleveraging Japan vs S&P 500 (2001-2025)",
        "JPX, quarterly rebalance, equal weight (returns in JPY, benchmark in USD)"
    )
    chart_annual_bars(
        jpx_key, "2_japan_annual_returns.png",
        "Deleveraging Japan vs S&P 500: Year-by-Year Returns (2001-2025)",
        "JPX (returns in JPY)"
    )

# --- UK ---
lse_key = next((k for k in data if k == "LSE"), None)
if lse_key:
    print("Generating UK charts...")
    chart_cumulative(
        [lse_key], "1_uk_cumulative_growth.png",
        "Growth of $10,000: Deleveraging UK vs S&P 500 (2001-2025)",
        "LSE, quarterly rebalance, equal weight (returns in GBP, benchmark in USD)"
    )
    chart_annual_bars(
        lse_key, "2_uk_annual_returns.png",
        "Deleveraging UK vs S&P 500: Year-by-Year Returns (2001-2025)",
        "LSE (returns in GBP)"
    )

# --- Germany ---
xetra_key = next((k for k in data if k == "XETRA"), None)
if xetra_key:
    print("Generating Germany charts...")
    chart_cumulative(
        [xetra_key], "1_germany_cumulative_growth.png",
        "Growth of $10,000: Deleveraging Germany vs S&P 500 (2001-2025)",
        "XETRA, quarterly rebalance, equal weight (returns in EUR, benchmark in USD)"
    )
    chart_annual_bars(
        xetra_key, "2_germany_annual_returns.png",
        "Deleveraging Germany vs S&P 500: Year-by-Year Returns (2001-2025)",
        "XETRA (returns in EUR)"
    )

# --- Canada ---
tsx_key = next((k for k in data if k == "TSX"), None)
if tsx_key:
    print("Generating Canada charts...")
    chart_cumulative(
        [tsx_key], "1_canada_cumulative_growth.png",
        "Growth of $10,000: Deleveraging Canada vs S&P 500 (2001-2025)",
        "TSX, quarterly rebalance, equal weight (returns in CAD, benchmark in USD)"
    )
    chart_annual_bars(
        tsx_key, "2_canada_annual_returns.png",
        "Deleveraging Canada vs S&P 500: Year-by-Year Returns (2001-2025)",
        "TSX (returns in CAD)"
    )

# --- Comparison ---
print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_excess("2_comparison_excess.png")

print(f"\nDone. Charts saved to {charts_dir}/")
