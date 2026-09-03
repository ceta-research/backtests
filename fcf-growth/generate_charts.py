"""Generate all FCF Growth charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import benchmark_label, benchmark_cumulative

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
    "STO": "#2e86c1",
    "TSX": "#7f8c8d",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "JPX": "#6e2f1a",
    "LSE": "#154360",
    "KSC": "#6c3483",
    "SIX": "#d68910",
    "TAI_TWO": "#1a252f",
    "JNB": "#6e7f80",
    "SET": "#b03a2e",
    "SPY": "#aab7b8",
}

# Returns are in each exchange's local currency, so the axis and the annotations
# must not all say "$". A EUR chart labelled "$10,000" contradicts the blog beside it.
CURRENCY = {
    "NYSE_NASDAQ_AMEX": "$", "NSE": "\u20b9", "XETRA": "\u20ac", "LSE": "\u00a3",
    "TSX": "C$", "JPX": "\u00a5", "KSC": "\u20a9", "TAI_TWO": "NT$",
    "SHZ_SHH": "\u00a5", "HKSE": "HK$", "STO": "kr", "SIX": "CHF ",
    "SET": "\u0e3f", "JNB": "R",
}


def cur(exchange_key):
    return CURRENCY.get(exchange_key, "$")


EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "FCF Growth US",
    "NSE": "FCF Growth India",
    "XETRA": "FCF Growth Germany",
    "STO": "FCF Growth Sweden",
    "TSX": "FCF Growth Canada",
    "SHZ_SHH": "FCF Growth China",
    "HKSE": "FCF Growth HK",
    "JPX": "FCF Growth Japan",
    "LSE": "FCF Growth UK",
    "KSC": "FCF Growth Korea",
    "SIX": "FCF Growth Switzerland",
    "TAI_TWO": "FCF Growth Taiwan",
    "JNB": "FCF Growth SA",
    "SET": "FCF Growth Thailand",
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


def get_spy_cumulative(ref_key, initial=10000):
    """Cumulative growth of THAT exchange's own benchmark series.

    The "spy" field holds whichever index the exchange was measured against,
    which for non-US markets is the local index.
    """
    return benchmark_cumulative(data, ref_key, initial)


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Generate cumulative growth chart for given exchanges vs their benchmark."""
    fig, ax = plt.subplots(figsize=(12, 6))
    sym = cur(exchanges[0])

    spy_years, spy_vals = get_spy_cumulative(exchanges[0])
    spy_cagr = data[exchanges[0]]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_label(data, exchanges[0])} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS[ex_key]} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS[ex_key], linewidth=2.2, label=label)

        final_k = vals[-1] / 1000
        ax.annotate(f"{sym}{final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS[ex_key])

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(f"{sym}{spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel(f"Portfolio Value ({sym.strip()})", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{sym}{x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, title, footer_universe):
    """Generate annual returns bar chart for a single exchange."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.38
    x = list(range(len(years)))

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label=benchmark_label(data, exchange_key), color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_returns, width,
           label=EXCHANGE_LABELS[exchange_key], color=COLORS[exchange_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename, eligible_exchanges):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if k in eligible_exchanges and v.get("invested_periods", 0) > 0
        and not v.get("window_truncated", False)
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.55)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    labels = [EXCHANGE_LABELS.get(n, n) for n in names]
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(labels, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Growth CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | FCF growth > 15%, OCF growth > 0%, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ============================================================
# Generate charts — update eligible_exchanges after results arrive
# ============================================================
print("Generating charts for FCF Growth blogs...")

# Eligible exchanges. Exclusions are about DATA COVERAGE, not about whether the
# strategy worked: dropping losers would cherry-pick the chart. SET stays in at
# 0.36% CAGR precisely because it's the clearest failure in the study.
#   JNB: only 8 of 25 periods investable, too sparse to plot as a 25-year record
#   SES: thin portfolio, avg 8.4 stocks, below the 10-stock minimum
# 10/25 keeps the UK (13/25) on the chart, because the blog discusses it at
# length as the cautionary tale, and drops JNB (8/25). Chart membership and the
# blog's 13-exchange table must match or the two contradict each other.
MIN_INVESTED_PERIODS = 10
ELIGIBLE = [k for k in data.keys()
            if not data[k].get("error")
            and data[k].get("invested_periods", 0) >= MIN_INVESTED_PERIODS
            and not data[k].get("window_truncated", False)
            and k not in ["SES"]]
_dropped = [(k, data[k].get("invested_periods", 0)) for k in data
            if not data[k].get("error") and k not in ELIGIBLE and k != "SES"]
if _dropped:
    print(f"  Excluded for coverage (<{MIN_INVESTED_PERIODS}/25 invested): "
          + ", ".join(f"{k} ({n}/25)" for k, n in _dropped))

print("US charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "us_cumulative_growth.png",
    "Growth of $10,000: FCF Growth US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    "NYSE_NASDAQ_AMEX", "us_annual_returns.png",
    "FCF Growth US: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

for ex_key in [k for k in ELIGIBLE if k != "NYSE_NASDAQ_AMEX"]:
    label = EXCHANGE_LABELS.get(ex_key, ex_key)
    region = ex_key.lower().replace("_", "-")
    print(f"{label} charts...")
    chart_cumulative(
        [ex_key], f"{region}_cumulative_growth.png",
        f"Growth of {cur(ex_key)}10,000: {label} vs {benchmark_label(data, ex_key)} (2000-2025)",
        ex_key
    )
    chart_annual_bars(
        ex_key, f"{region}_annual_returns.png",
        f"{label}: Year-by-Year Returns (2000-2024)",
        ex_key
    )

print("Comparison chart...")
chart_comparison_cagr("comparison_cagr.png", ELIGIBLE)

print(f"\nDone. Charts saved to {charts_dir}/")
