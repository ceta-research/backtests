"""Generate all Small-Cap Growth charts for blog posts from exchange_comparison.json."""
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
    "TAI": "#1a252f",
    "SET": "#5b2c6f",
    "JNB": "#117a65",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Small-Cap US",
    "NSE": "Small-Cap India",
    "XETRA": "Small-Cap Germany",
    "STO": "Small-Cap Sweden",
    "TSX": "Small-Cap Canada",
    "SHZ_SHH": "Small-Cap China",
    "HKSE": "Small-Cap HK",
    "JPX": "Small-Cap Japan",
    "LSE": "Small-Cap UK",
    "KSC": "Small-Cap Korea",
    "SIX": "Small-Cap Switzerland",
    "TAI": "Small-Cap Taiwan",
    "SET": "Small-Cap Thailand",
    "JNB": "Small-Cap South Africa",
}

EXCHANGE_DISPLAY_NAMES = {
    "NYSE_NASDAQ_AMEX": "US (NYSE+NASDAQ+AMEX)",
    "NSE": "India (NSE)",
    "XETRA": "Germany (XETRA)",
    "STO": "Sweden (STO)",
    "TSX": "Canada (TSX)",
    "SHZ_SHH": "China (SHZ+SHH)",
    "HKSE": "Hong Kong",
    "JPX": "Japan (JPX)",
    "LSE": "UK (LSE)",
    "KSC": "Korea (KSC)",
    "SIX": "Switzerland (SIX)",
    "TAI": "Taiwan (TAI)",
    "SET": "Thailand (SET)",
    "JNB": "South Africa (JNB)",
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
    """Generate cumulative growth chart for given exchanges vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative(exchanges[0])
    # Take the CAGR from the SAME entry the series was built from. Reading it
    # off the US entry printed the S&P 500's 7.85% next to a "DAX" or "SMI"
    # label, which is a wrong number under a right name.
    ref_key = exchanges[0] if exchanges[0] in data else list(data.keys())[0]
    spy_cagr = data[ref_key]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_label(data, exchanges[0])} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in data:
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
             f"Data: Ceta Research | {footer_universe}, annual rebalance (July), 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart."""
    ex_key = exchanges[0]
    if ex_key not in data:
        print(f"  Skipped (no data): {filename}")
        return

    ex = data[ex_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=benchmark_label(data, exchanges[0]), color=COLORS["SPY"], alpha=0.7)

    for idx, ek in enumerate(exchanges):
        if ek not in data:
            continue
        returns = [ar["portfolio"] for ar in data[ek]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ek, ek), color=COLORS.get(ek, "#95a5a6"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance (July), 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0 and v.get("portfolio", {}).get("cagr") is not None
        and not v.get("window_truncated", False)
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_DISPLAY_NAMES.get(k, k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    raw_keys = [k for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in raw_keys]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.5 + 1)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data.get("NYSE_NASDAQ_AMEX", data[list(data.keys())[0]])["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Small-Cap Growth: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Rev growth >15%, netIncome >0, D/E <2.0, annual rebalance",
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
        if v.get("invested_periods", 0) > 0 and v.get("portfolio", {}).get("max_drawdown") is not None
        and not v.get("window_truncated", False)
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])  # Most negative first

    names = [EXCHANGE_DISPLAY_NAMES.get(k, k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    raw_keys = [k for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in raw_keys]

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.5 + 1)))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Small-Cap Growth: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.axvline(x=0, color="black", linewidth=0.5)

    for i, dd in enumerate(drawdowns):
        ax.text(dd - 0.5, i, f"{dd:.1f}%", va="center", ha="right",
                fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Small-Cap Growth strategy, annual rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Per-market charts.
#
# (result key, blog slug, chart country name, footer universe). Every market with
# a regional blog needs an entry: the earlier version of this script only covered
# six of them, so the other seven blogs kept charts from an older run with stale
# numbers while the script reported success.
MARKETS = [
    ("NYSE_NASDAQ_AMEX", "us",           "US",           "NYSE + NASDAQ + AMEX"),
    ("NSE",              "india",        "India",        "NSE (returns in INR)"),
    ("SHZ_SHH",          "china",        "China",        "China (SHZ+SHH, returns in CNY)"),
    ("JNB",              "southafrica",  "South Africa", "South Africa (JNB, returns in ZAR)"),
    ("TSX",              "canada",       "Canada",       "Canada (TSX, returns in CAD)"),
    ("SIX",              "switzerland",  "Switzerland",  "Switzerland (SIX, returns in CHF)"),
    ("STO",              "sweden",       "Sweden",       "Sweden (STO, returns in SEK)"),
    ("XETRA",            "germany",      "Germany",      "Germany (XETRA, returns in EUR)"),
    ("KSC",              "korea",        "Korea",        "Korea (KSC, returns in KRW)"),
    ("LSE",              "uk",           "UK",           "UK (LSE, returns in GBP)"),
    ("TAI",              "taiwan",       "Taiwan",       "Taiwan (TAI, returns in TWD)"),
    ("SET",              "thailand",     "Thailand",     "Thailand (SET, returns in THB)"),
    ("JPX",              "japan",        "Japan",        "Japan (JPX, returns in JPY)"),
    ("HKSE",             "hongkong",     "Hong Kong",    "Hong Kong (HKSE, returns in HKD)"),
]

print("Generating charts for Small-Cap Growth blogs...")

missing = [k for k, _, _, _ in MARKETS if k not in data]
if missing:
    print(f"  WARNING: no results for {missing}; their charts will not be refreshed")

for key, slug, country, footer in MARKETS:
    if key not in data:
        continue
    print(f"{country} charts...")
    chart_cumulative(
        [key], f"1_{slug}_cumulative_growth.png",
        f"Growth of $10,000: Small-Cap Growth {country} vs {benchmark_label(data, key)} (2000-2025)",
        footer
    )
    chart_annual_bars(
        [key], f"2_{slug}_annual_returns.png",
        f"Small-Cap Growth {country}: Year-by-Year Returns (2000-2024)",
        footer
    )

# Comparison charts
print("Comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
