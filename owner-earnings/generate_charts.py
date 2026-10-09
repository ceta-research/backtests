"""Generate all Owner Earnings Yield charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import json
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cagr, benchmark_cumulative, benchmark_label, localize_money_title, money,
                         money_axis_label, money_formatter)

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
    "India": "#e67e22",
    "STO": "#1abc9c",
    "SIX": "#e67e22",
    "Norway": "#3498db",
    "JSE": "#8e44ad",
    "SAU": "#f1c40f",
    "TLV": "#9b59b6",
    "SET": "#27ae60",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "OE Yield US (NYSE+NASDAQ+AMEX)",
    "LSE": "OE Yield LSE (UK)",
    "XETRA": "OE Yield XETRA (Germany)",
    "JPX": "OE Yield JPX (Japan)",
    "HKSE": "OE Yield HKSE (Hong Kong)",
    "KSC": "OE Yield KSC (Korea)",
    "Taiwan": "OE Yield Taiwan (TAI+TWO)",
    "Indonesia": "OE Yield JKT (Indonesia)",
    "SET": "OE Yield SET (Thailand)",
    "Canada": "OE Yield TSX (Canada)",
    "China": "OE Yield China (SHH+SHZ)",
    "India": "OE Yield India (NSE)",
    "STO": "OE Yield STO (Sweden)",
    "SIX": "OE Yield SIX (Switzerland)",
    "Norway": "OE Yield OSL (Norway)",
    "JSE": "OE Yield JNB (South Africa)",
    "SAU": "OE Yield SAU (Saudi Arabia)",
    "TLV": "OE Yield TLV (Israel)",
}

FOOTER = "Data: Ceta Research | OE Yield >5%, ROE >10%, OPM >10%, annual rebalance, equal weight, 2000-2025"

# Excluded from the comparison post (EXCLUDED_PRESETS in backtest.py) but still in exchange_comparison.json
COMPARISON_EXCLUDED = {"XETRA"}
# No local index data: measured against SPY in USD, so the gap is cross-currency
CROSS_CCY_EXCHANGES = {"JSE", "TLV", "SAU"}
EXCESS_POS_COLOR = "#4CAF50"
EXCESS_NEG_COLOR = "#F44336"
CROSS_CCY_COLOR = "#9E9E9E"
COMPARISON_NAMES = {
    "US_MAJOR": "US (NYSE+NASDAQ+AMEX)", "India": "India (NSE)", "STO": "Sweden (STO)", "LSE": "UK (LSE)",
    "JSE": "South Africa (JSE)", "China": "China (SHH+SHZ)", "TLV": "Israel (TLV)", "JPX": "Japan (JPX)",
    "HKSE": "Hong Kong (HKSE)", "Taiwan": "Taiwan (TAI+TWO)", "SET": "Thailand (SET)", "KSC": "Korea (KSC)",
    "SIX": "Switzerland (SIX)", "SAU": "Saudi (SAU)",
}
# USD conversions over the backtest window (USDZAR 6.8055 -> 17.5786, USDILS 4.063 -> 3.3765, Jul 2000-Jul 2025)
COMPARISON_NOTE = ("Each exchange in local currency vs its own local index. JSE, TLV and SAU have no local index data, "
                   "so their local returns sit against the S&P 500 in USD.\n"
                   "Converted to USD (Jul 2000-Jul 2025): JSE roughly 4.6% (USD/ZAR +3.9%/yr), TLV roughly 7.0%, "
                   "SAU 3.6% (riyal pegged). All three trail the S&P 500's 7.85%.")


def comparison_exchanges(extra=lambda v: True):
    """Exchanges in the comparison post: invested, full window, not excluded."""
    return [
        (k, v) for k, v in data.items()
        if k not in COMPARISON_EXCLUDED and v.get("invested_periods", 0) > 0
        and not v.get("window_truncated", False) and extra(v)
    ]


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
    if spy_years:
        # Per-exchange, not US_MAJOR: the series and label are per-exchange
        spy_cagr = data.get(exchanges[0], {}).get("spy", {}).get("cagr", "?")
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"{benchmark_label(data, exchanges[0])} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if (ex_key not in data or data[ex_key]["invested_periods"] == 0
                or data[ex_key].get("window_truncated", False)):
            continue
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    if spy_years:
        spy_final_k = spy_vals[-1] / 1000
        ax.annotate(money(spy_final_k, exchanges[0], suffix="K"),
                    xy=(spy_years[-1], spy_vals[-1]),
                    xytext=(8, -12), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel(money_axis_label(exchanges[0]), fontsize=12, fontweight="bold")
    ax.set_title(localize_money_title(title, exchanges[0]), fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(exchanges[0]))
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
    active = [e for e in exchanges if e in data and data[e]["invested_periods"] > 0
              and not data[e].get("window_truncated", False)]
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
           label=benchmark_label(data, exchanges[0]), color=COLORS["SPY"], alpha=0.7)

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
    """CAGR per exchange, coloured by result vs its own local index, with the benchmark CAGR as a marker."""
    exchanges_with_data = comparison_exchanges()
    # Sorted by CAGR, as in the post's table
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    keys = [k for k, _ in exchanges_with_data]
    names = [COMPARISON_NAMES.get(k, k) for k in keys]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    excess = [v["excess_cagr"] for _, v in exchanges_with_data]
    benches = [benchmark_cagr(data, k) for k in keys]
    colors = [CROSS_CCY_COLOR if k in CROSS_CCY_EXCHANGES
              else EXCESS_POS_COLOR if e > 0 else EXCESS_NEG_COLOR for k, e in zip(keys, excess)]

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, range(len(keys)), marker="|", s=500, linewidths=3, color="black", zorder=3)

    for i, (k, cagr, ex, bench) in enumerate(zip(keys, cagrs, excess, benches)):
        # Cross-currency rows get no excess figure: local CAGR vs a USD index isn't alpha
        label = (f"{cagr:.2f}% (no local index; S&P 500 in USD)" if k in CROSS_CCY_EXCHANGES
                 else f"{cagr:.2f}% ({ex:+.2f}% vs {benchmark_label(data, k)})")
        ax.text(max(cagr, bench, 0) + 0.4, i, label, va="center", fontsize=9)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_xlim(0, max(max(c, b) for c, b in zip(cagrs, benches)) + 10)
    ax.set_title("Owner Earnings Yield CAGR vs Local Benchmark by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(handles=[
        Patch(color=EXCESS_POS_COLOR, alpha=0.85, label="Beat local index"),
        Patch(color=EXCESS_NEG_COLOR, alpha=0.85, label="Trailed local index"),
        Patch(color=CROSS_CCY_COLOR, alpha=0.85, label="No local index (vs S&P 500, USD)"),
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark CAGR"),
    ], fontsize=10, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.01, -0.02, COMPARISON_NOTE, fontsize=8, color="#555555", ha="left")
    fig.text(0.5, -0.06, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data = comparison_exchanges()
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
    ax.set_title("Owner Earnings Yield Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
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
    exchanges_with_data = comparison_exchanges(lambda v: v["portfolio"].get("sharpe_ratio") is not None)
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
    ax.set_title("Owner Earnings Yield Sharpe Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
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
# Charts are generated based on which exchanges appear in results.
# Regional charts are added dynamically based on exchange_comparison.json content.

print("Generating US charts...")
chart_cumulative(
    ["US_MAJOR"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Owner Earnings Yield US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"
)
chart_annual_bars(
    ["US_MAJOR"], "2_us_annual_returns.png",
    "Owner Earnings Yield US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"
)

# Generate charts for each exchange that has data
REGIONAL_CHART_MAP = {
    "LSE": ("uk", "UK", "LSE (returns and benchmark in GBP)"),
    "XETRA": ("germany", "Germany", "XETRA (returns and benchmark in EUR)"),
    "JPX": ("japan", "Japan", "JPX (returns and benchmark in JPY)"),
    "HKSE": ("hongkong", "Hong Kong", "HKSE (HKD pegged to USD)"),
    "KSC": ("korea", "Korea", "KSC (returns and benchmark in KRW)"),
    "Taiwan": ("taiwan", "Taiwan", "TAI+TWO (returns and benchmark in TWD)"),
    "Indonesia": ("indonesia", "Indonesia", "JKT (returns and benchmark in IDR)"),
    "SET": ("thailand", "Thailand", "SET (returns and benchmark in THB)"),
    "Canada": ("canada", "Canada", "TSX (returns and benchmark in CAD)"),
    "China": ("china", "China", "SHH+SHZ (returns and benchmark in CNY)"),
    "India": ("india", "India", "NSE (returns and benchmark in INR)"),
    "STO": ("sweden", "Sweden", "STO (returns and benchmark in SEK)"),
    "SIX": ("switzerland", "Switzerland", "SIX (returns and benchmark in CHF)"),
    "Norway": ("norway", "Norway", "OSL (returns and benchmark in NOK)"),
    "JSE": ("southafrica", "South Africa", "JNB (returns and benchmark in ZAR)"),
    "SAU": ("saudi", "Saudi Arabia", "SAU (returns and benchmark in SAR)"),
    "TLV": ("israel", "Israel", "TLV (returns and benchmark in ILS)"),
}

for ex_key, (slug, name, footer) in REGIONAL_CHART_MAP.items():
    if (ex_key in data and data[ex_key].get("invested_periods", 0) > 0
            and not data[ex_key].get("window_truncated", False)):
        print(f"Generating {name} charts...")
        chart_cumulative(
            [ex_key], f"1_{slug}_cumulative_growth.png",
            f"Growth of $10,000: Owner Earnings Yield {name} vs {benchmark_label(data, ex_key)} (2000-2025)",
            footer
        )
        chart_annual_bars(
            [ex_key], f"2_{slug}_annual_returns.png",
            f"Owner Earnings Yield {name} vs {benchmark_label(data, ex_key)}: Year-by-Year Returns (2000-2024)",
            footer
        )

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")
chart_comparison_sharpe("3_comparison_sharpe.png")

print(f"\nDone. Charts generated in {charts_dir}/")
