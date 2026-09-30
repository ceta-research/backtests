"""Generate all QARP charts for blog posts.

US and comparison charts read exchange_comparison.json. Regional charts read the
per-exchange *_results.json, which carry the local index in "spy" and match the posts.
"""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_label, localize_money_title,
                         money, money_axis_label, money_formatter)
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Regional runs against the local index (these are the figures the regional posts quote)
local = {}
for _ex in ["BSE", "NSE", "XETRA", "SHH", "SHZ", "HKSE"]:
    with open(results_dir / f"{_ex.lower()}_results.json") as f:
        local[_ex] = json.load(f)

# Color palette
COLORS = {
    "US_MAJOR": "#1a5276",
    "NYSE": "#2980b9",
    "NASDAQ": "#7fb3d8",
    "BSE": "#e67e22",
    "NSE": "#f39c12",
    "XETRA": "#27ae60",
    "SHZ": "#c0392b",
    "SHH": "#e74c3c",
    "HKSE": "#8e44ad",
    "KSC": "#95a5a6",
    "ASX": "#bdc3c7",
    "TSX": "#7f8c8d",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "QARP US (NYSE+NASDAQ+AMEX)",
    "NYSE": "QARP NYSE",
    "NASDAQ": "QARP NASDAQ",
    "BSE": "QARP BSE (India)",
    "NSE": "QARP NSE (India)",
    "XETRA": "QARP XETRA (Germany)",
    "SHZ": "QARP Shenzhen",
    "SHH": "QARP Shanghai",
    "HKSE": "QARP HKSE (Hong Kong)",
    "KSC": "QARP KSC (Korea)",
    "ASX": "QARP ASX (Australia)",
    "TSX": "QARP TSX (Canada)",
}


def get_cumulative_growth(exchange_key, initial=10000, src=None):
    """Compute cumulative growth from annual returns."""
    ex = (src or data)[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]  # start year
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, src=None, bench_key="US_MAJOR"):
    """Generate cumulative growth chart for given exchanges vs bench_key's benchmark."""
    src = src or data
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = benchmark_cumulative(src, bench_key)
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_label(src, bench_key)} ({src[bench_key]['spy']['cagr']:.2f}% CAGR)",
            linestyle="--")

    for ex_key in exchanges:
        ex = src[ex_key]
        years, vals = get_cumulative_growth(ex_key, src=src)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS[ex_key]} ({cagr:.2f}% CAGR)"
        ax.plot(years, vals, color=COLORS[ex_key], linewidth=2.2, label=label)

        # Final value annotation
        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS[ex_key])

    # SPY final value
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

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, semi-annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe, src=None, bench_key=None,
                      legend_loc="upper left"):
    """Generate annual returns bar chart for given exchanges vs bench_key's benchmark."""
    src = src or data
    bench_key = bench_key or exchanges[0]
    ex = src[bench_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1  # exchanges + SPY
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    # SPY bars
    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=benchmark_label(src, bench_key), color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(exchanges):
        returns = [ar["portfolio"] for ar in src[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS[ex_key], color=COLORS[ex_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc=legend_loc, ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, semi-annual rebalance, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange (all exchanges with data)."""
    # Sort by CAGR descending, exclude zero-data exchanges
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and not v.get("window_truncated", False)
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = []
    cagrs = []
    colors = []
    for k, v in exchanges_with_data:
        cagr = v["portfolio"]["cagr"]
        names.append(k)
        cagrs.append(cagr)
        colors.append(COLORS.get(k, "#95a5a6"))

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    # SPY reference line
    spy_cagr = data["US_MAJOR"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("QARP CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    # Value labels on bars
    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
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
        if v["invested_periods"] > 0 and not v.get("window_truncated", False)
    ]
    # Sort by drawdown (least negative = best at top)
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    # SPY reference line
    spy_dd = data["US_MAJOR"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("QARP Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sortino(filename):
    """Horizontal bar chart: Sortino ratio by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and not v.get("window_truncated", False)
        and v["portfolio"].get("sortino_ratio") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no sortino_ratio data in exchange_comparison.json")
        return

    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sortino_ratio"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    sortinos = [v["portfolio"]["sortino_ratio"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), sortinos, color=colors, alpha=0.85, height=0.6)

    # SPY reference line (if available)
    spy_sortino = data.get("US_MAJOR", {}).get("spy", {}).get("sortino_ratio")
    if spy_sortino is not None:
        ax.axvline(x=spy_sortino, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sortino:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sortino Ratio", fontsize=12, fontweight="bold")
    ax.set_title("QARP Sortino Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, val) in enumerate(zip(bars, sortinos)):
        x_pos = max(val, 0) + 0.02
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_capture(filename):
    """Scatter plot: Up capture (x) vs Down capture (y) by exchange.

    Ideal zone: upper-right for up capture, lower values for down capture.
    Points in bottom-right quadrant are best (high upside, low downside).
    """
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v["invested_periods"] > 0 and not v.get("window_truncated", False)
        and v.get("comparison", {}).get("up_capture") is not None
        and v.get("comparison", {}).get("down_capture") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no up_capture/down_capture data in exchange_comparison.json")
        return

    fig, ax = plt.subplots(figsize=(10, 10))

    for k, v in exchanges_with_data:
        up = v["comparison"]["up_capture"]
        down = v["comparison"]["down_capture"]
        color = COLORS.get(k, "#95a5a6")
        ax.scatter(up, down, color=color, s=120, zorder=5, edgecolors="white", linewidth=1)
        ax.annotate(k, (up, down), textcoords="offset points", xytext=(8, 4),
                    fontsize=10, fontweight="bold", color=color)

    # Reference lines at 100%
    ax.axvline(x=100, color="#bdc3c7", linewidth=1, linestyle="--", alpha=0.7)
    ax.axhline(y=100, color="#bdc3c7", linewidth=1, linestyle="--", alpha=0.7)

    # Ideal zone annotation
    ax.annotate("Ideal zone\n(high up, low down)",
                xy=(0.95, 0.05), xycoords="axes fraction",
                fontsize=9, color="#27ae60", alpha=0.6, ha="right",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#e8f8f5", edgecolor="#27ae60", alpha=0.3))

    ax.set_xlabel("Up Capture (%)", fontsize=12, fontweight="bold")
    ax.set_ylabel("Down Capture (%)", fontsize=12, fontweight="bold")
    ax.set_title("QARP Up/Down Capture by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same 7-factor QARP screen, semi-annual rebalance, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating charts for blog.md (US)...")
chart_cumulative(
    ["US_MAJOR"], "us_cumulative_growth.png",
    "Growth of $10,000: QARP US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["US_MAJOR"], "us_annual_returns.png",
    "QARP US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

print("Generating charts for blog_india.md...")
chart_cumulative(
    ["BSE", "NSE"], "india_cumulative_growth.png",
    "Growth of $10,000: QARP India vs Sensex (2000-2025)",
    "BSE + NSE (returns in INR)", src=local, bench_key="BSE"
)
chart_annual_bars(
    ["BSE", "NSE"], "india_annual_returns.png",
    "QARP India vs Sensex: Year-by-Year Returns (2000-2025)",
    "BSE + NSE (returns in INR)", src=local, bench_key="BSE",
    legend_loc="upper right"  # upper left hides the 2003 Sensex bar
)

print("Generating charts for blog_germany.md...")
chart_cumulative(
    ["XETRA"], "germany_cumulative_growth.png",
    "Growth of $10,000: QARP Germany vs DAX (2000-2025)",
    "XETRA (returns in EUR)", src=local, bench_key="XETRA"
)
chart_annual_bars(
    ["XETRA"], "germany_annual_returns.png",
    "QARP Germany vs DAX: Year-by-Year Returns (2000-2025)",
    "XETRA (returns in EUR)", src=local, bench_key="XETRA"
)

# SSE Composite is Shanghai's benchmark; SHZ's "spy" is an S&P 500 fallback, so it is not plotted
print("Generating charts for blog_china.md...")
chart_cumulative(
    ["SHH", "SHZ"], "china_cumulative_growth.png",
    "Growth of $10,000: QARP China vs SSE Composite (2000-2025)",
    "SHH + SHZ (returns in CNY), no SZSE index in data for SHZ", src=local, bench_key="SHH"
)
chart_annual_bars(
    ["SHH"], "china_annual_returns.png",
    "QARP Shanghai vs SSE Composite: Year-by-Year Returns (2000-2025)",
    "SHH (returns in CNY)", src=local, bench_key="SHH"
)

print("Generating charts for blog_hongkong.md...")
chart_cumulative(
    ["HKSE"], "hongkong_cumulative_growth.png",
    "Growth of $10,000: QARP Hong Kong vs Hang Seng (2000-2025)",
    "HKSE (returns in HKD)", src=local, bench_key="HKSE"
)
chart_annual_bars(
    ["HKSE"], "hongkong_annual_returns.png",
    "QARP Hong Kong vs Hang Seng: Year-by-Year Returns (2000-2025)",
    "HKSE (returns in HKD)", src=local, bench_key="HKSE"
)

print("Generating charts for blog_comparison.md...")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_drawdown("comparison_drawdown.png")
chart_comparison_sortino("comparison_sortino.png")
chart_comparison_capture("comparison_capture.png")

print(f"\nDone. Charts generated in {charts_dir}/")
