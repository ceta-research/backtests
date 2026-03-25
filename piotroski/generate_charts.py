#!/usr/bin/env python3
"""Generate Piotroski F-Score charts from per-exchange result JSONs.

Generates:
- Per-exchange: cumulative growth (Score 8-9 vs SPY), annual returns bar chart
- Comparison: CAGR spread by exchange, max drawdown comparison

Usage:
    cd backtests
    python3 piotroski/generate_charts.py
"""

import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

# Load all exchange results
exchange_data = {}
for f in sorted(results_dir.glob("piotroski_*.json")):
    name = f.stem.replace("piotroski_", "")
    if name == "metrics_US":
        continue
    with open(f) as fh:
        data = json.load(fh)
    if "error" not in data and "annual_returns" in data:
        exchange_data[name] = data

COLORS = {
    "score_8_9": "#1a5276",
    "score_0_2": "#c0392b",
    "all_value": "#27ae60",
    "spy": "#aab7b8",
}

EXCHANGE_COLORS = {
    "US_MAJOR": "#1a5276", "JPX": "#e67e22", "India": "#27ae60",
    "LSE": "#8e44ad", "ASX": "#c0392b", "HKSE": "#2980b9",
    "KSC": "#f39c12", "China": "#e74c3c", "Taiwan": "#95a5a6",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "US (NYSE+NASDAQ+AMEX)", "JPX": "Japan (JPX)",
    "India": "India (NSE)", "LSE": "UK (LSE)",
    "ASX": "Australia (ASX)", "HKSE": "Hong Kong (HKSE)",
    "KSC": "Korea (KSC)", "China": "China (SHZ+SHH)",
    "Taiwan": "Taiwan (TAI+TWO)",
    "Canada": "Canada (TSX)", "STO": "Sweden (STO)",
    "SET": "Thailand (SET)", "SAO": "Brazil (SAO)",
    "JSE": "South Africa (JNB)", "SIX": "Switzerland (SIX)",
    "OSL": "Norway (OSL)", "XETRA": "Germany (XETRA)",
}


def chart_cumulative_piotroski(exchange_key, region_label, filename):
    """Cumulative growth: Score 8-9 vs All Value vs Score 0-2 vs SPY."""
    data = exchange_data[exchange_key]
    ar = data["annual_returns"]

    # Filter to years with SPY data and high_count > 0 for at least some years
    ar = [y for y in ar if y["spy"] is not None]
    if len(ar) < 5:
        print(f"  Skipping {filename}: only {len(ar)} valid years")
        return

    start_year = ar[0]["year"] - 1
    tracks = {
        "Score 8-9": ("high", COLORS["score_8_9"]),
        "All Value": ("all", COLORS["all_value"]),
        "Score 0-2": ("low", COLORS["score_0_2"]),
        "S&P 500": ("spy", COLORS["spy"]),
    }

    fig, ax = plt.subplots(figsize=(12, 6))

    for label, (key, color) in tracks.items():
        vals = [10000]
        years = [start_year]
        for y in ar:
            ret = y[key]
            if ret is None:
                ret = 0
            vals.append(vals[-1] * (1 + ret / 100))
            years.append(y["year"])

        ls = "--" if key == "spy" else "-"
        lw = 1.8 if key == "spy" else 2.2

        p = data["portfolios"]
        track_map = {"high": "score_8_9", "low": "score_0_2",
                     "all": "all_value", "spy": "sp500"}
        cagr = p[track_map[key]]["cagr"]

        ax.plot(years, vals, color=color, linewidth=lw, linestyle=ls,
                label=f"{label} ({cagr:.1f}% CAGR)")

        final_k = vals[-1] / 1000
        offset_y = {"high": 8, "all": -4, "low": -16, "spy": -28}.get(key, 0)
        ax.annotate(f"${final_k:,.0f}K", xy=(years[-1], vals[-1]),
                    xytext=(8, offset_y), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=color)

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(f"Piotroski F-Score: Growth of $10,000 on {region_label}",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=9, loc="upper left")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {region_label}, annual rebalance (April), "
             f"equal weight, bottom 20% P/B value universe",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars_piotroski(exchange_key, region_label, filename):
    """Annual returns bar chart: Score 8-9 vs SPY."""
    data = exchange_data[exchange_key]
    ar = [y for y in data["annual_returns"]
          if y["spy"] is not None and y["year"] >= 2000]

    if len(ar) < 5:
        print(f"  Skipping {filename}: only {len(ar)} years post-2000")
        return

    years = [y["year"] for y in ar]
    high_rets = [y["high"] for y in ar]
    spy_rets = [y["spy"] for y in ar]

    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.35
    x = list(range(len(years)))

    ax.bar([i - width/2 for i in x], spy_rets, width,
           label="S&P 500", color=COLORS["spy"], alpha=0.7)
    ax.bar([i + width/2 for i in x], high_rets, width,
           label="Score 8-9", color=COLORS["score_8_9"], alpha=0.85)

    # Add stock count annotations
    for i, y in enumerate(ar):
        count = y["high_count"]
        if count > 0:
            y_pos = max(high_rets[i], 0) + 2
            ax.text(i + width/2, y_pos, str(count),
                    ha="center", fontsize=7, color="#555", alpha=0.7)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"Piotroski Score 8-9 vs S&P 500: {region_label} ({years[0]}-{years[-1]})",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {region_label}, annual rebalance (April). "
             f"Numbers above bars = stock count in Score 8-9 portfolio.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_spread(filename):
    """Horizontal bar: Score 8-9 vs 0-2 CAGR spread by exchange."""
    items = []
    for name, d in exchange_data.items():
        spread = d.get("spread_cagr", 0)
        h_cagr = d["portfolios"]["score_8_9"]["cagr"]
        items.append((name, spread, h_cagr))

    items.sort(key=lambda x: x[1], reverse=True)

    names = [EXCHANGE_LABELS.get(i[0], i[0]) for i in items]
    spreads = [i[1] for i in items]
    colors = ["#27ae60" if s >= 0 else "#c0392b" for s in spreads]

    fig, ax = plt.subplots(figsize=(10, max(6, len(items) * 0.45)))
    bars = ax.barh(range(len(names)), spreads, color=colors, alpha=0.85,
                   height=0.6)

    ax.axvline(x=0, color="black", linewidth=1)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR Spread: Score 8-9 minus Score 0-2 (%)",
                  fontsize=11, fontweight="bold")
    ax.set_title("Piotroski F-Score Spread by Exchange",
                 fontsize=13, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, spread) in enumerate(zip(bars, spreads)):
        x_pos = spread + 0.5 if spread >= 0 else spread - 3
        ax.text(x_pos, i, f"{spread:+.1f}%", va="center", fontsize=10,
                fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Annual rebalance (April), equal weight, "
             "bottom 20% P/B value universe",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_risk(filename):
    """Grouped bar: Sharpe and MaxDD comparison for Score 8-9 across exchanges."""
    # Filter to exchanges with decent data
    blog_exchanges = ["US_MAJOR", "JPX", "India", "LSE", "ASX", "HKSE",
                      "KSC", "China", "Taiwan"]
    items = [(name, exchange_data[name]) for name in blog_exchanges
             if name in exchange_data]

    names = [EXCHANGE_LABELS.get(n, n) for n, _ in items]
    sharpes = [d["portfolios"]["score_8_9"]["sharpe"] for _, d in items]
    maxdds = [d["portfolios"]["score_8_9"]["max_drawdown"] for _, d in items]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Sharpe
    colors_s = [EXCHANGE_COLORS.get(n, "#95a5a6") for n, _ in items]
    ax1.barh(range(len(names)), sharpes, color=colors_s, alpha=0.85,
             height=0.6)
    ax1.axvline(x=0, color="black", linewidth=0.5)
    ax1.set_yticks(range(len(names)))
    ax1.set_yticklabels(names, fontsize=9)
    ax1.invert_yaxis()
    ax1.set_xlabel("Sharpe Ratio", fontsize=11, fontweight="bold")
    ax1.set_title("Score 8-9 Sharpe Ratio", fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3, axis="x", linestyle="--")
    for i, v in enumerate(sharpes):
        ax1.text(max(v, 0) + 0.01, i, f"{v:.3f}", va="center", fontsize=9)

    # MaxDD
    ax2.barh(range(len(names)), maxdds, color=colors_s, alpha=0.85,
             height=0.6)
    ax2.set_yticks(range(len(names)))
    ax2.set_yticklabels(names, fontsize=9)
    ax2.invert_yaxis()
    ax2.set_xlabel("Max Drawdown (%)", fontsize=11, fontweight="bold")
    ax2.set_title("Score 8-9 Max Drawdown", fontsize=12, fontweight="bold")
    ax2.grid(True, alpha=0.3, axis="x", linestyle="--")
    for i, v in enumerate(maxdds):
        ax2.text(v - 2, i, f"{v:.1f}%", va="center", fontsize=9)

    fig.suptitle("Piotroski F-Score Risk Metrics by Exchange",
                 fontsize=14, fontweight="bold", y=1.02)

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print(f"Found {len(exchange_data)} exchanges with period data\n")

# Per-exchange charts (blog-eligible exchanges)
blog_exchanges = {
    "US_MAJOR": "US (NYSE + NASDAQ + AMEX)",
    "JPX": "Japan (JPX)",
    "India": "India (NSE)",
    "LSE": "UK (LSE)",
    "ASX": "Australia (ASX)",
    "HKSE": "Hong Kong (HKSE)",
}

for key, label in blog_exchanges.items():
    if key not in exchange_data:
        print(f"  Skipping {key}: no data")
        continue
    region = key.lower()
    if region == "us_major":
        region = "us"
    print(f"Generating charts for {label}...")
    chart_cumulative_piotroski(key, label, f"1_{region}_cumulative_growth.png")
    chart_annual_bars_piotroski(key, label, f"2_{region}_annual_returns.png")

# Comparison charts
print("\nGenerating comparison charts...")
chart_comparison_spread("1_comparison_spread.png")
chart_comparison_risk("2_comparison_risk.png")

print(f"\nDone. {len(os.listdir(charts_dir))} charts in {charts_dir}/")
