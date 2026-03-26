#!/usr/bin/env python3
"""Generate Altman Z-Score charts from per-exchange result JSONs.

Generates:
- Per-exchange: cumulative growth (safe vs distress vs SPY), annual returns bar chart
- Comparison: CAGR bar chart across exchanges (sorted by spread), drawdown comparison

Usage:
    cd backtests
    python3 altman-z/generate_charts.py                       # all exchanges
    python3 altman-z/generate_charts.py --exchange US_MAJOR    # single exchange
    python3 altman-z/generate_charts.py --all                  # comparison charts only
"""

import argparse
import json
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

# Load all exchange results from per-exchange JSON files
exchange_data = {}
for f in sorted(results_dir.glob("altman_z_metrics_*.json")):
    name = f.stem.replace("altman_z_metrics_", "")
    with open(f) as fh:
        data = json.load(fh)
    if "error" not in data and "annual_returns" in data:
        exchange_data[name] = data

COLORS = {
    "safe": "#1a5276",
    "distress": "#c0392b",
    "gray": "#f39c12",
    "all_ex_distress": "#27ae60",
    "spy": "#aab7b8",
}

EXCHANGE_COLORS = {
    "US_MAJOR": "#1a5276", "India": "#27ae60", "XETRA": "#8e44ad",
    "China": "#e74c3c", "HKSE": "#2980b9", "Canada": "#7f8c8d",
    "LSE": "#16a085", "SIX": "#d35400", "STO": "#f39c12",
    "KSC": "#95a5a6", "SAO": "#c0392b", "Taiwan": "#e67e22",
    "SGX": "#2c3e50", "JSE": "#27ae60", "PAR": "#8e44ad",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "US (NYSE+NASDAQ+AMEX)", "India": "India (NSE)",
    "XETRA": "Germany (XETRA)", "China": "China (SHZ+SHH)",
    "HKSE": "Hong Kong (HKSE)", "Canada": "Canada (TSX)",
    "LSE": "UK (LSE)", "SIX": "Switzerland (SIX)",
    "STO": "Sweden (STO)", "KSC": "Korea (KSC)",
    "SAO": "Brazil (SAO)", "Taiwan": "Taiwan (TAI+TWO)",
    "SGX": "Singapore (SGX)", "JSE": "South Africa (JNB)",
    "PAR": "France (PAR)",
}

FOOTER = ("Data: Ceta Research | Altman Z-Score, annual rebalance (April), "
          "equal weight, excl. financials/utilities, 2000-2025")


def chart_cumulative_growth(exchange_key, filename):
    """Cumulative $10k growth: Safe zone vs Distress zone vs SPY."""
    data = exchange_data[exchange_key]
    ar = data["annual_returns"]

    ar = [y for y in ar if y["spy"] is not None]
    if len(ar) < 5:
        print(f"  Skipping {filename}: only {len(ar)} valid years")
        return

    start_year = ar[0]["year"] - 1
    label = EXCHANGE_LABELS.get(exchange_key, exchange_key)

    tracks = [
        ("Safe (Z>2.99)", "safe", COLORS["safe"], "-", 2.2),
        ("Distress (Z<1.81)", "distress", COLORS["distress"], "-", 2.2),
        ("S&P 500", "spy", COLORS["spy"], "--", 1.8),
    ]

    fig, ax = plt.subplots(figsize=(12, 6))

    for track_label, key, color, ls, lw in tracks:
        vals = [10000]
        years = [start_year]
        for y in ar:
            ret = y[key]
            if ret is None:
                ret = 0
            vals.append(vals[-1] * (1 + ret / 100))
            years.append(y["year"])

        p = data["portfolios"]
        track_map = {"safe": "safe_zone", "distress": "distress_zone", "spy": "sp500"}
        cagr = p[track_map[key]]["cagr"]

        ax.plot(years, vals, color=color, linewidth=lw, linestyle=ls,
                label=f"{track_label} ({cagr:.1f}% CAGR)")

        final_k = vals[-1] / 1000
        offset_y = {"safe": 8, "distress": -16, "spy": -28}.get(key, 0)
        ax.annotate(f"${final_k:,.0f}K", xy=(years[-1], vals[-1]),
                    xytext=(8, offset_y), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=color)

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(f"Altman Z-Score: Growth of $10,000 on {label}",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=9, loc="upper left")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02, f"Data: Ceta Research | {label}, {FOOTER.split('|')[1].strip()}",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_returns(exchange_key, filename):
    """Annual returns bar chart: Safe zone vs Distress zone."""
    data = exchange_data[exchange_key]
    ar = [y for y in data["annual_returns"]
          if y["spy"] is not None and y["year"] >= 2000]

    if len(ar) < 5:
        print(f"  Skipping {filename}: only {len(ar)} years")
        return

    label = EXCHANGE_LABELS.get(exchange_key, exchange_key)
    years = [y["year"] for y in ar]
    safe_rets = [y["safe"] for y in ar]
    distress_rets = [y["distress"] for y in ar]
    spy_rets = [y["spy"] for y in ar]

    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.25
    x = list(range(len(years)))

    ax.bar([i - width for i in x], spy_rets, width,
           label="S&P 500", color=COLORS["spy"], alpha=0.7)
    ax.bar(x, safe_rets, width,
           label="Safe (Z>2.99)", color=COLORS["safe"], alpha=0.85)
    ax.bar([i + width for i in x], distress_rets, width,
           label="Distress (Z<1.81)", color=COLORS["distress"], alpha=0.85)

    # Add safe stock count annotations
    for i, y in enumerate(ar):
        count = y.get("safe_count", 0)
        if count > 0:
            y_pos = max(safe_rets[i], 0) + 2
            ax.text(i, y_pos, str(count),
                    ha="center", fontsize=7, color="#555", alpha=0.7)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"Altman Z-Score: Safe vs Distress on {label} ({years[0]}-{years[-1]})",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {label}, annual rebalance (April). "
             f"Numbers above bars = stock count in safe-zone portfolio.",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar: Safe-Distress CAGR spread by exchange, sorted by spread."""
    items = []
    for name, d in exchange_data.items():
        spread = d.get("spread_cagr", 0)
        safe_cagr = d["portfolios"]["safe_zone"]["cagr"]
        items.append((name, spread, safe_cagr))

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
    ax.set_xlabel("CAGR Spread: Safe minus Distress (%)",
                  fontsize=11, fontweight="bold")
    ax.set_title("Altman Z-Score: Safe-Distress Spread by Exchange",
                 fontsize=13, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, spread) in enumerate(zip(bars, spreads)):
        x_pos = spread + 0.5 if spread >= 0 else spread - 3
        ax.text(x_pos, i, f"{spread:+.1f}%", va="center", fontsize=10,
                fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar: Max drawdown for safe-zone portfolio by exchange."""
    items = []
    for name, d in exchange_data.items():
        dd = d["portfolios"]["safe_zone"]["max_drawdown"]
        items.append((name, dd))

    # Sort: least negative (best) at top
    items.sort(key=lambda x: x[1], reverse=True)

    names = [EXCHANGE_LABELS.get(i[0], i[0]) for i in items]
    drawdowns = [i[1] for i in items]
    colors = [EXCHANGE_COLORS.get(i[0], "#95a5a6") for i in items]

    fig, ax = plt.subplots(figsize=(10, max(6, len(items) * 0.45)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85,
                   height=0.6)

    # SPY reference line from US data
    spy_dd = exchange_data.get("US_MAJOR", {}).get("portfolios", {}).get(
        "sp500", {}).get("max_drawdown")
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")
        ax.legend(fontsize=10, loc="lower left")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=11, fontweight="bold")
    ax.set_title("Altman Z-Score Safe Zone: Max Drawdown by Exchange",
                 fontsize=13, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 2
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10,
                fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(
        description="Generate Altman Z-Score charts from result JSONs")
    parser.add_argument("--exchange", type=str,
                        help="Generate charts for a single exchange (e.g., US_MAJOR, India)")
    parser.add_argument("--all", action="store_true",
                        help="Generate comparison charts only")
    args = parser.parse_args()

    print(f"Found {len(exchange_data)} exchanges with result data\n")

    if not exchange_data:
        print("No result files found in results/. Run backtest.py first.")
        sys.exit(1)

    if args.exchange:
        # Single exchange mode
        key = args.exchange
        if key not in exchange_data:
            print(f"No data for '{key}'. Available: {', '.join(exchange_data.keys())}")
            sys.exit(1)
        label = EXCHANGE_LABELS.get(key, key)
        region = key.lower().replace("_", "")
        print(f"Generating charts for {label}...")
        chart_cumulative_growth(key, f"1_{region}_cumulative_growth.png")
        chart_annual_returns(key, f"2_{region}_annual_returns.png")
    elif args.all:
        # Comparison charts only
        print("Generating comparison charts...")
        chart_comparison_cagr("1_comparison_cagr.png")
        chart_comparison_drawdown("2_comparison_drawdown.png")
    else:
        # All per-exchange charts + comparison
        blog_exchanges = {
            "US_MAJOR": "usmajor",
            "India": "india",
            "XETRA": "xetra",
            "China": "china",
            "HKSE": "hkse",
            "Canada": "canada",
            "SAO": "sao",
            "KSC": "ksc",
            "Taiwan": "taiwan",
            "JSE": "jse",
            "SIX": "six",
            "STO": "sto",
            "SGX": "sgx",
            "LSE": "lse",
            "PAR": "par",
        }

        for key, region in blog_exchanges.items():
            if key not in exchange_data:
                print(f"  Skipping {key}: no data")
                continue
            label = EXCHANGE_LABELS.get(key, key)
            print(f"Generating charts for {label}...")
            chart_cumulative_growth(key, f"1_{region}_cumulative_growth.png")
            chart_annual_returns(key, f"2_{region}_annual_returns.png")

        print("\nGenerating comparison charts...")
        chart_comparison_cagr("1_comparison_cagr.png")
        chart_comparison_drawdown("2_comparison_drawdown.png")

    print(f"\nDone. Charts in {charts_dir}/")


if __name__ == "__main__":
    main()
