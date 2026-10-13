#!/usr/bin/env python3
"""
Revenue Acceleration Growth - Chart Generator

Generates standard charts from backtest results:
  - Cumulative growth (strategy vs its own benchmark) per exchange
  - Annual returns bar chart per exchange
  - CAGR comparison across exchanges (vs SPY, USD)
  - Max drawdown comparison

Reads results/exchange_comparison.json only. Run after backtest.py --global, or
after per-market runs plus scripts/rebuild_comparison.py revenue-accel:
    cd backtests
    python3 revenue-accel/generate_charts.py

Charts saved to revenue-accel/charts/. Move to ts-content-creator after generation.
A chart of this script's that a run does not redraw is deleted from charts/.
"""

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from chart_utils import benchmark_legend

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import numpy as np
except ImportError:
    print("matplotlib not installed. Run: pip install matplotlib")
    sys.exit(1)

# ---- Config ----
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "results", "exchange_comparison.json")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "charts")
STRATEGY_NAME = "Revenue Acceleration"

COLORS = {
    "strategy": "#1a73e8",   # Blue
    "spy": "#e8711a",        # Orange
    "positive": "#2e7d32",   # Green
    "negative": "#c62828",   # Red
    "cash": "#9e9e9e",       # Grey band over a cash period
}
SAVED = []  # charts written this run; remove_stale_charts() deletes this script's others

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "United States",
    "NSE": "India",
    "JPX": "Japan",
    "LSE": "United Kingdom",
    "SHZ_SHH": "China",
    "HKSE": "Hong Kong",
    "KSC": "South Korea",
    "TAI_TWO": "Taiwan",
    "XETRA": "Germany",
    "TSX": "Canada",
    "SET": "Thailand",
    "STO": "Sweden",
    "SIX": "Switzerland",
    "SES": "Singapore",
    "JNB": "South Africa",
}

REGION_SLUGS = {
    "NYSE_NASDAQ_AMEX": "us",
    "NSE": "india",
    "JPX": "japan",
    "LSE": "uk",
    "SHZ_SHH": "china",
    "HKSE": "hongkong",
    "KSC": "korea",
    "TAI_TWO": "taiwan",
    "XETRA": "germany",
    "TSX": "canada",
    "SET": "thailand",
    "STO": "sweden",
    "SIX": "switzerland",
    "SES": "singapore",
    "JNB": "southafrica",
}


def load_results():
    if not os.path.exists(RESULTS_FILE):
        print(f"Results file not found: {RESULTS_FILE}")
        print("Run backtest.py --global first.")
        sys.exit(1)
    with open(RESULTS_FILE) as f:
        return json.load(f)


def bench_name(exchange_key, data):
    """Benchmark display name: the local index, "S&P 500" for the US, and
    "S&P 500, USD" where a non-US market has no local index and ran against SPY."""
    return benchmark_legend({exchange_key: data}, exchange_key)


def cash_counts(data):
    """(cash periods, rebalances run, unit). Files that predate total_rebalances fall back to n_periods."""
    total = data.get("total_rebalances") or data.get("n_periods") or len(data.get("annual_returns") or [])
    unit = "yrs" if data.get("frequency", "annual") == "annual" else "periods"
    return data.get("cash_periods") or 0, total, unit


def too_thin(data):
    """Invested in fewer than half its periods (never invested included): too thin to test."""
    cash, total, _ = cash_counts(data)
    return (total - cash) * 2 < total


def thin_note(data):
    """Red line under a per-market title, "" for a market invested at least half the time."""
    cash, total, unit = cash_counts(data)
    return f"Too thin to test: in cash {cash} of {total} {unit}" if too_thin(data) else ""


def cash_years(data):
    """Start years in which every holding period sat in cash (too few stocks buyable)."""
    held = {}
    for p in data.get("period_data") or []:
        held.setdefault(int(p["rebalance_date"][:4]), []).append(p.get("stocks_held"))
    if held:
        return {y for y, h in held.items() if not any(h)}
    # Files without period_data: one rebalance a year, and a cash year is the one that prints exactly 0.
    return {ar["year"] for ar in data.get("annual_returns") or [] if ar["portfolio"] == 0}


def mark_cash(ax, spans):
    """Grey band over each cash period, so a flat 0% stretch does not read as an invested one."""
    for i, (lo, hi) in enumerate(spans):
        ax.axvspan(lo, hi, color=COLORS["cash"], alpha=0.25, linewidth=0,
                   label="In cash (too few stocks qualified)" if i == 0 else None)


def plot_cumulative_growth(exchange_key, data, label, filename):
    """Plot cumulative growth of strategy vs its benchmark."""
    annual = data.get("annual_returns", [])
    if not annual:
        print(f"  Skipping {filename}: no annual returns")
        return
    bench = bench_name(exchange_key, data)
    cash, total, unit = cash_counts(data)
    in_cash = cash_years(data)
    thin = thin_note(data)

    years = [ar["year"] for ar in annual]
    port_cum = [1.0]
    spy_cum = [1.0]
    for ar in annual:
        port_cum.append(port_cum[-1] * (1 + ar["portfolio"] / 100))
        spy_cum.append(spy_cum[-1] * (1 + ar["spy"] / 100))

    # "year" is the year a holding period STARTS: 1.0 sits at the first start, each value at its period's end
    x = [years[0]] + [y + 1 for y in years]

    fig, ax = plt.subplots(figsize=(12, 6))
    mark_cash(ax, [(y, y + 1) for y in years if y in in_cash])
    ax.plot(x, port_cum, color=COLORS["strategy"], linewidth=2.5,
            label=f"{STRATEGY_NAME} ({data['portfolio']['cagr']}% CAGR"
                  + (f", cash {cash} of {total} {unit})" if cash else ")"))
    ax.plot(x, spy_cum, color=COLORS["spy"], linewidth=2, linestyle="--",
            label=f"{bench} ({data['spy']['cagr']}% CAGR)")

    ax.set_title(f"{STRATEGY_NAME} vs {bench}: {label}",
                 fontsize=14, fontweight="bold", pad=32 if thin else 15)
    ax.text(0.5, 1.015, thin, transform=ax.transAxes, ha="center", va="bottom",
            fontsize=12, fontweight="bold", color=COLORS["negative"])
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Portfolio Value (1 unit invested)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}x"))
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    if not thin:  # a mostly-cash book's ratios are not a result
        p = data.get("portfolio", {})
        c = data.get("comparison", {})
        excess = c.get("excess_cagr")
        excess_str = "N/A" if excess is None else f"+{excess:.2f}%" if excess > 0 else f"{excess:.2f}%"
        note = (f"Excess CAGR: {excess_str}  |  "
                f"Sharpe: {p.get('sharpe_ratio', 'N/A')}  |  "
                f"Max DD: {p.get('max_drawdown', 'N/A')}%")
        ax.text(0.02, 0.05, note, transform=ax.transAxes, fontsize=9,
                color="#555555", va="bottom")

    ax.set_xticks([v for i, v in enumerate(x) if v % 5 == 0 or i == 0])

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    SAVED.append(filename)
    print(f"  Saved: {filename}")


def plot_annual_returns(exchange_key, data, label, filename):
    """Plot annual returns bar chart."""
    annual = data.get("annual_returns", [])
    if not annual:
        print(f"  Skipping {filename}: no annual returns")
        return

    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]
    bench = bench_name(exchange_key, data)
    in_cash = cash_years(data)
    thin = thin_note(data)

    x = range(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 6))
    mark_cash(ax, [(i - 0.5, i + 0.5) for i, y in enumerate(years) if y in in_cash])
    ax.bar([i - width/2 for i in x], port_rets, width,
           color=[COLORS["positive"] if r >= 0 else COLORS["negative"] for r in port_rets],
           alpha=0.85, label=STRATEGY_NAME)
    ax.bar([i + width/2 for i in x], spy_rets, width,
           color=COLORS["spy"], alpha=0.5, label=bench)

    ax.axhline(y=0, color="black", linewidth=0.8, alpha=0.5)
    ax.set_title(f"Annual Returns: {STRATEGY_NAME} vs {bench} ({label})",
                 fontsize=13, fontweight="bold", pad=30 if thin else 12)
    ax.text(0.5, 1.015, thin, transform=ax.transAxes, ha="center", va="bottom",
            fontsize=12, fontweight="bold", color=COLORS["negative"])
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    SAVED.append(filename)
    print(f"  Saved: {filename}")


def spy_series(data):
    """SPY (USD, total return) series, or None. Cross-market charts must use this, not the
    primary block, which holds a different local index for each exchange. A node with no
    spy_usd block was itself measured against SPY (the US, or a market with no local index)."""
    if data.get("spy_usd"):
        return data["spy_usd"]
    return data.get("spy") if (data.get("benchmark_name") or "S&P 500") == "S&P 500" else None


def comparison_legs(all_data):
    """({key: data} fit to rank against SPY, footer naming every market left out and why).

    Left out: a market invested in fewer than half its periods (too thin to test), one with
    no result, one whose measured window was cut short (its CAGR would not span SPY's
    years), and one with no SPY series.
    """
    ranked, thin, dropped = {}, [], []
    for key, data in all_data.items():
        label = EXCHANGE_LABELS.get(key, key)
        cash, total, unit = cash_counts(data)
        if "error" in data or not data.get("portfolio"):
            dropped.append(f"{label} (no result)")
        elif too_thin(data):
            thin.append(f"{label} ({cash} of {total} {unit})")
        elif data.get("window_truncated"):
            dropped.append(f"{label} (measured window cut short)")
        elif not spy_series(data):
            dropped.append(f"{label} (no SPY series)")
        else:
            ranked[key] = data

    note = "Strategy returns in local currency, not translated. S&P 500 is SPY in USD, total return."
    if any(cash_counts(d)[0] for d in ranked.values()):
        note += ("\n\"cash\" after a market's name: periods with too few qualifying stocks to hold."
                 " A cash period earns 0% and counts in the CAGR and drawdown.")
    if thin:
        note += "\nToo thin to test, not ranked (in cash more than half the time): " + ", ".join(thin) + "."
    if dropped:
        note += "\nNot shown: " + ", ".join(dropped) + "."
    return ranked, note


def leg_label(key, data):
    """Market name for a comparison axis, with its cash count when it sat in cash at all."""
    cash, _, unit = cash_counts(data)
    label = EXCHANGE_LABELS.get(key, key)
    return f"{label}, {cash} {unit} cash" if cash else label


def plot_comparison_cagr(all_data, filename="1_comparison_cagr.png"):
    """CAGR comparison bar chart across exchanges, benchmarked to SPY."""
    labels = []
    port_cagrs = []
    spy_cagrs = []

    ranked, note = comparison_legs(all_data)
    for key, data in ranked.items():
        labels.append(leg_label(key, data))
        port_cagrs.append(data["portfolio"]["cagr"])
        spy_cagrs.append(spy_series(data)["cagr"])

    if not labels:
        print(f"  Skipping {filename}: no market fit to rank")
        return

    sorted_pairs = sorted(zip(labels, port_cagrs, spy_cagrs), key=lambda x: x[1], reverse=True)
    labels, port_cagrs, spy_cagrs = zip(*sorted_pairs)

    x = range(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar([i - width/2 for i in x], port_cagrs, width,
           color=[COLORS["positive"] if c > s else COLORS["negative"]
                  for c, s in zip(port_cagrs, spy_cagrs)],
           alpha=0.85, label=STRATEGY_NAME)
    ax.bar([i + width/2 for i in x], spy_cagrs, width,
           color=COLORS["spy"], alpha=0.5, label="S&P 500 (SPY, USD)")

    ax.set_title(f"{STRATEGY_NAME} CAGR vs S&P 500 by exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("CAGR (%)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.margins(y=0.15)  # headroom: the legend covered the SPY bars when no market beat SPY
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)
    fig.text(0.5, -0.01, note, ha="center", va="top", fontsize=8, color="#555555")

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    SAVED.append(filename)
    print(f"  Saved: {filename}")


def plot_comparison_drawdown(all_data, filename="2_comparison_drawdown.png"):
    """Max drawdown comparison across exchanges."""
    labels = []
    port_mdd = []
    spy_mdd = []

    ranked, note = comparison_legs(all_data)
    for key, data in ranked.items():
        labels.append(leg_label(key, data))
        port_mdd.append(abs(data["portfolio"]["max_drawdown"]))
        spy_mdd.append(abs(spy_series(data)["max_drawdown"]))

    if not labels:
        print(f"  Skipping {filename}: no market fit to rank")
        return

    sorted_pairs = sorted(zip(labels, port_mdd, spy_mdd), key=lambda x: x[1])
    labels, port_mdd, spy_mdd = zip(*sorted_pairs)

    x = range(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar([i - width/2 for i in x], port_mdd, width, color=COLORS["negative"],
           alpha=0.8, label=STRATEGY_NAME)
    ax.bar([i + width/2 for i in x], spy_mdd, width, color=COLORS["spy"],
           alpha=0.5, label="S&P 500 (SPY, USD)")

    ax.set_title(f"Max Drawdown: {STRATEGY_NAME} vs S&P 500 (2000-2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Max Drawdown (%, lower = better)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)
    fig.text(0.5, -0.01, note, ha="center", va="top", fontsize=8, color="#555555")

    plt.tight_layout()
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    SAVED.append(filename)
    print(f"  Saved: {filename}")


OWN_CHARTS = re.compile(r"(1_.+_cumulative_growth|2_.+_annual_returns"
                        r"|1_comparison_cagr|2_comparison_drawdown)\.png")


def remove_stale_charts():
    """Delete this script's charts that the run did not redraw, so none is left looking current."""
    for name in sorted(os.listdir(CHARTS_DIR)) if os.path.isdir(CHARTS_DIR) else []:
        if OWN_CHARTS.fullmatch(name) and name not in SAVED:
            os.remove(os.path.join(CHARTS_DIR, name))
            print(f"  Removed stale chart, not drawn this run: {name}")


def main():
    print("Loading results...")
    all_data = load_results()
    print(f"  {len(all_data)} exchanges found\n")

    for exchange_key in REGION_SLUGS:
        if exchange_key not in all_data:
            print(f"  No results for {exchange_key} ({EXCHANGE_LABELS[exchange_key]}): no charts")

    for exchange_key, data in all_data.items():
        if "error" in data or not data.get("portfolio"):
            print(f"  Skipping {exchange_key} (no data)")
            continue

        slug = REGION_SLUGS.get(exchange_key, exchange_key.lower())
        label = EXCHANGE_LABELS.get(exchange_key, exchange_key)

        print(f"  Generating charts for {label}... {thin_note(data)}".rstrip())
        plot_cumulative_growth(exchange_key, data, label,
                               f"1_{slug}_cumulative_growth.png")
        plot_annual_returns(exchange_key, data, label,
                            f"2_{slug}_annual_returns.png")

    print("\n  Generating comparison charts...")
    plot_comparison_cagr(all_data)
    plot_comparison_drawdown(all_data)
    print("  " + comparison_legs(all_data)[1].replace("\n", "\n  "))
    remove_stale_charts()

    print(f"\nDone. Charts saved to: {CHARTS_DIR}")
    print("\nNext step: move charts to ts-content-creator blog directories:")
    print("  mv charts/1_us_cumulative_growth.png "
          "../ts-content-creator/content/_current/growth-01-revenue-accel/blogs/us/")
    print("  # ... repeat for each exchange")


if __name__ == "__main__":
    main()
