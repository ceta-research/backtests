#!/usr/bin/env python3
"""
Generate charts for Dividend Growth (Aristocrats) backtest results.

Reads results/exchange_comparison.json and generates:
- Cumulative growth charts (per exchange)
- Annual returns bar charts (per exchange)
- CAGR comparison across exchanges

Cash years are marked on the per-exchange charts. A market invested in fewer
than half its years is labelled mostly cash, too thin to test. A market with no
results or no invested year gets no chart, and its old PNG is deleted.

Usage:
    cd backtests
    python3 dividend-growth/generate_charts.py
"""

import json
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (money, money_axis_label, money_formatter, currency_prefix, localize_money_title,
                         benchmark_legend, is_usd_benchmark_proxy)
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
import numpy as np


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
CHARTS_DIR = os.path.join(SCRIPT_DIR, "charts")

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "US", "US": "US", "NSE": "India", "XETRA": "Germany",
    "STO": "Sweden", "TSX": "Canada", "SHZ_SHH": "China",
    "HKSE": "Hong Kong", "JPX": "Japan", "LSE": "UK", "ASX": "Australia",
    "KSC": "Korea", "SIX": "Switzerland", "TAI": "Taiwan",
    "JNB": "South Africa", "PAR": "France",
}

STRATEGY_NAME = "Dividend Growth"
CASH_COLOR = "#E0E0E0"  # cash years, mostly-cash markets
THIN_COLOR = "#B71C1C"  # the "too thin to test" line


def load_results():
    path = os.path.join(RESULTS_DIR, "exchange_comparison.json")
    if not os.path.exists(path):
        print(f"Results not found at {path}")
        print("Run: python3 dividend-growth/backtest.py --global "
              "--output dividend-growth/results/exchange_comparison.json")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def market_charts(label):
    """The two per-market chart file names. Live posts embed them: do not rename."""
    slug = label.lower().replace(" ", "_")
    return f"1_{slug}_cumulative_growth.png", f"2_{slug}_annual_returns.png"


def skip_charts(what, why, *names):
    """Say why charts are not drawn, and delete old copies so they cannot pass as current."""
    deleted = []
    for name in names:
        path = os.path.join(CHARTS_DIR, name)
        if os.path.exists(path):
            os.remove(path)
            deleted.append(name)
    print(f"  Skipped {what}: {why}" + (f" (deleted old {', '.join(deleted)})" if deleted else ""))


def cash_years(data):
    """Years the portfolio sat in cash (too few qualifying stocks): 0%, still compounded."""
    annual = data.get("annual_returns") or []
    held = {}
    for p in data.get("period_data") or []:
        held.setdefault(int(p["rebalance_date"][:4]), []).append(p.get("stocks_held"))
    if held:
        return [a["year"] for a in annual if not any(held.get(a["year"], [1]))]
    # results from before period_data was recorded: a cash year is an exact 0.0
    return [a["year"] for a in annual if a["portfolio"] == 0]


def invested_years(data):
    """(years invested, years charted)."""
    total = len(data.get("annual_returns") or [])
    return total - len(cash_years(data)), total


def mostly_cash(data):
    """Invested in fewer than half its years: too thin to test, must not read as a normal result."""
    invested, total = invested_years(data)
    return invested * 2 < total


def set_title(ax, title, data):
    """Chart title, with a red too-thin-to-test line under it for a mostly-cash market."""
    if not mostly_cash(data):
        ax.set_title(title, fontsize=14)
        return
    invested, total = invested_years(data)
    ax.set_title(title, fontsize=14, pad=24)
    ax.text(0.5, 1.015, f"Mostly cash: invested in {invested} of {total} years. Too thin to test.",
            transform=ax.transAxes, ha="center", va="bottom", fontsize=11, fontweight="bold",
            color=THIN_COLOR)


def plot_cumulative(data, exchange_key, label, bench_name):
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [a["year"] for a in annual]
    port_vals = [1.0]
    spy_vals = [1.0]
    for a in annual:
        port_vals.append(port_vals[-1] * (1 + a["portfolio"] / 100))
        spy_vals.append(spy_vals[-1] * (1 + a["spy"] / 100))
    # "year" is the year a holding period starts: 1 at the first start, each value at its period's end
    x = [years[0]] + [y + 1 for y in years]
    cash = cash_years(data)

    fig, ax = plt.subplots(figsize=(10, 6))
    for y in cash:
        ax.axvspan(y, y + 1, color=CASH_COLOR, linewidth=0, zorder=0)
    ax.plot(x, port_vals, "b-", linewidth=2, label=STRATEGY_NAME)
    ax.plot(x, spy_vals, "r--", linewidth=1.5, label=bench_name)
    set_title(ax, f"{STRATEGY_NAME}: Cumulative Growth ({label})", data)
    ax.set_xlabel("Year")
    ax.set_ylabel("Growth of " + currency_prefix(exchange_key) + "1")
    if cash:
        handles, _ = ax.get_legend_handles_labels()
        handles.append(Patch(color=CASH_COLOR, label="In cash, too few qualifying stocks "
                                                     f"({len(cash)} of {len(years)} years)"))
        # under the axes: the shading pushes an in-axes legend onto the lines
        ax.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3,
                  frameon=False)
    else:
        ax.legend()
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(money_formatter(exchange_key, decimals=1))

    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, market_charts(label)[0])
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def plot_annual(data, exchange_key, label, bench_name):
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [str(a["year"]) for a in annual]
    port_ret = [a["portfolio"] for a in annual]
    spy_ret = [a["spy"] for a in annual]

    x = np.arange(len(years))
    width = 0.35
    cash = cash_years(data)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width/2, port_ret, width, label=STRATEGY_NAME, color="#2196F3")
    ax.bar(x + width/2, spy_ret, width, label=bench_name, color="#FF5722", alpha=0.7)
    # A cash year has no bar: label it so it does not read as a 0% invested year.
    for i, a in enumerate(annual):
        if a["year"] in cash:
            ax.text(i - width/2, 0, "cash", rotation=90, ha="center", va="bottom", fontsize=7,
                    color="#2196F3")
    set_title(ax, f"{STRATEGY_NAME}: Annual Returns ({label})", data)
    ax.set_xlabel("Year")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    ax.axhline(y=0, color="black", linewidth=0.5)
    if cash:
        fig.text(0.5, -0.02, "\"cash\": too few qualifying stocks at that rebalance. The portfolio held "
                             f"cash (0%) in {len(cash)} of {len(years)} years.",
                 ha="center", va="top", fontsize=8, color="gray")

    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, market_charts(label)[1])
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def plot_cagr_comparison(legs, left_out, all_results):
    exchanges = []
    cagrs = []
    colors = []
    notes = []
    proxies = []
    left_out = list(left_out)

    for key, data, label in legs:
        cagr = data["portfolio"].get("cagr")
        if cagr is None or data.get("window_truncated"):
            # a shorter measured window does not rank against the full-window CAGRs
            why = "no CAGR" if cagr is None else f"measured {data.get('window_label')} only"
            print(f"  Left {label} out of the comparison: {why}")
            left_out.append(f"{label} ({why})")
            continue
        excess = (data.get("comparison") or {}).get("excess_cagr", 0) or 0
        if mostly_cash(data):
            # drawn apart from the ranking, hatched grey: too thin to test
            exchanges.append(f"{label} (mostly cash)")
            colors.append(CASH_COLOR)
            notes.append("  invested %d of %d years" % invested_years(data))
        else:
            exchanges.append(label)
            colors.append("#4CAF50" if excess > 0 else "#F44336")
            notes.append("")
        cagrs.append(cagr)
        if is_usd_benchmark_proxy(all_results, key):
            proxies.append(label)

    if not exchanges:
        skip_charts("comparison", "no market to compare", "3_comparison_cagr.png")
        return

    # Sort by CAGR descending, mostly-cash markets below the ranked ones
    sorted_data = sorted(zip(exchanges, cagrs, colors, notes), key=lambda x: (not x[3], x[1]))
    exchanges, cagrs, colors, notes = zip(*sorted_data)

    fig, ax = plt.subplots(figsize=(10, max(6, len(exchanges) * 0.4)))
    y_pos = np.arange(len(exchanges))
    bars = ax.barh(y_pos, cagrs, color=colors, height=0.6)
    for bar, note in zip(bars, notes):
        if note:
            bar.set(hatch="///", edgecolor="gray")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("CAGR (%)")
    ax.set_title(f"{STRATEGY_NAME}: CAGR by Exchange", fontsize=14)
    ax.grid(True, alpha=0.3, axis="x")
    handles = [Patch(color="#4CAF50", label="Beat its benchmark"),
               Patch(color="#F44336", label="Trailed its benchmark")]
    if any(notes):
        handles.append(Patch(facecolor=CASH_COLOR, edgecolor="gray", hatch="///",
                             label="Mostly cash: too thin to test"))
    # below the axes: inside, it could sit on a bar's label
    ax.legend(handles=handles, fontsize=9, loc="upper center", bbox_to_anchor=(0.5, -0.11),
              ncol=len(handles), frameon=False)

    for i, v in enumerate(cagrs):
        ax.text(max(v, 0) + 0.2, i, f"{v:.1f}%{notes[i]}", va="center", fontsize=9)

    footer = "Returns in local currency, each market against its own index"
    footer += f" ({', '.join(proxies)}: S&P 500 in USD)." if proxies else "."
    if any(notes):
        footer += " Mostly cash: invested in fewer than half the years."
    if left_out:
        footer += f"\nLeft out: {', '.join(left_out)}."
    fig.text(0.5, -0.04, footer, ha="center", va="top", fontsize=8, color="gray")

    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, "3_comparison_cagr.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def main():
    print(f"Generating {STRATEGY_NAME} charts...")
    all_results = load_results()
    os.makedirs(CHARTS_DIR, exist_ok=True)

    legs = []     # (key, data, label) to draw
    skipped = {}  # label -> why its charts are not drawn
    never = []    # labels in the results that never invested
    for key, data in all_results.items():
        label = EXCHANGE_LABELS.get(key)
        if label is None:
            print(f"  Skipping {key}: no label in EXCHANGE_LABELS")
        elif label in skipped or label in [leg[2] for leg in legs]:
            print(f"Two keys in exchange_comparison.json chart as {label} (second: {key}). Keep one.")
            sys.exit(1)
        elif "error" in data or not data.get("portfolio") or not data.get("annual_returns"):
            skipped[label] = "no results"
        elif invested_years(data)[0] == 0:
            never.append(f"{label} (never invested)")
            skipped[label] = f"never invested, cash in all {len(data['annual_returns'])} years"
        else:
            legs.append((key, data, label))
    for label in EXCHANGE_LABELS.values():
        if label not in skipped and label not in [leg[2] for leg in legs]:
            skipped[label] = "not in exchange_comparison.json"
    for label, why in skipped.items():
        skip_charts(label, why, *market_charts(label))

    for key, data, label in legs:
        thin = " mostly cash, labelled too thin to test" if mostly_cash(data) else ""
        print(f"\n  {label} ({key}):{thin}")
        # the "spy" fields hold the market's own index (S&P 500 in USD where it has none)
        bench_name = benchmark_legend(all_results, key)
        plot_cumulative(data, key, label, bench_name)
        plot_annual(data, key, label, bench_name)

    print("\n  Comparison:")
    plot_cagr_comparison(legs, never, all_results)
    print(f"\nAll charts saved to {CHARTS_DIR}/")


if __name__ == "__main__":
    main()
