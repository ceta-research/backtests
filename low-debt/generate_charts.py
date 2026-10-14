#!/usr/bin/env python3
"""
Low Debt Quality - Chart Generator

Generates standard charts from backtest results:
  - Cumulative growth (strategy vs that exchange's benchmark) per exchange
  - Annual returns bar chart per exchange
  - CAGR comparison across exchanges
  - Max drawdown comparison

Run after backtest.py --global:
    cd backtests
    python3 low-debt/generate_charts.py

Charts saved to low-debt/charts/. Move to ts-content-creator after generation.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from chart_utils import (benchmark_legend, currency_prefix, is_usd_benchmark_proxy,
                         money_formatter)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    from matplotlib.patches import Patch
    import numpy as np
except ImportError:
    print("matplotlib not installed. Run: pip install matplotlib")
    sys.exit(1)

# ---- Config ----
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "results", "exchange_comparison.json")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "charts")

COLORS = {
    "strategy": "#1a73e8",   # Blue
    "spy": "#e8711a",        # Orange
    "positive": "#2e7d32",   # Green
    "negative": "#c62828",   # Red
    "cash": "#9e9e9e",       # Grey
}
MIN_STOCKS = 10  # backtest.py's cash rule: fewer names than this and the period is held in cash
TIE_PP = 0.2     # excess CAGR under this many points either way is a tie

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


def load_results():
    if not os.path.exists(RESULTS_FILE):
        print(f"Results file not found: {RESULTS_FILE}")
        print("Run backtest.py --global first.")
        sys.exit(1)
    with open(RESULTS_FILE) as f:
        return json.load(f)


def cash_count(data):
    """(cash periods, all periods). Older results carry no total_rebalances."""
    total = (data.get("total_rebalances") or data.get("n_periods")
             or len(data.get("annual_returns") or []))
    return data.get("cash_periods") or 0, total


def cash_years(data):
    """Start years of the periods held in cash."""
    periods = data.get("period_data")
    if periods:
        return {int(r["rebalance_date"][:4]) for r in periods if r.get("stocks_held") == 0}
    # Older results have no period_data: a cash year is recorded as exactly 0.0%.
    if not cash_count(data)[0]:
        return set()
    return {ar["year"] for ar in data.get("annual_returns") or [] if ar["portfolio"] == 0}


def too_thin(data):
    """Invested in fewer than half its periods (or never): too thin to test, not a result."""
    cash, total = cash_count(data)
    return total - cash <= 0 or (total - cash) * 2 < total


def thin_title(data):
    """Extra title line for a market that is too thin to test, else ''."""
    cash, total = cash_count(data)
    return f"\nToo thin to test: in cash {cash} of {total} years" if too_thin(data) else ""


def drop_stale(filename):
    """Delete a chart this run did not draw, so an old one cannot pass for current."""
    path = os.path.join(CHARTS_DIR, filename)
    if os.path.exists(path):
        os.remove(path)
        print(f"  Removed stale chart: {filename}")


def get_cumulative_returns(annual_returns, key="portfolio"):
    """Compute cumulative growth from annual returns list."""
    values = [1.0]
    benchmark_key = "spy"
    for ar in annual_returns:
        port_r = ar[key if key != "spy" else "spy"] / 100
        values.append(values[-1] * (1 + port_r))
    return values


def plot_cumulative_growth(exchange_key, data, label, bench, filename):
    """Plot cumulative growth of strategy vs that exchange's own benchmark.

    `bench` is the benchmark display name, resolved in main() where the full
    keyed results dict is in scope. Do not resolve it here: `data` is the
    per-exchange entry, so a lookup would silently fall back to "S&P 500".
    """
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [ar["year"] for ar in annual]
    port_cum = [1.0]
    spy_cum = [1.0]
    for ar in annual:
        port_cum.append(port_cum[-1] * (1 + ar["portfolio"] / 100))
        spy_cum.append(spy_cum[-1] * (1 + ar["spy"] / 100))

    # "year" is the July a period STARTS: 1 invested at the first start, each value at its period's end
    x = [years[0]] + [y + 1 for y in years]
    cash, total = cash_count(data)
    cash_note = f", cash {cash} of {total} yrs" if cash else ""

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x, port_cum, color=COLORS["strategy"], linewidth=2.5,
            label=f"Low Debt Quality ({data['portfolio']['cagr']}% CAGR{cash_note})")
    ax.plot(x, spy_cum, color=COLORS["spy"], linewidth=2, linestyle="--",
            label=f"{bench} ({data['spy']['cagr']}% CAGR)")
    # Shade the cash years so a flat stretch does not read as a 0% invested year.
    for i, y in enumerate(sorted(cash_years(data) & set(years))):
        ax.axvspan(y, y + 1, color=COLORS["cash"], alpha=0.25, linewidth=0,
                   label=f"In cash (fewer than {MIN_STOCKS} stocks to buy)" if i == 0 else None)

    ax.set_title(f"Low Debt Quality vs {bench} — {label}{thin_title(data)}",
                 fontsize=14, fontweight="bold", pad=15)
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Portfolio Value (" + currency_prefix(exchange_key) + "1 invested)",
                  fontsize=11)
    ax.yaxis.set_major_formatter(money_formatter(exchange_key, decimals=1))
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    # Annotations
    p = data.get("portfolio", {})
    c = data.get("comparison", {})
    excess = c.get("excess_cagr", 0)
    excess_str = f"+{excess:.2f}%" if excess and excess > 0 else f"{excess:.2f}%"
    note = (f"Excess CAGR: {excess_str}  |  "
            f"Sharpe: {p.get('sharpe_ratio', 'N/A')}  |  "
            f"Max DD: {p.get('max_drawdown', 'N/A')}%")
    if too_thin(data):
        note = ""  # mostly the cash line: not figures to quote

    # Show fewer x ticks for readability
    ax.set_xticks([x[0]] + [yr for yr in range(x[0] + 1, x[-1] + 1) if yr % 5 == 0])

    plt.tight_layout()
    # Below the axes: inside them the lines can run through it and hide a minus sign.
    if note:
        fig.text(0.5, -0.01, note, ha="center", va="top", fontsize=9, color="#555555")
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def plot_annual_returns(exchange_key, data, label, bench, filename):
    """Plot annual returns bar chart. `bench` is resolved in main() (see above)."""
    annual = data.get("annual_returns", [])
    if not annual:
        return

    years = [ar["year"] for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = range(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 6))
    bars_port = ax.bar([i - width/2 for i in x], port_rets, width,
                       color=[COLORS["positive"] if r >= 0 else COLORS["negative"] for r in port_rets],
                       alpha=0.85, label="Low Debt Quality")
    bars_spy = ax.bar([i + width/2 for i in x], spy_rets, width,
                      color=COLORS["spy"], alpha=0.5, label=bench)

    # A cash year has no bar: label it so it does not read as a 0% invested year.
    cash_x = [i for i, y in zip(x, years) if y in cash_years(data)]
    for i in cash_x:
        ax.text(i - width/2, 0, "cash", rotation=90, ha="center", va="bottom",
                fontsize=7, color="#555555")

    ax.axhline(y=0, color="black", linewidth=0.8, alpha=0.5)
    ax.set_title(f"Annual Returns — Low Debt Quality vs {bench} ({label}){thin_title(data)}",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    if cash_x:
        fig.text(0.5, -0.01, f"\"cash\" marks a year with fewer than {MIN_STOCKS} stocks to buy: the "
                 f"portfolio held cash (0%) in {len(cash_x)} of {len(years)} years.",
                 ha="center", va="top", fontsize=8, color="#555555")
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def comparison_legs(all_data, metric):
    """(key, portfolio metric, benchmark metric) per market fit to rank, plus footer notes.

    Not ranked: a market that is too thin to test (its CAGR and drawdown are mostly the
    cash line) and one whose benchmark could not price the full window.
    """
    legs, thin, cut = [], [], []
    for key, data in all_data.items():
        port = (data.get("portfolio") or {}).get(metric)
        spy = (data.get("spy") or {}).get(metric)
        if "error" in data or port is None or spy is None:
            continue
        label = EXCHANGE_LABELS.get(key, key)
        cash, total = cash_count(data)
        if too_thin(data):
            thin.append(f"{label} (cash {cash} of {total} yrs)")
        elif data.get("window_truncated"):
            cut.append(f"{label}, measured {data.get('window_label')}")
        else:
            legs.append((key, port, spy))

    notes = []
    if any(cash_count(all_data[key])[0] for key, _, _ in legs):
        notes.append(f"\"yrs cash\": years with fewer than {MIN_STOCKS} stocks to buy. "
                     "A cash year earns 0% and counts in the CAGR and drawdown.")
    if thin:
        notes.append("Too thin to test, not ranked (in cash more than half the time): "
                     + ", ".join(thin) + ".")
    if cut:
        notes.append("Not ranked, benchmark covers only part of the window: " + "; ".join(cut) + ".")
    return legs, "\n".join(notes)


def leg_label(all_data, key):
    """Tick label of a ranked market: names its cash years and a USD benchmark."""
    label = EXCHANGE_LABELS.get(key, key)
    cash = cash_count(all_data[key])[0]
    if cash:
        label += f", {cash} yr{'' if cash == 1 else 's'} cash"
    if is_usd_benchmark_proxy(all_data, key):
        label += " (vs S&P 500, USD)"
    return label


def window_of(all_data, legs):
    """Measured window of the ranked markets for a title, e.g. ' (2000-2025)'."""
    spans = set()
    for key, _, _ in legs:
        annual = all_data[key].get("annual_returns") or []
        # Older results carry no window_label: first start year to last end year.
        span = all_data[key].get("window_label") or (
            f"{annual[0]['year']}-{annual[-1]['year'] + 1}" if annual else None)
        if span:
            spans.add(span)
    return f" ({' / '.join(sorted(spans))})" if spans else ""


def plot_comparison_cagr(all_data, filename="1_comparison_cagr.png"):
    """CAGR comparison bar chart across the exchanges fit to rank."""
    legs, notes = comparison_legs(all_data, "cagr")
    if not legs:
        print(f"  Skipping {filename} (no market to rank)")
        drop_stale(filename)
        return

    # Sort by portfolio CAGR
    legs.sort(key=lambda leg: leg[1], reverse=True)
    labels = [leg_label(all_data, key) for key, _, _ in legs]
    port_cagrs = [port for _, port, _ in legs]
    spy_cagrs = [spy for _, _, spy in legs]

    x = range(len(labels))
    width = 0.38

    # Colour by the file's excess: under TIE_PP either way is a tie, not a beat or a trail.
    def bar_colour(key, port, spy):
        excess = (all_data[key].get("comparison") or {}).get("excess_cagr")
        excess = port - spy if excess is None else excess
        if excess >= TIE_PP:
            return COLORS["positive"]
        return COLORS["negative"] if excess <= -TIE_PP else COLORS["cash"]

    colours = [bar_colour(*leg) for leg in legs]
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar([i - width/2 for i in x], port_cagrs, width, color=colours, alpha=0.85)
    ax.bar([i + width/2 for i in x], spy_cagrs, width, color=COLORS["spy"], alpha=0.5)
    # Explicit handles: an auto legend takes the first bar's colour, whatever it is.
    used = set(colours)
    handles = [Patch(color=colour, alpha=0.85, label=f"Low Debt Quality, {text}")
               for colour, text in [
                   (COLORS["positive"], f"ahead by {TIE_PP} points or more"),
                   (COLORS["cash"], f"within {TIE_PP} points (a tie)"),
                   (COLORS["negative"], f"behind by {TIE_PP} points or more")]
               if colour in used]
    handles.append(Patch(color=COLORS["spy"], alpha=0.5, label="Local Benchmark"))

    ax.set_title(f"Low Debt Quality CAGR by Exchange{window_of(all_data, legs)}",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("CAGR (%)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.legend(handles=handles, fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    if notes:
        fig.text(0.5, -0.01, notes, ha="center", va="top", fontsize=8, color="#555555")
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def plot_comparison_drawdown(all_data, filename="2_comparison_drawdown.png"):
    """Max drawdown comparison across the exchanges fit to rank."""
    legs, notes = comparison_legs(all_data, "max_drawdown")
    if not legs:
        print(f"  Skipping {filename} (no market to rank)")
        drop_stale(filename)
        return

    legs.sort(key=lambda leg: abs(leg[1]))
    labels = [leg_label(all_data, key) for key, _, _ in legs]
    port_mdd = [abs(port) for _, port, _ in legs]
    spy_mdd = [abs(spy) for _, _, spy in legs]

    x = range(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar([i - width/2 for i in x], port_mdd, width, color=COLORS["negative"],
           alpha=0.8, label="Low Debt Quality")
    ax.bar([i + width/2 for i in x], spy_mdd, width, color=COLORS["spy"],
           alpha=0.5, label="Local Benchmark")

    ax.set_title(f"Max Drawdown — Low Debt Quality vs Local Benchmark{window_of(all_data, legs)}",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Max Drawdown (%, lower = better)", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    if notes:
        fig.text(0.5, -0.01, notes, ha="center", va="top", fontsize=8, color="#555555")
    os.makedirs(CHARTS_DIR, exist_ok=True)
    path = os.path.join(CHARTS_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def main():
    print("Loading results...")
    all_data = load_results()
    print(f"  {len(all_data)} exchanges found\n")

    # Per-exchange charts
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

    # A key outside the map is a stale or renamed results file, not a 16th market.
    for key in [k for k in all_data if k not in REGION_SLUGS]:
        print(f"  Skipping {key} (no region mapping)")
        del all_data[key]

    for exchange_key, slug in REGION_SLUGS.items():
        data = all_data.get(exchange_key) or {}
        if "error" in data or not data.get("portfolio") or not data.get("annual_returns"):
            print(f"  Skipping {exchange_key} (no data)")
            drop_stale(f"1_{slug}_cumulative_growth.png")
            drop_stale(f"2_{slug}_annual_returns.png")
            continue

        label = EXCHANGE_LABELS.get(exchange_key, exchange_key)
        # Resolved here, where the full keyed results dict is in scope.
        # "S&P 500, USD" where the market has no local index (JNB).
        bench = benchmark_legend(all_data, exchange_key)

        print(f"  Generating charts for {label} (benchmark: {bench})...")
        plot_cumulative_growth(exchange_key, data, label, bench,
                               f"1_{slug}_cumulative_growth.png")
        plot_annual_returns(exchange_key, data, label, bench,
                            f"2_{slug}_annual_returns.png")

    # Comparison charts
    print("\n  Generating comparison charts...")
    plot_comparison_cagr(all_data)
    plot_comparison_drawdown(all_data)

    print(f"\nDone. Charts saved to: {CHARTS_DIR}")
    print("\nNext step: move charts to ts-content-creator blog directories:")
    print("  mv charts/1_us_cumulative_growth.png ../ts-content-creator/content/_current/risk-01-low-debt/blogs/us/")
    print("  # ... repeat for each exchange")


if __name__ == "__main__":
    main()
