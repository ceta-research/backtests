#!/usr/bin/env python3
"""
Generate charts for ETF Concentration backtest results.

Reads JSON results from results/ directory and generates:
- Cumulative growth charts (strategy vs SPY) per exchange
- Annual returns bar charts per exchange
- CAGR comparison chart across all exchanges
- Max drawdown comparison chart

Usage:
    python3 etf-concentration/generate_charts.py
"""

import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError:
    print("matplotlib required: pip install matplotlib")
    sys.exit(1)

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (localize_money_title, money_axis_label, money_formatter,
                         benchmark_label, benchmark_cagr, benchmark_legend,
                         benchmark_money, currency_code, money,
                         is_usd_benchmark_proxy)
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")

STRATEGY_NAME = "ETF Anti-Concentration"
STRATEGY_COLOR = "#7B1FA2"  # Purple (distinct from crowding's blue)
BENCH_COLOR = "#FF9800"
EXCESS_POS_COLOR = "#4CAF50"
EXCESS_NEG_COLOR = "#F44336"

# Benchmark labels per exchange (matches LOCAL_INDEX_NAMES in data_utils.py)
BENCHMARK_LABELS = {
    "us": "S&P 500", "india": "Sensex", "germany": "DAX",
    "china": "SSE Composite", "norway": "Oslo All Share",
    "southafrica": "S&P 500", "hongkong": "Hang Seng",
    "japan": "Nikkei 225", "korea": "KOSPI", "uk": "FTSE 100",
    "sweden": "OMX Stockholm 30", "switzerland": "SMI",
    "canada": "TSX Composite", "taiwan": "TAIEX",
    "thailand": "SET Index", "singapore": "STI",
}

# Exchanges in the comparison post (current local-benchmark run). SAO/ASX excluded
# for data quality; BSE_NSE is a stale SPY-benchmarked run superseded by NSE.
COMPARISON_UNIVERSES = {
    "OSL": "OSL (Norway)*", "STO": "STO (Sweden)", "SET": "SET (Thailand)",
    "SIX": "SIX (Switzerland)", "SES": "SES (Singapore)", "TSX": "TSX (Canada)",
    "JNB": "JNB (South Africa)", "SHZ_SHH": "SHZ+SHH (China)", "LSE": "LSE (UK)",
    "KSC": "KSC (Korea)", "XETRA": "XETRA (Germany)", "NSE": "NSE (India)",
    "HKSE": "HKSE (Hong Kong)", "TAI": "TAI (Taiwan)", "JPX": "JPX (Japan)",
    "NYSE_NASDAQ": "NYSE+NASDAQ (US)",
}
# No local index data: measured against SPY in USD, so the gap is cross-currency
CROSS_CCY_UNIVERSES = {"JNB"}
CROSS_CCY_COLOR = "#9E9E9E"
COMPARISON_NOTE = ("Each exchange in local currency vs its own local index. JNB has no local index data, "
                   "so its ZAR returns sit against the S&P 500 in USD (ZAR fell 4.8%/yr vs USD, 2005-2025).\n"
                   "*Norway: 12 periods (2013-2024). SAO and ASX excluded for data quality.")


def load_results(filename):
    path = os.path.join(RESULTS_DIR, filename)
    with open(path) as f:
        return json.load(f)


def cumulative_growth_chart(results, exchange_name, output_path, bench_label="S&P 500",
                            data=None, data_key=None):
    """Cumulative growth of $10,000. `data`/`data_key` enable the S&P 500 (USD) proxy test."""
    annual = results.get("annual_returns", [])
    if not annual:
        return

    years = [ar["year"] for ar in annual]
    port_cum = [10000]
    spy_cum = [10000]
    for ar in annual:
        port_cum.append(port_cum[-1] * (1 + ar["portfolio"] / 100))
        spy_cum.append(spy_cum[-1] * (1 + ar["spy"] / 100))

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(range(len(port_cum)), port_cum, color=STRATEGY_COLOR, linewidth=2,
            label=f"{STRATEGY_NAME}")
    ax.plot(range(len(spy_cum)), spy_cum, color=BENCH_COLOR, linewidth=2,
            label=bench_label)

    x_labels = [str(years[0] - 1)] + [str(y) for y in years]
    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
    ex_key = results.get("universe")
    usd_proxy = data is not None and is_usd_benchmark_proxy(data, data_key)
    if usd_proxy:
        # Strategy in local currency, S&P 500 in USD: no single currency fits the axis
        ccy = currency_code(ex_key)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.set_title(f"{STRATEGY_NAME}: Growth of {ccy} 10,000 vs US\\$10,000 ({exchange_name})",
                     fontsize=13, fontweight="bold")
        ax.set_ylabel(f"Value (strategy in {ccy}, S&P 500 in USD)")
        # higher end label above its point, lower one below, so close endpoints don't collide
        port_dy, spy_dy = (4, -12) if port_cum[-1] >= spy_cum[-1] else (-12, 4)
        ax.annotate(money(port_cum[-1] / 1000, ex_key, suffix="K"),
                    (len(port_cum) - 1, port_cum[-1]), xytext=(6, port_dy), textcoords="offset points",
                    color=STRATEGY_COLOR, fontsize=9, fontweight="bold")
        ax.annotate(benchmark_money(spy_cum[-1] / 1000, data, data_key, suffix="K"),
                    (len(spy_cum) - 1, spy_cum[-1]), xytext=(6, spy_dy), textcoords="offset points",
                    color=BENCH_COLOR, fontsize=9, fontweight="bold")
        fig.text(0.5, -0.02, "S&P 500 in USD (no local index in the data)",
                 ha="center", fontsize=8, color="gray")
    else:
        ax.yaxis.set_major_formatter(money_formatter(ex_key))
        ax.set_title(localize_money_title(
                         f"{STRATEGY_NAME}: Cumulative Growth of $10,000 ({exchange_name})",
                         ex_key),
                     fontsize=13, fontweight="bold")
        ax.set_ylabel(money_axis_label(ex_key))
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def annual_returns_chart(results, exchange_name, output_path, bench_label="S&P 500"):
    """Annual returns bar chart (strategy vs benchmark)."""
    annual = results.get("annual_returns", [])
    if not annual:
        return

    years = [str(ar["year"]) for ar in annual]
    port_rets = [ar["portfolio"] for ar in annual]
    spy_rets = [ar["spy"] for ar in annual]

    x = range(len(years))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar([i - width / 2 for i in x], port_rets, width, color=STRATEGY_COLOR,
           label=STRATEGY_NAME, alpha=0.85)
    ax.bar([i + width / 2 for i in x], spy_rets, width, color=BENCH_COLOR,
           label=bench_label, alpha=0.85)

    ax.set_xticks(list(x))
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.set_title(f"{STRATEGY_NAME}: Annual Returns ({exchange_name})",
                 fontsize=13, fontweight="bold")
    ax.set_ylabel("Return (%)")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def comparison_cagr_chart(all_results, output_path):
    """CAGR per exchange, with each exchange's own local benchmark CAGR as a marker."""
    data = []
    for uni, r in all_results.items():
        if "error" in r or not r.get("portfolio"):
            continue
        cagr = r["portfolio"].get("cagr")
        excess = r["comparison"].get("excess_cagr")
        bench = benchmark_cagr(all_results, uni)
        if cagr is not None and excess is not None and bench is not None:
            data.append((uni, cagr, excess, bench, benchmark_label(all_results, uni)))

    # Sorted by excess vs local index, as in the post's table
    data.sort(key=lambda x: x[2], reverse=True)
    names = [COMPARISON_UNIVERSES.get(d[0], d[0]) for d in data]
    cagrs = [d[1] for d in data]
    colors = [CROSS_CCY_COLOR if d[0] in CROSS_CCY_UNIVERSES
              else EXCESS_POS_COLOR if d[2] > 0 else EXCESS_NEG_COLOR for d in data]

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85)
    ax.scatter([d[3] for d in data], range(len(data)), marker="|", s=500,
               linewidths=3, color="black", zorder=3)

    for i, (uni, cagr, excess, bench, bname) in enumerate(data):
        suffix = ", cross-currency" if uni in CROSS_CCY_UNIVERSES else ""
        label = f"{cagr:.2f}% ({excess:+.2f}% vs {bname}{suffix})"
        ax.text(max(cagr, bench, 0) + 0.4, i, label, va="center", fontsize=8)

    ax.axvline(x=0, color="black", linewidth=0.5)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.set_xlabel("CAGR (%, local currency)")
    ax.set_xlim(right=max(max(d[1], d[3]) for d in data) + 9)
    ax.set_title(f"{STRATEGY_NAME}: CAGR vs Local Benchmark by Exchange (2005-2025)",
                 fontsize=13, fontweight="bold")
    ax.legend(handles=[
        Patch(color=EXCESS_POS_COLOR, alpha=0.85, label="Beat local index"),
        Patch(color=EXCESS_NEG_COLOR, alpha=0.85, label="Trailed local index"),
        Patch(color=CROSS_CCY_COLOR, alpha=0.85, label="No local index (vs S&P 500, USD)"),
        Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
               markeredgewidth=3, label="Benchmark CAGR"),
    ], loc="lower right")
    ax.grid(True, alpha=0.3, axis="x")
    ax.invert_yaxis()
    fig.text(0.01, 0.005, COMPARISON_NOTE, fontsize=7.5, color="#555555", ha="left")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def comparison_drawdown_chart(all_results, output_path):
    """Max drawdown comparison across exchanges."""
    data = []
    for uni, r in all_results.items():
        if "error" in r or not r.get("portfolio"):
            continue
        maxdd = r["portfolio"].get("max_drawdown")
        bench_dd = (r.get("spy") or {}).get("max_drawdown")
        if maxdd is not None:
            data.append((uni, maxdd, bench_dd))

    data.sort(key=lambda x: x[1])  # Most negative first
    names = [COMPARISON_UNIVERSES.get(d[0], d[0]) for d in data]
    drawdowns = [d[1] for d in data]

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(names)), drawdowns, color="#E53935", alpha=0.75)
    bench_rows = [(i, d[2]) for i, d in enumerate(data) if d[2] is not None]
    ax.scatter([b for _, b in bench_rows], [i for i, _ in bench_rows], marker="|", s=500,
               linewidths=3, color="black", zorder=3, label="Benchmark max drawdown (local index; S&P 500 for US, JNB)")

    for i, (name, dd, _) in enumerate(data):
        ax.text(dd + 0.5, i, f"{dd:.1f}%", va="center", ha="left", fontsize=8, color="white")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.set_xlabel("Max Drawdown (%)")
    ax.set_title(f"{STRATEGY_NAME}: Max Drawdown by Exchange",
                 fontsize=13, fontweight="bold")
    ax.legend(loc="lower left")
    ax.grid(True, alpha=0.3, axis="x")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {output_path}")


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # Per-exchange charts
    exchange_map = {
        "returns_NYSE_NASDAQ.json": ("US (NYSE + NASDAQ)", "us"),
        "returns_NSE.json": ("India (NSE)", "india"),
        "returns_XETRA.json": ("Germany (XETRA)", "germany"),
        "returns_SHZ_SHH.json": ("China (SHZ + SHH)", "china"),
        "returns_OSL.json": ("Norway (OSL)", "norway"),
        "returns_JNB.json": ("South Africa (JNB)", "southafrica"),
    }

    # Keyed like the results files, US included, so the USD-proxy series test can run
    loaded = {f[len("returns_"):-len(".json")]: load_results(f) for f in exchange_map
              if os.path.exists(os.path.join(RESULTS_DIR, f))}

    for filename, (display_name, short_name) in exchange_map.items():
        key = filename[len("returns_"):-len(".json")]
        if key not in loaded:
            print(f"  Skipping {filename} (not found)")
            continue

        results = loaded[key]
        bench_label = BENCHMARK_LABELS.get(short_name, "S&P 500")
        # S&P 500 on a local-currency axis (JNB): say it's USD
        cum_label = (benchmark_legend(loaded, key)
                     if is_usd_benchmark_proxy(loaded, key) else bench_label)
        print(f"\nGenerating charts for {display_name}...")

        cumulative_growth_chart(
            results, display_name,
            os.path.join(CHARTS_DIR, f"1_{short_name}_cumulative_growth.png"),
            bench_label=cum_label, data=loaded, data_key=key)
        annual_returns_chart(
            results, display_name,
            os.path.join(CHARTS_DIR, f"2_{short_name}_annual_returns.png"),
            bench_label=bench_label)

    # Comparison charts - only the exchanges the comparison post covers
    print("\nGenerating comparison charts...")
    all_results = {}
    for uni in COMPARISON_UNIVERSES:
        fname = f"returns_{uni}.json"
        if not os.path.exists(os.path.join(RESULTS_DIR, fname)):
            print(f"  WARNING: {fname} missing, dropped from comparison charts")
            continue
        all_results[uni] = load_results(fname)
    if all_results:
        comparison_cagr_chart(
            all_results,
            os.path.join(CHARTS_DIR, "1_comparison_cagr.png"))
        comparison_drawdown_chart(
            all_results,
            os.path.join(CHARTS_DIR, "2_comparison_drawdown.png"))

    print(f"\nAll charts saved to {CHARTS_DIR}/")


if __name__ == "__main__":
    main()
