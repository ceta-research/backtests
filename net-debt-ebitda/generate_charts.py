#!/usr/bin/env python3
"""Generate all Net Debt/EBITDA charts for blog posts.

Single-exchange charts read exchange_comparison.json. The comparison charts read
the per-exchange files the comparison post's table was built from (see
COMPARISON_FILES); exchange_comparison.json is an older run that disagrees with it.
"""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
import json
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_label, currency_prefix,
                         localize_money_title, money)
from cli_utils import get_mktcap_threshold

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Comparison-chart sources, one per post-table row. returns_STO.json is a SPY-benchmarked
# run, so Sweden comes from sweden.json (OMX30). OSL is left out: no file reproduces its row (B006).
COMPARISON_FILES = {"NSE": "india.json", "STO": "sweden.json", "NYSE_NASDAQ_AMEX": "us.json"}
COMPARISON_FILES.update({k: f"returns_{k}.json" for k in (
    "SAO", "TSX", "MIL", "SHZ_SHH", "XETRA", "SIX", "KSC", "JPX", "TAI", "LSE",
    "AMS", "HKSE", "ASX", "SAU", "SET", "BME", "SES", "TLV")})
OMITTED_NOTE = "Oslo omitted: no reproducible run."


def load_comparison():
    out = {}
    for key, fname in COMPARISON_FILES.items():
        with open(results_dir / fname) as f:
            out[key] = json.load(f)
    return out


comparison = load_comparison()


def spy_benchmarked(entries):
    """Keys whose benchmark series is the US entry's SPY series, in display form."""
    spy_series = [a["spy"] for a in entries["NYSE_NASDAQ_AMEX"]["annual_returns"]]
    keys = [k for k, v in entries.items() if [a["spy"] for a in v["annual_returns"]] == spy_series]
    return sorted("US" if k == "NYSE_NASDAQ_AMEX" else k for k in keys)

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "STO": "#3498db",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI": "#d35400",
    "KSC": "#95a5a6",
    "SET": "#2ecc71",
    "XETRA": "#27ae60",
    "SIX": "#1abc9c",
    "TSX": "#7f8c8d",
    "ASX": "#f39c12",
    "SAO": "#e74c3c",
    "OSL": "#17a589",
    "SES": "#a93226",
    "MIL": "#5d6d7e",
    "TLV": "#9b59b6",
    "AMS": "#2e86c1",
    "BME": "#cb4335",
    "SAU": "#d4ac0d",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Net Debt/EBITDA US",
    "NSE": "Net Debt/EBITDA India",
    "STO": "Net Debt/EBITDA Sweden",
    "SHZ_SHH": "Net Debt/EBITDA China",
    "HKSE": "Net Debt/EBITDA Hong Kong",
    "TAI": "Net Debt/EBITDA Taiwan",
    "KSC": "Net Debt/EBITDA Korea",
    "SET": "Net Debt/EBITDA Thailand",
    "XETRA": "Net Debt/EBITDA Germany",
    "SIX": "Net Debt/EBITDA Switzerland",
    "TSX": "Net Debt/EBITDA Canada",
    "ASX": "Net Debt/EBITDA Australia",
    "SAO": "Net Debt/EBITDA Brazil",
    "OSL": "Net Debt/EBITDA Norway",
    "SES": "Net Debt/EBITDA Singapore",
    "MIL": "Net Debt/EBITDA Italy",
    "TLV": "Net Debt/EBITDA Israel",
    "AMS": "Net Debt/EBITDA Netherlands",
    "BME": "Net Debt/EBITDA Spain",
    "SAU": "Net Debt/EBITDA Saudi Arabia",
}

FOOTER = "Data: Ceta Research | Net Debt/EBITDA <2x, ROE >10%, MCap >$1B, top 30 by lowest ratio, quarterly rebalance, equal weight, 2000-2025"


def mcap_floor(exchange_key):
    """The exchange's own local-currency market-cap floor (e.g. Rs20B, SEK 5B)."""
    n = get_mktcap_threshold(exchange_key.split("_"))
    div, unit = next((d, u) for d, u in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1, "")) if n >= d)
    return f"{currency_prefix(exchange_key)}{n / div:g}{unit}"


def footer(exchange_key):
    """Footer with the exchange's own market-cap floor."""
    return FOOTER.replace("MCap >$1B", f"MCap >{mcap_floor(exchange_key)}")


def comparison_footer():
    """Multi-exchange footer: the floor differs per exchange, so name a few."""
    eg = ", ".join(f"{mcap_floor(k)} {c}" for k, c in
                   (("NYSE_NASDAQ_AMEX", "US"), ("NSE", "India"), ("STO", "Sweden")))
    return (FOOTER.replace("MCap >$1B", f"MCap floor per exchange in local currency ({eg})")
            + f"\nReturns in local currency. {OMITTED_NOTE}")


def get_cumulative_growth(exchange_key, initial=10000):
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


def format_k(exchange_key):
    """Tick formatter for thousands in the exchange's own currency, e.g. "Rs149K".

    Returns are in the local currency, so the ticks must be too. A hardcoded "$"
    here overstates the implied wealth by the FX rate.
    """
    prefix = currency_prefix(exchange_key)
    return mticker.FuncFormatter(lambda val, pos: f"{prefix}{val/1000:,.0f}K")


def chart_cumulative_single(exchange_key, filename, title_suffix=""):
    """Cumulative growth chart for a single exchange vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    spy_years, spy_vals = get_spy_cumulative(exchange_key)
    spy_cagr = data[exchange_key]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_label(data, exchange_key)} ({spy_cagr}% CAGR)", linestyle="--", zorder=2)

    years, vals = get_cumulative_growth(exchange_key)
    ex = data[exchange_key]
    cagr = ex["portfolio"]["cagr"]
    label = f"{EXCHANGE_LABELS.get(exchange_key, exchange_key)} ({cagr}% CAGR)"
    color = COLORS.get(exchange_key, "#1a5276")
    ax.plot(years, vals, color=color, linewidth=2.4, label=label, zorder=3)

    ax.yaxis.set_major_formatter(format_k(exchange_key))
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel(f"Portfolio Value ({money(10000, exchange_key)} initial)", fontsize=11)
    ax.set_title(localize_money_title(
                     f"Net Debt/EBITDA Strategy vs {benchmark_label(data, exchange_key)}{title_suffix}"
                     "\n$10,000 initial investment, 2000–2025",
                     exchange_key),
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10, loc="upper left")
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.02, footer(exchange_key), ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def chart_annual_returns(exchange_key, filename, title_suffix=""):
    """Annual returns bar chart for a single exchange vs SPY."""
    ex = data[exchange_key]
    years = [r["year"] for r in ex["annual_returns"]]
    portfolio = [r["portfolio"] for r in ex["annual_returns"]]
    spy = [r["spy"] for r in ex["annual_returns"]]

    x = range(len(years))
    width = 0.38
    color = COLORS.get(exchange_key, "#1a5276")

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    ax.bar([i - width/2 for i in x], portfolio, width, label="Net Debt/EBITDA Strategy",
           color=color, alpha=0.85)
    ax.bar([i + width/2 for i in x], spy, width, label=benchmark_label(data, exchange_key),
           color=COLORS["SPY"], alpha=0.85)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.set_title(f"Annual Returns: Net Debt/EBITDA Strategy vs {benchmark_label(data, exchange_key)}{title_suffix}",
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.04, footer(exchange_key), ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {filename}")


def chart_comparison_cagr():
    """CAGR bar chart across all exchanges, each against its own benchmark."""
    spy_keys = spy_benchmarked(comparison)
    exchange_data = []
    for key, val in comparison.items():
        exchange_data.append({
            "key": key,
            "cagr": val["portfolio"]["cagr"],
            "bench": val["spy"]["cagr"],
            "excess": val["comparison"]["excess_cagr"],
            "is_spy": ("US" if key == "NYSE_NASDAQ_AMEX" else key) in spy_keys,
        })
    exchange_data.sort(key=lambda x: x["cagr"], reverse=True)

    labels = [e["key"].replace("_", "\n") for e in exchange_data]
    cagrs = [e["cagr"] for e in exchange_data]
    x = range(len(labels))

    # Colour = excess vs the exchange's OWN benchmark, which is the marker on each bar.
    colors = ["#27ae60" if e["excess"] > 0 else "#c0392b" for e in exchange_data]

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    bars = ax.bar(x, cagrs, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
    for is_spy, color, label in ((False, "#1a1a1a", "Own benchmark: local index CAGR (local currency)"),
                                 (True, "#1a5276", "Own benchmark: S&P 500 CAGR (USD)")):
        pts = [(i, e["bench"]) for i, e in enumerate(exchange_data) if e["is_spy"] == is_spy]
        ax.scatter([p[0] for p in pts], [p[1] for p in pts], marker="_", s=700, linewidths=3,
                   color=color, zorder=6, label=label)

    for bar, e in zip(bars, exchange_data):
        ax.text(bar.get_x() + bar.get_width()/2, max(e["cagr"], e["bench"]) + 0.25,
                f"{e['cagr']:.2f}%", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("CAGR (%, local currency)", fontsize=11)
    ax.set_title(f"Net Debt/EBITDA Strategy CAGR: {len(comparison)} Exchanges (2000–2025)",
                 fontsize=13, fontweight="bold", pad=26)
    ax.text(0.5, 1.015, f"Colour = CAGR vs own benchmark (marker): S&P 500 in USD for {', '.join(spy_keys)}; "
            f"local index for the other {len(comparison) - len(spy_keys)}",
            transform=ax.transAxes, ha="center", fontsize=10)
    handles, _ = ax.get_legend_handles_labels()
    ax.legend(handles=[Patch(color="#27ae60", alpha=0.85, label="Beats its own benchmark"),
                       Patch(color="#c0392b", alpha=0.85, label="Trails its own benchmark")] + handles,
              fontsize=10)
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.04, comparison_footer(), ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / "1_comparison_cagr.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: 1_comparison_cagr.png")


def chart_comparison_drawdown():
    """Max drawdown comparison across all exchanges."""
    exchange_data = []
    for key, val in comparison.items():
        exchange_data.append({
            "key": key,
            "drawdown": val["portfolio"]["max_drawdown"],
        })
    exchange_data.sort(key=lambda x: x["drawdown"])

    labels = [e["key"].replace("_", "\n") for e in exchange_data]
    dds = [e["drawdown"] for e in exchange_data]
    spy_dd = comparison["NYSE_NASDAQ_AMEX"]["spy"]["max_drawdown"]  # US entry's SPY

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor("#f8f9fa")
    ax.set_facecolor("#f8f9fa")

    bars = ax.bar(range(len(labels)), dds, color="#c0392b", alpha=0.75, edgecolor="white", linewidth=0.5)
    ax.axhline(spy_dd, color="#1a5276", linewidth=2, linestyle="--",
               label=f"S&P 500 max drawdown in USD ({spy_dd:.2f}%), reference only")
    for bar, dd in zip(bars, dds):
        ax.text(bar.get_x() + bar.get_width()/2, dd - 0.8, f"{dd:.1f}%",
                ha="center", va="top", fontsize=7.5, fontweight="bold")

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Max Drawdown (%)", fontsize=11)
    ax.set_ylim(min(dds) - 6, 0)
    ax.set_title(f"Max Drawdown of the Net Debt/EBITDA Strategy: {len(comparison)} Exchanges (2000–2025)",
                 fontsize=13, fontweight="bold", pad=12)
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, color="#cccccc")
    ax.spines[["top", "right"]].set_visible(False)
    plt.figtext(0.5, -0.04, comparison_footer(), ha="center", fontsize=8, color="#666666")
    plt.tight_layout()
    plt.savefig(charts_dir / "2_comparison_drawdown.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: 2_comparison_drawdown.png")


if __name__ == "__main__":
    print("Generating Net Debt/EBITDA charts...")

    # US charts
    print("\nUS:")
    chart_cumulative_single("NYSE_NASDAQ_AMEX", "1_us_cumulative_growth.png", " (US)")
    chart_annual_returns("NYSE_NASDAQ_AMEX", "2_us_annual_returns.png", " (US)")

    # India charts
    print("\nIndia:")
    chart_cumulative_single("NSE", "1_india_cumulative_growth.png", " (India: NSE)")
    chart_annual_returns("NSE", "2_india_annual_returns.png", " (India: NSE)")

    # Sweden charts
    print("\nSweden:")
    chart_cumulative_single("STO", "1_sweden_cumulative_growth.png", " (Sweden: STO)")
    chart_annual_returns("STO", "2_sweden_annual_returns.png", " (Sweden: STO)")

    # Comparison charts
    print("\nComparison:")
    chart_comparison_cagr()
    chart_comparison_drawdown()

    print(f"\nAll charts saved to: {charts_dir}")
    print("Copy to content directory:")
    print("  cp charts/1_us_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/us/")
    print("  cp charts/2_us_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/us/")
    print("  cp charts/1_india_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/india/")
    print("  cp charts/2_india_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/india/")
    print("  cp charts/1_sweden_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/sweden/")
    print("  cp charts/2_sweden_* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/sweden/")
    print("  cp charts/1_comparison* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/comparison/")
    print("  cp charts/2_comparison* ../ts-content-creator/content/_current/risk-04-net-debt-ebitda/blogs/comparison/")
