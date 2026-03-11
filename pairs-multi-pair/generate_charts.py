#!/usr/bin/env python3
"""Generate charts for multi-pair portfolio construction analysis.

Reads:
    results/exchange_comparison.json

Produces 3 charts saved to pairs-multi-pair/charts/:
    1. 1_us_cumulative_growth.png   — $1000 growth: 20-pair inv-vol vs 20-pair equal vs SPY
    2. 2_us_annual_returns.png      — Annual bars: 20-pair inv-vol vs SPY
    3. 3_diversification_curve.png  — Sharpe and MaxDD vs portfolio size (equal vs inv-vol)

Usage:
    python3 pairs-multi-pair/generate_charts.py

    # Use a different results file
    python3 pairs-multi-pair/generate_charts.py --results path/to/results.json
"""

import argparse
import json
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ─── Paths ────────────────────────────────────────────────────────────────────
_ROOT       = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR  = os.path.join(_ROOT, "charts")
RESULTS_DIR = os.path.join(_ROOT, "results")

DEFAULT_RESULTS = os.path.join(RESULTS_DIR, "exchange_comparison.json")

# ─── Color palette ────────────────────────────────────────────────────────────
COLOR_INV_VOL  = "#1a5276"   # dark blue — primary (inv-vol) series
COLOR_EQUAL    = "#7fb3d3"   # light blue — secondary (equal weight) series
COLOR_SPY      = "#aaaaaa"   # gray — SPY benchmark
COLOR_POSITIVE = "#1a5276"   # dark blue bars (positive annual returns)
COLOR_NEGATIVE = "#c0392b"   # red bars (negative annual returns)
COLOR_SHARPE   = "#1a5276"   # dark blue line for Sharpe
COLOR_MDD      = "#c0392b"   # red line for MaxDD

FOOTER_TEXT = "Data: Ceta Research (FMP warehouse) | 2005-2024 | Costs: size-tiered"


def load_results(path):
    """Load exchange_comparison.json. Exits if not found."""
    if not os.path.exists(path):
        print(f"ERROR: Results file not found: {path}")
        print("Run backtest.py first to generate results.")
        sys.exit(1)
    with open(path) as f:
        data = json.load(f)
    print(f"Loaded results from {path}")
    return data


def extract_us_data(data):
    """Extract US result from exchange_comparison.json.

    Handles both single-exchange format (when run with --preset us) and
    multi-exchange format (when run with --global, keyed by universe name).
    """
    # Single exchange format: data has "annual_returns" and "portfolio" directly
    if "annual_returns" in data and "portfolio" in data:
        return data

    # Multi-exchange format: look for US keys
    for key in ["NYSE_NASDAQ_AMEX", "US_MAJOR", "us"]:
        if key in data:
            return data[key]

    # Fall back to first entry that has annual_returns
    for key, val in data.items():
        if isinstance(val, dict) and "annual_returns" in val:
            print(f"  Using '{key}' as US proxy (no NYSE_NASDAQ_AMEX key found)")
            return val

    print("ERROR: No valid exchange data found in results file.")
    sys.exit(1)


def compute_cumulative(annual_returns, key="portfolio"):
    """Compute cumulative $1000 growth from annual return series."""
    values = [1000.0]
    for row in annual_returns:
        r = row.get(key)
        if r is None:
            values.append(values[-1])
        else:
            values.append(values[-1] * (1 + r / 100.0))
    return values


def chart_cumulative_growth(us_data, output_path):
    """Chart 1: $1000 cumulative growth for 20-pair inv-vol vs equal-weight vs SPY.

    The primary series (inv-vol) is stored in annual_returns["portfolio"].
    The equal-weight 20-pair series is stored in diversification_analysis
    as {n_pairs=20, allocation="equal"} — but we don't have its yearly series
    directly. We reconstruct a proxy from the diversification_analysis metrics
    for display purposes.

    NOTE: We only have the per-year inv-vol series in annual_returns. For the
    equal-weight 20-pair overlay we use the summary CAGR to draw an idealized
    line, which is clearly labelled. This avoids requiring a separate annual
    series for every combination in the JSON.
    """
    annual_returns = us_data.get("annual_returns", [])
    if not annual_returns:
        print("WARNING: No annual_returns data, skipping Chart 1.")
        return

    years      = [r["year"] for r in annual_returns]
    inv_vol    = compute_cumulative(annual_returns, "portfolio")
    spy_series = compute_cumulative(annual_returns, "spy")

    # Equal-weight 20-pair: pull CAGR from diversification_analysis
    div_analysis = us_data.get("diversification_analysis", [])
    eq_20_entry  = next((d for d in div_analysis
                         if d["n_pairs"] == 20 and d["allocation"] == "equal"), None)

    # Build x-axis: years + final year
    x_years = [years[0] - 1] + years   # starting point before first year

    fig, ax = plt.subplots(figsize=(13, 6))

    # Primary: 20-pair inv-vol
    ax.plot(x_years, inv_vol, color=COLOR_INV_VOL, linewidth=2.5,
            label="20-pair (inv-vol)", zorder=3)

    # Overlay: SPY
    ax.plot(x_years, spy_series, color=COLOR_SPY, linewidth=1.8,
            label="S&P 500 (SPY)", linestyle="--", zorder=2)

    # Overlay: 20-pair equal-weight (idealised from CAGR if no yearly series)
    if eq_20_entry and eq_20_entry.get("cagr_pct") is not None:
        eq_cagr    = eq_20_entry["cagr_pct"] / 100.0
        n_years    = len(years)
        eq_series  = [1000.0 * ((1 + eq_cagr) ** i) for i in range(n_years + 1)]
        ax.plot(x_years, eq_series, color=COLOR_EQUAL, linewidth=1.8,
                label=f"20-pair (equal, {eq_cagr*100:.1f}% CAGR idealised)",
                linestyle="-.", zorder=2)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"${v:,.0f}"
    ))
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Portfolio Value (starting $1,000)", fontsize=11)

    inv_vol_cagr = (us_data.get("portfolio") or {}).get("cagr")
    spy_cagr     = (us_data.get("spy") or {}).get("cagr")
    title = "Multi-Pair Portfolio: Cumulative Growth of $1,000 (US)"
    if inv_vol_cagr is not None and spy_cagr is not None:
        title += f"\n20-pair inv-vol: {inv_vol_cagr}% CAGR vs SPY: {spy_cagr}% CAGR"
    ax.set_title(title, fontsize=12, pad=14)

    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 1 saved: {output_path}")


def chart_annual_returns(us_data, output_path):
    """Chart 2: Annual return bars for 20-pair inv-vol vs SPY."""
    annual_returns = us_data.get("annual_returns", [])
    if not annual_returns:
        print("WARNING: No annual_returns data, skipping Chart 2.")
        return

    years      = [r["year"] for r in annual_returns]
    port_rets  = [r.get("portfolio") for r in annual_returns]
    spy_rets   = [r.get("spy") for r in annual_returns]
    is_cash    = [r.get("is_cash", False) for r in annual_returns]

    x     = np.arange(len(years))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 6))

    # Portfolio bars (color by sign; hatched if cash period)
    for i, (yr, pr, cash) in enumerate(zip(years, port_rets, is_cash)):
        if pr is None:
            continue
        color   = COLOR_INV_VOL if pr >= 0 else COLOR_NEGATIVE
        hatch   = "/" if cash else None
        bar = ax.bar(i - width / 2, pr, width, color=color, alpha=0.85,
                     hatch=hatch, edgecolor="white", linewidth=0.5)

    # SPY bars (always gray)
    for i, sr in enumerate(spy_rets):
        if sr is None:
            continue
        color = COLOR_SPY if sr >= 0 else "#888888"
        ax.bar(i + width / 2, sr, width, color=color, alpha=0.7,
               edgecolor="white", linewidth=0.5)

    # Legend proxies
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLOR_INV_VOL, label="20-pair inv-vol (positive)"),
        Patch(facecolor=COLOR_NEGATIVE, label="20-pair inv-vol (negative)"),
        Patch(facecolor=COLOR_SPY, label="SPY", alpha=0.7),
        Patch(facecolor="white", hatch="////", edgecolor="gray",
              label="Cash period (< 3 active pairs)"),
    ]
    ax.legend(handles=legend_elements, fontsize=9)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.set_ylabel("Annual Return (%)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))

    n_cash = sum(1 for c in is_cash if c)
    ax.set_title(
        f"Multi-Pair Portfolio: Annual Returns vs SPY (US, 20-pair inv-vol)\n"
        f"Cash periods: {n_cash}/{len(years)} years",
        fontsize=12, pad=14,
    )
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 2 saved: {output_path}")


def chart_diversification_curve(us_data, output_path):
    """Chart 3: Sharpe ratio and Max Drawdown vs portfolio size (N=5,10,15,20).

    Two lines per metric: equal-weight vs inverse-volatility.
    Dual-axis: Sharpe on left (higher = better), MaxDD on right (less negative = better).
    """
    div_analysis = us_data.get("diversification_analysis", [])
    if not div_analysis:
        print("WARNING: No diversification_analysis data, skipping Chart 3.")
        return

    # Organise into lookup: {(n_pairs, allocation): row}
    lookup = {(d["n_pairs"], d["allocation"]): d for d in div_analysis}

    sizes      = sorted(set(d["n_pairs"] for d in div_analysis))
    allocs     = ["equal", "inverse_vol"]
    alloc_labels = {"equal": "Equal weight", "inverse_vol": "Inverse-vol"}

    # Sharpe lines
    sharpe_eq  = [lookup.get((n, "equal"),       {}).get("sharpe") for n in sizes]
    sharpe_iv  = [lookup.get((n, "inverse_vol"),  {}).get("sharpe") for n in sizes]

    # MaxDD lines (stored as negative percentage, e.g. -30.5)
    mdd_eq     = [lookup.get((n, "equal"),       {}).get("max_drawdown_pct") for n in sizes]
    mdd_iv     = [lookup.get((n, "inverse_vol"),  {}).get("max_drawdown_pct") for n in sizes]

    # Excess CAGR lines
    exc_eq     = [lookup.get((n, "equal"),       {}).get("excess_cagr_pct") for n in sizes]
    exc_iv     = [lookup.get((n, "inverse_vol"),  {}).get("excess_cagr_pct") for n in sizes]

    fig, axes = plt.subplots(1, 3, figsize=(16, 6))

    # ── Panel 1: Sharpe ratio ─────────────────────────────────────────────────
    ax1 = axes[0]
    if any(v is not None for v in sharpe_eq):
        ax1.plot(sizes, sharpe_eq, color=COLOR_EQUAL, linewidth=2,
                 marker="o", label="Equal weight")
    if any(v is not None for v in sharpe_iv):
        ax1.plot(sizes, sharpe_iv, color=COLOR_INV_VOL, linewidth=2.5,
                 marker="s", label="Inverse-vol")
    ax1.set_xlabel("Portfolio Size (N pairs)", fontsize=10)
    ax1.set_ylabel("Sharpe Ratio", fontsize=10)
    ax1.set_title("Sharpe Ratio vs Portfolio Size", fontsize=11)
    ax1.legend(fontsize=9)
    ax1.set_xticks(sizes)
    ax1.grid(alpha=0.3, linestyle="--")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # ── Panel 2: Max Drawdown ─────────────────────────────────────────────────
    ax2 = axes[1]
    if any(v is not None for v in mdd_eq):
        ax2.plot(sizes, mdd_eq, color=COLOR_EQUAL, linewidth=2,
                 marker="o", label="Equal weight")
    if any(v is not None for v in mdd_iv):
        ax2.plot(sizes, mdd_iv, color=COLOR_MDD, linewidth=2.5,
                 marker="s", linestyle="--", label="Inverse-vol")
    ax2.set_xlabel("Portfolio Size (N pairs)", fontsize=10)
    ax2.set_ylabel("Max Drawdown (%)", fontsize=10)
    ax2.set_title("Max Drawdown vs Portfolio Size", fontsize=11)
    ax2.legend(fontsize=9)
    ax2.set_xticks(sizes)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}%"))
    ax2.grid(alpha=0.3, linestyle="--")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    # ── Panel 3: Excess CAGR ──────────────────────────────────────────────────
    ax3 = axes[2]
    if any(v is not None for v in exc_eq):
        ax3.plot(sizes, exc_eq, color=COLOR_EQUAL, linewidth=2,
                 marker="o", label="Equal weight")
    if any(v is not None for v in exc_iv):
        ax3.plot(sizes, exc_iv, color=COLOR_INV_VOL, linewidth=2.5,
                 marker="s", label="Inverse-vol")
    ax3.axhline(0, color="black", linewidth=0.8, linestyle=":")
    ax3.set_xlabel("Portfolio Size (N pairs)", fontsize=10)
    ax3.set_ylabel("Excess CAGR vs SPY (%)", fontsize=10)
    ax3.set_title("Excess CAGR vs Portfolio Size", fontsize=11)
    ax3.legend(fontsize=9)
    ax3.set_xticks(sizes)
    ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.2f}%"))
    ax3.grid(alpha=0.3, linestyle="--")
    ax3.spines["top"].set_visible(False)
    ax3.spines["right"].set_visible(False)

    fig.suptitle(
        "Portfolio Size vs Risk-Adjusted Returns (US): Equal Weight vs Inverse-Vol",
        fontsize=13, y=1.02,
    )

    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 3 saved: {output_path}")


def chart_exchange_comparison(all_data, output_path):
    """Chart 4: Horizontal bar chart of CAGR by exchange (20-pair inv-vol, excluding JNB).

    Bars colored by sign. SPY CAGR shown as vertical reference line.
    Exchanges sorted by CAGR descending.
    """
    EXCLUDE = {"JNB"}
    EXCHANGE_LABELS = {
        "KSC": "Korea (KSC)",
        "LSE": "UK (LSE)",
        "BSE_NSE": "India (BSE+NSE)",
        "STO": "Sweden (STO)",
        "SHZ_SHH": "China (SHZ+SHH)",
        "TAI_TWO": "Taiwan (TAI+TWO)",
        "HKSE": "Hong Kong (HKSE)",
        "JPX": "Japan (JPX)",
        "NYSE_NASDAQ_AMEX": "US (NYSE+NASDAQ)",
        "XETRA": "Germany (XETRA)*",
        "TSX": "Canada (TSX)",
    }

    rows = []
    spy_cagr = None
    for ex, v in all_data.items():
        if ex in EXCLUDE:
            continue
        p = v.get("portfolio", {})
        cagr = p.get("cagr")
        if cagr is None:
            continue
        if spy_cagr is None:
            spy_cagr = (v.get("spy") or {}).get("cagr")
        label = EXCHANGE_LABELS.get(ex, ex)
        cash_pct = round(100 * v.get("cash_periods", 0) / max(v.get("n_years", 20), 1))
        rows.append((label, cagr, cash_pct))

    rows.sort(key=lambda x: x[1])  # ascending so best is at top in horizontal bar

    labels = [r[0] for r in rows]
    cagrs  = [r[1] for r in rows]
    colors = [COLOR_INV_VOL if c >= 0 else COLOR_NEGATIVE for c in cagrs]

    fig, ax = plt.subplots(figsize=(11, 6))

    bars = ax.barh(labels, cagrs, color=colors, edgecolor="white", linewidth=0.5)

    # Value labels
    for bar, val in zip(bars, cagrs):
        x_pos = val + 0.05 if val >= 0 else val - 0.05
        ha = "left" if val >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2,
                f"{val:+.2f}%", va="center", ha=ha, fontsize=9)

    # SPY reference line
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color=COLOR_SPY, linewidth=1.8, linestyle="--",
                   label=f"SPY: {spy_cagr:.2f}% CAGR")
        ax.legend(fontsize=10)

    ax.axvline(x=0, color="black", linewidth=0.8)
    ax.set_xlabel("CAGR (%)", fontsize=11)
    ax.set_title(
        "Multi-Pair Pairs Trading: CAGR by Exchange (20-pair inv-vol, 2005-2024)\n"
        "*XETRA: 40% cash periods — limited cointegrated pairs",
        fontsize=12, pad=14,
    )
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.1f}%"))
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 4 saved: {output_path}")


def chart_india_alpha_dilution(all_data, output_path):
    """Chart 5: India CAGR vs portfolio size (equal weight vs inv-vol).

    Shows the alpha-dilution curve — how India CAGR drops as more pairs are added.
    SPY reference line included.
    """
    india_data = all_data.get("BSE_NSE", {})
    div_analysis = india_data.get("diversification_analysis", [])
    if not div_analysis:
        print("WARNING: No India diversification data, skipping Chart 5.")
        return

    spy_cagr = (india_data.get("spy") or {}).get("cagr", 9.81)
    lookup   = {(d["n_pairs"], d["allocation"]): d for d in div_analysis}
    sizes    = sorted(set(d["n_pairs"] for d in div_analysis))

    cagr_eq = [lookup.get((n, "equal"), {}).get("cagr_pct") for n in sizes]
    cagr_iv = [lookup.get((n, "inverse_vol"), {}).get("cagr_pct") for n in sizes]

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.plot(sizes, cagr_eq, color=COLOR_EQUAL, linewidth=2.5, marker="o",
            label="Equal weight")
    ax.plot(sizes, cagr_iv, color=COLOR_INV_VOL, linewidth=2.5, marker="s",
            linestyle="--", label="Inverse-vol")
    ax.axhline(y=spy_cagr, color=COLOR_SPY, linewidth=1.8, linestyle=":",
               label=f"SPY benchmark ({spy_cagr:.2f}% CAGR)")

    ax.set_xlabel("Portfolio Size (N pairs)", fontsize=11)
    ax.set_ylabel("CAGR (%)", fontsize=11)
    ax.set_title(
        "India (BSE+NSE): CAGR vs Portfolio Size — The Alpha-Dilution Curve\n"
        "2005-2024 | Equal weight vs Inverse-vol",
        fontsize=12, pad=14,
    )
    ax.set_xticks(sizes)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.1f}%"))
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(0.5, -0.02, FOOTER_TEXT, ha="center", fontsize=8, color="#666666")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart 5 saved: {output_path}")


def print_diversification_table(div_analysis):
    """Print console summary of diversification analysis."""
    if not div_analysis:
        return
    print(f"\n  Diversification analysis:")
    print(f"  {'N':>4} {'Alloc':<12} {'CAGR':>7} {'Excess':>8} "
          f"{'Sharpe':>8} {'MaxDD':>8} {'Cash':>6}")
    print("  " + "-" * 60)
    for row in sorted(div_analysis, key=lambda r: (r["n_pairs"], r["allocation"])):
        cagr = f"{row['cagr_pct']:+.2f}%" if row.get("cagr_pct") is not None else "N/A"
        exc  = f"{row['excess_cagr_pct']:+.2f}%" if row.get("excess_cagr_pct") is not None else "N/A"
        shp  = f"{row['sharpe']:.3f}" if row.get("sharpe") is not None else "N/A"
        mdd  = f"{row['max_drawdown_pct']:.2f}%" if row.get("max_drawdown_pct") is not None else "N/A"
        print(f"  {row['n_pairs']:>4} {row['allocation']:<12} {cagr:>7} {exc:>8} "
              f"{shp:>8} {mdd:>8} {row['cash_periods']:>6}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate charts for multi-pair portfolio backtest")
    parser.add_argument("--results", type=str, default=DEFAULT_RESULTS,
                        help=f"Path to exchange_comparison.json (default: {DEFAULT_RESULTS})")
    args = parser.parse_args()

    print("Generating multi-pair portfolio charts...")
    print()

    # ── Load data ──────────────────────────────────────────────────────────────
    data    = load_results(args.results)
    us_data = extract_us_data(data)

    print(f"  Universe: {us_data.get('universe', 'unknown')}")
    print(f"  Years: {us_data.get('years', 'unknown')}")
    print(f"  Portfolio config: {us_data.get('portfolio_config', '20-pair inverse_vol')}")
    print_diversification_table(us_data.get("diversification_analysis", []))

    # ── Create output directory ────────────────────────────────────────────────
    os.makedirs(CHARTS_DIR, exist_ok=True)

    # ── Generate charts ────────────────────────────────────────────────────────
    chart_cumulative_growth(
        us_data,
        os.path.join(CHARTS_DIR, "1_us_cumulative_growth.png"),
    )
    chart_annual_returns(
        us_data,
        os.path.join(CHARTS_DIR, "2_us_annual_returns.png"),
    )
    chart_diversification_curve(
        us_data,
        os.path.join(CHARTS_DIR, "3_diversification_curve.png"),
    )
    chart_exchange_comparison(
        data,
        os.path.join(CHARTS_DIR, "4_exchange_comparison.png"),
    )
    chart_india_alpha_dilution(
        data,
        os.path.join(CHARTS_DIR, "5_india_alpha_dilution.png"),
    )

    print()
    print("Done. Charts saved to pairs-multi-pair/charts/")
    print()
    print("Next: move charts to blog directory:")
    print("  mv pairs-multi-pair/charts/*.png \\")
    print("     ../ts-content-creator/content/_current/pairs-06-multi-pair/blogs/us/")


if __name__ == "__main__":
    main()
