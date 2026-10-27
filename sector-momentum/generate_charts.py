"""Generate all Sector Momentum charts for blog posts from exchange_comparison.json.

Run from backtests/ directory:
    python3 sector-momentum/generate_charts.py

Charts are saved to backtests/sector-momentum/charts/.
Move each chart to the matching ts-content-creator blog directory after generation.
"""
import json
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_label, leg_excess, leg_invested,
                         leg_is_too_thin, market_name, not_ranked_lines, own_index_counts,
                         own_index_verdict, save_own_index_chart)

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "XETRA": "#27ae60",
    "LSE": "#154360",
    "TSX": "#7f8c8d",
    "KSC": "#6c3483",
    # ASX excluded: adjClose split artifacts (see DATA_QUALITY_ISSUES.md)
    "TAI_TWO": "#1a252f",
    # SAO excluded: adjClose split artifacts (see DATA_QUALITY_ISSUES.md)
    "HKSE": "#8e44ad",
    "SIX": "#d68910",
    "STO": "#2e86c1",
    "SET": "#5b2c6f",
    "JNB": "#6e2f1a",
    "JPX": "#1f618d",
    "SHH_SHZ": "#c0392b",
    # SES excluded: 61% cash (not enough sector diversity for strategy to run)
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Sector Momentum US",
    "NSE": "Sector Momentum India",
    "XETRA": "Sector Momentum Germany",
    "LSE": "Sector Momentum UK",
    "TSX": "Sector Momentum Canada",
    "KSC": "Sector Momentum Korea",
    "TAI_TWO": "Sector Momentum Taiwan",
    "HKSE": "Sector Momentum HK",
    "SIX": "Sector Momentum Switzerland",
    "STO": "Sector Momentum Sweden",
    "SET": "Sector Momentum Thailand",
    "JNB": "Sector Momentum S.Africa",
    "JPX": "Sector Momentum Japan",
    "SHH_SHZ": "Sector Momentum China",
}

EXCHANGE_UNIVERSE_LABELS = {
    "NYSE_NASDAQ_AMEX": "NYSE + NASDAQ + AMEX",
    "NSE": "NSE (returns in INR)",
    "XETRA": "XETRA (returns in EUR)",
    "LSE": "LSE (returns in GBP)",
    "TSX": "TSX (returns in CAD)",
    "KSC": "KSC (returns in KRW)",
    "TAI_TWO": "TAI + TWO (returns in TWD)",
    "HKSE": "HKSE (returns in HKD)",
    "SIX": "SIX (returns in CHF)",
    "STO": "STO (returns in SEK)",
    "SET": "SET (returns in THB)",
    "JNB": "JNB (returns in ZAR)",
    "JPX": "JPX (returns in JPY)",
    "SHH_SHZ": "SHH + SHZ (returns in CNY)",
}


def get_cumulative(key, initial=10000):
    ex = data[key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(key, filename, footer_universe):
    ex = data[key]
    cagr = ex["portfolio"]["cagr"]
    bench_cagr = ex["spy"]["cagr"]
    # The `spy` field holds whichever index THIS exchange was measured against.
    bench = benchmark_label(data, key)
    title = (f"Growth of 10,000: Sector Momentum {EXCHANGE_LABELS[key].replace('Sector Momentum ', '')} "
             f"vs {bench} (2000-2025)")

    fig, ax = plt.subplots(figsize=(12, 6))

    # Series must come from the charted exchange, not from the US anchor.
    spy_years, spy_vals = benchmark_cumulative(data, key)
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8, linestyle="--",
            label=f"{bench} ({bench_cagr}% CAGR)")

    years, vals = get_cumulative(key)
    ax.plot(years, vals, color=COLORS[key], linewidth=2.2,
            label=f"{EXCHANGE_LABELS[key]} ({cagr}% CAGR)")

    # Annotate endpoints
    ax.annotate(f"{vals[-1] / 1000:,.0f}K",
                xy=(years[-1], vals[-1]), xytext=(8, 0),
                textcoords="offset points", fontsize=9, fontweight="bold",
                color=COLORS[key])
    if spy_vals:
        ax.annotate(f"{spy_vals[-1] / 1000:,.0f}K",
                    xy=(spy_years[-1], spy_vals[-1]), xytext=(8, -12),
                    textcoords="offset points", fontsize=9, fontweight="bold",
                    color=COLORS["SPY"])

    # Returns are in local currency, so the axis carries no currency symbol.
    ax.set_ylabel("Portfolio Value (local currency)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)
    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(key, filename, footer_universe):
    ex = data[key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_rets = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_rets = [ar["spy"] for ar in ex["annual_returns"]]
    bench = benchmark_label(data, key)
    title = (f"Sector Momentum {EXCHANGE_LABELS[key].replace('Sector Momentum ', '')}: "
             f"Annual Returns vs {bench} ({years[0]}-{years[-1]})")

    fig, ax = plt.subplots(figsize=(14, 5))
    x = list(range(len(years)))
    width = 0.35

    ax.bar([xi - width / 2 for xi in x], spy_rets, width,
           label=bench, color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_rets, width,
           label=EXCHANGE_LABELS[key], color=COLORS[key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Ahead of a price index by less than its missing dividends, as the comparison post counts it
NOT_CLEAR_WIN = {"SHH_SHZ"}

DATA_LINE = "Data: Ceta Research | Top 2 sectors by 12M trailing return, quarterly rebalance, 2000-2025."


def comparison_legs():
    return [k for k, v in data.items()
            if v.get("invested_periods", 0) > 0 and v.get("portfolio", {}).get("cagr") is not None
            and not v.get("window_truncated", False)]


def left_out_lines():
    """Markets with a results file but left out of the comparison (too thin to test)."""
    thin = []
    for p in sorted(results_dir.glob("returns_*.json")):
        key = p.stem[len("returns_"):]
        if key in data:
            continue
        with open(p) as f:
            entry = json.load(f)
        if leg_is_too_thin(entry):
            inv, tot = leg_invested(entry)
            thin.append(f"{market_name(key, short=True)} (too thin to test, invested {inv} of "
                        f"{tot} quarters)")
    return [f"Not shown: {', '.join(thin)}."] if thin else []


def chart_comparison_cagr(filename):
    """CAGR by exchange, each against its own index."""
    keys = sorted(comparison_legs(), key=lambda k: data[k]["portfolio"]["cagr"], reverse=True)
    verdicts = [own_index_verdict(data, k, NOT_CLEAR_WIN) for k in keys]
    c = own_index_counts(verdicts)
    in_band = [f"{market_name(k, short=True)}'s {leg_excess(data[k]):+.2f} over the "
               f"{benchmark_label(data, k)}" for k, v in zip(keys, verdicts) if v == "dividend"]
    beat = f"All {c['beat']}" if c["beat"] == c["ranked"] else f"{c['beat']} of the {c['ranked']}"
    save_own_index_chart(
        data, keys, [market_name(k) for k in keys], charts_dir / filename,
        "Sector Momentum Rotation: CAGR vs Own Index by Exchange (2000-2025)",
        [DATA_LINE,
         f"Each market in its own currency against its own index. {beat} markets beat it as "
         "measured.",
         f"{c['clear']} keep a clear lead after the dividends price indices leave out "
         f"({', '.join(in_band)} doesn't)."]
        + not_ranked_lines(data, keys, verdicts) + left_out_lines(),
        not_clear_win=NOT_CLEAR_WIN)


def chart_comparison_drawdown(filename):
    """Max drawdown by exchange, each against its own index's."""
    keys = sorted(comparison_legs(), key=lambda k: data[k]["portfolio"]["max_drawdown"],
                  reverse=True)
    verdicts = [own_index_verdict(data, k, NOT_CLEAR_WIN) for k in keys]
    save_own_index_chart(
        data, keys, [market_name(k) for k in keys], charts_dir / filename,
        "Sector Momentum Rotation: Max Drawdown vs Own Index by Exchange (2000-2025)",
        [DATA_LINE, "Each market in its own currency; the tick is its own index's max drawdown."]
        + not_ranked_lines(data, keys, verdicts) + left_out_lines(),
        metric="max_drawdown", decimals=1)


# --- Generate charts ---

print("Generating Sector Momentum charts...\n")

for ex_key in EXCHANGE_LABELS:
    if ex_key not in data or not data[ex_key].get("annual_returns"):
        continue
    label = EXCHANGE_LABELS[ex_key].replace("Sector Momentum ", "").lower()
    universe_label = EXCHANGE_UNIVERSE_LABELS.get(ex_key, ex_key)

    print(f"{label.capitalize()} charts...")
    chart_cumulative(ex_key, f"1_{label.replace(' ', '_')}_cumulative_growth.png", universe_label)
    chart_annual_bars(ex_key, f"2_{label.replace(' ', '_')}_annual_returns.png", universe_label)

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("\nNext step: python3 scripts/copy_charts_to_content.py --apply")
print("(content dir: ts-content-creator/content/_ready/sector-07-momentum/blogs/{region}/)")
