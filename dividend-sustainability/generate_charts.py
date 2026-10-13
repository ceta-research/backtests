"""Generate all Dividend Sustainability charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
import textwrap
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_legend, benchmark_money, localize_money_title,
                         money, money_axis_label, money_formatter)

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
    "LSE": "#2980b9",
    "STO": "#8e44ad",
    "TSX": "#7f8c8d",
    "ASX": "#d35400",
    "JPX": "#c0392b",
    "TAI": "#16a085",
    "HKSE": "#9b59b6",
    "SIX": "#f39c12",
    "SHZ_SHH": "#e74c3c",
    "KSC": "#95a5a6",
    "JNB": "#2c3e50",
    "SPY": "#aab7b8",
    "CASH": "#d5d8dc",
    "THIN": "#c0392b",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Sustainability US",
    "NSE": "Sustainability India (NSE)",
    "XETRA": "Sustainability Germany (XETRA)",
    "LSE": "Sustainability UK (LSE)",
    "STO": "Sustainability Sweden (STO)",
    "TSX": "Sustainability Canada (TSX+TSXV)",
    "ASX": "Sustainability Australia (ASX)",
    "JPX": "Sustainability Japan (JPX)",
    "TAI": "Sustainability Taiwan (TAI+TWO)",
    "HKSE": "Sustainability Hong Kong (HKSE)",
    "SIX": "Sustainability Switzerland (SIX)",
    "SHZ_SHH": "Sustainability China (SHZ+SHH)",
    "KSC": "Sustainability Korea (KSC)",
    "JNB": "Sustainability South Africa (JNB)",
    "PAR": "Sustainability France (PAR)",
}

# Exchanges with dedicated blogs
DEDICATED = [
    "NYSE_NASDAQ_AMEX", "NSE", "XETRA", "TSX", "LSE", "STO",
    "SIX", "JPX", "HKSE", "SHZ_SHH", "ASX", "TAI",
]

MIN_STOCKS = 10  # backtest.py's floor: fewer names buyable at entry means cash for the year


def drawable(key):
    """True if the comparison file holds a usable node for `key`: present, no error, with returns."""
    ex = data.get(key) or {}
    return bool(ex.get("portfolio") and ex.get("spy") and ex.get("annual_returns"))


def periods(key):
    """(invested, total) rebalances; cash + invested is the total in old and new files alike."""
    ex = data.get(key) or {}
    invested = ex.get("invested_periods", 0)
    return invested, invested + ex.get("cash_periods", 0)


def too_thin(key):
    """Invested in fewer than half its periods: too thin to test, must never read as a normal result."""
    invested, total = periods(key)
    return invested * 2 < total


def cash_years(key):
    """Start years of the periods the book sat in cash (0% that year, still compounded)."""
    ex = data[key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    if ex.get("period_data"):
        cash = {int(p["rebalance_date"][:4]) for p in ex["period_data"] if p.get("stocks_held") == 0}
        return [y for y in years if y in cash]
    # files from before the rerun carry no period_data; a cash year is an exact 0.0 there
    return [ar["year"] for ar in ex["annual_returns"] if ar["portfolio"] == 0]


def stamp_thin(ax, key):
    """Stamp a mostly-cash market's chart so it cannot pass as a normal result."""
    if not too_thin(key):
        return
    invested, total = periods(key)
    ax.text(0.5, 0.5, f"TOO THIN TO TEST\nin cash {total - invested} of {total} years",
            transform=ax.transAxes, ha="center", va="center", fontsize=22, fontweight="bold",
            color=COLORS["THIN"], zorder=5,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor=COLORS["THIN"], alpha=0.85))


def cash_footer(fig, y, key, mark):
    """Second footer line saying what `mark` means, when the book sat in cash at all."""
    invested, total = periods(key)
    if total > invested:
        fig.text(0.5, y,
                 f"{mark}: fewer than {MIN_STOCKS} names buyable at entry, so the portfolio held cash (0%)"
                 f" in {total - invested} of {total} years.",
                 ha="center", va="top", fontsize=8, color="#7f8c8d")


def skip_chart(filename, why):
    """Not drawable: say so, and delete any earlier PNG so a stale one cannot pass as current."""
    out = charts_dir / filename
    stale = out.exists()
    if stale:
        out.unlink()
    print(f"  Skipped: {filename} ({why}); "
          + ("deleted the earlier file" if stale else "no earlier file to delete"))


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    # "year" is the July a holding period starts: start there, each value lands at its period's end
    years = [ex["annual_returns"][0]["year"]]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"] + 1)
    return years, values


def get_spy_cumulative(ref_key, initial=10000):
    """Cumulative growth of THAT exchange's own benchmark series.

    The "spy" field holds whichever index the exchange was measured against,
    which for non-US markets is the local index.
    """
    return benchmark_cumulative(data, ref_key, initial, period_end=True)


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative(exchanges[0])
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_legend(data, exchanges[0])} ({data[exchanges[0]]['spy']['cagr']}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        invested, total = periods(ex_key)
        cash = f", cash {total - invested} of {total} yrs" if total > invested else ""
        label = f"{EXCHANGE_LABELS[ex_key]} ({cagr}% CAGR{cash})"
        ax.plot(years, vals, color=COLORS[ex_key], linewidth=2.2, label=label)
        # Shade each cash period (its start year to the next) so the flat stretch does not read as invested.
        for i, y in enumerate(cash_years(ex_key)):
            ax.axvspan(y, y + 1, color=COLORS["CASH"], alpha=0.6, linewidth=0,
                       label="In cash" if i == 0 else None)

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS[ex_key])

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(benchmark_money(spy_final_k, data, exchanges[0], suffix="K"),
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
    stamp_thin(ax, exchanges[0])

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, annual rebalance (July), equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")
    cash_footer(fig, -0.035, exchanges[0], "Shaded")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    ex = data[exchanges[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=benchmark_legend(data, exchanges[0]), color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(exchanges):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS[ex_key], color=COLORS[ex_key], alpha=0.85)
        # A cash year has no bar: label it so it does not read as a 0% invested year.
        cash = set(cash_years(ex_key))
        for o, year in zip(offsets, years):
            if year in cash:
                ax.text(o + (idx + 1) * width, 0, "cash", rotation=90, ha="center", va="bottom",
                        fontsize=7, color=COLORS[ex_key])

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)
    stamp_thin(ax, exchanges[0])

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance (July), equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")
    cash_footer(fig, -0.078, exchanges[0], '"cash"')

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def comparison_legs(metric):
    """Markets fit to rank on a portfolio metric, plus a footer note naming the ones left out."""
    ranked, thin, other = [], [], []
    for k, v in data.items():
        if k not in EXCHANGE_LABELS:  # a stale or renamed results file, not a 16th market
            continue
        name = EXCHANGE_LABELS[k].replace("Sustainability ", "")
        invested, total = periods(k)
        if not drawable(k) or v["portfolio"].get(metric) is None or v.get("window_truncated", False):
            other.append(name)
        elif invested <= 0 or too_thin(k):
            thin.append(f"{name} {total - invested} of {total}")
        else:
            ranked.append((k, v))
    notes = []
    if thin:
        notes.append(f"Not ranked, too thin to test (years in cash): {', '.join(thin)}.")
    if other:
        notes.append(f"Not ranked (truncated window or no result): {', '.join(other)}.")
    if any(v.get("cash_periods") for _, v in ranked):
        notes.append(f"\"yrs cash\": years with fewer than {MIN_STOCKS} names buyable at entry."
                     " A cash year earns 0% and counts in the CAGR and drawdown.")
    return ranked, "\n".join(textwrap.fill(n, 150) for n in notes)


def leg_label(k, v):
    """Bar label for a ranked market, with its cash years when it had any."""
    n = v.get("cash_periods", 0)
    label = EXCHANGE_LABELS.get(k, k)
    return f"{label}, {n} yr{'' if n == 1 else 's'} cash" if n else label


def us_spy(metric):
    """The US node's S&P 500 figure for the reference line, or None if the US market is absent."""
    return ((data.get("NYSE_NASDAQ_AMEX") or {}).get("spy") or {}).get(metric)


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data, note = comparison_legs("cagr")
    if not exchanges_with_data:
        skip_chart(filename, "no market fit to rank")
        return
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [leg_label(k, v) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = us_spy("cagr")
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500, USD ({spy_cagr}% CAGR)")
        ax.legend(fontsize=10)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_title("Dividend Sustainability CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.2f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Sustainability screen, annual rebalance (July), equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")
    if note:
        fig.text(0.5, -0.035, note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data, note = comparison_legs("max_drawdown")
    if not exchanges_with_data:
        skip_chart(filename, "no market fit to rank")
        return
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [leg_label(k, v) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = us_spy("max_drawdown")
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500, USD ({spy_dd:.1f}%)")
        ax.legend(fontsize=10, loc="lower left")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Dividend Sustainability Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Sustainability screen, annual rebalance (July), equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")
    if note:
        fig.text(0.5, -0.035, note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

# Per-exchange: cumulative growth + annual returns
per_exchange_charts = {
    "us": (["NYSE_NASDAQ_AMEX"], "NYSE + NASDAQ + AMEX"),
    "india": (["NSE"], "NSE (returns in INR)"),
    "germany": (["XETRA"], "XETRA (returns in EUR)"),
    "uk": (["LSE"], "LSE (returns in GBP)"),
    "sweden": (["STO"], "STO (returns in SEK)"),
    "canada": (["TSX"], "TSX + TSXV (returns in CAD)"),
    "switzerland": (["SIX"], "SIX (returns in CHF)"),
    "japan": (["JPX"], "JPX (returns in JPY)"),
    "hongkong": (["HKSE"], "HKSE (returns in HKD)"),
    "china": (["SHZ_SHH"], "SHZ + SHH (returns in CNY)"),
    "australia": (["ASX"], "ASX (returns in AUD)"),
    "taiwan": (["TAI"], "TAI + TWO (returns in TWD)"),
}
REGION_NAMES = {"us": "US", "uk": "UK", "hongkong": "Hong Kong"}  # the rest read right from .title()

# The TSX and TAI labels describe the --preset canada / taiwan universes; --global runs TSX / TAI alone.
for key, preset in (("TSX", "Canada"), ("TAI", "Taiwan")):
    if drawable(key) and data[key].get("universe") != preset:
        print(f"WARNING: {key} universe is {data[key].get('universe')!r}, not the {preset!r} preset"
              " its chart labels describe")
for key in data:
    if key not in EXCHANGE_LABELS:
        print(f"WARNING: {key} is not one of this topic's markets (a stale results file?); not charted")

for region, (exchanges, universe) in per_exchange_charts.items():
    print(f"Generating charts for {region}...")
    missing = [k for k in exchanges if not drawable(k)]
    if missing:
        for filename in (f"1_{region}_cumulative_growth.png", f"2_{region}_annual_returns.png"):
            skip_chart(filename, f"no usable {', '.join(missing)} node in exchange_comparison.json")
        continue
    if too_thin(exchanges[0]):
        print("  Too thin to test (in cash more than half its years): both charts carry the stamp")
    if data[exchanges[0]].get("window_truncated"):
        print(f"  WARNING: {exchanges[0]} measured only {data[exchanges[0]].get('window_label')};"
              " its charts are labelled 2000-2025")
    name = REGION_NAMES.get(region, region.title())
    bench = benchmark_legend(data, exchanges[0])
    chart_cumulative(
        exchanges, f"1_{region}_cumulative_growth.png",
        f"Growth of $10,000: Dividend Sustainability {name} vs {bench} (2000-2025)",
        universe
    )
    chart_annual_bars(
        exchanges, f"2_{region}_annual_returns.png",
        f"Dividend Sustainability {name} vs {bench}: Annual Returns (2000-2024)",
        universe
    )

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
