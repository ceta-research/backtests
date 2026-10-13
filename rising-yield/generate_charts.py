"""Generate all Rising Dividend Yield charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
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
    "TSX": "#7f8c8d",
    "JPX": "#c0392b",
    "LSE": "#8e44ad",
    "ASX": "#2980b9",
    "SAO": "#f39c12",
    "STO": "#16a085",
    "JNB": "#d35400",
    "TAI": "#2c3e50",
    "SIX": "#e74c3c",
    "HKSE": "#9b59b6",
    "SHZ_SHH": "#e74c3c",
    "KSC": "#95a5a6",
    "SES": "#bdc3c7",
    "SET": "#7f8c8d",
    "SPY": "#aab7b8",
    "CASH": "#d5d8dc",
    "THIN": "#c0392b",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Rising Yield US",
    "NSE": "Rising Yield India (NSE)",
    "XETRA": "Rising Yield Germany (XETRA)",
    "TSX": "Rising Yield Canada (TSX)",
    "JPX": "Rising Yield Japan (JPX)",
    "LSE": "Rising Yield UK (LSE)",
    "ASX": "Rising Yield Australia (ASX)",
    "SAO": "Rising Yield Brazil (SAO)",
    "STO": "Rising Yield Sweden (STO)",
    "JNB": "Rising Yield South Africa (JNB)",
    "TAI": "Rising Yield Taiwan (TAI)",
    "SIX": "Rising Yield Switzerland (SIX)",
    "HKSE": "Rising Yield Hong Kong (HKSE)",
    "SHZ_SHH": "Rising Yield China (SHZ+SHH)",
    "KSC": "Rising Yield Korea (KSC)",
    "SES": "Rising Yield Singapore (SES)",
    "SET": "Rising Yield Thailand (SET)",
}

FOOTER = "Data: Ceta Research | Annual rebalance (July), equal weight top 30, 2000-2025"

MIN_STOCKS = 10  # backtest.py's floor: fewer qualifying stocks buyable at entry means cash for the year
NOT_COMPARED = ("ASX", "SAO")  # Australia + Brazil: fatal data quality, off both comparison charts


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
    if ex.get("period_data"):
        cash = {int(p["rebalance_date"][:4]) for p in ex["period_data"] if p.get("stocks_held") == 0}
        return [ar["year"] for ar in ex["annual_returns"] if ar["year"] in cash]
    # files from before the rerun carry no period_data; a cash year is an exact 0.0 there
    return [ar["year"] for ar in ex["annual_returns"] if ar["portfolio"] == 0]


def stamp_thin(ax, key, y=0.5):
    """Stamp a mostly-cash market's chart so it cannot pass as a normal result."""
    if not too_thin(key):
        return
    invested, total = periods(key)
    why = "never invested" if invested <= 0 else "mostly cash"
    print(f"  {key} is too thin to test ({why}, in cash {total - invested} of {total} years): chart stamped")
    ax.text(0.5, y, f"TOO THIN TO TEST\n{why}: in cash {total - invested} of {total} years",
            transform=ax.transAxes, ha="center", va="center", fontsize=22, fontweight="bold",
            color=COLORS["THIN"], zorder=5,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor=COLORS["THIN"], alpha=0.85))


def cash_footer(fig, y, key, mark):
    """Second footer line saying what `mark` means, when the book sat in cash at all."""
    invested, total = periods(key)
    if total > invested:
        fig.text(0.5, y,
                 f"{mark}: fewer than {MIN_STOCKS} qualifying stocks buyable at entry, so the portfolio held"
                 f" cash (0%) in {total - invested} of {total} years.",
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
    missing = [k for k in exchanges if not drawable(k)]
    if missing:
        skip_chart(filename, f"no usable {', '.join(missing)} node in exchange_comparison.json")
        return
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

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")
    cash_footer(fig, -0.035, exchanges[0], "Shaded")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    missing = [k for k in exchanges if not drawable(k)]
    if missing:
        skip_chart(filename, f"no usable {', '.join(missing)} node in exchange_comparison.json")
        return
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
    stamp_thin(ax, exchanges[0], y=0.78)  # clear of the "cash" marks on the zero line

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")
    cash_footer(fig, -0.078, exchanges[0], '"cash"')

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Annualised rise of USD/local (% a year) over the backtest window, 2000-07-03 to
# 2025-07-01, from the warehouse forex series. HKD is pegged: no adjustment, as the post does.
FX_RISE_PCT = {
    "NYSE_NASDAQ_AMEX": 0.0, "NSE": 2.64, "XETRA": -0.86, "LSE": 0.39, "STO": 0.29,
    "TSX": -0.33, "JPX": 1.23, "HKSE": 0.0, "TAI": -0.19, "SHZ_SHH": -0.57,
    "KSC": 0.79, "SIX": -2.87, "JNB": 3.87, "SES": -1.23, "SET": -0.77,
}


def usd_cagr(exchange_key, local_cagr):
    """Rough USD CAGR = (1 + local) / (1 + USD/local rise) - 1. KeyError if no FX entry."""
    return ((1 + local_cagr / 100) / (1 + FX_RISE_PCT[exchange_key] / 100) - 1) * 100


def comparison_legs(metric):
    """Markets fit to rank on a portfolio metric, plus a footer note naming the ones left out."""
    ranked, thin, other = [], [], []
    known = [k for k in EXCHANGE_LABELS if k not in NOT_COMPARED]
    for k in [k for k in data if k in known] + [k for k in known if k not in data]:
        v = data.get(k) or {}
        name = EXCHANGE_LABELS[k].replace("Rising Yield ", "", 1)
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
    for n in notes:
        print(f"  {n}")
    if any(v.get("cash_periods") for _, v in ranked):
        notes.append(f"\"yrs cash\": years with fewer than {MIN_STOCKS} qualifying stocks buyable at entry."
                     " A cash year earns 0% and counts in the CAGR and drawdown.")
    return ranked, "\n".join(textwrap.fill(n, 150) for n in notes)


def leg_label(name, v):
    """Bar label for a ranked market, with its cash years when it had any."""
    n = v.get("cash_periods", 0)
    return f"{name}, {n} yr{'' if n == 1 else 's'} cash" if n else name


def us_spy(metric):
    """The US node's S&P 500 figure for the reference line, or None if the US market is absent."""
    return ((data.get("NYSE_NASDAQ_AMEX") or {}).get("spy") or {}).get(metric)


def chart_comparison_cagr(filename):
    """Local-currency CAGR bars with a rough USD estimate each, against the S&P 500 in USD.

    Bar colour is the USD verdict: a local bar crossing the USD line is not a beat.
    """
    exchanges_with_data, note = comparison_legs("cagr")
    spy_cagr = us_spy("cagr")
    if not exchanges_with_data or spy_cagr is None:
        skip_chart(filename, "no market fit to rank" if not exchanges_with_data
                   else "no NYSE_NASDAQ_AMEX node, so no S&P 500 line to judge against")
        return
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    keys = [k for k, v in exchanges_with_data]
    names = [EXCHANGE_LABELS.get(k, k).replace("Rising Yield ", "", 1) for k in keys]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    usds = [usd_cagr(k, c) for k, c in zip(keys, cagrs)]
    beats = [u > spy_cagr for u in usds]
    colors = ["#27ae60" if b else "#95a5a6" for b in beats]

    fig, ax = plt.subplots(figsize=(11, 8))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    fx_rows = [i for i, k in enumerate(keys) if k != "NYSE_NASDAQ_AMEX"]
    ax.scatter([usds[i] for i in fx_rows], fx_rows, marker="D", s=40, color="black", zorder=3)
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([leg_label(n, v) for n, (k, v) in zip(names, exchanges_with_data)], fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    left = min(cagrs + usds)
    if left < 0:  # a negative CAGR keeps its bar
        ax.axvline(x=0, color="black", linewidth=0.5)
    ax.set_xlim(left - 1 if left < 0 else 0, max(cagrs + usds + [spy_cagr]) + 6)
    ax.set_title("Rising Dividend Yield: CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(handles=[
        Patch(color="#27ae60", alpha=0.85, label="Clears S&P 500 in USD (est.)"),
        Patch(color="#95a5a6", alpha=0.85, label="Trails S&P 500 in USD (est.)"),
        Line2D([], [], color="black", marker="D", linestyle="none", markersize=6, label="Est. USD CAGR"),
        Line2D([], [], color="#e74c3c", linewidth=1.5, linestyle="--", label=f"S&P 500 ({spy_cagr}%, USD)"),
    ], fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (k, c, u) in enumerate(zip(keys, cagrs, usds)):
        label = f"{c:.1f}%" if k == "NYSE_NASDAQ_AMEX" else f"{c:.1f}%  (USD ~{u:.1f}%)"
        ax.text(max(c, u, 0) + 0.45, i, label, va="center", fontsize=9.5, fontweight="bold",
                bbox=dict(facecolor="white", edgecolor="none", pad=1, alpha=0.85))

    winners = sorted((u, n.split(" (")[0]) for u, n, b in zip(usds, names, beats) if b)[::-1]
    cleared = (f"{len(winners)} of {len(keys)} clear it: {', '.join(n for u, n in winners)}." if winners
               else f"none of {len(keys)} clear it.")
    fig.text(0.5, -0.05,
             f"{FOOTER}\n"
             "Bars: CAGR in local currency. Diamonds: rough USD CAGR at each currency's move vs the dollar, "
             "July 2000 to July 2025.\n"
             f"Dashed line: S&P 500 total return (SPY) in USD. In USD, {cleared}",
             ha="center", fontsize=8, color="#7f8c8d")
    if note:
        fig.text(0.5, -0.06, note, ha="center", va="top", fontsize=8, color="#7f8c8d")

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

    names = [leg_label(EXCHANGE_LABELS.get(k, k), v) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, 8))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = us_spy("max_drawdown")
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")
        ax.legend(fontsize=10, loc="lower left")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Rising Dividend Yield: Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")
    if note:
        fig.text(0.5, -0.03, note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

# Each key is one --exchange leg (TSX and TAI alone, not the canada / taiwan presets), and the labels say so.
for key, ex in data.items():
    if key not in EXCHANGE_LABELS:
        print(f"WARNING: {key} is not one of this topic's markets (a stale results file?); not charted")
    if key not in EXCHANGE_LABELS or not drawable(key):
        continue
    if ex.get("universe", key) != key:
        print(f"WARNING: {key} universe is {ex['universe']!r}, not the {key} its chart labels say")
    if ex.get("window_truncated"):
        print(f"WARNING: {key} measured only {ex.get('window_label')}; its chart titles say 2000-2025")
    if len(cash_years(key)) != ex.get("cash_periods", 0):
        print(f"WARNING: {key} counts {ex.get('cash_periods', 0)} cash periods"
              f" but {len(cash_years(key))} cash years are marked on its charts")

print("Generating charts for US blog...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Rising Yield US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, annual rebalance (July), equal weight top 30"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "Rising Yield US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, annual rebalance (July)"
)

print("Generating charts for India blog...")
chart_cumulative(
    ["NSE"], "1_india_cumulative_growth.png",
    f"Growth of $10,000: Rising Yield India vs {benchmark_legend(data, 'NSE')} (2000-2025)",
    "NSE (returns and benchmark in INR)"
)
chart_annual_bars(
    ["NSE"], "2_india_annual_returns.png",
    f"Rising Yield India vs {benchmark_legend(data, 'NSE')}: Year-by-Year Returns (2000-2024)",
    "NSE (returns in INR)"
)

print("Generating charts for Germany blog...")
chart_cumulative(
    ["XETRA"], "1_germany_cumulative_growth.png",
    f"Growth of $10,000: Rising Yield Germany vs {benchmark_legend(data, 'XETRA')} (2000-2025)",
    "XETRA (returns and benchmark in EUR)"
)
chart_annual_bars(
    ["XETRA"], "2_germany_annual_returns.png",
    f"Rising Yield Germany vs {benchmark_legend(data, 'XETRA')}: Year-by-Year Returns (2000-2024)",
    "XETRA (returns in EUR)"
)

print("Generating charts for Canada blog...")
chart_cumulative(
    ["TSX"], "1_canada_cumulative_growth.png",
    f"Growth of $10,000: Rising Yield Canada vs {benchmark_legend(data, 'TSX')} (2000-2025)",
    "TSX (returns and benchmark in CAD)"
)
chart_annual_bars(
    ["TSX"], "2_canada_annual_returns.png",
    f"Rising Yield Canada vs {benchmark_legend(data, 'TSX')}: Year-by-Year Returns (2000-2024)",
    "TSX (returns in CAD)"
)

print("Generating charts for Japan blog...")
chart_cumulative(
    ["JPX"], "1_japan_cumulative_growth.png",
    f"Growth of $10,000: Rising Yield Japan vs {benchmark_legend(data, 'JPX')} (2000-2025)",
    "JPX (returns and benchmark in JPY)"
)
chart_annual_bars(
    ["JPX"], "2_japan_annual_returns.png",
    f"Rising Yield Japan vs {benchmark_legend(data, 'JPX')}: Year-by-Year Returns (2000-2024)",
    "JPX (returns in JPY)"
)

print("Generating charts for UK blog...")
chart_cumulative(
    ["LSE"], "1_uk_cumulative_growth.png",
    f"Growth of $10,000: Rising Yield UK vs {benchmark_legend(data, 'LSE')} (2000-2025)",
    "LSE (returns and benchmark in GBP)"
)
chart_annual_bars(
    ["LSE"], "2_uk_annual_returns.png",
    f"Rising Yield UK vs {benchmark_legend(data, 'LSE')}: Year-by-Year Returns (2000-2024)",
    "LSE (returns in GBP)"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
