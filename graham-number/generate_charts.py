"""Generate all Graham Number charts for blog posts from exchange_comparison.json.

Cash. A year with fewer than MIN_STOCKS names buyable at entry is held in cash (0%), and
the per-market charts mark those years. A market invested in fewer than half its years is
mostly cash, too thin to test: its charts carry a stamp and the two comparison charts name
it in the footer instead of ranking it. A market that never invested, has a truncated
benchmark window or is missing gets no per-market chart, and any earlier PNG of it is deleted.
"""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
import json
import textwrap
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_label, benchmark_legend, localize_money_title, money,
                         money_axis_label, money_formatter)

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "US_MAJOR":    "#1a5276",
    "Japan":       "#c0392b",
    "Sweden":      "#0d6efd",
    "Canada":      "#c75000",
    "Brazil":      "#157347",
    "Germany":     "#27ae60",
    "Switzerland": "#8e44ad",
    "India":       "#e67e22",
    "Taiwan":      "#2980b9",
    "Korea":       "#7f8c8d",
    "Norway":      "#16a085",
    "Australia":   "#bdc3c7",
    "UK":          "#34495e",
    "HKSE":        "#95a5a6",
    "SPY":         "#aab7b8",
    "CASH":        "#d5d8dc",
    "THIN":        "#c0392b",
}
C_BEAT, C_TRAIL, C_PROXY = "#27ae60", "#c0392b", "#9E9E9E"  # vs own benchmark; grey = S&P 500 (USD) proxy

EXCHANGE_LABELS = {
    "US_MAJOR":    "Graham Number US (NYSE+NASDAQ+AMEX)",
    "Japan":       "Graham Number Japan (JPX)",
    "Sweden":      "Graham Number Sweden (STO)",
    "Canada":      "Graham Number Canada (TSX+TSXV)",
    "Brazil":      "Graham Number Brazil (SAO)",
    "Germany":     "Graham Number Germany (XETRA)",
    "Switzerland": "Graham Number Switzerland (SIX)",
    "India":       "Graham Number India (NSE)",
    "Taiwan":      "Graham Number Taiwan (TAI+TWO)",
    "Korea":       "Graham Number Korea (KSC)",
    "Norway":      "Graham Number Norway (OSL)",
    "Australia":   "Graham Number Australia (ASX)",
    "UK":          "Graham Number UK (LSE)",
    "HKSE":        "Graham Number HK (HKSE)",
    "France":      "Graham Number France (PAR)",
    "SouthAfrica": "Graham Number South Africa (JNB)",
    "Singapore":   "Graham Number Singapore (SES)",
}

FOOTER = (
    "Data: Ceta Research | Graham Number (price < sqrt(22.5 x EPS x BVPS)), "
    "annual rebalance, equal weight, 2000-2025"
)

MIN_STOCKS = 10  # backtest.py's floor: fewer names buyable at entry means cash for the year


def drawable(key):
    """True if the comparison file holds a usable node for `key`: present, no error, with returns."""
    ex = data.get(key)
    return isinstance(ex, dict) and bool(ex.get("portfolio") and ex.get("spy") and ex.get("annual_returns"))


def periods(key):
    """(invested, total) rebalances; cash + invested is the total in old and new files alike."""
    ex = data.get(key) or {}
    invested = ex.get("invested_periods") or 0
    return invested, invested + (ex.get("cash_periods") or 0)


def too_thin(key):
    """Invested in fewer than half its periods: mostly cash, must never read as a normal result."""
    invested, total = periods(key)
    return invested * 2 < total


def skip_reason(key):
    """Why a market gets no per-market chart, or None when it is drawn."""
    ex = data.get(key)
    if not isinstance(ex, dict):
        return "not in exchange_comparison.json"
    if not drawable(key):
        return f"run errored ({ex['error']})" if "error" in ex else "no measured periods"
    if ex.get("window_truncated", False):
        return "benchmark window truncated"
    if periods(key)[0] <= 0:
        return "all cash, never invested: too thin to test"
    return None


def cash_years(key):
    """Start years of the periods the book sat in cash (0% that year, still compounded)."""
    ex = data[key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    if ex.get("period_data"):
        cash = {int(p["rebalance_date"][:4]) for p in ex["period_data"] if p.get("stocks_held") == 0}
        return [y for y in years if y in cash]
    # files from before the rerun carry no period_data; a cash year is an exact 0.0 there
    return [ar["year"] for ar in ex["annual_returns"] if ar["portfolio"] == 0]


def stamp_thin(ax, key, y=0.5):
    """Stamp a mostly-cash market's chart so it cannot pass as a normal result."""
    if not too_thin(key):
        return
    invested, total = periods(key)
    ax.text(0.5, y, f"MOSTLY CASH: TOO THIN TO TEST\nin cash {total - invested} of {total} years",
            transform=ax.transAxes, ha="center", va="center", fontsize=18, fontweight="bold",
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
    # "year" is the January a holding period starts: start there, each value lands at its period's end
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
    why = skip_reason(exchanges[0])  # the benchmark line and the currency are exchanges[0]'s
    if why:
        skip_chart(filename, f"{exchanges[0]}: {why}")
        return
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative(exchanges[0])
    if spy_years:
        # Per-exchange, not US_MAJOR: the series and label are per-exchange
        spy_cagr = data.get(exchanges[0], {}).get("spy", {}).get("cagr", "?")
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"{benchmark_label(data, exchanges[0])} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if skip_reason(ex_key):
            continue
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        invested, total = periods(ex_key)
        cash = f", cash {total - invested} of {total} yrs" if total > invested else ""
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{cash})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        # Shade each cash period (its start year to the next) so the flat stretch does not read as invested.
        for i, y in enumerate(cash_years(ex_key)):
            ax.axvspan(y, y + 1, color=COLORS["CASH"], alpha=0.6, linewidth=0,
                       label="In cash (0%)" if i == 0 else None)

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    if spy_years:
        spy_final_k = spy_vals[-1] / 1000
        ax.annotate(money(spy_final_k, exchanges[0], suffix="K"),
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

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center",
             fontsize=8, color="#7f8c8d")
    cash_footer(fig, -0.035, exchanges[0], "Shaded")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    active = [e for e in exchanges if not skip_reason(e)]
    if exchanges[0] not in active:  # the benchmark bars are exchanges[0]'s
        skip_chart(filename, f"{exchanges[0]}: {skip_reason(exchanges[0])}")
        return

    ex = data[active[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(active) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))
    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=benchmark_label(data, exchanges[0]), color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(active):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        # A cash year has no bar: label it so it does not read as a 0% invested year.
        cash = set(cash_years(ex_key))
        for o, year in zip(offsets, years):
            if year in cash:
                ax.text(o + (idx + 1) * width, 0, "cash", rotation=90, ha="center", va="bottom",
                        fontsize=7, color=COLORS.get(ex_key, "#95a5a6"))

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)
    stamp_thin(ax, exchanges[0], y=0.82)  # clear of the "cash" labels on the zero line

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}", ha="center",
             fontsize=8, color="#7f8c8d")
    cash_footer(fig, -0.078, exchanges[0], '"cash"')

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def short_name(key):
    """Market name without the strategy prefix, for the comparison charts."""
    return EXCHANGE_LABELS.get(key, key).replace("Graham Number ", "")


def bench_name(key):
    """Name of the index a market's "spy" fields hold: the local index, or the S&P 500 where there is none."""
    name = benchmark_legend(data, key)
    # France and SouthAfrica have no local index and no currency chart_utils can resolve from the key
    return "S&P 500, USD" if name == "S&P 500" and key != "US_MAJOR" else name


def excess_cagr(val):
    """Excess CAGR over the market's own benchmark."""
    excess = (val.get("comparison") or {}).get("excess_cagr")
    return excess if excess is not None else val["portfolio"]["cagr"] - val["spy"]["cagr"]


def comparison_legs():
    """(key, results) of the markets fit to rank, plus a footer note naming the ones left out."""
    ranked, thin, other = [], [], []
    for k, v in data.items():
        invested, total = periods(k)
        if (not drawable(k) or v.get("window_truncated", False)
                or v["portfolio"].get("cagr") is None or v["spy"].get("cagr") is None):
            other.append(short_name(k))
        elif too_thin(k):  # never invested included
            thin.append(f"{short_name(k)} {total - invested} of {total}")
        else:
            ranked.append((k, v))
    notes = []
    if thin:
        notes.append(f"Not ranked, mostly cash and too thin to test (years in cash): {', '.join(thin)}.")
    if other:
        notes.append(f"Not ranked (truncated benchmark window or no result): {', '.join(other)}.")
    for n in [f"Ranked ({len(ranked)}): {', '.join(k for k, _ in ranked) or 'none'}"] + notes:
        print(f"  {n}")
    if any(periods(k)[1] > periods(k)[0] for k, _ in ranked):
        notes.append(f"\"yrs cash\": years with fewer than {MIN_STOCKS} names buyable at entry."
                     " A cash year earns 0% and counts in the CAGR.")
    return ranked, "\n".join(textwrap.fill(n, 150) for n in notes)


def leg_label(key):
    """Row label for a ranked market, with its cash years when it had any."""
    invested, total = periods(key)
    n = total - invested
    return f"{short_name(key)}, {n} yr{'' if n == 1 else 's'} cash" if n else short_name(key)


def chart_comparison_cagr(filename, legs, note):
    """Horizontal bar chart: CAGR by exchange (markets fit to rank only)."""
    if not legs:
        skip_chart(filename, "no market fit to rank")
        return
    exchanges_with_data = sorted(legs, key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [leg_label(k) for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, _ in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.7)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    # The one cross-market reference: the S&P 500 as the US entry measured it, in USD
    spy_cagr = ((data.get("US_MAJOR") or {}).get("spy") or {}).get("cagr")
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500, USD ({spy_cagr}% CAGR)")
        ax.legend(fontsize=10)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_xlim(right=max(cagrs + [spy_cagr or 0]) + 1.5)  # room for the top bar's label
    ax.set_title("Graham Number CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")
    if note:
        fig.text(0.5, -0.035, note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_excess(filename, legs, note):
    """Horizontal bar chart: excess CAGR by exchange, each over its own benchmark."""
    if not legs:
        skip_chart(filename, "no market fit to rank")
        return
    exchanges_with_data = sorted(legs, key=lambda x: excess_cagr(x[1]), reverse=True)

    names = [leg_label(k) for k, _ in exchanges_with_data]
    excesses = [excess_cagr(v) for _, v in exchanges_with_data]
    benches = [bench_name(k) for k, _ in exchanges_with_data]
    proxy = [b == "S&P 500, USD" for b in benches]
    colors = [C_PROXY if p else C_BEAT if e >= 0 else C_TRAIL for p, e in zip(proxy, excesses)]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.7)))
    ax.barh(range(len(names)), excesses, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=0, color="black", linewidth=1.2)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Excess CAGR vs own benchmark (%, local currency)", fontsize=12, fontweight="bold")
    span = max(excesses + [0]) - min(excesses + [0])
    ax.set_xlim(right=max(excesses + [0]) + 0.45 * span + 1)  # room for the labels right of the bars
    ax.set_title("Graham Number: Excess CAGR vs Own Benchmark by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    handles = [Patch(color=C_BEAT, alpha=0.85, label="Beat its own benchmark"),
               Patch(color=C_TRAIL, alpha=0.85, label="Trailed its own benchmark")]
    if any(proxy):
        handles.append(Patch(color=C_PROXY, alpha=0.85, label="No local index (vs S&P 500, USD)"))
    ax.legend(handles=handles, fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (e, bench) in enumerate(zip(excesses, benches)):
        ax.text(max(e, 0) + 0.3, i, f"{e:+.1f}% vs {bench}", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")
    if note:
        fig.text(0.5, -0.035, note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate per-exchange charts ----

# The Canada and Taiwan labels describe the --preset canada / taiwan universes (TSX+TSXV, TAI+TWO).
for key in ("Canada", "Taiwan"):
    if drawable(key) and data[key].get("universe") != key:
        print(f"WARNING: {key} universe is {data[key].get('universe')!r}, not the {key!r} preset"
              " its chart labels describe")

print("Generating charts for US...")
chart_cumulative(
    ["US_MAJOR"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Graham Number US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"
)
chart_annual_bars(
    ["US_MAJOR"], "2_us_annual_returns.png",
    "Graham Number US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"
)

print("Generating charts for Japan...")
chart_cumulative(
    ["Japan"], "1_japan_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Japan vs {benchmark_label(data, 'Japan')} (2000-2025)",
    "JPX (returns and benchmark in JPY)"
)
chart_annual_bars(
    ["Japan"], "2_japan_annual_returns.png",
    f"Graham Number Japan vs {benchmark_label(data, 'Japan')}: Year-by-Year Returns (2000-2024)",
    "JPX (returns in JPY)"
)

print("Generating charts for Sweden...")
chart_cumulative(
    ["Sweden"], "1_sweden_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Sweden vs {benchmark_label(data, 'Sweden')} (2000-2025)",
    "STO (returns and benchmark in SEK)"
)
chart_annual_bars(
    ["Sweden"], "2_sweden_annual_returns.png",
    f"Graham Number Sweden vs {benchmark_label(data, 'Sweden')}: Year-by-Year Returns (2000-2024)",
    "STO (returns in SEK)"
)

print("Generating charts for Canada...")
chart_cumulative(
    ["Canada"], "1_canada_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Canada vs {benchmark_label(data, 'Canada')} (2000-2025)",
    "TSX + TSXV (returns and benchmark in CAD)"
)
chart_annual_bars(
    ["Canada"], "2_canada_annual_returns.png",
    f"Graham Number Canada vs {benchmark_label(data, 'Canada')}: Year-by-Year Returns (2000-2024)",
    "TSX + TSXV (returns in CAD)"
)

print("Generating charts for Brazil...")
chart_cumulative(
    ["Brazil"], "1_brazil_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Brazil vs {benchmark_label(data, 'Brazil')} (2000-2025)",
    "SAO (returns and benchmark in BRL)"
)
chart_annual_bars(
    ["Brazil"], "2_brazil_annual_returns.png",
    f"Graham Number Brazil vs {benchmark_label(data, 'Brazil')}: Year-by-Year Returns (2000-2024)",
    "SAO (returns in BRL)"
)

print("Generating charts for Germany...")
chart_cumulative(
    ["Germany"], "1_germany_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Germany vs {benchmark_label(data, 'Germany')} (2000-2025)",
    "XETRA (returns and benchmark in EUR)"
)
chart_annual_bars(
    ["Germany"], "2_germany_annual_returns.png",
    f"Graham Number Germany vs {benchmark_label(data, 'Germany')}: Year-by-Year Returns (2000-2024)",
    "XETRA (returns in EUR)"
)

print("Generating charts for India...")
chart_cumulative(
    ["India"], "1_india_cumulative_growth.png",
    f"Growth of $10,000: Graham Number India vs {benchmark_label(data, 'India')} (2000-2025)",
    "NSE (returns and benchmark in INR)"
)
chart_annual_bars(
    ["India"], "2_india_annual_returns.png",
    f"Graham Number India vs {benchmark_label(data, 'India')}: Year-by-Year Returns (2000-2024)",
    "NSE (returns in INR)"
)

print("Generating charts for Taiwan...")
chart_cumulative(
    ["Taiwan"], "1_taiwan_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Taiwan vs {benchmark_label(data, 'Taiwan')} (2000-2025)",
    "TAI + TWO (returns and benchmark in TWD)"
)
chart_annual_bars(
    ["Taiwan"], "2_taiwan_annual_returns.png",
    f"Graham Number Taiwan vs {benchmark_label(data, 'Taiwan')}: Year-by-Year Returns (2000-2024)",
    "TAI + TWO (returns in TWD)"
)

print("Generating charts for Switzerland...")
chart_cumulative(
    ["Switzerland"], "1_switzerland_cumulative_growth.png",
    f"Growth of $10,000: Graham Number Switzerland vs {benchmark_label(data, 'Switzerland')} (2000-2025)",
    "SIX (returns and benchmark in CHF)"
)
chart_annual_bars(
    ["Switzerland"], "2_switzerland_annual_returns.png",
    f"Graham Number Switzerland vs {benchmark_label(data, 'Switzerland')}: Year-by-Year Returns (2000-2024)",
    "SIX (returns in CHF)"
)

print("Generating comparison charts...")
ranking = comparison_legs()
chart_comparison_cagr("1_comparison_cagr.png", *ranking)
chart_comparison_excess("2_comparison_excess.png", *ranking)

print(f"\nDone. Charts generated in {charts_dir}/")
