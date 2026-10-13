"""Generate all P/E Mean Reversion charts for blog posts from exchange_comparison.json.

Each entry's "spy" field holds whichever index that exchange was measured
against, which for every non-US market here is the local index (Sensex, FTSE
100, DAX, ...). Legends and titles therefore resolve the benchmark name per
exchange via chart_utils rather than hardcoding "S&P 500". The two comparison
charts are the exception: their single reference line is a genuine
cross-market S&P 500 read off the US entry.

Cash. A year with too few qualifying stocks is held in cash (0%), and the
per-market charts mark those years. A market invested in fewer than half its
years is mostly cash, too thin to test: its charts say so and the comparison
charts draw it hatched below the ranking. A market that never invested gets no
chart, and any old PNG of it is deleted.
"""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
import json
from pathlib import Path

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import benchmark_cumulative, benchmark_label, money, money_axis_label, money_formatter

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "JPX": "#c0392b",
    "LSE": "#8e44ad",
    "SHZ_SHH": "#e74c3c",
    "HKSE": "#a569bd",
    "TAI_TWO": "#7f8c8d",
    "SET": "#f39c12",
    "XETRA": "#16a085",
    "KSC": "#95a5a6",
    "TSX": "#117a65",
    "STO": "#2980b9",
    "SIX": "#2c3e50",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "P/E Mean Reversion US (NYSE+NASDAQ+AMEX)",
    "NSE": "P/E Mean Reversion India (NSE)",
    "JPX": "P/E Mean Reversion Japan (JPX)",
    "LSE": "P/E Mean Reversion UK (LSE)",
    "SHZ_SHH": "P/E Mean Reversion China (SHZ+SHH)",
    "HKSE": "P/E Mean Reversion Hong Kong (HKSE)",
    "TAI_TWO": "P/E Mean Reversion Taiwan (TAI+TWO)",
    "SET": "P/E Mean Reversion Thailand (SET)",
    "XETRA": "P/E Mean Reversion Germany (XETRA)",
    "KSC": "P/E Mean Reversion Korea (KSC)",
    "TSX": "P/E Mean Reversion Canada (TSX)",
    "STO": "P/E Mean Reversion Sweden (STO)",
    "SIX": "P/E Mean Reversion Switzerland (SIX)",
}

# Mostly-cash markets on the comparison charts: hatched grey, never a market colour
THIN_FILL, THIN_EDGE = "#d5d8dc", "#7f8c8d"


def cash_summary(exchange_key):
    """(cash years, invested periods, total periods, cash floor) for a market.

    A cash year had fewer qualifying stocks than the floor: 0%, still compounded.
    """
    ex = data[exchange_key]
    periods = ex.get("period_data") or []
    if periods:
        cash = {int(p["rebalance_date"][:4]) for p in periods if p.get("stocks_held") == 0}
    else:  # files from before the rerun carry no period_data; a cash year reads 0.0 there
        cash = {ar["year"] for ar in ex["annual_returns"] if ar["portfolio"] == 0}
    invested = ex.get("invested_periods", 0)
    total = ex.get("total_rebalances") or invested + ex.get("cash_periods", 0)
    floor = periods[0].get("min_stocks", 10) if periods else 10
    return cash, invested, total, floor


def mostly_cash(exchange_key):
    """Invested in fewer than half its periods: too thin to test, never shown as a normal result."""
    _, invested, total, _ = cash_summary(exchange_key)
    return invested * 2 < total


def set_title(ax, title, exchange_key):
    """Chart title; a mostly-cash market gets a red too-thin-to-test line under it."""
    _, invested, total, _ = cash_summary(exchange_key)
    thin = mostly_cash(exchange_key)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=34 if thin else 15)
    if thin:
        ax.text(0.5, 1.02, f"Mostly cash: invested in {invested} of {total} years. Too thin to test.",
                transform=ax.transAxes, ha="center", va="bottom", fontsize=11, fontweight="bold",
                color="#c0392b")


def remove_stale(filenames):
    """Delete charts this run did not redraw, so an old PNG cannot pass for a current one."""
    for name in filenames:
        out = charts_dir / name
        if out.exists():
            out.unlink()
            print(f"  Removed stale chart: {out}")


def get_cumulative_growth(exchange_key, initial=10000):
    # "year" is the year a holding period starts (January to January): the start value sits
    # there and each value at its period's end, as benchmark_cumulative(period_end=True) does
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"]]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"] + 1)
    return years, values


def cash_spans(exchange_key):
    """Runs of consecutive cash years as [start, end] on the cumulative chart's axis."""
    cash = cash_summary(exchange_key)[0]
    spans = []
    for ar in data[exchange_key]["annual_returns"]:
        if ar["year"] not in cash:
            continue
        if spans and spans[-1][1] == ar["year"]:
            spans[-1][1] = ar["year"] + 1
        else:
            spans.append([ar["year"], ar["year"] + 1])
    return spans


def chart_cumulative(exchanges, filename, title, footer_universe, ref_key=None, currency="$"):
    fig, ax = plt.subplots(figsize=(12, 6))

    if ref_key is None:
        ref_key = exchanges[0]
    spy_years, spy_vals = benchmark_cumulative(data, ref_key, period_end=True)
    spy_cagr = data[ref_key]["spy"]["cagr"]
    bench = benchmark_label(data, ref_key)
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{bench} ({spy_cagr}% CAGR)", linestyle="--")

    cash_note = ""
    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        _, invested, total, floor = cash_summary(ex_key)
        in_cash = f", cash {total - invested} of {total} yrs" if total > invested else ""
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{in_cash})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        # Cash years are shaded: a flat stretch must not read as a 0% invested year.
        for i, (start, end) in enumerate(cash_spans(ex_key)):
            ax.axvspan(start, end, color=COLORS.get(ex_key, "#95a5a6"), alpha=0.12, linewidth=0,
                       label="In cash (0%)" if i == 0 else None)
            cash_note = (f"\nShaded: years with fewer than {floor} qualifying stocks."
                         " The portfolio held cash (0%, flat line).")

        final_k = vals[-1] / 1000
        ax.annotate(f"{currency}{final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(f"{currency}{spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel(f"Portfolio Value ({currency.strip()})", fontsize=12, fontweight="bold")
    set_title(ax, title, exchanges[0])  # the caller's title already carries the currency
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{currency}{x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025"
             + cash_note,
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    ex = data[exchanges[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))
    offsets = [i - (n_series - 1) * width / 2 for i in x]

    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=benchmark_label(data, exchanges[0]), color=COLORS["SPY"], alpha=0.7)

    cash_note = ""
    for idx, ex_key in enumerate(exchanges):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        # Cash years have no bar: label them so they don't read as a 0% invested year.
        cash, _, _, floor = cash_summary(ex_key)
        cash_x = [o + (idx + 1) * width for o, y in zip(offsets, years) if y in cash]
        for cx in cash_x:
            ax.text(cx, 0, "cash", rotation=90, ha="center", va="bottom", fontsize=7,
                    color=COLORS.get(ex_key, "#95a5a6"))
        if cash_x:
            cash_note = (f"\n\"cash\" marks a year with fewer than {floor} qualifying stocks."
                         f" The portfolio held cash (0%) in {len(cash_x)} of {len(years)} years.")

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    set_title(ax, title, exchanges[0])
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, 2000-2025"
             + cash_note,
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def comparison_legs(metric):
    """(key, value, mostly cash) per market for a ranked chart, plus the markets left out.

    Left out: a truncated benchmark window or never invested. Mostly-cash markets
    come after the ranked ones in file order, unranked, and mark_thin() draws them.
    """
    legs, excluded = [], []
    for k, v in data.items():
        if not isinstance(v, dict) or (v.get("portfolio") or {}).get(metric) is None:
            continue
        if v.get("window_truncated", False) or v.get("invested_periods", 0) <= 0:
            excluded.append(EXCHANGE_LABELS.get(k, k).replace("P/E Mean Reversion ", ""))
            continue
        legs.append((k, v["portfolio"][metric], mostly_cash(k)))
    legs.sort(key=lambda x: (x[2], 0 if x[2] else -x[1]))
    return legs, excluded


def leg_label(exchange_key):
    """Row label with its cash count, so a part-cash bar does not read as fully invested."""
    name = EXCHANGE_LABELS.get(exchange_key, exchange_key)
    _, invested, total, _ = cash_summary(exchange_key)
    if mostly_cash(exchange_key):
        return f"{name}\nmostly cash: invested {invested} of {total} yrs, too thin to test"
    return f"{name}, {total - invested} yrs cash" if total > invested else name


def mark_thin(ax, bars, legs):
    """Hatch the mostly-cash bars, grey their row labels and rule them off; returns the legend entry."""
    thin = [t for _, _, t in legs]
    for bar, tick, t in zip(bars, ax.get_yticklabels(), thin):
        if t:
            bar.set_hatch("///")
            bar.set_edgecolor(THIN_EDGE)
            tick.set_color(THIN_EDGE)
    if not any(thin):
        return []
    if not all(thin):
        ax.axhline(y=thin.index(True) - 0.5, color=THIN_EDGE, linewidth=0.8, linestyle=":")
    return [Patch(facecolor=THIN_FILL, edgecolor=THIN_EDGE, hatch="///",
                  label="Mostly cash: too thin to test")]


def sp500_reference(metric):
    """S&P 500 figure for the comparison charts' reference line: the US entry's, or None without one."""
    return ((data.get("NYSE_NASDAQ_AMEX") or {}).get("spy") or {}).get(metric)


def comparison_note(legs, excluded):
    """Footer lines for the cash counts on the row labels and for the markets left out."""
    note = ""
    if any("yrs cash" in leg_label(k) for k, _, _ in legs):
        note += (f"\n\"yrs cash\": years with fewer than {cash_summary(legs[0][0])[3]} qualifying stocks."
                 " A cash year earns 0% and counts in the CAGR and drawdown.")
    if any(thin for _, _, thin in legs):
        note += "\nHatched: invested in fewer than half the years. Too thin to test, so not ranked."
    if excluded:
        note += f"\nExcluded (truncated window or never invested): {', '.join(excluded)}."
    return note


def chart_comparison_cagr(filename):
    legs, excluded = comparison_legs("cagr")
    if not legs:
        print(f"  Skipping {filename}: no market with a full window and an invested period")
        remove_stale([filename])
        return

    names = [leg_label(k) for k, _, _ in legs]
    cagrs = [c for _, c, _ in legs]
    colors = [THIN_FILL if thin else COLORS.get(k, "#95a5a6") for k, _, thin in legs]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = sp500_reference("cagr")
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500, USD ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("P/E Mean Reversion CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    handles = ax.get_legend_handles_labels()[0] + mark_thin(ax, bars, legs)
    if handles:
        ax.legend(handles=handles, fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (_, cagr, thin) in enumerate(legs):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9,
                fontweight="normal" if thin else "bold", color=THIN_EDGE if thin else "black")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same sector-relative P/E screen, annual rebalance, equal weight. Local currency returns."
             + comparison_note(legs, excluded),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    legs, excluded = comparison_legs("max_drawdown")
    if not legs:
        print(f"  Skipping {filename}: no market with a full window and an invested period")
        remove_stale([filename])
        return

    names = [leg_label(k) for k, _, _ in legs]
    drawdowns = [dd for _, dd, _ in legs]
    colors = [THIN_FILL if thin else COLORS.get(k, "#95a5a6") for k, _, thin in legs]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = sp500_reference("max_drawdown")
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500, USD ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_xlim(left=min(drawdowns) - 8)  # room for the deepest bar's label
    ax.set_title("P/E Mean Reversion Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    handles = ax.get_legend_handles_labels()[0] + mark_thin(ax, bars, legs)
    if handles:
        ax.set_ylim(bottom=len(names) - 0.2 + 0.85 * len(handles))  # blank rows for the legend
        ax.legend(handles=handles, fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (_, dd, thin) in enumerate(legs):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=9, ha="right",
                fontweight="normal" if thin else "bold", color=THIN_EDGE if thin else "black")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same sector-relative P/E screen, annual rebalance, equal weight."
             + comparison_note(legs, excluded),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def skip_reason(exchange_key):
    """Why a market gets no per-market charts, or None when it is drawn."""
    v = data.get(exchange_key)
    if not isinstance(v, dict):
        return "not in exchange_comparison.json"
    if "error" in v:
        return f"run errored ({v['error']})"
    if not v.get("annual_returns") or (v.get("portfolio") or {}).get("cagr") is None:
        return "no measured periods"
    if v.get("window_truncated", False):
        return "benchmark window truncated"
    if v.get("invested_periods", 0) <= 0:
        return "all cash, never invested: too thin to test"
    return None


# ---- Generate charts for each available exchange ----
valid_exchanges = [k for k in data if skip_reason(k) is None]

print(f"Valid exchanges with data: {valid_exchanges}")

# (result key, blog dir, footer universe, currency symbol, display name)
EXCHANGE_CHART_CONFIGS = [
    ("NYSE_NASDAQ_AMEX", "us", "NYSE + NASDAQ + AMEX", "$", "US"),
    ("NSE", "india", "NSE (returns in INR)", "Rs", "India"),
    ("JPX", "japan", "JPX (returns in JPY)", "¥", "Japan"),
    ("LSE", "uk", "LSE (returns in GBP)", "£", "UK"),
    ("SHZ_SHH", "china", "SHZ + SHH (returns in CNY)", "CN¥", "China"),
    ("HKSE", "hongkong", "HKSE (returns in HKD)", "HK$", "Hong Kong"),
    ("TAI_TWO", "taiwan", "TAI + TWO (returns in TWD)", "NT$", "Taiwan"),
    ("SET", "thailand", "SET (returns in THB)", "THB ", "Thailand"),
    ("XETRA", "germany", "XETRA (returns in EUR)", "€", "Germany"),
    ("KSC", "korea", "KSC (returns in KRW)", "KRW ", "Korea"),
    ("TSX", "canada", "TSX (returns in CAD)", "C$", "Canada"),
    ("STO", "sweden", "STO (returns in SEK)", "SEK ", "Sweden"),
    ("SIX", "switzerland", "SIX (returns in CHF)", "CHF ", "Switzerland"),
]

for ex_key, region_slug, footer, currency, display in EXCHANGE_CHART_CONFIGS:
    if ex_key not in valid_exchanges:
        print(f"\nSkipping blogs/{region_slug}/ ({ex_key}): {skip_reason(ex_key)}")
        remove_stale([f"{region_slug}_cumulative_growth.png", f"{region_slug}_annual_returns.png"])
        continue
    print(f"\nGenerating charts for blogs/{region_slug}/...")
    if mostly_cash(ex_key):
        print("  Mostly cash: both charts carry the too-thin-to-test line")
    ex = data[ex_key]
    start_year = ex["annual_returns"][0]["year"] if ex["annual_returns"] else 2000
    end_year = ex["annual_returns"][-1]["year"] if ex["annual_returns"] else 2025
    bench = benchmark_label(data, ex_key)
    chart_cumulative(
        [ex_key], f"{region_slug}_cumulative_growth.png",
        # the last period starts in end_year and ends a year later
        f"Growth of {currency}10,000: P/E Mean Reversion {display} vs {bench} ({start_year}-{end_year + 1})",
        footer, currency=currency
    )
    chart_annual_bars(
        [ex_key], f"{region_slug}_annual_returns.png",
        f"P/E Mean Reversion {display} vs {bench}: Year-by-Year Returns ({start_year}-{end_year})",
        footer
    )

for ex_key in data:
    if ex_key not in EXCHANGE_LABELS:
        print(f"\nNo chart config for {ex_key}: no per-market charts, comparison charts only")

print("\nGenerating comparison charts...")
if sp500_reference("cagr") is None:
    print("  No US entry: comparison charts are drawn without the S&P 500 line")
chart_comparison_cagr("comparison_cagr.png")
chart_comparison_drawdown("comparison_drawdown.png")

print(f"\nDone. Charts saved to {charts_dir}/")
print("Move charts to ts-content-creator/content/_ready/timing-01-pe-mean-revert/blogs/{region}/")
print("  Prefix with 1_ (cumulative, comparison CAGR) and 2_ (annual bars, comparison drawdown) per the runbook convention.")
