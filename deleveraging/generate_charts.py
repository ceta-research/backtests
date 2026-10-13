"""Generate all Deleveraging charts for blog posts from exchange_comparison.json.

Cash. A quarter with too few stocks to buy is held in cash (0%), and the per-market
charts mark those quarters. A market invested in fewer than half its quarters is
mostly cash, too thin to test: its charts say so and the comparison charts leave it
out of the ranking and name it in the footer. A market with no usable results gets
no chart, and any earlier PNG of it is deleted.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import json
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cagr, benchmark_cumulative, benchmark_label, benchmark_legend,
                         currency_code, is_usd_benchmark_proxy, localize_money_title, money,
                         money_axis_label, money_formatter)

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
    "LSE": "#27ae60",
    "XETRA": "#2980b9",
    "TSX": "#7f8c8d",
    "SET": "#2ecc71",
    "STO": "#3498db",
    "HKSE": "#8e44ad",
    "SHZ_SHH": "#e74c3c",
    "SIX": "#1abc9c",
    "JNB": "#d35400",
    "OSL": "#16a085",
    "SES": "#f39c12",
    "TAI_TWO": "#95a5a6",
    "KSC": "#7d6608",
    "SPY": "#aab7b8",
}
C_BEAT, C_TRAIL, C_PROXY = "#27ae60", "#e74c3c", "#9E9E9E"  # comparison bars; grey = S&P 500 (USD) proxy

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Deleveraging US",
    "NSE": "Deleveraging India",
    "JPX": "Deleveraging Japan",
    "LSE": "Deleveraging UK",
    "XETRA": "Deleveraging Germany",
    "TSX": "Deleveraging Canada",
    "SET": "Deleveraging Thailand",
    "STO": "Deleveraging Sweden",
    "HKSE": "Deleveraging Hong Kong",
    "SHZ_SHH": "Deleveraging China",
    "SIX": "Deleveraging Switzerland",
    "JNB": "Deleveraging South Africa",
    "OSL": "Deleveraging Norway",
    "SES": "Deleveraging Singapore",
    "TAI_TWO": "Deleveraging Taiwan",
    "KSC": "Deleveraging Korea",
}

# Per-market charts: comparison key (the returns_<KEY>.json file stem) -> chart file slug
MARKET_SLUGS = {"NYSE_NASDAQ_AMEX": "us", "NSE": "india", "JPX": "japan",
                "LSE": "uk", "XETRA": "germany", "TSX": "canada"}

# Results "universe" -> exchanges screened, where the name is not the exchange list itself.
# The canada preset (TSX + TSXV) is stored under the TSX key.
UNIVERSE_EXCHANGES = {"US_MAJOR": "NYSE + NASDAQ + AMEX", "Canada": "TSX + TSXV"}

FOOTER = ("Data: Ceta Research | Deleveraging (D/E YoY <-10%, prior D/E>0.1, ROE>8%), "
          "quarterly rebalance, equal weight")


def unusable(val):
    """Why an exchange's results cannot be charted or ranked, or None if they can."""
    if val is None:
        return "not in exchange_comparison.json"
    if "error" in val or not val.get("annual_returns"):
        return "no results"
    if val.get("window_truncated", False):
        return "truncated window"
    if val.get("invested_periods", 0) <= 0:
        return "never invested"
    return None


def cash_stats(ex):
    """(cash, total) quarters. Files from before the rerun carry no total_rebalances."""
    return ex.get("cash_periods", 0), ex.get("total_rebalances") or ex.get("n_periods", 0)


def mostly_cash(ex):
    """In cash more than half its quarters: too thin to test, never shown as a normal result."""
    cash, total = cash_stats(ex)
    return cash * 2 > total


def cash_floor(ex):
    """Stock count below which a quarter is held in cash."""
    return (ex.get("period_data") or [{}])[0].get("min_stocks", 10)


def cash_quarters(ex):
    """{year: [cash quarters, quarters]} from period_data; empty for files that predate it."""
    by_year = {}
    for p in ex.get("period_data") or []:
        n = by_year.setdefault(int(p["rebalance_date"][:4]), [0, 0])
        n[0] += p.get("stocks_held") == 0
        n[1] += 1
    return by_year


def cash_spans(ex):
    """Stretches in cash as [start, end] in fractional years, consecutive quarters merged."""
    spans = []
    for p in ex.get("period_data") or []:
        if p.get("stocks_held") != 0:
            continue
        start, end = (int(d[:4]) + (int(d[5:7]) - 1) / 12 for d in (p["rebalance_date"], p["exit_date"]))
        if spans and spans[-1][1] == start:
            spans[-1][1] = end
        else:
            spans.append([start, end])
    return spans


def window(ex):
    """Measured window, e.g. '2000-2025'."""
    ar = ex["annual_returns"]
    return ex.get("window_label") or f"{ar[0]['year']}-{ar[-1]['year']}"


def set_title(ax, title, ex):
    """Chart title; a mostly-cash market gets a red too-thin-to-test line under it."""
    thin = mostly_cash(ex)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=34 if thin else 15)
    if thin:
        cash, total = cash_stats(ex)
        ax.text(0.5, 1.02, f"Mostly cash: in cash {cash} of {total} quarters. Too thin to test.",
                transform=ax.transAxes, ha="center", va="bottom", fontsize=11, fontweight="bold",
                color="#c0392b")


def skip_chart(filename, why):
    """Skip a chart and delete any earlier PNG, so a stale one cannot pass for current."""
    out = charts_dir / filename
    stale = out.exists()
    out.unlink(missing_ok=True)
    print(f"  Skipping {filename}: {why}" + (" (deleted the stale PNG)" if stale else ""))


def growth_x(ex):
    """x of the start value, then of each annual_returns year's end, in fractional years.

    "year" is the year its quarters start in. It ends at its last quarter's exit date: a 2025
    of three quarters at 2025.75, not 2026. Files from before the rerun carry no period_data:
    whole years there.
    """
    span = {}  # year -> [first quarter's start, last quarter's end]
    for p in ex.get("period_data") or []:
        if p.get("portfolio_return") is None or p.get("spy_return") is None:
            continue  # not in annual_returns
        start, end = (int(d[:4]) + (int(d[5:7]) - 1) / 12 for d in (p["rebalance_date"], p["exit_date"]))
        span.setdefault(int(start), [start, end])[1] = end
    years = [ar["year"] for ar in ex["annual_returns"]]
    return [span.get(years[0], [years[0]])[0]] + [span.get(y, [y, y + 1])[1] for y in years]


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
    return growth_x(ex), values


def get_spy_cumulative(ref_key, initial=10000):
    """Cumulative growth of THAT exchange's own benchmark series, on the portfolio line's x.

    The "spy" field holds whichever index the exchange was measured against,
    which for non-US markets is the local index.
    """
    return growth_x(data[ref_key]), benchmark_cumulative(data, ref_key, initial)[1]


def chart_cumulative(ex_key, filename, footer_universe):
    """Generate cumulative growth chart for one exchange vs its own benchmark."""
    why = unusable(data.get(ex_key))
    if why:
        skip_chart(filename, f"{ex_key}: {why}")
        return

    ex = data[ex_key]
    label = EXCHANGE_LABELS.get(ex_key, ex_key)
    bench = benchmark_label(data, ex_key)
    color = COLORS.get(ex_key, "#95a5a6")
    cash, total = cash_stats(ex)

    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative(ex_key)
    # CAGR of the line actually plotted (the exchange's own benchmark), not the US SPY's.
    spy_cagr = benchmark_cagr(data, ex_key)
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{bench} ({spy_cagr}% CAGR)", linestyle="--")

    years, vals = get_cumulative_growth(ex_key)
    cagr = ex["portfolio"]["cagr"]
    in_cash = f", cash {cash} of {total} quarters" if cash else ""
    ax.plot(years, vals, color=color, linewidth=2.2, label=f"{label} ({cagr}% CAGR{in_cash})")

    # Cash quarters are shaded: a flat stretch must not read as a 0% invested period.
    spans = cash_spans(ex)
    for i, (start, end) in enumerate(spans):
        ax.axvspan(start, end, color=color, alpha=0.12, linewidth=0,
                   label="In cash (0%)" if i == 0 else None)

    final_k = vals[-1] / 1000
    ax.annotate(money(final_k, ex_key, suffix="K"),
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=color)

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(money(spy_final_k, ex_key, suffix="K"),
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, 12 if spy_vals[-1] > vals[-1] else -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel(money_axis_label(ex_key), fontsize=12, fontweight="bold")
    set_title(ax, localize_money_title(f"Growth of $10,000: {label} vs {bench} ({window(ex)})", ex_key), ex)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(ex_key))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {footer_universe}"
    if spans:
        footer += (f"\nShaded: quarters with fewer than {cash_floor(ex)} stocks to buy."
                   " The portfolio held cash (0%, flat line).")
    elif cash:
        footer += f"\nThe portfolio held cash (0%, flat line) in {cash} of {total} quarters."
    fig.text(0.5, -0.02, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, footer_universe):
    """Generate annual returns bar chart for one exchange vs its own benchmark."""
    why = unusable(data.get(exchange_key))
    if why:
        skip_chart(filename, f"{exchange_key}: {why}")
        return

    ex = data[exchange_key]
    label = EXCHANGE_LABELS.get(exchange_key, exchange_key)
    bench = benchmark_label(data, exchange_key)
    color = COLORS.get(exchange_key, "#1a5276")
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.38
    x = list(range(len(years)))

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label=bench, color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_returns, width,
           label=label, color=color, alpha=0.85)
    # Cash quarters are labelled on their year, so a missing or short bar doesn't read as an invested year.
    by_year, marks = cash_quarters(ex), []
    for xi, year, ret in zip(x, years, port_returns):
        n_cash, n = by_year.get(year, (0, 0))
        if n_cash:
            marks.append(n_cash == n)
            ax.text(xi + width / 2, 0, " cash " if n_cash == n else f" cash {n_cash}/{n} ", rotation=90,
                    ha="center", va="top" if ret > 0 else "bottom", fontsize=7, color=color)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    set_title(ax, f"{label} vs {bench}: Year-by-Year Returns ({window(ex)})", ex)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {footer_universe}"
    cash, total = cash_stats(ex)
    if marks:
        footer += f"\n\"cash\" marks a year held in cash: fewer than {cash_floor(ex)} stocks to buy (0%)."
        if not all(marks):
            footer += " \"cash N/M\" is a year with N of its M quarters in cash."
        footer += f" In cash {cash} of {total} quarters."
    elif cash:
        footer += f"\nThe portfolio held cash (0%) in {cash} of {total} quarters."
    fig.text(0.5, -0.06, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def short_name(key):
    """Market name without the strategy prefix, for the comparison charts."""
    return EXCHANGE_LABELS.get(key, key).replace("Deleveraging ", "")


def excess_cagr(val):
    """Excess CAGR over the exchange's own benchmark."""
    excess = val.get("comparison", {}).get("excess_cagr")
    return excess if excess is not None else val["portfolio"]["cagr"] - val["spy"]["cagr"]


def comparison_legs():
    """(key, results) of the markets fit to rank, plus those left out and those mostly in cash."""
    legs, left_out, thin = [], [], []
    for k, v in data.items():
        # an unknown key (e.g. "nse" from a missed file rename) would get the wrong benchmark label
        why = unusable(v) if k in EXCHANGE_LABELS else "unknown key"
        if why:
            left_out.append(f"{short_name(k)} ({why})")
        elif mostly_cash(v):
            thin.append("{} (cash {} of {} quarters)".format(short_name(k), *cash_stats(v)))
        else:
            legs.append((k, v))
    for name in left_out + thin:
        print(f"  Not ranked: {name}")
    return legs, left_out, thin


def leg_label(key, val):
    cash, total = cash_stats(val)
    return f"{short_name(key)}, cash {cash} of {total} qtrs" if cash else short_name(key)


def comparison_title(what, legs):
    return f"Deleveraging Strategy: {what} ({' / '.join(sorted({window(v) for _, v in legs}))})"


def comparison_legend(any_proxy, marker=None):
    handles = [Patch(color=C_BEAT, alpha=0.85, label="Beat its own benchmark"),
               Patch(color=C_TRAIL, alpha=0.85, label="Trailed its own benchmark")]
    if any_proxy:
        handles.append(Patch(color=C_PROXY, alpha=0.85, label="No local index (vs S&P 500, USD)"))
    if marker:
        handles.append(Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
                              markeredgewidth=3, label=marker))
    return handles


def comparison_footer(legs, left_out, thin):
    """Footer: method, benchmark basis, beat count (S&P 500 proxies not counted), markets not ranked, cash."""
    proxies = [short_name(k) for k, _ in legs if is_usd_benchmark_proxy(data, k)]
    counted = [v for k, v in legs if not is_usd_benchmark_proxy(data, k)]
    note = "Returns in local currency, each against its own market's index"
    note += f"; {', '.join(proxies)} against the S&P 500 in USD, not counted." if proxies else "."
    note += f" {sum(excess_cagr(v) > 0 for v in counted)} of {len(counted)} beat their own benchmark."
    if thin:
        note += f"\nToo thin to test (mostly cash, not ranked): {', '.join(thin)}."
    if left_out:
        note += f"\nLeft out: {', '.join(left_out)}."
    cash_legs = [v for _, v in legs if cash_stats(v)[0]]
    if cash_legs:
        note += (f"\n\"cash N of M qtrs\": quarters with fewer than {cash_floor(cash_legs[0])} stocks to buy."
                 " A cash quarter earns 0% and counts in the CAGR.")
    return f"{FOOTER}\n{note}"


def chart_comparison_cagr(filename, legs, left_out, thin):
    """Horizontal bar chart: CAGR by exchange, each against its own benchmark (marker)."""
    if not legs:
        skip_chart(filename, "no market to rank")
        return
    legs = sorted(legs, key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [leg_label(k, v) for k, v in legs]
    cagrs = [v["portfolio"]["cagr"] for _, v in legs]
    benches = [v["spy"]["cagr"] for _, v in legs]
    excesses = [excess_cagr(v) for _, v in legs]
    proxy = [is_usd_benchmark_proxy(data, k) for k, _ in legs]
    colors = [C_PROXY if p else C_BEAT if e > 0 else C_TRAIL for p, e in zip(proxy, excesses)]

    fig, ax = plt.subplots(figsize=(12, max(5, len(names) * 0.65)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, range(len(names)), marker="|", s=500, linewidths=3, color="black", zorder=3)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_xlim(right=max(cagrs + benches) + 7)
    ax.set_title(comparison_title("CAGR vs Own Benchmark by Exchange", legs),
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(handles=comparison_legend(any(proxy), "Benchmark CAGR"), fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, ((k, _), cagr, bench, exc) in enumerate(zip(legs, cagrs, benches, excesses)):
        ax.text(max(cagr, bench, 0) + 0.3, i, f"{cagr:.2f}% ({exc:+.2f} vs {benchmark_legend(data, k)})",
                va="center", fontsize=9)

    fig.text(0.5, -0.02, comparison_footer(legs, left_out, thin), ha="center", va="top",
             fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_excess(filename, legs, left_out, thin):
    """Horizontal bar chart: Excess CAGR by exchange, each over its own benchmark."""
    if not legs:
        skip_chart(filename, "no market to rank")
        return
    legs = sorted(legs, key=lambda x: excess_cagr(x[1]), reverse=True)

    names = [leg_label(k, v) for k, v in legs]
    excesses = [excess_cagr(v) for _, v in legs]
    proxy = [is_usd_benchmark_proxy(data, k) for k, _ in legs]
    colors = [C_PROXY if p else C_BEAT if e > 0 else C_TRAIL for p, e in zip(proxy, excesses)]

    fig, ax = plt.subplots(figsize=(12, max(5, len(names) * 0.65)))
    ax.barh(range(len(names)), excesses, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=0, color="black", linewidth=1.0)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Excess CAGR vs own benchmark (%)", fontsize=12, fontweight="bold")
    ax.margins(x=0.12)  # room for the value labels past both bar ends
    ax.set_title(comparison_title("Excess Return vs Own Benchmark by Exchange", legs),
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(handles=comparison_legend(any(proxy)), fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, exc in enumerate(excesses):
        ax.text(exc + 0.1 if exc >= 0 else exc - 0.1, i, f"{exc:+.2f}%", va="center",
                ha="left" if exc >= 0 else "right", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, comparison_footer(legs, left_out, thin), ha="center", va="top",
             fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----
# A key missing from exchange_comparison.json is skipped with a printed line, never silently.
for key, slug in MARKET_SLUGS.items():
    print(f"Generating {slug} charts...")
    universe = (data.get(key) or {}).get("universe", key)
    exchanges = UNIVERSE_EXCHANGES.get(universe, universe.replace("_", " + "))
    currency = currency_code(key)
    chart_cumulative(
        key, f"1_{slug}_cumulative_growth.png",
        f"{exchanges}, quarterly rebalance, equal weight, size-tiered transaction costs"
        f" (returns and benchmark in {currency})"
    )
    chart_annual_bars(
        key, f"2_{slug}_annual_returns.png",
        f"{exchanges}, quarterly rebalance, equal weight (returns in {currency})"
    )

# --- Comparison ---
print("Generating comparison charts...")
ranking = comparison_legs()
chart_comparison_cagr("1_comparison_cagr.png", *ranking)
chart_comparison_excess("2_comparison_excess.png", *ranking)

print(f"\nDone. Charts saved to {charts_dir}/")
