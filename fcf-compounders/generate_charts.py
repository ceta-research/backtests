"""Generate all FCF Compounders charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_legend, benchmark_money, localize_money_title,
                         money, money_axis_label, money_formatter)
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# A market that errored or wrote no annual returns cannot be charted.
for k in [k for k, v in data.items() if not (v.get("portfolio") and v.get("annual_returns"))]:
    print(f"  Dropping {k}: no results in exchange_comparison.json")
    del data[k]

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "XETRA": "#27ae60",
    "LSE": "#16a085",
    "NSE": "#e67e22",
    "HKSE": "#8e44ad",
    "SHZ_SHH": "#e74c3c",
    "JPX": "#d35400",
    "SIX": "#2c3e50",
    "TAI": "#c0392b",
    "SPY": "#aab7b8",
    "CASH": "#bdc3c7",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "FCF Compounders US",
    "XETRA": "FCF Compounders Germany",
    "LSE": "FCF Compounders UK",
    "NSE": "FCF Compounders India",
    "HKSE": "FCF Compounders Hong Kong",
    "SHZ_SHH": "FCF Compounders China",
    "JPX": "FCF Compounders Japan",
    "SIX": "FCF Compounders Switzerland",
    "TAI": "FCF Compounders Taiwan",  # taiwan preset: TAI + TWO
    "TSX": "FCF Compounders Canada",  # canada preset: TSX + TSXV
    "KSC": "FCF Compounders Korea",
    "STO": "FCF Compounders Sweden",
    "OSL": "FCF Compounders Norway",
    "SET": "FCF Compounders Thailand",
    "SES": "FCF Compounders Singapore",
    "JNB": "FCF Compounders South Africa",
    "SAU": "FCF Compounders Saudi Arabia",
    "TLV": "FCF Compounders Israel",
}

STRATEGY_DESC = "FCF grew 4+/5yr, ROIC >15%, OPM >15%, annual rebalance, equal weight, 2000-2025"
FOOTER = f"Data: Ceta Research | {STRATEGY_DESC}"
MIN_STOCKS = 10  # backtest.py: with fewer qualifying stocks buyable at entry the portfolio holds cash


def invested_periods(ex):
    """(invested, total) rebalances. A leg with window_truncated counts as never invested.

    Files from before the rerun have no total_rebalances and can carry a negative invested count.
    """
    total = ex.get("total_rebalances") or ex.get("n_periods") or len(ex["annual_returns"])
    if ex.get("window_truncated", False):
        return 0, total
    return max(ex.get("invested_periods", total - ex.get("cash_periods", 0)), 0), total


def uncharted(ex_key):
    """Missing, never invested or truncated: no chart of its own and no place in a ranking."""
    return ex_key not in data or invested_periods(data[ex_key])[0] == 0


def why_uncharted(ex_key):
    """The reason printed when a market's chart is skipped."""
    if ex_key not in data:
        return f"{ex_key} is not in exchange_comparison.json"
    if data[ex_key].get("window_truncated", False):
        return f"{ex_key} was measured over a truncated benchmark window"
    return f"{ex_key} never invested (too thin to test)"


def mostly_cash(ex_key):
    """Invested in fewer than half its periods: too thin to test, must not read as a normal result."""
    invested, total = invested_periods(data[ex_key])
    return invested * 2 < total


def cash_years(ex):
    """Entry years spent in cash. Files from before the rerun have no period_data: a 0.0% year is cash."""
    if ex.get("period_data"):
        return {int(p["rebalance_date"][:4]) for p in ex["period_data"] if p["stocks_held"] == 0}
    return {ar["year"] for ar in ex["annual_returns"] if ar["portfolio"] == 0}


def cash_tag(ex_key):
    """', cash N of T yrs' for a legend entry; '' for a market that never held cash."""
    invested, total = invested_periods(data[ex_key])
    return f", cash {total - invested} of {total} yrs" if invested < total else ""


def cash_footnote(exchanges, mark):
    """Footer line saying what the cash marking means; '' when no charted market held cash."""
    if not any(cash_tag(e) for e in exchanges):
        return ""
    return (f"\n{mark}: fewer than {MIN_STOCKS} qualifying stocks were buyable at entry,"
            " so the portfolio held cash (0% that year).")


def market_title(title, ex_key, bench):
    """Fill in the benchmark name; a mostly-cash market carries its label in the title."""
    title = title.format(bench=bench)
    if mostly_cash(ex_key):
        invested, total = invested_periods(data[ex_key])
        title += f"\nMostly cash (invested {invested} of {total} years): too thin to test"
    return title


def skip(filename, why):
    """Say why a chart is not drawn and delete any earlier copy, so no stale PNG is left looking current."""
    out = charts_dir / filename
    stale = out.exists()
    out.unlink(missing_ok=True)
    print(f"  Skipping {filename}: {why}" + (f" (deleted stale {out})" if stale else ""))


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    # "year" is the July entry year: start there, each value lands at its exit year
    years = [ex["annual_returns"][0]["year"]]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"] + 1)
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, benchmark_from=None):
    """Generate cumulative growth chart for given exchanges vs benchmark."""
    active = [e for e in exchanges if not uncharted(e)]
    if not active:
        skip(filename, "; ".join(why_uncharted(e) for e in exchanges))
        return

    # Benchmark series and name come from the charted market's own node (local index, or S&P 500 in USD)
    bench_ex = benchmark_from if benchmark_from in data else active[0]
    bench = benchmark_legend(data, bench_ex)
    fig, ax = plt.subplots(figsize=(12, 6))

    bench_years, bench_vals = benchmark_cumulative(data, bench_ex, period_end=True)
    bench_cagr = data[bench_ex].get("spy", {}).get("cagr", "?")

    if bench_years:
        ax.plot(bench_years, bench_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"{bench} ({bench_cagr}% CAGR)", linestyle="--")

    cash = set()
    for ex_key in active:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{cash_tag(ex_key)})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        cash |= cash_years(ex)

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    # Shade cash years so a flat stretch does not read as an invested 0% year.
    for y in sorted(cash):
        ax.axvspan(y, y + 1, color=COLORS["CASH"], alpha=0.35, linewidth=0,
                   label="Year in cash" if y == min(cash) else None)

    if bench_years:
        bench_final_k = bench_vals[-1] / 1000
        ax.annotate(benchmark_money(bench_final_k, data, bench_ex, suffix="K"),
                    xy=(bench_years[-1], bench_vals[-1]),
                    xytext=(8, -12), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel(money_axis_label(exchanges[0]), fontsize=12, fontweight="bold")
    ax.set_title(localize_money_title(market_title(title, active[0], bench), exchanges[0]),
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(exchanges[0]))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}" + cash_footnote(active, "Shaded years"),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe, benchmark_from=None):
    """Generate annual returns bar chart for given exchanges vs benchmark."""
    active = [e for e in exchanges if not uncharted(e)]
    if not active:
        skip(filename, "; ".join(why_uncharted(e) for e in exchanges))
        return

    # Use benchmark from specified exchange or default to first exchange's benchmark
    bench_ex = benchmark_from if benchmark_from and benchmark_from in data else active[0]
    bench = benchmark_legend(data, bench_ex)
    ex = data[bench_ex]
    years = [ar["year"] for ar in ex["annual_returns"]]
    bench_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(active) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], bench_returns, width,
           label=bench, color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(active):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key) + cash_tag(ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        # Cash years have no bar: label them so they don't read as an invested 0% year.
        cash = cash_years(data[ex_key])
        for o, ar in zip(offsets, data[ex_key]["annual_returns"]):
            if ar["year"] in cash:
                ax.text(o + (idx + 1) * width, 0, "cash", rotation=90, ha="center", va="bottom",
                        fontsize=7, color=COLORS.get(ex_key, "#95a5a6"))

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(market_title(title, active[0], bench), fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}" + cash_footnote(active, '"cash"'),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def ranked(metric):
    """(key, node) per charted market, best first; mostly-cash markets follow the ranked ones."""
    legs = [(k, v) for k, v in data.items()
            if not uncharted(k) and v["portfolio"].get(metric) is not None]
    legs.sort(key=lambda x: (mostly_cash(x[0]), -x[1]["portfolio"][metric]))
    return legs


def leg_label(ex_key):
    """Comparison axis label; a mostly-cash market says so."""
    name = EXCHANGE_LABELS.get(ex_key, ex_key)
    if mostly_cash(ex_key):
        invested, total = invested_periods(data[ex_key])
        return f"{name}\nmostly cash: invested {invested} of {total} yrs\ntoo thin to test"
    return name


def hatch_mostly_cash(bars, keys):
    """Redraw the mostly-cash markets' bars grey and hatched."""
    for bar, k in zip(bars, keys):
        if mostly_cash(k):
            bar.set(facecolor="#ecf0f1", edgecolor="#7f8c8d", hatch="///", alpha=1)


def comparison_footer(keys):
    """FOOTER plus what the hatching means and which markets the chart leaves out."""
    note = "Returns in local currency" + ("; the S&P 500 line is in USD." if "NYSE_NASDAQ_AMEX" in data else ".")
    if any(mostly_cash(k) for k in keys):
        note += " Hatched: in cash for more than half the years, too thin to test, listed after the ranked markets."
    left_out = [EXCHANGE_LABELS.get(k, k).replace("FCF Compounders ", "") for k in data if uncharted(k)]
    if left_out:
        note += f"\nLeft out (never invested, or benchmark window truncated): {', '.join(left_out)}."
    return f"{FOOTER}\n{note}"


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = ranked("cagr")
    if not exchanges_with_data:
        skip(filename, "no market was invested")
        return

    names = [leg_label(k) for k, _ in exchanges_with_data]
    keys = [k for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    hatch_mostly_cash(bars, keys)

    spy_cagr = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("cagr")
    if spy_cagr:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Compounders CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, comparison_footer(keys), ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data = [(k, v) for k, v in ranked("max_drawdown") if v["portfolio"]["max_drawdown"] != 0]
    if not exchanges_with_data:
        skip(filename, "no market has a drawdown to show")
        return

    names = [leg_label(k) for k, _ in exchanges_with_data]
    keys = [k for k, _ in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)
    hatch_mostly_cash(bars, keys)

    spy_dd = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("max_drawdown")
    if spy_dd:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Compounders Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left", bbox_to_anchor=(1.01, 1))  # outside: in-axes it hid a bar's label
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, comparison_footer(keys), ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    exchanges_with_data = ranked("sharpe_ratio")
    if not exchanges_with_data:
        skip(filename, "no sharpe_ratio data")
        return

    names = [leg_label(k) for k, _ in exchanges_with_data]
    keys = [k for k, _ in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)
    hatch_mostly_cash(bars, keys)

    spy_sharpe = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("sharpe_ratio")
    if spy_sharpe is not None:
        ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("FCF Compounders Sharpe Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower right")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, val in enumerate(sharpes):
        x_pos = max(val, 0) + 0.01
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, comparison_footer(keys), ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating US charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: FCF Compounders US vs {bench} (2000-2025)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight",
    benchmark_from="NYSE_NASDAQ_AMEX"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "FCF Compounders US vs {bench}: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight",
    benchmark_from="NYSE_NASDAQ_AMEX"
)

print("Generating Germany charts...")
chart_cumulative(
    ["XETRA"], "1_germany_cumulative_growth.png",
    "Growth of $10,000: FCF Compounders Germany vs {bench} (2000-2025)",
    "XETRA, returns in EUR",
    benchmark_from="XETRA"
)
chart_annual_bars(
    ["XETRA"], "2_germany_annual_returns.png",
    "FCF Compounders Germany vs {bench}: Year-by-Year Returns (2000-2024)",
    "XETRA, returns in EUR",
    benchmark_from="XETRA"
)

print("Generating UK charts...")
chart_cumulative(
    ["LSE"], "1_uk_cumulative_growth.png",
    "Growth of $10,000: FCF Compounders UK vs {bench} (2000-2025)",
    "LSE, returns in GBP",
    benchmark_from="LSE"
)
chart_annual_bars(
    ["LSE"], "2_uk_annual_returns.png",
    "FCF Compounders UK vs {bench}: Year-by-Year Returns (2000-2024)",
    "LSE, returns in GBP",
    benchmark_from="LSE"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")
chart_comparison_sharpe("3_comparison_sharpe.png")

print(f"\nDone. Charts generated in {charts_dir}/")
