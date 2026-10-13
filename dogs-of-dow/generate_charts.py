"""Generate all Dogs of the Dow charts for blog posts from exchange_comparison.json.

Every chart reads that one file, so rebuild it from the per-market results first:
    python3 scripts/rebuild_comparison.py dogs-of-dow
"""
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

# Color palette
COLORS = {
    "US_MAJOR": "#1a5276",
    "BSE": "#e67e22",
    "NSE": "#f39c12",
    "STO": "#27ae60",
    "SAO": "#16a085",
    "SET": "#d35400",
    "KSC": "#95a5a6",
    "HKSE": "#8e44ad",
    "TAI": "#2c3e50",
    "ASX": "#bdc3c7",
    "TSX": "#7f8c8d",
    "SHZ": "#c0392b",
    "SHH": "#e74c3c",
    "SIX": "#34495e",
    "XETRA": "#1abc9c",
    "JNB": "#b7950b",
    "JPX": "#a93226",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "Dogs of the Dow (US)",
    "BSE": "High Yield Blue Chips (India BSE)",
    "NSE": "High Yield Blue Chips (India NSE)",
    "STO": "High Yield Blue Chips (Sweden)",
    "SAO": "High Yield Blue Chips (Brazil)",
    "SET": "High Yield Blue Chips (Thailand)",
    "KSC": "High Yield Blue Chips (Korea)",
    "HKSE": "High Yield Blue Chips (Hong Kong)",
    "TAI": "High Yield Blue Chips (Taiwan)",
    "ASX": "High Yield Blue Chips (Australia)",
    "TSX": "High Yield Blue Chips (Canada)",
    "SHZ": "High Yield Blue Chips (Shenzhen)",
    "SHH": "High Yield Blue Chips (Shanghai)",
}

# Exchanges ranked in the comparison charts: the 13 the comparison blog reports
# as tested. XETRA, SIX, LSE and SAU are in the results; the blog excludes them.
COMPARISON_EXCHANGES = [
    "US_MAJOR", "BSE", "NSE", "JNB", "STO", "KSC",
    "TAI", "HKSE", "TSX", "JPX", "SET", "SHH", "SHZ",
]

# A preset universe spans more than one exchange: name it so, not by its node key.
UNIVERSE_NAMES = {"Canada": "TSX+TSXV", "Taiwan": "TAI+TWO"}


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    # "year" is the year a holding period STARTS: initial at the first start, each value at its period's end
    years = [ex["annual_returns"][0]["year"]]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"] + 1)
    return years, values


def benchmark_name(exchange_key):
    """Benchmark the results record for an exchange (Sensex, ...); "S&P 500, USD" where a non-US market ran against SPY."""
    return benchmark_legend(data, exchange_key)


def get_spy_cumulative(exchange_key="US_MAJOR", initial=10000):
    """Cumulative growth of an exchange's OWN benchmark series.

    The "spy" key holds whichever benchmark that exchange was measured
    against, which for non-US markets is the local index, not SPY.
    """
    return benchmark_cumulative(data, exchange_key, initial, period_end=True)


def window(exchange_key, end=1):
    """'2000-2025' from the annual returns: first start year to last end year (end=0: last start year)."""
    ar = data[exchange_key]["annual_returns"]
    return f"{ar[0]['year']}-{ar[-1]['year'] + end}"


def periods(exchange_key):
    """(invested, total) rebalances. Pre-rerun files carry no total_rebalances."""
    ex = data[exchange_key]
    total = ex.get("total_rebalances") or ex.get("n_periods") or len(ex.get("annual_returns") or [])
    return total - ex.get("cash_periods", 0), total


def mostly_cash(exchange_key):
    """Invested in fewer than half its periods: too thin to test, never a normal result."""
    invested, total = periods(exchange_key)
    return invested * 2 < total


def cash_years(exchange_key):
    """Start years the portfolio sat in cash (too few names buyable at entry): 0%, still compounded."""
    ex = data[exchange_key]
    if ex.get("period_data"):
        return {int(p["rebalance_date"][:4]) for p in ex["period_data"] if p.get("stocks_held") == 0}
    # Pre-rerun files carry no period_data: a cash year is the exact 0.0 the backtest wrote.
    if not ex.get("cash_periods"):
        return set()
    return {ar["year"] for ar in ex.get("annual_returns") or [] if ar["portfolio"] == 0}


def min_stocks(exchange_key):
    """Cash floor the backtest recorded (backtest.py MIN_STOCKS)."""
    rows = data[exchange_key].get("period_data") or [{}]
    return rows[0].get("min_stocks", 5)


def thin_title(exchange_key):
    """Second title line for a mostly-cash market, so its chart cannot pass for a normal result."""
    invested, total = periods(exchange_key)
    if not mostly_cash(exchange_key):
        return ""
    return f"\nToo thin to test: in cash {total - invested} of {total} years"


def skip_chart(filename, why):
    """A chart that cannot be drawn must not leave an older PNG behind looking current."""
    out = charts_dir / filename
    stale = out.exists()
    out.unlink(missing_ok=True)
    print(f"  Skipped: {out} ({why})" + (", stale PNG deleted" if stale else ""))


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Cumulative growth for given exchanges vs their own local benchmark."""
    fig, ax = plt.subplots(figsize=(12, 6))

    bench_key = exchanges[0]
    spy_years, spy_vals = get_spy_cumulative(bench_key)
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_name(bench_key)} ({data[bench_key]['spy']['cagr']}% CAGR)",
            linestyle="--")

    shaded = False
    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        cash = [ar["year"] for ar in ex["annual_returns"] if ar["year"] in cash_years(ex_key)]
        cash_note = f", cash {len(cash)} of {len(ex['annual_returns'])} yrs" if cash else ""
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{cash_note})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        # Shade the cash years so a flat stretch does not read as an invested 0%.
        for y in cash:
            ax.axvspan(y, y + 1, color=COLORS["SPY"], alpha=0.25, linewidth=0)
            shaded = True

        # Final value annotation
        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    # Benchmark final value (US$ when the benchmark is the S&P 500 standing in for a local index)
    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(benchmark_money(spy_final_k, data, bench_key, suffix="K"),
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel(money_axis_label(exchanges[0]), fontsize=12, fontweight="bold")
    ax.set_title(localize_money_title(title, exchanges[0]) + thin_title(exchanges[0]),
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(exchanges[0]))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, {window(exchanges[0])}"
    if shaded:
        footer += (f"\nShaded: fewer than {min_stocks(exchanges[0])} names buyable at entry,"
                   " so the portfolio held cash that year (0%, flat line).")
    fig.text(0.5, -0.02, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

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
           label=benchmark_name(exchanges[0]), color=COLORS["SPY"], alpha=0.7)

    n_cash = 0
    for idx, ex_key in enumerate(exchanges):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key), color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        # Cash years have no bar: label them so they don't read as a 0% invested year.
        for o, ar in zip(offsets, data[ex_key]["annual_returns"]):
            if ar["year"] in cash_years(ex_key):
                ax.text(o + (idx + 1) * width, 0, "cash", rotation=90, ha="center", va="bottom",
                        fontsize=7, color=COLORS.get(ex_key, "#95a5a6"))
                n_cash += 1

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title + thin_title(exchanges[0]), fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {footer_universe}, annual rebalance, equal weight, {window(exchanges[0])}"
    if n_cash:
        footer += (f"\n\"cash\" marks a year with fewer than {min_stocks(exchanges[0])} names buyable at entry."
                   f" The portfolio held cash (0%) in {n_cash} of {len(years)} years.")
    fig.text(0.5, -0.06, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def market_name(exchange_key):
    """Axis name for a market: its node key, or the exchange list of a preset universe."""
    return UNIVERSE_NAMES.get(data[exchange_key].get("universe"), exchange_key)


def comparison_items(metric, filename):
    """(key, node) pairs fit to rank on `metric`, plus footer lines naming the ones left out.

    Left out: a market that never invested, one in cash for more than half its
    periods (too thin to test) and one whose window was cut short. None of them
    is a full-window result, so none may sit in a ranking as if it were.
    """
    items, left_out, missing = [], [], []
    for k in COMPARISON_EXCHANGES:
        ex = data.get(k) or {}
        if not ex.get("portfolio"):
            missing.append(k)
            continue
        invested, total = periods(k)
        if invested <= 0:
            left_out.append(f"{market_name(k)} (never invested)")
        elif mostly_cash(k):
            left_out.append(f"{market_name(k)} (too thin to test: in cash {total - invested} of {total} years)")
        elif ex.get("window_truncated"):
            left_out.append(f"{market_name(k)} (window cut short)")
        elif ex["portfolio"].get(metric) is None:
            missing.append(k)
        else:
            items.append((k, ex))
    if missing:
        print(f"  {filename}: no {metric} for {', '.join(missing)}")
    note = f"\nNot ranked: {'; '.join(left_out)}." if left_out else ""
    if any(v.get("cash_periods") for _, v in items):
        note += (f"\nCash years: fewer than {min_stocks(items[0][0])} names buyable at entry."
                 " A cash year earns 0% and counts in every figure.")
    return items, note


def tick_label(exchange_key):
    """Market name plus its cash years, so a part-cash bar is not read as fully invested."""
    n = data[exchange_key].get("cash_periods", 0)
    return f"{market_name(exchange_key)}, {n} yr{'s' if n > 1 else ''} cash" if n else market_name(exchange_key)


def us_benchmark(metric):
    """The US node's S&P 500 figure, the blog's cross-market yardstick. None without a US node."""
    return ((data.get("US_MAJOR") or {}).get("spy") or {}).get(metric)


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    items, left_out = comparison_items("cagr", filename)
    if not items:
        skip_chart(filename, "no market to rank")
        return
    items.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, v in items]
    cagrs = [v["portfolio"]["cagr"] for k, v in items]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = us_benchmark("cagr")
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"{benchmark_name('US_MAJOR')}, USD ({spy_cagr}% CAGR)")
        ax.legend(fontsize=10)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([tick_label(k) for k in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_title("High Yield Blue Chips: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 30 by market cap, top 10 by yield, annual rebalance, equal weight"
             + left_out, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    items, left_out = comparison_items("max_drawdown", filename)
    if not items:
        skip_chart(filename, "no market to rank")
        return
    items.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, v in items]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in items]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = us_benchmark("max_drawdown")
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"{benchmark_name('US_MAJOR')}, USD ({spy_dd:.1f}%)")
        ax.legend(fontsize=10, loc="upper left")  # shallowest bars sit on top, so that corner is clear

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([tick_label(k) for k in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_title("High Yield Blue Chips: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 30 by market cap, top 10 by yield, annual rebalance, equal weight"
             + left_out, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    items, left_out = comparison_items("sharpe_ratio", filename)
    if not items:
        skip_chart(filename, "no market to rank")
        return
    items.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [k for k, v in items]
    sharpes = [v["portfolio"]["sharpe_ratio"] for k, v in items]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = us_benchmark("sharpe_ratio")
    if spy_sharpe is not None:
        ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"{benchmark_name('US_MAJOR')}, USD ({spy_sharpe:.3f})")
        ax.legend(fontsize=10)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([tick_label(k) for k in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("High Yield Blue Chips: Sharpe Ratio by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, val) in enumerate(zip(bars, sharpes)):
        x_pos = max(val, 0) + 0.01
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Top 30 by market cap, top 10 by yield, annual rebalance, equal weight"
             + left_out, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

REGIONAL_CHARTS = [
    ("US_MAJOR", "us", "Dogs of the Dow", "Dow 30, top 10 by dividend yield"),
    # The India blog is written on BSE (Bombay) results, so chart BSE, not NSE.
    ("BSE", "india", "High Yield Blue Chips India",
     "BSE, top 30 by market cap, top 10 by yield"),
    ("STO", "sweden", "High Yield Blue Chips Sweden",
     "STO, top 30 by market cap, top 10 by yield"),
]

# The rebuilt exchange_comparison.json holds each per-market file as its node
# (us.json as US_MAJOR, returns_BSE.json as BSE, sweden.json as STO), so the
# regional charts read the figures the blogs quote.
for ex_key, slug, strategy_label, footer in REGIONAL_CHARTS:
    print(f"Generating charts for blog_{slug}.md...")
    ex = data.get(ex_key) or {}
    why = None
    if not ex.get("portfolio") or not ex.get("annual_returns"):
        why = f"no {ex_key} results"
    elif periods(ex_key)[0] <= 0:
        why = f"{ex_key} never invested"
    if why:
        skip_chart(f"{slug}_cumulative_growth.png", why)
        skip_chart(f"{slug}_annual_returns.png", why)
        continue
    bench = benchmark_name(ex_key)
    chart_cumulative(
        [ex_key], f"{slug}_cumulative_growth.png",
        f"Growth of $10,000: {strategy_label} vs {bench} ({window(ex_key)})",
        footer
    )
    chart_annual_bars(
        [ex_key], f"{slug}_annual_returns.png",
        f"{strategy_label} vs {bench}: Year-by-Year Returns ({window(ex_key, 0)})",
        footer
    )

print("Generating charts for blog_comparison.md...")
chart_comparison_cagr("global_cagr_comparison.png")
chart_comparison_drawdown("global_drawdown_comparison.png")
chart_comparison_sharpe("global_sharpe_comparison.png")

print(f"\nDone. Charts generated in {charts_dir}/")
