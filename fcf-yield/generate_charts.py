"""Generate all FCF Yield charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import localize_money_title, money, money_axis_label, money_formatter
import matplotlib.ticker as mticker
import json
import textwrap
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "US_MAJOR": "#1a5276",
    "NYSE": "#2980b9",
    "NASDAQ": "#7fb3d8",
    "LSE": "#16a085",
    "XETRA": "#27ae60",
    "JPX": "#d35400",
    "HKSE": "#8e44ad",
    "KSC": "#2c3e50",
    "Taiwan": "#c0392b",
    "SET": "#27ae60",
    "Canada": "#7f8c8d",
    "China": "#e74c3c",
    "NSE": "#f39c12",
    "STO": "#1abc9c",
    "SIX": "#e67e22",
    "OSL": "#3498db",
    "BENCH": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "FCF Yield US (NYSE+NASDAQ+AMEX)",
    "LSE": "FCF Yield LSE (UK)",
    "XETRA": "FCF Yield XETRA (Germany)",
    "JPX": "FCF Yield JPX (Japan)",
    "HKSE": "FCF Yield HKSE (Hong Kong)",
    "KSC": "FCF Yield KSC (Korea)",
    "Taiwan": "FCF Yield Taiwan (TAI+TWO)",
    "SET": "FCF Yield SET (Thailand)",
    "Canada": "FCF Yield Canada (TSX+TSXV)",
    "China": "FCF Yield China (SHH+SHZ)",
    "NSE": "FCF Yield NSE (India)",
    "STO": "FCF Yield STO (Sweden)",
    "SIX": "FCF Yield SIX (Switzerland)",
    "OSL": "FCF Yield OSL (Norway)",
    "SAU": "FCF Yield SAU (Saudi Arabia)",
    "TLV": "FCF Yield TLV (Israel)",
}

MIN_STOCKS = 10  # backtest.py MIN_STOCKS: fewer buyable names at entry is a cash year


def benchmark_name(exchange_key):
    """Local benchmark name for an exchange (Sensex, FTSE 100, ...)."""
    return data.get(exchange_key, {}).get("benchmark_name", "Benchmark")

FOOTER = "Data: Ceta Research | FCF Yield >8%, ROE >10%, IC >3x, OPM >10%, annual rebalance, equal weight, 2000-2025"


def market_label(exchange_key):
    """Market name for an axis or footer, e.g. 'Canada (TSX+TSXV)'."""
    return EXCHANGE_LABELS.get(exchange_key, exchange_key).replace("FCF Yield ", "")


def cash_split(exchange_key):
    """(cash, total) rebalances of a market; total is cash + invested."""
    ex = data[exchange_key]
    cash = ex.get("cash_periods", 0)
    total = cash + ex.get("invested_periods", 0)
    return cash, total


def cash_years(exchange_key):
    """Start years of the periods a market sat in cash."""
    ex = data[exchange_key]
    if "period_data" in ex:
        return {int(r["rebalance_date"][:4]) for r in ex["period_data"] if r["stocks_held"] == 0}
    if not ex.get("cash_periods"):
        return set()
    # files from before the rerun carry no period_data: a cash year is the exact 0.0 recorded
    return {ar["year"] for ar in ex["annual_returns"] if ar["portfolio"] == 0}


def not_charted(exchange_key):
    """Why a market has no result to chart, or None if it has one."""
    ex = data.get(exchange_key)
    if not ex or not ex.get("annual_returns") or not ex.get("portfolio"):
        return "no results"
    if ex.get("window_truncated", False):
        return "truncated benchmark window"
    if ex.get("invested_periods", 0) <= 0:
        return "all cash, too thin to test"
    return None


def mostly_cash(exchange_key):
    """Invested in fewer than half its periods: too thin to test, never a normal result."""
    cash, total = cash_split(exchange_key)
    return cash * 2 > total


def not_ranked(exchange_key):
    """Why a market stays out of the ranked comparison charts, or None."""
    why = not_charted(exchange_key)
    if why is None and mostly_cash(exchange_key):
        cash, total = cash_split(exchange_key)
        why = f"mostly cash ({cash} of {total} yrs), too thin to test"
    return why


def skip_chart(filename, why):
    """Skip a chart and delete any earlier PNG, so a stale one cannot pass as current."""
    out = charts_dir / filename
    stale = out.exists()
    out.unlink(missing_ok=True)
    print(f"  Skipping {filename}: {why}" + (f" (deleted stale {out})" if stale else ""))


def ranked_markets(metric):
    """(key, node) pairs fit to rank on a portfolio metric, tick labels by key, and a footer note."""
    ranked, labels, left_out, note = [], {}, [], ""
    for k, v in data.items():
        why = not_ranked(k)
        if why is None and v["portfolio"].get(metric) is None:
            why = f"no {metric}"
        if why:
            left_out.append(f"{market_label(k)}: {why}")
            continue
        cash, _ = cash_split(k)
        ranked.append((k, v))
        labels[k] = f"{market_label(k)}, {cash} yrs cash" if cash else market_label(k)
    if any(cash_split(k)[0] for k, _ in ranked):
        note += (f"\n\"yrs cash\": years with fewer than {MIN_STOCKS} qualifying stocks to buy."
                 " A cash year earns 0% and counts in every figure.")
    if left_out:
        note += "\n" + textwrap.fill(f"Not ranked: {'; '.join(left_out)}.", 150)
    return ranked, labels, note


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    # "year" is the period's START year (July Y -> July Y+1): initial at the first start, each value at its end
    years = [ex["annual_returns"][0]["year"]]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"] + 1)
    return years, values


def get_benchmark_cumulative(exchange_key, initial=10000):
    """Compute cumulative growth of an exchange's own local benchmark."""
    ex = data.get(exchange_key)
    if not ex or not ex.get("annual_returns"):
        return [], []
    values = [initial]
    years = [ex["annual_returns"][0]["year"]]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"] + 1)
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Cumulative growth for given exchanges vs their own local benchmark."""
    active = [e for e in exchanges if not not_charted(e)]
    if not active:
        skip_chart(filename, "; ".join(f"{e}: {not_charted(e)}" for e in exchanges))
        return
    fig, ax = plt.subplots(figsize=(12, 6))

    bench_key = exchanges[0]
    bench_years, bench_vals = get_benchmark_cumulative(bench_key)
    if bench_years:
        bench_cagr = data[bench_key]["spy"]["cagr"]
        ax.plot(bench_years, bench_vals, color=COLORS["BENCH"], linewidth=1.8,
                label=f"{benchmark_name(bench_key)} ({bench_cagr}% CAGR)", linestyle="--")

    any_cash = False
    for ex_key in active:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        cash, total = cash_split(ex_key)
        cash_note = f", cash {cash} of {total} yrs" if cash else ""
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{cash_note})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        # Cash years are flat: shade them so they don't read as a 0% invested year.
        for y in sorted(cash_years(ex_key)):
            ax.axvspan(y, y + 1, color=COLORS["BENCH"], alpha=0.3, linewidth=0,
                       label=None if any_cash else "Cash year")
            any_cash = True
        if mostly_cash(ex_key):
            title += f"\nMostly cash ({cash} of {total} years): too thin to test"

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    if bench_years:
        bench_final_k = bench_vals[-1] / 1000
        ax.annotate(money(bench_final_k, exchanges[0], suffix="K"),
                    xy=(bench_years[-1], bench_vals[-1]),
                    xytext=(8, -12), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS["BENCH"])

    ax.set_ylabel(money_axis_label(exchanges[0]), fontsize=12, fontweight="bold")
    ax.set_title(localize_money_title(title, exchanges[0]), fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(exchanges[0]))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {footer_universe}"
    if any_cash:
        footer += (f"\nShaded: fewer than {MIN_STOCKS} qualifying stocks to buy that year,"
                   " so the portfolio held cash (0%, flat line).")
    fig.text(0.5, -0.02, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Annual returns bar chart for given exchanges vs their own local benchmark."""
    active = [e for e in exchanges if not not_charted(e)]
    if not active:
        skip_chart(filename, "; ".join(f"{e}: {not_charted(e)}" for e in exchanges))
        return

    ex = data[active[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    bench_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(active) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], bench_returns, width,
           label=benchmark_name(active[0]), color=COLORS["BENCH"], alpha=0.7)

    footer = f"Data: Ceta Research | {footer_universe}"
    for idx, ex_key in enumerate(active):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key), color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        # Cash years have no bar: label them so they don't read as a 0% invested year.
        cash, total = cash_split(ex_key)
        for o, ar in zip(offsets, data[ex_key]["annual_returns"]):
            if ar["year"] in cash_years(ex_key):
                ax.text(o + (idx + 1) * width, 0, "cash", rotation=90, ha="center", va="bottom",
                        fontsize=7, color=COLORS.get(ex_key, "#95a5a6"))
        if cash:
            footer += (f"\n\"cash\" marks a year with fewer than {MIN_STOCKS} qualifying stocks to buy."
                       f" The portfolio held cash (0%) in {cash} of {total} years.")
        if mostly_cash(ex_key):
            title += f"\nMostly cash ({cash} of {total} years): too thin to test"

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data, labels, note = ranked_markets("cagr")
    if not exchanges_with_data:
        skip_chart(filename, "no market to rank")
        return
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data.get("US_MAJOR", {}).get("spy", {}).get("cagr")
    if spy_cagr:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([labels[k] for k in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Yield CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER + note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data, labels, note = ranked_markets("max_drawdown")
    if not exchanges_with_data:
        skip_chart(filename, "no market to rank")
        return
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data.get("US_MAJOR", {}).get("spy", {}).get("max_drawdown")
    if spy_dd:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([labels[k] for k in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Yield Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")  # the deepest bar's label sits lower left
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER + note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    exchanges_with_data, labels, note = ranked_markets("sharpe_ratio")
    if not exchanges_with_data:
        skip_chart(filename, "no sharpe_ratio data")
        return

    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [k for k, _ in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = data.get("US_MAJOR", {}).get("spy", {}).get("sharpe_ratio")
    if spy_sharpe is not None:
        ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([labels[k] for k in names], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("FCF Yield Sharpe Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, val in enumerate(sharpes):
        x_pos = max(val, 0) + 0.01
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER + note, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

REGIONAL_CHARTS = [
    ("US_MAJOR", "us", "US", "NYSE + NASDAQ + AMEX, annual rebalance, equal weight"),
    ("LSE", "uk", "UK", "LSE, returns and benchmark in GBP"),
    ("JPX", "japan", "Japan", "JPX, returns and benchmark in JPY"),
    ("HKSE", "hongkong", "Hong Kong", "HKSE, returns and benchmark in HKD"),
]

for ex_key, slug, region, footer in REGIONAL_CHARTS:
    print(f"Generating {region} charts...")
    bench = benchmark_name(ex_key)
    chart_cumulative(
        [ex_key], f"1_{slug}_cumulative_growth.png",
        f"Growth of $10,000: FCF Yield {region} vs {bench} (2000-2025)",
        footer
    )
    chart_annual_bars(
        [ex_key], f"2_{slug}_annual_returns.png",
        f"FCF Yield {region} vs {bench}: Year-by-Year Returns (2000-2024)",
        footer
    )

print("Generating comparison charts...")
for ex_key in data:
    if not_ranked(ex_key):
        print(f"  Not ranked: {ex_key}: {not_ranked(ex_key)}")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")
chart_comparison_sharpe("3_comparison_sharpe.png")

print(f"\nDone. Charts generated in {charts_dir}/")
