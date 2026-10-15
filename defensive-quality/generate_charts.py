"""Generate all Defensive Quality charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_cagr, benchmark_cumulative, benchmark_legend, benchmark_money,
                         localize_money_title, money, money_axis_label, money_formatter)
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# India/UK posts quote the per-exchange reruns, whose "spy" is the local index (Sensex, FTSE 100).
local = {}
for _k in ("NSE", "LSE"):
    _p = results_dir / f"returns_{_k}.json"
    if _p.exists():
        with open(_p) as f:
            local[_k] = json.load(f)

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE":          "#e67e22",
    "SHZ_SHH":          "#c0392b",
    "HKSE":             "#8e44ad",
    "SET":              "#5b2c6f",
    "JPX":              "#6e2f1a",
    "LSE":              "#154360",
    "XETRA":            "#27ae60",
    "TAI_TWO":          "#1a252f",
    "KSC":              "#6c3483",
    "TSX":              "#7f8c8d",
    "SIX":              "#d68910",
    "STO":              "#2e86c1",
    "JNB":              "#117a65",
    "KLS":              "#a04000",
    "JKT":              "#1e8449",
    "SPY":              "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Defensive US",
    "NSE":          "Defensive India",
    "SHZ_SHH":          "Defensive China",
    "HKSE":             "Defensive Hong Kong",
    "SET":              "Defensive Thailand",
    "JPX":              "Defensive Japan",
    "LSE":              "Defensive UK",
    "XETRA":            "Defensive Germany",
    "TAI_TWO":          "Defensive Taiwan",
    "KSC":              "Defensive Korea",
    "TSX":              "Defensive Canada",
    "SIX":              "Defensive Switzerland",
    "STO":              "Defensive Sweden",
    "JNB":              "Defensive South Africa",
    "KLS":              "Defensive Malaysia",
    "JKT":              "Defensive Indonesia",
}

MIN_STOCKS = 10  # backtest.py's floor: fewer names buyable at entry means a cash year


def periods(ex):
    """(invested, total) rebalances of a results node."""
    invested = ex.get("invested_periods", 0)
    return invested, invested + ex.get("cash_periods", 0)


def too_thin(ex):
    """Invested in fewer than half its periods: too thin to test, not a result."""
    invested, total = periods(ex)
    return invested * 2 < total


def cash_years(ex):
    """Start years of the periods held in cash."""
    return {int(p["rebalance_date"][:4]) for p in ex.get("period_data") or [] if p.get("stocks_held") == 0}


def get_cumulative_growth(exchange_key, initial=10000, src=None):
    """Compute cumulative growth from annual returns."""
    ex = (data if src is None else src)[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, src=None):
    """Generate cumulative growth chart vs the market's own benchmark (the US: S&P 500)."""
    src = data if src is None else src
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = benchmark_cumulative(src, exchanges[0])
    spy_cagr = benchmark_cagr(src, exchanges[0])
    bench = benchmark_legend(src, exchanges[0])
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{bench} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in src:
            print(f"  Warning: {ex_key} not in results, skipping")
            continue
        ex = src[ex_key]
        years, vals = get_cumulative_growth(ex_key, src=src)
        cagr = ex["portfolio"]["cagr"]
        invested, total = periods(ex)
        cash = f", cash {total - invested} of {total} yrs" if total > invested else ""
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{cash})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        # Legacy axis: period Y's value sits at x=Y, so a cash year is the flat stretch Y-1 to Y.
        for i, y in enumerate(sorted(cash_years(ex))):
            ax.axvspan(y - 1, y, color=COLORS["SPY"], alpha=0.25, linewidth=0,
                       label=f"In cash (fewer than {MIN_STOCKS} stocks to buy)" if i == 0 else None)

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(benchmark_money(spy_final_k, src, exchanges[0], suffix="K"),
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

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe, src=None):
    """Generate annual returns bar chart."""
    src = data if src is None else src
    bench = benchmark_legend(src, exchanges[0])
    ex = src[exchanges[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=bench, color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(exchanges):
        if ex_key not in src:
            continue
        returns = [ar["portfolio"] for ar in src[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        # A cash year has no bar: label it so it does not read as a 0% invested year.
        cash = cash_years(src[ex_key])
        for i, y in enumerate(years):
            if y in cash:
                ax.text(offsets[i] + (idx + 1) * width, 0, "cash", rotation=90, ha="center",
                        va="bottom", fontsize=7, color="#555555")

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, annual rebalance, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def ranked(metric):
    """(key, node) of the markets fit to rank on `metric`, and the footer.

    Not drawn: a market too thin to test (its figures are mostly the cash line)
    and one whose benchmark could not price the full window.
    """
    legs, thin = [], []
    for k, v in data.items():
        if (not isinstance(v, dict) or v.get("window_truncated", False)
                or v.get("portfolio", {}).get(metric) is None):
            continue
        (thin if too_thin(v) else legs).append((k, v))
    thin.sort(key=lambda kv: periods(kv[1])[1] - periods(kv[1])[0])
    thin_names = [f"{EXCHANGE_LABELS.get(k, k).replace('Defensive ', '')} "
                  f"({periods(v)[1] - periods(v)[0]} of {periods(v)[1]})" for k, v in thin]
    print(f"  Drawn: {', '.join(k for k, _ in legs)}")
    print(f"  Too thin to test, not drawn: {', '.join(thin_names) or 'none'}")
    footer = ("Data: Ceta Research | Consumer Defensive + Utilities + Healthcare, annual rebalance, "
              "2000-2025. Returns in local currency.\n"
              f"\"yrs cash\": years with fewer than {MIN_STOCKS} stocks to buy. "
              "A cash year earns 0% and counts in the CAGR and drawdown.")
    if thin_names:
        footer += ("\nNot shown, too thin to test (in cash in more than half the years; years in cash): "
                   + ", ".join(thin_names) + ".")
    return legs, footer


def leg_label(k, v):
    """Tick label of a drawn market, naming its cash years."""
    invested, total = periods(v)
    cash = total - invested
    return EXCHANGE_LABELS.get(k, k) + (f" ({cash} yr{'' if cash == 1 else 's'} cash)" if cash else "")


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange vs S&P 500."""
    exchanges_with_data, footer = ranked("cagr")
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [leg_label(k, v) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    raw_keys = [k for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in raw_keys]

    fig, ax = plt.subplots(figsize=(11, max(6, len(names) * 0.5)))
    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data["NYSE_NASDAQ_AMEX"]["spy"]["cagr"]
    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Defensive Quality CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, cagr in enumerate(cagrs):
        x_pos = max(cagr, 0) + 0.2
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.01, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data, footer = ranked("max_drawdown")
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"])  # Most negative first

    names = [leg_label(k, v) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    raw_keys = [k for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in raw_keys]

    fig, ax = plt.subplots(figsize=(11, max(6, len(names) * 0.5)))
    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data["NYSE_NASDAQ_AMEX"]["spy"]["max_drawdown"]
    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Defensive Quality: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")

    for i, dd in enumerate(drawdowns):
        ax.text(dd - 0.5, i, f"{dd:.1f}%", va="center", ha="right", fontsize=10)
    ax.set_xlim(min(drawdowns + [spy_dd]) * 1.12, 0)  # room for the deepest bar's label

    fig.text(0.5, -0.01, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Generate charts
print("Generating charts for Defensive Quality blogs...")

print("\nUS charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Defensive Quality US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "Defensive Quality US: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX"
)

# India charts (if available)
if "NSE" in local:
    print("\nIndia charts...")
    chart_cumulative(
        ["NSE"], "1_india_cumulative_growth.png",
        "Growth of $10,000: Defensive Quality India vs Sensex (2000-2025)",
        "NSE (returns in INR)", src=local
    )
    chart_annual_bars(
        ["NSE"], "2_india_annual_returns.png",
        "Defensive Quality India: Year-by-Year Returns (2000-2024)",
        "NSE (returns in INR)", src=local
    )

# China charts (if available)
if "SHZ_SHH" in data:
    print("\nChina charts...")
    chart_cumulative(
        ["SHZ_SHH"], "1_china_cumulative_growth.png",
        "Growth of $10,000: Defensive Quality China vs SSE Composite (2000-2025)",
        "SHZ + SHH (returns in CNY)"
    )
    chart_annual_bars(
        ["SHZ_SHH"], "2_china_annual_returns.png",
        "Defensive Quality China: Year-by-Year Returns (2000-2024)",
        "SHZ + SHH (returns in CNY)"
    )

# UK charts (if available)
if "LSE" in local:
    print("\nUK charts...")
    chart_cumulative(
        ["LSE"], "1_uk_cumulative_growth.png",
        "Growth of $10,000: Defensive Quality UK vs FTSE 100 (2000-2025)",
        "LSE (returns in GBP)", src=local
    )
    chart_annual_bars(
        ["LSE"], "2_uk_annual_returns.png",
        "Defensive Quality UK: Year-by-Year Returns (2000-2024)",
        "LSE (returns in GBP)", src=local
    )

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts in {charts_dir}/")
