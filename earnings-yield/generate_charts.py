"""Generate all Earnings Yield charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_label, is_usd_benchmark_proxy, leg_invested, localize_money_title,
                         market_name, money, money_axis_label, money_formatter, not_ranked_lines,
                         own_index_counts, own_index_verdict, save_own_index_chart)
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
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "LSE": "#16a085",
    "XETRA": "#27ae60",
    "JPX": "#d35400",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "TAI": "#2ecc71",
    "TSX": "#7f8c8d",
    "SIX": "#f39c12",
    "STO": "#1abc9c",
    "SET": "#e91e63",
    "SAU": "#9c27b0",
    "JNB": "#ff5722",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Earnings Yield US (NYSE+NASDAQ+AMEX)",
    "NSE": "Earnings Yield India (NSE)",
    "LSE": "Earnings Yield UK (LSE)",
    "XETRA": "Earnings Yield Germany (XETRA)",
    "JPX": "Earnings Yield Japan (JPX)",
    "SHZ_SHH": "Earnings Yield China (SHZ+SHH)",
    "HKSE": "Earnings Yield Hong Kong (HKSE)",
    "TAI": "Earnings Yield Taiwan (TAI)",
    "TSX": "Earnings Yield Canada (TSX)",
    "SIX": "Earnings Yield Switzerland (SIX)",
    "STO": "Earnings Yield Sweden (STO)",
    "SET": "Earnings Yield Thailand (SET)",
    "SAU": "Earnings Yield Saudi Arabia (SAU)",
    "JNB": "Earnings Yield South Africa (JNB)",
}

FOOTER = (
    "Data: Ceta Research (FMP) | Earnings Yield (EY>0%, ROE>12%, D/E<1.5, IC>3x), "
    "quarterly rebalance, equal weight, 2000-2025"
)

# SAU is excluded from the posts (40% cash periods, thin data)
COMPARISON_EXCLUDE = {"SAU"}
# Annual rise of USD/local, 2000-01-03 to 2025-10-01 (FMP forex), for cross-currency rows
FX_RISE_PCT = {"JNB": 4.09}


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_benchmark_cumulative(exchange_key, initial=10000):
    """Get benchmark cumulative from an exchange's own annual 'spy' series.

    The 'spy' field holds whatever benchmark the backtest used for that
    exchange (SPY for US/JNB/SAU, local index otherwise).
    """
    if exchange_key not in data or not data[exchange_key].get("annual_returns"):
        return [], []
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe, bench_label="S&P 500"):
    """Generate cumulative growth chart for given exchanges vs their benchmark."""
    fig, ax = plt.subplots(figsize=(12, 6))

    bench_key = exchanges[0]
    spy_years, spy_vals = get_benchmark_cumulative(bench_key)
    if spy_years:
        spy_cagr = data.get(bench_key, {}).get("spy", {}).get("cagr", "?")
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"{bench_label} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if (ex_key not in data or data[ex_key].get("invested_periods", 0) == 0
                or data[ex_key].get("window_truncated", False)):
            continue
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)

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

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe, bench_label="S&P 500"):
    """Generate annual returns bar chart for given exchanges vs their benchmark."""
    active = [e for e in exchanges if e in data and data[e].get("invested_periods", 0) > 0
              and not data[e].get("window_truncated", False)]
    if not active:
        print(f"  Skipping {filename}: no data for {exchanges}")
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
           label=bench_label, color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(active):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key), color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# The comparison post's dividend calls: ties after dividends, and leads it doesn't call
DIVIDEND_TIES = {"SET", "SIX"}
NOT_CALLED = {"NSE", "TAI", "SHZ_SHH", "TSX", "JPX"}


def comparison_legs():
    return [k for k, v in data.items()
            if k not in COMPARISON_EXCLUDE
            and not v.get("error") and v.get("invested_periods", 0) > 0
            and not v.get("window_truncated", False)]


def usd_notes(keys):
    """'no local index; ~4.8% in USD' for a market on the S&P 500 with a known FX rise."""
    notes = {}
    for k in keys:
        r = FX_RISE_PCT.get(k)
        if r is not None and is_usd_benchmark_proxy(data, k):
            c = data[k]["portfolio"]["cagr"]
            notes[k] = f"no local index; ~{((1 + c / 100) / (1 + r / 100) - 1) * 100:.1f}% in USD"
    return notes


def excluded_lines():
    parts = [f"{market_name(k, short=True)} (cash in {data[k]['cash_periods']} of "
             f"{leg_invested(data[k])[1]} quarters)" for k in sorted(COMPARISON_EXCLUDE) if k in data]
    return [f"Not shown: {', '.join(parts)}, set aside for its cash periods."] if parts else []


def chart_comparison_cagr(filename):
    """CAGR by exchange, each against its own index."""
    keys = sorted(comparison_legs(), key=lambda k: data[k]["portfolio"]["cagr"], reverse=True)
    verdicts = [own_index_verdict(data, k, dividend_ties=DIVIDEND_TIES, not_called=NOT_CALLED)
                for k in keys]
    c = own_index_counts(verdicts)

    def named(verdict):
        return _names([k for k, v in zip(keys, verdicts) if v == verdict])

    beat = ("Every market with a local index leads it as measured, but not every lead is clear."
            if c["beat"] == c["ranked"] else
            f"{c['beat']} of the {c['ranked']} markets with a local index lead it as measured.")
    save_own_index_chart(
        data, keys, [market_name(k) for k in keys], charts_dir / filename,
        "Earnings Yield: CAGR vs Own Index by Exchange (2000-2025)",
        [FOOTER + ".",
         f"Each market in its own currency against its own index. {beat}",
         f"{named('div_tie')} lead by less than the dividends their price indices leave out: ties.",
         f"{named('not_called')} lead by amounts within the 1.3%-3.5% dividend range, not called "
         "either way."]
        + not_ranked_lines(data, keys, verdicts) + excluded_lines(),
        dividend_ties=DIVIDEND_TIES, not_called=NOT_CALLED, notes=usd_notes(keys))


def _names(keys):
    names = [market_name(k, short=True) for k in keys]
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def chart_comparison_drawdown(filename):
    """Max drawdown by exchange, each against its own index's."""
    keys = sorted(comparison_legs(), key=lambda k: data[k]["portfolio"]["max_drawdown"],
                  reverse=True)
    verdicts = [own_index_verdict(data, k) for k in keys]
    save_own_index_chart(
        data, keys, [market_name(k) for k in keys], charts_dir / filename,
        "Earnings Yield: Max Drawdown vs Own Index by Exchange (2000-2025)",
        [FOOTER + ".", "Each market in its own currency; the tick is its own index's max drawdown."]
        + not_ranked_lines(data, keys, verdicts) + excluded_lines(),
        metric="max_drawdown", decimals=1)


# ---- Generate all charts (run AFTER backtest produces exchange_comparison.json) ----

print("Generating charts for US...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "Earnings Yield US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight"
)

print("Generating charts for India...")
chart_cumulative(
    ["NSE"], "1_india_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield India vs Sensex (2000-2025)",
    "NSE (returns in INR vs Sensex)", bench_label="Sensex"
)
chart_annual_bars(
    ["NSE"], "2_india_annual_returns.png",
    "Earnings Yield India vs Sensex: Year-by-Year Returns (2000-2025)",
    "NSE (returns in INR vs Sensex)", bench_label="Sensex"
)

print("Generating charts for UK...")
chart_cumulative(
    ["LSE"], "1_uk_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield UK vs FTSE 100 (2000-2025)",
    "LSE (returns in GBP vs FTSE 100)", bench_label="FTSE 100"
)
chart_annual_bars(
    ["LSE"], "2_uk_annual_returns.png",
    "Earnings Yield UK vs FTSE 100: Year-by-Year Returns (2000-2025)",
    "LSE (returns in GBP vs FTSE 100)", bench_label="FTSE 100"
)

print("Generating charts for Germany...")
chart_cumulative(
    ["XETRA"], "1_germany_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield Germany vs DAX (2000-2025)",
    "XETRA (returns in EUR vs DAX)", bench_label="DAX"
)
chart_annual_bars(
    ["XETRA"], "2_germany_annual_returns.png",
    "Earnings Yield Germany vs DAX: Year-by-Year Returns (2000-2025)",
    "XETRA (returns in EUR vs DAX)", bench_label="DAX"
)

print("Generating charts for Japan...")
chart_cumulative(
    ["JPX"], "1_japan_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield Japan vs Nikkei 225 (2000-2025)",
    "JPX (returns in JPY vs Nikkei 225)", bench_label="Nikkei 225"
)
chart_annual_bars(
    ["JPX"], "2_japan_annual_returns.png",
    "Earnings Yield Japan vs Nikkei 225: Year-by-Year Returns (2000-2025)",
    "JPX (returns in JPY vs Nikkei 225)", bench_label="Nikkei 225"
)

print("Generating charts for Hong Kong...")
chart_cumulative(
    ["HKSE"], "1_hongkong_cumulative_growth.png",
    "Growth of $10,000: Earnings Yield Hong Kong vs Hang Seng (2000-2025)",
    "HKSE (HKD vs Hang Seng)", bench_label="Hang Seng"
)
chart_annual_bars(
    ["HKSE"], "2_hongkong_annual_returns.png",
    "Earnings Yield Hong Kong vs Hang Seng: Year-by-Year Returns (2000-2025)",
    "HKSE (HKD vs Hang Seng)", bench_label="Hang Seng"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
