"""Generate all Equity Growth charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import (benchmark_cumulative, benchmark_label, localize_money_title, money,
                         market_name, money_axis_label, money_formatter, not_ranked_lines,
                         own_index_counts, own_index_verdict, save_own_index_chart)

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
    "STO": "#2e86c1",
    "TSX": "#7f8c8d",
    "SHZ_SHH": "#c0392b",
    "HKSE": "#8e44ad",
    "JPX": "#6e2f1a",
    "LSE": "#154360",
    "KSC": "#6c3483",
    "SIX": "#d68910",
    "TAI_TWO": "#1a252f",
    "SET": "#5b2c6f",
    "JNB": "#1e8449",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "Equity Growth US",
    "NSE": "Equity Growth India",
    "XETRA": "Equity Growth Germany",
    "STO": "Equity Growth Sweden",
    "TSX": "Equity Growth Canada",
    "SHZ_SHH": "Equity Growth China",
    "HKSE": "Equity Growth HK",
    "JPX": "Equity Growth Japan",
    "LSE": "Equity Growth UK",
    "KSC": "Equity Growth Korea",
    "SIX": "Equity Growth Switzerland",
    "TAI_TWO": "Equity Growth Taiwan",
    "SET": "Equity Growth Thailand",
    "JNB": "Equity Growth S. Africa",
}

EXCHANGE_UNIVERSE_LABELS = {
    "NYSE_NASDAQ_AMEX": "NYSE + NASDAQ + AMEX",
    "NSE": "NSE (returns in INR)",
    "XETRA": "XETRA (returns in EUR)",
    "STO": "STO (returns in SEK)",
    "TSX": "TSX (returns in CAD)",
    "SHZ_SHH": "SHZ + SHH (returns in CNY)",
    "HKSE": "HKSE (returns in HKD)",
    "JPX": "JPX (returns in JPY)",
    "LSE": "LSE (returns in GBP)",
    "KSC": "KSC (returns in KRW)",
    "SIX": "SIX (returns in CHF)",
    "TAI_TWO": "TAI + TWO (returns in TWD)",
    "SET": "SET (returns in THB)",
    "JNB": "JNB (returns in ZAR)",
}


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchange_key, filename, title):
    """Generate cumulative growth chart for one exchange vs its own benchmark."""
    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = benchmark_cumulative(data, exchange_key)
    spy_cagr = data[exchange_key]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_label(data, exchange_key)} ({spy_cagr}% CAGR)", linestyle="--")

    years, vals = get_cumulative_growth(exchange_key)
    ex = data[exchange_key]
    cagr = ex["portfolio"]["cagr"]
    label = f"{EXCHANGE_LABELS[exchange_key]} ({cagr}% CAGR)"
    ax.plot(years, vals, color=COLORS[exchange_key], linewidth=2.2, label=label)

    final_k = vals[-1] / 1000
    ax.annotate(money(final_k, exchange_key, suffix="K"),
                xy=(years[-1], vals[-1]),
                xytext=(8, 0), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS[exchange_key])

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(money(spy_final_k, exchange_key, suffix="K"),
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    universe_label = EXCHANGE_UNIVERSE_LABELS.get(exchange_key, exchange_key)
    ax.set_ylabel(money_axis_label(exchange_key), fontsize=12, fontweight="bold")
    ax.set_title(localize_money_title(title, exchange_key), fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(exchange_key))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {universe_label}, annual rebalance (July), 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchange_key, filename, title):
    """Generate annual returns bar chart for one exchange."""
    ex = data[exchange_key]
    years = [ar["year"] for ar in ex["annual_returns"]]
    port_returns = [ar["portfolio"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    fig, ax = plt.subplots(figsize=(14, 5))
    width = 0.35
    x = list(range(len(years)))

    ax.bar([xi - width / 2 for xi in x], spy_returns, width,
           label=benchmark_label(data, exchange_key), color=COLORS["SPY"], alpha=0.7)
    ax.bar([xi + width / 2 for xi in x], port_returns, width,
           label=EXCHANGE_LABELS[exchange_key], color=COLORS[exchange_key], alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")

    universe_label = EXCHANGE_UNIVERSE_LABELS.get(exchange_key, exchange_key)
    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {universe_label}, annual rebalance (July), 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# Ahead of a price index by less than its missing dividends, as the comparison post counts it
NOT_CLEAR_WIN = {"TSX"}

DATA_LINE = "Data: Ceta Research | 5-yr equity CAGR > 10%, ROE > 8%, OPM > 8%, annual July rebalance, 2000-2025."


def comparison_legs():
    return [k for k, v in data.items()
            if isinstance(v, dict) and v.get("invested_periods", 0) > 0
            and not v.get("window_truncated", False)]


def chart_comparison_cagr(filename):
    """CAGR by exchange, each against its own index."""
    keys = sorted(comparison_legs(), key=lambda k: data[k]["portfolio"]["cagr"], reverse=True)
    verdicts = [own_index_verdict(data, k, NOT_CLEAR_WIN) for k in keys]
    c = own_index_counts(verdicts)
    dropped = [market_name(k, short=True) for k, v in zip(keys, verdicts) if v == "dividend"]
    save_own_index_chart(
        data, keys, [market_name(k) for k in keys], charts_dir / filename,
        "Compounding Equity Screen: CAGR vs Own Index (2000-2025)",
        [DATA_LINE,
         f"Each market in its own currency against its own index. {c['beat']} of the {c['ranked']} "
         f"markets with a local index beat it.",
         f"{c['clear']} of those {c['beat']} keep the edge after the dividends price indices leave "
         f"out ({', '.join(dropped)} doesn't)."]
        + not_ranked_lines(data, keys, verdicts),
        not_clear_win=NOT_CLEAR_WIN)


def chart_comparison_drawdown(filename):
    """Max drawdown by exchange, each against its own index's."""
    keys = sorted(comparison_legs(), key=lambda k: data[k]["portfolio"]["max_drawdown"],
                  reverse=True)
    verdicts = [own_index_verdict(data, k, NOT_CLEAR_WIN) for k in keys]
    save_own_index_chart(
        data, keys, [market_name(k) for k in keys], charts_dir / filename,
        "Compounding Equity Screen: Max Drawdown vs Own Index (2000-2025)",
        [DATA_LINE, "Each market in its own currency; the tick is its own index's max drawdown."]
        + not_ranked_lines(data, keys, verdicts),
        metric="max_drawdown", decimals=1)


# Generate all charts
print("Generating charts for Equity Growth blogs...")

# Per-exchange charts (will add more once we know which exchanges have clean results)
print("\nUS charts...")
chart_cumulative(
    "NYSE_NASDAQ_AMEX",
    "1_us_cumulative_growth.png",
    "Growth of $10,000: Compounding Equity Screen US vs S&P 500 (2000-2025)"
)
chart_annual_bars(
    "NYSE_NASDAQ_AMEX",
    "2_us_annual_returns.png",
    "Compounding Equity Screen US: Year-by-Year Returns (2000-2024)"
)

if "NSE" in data:
    print("\nIndia charts...")
    chart_cumulative(
        "NSE",
        "1_india_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen India vs {benchmark_label(data, 'NSE')} (2000-2025)"
    )
    chart_annual_bars(
        "NSE",
        "2_india_annual_returns.png",
        "Compounding Equity Screen India: Year-by-Year Returns (2000-2024)"
    )

if "JPX" in data:
    print("\nJapan charts...")
    chart_cumulative(
        "JPX",
        "1_japan_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen Japan vs {benchmark_label(data, 'JPX')} (2000-2025)"
    )
    chart_annual_bars(
        "JPX",
        "2_japan_annual_returns.png",
        "Compounding Equity Screen Japan: Year-by-Year Returns (2000-2024)"
    )

if "LSE" in data:
    print("\nUK charts...")
    chart_cumulative(
        "LSE",
        "1_uk_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen UK vs {benchmark_label(data, 'LSE')} (2000-2025)"
    )
    chart_annual_bars(
        "LSE",
        "2_uk_annual_returns.png",
        "Compounding Equity Screen UK: Year-by-Year Returns (2000-2024)"
    )

if "XETRA" in data:
    print("\nGermany charts...")
    chart_cumulative(
        "XETRA",
        "1_germany_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen Germany vs {benchmark_label(data, 'XETRA')} (2000-2025)"
    )
    chart_annual_bars(
        "XETRA",
        "2_germany_annual_returns.png",
        "Compounding Equity Screen Germany: Year-by-Year Returns (2000-2024)"
    )

if "SHZ_SHH" in data:
    print("\nChina charts...")
    chart_cumulative(
        "SHZ_SHH",
        "1_china_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen China vs {benchmark_label(data, 'SHZ_SHH')} (2000-2025)"
    )
    chart_annual_bars(
        "SHZ_SHH",
        "2_china_annual_returns.png",
        "Compounding Equity Screen China: Year-by-Year Returns (2000-2024)"
    )

if "TSX" in data:
    print("\nCanada charts...")
    chart_cumulative(
        "TSX",
        "1_canada_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen Canada vs {benchmark_label(data, 'TSX')} (2000-2025)"
    )
    chart_annual_bars(
        "TSX",
        "2_canada_annual_returns.png",
        "Compounding Equity Screen Canada: Year-by-Year Returns (2000-2024)"
    )

if "HKSE" in data:
    print("\nHong Kong charts...")
    chart_cumulative(
        "HKSE",
        "1_hongkong_cumulative_growth.png",
        f"Growth of $10,000: Compounding Equity Screen Hong Kong vs {benchmark_label(data, 'HKSE')} (2000-2025)"
    )
    chart_annual_bars(
        "HKSE",
        "2_hongkong_annual_returns.png",
        "Compounding Equity Screen Hong Kong: Year-by-Year Returns (2000-2024)"
    )

print("\nComparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")

print(f"\nDone. Charts generated in {charts_dir}/")
