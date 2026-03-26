"""Generate all FCF Compounders charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
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
    "XETRA": "#27ae60",
    "LSE": "#16a085",
    "NSE": "#e67e22",
    "HKSE": "#8e44ad",
    "SHZ_SHH": "#e74c3c",
    "JPX": "#d35400",
    "SIX": "#2c3e50",
    "TAI": "#c0392b",
    "SPY": "#aab7b8",
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
    "TAI": "FCF Compounders Taiwan",
}

STRATEGY_DESC = "FCF grew 4+/5yr, ROIC >15%, OPM >15%, annual rebalance, equal weight, 2000-2025"
FOOTER = f"Data: Ceta Research | {STRATEGY_DESC}"


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(initial=10000):
    """Get SPY cumulative from US data."""
    for k in ["NYSE_NASDAQ_AMEX"]:
        if k in data and data[k].get("annual_returns"):
            ex = data[k]
            break
    else:
        for k in data:
            if data[k].get("annual_returns"):
                ex = data[k]
                break
        else:
            return [], []

    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def get_benchmark_cumulative(exchange_key, initial=10000):
    """Get benchmark cumulative from a specific exchange."""
    if exchange_key not in data or not data[exchange_key].get("annual_returns"):
        return [], []
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values

def chart_cumulative(exchanges, filename, title, footer_universe, benchmark_from=None, benchmark_label="S&P 500"):
    """Generate cumulative growth chart for given exchanges vs benchmark."""
    fig, ax = plt.subplots(figsize=(12, 6))

    if benchmark_from:
        bench_years, bench_vals = get_benchmark_cumulative(benchmark_from)
        bench_cagr = data.get(benchmark_from, {}).get("spy", {}).get("cagr", "?")
    else:
        bench_years, bench_vals = get_spy_cumulative()
        bench_cagr = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("cagr", "?")

    if bench_years:
        ax.plot(bench_years, bench_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"{benchmark_label} ({bench_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in data or data[ex_key].get("invested_periods", 0) == 0:
            continue
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)

        final_k = vals[-1] / 1000
        ax.annotate(f"${final_k:,.0f}K",
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    if bench_years:
        bench_final_k = bench_vals[-1] / 1000
        ax.annotate(f"${bench_final_k:,.0f}K",
                    xy=(bench_years[-1], bench_vals[-1]),
                    xytext=(8, -12), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS["SPY"])

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02, f"Data: Ceta Research | {footer_universe}", ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe, benchmark_from=None, benchmark_label="S&P 500"):
    """Generate annual returns bar chart for given exchanges vs benchmark."""
    active = [e for e in exchanges if e in data and data[e].get("invested_periods", 0) > 0]
    if not active:
        print(f"  Skipping {filename}: no data for {exchanges}")
        return

    # Use benchmark from specified exchange or default to first exchange's benchmark
    bench_ex = benchmark_from if benchmark_from and benchmark_from in data else active[0]
    ex = data[bench_ex]
    years = [ar["year"] for ar in ex["annual_returns"]]
    bench_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(active) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))

    offsets = [i - (n_series - 1) * width / 2 for i in x]
    ax.bar([o + 0 * width for o in offsets], bench_returns, width,
           label=benchmark_label, color=COLORS["SPY"], alpha=0.7)

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


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in exchanges_with_data]
    keys = [k for k, _ in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    spy_cagr = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("cagr")
    if spy_cagr:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Compounders CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0
        and v["portfolio"].get("max_drawdown") is not None
        and v["portfolio"]["max_drawdown"] != 0
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in exchanges_with_data]
    keys = [k for k, _ in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_dd = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("max_drawdown")
    if spy_dd:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("FCF Compounders Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if v.get("invested_periods", 0) > 0
        and v["portfolio"].get("sharpe_ratio") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no sharpe_ratio data")
        return

    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [EXCHANGE_LABELS.get(k, k) for k, _ in exchanges_with_data]
    keys = [k for k, _ in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for _, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in keys]

    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.8)))
    ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    spy_sharpe = data.get("NYSE_NASDAQ_AMEX", {}).get("spy", {}).get("sharpe_ratio")
    if spy_sharpe is not None:
        ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title("FCF Compounders Sharpe Ratio by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, val in enumerate(sharpes):
        x_pos = max(val, 0) + 0.01
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02, FOOTER, ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts ----

print("Generating US charts...")
chart_cumulative(
    ["NYSE_NASDAQ_AMEX"], "1_us_cumulative_growth.png",
    "Growth of $10,000: FCF Compounders US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight",
    benchmark_from="NYSE_NASDAQ_AMEX", benchmark_label="S&P 500"
)
chart_annual_bars(
    ["NYSE_NASDAQ_AMEX"], "2_us_annual_returns.png",
    "FCF Compounders US vs S&P 500: Year-by-Year Returns (2000-2024)",
    "NYSE + NASDAQ + AMEX, annual rebalance, equal weight",
    benchmark_from="NYSE_NASDAQ_AMEX", benchmark_label="S&P 500"
)

print("Generating Germany charts...")
chart_cumulative(
    ["XETRA"], "1_germany_cumulative_growth.png",
    "Growth of $10,000: FCF Compounders Germany vs DAX (2000-2025)",
    "XETRA, returns in EUR",
    benchmark_from="XETRA", benchmark_label="DAX"
)
chart_annual_bars(
    ["XETRA"], "2_germany_annual_returns.png",
    "FCF Compounders Germany vs DAX: Year-by-Year Returns (2000-2024)",
    "XETRA, returns in EUR",
    benchmark_from="XETRA", benchmark_label="DAX"
)

print("Generating UK charts...")
chart_cumulative(
    ["LSE"], "1_uk_cumulative_growth.png",
    "Growth of $10,000: FCF Compounders UK vs FTSE 100 (2000-2025)",
    "LSE, returns in GBP",
    benchmark_from="LSE", benchmark_label="FTSE 100"
)
chart_annual_bars(
    ["LSE"], "2_uk_annual_returns.png",
    "FCF Compounders UK vs FTSE 100: Year-by-Year Returns (2000-2024)",
    "LSE, returns in GBP",
    benchmark_from="LSE", benchmark_label="FTSE 100"
)

print("Generating comparison charts...")
chart_comparison_cagr("1_comparison_cagr.png")
chart_comparison_drawdown("2_comparison_drawdown.png")
chart_comparison_sharpe("3_comparison_sharpe.png")

print(f"\nDone. Charts generated in {charts_dir}/")
