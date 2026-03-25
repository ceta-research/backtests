"""Generate all Working Capital Efficiency charts for blog posts from results/exchange_comparison.json."""
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
    "US_MAJOR": "#1a5276",
    "NYSE": "#2980b9",
    "NASDAQ": "#7fb3d8",
    "BSE_NSE": "#e67e22",
    "BSE": "#e67e22",
    "NSE": "#f39c12",
    "XETRA": "#27ae60",
    "SHZ_SHH": "#c0392b",
    "SHZ": "#c0392b",
    "SHH": "#e74c3c",
    "HKSE": "#8e44ad",
    "KSC": "#6c3483",
    "ASX": "#148f77",
    "TSX": "#7f8c8d",
    "STO": "#2e86c1",
    "SIX": "#d68910",
    "SAO": "#cb4335",
    "SAU": "#1e8449",
    "SET": "#5b2c6f",
    "TAI": "#1a252f",
    "SGX": "#0e6655",
    "JPX": "#6e2f1a",
    "LSE": "#154360",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "WC US (NYSE+NASDAQ+AMEX)",
    "BSE_NSE": "WC India (BSE+NSE)",
    "BSE": "WC BSE (India)",
    "NSE": "WC NSE (India)",
    "XETRA": "WC XETRA (Germany)",
    "SHZ_SHH": "WC China (SHZ+SHH)",
    "SHZ": "WC Shenzhen",
    "SHH": "WC Shanghai",
    "HKSE": "WC HKSE (Hong Kong)",
    "KSC": "WC KSC (Korea)",
    "ASX": "WC ASX (Australia)",
    "TSX": "WC TSX (Canada)",
    "STO": "WC STO (Sweden)",
    "SIX": "WC SIX (Switzerland)",
    "SAO": "WC SAO (Brazil)",
    "SAU": "WC SAU (Saudi)",
    "SET": "WC SET (Thailand)",
    "TAI": "WC TAI (Taiwan)",
    "SGX": "WC SGX (Singapore)",
    "JPX": "WC JPX (Japan)",
    "LSE": "WC LSE (UK)",
}

STRATEGY_NAME = "Working Capital Efficiency"
REBALANCE_NOTE = "annual rebalance (June)"


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns."""
    ex = data[exchange_key]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(ref_key="US_MAJOR", initial=10000):
    """Get SPY cumulative from the US exchange (all have same SPY benchmark)."""
    ref = ref_key if ref_key in data else list(data.keys())[0]
    ex = data[ref]
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    # Use first valid exchange's SPY series as benchmark
    ref_key = next((k for k in exchanges if k in data), "US_MAJOR")
    spy_cagr_ref = data.get("US_MAJOR", data[ref_key]).get("spy", {}).get("cagr", "?")

    fig, ax = plt.subplots(figsize=(12, 6))

    spy_years, spy_vals = get_spy_cumulative(ref_key)
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"S&P 500 ({spy_cagr_ref}% CAGR)", linestyle="--")

    spy_final_k = spy_vals[-1] / 1000
    ax.annotate(f"${spy_final_k:,.0f}K",
                xy=(spy_years[-1], spy_vals[-1]),
                xytext=(8, -12), textcoords="offset points",
                fontsize=9, fontweight="bold", color=COLORS["SPY"])

    for ex_key in exchanges:
        if ex_key not in data:
            print(f"  Warning: {ex_key} not in results, skipping.")
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

    ax.set_ylabel("Portfolio Value ($)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"${x:,.0f}"))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, {REBALANCE_NOTE}, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    # Use first available exchange for year list
    ref = next((k for k in exchanges if k in data), None)
    if ref is None:
        print(f"  Skipping {filename}: no data for exchanges {exchanges}")
        return

    ex = data[ref]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    valid_exchanges = [k for k in exchanges if k in data]
    n_series = len(valid_exchanges) + 1

    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))
    offsets = [i - (n_series - 1) * width / 2 for i in x]

    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label="S&P 500", color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(valid_exchanges):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, {REBALANCE_NOTE}, equal weight, 2000-2025",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if not v.get("error") and v.get("invested_periods", 0) > 0
        and v.get("portfolio", {}).get("cagr") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    spy_cagr = data.get("US_MAJOR", list(data.values())[0]).get("spy", {}).get("cagr", 7.83)

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.6)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_cagr}% CAGR)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"{STRATEGY_NAME}: CAGR by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | Same WC/Revenue screen, {REBALANCE_NOTE}, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if not v.get("error") and v.get("invested_periods", 0) > 0
        and v.get("portfolio", {}).get("max_drawdown") is not None
    ]
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    spy_dd = data.get("US_MAJOR", list(data.values())[0]).get("spy", {}).get("max_drawdown", -36.0)

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.6)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
               label=f"S&P 500 ({spy_dd:.1f}%)")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"{STRATEGY_NAME}: Max Drawdown by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="lower left")
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | Same WC/Revenue screen, {REBALANCE_NOTE}, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_sharpe(filename):
    """Horizontal bar chart: Sharpe ratio by exchange."""
    exchanges_with_data = [
        (k, v) for k, v in data.items()
        if not v.get("error") and v.get("invested_periods", 0) > 0
        and v.get("portfolio", {}).get("sharpe_ratio") is not None
    ]
    if not exchanges_with_data:
        print(f"  Skipping {filename}: no sharpe_ratio data")
        return

    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["sharpe_ratio"], reverse=True)

    names = [k for k, v in exchanges_with_data]
    sharpes = [v["portfolio"]["sharpe_ratio"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k in names]

    spy_sharpe = data.get("US_MAJOR", list(data.values())[0]).get("spy", {}).get("sharpe_ratio")

    fig, ax = plt.subplots(figsize=(10, max(6, len(names) * 0.6)))
    bars = ax.barh(range(len(names)), sharpes, color=colors, alpha=0.85, height=0.6)

    if spy_sharpe is not None:
        ax.axvline(x=spy_sharpe, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"S&P 500 ({spy_sharpe:.3f})")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.invert_yaxis()
    ax.set_xlabel("Sharpe Ratio", fontsize=12, fontweight="bold")
    ax.set_title(f"{STRATEGY_NAME}: Sharpe Ratio by Exchange (2000-2025)",
                 fontsize=14, fontweight="bold", pad=15)
    if spy_sharpe is not None:
        ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, val) in enumerate(zip(bars, sharpes)):
        x_pos = max(val, 0) + 0.02
        ax.text(x_pos, i, f"{val:.3f}", va="center", fontsize=10, fontweight="bold")

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | Same WC/Revenue screen, {REBALANCE_NOTE}, equal weight",
             ha="center", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Generate all charts based on available data ----

print("Generating Working Capital Efficiency charts...")
print(f"Available exchanges: {list(data.keys())}")

# US flagship (key may be US_MAJOR or NYSE_NASDAQ_AMEX)
us_key = next((k for k in ["US_MAJOR", "NYSE_NASDAQ_AMEX"] if k in data), None)
if us_key:
    print("\nGenerating US charts...")
    chart_cumulative(
        [us_key], "us_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency vs S&P 500 (2000-2025)",
        "NYSE + NASDAQ + AMEX"
    )
    chart_annual_bars(
        [us_key], "us_annual_returns.png",
        "Working Capital Efficiency vs S&P 500: Year-by-Year Returns (2000-2024)",
        "NYSE + NASDAQ + AMEX"
    )

# India
india_keys = [k for k in ["BSE_NSE", "BSE", "NSE"] if k in data]
if india_keys:
    print("\nGenerating India charts...")
    chart_cumulative(
        india_keys, "india_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency India vs S&P 500 (2000-2025)",
        "BSE + NSE (returns in INR, benchmark in USD)"
    )
    chart_annual_bars(
        india_keys, "india_annual_returns.png",
        "Working Capital Efficiency India vs S&P 500: Year-by-Year Returns (2000-2024)",
        "BSE + NSE (returns in INR)"
    )

# Germany
if "XETRA" in data:
    print("\nGenerating Germany charts...")
    chart_cumulative(
        ["XETRA"], "germany_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Germany vs S&P 500 (2000-2025)",
        "XETRA (returns in EUR, benchmark in USD)"
    )
    chart_annual_bars(
        ["XETRA"], "germany_annual_returns.png",
        "Working Capital Efficiency Germany vs S&P 500: Year-by-Year Returns (2000-2024)",
        "XETRA (returns in EUR)"
    )

# China
china_keys = [k for k in ["SHZ_SHH", "SHZ", "SHH"] if k in data]
if china_keys:
    print("\nGenerating China charts...")
    chart_cumulative(
        china_keys, "china_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency China vs S&P 500 (2000-2025)",
        "SHZ + SHH (returns in CNY, benchmark in USD)"
    )
    chart_annual_bars(
        china_keys, "china_annual_returns.png",
        "Working Capital Efficiency China vs S&P 500: Year-by-Year Returns (2000-2024)",
        "SHZ + SHH (returns in CNY)"
    )

# Hong Kong
if "HKSE" in data:
    print("\nGenerating Hong Kong charts...")
    chart_cumulative(
        ["HKSE"], "hongkong_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Hong Kong vs S&P 500 (2000-2025)",
        "HKSE (HKD pegged to USD)"
    )
    chart_annual_bars(
        ["HKSE"], "hongkong_annual_returns.png",
        "Working Capital Efficiency Hong Kong vs S&P 500: Year-by-Year Returns (2000-2024)",
        "HKSE (HKD pegged to USD)"
    )

# Korea
if "KSC" in data:
    print("\nGenerating Korea charts...")
    chart_cumulative(
        ["KSC"], "korea_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Korea vs S&P 500 (2000-2025)",
        "KSC (returns in KRW, benchmark in USD)"
    )
    chart_annual_bars(
        ["KSC"], "korea_annual_returns.png",
        "Working Capital Efficiency Korea vs S&P 500: Year-by-Year Returns (2000-2024)",
        "KSC (returns in KRW)"
    )

# Australia
if "ASX" in data:
    print("\nGenerating Australia charts...")
    chart_cumulative(
        ["ASX"], "australia_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Australia vs S&P 500 (2000-2025)",
        "ASX (returns in AUD, benchmark in USD)"
    )
    chart_annual_bars(
        ["ASX"], "australia_annual_returns.png",
        "Working Capital Efficiency Australia vs S&P 500: Year-by-Year Returns (2000-2024)",
        "ASX (returns in AUD)"
    )

# Sweden
if "STO" in data:
    print("\nGenerating Sweden charts...")
    chart_cumulative(
        ["STO"], "sweden_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Sweden vs S&P 500 (2000-2025)",
        "STO (returns in SEK, benchmark in USD)"
    )
    chart_annual_bars(
        ["STO"], "sweden_annual_returns.png",
        "Working Capital Efficiency Sweden vs S&P 500: Year-by-Year Returns (2000-2024)",
        "STO (returns in SEK)"
    )

# Canada
if "TSX" in data:
    print("\nGenerating Canada charts...")
    chart_cumulative(
        ["TSX"], "canada_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Canada vs S&P 500 (2000-2025)",
        "TSX (returns in CAD, benchmark in USD)"
    )
    chart_annual_bars(
        ["TSX"], "canada_annual_returns.png",
        "Working Capital Efficiency Canada vs S&P 500: Year-by-Year Returns (2000-2024)",
        "TSX (returns in CAD)"
    )

# Switzerland
if "SIX" in data:
    print("\nGenerating Switzerland charts...")
    chart_cumulative(
        ["SIX"], "switzerland_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Switzerland vs S&P 500 (2000-2025)",
        "SIX (returns in CHF, benchmark in USD)"
    )
    chart_annual_bars(
        ["SIX"], "switzerland_annual_returns.png",
        "Working Capital Efficiency Switzerland vs S&P 500: Year-by-Year Returns (2000-2024)",
        "SIX (returns in CHF)"
    )

# Taiwan
if "TAI" in data:
    print("\nGenerating Taiwan charts...")
    chart_cumulative(
        ["TAI"], "taiwan_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Taiwan vs S&P 500 (2000-2025)",
        "TAI (returns in TWD, benchmark in USD)"
    )
    chart_annual_bars(
        ["TAI"], "taiwan_annual_returns.png",
        "Working Capital Efficiency Taiwan vs S&P 500: Year-by-Year Returns (2000-2024)",
        "TAI (returns in TWD)"
    )

# Thailand
if "SET" in data:
    print("\nGenerating Thailand charts...")
    chart_cumulative(
        ["SET"], "thailand_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Thailand vs S&P 500 (2000-2025)",
        "SET (returns in THB, benchmark in USD)"
    )
    chart_annual_bars(
        ["SET"], "thailand_annual_returns.png",
        "Working Capital Efficiency Thailand vs S&P 500: Year-by-Year Returns (2000-2024)",
        "SET (returns in THB)"
    )

# UK (LSE)
if "LSE" in data:
    print("\nGenerating UK charts...")
    chart_cumulative(
        ["LSE"], "uk_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency UK vs S&P 500 (2000-2025)",
        "LSE (returns in GBP, benchmark in USD)"
    )
    chart_annual_bars(
        ["LSE"], "uk_annual_returns.png",
        "Working Capital Efficiency UK vs S&P 500: Year-by-Year Returns (2000-2024)",
        "LSE (returns in GBP)"
    )

# Japan
if "JPX" in data:
    print("\nGenerating Japan charts...")
    chart_cumulative(
        ["JPX"], "japan_cumulative_growth.png",
        "Growth of $10,000: Working Capital Efficiency Japan vs S&P 500 (2000-2025)",
        "JPX (returns in JPY, benchmark in USD)"
    )
    chart_annual_bars(
        ["JPX"], "japan_annual_returns.png",
        "Working Capital Efficiency Japan vs S&P 500: Year-by-Year Returns (2000-2024)",
        "JPX (returns in JPY)"
    )

# Comparison charts (need multiple exchanges)
valid_exchanges = [k for k, v in data.items()
                   if not v.get("error") and v.get("invested_periods", 0) > 0]
if len(valid_exchanges) >= 2:
    print("\nGenerating comparison charts...")
    chart_comparison_cagr("comparison_cagr.png")
    chart_comparison_drawdown("comparison_drawdown.png")
    chart_comparison_sharpe("comparison_sharpe.png")

print(f"\nDone. Charts generated in {charts_dir}/")
