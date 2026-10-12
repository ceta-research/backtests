"""Generate CCC backtest charts from per-exchange result JSON files."""
import matplotlib
matplotlib.use('Agg')
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_label, is_usd_benchmark_proxy, localize_money_title, money,
                         money_axis_label, money_formatter)
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

# Region mapping: JSON universe name -> short region label for filenames
REGION_MAP = {
    "US_MAJOR": "us",
    "India": "india",
    "NSE": "india",
    "Canada": "canada",
    "XETRA": "germany",
    "China": "china",
    "HKSE": "hongkong",
    "LSE": "uk",
    "SIX": "switzerland",
    "STO": "sweden",
    "KSC": "korea",
    "SAO": "brazil",
    "Taiwan": "taiwan",
    "JSE": "southafrica",
    "JPX": "japan",
    "SES": "singapore",
}

REGION_LABELS = {
    "US_MAJOR": "US (NYSE + NASDAQ + AMEX)",
    "India": "India (NSE)",
    "NSE": "India (NSE)",
    "Canada": "Canada (TSX + TSXV)",
    "XETRA": "Germany (XETRA)",
    "China": "China (SHZ + SHH)",
    "HKSE": "Hong Kong (HKSE)",
    "LSE": "UK (LSE)",
    "SIX": "Switzerland (SIX)",
    "STO": "Sweden (STO)",
    "KSC": "Korea (KSC)",
    "SAO": "Brazil (SAO)",
    "Taiwan": "Taiwan (TAI + TWO)",
    "JSE": "South Africa (JSE)",
    "SES": "Singapore (SES)",
    "JPX": "Japan (JPX)",
}

# Colors
C_LOW = "#2563eb"     # blue - Low CCC
C_MID = "#6b7280"     # gray - Mid CCC
C_HIGH = "#ea580c"    # orange - High CCC
C_SPY = "#111827"     # black - SPY
C_BEAT, C_TRAIL, C_PROXY = "#27ae60", "#c0392b", "#9E9E9E"  # vs own benchmark; grey = S&P 500 (USD) proxy


def load_all_results():
    """Load all per-exchange result JSONs. Skip errored exchanges."""
    data = {}
    for path in sorted(results_dir.glob("ccc_metrics_*.json")):
        with open(path) as f:
            d = json.load(f)
        universe = d.get("universe", "")
        if "error" in d:
            print(f"  Skipping {universe}: {d['error']}")
            continue
        if "annual_returns" not in d:
            print(f"  Skipping {universe}: no annual_returns")
            continue
        data[universe] = d
    return data


def cumulative_growth(returns, initial=10000):
    """Compound a list of annual return percentages into cumulative values."""
    values = [initial]
    for r in returns:
        values.append(values[-1] * (1 + r / 100))
    return values


def cash_years(d, t):
    """Years bucket t held cash (fewer than min_stocks buyable at entry): 0% and still compounded."""
    return d.get("cash_periods", {}).get(t, 0)


def bench_name(data, universe):
    """Name of the index the "spy"/"sp500" fields hold (local index after the rerun)."""
    b = data[universe].get("benchmark")
    if isinstance(b, dict) and b.get("name"):
        return b["name"]
    return benchmark_label(data, universe)


def chart_cumulative_growth(data, universe, region):
    """Chart 1: Cumulative growth - Low CCC vs Mid CCC vs High CCC vs SPY."""
    d = data[universe]
    ar = d["annual_returns"]
    # "year" is the April entry year: start at the first entry, each value lands at its exit year
    years = [ar[0]["year"]] + [row["year"] + 1 for row in ar]

    low_returns = [row["low"] for row in ar]
    mid_returns = [row["mid"] for row in ar]
    high_returns = [row["high"] for row in ar]
    spy_returns = [row["spy"] for row in ar]

    low_cum = cumulative_growth(low_returns)
    mid_cum = cumulative_growth(mid_returns)
    high_cum = cumulative_growth(high_returns)
    spy_cum = cumulative_growth(spy_returns)

    low_cagr = d["portfolios"]["low_ccc"]["cagr"]
    mid_cagr = d["portfolios"]["mid_ccc"]["cagr"]
    high_cagr = d["portfolios"]["high_ccc"]["cagr"]
    spy_cagr = d["portfolios"]["sp500"]["cagr"]
    bench = bench_name(data, universe)
    cash = {t: f", cash {cash_years(d, t)} of {len(ar)} yrs" if cash_years(d, t) else ""
            for t in ("low", "mid", "high")}

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(years, low_cum, color=C_LOW, linewidth=2.2,
            label=f"Low CCC <30d ({low_cagr}% CAGR{cash['low']})")
    ax.plot(years, mid_cum, color=C_MID, linewidth=1.6, alpha=0.7,
            label=f"Mid CCC 30-90d ({mid_cagr}% CAGR{cash['mid']})")
    ax.plot(years, high_cum, color=C_HIGH, linewidth=1.6, alpha=0.7,
            label=f"High CCC >90d ({high_cagr}% CAGR{cash['high']})")
    ax.plot(years, spy_cum, color=C_SPY, linewidth=1.8, linestyle="--",
            label=f"{bench} ({spy_cagr}% CAGR)")

    # Final value annotations
    for vals, color, offset_y in [
        (low_cum, C_LOW, 0),
        (mid_cum, C_MID, -14),
        (high_cum, C_HIGH, 14),
        (spy_cum, C_SPY, -14),
    ]:
        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, universe, suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, offset_y), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=color)

    label = REGION_LABELS.get(universe, universe)
    ax.set_ylabel(money_axis_label(universe), fontsize=12, fontweight="bold")
    ax.set_title(localize_money_title(
                     f"Growth of $10,000: CCC Portfolios vs {bench} - {label}", universe),
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left")
    ax.yaxis.set_major_formatter(money_formatter(universe))
    ax.set_ylim(0, None)
    ax.grid(True, alpha=0.2, linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {label}, annual rebalance, equal weight, 2000-2025"
    if any(cash.values()):
        footer += (f"\nA bucket with fewer than {d.get('min_stocks', 10)} names buyable at entry"
                   " holds cash that year (0%, flat line).")
    fig.text(0.5, -0.02, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / f"1_{region}_cumulative_growth.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_returns(data, universe, region):
    """Chart 2: Annual returns bar chart - Low CCC vs SPY."""
    d = data[universe]
    ar = d["annual_returns"]
    years = [row["year"] for row in ar]
    low_returns = [row["low"] for row in ar]
    spy_returns = [row["spy"] for row in ar]
    bench = bench_name(data, universe)

    fig, ax = plt.subplots(figsize=(12, 7))

    x = list(range(len(years)))
    width = 0.35
    offsets = [i - width / 2 for i in x]

    ax.bar([o for o in offsets], low_returns, width,
           label="Low CCC (<30d)", color=C_LOW, alpha=0.85)
    ax.bar([o + width for o in offsets], spy_returns, width,
           label=bench, color=C_SPY, alpha=0.4)
    # Cash years have no bar: label them so they don't read as a 0% invested year.
    floor = d.get("min_stocks", 10)
    cash_x = [o for o, row in zip(offsets, ar) if row.get("low_buyable", floor) < floor]
    for o in cash_x:
        ax.text(o, 0, "cash", rotation=90, ha="center", va="bottom", fontsize=7, color=C_LOW)

    label = REGION_LABELS.get(universe, universe)
    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(f"Low CCC vs {bench}: Year-by-Year Returns - {label}",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10, loc="upper left")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)

    footer = f"Data: Ceta Research | {label}, annual rebalance, equal weight, 2000-2025"
    if cash_x:
        footer += (f"\n\"cash\" marks a year with fewer than {floor} Low CCC names buyable at entry."
                   f" The portfolio held cash (0%) in {len(cash_x)} of {len(ar)} years.")
    fig.text(0.5, -0.04, footer, ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / f"2_{region}_annual_returns.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def comparison_legs(data, metric):
    """(universe, low_ccc metric, own-benchmark metric, is USD proxy) per market, plus exclusions."""
    legs, excluded = [], []
    for universe, d in data.items():
        invested = len(d.get("annual_returns", [])) - d.get("cash_periods", {}).get("low", 0)
        if d.get("window_truncated", False) or invested <= 0:
            excluded.append(REGION_LABELS.get(universe, universe))
            continue
        legs.append((universe, d["portfolios"]["low_ccc"][metric],
                     d["portfolios"]["sp500"][metric], is_usd_benchmark_proxy(data, universe)))
    return legs, excluded


def comparison_footer(data, legs, excluded, n_beat, verb):
    """Three-line footer: method, benchmark basis, beat count (proxies excluded), exclusions, cash."""
    proxies = [REGION_LABELS.get(u, u) for u, _, _, p in legs if p]
    note = "Returns in local currency, each against its own market's index"
    note += f"; {', '.join(proxies)} against the S&P 500 in USD." if proxies else "."
    note += f" {n_beat} of {len(legs) - len(proxies)} {verb} their own benchmark."
    if excluded:
        note += f" Excluded (truncated window or never invested): {', '.join(excluded)}."
    if any(cash_years(data[u], "low") for u, *_ in legs):
        floor = next(iter(data.values())).get("min_stocks", 10)
        note += (f"\n\"yrs cash\": years with fewer than {floor} Low CCC names buyable at entry."
                 " A cash year earns 0% and counts in the CAGR and drawdown.")
    return "Data: Ceta Research | Low CCC (<30 days), annual rebalance, equal weight\n" + note


def leg_label(data, universe):
    n = cash_years(data[universe], "low")
    label = REGION_LABELS.get(universe, universe)
    return f"{label}, {n} yrs cash" if n else label


def comparison_legend(any_proxy, beat, trail, marker):
    handles = [Patch(color=C_BEAT, alpha=0.85, label=beat),
               Patch(color=C_TRAIL, alpha=0.85, label=trail)]
    if any_proxy:
        handles.append(Patch(color=C_PROXY, alpha=0.85, label="No local index (vs S&P 500, USD)"))
    handles.append(Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
                          markeredgewidth=3, label=marker))
    return handles


def chart_comparison_cagr(data):
    """Chart 3: Low CCC CAGR by exchange, each against its own benchmark (marker)."""
    legs, excluded = comparison_legs(data, "cagr")
    if not legs:
        return
    legs.sort(key=lambda x: x[1], reverse=True)

    names = [leg_label(data, u) for u, _, _, _ in legs]
    cagrs = [c for _, c, _, _ in legs]
    benches = [b for _, _, b, _ in legs]
    colors = [C_PROXY if p else C_BEAT if c > b else C_TRAIL for _, c, b, p in legs]

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)
    ax.scatter(benches, range(len(names)), marker="|", s=500, linewidths=3, color="black", zorder=3)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%, local currency)", fontsize=12, fontweight="bold")
    ax.set_xlim(right=max(max(cagrs), max(benches)) + 7)
    ax.set_title("Low CCC Portfolio CAGR vs Own Benchmark by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(handles=comparison_legend(any(p for *_, p in legs), "Beat its own benchmark",
                                        "Trailed its own benchmark", "Benchmark CAGR"),
              fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.2, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (u, c, b, _) in enumerate(legs):
        e = data[u].get("low_vs_spy", c - b)
        ax.text(max(c, b, 0) + 0.3, i, f"{c:.2f}% ({e:+.2f} vs {bench_name(data, u)})",
                va="center", fontsize=9)

    beat = sum(c > b for _, c, b, p in legs if not p)
    fig.text(0.5, -0.04, comparison_footer(data, legs, excluded, beat, "beat"),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "1_comparison_cagr.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(data):
    """Chart 4: Low CCC max drawdown by exchange, each against its own benchmark (marker)."""
    legs, excluded = comparison_legs(data, "max_drawdown")
    if not legs:
        return
    # Sort by drawdown: least negative (best) at top
    legs.sort(key=lambda x: x[1], reverse=True)

    names = [leg_label(data, u) for u, _, _, _ in legs]
    drawdowns = [dd for _, dd, _, _ in legs]
    bench_dds = [b for _, _, b, _ in legs]
    colors = [C_PROXY if p else C_BEAT if dd > b else C_TRAIL for _, dd, b, p in legs]

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)
    ax.scatter(bench_dds, range(len(names)), marker="|", s=500, linewidths=3, color="black", zorder=3)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_xlim(left=min(min(drawdowns), min(bench_dds)) - 8)
    ax.set_title("Low CCC Portfolio Max Drawdown vs Own Benchmark by Exchange (2000-2025)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.legend(handles=comparison_legend(any(p for *_, p in legs), "Shallower than its own benchmark",
                                        "Deeper than its own benchmark", "Benchmark max drawdown"),
              fontsize=9, loc="upper left", bbox_to_anchor=(1.01, 1))  # outside: in-axes it hid markers
    ax.grid(True, alpha=0.2, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (dd, b) in enumerate(zip(drawdowns, bench_dds)):
        # left of the bar end; also clear of the marker when it sits just past the bar
        x_pos = (b if 0 < dd - b < 6 else dd) - 0.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", ha="right", fontsize=10, fontweight="bold")

    beat = sum(dd > b for _, dd, b, p in legs if not p)
    fig.text(0.5, -0.04, comparison_footer(data, legs, excluded, beat, "fell less than"),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / "2_comparison_drawdown.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# ---- Main ----

if __name__ == "__main__":
    print("Loading results...")
    data = load_all_results()

    if not data:
        print("No valid result files found in results/")
        exit(1)

    print(f"\nFound {len(data)} exchanges with valid data.\n")

    # Per-exchange charts
    for universe in data:
        region = REGION_MAP.get(universe)
        if not region:
            print(f"  Skipping {universe}: no region mapping")
            continue
        print(f"Generating charts for {universe}...")
        chart_cumulative_growth(data, universe, region)
        chart_annual_returns(data, universe, region)

    # Comparison charts
    print("\nGenerating comparison charts...")
    chart_comparison_cagr(data)
    chart_comparison_drawdown(data)

    print(f"\nDone. Charts saved to {charts_dir}/")
