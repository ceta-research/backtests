"""Generate all Price-to-Sales charts for blog posts from exchange_comparison.json."""
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import json
import textwrap
from pathlib import Path
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from chart_utils import benchmark_cumulative, benchmark_label, benchmark_legend, benchmark_money, is_usd_benchmark_proxy, localize_money_title, money, money_axis_label, money_formatter

results_dir = Path(__file__).parent / "results"
charts_dir = Path(__file__).parent / "charts"
charts_dir.mkdir(exist_ok=True)

with open(results_dir / "exchange_comparison.json") as f:
    data = json.load(f)

# Color palette
COLORS = {
    "NYSE_NASDAQ_AMEX": "#1a5276",
    "NSE": "#e67e22",
    "JPX": "#e74c3c",
    "LSE": "#8e44ad",
    "XETRA": "#16a085",
    "KSC": "#95a5a6",
    "SAO": "#f39c12",
    "TSX": "#2980b9",
    "HKSE": "#a569bd",
    "TAI": "#7f8c8d",
    "TAI_TWO": "#7f8c8d",
    "STO": "#d35400",
    "SIX": "#2c3e50",
    "JNB": "#27ae60",
    "SHZ_SHH": "#bdc3c7",
    "SET": "#1abc9c",
    "SES": "#e91e63",
    "KLS": "#c0392b",
    "SPY": "#aab7b8",
    "CASH": "#f39c12",
}

EXCHANGE_LABELS = {
    "NYSE_NASDAQ_AMEX": "P/S US (NYSE+NASDAQ+AMEX)",
    "NSE": "P/S India (NSE)",
    "JPX": "P/S Japan (JPX)",
    "LSE": "P/S UK (LSE)",
    "XETRA": "P/S Germany (XETRA)",
    "KSC": "P/S Korea (KSC)",
    "SAO": "P/S Brazil (SAO)",
    "TSX": "P/S Canada (TSX+TSXV)",
    "HKSE": "P/S Hong Kong (HKSE)",
    "TAI": "P/S Taiwan (TAI+TWO)",
    "TAI_TWO": "P/S Taiwan (TAI+TWO)",
    "STO": "P/S Sweden (STO)",
    "SIX": "P/S Switzerland (SIX)",
    "JNB": "P/S South Africa (JNB)",
    "SHZ_SHH": "P/S China (SHZ+SHH)",
    "SET": "P/S Thailand (SET)",
    "SES": "P/S Singapore (SES)",
    "KLS": "P/S Malaysia (KLS)",
}

# The canada and taiwan presets span two exchanges but are filed under the first one's key.
PRESET_UNIVERSE = {"TSX": "Canada", "TAI": "Taiwan"}

# Regional blog charts: (results key, blog directory, country in titles, universe in footers)
REGIONS = [
    ("NYSE_NASDAQ_AMEX", "us", "US", "NYSE + NASDAQ + AMEX"),
    ("NSE", "india", "India", "NSE (returns in INR)"),
    ("JPX", "japan", "Japan", "JPX (returns in JPY)"),
    ("XETRA", "germany", "Germany", "XETRA (returns in EUR)"),
    ("TSX", "canada", "Canada", "TSX + TSXV (returns in CAD)"),
    ("STO", "sweden", "Sweden", "STO (returns in SEK)"),
    ("JNB", "southafrica", "South Africa", "JNB (returns in ZAR)"),
]


def periods(exchange_key):
    """(invested, total) rebalance periods. Files from before the B006 rerun have no total_rebalances."""
    ex = data[exchange_key]
    total = ex.get("total_rebalances") or ex.get("n_periods") or 0
    invested = ex.get("invested_periods")
    if invested is None:
        invested = total - (ex.get("cash_periods") or 0)
    return invested, total


def thin_note(exchange_key):
    """Why a market is too thin to test (never invested, or in fewer than half its periods), else ''."""
    invested, total = periods(exchange_key)
    if invested <= 0:
        return "all cash: never invested"
    if invested * 2 < total:
        return f"mostly cash: invested in {invested} of {total} quarters"
    return ""


def cash_years(exchange_key):
    """{year: (cash quarters, quarters)} for charted years with a cash quarter, from period_data.

    Files from before the B006 rerun carry no period_data: there a 0.0% year is taken as all cash.
    """
    ex = data[exchange_key]
    annual = ex.get("annual_returns") or []
    counts = {}
    for p in ex.get("period_data") or []:
        year = int(p["rebalance_date"][:4])
        cash, n = counts.get(year, (0, 0))
        counts[year] = (cash + (p.get("stocks_held") == 0), n + 1)
    if not counts:
        counts = {ar["year"]: (1, 1) for ar in annual if ar["portfolio"] == 0}
    charted = {ar["year"] for ar in annual}
    return {y: c for y, c in counts.items() if c[0] and y in charted}


def mark_cash(ax, exchange_key, spans):
    """Band each cash year: solid if every quarter was cash, hatched if some were. spans: {year: (x0, x1)}."""
    seen = {p.get_label() for p in ax.patches}  # one legend entry per kind of band
    for year, (cash, n) in sorted(cash_years(exchange_key).items()):
        if year not in spans:
            continue
        label = "In cash all year (0%)" if cash == n else "In cash part of the year"
        ax.axvspan(*spans[year], facecolor=COLORS["CASH"] if cash == n else "none",
                   edgecolor=COLORS["CASH"], hatch=None if cash == n else "///", alpha=0.25,
                   linewidth=0, zorder=0, label=None if label in seen else label)
        seen.add(label)


def stamp_thin(ax, exchanges):
    """Stamp a too-thin market's chart so it cannot read as a normal result."""
    notes = [thin_note(k) for k in exchanges if thin_note(k)]
    if notes:
        print(f"  Too thin to test, chart stamped: {'; '.join(notes)}")
        ax.text(0.5, 0.6, "TOO THIN TO TEST\n" + "\n".join(notes), transform=ax.transAxes,
                ha="center", va="center", fontsize=14, fontweight="bold", color="#c0392b",
                bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="#c0392b", alpha=0.9),
                zorder=5)


def cash_footer(exchanges):
    """Footer line saying how much of the run each charted market sat in cash."""
    lines = []
    for k in exchanges:
        invested, total = periods(k)
        if invested < total:
            who = f"{EXCHANGE_LABELS.get(k, k)}: in" if len(exchanges) > 1 else "In"
            lines.append(f"\n{who} cash (too few qualifying stocks, 0% return) for {total - invested} of {total}"
                         f" quarters{'; cash years are shaded' if cash_years(k) else ''}.")
    return "".join(lines)


def undrawable(exchanges):
    """Why a per-market chart cannot be drawn for these keys, or '' if it can."""
    for k in exchanges:
        ex = data.get(k)
        if not isinstance(ex, dict):
            return f"{k} is not in exchange_comparison.json"
        if "error" in ex:
            return f"{k} errored: {ex['error']}"
        if not (ex.get("annual_returns") and ex.get("portfolio") and ex.get("spy")):
            return f"{k} has no annual returns"
        if periods(k)[1] <= 0:
            return f"{k} has no period counts, so its cash share is unknown"
    return "" if exchanges else "no market given"


def skip_chart(filename, why):
    """Say a chart was not drawn, and delete any PNG from an earlier run so it cannot pass as current."""
    out = charts_dir / filename
    stale = out.exists()
    if stale:
        out.unlink()
    print(f"  Skipped: {filename} ({why}); "
          + ("deleted the stale PNG from an earlier run" if stale else "no earlier PNG to delete"))


def growth_x(exchange_key):
    """x of the start value, then of each annual_returns year's end, in fractional years.

    A year ends at its last quarter's exit date: a 2025 of three quarters at 2025.75, not 2026.
    Files from before the B006 rerun carry no period_data: whole years there.
    """
    ex = data[exchange_key]
    span = {}  # year -> [first quarter's start, last quarter's end]
    for p in ex.get("period_data") or []:
        if p.get("portfolio_return") is None or p.get("spy_return") is None:
            continue  # not in annual_returns
        start, end = (int(d[:4]) + (int(d[5:7]) - 1) / 12 for d in (p["rebalance_date"], p["exit_date"]))
        span.setdefault(int(start), [start, end])[1] = end
    years = [ar["year"] for ar in ex["annual_returns"]]
    return [span.get(years[0], [years[0]])[0]] + [span.get(y, [y, y + 1])[1] for y in years]


def get_cumulative_growth(exchange_key, initial=10000):
    """Cumulative growth from annual returns: initial at the first year's start, each value at its year's end."""
    ex = data[exchange_key]
    values = [initial]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
    return growth_x(exchange_key), values


def get_spy_cumulative(ref_key, initial=10000):
    """Cumulative growth of ref_key's own benchmark (local index or S&P 500), on the portfolio line's x."""
    return growth_x(ref_key), benchmark_cumulative(data, ref_key, initial)[1]


def chart_cumulative(exchanges, filename, title, footer_universe, ref_key=None):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    why = undrawable(exchanges + ([ref_key] if ref_key else []))
    if why:
        return skip_chart(filename, why)
    fig, ax = plt.subplots(figsize=(12, 6))

    if ref_key is None:
        ref_key = exchanges[0]
    spy_years, spy_vals = get_spy_cumulative(ref_key)
    spy_cagr = data[ref_key]["spy"]["cagr"]
    ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
            label=f"{benchmark_legend(data, exchanges[0])} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        ex = data[ex_key]
        years, vals = get_cumulative_growth(ex_key)
        cagr = ex["portfolio"]["cagr"]
        note = thin_note(ex_key)
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR{', ' + note if note else ''})"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)
        # each year's band runs from the value before it to its own
        mark_cash(ax, ex_key, {ar["year"]: x for ar, x in zip(ex["annual_returns"], zip(years, years[1:]))})

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    spy_final_k = spy_vals[-1] / 1000
    # S&P proxy line is USD, not the local currency
    ax.annotate(benchmark_money(spy_final_k, data, exchanges[0], suffix="K"),
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
    stamp_thin(ax, exchanges)

    fig.text(0.5, -0.02,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, equal weight, 2000-2025"
             + cash_footer(exchanges),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_annual_bars(exchanges, filename, title, footer_universe):
    """Generate annual returns bar chart for given exchanges vs SPY."""
    why = undrawable(exchanges)
    if why:
        return skip_chart(filename, why)
    ex = data[exchanges[0]]
    years = [ar["year"] for ar in ex["annual_returns"]]
    spy_returns = [ar["spy"] for ar in ex["annual_returns"]]

    n_series = len(exchanges) + 1
    fig, ax = plt.subplots(figsize=(14, 5))

    width = 0.8 / n_series
    x = list(range(len(years)))
    offsets = [i - (n_series - 1) * width / 2 for i in x]

    ax.bar([o + 0 * width for o in offsets], spy_returns, width,
           label=benchmark_legend(data, exchanges[0]), color=COLORS["SPY"], alpha=0.7)

    for idx, ex_key in enumerate(exchanges):
        returns = [ar["portfolio"] for ar in data[ex_key]["annual_returns"]]
        note = thin_note(ex_key)
        ax.bar([o + (idx + 1) * width for o in offsets], returns, width,
               label=EXCHANGE_LABELS.get(ex_key, ex_key) + (f" ({note})" if note else ""),
               color=COLORS.get(ex_key, "#95a5a6"), alpha=0.85)
        mark_cash(ax, ex_key, {y: (i - 0.5, i + 0.5) for i, y in enumerate(years)})

    ax.set_ylabel("Annual Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(years, rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=9, loc="upper left", ncol=min(n_series, 3))
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.2, axis="y", linestyle="--")
    ax.set_axisbelow(True)
    stamp_thin(ax, exchanges)

    fig.text(0.5, -0.06,
             f"Data: Ceta Research | {footer_universe}, quarterly rebalance, equal weight, 2000-2025"
             + cash_footer(exchanges),
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def comparison_legs(metric):
    """Markets fit to rank on a metric, plus footer lines for those left out and for cash counts.

    Left out: never invested, invested in fewer than half the periods (too thin to test), or a
    measured window the benchmark cut short (window_truncated).
    """
    legs, thin, cut = [], [], []
    for k, v in data.items():
        if (not isinstance(v, dict) or (v.get("portfolio") or {}).get(metric) is None
                or periods(k)[1] <= 0):
            print(f"  Not ranked: {k} (no {metric} or no period counts)")
            continue
        name = EXCHANGE_LABELS.get(k, k).removeprefix("P/S ")
        note = thin_note(k)
        if note:
            thin.append(f"{name}, {note}")
        elif v.get("window_truncated", False):
            cut.append(name)
        else:
            legs.append((k, v))
    footer = []
    if thin:
        footer.append("Too thin to test, not ranked: " + "; ".join(thin) + ".")
    if cut:
        footer.append("Window cut short by the benchmark, not ranked: " + ", ".join(cut) + ".")
    for line in footer:
        print(f"  {line}")
    if any(periods(k)[0] < periods(k)[1] for k, v in legs):
        footer.append('"cash N of M qtrs": quarters with too few qualifying stocks.'
                      " A cash quarter earns 0% and counts in the CAGR and drawdown.")
    return legs, "".join("\n" + textwrap.fill(line, 150) for line in footer)


def leg_label(exchange_key):
    """Tick label, with the cash count for a market that sat out some quarters."""
    invested, total = periods(exchange_key)
    name = EXCHANGE_LABELS.get(exchange_key, exchange_key)
    return f"{name}, cash {total - invested} of {total} qtrs" if invested < total else name


def us_benchmark(metric):
    """(label, value) of the S&P 500 reference line. US entry only: any other entry's "spy" is its own index."""
    us = data.get("NYSE_NASDAQ_AMEX")
    value = ((us if isinstance(us, dict) else {}).get("spy") or {}).get(metric)
    return f"{benchmark_label(data, 'NYSE_NASDAQ_AMEX')}, USD", value


def chart_comparison_cagr(filename):
    """Horizontal bar chart: CAGR by exchange (all exchanges with data)."""
    exchanges_with_data, left_out = comparison_legs("cagr")
    if not exchanges_with_data:
        return skip_chart(filename, "no market to rank")
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["cagr"], reverse=True)

    names = [leg_label(k) for k, v in exchanges_with_data]
    cagrs = [v["portfolio"]["cagr"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5)))
    bars = ax.barh(range(len(names)), cagrs, color=colors, alpha=0.85, height=0.6)

    # Use US SPY CAGR as reference
    spy_name, spy_cagr = us_benchmark("cagr")
    if spy_cagr is not None:
        ax.axvline(x=spy_cagr, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"{spy_name} ({spy_cagr}% CAGR)")
        ax.legend(fontsize=10)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("CAGR (%)", fontsize=12, fontweight="bold")
    ax.set_title("Price-to-Sales CAGR by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, cagr) in enumerate(zip(bars, cagrs)):
        x_pos = max(cagr, 0) + 0.3
        ax.text(x_pos, i, f"{cagr:.1f}%", va="center", fontsize=9, fontweight="bold")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same P/S screen, quarterly rebalance, equal weight. Local currency returns."
             + left_out,
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def chart_comparison_drawdown(filename):
    """Horizontal bar chart: Max drawdown by exchange."""
    exchanges_with_data, left_out = comparison_legs("max_drawdown")
    if not exchanges_with_data:
        return skip_chart(filename, "no market to rank")
    exchanges_with_data.sort(key=lambda x: x[1]["portfolio"]["max_drawdown"], reverse=True)

    names = [leg_label(k) for k, v in exchanges_with_data]
    drawdowns = [v["portfolio"]["max_drawdown"] for k, v in exchanges_with_data]
    colors = [COLORS.get(k, "#95a5a6") for k, v in exchanges_with_data]

    fig, ax = plt.subplots(figsize=(12, max(6, len(names) * 0.5)))
    bars = ax.barh(range(len(names)), drawdowns, color=colors, alpha=0.85, height=0.6)

    spy_name, spy_dd = us_benchmark("max_drawdown")
    if spy_dd is not None:
        ax.axvline(x=spy_dd, color="#e74c3c", linewidth=1.5, linestyle="--",
                   label=f"{spy_name} ({spy_dd:.1f}%)")
        ax.legend(fontsize=10, loc="upper left")  # the deepest bar and its label fill the lower left

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlim(left=min(drawdowns + [spy_dd or 0]) - 8)  # room for the deepest bar's label
    ax.set_xlabel("Max Drawdown (%)", fontsize=12, fontweight="bold")
    ax.set_title("Price-to-Sales Max Drawdown by Exchange (2000-2025)", fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    for i, (bar, dd) in enumerate(zip(bars, drawdowns)):
        x_pos = dd - 1.5
        ax.text(x_pos, i, f"{dd:.1f}%", va="center", fontsize=9, fontweight="bold",
                ha="right")

    fig.text(0.5, -0.02,
             "Data: Ceta Research | Same P/S screen, quarterly rebalance, equal weight."
             + left_out,
             ha="center", va="top", fontsize=8, color="#7f8c8d")

    plt.tight_layout()
    out = charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


def generate_all():
    """Generate all charts based on available exchange data."""
    available = set(data.keys())
    print(f"Generating charts for {len(available)} exchanges: {', '.join(sorted(available))}")

    for key, universe in PRESET_UNIVERSE.items():
        if (data.get(key) or {}).get("universe", universe) != universe:
            print(f"  WARNING: {key} holds universe {data[key]['universe']!r}, not the {universe} preset"
                  " its labels describe")

    # A market missing from the comparison file is skipped by name, never silently
    for key, region, country, universe in REGIONS:
        print(f"\nGenerating charts for blogs/{region}/...")
        if (data.get(key) or {}).get("window_truncated"):
            print(f"  WARNING: {key} measured {data[key].get('window_label')}; chart titles say 2000-2025")
        bench = benchmark_label(data, key)
        chart_cumulative(
            [key], f"{region}_cumulative_growth.png",
            f"Growth of $10,000: P/S Value {country} vs {bench} (2000-2025)",
            universe.replace(")", ", benchmark in USD)") if is_usd_benchmark_proxy(data, key) else universe
        )
        chart_annual_bars(
            [key], f"{region}_annual_returns.png",
            f"P/S Value {country} vs {bench}: Year-by-Year Returns (2000-2025)",
            universe
        )

    # Comparison charts (all exchanges)
    print("\nGenerating comparison charts...")
    chart_comparison_cagr("comparison_cagr.png")
    chart_comparison_drawdown("comparison_drawdown.png")

    print(f"\nDone. Charts saved to {charts_dir}/")
    print("Copy them to the content repo with: python3 scripts/copy_charts_to_content.py price-to-sales --apply")


if __name__ == "__main__":
    generate_all()
