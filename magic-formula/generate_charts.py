"""Generate all Magic Formula charts for blog posts."""
import matplotlib.pyplot as plt
import os as _cu_os, sys as _cu_sys
_cu_sys.path.insert(0, _cu_os.path.dirname(_cu_os.path.dirname(_cu_os.path.abspath(__file__))))
from chart_utils import (benchmark_label, benchmark_legend, benchmark_money, localize_money_title,
                         market_name, money, money_axis_label, money_formatter, not_ranked_lines,
                         own_index_counts, own_index_verdict, save_own_index_chart)
import matplotlib.ticker as mticker
import json
from pathlib import Path

results_dir = Path(__file__).parent / "results"
content_charts_dir = Path(__file__).parent / "charts"
content_charts_dir.mkdir(exist_ok=True)

# Load individual result files (have annual_returns for growth charts)
individual = {}
for p in results_dir.glob("magic_formula_*.json"):
    key = p.stem.replace("magic_formula_", "")
    with open(p) as f:
        individual[key] = json.load(f)
# The India and Indonesia posts quote the factor-01_* reruns (India NSE-only);
# the magic_formula_* files for those two are older runs (India BSE+NSE).
for key, name in {"India": "factor-01_nse.json", "JKT": "factor-01_jkt.json"}.items():
    with open(results_dir / name) as f:
        individual[key] = json.load(f)

# Color palette
COLORS = {
    "US_MAJOR": "#1a5276",
    "India": "#e67e22",
    "JKT": "#27ae60",
    "SAO": "#c0392b",
    "Canada": "#7f8c8d",
    "China": "#e74c3c",
    "HKSE": "#8e44ad",
    "KSC": "#95a5a6",
    "ASX": "#bdc3c7",
    "XETRA": "#2980b9",
    "STO": "#16a085",
    "SET": "#d35400",
    "TAI": "#f39c12",
    "TLV": "#7fb3d8",
    "SAU": "#2c3e50",
    "PAR": "#ecf0f1",
    "SPY": "#aab7b8",
}

EXCHANGE_LABELS = {
    "US_MAJOR": "Magic Formula US",
    "India": "Magic Formula India (NSE)",
    "JKT": "Magic Formula Indonesia (JKT)",
    "SAO": "Magic Formula Brazil (SAO)",
    "Canada": "Magic Formula Canada",
    "China": "Magic Formula China",
    "HKSE": "Magic Formula Hong Kong",
    "KSC": "Magic Formula Korea",
    "ASX": "Magic Formula Australia",
    "XETRA": "Magic Formula Germany",
    "STO": "Magic Formula Sweden",
    "SET": "Magic Formula Thailand",
    "TAI": "Magic Formula Taiwan",
    "TLV": "Magic Formula Israel",
    "SAU": "Magic Formula Saudi Arabia",
    "PAR": "Magic Formula France",
}

FOOTER = "Data: Ceta Research | Magic Formula (Rank EY + Rank ROCE, top 30), quarterly rebalance, 2000-2025"


def get_cumulative_growth(exchange_key, initial=10000):
    """Compute cumulative growth from annual returns in individual result file."""
    if exchange_key not in individual:
        return [], []
    ex = individual[exchange_key]
    if "annual_returns" not in ex or not ex["annual_returns"]:
        return [], []
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["portfolio"] / 100))
        years.append(ar["year"])
    return years, values


def get_spy_cumulative(exchange_key="US_MAJOR", initial=10000):
    """Get SPY cumulative from an individual result file."""
    if exchange_key not in individual:
        return [], []
    ex = individual[exchange_key]
    if "annual_returns" not in ex or not ex["annual_returns"]:
        return [], []
    values = [initial]
    years = [ex["annual_returns"][0]["year"] - 1]
    for ar in ex["annual_returns"]:
        values.append(values[-1] * (1 + ar["spy"] / 100))
        years.append(ar["year"])
    return years, values


def chart_cumulative(exchanges, filename, title, footer_universe):
    """Generate cumulative growth chart for given exchanges vs SPY."""
    fig, ax = plt.subplots(figsize=(12, 6))

    # Use the first available exchange for SPY data
    spy_source = exchanges[0] if exchanges else "US_MAJOR"
    spy_years, spy_vals = get_spy_cumulative(spy_source)
    if spy_years:
        spy_cagr = individual.get(spy_source, {}).get("spy", {}).get("cagr", "?")
        ax.plot(spy_years, spy_vals, color=COLORS["SPY"], linewidth=1.8,
                label=f"{benchmark_legend(individual, spy_source)} ({spy_cagr}% CAGR)", linestyle="--")

    for ex_key in exchanges:
        if ex_key not in individual:
            continue
        years, vals = get_cumulative_growth(ex_key)
        if not years:
            continue
        cagr = individual[ex_key]["portfolio"]["cagr"]
        label = f"{EXCHANGE_LABELS.get(ex_key, ex_key)} ({cagr}% CAGR)"
        ax.plot(years, vals, color=COLORS.get(ex_key, "#95a5a6"), linewidth=2.2, label=label)

        final_k = vals[-1] / 1000
        ax.annotate(money(final_k, exchanges[0], suffix="K"),
                    xy=(years[-1], vals[-1]),
                    xytext=(8, 0), textcoords="offset points",
                    fontsize=9, fontweight="bold", color=COLORS.get(ex_key, "#95a5a6"))

    if spy_years:
        spy_final_k = spy_vals[-1] / 1000
        # S&P-proxy line is in USD, not the exchange's currency
        ax.annotate(benchmark_money(spy_final_k, individual, exchanges[0], suffix="K"),
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
    out = content_charts_dir / filename
    plt.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
    print(f"  Saved: {out}")
    plt.close()


# The comparison's 16 markets from the current factor-01 run (India NSE-only, China SHH)
COMPARISON_FILES = {
    "US_MAJOR": "factor-01_us.json", "NSE": "factor-01_nse.json", "LSE": "factor-01_lse.json",
    "STO": "factor-01_sto.json", "HKSE": "factor-01_hkse.json", "SIX": "factor-01_six.json",
    "TAI": "factor-01_tai.json", "XETRA": "factor-01_xetra.json", "JPX": "factor-01_jpx.json",
    "KSC": "factor-01_ksc.json", "SET": "factor-01_set.json", "TSX": "factor-01_tsx.json",
    "SHH": "factor-01_shh.json", "JNB": "factor-01_jnb.json", "JKT": "factor-01_jkt.json",
    "SAU": "factor-01_sau.json",
}
# Leads over a price index below the bottom of the 1.3%-3.5% dividend range: ties
DIVIDEND_TIES = {"SHH", "SET", "TSX"}
# Leads over a price index inside that range, which the comparison post doesn't call
NOT_CALLED = {"TAI", "JPX", "KSC"}


def chart_comparison_cagr(filename):
    """CAGR by exchange, each against its own index."""
    legs = {}
    for key, name in COMPARISON_FILES.items():
        with open(results_dir / name) as f:
            legs[key] = json.load(f)
    keys = sorted(legs, key=lambda k: legs[k]["portfolio"]["cagr"], reverse=True)
    verdicts = [own_index_verdict(legs, k, dividend_ties=DIVIDEND_TIES, not_called=NOT_CALLED)
                for k in keys]
    c = own_index_counts(verdicts)
    ties = [market_name(k, short=True) for k, v in zip(keys, verdicts) if v == "div_tie"]
    uncalled = [market_name(k, short=True) for k, v in zip(keys, verdicts) if v == "not_called"]
    beat = ("Every market with a local index leads it as measured."
            if c["beat"] == c["ranked"] else
            f"{c['beat']} of the {c['ranked']} markets with a local index lead it as measured.")
    save_own_index_chart(
        legs, keys, [market_name(k) for k in keys], content_charts_dir / filename,
        "Magic Formula: CAGR vs Own Index by Exchange (2000-2025)",
        [FOOTER + ".",
         f"Each market in its own currency against its own index. {beat}",
         f"{', '.join(ties[:-1])} and {ties[-1]} lead by less than the bottom of the 1.3%-3.5% "
         "dividend range their price indices leave out: ties.",
         f"{', '.join(uncalled[:-1])} and {uncalled[-1]} lead by amounts within the 1.3%-3.5% "
         "dividend range, not called either way."]
        + not_ranked_lines(legs, keys, verdicts)
        + ["Not shown: Brazil and Australia, excluded for split-adjustment errors in their price data."],
        dividend_ties=DIVIDEND_TIES, not_called=NOT_CALLED)


# ---- Generate all charts ----

print("Generating Magic Formula charts...")
print()

print("US cumulative growth...")
chart_cumulative(
    ["US_MAJOR"], "us_cumulative_growth.png",
    "Growth of $10,000: Magic Formula US vs S&P 500 (2000-2025)",
    "NYSE + NASDAQ + AMEX, quarterly rebalance, equal weight, top 30"
)

print("India cumulative growth...")
chart_cumulative(
    ["India"], "india_cumulative_growth.png",
    f"Growth of $10,000: Magic Formula India vs {benchmark_label(individual, 'India')} (2000-2025)",
    "NSE, quarterly rebalance, equal weight, top 30"
)

print("Indonesia (JKT) cumulative growth...")
chart_cumulative(
    ["JKT"], "jkt_cumulative_growth.png",
    f"Growth of $10,000: Magic Formula Indonesia vs {benchmark_label(individual, 'JKT')} (2000-2025)",
    "Jakarta Stock Exchange (returns in IDR, benchmark in USD), quarterly rebalance, equal weight, top 30"
)

print("Brazil (SAO) cumulative growth...")
chart_cumulative(
    ["SAO"], "brazil_cumulative_growth.png",
    f"Growth of $10,000: Magic Formula Brazil vs {benchmark_label(individual, 'SAO')} (2000-2025)",
    "B3 (Bovespa), quarterly rebalance, equal weight, top 30"
)

print("Comparison CAGR chart...")
chart_comparison_cagr("comparison_cagr.png")

print(f"\nDone. Charts in {content_charts_dir}/")
