"""Shared chart helpers.

The one job here is getting the benchmark right. After the local-benchmark
reruns, each results entry's "spy" field holds whichever index that exchange
was actually measured against, which for non-US markets is the local index.
Chart code that hardcodes "S&P 500" therefore mislabels the line, and chart
code that pulls the series from a hardcoded US key plots the wrong data
entirely.

Use `benchmark_label(data, key)` for the legend and `benchmark_cumulative(
data, key)` for the series, both keyed on the exchange being charted.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_utils import LOCAL_INDEX_BENCHMARKS, LOCAL_INDEX_NAMES, LOCAL_CURRENCY

# Results files key exchanges inconsistently across topics. Normalise to the
# exchange codes used by LOCAL_INDEX_BENCHMARKS so a benchmark name can be
# recovered even when the results predate the benchmark_name field.
RESULT_KEY_TO_EXCHANGE = {
    # Lowercase region slugs: some generators pass the blog's directory slug
    # ("india", "uk") rather than an exchange code. Resolve those too, so a
    # money axis does not silently fall back to unlabelled.
    "us": "NYSE", "usa": "NYSE", "canada": "TSX", "uk": "LSE", "britain": "LSE",
    "germany": "XETRA", "india": "NSE", "japan": "JPX", "hongkong": "HKSE",
    "hong_kong": "HKSE", "china": "SHH", "korea": "KSC", "taiwan": "TAI",
    "sweden": "STO", "switzerland": "SIX", "thailand": "SET", "brazil": "SAO",
    "southafrica": "JNB", "south_africa": "JNB", "australia": "ASX",
    "norway": "OSL", "italy": "MIL", "malaysia": "KLS", "indonesia": "JKT",
    "singapore": "SGX", "netherlands": "AMS", "france": "PAR", "spain": "BME",
    "israel": "TLV", "poland": "WSE", "saudi": "SAU",

    "US_MAJOR": "NYSE", "NYSE_NASDAQ_AMEX": "NYSE", "US": "NYSE",
    "NYSE_NASDAQ": "NYSE", "NYSE": "NYSE", "NASDAQ": "NASDAQ", "AMEX": "AMEX",
    "Canada": "TSX", "TSX": "TSX",
    "UK": "LSE", "LSE": "LSE",
    "Germany": "XETRA", "XETRA": "XETRA",
    "India": "NSE", "NSE": "NSE", "BSE": "BSE", "BSE_NSE": "NSE",
    "Japan": "JPX", "JPX": "JPX",
    "HKSE": "HKSE", "HongKong": "HKSE", "Hong Kong": "HKSE",
    "China": "SHH", "SHH": "SHH", "SHZ": "SHZ", "SHZ_SHH": "SHH",
    "Korea": "KSC", "KSC": "KSC",
    "Taiwan": "TAI", "TAI": "TAI", "TWO": "TWO",
    "Switzerland": "SIX", "SIX": "SIX",
    "Sweden": "STO", "STO": "STO",
    "Norway": "OSL", "OSL": "OSL",
    "Thailand": "SET", "SET": "SET",
    "Australia": "ASX", "ASX": "ASX",
    "Brazil": "SAO", "SAO": "SAO",
    "JSE": "JNB", "JNB": "JNB",
    # Multi-exchange keys. Without these the lookup misses and the label
    # silently falls back to "S&P 500" with no error, so the chart stays
    # wrong while looking fixed. TAI_TWO alone appears in 24 topics.
    "Singapore": "SES", "SES": "SES", "SGX": "SES",
    "TAI_TWO": "TAI", "TAI+TWO": "TAI",
    "SHH_SHZ": "SHH", "SHH+SHZ": "SHH",
    "TSX+TSXV": "TSX",
    "AMEX+NASDAQ+NYSE": "NYSE",
    # Deliberately NOT mapped, because these markets have no local index in
    # LOCAL_INDEX_BENCHMARKS and their backtests genuinely ran against SPY
    # (verified: MIL and KLS record benchmark_name "S&P 500"). Falling through
    # to the default is the correct answer for them, not an oversight:
    #   KLS MIL SAU JKT TLV WSE PAR AMS BME
}


def _clean_benchmark_name(name):
    """Turn whatever the backtest recorded into something fit for a legend.

    Topics record this field three different ways: a friendly name
    ("FTSE 100"), a raw symbol ("^TWII"), or both ("FTSE 100 (^FTSE)").
    Printed verbatim the last two give legends like "^TWII (4.1% CAGR)" and
    "FTSE 100 (^FTSE) (7.8% CAGR)".
    """
    name = name.strip()
    if name in LOCAL_INDEX_NAMES:              # a raw symbol like ^TWII
        return LOCAL_INDEX_NAMES[name]
    if name.endswith(")") and " (" in name:    # "FTSE 100 (^FTSE)"
        head, _, tail = name.rpartition(" (")
        if tail[:-1].startswith("^") or tail[:-1] in LOCAL_INDEX_NAMES:
            return head
    return name


def benchmark_label(data, exchange_key, default="S&P 500"):
    """Human name of the benchmark an exchange was measured against.

    Prefers the benchmark_name recorded by the backtest. Falls back to the
    local index for that exchange, then to `default` for exchanges that
    genuinely used SPY (no local index available at run time).
    """
    entry = (data or {}).get(exchange_key) or {}
    name = entry.get("benchmark_name") or entry.get("benchmark")
    if isinstance(name, str) and name:
        return _clean_benchmark_name(name)
    ex = RESULT_KEY_TO_EXCHANGE.get(exchange_key, exchange_key)
    symbol = LOCAL_INDEX_BENCHMARKS.get(ex)
    if symbol:
        return LOCAL_INDEX_NAMES.get(symbol, symbol)
    return default


def benchmark_cumulative(data, exchange_key, initial=10000, period_end=False):
    """Cumulative growth of THAT exchange's own benchmark series.

    Returns (years, values). Empty lists if the exchange has no annual returns.

    annual_returns "year" is the year a holding period STARTS. period_end=True
    puts `initial` at the first period's start year and each value at its
    period's end (year + 1), to pair with a portfolio curve built as
    [years[0]] + [y + 1 for y in years]. The default is the legacy axis, one
    year early (first year - 1, then each period's own year), kept for the
    generators whose portfolio curve still uses it.
    """
    entry = (data or {}).get(exchange_key) or {}
    annual = entry.get("annual_returns") or []
    if not annual:
        return [], []
    shift = 1 if period_end else 0
    values = [initial]
    years = [annual[0]["year"] - 1 + shift]
    for ar in annual:
        values.append(values[-1] * (1 + ar.get("spy", 0) / 100))
        years.append(ar["year"] + shift)
    return years, values


def benchmark_cagr(data, exchange_key):
    """Benchmark CAGR for an exchange, or None."""
    return ((data or {}).get(exchange_key) or {}).get("spy", {}).get("cagr")


# ---------------------------------------------------------------------------
# S&P 500 (USD) standing in for a missing local index.
#
# Exchanges with no local index (JNB, JKT, MIL, KLS, SAU, TLV, ...) ran against
# SPY, and so did some stale runs on exchanges that do have one. That line is
# in USD while the strategy line is local, so money() would print "ZAR 66K" for
# a US$66K value. Use benchmark_money() / benchmark_legend() for the benchmark.
# ---------------------------------------------------------------------------

_US_RESULT_KEYS = ("US_MAJOR", "NYSE_NASDAQ_AMEX", "NYSE_NASDAQ", "US")


def _benchmark_series(entry):
    """{year: benchmark return in percent} from an entry's annual_returns."""
    series = {}
    for ar in (entry or {}).get("annual_returns") or []:
        if not isinstance(ar, dict):
            continue
        v = ar.get("spy", ar.get("benchmark"))
        try:
            year = int(ar.get("year"))
        except (TypeError, ValueError):
            continue
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            series[year] = float(v)
    if series and max(abs(v) for v in series.values()) < 1.5:  # fractions
        series = {y: v * 100 for y, v in series.items()}
    return series


def _series_matches_us(data, exchange_key, tol=0.02, min_years=5):
    """True/False if the entry's benchmark series is/isn't the US entry's, else None.

    Year-aligned and scale-aware (as scripts/check_rerun_vintage.py): windows
    differ across entries, and some topics store fractions. Only the first and
    last shared years may differ (partial periods: net-debt-ebitda's 2025 is
    15.34 on every non-US entry vs 15.47 on the US one); a local index misses
    nearly every year.
    """
    series = _benchmark_series((data or {}).get(exchange_key))
    if not series:
        return None
    verdict = None
    for us_key in _US_RESULT_KEYS:
        if us_key == exchange_key:
            continue
        us = _benchmark_series((data or {}).get(us_key))
        shared = sorted(set(series) & set(us))
        if len(shared) < min_years:
            continue
        misses = {y for y in shared if abs(series[y] - us[y]) > tol}
        if (misses <= {shared[0], shared[-1]}
                and len(shared) - len(misses) >= min_years):
            return True
        verdict = False
    return verdict


def _recorded_benchmark_is_spy(entry):
    """True if the recorded benchmark is SPY/S&P 500, False if it names another, None if none."""
    names = [
        (entry or {}).get(f)
        for f in ("benchmark_symbol", "benchmark_name", "benchmark")
    ]
    names = [n for n in names if isinstance(n, str) and n.strip()]
    if not names:
        return None
    return any(_clean_benchmark_name(n) == "S&P 500" for n in names)


def _has_local_index(exchange_key):
    """Whether LOCAL_INDEX_BENCHMARKS maps the key (resolved like currency_code)."""
    key = str(exchange_key)
    if key.endswith("_dom"):
        key = key[:-4]
    ex = RESULT_KEY_TO_EXCHANGE.get(key)
    if ex is not None:
        return LOCAL_INDEX_BENCHMARKS.get(ex) is not None
    parts = [p for p in key.replace("+", "_").split("_") if p]
    return any(
        LOCAL_INDEX_BENCHMARKS.get(RESULT_KEY_TO_EXCHANGE.get(p, p)) for p in parts
    )


def is_usd_benchmark_proxy(data, exchange_key):
    """True when a non-USD exchange's benchmark line is the S&P 500 (so in USD).

    Evidence order: (a) a recorded SPY/S&P 500 benchmark; (b) the series test
    against the US entry in `data`: a match beats a recorded local name (those
    were hand-stamped over SPY series), a mismatch counts only where a local
    index exists or was recorded; (c) no local index for the key. A recorded
    local name with no per-year series to test means False.
    """
    code = currency_code(exchange_key)
    if code is None or code == "USD":
        return False
    recorded = _recorded_benchmark_is_spy((data or {}).get(exchange_key))
    if recorded:
        return True
    matched = _series_matches_us(data, exchange_key)
    if matched is True:
        return True
    # A mismatch can be a different US-file vintage; it rules out a proxy only
    # where a local index exists or was recorded.
    if matched is False and (recorded is False or _has_local_index(exchange_key)):
        return False
    if recorded is False:
        return False
    return not _has_local_index(exchange_key)


def benchmark_money(value, data, exchange_key, suffix="", decimals=0):
    """Benchmark end label: 'US$66K' for an S&P 500 proxy, else money()."""
    if is_usd_benchmark_proxy(data, exchange_key):
        return f"US${value:,.{decimals}f}{suffix}"
    return money(value, exchange_key, decimals=decimals, suffix=suffix)


def benchmark_legend(data, exchange_key):
    """Benchmark legend name: 'S&P 500, USD' for an S&P 500 proxy, else benchmark_label()."""
    if is_usd_benchmark_proxy(data, exchange_key):
        # not benchmark_label(): it guesses the local index ("OMX Stockholm 30")
        # for SPY runs on exchanges that have one
        return "S&P 500, USD"
    return benchmark_label(data, exchange_key)


# ---------------------------------------------------------------------------
# Currency on money-denominated axes.
#
# Backtest returns are in the exchange's LOCAL currency, so a cumulative-growth
# chart's axis, ticks and end annotations must be too. Hardcoding "$" produced
# charts reading "Growth of $10,000 ... $149K" over an INR series whose own
# footer said "returns in INR" — the plotted data was right, but a reader taking
# the unit literally overstates the implied wealth by the FX rate (87x for INR,
# 147x for JPY, 1380x for KRW).
#
# Percentage charts (annual returns, CAGR bars, drawdown) are currency-free and
# need none of this.
# ---------------------------------------------------------------------------

# ISO code -> display prefix. Symbols only where they are unambiguous to a
# global reader; otherwise the ISO code plus a space, which is always readable.
# "$" alone is reserved for USD.
CURRENCY_PREFIX = {
    "USD": "$",
    "INR": "Rs",
    "BRL": "R$",
    "CAD": "C$",
    "AUD": "A$",
    "SGD": "S$",
    "EUR": "EUR ",
    "GBP": "GBP ",
    "JPY": "JPY ",
    "SEK": "SEK ",
    "CHF": "CHF ",
    "HKD": "HKD ",
    "KRW": "KRW ",
    "TWD": "TWD ",
    "THB": "THB ",
    "ZAR": "ZAR ",
    "CNY": "CNY ",
    "NOK": "NOK ",
    "MYR": "MYR ",
    "IDR": "IDR ",
    "ILS": "ILS ",
    "PLN": "PLN ",
    "SAR": "SAR ",
}

_warned_currency = set()


def currency_code(exchange_key):
    """ISO currency code for a results key, or None if it cannot be resolved.

    Handles the key shapes results files actually use: plain codes ("STO"),
    composites ("SHZ_SHH", "TAI+TWO", "NYSE_NASDAQ_AMEX"), the domicile
    variants ("LSE_dom") and country names ("India").
    """
    if not exchange_key:
        return None
    key = str(exchange_key)
    if key.endswith("_dom"):
        key = key[:-4]
    ex = RESULT_KEY_TO_EXCHANGE.get(key)
    if ex is None:
        # composite like SHZ_SHH / TAI+TWO / TSX+TSXV: every leg should agree
        parts = [p for p in key.replace("+", "_").split("_") if p]
        codes = {
            LOCAL_CURRENCY.get(RESULT_KEY_TO_EXCHANGE.get(p, p))
            for p in parts
        }
        codes.discard(None)
        if len(codes) == 1:
            return codes.pop()
        ex = key
    return LOCAL_CURRENCY.get(ex)


def currency_prefix(exchange_key):
    """Display prefix for money on `exchange_key`'s charts.

    Returns "" for an unresolved key rather than defaulting to "$". An
    unlabelled number is merely uninformative; a wrongly-labelled one asserts
    something false, which is the bug this exists to kill. Unresolved keys warn
    once so they get added to LOCAL_CURRENCY rather than silently degrading.
    """
    code = currency_code(exchange_key)
    if code is None:
        if exchange_key not in _warned_currency:
            _warned_currency.add(exchange_key)
            print(
                f"  chart_utils: no currency for {exchange_key!r}; money axis "
                f"will be unlabelled. Add it to data_utils.LOCAL_CURRENCY."
            )
        return ""
    return CURRENCY_PREFIX.get(code, code + " ")


def money(value, exchange_key, decimals=0, suffix=""):
    """Format `value` as money in `exchange_key`'s currency."""
    return f"{currency_prefix(exchange_key)}{value:,.{decimals}f}{suffix}"


def money_formatter(exchange_key, decimals=0):
    """A matplotlib tick formatter for a money axis in the local currency.

    Usage: ax.yaxis.set_major_formatter(money_formatter(ex_key))
    """
    import matplotlib.ticker as _mticker

    prefix = currency_prefix(exchange_key)
    return _mticker.FuncFormatter(
        lambda x, p: f"{prefix}{x:,.{decimals}f}"
    )


def money_axis_label(exchange_key, base="Portfolio Value"):
    """Y-axis label carrying the currency, e.g. 'Portfolio Value (Rs)'."""
    code = currency_code(exchange_key)
    if code is None:
        return base
    return f"{base} ({CURRENCY_PREFIX.get(code, code).strip()})"


_MONEY_TITLE = re.compile(r"\$(\d[\d,]*)")


def localize_money_title(title, exchange_key):
    """Swap a '$10,000' style figure in a chart title into the local currency.

    Titles are usually literal strings passed in from the call sites
    ("Growth of $10,000: Strategy India vs Sensex"), so rewriting them here
    keeps the fix to one place per generator instead of one per chart.
    """
    if not title or "$" not in title:
        return title
    prefix = currency_prefix(exchange_key)
    if prefix == "$":
        return title
    return _MONEY_TITLE.sub(lambda m: f"{prefix}{m.group(1)}", title)


# ---------------------------------------------------------------------------
# Comparison charts: each market against its own index, never one S&P 500 line.
# ---------------------------------------------------------------------------

TIE_BAND = 0.2  # excess CAGR points, either way, that count as a tie

OWN_INDEX_STYLE = {  # verdict -> (colour, hatch, legend label)
    "ahead": ("#27ae60", None, "Beat its own index"),
    "dividend": ("#f0b429", None, "Ahead by less than the dividends its index leaves out"),
    "tie": ("#d5d8dc", None, f"Within {TIE_BAND} pts of its own index (tie)"),
    "behind": ("#c0392b", None, "Trailed its own index"),
    "no_index": ("#9e9e9e", "//", "No local index in the data: not ranked"),
    "thin": ("#9e9e9e", "..", "Too thin to test: not ranked"),
}
RANKED_VERDICTS = ("ahead", "dividend", "tie", "behind")


def leg_excess(entry):
    """Excess CAGR over the leg's own benchmark, as the results file records it."""
    for v in (entry.get("excess_cagr"), (entry.get("comparison") or {}).get("excess_cagr")):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return v
    return round(entry["portfolio"]["cagr"] - entry["spy"]["cagr"], 2)


def leg_invested(entry):
    """(invested, total) periods, or None if the file doesn't record them."""
    invested = entry.get("invested_periods")
    total = entry.get("total_rebalances") or entry.get("n_periods")
    if invested is None or not total:
        return None
    return invested, total


def leg_is_too_thin(entry):
    """Invested in fewer than half its rebalance periods (METHODOLOGY)."""
    counts = leg_invested(entry)
    return counts is not None and counts[0] < counts[1] / 2


def own_index_verdict(data, key, not_clear_win=()):
    """'ahead', 'dividend', 'tie', 'behind', 'no_index' or 'thin' for one leg."""
    entry = data[key]
    if is_usd_benchmark_proxy(data, key):
        return "no_index"
    if leg_is_too_thin(entry):
        return "thin"
    excess = leg_excess(entry)
    if abs(excess) < TIE_BAND:
        return "tie"
    if excess < 0:
        return "behind"
    return "dividend" if key in not_clear_win else "ahead"


MARKET_NAMES = {  # results key -> (market, exchange label) for comparison charts
    "NYSE_NASDAQ_AMEX": ("US", "NYSE+NASDAQ+AMEX"), "NSE": ("India", "NSE"),
    "XETRA": ("Germany", "XETRA"), "STO": ("Sweden", "STO"), "TSX": ("Canada", "TSX"),
    "SHZ_SHH": ("China", "SHZ+SHH"), "SHH_SHZ": ("China", "SHH+SHZ"), "HKSE": ("Hong Kong", "HKSE"),
    "JPX": ("Japan", "JPX"), "LSE": ("UK", "LSE"), "KSC": ("Korea", "KSC"),
    "SIX": ("Switzerland", "SIX"), "TAI": ("Taiwan", "TAI"), "TAI_TWO": ("Taiwan", "TAI+TWO"),
    "SET": ("Thailand", "SET"), "JNB": ("South Africa", "JNB"), "SES": ("Singapore", "SES"),
    "KLS": ("Malaysia", "KLS"), "OSL": ("Norway", "OSL"), "MIL": ("Italy", "MIL"),
    "JKT": ("Indonesia", "JKT"), "SAU": ("Saudi Arabia", "SAU"), "WSE": ("Poland", "WSE"),
    "SAO": ("Brazil", "SAO"),
}


def market_name(key, short=False):
    """'UK (LSE)', or 'UK' with short=True."""
    market, exchange = MARKET_NAMES.get(key, (key, None))
    return market if short or not exchange else f"{market} ({exchange})"


def _join_names(names):
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def not_ranked_lines(data, keys, verdicts):
    """Footnote lines naming the unranked legs and why."""
    lines = []
    no_index = [market_name(k, short=True) for k, v in zip(keys, verdicts) if v == "no_index"]
    if no_index:
        lines.append(f"Not ranked: {_join_names(no_index)} {'has' if len(no_index) == 1 else 'have'} "
                     "no local index in the data, so the run used the S&P 500 in USD, "
                     "which isn't comparable with a local-currency return.")
    thin = [f"{market_name(k, short=True)} ({leg_invested(data[k])[0]} of {leg_invested(data[k])[1]})"
            for k, v in zip(keys, verdicts) if v == "thin"]
    if thin:
        lines.append(f"Not ranked, too thin to test (invested in fewer than half its periods): "
                     f"{_join_names(thin)}.")
    return lines


def short_window_lines(data, keys):
    """Footnote naming legs measured over a shorter window, which aren't drawn."""
    parts = []
    for k in keys:
        entry = data[k]
        if not entry.get("window_truncated"):
            continue
        start, end = entry.get("measured_start"), entry.get("measured_end")
        span = f"{start[:4]}-{end[:4]}" if start and end else entry.get("window_label", "")
        parts.append(f"{market_name(k, short=True)} (measured {span} only)")
    if not parts:
        return []
    return [f"Not shown: {_join_names(parts)}, because its index covers only part of the "
            "window, so it isn't ranked with full-window markets."]


def own_index_counts(verdicts):
    """Counts for a caption: ranked legs, legs ahead as measured, clear leads."""
    ranked = [v for v in verdicts if v in RANKED_VERDICTS]
    return {
        "ranked": len(ranked),
        "beat": sum(v in ("ahead", "dividend") for v in ranked),
        "clear": sum(v == "ahead" for v in ranked),
        "behind": sum(v == "behind" for v in ranked),
        "tie": sum(v == "tie" for v in ranked),
    }


def save_own_index_chart(data, keys, names, out_path, title, footer_lines,
                         metric="cagr", not_clear_win=(), bar_color="#5d6d7e",
                         figsize=None, decimals=2):
    """Horizontal bars, one per leg, each with its own index as a black tick.

    metric "cagr": bars coloured by own_index_verdict(). metric "max_drawdown":
    bars take `bar_color`. Unranked legs are grey and hatched and get no tick.
    `keys` are drawn top to bottom in the order given. Returns the verdicts.
    """
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    verdicts = [own_index_verdict(data, k, not_clear_win) for k in keys]
    values = [data[k]["portfolio"][metric] for k in keys]
    benches = [data[k]["spy"][metric] for k in keys]
    unranked = [v not in RANKED_VERDICTS for v in verdicts]

    fig, ax = plt.subplots(figsize=figsize or (12, max(5.5, 0.5 * len(keys) + 2)))
    for i, (val, verdict) in enumerate(zip(values, verdicts)):
        colour, hatch, _ = OWN_INDEX_STYLE[verdict]
        if metric != "cagr" and verdict in RANKED_VERDICTS:
            colour = bar_color
        ax.barh(i, val, color=colour, alpha=0.85, height=0.6, hatch=hatch,
                edgecolor="white" if hatch else None, linewidth=0)
    ticked = [(b, i) for i, (b, u) in enumerate(zip(benches, unranked)) if not u]
    if ticked:
        ax.scatter([b for b, _ in ticked], [i for _, i in ticked], marker="|", s=500,
                   linewidths=3, color="black", zorder=3)

    lo, hi = min(values + benches + [0]), max(values + benches + [0])
    span = hi - lo
    pad = span * 0.012
    if metric == "cagr":
        ax.set_xlim(min(lo * 1.15, 0), hi + span * 0.05)
    else:
        ax.set_xlim(lo - span * 0.05, 0)
    labels = []
    for i, (k, val, b, verdict) in enumerate(zip(keys, values, benches, verdicts)):
        if verdict == "no_index":
            note = "no local index"
        elif verdict == "thin":
            inv, tot = leg_invested(data[k])
            note = f"too thin: invested {inv} of {tot} periods"
        elif metric == "cagr":
            note = f"{leg_excess(data[k]):+.2f} vs {benchmark_label(data, k)}"
        else:
            note = f"{benchmark_label(data, k)} {b:.{decimals}f}%"
        text = f"{val:.{decimals}f}% ({note})"
        if metric == "cagr":
            labels.append(ax.text(max(val, b if not unranked[i] else val, 0) + pad, i, text,
                                  va="center", fontsize=9))
        else:
            labels.append(ax.text(min(val, b if not unranked[i] else val, 0) - pad, i, text,
                                  va="center", ha="right", fontsize=9))

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    if metric == "cagr":
        ax.set_xlabel("CAGR (%, each market in its own currency)", fontsize=12, fontweight="bold")
    else:
        ax.set_xlabel("Max drawdown (%, each market in its own currency)", fontsize=12,
                      fontweight="bold")
    # widen the axis until every bar label fits (repeat: widening changes the scale)
    plt.tight_layout()
    for _ in range(6):
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        to_data = ax.transData.inverted()
        edges = [to_data.transform(t.get_window_extent(renderer))[:, 0] for t in labels]
        x0, x1 = ax.get_xlim()
        if metric == "cagr":
            need = max(e[1] for e in edges) + span * 0.02
            if need <= x1:
                break
            ax.set_xlim(x0, need)
        else:
            need = min(e[0] for e in edges) - span * 0.02
            if need >= x0:
                break
            ax.set_xlim(need, x1)
    ax.axvline(0, color="black", linewidth=0.6)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.grid(True, alpha=0.3, axis="x", linestyle="--")
    ax.set_axisbelow(True)

    handles = []
    if metric == "cagr":
        for verdict in OWN_INDEX_STYLE:
            if verdict in verdicts:
                colour, hatch, label = OWN_INDEX_STYLE[verdict]
                handles.append(Patch(facecolor=colour, alpha=0.85, hatch=hatch,
                                     edgecolor="white" if hatch else colour, label=label))
    else:
        if not all(unranked):
            handles.append(Patch(color=bar_color, alpha=0.85, label="Strategy max drawdown"))
        for verdict in ("no_index", "thin"):
            if verdict in verdicts:
                colour, hatch, label = OWN_INDEX_STYLE[verdict]
                handles.append(Patch(facecolor=colour, alpha=0.85, hatch=hatch,
                                     edgecolor="white", label=label))
    if ticked:
        handles.append(Line2D([], [], color="black", marker="|", linestyle="none", markersize=14,
                              markeredgewidth=3,
                              label="Its own index" + (" CAGR" if metric == "cagr" else
                                                       " max drawdown")))
    # first corner where the legend covers no bar, tick or label; else below the axes
    plt.tight_layout()
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    taken = [t.get_window_extent(renderer) for t in labels]
    taken += [p.get_window_extent(renderer) for p in ax.patches]
    tick_px = ax.transData.transform(ticked) if ticked else []
    corners = (["lower right", "upper right", "lower left", "upper left"] if metric == "cagr"
               else ["upper left", "lower left", "upper right", "lower right"])
    legend, footer_y = None, -0.01
    for loc in corners:
        legend = ax.legend(handles=handles, fontsize=9, loc=loc)
        fig.canvas.draw()
        box = legend.get_window_extent(renderer).padded(4)
        if not any(box.overlaps(b) for b in taken) and \
                not any(box.contains(x, y) for x, y in tick_px):
            break
    else:
        legend = ax.legend(handles=handles, fontsize=9, loc="upper center", ncol=2,
                           bbox_to_anchor=(0.5, -0.1))
        fig.canvas.draw()
        footer_y = fig.transFigure.inverted().transform(
            legend.get_window_extent(renderer))[0, 1] - 0.01

    fig.text(0.5, footer_y, "\n".join(footer_lines), ha="center", va="top", fontsize=8.5,
             color="#555555")
    plt.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"  Saved: {out_path}")
    return verdicts
