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


def benchmark_cumulative(data, exchange_key, initial=10000):
    """Cumulative growth of THAT exchange's own benchmark series.

    Returns (years, values). Empty lists if the exchange has no annual returns.
    """
    entry = (data or {}).get(exchange_key) or {}
    annual = entry.get("annual_returns") or []
    if not annual:
        return [], []
    values = [initial]
    years = [annual[0]["year"] - 1]
    for ar in annual:
        values.append(values[-1] * (1 + ar.get("spy", 0) / 100))
        years.append(ar["year"])
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
