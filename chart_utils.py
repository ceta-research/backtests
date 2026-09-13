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
