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
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_utils import LOCAL_INDEX_BENCHMARKS, LOCAL_INDEX_NAMES

# Results files key exchanges inconsistently across topics. Normalise to the
# exchange codes used by LOCAL_INDEX_BENCHMARKS so a benchmark name can be
# recovered even when the results predate the benchmark_name field.
RESULT_KEY_TO_EXCHANGE = {
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
