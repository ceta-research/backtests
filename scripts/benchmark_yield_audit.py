#!/usr/bin/env python3
"""Which published "beat the local benchmark" claims survive a dividend adjustment?

Portfolio returns use adjClose and therefore include dividends. Most of the local
index benchmarks are PRICE indices, so measured excess against them is overstated
by roughly the local dividend yield. SPY (a dividend-adjusted ETF), the DAX (a
performance index) and the Ibovespa (a total-return index) are the exceptions.

This recomputes nothing. It reads the committed results, works out which series
each exchange was actually measured against, and reports which positive-excess
claims sit inside the dividend margin and are therefore not established.

Resolving the benchmark matters more than it sounds. 818 of ~1,000 committed
result entries carry no benchmark label at all, and the "spy" key is a legacy
field name that now holds whichever index was used. So resolution runs in this
order, and anything that cannot be resolved is flagged rather than guessed:

  1. Annual benchmark series identical to the topic's US entry -> really SPY.
     Catches the topics that never got the local-benchmark migration.
  2. Stored benchmark_name / benchmark field, if present.
  3. The registry in data_utils.get_local_benchmark: unknown or mixed exchange
     sets fall back to SPY, which is total return and needs no adjustment.

Usage:
    python3 scripts/benchmark_yield_audit.py            # summary + per-topic counts
    python3 scripts/benchmark_yield_audit.py --flags    # unresolved entries only
    python3 scripts/benchmark_yield_audit.py --json     # machine-readable
"""
import argparse
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_utils import LOCAL_INDEX_BENCHMARKS, LOCAL_INDEX_NAMES  # noqa: E402

# Series that already include dividends. No adjustment applies.
#   SPY      dividend-adjusted ETF
#   ^GDAXI   the DAX is a performance index
#   ^BVSP    the Ibovespa reinvests dividends
TOTAL_RETURN = {"SPY", "^GDAXI", "^BVSP"}

# Approximate long-run gross dividend yields for the PRICE indices, in percent.
# Deliberately round: these decide whether a thin claim sits inside the margin,
# not a restated return.
PRICE_INDEX_YIELD = {
    "^BSESN": 1.3,      # Sensex
    "^N225": 1.8,       # Nikkei 225
    "^FTSE": 3.5,       # FTSE 100
    "^HSI": 3.2,        # Hang Seng
    "^KS11": 1.8,       # KOSPI
    "^TWII": 3.5,       # TAIEX
    "000001.SS": 2.0,   # SSE Composite
    "^OMXS30": 3.3,     # OMX Stockholm 30
    "^SSMI": 3.0,       # SMI
    "^SET.BK": 3.0,     # SET Index
    "^GSPTSE": 2.8,     # TSX Composite
    "^OSEAX": 3.0,      # Oslo All Share
    "^STI": 3.5,        # Straits Times
    "^AXJO": 4.0,       # ASX 200
}

# Result keys that name a country or a combined universe rather than an exchange.
KEY_ALIASES = {
    "India": "NSE", "China": "SHH", "Canada": "TSX", "Switzerland": "SIX",
    "Sweden": "STO", "Germany": "XETRA", "Japan": "JPX", "Korea": "KSC",
    "UK": "LSE", "Taiwan": "TAI", "Brazil": "SAO", "JSE": "JNB",
    "US": "NYSE", "US_MAJOR": "NYSE",
}

# Reversing LOCAL_INDEX_NAMES is lossy: two symbols are both named "S&P 500".
# The benchmark actually used is the SPY ETF, so pin that one explicitly.
NAME_TO_SYMBOL = {v: k for k, v in LOCAL_INDEX_NAMES.items()}
NAME_TO_SYMBOL["S&P 500"] = "SPY"

US_KEYS = {"NYSE_NASDAQ_AMEX", "US_MAJOR", "US", "NYSE_NASDAQ", "AMEX+NASDAQ+NYSE"}


def registry_symbol(key):
    """Mirror get_local_benchmark() for a result key: mixed or unknown -> SPY."""
    codes = []
    for part in key.replace("+", "_").split("_"):
        codes.append(KEY_ALIASES.get(part, part))
    symbols = {LOCAL_INDEX_BENCHMARKS[c] for c in codes if c in LOCAL_INDEX_BENCHMARKS}
    return symbols.pop() if len(symbols) == 1 else "SPY"


def series(v):
    return tuple(round(a.get("spy", 0), 2) for a in (v.get("annual_returns") or []))


def resolve(key, entry, us_series):
    """(symbol, yield_pct, how) for one result entry. yield_pct 0 means fair."""
    s = series(entry)
    if us_series and s:
        n = min(len(s), len(us_series))
        if n and s[:n] == us_series[:n]:
            return "SPY", 0.0, "series-matches-US"

    stored = entry.get("benchmark_name") or entry.get("benchmark")
    sym = NAME_TO_SYMBOL.get(stored) if stored else None
    how = "stored-label" if sym else "registry"
    if not sym:
        sym = registry_symbol(key)

    if sym in TOTAL_RETURN:
        # The registry says SPY but the series is not the US one. Either the run
        # used something else, or the comparison file was assembled from runs at
        # different dates. Not safe to call fair, not safe to demote.
        if sym == "SPY" and how == "registry" and us_series:
            return sym, 0.0, "AMBIGUOUS-spy-registry-but-series-differs"
        return sym, 0.0, how
    y = PRICE_INDEX_YIELD.get(sym)
    if y is None:
        return sym, 0.0, "AMBIGUOUS-unknown-symbol"
    return sym, y, how


def audit():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = {}
    for f in sorted(glob.glob(os.path.join(root, "*/results/exchange_comparison.json"))):
        topic = os.path.relpath(f, root).split(os.sep)[0]
        try:
            d = json.load(open(f))
        except Exception:
            continue
        usk = next((k for k in d if k in US_KEYS and isinstance(d[k], dict)), None)
        us_series = series(d[usk]) if usk else None

        rows = []
        for k, v in d.items():
            if not isinstance(v, dict) or v.get("error"):
                continue
            ex = (v.get("comparison") or {}).get("excess_cagr")
            if ex is None:
                continue
            sym, y, how = resolve(k, v, us_series if k != usk else None)
            rows.append({
                "exchange": k, "excess": ex, "benchmark": sym,
                "benchmark_name": LOCAL_INDEX_NAMES.get(sym, sym),
                "yield": y, "adjusted": round(ex - y, 2), "resolved_by": how,
                "ambiguous": how.startswith("AMBIGUOUS"),
                "invested_periods": v.get("invested_periods"),
                "n_periods": v.get("n_periods"),
            })
        if not rows:
            continue
        priced = [r for r in rows if r["yield"] > 0]
        out[topic] = {
            "n_exchanges": len(rows),
            "claimed_beats": sum(1 for r in rows if r["excess"] > 0),
            "surviving_beats": sum(1 for r in rows if r["adjusted"] > 0),
            "n_price_index": len(priced),
            "all_spy": not priced,
            "ambiguous": sum(1 for r in rows if r["ambiguous"]),
            "price_index_names": sorted({r["benchmark_name"] for r in priced}),
            "rows": sorted(rows, key=lambda r: -r["excess"]),
        }
        out[topic]["changes"] = out[topic]["claimed_beats"] != out[topic]["surviving_beats"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--flags", action="store_true")
    a = ap.parse_args()
    res = audit()
    if a.json:
        print(json.dumps(res, indent=2))
        return

    if a.flags:
        for t, v in sorted(res.items()):
            for r in v["rows"]:
                if r["ambiguous"]:
                    print(f"{t:<26}{r['exchange']:<18}{r['resolved_by']:<44}"
                          f"excess={r['excess']}")
        return

    changed = {k: v for k, v in res.items() if v["changes"]}
    allspy = [k for k, v in res.items() if v["all_spy"]]
    print(f"{len(res)} topics with an exchange_comparison.json")
    print(f"{len(changed)} have a 'beats local benchmark' count that changes")
    print(f"{len(allspy)} are benchmarked entirely against SPY (no local index anywhere)\n")
    print(f"{'topic':<26}{'exch':>5}{'claimed':>9}{'survives':>10}   demoted")
    print("-" * 80)
    for t, v in sorted(changed.items(),
                       key=lambda kv: kv[1]["claimed_beats"] - kv[1]["surviving_beats"],
                       reverse=True):
        demoted = [r["exchange"] for r in v["rows"] if r["excess"] > 0 >= r["adjusted"]]
        print(f"{t:<26}{v['n_exchanges']:>5}{v['claimed_beats']:>9}{v['surviving_beats']:>10}   "
              + ", ".join(demoted[:8]) + (" ..." if len(demoted) > 8 else ""))
    tot_c = sum(v["claimed_beats"] for v in res.values())
    tot_s = sum(v["surviving_beats"] for v in res.values())
    amb = sum(v["ambiguous"] for v in res.values())
    print(f"\nTOTAL positive-excess claims: {tot_c} -> {tot_s} survive "
          f"({tot_c - tot_s} inside the dividend margin)")
    print(f"entries needing a human: {amb}   (see --flags)")
    if allspy:
        print(f"\nall-SPY topics (a local-benchmark methodology line would be false here):")
        print("  " + ", ".join(sorted(allspy)))


if __name__ == "__main__":
    main()
