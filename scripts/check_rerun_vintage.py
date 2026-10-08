#!/usr/bin/env python3
"""Validate a fresh --global exchange_comparison.json before it replaces the tracked file.

Checks, per non-US entry:
  1. benchmark_name AND benchmark_symbol both present (a hand-stamp has the name only)
  2. benchmark series is DISTINCT from the US entry's, year-aligned, scale-aware
     (except exchanges with no local index, where SPY is the truth: JSE/JNB, JKT, KLS, MIL, SAU, ...)
  3. the entry key is one the topic's generate_charts.py REGION_MAP looks up
  4. headline figures vs the April per-region file (sub-0.05pp = noise)

usage: check_rerun.py <topic-dir> <new.json>
"""
import json, re, sys, glob, os
sys.path.insert(0, "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests")
from data_utils import LOCAL_INDEX_BENCHMARKS  # noqa
from chart_utils import RESULT_KEY_TO_EXCHANGE  # noqa

topic, new_path = sys.argv[1], sys.argv[2]
BT = "/Users/swas/Desktop/Swas/Kite/ATO_SUITE/backtests"
new = json.load(open(new_path))
US = re.compile(r"NYSE|US_MAJOR|AMEX|^US$", re.I)

def ser(e):
    s = {a["year"]: a.get("spy") for a in (e.get("annual_returns") or []) if a.get("spy") is not None}
    if s and max(abs(v) for v in s.values()) < 1.5:
        s = {y: v * 100 for y, v in s.items()}
    return {y: round(v, 3) for y, v in s.items()}

usk = next(k for k in new if isinstance(new[k], dict) and US.search(k))
us = ser(new[usk])

# REGION_MAP keys the generator will look up
gen = open(f"{BT}/{topic}/generate_charts.py").read()
m = re.search(r"REGION_MAP\s*=\s*\{(.*?)\}", gen, re.S)
region_keys = set(re.findall(r"['\"]([^'\"]+)['\"]\s*:", m.group(1))) if m else set()

# April per-region files for figure diff
april = {}
for f in glob.glob(f"{BT}/{topic}/results/*_*.json"):
    if "exchange_comparison" in f: continue
    try:
        d = json.load(open(f))
        if isinstance(d, dict) and "portfolios" in d:
            april[os.path.basename(f)] = d
    except Exception:
        pass

bad = 0
print(f"{'key':10} {'name':18} {'symbol':9} {'series':15} {'in REGION_MAP':13} {'hi cagr new/apr':>16} {'bench cagr new/apr':>19}")
for k, e in new.items():
    if not isinstance(e, dict) or k == usk or "portfolios" not in e: continue
    name, sym = e.get("benchmark_name"), e.get("benchmark_symbol")
    s = ser(e); sh = sorted(set(us) & set(s)); mism = [y for y in sh if abs(us[y] - s[y]) > 0.02]
    ex = RESULT_KEY_TO_EXCHANGE.get(k, k)
    expects_spy = LOCAL_INDEX_BENCHMARKS.get(ex) is None
    series = ("SPY" if not mism else "LOCAL") if sh else "no-per-year"
    ok_series = (series == "SPY") == expects_spy or series == "no-per-year"
    in_map = k in region_keys
    # april match by universe/exchange name heuristics
    ap = next((v for f, v in april.items() if k.lower()[:4] in f.lower() or (v.get("universe") or "").lower().startswith(k.lower()[:3])), None)
    hn = e["portfolios"].get("high", e["portfolios"].get("sustained", {})).get("cagr")
    bn = e["portfolios"]["sp500"].get("cagr")
    ha = ap["portfolios"].get("high", ap["portfolios"].get("sustained", {})).get("cagr") if ap else None
    ba = ap["portfolios"]["sp500"].get("cagr") if ap else None
    flags = []
    if not (name and sym): flags.append("NO-NAME/SYMBOL")
    if not ok_series: flags.append(f"SERIES-MISMATCH(expects {'SPY' if expects_spy else 'LOCAL'})")
    if not in_map: flags.append("NOT-IN-REGION_MAP")
    if ha is not None and hn is not None and abs(ha - hn) > 0.5: flags.append(f"HI-DRIFT {hn-ha:+.2f}pp")
    if flags: bad += 1
    print(f"{k:10} {str(name)[:18]:18} {str(sym)[:9]:9} {series:15} {str(in_map):13} {str(hn):>7}/{str(ha):<8} {str(bn):>8}/{str(ba):<9} {' '.join(flags)}")
print(f"\n{'OK' if not bad else f'{bad} entr(y/ies) flagged'}  | US key={usk}  | REGION_MAP keys={sorted(region_keys)}")
sys.exit(1 if bad else 0)
