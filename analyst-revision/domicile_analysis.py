#!/usr/bin/env python3
"""
Domicile decomposition of the analyst revision event study.

A screen picks every company LISTED on an exchange. Outside the US that is
mostly foreign companies' secondary listings, and this study benchmarks each
event against the exchange's LOCAL index. When a US mega-cap's Frankfurt line
is measured against the DAX, the "abnormal" return is mostly US-versus-Germany
market performance, not the analyst event.

The tell is direction. A real revision signal cannot push upgrades AND
downgrades the same way. If both are positive against the local index, what is
being measured is the benchmark mismatch.

Reads the per-exchange event CSVs written by backtest.py, joins each event's
symbol to profile.country, and recomputes CAR per domicile group using the same
winsorization as the main study.

Usage:
    python3 analyst-revision/domicile_analysis.py
    python3 analyst-revision/domicile_analysis.py --output analyst-revision/results/domicile_decomposition.json
"""

import argparse
import csv
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# Exchange -> (result-file key, home country code, benchmark label)
EXCHANGES = [
    ("XETRA", "XETRA", "DE", "DAX"),
    ("LSE",   "LSE",   "GB", "FTSE 100"),
    ("SIX",   "SIX",   "CH", "SMI"),
    ("TSX",   "TSX",   "CA", "TSX Composite"),
]

WINDOWS = [1, 5, 21, 63]
WINSORIZE_PCT = 1.0
MIN_GROUP = 50


def winsorize(values, pct=WINSORIZE_PCT):
    if len(values) < 10:
        return values
    s = sorted(values)
    n = len(s)
    lo = s[max(0, int(n * pct / 100))]
    hi = s[min(n - 1, int(n * (100 - pct) / 100))]
    return [max(lo, min(hi, v)) for v in values]


def car_stats(raw):
    """Mean CAR, t-stat and hit rate. Matches backtest.py compute_car_stats."""
    if len(raw) < MIN_GROUP:
        return None
    vals = winsorize(raw)
    n = len(vals)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / (n - 1) if n > 1 else 0
    se = math.sqrt(var) / math.sqrt(n) if var > 0 else 0
    t = mean / se if se else 0.0
    return {
        "mean_car": round(mean * 100, 4),
        "t_stat": round(t, 3),
        "n": n,
        "significant_5pct": abs(t) > 1.96,
    }


def country_lookup(cr, exchanges):
    ex_in = ", ".join(f"'{e}'" for e in exchanges)
    sql = (f"SELECT DISTINCT symbol, COALESCE(country, '??') AS country, exchange "
           f"FROM profile WHERE exchange IN ({ex_in})")
    lut = {}
    for r in cr.query(sql, verbose=False):
        lut.setdefault(r["exchange"], {})[r["symbol"]] = r["country"]
    return lut


def main():
    ap = argparse.ArgumentParser(description="Domicile decomposition of analyst revision CARs")
    ap.add_argument("--output", default=os.path.join(RESULTS_DIR, "domicile_decomposition.json"))
    args = ap.parse_args()

    cr = CetaResearch()
    lut = country_lookup(cr, [e[0] for e in EXCHANGES])

    out = {}
    for ex, key, home, bench in EXCHANGES:
        path = os.path.join(RESULTS_DIR, f"analyst_revision_{key}_events.csv")
        if not os.path.exists(path):
            print(f"  {ex}: no events CSV at {path}. Run backtest.py --global first.")
            continue
        symbols = lut.get(ex, {})

        buckets = {}
        totals = {}
        for row in csv.DictReader(open(path)):
            c = symbols.get(row["symbol"], "??")
            grp = "domestic" if c == home else ("us_domiciled" if c == "US" else "other_foreign")
            act = row["action"]
            totals[(act, grp)] = totals.get((act, grp), 0) + 1
            for w in WINDOWS:
                v = row.get(f"abnormal_ret_{w}d")
                if v not in (None, "", "None"):
                    buckets.setdefault((act, w, grp), []).append(float(v))

        ex_out = {"benchmark_name": bench, "home_country": home, "groups": {}}
        n_all = sum(totals.values())
        for grp in ("domestic", "us_domiciled", "other_foreign"):
            n_grp = sum(v for (a, g), v in totals.items() if g == grp)
            g_out = {
                "n_events": n_grp,
                "share_pct": round(100 * n_grp / n_all, 2) if n_all else 0,
                "n_upgrades": totals.get(("upgrade", grp), 0),
                "n_downgrades": totals.get(("downgrade", grp), 0),
            }
            for act in ("upgrade", "downgrade"):
                for w in WINDOWS:
                    s = car_stats(buckets.get((act, w, grp), []))
                    if s:
                        g_out[f"{act}_T+{w}"] = s
            ex_out["groups"][grp] = g_out
        out[ex] = ex_out

        print(f"\n{'=' * 92}")
        print(f"  {ex}: abnormal return vs {bench}, split by where the company is domiciled")
        print(f"{'=' * 92}")
        print(f"  {'group':<16}{'share':>8}{'up T+21':>11}{'t':>8}{'up T+63':>11}{'t':>8}"
              f"{'down T+21':>12}{'down T+63':>12}")
        for grp in ("domestic", "us_domiciled", "other_foreign"):
            g = ex_out["groups"][grp]
            def f(k):
                d = g.get(k)
                return f"{d['mean_car']:+.3f}%" if d else "n<50"
            def t(k):
                d = g.get(k)
                return f"{d['t_stat']:+.2f}" if d else "-"
            print(f"  {grp:<16}{g['share_pct']:>7.1f}%{f('upgrade_T+21'):>11}{t('upgrade_T+21'):>8}"
                  f"{f('upgrade_T+63'):>11}{t('upgrade_T+63'):>8}"
                  f"{f('downgrade_T+21'):>12}{f('downgrade_T+63'):>12}")

    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\n  Saved: {args.output}")


if __name__ == "__main__":
    main()
