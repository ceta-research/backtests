#!/usr/bin/env python3
"""A run window's end date for a set of legs (data_utils.RunWindow, METHODOLOGY.md "Run window").

Prints each leg's latest good day (data_end), the run's end_date (the latest day every leg traded) and, per
exchange, the lines that traded over the 21-day median for the last 5 dates. Use it to choose a gate's
--end-date, and save its output next to the gate's results so a pinned file's dates stay auditable.

usage: python3 scripts/data_end.py NSE NYSE,NASDAQ,AMEX SHZ,SHH        one leg per argument
       python3 scripts/data_end.py --legs-file scripts/gate_legs/wave3.json --topic defensive-quality
       add --json for one JSON object instead of the table
Needs CR_API_KEY. Four queries.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cli_utils import EXCHANGE_PRESETS  # noqa: E402
from cr_client import CetaResearch  # noqa: E402
from data_utils import DATA_END_LOOKBACK_DAYS, RunWindow, get_local_benchmark, traded_ratios  # noqa: E402


def legs_from_file(path, topic):
    """{universe: exchanges} for a topic's legs in a gate legs file (--exchange or --preset args)."""
    spec = json.load(open(path))[topic]["legs"]
    out = {}
    for args in spec.values():
        a = dict(zip(args[::2], args[1::2]))
        exs = a["--exchange"].split(",") if "--exchange" in a else EXCHANGE_PRESETS[a["--preset"]]["exchanges"]
        out["_".join(exs)] = exs
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("legs", nargs="*", help="a leg's exchanges, comma-joined (NYSE,NASDAQ,AMEX)")
    ap.add_argument("--legs-file")
    ap.add_argument("--topic")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    exs = legs_from_file(a.legs_file, a.topic) if a.legs_file else {}
    exs.update({leg.replace(",", "_"): leg.split(",") for leg in a.legs})
    if not exs:
        ap.error("no legs: give exchanges or --legs-file and --topic")
    legs = {u: (e, get_local_benchmark(e)[0]) for u, e in exs.items()}
    client = CetaResearch()
    today = datetime.now(timezone.utc).date()
    ratios, bench_days = traded_ratios(client, legs, today)
    win = RunWindow.latest(client, legs, today=today)
    out = {"checked_at": win.checked_at, "lookback_days": DATA_END_LOOKBACK_DAYS, "until": today.isoformat(),
           "end_date": win.end_date.isoformat(), "data_end_by_market": win.data_end_by_market,
           "legs": {u: {"benchmark": legs[u][1], "data_end": win.data_end_by_market[u],
                        "last_dates": {e: {d: round(r[d], 3) for d in sorted(r)[-5:]} for e, r in ratios[u].items()},
                        "benchmark_last": max(bench_days[u]) if bench_days[u] else None} for u in legs}}
    if a.json:
        print(json.dumps(out, indent=1))
        return
    print(f"checked {out['checked_at']}, the {DATA_END_LOOKBACK_DAYS} days to {today}")
    for u, v in out["legs"].items():
        print(f"\n{u}  benchmark {v['benchmark']} (last bar {v['benchmark_last']})  data_end {v['data_end']}")
        for e, days in v["last_dates"].items():
            print(f"  {e:<8} " + "  ".join(f"{d[5:]} {x:.2f}" for d, x in days.items()))
    print(f"\nend_date {out['end_date']}: the latest day every leg traded")


if __name__ == "__main__":
    main()
