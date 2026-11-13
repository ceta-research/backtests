#!/usr/bin/env python3
"""The same-bars scan: pairs of held lines in one leg that print the same bars, so one line copies another's prices.

For every rebalance date of a leg, each held line's first window bar (the first bar after the date, within 11
days, as the backtests enter) is read from a bars file. Two held lines of the leg whose first window bars agree on
the close to the cent and on the volume in 3 or more rebalance windows are listed, minus the pairs the leg's
universe rules already cover:
  - check N twins: an X-R.BK NVDR line beside X.BK;
  - class twins, on a leg whose universe_rules block has class_stand_ins: two SAO lines of one issuer (the
    ticker's first four characters, ur-2's key for a line with no ISIN; an issuer only the ISIN names isn't seen);
  - a line the leg's rules removed as a known line (universe_rules.ordinary_shares.removed, reason known);
  - window by window, a match before a line's first own day (universe_rules.line_starts) or after its last own
    day (universe_rules.line_ends).
A zero-volume bar isn't compared: an untraded day's close carries no volume to match. A date with two bars keeps
one, the highest adjClose and then the highest close (split_check.one_per_day, the backtests' price loaders'
rule). Every held line is read in every window of its leg, held then or not, since a copy is rarely held beside
its source. The holdings string lists every booked name (holdings_full adds any a topic cuts from it).

Exits 1 when a pair is left (a new copy stops an install until a rule covers it), else 0.

Usage:
  python scripts/same_bars_scan.py RESULTS.json [RESULTS.json ...] --bars BARS.json [--json OUT]
  --fetch first writes BARS.json from one read-only stock_eod query over the held lines' window bars (needs
  CR_API_KEY); without it the scan is offline. BARS.json: {symbol: [[date, close, volume, adjClose], ...]}.
"""
import argparse
import collections
import itertools
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from split_check import WINDOW_DAYS, epoch, leg_key, one_per_day, plus, spans, sql_list   # noqa: E402

MIN_WINDOWS = 3


def held_lines(res):
    return sorted({s for s, *_ in spans(res)})


def rebalance_dates(res):
    return sorted({r["rebalance_date"][:10] for r in res["period_data"]})


def sao_issuer(symbol):
    return symbol[:4] if symbol.endswith(".SA") else None


def twin(a, b, class_twins):
    """Why two held lines are one security the rules key already, or None."""
    if any(x.endswith("-R.BK") and y == x[:-len("-R.BK")] + ".BK" for x, y in ((a, b), (b, a))):
        return "check N twin"
    if class_twins and sao_issuer(a) is not None and sao_issuer(a) == sao_issuer(b):
        return "class twin"
    return None


def rules(res):
    """(known lines, first own days, last own days, whether the leg keys class twins) from its universe_rules."""
    ur = res.get("universe_rules") or {}
    known = {r["symbol"] for r in (ur.get("ordinary_shares") or {}).get("removed", []) if r.get("reason") == "known"}
    starts = {x["symbol"]: x["first_day"] for x in (ur.get("line_starts") or {}).get("lines", [])}
    ends = {x["symbol"]: x["last_day"] for x in (ur.get("line_ends") or {}).get("lines", [])}
    return known, starts, ends, "class_stand_ins" in ur


def first_bars(bars, dates, lines):
    """{rebalance date: {symbol: (bar date, close in cents, volume)}}: each line's first bar after the date within
    WINDOW_DAYS, when that bar traded, from one bar a day."""
    rows = collections.defaultdict(list)
    for r in one_per_day({"symbol": s, "date": d, "close": c, "volume": v, "adjClose": a}
                         for s in lines for d, c, v, a in bars.get(s, [])):
        rows[r["symbol"]].append((r["date"], r["close"], r["volume"]))
    rows = {s: sorted(v) for s, v in rows.items()}
    out = {}
    for d in dates:
        hi, got = plus(d, WINDOW_DAYS), {}
        for s in lines:
            b = next((b for b in rows.get(s, []) if d < b[0] <= hi), None)
            if b is not None and b[2]:
                got[s] = (b[0], round(float(b[1]) * 100), round(float(b[2])))
        out[d] = got
    return out


def pairs(res, bars):
    """(listed, exempt): the pairs of res's held lines whose first window bars match in MIN_WINDOWS or more
    rebalance windows, each {"pair", "windows": [bar dates]}; a listed one also gives the windows inside both
    lines' own days, and an exempt one its reason."""
    known, starts, ends, class_twins = rules(res)
    hits = collections.defaultdict(list)
    for got in first_bars(bars, rebalance_dates(res), held_lines(res)).values():
        same = collections.defaultdict(list)
        for s, (_, cents, vol) in got.items():
            same[(cents, vol)].append(s)
        for group in same.values():
            for a, b in itertools.combinations(sorted(group), 2):
                hits[(a, b)].append((got[a][0], got[b][0]))
    listed, exempt = [], []
    for (a, b), days in sorted(hits.items()):
        if len(days) < MIN_WINDOWS:
            continue
        rec = {"pair": [a, b], "windows": [da for da, _ in days]}
        why = twin(a, b, class_twins) or ("known list" if a in known or b in known else None)
        own = [da for da, db in days
               if all(starts.get(x, "") <= dx <= ends.get(x, "9999-12-31") for x, dx in ((a, da), (b, db)))]
        if why:
            exempt.append({**rec, "reason": why})
        elif len(own) < MIN_WINDOWS:
            exempt.append({**rec, "reason": f"line start or end: {len(own)} of {len(days)} windows inside both "
                                            "lines' own days"})
        else:
            listed.append({**rec, "own_windows": own})
    return listed, exempt


def fetch_sql(legs):
    """The one read-only stock_eod query: every held line's bars in every rebalance window of the legs."""
    lines = sorted({s for res in legs for s in held_lines(res)})
    dates = sorted({d for res in legs for d in rebalance_dates(res)})
    wins = ", ".join(f"({epoch(d)}, {epoch(plus(d, WINDOW_DAYS))})" for d in dates)
    return ("SELECT DISTINCT e.symbol, e.date, e.close, e.volume, e.adjClose FROM stock_eod e "
            f"JOIN (VALUES {wins}) w(a, b) ON e.dateEpoch > w.a AND e.dateEpoch <= w.b "
            f"WHERE e.symbol IN ({sql_list(lines)}) AND e.date > '{dates[0]}' "
            f"AND e.date <= '{plus(dates[-1], WINDOW_DAYS)}'")


def bars_file(rows):
    """{symbol: [[date, close, volume, adjClose], ...]} by date from the fetch's rows (bars with a close)."""
    out = collections.defaultdict(list)
    for r in rows:
        if r.get("close") is not None:
            out[r["symbol"]].append([str(r["date"])[:10], float(r["close"]), r.get("volume") or 0,
                                     float(r.get("adjClose") or 0)])
    return {s: sorted(v) for s, v in sorted(out.items())}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("results", nargs="+", help="results files (a leg each)")
    ap.add_argument("--bars", required=True, metavar="BARS.json", help="the window bars (written first by --fetch)")
    ap.add_argument("--fetch", action="store_true", help="build BARS.json with one stock_eod query first")
    ap.add_argument("--json", metavar="OUT", help="the record, one entry per leg")
    args = ap.parse_args(argv)
    legs = []
    for path in args.results:
        with open(path) as fh:
            legs.append((path, json.load(fh)))
    if args.fetch:
        from cr_client import CetaResearch
        rows = CetaResearch().query(fetch_sql([res for _, res in legs]), verbose=False, limit=1000000, timeout=600)
        with open(args.bars, "w") as fh:
            json.dump(bars_file(rows or []), fh)
    with open(args.bars) as fh:
        bars = json.load(fh)
    report, left = {}, 0
    for path, res in legs:
        listed, exempt = pairs(res, bars)
        key = leg_key(report, res["universe"], path)
        report[key] = {"file": path, "held_lines": len(held_lines(res)), "windows": len(rebalance_dates(res)),
                       "pairs": listed, "exempt": exempt}
        left += len(listed)
        print(f"{key}: {len(held_lines(res))} held lines, {len(rebalance_dates(res))} windows: {len(listed)} pairs "
              f"left, {len(exempt)} exempt")
        for p in listed:
            print(f"  PAIR {p['pair'][0]} {p['pair'][1]}: {len(p['own_windows'])} windows ({', '.join(p['own_windows'])})")
        for p in exempt:
            print(f"  exempt {p['pair'][0]} {p['pair'][1]} ({p['reason']}): {len(p['windows'])} windows")
    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"min_windows": MIN_WINDOWS, "legs": report}, fh, indent=1)
    return 1 if left else 0


if __name__ == "__main__":
    sys.exit(main())
