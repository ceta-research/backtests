#!/usr/bin/env python3
"""The same-bars scan: pairs of held lines in one leg that print the same bars, so one line copies another's prices.

For every rebalance date of a leg, each held line's first window bar (the first bar after the date, within 11
days, as the backtests enter) is read from a bars file. Two held lines of the leg whose first window bars agree on
the close to the cent and on the volume in 3 or more rebalance windows are listed, minus the pairs the leg's
universe rules already cover:
  - check N twins: an X-R.BK NVDR line beside X.BK;
  - class twins, on a leg whose universe_rules block has class_stand_ins: two SAO lines of one issuer (the
    ticker's first four characters, ur-2's key for a line with no ISIN; an issuer only the ISIN names isn't seen);
  - one ISIN, on a leg whose results say "isin_dedup": true: two lines whose profile ISINs are equal and not
    empty, since the dedup books one line of an ISIN a period (a bars file whose rows carry no ISIN exempts
    nothing this way);
  - window by window, a match before a line's first own day (universe_rules.line_starts) or after its last own
    day (universe_rules.line_ends).
A line the rules remove at the fetch, a known line among them, is never held, so it needs no exemption.
A zero-volume bar isn't compared: an untraded day's close carries no volume to match. A date with two bars keeps
one, the highest adjClose and then the highest close (split_check.one_per_day, the backtests' price loaders'
rule). Every held line is read in every window of its leg, held then or not, since a copy is rarely held beside
its source. The holdings string lists every booked name (holdings_full adds any a topic cuts from it).

Exits 1 when a pair is left (a new copy stops an install until a rule covers it), else 0.

Usage:
  python scripts/same_bars_scan.py RESULTS.json [RESULTS.json ...] --bars BARS.json [--json OUT]
  --fetch first writes BARS.json from read-only stock_eod queries over the held lines' window bars, each bar with
  its line's profile ISIN (needs CR_API_KEY): one query per group of held lines, one group unless a query would
  pass split_check's MAX_SQL. Without it the scan is offline. BARS.json: {symbol: [[date, close, volume,
  adjClose, isin], ...]}; a file without the isin field still loads.
"""
import argparse
import collections
import itertools
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from split_check import (MAX_SQL, WINDOW_DAYS, epoch, grouped, leg_key, one_per_day, plus, spans,   # noqa: E402
                         sql_list)

MIN_WINDOWS = 3


def held_lines(res):
    return sorted({s for s, *_ in spans(res)})


def rebalance_dates(res):
    return sorted({r["rebalance_date"][:10] for r in res["period_data"]})


def sao_issuer(symbol):
    return symbol[:4] if symbol.endswith(".SA") else None


def twin(a, b, class_twins, isin):
    """Why two held lines are one security the rules key already, or None. isin: each line's profile ISIN on a leg
    whose ISIN dedup books one line of an ISIN a period, else empty."""
    if any(x.endswith("-R.BK") and y == x[:-len("-R.BK")] + ".BK" for x, y in ((a, b), (b, a))):
        return "check N twin"
    if class_twins and sao_issuer(a) is not None and sao_issuer(a) == sao_issuer(b):
        return "class twin"
    if isin.get(a) and isin.get(a) == isin.get(b):
        return "same ISIN: the leg's ISIN dedup books one of the two a period"
    return None


def rules(res):
    """(first own days, last own days, whether the leg keys class twins, whether its ISIN dedup books one line of
    an ISIN a period) from its universe_rules and isin_dedup."""
    ur = res.get("universe_rules") or {}
    starts = {x["symbol"]: x["first_day"] for x in (ur.get("line_starts") or {}).get("lines", [])}
    ends = {x["symbol"]: x["last_day"] for x in (ur.get("line_ends") or {}).get("lines", [])}
    return starts, ends, "class_stand_ins" in ur, res.get("isin_dedup") is True


def isins(bars, lines):
    """{line: its profile ISIN} from the bars rows' fifth field, None or empty where the profile has none; a bars
    file without the field gives none."""
    return {s: r[4] for s in lines for r in bars.get(s, []) if len(r) > 4}


def first_bars(bars, dates, lines):
    """{rebalance date: {symbol: (bar date, close in cents, volume)}}: each line's first bar after the date within
    WINDOW_DAYS, when that bar traded, from one bar a day."""
    rows = collections.defaultdict(list)
    for r in one_per_day({"symbol": s, "date": d, "close": c, "volume": v, "adjClose": a}
                         for s in lines for d, c, v, a, *_ in bars.get(s, [])):
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
    starts, ends, class_twins, isin_dedup = rules(res)
    lines = held_lines(res)
    isin = isins(bars, lines) if isin_dedup else {}
    hits = collections.defaultdict(list)
    for got in first_bars(bars, rebalance_dates(res), lines).values():
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
        why = twin(a, b, class_twins, isin)
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


def fetch_sql(legs, max_len=None):
    """The read-only stock_eod queries, each max_len (default MAX_SQL) characters or shorter: every held line's
    bars in every rebalance window of the legs, one query per group of lines (split_check.grouped)."""
    lines = sorted({s for res in legs for s in held_lines(res)})
    dates = sorted({d for res in legs for d in rebalance_dates(res)})
    build = lambda group: (_fetch_sql([s for s, in group], dates),)
    return [q for q, in grouped([(s,) for s in lines], build, max_len or MAX_SQL)]


def _fetch_sql(lines, dates):
    """One query: the lines' bars in the windows of dates, each with its line's profile ISIN as the dedup keys it
    (data_utils.isin_dedup_sql: trimmed, upper case, an empty one NULL, one per symbol)."""
    wins = ", ".join(f"({epoch(d)}, {epoch(plus(d, WINDOW_DAYS))})" for d in dates)
    return ("SELECT DISTINCT e.symbol, e.date, e.close, e.volume, e.adjClose, i.isin FROM stock_eod e "
            f"JOIN (VALUES {wins}) w(a, b) ON e.dateEpoch > w.a AND e.dateEpoch <= w.b "
            "LEFT JOIN (SELECT symbol, MIN(NULLIF(UPPER(TRIM(isin)), '')) AS isin FROM profile GROUP BY symbol) i "
            f"ON i.symbol = e.symbol WHERE e.symbol IN ({sql_list(lines)}) AND e.date > '{dates[0]}' "
            f"AND e.date <= '{plus(dates[-1], WINDOW_DAYS)}'")


def bars_file(rows):
    """{symbol: [[date, close, volume, adjClose, isin], ...]} by date from the fetch's rows (bars with a close)."""
    out = collections.defaultdict(list)
    for r in rows:
        if r.get("close") is not None:
            out[r["symbol"]].append([str(r["date"])[:10], float(r["close"]), r.get("volume") or 0,
                                     float(r.get("adjClose") or 0), r.get("isin")])
    return {s: sorted(v) for s, v in sorted(out.items())}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("results", nargs="+", help="results files (a leg each)")
    ap.add_argument("--bars", required=True, metavar="BARS.json", help="the window bars (written first by --fetch)")
    ap.add_argument("--fetch", action="store_true", help="build BARS.json with stock_eod queries first")
    ap.add_argument("--json", metavar="OUT", help="the record, one entry per leg")
    args = ap.parse_args(argv)
    legs = []
    for path in args.results:
        with open(path) as fh:
            legs.append((path, json.load(fh)))
    if args.fetch:
        from cr_client import CetaResearch
        cr, rows = CetaResearch(), []
        for sql in fetch_sql([res for _, res in legs]):
            rows += cr.query(sql, verbose=False, limit=1000000, timeout=600) or []
        with open(args.bars, "w") as fh:
            json.dump(bars_file(rows), fh)
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
