#!/usr/bin/env python3
"""The split check: whether a split or consolidation the vendor's adjClose carries unadjusted moves a leg.

DATA_QUALITY_ISSUES lists the fault on ASX and SAO: the price steps by the split ratio on one day with adjClose
equal to close, so check F's drift stays 1 and the data checks can't see it. Four reads per results file:
  1. check L: the file's own large-move lists (data_checks.large_moves, ltp.large_moves);
  2. the max-to-min adjClose ratio of every held line over its whole history (DATA_QUALITY_ISSUES' query);
  3. inside every held window (the first bar after the rebalance date to 11 days past the exit date, or to the
     exit date itself for the partial period, whose exit the backtest prices at the close on its end date), every
     one-day adjClose move of +80% or more or -44% or less up to the holding's exit bar, with the close's move
     that day, the splits_calendar rows within 3 days and whether it reverses within 5 trading days, also up to
     the exit bar (an undoing after it isn't in the booked return);
  4. every splits_calendar row inside a holding (after its entry bar, up to its exit bar), tested for absorption:
     adjClose stepping by the row's price factor means it wasn't adjusted.
A date with two bars keeps one, the highest adjClose and then the highest close, as the backtests' price loaders
do. A step that reverses nets out of the holding's return and changes nothing, and so does a later step up to the
bar that completed the reversal, when it is the reversing step's opposite. A one-way step with a split row,
or whose adjClose step equals the close's at a round ratio (and isn't a reopening after zero-volume days), is an
artifact candidate: its step is divided out of the holding's return. A split row adjClose didn't absorb is
rebuilt with the day's traded move restored. A split row on an artifact's day is rebuilt once, as the step, and
the rebuilds of one holding compound. The leg's CAGR and excess are recomputed from its period returns
(metrics.compute_metrics, the file's periods per year, fractions for a partial period).

The bar: each confirmed artifact's move of the leg's excess, and the leg's net move with every confirmed artifact
rebuilt, both under 0.15 (pp) in absolute value. A candidate stays confirmed until a person calls it no change
with --not-artifact and the key its rebuild prints (SYMBOL:REBALANCE:DATE, the step's day or the split row's
bar): it then leaves the rebuilds, the net and the verdict, and the leg's not_artifact lists it. The record keeps
the every-candidate net beside the confirmed one. A key that matches no candidate is refused. A leg over the bar
lists why in over_bar; the script exits 0 either way.
Limits: a split under 1.8x with no splits_calendar row doesn't show, and every candidate needs a person's
verdict (a real one-day move can pass the round-ratio test).

Usage: python scripts/split_check.py RESULTS.json [RESULTS.json ...] --json OUT [--not-artifact KEY ...]
Needs CR_API_KEY: three read-only queries per file. Writes only OUT.
"""
import argparse
import collections
import datetime as dt
import json
import math
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
from data_utils import _sql_list, utc_epoch   # noqa: E402
from factor_review import PPY, _cagr_excess, _rows   # noqa: E402
from verify_floor_guard import held_symbols   # noqa: E402

WINDOW_DAYS = 11        # past a rebalance date: the backtests' next-day entry plus get_prices' 10-day window
STEP = math.log(1.8)    # a one-day adjClose move of +80% or more, or -44% or less
SPLIT_DAYS = 3          # a splits_calendar row this many days from a step explains it
REVERSAL_BARS = 5       # a step undone within this many trading days nets out
ROUND = (2, 3, 4, 5, 10, 20, 25, 50, 100)
BAR = 0.15


def plus(d, n):
    return (dt.date.fromisoformat(d) + dt.timedelta(days=n)).isoformat()


def days_apart(a, b):
    return abs((dt.date.fromisoformat(a) - dt.date.fromisoformat(b)).days)


def epoch(d):
    """An ISO date's UTC-midnight epoch, as stock_eod.dateEpoch holds it."""
    return utc_epoch(dt.date.fromisoformat(d))


def leg_key(legs, universe, path):
    """A leg's key in a record's legs: its universe, with its file beside it when another leg has the name."""
    return universe if universe not in legs else f"{universe} ({path})"


def spans(res):
    """[(symbol, rebalance date, exit date, names held, window end)]: every held line-period of res's invested
    rows. The window ends WINDOW_DAYS past the exit date, or on it for the partial period, whose exit the backtest
    prices at the close on the end date."""
    out = []
    for p in res["period_data"]:
        if p.get("stocks_held") and not str(p.get("holdings") or "").startswith("CASH"):
            xd = p["exit_date"][:10]
            end = xd if p.get("partial") else plus(xd, WINDOW_DAYS)
            out += [(s, p["rebalance_date"][:10], xd, p["stocks_held"], end) for s in sorted(held_symbols(p) - {""})]
    return out


def sql_list(symbols):
    """The symbols as a SQL list. A symbol holding ; or ' is refused before any SQL is built."""
    bad = [s for s in symbols if ";" in s or "'" in s]
    if bad:
        raise ValueError(f"symbols unsafe in SQL: {bad}")
    return _sql_list(symbols)


def queries(res):
    """The three read-only queries on res's held lines: the adjClose ratio, the window bars, the split rows."""
    sp = spans(res)
    if not sp:
        return None
    lst = sql_list(sorted({s for s, *_ in sp}))
    vals = ", ".join(f"('{s}', {epoch(r)}, {epoch(end)})" for s, r, *_, end in sp)
    first, last = min(r for _, r, *_ in sp), max(end for *_, end in sp)
    return (("SELECT symbol, MIN(adjClose) AS min_price, MAX(adjClose) AS max_price, "
             "MAX(adjClose) / NULLIF(MIN(adjClose), 0) AS price_ratio FROM stock_eod "
             f"WHERE symbol IN ({lst}) AND adjClose > 0 GROUP BY symbol"),
            (f"WITH spans(symbol, a, b) AS (VALUES {vals}) "
             "SELECT DISTINCT e.symbol, e.date, e.close, e.adjClose, e.volume FROM stock_eod e "
             f"JOIN spans p ON e.symbol = p.symbol WHERE e.date >= '{first}' AND e.date <= '{last}' "
             "AND e.dateEpoch >= p.a AND e.dateEpoch <= p.b"),
            f"SELECT symbol, date, numerator, denominator FROM splits_calendar WHERE symbol IN ({lst})")


def one_per_day(rows):
    """One bar row per (symbol, date): the highest adjClose, then the highest close, as the backtests' price
    loaders keep, so a duplicate date makes no step. same_bars_scan reads its bars through this too."""
    best = {}
    for r in rows:
        k, rank = (r["symbol"], str(r["date"])[:10]), (r.get("adjClose") or 0, r.get("close") or 0)
        if k not in best or rank > best[k][0]:
            best[k] = (rank, r)
    return [r for _, r in best.values()]


def by_symbol(bars):
    """{symbol: [(date, close, adjClose, volume)]} by date, one a day, from bar rows with both prices above zero."""
    out = collections.defaultdict(list)
    for r in one_per_day(r for r in bars if (r.get("adjClose") or 0) > 0 and (r.get("close") or 0) > 0):
        out[r["symbol"]].append((str(r["date"])[:10], float(r["close"]), float(r["adjClose"]), r.get("volume")))
    for s in out:
        out[s].sort()
    return out


def holding(seq, reb, xd, end):
    """(window bars, entry index, exit index) or None: the bars after reb to end (spans' window end), the first one
    inside the entry window, and the first after xd, else the window's last bar (a partial period's window ends on
    xd, so its exit is the last bar on or before it)."""
    seq = [b for b in seq if reb < b[0] <= end]
    ent = 0 if seq and seq[0][0] <= plus(reb, WINDOW_DAYS) else None
    ext = next((i for i, b in enumerate(seq) if b[0] > xd), len(seq) - 1)
    return (seq, ent, ext) if ent is not None and ext > ent else None


def steps(seq, ent, ext, split_rows):
    """The one-day adjClose moves of +80% or -44% inside the holding, each with its close move, the split rows
    within SPLIT_DAYS and whether it reverses within REVERSAL_BARS trading days, up to the exit bar (undone_by:
    the bar that completed the reversal)."""
    out = []
    for i in range(ent + 1, ext + 1):
        la = math.log(seq[i][2] / seq[i - 1][2])
        if -STEP < la < STEP:
            continue
        back = [math.log(seq[j][2] / seq[i][2]) for j in range(i + 1, min(i + 1 + REVERSAL_BARS, ext + 1))]
        k = next((k for k, b in enumerate(back) if abs(b + la) < 0.25 * abs(la)), None)
        out.append({"date": seq[i][0], "prev": seq[i - 1][0], "adj_step": math.exp(la),
                    "close_step": seq[i][1] / seq[i - 1][1], "volume": [seq[i - 1][3], seq[i][3]],
                    "split_rows": [list(x) for x in split_rows if days_apart(x[0], seq[i][0]) <= SPLIT_DAYS],
                    "reverses": k is not None, "undone_by": None if k is None else seq[i + 1 + k][0]})
    return out


def classify(cands):
    """Marks each step netted (it reverses, or it undoes a step that does, on or before the bar that completed the
    reversal), artifact and why."""
    netted = set()
    for i, c in enumerate(cands):
        if c["reverses"]:
            netted.add(i)
            lc = math.log(c["adj_step"])
            j = next((j for j in range(i + 1, len(cands)) if (cands[j]["symbol"], cands[j]["rebalance"]) ==
                      (c["symbol"], c["rebalance"]) and abs(math.log(cands[j]["adj_step"]) + lc) < 0.25 * abs(lc)
                      and cands[j]["date"] <= c["undone_by"]), None)
            if j is not None:
                netted.add(j)
    for i, c in enumerate(cands):
        st = c["adj_step"]
        k = st if st >= 1 else 1 / st
        round_ratio = any(abs(k / q - 1) < 0.06 for q in ROUND)
        same = abs(st / c["close_step"] - 1) < 0.01
        reopen = not c["volume"][0] and bool(c["volume"][1])
        c["netted"] = i in netted
        c["artifact"] = not c["netted"] and (bool(c["split_rows"]) or (round_ratio and same and not reopen))
        c["why"] = ("reverses inside the window, nets out" if c["netted"] else
                    "split row" if c["split_rows"] else
                    "trading resumed after zero-volume days: a repricing" if reopen else
                    "round ratio with the close moving the same" if round_ratio and same else "no split signature")


def split_tests(seq, ent, ext, split_rows):
    """Each split row whose day falls inside the holding: adjClose's and the close's step on the first bar on or
    after it, the row's price factor (a num-for-den split moves the raw price by den/num) and whether adjClose
    stepped by it (unadjusted)."""
    out = []
    for d0, num, den in split_rows:
        i = next((i for i in range(ent + 1, ext + 1) if seq[i][0] >= d0), None)
        if not num or not den or i is None or seq[i - 1][0] >= d0:
            continue
        a_st, c_st, pf = seq[i][2] / seq[i - 1][2], seq[i][1] / seq[i - 1][1], float(den) / float(num)
        out.append({"date": d0, "bar": seq[i][0], "ratio": f"{float(num):g}:{float(den):g}", "price_factor": pf,
                    "close_step": c_st, "adj_step": a_st,
                    "unadjusted": abs(math.log(a_st / pf)) < 0.1 and abs(math.log(pf)) > 0.05})
    return out


def fixes(cands, tests):
    """[(item, correction)]: the holding's return times the correction rebuilds it. An artifact step divides its
    step out. A split row adjClose didn't absorb restores the day's traded move: the close's move over the price
    factor when the close carries the split, the close's move for a factor step on a flat close. Each item carries
    its key, SYMBOL:REBALANCE:DATE (the step's day or the split row's bar), as --not-artifact takes it."""
    out = [("step", c, c["date"], 1 / c["adj_step"]) for c in cands if c["artifact"]]
    # a split row on a step's day is that step: rebuilt with it if it's an artifact, netted out with it if it reverses
    done = {(c["symbol"], c["rebalance"], c["date"]) for c in cands if c["artifact"] or c["netted"]}
    for t in tests:
        key = (t["symbol"], t["rebalance"], t["bar"])
        if not t["unadjusted"] or key in done:
            continue
        done.add(key)
        flat = abs(math.log(t["close_step"])) < 0.02
        t["kind"] = "factor step, close flat" if flat else "bonus or split not in adjClose"
        out.append((t["kind"], t, t["bar"],
                    (t["close_step"] if flat else t["close_step"] / t["price_factor"]) / t["adj_step"]))
    return [(dict(item, kind=kind, key=f"{item['symbol']}:{item['rebalance']}:{d}"), f) for kind, item, d, f in out]


def score(res, held, rebuilt):
    """(effects, recompounded, all rebuilt): each rebuild with its booked and rebuilt return and its own move of
    the leg's excess, and the leg's (CAGR, excess) from its period returns, as stored and with every rebuild."""
    rows = _rows(res)
    base = [r["portfolio_return"] for r in rows]
    at = {r["rebalance_date"][:10]: i for i, r in enumerate(rows)}
    x0 = _cagr_excess(res, rows, base)

    def moved(corrections):
        rets = list(base)
        for (s, reb), f in corrections.items():
            r_b, n = held[(s, reb)]
            rets[at[reb]] += (1 + r_b) * (f - 1) / n
        return _cagr_excess(res, rows, rets)

    effects, total = [], collections.defaultdict(lambda: 1.0)
    for item, f in rebuilt:
        key = (item["symbol"], item["rebalance"])
        if item["rebalance"] not in at:
            continue
        r_b, _ = held[key]
        total[key] *= f
        effects.append({**item, "r_booked": r_b, "r_rebuilt": (1 + r_b) * f - 1,
                        "leg_excess_move": moved({key: f})[1] - x0[1]})
    return effects, x0, moved(dict(total))


def _rounded(x, nd=6):
    if isinstance(x, float):
        return round(x, nd)
    if isinstance(x, dict):
        return {k: _rounded(v, 4 if k == "leg_excess_move" else nd) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_rounded(v, nd) for v in x]
    return x


def check_leg(res, bars, splits, ratio, not_artifact=()):
    """The split check's record of one results file from the three queries' rows (no network). not_artifact holds
    the keys of the candidates a person called no change."""
    sp, by = spans(res), by_symbol(bars)
    rows_of = collections.defaultdict(list)
    for r in splits:
        rows_of[r["symbol"]].append((str(r["date"])[:10], r.get("numerator"), r.get("denominator")))
    cands, tests, held = [], [], {}
    for s, reb, xd, n, end in sp:
        h = holding(by.get(s, []), reb, xd, end)
        if h is None:
            continue
        seq, ent, ext = h
        held[(s, reb)] = (seq[ext][2] / seq[ent][2] - 1, n)
        span, rows_s = {"symbol": s, "rebalance": reb, "exit": xd, "n": n}, sorted(rows_of[s], key=lambda x: x[0])
        cands += [{**span, **c} for c in steps(seq, ent, ext, rows_s)]
        tests += [{**span, **t} for t in split_tests(seq, ent, ext, rows_s)]
    classify(cands)
    every = fixes(cands, tests)
    effects, x0, x_every = score(res, held, every)
    x1 = score(res, held, [(i, f) for i, f in every if i["key"] not in not_artifact])[2]
    kept = [e for e in effects if e["key"] not in not_artifact]
    net, net_every = x1[1] - x0[1], x_every[1] - x0[1]
    over = [f"{e['key']} ({e['kind']}) moves the leg's excess {e['leg_excess_move']:+.3f}pp"
            for e in kept if abs(e["leg_excess_move"]) >= BAR]
    if abs(net) >= BAR:
        over.append(f"every confirmed rebuild together moves the leg's excess {net:+.3f}pp")
    ratios = [r for r in ratio if r.get("price_ratio") is not None]
    dcb, ltp = res.get("data_checks") or {}, res.get("ltp") or {}
    return _rounded({
        "universe": res["universe"], "frequency": res["frequency"], "periods_per_year": PPY[res["frequency"]],
        "stored": [res["portfolio"]["cagr"], res["comparison"]["excess_cagr"]],
        "recompounded": [round(v, 2) for v in x0], "rebuilt": [round(v, 2) for v in x1], "net_move": round(net, 4),
        "net_move_every_candidate": round(net_every, 4),
        "verdict": "over the bar" if over else "under the bar", "over_bar": over,
        "check_L": {"large_moves": dcb.get("large_moves", []), "ltp_large_moves": ltp.get("large_moves", [])},
        "ratio_over_1000": sorted([{"symbol": r["symbol"], "price_ratio": r["price_ratio"]} for r in ratios
                                   if r["price_ratio"] > 1000], key=lambda r: -r["price_ratio"]),
        "ratio_over_100": sum(1 for r in ratios if r["price_ratio"] > 100),
        "held_lines": len({s for s, *_ in sp}), "held_line_periods": len(sp), "line_periods_priced": len(held),
        "steps": cands, "split_rows_in_windows": tests, "rebuilds": kept,
        "not_artifact": [e for e in effects if e["key"] in not_artifact]})


def report_lines(key, leg):
    out = [f"{key}: stored {leg['stored']}, recompounded {leg['recompounded']}, every confirmed rebuild "
           f"{leg['rebuilt']} (net {leg['net_move']:+.3f}pp; every candidate {leg['net_move_every_candidate']:+.3f}pp)"
           f": {leg['verdict']}",
           f"  check L lists {len(leg['check_L']['large_moves'])} (+{len(leg['check_L']['ltp_large_moves'])} fills); "
           f"ratio over 1000: {len(leg['ratio_over_1000'])} lines, over 100: {leg['ratio_over_100']}; priced "
           f"{leg['line_periods_priced']} of {leg['held_line_periods']} held line-periods"]
    out += [f"  {c['symbol']:10s} held {c['rebalance']} {c['date']} adj x{c['adj_step']} close x{c['close_step']} "
            f"vol {c['volume']} splits {c['split_rows']} -> {'ARTIFACT' if c['artifact'] else 'no change'} "
            f"({c['why']})" for c in leg["steps"]]
    out += [f"  split row {t['symbol']} {t['ratio']} on {t['date']}: adj x{t['adj_step']} close x{t['close_step']}, "
            f"{'UNADJUSTED' if t['unadjusted'] else 'absorbed'}" for t in leg["split_rows_in_windows"]]
    out += [f"  rebuilt {e['key']} ({e['kind']}): {e['r_booked']:+.4f} -> {e['r_rebuilt']:+.4f} "
            f"in a book of {e['n']}, leg excess {e['leg_excess_move']:+.3f}pp" for e in leg["rebuilds"]]
    out += [f"  no change, a person's call: {e['key']} ({e['kind']}), leg excess {e['leg_excess_move']:+.3f}pp"
            for e in leg["not_artifact"]]
    return out + [f"  OVER THE BAR: {o}" for o in leg["over_bar"]]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("results", nargs="+", help="results files (a leg each)")
    ap.add_argument("--json", required=True, metavar="OUT", help="the record, one entry per leg")
    ap.add_argument("--not-artifact", action="append", default=[], metavar="SYMBOL:REBALANCE:DATE",
                    help="a candidate a person called no change, by the key its rebuild prints (repeatable)")
    args = ap.parse_args(argv)
    from cr_client import CetaResearch
    cr = CetaResearch()
    report = {"bar": BAR, "not_artifact": args.not_artifact, "legs": {}}
    for path in args.results:
        with open(path) as fh:
            res = json.load(fh)
        q = queries(res)
        ratio, bars, splits = ([cr.query(sql, verbose=False, limit=1000000, timeout=600) or [] for sql in q]
                               if q else ([], [], []))
        leg = check_leg(res, bars, splits, ratio, args.not_artifact)
        key = leg_key(report["legs"], leg["universe"], path)
        report["legs"][key] = {"file": path, **leg}
        print("\n".join(report_lines(key, leg)), flush=True)
    unmatched = set(args.not_artifact) - {e["key"] for leg in report["legs"].values() for e in leg["not_artifact"]}
    if unmatched:
        ap.error(f"--not-artifact matches no candidate: {', '.join(sorted(unmatched))}")
    with open(args.json, "w") as fh:
        json.dump(report, fh, indent=1, default=str)


if __name__ == "__main__":
    main()
