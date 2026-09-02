#!/usr/bin/env python3
"""Detect duplicate-listing contamination in every strategy's screen.

rd-efficiency's screen let one company occupy several portfolio slots through its share
classes and secondary lines: Becton Dickinson held 3 of 30 US slots for six consecutive years.
Fixing it moved US CAGR 4.62% -> 4.09% and max drawdown -35.74% -> -39.75%, which crossed SPY's
-38.01% and REVERSED a published risk claim. Every published doc screen already carries a
`QUALIFY ... PARTITION BY companyName` guard; the backtests behind those numbers do not.

This is the cheap detector: run each strategy's screen at a few dates, resolve the symbols to
companyName, and count how many slots a single company takes. It changes nothing.

DETECTION ONLY. It does not fix, rerun, or touch content.

Usage:  python3 scripts/detect_listing_dups.py [--dates 2005-07-01,2015-07-01,2024-07-01]
Output: scripts/listing_dup_report.jsonl  (one line per strategy, written incrementally)
"""
import argparse, datetime, importlib.util, io, json, os, sys, traceback
import contextlib
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from cr_client import CetaResearch

OUT = os.path.join(ROOT, 'scripts', 'listing_dup_report.jsonl')


def load_module(strategy):
    path = os.path.join(ROOT, strategy, 'backtest.py')
    spec = importlib.util.spec_from_file_location(f'bt_{strategy}', path)
    m = importlib.util.module_from_spec(spec)
    with contextlib.redirect_stdout(io.StringIO()):
        spec.loader.exec_module(m)
    return m


def screen_fn(m):
    for name in ('screen_stocks', 'screen_quality', 'screen_value', 'screen_and_score',
                 'screen_low_pe', 'screen_dogs'):
        if hasattr(m, name):
            return name, getattr(m, name)
    return None, None


def run_one(cr, strategy, dates, exchanges):
    m = load_module(strategy)
    fname, fn = screen_fn(m)
    if fn is None:
        return {'strategy': strategy, 'status': 'SKIPPED', 'reason': 'no recognised screen function'}

    fetch = getattr(m, 'fetch_data_via_api', None) or getattr(m, 'fetch_data', None)
    if fetch is None:
        return {'strategy': strategy, 'status': 'SKIPPED', 'reason': 'no recognised fetch function'}

    mktcap = getattr(m, 'MKTCAP_MIN', None) or 1_000_000_000
    con = None
    for attempt in range(3):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                try:
                    con = fetch(cr, exchanges, dates, verbose=False)
                except TypeError:
                    con = fetch(cr, exchanges, mktcap, verbose=False)
            break
        except Exception as e:
            if attempt == 2:
                return {'strategy': strategy, 'status': 'SKIPPED',
                        'reason': f'fetch failed: {type(e).__name__}: {str(e)[:160]}'}
    if con is None:
        return {'strategy': strategy, 'status': 'SKIPPED', 'reason': 'fetch returned None'}

    per_date, all_syms = {}, set()
    for d in dates:
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                rows = fn(con, d, mktcap)
            syms = [r[0] if isinstance(r, (list, tuple)) else r for r in rows]
        except Exception as e:
            per_date[d.isoformat()] = {'error': f'{type(e).__name__}: {str(e)[:120]}'}
            continue
        per_date[d.isoformat()] = {'n': len(syms), 'symbols': syms}
        all_syms.update(syms)

    if not all_syms:
        return {'strategy': strategy, 'status': 'SKIPPED', 'reason': 'screen returned nothing',
                'screen_fn': fname, 'per_date': {k: v.get('error', v.get('n'))
                                                 for k, v in per_date.items()}}

    prof = cr.query("SELECT symbol, companyName FROM profile WHERE symbol IN (%s)"
                    % ",".join(f"'{s}'" for s in all_syms), verbose=False)
    name_of = {r['symbol']: (r['companyName'] or r['symbol']) for r in prof}

    dup_slots, worst, detail = 0, None, {}
    for ds, info in per_date.items():
        if 'symbols' not in info:
            continue
        c = Counter(name_of.get(s, s) for s in info['symbols'])
        dups = {n: k for n, k in c.items() if k > 1}
        extra = sum(k - 1 for k in dups.values())
        dup_slots += extra
        if dups:
            detail[ds] = {'held': info['n'], 'extra_slots': extra,
                          'companies': sorted(dups.items(), key=lambda x: -x[1])[:5]}
            top = max(dups.items(), key=lambda x: x[1])
            if worst is None or top[1] > worst[1]:
                worst = top

    total_slots = sum(v['n'] for v in per_date.values() if 'n' in v)
    return {'strategy': strategy, 'status': 'CONTAMINATED' if dup_slots else 'CLEAN',
            'screen_fn': fname, 'dates_checked': len(detail) or len(per_date),
            'total_slots': total_slots, 'extra_slots': dup_slots,
            'pct_slots': round(dup_slots / total_slots * 100, 2) if total_slots else 0,
            'worst_company': worst, 'detail': detail}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dates', default='2005-07-01,2015-07-01,2024-07-01')
    ap.add_argument('--exchanges', default='NYSE,NASDAQ,AMEX')
    ap.add_argument('--only', default=None, help='comma-separated strategy names')
    args = ap.parse_args()

    dates = [datetime.date.fromisoformat(d) for d in args.dates.split(',')]
    exchanges = args.exchanges.split(',')
    strategies = sorted(d for d in os.listdir(ROOT)
                        if os.path.isfile(os.path.join(ROOT, d, 'backtest.py')))
    if args.only:
        keep = set(args.only.split(','))
        strategies = [s for s in strategies if s in keep]

    cr = CetaResearch()
    done = set()
    if os.path.exists(OUT):
        for line in open(OUT):
            try: done.add(json.loads(line)['strategy'])
            except Exception: pass
        print(f"resuming: {len(done)} already recorded", flush=True)

    for i, s in enumerate(strategies, 1):
        if s in done:
            continue
        print(f"[{i}/{len(strategies)}] {s} ...", end=' ', flush=True)
        try:
            rec = run_one(cr, s, dates, exchanges)
        except Exception as e:
            rec = {'strategy': s, 'status': 'SKIPPED',
                   'reason': f'{type(e).__name__}: {str(e)[:160]}',
                   'trace': traceback.format_exc()[-400:]}
        with open(OUT, 'a') as f:
            f.write(json.dumps(rec) + '\n')
        print(rec['status'] + (f" ({rec.get('extra_slots')} extra slots)"
                               if rec.get('extra_slots') else ''), flush=True)

    # summary — and it REPORTS what it could not check, rather than implying full coverage
    recs = [json.loads(l) for l in open(OUT)]
    con_ = [r for r in recs if r['status'] == 'CONTAMINATED']
    cln = [r for r in recs if r['status'] == 'CLEAN']
    skp = [r for r in recs if r['status'] == 'SKIPPED']
    print(f"\n{'='*70}\nCONTAMINATED {len(con_)}  |  CLEAN {len(cln)}  |  "
          f"NOT CHECKED {len(skp)}  (of {len(recs)})")
    print("\nRanked by share of slots lost to duplicate listings:")
    for r in sorted(con_, key=lambda x: -x['pct_slots']):
        w = r.get('worst_company')
        print(f"  {r['pct_slots']:>5.2f}%  {r['strategy']:<28} "
              f"{r['extra_slots']:>3} extra of {r['total_slots']:>4} slots"
              + (f"   worst: {w[0][:34]} x{w[1]}" if w else ''))
    if skp:
        print(f"\n⚠️  {len(skp)} strategies NOT CHECKED — absence of a finding here is not a "
              f"clean bill:")
        for r in skp:
            print(f"     {r['strategy']:<28} {r.get('reason','')[:90]}")


if __name__ == '__main__':
    main()
