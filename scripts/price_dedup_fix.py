#!/usr/bin/env python3
"""Price the one-slot-per-company fix WITHOUT committing to it.

Runs each strategy's own backtest twice through the same harness — once as shipped, once with
screen_stocks wrapped so it fetches wide, collapses to one line per company, then truncates to
MAX_STOCKS. That is what the real guard does (dedup BEFORE the top-N cut), so the DELTA between
the two runs is the fix's impact.

⚠️ Trust the DELTA, not the absolutes. On rd-efficiency a harness like this reproduced 4.93%
against a published 4.62% while predicting the MaxDD move to within 0.05pp. Both legs here run
through the identical harness, so the delta is apples to apples even where the level is not.

Changes nothing: no file is written outside the report, no content is touched.

Usage: python3 scripts/price_dedup_fix.py --strategies dcf-threshold,fcf-yield [--exchange ...]
"""
import argparse, contextlib, importlib.util, io, json, os, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from cr_client import CetaResearch

OUT = os.path.join(ROOT, 'scripts', 'dedup_pricing.jsonl')


def load(strategy):
    spec = importlib.util.spec_from_file_location(
        f'p_{strategy}', os.path.join(ROOT, strategy, 'backtest.py'))
    m = importlib.util.module_from_spec(spec)
    with contextlib.redirect_stdout(io.StringIO()):
        spec.loader.exec_module(m)
    return m


def call_run_single(m, cr, exchanges, uni, mktcap, out_path):
    """run_single signatures differ slightly across strategies; try the known shapes."""
    import inspect
    sig = inspect.signature(m.run_single)
    kw = {}
    for name, p in sig.parameters.items():
        if name in ('cr', 'client'): kw[name] = cr
        elif name == 'exchanges': kw[name] = exchanges
        elif name in ('universe_name', 'uni_name'): kw[name] = uni
        elif name == 'frequency': kw[name] = getattr(m, 'DEFAULT_FREQUENCY', 'annual')
        elif name == 'use_costs': kw[name] = True
        elif name in ('risk_free_rate', 'rfr'): kw[name] = 0.02
        elif 'mktcap' in name: kw[name] = mktcap
        elif name == 'verbose': kw[name] = False
        elif name in ('output_path', 'output'): kw[name] = out_path
        elif p.default is not inspect.Parameter.empty: kw[name] = p.default
        else: raise TypeError(f'cannot fill run_single arg {name!r}')
    return m.run_single(**kw)


def metrics(d):
    """Normalise a strategy's output JSON to comparable metrics.

    RAISES if it cannot find a portfolio block. Returning a dict of Nones here is exactly the
    silent-skip failure this whole exercise exists to stamp out: piotroski priced for 475s and
    reported None on every field because its output nests under `portfolios`, which read as
    'no impact' rather than 'not measured'.
    """
    if not d:
        raise ValueError('empty output')

    if 'portfolio' in d:                       # the common shape
        p = d['portfolio']
        return {'cagr': p.get('cagr'), 'max_drawdown': p.get('max_drawdown'),
                'sharpe': p.get('sharpe_ratio'), 'total_return': p.get('total_return'),
                'excess_cagr': d.get('excess_cagr'), 'win_rate': d.get('win_rate_vs_spy'),
                'cash_periods': d.get('cash_periods'),
                'avg_stocks': d.get('avg_stocks_when_invested'),
                'bench_maxdd': (d.get('spy') or {}).get('max_drawdown')}

    if 'portfolios' in d:                      # piotroski: tranche comparison
        ps = d['portfolios']
        hi = ps.get('score_8_9') or next(iter(ps.values()))
        bm = ps.get('sp500') or {}
        return {'cagr': hi.get('cagr'), 'max_drawdown': hi.get('max_drawdown'),
                'sharpe': hi.get('sharpe'), 'total_return': hi.get('total_return'),
                # the headline claim for this strategy is the high-minus-low SPREAD
                'excess_cagr': d.get('spread_cagr'),
                'win_rate': None, 'cash_periods': None, 'avg_stocks': None,
                'bench_maxdd': bm.get('max_drawdown'),
                'low_cagr': (ps.get('score_0_2') or {}).get('cagr'),
                'selection_alpha': (d.get('alpha_decomposition') or {}).get('selection_alpha')}

    raise ValueError(f'unrecognised output shape: keys={sorted(d)[:10]}')


def run_via_main(m, exchanges, out_path):
    """Fallback for strategies with no run_single: drive main() with argv, read its output.

    Both legs go through the same entry point, so the delta stays apples to apples even though
    the harness never reimplements the strategy's own cost/offset/benchmark choices.
    """
    argv = ['backtest.py', '--exchange', ','.join(exchanges), '--output', out_path]
    old = sys.argv
    sys.argv = argv
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                m.main()
            except SystemExit:
                pass
    finally:
        sys.argv = old
    with open(out_path) as f:
        return json.load(f)


def price(cr, strategy, exchanges, uni, widen=4):
    m = load(strategy)
    mktcap = getattr(m, 'MKTCAP_MIN', 1_000_000_000)
    tmp = f'/tmp/_price_{strategy}.json'
    use_main = not hasattr(m, 'run_single')

    # ---- baseline, exactly as shipped ----
    if use_main:
        base = metrics_raw = run_via_main(m, exchanges, tmp)
        base = metrics(base)
    else:
        with contextlib.redirect_stdout(io.StringIO()):
            base = call_run_single(m, cr, exchanges, uni, mktcap, tmp)

    # ---- patched: fetch wide, dedup on companyName, truncate ----
    # The screen is not always called screen_stocks (piotroski uses screen_and_score), and
    # hardcoding the name makes the harness ERROR rather than silently mis-measure - but it
    # still means no result. Resolve it the same way the detector does.
    sname = next((n for n in ('screen_stocks', 'screen_and_score', 'screen_quality',
                              'screen_value', 'screen_low_pe', 'screen_dogs')
                  if hasattr(m, n)), None)
    if sname is None:
        raise AttributeError(f'{strategy}: no recognised screen function to patch')
    orig = getattr(m, sname)
    MAX = getattr(m, 'MAX_STOCKS', 30)
    name_cache = {}

    def names_for(syms):
        miss = [s for s in syms if s not in name_cache]
        if miss:
            rows = cr.query("SELECT symbol, companyName FROM profile WHERE symbol IN (%s)"
                            % ",".join(f"'{s}'" for s in miss), verbose=False)
            got = {r['symbol']: r['companyName'] for r in rows}
            for s in miss:
                name_cache[s] = got.get(s) or s
        return name_cache

    def patched(con, target_date, *a, **k):
        m.MAX_STOCKS = MAX * widen
        try:
            rows = orig(con, target_date, *a, **k)
        finally:
            m.MAX_STOCKS = MAX

        # Some screens return {symbol: (score, marketCap)} and form the book by threshold
        # rather than a top-N cut (piotroski). There is nothing to widen or truncate there —
        # just collapse to one line per company, keeping the largest marketCap.
        if isinstance(rows, dict):
            nm = names_for(list(rows))
            best = {}
            for sym, v in rows.items():
                n = nm.get(sym, sym)
                mc = v[1] if isinstance(v, (list, tuple)) and len(v) > 1 else 0
                mc = mc or 0
                # ⚠️ marketCap TIES ARE THE COMMON CASE: FMP gives every line of a company
                # the same cap, so RGA/RZA/RZB/RZC all tie. Without a deterministic
                # tiebreak the winner depends on SQL row order, which DuckDB does not
                # guarantee — two runs then price different books. The real guard in
                # rd-efficiency/backtest.py breaks ties on `inc.symbol` for exactly this
                # reason; mirror it.
                cand = (-mc, sym)
                if n not in best or cand < best[n][0]:
                    best[n] = (cand, sym)
            keep_syms = {sym for _, sym in best.values()}
            return {s_: v for s_, v in rows.items() if s_ in keep_syms}

        syms = [r[0] for r in rows]
        nm = names_for(syms)
        # Keep the strategy's own ranking, but when one company appears more than once
        # choose which line survives deterministically (largest marketCap, then symbol)
        # rather than whichever the scan happened to emit first.
        by_name = {}
        for i, r in enumerate(rows):
            n = nm.get(r[0], r[0])
            mc = r[1] if len(r) > 1 and isinstance(r[1], (int, float)) else 0
            cand = (-(mc or 0), r[0])
            if n not in by_name or cand < by_name[n][0]:
                by_name[n] = (cand, i, r)
        winners = {v[1] for v in by_name.values()}
        keep = [r for i, r in enumerate(rows) if i in winners][:MAX]
        return keep

    setattr(m, sname, patched)
    try:
        if use_main:
            fixed = metrics(run_via_main(m, exchanges, tmp))
        else:
            with contextlib.redirect_stdout(io.StringIO()):
                fixed = call_run_single(m, cr, exchanges, uni, mktcap, tmp)
    finally:
        setattr(m, sname, orig)
    return (base if use_main else metrics(base)), (fixed if use_main else metrics(fixed))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--strategies', required=True)
    ap.add_argument('--exchanges', default='NYSE,NASDAQ,AMEX')
    ap.add_argument('--universe', default='NYSE_NASDAQ_AMEX')
    args = ap.parse_args()
    cr = CetaResearch()
    ex = args.exchanges.split(',')

    done = set()
    if os.path.exists(OUT):
        for l in open(OUT):
            try: done.add(json.loads(l)['strategy'])
            except Exception: pass

    for s in args.strategies.split(','):
        if s in done:
            print(f"{s}: cached, skipping", flush=True); continue
        print(f"pricing {s} ...", flush=True)
        t0 = time.time()
        try:
            # CR occasionally returns a truncated parquet ("invalid end magic bytes"). It is
            # transient and costs a full run, so retry with backoff rather than recording a
            # failure that looks like a result.
            last = None
            for attempt in range(5):
                try:
                    b, f = price(cr, s, ex, args.universe); last = None; break
                except Exception as e:
                    last = e
                    msg = str(e).lower()
                    transient = ('parquet' in msg or 'magic bytes' in msg
                                 or 'rate limit' in msg or '429' in msg)
                    if not transient:
                        raise
                    # The API asks for 60s on a rate limit; honour it rather than hammering.
                    wait = 75 if 'rate limit' in msg else 20
                    print(f"    transient ({type(e).__name__}), retry {attempt+1}/3 in {wait}s",
                          flush=True)
                    time.sleep(wait)
            if last is not None:
                raise last
            rec = {'strategy': s, 'baseline': b, 'deduped': f,
                   'secs': round(time.time() - t0)}
            if b and f:
                rec['delta'] = {k: (None if b.get(k) is None or f.get(k) is None
                                    else round(f[k] - b[k], 3))
                                for k in ('cagr', 'max_drawdown', 'sharpe', 'excess_cagr',
                                          'win_rate', 'avg_stocks', 'total_return',
                                          'low_cagr', 'selection_alpha')}
        except Exception as e:
            import traceback
            rec = {'strategy': s, 'error': f'{type(e).__name__}: {str(e)[:200]}',
                   'trace': traceback.format_exc()[-500:]}
        with open(OUT, 'a') as fh:
            fh.write(json.dumps(rec) + '\n')
        d = rec.get('delta') or {}
        print(f"  {s}: CAGR {d.get('cagr')}  MaxDD {d.get('max_drawdown')}  "
              f"Sharpe {d.get('sharpe')}  ({rec.get('secs','?')}s) {rec.get('error','')}",
              flush=True)


if __name__ == '__main__':
    main()
