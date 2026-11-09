#!/usr/bin/env python3
"""The final partial period in metrics.py (runs that end on the latest close).

A run that ends on the latest close holds its last rebalance for part of a period. METHODOLOGY.md,
"Run Window", sets how each figure treats that period:

  1 IDENTITY   with no partial period, compute_metrics, compute_annual_returns and period_accounting
               give the base ref's results to the last bit, on many series and every frequency
  2 SPLIT      a partial period counts in total return, CAGR (over the exact span), drawdown, Calmar
               and excess CAGR; volatility, Sortino, VaR, CVaR, best and worst, the share of negative
               periods, the loss streak, skew, kurtosis and the comparison statistics use full periods
               only; Sharpe is the span CAGR less the risk-free rate over the full-period volatility
  3 GUARDS     only the last period may be partial, a fraction lies in [0, 1], lengths match
  4 SPAN       span_years adds the fractions up
  5 YEARS      annual_with_partial keeps full years in annual_returns and puts a short final year,
               labelled as annual_returns labels years, in partial_year; that year is never dropped,
               and the two compound to the whole run
  6 MUTANTS    each rule above, broken in a copy of metrics.py, fails a check

Usage:  python scripts/test_partial_metrics.py [--base REF]   (REF defaults to the merge base with origin/main)
"""
import argparse
import importlib.util
import math
import os
import random
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FAIL = []


def check(cond, what):
    if not cond:
        FAIL.append(what)
        print(f"  FAIL {what}")
    return cond


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def base_ref():
    r = subprocess.run(["git", "merge-base", "HEAD", "origin/main"], cwd=ROOT, capture_output=True, text=True)
    if r.returncode:
        raise SystemExit("FAIL: no merge base with origin/main; pass --base REF")
    return r.stdout.strip()


def load_base(tmp, ref):
    try:
        src = subprocess.run(["git", "show", f"{ref}:metrics.py"], cwd=ROOT, capture_output=True, text=True,
                             check=True).stdout
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"FAIL: cannot read metrics.py at {ref} ({e.stderr.strip()})")
    path = os.path.join(tmp, "metrics_base.py")
    with open(path, "w") as fh:
        fh.write(src)
    return load(path, "metrics_base")


def series(rng, n):
    return [round(rng.gauss(0.02, 0.12), 6) for _ in range(n)]


# ---------------------------------------------------------------- 1 identity

def identity_failures(m, base):
    f = []
    rng = random.Random(682)
    for ppy in (1, 2, 4, 12):
        for n in (2, 3, 7, 26, 104):
            p, b, x = series(rng, n), series(rng, n), series(rng, n)
            for kw in ({}, {"additional_benchmarks": {"X": x}}, {"years": n / ppy + 0.37}):
                want = base.compute_metrics(p, b, ppy, risk_free_rate=0.03, **kw)
                for frac in (None, [1.0] * n):
                    got = m.compute_metrics(p, b, ppy, risk_free_rate=0.03, fractions=frac, **kw)
                    if got != want:
                        f.append(f"compute_metrics ppy={ppy} n={n} {sorted(kw)} fractions={frac is not None}")
            dates = [f"{2000 + i // ppy}-{1 + (i % ppy) * (12 // ppy):02d}-01" for i in range(n)]
            if m.compute_annual_returns(p, b, dates, ppy) != base.compute_annual_returns(p, b, dates, ppy):
                f.append(f"compute_annual_returns ppy={ppy} n={n}")
    rows = [{"rebalance_date": f"{2000 + i}-07-01", "exit_date": f"{2001 + i}-07-01"} for i in range(10)]
    if m.period_accounting(rows, rows[2:], 3, warn=False) != base.period_accounting(rows, rows[2:], 3, warn=False):
        f.append("period_accounting")
    return f


# ---------------------------------------------------------------- 2 split

FULL_P = [0.10, -0.05, 0.20, 0.04, -0.02]
FULL_B = [0.05, 0.02, 0.10, -0.03, 0.01]
PART_P, PART_B, FRAC = -0.30, 0.06, 0.347945   # the partial period: the run's worst move, under half a period


def split_failures(m):
    f = []
    rf, ppy = 0.02, 1
    p, b = FULL_P + [PART_P], FULL_B + [PART_B]
    fr = [1.0] * len(FULL_P) + [FRAC]
    got = m.compute_metrics(p, b, ppy, risk_free_rate=rf, fractions=fr)
    full = m.compute_metrics(FULL_P, FULL_B, ppy, risk_free_rate=rf)
    span = (len(FULL_P) + FRAC) / ppy
    for side, rets, fo in (("portfolio", p, full["portfolio"]), ("benchmark", b, full["benchmark"])):
        g = got[side]
        growth = math.prod(1 + r for r in rets)
        cagr = growth ** (1 / span) - 1
        path, peak, dd = 1.0, 1.0, 0.0
        for r in rets:
            path *= 1 + r
            peak = max(peak, path)
            dd = min(dd, (path - peak) / peak)
        exp = {"total_return": growth - 1, "cagr": cagr, "max_drawdown": dd}
        for k, v in exp.items():
            if not math.isclose(g[k], v, rel_tol=0, abs_tol=1e-12):
                f.append(f"{side}.{k} {g[k]!r}, want {v!r} (all periods, span {span})")
        for k in ("annualized_volatility", "var_95", "cvar_95", "best_period", "worst_period",
                  "pct_negative_periods", "max_consecutive_losses", "skewness", "kurtosis"):
            if g[k] != fo[k]:
                f.append(f"{side}.{k} {g[k]!r}, want the full periods' {fo[k]!r}")
        if not math.isclose(g["sharpe_ratio"], (cagr - rf) / fo["annualized_volatility"], abs_tol=1e-12):
            f.append(f"{side}.sharpe_ratio {g['sharpe_ratio']!r}: not (span CAGR - rf) / full-period volatility")
        if fo["sortino_ratio"] is not None:
            dd_full = (fo["cagr"] - rf) / fo["sortino_ratio"]   # the full periods' downside deviation
            if not math.isclose(g["sortino_ratio"], (cagr - rf) / dd_full, abs_tol=1e-12):
                f.append(f"{side}.sortino_ratio {g['sortino_ratio']!r}: not span CAGR over full-period downside")
        if not math.isclose(g["calmar_ratio"], cagr / abs(dd), abs_tol=1e-12):
            f.append(f"{side}.calmar_ratio {g['calmar_ratio']!r}: not span CAGR / all-period drawdown")
    # the portfolio's drawdown opens at period 5 and is still open after the partial period: 2 periods
    if got["portfolio"]["max_dd_duration_periods"] != 2:
        f.append(f"portfolio.max_dd_duration_periods {got['portfolio']['max_dd_duration_periods']}, want 2")
    c, cf = got["comparison"], full["comparison"]
    for k in ("win_rate", "tracking_error", "information_ratio", "up_capture", "down_capture", "beta"):
        if c[k] != cf[k]:
            f.append(f"comparison.{k} {c[k]!r}, want the full periods' {cf[k]!r}")
    pc, bc = got["portfolio"]["cagr"], got["benchmark"]["cagr"]
    if not math.isclose(c["excess_cagr"], pc - bc, abs_tol=1e-12):
        f.append("comparison.excess_cagr is not the span CAGRs' difference")
    alpha = pc - (rf + cf["beta"] * (bc - rf))
    if not math.isclose(c["alpha"], alpha, abs_tol=1e-12):
        f.append("comparison.alpha is not from the span CAGRs and the full-period beta")
    # an extra benchmark gets the same split
    x = [0.03, 0.01, 0.05, 0.02, 0.00, 0.40]
    xa = m.compute_metrics(p, b, ppy, risk_free_rate=rf, fractions=fr, additional_benchmarks={"X": x})
    xm = xa["additional_benchmarks"]["X"]["metrics"]
    xf = m.compute_metrics(FULL_P, x[:-1], ppy, risk_free_rate=rf)["benchmark"]
    if xm["best_period"] != xf["best_period"] or not math.isclose(
            xm["total_return"], math.prod(1 + r for r in x) - 1, abs_tol=1e-12):
        f.append("additional benchmark: not split like the primary one")
    # a fraction of 1 is a full period
    if m.compute_metrics(p, b, ppy, risk_free_rate=rf, fractions=[1.0] * len(p)) != \
            m.compute_metrics(p, b, ppy, risk_free_rate=rf):
        f.append("fractions all 1 differ from no fractions")
    # fewer than two full periods: nothing to measure
    short = m.compute_metrics([0.1, 0.2], [0.0, 0.1], 1, fractions=[1.0, 0.5])
    if short["portfolio"]["cagr"] is not None:
        f.append("one full period and a partial one: want empty metrics")
    return f


# ---------------------------------------------------------------- 3 guards

def guard_failures(m):
    f = []
    p, b = [0.1, 0.2, 0.3], [0.0, 0.1, 0.2]
    for bad, why in (([0.5, 1.0, 1.0], "a partial period before the last"),
                     ([1.0, 1.0, 1.2], "a fraction above 1"),
                     ([1.0, 1.0, -0.1], "a negative fraction"),
                     ([1.0, 1.0], "fractions shorter than the returns")):
        try:
            m.compute_metrics(p, b, 1, fractions=bad)
            f.append(f"compute_metrics accepted {why}")
        except ValueError:
            pass
    return f


# ---------------------------------------------------------------- 4 span

def span_failures(m):
    f = []
    for fr, ppy, want in (([1.0] * 26 + [0.347945], 1, 26.347945), ([1.0] * 7 + [0.5], 4, 1.875),
                          ([1.0] * 10, 2, 5.0)):
        got = m.span_years(fr, ppy)
        if not math.isclose(got, want, abs_tol=1e-12):
            f.append(f"span_years({len(fr)} periods, ppy={ppy}) = {got}, want {want}")
    return f


# ---------------------------------------------------------------- 5 years

def grid(start, end_exclusive, months):
    return [f"{y}-{mo:02d}-01" for y in range(start, end_exclusive) for mo in months]


def years_failures(m, base):
    f = []
    rng = random.Random(68)

    def run(name, dates, exits, fr, ppy, want_year, want_periods, want_yf):
        n = len(dates)
        p, b = series(rng, n), series(rng, n)
        annual, part = m.annual_with_partial(p, b, dates, exits, fr, ppy)
        last = int(dates[-1][:4])
        keep = [i for i, d in enumerate(dates) if int(d[:4]) != last] if want_year else list(range(n))
        want_annual = base.compute_annual_returns([p[i] for i in keep], [b[i] for i in keep],
                                                  [dates[i] for i in keep], ppy)
        if annual != want_annual:
            f.append(f"{name}: annual_returns are not the base's over the full years")
        if want_year is None:
            if part is not None:
                f.append(f"{name}: a partial_year on a run whose final year is full: {part}")
            return
        if part is None:
            return f.append(f"{name}: no partial_year")
        idx = [i for i, d in enumerate(dates) if int(d[:4]) == want_year]
        exp = {"year": want_year, "start": dates[idx[0]], "end": exits[idx[-1]], "periods": want_periods,
               "year_fraction": want_yf}
        for k, v in exp.items():
            if part.get(k) != v:
                f.append(f"{name}: partial_year.{k} {part.get(k)!r}, want {v!r}")
        gp = math.prod(1 + p[i] for i in idx) - 1
        gb = math.prod(1 + b[i] for i in idx) - 1
        for k, v in (("portfolio", gp), ("benchmark", gb), ("excess", gp - gb)):
            if not math.isclose(part.get(k, math.nan), v, abs_tol=1e-12):
                f.append(f"{name}: partial_year.{k} {part.get(k)!r}, want {v!r}")
        if any(a["year"] == want_year for a in annual):
            f.append(f"{name}: {want_year} is in annual_returns too")
        whole = math.prod(1 + r for r in p)
        parts = math.prod(1 + a["portfolio"] for a in annual) * (1 + part["portfolio"])
        if not math.isclose(whole, parts, rel_tol=1e-12):
            f.append(f"{name}: annual_returns x partial_year != the whole run ({parts} vs {whole})")

    # annual July strategy to 2026-11-06: 26 full years, 2026 partial
    d = grid(2000, 2027, [7])
    x = d[1:] + ["2026-11-06"]
    run("annual, partial final period", d, x, [1.0] * 26 + [0.347945], 1, 2026, 1, 0.347945)
    # quarterly to 2026-11-06: 2026 holds three full quarters and the partial one
    d = grid(2000, 2027, [1, 4, 7, 10])
    x = d[1:] + ["2026-11-06"]
    run("quarterly, partial final period", d, x, [1.0] * (len(d) - 1) + [0.39], 4, 2026, 4, round(3.39 / 4, 6))
    # quarterly whose final stub was under 7 days: 2026 is one full quarter, kept (today's rule drops it)
    d = grid(2000, 2026, [1, 4, 7, 10]) + ["2026-01-01"]
    x = d[1:] + ["2026-04-01"]
    run("quarterly, one full quarter in the final year", d, x, [1.0] * len(d), 4, 2026, 1, 0.25)
    # semi-annual to 2026-03-13: 2026 holds the partial January period only
    d = grid(2000, 2027, [1, 7])[:-1]
    x = d[1:] + ["2026-03-13"]
    run("semi-annual, partial final period", d, x, [1.0] * (len(d) - 1) + [0.39], 2, 2026, 1, 0.195)
    # annual run ending on a grid date: every year full, no partial_year
    d = grid(2000, 2026, [7])
    x = d[1:] + ["2026-07-01"]
    run("annual, final year full", d, x, [1.0] * len(d), 1, None, None, None)
    return f


# ---------------------------------------------------------------- 6 mutants

MUTANTS = [
    # (name, old text, new text): each must make a check above fail
    ("volatility over every period", "_compute_series_metrics(rets[:full], ppy", "_compute_series_metrics(rets, ppy"),
    ("CAGR over the period count", "if partial and years is None:", "if False:"),
    ("growth path without the partial period", "path = returns if path_returns is None else path_returns",
     "path = returns"),
    ("drawdown duration counted over full periods", "duration = len(path) - current_dd_start",
     "duration = n - current_dd_start"),
    ("comparison over every period", "comp = _compute_comparison(period_returns[:full], benchmark_returns[:full]",
     "comp = _compute_comparison(period_returns, benchmark_returns"),
    ("extra benchmark not split", "ab_metrics = series(bench_rets)",
     "ab_metrics = _compute_series_metrics(bench_rets, ppy, risk_free_rate, years=years)"),
    ("a partial period before the last", "if any(f != 1 for f in fractions[:-1]):", "if False:"),
    ("a fraction outside [0, 1]", "if n and not 0 <= fractions[-1] <= 1:", "if False:"),
    ("span counts periods", "return sum(fractions) / periods_per_year", "return len(fractions) / periods_per_year"),
    ("short final year left in annual_returns", "if year_fraction >= 1:", "if year_fraction >= 0:"),
    ("partial_year ends at its last rebalance", '"end": exit_dates[idx[-1]]', '"end": period_dates[idx[-1]]'),
]


def run_all(m, base):
    out = []
    for name, fn in (("1 IDENTITY", lambda: identity_failures(m, base)), ("2 SPLIT", lambda: split_failures(m)),
                     ("3 GUARDS", lambda: guard_failures(m)), ("4 SPAN", lambda: span_failures(m)),
                     ("5 YEARS", lambda: years_failures(m, base))):
        try:
            out += [f"{name}: {x}" for x in fn()]
        except Exception as e:   # a missing helper or a crash is a failure, not a pass
            out.append(f"{name}: {type(e).__name__}: {e}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None)
    a = ap.parse_args()
    ref = a.base or base_ref()
    with tempfile.TemporaryDirectory() as tmp:
        base = load_base(tmp, ref)
        import metrics as m
        print(f"base {ref[:12]}")
        for x in run_all(m, base):
            check(False, x)
        print("6 MUTANTS")
        src = open(os.path.join(ROOT, "metrics.py")).read()
        for name, old, new in MUTANTS:
            if src.count(old) != 1:
                check(False, f"mutant {name}: anchor found {src.count(old)} times")
                continue
            path = os.path.join(tmp, f"mut_{len(FAIL)}_{abs(hash(name))}.py")
            with open(path, "w") as fh:
                fh.write(src.replace(old, new))
            check(bool(run_all(load(path, "metrics_mut"), base)), f"mutant {name} passes every check")
    print("FAIL" if FAIL else "PASS", f"({len(FAIL)} failures)")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
