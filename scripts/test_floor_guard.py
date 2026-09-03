#!/usr/bin/env python3
"""Verification harness for the post-price stock-floor guard (blocker B005).

THE DEFECT THIS GUARDS AGAINST
------------------------------
Every topic holds cash when fewer than MIN_STOCKS names pass the SCREEN. But the
price filter runs AFTER that check and silently drops names whose entry or exit
price is missing. Without a second check, the average of the handful that
survive gets written out as `portfolio_return` with a `stocks_held` that nobody
compares against the floor. fcf-growth XETRA 2001 published a single stock's
-49.4% year that way, and that one number set the reported max drawdown.

WHAT THIS PROVES
----------------
S1  guard fires: screen passes floor+5 names, only 3 of them price, so the
    period must be recorded as CASH (stocks_held 0, portfolio_return 0.0) and
    the holdings string must name both counts so the re-run population stays
    greppable.
S2  guard is inert: all screened names price, so the record must be byte-for-byte
    what the topic produced before the guard existed. This is the no-regression
    half, and it is what makes S1 meaningful: a guard that fires always would
    pass S1 and silently zero out every strategy.
S3  holdings tells the truth: screen passes floor+5, exactly floor+2 price. The
    book stays ABOVE the floor so the guard must stay quiet, but `holdings` must
    name the floor+2 names actually held, not the floor+5 that were screened.
    S2 cannot catch this: when every name prices, the pre-filter and post-filter
    lists are identical, so the misreporting is invisible. S3 is the only check
    that fails on `','.join(symbols)`.

It never touches DuckDB, the network, or a real backtest. `screen_stocks`,
`get_prices` and `get_benchmark_return` are imported into each topic's own module
namespace, so `run_backtest` resolves them from module globals and rebinding the
module attribute is enough to stand them in.

Usage:
    python scripts/test_floor_guard.py                # run S1+S2 over every floor topic
    python scripts/test_floor_guard.py --topic low-debt --verbose
    python scripts/test_floor_guard.py --baseline out.json    # dump the S2 series
    python scripts/test_floor_guard.py --compare out.json     # diff S2 against a dump
"""
import argparse
import ast
import datetime
import importlib.util
import json
import pathlib
import sys
import traceback

ROOT = pathlib.Path(__file__).resolve().parent.parent

ENTRY = datetime.date(2020, 1, 2)
EXIT = datetime.date(2021, 1, 4)
ENTRY_PX = 100.0
EXIT_PX = 110.0          # every priced name returns exactly +10%
EXPECTED_RET = 0.10
BENCH_RET = 0.05
BIG_MCAP = 5.0e9         # above every tiered_cost breakpoint, keeps costs flat

FLOOR_NAMES = ("MIN_STOCKS", "MIN_PORTFOLIO_STOCKS")

# Topics whose run_backtest is not a (con, rebalance_dates, ...) period loop, or
# whose data boundary is not the screen_stocks/get_prices pair. All four are
# already-guarded topics that this change touches only with the one-line
# period_data insert, so they are covered by the static gates in verify.py
# (AST byte-identity of the guard block + schema scan) instead.
STATIC_ONLY = {
    "graham-timing",     # run_backtest(exchanges) drives its own connection
    "sector-momentum",   # screen_sectors + get_prices_at
    "sector-rotation",   # screen_sectors + get_prices_at
    "capex-efficiency",  # different record schema; handled by its own case below
}

SCREEN_FN = {           # topics whose screen function is not named screen_stocks
    "dogs-of-dow": "screen_dogs",
}


def load(topic):
    path = ROOT / topic / "backtest.py"
    name = "bt_" + topic.replace("-", "_")
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def floor_of(mod):
    for n in FLOOR_NAMES:
        if hasattr(mod, n):
            return n, getattr(mod, n)
    raise AssertionError("no floor constant")


def run_backtest_kwargs(mod, verbose=False):
    """Build a call for whatever run_backtest signature this topic has."""
    import inspect
    sig = inspect.signature(mod.run_backtest)
    kw = {}
    for name, p in sig.parameters.items():
        if name == "con":
            kw[name] = None
        elif name == "rebalance_dates":
            kw[name] = [ENTRY, EXIT]
        elif name == "verbose":
            kw[name] = verbose
        elif name == "use_costs":
            kw[name] = False           # keep the expected return exactly +10%
        elif name in ("mktcap_min", "small_cap_min", "cap_min"):
            kw[name] = 0.0
        elif name in ("small_cap_max", "cap_max"):
            kw[name] = 1.0e15
        elif name == "ey_threshold":
            kw[name] = 0.0
        elif p.default is not inspect.Parameter.empty:
            continue                   # leave every other optional at its default
        else:
            kw[name] = None
    return kw


class Stubs:
    """Fake data boundary. Records how often the benchmark was fetched so a
    double-fetch introduced by a bad guard shows up as a failure."""

    def __init__(self, symbols, priced):
        self.symbols = list(symbols)
        self.priced = set(priced)
        self.bench_calls = 0

    def screen(self, *a, **k):
        return self._shaped

    def get_prices(self, con, symbols, target_date, *a, **k):
        px = ENTRY_PX if target_date == ENTRY else EXIT_PX
        out = {}
        for s in symbols:
            if s == "SPY" or s in self.priced:
                out[s] = px
        return out

    def get_prices_at(self, con, symbols, target_date, *a, **k):
        return self.get_prices(con, symbols, target_date, *a, **k)

    def get_benchmark_return(self, *a, **k):
        self.bench_calls += 1
        return BENCH_RET


def shape(symbols, arity):
    if arity == 1:
        return list(symbols)
    fill = [BIG_MCAP, 0.25, 0.30, 0.35, 0.40, 0.45]
    return [tuple([s] + fill[:arity - 1]) for s in symbols]


def attempt(topic, n_screen, n_priced, arity, verbose=False, drop_first=False):
    """One run at a fixed screen arity. Returns (results, stubs).

    drop_first prices the LAST n_priced names, so the unpriced ones sort to the
    front of the screened list. Most topics truncate holdings to the first 10
    names; dropping from the tail would hide the leak behind that truncation.
    """
    mod = load(topic)
    _, floor = floor_of(mod)
    symbols = [f"S{i:03d}" for i in range(n_screen)]
    priced = symbols[n_screen - n_priced:] if drop_first else symbols[:n_priced]
    stubs = Stubs(symbols, priced)
    stubs._shaped = shape(symbols, arity)

    setattr(mod, SCREEN_FN.get(topic, "screen_stocks"), stubs.screen)
    for name in ("get_prices", "get_prices_at", "get_benchmark_return"):
        if hasattr(mod, name):
            setattr(mod, name, getattr(stubs, name))
    # cyclical-timing gates the whole book on a macro signal; force it invested
    if hasattr(mod, "compute_expansion_signal"):
        mod.compute_expansion_signal = lambda *a, **k: (True, 1.0, 50)

    results = mod.run_backtest(**run_backtest_kwargs(mod, verbose=verbose))
    return results, stubs, floor


_ARITY_CACHE = {}


def probe_arity(topic, n_screen):
    """Pick the screen tuple arity by running the ALL-PRICED case and requiring a
    fully invested book.

    'First arity that does not raise' is not safe once a floor guard exists: the
    guard's `continue` skips the rest of the loop body, so a wrong arity that
    used to raise now returns a cash record instead, and the probe would lock in
    the wrong shape and call the result a pass. Requiring stocks_held == n_screen
    forces the probe through the full invested path, which is the only place the
    arity actually has to be right.
    """
    if topic in _ARITY_CACHE:
        return _ARITY_CACHE[topic]
    last = None
    for arity in (2, 1, 3, 4, 5, 6):
        try:
            res, stubs, floor = attempt(topic, n_screen, n_screen, arity)
            rec = res[-1] if res else None
            held = rec.get("stocks_held", rec.get("n_stocks")) if rec else None
            if held == n_screen:
                _ARITY_CACHE[topic] = arity
                return arity
            last = (arity, AssertionError(f"arity {arity} gave stocks_held={held}"), "")
        except Exception as e:
            last = (arity, e, traceback.format_exc())
    raise AssertionError(f"no workable screen arity for {topic}: {last[1]!r}\n{last[2]}")


def run_case(topic, n_screen, n_priced, verbose=False, drop_first=False):
    arity = probe_arity(topic, n_screen)
    return attempt(topic, n_screen, n_priced, arity, verbose=verbose,
                   drop_first=drop_first)


def check(topic, verbose=False):
    """Returns (s1_ok, s2_ok, detail dict)."""
    mod = load(topic)
    _, floor = floor_of(mod)
    n_screen = floor + 5
    n_priced_s1 = 3
    det = {"floor": floor, "n_screen": n_screen}

    # ---- S1: only 3 of the screened names price -> must be CASH
    s1_ok = False
    try:
        res, stubs, _ = run_case(topic, n_screen, n_priced_s1, verbose)
        rec = res[-1] if res else None
        det["s1_record"] = rec
        det["s1_bench_calls"] = stubs.bench_calls
        want_holdings = f"CASH ({n_priced_s1} priced of {n_screen} screened)"
        if topic == "capex-efficiency":
            s1_ok = (rec is not None
                     and rec.get("n_stocks") == 0
                     and rec.get("stocks_held") == 0
                     and rec.get("return") == 0.0
                     and rec.get("msg", "").startswith(
                         f"cash ({n_priced_s1} priced of {n_screen} screened)"))
        else:
            # At most one benchmark fetch: a guard that refetches where the topic
            # already had the value in scope issues a second query. Zero is legal
            # too, for the topics that price SPY through get_prices instead of
            # get_benchmark_return (dcf-threshold, quality-momentum).
            s1_ok = (rec is not None
                     and rec.get("stocks_held") == 0
                     and rec.get("portfolio_return") == 0.0
                     and rec.get("spy_return") is not None
                     and stubs.bench_calls <= 1
                     and rec.get("holdings") == want_holdings)
        det["s1_want_holdings"] = want_holdings
    except Exception as e:
        det["s1_error"] = f"{e.__class__.__name__}: {e}"
        if verbose:
            det["s1_traceback"] = traceback.format_exc()

    # ---- S2: every screened name prices -> guard must be inert
    s2_ok = False
    try:
        res, stubs, _ = run_case(topic, n_screen, n_screen, verbose)
        rec = res[-1] if res else None
        det["s2_record"] = rec
        det["s2_bench_calls"] = stubs.bench_calls
        if topic == "capex-efficiency":
            s2_ok = (rec is not None
                     and rec.get("n_stocks") == n_screen
                     and rec.get("stocks_held") == n_screen
                     and abs(rec.get("return", 0) - EXPECTED_RET) < 1e-9)
        else:
            held = rec.get("holdings", "") if rec else ""
            named = [x for x in held.replace("...", "").split(",") if x.strip()]
            # holdings must name the PRICED book, never the pre-filter screen list
            holdings_ok = bool(named) and all(n.strip().split("(")[0] in
                                              {f"S{i:03d}" for i in range(n_screen)}
                                              for n in named)
            s2_ok = (rec is not None
                     and rec.get("stocks_held") == n_screen
                     and abs(rec.get("portfolio_return", 0) - EXPECTED_RET) < 1e-9
                     and holdings_ok)
    except Exception as e:
        det["s2_error"] = f"{e.__class__.__name__}: {e}"
        if verbose:
            det["s2_traceback"] = traceback.format_exc()

    # ---- S3: floor+2 of floor+5 price -> invested, but holdings must name only
    # the priced book. This is the only scenario that can see the (c) defect.
    s3_ok = False
    n_priced_s3 = floor + 2
    try:
        res, stubs, _ = run_case(topic, n_screen, n_priced_s3, verbose, drop_first=True)
        rec = res[-1] if res else None
        det["s3_record"] = rec
        if topic == "capex-efficiency":
            s3_ok = rec is not None and rec.get("n_stocks") == n_priced_s3
        else:
            held = rec.get("holdings", "") if rec else ""
            truncated = "..." in held
            named = [x.strip().split("(")[0].strip()
                     for x in held.replace("...", "").split(",") if x.strip()]
            # the first (n_screen - n_priced_s3) names were never priced
            dropped = {f"S{i:03d}" for i in range(n_screen - n_priced_s3)}
            # The requirement is honesty, not completeness: no name that was never
            # priced may appear, and the list may not claim more names than were
            # held. How far a topic truncates the display (10, 20, all, with or
            # without an ellipsis) is a formatting choice and not this check's
            # business.
            leaks = [n for n in named if n in dropped]
            s3_ok = (rec is not None
                     and rec.get("stocks_held") == n_priced_s3
                     and not leaks
                     and len(named) <= n_priced_s3)
            det["s3_leaks"] = leaks
    except Exception as e:
        det["s3_error"] = f"{e.__class__.__name__}: {e}"
        if verbose:
            det["s3_traceback"] = traceback.format_exc()

    return s1_ok, s2_ok, s3_ok, det


def floor_topics():
    sys.path.insert(0, str(pathlib.Path(__file__).parent))
    topics = []
    for path in sorted(ROOT.glob("*/backtest.py")):
        src = path.read_text()
        tree = ast.parse(src)
        has_floor = any(
            isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id in FLOOR_NAMES for t in n.targets)
            for n in tree.body)
        if has_floor:
            topics.append(path.parent.name)
    return topics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--baseline")
    ap.add_argument("--compare")
    args = ap.parse_args()

    topics = [args.topic] if args.topic else floor_topics()
    topics = [t for t in topics if t not in STATIC_ONLY or args.topic]

    rows, series = {}, {}
    for t in topics:
        if t in STATIC_ONLY and not args.topic:
            continue
        s1, s2, s3, det = check(t, verbose=args.verbose)
        rows[t] = (s1, s2, s3, det)
        series[t] = {"s2_record": det.get("s2_record"),
                     "s1_record": det.get("s1_record"),
                     "s3_record": det.get("s3_record")}

    s1_pass = sorted(t for t, r in rows.items() if r[0])
    s1_fail = sorted(t for t, r in rows.items() if not r[0])
    s2_pass = sorted(t for t, r in rows.items() if r[1])
    s2_fail = sorted(t for t, r in rows.items() if not r[1])
    s3_pass = sorted(t for t, r in rows.items() if r[2])
    s3_fail = sorted(t for t, r in rows.items() if not r[2])

    print(f"topics exercised: {len(rows)}   (static-only, excluded: {sorted(STATIC_ONLY)})")
    print(f"S1 guard fires      PASS {len(s1_pass):3d}   FAIL {len(s1_fail):3d}")
    print(f"S2 guard inert      PASS {len(s2_pass):3d}   FAIL {len(s2_fail):3d}")
    print(f"S3 holdings honest  PASS {len(s3_pass):3d}   FAIL {len(s3_fail):3d}")
    if s1_fail:
        print("\nS1 FAIL (no cash record when the book fell below the floor):")
        for t in s1_fail:
            d = rows[t][3]
            err = d.get("s1_error")
            rec = d.get("s1_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held') if rec else None} "
                                   f"holdings={str(rec.get('holdings') if rec else None)[:60]!r}"))
    if s2_fail:
        print("\nS2 FAIL (guard fired when it should not have, or return wrong):")
        for t in s2_fail:
            d = rows[t][3]
            err = d.get("s2_error")
            rec = d.get("s2_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held') if rec else None} "
                                   f"ret={rec.get('portfolio_return') if rec else None} "
                                   f"holdings={str(rec.get('holdings') if rec else None)[:50]!r}"))
    if s3_fail:
        print("\nS3 FAIL (holdings names stocks that were never priced/held):")
        for t in s3_fail:
            d = rows[t][3]
            err = d.get("s3_error")
            rec = d.get("s3_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held') if rec else None} "
                                   f"leaked={d.get('s3_leaks')} "
                                   f"holdings={str(rec.get('holdings') if rec else None)[:60]!r}"))

    if args.verbose and args.topic:
        print(json.dumps(rows[args.topic][3], indent=2, default=str))

    if args.baseline:
        pathlib.Path(args.baseline).write_text(json.dumps(series, indent=1, default=str))
        print(f"\nbaseline written: {args.baseline}")

    if args.compare:
        old = json.loads(pathlib.Path(args.compare).read_text())
        drift = []
        for t, cur in series.items():
            prev = old.get(t)
            if prev is None:
                continue
            a, b = prev.get("s2_record"), cur.get("s2_record")
            if not a or not b:
                continue
            for k in ("portfolio_return", "stocks_held", "spy_return", "return", "n_stocks"):
                if k in a and a.get(k) != b.get(k):
                    drift.append((t, k, a.get(k), b.get(k)))
        print(f"\nS2 series vs {args.compare}: "
              f"{'IDENTICAL on portfolio_return/stocks_held/spy_return' if not drift else 'DRIFT'}")
        for d in drift:
            print("   ", d)
        if drift:
            return 1

    return 0 if not s1_fail and not s2_fail else 1


if __name__ == "__main__":
    sys.exit(main())
