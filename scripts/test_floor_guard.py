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
S4  the guard does NOT read the exit date: every screened name prices at ENTRY,
    only floor-1 of them price at EXIT. The strategy could buy the whole book on
    the rebalance date, so the period is INVESTED and the surviving names are
    averaged. A guard on the post-filter book turns this into a flat 0.0% cash
    period and erases a realised result -- the exact look-ahead this file exists
    to make unreintroducible.
S5  same, for the other exit-conditioned drop: every name prices at both dates,
    but floor+5-(floor-1) of them return +1000% and trip max_single_return.
    Nothing about that is knowable at entry either. S5 is NOT redundant with S4:
    a half-fix that only special-cases the missing-exit-price drop passes S4 and
    still fails here.

S6  zero survivors: every screened name buyable at ENTRY, NONE surviving to the
    exit side -- via lost exit prices (S6a) and via every name tripping
    max_single_return (S6b). Correcting the guard MADE this branch reachable,
    and 4 of the 66 topics divided by the empty book there and raised
    ZeroDivisionError. S6 is a CRASH regression, not a look-ahead test: a
    zero-survivor period is legitimately 0.0% under either guard, so S6 cannot
    tell a reverted guard from a correct one. It asserts only that the period is
    reached, recorded and does not raise.

S4 and S5 are what makes the correction stick, and S6 keeps the correction from
crashing, so all three gate the exit code.

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
import itertools
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

# Topics that cannot be driven dynamically and are covered by the static gates
# in verify_floor_guard.py instead.
#
# This set USED to hold graham-timing, sector-momentum and sector-rotation, and
# that was a hole, not a simplification. All three carried the look-ahead on
# main, so they were the highest-risk topics in the population, and the static
# gate anchors on the bound name `buyable` rather than on how the counted
# collection was built -- so swapping entry_buyable_prices(symbols, entry_map)
# back to entry_buyable(symbol_data) fully reintroduced the look-ahead with
# every gate and every test green. They are driven now (see DRIVERS below), and
# gate 3 checks construction-site provenance as a second line.
STATIC_ONLY = set()

SCREEN_FN = {           # topics whose screen function is not named screen_stocks
    "dogs-of-dow": "screen_dogs",
}

# S5 is VACUOUS for these two and must be reported n/a, never PASS. Neither
# calls filter_returns; their inline filter is `if ep and xp and ep > 0` with no
# return cap at all, so a +1000% name is simply kept, the book stays at n_screen
# and the period is invested under any predicate. Printing PASS here would be
# the "a check that matches nothing looks like a check that finds nothing"
# failure the rest of this harness is built to avoid.
NO_RETURN_CAP = {"qarp", "low-pe"}


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
    double-fetch introduced by a bad guard shows up as a failure.

    entry_priced and exit_priced are SEPARATE sets, so a name can be priced at
    entry and unpriced at exit -- the delisting case, which is the one the
    original B005 guard got wrong and which the single `priced` set could not
    express. exit_px overrides the exit price per symbol, so a realised return
    above max_single_return is expressible too.
    """

    def __init__(self, symbols, priced=None, entry_priced=None,
                 exit_priced=None, exit_px=None, n_qualifying=6):
        self.symbols = list(symbols)
        if priced is not None:          # legacy S1/S2/S3 form: same at both dates
            entry_priced = exit_priced = priced
        self.entry_priced = set(entry_priced or ())
        self.exit_priced = set(exit_priced or ())
        self.exit_px = dict(exit_px or {})
        self.n_qualifying = n_qualifying
        self._asked = list(symbols)
        self.bench_calls = 0

    def screen(self, *a, **k):
        return self._shaped

    def _leg(self, entry):
        allowed = self.entry_priced if entry else self.exit_priced
        out = {}
        for s in self._asked:
            if s == "SPY":
                out[s] = ENTRY_PX if entry else EXIT_PX
            elif s in allowed:
                out[s] = ENTRY_PX if entry else self.exit_px.get(s, EXIT_PX)
        return out

    def get_prices(self, con, symbols, target_date, *a, **k):
        self._asked = symbols
        return self._leg(target_date == ENTRY)

    def get_prices_at(self, con, symbols, year, month, *a, **k):
        """sector-momentum/sector-rotation's own price fetcher.

        Its signature is (con, symbols, YEAR, MONTH, offset_days), NOT the
        (con, symbols, date) form get_prices takes. Delegating to get_prices
        here -- which the first version of this stub did -- compares an int year
        against a date, so EVERY fetch resolved to the exit leg and neither
        topic could be driven at all. That mis-signature is the whole reason the
        sector twins sat in STATIC_ONLY.
        """
        self._asked = symbols
        return self._leg((year, month) == (ENTRY.year, ENTRY.month))

    def screen_sectors(self, con, target_date, *a, **k):
        """(rows, n_qualifying) for the sector twins.

        Rows are the 6-tuples their SQL returns:
        (symbol, sector, recent_price, market_cap, avg_sector_return,
        n_qualifying). Sectors alternate over two names because both topics take
        the best/worst N_SECTORS=2 buckets and derive `sectors_selected` from
        the set. n_qualifying is returned above MIN_QUALIFYING_SECTORS so the
        SECTOR floor never fires and the STOCK floor -- the one under test --
        is what decides the period.
        """
        n_qual = self.n_qualifying
        return ([(s, f"Sector{i % 2}", ENTRY_PX, BIG_MCAP, 0.25, n_qual)
                 for i, s in enumerate(self.symbols)], n_qual)

    def get_benchmark_return(self, *a, **k):
        self.bench_calls += 1
        return BENCH_RET

    def close(self):        # graham-timing calls con.close()
        pass


def shape(symbols, arity):
    if arity == 1:
        return list(symbols)
    fill = [BIG_MCAP, 0.25, 0.30, 0.35, 0.40, 0.45]
    return [tuple([s] + fill[:arity - 1]) for s in symbols]


def attempt(topic, n_screen, n_priced, arity, verbose=False, drop_first=False,
            entry_priced=None, exit_priced=None, exit_px=None):
    """One run at a fixed screen arity. Returns (results, stubs, floor).

    drop_first prices the LAST n_priced names, so the unpriced ones sort to the
    front of the screened list. Most topics truncate holdings to the first 10
    names; dropping from the tail would hide the leak behind that truncation.

    n_priced prices a name at BOTH dates or NEITHER, which is all S1/S2/S3 need.
    entry_priced/exit_priced/exit_px take counts and a price map instead and let
    the two dates differ, which is what S4 and S5 turn on.
    """
    mod = load(topic)
    _, floor = floor_of(mod)
    symbols = [f"S{i:03d}" for i in range(n_screen)]

    def pick(n):
        return symbols[n_screen - n:] if drop_first else symbols[:n]

    if entry_priced is None and exit_priced is None:
        stubs = Stubs(symbols, priced=pick(n_priced))
    else:
        stubs = Stubs(symbols,
                      entry_priced=pick(n_screen if entry_priced is None
                                        else entry_priced),
                      exit_priced=pick(n_screen if exit_priced is None
                                       else exit_priced),
                      exit_px=exit_px)
    stubs._shaped = shape(symbols, arity)

    setattr(mod, SCREEN_FN.get(topic, "screen_stocks"), stubs.screen)
    for name in ("get_prices", "get_prices_at", "get_benchmark_return",
                 "screen_sectors"):
        if hasattr(mod, name):
            setattr(mod, name, getattr(stubs, name))
    # cyclical-timing gates the whole book on a macro signal; force it invested
    if hasattr(mod, "compute_expansion_signal"):
        mod.compute_expansion_signal = lambda *a, **k: (True, 1.0, 50)

    results = DRIVERS.get(topic, drive_default)(mod, stubs, symbols, verbose)
    return results, stubs, floor


# ---------------------------------------------------------------------------
# Drivers. Most topics are a (con, rebalance_dates, ...) period loop over the
# screen_stocks/get_prices pair and need nothing beyond the default. The three
# below are not, which is why they used to be STATIC_ONLY -- a static gate can
# see that a guard block is byte-identical, but only a driven run can prove the
# guard reads the entry leg, and only a driven run reaches the invested branch
# where the zero-survivor crash lives.
# ---------------------------------------------------------------------------

def drive_default(mod, stubs, symbols, verbose):
    return mod.run_backtest(**run_backtest_kwargs(mod, verbose=verbose))


def drive_sector(mod, stubs, symbols, verbose):
    """sector-momentum / sector-rotation.

    Their data boundary is screen_sectors + get_prices_at, both stubbed above.
    The rest is the ordinary (con, rebalance_dates) loop and returns a list of
    period dicts, so the scenario assertions apply unchanged.
    """
    return mod.run_backtest(None, [ENTRY, EXIT], use_costs=False, verbose=verbose)


def drive_graham_timing(mod, stubs, symbols, verbose):
    """graham-timing drives its OWN connection, so the seam is one level up.

    run_backtest(exchanges, ...) builds a CetaResearch client, generates its own
    rebalance dates and opens a DuckDB via fetch_data_via_api. Stub those four
    and the period loop below them is the same screen/price loop as everywhere
    else. It returns a metrics dict rather than a list, with the per-period
    records under 'period_data'.
    """
    mod.CetaResearch = lambda *a, **k: object()
    mod.fetch_data_via_api = lambda *a, **k: stubs
    mod.generate_rebalance_dates = lambda *a, **k: [ENTRY, EXIT]
    mod.get_mktcap_threshold = lambda *a, **k: 0.0
    res = mod.run_backtest(["NYSE"], apply_costs=False, verbose=verbose)
    return (res or {}).get("period_data", [])


DRIVERS = {
    "sector-momentum": drive_sector,
    "sector-rotation": drive_sector,
    "graham-timing": drive_graham_timing,
}


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


def run_case(topic, n_screen, n_priced=None, verbose=False, drop_first=False,
             entry_priced=None, exit_priced=None, exit_px=None):
    arity = probe_arity(topic, n_screen)
    return attempt(topic, n_screen, n_priced, arity, verbose=verbose,
                   drop_first=drop_first, entry_priced=entry_priced,
                   exit_priced=exit_priced, exit_px=exit_px)


# How a topic spells its period record. These are real, pre-existing dialect
# differences, NOT defects, and they are declared rather than sniffed so that a
# topic silently changing its units or dropping a field shows up as a FAIL
# instead of being absorbed. Every value here was read off the topic's own
# `results.append(...)`.
#
#   ret_scale : divisor to get a FRACTION out of the stored return.
#               graham-timing stores round(period_return * 100, 2), a PERCENT.
#   cash_fmt  : the exact holdings/msg string the cash branch writes.
#               The sector twins have no screened-count in scope at that point
#               and write the shorter form.
#   has_bench : whether the per-period record carries a benchmark at all.
#               graham-timing accumulates spy_returns in a PARALLEL list and
#               never puts it in period_data, so asserting on it there would be
#               asserting on a field the topic has never had.
DEFAULT_DIALECT = {"ret_scale": 1.0, "has_bench": True,
                   "cash_fmt": "CASH ({b} buyable at entry of {n} screened)"}
DIALECT = {
    "graham-timing":   {"ret_scale": 100.0, "has_bench": False},
    "sector-momentum": {"cash_fmt": "CASH (only {b} buyable at entry)"},
    "sector-rotation": {"cash_fmt": "CASH (only {b} buyable at entry)"},
}


def dialect_of(topic):
    d = dict(DEFAULT_DIALECT)
    d.update(DIALECT.get(topic, {}))
    return d


def check(topic, verbose=False):
    """Returns (s1_ok, s2_ok, detail dict)."""
    mod = load(topic)
    _, floor = floor_of(mod)
    n_screen = floor + 5
    n_priced_s1 = 3
    dia = dialect_of(topic)
    scale = dia["ret_scale"]
    det = {"floor": floor, "n_screen": n_screen}

    def ret_ok(rec, want=EXPECTED_RET):
        """Compare the stored return against `want` as a FRACTION."""
        if rec is None:
            return False
        v = rec.get("portfolio_return", rec.get("return"))
        return v is not None and abs(v / scale - want) < 1e-9

    # ---- S1: only 3 of the screened names price -> must be CASH
    s1_ok = False
    try:
        res, stubs, _ = run_case(topic, n_screen, n_priced_s1, verbose)
        rec = res[-1] if res else None
        det["s1_record"] = rec
        det["s1_bench_calls"] = stubs.bench_calls
        # The counts in this string are ENTRY-knowable ones. S1's unpriced names
        # are unpriced at BOTH dates, so they were never buyable and cash is
        # still the right answer; only the wording moved.
        want_holdings = dia["cash_fmt"].format(b=n_priced_s1, n=n_screen)
        if topic == "capex-efficiency":
            s1_ok = (rec is not None
                     and rec.get("n_stocks") == 0
                     and rec.get("stocks_held") == 0
                     and rec.get("return") == 0.0
                     and rec.get("msg", "").startswith(
                         f"cash ({n_priced_s1} buyable at entry of "
                         f"{n_screen} screened)"))
        else:
            # At most one benchmark fetch: a guard that refetches where the topic
            # already had the value in scope issues a second query. Zero is legal
            # too, for the topics that price SPY through get_prices instead of
            # get_benchmark_return (dcf-threshold, quality-momentum).
            s1_ok = (rec is not None
                     and rec.get("stocks_held") == 0
                     and rec.get("portfolio_return") == 0.0
                     and (rec.get("spy_return") is not None
                          or not dia["has_bench"])
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
                     and ret_ok(rec))
        else:
            held = rec.get("holdings", "") if rec else ""
            named = [x for x in held.replace("...", "").split(",") if x.strip()]
            # holdings must name the PRICED book, never the pre-filter screen list
            holdings_ok = bool(named) and all(n.strip().split("(")[0] in
                                              {f"S{i:03d}" for i in range(n_screen)}
                                              for n in named)
            s2_ok = (rec is not None
                     and rec.get("stocks_held") == n_screen
                     and ret_ok(rec)
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

    # ---- S4: EVERY screened name is priced at ENTRY; only floor-1 of them are
    # priced at EXIT. The strategy could buy the whole book on the rebalance
    # date, so it ENTERED. Nothing that happens at the exit date may
    # retroactively unwind that.
    #
    # This is the scenario the cold-eyes review reproduced. Under
    # `len(clean) < MIN_STOCKS` the book reads floor-1 and the period is
    # rewritten as a flat 0.0% cash period, erasing a realised result. Under
    # `buyable < MIN_STOCKS` it reads floor+5, the period stays invested, and
    # the exit-side survivors are averaged -- which is exactly what the topic
    # did before the guard existed.
    #
    # MUTATION IT MUST FAIL UNDER: revert the guard to
    #     if len(clean) < MIN_STOCKS:
    # Then stocks_held == 0, portfolio_return == 0.0, and S4 fails.
    s4_ok = False
    n_exit_s4 = floor - 1
    det["s4_n_exit_priced"] = n_exit_s4
    try:
        res, stubs, _ = run_case(topic, n_screen, entry_priced=n_screen,
                                 exit_priced=n_exit_s4, verbose=verbose,
                                 drop_first=True)
        rec = res[-1] if res else None
        det["s4_record"] = rec
        if topic == "capex-efficiency":
            s4_ok = (rec is not None
                     and rec.get("n_stocks") == n_exit_s4
                     and ret_ok(rec)
                     and "cash" not in str(rec.get("msg", "")))
        else:
            s4_ok = (rec is not None
                     and rec.get("stocks_held") == n_exit_s4
                     and ret_ok(rec)
                     and "CASH" not in str(rec.get("holdings", "")))
    except Exception as e:
        det["s4_error"] = f"{e.__class__.__name__}: {e}"
        if verbose:
            det["s4_traceback"] = traceback.format_exc()

    # ---- S5: every screened name prices at BOTH dates, but the first
    # (n_screen - floor + 1) of them return +1000%, above every topic's
    # max_single_return. filter_returns drops those, leaving floor-1 in `clean`.
    # Every name was buyable at entry, so the period is INVESTED.
    #
    # S5 is not redundant with S4. A half-fix that special-cases only the
    # missing-exit-price drop -- e.g. by counting names that have an entry price
    # and calling that "priced" -- passes S4 and still flips this period to cash
    # off a REALISED return. S5 is the only scenario that separates the two.
    #
    # 11x, not 4x: most topics cap at 2.0, but small-cap, small-value,
    # trending-value and graham-net-net cap at 3.0, and the test is `>` not
    # `>=`, so a 3.0 return would NOT be dropped there and S5 would silently
    # degenerate into S2.
    s5_ok = None if topic in NO_RETURN_CAP else False
    n_blown = n_screen - (floor - 1)
    det["s5_n_blown"] = n_blown
    if topic not in NO_RETURN_CAP:
        try:
            blown = {f"S{i:03d}": ENTRY_PX * 11 for i in range(n_blown)}
            res, stubs, _ = run_case(topic, n_screen, entry_priced=n_screen,
                                     exit_priced=n_screen, exit_px=blown,
                                     verbose=verbose)
            rec = res[-1] if res else None
            det["s5_record"] = rec
            if topic == "capex-efficiency":
                s5_ok = (rec is not None
                         and rec.get("n_stocks") == floor - 1
                         and ret_ok(rec)
                         and "cash" not in str(rec.get("msg", "")))
            else:
                s5_ok = (rec is not None
                         and rec.get("stocks_held") == floor - 1
                         and ret_ok(rec)
                         and "CASH" not in str(rec.get("holdings", "")))
        except Exception as e:
            det["s5_error"] = f"{e.__class__.__name__}: {e}"
            if verbose:
                det["s5_traceback"] = traceback.format_exc()

    # ---- S6: the book is buyable at entry and ZERO names survive to the exit
    # side. This is the crash regression, not a look-ahead test.
    #
    # Correcting the guard MADE this reachable. Under the old
    # `len(clean) < MIN_STOCKS` an empty book cashed out and never reached the
    # invested branch; under `buyable < MIN_STOCKS` the period is correctly
    # invested and execution falls into `sum(returns) / len(returns)`. 62 topics
    # carry an `if returns else 0.0` fallback there and 4 did not, so
    # capex-efficiency, graham-timing, sector-momentum and sector-rotation
    # raised ZeroDivisionError on an ordinary input: a final rebalance whose
    # exit date runs past data coverage, an exchange-wide halt, a mass
    # delisting. The remedy is the fallback. Reverting the guard is NOT the
    # remedy -- that restores the look-ahead S4 and S5 exist to forbid.
    #
    # S6 cannot discriminate a reverted guard: a zero-survivor period is
    # legitimately 0.0% either way. S4 and S5 stay the look-ahead sentinels.
    # All S6 asserts is that the period is REACHED, RECORDED and does not raise.
    #
    # Two routes, because they empty the book through different code:
    #   S6a  no name prices at EXIT       -- the delisting/coverage route
    #   S6b  every name blows the cap     -- the realised-return route. Topics
    #        with no cap cannot express it, so it is n/a there, never PASS.
    #        This is the route that reached capex-efficiency: its "no prices for
    #        either leg" pre-guard absorbs S6a, so S6a alone would have missed
    #        two of the five sites.
    s6_ok = False
    try:
        res, stubs, _ = run_case(topic, n_screen, entry_priced=n_screen,
                                 exit_priced=0, verbose=verbose)
        rec = res[-1] if res else None
        det["s6a_record"] = rec
        s6a = (rec is not None
               and rec.get("stocks_held", rec.get("n_stocks")) == 0
               and (rec.get("portfolio_return", rec.get("return"))) == 0.0)
    except Exception as e:
        s6a = False
        det["s6a_error"] = f"{e.__class__.__name__}: {e}"
        if verbose:
            det["s6a_traceback"] = traceback.format_exc()

    if topic in NO_RETURN_CAP:
        s6b = None
    else:
        try:
            all_blown = {f"S{i:03d}": ENTRY_PX * 11 for i in range(n_screen)}
            res, stubs, _ = run_case(topic, n_screen, entry_priced=n_screen,
                                     exit_priced=n_screen, exit_px=all_blown,
                                     verbose=verbose)
            rec = res[-1] if res else None
            det["s6b_record"] = rec
            s6b = (rec is not None
                   and rec.get("stocks_held", rec.get("n_stocks")) == 0
                   and (rec.get("portfolio_return", rec.get("return"))) == 0.0)
        except Exception as e:
            s6b = False
            det["s6b_error"] = f"{e.__class__.__name__}: {e}"
            if verbose:
                det["s6b_traceback"] = traceback.format_exc()

    s6_ok = s6a and (s6b is not False)
    det["s6a"], det["s6b"] = s6a, s6b

    return s1_ok, s2_ok, s3_ok, s4_ok, s5_ok, s6_ok, det


def tie_test():
    """Pin entry_usable() against filter_returns' own entry-side lines.

    The helper DUPLICATES filter_returns' rules rather than being called by it,
    because reordering that loop would change which names land in `skipped` (a
    name with ep=0.50, xp=None is silent today and would start being reported).
    Silently changing observable behaviour is the class of move this whole change
    exists to prevent, so the duplication stays and this test pins it.

    With every exit price present and the cap switched off, the ONLY thing
    filter_returns can drop is an entry-side failure, so the two counts must
    agree exactly for every combination of entry price and floor.
    """
    sys.path.insert(0, str(ROOT))
    from data_utils import filter_returns, entry_buyable, entry_buyable_prices
    bad = []
    for m in (0.0, 0.5, 1.0):
        for eps in itertools.product([None, -1.0, 0.0, 0.5, 1.0, 5.0], repeat=3):
            data = [(f"S{i}", ep, 100.0, BIG_MCAP) for i, ep in enumerate(eps)]
            clean, _ = filter_returns(data, min_entry_price=m,
                                      max_single_return=1e9)
            n = len(clean)
            if entry_buyable(data, m) != n:
                bad.append(("entry_buyable", m, eps, entry_buyable(data, m), n))
            prices = {f"S{i}": ep for i, ep in enumerate(eps) if ep is not None}
            got = entry_buyable_prices([f"S{i}" for i in range(len(eps))],
                                       prices, m)
            if got != n:
                bad.append(("entry_buyable_prices", m, eps, got, n))
    return bad


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
        s1, s2, s3, s4, s5, s6, det = check(t, verbose=args.verbose)
        rows[t] = (s1, s2, s3, s4, s5, s6, det)
        series[t] = {"s2_record": det.get("s2_record"),
                     "s1_record": det.get("s1_record"),
                     "s3_record": det.get("s3_record"),
                     "s4_record": det.get("s4_record"),
                     "s5_record": det.get("s5_record"),
                     "s6a_record": det.get("s6a_record"),
                     "s6b_record": det.get("s6b_record")}

    s1_pass = sorted(t for t, r in rows.items() if r[0])
    s1_fail = sorted(t for t, r in rows.items() if not r[0])
    s2_pass = sorted(t for t, r in rows.items() if r[1])
    s2_fail = sorted(t for t, r in rows.items() if not r[1])
    s3_pass = sorted(t for t, r in rows.items() if r[2])
    s3_fail = sorted(t for t, r in rows.items() if not r[2])
    s4_pass = sorted(t for t, r in rows.items() if r[3])
    s4_fail = sorted(t for t, r in rows.items() if not r[3])
    # None means the scenario cannot exist for that topic. Counted separately so
    # it can never be read as a pass.
    s5_na = sorted(t for t, r in rows.items() if r[4] is None)
    s5_pass = sorted(t for t, r in rows.items() if r[4] is True)
    s5_fail = sorted(t for t, r in rows.items() if r[4] is False)
    s6_pass = sorted(t for t, r in rows.items() if r[5])
    s6_fail = sorted(t for t, r in rows.items() if not r[5])
    s6b_na = sorted(t for t, r in rows.items() if r[6].get("s6b") is None)

    print(f"topics exercised: {len(rows)}   (static-only, excluded: {sorted(STATIC_ONLY)})")
    print(f"S1 guard fires      PASS {len(s1_pass):3d}   FAIL {len(s1_fail):3d}")
    print(f"S2 guard inert      PASS {len(s2_pass):3d}   FAIL {len(s2_fail):3d}")
    print(f"S3 holdings honest  PASS {len(s3_pass):3d}   FAIL {len(s3_fail):3d}")
    print(f"S4 entry-priced,    PASS {len(s4_pass):3d}   FAIL {len(s4_fail):3d}"
          "   (exit-unpriced must NOT flip the period to cash)")
    print(f"S5 return cap       PASS {len(s5_pass):3d}   FAIL {len(s5_fail):3d}"
          f"   n/a {len(s5_na):3d} {sorted(NO_RETURN_CAP)} -- no cap to trip")
    print(f"S6 zero survivors   PASS {len(s6_pass):3d}   FAIL {len(s6_fail):3d}"
          f"   (S6b n/a {len(s6b_na)}: no cap) -- must record 0.0%, never raise")
    if s1_fail:
        print("\nS1 FAIL (no cash record when the book fell below the floor):")
        for t in s1_fail:
            d = rows[t][6]
            err = d.get("s1_error")
            rec = d.get("s1_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held') if rec else None} "
                                   f"holdings={str(rec.get('holdings') if rec else None)[:60]!r}"))
    if s2_fail:
        print("\nS2 FAIL (guard fired when it should not have, or return wrong):")
        for t in s2_fail:
            d = rows[t][6]
            err = d.get("s2_error")
            rec = d.get("s2_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held') if rec else None} "
                                   f"ret={rec.get('portfolio_return') if rec else None} "
                                   f"holdings={str(rec.get('holdings') if rec else None)[:50]!r}"))
    if s3_fail:
        print("\nS3 FAIL (holdings names stocks that were never priced/held):")
        for t in s3_fail:
            d = rows[t][6]
            err = d.get("s3_error")
            rec = d.get("s3_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held') if rec else None} "
                                   f"leaked={d.get('s3_leaks')} "
                                   f"holdings={str(rec.get('holdings') if rec else None)[:60]!r}"))
    if s4_fail:
        print("\nS4 FAIL (LOOK-AHEAD: an exit-date fact decided the cash rule -- "
              "every name was buyable at entry, so the period was INVESTED):")
        for t in s4_fail:
            d = rows[t][6]
            err = d.get("s4_error")
            rec = d.get("s4_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held', rec.get('n_stocks')) if rec else None} "
                                   f"want={d.get('s4_n_exit_priced')} "
                                   f"ret={rec.get('portfolio_return', rec.get('return')) if rec else None} "
                                   f"holdings={str(rec.get('holdings', rec.get('msg')) if rec else None)[:50]!r}"))
    if s5_fail:
        print("\nS5 FAIL (LOOK-AHEAD: a REALISED return above max_single_return "
              "decided the cash rule):")
        for t in s5_fail:
            d = rows[t][6]
            err = d.get("s5_error")
            rec = d.get("s5_record")
            print(f"  {t:30s} " + (f"ERROR {err}" if err else
                                   f"stocks_held={rec.get('stocks_held', rec.get('n_stocks')) if rec else None} "
                                   f"ret={rec.get('portfolio_return', rec.get('return')) if rec else None} "
                                   f"holdings={str(rec.get('holdings', rec.get('msg')) if rec else None)[:50]!r}"))

    if s6_fail:
        print("\nS6 FAIL (a period that was BUYABLE at entry and lost every exit "
              "price must record 0.0%, not raise and not vanish):")
        for t in s6_fail:
            d = rows[t][6]
            for leg in ("s6a", "s6b"):
                if d.get(leg) is False or d.get(f"{leg}_error"):
                    err = d.get(f"{leg}_error")
                    rec = d.get(f"{leg}_record")
                    print(f"  {t:26s} {leg} " + (f"ERROR {err}" if err else
                          f"stocks_held={rec.get('stocks_held', rec.get('n_stocks')) if rec else None} "
                          f"ret={rec.get('portfolio_return', rec.get('return')) if rec else None}"))

    # The helper the cash rule now reads must agree with filter_returns' own
    # entry-side lines. Run once, not per topic.
    tie_bad = tie_test()
    print(f"\nTIE entry_usable vs filter_returns: "
          f"{'AGREE on all 324 combinations' if not tie_bad else 'DIVERGED'}")
    for b in tie_bad[:10]:
        print("   ", b)

    if args.verbose and args.topic:
        print(json.dumps(rows[args.topic][6], indent=2, default=str))

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

    # S4, S5 and S6 gate the exit code. S4/S5 are the look-ahead tests and S6 is
    # the crash the correction made reachable. This repo has no CI, and a test
    # nobody's exit status depends on is a comment.
    return 0 if not (s1_fail or s2_fail or s4_fail or s5_fail or s6_fail
                     or tie_bad) else 1


if __name__ == "__main__":
    sys.exit(main())
