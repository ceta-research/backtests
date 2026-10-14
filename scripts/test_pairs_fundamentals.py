#!/usr/bin/env python3
"""Offline check of pairs-fundamentals on the per-year pairs universe (no network).

A stand-in client answers pairs_universe's own warehouse SQL from made-up rows, so the topic's
fetch, pair formation, entry signal, year book and results run end to end on a made-up market.
What each year must hold is worked out here from the same made-up bars, by this file's own
arithmetic, and compared with what the topic returns.

Formation: a pair joins two candidates of the trading year, same sector, 200 common return days
(199 out, whichever leg starts late), correlation 0.7000 (0.6999 out), never NaN; ranked by
correlation then symbols, 100 rows, the first 20 held; a day printed twice does not make the
returns depend on row order. Correlation, hedge ratio, spread mean and std read the formation
year's bars alone, also for a pair with bars from the year before.

Entry: the z-score is the spread on the pair's last common bar of the formation year (the higher
print of a day printed twice), not the spread of the bar it trades on, and not of a later bar
that only one leg printed, whichever leg ends the year first; |z| 1.51 enters and 1.49 does not, on
either side; a spread flat to 1e-11 gives no signal; each leg needs a bar in the 10 days from
the entry date priced 1.00 or more. Fewer than 3 entered pairs is a cash year: nothing is priced
and the exit helper is asked nothing; 3 entered pairs are invested even when only 2 can be
priced at year end.

The book: a leg's gain above 200% is capped and counted, leg by leg (201% capped, exactly 200%
and 199% not), the pair stays; a round trip costs each leg's tiered rate once, on that year's
USD cap (none: the smallest-cap tier); the year's return is the mean over the priced pairs.
Exits: a leg with no bar in the 15 days from the exit date makes the pair a missing exit: dropped
(drop), or closed at the pair's last common bar inside the delisting bound, both legs at that bar
(ltp), where either leg's gain is capped and counted like any other, in the LTP book alone: each
run records the capped legs of the book it publishes. drop, ltp and both agree on everything
decided at entry, the both run publishes the drop book next to the LTP book, a drop run sends
every query but the delisting one and writes no exit key, and scripts/gate_ltp.py's stage A
check accepts the three results files.

Results: each year's universe counts and pairs, the universe rule, the listing rule, both dedups,
the capped legs and the exit block, with the keys the charts and posts read at the values the
year records give. The benchmark's year runs from its first bar in the 10 days from the entry
date to its first in the 15 from the exit date; a year without one is run and counted, not
measured, and an index with no bars gives way to the S&P 500. The command line passes
--exit-method (default data_utils.DEFAULT_EXIT_METHOD) and the market's risk-free rate to every
market; a market that fails is recorded and the others still run.

Every mutant of the topic's source must trip a check.

Usage:
    python scripts/test_pairs_fundamentals.py [repo root]
"""
import contextlib
import io
import json
import math
import os
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "pairs-fundamentals", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import cr_client  # noqa: E402
import gate_ltp  # noqa: E402
import pairs_universe  # noqa: E402
from cli_utils import get_risk_free_rate  # noqa: E402
from data_utils import DEFAULT_EXIT_METHOD, EXIT_METHODS, get_local_benchmark  # noqa: E402
from metrics import compute_metrics  # noqa: E402

pairs_universe.print = lambda *a, **k: None   # its progress lines

ORIGIN, END = date(2018, 1, 1), date(2021, 1, 20)   # bars run from a line's first year past 2020
T19, T20 = date(2019, 1, 1), date(2020, 1, 1)
Y18_END, Y19_END, Y20_END = date(2018, 12, 31), date(2019, 12, 31), date(2020, 12, 31)
FY16, FY17, FY18 = date(2016, 12, 31), date(2017, 12, 31), date(2018, 12, 31)
YEARS = (2019, 2020)
ONE = timedelta(days=1)
JUN15 = date(2020, 6, 15)
TABLES = ("profile", "key_metrics", "stock_eod", "forex_prices_daily", "delisted_companies")


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def span(first, last):
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def n(d):
    return (d - ORIGIN).days


def wave(d):
    """A price that moves a few percent every day and never trends."""
    return 100.0 + 10.0 * ((n(d) * 7) % 13) / 13.0


def mean(xs):
    return sum(xs) / len(xs)


def cov(xs, ys):
    mx, my = mean(xs), mean(ys)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys))


def pearson(xs, ys):
    den = math.sqrt(cov(xs, xs) * cov(ys, ys))
    return cov(xs, ys) / den if den else float("nan")


def stats(a, b, first, last, prints=None):
    """(beta, mean, std, spread on the last common bar): the topic's estimate, redone here.
    prints: [(log A, log B)] added to the last day's own, when that day is printed more than once."""
    days = [d for d in span(first, last) if d in a and d in b]
    rows = [(math.log(a[d]), math.log(b[d])) for d in days] + list(prints or ())
    la, lb = [r[0] for r in rows], [r[1] for r in rows]
    beta = cov(la, lb) / cov(lb, lb)
    s = [x - beta * y for x, y in rows]
    m = mean(s)
    return beta, m, math.sqrt(sum((x - m) ** 2 for x in s) / (len(s) - 1)), s[len(days) - 1]


def z_at(a, b, fy, pa, pb):
    """z-score of the prices (pa, pb) against the pair's formation year fy."""
    beta, m, sd, _ = stats(a, b, date(fy, 1, 1), date(fy, 12, 31))
    return (math.log(pa) - beta * math.log(pb) - m) / sd


def tune(z_of, target):
    """The level at which z_of(level), which rises with it, first reaches target."""
    lo, hi = -0.2, 0.2
    for _ in range(70):
        mid = (lo + hi) / 2.0
        lo, hi = (mid, hi) if z_of(mid) < target else (lo, mid)
    return hi


# --- the made-up warehouse --------------------------------------------------------------------------

PX, PROFILE, METRICS, EOD, RATES, Z_END = {}, [], [], [], {}, {}
FX = [("USDEUR", ep(date(2018, 12, 28)), 0.8), ("USDEUR", ep(date(2019, 12, 30)), 0.9)]
DELISTED = [("P16B", "2020-06-10")]
BOUNDS = {"P16B": date(2020, 6, 10)}


def bars(symbol, px, volume=10.0):
    PX[symbol] = px
    EOD.extend((symbol, d.isoformat(), ep(d), p, p, volume) for d, p in sorted(px.items()))


def line(symbol, exchange, sector, px, caps=None, rates=None, country="US", currency="USD", isin=None):
    """One listed line: its bars, a profile row, its FY filings [(period end, market cap)] and the
    one-way cost rate those caps mean in each trading year."""
    bars(symbol, px)
    PROFILE.append((symbol, exchange, sector, country, currency, isin or "ZZ" + symbol, None,
                    symbol + " Corp", True, False, False, 100))
    METRICS.extend((symbol, "FY", cap, currency, ep(d)) for d, cap in ([(FY18, 2e10)] if caps is None else caps))
    RATES[symbol] = rates or {2019: 0.005, 2020: 0.001}   # a cap filed for 2018 is too new for 2019


def formation(fy, amp, z, a_scale=1.0, b_scale=1.0, last=None):
    """(A, B) bars of year fy: B is the wave, A the wave times exp(+-amp), and on the year's last
    day A stands where the pair's z-score is exactly `z`. last: the bars stop on that day instead."""
    days = span(date(fy, 1, 1), last or date(fy, 12, 31))
    b = {d: b_scale * wave(d) for d in days}

    def a_with(level):
        return {d: a_scale * wave(d) * math.exp(level if d == days[-1] else (amp if n(d) % 2 else -amp)) for d in days}

    level = tune(lambda e: z_at(a_with(e), b, fy, a_with(e)[days[-1]], b[days[-1]]), z)
    return a_with(level), b


def pair(name, exchange, sector, fy, amp, z=3.0, g=0.03, a_scale=1.0, b_scale=1.0, a_abs=None, a_z=None,
         b_abs=None, a_jump=None, b_jump=None, a_gap=None, b_gap=None, a_last=END, b_last=END,
         a_caps=None, b_caps=None, a_rates=None, b_rates=None, pre=None, **ccy):
    """Two lines that form a pair on year fy and trade the next year. From 1 January A is flat (at
    a_abs, at the level whose entry-bar z-score is a_z, or where it ended) and B gains g a year
    from the entry bar of 2 January (from b_abs, or from where it ended). a_jump, b_jump:
    (day, factor) from that day on. a_gap, b_gap: (first, last) days without a bar. pre: (A, B)
    multiples of the wave from 1 July of the year before fy: 184 bars, too few to be listed then."""
    a, b = formation(fy, amp, z, a_scale, b_scale)
    end, entry = date(fy, 12, 31), date(fy + 1, 1, 2)
    Z_END[name] = z_at(a, b, fy, a[end], b[end])
    for d in span(date(fy - 1, 7, 1), date(fy - 1, 12, 31)) if pre else ():
        a[d], b[d] = pre[0] * wave(d), pre[1] * wave(d)
    b0 = b[end] if b_abs is None else b_abs
    if a_z is not None:
        level = lambda e: a_scale / b_scale * b0 * math.exp(e)
        a_abs = level(tune(lambda e: z_at(a, b, fy, level(e), b0), a_z))
    a0 = a[end] if a_abs is None else a_abs
    for d in span(end + ONE, END):
        a[d] = a0 * (a_jump[1] if a_jump and d >= a_jump[0] else 1.0)
        b[d] = b0 * (1.0 + g * (d - entry).days / 365.0) * (b_jump[1] if b_jump and d >= b_jump[0] else 1.0)
    gone = lambda d, gap: gap is not None and gap[0] <= d <= gap[1]
    line(name + "A", exchange, sector, {d: p for d, p in a.items() if d <= a_last and not gone(d, a_gap)},
         caps=a_caps, rates=a_rates, **ccy)
    line(name + "B", exchange, sector, {d: p for d, p in b.items() if d <= b_last and not gone(d, b_gap)},
         caps=b_caps, rates=b_rates, **ccy)


def correlated(rho, days):
    """(A, B) bars over `days` whose daily returns have a sample correlation of exactly rho."""
    r = [wave(d) / wave(d - ONE) - 1.0 for d in days[1:]]
    e = [math.sin(1.7 * i) for i in range(len(r))]
    rc, ec = [x - mean(r) for x in r], [x - mean(e) for x in e]
    ec = [y - cov(e, r) / cov(r, r) * x for x, y in zip(rc, ec)]            # orthogonal to the wave's returns
    scale = math.sqrt(cov(r, r) / sum(y * y for y in ec))
    a, level = {days[0]: 100.0}, 100.0
    for d, x, y in zip(days[1:], rc, ec):
        level *= 1.0 + mean(r) + rho * x + math.sqrt(1.0 - rho * rho) * scale * y
        a[d] = level
    b = {d: wave(d) for d in days}
    for d in span(days[-1] + ONE, END):
        a[d], b[d] = level, wave(days[-1])
    return a, b


# The main leg: XETRA, German lines, EUR. Pair k of 2020 has noise amplitude 0.004 + 0.0006 k in 2019,
# so k is also its correlation rank: pairs 0-19 are the 20 held, pair 20 is the 21st.
MAIN, BENCH = ["XETRA"], "^GDAXI"
EUR = dict(country="DE", currency="EUR")
EUR_CAP, EUR_RATES = [(FY18, 9.5e9)], {2019: 0.005, 2020: 0.001}    # 10.6bn USD at 0.9 EUR a dollar
D20 = lambda month, day: date(2020, month, day)
SCENARIOS = {
    0: dict(pre=(2.0, 0.5)),                                         # bars of 2018 at other levels: not formation bars
    1: dict(z=-3.0),                                                 # A cheap: long A, short B
    2: dict(a_caps=[(FY18, 1.9e9)], a_rates={2020: 0.003},           # 2.1bn USD: the 0.3% tier
            b_caps=[(FY16, 9.5e9)], b_rates={2020: 0.005}),          # a filing too old to count: 0.5%
    3: dict(z=1.51), 4: dict(z=1.49), 5: dict(z=-1.51), 6: dict(z=-1.49),
    7: dict(a_z=0.0),                                                # extended at year end, not on the entry bar
    8: dict(z=0.5, a_z=3.0),                                         # extended on the entry bar only
    9: dict(a_scale=0.01, a_abs=0.99),                               # A enters at 0.99
    10: dict(a_scale=0.01, a_abs=1.0),                               # A at exactly 1.00
    11: dict(b_scale=0.01, b_abs=0.99),                              # B at 0.99
    12: dict(b_scale=0.01, b_abs=1.0),                               # B at exactly 1.00 on 2 January
    13: dict(b_gap=(T20, D20(1, 11))),                               # B's first bar is 12 January
    14: dict(b_gap=(T20, D20(1, 12))),                               # 13 January
    15: dict(a_last=JUN15, a_jump=(D20(6, 1), 3.5)),                 # A, the short leg, stops on 15 June, 250% up
    16: dict(b_last=D20(7, 10)),                                     # B delisted 10 June, bars to 10 July
    17: dict(b_gap=(D20(12, 20), date(2021, 1, 15))),                # B's next bar is 16 January 2021
    18: dict(b_gap=(D20(12, 20), date(2021, 1, 16)),                 # 17 January 2021
             b_jump=(D20(12, 1), 3.2)),                              # B, the long leg, over 200% up by 19 December
}
for k in range(21):
    kw = {"a_caps": EUR_CAP, "b_caps": EUR_CAP, "a_rates": EUR_RATES, "b_rates": EUR_RATES, **SCENARIOS.get(k, {})}
    pair(f"P{k:02d}", "XETRA", f"S{k:02d}", 2019, 0.004 + 0.0006 * k, g=0.02 + 0.005 * k, **kw, **EUR)
line("P19A-PFA", "XETRA", "S19", dict(PX["P19A"]), caps=EUR_CAP, **EUR)        # a second line of P19A's company
line("USX", "XETRA", "S00", dict(PX["P00B"]), caps=EUR_CAP, country="US", currency="EUR")   # a foreign line
line("NANA", "XETRA", "SNAN", dict(PX["P20B"]), caps=EUR_CAP, **EUR)
line("NANB", "XETRA", "SNAN", {d: 50.0 for d in span(T19, END)}, caps=EUR_CAP, **EUR)   # a frozen price: CORR is NaN
# The 2019 pairs form on 2018. In 2019 their A legs are flat or jump once, so they form no pair for 2020.
Q_CAPS, Q_RATES = [(FY17, 8.5e9), (FY18, 1.0e9)], {2019: 0.001, 2020: 0.005}   # 10.6bn USD, then 1.1bn
JUL1 = date(2019, 7, 1)
Q_SCENARIOS = {
    1: dict(a_scale=0.1, a_abs=10.0, a_jump=(JUL1, 2.99)),           # the short leg gains 199%
    2: dict(a_scale=0.1, a_abs=10.0, a_jump=(JUL1, 3.0)),            # exactly 200%
    3: dict(g=0.0, b_jump=(JUL1, 3.5)),                              # the long leg gains 250%
    4: dict(a_scale=0.1, a_abs=10.0, a_jump=(JUL1, 3.01),            # the short leg gains 201%
            g=0.0, b_jump=(date(2019, 8, 1), 3.2)),                  # and the long leg 220%, on another day
    5: dict(a_last=date(2019, 1, 2)),                                # A stops on its entry bar
    6: dict(b_scale=0.1, b_abs=10.0, g=0.0, b_jump=(JUL1, 3.0)),     # the long leg gains exactly 200%
    7: dict(a_gap=(T19, date(2019, 1, 12))),                         # A's first bar of 2019 is 13 January
}
for k in range(8):
    kw = {"g": 0.02 + 0.005 * k, **Q_SCENARIOS.get(k, {})}
    pair(f"Q{k}", "XETRA", f"T{k}", 2018, 0.004 + 0.002 * k, a_caps=Q_CAPS, b_caps=Q_CAPS,
         a_rates=Q_RATES, b_rates=Q_RATES, **kw, **EUR)
bars(BENCH, {d: 10000.0 + 3.0 * n(d) for d in span(ORIGIN, END)})
bars("SPY", {d: 300.0 + 0.1 * n(d) for d in span(ORIGIN, END)})
# Two indices with gaps. KOSPI: first bar 13 January 2019, none on 1-11 January 2020 or from 21 December 2020
# to 15 January 2021. TAIEX: none from 21 December 2020 to 16 January 2021.
KOSPI, TAIEX = "^KS11", "^TWII"
bars(KOSPI, {d: 2000.0 + n(d) for d in span(date(2019, 1, 13), END)
             if not (T20 <= d <= D20(1, 11) or D20(12, 21) <= d <= date(2021, 1, 15))})
bars(TAIEX, {d: 9000.0 + 2.0 * n(d) for d in span(ORIGIN, END) if not D20(12, 21) <= d <= date(2021, 1, 16)})

# Two entered pairs, one without an exit price: a cash year. Three, one without: invested.
for exchange, count in (("ZCASH", 2), ("ZTHREE", 3)):
    for k in range(1, count + 1):
        pair(f"{exchange[1]}{k}", exchange, f"{exchange[1]}{k}", 2019, 0.004 + 0.002 * k, g=0.12 - 0.02 * k,
             a_last=JUN15 if k == count else END)

# Legs that end the formation year on different days: one prints no bar on 30 and 31 December 2019 while the
# other moves 6%. On their last common bar, 29 December, the pair is 1.0 std out: with two plain pairs, a cash year.
DEC29 = date(2019, 12, 29)
for k in (1, 2):
    pair(f"S{k}", "ZSIG", f"S{k}", 2019, 0.004 + 0.002 * k, g=0.12 - 0.02 * k)
for k, late, z in ((3, "A", 1.0), (4, "B", -1.0)):                         # the leg that prints on 30 and 31 December
    px = dict(zip("AB", formation(2019, 0.004 + 0.002 * k, z, last=DEC29)))
    px[late].update({d: px[late][DEC29] * (1.0 + 0.03 * i) for i, d in enumerate(span(DEC29 + ONE, Y19_END), 1)})
    for side in "AB":
        flat = px[side][max(px[side])]
        px[side].update({d: flat for d in span(T20, END)})
        line(f"S{k}{side}", "ZSIG", f"S{k}", px[side])

# Three pairs whose spread is flat but for 1e-11 on the year's last day: no signal, whatever its z-score.
for k in range(1, 4):
    pb = {d: (1.0 + 0.1 * k) * wave(d) for d in span(T19, Y19_END)}
    pa = {**pb, Y19_END: pb[Y19_END] * (1.0 + 1e-11)}
    for d in span(T20, END):
        pa[d], pb[d] = pa[Y19_END], pb[Y19_END] * (1.0 + 0.05 * (d - D20(1, 2)).days / 365.0)
    line(f"F{k}A", "ZFLAT", f"F{k}", pa)
    line(f"F{k}B", "ZFLAT", f"F{k}", pb)

# Formation boundaries: common days and the correlation floor.
for name, late, days in (("D200", "B", 200), ("D199", "B", 199), ("E199", "A", 199)):   # the late leg's return days
    pa, pb = formation(2019, 0.02, 3.0)
    for d in span(T20, END):
        pa[d], pb[d] = pa[Y19_END], pb[Y19_END]
    for side, px in (("A", pa), ("B", pb)):
        line(name + side, "ZFORM", "S" + name,
             {d: p for d, p in px.items() if side != late or d >= Y19_END - timedelta(days=days)})
for name, rho in (("C70", 0.70004), ("C69", 0.69994)):                     # round to 0.7000 and 0.6999
    pa, pb = correlated(rho, span(T19, Y19_END))
    line(name + "A", "ZFORM", "S" + name, pa)
    line(name + "B", "ZFORM", "S" + name, pb)
line("FRZA", "ZFORM", "SFRZ", {d: wave(d) for d in span(T19, END)})
line("FRZB", "ZFORM", "SFRZ", {d: 50.0 for d in span(T19, END)})

# A leg of 21 lines that move as one: 210 pairs at correlation 1.0000.
for i in range(1, 22):
    line(f"L{i:02d}", "ZLIM", "L", {d: (1.0 + 0.1 * i) * wave(d) for d in span(T19, END)})


def listed(n_bars, last=Y19_END, entry=None, close=10.0):
    """n_bars daily bars ending on `last`, then the January 2020 bars {day: price}."""
    px = {last - timedelta(days=i): close for i in range(n_bars)}
    px.update({date(2020, 1, day): p for day, p in (entry or {}).items()})
    return px


# The fetch leg: what the topic passes to the universe. Legs of 19 and 20 candidates.
for i in range(1, 32):
    line(f"C{i:02d}", "ZFETCH", "Cut31", listed(205, close=100.0 - i))     # size falls with i
line("B200", "ZFETCH", "Bars", listed(200))
line("B199", "ZFETCH", "Bars", listed(199))
line("ISA", "ZFETCH", "Offs", listed(205, entry={1: 5.0}), isin="XI3")      # buyable at offset 0 only
line("ISB", "ZFETCH", "Offs", listed(205, entry={12: 5.0}), isin="XI3")     # buyable at offset 1 only
line("TWA", "ZFETCH", "Floor", listed(205, entry={2: 0.995, 3: 0.995}), isin="XF1")   # under the 1.00 floor
line("TWB", "ZFETCH", "Floor", listed(205, entry={2: 5.0, 3: 5.0}), isin="XF1")
line("TWC", "ZFETCH", "Floor", listed(205, entry={2: 1.0, 3: 1.0}), isin="XF2")       # exactly on it
line("TWD", "ZFETCH", "Floor", listed(205, entry={2: 5.0, 3: 5.0}), isin="XF2")
for i in range(1, 20):
    line(f"N{i:02d}", "ZG19", "G", listed(205))
for i in range(1, 21):
    line(f"M{i:02d}", "ZG20", "G", listed(205))
FETCH_BOOK = {"Cut31": [f"C{i:02d}" for i in range(1, 31)], "Bars": ["B200"], "Offs": ["ISB"],
              "Floor": ["TWB", "TWC"]}


class Warehouse:
    """Stands in for CetaResearch: runs each query on the made-up warehouse tables."""

    def __init__(self):
        c = self.con = duckdb.connect()
        self.sent, self.problems = [], []
        c.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, sector VARCHAR, country VARCHAR,"
                  " currency VARCHAR, isin VARCHAR, cik VARCHAR, companyName VARCHAR, isActivelyTrading BOOLEAN,"
                  " isEtf BOOLEAN, isFund BOOLEAN, fetchedAtEpoch BIGINT)")
        c.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, marketCap DOUBLE,"
                  " reportedCurrency VARCHAR, dateEpoch UINTEGER)")
        c.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch UINTEGER, close DOUBLE,"
                  " adjClose DOUBLE, volume DOUBLE)")
        c.execute("CREATE TABLE forex_prices_daily(symbol VARCHAR, dateEpoch UINTEGER, close DOUBLE)")
        c.execute("CREATE TABLE delisted_companies(symbol VARCHAR, delistedDate VARCHAR)")
        for table, rows in (("profile", PROFILE), ("key_metrics", METRICS), ("stock_eod", EOD),
                            ("forex_prices_daily", FX), ("delisted_companies", DELISTED)):
            marks = "(" + ", ".join("?" * len(rows[0])) + ")"
            for i in range(0, len(rows), 2000):
                part = rows[i:i + 2000]
                c.execute(f"INSERT INTO {table} VALUES " + ", ".join([marks] * len(part)), [v for r in part for v in r])

    def query(self, sql, format="json", **kw):
        self.sent.append(sql)
        literals = [s for i, s in enumerate(sql.split("'")) if i % 2]
        if ";" in sql or len(sql) > 50000 or any("[" in s or any(t in s for t in TABLES) for s in literals):
            self.problems.append(f"warehouse SQL breaks a platform rule: {sql[:120]!r}")
        if format != "parquet":
            cur = self.con.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "result.parquet")
            self.con.execute(f"COPY ({sql}) TO '{out}' (FORMAT PARQUET)")
            if not self.con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]:
                return b""   # the empty result query_parquet turns into its placeholder table
            with open(out, "rb") as fh:
                return fh.read()


WH = Warehouse()
_CONS = {}


def cached(key, build):
    """Fixture connections are only read: one build serves the base run and every mutant."""
    if key not in _CONS:
        _CONS[key] = build()
    return _CONS[key]


def leg(*exchanges):
    """(con, per-year counts) of a leg, built by the module with the topic's published parameters."""
    def build():
        con = duckdb.connect()
        pairs_universe.fetch_universe(WH, con, list(exchanges), YEARS)
        counts = pairs_universe.build_candidates(con, YEARS, 1, 1.0, 30, 200)
        pairs_universe.fetch_prices(WH, con, None, [BENCH if exchanges == ("XETRA",) else "SPY"])
        return con, counts
    return cached(exchanges, build)


class Offline:
    """The client the topic under test imports: a source whose main() runs must not reach the warehouse."""

    def __init__(self, *a, **k):
        raise RuntimeError("the topic opened a warehouse client inside the offline check")


def load(src):
    mod = types.ModuleType("pairs_fundamentals_under_test")
    mod.__file__ = PATH
    real, cr_client.CetaResearch = cr_client.CetaResearch, Offline
    try:
        exec(compile(src, PATH, "exec"), mod.__dict__)
    finally:
        cr_client.CetaResearch = real
    mod.print = lambda *a, **k: None
    mod.START_YEAR, mod.END_YEAR = YEARS
    return mod


def near(x, y, tol=1.5e-6):
    return x is not None and y is not None and abs(x - y) <= tol


def book(con, year):
    out = {}
    for sector, symbol in con.execute("SELECT sector, symbol FROM candidates WHERE yr = ? ORDER BY 1, 2", [year]).fetchall():
        out.setdefault(sector, []).append(symbol)
    return out


# --- what the made-up market must produce -----------------------------------------------------------

def corr_row(a, b, sector, first, last):
    """A pair's row of the correlation list, from the fixture's own bars."""
    rets = []
    for px in (PX[a], PX[b]):
        days = sorted(d for d in px if first <= d <= last)
        rets.append({d: px[d] / px[p] - 1.0 for p, d in zip(days, days[1:])})
    common = sorted(set(rets[0]) & set(rets[1]))
    return (a, b, sector, pearson([rets[0][d] for d in common], [rets[1][d] for d in common]), len(common))


def ranked(con, year):
    """The year's correlation list: same-sector candidate pairs, 200 common days, 0.70, never NaN."""
    first, last = date(year - 1, 1, 1), date(year - 1, 12, 31)
    rows = [corr_row(a, b, sector, first, last) for sector, names in sorted(book(con, year).items())
            for i, a in enumerate(names) for b in names[i + 1:]]
    rows = [r for r in rows if r[4] >= 200 and not math.isnan(r[3]) and round(r[3], 4) >= 0.70]
    return sorted(rows, key=lambda r: (-round(r[3], 4), r[0], r[1]))[:100]


def rows_match(got, wanted):
    return (len(got) == len(wanted)
            and all(tuple(g[:3]) == w[:3] and near(g[3], round(w[3], 4), 1e-9) and g[4] == w[4]
                    for g, w in zip(got, wanted)))


def first_bar(symbol, first, days):
    """(day, price) of a line's first bar in the `days` days from `first`, or None."""
    return next(((d, PX[symbol][d]) for d in span(first, first + timedelta(days=days)) if d in PX[symbol]), None)


def pair_return(a, b, year, direction, entry_a, entry_b, exit_a, exit_b, costs=True):
    """(return, legs capped) of one pair: half the spread return, each leg's gain capped at 200%."""
    ra, rb = (exit_a - entry_a) / entry_a, (exit_b - entry_b) / entry_b
    capped = (ra > 2.0) + (rb > 2.0)
    r = direction * (min(ra, 2.0) - min(rb, 2.0)) / 2.0
    return r - (RATES[a][year] + RATES[b][year] if costs else 0.0), capped


def model(selected, year, offset=1, costs=True, bounds=None):
    """One trading year from the fixture's own bars: who entered, the drop book, the LTP book, the fills."""
    bounds = BOUNDS if bounds is None else bounds
    fs, fe, t, te = date(year - 1, 1, 1), date(year - 1, 12, 31), date(year, 1, 1), date(year, 12, 31)
    entered = []
    for a, b, *_ in selected:
        _, m, sd, s_end = stats(PX[a], PX[b], fs, fe)
        ea, eb = first_bar(a, t + offset * ONE, 10), first_bar(b, t + offset * ONE, 10)
        if ea is None or eb is None or ea[1] < 1.0 or eb[1] < 1.0 or sd < 1e-10 or abs((s_end - m) / sd) < 1.5:
            continue
        entered.append((a, b, -1 if s_end > m else 1, ea, eb))
    out = {"entered": [(e[0], e[1]) for e in entered], "invested": len(entered) >= 3,
           "drop": [], "ltp": [], "capped": {"drop": 0, "ltp": 0}, "fills": [], "missing": 0}
    for a, b, direction, ea, eb in entered if out["invested"] else []:
        xa, xb = first_bar(a, te + offset * ONE, 15), first_bar(b, te + offset * ONE, 15)
        if xa and xb:
            r, capped = pair_return(a, b, year, direction, ea[1], eb[1], xa[1], xb[1], costs)
            for name in ("drop", "ltp"):
                out[name].append(r)
                out["capped"][name] += capped
            continue
        out["missing"] += 1
        hi, kinds = te, []
        for s, e in ((a, ea), (b, eb)):   # a delisting bounds the pair 7 days on, unless 30 days before the leg's entry
            dd = bounds.get(s)
            if dd is None:
                kinds.append("none")
            elif dd + timedelta(days=30) >= e[0]:
                kinds.append("applied" if dd + timedelta(days=7) < te else "not_binding")
                hi = min(hi, dd + timedelta(days=7))
            else:
                kinds.append("ignored_predates_entry")
        day = next((d for d in reversed(span(max(ea[0], eb[0]) + ONE, hi)) if d in PX[a] and d in PX[b]), None)
        if day is None:
            continue
        r, capped = pair_return(a, b, year, direction, ea[1], eb[1], PX[a][day], PX[b][day], costs)
        out["ltp"].append(r)
        out["capped"]["ltp"] += capped
        out["fills"].append({"pair": [a, b], "year": year, "entry_bars": [ea[0].isoformat(), eb[0].isoformat()],
                             "entry_prices": [ea[1], eb[1]], "last_common_bar": day.isoformat(),
                             "prices": [PX[a][day], PX[b][day]],
                             "missing_leg": [s for s, x in ((a, xa), (b, xb)) if x is None],
                             "delisted_date": [bounds[s].isoformat() if s in bounds else None for s in (a, b)],
                             "delisting_bound": kinds, "exit_date": te.isoformat()})
    return out


def bench_return(symbol, year, offset=1):
    """The benchmark's year: first bar in the 10 days from the entry date to first bar in the 15 from the exit date."""
    first = first_bar(symbol, date(year, 1, 1) + offset * ONE, 10)
    last = first_bar(symbol, date(year, 12, 31) + offset * ONE, 15)
    return last[1] / first[1] - 1.0 if first and last else None


def wanted_record(con, year, mode, offset=1, costs=True, bench="SPY", bounds=None):
    """(the year's record as the topic must return it, the model behind it)."""
    selected = ranked(con, year)[:20]
    m = model(selected, year, offset, costs, bounds)
    name = "ltp" if mode == "ltp" else "drop"
    rec = {"year": year, "portfolio_return": mean(m[name]) if m[name] else 0.0,
           "spy_return": bench_return(bench, year, offset), "pairs_formed": len(selected),
           "pairs_entered": len(m["entered"]), "pairs_active": len(m[name]), "legs_capped": m["capped"][name],
           "same_company_pairs": 0,
           "pairs": [{"a": r[0], "b": r[1], "sector": r[2], "corr": round(r[3], 4)} for r in selected]}
    if mode == "both" and m["invested"]:
        rec.update({"portfolio_return_ltp": mean(m["ltp"]) if m["ltp"] else 0.0,
                    "pairs_active_ltp": len(m["ltp"]), "ltp_filled": len(m["fills"])})
    return rec, m


def same_record(got, wanted):
    """Keys and values match: counts exactly, returns to 1.5e-6, correlations to 1e-9."""
    if sorted(got) != sorted(wanted):
        return False
    for key, value in wanted.items():
        if key == "pairs":
            if len(got[key]) != len(value) or any(
                    sorted(g) != sorted(w) or (g["a"], g["b"], g["sector"]) != (w["a"], w["b"], w["sector"])
                    or not near(g["corr"], w["corr"], 1e-9) for g, w in zip(got[key], value)):
                return False
        elif isinstance(value, float):
            if not near(got[key], value):
                return False
        elif got[key] != value:
            return False
    return True


def same_fills(got, wanted):
    """PairLtp's fill records match the model's: names, bars and bounds exactly, prices to 1e-9."""
    if len(got) != len(wanted):
        return False
    for g, w in zip(got, wanted):
        if sorted(g) != sorted(w) or any(g[k] != w[k] for k in w if k not in ("entry_prices", "prices")):
            return False
        if any(not near(x, y, 1e-9) for k in ("entry_prices", "prices") for x, y in zip(g[k], w[k])):
            return False
    return True


P = lambda *ks: [(f"P{k:02d}A", f"P{k:02d}B") for k in ks]
ENTERED_2020 = P(0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 17, 18, 19)
FILLED_2020 = {("P15A", "P15B"): ("2020-06-15", ["P15A"]), ("P16A", "P16B"): ("2020-06-17", ["P16B"]),
               ("P18A", "P18B"): ("2020-12-19", ["P18B"])}
ENTERED_2019 = [(f"Q{k}A", f"Q{k}B") for k in range(7)]   # Q7's A leg has no entry bar


def fixture_problems():
    """The made-up market is what the checks below assume, by this file's own arithmetic."""
    out = []
    con, counts = leg("XETRA")
    for exchange in ("XETRA", "ZCASH", "ZTHREE", "ZSIG", "ZFLAT", "ZFORM", "ZLIM"):   # the price cleaning kept every made-up bar
        have = {}
        for symbol, day, price in leg(exchange)[0].execute("SELECT * EXCLUDE (trade_epoch) FROM prices_cache").fetchall():
            have.setdefault(symbol, {})[day] = price
        if any(px != PX[symbol] for symbol, px in have.items()) or len(have) < 5:
            out.append(f"{exchange}: prices_cache does not hold the bars as made up")
    rows20, rows19 = ranked(con, 2020), ranked(con, 2019)
    if [r[:2] for r in rows20] != P(*range(21)) or [r[:2] for r in rows19] != ENTERED_2019 + [("Q7A", "Q7B")]:
        out.append(f"the main leg's pairs are not P00-P20 and Q0-Q7 in correlation order: {rows20} {rows19}")
    form = ranked(leg("ZFORM")[0], 2020)
    if [(r[0], round(r[3], 4), r[4]) for r in form] != [("D200A", round(form[0][3], 4), 200), ("C70A", 0.7, 364)]:
        out.append(f"the formation leg's pairs: {form}")
    edge = {a: corr_row(a, a[:-1] + "B", "", T19, Y19_END) for a in ("D199A", "E199A", "C69A", "FRZA")}
    if ((edge["D199A"][4], edge["E199A"][4]) != (199, 199) or round(edge["C69A"][3], 4) != 0.6999
            or not math.isnan(edge["FRZA"][3])):
        out.append(f"the formation leg's rejected pairs: {edge}")
    corrs = [r[3] for r in rows20 + rows19 + form]
    if len({round(c, 4) for c in corrs}) != len(corrs) or any(abs((c * 1e4) % 1 - 0.5) < 1e-3 for c in corrs):
        out.append(f"two pairs share a rounded correlation, or one sits on a rounding midpoint: {corrs}")
    targets = {**{f"P{k:02d}": 3.0 for k in range(21)}, **{f"P{k:02d}": kw["z"] for k, kw in SCENARIOS.items() if "z" in kw}}
    if any(not near(Z_END[name], z, 1e-9) for name, z in targets.items()):
        out.append(f"a pair's year-end z-score is off its target: {Z_END}")
    on_entry = {k: z_at(PX[f"P{k:02d}A"], PX[f"P{k:02d}B"], 2019, PX[f"P{k:02d}A"][D20(1, 2)], PX[f"P{k:02d}B"][D20(1, 2)])
                for k in (7, 8)}
    if not (near(on_entry[7], 0.0, 1e-9) and near(on_entry[8], 3.0, 1e-9)):
        out.append(f"the two pairs whose entry-bar z-score differs from the year-end one: {on_entry}")
    m20, m19 = model(rows20[:20], 2020), model(rows19[:20], 2019)
    fills = {tuple(f["pair"]): (f["last_common_bar"], f["missing_leg"]) for f in m20["fills"]}
    if (m20["entered"] != ENTERED_2020 or fills != FILLED_2020 or (len(m20["drop"]), len(m20["ltp"]), m20["missing"]) != (11, 14, 3)
            or m20["capped"] != {"drop": 0, "ltp": 2}):   # P15's short leg and P18's long leg, at their last common bars
        out.append(f"2020: {m20['entered']} entered, fills {fills}, books {len(m20['drop'])}/{len(m20['ltp'])}, capped {m20['capped']}")
    fill_gains = [PX[s][d] / PX[s][D20(1, 2)] - 1.0 for s, d in (("P15A", JUN15), ("P15B", JUN15), ("P18A", D20(12, 19)), ("P18B", D20(12, 19)))]
    if not (fill_gains[0] > 2.4 and fill_gains[3] > 2.4 and abs(fill_gains[1]) < 0.1 and fill_gains[2] == 0.0):
        out.append(f"the filled pairs' capped legs are not P15's A and P18's B alone: {fill_gains}")
    if (m19["entered"] != ENTERED_2019 or (len(m19["drop"]), len(m19["ltp"]), m19["missing"], m19["fills"]) != (6, 6, 1, [])
            or m19["capped"] != {"drop": 3, "ltp": 3}):   # one short leg and two long legs
        out.append(f"2019: {m19['entered']} entered, books {len(m19['drop'])}/{len(m19['ltp'])}, capped {m19['capped']}")
    gains = [PX[s][Y19_END] / PX[s][date(2019, 1, 2)] - 1.0 for s in ("Q1A", "Q2A", "Q3B", "Q4A", "Q4B", "Q6B")]
    if not (1.98 < gains[0] < 2.0 and gains[1] == 2.0 and gains[2] > 2.4 and 2.0 < gains[3] < 2.02 and gains[4] > 2.1
            and gains[5] == 2.0):
        out.append(f"the capped legs do not gain 199%, 200%, 250%, 201%, 220% and 200%: {gains}")
    off0 = model(rows20[:20], 2020, offset=0)
    if off0["entered"] != P(0, 1, 2, 3, 5, 7, 10, 15, 16, 17, 18, 19) or off0["missing"] != 4:
        out.append(f"2020 at offset 0: {off0['entered']} entered, {off0['missing']} missing exits")
    if (counts[2019]["candidates"], counts[2019]["sectors"], counts[2020]["candidates"], counts[2020]["same_company"],
            counts[2020]["no_cost_cap"]) != (16, 8, 59, 1, 1):
        out.append(f"the main leg's universe counts: {counts}")
    for exchange, entered, books in (("ZCASH", 2, (0, 0)), ("ZTHREE", 3, (2, 3)), ("ZFLAT", 0, (0, 0)), ("ZSIG", 2, (0, 0))):
        m = model(ranked(leg(exchange)[0], 2020)[:20], 2020, bounds={})
        if (len(m["entered"]), (len(m["drop"]), len(m["ltp"]))) != (entered, books):
            out.append(f"{exchange}: {len(m['entered'])} entered, books {len(m['drop'])}/{len(m['ltp'])}")
    sig = {}
    for name in ("S3", "S4"):   # (A's last bar of 2019, B's, z on the last common bar, z on each leg's own last bar)
        a, b = PX[name + "A"], PX[name + "B"]
        last_a, last_b = (max(d for d in px if d <= Y19_END) for px in (a, b))
        sig[name] = (last_a, last_b, z_at(a, b, 2019, a[DEC29], b[DEC29]), z_at(a, b, 2019, a[last_a], b[last_b]))
    sig_rows = ranked(leg("ZSIG")[0], 2020)
    if (sig["S3"][:2] != (Y19_END, DEC29) or sig["S4"][:2] != (DEC29, Y19_END)
            or not (near(sig["S3"][2], 1.0, 1e-9) and near(sig["S4"][2], -1.0, 1e-9))
            or sig["S3"][3] < 3.0 or sig["S4"][3] > -3.0
            or sorted(r[:2] for r in sig_rows) != [(f"S{k}A", f"S{k}B") for k in range(1, 5)]
            or any(abs((r[3] * 1e4) % 1 - 0.5) < 1e-3 for r in sig_rows)):
        out.append(f"the pairs whose legs end 2019 on different days: {sig}, {sig_rows}")
    year, wide = stats(PX["P00A"], PX["P00B"], T19, Y19_END), stats(PX["P00A"], PX["P00B"], ORIGIN, Y19_END)
    if min(PX["P00A"]) != date(2018, 7, 1) or abs(wide[0] - year[0]) < 0.1 or abs(wide[1] - year[1]) < 0.1:
        out.append(f"P00's bars of 2018 do not move its hedge ratio and spread mean: {year} {wide}")
    flat = [stats(PX[r[0]], PX[r[1]], T19, Y19_END) for r in ranked(leg("ZFLAT")[0], 2020)]
    if len(flat) != 3 or any(not 1e-14 < sd < 1e-10 or abs(s_end - m) / sd < 10 for _, m, sd, s_end in flat):
        out.append(f"the flat-spread pairs are not three pairs under the std floor with a large z-score: {flat}")
    firsts = (first_bar(KOSPI, date(2019, 1, 2), 11), first_bar(KOSPI, D20(1, 2), 10), first_bar(KOSPI, date(2021, 1, 1), 15),
              first_bar(TAIEX, date(2021, 1, 1), 16))
    if ([f and f[0] for f in firsts] != [date(2019, 1, 13), D20(1, 12), date(2021, 1, 16), date(2021, 1, 17)]
            or [bench_return(i, y) is None for i in (KOSPI, TAIEX) for y in YEARS] != [True, False, False, True]
            or "^N225" in PX):
        out.append(f"the indices with gaps: first bars {firsts}")
    return out


def run(mod, con, mode, bench="SPY", bounds=None, **kw):
    ltp = pairs_universe.PairLtp(con, BOUNDS if bounds is None else bounds) if mode != "drop" else None
    return mod.run_backtest(con, benchmark_symbol=bench, ltp=ltp, exit_method=mode, **kw), ltp


def year_checks(mod):
    """run_backtest on the main leg: both years, the three exit methods, both execution offsets."""
    con = leg("XETRA")[0]
    for mode, kw in ([(m, {}) for m in EXIT_METHODS] + [("both", {"offset_days": 0}), ("drop", {"use_costs": False})]):
        records, ltp = run(mod, con, mode, BENCH, **kw)
        if [r["year"] for r in records] != list(YEARS):
            yield f"{mode} {kw}: years {[r['year'] for r in records]}"
            continue
        models = []
        for rec in records:
            wanted, m = wanted_record(con, rec["year"], mode, kw.get("offset_days", 1), kw.get("use_costs", True), BENCH)
            models.append(m)
            if not same_record(rec, wanted):
                diff = sorted(k for k in set(rec) | set(wanted) if k == "pairs" or rec.get(k) != wanted.get(k))
                yield f"{rec['year']} {mode} {kw}: differs on {diff}: {({k: v for k, v in rec.items() if k != 'pairs'})}"
        if ltp is not None:
            fills = [f for m in models for f in m["fills"]]
            if (ltp.missing, ltp.filled) != (sum(m["missing"] for m in models), len(fills)) or not same_fills(ltp.fills, fills):
                yield f"{mode} {kw}: {ltp.missing} missing exits, {ltp.filled} filled: {ltp.fills}"


def formation_checks(mod):
    """compute_correlations on the main leg, the boundary leg, the 21-line leg and a day printed twice."""
    for exchange in ("XETRA", "ZFORM", "ZLIM"):
        con = leg(exchange)[0]
        for year in YEARS:
            got = mod.compute_correlations(con, date(year - 1, 1, 1), date(year - 1, 12, 31))
            if not rows_match(got, ranked(con, year)):
                yield f"{exchange} {year} correlation list: {[tuple(g[:2]) + tuple(g[3:]) for g in got][:25]}"
    # hedge ratio, spread mean and std come from the formation year alone: P00 has bars from the year before
    wanted = stats(PX["P00A"], PX["P00B"], T19, Y19_END)
    params = mod.compute_spread_params(leg("XETRA")[0], "P00A", "P00B", T19, Y19_END)
    if params is None or len(params) != 4 or not all(near(x, y, 1e-9) for x, y in zip(params, wanted)):
        yield f"spread parameters of a pair with bars before the formation year: {params} != {wanted}"
    # legs that end the formation year on different days: the signal is the spread on the last bar both printed
    for name in ("S3", "S4"):
        wanted = stats(PX[name + "A"], PX[name + "B"], T19, Y19_END)
        params = mod.compute_spread_params(leg("ZSIG")[0], name + "A", name + "B", T19, Y19_END)
        if params is None or len(params) != 4 or not all(near(x, y, 1e-9) for x, y in zip(params, wanted)):
            yield f"spread parameters of {name}, whose legs end the year on different days: {params} != {wanted}"
    names = [f"L{i:02d}" for i in range(1, 22)]
    tied = [(a, b, 1.0, 364) for i, a in enumerate(names) for b in names[i + 1:]][:100]
    if [(r[0], r[1], round(r[3], 4), r[4]) for r in ranked(leg("ZLIM")[0], 2020)] != tied:
        yield "fixture: the 21-line leg does not give 210 pairs at correlation 1.0000"
    high, low = dup_corr(True), dup_corr(False)
    if round(high[0], 4) == round(low[0], 4) or high[0] < 0.75:
        yield f"fixture: the twice-printed day does not move the correlation: {high} {low}"
    for order in (1, -1):
        con = dup_con(order)
        got = mod.compute_correlations(con, T19, Y19_END)
        if not rows_match(got, [("DA", "DB", "Dup") + high]):
            yield f"a day printed twice, rows {'as' if order > 0 else 'against'} inserted: {got} != {high}"
        # the entry signal takes the higher print of each leg on the last common bar
        beta, m, sd, s_end = stats(PX["P05A"], PX["P05B"], T19, Y19_END, prints=dup_prints())
        params = mod.compute_spread_params(con, "DA", "DB", T19, Y19_END)
        if params is None or len(params) != 4 or not all(near(x, y, 1e-9) for x, y in zip(params, (beta, m, sd, s_end))):
            yield f"spread parameters with the last day printed twice: {params} != {(beta, m, sd, s_end)}"
        # so does a year-end or benchmark price
        price = mod.get_price_at_date(con, "DA", Y19_END)
        if not near(price, PX["P05A"][Y19_END], 1e-9):
            yield f"the price of a day printed twice, rows {'as' if order > 0 else 'against'} inserted: {price}"


DUP_DAY = date(2019, 7, 1)


def dup_prints():
    """(log A, log B) of the rows the second prints add to the pair's common bars."""
    a, b = PX["P05A"], PX["P05B"]
    extra = [(1.5 * a[DUP_DAY], b[DUP_DAY]), (a[Y19_END], 0.9 * b[Y19_END]), (0.9 * a[Y19_END], b[Y19_END]),
             (0.9 * a[Y19_END], 0.9 * b[Y19_END])]
    return [(math.log(pa), math.log(pb)) for pa, pb in extra]


def dup_con(order):
    """Two 2020 candidates. DA prints 1 July 2019 twice, the second print 50% higher; both lines
    print 31 December twice, the second print 10% lower."""
    con = duckdb.connect()
    con.execute("CREATE TABLE candidates(yr INTEGER, symbol VARCHAR, sector VARCHAR, company VARCHAR,"
                " size_usd DOUBLE, cap_usd DOUBLE)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)")
    con.execute("INSERT INTO candidates VALUES (2020, 'DA', 'Dup', 'DA', 1.0, NULL), (2020, 'DB', 'Dup', 'DB', 1.0, NULL)")
    rows = [(s, d, ep(d), PX[p][d]) for s, p in (("DA", "P05A"), ("DB", "P05B")) for d in span(T19, Y19_END)]
    rows.append(("DA", DUP_DAY, ep(DUP_DAY), 1.5 * PX["P05A"][DUP_DAY]))
    rows += [(s, Y19_END, ep(Y19_END), 0.9 * PX[p][Y19_END]) for s, p in (("DA", "P05A"), ("DB", "P05B"))]
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)", rows[::order])
    return con


def dup_corr(high_first):
    """DA/DB correlation and common days when each day's two prints are taken high first, or low first."""
    a, b, days = PX["P05A"], PX["P05B"], span(T19, Y19_END)
    seqs = []
    for px, twice in ((a, {DUP_DAY: 1.5, Y19_END: 0.9}), (b, {Y19_END: 0.9})):
        seq = []
        for d in days:
            prints = sorted({px[d], twice.get(d, 1.0) * px[d]}, reverse=high_first)
            seq += [(d, p) for p in prints]
        seqs.append([(d, p / q - 1.0) for (_, q), (d, p) in zip(seq, seq[1:])])
    rb = {}
    for d, r in seqs[1]:
        rb.setdefault(d, []).append(r)
    joined = [(ra, r) for d, ra in seqs[0] for r in rb.get(d, [])]   # the self-join pairs every print of a day
    return pearson([x for x, _ in joined], [y for _, y in joined]), len(joined)


CASH_RESULTS = {"ZCASH": (2, 0, 0), "ZTHREE": (1, 1, 2.0), "ZFLAT": (2, 0, 0), "ZSIG": (2, 0, 0)}   # cash years, invested years, average pairs


def cash_checks(mod):
    """Two entered pairs are a cash year, three are invested, on the entry-side count; a pair whose
    spread is flat does not enter. Then the results of those records, and the same-company guard."""
    for exchange in CASH_RESULTS:
        con, runs = leg(exchange)[0], {}
        for mode in EXIT_METHODS:
            records, ltp = run(mod, con, mode, bounds={})
            rec, (wanted, m) = records[-1], wanted_record(con, 2020, mode, bounds={})
            runs[mode] = records
            if not same_record(rec, wanted):
                yield f"{exchange} {mode}: {({k: v for k, v in rec.items() if k != 'pairs'})}"
            if ltp is not None and (ltp.missing, ltp.filled, len(ltp.fills)) != (m["missing"], len(m["fills"]), len(m["fills"])):
                yield f"{exchange} {mode}: the exit helper counted {ltp.missing} missing, {ltp.filled} filled"
        if exchange != "ZTHREE" and any("ltp" in k for records in runs.values() for r in records for k in r):
            yield f"{exchange}: a cash year carries an ltp key: {runs}"
        if exchange == "ZTHREE":
            strip = lambda records: [{k: v for k, v in r.items() if "ltp" not in k} for r in records]
            if strip(runs["both"]) != runs["drop"] or any("ltp" in k for r in runs["drop"] + runs["ltp"] for k in r):
                yield f"the both run is not the drop run plus the LTP book: {runs['both']} vs {runs['drop']}"
        # the results count cash and average the book over the entered pairs
        valid = runs["drop"]
        metrics = compute_metrics([r["portfolio_return"] for r in valid], [r["spy_return"] for r in valid], 1,
                                  risk_free_rate=0.02)
        out = mod.build_output(metrics, [], valid, valid, exchange)
        got = (out.get("cash_periods"), out.get("invested_periods"), out.get("avg_pairs_when_invested"))
        if got != CASH_RESULTS[exchange]:
            yield f"{exchange} results: cash, invested, average pairs {got}"
    con = leg("ZTHREE")[0]
    con.execute("UPDATE candidates SET company = 'T1A' WHERE symbol = 'T1B'")
    try:
        run(mod, con, "drop")
        yield "two lines of one company among the pairs did not stop the run"
    except AssertionError:
        pass
    finally:
        con.execute("UPDATE candidates SET company = 'T1B' WHERE symbol = 'T1B'")


def fetch_checks(mod):
    """fetch_data_via_api: what the topic asks of the universe, and the 20-candidate floor."""
    for offset, offs in ((1, ["ISB"]), (0, ["ISA"])):
        got = mod.fetch_data_via_api(WH, ["ZFETCH"], offset_days=offset)
        if got is None:
            yield f"the fetch leg at offset {offset} returned nothing"
            continue
        con, per_year = got
        books, wanted = book(con, 2020), {**FETCH_BOOK, "Offs": offs}
        symbols = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM prices_cache").fetchall()}
        if books != wanted or book(con, 2019) or sorted(per_year) != list(YEARS) or per_year[2020].get("candidates") != 34:
            yield f"fetch leg at offset {offset}: {books} != {wanted}, counts {per_year}"
        if symbols != {s for v in wanted.values() for s in v} | {"SPY"}:
            yield f"fetch leg at offset {offset}: prices of {sorted(symbols)}"
        con.close()
    for exchange, some in (("ZG20", True), ("ZG19", False), ("ZNONE", False)):
        got = mod.fetch_data_via_api(WH, [exchange])
        if (got is not None) != some:
            yield f"a leg of {exchange[2:]} candidates: {'skipped' if some else 'run'}"
        if got:
            got[0].close()


FILL_2020 = {"pair": ["P16A", "P16B"], "year": 2020, "entry_bars": ["2020-01-02", "2020-01-02"],
             "last_common_bar": "2020-06-17", "missing_leg": ["P16B"], "delisted_date": [None, "2020-06-10"],
             "delisting_bound": ["none", "applied"], "exit_date": "2020-12-31"}
OVERCLAIM = "survivorship" + "-" + "free"   # the wording no results file or source may carry
CHART_KEYS = ("universe", "annual_returns", "portfolio", "spy", "benchmark_name", "cash_periods", "invested_periods",
              "total_rebalances", "n_years", "avg_pairs_when_invested", "comparison", "excess_cagr", "win_rate_vs_spy",
              "years", "frequency", "execution", "benchmark_symbol")   # in all, every key of the committed results files
BOOK_KEYS = ("cagr", "max_drawdown", "sharpe_ratio", "total_return", "annualized_volatility")   # a book in the exit block


def metric_blocks(records, rfr):
    """The portfolio, benchmark and comparison figures the charts and posts read, from the year records."""
    m = compute_metrics([r["portfolio_return"] for r in records], [r["spy_return"] for r in records], 1, risk_free_rate=rfr)
    pct = lambda v: None if v is None else round(v * 100, 2)
    rnd = lambda v: None if v is None else round(v, 3)
    side = lambda s: {**{k: pct(s.get(k)) for k in ("cagr", "max_drawdown", "annualized_volatility", "total_return")},
                      **{k: rnd(s.get(k)) for k in ("sharpe_ratio", "sortino_ratio", "calmar_ratio")}}
    c = m["comparison"]
    comparison = {**{k: pct(c.get(k)) for k in ("excess_cagr", "win_rate", "up_capture", "down_capture", "alpha")},
                  **{k: rnd(c.get(k)) for k in ("information_ratio", "beta")}}
    return {"portfolio": side(m["portfolio"]), "spy": side(m["benchmark"]), "comparison": comparison,
            "excess_cagr": comparison["excess_cagr"], "win_rate_vs_spy": comparison["win_rate"]}


def main_checks(mod):
    """run_single on the XETRA leg in the three exit methods: fetch, two years, the results JSON."""
    con, counts = leg("XETRA")
    seen, real, outs, queries = {}, mod.run_backtest, {}, {}

    def spy(con_, **kw):
        seen["books"] = {y: book(con_, y) for y in YEARS}
        seen["kw"], seen["out"] = kw, real(con_, **kw)
        return seen["out"]

    mod.run_backtest = spy
    # the run's offset and cost switch reach the fetch and the year loop
    with contextlib.redirect_stdout(io.StringIO()):   # cli_utils prints the run's header
        out = mod.run_single(WH, ["ZFETCH"], "ZFETCH", False, 0.02, False, None, offset_days=0, exit_method="drop")
    kw = seen.get("kw", {})
    if (out is None or seen["books"][2020].get("Offs") != ["ISA"] or out.get("listing_filter") is not None
            or (kw.get("offset_days"), kw.get("use_costs"), kw.get("benchmark_symbol")) != (0, False, "SPY")):
        yield f"a run without costs at offset 0: run_backtest was called with {kw} on {seen.get('books')}"
    for mode in EXIT_METHODS:
        seen.clear()
        sent = len(WH.sent)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "results", "returns_XETRA.json")
            with contextlib.redirect_stdout(io.StringIO()):   # cli_utils prints the run's header
                out = mod.run_single(WH, MAIN, "XETRA", True, 0.03, False, path, offset_days=1, exit_method=mode)
            with open(path) as fh:
                if out is None or json.load(fh) != json.loads(json.dumps(out)):
                    yield f"{mode}: the results file is not the returned results"
        if out is None or "out" not in seen:
            yield f"{mode}: run_single returned nothing"
            return
        outs[mode] = json.loads(json.dumps(out))
        asked, queries[mode] = sum("delisted_companies" in sql for sql in WH.sent[sent:]), len(WH.sent) - sent
        if asked != (mode != "drop") or queries[mode] != queries["drop"] + asked:
            yield f"{mode}: {asked} delisting queries, {queries[mode]} queries against the drop run's {queries['drop']}"
        kw, records = seen["kw"], seen["out"]
        if seen["books"] != {y: book(con, y) for y in YEARS}:
            yield f"{mode}: the main leg's candidates are not the module's: {seen['books']}"
        if ((kw.get("benchmark_symbol"), kw.get("offset_days"), kw.get("use_costs"), kw.get("exit_method"))
                != (BENCH, 1, True, mode) or (kw.get("ltp") is None) != (mode == "drop")):
            yield f"{mode}: run_backtest was called with {kw}"
        for rec in records:
            if not same_record(rec, wanted_record(con, rec["year"], mode, bench=BENCH)[0]):
                yield f"{mode} {rec['year']}: run_single's record {({k: v for k, v in rec.items() if k != 'pairs'})}"

        # the results JSON
        if any(k not in out for k in CHART_KEYS) or out.get("universe") != "XETRA":
            yield f"{mode}: a key the charts and posts read is gone, or universe is not the leg's name: {sorted(out)}"
        rule = {**pairs_universe.UNIVERSE_NOTES, "top_n_per_sector": 30, "min_formation_bars": 200}
        active = [r["pairs_active"] for r in records]
        for key, value in (("universe_rule", rule), ("listing_filter", "country DE"), ("isin_dedup", True),
                           ("company_dedup", True), ("years", "2019-2020"), ("total_rebalances", 2), ("n_periods", 2),
                           ("n_years", 2), ("cash_periods", 0), ("invested_periods", 2),
                           ("legs_capped", 5 if mode == "ltp" else 3),   # the published book's: two more at last common bars
                           ("avg_pairs_when_invested", round(sum(active) / 2, 1)),
                           ("benchmark_name", get_local_benchmark(MAIN)[1]), ("benchmark_symbol", BENCH)):
            if out.get(key) != value:
                yield f"{mode}: results {key}: {out.get(key)} != {value}"
        for key, value in metric_blocks(records, 0.03).items():
            if out.get(key) != value:
                yield f"{mode}: results {key}: {out.get(key)} != {value}"
        annual = out.get("annual_returns") or []
        for rec, row in zip(records, annual if len(annual) == len(records) else [{}] * len(records)):
            wanted = {"year": rec["year"], "portfolio": round(rec["portfolio_return"] * 100, 2),
                      "spy": round(rec["spy_return"] * 100, 2),
                      "excess": round((rec["portfolio_return"] - rec["spy_return"]) * 100, 2),
                      **{k: v for k, v in rec.items() if k not in ("year", "portfolio_return", "spy_return")},
                      **counts[rec["year"]]}
            if row != wanted:
                diff = sorted(k for k in set(row) | set(wanted) if row.get(k) != wanted.get(k))
                yield f"{mode}: annual_returns {rec['year']} differs on {diff}"
        block = out.get("ltp")
        if mode == "drop":
            if block is not None or "exit_method" in out or gate_ltp.has_ltp_keys(outs[mode]):
                yield f"the drop results carry an exit-method key: {sorted(out)}"
        else:
            keys = {"exit_method", "source", "delist_grace_days", "missing_exits", "ltp_filled", "still_missing", "fills"}
            keys |= {"drop_portfolio", "ltp_portfolio"} if mode == "both" else set()
            if out.get("exit_method") != mode or not isinstance(block, dict) or set(block) != keys:
                yield f"{mode}: exit_method {out.get('exit_method')}, ltp block {block and sorted(block)}"
            elif ((block["exit_method"], block["missing_exits"], block["ltp_filled"], block["still_missing"])
                    != (mode, 4, 3, 1) or [f["pair"] for f in block["fills"]] != [list(p) for p in FILLED_2020]
                    or {k: block["fills"][1].get(k) for k in FILL_2020} != FILL_2020):
                yield f"{mode}: the ltp block: {block}"
            elif mode == "both" and block["drop_portfolio"] != {k: out["portfolio"][k] for k in BOOK_KEYS}:
                yield f"both: the exit block's drop book is not the published book: {block['drop_portfolio']}"
        if OVERCLAIM in json.dumps(out).lower():
            yield f"{mode}: the results claim survivorship is removed: it is only reduced"
    # the both run's LTP book is the ltp run's published book, and the gate's own stage A check on the three files
    if outs["both"]["ltp"]["ltp_portfolio"] != {k: outs["ltp"]["portfolio"][k] for k in BOOK_KEYS}:
        yield f"the both run's LTP book is not the ltp run's published book: {outs['both']['ltp']['ltp_portfolio']}"
    gate = {"topic": "pairs-fundamentals", "pairs": [{"published_cagr_path": "portfolio.cagr", "ltp_block_path": "ltp"}]}
    problems = gate_ltp.check_stage_a(gate, outs["both"], outs["drop"], outs["ltp"], with_ltp=True)[1]
    if problems or outs["drop"]["portfolio"]["cagr"] == outs["ltp"]["portfolio"]["cagr"]:
        yield f"gate_ltp stage A on the three results: {problems}, drop and ltp CAGR {outs['drop']['portfolio']['cagr']}"

    # an index with no bar at the start of 2019, or at the end of 2020: that year is run and counted, not measured.
    # ZG20 brings the leg to 20 candidates; KSC and TAI add an index and no line, as TWO adds none to the Taiwan leg.
    for code, index, mode, measured in (("KSC", KOSPI, "both", 2020), ("TAI", TAIEX, "drop", 2019)):
        seen.clear()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):   # and the window warning
            out = mod.run_single(WH, ["ZTHREE", "ZG20", code], code, True, 0.02, False, None, offset_days=1, exit_method=mode)
        records = seen.get("out") or []
        wanted = [wanted_record(leg("ZTHREE", "ZG20")[0], y, mode, bench=index)[0] for y in YEARS]
        if ([w["year"] for w in wanted if w["spy_return"] is not None] != [measured] or out is None or len(records) != 2
                or not all(same_record(r, w) for r, w in zip(records, wanted))):
            yield f"{code}, an index with a gap: {[({k: v for k, v in r.items() if k != 'pairs'}) for r in records]}"
            continue
        got = [out.get(k) for k in ("n_years", "n_periods", "total_rebalances", "cash_periods", "invested_periods",
                                    "window_truncated", "measured_start", "measured_end", "benchmark_symbol",
                                    "benchmark_name")]
        if got != [1, 1, 2, 1, 1, True, str(measured), str(measured), index, get_local_benchmark([code])[1]]:
            yield f"{code}, an index with a gap: years measured, run, cash and invested, window, benchmark {got}"
        rows = [(r.get("year"), r.get("spy"), r.get("excess")) for r in out.get("annual_returns") or []]
        if rows != [(r["year"], None if r["spy_return"] is None else round(r["spy_return"] * 100, 2),
                     round((r["portfolio_return"] - (r["spy_return"] or 0.0)) * 100, 2)) for r in records]:
            yield f"{code}, an index with a gap: annual rows {rows}"
        block = out.get("ltp") or {}
        if mode == "both" and ((block.get("missing_exits"), block.get("ltp_filled")) != (1, 1)
                               or block.get("drop_portfolio") != {k: out["portfolio"][k] for k in BOOK_KEYS}):
            yield f"{code}, an index with a gap: the exit block {block}"
    # an index with no bar at all: the S&P 500 stands in
    seen.clear()
    with contextlib.redirect_stdout(io.StringIO()):
        out = mod.run_single(WH, ["ZCASH", "ZG20", "JPX"], "NONE", True, 0.02, False, None, offset_days=1, exit_method="drop")
    got = out and (out.get("benchmark_symbol"), out.get("benchmark_name"), out.get("cash_periods"), out.get("n_years"))
    if got != ("SPY", "S&P 500", 2, 2) or seen.get("kw", {}).get("benchmark_symbol") != "SPY":
        yield f"a leg whose index has no bars: {got}, run_backtest was called with {seen.get('kw')}"
    for problem in WH.problems:
        yield problem


LEGS = [("us", ["NYSE", "NASDAQ", "AMEX"]), ("india", ["NSE"]), ("japan", ["JPX"]), ("uk", ["LSE"]),
        ("china", ["SHZ", "SHH"]), ("hongkong", ["HKSE"]), ("korea", ["KSC"]), ("taiwan", ["TAI", "TWO"]),
        ("germany", ["XETRA"]), ("canada", ["TSX"]), ("sweden", ["STO"]), ("switzerland", ["SIX"]),
        ("thailand", ["SET"]), ("southafrica", ["JNB"]), ("singapore", ["SES"])]


def cli_checks(mod):
    """main(): the exit method, the default from data_utils, reaches every market of the all-market loop,
    with the market's own risk-free rate; a market that fails is recorded and the loop goes on."""
    calls, broken = [], []

    def run_single(cr, exchanges, name, use_costs, rfr, verbose, output_path=None, **kw):
        calls.append((exchanges, name, use_costs, rfr, verbose, output_path, kw))
        if name in broken:
            raise RuntimeError(f"no {name} today")
        return {"portfolio": {"cagr": 1.0}, "universe": name}

    def markets(folder, method, rfr=None, verbose=False):
        return [(ex, "_".join(ex), True, rfr or get_risk_free_rate(ex), verbose,
                 os.path.join(folder, f"returns_{'_'.join(ex)}.json"), {"offset_days": 1, "exit_method": method})
                for _, ex in LEGS]

    us, default = ["NYSE", "NASDAQ", "AMEX"], {"offset_days": 1, "exit_method": DEFAULT_EXIT_METHOD}
    if len({get_risk_free_rate(ex) for _, ex in LEGS}) < 5:
        yield "fixture: the markets do not have risk-free rates of their own"
    argv, real = sys.argv, (mod.CetaResearch, mod.run_single)
    mod.CetaResearch = lambda *a, **k: calls.append("client") or "client"
    mod.run_single = run_single
    try:
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "exchange_comparison.json")
            runs = [(["--exchange", "NYSE,NASDAQ,AMEX", "--output", target],
                     [(us, "NYSE_NASDAQ_AMEX", True, get_risk_free_rate(us), False, target, default)], []),
                    (["--preset", "japan"], [(["JPX"], "JPX", True, get_risk_free_rate(["JPX"]), False, None, default)], []),
                    (["--preset", "japan", "--exit-method", "both", "--no-next-day", "--no-costs",
                      "--risk-free-rate", "0.07", "--verbose"],
                     [(["JPX"], "JPX", False, 0.07, True, None, {"offset_days": 0, "exit_method": "both"})], []),
                    (["--global", "--output", target], markets(tmp, DEFAULT_EXIT_METHOD), []),
                    (["--global", "--output", target, "--exit-method", "drop", "--risk-free-rate", "0.07", "--verbose"],
                     markets(tmp, "drop", 0.07, True), []),
                    (["--global", "--output", target, "--exit-method", "ltp"], markets(tmp, "ltp"), ["JPX", "SES"])]
            runs += [(["--exit-method", method], [(us, "US_MAJOR", True, get_risk_free_rate(us), False, None,
                                                   {"offset_days": 1, "exit_method": method})], []) for method in EXIT_METHODS]
            for flags, wanted, failing in runs:
                del calls[:]
                broken[:] = failing
                sys.argv = ["backtest.py"] + flags
                try:
                    with contextlib.redirect_stderr(io.StringIO()):   # a failed market's traceback
                        mod.main()
                    got = calls[1:] if calls[:1] == ["client"] else f"no client first: {calls}"
                except SystemExit as e:
                    got = f"exit {e.code} after {calls}"
                if got != wanted:
                    yield f"main() with {flags}: {got} != {wanted}"
                if "--global" in flags:
                    with open(target) as fh:
                        saved = json.load(fh)
                    if (list(saved) != ["_".join(ex) for _, ex in LEGS]
                            or any(node != ({"error": f"no {key} today"} if key in broken
                                            else {"portfolio": {"cagr": 1.0}, "universe": key}) for key, node in saved.items())):
                        yield f"main() with {flags}: the all-market file holds {saved}"
    finally:
        sys.argv = argv
        mod.CetaResearch, mod.run_single = real


def failures(src, first_only=False):
    out = []
    try:
        mod = load(src)
        for check in (cli_checks, year_checks, formation_checks, cash_checks, fetch_checks, main_checks):
            for f in check(mod):
                out.append(f)
                if first_only:
                    return out
    except Exception as e:  # a mutant that breaks the topic fails loudly
        out.append(f"error: {type(e).__name__}: {e}")
    finally:
        WH.problems = []
    return out


def const(name, value=None):
    """A parameter line's 'NAME = value ' as the topic spells it, or with another value."""
    line_ = next(ln for ln in SRC.splitlines() if ln.startswith(name + " "))
    return line_[:line_.index("#")].rstrip() + " " if value is None else f"{name} = {value} "


LAGS = ("(p.adjClose - LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date, p.adjClose DESC NULLS LAST))\n"
        "                    / NULLIF(LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date, p.adjClose DESC NULLS LAST), 0)")
RANK = "ORDER BY correlation DESC, sym_a, sym_b"
SIGNAL = "ORDER BY a.trade_date DESC, a.adjClose DESC NULLS LAST, b.adjClose DESC NULLS LAST"
IN_YEAR = "WHERE a.trade_date >= '{fs}' AND a.trade_date <= '{fe}'\n          AND a.adjClose > 0 AND b.adjClose > 0"
END_ROW = ("ON a.trade_date = b.trade_date\n           AND a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'\n        "
           + IN_YEAR + "\n        " + SIGNAL)


def own_bars(relation, order):
    """The signal query with the legs joined on `relation` in place of the same day, the latest by `order`."""
    return ("ON a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'" + relation + "\n        " + IN_YEAR
            + "\n          AND b.trade_date >= '{fs}' AND b.trade_date <= '{fe}'\n        ORDER BY " + order
            + SIGNAL[SIGNAL.index(", a.adjClose"):])


Z_START = "z_start = (spread_end - spread_mean) / spread_std"
FLOOR_TEST = "if not p_a0 or not p_b0 or p_a0 < MIN_LEG_PRICE or p_b0 < MIN_LEG_PRICE:"
SIDE = "entered.append((sym_a, sym_b, -1 if z_start > 0 else 1))"
ENTRY = "trading_start,\n                           offset_days=offset_days, with_epochs=True)"
BENCH_START = "trading_start,\n                                        offset_days=offset_days)"
BENCH_END = "window_days=15, offset_days=offset_days)"
EXITS = "trading_end, window_days=15,\n                                          offset_days=offset_days)"
CASH = "invested = n_entered >= MIN_PAIRS_ACTIVE"
HELD = "        if invested:\n            # Prices at trading year end"
FILL = "entry, exits,\n                                 trading_end, MIN_PAIRS_ACTIVE)"
CAPS = 'SELECT symbol, cap_usd FROM candidates WHERE yr = ?",\n                                [year]'
PER_PAIR = "p_a1, p_b1 = pair_exits.get((sym_a, sym_b)) or (exits.get(sym_a), exits.get(sym_b))"
PER_LEG = ("p_a1, p_b1 = [x or y for x, y in zip((exits.get(sym_a), exits.get(sym_b)),"
           " pair_exits.get((sym_a, sym_b)) or (None, None))]")
COUNT = "capped += (ret_a > MAX_SINGLE_RETURN) + (ret_b > MAX_SINGLE_RETURN)"
CAP_A, CAP_B = "ret_a = min(ret_a, MAX_SINGLE_RETURN)", "ret_b = min(ret_b, MAX_SINGLE_RETURN)"
CAP = COUNT + "\n                " + CAP_A + "\n                " + CAP_B
FILLED = "(sym_a, sym_b) in pair_exits"
LTP_FIELDS = '"pairs_active_ltp": len(r_ltp), "ltp_filled": n_filled})'
OLD_CAP = "if abs(ret_a) > MAX_SINGLE_RETURN or abs(ret_b) > MAX_SINGLE_RETURN:\n                    continue"
RET = "pair_return = direction * (ret_a - ret_b) / 2.0"
COST = "pair_return -= tiered_cost(caps.get(sym_a)) + tiered_cost(caps.get(sym_b))"
PUBLISH = 'book(fills if exit_method == "ltp" else {})'
BOTH = 'if exit_method == "both" and invested:'
MEAN = "port_return = sum(active_returns) / n_active if n_active else 0.0"
CHECK = "same_company = check_pairs(con, year, [(p[0], p[1], p[3]) for p in selected])"
PAIRS = 'for p in selected],\n        })'
CASH_OUT = 'cash_periods = sum(1 for r in executed if r["pairs_entered"] < MIN_PAIRS_ACTIVE)\n    invested = ['
INVESTED_OUT = 'invested = [r["pairs_active"] for r in executed if r["pairs_entered"] >= MIN_PAIRS_ACTIVE]'
COUNTS = '**(per_year or {}).get(r["year"], {}),'
LTP_ROWS = '**{k: v for k, v in r.items() if "ltp" in k},'
RULE = ('        "universe_rule": {**UNIVERSE_NOTES, "top_n_per_sector": TOP_N_PER_SECTOR,\n'
        '                          "min_formation_bars": MIN_CORR_DAYS},\n')
BUILD = "build_candidates(con, years, offset_days, MIN_LEG_PRICE,"
HELPER = 'PairLtp(con, load_delist_bounds(cr), verbose=verbose) if exit_method != "drop" else None'
YEAR_LOOP = "results = run_backtest(con, use_costs=use_costs, verbose=verbose,\n                           offset_days=offset_days,"
TO_LOOP = "ltp=ltp, exit_method=exit_method)"
BLOCK = "ltp.results_block(exit_method, valid, 1, risk_free_rate)"
ALL_MARKETS = "offset_days=offset_days,\n                                    exit_method=args.exit_method)"
ONE_MARKET = "args.verbose, args.output, offset_days=offset_days,\n               exit_method=args.exit_method)"
CORR_WIN = "WHERE p.trade_date >= '{fs}' AND p.trade_date <= '{fe}'"
STAT_WIN = ("WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'\n              AND symbol IN ('{sym_a}', '{sym_b}')\n"
            "              AND adjClose > 0\n        ),\n        ")
BETA_WIN, SPREAD_WIN = STAT_WIN + "paired AS (", STAT_WIN + "spread AS ("
NOT_BEFORE, NOT_AFTER = "trade_date >= '{fs}' AND ", " AND trade_date <= '{fe}'"
F_START, F_END = "formation_start = date(year - 1, 1, 1)", "formation_end   = date(year - 1, 12, 31)"
T_START, T_END = "trading_start   = date(year, 1, 1)", "trading_end     = date(year, 12, 31)"
CORR = "ROUND(CORR(a.ret, b.ret), 4)"
ASK = "ltp.fill(year, [(p[0], p[1]) for p in entered], entry, exits,"
BENCH_WINDOW = "window_days=10, offset_days=0):"
VALID = 'if r["portfolio_return"] is not None and r["spy_return"] is not None]'
RFR_ALL = "rfr = get_risk_free_rate(preset_exchanges, args.risk_free_rate)"
RFR_ONE = "risk_free_rate = get_risk_free_rate(exchanges, args.risk_free_rate)"
FAILED = 'all_results[uni_name] = {"error": str(e)}'
MUTANTS = {
    # formation: the year's candidates, same sector, 200 common days, correlation 0.70, never NaN
    "pairs from the formation year's candidates": ("year = formation_end.year + 1", "year = formation_end.year"),
    "pairs from any year's candidates": ("ON c.symbol = p.symbol AND c.yr = {year}", "ON c.symbol = p.symbol"),
    "formation year from 2 January": (F_START, F_START.replace("1, 1)", "1, 2)")),
    "formation from two years before": (F_START, F_START.replace("year - 1", "year - 2")),
    "formation year to 30 December": (F_END, F_END.replace("12, 31)", "12, 30)")),
    "formation year to 1 January": (F_END, "formation_end   = date(year, 1, 1)"),
    "correlation over bars before the formation year": (CORR_WIN, CORR_WIN.replace("p." + NOT_BEFORE, "")),
    "correlation over bars after the formation year": (CORR_WIN, CORR_WIN.replace(" AND p.trade_date <= '{fe}'", "")),
    "a pair listed in both orders": ("AND a.symbol < b.symbol", "AND a.symbol <> b.symbol"),
    "a line's first bar counted as a common day": ("WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL", "WHERE TRUE"),
    "A's first bar counted as a common day": ("WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL", "WHERE b.ret IS NOT NULL"),
    "B's first bar counted as a common day": ("WHERE a.ret IS NOT NULL AND b.ret IS NOT NULL", "WHERE a.ret IS NOT NULL"),
    "correlation not rounded": (CORR, "CORR(a.ret, b.ret)"),
    "correlation rounded to 3 places": (CORR, CORR.replace("4)", "3)")),
    "pairs across sectors": ("\n               AND a.sector = b.sector", ""),
    "199 common days enough": ("HAVING COUNT(*) >= {MIN_CORR_DAYS}", "HAVING COUNT(*) >= {MIN_CORR_DAYS - 1}"),
    "200 common days not enough": ("HAVING COUNT(*) >= {MIN_CORR_DAYS}", "HAVING COUNT(*) > {MIN_CORR_DAYS}"),
    "correlation 0.7000 refused": ("WHERE correlation >= {MIN_CORR}", "WHERE correlation > {MIN_CORR}"),
    "correlation 0.6999 accepted": (const("MIN_CORR"), const("MIN_CORR", "0.6999")),
    "NaN correlation ranks first": ("\n          AND NOT isnan(correlation)", ""),
    "lowest correlation first": (RANK, "ORDER BY correlation ASC, sym_a, sym_b"),
    "correlation ties: first symbol descending": (RANK, "ORDER BY correlation DESC, sym_a DESC, sym_b"),
    "correlation ties: second symbol descending": (RANK, "ORDER BY correlation DESC, sym_a, sym_b DESC"),
    # stands in for "no tie-break": DuckDB's own order among equals changes from run to run
    "correlation ties in hash order": (RANK, "ORDER BY correlation DESC, hash(sym_a || sym_b)"),
    "101 correlation rows": ("LIMIT {MAX_PAIRS * 5}", "LIMIT {MAX_PAIRS * 5 + 1}"),
    "99 correlation rows": ("LIMIT {MAX_PAIRS * 5}", "LIMIT {MAX_PAIRS * 5 - 1}"),
    "a day printed twice: lower print first": (LAGS, LAGS.replace("DESC", "ASC")),
    "19 pairs": (const("MAX_PAIRS"), const("MAX_PAIRS", 19)),
    "21 pairs": (const("MAX_PAIRS"), const("MAX_PAIRS", 21)),
    "every correlation row held": ("selected  = corr_rows[:MAX_PAIRS]", "selected  = corr_rows"),
    "same-company pairs not checked": (CHECK, "same_company = 0"),
    # the entry signal: the last common bar of the formation year
    "z-score from the bar the pair trades on": (Z_START, Z_START.replace(
        "spread_end", "math.log(p_a0) - beta * math.log(p_b0)")),
    "signal from the pair's first common bar": (SIGNAL, SIGNAL.replace("a.trade_date DESC", "a.trade_date ASC")),
    "signal from a bar after the formation year": (IN_YEAR, IN_YEAR.replace(" AND a.trade_date <= '{fe}'", "")),
    "signal from 31 December alone": (IN_YEAR, IN_YEAR.replace("a.trade_date >= '{fs}' AND a.trade_date <= '{fe}'",
                                                               "a.trade_date = '{fe}'")),
    "signal from each leg's own last bar of the year": (END_ROW, own_bars("", "a.trade_date DESC, b.trade_date DESC")),
    "signal from A's last bar and B's latest up to it": (END_ROW, own_bars(" AND b.trade_date <= a.trade_date",
                                                                           "a.trade_date DESC, b.trade_date DESC")),
    "signal from B's last bar and A's latest up to it": (END_ROW, own_bars(" AND a.trade_date <= b.trade_date",
                                                                           "b.trade_date DESC, a.trade_date DESC")),
    "signal from A's lower print of the day": (SIGNAL, SIGNAL.replace("a.adjClose DESC", "a.adjClose ASC")),
    "signal from B's lower print of the day": (SIGNAL, SIGNAL.replace("b.adjClose DESC", "b.adjClose ASC")),
    "signal on a hedge ratio of 1": ("SELECT LN(a.adjClose) - {beta} * LN(b.adjClose) AS s",
                                     "SELECT LN(a.adjClose) - LN(b.adjClose) AS s"),
    # the hedge ratio, spread mean and std: the formation year alone
    "hedge ratio over bars before the formation year": (BETA_WIN, BETA_WIN.replace(NOT_BEFORE, "")),
    "hedge ratio over bars after the formation year": (BETA_WIN, BETA_WIN.replace(NOT_AFTER, "")),
    "spread mean and std over bars before the formation year": (SPREAD_WIN, SPREAD_WIN.replace(NOT_BEFORE, "")),
    "spread mean and std over bars after the formation year": (SPREAD_WIN, SPREAD_WIN.replace(NOT_AFTER, "")),
    "hedge ratio of B on A": ("corr * std_a / NULLIF(std_b, 0)              AS beta",
                              "corr * std_b / NULLIF(std_a, 0)              AS beta"),
    "spread mean and std on a hedge ratio of 1": ("SELECT a.lp - {beta} * b.lp AS s", "SELECT a.lp - b.lp AS s"),
    "population std of the spread": ("STDDEV(s) AS std_s", "STDDEV_POP(s) AS std_s"),
    "a flat spread gives a signal": (" or float(spread_row[1]) < 1e-10):", "):"),
    "entry at z 1.49": (const("Z_ENTRY"), const("Z_ENTRY", 1.48)),
    "no entry at z 1.51": (const("Z_ENTRY"), const("Z_ENTRY", 1.52)),
    "no entry on a negative z": ("if abs(z_start) < Z_ENTRY:", "if z_start < Z_ENTRY:"),
    "the rich leg bought": (SIDE, SIDE.replace("-1 if z_start > 0 else 1", "1 if z_start > 0 else -1")),
    # entry prices: a bar in the 10 days from the entry date, 1.00 or more on both legs
    "entry price of 0.99 accepted": (const("MIN_LEG_PRICE"), const("MIN_LEG_PRICE", 0.99)),
    "entry price of 1.00 refused": (const("MIN_LEG_PRICE"), const("MIN_LEG_PRICE", 1.01)),
    "A at exactly 1.00 refused": (FLOOR_TEST, FLOOR_TEST.replace("p_a0 < MIN", "p_a0 <= MIN")),
    "B at exactly 1.00 refused": (FLOOR_TEST, FLOOR_TEST.replace("p_b0 < MIN", "p_b0 <= MIN")),
    "an A leg without an entry bar enters": (FLOOR_TEST, FLOOR_TEST.replace("not p_a0 or ", "")),
    "a B leg without an entry bar enters": (FLOOR_TEST, FLOOR_TEST.replace("or not p_b0 ", "")),
    "A leg's entry price not tested": (FLOOR_TEST, FLOOR_TEST.replace(" or p_a0 < MIN_LEG_PRICE", "")),
    "B leg's entry price not tested": (FLOOR_TEST, FLOOR_TEST.replace(" or p_b0 < MIN_LEG_PRICE", "")),
    "entry always on the day after the year starts": (ENTRY, ENTRY.replace("offset_days=offset_days", "offset_days=1")),
    "entry always on the day the year starts": (ENTRY, ENTRY.replace("offset_days=offset_days", "offset_days=0")),
    "entry prices from the year's end": (ENTRY, ENTRY.replace("trading_start", "trading_end")),
    "entry window of 9 days": (ENTRY, ENTRY.replace("with_epochs=True", "with_epochs=True, window_days=9")),
    "entry window of 11 days": (ENTRY, ENTRY.replace("with_epochs=True", "with_epochs=True, window_days=11")),
    "trading year from 2 January": (T_START, T_START.replace("1, 1)", "1, 2)")),
    "trading year from 31 December": (T_START, "trading_start   = date(year - 1, 12, 31)"),
    "trading year to 30 December": (T_END, T_END.replace("12, 31)", "12, 30)")),
    "trading year to 1 January": (T_END, "trading_end     = date(year + 1, 1, 1)"),
    # the benchmark: first bar in 10 days from the entry date and in 15 from the exit date, else no return
    "benchmark start ignores the offset": (BENCH_START, BENCH_START.replace("=offset_days", "=0")),
    "benchmark start always a day on": (BENCH_START, BENCH_START.replace("=offset_days", "=1")),
    "benchmark end ignores the offset": (BENCH_END, BENCH_END.replace("=offset_days", "=0")),
    "benchmark end always a day on": (BENCH_END, BENCH_END.replace("=offset_days", "=1")),
    "benchmark start window of 9 days": (BENCH_WINDOW, BENCH_WINDOW.replace("10", "9")),
    "benchmark start window of 11 days": (BENCH_WINDOW, BENCH_WINDOW.replace("10", "11")),
    "benchmark end window of 14 days": (BENCH_END, BENCH_END.replace("window_days=15", "window_days=14")),
    "benchmark end window of 16 days": (BENCH_END, BENCH_END.replace("window_days=15", "window_days=16")),
    "benchmark return on its exit price": ("(bench_end - bench_start) / bench_start", "(bench_end - bench_start) / bench_end"),
    "no fallback to the S&P 500": ('if bench_check == 0 and benchmark_symbol != "SPY":', "if False:"),
    "a year without a benchmark return measured": (VALID, VALID.replace(' and r["spy_return"] is not None', "")),
    # the cash rule: on the pairs entered, before anything is priced
    "cash with exactly 3 entered pairs": (CASH, CASH.replace(">=", ">")),
    "invested with 2 entered pairs": (const("MIN_PAIRS_ACTIVE"), const("MIN_PAIRS_ACTIVE", 2)),
    "cash with 3 entered pairs": (const("MIN_PAIRS_ACTIVE"), const("MIN_PAIRS_ACTIVE", 4)),
    "cash decided on the pairs priced at year end": (MEAN, MEAN.replace("if n_active else", "if n_active >= MIN_PAIRS_ACTIVE else")),
    "a cash year still holds its pairs": (HELD, HELD.replace("if invested:", "if True:")),
    # exits: a bar in the 15 days from the exit date, else the pair's last common bar
    "exit window of 14 days": (EXITS, EXITS.replace("window_days=15", "window_days=14")),
    "exit window of 16 days": (EXITS, EXITS.replace("window_days=15", "window_days=16")),
    "exits ignore the offset": (EXITS, EXITS.replace("=offset_days", "=0")),
    "exits always a day on": (EXITS, EXITS.replace("=offset_days", "=1")),
    "exit prices from the window's last bar": ("ORDER BY trade_date ASC, adjClose DESC NULLS LAST LIMIT 1",
                                               "ORDER BY trade_date DESC, adjClose DESC NULLS LAST LIMIT 1"),
    "exit price from the lower print of a day": ("ORDER BY trade_date ASC, adjClose DESC NULLS LAST LIMIT 1",
                                                 "ORDER BY trade_date ASC, adjClose ASC NULLS LAST LIMIT 1"),
    "fills take bars after the year end": (FILL, FILL.replace("trading_end,", "trading_end + timedelta(days=30),")),
    "fills requested in ltp mode only": ("            if ltp is not None:\n",
                                         '            if ltp is not None and exit_method == "ltp":\n'),
    "fills recorded against the year before": (ASK, ASK.replace("ltp.fill(year,", "ltp.fill(year - 1,")),
    "fills requested in reverse order": (ASK, ASK.replace("in entered]", "in entered][::-1]")),
    "fills requested with the legs swapped": (ASK, ASK.replace("(p[0], p[1])", "(p[1], p[0])")),
    "a filled pair keeps a leg's year-end price": (PER_PAIR, PER_LEG),
    "ltp mode publishes the drop book": (PUBLISH, "book({})"),
    "every mode with the helper publishes the LTP book": (PUBLISH, PUBLISH.replace('== "ltp"', '!= "drop"')),
    "both mode adds no LTP book": (BOTH, "if False:"),
    "LTP fields on a cash year": (BOTH, 'if exit_method == "both":'),
    "LTP fields in ltp mode": (BOTH, 'if exit_method != "drop" and invested:'),
    "both mode's LTP book is the drop book": ("r_ltp, _, n_filled = book(fills)", "r_ltp, _, n_filled = book({})"),
    "ltp_filled counts every priced pair": ("filled += (sym_a, sym_b) in pair_exits", "filled += 1"),
    "LTP book averaged over the entered pairs": ("round(sum(r_ltp) / len(r_ltp), 6)", "round(sum(r_ltp) / n_entered, 6)"),
    "LTP book's pair count from the drop book": ('"pairs_active_ltp": len(r_ltp)', '"pairs_active_ltp": n_active'),
    "the year averaged over the entered pairs": (MEAN, MEAN.replace("/ n_active if", "/ n_entered if")),
    # the book: half the spread return, each leg's gain capped at 200%, one cost per leg
    "the whole spread return": (RET, RET.replace("/ 2.0", "/ 1.0")),
    "B's return ignored": (RET, RET.replace("(ret_a - ret_b)", "ret_a")),
    "A's return ignored": (RET, RET.replace("(ret_a - ret_b)", "(-ret_b)")),
    "a pair with a leg above 200% is dropped": (CAP, OLD_CAP),
    "A's gain not capped": (CAP_A, "ret_a = ret_a"),
    "B's gain not capped": (CAP_B, "ret_b = ret_b"),
    "cap at 198%": (const("MAX_SINGLE_RETURN"), const("MAX_SINGLE_RETURN", 1.98)),
    "cap at 202%": (const("MAX_SINGLE_RETURN"), const("MAX_SINGLE_RETURN", 2.02)),
    "a gain of exactly 200% counted as capped": (COUNT, COUNT.replace(" > ", " >= ")),
    "A's gain of exactly 200% counted as capped": (COUNT, COUNT.replace("(ret_a > ", "(ret_a >= ")),
    "B's gain of exactly 200% counted as capped": (COUNT, COUNT.replace("(ret_b > ", "(ret_b >= ")),
    "B's capped gain counted in place of A's": (COUNT, COUNT.replace("(ret_a > ", "(ret_b > ")),
    "A's capped gain counted in place of B's": (COUNT, COUNT.replace("(ret_b > ", "(ret_a > ")),
    "A's capped gain not counted": (COUNT, "capped += (ret_b > MAX_SINGLE_RETURN)"),
    "B's capped gain not counted": (COUNT, "capped += (ret_a > MAX_SINGLE_RETURN)"),
    # a pair closed at its last common bar is capped and counted like any other, in the book that holds it
    "a filled pair's gains not capped": (CAP_A + "\n                " + CAP_B,
                                         f"if not {FILLED}:\n                    {CAP_A}\n                    {CAP_B}"),
    "a filled pair's A gain not capped": (CAP_A, f"ret_a = ret_a if {FILLED} else min(ret_a, MAX_SINGLE_RETURN)"),
    "a filled pair's B gain not capped": (CAP_B, f"ret_b = ret_b if {FILLED} else min(ret_b, MAX_SINGLE_RETURN)"),
    "a filled pair's capped legs not counted": (COUNT, COUNT.replace("capped += ", f"capped += 0 if {FILLED} else ")),
    "a filled pair's capped A leg not counted": (COUNT, COUNT.replace("(ret_a > MAX_SINGLE_RETURN)",
                                                                      f"(ret_a > MAX_SINGLE_RETURN and not {FILLED})")),
    "a filled pair's capped B leg not counted": (COUNT, COUNT.replace("(ret_b > MAX_SINGLE_RETURN)",
                                                                      f"(ret_b > MAX_SINGLE_RETURN and not {FILLED})")),
    "both mode records the LTP book's capped legs": (LTP_FIELDS, LTP_FIELDS.replace("})", ', "legs_capped": book(fills)[1]})')),
    "ltp mode records the drop book's capped legs": ('"legs_capped": legs_capped,', '"legs_capped": book({})[1],'),
    "each leg's cost charged twice": (COST, COST.replace("-= ", "-= 2 * (") + ")"),
    "A's cost for both legs": (COST, COST.replace("caps.get(sym_b)", "caps.get(sym_a)")),
    "B's cost for both legs": (COST, COST.replace("caps.get(sym_a)", "caps.get(sym_b)")),
    "costs when switched off": ("                if use_costs:\n", "                if True:\n"),
    "cost tier from the year before": (CAPS, CAPS.replace("[year]", "[year - 1]")),
    "cost tier from the year after": (CAPS, CAPS.replace("[year]", "[year + 1]")),
    "cost tier from the traded-value size": (CAPS, CAPS.replace("cap_usd", "size_usd")),
    # the year's record
    "entered pairs recorded as formed": ('"pairs_formed": len(selected),', '"pairs_formed": n_entered,'),
    "priced pairs recorded as entered": ('"pairs_entered": n_entered,', '"pairs_entered": n_active,'),
    "entered pairs recorded as active": ('"pairs_active": n_active,', '"pairs_active": n_entered,'),
    "capped legs not recorded": ('"legs_capped": legs_capped,', '"legs_capped": 0,'),
    "same-company count not recorded": ('"same_company_pairs": same_company,', '"same_company_pairs": None,'),
    "only the first pairs recorded": (PAIRS, PAIRS.replace("in selected]", "in selected[:n_entered]]")),
    # the results
    "cash years counted on the priced pairs": (CASH_OUT, CASH_OUT.replace("pairs_entered", "pairs_active")),
    "invested years taken from the priced pairs": (INVESTED_OUT, INVESTED_OUT.replace('if r["pairs_entered"]', 'if r["pairs_active"]')),
    "average over the entered pairs": (INVESTED_OUT, INVESTED_OUT.replace('[r["pairs_active"]', '[r["pairs_entered"]')),
    "capped legs not totalled": ('"legs_capped": sum(r["legs_capped"] for r in executed),', '"legs_capped": 0,'),
    "entered pairs not written to the results": ('"pairs_entered": r["pairs_entered"],', '"pairs_entered": r["pairs_formed"],'),
    "capped legs not written to the results": ('"legs_capped": r["legs_capped"],', '"legs_capped": 0,'),
    "same-company count not written to the results": ('"same_company_pairs": r["same_company_pairs"],',
                                                      '"same_company_pairs": None,'),
    "pairs not written to the results": ('"pairs": r["pairs"],', '"pairs": [],'),
    "the both run's LTP book not written to the results": (LTP_ROWS, "**{},"),
    "universe counts of the year before": (COUNTS, COUNTS.replace('r["year"]', 'r["year"] - 1')),
    "universe counts not recorded": (COUNTS, "**{},"),
    "universe counts not passed": ("                       per_year=per_year,\n", ""),
    "universe rule not recorded": (RULE, ""),
    "universe rule in place of the leg's name": ('"universe": universe_name,', '"universe": dict(UNIVERSE_NOTES),'),
    "top N recorded from another constant": ('"top_n_per_sector": TOP_N_PER_SECTOR,', '"top_n_per_sector": MAX_PAIRS,'),
    "formation bars recorded from another constant": ('"min_formation_bars": MIN_CORR_DAYS}', '"min_formation_bars": MAX_PAIRS}'),
    "listing rule not recorded": ('"listing_filter": listing_filter,', '"listing_filter": None,'),
    "listing rule not passed": ("listing_filter=campaign_listing_label(exchanges),", "listing_filter=None,"),
    "ISIN dedup not recorded": ('"isin_dedup": True,', '"isin_dedup": False,'),
    "company dedup not recorded": ('"company_dedup": True,', '"company_dedup": False,'),
    "exit block not recorded": ("if ltp_block is not None:", "if False:"),
    "exit block not passed": ("ltp_block=ltp_block)", "ltp_block=None)"),
    "exit method not written beside its block": ('out.update({"exit_method": ltp_block["exit_method"], "ltp": ltp_block})',
                                                 'out.update({"ltp": ltp_block})'),
    "measured years counted over every year": ('"n_years": len(valid),', '"n_years": len(results),'),
    "years counted over the measured ones only": ("**period_accounting(executed, valid, cash_periods,",
                                                  "**period_accounting(valid, valid, cash_periods,"),
    "annual rows of the measured years only": ("            for r in results\n        ],",
                                               "            for r in valid\n        ],"),
    "excess as a sum": ('(r["portfolio_return"] - (r["spy_return"] or 0))', '(r["portfolio_return"] + (r["spy_return"] or 0))'),
    "a year's return to one decimal": ('"portfolio": round(r["portfolio_return"] * 100, 2),',
                                       '"portfolio": round(r["portfolio_return"] * 100, 1),'),
    "average pairs to a whole number": ("round(sum(invested) / len(invested), 1)", "round(sum(invested) / len(invested), 0)"),
    "win rate as the excess CAGR": ('"excess_cagr": pct(c.get("excess_cagr")),\n        "win_rate_vs_spy"',
                                    '"excess_cagr": pct(c.get("win_rate")),\n        "win_rate_vs_spy"'),
    "Sortino as the Sharpe ratio": ('rnd(s.get("sharpe_ratio")),', 'rnd(s.get("sortino_ratio")),'),
    "the portfolio as its own benchmark": ('"spy": fmt(b),', '"spy": fmt(p),'),
    "metrics on a zero risk-free rate": ("compute_metrics(port_returns, spy_returns, 1, risk_free_rate=risk_free_rate)",
                                         "compute_metrics(port_returns, spy_returns, 1, risk_free_rate=0.0)"),
    "results under another name": ("out = build_output(m, annual, valid, results, universe_name,",
                                   'out = build_output(m, annual, valid, results, "X",'),
    "results file not written": ("            json.dump(out, f, indent=2)\n        print(f\"  Results saved",
                                 "            pass\n        print(f\"  Results saved"),
    "benchmark name not passed": ("                       benchmark_name=benchmark_name,\n", ""),
    "benchmark symbol not passed": ("benchmark_symbol=benchmark_symbol,\n                       per_year=per_year,",
                                    "per_year=per_year,"),
    # one market's run
    "exit helper built without the delisting table": (HELPER, HELPER.replace("load_delist_bounds(cr)", "{}")),
    "exit helper built in a drop run": (HELPER, HELPER.replace(' if exit_method != "drop" else None', "")),
    "exit method not passed to the year loop": (TO_LOOP, "ltp=ltp)"),
    "exit helper not passed to the year loop": (TO_LOOP, "exit_method=exit_method)"),
    "LTP block on monthly periods": (BLOCK, BLOCK.replace("valid, 1,", "valid, 12,")),
    "LTP block on a zero risk-free rate": (BLOCK, BLOCK.replace("risk_free_rate", "0.0")),
    "LTP block over years without a benchmark return": (BLOCK, BLOCK.replace("valid,", "results,")),
    "offset not passed to the year loop": (YEAR_LOOP, YEAR_LOOP.replace("\n                           offset_days=offset_days,", "")),
    "cost switch not passed to the year loop": (YEAR_LOOP, YEAR_LOOP.replace("use_costs=use_costs", "use_costs=True")),
    "offset not passed to the fetch": ("verbose=verbose, offset_days=offset_days)", "verbose=verbose)"),
    # the fetch: what the topic asks of the universe
    "entry offset not passed to the universe": (BUILD, BUILD.replace("offset_days", "1")),
    "entry floor not passed to the universe": (BUILD, BUILD.replace("MIN_LEG_PRICE", "0.0")),
    "top 29 per sector": (const("TOP_N_PER_SECTOR"), const("TOP_N_PER_SECTOR", 29)),
    "top 31 per sector": (const("TOP_N_PER_SECTOR"), const("TOP_N_PER_SECTOR", 31)),
    "199 bars and 199 common days enough": (const("MIN_CORR_DAYS"), const("MIN_CORR_DAYS", 199)),
    "200 bars and 200 common days not enough": (const("MIN_CORR_DAYS"), const("MIN_CORR_DAYS", 201)),
    "a leg of 19 candidates runs": (const("MIN_CANDIDATES"), const("MIN_CANDIDATES", 19)),
    "a leg of 20 candidates is skipped": (const("MIN_CANDIDATES"), const("MIN_CANDIDATES", 21)),
    "candidate floor on the thinnest year": ("if max(sizes) < MIN_CANDIDATES:", "if min(sizes) < MIN_CANDIDATES:"),
    "one year's universe for the whole run": ("    years = range(START_YEAR, END_YEAR + 1)", "    years = range(END_YEAR, END_YEAR + 1)"),
    "the S&P 500's bars not fetched": ('bench_symbols = {"SPY"}', "bench_symbols = set()"),
    "the local index's bars not fetched": ("                bench_symbols.add(sym)", "                pass"),
    # the command line: the exit method reaches every market
    "no --exit-method flag": ("    add_exit_method_arg(parser)\n", ""),
    "exit method not passed in the all-market loop": (ALL_MARKETS, "offset_days=offset_days)"),
    "exit method not passed for one market": (ONE_MARKET, "args.verbose, args.output, offset_days=offset_days)"),
    "a market missing from the all-market loop": ('            ("germany",     ["XETRA"]),\n', ""),
    "taiwan run as TAI alone": ('("taiwan",      ["TAI", "TWO"]),', '("taiwan",      ["TAI"]),'),
    "every market on the US risk-free rate": (RFR_ALL, RFR_ALL.replace("preset_exchanges", "None")),
    "--risk-free-rate ignored in the all-market loop": (RFR_ALL, RFR_ALL.replace(", args.risk_free_rate", "")),
    "one market on the US risk-free rate": (RFR_ONE, RFR_ONE.replace("(exchanges", "(None")),
    "--risk-free-rate ignored for one market": (RFR_ONE, RFR_ONE.replace(", args.risk_free_rate", "")),
    "the all-market loop always verbose": ("args.verbose, output_path,", "True, output_path,"),
    "a market's file beside the script, not the output": ('out_dir = os.path.dirname(args.output) or "."', 'out_dir = "."'),
    "a failed market not recorded": (FAILED, "pass"),
    "a failed market stops the loop": (FAILED, "raise"),
}


def main():
    base = fixture_problems()
    if OVERCLAIM in SRC.lower():
        base.append("the topic's source claims survivorship is removed: it is only reduced")
    base = base or failures(SRC)
    if base:
        print("test_pairs_fundamentals: FAIL", base)
        sys.exit(1)
    loose = [name for name, (old, new) in MUTANTS.items() if SRC.count(old) != 1 or old == new]
    if loose:
        print("test_pairs_fundamentals: FAIL (mutant anchors not unique:", loose, ")")
        sys.exit(1)
    survived = [name for name, (old, new) in MUTANTS.items() if not failures(SRC.replace(old, new), first_only=True)]
    if survived:
        print("test_pairs_fundamentals: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_pairs_fundamentals: PASS ({len(PROFILE)} lines, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
