#!/usr/bin/env python3
"""Offline check of pairs-zscore on the per-year pairs universe (no network).

A stand-in client answers pairs_universe's own warehouse SQL from made-up rows, so the topic's
fetch, pair formation, trade simulation and results run end to end on a made-up market.

Formation: a pair joins two candidates of the trading year, same sector, 200 common return days
(199 out), correlation 0.7000 (0.6999 out), never NaN; ranked by correlation then symbols, 200
rows; a day printed twice does not make the returns depend on row order. The half-life filter
keeps 5 to 60 days and the walk down the list stops at 20 pairs.

Formation statistics read the formation year and not a day either side of it.

Trading: a z-score needs 40 common bars, on weekday bars too, and a bar inside the year; entry
at |z| 2.0 and exits at |z| 0.5, 60 held days or a 5% loss, each executed on the next bar; a
round trip costs each leg's tiered rate once, on that year's USD cap; the loss is floored at
-200%. Inside the delisting bound, a position still open at the pair's last common bar closes
there, labelled last_bar or year_end, whatever exit was queued, and pays its cost.

The year: a pair is entered when each leg's entry bar prints 1.00 or more inside the pair's
delisting bound. The entry bar is the leg's first bar from 1 January plus the execution offset
to ten days later: the window's last day counts, the day after it does not, and no later bar is
read: a leg that stops on the window's last day is entered all the same. Fewer than 3 entered
pairs is a cash year that trades nothing and asks the exit helper
nothing, however many pairs formed; in an invested year only the entered pairs trade; the
return is the sum over pairs divided by the pairs formed; two lines of one company stop the
run, entered or not. The results carry each year's universe counts and pairs, the universe
rule, the listing rule, both dedups and the exit block, and keep the keys the charts read. Only
--exit-method ltp runs.

Every mutant of the topic's source must trip a check. The source is loaded without its
__main__ block and with a client that raises, a def that would call something when it is
defined is refused, and no socket connects from this process.

Usage:
    python scripts/test_pairs_zscore.py [repo root]
"""
import ast
import contextlib
import io
import json
import math
import os
import socket
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "pairs-zscore", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

import cr_client  # noqa: E402
import pairs_universe  # noqa: E402
from metrics import compute_metrics  # noqa: E402

pairs_universe.print = lambda *a, **k: None   # its progress lines


def no_network(*a, **k):
    raise RuntimeError("the offline check reached for the network")


class Offline:
    """The client a topic under test imports: a source whose main() runs must not reach the warehouse."""
    built = 0

    def __init__(self, *a, **k):
        Offline.built += 1
        raise RuntimeError("the topic opened a warehouse client inside the offline check")


# Whatever source is loaded, by this file or one that imports it, nothing leaves the machine.
socket.socket.connect = socket.socket.connect_ex = socket.getaddrinfo = no_network
cr_client.CetaResearch = Offline

ORIGIN, END = date(2018, 1, 1), date(2021, 1, 10)   # bars run from a line's first year past 2020
T19, T20 = date(2019, 1, 1), date(2020, 1, 1)
Y18_END, Y19_END, Y20_END = date(2018, 12, 31), date(2019, 12, 31), date(2020, 12, 31)
FY16, FY17, FY18 = date(2016, 12, 31), date(2017, 12, 31), date(2018, 12, 31)
YEARS = (2019, 2020)
HI = 0.05            # a spread far outside its +-0.001 band
ONE = timedelta(days=1)
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


def noise(d):
    return 0.001 if n(d) % 2 else -0.001


def drift(d):
    """The B leg of the simulation fixtures: it moves between any two days."""
    return 100.0 + 0.3 * ((n(d) * 3) % 7) / 7.0


def ev(first, last, value):
    return {d: value for d in span(first, last)}


# --- the made-up warehouse --------------------------------------------------------------------------

PX, PROFILE, METRICS, EOD = {}, [], [], []
FX = [("USDEUR", ep(date(2018, 12, 28)), 0.8), ("USDEUR", ep(date(2019, 12, 30)), 0.9)]
DELISTED = [("P02A", "2020-06-10")]


def bars(symbol, px, volume=10.0):
    PX[symbol] = px
    EOD.extend((symbol, d.isoformat(), ep(d), p, p, volume) for d, p in sorted(px.items()))


def line(symbol, exchange, sector, px, caps=None, country="US", currency="USD", isin=None, live=True):
    """One listed line: its bars, a profile row and its FY filings [(period end, market cap)]."""
    bars(symbol, px)
    PROFILE.append((symbol, exchange, sector, country, currency, isin or "ZZ" + symbol, None,
                    symbol + " Corp", live, False, False, 100))
    METRICS.extend((symbol, "FY", cap, currency, ep(d)) for d, cap in ([(FY18, 9.5e9)] if caps is None else caps))


def formed(fy, amp, events=None, alternate=False, power=None, a_scale=1.0, b_scale=1.0, a_flat=None, b_flat=None,
           a_last=END, b_last=END, a_gap=None, b_gap=None, b_first=None):
    """(A, B) bars of a pair that forms on year fy. That year B is the wave and A the wave times
    exp(spread), spread = amp * sin on a 20-day period (alternate: +-amp, which has no half-life).
    From the next 1 January B is flat and A is B * exp(spread), spread = +-0.001 except on `events`.
    power: A is B ** power in both years and B never goes flat, so the pair trades on that hedge ratio."""
    first, t, a, b = date(fy, 1, 1), date(fy + 1, 1, 1), {}, {}
    year = span(first, t - ONE)
    spread = {d: (amp if j % 2 else -amp) if alternate else amp * math.sin(2 * math.pi * j / 20) for j, d in enumerate(year)}
    if power:   # a spread with no log B left in it: the OLS beta is exactly `power`
        lb = [math.log(wave(d)) for d in year]
        tilt = cov(list(spread.values()), lb) / cov(lb, lb)
        spread = {d: s - tilt * (x - mean(lb)) for (d, s), x in zip(spread.items(), lb)}
    for d in span(first, END):
        if d < t:
            a[d], b[d] = a_scale * wave(d) ** (power or 1.0) * math.exp(spread[d]), b_scale * wave(d)
        elif power:
            a[d], b[d] = wave(d) ** power * math.exp((events or {}).get(d, noise(d))), wave(d)
        else:
            s = (events or {}).get(d, noise(d))
            a[d] = a_scale * 100.0 * math.exp(s) if a_flat is None else a_flat
            b[d] = b_scale * 100.0 if b_flat is None else b_flat
    gone = lambda d, gap: gap is not None and gap[0] <= d <= gap[1]
    a = {d: p for d, p in a.items() if d <= a_last and not gone(d, a_gap)}
    b = {d: p for d, p in b.items() if (b_first or first) <= d <= b_last and not gone(d, b_gap)}
    return a, b


def mean(xs):
    return sum(xs) / len(xs)


def cov(xs, ys):
    mx, my = mean(xs), mean(ys)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys))


def pearson(xs, ys):
    den = math.sqrt(cov(xs, xs) * cov(ys, ys))
    return cov(xs, ys) / den if den else float("nan")


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
        a[d], b[d] = level * math.exp(noise(d)), wave(days[-1])
    return a, b


# The main leg: XETRA, German lines, EUR. Pair k of 2020 has spread amplitude 0.010 + 0.002 k in 2019,
# so k is also its correlation rank. Pairs 0-19 are the 20 held; pair 20 is the 21st.
MAIN, BENCH = ["XETRA"], "^GDAXI"
SCENARIOS = {
    0: dict(events=ev(date(2020, 3, 10), date(2020, 3, 12), HI)),          # a round trip in March
    1: dict(events=ev(date(2020, 4, 10), date(2020, 4, 12), HI)),          # one in April, on dearer legs
    2: dict(events={**ev(date(2020, 6, 10), date(2020, 6, 13), HI),        # A delisted 10 June: its bars
                    **ev(date(2020, 6, 14), date(2020, 7, 10), 0.07)},     # repeat to 10 July
            a_last=date(2020, 7, 10)),
    3: dict(events={**ev(date(2020, 12, 26), date(2020, 12, 28), HI),      # open at year end
                    **ev(date(2020, 12, 29), END, 0.06)}),
    4: dict(a_scale=0.01, a_flat=0.99),                                    # A prints 0.99 in 2020
    5: dict(b_scale=0.01, b_flat=0.99),                                    # B prints 0.99
    6: dict(b_scale=0.01, b_flat=1.0),                                     # B prints exactly 1.00
    7: dict(b_gap=(T20, date(2021, 1, 4))),                                # B has no bar in 2020, back in 2021
    8: dict(a_gap=(T20, date(2021, 1, 4))),                                # nor has A
    9: dict(events={**ev(date(2020, 12, 24), date(2020, 12, 26), HI),      # a second position open at year end
                    **ev(date(2020, 12, 27), END, 0.06)}),
    11: dict(events=ev(date(2020, 3, 20), date(2020, 3, 22), HI), power=1.5),   # a round trip at a hedge ratio of 1.5
}
EUR = dict(country="DE", currency="EUR")
for k in range(21):
    pa, pb = formed(2019, (0.010 + 0.002 * k) * (SCENARIOS.get(k, {}).get("power") or 1.0), **SCENARIOS.get(k, {}))
    line(f"P{k:02d}A", "XETRA", f"S{k:02d}", pa, caps=[(FY18, 1.9e9)] if k == 1 else None, **EUR)
    line(f"P{k:02d}B", "XETRA", f"S{k:02d}", pb, caps=[(FY16, 9.5e9)] if k == 1 else None, **EUR)
line("P10A-PFA", "XETRA", "S10", dict(PX["P10A"]), **EUR)                  # a second line of P10A's company
line("USX", "XETRA", "S09", dict(PX["P09B"]), country="US", currency="EUR")   # a foreign line: listing rule
for name, kw in (("PIV", {}), ("D200", dict(b_first=Y19_END - timedelta(days=200))),
                 ("D199", dict(b_first=Y19_END - timedelta(days=199)))):
    pa, pb = formed(2019, 0.004, alternate=True, **kw)                     # high correlation, no half-life
    line(name + "A", "XETRA", "S" + name, pa, **EUR)
    line(name + "B", "XETRA", "S" + name, pb, **EUR)
for name, rho in (("C70", 0.70004), ("C69", 0.69994)):                     # round to 0.7000 and 0.6999
    pa, pb = correlated(rho, span(T19, Y19_END))
    line(name + "A", "XETRA", "S" + name, pa, **EUR)
    line(name + "B", "XETRA", "S" + name, pb, **EUR)
line("NANA", "XETRA", "SNAN", formed(2019, 0.01)[1], **EUR)
line("NANB", "XETRA", "SNAN", {d: 50.0 for d in span(T19, END)}, **EUR)    # a frozen price: CORR is NaN
# The 2019 pairs form on 2018. In 2019 their B legs are flat, so they form no pair for 2020.
Q_CAPS = [(FY17, 8.5e9), (FY18, 1.0e9)]                                    # 0.1% tier in 2019, 0.5% in 2020
for k, kw in ((1, dict(events=ev(date(2019, 3, 10), date(2019, 3, 12), HI))),
              (2, dict(events=ev(date(2019, 4, 10), date(2019, 4, 12), HI))),
              (3, dict(b_last=date(2019, 6, 30)))):                        # Q3B stopped trading in 2019
    pa, pb = formed(2018, 0.008 + 0.002 * k, **kw)
    line(f"Q{k}A", "XETRA", f"T{k}", pa, caps=Q_CAPS, **EUR)
    line(f"Q{k}B", "XETRA", f"T{k}", pb, caps=Q_CAPS, live=k != 3, **EUR)
bars(BENCH, {d: 10000.0 + 3.0 * n(d) for d in span(ORIGIN, END)})
bars("SPY", {d: 300.0 + 0.1 * n(d) for d in span(ORIGIN, END)})

# A leg of two entered pairs, both with a signal: a cash year.
for k, kw in ((1, dict(events=ev(date(2020, 3, 10), date(2020, 3, 12), HI))),
              (2, dict(events=ev(date(2020, 6, 10), date(2020, 6, 15), HI), a_last=date(2020, 6, 15)))):
    pa, pb = formed(2019, 0.008 + 0.002 * k, **kw)
    line(f"K{k}A", "ZCASH", f"K{k}", pa)
    line(f"K{k}B", "ZCASH", f"K{k}", pb)

# Three legs for the entry bars, the window being 2 to 12 January at next-bar execution. Pair k has spread
# amplitude 0.010 + 0.002 k in 2019, so k is its correlation rank.
MARCH, JUNE = ev(date(2020, 3, 10), date(2020, 3, 12), HI), ev(date(2020, 6, 10), date(2020, 6, 12), HI)
JAN11, JAN12 = date(2020, 1, 11), date(2020, 1, 12)
ENTRY_LEGS = {
    "ZWIN": [("W0", dict(events=MARCH)),                                   # four formed, three entered: one round trip
             ("W1", {}),                                                   # with a bar the day before its formation year
             ("W2", dict(a_gap=(T20, JAN11))),                             # A's first 2020 bar is 12 January
             ("W3", dict(events=JUNE, a_scale=0.0102))],                   # A prints 0.99 on 1 and 2 January; a June signal
    "ZWOUT": [("X0", dict(events=MARCH)),                                  # three formed, two entered: a cash year
              ("X1", {}),
              ("X2", dict(b_gap=(T20, JAN12)))],                           # B's first 2020 bar is 13 January
    "ZSTOP": [("Y0", dict(events=MARCH)),                                  # three formed, three entered: an invested year
              ("Y1", {}),
              ("Y2", dict(a_last=JAN12))],                                 # A's last bar is 12 January, the window's last day
}
for exchange, pairs in ENTRY_LEGS.items():
    for k, (name, kw) in enumerate(pairs):
        pa, pb = formed(2019, 0.010 + 0.002 * k, **kw)
        if name == "W1":
            pa[Y18_END] = pb[Y18_END] = wave(Y18_END)
        if name == "W3":
            pa[T20] = pa[T20 + ONE] = 0.99
        line(name + "A", exchange, name, pa)
        line(name + "B", exchange, name, pb)

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
    bars_i = listed(205, close=100.0 - i)                                  # size falls with i
    line(f"C{i:02d}", "ZFETCH", "Cut31", bars_i)
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


# --- simulation fixtures: bars only, B drifts and A is B * exp(spread) ------------------------------

def unit(name, events, a_last=END, first=date(2019, 11, 1)):
    days = span(first, END)
    b = {d: drift(d) for d in days}
    a = {d: b[d] * math.exp(events.get(d, noise(d))) for d in days if d <= a_last}
    bars(f"U{name}A", a)
    bars(f"U{name}B", b)
    return a, b


def zscore(window):
    m = mean(window)
    return (window[-1] - m) / math.sqrt(sum((x - m) ** 2 for x in window) / (len(window) - 1))


def spread_at(target, day, events):
    """The spread on `day` whose 40-bar z-score is `target`, given the 39 days before it."""
    prior = [events.get(d, noise(d)) for d in span(day - timedelta(days=39), day - ONE)]
    lo, hi = -1.0, 1.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        lo, hi = (mid, hi) if zscore(prior + [mid]) < target else (lo, mid)
    return hi


def at_loss(target, day, entry, events):
    """The spread on `day` at which a short-A position entered on `entry` shows the loss `target`."""
    a_entry = drift(entry) * math.exp(events[entry])
    return math.log(a_entry * (drift(day) / drift(entry) - 2.0 * target) / drift(day))


def jump(factor, day, entry, events):
    """The spread on `day` at which A stands at `factor` times its price on `entry`."""
    return math.log(factor * drift(entry) * math.exp(events[entry]) / drift(day))


MAR10, APR10, JUN1, JUL1, SEP10 = date(2020, 3, 10), date(2020, 4, 10), date(2020, 6, 1), date(2020, 7, 1), date(2020, 9, 10)
unit("1", ev(MAR10, date(2020, 3, 12), HI))                               # rich A: short A, long B
unit("2", ev(APR10, date(2020, 4, 12), -HI))                              # cheap A: long A, short B
E3 = {}
E3[date(2020, 5, 5)] = spread_at(1.98, date(2020, 5, 5), E3)              # under the entry threshold
E3[JUL1] = spread_at(2.02, JUL1, E3)                                      # over it
unit("3", E3)
E4 = ev(SEP10, date(2020, 9, 12), HI)
E4[date(2020, 9, 13)] = spread_at(0.52, date(2020, 9, 13), E4)            # over the exit threshold
E4[date(2020, 9, 14)] = spread_at(0.48, date(2020, 9, 14), E4)            # under it
unit("4", E4)
MAR1 = date(2020, 3, 1)
unit("5", {d: HI + 0.001 * (d - MAR1).days for d in span(MAR1, END)})     # never converges, never loses 5%
E6 = ev(JUN1, JUN1 + ONE, HI)
E6[JUN1 + 2 * ONE] = at_loss(-0.0499, JUN1 + 2 * ONE, JUN1 + ONE, E6)     # a loss of 4.99%
E6[JUN1 + 3 * ONE] = at_loss(-0.0501, JUN1 + 3 * ONE, JUN1 + ONE, E6)     # then 5.01%
E6[JUN1 + 4 * ONE] = E6[JUN1 + 3 * ONE]
unit("6", E6)
for name, factor, stops in (("7a", 8.0, 3), ("7b", 4.9, 3), ("7c", 8.0, 2)):   # A jumps on the second held day
    e7 = ev(JUL1, JUL1 + ONE, HI)
    e7.update(ev(JUL1 + 2 * ONE, END, jump(factor, JUL1 + 2 * ONE, JUL1 + ONE, e7)))
    for d in span(JUL1 + 3 * ONE, END):
        e7[d] = e7[JUL1 + 2 * ONE] + math.log(drift(JUL1 + 2 * ONE) / drift(d))   # A holds its new price
    unit(name, e7, a_last=JUL1 + stops * ONE)
YEAR = span(date(2020, 1, 2), END)
unit("9a", {YEAR[38]: HI}, first=YEAR[0])                                 # a spike on the 39th common bar
unit("9b", {YEAR[i]: HI for i in (39, 40, 41)}, first=YEAR[0])            # on the 40th
unit("10a", {Y19_END: HI})                                                # a signal on the last bar before the year
unit("10b", {T20: HI})                                                    # on the first bar inside it
unit("11a", ev(date(2020, 6, 10), date(2020, 6, 15), HI), a_last=date(2020, 6, 15))
unit("11b", ev(date(2020, 12, 26), END, HI))
unit("11c", ev(date(2020, 6, 12), date(2020, 7, 10), HI), a_last=date(2020, 7, 10))
AUG10 = date(2020, 8, 10)
unit("11d", ev(AUG10, AUG10 + 2 * ONE, HI), a_last=AUG10 + 3 * ONE)        # converged on A's last bar: the exit never comes
WEEK = [d for d in span(date(2019, 10, 1), date(2020, 3, 31)) if d.weekday() < 5]
bars("U12A", {d: drift(d) * math.exp(HI if d == T20 else noise(d)) for d in WEEK})   # weekday bars, a signal on the year's first
bars("U12B", {d: drift(d) for d in WEEK})
UNIT_SYMBOLS = sorted(s for s in PX if s.startswith("U"))


def half_life(a, b, first=T19, last=Y19_END):
    """The topic's estimate, redone here: OLS beta on log prices, AR(1) on the spread."""
    days = [d for d in span(first, last) if d in a and d in b]
    la, lb = [math.log(a[d]) for d in days], [math.log(b[d]) for d in days]
    beta = cov(la, lb) / cov(lb, lb)
    s = [x - beta * y for x, y in zip(la, lb)]
    phi = cov([y - x for x, y in zip(s, s[1:])], s[:-1]) / cov(s[:-1], s[:-1])
    return -math.log(2) / math.log(1 + phi) if -1 < phi < 0 else None


def tuned(target):
    """(A, B) 2019 bars whose spread has a half-life of `target` days: a sine of the right period."""
    days = span(T19, Y19_END)

    def build(omega):
        return {d: wave(d) * math.exp(0.02 * math.sin(omega * i)) for i, d in enumerate(days)}, {d: wave(d) for d in days}

    centre = math.acos(2.0 ** (-1.0 / target))
    lo, hi = 0.9 * centre, 1.1 * centre           # a faster sine reverts sooner
    for _ in range(60):
        mid = (lo + hi) / 2.0
        lo, hi = (mid, hi) if half_life(*build(mid)) > target else (lo, mid)
    return build(hi)


HALF_LIVES = {"H1": 5.5, "H2": 4.5, "H3": 59.5, "H4": 60.5}                # inside 5..60: H1 and H3
for name, target in HALF_LIVES.items():
    pa, pb = tuned(target)
    for d in (Y18_END, T20):                                               # a bar either side of the formation year
        pa[d], pb[d] = wave(d) * math.exp(0.05), wave(d)
    bars(name + "A", pa)
    bars(name + "B", pb)
UNIT_SYMBOLS += sorted(s for s in PX if s[0] == "H")


def january(first, prices=None):
    """A leg at 5.00 every day from December 2019 through 2020, without 1 January 2020 to the day before `first`."""
    px = {d: 5.0 for d in span(date(2019, 12, 1), Y20_END) if d < T20 or d >= first}
    px.update(prices or {})
    return px


# Bars for entered_pairs alone. The entry window is 2 to 12 January at next-bar execution, 1 to 11 at same-bar.
ENTRY_PX = {
    "OKA": january(T20), "OKB": january(T20),
    "D11": january(JAN11), "D12": january(JAN12), "D13": january(JAN12 + ONE),
    "LOW": january(T20, {T20 + ONE: 0.99}),                                # under the floor on 2 January only
    "ONE": january(T20, {T20 + ONE: 1.0}),                                 # exactly on it
    "DIP": january(T20, {T20: 0.99}),                                      # under it on 1 January only
    "LATE": january(T20, {d: 0.5 for d in span(T20 + 2 * ONE, Y20_END)}),  # 5.00 to 2 January, 0.50 after
}
D24, D25, D26 = date(2019, 12, 24), date(2019, 12, 25), date(2019, 12, 26)   # delisted then: bars count to 31 Dec, 1 Jan, 2 Jan
ENTRY_CASES = (   # (execution offset, {leg: delisted} or None for no exit helper, [(A, B, entered)])
    (1, None, [("OKA", "OKB", True),
               ("D12", "OKB", True), ("OKA", "D12", True),                 # first bar on the window's last day
               ("D13", "OKB", False), ("OKA", "D13", False),               # on the day after it
               ("LOW", "OKB", False), ("OKA", "LOW", False),               # 0.99 on the entry bar, 5.00 around it
               ("ONE", "OKB", True), ("OKA", "ONE", True),
               ("DIP", "OKB", True), ("OKA", "DIP", True),                 # 0.99 the day before the window
               ("LATE", "OKB", True), ("OKA", "LATE", True)]),             # only the entry bar is tested
    (0, None, [("OKA", "OKB", True), ("D11", "OKB", True), ("OKA", "D11", True), ("D12", "OKB", False),
               ("OKA", "D12", False), ("LOW", "OKB", True), ("DIP", "OKB", False), ("OKA", "DIP", False)]),
    (1, {}, [("OKA", "OKB", True), ("D13", "OKB", False)]),
    (1, {"OKA": D26}, [("OKA", "OKB", True), ("OKA", "D12", False)]),      # B's entry bar is past A's bound
    (1, {"OKB": D26}, [("OKA", "OKB", True), ("D12", "OKB", False)]),
    (1, {"OKA": D25}, [("OKA", "OKB", False)]),
    (1, {"OKB": D25}, [("OKA", "OKB", False)]),
    (0, {"OKA": D25}, [("OKA", "OKB", True)]),
    (0, {"OKA": D24}, [("OKA", "OKB", False)]),
    (1, {"OKA": date(2020, 1, 5)}, [("OKA", "D12", True)]),                # the bound is 12 January
    (1, {"OKA": date(2020, 1, 4)}, [("OKA", "D12", False)]),
    (1, {"OKA": date(2019, 12, 1)}, [("OKA", "OKB", True)]),               # 31 days before the year: an earlier holder's
    (1, {"OKA": date(2019, 12, 2)}, [("OKA", "OKB", False)]),              # 30 days: it bounds
)


def entry_con():
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)",
                    [(s, d, ep(d), p) for s, px in ENTRY_PX.items() for d, p in px.items()])
    return con


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


def leg(exchanges, bench="SPY"):
    """(con, per-year counts) of a leg, built by the module with the topic's published parameters."""
    con = duckdb.connect()
    pairs_universe.fetch_universe(WH, con, exchanges, YEARS)
    counts = pairs_universe.build_candidates(con, YEARS, 1, 1.0, 30, 200)
    pairs_universe.fetch_prices(WH, con, None, [bench])
    return con, counts


def unit_con():
    con = duckdb.connect()
    pairs_universe.fetch_prices(WH, con, UNIT_SYMBOLS, [])
    return con


def load(src):
    """The topic's module from `src`, without its top-level `if` (the __main__ block): main() cannot run here.
    A def that would call something when it is defined (a decorator, a default, an annotation) is refused."""
    tree = ast.parse(src, PATH)
    tree.body = [node for node in tree.body if not isinstance(node, ast.If)]
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            header = [node.args] + ([node.returns] if node.returns else [])
            if node.decorator_list or any(isinstance(n, ast.Call) for part in header for n in ast.walk(part)):
                raise RuntimeError(f"line {node.lineno} of the topic runs code when def {node.name} is defined")
    mod = types.ModuleType("pairs_zscore_under_test")
    mod.__file__ = PATH
    exec(compile(tree, PATH, "exec"), mod.__dict__)
    mod.print = lambda *a, **k: None
    mod.PUBLISHED_YEARS = (mod.START_YEAR, mod.END_YEAR)
    mod.START_YEAR, mod.END_YEAR = YEARS
    return mod


def guard_problems():
    """The guards work: no socket call gets out, a source whose __main__ test is true loads without
    running, a def that would call main() when it is defined is refused, and main() stops at the
    stand-in client. Each step runs only if the one before held."""
    for what, call in (("create_connection", lambda s: socket.create_connection(("127.0.0.1", 9), 1)),
                       ("connect", lambda s: s.connect(("127.0.0.1", 9))),
                       ("connect_ex", lambda s: s.connect_ex(("127.0.0.1", 9))),
                       ("getaddrinfo", lambda s: socket.getaddrinfo("localhost", 80))):
        with socket.socket() as sock:
            try:
                call(sock)
                return [f"socket {what} went through"]
            except RuntimeError:
                pass
            except OSError as e:
                return [f"socket {what} reached the network: {e}"]
    main_if, argv, built = 'if __name__ == "__main__":', sys.argv, Offline.built
    if SRC.count(main_if) != 1 or cr_client.CetaResearch is not Offline:
        return ["the source has no __main__ test to make true, or the client is not the stand-in"]
    sys.argv = ["backtest.py", "--preset", "us"]   # a main() that ran would ask for a client
    try:
        try:
            mod = load(SRC.replace(main_if, "if True:"))
        except RuntimeError:
            mod = None
        if mod is None or Offline.built != built or mod.CetaResearch is not Offline:
            return ["a source whose __main__ test is true ran its main() on loading, or did not import the stand-in"]
        for what, late in (("decorator", "@(lambda f: main())\ndef late():\n    pass\n"),
                           ("default", "def late(x=main()):\n    pass\n"),
                           ("annotation", "def late(x: main() = 1):\n    pass\n"),
                           ("return annotation", "def late() -> main():\n    pass\n")):
            try:
                load(SRC + "\n" + late)
                return [f"a def whose {what} calls main() was loaded"]
            except RuntimeError:   # refused before anything ran: a main() that ran would have asked for a client
                if Offline.built != built:
                    return [f"a def whose {what} calls main() ran it on loading"]
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                mod.main()
            return ["main() ran to its end"]
        except RuntimeError:
            return [] if Offline.built == built + 1 else ["main() failed before it asked for a client"]
    finally:
        sys.argv = argv


def near(x, y, tol=1.5e-6):
    return x is not None and y is not None and abs(x - y) <= tol


def book(con, year):
    out = {}
    for sector, symbol in con.execute("SELECT sector, symbol FROM candidates WHERE yr = ? ORDER BY 1, 2", [year]).fetchall():
        out.setdefault(sector, []).append(symbol)
    return out


# --- what the made-up market must produce -----------------------------------------------------------

def want(a, b, entry, exit_, kind, held, direction, cost):
    """A trade as the topic records it, its return worked out from the fixture's own bars."""
    r = direction * ((PX[a][exit_] / PX[a][entry] - 1.0) - (PX[b][exit_] / PX[b][entry] - 1.0)) / 2.0 - cost
    return (entry.isoformat(), exit_.isoformat(), kind, held, max(r, -2.0))


def same(got, wanted):
    """Trades match: dates, exit type and held days exactly, the return to 1.5e-6."""
    return (len(got) == len(wanted)
            and all(g[:4] == w[:4] and near(g[4], w[4]) for g, w in zip(got, wanted)))


def traded(mod, con, name, caps=(2e10, 5e9), **kw):
    out = mod.simulate_pair_trades(con, f"U{name}A", f"U{name}B", 1.0, T20, Y20_END, *caps, **kw)
    return [(t["entry_date"], t["exit_date"], t["exit_type"], t["holding_days"], t["pair_return"]) for t in out or []]


def unit_wanted():
    """{fixture: its trades at next-bar execution with a 0.1% and a 0.3% leg}."""
    u = lambda name, *a: want(f"U{name}A", f"U{name}B", *a, 0.004)
    d = lambda month, day: date(2020, month, day)
    return {
        "1": [u("1", d(3, 11), d(3, 14), "convergence", 2, -1)],
        "2": [u("2", d(4, 11), d(4, 14), "convergence", 2, 1)],
        "3": [u("3", d(7, 2), d(9, 1), "time_stop", 60, -1)],              # 1.98 enters nothing, 2.02 does
        "4": [u("4", d(9, 11), d(9, 15), "convergence", 3, -1)],           # 0.52 holds, 0.48 exits
        "5": [u("5", d(3, 2), d(5, 2), "time_stop", 60, -1)],              # flagged on the 60th held day
        "6": [u("6", d(6, 2), d(6, 5), "loss_stop", 2, -1),                # 4.99% holds, 5.01% stops
              u("6", d(6, 6), d(6, 8), "convergence", 1, -1)],
        "7a": [u("7a", d(7, 2), d(7, 4), "loss_stop", 1, -1)],             # floored at -200%
        "7b": [u("7b", d(7, 2), d(7, 4), "loss_stop", 1, -1)],             # -195%: not floored
        "9a": [],
        "9b": [u("9b", YEAR[40], YEAR[43], "convergence", 2, -1)],
        "10a": [],
        "10b": [u("10b", d(1, 2), d(1, 4), "convergence", 1, -1)],
        "12": [u("12", d(1, 2), d(1, 6), "convergence", 1, -1)],           # 39 weekday bars before the year fill the window
    }


def unit_checks(mod):
    """simulate_pair_trades on the simulation fixtures."""
    con = cached("unit", unit_con)
    wanted = unit_wanted()
    for name, trades in wanted.items():
        got = traded(mod, con, name)
        if not same(got, trades):
            yield f"simulate U{name}: {got} != {trades}"
    if not (near(wanted["7a"][0][4], -2.0) and -2.0 < wanted["7b"][0][4] < -1.9):
        yield f"fixture: the floor cases are not either side of -200%: {wanted['7a']} {wanted['7b']}"
    # costs: each leg's tiered rate once; None is the smallest-cap tier
    gross = want("U1A", "U1B", date(2020, 3, 11), date(2020, 3, 14), "convergence", 2, -1, 0.0)
    for caps, kw, cost in (((2e10, 2e10), {}, 0.002), ((None, None), {}, 0.010), ((5e9, None), {}, 0.008),
                           ((2e10, 5e9), {"use_costs": False}, 0.0)):
        got = traded(mod, con, "1", caps=caps, **kw)
        if not same(got, [gross[:4] + (gross[4] - cost,)]):
            yield f"simulate U1 with caps {caps} {kw}: {got}, cost should be {cost}"
    # same-bar execution (--no-next-day): entry and exit on their signal bars, cost and floor as above
    d = lambda month, day: date(2020, month, day)
    for name, trades in (("1", [want("U1A", "U1B", d(3, 10), d(3, 13), "convergence", 3, -1, 0.004)]),
                         ("7c", [want("U7cA", "U7cB", d(7, 1), d(7, 3), "loss_stop", 2, -1, 0.004)])):
        got = traded(mod, con, name, offset_days=0)
        if not same(got, trades):
            yield f"simulate U{name} at offset 0: {got} != {trades}"
    # the forced close: the pair's last common bar inside the delisting bound, labelled by the helper
    closes = (
        ("11a", {}, [want("U11aA", "U11aB", d(6, 11), d(6, 15), "last_bar", 4, -1, 0.004)],
         [{"pair": ["U11aA", "U11aB"], "year": 2020, "entry_date": "2020-06-11", "last_common_bar": "2020-06-15",
           "delisted_date": [None, None], "delisting_bound": ["none", "none"]}]),
        ("11b", {}, [want("U11bA", "U11bB", d(12, 27), d(12, 31), "year_end", 4, -1, 0.004)], []),
        ("11c", {"U11cA": d(6, 10)}, [want("U11cA", "U11cB", d(6, 13), d(6, 17), "last_bar", 4, -1, 0.004)],
         [{"pair": ["U11cA", "U11cB"], "year": 2020, "entry_date": "2020-06-13", "last_common_bar": "2020-06-17",
           "delisted_date": ["2020-06-10", None], "delisting_bound": ["applied", "none"]}]),
        # a stop queued on the last bar never executes: the close is a last_bar close all the same
        ("7c", {}, [want("U7cA", "U7cB", d(7, 2), d(7, 3), "last_bar", 1, -1, 0.004)],
         [{"pair": ["U7cA", "U7cB"], "year": 2020, "entry_date": "2020-07-02", "last_common_bar": "2020-07-03",
           "delisted_date": [None, None], "delisting_bound": ["none", "none"]}]),
        # so does a convergence: this close is not floored, so its cost shows
        ("11d", {}, [want("U11dA", "U11dB", d(8, 11), d(8, 13), "last_bar", 2, -1, 0.004)],
         [{"pair": ["U11dA", "U11dB"], "year": 2020, "entry_date": "2020-08-11", "last_common_bar": "2020-08-13",
           "delisted_date": [None, None], "delisting_bound": ["none", "none"]}]),
    )
    for name, bounds, trades, fills in closes:
        ltp = pairs_universe.PairLtp(None, bounds)
        got = traded(mod, con, name, ltp=ltp)
        if not same(got, trades) or ltp.fills != fills:
            yield f"forced close U{name}: {got} != {trades}, recorded {ltp.fills}"
    # without the helper: no bound, the year-end label, a queued exit keeps its own
    for name, trades in (("11a", [want("U11aA", "U11aB", d(6, 11), d(6, 15), "year_end", 4, -1, 0.004)]),
                         ("11b", [want("U11bA", "U11bB", d(12, 27), d(12, 31), "year_end", 4, -1, 0.004)]),
                         ("7c", [want("U7cA", "U7cB", d(7, 2), d(7, 3), "loss_stop", 1, -1, 0.004)]),
                         ("11d", [want("U11dA", "U11dB", d(8, 11), d(8, 13), "convergence", 2, -1, 0.004)])):
        got = traded(mod, con, name)
        if not same(got, trades):
            yield f"forced close U{name} without the helper: {got} != {trades}"
    # the half-life window, 5 to 60 days
    for name, target in HALF_LIVES.items():
        got = mod.estimate_spread_params(con, name + "A", name + "B", T19, Y19_END)
        if (got is not None) != (5 <= target <= 60) or (got and not near(got[3], target, 1e-6)):
            yield f"half-life {target} days: {got}"


def corr_row(a, b, sector, first, last):
    """A pair's row of the correlation list, from the fixture's own bars."""
    rets = []
    for px in (PX[a], PX[b]):
        days = sorted(d for d in px if first <= d <= last)
        rets.append({d: px[d] / px[p] - 1.0 for p, d in zip(days, days[1:])})
    common = sorted(set(rets[0]) & set(rets[1]))
    return (a, b, sector, pearson([rets[0][d] for d in common], [rets[1][d] for d in common]), len(common))


PAIRS_2020 = [(f"P{k:02d}A", f"P{k:02d}B", f"S{k:02d}") for k in range(21)]
ODD_2020 = [("PIVA", "PIVB", "SPIV"), ("D200A", "D200B", "SD200"), ("C70A", "C70B", "SC70")]
OUT_2020 = [("D199A", "D199B", "SD199"), ("C69A", "C69B", "SC69"), ("NANA", "NANB", "SNAN")]
PAIRS_2019 = [(f"Q{k}A", f"Q{k}B", f"T{k}") for k in (1, 2, 3)]


def ranked(pairs, first, last):
    return sorted((corr_row(*p, first, last) for p in pairs), key=lambda r: (-round(r[3], 4), r[0], r[1]))


def rows_match(got, wanted):
    return (len(got) == len(wanted)
            and all(g[:3] == w[:3] and near(g[3], round(w[3], 4), 1e-9) and g[4] == w[4] for g, w in zip(got, wanted)))


def fixture_problems():
    """The made-up market is what the checks below assume, by this file's own arithmetic."""
    out = []
    rows = {r[0]: r for r in (corr_row(*p, T19, Y19_END) for p in PAIRS_2020 + ODD_2020 + OUT_2020)}
    if [round(rows[a][3], 4) for a in ("C70A", "C69A")] != [0.7, 0.6999]:
        out.append(f"the correlation boundary pairs: {rows['C70A'][3]} {rows['C69A'][3]}")
    if (rows["D200A"][4], rows["D199A"][4]) != (200, 199) or not math.isnan(rows["NANA"][3]):
        out.append(f"the common-day pairs or the frozen leg: {rows['D200A']} {rows['D199A']} {rows['NANA']}")
    order = [r[0] for r in ranked(PAIRS_2020 + ODD_2020, T19, Y19_END)]
    if [a for a in order if a[0] == "P" and a[1] != "I"] != [p[0] for p in PAIRS_2020]:
        out.append(f"pair k is not the k-th by correlation: {order}")
    if not order.index("PIVA") < order.index("P19A") or order[-1] != "C70A":
        out.append(f"the pair without a half-life is not inside the first 20 rows: {order}")
    corrs = [round(r[3], 4) for r in ranked(PAIRS_2020 + ODD_2020, T19, Y19_END) + ranked(PAIRS_2019, ORIGIN, Y18_END)]
    if any(abs((c * 1e4) % 1 - 0.5) < 1e-3 for r in rows.values() for c in [r[3]] if not math.isnan(c)):
        out.append("a correlation sits on a rounding midpoint")
    if len(set(corrs[:24])) != 24 or len(set(corrs[24:])) != 3:
        out.append(f"two pairs share a rounded correlation: {corrs}")
    for a, b, _ in PAIRS_2020 + PAIRS_2019:
        fy = 2018 if a[0] == "Q" else 2019
        hl = half_life(PX[a], PX[b], date(fy, 1, 1), date(fy, 12, 31))
        if hl is None or not 5 < hl < 60:
            out.append(f"{a}/{b} has no usable half-life: {hl}")
    for a, b, _ in ODD_2020[:2]:
        if half_life(PX[a], PX[b]) is not None:
            out.append(f"{a}/{b} has a half-life")
    for name, target in HALF_LIVES.items():
        if not near(half_life(PX[name + "A"], PX[name + "B"]), target, 1e-9):
            out.append(f"fixture {name} is not tuned to {target} days")
        wider = [half_life(PX[name + "A"], PX[name + "B"], first, last) for first, last in ((Y18_END, Y19_END), (T19, T20))]
        if any(h is not None and abs(h - target) < 1e-3 for h in wider):
            out.append(f"fixture {name}: a bar either side of the formation year does not move its half-life: {wider}")
    for exchange, pairs in ENTRY_LEGS.items():
        names = [(p + "A", p + "B", p) for p, _ in pairs]
        rows, hl = ranked(names, T19, Y19_END), [half_life(PX[a], PX[b]) for a, b, _ in names]
        if ([r[0] for r in rows] != [a for a, _, _ in names] or len({round(r[3], 4) for r in rows}) != len(rows)
                or rows[-1][3] < 0.71 or any(h is None or not 5 < h < 60 for h in hl)):
            out.append(f"{exchange}: pair k is not the k-th by correlation, or has no usable half-life: {rows} {hl}")
    first = lambda s: min(d for d in PX[s] if d >= T20)
    w3 = PX["W3A"]
    if ((first("W2A"), first("X2B"), w3[T20], w3[T20 + ONE]) != (JAN12, JAN12 + ONE, 0.99, 0.99)
            or min(p for d, p in w3.items() if d > T20 + ONE) < 1.0 or Y18_END not in PX["W1A"] or Y18_END not in PX["W1B"]):
        out.append(f"the entry legs' bars: {first('W2A')} {first('X2B')} {w3[T20]} {w3[T20 + ONE]}")
    if (first("Y2A"), max(PX["Y2A"]), max(PX["Y2B"])) != (T20, JAN12, END):
        out.append(f"Y2A does not print from 1 January to the window's last day and no further: {max(PX['Y2A'])}")
    if (pairs_universe.ENTRY_WINDOW_DAYS, pairs_universe.LTP_DELIST_GRACE_DAYS, pairs_universe.LTP_EARLIER_HOLDER_DAYS) != (10, 7, 30):
        out.append("the entry cases assume a 10-day window, 7 days of delisting grace and 30 for an earlier holder")
    before = lambda days: sum(1 for d in WEEK if T20 - timedelta(days=days) <= d < T20)
    if not before(40) < 39 <= before(80):   # 39 bars before the year fill the window on its first bar
        out.append(f"weekday bars before the year: {before(40)} in 40 calendar days, {before(80)} in 80")
    for day, target in ((date(2020, 5, 5), 1.98), (JUL1, 2.02)):
        if not near(zscore([E3.get(d, noise(d)) for d in span(day - timedelta(days=39), day)]), target, 1e-9):
            out.append(f"the entry-threshold spread of {day} is not at z {target}")
    for day, target in ((date(2020, 9, 13), 0.52), (date(2020, 9, 14), 0.48)):
        if not near(zscore([E4.get(d, noise(d)) for d in span(day - timedelta(days=39), day)]), target, 1e-9):
            out.append(f"the exit-threshold spread of {day} is not at z {target}")
    return out


def dup_con(order):
    """Two 2020 candidates; DA prints 1 July 2019 twice, the second print 50% higher."""
    con = duckdb.connect()
    con.execute("CREATE TABLE candidates(yr INTEGER, symbol VARCHAR, sector VARCHAR, company VARCHAR,"
                " size_usd DOUBLE, cap_usd DOUBLE)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE)")
    con.execute("INSERT INTO candidates VALUES (2020, 'DA', 'Dup', 'DA', 1.0, NULL), (2020, 'DB', 'Dup', 'DB', 1.0, NULL)")
    rows = [(s, d, ep(d), PX[p][d]) for s, p in (("DA", "P05A"), ("DB", "P05B")) for d in span(T19, Y19_END)]
    rows.append(("DA", DUP_DAY, ep(DUP_DAY), 1.5 * PX["P05A"][DUP_DAY]))
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?)", rows[::order])
    return con


DUP_DAY = date(2019, 7, 1)


def dup_corr(high_first):
    """DA/DB correlation and common days when the day's two prints are taken high first, or low first."""
    a, b, days = PX["P05A"], PX["P05B"], span(T19, Y19_END)
    two = [1.5 * a[DUP_DAY], a[DUP_DAY]][::1 if high_first else -1]
    seq = [(d, a[d]) for d in days if d < DUP_DAY] + [(DUP_DAY, p) for p in two] + [(d, a[d]) for d in days if d > DUP_DAY]
    ra = [(d, p / q - 1.0) for (_, q), (d, p) in zip(seq, seq[1:])]
    rb = {d: b[d] / b[d - ONE] - 1.0 for d in days[1:]}
    return pearson([x for _, x in ra], [rb[d] for d, _ in ra]), len(ra)


def formation_checks(mod):
    """compute_pair_candidates on the main leg, the 21-line leg and a day printed twice."""
    con = cached("main", lambda: leg(MAIN, BENCH))[0]
    for year, pairs in ((2020, PAIRS_2020 + ODD_2020), (2019, PAIRS_2019)):
        first, last = date(year - 1, 1, 1), date(year - 1, 12, 31)
        got, wanted = mod.compute_pair_candidates(con, first, last), ranked(pairs, first, last)
        if not rows_match(got, wanted):
            yield f"{year} correlation list: {[g[:2] + g[3:] for g in got]} != {[w[:2] + w[3:] for w in wanted]}"
    lim = cached("lim", lambda: leg(["ZLIM"]))[0]
    names = [f"L{i:02d}" for i in range(1, 22)]
    wanted = [(a, b, "L", 1.0, 364) for i, a in enumerate(names) for b in names[i + 1:]][:200]
    got = mod.compute_pair_candidates(lim, T19, Y19_END)
    if not rows_match(got, wanted):
        yield f"210 tied pairs: {len(got)} rows, first {got[:2]}, last {got[-2:]}"
    got = mod.compute_pair_candidates(cached("ZWIN", lambda: leg(["ZWIN"]))[0], T19, Y19_END)
    if not rows_match(got, ranked([(p + "A", p + "B", p) for p, _ in ENTRY_LEGS["ZWIN"]], T19, Y19_END)):
        yield f"a pair with a bar the day before its formation year: {[g[:2] + g[3:] for g in got]}"
    high, low = dup_corr(True), dup_corr(False)
    if round(high[0], 4) == round(low[0], 4) or high[0] < 0.75:
        yield f"fixture: the twice-printed day does not move the correlation: {high} {low}"
    for order in (1, -1):
        got = mod.compute_pair_candidates(dup_con(order), T19, Y19_END)
        if not rows_match(got, [("DA", "DB", "Dup") + high]):
            yield f"a day printed twice, rows {'as' if order > 0 else 'against'} inserted: {got} != {high}"


def year_counts(records):
    return [(r["year"], r["portfolio_return"], r["pairs_formed"], r["pairs_entered"], r["pairs_with_trades"],
             r["total_trades"]) for r in records]


def one_company(mod, con, a, b):
    """Problems unless run_backtest stops when lines a and b are one company."""
    con.execute("UPDATE candidates SET company = ? WHERE symbol = ?", [a, b])
    try:
        mod.run_backtest(con, benchmark_symbol="SPY", ltp=pairs_universe.PairLtp(con, {}))
        return [f"{a} and {b}, two lines of one company in a formed pair, did not stop the run"]
    except AssertionError:
        return []
    finally:
        con.execute("UPDATE candidates SET company = ? WHERE symbol = ?", [b, b])


def cash_checks(mod):
    """run_backtest on a leg of two entered pairs: a cash year. Then the same-company guard, on each pair."""
    con = cached("cash", lambda: leg(["ZCASH"]))[0]
    ltp = pairs_universe.PairLtp(con, {})
    records, stats = mod.run_backtest(con, benchmark_symbol="SPY", ltp=ltp)
    got = year_counts(records)
    if got != [(2019, 0.0, 0, 0, 0, 0), (2020, 0.0, 2, 2, 0, 0)] or stats or ltp.fills:
        yield f"two entered pairs are a cash year that trades nothing: {got}, stats {stats}, closes {ltp.fills}"
    yield from one_company(mod, con, "K1A", "K1B")
    yield from one_company(mod, con, "K2A", "K2B")


def entry_checks(mod):
    """entered_pairs on hand-built bars, then the year's cash decision and its results on the three entry legs."""
    con = cached("entry", entry_con)
    for offset, bounds, cases in ENTRY_CASES:
        pairs = [(a, b, "S", 0.9) for a, b, _ in cases]
        wanted = [p for p, case in zip(pairs, cases) if case[2]]
        got = mod.entered_pairs(con, pairs, T20, Y20_END, offset, None if bounds is None else pairs_universe.PairLtp(con, bounds))
        if got != wanted:
            yield f"entered at offset {offset}, delisted {bounds}: {[p[:2] for p in got]} != {[p[:2] for p in wanted]}"
        if (offset, bounds) == (1, None) and mod.entered_pairs(con, pairs, T20, Y20_END) != wanted:
            yield "entered_pairs without an offset or an exit helper is not next-bar execution with no bound"
    # ZWIN: W2's A leg is back on 12 January, the last day of the next-bar window and a day past the same-bar one
    win, idle = cached("ZWIN", lambda: leg(["ZWIN"]))[0], (2019, 0.0, 0, 0, 0, 0)
    march = want("W0A", "W0B", date(2020, 3, 11), date(2020, 3, 14), "convergence", 2, -1, 0.006)[4]   # two 0.3% legs
    for what, offset, bounds, year in (("three of four pairs entered", 1, {}, (march / 4, 4, 3, 1, 1)),
                                       ("same-bar execution", 0, {}, (0.0, 4, 2, 0, 0)),
                                       ("W1A's bars count to 1 January", 1, {"W1A": D25}, (0.0, 4, 2, 0, 0)),
                                       ("W1A's bars count to 2 January", 1, {"W1A": D26}, (march / 4, 4, 3, 1, 1))):
        ltp = pairs_universe.PairLtp(win, bounds)
        records, stats = mod.run_backtest(win, benchmark_symbol="SPY", offset_days=offset, ltp=ltp)
        got = year_counts(records)
        if (got[:-1] != [idle] or got[-1][0] != 2020 or not near(got[-1][1], year[0]) or got[-1][2:] != year[1:]
                or stats.get("total_trades", 0) != year[4] or ltp.fills):
            yield f"ZWIN, {what}: {got}, stats {stats}, closes {ltp.fills}; 2020 should be {year}"
    yield from one_company(mod, win, "W3A", "W3B")   # formed, not entered
    # ZSTOP: Y2's A leg prints its last bar on 12 January, the window's last day. The pair is entered all the
    # same, so the year is invested and Y0's March trade counts: no bar after the window is asked of a pair
    stop = cached("ZSTOP", lambda: leg(["ZSTOP"]))[0]
    trip = want("Y0A", "Y0B", date(2020, 3, 11), date(2020, 3, 14), "convergence", 2, -1, 0.006)[4]   # two 0.3% legs
    ltp = pairs_universe.PairLtp(stop, {})
    records, stats = mod.run_backtest(stop, benchmark_symbol="SPY", ltp=ltp)
    got = year_counts(records)
    if (got[:-1] != [idle] or got[-1][0] != 2020 or not near(got[-1][1], trip / 3) or got[-1][2:] != (3, 3, 1, 1)
            or stats.get("total_trades", 0) != 1 or ltp.fills):
        yield f"ZSTOP, a leg whose last bar is the window's last day: {got}, stats {stats}, closes {ltp.fills}"
    # ZWOUT: X2's B leg is back on 13 January, so two of three pairs are entered and X0's March signal trades nothing
    out_con = cached("ZWOUT", lambda: leg(["ZWOUT"]))[0]
    for offset in (1, 0):
        ltp = pairs_universe.PairLtp(out_con, {})
        records, stats = mod.run_backtest(out_con, benchmark_symbol="SPY", offset_days=offset, ltp=ltp)
        if year_counts(records) != [idle, (2020, 0.0, 3, 2, 0, 0)] or stats or ltp.fills:
            yield f"ZWOUT at offset {offset}: {year_counts(records)}, stats {stats}, closes {ltp.fills}"
    # the same leg through run_single at same-bar execution, with a second exchange for the 20 candidates
    seen, fetch, run = {}, mod.fetch_data_via_api, mod.run_backtest
    mod.fetch_data_via_api = lambda *a, **kw: seen.update(fetch=kw) or fetch(*a, **kw)
    mod.run_backtest = lambda *a, **kw: seen.update(run=kw) or run(*a, **kw)
    try:
        with contextlib.redirect_stdout(io.StringIO()):   # cli_utils prints the run's header
            out = mod.run_single(WH, ["ZWOUT", "ZG20"], "ZWOUT_ZG20", True, 0.02, False, None, offset_days=0,
                                 exit_method="ltp") or {}
    finally:
        mod.fetch_data_via_api, mod.run_backtest = fetch, run
    got = ([(r["year"], r["portfolio"], r.get("candidates"), r["pairs_formed"], r["pairs_entered"]) for r in out.get("annual_returns", [])],
           [out.get(k) for k in ("total_rebalances", "cash_periods", "invested_periods", "total_trades", "trade_stats")],
           [seen.get(k, {}).get("offset_days") for k in ("fetch", "run")])
    if got != ([(2019, 0.0, 0, 0, 0), (2020, 0.0, 26, 3, 2)], [2, 2, 0, 0, {}], [0, 0]):
        yield f"the cash leg through run_single: {got}"
    # build_output on hand-built years: three formed pairs are not three entered ones
    rows = [dict(year=2001, portfolio_return=0.02, spy_return=0.01, pairs_formed=5, pairs_entered=3, pairs_with_trades=2,
                 total_trades=4, same_company_pairs=0, pairs=[]),
            dict(year=2002, portfolio_return=0.0, spy_return=0.01, pairs_formed=3, pairs_entered=2, pairs_with_trades=0,
                 total_trades=0, same_company_pairs=0, pairs=[])]
    m = compute_metrics([r["portfolio_return"] for r in rows], [r["spy_return"] for r in rows], 1, risk_free_rate=0.02)
    out = mod.build_output(m, [], rows, rows, {}, "X")
    got = [out.get(k) for k in ("cash_periods", "invested_periods", "avg_pairs_with_trades")]
    if got != [1, 1, 2.0]:
        yield f"results of one invested year and one of three formed, two entered pairs: {got}"


def fetch_checks(mod):
    """fetch_data_via_api: what the topic asks of the universe, and the 20-candidate floor."""
    for offset, offs in ((1, ["ISB"]), (0, ["ISA"])):
        got = mod.fetch_data_via_api(WH, ["ZFETCH"], benchmark_symbol="SPY", offset_days=offset)
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
        got = mod.fetch_data_via_api(WH, [exchange], benchmark_symbol="SPY")
        if (got is not None) != some:
            yield f"a leg of {exchange[2:]} candidates: {'skipped' if some else 'run'}"
        if got:
            got[0].close()


def main_wanted():
    """The main leg's two years: (trades, year records) from the fixture's own bars."""
    d = date
    t19 = [want("Q1A", "Q1B", d(2019, 3, 11), d(2019, 3, 14), "convergence", 2, -1, 0.002),
           want("Q2A", "Q2B", d(2019, 4, 11), d(2019, 4, 14), "convergence", 2, -1, 0.002)]
    t20 = [want("P00A", "P00B", d(2020, 3, 11), d(2020, 3, 14), "convergence", 2, -1, 0.002),
           want("P01A", "P01B", d(2020, 4, 11), d(2020, 4, 14), "convergence", 2, -1, 0.008),   # 0.3% + no cap
           want("P02A", "P02B", d(2020, 6, 11), d(2020, 6, 17), "last_bar", 6, -1, 0.002),
           want("P03A", "P03B", d(2020, 12, 27), d(2020, 12, 31), "year_end", 4, -1, 0.002),
           want("P09A", "P09B", d(2020, 12, 25), d(2020, 12, 31), "year_end", 6, -1, 0.002),
           want("P11A", "P11B", d(2020, 3, 21), d(2020, 3, 24), "convergence", 2, -1, 0.002)]
    bench = lambda y: PX[BENCH][d(y, 12, 31)] / PX[BENCH][d(y, 1, 1)] - 1.0
    years = [dict(year=2019, portfolio_return=sum(t[4] for t in t19) / 3, spy_return=bench(2019), pairs_formed=3,
                  pairs_entered=3, pairs_with_trades=2, total_trades=2, pairs=PAIRS_2019),
             dict(year=2020, portfolio_return=sum(t[4] for t in t20) / 20, spy_return=bench(2020), pairs_formed=20,
                  pairs_entered=16, pairs_with_trades=6, total_trades=6, pairs=PAIRS_2020[:20])]
    return t19 + t20, years


CLOSE_2020 = {"pair": ["P02A", "P02B"], "year": 2020, "entry_date": "2020-06-11", "last_common_bar": "2020-06-17",
              "delisted_date": ["2020-06-10", None], "delisting_bound": ["applied", "none"]}
OVERCLAIM = "survivorship" + "-" + "free"   # the wording no results file or source may carry
CHART_KEYS = ("universe", "annual_returns", "portfolio", "spy", "trade_stats", "n_years", "total_rebalances",
              "cash_periods", "comparison", "excess_cagr", "total_trades", "trades_per_year")


def main_checks(mod):
    """run_single on the XETRA leg: fetch, two years, the results JSON."""
    _, counts = cached("main", lambda: leg(MAIN, BENCH))
    trades, years = main_wanted()
    seen, real = {}, mod.run_backtest

    def spy(con, **kw):
        seen["books"] = {y: book(con, y) for y in YEARS}
        seen["kw"], seen["out"] = kw, real(con, **kw)
        return seen["out"]

    mod.run_backtest = spy
    sent = len(WH.sent)
    for method in ("drop", "both"):
        try:
            with contextlib.redirect_stdout(io.StringIO()):   # cli_utils prints the run's header
                mod.run_single(WH, MAIN, "XETRA", True, 0.02, False, None, offset_days=1, exit_method=method)
            yield f"run_single ran with exit_method={method}"
        except ValueError:
            pass
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            mod.run_single(WH, MAIN, "XETRA", True, 0.02, False, None, offset_days=1)
        yield "run_single ran without an exit method"
    except TypeError:
        pass
    if len(WH.sent) != sent:
        yield "run_single queried the warehouse before refusing an exit method"
    if mod.PUBLISHED_YEARS != (2005, 2024):
        yield f"the source's trading years are {mod.PUBLISHED_YEARS}, not 2005 to 2024"
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "results", "returns_XETRA.json")
        with contextlib.redirect_stdout(io.StringIO()):   # cli_utils prints the run's header
            out = mod.run_single(WH, MAIN, "XETRA", True, 0.02, False, path, offset_days=1, exit_method="ltp")
        with open(path) as fh:
            if out is None or json.load(fh) != json.loads(json.dumps(out)):
                yield "the results file is not the returned results"
    if out is None or "out" not in seen:
        yield "run_single returned nothing"
        return
    if seen["books"] != {y: book(cached("main", None)[0], y) for y in YEARS}:
        yield f"the main leg's candidates are not the module's: {seen['books']}"
    if seen["kw"].get("benchmark_symbol") != BENCH or seen["kw"].get("offset_days") != 1 or seen["kw"].get("use_costs") is not True:
        yield f"run_backtest was called with {seen['kw']}"
    records, stats = seen["out"]
    for rec, wanted in zip(records, years):
        for key, value in wanted.items():
            got = rec.get(key)
            if key == "pairs":
                first, last = date(rec["year"] - 1, 1, 1), date(rec["year"] - 1, 12, 31)
                rows = [corr_row(*p, first, last) for p in value]
                ok = (isinstance(got, list) and len(got) == len(rows) and all(
                    sorted(g) == ["a", "b", "corr", "sector"] and (g["a"], g["b"], g["sector"]) == r[:3]
                    and near(g["corr"], round(r[3], 4), 1e-9) for g, r in zip(got, rows)))
            else:
                ok = near(got, value) if isinstance(value, float) else got == value
            if not ok:
                yield f"{rec['year']} {key}: {got} != {value}"
        if rec.get("same_company_pairs") != 0:
            yield f"{rec['year']} same_company_pairs: {rec.get('same_company_pairs')}"
    n_trades = len(trades)
    share = lambda kind: round(sum(1 for t in trades if t[2] == kind) * 100.0 / n_trades, 1)
    wanted_stats = {"total_trades": n_trades, "convergence_rate": share("convergence"), "time_stop_rate": 0.0,
                    "loss_stop_rate": 0.0, "year_end_rate": share("year_end"), "last_bar_rate": share("last_bar"),
                    "avg_holding_days": round(sum(t[3] for t in trades) / n_trades, 1),
                    "avg_trade_return_pct": sum(t[4] for t in trades) / n_trades * 100}
    if (share("convergence"), share("year_end"), share("last_bar")) != (62.5, 25.0, 12.5):
        yield f"fixture: the main leg's exits are not 5, 2 and 1 of 8 trades: {trades}"
    if sorted(stats) != sorted(wanted_stats) or any(not near(stats[k], v, 6e-4) for k, v in wanted_stats.items()):
        yield f"trade_stats: {stats} != {wanted_stats}"

    # the results JSON
    if any(k not in out for k in CHART_KEYS) or out.get("universe") != "XETRA":
        yield f"a key the charts read is gone, or universe is not the leg's name: {sorted(out)}"
    rule = {**pairs_universe.UNIVERSE_NOTES, "top_n_per_sector": 30, "min_formation_bars": 200}
    block = {"exit_method": "ltp", "source": "last common bar of the pair", "delist_grace_days": 7,
             "early_closes": 1, "fills": [CLOSE_2020]}
    for key, value in (("universe_rule", rule), ("listing_filter", "country DE"), ("isin_dedup", True),
                       ("company_dedup", True), ("exit_method", "ltp"), ("ltp", block), ("years", "2019-2020"),
                       ("total_rebalances", 2), ("n_periods", 2), ("n_years", 2), ("cash_periods", 0),
                       ("invested_periods", 2), ("avg_pairs_with_trades", 4.0), ("total_trades", n_trades),
                       ("trades_per_year", 4.0), ("trade_stats", stats)):
        if out.get(key) != value:
            yield f"results {key}: {out.get(key)} != {value}"
    m = compute_metrics([r["portfolio_return"] for r in records], [r["spy_return"] for r in records], 1,
                        risk_free_rate=0.02)
    for node, key in (("portfolio", "portfolio"), ("spy", "benchmark")):
        if out[node].get("cagr") != round(m[key]["cagr"] * 100, 2):
            yield f"results {node}.cagr: {out[node].get('cagr')} != {round(m[key]['cagr'] * 100, 2)}"
    annual = out.get("annual_returns") or []
    for rec, row in zip(records, annual if len(annual) == len(records) else [{}] * len(records)):
        wanted = {"year": rec["year"], "portfolio": round(rec["portfolio_return"] * 100, 2),
                  "spy": round(rec["spy_return"] * 100, 2),
                  "excess": round((rec["portfolio_return"] - rec["spy_return"]) * 100, 2),
                  **{k: rec[k] for k in ("pairs_formed", "pairs_entered", "pairs_with_trades", "total_trades",
                                         "same_company_pairs", "pairs")}, **counts[rec["year"]]}
        if row != wanted:
            diff = sorted(k for k in set(row) | set(wanted) if row.get(k) != wanted.get(k))
            yield f"annual_returns {rec['year']} differs on {diff}"
    if (counts[2019]["candidates"], counts[2019]["sectors"], counts[2020]["same_company"],
            counts[2020]["no_cost_cap"]) != (6, 3, 1, 1):
        yield f"fixture: the main leg's universe counts: {counts}"
    if OVERCLAIM in json.dumps(out).lower():
        yield "the results claim survivorship is removed: it is only reduced"
    for problem in WH.problems:
        yield problem


LEGS = [("us", ["NYSE", "NASDAQ", "AMEX"]), ("japan", ["JPX"]), ("canada", ["TSX"]), ("hongkong", ["HKSE"]),
        ("china", ["SHZ", "SHH"]), ("korea", ["KSC"]), ("taiwan", ["TAI", "TWO"]), ("sweden", ["STO"]),
        ("southafrica", ["JNB"]), ("india", ["NSE"]), ("uk", ["LSE"]), ("germany", ["XETRA"])]


def cli_checks(mod):
    """main(): only ltp runs, and every market of the all-market loop gets the exit method."""
    calls = []

    def run_single(cr, exchanges, name, use_costs, rfr, verbose, output_path=None, **kw):
        calls.append((exchanges, name, use_costs, output_path and os.path.basename(output_path), kw))
        return {"portfolio": {"cagr": 1.0}, "universe": name}

    mod.CetaResearch = lambda *a, **k: calls.append("client") or "client"
    mod.run_single = run_single
    argv = sys.argv
    try:
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "exchange_comparison.json")
            runs = (
                (["--exit-method", "drop"], "refused"), (["--exit-method", "both"], "refused"),
                (["--global", "--exit-method", "both"], "refused"),
                (["--exchange", "NYSE,NASDAQ,AMEX", "--output", target],
                 [(["NYSE", "NASDAQ", "AMEX"], "NYSE_NASDAQ_AMEX", True, "exchange_comparison.json",
                   {"offset_days": 1, "exit_method": "ltp"})]),
                (["--preset", "japan", "--exit-method", "ltp", "--no-next-day", "--no-costs"],
                 [(["JPX"], "JPX", False, None, {"offset_days": 0, "exit_method": "ltp"})]),
                (["--global", "--output", target],
                 [(ex, "_".join(ex), True, f"returns_{'_'.join(ex)}.json", {"offset_days": 1, "exit_method": "ltp"})
                  for _, ex in LEGS]),
            )
            for flags, wanted in runs:
                del calls[:]
                sys.argv = ["backtest.py"] + flags
                try:
                    with contextlib.redirect_stderr(io.StringIO()):   # parser.error prints the usage
                        mod.main()
                    got = calls[1:] if calls[:1] == ["client"] else f"no client first: {calls}"
                except SystemExit as e:
                    got = "refused" if e.code not in (0, None) and not calls else f"exit {e.code} after {calls}"
                if got != wanted:
                    yield f"main() with {flags}: {got} != {wanted}"
            with open(target) as fh:
                if list(json.load(fh)) != ["_".join(ex) for _, ex in LEGS]:
                    yield "the all-market file does not hold the 12 markets"
    finally:
        sys.argv = argv


def failures(src, first_only=False):
    out = []
    try:
        mod = load(src)
        for check in (unit_checks, formation_checks, cash_checks, entry_checks, fetch_checks, main_checks, cli_checks):
            for f in check(mod):
                out.append(f)
                if first_only:
                    return out
    except Exception as e:  # a mutant that breaks the topic fails loudly
        out.append(f"error: {type(e).__name__}: {e}")
    finally:
        WH.problems = []
    return out


def const(name, value):
    """A parameter line's 'NAME = value ' as the topic spells it."""
    line_ = next(ln for ln in SRC.splitlines() if ln.startswith(name + " "))
    return line_[:line_.index("#")].rstrip() + " " if value is None else f"{name.ljust(17)} = {value} "


def later_bars(count, enough):
    """A mutant: a pair is entered only if `count`, a statement on bars after its entry bars, finds `enough` of them."""
    return KEEP, f'        if con.execute(f"SELECT {count}").fetchone()[0] < {enough}:\n            continue\n' + KEEP


LAGS = ("(p.adjClose - LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date, p.adjClose DESC NULLS LAST))\n"
        "                    / NULLIF(LAG(p.adjClose) OVER (PARTITION BY p.symbol ORDER BY p.trade_date, p.adjClose DESC NULLS LAST), 0)")
RANK = "ORDER BY correlation DESC, sym_a, sym_b"
IN_YEAR = "WHERE trade_date >= '{trading_start.isoformat()}'\n          AND z_score IS NOT NULL"
RET = "pair_return = direction * (ret_a - ret_b) / 2.0"
RET_NEXT = "ret_b      = (pb - entry_pb) / entry_pb\n            " + RET          # the queued exit's return
RET_HELD = "ret_b = (pb - entry_pb) / entry_pb\n            " + RET               # the held position's, each bar
RET_LAST = "ret_b = (last_pb - entry_pb) / entry_pb\n            " + RET          # the forced close's
FLOOR = "pair_return = max(pair_return, -MAX_SINGLE_RETURN)\n"
NEXT_TAIL = "            _append_trade(rows[entry_idx][0], trade_date,\n                          entry_z, z, exit_type"
LAST_TAIL = "            trading_day = len(rows) - 1 - entry_idx"
COST_NEXT = "            pair_return -= cost\n            " + FLOOR + NEXT_TAIL
COST_SAME = "                    pair_return -= cost\n                    " + FLOOR
COST_LAST = "            pair_return -= cost\n            " + FLOOR + LAST_TAIL
ENTER = "if abs(z) >= Z_ENTRY:\n                if offset_days == 0:"
EXIT = "if exit_type:\n                if offset_days == 0:"
BOUND = "last_day = ltp.bound(sym_a, sym_b, trading_start, trading_end) if ltp is not None else trading_end"
CLOSE = "if ltp is not None:   # an exit queued for a bar that never came is a forced close too"
CLOSE_ARGS = "ltp.close(sym_a, sym_b, rows[entry_idx][0], last_date,"
FLOOR_TEST = "if float(pa_row[0]) < MIN_LEG_PRICE or float(pb_row[0]) < MIN_LEG_PRICE:"
ENTRY = "window_days=ENTRY_WINDOW_DAYS, offset_days=offset_days, with_epochs=True)"
PAST = "if utc_date(max(pa_row[1], pb_row[1])) > bound:"
ENTRY_BOUND = "bound = ltp.bound(sym_a, sym_b, trading_start, trading_end)"
ENTERED = "entered = entered_pairs(con, selected, trading_start, trading_end, offset_days, ltp)"
STEP_5 = "# ── Step 5: Simulate z-score trades"
CORR_WINDOW = "WHERE p.trade_date >= '{fs}' AND p.trade_date <= '{fe}'"
FORM_WINDOW = "WHERE trade_date >= '{fs}' AND trade_date <= '{fe}'"   # twice in estimate_spread_params: hedge ratio, half-life
DAY_EARLY = (">= '{fs}'", ">= '{(formation_start - timedelta(days=1)).isoformat()}'")
DAY_LATE = ("<= '{fe}'", "<= '{(formation_end + timedelta(days=1)).isoformat()}'")
WARM_UP = "warmup_start = trading_start - timedelta(days=Z_LOOKBACK * 2)"
FETCH = "benchmark_symbol=benchmark_symbol, offset_days=offset_days)\n    if fetched is None:"
RUN = "offset_days=offset_days,\n                                        benchmark_symbol=benchmark_symbol, ltp=ltp)"
CASH = "invested = n_entered >= MIN_PAIRS_ACTIVE"
BOOK = "if invested:\n            port_return = sum(year_trade_returns) / n_formed"
CAPS = 'SELECT symbol, cap_usd FROM candidates WHERE yr = ?",\n                                [year]'
CHECK = "same_company = check_pairs(con, year, [(p[0], p[1], p[3]) for p in selected])"
BENCH_END = "AND trade_date <= '{trading_end.isoformat()}'\n            ORDER BY trade_date DESC"
CASH_OUT = 'cash_periods  = sum(1 for r in executed if r["pairs_entered"] < MIN_PAIRS_ACTIVE)'
INVESTED_OUT = 'invested_yrs  = [r for r in executed if r["pairs_entered"] >= MIN_PAIRS_ACTIVE]'
COUNTS = '**(per_year or {}).get(r["year"], {}),'
RULE = ('        "universe_rule":       {**UNIVERSE_NOTES, "top_n_per_sector": TOP_N_PER_SECTOR,\n'
        '                                "min_formation_bars": MIN_CORR_DAYS},\n')
BUILD = "build_candidates(con, years, offset_days, MIN_LEG_PRICE,"
REFUSE = 'if exit_method != "ltp":   # a pair has no drop book'
REFUSE_CLI = 'if args.exit_method != "ltp":   # no drop book exists'
ALL_MARKETS = "offset_days=offset_days,\n                                    exit_method=args.exit_method)"
ONE_MARKET = "args.verbose, args.output, offset_days=offset_days,\n               exit_method=args.exit_method)"
KEEP = "        entered.append(pair)\n    return entered"   # the last lines of entered_pairs
LEG_BARS = "FROM prices_cache WHERE symbol IN ('{sym_a}', '{sym_b}')"
PAIR_BARS = ("FROM prices_cache a JOIN prices_cache b ON a.trade_date = b.trade_date"
             " WHERE a.symbol = '{sym_a}' AND b.symbol = '{sym_b}'")
WINDOW_END = "'{(trading_start + timedelta(days=offset_days + ENTRY_WINDOW_DAYS)).isoformat()}'"
THE_YEAR = "a.trade_date >= '{trading_start.isoformat()}' AND a.trade_date <= '{trading_end.isoformat()}'"
MUTANTS = {
    # formation: the year's candidates, same sector, 200 common days, correlation 0.70, never NaN
    "pairs from the formation year's candidates": ("year = formation_end.year + 1", "year = formation_end.year"),
    "pairs from any year's candidates": ("ON c.symbol = p.symbol AND c.yr = {year}", "ON c.symbol = p.symbol"),
    "pairs across sectors": ("\n               AND a.sector = b.sector", ""),
    "199 common days enough": ("HAVING COUNT(*) >= {MIN_CORR_DAYS}", "HAVING COUNT(*) >= {MIN_CORR_DAYS - 1}"),
    "200 common days not enough": ("HAVING COUNT(*) >= {MIN_CORR_DAYS}", "HAVING COUNT(*) > {MIN_CORR_DAYS}"),
    "correlation 0.7000 refused": ("WHERE correlation >= {MIN_CORR}", "WHERE correlation > {MIN_CORR}"),
    "correlation 0.6999 accepted": (const("MIN_CORR", None), const("MIN_CORR", "0.6999")),
    "NaN correlation ranks first": ("\n          AND NOT isnan(correlation)", ""),
    "lowest correlation first": (RANK, "ORDER BY correlation ASC, sym_a, sym_b"),
    "correlation ties: first symbol descending": (RANK, "ORDER BY correlation DESC, sym_a DESC, sym_b"),
    "correlation ties: second symbol descending": (RANK, "ORDER BY correlation DESC, sym_a, sym_b DESC"),
    # stands in for "no tie-break": DuckDB's own order among equals changes from run to run
    "correlation ties in hash order": (RANK, "ORDER BY correlation DESC, hash(sym_a || sym_b)"),
    "201 correlation rows": ("LIMIT {MAX_PAIRS * 10}", "LIMIT {MAX_PAIRS * 10 + 1}"),
    "199 correlation rows": ("LIMIT {MAX_PAIRS * 10}", "LIMIT {MAX_PAIRS * 10 - 1}"),
    "a day printed twice: lower print first": (LAGS, LAGS.replace("DESC", "ASC")),
    "a day printed twice: rows as stored": (LAGS, LAGS.replace(", p.adjClose DESC NULLS LAST", "")),
    # the half-life window and the 20 pairs
    "half-life of 4.5 days kept": (const("HALF_LIFE_MIN", None), const("HALF_LIFE_MIN", 4)),
    "half-life of 5.5 days dropped": (const("HALF_LIFE_MIN", None), const("HALF_LIFE_MIN", 6)),
    "half-life of 59.5 days dropped": (const("HALF_LIFE_MAX", None), const("HALF_LIFE_MAX", 59)),
    "half-life of 60.5 days kept": (const("HALF_LIFE_MAX", None), const("HALF_LIFE_MAX", 61)),
    "19 pairs": (const("MAX_PAIRS", None), const("MAX_PAIRS", 19)),
    "21 pairs": (const("MAX_PAIRS", None), const("MAX_PAIRS", 21)),
    "half-life filter on the first 20 correlation rows only": (
        "for (sym_a, sym_b, sector, corr, common_days) in candidates:",
        "for (sym_a, sym_b, sector, corr, common_days) in candidates[:MAX_PAIRS]:"),
    # the z-score: 40 common bars, a bar inside the year
    "z-score on 39 bars": (const("Z_LOOKBACK", None), const("Z_LOOKBACK", 39)),
    "z-score on 41 bars": (const("Z_LOOKBACK", None), const("Z_LOOKBACK", 41)),
    "z-score on a short window": ("\n          AND window_bars = {Z_LOOKBACK}", ""),
    "signal on a bar before the year": (IN_YEAR, "WHERE z_score IS NOT NULL"),
    "first bar of the year gives no signal": (IN_YEAR, IN_YEAR.replace(">=", ">")),
    # entry and exit thresholds, one step either side
    "entry at z 1.98": (const("Z_ENTRY", None), const("Z_ENTRY", 1.95)),
    "no entry at z 2.02": (const("Z_ENTRY", None), const("Z_ENTRY", 2.05)),
    "no entry on a negative z": ("if abs(z) >= Z_ENTRY:", "if z >= Z_ENTRY:"),
    "exit at z 0.52": (const("Z_EXIT", None), const("Z_EXIT", 0.55)),
    "no exit at z 0.48": (const("Z_EXIT", None), const("Z_EXIT", 0.45)),
    "a deeply negative z counts as converged": ("if abs(z) <= Z_EXIT:", "if z <= Z_EXIT:"),
    "time stop on the 59th held day": (const("MAX_HOLD_DAYS", None), const("MAX_HOLD_DAYS", 59)),
    "time stop on the 61st held day": (const("MAX_HOLD_DAYS", None), const("MAX_HOLD_DAYS", 61)),
    "loss stop at 4.99%": (const("LOSS_STOP", None), const("LOSS_STOP", -0.0495)),
    "no loss stop at 5.01%": (const("LOSS_STOP", None), const("LOSS_STOP", -0.0505)),
    "floor at -190%": (const("MAX_SINGLE_RETURN", None), const("MAX_SINGLE_RETURN", 1.9)),
    "floor at -210%": (const("MAX_SINGLE_RETURN", None), const("MAX_SINGLE_RETURN", 2.1)),
    "no floor on a next-bar exit": (FLOOR + NEXT_TAIL, NEXT_TAIL.lstrip()),
    "no floor on a same-bar exit": ("                    " + FLOOR, ""),
    "no floor on the forced close": (FLOOR + LAST_TAIL, LAST_TAIL.lstrip()),
    # execution: the bar after the signal, the right side, half the spread return
    "entry on the signal bar": (ENTER, ENTER.replace("offset_days == 0", "True")),
    "exit on the signal bar": (EXIT, EXIT.replace("offset_days == 0", "True")),
    "same-bar mode queues the entry": (ENTER, ENTER.replace("offset_days == 0", "False")),
    "same-bar mode queues the exit": (EXIT, EXIT.replace("offset_days == 0", "False")),
    "exits tested on the entry bar": ("            continue  # don't evaluate exits on entry bar\n", ""),
    "spread without the hedge ratio": ("a.lp - {beta} * b.lp AS spread", "a.lp - b.lp AS spread"),
    "every pair traded at a hedge ratio of 1": ("con, sym_a, sym_b, beta,\n                trading_start, trading_end,\n                caps",
                                                "con, sym_a, sym_b, 1.0,\n                trading_start, trading_end,\n                caps"),
    "next-bar entry on the wrong side": ("direction   = -1 if sig_z > 0 else 1", "direction   = 1 if sig_z > 0 else -1"),
    "same-bar entry on the wrong side": ("direction   = -1 if z > 0 else 1", "direction   = 1 if z > 0 else -1"),
    "next-bar exit: the whole spread return": (RET_NEXT, RET_NEXT.replace("/ 2.0", "/ 1.0")),
    "next-bar exit: B's return ignored": (RET_NEXT, RET_NEXT.replace("(ret_a - ret_b)", "ret_a")),
    "held position: the whole spread return": (RET_HELD, RET_HELD.replace("/ 2.0", "/ 1.0")),
    "held position: B's return ignored": (RET_HELD, RET_HELD.replace("(ret_a - ret_b)", "ret_a")),
    "forced close: the whole spread return": (RET_LAST, RET_LAST.replace("/ 2.0", "/ 1.0")),
    "forced close: B's return ignored": (RET_LAST, RET_LAST.replace("(ret_a - ret_b)", "ret_a")),
    # costs: each leg's rate once per round trip
    "each leg's cost charged twice": ("cost = (tiered_cost(mc_a)", "cost = 2 * (tiered_cost(mc_a)"),
    "A's cost for both legs": ("tiered_cost(mc_a) + tiered_cost(mc_b)", "tiered_cost(mc_a) + tiered_cost(mc_a)"),
    "costs when switched off": (") if use_costs else 0.0", ") if True else 0.0"),
    "no cost on a next-bar exit": (COST_NEXT, COST_NEXT.replace("pair_return -= cost\n            ", "", 1)),
    "no cost on a same-bar exit": (COST_SAME, COST_SAME.replace("pair_return -= cost\n                    ", "", 1)),
    "no cost on the forced close": (COST_LAST, COST_LAST.replace("pair_return -= cost\n            ", "", 1)),
    "no cost on a forced close whose exit was queued": (
        COST_LAST, COST_LAST.replace("pair_return -= cost", "pair_return -= 0.0 if pending_exit_type else cost", 1)),
    # the forced close: inside the delisting bound and the year, labelled by the helper
    "bars past the delisting bound": (BOUND, "last_day = trading_end"),
    "bound from the B leg alone": (BOUND, BOUND.replace("ltp.bound(sym_a, sym_b,", "ltp.bound(sym_b, sym_b,")),
    "bars after the year end": ("\n              AND trade_date <= '{last_day.isoformat()}'", ""),
    "a queued exit keeps its own label": (CLOSE, CLOSE.replace("is not None:", "is not None and pending_exit_type is None:")),
    "forced close never labelled by the helper": (CLOSE, CLOSE.replace("ltp is not None:", "False:")),
    "close recorded with the last bar as its entry": (CLOSE_ARGS, CLOSE_ARGS.replace("rows[entry_idx][0]", "last_date")),
    "close recorded with the legs swapped": (CLOSE_ARGS, CLOSE_ARGS.replace("sym_a, sym_b", "sym_b, sym_a")),
    "exit helper not passed to the simulation": ("offset_days=offset_days, ltp=ltp\n", "offset_days=offset_days\n"),
    # the year: entered pairs, the cash rule, the book
    "entry price of 0.99 accepted": (const("MIN_LEG_PRICE", None), const("MIN_LEG_PRICE", 0.99)),
    "entry price of 1.00 refused": (const("MIN_LEG_PRICE", None), const("MIN_LEG_PRICE", 1.01)),
    "entry price of exactly 1.00 refused in the year loop": (FLOOR_TEST, FLOOR_TEST.replace("<", "<=")),
    "A leg's entry price not tested": (FLOOR_TEST, "if float(pb_row[0]) < MIN_LEG_PRICE:"),
    "B leg's entry price not tested": (FLOOR_TEST, "if float(pa_row[0]) < MIN_LEG_PRICE:"),
    # entered on the entry bars alone: no bar after the window is asked of a pair
    "entered only with a common bar after the window": later_bars(f"COUNT(*) {PAIR_BARS} AND a.trade_date > {WINDOW_END}", 1),
    "entered only if each leg prints after the window": later_bars(
        f"COUNT(DISTINCT symbol) {LEG_BARS} AND trade_date > {WINDOW_END}", 2),
    "entered only with 40 common bars in the year": later_bars(f"COUNT(*) {PAIR_BARS} AND {THE_YEAR}", 40),
    "cash with exactly 3 entered pairs": (CASH, CASH.replace(">=", ">")),
    "invested with 2 entered pairs": (const("MIN_PAIRS_ACTIVE", None), const("MIN_PAIRS_ACTIVE", 2)),
    "cash with 3 entered pairs": (const("MIN_PAIRS_ACTIVE", None), const("MIN_PAIRS_ACTIVE", 4)),
    "cash decided on the pairs that traded": (BOOK, BOOK.replace("if invested:", "if pairs_with_trades >= MIN_PAIRS_ACTIVE:")),
    "a cash year still trades": ("in (entered if invested else []):", "in entered:"),
    "return over the entered pairs": (BOOK, BOOK.replace("/ n_formed", "/ n_entered")),
    "return over the pairs that traded": (BOOK, BOOK.replace("/ n_formed", "/ max(pairs_with_trades, 1)")),
    "cost tier from the year before": (CAPS, CAPS.replace("[year]", "[year - 1]")),
    "cost tier from a later year's cap": (CAPS, CAPS.replace("yr = ?", "yr >= ?")),
    "cost tier from the traded-value size": (CAPS, CAPS.replace("cap_usd", "size_usd")),
    "cost tier without the caps": ("caps.get(sym_a), caps.get(sym_b), use_costs", "None, None, use_costs"),
    "same-company pairs not checked": (CHECK, "same_company = 0"),
    "only the entered pairs recorded": ("for p in selected],", "for p in entered],"),
    "formed pairs recorded as entered": ('"pairs_entered":    n_entered,', '"pairs_entered":    n_formed,'),
    "same-company count not recorded": ('"same_company_pairs": same_company,', '"same_company_pairs": None,'),
    "pairs not written to the results": ('"pairs":             r["pairs"],', '"pairs":             [],'),
    "benchmark year ends a day early": (BENCH_END, BENCH_END.replace("<=", "<")),
    "last-bar closes counted as year-end": ('if t["exit_type"] == "last_bar")', 'if t["exit_type"] == "year_end")'),
    # the results
    "cash years counted on the pairs that traded": (CASH_OUT, CASH_OUT.replace("pairs_entered", "pairs_with_trades")),
    "invested years taken from the pairs that traded": (INVESTED_OUT, INVESTED_OUT.replace("pairs_entered", "pairs_with_trades")),
    "universe counts of the year before": (COUNTS, COUNTS.replace('r["year"]', 'r["year"] - 1')),
    "universe counts not recorded": (COUNTS, "**{},"),
    "universe counts not passed": ("per_year=per_year, ", ""),
    "universe rule not recorded": (RULE, ""),
    "universe rule in place of the leg's name": ('"universe":             universe_name,',
                                                 '"universe":             dict(UNIVERSE_NOTES),'),
    "top N recorded from another constant": ('"top_n_per_sector": TOP_N_PER_SECTOR,', '"top_n_per_sector": MAX_PAIRS,'),
    "listing rule not recorded": ('"listing_filter":      listing_filter,', '"listing_filter":      None,'),
    "listing rule not passed": ("listing_filter=campaign_listing_label(exchanges),", "listing_filter=None,"),
    "ISIN dedup not recorded": ('"isin_dedup":          True,', '"isin_dedup":          False,'),
    "company dedup not recorded": ('"company_dedup":       True,', '"company_dedup":       False,'),
    "exit block not recorded": ("if ltp_block is not None:", "if False:"),
    "exit helper built without the delisting table": ("PairLtp(con, load_delist_bounds(cr), verbose=verbose)",
                                                      "PairLtp(con, {}, verbose=verbose)"),
    # the fetch: what the topic asks of the universe
    "entry offset not passed to the universe": (BUILD, BUILD.replace("offset_days", "1")),
    "entry floor not passed to the universe": (BUILD, BUILD.replace("MIN_LEG_PRICE", "0.0")),
    "top 29 per sector": (const("TOP_N_PER_SECTOR", None), const("TOP_N_PER_SECTOR", 29)),
    "top 31 per sector": (const("TOP_N_PER_SECTOR", None), const("TOP_N_PER_SECTOR", 31)),
    "199 bars and 199 common days enough": (const("MIN_CORR_DAYS", None), const("MIN_CORR_DAYS", 199)),
    "200 bars and 200 common days not enough": (const("MIN_CORR_DAYS", None), const("MIN_CORR_DAYS", 201)),
    "a leg of 19 candidates runs": (const("MIN_CANDIDATES", None), const("MIN_CANDIDATES", 19)),
    "a leg of 20 candidates is skipped": (const("MIN_CANDIDATES", None), const("MIN_CANDIDATES", 21)),
    "candidate floor on the thinnest year": ("if max(sizes) < MIN_CANDIDATES:", "if min(sizes) < MIN_CANDIDATES:"),
    "one year's universe for the whole run": ("years = range(START_YEAR, END_YEAR + 1)", "years = range(END_YEAR, END_YEAR + 1)"),
    "benchmark bars not fetched": ("fetch_prices(cr, con, None, [benchmark_symbol],", "fetch_prices(cr, con, None, [],"),
    # only ltp exits exist, on every market
    "run_single takes any exit method": (REFUSE, REFUSE.replace('exit_method != "ltp"', "False")),
    "the CLI takes drop and both": (REFUSE_CLI, REFUSE_CLI.replace('args.exit_method != "ltp"', "False")),
    "exit method not passed in the all-market loop": (ALL_MARKETS, "offset_days=offset_days)"),
    "exit method not passed for one market": (ONE_MARKET, "args.verbose, args.output, offset_days=offset_days)"),
    "a market missing from the all-market loop": ('            ("germany",     ["XETRA"]),\n', ""),
    "taiwan run as TAI alone": ('("taiwan",      ["TAI", "TWO"]),', '("taiwan",      ["TAI"]),'),
}


def nth(k, old, new, start="def estimate_spread_params", stop="def entered_pairs"):
    """A mutant of the k-th of two `old` between two lines of the source: (that stretch, the same with it replaced)."""
    seg = SRC[SRC.index(start):SRC.index(stop)]
    parts = seg.split(old)
    if len(parts) != 3:
        return f"<{old!r} is not twice in {start}>", new
    return seg, old.join(parts[:k + 1]) + new + old.join(parts[k + 1:])


def moved():
    """A mutant: the company check runs on the entered pairs, after the entry bars, not on the year's selected pairs."""
    seg = SRC[SRC.index(CHECK):SRC.index(STEP_5)]
    return seg, seg.replace(CHECK, "same_company = 0") + CHECK.replace("in selected", "in entered") + "\n\n        "


# Mutants of text this topic words its own way. scripts/test_pairs_multi_pair.py applies MUTANTS to its topic, not these.
ZSCORE_ONLY = {
    # entered pairs: the entry window at either offset, the floor on its first bar, the pair's delisting bound
    "entry window closes a day early": (ENTRY, ENTRY.replace("ENTRY_WINDOW_DAYS", "ENTRY_WINDOW_DAYS - 1")),
    "entry window closes a day late": (ENTRY, ENTRY.replace("ENTRY_WINDOW_DAYS", "ENTRY_WINDOW_DAYS + 1")),
    "entry bar from anywhere in the year": (ENTRY, ENTRY.replace("ENTRY_WINDOW_DAYS", "(trading_end - trading_start).days")),
    "entry window opens on 1 January at next-bar execution": (ENTRY, ENTRY.replace("offset_days=offset_days", "offset_days=0")),
    "entry window opens on 2 January at same-bar execution": (ENTRY, ENTRY.replace("offset_days=offset_days", "offset_days=1")),
    "delisting bound not applied to the entry bars": (PAST, "if False:"),
    "entry bar on the bound's own day refused": (PAST, PAST.replace("> bound", ">= bound")),
    "bound applied to A's entry bar alone": (PAST, PAST.replace("max(pa_row[1], pb_row[1])", "pa_row[1]")),
    "bound applied to B's entry bar alone": (PAST, PAST.replace("max(pa_row[1], pb_row[1])", "pb_row[1]")),
    "entry bound from A's delisting alone": (ENTRY_BOUND, ENTRY_BOUND.replace("sym_a, sym_b", "sym_a, sym_a")),
    "entry bound from B's delisting alone": (ENTRY_BOUND, ENTRY_BOUND.replace("sym_a, sym_b", "sym_b, sym_b")),
    "entry bound anchored on the year's end": (ENTRY_BOUND, ENTRY_BOUND.replace("trading_start, trading_end", "trading_end, trading_end")),
    "entry bars default to same-bar execution": ("trading_start, trading_end, offset_days=1, ltp=None):",
                                                 "trading_start, trading_end, offset_days=0, ltp=None):"),
    # the year: cash on the entered pairs, only they trade, the company check on every formed pair
    "cash decided on the pairs formed": (CASH, CASH.replace("n_entered", "n_formed")),
    "cash decided on the pairs selected": (CASH, CASH.replace("n_entered", "len(selected)")),
    "formed pairs counted as entered": ("n_formed, n_entered = len(selected), len(entered)",
                                        "n_formed, n_entered = len(selected), len(selected)"),
    "every formed pair trades in an invested year": ("in (entered if invested else []):", "in (selected if invested else []):"),
    "entry bars of a next-bar run in a same-bar one": (ENTERED, ENTERED.replace("offset_days, ltp)", "1, ltp)")),
    "entry bars of a same-bar run in a next-bar one": (ENTERED, ENTERED.replace("offset_days, ltp)", "0, ltp)")),
    "exit helper not passed to the entry bars": (ENTERED, ENTERED.replace("offset_days, ltp)", "offset_days, None)")),
    "same-company pairs checked on the first pair only": (CHECK, CHECK.replace("in selected]", "in selected[:1]]")),
    "same-company pairs checked among the entered only": moved(),
    "cash years counted on the pairs formed": (CASH_OUT, CASH_OUT.replace("pairs_entered", "pairs_formed")),
    "invested years taken from the pairs formed": (INVESTED_OUT, INVESTED_OUT.replace("pairs_entered", "pairs_formed")),
    # formation statistics: the formation year, not a day either side
    "correlation window opens a day early": (CORR_WINDOW, CORR_WINDOW.replace(*DAY_EARLY)),
    "correlation window closes a day late": (CORR_WINDOW, CORR_WINDOW.replace(*DAY_LATE)),
    "hedge ratio window opens a day early": nth(0, FORM_WINDOW, FORM_WINDOW.replace(*DAY_EARLY)),
    "hedge ratio window closes a day late": nth(0, FORM_WINDOW, FORM_WINDOW.replace(*DAY_LATE)),
    "half-life window opens a day early": nth(1, FORM_WINDOW, FORM_WINDOW.replace(*DAY_EARLY)),
    "half-life window closes a day late": nth(1, FORM_WINDOW, FORM_WINDOW.replace(*DAY_LATE)),
    # the z-score warm-up on weekday bars
    "z-score warm-up of 40 calendar days": (WARM_UP, WARM_UP.replace("Z_LOOKBACK * 2", "Z_LOOKBACK")),
    "z-score warm-up of 39 calendar days": (WARM_UP, WARM_UP.replace("Z_LOOKBACK * 2", "Z_LOOKBACK - 1")),
    # run_single and the fetch
    "entry offset not passed to the fetch": (FETCH, FETCH.replace(", offset_days=offset_days)", ")")),
    "entry offset not passed to the year loop": (RUN, RUN.split("\n", 1)[1].lstrip()),
    "run_single defaults to an exit method": ("offset_days=1, *, exit_method):", 'offset_days=1, *, exit_method="ltp"):'),
    "one exchange of a two-exchange leg": ("fetch_universe(cr, con, exchanges, years,", "fetch_universe(cr, con, exchanges[:1], years,"),
    "trading years from 2006": ("START_YEAR        = 2005\n", "START_YEAR        = 2006\n"),
    "trading years to 2023": ("END_YEAR          = 2024\n", "END_YEAR          = 2023\n"),
}


def main():
    base = fixture_problems() + guard_problems()
    if OVERCLAIM in SRC.lower():
        base.append("the topic's source claims survivorship is removed: it is only reduced")
    mutants = {**MUTANTS, **ZSCORE_ONLY}
    if len(mutants) != len(MUTANTS) + len(ZSCORE_ONLY):
        base.append(f"mutants named in both lists: {sorted(set(MUTANTS) & set(ZSCORE_ONLY))}")
    base = base or failures(SRC)
    if base:
        print("test_pairs_zscore: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in mutants.items():
        if SRC.count(old) != 1:
            print(f"test_pairs_zscore: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new), first_only=True):
            survived.append(name)
    if survived:
        print("test_pairs_zscore: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    print(f"test_pairs_zscore: PASS ({len(PROFILE)} lines, {len(mutants)} mutants caught)")


if __name__ == "__main__":
    main()
