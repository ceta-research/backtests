#!/usr/bin/env python3
"""Offline check of 52-week-high's selection: proximity on the price basis (no network).

screen_stocks must rank close / MAX(high) over the 252 bars ending on the signal bar, both
split-adjusted and neither dividend-adjusted. adjClose is back-adjusted for every later
dividend, so adjClose / MAX(high) ranked a name by how little it paid out after the rebalance
date: KO on its high on 2003-01-02 scored 0.51. Exact ties (names closing on their high) rank
by the larger latest eligible market cap, then symbol. The signal bar is the last bar in
[R - 10d, R] with volume > 0, never a later one (entry is the first bar from R + 1); a line with
no such bar is not ranked, and a signal bar without a close drops the name. Untraded (zero or NULL
volume) bars are never the signal but count in the 252-bar window that ends at it, toward its high
and the 100-bar floor; an untraded twin of the signal bar sorts after it and takes no slot. The signal guard
counts, per rebalance, the lines over the floor it leaves unranked, by reason (no traded bar in the span: STALE and
TV_STALE; fewer than 100 bars: H99), never a line under the floor, and check M's events read ranked lines only.
There is no proximity cap: a close over the window's high ranks as it reads. The
topic's own fetch SQL fills the caches from made-up warehouse rows, so it must carry close and
volume and filter on neither. They are fetched over two rebalance dates, so they are
continuous as in a real run and only the screen's own window start keeps a bar older than
R - 390d out; a one-date cache is the first rebalance, where the fetch's start is the bound.
The same-security dedup still runs before the top-30 cut, on the run's own entry offset. The floor, the
book's market cap and the tie-break read cap_used (check M): a cap that check M divides onto the floor is out, an
ineligible row is out, each with its event, and the tie-break follows cap_used, not the raw cap. Every
mutant of the topic's source must trip a check; the traded-bar rule's must trip it on a wrong
book, not an error.

Universe rules ur-4, each leg through the topic's fetch: an SAO company holds one slot with data checks off (its
preferred line stands in for an ordinary line with no entry bar); a JPX line whose profile industry starts with
REIT is out on JPX and kept on another leg; a fund the fetch caches is never ranked (the universe semi-join); COG
and CLR rank on their last own day and not the day after; GETY, a lookback_clip start, ranks on its own bars
only, its first own day's among them, and DOC, a rename, on the 252 bars ending at its signal bar.

Check T on the ranking cache, with data checks on: on a closed rebalance date a line's traded copy moves, so the
line ranks on its last traded bar before it, while a copy 40 days back, outside every fetch window, stays in the
lookback. The fetch runs check T, its sibling rule on, before the oscillation filter: a one-day spike outside every
window goes, a spiked traded copy on the closed date is check T's to move, and an NVDR line's first bar after that
date, off its ordinary line's scale, moves as a sibling gap.

The run on test_data_checks' synthetic SET leg (wh52_source): run_single on --end-year 2025 books today's 103
quarterly periods; each invested row's holdings string names every booked name, and --full-holdings adds
holdings_full, every booked name in that order, changing nothing else; check N keys an NVDR line to its ordinary
line, which holds the slot, and the NVDR stands in for an ordinary line with no entry bar; check M reads INFL.BK's
FY rows, which carry no ROE, so its inflated cap fails the floor at every rebalance and it is never held; the SAO
company's preferred line stands in for its ordinary line in every invested book. The results file carries data_checks,
universe_rules, signal_guard (its count on every rebalance, equal in drop and ltp) and cost_model after
isin_dedup (the tiers at 35 baht a dollar on the "trades" convention, its keys in the order function, fx_per_usd,
turnover, and null on --no-costs), the fetch asks for no
MTUM, entry_unpriced and the data checks' counts on each row, and in each holdings_full item the name's one-way
tier (cost_rate); the rows reproduce from the items under the trades convention, the first paying its entries
only, and a no-costs run never calls the cost walk. Costs, with run_backtest's screen and prices
stubbed: a JPX, an LSE and a KLS cap pay the tier at their leg's FX rate, and a name with no cap the top one; with
--full-holdings, each name of a book across two tiers records its own rate; and costs.FX_PER_USD has a rate for
every exchange --global runs.

The trades convention on run_backtest itself (costs.trade_costs charges each series the trades its books make):
nine quarters with every row typed here, in drop, ltp and both. Two tiers with unequal returns; a rebalance with
an entry, an exit and a carried name on a new tier; cash for too few buyable at entry, the short book on another
tier; re-entry; a quarter where the drop series books no name and the LTP series books a fill; cash for too few
screened; an invested quarter, a quarter where neither series books a name, and a last invested quarter straight
after it, which enters from an empty position and pays no sale. A cash quarter's sale lands on each series' own
last invested row, both's halves equal the single runs, a no-costs run never calls the walk, and a costed row has
the keys of the same row run without costs. The same run with two more quarters ends in cash: a quarter where the
drop series books one name and the LTP series two, one of them a name carried into its fill, so each series' row
shows whose book it was charged on; then a cash quarter, whose sale lands on the last invested row though no
quarter follows. A flat quarter whose only cost is float noise stores 0.0, not -0.0, and one whose only cost is its
drift back to equal weight (under 0.0001, on a mean of 0.0) pays it, in drop and in each of both's two series. The
--verbose line says its return and its excess are before costs when costs are on. A value the walk refuses (a
price that is not a number, as an exit bar and as an LTP fill) stops the run with a message that names the
quarter's rebalance date and the series' key, the row after a cash quarter and the LTP series among them. The quarterly
run window, with test_latest_window_topics' 2026 bars: pinned to 2026-11-06, the last row is the partial
2026-10-01 period, its window exits and the benchmark's priced on the end date's close, a name with no bar after
2026-10-30 filled at its last bar, CAGR over span_years and the run window's keys written; main() takes
--end-date, and --end-year 2025 books the 103 periods. Their mutants must trip these checks.

The gate's legs: scripts/gate_legs/wave3.json holds one 52-week-high leg per --global preset and no other, ASX and SAO
among them, each pinned to --end-year 2025, ending with --full-holdings (a leg without it, with it before its
exchange args, with it twice or with an argument after it must fail on the leg's own message) and installed as the
file the global run writes.

The live screen (screen.py) runs the backtest's fetch and screen on one date with data checks on, check M's server
query (the one reading every FY row over the floor) standing in as cap_check rows: on a JPX leg a J-REIT (ur-4)
and a fund (the semi-join) stay out, the floor reads cap_used (a cap on the floor once divided is out, a unit over
it in, an ineligible row out), ties go to the larger cap_used, then the symbol; on NYSE a day after COG's last own
day CLR ranks alone, and 130 days after GETY's first own day GETY ranks on its own bars (ur-4's line start and
lookback clip); main() prints the book on today's leg as CSV and as a table, the fetch's progress on stderr. Its
mutants must trip these checks.

Usage:
    python scripts/test_52wh_selection.py [repo root]
"""
import ast
import contextlib
import io
import json
import os
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "52-week-high", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
import data_utils as du   # noqa: E402
from cli_utils import EXCHANGE_PRESETS, get_mktcap_threshold   # noqa: E402
from costs import get_fx_per_usd   # noqa: E402
import cost_conventions   # noqa: E402
import test_data_checks as tdc   # noqa: E402
import test_latest_window_topics as tlw   # noqa: E402  (its 2026 bars, pinned window and partial period)

D = date(2020, 4, 1)                    # the rebalance date R; bars are keyed by days from it
PREV = date(2020, 1, 1)                 # the rebalance date before R: its fetch window reaches back past R-390
EX = "NSE"                              # a leg with no listing rule
FLOOR = 5e8
FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)
AT_CUTOFF, IN_LAG = D - timedelta(days=45), D - timedelta(days=44)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src, path=PATH):
    mod = types.ModuleType("wh52_selection")
    mod.__file__ = path
    exec(compile(src, path, "exec"), mod.__dict__)
    return mod


def flat(n, px=100.0, end=0, step=1):
    """n traded bars `step` days apart, the last on day `end`: {day: [adjClose, close, high, volume]}, all at px."""
    return {end - step * i: [px, px, px, 1e6] for i in range(n)}


def bar(bars, day, **cols):
    """bars with one bar's adj / close / high / vol replaced."""
    out = {k: list(v) for k, v in bars.items()}
    for i, c in enumerate(("adj", "close", "high", "vol")):
        if c in cols:
            out[day][i] = cols[c]
    return out


def untraded(bars, days, **cols):
    """bars with zero volume (and any adj / close / high given) on each of `days`."""
    for day in days:
        bars = bar(bars, day, vol=0.0, **cols)
    return bars


def line(bars, cap=1e10, isin=None, filings=None, after=((1, 5.0),), dup=(), at=D, **profile):
    """One listing. `after`: (day, adjClose) bars past R, each closing on a new high the signal must not read.
    Bars are keyed by days from `at` (default D). profile: its profile row's other columns (companyName, isFund,
    industry ...)."""
    rows = [(k, *v) for k, v in bars.items()] + [(k, adj, 1000.0, 1000.0, 1e6) for k, adj in after] + list(dup)
    return {"bars": rows, "filings": filings or [(FY19, cap)], "isin": isin, "at": at, "profile": profile}


SIGNAL = {
    # the defect: a payer on its high (adjClose half its close) against a non-payer 8% under an intraday high
    "PAYER": line({k: [50.0, 100.0, 100.0, 1e6] for k in range(-119, 1)}, cap=9e10),
    "NONPAYER": line(bar(bar(flat(120, 90.0), -60, high=100.0), 0, adj=92.0, close=92.0, high=92.5)),
    # went ex-dividend inside the window: 3% under its high on price, on it once dividends are added back
    "EXDIV": line({k: [97.0, 100.0, 100.0, 1e6] if k < -59 else [97.0, 97.0, 97.0, 1e6] for k in range(-119, 1)}),
    # signal bar: the bar on R is read (not R-1's), R-10 is the oldest allowed, R-11 is stale
    "ON_R": line(bar(bar(flat(120), -1, adj=80.0, close=80.0), 0, adj=95.0, close=95.0)),
    "EDGE_FLOOR": line(bar(flat(120, end=-10), -10, adj=85.0, close=85.0)),
    "STALE": line(bar(flat(120, end=-11), -11, adj=99.0, close=99.0)),
    # a signal bar with no usable close drops the name: no falling back to the bar before
    "ZERO_CLOSE": line(bar(flat(120), 0, close=0.0)),
    "NULL_CLOSE": line(bar(flat(120), 0, close=None)),
    # two bars on R: the larger close is read
    "DUP": line(bar(flat(120), 0, adj=90.0, close=90.0, high=90.0), dup=[(0, 99.0, 99.0, 99.0, 1e6)]),
    # the signal bar is the latest bar with volume > 0: a zero-volume bar on R falls back to R-1, not R-2
    "ZV_ON_R": line(bar(bar(untraded(flat(120), [0], adj=80.0, close=80.0), -1, adj=96.0, close=96.0),
                        -2, adj=90.0, close=90.0)),
    # last traded bar on R-10 counts, on R-11 the line is skipped, though zero-volume bars fill the span
    "TV_EDGE": line(untraded(bar(flat(120), -10, adj=86.0, close=86.0), range(-9, 1))),
    "TV_STALE": line(untraded(bar(flat(120), -11, adj=99.0, close=99.0), range(-10, 1))),
    # a holiday-style row on R, spiked and untraded, is never the signal
    "HOLIDAY": line(bar(untraded(flat(120), [0], adj=109.0, close=109.0, high=109.0), -1, adj=94.0, close=94.0)),
    # traded inside the span, last bars untraded: ranks on the traded bar's close
    "ZV_TAIL": line(untraded(bar(flat(118, end=-2), -5, adj=93.0, close=93.0), (-4, -3, -2),
                             adj=70.0, close=70.0, high=70.0)),
    # NULL volume is not traded either
    "NULL_VOL": line(bar(bar(flat(120), 0, adj=70.0, close=70.0, vol=None), -1, adj=98.0, close=98.0)),
    # the window ends at the signal bar: its 252nd bar back counts, its own high counts, the next bar's does not
    "END_252": line({**bar(flat(252, end=-1), -252, high=200.0), 0: [100.0, 100.0, 100.0, 0.0]}, cap=1.5e10),
    "END_NEXT": line({**bar(flat(120, end=-1), -1, high=125.0), 0: [100.0, 100.0, 150.0, 0.0]}),
    # every bar counts in the window: an untraded bar's high, and untraded bars toward the 100-bar floor
    "ZV_HIGH": line(untraded(flat(120), [-60], high=160.0)),
    "ZV_FLOOR": line(untraded(flat(100), range(-99, 0)), cap=5.5e10),
    # an untraded twin of the signal bar, zero or NULL volume, sorts after it and takes no frame slot
    "DUP_ZV": line(bar(flat(252), -251, high=200.0), cap=7.5e10, dup=[(0, 100.0, 100.0, 100.0, 0.0)]),
    "DUP_NV": line(bar(flat(252), -251, high=200.0), cap=6.5e10, dup=[(0, 100.0, 100.0, 100.0, None)]),
    # a locked limit-up on one share closes on its high: 1.0, ranked among the 1.0s by cap
    "LIMIT_UP": line(bar(bar(flat(120), -60, high=105.0), 0, adj=110.0, close=110.0, high=110.0, vol=1.0),
                     cap=8.5e10),
    # window: 252 rows (the 252nd back counts, the 253rd does not) inside 390 days (R-390 counts, R-391 does not)
    "W252": line(bar(flat(252), -251, high=200.0), cap=2e10),
    "W253": line(bar(flat(253), -252, high=200.0), cap=8e10),
    "SPARSE_IN": line(bar(flat(131, step=3), -390, high=200.0)),
    "SPARSE_OUT": line({**flat(130, step=3), -391: [100.0, 100.0, 200.0, 1e6]}, cap=7e10),
    # 100 bars are enough, 99 are not
    "H100": line(flat(100), cap=6e10),
    "H99": line(flat(99)),
    # a close over the window's high ranks as it reads: the screen sets no proximity cap (on ASX and SAO no ranked
    # ratio passes 1.0 and no ranked signal bar closes above its own high)
    "CAP_OUT": line(bar(flat(120, 1000.0), 0, adj=1101.0, close=1101.0)),
    # under the floor, and stale: the signal guard counts only the lines over the floor
    "SMALL_STALE": line(flat(120, end=-11), cap=FLOOR),
    # size floor on the latest filing at the 45-day cutoff
    "ABOVE": line(flat(120), cap=FLOOR + 1),
    "AT_FLOOR": line(flat(120), cap=FLOOR),
    "EDGE_LAG": line(flat(120), filings=[(AT_CUTOFF, 5e10)]),
    "IN_LAG": line(flat(120), filings=[(IN_LAG, 5e10)]),
    "LAGGED": line(flat(120), filings=[(FY19, 7e9), (IN_LAG, 1e5)]),
    "SHRANK": line(flat(120), filings=[(FY18, 9e9), (FY19, 1e5)]),
    # one security, two lines: the line buyable at the run's own entry bar is kept
    "OFF_A": line(bar(flat(120), 0, adj=5.0), cap=3e10, isin="X2", after=((1, 0.5),)),
    "OFF_B": line(bar(flat(120), 0, adj=0.5), cap=4e10, isin="X2", after=((1, 5.0),)),
    # ... and an entry bar on R+11 is in the cache
    "E11_A": line(flat(120), cap=1.5e10, isin="X3", after=()),
    "E11_B": line(flat(120), cap=2e10, isin="X3", after=((11, 5.0),)),
}
# 33 securities on their high for 30 slots, and one a tick under it with the largest cap of all
CUT = {f"K{i:02d}": line(flat(120), cap=i * 1e9) for i in range(4, 32)}
CUT.update({
    "K02": line(flat(120), cap=3e9),                # equal caps at the cut: symbol decides
    "K03": line(flat(120), cap=3e9),
    # the tie-break cap is the latest eligible filing's, not an older or a not-yet-usable one
    "K01": line(flat(120), filings=[(FY18, 5e12), (FY19, 1e9), (IN_LAG, 5e12)]),
    "NEAR": line(bar(flat(120), 0, adj=99.9, close=99.9), cap=1e12),
    # twin lines: the one under the 1.0 entry floor (on adjClose) loses and frees its slot
    "TWIN_A": line(flat(120), cap=9e11, isin="X1", after=((1, 0.99),)),
    "TWIN_B": line(flat(120), cap=8e11, isin="X1", after=((1, 1.0),)),
})
# Check M: the floor, the book's cap and the tie-break read cap_used, the FY cap over check M's divisor (none when
# the row is ineligible). CAP_RULES: symbol -> (rule, divisor), check M's row for the line's FY2019 cap.
CAPS = {
    "DIV_AT": line(flat(120), cap=FLOOR * 10),          # cap_used on the floor: out
    "DIV_OVER": line(flat(120), cap=FLOOR * 10 + 10),   # cap_used a unit over it: in
    "INELIG": line(flat(120), cap=1e12),                # ineligible: out
    "TIE_RAW": line(flat(120), cap=9e11),               # cap_used 9e9: after TIE_USED's 5e10, though its raw cap leads
    "TIE_USED": line(flat(120), cap=5e10),
    "INELIG_STALE": line(flat(120, end=-11), cap=1e12),   # ineligible with no signal bar: no event, as it isn't ranked
}
CAP_RULES = {"DIV_AT": ("cap_over_k", 10.0), "DIV_OVER": ("cap_over_k", 10.0), "INELIG": ("ineligible", None),
             "TIE_RAW": ("cap_over_k", 100.0), "INELIG_STALE": ("ineligible", None)}
CAP_EVENTS = [("DIV_AT", "cap_over_k"), ("INELIG", "ineligible")]   # over the floor on the raw cap, not on cap_used
SIGNAL_GUARD = {"no_traded_bar": 2, "short_history": 1}   # STALE and TV_STALE; H99


def signal_book(off, e11):
    """Proximity descending, then cap descending, then symbol."""
    return [("CAP_OUT", 1e10, 1101.0 / 1000.0), ("PAYER", 9e10, 1.0), ("LIMIT_UP", 8.5e10, 1.0), ("W253", 8e10, 1.0),
            ("SPARSE_OUT", 7e10, 1.0), ("H100", 6e10, 1.0), ("ZV_FLOOR", 5.5e10, 1.0),
            ("EDGE_LAG", 5e10, 1.0), off, e11, ("LAGGED", 7e9, 1.0), ("ABOVE", FLOOR + 1, 1.0),
            ("DUP", 1e10, 0.99), ("NULL_VOL", 1e10, 98.0 / 100.0), ("EXDIV", 1e10, 0.97),
            ("ZV_ON_R", 1e10, 96.0 / 100.0), ("ON_R", 1e10, 0.95), ("HOLIDAY", 1e10, 94.0 / 100.0),
            ("ZV_TAIL", 1e10, 93.0 / 100.0), ("NONPAYER", 1e10, 0.92), ("TV_EDGE", 1e10, 86.0 / 100.0),
            ("EDGE_FLOOR", 1e10, 0.85), ("END_NEXT", 1e10, 100.0 / 125.0), ("ZV_HIGH", 1e10, 100.0 / 160.0),
            ("DUP_ZV", 7.5e10, 0.5), ("DUP_NV", 6.5e10, 0.5), ("W252", 2e10, 0.5), ("END_252", 1.5e10, 0.5),
            ("SPARSE_IN", 1e10, 0.5)]


EXPECT = {
    "signal, entry next day": signal_book(("OFF_B", 4e10, 1.0), ("E11_B", 2e10, 1.0)),
    "signal, entry same day": signal_book(("OFF_A", 3e10, 1.0), ("E11_A", 1.5e10, 1.0)),
    "signal, first rebalance date": signal_book(("OFF_B", 4e10, 1.0), ("E11_B", 2e10, 1.0)),
    "ties at the cut": ([("TWIN_B", 8e11, 1.0)] + [(f"K{i:02d}", i * 1e9, 1.0) for i in range(31, 3, -1)]
                        + [("K02", 3e9, 1.0)]),
    "check M": [("TIE_USED", 5e10, 1.0), ("TIE_RAW", 9e9, 1.0), ("DIV_OVER", FLOOR + 1, 1.0)],
    "check M's events": CAP_EVENTS,
    "signal guard": {"keys": ["rule", "dropped", "by_reason", "by_period"], "dropped": 3, "by_reason": SIGNAL_GUARD,
                     "by_period": [{"rebalance_date": D.isoformat(), **SIGNAL_GUARD}]},
}


def sql(v):
    return "NULL" if v is None else f"'{v}'" if isinstance(v, str) else repr(v)


def values(rows):
    return ", ".join("(" + ", ".join(sql(v) for v in r) + ")" for r in rows)


PROFILE = (("isin", "VARCHAR"), ("isActivelyTrading", "BOOLEAN"), ("currency", "VARCHAR"), ("cik", "VARCHAR"),
           ("companyName", "VARCHAR"), ("isFund", "BOOLEAN"), ("isEtf", "BOOLEAN"), ("isAdr", "BOOLEAN"),
           ("industry", "VARCHAR"))


def seed(uni, ex):
    """The made-up warehouse on leg `ex`: profile (the isin map's and the universe rules' columns: each line named by
    its symbol, live, in the leg's currency, no fund or ADR flag unless its profile says), key_metrics, stock_eod."""
    src = duckdb.connect()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, "
                + ", ".join(f"{c} {t}" for c, t in PROFILE) + ")")
    src.execute("CREATE TABLE key_metrics(symbol VARCHAR, marketCap DOUBLE, dateEpoch BIGINT, period VARCHAR)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " high DOUBLE, volume DOUBLE)")
    base = {"isActivelyTrading": True, "currency": du.leg_quote_currency([ex]), "cik": None, "isFund": False,
            "isEtf": False, "isAdr": False, "industry": None}
    src.execute("INSERT INTO profile VALUES " + values(
        (s, ex, *({**base, "isin": x["isin"], "companyName": s, **x["profile"]}[c] for c, _ in PROFILE))
        for s, x in uni.items()))
    src.execute("INSERT INTO key_metrics VALUES "
                + values((s, cap, ep(d), "FY") for s, x in uni.items() for d, cap in x["filings"]))
    src.execute("INSERT INTO stock_eod VALUES " + values(
        (s, (x["at"] + timedelta(days=k)).isoformat(), ep(x["at"] + timedelta(days=k)), adj, close, high, vol)
        for s, x in uni.items() for k, adj, close, high, vol in x["bars"]))
    return src


def cache(mod, uni, dates, ex=EX, **kw):
    """The topic's own fetch over the made-up warehouse for `dates` (test_data_checks' fake client runs its SQL):
    it builds universe, metrics_cache, prices_cache and the isin map. kw: fetch_data_via_api's own (ur, dc ...)."""
    mod.remove_price_oscillations = lambda *a, **k: None
    with contextlib.redirect_stdout(io.StringIO()):
        return mod.fetch_data_via_api(tdc.FakeClient(seed(uni, ex)), [ex], dates, **kw)


def failures(src):
    try:
        mod = load(src)
        # [PREV, D]: bars older than R-390 are in the cache, as on every date but a run's first; [D]: the first
        sig, first, cut = cache(mod, SIGNAL, [PREV, D]), cache(mod, SIGNAL, [D]), cache(mod, CUT, [PREV, D])
        caps = cache(mod, CAPS, [PREV, D])
        caps.executemany("INSERT INTO cap_check (symbol, filing_epoch, rule, cap_div) VALUES (?, ?, ?, ?)",
                         [(s, ep(FY19), rule, div) for s, (rule, div) in CAP_RULES.items()])
        du.mark_nvdr_twins(caps)   # the twin key's columns, as a data-checks run has them
        dc, guard = du.DataChecks("on", mod.DATA_CHECKS), mod.SignalGuard()
        got = {"signal, entry next day": mod.screen_stocks(sig, D, FLOOR, offset_days=1, guard=guard),
               "signal, entry same day": mod.screen_stocks(sig, D, FLOOR, offset_days=0),
               "signal, first rebalance date": mod.screen_stocks(first, D, FLOOR, offset_days=1),
               "ties at the cut": mod.screen_stocks(cut, D, FLOOR, offset_days=1),
               "check M": mod.screen_stocks(caps, D, FLOOR, offset_days=1, dc=dc)}
        got["check M's events"] = sorted((e["symbol"], e["rule"]) for e in dc.events if e["check"] == "cap")
        b = guard.block()
        got["signal guard"] = {"keys": list(b), **{k: b[k] for k in ("dropped", "by_reason", "by_period")}}
        for con in (sig, first, cut, caps):
            con.close()
    except Exception as e:  # a mutant that breaks the module fails loudly
        return [f"error: {type(e).__name__}: {e}"]
    return [f"{k}: book {got[k]} != {want}" for k, want in EXPECT.items() if got[k] != want]


COLS = "SELECT symbol, trade_epoch, close, high,"
FROM = "            FROM prices_cache\n            WHERE symbol IN (SELECT symbol FROM eligible)"
FRAME = "ROWS BETWEEN 251 PRECEDING AND CURRENT ROW\n                ) AS high_52w"
FLOOR_BAR = "              AND trade_epoch >= {signal_floor_epoch}\n"
START_BAR = "              AND trade_epoch >= {start_epoch}\n"
START = "    start_epoch = utc_epoch(target_date - timedelta(days=HIGH_WINDOW_FETCH))\n"   # the screen's window start
FETCH_START = "fetch_start = d - timedelta(days=HIGH_WINDOW_FETCH)\n"                      # the fetch's
TARGET = "    target_epoch = utc_epoch(target_date)\n"
LAG = "cutoff_epoch = utc_epoch(target_date - timedelta(days=45))"
CUT_BY = "ORDER BY proximity_ratio DESC, marketCap DESC NULLS LAST, symbol\n"
WRAP = "FROM ({isin_dedup_sql(pool, target_date, offset_days, MIN_ENTRY_PRICE,"
TWIN_KEY = 'twin_key=(dc is not None and dc.on) or bool(UNIVERSE_RULES.get("class_twins")))'
DEDUP = WRAP + "\n                              " + TWIN_KEY + "})"
FLOOR_TEST = 'sql("km.cap_used > ?")'
FETCH_COLS = "adjClose, close, high, volume\n"                                   # the fetch SQL's, not the docstring's
FETCH_PASS = "          AND adjClose > 0\n          AND high > 0\n"
TRADED = "              AND volume > 0\n"                                                  # the signal bar's volume test
PICK = TRADED + ("            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch DESC, close DESC NULLS LAST,\n"
                 "                                       high_52w DESC NULLS LAST, row_count DESC) = 1\n")
LATEST = "            SELECT symbol, close, high_52w, row_count\n            FROM windowed\n"
VOL_BY = "volume DESC NULLS LAST"                                                       # same-day twins: traded first
FRAMES_BY = (f"high, {VOL_BY}\n                    " + FRAME + ",\n                COUNT(*) OVER (\n"
             f"                    PARTITION BY symbol ORDER BY trade_epoch, close, high, {VOL_BY}\n")
# The traded-signal-bar rule's mutants: each must trip a check on a wrong book, not on an error.
TRADED_MUTANTS = {
    "no volume test (the zero-volume bar is read)": (TRADED, ""),
    "zero volume counts as traded": (TRADED, TRADED.replace(">", ">=")),
    "one share is not traded": (TRADED, TRADED.replace("0", "1")),
    "NULL volume counts as traded": (TRADED, TRADED.replace("> 0", "IS DISTINCT FROM 0")),
    "a close on the high is no signal": (TRADED, TRADED.replace("\n", " AND close < high\n")),
    "the latest bar must have traded (naive)": (PICK, PICK.replace(TRADED, "").replace(") = 1\n", ") = 1 AND volume > 0\n")),
    "untraded bar read when nothing in the span traded": (PICK, PICK.replace(TRADED, "").replace(
        "ORDER BY trade_epoch DESC,", "ORDER BY volume > 0 DESC, trade_epoch DESC,")),
    "window ends at R, not the signal bar": (LATEST, LATEST.replace("high_52w,", "(SELECT arg_max(w.high_52w, w.trade_epoch)"
                                         " FROM windowed w WHERE w.symbol = windowed.symbol) AS high_52w,")),
    "window ends a bar after the signal bar": (FRAME, FRAME.replace("251 PRECEDING AND CURRENT ROW", "250 PRECEDING AND 1 FOLLOWING")),
    "signal bar's own high left out": (FRAME, FRAME.replace("251 PRECEDING AND CURRENT ROW", "252 PRECEDING AND 1 PRECEDING")),
    "high over traded bars only": ("MAX(high) OVER (", "MAX(high) FILTER (WHERE volume > 0) OVER ("),
    "rows counted over traded bars only": ("COUNT(*) OVER (", "COUNT(*) FILTER (WHERE volume > 0) OVER ("),
    "window over traded bars only": (FROM, FROM + " AND volume > 0"),
    "traded-only fetch": (FETCH_PASS, FETCH_PASS + "          AND volume > 0\n"),
    "zero-volume duplicate sorts before the traded bar (ascending volume)": (FRAMES_BY, FRAMES_BY.replace(" DESC NULLS LAST", "")),
    "NULL-volume duplicate sorts before the traded bar": (FRAMES_BY, FRAMES_BY.replace("NULLS LAST", "NULLS FIRST")),
}
MUTANTS = {
    # the old bug and its neighbours: any dividend adjustment on either side of the ratio
    "the old ratio, adjClose over the high": (COLS, COLS.replace("close,", "adjClose AS close,")),
    "the old ratio, through the fetch": (FETCH_COLS, FETCH_COLS.replace("close,", "adjClose AS close,")),
    "both sides on adjClose's basis": (FROM, FROM.replace(
        "prices_cache", "(SELECT p.symbol, p.trade_epoch, p.adjClose AS close,"
                        " p.high * p.adjClose / p.close AS high, p.volume FROM prices_cache p)")),
    "the high on adjClose's basis": ("MAX(high) OVER (", "MAX(high * adjClose / close) OVER ("),
    "highest close, not highest high": ("MAX(high) OVER (", "MAX(close) OVER ("),
    "no close in the fetch": (FETCH_COLS, FETCH_COLS.replace(" close,", "")),
    "no volume in the fetch": (FETCH_COLS, FETCH_COLS.replace(", volume", "")),
    # the signal bar: on or before R, at most 10 days old, picked before its close is tested
    "signal bar read to R+5 (the old look-ahead)": (TARGET, TARGET.replace(")", " + timedelta(days=5))", 1)),
    "signal bar a day late": (TARGET, TARGET.replace(")", " + timedelta(days=1))", 1)),
    "signal bar a day early": (TARGET, TARGET.replace(")", " - timedelta(days=1))", 1)),
    "stale signal bar allowed": (FLOOR_BAR, ""),
    "signal bar may be 11 days old": ("SIGNAL_TOLERANCE_DAYS = 10 ", "SIGNAL_TOLERANCE_DAYS = 11 "),
    "signal bar at most 9 days old": ("SIGNAL_TOLERANCE_DAYS = 10 ", "SIGNAL_TOLERANCE_DAYS = 9 "),
    "signal floor exclusive": (FLOOR_BAR, FLOOR_BAR.replace(">=", ">")),
    "close tested before the latest bar": (FLOOR_BAR, FLOOR_BAR + "              AND close > 0\n"),
    "close-passing-only fetch": (FETCH_PASS, FETCH_PASS + "          AND close > 0\n"),
    "no close test": ("AND s.close > 0)", "AND TRUE)"),
    "smaller close of two bars on R": ("ORDER BY trade_epoch DESC, close DESC NULLS LAST",
                                       "ORDER BY trade_epoch DESC, close ASC NULLS LAST"),
    # the window and the history floor, each one unit either side
    "window a bar short": (FRAME, FRAME.replace("251", "250")),
    "window a bar long": (FRAME, FRAME.replace("251", "252")),
    "window a day short": ("HIGH_WINDOW_FETCH = 390 ", "HIGH_WINDOW_FETCH = 389 "),
    "window a day long": ("HIGH_WINDOW_FETCH = 390 ", "HIGH_WINDOW_FETCH = 391 "),
    "window start exclusive": ("AND trade_epoch >= {start_epoch}", "AND trade_epoch > {start_epoch}"),
    # the start bound in the screen alone (all a continuous cache has) and in the fetch alone (a run's first date)
    "window a day long, screen only": (START, START.replace("FETCH)", "FETCH + 1)")),
    "window a day short, screen only": (START, START.replace("FETCH)", "FETCH - 1)")),
    "no window start in the screen": (START_BAR, ""),
    "window a day short, first fetch only": (FETCH_START, FETCH_START.replace("FETCH)", "FETCH - 1)")),
    "99 bars are enough": ("MIN_HISTORY_ROWS = 100 ", "MIN_HISTORY_ROWS = 99 "),
    "101 bars needed": ("MIN_HISTORY_ROWS = 100 ", "MIN_HISTORY_ROWS = 101 "),
    "history floor exclusive": ("< {MIN_HISTORY_ROWS}", "<= {MIN_HISTORY_ROWS}"),
    "a proximity cap": ("AND s.close > 0)", "AND s.close > 0 AND s.close / s.high_52w <= 1.1)"),
    # the signal guard: the unranked lines over the floor, by reason, and check M's events on ranked lines only
    "signal guard not recorded": ("guard.record(con.execute(", "(lambda *a: None)(con.execute("),
    "the guard counts lines under the floor": ("_signaled(candidates, 'IS NOT NULL')",
                                               "_signaled(without_lines_sql(sql('? IS NOT NULL'), left_out), 'IS NOT NULL')"),
    "a stale line counted as short history": ("THEN 'no_traded_bar'", "THEN 'short_history'"),
    "check M's events on unranked lines": ("_signaled(without_lines_sql(sql(CAP_FAIL_TEST, events=True), left_out))",
                                           "without_lines_sql(sql(CAP_FAIL_TEST, events=True), left_out)"),
    # the size floor: latest filing at the 45-day cutoff first, then the test on cap_used (check M)
    "size floor inclusive": (FLOOR_TEST, 'sql("km.cap_used >= ?")'),
    "no size floor": (FLOOR_TEST, 'sql("? IS NOT NULL")'),
    "the floor on the raw cap": (FLOOR_TEST, 'sql("km.marketCap > ?")'),
    "the book and the tie-break on the raw cap": ("SELECT km.symbol, km.cap_used AS marketCap",
                                                  "SELECT km.symbol, km.marketCap AS marketCap"),
    "check M's events not recorded": ("dc.caps(con, ", "(lambda *a: None)(con, "),
    "size test before latest": ("            WHERE m.filing_epoch <= ?\n",
                                "            WHERE m.filing_epoch <= ? AND m.marketCap > 500000000\n"),
    "no lag": (LAG, "cutoff_epoch = utc_epoch(target_date)"),
    "lag a day short": (LAG, LAG.replace("45", "44")),
    "lag a day long": (LAG, LAG.replace("45", "46")),
    # the cut: proximity, then larger cap, then symbol, 30 names
    "ties by symbol only (the old cut)": (CUT_BY, "ORDER BY proximity_ratio DESC, symbol\n"),
    "smaller cap first": (CUT_BY, CUT_BY.replace("marketCap DESC", "marketCap ASC")),
    "cap before proximity": (CUT_BY, "ORDER BY marketCap DESC NULLS LAST, proximity_ratio DESC, symbol\n"),
    "equal caps by symbol descending": (CUT_BY, CUT_BY.replace("symbol\n", "symbol DESC\n")),
    "farthest from the high first": (CUT_BY, CUT_BY.replace("proximity_ratio DESC", "proximity_ratio ASC")),
    "29 names": ("MAX_STOCKS = 30 ", "MAX_STOCKS = 29 "),
    "31 names": ("MAX_STOCKS = 30 ", "MAX_STOCKS = 31 "),
    # the same-security dedup: gone, after the cut, on the wrong entry bar, or blind to the R+11 bar
    "no dedup": (DEDUP, "FROM ({pool})"),
    "cut before dedup": (WRAP, WRAP.replace("pool,", "'SELECT * FROM (' + pool + ') "
                                            + CUT_BY.strip() + " LIMIT 30',")),
    "entry offset not passed": (WRAP, WRAP.replace("target_date, offset_days,", "target_date, 1,")),
    "entry floor a cent lower": ("MIN_ENTRY_PRICE = 1.0 ", "MIN_ENTRY_PRICE = 0.99 "),
    "entry floor a cent higher": ("MIN_ENTRY_PRICE = 1.0 ", "MIN_ENTRY_PRICE = 1.01 "),
    "prices cached to R+10": ("win.fetch_end(d, 11)", "win.fetch_end(d, 10)"),
    **TRADED_MUTANTS,
}


# ---------------------------------------------------------------- the run on the synthetic SET leg

TOPIC = "52-week-high"
GRID = du.generate_rebalance_dates(2000, 2025, "quarterly")   # --end-year 2025: 104 dates, 103 periods
_LEGS = {}


def set_leg():
    """test_data_checks' synthetic SET leg as a 52-week-high leg, built once (runs only read it)."""
    if "leg" not in _LEGS:
        _LEGS["leg"] = tdc.wh52_source()
    return _LEGS["leg"]


def strip_full(res):
    return {**res, "period_data": [{k: v for k, v in r.items() if k != "holdings_full"} for r in res["period_data"]]}


COST_MODEL = {"function": "costs.tiered_cost", "fx_per_usd": 35.0, "turnover": "trades"}   # the SET leg: 35 baht a dollar
SET_RATE = 0.005   # the synthetic caps, about 5e10 baht, are about 1.4B dollars: the 0.5% tier (0.1% unconverted)
# the signal guard on the SET leg: WH52_UNRANKED's two lines, whose sparse bars never make 100, at each rebalance
# but the first, short_history when a bar falls in the 10 days to R (71 each) and no_traded_bar otherwise (31 each);
# and DLST.BK's four quarters from its delisting until its last bar leaves the lookback (no_traded_bar)
SET_GUARD = {"no_traded_bar": 66, "short_history": 142}
NO_ROE = "INFL.BK"   # an inflated FY cap on rows with no ROE (wh52_source): check M must read them
ROW_KEYS = ("entry_unpriced","entry_bars_moved", "exit_bars_moved", "entries_lost", "exits_lost", "twins_collapsed",
            "cap_excluded", "factor_flags")


def cost_failures(res, rate):
    """Each holdings_full item's cost_rate is `rate` (the name's one-way tier; None on a no-costs run). A costs-on
    file passes scripts/cost_conventions.py's check on the trades convention, every row reproducing from its
    items, and its first row pays its entries only: one tier, so `rate`. A no-costs file is refused there, and
    each of its rows is the mean of its items."""
    rows = res["period_data"]
    for r in rows:
        got = {str(h.get("cost_rate", "missing")) for h in r.get("holdings_full") or []}
        if got - {str(rate)}:
            return [f"{r['rebalance_date']}: cost_rate {sorted(got)}, want {rate}"]
    mean = lambda r: sum(h["return"] for h in r["holdings_full"]) / len(r["holdings_full"])
    try:
        got = cost_conventions.check(res)
    except cost_conventions.Refusal as e:
        if rate is not None:
            return [f"the cost check refuses the file: {e}"]
        off = [r["rebalance_date"] for r in rows if r.get("holdings_full") and abs(mean(r) - r["portfolio_return"]) > 2e-6]
        return [f"rows that are not the mean of their items: {off[:3]}"] if off else []
    if rate is None:
        return ["the cost check reads a no-costs file"]
    out = []
    if (got["convention"], got["rows_checked"], got["bad"]) != ("trades", len(rows), []):
        out.append(f"the cost check: convention {got['convention']}, {got['rows_checked']} rows, faults {got['bad'][:3]}")
    if not rows[0].get("holdings_full") or abs(mean(rows[0]) - rate - rows[0]["portfolio_return"]) > 2e-6:
        out.append(f"the first row {rows[0]['portfolio_return']} is not its items less their entries at {rate}")
    return out


class WalkCalled(Exception):
    """Raised by the stand-in for costs.trade_costs on a no-costs run."""


def no_walk(*_a, **_k):
    raise WalkCalled()


def results_failures(src):
    """run_single on the synthetic SET leg, --end-year 2025: 103 quarterly periods from 2000-01-01; each invested
    row's holdings names every booked name (books of more than 10, so a cut shows); --full-holdings adds
    holdings_full, every booked name in the holdings' order, and changes nothing else. The results file: after
    isin_dedup come data_checks, universe_rules, signal_guard (SET_GUARD) and cost_model (then the LTP keys), the
    fetch asks for no MTUM, cost_model the tiers at 35
    baht a dollar on the trades convention (COST_MODEL, its keys in that order), the rate run_single passes to
    run_backtest, and null on a no-costs run;
    each row carries entry_unpriced (UNP.BK, which ranks with no entry bar, on every priced row; null when the
    screen held cash) and the data checks' counts; each holdings_full item records its name's one-way tier
    (cost_rate: 0.5% here, 0.1% at one baht a dollar; null on a no-costs run), and each row's return is its items'
    less the cost of its trades (cost_failures); the no-costs run never calls the cost walk."""
    try:
        mod = load(src)
        fx = []
        run_bt = mod.run_backtest
        mod.run_backtest = lambda *a, **k: fx.append(k.get("fx")) or run_bt(*a, **k)
        with tempfile.TemporaryDirectory() as tmp:
            plain, sqls = tdc.run_topic(TOPIC, set_leg(), "ltp", "on", tmp, mod=mod)
            full = tdc.run_topic(TOPIC, set_leg(), "ltp", "on", tmp, mod=mod, full_holdings=True)[0]
            walk, mod.trade_costs = getattr(mod, "trade_costs", None), no_walk
            try:
                free = tdc.run_topic(TOPIC, set_leg(), "ltp", "on", tmp, mod=mod, full_holdings=True,
                                     use_costs=False, tag=".free")[0]
            except WalkCalled:
                return ["a no-costs run calls the cost walk"]
            finally:
                mod.trade_costs = walk
            drop = tdc.run_topic(TOPIC, set_leg(), "drop", "on", tmp, mod=mod)[0]
    except Exception as e:  # reported as an error: main() does not count it as a caught mutant
        return [f"error: {type(e).__name__}: {e}"]
    out = []
    if fx != [35.0] * 4:
        out.append(f"run_single passes fx {fx} to run_backtest, want 35.0 on every run")
    if any("MTUM" in s for s in sqls):
        out.append("the fetch asks for MTUM, which nothing reads")
    g = plain.get("signal_guard") or {}
    if drop.get("signal_guard") != g or list(g) != ["rule", "dropped", "by_reason", "by_period"] or \
            g["by_reason"] != SET_GUARD or [p["rebalance_date"] for p in g["by_period"]] != [d.isoformat() for d in GRID[:-1]]:
        out.append(f"signal_guard {g.get('by_reason')}, want {SET_GUARD} over every rebalance, equal in drop and ltp")
    for name, res in (("ltp", plain), ("no costs", free), ("drop", drop)):
        keys = list(res)
        tail = keys[keys.index("isin_dedup") + 1:] if "isin_dedup" in keys else keys
        want = (["data_checks", "universe_rules", "signal_guard", "cost_model"]
                + (["exit_method", "ltp"] if name != "drop" else []))
        if tail != want:
            out.append(f"{name}: keys after isin_dedup {tail}, want {want}")
        cm = res.get("cost_model", "missing")   # its keys' order too: the order is the file's bytes
        if cm != (None if name == "no costs" else COST_MODEL) or (cm and list(cm) != list(COST_MODEL)):
            out.append(f"{name}: cost_model {cm}, want {COST_MODEL} in that order (null on a no-costs run)")
    out += cost_failures(full, SET_RATE) + [f"no costs: {f}" for f in cost_failures(free, None)]
    for r in plain["period_data"]:
        priced = r["entry_buyable"] is not None
        if [k for k in ROW_KEYS if k not in r] or (r["entry_unpriced"] is not None) != priced or \
                (priced and (tdc.WH52_UNPRICED not in r["entry_unpriced"]
                             or len(r["entry_unpriced"]) != r["screened"] - r["entry_buyable"])):
            out.append(f"{r['rebalance_date']}: row keys {[k for k in ROW_KEYS if k not in r]}, entry_unpriced "
                       f"{r.get('entry_unpriced', 'missing')}")
            break
    rows = plain["period_data"]
    if [r["rebalance_date"] for r in rows] != [d.isoformat() for d in GRID[:-1]] or rows[-1]["exit_date"] != "2025-10-01":
        out.append(f"grid {rows[0]['rebalance_date']} to {rows[-1]['exit_date']}, {len(rows)} periods, want 103")
    held = [r for r in rows if r["stocks_held"]]
    if not held or max(r["stocks_held"] for r in held) <= 10:
        out.append("no book holds more than 10 names: a cut holdings string would not show")
    for r in held:
        names = r["holdings"].split(",")
        if len(names) != r["stocks_held"] or len(set(names)) != len(names) or "..." in r["holdings"]:
            out.append(f"{r['rebalance_date']}: holdings {r['holdings']!r} for {r['stocks_held']} held")
            break
    if any("holdings_full" in r for r in rows):
        out.append("holdings_full written without --full-holdings")
    if strip_full(full) != plain:
        out.append(f"--full-holdings changed something else: {tdc.first_diff(strip_full(full), plain)}")
    for r in full["period_data"]:
        names = [h["symbol"] for h in r.get("holdings_full") or []]
        if names != (r["holdings"].split(",") if r["stocks_held"] else []):
            out.append(f"{r['rebalance_date']}: holdings_full {names}, holdings {r['holdings']!r}")
            break
    # check M reads INFL.BK's FY rows, which carry no ROE: its inflated cap fails the floor at every rebalance
    held_at, invested = tdc.held_dates(rows)
    infl = [e for e in (plain.get("data_checks") or {}).get("events", [])
            if e["symbol"] == NO_ROE and e["check"] == "cap"]
    if held_at(NO_ROE) or len(infl) != len(rows):
        out.append(f"{NO_ROE} (no ROE) held at {len(held_at(NO_ROE))} rebalances, {len(infl)} cap events of {len(rows)}")
    # check N: an NVDR line shares its ordinary line's security (twin_key on the dedup), so TWN.BK holds the slot
    # and STD-R.BK stands in for STD.BK, which ranks with no entry bar
    if held_at("TWN-R.BK") or held_at("STD.BK") or held_at("TWN.BK") != invested or held_at("STD-R.BK") != invested:
        out.append(f"NVDR twins: TWN-R.BK held at {sorted(held_at('TWN-R.BK'))[:3]}, STD-R.BK at "
                   f"{len(held_at('STD-R.BK'))} and TWN.BK at {len(held_at('TWN.BK'))} of {len(invested)} invested")
    # ur-4: its block, and the SAO company's preferred line standing in for its ordinary line, which has no bar
    if (plain.get("universe_rules") or {}).get("version") != "ur-4":
        return out + [f"universe_rules {(plain.get('universe_rules') or {}).get('version')}, want ur-4"]
    return out + tdc.stand_in_failures(plain)


RESULT_MUTANTS = {
    "holdings cut to 10 names": ('"holdings": ",".join(sym for sym, _, _ in clean),',
                                 '"holdings": ",".join(sym for sym, _, _ in clean[:10]),'),
    "--full-holdings ignored": ("DataChecks(data_checks, DATA_CHECKS, full_holdings=full_holdings)",
                                "DataChecks(data_checks, DATA_CHECKS, full_holdings=False)"),
    "no twin key on the dedup": (TWIN_KEY, "twin_key=False)"),
    "class twins not marked": ("    ur.mark_twins(con)", "    None"),
    "stand-ins not recorded": ("ur.stand_ins([s for s, _, _ in picked], target_date)", "None"),
    "costs ignore the leg's FX rate": ("tiered_cost(mc, fx_per_usd=fx)", "tiered_cost(mc)"),
    "costs at one unit a dollar": ("fx = get_fx_per_usd(exchanges)", "fx = 1.0"),
    "run_single doesn't pass the FX rate": ("win=win, fx=fx,", "win=win,"),
    "a name with no cap charged nothing": ("tiered_cost(mc, fx_per_usd=fx) if use_costs else None",
                                           "(tiered_cost(mc, fx_per_usd=fx) if mc else 0.0) if use_costs else None"),
    "cost_model not written": (
        '"cost_model": {"function": "costs.tiered_cost", "fx_per_usd": fx, "turnover": "trades"} if use_costs else None',
        '"cost_model": None'),
    "cost_model written on a no-costs run": ('"turnover": "trades"} if use_costs else None', '"turnover": "trades"}'),
    "cost_model without its convention": (', "turnover": "trades"}', "}"),
    "cost_model's keys in another order": (
        '{"function": "costs.tiered_cost", "fx_per_usd": fx, "turnover": "trades"} if use_costs',
        '{"turnover": "trades", "function": "costs.tiered_cost", "fx_per_usd": fx} if use_costs'),
    "cost_rate not written": ("cost_rates=rates)", "cost_rates=None)"),
    "every item records the first name's rate": ("cost_rates=rates)",
                                                 "cost_rates={s: next(iter(rates.values())) for s in rates})"),
    "data_checks after the other blocks": (
        '    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both\n',
        ""),
    "no unpriced names": ("unpriced = sorted(s for s in symbols if not entry_usable(entry_prices.get(s), MIN_ENTRY_PRICE))",
                          "unpriced = []"),
    "the signal guard's block not written": ('        "signal_guard": gb,', ""),
    "MTUM fetched again": ("            symbol IN ({bench_list})\n",
                           "            symbol IN ({bench_list})\n            OR symbol = 'MTUM'\n"),
    "check M on the rows with an ROE only": ("require_roe=False", "require_roe=True"),
}


# run_backtest on one leg, the screen and prices stubbed: ten holdings at one market cap (cap_used) in the leg's
# currency, each up 10% before costs. The cost tiers are in USD (over 10B 0.1%, over 2B 0.3%, else 0.5%), converted
# at the leg's rate (costs.get_fx_per_usd); a name with no cap pays the top rate. One period: the book pays its
# entries, ten tenths at the one-way rate, and no sale at the run's end.
COST_ENTRY, COST_EXIT, COST_RAW = date(2020, 4, 1), date(2020, 7, 1), 0.10
COST_CASES = [   # (leg, market cap in its currency, one-way rate): JPX at 150 yen, LSE at 0.79 pounds a dollar
    ("JPX", 1e12, 0.003),    # 1 trillion yen is about 6.7B dollars: the 0.3% tier, where 1e12 > 10B would read 0.1%
    ("LSE", 8e9, 0.001),     # 8B pounds is about 10.1B dollars: the 0.1% tier, where 8e9 < 10B would read 0.3%
    ("KLS", 2e10, 0.003),    # 20B ringgit is about 4.5B dollars at 4.46: the 0.3% tier, where 2e10 > 10B would read 0.1%
    ("NYSE", None, 0.005),   # no cap: the 0.5% tier, where the topic charged nothing before
]


class FakeCon:
    """A connection run_backtest must not read: the screen, prices and benchmark are stubbed."""

    def __getattr__(self, name):
        raise AssertionError(f"run_backtest read the connection ({name})")


def cost_unit_failures(src):
    """A holding's cost tier reads its cap_used against the USD tiers converted at the leg's FX rate."""
    out = []
    names = [f"C{i:02d}" for i in range(10)]
    for exchange, cap, rate in COST_CASES:
        try:
            mod = load(src)
            mod.screen_stocks = lambda *a, cap=cap, **k: [(s, cap, 1.0) for s in names]
            mod.get_prices = lambda con, symbols, d, **k: {s: 10.0 * (1 + COST_RAW * (d == COST_EXIT)) for s in symbols}
            mod.get_benchmark_return = lambda *a, **k: 0.05
            got = mod.run_backtest(FakeCon(), [COST_ENTRY, COST_EXIT], 0.0, use_costs=True,
                                   fx=get_fx_per_usd([exchange]))
        except Exception as e:
            return out + [f"error: {type(e).__name__}: {e}"]
        want = round(COST_RAW - rate, 6)
        if got[0]["portfolio_return"] != want:
            out.append(f"costs: {exchange} at {cap} books {got[0]['portfolio_return']}, want {want} (the {rate:.1%} tier)")
    return out


# --full-holdings on a book whose caps span two tiers (NYSE, dollars): five names over 10B (0.1%), five at 2B to 10B
# (0.3%), each with its entry and exit bars in a prices cache
TIER_BOOK = {f"T{i:02d}": (2e10 if i < 5 else 5e9) for i in range(10)}
TIER_RATE = {2e10: 0.001, 5e9: 0.003}


def cost_rate_failures(src):
    """run_backtest with --full-holdings on TIER_BOOK, the screen and the benchmark stubbed, each name up 10%: each
    holdings_full item's cost_rate is its own name's tier, and the row's return is the items' less their entries,
    a tenth of the book at each name's rate (the mean rate, 0.2%)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " high DOUBLE, volume DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?, ?, 1e6)",
                    [(s, ep(d + timedelta(days=1)), px, px, px) for s in TIER_BOOK
                     for d, px in ((COST_ENTRY, 10.0), (COST_EXIT, 11.0))])
    try:
        mod = load(src)
        mod.screen_stocks = lambda *a, **k: [(s, cap, 1.0) for s, cap in TIER_BOOK.items()]
        mod.get_benchmark_return = lambda *a, **k: 0.05
        dc = du.DataChecks("uncapped", mod.DATA_CHECKS, full_holdings=True)
        row = mod.run_backtest(con, [COST_ENTRY, COST_EXIT], 0.0, use_costs=True, fx=1.0, dc=dc)[0]
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    finally:
        con.close()
    want = {s: TIER_RATE[cap] for s, cap in TIER_BOOK.items()}
    got = {h["symbol"]: h.get("cost_rate", "missing") for h in row.get("holdings_full") or []}
    out = [] if got == want else [f"--full-holdings cost_rate {got}, want {want}"]
    net = round(COST_RAW - sum(want.values()) / len(want), 6)
    if row["portfolio_return"] != net:
        out.append(f"a book across two tiers returns {row['portfolio_return']}, want {net}")
    return out


def run_failures(src):
    return cost_unit_failures(src) + cost_rate_failures(src) + results_failures(src)


# The trades convention on run_backtest itself: nine quarters, the screen, the benchmark and the LTP fills stubbed
# and the prices in a cache, with MIN_STOCKS at 2. Each series' cost comes from costs.trade_costs over its books: a
# rebalance pays each name's gap between its drifted weight and the new equal weight at the new quarter's rate and
# sells a leaver at the rate it last carried; a cash quarter's sale lands on the series' last invested row, and so
# does the sale of a quarter whose series books no name.
WALK_DATES = [date(2020, 1, 1), date(2020, 4, 1), date(2020, 7, 1), date(2020, 10, 1), date(2021, 1, 1),
              date(2021, 4, 1), date(2021, 7, 1), date(2021, 10, 1), date(2022, 1, 1), date(2022, 4, 1)]
WALK_CAP = {0.005: 1e9, 0.003: 5e9, 0.001: 2e10}   # a cap on each tier, at one unit a dollar
WALK_SCREEN = [   # each quarter's screen, {symbol: its one-way rate}
    {"A": 0.005, "B": 0.001},
    {"A": 0.003, "C": 0.005},
    {"D": 0.001, "E": 0.001},              # E has no bar: one buyable of two screened
    {"A": 0.003, "D": 0.001, "F": 0.005},
    {"D": 0.001, "G": 0.003},              # neither has an exit bar; G's last traded price fills under ltp
    {"H": 0.005},                          # one screened, fewer than MIN_STOCKS
    {"A": 0.001, "H": 0.005},
    {"J": 0.003, "K": 0.001},              # neither has an exit bar or a fill: both series book no name
    {"A": 0.003, "B": 0.001},
]
# each line's bar the day after a rebalance date, by the date's index: the entry there and the exit of the quarter before
WALK_BARS = {"A": {0: 10.0, 1: 12.0, 2: 13.2, 3: 20.0, 4: 22.0, 6: 10.0, 7: 10.5, 8: 10.0, 9: 12.0, 10: 13.2},
             "B": {0: 10.0, 1: 10.0, 8: 10.0, 9: 10.0}, "C": {1: 10.0, 2: 12.999992}, "D": {2: 10.0, 3: 10.0, 4: 8.0},
             "F": {3: 10.0, 4: 14.0}, "G": {4: 10.0}, "H": {6: 10.0, 7: 11.5}, "J": {7: 10.0}, "K": {7: 10.0}}
# quarter -> {symbol: (last traded price, its bar)}
WALK_FILLS = {4: {"G": (8.0, "2021-02-15")}, 9: {"B": (8.0, "2022-05-16")}}
# Each row, derived from the rule:
#  0 A +20% at 0.5%, B 0% at 0.1%: mean 0.10, entries 1/2 x 0.005 + 1/2 x 0.001 = 0.003: 0.097.
#    End weights A 1.2/2.2 = 6/11, B 5/11.
#  1 A +10% now at 0.3%, C +29.99992% at 0.5%: mean 0.1999996. A trades |1/2 - 6/11| = 1/22 at 0.003 (0.000136),
#    C enters at 1/2 x 0.005 (0.0025), B sells 5/11 at its 0.001 (0.000455): 0.003091. Quarter 2 is cash, so the
#    book's sale lands here: A 1.1/2.3999992 x 0.003 + C 1.2999992/2.3999992 x 0.005 = 0.001375 + 0.002708 =
#    0.004083. 0.1999996 - 0.0071742 = 0.1928254: 0.192825. Taken off the rounded mean, 0.2, it reads 0.192826.
#  2 cash: D alone is buyable.
#  3 A +10% at 0.3%, D -20% at 0.1%, F +40% at 0.5%: mean 0.10, entries (0.003 + 0.001 + 0.005)/3 = 0.003: 0.097.
#    End weights A 1.1/3.3, D 0.8/3.3, F 1.4/3.3. Under drop quarter 4 books no name, so the sale lands here:
#    (1.1 x 0.003 + 0.8 x 0.001 + 1.4 x 0.005)/3.3 = 0.111/33 = 0.003364, and the row reads 0.093636.
#  4 drop books nothing: 0.0. ltp books G's fill, -20% at 0.3%: the rebalance sells A, D and F (0.003364) and
#    G enters whole (0.003); quarter 5 is cash, so G's sale lands here too, 1 x 0.003: -0.2 - 0.009364 = -0.209364.
#  5 cash: one name screened.
#  6 A +5% at 0.1%, H +15% at 0.5%: mean 0.10, entries 0.003: 0.097. End weights A 1.05/2.2, H 1.15/2.2. Quarter 7
#    books no name in either series, so the book's sale lands here: (1.05 x 0.001 + 1.15 x 0.005)/2.2 = 0.0068/2.2
#    = 0.003091, and the row reads 0.093909.
#  7 J and K are bought and neither has an exit bar or a fill: both series book nothing, 0.0, and hold no position.
#  8 A +20% at 0.3%, B 0% at 0.1%: mean 0.10. Both enter from an empty position, 1/2 x 0.003 + 1/2 x 0.001 = 0.002:
#    0.098, with no sale at the run's end (a sale would cost (1.2 x 0.003 + 1.0 x 0.001)/2.2 = 0.002091 more).
#    Were quarter 7 left out of the walk, A would be a carried name here, trading |1/2 - 1.05/2.2| at 0.003
#    (0.000068), and H would sell here (1.15/2.2 x 0.005 = 0.002614): 0.096818, with row 6 left at 0.097.
WALK_ROWS = {"drop": [0.097, 0.192825, 0.0, 0.093636, 0.0, 0.0, 0.093909, 0.0, 0.098],
             "ltp": [0.097, 0.192825, 0.0, 0.097, -0.209364, 0.0, 0.093909, 0.0, 0.098]}
WALK_GROSS = {"drop": [0.1, 0.2, 0.0, 0.1, 0.0, 0.0, 0.1, 0.0, 0.1], "ltp": [0.1, 0.2, 0.0, 0.1, -0.2, 0.0, 0.1, 0.0, 0.1]}
# (names held, names buyable at entry) per row: None buyable when the screen held cash
WALK_SHAPE = {"drop": [(2, 2), (2, 2), (0, 1), (3, 3), (0, 2), (0, None), (2, 2), (0, 2), (2, 2)],
              "ltp": [(2, 2), (2, 2), (0, 1), (3, 3), (1, 2), (0, None), (2, 2), (0, 2), (2, 2)]}
WALK_CASH = (2, 5)
# The same run with two more quarters (walk_run's tail), so that both series hold names on different books in one
# quarter, and the run ends in cash:
#  9 A +10% at 0.5%, and B at 0.3%, which has no exit bar: drop books A alone, ltp books B's fill too, -20%.
#    drop: mean 0.10. A is carried from 6/11 of the book to all of it, 5/11 at 0.005 (0.002273), and B leaves,
#    5/11 at the 0.001 it last carried (0.000455). Quarter 10 is cash, so A's sale lands here, 1 x 0.005:
#    0.10 - 0.007727 = 0.092273.
#    ltp: mean -0.05. A and B are both carried, each trading 1/22, at 0.005 and 0.003 (0.000364): B is a hold, its
#    fill a booked return like any other. The sale is (1.1 x 0.005 + 0.8 x 0.003)/1.9 = 0.004158:
#    -0.05 - 0.004522 = -0.054522.
#    Charged on the other series' book, drop would read 0.095478 and ltp -0.057727. With B sold and bought back
#    at its fill, ltp would read -0.056340. Without the cash quarter's sale they would read 0.097273 and -0.050364.
#    Row 8 stays 0.098: its book is carried, not sold.
# 10 cash: one name screened. No quarter follows it, and the sale is still charged.
TAIL_DATES = [date(2022, 7, 1), date(2022, 10, 1)]
TAIL_SCREEN = [{"A": 0.005, "B": 0.003}, {"H": 0.005}]
TAIL_ROWS = {"drop": WALK_ROWS["drop"] + [0.092273, 0.0], "ltp": WALK_ROWS["ltp"] + [-0.054522, 0.0]}
TAIL_SHAPE = {"drop": WALK_SHAPE["drop"] + [(1, 2), (0, None)], "ltp": WALK_SHAPE["ltp"] + [(2, 2), (0, None)]}
TAIL_CASH = WALK_CASH + (10,)
BAR_DATES = WALK_DATES + TAIL_DATES   # the dates WALK_BARS', WALK_FILLS' and the screens' indices count


class WalkFills:
    """LtpExits as run_backtest uses it: fill() gives a name with an entry and no exit its price in `table`
    (WALK_FILLS unless one is passed), and fills lists each one as holdings_full reads it."""

    def __init__(self, table=None):
        self.fills, self.table = [], WALK_FILLS if table is None else table

    def fill(self, symbols, entry_prices, exit_prices, entry_date, exit_date, **_kw):
        got = {s: v for s, v in self.table.get(BAR_DATES.index(entry_date), {}).items()
               if s in symbols and exit_prices.get(s) is None and entry_prices.get(s) is not None}
        self.fills += [{"symbol": s, "entry_date": entry_date.isoformat(), "last_bar": bar} for s, (_, bar) in got.items()]
        return {s: px for s, (px, _) in got.items()}


def walk_run(mod, mode, use_costs=True, verbose=False, tail=False, bars=None, fills=None, full=True):
    """run_backtest over WALK_DATES in one exit mode, with full holdings: its rows. tail: over TAIL_DATES too.
    bars: {symbol: {date index: price}} read over WALK_BARS'. fills: a table read in WALK_FILLS' place. full:
    False for a run without full holdings."""
    con = duckdb.connect()
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " high DOUBLE, volume DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?, ?, 1e6)",
                    [(s, ep(BAR_DATES[q] + timedelta(days=1)), px, px, px) for s, own in WALK_BARS.items()
                     for q, px in {**own, **(bars or {}).get(s, {})}.items()])
    screens = WALK_SCREEN + TAIL_SCREEN
    mod.MIN_STOCKS = 2
    mod.screen_stocks = lambda con, d, *a, **k: [(s, WALK_CAP[rate], 1.0)
                                                 for s, rate in screens[BAR_DATES.index(d)].items()]
    mod.get_benchmark_return = lambda con, sym, d, *a, **k: None if d == WALK_DATES[0] else 0.02
    dc = du.DataChecks("uncapped", mod.DATA_CHECKS, full_holdings=full)
    try:
        return mod.run_backtest(con, BAR_DATES if tail else WALK_DATES, 0.0, use_costs=use_costs, fx=1.0, dc=dc,
                                exit_method=mode, ltp=None if mode == "drop" else WalkFills(fills), verbose=verbose)
    finally:
        con.close()


# A value the walk refuses stops the run after its loop, and the message names the quarter by its rebalance date
# and the series by its key, where the walk gives the book's place in the series. Each damage is one price that is
# not a number, on a run without full holdings (with them, the lists' own check stops a run on such a bar first):
#  A's exit bar of quarter 3, the quarter after the cash one. The main series books A there in every mode, so the
#  message names portfolio_return and 2020-10-01. Counted over the invested quarters only, place 3 would read
#  2021-07-01.
#  G's last traded price in quarter 4, under both. The main series books no name there and the LTP series books
#  G's fill, so the message names portfolio_return_ltp and 2021-01-01.
NOT_A_NUMBER = float("nan")
WALK_REFUSAL = "a booked return under -100% or not finite"
WALK_REFUSED = [("drop", {"bars": {"A": {4: NOT_A_NUMBER}}}, f"2020-10-01, portfolio_return: {WALK_REFUSAL}"),
                ("ltp", {"bars": {"A": {4: NOT_A_NUMBER}}}, f"2020-10-01, portfolio_return: {WALK_REFUSAL}"),
                ("both", {"bars": {"A": {4: NOT_A_NUMBER}}}, f"2020-10-01, portfolio_return: {WALK_REFUSAL}"),
                ("both", {"fills": {4: {"G": (NOT_A_NUMBER, "2021-02-15")}}},
                 f"2021-01-01, portfolio_return_ltp: {WALK_REFUSAL}")]


def refusal_failures(src):
    """Each run of WALK_REFUSED raises ValueError with its message. Any other raise, or none, is a wrong answer."""
    out = []
    for mode, damage, want in WALK_REFUSED:
        try:
            walk_run(load(src), mode, full=False, **damage)
            got = "no raise"
        except Exception as e:
            got = f"{type(e).__name__}: {e}"
        if got != f"ValueError: {want}":
            out.append(f"a price that is not a number ({mode}, {sorted(damage)[0]}): {got}, want ValueError: {want}")
    return out


# The --verbose line of quarter 1, printed inside the loop, where a row's costs aren't known yet: its return
# (0.1999996) and its excess over the benchmark's 2% are both before costs, and say so when costs are on.
WALK_LINE = "    2020-04-01: 2 stocks (avg prox=1.000), port=20.0%{0}, bench=2.0%  ex=+18.0%{0}"
FLAT = date(2020, 10, 1)


class NoFills:
    """LtpExits as run_backtest uses it, filling nothing: under both, the LTP series books the main series' names."""

    fills = ()

    def fill(self, *_a, **_k):
        return {}


def ten_names(src, cap, px, mode="drop"):
    """run_backtest over two quarters on ten names at one cap, the screen and the benchmark stubbed and each
    price px(symbol, date): the two rows. mode both: with NoFills."""
    mod = load(src)
    mod.screen_stocks = lambda *a, **k: [(f"C{i:02d}", cap, 1.0) for i in range(10)]
    mod.get_prices = lambda con, symbols, d, **k: {s: px(s, d) for s in symbols}
    mod.get_benchmark_return = lambda *a, **k: 0.05
    return mod.run_backtest(FakeCon(), [COST_ENTRY, COST_EXIT, FLAT], 0.0, use_costs=True, fx=1.0, exit_method=mode,
                            ltp=NoFills() if mode == "both" else None)


def flat_row_failures(src):
    """Two quarters whose second is flat, on ten names.
    All ten up 20% (0.3% tier), then flat. Each drifted weight, 1.2 over the sum of ten of them, is not 1/10 to
    the last bit, so the flat quarter's drift trade is float noise (about 4e-19) and the row is 0.0 less that. It
    is stored 0.0, not -0.0. The first row pays its entries, 0.2 - 0.003.
    Five up 20% and five flat (0.1% tier), then all flat. The second quarter's mean is 0.0 and its only cost the
    drift back to equal weight: each name trades 0.1/11 of the book, 1/11 in all, at 0.001. It stores -0.000091:
    a cost under 0.0001, on a mean of nothing, is still charged. The first row pays its entries, 0.1 - 0.001.
    Each book runs in drop and in both. Under both no name is filled, so the LTP series books the same names and
    its rows read the same: portfolio_return_ltp is held to the same two values."""
    books = (("a flat quarter after equal returns", 5e9, lambda s, d: 10.0 if d == COST_ENTRY else 12.0, [0.197, 0.0]),
             ("a flat quarter that trades its drift", 2e10, lambda s, d: 12.0 if d != COST_ENTRY and s < "C05" else 10.0,
              [0.099, -0.000091]))
    out = []
    try:
        for label, cap, px, want in books:
            for mode, keys in (("drop", ("portfolio_return",)), ("both", ("portfolio_return", "portfolio_return_ltp"))):
                rows = ten_names(src, cap, px, mode)
                for key in keys:   # by repr: -0.0 == 0.0
                    got = [repr(r.get(key)) for r in rows]
                    if got != [repr(x) for x in want]:
                        out.append(f"{label} stores {got} as its {key} in {mode}, want {[repr(x) for x in want]}")
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return out


def halves_failures(name, rows, want, cash):
    """A both run's portfolio_return is want's drop series and its portfolio_return_ltp the ltp one, with no _ltp
    key on a cash row."""
    halves = ([r["portfolio_return"] for r in rows], [r.get("portfolio_return_ltp") for r in rows])
    want = (want["drop"], [None if i in cash else x for i, x in enumerate(want["ltp"])])
    return [] if halves == want else [f"{name}: portfolio_return and portfolio_return_ltp {halves}, want {want}"]


def typed_failures(runs, want, shape, cash, label=""):
    """Each row of the drop and ltp runs is want's, on shape's books, each item with its own name's rate and no
    list on a cash row; the both run's halves are the single runs' (halves_failures)."""
    out, screens = [], WALK_SCREEN + TAIL_SCREEN
    for mode in ("drop", "ltp"):
        got = [r["portfolio_return"] for r in runs[mode]]
        if got != want[mode]:
            out.append(f"{label}{mode} rows {got}, want {want[mode]}")
        books = [(r["stocks_held"], r["entry_buyable"]) for r in runs[mode]]
        if books != shape[mode]:
            out.append(f"{label}{mode} books (held, buyable) {books}, want {shape[mode]}")
        for i, r in enumerate(runs[mode]):
            rates = {h["symbol"]: h.get("cost_rate") for h in r.get("holdings_full") or []}
            if rates != {s: screens[i][s] for s in rates} or ("holdings_full" in r) != (i not in cash):
                out.append(f"{label}{mode} quarter {i}: holdings_full {r.get('holdings_full', 'missing')}")
    return out + halves_failures(f"{label}both", runs["both"], want, cash)


def walk_failures(src):
    """run_backtest over WALK_DATES: each row of the drop and ltp runs is WALK_ROWS', on WALK_SHAPE's books, each
    item with its own name's rate; both's portfolio_return is drop's and its portfolio_return_ltp ltp's, with no
    _ltp key on a cash row (typed_failures). The same over TAIL_DATES too, against TAIL_ROWS: the run that ends
    in cash. A no-costs run in each mode never calls the cost walk and books WALK_GROSS, and each costed row has
    the keys of the same row run without costs, in the same order. The ltp run and the no-costs run in both are
    verbose: quarter 1's line is WALK_LINE, labelled before costs only when costs are on. A flat quarter stores
    0.0, and one that only trades its drift pays for it (flat_row_failures). A value the walk refuses is named by
    its quarter's rebalance date and its series' key (refusal_failures)."""
    said = {True: io.StringIO(), False: io.StringIO()}
    try:
        runs = {mode: walk_run(load(src), mode) for mode in ("drop", "both")}
        with contextlib.redirect_stdout(said[True]):
            runs["ltp"] = walk_run(load(src), "ltp", verbose=True)
        tail = {mode: walk_run(load(src), mode, tail=True) for mode in ("drop", "ltp", "both")}
        free = load(src)
        free.trade_costs = no_walk
        try:
            with contextlib.redirect_stdout(said[False]):
                gross = {mode: walk_run(free, mode, use_costs=False, verbose=mode == "both") for mode in runs}
        except WalkCalled:
            return ["a no-costs run calls the cost walk"]
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    out = flat_row_failures(src) + refusal_failures(src)
    for costs_on, note in ((True, " before costs"), (False, "")):
        if WALK_LINE.format(note) not in said[costs_on].getvalue().splitlines():
            out.append(f"the verbose line with costs {'on' if costs_on else 'off'} is not {WALK_LINE.format(note)!r}: "
                       f"{[x for x in said[costs_on].getvalue().splitlines() if '2020-04-01' in x]}")
    out += typed_failures(runs, WALK_ROWS, WALK_SHAPE, WALK_CASH)
    out += typed_failures(tail, TAIL_ROWS, TAIL_SHAPE, TAIL_CASH, label="the run that ends in cash, ")
    out += halves_failures("no costs", gross["both"], WALK_GROSS, WALK_CASH)
    for mode, rows in gross.items():
        got = [r["portfolio_return"] for r in rows]
        if got != WALK_GROSS["ltp" if mode == "ltp" else "drop"]:
            out.append(f"no costs, {mode} rows {got}")
        keys = [(i, list(r), list(g)) for i, (r, g) in enumerate(zip(runs[mode], rows)) if list(r) != list(g)]
        if keys:   # no key is added with the costs
            out.append(f"{mode} quarter {keys[0][0]}: row keys {keys[0][1]} with costs, {keys[0][2]} without")
    return out


CASH_SCREEN = ('            note_cash()\n            if verbose:\n'
               '                print(f"    {entry_date}: {len(portfolio)} passed')
CASH_BUYABLE = ('            note_cash()\n            if verbose:\n'
                '                print(f"    {entry_date}: only {buyable} of')
LTP_BOOK = 'note_book("portfolio_return_ltp", c_ltp, rates_ltp, mean_ltp)'
WALKED = "trade_costs([b for _, b, _ in periods])"
COSTED = "            if b:\n"
REFUSAL_DAY = 'day = periods[int(place.split()[1])][0]["rebalance_date"]'
REFUSAL_SAYS = 'raise ValueError(f"{day}, {key}: {why}") from None'
WALK_MUTANTS = {   # each must fail walk_failures on a wrong row, not an error
    "a book taken before the entry-side cash check": (
        CASH_BUYABLE, CASH_BUYABLE.replace("note_cash()", "[note_book(k, clean, rates, 0.0) for k in list(series)]")),
    "no sale on the too-few-screened path": (CASH_SCREEN, CASH_SCREEN.replace("note_cash()", "pass")),
    "no sale on the too-few-buyable path": (CASH_BUYABLE, CASH_BUYABLE.replace("note_cash()", "pass")),
    "a cash quarter left out of the LTP series' walk": ("        for periods in series.values():\n            periods.append(",
                                                        "        for periods in list(series.values())[:1]:\n"
                                                        "            periods.append("),
    "the costs taken off the rounded row": ("row[key] = round(mean - cost, 6)", "row[key] = round(row[key] - cost, 6)"),
    "a sale charged at the run's end": (WALKED, "trade_costs([b for _, b, _ in periods] + [None])"),
    "the cash quarters that end a run left out of the walk": (
        WALKED, "trade_costs([b for j, (_, b, _) in enumerate(periods) if any(x for _, x, _ in periods[j:])])"),
    "the LTP book loses a fill's rate": (
        LTP_BOOK, 'note_book("portfolio_return_ltp", c_ltp, {s: rates.get(s, 0.0) for s, _, _ in c_ltp}, mean_ltp)'),
    "the LTP series walks the drop book": (LTP_BOOK, 'note_book("portfolio_return_ltp", clean, rates, mean_ltp)'),
    "the LTP series walks the drop book whenever that book holds a name": (
        LTP_BOOK, 'note_book("portfolio_return_ltp", clean or c_ltp, rates if clean else rates_ltp, mean_ltp)'),
    "under both, the main series charged on the LTP book whenever its own holds a name": (
        WALKED, "trade_costs([(l if b else b) for (_, b, _), (_, l, _) in zip(periods, series[SERIES[len(series) - 1]])])"),
    "a filled name sold at its fill and bought back, not held": (
        "{s: (rr, rates[s]) for s, rr, _ in clean}",
        '{(s + " (filled)" if s in ltp_fill else s): (rr, rates[s]) for s, rr, _ in clean}'),
    "a cost key added to each costed row": (COSTED, COSTED + '                row["cost" + key[16:]] = round(cost, 6)\n'),
    "a row whose mean before costs is 0.0 pays nothing": (COSTED, "            if b and mean:\n"),
    "a row whose cost is under 0.0001 pays nothing": (COSTED, "            if b and cost >= 1e-4:\n"),
    "each name charged its round trip as well": ("return clean, [rr for _, rr, _ in clean], rates",
                                                 "return clean, [rr - 2 * (rates[s] or 0.0) for s, rr, _ in clean], rates"),
    "the walk called on a no-costs run": ("} if use_costs else {}", "}"),
    "a quarter that books no name left out of its series' walk": ("        if key in series:\n",
                                                                  "        if key in series and clean:\n"),
    "a flat row less float noise stored -0.0": ("round(mean - cost, 6) + 0.0", "round(mean - cost, 6)"),
    "a flat row of the LTP series less float noise stored -0.0": (
        "round(mean - cost, 6) + 0.0", "round(mean - cost, 6) + (0.0 if key == SERIES[0] else -0.0)"),
    "a row of the LTP series whose mean before costs is 0.0 pays nothing": (
        COSTED, "            if b and (mean or key == SERIES[0]):\n"),
    "the verbose line's excess not labelled before costs": (':+.1f}%{note}"', ':+.1f}%"'),
    "the verbose line labelled before costs on a no-costs run": ('note = " before costs" if use_costs else ""',
                                                                 'note = " before costs"'),
    # the next four fail on the message a refused run raises (refusal_failures), which is its answer
    "a value the walk refuses left named by its place in the series": (REFUSAL_SAYS, "raise"),
    "a value the walk refuses named for the main series, whichever series held it": (
        REFUSAL_SAYS, REFUSAL_SAYS.replace("{key}", "{SERIES[0]}")),
    "a value the walk refuses dated by its quarter's exit": (REFUSAL_DAY, REFUSAL_DAY.replace("rebalance_date", "exit_date")),
    "a value the walk refuses dated by its place among the invested quarters": (
        REFUSAL_DAY, REFUSAL_DAY.replace("periods[", "[p for p in periods if p[1]][")),
}


# ur-4 on the screen (UNIVERSE_RULES), each leg through the topic's fetch: an SAO company holds one slot with data
# checks off (the ordinary line when it has an entry price, else its preferred line stands in); a JPX line whose
# profile industry starts with REIT is out on JPX and kept on another leg; a line the rules remove (a fund) is never
# ranked, though the fetch caches its rows; ur-3's line ends rank COG and CLR on their last own day and not the day
# after; GETY, a lookback_clip start, ranks on its own bars only (none before 100 of them), DOC, a rename, on
# the 252 bars ending at its signal bar.
SAO = {"BRAA3.SA": line(flat(120), cap=6e10, isin="BRBRAAACNOR1"), "BRAA4.SA": line(flat(120), cap=6e10, isin="BRBRAAACNPR2"),
       "BRBB3.SA": line(flat(120), cap=5e10, isin="BRBRBBACNOR1", after=()),   # no entry bar
       "BRBB4.SA": line(flat(120), cap=5e10, isin="BRBRBBACNPR2")}
JPX = {"8951.T": line(flat(120), industry="REIT - Office"), "8801.T": line(flat(120), industry="Real Estate - Development")}
NSE_RULES = {"EMBASSY.NS": line(flat(120), industry="REIT - Office"),
             "FUNDX.NS": line(flat(120), cap=2e10, isFund=True)}   # the larger cap, so it would rank first
ENDS_AT = date(2023, 1, 12)
ENDS = {s: line(flat(955), at=ENDS_AT, after=(), filings=[(date(2020, 12, 31), 1e10)]) for s in ("COG", "CLR")}
END_SCREENS = {date(2021, 10, 1): ["CLR", "COG"], date(2021, 10, 2): ["CLR"], date(2022, 11, 22): ["CLR"],
               date(2022, 11, 23): []}
STARTS = {s: date.fromisoformat(d) for s, d, _ in du.UNIVERSE_RULES_V4["line_starts"] if s in ("GETY", "DOC")}


def started(first):
    """Bars from 250 days before a line's first own day to 130 after: the earlier ones (another company's or the
    same one's under its old ticker) carry a high of 200 over a close of 100, the line's own close on their high but
    on its first own day, whose high of 125 tops its own bars, so a frame that starts a day late reads 1.0."""
    bars = {k: [100.0, 100.0, 200.0 if k < 0 else 125.0 if k == 0 else 100.0, 1e6] for k in range(-250, 131)}
    return line(bars, at=first, after=(), filings=[(first - timedelta(days=400), 1e10)])


CLIPS = {s: started(first) for s, first in STARTS.items()}
CLIP_SCREENS = {"GETY": {-1: [], 70: [], 130: [("GETY", 1e10, 0.8)]}, "DOC": {-1: [], 70: [("DOC", 1e10, 0.5)]}}
RULES_EXPECT = {
    "SAO book, data checks off": [("BRAA3.SA", 6e10, 1.0), ("BRBB4.SA", 5e10, 1.0)],
    "SAO stand-ins": [{"rebalance_date": D.isoformat(), "symbols": ["BRBB4.SA"]}],
    "JPX book": [("8801.T", 1e10, 1.0)],
    "NSE book": [("EMBASSY.NS", 1e10, 1.0)],
    **{f"line ends {d}": want for d, want in END_SCREENS.items()},
    **{f"{s} {k:+d} days": want for s, by in CLIP_SCREENS.items() for k, want in by.items()},
}


def rules_failures(src):
    try:
        mod = load(src)

        def leg(uni, dates, ex=EX):
            ur = du.UniverseRules(mod.UNIVERSE_RULES)
            return cache(mod, uni, dates, ex=ex, ur=ur), ur

        got = {}
        con, ur = leg(SAO, [PREV, D], "SAO")
        got["SAO book, data checks off"] = mod.screen_stocks(con, D, FLOOR, ur=ur)
        got["SAO stand-ins"] = (ur.block().get("class_stand_ins") or {}).get("by_period")
        for name, uni, ex in (("JPX book", JPX, "JPX"), ("NSE book", NSE_RULES, EX)):
            con, ur = leg(uni, [PREV, D], ex)
            got[name] = mod.screen_stocks(con, D, FLOOR, ur=ur)
        con, ur = leg(ENDS, sorted(END_SCREENS))
        for d in END_SCREENS:
            got[f"line ends {d}"] = sorted(s for s, _, _ in mod.screen_stocks(con, d, FLOOR, ur=ur))
        screens = {(s, k): STARTS[s] + timedelta(days=k) for s, by in CLIP_SCREENS.items() for k in by}
        con, ur = leg(CLIPS, sorted(set(screens.values())))
        for (s, k), d in screens.items():
            got[f"{s} {k:+d} days"] = mod.screen_stocks(con, d, FLOOR, ur=ur)
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return [f"{k}: {got[k]} != {want}" for k, want in RULES_EXPECT.items() if got[k] != want]


CLIPPED = "for s in ur.clips}"
RULE_MUTANTS = {
    "ur-3 in place of ur-4": ("UNIVERSE_RULES = UNIVERSE_RULES_V4",
                              "UNIVERSE_RULES = __import__('data_utils').UNIVERSE_RULES_V3"),
    "class twins keyed only with data checks on": (TWIN_KEY, "twin_key=dc is not None and dc.on)"),
    "stand-ins not recorded": ("ur.stand_ins([s for s, _, _ in picked], target_date)", "None"),
    "no universe semi-join": ("              AND km.symbol IN (SELECT symbol FROM universe)\n", ""),
    "no line starts": ("late = ur.not_started(target_date) if ur is not None else []", "late = []"),
    "no line ends": ("gone = ur.ended(target_date) if ur is not None else []", "gone = []"),
    "line ends read a day late": ("ur.ended(target_date)", "ur.ended(target_date - timedelta(days=1))"),
    "line ends read a day early": ("ur.ended(target_date)", "ur.ended(target_date + timedelta(days=1))"),
    "no lookback clip": (CLIPPED, "for s in ()}"),
    "every line start clipped": (CLIPPED, "for s in ur.starts}"),
    "the clipped frame starts a day after the first own day": ("trade_epoch >= CASE symbol", "trade_epoch > CASE symbol"),
}


# The gate's legs (scripts/gate_legs/wave3.json) are --global's presets_to_run, both ways, ASX and SAO among them.
LEGS_FILE = os.path.join(ROOT, "scripts", "gate_legs", "wave3.json")
LEG_PIN = ["--end-year", "2025"]
# last on every leg: the topic states its cost convention, and a committed leg's rows are recomputed from its
# holdings_full (scripts/cost_conventions.py, gate 15)
LEG_LAST = ["--full-holdings"]
RANKED_ADDED = ({"ASX"}, {"SAO"})   # the markets the port ranks


def global_presets(src):
    """{name: exchanges} of main()'s presets_to_run in src."""
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "presets_to_run" for t in node.targets):
            return dict(ast.literal_eval(node.value))
    raise ValueError("no presets_to_run")


def leg_exchanges(args):
    """A leg's exchanges: --exchange's list, or --preset's from cli_utils.EXCHANGE_PRESETS."""
    flag, value = args[len(LEG_PIN)], args[len(LEG_PIN) + 1]
    return value.split(",") if flag == "--exchange" else EXCHANGE_PRESETS[value]["exchanges"]


def legs_spec():
    with open(LEGS_FILE) as fh:
        return json.load(fh)[TOPIC]


def legs_failures(src, spec=None):
    """wave3.json's 52-week-high legs, one per --global preset and no other: each runs the preset's exchanges pinned to
    --end-year 2025 before its exchange args, ends with --full-holdings, and installs as the returns_<exchanges>.json
    the global run writes. spec: the topic's legs block to read (default the file's)."""
    try:
        want = global_presets(src)
        spec = spec if spec is not None else legs_spec()
        out = [f"legs {sorted(spec[k])} != presets {sorted(want)}" for k in ("legs", "_outputs")
               if set(spec[k]) != set(want)]
        for name, ex in want.items():
            args = spec["legs"].get(name) or []
            if args[:len(LEG_PIN)] != LEG_PIN or args[len(LEG_PIN) + 2:] != LEG_LAST or leg_exchanges(args) != ex:
                out.append(f"leg {name}: {args}, want {LEG_PIN}, then {ex}, then {LEG_LAST}")
            if spec["_outputs"].get(name) != f"returns_{'_'.join(ex)}.json":
                out.append(f"leg {name} installs as {spec['_outputs'].get(name)}")
        out += [f"{sorted(m)} not ranked" for m in RANKED_ADDED if m not in map(set, want.values())]
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return out


LEG_MUTANTS = {"australia left out of --global": ('            ("australia",   ["ASX"]),\n', "")}


def leg_file_survivors():
    """The edits of the legs block that legs_failures lets through. --full-holdings missing from a leg, put before
    its exchange args, given twice, or followed by another argument must each fail it on the leg's own message:
    an error raised while reading the edited leg is not a catch."""
    spec = legs_spec()
    us, uk = spec["legs"]["us"], spec["legs"]["uk"]
    edits = {"--full-holdings missing from a leg": {"us": us[:-1]},
             "--full-holdings before the exchange args": {"uk": [*LEG_PIN, *LEG_LAST, *uk[len(LEG_PIN):-1]]},
             "--full-holdings twice": {"us": [*us, *LEG_LAST]},
             "an argument after --full-holdings": {"uk": [*uk, "--verbose"]}}
    return [name for name, legs in edits.items()
            if not [f for f in legs_failures(SRC, {**spec, "legs": {**spec["legs"], **legs}}) if f.startswith("leg ")]]

# Every exchange --global runs has its own rate in costs.FX_PER_USD: get_fx_per_usd reads 1.0 for one it lacks,
# which would leave that leg's cost tiers unconverted.
COSTS_PATH = os.path.join(ROOT, "costs.py")
with open(COSTS_PATH) as fh:
    COSTS_SRC = fh.read()


def fx_failures(costs_src, src=SRC):
    """The exchanges of src's presets_to_run that costs_src's FX_PER_USD has no rate for."""
    try:
        rates = load(costs_src, COSTS_PATH).FX_PER_USD
        missing = sorted({e for ex in global_presets(src).values() for e in ex} - set(rates))
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return [f"no FX_PER_USD rate for {missing}: their cost tiers stay in dollars"] if missing else []


FX_MUTANTS = {"Malaysia's rate left out": ('"KLS": 4.46,', "")}


# The live screen (screen.py): the backtest's fetch and screen on one date, with data checks on.
SCREEN_PATH = os.path.join(ROOT, "52-week-high", "screen.py")
with open(SCREEN_PATH) as fh:
    SCREEN_SRC = fh.read()
JF = get_mktcap_threshold(["JPX"])   # the JPX leg's floor
# The JPX leg on D: a J-REIT ur-4 removes and a fund the semi-join leaves out, both over the floor; check M's lines
# (on the floor and a unit over it once divided, ineligible, and two in cap_used's order, not the raw cap's); two
# lines equal in ratio and cap, in symbol order; a line 1% under its high, after every 1.0.
SCREEN_LEG = {
    "8951.T": line(flat(120), cap=8 * JF, industry="REIT - Office", after=()),
    "FUND.T": line(flat(120), cap=7 * JF, isFund=True, after=()),
    "M_AT.T": line(flat(120), cap=10 * JF, after=()),
    "M_OVER.T": line(flat(120), cap=10 * JF + 10, after=()),
    "M_INEL.T": line(flat(120), cap=20 * JF, after=()),
    "M_RAW.T": line(flat(120), cap=300 * JF, after=()),
    "M_USED.T": line(flat(120), cap=4 * JF, after=()),
    "TIE_B.T": line(flat(120), cap=2 * JF, after=()),
    "TIE_A.T": line(flat(120), cap=2 * JF, after=()),
    "NEAR.T": line(bar(flat(120), 0, adj=99.0, close=99.0), cap=50 * JF, after=()),
}
SCREEN_CAP_RULES = {"M_AT.T": ("cap_over_k", 10.0), "M_OVER.T": ("cap_over_k", 10.0),
                    "M_INEL.T": ("ineligible", None), "M_RAW.T": ("cap_over_k", 100.0)}
SCREEN_BOOK = [("M_USED.T", 4 * JF, 1.0), ("M_RAW.T", 3 * JF, 1.0), ("TIE_A.T", 2 * JF, 1.0),
               ("TIE_B.T", 2 * JF, 1.0), ("M_OVER.T", JF + 1, 1.0), ("NEAR.T", 50 * JF, 0.99)]
SCREEN_ENDS_DAY = date(2021, 10, 2)   # COG's line ends the day before; CLR's on 2022-11-22
SCREEN_CLIP_DAY = 130   # days after GETY's first own day (ur-4's line start and lookback clip, which ur-3 lacks)
TODAY = datetime.now(timezone.utc).date()
LIVE_LEG = {"8951.T": line(flat(120), cap=8 * JF, industry="REIT - Office", at=TODAY, after=()),
            "LIVE_A.T": line(flat(120), cap=3 * JF, at=TODAY, after=()),
            "LIVE_B.T": line(bar(flat(120), 0, adj=95.0, close=95.0), cap=5 * JF, at=TODAY, after=())}
LIVE_BOOK = ["LIVE_A.T", "LIVE_B.T"]


@contextlib.contextmanager
def bound(name, module):
    """sys.modules[name] is module while the block runs (screen.py's `from backtest import ...`)."""
    saved = sys.modules.get(name)
    sys.modules[name] = module
    try:
        yield
    finally:
        if saved is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = saved


def screen_backtest(cap_rules):
    """The backtest screen.py imports: check M's server query stands in as cap_rules' cap_check rows (symbol ->
    (rule, divisor) on its FY2019 row), and the oscillation filter is off, as in cache(). The query must be the
    one that reads every FY row over the floor (require_roe=False)."""
    mod = load(SRC)

    def load_cap_check(client, con, sym_filter_sql, mktcap_min, cfg, verbose=False, require_roe=True):
        if require_roe:
            raise AssertionError("check M's query reads only the FY rows with an ROE")
        con.execute(du.CAP_INPUTS_DDL)
        con.execute(du.CAP_CHECK_DDL)
        con.executemany("INSERT INTO cap_check (symbol, filing_epoch, rule, cap_div) VALUES (?, ?, ?, ?)",
                        [(s, ep(FY19), rule, div) for s, (rule, div) in cap_rules.items()])
        return {}

    mod.load_cap_check = load_cap_check
    mod.remove_price_oscillations = lambda *a, **k: None
    return mod


def screen_client(uni, ex):
    """test_data_checks' fake client on seed()'s warehouse, with the splits table a data-checks fetch reads."""
    src = seed(uni, ex)
    src.execute("CREATE TABLE splits_calendar(symbol VARCHAR, dateEpoch BIGINT, numerator DOUBLE, denominator DOUBLE)")
    return tdc.FakeClient(src)


def screen_main(mod, client, args):
    """screen.py's main() with args, CetaResearch standing in as client: its stdout."""
    out, argv, sys.argv = io.StringIO(), sys.argv, ["screen.py", *args]
    mod.CetaResearch = lambda **_kw: client
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            mod.main()
    finally:
        sys.argv = argv
    return out.getvalue()


def screen_failures(src):
    """screen.py on the backtest's rules: on the JPX leg on D the book is SCREEN_BOOK (ur-4's J-REIT and the fund
    out; check M's cap_used at the floor and in the tie order, then the symbol); on NYSE on SCREEN_ENDS_DAY, CLR
    alone (COG's line ends the day before); on NYSE SCREEN_CLIP_DAY days after GETY's first own day, GETY on its own
    bars (the screen's line lists are ur-4's); main() on today's JPX leg prints LIVE_BOOK, as CSV and as a table, and
    sends the fetch's progress to stderr."""
    clip_day = STARTS["GETY"] + timedelta(days=SCREEN_CLIP_DAY)
    try:
        with bound("backtest", screen_backtest(SCREEN_CAP_RULES)):
            mod = load(src, SCREEN_PATH)
        with contextlib.redirect_stdout(io.StringIO()):
            got = {"JPX": mod.screen(screen_client(SCREEN_LEG, "JPX"), ["JPX"], D),
                   "NYSE": mod.screen(screen_client(ENDS, "NYSE"), ["NYSE"], SCREEN_ENDS_DAY),
                   "GETY": mod.screen(screen_client({"GETY": CLIPS["GETY"]}, "NYSE"), ["NYSE"], clip_day)}
        live = screen_client(LIVE_LEG, "JPX")
        csv_out = screen_main(mod, live, ["--exchange", "JPX", "--csv"]).splitlines()
        table = screen_main(mod, live, ["--exchange", "JPX"]).splitlines()
    except (Exception, SystemExit) as e:
        return [f"error: {type(e).__name__}: {e}"]
    out = [f"{k} book {got[k]} != {want}" for k, want in (("JPX", SCREEN_BOOK), ("NYSE", [("CLR", 1e10, 1.0)]),
                                                         ("GETY", CLIP_SCREENS["GETY"][SCREEN_CLIP_DAY]))
           if got[k] != want]
    rows = [r.split(",") for r in csv_out[1:]]
    if csv_out[:1] != ["rank,symbol,market_cap,proximity_ratio,pct_below_high"] or \
            [(r[0], r[1]) for r in rows] != [(str(i), s) for i, s in enumerate(LIVE_BOOK, 1)]:
        out.append(f"main --csv printed {csv_out}")
    listed = [ln.split()[1] for ln in table if ln.split()[:1] and ln.split()[0].isdigit()]
    if listed != LIVE_BOOK:
        out.append(f"main's table lists {listed}, want {LIVE_BOOK}")
    return out


SCREEN_MUTANTS = {
    "the screen's line lists from ur-2": ("UniverseRules(UNIVERSE_RULES)",
                                          "UniverseRules(__import__('data_utils').UNIVERSE_RULES_V2)"),
    "the screen's line lists from ur-3": ("UniverseRules(UNIVERSE_RULES)",
                                          "UniverseRules(__import__('data_utils').UNIVERSE_RULES_V3)"),
    "the screen with data checks off": ('DataChecks("on", DATA_CHECKS)', 'DataChecks("uncapped", DATA_CHECKS)'),
    "the screen with no floor": ("mktcap_min = get_mktcap_threshold(exchanges)\n", "mktcap_min = 0\n"),
    "the screen ignores its day": ("day = day or datetime.now(timezone.utc).date()",
                                   "day = datetime.now(timezone.utc).date()"),
    "the screen without the line lists": ("screen_stocks(con, day, mktcap_min, verbose=verbose, dc=dc, ur=ur)",
                                          "screen_stocks(con, day, mktcap_min, verbose=verbose, dc=dc)"),
    "main ignores --exchange": ('exchanges = [e.strip().upper() for e in args.exchange.split(",")]',
                                'exchanges = ["NYSE", "NASDAQ", "AMEX"]'),
    "main's progress on stdout": ("with contextlib.redirect_stdout(sys.stderr):", "if True:"),
}


# Check T on the ranking cache: T_R, a rebalance date, and T_BACK, 40 days before it and outside every window, are
# closed days on test_data_checks' synthetic leg (only A00.BK to A02.BK trade, as on its CLOSED days) where one line
# prints a traded copy: T_COPY at 80% of its close on T_R (under the oscillation filter's thresholds), T_KEPT with
# twice its high on T_BACK. The fetch's order, check T with the sibling rule and then the oscillation filter:
# T_OSC prints 2.5 times its price on T_SPIKE, outside every window, which only the filter removes; T_FIRST prints a
# traded copy at 3 times its price on T_R, which check T moves as a closed-day row before the filter could remove
# it; T_THIN, an NVDR line that trades a tenth of its ordinary line's volume (siblings: one FY cap), opens T_R's
# window at 0.6 of that line's close on T_AFTER, which only check T's sibling rule moves.
T_R = date(2014, 1, 1)
T_BACK = T_R - timedelta(days=40)
T_DATES = [date(2013, 7, 1), date(2013, 10, 1), T_R]   # the sibling rule's reference needs two other windows
T_COPY, T_KEPT, T_OSC, T_FIRST = "A10.BK", "A11.BK", "A12.BK", "A13.BK"
T_SPIKE, T_AFTER = date(2013, 11, 6), date(2014, 1, 2)
T_THIN = "TWN-R.BK"


def t_leg():
    if "t" not in _LEGS:
        src = tdc.wh52_source()
        for d, s in ((T_R, T_COPY), (T_BACK, T_KEPT)):
            src.execute("UPDATE stock_eod SET volume = 0 WHERE date = ? AND symbol NOT IN"
                        " ('A00.BK', 'A01.BK', 'A02.BK', '^SET.BK', 'SPY', ?)", [d.isoformat(), s])
        scale = "UPDATE stock_eod SET adjClose = adjClose * ?, close = close * ?, high = high * ? WHERE date = ? AND symbol = ?"
        for k, d, s in ((0.8, T_R, T_COPY), (2.5, T_SPIKE, T_OSC), (3.0, T_R, T_FIRST), (0.6, T_AFTER, T_THIN)):
            src.execute(scale, [k, k, k, d.isoformat(), s])
        src.execute("UPDATE stock_eod SET volume = 1e5 WHERE date = ? AND symbol = ?", [T_R.isoformat(), T_FIRST])
        src.execute("UPDATE stock_eod SET volume = volume / 10 WHERE symbol = ?", [T_THIN])
        src.execute("UPDATE stock_eod SET high = high * 2 WHERE date = ? AND symbol = ?", [T_BACK.isoformat(), T_KEPT])
        _LEGS["t"] = src
    return _LEGS["t"]


def t_failures(src):
    """The fetch with data checks on cleans the grid's windows [d, d + 11] (check T) before the screen reads them:
    T_R's copy moves, so T_COPY ranks on its last traded bar before T_R, at 1.0, while T_BACK's copy stays in
    T_KEPT's lookback and its high holds T_KEPT's ratio near 0.5. The block lists T_R as a closed date, not T_BACK.
    The oscillation filter runs after check T: T_OSC ranks at 1.0 without its spike, T_FIRST's copy on T_R lands in
    prices_dropped as a closed-day row, and the block lists T_THIN's bar on T_AFTER as its one sibling gap."""
    try:
        mod, dc = load(src), du.DataChecks("on", du.DATA_CHECKS_V3)
        with contextlib.redirect_stdout(io.StringIO()):
            con = mod.fetch_data_via_api(tdc.FakeClient(t_leg()), ["SET"], T_DATES, mktcap_min=1e9, dc=dc)
            prox = {s: p for s, _, p in mod.screen_stocks(con, T_R, 1e9, dc=dc)}
        first = con.execute("SELECT reason FROM prices_dropped WHERE symbol = ? AND trade_epoch = ?",
                            [T_FIRST, ep(T_R)]).fetchall()
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    out = []
    if prox.get(T_COPY) != 1.0 or not 0.4 < (prox.get(T_KEPT) or 1.0) < 0.6:
        out.append(f"ratios {T_COPY} {prox.get(T_COPY)}, want 1.0; {T_KEPT} {prox.get(T_KEPT)}, want about 0.5")
    block = dc.block()
    closed = [x["date"] for x in block["closed_dates"]]
    if T_R.isoformat() not in closed or T_BACK.isoformat() in closed:
        out.append(f"closed dates {closed}: want {T_R} and not {T_BACK}")
    if prox.get(T_OSC) != 1.0:
        out.append(f"{T_OSC} ranks at {prox.get(T_OSC)}: its spike on {T_SPIKE} stayed, want 1.0")
    if first != [("closed_day",)]:
        out.append(f"{T_FIRST}'s copy on {T_R} in prices_dropped as {first}, want closed_day (check T before the filter)")
    gaps = [(g["symbol"], g["date"]) for g in block["sibling_gaps"]]
    if gaps != [(T_THIN, T_AFTER.isoformat())]:
        out.append(f"sibling gaps {gaps}, want {T_THIN} on {T_AFTER}")
    return out


T_CALL = "clean_price_cache(con, bench_symbols, rebalance_dates, dc.cfg"
OSC = "    remove_price_oscillations(con, verbose=verbose)\n"
T_STEP = ("    if checks:   # check T: closed days, zero-volume and flat lines, non-positive prices, sibling-class gaps\n"
          '        dc.record_clean(clean_price_cache(con, bench_symbols, rebalance_dates, dc.cfg, siblings="cap_inputs"))\n')
T_MUTANTS = {
    "check T on no window": (T_CALL, T_CALL.replace("rebalance_dates", "[]")),
    "check T over the lookback": (T_CALL, T_CALL.replace(
        "rebalance_dates", "[x for d in rebalance_dates for x in (d - timedelta(days=40), d)]")),
    "check T without the sibling rule": (', siblings="cap_inputs")', ")"),
    "no oscillation filter": (OSC, ""),
    "the oscillation filter before check T": (T_STEP + OSC, OSC + T_STEP),
}


def window_leg():
    """The synthetic SET leg with test_latest_window_topics' 2026 bars, as a 52-week-high leg, built once."""
    if "window" not in _LEGS:
        _LEGS["window"] = tdc.wh52_source(tlw.extend(tdc.synthetic_source()))
    return _LEGS["window"]


def main_run(mod, leg, tmp, args):
    """main() on the SET leg with args, CetaResearch standing in as test_data_checks' fake client: its results."""
    out = os.path.join(tmp, f"main.{len(os.listdir(tmp))}.json")
    mod.CetaResearch = lambda **_kw: tdc.FakeClient(leg)
    argv, sys.argv = sys.argv, ["backtest.py", "--exchange", "SET", "--exit-method", "ltp", "--output", out, *args]
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            mod.main()
    finally:
        sys.argv = argv
    with open(out) as fh:
        return json.load(fh)


def partial_failures(src):
    """The quarterly run window: run_single pinned to test_latest_window_topics' END (2026-11-06) marks every row
    and only the last, 2026-10-01 to the end date, partial (its PARTIAL["quarterly"] period); that row's window
    exits and the benchmark's are the end date's closes, and HALT (no bar after 2026-10-30) fills at its last bar
    under ltp; CAGR runs over span_years; the run window's keys are written. main() takes --end-date, and
    --end-year 2025 books today's 103 periods with no run-window key."""
    try:
        mod, leg = load(src), window_leg()
        with tempfile.TemporaryDirectory() as tmp:
            r = tdc.run_topic(TOPIC, leg, "ltp", "on", tmp, mod=mod, full_holdings=True, win=tlw.window(leg, "pin"))[0]
            by_date = main_run(load(src), leg, tmp, ["--end-date", tlw.END.isoformat()])
            by_year = main_run(load(src), leg, tmp, ["--end-year", "2025"])
    except (Exception, SystemExit) as e:  # an error, argparse's exit on an unknown flag too: caught here
        return [f"error: {type(e).__name__}: {e}"]
    out = []
    part, rows = tlw.PARTIAL["quarterly"], r["period_data"]
    if (r.get("partial_period"), r.get("end_date"), r.get("priced_to"), r.get("full_periods")) != \
            (part, "2026-11-06", "2026-11-06", r["n_periods"] - 1):
        out.append(f"window keys {[r.get(k) for k in ('partial_period', 'end_date', 'priced_to', 'full_periods')]}")
    if [x.get("partial") for x in rows] != [False] * (len(rows) - 1) + [True] or \
            rows[-1].get("fraction") != part["fraction"]:
        out.append(f"partial flags {[x.get('partial') for x in rows][-3:]}, fraction {rows[-1].get('fraction')}")
    last = rows[-1]
    entry = date.fromisoformat(part["entry_day"])
    want = round(tlw.bench_close(leg, tlw.END) / tlw.bench_close(leg, entry) - 1, 6)
    if last["spy_return"] is None or abs(last["spy_return"] - want) > 1e-6:
        out.append(f"the partial benchmark return {last['spy_return']}, want the end date's close {want}")
    held = {h["symbol"]: h for h in last.get("holdings_full") or []}
    off = sorted(s for s, h in held.items() if s not in (tlw.HALT, tlw.ZEROV)
                 and (h["exit"], h["exit_bar"]) != ("window", "2026-11-06"))
    if not held or off:
        out.append(f"the partial row's window exits not on the end date: {off or 'nothing held'}")
    if (held.get(tlw.HALT) or {}).get("exit_bar") != "2026-10-30" or (held.get(tlw.HALT) or {}).get("exit") != "last_traded":
        out.append(f"{tlw.HALT} exits {held.get(tlw.HALT)}, want its last traded bar on 2026-10-30")
    if not r.get("span_years"):
        return out + ["no span_years"]
    cagr = ((1 + r["portfolio"]["total_return"] / 100) ** (1 / r["span_years"]) - 1) * 100
    if abs(cagr - r["portfolio"]["cagr"]) > 0.011:
        out.append(f"CAGR {r['portfolio']['cagr']}, want {cagr:.2f} over span_years")
    if (by_date.get("end_date"), by_date.get("partial_period")) != ("2026-11-06", part):
        out.append(f"main() --end-date: {by_date.get('end_date')} {by_date.get('partial_period')}")
    if len(by_year["period_data"]) != 103 or "end_date" in by_year or by_year["period_data"][-1]["exit_date"] != "2025-10-01":
        out.append(f"main() --end-year 2025: {len(by_year['period_data'])} periods, end_date {by_year.get('end_date')}")
    return out


WINDOW_MUTANTS = {   # each must fail partial_failures, by a wrong row or by an error
    "the fetch past the end date": ("entry_end = win.fetch_end(d, 11)", "entry_end = d + timedelta(days=11)"),
    "stock exits priced next day": ("exit_prices = get_prices(con, symbols, exit_date, offset_days=x_off)",
                                    "exit_prices = get_prices(con, symbols, exit_date, offset_days=offset_days)"),
    "the benchmark's exit priced next day": ("offset_days=offset_days, exit_offset_days=x_off)",
                                             "offset_days=offset_days)"),
    "rows unmarked": ("    win.mark(results)\n", "\n"),
    "CAGR over the period count": ("risk_free_rate=risk_free_rate, fractions=fractions)",
                                   "risk_free_rate=risk_free_rate)"),
    "the run window's keys not written": (
        "**win.fields(con, benchmark_symbol, valid, periods_per_year, partial_year, offset_days),", ""),
    "main() ignores --end-date": ("win = RunWindow.from_args(args, cr, {universe_name: exchanges})",
                                  "win = RunWindow.typed(2025)"),
}


def mutant_results(mutants, check, must_book=(), src=SRC):
    """(survived, errored): the mutants of src no check catches, and those in must_book caught only by an error."""
    survived, errored = [], []
    for name, (old, new) in mutants.items():
        if src.count(old) != 1:
            print(f"test_52wh_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        got = check(src.replace(old, new))
        if not got:
            survived.append(name)
        elif name in must_book and all(f.startswith("error:") for f in got):
            errored.append(name)
    return survived, errored


def main():
    base = (legs_failures(SRC) + fx_failures(COSTS_SRC) + walk_failures(SRC) + screen_failures(SCREEN_SRC)
            + failures(SRC) + rules_failures(SRC) + run_failures(SRC) + partial_failures(SRC) + t_failures(SRC))
    if base:
        print("test_52wh_selection: FAIL", base)
        sys.exit(1)
    survived, errored = mutant_results(MUTANTS, failures, TRADED_MUTANTS)
    s1, e1 = mutant_results(RULE_MUTANTS, rules_failures, RULE_MUTANTS)
    s2, e2 = mutant_results(RESULT_MUTANTS, run_failures, RESULT_MUTANTS)
    s3, _ = mutant_results(WINDOW_MUTANTS, partial_failures)
    s4, e4 = mutant_results(T_MUTANTS, t_failures, T_MUTANTS)
    s5, e5 = mutant_results(LEG_MUTANTS, legs_failures, LEG_MUTANTS)
    s6, e6 = mutant_results(SCREEN_MUTANTS, screen_failures, SCREEN_MUTANTS, src=SCREEN_SRC)
    s7, e7 = mutant_results(FX_MUTANTS, fx_failures, FX_MUTANTS, src=COSTS_SRC)
    s8, e8 = mutant_results(WALK_MUTANTS, walk_failures, WALK_MUTANTS)
    survived, errored = survived + s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8, errored + e1 + e2 + e4 + e5 + e6 + e7 + e8
    survived += leg_file_survivors()
    if survived or errored:
        print("test_52wh_selection: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    n_rules = len(SAO) + len(JPX) + len(NSE_RULES) + len(ENDS) + len(CLIPS)
    print(f"test_52wh_selection: PASS ({len(SIGNAL) + len(CUT) + len(CAPS)} listings, {len(MUTANTS)} mutants caught;"
          f" universe rules ur-4 on {n_rules} listings, {len(RULE_MUTANTS)} mutants caught; the run on the synthetic"
          f" SET leg, {len(RESULT_MUTANTS)} mutants caught; the run window, {len(WINDOW_MUTANTS)} mutants caught;"
          f" check T, {len(T_MUTANTS)} mutants caught; the gate's legs, {len(LEG_MUTANTS)} mutants caught;"
          f" the cost rates, {len(FX_MUTANTS)} mutants caught; the trades convention over {len(WALK_SCREEN)} typed"
          f" quarters and {len(TAIL_SCREEN)} more that end in cash, {len(WALK_MUTANTS)} mutants caught; the live"
          f" screen, {len(SCREEN_MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
