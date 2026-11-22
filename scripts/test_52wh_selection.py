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
only, and DOC, a rename, on the 252 bars ending at its signal bar.

Check T on the ranking cache, with data checks on: on a closed rebalance date a line's traded copy moves, so the
line ranks on its last traded bar before it, while a copy 40 days back, outside every fetch window, stays in the
lookback.

The run on test_data_checks' synthetic SET leg (wh52_source): run_single on --end-year 2025 books today's 103
quarterly periods; each invested row's holdings string names every booked name, and --full-holdings adds
holdings_full, every booked name in that order, changing nothing else; check N keys an NVDR line to its ordinary
line, which holds the slot, and the NVDR stands in for an ordinary line with no entry bar; the SAO company's
preferred line stands in for its ordinary line in every invested book. The results file carries data_checks,
universe_rules, signal_guard (its count on every rebalance, equal in drop and ltp) and cost_model after
isin_dedup (the tiers at 35 baht a dollar, null on --no-costs), the fetch asks for no MTUM, entry_unpriced
and the data checks' counts on each row, and in each holdings_full item the one-way rate the name paid
(cost_rate), which with the item's return gives the row's return. Costs, with run_backtest's screen and prices
stubbed: a JPX and an LSE cap pay the tier at their leg's FX rate, and a name with no cap the top one. The quarterly
run window, with test_latest_window_topics' 2026 bars: pinned to 2026-11-06, the last row is the partial
2026-10-01 period, its window exits and the benchmark's priced on the end date's close, a name with no bar after
2026-10-30 filled at its last bar, CAGR over span_years and the run window's keys written; main() takes
--end-date, and --end-year 2025 books the 103 periods. Their mutants must trip these checks.

The gate's legs: scripts/gate_legs/wave3.json holds one 52-week-high leg per --global preset and no other, ASX and SAO
among them, each pinned to --end-year 2025 and installed as the file the global run writes.

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
from cli_utils import EXCHANGE_PRESETS   # noqa: E402
from costs import get_fx_per_usd   # noqa: E402
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


def load(src):
    mod = types.ModuleType("wh52_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
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
    # a close over the window's high ranks as it reads: the screen sets no proximity cap (a real bar's high holds its
    # close, so a cap never acted)
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


COST_MODEL = {"function": "costs.tiered_cost", "fx_per_usd": 35.0}   # the SET leg: 35 baht a dollar
SET_RATE = 0.005   # the synthetic caps, about 5e10 baht, are about 1.4B dollars: the 0.5% tier (0.1% unconverted)
# the signal guard on the SET leg: WH52_UNRANKED's two lines, whose sparse bars never make 100, at each rebalance
# but the first, short_history when a bar falls in the 10 days to R (71 each) and no_traded_bar otherwise (31 each);
# and DLST.BK's four quarters from its delisting until its last bar leaves the lookback (no_traded_bar)
SET_GUARD = {"no_traded_bar": 66, "short_history": 142}
ROW_KEYS = ("entry_unpriced","entry_bars_moved", "exit_bars_moved", "entries_lost", "exits_lost", "twins_collapsed",
            "cap_excluded", "factor_flags")


def cost_failures(res, rate):
    """Each holdings_full item's cost_rate is `rate` (the one-way rate it paid; None on a no-costs run), and each
    invested row's return is the mean of its items' returns less the round trip."""
    for r in res["period_data"]:
        hf = r.get("holdings_full") or []
        if any(h.get("cost_rate", "missing") != rate for h in hf):
            return [f"{r['rebalance_date']}: cost_rate {sorted({str(h.get('cost_rate', 'missing')) for h in hf})}, "
                    f"want {rate}"]
        net = [h["return"] - 2 * (rate or 0.0) for h in hf]
        if hf and abs(sum(net) / len(net) - r["portfolio_return"]) > 2e-6:
            return [f"{r['rebalance_date']}: portfolio_return {r['portfolio_return']}, items less costs "
                    f"{sum(net) / len(net):.6f}"]
    return []


def results_failures(src):
    """run_single on the synthetic SET leg, --end-year 2025: 103 quarterly periods from 2000-01-01; each invested
    row's holdings names every booked name (books of more than 10, so a cut shows); --full-holdings adds
    holdings_full, every booked name in the holdings' order, and changes nothing else. The results file: after
    isin_dedup come data_checks, universe_rules, signal_guard (SET_GUARD) and cost_model (then the LTP keys), the
    fetch asks for no MTUM, cost_model the tiers at 35
    baht a dollar, the rate run_single passes to run_backtest, and null on a no-costs run; each row carries
    entry_unpriced (UNP.BK, which ranks with no entry bar, on every priced row; null when the screen held cash) and
    the data checks' counts; each holdings_full item records the one-way rate it paid (cost_rate: 0.5% here,
    0.1% at one baht a dollar; null on a no-costs run), and each row's return is its items' less the round
    trip."""
    try:
        mod = load(src)
        fx = []
        run_bt = mod.run_backtest
        mod.run_backtest = lambda *a, **k: fx.append(k.get("fx")) or run_bt(*a, **k)
        with tempfile.TemporaryDirectory() as tmp:
            plain, sqls = tdc.run_topic(TOPIC, set_leg(), "ltp", "on", tmp, mod=mod)
            full = tdc.run_topic(TOPIC, set_leg(), "ltp", "on", tmp, mod=mod, full_holdings=True)[0]
            free = tdc.run_topic(TOPIC, set_leg(), "ltp", "on", tmp, mod=mod, full_holdings=True, use_costs=False,
                                 tag=".free")[0]
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
        if res.get("cost_model", "missing") != (None if name == "no costs" else COST_MODEL):
            out.append(f"{name}: cost_model {res.get('cost_model', 'missing')}")
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
    # check N: an NVDR line shares its ordinary line's security (twin_key on the dedup), so TWN.BK holds the slot
    # and STD-R.BK stands in for STD.BK, which ranks with no entry bar
    held_at, invested = tdc.held_dates(rows)
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
    "a name with no cap charged nothing": ("if use_costs else None for s, _, mc in clean",
                                           "if use_costs and mc else None for s, _, mc in clean"),
    "cost_model not written": ('"cost_model": {"function": "costs.tiered_cost", "fx_per_usd": fx} if use_costs else None',
                               '"cost_model": None'),
    "cost_model written on a no-costs run": ('"fx_per_usd": fx} if use_costs else None', '"fx_per_usd": fx}'),
    "cost_rate not written": ("cost_rates=rates)", "cost_rates=None)"),
    "data_checks after the other blocks": (
        '    output["data_checks"] = dc.block()   # window bars and the screen only: equal in drop, ltp and both\n',
        ""),
    "no unpriced names": ("unpriced = sorted(s for s in symbols if not entry_usable(entry_prices.get(s), MIN_ENTRY_PRICE))",
                          "unpriced = []"),
    "the signal guard's block not written": ('        "signal_guard": gb,', ""),
    "MTUM fetched again": ("            symbol IN ({bench_list})\n",
                           "            symbol IN ({bench_list})\n            OR symbol = 'MTUM'\n"),
}


# run_backtest on one leg, the screen and prices stubbed: ten holdings at one market cap (cap_used) in the leg's
# currency, each up 10% before costs. The cost tiers are in USD (over 10B 0.1%, over 2B 0.3%, else 0.5%), converted
# at the leg's rate (costs.get_fx_per_usd); a name with no cap pays the top rate.
COST_ENTRY, COST_EXIT, COST_RAW = date(2020, 4, 1), date(2020, 7, 1), 0.10
COST_CASES = [   # (leg, market cap in its currency, one-way rate): JPX at 150 yen, LSE at 0.79 pounds a dollar
    ("JPX", 1e12, 0.003),    # 1 trillion yen is about 6.7B dollars: the 0.3% tier, where 1e12 > 10B would read 0.1%
    ("LSE", 8e9, 0.001),     # 8B pounds is about 10.1B dollars: the 0.1% tier, where 8e9 < 10B would read 0.3%
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
        want = round(COST_RAW - 2 * rate, 6)
        if got[0]["portfolio_return"] != want:
            out.append(f"costs: {exchange} at {cap} books {got[0]['portfolio_return']}, want {want} (the {rate:.1%} tier)")
    return out


def run_failures(src):
    return cost_unit_failures(src) + results_failures(src)


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
    same one's under its old ticker) carry a high of 200 over a close of 100, the line's own close on their high."""
    bars = {k: [100.0, 100.0, 200.0 if k < 0 else 100.0, 1e6] for k in range(-250, 131)}
    return line(bars, at=first, after=(), filings=[(first - timedelta(days=400), 1e10)])


CLIPS = {s: started(first) for s, first in STARTS.items()}
CLIP_SCREENS = {"GETY": {-1: [], 70: [], 130: [("GETY", 1e10, 1.0)]}, "DOC": {-1: [], 70: [("DOC", 1e10, 0.5)]}}
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
}


# The gate's legs (scripts/gate_legs/wave3.json) are --global's presets_to_run, both ways, ASX and SAO among them.
LEGS_FILE = os.path.join(ROOT, "scripts", "gate_legs", "wave3.json")
LEG_PIN = ["--end-year", "2025"]
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


def legs_failures(src):
    """wave3.json's 52-week-high legs, one per --global preset and no other: each runs the preset's exchanges pinned to
    --end-year 2025 before its exchange args, and installs as the returns_<exchanges>.json the global run writes."""
    try:
        want = global_presets(src)
        with open(LEGS_FILE) as fh:
            spec = json.load(fh)[TOPIC]
        out = [f"legs {sorted(spec[k])} != presets {sorted(want)}" for k in ("legs", "_outputs")
               if set(spec[k]) != set(want)]
        for name, ex in want.items():
            args = spec["legs"].get(name) or []
            if args[:len(LEG_PIN)] != LEG_PIN or len(args) != len(LEG_PIN) + 2 or leg_exchanges(args) != ex:
                out.append(f"leg {name}: {args}, want {LEG_PIN} then {ex}")
            if spec["_outputs"].get(name) != f"returns_{'_'.join(ex)}.json":
                out.append(f"leg {name} installs as {spec['_outputs'].get(name)}")
        out += [f"{sorted(m)} not ranked" for m in RANKED_ADDED if m not in map(set, want.values())]
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    return out


LEG_MUTANTS = {"australia left out of --global": ('            ("australia",   ["ASX"]),\n', "")}


# Check T on the ranking cache: T_R, a rebalance date, and T_BACK, 40 days before it and outside every window, are
# closed days on test_data_checks' synthetic leg (only A00.BK to A02.BK trade, as on its CLOSED days) where one line
# prints a traded copy: T_COPY at 80% of its close on T_R (under the oscillation filter's thresholds), T_KEPT with
# twice its high on T_BACK.
T_R = date(2014, 1, 1)
T_BACK = T_R - timedelta(days=40)
T_COPY, T_KEPT = "A10.BK", "A11.BK"


def t_leg():
    if "t" not in _LEGS:
        src = tdc.wh52_source()
        for d, s in ((T_R, T_COPY), (T_BACK, T_KEPT)):
            src.execute("UPDATE stock_eod SET volume = 0 WHERE date = ? AND symbol NOT IN"
                        " ('A00.BK', 'A01.BK', 'A02.BK', '^SET.BK', 'SPY', ?)", [d.isoformat(), s])
        src.execute("UPDATE stock_eod SET adjClose = adjClose * 0.8, close = close * 0.8, high = high * 0.8"
                    " WHERE date = ? AND symbol = ?", [T_R.isoformat(), T_COPY])
        src.execute("UPDATE stock_eod SET high = high * 2 WHERE date = ? AND symbol = ?", [T_BACK.isoformat(), T_KEPT])
        _LEGS["t"] = src
    return _LEGS["t"]


def t_failures(src):
    """The fetch with data checks on cleans the grid's windows [d, d + 11] (check T) before the screen reads them:
    T_R's copy moves, so T_COPY ranks on its last traded bar before T_R, at 1.0, while T_BACK's copy stays in
    T_KEPT's lookback and its high holds T_KEPT's ratio near 0.5. The block lists T_R as a closed date, not T_BACK."""
    try:
        mod, dc = load(src), du.DataChecks("on", du.DATA_CHECKS_V3)
        with contextlib.redirect_stdout(io.StringIO()):
            con = mod.fetch_data_via_api(tdc.FakeClient(t_leg()), ["SET"], [date(2013, 10, 1), T_R], mktcap_min=1e9,
                                         dc=dc)
            prox = {s: p for s, _, p in mod.screen_stocks(con, T_R, 1e9, dc=dc)}
    except Exception as e:
        return [f"error: {type(e).__name__}: {e}"]
    out = []
    if prox.get(T_COPY) != 1.0 or not 0.4 < (prox.get(T_KEPT) or 1.0) < 0.6:
        out.append(f"ratios {T_COPY} {prox.get(T_COPY)}, want 1.0; {T_KEPT} {prox.get(T_KEPT)}, want about 0.5")
    closed = [x["date"] for x in dc.block()["closed_dates"]]
    if T_R.isoformat() not in closed or T_BACK.isoformat() in closed:
        out.append(f"closed dates {closed}: want {T_R} and not {T_BACK}")
    return out


T_CALL = "clean_price_cache(con, bench_symbols, rebalance_dates, dc.cfg"
T_MUTANTS = {
    "check T on no window": (T_CALL, T_CALL.replace("rebalance_dates", "[]")),
    "check T over the lookback": (T_CALL, T_CALL.replace(
        "rebalance_dates", "[x for d in rebalance_dates for x in (d - timedelta(days=40), d)]")),
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


def mutant_results(mutants, check, must_book=()):
    """(survived, errored): the mutants of SRC no check catches, and those in must_book caught only by an error."""
    survived, errored = [], []
    for name, (old, new) in mutants.items():
        if SRC.count(old) != 1:
            print(f"test_52wh_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        got = check(SRC.replace(old, new))
        if not got:
            survived.append(name)
        elif name in must_book and all(f.startswith("error:") for f in got):
            errored.append(name)
    return survived, errored


def main():
    base = (legs_failures(SRC) + failures(SRC) + rules_failures(SRC) + run_failures(SRC) + partial_failures(SRC)
            + t_failures(SRC))
    if base:
        print("test_52wh_selection: FAIL", base)
        sys.exit(1)
    survived, errored = mutant_results(MUTANTS, failures, TRADED_MUTANTS)
    s1, e1 = mutant_results(RULE_MUTANTS, rules_failures, RULE_MUTANTS)
    s2, e2 = mutant_results(RESULT_MUTANTS, run_failures, RESULT_MUTANTS)
    s3, _ = mutant_results(WINDOW_MUTANTS, partial_failures)
    s4, e4 = mutant_results(T_MUTANTS, t_failures, T_MUTANTS)
    s5, e5 = mutant_results(LEG_MUTANTS, legs_failures, LEG_MUTANTS)
    survived, errored = survived + s1 + s2 + s3 + s4 + s5, errored + e1 + e2 + e4 + e5
    if survived or errored:
        print("test_52wh_selection: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    n_rules = len(SAO) + len(JPX) + len(NSE_RULES) + len(ENDS) + len(CLIPS)
    print(f"test_52wh_selection: PASS ({len(SIGNAL) + len(CUT) + len(CAPS)} listings, {len(MUTANTS)} mutants caught;"
          f" universe rules ur-4 on {n_rules} listings, {len(RULE_MUTANTS)} mutants caught; the run on the synthetic"
          f" SET leg, {len(RESULT_MUTANTS)} mutants caught; the run window, {len(WINDOW_MUTANTS)} mutants caught;"
          f" check T, {len(T_MUTANTS)} mutants caught; the gate's legs, {len(LEG_MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
