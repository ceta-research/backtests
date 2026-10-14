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
and the 100-bar floor; an untraded twin of the signal bar sorts after it and takes no slot. The
topic's own fetch SQL fills the caches from made-up warehouse rows, so it must carry close and
volume and filter on neither. They are fetched over two rebalance dates, so they are
continuous as in a real run and only the screen's own window start keeps a bar older than
R - 390d out; a one-date cache is the first rebalance, where the fetch's start is the bound.
The same-security dedup still runs before the top-30 cut, on the run's own entry offset. Every
mutant of the topic's source must trip a check; the traded-bar rule's must trip it on a wrong
book, not an error.

Usage:
    python scripts/test_52wh_selection.py [repo root]
"""
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "52-week-high", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

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


def line(bars, cap=1e10, isin=None, filings=None, after=((1, 5.0),), dup=()):
    """One listing. `after`: (day, adjClose) bars past R, each closing on a new high the signal must not read."""
    rows = [(k, *v) for k, v in bars.items()] + [(k, adj, 1000.0, 1000.0, 1e6) for k, adj in after] + list(dup)
    return {"bars": rows, "filings": filings or [(FY19, cap)], "isin": isin}


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
    # a close 10% over the window's high is kept, one tick more is a data error
    "CAP_IN": line(bar(flat(120, 1000.0), 0, adj=1100.0, close=1100.0)),
    "CAP_OUT": line(bar(flat(120, 1000.0), 0, adj=1101.0, close=1101.0)),
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


def signal_book(off, e11):
    """Proximity descending, then cap descending, then symbol."""
    return [("CAP_IN", 1e10, 1100.0 / 1000.0), ("PAYER", 9e10, 1.0), ("LIMIT_UP", 8.5e10, 1.0), ("W253", 8e10, 1.0),
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
}


def sql(v):
    return "NULL" if v is None else f"'{v}'" if isinstance(v, str) else repr(v)


def values(rows):
    return ", ".join("(" + ", ".join(sql(v) for v in r) + ")" for r in rows)


def seed(con, uni):
    """The made-up warehouse (profile, key_metrics, stock_eod) and the leg's isin_map."""
    con.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR)")
    con.execute("CREATE TABLE key_metrics(symbol VARCHAR, marketCap DOUBLE, dateEpoch BIGINT, period VARCHAR)")
    con.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE, close DOUBLE,"
                " high DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    con.execute("INSERT INTO profile VALUES " + values((s, EX) for s in uni))
    con.execute("INSERT INTO isin_map VALUES " + values((s, x["isin"], True, True) for s, x in uni.items()))
    con.execute("INSERT INTO key_metrics VALUES "
                + values((s, cap, ep(d), "FY") for s, x in uni.items() for d, cap in x["filings"]))
    con.execute("INSERT INTO stock_eod VALUES " + values(
        (s, (D + timedelta(days=k)).isoformat(), ep(D + timedelta(days=k)), adj, close, high, vol)
        for s, x in uni.items() for k, adj, close, high, vol in x["bars"]))


class _Client:
    """Stands in for CetaResearch in fetch_data_via_api: the universe's profile rows, nothing else."""

    def __init__(self, uni):
        self.uni = uni

    def query(self, sql, **kw):
        return [{"symbol": s, "exchange": EX} for s in self.uni]


def cache(mod, uni, dates):
    """The topic's own fetch over the made-up warehouse for `dates`: its SQL builds metrics_cache and prices_cache."""
    seeded = []

    def warehouse(con):
        if not seeded:
            seeded.append(seed(con, uni))

    def fake_isin_map(client, con, *a, **kw):
        warehouse(con)
        return len(uni)

    def fake_query_parquet(client, sql, con, table_name, **kw):
        warehouse(con)
        con.execute(f"CREATE TABLE {table_name} AS {sql}")
        return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    mod.load_isin_map, mod.query_parquet = fake_isin_map, fake_query_parquet
    mod.remove_price_oscillations = lambda *a, **k: None
    mod.print = lambda *a, **k: None
    return mod.fetch_data_via_api(_Client(uni), [EX], dates)


def failures(src):
    try:
        mod = load(src)
        # [PREV, D]: bars older than R-390 are in the cache, as on every date but a run's first; [D]: the first
        sig, first, cut = cache(mod, SIGNAL, [PREV, D]), cache(mod, SIGNAL, [D]), cache(mod, CUT, [PREV, D])
        got = {"signal, entry next day": mod.screen_stocks(sig, D, FLOOR, offset_days=1),
               "signal, entry same day": mod.screen_stocks(sig, D, FLOOR, offset_days=0),
               "signal, first rebalance date": mod.screen_stocks(first, D, FLOOR, offset_days=1),
               "ties at the cut": mod.screen_stocks(cut, D, FLOOR, offset_days=1)}
        sig.close()
        first.close()
        cut.close()
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
WRAP = "FROM ({isin_dedup_sql(candidates, target_date, offset_days, MIN_ENTRY_PRICE)})"
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
    "no close test": ("          AND l.close > 0\n", ""),
    "smaller close of two bars on R": ("ORDER BY trade_epoch DESC, close DESC NULLS LAST",
                                       "ORDER BY trade_epoch DESC, close ASC NULLS LAST"),
    # the window, the history floor and the data-error cap, each one unit either side
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
    "history floor exclusive": (">= {MIN_HISTORY_ROWS}", "> {MIN_HISTORY_ROWS}"),
    "cap exclusive": ("<= {MAX_PROXIMITY}", "< {MAX_PROXIMITY}"),
    "cap a tick higher": ("MAX_PROXIMITY = 1.1 ", "MAX_PROXIMITY = 1.101 "),
    "no cap": ("          AND l.close / l.high_52w <= {MAX_PROXIMITY}\n", ""),
    # the size floor: latest filing at the 45-day cutoff first, then the test
    "size floor inclusive": ("          AND marketCap > ?", "          AND marketCap >= ?"),
    "no size floor": ("          AND marketCap > ?", "          AND ? IS NOT NULL"),
    "size test before latest": ("            WHERE filing_epoch <= ?\n",
                                "            WHERE filing_epoch <= ? AND marketCap > 500000000\n"),
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
    "no dedup": (WRAP, "FROM ({candidates})"),
    "cut before dedup": (WRAP, WRAP.replace("candidates,", "'SELECT * FROM (' + candidates + ') "
                                            + CUT_BY.strip() + " LIMIT 30',")),
    "entry offset not passed": ("compute_proximity(con, eligible, target_date, offset_days=offset_days)",
                                "compute_proximity(con, eligible, target_date)"),
    "entry floor a cent lower": ("MIN_ENTRY_PRICE = 1.0 ", "MIN_ENTRY_PRICE = 0.99 "),
    "entry floor a cent higher": ("MIN_ENTRY_PRICE = 1.0 ", "MIN_ENTRY_PRICE = 1.01 "),
    "prices cached to R+10": ("entry_end = d + timedelta(days=11)", "entry_end = d + timedelta(days=10)"),
    **TRADED_MUTANTS,
}


def main():
    base = failures(SRC)
    if base:
        print("test_52wh_selection: FAIL", base)
        sys.exit(1)
    survived, errored = [], []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_52wh_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        got = failures(SRC.replace(old, new))
        if not got:
            survived.append(name)
        elif name in TRADED_MUTANTS and all(f.startswith("error:") for f in got):
            errored.append(name)
    if survived or errored:
        print("test_52wh_selection: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    print(f"test_52wh_selection: PASS ({len(SIGNAL) + len(CUT)} listings, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
