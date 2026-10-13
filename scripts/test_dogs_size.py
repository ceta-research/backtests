#!/usr/bin/env python3
"""Offline check of dogs-of-dow's non-US blue-chip size (no network).

A non-US line is sized as the SMALLER of two market caps, both in the leg's currency
(data_utils.get_local_currency: pounds on LSE, rand on JNB, one currency for TSX+TSXV, USD on a
mixed or global leg):
  A  key_metrics.marketCap of the latest FY filing (45-day lag, at most MAX_FILING_AGE_DAYS old),
     restated from its reportedCurrency;
  B  the entry bar's close x weightedAverageShsOut of the latest FY income statement (same bounds),
     x0.01 on sub-unit quotes (GBp, ZAc, ILA), restated from the line's own quote currency.
Restating uses the newest USDxxx close on or before the entry bar, at most FX_MAX_STALE_DAYS old;
a figure with no currency or no fresh rate does not exist. With one figure missing the other is
the size; a line with neither is out. A line takes a blue-chip slot only if the first bar in
[d+offset, d+offset+10] has adjClose >= 1.0 (the bar get_prices, the same-ISIN dedup and
entry_buyable read) and its size is at or above the floor and at most SIZE_CEILING_MULT (15000)
times the floor: a larger size is a data error. Then one line per ISIN, the 30 largest
by size (ties by symbol), the 10 highest yields. The Dow branch is not sized and fetches none of
this. Results record, per rebalance and in total, the lines whose reported cap clears the floor
but which had no entry price, no size, or (priced) a size over the ceiling.

Every case drives the topic's real fetch_data_via_api (a fake client running its SQL on a local
DuckDB) and screen_dogs; main() is run end to end for the results. Every mutant of the topic's
source must trip its own named check.

Usage:
    python scripts/test_dogs_size.py [repo root]
"""
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
PATH = os.path.join(ROOT, "dogs-of-dow", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)
from data_utils import LOCAL_CURRENCY, QUOTE_CURRENCY  # noqa: E402

D = date(2020, 1, 1)
BAR = D + timedelta(days=1)             # first day of the entry window with offset_days=1
FY = date(2019, 9, 30)                  # 93 days before D
FY_PREV = date(2018, 9, 30)             # 458 days before D
NEXT, LAST = date(2021, 1, 1), date(2022, 1, 1)


def day(n):
    return D + timedelta(days=n)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    """The topic module built from `src` (the real file, or a mutant of it)."""
    mod = types.ModuleType("dogs_size")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def L(sym, cap=None, shs=None, rccy="HKD", ccy="HKD", ex="HKSE", px=10.0, close=None, bars=None,
      caps=None, shares=None, yld=0.05, isin=None):
    """One made-up line: FY market caps [(date, cap, reportedCurrency)], share counts [(date, n)],
    bars [(date, adjClose, close)] (default: one bar on BAR), quote currency, latest yield."""
    return {"sym": sym, "ex": ex, "ccy": ccy, "isin": isin, "yld": yld,
            "caps": caps if caps is not None else ([(FY, cap, rccy)] if cap is not None else []),
            "shares": shares if shares is not None else ([(FY, shs)] if shs is not None else []),
            "bars": bars if bars is not None else [(BAR, px, px if close is None else close)]}


def source(lines, fx=(), dow=(), bench=()):
    """The warehouse tables the fetch reads (column types as in the live schema)."""
    src = duckdb.connect()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR,"
                " isActivelyTrading BOOLEAN, currency VARCHAR)")
    src.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, dateEpoch INTEGER, marketCap DOUBLE,"
                " reportedCurrency VARCHAR)")
    src.execute("CREATE TABLE financial_ratios(symbol VARCHAR, period VARCHAR, dateEpoch INTEGER,"
                " dividendYield DOUBLE, dividendPerShare DOUBLE)")
    src.execute("CREATE TABLE income_statement(symbol VARCHAR, period VARCHAR, dateEpoch INTEGER,"
                " weightedAverageShsOut DOUBLE)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date VARCHAR, dateEpoch INTEGER, adjClose DOUBLE,"
                " close DOUBLE, volume DOUBLE)")
    src.execute("CREATE TABLE forex_prices_daily(symbol VARCHAR, date VARCHAR, dateEpoch INTEGER, close DOUBLE)")
    src.execute("CREATE TABLE dowjones_constituent(symbol VARCHAR)")
    for ln in lines:
        s = ln["sym"]
        src.execute("INSERT INTO profile VALUES (?, ?, 'XX', ?, TRUE, ?)", [s, ln["ex"], ln["isin"], ln["ccy"]])
        for d, cap, rccy in ln["caps"]:
            src.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, ?)", [s, ep(d), cap, rccy])
        for d, n in ln["shares"]:
            src.execute("INSERT INTO income_statement VALUES (?, 'FY', ?, ?)", [s, ep(d), n])
        src.execute("INSERT INTO financial_ratios VALUES (?, 'FY', ?, ?, 1.0)", [s, ep(FY), ln["yld"]])
        for d, adj, close in ln["bars"]:
            src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, ?, ?, 1e6)", [s, d.isoformat(), ep(d), adj, close])
    for pair, d, rate in fx:
        src.execute("INSERT INTO forex_prices_daily VALUES (?, ?, ?, ?)", [pair, d.isoformat(), ep(d), rate])
    for s in dow:
        src.execute("INSERT INTO dowjones_constituent VALUES (?)", [s])
    for s in bench:                     # a benchmark bar in every January window, so main() has valid periods
        for y in range(2000, 2026):
            d = date(y, 1, 2)
            src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, 100.0, 100.0, 1e6)", [s, d.isoformat(), ep(d)])
    return src


class FakeClient:
    """Stands in for CetaResearch: runs the topic's own SQL on a source DB. An empty parquet result
    comes back as no bytes, so query_parquet's column-less placeholder table is what the fetch sees."""

    def __init__(self, src):
        self.src, self.sqls = src, []

    def query(self, sql, format="json", **_kw):
        self.sqls.append(sql)
        if format != "parquet":
            cur = self.src.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        if not self.src.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]:
            return None
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.parquet")
            self.src.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
            with open(path, "rb") as fh:
                return fh.read()


def fetch(mod, src, exchanges, dates=(D,)):
    client = FakeClient(src)
    with contextlib.redirect_stdout(io.StringIO()):
        con = mod.fetch_data_via_api(client, exchanges, list(dates))
    return con, client.sqls


@contextlib.contextmanager
def cuts(mod, bluechips, dogs):
    saved = mod.BLUECHIP_COUNT, mod.DOGS_COUNT
    mod.BLUECHIP_COUNT, mod.DOGS_COUNT = bluechips, dogs
    try:
        yield
    finally:
        mod.BLUECHIP_COUNT, mod.DOGS_COUNT = saved


def sizes(mod, con, floor, offset_days=1):
    """{symbol: size} of every line the screen admits (both cuts opened wide, every line pays a dividend)."""
    with cuts(mod, 999, 999):
        return dict(mod.screen_dogs(con, D, use_dow=False, mktcap_min=floor, offset_days=offset_days))


# ---- Hong Kong leg (HKD), floor 2e9. USDxxx closes, dated against the entry bar (BAR = D+1). ----
HK_FLOOR = 2e9
FX_HK = [
    ("USDHKD", BAR, 7.8), ("USDHKD", day(-2), 7.75), ("USDHKD", day(11), 7.9),
    ("USDCNY", BAR, 7.0), ("USDCNY", BAR, 6.0),               # two closes on one day: the larger is kept
    ("USDCNY", day(10), 7.1),
    ("USDJPY", BAR - timedelta(days=3), 110.0),               # 3 days old: used
    ("USDGBP", BAR - timedelta(days=7), 0.8),                 # exactly 7 days old: used
    ("USDEUR", BAR - timedelta(days=8), 0.9),                 # 8 days old: too old (7 days before a bar on D)
    ("USDTHB", day(11), 30.0),
    # dated after the bar: must never be used
    ("USDHKD", BAR + timedelta(days=1), 100.0), ("USDCNY", BAR + timedelta(days=1), 1.0),
    ("CNYUSD", BAR, 9.9),                                     # the inverse pair is never read
]
# symbol -> (line, size the screen must give it; None = it takes no slot)
HK = {
    # the smaller of the two figures, and each fallback
    "DOM.HK": (L("DOM.HK", 5e10, 6e9), 5e10),                           # A 5e10 < B 6e10
    "BSM.HK": (L("BSM.HK", 9e10, 3e9), 3e10),                           # B 3e10 < A 9e10
    "NOSHS.HK": (L("NOSHS.HK", 4.4e10), 4.4e10),                        # no share count: A
    "NOCAP.HK": (L("NOCAP.HK", None, 5.2e9), 5.2e10),                   # no key_metrics filing: B
    "ZCAP.HK": (L("ZCAP.HK", 0.0, 4.6e9), 4.6e10),                      # a cap of zero is no figure: B
    "ADJ.HK": (L("ADJ.HK", 9e10, 2e9, px=5.0, close=12.0), 2.4e10),     # B is on close, not adjClose
    # A restated from its reporting currency
    "CNY.HK": (L("CNY.HK", 7e10, 9e9, rccy="CNY"), 7e10 * 7.8 / 7.0),
    "USD.HK": (L("USD.HK", 5e9, 9e9, rccy="USD"), 5e9 * 7.8),
    "JPY.HK": (L("JPY.HK", 2.2e11, 9e9, rccy="JPY"), 2.2e11 * 7.8 / 110.0),
    "GBP7.HK": (L("GBP7.HK", 2e9, 9e9, rccy="GBP"), 2e9 * 7.8 / 0.8),   # rate exactly 7 days old
    "EUR8.HK": (L("EUR8.HK", 3e9, 4e9, rccy="EUR"), 4e10),              # rate 8 days old: A is gone, B stands
    "EUR8N.HK": (L("EUR8N.HK", 3e9, rccy="EUR"), None),                 # ... and with no B the line is out
    "RUB.HK": (L("RUB.HK", 1e12, rccy="RUB"), None),                    # no rate at all, no B: out
    "RUBS.HK": (L("RUBS.HK", 1e9, rccy="RUB"), None),                   # the same under the floor: not counted
    "NULLR.HK": (L("NULLR.HK", 6e10, 9e9, rccy=None), 9e10),            # no reporting currency: A is gone
    # B restated from the line's quote currency, or gone without one
    "CNYQ.HK": (L("CNYQ.HK", 8e10, 6.3e9, rccy="CNY", ccy="CNY"), 6.3e10 * 7.8 / 7.0),
    "EURQ.HK": (L("EURQ.HK", 8.8e10, 1e9, ccy="EUR"), 8.8e10),          # quote rate 8 days old: B is gone
    "NOCCY.HK": (L("NOCCY.HK", 3.3e10, 1e9, ccy=None), 3.3e10),         # no quote currency: B is gone
    # the entry bar: get_prices' first bar in [d+1, d+11] with adjClose >= 1.0
    "UNPX.HK": (L("UNPX.HK", 9e11, 9e10, bars=[]), None),
    "UNPXS.HK": (L("UNPXS.HK", 1e9, 9e10, bars=[]), None),              # unpriced and under the floor: not counted
    "PENNY.HK": (L("PENNY.HK", 8e11, 9e11, px=0.99), None),
    "ONE.HK": (L("ONE.HK", 3.1e10, 5e10, px=1.0), 3.1e10),
    "FIRST.HK": (L("FIRST.HK", 7e11, 9e10, bars=[(BAR, 0.5, 0.5), (day(2), 50.0, 50.0)]), None),
    "ADJLOW.HK": (L("ADJLOW.HK", 7.2e11, 9e10, px=0.52, close=45.5), None),          # the test is on adjClose, not close
    "ADJNUL.HK": (L("ADJNUL.HK", 7.3e11, 9e10, bars=[(BAR, None, 10.0)]), None),     # a bar with no adjClose
    "DAY0.HK": (L("DAY0.HK", 3e9, rccy="EUR", bars=[(D, 10.0, 10.0)]), None),       # before the offset window
    "DAY0B.HK": (L("DAY0B.HK", 4e10, bars=[(D, 10.0, 10.0)]), None),
    "DAY10.HK": (L("DAY10.HK", 7e10, rccy="CNY", bars=[(day(10), 10.0, 10.0)]), None),  # HKD rate 8 days old
    "DAY11.HK": (L("DAY11.HK", 1.2e11, rccy="THB", bars=[(day(11), 10.0, 10.0)]), 1.2e11 * 7.9 / 30.0),
    "DAY12.HK": (L("DAY12.HK", 6e11, 9e10, bars=[(day(12), 10.0, 10.0)]), None),
    # the floor is on the final size
    "FLOORB.HK": (L("FLOORB.HK", 3e9, 1.9e8), None),                    # A 3e9 clears it, the size 1.9e9 does not
    "FLOORA.HK": (L("FLOORA.HK", 1.9e9, 3e8), None),
    "ATFLOOR.HK": (L("ATFLOOR.HK", 2e9, 5e8), 2e9),                     # exactly the floor: in
    # the ceiling is on the final size too: 15000 x the floor = 3e13
    "ATCEIL.HK": (L("ATCEIL.HK", 3e13, 4e12), 3e13),                    # exactly the ceiling: in
    "OVCEIL.HK": (L("OVCEIL.HK", 3e13 + 1, 4e12), None),                # one over: out
    "CEILB.HK": (L("CEILB.HK", 9e13, 3e12), 3e13),                      # the size is B, exactly the ceiling: in
    "OVERB.HK": (L("OVERB.HK", 9e13, 3e12 + 1), None),                  # B 3e13 + 10, A over as well: out
    "AOVER.HK": (L("AOVER.HK", 5e13, 3e9), 3e10),                       # A is over, the size is B: in
    "BOVER.HK": (L("BOVER.HK", 3e10, 5e12), 3e10),                      # B 5e13 is over, the size is A: in
    "AONLY.HK": (L("AONLY.HK", 5e13), None),                            # a cap alone, over: out
    "BONLY.HK": (L("BONLY.HK", None, 5e12), None),                      # price x shares alone, over: out (no cap, not counted)
    "OVUNPX.HK": (L("OVUNPX.HK", 5e13, 5e12, bars=[]), None),           # over and unpriced: counted as unpriced
    "CTA.HK": (L("CTA.HK", 5e13, 5e12, isin="HK0000000003"), None),     # an over-ceiling line does not hold
    "CTB.HK": (L("CTB.HK", 6.4e10, 9e9, isin="HK0000000003"), 6.4e10),  # ... its security's slot
    # a corrupt cap loses its slot, or is cut down to price x shares
    "CORRUPT.HK": (L("CORRUPT.HK", 3.8e18, 6.7e7), None),
    "CORRLO.HK": (L("CORRLO.HK", 1.5e13, 6.7e7), None),                 # inflated but under the ceiling: out on B
    "INFL.HK": (L("INFL.HK", 9e15, 2.7e9), 2.7e10),
    # the share-count filing: 45-day lag, at most 540 days old, the newest one
    "SHS540.HK": (L("SHS540.HK", 9.5e10, shares=[(day(-540), 2.1e9)]), 2.1e10),
    "SHS541.HK": (L("SHS541.HK", 9.6e10, shares=[(day(-541), 2e9)]), 9.6e10),
    "LAG45.HK": (L("LAG45.HK", 9.7e10, shares=[(FY, 8e9), (day(-45), 2.2e9)]), 2.2e10),
    "LAG44.HK": (L("LAG44.HK", 9.8e10, shares=[(FY, 8.1e9), (day(-44), 2e9)]), 8.1e10),
    "NEWSHS.HK": (L("NEWSHS.HK", 9.9e10, shares=[(FY_PREV, 9e9), (FY, 2.3e9)]), 2.3e10),
    # duplicate rows resolve the same way every run
    "DUPS.HK": (L("DUPS.HK", 9.4e10, shares=[(FY, 1e9), (FY, 2.6e9)]), 2.6e10),
    "DUPM.HK": (L("DUPM.HK", shs=9e9, caps=[(FY, 1e10, "HKD"), (FY, 2.8e10, "HKD")]), 2.8e10),
    "DUPR.HK": (L("DUPR.HK", shs=9e9, caps=[(FY, 3.2e10, "CNY"), (FY, 3.2e10, "HKD")]), 3.2e10),
    "DUPB.HK": (L("DUPB.HK", 9.3e10, 1.7e9, bars=[(BAR, 10.0, 9.0), (BAR, 10.0, 11.0)]), 1.87e10),
    # one security, two lines: the line that clears the floor / trades is the one kept
    "TWA.HK": (L("TWA.HK", 1e9, 1e8, isin="HK0000000001"), None),
    "TWB.HK": (L("TWB.HK", 6.6e10, 9e9, isin="HK0000000001"), 6.6e10),
    "UTA.HK": (L("UTA.HK", 6.2e10, bars=[], isin="HK0000000002"), None),
    "UTB.HK": (L("UTB.HK", 6.2e10, 7e9, isin="HK0000000002"), 6.2e10),
}
# Lines with a reported cap at or above the floor that the rule drops at D ...
HK_NO_PRICE = ["UNPX.HK", "PENNY.HK", "FIRST.HK", "ADJLOW.HK", "ADJNUL.HK", "DAY0.HK", "DAY0B.HK", "DAY12.HK", "UTA.HK",
               "OVUNPX.HK"]
HK_NO_SIZE = ["EUR8N.HK", "RUB.HK", "DAY10.HK"]
HK_OVER = ["OVCEIL.HK", "OVERB.HK", "AONLY.HK", "CTA.HK"]     # priced, sized over the ceiling
# ... and a year on, when every FY2019 filing is still in range and nothing has a bar: every such line.
HK_CAPPED = [s for s, (ln, _) in HK.items() if any(c is not None and c >= HK_FLOOR for _, c, _ in ln["caps"])]

# Without a USDHKD close (the leg's own rate): HKD-reported caps and HKD quotes need none.
NO_LEG_RATE = {"DOM.HK": 5e10, "BSM.HK": 3e10, "CNY.HK": 9e10, "USD.HK": 9e10, "CNYQ.HK": None}
# offset_days=0: the window is [d, d+10], and DAY0's EUR rate is exactly 7 days older than its bar.
OFFSET0 = {"DAY0.HK": 3e9 * 7.75 / 0.9, "DAY0B.HK": 4e10, "DAY11.HK": None, "DOM.HK": 5e10,
           "CNY.HK": 7e10 * 7.8 / 7.0}

# ---- Johannesburg leg (ZAR): cent, rand, pence, agorot and dollar quotes side by side ----
JNB_FLOOR = 1e10
FX_JNB = [("USDZAR", BAR, 15.0), ("USDGBP", BAR, 0.8), ("USDILS", BAR, 3.5)]
JNB = {
    "CENT.JO": (L("CENT.JO", 9e10, 3e8, rccy="ZAR", ccy="ZAc", ex="JNB", px=20000.0), 6e10),        # x0.01
    "RAND.JO": (L("RAND.JO", 9e10, 2.5e8, rccy="ZAR", ccy="ZAR", ex="JNB", px=200.0), 5e10),        # no x0.01
    "PNCE.JO": (L("PNCE.JO", 4e9, 6e8, rccy="GBP", ccy="GBp", ex="JNB", px=500.0), 3e9 * 15.0 / 0.8),
    "AGOR.JO": (L("AGOR.JO", 2e10, 4e8, rccy="ILS", ccy="ILA", ex="JNB", px=4000.0), 1.6e10 * 15.0 / 3.5),
    "USDQ.JO": (L("USDQ.JO", 5e9, 9e7, rccy="USD", ccy="USD", ex="JNB", px=50.0), 4.5e9 * 15.0),
}

# ---- London leg (GBP; pence lines only): the leg's unit is the pound, not the penny ----
LSE_FLOOR = 5e8
FX_LSE = [("USDGBP", BAR, 0.8)]
LSE = {
    "VOD.L": (L("VOD.L", 3e10, 2.6e10, rccy="GBP", ccy="GBp", ex="LSE", px=150.0), 3e10),
    "EXPN.L": (L("EXPN.L", 3e10, 9e8, rccy="USD", ccy="GBp", ex="LSE", px=2500.0), 2.25e10),
    "POUND.L": (L("POUND.L", 4e10, 9e9, rccy="GBP", ccy="GBP", ex="LSE"), None),    # the listing rule drops it
    # the ceiling is in pounds too: 15000 x the floor = 7.5e12
    "BIGP.L": (L("BIGP.L", 2e11, 1e10, rccy="GBP", ccy="GBp", ex="LSE", px=2500.0), 2e11),      # 400x the floor: in
    "OVERP.L": (L("OVERP.L", 9e12, 3.2e11, rccy="GBP", ccy="GBp", ex="LSE", px=2500.0), None),  # B 8e12, A over as well: out
}

# ---- Canada (TSX + TSXV): two exchanges, one currency ----
TSX_FLOOR = 5e8
FX_TSX = [("USDCAD", BAR, 1.3)]
TSX = {
    "RY.TO": (L("RY.TO", 1.5e11, 1.4e10, rccy="CAD", ccy="CAD", ex="TSX"), 1.4e11),
    "VNT.V": (L("VNT.V", 1e9, 1.2e8, rccy="USD", ccy="CAD", ex="TSXV"), 1.2e9),
    "HOM-U.TO": (L("HOM-U.TO", 2e9, 1.5e8, rccy="USD", ccy="USD", ex="TSX"), 1.5e9 * 1.3),
}

# ---- a leg with no single currency (STO + OSL, or no exchange filter at all): sized in USD ----
MIX_FLOOR = 1e9
FX_MIX = [("USDSEK", BAR, 10.0), ("USDNOK", BAR, 8.0)]
MIX = {
    "A.ST": (L("A.ST", 1e11, 9e8, rccy="SEK", ccy="SEK", ex="STO", px=100.0), 9e9),
    "B.OL": (L("B.OL", 5e10, rccy="NOK", ccy="NOK", ex="OSL"), 5e10 / 8.0),
    "C.ST": (L("C.ST", 4e9, 1e9, rccy="USD", ccy="SEK", ex="STO", px=50.0), 4e9),
}

# ---- a leg with no share count and no foreign currency: the empty fetches leave typed tables ----
SAU = {"SA1.SR": (L("SA1.SR", 5e10, rccy="SAR", ccy="SAR", ex="SAU"), 5e10),
       "SA2.SR": (L("SA2.SR", 3e10, rccy="SAR", ccy="SAR", ex="SAU"), 3e10)}

# ---- the cuts (SET, THB, floor 1e10): 4 blue chips, then 2 dogs ----
CUT_FLOOR = 1e10
CUT = [
    L("BIG.BK", 9e11, 8e10, rccy="THB", ccy="THB", ex="SET", yld=0.04),                         # 8e11
    L("CORR.BK", 5e18, 5e7, rccy="THB", ccy="THB", ex="SET", yld=0.10),                         # corrupt cap; B 5e8: out
    L("MIDA.BK", 5e9, 4e8, rccy="THB", ccy="THB", ex="SET", yld=0.01, isin="TH0000000001"),     # under the floor
    L("MIDB.BK", 5e11, 6e10, rccy="THB", ccy="THB", ex="SET", yld=0.05, isin="TH0000000001"),   # 5e11
    L("TIEA.BK", 3e11, 4e10, rccy="THB", ccy="THB", ex="SET", yld=0.06),                        # 3e11 on A
    L("TIEB.BK", 4e11, 3e10, rccy="THB", ccy="THB", ex="SET", yld=0.07),                        # 3e11 on B
    L("TIEC.BK", 3e11, rccy="THB", ccy="THB", ex="SET", yld=0.08),                              # 3e11: fifth by symbol
    L("SMALL.BK", 2e11, rccy="THB", ccy="THB", ex="SET", yld=0.09),
    # the cut ranks on the size: first on its cap or on its price x shares, each of these is last on the size
    L("INFL.BK", 9e15, 2e9, rccy="THB", ccy="THB", ex="SET", yld=0.11),                         # 2e10 on B
    L("BIGB.BK", 1.5e10, 9e12, rccy="THB", ccy="THB", ex="SET", yld=0.12),                      # 1.5e10 on A
]
CUT_BLUECHIPS = [("TIEB.BK", 3e11), ("TIEA.BK", 3e11), ("MIDB.BK", 5e11), ("BIG.BK", 8e11)]   # by yield
CUT_BOOK = [("TIEB.BK", 3e11), ("TIEA.BK", 3e11)]

# ---- the Dow branch: no size rule, no size fetch ----
US = [L("DOWA", 5e11, 1.0, rccy="USD", ccy="USD", ex="NYSE", yld=0.05),
      L("DOWB", 9e18, 1.0, rccy="JPY", ccy="USD", ex="NYSE", yld=0.04),        # ranked on yield: the cap is not read
      L("DOWC", None, 1.0, ccy="USD", ex="NYSE", yld=0.03),                    # no cap filing: the default cap
      L("DOWD", 2e11, 1.0, rccy="USD", ccy="USD", ex="NYSE", yld=0.06, bars=[])]   # no entry bar: still screened
US_BOOK = [("DOWD", 2e11), ("DOWA", 5e11), ("DOWB", 9e18), ("DOWC", 1e11)]
SIZE_TABLES = {"shares_cache", "fx_cache", "ccy_map", "leg_ccy"}


def close_to(got, want):
    return got is not None and abs(got - want) <= 1e-9 * abs(want)


def run_main(mod, src, argv):
    """main() end to end on the fake client; returns (results JSON, SQL sent)."""
    client = FakeClient(src)
    saved_argv, saved_cr = sys.argv, mod.CetaResearch
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "out.json")
        sys.argv = ["backtest.py", *argv, "--exit-method", "drop", "--output", out]
        mod.CetaResearch = lambda **_kw: client
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                mod.main()
        finally:
            sys.argv, mod.CetaResearch = saved_argv, saved_cr
        with open(out) as fh:
            return json.load(fh), client.sqls


def failures(mod, src):
    f = []

    def chk(cond, what):
        if not cond:
            f.append(what)

    def section(name, fn):
        try:
            fn()
        except Exception as e:  # a mutant can break the SQL outright; that is a failure too
            f.append(f"{name}: raised {type(e).__name__}: {str(e).splitlines()[0][:160]}")

    def leg(name, table, fx, exchanges, floor, ccy):
        con, sqls = fetch(mod, source([ln for ln, _ in table.values()], fx), exchanges)
        got = sizes(mod, con, floor)
        for s, (_, want) in table.items():
            ok = s not in got if want is None else close_to(got.get(s), want)
            chk(ok, f"{name} size {s}: {got.get(s)} != {want}")
        chk(con.execute("SELECT ccy FROM leg_ccy").fetchall() == [(ccy,)], f"{name} leg currency {ccy}")
        return con, sqls

    def hong_kong():
        con, sqls = leg("hk", HK, FX_HK, ["HKSE"], HK_FLOOR, "HKD")
        n, keys = con.execute("SELECT COUNT(*), COUNT(DISTINCT (ccy, fx_epoch)) FROM fx_cache").fetchone()
        chk(n == keys and n > 0, f"hk fx_cache: one row per currency and day ({n} rows, {keys} keys)")
        fx_sql = [s for s in sqls if "forex_prices_daily" in s]
        chk(len(fx_sql) == 1 and "'USDHKD'" in fx_sql[0] and "'USDCNY'" in fx_sql[0] and "USDUSD" not in fx_sql[0]
            and "HKDUSD" not in fx_sql[0], "hk fetch: one FX query, USDxxx pairs, the leg's own included")

    def no_leg_rate():
        con, _ = fetch(mod, source([ln for ln, _ in HK.values()], [x for x in FX_HK if x[0] != "USDHKD"]), ["HKSE"])
        got = sizes(mod, con, HK_FLOOR)
        for s, want in NO_LEG_RATE.items():
            chk(s not in got if want is None else close_to(got.get(s), want), f"no HKD rate, size {s}: {got.get(s)} != {want}")

    def offset0():
        con, _ = fetch(mod, source([ln for ln, _ in HK.values()], FX_HK), ["HKSE"])
        got = sizes(mod, con, HK_FLOOR, offset_days=0)
        for s, want in OFFSET0.items():
            chk(s not in got if want is None else close_to(got.get(s), want), f"offset 0 size {s}: {got.get(s)} != {want}")

    def sub_units():
        chk(set(QUOTE_CURRENCY.values()) <= set(mod.SUBUNIT_CCY), "every QUOTE_CURRENCY sub-unit is scaled")
        chk(all(mod.SUBUNIT_CCY.get(QUOTE_CURRENCY[e]) == LOCAL_CURRENCY[e] for e in QUOTE_CURRENCY),
            "sub-units map to the exchange's reporting currency")
        chk("GBP" not in mod.SUBUNIT_CCY and "ZAR" not in mod.SUBUNIT_CCY, "pounds and rand are not sub-units")
        leg("jnb", JNB, FX_JNB, ["JNB"], JNB_FLOOR, "ZAR")
        _, sqls = leg("lse", LSE, FX_LSE, ["LSE"], LSE_FLOOR, "GBP")
        chk(all("currency IN ('GBp')" in s for s in sqls if "forex_prices_daily" not in s),
            "lse fetch: every query but the FX one carries the listing rule")

    def legs():
        leg("tsx", TSX, FX_TSX, ["TSX", "TSXV"], TSX_FLOOR, "CAD")
        leg("mixed", MIX, FX_MIX, ["STO", "OSL"], MIX_FLOOR, "USD")
        leg("global", MIX, FX_MIX, None, MIX_FLOOR, "USD")

    def empty_fetches():
        con, sqls = leg("sau", SAU, (), ["SAU"], 1e9, "SAR")
        chk(not any("forex_prices_daily" in s for s in sqls), "sau fetch: no foreign currency, no FX query")
        chk([c[0] for c in con.execute("DESCRIBE shares_cache").fetchall()]
            == ["symbol", "weightedAverageShsOut", "filing_epoch"]
            and [c[1] for c in con.execute("DESCRIBE fx_cache").fetchall()] == ["VARCHAR", "BIGINT", "DOUBLE"],
            "sau fetch: empty share and FX results leave typed tables")

    def the_cuts():
        con, _ = fetch(mod, source(CUT), ["SET"])
        with cuts(mod, 4, 999):
            got = mod.screen_dogs(con, D, use_dow=False, mktcap_min=CUT_FLOOR, offset_days=1)
        chk(got == CUT_BLUECHIPS, f"cut blue chips {got} != {CUT_BLUECHIPS}")
        with cuts(mod, 4, 2):
            got = mod.screen_dogs(con, D, use_dow=False, mktcap_min=CUT_FLOOR, offset_days=1)
        chk(got == CUT_BOOK, f"cut book {got} != {CUT_BOOK}")

    def counters():
        con, _ = fetch(mod, source([ln for ln, _ in HK.values()], FX_HK), ["HKSE"], (D, NEXT, LAST))
        got = mod.size_excluded(con, [D, NEXT, LAST], HK_FLOOR, offset_days=1)
        at_d = {"no_entry_price": len(HK_NO_PRICE), "no_size": len(HK_NO_SIZE), "over_ceiling": len(HK_OVER)}
        want = {"total": sum(at_d.values()) + len(HK_CAPPED),
                "no_entry_price": len(HK_NO_PRICE) + len(HK_CAPPED), "no_size": len(HK_NO_SIZE),
                "over_ceiling": len(HK_OVER),
                "by_period": {D.isoformat(): at_d,
                              NEXT.isoformat(): {"no_entry_price": len(HK_CAPPED), "no_size": 0, "over_ceiling": 0}}}
        chk(got == want, f"counters {got} != {want}")
        got0 = mod.size_excluded(con, [D, NEXT], HK_FLOOR, offset_days=0)["by_period"][D.isoformat()]
        chk(got0 == {**at_d, "no_entry_price": len(HK_NO_PRICE) - 1},   # DAY0, DAY0B in; DAY11 out
            f"counters follow offset_days: {got0}")

    def results():
        out, _ = run_main(mod, source([ln for ln, _ in HK.values()], FX_HK, bench=["^HSI"]), ["--exchange", "HKSE"])
        blk = out.get("size_excluded") or {}
        by = blk.get("by_period") or {}
        chk(list(blk) == ["total", "no_entry_price", "no_size", "over_ceiling", "by_period"] and len(by) == 25,
            f"results block: keys {list(blk)}, {len(by)} periods")
        chk(by.get(D.isoformat()) == {"no_entry_price": len(HK_NO_PRICE), "no_size": len(HK_NO_SIZE),
                                      "over_ceiling": len(HK_OVER)},
            f"results block at {D}: {by.get(D.isoformat())}")
        chk(blk.get("total") == sum(v["no_entry_price"] + v["no_size"] + v["over_ceiling"] for v in by.values())
            and blk.get("no_entry_price") == sum(v["no_entry_price"] for v in by.values())
            and blk.get("no_size") == len(HK_NO_SIZE) and blk.get("over_ceiling") == len(HK_OVER),
            f"results block totals {blk.get('total')}")
        chk(not any("ltp" in k for k in out) and out.get("isin_dedup") is True, "results: a drop run has no LTP key")

    def us():
        src_db = source(US, dow=[ln["sym"] for ln in US], bench=["SPY"])
        con, sqls = fetch(mod, src_db, ["NYSE", "NASDAQ", "AMEX"])
        got = mod.screen_dogs(con, D, use_dow=True, offset_days=1)
        chk(got == US_BOOK, f"us book {got} != {US_BOOK}")
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        chk(not tables & SIZE_TABLES and not any("income_statement" in s or "forex_prices_daily" in s for s in sqls),
            f"us fetch: no size inputs ({sorted(tables & SIZE_TABLES)})")
        out, _ = run_main(mod, src_db, ["--preset", "us"])
        chk("size_excluded" not in out and out.get("strategy") == "Dogs of the Dow", "us results: no size block")

    for name, fn in (("hk", hong_kong), ("no HKD rate", no_leg_rate), ("offset 0", offset0), ("sub-units", sub_units),
                     ("legs", legs), ("sau", empty_fetches), ("cut", the_cuts), ("counters", counters),
                     ("results", results), ("us", us)):
        section(name, fn)
    return f


RUNNER = os.path.join(ROOT, "dogs-of-dow", "run_all_exchanges.py")
RUNNER_KEY = ('**({} if use_dow else {"size_excluded": size_excluded(con, rebalance_dates, mktcap_threshold,'
              ' offset_days=1)}),')


def runner_failures(text):
    """run_all_exchanges.py can't run offline: its results must carry the same block, off the Dow leg only."""
    ok = RUNNER_KEY in text and "is_us_exchange, size_excluded," in text and "offset_days=1,\n" in text
    return [] if ok else ["runner: results record size_excluded at run_backtest's offset_days"]


# ---- every check can fail: each broken rule trips its own check ----
SIZE = "LEAST(COALESCE(a, b), COALESCE(b, a)) AS size"
A_FX = "CASE WHEN marketCap > 0 THEN marketCap END * {to_leg('rccy', 'r')} AS a"
B_PX = "CASE WHEN close > 0 THEN close END * shares"
LEG_RATE = "CASE WHEN lccy = 'USD' THEN 1.0 WHEN trade_epoch - l_ep <= {stale} THEN l_usd END"
SRC_RATE = "CASE WHEN {ccy} = 'USD' THEN 1.0 WHEN trade_epoch - {fx}_ep <= {stale} THEN {fx}_usd END"
SHS_WHERE = "WHERE filing_epoch <= {cutoff_epoch} AND filing_epoch >= {earliest_epoch} AND weightedAverageShsOut > 0"
WINDOW = "lo, hi = utc_epoch(shifted), utc_epoch(shifted + timedelta(days=10))"
PRICED = "COALESCE(adjClose > 0 AND adjClose >= 1.0, FALSE) AS priced"
BAR_ORDER = "ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST,\n                        close DESC NULLS LAST) AS rn"
FLOOR = "            FROM sized\n            WHERE size >= ? AND size <= ?\n"
AFTER_DEDUP = "                WHERE priced\n"
CUT_ORDER = "                ORDER BY marketCap DESC, symbol\n"
CAND = "            SELECT symbol, size AS marketCap, priced\n"
COUNT = ("SELECT COUNT(*) FILTER (WHERE NOT priced), COUNT(*) FILTER (WHERE priced AND size IS NULL),\n"
         "                   COUNT(*) FILTER (WHERE priced AND size > ?)\n"
         "            FROM sized\n            WHERE marketCap >= ?")
CEIL_MULT = "SIZE_CEILING_MULT = 15000 "
CEILING = "ceiling = mktcap_min * SIZE_CEILING_MULT "
SUB_LEG = '(100 if con.execute("SELECT ccy FROM leg_ccy").fetchone()[0] in SUBUNIT_CCY.values() else 1) '
P_SCREEN = "mktcap_min, ceiling, BLUECHIP_COUNT,"
P_COUNT = "[cutoff_epoch, earliest_epoch, ceiling, mktcap_min]"
LEG_CCY = '[get_local_currency(exchanges) or "USD"]'
FX_FROM = "utc_epoch(d - timedelta(days=FX_MAX_STALE_DAYS))"
FX_TO = "AND dateEpoch <= {utc_epoch(d + timedelta(days=11))})"
SHS_FETCH = "WHERE period = 'FY' AND weightedAverageShsOut > 0 AND {sym_filter_sql}"
RETYPE = ('            con.execute("DROP TABLE shares_cache")\n            con.execute("CREATE TABLE shares_cache('
          'symbol VARCHAR, weightedAverageShsOut DOUBLE, filing_epoch BIGINT)")\n')


def cut_on(figure):
    """Edits ranking the top-30 cut on `figure`; the floor and the returned cap stay on the size."""
    return [(SIZE, f"{SIZE}, {figure} AS big"), (CAND, CAND.replace("priced", "priced, big")),
            (CUT_ORDER, CUT_ORDER.replace("marketCap", "big"))]


def ceiling_on(figure):
    """Edits testing the ceiling on `figure`; the floor, the cut and the returned cap stay on the size."""
    return [(SIZE, f"{SIZE}, {figure} AS big"), (FLOOR, FLOOR.replace("size <= ?", "big <= ?"))]


MUTANTS = {
    # the size: the smaller figure, either one alone when the other is missing
    "the larger of the two": ([(SIZE, "GREATEST(COALESCE(a, b), COALESCE(b, a)) AS size")], "hk size BSM.HK"),
    "the cap alone (the old rule, restated)": ([(SIZE, "a AS size")], "hk size CORRLO.HK"),   # CORRUPT.HK: the ceiling stops it
    "price x shares alone": ([(SIZE, "b AS size")], "hk size DOM.HK"),
    "both figures required": ([(SIZE, "CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN LEAST(a, b) END AS size")],
                              "hk size NOSHS.HK"),
    "no fallback to price x shares": ([(SIZE, "CASE WHEN a IS NOT NULL THEN LEAST(a, COALESCE(b, a)) END AS size")],
                                      "hk size NOCAP.HK"),
    "no fallback to the cap": ([(SIZE, "CASE WHEN b IS NOT NULL THEN LEAST(COALESCE(a, b), b) END AS size")],
                               "hk size NOSHS.HK"),
    "a zero cap is a figure": ([("CASE WHEN marketCap > 0 THEN marketCap END", "marketCap")], "hk size ZCAP.HK"),
    "price x shares on adjClose": ([(B_PX, "CASE WHEN adjClose > 0 THEN adjClose END * shares")], "hk size ADJ.HK"),
    # currency: the cap from its reporting currency, price x shares from the quote currency
    "the cap not restated (the old rule)": ([(A_FX, "CASE WHEN marketCap > 0 THEN marketCap END AS a")],
                                            "hk size CNY.HK"),
    "FX inverted": ([(LEG_RATE + '"', "@L@"), (SRC_RATE, LEG_RATE), ("@L@", SRC_RATE + '"')], "hk size CNY.HK"),
    "missing FX passes unconverted": ([("{to_leg('rccy', 'r')} AS a", "COALESCE({to_leg('rccy', 'r')}, 1.0) AS a")],
                                      "hk size EUR8N.HK"),
    "no reporting currency read as the leg's": ([("m.reportedCurrency AS rccy", "COALESCE(m.reportedCurrency, l.ccy) AS rccy")],
                                                "hk size NULLR.HK"),
    "rate may be 8 days old": ([("FX_MAX_STALE_DAYS = 7 ", "FX_MAX_STALE_DAYS = 8 ")], "hk size EUR8.HK"),
    "rate may be 6 days old at most": ([("FX_MAX_STALE_DAYS = 7 ", "FX_MAX_STALE_DAYS = 6 ")], "hk size GBP7.HK"),
    "no age test on the leg's rate": ([("WHEN trade_epoch - l_ep <= {stale} THEN l_usd END", "ELSE l_usd END")],
                                      "hk size DAY10.HK"),
    "no age test on the other rate": ([("WHEN trade_epoch - {fx}_ep <= {stale} THEN {fx}_usd END", "ELSE {fx}_usd END")],
                                      "hk size EUR8N.HK"),
    "the leg's rate from after the bar": ([("f.ccy = b.lccy AND b.trade_epoch >= f.fx_epoch",
                                            "f.ccy = b.lccy AND b.trade_epoch + 86400 >= f.fx_epoch")], "hk size CNY.HK"),
    "the cap's rate from after the bar": ([("f.ccy = b.rccy AND b.trade_epoch >= f.fx_epoch",
                                            "f.ccy = b.rccy AND b.trade_epoch + 86400 >= f.fx_epoch")], "hk size CNY.HK"),
    "the quote's rate from after the bar": ([("f.ccy = b.tmaj AND b.trade_epoch >= f.fx_epoch",
                                              "f.ccy = b.tmaj AND b.trade_epoch + 86400 >= f.fx_epoch")], "hk size CNYQ.HK"),
    "price x shares not restated from the quote currency": ([("* {to_leg('tmaj', 't')} AS b", "AS b")], "hk size CNYQ.HK"),
    "no quote currency read as the leg's": ([("c.currency AS tccy, {major_ccy_sql('c.currency')} AS tmaj",
                                              "c.currency AS tccy, COALESCE({major_ccy_sql('c.currency')}, l.ccy) AS tmaj")],
                                            "hk size NOCCY.HK"),
    "no x0.01 on sub-unit quotes": ([("THEN 0.01 ELSE 1.0 END", "THEN 1.0 ELSE 1.0 END")], "jnb size CENT.JO"),
    "sub-unit quotes x100": ([("THEN 0.01 ELSE 1.0 END", "THEN 100.0 ELSE 1.0 END")], "jnb size CENT.JO"),
    "rand scaled as cents": ([('SUBUNIT_CCY = {"GBp": "GBP",', 'SUBUNIT_CCY = {"ZAR": "ZAR", "GBp": "GBP",')],
                             "pounds and rand are not sub-units"),
    "cents not scaled": ([('"ZAc": "ZAR", ', "")], "jnb size CENT.JO"),
    "pence not scaled": ([('{"GBp": "GBP", ', "{")], "jnb size PNCE.JO"),
    "agorot not scaled": ([(', "ILA": "ILS"}', "}")], "jnb size AGOR.JO"),
    "a sub-unit quote read as a foreign currency": ([("{major_ccy_sql('c.currency')} AS tmaj", "c.currency AS tmaj")],
                                                    "jnb size CENT.JO"),
    "the leg sized in its quote unit (pence)": ([(LEG_CCY, '[(get_local_currency(exchanges) or "USD").replace("GBP", "GBp")]')],
                                                "lse leg currency GBP"),
    "every leg sized in USD": ([(LEG_CCY, '["USD"]')], "hk size DOM.HK"),
    "a mixed leg sized in no currency": ([(LEG_CCY, "[get_local_currency(exchanges)]")], "mixed size A.ST"),
    # the entry bar
    "no entry-price test": ([(AFTER_DEDUP, "")], "hk size UNPX.HK"),
    "entry floor strict": ([(PRICED, PRICED.replace(">= 1.0", "> 1.0"))], "hk size ONE.HK"),
    "entry floor a cent lower": ([(PRICED, PRICED.replace(">= 1.0", ">= 0.99"))], "hk size PENNY.HK"),
    "entry test on close": ([(PRICED, PRICED.replace("adjClose", "close"))], "hk size ADJLOW.HK"),
    "close passes where adjClose fails": ([(PRICED, PRICED.replace("adjClose >= 1.0", "(adjClose >= 1.0 OR close >= 1.0)"))],
                                          "hk size ADJLOW.HK"),
    "no adjClose falls back to close": ([(PRICED, PRICED.replace("FALSE", "close >= 1.0, FALSE"))], "hk size ADJNUL.HK"),
    "best bar in the window, not the first": ([(BAR_ORDER, BAR_ORDER.replace("trade_epoch ASC, adjClose DESC NULLS LAST",
                                                                             "adjClose DESC NULLS LAST, trade_epoch ASC"))],
                                              "hk size FIRST.HK"),
    # (the price fetch stops at d+11, so a longer window shows with offset_days=0)
    "window a day longer": ([(WINDOW, WINDOW.replace("days=10", "days=11"))], "offset 0 size DAY11.HK"),
    "window a day shorter": ([(WINDOW, WINDOW.replace("days=10", "days=9"))], "hk size DAY11.HK"),
    "window not shifted by offset_days": ([("    shifted = target_date + timedelta(days=offset_days)\n    lo, hi",
                                            "    shifted = target_date\n    lo, hi")], "hk size DAY0.HK"),
    "window shifted a day too far": ([("    shifted = target_date + timedelta(days=offset_days)\n    lo, hi",
                                       "    shifted = target_date + timedelta(days=offset_days + 1)\n    lo, hi")],
                                     "hk size DOM.HK"),
    # the floor, on the final size and before the dedup
    "floor on the reported cap": ([(FLOOR, FLOOR.replace("size >= ?", "size IS NOT NULL AND marketCap >= ?"))],
                                  "hk size FLOORB.HK"),
    "floor strict": ([(FLOOR, FLOOR.replace("size >= ?", "size > ?"))], "hk size ATFLOOR.HK"),
    "no floor": ([(FLOOR, FLOOR.replace("size >= ?", "size IS NOT NULL AND ? IS NOT NULL"))], "hk size FLOORB.HK"),
    "floor after the dedup": ([(FLOOR, FLOOR.replace("size >= ?", "? IS NOT NULL")),
                               (AFTER_DEDUP, AFTER_DEDUP.replace("priced", "priced AND marketCap >= {mktcap_min}"))],
                              "hk size TWB.HK"),
    # the ceiling: 15000 x the floor, inclusive, on the final size and before the dedup
    "no ceiling": ([(FLOOR, FLOOR.replace("size <= ?", "? IS NOT NULL"))], "hk size OVCEIL.HK"),
    "ceiling on the cap alone": (ceiling_on("COALESCE(a, b)"), "hk size AOVER.HK"),
    "ceiling on price x shares alone": (ceiling_on("COALESCE(b, a)"), "hk size BOVER.HK"),
    "ceiling on the larger figure": (ceiling_on("GREATEST(COALESCE(a, b), COALESCE(b, a))"), "hk size BOVER.HK"),
    "ceiling ten times lower": ([(CEIL_MULT, CEIL_MULT.replace("15000", "1500"))], "hk size ATCEIL.HK"),
    "ceiling ten times higher": ([(CEIL_MULT, CEIL_MULT.replace("15000", "150000"))], "hk size OVCEIL.HK"),
    # (a sub-unit leg's floor is in pounds or rand, as its sizes are: the ceiling takes no x100 either way)
    "ceiling 100 times lower on a sub-unit leg": ([(CEILING, CEILING + "/ " + SUB_LEG)], "lse size BIGP.L"),
    "ceiling 100 times higher on a sub-unit leg": ([(CEILING, CEILING + "* " + SUB_LEG)], "lse size OVERP.L"),
    "ceiling exclusive": ([(FLOOR, FLOOR.replace("size <= ?", "size < ?"))], "hk size ATCEIL.HK"),
    "ceiling after the dedup": ([(FLOOR, FLOOR.replace("size <= ?", "? IS NOT NULL")),
                                 (AFTER_DEDUP, AFTER_DEDUP.replace("priced", "priced AND marketCap <= {ceiling}"))],
                                "hk size CTB.HK"),
    "floor and ceiling binds swapped": ([(P_SCREEN, "ceiling, mktcap_min, BLUECHIP_COUNT,")], "hk size DOM.HK"),
    # the share-count filing
    "share filing a day too old allowed": ([(SHS_WHERE, SHS_WHERE.replace(">= {earliest_epoch}", ">= {earliest_epoch} - 86400"))],
                                           "hk size SHS541.HK"),
    "share filing age a day short": ([(SHS_WHERE, SHS_WHERE.replace(">= {earliest_epoch}", ">= {earliest_epoch} + 86400"))],
                                     "hk size SHS540.HK"),
    "share filing lag a day short": ([(SHS_WHERE, SHS_WHERE.replace("<= {cutoff_epoch}", "<= {cutoff_epoch} + 86400"))],
                                     "hk size LAG44.HK"),
    "share filing lag a day long": ([(SHS_WHERE, SHS_WHERE.replace("<= {cutoff_epoch}", "<= {cutoff_epoch} - 86400"))],
                                    "hk size LAG45.HK"),
    "filings 541 days old allowed": ([("MAX_FILING_AGE_DAYS = 540", "MAX_FILING_AGE_DAYS = 541")], "hk size SHS541.HK"),
    "filings 539 days old at most": ([("MAX_FILING_AGE_DAYS = 540", "MAX_FILING_AGE_DAYS = 539")], "hk size SHS540.HK"),
    "lag of 44 days": ([("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
                         "cutoff_epoch = utc_epoch(target_date - timedelta(days=44))")], "hk size LAG44.HK"),
    "lag of 46 days": ([("cutoff_epoch = utc_epoch(target_date - timedelta(days=45))",
                         "cutoff_epoch = utc_epoch(target_date - timedelta(days=46))")], "hk size LAG45.HK"),
    "oldest share filing first": ([("ORDER BY filing_epoch DESC,\n                        weightedAverageShsOut DESC NULLS LAST",
                                    "ORDER BY filing_epoch ASC,\n                        weightedAverageShsOut DESC NULLS LAST")],
                                  "hk size NEWSHS.HK"),
    # duplicate rows and ties: one fixed order
    "share-count tie-break flipped": ([("weightedAverageShsOut DESC NULLS LAST) AS rn", "weightedAverageShsOut ASC) AS rn")],
                                      "hk size DUPS.HK"),
    "cap tie-break flipped": ([("ORDER BY filing_epoch DESC, marketCap DESC NULLS LAST,\n                        reported",
                                "ORDER BY filing_epoch DESC, marketCap ASC,\n                        reported")],
                              "hk size DUPM.HK"),
    "reporting-currency tie-break flipped": ([("reportedCurrency DESC NULLS LAST) AS rn", "reportedCurrency ASC) AS rn")],
                                             "hk size DUPR.HK"),
    "close tie-break flipped": ([(BAR_ORDER, BAR_ORDER.replace("close DESC NULLS LAST", "close ASC"))], "hk size DUPB.HK"),
    "smaller of two same-day FX closes": ([("MAX(close) AS per_usd", "MIN(close) AS per_usd")], "hk size CNY.HK"),
    "every same-day FX close kept": ([("MAX(close) AS per_usd", "close AS per_usd"), ("            GROUP BY 1, 2\n", "")],
                                     "one row per currency and day"),
    "smallest blue chips first": ([(CUT_ORDER, CUT_ORDER.replace("DESC", "ASC"))], "cut blue chips"),
    "ties at the cut by symbol descending": ([(CUT_ORDER, CUT_ORDER.replace("symbol", "symbol DESC"))], "cut blue chips"),
    "cut ranked on the cap (the old ranking, restated)": (cut_on("COALESCE(a, b)"), "cut blue chips"),
    "cut ranked on price x shares": (cut_on("COALESCE(b, a)"), "cut blue chips"),
    "cut ranked on the larger figure": (cut_on("GREATEST(COALESCE(a, b), COALESCE(b, a))"), "cut blue chips"),
    # the fetch
    "reportedCurrency not fetched": ([("SELECT symbol, marketCap, reportedCurrency, dateEpoch",
                                       "SELECT symbol, marketCap, CAST(NULL AS VARCHAR) AS reportedCurrency, dateEpoch")],
                                     "hk size DOM.HK"),
    "close not fetched": ([("adjClose, close, volume", "adjClose, CAST(NULL AS DOUBLE) AS close, volume")], "hk size BSM.HK"),
    "quote currency not fetched": ([("SELECT DISTINCT symbol, exchange, currency FROM profile",
                                     "SELECT DISTINCT symbol, exchange, CAST(NULL AS VARCHAR) AS currency FROM profile")],
                                   "hk size BSM.HK"),
    "quarterly share counts fetched": ([(SHS_FETCH, SHS_FETCH.replace("period = 'FY'", "period <> 'FY'"))], "hk size BSM.HK"),
    "share fetch without the leg's filter": ([(SHS_FETCH, SHS_FETCH.replace(" AND {sym_filter_sql}", ""))],
                                             "lse fetch: every query"),
    "empty share fetch keeps the placeholder": ([(RETYPE, "            pass\n")], "sau"),
    "xxxUSD pairs": ([("'USD{c}'", "'{c}USD'")], "hk fetch: one FX query"),
    "FX fetch starts a day late": ([(FX_FROM, FX_FROM.replace("FX_MAX_STALE_DAYS)", "FX_MAX_STALE_DAYS - 1)"))],
                                   "offset 0 size DAY0.HK"),
    "FX fetch ends a day early": ([(FX_TO, FX_TO.replace("days=11", "days=10"))], "hk size DAY11.HK"),
    "FX fetch without the leg's own rate": ([("SELECT UNNEST([l.ccy, s.c]) AS c", "SELECT UNNEST([s.c]) AS c")],
                                            "hk fetch: one FX query"),
    # the counters and the results
    "counter without the floor": ([(COUNT, COUNT.replace("marketCap >= ?", "? IS NOT NULL"))], "counters {"),
    "counter on the size, not the reported cap": ([(COUNT, COUNT.replace("marketCap >= ?", "size >= ?"))], "counters {"),
    "counters swapped": ([(COUNT, COUNT.replace("WHERE NOT priced", "WHERE priced AND size IS NULL", 1)
                           .replace("WHERE priced AND size IS NULL),\n", "WHERE NOT priced),\n"))], "counters {"),
    "unsized lines counted whether priced or not": ([(COUNT, COUNT.replace("WHERE priced AND size IS NULL", "WHERE size IS NULL"))],
                                                    "counters {"),
    "over-ceiling lines counted whether priced or not": ([(COUNT, COUNT.replace("WHERE priced AND size > ?", "WHERE size > ?"))],
                                                         "counters {"),
    "lines at the ceiling counted": ([(COUNT, COUNT.replace("size > ?", "size >= ?"))], "counters {"),
    "over-ceiling counter dropped": ([(COUNT, COUNT.replace("priced AND size > ?", "FALSE AND ? IS NOT NULL"))], "counters {"),
    "counter's floor and ceiling binds swapped": ([(P_COUNT, "[cutoff_epoch, earliest_epoch, mktcap_min, ceiling]")],
                                                  "counters {"),
    "counter ignores offset_days": ([("offset_days=offset_days, excluded=True", "excluded=True")], "counters follow offset_days"),
    "last rebalance date counted": ([("          for d in rebalance_dates[:-1]}\n    tot", "          for d in rebalance_dates}\n    tot")],
                                    "counters {"),
    "results drop the block": ([('            **({"size_excluded": size_excl} if size_excl else {}),\n', "")], "results block: keys"),
    "results count on the default floor": ([("size_excluded(con, rebalance_dates, mktcap_threshold, offset_days)",
                                             "size_excluded(con, rebalance_dates, 1_000_000_000, offset_days)")],
                                           "results block at"),
    # the Dow branch stays as it was
    "size inputs fetched on the Dow leg": ([("    if not use_dow:\n        # Share counts", "    if True:\n        # Share counts")],
                                           "us: raised"),
    "Dow leg sized": ([("    if use_dow:\n        # Dogs of the Dow: rank", "    if False:\n        # Dogs of the Dow: rank")],
                      "us: raised"),
    "Dow leg held to the ceiling": ([("WHERE r.rn = 1 AND r.dividendYield > 0",
                                      "WHERE r.rn = 1 AND r.dividendYield > 0 AND COALESCE(m.marketCap, 0) <= 1.5e13")],
                                    "us book"),   # 1.5e13 = the default floor x 15000; DOWB is at 9e18
    "Dow results carry the block": ([("size_excl = None if use_dow else size_excluded(", "size_excl = size_excluded(")],
                                    "us: raised"),
}


def main():
    with open(RUNNER) as fh:
        runner = fh.read()
    base = failures(load(SRC), SRC) + runner_failures(runner)
    if base:
        print("test_dogs_size: FAIL\n  " + "\n  ".join(base))
        sys.exit(1)
    if not runner_failures(runner.replace(RUNNER_KEY, "")):
        print("test_dogs_size: FAIL (a runner without the block is not caught)")
        sys.exit(1)
    for name, (edits, expect) in MUTANTS.items():
        src = SRC
        for old, new in edits:
            if src.count(old) != 1:
                print(f"test_dogs_size: FAIL (mutant anchor not unique: {name}: {old!r})")
                sys.exit(1)
            src = src.replace(old, new)
        try:
            fails = failures(load(src), src)
        except Exception as e:  # a mutant that cannot even be loaded
            fails = [f"load: raised {type(e).__name__}: {e}"]
        if not any(expect in x for x in fails):
            print(f"test_dogs_size: FAIL (mutant '{name}' not caught by '{expect}': {fails})")
            sys.exit(1)
    n = sum(len(t) for t in (HK, JNB, LSE, TSX, MIX, SAU)) + len(CUT) + len(US)
    print(f"test_dogs_size: PASS ({n} lines on 8 legs, main() run on 2; {len(MUTANTS) + 1} mutants caught)")


if __name__ == "__main__":
    main()
