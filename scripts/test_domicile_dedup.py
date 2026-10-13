#!/usr/bin/env python3
"""Offline checks for the B005 stage B universe policy (no network).

campaign_listing_condition(): XETRA/SIX legs keep home-country names; LSE legs keep pence (GBp)
lines, so 0XXX.L foreign lines, USD receipts and pound-quoted lines go while a pence-quoted
foreign-registered company stays; STO and mixed legs are untouched. load_isin_map(): fetched on
EVERY leg, with a home-currency flag. isin_dedup_sql(): one row per security (normalised ISIN,
else symbol): the line buyable at entry first (get_prices' FIRST bar in [d+off, d+off+10] against
the floor), then the line profile marks isActivelyTrading, then the line quoted in the leg's home
currency, then alphabetical. A symbol fanned out by a join keeps one row, symbols without ISINs
never merge, a collapsed twin frees its top-N slot. Then each campaign topic (TOPICS: wave 1 and
wave 2) runs its real fetch_data_via_api + screen (screen_stocks; dogs-of-dow's screen_dogs) against
a fake client on XETRA, SIX, STO, LSE and NYSE legs, run_backtest's offset_days is followed through
the screen into the dedup, and the two topics whose top-N cut is not one query around the dedup
show the cut running after it. Each check is re-run against mutants of the rule and must fail.
Checks survive python -O.
"""
import contextlib
import importlib.util
import io
import os
import random
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import data_utils
from data_utils import (EXCHANGE_COUNTRY, FILTERED_DOMICILE_EXCHANGES, campaign_listing_condition,
                        campaign_listing_label, domicile_sql_condition, entry_usable, get_local_currency,
                        get_prices, isin_dedup_sql, leg_quote_currency, load_isin_map, query_parquet)


def check(cond, what):
    if not cond:
        raise SystemExit(f"FAIL: {what}")


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


@contextlib.contextmanager
def patched(mod, **attrs):
    saved = {k: getattr(mod, k) for k in attrs}
    for k, v in attrs.items():
        setattr(mod, k, v)
    try:
        yield
    finally:
        for k, v in saved.items():
            setattr(mod, k, v)


D = date(2015, 7, 1)          # rebalance; offset_days=1 -> entry window [07-02, 07-12]
BAR = date(2015, 7, 2)

# Shaped like fmp.profile. ISINs/countries/currencies for real names are today's profile values
# unless marked invented.
PROFILE = [
    # symbol, exchange, country, isin, isFund, isEtf
    ("SAP.DE", "XETRA", "DE", "DE0007164600", False, False),
    ("DHL.DE", "XETRA", "DE", "DE0005552004", False, False),     # same security, two tickers
    ("DPW.DE", "XETRA", "DE", "DE0005552004", False, False),
    ("PG.DE", "XETRA", "US", "US7427181091", False, False),      # foreign secondary line
    ("EUNL.DE", "XETRA", "IE", "IE00B4L5Y983", False, True),
    ("NOCTRY.DE", "XETRA", None, None, False, False),
    ("ROG.SW", "SIX", "CH", "CH0012032048", False, False),       # Roche share classes: two ISINs
    ("RO.SW", "SIX", "CH", "CH0012032113", False, False),
    ("AVOL.SW", "SIX", "CH", "CH0023405456", False, False),      # Dufry, two tickers
    ("DUFN.SW", "SIX", "CH", "CH0023405456", False, False),
    ("BA.SW", "SIX", "US", "US0970231058", False, False),
    ("RIO.SW", "SIX", "GB", "AU000000RIO1", False, False),
    ("ATCO-A.ST", "STO", "SE", "SE0017486889", False, False),    # share classes: two ISINs
    ("ATCO-B.ST", "STO", "SE", "SE0017486897", False, False),
    ("ABB.ST", "STO", "CH", "CH0012221716", False, False),       # foreign-domiciled, real index member
    ("VOD.L", "LSE", "GB", "GB00BH4HKS39", False, False),
    ("0R2V.L", "LSE", "US", "US0378331005", False, False),       # 0-line, quoted in USD
    ("AADI", "NASDAQ", "US", "US00032Q1040", False, False),      # renamed: old line unpriced at entry
    ("WHWK", "NASDAQ", "US", "US00032Q1040", False, False),
    ("AAA1", "NYSE", "US", "US0000000AA1", False, False),        # twin priced below the floor
    ("ZZZ1", "NYSE", "US", "US0000000AA1", False, False),
    ("BBB1", "NYSE", "US", "US0000000BB1", False, False),        # twin, neither priced
    ("BBB2", "NYSE", "US", "US0000000BB1", False, False),
    ("CCC1", "NYSE", "US", " us0000000cc1 ", False, False),      # same ISIN, untrimmed lower case
    ("CCC2", "NYSE", "US", "US0000000CC1", False, False),
    ("NUL1", "NYSE", "US", None, False, False),                  # NULL ISINs: never one security
    ("NUL2", "NYSE", "US", None, False, False),
    ("BLK1", "NYSE", "US", "", False, False),                    # blank ISINs: never one security
    ("BLK2", "NYSE", "US", "  ", False, False),
    ("8DEAD", "NYSE", "US", "US0000000DD1", False, False),       # both buyable; the retired ticker sorts first
    ("ELIVE", "NYSE", "US", "US0000000DD1", False, False),
    ("OND1", "NYSE", "US", "US0000000OD1", False, False),        # priced only ON the rebalance day
    ("OND2", "NYSE", "US", "US0000000OD1", False, False),
    ("AFT1", "NYSE", "US", "US0000000AF1", False, False),        # priced only after the 10-day window
    ("AFT2", "NYSE", "US", "US0000000AF1", False, False),
    ("FST1", "NYSE", "US", "US0000000FS1", False, False),        # first in-window bar under the floor, later one over
    ("FST2", "NYSE", "US", "US0000000FS1", False, False),
    ("EDG1", "NYSE", "US", "US0000000ED1", False, False),        # priced only on the window's last day
    ("EDG2", "NYSE", "US", "US0000000ED1", False, False),
    ("EXPN.L", "LSE", "IE", "GB00B19NLV48", False, False),       # pence-quoted, Irish-registered: LSE keeps it
    ("NBS.L", "LSE", "GB", "GB00BBQ33664", False, False),        # quoted in pounds (GBP), not pence
    ("0Y4Q.L", "LSE", "GB", "IE00BDB6Q211", False, False),       # USD line that profile calls GB
    ("0QQ6.L", "LSE", "CH", "CH00000000Q6", False, False),       # CHF 0-line (ISIN invented)
    ("0A28.L", "LSE", "FR", "FR00000000A8", False, False),       # EUR 0-line (ISIN invented)
    ("SMSN.L", "LSE", "KR", "US7960508882", False, False),       # USD GDR on a plain ticker
    ("BC94.L", "LSE", "KR", "US7960508882", False, False),       # the same GDR, second ticker
    ("HOM-U.TO", "TSX", "US", "CA05585D1033", False, False),     # USD line sorts before the CAD line
    ("HOM-UN.TO", "TSX", "US", "CA05585D1033", False, False),
    ("89988.HK", "HKSE", "CN", "KYG017191142", False, False),    # CNY counter buyable, HKD line unpriced
    ("9988.HK", "HKSE", "CN", "KYG017191142", False, False),
    ("USCC-U.TO", "TSX", "CA", "CA37964A1030", False, True),     # USD line live, CAD line retired
    ("USCC.TO", "TSX", "CA", "CA37964A1030", False, True),
]
# Quote currency: the exchange's own (profile.currency of its home lines) unless listed in CCY.
EX_CCY = {"XETRA": "EUR", "SIX": "CHF", "STO": "SEK", "LSE": "GBp", "NYSE": "USD", "NASDAQ": "USD",
          "TSX": "CAD", "HKSE": "HKD"}
CCY = {"0R2V.L": "USD", "NBS.L": "GBP", "0Y4Q.L": "USD", "0QQ6.L": "CHF", "0A28.L": "EUR", "SMSN.L": "USD",
       "BC94.L": "USD", "HOM-U.TO": "USD", "89988.HK": "CNY", "USCC-U.TO": "USD"}
DEAD = {"8DEAD", "USCC.TO"}   # isActivelyTrading = false
# Bars per symbol; every other symbol has one 50.0 bar on BAR.
BARS = {"AADI": [(date(2015, 6, 30), 40.0)],                  # before the window: must not make AADI buyable
        "BBB1": [], "BBB2": [], "9988.HK": [],
        "AAA1": [(BAR, 0.5)],                                  # below MIN_ENTRY_PRICE=1.0
        "OND1": [(D, 50.0)],                                   # D itself: outside [D+1, D+11]
        "AFT1": [(date(2015, 7, 14), 50.0)],                   # D+13: past the window
        "FST1": [(BAR, 0.5), (date(2015, 7, 3), 50.0)],        # get_prices takes the FIRST bar
        "EDG1": [(date(2015, 7, 12), 50.0)]}                   # D+11: inside, the bound is inclusive
EMPTY_MAP = "CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)"


def prices(con):
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
    con.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)",
                    [(s, ep(d), px) for s, *_ in PROFILE for d, px in BARS.get(s, [(BAR, 50.0)])])


def cands(con):
    con.execute("CREATE TABLE cands(symbol VARCHAR, score DOUBLE)")
    con.executemany("INSERT INTO cands VALUES (?, ?)", [(s, 100.0 - i) for i, (s, *_) in enumerate(PROFILE)])


def fixture():
    con = duckdb.connect()
    con.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR,"
                " isFund BOOLEAN, isEtf BOOLEAN, currency VARCHAR)")
    con.executemany("INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(*r, CCY.get(r[0], EX_CCY[r[1]])) for r in PROFILE])
    # Shaped like load_isin_map's table (home_ccy: quoted in its exchange's own currency);
    # a duplicated profile row must not fan out.
    con.execute(EMPTY_MAP)
    con.executemany("INSERT INTO isin_map VALUES (?, ?, ?, ?)",
                    [(s, isin, s not in DEAD, s not in CCY) for s, _ex, _c, isin, *_ in PROFILE])
    con.execute("INSERT INTO isin_map VALUES ('SAP.DE', NULL, NULL, NULL)")
    prices(con)
    cands(con)
    return con


def leg(con, exchanges, cond_fn):
    cond = cond_fn(exchanges)
    ex = ", ".join(f"'{e}'" for e in exchanges)
    rows = con.execute(f"SELECT symbol FROM profile WHERE exchange IN ({ex})"
                       + (f" AND {cond}" if cond else "")).fetchall()
    return {r[0] for r in rows}


def everything(con, exchanges):
    return leg(con, exchanges, lambda _e: "")


def listing_failures(con, cond_fn):
    f = []
    if leg(con, ["XETRA"], cond_fn) != {"SAP.DE", "DHL.DE", "DPW.DE"}:
        f.append("XETRA keeps only DE names")
    if leg(con, ["SIX"], cond_fn) != {"ROG.SW", "RO.SW", "AVOL.SW", "DUFN.SW"}:
        f.append("SIX keeps only CH names")
    if leg(con, ["XETRA", "SIX"], cond_fn) != leg(con, ["XETRA"], cond_fn) | leg(con, ["SIX"], cond_fn):
        f.append("XETRA+SIX leg keeps DE and CH names")
    if leg(con, ["LSE"], cond_fn) != {"VOD.L", "EXPN.L"}:
        f.append("LSE keeps only pence lines")
    if "EXPN.L" not in leg(con, ["LSE"], cond_fn):
        f.append("LSE keeps Experian (IE-registered, pence)")
    for exs in (["STO"], ["NYSE", "NASDAQ"], ["TSX"], ["HKSE"], ["XETRA", "STO"], ["SIX", "LSE"], ["LSE", "NYSE"]):
        if leg(con, exs, cond_fn) != everything(con, exs):
            f.append(f"{'+'.join(exs)} leg untouched")
    if "ABB.ST" not in leg(con, ["STO"], cond_fn):
        f.append("STO keeps ABB (CH-domiciled)")
    if cond_fn(None) != "" or cond_fn([]) != "":
        f.append("no-exchange (global) run untouched")
    return f


class Recorder:
    """A client that records SQL and returns nothing (query_parquet then loads 0 rows)."""

    def __init__(self):
        self.sqls = []

    def query(self, sql, **_kw):
        self.sqls.append(sql)
        return None


# leg -> the home quote currency its isin_map flags (None: mixed/unknown, no flag)
GATE_LEGS = {("STO",): "SEK", ("LSE",): "GBp", ("XETRA",): "EUR", ("SIX",): "CHF", ("TSX",): "CAD",
             ("NYSE", "NASDAQ"): "USD", ("XETRA", "SIX"): None, ("XETRA", "STO"): None, ("SIX", "LSE"): None,
             None: None, (): None}


def gate_failures(load_fn):
    f = []
    for exs, ccy in GATE_LEGS.items():
        con, rec = duckdb.connect(), Recorder()
        load_fn(rec, con, None if exs is None else list(exs), "symbol IN (SELECT 'LEGFILTER')")
        sql = rec.sqls[0] if len(rec.sqls) == 1 else ""
        if "symbol IN (SELECT 'LEGFILTER')" not in sql or "isActivelyTrading" not in sql:
            f.append(f"{exs}: isin_map fetched once with the leg filter and the live flag")
        if (f"currency = '{ccy}'" not in sql) if ccy else ("currency" in sql):
            f.append(f"{exs}: home-currency flag {ccy}")
        cols = [r[0] for r in con.execute("DESCRIBE isin_map").fetchall()]
        if cols != ["symbol", "isin", "isActivelyTrading", "home_ccy"] \
                or con.execute("SELECT COUNT(*) FROM isin_map").fetchone()[0]:
            f.append(f"{exs}: an empty result leaves a typed, empty isin_map")
    return f


def ranked(con, dedup_fn, n=1000, where=""):
    sql = dedup_fn(f"SELECT symbol, score FROM cands{where}", D, 1, 1.0)
    return [r[0] for r in con.execute(f"SELECT symbol FROM ({sql}) ORDER BY score DESC, symbol LIMIT ?",
                                      [n]).fetchall()]


TWIN_KEEP = {("DHL.DE", "DPW.DE"): "DHL.DE", ("AVOL.SW", "DUFN.SW"): "AVOL.SW",   # both priced: alphabetical
             ("AADI", "WHWK"): "WHWK",          # only WHWK priced in the window
             ("AAA1", "ZZZ1"): "ZZZ1",          # AAA1 priced under the floor
             ("BBB1", "BBB2"): "BBB1",          # neither priced: alphabetical
             ("CCC1", "CCC2"): "CCC1",          # ISIN compared trimmed and upper-cased
             ("8DEAD", "ELIVE"): "ELIVE",       # both buyable: the live line beats alphabetical
             ("OND1", "OND2"): "OND2",          # OND1's bar is on D, before the offset window
             ("AFT1", "AFT2"): "AFT2",          # AFT1's bar is past D+1+10
             ("FST1", "FST2"): "FST2",          # FST1's FIRST in-window bar (0.5) decides, not its best
             ("EDG1", "EDG2"): "EDG1",          # D+11 is inside the window: both buyable, alphabetical
             ("BC94.L", "SMSN.L"): "BC94.L",    # neither in the home currency: alphabetical
             ("HOM-U.TO", "HOM-UN.TO"): "HOM-UN.TO",   # both buyable and live: home currency beats alphabetical
             ("89988.HK", "9988.HK"): "89988.HK",      # home-currency line unpriced: buyable beats currency
             ("USCC-U.TO", "USCC.TO"): "USCC-U.TO"}    # home-currency line retired: live beats currency
FANNED = ("NUL1", "BLK2", "DHL.DE", "HOM-UN.TO")   # rows a join duplicates


def dedup_failures(con, dedup_fn):
    f = []
    got = ranked(con, dedup_fn)
    if len(got) != len(set(got)):
        f.append("one row per symbol (isin_map duplicates do not fan out)")
    for pair, want in TWIN_KEEP.items():
        if [s for s in got if s in pair] != [want]:
            f.append(f"twins {pair} collapse to {want}")
    for pair in (("NUL1", "NUL2"), ("BLK1", "BLK2")):
        if not set(pair) <= set(got):
            f.append(f"no-ISIN lines {pair} both stay")
    for pair in (("ATCO-A.ST", "ATCO-B.ST"), ("ROG.SW", "RO.SW")):
        if not set(pair) <= set(got):
            f.append(f"share classes {pair} both stay")
    dropped = {s for pair, want in TWIN_KEEP.items() for s in pair if s != want}
    if sorted(got) != sorted(s for s, *_ in PROFILE if s not in dropped):
        f.append("exactly the twins are removed")
    fanned = dedup_fn("SELECT symbol, score FROM cands UNION ALL SELECT symbol, score FROM cands WHERE symbol IN ("
                      + ", ".join(f"'{s}'" for s in FANNED) + ")", D, 1, 1.0)
    if sorted(r[0] for r in con.execute(f"SELECT symbol FROM ({fanned})").fetchall()) != sorted(got):
        f.append("a fanned-out symbol keeps one row")
    # Top-N: DHL.DE/DPW.DE rank 2nd and 3rd; the collapsed twin frees its slot for the 6th name.
    if ranked(con, dedup_fn, n=5) != ["SAP.DE", "DHL.DE", "PG.DE", "EUNL.DE", "NOCTRY.DE"]:
        f.append("collapsed twin frees its top-N slot")
    return f


def empty_map_failures(dedup_fn):
    """The leg's profile query came back empty: a typed, empty isin_map; every symbol keeps one row, none merge."""
    con = duckdb.connect()
    load_isin_map(Recorder(), con, ["STO"], "1=1")
    prices(con)
    cands(con)
    con.execute("INSERT INTO cands SELECT * FROM cands WHERE symbol IN ('DHL.DE', 'NUL1')")  # fanned-out rows
    want = sorted(set(con.execute("SELECT symbol, score FROM cands").fetchall()))
    got = sorted(con.execute(f"SELECT symbol, score FROM ({dedup_fn('SELECT symbol, score FROM cands', D, 1, 1.0)})")
                 .fetchall())
    return [] if got == want else ["empty isin_map: every symbol once, none merged"]


def property_failures(dedup_fn, seeds):
    """Random multi-bar twins: the winner is get_prices + entry_usable's buyable line, then live, then home, then name."""
    bad = []
    for seed in seeds:
        rnd = random.Random(seed)
        off, floor = seed % 2, (0.5, 1.0)[seed // 2 % 2]
        con = duckdb.connect()
        con.execute(EMPTY_MAP)
        con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
        live, home = {}, {}
        for g in range(4):
            for k in "AB":
                s = f"G{g}{k}"
                live[s], home[s] = rnd.random() < 0.5, rnd.choice([True, False, None])
                con.execute("INSERT INTO isin_map VALUES (?, ?, ?, ?)", [s, f"ISIN{g}", live[s], home[s]])
                for _ in range(rnd.randint(0, 4)):
                    day = D + timedelta(days=rnd.randint(-3, 14))
                    con.execute("INSERT INTO prices_cache VALUES (?, ?, ?)",
                                [s, ep(day), rnd.choice([None, 0.0, 0.4, 0.7, 2.0, 50.0])])
        got = {r[0] for r in con.execute(f"SELECT symbol FROM ({dedup_fn('SELECT symbol FROM isin_map', D, off, floor)})")
               .fetchall()}
        px = get_prices(con, list(live), D, offset_days=off)
        for g in range(4):
            pair = (f"G{g}A", f"G{g}B")
            want = min(pair, key=lambda s: (not entry_usable(px.get(s), floor), not live[s], not home[s], s))
            if got & set(pair) != {want}:
                bad.append(seed)
    return bad


def ranked_param(con, dedup_fn):
    """`?` params inside the candidate SQL still bind ahead of the outer LIMIT."""
    sql = dedup_fn("SELECT symbol, score FROM cands WHERE score <= ?", D, 1, 1.0)
    return [r[0] for r in con.execute(f"SELECT symbol FROM ({sql}) ORDER BY score DESC, symbol LIMIT ?",
                                      [99.0, 2]).fetchall()]


def mutant(old, new):
    def fn(*a, **k):
        sql = isin_dedup_sql(*a, **k)
        check(old in sql, f"mutant target {old!r} not found: the mutation would test nothing")
        return sql.replace(old, new)
    return fn


# ---- topic wiring: each topic's own fetch + screen against a fake client ----
TOPICS = ["cyclical-timing", "asset-growth", "fcf-conversion", "cash-conversion", "graham-net-net",       # wave 1
          "dogs-of-dow", "low-debt", "fcf-compounders", "fcf-yield", "price-to-sales", "revenue-accel",  # wave 2
          "dividend-growth", "deleveraging", "pe-mean-revert", "dividend-sustainability", "rising-yield",
          "graham-number"]
TOPIC_FLOOR = {"graham-net-net": 0.50}      # entry floor each topic's dedup must use; others 1.0
# Screen entry point: (name, call, legs that reach every dedup site). Topics not listed use SCREEN_STOCKS.
SCREEN_STOCKS = ("screen_stocks", lambda mod, con, _ex, **k: mod.screen_stocks(con, D, 1e6, **k), ("XETRA",))
SCREENS = {"dogs-of-dow": ("screen_dogs",   # the NYSE leg is its Dow branch, every other leg its blue-chip branch
                           lambda mod, con, ex, **k: mod.screen_dogs(con, D, use_dow=mod.is_us_exchange([ex]),
                                                                     mktcap_min=1e6, **k),
                           ("XETRA", "NYSE"))}
RESULT_FILES = {"dogs-of-dow": ("backtest.py", "run_all_exchanges.py")}   # files that write results; others backtest.py
RESULT_KEYS = ('"listing_filter": campaign_listing_label(exchanges)', '"isin_dedup": True')
FANNED_TOPICS = ("cyclical-timing", "pe-mean-revert")   # screens that join profile_cache: a fan-out reaches only them
# A sixth leg (no listing rule) for top-N cuts that are not `FROM (dedup) ORDER BY .. LIMIT`: 31 securities ranked
# S00 (largest, deepest value, lowest yield) to S30, and S00's twin ranked second. topic -> the book it must hold.
SLOT_LEG, SLOT_NAMES = "OSL", [f"S{i:02d}.OL" for i in range(31)]
SLOT_LINES = [(s, i) for i, s in enumerate(SLOT_NAMES)] + [("S00B.OL", 0.5)]
SLOT_BOOK = {"dogs-of-dow": SLOT_NAMES[29:19:-1],      # 30 largest securities, then the 10 highest yields
             "graham-number": SLOT_NAMES[:30]}         # 30 deepest discounts
LEGS = {"XETRA": ".DE", "SIX": ".SW", "STO": ".ST", "LSE": ".L", "NYSE": ""}
HOME = {"XETRA": "DE", "SIX": "CH", "STO": "SE", "LSE": "GB", "NYSE": "US"}
FOREIGN = {"XETRA": "US", "SIX": "US", "STO": "CH", "LSE": "IE", "NYSE": "IE"}  # STO/LSE/NYSE's must stay
QUOTE = {"XETRA": "EUR", "SIX": "CHF", "STO": "SEK", "LSE": "GBp", "NYSE": "USD"}
OTHER_CCY = {"XETRA": "USD", "SIX": "USD", "STO": "EUR", "LSE": "USD", "NYSE": "EUR"}
LISTING = {"XETRA": "country IN ('DE')", "SIX": "country IN ('CH')", "LSE": "currency IN ('GBp')"}
DATES = [D, date(2016, 7, 1)]


def topic_lines():
    rows = []
    for ex, sfx in LEGS.items():
        h, fo, q = HOME[ex], FOREIGN[ex], QUOTE[ex]
        rows += [(f"HOME{sfx}", ex, h, f"{h}00HOME{ex}", 50.0, True, q),
                 (f"AAA{sfx}", ex, h, f"{h}00TWIN{ex}", 0.75, True, q),     # twin priced in [0.50, 1.0)
                 (f"BBB{sfx}", ex, h, f"{h}00TWIN{ex}", 50.0, True, q),
                 (f"CCC{sfx}", ex, h, f"{h}00LIVE{ex}", 50.0, False, q),    # twin retired in profile, sorts first
                 (f"DDD{sfx}", ex, h, f"{h}00LIVE{ex}", 50.0, True, q),
                 (f"EEE{sfx}", ex, h, f"{h}00CCY{ex}", 50.0, True, OTHER_CCY[ex]),  # twin in another currency
                 (f"FFF{sfx}", ex, h, f"{h}00CCY{ex}", 50.0, True, q),
                 (f"NOI1{sfx}", ex, h, None, 50.0, True, q),                # no ISIN, two profile rows
                 (f"NOI2{sfx}", ex, h, None, 50.0, True, q),                # no ISIN: never merges with NOI1
                 (f"FRGN{sfx}", ex, fo, f"{fo}00FRGN{ex}", 50.0, True, q)]
    return rows + [("0R2V.L", "LSE", "US", "US0378331005", 50.0, True, "USD"),   # 0-lines in their home currency
                   ("0QQ6.L", "LSE", "CH", "CH00000000Q6", 50.0, True, "CHF"),
                   ("0A28.L", "LSE", "FR", "FR00000000A8", 50.0, True, "EUR"),
                   ("SMSN.L", "LSE", "KR", "US7960508882", 50.0, True, "USD"),   # USD GDR, plain ticker
                   ("NBS.L", "LSE", "GB", "GB00BBQ33664", 50.0, True, "GBP"),    # pounds, not pence
                   ("0Y4Q.L", "LSE", "GB", "IE00BDB6Q211", 50.0, True, "USD")]   # USD line profile calls GB


# Six consecutive FY filings, the newest 182 days before D: inside the 45-day lag and any filing-age cap.
FYS = [date(y, 12, 31) for y in range(2009, 2015)]


def then(prior, latest):
    return (prior,) * (len(FYS) - 1) + (latest,)       # `prior` on every earlier filing, `latest` on the newest


def rising(latest, step):
    return tuple(latest - step * k for k in reversed(range(len(FYS))))    # up by `step` a year, ending at `latest`


# FMP statement tables: column -> its value on every FY filing, or one value per filing (oldest first).
# Every line passes every topic's screen on its LATEST filing; year-on-year tests hold on every pair.
FY_TABLES = {
    "key_metrics": {
        "returnOnEquity": 0.2, "returnOnAssets": 0.1, "marketCap": 5e9, "grahamNetNet": 100.0, "currentRatio": 2.0,
        "cashConversionCycle": 40.0, "daysOfSalesOutstanding": 10.0, "daysOfInventoryOutstanding": 10.0,
        "daysOfPayablesOutstanding": 10.0, "returnOnInvestedCapital": 0.2, "freeCashFlowYield": 0.1,
        "grahamNumber": 100.0,                          # above every line's price (50.0, 0.75)
        "reportedCurrency": ""},                        # per line, see source_db
    "balance_sheet": {                                  # with income and cash flow: Piotroski 9 of 9
        "totalAssets": then(1000.0, 1050.0), "totalCurrentAssets": then(400.0, 500.0),
        "totalCurrentLiabilities": 200.0, "longTermDebt": then(100.0, 50.0),
        "totalStockholdersEquity": then(500.0, 600.0)},
    "income_statement": {
        "revenue": (790.0, 800.0, 820.0, 850.0, 900.0, 1000.0),         # growth 5.9% -> 11.1%: accelerating
        "netIncome": then(80.0, 100.0), "grossProfit": then(300.0, 400.0)},
    "cash_flow_statement": {
        "freeCashFlow": rising(150.0, 10.0), "operatingCashFlow": 200.0,
        "commonDividendsPaid": -50.0},                  # covered 3x by free cash flow
    "financial_ratios": {
        "operatingProfitMargin": 0.2, "grossProfitMargin": 0.4, "interestCoverageRatio": 10.0,
        "priceToSalesRatio": 0.5, "priceToEarningsRatio": 5.0,          # a sixth of the dear names' 30.0
        "debtToEquityRatio": then(0.4, 0.2),            # halved in the latest year, and under every cap
        "dividendYield": rising(0.04, 0.002), "dividendPerShare": rising(1.5, 0.1), "dividendPayoutRatio": 0.3},
}
N_DEAR = {"Industrials": 20, "Energy": 5}     # dear names per leg: each sector's median P/E stays 30.0 under every mutant


def fy_rows(src, sym, tables, **over):
    """One row per FYS filing in each table: FY_TABLES' values, `over` replacing columns by name."""
    for table in tables:
        for k, fy in enumerate(FYS):
            vals = [over.get(c, v[k] if isinstance(v, tuple) else v) for c, v in FY_TABLES[table].items()]
            src.execute(f"INSERT INTO {table} VALUES (?, 'FY', ?, ?{', ?' * len(vals)})", [sym, fy, ep(fy), *vals])


def source_db():
    """The FMP tables the topics' fetches read, with every line passing every topic's screen."""
    src = duckdb.connect()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR,"
                " sector VARCHAR, marketCap DOUBLE, isActivelyTrading BOOLEAN, currency VARCHAR, isAdr BOOLEAN)")
    for table, cols in FY_TABLES.items():
        src.execute(f"CREATE TABLE {table}(symbol VARCHAR, period VARCHAR, date DATE, dateEpoch BIGINT, " + ", ".join(
            f"{c} {'VARCHAR' if isinstance(v, str) else 'DOUBLE'}" for c, v in cols.items()) + ")")
    src.execute("CREATE TABLE dividend_calendar(symbol VARCHAR, date DATE, adjDividend DOUBLE)")
    src.execute("CREATE TABLE dowjones_constituent(symbol VARCHAR)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    lines = [(*r, {}) for r in topic_lines()] + [
        (sym, SLOT_LEG, "NO", f"NO00SLOT{int(k):02d}", 50.0, True, "NOK",
         {"marketCap": 1e12 * (40 - k), "grahamNumber": 1000.0 - 10 * k, "dividendYield": 0.01 * (k + 1)})
        for sym, k in SLOT_LINES]
    for sym, ex, country, isin, px, live, ccy, over in lines:
        src.execute("INSERT INTO profile VALUES (?, ?, ?, ?, 'Industrials', 5e9, ?, ?, FALSE)",
                    [sym, ex, country, isin, live, ccy])
        # reports in its quote currency (pence -> pounds): no FX fetch, graham-net-net's x100 only
        fy_rows(src, sym, FY_TABLES, reportedCurrency="GBP" if ccy == "GBp" else ccy, **over)
        for y in range(FYS[0].year - 1, FYS[-1].year + 1):     # one payment a year, raised every year
            src.execute("INSERT INTO dividend_calendar VALUES (?, ?, ?)", [sym, date(y, 5, 15), 1.0 + 0.1 * (y - 2000)])
        if ex == "NYSE":                                        # dogs-of-dow's US universe
            src.execute("INSERT INTO dowjones_constituent VALUES (?)", [sym])
        for day in (BAR, date(2016, 7, 5)):
            src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, ?, 1e6)", [sym, day, ep(day), px])
    # A second profile row in another (cyclical) sector: cyclical-timing's and pe-mean-revert's profile_cache
    # joins fan NOI1 out.
    src.execute("INSERT INTO profile SELECT symbol, exchange, country, isin, 'Energy', marketCap,"
                " isActivelyTrading, currency, isAdr FROM profile WHERE symbol LIKE 'NOI1%'")
    # pe-mean-revert: dear home lines set each sector's median P/E. Only the three columns its median reads
    # are filled, so they pass no screen and rank below every line in dogs-of-dow's size cut.
    dear = {**{c: None for t in ("key_metrics", "financial_ratios") for c in FY_TABLES[t]},
            "returnOnEquity": 0.2, "marketCap": 1e9, "priceToEarningsRatio": 30.0}
    for ex, sfx in LEGS.items():
        for sector, n in N_DEAR.items():
            for i in range(n):
                sym = f"DEAR{sector[0]}{i:02d}{sfx}"
                src.execute("INSERT INTO profile VALUES (?, ?, ?, NULL, ?, 1e9, TRUE, ?, FALSE)",
                            [sym, ex, HOME[ex], sector, QUOTE[ex]])
                fy_rows(src, sym, ("key_metrics", "financial_ratios"), **dear)
    return src


class FakeClient:
    """Stands in for CetaResearch: runs the topic's own SQL on the fixture, parquet like the API."""

    def __init__(self, src):
        self.src, self.sqls = src, []

    def query(self, sql, format="json", **_kw):
        self.sqls.append(sql)
        if format != "parquet":
            cur = self.src.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.parquet")
            self.src.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
            with open(path, "rb") as fh:
                return fh.read()


def load_topic(topic):
    spec = importlib.util.spec_from_file_location("dd_" + topic.replace("-", "_"),
                                                  os.path.join(ROOT, topic, "backtest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch(mod, src, ex):
    client = FakeClient(src)
    with contextlib.redirect_stdout(io.StringIO()):
        con = mod.fetch_data_via_api(client, [ex], DATES)
    return con, client.sqls


def expected(ex, floor):
    sfx = LEGS[ex]
    keep = {f"HOME{sfx}", f"AAA{sfx}" if floor <= 0.75 else f"BBB{sfx}", f"DDD{sfx}", f"FFF{sfx}",
            f"NOI1{sfx}", f"NOI2{sfx}"}
    return keep if ex in FILTERED_DOMICILE_EXCHANGES else keep | {f"FRGN{sfx}"}


def leg_failures(mod, topic, src, legs=tuple(LEGS)):
    f = []
    for ex in legs:
        con, sqls = fetch(mod, src, ex)
        cond = LISTING.get(ex)
        data = [s for s in sqls if "forex_prices_daily" not in s]   # an FX fetch has no listing to filter
        if cond and not all(cond in s for s in data):
            f.append(f"{ex}: every query carries {cond}")
        if not cond and any("country IN" in s or "currency IN" in s for s in sqls):
            f.append(f"{ex}: no query filters on country or currency")
        n_isin_sql = sum(1 for s in sqls if re.search(r"\bisin\b", s))
        if n_isin_sql != 1 or not con.execute("SELECT COUNT(*) FROM isin_map").fetchone()[0]:
            f.append(f"{ex}: isin_map fetched")
        with contextlib.redirect_stdout(io.StringIO()):
            picked = SCREENS.get(topic, SCREEN_STOCKS)[1](mod, con, ex)
        syms = list(picked) if isinstance(picked, dict) else [r[0] for r in picked]
        if len(syms) != len(set(syms)):
            f.append(f"{ex}: a symbol held twice")
        want = expected(ex, TOPIC_FLOOR.get(topic, 1.0))
        if set(syms) != want:
            f.append(f"{ex}: screen keeps {sorted(want)}, got {sorted(set(syms))}")
    return f


class _Stop(Exception):
    pass


def forwarding_failures(mod, topic, src):
    """run_backtest hands its offset_days to the screen, and the screen to the dedup (its entry window)."""
    name, call, legs = SCREENS.get(topic, SCREEN_STOCKS)
    f = []
    for ex in legs:
        con, _ = fetch(mod, src, ex)
        calls, seen = [], []

        def rec(*_a, offset_days=1, **_k):
            calls.append(offset_days)
            raise _Stop

        def spy(c, d, off, fl, **k):
            seen.append(off)
            return isin_dedup_sql(c, d, off, fl, **k)

        saved = getattr(mod, name)
        setattr(mod, name, rec)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                mod.run_backtest(con, DATES, mktcap_min=1e6, offset_days=0)
        except _Stop:
            pass
        finally:
            setattr(mod, name, saved)
        if calls != [0]:
            f.append(f"{ex}: run_backtest forwards offset_days=0 to {name}, got {calls}")
        with patched(mod, isin_dedup_sql=spy), contextlib.redirect_stdout(io.StringIO()):
            call(mod, con, ex, offset_days=0)
        if seen != [0]:
            f.append(f"{ex}: {name} forwards offset_days=0 to the dedup, got {seen}")
    return f


def slot_failures(mod, topic, src):
    """The dedup runs before the topic's top-N cut: the twin collapses and the next security takes its slot."""
    con, _ = fetch(mod, src, SLOT_LEG)
    with contextlib.redirect_stdout(io.StringIO()):
        picked = [r[0] for r in SCREENS.get(topic, SCREEN_STOCKS)[1](mod, con, SLOT_LEG)]
    return [] if picked == SLOT_BOOK[topic] else [f"{SLOT_LEG}: top-N book {SLOT_BOOK[topic]}, got {picked}"]


def results_key_failures(topic, drop=None):
    """run_single/main can't run offline: the results must record the applied rule, and dedup as always on."""
    f = []
    for name in RESULT_FILES.get(topic, ("backtest.py",)):
        with open(os.path.join(ROOT, topic, name)) as fh:
            text = fh.read()
        if drop:
            check(drop in text, f"mutant target {drop!r} not in {topic}/{name}")
            text = text.replace(drop, "")
        f += [f"{topic}/{name}: results record {k}" for k in RESULT_KEYS if k not in text]
    return f


con = fixture()

# ---- 1. preconditions: the home-country map and the leg quote currencies the policy reuses ----
check(FILTERED_DOMICILE_EXCHANGES == {"XETRA", "SIX"}, f"filtered set {FILTERED_DOMICILE_EXCHANGES}")
check(EXCHANGE_COUNTRY["XETRA"] == ("DE",) and EXCHANGE_COUNTRY["SIX"] == ("CH",), "XETRA->DE, SIX->CH")
check(campaign_listing_condition(["XETRA"]) == domicile_sql_condition(["XETRA"]) == "country IN ('DE')",
      "XETRA condition is the repo's domicile_sql_condition")
check(campaign_listing_condition(["SIX"]) == "country IN ('CH')", "SIX condition")
check(campaign_listing_condition(["LSE"]) == "currency IN ('GBp')", "LSE condition: pence lines")
check(campaign_listing_condition(["STO"]) == "", "STO untouched")
LABELS = {("XETRA",): "country DE", ("SIX",): "country CH", ("XETRA", "SIX"): "country CH, DE",
          ("LSE",): "currency GBp", ("STO",): None, ("SIX", "LSE"): None, (): None}
for exs, want in LABELS.items():
    check(campaign_listing_label(list(exs)) == want, f"label {exs}: {campaign_listing_label(list(exs))!r}")
check(campaign_listing_label(None) is None, "label of a global run")
QUOTES = {("TSX",): "CAD", ("TSX", "TSXV"): "CAD", ("LSE",): "GBp", ("JNB",): "ZAc", ("TLV",): "ILA",
          ("HKSE",): "HKD", ("SIX",): "CHF", ("XETRA",): "EUR", ("STO",): "SEK", ("NYSE", "NASDAQ", "AMEX"): "USD",
          ("XETRA", "SIX"): None, ("LSE", "MEX"): None, ("MEX",): None, (): None}
for exs, want in QUOTES.items():
    check(leg_quote_currency(list(exs)) == want, f"quote currency {exs}: {leg_quote_currency(list(exs))!r}")
check(leg_quote_currency(None) is None, "quote currency of a global run")

# ---- 2. the real rules pass every check ----
check(listing_failures(con, campaign_listing_condition) == [],
      f"listing: {listing_failures(con, campaign_listing_condition)}")
check(gate_failures(load_isin_map) == [], f"gate: {gate_failures(load_isin_map)}")
check(dedup_failures(con, isin_dedup_sql) == [], f"dedup: {dedup_failures(con, isin_dedup_sql)}")
check(empty_map_failures(isin_dedup_sql) == [], f"empty map: {empty_map_failures(isin_dedup_sql)}")
PROP_SEEDS = range(200)       # 50 each of offset {0,1} x floor {0.5,1.0}
check(property_failures(isin_dedup_sql, PROP_SEEDS) == [], "property: winner != get_prices+entry_usable pick")
check(ranked_param(con, isin_dedup_sql) == ["DHL.DE", "PG.DE"], f"params bind {ranked_param(con, isin_dedup_sql)}")
# No twins in the book -> the wrapper is a no-op against the plain ranked query.
plain = [r[0] for r in con.execute("SELECT symbol FROM cands WHERE symbol LIKE '%.ST'"
                                   " ORDER BY score DESC, symbol LIMIT 3").fetchall()]
check(ranked(con, isin_dedup_sql, n=3, where=" WHERE symbol LIKE '%.ST'") == plain, "twin-free book unchanged")

SRC = source_db()
MODS = {t: load_topic(t) for t in TOPICS}
for t, mod in MODS.items():
    fails = leg_failures(mod, t, SRC) + forwarding_failures(mod, t, SRC) + results_key_failures(t)
    check(fails == [], f"{t} wiring: {fails}")
for t in SLOT_BOOK:
    check(slot_failures(MODS[t], t, SRC) == [], f"{t} top-N cut: {slot_failures(MODS[t], t, SRC)}")

# ---- 3. every check can fail: each broken rule trips its own check ----
real = campaign_listing_condition
LISTING_MUTANTS = {
    "STO added to the filtered set": (
        lambda e: domicile_sql_condition(e) if e and set(e) <= {"XETRA", "SIX", "STO"} else real(e),
        "STO keeps ABB"),
    "any-exchange instead of all-exchanges": (
        lambda e: domicile_sql_condition([x for x in e if x in FILTERED_DOMICILE_EXCHANGES])
        if e and set(e) & FILTERED_DOMICILE_EXCHANGES else real(e),
        "XETRA+STO leg untouched"),
    "filter never applied": (lambda e: "", "XETRA keeps only DE names"),
    "LSE gets country GB, not pence": (
        lambda e: domicile_sql_condition(e) if e and set(e) == {"LSE"} else real(e), "LSE keeps only pence lines"),
    "LSE keeps pounds (GBP) too": (
        lambda e: real(e).replace("('GBp')", "('GBp', 'GBP')"), "LSE keeps only pence lines"),
    "currency compared case-blind": (
        lambda e: real(e).replace("currency IN ('GBp')", "UPPER(currency) IN ('GBP')"), "LSE keeps only pence lines"),
    "LSE rule on any leg with LSE": (
        lambda e: "currency IN ('GBp')" if e and "LSE" in e else real(e), "SIX+LSE leg untouched"),
    "LSE rule dropped": (lambda e: "" if e and set(e) == {"LSE"} else real(e), "LSE keeps only pence lines"),
}
for name, (fn, expect) in LISTING_MUTANTS.items():
    fails = listing_failures(con, fn)
    check(any(expect in x for x in fails), f"listing mutant '{name}' not caught: {fails}")

OLD_SCOPE = lambda c, cn, e, sf, verbose=False: (load_isin_map(c, cn, e, sf) if e and set(e) <= FILTERED_DOMICILE_EXCHANGES
                                                 else cn.execute(EMPTY_MAP))
GATE_MUTANTS = {
    "isin_map gated to XETRA/SIX (the old scope)": (OLD_SCOPE, "('STO',): isin_map fetched once"),
    "home currency never flagged": (
        lambda c, cn, _e, sf, verbose=False: load_isin_map(c, cn, None, sf), "('TSX',): home-currency flag CAD"),
    "empty result keeps query_parquet's placeholder": (
        lambda c, cn, _e, sf, verbose=False: query_parquet(
            c, f"SELECT symbol, isin, isActivelyTrading FROM profile WHERE {sf}", cn, "isin_map"),
        "typed, empty isin_map"),
}
for name, (fn, expect) in GATE_MUTANTS.items():
    fails = gate_failures(fn)
    check(any(expect in x for x in fails), f"gate mutant '{name}' not caught: {fails}")
with patched(data_utils, leg_quote_currency=get_local_currency):    # LOCAL_CURRENCY's GBP is a reporting code
    fails = gate_failures(load_isin_map)
check(any("('LSE',): home-currency flag GBp" in x for x in fails), f"LSE quoted in pounds not caught: {fails}")

DEDUP_MUTANTS = {
    "NULL ISINs merge (partition by ISIN only)": (
        mutant("COALESCE(_i.isin, _c.symbol)", "_i.isin"), "no-ISIN lines ('NUL1', 'NUL2')"),
    "fanned-out rows kept (no-ISIN rows pass untouched)": (
        mutant("COALESCE(_i.isin, _c.symbol)", "COALESCE(_i.isin, CAST(uuid() AS VARCHAR))"),
        "a fanned-out symbol keeps one row"),
    "blank ISIN not nulled": (
        mutant("MIN(NULLIF(UPPER(TRIM(isin)), ''))", "MIN(UPPER(TRIM(isin)))"), "no-ISIN lines ('BLK1', 'BLK2')"),
    "ISIN not normalised": (
        mutant("MIN(NULLIF(UPPER(TRIM(isin)), ''))", "MIN(NULLIF(isin, ''))"), "twins ('CCC1', 'CCC2')"),
    "alphabetical only (entry price ignored)": (
        mutant("ORDER BY CASE WHEN", "ORDER BY _c.symbol, CASE WHEN"), "twins ('AADI', 'WHWK')"),
    "floor ignored": (
        mutant("_px.adjClose > 0 AND _px.adjClose >= 1.0", "_px.adjClose > 0"), "twins ('AAA1', 'ZZZ1')"),
    "live ticker ignored": (
        mutant("CASE WHEN _i.live THEN 0 ELSE 1 END, ", ""), "twins ('8DEAD', 'ELIVE')"),
    "home currency ignored": (
        mutant("CASE WHEN _i.home THEN 0 ELSE 1 END,", ""), "twins ('HOM-U.TO', 'HOM-UN.TO')"),
    "home currency before buyable": (
        mutant("ORDER BY CASE WHEN _px", "ORDER BY CASE WHEN _i.home THEN 0 ELSE 1 END, CASE WHEN _px"),
        "twins ('89988.HK', '9988.HK')"),
    "home currency before live": (
        mutant("CASE WHEN _i.live THEN 0 ELSE 1 END, CASE WHEN _i.home THEN 0 ELSE 1 END",
               "CASE WHEN _i.home THEN 0 ELSE 1 END, CASE WHEN _i.live THEN 0 ELSE 1 END"),
        "twins ('USCC-U.TO', 'USCC.TO')"),
    "offset ignored": (
        lambda c, d, _off, fl, **k: isin_dedup_sql(c, d, 0, fl, **k), "twins ('OND1', 'OND2')"),
    "window 20 days too wide": (
        lambda c, d, off, fl, **k: isin_dedup_sql(c, d, off, fl, window_days=30), "twins ('AFT1', 'AFT2')"),
    "window one day short": (
        lambda c, d, off, fl, **k: isin_dedup_sql(c, d, off, fl, window_days=9), "twins ('EDG1', 'EDG2')"),
    "best bar in window, not the first": (
        mutant("ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST) = 1) _px",
               "ORDER BY adjClose DESC NULLS LAST, trade_epoch ASC) = 1) _px"), "twins ('FST1', 'FST2')"),
    "no dedup (partition by symbol)": (
        mutant("PARTITION BY COALESCE(_i.isin, _c.symbol)", "PARTITION BY _c.symbol"),
        "collapsed twin frees its top-N slot"),
    "issuer-level merge (share classes collapse)": (
        mutant("COALESCE(_i.isin, _c.symbol)", "COALESCE(SUBSTR(_i.isin, 1, 9), _c.symbol)"),
        "share classes ('ATCO-A.ST', 'ATCO-B.ST')"),
    "isin_map duplicates fan out": (
        mutant(" GROUP BY symbol) _i", " GROUP BY symbol, isin) _i"), "one row per symbol"),
}
for name, (fn, expect) in DEDUP_MUTANTS.items():
    fails = dedup_failures(con, fn)
    check(any(expect in x for x in fails), f"dedup mutant '{name}' not caught: {fails}")
for name in ("NULL ISINs merge (partition by ISIN only)", "fanned-out rows kept (no-ISIN rows pass untouched)"):
    check(empty_map_failures(DEDUP_MUTANTS[name][0]) != [], f"empty-map check misses '{name}'")
for name in ("offset ignored", "window 20 days too wide", "best bar in window, not the first",
             "live ticker ignored", "home currency ignored", "home currency before buyable",
             "home currency before live"):
    check(property_failures(DEDUP_MUTANTS[name][0], range(80)) != [], f"property check misses '{name}'")

TOPIC_MUTANTS = {
    "listing filter dropped": ({"campaign_listing_condition": lambda _e: ""}, ("XETRA",), "XETRA: screen keeps"),
    "listing filter dropped (the queries show it)": (
        {"campaign_listing_condition": lambda _e: ""}, ("SIX",), "SIX: every query carries"),
    "LSE pence rule dropped": (
        {"campaign_listing_condition": lambda e: "" if set(e) == {"LSE"} else real(e)}, ("LSE",), "LSE: screen keeps"),
    "home-country rule on an untouched leg": (
        {"campaign_listing_condition": lambda e: domicile_sql_condition(e) if set(e) == {"STO"} else real(e)},
        ("STO",), "STO: no query filters"),
    "isin_map gated to XETRA/SIX (the old scope)": ({"load_isin_map": OLD_SCOPE}, ("STO",), "STO: isin_map fetched"),
    "dedup wrapper removed": ({"isin_dedup_sql": lambda c, *_a, **_k: c}, ("NYSE",), "NYSE: screen keeps"),
    "live flag not fetched": (
        {"load_isin_map": lambda c, cn, e, sf, verbose=False: (
            load_isin_map(c, cn, e, sf), cn.execute("UPDATE isin_map SET isActivelyTrading = NULL"))},
        ("XETRA",), "XETRA: screen keeps"),
    "home-currency flag not set": (
        {"load_isin_map": lambda c, cn, _e, sf, verbose=False: load_isin_map(c, cn, None, sf)},
        ("SIX",), "SIX: screen keeps"),
    "dedup floor swapped (0.50 <-> 1.0)": (
        {"isin_dedup_sql": lambda c, d, off, fl, **k: isin_dedup_sql(c, d, off, 1.5 - fl, **k)},
        ("SIX",), "SIX: screen keeps"),
}
# what drops offset_days (so its callee runs on the default) -> the forwarding check it must trip
FORWARD_MUTANTS = {"run_backtest": "run_backtest forwards", "screen": "to the dedup"}
drops_offset = lambda fn: lambda *a, offset_days=1, **k: fn(*a, **k)
for t, mod in MODS.items():
    for name, (attrs, legs, expect) in TOPIC_MUTANTS.items():
        with patched(mod, **attrs):
            fails = leg_failures(mod, t, SRC, legs)
        check(any(expect in x for x in fails), f"{t} mutant '{name}' not caught: {fails}")
    for what, expect in FORWARD_MUTANTS.items():
        name, _call, legs = SCREENS.get(t, SCREEN_STOCKS)
        name = name if what == "screen" else what
        with patched(mod, **{name: drops_offset(getattr(mod, name))}):
            fails = forwarding_failures(mod, t, SRC)
        check(sum(expect in x for x in fails) == len(legs),      # on every leg, so at every dedup site
              f"{t} mutant '{what} drops offset_days' not caught: {fails}")
    for key in RESULT_KEYS:
        fails = results_key_failures(t, drop=key)
        check(len(fails) == len(RESULT_FILES.get(t, ("backtest.py",))) and all(key in x for x in fails),
              f"{t} mutant 'results lose {key}' not caught: {fails}")
# Only these screens join profile, so only they can hold a fanned-out symbol twice.
for t in FANNED_TOPICS:
    with patched(MODS[t], isin_dedup_sql=DEDUP_MUTANTS["fanned-out rows kept (no-ISIN rows pass untouched)"][0]):
        fails = leg_failures(MODS[t], t, SRC, ("STO",))
    check(any("STO: a symbol held twice" in x for x in fails), f"{t} fan-out mutant not caught: {fails}")
# No dedup: the twin takes a slot and the 30th security is cut.
for t in SLOT_BOOK:
    with patched(MODS[t], **TOPIC_MUTANTS["dedup wrapper removed"][0]):
        fails = slot_failures(MODS[t], t, SRC)
    check(any("top-N book" in x for x in fails), f"{t} top-N mutant not caught: {fails}")

n_mut = (len(LISTING_MUTANTS) + len(GATE_MUTANTS) + 1 + len(DEDUP_MUTANTS)
         + (len(TOPIC_MUTANTS) + len(FORWARD_MUTANTS) + len(RESULT_KEYS)) * len(MODS) + len(FANNED_TOPICS)
         + len(SLOT_BOOK))
print(f"test_domicile_dedup: PASS ({len(MODS)} topics x {len(LEGS)} legs wired; {n_mut} mutants caught)")
