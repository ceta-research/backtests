#!/usr/bin/env python3
"""Offline checks for graham-net-net's price/NCAV currency handling (no network, no backtest).

adjClose is in profile.currency, grahamNetNet in key_metrics.reportedCurrency. The screen
restates NCAV in the price's unit: x100 on sub-unit quotes (GBp), and the newest USDxxx close
on/before the price bar, at most FX_MAX_STALE_DAYS old, when the currencies differ. A line with
no currency on either side, no fresh rate on either side, or a depositary receipt (profile.isAdr)
across currencies is dropped, and ccy_excluded() counts it (receipts=True: the receipts alone).

Every case runs on a local DuckDB fixture shaped like the tables the screen reads; every
mutant of the topic's source must trip its own named check.

Usage:
    python scripts/test_gnn_currency.py
"""
import contextlib
import io
import os
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from data_utils import LOCAL_CURRENCY, QUOTE_CURRENCY  # noqa: E402

PATH = os.path.join(ROOT, "graham-net-net", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()

D, BAR, FY = date(2020, 4, 1), date(2020, 4, 2), date(2019, 12, 31)


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    """The topic module built from `src` (the real file, or a mutant of it)."""
    mod = types.ModuleType("gnn_ccy")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


# symbol, trading ccy (profile), reporting ccy (key_metrics), adjClose, grahamNetNet
LINES = [
    ("INFY", "USD", "INR", 15.0, 300.0),      # USD line vs INR NCAV: 15 / (300/75) = 3.75 -> out (raw 0.05)
    ("WIT", "USD", "INR", 2.8, 300.0),        # 2.8 / 4 = 0.70 -> in at the converted ratio (not flagged isAdr)
    ("7203.T", "JPY", "JPY", 800.0, 1000.0),  # domestic JPY/JPY: 0.80, needs no FX
    ("UKP.L", "GBp", "GBP", 150.0, 2.0),      # 150p / 200p = 0.75 (raw 75 -> out)
    ("UKX.L", "GBp", "GBP", 1.5, 2.0),        # 1.5p / 200p = 0.0075 (raw 0.75)
    ("NBS.L", "GBP", "GBP", 1.3, 2.0),        # quoted in pounds: 0.65, no x100
    ("EXPN.L", "GBp", "USD", 68.0, 1.0),      # 68p / ($1 x 0.8 x 100) = 0.85 (raw 68 -> out)
    ("HSBK.L", "USD", "KZT", 12.0, 3000.0),   # 12 / (3000/430) = 1.72 -> out (raw 0.004)
    ("0939.HK", "HKD", "CNY", 9.5, 10.0),     # 9.5 / (10 x 7.75/7.1) = 0.870, HKD rate 3 days old
    ("NCU.TO", "CAD", "USD", 1.30, 1.0),      # 1.30 / 1.40 = 0.929 (raw 1.30 -> out)
    ("NULLR", "USD", None, 1.0, 5.0),         # NULL reportedCurrency -> dropped, counted (raw 0.2)
    ("NOCCY", None, "USD", 1.0, 5.0),         # no profile currency -> dropped, counted
    ("STALE.L", "USD", "RUB", 1.0, 1000.0),   # only a 10-day-old USDRUB (reporting side) -> dropped, counted
    ("STALET", "EUR", "USD", 1.0, 2.0),       # only an 8-day-old USDEUR (trading side) -> dropped (fresh: 0.55)
    ("ADRX", "USD", "TWD", 1.0, 300.0),       # receipt across currencies -> dropped, counted (converted: 0.10)
    ("ADRU", "USD", "USD", 4.5, 5.0),         # receipt in its reporting currency: 0.90, kept
]
ADR = {"ADRX", "ADRU"}   # profile.isAdr
RATIO = {"INFY": 3.75, "WIT": 0.70, "7203.T": 0.80, "UKP.L": 0.75, "UKX.L": 0.0075, "NBS.L": 0.65,
         "EXPN.L": 0.85, "HSBK.L": 12 / (3000 / 430), "0939.HK": 9.5 / (10 * 7.75 / 7.1), "NCU.TO": 1.30 / 1.40,
         "NULLR": None, "NOCCY": None, "STALE.L": None, "STALET": None, "ADRX": None, "ADRU": 0.90}
BOOK = ["UKX.L", "NBS.L", "WIT", "UKP.L", "7203.T", "EXPN.L", "0939.HK", "ADRU", "NCU.TO"]
FX = [("INR", BAR, 75.0), ("GBP", BAR, 0.8), ("KZT", BAR, 430.0), ("HKD", BAR - timedelta(days=3), 7.75),
      ("CNY", BAR, 7.1), ("CAD", BAR, 1.40), ("RUB", BAR - timedelta(days=10), 75.0),
      ("EUR", BAR - timedelta(days=8), 0.91), ("TWD", BAR, 30.0),
      # after the bar: must never be used
      ("INR", BAR + timedelta(days=1), 1.0), ("GBP", BAR + timedelta(days=1), 0.5), ("KZT", BAR + timedelta(days=1), 9.0)]


def screen_db(lines=LINES, fx=FX, isin=()):
    con = duckdb.connect()
    con.execute("CREATE TABLE metrics_cache(symbol VARCHAR, grahamNetNet DOUBLE, marketCap DOUBLE,"
                " currentRatio DOUBLE, reportedCurrency VARCHAR, filing_epoch BIGINT)")
    con.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE ccy_map(symbol VARCHAR, currency VARCHAR, is_adr BOOLEAN)")
    con.execute("CREATE TABLE fx_cache(ccy VARCHAR, fx_epoch BIGINT, per_usd DOUBLE)")
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    for s, t, r, px, g in lines:
        con.execute("INSERT INTO metrics_cache VALUES (?, ?, 1e12, 2.0, ?, ?)", [s, g, r, ep(FY)])
        con.execute("INSERT INTO prices_cache VALUES (?, ?, ?, 1e6)", [s, ep(BAR), px])
        if t is not None:
            con.execute("INSERT INTO ccy_map VALUES (?, ?, ?)", [s, t, s in ADR])
    for c, d, v in fx:
        con.execute("INSERT INTO fx_cache VALUES (?, ?, ?)", [c, ep(d), v])
    for s, i in isin:
        con.execute("INSERT INTO isin_map VALUES (?, ?, TRUE, NULL)", [s, i])
    return con


def book(mod, con):
    return [r[0] for r in mod.screen_stocks(con, D, 1e6, offset_days=1)]


def ratios(mod, con):
    ctes, params = mod.ncav_lines_sql(D, 1e6)
    return dict(con.execute(ctes + " SELECT symbol, adjClose / ncav_px FROM conv", params).fetchall())


class FakeClient:
    """Stands in for CetaResearch: runs the topic's own SQL on a source DB, parquet like the API."""

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


def fx_source():
    src = duckdb.connect()
    src.execute("CREATE TABLE forex_prices_daily(symbol VARCHAR, date DATE, dateEpoch BIGINT, close DOUBLE)")
    for sym, d, v in [("USDINR", BAR, 75.0), ("USDGBP", BAR, 0.8), ("EURUSD", BAR, 1.1), ("USDEUR", BAR, 0.91),
                      ("USDKZT", BAR, 430.0), ("USDKZT", D - timedelta(days=30), 400.0), ("USDTWD", BAR, 30.0)]:
        src.execute("INSERT INTO forex_prices_daily VALUES (?, ?, ?, ?)", [sym, d, ep(d), v])
    return src


# profile + key_metrics + stock_eod + forex_prices_daily: what fetch_data_via_api reads.
E2E = [  # symbol, exchange, trading ccy, reporting ccy, adjClose, grahamNetNet
    ("INFY", "NYSE", "USD", "INR", 15.0, 300.0), ("WIT", "NYSE", "USD", "INR", 2.8, 300.0),
    ("AAA", "NYSE", "USD", "USD", 4.0, 5.0), ("NULLR", "NYSE", "USD", None, 1.0, 5.0),
    ("ADRX", "NYSE", "USD", "TWD", 1.0, 300.0),   # receipt across currencies: dropped once isAdr is read
    ("UKP.L", "LSE", "GBp", "GBP", 150.0, 2.0), ("EXPN.L", "LSE", "GBp", "USD", 68.0, 1.0),
    ("NBS.L", "LSE", "GBP", "GBP", 1.3, 2.0),   # pounds line: the LSE listing rule drops it
]


def e2e_source():
    src = fx_source()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, isin VARCHAR,"
                " isActivelyTrading BOOLEAN, currency VARCHAR, isAdr BOOLEAN)")
    src.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, dateEpoch BIGINT, grahamNetNet DOUBLE,"
                " marketCap DOUBLE, currentRatio DOUBLE, reportedCurrency VARCHAR)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    for i, (s, ex, t, r, px, g) in enumerate(E2E):
        src.execute("INSERT INTO profile VALUES (?, ?, 'XX', ?, TRUE, ?, ?)", [s, ex, f"XX{i:010d}", t, s in ADR])
        src.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?, 1e12, 2.0, ?)", [s, ep(FY), g, r])
        src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, ?, 1e6)", [s, BAR, ep(BAR), px])
    return src


def failures(mod, src):
    f = []

    def chk(cond, what):
        if not cond:
            f.append(what)

    def section(name, fn):
        try:
            fn()
        except Exception as e:  # a mutant can break the SQL outright; that is a failure too
            f.append(f"{name}: raised {type(e).__name__}: {str(e).splitlines()[0][:120]}")

    def sub_units():
        chk(set(QUOTE_CURRENCY.values()) <= set(mod.SUBUNIT_CCY), "every QUOTE_CURRENCY sub-unit is scaled")
        chk(all(mod.SUBUNIT_CCY[QUOTE_CURRENCY[e]] == LOCAL_CURRENCY[e] for e in QUOTE_CURRENCY),
            "sub-units map to the exchange's reporting currency")
        chk("GBP" not in mod.SUBUNIT_CCY, "pounds are not a sub-unit")

    def converted():
        con = screen_db()
        got = ratios(mod, con)
        for s, want in RATIO.items():
            ok = got.get(s) is None if want is None else got.get(s) is not None and abs(got[s] - want) < 1e-9
            chk(ok, f"ratio {s}: {got.get(s)} != {want}")
        chk(book(mod, con) == BOOK, f"converted book {book(mod, con)} != {BOOK}")
        chk(mod.ccy_excluded(con, D, 1e6) == 5, f"ccy_excluded with FX {mod.ccy_excluded(con, D, 1e6)} != 5")
        chk(mod.ccy_excluded(con, D, 1e6, receipts=True) == 1, "ccy_excluded receipts: ADRX alone")
        chk("INFY" not in book(mod, con), "USD line vs INR NCAV: excluded once converted")
        chk("NULLR" not in book(mod, con), "NULL reportedCurrency: dropped")

    def no_fx():
        # matched and pence/pound lines need no FX; every cross-currency line drops and is counted
        con = screen_db(fx=())
        chk(book(mod, con) == ["UKX.L", "NBS.L", "UKP.L", "7203.T", "ADRU"], f"no-FX book {book(mod, con)}")
        chk(mod.ccy_excluded(con, D, 1e6) == 11, f"no-FX ccy_excluded {mod.ccy_excluded(con, D, 1e6)} != 11")

    def adr_no_rate():
        # USD line vs INR NCAV with only an 8-day-old USDINR: both lines excluded and counted
        con = screen_db(fx=[x for x in FX if x[0] != "INR"] + [("INR", BAR - timedelta(days=8), 75.0)])
        got = book(mod, con)
        chk("INFY" not in got and "WIT" not in got, f"USD line vs INR NCAV: excluded without a fresh rate {got}")
        chk(mod.ccy_excluded(con, D, 1e6) == 7, f"stale-INR ccy_excluded {mod.ccy_excluded(con, D, 1e6)} != 7")

    def domestic():
        con = screen_db(lines=[l for l in LINES if l[0] == "7203.T"], fx=())
        chk(book(mod, con) == ["7203.T"] and mod.ccy_excluded(con, D, 1e6) == 0, "domestic JPY/JPY kept")

    def dedup():
        # conversion happens before the ISIN dedup: the twin that fails conversion can't take the slot
        con = screen_db(isin=[("HSBK.L", "KZ000A0LE0S4"), ("HSBK2.L", "KZ000A0LE0S4")],
                        lines=LINES + [("HSBK2.L", "USD", "KZT", 5.0, 3000.0)])   # 5 / 6.98 = 0.717 -> in
        got = book(mod, con)
        chk("HSBK2.L" in got and "HSBK.L" not in got, f"dedup sees converted candidates {got}")

    def top_n():
        saved, mod.MAX_STOCKS = mod.MAX_STOCKS, 2
        try:
            chk(book(mod, screen_db()) == ["UKX.L", "NBS.L"], "top-N cut on the converted ratio")
        finally:
            mod.MAX_STOCKS = saved

    def fx_case(lines):
        con = screen_db(lines=lines, fx=())
        con.execute("DROP TABLE fx_cache")
        cl = FakeClient(fx_source())
        with contextlib.redirect_stdout(io.StringIO()):
            mod.fetch_fx(cl, con, [D])
        typed = [c[1] for c in con.execute("DESCRIBE fx_cache").fetchall()] == ["VARCHAR", "BIGINT", "DOUBLE"]
        return cl.sqls, sorted(con.execute("SELECT ccy, per_usd FROM fx_cache").fetchall()), typed

    def fetch_fx():
        sqls, rows, typed = fx_case([l for l in LINES if l[1] == l[2] or (l[1], l[2]) == ("GBp", "GBP")])
        chk(sqls == [] and rows == [] and typed, f"no mismatch -> no FX query, typed empty fx_cache {sqls}")
        sqls, rows, typed = fx_case([("HSBK.L", "USD", "KZT", 12.0, 3000.0), ("EXPN.L", "GBp", "USD", 68.0, 1.0)])
        chk(len(sqls) == 1 and "'USDGBP'" in sqls[0] and "'USDKZT'" in sqls[0] and "USDUSD" not in sqls[0],
            f"fetch_fx asks USDxxx for exactly the needed currencies {sqls}")
        chk(rows == [("GBP", 0.8), ("KZT", 430.0)], f"fetch_fx keeps the rebalance window only {rows}")
        sqls, rows, typed = fx_case([("X.DE", "USD", "EUR", 1.0, 2.0)])
        chk(rows == [("EUR", 0.91)], f"EUR from USDEUR, not EURUSD {rows}")
        sqls, rows, typed = fx_case([("Y.L", "USD", "LBP", 1.0, 2.0)])
        chk(len(sqls) == 1 and rows == [] and typed, "empty FX result leaves a typed fx_cache")

    def wiring():
        for ex, want, n_excl in (("NYSE", ["WIT", "AAA"], 2), ("LSE", ["UKP.L", "EXPN.L"], 0)):
            with contextlib.redirect_stdout(io.StringIO()):
                con = mod.fetch_data_via_api(FakeClient(e2e_source()), [ex], [D])
                got = book(mod, con)
            chk(got == want, f"wiring {ex}: profile/key_metrics/forex -> book {got} != {want}")
            chk(mod.ccy_excluded(con, D, 1e6) == n_excl, f"wiring {ex}: ccy_excluded != {n_excl}")

    def results():
        chk('"ncav_ccy_excluded": {"total": sum(ccy_excl.values()), "receipts": dr_excl, "by_period": ccy_excl}' in src
            and "ccy_excluded(con, d, mktcap_threshold) for d in rebalance_dates[:-1]" in src
            and "ccy_excluded(con, d, mktcap_threshold, receipts=True) for d in rebalance_dates[:-1]" in src,
            "results record the per-period, total and receipt currency exclusions")

    for name, fn in (("sub_units", sub_units), ("converted", converted), ("no_fx", no_fx),
                     ("adr_no_rate", adr_no_rate), ("domestic", domestic), ("dedup", dedup), ("top_n", top_n),
                     ("fetch_fx", fetch_fx), ("wiring", wiring), ("results", results)):
        section(name, fn)
    return f


fails = failures(load(SRC), SRC)
if fails:
    raise SystemExit("FAIL:\n  " + "\n  ".join(fails))

# ---- every check can fail: each broken rule trips its own check ----
T_LEG = "CASE WHEN tmaj = 'USD' THEN 1.0 WHEN trade_epoch - t_ep <= {stale} THEN t_usd END"   # FX numerator
R_LEG = "CASE WHEN rccy = 'USD' THEN 1.0 WHEN trade_epoch - r_ep <= {stale} THEN r_usd END"   # FX denominator
MUTANTS = {
    "no conversion at all (the old screen)": (
        [("END AS ncav_px", "END AS ncav_fx, grahamNetNet AS ncav_px")], "ratio INFY"),
    "no x100 on pence lines": ([("THEN 100.0 ELSE 1.0 END", "THEN 1.0 ELSE 1.0 END")], "ratio UKP.L"),
    "pounds scaled as pence": (
        [('SUBUNIT_CCY = {"GBp": "GBP",', 'SUBUNIT_CCY = {"GBP": "GBP", "GBp": "GBP",')], "ratio NBS.L"),
    "pence lines compared to GBP as a foreign currency": (
        [("{major_ccy_sql('u.currency')} AS tmaj", "u.currency AS tmaj")], "no-FX book"),
    "FX inverted": ([(T_LEG, "@T@"), (R_LEG, T_LEG), ("@T@", R_LEG)], "ratio 0939.HK"),
    "no staleness bound": ([("FX_MAX_STALE_DAYS = 7 ", "FX_MAX_STALE_DAYS = 30 ")], "ratio STALE.L"),
    "no trading-side staleness test": (
        [("WHEN trade_epoch - t_ep <= {stale} THEN t_usd END", "ELSE t_usd END")], "ratio STALET"),
    "no reporting-side staleness test": (
        [("WHEN trade_epoch - r_ep <= {stale} THEN r_usd END", "ELSE r_usd END")], "ratio STALE.L"),
    "cross-currency receipts converted": ([("WHEN adr THEN NULL\n", "")], "ratio ADRX"),
    "receipts dropped in their own currency too": (
        [("CASE WHEN tmaj = rccy THEN 1.0\n", "CASE WHEN adr THEN NULL WHEN tmaj = rccy THEN 1.0\n")], "ratio ADRU"),
    "receipt count ignores the DR flag": ([('dr = " AND dr" if receipts', 'dr = "" if receipts')],
                                          "ccy_excluded receipts"),
    "isAdr not fetched": (
        [("currency, isAdr FROM profile", "currency, CAST(NULL AS BOOLEAN) AS isAdr FROM profile")], "wiring NYSE"),
    "missing FX passes unconverted": (
        [("END AS ncav_px", "END AS ncav_fx, COALESCE(ncav_fx, grahamNetNet) AS ncav_px")], "no-FX ccy_excluded"),
    "rate after the bar allowed": (
        [("b.trade_epoch >= f.fx_epoch", "b.trade_epoch + 86400 >= f.fx_epoch")], "ratio INFY"),
    "NULL reportedCurrency treated as matched": (
        [("km.reportedCurrency AS rccy", "COALESCE(km.reportedCurrency, {major_ccy_sql('u.currency')}) AS rccy")],
        "NULL reportedCurrency: dropped"),
    "screen filter after the dedup": (
        [("FROM conv\n        WHERE adjClose < ncav_px", "FROM conv"),
         ("ORDER BY price_to_ncav ASC, symbol", "WHERE price_to_ncav < 1 ORDER BY price_to_ncav ASC, symbol")],
        "dedup sees converted candidates"),
    "count misses lines without a currency": (
        [("SELECT COUNT(*) FROM conv WHERE ncav_px IS NULL", "SELECT COUNT(*) FROM conv WHERE NOT adjClose < ncav_px")],
        "ccy_excluded with FX"),
    "xxxUSD pairs": ([("'USD{c}'", "'{c}USD'")], "USDxxx for exactly the needed currencies"),
    "FX fetched for matched lines": ([("WHERE t <> r)", "WHERE TRUE)")], "no mismatch -> no FX query"),
    "profile currency not fetched": (
        [("SELECT DISTINCT symbol, exchange, currency, isAdr FROM profile",
          "SELECT DISTINCT symbol, exchange, isAdr FROM profile")],
        "wiring NYSE"),
    "reportedCurrency not fetched": (
        [("currentRatio, reportedCurrency,\n", "currentRatio, CAST(NULL AS VARCHAR) AS reportedCurrency,\n")],
        "wiring NYSE"),
    "results drop the count": (
        [('"ncav_ccy_excluded": {"total": sum(ccy_excl.values()), "receipts": dr_excl, "by_period": ccy_excl},', "")],
        "results record"),
}
for name, (edits, expect) in MUTANTS.items():
    src = SRC
    for old, new in edits:
        if old not in src:
            raise SystemExit(f"FAIL: mutant '{name}' anchor missing: {old!r}")
        src = src.replace(old, new)
    fails = failures(load(src), src)
    if not any(expect in x for x in fails):
        raise SystemExit(f"FAIL: mutant '{name}' not caught by '{expect}': {fails}")

print(f"test_gnn_currency: PASS ({len(LINES)} screen lines, fetch_fx, 2 wired legs; {len(MUTANTS)} mutants caught)")
