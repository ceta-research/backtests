#!/usr/bin/env python3
"""Offline check of dcf-discount's selection: latest FY filing first, FCF yield in a closed band (no network).

screen_stocks must take each company's latest FY cash-flow filing on or before the 45-day cutoff
and at most 540 days old first, and only then require freeCashFlow > 0 in the leg's reporting
currency; the key_metrics filing is picked the same way before the size floor. Testing first
(positive-only, leg-currency-only fetches, then rn = 1) kept a company whose free cash flow had
turned negative, or whose newest year was filed in another currency, on an older year's figure.
FCF yield must sit in [FCF_YIELD_MIN, FCF_YIELD_MAX], both ends included. A leg with no single
reporting currency applies no currency rule. The listing rule, the fund filter and the
same-security dedup before the top-N cut still hold.

The rows are made-up warehouse tables; a stand-in client runs the topic's own SQL on them, so every
book comes out of the real fetch_data_via_api and screen_stocks. Every mutant of the topic's source
must trip a check.

Usage:
    python scripts/test_dcf_selection.py [repo root]
"""
import contextlib
import io
import math
import os
import sys
import tempfile
import types
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PATH = os.path.join(ROOT, "dcf-discount", "backtest.py")
with open(PATH) as fh:
    SRC = fh.read()
sys.path.insert(0, ROOT)

D = date(2020, 4, 1)
FLOOR, NO_FLOOR, MC = 5e9, -1e30, 1e10
FY18, FY19 = date(2018, 12, 31), date(2019, 12, 31)   # 457 and 92 days before D
AT_CUTOFF, IN_LAG = D - timedelta(days=45), D - timedelta(days=44)
AT_MAX_AGE, TOO_OLD = D - timedelta(days=540), D - timedelta(days=541)
QTR = date(2020, 1, 31)                 # a quarterly row newer than FY19: never read
YMIN = (1 + 0.20) / ((1 + 0.025) / (0.10 - 0.025))   # 20% DCF discount as an FCF yield, ~8.78%
YMAX = 0.50
P2 = float(2 ** 34)                     # a power-of-two cap: FCF / P2 is exact, so a yield can sit on a bound
CAPS = [(FY18, MC), (FY19, MC)]


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def load(src):
    mod = types.ModuleType("dcf_selection")
    mod.__file__ = PATH
    exec(compile(src, PATH, "exec"), mod.__dict__)
    return mod


def one(yld, mc=MC, ccy="SEK"):
    """A company with a single FY19 filing at this yield."""
    return [(FY19, yld * mc, ccy)], [(FY19, mc)]


# symbol: (cash_flow_statement FY rows [(date, freeCashFlow, reportedCurrency)], key_metrics FY rows [(date, marketCap)])
STO = {
    # both years pass: ranked on the latest figure, not the better one
    "OK": ([(FY18, 0.40 * MC, "SEK"), (FY19, 0.30 * MC, "SEK")], CAPS),
    # latest free cash flow negative, zero or missing: out (the old screen held all three on FY18)
    "NEG": ([(FY18, 0.45 * MC, "SEK"), (FY19, -0.45 * MC, "SEK")], CAPS),
    "ZERO": ([(FY18, 0.44 * MC, "SEK"), (FY19, 0.0, "SEK")], CAPS),
    "NULLF": ([(FY18, 0.43 * MC, "SEK"), (FY19, None, "SEK")], CAPS),
    # latest filing in another currency, or with none stated: out
    "CCY": ([(FY18, 0.42 * MC, "SEK"), (FY19, 0.42 * MC, "EUR")], CAPS),
    "CCY_NULL": ([(FY18, 0.41 * MC, "SEK"), (FY19, 0.41 * MC, None)], CAPS),
    # older year in another currency, latest in the leg's: in
    "CCY_BACK": ([(FY18, 0.40 * MC, "EUR"), (FY19, 0.25 * MC, "SEK")], CAPS),
    # newest row is one day inside the 45-day lag and fails; the one before passes: in
    "LAG_F": ([(FY19, 0.26 * MC, "SEK"), (IN_LAG, -0.50 * MC, "SEK")], CAPS),
    "LAG_M": ([(FY19, 0.23 * MC, "SEK")], [(FY19, MC), (IN_LAG, 0.0)]),
    # filings exactly on the cutoff and exactly 540 days old: both bounds are inclusive
    "EDGE_LAG": ([(AT_CUTOFF, 0.27 * MC, "SEK")], [(AT_CUTOFF, MC)]),
    "EDGE_AGE": ([(AT_MAX_AGE, 0.28 * MC, "SEK")], [(AT_MAX_AGE, MC)]),
    # only filing is 541 days old, in either table: out
    "OLD_F": ([(TOO_OLD, 0.39 * MC, "SEK")], [(FY19, MC)]),
    "OLD_M": ([(FY19, 0.38 * MC, "SEK")], [(TOO_OLD, MC)]),
    # latest market cap zero or missing: out (the old fetch dropped that row and used FY18)
    "MC_ZERO": ([(FY18, 0.20 * MC, "SEK"), (FY19, 0.20 * MC, "SEK")], [(FY18, MC), (FY19, 0.0)]),
    "MC_NULL": ([(FY18, 0.20 * MC, "SEK"), (FY19, 0.20 * MC, "SEK")], [(FY18, MC), (FY19, None)]),
    # latest market cap on the size floor: in; one unit under: out
    "AT_FLOOR": one(0.21, FLOOR),
    "SHRANK": ([(FY18, 0.20 * FLOOR, "SEK"), (FY19, 0.20 * FLOOR, "SEK")], [(FY18, MC), (FY19, FLOOR - 1)]),
    # yield exactly on a bound: in; the next representable free cash flow outside it: out
    "AT_CAP": ([(FY19, YMAX * P2, "SEK")], [(FY19, P2)]),
    "OVER_CAP": ([(FY19, math.nextafter(YMAX * P2, math.inf), "SEK")], [(FY19, P2)]),
    "AT_MIN": ([(FY19, YMIN * P2, "SEK")], [(FY19, P2)]),
    "UNDER_MIN": ([(FY19, math.nextafter(YMIN * P2, 0.0), "SEK")], [(FY19, P2)]),
    # with the size floor off: one unit of free cash flow is in, minus one over a negative cap is out
    "TINY": ([(FY19, 1.0, "SEK")], [(FY19, 10.0)]),
    "TINY_NEG": ([(FY19, -1.0, "SEK")], [(FY19, -10.0)]),
    # two rows on one date: the larger free cash flow, then the leg-currency row, the larger cap
    "DUP_F": ([(FY19, 0.10 * MC, "SEK"), (FY19, 0.32 * MC, "SEK")], [(FY19, MC)]),
    "DUP_C": ([(FY19, 0.31 * MC, "EUR"), (FY19, 0.31 * MC, "SEK")], [(FY19, MC)]),
    "DUP_M": ([(FY19, 0.09 * MC, "SEK")], [(FY19, MC / 4), (FY19, MC)]),
    # one security, two lines: the line priced under 1.0 loses
    "TWIN_A": one(0.35),
    "TWIN_B": one(0.35),
    # a fund: never in the universe
    "FUND": one(0.34),
}
# 52 names, yield falling with the number, C50 and C51 tied; C01 has a twin line that must not take a slot
OSL = {f"C{i:02d}": one(0.20 if i in (50, 51) else 0.45 - i / 200, ccy="NOK") for i in range(1, 53)}
OSL["C01Z"] = OSL["C01"]
# two reporting currencies on the leg: no currency rule, the latest-filing rule still holds
MIXED = {
    "MIX_USD": (one(0.30, ccy="USD"), "NYSE"),
    "MIX_CAD": (one(0.20, ccy="CAD"), "TSX"),
    "MIX_NOCCY": (one(0.15, ccy=None), "TSX"),
    "MIX_SW": (([(FY18, 0.50 * MC, "USD"), (FY19, 0.10 * MC, "EUR")], CAPS), "NYSE"),
    "MIX_NEG": (([(FY18, 0.40 * MC, "CAD"), (FY19, -0.10 * MC, "CAD")], CAPS), "TSX"),
}
# the XETRA listing rule: the line of a company domiciled elsewhere is out
XETRA = {"HOME.DE": (one(0.20, ccy="EUR"), "DE"), "FRGN.DE": (one(0.30, ccy="EUR"), "US")}
ISIN = {"TWIN_A": "SE0000000001", "TWIN_B": "SE0000000001", "C01": "NO0000000001", "C01Z": "NO0000000001"}
PRICE = {"TWIN_A": 0.5}

STO_BOOK = [("AT_CAP", P2), ("TWIN_B", MC), ("DUP_F", MC), ("DUP_C", MC), ("OK", MC), ("EDGE_AGE", MC),
            ("EDGE_LAG", MC), ("LAG_F", MC), ("CCY_BACK", MC), ("LAG_M", MC), ("AT_FLOOR", FLOOR)]
EXPECT = {   # yield descending, then symbol
    "STO": STO_BOOK + [("DUP_M", MC), ("AT_MIN", P2)],
    "STO, no size floor": STO_BOOK + [("SHRANK", FLOOR - 1), ("TINY", 10.0), ("DUP_M", MC), ("AT_MIN", P2)],
    "OSL top 50 of 52": [(f"C{i:02d}", MC) for i in range(1, 51)],
    "NYSE+TSX": [("MIX_USD", MC), ("MIX_CAD", MC), ("MIX_NOCCY", MC), ("MIX_SW", MC)],
    "XETRA": [("HOME.DE", MC)],
}
# leg: (exchanges, [(book, size floor)])
LEGS = {
    "STO": (["STO"], [("STO", FLOOR), ("STO, no size floor", NO_FLOOR)]),
    "OSL": (["OSL"], [("OSL top 50 of 52", FLOOR)]),
    "NYSE+TSX": (["NYSE", "TSX"], [("NYSE+TSX", FLOOR)]),
    "XETRA": (["XETRA"], [("XETRA", FLOOR)]),
}


def warehouse():
    """profile, cash_flow_statement, key_metrics and stock_eod: what fetch_data_via_api reads."""
    src = duckdb.connect()
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, country VARCHAR, currency VARCHAR,"
                " isin VARCHAR, isFund BOOLEAN, isEtf BOOLEAN, isActivelyTrading BOOLEAN)")
    src.execute("CREATE TABLE cash_flow_statement(symbol VARCHAR, period VARCHAR, dateEpoch BIGINT,"
                " freeCashFlow DOUBLE, reportedCurrency VARCHAR)")
    src.execute("CREATE TABLE key_metrics(symbol VARCHAR, period VARCHAR, dateEpoch BIGINT, marketCap DOUBLE)")
    src.execute("CREATE TABLE stock_eod(symbol VARCHAR, date DATE, dateEpoch BIGINT, adjClose DOUBLE, volume DOUBLE)")
    lines = ([(s, f, "STO", "SE", "SEK") for s, f in STO.items()]
             + [(s, f, "OSL", "NO", "NOK") for s, f in OSL.items()]
             + [(s, f, ex, "US", "USD" if ex == "NYSE" else "CAD") for s, (f, ex) in MIXED.items()]
             + [(s, f, "XETRA", country, "EUR") for s, (f, country) in XETRA.items()])
    bar = D + timedelta(days=1)
    for n, (s, (cash_flows, caps), ex, country, quote) in enumerate(lines):
        src.execute("INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?, FALSE, TRUE)",
                    [s, ex, country, quote, ISIN.get(s, f"XX{n:010d}"), s == "FUND"])
        for d, fcf, ccy in cash_flows:
            src.execute("INSERT INTO cash_flow_statement VALUES (?, 'FY', ?, ?, ?)", [s, ep(d), fcf, ccy])
        for d, mcap in caps:
            src.execute("INSERT INTO key_metrics VALUES (?, 'FY', ?, ?)", [s, ep(d), mcap])
        src.execute("INSERT INTO stock_eod VALUES (?, ?, ?, ?, 1e6)", [s, bar, ep(bar), PRICE.get(s, 5.0)])
    # quarterly rows after FY19 that would sink OK if a fetch read them
    src.execute("INSERT INTO cash_flow_statement VALUES ('OK', 'Q4', ?, ?, 'SEK')", [ep(QTR), -0.30 * MC])
    src.execute("INSERT INTO key_metrics VALUES ('OK', 'Q4', ?, 0.0)", [ep(QTR)])
    return src


class _Client:
    """Stands in for CetaResearch: runs the topic's own SQL on the made-up warehouse, parquet like the API."""

    def __init__(self, src):
        self.src, self.sent = src, []

    def query(self, sql, format="json", **_kw):
        self.sent.append(sql)
        if format != "parquet":
            cur = self.src.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.parquet")
            self.src.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
            with open(path, "rb") as fh:
                return fh.read()


WAREHOUSE = warehouse()


def failures(src):
    out = []
    try:
        mod = load(src)
        for leg, (exchanges, books) in LEGS.items():
            client = _Client(WAREHOUSE)
            with contextlib.redirect_stdout(io.StringIO()):
                con = mod.fetch_data_via_api(client, exchanges, [D])
            for name, floor in books:
                got = [(r[0], r[1]) for r in mod.screen_stocks(con, D, floor, offset_days=1)]
                if got != EXPECT[name]:
                    out.append(f"{name}: book {got} != {EXPECT[name]}")
            con.close()
            # universe, isin_map, cash flow, key metrics, prices: the fix adds no query
            if len(client.sent) != 5:
                out.append(f"{leg}: {len(client.sent)} queries sent, not 5")
    except Exception as e:  # a mutant that breaks the module or its SQL fails loudly
        return [f"error: {type(e).__name__}: {str(e).splitlines()[0][:200]}"]
    return out


F_WHERE = "            FROM fcf_cache\n            WHERE filing_epoch <= ? AND "
M_WHERE = "            FROM metrics_cache\n            WHERE filing_epoch <= ? AND "
AGE, NO_AGE = "filing_epoch >= ?", "? IS NOT NULL"
F_TIES = "freeCashFlow DESC NULLS LAST, ccy_ok DESC NULLS LAST) AS rn"
M_TIES = "marketCap DESC NULLS LAST) AS rn"
T_FCF, T_CCY = "          AND f.freeCashFlow > 0\n", "          AND f.ccy_ok\n"
T_FLOOR = "          AND km.marketCap >= ?"
T_MIN, T_MAX = "          AND f.freeCashFlow / km.marketCap >= ?", "          AND f.freeCashFlow / km.marketCap <= ?"
LAG = "cutoff_epoch = utc_epoch(target_date - timedelta(days=45))"
MAX_AGE = "earliest_epoch = utc_epoch(target_date - timedelta(days=540))"
PARAMS = ("[cutoff_epoch, earliest_epoch, cutoff_epoch, earliest_epoch,\n"
          "          mktcap_min, FCF_YIELD_MIN, FCF_YIELD_MAX, MAX_STOCKS]")
C_MIN, C_MAX = "FCF_YIELD_MIN = (1 + DISCOUNT_THRESHOLD) / DCF_MULTIPLE", "FCF_YIELD_MAX = 0.50"
WRAP = "FROM ({isin_dedup_sql(candidates, target_date, offset_days, MIN_ENTRY_PRICE)})\n"
CUT = "        ORDER BY fcf_yield DESC, symbol\n"
CF_FETCH = "            FROM cash_flow_statement\n            WHERE period = 'FY'\n"
KM_FETCH = "            FROM key_metrics\n            WHERE period = 'FY'\n"
CCY_FLAG = "ccy_ok_sql = f\"reportedCurrency = '{local_ccy}'\" if local_ccy else \"TRUE\""
MUTANTS = {
    # the old bug: a test applied before picking the latest filing
    "FCF test before latest": (F_WHERE + AGE, F_WHERE + AGE + " AND freeCashFlow > 0"),
    "missing FCF skipped": (F_WHERE + AGE, F_WHERE + AGE + " AND freeCashFlow IS NOT NULL"),
    "currency test before latest": (F_WHERE + AGE, F_WHERE + AGE + " AND ccy_ok"),
    "cap test before latest": (M_WHERE + AGE, M_WHERE + AGE + " AND marketCap > 0"),
    "missing cap skipped": (M_WHERE + AGE, M_WHERE + AGE + " AND marketCap IS NOT NULL"),
    "oldest filing in the window (cash flow)": ("ORDER BY filing_epoch DESC,\n                    freeCashFlow",
                                                "ORDER BY filing_epoch ASC,\n                    freeCashFlow"),
    "oldest filing in the window (cap)": ("ORDER BY filing_epoch DESC,\n                    marketCap",
                                          "ORDER BY filing_epoch ASC,\n                    marketCap"),
    # its other half: only passing rows loaded into the caches
    "positive-only cash-flow fetch": (CF_FETCH, CF_FETCH + "              AND freeCashFlow > 0\n"),
    "non-missing-only cash-flow fetch": (CF_FETCH, CF_FETCH + "              AND freeCashFlow IS NOT NULL\n"),
    "leg-currency-only cash-flow fetch": (CF_FETCH, CF_FETCH + "              AND {ccy_ok_sql}\n"),
    "positive-only cap fetch": (KM_FETCH, KM_FETCH + "              AND marketCap > 0\n"),
    "non-missing-only cap fetch": (KM_FETCH, KM_FETCH + "              AND marketCap IS NOT NULL\n"),
    "quarterly rows fetched (cash flow)": (CF_FETCH, CF_FETCH.replace("period = 'FY'", "period IS NOT NULL")),
    "quarterly rows fetched (cap)": (KM_FETCH, KM_FETCH.replace("period = 'FY'", "period IS NOT NULL")),
    # the tests on the latest filing
    "no FCF > 0 test": (T_FCF, ""),
    "no currency test": (T_CCY, ""),
    "currency flag always true": (CCY_FLAG, "ccy_ok_sql = \"TRUE\""),
    "currency flag never true on a mixed leg": (CCY_FLAG, CCY_FLAG.replace("\"TRUE\"", "\"FALSE\"")),
    "mixed leg needs a stated currency": (CCY_FLAG, CCY_FLAG.replace("\"TRUE\"", "\"reportedCurrency IS NOT NULL\"")),
    "no size floor": (T_FLOOR, "          AND ? IS NOT NULL"),
    "size floor exclusive": (T_FLOOR, T_FLOOR.replace(">=", ">")),
    # the yield band, each end one step either way
    "no yield floor": (T_MIN, "          AND ? IS NOT NULL"),
    "yield floor exclusive": (T_MIN, T_MIN.replace(">=", ">")),
    "yield floor one step lower": (C_MIN, C_MIN + " * (1 - 2 ** -52)"),
    "yield floor one step higher": (C_MIN, C_MIN + " * (1 + 2 ** -52)"),
    "no yield cap": (T_MAX, "          AND ? IS NOT NULL"),
    "yield cap exclusive": (T_MAX, T_MAX.replace("<=", "<")),
    "yield cap one step higher": (C_MAX, "FCF_YIELD_MAX = 0.5000000000000001"),
    "yield cap one step lower": (C_MAX, "FCF_YIELD_MAX = 0.49999999999999994"),
    "yield bounds swapped": (PARAMS, PARAMS.replace("FCF_YIELD_MIN, FCF_YIELD_MAX", "FCF_YIELD_MAX, FCF_YIELD_MIN")),
    "size floor and yield floor swapped": (PARAMS, PARAMS.replace("mktcap_min, FCF_YIELD_MIN", "FCF_YIELD_MIN, mktcap_min")),
    # the filing window, each bound one day either way
    "no staleness bound (cash flow)": (F_WHERE + AGE, F_WHERE + NO_AGE),
    "no staleness bound (cap)": (M_WHERE + AGE, M_WHERE + NO_AGE),
    "filing a day too old allowed": (MAX_AGE, MAX_AGE.replace("540", "541")),
    "filing of the maximum age dropped": (MAX_AGE, MAX_AGE.replace("540", "539")),
    "filing age measured from the cutoff": (MAX_AGE, "earliest_epoch = cutoff_epoch - 540 * 86400"),
    "no lag": (LAG, "cutoff_epoch = utc_epoch(target_date)"),
    "lag a day short": (LAG, LAG.replace("45", "44")),
    "lag a day long": (LAG, LAG.replace("45", "46")),
    "cash-flow bounds swapped": (PARAMS, PARAMS.replace("[cutoff_epoch, earliest_epoch,", "[earliest_epoch, cutoff_epoch,")),
    "cap bounds swapped": (PARAMS, PARAMS.replace(" cutoff_epoch, earliest_epoch,\n", " earliest_epoch, cutoff_epoch,\n")),
    # two rows on one date: each tie-break turned round
    "smaller FCF wins a same-date tie": (F_TIES, F_TIES.replace("freeCashFlow DESC", "freeCashFlow ASC")),
    "other-currency row wins a same-date tie": (F_TIES, F_TIES.replace("ccy_ok DESC", "ccy_ok ASC")),
    "smaller cap wins a same-date tie": (M_TIES, M_TIES.replace("DESC", "ASC")),
    # the campaign wiring around the screen: listing rule, fund filter, dedup before the cut, the cut itself
    "no listing rule": ("    listing_and = f\" AND {listing_cond}\" if listing_cond else \"\"", "    listing_and = \"\""),
    "funds in the universe": ("quality_filters = \"AND isFund = false AND isEtf = false\"", "quality_filters = \"\""),
    "no dedup": ("        " + WRAP, "        FROM ({candidates})\n"),
    "cut before dedup": (WRAP + CUT, WRAP.replace(
        "candidates,", "'SELECT * FROM (' + candidates + ') ORDER BY fcf_yield DESC, symbol LIMIT 50',") + CUT),
    "cut tie broken the other way": (CUT, CUT.replace("symbol", "symbol DESC")),
    "lowest yield first": (CUT, CUT.replace("DESC", "ASC")),
    "one name more": ("MAX_STOCKS = 50", "MAX_STOCKS = 51"),
    "one name fewer": ("MAX_STOCKS = 50", "MAX_STOCKS = 49"),
}


def main():
    base = failures(SRC)
    if base:
        print("test_dcf_selection: FAIL", base)
        sys.exit(1)
    survived = []
    for name, (old, new) in MUTANTS.items():
        if SRC.count(old) != 1:
            print(f"test_dcf_selection: FAIL (mutant anchor not unique: {name})")
            sys.exit(1)
        if not failures(SRC.replace(old, new)):
            survived.append(name)
    if survived:
        print("test_dcf_selection: FAIL (mutants survived:", survived, ")")
        sys.exit(1)
    n = len(STO) + len(OSL) + len(MIXED) + len(XETRA)
    print(f"test_dcf_selection: PASS ({n} companies, {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
