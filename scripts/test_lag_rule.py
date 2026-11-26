#!/usr/bin/env python3
"""Offline check of lag rule lr-1 (data_utils.LagRule): the day each fiscal-year row becomes readable (no network).

On NYSE, NASDAQ and AMEX a row is read from the earliest filing date the vendor holds for it (income_statement's
filingDate, matched on symbol and period end), after its period end and within 180 days of it, and a row with no such
date 105 days after its period end. On a US leg's typed rows, with UINTEGER epochs as the server stores them and the
cache in two tables: a filing on the rebalance date is read that day and one the day after is not; an early filing is
read before day 45 and a late one waits for itself past day 105; a row filed nowhere, with a null date or on its
period end waits 105 days, on either side of the rebalance date; a filing 180 days on is read and one 181 days on is
not; of two filings the earlier wins; a date with a time of day reads as its date; and another year's filing dates
nothing. Both tables carry the same days, the counts are the distinct rows over them and the rows a filing dated, the
leg's markets each show 105, and every query stays under 45,000 characters. Any other exchange waits its market's
days whatever filing date it holds: NSE's 60, with no filing query sent and a null dated count. A US leg whose filing
query returns no bytes dates every row at 105. A leg that mixes markets of different days, a US exchange beside
another, an exchange with no entry (TSXV among them) or no exchange is refused; China's two exchanges share 120 days,
and every exchange lr-1 lists has the days typed here (KLS, ASX and SES 60, JNB 90). Each mutant of data_utils.py
must fail a check on a wrong value; the empty-result read alone fails by an error.

Usage:
    python scripts/test_lag_rule.py [repo root]
"""
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone

import duckdb

ROOT = (os.path.abspath(sys.argv[1]) if len(sys.argv) > 1
        else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DU_PATH = os.path.join(ROOT, "data_utils.py")
sys.path[:0] = [ROOT, os.path.join(ROOT, "scripts")]
import data_utils as du   # noqa: E402
import test_data_checks as tdc   # noqa: E402  (its fake clients and topic_from_text)

R = date(2020, 4, 1)                 # the rebalance date
P = R - timedelta(days=70)           # most rows' period end
DAY = 86400
US = ["NYSE", "NASDAQ", "AMEX"]
MAX_SQL = 45000                      # scripts/split_check.py's bound, under the API's 50,000-character limit


def ep(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def on(n):
    return (P + timedelta(days=n)).isoformat()


# (symbol, period end) of the US leg's FY rows, their filings (symbol, period end, filingDate), and each row's days
# from its period end to its availability day
US_ROWS = [("U_ON", P), ("U_NEXT", P), ("U_EARLY", P), ("U_LATE", P), ("U_ND104", R - timedelta(days=104)),
           ("U_ND105", R - timedelta(days=105)), ("U_NULL", P), ("U_END", P), ("U_180", P), ("U_181", P),
           ("U_TWO", P), ("U_TIME", P), ("U_OTHER", P)]
US_FILED = [("U_ON", P, R.isoformat()), ("U_NEXT", P, (R + timedelta(days=1)).isoformat()), ("U_EARLY", P, on(30)),
            ("U_LATE", P, on(120)), ("U_NULL", P, None), ("U_END", P, on(0)), ("U_180", P, on(180)),
            ("U_181", P, on(181)), ("U_TWO", P, on(80)), ("U_TWO", P, on(50)), ("U_TIME", P, on(50) + " 16:30:00"),
            ("U_OTHER", P - timedelta(days=365), on(-300))]
WANT_US = {"U_ON": 70, "U_NEXT": 71, "U_EARLY": 30, "U_LATE": 120, "U_ND104": 105, "U_ND105": 105, "U_NULL": 105,
           "U_END": 105, "U_180": 180, "U_181": 105, "U_TWO": 50, "U_TIME": 50, "U_OTHER": 105}
US_COUNTS = {"rows": len(US_ROWS), "us_filing_dated": 7}   # U_ON, U_NEXT, U_EARLY, U_LATE, U_180, U_TWO, U_TIME
US_BLOCK = {"exchanges": US, "filing_date": "income_statement.filingDate", "window_days": 180, "no_date_days": 105}
NSE_ROWS = [("N_FILED", R - timedelta(days=50)), ("N59", R - timedelta(days=59)), ("N60", R - timedelta(days=60))]
NSE_FILED = [("N_FILED", R - timedelta(days=50), (R - timedelta(days=30)).isoformat())]
WANT_NSE = {"N_FILED": 60, "N59": 60, "N60": 60}
REFUSED = (["NYSE", "LSE"], ["OTC"], ["TSXV"], ["NSE", "LSE"], [])
# lr-1's days per exchange outside the US
WANT_DAYS = {**{e: 120 for e in ("LSE", "XETRA", "STO", "OSL", "PAR", "MIL", "BME", "AMS", "WSE", "CPH", "HEL", "BRU",
                                 "LIS", "VIE", "SIX", "SHZ", "SHH")},
             **{e: 90 for e in ("HKSE", "KSC", "KOE", "TAI", "TWO", "TSX", "SAO", "SET", "SAU", "JKT", "JPX")},
             "NSE": 60, "BSE": 60, "KLS": 60, "ASX": 60, "JNB": 90, "SES": 60}
RULE_NUMBERS = {"KLS": "9.22(1)", "ASX": "4.3B", "JNB": "6.43", "SES": "705(1)"}   # in each looked-up source


def warehouse(rows, filings, ex):
    """key_metrics FY rows and a profile for each (symbol, period end) on exchange ex, and the income_statement
    filings, its date and filingDate VARCHAR as the warehouse holds them."""
    src = duckdb.connect()
    src.execute("CREATE TABLE key_metrics(symbol VARCHAR, marketCap DOUBLE, dateEpoch UINTEGER, period VARCHAR)")
    src.execute("CREATE TABLE income_statement(symbol VARCHAR, period VARCHAR, date VARCHAR, dateEpoch UINTEGER,"
                " filingDate VARCHAR)")
    src.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR)")
    for sym, p in rows:
        src.execute("INSERT INTO key_metrics VALUES (?, 1e10, ?, 'FY')", [sym, ep(p)])
        src.execute("INSERT INTO profile VALUES (?, ?)", [sym, ex])
    for sym, p, filed in filings:
        src.execute("INSERT INTO income_statement VALUES (?, 'FY', ?, ?, ?)", [sym, p.isoformat(), ep(p), filed])
    return src


def dated(m, rows, filings, ex, client=tdc.FakeClient):
    """Module m's LagRule on a leg, its FY rows cached in two tables as a topic's fetch caches them: ({table: {symbol:
    days from the period end to the availability day}}, the block, the queries sent)."""
    c = client(warehouse(rows, filings, ex[0]))
    con = duckdb.connect()
    flt = f"symbol IN (SELECT DISTINCT symbol FROM profile WHERE exchange IN ({', '.join(repr(e) for e in ex)}))"
    tables = ("metrics_cache", "ratios_cache")
    for t in tables:
        du.query_parquet(c, f"SELECT symbol, marketCap, dateEpoch AS filing_epoch FROM key_metrics WHERE {flt}", con, t)
    lag = m.LagRule()
    lag.load(c, con, ex, list(tables), flt)
    got = {t: {s: (a - f) // DAY for s, a, f in con.execute(f"SELECT symbol, avail_epoch, filing_epoch FROM {t}").fetchall()}
           for t in tables}
    return got, lag.block(), c.sqls


def leg_failures(label, got, block, want, counts, markets):
    out = [f"{label}: {t} days {d}, want {want}" for t, d in got.items() if d != want]
    if block["counts"] != counts:
        out.append(f"{label}: counts {block['counts']}, want {counts}")
    if {e: v["days"] for e, v in block["markets"].items()} != markets or list(block["markets"]) != list(markets):
        out.append(f"{label}: markets {block['markets']}, want the days {markets} in leg order")
    return out


def unit_failures(m):
    """Every check above on module m (data_utils or a mutant of it)."""
    out = []
    try:
        got, b, sqls = dated(m, US_ROWS, US_FILED, US)
        out += leg_failures("US", got, b, WANT_US, US_COUNTS, {e: 105 for e in US})
        if list(b) != ["version", "rule", "us", "markets", "counts"] or b["version"] != "lr-1" or b["us"] != US_BLOCK:
            out.append(f"US block {list(b)}, version {b['version']}, us {b['us']}")
        if max(map(len, sqls)) >= MAX_SQL:
            out.append(f"a query of {max(map(len, sqls))} characters")
        got, b, sqls = dated(m, NSE_ROWS, NSE_FILED, ["NSE"])
        out += leg_failures("NSE", got, b, WANT_NSE, {"rows": len(NSE_ROWS), "us_filing_dated": None}, {"NSE": 60})
        if any("income_statement" in q for q in sqls):
            out.append("a non-US leg reads filing dates")
        got, b, _ = dated(m, [("U_ND", P)], [], US, client=tdc.EmptyBytesClient)
        out += leg_failures("US, no filing", got, b, {"U_ND": 105}, {"rows": 1, "us_filing_dated": 0},
                            {e: 105 for e in US})
        for ex in REFUSED:
            try:
                out.append(f"{ex} not refused: {m.LagRule().days(ex)} days")
            except ValueError:
                pass
        if m.LagRule().days(["SHZ", "SHH"]) != 120:
            out.append(f"China {m.LagRule().days(['SHZ', 'SHH'])} days, want 120")
        markets = m.LAG_RULES_V1["markets"]
        if {e: v["days"] for e, v in markets.items()} != WANT_DAYS:
            out.append(f"lr-1's days {sorted((e, v['days']) for e, v in markets.items())}, want {sorted(WANT_DAYS.items())}")
        out += [f"{e}'s source {markets[e]['source']!r} names no {n}" for e, n in RULE_NUMBERS.items()
                if n not in markets[e]["source"]]
    except Exception as e:   # reported as an error: main() requires most mutants to fail on a value
        out.append(f"error: {type(e).__name__}: {e}")
    return out


MUTANTS = {   # text replacements on data_utils.py
    "undated US rows at 104 days": ('"no_date_days": 105', '"no_date_days": 104'),
    "undated US rows at 106 days": ('"no_date_days": 105', '"no_date_days": 106'),
    "undated US rows at 45 days": ('"no_date_days": 105', '"no_date_days": 45'),
    "filing window 179 days": ('"window_days": 180', '"window_days": 179'),
    "filing window 181 days": ('"window_days": 180', '"window_days": 181'),
    "a filing on the period end counts": ("AS DATE) > TRY_CAST(date AS DATE)", "AS DATE) >= TRY_CAST(date AS DATE)"),
    "the latest filing date": ("MIN(TRY_CAST(filingDate AS DATE))", "MAX(TRY_CAST(filingDate AS DATE))"),
    "read the day after the filing": ("CAST(filed AS TIMESTAMP)) FROM", "CAST(filed AS TIMESTAMP)) + 86400 FROM"),
    "dated by another year's filing": (' AND f.period_epoch = {t}.filing_epoch")', '")'),
    "a late filing capped at 105 days": ("SET avail_epoch = f.filed_epoch FROM",
                                         "SET avail_epoch = LEAST(f.filed_epoch, avail_epoch) FROM"),
    "an early filing held to 45 days": ("SET avail_epoch = f.filed_epoch FROM",
                                        "SET avail_epoch = GREATEST(f.filed_epoch, CAST(filing_epoch AS BIGINT)"
                                        " + 45 * 86400) FROM"),
    "filing dates on every leg": ("        if self.us:\n", "        if True:\n"),
    "India at 59 days": ('"NSE": (60, _INDIA)', '"NSE": (59, _INDIA)'),
    "India at 61 days": ('"NSE": (60, _INDIA)', '"NSE": (61, _INDIA)'),
    "Malaysia at 59 days": ('"KLS": (60, _KLS)', '"KLS": (59, _KLS)'),
    "Australia at 62 days": ('"ASX": (60, _ASX)', '"ASX": (62, _ASX)'),
    "South Africa at 120 days": ('"JNB": (90, _JNB)', '"JNB": (120, _JNB)'),
    "Singapore at 90 days": ('"SES": (60, _SES)', '"SES": (90, _SES)'),
    "TSXV at three months": ('"TSX", "SAO"', '"TSX", "TSXV", "SAO"'),
    "an unknown exchange passes": ("if None in days or len(days) != 1:", "if len(days) != 1:"),
    "two lags in one leg pass": ("if None in days or len(days) != 1:", "if None in days or len(days) < 1:"),
    "every row counted as dated": ("count(f.filed_epoch)", "count(*)"),
    "a non-US leg's count is 0": ("self.us_dated = dated if self.us else None", "self.us_dated = dated"),
    "rows counted per table": (' " UNION ".join(', ' " UNION ALL ".join('),
    "the empty result read": ("            if n:\n", "            if True:\n"),
}
BY_ERROR = {"the empty result read"}   # an empty result has no columns: reading it raises


def main():
    with open(DU_PATH) as fh:
        src = fh.read()
    base = unit_failures(du)
    if base:
        print("test_lag_rule: FAIL", base)
        sys.exit(1)
    survived, errored = [], []
    with tempfile.TemporaryDirectory() as tmp:
        for i, (name, (old, new)) in enumerate(MUTANTS.items()):
            if src.count(old) != 1:
                print(f"test_lag_rule: FAIL (mutant anchor found {src.count(old)} times: {name})")
                sys.exit(1)
            try:
                got = unit_failures(tdc.topic_from_text(src.replace(old, new), tmp, f"lag_mutant_{i}"))
            except Exception as e:   # a mutant that breaks the module's import
                got = [f"error: {type(e).__name__}: {e}"]
            if not got:
                survived.append(name)
            elif name not in BY_ERROR and all(f.startswith("error:") for f in got):
                errored.append(name)
    if survived or errored:
        print("test_lag_rule: FAIL (mutants survived:", survived, "; caught only by an error:", errored, ")")
        sys.exit(1)
    print(f"test_lag_rule: PASS ({len(US_ROWS)} US rows, {len(NSE_ROWS)} NSE rows, an empty filing result,"
          f" {len(REFUSED)} refused legs, {len(WANT_DAYS)} markets' days; {len(MUTANTS)} mutants caught)")


if __name__ == "__main__":
    main()
