#!/usr/bin/env python3
"""Offline tests for the universe rules ur-1 (ordinary shares only, the dividend-yield rule), defensive-quality.

non_ordinary_sql() on profile rows shaped like the measured cases: a fund flag, the hand list
(SREA, ROG.SW beside RO.SW, CIG beside CIG-C, CHSCL), Korean preferred codes with digits and
letters against Samsung Epis's 0126Z0, German preference lines with an ISIN-stem sibling against
PNE3.DE (PNE's only line), Canadian preferreds against trust units, US hyphen suffixes against
share classes (BF-B, MKC-V, CIG-C), NASDAQ fifth letters against CENTA and GOOGL, name terms
against "China NT Pharma", depositary-share ADRs, "United" and "Service", and a Thai NVDR, which
stays for check N (DECISIONS #064). yield_rule_sql() on FMX-like cross-currency lines, a
same-currency disagreement, the 25% ceiling, the sub-unit currencies, the bounds and the
missing-data policy. cik_shared() groups kept US lines by CIK. Each check also runs on a mutant
and has to fail.

Usage: python scripts/test_universe_rules.py
"""
import os
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import data_utils as du   # noqa: E402


def check(cond, what):
    if not cond:
        raise SystemExit(f"FAIL: {what}")


F = False
# symbol, exchange, companyName, isin, isFund, isEtf, reason wanted (None = an ordinary line, kept)
PROFILE = [
    ("TRIG.L", "LSE", "The Renewables Infrastructure Group Limited", "GG00BBHX2H91", True, F, "fund_flag"),
    ("PGINVIT-IV.NS", "NSE", "POWERGRID Infrastructure Investment Trust", "INE0GGX23010", F, True, "fund_flag"),
    ("SREA", "NYSE", "Sempra", "US8168516040", F, F, "known"),
    ("SRE", "NYSE", "Sempra", "US8168511090", F, F, None),
    ("ROG.SW", "SIX", "Roche Holding AG", "CH0012032048", F, F, "known"),
    ("RO.SW", "SIX", "Roche Holding AG", "CH0012032113", F, F, None),
    ("CIG", "NYSE", "Companhia Energética de Minas Gerais", "US2044096012", F, F, "known"),
    ("CIG-C", "NYSE", "Companhia Energética de Minas Gerais", "US2044098828", F, F, None),
    ("CHSCL", "NASDAQ", "CHS Inc.", "US12542R8034", F, F, "known"),
    ("005935.KS", "KSC", "Samsung Electronics Co., Ltd.", "KR7005931001", F, F, "korean_preferred"),
    ("005930.KS", "KSC", "Samsung Electronics Co., Ltd.", "KR7005930003", F, F, None),
    ("00279K.KS", "KSC", "AMOREPACIFIC Group", "KR700279K010", F, F, "korean_preferred"),
    ("0126Z0.KS", "KSC", "Samsung Epis Holdings", "KR70126Z0002", F, F, None),
    ("HEN3.DE", "XETRA", "Henkel AG & Co. KGaA", "DE0006048432", F, F, "german_preference"),
    ("HEN.DE", "XETRA", "Henkel AG & Co. KGaA", "DE0006048408", F, F, None),
    ("DRW3.DE", "XETRA", "Drägerwerk AG & Co. KGaA", "DE0005550636", F, F, "german_preference"),
    ("DRW8.DE", "XETRA", "Drägerwerk AG & Co. KGaA", "DE0005550602", F, F, None),
    ("PNE3.DE", "XETRA", "PNE AG", "DE000A0JBPG2", F, F, None),
    ("BEP-PM.TO", "TSX", "Brookfield Renewable Partners L.P.", "CA11258F4067", F, F, "canadian_preferred"),
    ("BEI-UN.TO", "TSX", "Boardwalk Real Estate Investment Trust", "CA0966311064", F, F, None),
    ("PCG-PA", "NYSE", "Pacific Gas and Electric Company", "US6943082064", F, F, "us_suffix"),
    ("NUVB-WT", "NYSE", "Nuvation Bio Inc.", "US67080N1191", F, F, "us_suffix"),
    ("PHGE-UN", "AMEX", "BiomX Inc.", "US09090D1037", F, F, "us_suffix"),
    ("CELG-RI", "NYSE", "Bristol-Myers Squibb Company Ce", "US1101221406", F, F, "us_suffix"),
    ("BF-B", "NYSE", "Brown-Forman Corporation", "US1156372096", F, F, None),
    ("MKC-V", "NYSE", "McCormick & Company, Incorporated", "US5797801074", F, F, None),
    ("CHSCP", "NASDAQ", "CHS Inc.", "US12542R2094", F, F, "nasdaq_fifth_letter"),
    ("ACONW", "NASDAQ", "Aclarion, Inc.", "US6551871105", F, F, "nasdaq_fifth_letter"),
    ("ILMNV", "NASDAQ", "Illumina, Inc. Common Stock Ex-distribution When-Issued", None, F, F, "nasdaq_fifth_letter"),
    ("AEPPZ", "NASDAQ", "American Electric Power Company, Inc.", "US02557T3077", F, F, "nasdaq_fifth_letter"),
    ("CENTA", "NASDAQ", "Central Garden & Pet Company", "US1535272058", F, F, None),
    ("GOOGL", "NASDAQ", "Alphabet Inc.", "US02079K3059", F, F, None),
    ("ABCDW", "NYSE", "A Five Letter Line Off NASDAQ", "US0000000001", F, F, None),
    ("CMSD", "NYSE", "CMS Energy Corporation 5.875% Junior Subordinated Notes due 2079", "US125896BX79", F, F, "name_term"),
    ("DUKU", "NYSE", "Duke Energy Corporation Units 1.08.29", "US26441C7091", F, F, "name_term"),
    ("EAI", "NYSE", "Entergy Arkansas, Inc. 1M BD 4.875%66", "US29364D7958", F, F, "name_term"),
    ("SOLN", "NYSE", "Southern Company (The) 2019 Ser", "US8425878266", F, F, "name_term"),
    ("25935.T", "JPX", "Ito En Ltd. Pfd.", "JP3143000010", F, F, "name_term"),
    ("1011.HK", "HKSE", "China NT Pharma Group Company Limited", "KYG2114H1062", F, F, None),
    ("FOCL", "NASDAQ", "EDAP TMS S.A. American Depositary Shares", "US2683111098", F, F, None),
    ("UU.L", "LSE", "United Utilities Group PLC", "GB00B39J2M42", F, F, None),
    ("SCI", "NYSE", "Service Corporation International", "US8175651046", F, F, None),
    ("GSK", "NYSE", "GSK plc", "US37733W2044", F, F, None),
    ("KBANK-R.BK", "SET", "Kasikornbank Public Company Limited", "TH0016010R14", F, F, None),
    ("NUL", "NYSE", None, None, None, None, None),
]


def profile_con():
    con = duckdb.connect()
    con.execute("CREATE TABLE profile(symbol VARCHAR, exchange VARCHAR, companyName VARCHAR, isin VARCHAR,"
                " isFund BOOLEAN, isEtf BOOLEAN)")
    con.executemany("INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?)", [r[:6] for r in PROFILE])
    return con


def ordinary_failures(sql_fn=du.non_ordinary_sql):
    con = profile_con()
    sql = sql_fn("p")
    f = []
    if ";" in sql or "\x08" in sql:
        f.append("non_ordinary_sql carries a ';' or a backspace (a non-raw '\\b')")
    got = dict(con.execute(f"SELECT p.symbol, {sql} FROM profile p").fetchall())
    for s, *_, want in PROFILE:
        if got.get(s) != want:
            f.append(f"{s}: {got.get(s)!r}, want {want!r}")
    # the share-link form: the same CASE pasted as a filter, beside the leg's own unqualified conditions
    kept = [r[0] for r in con.execute(f"SELECT symbol FROM profile p WHERE exchange IN ('XETRA', 'SIX')"
                                      f" AND ({sql}) IS NULL ORDER BY symbol").fetchall()]
    if kept != ["DRW8.DE", "HEN.DE", "PNE3.DE", "RO.SW"]:
        f.append(f"pasted as a filter keeps {kept}")
    return f


def ordinary_mutants():
    base = du.non_ordinary_sql
    return {
        "rule off": lambda p="p", profile="profile": "CAST(NULL AS VARCHAR)",
        "German without the sibling": lambda p="p", profile="profile": base(p, profile).replace(
            "AND EXISTS", "OR EXISTS", 1),
        "hand list empty": lambda p="p", profile="profile": base(p, profile, known=()),
        "Korean digits only": lambda p="p", profile="profile": base(p, profile).replace("[0-9A-Z]{6}", "[0-9]{6}", 1),
    }


# vendor yield, dividends paid, FY market cap, trading currency, reporting currency, reason wanted
YIELD_CASES = [
    (0.30, -3.0e9, 1.0e10, "USD", "USD", "above_ceiling"),
    (0.30, -3.0e9, 1.0e10, "USD", "MXN", "above_ceiling"),
    (0.25, -2.5e9, 1.0e10, "USD", "USD", None),                 # the ceiling is strict
    (0.13, -2.55e8, 1.0e10, "USD", "MXN", "cross_currency"),    # FMX's low end: 5.1 times its own paid yield
    (0.05, -1.0e8, 1.0e10, "USD", "USD", None),                 # same currency: disagreement kept
    (0.05, -1.0e8, 1.0e10, "GBp", "GBP", None),                 # pence quote, pound report
    (0.05, -1.0e8, 1.0e10, "ZAc", "ZAR", None),
    (0.05, -1.0e8, 1.0e10, "ILA", "ILS", None),
    (0.05, -4.4e8, 1.0e10, "HKD", "CNY", None),                 # cross, ratio 1.136: agrees
    (0.0625, -7.8125e8, 1.0e10, "HKD", "CNY", None),            # ratio 0.8 (exact in binary): the bound keeps
    (0.078125, -6.25e8, 1.0e10, "HKD", "CNY", None),            # ratio 1.25 (exact): the bound keeps
    (0.0624, -7.8125e8, 1.0e10, "HKD", "CNY", "cross_currency"),  # just under 0.8
    (0.0782, -6.25e8, 1.0e10, "HKD", "CNY", "cross_currency"),    # just over 1.25
    (0.05, 4.4e8, 1.0e10, "HKD", "CNY", None),                  # a positive paid figure reads as its size
    (0.05, None, 1.0e10, "USD", "MXN", "unverifiable"),         # no same-FY cash-flow row
    (0.05, 0.0, 1.0e10, "USD", "MXN", "unverifiable"),          # zero paid
    (0.05, -5.0e8, None, "USD", "MXN", "unverifiable"),         # no FY cap
    (0.05, -5.0e8, 0.0, "USD", "MXN", "unverifiable"),
    (None, -5.0e8, 1.0e10, "USD", "MXN", None),                 # no vendor yield: nothing to check
    (0.05, None, 1.0e10, None, "MXN", None),                    # trading currency unknown
    (0.05, None, 1.0e10, "USD", None, None),                    # reporting currency unknown
    (0.05, None, 1.0e10, "usd", "USD", None),                   # case
]


def yield_failures(sql_fn=du.yield_rule_sql):
    con = duckdb.connect()
    con.execute("CREATE TABLE t(i INTEGER, vendor DOUBLE, paid DOUBLE, cap DOUBLE, trade VARCHAR, report VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", [(i, *c[:5]) for i, c in enumerate(YIELD_CASES)])
    sql = sql_fn("vendor", "paid", "cap", "trade", "report")
    f = [] if ";" not in sql else ["yield_rule_sql carries a ';'"]
    got = dict(con.execute(f"SELECT i, {sql} FROM t").fetchall())
    for i, c in enumerate(YIELD_CASES):
        if got[i] != c[5]:
            f.append(f"{c[:5]}: {got[i]!r}, want {c[5]!r}")
    if du.paid_yield(-5.2e8, 1.0e10) != 0.052 or du.paid_yield(None, 1e10) is not None or \
            du.paid_yield(-5.2e8, 0.0) is not None:
        f.append("paid_yield")
    return f


def yield_mutants():
    base = du.yield_rule_sql
    return {
        "same-currency lines checked too": lambda *a: base(*a).replace(" = ", " = 'never' AND 'x' = ", 1),
        "sub-units not mapped": lambda *a: base(*a).replace("'ZAc'", "'ZAx'"),
        "missing data kept": lambda *a: base(*a).replace("'unverifiable'", "NULL", 1),
        "ceiling off": lambda *a: base(*a).replace("> 0.25", "> 25.0", 1),
    }


def cik_failures(fn=du.cik_shared):
    rows = [{"symbol": "BF-B", "exchange": "NYSE", "cik": "0000014693", "non_ordinary": None},
            {"symbol": "BF-A", "exchange": "NYSE", "cik": "0000014693", "non_ordinary": None},
            {"symbol": "SRE", "exchange": "NYSE", "cik": "0001032208", "non_ordinary": None},
            {"symbol": "SREA", "exchange": "NYSE", "cik": "0001032208", "non_ordinary": "known"},
            {"symbol": "X.TO", "exchange": "TSX", "cik": "0000000001", "non_ordinary": None},
            {"symbol": "Y.TO", "exchange": "TSX", "cik": "0000000001", "non_ordinary": None},
            {"symbol": "NOC1", "exchange": "NASDAQ", "cik": None, "non_ordinary": None},
            {"symbol": "NOC2", "exchange": "NASDAQ", "cik": None, "non_ordinary": None},
            {"symbol": "BF-B", "exchange": "NYSE", "cik": "0000014693", "non_ordinary": None}]
    got = fn(rows)
    want = [{"cik": "0000014693", "symbols": ["BF-A", "BF-B"]}]
    return [] if got == want else [f"cik_shared {got}, want {want}"]


def main():
    ran = []

    def expect(name, failures):
        check(failures == [], f"{name}: {failures}")
        ran.append(name)

    def expect_fail(name, failures):
        check(failures != [], f"must-fail mutant passed: {name}")
        ran.append(f"mutant {name}")

    expect("ordinary shares: every reason, every kept look-alike", ordinary_failures())
    for label, m in ordinary_mutants().items():
        expect_fail(f"ordinary shares, {label}", ordinary_failures(m))
    expect("yield rule: ceiling, cross-currency agreement, sub-units, bounds, missing data", yield_failures())
    for label, m in yield_mutants().items():
        check(m("v", "p", "c", "t", "r") != du.yield_rule_sql("v", "p", "c", "t", "r"),
              f"yield mutant {label} changed nothing")
        expect_fail(f"yield rule, {label}", yield_failures(m))
    expect("CIK report: kept US lines only", cik_failures())
    expect_fail("CIK report, removed lines counted", cik_failures(
        lambda rows: du.cik_shared([dict(r, non_ordinary=None) for r in rows])))
    check(len({s for s, _, _ in du.NON_ORDINARY_KNOWN}) == len(du.NON_ORDINARY_KNOWN), "hand list has no repeats")
    check(all(k and e for _, k, e in du.NON_ORDINARY_KNOWN), "every hand-list line has a kind and evidence")
    ran.append("hand list shape")
    print("\n".join(f"ok {r}" for r in ran))
    print(f"test_universe_rules: PASS ({len(ran)} checks)")


if __name__ == "__main__":
    main()
