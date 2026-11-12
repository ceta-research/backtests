#!/usr/bin/env python3
"""Offline tests for the universe rules ur-1 (ordinary shares only, the dividend-yield rule), defensive-quality.

non_ordinary_sql() on profile rows shaped like the measured cases: a fund flag, the hand list
(SREA, ROG.SW beside RO.SW, CIG beside CIG-C, CHSCL), Korean preferred codes with digits and
letters against Samsung Epis's 0126Z0, German preference lines with an ISIN-stem sibling against
PNE3.DE (PNE's only line), Canadian preferreds against trust units, US hyphen suffixes against
share classes (BF-B, MKC-V, CIG-C), NASDAQ fifth letters against CENTA and GOOGL, name terms
against "China NT Pharma", depositary-share ADRs, "United" and "Service", and a Thai NVDR, which
stays for check N. yield_rule_sql() on FMX-like cross-currency lines, a
same-currency disagreement, the 25% ceiling, the sub-unit currencies, the bounds and the
missing-data policy. cik_shared() groups kept US lines by CIK. Each check also runs on a mutant
and has to fail.

ur-1 is frozen: with its defaults non_ordinary_sql() gives FROZEN's text, ORDINARY_RULES equals the
rules block of every committed defensive-quality results file, and UniverseRules().block() writes
FROZEN's JSON on an exercised run. ur-2 (cfg=UNIVERSE_RULES_V2) on PROFILE_V2: the keep list ahead of
every rule (JUP.L's fund flag, bond, note and REIT names, SALE and PTRN's series, BCE-PY.TO), the
longer hand list, Brazil's ISIN class with the ticker digit when there is no ISIN, ADRs and trusts and
LPs on US exchanges only, and ser only at a name's end. Every row also carries its ur-1 reason, so
ur-2 is opt-in. Each new reason has a mutant.

ur-2 is frozen at FROZEN_V2 as ur-1 is at FROZEN: its config JSON, its SQL, an exercised block, and the
rules block of every committed asset-growth results file. ur-3 (cfg=UNIVERSE_RULES_V3) is ur-2 plus the
lines PROFILE_V3 holds (real profile rows: each new line reads known under ur-3 and None under ur-1 and
ur-2, the lines beside them stay) and the new line starts, each out of a screen before its first own day.
started_sql (the line-start filter) and RowGuard (a screen guard's record) are checked here too, with
asset growth running data_utils' copies.

Usage: python scripts/test_universe_rules.py
"""
import glob
import importlib.util
import json
import os
import re
import sys
import tempfile
from datetime import date, timedelta
from types import SimpleNamespace

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import data_utils as du   # noqa: E402
from test_data_checks import load_base   # noqa: E402

FROZEN = "9b72870"   # the commit defensive-quality's published ur-1 results ran on
FROZEN_V2 = "2a741b7"   # ur-2 as asset-growth's committed results ran it


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
                " isFund BOOLEAN, isEtf BOOLEAN, isAdr BOOLEAN)")
    con.executemany("INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?, NULL)", [r[:6] for r in PROFILE])
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


# ---------------------------------------------------------------- ur-1 frozen

def ur_run(mod):
    """mod.UniverseRules() after one sector query (PROFILE, BF-A beside BF-B) and one screen's yield drops,
    as the JSON text a results file would hold."""
    ur = mod.UniverseRules()
    lines = PROFILE + [("BF-A", "NYSE", "Brown-Forman Corporation", None, F, F, None)]
    ur.sector_rows([{"symbol": s, "exchange": ex, "cik": "0000014693" if s.startswith("BF-") else None,
                     "companyName": n, "non_ordinary": want} for s, ex, n, *_, want in lines])
    ur.yield_drops([("FMX", "cross_currency", 1719792000, 0.13, -2.55e8, 1.0e10, "USD", "MXN"),
                    ("AAA", "above_ceiling", 1719792000, 0.30, -3.0e9, 1.0e10, "USD", "USD")], date(2025, 7, 1))
    return json.dumps(ur.block())


def freeze_failures(frozen, mod=du):
    """mod's ur-1 against FROZEN's data_utils and defensive-quality's committed results."""
    f = []
    for a, kw in (((), {}), (("q",), {}), (("p", "prof"), {}), ((), {"known": ()})):
        if mod.non_ordinary_sql(*a, **kw) != frozen.non_ordinary_sql(*a, **kw):
            f.append(f"non_ordinary_sql{a} {kw} differs from {FROZEN}'s")
    files = sorted(glob.glob(os.path.join(ROOT, "defensive-quality", "results", "returns_*.json")))
    rules = json.loads(json.dumps(mod.ORDINARY_RULES))
    for path in files:
        with open(path) as fh:
            stored = json.load(fh)["universe_rules"]
        if stored["version"] != "ur-1" or stored["ordinary_shares"]["rules"] != rules:
            f.append(f"ORDINARY_RULES differs from {os.path.basename(path)}'s rules block")
    if not files:
        f.append("no committed defensive-quality results file")
    empty = mod.UniverseRules().block()
    if list(empty) != ["version", "ordinary_shares", "dividend_yield"] or empty["version"] != "ur-1":
        f.append(f"UniverseRules().block() keys {list(empty)}, version {empty.get('version')}")
    if ur_run(mod) != ur_run(frozen):
        f.append(f"UniverseRules().block() JSON differs from {FROZEN}'s")
    v1 = mod.UNIVERSE_RULES_V1
    if not (v1["ordinary"] is mod.ORDINARY_RULES and v1["known"] is mod.NON_ORDINARY_KNOWN
            and v1["name_terms"] is mod.NAME_TERMS and v1["keep"] == () and v1["v2"] is False
            and v1["dividend_yield"] is True and v1["version"] == "ur-1"):
        f.append("UNIVERSE_RULES_V1 is not ur-1's objects")
    return f


def freeze_mutant():
    """du with one line appended to ur-1's hand list (its SQL, its rules text and its block)."""
    extra = ("XMUT", "notes", "a line the frozen hand list never held")
    known = du.NON_ORDINARY_KNOWN + (extra,)
    entry = dict(zip(("symbol", "kind", "evidence"), extra))
    rules = dict(du.ORDINARY_RULES, known=du.ORDINARY_RULES["known"] + [entry])
    cfg = dict(du.UNIVERSE_RULES_V1, ordinary=rules, known=known)
    return SimpleNamespace(
        non_ordinary_sql=lambda p="p", profile="profile", known=None: du.non_ordinary_sql(p, profile, known, cfg=cfg),
        ORDINARY_RULES=rules, NON_ORDINARY_KNOWN=known, NAME_TERMS=du.NAME_TERMS, UNIVERSE_RULES_V1=cfg,
        UniverseRules=lambda: du.UniverseRules(cfg))


# ---------------------------------------------------------------- ur-2

T = True
# symbol, exchange, companyName, isin, cik, isFund, isEtf, isAdr, ur-2 reason wanted, ur-1 reason wanted
PROFILE_V2 = [
    ("SJT", "NYSE", "San Juan Basin Royalty Trust", "US7982411057", "0000319655", F, F, F, "us_trust_lp", None),
    ("DMLP", "NASDAQ", "Dorchester Minerals, L.P.", "US25820R1059", "0001172358", F, F, F, "us_trust_lp", None),
    ("EPD", "NYSE", "Enterprise Products Partners L.P.", "US2937921078", "0001061219", F, F, F, "us_trust_lp", None),
    ("MPLX", "NYSE", "MPLX Lp", "US55336V1008", "0001552000", F, F, F, "us_trust_lp", None),
    ("MARPS", "NASDAQ", "Marine Petroleum Trust", "US5684231070", "0000062362", F, F, F, "us_trust_lp", None),
    ("APLP", "NASDAQ", "Archrock Partners, L.P. repres", None, "0001367064", F, F, F, "us_trust_lp", None),
    ("NEN", "AMEX", "New England Realty Associates Limited Partnership", "US6442061049", "0000746514", F, F, F,
     "us_trust_lp", None),
    ("MSB", "NYSE", "Mesabi Trust", "US5906721015", "0000065172", F, F, F, "us_trust_lp", None),
    ("FISK", "AMEX", "Empire State Realty OP, L.P.", "US2921023089", "0001553079", F, F, F, "us_trust_lp", None),
    ("RPRX", "NASDAQ", "Royalty Pharma plc", "GB00BMVP7Y09", "0001802768", F, F, F, None, None),
    ("GROY", "AMEX", "Gold Royalty Corp.", "CA38071H1064", "0001834026", F, F, F, None, None),
    ("NTRS", "NASDAQ", "Northern Trust Corporation", "US6658591044", "0000073124", F, F, F, None, None),
    ("FRT", "NYSE", "Federal Realty Investment Trust", "US3137451015", "0000034903", F, F, F, None, None),
    ("XYZP", "NASDAQ", "XYZ Partners Acquisition Corp", "US0000000002", None, F, F, F, None, None),
    ("CORR", "NYSE", "CorEnergy Infrastructure Trust, Inc.", "US21870U5020", "0001347652", F, F, F, None, None),
    ("BEP-UN.TO", "TSX", "Brookfield Renewable Partners L.P.", "BMG162581083", None, F, F, F, None, None),
    ("BEI-UN.TO", "TSX", "Boardwalk Real Estate Investment Trust", "CA0966311064", None, F, F, F, None, None),
    ("NVS", "NYSE", "Novartis AG", "US66987V1098", None, F, F, T, "us_adr", None),
    ("PBR", "NYSE", "Petróleo Brasileiro S.A. - Petrobras", "US71654V4086", "0001119639", F, F, T, "us_adr", None),
    ("BATS.L", "LSE", "British American Tobacco p.l.c.", "GB0002875804", None, F, F, T, None, None),
    ("LIN", "NYSE", "Linde plc", "IE000S9YS762", None, F, F, F, None, None),
    ("PETR4.SA", "SAO", "Petróleo Brasileiro S.A. - Petrobras", "BRPETRACNPR6", None, F, F, F, None, None),
    ("KLBN11.SA", "SAO", "Klabin S.A.", "BRKLBNCDAM18", None, F, F, F, None, None),
    ("KLBN3.SA", "SAO", "Klabin S.A.", "brklbnacnor1", None, F, F, F, None, None),
    ("GGBR4.SA", "SAO", "Gerdau S.A.", None, None, F, F, F, None, None),
    ("GGBR3.SA", "SAO", "Gerdau S.A.", None, None, F, F, F, None, None),
    ("CMIG4.SA", "SAO", "Companhia Energética de Minas Gerais", "BRCMIGACNPR3", None, F, F, F, None, None),
    ("CMIG3.SA", "SAO", "Companhia Energética de Minas Gerais", None, None, F, F, F, None, None),
    ("AXIA6.SA", "SAO", "Centrais Elétricas Brasileiras S.A. - Eletrobras", "BRELETACNPB7", None, F, F, F, None, None),
    ("ELET3.SA", "SAO", "Centrais Elétricas Brasileiras S.A. - Eletrobras", "BRELETACNOR6", None, F, F, F, None, None),
    ("ABCD3.SA", "SAO", "Abcd Industria S.A.", " ", None, F, F, F, None, None),
    ("TAEE11.SA", "SAO", "Transmissora Aliança de Energia Elétrica S.A.", "BRTAEECDAM10", None, F, F, F,
     "brazil_class", None),
    ("XPML11.SA", "SAO", "XP Malls Fundo de Investimento Imobiliario", "BRXPMLCTF000", None, F, F, F,
     "brazil_class", None),
    ("JBSS32.SA", "SAO", "Jbs N.V.", "BRJBSSBDR002", None, F, F, F, "brazil_class", None),
    ("TAEE11.SA", "SAO", "Transmissora Aliança de Energia Elétrica S.A.", None, None, F, F, F, "brazil_class", None),
    ("COCE5.SA", "SAO", "Companhia Energética do Ceará - Coelce", None, None, F, F, F, "brazil_class", None),
    ("X3YZ4.SA", "SAO", "X3YZ Participacoes S.A.", None, None, F, F, F, "brazil_class", None),
    ("AURA33.SA", "SAO", "Aura Minerals Inc.", None, None, F, F, F, "brazil_class", None),
    ("ABCD4.SA", "SAO", "Abcd Industria S.A.", " ", None, F, F, F, None, None),
    ("FESA4.SA", "SAO", "Cia de Ferro Ligas da Bahia-Ferbasa Pfd", "BRFESAACNPR5", None, F, F, F, "brazil_class",
     "name_term"),
    ("PETR3.SA", "SAO", "Petróleo Brasileiro S.A. - Petrobras", "BRPETRACNOR9", None, F, F, F, None, None),
    ("JBSS3.SA", "SAO", "JBS S.A.", "BRJBSSACNOR8", None, F, F, F, None, None),
    ("B3SA3.SA", "SAO", "B3 S.A. - Brasil, Bolsa, Balcão", None, None, F, F, F, None, None),
    ("VALE3.SA", "SAO", "Vale S.A.", "", None, F, F, F, None, None),
    ("SEER3.SA", "SAO", "Ser Educacional S.A.", "BRSEERACNOR5", None, F, F, F, None, "name_term"),
    ("KARNEL-B.ST", "STO", "Karnell Group AB ser. B", "SE0017832173", None, F, F, F, None, "name_term"),
    ("SOLN", "NYSE", "Southern Company (The) 2019 Ser", "US8425876021", "0000092122", F, F, F, "name_term",
     "name_term"),
    ("4286.HK", "HKSE", "Government of Hong Kong Special Administrative Region 3.5% 17-DEC-2027", "HK0001086013",
     None, F, F, F, "name_term", "name_term"),
    ("JUP.L", "LSE", "Jupiter Fund Management Plc", "GB00B53P2009", None, T, F, F, None, "fund_flag"),
    ("1414.T", "JPX", "SHO-BOND Holdings Co.,Ltd.", "JP3360250009", None, F, F, F, None, "name_term"),
    ("4764.TW", "TAI", "Double Bond Chemical Ind. Co., Ltd.", "TW0004764006", None, F, F, F, None, "name_term"),
    ("ABE.AX", "ASX", "Australian Bond Exchange Holdings Limited", "AU0000187577", None, F, F, F, None, "name_term"),
    ("5243.T", "JPX", "Note Inc.", "JP3759300001", None, F, F, F, None, "name_term"),
    ("NOTE.ST", "STO", "NOTE AB (publ)", "SE0001161654", None, F, F, F, None, "name_term"),
    ("SALE", "NASDAQ", "RetailMeNot, Inc. - Series 1", None, "0001475274", F, F, F, None, "name_term"),
    ("PTRN", "NASDAQ", "Pattern Group Inc. Series A Common Stock", "US70339W1045", "0001811935", F, F, F, None,
     "name_term"),
    ("MINDSPACE.NS", "NSE", "Mindspace Business Parks REIT Unit", "INE0CCU25019", None, F, F, F, None, "name_term"),
    ("348950.KS", "KSC", "JR Global REIT", "KR7348950007", None, F, F, F, None, "name_term"),
    ("BCE-PY.TO", "TSX", "BCE Inc.", "CA05534B8511", None, F, F, F, None, "canadian_preferred"),
    ("BCE-PA.TO", "TSX", "BCE Inc.", "CA05534B7943", None, F, F, F, "canadian_preferred", "canadian_preferred"),
    ("BCE.TO", "TSX", "BCE Inc.", "CA05534B7604", None, F, F, F, None, None),
    ("AFGB", "NYSE", "American Financial Group, Inc.", "US0259328070", "0001042046", F, F, F, "known", None),
    ("AFGC", "NYSE", "American Financial Group, Inc.", "US0259328807", "0001042046", F, F, F, "known", None),
    ("AFGD", "NYSE", "American Financial Group, Inc.", "US0259328724", "0001042046", F, F, F, "known", None),
    ("AFGE", "NYSE", "American Financial Group, Inc.", "US0259328641", "0001042046", F, F, F, "known", None),
    ("AFG", "NYSE", "American Financial Group, Inc.", "US0259321042", "0001042046", F, F, F, None, None),
    ("MGR", "NYSE", "Affiliated Managers Group, Inc.", "US0082528508", "0001004434", F, F, F, "known", None),
    ("MGRB", "NYSE", "Affiliated Managers Group, Inc.", "US0082528433", "0001004434", F, F, F, "known", None),
    ("MGRD", "NYSE", "Affiliated Managers Group, Inc.", "US0082528359", "0001004434", F, F, F, "known", None),
    ("SCHP.SW", "SIX", "Schindler Holding AG", "CH0024638196", None, F, F, F, "known", None),
    ("SCHN.SW", "SIX", "Schindler Holding AG", "CH0024638212", None, F, F, F, None, None),
    ("PBR-A", "NYSE", "Petróleo Brasileiro S.A. - Petrobras", "US71654V1017", "0001119639", F, F, T, "known", None),
    ("AZN", "NYSE", "AstraZeneca PLC", "GB0009895292", None, F, F, F, "known", None),
    ("RDS-B", "NYSE", "Shell plc", "US7802591070", None, F, F, F, "known", None),
    ("WIMI", "NASDAQ", "WiMi Hologram Cloud Inc.", "KYG9687V2040", None, F, F, F, "known", None),
    ("JHX", "NYSE", "James Hardie Industries plc", "IE000R94NGM2", None, F, F, F, "known", None),
    ("MAN3.DE", "XETRA", "MAN SE", None, None, F, F, F, "known", None),
    ("MAN.DE", "XETRA", "MAN SE", "DE0005937007", None, F, F, F, None, None),
    ("BASA.DE", "XETRA", "BASF Se", None, None, F, F, F, "known", None),
    ("BAS.DE", "XETRA", "BASF Se", "DE000BASF111", None, F, F, F, None, None),
    ("4332.HK", "HKSE", "Amgen Inc.", "US0311621009", None, F, F, F, "known", None),
    ("4333.HK", "HKSE", "Cisco Systems, Inc.", "US17275R1023", None, F, F, F, "known", None),
    ("4335.HK", "HKSE", "Intel Corporation", "US4581401001", None, F, F, F, "known", None),
    ("4336.HK", "HKSE", "Applied Materials, Inc.", "US0382221051", None, F, F, F, "known", None),
    ("4337.HK", "HKSE", "Starbucks-t", "US8552441094", None, F, F, F, "known", None),
    ("4338.HK", "HKSE", "Microsoft Corporation", "US5949181045", None, F, F, F, "known", None),
    ("029960.KS", "KSC", "Korea Environment Technology Co Ltd", None, None, F, F, F, "known", None),
    ("049070.KS", "KSC", "Intops Co Ltd", None, None, F, F, F, "known", None),
    ("064760.KS", "KSC", "Tokai Carbon Korea Co Ltd", None, None, F, F, F, "known", None),
    ("064760.KQ", "KOE", "Tokai Carbon Korea Co., Ltd.", "KR7064760002", None, F, F, F, None, None),
    ("069080.KS", "KSC", "Webzen Inc", None, None, F, F, F, "known", None),
    ("265520.KS", "KSC", "Advanced Process Systems Corporation", "KR7265520007", None, F, F, F, "known", None),
    ("GUD.AX", "ASX", "Amotiv Ltd.", "AU000000GUD2", None, F, F, F, "known", None),
    ("AOV.AX", "ASX", "Amotiv Limited", "AU0000340770", None, F, F, F, None, None),
]
NEW_KNOWN = {**dict.fromkeys(("AFGB", "AFGC", "AFGD", "AFGE", "MGR", "MGRB", "MGRD"), "notes"),
             "SCHP.SW": "certificate", "PBR-A": "preferred ADR", "MAN3.DE": "preferred", "BASA.DE": "second line",
             **dict.fromkeys(("AZN", "RDS-B", "WIMI", "JHX"), "ADR"),
             **dict.fromkeys(("4332.HK", "4333.HK", "4335.HK", "4336.HK", "4337.HK", "4338.HK"), "pilot line"),
             **dict.fromkeys(("029960.KS", "049070.KS", "064760.KS", "069080.KS", "265520.KS"), "kosdaq duplicate"),
             "GUD.AX": "renamed line"}
KEEP = {"JUP.L", "1414.T", "4764.TW", "ABE.AX", "5243.T", "NOTE.ST", "SALE", "PTRN", "MINDSPACE.NS", "348950.KS",
        "BCE-PY.TO"}
V2_ORDER = ["fund_flag", "known", "korean_preferred", "german_preference", "canadian_preferred", "us_suffix",
            "nasdaq_fifth_letter", "brazil_class", "us_adr", "us_trust_lp", "name_term"]


def v2_sql(p="p", profile="profile", **kw):
    return du.non_ordinary_sql(p, profile, cfg=du.UNIVERSE_RULES_V2, **kw)


def v3_sql(p="p", profile="profile", **kw):
    return du.non_ordinary_sql(p, profile, cfg=du.UNIVERSE_RULES_V3, **kw)


def v2_rows():
    """PROFILE_V2, then every ur-1 case (no CIK, isAdr NULL) wanting its ur-1 reason under both versions."""
    return PROFILE_V2 + [(s, ex, n, isin, None, fu, et, None, w, w) for s, ex, n, isin, fu, et, w in PROFILE]


def profile_v2_con(rows=None):
    con = duckdb.connect()
    con.execute("CREATE TABLE profile(i INTEGER, symbol VARCHAR, exchange VARCHAR, companyName VARCHAR, isin VARCHAR,"
                " cik VARCHAR, isFund BOOLEAN, isEtf BOOLEAN, isAdr BOOLEAN)")
    con.executemany("INSERT INTO profile VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [(i, *r[:8]) for i, r in enumerate(v2_rows() if rows is None else rows)])
    return con


def v2_failures(sql_fn=v2_sql):
    """sql_fn's reasons on v2_rows (ur-2 column) and ur-1's on the same rows (ur-1 column)."""
    con, rows = profile_v2_con(), v2_rows()
    sql = sql_fn("p")
    f = [] if ";" not in sql and "\x08" not in sql else ["ur-2 SQL carries a ';' or a backspace"]
    for label, fn, col in (("ur-2", sql_fn, 8), ("ur-1", du.non_ordinary_sql, 9)):
        got = dict(con.execute(f"SELECT p.i, {fn('p')} FROM profile p").fetchall())
        f += [f"{label} {r[0]} ({r[3]!r}): {got[i]!r}, want {r[col]!r}" for i, r in enumerate(rows) if got[i] != r[col]]
    return f


def v2_mutants():
    us = du._sql_list(du.US_EXCHANGES)
    trust = f"regexp_matches(COALESCE(p.companyName, ''), '{du.US_TRUST_LP_TERMS}'"
    return {
        "keep list off": lambda p="p", profile="profile": du.non_ordinary_sql(
            p, profile, cfg=dict(du.UNIVERSE_RULES_V2, keep=())),
        "hand list empty": lambda p="p", profile="profile": v2_sql(p, profile, known=()),
        "Brazil class inverted": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "SUBSTR(UPPER(TRIM(p.isin)), 9, 2) = 'NO'", "SUBSTR(UPPER(TRIM(p.isin)), 9, 2) <> 'NO'", 1),
        "Brazil ticker route off": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "'[A-Z0-9]{4}3[A-Z]?[.]SA'", "'.*'", 1),
        "Brazil blank ISIN not routed": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "NULLIF(TRIM(p.isin), '') IS NOT NULL", "p.isin IS NOT NULL"),
        "Brazil no-ordinary test off": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            " OR NOT EXISTS (SELECT 1 FROM profile _b", " OR FALSE AND NOT EXISTS (SELECT 1 FROM profile _b", 1),
        "Brazil BDRs as stand-ins": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "IN ('R0', 'F0')", "IN ('F0')", 1),
        "Brazil sibling ISIN case-sensitive": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "SUBSTR(UPPER(TRIM(_b.isin)), 9, 2) = 'NO'", "SUBSTR(TRIM(_b.isin), 9, 2) = 'NO'", 1),
        "Brazil issuer by ticker only": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "(CASE WHEN NULLIF(TRIM(p.isin), '') IS NOT NULL THEN SUBSTR(UPPER(TRIM(p.isin)), 3, 4)"
            " ELSE SUBSTR(p.symbol, 1, 4) END)", "SUBSTR(p.symbol, 1, 4)", 1),
        "ADR rule off": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "COALESCE(p.isAdr, FALSE)", "FALSE", 1),
        "ADR rule on every exchange": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            f"p.exchange IN ({us}) AND COALESCE(p.isAdr", "COALESCE(p.isAdr", 1),
        "trust/LP rule off": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            "THEN 'us_trust_lp'", "THEN NULL", 1),
        "trust/LP on every exchange": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            f"p.exchange IN ({us}) AND {trust}", trust, 1),
        "trust/LP case-sensitive": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            f"{trust}, 'i')", f"{trust})", 1),
        "ser anywhere in a name (ur-1's terms)": lambda p="p", profile="profile": v2_sql(p, profile).replace(
            f"'{du.NAME_TERMS_V2}'", f"'{du.NAME_TERMS}'", 1),
    }


def reason_failures():
    """Every reason each CASE can emit, in CASE order, is its rules' order; ur-2's and ur-3's order is V2_ORDER
    and their keep list is their first WHEN."""
    f = []
    for name, sql, rules in (("ur-1", du.non_ordinary_sql("p"), du.ORDINARY_RULES),
                             ("ur-2", v2_sql("p"), du.ORDINARY_RULES_V2),
                             ("ur-3", v3_sql("p"), du.ORDINARY_RULES_V3)):
        emitted = re.findall(r"THEN '([a-z_]+)'", sql)
        if emitted != list(rules["order"]):
            f.append(f"{name} emits {emitted}, its rules order {list(rules['order'])}")
    keep = f"CASE WHEN p.symbol IN ({du._sql_list(s for s, _ in du.ORDINARY_KNOWN_V2)}) THEN NULL WHEN "
    for name, sql, rules in (("ur-2", v2_sql("p"), du.ORDINARY_RULES_V2), ("ur-3", v3_sql("p"), du.ORDINARY_RULES_V3)):
        if list(rules["order"]) != V2_ORDER:
            f.append(f"{name} order {list(rules['order'])}")
        if not sql.startswith(keep):
            f.append(f"{name}'s keep list is not its first WHEN")
    if "THEN NULL" in du.non_ordinary_sql("p"):
        f.append("ur-1 carries a keep list")
    return f


def v2_shape_failures():
    f = []
    known, keep, n1 = du.NON_ORDINARY_KNOWN_V2, du.ORDINARY_KNOWN_V2, len(du.NON_ORDINARY_KNOWN)
    if known[:n1] != du.NON_ORDINARY_KNOWN:
        f.append("ur-2's hand list does not start with ur-1's")
    if {s: k for s, k, _ in known[n1:]} != NEW_KNOWN or len(known) - n1 != len(NEW_KNOWN):
        f.append(f"ur-2's added lines {[e[:2] for e in known[n1:]]}")
    if {s for s, _ in keep} != KEEP:
        f.append(f"keep list {[s for s, _ in keep]}")
    for name, entries, width in (("hand list", known, 3), ("keep list", keep, 2)):
        syms = [e[0] for e in entries]
        if len(set(syms)) != len(syms):
            f.append(f"ur-2 {name} repeats a symbol")
        if not all(len(e) == width and all(isinstance(x, str) and x.strip() for x in e) for e in entries):
            f.append(f"ur-2 {name} has an empty or missing field")
    if {e[0] for e in known} & {e[0] for e in keep}:
        f.append("a symbol is on both the hand list and the keep list")
    r = du.ORDINARY_RULES_V2
    if list(r) != ["rule", "order", "kept", "known", "keep"]:
        f.append(f"ORDINARY_RULES_V2 keys {list(r)}")
    if r["known"] != [{"symbol": s, "kind": k, "evidence": e} for s, k, e in known] or \
            r["keep"] != [{"symbol": s, "evidence": e} for s, e in keep]:
        f.append("ORDINARY_RULES_V2's lists are not the hand list and the keep list")
    if re.search(r"#\d", json.dumps(r)):
        f.append("ur-2's rules text cites an issue or decision number")
    for name, terms in (("NAME_TERMS_V2", du.NAME_TERMS_V2), ("US_TRUST_LP_TERMS", du.US_TRUST_LP_TERMS)):
        if "'" in terms or ";" in terms or "\x08" in terms:
            f.append(f"{name} carries a quote, a ';' or a backspace")
    if du.NAME_TERMS_V2 != du.NAME_TERMS.replace(r"\bser\b", r"\bser$") or r"\bser\b" in du.NAME_TERMS_V2:
        f.append("NAME_TERMS_V2 is not ur-1's with ser anchored at the end")
    v2 = du.UNIVERSE_RULES_V2
    want = {"version": "ur-2", "ordinary": r, "known": known, "name_terms": du.NAME_TERMS_V2, "keep": keep,
            "v2": True, "dividend_yield": False, "class_twins": True, "line_starts": du.LINE_STARTS_V2}
    if v2 != want or v2["ordinary"] is not r or v2["known"] is not known or v2["keep"] is not keep:
        f.append("UNIVERSE_RULES_V2 is not ur-2's objects")
    b = du.UniverseRules(v2).block()
    if list(b) != ["version", "ordinary_shares", "class_stand_ins"] or b["version"] != "ur-2" or \
            b["ordinary_shares"]["rules"] is not r:
        f.append(f"UniverseRules(V2).block() keys {list(b)}, version {b.get('version')}")
    if re.search(r"#\d", du.CLASS_TWIN_RULE):
        f.append("the class-twin rule text cites an issue or decision number")
    # each keep line's row is one ur-2 would remove without the keep list
    nokeep = du.non_ordinary_sql("p", cfg=dict(du.UNIVERSE_RULES_V2, keep=()))
    got = dict(profile_v2_con().execute(f"SELECT p.symbol, {nokeep} FROM profile p"
                                        f" WHERE p.symbol IN ({du._sql_list(KEEP)})").fetchall())
    if sorted(s for s, w in got.items() if w) != sorted(KEEP):
        f.append(f"keep rows ur-2 would remove anyway: {sorted(s for s, w in got.items() if w)}")
    if {r[0] for r in PROFILE_V2 if r[8] == "known" and r[9] is None} != set(NEW_KNOWN):
        f.append("PROFILE_V2 lacks a row for an added hand-list line")
    return f


CLASS_TWIN_MAP = [   # isin_map rows: (symbol, isin), and the (sec_key, flag) mark_class_twins should leave
    ("PETR3.SA", "BRPETRACNOR9", None), ("PETR4.SA", "BRPETRACNPR6", "BRPETRACNOR9"),
    ("KLBN3.SA", "brklbnacnor1", None), ("KLBN11.SA", "BRKLBNCDAM18", "BRKLBNACNOR1"),
    ("GGBR3.SA", None, None), ("GGBR4.SA", "", "GGBR3.SA"),
    ("CMIG3.SA", None, None), ("CMIG4.SA", "BRCMIGACNPR3", "CMIG3.SA"),
    ("ELET3.SA", "BRELETACNOR6", None), ("AXIA6.SA", "BRELETACNPB7", "BRELETACNOR6"),
    ("FESA4.SA", "BRFESAACNPR5", None),
    ("PTT.BK", "TH0646010007", None), ("PTT-R.BK", "TH0646010R18", None), ("MSFT", "US5949181045", None),
]


def class_twin_failures():
    """mark_class_twins keys each SAO non-ordinary line to its company's ordinary line (ISIN, else symbol) and
    flags it, leaves ordinary lines, companies with no ordinary line and other markets alone, after check N too;
    ur-2's block records the stand-ins by date, ur-1's has no such key."""
    def isin_con():
        con = duckdb.connect()
        con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
        con.executemany("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", [(s, i) for s, i, _ in CLASS_TWIN_MAP])
        return con

    f = []
    con = isin_con()
    nvdr = du.mark_nvdr_twins(con)
    n = du.mark_class_twins(con)
    got = {s: rest for s, *rest in con.execute("SELECT symbol, sec_key, nvdr, class_twin FROM isin_map").fetchall()}
    for s, _, key in CLASS_TWIN_MAP:
        want = [key, None, True if key else None]
        if s == "PTT-R.BK":
            want = ["TH0646010007", True, None]   # check N's own key and flag, untouched
        if got[s] != want:
            f.append(f"{s}: {got[s]}, want {want}")
    keyed = {s for s, _, k in CLASS_TWIN_MAP if k}
    if (nvdr, n, du.class_twin_symbols(con)) != (1, len(keyed), keyed):
        f.append(f"counts: check N {nvdr}, class twins {n}, want 1 and {len(keyed)}")
    # the leg's UniverseRules keys only its universe's lines, and its stand-ins are the class twins it picked
    con = isin_con()
    con.execute("CREATE TABLE universe AS SELECT symbol FROM isin_map WHERE symbol <> 'AXIA6.SA'")
    ur = du.UniverseRules(du.UNIVERSE_RULES_V2)
    ur.mark_twins(con)
    ur.stand_ins(["PETR4.SA", "PETR3.SA", "MSFT"], date(2021, 7, 1))
    ur.stand_ins(["ELET3.SA"], date(2022, 7, 1))
    ur.stand_ins(["KLBN11.SA", "CMIG4.SA", "AXIA6.SA"], date(2020, 7, 1))
    b = ur.block().get("class_stand_ins")
    want = {"rule": du.CLASS_TWIN_RULE, "lines_keyed": len(keyed) - 1, "picked": 3,
            "by_period": [{"rebalance_date": "2020-07-01", "symbols": ["CMIG4.SA", "KLBN11.SA"]},
                          {"rebalance_date": "2021-07-01", "symbols": ["PETR4.SA"]}]}
    if b != want:
        f.append(f"ur-2 class_stand_ins {b}")
    v1 = du.UniverseRules()
    v1.mark_twins(con)
    if "class_stand_ins" in v1.block() or v1.class_twins:
        f.append("ur-1 keys class twins or carries class_stand_ins")
    return f


def line_start_failures(rules=du.UniverseRules):
    """ur-2's line_starts: each entry is a line on neither hand list, with an ISO first day and evidence. A leg
    that keeps the line leaves it out of screens before that day, not on it, and its block lists it; a leg
    without the line, a leg that removed it, and ur-1 have no line_starts key and leave nothing out."""
    f = []
    starts = du.UNIVERSE_RULES_V2["line_starts"]
    listed = {s for s, _, _ in du.NON_ORDINARY_KNOWN_V2} | {s for s, _ in du.ORDINARY_KNOWN_V2}
    for s, d, e in starts:
        if s in listed or not e or date.fromisoformat(d).isoformat() != d:
            f.append(f"line_starts entry {s} {d}")
    if du.UNIVERSE_RULES_V1.get("line_starts"):
        f.append("ur-1 carries line_starts")
    row = lambda s, why=None: {"symbol": s, "exchange": "SAO", "cik": None, "companyName": s, "non_ordinary": why}
    ur = rules(du.UNIVERSE_RULES_V2)
    ur.sector_rows([row("AZZA3.SA"), row("ARZZ3.SA")])
    got = [ur.not_started(date(2024, 8, 26)), ur.not_started(date(2024, 8, 27)), ur.not_started(date(2016, 7, 1))]
    if got != [["AZZA3.SA"], [], ["AZZA3.SA"]]:
        f.append(f"not_started on 2024-08-26, 2024-08-27, 2016-07-01: {got}")
    evidence = {s: e for s, _, e in starts}["AZZA3.SA"]
    want = {"rule": du.LINE_START_RULE, "lines": [{"symbol": "AZZA3.SA", "first_day": "2024-08-27", "evidence": evidence}]}
    if ur.block().get("line_starts") != want:
        f.append(f"line_starts block {ur.block().get('line_starts')}")
    for label, rows, cfg in (("a leg without the line", [row("PETR3.SA")], du.UNIVERSE_RULES_V2),
                             ("a leg that removed it", [row("AZZA3.SA", "known")], du.UNIVERSE_RULES_V2),
                             ("ur-1", [row("AZZA3.SA")], du.UNIVERSE_RULES_V1)):
        u = rules(cfg)
        u.sector_rows(rows)
        if "line_starts" in u.block() or u.not_started(date(2016, 7, 1)):
            f.append(f"{label}: line_starts {u.block().get('line_starts')}, left out {u.not_started(date(2016, 7, 1))}")
    return f


def line_start_mutants():
    class OnTheDay(du.UniverseRules):
        def not_started(self, target_date):
            return sorted(s for s, (d, _) in self.starts.items() if target_date.isoformat() <= d)

    class EveryLeg(du.UniverseRules):
        def block(self):
            return {"line_starts": {}, **super().block()}

    class RemovedToo(du.UniverseRules):
        def sector_rows(self, rows):
            kept = super().sector_rows(rows)
            self.starts = {s: (d, e) for s, d, e in self.cfg.get("line_starts", ()) if s in {r["symbol"] for r in rows}}
            return kept

    return {"left out on its first day too": OnTheDay, "a block on every leg": EveryLeg,
            "a removed line still listed": RemovedToo}


def du_with(**names):
    """du's names with some replaced: a mutant of data_utils' constants for the checks below, which read every
    name through it."""
    return SimpleNamespace(**{**{k: getattr(du, k) for k in dir(du) if not k.startswith("__")}, **names})


# ---------------------------------------------------------------- ur-2 frozen

def ur2_run(m):
    """m.UniverseRules(m.UNIVERSE_RULES_V2) after one sector query (PROFILE_V2 with its ur-2 reasons, AZZA3.SA
    beside ARZZ3.SA, BF-A beside BF-B) and one screen's class twins, as the JSON text a results file would hold."""
    rows = [(s, ex, n, cik, w) for s, ex, n, _, cik, _, _, _, w, _ in PROFILE_V2]
    rows += [("AZZA3.SA", "SAO", "Azzas 2154 S.A.", None, None), ("ARZZ3.SA", "SAO", "Arezzo", None, None),
             ("BF-A", "NYSE", "Brown-Forman Corporation", "0000014693", None),
             ("BF-B", "NYSE", "Brown-Forman Corporation", "0000014693", None)]
    ur = m.UniverseRules(m.UNIVERSE_RULES_V2)
    ur.sector_rows([{"symbol": s, "exchange": ex, "cik": cik, "companyName": n, "non_ordinary": w}
                    for s, ex, n, cik, w in rows])
    con = duckdb.connect()
    con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN, home_ccy BOOLEAN)")
    con.executemany("INSERT INTO isin_map VALUES (?, ?, TRUE, TRUE)", [(s, i) for s, i, _ in CLASS_TWIN_MAP])
    con.execute("CREATE TABLE universe AS SELECT symbol FROM isin_map")
    ur.mark_twins(con)
    ur.stand_ins(["PETR4.SA", "PETR3.SA", "CMIG4.SA"], date(2021, 7, 1))
    return json.dumps(ur.block())


def ur2_freeze_failures(frozen, m=du):
    """m's ur-2 against FROZEN_V2's data_utils and asset-growth's committed results."""
    f = []
    for a, kw in (((), {}), (("q",), {}), (("p", "prof"), {}), ((), {"known": ()})):
        if m.non_ordinary_sql(*a, cfg=m.UNIVERSE_RULES_V2, **kw) != \
                frozen.non_ordinary_sql(*a, cfg=frozen.UNIVERSE_RULES_V2, **kw):
            f.append(f"non_ordinary_sql{a} {kw} under ur-2 differs from {FROZEN_V2}'s")
    if json.dumps(m.UNIVERSE_RULES_V2) != json.dumps(frozen.UNIVERSE_RULES_V2):
        f.append(f"UNIVERSE_RULES_V2's JSON differs from {FROZEN_V2}'s")
    if ur2_run(m) != ur2_run(frozen):
        f.append(f"UniverseRules(UNIVERSE_RULES_V2).block() JSON differs from {FROZEN_V2}'s")
    files = sorted(glob.glob(os.path.join(ROOT, "asset-growth", "results", "returns_*.json")))
    rules = json.loads(json.dumps(m.ORDINARY_RULES_V2))
    starts = [{"symbol": s, "first_day": d, "evidence": e} for s, d, e in m.LINE_STARTS_V2]
    for path in files:
        with open(path) as fh:
            stored = json.load(fh)["universe_rules"]
        if stored["version"] != "ur-2" or stored["ordinary_shares"]["rules"] != rules:
            f.append(f"ORDINARY_RULES_V2 differs from {os.path.basename(path)}'s rules block")
        ls = stored.get("line_starts")
        if ls and (ls["rule"] != m.LINE_START_RULE or any(x not in starts for x in ls["lines"])):
            f.append(f"{os.path.basename(path)}'s line_starts are not LINE_STARTS_V2's")
    if not files:
        f.append("no committed asset-growth results file")
    return f


def ur2_freeze_mutants():
    """ur-2 grown in place: one line start, one hand-list entry."""
    starts = du.LINE_STARTS_V2 + (("XMUT3.SA", "2025-01-02", "a line start the frozen list never held"),)
    extra = ("XMUT", "notes", "a line the frozen hand list never held")
    known = du.NON_ORDINARY_KNOWN_V2 + (extra,)
    rules = dict(du.ORDINARY_RULES_V2,
                 known=du.ORDINARY_RULES_V2["known"] + [dict(zip(("symbol", "kind", "evidence"), extra))])
    return {
        "a line appended to LINE_STARTS_V2": du_with(
            LINE_STARTS_V2=starts, UNIVERSE_RULES_V2=dict(du.UNIVERSE_RULES_V2, line_starts=starts)),
        "an entry appended to NON_ORDINARY_KNOWN_V2": du_with(
            NON_ORDINARY_KNOWN_V2=known, ORDINARY_RULES_V2=rules,
            UNIVERSE_RULES_V2=dict(du.UNIVERSE_RULES_V2, ordinary=rules, known=known)),
    }


# ---------------------------------------------------------------- ur-3

NEW_KNOWN_V3 = {**dict.fromkeys(("FITBP", "76ID.L", "SANB.L", "SDIP-PREF.ST"), "preferred"),
                **dict.fromkeys(("AN3PH.AX", "AN3PI.AX", "AN3PJ.AX", "CBAPK.AX"), "notes"),
                **dict.fromkeys(("6288.HK", "ARION-SDB.ST"), "depositary receipt"),
                "NTCO3.SA": "renamed line", "9512.SR": "second line", "JP": "ADR"}
NEW_STARTS_V3 = {"L1G.AX": "2025-10-04", "MATR.TO": "2023-06-07", "TKO": "2023-09-12", "AXIA6.SA": "2025-12-05",
                 "AXIA3.SA": "2025-12-05"}
OLD_LINE = {"AZZA3.SA": "ARZZ3.SA", "L1G.AX": "PTM.AX", "MATR.TO": "SCL.TO", "TKO": "WWE", "AXIA6.SA": "ELET6.SA",
            "AXIA3.SA": "ELET3.SA"}
ELETROBRAS = "Centrais Elétricas Brasileiras S.A. - Eletrobras"
# symbol, exchange, companyName, isin, cik, isFund, isEtf, isAdr, then the ur-3, ur-2 and ur-1 reasons wanted.
# The profile rows of ur-3's added lines, the lines beside them, and the line-start lines with their old lines
# (kept: a line start acts at the screen, not on the universe).
PROFILE_V3 = [
    ("FITBP", "NYSE", "Fifth Third Bancorp", "US3167738869", None, F, F, F, "known", None, None),
    ("FITB", "NYSE", "Fifth Third Bancorp", "US3167731005", None, F, F, F, None, None, None),
    ("76ID.L", "LSE", "Rotork PLC", "GB0007530149", None, F, F, F, "known", None, None),
    ("ROR.L", "LSE", "Rotork plc", "GB00BVFNZH21", None, F, F, F, None, None, None),
    ("SANB.L", "LSE", "Santander UK plc", "GB0000044221", None, F, F, F, "known", None, None),
    ("SDIP-PREF.ST", "STO", "Sdiptech AB (publ)", "SE0006758348", None, F, F, F, "known", None, None),
    ("SDIP-B.ST", "STO", "Sdiptech AB (publ)", "SE0003756758", None, F, F, F, None, None, None),
    ("AN3PH.AX", "ASX", "Australia and New Zealand Banking Group Ltd", "AU0000261406", None, F, F, F, "known", None,
     None),
    ("AN3PI.AX", "ASX", "ANZ Group Holdings Ltd", "AU0000261422", None, F, F, F, "known", None, None),
    ("AN3PJ.AX", "ASX", "ANZ Group Holdings Limited", "AU0000261414", None, F, F, F, "known", None, None),
    ("ANZ.AX", "ASX", "ANZ Group Holdings Limited", "AU000000ANZ3", None, F, F, F, None, None, None),
    ("CBAPK.AX", "ASX", "Commonwealth Bank of Australia", "AU0000215618", None, F, F, F, "known", None, None),
    ("CBA.AX", "ASX", "Commonwealth Bank of Australia", "AU000000CBA7", None, F, F, F, None, None, None),
    ("6288.HK", "HKSE", "Fast Retailing Co., Ltd.", "US31188H2004", None, F, F, F, "known", None, None),
    ("9987.HK", "HKSE", "Yum China Holdings, Inc.", "US98850P1093", None, F, F, F, None, None, None),
    ("ARION-SDB.ST", "STO", "Arion banki hf.", "SE0010413567", None, F, F, F, "known", None, None),
    ("NTCO3.SA", "SAO", "Natura &Co Holding S.A.", "BRNTCOACNOR5", None, F, F, F, "known", None, None),
    ("NATU3.SA", "SAO", "Natura & Co Holding S.A.", "BRNATUACNOR6", None, F, F, F, None, None, None),
    ("9512.SR", "SAU", "Riyadh Cement Company", "SA153GA4KK14", None, F, F, F, "known", None, None),
    ("3092.SR", "SAU", "Riyadh Cement Company", "SA15TGH1GA14", None, F, F, F, None, None, None),
    ("JP", "NYSE", "Jupai Holdings Limited", "US48205B1070", None, F, F, F, "known", None, None),
    ("L1G.AX", "ASX", "L1 Group Limited", "AU0000423501", None, F, F, F, None, None, None),
    ("PTM.AX", "ASX", "Platinum Investment Management Limited", "AU000000PTM6", None, F, F, F, None, None, None),
    ("MATR.TO", "TSX", "Mattr Corp.", "CA57722Y1025", None, F, F, F, None, None, None),
    ("SCL.TO", "TSX", "Shawcor Ltd.", "CA8204391079", None, F, F, F, None, None, None),
    ("TKO", "NYSE", "TKO Group Holdings, Inc.", "US87256C1018", None, F, F, F, None, None, None),
    ("WWE", "NYSE", "World Wrestling Entertainment, Inc.", "US98156Q1085", None, F, F, F, None, None, None),
    ("AXIA6.SA", "SAO", "AXIA Energia S.A.", "BRAXIAACNPB1", None, F, F, F, None, None, None),
    ("AXIA3.SA", "SAO", "AXIA Energia", "BRAXIAACNOR0", None, F, F, F, None, None, None),
    ("ELET6.SA", "SAO", ELETROBRAS, "BRELETACNPB7", None, F, F, F, None, None, None),
]


def v3_rows():
    """PROFILE_V3, then every v2_rows case wanting its ur-2 reason under ur-3 too."""
    return PROFILE_V3 + [(*r[:8], r[8], r[8], r[9]) for r in v2_rows()]


def v3_failures(m=du):
    """ur-3's, ur-2's and ur-1's reasons on v3_rows: each added line reads known under ur-3 alone."""
    con, rows = profile_v2_con(v3_rows()), v3_rows()
    f = []
    for label, cfg, col in (("ur-3", m.UNIVERSE_RULES_V3, 8), ("ur-2", m.UNIVERSE_RULES_V2, 9),
                            ("ur-1", m.UNIVERSE_RULES_V1, 10)):
        sql = m.non_ordinary_sql("p", cfg=cfg)
        if ";" in sql or "\x08" in sql:
            f.append(f"{label} SQL carries a ';' or a backspace")
        got = dict(con.execute(f"SELECT p.i, {sql} FROM profile p").fetchall())
        f += [f"{label} {r[0]} ({r[3]!r}): {got[i]!r}, want {r[col]!r}" for i, r in enumerate(rows) if got[i] != r[col]]
    return f


def v3_shape_failures(m=du):
    """ur-3 is ur-2's hand list plus NEW_KNOWN_V3, ur-2's keep list, name terms and rules, its own version,
    order entry and kept text, and v2 on."""
    f = []
    known, n2, keep = m.NON_ORDINARY_KNOWN_V3, len(m.NON_ORDINARY_KNOWN_V2), m.ORDINARY_KNOWN_V2
    if known[:n2] != m.NON_ORDINARY_KNOWN_V2:
        f.append("ur-3's hand list does not start with ur-2's")
    if {s: k for s, k, _ in known[n2:]} != NEW_KNOWN_V3 or len(known) - n2 != len(NEW_KNOWN_V3):
        f.append(f"ur-3's added lines {[e[:2] for e in known[n2:]]}")
    syms = [e[0] for e in known]
    if len(set(syms)) != len(syms) or set(syms) & {s for s, _ in keep}:
        f.append("ur-3's hand list repeats a symbol or shares one with the keep list")
    if not all(len(e) == 3 and all(isinstance(x, str) and x.strip() for x in e) for e in known):
        f.append("ur-3's hand list has an empty or missing field")
    r, r2 = m.ORDINARY_RULES_V3, m.ORDINARY_RULES_V2
    if list(r) != list(r2) or r["rule"] != r2["rule"]:
        f.append(f"ORDINARY_RULES_V3 keys {list(r)}, or its rule is not ur-2's")
    others = lambda order: {k: v for k, v in order.items() if k != "known"}
    if list(r["order"]) != V2_ORDER or others(r["order"]) != others(r2["order"]) or \
            "NON_ORDINARY_KNOWN_V3" not in r["order"]["known"]:
        f.append("ur-3's order is not ur-2's with its own hand list named")
    kept = r["kept"]
    if kept == r2["kept"] or not all(t in kept for t in ("`known` list below", "until a book holds one",
                                                          "listed first elsewhere")):
        f.append(f"ur-3's kept text {kept!r}")
    if r["known"] != [{"symbol": s, "kind": k, "evidence": e} for s, k, e in known] or r["keep"] != r2["keep"]:
        f.append("ORDINARY_RULES_V3's lists are not its hand list and ur-2's keep list")
    if re.search(r"#\d", json.dumps(r)) or re.search(r"#\d", json.dumps(m.LINE_STARTS_V3)):
        f.append("ur-3's rules text or line starts cite an issue or decision number")
    v3, v2 = m.UNIVERSE_RULES_V3, m.UNIVERSE_RULES_V2
    want = {"version": "ur-3", "ordinary": r, "known": known, "name_terms": m.NAME_TERMS_V2, "keep": keep,
            "v2": True, "dividend_yield": False, "class_twins": True, "line_starts": m.LINE_STARTS_V3}
    if v3 != want or list(v3) != list(v2) or v3["ordinary"] is not r or v3["known"] is not known or \
            v3["keep"] is not keep or v3["line_starts"] is not m.LINE_STARTS_V3:
        f.append("UNIVERSE_RULES_V3 is not ur-3's objects in ur-2's keys")
    b = m.UniverseRules(v3).block()
    if list(b) != ["version", "ordinary_shares", "class_stand_ins"] or b["version"] != "ur-3" or \
            b["ordinary_shares"]["rules"] is not r:
        f.append(f"UniverseRules(V3).block() keys {list(b)}, version {b.get('version')}")
    if {x[0] for x in PROFILE_V3 if x[8:] == ("known", None, None)} != set(NEW_KNOWN_V3):
        f.append("PROFILE_V3 lacks a row for an added hand-list line")
    return f


def v3_line_start_failures(m=du):
    """ur-3's line_starts: ur-2's, then NEW_STARTS_V3's, each on neither list, with an ISO day and evidence. A leg
    that keeps every listed line and its old line leaves each listed line out of a screen the day before its
    first own day, not on it, never the old line, and lists them all; ur-2 on the same leg lists AZZA3.SA
    alone."""
    f = []
    v3 = m.UNIVERSE_RULES_V3
    starts, n2 = v3["line_starts"], len(m.LINE_STARTS_V2)
    if starts[:n2] != m.LINE_STARTS_V2 or {s: d for s, d, _ in starts[n2:]} != NEW_STARTS_V3 or \
            len(starts) - n2 != len(NEW_STARTS_V3):
        f.append(f"ur-3's line starts {[e[:2] for e in starts]}")
    listed = {s for s, _, _ in v3["known"]} | {s for s, _ in v3["keep"]}
    for s, d, e in starts:
        if s in listed or not e.strip() or date.fromisoformat(d).isoformat() != d:
            f.append(f"line_starts entry {s} {d}")
    row = lambda s: {"symbol": s, "exchange": "X", "cik": None, "companyName": s, "non_ordinary": None}
    leg = [row(s) for s in sorted(set(OLD_LINE) | set(OLD_LINE.values()))]
    ur = m.UniverseRules(v3)
    ur.sector_rows(leg)
    for s, d, _ in starts:
        day = date.fromisoformat(d)
        before, on = ur.not_started(day - timedelta(days=1)), ur.not_started(day)
        if s not in before or s in on:
            f.append(f"{s}: left out the day before {d}: {s in before}, on it: {s in on}")
    if ur.not_started(date(2000, 7, 1)) != sorted(s for s, _, _ in starts):
        f.append(f"left out of a 2000 screen: {ur.not_started(date(2000, 7, 1))}")
    want = {"rule": m.LINE_START_RULE,
            "lines": [{"symbol": s, "first_day": d, "evidence": e} for s, d, e in sorted(starts)]}
    if ur.block().get("line_starts") != want:
        f.append(f"ur-3 line_starts block {ur.block().get('line_starts')}")
    u2 = m.UniverseRules(m.UNIVERSE_RULES_V2)
    u2.sector_rows(leg)
    if u2.not_started(date(2000, 7, 1)) != ["AZZA3.SA"]:
        f.append(f"ur-2 leaves out {u2.not_started(date(2000, 7, 1))}")
    return f


def v3_mutants():
    """name: (data_utils with ur-3 changed, the checks each of which must fail)."""
    v3, drop = du.UNIVERSE_RULES_V3, "6288.HK"
    known = tuple(e for e in du.NON_ORDINARY_KNOWN_V3 if e[0] != drop)
    rules = dict(du.ORDINARY_RULES_V3, known=[x for x in du.ORDINARY_RULES_V3["known"] if x["symbol"] != drop])
    starts = du.LINE_STARTS_V3[:-1]
    return {
        "a known entry dropped": (du_with(NON_ORDINARY_KNOWN_V3=known, ORDINARY_RULES_V3=rules,
                                          UNIVERSE_RULES_V3=dict(v3, ordinary=rules, known=known)),
                                  (v3_shape_failures, v3_failures)),
        "ur-2's rules under the ur-3 name": (du_with(UNIVERSE_RULES_V3=dict(du.UNIVERSE_RULES_V2, version="ur-3")),
                                             (v3_shape_failures, v3_failures, v3_line_start_failures)),
        "the v2 rules off": (du_with(UNIVERSE_RULES_V3=dict(v3, v2=False)), (v3_shape_failures, v3_failures)),
        "ur-2's kept text": (du_with(ORDINARY_RULES_V3=dict(du.ORDINARY_RULES_V3, kept=du.ORDINARY_RULES_V2["kept"])),
                             (v3_shape_failures,)),
        "a line start dropped": (du_with(LINE_STARTS_V3=starts, UNIVERSE_RULES_V3=dict(v3, line_starts=starts)),
                                 (v3_line_start_failures,)),
    }


# ---------------------------------------------------------------- shared screen helpers

def started_sql_failures(fn=None):
    """started_sql leaves its SQL as it is with no line out, else drops the listed lines and no others (quotes too)."""
    fn = du.started_sql if fn is None else fn
    sql = "SELECT symbol FROM lines"
    f = [] if fn(sql, []) == sql else ["started_sql changes the SQL when no line is left out"]
    con = duckdb.connect()
    con.execute("CREATE TABLE lines AS SELECT * FROM (VALUES ('AZZA3.SA'), ('ARZZ3.SA'), ('O''X')) t(symbol)")
    got = sorted(r[0] for r in con.execute(fn(sql, ["AZZA3.SA", "O'X"])).fetchall())
    if got != ["ARZZ3.SA"]:
        f.append(f"started_sql keeps {got}")
    return f


def row_guard_failures(cls=None):
    """RowGuard counts each screen's unranked candidates by reason (a reason off its list isn't counted, and a
    date screened again replaces its count), and its block holds rule, dropped, by_reason and by_period in date
    order, each with the reasons in the guard's order."""
    g = (du.RowGuard if cls is None else cls)("the rule text", ("late", "early"))
    g.record([("A", "early")], date(2021, 7, 1))
    g.record([("A", "early"), ("B", "late"), ("C", "early"), ("D", "other")], date(2021, 7, 1))
    g.record([], date(2020, 7, 1))
    want = {"rule": "the rule text", "dropped": 3, "by_reason": {"late": 1, "early": 2},
            "by_period": [{"rebalance_date": "2020-07-01", "late": 0, "early": 0},
                          {"rebalance_date": "2021-07-01", "late": 1, "early": 2}]}
    return [] if json.dumps(g.block()) == json.dumps(want) else [f"RowGuard block {g.block()}"]


def row_guard_mutants():
    class Adds(du.RowGuard):
        def record(self, rows, target_date):
            old = self.periods.get(target_date.isoformat(), {})
            super().record(rows, target_date)
            now = self.periods[target_date.isoformat()]
            self.periods[target_date.isoformat()] = {k: v + old.get(k, 0) for k, v in now.items()}

    class NameOrder(du.RowGuard):
        def block(self):
            b = super().block()
            return dict(b, by_reason=dict(sorted(b["by_reason"].items())))

    return {"a date screened again counted twice": Adds, "reasons in name order": NameOrder}


def one_copy_failures():
    """Asset growth runs data_utils' started_sql as _started and a no-argument RowGuard as PriorYearGuard."""
    spec = importlib.util.spec_from_file_location("asset_growth_one_copy",
                                                  os.path.join(ROOT, "asset-growth", "backtest.py"))
    ag = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ag)
    f = [] if ag._started is du.started_sql else ["asset growth's _started is not data_utils.started_sql"]
    own = {k for k in vars(ag.PriorYearGuard) if not (k.startswith("__") and k.endswith("__"))} - {"RULE"}
    if not issubclass(ag.PriorYearGuard, du.RowGuard) or own:
        f.append(f"PriorYearGuard is not a RowGuard, or overrides {sorted(own)}")
    want = {"rule": ag.PriorYearGuard.RULE, "dropped": 0,
            "by_reason": {"currency_switch": 0, "no_prior_in_window": 0}, "by_period": []}
    if json.dumps(ag.PriorYearGuard().block()) != json.dumps(want):
        f.append(f"PriorYearGuard().block() {ag.PriorYearGuard().block()}")
    return f


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
    with tempfile.TemporaryDirectory() as tmp:
        frozen = load_base(tmp, FROZEN)[0]
    expect(f"ur-1 frozen at {FROZEN}: SQL text, every committed rules block, the block JSON", freeze_failures(frozen))
    expect_fail("ur-1 freeze, one more hand-list line", freeze_failures(frozen, freeze_mutant()))
    expect("ur-2: keep list, hand list, Brazil classes, US ADRs, US trusts and LPs, ser at a name's end;"
           " ur-1 unchanged on the same rows", v2_failures())
    for label, m in v2_mutants().items():
        check(m("p") != v2_sql("p"), f"ur-2 mutant {label} changed nothing")
        expect_fail(f"ur-2, {label}", v2_failures(m))
    expect("ur-2 on the ur-1 cases, pasted as a filter too", ordinary_failures(v2_sql))
    expect("each CASE emits its rules' reasons in order; ur-2's keep list first", reason_failures())
    expect("ur-2 hand list, keep list, rules text and config", v2_shape_failures())
    expect("ur-2 class twins: keyed to the ordinary line, the rest untouched, stand-ins by date", class_twin_failures())
    expect("ur-2 line_starts: out before the first own day, listed on its leg only", line_start_failures())
    for label, cls in line_start_mutants().items():
        expect_fail(f"ur-2 line_starts, {label}", line_start_failures(cls))
    with tempfile.TemporaryDirectory() as tmp:
        frozen_v2 = load_base(tmp, FROZEN_V2)[0]
    expect(f"ur-2 frozen at {FROZEN_V2}: config JSON, SQL text, an exercised block, every committed asset-growth"
           " rules block", ur2_freeze_failures(frozen_v2))
    for label, m in ur2_freeze_mutants().items():
        expect_fail(f"ur-2 freeze, {label}", ur2_freeze_failures(frozen_v2, m))
    expect("ur-3: the added lines known, the lines beside them kept, ur-2's reasons on ur-2's rows; ur-1 and ur-2"
           " unchanged on the same rows", v3_failures())
    expect("ur-3 on the ur-1 cases, pasted as a filter too", ordinary_failures(v3_sql))
    expect("ur-3 hand list, keep list, rules text and config", v3_shape_failures())
    expect("ur-3 line_starts: each out before its first own day, ur-2's leg unchanged", v3_line_start_failures())
    for label, (m, checks) in v3_mutants().items():
        for fn in checks:
            expect_fail(f"ur-3 {fn.__name__}, {label}", fn(m))
    expect("started_sql: unchanged with no line out, the listed lines dropped", started_sql_failures())
    expect_fail("started_sql, nothing dropped", started_sql_failures(lambda sql, late: sql))
    expect("RowGuard: counts by reason, a date replaced, block keys and order", row_guard_failures())
    for label, cls in row_guard_mutants().items():
        expect_fail(f"RowGuard, {label}", row_guard_failures(cls))
    expect("one copy: asset growth's _started and PriorYearGuard are data_utils'", one_copy_failures())
    print("\n".join(f"ok {r}" for r in ran))
    print(f"test_universe_rules: PASS ({len(ran)} checks)")


if __name__ == "__main__":
    main()
