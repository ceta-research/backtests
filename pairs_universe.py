"""Per-year candidate universe and pair-level exits for the three pairs backtests.

pairs-fundamentals, pairs-zscore and pairs-multi-pair take their candidates from here: for each
trading year, the lines listed and traded through the formation year before it, one line per
security and per company, the top N of each sector by that year's median daily traded value in USD.
The size look-ahead of a fixed "largest today" list is gone. Survivorship is reduced, not removed:
stock_eod holds almost no name that stopped trading before 2015 (US) or 2020 (elsewhere).
The operating-company test is not point-in-time: a line needs an FY filing of any date, so a line
whose first filing is dated years after a formation year is still a candidate in it.

Usage:
    from pairs_universe import fetch_universe, build_candidates, fetch_prices

    if fetch_universe(cr, con, exchanges, years) is None:
        return None
    per_year = build_candidates(con, years, offset_days, MIN_LEG_PRICE, TOP_N_PER_SECTOR, MIN_CORR_DAYS)
    fetch_prices(cr, con, None, bench_symbols)
"""

import math
import re
import time
from datetime import date, timedelta

from data_utils import (LTP_DELIST_GRACE_DAYS, LTP_EARLIER_HOLDER_DAYS, LtpExits,
                        campaign_listing_condition, isin_dedup_sql, load_isin_map, query_parquet,
                        remove_price_oscillations, utc_date, utc_epoch)

LISTED_GRACE_DAYS = 10        # a line's last formation-year bar is at most this far before year end
FX_MAX_STALE_DAYS = 7         # newest USDxxx close on/before formation year end, at most this old
ENTRY_WINDOW_DAYS = 10        # entry bar: first bar in [T + offset, T + offset + this], as get_prices
COST_CAP_LAG_DAYS = 45        # a filing counts this long after its period end
COST_CAP_MAX_AGE_DAYS = 540   # and no longer than this
YEAR_END_DAYS = 10            # a position closed more than this before year end is a "last_bar" close
PRICES_FROM = "2004-01-01"    # first bar of prices_cache: the formation year of 2005
MAX_IN_CHARS = 30000          # literal IN-list budget of one query (the API rejects SQL over 50,000)
# profile.currency of a sub-unit quote: (major currency, majors per quoted unit)
SUB_UNITS = {"GBp": ("GBP", 0.01), "ZAc": ("ZAR", 0.01), "ILA": ("ILS", 0.01)}
# The `universe` block of a results JSON; the topic adds top_n_per_sector and min_formation_bars.
UNIVERSE_NOTES = {
    "rule": "per formation year: lines with a sector and an FY filing of any date (not point-in-time), "
            "funds out, listed through the year and traded; one line per security and per company; "
            "top N per sector by size",
    "filing": "operating-company test: at least one FY key_metrics row with a market cap, dated at any "
              "time, so a filing first made after the formation year admits a line to that year",
    "size": "formation-year median of daily close x volume, in USD at the year-end rate",
    "cost_cap": f"latest FY market cap dated {COST_CAP_LAG_DAYS} to {COST_CAP_MAX_AGE_DAYS} days before "
                "the trading year, in USD at the same rate; none: the smallest-cap cost tier",
    "sector": "profile sector as fetched, not point-in-time",
}


def _norm_name(name):
    """Company name for matching: upper case, parenthesised text out, punctuation to single spaces."""
    return " ".join(re.sub(r"[\W_]+", " ", re.sub(r"\([^)]*\)", " ", str(name or "").upper())).split())


def _ticker_root(symbol):
    """(root, suffix): the ticker before its first hyphen; a six-character .KS/.KQ code loses its class digit."""
    base, dot, suffix = symbol.rpartition(".")
    if not dot:
        base, suffix = symbol, ""
    root = base.split("-")[0]
    if suffix in ("KS", "KQ") and len(root) == 6:
        root = root[:5]
    return root, suffix


def company_keys(rows):
    """{symbol: company key} for a leg's lines: connected components, key = the smallest symbol.

    Two lines are one company when their normalised ISINs match, their ciks match, their
    normalised names match and at most one of them carries a cik (18 US names carry two ciks),
    or they share a ticker root on the same suffix. rows: dicts with symbol, isin, cik, companyName.
    """
    lines = {r["symbol"]: r for r in rows}
    parent = {s: s for s in lines}

    def find(s):
        while parent[s] != s:
            parent[s] = parent[parent[s]]
            s = parent[s]
        return s

    def union(group):
        roots = sorted({find(s) for s in group})
        for r in roots[1:]:
            parent[r] = roots[0]   # the smaller symbol stays the root

    by_isin, by_cik, by_name, by_root = {}, {}, {}, {}
    for s in sorted(lines):
        isin = str(lines[s].get("isin") or "").strip().upper()
        cik = str(lines[s].get("cik") or "").strip().lstrip("0")
        name, root = _norm_name(lines[s].get("companyName")), _ticker_root(s)
        if isin:
            by_isin.setdefault(isin, []).append(s)
        if cik:
            by_cik.setdefault(cik, []).append(s)
        if name:
            by_name.setdefault(name, []).append((s, bool(cik)))
        if root[0]:
            by_root.setdefault(root, []).append(s)
    for group in by_isin.values():
        union(group)
    for group in by_cik.values():
        union(group)
    for group in by_name.values():
        if not all(has_cik for _, has_cik in group):   # two lines that both carry a cik are left to the cik
            union([s for s, _ in group])
    for group in by_root.values():
        union(group)
    return {s: find(s) for s in sorted(lines)}


def _fetch(cr, sql, con, table, schema, verbose=False, **kw):
    """query_parquet into `table`, waiting out a rate limit; an empty result keeps the declared columns."""
    for attempt in range(4):
        con.execute(f"DROP TABLE IF EXISTS {table}")
        try:
            n = query_parquet(cr, sql, con, table, verbose=verbose, **kw)
            break
        except Exception as e:
            wait = re.search(r"Retry after (\d+)s", str(e))
            if attempt == 3 or not wait:
                raise
            time.sleep(max(int(wait.group(1)), 5 * (attempt + 1)))
    if not n:  # query_parquet's empty-result placeholder has none of the columns
        con.execute(f"DROP TABLE {table}")
        con.execute(f"CREATE TABLE {table}({schema})")
    return n


def _fetch_in(cr, con, table, schema, sql_for, symbols, verbose=False, **kw):
    """Fetch sql_for(<quoted symbol list>) into `table`, in as many queries as MAX_IN_CHARS needs."""
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TABLE {table}({schema})")
    cols = ", ".join(c.split()[0] for c in schema.split(", "))
    total, chunk, size = 0, [], 0
    for q in ["'{}'".format(s.replace("'", "''")) for s in symbols] + [None]:
        if chunk and (q is None or size + len(q) + 2 > MAX_IN_CHARS):
            total += _fetch(cr, sql_for(", ".join(chunk)), con, f"{table}_part", schema, verbose=verbose, **kw)
            con.execute(f"INSERT INTO {table} SELECT {cols} FROM {table}_part")
            con.execute(f"DROP TABLE {table}_part")
            chunk, size = [], 0
        if q is not None:
            chunk.append(q)
            size += len(q) + 2
    return total


def _fx_window(fy):
    """Epoch bounds of the USD rate of formation year fy: on or before its last day, at most FX_MAX_STALE_DAYS old."""
    end = date(fy, 12, 31)
    return utc_epoch(end - timedelta(days=FX_MAX_STALE_DAYS)), utc_epoch(end)


def fetch_universe(cr, con, exchanges, years, verbose=False):
    """Fetch a leg's static universe and its per-year inputs into con. Returns {table: rows}, None if empty.

    years are the trading years; the formation year of trading year Y is Y - 1. Tables:
        universe(symbol, sector, company, currency)   one row per line, company = company_keys()
        isin_map                                      load_isin_map()'s
        metrics_cache(symbol, marketCap, reportedCurrency, filing_epoch)   every FY filing with a cap
        bar_stats(symbol, yr, n_days, last_epoch, med_tv)   per calendar year of bars
        fx_year(yr, ccy, per_usd)                     units of ccy per USD at the end of formation year yr
        entry_px(symbol, trade_epoch, adjClose)       January bars of lines that share a company
    The universe has no isActivelyTrading test: a name that has since stopped trading stays in the
    years it was listed.
    """
    if not exchanges:
        raise ValueError("the pairs universe needs an exchange list")
    years = sorted(int(y) for y in years)
    fys = [y - 1 for y in years]
    ex_in = ", ".join("'{}'".format(e.replace("'", "''")) for e in exchanges)
    listing = campaign_listing_condition(exchanges)
    profile_where = f"exchange IN ({ex_in})" + (f" AND {listing}" if listing else "")
    if listing:
        print(f"  Listing filter: {listing}")
    # Operating-company test: notes, preferreds, warrants and when-issued lines file no FY key metrics.
    # Any filing date, a later one included: not point-in-time (UNIVERSE_NOTES says so).
    has_filing = "symbol IN (SELECT k.symbol FROM key_metrics k WHERE k.period = 'FY' AND k.marketCap > 0)"
    # Always a subquery: a literal list of the US universe would pass the SQL length limit.
    sym_filter = f"symbol IN (SELECT p.symbol FROM profile p WHERE {profile_where}) AND {has_filing}"
    counts = {}

    print("  Fetching the universe (profile)...")
    line_cols = "symbol, sector, isin, cik, companyName, currency"
    _fetch(cr, f"""
        WITH dedup AS (
            SELECT p.symbol, p.sector, p.isin, p.cik, p.companyName, p.currency, p.isEtf, p.isFund,
                   ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY p.fetchedAtEpoch DESC NULLS LAST,
                       p.sector DESC NULLS LAST, p.isin DESC NULLS LAST, p.cik DESC NULLS LAST,
                       p.companyName DESC NULLS LAST, p.currency DESC NULLS LAST,
                       p.isEtf DESC NULLS LAST, p.isFund DESC NULLS LAST) AS rn
            FROM profile p
            WHERE {profile_where}
        )
        SELECT {line_cols}
        FROM dedup
        WHERE rn = 1
          AND sector IS NOT NULL AND sector <> ''
          AND COALESCE(isEtf, false) = false AND COALESCE(isFund, false) = false
          AND {has_filing}
    """, con, "_pairs_lines", "symbol VARCHAR, sector VARCHAR, isin VARCHAR, cik VARCHAR,"
                               " companyName VARCHAR, currency VARCHAR", verbose=verbose)
    rows = [dict(zip(line_cols.split(", "), r)) for r in
            con.execute(f"SELECT {line_cols} FROM _pairs_lines ORDER BY symbol").fetchall()]
    con.execute("DROP TABLE _pairs_lines")
    if not rows:
        print("  No lines with a sector and an FY filing.")
        return None
    keys = company_keys(rows)
    con.execute("DROP TABLE IF EXISTS universe")
    con.execute("CREATE TABLE universe(symbol VARCHAR, sector VARCHAR, company VARCHAR, currency VARCHAR)")
    con.executemany("INSERT INTO universe VALUES (?, ?, ?, ?)",
                    [(r["symbol"], r["sector"], keys[r["symbol"]], r["currency"]) for r in rows])
    counts["universe"] = len(rows)
    print(f"  Universe: {len(rows)} lines, {len(set(keys.values()))} companies,"
          f" {len({r['sector'] for r in rows})} sectors")

    con.execute("DROP TABLE IF EXISTS isin_map")
    counts["isin_map"] = load_isin_map(cr, con, exchanges, sym_filter, verbose=verbose)

    print("  Fetching FY market caps...")
    counts["metrics_cache"] = _fetch(cr, f"""
        SELECT symbol, marketCap, reportedCurrency, CAST(dateEpoch AS BIGINT) AS filing_epoch
        FROM key_metrics
        WHERE period = 'FY' AND marketCap > 0 AND dateEpoch IS NOT NULL
          AND {sym_filter}
    """, con, "metrics_cache", "symbol VARCHAR, marketCap DOUBLE, reportedCurrency VARCHAR, filing_epoch BIGINT",
        verbose=verbose, memory_mb=4096, threads=2)
    if con.execute("SELECT COUNT(*) FROM metrics_cache WHERE filing_epoch IS NULL").fetchone()[0]:
        raise AssertionError("metrics_cache holds a filing without an epoch: the latest-filing pick needs one")

    print("  Fetching formation-year bar statistics...")
    counts["bar_stats"] = _fetch(cr, f"""
        SELECT symbol, CAST(YEAR(CAST(date AS DATE)) AS INTEGER) AS yr,
               COUNT(DISTINCT dateEpoch) AS n_days, CAST(MAX(dateEpoch) AS BIGINT) AS last_epoch,
               MEDIAN(COALESCE(close * volume, 0)) AS med_tv
        FROM stock_eod
        WHERE date >= '{fys[0]}-01-01' AND date <= '{fys[-1]}-12-31' AND adjClose > 0
          AND {sym_filter}
        GROUP BY 1, 2
    """, con, "bar_stats", "symbol VARCHAR, yr INTEGER, n_days BIGINT, last_epoch BIGINT, med_tv DOUBLE",
        verbose=verbose, timeout=600, memory_mb=16384, threads=6)

    # USDxxx (units per USD) for every quote and reporting currency the leg holds.
    ccys = {SUB_UNITS.get(c, (c,))[0] for (c,) in con.execute("SELECT DISTINCT currency FROM universe").fetchall() if c}
    ccys |= {c for (c,) in con.execute("SELECT DISTINCT reportedCurrency FROM metrics_cache").fetchall() if c}
    ccys = sorted(c for c in ccys if c != "USD" and re.fullmatch(r"[A-Z]{3}", c))
    fx_schema = "ccy VARCHAR, fx_epoch BIGINT, per_usd DOUBLE"
    if ccys:
        print(f"  Fetching year-end FX for {len(ccys)} currencies...")
        wins = " OR ".join("(dateEpoch >= {} AND dateEpoch <= {})".format(*_fx_window(fy)) for fy in fys)
        _fetch(cr, f"""
            SELECT SUBSTR(symbol, 4, 3) AS ccy, CAST(dateEpoch AS BIGINT) AS fx_epoch, close AS per_usd
            FROM forex_prices_daily
            WHERE symbol IN ({", ".join(f"'USD{c}'" for c in ccys)}) AND close > 0 AND ({wins})
        """, con, "_pairs_fx", fx_schema, verbose=verbose)
    else:
        con.execute("DROP TABLE IF EXISTS _pairs_fx")
        con.execute(f"CREATE TABLE _pairs_fx({fx_schema})")
    con.execute("DROP TABLE IF EXISTS fx_year")
    con.execute("CREATE TABLE fx_year(yr INTEGER, ccy VARCHAR, per_usd DOUBLE)")
    for fy in fys:
        lo, hi = _fx_window(fy)
        rates = dict(con.execute(f"""
            SELECT ccy, per_usd FROM _pairs_fx
            WHERE fx_epoch >= {lo} AND fx_epoch <= {hi}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY fx_epoch DESC, per_usd DESC) = 1
        """).fetchall())
        fx_rows = [(fy, "USD", 1.0)] + [(fy, c, r) for c, r in sorted(rates.items())]
        fx_rows += [(fy, q, rates[major] / unit) for q, (major, unit) in sorted(SUB_UNITS.items()) if major in rates]
        con.executemany("INSERT INTO fx_year VALUES (?, ?, ?)", fx_rows)
    con.execute("DROP TABLE _pairs_fx")
    counts["fx_year"] = con.execute("SELECT COUNT(*) FROM fx_year").fetchone()[0]

    # Entry bars only where rules d and e can separate lines: companies with two or more of them.
    n_lines = {}
    for k in keys.values():
        n_lines[k] = n_lines.get(k, 0) + 1
    shared = sorted(s for s, k in keys.items() if n_lines[k] > 1)
    # [T, T + window + 1] covers get_prices' entry window at offset_days 0 and 1.
    wins = " OR ".join("(date >= '{}' AND date <= '{}')".format(
        date(y, 1, 1), date(y, 1, 1) + timedelta(days=ENTRY_WINDOW_DAYS + 1)) for y in years)
    print(f"  Fetching entry bars of {len(shared)} lines that share a company...")
    counts["entry_px"] = _fetch_in(cr, con, "entry_px", "symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE",
                                   lambda in_list: f"""
        SELECT symbol, CAST(dateEpoch AS BIGINT) AS trade_epoch, adjClose
        FROM stock_eod
        WHERE symbol IN ({in_list}) AND adjClose > 0 AND ({wins})""", shared, verbose=verbose)
    return counts


def _entry_bar_sql(year, offset_days):
    """Each line's first entry_px bar in get_prices' entry window of trading year `year`."""
    first = date(year, 1, 1) + timedelta(days=offset_days)
    lo, hi = utc_epoch(first), utc_epoch(first + timedelta(days=ENTRY_WINDOW_DAYS))
    return f"""
                SELECT symbol, adjClose FROM entry_px
                WHERE trade_epoch >= {lo} AND trade_epoch <= {hi}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol
                    ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST) = 1"""


def _ranked_sql(sql):
    """`sql` plus rnk, each line's size rank inside its sector."""
    return f"""
            SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.sector ORDER BY c.size_usd DESC, c.symbol) AS rnk
            FROM ({sql}) c"""


def _cut_sql(sql, top_n):
    """The top_n lines of each sector of `sql`."""
    return f"""
        SELECT * EXCLUDE (rnk) FROM ({_ranked_sql(sql)}) _r WHERE rnk <= {int(top_n)}"""


def _stages(year, offset_days, min_leg_price, top_n, min_days):
    """One trading year's candidate pipeline as nested SELECTs: listed, sized, isin, company, final."""
    fy, t = year - 1, date(year, 1, 1)
    last_ok = utc_epoch(date(fy, 12, 31) - timedelta(days=LISTED_GRACE_DAYS))
    cap_lo = utc_epoch(t - timedelta(days=COST_CAP_MAX_AGE_DAYS))
    cap_hi = utc_epoch(t - timedelta(days=COST_CAP_LAG_DAYS))
    s = {}
    # a, b: listed through the formation year and traded. c: size in USD, NULL without a rate.
    s["listed"] = f"""
        SELECT u.symbol, u.sector, u.company, b.med_tv / f.per_usd AS size_usd
        FROM universe u
        JOIN bar_stats b ON b.symbol = u.symbol AND b.yr = {fy}
        LEFT JOIN fx_year f ON f.yr = {fy} AND f.ccy = u.currency
        WHERE b.n_days >= {int(min_days)}
          AND b.last_epoch >= {last_ok}
          AND b.med_tv > 0"""
    s["sized"] = f"""
        SELECT * FROM ({s["listed"]}) _l WHERE size_usd IS NOT NULL"""
    # d: one line per security, the campaign's rule.
    s["isin"] = isin_dedup_sql(s["sized"], t, offset_days, min_leg_price,
                               window_days=ENTRY_WINDOW_DAYS, prices_table="entry_px")
    # e: one line per company: buyable at entry, then the shorter symbol, the larger size, the symbol.
    s["company"] = f"""
        SELECT * EXCLUDE (_co_rn) FROM (
            SELECT d.*, ROW_NUMBER() OVER (
                PARTITION BY d.company
                ORDER BY CASE WHEN px.adjClose > 0 AND px.adjClose >= {float(min_leg_price)} THEN 0 ELSE 1 END,
                         LENGTH(d.symbol), d.size_usd DESC, d.symbol) AS _co_rn
            FROM ({s["isin"]}) d
            LEFT JOIN ({_entry_bar_sql(year, offset_days)}) px ON px.symbol = d.symbol
        ) _e WHERE _co_rn = 1"""
    # Cost cap: the latest FY filing in the lag window, in USD at the formation year-end rate.
    cap = f"""
            SELECT symbol, marketCap / per_usd AS cap_usd FROM (
                SELECT m.symbol, m.marketCap, f.per_usd, ROW_NUMBER() OVER (
                    PARTITION BY m.symbol
                    ORDER BY m.filing_epoch DESC, m.marketCap DESC NULLS LAST,
                             m.reportedCurrency DESC NULLS LAST) AS rn
                FROM metrics_cache m
                LEFT JOIN fx_year f ON f.yr = {fy} AND f.ccy = m.reportedCurrency
                WHERE m.filing_epoch >= {cap_lo}
                  AND m.filing_epoch <= {cap_hi}
            ) _m WHERE rn = 1"""
    # f: the cut runs after d and e, so a collapsed line frees its slot.
    s["final"] = f"""
        SELECT {int(year)} AS yr, r.symbol, r.sector, r.company, r.size_usd, k.cap_usd
        FROM ({_cut_sql(s["company"], top_n)}) r
        LEFT JOIN ({cap}) k ON k.symbol = r.symbol"""
    return s


def candidates_sql(year, offset_days, min_leg_price, top_n, min_days):
    """Local SELECT of trading year `year`'s candidates: yr, symbol, sector, company, size_usd, cap_usd.

    Reads fetch_universe()'s tables; no ORDER BY. A line qualifies when it has min_days bars in the
    formation year with the last one inside the year's final LISTED_GRACE_DAYS, a positive median
    daily close x volume and a USD rate. One line per ISIN (isin_dedup_sql), then per company, then
    the top_n of each sector by size_usd. cap_usd is NULL without a filing or a rate.
    """
    return _stages(year, offset_days, min_leg_price, top_n, min_days)["final"]


def build_candidates(con, years, offset_days, min_leg_price, top_n, min_days):
    """Create candidates(yr, symbol, sector, company, size_usd, cap_usd), one list per trading year.

    Returns {year: counts}: listed (rules a and b), no_fx, same_isin and same_company (lines each
    step removed), eligible (lines before the cut), candidates, sectors, min_size_usd (smallest
    admitted size), sector_cut_usd ({sector: size of its top_n-th line}, sectors that reached the
    cut only) and no_cost_cap (candidates with a NULL cap_usd).
    """
    if offset_days not in (0, 1):
        raise ValueError("entry_px covers offset_days 0 and 1 only")
    years = sorted(int(y) for y in years)
    con.execute("DROP TABLE IF EXISTS candidates")
    con.execute("CREATE TABLE candidates(yr INTEGER, symbol VARCHAR, sector VARCHAR, company VARCHAR,"
                " size_usd DOUBLE, cap_usd DOUBLE)")
    n = lambda sql: con.execute(f"SELECT COUNT(*) FROM ({sql}) _n").fetchone()[0]
    counts = {}
    for y in years:
        con.execute(f"""
            INSERT INTO candidates SELECT {y}, symbol, sector, company, size_usd, cap_usd
            FROM ({candidates_sql(y, offset_days, min_leg_price, top_n, min_days)}) _c ORDER BY symbol""")
        s = _stages(y, offset_days, min_leg_price, top_n, min_days)
        listed, sized, isin, eligible = n(s["listed"]), n(s["sized"]), n(s["isin"]), n(s["company"])
        cut = con.execute(f"""
            SELECT sector, size_usd FROM ({_ranked_sql(s["company"])}) _r
            WHERE rnk = {int(top_n)} ORDER BY sector""").fetchall()
        got = con.execute("SELECT COUNT(*), COUNT(DISTINCT sector), MIN(size_usd), COUNT(*) - COUNT(cap_usd)"
                          " FROM candidates WHERE yr = ?", [y]).fetchone()
        counts[y] = {"listed": listed, "no_fx": listed - sized, "same_isin": sized - isin,
                     "same_company": isin - eligible, "eligible": eligible, "candidates": got[0],
                     "sectors": got[1], "min_size_usd": got[2], "sector_cut_usd": dict(cut),
                     "no_cost_cap": got[3]}
    return counts


def check_pairs(con, year, pairs):
    """Raise unless every (sym_a, sym_b, correlation) joins two `year` candidates of two companies
    with a finite correlation. Returns 0, the same_company_pairs count a results JSON records."""
    company = dict(con.execute("SELECT symbol, company FROM candidates WHERE yr = ?", [int(year)]).fetchall())
    for a, b, corr in pairs:
        if a not in company or b not in company:
            raise AssertionError(f"pair leg outside the {year} candidates: {a} / {b}")
        if company[a] == company[b]:
            raise AssertionError(f"same-company pair in {year}: {a} / {b} ({company[a]})")
        if corr is None or not math.isfinite(corr):
            raise AssertionError(f"pair without a finite correlation in {year}: {a} / {b}")
    return 0


def fetch_prices(cr, con, symbols, bench_symbols, verbose=False):
    """Daily bars from PRICES_FROM for `symbols` (None: every candidate) and the benchmarks. Returns rows.

    Creates prices_cache(symbol, trade_date DATE, trade_epoch BIGINT, adjClose), indexed on
    (symbol, trade_date) and passed through remove_price_oscillations.
    """
    if symbols is None:
        symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM candidates").fetchall()]
    count = _fetch_in(cr, con, "prices_cache",
                      "symbol VARCHAR, trade_date DATE, trade_epoch BIGINT, adjClose DOUBLE",
                      lambda in_list: f"""
        SELECT symbol, CAST(date AS DATE) AS trade_date, CAST(dateEpoch AS BIGINT) AS trade_epoch, adjClose
        FROM stock_eod
        WHERE symbol IN ({in_list}) AND date >= '{PRICES_FROM}' AND adjClose > 0""",
                      sorted(set(symbols) | set(bench_symbols or ())), verbose=verbose,
                      limit=10_000_000, timeout=600, memory_mb=16384, threads=6)
    if count:
        con.execute("CREATE INDEX idx_pc_sym_date ON prices_cache(symbol, trade_date)")
        remove_price_oscillations(con, verbose=verbose)
    return count


def load_delist_bounds(cr):
    """{symbol: delisted date} from delisted_companies: LtpExits' query, retries and latest-row rule."""
    return LtpExits(cr, None).delisted


def _leg_bound(symbol, anchor, on_or_before, bounds):
    """(last date a leg's bars count, how its delisting row was used): LtpExits' rule for one leg."""
    dd = bounds.get(symbol)
    if dd is None:
        return on_or_before, "none"
    if dd + timedelta(days=LTP_EARLIER_HOLDER_DAYS) >= anchor:
        cap = dd + timedelta(days=LTP_DELIST_GRACE_DAYS)
        return min(on_or_before, cap), "applied" if cap < on_or_before else "not_binding"
    return on_or_before, "ignored_predates_entry"   # an earlier holder of the ticker


def last_common_bar(con, a, b, after, on_or_before, bounds, anchors=None):
    """(bar date, price_a, price_b) of the last day both legs traded, or None.

    The bar is dated after `after` (the later of the two entry bars) and on or before
    `on_or_before` (the exit date). bounds is load_delist_bounds()'s map: a leg's bars stop
    counting LTP_DELIST_GRACE_DAYS after its delisting, unless the delisting is more than
    LTP_EARLIER_HOLDER_DAYS before the leg's anchor (anchors[symbol], else `after`).
    Reads con's prices_cache (trade_epoch schema).
    """
    anchors = anchors or {}
    hi = min(_leg_bound(s, anchors.get(s, after), on_or_before, bounds)[0] for s in (a, b))
    if hi <= after:
        return None
    lo_epoch, hi_epoch = utc_epoch(after + timedelta(days=1)), utc_epoch(hi + timedelta(days=1))
    leg = ("(SELECT trade_epoch, MAX(adjClose) AS px FROM prices_cache"
           " WHERE symbol = ? AND adjClose > 0 AND trade_epoch >= ? AND trade_epoch < ? GROUP BY trade_epoch)")
    row = con.execute(f"""
        SELECT x.trade_epoch, x.px, y.px
        FROM {leg} x
        JOIN {leg} y ON y.trade_epoch = x.trade_epoch
        ORDER BY x.trade_epoch DESC, y.trade_epoch DESC LIMIT 1
    """, [a, lo_epoch, hi_epoch, b, lo_epoch, hi_epoch]).fetchone()
    return (utc_date(row[0]), float(row[1]), float(row[2])) if row else None


def _iso(d):
    return d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]


class PairLtp(LtpExits):
    """Pair-level last-traded-price exits: a pair is one position, closed at its last common bar.

    LtpExits fills one leg at a time, which would price a dead leg in June and its partner in
    December. This keeps LtpExits' results block (results_block, read unchanged by
    scripts/gate_ltp.py) and counts everything in pairs. bounds: load_delist_bounds()'s map.
    pairs-fundamentals uses fill(); pairs-zscore and pairs-multi-pair use bound() and close().
    """

    SOURCE = "last common bar of the pair"

    def __init__(self, con, bounds, verbose=False):   # no query: LtpExits.__init__ is not called
        self.con, self.delisted, self.verbose = con, dict(bounds or {}), verbose
        self.missing = self.filled = 0
        self.fills = []

    def _delisting(self, legs, anchors, on_or_before):
        """([delisted date or None per leg], [how each leg's delisting row was used])."""
        return ([_iso(self.delisted[s]) if s in self.delisted else None for s in legs],
                [_leg_bound(s, anchors[s], on_or_before, self.delisted)[1] for s in legs])

    def fill(self, year, pairs, entry, exits, exit_date, min_pairs):
        """{(a, b): (price_a, price_b)} at the last common bar, for entered pairs missing an exit price.

        pairs: the year's entered (a, b) pairs in the caller's order. entry: {symbol: (price,
        bar epoch)}, get_prices(with_epochs=True). exits: {symbol: price}; a leg absent or None
        has no exit bar. Fewer than min_pairs entered pairs is a cash year: nothing is requested
        or counted. A pair with no common bar after both entry bars stays dropped.
        """
        if len(pairs) < min_pairs:
            return {}
        out = {}
        for a, b in pairs:
            gone = [s for s in (a, b) if exits.get(s) is None]
            if not gone:
                continue
            self.missing += 1
            anchors = {s: utc_date(entry[s][1]) for s in (a, b)}
            after = max(anchors.values())
            got = last_common_bar(self.con, a, b, after, exit_date, self.delisted, anchors=anchors)
            if got is None:
                continue
            bar, price_a, price_b = got
            if not after < bar <= exit_date:
                raise AssertionError(f"last common bar outside its window: {a} {b} {after} {bar} {exit_date}")
            out[(a, b)] = (price_a, price_b)
            self.filled += 1
            delisted, used = self._delisting((a, b), anchors, exit_date)
            self.fills.append({"pair": [a, b], "year": int(year),
                               "entry_bars": [_iso(anchors[a]), _iso(anchors[b])],
                               "entry_prices": [entry[a][0], entry[b][0]],
                               "last_common_bar": _iso(bar), "prices": [price_a, price_b],
                               "missing_leg": gone, "delisted_date": delisted, "delisting_bound": used,
                               "exit_date": _iso(exit_date)})
        if self.verbose and self.missing:
            print(f"      LTP: {len(out)} pairs closed at their last common bar")
        return out

    def provenance(self):
        """Counts (in pairs) and every fill, for the results JSON."""
        return {"source": self.SOURCE, "delist_grace_days": LTP_DELIST_GRACE_DAYS,
                "missing_exits": self.missing, "ltp_filled": self.filled,
                "still_missing": self.missing - self.filled, "fills": self.fills}

    def bound(self, a, b, anchor, on_or_before):
        """Last date a pair's bars count in the year that starts at `anchor`: the bars query's upper bound."""
        return min(_leg_bound(s, anchor, on_or_before, self.delisted)[0] for s in (a, b))

    def close(self, a, b, entry_date, last_bar, trading_start, trading_end):
        """Exit type of a position still open at the pair's last bar of the year.

        "last_bar" (recorded as an early close) when that bar is more than YEAR_END_DAYS before
        trading_end, else "year_end".
        """
        if (trading_end - last_bar).days <= YEAR_END_DAYS:
            return "year_end"
        delisted, used = self._delisting((a, b), {a: trading_start, b: trading_start}, trading_end)
        self.fills.append({"pair": [a, b], "year": trading_end.year, "entry_date": _iso(entry_date),
                           "last_common_bar": _iso(last_bar), "delisted_date": delisted,
                           "delisting_bound": used})
        return "last_bar"

    def closes_block(self, exit_method):
        """Results-JSON block of a topic whose positions always close at the last common bar."""
        return {"exit_method": exit_method, "source": self.SOURCE, "delist_grace_days": LTP_DELIST_GRACE_DAYS,
                "early_closes": len(self.fills), "fills": self.fills}
