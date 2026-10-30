"""Shared data loading utilities for backtesting.

Extracted from qarp/backtest.py and piotroski/backtest.py to avoid duplication.
Handles API queries, DuckDB loading, price lookups, and rebalance date generation.

Usage:
    from data_utils import query_parquet, get_prices, generate_rebalance_dates

    # Load API data into DuckDB
    count = query_parquet(client, sql, con, "prices_cache")

    # Get prices at a rebalance date
    prices = get_prices(con, ["AAPL", "MSFT"], date(2024, 1, 1))

    # Generate rebalance dates
    dates = generate_rebalance_dates(2000, 2025, "semi-annual")
"""

import json
import math
import os
import tempfile
from datetime import date, timedelta


# Regional benchmark ETFs - iShares country ETFs (trade on US exchanges)
# Used for cross-border performance comparison. Silently skipped if no price data.
REGIONAL_BENCHMARKS = {
    "BSE": "INDA", "NSE": "INDA",         # India
    "XETRA": "EWG", "FSX": "EWG",         # Germany
    "SHZ": "FXI", "SHH": "FXI",           # China
    "HKSE": "EWH",                         # Hong Kong
    "JPX": "EWJ",                          # Japan
    "KSC": "EWY", "KOE": "EWY",           # South Korea
    "ASX": "EWA",                          # Australia
    "LSE": "EWU",                          # UK
    "TSX": "EWC", "TSXV": "EWC",          # Canada
    "SAO": "EWZ",                          # Brazil
    "SGX": "EWS",                          # Singapore
    "MEX": "EWW", "BMV": "EWW",           # Mexico (FMP profile code is MEX; BMV matches no rows, B039)
    "JSE": "EZA",                          # South Africa
    "SAU": "KSA",                          # Saudi Arabia
    # European exchanges -> Vanguard FTSE Europe
    "PAR": "VGK", "AMS": "VGK", "BRU": "VGK", "MIL": "VGK",
    "STO": "VGK", "OSL": "VGK", "CPH": "VGK", "HEL": "VGK",
    "SIX": "EWL",                          # Switzerland
}

# Factor benchmark ETFs
FACTOR_BENCHMARKS = {
    "value": "IWD",      # Russell 1000 Value
    "quality": "QUAL",   # iShares MSCI USA Quality Factor
    "small_cap": "IWM",  # Russell 2000
    "momentum": "MTUM",  # iShares MSCI USA Momentum Factor
}


# Local currency index benchmarks (same currency as portfolio stocks)
# These are the correct benchmarks for measuring alpha. SPY is kept as secondary.
LOCAL_INDEX_BENCHMARKS = {
    "BSE": "^BSESN", "NSE": "^BSESN",              # Sensex (INR, 1979+)
    "JPX": "^N225",                                   # Nikkei 225 (JPY, 1970+)
    "LSE": "^FTSE",                                   # FTSE 100 (GBP, 1984+)
    "XETRA": "^GDAXI", "FSX": "^GDAXI",             # DAX (EUR, 1987+)
    "HKSE": "^HSI",                                   # Hang Seng (HKD, 1986+)
    "KSC": "^KS11", "KOE": "^KS11",                  # KOSPI (KRW, 1980+)
    "TAI": "^TWII", "TWO": "^TWII",                  # Taiwan Weighted (TWD, 1997+)
    "SGX": "^STI", "SES": "^STI",                     # Straits Times (SGD, 1987+)
    "ASX": "^AXJO",                                   # ASX 200 (AUD, 1992+)
    "NYSE": "SPY", "NASDAQ": "SPY", "AMEX": "SPY",         # SPY ETF (USD, dividend-adjusted)
    "SAO": "^BVSP",                                   # Bovespa (BRL)
    "TSX": "^GSPTSE", "TSXV": "^GSPTSE",             # TSX Composite (CAD)
    # "JNB": "^J203.JO" — no price data in FMP stock_eod. Falls back to SPY.
    "SHH": "000001.SS",                                # Shanghai Composite (CNY)
    # SHZ: 399001.SZ has no price data in FMP stock_eod. Falls back to SPY.
    "STO": "^OMXS30",                                   # OMX Stockholm 30 (SEK, 1986+)
    "SIX": "^SSMI",                                     # Swiss Market Index (CHF, 1990+)
    "OSL": "^OSEAX",                                    # Oslo All-share GR, dividends reinvested (NOK, 2013+)
    "SET": "^SET.BK",                                   # SET Index (THB, 1982+)
}

# Human-readable names for local benchmarks
LOCAL_INDEX_NAMES = {
    "^BSESN": "Sensex", "^NSEI": "Nifty 50",
    "^N225": "Nikkei 225", "^FTSE": "FTSE 100",
    "^GDAXI": "DAX", "^HSI": "Hang Seng",
    "^KS11": "KOSPI", "^TWII": "TAIEX",
    "^STI": "STI", "^AXJO": "ASX 200",
    "SPY": "S&P 500", "^GSPC": "S&P 500", "^BVSP": "Bovespa",
    "^GSPTSE": "TSX Composite", "^J203.JO": "JSE All Share",
    "000001.SS": "SSE Composite", "399001.SZ": "SZSE Component",
    "^OMXS30": "OMX Stockholm 30", "^SSMI": "SMI",
    "^OSEAX": "Oslo All Share", "^SET.BK": "SET Index",
}


# Local reporting currency by exchange (matches cash_flow_statement.reportedCurrency)
# Used to filter out foreign-listed ADRs whose parent filings are in a different currency
# than the local listing's market cap, which produces unit-mismatched FCF yields.
LOCAL_CURRENCY = {
    "NYSE": "USD", "NASDAQ": "USD", "AMEX": "USD",
    "TSX": "CAD", "TSXV": "CAD",
    "XETRA": "EUR", "FSX": "EUR",
    "STO": "SEK",
    "LSE": "GBP",
    "JPX": "JPY",
    "HKSE": "HKD",
    "KSC": "KRW", "KOE": "KRW",
    "TAI": "TWD", "TWO": "TWD",
    "SHH": "CNY", "SHZ": "CNY",
    "NSE": "INR", "BSE": "INR",
    "SIX": "CHF",
    "SET": "THB",
    "OSL": "NOK",
    "SAU": "SAR",
    "ASX": "AUD",
    "SAO": "BRL",
    "SGX": "SGD", "SES": "SGD",
    "JNB": "ZAR", "JSE": "ZAR",
    "AMS": "EUR", "PAR": "EUR", "MIL": "EUR", "BME": "EUR",
    "KLS": "MYR",
    "JKT": "IDR",
    "TLV": "ILS",
    "WSE": "PLN",
}

# Home country per exchange, for the opt-in domicile filter.
#
# WHY THIS EXISTS. The default universe is every company LISTED on an exchange,
# which outside the US and Hong Kong is mostly foreign companies' secondary
# lines. Measured on the 2015 XETRA screen: 32 of 36 names were US-domiciled,
# 3 German. The 2020 LSE screen was 46 US / 20 UK. Those listings are also
# barely tradeable: 18.6% of XETRA and 20.8% of LSE daily rows have zero volume
# versus 1.1% on NYSE, with prices frozen for days and then jumping.
#
# Restricting to domicile can invert a published conclusion, so it is OPT-IN
# and every result must say which universe it used. Measured on fcf-yield:
# XETRA +4.01% -> -3.60% and SIX +2.56% -> -1.00%, both flipping sign, while
# JPX, NSE and Canada were unchanged. The effect concentrates in thin European
# markets that cannot fill a 30-stock screen domestically.
EXCHANGE_COUNTRY = {
    "NYSE": ("US",), "NASDAQ": ("US",), "AMEX": ("US",),
    "XETRA": ("DE",), "FSX": ("DE",),
    "LSE": ("GB",),
    "NSE": ("IN",), "BSE": ("IN",),
    "JPX": ("JP",),
    "HKSE": ("HK", "CN"),          # mainland issuers are local to the HK market
    "TSX": ("CA",), "TSXV": ("CA",),
    "SIX": ("CH",),
    "STO": ("SE",),
    "OSL": ("NO",),
    "SET": ("TH",),
    "ASX": ("AU",),
    "SAO": ("BR",),
    "SGX": ("SG",), "SES": ("SG",),
    "JNB": ("ZA",),
    "KSC": ("KR",), "KOE": ("KR",),
    "TAI": ("TW",), "TWO": ("TW",),
    "SHH": ("CN",), "SHZ": ("CN",),
    "MIL": ("IT",),
    "PAR": ("FR",),
    "AMS": ("NL",),
    "BME": ("ES",),
    "WSE": ("PL",),
    "TLV": ("IL",),
    "SAU": ("SA",),
    "KLS": ("MY",),
    "JKT": ("ID",),
}


def domicile_countries(exchanges):
    """ISO country codes for a set of exchanges, or () if any is unknown.

    Returns a tuple suitable for a `country IN (...)` SQL filter. Returns an
    empty tuple when no exchange maps, so callers can skip the filter rather
    than silently screening everything out.
    """
    if not exchanges:
        return ()
    return tuple(sorted({c for e in exchanges for c in EXCHANGE_COUNTRY.get(e, ())}))


def domicile_sql_condition(exchanges):
    """`country IN ('DE')`-style condition, or "" when the filter cannot apply."""
    countries = domicile_countries(exchanges)
    if not countries:
        return ""
    return "country IN (" + ", ".join(f"'{c}'" for c in countries) + ")"


# Topics rerun with LTP exits: the listing rules are ON, not opt-in. XETRA "Germany" books held
# 74-78% foreign lines on average, SIX 22-34%: home country only. STO is deliberately absent:
# profile.country would drop ABB, AstraZeneca, Nokia.
FILTERED_DOMICILE_EXCHANGES = frozenset({"XETRA", "SIX"})

# profile.currency of a market's own lines where it quotes in a sub-unit, not LOCAL_CURRENCY's
# reporting code (profile 2026-10-12; every other campaign exchange matches LOCAL_CURRENCY).
QUOTE_CURRENCY = {"LSE": "GBp", "JNB": "ZAc", "TLV": "ILA"}


def campaign_listing_condition(exchanges):
    """Rerun-campaign listing rule for a leg, or "" when none applies.

    XETRA/SIX legs: domicile_sql_condition(). LSE legs: pence (GBp) lines only, which drops
    the 0XXX.L foreign lines (quoted in their home currency) and USD/EUR receipts but keeps
    LSE-primary foreign-registered names (Experian, Coca-Cola HBC, Wizz Air). 'GBP' is not
    pence: 569 lines, 8 of them non-fund and 7 of those dead (profile 2026-10-12).
    """
    if exchanges and set(exchanges) <= FILTERED_DOMICILE_EXCHANGES:
        return domicile_sql_condition(exchanges)
    if exchanges and set(exchanges) == {"LSE"}:
        return f"currency IN ('{QUOTE_CURRENCY['LSE']}')"
    return ""


def campaign_listing_label(exchanges):
    """campaign_listing_condition() as results record it: "country DE", "currency GBp", or None."""
    return campaign_listing_condition(exchanges).replace(" IN (", " ").replace("'", "").rstrip(")") or None


def leg_quote_currency(exchanges):
    """profile.currency of a leg's home lines (QUOTE_CURRENCY, else LOCAL_CURRENCY); None if mixed/unknown."""
    ccys = {QUOTE_CURRENCY.get(e, LOCAL_CURRENCY.get(e)) for e in exchanges or ()}
    return ccys.pop() if len(ccys) == 1 else None


def load_isin_map(client, con, exchanges, sym_filter_sql, verbose=False):
    """Create isin_dedup_sql()'s `isin_map` for a leg: rerun-campaign topics de-duplicate every leg.

    home_ccy flags lines quoted in leg_quote_currency(); it is NULL on a mixed/unknown leg,
    which drops that tie-break term.
    """
    ccy = leg_quote_currency(exchanges)
    home = f"currency = '{ccy}'" if ccy else "CAST(NULL AS BOOLEAN)"
    n = query_parquet(client, f"SELECT symbol, isin, isActivelyTrading, {home} AS home_ccy FROM profile"
                              f" WHERE {sym_filter_sql}", con, "isin_map", verbose=verbose)
    if not n:  # query_parquet's empty-result placeholder has none of the columns
        con.execute("DROP TABLE isin_map")
        con.execute("CREATE TABLE isin_map(symbol VARCHAR, isin VARCHAR, isActivelyTrading BOOLEAN,"
                    " home_ccy BOOLEAN)")
    return n


def isin_dedup_sql(candidates_sql, target_date, offset_days, min_entry_price,
                   window_days=10, isin_table="isin_map", prices_table="prices_cache", twin_key=False):
    """Wrap a screen's candidate SELECT so one security keeps one row.

    A security is its normalised ISIN, else its symbol: a symbol fanned out by a join keeps
    one row, and lines of different symbols with no ISIN never merge; share classes have
    their own ISINs and stay. Rule per security: the line buyable at entry wins (get_prices'
    bar for the same window passes entry_usable), then a line profile still marks
    isActivelyTrading (a retired ticker's bars stop at the rename), then the line quoted in
    the leg's home currency (TSX HOM-UN.TO over HOM-U.TO, HKSE 9988.HK over 89988.HK), then
    the alphabetically first symbol. `candidates_sql` exposes `symbol` and has no ORDER
    BY/LIMIT: rank and LIMIT go outside, so a collapsed twin frees its slot for the next
    name. `isin_table` is load_isin_map()'s (symbol, isin, isActivelyTrading, home_ccy).

    twin_key=True (data check N, after mark_nvdr_twins): a security is COALESCE(sec_key, ISIN,
    symbol), so a SET NVDR line shares its ordinary line's key, and the ordinary line ranks
    before the NVDR right after the buyable term. False returns the SQL this function always did.
    """
    if twin_key:
        return (f"\n        SELECT * EXCLUDE (_isin_rn, _tw_key, _tw_nvdr) FROM ("
                f"{_twin_ranked_sql(candidates_sql, target_date, offset_days, min_entry_price, window_days, isin_table, prices_table)}"
                f"\n        ) _d WHERE _isin_rn = 1")
    shifted = target_date + timedelta(days=offset_days)
    lo, hi = utc_epoch(shifted), utc_epoch(shifted + timedelta(days=window_days))
    return f"""
        SELECT * EXCLUDE (_isin_rn) FROM (
            SELECT _c.*, ROW_NUMBER() OVER (
                PARTITION BY COALESCE(_i.isin, _c.symbol)
                ORDER BY CASE WHEN _px.adjClose > 0 AND _px.adjClose >= {float(min_entry_price)}
                              THEN 0 ELSE 1 END,
                         CASE WHEN _i.live THEN 0 ELSE 1 END, CASE WHEN _i.home THEN 0 ELSE 1 END,
                         _c.symbol) AS _isin_rn
            FROM ({candidates_sql}) _c
            LEFT JOIN (SELECT symbol, MIN(NULLIF(UPPER(TRIM(isin)), '')) AS isin,
                              BOOL_OR(isActivelyTrading) AS live, BOOL_OR(home_ccy) AS home
                       FROM {isin_table} GROUP BY symbol) _i ON _i.symbol = _c.symbol
            LEFT JOIN (SELECT symbol, adjClose FROM {prices_table}
                       WHERE trade_epoch >= {lo} AND trade_epoch <= {hi}
                       QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol
                           ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST) = 1) _px
                ON _px.symbol = _c.symbol
        ) _d WHERE _isin_rn = 1"""


def get_local_currency(exchanges):
    """Get the reportedCurrency for a set of exchanges.

    Returns the local currency string for filtering cash_flow_statement
    by reportedCurrency. Single-currency presets only; mixed-currency
    exchange sets return None (caller should not apply the filter).

    Args:
        exchanges: list[str] or None - exchange codes

    Returns:
        str or None - currency code, or None if mixed/unknown
    """
    if not exchanges:
        return None
    currencies = {LOCAL_CURRENCY.get(ex) for ex in exchanges if LOCAL_CURRENCY.get(ex)}
    if len(currencies) == 1:
        return currencies.pop()
    return None


def get_local_benchmark(exchanges):
    """Get the local currency index symbol for a set of exchanges.

    For single-region exchanges, returns the local index.
    For mixed regions or unknown exchanges, falls back to SPY.

    Args:
        exchanges: list[str] or None - exchange codes

    Returns:
        tuple(str, str) - (benchmark_symbol, benchmark_name)
    """
    if not exchanges:
        return "SPY", "S&P 500"

    # Collect unique benchmark symbols for all exchanges
    symbols = set()
    for ex in exchanges:
        sym = LOCAL_INDEX_BENCHMARKS.get(ex)
        if sym:
            symbols.add(sym)

    if len(symbols) == 1:
        sym = symbols.pop()
        return sym, LOCAL_INDEX_NAMES.get(sym, sym)

    # Mixed regions or unknown: fall back to SPY
    return "SPY", "S&P 500"


def get_benchmark_return(con, benchmark_symbol, entry_date, exit_date,
                         offset_days=0, window_days=10):
    """Get benchmark return for a period.

    Convenience wrapper around get_prices() for the common 8-line benchmark
    lookup pattern repeated in every strategy.

    Args:
        con: duckdb.Connection with prices_cache table
        benchmark_symbol: str - e.g. "SPY", "^BSESN"
        entry_date: date - period start
        exit_date: date - period end
        offset_days: int - days to shift for MOC execution
        window_days: int - price lookup window

    Returns:
        float or None - benchmark return for the period
    """
    entry = get_prices(con, [benchmark_symbol], entry_date,
                       window_days=window_days, offset_days=offset_days)
    exit_ = get_prices(con, [benchmark_symbol], exit_date,
                       window_days=window_days, offset_days=offset_days)
    ep = entry.get(benchmark_symbol)
    xp = exit_.get(benchmark_symbol)
    if ep and xp and ep > 0:
        return (xp - ep) / ep
    return None


def utc_epoch(d):
    """UTC-midnight epoch of a date. Bar epochs (stock_eod.dateEpoch) are UTC midnight, so every
    epoch bound must be built with this, never with a naive datetime (which moves with the
    machine's timezone and daylight saving)."""
    from datetime import datetime, timezone
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def utc_date(epoch):
    """Date of a UTC epoch."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).date()


def filter_by_liquidity(con, symbols, target_date, lookback_days=90,
                        min_avg_turnover=0):
    """Filter symbols by average daily turnover.

    Requires prices_cache to have 'volume' column. If volume is missing or
    min_avg_turnover is 0, returns all symbols unchanged (graceful degradation).

    Args:
        con: duckdb.Connection with prices_cache table
        symbols: list[str] - candidate symbols
        target_date: date - compute turnover up to this date
        lookback_days: int - days of history to average (default 90)
        min_avg_turnover: float - minimum avg(volume * adjClose) to pass.
            0 = no filter (default). Reasonable values:
            US: 10_000_000, India: 5_000_000, Japan: 500_000_000

    Returns:
        tuple(list[str], list[str]) - (passed, filtered_out)
    """
    if not symbols or min_avg_turnover <= 0:
        return list(symbols), []

    # Check if volume column exists
    try:
        con.execute("SELECT volume FROM prices_cache LIMIT 0")
    except Exception:
        return list(symbols), []

    target_epoch = utc_epoch(target_date)
    start_epoch = utc_epoch(target_date - timedelta(days=lookback_days))
    sym_list = ",".join("'{}'".format(s.replace("'", "''")) for s in symbols)

    try:
        rows = con.execute(f"""
            SELECT symbol, AVG(volume * adjClose) as avg_turnover
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_epoch >= {start_epoch}
              AND trade_epoch <= {target_epoch}
              AND volume > 0 AND adjClose > 0
            GROUP BY symbol
        """).fetchall()
    except Exception:
        return list(symbols), []

    turnover_map = {r[0]: r[1] for r in rows}
    passed = []
    filtered = []
    for s in symbols:
        t = turnover_map.get(s, 0)
        if t >= min_avg_turnover:
            passed.append(s)
        else:
            filtered.append(s)

    return passed, filtered


def query_parquet(client, sql, con, table_name, verbose=False, limit=1000000, timeout=300,
                  memory_mb=None, threads=None, max_retries=3):
    """Query API as parquet, load directly into DuckDB. Returns row count.

    Args:
        client: CetaResearch client instance
        sql: str - SQL query to execute
        con: duckdb.Connection
        table_name: str - DuckDB table name to create
        verbose: bool - print debug info
        limit: int - max rows
        timeout: int - query timeout in seconds
        memory_mb: int or None - server-side memory (e.g. 16384 for backtests)
        threads: int or None - server-side threads (e.g. 6 for backtests)
        max_retries: int - max retries on corrupted parquet (default 3)

    Returns:
        int - number of rows loaded
    """
    # Retry loop for handling corrupted parquet downloads (transient FMP download errors)
    for attempt in range(max_retries):
        try:
            parquet_bytes = client.query(sql, format="parquet", limit=limit, timeout=timeout,
                                         verbose=verbose, memory_mb=memory_mb, threads=threads)
        except Exception as e:
            # cr_client rejects a truncated download ("Corrupted parquet file: ...") before DuckDB sees it
            if "Corrupted parquet file" in str(e) and attempt < max_retries - 1:
                import time
                time.sleep(2 * (attempt + 1))
                continue
            raise
        if not parquet_bytes:
            con.execute(f"CREATE TABLE {table_name}(dummy INTEGER)")
            con.execute(f"DELETE FROM {table_name}")
            return 0

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            f.write(parquet_bytes)
            tmp_path = f.name

        try:
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{tmp_path}')")
            row_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            os.unlink(tmp_path)
            return row_count
        except Exception as e:
            # Clean up tempfile before potentially retrying
            try:
                os.unlink(tmp_path)
            except:
                pass

            # Check if this is a corrupted parquet error
            error_str = str(e)
            is_corrupted = "No magic bytes" in error_str or "Invalid Input Error" in error_str

            if is_corrupted and attempt < max_retries - 1:
                wait_time = 2 * (attempt + 1)  # 2s, 4s, 6s
                if verbose:
                    print(f"  ⚠ Corrupted parquet download (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                import time
                time.sleep(wait_time)
                continue

            # Not a retryable error, or max retries reached
            raise


def entry_usable(entry_price, min_entry_price=1.0):
    """True when a name could be BOUGHT on the ENTRY leg of the period.

    This is exactly filter_returns' entry-side test and nothing else: an entry
    price that exists, is positive, and clears the floor. The floor is compared
    to LOCAL-currency adjClose (GBp, ZAc, agorot included) and is a bad-price
    guard, not a size rule; see METHODOLOGY.md. It says nothing about
    the exit price or the realised return, because neither is knowable on the
    entry date. That is the whole point -- a cash decision may only read this.

    "Entry leg", not "true on the rebalance date": the entry-price map this
    reads is not STRICTLY rebalance-dated, and the distinction is worth keeping
    honest in a repo whose results are published.
      - get_prices searches FORWARD up to window_days=10, so a name whose first
        trade is nine days after the rebalance still counts as buyable.
      - remove_price_oscillations deletes rows using LEAD(adjClose, 1) and
        LEAD(adjClose, 2), so whether a given day's price survives into the map
        depends on the two bars that follow it.
    Both are PRE-EXISTING and byte-identical on main -- filter_returns read the
    same map -- so neither is a regression here, and the forward window pushes
    periods from cash to INVESTED, which is the conservative direction for this
    particular guard. They are bounded and logged in DATA_QUALITY_ISSUES.md. The
    claim this docstring makes is the one that literally holds: the cash rule
    reads the entry leg only, never the exit leg.
    """
    return (entry_price is not None
            and entry_price > 0
            and entry_price >= min_entry_price)


def entry_buyable(symbol_returns, min_entry_price=1.0):
    """How many names were buyable at entry, over filter_returns' own input.

    Pass the SAME min_entry_price the matching filter_returns call uses, or a
    topic's cash rule and its price filter disagree. The 1.0 default matches
    filter_returns' default, so the topics that call filter_returns with only
    `verbose=` inherit the same floor here.
    """
    return sum(1 for _sym, ep, _xp, _mcap in symbol_returns
               if entry_usable(ep, min_entry_price))


def entry_buyable_prices(symbols, entry_prices, min_entry_price=1.0):
    """The same count, from the screened symbols and the entry-price map.

    For callers that never build the 4-tuple list (qarp, low-pe), and for the
    nine topics that build it with the EXIT price already filtered out
    (capex-efficiency, fcf-yield, graham-net-net, graham-timing,
    interest-coverage, ocf-growth, owner-earnings, sector-momentum,
    sector-rotation). Counting those pre-filtered lists would still be
    exit-conditioned, which is the defect this whole change is about.
    """
    return sum(1 for s in symbols
               if entry_usable(entry_prices.get(s), min_entry_price))


# ---------------------------------------------------------------------------
# THE ENTRY FUNNEL, the per-period audit trail for the cash rule.
#
# B005 corrected WHICH count the stock floor is compared against. It did not
# make the decision auditable: a committed cash period records
# `stocks_held: 0` and nothing else, so PRE-SCREEN cash (the screen returned
# too few names) and POST-PRICE cash (the screen was fine, the guard fired) are
# indistinguishable in an artifact. Five of the six topics where the guard
# actually fires serialize only the AGGREGATE cash_periods, which lumps the two
# together, so the class could not be measured from published output at all --
# only by git forensics or a re-run.
#
# Every period record of every floor topic therefore carries three more keys,
# written immediately after `stocks_held`:
#
#   "screened"      int|None  names the SCREEN returned on the rebalance date.
#                             None = the screen never ran (a market-timing
#                             signal was off, so there was nothing to screen).
#   "entry_buyable" int|None  how many of those had a usable ENTRY price, i.e.
#                             entry_buyable()/entry_buyable_prices() over the
#                             screened names. None = prices were never fetched.
#   "min_stocks"    int       the floor this topic's cash rule compares against
#                             (MIN_STOCKS, or MIN_PORTFOLIO_STOCKS for the
#                             sector twins). Written as the CONSTANT, never a
#                             literal, so it cannot drift from the rule.
#
# All three are ENTRY-KNOWABLE. None of them may be computed from `clean` /
# `returns` / `held`, which are the post-filter books and are exit-conditioned;
# that is the B005 defect, and putting it inside the audit field would be worse
# than not having the field. Gate 13 in scripts/verify_floor_guard.py enforces
# it statically.
#
# `min_stocks` is what makes the record decodable, and it is not redundant.
# Correcting the guard made a ZERO-SURVIVOR invested period reachable (every
# name buyable at entry, none surviving to the exit side -- see S6 in
# scripts/test_floor_guard.py), and that period also records `stocks_held: 0`.
# Without the floor in the record, it is indistinguishable from guard-fired
# cash. The floor also VARIES across the 66 topics -- 5, 8 and 10 -- so no
# scanner can supply one itself.
#
# Ordering invariant, true by construction because each stage is a subset of
# the one above it:  stocks_held <= entry_buyable <= screened.
# Null monotonicity: `screened is None` implies `entry_buyable is None`.
# ---------------------------------------------------------------------------

CASH_NEVER_SCREENED = "cash-never-screened"
CASH_PRE_SCREEN = "cash-pre-screen"
CASH_POST_PRICE = "cash-post-price"
INVESTED = "invested"


def period_state(record):
    """Decode one period record into its funnel state. The canonical reader.

    Returns one of the four constants above, or None when the record predates
    the funnel fields (the committed corpus does -- nothing here was re-run, so
    every scanner over it must treat the fields as OPTIONAL).

    `INVESTED` covers a zero-survivor period: the strategy entered, and every
    name it bought lost its exit price or tripped max_single_return. That is a
    realised 0.0%, NOT a cash period, and telling the two apart is the reason
    `min_stocks` is in the record.
    """
    if "entry_buyable" not in record or "min_stocks" not in record:
        return None
    buyable, floor = record["entry_buyable"], record["min_stocks"]
    if buyable is None:
        return (CASH_NEVER_SCREENED if record.get("screened") is None
                else CASH_PRE_SCREEN)
    return CASH_POST_PRICE if buyable < floor else INVESTED


def filter_returns(symbol_returns, min_entry_price=1.0, max_single_return=2.0, verbose=False):
    """Filter individual stock returns for data quality.

    Drops a name for five reasons in two classes. The class matters: only the
    ENTRY-KNOWABLE ones may decide whether the strategy holds cash.

    ENTRY-KNOWABLE (true on the rebalance date -- see entry_usable()):
    - entry price missing
    - entry price <= 0
    - entry price below min_entry_price (artifact guard on LOCAL-currency
      adjClose, sub-units included: GBp, ZAc, agorot; not a size or US$ rule)

    EXIT-CONDITIONED (not knowable until the period ends):
    - exit price missing (delisting, acquisition, coverage gap)
    - realised return above max_single_return (price artifacts, symbol
      reassignments). UPSIDE ONLY: a -99% return is kept.

    NOTE: `skipped` is not a complete drop log. The three conditions on the
    first `continue` report nothing, so len(clean) + len(skipped) is strictly
    less than len(symbol_returns) whenever a name lacks an entry or exit price.
    Do not reconstruct the input population from the two return values.

    Args:
        symbol_returns: list of (symbol, entry_price, exit_price, market_cap) tuples
        min_entry_price: float - minimum entry price to include, in the
            exchange's own price units (default 1.0)
        max_single_return: float - maximum return to include (default 2.0 = 200%);
            None keeps every return (no upper test; the data-checks topics pass it)
        verbose: bool - print skipped stocks

    Returns:
        tuple(clean_returns, skipped) where:
        - clean_returns: list of (symbol, raw_return, market_cap) tuples
        - skipped: list of skip reason strings (see NOTE -- not every drop)
    """
    clean = []
    skipped = []
    for sym, ep, xp, mcap in symbol_returns:
        if ep is None or xp is None or ep <= 0:
            continue
        if ep < min_entry_price:
            skipped.append(f"{sym}(price=${ep:.2f})")
            continue
        raw_ret = (xp - ep) / ep
        if max_single_return is not None and raw_ret > max_single_return:
            skipped.append(f"{sym}(ret={raw_ret*100:.0f}%)")
            continue
        clean.append((sym, raw_ret, mcap))

    if skipped and verbose:
        print(f"      Skipped (data quality): {', '.join(skipped)}")

    return clean, skipped


def load_into_duckdb(con, table_name, rows, schema):
    """Load list of dicts into a DuckDB table.

    Args:
        con: duckdb.Connection
        table_name: str
        rows: list[dict] - data rows
        schema: dict[str, str] - column_name -> DuckDB type (e.g. {"symbol": "VARCHAR", "price": "DOUBLE"})
    """
    if not rows:
        col_defs = ", ".join(f"{col} {dtype}" for col, dtype in schema.items())
        con.execute(f"CREATE TABLE {table_name}({col_defs})")
        return

    cols = list(schema.keys())
    col_defs = ", ".join(f"{col} {schema[col]}" for col in cols)
    con.execute(f"CREATE TABLE {table_name}({col_defs})")

    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    for row in rows:
        values = [row.get(col) for col in cols]
        con.execute(insert_sql, values)


def get_prices(con, symbols, target_date, window_days=10, offset_days=0, with_epochs=False):
    """Get adjusted close prices for symbols at/near a target date.

    Uses the first available price in [target_date + offset_days,
    target_date + offset_days + window_days].
    Handles both epoch-based (trade_epoch) and date-based (trade_date) schemas.

    Args:
        con: duckdb.Connection with prices_cache table
        symbols: list[str] - stock symbols
        target_date: date - target rebalance date
        window_days: int - number of days to search forward
        offset_days: int - days to shift forward (1 = next-day / MOC execution)
        with_epochs: bool - return {symbol: (price, trade_epoch)} instead (UTC-midnight epoch of
            trade_date on the date schema)

    Returns:
        dict[str, float] - {symbol: price}
    """
    if not symbols:
        return {}

    shifted_date = target_date + timedelta(days=offset_days)
    target_epoch = utc_epoch(shifted_date)
    end_epoch = utc_epoch(shifted_date + timedelta(days=window_days))
    sym_list = ",".join("'{}'".format(s.replace("'", "''")) for s in symbols)

    # Try epoch-based schema first (used by most backtests)
    try:
        rows = con.execute(f"""
            SELECT symbol, trade_epoch, adjClose
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_epoch >= {target_epoch}
              AND trade_epoch <= {end_epoch}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST) = 1
        """).fetchall()
        return {r[0]: ((r[2], r[1]) if with_epochs else r[2]) for r in rows}
    except Exception:
        pass

    # Fallback: date-based schema
    try:
        target_str = shifted_date.isoformat()
        end_str = (shifted_date + timedelta(days=window_days)).isoformat()
        rows = con.execute(f"""
            SELECT symbol, trade_date, adjClose
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_date >= '{target_str}'
              AND trade_date <= '{end_str}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date ASC, adjClose DESC NULLS LAST) = 1
        """).fetchall()
        if with_epochs:
            return {r[0]: (r[2], utc_epoch(date.fromisoformat(str(r[1])[:10]))) for r in rows}
        return {r[0]: r[2] for r in rows}
    except Exception:
        return {}


EXIT_METHODS = ("drop", "ltp", "both")
# The one place the default lives: CLIs and runners read it. run_backtest signature defaults stay "drop";
# verify_floor_guard gate 14 requires every caller to pass the resolved method.
DEFAULT_EXIT_METHOD = "ltp"
# US delistings 2001+: 3,498/4,969 last bars fall on/before delistedDate + 7d; most later bars to day 30 are zero-volume repeats, past day 30 they mostly trade (OTC / ticker reuse).
LTP_DELIST_GRACE_DAYS = 7
# A delisting more than this many days before the entry bar belongs to an earlier holder of the
# ticker; a closer one still bounds the window (zero-volume repeats run to about day 30).
LTP_EARLIER_HOLDER_DAYS = 30



def add_exit_method_arg(parser):
    """--exit-method drop|ltp|both (default DEFAULT_EXIT_METHOD)."""
    parser.add_argument("--exit-method", choices=EXIT_METHODS, default=DEFAULT_EXIT_METHOD,
                        help=f"Missing exit price: drop the name, exit at last traded price, or compute both (default {DEFAULT_EXIT_METHOD})")


class LtpExits:
    """Last-traded-price exits for names that have an entry price but no exit price.

    LTP = last adjClose with entry_bar_date < trade_date <= min(exit_date, delistedDate + grace).
    The delisted bound is ignored only when delistedDate is more than LTP_EARLIER_HOLDER_DAYS
    before the entry bar (an earlier holder of the ticker). No bar in the window -> the name stays
    dropped. Fills feed filter_returns unchanged, so max_single_return etc. still apply.
    Every fill passes remove_price_oscillations, like the topic's own exit prices.
    local=True reads the topic's own prices_cache, which must use the trade_epoch schema.
    traded_only=True (data check T, rule 2): the remote query also reads close and volume; a symbol
    with a traded row in the fetched span loses its zero-volume rows before the filter, and each
    fill records ltp_close and ltp_volume. With local=True the topic's prices_cache must then carry
    close and volume. False sends the SQL and builds the temp table this class always did.
    """

    # Remote cleaning context each side of the window; the filter's widest neighbour span is 20 days.
    CONTEXT_DAYS = 30

    def __init__(self, client, con, local=False, verbose=False, traded_only=False):
        self.client, self.con, self.local, self.verbose = client, con, local, verbose
        self.traded_only, self.zero_volume_moved, self._bar_extra = traded_only, 0, {}
        # One row per symbol (9,900 rows / 9,900 symbols, checked 2026-10-09).
        # Ordered so a symbol with two rows always keeps its latest delisting (the dict keeps the last row).
        rows = self._query("SELECT symbol, delistedDate FROM delisted_companies WHERE delistedDate IS NOT NULL"
                           " ORDER BY symbol, delistedDate", limit=100000)
        self.delisted = {r["symbol"]: date.fromisoformat(str(r["delistedDate"])[:10]) for r in rows or []}
        self.missing = self.filled = self.closed = 0
        self.fills = []

    def _query(self, sql, **kw):
        """client.query with retries: one transient failure must not kill a 25-year run."""
        import re
        import time
        for attempt in range(4):
            try:
                return self.client.query(sql, **kw)
            except Exception as e:
                # A bad query fails the same way every time: don't sleep on it.
                if attempt == 3 or any(k in str(e) for k in ("[SYNTAX_ERROR]", "Binder Error", "Parser Error", "Catalog Error")):
                    raise
                # cr_client raises a 429 as "Rate limited. Retry after Ns" and never retries it itself.
                m = re.search(r"Retry after (\d+)s", str(e))
                time.sleep(max(int(m.group(1)) if m else 0, 5 * (attempt + 1)))

    def fill(self, symbols, entry_prices, exit_prices, entry_date, exit_date, offset_days=0,
             min_entry_price=0.0):
        """Return {symbol: ltp} for names filter_returns would drop only for a missing exit price."""
        want = [s for s in symbols if exit_prices.get(s) is None
                and entry_usable(entry_prices.get(s), min_entry_price)]
        self.missing += len(want)
        if not want:
            return {}
        # The window starts the day AFTER the entry bar, so no print from the entry day can come back as the "exit".
        bars = get_prices(self.con, want, entry_date, offset_days=offset_days, with_epochs=True)
        bounds = {}
        for s in want:
            e = bars.get(s, (None, None))[1]
            if e is None:
                continue
            lo = utc_date(e)
            hi, dd = exit_date, self.delisted.get(s)
            ctx = hi + timedelta(days=self.CONTEXT_DAYS)   # remote cleaning context end
            if dd is None:
                kind = "none"
            elif dd + timedelta(days=LTP_EARLIER_HOLDER_DAYS) >= lo:
                bound = dd + timedelta(days=LTP_DELIST_GRACE_DAYS)
                kind = "applied" if bound < hi else "not_binding"   # applied = the bound cut the window short
                hi, ctx = min(hi, bound), min(ctx, bound)   # no bar past the bound, not even as context
            else:
                kind = "ignored_predates_entry"
            if hi > lo:
                bounds[s] = (lo, hi, kind, ctx)
            else:
                self.closed += 1   # the delisting bound closes the window before it opens: stays dropped
        got = self._last_bars(bounds) if bounds else {}
        out = {}
        for s in bounds:   # caller's order, not the query's: the fills list is reproducible
            if s not in got:
                continue
            bar_epoch, px, src = got[s]
            lo, hi, kind, _ = bounds[s]
            bar = utc_date(bar_epoch)
            if not lo < bar <= hi <= exit_date:
                raise AssertionError(f"LTP bar outside its window: {s} {lo} {bar} {hi}")
            out[s] = px
            self.fills.append({"symbol": s, "entry_date": entry_date.isoformat(),
                               "entry_bar": lo.isoformat(), "entry_price": entry_prices.get(s),
                               "last_bar": bar.isoformat(), "ltp": px,
                               "delisted_date": self.delisted[s].isoformat() if s in self.delisted else None,
                               "delisting_bound": kind, "source": src,
                               "exit_date": exit_date.isoformat()})
            if self.traded_only:
                self.fills[-1]["ltp_close"], self.fills[-1]["ltp_volume"] = self._bar_extra[s]
        self.filled += len(out)
        if self.verbose and want:
            print(f"      LTP: {len(out)}/{len(want)} missing exits filled")
        return out

    def _last_bars(self, bounds):
        """{symbol: (last_bar_epoch, adjClose, "local"|"remote")} with entry_bar_date < bar date <= hi.

        Local: the topic's prices_cache (already through remove_price_oscillations, must span the
        holding period), except names whose delisting bound caps the cleaning context. The topic's
        filter ran over its whole cache, where a reused ticker trading after the delisting can delete
        a genuine final crash, so those names go remote.
        Remote: ONE stock_eod query for each name's bars from CONTEXT_DAYS before the window to its
        context end, the same filter over them, then the same last-bar pick. Bars after hi are
        cleaning context only. A name here has no bar in the exit window, but trading that resumes
        soon after it gives the last bar a later neighbour, so the filter CAN change the fill.
        Tier 1 sees the same neighbours as a whole-series pass; tier 2 counts over the rows fetched.
        """
        # Local only when no delisting bound capped the context: the cache's filter saw bars past it.
        local = {s: b for s, b in bounds.items() if self.local and b[3] >= b[1] + timedelta(days=self.CONTEXT_DAYS)}
        remote = {s: b for s, b in bounds.items() if s not in local}
        pick = self._pick_traded if self.traded_only else self._pick
        out = {s: (e, px, "local") for s, (e, px) in pick(self.con, local).items()} if local else {}
        if remote:
            start = lambda lo: lo + timedelta(days=1 - self.CONTEXT_DAYS)
            vals = ",".join("('{}', {}, {})".format(s.replace("'", "''"), utc_epoch(start(lo)),
                                                    utc_epoch(ctx + timedelta(days=1)))
                            for s, (lo, _, _, ctx) in remote.items())
            first = min(start(lo) for lo, _, _, _ in remote.values()).isoformat()
            after = (max(ctx for _, _, _, ctx in remote.values()) + timedelta(days=1)).isoformat()
            cols = "e.symbol, e.dateEpoch, e.adjClose" + (", e.close, e.volume" if self.traded_only else "")
            rows = self._query(f"""
                WITH m(symbol, a, b) AS (VALUES {vals})
                SELECT {cols} FROM stock_eod e JOIN m ON e.symbol = m.symbol
                WHERE e.date >= '{first}' AND e.date < '{after}'
                  AND e.dateEpoch >= m.a AND e.dateEpoch < m.b AND e.adjClose > 0
            """, limit=1000000, timeout=600, memory_mb=16384, threads=6) or []
            if rows:
                import duckdb
                tmp = duckdb.connect()   # never the topic's con: its prices_cache stays as it was
                if self.traded_only:
                    tmp.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE,"
                                " close DOUBLE, volume DOUBLE)")
                    tmp.executemany("INSERT INTO prices_cache VALUES (?, ?, ?, ?, ?)",
                                    [(r["symbol"], int(r["dateEpoch"]), r["adjClose"], r.get("close"),
                                      r.get("volume")) for r in rows])
                    # T rule 2 over the fetched span: a line that traded loses its zero-volume rows
                    self.zero_volume_moved += tmp.execute(
                        "DELETE FROM prices_cache WHERE (volume IS NULL OR volume <= 0)"
                        " AND symbol IN (SELECT symbol FROM prices_cache WHERE volume > 0)").fetchone()[0]
                else:
                    tmp.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
                    tmp.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)",
                                    [(r["symbol"], int(r["dateEpoch"]), r["adjClose"]) for r in rows])
                remove_price_oscillations(tmp, verbose=False)
                out.update({s: (e, px, "remote") for s, (e, px) in pick(tmp, remote).items()})
                tmp.close()
        return out

    def _pick_traded(self, con, bounds):
        """_pick for traded_only: the same bar, its close and volume kept in self._bar_extra."""
        day_end = lambda d: utc_epoch(d + timedelta(days=1))
        vals = ",".join("('{}', {}, {})".format(s.replace("'", "''"), day_end(lo), day_end(hi))
                        for s, (lo, hi, _, _) in bounds.items())
        rows = con.execute(f"""
            WITH m(symbol, lo, hi) AS (VALUES {vals})
            SELECT p.symbol, p.trade_epoch, p.adjClose, p.close, p.volume FROM prices_cache p JOIN m ON p.symbol = m.symbol
            WHERE p.trade_epoch >= m.lo AND p.trade_epoch < m.hi AND p.adjClose > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY p.trade_epoch DESC, p.adjClose DESC,
                                       p.close DESC NULLS LAST, p.volume DESC NULLS LAST) = 1
        """).fetchall()
        self._bar_extra.update({r[0]: (r[3], r[4]) for r in rows})
        return {r[0]: (int(r[1]), r[2]) for r in rows}

    @staticmethod
    def _pick(con, bounds):
        """{symbol: (epoch, adjClose)}: the last bar in con's prices_cache with entry_bar_date < bar date <= hi."""
        day_end = lambda d: utc_epoch(d + timedelta(days=1))   # exclusive end of a day
        vals = ",".join("('{}', {}, {})".format(s.replace("'", "''"), day_end(lo), day_end(hi))
                        for s, (lo, hi, _, _) in bounds.items())
        rows = con.execute(f"""
            WITH m(symbol, lo, hi) AS (VALUES {vals})
            SELECT p.symbol, p.trade_epoch, p.adjClose FROM prices_cache p JOIN m ON p.symbol = m.symbol
            WHERE p.trade_epoch >= m.lo AND p.trade_epoch < m.hi AND p.adjClose > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY p.trade_epoch DESC, p.adjClose DESC) = 1
        """).fetchall()
        return {r[0]: (int(r[1]), r[2]) for r in rows}

    def results_block(self, exit_method, valid, periods_per_year, risk_free_rate,
                      key="portfolio_return", spy_key="spy_return"):
        """Results-JSON block. In both mode, also the LTP series' metrics over the SAME periods/benchmark."""
        out = {"exit_method": exit_method, **self.provenance()}
        if exit_method == "both":
            from metrics import compute_metrics
            pct = lambda v: round(v * 100, 2) if v is not None else None
            # Cash periods carry no _ltp key: identical under both methods.
            for m, rets in (("drop", [r[key] for r in valid]),
                            ("ltp", [r.get(f"{key}_ltp", r[key]) for r in valid])):
                p = compute_metrics(rets, [r[spy_key] for r in valid], periods_per_year,
                                    risk_free_rate=risk_free_rate)["portfolio"]
                out[f"{m}_portfolio"] = {"cagr": pct(p.get("cagr")), "max_drawdown": pct(p.get("max_drawdown")),
                                         "sharpe_ratio": round(p["sharpe_ratio"], 3) if p.get("sharpe_ratio") is not None else None,
                                         "total_return": pct(p.get("total_return")),
                                         "annualized_volatility": pct(p.get("annualized_volatility"))}
                print(f"  {m.upper()} exits: CAGR {out[f'{m}_portfolio']['cagr']}%, "
                      f"MaxDD {out[f'{m}_portfolio']['max_drawdown']}%, Sharpe {out[f'{m}_portfolio']['sharpe_ratio']}")
            print(f"  missing exits {self.missing}, LTP filled {self.filled}")
        return out

    def provenance(self):
        """Counts + every fill, for the results JSON."""
        return {"source": ("local prices_cache, remote stock_eod for delisting-bounded names" if self.local
                           else "remote stock_eod"),
                "delist_grace_days": LTP_DELIST_GRACE_DAYS, "missing_exits": self.missing,
                "ltp_filled": self.filled, "still_missing": self.missing - self.filled,
                "filled_local": sum(1 for f in self.fills if f["source"] == "local"),
                "filled_remote": sum(1 for f in self.fills if f["source"] == "remote"),
                "closed_by_delisting": self.closed,
                "filled_without_delisting_row": sum(1 for f in self.fills if f["delisted_date"] is None),
                "filled_ignoring_delisting_row": sum(1 for f in self.fills
                                                     if f["delisting_bound"] == "ignored_predates_entry"),
                "filled_delisting_not_binding": sum(1 for f in self.fills if f["delisting_bound"] == "not_binding"),
                **({"traded_only": True, "zero_volume_rows_moved": self.zero_volume_moved} if self.traded_only else {}),
                "fills": self.fills}


def generate_rebalance_dates(start_year, end_year, frequency, months=None):
    """Generate rebalance dates for any frequency.

    Args:
        start_year: int - first year
        end_year: int - last year (inclusive)
        frequency: str - 'monthly', 'quarterly', 'semi-annual', 'annual'
        months: list[int] or None - override specific months
                e.g. [4] for April-only annual, [1,7] for Jan/Jul semi-annual

    Returns:
        list[date] - sorted rebalance dates (first day of month)
    """
    if months is None:
        if frequency == "monthly":
            months = list(range(1, 13))
        elif frequency == "quarterly":
            months = [1, 4, 7, 10]
        elif frequency == "semi-annual":
            months = [1, 7]
        elif frequency == "annual":
            months = [1]
        else:
            raise ValueError(f"Unknown frequency: {frequency}. "
                             f"Use: monthly, quarterly, semi-annual, annual")

    dates = []
    for year in range(start_year, end_year + 1):
        for month in months:
            d = date(year, month, 1)
            dates.append(d)

    return sorted(dates)


def get_benchmark_tickers(exchanges, factor_type=None):
    """Get benchmark ticker symbols for a set of exchanges.

    Args:
        exchanges: list[str] or None - exchange codes (e.g. ["BSE", "NSE"])
        factor_type: str or None - "value", "quality", "small_cap", "momentum"

    Returns:
        dict[str, str] - {"SPY": "S&P 500", "INDA": "India", "IWD": "Value Factor"}
    """
    benchmarks = {"SPY": "S&P 500"}

    # Add factor benchmark
    if factor_type and factor_type in FACTOR_BENCHMARKS:
        ticker = FACTOR_BENCHMARKS[factor_type]
        benchmarks[ticker] = f"{factor_type.replace('_', ' ').title()} Factor"

    # Add regional benchmarks
    if exchanges:
        seen = set()
        for ex in exchanges:
            ticker = REGIONAL_BENCHMARKS.get(ex)
            if ticker and ticker not in benchmarks and ticker not in seen:
                seen.add(ticker)
                benchmarks[ticker] = f"Regional ({ex})"

    return benchmarks


def remove_price_oscillations(con, table_name="prices_cache", verbose=True,
                              spike_threshold=2.0, mild_threshold=1.3,
                              min_mild_count=5, max_neighbor_gap_days=10):
    """Remove rows with oscillating adjClose from a DuckDB price table.

    FMP's EOD data contains bad rows where adjClose spikes for 1-2 days then
    reverts. These are phantom holiday rows or broken split adjustments.
    Legitimate stock splits don't revert — the adjustment factor changes permanently.

    Two-tier detection (single pass, no iteration):
      Tier 1 (spike_threshold, default 2.0x): Any spike+revert is flagged.
        A 100%+ move that fully reverts in 1-2 days is always bad data.
      Tier 2 (mild_threshold, default 1.3x): Only flagged if the symbol has
        >= min_mild_count such oscillations. Catches BSAC-style persistent
        oscillation (30-40% flips, 70+ times) without flagging legitimate
        earnings-day volatility.

    Detection uses ±1 and ±2 neighbor windows:
      ±1: adjClose[N]/adjClose[N-1] spikes AND adjClose[N-1] ≈ adjClose[N+1]
      ±2: adjClose[N] far from both adjClose[N-2] and adjClose[N+2], which agree

    Neighbours must be TEMPORALLY adjacent (max_neighbor_gap_days), because a
    spike+revert is by definition a 1-2 day event. Many backtests fetch prices
    only in short windows around each rebalance date, so LAG/LEAD rows straddle
    year-long gaps. Without this guard, tier 2 fires at every window boundary
    where a stock moved 30-100% over the intervening year, and any symbol with
    >= min_mild_count such years has its window-boundary rows deleted. That
    silently removed legitimate rows for steady compounders (verified on
    BMW.DE, MUV2.DE and RWE.DE, none of which oscillate) and could empty a
    symbol's entire window, dropping it from that rebalance. On continuously
    fetched series every neighbour is already adjacent, so the guard is a no-op.

    Args:
        con: duckdb.Connection with price table loaded
        table_name: str - DuckDB table name (default "prices_cache")
        verbose: bool - print summary
        spike_threshold: float - tier 1 threshold (default 2.0 = 100% move)
        mild_threshold: float - tier 2 threshold (default 1.3 = 30% move)
        min_mild_count: int - minimum mild oscillations per symbol (default 5)
        max_neighbor_gap_days: int - maximum calendar-day span between the two
            outer neighbours for a comparison to count (default 10, which
            absorbs weekends and holiday closures)

    Returns:
        dict with keys: rows_removed, symbols_affected, rows_before, rows_after
    """
    try:
        rows_before = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except Exception:
        return {"rows_removed": 0, "symbols_affected": 0, "rows_before": 0, "rows_after": 0}

    # Determine date column name
    date_col = None
    for col in ["trade_epoch", "trade_date", "dateEpoch"]:
        try:
            con.execute(f"SELECT {col} FROM {table_name} LIMIT 0")
            date_col = col
            break
        except Exception:
            continue
    if not date_col:
        return {"rows_removed": 0, "symbols_affected": 0,
                "rows_before": rows_before, "rows_after": rows_before}

    st = spike_threshold
    inv_st = 1.0 / st
    mt = mild_threshold
    inv_mt = 1.0 / mt

    # Seconds-since-epoch form of the date column, so neighbour gaps are
    # measurable whether the caller stored an epoch int or a real DATE.
    try:
        col_type = (con.execute(
            f"SELECT typeof({date_col}) FROM {table_name} LIMIT 1"
        ).fetchone() or [""])[0].upper()
    except Exception:
        col_type = ""
    if col_type.startswith("DATE") or col_type.startswith("TIMESTAMP"):
        dt_secs = f"epoch(CAST({date_col} AS TIMESTAMP))"
    else:
        dt_secs = f"CAST({date_col} AS BIGINT)"

    gap1 = int(max_neighbor_gap_days) * 86400          # span across p1..n1
    gap2 = int(max_neighbor_gap_days) * 2 * 86400      # span across p2..n2

    # Build neighbor table once (prices AND their timestamps). adjClose breaks ties between
    # duplicate bars, so the neighbours (and the rows removed) are the same every run.
    w = f"PARTITION BY symbol ORDER BY {date_col}, adjClose"
    for t in ["_nb", "_tier1", "_mild_all", "_tier2", "_bad_rows"]:   # leftovers of a call that raised
        con.execute(f"DROP TABLE IF EXISTS {t}")
    con.execute(f"""
        CREATE TEMPORARY TABLE _nb AS
        SELECT symbol, {date_col} AS dt, adjClose,
            LAG(adjClose, 1) OVER ({w}) AS p1,
            LEAD(adjClose, 1) OVER ({w}) AS n1,
            LAG(adjClose, 2) OVER ({w}) AS p2,
            LEAD(adjClose, 2) OVER ({w}) AS n2,
            LAG({dt_secs}, 1) OVER ({w}) AS pt1,
            LEAD({dt_secs}, 1) OVER ({w}) AS nt1,
            LAG({dt_secs}, 2) OVER ({w}) AS pt2,
            LEAD({dt_secs}, 2) OVER ({w}) AS nt2
        FROM {table_name}
        WHERE adjClose > 0
    """)

    # Tier 1: definite spikes (>= spike_threshold, always flagged)
    con.execute(f"""
        CREATE TEMPORARY TABLE _tier1 AS
        SELECT symbol, dt FROM _nb
        WHERE
            (p1 > 0 AND n1 > 0
             AND nt1 - pt1 <= {gap1}
             AND (adjClose / p1 > {st} OR adjClose / p1 < {inv_st})
             AND n1 / p1 BETWEEN 0.5 AND 2.0)
            OR
            (p2 > 0 AND n2 > 0
             AND nt2 - pt2 <= {gap2}
             AND (adjClose / p2 > {st} OR adjClose / p2 < {inv_st})
             AND (adjClose / n2 > {st} OR adjClose / n2 < {inv_st})
             AND n2 / p2 BETWEEN 0.5 AND 2.0)
    """)

    # Tier 2: mild oscillations (>= mild_threshold, only if symbol has enough)
    con.execute(f"""
        CREATE TEMPORARY TABLE _mild_all AS
        SELECT symbol, dt FROM _nb
        WHERE (
            (p1 > 0 AND n1 > 0
             AND nt1 - pt1 <= {gap1}
             AND (adjClose / p1 > {mt} OR adjClose / p1 < {inv_mt})
             AND n1 / p1 BETWEEN 0.5 AND 2.0)
            OR
            (p2 > 0 AND n2 > 0
             AND nt2 - pt2 <= {gap2}
             AND (adjClose / p2 > {mt} OR adjClose / p2 < {inv_mt})
             AND (adjClose / n2 > {mt} OR adjClose / n2 < {inv_mt})
             AND n2 / p2 BETWEEN 0.5 AND 2.0)
        )
        AND (symbol, dt) NOT IN (SELECT symbol, dt FROM _tier1)
    """)

    # Keep only mild rows from symbols with >= min_mild_count oscillations
    con.execute(f"""
        CREATE TEMPORARY TABLE _tier2 AS
        SELECT m.symbol, m.dt
        FROM _mild_all m
        JOIN (
            SELECT symbol FROM _mild_all GROUP BY symbol HAVING COUNT(*) >= {min_mild_count}
        ) freq ON m.symbol = freq.symbol
    """)

    # Combine and delete
    con.execute(f"""
        CREATE TEMPORARY TABLE _bad_rows AS
        SELECT symbol, dt FROM _tier1
        UNION
        SELECT symbol, dt FROM _tier2
    """)

    bad_count = con.execute("SELECT COUNT(*) FROM _bad_rows").fetchone()[0]
    bad_symbols = con.execute("SELECT COUNT(DISTINCT symbol) FROM _bad_rows").fetchone()[0]

    if bad_count > 0:
        tier1_count = con.execute("SELECT COUNT(*) FROM _tier1").fetchone()[0]
        tier2_count = con.execute("SELECT COUNT(*) FROM _tier2").fetchone()[0]

        con.execute(f"""
            DELETE FROM {table_name}
            WHERE (symbol, {date_col}) IN (SELECT symbol, dt FROM _bad_rows)
        """)

        if verbose:
            print(f"  Price oscillation filter: removed {bad_count} bad rows "
                  f"across {bad_symbols} symbols "
                  f"(tier1: {tier1_count} rows >{spike_threshold}x, "
                  f"tier2: {tier2_count} rows >{mild_threshold}x with >={min_mild_count}/sym) "
                  f"({rows_before} → {rows_before - bad_count} rows)")

    # Cleanup temp tables
    for t in ["_nb", "_tier1", "_mild_all", "_tier2", "_bad_rows"]:
        con.execute(f"DROP TABLE IF EXISTS {t}")

    rows_after = rows_before - bad_count

    return {
        "rows_removed": bad_count,
        "symbols_affected": bad_symbols,
        "rows_before": rows_before,
        "rows_after": rows_after,
    }


def validate_price_data(con, max_price_ratio=1000, verbose=True):
    """Check prices_cache for data quality issues. Returns list of flagged symbols.

    Flags stocks where max(adjClose)/min(adjClose) > max_price_ratio, which
    indicates broken split adjustments (e.g. unadjusted stock splits on ASX/SAO).

    Non-blocking: prints warnings and returns flags for caller to handle.

    Args:
        con: duckdb.Connection with prices_cache table loaded
        max_price_ratio: float - flag symbols exceeding this max/min ratio (default 1000)
        verbose: bool - print warnings for flagged symbols

    Returns:
        list[dict] - flagged symbols with keys: symbol, min_price, max_price, ratio
    """
    try:
        rows = con.execute(f"""
            SELECT symbol,
                   MIN(adjClose) AS min_price,
                   MAX(adjClose) AS max_price,
                   MAX(adjClose) / NULLIF(MIN(adjClose), 0) AS price_ratio
            FROM prices_cache
            WHERE adjClose > 0
            GROUP BY symbol
            HAVING MAX(adjClose) / NULLIF(MIN(adjClose), 0) > {max_price_ratio}
            ORDER BY price_ratio DESC
        """).fetchall()
    except Exception:
        return []

    flagged = []
    for sym, min_p, max_p, ratio in rows:
        flagged.append({
            "symbol": sym,
            "min_price": round(min_p, 4),
            "max_price": round(max_p, 4),
            "ratio": round(ratio, 1),
        })

    if flagged and verbose:
        print(f"\n  WARNING: {len(flagged)} symbols with suspicious price ratios (>{max_price_ratio}x):")
        for f in flagged[:10]:
            print(f"    {f['symbol']}: ${f['min_price']} -> ${f['max_price']} ({f['ratio']}x)")
        if len(flagged) > 10:
            print(f"    ... and {len(flagged) - 10} more")

    return flagged


# ---------------------------------------------------------------------------
# DATA CHECKS dc-1: data checks in place of the 200% single-return cap, for the topics that opt
# in (deleveraging and cyclical-timing; gate 15 in scripts/verify_floor_guard.py holds the list).
# Every other topic calls none of this and keeps filter_returns' max_single_return=2.0.
#
#   T traded bars   clean_price_cache(): closed-day rows, zero-volume rows of lines that traded in
#                   the same window and non-positive prices leave the cache before
#                   remove_price_oscillations. LtpExits(traded_only=True) applies rule 2 to fills.
#   N NVDR twins    mark_nvdr_twins() and isin_dedup_sql(twin_key=True).
#   M market cap    load_cap_check() and cap_used_sql() at every market-cap floor.
#   F factor flags  DataChecks records stage-1 drift and split events; it changes no price.
#   L large moves   DataChecks lists every kept window-bar holding with |ln(1+r)| >= ln 2.
# All thresholds live in DATA_CHECKS_V1, which every results file carries.
# ---------------------------------------------------------------------------

DATA_CHECKS_V1 = {
    "version": "dc-1",
    "fetch_window_days": 11,
    "closed_day_ratio": 0.5,
    "closed_day_min_ref": 20,
    "closed_day_min_other_dates": 3,
    "closed_day_skip": ("a date is not tested when its window has fewer than 3 other dates or their median "
                        "traded count is under 20; closed_day_test_skipped counts windows with such a date"),
    "zero_volume": "moved only when the same line has a traded row in the same window",
    "nonpositive_price": "rows with adjClose NULL or <= 0 moved",
    "benchmarks_exempt": True,
    "twins": ("SET X-R.BK takes X.BK's ISIN as its security key when X.BK is in the leg, the ISINs share "
              "characters 1 to 6 and character 10 of the NVDR's is R; within a key: buyable at entry, then "
              "the ordinary line, then isActivelyTrading, home currency, symbol"),
    "cap": {"pf_min": 3.0, "turnover_max": 1e-4, "bars": 20, "lookback_days": 45, "min_traded_bars": 10,
            "k_min": 3.0, "eps_min": 0.05, "net_income_min": 1e6, "order": "turnover then k",
            "lower_only": True},
    "factor": {"band": [1 / 1.05, 1.15], "split_min": 2.0, "mode": "flag only: no price or return is changed"},
    "large_move_log": math.log(2),
}


def _sig(x, digits=6):
    """x to `digits` significant digits, None passing through (keeps results files readable)."""
    return None if x is None else float(f"{x:.{digits}g}")


def clean_price_cache(con, bench_symbols, rebalance_dates, cfg=DATA_CHECKS_V1, table="prices_cache"):
    """Check T on a topic's windowed price cache. Call it before remove_price_oscillations.

    Per fetch window W = [d, d + fetch_window_days] of each rebalance date d (overlapping windows
    merged), over non-benchmark rows, in this order:
      1 closed_day         traded(D) = rows on date D with volume > 0, ref(D) = the median of traded
                           over W's other dates. D is closed when traded(D) < closed_day_ratio x
                           ref(D), and every non-benchmark row on D moves. D is not tested when W
                           has fewer than closed_day_min_other_dates other dates or ref(D) is under
                           closed_day_min_ref.
      2 zero_volume        a (symbol, W) with a row of volume > 0 loses its rows with volume <= 0 or
                           NULL; a (symbol, W) with no traded row keeps every row and is counted.
      3 nonpositive_price  rows with adjClose NULL or <= 0.
    Moved rows land in prices_dropped(symbol, trade_epoch, adjClose, close, volume, reason,
    window_start). The cache needs symbol, trade_epoch (UTC midnight), adjClose, close and volume.
    bench_symbols may carry SQL quotes ("'SPY'"). Benchmark rows are never touched.
    Returns {rows_moved, symbol_windows_without_volume, closed_day_test_skipped, closed_dates}.
    """
    bench = sorted({str(s).strip().strip("'") for s in bench_symbols})
    keep = ("p.symbol NOT IN (" + ", ".join("'" + s.replace("'", "''") + "'" for s in bench) + ")"
            if bench else "TRUE")
    span = timedelta(days=int(cfg["fetch_window_days"]))
    wins = []
    for d in sorted(set(rebalance_dates)):
        lo, hi = utc_epoch(d), utc_epoch(d + span)
        if wins and lo <= wins[-1][1]:
            wins[-1][1] = max(wins[-1][1], hi)
        else:
            wins.append([lo, hi])
    con.execute("CREATE OR REPLACE TABLE prices_dropped(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE,"
                " close DOUBLE, volume DOUBLE, reason VARCHAR, window_start BIGINT)")
    con.execute("CREATE OR REPLACE TEMP TABLE _dc_win(ws BIGINT, we BIGINT)")
    if wins:
        con.executemany("INSERT INTO _dc_win VALUES (?, ?)", wins)

    def tag():   # current non-benchmark rows with their window and day
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _dc_rows AS
            SELECT p.rowid AS rid, p.symbol, p.trade_epoch, p.adjClose, p.close, p.volume, w.ws,
                   p.trade_epoch // 86400 AS day
            FROM {table} p JOIN _dc_win w ON p.trade_epoch BETWEEN w.ws AND w.we
            WHERE {keep}""")

    def move(where, reason):   # rows of _dc_rows matching `where` go to prices_dropped
        con.execute(f"INSERT INTO prices_dropped SELECT symbol, trade_epoch, adjClose, close, volume, '{reason}', ws"
                    f" FROM _dc_rows r WHERE {where}")
        n = con.execute(f"DELETE FROM {table} WHERE rowid IN (SELECT rid FROM _dc_rows r WHERE {where})").fetchone()[0]
        return int(n)

    tag()
    rows = con.execute("""
        WITH d AS (SELECT ws, day, SUM(CASE WHEN volume > 0 THEN 1 ELSE 0 END) AS traded
                   FROM _dc_rows GROUP BY ws, day)
        SELECT a.ws, a.day, a.traded, COUNT(b.day) AS n_other, MEDIAN(b.traded) AS ref
        FROM d a LEFT JOIN d b ON b.ws = a.ws AND b.day <> a.day
        GROUP BY a.ws, a.day, a.traded ORDER BY a.ws, a.day""").fetchall()
    closed, skipped = [], set()
    for ws, day, traded, n_other, ref in rows:
        if n_other < cfg["closed_day_min_other_dates"] or ref is None or ref < cfg["closed_day_min_ref"]:
            skipped.add(ws)
        elif traded < cfg["closed_day_ratio"] * ref:
            closed.append((ws, day, int(traded), float(ref)))
    con.execute("CREATE OR REPLACE TEMP TABLE _dc_closed(ws BIGINT, day BIGINT)")
    if closed:
        con.executemany("INSERT INTO _dc_closed VALUES (?, ?)", [(ws, day) for ws, day, _, _ in closed])
    moved = {"closed_day": move("(r.ws, r.day) IN (SELECT ws, day FROM _dc_closed)", "closed_day")}
    tag()
    con.execute("""CREATE OR REPLACE TEMP TABLE _dc_sw AS
        SELECT symbol, ws, COALESCE(BOOL_OR(volume > 0), FALSE) AS traded FROM _dc_rows GROUP BY symbol, ws""")
    no_volume = con.execute("SELECT COUNT(*) FROM _dc_sw WHERE NOT traded").fetchone()[0]
    moved["zero_volume"] = move("(r.volume IS NULL OR r.volume <= 0) AND (r.symbol, r.ws) IN"
                                " (SELECT symbol, ws FROM _dc_sw WHERE traded)", "zero_volume")
    tag()
    moved["nonpositive_price"] = move("r.adjClose IS NULL OR r.adjClose <= 0", "nonpositive_price")
    for t in ("_dc_win", "_dc_rows", "_dc_closed", "_dc_sw"):
        con.execute(f"DROP TABLE IF EXISTS {t}")
    return {"rows_moved": moved, "symbol_windows_without_volume": int(no_volume),
            "closed_day_test_skipped": len(skipped),
            "closed_dates": [{"date": utc_date(day * 86400).isoformat(), "traded": traded, "ref": round(ref, 1)}
                             for _, day, traded, ref in sorted(closed, key=lambda c: c[1])]}


def mark_nvdr_twins(con, table="isin_map"):
    """Check N: key each SET NVDR line (X-R.BK) to its ordinary line's security.

    Adds sec_key VARCHAR and nvdr BOOLEAN to load_isin_map()'s table. X-R.BK gets sec_key = X.BK's
    normalised ISIN and nvdr = TRUE when X.BK is in the map, both ISINs share characters 1 to 6 and
    character 10 of the NVDR's ISIN is R. Every other row keeps NULLs, so isin_dedup_sql(twin_key=True)
    keys it on its own ISIN. Returns the number of NVDR symbols linked.
    """
    con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sec_key VARCHAR")
    con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS nvdr BOOLEAN")
    con.execute(f"""
        UPDATE {table} AS t SET sec_key = o.isin, nvdr = TRUE
        FROM (SELECT symbol, MIN(NULLIF(UPPER(TRIM(isin)), '')) AS isin FROM {table} GROUP BY symbol) o
        WHERE t.symbol LIKE '%-R.BK'
          AND o.symbol = LEFT(t.symbol, LENGTH(t.symbol) - 5) || '.BK'
          AND o.isin IS NOT NULL
          AND SUBSTR(UPPER(TRIM(t.isin)), 1, 6) = SUBSTR(o.isin, 1, 6)
          AND SUBSTR(UPPER(TRIM(t.isin)), 10, 1) = 'R'""")
    return con.execute(f"SELECT COUNT(DISTINCT symbol) FROM {table} WHERE nvdr").fetchone()[0]


def _twin_ranked_sql(candidates_sql, target_date, offset_days, min_entry_price, window_days=10,
                     isin_table="isin_map", prices_table="prices_cache"):
    """isin_dedup_sql's ranking under the twin key, every row kept, with _tw_key, _tw_nvdr, _isin_rn."""
    shifted = target_date + timedelta(days=offset_days)
    lo, hi = utc_epoch(shifted), utc_epoch(shifted + timedelta(days=window_days))
    return f"""
            SELECT _c.*, COALESCE(_i.sec_key, _i.isin, _c.symbol) AS _tw_key,
                   COALESCE(_i.nvdr, FALSE) AS _tw_nvdr,
                   ROW_NUMBER() OVER (
                PARTITION BY COALESCE(_i.sec_key, _i.isin, _c.symbol)
                ORDER BY CASE WHEN _px.adjClose > 0 AND _px.adjClose >= {float(min_entry_price)}
                              THEN 0 ELSE 1 END,
                         CASE WHEN _i.nvdr THEN 1 ELSE 0 END,
                         CASE WHEN _i.live THEN 0 ELSE 1 END, CASE WHEN _i.home THEN 0 ELSE 1 END,
                         _c.symbol) AS _isin_rn
            FROM ({candidates_sql}) _c
            LEFT JOIN (SELECT symbol, MIN(NULLIF(UPPER(TRIM(isin)), '')) AS isin,
                              BOOL_OR(isActivelyTrading) AS live, BOOL_OR(home_ccy) AS home,
                              MIN(sec_key) AS sec_key, BOOL_OR(nvdr) AS nvdr
                       FROM {isin_table} GROUP BY symbol) _i ON _i.symbol = _c.symbol
            LEFT JOIN (SELECT symbol, adjClose FROM {prices_table}
                       WHERE trade_epoch >= {lo} AND trade_epoch <= {hi}
                       QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol
                           ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST) = 1) _px
                ON _px.symbol = _c.symbol"""


def nvdr_twins_sql(candidates_sql, target_date, offset_days, min_entry_price, window_days=10,
                   isin_table="isin_map", prices_table="prices_cache"):
    """(kept, merged, kept_nvdr) for every pair isin_dedup_sql(twin_key=True) collapses across an NVDR
    link. Binds the same `?` parameters as candidates_sql, once."""
    r = _twin_ranked_sql(candidates_sql, target_date, offset_days, min_entry_price, window_days,
                         isin_table, prices_table)
    return f"""
        WITH _r AS ({r})
        SELECT DISTINCT k.symbol AS kept, m.symbol AS merged, k._tw_nvdr AS kept_nvdr
        FROM _r k JOIN _r m ON m._tw_key = k._tw_key AND m._isin_rn > 1 AND m.symbol <> k.symbol
        WHERE k._isin_rn = 1 AND (k._tw_nvdr OR m._tw_nvdr)"""


CAP_CHECK_DDL = ("CREATE OR REPLACE TABLE cap_check(symbol VARCHAR, filing_epoch BIGINT, market_cap DOUBLE,"
                 " pf DOUBLE, pf_source VARCHAR, u DOUBLE, n_traded BIGINT, k DOUBLE, rule VARCHAR, k_div DOUBLE)")
CAP_INPUTS_DDL = ("CREATE OR REPLACE TABLE cap_inputs(symbol VARCHAR, filing_epoch BIGINT, market_cap DOUBLE,"
                  " net_income DOUBLE, eps DOUBLE, shares DOUBLE, n_bars BIGINT, n_traded BIGINT,"
                  " turnover DOUBLE, pf_raw DOUBLE, pf_splits DOUBLE)")
# A floor's failing side, for check M's events: over the floor on the raw cap, not on cap_used.
CAP_FAIL_TEST = "km.marketCap > ? AND (km.cap_used IS NULL OR km.cap_used <= ?)"


def cap_used_sql(m="m", cc="cc"):
    """The cap a floor reads under check M: NULL for an ineligible FY row, else marketCap / k_div."""
    return (f"CASE WHEN {cc}.rule = 'ineligible' THEN NULL WHEN {cc}.k_div IS NOT NULL"
            f" THEN {m}.marketCap / {cc}.k_div ELSE {m}.marketCap END")


def empty_cap_check(con):
    """A typed, empty cap_check: the floors LEFT JOIN it, so cap_used is marketCap everywhere."""
    con.execute(CAP_CHECK_DDL)


def cap_check_sql(sym_filter_sql, mktcap_min, date_ranges, cfg=DATA_CHECKS_V1):
    """Check M's server query: the inputs for every FY key_metrics row over the leg's floor.

    Per (symbol, FY period end): marketCap, the same FY's net income, EPS and weighted shares, the
    last `bars` stock_eod bars up to the period end (count, traded count, median close x volume of
    the traded ones), stock_eod close over the raw close on the latest day both exist, and the
    product of denominator / numerator over splits_calendar events after the period end.
    date_ranges [(first, last)] bound the bar scans to the days before each period end.
    """
    c = cfg["cap"]
    look = int(c["lookback_days"]) * 86400
    days = lambda col: " OR ".join(f"({col} >= '{a}' AND {col} <= '{b}')" for a, b in date_ranges) or "FALSE"
    return f"""
        WITH km AS (
            SELECT symbol, dateEpoch AS fe, marketCap FROM key_metrics
            WHERE period = 'FY' AND returnOnEquity IS NOT NULL AND marketCap > {float(mktcap_min)}
              AND {sym_filter_sql}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, dateEpoch ORDER BY marketCap DESC) = 1),
        inc AS (
            SELECT symbol, dateEpoch AS fe, netIncome, eps, weightedAverageShsOut FROM income_statement
            WHERE period = 'FY' AND symbol IN (SELECT symbol FROM km)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, dateEpoch
                                       ORDER BY filingDate DESC NULLS LAST, netIncome DESC NULLS LAST) = 1),
        kb AS (
            SELECT symbol, fe, fe // {look} AS blk FROM km
            UNION ALL SELECT symbol, fe, fe // {look} - 1 FROM km),
        bars AS (
            SELECT kb.symbol, kb.fe, e.dateEpoch AS e, e.close, e.volume,
                   ROW_NUMBER() OVER (PARTITION BY kb.symbol, kb.fe ORDER BY e.dateEpoch DESC,
                                      e.volume DESC NULLS LAST, e.close DESC NULLS LAST) AS rn
            FROM stock_eod e JOIN kb ON e.symbol = kb.symbol AND e.dateEpoch // {look} = kb.blk
            WHERE ({days("e.date")}) AND e.dateEpoch <= kb.fe AND e.dateEpoch > kb.fe - {look}),
        lb AS (SELECT * FROM bars WHERE rn <= {int(c["bars"])}),
        raw AS (
            SELECT symbol, dateEpoch AS e, MAX(adjClose) AS raw_close FROM stock_prices_non_split_adjusted_daily
            WHERE ({days("date")}) AND adjClose > 0 AND symbol IN (SELECT symbol FROM km)
            GROUP BY symbol, dateEpoch),
        pfr AS (
            SELECT lb.symbol, lb.fe, arg_max(lb.close / raw.raw_close, lb.e) AS pf_raw
            FROM lb JOIN raw ON raw.symbol = lb.symbol AND raw.e = lb.e
            WHERE lb.close > 0 GROUP BY lb.symbol, lb.fe),
        agg AS (
            SELECT symbol, fe, COUNT(*) AS n_bars,
                   SUM(CASE WHEN volume > 0 AND close > 0 THEN 1 ELSE 0 END) AS n_traded,
                   MEDIAN(CASE WHEN volume > 0 AND close > 0 THEN close * volume END) AS turnover
            FROM lb GROUP BY symbol, fe),
        spl AS (
            SELECT km.symbol, km.fe, EXP(SUM(LN(s.denominator / s.numerator))) AS pf_splits
            FROM km JOIN splits_calendar s ON s.symbol = km.symbol AND s.dateEpoch > km.fe
            WHERE s.numerator > 0 AND s.denominator > 0 GROUP BY km.symbol, km.fe)
        SELECT km.symbol, km.fe AS filing_epoch, km.marketCap AS market_cap, inc.netIncome AS net_income,
               inc.eps, inc.weightedAverageShsOut AS shares, agg.n_bars, agg.n_traded, agg.turnover,
               pfr.pf_raw, spl.pf_splits
        FROM km
        LEFT JOIN inc ON inc.symbol = km.symbol AND inc.fe = km.fe
        LEFT JOIN agg ON agg.symbol = km.symbol AND agg.fe = km.fe
        LEFT JOIN pfr ON pfr.symbol = km.symbol AND pfr.fe = km.fe
        LEFT JOIN spl ON spl.symbol = km.symbol AND spl.fe = km.fe
        ORDER BY km.symbol, km.fe"""


def apply_cap_rule(con, cfg=DATA_CHECKS_V1, inputs="cap_inputs"):
    """Check M's rule, from cap_inputs into cap_check (one row per symbol and FY period end).

    Only when pf >= pf_min: u known and under turnover_max makes the FY row ineligible (cap_used
    NULL); else k > k_min lowers the cap to marketCap / k ('cap_over_k'); else the cap stands. It
    never raises a cap. pf = close / raw close at the period end, else the later splits' product;
    u = median close x volume of the traded bars / marketCap, unknown under min_traded_bars; k =
    shares x EPS / net income, when |EPS| >= eps_min, |net income| >= net_income_min and the signs
    agree. pf comes from later splits but only gates: the decision reads k (same filing) and u
    (trading before the period end). Returns {cap_rows_checked, cap_rows_flagged}.
    """
    c = cfg["cap"]
    pf_min, u_max, k_min = float(c["pf_min"]), float(c["turnover_max"]), float(c["k_min"])
    con.execute(CAP_CHECK_DDL)
    con.execute(f"""
        INSERT INTO cap_check
        WITH x AS (
            SELECT symbol, CAST(filing_epoch AS BIGINT) AS filing_epoch, CAST(market_cap AS DOUBLE) AS market_cap,
                   CAST(COALESCE(pf_raw, pf_splits) AS DOUBLE) AS pf,
                   CASE WHEN pf_raw IS NOT NULL THEN 'raw' WHEN pf_splits IS NOT NULL THEN 'splits' END AS pf_source,
                   CASE WHEN n_traded >= {int(c["min_traded_bars"])} AND market_cap > 0
                        THEN CAST(turnover AS DOUBLE) / market_cap END AS u,
                   CAST(n_traded AS BIGINT) AS n_traded,
                   CASE WHEN ABS(eps) >= {float(c["eps_min"])} AND ABS(net_income) >= {float(c["net_income_min"])}
                             AND SIGN(eps) = SIGN(net_income) AND shares > 0
                        THEN CAST(shares AS DOUBLE) * eps / net_income END AS k
            FROM {inputs})
        SELECT symbol, filing_epoch, market_cap, pf, pf_source, u, n_traded, k,
               CASE WHEN pf >= {pf_min} AND u < {u_max} THEN 'ineligible'
                    WHEN pf >= {pf_min} AND k > {k_min} THEN 'cap_over_k' ELSE 'stands' END,
               CASE WHEN pf >= {pf_min} AND NOT COALESCE(u < {u_max}, FALSE) AND k > {k_min} THEN k END
        FROM x""")
    dup = con.execute("SELECT COUNT(*) FROM (SELECT symbol, filing_epoch FROM cap_check"
                      " GROUP BY symbol, filing_epoch HAVING COUNT(*) > 1)").fetchone()[0]
    if dup:
        raise AssertionError(f"cap_check has {dup} duplicate (symbol, filing_epoch) keys: the floors would fan out")
    n, flagged = con.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE rule <> 'stands') FROM cap_check").fetchone()
    return {"cap_rows_checked": int(n), "cap_rows_flagged": int(flagged)}


def load_cap_check(client, con, sym_filter_sql, mktcap_min, cfg=DATA_CHECKS_V1, verbose=False,
                   metrics_table="metrics_cache"):
    """Check M for a leg: one server query (cap_check_sql), then apply_cap_rule() into cap_check.

    metrics_table (symbol, marketCap, filing_epoch: the topic's FY key_metrics) bounds the bar scans
    to the lookback before each FY period end over the floor. Returns apply_cap_rule()'s counts.
    """
    look = timedelta(days=int(cfg["cap"]["lookback_days"]))
    ranges = []
    for (fe,) in con.execute(f"SELECT DISTINCT filing_epoch FROM {metrics_table} WHERE marketCap > ?"
                             " ORDER BY 1", [float(mktcap_min)]).fetchall():
        hi = utc_date(fe)
        if ranges and hi - look <= ranges[-1][1] + timedelta(days=1):
            ranges[-1][1] = max(ranges[-1][1], hi)
        else:
            ranges.append([hi - look, hi])
    con.execute(CAP_INPUTS_DDL)
    if ranges:
        con.execute("DROP TABLE cap_inputs")
        sql = cap_check_sql(sym_filter_sql, mktcap_min, [(a.isoformat(), b.isoformat()) for a, b in ranges], cfg)
        if not query_parquet(client, sql, con, "cap_inputs", verbose=verbose, limit=5000000, timeout=600,
                             memory_mb=16384, threads=6):
            con.execute("DROP TABLE cap_inputs")   # query_parquet's empty placeholder has no columns
            con.execute(CAP_INPUTS_DDL)
    return apply_cap_rule(con, cfg)


def load_splits(client, con, sym_filter_sql, verbose=False):
    """splits(symbol, split_epoch, numerator, denominator): the leg's splits_calendar, for check F."""
    n = query_parquet(client, "SELECT symbol, dateEpoch AS split_epoch, numerator, denominator"
                              f" FROM splits_calendar WHERE {sym_filter_sql}", con, "_splits_raw", verbose=verbose)
    con.execute("CREATE OR REPLACE TABLE splits(symbol VARCHAR, split_epoch BIGINT, numerator DOUBLE,"
                " denominator DOUBLE)")
    if n:
        con.execute("INSERT INTO splits SELECT symbol, split_epoch, numerator, denominator FROM _splits_raw")
    con.execute("DROP TABLE _splits_raw")
    return n


def _event_key(e):
    return (e["rebalance_date"], e["symbol"], e["check"], e.get("side") or "",
            json.dumps(e, sort_keys=True, default=str))


class DataChecks:
    """One leg's data checks dc-1: collects what T, N, M, F and L did, for the results file.

    mode "on" runs every check. mode "uncapped" runs none: it exists for the same-day attribution
    arm, issues no query and leaves every count at 0. Both modes book with max_single_return=None.
    A topic's run_backtest builds an "uncapped" instance when it is called without one.
    Window bars only: nothing here reads an LTP fill, so a drop run and a both run write the same
    block (annotate_ltp writes into the ltp block, which strip_ltp removes).
    """

    MODES = ("on", "uncapped")
    PRICE_KEYS = ("entry_bars_moved", "exit_bars_moved", "entries_lost")

    def __init__(self, mode="on", cfg=DATA_CHECKS_V1):
        if mode not in self.MODES:
            raise ValueError(f"data checks mode {mode!r} not in {self.MODES}")
        self.mode, self.cfg, self.on = mode, cfg, mode == "on"
        self.clean = {"rows_moved": {"closed_day": 0, "zero_volume": 0, "nonpositive_price": 0},
                      "symbol_windows_without_volume": 0, "closed_day_test_skipped": 0, "closed_dates": []}
        self.cap = {"cap_rows_checked": 0, "cap_rows_flagged": 0}
        self.twins_linked = self.factor_checked = 0
        self.events, self.large_moves, self.periods = [], [], {}

    # ---- leg-wide steps, recorded by the topic's fetch
    def record_clean(self, stats):
        self.clean = stats

    def record_cap(self, stats):
        self.cap = stats

    def record_twins_linked(self, n):
        self.twins_linked = int(n)

    def _period(self, d):
        return self.periods.setdefault(d.isoformat(), {})

    def _event(self, d, symbol, check, side=None, **fields):
        self.events.append({"rebalance_date": d.isoformat(), "symbol": symbol, "check": check, "side": side,
                            **fields})

    # ---- per screen
    def twins(self, con, candidates_sql, params, target_date, offset_days, min_entry_price, kept):
        """Check N's events: each kept (screened) line that absorbed a twin across an NVDR link."""
        n = 0
        if self.on:
            keep = set(kept)
            rows = con.execute(nvdr_twins_sql(candidates_sql, target_date, offset_days, min_entry_price),
                               list(params)).fetchall()
            for k, m, k_nvdr in sorted(rows):
                if k in keep:
                    n += 1
                    self._event(target_date, k, "twin", merged=m, reason="stand_in" if k_nvdr else "ordinary_first")
        self._period(target_date)["twins_collapsed"] = n
        return n

    def caps(self, con, sql, params, target_date, floor, check="cap"):
        """Check M's events: candidates over `floor` on the raw cap that fail it on cap_used.

        `sql` is the screen's candidate SELECT with CAP_FAIL_TEST as its floor and km_epoch, raw_cap
        among its columns."""
        n = 0
        if self.on:
            rows = con.execute(f"""
                SELECT DISTINCT c.symbol, c.km_epoch, c.raw_cap, cc.pf, cc.pf_source, cc.u, cc.k, cc.rule, cc.k_div
                FROM ({sql}) c LEFT JOIN cap_check cc ON cc.symbol = c.symbol AND cc.filing_epoch = c.km_epoch
                ORDER BY c.symbol, c.km_epoch""", list(params)).fetchall()
            for sym, fe, cap, pf, src, u, k, rule, kd in rows:
                n += 1
                used = None if rule == "ineligible" else (cap / kd if kd else cap)
                self._event(target_date, sym, check, fy=utc_date(fe).isoformat(), cap=cap, k=_sig(k), pf=_sig(pf),
                            pf_source=src, u=_sig(u), rule=rule, cap_used=_sig(used, 10), floor=floor)
        self._period(target_date)[f"{check}_excluded"] = n
        return n

    # ---- per priced period
    @staticmethod
    def _window(target_date, offset_days, window_days=10):
        shifted = target_date + timedelta(days=offset_days)
        return utc_epoch(shifted), utc_epoch(shifted + timedelta(days=window_days))

    @classmethod
    def _bars(cls, con, symbols, target_date, offset_days):
        """get_prices' bar per symbol (same window and order), with its date, close and volume."""
        if not symbols:
            return {}
        lo, hi = cls._window(target_date, offset_days)
        syms = ",".join("'{}'".format(s.replace("'", "''")) for s in symbols)
        rows = con.execute(f"""
            SELECT symbol, trade_epoch, adjClose, close, volume FROM prices_cache
            WHERE symbol IN ({syms}) AND trade_epoch >= {lo} AND trade_epoch <= {hi}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST,
                                       close DESC NULLS LAST, volume DESC NULLS LAST) = 1""").fetchall()
        return {r[0]: {"date": utc_date(r[1]).isoformat(), "epoch": int(r[1]), "adjClose": r[2], "close": r[3],
                       "volume": r[4]} for r in rows}

    @classmethod
    def _first_moved(cls, con, symbols, target_date, offset_days):
        """{symbol: row} where the window's first row over the cache and prices_dropped is one T moved."""
        if not symbols:
            return {}
        lo, hi = cls._window(target_date, offset_days)
        syms = ",".join("'{}'".format(s.replace("'", "''")) for s in symbols)
        w = f"symbol IN ({syms}) AND trade_epoch >= {lo} AND trade_epoch <= {hi}"
        rows = con.execute(f"""
            SELECT symbol, trade_epoch, adjClose, close, volume, reason FROM (
                SELECT symbol, trade_epoch, adjClose, close, volume, NULL AS reason FROM prices_cache WHERE {w}
                UNION ALL
                SELECT symbol, trade_epoch, adjClose, close, volume, reason FROM prices_dropped WHERE {w})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC, adjClose DESC NULLS LAST,
                                       CASE WHEN reason IS NULL THEN 0 ELSE 1 END, reason) = 1""").fetchall()
        return {r[0]: {"date": utc_date(r[1]).isoformat(), "adjClose": r[2], "close": r[3], "volume": r[4],
                       "reason": r[5]} for r in rows if r[5] is not None}

    def _splits(self, con, spans):
        """{symbol: [split events with |ln(denominator / numerator)| >= ln split_min in (lo, hi]]}."""
        if not spans:
            return {}
        vals = ",".join("('{}', {}, {})".format(s.replace("'", "''"), lo, hi) for s, (lo, hi) in spans.items())
        rows = con.execute(f"""
            WITH m(symbol, lo, hi) AS (VALUES {vals})
            SELECT s.symbol, s.split_epoch, s.numerator, s.denominator FROM splits s JOIN m ON m.symbol = s.symbol
            WHERE s.split_epoch > m.lo AND s.split_epoch <= m.hi AND s.numerator > 0 AND s.denominator > 0
              AND ABS(LN(s.denominator / s.numerator)) >= LN({float(self.cfg['factor']['split_min'])})
            ORDER BY s.symbol, s.split_epoch, s.numerator, s.denominator""").fetchall()
        out = {}
        for s, e, num, den in rows:
            out.setdefault(s, []).append({"date": utc_date(e).isoformat(), "numerator": num, "denominator": den})
        return out

    def prices(self, con, entry_date, exit_date, symbol_data, offset_days, min_entry_price, held=True):
        """Checks on one priced period's window bars: T's bar events, then for a held book L and F.

        symbol_data is the topic's (symbol, entry, exit, mcap) list from get_prices. The bars are read
        again with their dates and closes, and must carry the same adjClose (an assert). F only
        records: a holding is flagged when its stage-1 drift (adjClose / close at the exit bar over the
        same at the entry bar) leaves the band, a split of ratio split_min or more falls in (entry bar,
        exit bar], or it is on L."""
        p = self._period(entry_date)
        p.update(entry_bars_moved=0, exit_bars_moved=0, entries_lost=0, factor_flags=0)
        if not self.on:
            return
        syms = [s for s, *_ in symbol_data]
        bars = {"entry": self._bars(con, syms, entry_date, offset_days),
                "exit": self._bars(con, syms, exit_date, offset_days)}
        for s, ep, xp, _ in symbol_data:
            for side, px in (("entry", ep), ("exit", xp)):
                got = bars[side].get(s, {}).get("adjClose")
                if got != px:
                    raise AssertionError(f"data checks: {side} bar of {s} on {entry_date} reads {got}, get_prices {px}")
        out = lambda b: None if b is None else {k: b[k] for k in ("date", "adjClose", "close", "volume")}
        for side, d in (("entry", entry_date), ("exit", exit_date)):
            for s, row in sorted(self._first_moved(con, syms, d, offset_days).items()):
                used = bars[side].get(s)
                if side == "entry" and used is None:
                    p["entries_lost"] += 1
                    self._event(entry_date, s, "entry_lost", side=side, first_bar=row)
                else:
                    p[f"{side}_bars_moved"] += 1
                    self._event(entry_date, s, f"{side}_bar", side=side, first_bar=row, used_bar=out(used))
        if not held:
            return
        held_syms = [(s, ep, xp) for s, ep, xp, _ in symbol_data
                     if entry_usable(ep, min_entry_price) and xp is not None and xp > 0]
        spans = {s: (bars["entry"][s]["epoch"], bars["exit"][s]["epoch"]) for s, _, _ in held_syms}
        splits = self._splits(con, spans)
        lo, hi = self.cfg["factor"]["band"]
        for s, ep, xp in held_syms:
            e, x = bars["entry"][s], bars["exit"][s]
            self.factor_checked += 1
            reasons = []
            if abs(math.log(xp / ep)) >= self.cfg["large_move_log"]:
                reasons.append("large_move")
                self.large_moves.append({"rebalance_date": entry_date.isoformat(), "symbol": s,
                                         "return": _sig(xp / ep - 1), "entry_bar": out(e), "exit_bar": out(x)})
            closes = bool(e["close"] and x["close"] and e["close"] > 0 and x["close"] > 0)
            drift = (x["adjClose"] / x["close"]) / (e["adjClose"] / e["close"]) if closes else None
            if drift is not None and not lo <= drift <= hi:
                reasons.append("band")
            if s in splits:
                reasons.append("split")
            if reasons:
                p["factor_flags"] += 1
                self._event(entry_date, s, "factor_flag", entry_bar_date=e["date"], exit_bar_date=x["date"],
                            drift=_sig(drift), r_adj=_sig(xp / ep - 1),
                            r_close=_sig(x["close"] / e["close"] - 1) if closes else None,
                            splits=splits.get(s, []), reasons=sorted(reasons))

    def annotate_ltp(self, con, ltp, block):
        """F's flag on each LTP fill (traded_only), written into the ltp block: drift from the entry bar
        to the fill and the splits between. Changes no fill."""
        if not (self.on and getattr(ltp, "traded_only", False)):
            return
        lo, hi = self.cfg["factor"]["band"]
        n = 0
        for f in ltp.fills:
            e0 = utc_epoch(date.fromisoformat(f["entry_bar"]))
            e = con.execute("SELECT adjClose, close FROM prices_cache WHERE symbol = ? AND trade_epoch = ?"
                            " ORDER BY adjClose DESC NULLS LAST, close DESC NULLS LAST LIMIT 1",
                            [f["symbol"], e0]).fetchone()
            drift = None
            if e and e[0] and e[1] and e[1] > 0 and f.get("ltp_close") and f["ltp_close"] > 0:
                drift = (f["ltp"] / f["ltp_close"]) / (e[0] / e[1])
            sp = self._splits(con, {f["symbol"]: (e0, utc_epoch(date.fromisoformat(f["last_bar"])))}).get(f["symbol"], [])
            flagged = bool(sp) or (drift is not None and not lo <= drift <= hi)
            f["factor_check"] = {"drift": _sig(drift), "splits": sp, "flagged": flagged}
            n += flagged
        block["factor_flagged"] = n

    def row(self, d, screened=True, priced=True, signal=False):
        """A period record's data-check counts: an int where the step ran, None where it did not."""
        p = self.periods.get(d.isoformat(), {})
        out = {k: (p.get(k, 0) if priced else None) for k in self.PRICE_KEYS}
        out.update({k: (p.get(k, 0) if screened else None) for k in ("twins_collapsed", "cap_excluded")})
        out["factor_flags"] = p.get("factor_flags", 0) if priced else None
        if signal:
            out["signal_cap_excluded"] = p.get("signal_cap_excluded", 0)
        return out

    def block(self):
        """The results file's data_checks block (sorted, no run-time field, no key naming LTP)."""
        ev = sorted(self.events, key=_event_key)
        n = lambda check: sum(1 for e in ev if e["check"] == check)
        counts = {
            "rows_moved": dict(self.clean["rows_moved"]),
            "symbol_windows_without_volume": self.clean["symbol_windows_without_volume"],
            "closed_day_test_skipped": self.clean["closed_day_test_skipped"],
            "entry_bars_moved": n("entry_bar"), "exit_bars_moved": n("exit_bar"), "entries_lost": n("entry_lost"),
            "twins_linked": self.twins_linked, "twins_collapsed": n("twin"),
            "nvdr_stand_ins": sum(1 for e in ev if e["check"] == "twin" and e["reason"] == "stand_in"),
            "cap_rows_checked": self.cap["cap_rows_checked"], "cap_rows_flagged": self.cap["cap_rows_flagged"],
            "cap_excluded": n("cap"), "signal_cap_excluded": n("signal_cap"),
            "factor_checked": self.factor_checked, "factor_flags": n("factor_flag"),
            "large_moves": len(self.large_moves),
        }
        return {"version": self.cfg["version"], "mode": self.mode, "max_single_return": None,
                "rules": {k: v for k, v in self.cfg.items() if k != "version"}, "counts": counts,
                "closed_dates": list(self.clean["closed_dates"]), "events": ev,
                "large_moves": sorted(self.large_moves, key=lambda m: (m["rebalance_date"], m["symbol"]))}

    def summary(self):
        c = self.block()["counts"]
        return (f"Data checks ({self.mode}): rows moved {c['rows_moved']}, entry/exit bars moved "
                f"{c['entry_bars_moved']}/{c['exit_bars_moved']}, entries lost {c['entries_lost']}, twins "
                f"{c['twins_collapsed']} ({c['nvdr_stand_ins']} stand-ins), cap rows flagged {c['cap_rows_flagged']}, "
                f"cap excluded {c['cap_excluded']} + signal {c['signal_cap_excluded']}, factor flags "
                f"{c['factor_flags']}, large moves {c['large_moves']}")
