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
from collections import Counter
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
                         offset_days=0, window_days=10, exit_offset_days=None):
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
        exit_offset_days: int or None - the exit's offset when it differs from the entry's
            (RunWindow.exit_offset: 0 for a partial exit). None uses offset_days.

    Returns:
        float or None - benchmark return for the period
    """
    entry = get_prices(con, [benchmark_symbol], entry_date,
                       window_days=window_days, offset_days=offset_days)
    exit_ = get_prices(con, [benchmark_symbol], exit_date, window_days=window_days,
                       offset_days=offset_days if exit_offset_days is None else exit_offset_days)
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
    with a traded row in its LTP window (entry bar, hi] loses its zero-volume rows over the fetched
    span before the filter, and a symbol with none keeps them all (as T keeps a window with no
    traded row). Each fill records ltp_close and ltp_volume. With local=True the topic's prices_cache
    must then carry close and volume. False sends the SQL and builds the temp table this class always did.
    traded_only=True also repairs a fill that lands on the other side of a feed whose adjClose basis
    flips row by row while its close runs smoothly (RAM.BK 2018-01: adjClose alternates between about
    close / 5.35 and close x 0.855): cfg["fill_basis"] decides it, the fill books entry price x
    fill close / entry close, and keeps its adjClose as ltp_adj with basis "close" (close_basis_fills).
    cfg (DATA_CHECKS_V1 when None) carries fill_basis and factor; with rule S (DATA_CHECKS_V2) a traded_only
    fill also loses each stale repeat over its rows to the window end (stale_rows_moved), remote only.
    """

    # Remote cleaning context each side of the window; the filter's widest neighbour span is 20 days.
    CONTEXT_DAYS = 30

    def __init__(self, client, con, local=False, verbose=False, traded_only=False, end_date=None, cfg=None):
        """end_date: the run window's end (RunWindow.end_date). No bar after it is fetched, not even as
        cleaning context, so no later price reaches a fill. None: no cut."""
        self.client, self.con, self.local, self.verbose = client, con, local, verbose
        self.end_date = end_date
        self.cfg = DATA_CHECKS_V1 if cfg is None else cfg
        self.stale = traded_only and "stale_repeat" in self.cfg
        if self.stale and local:
            raise ValueError("LtpExits: rule S runs on the remote path only; pass local=False")
        self.traded_only, self.zero_volume_moved, self._bar_extra = traded_only, 0, {}
        self.close_basis, self._flips, self.stale_moved = 0, {}, 0
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
            if self.end_date is not None:
                ctx = min(ctx, self.end_date)
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
                alt = self._close_basis(s, bars[s][1], entry_prices.get(s), px, self._bar_extra[s][0])
                if alt is not None:   # the fill sits on the other adjClose basis: book the close's move
                    self.fills[-1].update(ltp=alt, ltp_adj=px, basis="close")
                    out[s] = alt
                    self.close_basis += 1
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
        if self.traded_only and local:
            self._flips.update(self._basis_flips(self.con, local))
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
                    # T rule 2: a line that traded inside its LTP window loses its zero-volume rows
                    day_end = lambda d: utc_epoch(d + timedelta(days=1))
                    tmp.execute("CREATE TABLE ltp_win(symbol VARCHAR, lo BIGINT, hi BIGINT)")
                    tmp.executemany("INSERT INTO ltp_win VALUES (?, ?, ?)",
                                    [(s, day_end(lo), day_end(hi)) for s, (lo, hi, _, _) in remote.items()])
                    self.zero_volume_moved += tmp.execute(
                        "DELETE FROM prices_cache WHERE (volume IS NULL OR volume <= 0) AND symbol IN ("
                        "SELECT p.symbol FROM prices_cache p JOIN ltp_win w ON p.symbol = w.symbol"
                        " WHERE p.volume > 0 AND p.trade_epoch >= w.lo AND p.trade_epoch < w.hi)").fetchone()[0]
                    self._flips.update(self._basis_flips(tmp, remote))   # before the filter drops either basis
                    if self.stale:   # rule S over each fill's rows to its window end, after the flips are counted
                        tmp.execute("CREATE TABLE _stale AS " + stale_repeat_sql(
                            "SELECT p.symbol, 0 AS k, p.trade_epoch, p.adjClose, p.close, p.volume FROM prices_cache p"
                            " JOIN ltp_win w ON p.symbol = w.symbol WHERE p.trade_epoch < w.hi",
                            self.cfg["stale_repeat"]["min_run"]))
                        self.stale_moved += tmp.execute("DELETE FROM prices_cache WHERE (symbol, trade_epoch) IN"
                                                        " (SELECT symbol, trade_epoch FROM _stale)").fetchone()[0]
                else:
                    tmp.execute("CREATE TABLE prices_cache(symbol VARCHAR, trade_epoch BIGINT, adjClose DOUBLE)")
                    tmp.executemany("INSERT INTO prices_cache VALUES (?, ?, ?)",
                                    [(r["symbol"], int(r["dateEpoch"]), r["adjClose"]) for r in rows])
                remove_price_oscillations(tmp, verbose=False)
                out.update({s: (e, px, "remote") for s, (e, px) in pick(tmp, remote).items()})
                tmp.close()
        return out

    def _basis_flips(self, con, bounds):
        """{symbol: reversals}: one row per date of con's prices_cache from the fetched context start to the
        LTP window end; a flip is a day whose ln(adjClose / close) moves more than ln(factor.band upper)
        while ln(close) moves less; a reversal is a flip opposite in sign to the flip before it."""
        if not bounds:
            return {}
        day_end = lambda d: utc_epoch(d + timedelta(days=1))
        lb = math.log(self.cfg["factor"]["band"][1])
        start = lambda lo: utc_epoch(lo + timedelta(days=1 - self.CONTEXT_DAYS))   # the fetched context start
        vals = ",".join("('{}', {}, {})".format(s.replace("'", "''"), start(lo), day_end(hi))
                        for s, (lo, hi, _, _) in bounds.items())
        return dict(con.execute(f"""
            WITH m(symbol, lo, hi) AS (VALUES {vals}),
            d AS (SELECT p.symbol, p.trade_epoch, p.adjClose, p.close FROM prices_cache p JOIN m ON p.symbol = m.symbol
                  WHERE p.trade_epoch >= m.lo AND p.trade_epoch < m.hi AND p.adjClose > 0 AND p.close > 0
                  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol, p.trade_epoch
                                             ORDER BY p.adjClose DESC, p.close DESC) = 1),
            s AS (SELECT symbol, trade_epoch,
                         LN(adjClose / close) - LAG(LN(adjClose / close)) OVER w AS dr,
                         LN(close) - LAG(LN(close)) OVER w AS dc
                  FROM d WINDOW w AS (PARTITION BY symbol ORDER BY trade_epoch)),
            f AS (SELECT symbol, SIGN(dr) AS sg,
                         LAG(SIGN(dr)) OVER (PARTITION BY symbol ORDER BY trade_epoch) AS prev
                  FROM s WHERE ABS(dr) > {lb} AND ABS(dc) <= {lb})
            SELECT symbol, COUNT(*) FILTER (WHERE sg <> prev) FROM f GROUP BY symbol""").fetchall())

    def _close_basis(self, s, entry_epoch, entry_price, px, close):
        """The close-based exit price for a fill that fill_basis repairs, else None."""
        if self._flips.get(s, 0) < self.cfg["fill_basis"]["min_reversals"]:
            return None
        if not (entry_price and entry_price > 0 and px and px > 0 and close and close > 0):
            return None
        if "close" not in {c[0] for c in self.con.execute("DESCRIBE prices_cache").fetchall()}:
            return None
        row = self.con.execute("SELECT close FROM prices_cache WHERE symbol = ? AND trade_epoch = ? AND adjClose = ?"
                               " ORDER BY close DESC NULLS LAST LIMIT 1", [s, entry_epoch, entry_price]).fetchone()
        if not (row and row[0] and row[0] > 0):
            return None
        lo, hi = self.cfg["factor"]["band"]
        if lo <= (px / close) / (entry_price / row[0]) <= hi:
            return None
        return entry_price * close / row[0]

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
                      key="portfolio_return", spy_key="spy_return", fractions=None):
        """Results-JSON block. In both mode, also the LTP series' metrics over the SAME periods/benchmark.
        fractions: as compute_metrics takes them (RunWindow.fractions)."""
        out = {"exit_method": exit_method, **self.provenance()}
        if exit_method == "both":
            from metrics import compute_metrics
            pct = lambda v: round(v * 100, 2) if v is not None else None
            # Cash periods carry no _ltp key: identical under both methods.
            for m, rets in (("drop", [r[key] for r in valid]),
                            ("ltp", [r.get(f"{key}_ltp", r[key]) for r in valid])):
                p = compute_metrics(rets, [r[spy_key] for r in valid], periods_per_year,
                                    risk_free_rate=risk_free_rate, fractions=fractions)["portfolio"]
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
                **({"traded_only": True, "zero_volume_rows_moved": self.zero_volume_moved,
                    "close_basis_fills": self.close_basis} if self.traded_only else {}),
                **({"stale_rows_moved": self.stale_moved} if self.stale else {}),
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


# The run window (METHODOLOGY.md, "Run Window"): a run ends on the latest close every market in it has.
END_DATE_MIN_STUB_DAYS = 7    # a shorter final stub isn't held: the last grid date is a full period's exit
END_DATE_LAG_WARN_DAYS = 5    # a leg whose latest close is further behind the latest leg's warns
DATA_END_LOOKBACK_DAYS = 21
# A good day: each exchange traded at least this share of its median line count (the ratio of data
# check T's closed-day rule, DATA_CHECKS_V1, kept apart so the run window doesn't move with dc versions).
DATA_END_MIN_COVERAGE = 0.5


def window_legs(exchanges_by_leg):
    """{universe: (exchanges, benchmark symbol)}: a run's legs as RunWindow takes them."""
    return {u: (exs, get_local_benchmark(exs)[0]) for u, exs in exchanges_by_leg.items()}


def good_days(client, legs, until, lookback=DATA_END_LOOKBACK_DAYS, ratios=None):
    """{leg: {"YYYY-MM-DD", ...}}: each leg's good days in the lookback days to `until`.

    legs is {name: (exchanges, benchmark_symbol)}. A good day has a bar of the leg's benchmark, and on it
    every exchange of the leg (one with rows in the lookback) traded at least DATA_END_MIN_COVERAGE of its
    median daily count of traded lines (stock_eod rows with volume > 0, as data check T counts them): FMP
    writes holiday rows with copied prices and no volume, and a day still loading has too few lines.
    ratios: traded_ratios' value for the same days, when the caller has it; else one query."""
    ratios, bench_days = ratios or traded_ratios(client, legs, until, lookback)
    return {name: {d for d in bench_days[name]
                   if ratios[name] and all(r.get(d, 0.0) >= DATA_END_MIN_COVERAGE for r in ratios[name].values())}
            for name in legs}


def traded_ratios(client, legs, until, lookback=DATA_END_LOOKBACK_DAYS):
    """({leg: {exchange: {"YYYY-MM-DD": traded lines / median}}}, {leg: benchmark bar days}) over the
    lookback days to `until`, for good_days and scripts/data_end.py, in one query. An exchange with no rows
    is left out."""
    import statistics
    since, until = (until - timedelta(days=lookback)).isoformat(), until.isoformat()
    exchanges = sorted({e for exs, _ in legs.values() for e in exs})
    rows = client.query(f"""
        SELECT 'lines' AS kind, p.exchange AS name, e.date AS d,
               COUNT(DISTINCT e.symbol) FILTER (WHERE e.volume > 0) AS n
        FROM stock_eod e
        JOIN (SELECT DISTINCT symbol, exchange FROM profile WHERE exchange IN ({_sql_list(exchanges)})) p
          ON p.symbol = e.symbol
        WHERE e.date >= '{since}' AND e.date <= '{until}'
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT DISTINCT 'benchmark', symbol, date, NULL FROM stock_eod
        WHERE symbol IN ({_sql_list(sorted({b for _, b in legs.values()}))})
          AND date >= '{since}' AND date <= '{until}' AND adjClose > 0
    """, timeout=300) or []
    per_ex, bench_days = {}, {}
    for r in rows:
        if r["kind"] == "lines":
            per_ex.setdefault(r["name"], {})[str(r["d"])[:10]] = int(r["n"] or 0)
        else:
            bench_days.setdefault(r["name"], set()).add(str(r["d"])[:10])
    ratios = {}
    for name, (exs, _) in legs.items():
        ratios[name] = {}
        for e in exs:
            days = per_ex.get(e)
            med = statistics.median(days.values()) if days else 0
            if med > 0:
                ratios[name][e] = {d: n / med for d, n in days.items()}
    return ratios, {name: bench_days.get(bench, set()) for name, (_, bench) in legs.items()}


class RunWindow:
    """A run's end date and the rebalance grid it implies (METHODOLOGY.md, "Run Window").

    latest(): end_date is the latest day every leg traded (good_days). pinned(): a given date that every
    leg traded (--end-date). typed(): a typed end year (--end-year), as before the run window existed:
    today's grid, today's exits and no new results key. A topic builds one window per run (every leg of a
    --global run shares it) and per leg: for_leg(), grid() before the loop, exit_offset() at each exit,
    mark() on the rows after it, then fractions(), annual() and fields() for the results."""

    def __init__(self, end_date=None, requested=None, data_end_by_market=None, checked_at=None, end_year=None):
        self.end_date, self.requested, self.end_year = end_date, requested, end_year
        self.data_end_by_market, self.checked_at = data_end_by_market, checked_at
        self.data_end = self.partial_exit = self.last_rebalance = None
        self.offset_days = self.period_days = None
        self._grid_done = False

    @property
    def legacy(self):
        """A typed end year, as before the run window: no end date."""
        return self.end_date is None

    @classmethod
    def latest(cls, client, legs, today=None, stream=None, ratios=None):
        """end_date: the latest day before the run (UTC) that is a good day (good_days) for every leg: a
        market still trading already has a bar for the run day, before its close. Each leg's data_end is
        its own latest good day; a leg more than END_DATE_LAG_WARN_DAYS behind the latest one warns.
        ratios: traded_ratios' value up to the day before the run, when the caller has it."""
        import sys
        from datetime import datetime, timezone
        good = good_days(client, legs, (today or datetime.now(timezone.utc).date()) - timedelta(days=1),
                         ratios=ratios)
        empty = sorted(n for n, g in good.items() if not g)
        if empty:
            raise ValueError(f"no good day in the last {DATA_END_LOOKBACK_DAYS} days for {', '.join(empty)}")
        by = {n: max(g) for n, g in good.items()}
        common = set.intersection(*good.values())
        if not common:
            raise ValueError(f"no day in the last {DATA_END_LOOKBACK_DAYS} days is a good day for every leg")
        end, newest = max(common), max(by.values())
        late = sorted(n for n, d in by.items()
                      if (date.fromisoformat(newest) - date.fromisoformat(d)).days > END_DATE_LAG_WARN_DAYS)
        if late:
            print(f"WARNING run window: {', '.join(f'{n} ({by[n]})' for n in late)} more than "
                  f"{END_DATE_LAG_WARN_DAYS} days behind the latest leg ({newest}); every leg ends on {end}",
                  file=stream or sys.stderr)
        if end < min(by.values()):
            print(f"  run window: {end}, the latest day every leg traded "
                  f"(some leg was closed on {min(by.values())})", file=stream or sys.stderr)
        return cls(end_date=date.fromisoformat(end), requested="latest", data_end_by_market=by,
                   checked_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

    @classmethod
    def pinned(cls, client, legs, end_date, today=None):
        """end_date as given; it must be before the run day (UTC) and a good day (good_days) for every leg."""
        from datetime import datetime, timezone
        if end_date >= (today or datetime.now(timezone.utc).date()):
            raise ValueError(f"--end-date {end_date}: not before the run day, and a market still trading "
                             "has a bar for it before its close")
        good = good_days(client, legs, end_date)
        bad = sorted(n for n, g in good.items() if end_date.isoformat() not in g)
        if bad:
            raise ValueError(f"--end-date {end_date}: not a good day (a benchmark bar and enough lines traded) "
                             f"for {', '.join(bad)}")
        return cls(end_date=end_date, requested=end_date.isoformat())

    @classmethod
    def typed(cls, end_year=None):
        """A legacy window: grid() ends in end_year as generate_rebalance_dates does. Without end_year it
        only prices exits and writes rows as before (a topic loop's default)."""
        return cls(end_year=end_year)

    @classmethod
    def from_args(cls, args, client, exchanges_by_leg):
        """From add_end_date_arg's flags (--end-year, else --end-date: a date, or latest), for the run's legs
        as {universe: exchanges}."""
        if getattr(args, "end_year", None) is not None:
            return cls.typed(args.end_year)
        legs = window_legs(exchanges_by_leg)
        if args.end_date != "latest":
            return cls.pinned(client, legs, args.end_date)
        return cls.latest(client, legs)

    def for_leg(self, name):
        """This window for one leg: its own data_end, and a grid of its own."""
        import copy
        w = copy.copy(self)
        w.data_end = (self.data_end_by_market or {}).get(name)
        w.partial_exit = w.last_rebalance = w.offset_days = w.period_days = None
        w._grid_done = False
        return w

    def grid(self, start_year, frequency, months=None, offset_days=1):
        """Rebalance dates, the final exit last, so `for i in range(len(dates) - 1)` loops run as before.

        The grid stops at the last grid date whose execution day (the date plus offset_days) is on or
        before end_date. That date is the last rebalance, held to end_date, when end_date is at least
        END_DATE_MIN_STUB_DAYS after it: end_date is then the final exit (partial_exit). Otherwise it's
        the final exit of a full period."""
        if self.legacy:
            return generate_rebalance_dates(start_year, self.end_year, frequency, months)
        end = self.end_date
        every = generate_rebalance_dates(start_year, end.year + 1, frequency, months)
        dates = [d for d in every if d + timedelta(days=offset_days) <= end]
        if not dates:
            raise ValueError(f"no rebalance date from {start_year} executes by {end}")
        last = dates[-1]
        self._grid_done = True
        if (end - last).days < END_DATE_MIN_STUB_DAYS:
            self.partial_exit = None
            return dates
        self.partial_exit, self.last_rebalance, self.offset_days = end, last, offset_days
        self.period_days = (every[len(dates)] - last).days
        return dates + [end]

    def exit_offset(self, exit_date, offset_days):
        """The offset to price an exit with: 0 (the close on end_date) at end_date, else offset_days. Only
        the partial exit falls on end_date: a full period's exit executes on or before it."""
        return 0 if self.end_date is not None and exit_date == self.end_date else offset_days

    def fetch_end(self, d, days):
        """End of the fetch window [d, d + days], cut at end_date so no later bar reaches the cache."""
        e = d + timedelta(days=days)
        return e if self.end_date is None else min(e, self.end_date)

    def held_days(self):
        """The partial period's calendar days, from its execution day to end_date."""
        return (self.partial_exit - self.last_rebalance - timedelta(days=self.offset_days)).days

    def fraction(self):
        """The partial period's share of a full period: held_days over the full period's grid days, 6 dp
        (the metrics use this rounded value)."""
        return round(self.held_days() / self.period_days, 6)

    def _gridded(self):
        if not self._grid_done:
            raise RuntimeError("RunWindow: call grid() on this leg's window first")

    def mark(self, results):
        """Set each period_data row's partial flag (and the partial row's fraction) in place, by its
        exit_date: one call after the loop, so no append site can miss it. A legacy window adds nothing."""
        if self.legacy:
            return results
        self._gridded()
        end = self.partial_exit.isoformat() if self.partial_exit is not None else None
        for r in results:
            r["partial"] = r["exit_date"] == end
            if r["partial"]:
                r["fraction"] = self.fraction()
        return results

    def fractions(self, rows):
        """compute_metrics' fractions for rows (None on a legacy window, so today's path runs). Raises when
        a row wasn't marked."""
        if self.legacy:
            return None
        if any("partial" not in r for r in rows):
            raise ValueError("RunWindow.fractions: a row without its partial flag (call mark() on the results)")
        return [r["fraction"] if r["partial"] else 1.0 for r in rows]

    def annual(self, valid, periods_per_year, key="portfolio_return", bench_key="spy_return"):
        """(annual_returns, partial_year) over the measured rows: metrics.annual_with_partial, or on a legacy
        window compute_annual_returns as before and None."""
        from metrics import annual_with_partial, compute_annual_returns
        args = ([r[key] for r in valid], [r[bench_key] for r in valid], [r["rebalance_date"] for r in valid])
        if self.legacy:
            return compute_annual_returns(*args, periods_per_year), None
        return annual_with_partial(*args, [r["exit_date"] for r in valid], self.fractions(valid), periods_per_year)

    def priced_to(self, con, benchmark_symbol, valid, offset_days):
        """The date of the close the last measured exit uses: the benchmark's exit bar of the last measured
        row. That's end_date when the partial period is measured, and the bar after the last grid date
        when the stub was under END_DATE_MIN_STUB_DAYS. None on a legacy window."""
        if self.legacy or not valid:
            return None
        x = date.fromisoformat(valid[-1]["exit_date"])
        bar = get_prices(con, [benchmark_symbol], x, offset_days=self.exit_offset(x, offset_days), with_epochs=True)
        return utc_date(bar[benchmark_symbol][1]).isoformat() if benchmark_symbol in bar else None

    def fields(self, con, benchmark_symbol, valid, periods_per_year, partial_year, offset_days):
        """The run window's results keys for a leg (METHODOLOGY.md, "Run Window"), `years` (1 dp: the span)
        among them; {} on a legacy window, whose `years` build_output already wrote. valid: the measured
        rows; partial_year: annual()'s second value; con, benchmark_symbol and offset_days for priced_to."""
        if self.legacy:
            return {}
        self._gridded()
        from metrics import span_years
        fr = self.fractions(valid)
        span = span_years(fr, periods_per_year)
        pct = lambda v: round(v * 100, 2)
        part = None
        if any(r["partial"] for r in valid):   # the partial period, when it's measured
            part = {"rebalance_date": self.last_rebalance.isoformat(),
                    "entry_day": (self.last_rebalance + timedelta(days=self.offset_days)).isoformat(),
                    "exit_date": self.partial_exit.isoformat(),
                    "days": self.held_days(), "period_days": self.period_days, "fraction": self.fraction()}
        py = None
        if partial_year is not None:
            py = {k: partial_year[k] for k in ("year", "start", "end", "periods", "year_fraction")}
            py.update({"portfolio": pct(partial_year["portfolio"]), "spy": pct(partial_year["benchmark"]),
                       "excess": pct(partial_year["excess"])})
        return {"years": round(span, 1), "end_date_requested": self.requested, "end_date": self.end_date.isoformat(),
                "priced_to": self.priced_to(con, benchmark_symbol, valid, offset_days),
                "data_end": self.data_end, "data_end_by_market": self.data_end_by_market,
                "data_end_checked_at": self.checked_at,
                "full_periods": sum(1 for f in fr if f == 1), "span_years": round(span, 6),
                "partial_period": part, "partial_year": py}


def with_partial_year(annual, partial_year):
    """annual (compute_annual_returns' rows) plus partial_year labelled '<year>p', for a printed table."""
    return annual + ([{**partial_year, "year": f"{partial_year['year']}p"}] if partial_year else [])


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
# in (gate 15's DATA_CHECKS_TOPICS in scripts/verify_floor_guard.py holds the list).
# Every other topic calls none of this and keeps filter_returns' max_single_return=2.0.
#
#   T traded bars   clean_price_cache(): closed-day rows, zero-volume rows of lines that traded in
#                   the same window, flat lines with no volume and non-positive prices leave the cache
#                   before remove_price_oscillations; then a thin class's replacement bar off its liquid
#                   sibling's scale.
#                   LtpExits(traded_only=True) applies rule 2 to fills.
#   N NVDR twins    mark_nvdr_twins() and isin_dedup_sql(twin_key=True).
#   M market cap    load_cap_check() and cap_used_sql() at every market-cap floor.
#   F factor flags  DataChecks records stage-1 drift and split events; it changes no price. dc-3 adds a hand
#                   list (FACTOR_HAND, DataChecks.hand): reviewed returns for the flags that move a leg.
#   L large moves   DataChecks lists every kept window-bar holding with |ln(1+r)| >= ln 2.
#   S stale repeats dc-2 only (DATA_CHECKS_V2): a run of identical traded rows keeps its first row, in
#                   clean_price_cache() and in LtpExits(traded_only=True, cfg=...) fills.
# All thresholds live in DATA_CHECKS_V1 (dc-1), DATA_CHECKS_V2 (dc-2) or DATA_CHECKS_V3 (dc-3), which every
# results file carries.
# ---------------------------------------------------------------------------

DATA_CHECKS_V1 = {
    "version": "dc-1",
    "fetch_window_days": 11,
    "closed_day_ratio": 0.5,
    "closed_day_min_ref": 20,
    "closed_day_min_other_dates": 3,
    "closed_day_skip": ("a date is not tested when its window has fewer than 3 other dates or their median "
                        "traded count is under 20; closed_day_test_skipped counts windows with such a date"),
    "zero_volume": ("moved when the same line has a traded row in the same window, or when the line has none "
                    "and its close changes on fewer than no_volume_min_changes days there, day over day in date "
                    "order (a flat line, or a stale quote that steps once, with no volume is not buyable); a line "
                    "with no traded row whose close changes on that many days or more keeps every row"),
    "no_volume_min_changes": 2,
    "nonpositive_price": "rows with adjClose NULL or <= 0 moved",
    "sibling": {"min_shared_fy": 2, "gap": 1.25, "ref_days": 400, "ref_min_days": 5, "ref_min_windows": 2,
                "stable_share": 0.8,
                "rule": ("siblings are lines that carry an identical FY key_metrics cap in min_shared_fy FYs or "
                         "more; a line is tested in a window only when T moved a row before its first remaining "
                         "row there (the anchor: the earliest moved row), and only against a thicker sibling "
                         "(median close x volume over the reference days); ref = the median of the per-window "
                         "median close ratios over the cache's other windows within ref_days (one window each "
                         "side on an annual leg), with ref_min_windows windows, ref_min_days traded days of both, "
                         "stable_share of those days within gap of ref, and the anchor's ratio within gap of ref; "
                         "from the first remaining row, a row more than gap from both ref and the anchor's ratio, "
                         "for every sibling that can test it, moves; the first row on scale or untestable stops "
                         "it")},
    "benchmarks_exempt": True,
    "twins": ("SET X-R.BK takes X.BK's ISIN as its security key when X.BK is in the leg, the ISINs share "
              "characters 1 to 6 and character 10 of the NVDR's is R; within a key: buyable at entry, then "
              "the ordinary line, then isActivelyTrading, home currency, symbol"),
    "cap": {"pf_min": 3.0, "turnover_max": 1e-4, "bars": 20, "lookback_days": 45, "min_traded_bars": 10,
            "k_min": 3.0, "eps_min": 0.05, "net_income_min": 1e6, "k_tol": 1.5, "split_max": 1e4,
            "split_min": 2.0, "split_dup_days": 120, "share_tol": 1.5, "share_step": [0.5, 2.0], "lower_only": True,
            "order": ("gate on pf or the later consolidations; turnover over every line with the same FY cap "
                      "(lines that share an identical FY cap in sibling.min_shared_fy FYs or more); k within "
                      "k_tol of pf lowers first and turnover is tested on the lowered cap; a failing cap is "
                      "lowered by the divisor (and k) when turnover clears on the lowered cap, else "
                      "ineligible; a k that disagrees with pf keeps turnover-then-k; unknown turnover lowers; "
                      "the divisor needs evidence beyond the split records: a measured price factor P (raw "
                      "close against stock_eod at the period end, or the same line's nearest FY with no split "
                      "record between) of pf_min or more gives P times the later forward splits, a P under "
                      "pf_min gives P times the records FMP's FY share count was restated for, no P gives the "
                      "records at which that count steps; a record repeating the ratio of a record kept within "
                      "split_dup_days before it counts once; a row is only made ineligible when pf opened the "
                      "gate and every line of its group has bars in the lookback, else a failing cap lowers; a "
                      "failing row whose divisor is within 1e-9 of 1 keeps FMP's cap as unchecked, counted in "
                      "cap_rows_flagged")},
    "factor": {"band": [1 / 1.05, 1.15], "split_min": 2.0, "mode": "flag only: no price or return is changed"},
    "fill_basis": {"min_reversals": 2,
                   "rule": ("an LTP fill books its close over the entry close (basis 'close') when the line's "
                            "adjClose / close flips by more than factor.band's upper bound on consecutive rows "
                            "while the close moves less than that bound, the flips reverse direction "
                            "min_reversals times or more from the fetched context to the window end, and the "
                            "fill's drift against the entry bar leaves factor.band")},
    "large_move_log": math.log(2),
}

# dc-2: dc-1 plus rule S. A topic opts in at its rerun, so a topic published on dc-1 keeps its files.
DATA_CHECKS_V2 = {**DATA_CHECKS_V1, "version": "dc-2", "stale_repeat": {
    "min_run": 4,
    "rule": ("in date order over one line's traded rows (volume > 0 and adjClose > 0, one row per date, zero-volume "
             "rows skipped), a run is consecutive rows with the same close and the same volume; in a run of min_run "
             "rows or more every row after the first is untraded (a feed printing its last real bar again, as "
             "PTNM's halt from 2025-10-03 or a market's holiday): T moves those rows from each window after "
             "nonpositive_price, and an LTP fill (traded_only) loses them over its rows to the exit window's end, "
             "so it lands on the run's first row, the last real trade")}}


def stale_repeat_sql(rows, min_run):
    """Rule S (DATA_CHECKS_V2["stale_repeat"]) as SQL: (symbol, k, trade_epoch) of every traded row after the
    first in a run of min_run or more. `rows` is SQL with symbol, k (the partition: T's window, or one per
    fill), trade_epoch, adjClose, close and volume."""
    return f"""
        WITH one AS (
            SELECT symbol, k, trade_epoch, close, volume FROM (
                SELECT symbol, k, trade_epoch, close, volume,
                       ROW_NUMBER() OVER (PARTITION BY symbol, k, trade_epoch
                                          ORDER BY adjClose DESC, close DESC NULLS LAST, volume DESC) AS rn
                FROM ({rows}) x WHERE volume > 0 AND adjClose > 0) y WHERE rn = 1),
        brk AS (
            SELECT *, CASE WHEN close = LAG(close) OVER w AND volume = LAG(volume) OVER w THEN 0 ELSE 1 END AS b
            FROM one WINDOW w AS (PARTITION BY symbol, k ORDER BY trade_epoch)),
        run AS (
            SELECT *, SUM(b) OVER (PARTITION BY symbol, k ORDER BY trade_epoch ROWS UNBOUNDED PRECEDING) AS r
            FROM brk),
        pos AS (
            SELECT *, COUNT(*) OVER (PARTITION BY symbol, k, r) AS n,
                   ROW_NUMBER() OVER (PARTITION BY symbol, k, r ORDER BY trade_epoch) AS i FROM run)
        SELECT symbol, k, trade_epoch FROM pos WHERE n >= {int(min_run)} AND i > 1"""


# dc-3: dc-2 plus check F's hand list. Every band flag whose close-basis swap moves its leg's excess by
# review_min_pp or more has an entry: (symbol, rebalance date, exit date, kind, booked return, range, the adjClose
# return it was checked against, the topic/legs it was found in, evidence). Kinds: "rebuilt" books the rebuilt
# return (traded close plus the cash paid, on the close's basis, reinvested at the ex-date close; a range books
# its midpoint), "real" leaves the adjClose return, "kept" was reviewed and can't be rebuilt (adjClose stands).
FACTOR_HAND = (
    ("AKSO.OL", "2024-10-01", "2025-01-01", "real", None, None, 0.359462, ("deleveraging/OSL",),
     "NOK 21 extraordinary dividend ex 2024-11-25 (41.7% of the close), the close falling 40.8% that day"),
    ("DLAR.L", "2008-07-01", "2009-07-01", "rebuilt", None, (0.0157, 0.0538), 1.51701, ("asset-growth/LSE",),
     "De La Rue's 2008 return of cash, 305p a share with a 25-for-39 consolidation that keeps the price level: "
     "the close steps up when the new shares deal and adjClose books the cash on top; priced from the new "
     "shares' first open and close with the cash and the ordinary dividends (asset-growth README, PR #35)"),
    ("3003.SR", "2020-07-01", "2021-07-01", "rebuilt", 0.5891, None, 2.05322,
     ("asset-growth/SAU", "cyclical-timing/SAU"),
     "City Cement's 2020 capital cut cancelled one share in 3.85 at SAR 10 each; adjClose books SAR 10 on every "
     "share and the close treats the cut as a reverse split; with the cash per share held and the ordinary "
     "dividends (asset-growth README, PR #35)"),
    ("ABG.JO", "2013-07-01", "2013-10-01", "rebuilt", 0.0100, None, -0.56947, ("deleveraging/JNB",),
     "adjClose / close alternates between two bases (five steps of 0.85 in log, the up-steps on zero-volume rows); "
     "close plus the 350c dividend ex 2013-09-09, the close falling 2.8% that day"),
    ("KINV-B.ST", "2014-07-01", "2015-07-01", "rebuilt", -0.0499, None, 0.704348, ("asset-growth/STO",),
     "the 2015-05-19 dividend row is about 20 times Kinnevik's real SEK 7 redemption on the same basis, the close "
     "flat that day; the A line's return, whose factor moves by the real payout (asset-growth README, PR #35)"),
    ("TSG.JO", "2011-10-01", "2012-01-01", "rebuilt", 0.0829, None, -0.395584, ("deleveraging/JNB",),
     "two adjClose bases (five steps of 0.58 in log, up-steps on zero-volume rows); close plus the 20c dividend"),
    ("KINV-B.ST", "2020-07-01", "2021-07-01", "rebuilt", 1.3151, None, 2.50489, ("asset-growth/STO",),
     "the 2020-08-25 dividend row copies the 2014 one (the real event was a SEK 7 redemption); the A line's "
     "return with the 2021 Zalando distribution in kind reinvested at its ex-date close (asset-growth README, "
     "PR #35)"),
    ("SCHB.OL", "2024-04-01", "2024-07-01", "real", None, None, 0.192835, ("deleveraging/OSL",),
     "Schibsted's NOK 77.10 extraordinary dividend ex 2024-05-30, the close falling 19.7%"),
    ("CRDA.L", "2005-07-01", "2006-07-01", "rebuilt", 0.2314, None, 1.03534, ("asset-growth/LSE",),
     "two adjClose bases (five steps of about 0.52 in log, two of the up-steps on zero-volume rows); close plus "
     "the 4.6p and 9.5p dividends"),
    ("0127.HK", "2016-07-01", "2017-07-01", "real", None, None, 0.243047, ("asset-growth/HKSE",),
     "four special dividends (HK$2.10, 3.23, 2.00, 2.91), the close falling by each on its ex-date"),
    ("SCHA.OL", "2024-04-01", "2024-07-01", "real", None, None, 0.179918, ("deleveraging/OSL",),
     "Schibsted's NOK 77.10 extraordinary dividend ex 2024-05-30, the close falling 16.8%"),
    ("1428.HK", "2024-07-01", "2025-07-01", "real", None, None, 4.80272, ("asset-growth/HKSE",),
     "HK$0.33 dividend ex 2024-08-21 (18.0% of the close), the close falling 19.1%"),
    ("CRDA.L", "2006-07-01", "2007-07-01", "rebuilt", 0.4581, None, -0.120111, ("cyclical-timing/LSE",),
     "the 2005 holding's two adjClose bases switching back (0.50 in log); close plus the 4.9p and 10.2p dividends"),
    ("LIGT3.SA", "2008-07-01", "2009-07-01", "rebuilt", 0.8803, None, 1.10997, ("asset-growth/SAO",),
     "the 2009-03-19 factor step implies a 17.8% payout against a R$1.945 (7.6%) dividend, the close falling "
     "9.5% that day; close plus the dividend"),
    ("ETP", "2014-07-01", "2015-07-01", "rebuilt", -0.0191, None, -0.749416, ("cyclical-timing/NYSE_NASDAQ_AMEX",),
     "two adjClose bases (flips of about 1.4 in log) and dividend rows of another security (monthly 0.07); close "
     "plus Energy Transfer Partners' own quarterly distributions from its 8-Ks: $0.955 ex 2014-07-31, $0.975 ex "
     "2014-10-30, $0.995 ex 2015-02-04, $1.015 ex 2015-05-06"),
    ("ELM.L", "2001-07-01", "2001-10-01", "rebuilt", -0.4316, None, -0.0123491, ("deleveraging/LSE",),
     "two adjClose bases (0.553 in log, 25 times in three months); the close falls from 63.6 to 51.7 and to 36 "
     "after September 11, with no cash paid"),
    ("600997.SS", "2006-01-01", "2006-04-01", "rebuilt", 0.1111, None, 0.548673, ("deleveraging/SHZ_SHH",),
     "halted 2005-12-19 to 2006-01-11 for the share-structure reform; 2006-01-12, the entry bar, already trades "
     "after the bonus (close 2.99 to 2.61) but its adjClose sits on the pre-reform basis, so adjClose credits "
     "the bonus to a holder who bought after it; close return, no cash paid"),
    ("SDF.DE", "2002-07-01", "2003-07-01", "rebuilt", -0.0937, None, 0.0858896, ("cyclical-timing/XETRA",),
     "two adjClose bases (0.565 in log); close plus the EUR 1 dividend of 2003-05-08 on the close's basis (0.237)"),
    ("PETR3.SA", "2022-07-01", "2023-07-01", "real", None, None, 0.684798, ("asset-growth/SAO",),
     "four Petrobras dividends (17.3%, 11.1%, 10.0%, 5.2% of the close), the close falling by each"),
    ("RAM.BK", "2017-10-01", "2018-01-01", "rebuilt", 0.0130, None, -0.778537, ("deleveraging/SET",),
     "two adjClose bases (1.52 in log, about 30 steps on zero- or thin-volume rows); close plus the THB 8 "
     "dividend on the close's basis (0.08)"),
    ("1919.HK", "2022-07-01", "2023-07-01", "real", None, None, 0.0597015, ("cyclical-timing/HKSE",),
     "COSCO Shipping special dividends (22.5%, 17.6% of the close), the close falling by each"),
    ("000423.SZ", "2006-07-01", "2007-07-01", "real", None, None, 3.87437, ("defensive-quality/SHZ_SHH",),
     "halted 2007-05-15 to 06-01 for the share-structure reform, which gave holders 4 bonus shares per 10; the "
     "close is unadjusted across it: 1.4 x 28.20 against 12.47 is +217%, as adjClose's +220%"),
    ("1171.HK", "2023-07-01", "2024-07-01", "rebuilt", 0.3732, None, 0.470845, ("cyclical-timing/HKSE",),
     "HK$4.6734 per H share with 5 bonus shares per 10 (ex 2023-07-05) and HK$1.6352 with 3 per 10 (ex "
     "2024-06-26), per the HKEX notices; the close is adjusted for both bonuses, but the factor over-applies "
     "both payouts; close plus HK$2.3966 and 1.2579 on the close's basis"),
)
FACTOR_HAND_RULE = ("each band flag whose close-basis swap moves its leg's excess by review_min_pp or more is "
                    "reviewed by hand and listed by holding span (symbol, rebalance date, exit date): rebuilt (book "
                    "the traded close plus the cash paid, on the close's basis, reinvested at the ex-date close; a "
                    "range books its midpoint), real (the adjClose return stands) or kept (reviewed, can't be "
                    "rebuilt: adjClose stands); an entry applies only while its holding's adjClose return is within "
                    "log_tolerance of the one it was checked against (ln(1 + r)), else the holding books adjClose "
                    "and is listed as hand_stale; flags under review_min_pp, and flags on LTP fills, book adjClose")


def _hand_entries(rows):
    out = []
    for s, reb, ex, kind, booked, rng, radj, found, ev in rows:
        if kind == "rebuilt" and booked is None:
            booked = round((rng[0] + rng[1]) / 2, 6)
        out.append({"symbol": s, "rebalance_date": reb, "exit_date": ex, "kind": kind, "booked": booked,
                    "range": list(rng) if rng else None, "return_adj": radj, "found_in": list(found), "evidence": ev})
    return out


def _hand_config(rows):
    import hashlib
    entries = _hand_entries(rows)
    return {"rule": FACTOR_HAND_RULE, "review_min_pp": 0.1, "log_tolerance": 0.005, "entries": entries,
            "entries_sha256": hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()}


DATA_CHECKS_V3 = {**DATA_CHECKS_V2, "version": "dc-3", "factor_hand": _hand_config(FACTOR_HAND)}


def _sig(x, digits=6):
    """x to `digits` significant digits, None passing through (keeps results files readable)."""
    return None if x is None else float(f"{x:.{digits}g}")


def clean_price_cache(con, bench_symbols, rebalance_dates, cfg=DATA_CHECKS_V1, table="prices_cache",
                      siblings=None):
    """Check T on a topic's windowed price cache. Call it before remove_price_oscillations.

    Per fetch window W = [d, d + fetch_window_days] of each rebalance date d (overlapping windows
    merged), over non-benchmark rows, in this order:
      1 closed_day         traded(D) = rows on date D with volume > 0, ref(D) = the median of traded
                           over W's other dates. D is closed when traded(D) < closed_day_ratio x
                           ref(D), and every non-benchmark row on D moves. D is not tested when W
                           has fewer than closed_day_min_other_dates other dates or ref(D) is under
                           closed_day_min_ref.
      2 zero_volume        a (symbol, W) with a row of volume > 0 loses its rows with volume <= 0 or
                           NULL. A (symbol, W) with no traded row is counted; it keeps every row when
                           its close changes on no_volume_min_changes (2) days or more there, day over
                           day in date order (one close per date, a date with no close skipped; a feed
                           that prints prices with no volume, even one ticking between two prices like
                           BH-R.BK's 48.0 and 48.5), and loses them all otherwise (reason
                           flat_no_volume: a suspended quote, or a stale one that steps once like
                           BC88.L's 95 then 70, is not buyable).
      3 nonpositive_price  rows with adjClose NULL or <= 0.
     3b stale_repeat       only when cfg has rule S (dc-2): a (symbol, W)'s traded rows after the first in a run
                           of stale_repeat.min_run or more with the same close and volume (stale_repeat_sql).
      4 sibling_gap        only with `siblings`, a table of (symbol, filing_epoch, market_cap) such
                           as cap_inputs: lines that share an identical FY cap in two FYs or more are
                           share classes of one company. Where steps 1 to 3 moved the bar a line's
                           window opened on, the replacement bar (and the ones after it, while they
                           stay off) moves when it sits off a thicker sibling's scale and off the
                           moved bar's own ratio to it (see _sibling_gaps, DATA_CHECKS_V1["sibling"]).
    Moved rows land in prices_dropped(symbol, trade_epoch, adjClose, close, volume, reason,
    window_start). The cache needs symbol, trade_epoch (UTC midnight), adjClose, close and volume.
    bench_symbols may carry SQL quotes ("'SPY'"). Benchmark rows are never touched.
    Returns {rows_moved, symbol_windows_without_volume, flat_windows_moved, closed_day_test_skipped,
    closed_dates, sibling_gap_rows, sibling_gaps}. rows_moved.zero_volume counts the rows of both
    rule-2 cases, and rows_moved.stale_repeat (dc-2 only) step 3b's; sibling_gap rows are counted apart
    (sibling_gap_rows) and listed in sibling_gaps.
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
    # n_change: days the close differs from the previous date's, one close per date (get_prices' row order),
    # a date whose row has no close skipped
    con.execute("""CREATE OR REPLACE TEMP TABLE _dc_sw AS
        WITH one AS (
            SELECT symbol, ws, day, close AS px FROM _dc_rows
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, ws, day
                                       ORDER BY adjClose DESC NULLS LAST, close DESC NULLS LAST,
                                                volume DESC NULLS LAST) = 1),
        ch AS (
            SELECT symbol, ws, CASE WHEN px <> LAG(px) OVER (PARTITION BY symbol, ws ORDER BY day)
                                    THEN 1 ELSE 0 END AS c
            FROM one WHERE px IS NOT NULL)
        SELECT t.symbol, t.ws, t.traded, COALESCE(n.n_change, 0) AS n_change
        FROM (SELECT symbol, ws, COALESCE(BOOL_OR(volume > 0), FALSE) AS traded FROM _dc_rows
              GROUP BY symbol, ws) t
        LEFT JOIN (SELECT symbol, ws, SUM(c) AS n_change FROM ch GROUP BY symbol, ws) n
          ON n.symbol = t.symbol AND n.ws = t.ws""")
    nc = int(cfg["no_volume_min_changes"])
    no_volume, flat = con.execute("SELECT COUNT(*) FILTER (WHERE NOT traded),"
                                  f" COUNT(*) FILTER (WHERE NOT traded AND n_change < {nc}) FROM _dc_sw").fetchone()
    moved["zero_volume"] = move("(r.volume IS NULL OR r.volume <= 0) AND (r.symbol, r.ws) IN"
                                " (SELECT symbol, ws FROM _dc_sw WHERE traded)", "zero_volume")
    moved["zero_volume"] += move(f"(r.symbol, r.ws) IN (SELECT symbol, ws FROM _dc_sw WHERE NOT traded AND n_change < {nc})",
                                 "flat_no_volume")
    tag()
    moved["nonpositive_price"] = move("r.adjClose IS NULL OR r.adjClose <= 0", "nonpositive_price")
    if "stale_repeat" in cfg:   # dc-2's rule S, per window
        tag()
        con.execute("CREATE OR REPLACE TEMP TABLE _dc_stale AS " + stale_repeat_sql(
            "SELECT symbol, ws AS k, trade_epoch, adjClose, close, volume FROM _dc_rows", cfg["stale_repeat"]["min_run"]))
        moved["stale_repeat"] = move("(r.symbol, r.ws, r.trade_epoch) IN (SELECT symbol, k, trade_epoch FROM _dc_stale)",
                                     "stale_repeat")
    gaps = []
    if siblings is not None:
        tag()
        gaps = _sibling_gaps(con, siblings, cfg["sibling"])
        if gaps:
            con.executemany("INSERT INTO _dc_gap VALUES (?, ?)", [(g["symbol"], g["epoch"]) for g in gaps])
            move("(r.symbol, r.trade_epoch) IN (SELECT symbol, trade_epoch FROM _dc_gap)", "sibling_gap")
    for t in ("_dc_win", "_dc_rows", "_dc_closed", "_dc_sw", "_dc_stale", "_dc_gap"):
        con.execute(f"DROP TABLE IF EXISTS {t}")
    return {"rows_moved": moved, "symbol_windows_without_volume": int(no_volume), "flat_windows_moved": int(flat),
            "closed_day_test_skipped": len(skipped),
            "closed_dates": [{"date": utc_date(day * 86400).isoformat(), "traded": traded, "ref": round(ref, 1)}
                             for _, day, traded, ref in sorted(closed, key=lambda c: c[1])],
            "sibling_gap_rows": len(gaps), "sibling_gaps": [{k: v for k, v in g.items() if k != "epoch"} for g in gaps]}


def _sibling_gaps(con, siblings, c):
    """Check T step 4 on the tagged rows (_dc_rows): a thin line's moved first bar is not replaced by
    a bar off its liquid sibling's scale.

    Siblings: lines that share an identical FY cap in `siblings` in min_shared_fy FYs or more. A
    (symbol, window) is tested only when T moved a row before the line's first remaining row there
    (the bar get_prices reads changed); the earliest moved row is the anchor. The reference against
    a sibling: per other cached window within ref_days (the cache holds only the windows, so on an
    annual leg that is one window each side), the median log close ratio over the days both traded;
    ref = the median of those window medians. The pair tests the line only when: the line is the
    thinner one (lower median close x volume over the reference days); the reference holds
    ref_min_windows windows and ref_min_days days; stable_share of its days sit within gap of ref;
    and the anchor's ratio to the sibling's close that day sits within gap of ref
    (the bar T moved was on scale). From the first remaining row on, a row is off when every sibling
    that can test it puts it more than gap from ref and more than gap from the anchor's ratio (T's
    move opened the gap); the leading run of off rows moves, and the first row on scale or untestable
    stops it. The liquid line is never moved. Returns one dict per row to move: symbol, date, epoch,
    close, sibling, sibling_close, ratio and ref, sorted. Creates _dc_gap(symbol, trade_epoch)."""
    con.execute("CREATE OR REPLACE TEMP TABLE _dc_gap(symbol VARCHAR, trade_epoch BIGINT)")
    gap, ref_s = math.log(float(c["gap"])), int(c["ref_days"]) * 86400
    rows = con.execute(f"""
        WITH sib AS (
            SELECT x.symbol AS a, y.symbol AS b FROM {siblings} x JOIN {siblings} y
              ON x.filing_epoch = y.filing_epoch AND x.market_cap = y.market_cap AND x.symbol <> y.symbol
            GROUP BY x.symbol, y.symbol HAVING COUNT(DISTINCT x.filing_epoch) >= {int(c["min_shared_fy"])}),
        tr AS (SELECT symbol, ws, day, MAX(close) AS close, MAX(close * volume) AS tv FROM _dc_rows
               WHERE volume > 0 AND close > 0 AND symbol IN (SELECT a FROM sib)
               GROUP BY symbol, ws, day),
        first_left AS (SELECT symbol, ws, MIN(trade_epoch) AS e FROM _dc_rows
                       WHERE symbol IN (SELECT a FROM sib) GROUP BY symbol, ws),
        trig AS (SELECT f.symbol, f.ws, MIN(d.trade_epoch) AS e0 FROM first_left f
                 JOIN prices_dropped d ON d.symbol = f.symbol AND d.window_start = f.ws
                 GROUP BY f.symbol, f.ws, f.e HAVING MIN(d.trade_epoch) < f.e),
        anc AS (SELECT t.symbol, t.ws, t.e0 // 86400 AS day, MAX(d.close) AS close FROM trig t
                JOIN prices_dropped d ON d.symbol = t.symbol AND d.trade_epoch = t.e0 WHERE d.close > 0
                GROUP BY t.symbol, t.ws, t.e0),
        allpx AS (SELECT symbol, trade_epoch // 86400 AS day, MAX(close) AS close
                  FROM (SELECT symbol, trade_epoch, close FROM _dc_rows
                        UNION ALL SELECT symbol, trade_epoch, close FROM prices_dropped) u
                  WHERE close > 0 AND symbol IN (SELECT a FROM sib) GROUP BY symbol, trade_epoch // 86400),
        pd AS (SELECT s.a, s.b, ta.ws, ta.day, LN(ta.close / tb.close) AS lr, ta.tv AS tva, tb.tv AS tvb
               FROM sib s JOIN tr ta ON ta.symbol = s.a JOIN tr tb ON tb.symbol = s.b AND tb.day = ta.day),
        pw AS (SELECT a, b, ws, MEDIAN(lr) AS m, COUNT(*) AS n FROM pd GROUP BY a, b, ws),
        ref AS (SELECT t.symbol AS a, w.b, t.ws, MEDIAN(w.m) AS med, COUNT(*) AS nw, SUM(w.n) AS nd
                FROM trig t JOIN pw w ON w.a = t.symbol AND w.ws <> t.ws AND ABS(w.ws - t.ws) <= {ref_s}
                GROUP BY t.symbol, w.b, t.ws),
        liq AS (SELECT r.a, r.b, r.ws, MEDIAN(p.tva) AS la, MEDIAN(p.tvb) AS lb,
                       AVG(CASE WHEN ABS(p.lr - r.med) <= {gap} THEN 1.0 ELSE 0.0 END) AS day_share
                FROM ref r JOIN pd p ON p.a = r.a AND p.b = r.b AND p.ws <> r.ws AND ABS(p.ws - r.ws) <= {ref_s}
                GROUP BY r.a, r.b, r.ws),
        stable AS (SELECT r.a, r.b, r.ws, r.med, LN(n.close / bx.close) AS lra FROM ref r
                   JOIN liq l ON l.a = r.a AND l.b = r.b AND l.ws = r.ws
                   JOIN anc n ON n.symbol = r.a AND n.ws = r.ws
                   JOIN allpx bx ON bx.symbol = r.b AND bx.day = n.day
                   WHERE r.nw >= {int(c["ref_min_windows"])} AND r.nd >= {int(c["ref_min_days"])} AND l.la < l.lb
                     AND l.day_share >= {float(c["stable_share"])}
                     AND ABS(LN(n.close / bx.close) - r.med) <= {gap}),
        cand AS (SELECT r.symbol, r.ws, r.trade_epoch, r.day, MAX(r.close) AS close FROM _dc_rows r
                 JOIN trig t ON t.symbol = r.symbol AND t.ws = r.ws WHERE r.close > 0
                 GROUP BY r.symbol, r.ws, r.trade_epoch, r.day),
        test AS (SELECT c.symbol, c.trade_epoch, st.b, tb.close AS b_close, EXP(st.med) AS ref,
                        ABS(LN(c.close / tb.close) - st.med) > {gap} AND ABS(LN(c.close / tb.close) - st.lra) > {gap} AS off
                 FROM cand c JOIN stable st ON st.a = c.symbol AND st.ws = c.ws
                 JOIN tr tb ON tb.symbol = st.b AND tb.day = c.day),
        verdict AS (SELECT c.symbol, c.ws, c.trade_epoch, c.close, COALESCE(BOOL_AND(t.off), FALSE) AS off,
                           arg_min(t.b, t.b) AS b, arg_min(t.b_close, t.b) AS b_close, arg_min(t.ref, t.b) AS ref
                    FROM cand c LEFT JOIN test t ON t.symbol = c.symbol AND t.trade_epoch = c.trade_epoch
                    GROUP BY c.symbol, c.ws, c.trade_epoch, c.close),
        stop AS (SELECT symbol, ws, MIN(trade_epoch) AS e FROM verdict WHERE NOT off GROUP BY symbol, ws)
        SELECT v.symbol, v.trade_epoch, v.close, v.b, v.b_close, v.ref FROM verdict v
        LEFT JOIN stop s ON s.symbol = v.symbol AND s.ws = v.ws
        WHERE v.off AND (s.e IS NULL OR v.trade_epoch < s.e)
        ORDER BY v.symbol, v.trade_epoch""").fetchall()
    return [{"symbol": s, "date": utc_date(e).isoformat(), "epoch": int(e), "close": cl, "sibling": b,
             "sibling_close": bc, "ratio": _sig(cl / bc), "ref": _sig(ref)} for s, e, cl, b, bc, ref in rows]


def mark_nvdr_twins(con, table="isin_map"):
    """Check N: key each SET NVDR line (X-R.BK) to its ordinary line's security.

    Adds sec_key VARCHAR and nvdr BOOLEAN to load_isin_map()'s table. X-R.BK gets sec_key = X.BK's
    normalised ISIN and nvdr = TRUE when X.BK is in the map, both ISINs share characters 1 to 6 and
    character 10 of the NVDR's ISIN is R. Every other row keeps NULLs, so isin_dedup_sql(twin_key=True)
    keys it on its own ISIN. Returns the number of NVDR symbols linked.
    """
    con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sec_key VARCHAR")
    con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS nvdr BOOLEAN")
    con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS class_twin BOOLEAN")   # ur-2's twins (mark_class_twins)
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
                         CASE WHEN COALESCE(_i.nvdr, FALSE) OR COALESCE(_i.class_twin, FALSE) THEN 1 ELSE 0 END,
                         CASE WHEN _i.live THEN 0 ELSE 1 END, CASE WHEN _i.home THEN 0 ELSE 1 END,
                         _c.symbol) AS _isin_rn
            FROM ({candidates_sql}) _c
            LEFT JOIN (SELECT symbol, MIN(NULLIF(UPPER(TRIM(isin)), '')) AS isin,
                              BOOL_OR(isActivelyTrading) AS live, BOOL_OR(home_ccy) AS home,
                              MIN(sec_key) AS sec_key, BOOL_OR(nvdr) AS nvdr, BOOL_OR(class_twin) AS class_twin
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
                 " pf DOUBLE, pf_source VARCHAR, pf_cons DOUBLE, pf_fwd DOUBLE, u DOUBLE, n_traded BIGINT,"
                 " lines BIGINT, k DOUBLE, rule VARCHAR, cap_div DOUBLE, sh_restated DOUBLE, sh_step DOUBLE)")
CAP_INPUTS_DDL = ("CREATE OR REPLACE TABLE cap_inputs(symbol VARCHAR, filing_epoch BIGINT, market_cap DOUBLE,"
                  " net_income DOUBLE, eps DOUBLE, shares DOUBLE, n_bars BIGINT, n_traded BIGINT,"
                  " turnover DOUBLE, pf_raw DOUBLE, pf_splits DOUBLE, pf_cons DOUBLE, pf_fwd DOUBLE,"
                  " next_split BIGINT, sh_restated DOUBLE, sh_step DOUBLE)")
# A floor's failing side, for check M's events: over the floor on the raw cap, not on cap_used.
CAP_FAIL_TEST = "km.marketCap > ? AND (km.cap_used IS NULL OR km.cap_used <= ?)"


def cap_used_sql(m="m", cc="cc"):
    """The cap a floor reads under check M: NULL for an ineligible FY row, else marketCap / cap_div."""
    return (f"CASE WHEN {cc}.rule = 'ineligible' THEN NULL WHEN {cc}.cap_div IS NOT NULL"
            f" THEN {m}.marketCap / {cc}.cap_div ELSE {m}.marketCap END")


def empty_cap_check(con):
    """A typed, empty cap_check: the floors LEFT JOIN it, so cap_used is marketCap everywhere."""
    con.execute(CAP_CHECK_DDL)


def cap_check_sql(sym_filter_sql, mktcap_min, date_ranges, cfg=DATA_CHECKS_V1):
    """Check M's server query: the inputs for every FY key_metrics row over the leg's floor.

    Per (symbol, FY period end): marketCap, the same FY's net income, EPS and weighted shares, the
    last `bars` stock_eod dates up to the period end, one row per date with the traded row kept
    first (count, traded count, median close x volume of
    the traded ones), stock_eod close over the raw close on the latest day both exist, and over the
    splits_calendar events after the period end: the product of denominator / numerator (pf_splits),
    of the consolidations alone (pf_cons) and of the forward splits of split_min or more (pf_fwd).
    An event whose ratio passes split_max either way (4541.T's 1:70,384,700 is a delisting cash-out,
    and FMP's prices do not carry it) is left out of all three. A record repeating the ratio of a record
    kept within split_dup_days before it is a duplicate and counts once (AMCR's two 2026-01-15 1:5
    rows); records are read in date order against the ones kept, so a chain of repeats cannot drop a
    record further than that from every kept one. next_split is the first later record's date. FMP's
    FY share counts (weightedAverageShsOut) tell how the row's shares sit against each later record:
    the count of the last FY before it against the first FY on or after it (records between the same
    two FYs taken together, ratio q = numerator / denominator). Within share_tol of 1 the count was
    restated for the record (no step); when the log of its change is share_step times the log of q
    (the same direction, at least half as large and at most twice) it steps there (the count is as
    traded); records under share_tol squared either way, or with no FY on one side, tell nothing.
    sh_restated is the product of q over the restated records, sh_step that of 1 / q over the stepping
    ones.
    date_ranges [(first, last)] bound the bar scans to the days before each period end. The server's
    dateEpoch columns are UINTEGER and some split records sit at 0, so epochs are cast to BIGINT first.
    """
    c = cfg["cap"]
    look = int(c["lookback_days"]) * 86400
    smax, smin = math.log(float(c["split_max"])), math.log(float(c["split_min"]))
    dup, lt = int(c["split_dup_days"]) * 86400, math.log(float(c["share_tol"]))
    st_lo, st_hi = (float(v) for v in c["share_step"])
    days = lambda col: " OR ".join(f"({col} >= '{a}' AND {col} <= '{b}')" for a, b in date_ranges) or "FALSE"
    return f"""
        WITH RECURSIVE km AS (
            SELECT symbol, CAST(dateEpoch AS BIGINT) AS fe, marketCap FROM key_metrics
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
            SELECT symbol, fe, e, close, volume, ROW_NUMBER() OVER (PARTITION BY symbol, fe ORDER BY e DESC) AS rn
            FROM (
                SELECT kb.symbol, kb.fe, e.dateEpoch AS e, e.close, e.volume
                FROM stock_eod e JOIN kb ON e.symbol = kb.symbol AND e.dateEpoch // {look} = kb.blk
                WHERE ({days("e.date")}) AND e.dateEpoch <= kb.fe AND e.dateEpoch > kb.fe - {look}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY kb.symbol, kb.fe, e.dateEpoch
                                           ORDER BY e.volume DESC NULLS LAST, e.close DESC NULLS LAST) = 1) one_per_day),
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
        spr AS (
            SELECT DISTINCT symbol, CAST(dateEpoch AS BIGINT) AS se, numerator AS num, denominator AS den
            FROM splits_calendar
            WHERE symbol IN (SELECT symbol FROM km) AND numerator > 0 AND denominator > 0
              AND ABS(LN(denominator / numerator)) <= {smax}),
        spo AS (
            SELECT symbol, se, num, den, DENSE_RANK() OVER (ORDER BY symbol, num / den) AS gid,
                   ROW_NUMBER() OVER (PARTITION BY symbol, num / den ORDER BY se, num) AS rn
            FROM spr),
        spk AS (
            SELECT gid, rn, symbol, se, num, den, se AS kept_se, TRUE AS keep FROM spo WHERE rn = 1
            UNION ALL
            SELECT o.gid, o.rn, o.symbol, o.se, o.num, o.den,
                   CASE WHEN o.se > k.kept_se + {dup} THEN o.se ELSE k.kept_se END, o.se > k.kept_se + {dup}
            FROM spk k JOIN spo o ON o.gid = k.gid AND o.rn = k.rn + 1),
        sp AS (SELECT symbol, se, LN(num / den) AS lq FROM spk WHERE keep),
        spl AS (
            SELECT km.symbol, km.fe, EXP(SUM(-s.lq)) AS pf_splits, EXP(SUM(GREATEST(-s.lq, 0))) AS pf_cons,
                   EXP(SUM(CASE WHEN s.lq >= {smin} THEN s.lq ELSE 0 END)) AS pf_fwd, MIN(s.se) AS next_split
            FROM km JOIN sp s ON s.symbol = km.symbol AND s.se > km.fe
            GROUP BY km.symbol, km.fe),
        sh AS (SELECT symbol, fe AS sfe, weightedAverageShsOut AS s FROM inc WHERE weightedAverageShsOut > 0),
        sb AS (
            SELECT p.symbol, p.se, p.lq, MAX(CASE WHEN h.sfe < p.se THEN h.sfe END) AS bfe,
                   MIN(CASE WHEN h.sfe >= p.se THEN h.sfe END) AS afe
            FROM sp p JOIN sh h ON h.symbol = p.symbol GROUP BY p.symbol, p.se, p.lq),
        iv AS (
            SELECT b.symbol, MIN(b.se) AS first_se, SUM(b.lq) AS lq, LN(ha.s / hb.s) AS lr
            FROM sb b JOIN sh hb ON hb.symbol = b.symbol AND hb.sfe = b.bfe
            JOIN sh ha ON ha.symbol = b.symbol AND ha.sfe = b.afe
            GROUP BY b.symbol, b.bfe, b.afe, ha.s, hb.s),
        shv AS (
            SELECT km.symbol, km.fe,
                   EXP(SUM(CASE WHEN ABS(v.lq) > {2 * lt} AND ABS(v.lr) <= {lt} THEN v.lq ELSE 0 END)) AS sh_restated,
                   EXP(SUM(CASE WHEN ABS(v.lq) > {2 * lt} AND v.lr / v.lq BETWEEN {st_lo} AND {st_hi}
                                THEN -v.lq ELSE 0 END)) AS sh_step
            FROM km JOIN iv v ON v.symbol = km.symbol AND v.first_se > km.fe
            GROUP BY km.symbol, km.fe)
        SELECT km.symbol, km.fe AS filing_epoch, km.marketCap AS market_cap, inc.netIncome AS net_income,
               inc.eps, inc.weightedAverageShsOut AS shares, agg.n_bars, agg.n_traded, agg.turnover,
               pfr.pf_raw, spl.pf_splits, spl.pf_cons, spl.pf_fwd, spl.next_split, shv.sh_restated, shv.sh_step
        FROM km
        LEFT JOIN inc ON inc.symbol = km.symbol AND inc.fe = km.fe
        LEFT JOIN agg ON agg.symbol = km.symbol AND agg.fe = km.fe
        LEFT JOIN pfr ON pfr.symbol = km.symbol AND pfr.fe = km.fe
        LEFT JOIN spl ON spl.symbol = km.symbol AND spl.fe = km.fe
        LEFT JOIN shv ON shv.symbol = km.symbol AND shv.fe = km.fe
        ORDER BY km.symbol, km.fe"""


def apply_cap_rule(con, cfg=DATA_CHECKS_V1, inputs="cap_inputs"):
    """Check M's rule, from cap_inputs into cap_check (one row per symbol and FY period end).

    Inputs: pf_raw = close / raw close at the period end; pf_splits, pf_cons and pf_fwd = the later
    split records', consolidations' and forward splits' products; sh_restated, sh_step and next_split
    from FMP's FY share counts and the records (cap_check_sql);
    k = shares x EPS / net income, when |EPS| >= eps_min, |net income| >= net_income_min and the
    signs agree; u = the turnover of every line that carries the same FY cap as this row and shares
    an identical FY cap with it in at least sibling.min_shared_fy FYs (share classes of one company,
    so a traded sibling clears a thin class; one shared FY can be a coincidence between unrelated
    companies): the sum of each line's median close x volume of its traded bars, over marketCap,
    unknown when the lines hold under min_traded_bars traded bars between them. A line with no bars
    in the lookback (not listed yet, or gone) adds nothing to u, and the row is then not strict.

    D, the divisor, needs evidence beyond the split records that FMP's cap sits on as-traded shares
    times a restated price. P, the measured price factor, is pf_raw, or for a row with bars but no raw
    close the pf_raw of the same line's nearest FY row that has one with no split record between the
    two period ends (pf_source 'raw_fy'; TOPS FY2004 from FY2006). Then
      P >= pf_min       price evidence: D = P x pf_fwd (the share count carries the forward splits).
      P under pf_min    D = P x sh_restated: only records FMP's FY share count was seen restated for
                        move it, so pf_fwd never multiplies a price factor near 1 (0568.HK FY2006:
                        5:1, 1:10 and 2:1 restated, D 1; Bonjour FY2008: the 2010 forward splits
                        restated, D = 1.375 x 12).
      no P              share evidence: D = sh_step, the records at which the FY share count steps
                        (a line with no bars, or no raw close: the split products alone never lower).
    D = max(1, D). pf (gate, k agreement, the events) is P, else pf_splits.

    The row is checked when pf >= pf_min or pf_cons >= pf_min (forward and reverse splits can net
    out in pf). It is strict when pf >= pf_min and every line of its group has bars. Then, with
    K = k when k > k_min:
      K within k_tol of pf  the cap lowers to cap / K first, and u is read on that cap: it clears ->
                            'cap_over_k'; it fails -> as below, with divisor max(K, D).
      K disagreeing with pf turnover then k, as before: u clears -> 'cap_over_k' (cap / K); u fails ->
                            'ineligible' when strict (EPS, shares and prices sit on different bases,
                            HAO), else lowered as below, by max(K, D).
      no K                  u clears -> 'stands'.
    A failing u lowers the cap to cap / divisor ('cap_over_pf') when u on the lowered cap clears
    turnover_max or the row is not strict, else the row is 'ineligible' (cap_used NULL). An unknown
    u lowers it the same way. So a gate opened by the consolidations alone (pf under pf_min), or a
    group whose liquid line has no bars yet (ERIC-B.ST before 2000), lowers the cap and never
    excludes the row. Where that divisor is 1 (no evidence and no K: HAO FY2025, pf 1.0,
    consolidations 2560; a line with no bars whose share count never steps), or within 1e-9 of it
    (0760.HK FY2006: 2 x 0.5000000000000002), nothing can lower the cap: the row is 'unchecked', keeps
    FMP's cap and is counted in cap_rows_flagged. It never raises a cap. pf, the split products and
    the later FY share counts come from after the period end but only gate and size the lowering (they
    say how FMP restated the row): the decision reads k (same filing) and u (trading before the period
    end).
    Returns {cap_rows_checked, cap_rows_flagged, cap_unchecked}: every row, those whose rule is not
    'stands', and the 'unchecked' rows ({symbol, fy, cap, pf, pf_source, pf_cons, pf_fwd, sh_restated,
    sh_step, u, lines, k}).
    """
    c = cfg["cap"]
    pf_min, u_max, k_min = float(c["pf_min"]), float(c["turnover_max"]), float(c["k_min"])
    min_fy = int(cfg["sibling"]["min_shared_fy"])
    con.execute(CAP_CHECK_DDL)
    con.execute(f"""
        INSERT INTO cap_check
        WITH x0 AS (
            SELECT symbol, CAST(filing_epoch AS BIGINT) AS filing_epoch, CAST(market_cap AS DOUBLE) AS market_cap,
                   CAST(pf_raw AS DOUBLE) AS pf_raw, CAST(pf_splits AS DOUBLE) AS pf_splits,
                   CAST(pf_cons AS DOUBLE) AS pf_cons, CAST(pf_fwd AS DOUBLE) AS pf_fwd,
                   CAST(next_split AS BIGINT) AS next_split, CAST(sh_restated AS DOUBLE) AS sh_restated,
                   CAST(sh_step AS DOUBLE) AS sh_step,
                   CAST(n_bars AS BIGINT) AS n_bars, CAST(n_traded AS BIGINT) AS n_traded,
                   CAST(turnover AS DOUBLE) AS turnover,
                   CASE WHEN ABS(eps) >= {float(c["eps_min"])} AND ABS(net_income) >= {float(c["net_income_min"])}
                             AND SIGN(eps) = SIGN(net_income) AND shares > 0
                        THEN CAST(shares AS DOUBLE) * eps / net_income END AS k
            FROM {inputs}),
        bw AS (
            SELECT a.symbol, a.filing_epoch,
                   arg_min(b.pf_raw, ABS(b.filing_epoch - a.filing_epoch) * 2
                                     + CASE WHEN b.filing_epoch > a.filing_epoch THEN 0 ELSE 1 END) AS pf_line
            FROM x0 a JOIN x0 b ON b.symbol = a.symbol AND b.pf_raw IS NOT NULL
             AND ((b.filing_epoch > a.filing_epoch AND COALESCE(a.next_split > b.filing_epoch, TRUE))
               OR (b.filing_epoch < a.filing_epoch AND COALESCE(b.next_split > a.filing_epoch, TRUE)))
            WHERE a.pf_raw IS NULL AND COALESCE(a.n_bars, 0) > 0
            GROUP BY a.symbol, a.filing_epoch),
        x AS (
            SELECT x0.*, COALESCE(x0.pf_raw, bw.pf_line) AS pp, COALESCE(x0.pf_raw, bw.pf_line, x0.pf_splits) AS pf,
                   CASE WHEN x0.pf_raw IS NOT NULL THEN 'raw' WHEN bw.pf_line IS NOT NULL THEN 'raw_fy'
                        WHEN x0.pf_splits IS NOT NULL THEN 'splits' END AS pf_source
            FROM x0 LEFT JOIN bw ON bw.symbol = x0.symbol AND bw.filing_epoch = x0.filing_epoch),
        cls AS (
            SELECT a.symbol AS a, b.symbol AS b FROM x a JOIN x b
              ON a.filing_epoch = b.filing_epoch AND a.market_cap = b.market_cap AND a.symbol <> b.symbol
            GROUP BY a.symbol, b.symbol HAVING COUNT(DISTINCT a.filing_epoch) >= {min_fy}),
        mem AS (
            SELECT symbol AS a, filing_epoch, n_bars, n_traded, turnover FROM x
            UNION ALL
            SELECT s.a, m.filing_epoch, m.n_bars, m.n_traded, m.turnover
            FROM cls s JOIN x o ON o.symbol = s.a
            JOIN x m ON m.symbol = s.b AND m.filing_epoch = o.filing_epoch AND m.market_cap = o.market_cap),
        g AS (
            SELECT a AS symbol, filing_epoch, COUNT(*) AS lines, SUM(COALESCE(n_traded, 0)) AS g_traded,
                   SUM(CASE WHEN n_traded > 0 THEN turnover END) AS g_turnover,
                   BOOL_OR(COALESCE(n_bars, 0) = 0) AS missing
            FROM mem GROUP BY a, filing_epoch),
        y AS (
            SELECT x.*, g.lines, g.g_traded,
                   CASE WHEN g.g_traded >= {int(c["min_traded_bars"])} AND x.market_cap > 0
                        THEN g.g_turnover / x.market_cap END AS u,
                   COALESCE(x.pf >= {pf_min} OR x.pf_cons >= {pf_min}, FALSE) AS gate,
                   COALESCE(x.pf >= {pf_min}, FALSE) AND NOT g.missing AS strict,
                   GREATEST(1.0, CASE WHEN x.pp >= {pf_min} THEN x.pp * COALESCE(x.pf_fwd, 1.0)
                                      WHEN x.pp IS NOT NULL THEN x.pp * COALESCE(x.sh_restated, 1.0)
                                      ELSE COALESCE(x.sh_step, 1.0) END) AS d,
                   CASE WHEN x.k > {k_min} THEN x.k END AS kk,
                   COALESCE(x.k > {k_min} AND x.pf > 0 AND ABS(LN(x.k / x.pf)) <= LN({float(c["k_tol"])}), FALSE) AS agree
            FROM x JOIN g ON g.symbol = x.symbol AND g.filing_epoch = x.filing_epoch),
        z AS (
            SELECT *, GREATEST(d, COALESCE(kk, 1.0)) AS dk,
                   CASE WHEN NOT gate THEN 'stands'
                        WHEN agree AND u * kk >= {u_max} THEN 'cap_over_k'
                        WHEN kk IS NOT NULL AND NOT agree AND u >= {u_max} THEN 'cap_over_k'
                        WHEN kk IS NULL AND u >= {u_max} THEN 'stands'
                        WHEN kk IS NOT NULL AND NOT agree AND u < {u_max} AND strict THEN 'ineligible'
                        WHEN u IS NULL THEN 'lower'
                        WHEN u * GREATEST(d, COALESCE(kk, 1.0)) >= {u_max} THEN 'lower'
                        WHEN NOT strict THEN 'lower'
                        ELSE 'ineligible' END AS r
            FROM y)
        SELECT symbol, filing_epoch, market_cap, pf, pf_source, pf_cons, pf_fwd, u, g_traded, lines, k,
               CASE WHEN r = 'lower' AND dk > 1 + 1e-9 THEN 'cap_over_pf' WHEN r = 'lower' THEN 'unchecked' ELSE r END,
               CASE WHEN r = 'cap_over_k' THEN kk WHEN r = 'lower' AND dk > 1 + 1e-9 THEN dk END, sh_restated, sh_step
        FROM z""")
    dup = con.execute("SELECT COUNT(*) FROM (SELECT symbol, filing_epoch FROM cap_check"
                      " GROUP BY symbol, filing_epoch HAVING COUNT(*) > 1)").fetchone()[0]
    if dup:
        raise AssertionError(f"cap_check has {dup} duplicate (symbol, filing_epoch) keys: the floors would fan out")
    n, flagged = con.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE rule <> 'stands') FROM cap_check").fetchone()
    # an unchecked row keeps FMP's cap, so it never fails a floor and leaves no cap event: listed here
    unchecked = [{"symbol": s, "fy": utc_date(fe).isoformat(), "cap": cap, "pf": _sig(pf), "pf_source": src,
                  "pf_cons": _sig(pfc), "pf_fwd": _sig(pff), "sh_restated": _sig(shr), "sh_step": _sig(shs),
                  "u": _sig(u), "lines": lines, "k": _sig(k)}
                 for s, fe, cap, pf, src, pfc, pff, shr, shs, u, lines, k in con.execute(
                     "SELECT symbol, filing_epoch, market_cap, pf, pf_source, pf_cons, pf_fwd, sh_restated, sh_step,"
                     " u, lines, k FROM cap_check WHERE rule = 'unchecked' ORDER BY symbol, filing_epoch").fetchall()]
    return {"cap_rows_checked": int(n), "cap_rows_flagged": int(flagged), "cap_unchecked": unchecked}


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
    """One leg's data checks (dc-1, dc-2 or dc-3 by cfg): collects what T, N, M, F and L did, for the results file.

    mode "on" runs every check. mode "uncapped" runs none: it exists for the same-day attribution
    arm, issues no query and leaves every count at 0. Both modes book with max_single_return=None.
    A topic's run_backtest builds an "uncapped" instance when it is called without one.
    Window bars only: nothing here reads an LTP fill, so a drop run and a both run write the same
    block (annotate_ltp writes F and L for the fills into the ltp block, which strip_ltp removes).
    full_holdings=True (opt-in, either mode): holdings_full() gives each invested period row every
    booked name with its bars, prices and return (the holdings string may list only the first names).
    """

    MODES = ("on", "uncapped")
    PRICE_KEYS = ("entry_bars_moved", "exit_bars_moved", "entries_lost", "exits_lost")
    LOST = {"entry": "entries_lost", "exit": "exits_lost"}

    def __init__(self, mode="on", cfg=DATA_CHECKS_V1, full_holdings=False):
        if mode not in self.MODES:
            raise ValueError(f"data checks mode {mode!r} not in {self.MODES}")
        self.mode, self.cfg, self.on, self.full_holdings = mode, cfg, mode == "on", bool(full_holdings)
        self.clean = {"rows_moved": {"closed_day": 0, "zero_volume": 0, "nonpositive_price": 0,
                                     **({"stale_repeat": 0} if "stale_repeat" in cfg else {})},
                      "symbol_windows_without_volume": 0, "flat_windows_moved": 0, "closed_day_test_skipped": 0,
                      "closed_dates": [], "sibling_gap_rows": 0, "sibling_gaps": []}
        self.cap = {"cap_rows_checked": 0, "cap_rows_flagged": 0, "cap_unchecked": []}
        hand = cfg.get("factor_hand")
        self._hand = {(e["symbol"], e["rebalance_date"], e["exit_date"]): e for e in hand["entries"]} if hand else None
        self._hand_pending, self._hand_booked = {}, {}
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
                SELECT DISTINCT c.symbol, c.km_epoch, c.raw_cap, cc.pf, cc.pf_source, cc.pf_cons, cc.pf_fwd,
                       cc.sh_restated, cc.sh_step, cc.u, cc.lines, cc.k, cc.rule, cc.cap_div
                FROM ({sql}) c LEFT JOIN cap_check cc ON cc.symbol = c.symbol AND cc.filing_epoch = c.km_epoch
                ORDER BY c.symbol, c.km_epoch""", list(params)).fetchall()
            for sym, fe, cap, pf, src, pfc, pff, shr, shs, u, lines, k, rule, div in rows:
                n += 1
                used = None if rule == "ineligible" else (cap / div if div else cap)
                self._event(target_date, sym, check, fy=utc_date(fe).isoformat(), cap=cap, k=_sig(k), pf=_sig(pf),
                            pf_source=src, pf_cons=_sig(pfc), pf_fwd=_sig(pff), sh_restated=_sig(shr),
                            sh_step=_sig(shs), u=_sig(u), lines=lines, rule=rule, cap_div=_sig(div),
                            cap_used=_sig(used, 10), floor=floor)
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

    def hand(self, clean, entry_date, exit_date, filled=(), record=True):
        """Check F's hand list (dc-3) on filter_returns' (symbol, raw return, mcap) list, before costs: a listed
        holding of this span books its entry's return when the entry is "rebuilt", and every match logs a
        `hand` event (the booked return, the adjClose return beside it); a match whose adjClose return is more
        than log_tolerance from the listed one logs `hand_stale` and books as given. Names in `filled` (an LTP
        fill's exit) are left alone. The events wait for prices(): a held period commits them, a cash one drops
        them; record=False (the second book of --exit-method both) logs nothing. Without the list, or in the
        uncapped mode, returns `clean` itself."""
        if self._hand is None or not self.on:
            return clean
        tol = self.cfg["factor_hand"]["log_tolerance"]
        span = (entry_date.isoformat(), exit_date.isoformat())
        out, pend = [], []
        for s, ret, mc in clean:
            e = self._hand.get((s, *span)) if s not in filled else None
            if e is None:
                out.append((s, ret, mc))
                continue
            near = ret > -1 and e["return_adj"] > -1 and abs(math.log1p(ret) - math.log1p(e["return_adj"])) <= tol
            booked = e["booked"] if near and e["kind"] == "rebuilt" else ret
            pend.append((s, "hand" if near else "hand_stale",
                         {"kind": e["kind"], "booked": _sig(booked), "return_adj": _sig(ret),
                          "listed_return_adj": e["return_adj"], "range": e["range"]}, ret))
            out.append((s, booked, mc))
        if record:
            self._hand_pending[span[0]] = pend
        return out

    def _commit_hand(self, entry_date, held):
        """prices()'s first step: the period's waiting hand events go to the events when it is held."""
        pend = self._hand_pending.pop(entry_date.isoformat(), [])
        for s, check, fields, ret in pend if held else []:
            self._event(entry_date, s, check, **fields)
            if check == "hand" and fields["kind"] == "rebuilt":
                self._hand_booked[(entry_date.isoformat(), s)] = round(ret, 6)   # holdings_full's rounding

    def prices(self, con, entry_date, exit_date, symbol_data, offset_days, min_entry_price, held=True,
               exit_offset_days=None):
        """Checks on one priced period's window bars: T's bar events, then for a held book L and F.
        exit_offset_days: the exit's offset when it differs (RunWindow.exit_offset); None uses offset_days.

        symbol_data is the topic's (symbol, entry, exit, mcap) list from get_prices. The bars are read
        again with their dates and closes, and must carry the same adjClose (an assert). F only
        records: a holding is flagged when its stage-1 drift (adjClose / close at the exit bar over the
        same at the entry bar) leaves the band, a split of ratio split_min or more falls in (entry bar,
        exit bar], or it is on L."""
        self._commit_hand(entry_date, held)
        p = self._period(entry_date)
        p.update(entry_bars_moved=0, exit_bars_moved=0, entries_lost=0, exits_lost=0, factor_flags=0)
        if not self.on:
            return
        syms = [s for s, *_ in symbol_data]
        x_off = offset_days if exit_offset_days is None else exit_offset_days
        bars = {"entry": self._bars(con, syms, entry_date, offset_days),
                "exit": self._bars(con, syms, exit_date, x_off)}
        for s, ep, xp, _ in symbol_data:
            for side, px in (("entry", ep), ("exit", xp)):
                got = bars[side].get(s, {}).get("adjClose")
                if got != px:
                    raise AssertionError(f"data checks: {side} bar of {s} on {entry_date} reads {got}, get_prices {px}")
        out = lambda b: None if b is None else {k: b[k] for k in ("date", "adjClose", "close", "volume")}
        for side, d, off in (("entry", entry_date, offset_days), ("exit", exit_date, x_off)):
            for s, row in sorted(self._first_moved(con, syms, d, off).items()):
                used = bars[side].get(s)
                if used is None:   # T moved every row of the window
                    p[self.LOST[side]] += 1
                    self._event(entry_date, s, f"{side}_lost", side=side, first_bar=row)
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
        """F and L on each LTP fill (traded_only), written into the ltp block: the drift from the entry
        bar to the fill's adjClose, the splits between, and the booked fill's return against
        large_move_log (listed in the block's large_moves). Changes no fill and adds nothing to the
        window-bar block."""
        if not (self.on and getattr(ltp, "traded_only", False)):
            return
        lo, hi = self.cfg["factor"]["band"]
        n, moves = 0, []
        for f in ltp.fills:
            e0 = utc_epoch(date.fromisoformat(f["entry_bar"]))
            e = con.execute("SELECT adjClose, close FROM prices_cache WHERE symbol = ? AND trade_epoch = ?"
                            " ORDER BY adjClose DESC NULLS LAST, close DESC NULLS LAST LIMIT 1",
                            [f["symbol"], e0]).fetchone()
            drift = None   # on the fill's own adjClose: a close-basis fill keeps the flag that repaired it
            if e and e[0] and e[1] and e[1] > 0 and f.get("ltp_close") and f["ltp_close"] > 0:
                drift = (f.get("ltp_adj", f["ltp"]) / f["ltp_close"]) / (e[0] / e[1])
            sp = self._splits(con, {f["symbol"]: (e0, utc_epoch(date.fromisoformat(f["last_bar"])))}).get(f["symbol"], [])
            reasons, ep, px = [], f.get("entry_price"), f["ltp"]
            if ep and ep > 0 and px and px > 0 and abs(math.log(px / ep)) >= self.cfg["large_move_log"]:
                reasons.append("large_move")
                moves.append({"rebalance_date": f["entry_date"], "symbol": f["symbol"], "return": _sig(px / ep - 1),
                              "entry_bar": f["entry_bar"], "entry_price": ep, "last_bar": f["last_bar"], "ltp": px})
            if drift is not None and not lo <= drift <= hi:
                reasons.append("band")
            if sp:
                reasons.append("split")
            f["factor_check"] = {"drift": _sig(drift), "splits": sp, "reasons": sorted(reasons),
                                 "flagged": bool(reasons)}
            n += bool(reasons)
        block["factor_flagged"] = n
        block["large_move_fills"] = len(moves)
        block["large_moves"] = sorted(moves, key=lambda m: (m["rebalance_date"], m["symbol"]))

    def holdings_full(self, con, clean, entry_prices, exit_prices, filled, entry_date, exit_date, offset_days,
                      ltp=None, exit_offset_days=None):
        """{} unless full_holdings, else {"holdings_full": [...]}: one item per booked name of `clean`
        (filter_returns' (symbol, return, mcap), in its order) with the entry bar and price, the exit
        bar and price ("window": the exit window's bar; "last_traded": an LTP fill, with its last bar)
        and the booked return before costs. `exit_prices` is the map the book read, `filled` the
        names whose exit came from an LTP fill (ltp holds the fills). exit_offset_days as in prices()."""
        if not self.full_holdings:
            return {}
        syms = [s for s, _, _ in clean]
        ebar = self._bars(con, syms, entry_date, offset_days)
        xbar = self._bars(con, [s for s in syms if s not in filled], exit_date,
                          offset_days if exit_offset_days is None else exit_offset_days)
        fills = {f["symbol"]: f for f in (ltp.fills if ltp is not None else [])
                 if f["entry_date"] == entry_date.isoformat()}
        out = []
        for s, ret, _ in clean:
            e, ep, xp = ebar.get(s), entry_prices.get(s), exit_prices.get(s)
            if s in filled:
                bar, src = fills[s]["last_bar"], "last_traded"
            else:
                bar, src = xbar[s]["date"], "window"
                if xbar[s]["adjClose"] != xp:
                    raise AssertionError(f"holdings_full: exit bar of {s} on {entry_date} reads {xbar[s]['adjClose']}, book {xp}")
            if e is None or e["adjClose"] != ep:
                raise AssertionError(f"holdings_full: entry bar of {s} on {entry_date} reads {e}, book {ep}")
            out.append({"symbol": s, "entry_bar": e["date"], "entry_price": ep, "exit_bar": bar, "exit_price": xp,
                        "exit": src, "return": round(ret, 6)})
            adj = self._hand_booked.get((entry_date.isoformat(), s))
            if adj is not None:   # a dc-3 rebuilt holding: the booked return, its adjClose return beside it
                out[-1]["return_adj"] = adj
        return {"holdings_full": out}

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
            "flat_windows_moved": self.clean.get("flat_windows_moved", 0),
            "sibling_gap_rows": self.clean.get("sibling_gap_rows", 0),
            "closed_day_test_skipped": self.clean["closed_day_test_skipped"],
            "entry_bars_moved": n("entry_bar"), "exit_bars_moved": n("exit_bar"), "entries_lost": n("entry_lost"),
            "exits_lost": n("exit_lost"),
            "twins_linked": self.twins_linked, "twins_collapsed": n("twin"),
            "nvdr_stand_ins": sum(1 for e in ev if e["check"] == "twin" and e["reason"] == "stand_in"),
            "cap_rows_checked": self.cap["cap_rows_checked"], "cap_rows_flagged": self.cap["cap_rows_flagged"],
            "cap_excluded": n("cap"), "signal_cap_excluded": n("signal_cap"),
            "factor_checked": self.factor_checked, "factor_flags": n("factor_flag"),
            "large_moves": len(self.large_moves),
        }
        if self._hand is not None:   # dc-3 only, so a dc-1 or dc-2 block stays as it was
            kinds = Counter(e["kind"] for e in ev if e["check"] == "hand")
            counts.update({f"hand_{k}": kinds.get(k, 0) for k in ("rebuilt", "real", "kept")},
                          hand_stale=n("hand_stale"))
        return {"version": self.cfg["version"], "mode": self.mode, "max_single_return": None,
                "rules": {k: v for k, v in self.cfg.items() if k != "version"}, "counts": counts,
                "closed_dates": list(self.clean["closed_dates"]), "events": ev,
                "large_moves": sorted(self.large_moves, key=lambda m: (m["rebalance_date"], m["symbol"])),
                "sibling_gaps": list(self.clean.get("sibling_gaps", [])),
                "cap_unchecked": list(self.cap.get("cap_unchecked", []))}

    def summary(self):
        c = self.block()["counts"]
        return (f"Data checks ({self.mode}): rows moved {c['rows_moved']} (flat windows {c['flat_windows_moved']}, "
                f"sibling-gap rows {c['sibling_gap_rows']}), entry/exit bars moved "
                f"{c['entry_bars_moved']}/{c['exit_bars_moved']}, entries/exits lost {c['entries_lost']}/"
                f"{c['exits_lost']}, twins "
                f"{c['twins_collapsed']} ({c['nvdr_stand_ins']} stand-ins), cap rows flagged {c['cap_rows_flagged']}, "
                f"cap excluded {c['cap_excluded']} + signal {c['signal_cap_excluded']}, factor flags "
                f"{c['factor_flags']}, large moves {c['large_moves']}"
                + (f", hand list rebuilt {c['hand_rebuilt']} real {c['hand_real']} kept {c['hand_kept']} "
                   f"stale {c['hand_stale']}" if "hand_stale" in c else ""))


# ---------------------------------------------------------------------------
# Universe rules ur-1: ordinary shares only, and the dividend-yield rule (METHODOLOGY.md, Universe Rules).
# Opt-in per topic like the data checks; defensive-quality uses them.

US_EXCHANGES = ("NYSE", "NASDAQ", "AMEX")
# Plain-named lines no pattern separates, each checked against its closes and its issuer's common:
# (symbol, kind, evidence).
NON_ORDINARY_KNOWN = (
    ("AESC", "units", "AES corporate units 2021-2024, $100 stated amount, AES is the common"),
    ("AQNU", "units", "Algonquin equity units 2021-2024, $50, AQN is the common"),
    ("BKDT", "units", "Brookdale tangible equity units 2022-2025, $100, BKD is the common"),
    ("DCUE", "units", "Dominion Energy corporate units 2019-2022, $100, D is the common"),
    ("DTP", "units", "DTE Energy corporate units 2019-2022, $50, DTE is the common"),
    ("NIMC", "units", "NiSource equity units 2021-2023, $100, NI is the common"),
    ("PCGU", "units", "PG&E equity units 2020-2023, $100, PCG is the common"),
    ("PPLC", "units", "a second PPL line whose closes (26 to 49) don't track PPL's, on about 1/1000 of its volume"),
    ("SJIV", "units", "South Jersey Industries equity units 2021-2023, $50, SJI is the common"),
    ("SOMN", "units", "Southern Company equity units from 2025-11, $50, SO is the common"),
    ("TSNU", "units", "Tyson Foods tangible equity units 2014-2017, $50, TSN is the common"),
    ("UGIC", "units", "UGI equity units 2021-2024, $100, UGI is the common"),
    ("WTRU", "units", "Aqua America tangible equity units 2019-2022, $50, WTRG is the common"),
    ("AQNA", "notes", "Algonquin subordinated notes, $25 par (closes 23 to 28)"),
    ("AQNB", "notes", "Algonquin subordinated notes, $25 par (closes 23 to 28)"),
    ("BEPI", "notes", "Brookfield BRP Holdings perpetual subordinated notes, $25 par (closes 15 to 22)"),
    ("SJIJ", "notes", "South Jersey Industries junior subordinated notes, $25 par (closes 17 to 27)"),
    ("SREA", "notes", "Sempra junior subordinated notes, $25 par (closes 21 to 28)"),
    ("ENJ", "bonds", "Entergy New Orleans bonds, $25 par, the issuer has no listed common"),
    ("ENO", "bonds", "Entergy New Orleans bonds, $25 par, the issuer has no listed common"),
    ("EZT", "bonds", "Entergy Texas bonds, $25 par, the issuer has no listed common"),
    ("TVC", "bonds", "Tennessee Valley Authority bonds, $25 par, a federal corporation with no shares"),
    ("TVE", "bonds", "Tennessee Valley Authority bonds, $25 par, a federal corporation with no shares"),
    ("BDXB", "preferred", "Becton Dickinson mandatory convertible preferred depositary shares 2020-2023, $50"),
    ("ETI", "preferred", "Entergy Texas preferred, ETI-P's ISIN, no bars, the issuer has no listed common"),
    ("CHSCL", "preferred", "CHS Inc. preferred, $25 par, a cooperative with no listed common"),
    ("CIG", "preferred ADR", "CEMIG preferred-share ADR, CIG-C is the common ADR"),
    ("ELP", "preferred ADR", "COPEL class B preferred ADR, ELPC is the common ADR"),
    ("EBR-B", "preferred ADR", "Eletrobras class B preferred ADR, EBR is the common ADR"),
    ("ROG.SW", "certificate", "Roche non-voting Genussschein at its own price on Roche's financials, RO.SW is the share"),
    ("ROP.SW", "certificate", "Roche's second line from 2026-04-22 (ROG.SW's bars end 2026-04-13), at its own price, RO.SW is the share"),
    ("LISP.SW", "certificate", "Lindt participation certificate (Partizipationsschein), LISN.SW is the share"),
    ("RE-B.L", "preferred", "R.E.A. Holdings cumulative preference shares, closes at a median of 100p (their par), RE.L is the common"),
)
NAME_TERMS = (r"%|\bnotes?\b|debenture|\bbonds?\b|\bbd\b|subordinated|\bjr\b|junior|collateral|preferred|"
              r"\bpfd\b|\bpref\b|\bunits?\b|\bser\b|\bseries\b")
ORDINARY_RULES = {
    "rule": ("a line is removed when it is a different instrument from the company's ordinary share (preferred, "
             "note, bond, unit, warrant, right, fund) or a second line at its own price on the same financials "
             "that no security key collapses; the first reason in `order` is recorded"),
    "order": {
        "fund_flag": "profile isFund or isEtf",
        "known": "NON_ORDINARY_KNOWN, the hand list below",
        "korean_preferred": "KSC six-character code whose sixth character is not 0",
        "german_preference": ("XETRA ticker ending in 3, beside another XETRA line whose ISIN shares its first "
                              "9 characters (PNE3.DE, PNE's only line, stays)"),
        "canadian_preferred": "TSX ticker ending -P plus letters",
        "us_suffix": "NYSE, NASDAQ or AMEX ticker ending -P plus letters, -WT, -WTA, -WS, -W, -U, -UN, -R or -RI",
        "nasdaq_fifth_letter": "five-letter NASDAQ ticker whose fifth letter is P, O, N, M (preferred), W, R, U, Z or V",
        "name_term": f"companyName matches, ignoring case: {NAME_TERMS}",
    },
    "kept": ("Thai NVDRs (check N keys X-R.BK to X.BK, DECISIONS #064), ADRs and TDRs of ordinary shares, share "
             "classes (BF-B, MKC-V, CENTA), Canadian trust units"),
    "known": [{"symbol": s, "kind": k, "evidence": e} for s, k, e in NON_ORDINARY_KNOWN],
}
YIELD_CEILING = 0.25
YIELD_AGREE = (0.8, 1.25)
# Sub-unit quotes read as their currency (LSE pence lines report in pounds).
SUBUNIT_CURRENCY = {QUOTE_CURRENCY[e]: LOCAL_CURRENCY[e] for e in QUOTE_CURRENCY}
YIELD_RULES = {
    "ceiling": YIELD_CEILING, "agree": list(YIELD_AGREE), "subunits": dict(SUBUNIT_CURRENCY),
    "rule": ("on the screen's FY financial_ratios row: no vendor dividendYield, kept; above the ceiling, dropped "
             "(above_ceiling); trading currency (profile) and reporting currency (the same FY cash_flow_statement "
             "row, else the ratios row) both known and different: dropped when that FY has no dividends paid "
             "(commonDividendsPaid, else netDividendsPaid) or no positive key_metrics marketCap (unverifiable), "
             "or when the vendor yield over |paid| / marketCap falls outside `agree` (cross_currency); applied "
             "before the security dedup and the top-N cut"),
}


def _sql_list(values):
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def non_ordinary_sql(p="p", profile="profile", known=None, cfg=None):
    """A CASE over a profile row aliased `p`: the first reason in cfg's rules (UNIVERSE_RULES_V1 when None,
    or UNIVERSE_RULES_V2 or V3) the line is not a company's ordinary share, else NULL. `profile` is the table the
    German and Brazil sibling tests read; `known` replaces cfg's hand list (tests). Paste as `AND (<this>) IS NULL` to
    keep ordinary lines only (the share-link SQL)."""
    cfg = UNIVERSE_RULES_V1 if cfg is None else cfg
    known = cfg["known"] if known is None else known
    whens = [f"WHEN {p}.symbol IN ({_sql_list(s for s, _ in cfg['keep'])}) THEN NULL"] if cfg["keep"] else []
    whens.append(f"WHEN COALESCE({p}.isFund, FALSE) OR COALESCE({p}.isEtf, FALSE) THEN 'fund_flag'")
    if known:
        whens.append(f"WHEN {p}.symbol IN ({_sql_list(s for s, _, _ in known)}) THEN 'known'")
    whens += [
        f"WHEN {p}.exchange = 'KSC' AND regexp_full_match({p}.symbol, '[0-9A-Z]{{6}}[.]KS')"
        f" AND SUBSTR({p}.symbol, 6, 1) <> '0' THEN 'korean_preferred'",
        f"WHEN {p}.exchange = 'XETRA' AND regexp_full_match({p}.symbol, '[A-Z0-9]+3[.]DE')"
        f" AND EXISTS (SELECT 1 FROM {profile} _o WHERE _o.exchange = 'XETRA' AND _o.symbol <> {p}.symbol"
        f" AND _o.isin <> {p}.isin AND SUBSTR(_o.isin, 1, 9) = SUBSTR({p}.isin, 1, 9)) THEN 'german_preference'",
        f"WHEN {p}.exchange = 'TSX' AND regexp_matches({p}.symbol, '-P[A-Z]*[.]TO$') THEN 'canadian_preferred'",
        f"WHEN {p}.exchange IN ({_sql_list(US_EXCHANGES)})"
        f" AND regexp_matches({p}.symbol, '-(P[A-Z]*|WTA?|WS|W|U|UN|R|RI)$') THEN 'us_suffix'",
        f"WHEN {p}.exchange = 'NASDAQ' AND regexp_full_match({p}.symbol, '[A-Z]{{4}}[PONMWRUZV]')"
        f" THEN 'nasdaq_fifth_letter'",
    ]
    if cfg["v2"]:
        us = _sql_list(US_EXCHANGES)
        whens += [
            f"WHEN {p}.exchange = 'SAO' AND NOT {_sao_ordinary_sql(p)}"
            f" AND (COALESCE(SUBSTR(NULLIF(UPPER(TRIM({p}.isin)), ''), 9, 2) IN ('R0', 'F0'), FALSE)"
            f" OR NOT EXISTS (SELECT 1 FROM {profile} _b WHERE _b.exchange = 'SAO'"
            f" AND {_sao_issuer_sql('_b')} = {_sao_issuer_sql(p)} AND {_sao_ordinary_sql('_b')}))"
            f" THEN 'brazil_class'",
            f"WHEN {p}.exchange IN ({us}) AND COALESCE({p}.isAdr, FALSE) THEN 'us_adr'",
            f"WHEN {p}.exchange IN ({us}) AND regexp_matches(COALESCE({p}.companyName, ''),"
            f" '{US_TRUST_LP_TERMS}', 'i') THEN 'us_trust_lp'",
        ]
    whens.append(f"WHEN regexp_matches(COALESCE({p}.companyName, ''), '{cfg['name_terms']}', 'i') THEN 'name_term'")
    return "CASE " + " ".join(whens) + " END"


def _ccy_sql(x):
    subs = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in SUBUNIT_CURRENCY.items())
    return f"(CASE {x} {subs} ELSE UPPER({x}) END)"


def yield_rule_sql(vendor, paid, cap, trade_ccy, report_ccy):
    """A CASE over SQL expressions for one screened FY row: the YIELD_RULES reason it drops, else NULL.
    paid is the FY's dividends paid (any sign), cap its key_metrics marketCap."""
    t, r = _ccy_sql(trade_ccy), _ccy_sql(report_ccy)
    lo, hi = YIELD_AGREE
    ratio = f"{vendor} / (ABS({paid}) / {cap})"
    return (f"CASE WHEN {vendor} IS NULL THEN NULL"
            f" WHEN {vendor} > {YIELD_CEILING} THEN 'above_ceiling'"
            f" WHEN {t} IS NULL OR {r} IS NULL OR {t} = {r} THEN NULL"
            f" WHEN {paid} IS NULL OR {paid} = 0 OR {cap} IS NULL OR {cap} <= 0 THEN 'unverifiable'"
            f" WHEN {ratio} NOT BETWEEN {lo} AND {hi} THEN 'cross_currency' END")


def paid_yield(paid, cap):
    """|paid| / cap, or None (yield_rule_sql's paid yield, for the drop records)."""
    return abs(paid) / cap if paid and cap and cap > 0 else None


def cik_shared(rows):
    """Report only: kept US lines (non_ordinary None) that share a CIK, as [{"cik", "symbols"}]. A rerun
    reads them for plain-named notes or units the rule missed (share classes like BF-A/BF-B recur)."""
    groups = {}
    for r in rows:
        if r.get("non_ordinary") is None and r.get("exchange") in US_EXCHANGES and r.get("cik"):
            groups.setdefault(r["cik"], set()).add(r["symbol"])
    return [{"cik": c, "symbols": sorted(s)} for c, s in sorted(groups.items()) if len(s) > 1]


class UniverseRules:
    """One leg's universe rules (cfg: UNIVERSE_RULES_V1 when None, or UNIVERSE_RULES_V2 or V3): the ordinary-shares
    removals and, when cfg has the yield rule, the yield drops, for the results file's universe_rules block
    (screen inputs only: equal in drop, ltp and both)."""

    def __init__(self, cfg=None):
        self.cfg = UNIVERSE_RULES_V1 if cfg is None else cfg
        self.lines, self.removed, self.cik, self.drops = 0, [], [], []
        self.class_twins, self.class_held, self.starts = set(), {}, {}

    def not_started(self, target_date):
        """The leg's line_starts lines a screen on target_date leaves out: before its first own day."""
        return sorted(s for s, (d, _) in self.starts.items() if target_date.isoformat() < d)

    def mark_twins(self, con, universe="universe"):
        """Key the cfg's class twins (mark_class_twins) among the `universe` table's lines; nothing outside ur-2."""
        if self.cfg.get("class_twins"):
            mark_class_twins(con, universe=universe)
            self.class_twins = class_twin_symbols(con)

    def stand_ins(self, picked, target_date):
        """picked: one screen's book; the class twins in it each hold their company's slot."""
        held = sorted(s for s in picked if s in self.class_twins)
        if held:
            self.class_held[target_date.isoformat()] = held

    def sector_rows(self, rows):
        """rows: the sector query's dicts (symbol, exchange, cik, companyName, non_ordinary). Returns the
        kept rows; a symbol any of whose rows has a reason is removed under its first such row."""
        why = {}
        for r in sorted(rows, key=lambda r: (r["symbol"], str(r.get("non_ordinary")), str(r.get("companyName")))):
            if r.get("non_ordinary") and r["symbol"] not in why:
                why[r["symbol"]] = (r["non_ordinary"], r.get("companyName"))
        self.lines = len({r["symbol"] for r in rows})
        self.removed = [{"symbol": s, "reason": w, "name": n} for s, (w, n) in sorted(why.items())]
        kept = [r for r in rows if r["symbol"] not in why]
        self.cik = cik_shared(kept)
        kept_symbols = {r["symbol"] for r in kept}
        self.starts = {s: (d, e) for s, d, e in self.cfg.get("line_starts", ()) if s in kept_symbols}
        return kept

    def yield_drops(self, rows, target_date):
        """rows: (symbol, reason, fy_epoch, vendor, paid, cap, trade_ccy, report_ccy) of one screen's drops."""
        for s, why, fe, v, pd, cap, t, r in rows:
            self.drops.append({"rebalance_date": target_date.isoformat(), "symbol": s, "reason": why,
                               "fy": utc_date(fe).isoformat(), "vendor_yield": _sig(v),
                               "paid_yield": _sig(paid_yield(pd, cap)), "trade_ccy": t, "report_ccy": r})

    def block(self):
        count = lambda items: dict(sorted(Counter(i["reason"] for i in items).items()))
        drops = sorted(self.drops, key=lambda d: (d["rebalance_date"], d["symbol"], d["fy"], d["reason"]))
        out = {"version": self.cfg["version"],
               "ordinary_shares": {"rules": self.cfg["ordinary"], "lines": self.lines,
                                   "removed_count": len(self.removed), "by_reason": count(self.removed),
                                   "removed": list(self.removed), "cik_shared": list(self.cik)}}
        if self.cfg["dividend_yield"]:
            out["dividend_yield"] = {"rules": YIELD_RULES, "dropped": len(drops), "by_reason": count(drops),
                                     "drops": drops}
        if self.cfg.get("class_twins"):
            by_period = [{"rebalance_date": d, "symbols": s} for d, s in sorted(self.class_held.items())]
            out["class_stand_ins"] = {"rule": CLASS_TWIN_RULE, "lines_keyed": len(self.class_twins),
                                      "picked": sum(len(p["symbols"]) for p in by_period), "by_period": by_period}
        if self.starts:   # only a leg that holds a listed line, so every other leg's block is unchanged
            out["line_starts"] = {"rule": LINE_START_RULE, "lines": [
                {"symbol": s, "first_day": d, "evidence": e} for s, (d, e) in sorted(self.starts.items())]}
        return out


# Universe rules ur-2, opt-in through cfg: ur-1 plus a keep list, Brazil, US ADRs, trusts and LPs, no yield rule.
# Ordinary shares a pattern or flag misreads: (symbol, evidence).
ORDINARY_KNOWN_V2 = (
    ("JUP.L", "Jupiter Fund Management plc's ordinary share, an asset manager the profile flags isFund"),
    ("1414.T", "SHO-BOND Holdings' ordinary share, the word bond is part of the company name"),
    ("4764.TW", "Double Bond Chemical Industry's ordinary share, the word bond is part of the company name"),
    ("ABE.AX", "Australian Bond Exchange Holdings' ordinary share, the word bond is part of the company name"),
    ("5243.T", "Note Inc.'s ordinary share, the word note is the company name"),
    ("NOTE.ST", "NOTE AB's ordinary share, the word note is the company name"),
    ("SALE", "RetailMeNot's common stock, its profile name ends Series 1"),
    ("PTRN", "Pattern Group's common stock, its profile name reads Series A Common Stock"),
    ("MINDSPACE.NS", "Mindspace Business Parks REIT's listed units (REITs and their units stay)"),
    ("348950.KS", "JR Global REIT's listed shares, JR is part of the name, not junior"),
    ("BCE-PY.TO", "BCE's common price series to 2025 (July closes C$6.97 in 1996 to C$63.37 in 2022, 0.4 to 11 "
                  "million shares a day), BCE.TO has bars only from 2026, when this line prints a preferred's price"),
)
NON_ORDINARY_KNOWN_V2 = NON_ORDINARY_KNOWN + (
    ("AFGB", "notes", "American Financial Group notes from 2019-03, $25 par (median close 24.68, high 28.92), "
                      "AFG is the common"),
    ("AFGC", "notes", "American Financial Group notes from 2019-12, $25 par (median close 21.70, high 29.30), "
                      "AFG is the common"),
    ("AFGD", "notes", "American Financial Group notes from 2020-06, $25 par (median close 23.22, high 29.56), "
                      "AFG is the common"),
    ("AFGE", "notes", "American Financial Group notes from 2020-09, $25 par (median close 19.10, high 28.07), "
                      "AFG is the common"),
    ("MGR", "notes", "Affiliated Managers Group notes from 2019-03, $25 par (median close 23.86, high 28.49), "
                     "AMG is the common"),
    ("MGRB", "notes", "Affiliated Managers Group notes from 2020-09, $25 par (median close 18.84, high 26.98), "
                      "AMG is the common"),
    ("MGRD", "notes", "Affiliated Managers Group notes from 2021-07, $25 par (median close 16.51, high 25.21), "
                      "AMG is the common"),
    ("SCHP.SW", "certificate", "Schindler participation certificate (Partizipationsschein) on Schindler's "
                               "financials, SCHN.SW is the registered share (bars only from 2026-04)"),
    ("PBR-A", "preferred ADR", "Petrobras preferred-share ADR, PBR is the common ADR"),
    ("AZN", "ADR", "AstraZeneca's US line, an ADR until its 2026 direct listing; the profile now carries the GB "
                   "ordinary share's ISIN and doesn't flag isAdr"),
    ("RDS-B", "ADR", "Royal Dutch Shell's class B ADR (ISIN US7802591070) to 2022, which the profile doesn't flag "
                     "isAdr; SHEL, the flagged ADR, sits beside it"),
    ("WIMI", "ADR", "WiMi Hologram Cloud's American depositary shares of a Cayman company, which the profile doesn't "
                    "flag isAdr"),
    ("JHX", "ADR", "James Hardie's US line, an ADR until its 2025 move to an NYSE primary listing; the profile now "
                   "carries the Irish ordinary share's ISIN and doesn't flag isAdr"),
    ("MAN3.DE", "preferred", "MAN SE preference shares 1998-07 to 2021-08 with no ISIN in the profile, so the German "
                             "sibling test cannot see them, MAN.DE is the common"),
    ("BASA.DE", "second line", "a second BASF line 2010-08 to 2021-03 at a quarter to a third of BAS.DE's price, no "
                               "ISIN, median volume 864 against BAS.DE's 2.5 million"),
    ("4332.HK", "pilot line", "Amgen's HKEX pilot-programme line from 2000-05-31 (US ISIN, not trading), AMGN is "
                              "the share"),
    ("4333.HK", "pilot line", "Cisco's HKEX pilot-programme line from 2000-05-31 (US ISIN): HKD closes 6.8 to 8.0 "
                              "times CSCO's to 2009 (the peg is near 7.8), then 1.64 to 8.98, and +354% from July "
                              "2016 against CSCO's +13%"),
    ("4335.HK", "pilot line", "Intel's HKEX pilot-programme line from 2000-05-31 (US ISIN): HKD closes 3.7 to 24.3 "
                              "times INTC's, and +220% from July 2017 against INTC's +54%"),
    ("4336.HK", "pilot line", "Applied Materials' HKEX pilot-programme line from 2000-05-31 (US ISIN, not trading), "
                              "AMAT is the share"),
    ("4337.HK", "pilot line", "Starbucks' HKEX pilot-programme line from 2000-05-31 (US ISIN, not trading), SBUX is "
                              "the share"),
    ("4338.HK", "pilot line", "Microsoft's HKEX pilot-programme line from 2000-05-31 (US ISIN): HKD closes 5.2 to 5.5 "
                              "times MSFT's in 2003-2009, then 1.05 to 7.21, and +731% from July 2017 against "
                              "MSFT's +50%"),
    ("029960.KS", "kosdaq duplicate", "Korea Environment Technology's KSC line, the same close as its KOSDAQ series "
                                      "029960.KQ on 4,948 of 4,977 shared days, ends 2024-08-14"),
    ("049070.KS", "kosdaq duplicate", "Intops' KSC line, the same close as its KOSDAQ series 049070.KQ on 5,576 of "
                                      "5,646 shared days, ends 2024-08-16"),
    ("064760.KS", "kosdaq duplicate", "Tokai Carbon Korea's KSC line, the same close as its KOSDAQ series 064760.KQ "
                                      "on 5,190 of 5,208 shared days, ends 2024-08-16"),
    ("069080.KS", "kosdaq duplicate", "Webzen's KSC line, the same close as its KOSDAQ series 069080.KQ on 5,049 of "
                                      "5,065 shared days"),
    ("265520.KS", "kosdaq duplicate", "Advanced Process Systems' KSC line, the same close as its KOSDAQ series "
                                      "265520.KQ on 1,797 of 1,807 shared days, ends 2024-08-14"),
    ("GUD.AX", "renamed line", "GUD Holdings' line, renamed Amotiv in 2024-06: AOV.AX carries the same July closes "
                               "1990 to 2024 under its own ISIN, GUD.AX delisted 2024-06-27"),
)
NAME_TERMS_V2 = NAME_TERMS.replace(r"\bser\b", r"\bser$")
US_TRUST_LP_TERMS = (r"\bL\.\s?P\.|\bLP\b|\blimited partnership\b|\b(royalty|oil|gas|energy|minerals?|petroleum) "
                     r"trust\b|^mesabi trust$")
ORDINARY_RULES_V2 = {
    "rule": ("a line is removed when it is a different instrument from the company's ordinary share (preferred, "
             "note, bond, unit, warrant, right, fund) or a second line at its own price on the same financials "
             "that no security key collapses, and a book holds what its market's main index can hold: the S&P 500 "
             "holds no ADRs, royalty trusts or LPs, the Ibovespa no BDRs; a `keep` line stays whatever a pattern "
             "or flag says, and otherwise the first reason in `order` is recorded"),
    # ur-1's reasons with the longer hand list, then the new ones ahead of name_term
    "order": {**{k: v for k, v in ORDINARY_RULES["order"].items() if k != "name_term"},
              "known": "NON_ORDINARY_KNOWN_V2, the hand list below",
              "brazil_class": ("SAO line that isn't the ordinary share (ISIN class, characters 9-10, NO; with "
                               "no ISIN, ticker class digit 3) and is a BDR (R0) or fund (F0), or a line of a "
                               "company with no ordinary line (issuer: ISIN characters 3-6, else the ticker's "
                               "first four)"),
              "us_adr": "NYSE, NASDAQ or AMEX line the profile flags isAdr",
              "us_trust_lp": f"NYSE, NASDAQ or AMEX line whose companyName matches, ignoring case: {US_TRUST_LP_TERMS}",
              "name_term": f"companyName matches, ignoring case: {NAME_TERMS_V2}"},
    "kept": ("Thai NVDRs (check N keys X-R.BK to X.BK), depositary receipts of ordinary shares outside NYSE, "
             "NASDAQ, AMEX and SAO (TDRs, isAdr lines), share classes (BF-B, MKC-V, CENTA, the Swedish ser. B "
             "lines), REITs and their units, trust units and LPs outside the US, SAO preferred and unit lines of "
             "a company with an ordinary line (class twins: one slot, see class_stand_ins), and the `keep` lines "
             "below"),
    "known": [{"symbol": s, "kind": k, "evidence": e} for s, k, e in NON_ORDINARY_KNOWN_V2],
    "keep": [{"symbol": s, "evidence": e} for s, e in ORDINARY_KNOWN_V2],
}
UNIVERSE_RULES_V1 = {"version": "ur-1", "ordinary": ORDINARY_RULES, "known": NON_ORDINARY_KNOWN,
                     "name_terms": NAME_TERMS, "keep": (), "v2": False, "dividend_yield": True}
# (symbol, first own day, evidence): a line whose earlier bars and rows copy another line's history
LINE_STARTS_V2 = (
    ("AZZA3.SA", "2024-08-27", "Azzas 2154, from Arezzo's merger with Soma: its bars to 2024-08-26 repeat ARZZ3.SA's "
                               "closes and volumes, and ARZZ3.SA's last bar is 2024-08-26"),
)
LINE_START_RULE = ("a listed line enters the universe on its first own day: before it, its bars and financial rows "
                   "copy another line's history, so a screen before that day leaves it out and its slot goes to the "
                   "next name")
UNIVERSE_RULES_V2 = {"version": "ur-2", "ordinary": ORDINARY_RULES_V2, "known": NON_ORDINARY_KNOWN_V2,
                     "name_terms": NAME_TERMS_V2, "keep": ORDINARY_KNOWN_V2, "v2": True, "dividend_yield": False,
                     "class_twins": True, "line_starts": LINE_STARTS_V2}
CLASS_TWIN_RULE = ("an SAO preferred or unit line of a company with an ordinary line shares that line's security "
                   "key, so the company holds one slot: the ordinary line when it has an entry price, else the "
                   "first priced sibling, which stands in; by_period lists the stand-ins each screen picked, as "
                   "check N's counts do, so a period the cash rule then holds in cash still lists its picks")

# Universe rules ur-3, opt-in through cfg: ur-2 with more hand-list lines (receipts listed abroad among them) and
# more line starts. ur-2 stays as its results files ran it.
NON_ORDINARY_KNOWN_V3 = NON_ORDINARY_KNOWN_V2 + (
    ("FITBP", "preferred", "Fifth Third Bancorp's preferred depositary shares on NYSE beside FITB, sharing FITB's "
                           "issuer code (316773) in its ISIN"),
    ("76ID.L", "preferred", "Rotork's cumulative preference line beside ROR.L, with its own ISIN and price path"),
    ("SANB.L", "preferred", "Santander UK's preference line, with no listed ordinary share"),
    ("SDIP-PREF.ST", "preferred", "Sdiptech's preference line beside SDIP-B.ST, the B share"),
    *((s, "notes", "ANZ capital notes, prices near a 100 face value beside ANZ.AX, the share")
      for s in ("AN3PH.AX", "AN3PI.AX", "AN3PJ.AX")),
    ("CBAPK.AX", "notes", "CommBank PERLS, prices near a 100 face value beside CBA.AX, the share"),
    ("6288.HK", "depositary receipt", "Fast Retailing's Hong Kong depositary receipts on the Tokyo share, a US ISIN "
                                      "on a Japanese company"),
    ("ARION-SDB.ST", "depositary receipt", "Arion banki's Swedish depository receipts on the Icelandic share"),
    ("NTCO3.SA", "renamed line", "Natura &Co Holding's line: it copies NATU3.SA's history (the same close on "
                                 "5,225 of 5,238 shared days) and ends 2025-07-24 while NATU3.SA trades on"),
    ("9512.SR", "second line", "Riyadh Cement's second line beside 3092.SR, the same close on 574 of 577 shared "
                               "days, ends 2023-11-29"),
    ("JP", "ADR", "Jupai Holdings, a Cayman company whose NYSE line was American depositary shares, six ordinary "
                  "shares each, per its Form 20-F; the profile doesn't flag isAdr"),
)
LINE_STARTS_V3 = LINE_STARTS_V2 + (
    ("L1G.AX", "2025-10-04", "L1 Group: its bars to 2025-10-03 repeat PTM.AX's (Platinum Investment Management) "
                             "closes and volumes, and PTM.AX's last bar is 2025-10-03"),
    ("MATR.TO", "2023-06-07", "Mattr, Shawcor renamed: its bars to 2023-06-06 carry SCL.TO's volumes, and SCL.TO's "
                              "last bar is 2023-06-06"),
    ("TKO", "2023-09-12", "TKO Group Holdings, from WWE's merger with UFC: its bars to 2023-09-11 repeat WWE's "
                          "closes, and WWE's last bar is 2023-09-11"),
    ("AXIA6.SA", "2025-12-05", "AXIA Energia, Eletrobras renamed: its bars to 2025-12-04 repeat ELET6.SA's closes "
                               "and volumes, and ELET6.SA's last bar is 2025-12-04"),
    ("AXIA3.SA", "2025-12-05", "AXIA Energia's ordinary line from the same rename: its FY rows copy Eletrobras's "
                               "from 2006, while its bars start in 2026"),
)
ORDINARY_RULES_V3 = {
    "rule": ORDINARY_RULES_V2["rule"],
    "order": {**ORDINARY_RULES_V2["order"], "known": "NON_ORDINARY_KNOWN_V3, the hand list below"},
    "kept": ("Thai NVDRs (check N keys X-R.BK to X.BK), depositary receipts of ordinary shares outside NYSE, "
             "NASDAQ, AMEX and SAO (TDRs, isAdr lines) until a book holds one (the receipts on the `known` list "
             "below leave), ordinary shares listed first elsewhere (disclosed), share classes (BF-B, MKC-V, CENTA, "
             "the Swedish ser. B lines), REITs and their units, trust units and LPs outside the US, SAO preferred "
             "and unit lines of a company with an ordinary line (class twins: one slot, see class_stand_ins), and "
             "the `keep` lines below"),
    "known": [{"symbol": s, "kind": k, "evidence": e} for s, k, e in NON_ORDINARY_KNOWN_V3],
    "keep": ORDINARY_RULES_V2["keep"],
}
UNIVERSE_RULES_V3 = {**UNIVERSE_RULES_V2, "version": "ur-3", "ordinary": ORDINARY_RULES_V3,
                     "known": NON_ORDINARY_KNOWN_V3, "line_starts": LINE_STARTS_V3}


def _sao_issuer_sql(t):
    """An SAO line's company: ISIN characters 3-6, else the ticker's first four."""
    return (f"(CASE WHEN NULLIF(TRIM({t}.isin), '') IS NOT NULL THEN SUBSTR(UPPER(TRIM({t}.isin)), 3, 4)"
            f" ELSE SUBSTR({t}.symbol, 1, 4) END)")


def _sao_ordinary_sql(t):
    """An SAO line is the ordinary share: ISIN class (characters 9-10) NO, else a ticker class digit 3."""
    return (f"(CASE WHEN NULLIF(TRIM({t}.isin), '') IS NOT NULL THEN SUBSTR(UPPER(TRIM({t}.isin)), 9, 2) = 'NO'"
            f" ELSE regexp_full_match({t}.symbol, '[A-Z0-9]{{4}}3[A-Z]?[.]SA') END)")


def mark_class_twins(con, table="isin_map", universe=None):
    """ur-2's Brazil stand-in: key each SAO line that isn't its company's ordinary share to the ordinary line's
    security (sec_key, flagged class_twin), as check N keys an NVDR, so isin_dedup_sql(twin_key=True) holds one
    line per company: the ordinary line when it has an entry price, else the first priced sibling. `universe`
    (a table of symbols) limits the keyed lines to the leg's kept ones. Adds the twin columns when check N
    hasn't. Returns the number of lines keyed."""
    for col, typ in (("sec_key", "VARCHAR"), ("nvdr", "BOOLEAN"), ("class_twin", "BOOLEAN")):
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {typ}")
    kept = f" AND t.symbol IN (SELECT symbol FROM {universe})" if universe else ""
    con.execute(f"""
        UPDATE {table} AS t SET sec_key = o.key, class_twin = TRUE
        FROM (SELECT {_sao_issuer_sql('m')} AS issuer, MIN(COALESCE(NULLIF(UPPER(TRIM(m.isin)), ''), m.symbol)) AS key
              FROM {table} m WHERE m.symbol LIKE '%.SA' AND {_sao_ordinary_sql('m')} GROUP BY 1) o
        WHERE t.symbol LIKE '%.SA' AND NOT {_sao_ordinary_sql('t')} AND {_sao_issuer_sql('t')} = o.issuer{kept}""")
    return len(class_twin_symbols(con, table))


def class_twin_symbols(con, table="isin_map"):
    """The symbols mark_class_twins keyed."""
    return {r[0] for r in con.execute(f"SELECT symbol FROM {table} WHERE class_twin").fetchall()}


# ---------------------------------------------------------------------------
# Screen helpers shared by the topics' screen_stocks.

def started_sql(candidates_sql, late):
    """candidates_sql without the lines in late (UniverseRules.not_started on the screen date); unchanged when
    late is empty."""
    if not late:
        return candidates_sql
    return f"SELECT * FROM ({candidates_sql}) _s WHERE _s.symbol NOT IN ({_sql_list(late)})"


class RowGuard:
    """One leg's record of a screen guard: per rebalance date, how many candidates each reason left unranked
    (screen inputs only, so equal in drop, ltp and both). rule: the guard's text for the block; reasons: its
    reason names in block order (a row with another reason isn't counted). A date screened again replaces its
    count."""

    def __init__(self, rule, reasons):
        self.rule, self.reasons, self.periods = rule, tuple(reasons), {}

    def record(self, rows, target_date):
        """rows: (symbol, reason) of one screen's unranked candidates."""
        self.periods[target_date.isoformat()] = {k: sum(1 for _, why in rows if why == k) for k in self.reasons}

    def block(self):
        by_period = [{"rebalance_date": d, **n} for d, n in sorted(self.periods.items())]
        by_reason = {k: sum(p[k] for p in by_period) for k in self.reasons}
        return {"rule": self.rule, "dropped": sum(by_reason.values()), "by_reason": by_reason,
                "by_period": by_period}
