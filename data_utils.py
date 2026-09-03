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
    "BMV": "EWW",                          # Mexico
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
    "OSL": "^OSEAX",                                    # Oslo All Share (NOK, 2013+)
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
    "JNB": "ZAR",
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
    from datetime import datetime
    try:
        con.execute("SELECT volume FROM prices_cache LIMIT 0")
    except Exception:
        return list(symbols), []

    target_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    start_epoch = int(datetime.combine(
        target_date - timedelta(days=lookback_days), datetime.min.time()
    ).timestamp())
    sym_list = ",".join(f"'{s}'" for s in symbols)

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
        parquet_bytes = client.query(sql, format="parquet", limit=limit, timeout=timeout,
                                     verbose=verbose, memory_mb=memory_mb, threads=threads)
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
    price that exists, is positive, and clears the floor. It says nothing about
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
    - entry price below min_entry_price (bad adjClose, penny-stock artifacts)

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
        min_entry_price: float - minimum entry price to include (default $1.00)
        max_single_return: float - maximum return to include (default 2.0 = 200%)
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
        if raw_ret > max_single_return:
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


def get_prices(con, symbols, target_date, window_days=10, offset_days=0):
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

    Returns:
        dict[str, float] - {symbol: price}
    """
    if not symbols:
        return {}

    from datetime import datetime
    shifted_date = target_date + timedelta(days=offset_days)
    target_epoch = int(datetime.combine(shifted_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(shifted_date + timedelta(days=window_days), datetime.min.time()).timestamp())
    sym_list = ",".join(f"'{s}'" for s in symbols)

    # Try epoch-based schema first (used by most backtests)
    try:
        rows = con.execute(f"""
            SELECT symbol, trade_epoch, adjClose
            FROM prices_cache
            WHERE symbol IN ({sym_list})
              AND trade_epoch >= {target_epoch}
              AND trade_epoch <= {end_epoch}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_epoch ASC) = 1
        """).fetchall()
        return {r[0]: r[2] for r in rows}
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
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date ASC) = 1
        """).fetchall()
        return {r[0]: r[2] for r in rows}
    except Exception:
        return {}


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

    # Build neighbor table once (prices AND their timestamps)
    con.execute(f"""
        CREATE TEMPORARY TABLE _nb AS
        SELECT symbol, {date_col} AS dt, adjClose,
            LAG(adjClose, 1) OVER (PARTITION BY symbol ORDER BY {date_col}) AS p1,
            LEAD(adjClose, 1) OVER (PARTITION BY symbol ORDER BY {date_col}) AS n1,
            LAG(adjClose, 2) OVER (PARTITION BY symbol ORDER BY {date_col}) AS p2,
            LEAD(adjClose, 2) OVER (PARTITION BY symbol ORDER BY {date_col}) AS n2,
            LAG({dt_secs}, 1) OVER (PARTITION BY symbol ORDER BY {date_col}) AS pt1,
            LEAD({dt_secs}, 1) OVER (PARTITION BY symbol ORDER BY {date_col}) AS nt1,
            LAG({dt_secs}, 2) OVER (PARTITION BY symbol ORDER BY {date_col}) AS pt2,
            LEAD({dt_secs}, 2) OVER (PARTITION BY symbol ORDER BY {date_col}) AS nt2
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
