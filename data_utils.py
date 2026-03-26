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


def filter_returns(symbol_returns, min_entry_price=1.0, max_single_return=2.0, verbose=False):
    """Filter individual stock returns for data quality.

    Removes stocks with:
    - Entry price below min_entry_price (bad adjClose data, penny stock artifacts)
    - Single-period return above max_single_return (price data artifacts, symbol reassignments)

    Args:
        symbol_returns: list of (symbol, entry_price, exit_price, market_cap) tuples
        min_entry_price: float - minimum entry price to include (default $1.00)
        max_single_return: float - maximum return to include (default 2.0 = 200%)
        verbose: bool - print skipped stocks

    Returns:
        tuple(clean_returns, skipped) where:
        - clean_returns: list of (symbol, raw_return, market_cap) tuples
        - skipped: list of skip reason strings
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
