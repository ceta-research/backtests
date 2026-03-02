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


def query_parquet(client, sql, con, table_name, verbose=False, limit=1000000, timeout=300,
                  memory_mb=None, threads=None):
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

    Returns:
        int - number of rows loaded
    """
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
        return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    finally:
        os.unlink(tmp_path)


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


def get_prices(con, symbols, target_date, window_days=10):
    """Get adjusted close prices for symbols at/near a target date.

    Uses the first available price in [target_date, target_date + window_days].
    Handles both epoch-based (trade_epoch) and date-based (trade_date) schemas.

    Args:
        con: duckdb.Connection with prices_cache table
        symbols: list[str] - stock symbols
        target_date: date - target rebalance date
        window_days: int - number of days to search forward

    Returns:
        dict[str, float] - {symbol: price}
    """
    if not symbols:
        return {}

    from datetime import datetime
    target_epoch = int(datetime.combine(target_date, datetime.min.time()).timestamp())
    end_epoch = int(datetime.combine(target_date + timedelta(days=window_days), datetime.min.time()).timestamp())
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
        target_str = target_date.isoformat()
        end_str = (target_date + timedelta(days=window_days)).isoformat()
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
