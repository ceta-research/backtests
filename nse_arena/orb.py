"""Opening Range Breakout (ORB) -- NSE Intraday Strategy
-------------------------------------------------------
Signal: Compute high/low of first N minutes (opening range).
        Enter long when price breaks above the OR high.
        Exit at take-profit, stop-loss (or below OR low), or EOD.

Pre-filter: Only liquid stocks (volume >= min_volume, price >= min_price).
Data: fmp.stock_prices_minute (NSE), fmp.stock_eod (volume filter)
Period: 2020-2026 (minute data availability)
"""

from nse_arena.framework import Strategy


class OpeningRangeBreakout(Strategy):

    @property
    def name(self):
        return "Opening Range Breakout"

    @property
    def strategy_type(self):
        return "intraday"

    @property
    def sort_key(self):
        return "signal_strength"

    def default_config(self):
        return dict(
            or_window=15,           # Opening range = first 15 bars (minutes)
            max_entry_bar=120,      # Must break out within first 2 hours
            min_volume=5_000_000,   # High volume (top ~100 liquid NSE stocks)
            min_price=100,          # Rs 100+ (mid/large cap only)
            min_range_pct=0.01,     # Minimum daily range (high-low)/low > 1%
            target_pct=0.015,       # 1.5% take-profit
            stop_pct=0.01,          # 1% stop-loss
            max_hold_bars=60,       # Exit within ~1 hour of entry
            start_date="2020-01-06",
            end_date="2026-03-09",
            initial_capital=500_000,
            max_positions=5,
            order_value=50_000,
        )

    def sweep_grid(self):
        return dict(
            or_window=[15, 30],
            max_entry_bar=[60, 120],
            min_volume=5_000_000,
            min_price=100,
            min_range_pct=0.01,
            target_pct=[0.01, 0.015, 0.02],
            stop_pct=[0.01, 0.015],
            max_hold_bars=[60, 120],
            start_date="2020-01-06",
            end_date="2026-03-09",
            initial_capital=500_000,
            max_positions=[5],
            order_value=50_000,
        )

    def build_sql(self, cfg):
        target_factor = round(1.0 + cfg["target_pct"], 6)
        stop_factor = round(1.0 - cfg["stop_pct"], 6)

        return f"""
WITH

-- Step 1: Scanner -- liquid, volatile NSE stocks from EOD
filtered_eod AS (
    SELECT
        symbol, date AS trade_date, open, close, high, low, volume,
        (close - open) / NULLIF(open, 0) AS oc_return
    FROM fmp.stock_eod
    WHERE symbol LIKE '%.NS'
      AND date BETWEEN '{cfg["start_date"]}' AND '{cfg["end_date"]}'
      AND volume >= {cfg["min_volume"]}
      AND open > {cfg["min_price"]}
      AND close > 0
      AND (high - low) / NULLIF(low, 0) >= {cfg["min_range_pct"]}
),

bench AS (
    SELECT trade_date, AVG(oc_return) AS bench_ret
    FROM filtered_eod
    GROUP BY trade_date
),

-- Step 2a: Minute bars (INNER JOIN prunes to liquid stocks only)
bars AS (
    SELECT
        m.symbol,
        to_timestamp(m.dateEpoch)::DATE AS trade_date,
        m.dateEpoch, m.open, m.high, m.low, m.close,
        ROW_NUMBER() OVER (
            PARTITION BY m.symbol, to_timestamp(m.dateEpoch)::DATE
            ORDER BY m.dateEpoch
        ) AS bar_num
    FROM fmp.stock_prices_minute m
    INNER JOIN filtered_eod f
        ON m.symbol = f.symbol
        AND to_timestamp(m.dateEpoch)::DATE = f.trade_date
    WHERE m.exchange = 'NSE'
),

-- Opening range: high/low of first N bars
opening_range AS (
    SELECT
        symbol, trade_date,
        MAX(high) AS or_high,
        MIN(low) AS or_low,
        MAX(high) - MIN(low) AS or_range
    FROM bars
    WHERE bar_num <= {cfg["or_window"]}
    GROUP BY symbol, trade_date
    HAVING MAX(high) > MIN(low)  -- Non-zero range
),

-- Step 2b: Entry -- first bar closing above OR high (breakout)
entry_candidates AS (
    SELECT
        b.symbol, b.trade_date, b.bar_num, b.close AS entry_price,
        o.or_high, o.or_low, o.or_range,
        ROW_NUMBER() OVER (PARTITION BY b.symbol, b.trade_date ORDER BY b.bar_num) AS rn
    FROM bars b
    JOIN opening_range o USING (symbol, trade_date)
    WHERE b.bar_num > {cfg["or_window"]}
      AND b.bar_num <= {cfg["max_entry_bar"]}
      AND b.close > o.or_high
),

first_entry AS (
    SELECT symbol, trade_date, bar_num AS entry_bar, entry_price,
           or_high, or_low, or_range
    FROM entry_candidates WHERE rn = 1
),

-- Step 2c: Exit -- target, stop-loss, or below OR low
exit_candidates AS (
    SELECT
        b.symbol, b.trade_date, b.bar_num, b.close AS exit_price,
        ROW_NUMBER() OVER (PARTITION BY b.symbol, b.trade_date ORDER BY b.bar_num) AS rn
    FROM bars b
    JOIN first_entry e USING (symbol, trade_date)
    WHERE b.bar_num > e.entry_bar
      AND b.bar_num <= e.entry_bar + {cfg["max_hold_bars"]}
      AND (b.close >= e.entry_price * {target_factor}
           OR b.close <= LEAST(e.entry_price * {stop_factor}, e.or_low))
),

first_exit AS (
    SELECT symbol, trade_date, exit_price
    FROM exit_candidates WHERE rn = 1
),

eod_exit AS (
    SELECT symbol, trade_date,
           FIRST(close ORDER BY bar_num DESC) AS eod_price
    FROM bars GROUP BY symbol, trade_date
)

SELECT
    e.symbol, e.trade_date, e.entry_bar, e.entry_price,
    COALESCE(x.exit_price, eod.eod_price) AS exit_price,
    CASE WHEN x.exit_price IS NOT NULL THEN 'signal' ELSE 'eod' END AS exit_type,
    e.or_range / NULLIF(e.or_low, 0) AS or_range_pct,
    e.or_range / NULLIF(e.or_low, 0) AS signal_strength,
    b.bench_ret
FROM first_entry e
LEFT JOIN first_exit x USING (symbol, trade_date)
JOIN eod_exit eod USING (symbol, trade_date)
JOIN bench b USING (trade_date)
ORDER BY e.trade_date, e.or_range DESC
"""
