"""VWAP Mean Reversion -- NSE Intraday Strategy
----------------------------------------------
Signal: Stock drops below its intraday VWAP by >= X%.
        Enter long expecting reversion to VWAP.
        Exit at VWAP (target), stop-loss, or EOD.

VWAP = cumulative(price * volume) / cumulative(volume) during the day.
Mean reversion logic: stocks below VWAP have selling exhaustion,
tend to revert towards the volume-weighted fair value.

Pre-filter: Liquid stocks (volume >= min_volume, price >= min_price).
Data: fmp.stock_prices_minute (NSE), fmp.stock_eod (volume filter)
Period: 2020-2026 (minute data availability)
"""

from nse_arena.framework import Strategy


class VWAPReversion(Strategy):

    @property
    def name(self):
        return "VWAP Mean Reversion"

    @property
    def strategy_type(self):
        return "intraday"

    @property
    def sort_key(self):
        return "signal_strength"

    def default_config(self):
        return dict(
            vwap_discount=0.02,     # Enter when price is 2% below VWAP
            min_bar=30,             # Don't enter in first 30 min (VWAP needs data)
            max_entry_bar=180,      # Must enter within first 3 hours
            min_volume=5_000_000,   # High volume (same as ORB, ~top 100 NSE stocks)
            min_price=100,          # Rs 100+ mid/large cap
            min_range_pct=0.015,    # At least 1.5% daily range (need volatility for reversion)
            target_pct=0.01,        # Target: 1% above entry (or VWAP)
            stop_pct=0.015,         # 1.5% stop
            max_hold_bars=60,
            start_date="2020-01-06",
            end_date="2026-03-09",
            initial_capital=500_000,
            max_positions=5,
            order_value=50_000,
        )

    def sweep_grid(self):
        return dict(
            vwap_discount=[0.015, 0.02, 0.03],
            min_bar=30,
            max_entry_bar=[120, 180],
            min_volume=5_000_000,
            min_price=100,
            min_range_pct=0.015,
            target_pct=[0.01, 0.015],
            stop_pct=[0.01, 0.015, 0.02],
            max_hold_bars=[60, 120],
            start_date="2020-01-06",
            end_date="2026-03-09",
            initial_capital=500_000,
            max_positions=[5],
            order_value=50_000,
        )

    def build_sql(self, cfg):
        vwap_factor = round(1.0 - cfg["vwap_discount"], 6)
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

-- Step 2a: Minute bars with running VWAP
bars_raw AS (
    SELECT
        m.symbol,
        to_timestamp(m.dateEpoch)::DATE AS trade_date,
        m.dateEpoch, m.open, m.high, m.low, m.close, m.volume AS bar_volume,
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

bars AS (
    SELECT *,
        SUM(close * bar_volume) OVER (
            PARTITION BY symbol, trade_date ORDER BY bar_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / NULLIF(
            SUM(bar_volume) OVER (
                PARTITION BY symbol, trade_date ORDER BY bar_num
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 0
        ) AS vwap
    FROM bars_raw
),

-- Step 2b: Entry -- first bar where close drops below VWAP by discount
entry_candidates AS (
    SELECT
        symbol, trade_date, bar_num, close AS entry_price, vwap,
        (vwap - close) / NULLIF(vwap, 0) AS vwap_discount_actual,
        ROW_NUMBER() OVER (PARTITION BY symbol, trade_date ORDER BY bar_num) AS rn
    FROM bars
    WHERE bar_num >= {cfg["min_bar"]}
      AND bar_num <= {cfg["max_entry_bar"]}
      AND close <= vwap * {vwap_factor}
      AND bar_volume > 0
      AND vwap > 0
),

first_entry AS (
    SELECT symbol, trade_date, bar_num AS entry_bar,
           entry_price, vwap AS entry_vwap, vwap_discount_actual
    FROM entry_candidates WHERE rn = 1
),

-- Step 2c: Exit -- target (price >= entry * target_factor or >= vwap), stop, or EOD
exit_candidates AS (
    SELECT
        b.symbol, b.trade_date, b.bar_num, b.close AS exit_price,
        ROW_NUMBER() OVER (PARTITION BY b.symbol, b.trade_date ORDER BY b.bar_num) AS rn
    FROM bars b
    JOIN first_entry e USING (symbol, trade_date)
    WHERE b.bar_num > e.entry_bar
      AND b.bar_num <= e.entry_bar + {cfg["max_hold_bars"]}
      AND (b.close >= e.entry_price * {target_factor}
           OR b.close >= b.vwap
           OR b.close <= e.entry_price * {stop_factor})
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
    e.vwap_discount_actual AS vwap_discount,
    e.vwap_discount_actual AS signal_strength,
    b.bench_ret
FROM first_entry e
LEFT JOIN first_exit x USING (symbol, trade_date)
JOIN eod_exit eod USING (symbol, trade_date)
JOIN bench b USING (trade_date)
ORDER BY e.trade_date, e.vwap_discount_actual DESC
"""
