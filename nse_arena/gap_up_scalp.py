"""Gap-Up Pullback Scalp -- NSE Intraday Strategy
-------------------------------------------------
Signal: Stock gaps up >= X% at open vs prior close, then pulls back >= Y%
        within first N minutes. Enter at pullback, exit at T/P or S/L or EOD.

Data: fmp.stock_prices_minute (NSE), fmp.stock_eod (prior close, volume)
Period: 2020-2026 (minute data availability)
"""

from nse_arena.framework import Strategy


class GapUpScalp(Strategy):

    @property
    def name(self):
        return "Gap-Up Pullback Scalp"

    @property
    def strategy_type(self):
        return "intraday"

    @property
    def sort_key(self):
        return "gap_pct"

    def default_config(self):
        return dict(
            gap_pct=0.10,
            min_volume=500_000,
            min_price=10,
            pullback_pct=0.03,
            entry_window=30,
            target_pct=0.02,
            stop_pct=0.02,
            max_hold_bars=60,
            start_date="2020-01-06",
            end_date="2026-03-09",
            initial_capital=500_000,
            max_positions=5,
            order_value=50_000,
        )

    def sweep_grid(self):
        return dict(
            gap_pct=[0.03, 0.05, 0.10],
            min_volume=500_000,
            min_price=10,
            pullback_pct=[0.02, 0.03],
            entry_window=[15, 30],
            target_pct=[0.02, 0.03],
            stop_pct=[0.01, 0.02],
            max_hold_bars=60,
            start_date="2020-01-06",
            end_date="2026-03-09",
            initial_capital=500_000,
            max_positions=[5],
            order_value=50_000,
        )

    def build_sql(self, cfg):
        pullback_factor = round(1.0 - cfg["pullback_pct"], 6)
        target_factor = round(1.0 + cfg["target_pct"], 6)
        stop_factor = round(1.0 - cfg["stop_pct"], 6)

        return f"""
WITH

-- Step 1: Scanner -- gap-up candidates from EOD
eod AS (
    SELECT
        symbol, date,
        open AS gap_open_price,
        close AS eod_close,
        volume,
        (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
            / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
        (close - open) / NULLIF(open, 0) AS oc_return
    FROM fmp.stock_eod
    WHERE symbol LIKE '%.NS'
      AND date BETWEEN '{cfg["start_date"]}' AND '{cfg["end_date"]}'
      AND open > {cfg["min_price"]}
      AND close > 0
),

gap_up AS (
    SELECT symbol, date AS trade_date, gap_open_price, gap_pct
    FROM eod
    WHERE gap_pct BETWEEN {cfg["gap_pct"]} AND 0.40
      AND volume >= {cfg["min_volume"]}
),

bench AS (
    SELECT date AS trade_date, AVG(oc_return) AS bench_ret
    FROM eod WHERE volume >= {cfg["min_volume"]}
    GROUP BY date
),

-- Step 2: Minute bars (INNER JOIN prunes to gap-up stocks only)
bars AS (
    SELECT
        m.symbol,
        to_timestamp(m.dateEpoch)::DATE AS trade_date,
        m.dateEpoch, m.open, m.high, m.low, m.close,
        ROW_NUMBER() OVER (
            PARTITION BY m.symbol, to_timestamp(m.dateEpoch)::DATE
            ORDER BY m.dateEpoch
        ) AS bar_num,
        FIRST_VALUE(m.open) OVER (
            PARTITION BY m.symbol, to_timestamp(m.dateEpoch)::DATE
            ORDER BY m.dateEpoch
        ) AS gap_open
    FROM fmp.stock_prices_minute m
    INNER JOIN gap_up g
        ON m.symbol = g.symbol
        AND to_timestamp(m.dateEpoch)::DATE = g.trade_date
    WHERE m.exchange = 'NSE'
),

-- Pullback entry: first bar within window where close <= gap_open * pullback_factor
entry_candidates AS (
    SELECT
        symbol, trade_date, bar_num, dateEpoch AS entry_epoch,
        close AS entry_price,
        ROW_NUMBER() OVER (PARTITION BY symbol, trade_date ORDER BY bar_num) AS rn
    FROM bars
    WHERE bar_num BETWEEN 2 AND {cfg["entry_window"]}
      AND close <= gap_open * {pullback_factor}
),

first_entry AS (
    SELECT symbol, trade_date, bar_num AS entry_bar, entry_epoch, entry_price
    FROM entry_candidates WHERE rn = 1
),

-- Exit: target or stop-loss
exit_candidates AS (
    SELECT
        b.symbol, b.trade_date, b.bar_num,
        b.close AS exit_price,
        ROW_NUMBER() OVER (PARTITION BY b.symbol, b.trade_date ORDER BY b.bar_num) AS rn
    FROM bars b
    INNER JOIN first_entry e USING (symbol, trade_date)
    WHERE b.bar_num > e.entry_bar
      AND b.bar_num <= e.entry_bar + {cfg["max_hold_bars"]}
      AND (b.close >= e.entry_price * {target_factor}
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
    g.gap_pct,
    g.gap_pct AS signal_strength,
    b.bench_ret
FROM first_entry e
JOIN gap_up g USING (symbol, trade_date)
LEFT JOIN first_exit x USING (symbol, trade_date)
JOIN eod_exit eod USING (symbol, trade_date)
JOIN bench b USING (trade_date)
ORDER BY e.trade_date, g.gap_pct DESC
"""
