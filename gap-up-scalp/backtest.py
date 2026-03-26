#!/usr/bin/env python3
"""
Gap-Up Pullback Scalp Backtest — NSE Intraday
----------------------------------------------
Strategy:
  Each day, identify NSE stocks that:
    1. Gap up >= GAP_PCT at open vs prior close
    2. Pull back >= PULLBACK_PCT from the gap-open within ENTRY_WINDOW minutes
  Enter at the first pullback bar.
  Exit: take-profit (TARGET_PCT above entry) OR stop-loss (STOP_PCT below entry)
        OR forced EOD close.

Data:
  fmp.stock_prices_minute  — NSE minute bars (IST timestamps stored as UTC, 2020-2026)
  fmp.stock_eod            — daily OHLCV (prior close, volume)

Borrows from ATO_Simulator:
  - NSE intraday brokerage charges  (ATO_Simulator.util.broker_functions)
  - Config-driven parameter sweeps  (ATO_Simulator.simulator.util.parse_input_config)
  - Portfolio state machine          (ATO_Simulator.simulator.steps.simulate_step)

Benchmark: equal-weight all liquid NSE stocks, open-to-close, same days.
"""

import sys
import math
import json
from pathlib import Path
from collections import defaultdict
from itertools import product
from datetime import date as date_type

sys.path.insert(0, str(Path(__file__).parent.parent))
from cr_client import CetaResearch
from metrics import compute_metrics


# ── Default parameters ─────────────────────────────────────────────────────────
#  These mirror the ATO_Simulator "config" pattern:
#  pass lists to sweep, single values for fixed params.

SCANNER = dict(
    gap_pct    = [0.03, 0.05],    # Min gap-up at open vs prior close
    min_volume = [500_000],        # Min EOD volume
    min_price  = [10],             # Min open price INR (exclude sub-₹10 stocks)
)

ENTRY = dict(
    pullback_pct  = [0.02, 0.03],  # Min pullback from gap-open to trigger entry
    entry_window  = [15, 30],      # Max minutes after open to find pullback
)

EXIT = dict(
    target_pct   = [0.02, 0.03],  # Take-profit above entry
    stop_pct     = [0.02, 0.03],  # Stop-loss below entry
    max_hold_bars = [60],          # Force EOD exit after this many bars post-entry
)

SIM = dict(
    initial_capital = [500_000],   # INR  (~$5k at ~₹84/USD)
    max_positions   = [5, 10],     # Max simultaneous intraday trades per day
)

START_DATE = "2020-01-06"
END_DATE   = "2026-03-09"
ORDER_VALUE = 50_000               # INR per trade (fixed, not swept here)

# Risk-free rate for Sharpe: India 10-yr Gsec yield ~7%
RISK_FREE_RATE = 0.07


# ── Brokerage (NSE intraday / Zerodha MIS) ─────────────────────────────────────
# Adapted from ATO_Simulator.util.broker_functions.calculate_charges()
def nse_intraday_charges(order_value: float) -> float:
    """Round-trip intraday brokerage cost for NSE equity (Zerodha MIS rates)."""
    brokerage_per_leg = min(order_value * 0.0003, 20.0)       # max ₹20/leg
    brokerage         = brokerage_per_leg * 2
    stt               = order_value * 0.00025                  # 0.025% sell-side only
    exchange          = order_value * 0.0000345 * 2            # 0.00345% both sides
    sebi              = order_value * 0.000001  * 2            # ₹10/crore both sides
    stamp             = order_value * 0.00003                  # 0.003% buy-side only
    gst               = (brokerage_per_leg * 2 + exchange) * 0.18
    return round(brokerage + stt + exchange + sebi + stamp + gst, 2)


# ── SQL ────────────────────────────────────────────────────────────────────────
def build_sql(gap_pct, min_volume, min_price,
              pullback_pct, entry_window,
              target_pct, stop_pct, max_hold_bars,
              start_date, end_date):
    """
    Server-side signal generation on CR platform.
    Returns one row per trade: (symbol, trade_date, entry_price, exit_price,
                                 exit_type, gap_pct, entry_bar)

    Pipeline mirrors ATO_Simulator's three steps:
      Step 1  → Scanner:  identify gap-up candidates from EOD data
      Step 2  → Orders:   find entry (pullback) and exit (target/stop/EOD) in minute data
      Step 3  → Simulate: done in Python below (portfolio state, P&L, metrics)
    """
    pullback_factor = round(1.0 - pullback_pct, 6)
    target_factor   = round(1.0 + target_pct,   6)
    stop_factor     = round(1.0 - stop_pct,     6)

    return f"""
WITH

-- ── Step 1: Scanner — gap-up candidates from EOD ─────────────────────────────
eod AS (
    SELECT
        symbol, date,
        open   AS gap_open_price,
        close  AS eod_close,
        volume,
        (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
            / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
        -- benchmark: open-to-close return for ALL liquid stocks
        (close - open) / NULLIF(open, 0) AS oc_return
    FROM fmp.stock_eod
    WHERE symbol LIKE '%.NS'
      AND date BETWEEN '{start_date}' AND '{end_date}'
      AND open  > {min_price}
      AND close > 0
),

gap_up AS (
    SELECT symbol, date AS trade_date, gap_open_price, gap_pct
    FROM eod
    WHERE gap_pct  BETWEEN {gap_pct} AND 0.40
      AND volume   >= {min_volume}
),

-- Benchmark returns (all liquid NSE stocks, open-to-close) per day
bench AS (
    SELECT date AS trade_date, AVG(oc_return) AS bench_ret
    FROM eod
    WHERE volume >= {min_volume}
    GROUP BY date
),

-- ── Step 2a: Minute bars — only for gap-up stocks (INNER JOIN prunes 385M rows) ──
bars AS (
    SELECT
        m.symbol,
        to_timestamp(m.dateEpoch)::DATE AS trade_date,
        m.dateEpoch,
        m.open, m.high, m.low, m.close,
        ROW_NUMBER() OVER (
            PARTITION BY m.symbol, to_timestamp(m.dateEpoch)::DATE
            ORDER BY m.dateEpoch
        ) AS bar_num,
        -- gap_open = open price of first bar (the gap-open at market open)
        FIRST_VALUE(m.open) OVER (
            PARTITION BY m.symbol, to_timestamp(m.dateEpoch)::DATE
            ORDER BY m.dateEpoch
        ) AS gap_open
    FROM fmp.stock_prices_minute m
    INNER JOIN gap_up g
           ON  m.symbol = g.symbol
           AND to_timestamp(m.dateEpoch)::DATE = g.trade_date
    WHERE m.exchange = 'NSE'
),

-- ── Step 2b: Orders — first pullback entry within entry_window ────────────────
entry_candidates AS (
    SELECT
        symbol, trade_date, bar_num, dateEpoch AS entry_epoch, close AS entry_price,
        ROW_NUMBER() OVER (PARTITION BY symbol, trade_date ORDER BY bar_num) AS rn
    FROM bars
    WHERE bar_num  BETWEEN 2 AND {entry_window}   -- within entry window
      AND close   <= gap_open * {pullback_factor}  -- pulled back enough from gap-open
),

first_entry AS (
    SELECT symbol, trade_date, bar_num AS entry_bar, entry_epoch, entry_price
    FROM entry_candidates
    WHERE rn = 1
),

-- ── Step 2c: Orders — first exit signal (target or stop-loss) ────────────────
exit_candidates AS (
    SELECT
        b.symbol, b.trade_date, b.bar_num,
        b.dateEpoch AS exit_epoch,
        b.close     AS exit_price,
        ROW_NUMBER() OVER (PARTITION BY b.symbol, b.trade_date ORDER BY b.bar_num) AS rn
    FROM bars b
    INNER JOIN first_entry e USING (symbol, trade_date)
    WHERE b.bar_num  > e.entry_bar
      AND b.bar_num <= e.entry_bar + {max_hold_bars}
      AND (   b.close >= e.entry_price * {target_factor}   -- hit take-profit
           OR b.close <= e.entry_price * {stop_factor})    -- hit stop-loss
),

first_exit AS (
    SELECT symbol, trade_date, exit_epoch, exit_price
    FROM exit_candidates
    WHERE rn = 1
),

-- EOD fallback: last bar if target/stop never hit
eod_exit AS (
    SELECT
        symbol, trade_date,
        MAX(dateEpoch)                              AS eod_epoch,
        FIRST(close ORDER BY bar_num DESC)          AS eod_price
    FROM bars
    GROUP BY symbol, trade_date
)

-- ── Final trade records ───────────────────────────────────────────────────────
SELECT
    e.symbol,
    e.trade_date,
    e.entry_bar,
    e.entry_price,
    COALESCE(x.exit_epoch,  eod.eod_epoch)                          AS exit_epoch,
    COALESCE(x.exit_price,  eod.eod_price)                          AS exit_price,
    CASE WHEN x.exit_epoch IS NOT NULL THEN 'signal' ELSE 'eod' END AS exit_type,
    g.gap_pct,
    b.bench_ret
FROM       first_entry e
JOIN       gap_up      g   USING (symbol, trade_date)
LEFT JOIN  first_exit  x   USING (symbol, trade_date)
JOIN       eod_exit    eod USING (symbol, trade_date)
JOIN       bench       b   USING (trade_date)
ORDER BY   e.trade_date, e.entry_price DESC
"""


# ── Portfolio simulation ────────────────────────────────────────────────────────
# Adapted from ATO_Simulator.simulator.steps.simulate_step.process_step
def simulate(trades: list[dict], max_positions: int,
             order_value: float, initial_capital: float):
    """
    Intraday portfolio simulation.

    All positions open and close within the same day — no overnight holds.
    Each day: select up to max_positions trades (highest gap_pct first),
    allocate order_value per trade, deduct NSE intraday charges.

    Returns (daily_returns, bench_returns) as parallel lists of floats.
    daily_returns[i]  = strategy return on day i (fraction of initial_capital)
    bench_returns[i]  = benchmark return on day i (equal-weight all liquid NSE)
    """
    by_date = defaultdict(list)
    bench_by_date = {}
    for t in trades:
        by_date[str(t["trade_date"])].append(t)
        bench_by_date[str(t["trade_date"])] = t["bench_ret"] or 0.0

    # charges as a fixed fraction of order_value (pre-compute for speed)
    charges = nse_intraday_charges(order_value)

    daily_returns  = []
    bench_returns  = []

    for d in sorted(by_date.keys()):
        day_trades = by_date[d]

        # Sort by gap_pct desc — biggest movers get priority (scanner ranking)
        day_trades.sort(key=lambda t: t["gap_pct"] or 0, reverse=True)
        selected = day_trades[:max_positions]

        day_pnl = sum(
            (t["exit_price"] - t["entry_price"]) / t["entry_price"] * order_value - charges
            for t in selected
        )

        # Return as fraction of total capital (same as ATO_Simulator % of account value)
        daily_returns.append(day_pnl / initial_capital)
        bench_returns.append(bench_by_date[d])

    return daily_returns, bench_returns


# ── Config sweep ───────────────────────────────────────────────────────────────
# Mirrors ATO_Simulator.simulator.util.parse_input_config.create_config_iterator()
def config_sweep(scanner, entry, exit_, sim):
    """Yield every combination of parameters (Cartesian product)."""
    s_keys, s_vals = zip(*scanner.items())
    e_keys, e_vals = zip(*entry.items())
    x_keys, x_vals = zip(*exit_.items())
    m_keys, m_vals = zip(*sim.items())

    for s in product(*s_vals):
        for e in product(*e_vals):
            for x in product(*x_vals):
                for m in product(*m_vals):
                    yield {
                        **dict(zip(s_keys, s)),
                        **dict(zip(e_keys, e)),
                        **dict(zip(x_keys, x)),
                        **dict(zip(m_keys, m)),
                    }


# ── Main ──────────────────────────────────────────────────────────────────────
def run(cfg: dict, client: CetaResearch, verbose=True) -> dict:
    sql = build_sql(
        gap_pct       = cfg["gap_pct"],
        min_volume    = cfg["min_volume"],
        min_price     = cfg["min_price"],
        pullback_pct  = cfg["pullback_pct"],
        entry_window  = cfg["entry_window"],
        target_pct    = cfg["target_pct"],
        stop_pct      = cfg["stop_pct"],
        max_hold_bars = cfg["max_hold_bars"],
        start_date    = START_DATE,
        end_date      = END_DATE,
    )

    trades = client.query(sql, memory_mb=16384, threads=6, timeout=600)

    if not trades:
        return {"error": "no trades"}

    daily_rets, bench_rets = simulate(
        trades,
        max_positions   = cfg["max_positions"],
        order_value     = ORDER_VALUE,
        initial_capital = cfg["initial_capital"],
    )

    if not daily_rets:
        return {"error": "no active days"}

    result = compute_metrics(
        period_returns     = daily_rets,
        benchmark_returns  = bench_rets,
        periods_per_year   = 252,
        risk_free_rate     = RISK_FREE_RATE,
    )

    n_trades      = len(trades)
    signal_exits  = sum(1 for t in trades if t["exit_type"] == "signal")
    eod_exits     = n_trades - signal_exits
    wins          = sum(1 for t in trades
                        if t["exit_price"] and t["entry_price"]
                        and t["exit_price"] > t["entry_price"])

    summary = {
        "cfg": cfg,
        "total_trades":   n_trades,
        "active_days":    len(daily_rets),
        "signal_exits_pct": round(signal_exits / n_trades * 100, 1) if n_trades else 0,
        "trade_win_rate": round(wins / n_trades * 100, 1) if n_trades else 0,
        "cagr":           round(result["portfolio"]["cagr"] * 100, 2),
        "bench_cagr":     round(result["benchmark"]["cagr"] * 100, 2),
        "excess":         round((result["portfolio"]["cagr"] - result["benchmark"]["cagr"]) * 100, 2),
        "sharpe":         round(result["portfolio"]["sharpe_ratio"], 3),
        "max_dd":         round(result["portfolio"]["max_drawdown"] * 100, 1),
        "avg_daily_ret":  round(sum(daily_rets) / len(daily_rets) * 100, 3),
    }

    if verbose:
        print(f"\n{'─'*60}")
        print(f"gap={cfg['gap_pct']:.0%}  pullback={cfg['pullback_pct']:.0%}  "
              f"window={cfg['entry_window']}m  "
              f"T/P={cfg['target_pct']:.0%}  SL={cfg['stop_pct']:.0%}  "
              f"positions={cfg['max_positions']}")
        print(f"  Trades: {n_trades:,}  |  Signal exits: {signal_exits/n_trades*100:.0f}%  "
              f"|  Trade win rate: {summary['trade_win_rate']}%")
        print(f"  CAGR:   {summary['cagr']:+.2f}%  (bench {summary['bench_cagr']:+.2f}%,"
              f" excess {summary['excess']:+.2f}%)")
        print(f"  Sharpe: {summary['sharpe']:.3f}  |  MaxDD: {summary['max_dd']:.1f}%")

    return summary


if __name__ == "__main__":
    client = CetaResearch()

    # ── Single run with default params ────────────────────────────────────────
    default_cfg = dict(
        gap_pct       = 0.03,
        min_volume    = 500_000,
        min_price     = 10,
        pullback_pct  = 0.03,
        entry_window  = 30,
        target_pct    = 0.02,
        stop_pct      = 0.02,
        max_hold_bars = 60,
        initial_capital = 500_000,
        max_positions   = 5,
    )

    print("=== Gap-Up Pullback Scalp — NSE 2020-2026 ===")
    print(f"Order value per trade: ₹{ORDER_VALUE:,}  |  "
          f"Round-trip charges: ₹{nse_intraday_charges(ORDER_VALUE):.2f}")
    print("\nRunning default config...")
    default_result = run(default_cfg, client)

    # ── Parameter sweep ───────────────────────────────────────────────────────
    print("\n\nRunning parameter sweep "
          f"({sum(1 for _ in config_sweep(SCANNER, ENTRY, EXIT, SIM))} configs)...")

    results = []
    for i, cfg in enumerate(config_sweep(SCANNER, ENTRY, EXIT, SIM)):
        print(f"\n[{i+1}] Fetching trades...", end=" ", flush=True)
        r = run(cfg, client, verbose=True)
        results.append(r)

    # ── Summary table ─────────────────────────────────────────────────────────
    valid = [r for r in results if "error" not in r]
    if valid:
        print(f"\n\n{'═'*70}")
        print(f"{'GAP':>5} {'PULL':>5} {'WIN':>4} {'TP':>4} {'SL':>4} "
              f"{'POS':>4} | {'CAGR':>7} {'BENCH':>7} {'XS':>7} {'SHP':>6} {'MDD':>7}")
        print(f"{'─'*70}")
        for r in sorted(valid, key=lambda x: x["cagr"], reverse=True):
            c = r["cfg"]
            print(f"{c['gap_pct']:>5.0%} {c['pullback_pct']:>5.0%} "
                  f"{c['entry_window']:>4} {c['target_pct']:>4.0%} "
                  f"{c['stop_pct']:>4.0%} {c['max_positions']:>4} | "
                  f"{r['cagr']:>+7.2f} {r['bench_cagr']:>+7.2f} "
                  f"{r['excess']:>+7.2f} {r['sharpe']:>6.3f} {r['max_dd']:>7.1f}%")
