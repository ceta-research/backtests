# Volume-Confirmed Momentum

A momentum strategy that filters for stocks with strong 12-month price performance *and* rising trading volume. Volume confirmation identifies momentum backed by real buying activity, not low-conviction price drift.

## Strategy

**Signal:** Stocks with positive 11-month price return (skip last month) AND 3-month average daily volume above their 12-month average.

- **Skip-last-month momentum**: Return from T-12 months to T-1 month (avoids short-term reversal)
- **Volume confirmation**: 3M avg volume > 12M avg volume (vol_ratio > 1.0)
- **Quality gate**: Positive net income AND operating cash flow (FY), market cap above exchange threshold

**Portfolio:** Top 30 stocks by momentum score, equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Semi-annual (January 1, July 1), 2001–2025.

## Academic Basis

Lee, C.M.C. & Swaminathan, B. (2000). *Price Momentum and Trading Volume.* Journal of Finance, 55(5), 2017–2069.

Key finding: High-volume momentum stocks sustain their outperformance significantly longer than low-volume momentum stocks. Volume acts as a signal of informed institutional participation — when price rises on above-average volume, the move is more likely to continue.

## Running the Backtest

```bash
# US stocks (default)
python3 volume-confirmed-momentum/backtest.py

# Single exchange
python3 volume-confirmed-momentum/backtest.py --preset india --verbose

# All exchanges
python3 volume-confirmed-momentum/backtest.py --global --output results/exchange_comparison.json --verbose

# Without transaction costs
python3 volume-confirmed-momentum/backtest.py --no-costs
```

## Live Screen

```bash
# Current qualifying stocks (US)
python3 volume-confirmed-momentum/screen.py

# India
python3 volume-confirmed-momentum/screen.py --preset india

# CSV output
python3 volume-confirmed-momentum/screen.py --preset us --csv
```

## Generate Charts

```bash
cd backtests
python3 volume-confirmed-momentum/generate_charts.py
# Charts saved to: backtests/volume-confirmed-momentum/charts/
# Move to: ts-content-creator/content/_current/momentum-08-volume-confirmed/blogs/{region}/
```

## Signal Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| Momentum lookback | 365 days | 12 months |
| Skip period | 30 days | Avoid short-term reversal |
| Volume lookback (3M) | 95 calendar days | ~63 trading days |
| Volume lookback (12M) | 400 calendar days | Full window fetch |
| Min volume days | 60 | Exclude thin-data symbols |
| Volume ratio threshold | > 1.0 | 3M avg must exceed 12M avg |
| Max portfolio size | 30 | Equal weight |
| Min portfolio size | 10 | Hold cash if below |
| Rebalancing | Semi-annual | Jan 1, Jul 1 |
| Transaction costs | Yes | Size-tiered model (costs.py) |
| Filing lag | 45 days | Point-in-time integrity |

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full historical backtest (2001–2025) |
| `screen.py` | Live stock screen using current TTM data |
| `generate_charts.py` | Chart generation from exchange_comparison.json |
| `results/exchange_comparison.json` | Backtest results (all exchanges) |
| `results/returns_{exchange}.json` | Per-exchange results (generated with --global) |

## Data Source

Financial data: FMP via Ceta Research warehouse.
Price and volume: `stock_eod` table (daily adjClose + volume).
Quality filter: `income_statement` and `cash_flow_statement` FY data.
Market cap: `key_metrics` FY data.

*Full methodology: [backtests/METHODOLOGY.md](../METHODOLOGY.md)*
