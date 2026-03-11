# 12-Month Price Momentum

Pure price momentum strategy. Buys the top 30 stocks by 12-month return within
each exchange, skipping the most recent month to avoid short-term reversal.

No financial quality filters. This is the canonical "12-1 momentum" signal from
Jegadeesh & Titman (1993).

## Signal

1. Universe: Market cap > exchange threshold (point-in-time, 45-day filing lag)
2. Momentum: Compute 12M-1M return for each stock (price 12M ago → price 1M ago)
3. Portfolio: Top 30 by 12M-1M return, equal weight. Cash if < 10 qualify.

The 1-month skip prevents short-term reversal from contaminating the signal.
Stocks that were last month's best performers often reverse, and excluding that
noise improves forward returns.

## Parameters

| Parameter | Value |
|-----------|-------|
| Lookback | 12 months - 1 month (12M-1M) |
| Portfolio size | Top 30 by 12M-1M return |
| Min to invest | 10 stocks |
| Rebalancing | Semi-annual (January, July) |
| Universe filter | Market cap > exchange threshold |
| Transaction costs | Yes (size-tiered, see costs.py) |
| Excluded | ASX, SAO (adjClose split artifacts) |

## Academic Basis

Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and Selling
Losers: Implications for Stock Market Efficiency." Journal of Finance, 48(1),
65-91.

The paper found that buying stocks in the top decile of 6-12 month returns and
holding for 3-12 months generates significant abnormal returns. The "12-1"
variant (skip last month) became the standard implementation.

## Difference from Quality Momentum

The `quality-momentum` strategy in this repo combines quality filters (ROE,
D/E, gross margin) with momentum. This strategy uses **no financial filters** —
pure price signal only. This tests whether momentum alone works, without
pre-screening for quality.

## Usage

```bash
# US (default)
python3 price-momentum/backtest.py

# Single exchange
python3 price-momentum/backtest.py --preset india
python3 price-momentum/backtest.py --exchange XETRA

# All exchanges (takes ~30-60 minutes)
python3 price-momentum/backtest.py --global --output results/exchange_comparison.json

# No transaction costs (academic comparison)
python3 price-momentum/backtest.py --preset us --no-costs --verbose

# Live screen (current momentum leaders)
python3 price-momentum/screen.py --preset us
python3 price-momentum/screen.py --preset india --top 50
```

## Data

- **key_metrics**: Historical annual market cap (point-in-time, 45-day lag)
- **stock_eod**: Daily adjusted close prices (full 12M+ window)
- **profile**: Exchange membership

Data via Ceta Research API (FMP financial data warehouse).

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025, all exchanges) |
| `screen.py` | Live momentum screen (current top stocks) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/` | Backtest output files (exchange_comparison.json, per-exchange CSVs) |
| `charts/` | Generated PNG charts (gitignored, move to ts-content-creator) |
