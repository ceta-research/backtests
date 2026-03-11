# Z-Score Pairs Trading: Signal Quality Backtest

Active daily z-score monitoring on mean-reverting stock pairs. Annual pair
formation (correlation + half-life filter), then daily z-score signal generation
and trade management during the trading year.

## Signal

| Parameter | Value |
|-----------|-------|
| Universe | Top 30 stocks/sector by market cap, same exchange |
| Pair formation | Same sector, 252-day returns correlation > 0.70, half-life 5-60 days |
| Max pairs | Top 20 pairs per year |
| Hedge ratio | OLS from log-prices during formation year (fixed for trading year) |
| Z-score lookback | 40-day rolling mean/std |
| Entry | \|z\| > 2.0 (not already in position for this pair) |
| Exit (convergence) | \|z\| < 0.5 |
| Exit (time stop) | Holding > 60 trading days |
| Exit (loss stop) | Pair P&L < -5% |
| Portfolio | Equal-dollar weight across all active trades |
| Cash condition | < 3 pairs with any trades during the year |
| Rebalancing | Annual pair renewal (formation in year Y-1, trading in year Y) |
| Costs | 4 one-way legs per trade (open + close × 2 stocks) |

**Reference:** Gatev, Goetzmann & Rouwenhorst (2006), *Review of Financial Studies*

## Key Finding

The strategy generates ~130 trades/year with **80-87% convergence rate** (spread
returns to the mean on most trades). Despite this, it loses money on every exchange
tested. The average trade return is -0.05% to -0.45% per trade after costs. Convergence
rate is not a reliable proxy for profitability.

**Why convergence doesn't equal profit:**
- Mean reversion gains are small (the spread only needs to reach \|z\| < 0.5 from \|z\| > 2.0)
- Transaction costs (4 legs × ~0.1% each = 0.4%) eat most of the spread gain
- 13-23% of trades exit via time stop or loss stop, locking in losses
- The strategy has near-zero market exposure (beta ≈ 0), meaning it earns no equity premium

## Results Summary (2005-2024, clean exchanges only)

| Exchange | CAGR | vs SPY | Sharpe | MaxDD | Cash% | Conv% | AvgTrade |
|----------|------|--------|--------|-------|-------|-------|----------|
| TAI+TWO (Taiwan) | -0.09% | -9.91% | -0.173 | -23.52% | 20% | 80.4% | +0.107% |
| HKSE (Hong Kong) | -0.88% | -10.69% | -1.913 | -17.31% | 5% | 82.6% | -0.143% |
| JPX (Japan) | -0.92% | -10.73% | -0.623 | -17.43% | 0% | 85.4% | -0.139% |
| LSE (UK) | -0.92% | -10.74% | -1.224 | -24.49% | 5% | 81.4% | -0.053% |
| **NYSE+NASDAQ+AMEX (US)** | **-1.22%** | **-11.03%** | **-2.750** | **-21.81%** | **0%** | **86.6%** | **-0.181%** |
| XETRA (Germany) | -1.38% | -11.19% | -0.951 | -24.31% | 40% | 80.9% | -0.261% |
| TSX (Canada) | -2.84% | -12.65% | -1.714 | -44.26% | 5% | 77.7% | -0.454% |

SPY benchmark: 9.81% CAGR (2005-2024).

**Excluded from content (data quality):**
- JNB (South Africa): 2006 +331%, 2011-2013 100-300% — multiple implausible years
- BSE+NSE (India): 2005 +50%, 2006 +111% — FMP data warmup (sparse 2004 coverage)
- KSC (Korea): 2005 +66%, 2008 +56% — unreliable beta estimates in thin data years
- STO (Sweden): 2005 +54% — formation warmup issue
- SHZ+SHH (China): 2005 +54% — formation warmup issue

The excluded exchanges show extreme single-year returns that are implausible for a
market-neutral strategy. Root cause: FMP data coverage for 2004 is sparse on these
exchanges, leading to unstable beta estimates in the first trading year (2005).

## Usage

```bash
export CR_API_KEY="your_key_here"

# Screen current z-score signals (US)
python3 pairs-zscore/screen.py

# Screen Japanese pairs
python3 pairs-zscore/screen.py --preset japan

# Run full backtest on US
python3 pairs-zscore/backtest.py

# Run on all exchanges, save results
python3 pairs-zscore/backtest.py --global --output results/exchange_comparison.json

# Run without transaction costs (to see raw convergence gains)
python3 pairs-zscore/backtest.py --preset us --no-costs

# Regenerate charts from existing results
python3 pairs-zscore/generate_charts.py
```

## Files

- `backtest.py` — Full historical backtest with active z-score signal tracking
- `screen.py` — Current z-score signals screen (live data)
- `generate_charts.py` — Charts from `results/exchange_comparison.json`
- `results/` — Pre-computed results (exchange_comparison.json, per-exchange JSON)
- `charts/` — PNG charts (gitignored, generated locally)

## How it works

1. **Fetch** (20-60s): Pull daily prices 2004-present for top 30 large-cap stocks per
   sector, plus SPY.
2. **Cache**: All price data loaded into in-memory DuckDB.
3. **Annual loop** (2005-2024):
   - **Formation** (prior year): Find top-N same-sector pairs by correlation > 0.70.
     For each pair, estimate OLS beta (log-prices) and half-life. Keep pairs with
     half-life 5-60 days (mean-reverting, not just noisy). Select top 20.
   - **Trading** (current year): For each pair, compute daily 40-day rolling z-scores.
     Enter when \|z\| > 2.0 (one position per pair at a time). Exit via convergence
     (\|z\| < 0.5), time stop (60 days), or loss stop (-5%).
   - **Annual return**: Sum of trade returns per pair, divided by total pairs formed.
     Cash year if < 3 pairs have any trades.
4. **Metrics**: Standard metrics via `metrics.py`. Trade statistics aggregated across
   all years: convergence rate, time stop rate, loss stop rate, avg holding period.

## Data notes

- **Short-selling**: Results assume short-selling is possible. Results for China and
  India (if run) are theoretical — short-selling is restricted in practice.
- **Survivorship bias**: Uses currently active stocks (isActivelyTrading = true). Stocks
  that went bankrupt or were acquired are excluded, slightly biasing results upward.
- **Beta stability**: Annual OLS beta becomes stale during regime changes. Rolling beta
  (60-day window) would adapt better but introduces estimation noise. Fixed annual beta
  is used here for simplicity and to avoid look-ahead bias.
- **Data warmup**: Formation requires full 2004 price data. Exchanges where FMP has
  sparse 2004 coverage (India, Korea, China, Sweden, South Africa) produce unstable
  beta estimates in 2005, causing extreme returns. These exchanges are excluded from
  content but run in `--global` mode.
