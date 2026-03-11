# Earnings Surprise (PEAD with Quintile Stratification)

Post-Earnings Announcement Drift (PEAD) event study. Stocks with large earnings surprises continue drifting in the surprise direction for weeks after the announcement. This strategy measures that drift with full quintile stratification.

## What is PEAD?

When a company reports earnings that beat (or miss) analyst estimates, the stock price doesn't fully adjust on the announcement day. Instead, it continues drifting in the surprise direction for 1–3 months. This is one of the most well-documented market anomalies in academic finance.

The key insight: bigger surprises produce larger and longer-lasting drift.

## Signal

```
surprise = (epsActual - epsEstimated) / ABS(epsEstimated)
```

- Positive values: earnings beat (epsActual > epsEstimated)
- Negative values: earnings miss
- Filter: ABS(epsEstimated) > $0.01 to avoid near-zero denominator distortions
- Cap: ABS(surprise) <= 500% to reduce extreme outlier noise

## Categories

**Direction-based:**
- `positive` — all earnings beats
- `negative` — all earnings misses

**Quintile-based (by surprise magnitude, globally across all events):**
- `Q1` — largest misses (bottom 20% by surprise)
- `Q2` — moderate misses
- `Q3` — near-zero surprises (around consensus)
- `Q4` — moderate beats
- `Q5` — largest beats (top 20% by surprise)

Monotonic Q1-through-Q5 drift is the core evidence for PEAD. Q5-Q1 spread at T+63 measures the strategy's economic significance.

## Event Windows

- T+1: next trading day after announcement (immediate reaction)
- T+5: 1 week post-earnings (short-term drift)
- T+21: 1 month post-earnings (medium-term drift)
- T+63: 3 months post-earnings (full drift window, pre-next-earnings)

## Benchmark

CAR (Cumulative Abnormal Return) = stock return minus benchmark return over the window.

- US: SPY (S&P 500)
- India: INDA (iShares MSCI India ETF)
- Japan: EWJ, Germany: EWG, UK: EWU, China: FXI, Korea: EWY, etc.

## Filters

- `ABS(epsEstimated) > 0.01` — avoids extreme ratios from near-zero estimates
- Market cap > exchange-specific threshold (see `cli_utils.py:MKTCAP_THRESHOLD_MAP`)
- Deduplicate: one event per symbol/date (FMP has duplicate records)
- Surprise cap: 500% maximum to reduce outlier noise

## How to Run

```bash
cd /path/to/backtests

# US (default)
python3 earnings-surprise/backtest.py

# US with output
python3 earnings-surprise/backtest.py --preset us \
    --output earnings-surprise/results/earnings_surprise_NYSE_NASDAQ_AMEX.json \
    --verbose

# Specific exchange
python3 earnings-surprise/backtest.py --preset india
python3 earnings-surprise/backtest.py --preset japan

# All exchanges
python3 earnings-surprise/backtest.py --global \
    --output earnings-surprise/results/exchange_comparison.json

# Generate charts (after running US backtest)
python3 earnings-surprise/generate_charts.py

# Live screen: recent large beats
python3 earnings-surprise/screen.py
python3 earnings-surprise/screen.py --direction negative --lookback 30
python3 earnings-surprise/screen.py --preset india
```

## Results Structure

Output JSON contains:
```json
{
  "car_metrics": {
    "overall": {"n_events": ..., "car_1d": {...}, "car_5d": {...}, ...},
    "positive": {"n_events": ..., "car_1d": {...}, ...},
    "negative": {"n_events": ..., "car_1d": {...}, ...},
    "Q1": {"n_events": ..., "mean_surprise_pct": ..., "car_1d": {...}, ...},
    "Q5": {"n_events": ..., "mean_surprise_pct": ..., "car_63d": {...}, ...}
  },
  "yearly_stats": [...]
}
```

Each CAR entry: `{mean, median, std, t_stat, n, hit_rate, significant_5pct, significant_1pct}`

## Academic References

- Ball, R. & Brown, P. (1968). "An Empirical Evaluation of Accounting Income Numbers." *Journal of Accounting Research*, 6(2), 159–178.
- Foster, G., Olsen, C. & Shevlin, T. (1984). "Earnings Releases, Anomalies, and the Behavior of Security Returns." *The Accounting Review*, 59(4), 574–603.
- Bernard, V.L. & Thomas, J.K. (1989). "Post-Earnings-Announcement Drift: Delayed Price Response or Risk Premium?" *Journal of Accounting Research*, 27, 1–36.
- Bernard, V.L. & Thomas, J.K. (1990). "Evidence That Stock Prices Do Not Fully Reflect the Implications of Current Earnings for Future Earnings." *Journal of Accounting and Economics*, 13(4), 305–340.

## Data Source

FMP (Financial Modeling Prep) via Ceta Research warehouse:
- `earnings_surprises` table: `epsActual`, `epsEstimated`, `date`
- `stock_eod` table: `adjClose` (split-adjusted prices)
- `key_metrics` FY: `marketCap` for universe filtering
- `profile`: exchange filter

Note: FMP `earnings_surprises` contains duplicate records per symbol/date. The backtest deduplicates using `ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY epsActual DESC) = 1`.
