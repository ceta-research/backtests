# Post-Earnings Announcement Drift (PEAD)

Event study measuring abnormal stock returns after earnings surprises.

## The Anomaly

When companies beat or miss analyst earnings estimates, the stock price adjusts at the announcement but continues drifting in the same direction for weeks. Positive surprises keep climbing. Negative surprises keep falling. The market under-reacts to earnings news.

Ball & Brown documented this in 1968. Bernard & Thomas confirmed it in 1989-1990. It remains one of the most replicated findings in finance.

## Signal

```
Surprise = (epsActual - epsEstimated) / |epsEstimated|
```

- **Positive surprise (beat):** actual EPS > estimated EPS
- **Negative surprise (miss):** actual EPS < estimated EPS
- **Filter:** |epsEstimated| > $0.01 (avoids extreme ratios from near-zero estimates)
- **Market cap:** > $500M (historical FY, not TTM)
- **Surprise cap:** |surprise| < 1000% (reduces outlier noise)

## Event Study Design

- **Event date:** Earnings announcement date
- **Windows:** T+1, T+5, T+21, T+63 trading days post-announcement
- **Abnormal return:** Stock return minus benchmark return over the same window
- **Benchmark:** SPY (US) or regional ETF (international)
- **Metric:** CAR (Cumulative Abnormal Return) with t-statistics
- **Stratification:** By surprise quintile (Q1=worst misses, Q5=biggest beats)

## Usage

```bash
# US event study (default)
python3 pead/backtest.py

# Specific exchange
python3 pead/backtest.py --preset india

# All exchanges
python3 pead/backtest.py --global --output results/exchange_comparison.json --verbose

# Current earnings surprise screen
python3 pead/screen.py
python3 pead/screen.py --direction negative
python3 pead/screen.py --preset india --min-mcap 10000000000
```

## Key Metrics

This is an **event study**, not a portfolio backtest. The key metrics are:

| Metric | Description |
|--------|-------------|
| Mean CAR | Average cumulative abnormal return at each window |
| Median CAR | Median (more robust to outliers) |
| t-statistic | Statistical significance (>1.96 = significant at 95%) |
| Hit rate | % of events with positive abnormal return |
| Q5-Q1 spread | Difference between top and bottom surprise quintiles |

## Data Source

- **Earnings surprises:** FMP `earnings_surprises` table (epsActual, epsEstimated)
- **Prices:** FMP `stock_eod` (adjClose)
- **Market cap:** FMP `key_metrics` (FY period, historical)
- **Exchange membership:** FMP `profile`

## Academic References

- Ball, R. & Brown, P. (1968). "An Empirical Evaluation of Accounting Income Numbers." *Journal of Accounting Research*, 6(2), 159-178.
- Bernard, V. & Thomas, J. (1989). "Post-Earnings-Announcement Drift: Delayed Price Response or Risk Premium?" *Journal of Accounting Research*, 27, 1-36.
- Bernard, V. & Thomas, J. (1990). "Evidence that Stock Prices Do Not Fully Reflect the Implications of Current Earnings for Future Earnings." *Journal of Accounting and Economics*, 13(4), 305-340.

*Data: Ceta Research (FMP financial data warehouse).*
