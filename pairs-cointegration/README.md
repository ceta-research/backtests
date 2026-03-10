# Pairs Trading Cointegration Testing

**Part of the [Pairs Trading Masterclass](https://blog.tradingstudio.finance/pairs-trading-fundamentals) series.**

## What It Does

Tests statistical cointegration on the 2,579 candidate pairs from the pairs-screening step.
A highly correlated pair is not necessarily a good trading pair. Cointegration tests whether
the price spread is stationary, meaning it reverts to its mean over time. That reversion is
what makes a pair tradeable.

**Pipeline:**
- Input: candidate_pairs.csv (2,579 pairs, from pairs-screening)
- Method: Engle-Granger two-step cointegration test (OLS + ADF on residual spread)
- Filters: ADF p-value < 0.05 AND half-life between 5 and 120 trading days
- Output: cointegrated_pairs.csv with full stats for each passing pair

## Key Results (Feb 2026 production run)

| Metric | Value |
|--------|-------|
| Candidates tested | 2,579 |
| Passed cointegration | 516 (20.0%) |
| Avg half-life | 16.6 days |
| Median half-life | 16.7 days |
| Half-life range | 5.1 - 59.8 days |
| Top sector (pass rate) | Utilities 31.4% (11/35) |
| Financial Services | 453/2,249 = 20.1% |
| Lookback window | 252 trading days (2024-01-09 to 2026-02-02) |

Only 1 in 5 highly correlated pairs passes cointegration. Correlation alone is not enough.

## Usage

```bash
# Run with default paths (reads from pairs-screening output)
python3 pairs-cointegration/backtest.py

# Specify custom input/output
python3 pairs-cointegration/backtest.py \
    --input path/to/candidate_pairs.csv \
    --output path/to/cointegrated_pairs.csv

# Verbose output (shows per-pair stats)
python3 pairs-cointegration/backtest.py --verbose

# Custom lookback and overlap requirements
python3 pairs-cointegration/backtest.py \
    --lookback-date 2023-01-01 \
    --min-days 200

# Check current z-scores (top 20 pairs by default)
python3 pairs-cointegration/screen.py

# Screen a specific sector, lower threshold
python3 pairs-cointegration/screen.py --sector Energy --top 30 --min-zscore 1.0

# Generate charts from existing results
python3 pairs-cointegration/generate_charts.py
```

## Files

```
pairs-cointegration/
  backtest.py         # Cointegration analysis pipeline (main script)
  screen.py           # Current z-score screen for top pairs
  generate_charts.py  # Creates pass rate and half-life distribution charts
  charts/             # Chart output (gitignored, move to content dir)
  results/            # Output directory (gitignored)
  README.md           # This file
```

**Output CSV columns (cointegrated_pairs.csv):**

| Column | Description |
|--------|-------------|
| symbol_a, symbol_b | Pair identifiers |
| sector | Shared sector |
| hedge_ratio | OLS beta (P_A = beta * P_B + intercept) |
| intercept | OLS intercept |
| adf_stat | ADF test statistic (more negative = more stationary) |
| adf_pvalue | ADF p-value (< 0.05 required) |
| r_squared | OLS R-squared of price regression |
| half_life_days | Mean reversion half-life in trading days |
| ar1_beta | AR(1) coefficient of spread (< 1 = mean-reverting) |
| spread_mean, spread_std | Spread distribution parameters |
| spread_skew, spread_kurt | Spread higher moments |
| n_observations | Number of aligned trading days |
| date_start, date_end | Date range of analysis |

## Methodology

**Step 1: OLS Regression (Price Levels)**

For each candidate pair (A, B), fit a linear regression on price levels:

```
P_A(t) = beta * P_B(t) + intercept + epsilon(t)
```

This estimates the hedge ratio (beta). Using price levels rather than log-prices
gives a better stationary spread for the ADF test when prices are integrated of
order 1 (I(1)), which equities typically are.

**Step 2: Spread Construction**

```
spread(t) = P_A(t) - beta * P_B(t)
```

**Step 3: ADF Test (Augmented Dickey-Fuller)**

Tests the null hypothesis that the spread has a unit root (non-stationary).
Rejection of the null (p < 0.05) indicates the spread is stationary. That stationarity
is the cointegration condition.

Parameters: maxlag=20, autolag='AIC' (AIC selects optimal lag count automatically).

**Step 4: Half-Life Estimation**

Fits an AR(1) model on spread differences to measure the speed of mean reversion:

```
delta_spread(t) = alpha + beta_1 * spread(t-1) + epsilon(t)
half_life = -log(2) / log(1 + beta_1)
```

Half-life is the expected time for the spread to revert halfway to its mean.
Too short (< 5 days) and transaction costs kill the edge. Too long (> 120 days)
and the capital is tied up for too long.

**Why OLS on price levels (not log-prices)?**

Pairs cointegration is typically tested on price levels because both stocks are I(1)
(random walks). Taking logs is appropriate for single-stock return modeling, but the
OLS cointegration test is designed to find a linear combination of I(1) variables
that is I(0). The hedge ratio from log-prices regression has a different economic
interpretation and can produce a spread that does not pass the ADF test even when
the pair is genuinely cointegrated.

## Data Source

Data: Ceta Research (FMP financial data warehouse)
- `stock_eod` table: adjusted close prices
- `profile` table: sector classification (from pairs-screening output)

Pairs-screening output (candidate_pairs.csv) provides the sector and symbol metadata.

## Academic References

- Engle, R. & Granger, C. (1987). "Co-integration and Error Correction: Representation, Estimation, and Testing." *Econometrica*, 55(2), 251-276.
- Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading: Performance of a Relative-Value Arbitrage Rule." *Review of Financial Studies*, 19(3), 797-827.
- Vidyamurthy, G. (2004). *Pairs Trading: Quantitative Methods and Analysis*. Wiley.
- Krauss, C. (2017). "Statistical Arbitrage Pairs Trading Strategies: Review and Outlook." *Journal of Economic Surveys*, 31(2), 513-545.

## Part of the Series

1. [Pairs Trading Fundamentals](../pairs-fundamentals/) — Theory and academic background
2. [Candidate Screening](../pairs-screening/) — Correlation-based pair filtering
3. **Cointegration Testing** (you are here) — Statistical validation
4. [Z-Score Signals](../pairs-zscore/) — Entry/exit logic
5. [Backtest Results](../pairs-backtest/) — Does it work? (Spoiler: barely)
6. [Multi-Pair Portfolio](../pairs-multi-pair/) — Diversification across pairs
