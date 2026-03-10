# Pairs Trading Candidate Screen

**Part of the [Pairs Trading Masterclass](https://blog.tradingstudio.finance/pairs-trading-fundamentals) series.**

## What It Does

Screens US large-cap stocks for pairs trading candidates using pairwise return correlation. Reduces ~4 million possible pairs to a focused set worth testing for cointegration.

**Strategy:**
- Universe: US stocks with market cap > $1B (3,700+ stocks)
- Method: Pairwise Pearson correlation of daily returns, 252-day rolling window
- Filters: Same sector, correlation ≥ 0.80, market cap ratio < 5x, ≥ 252 overlapping trading days
- Output: Candidate pairs CSV with correlation, sector, industry, market cap metadata

## Key Results (Feb 2026 production run)

| Metric | Value |
|--------|-------|
| Universe | 3,701 US stocks > $1B |
| Same-sector pairs tested | ~884,000 |
| Candidate pairs (corr ≥ 0.80) | 2,579 |
| Same-industry pairs | 2,359 (91.5%) |
| Top sector | Financial Services (2,249 pairs, 87%) |
| Average correlation | 0.833 |

The Financial Services dominance is structural: all banks share interest rate sensitivity, creating sector-wide co-movement even among very different business models.

## Usage

```bash
# Show current universe by sector (fast, no correlation computation)
python3 pairs-screening/screen.py --universe

# Screen a single sector (1-2 min)
python3 pairs-screening/screen.py --sector Energy

# Screen all sectors and save results (10-15 min)
python3 pairs-screening/screen.py --global --output results/candidate_pairs.csv

# Generate charts from existing results
python3 pairs-screening/generate_charts.py
```

## Files

```
pairs-screening/
  screen.py           # Runs the pairwise correlation screening
  generate_charts.py  # Creates sector distribution and correlation charts
  results/            # Output directory (gitignored)
  charts/             # Chart output (gitignored, move to content dir)
  README.md           # This file
```

## Methodology

**Why same-sector only?** Cross-sector pairs with high correlation typically lack an economic anchor. XOM/CVX move together because they share commodity exposure. Cross-sector correlations are usually coincidental.

**Why 0.80 threshold?** Academic standard (Gatev et al. 2006). Lower thresholds (0.60) produce too many weak candidates. Higher (0.90) misses many valid pairs.

**Why market cap ratio < 5x?** Asymmetric market caps create asymmetric risk. A $200B stock paired with a $2B stock has very different liquidity profiles, making execution and position sizing difficult.

**Share-class pairs:** Pairs with correlation ≈ 1.000 are often share-class variants (e.g., GOOG/GOOGL) or corporate restructuring artifacts. These should be identified and filtered before cointegration testing.

## Data Source

Data: Ceta Research (FMP financial data warehouse)
- `profile` table: sector, industry, country
- `key_metrics` table: market cap (FY historical)
- `stock_eod` table: adjusted close prices

## Academic References

- Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading: Performance of a Relative-Value Arbitrage Rule." *Review of Financial Studies*, 19(3), 797–827.
- Krauss, C. (2017). "Statistical Arbitrage Pairs Trading Strategies: Review and Outlook." *Journal of Economic Surveys*, 31(2), 513–545.
- Vidyamurthy, G. (2004). *Pairs Trading: Quantitative Methods and Analysis*. Wiley.

## Part of the Series

1. [Pairs Trading Fundamentals](../pairs-fundamentals/) — Theory and academic background
2. **Candidate Screening** ← You are here
3. [Cointegration Testing](../pairs-cointegration/) — Statistical validation
4. [Z-Score Signals](../pairs-zscore/) — Entry/exit logic
5. [Backtest Results](../pairs-backtest/) — Does it work? (Spoiler: barely)
6. [Multi-Pair Portfolio](../pairs-multi-pair/) — Diversification across pairs
