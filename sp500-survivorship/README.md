# S&P 500 Survivorship Bias Analysis

Measures how much survivorship bias inflates backtest returns. Compares two identical low P/E screens run against different universes: today's S&P 500 members (biased) vs the actual S&P 500 membership at each historical rebalance date (unbiased, point-in-time).

**Academic basis:** Elton, Gruber & Blake (1996), Garcia & Norli (2001), Rohleder, Scholz & Wilkens (2011)

## Signal

Same screen applied to both universes:

| Filter | Threshold | Purpose |
|--------|-----------|---------|
| P/E Ratio | 0 < P/E < 15 | Classic value: cheap relative to earnings |
| Selection | Top 100 by lowest P/E | Concentrated value portfolio |
| Market Cap | S&P 500 members | Large-cap universe |

**Portfolio construction:** Top 100 stocks by lowest P/E. Equal weight. Quarterly rebalance (Jan/Apr/Jul/Oct).

**Two universes tested:**
- **Biased (current S&P 500):** Screens today's ~500 members across all historical dates. This is what most retail screeners and many academic papers do.
- **Unbiased (point-in-time):** Reconstructs S&P 500 membership at each rebalance date using historical constituent change data. Includes companies that were later removed (bankruptcies, mergers, demotions).

## Results

| Metric | Biased (Current) | Unbiased (PIT) | SPY | Bias Gap |
|--------|-------------------|-----------------|-----|----------|
| CAGR | 13.83% | 11.81% | 8.01% | +2.02% |
| Sharpe | 0.609 | 0.465 | 0.354 | +0.144 |
| Sortino | 0.941 | 0.701 | 0.523 | +0.240 |
| Max Drawdown | -44.0% | -51.8% | -45.5% | +7.8pp |
| Volatility | 19.43% | 21.11% | 16.97% | -1.68pp |

Survivorship bias inflated CAGR by 2.02 percentage points and Sharpe by 0.144. The biased portfolio also showed shallower drawdowns (-44.0% vs -51.8%) because it excluded companies that experienced the worst declines before being removed from the index.

**Portfolio characteristics:** Average 92 stocks in both portfolios. Average 665 point-in-time S&P 500 members (vs 500 current). Average 25.6 "survivorship victims" per period (stocks in unbiased screen but not biased).

## Usage

```bash
# Set your API key
export CR_API_KEY="your_key"

# Run the full backtest (2000-2025, ~5 min)
python3 sp500-survivorship/backtest.py

# Run with verbose output
python3 sp500-survivorship/backtest.py --verbose

# Save results to JSON
python3 sp500-survivorship/backtest.py --output results/summary.json

# Run without transaction costs
python3 sp500-survivorship/backtest.py --no-costs

# Screen current stocks (biased vs unbiased comparison)
python3 sp500-survivorship/screen.py

# Generate charts from saved results
python3 sp500-survivorship/generate_charts.py
```

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Historical backtest (2000-2025). Reconstructs S&P 500 membership at each date using constituent change data. |
| `screen.py` | Live screen comparing biased vs unbiased universes using current TTM data. |
| `generate_charts.py` | Generates cumulative growth and annual returns charts from `results/summary.json`. |
| `results/` | Pre-computed backtest results (summary JSON with quarterly returns). |
| `charts/` | Generated charts (PNG, 200 DPI). |

## How the backtest works

1. **Fetch** constituent changes from `historical_sp500_constituent` and current members from `sp500_constituent`
2. **Reconstruct** S&P 500 membership at each quarterly rebalance date by replaying add/remove events
3. **Screen** both universes (current members vs point-in-time members) with the same low P/E filter
4. **Compute** equal-weight returns for each portfolio, net of size-tiered transaction costs
5. **Compare** cumulative returns, risk metrics, and the gap attributable to survivorship bias

## Why it matters

Most backtesting platforms screen today's index members across historical dates. This introduces survivorship bias: companies that went bankrupt, were acquired, or were demoted from the index are excluded from the historical universe. The biased approach only holds "winners" -- stocks that survived to the present day.

This backtest quantifies that bias at +2.02% CAGR. Any S&P 500-based backtest that doesn't use point-in-time membership data is overstating returns by roughly this amount.

## Data Source

Scripts use the [Ceta Research](https://cetaresearch.com) SQL API. Tables used:
- `sp500_constituent` -- current S&P 500 members
- `historical_sp500_constituent` -- all historical add/remove events
- `financial_ratios` -- P/E ratios (FY)
- `key_metrics` -- market cap (FY)
- `stock_eod` -- adjusted close prices

Configure the data source by setting `CR_API_KEY` or passing `--api-key`.

## References

- Elton, E. J., Gruber, M. J., & Blake, C. R. (1996). "Survivorship Bias and Mutual Fund Performance." *Review of Financial Studies*
- Garcia, C. B., & Norli, O. (2001). "The Impact of Survivorship Bias on S&P 500 Index Returns." Working Paper
- Rohleder, M., Scholz, H., & Wilkens, M. (2011). "Survivorship Bias and Mutual Fund Performance: Relevance, Significance, and Methodical Differences." *Review of Finance*

See [METHODOLOGY.md](../METHODOLOGY.md) for complete backtesting methodology.
