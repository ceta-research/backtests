# DuPont ROE Decomposition

Screen and backtest for stocks with high ROE driven by profitability, not leverage.

## The Idea

ROE = Net Profit Margin x Asset Turnover x Equity Multiplier

Two companies can both have 20% ROE through completely different paths. One earns it from fat margins on a capital-light business. The other earns it from 3% margins and 5x leverage. Same headline number, opposite risk profiles.

Academic basis: Soliman (2008) showed that margin-driven ROE predicts future returns better than leverage-driven ROE. Fairfield & Yohn (2001) showed that profit margin changes are more persistent than asset turnover changes.

## Signal

**Quality ROE portfolio** (primary):
- ROE > 15%
- Net profit margin > 8%
- Equity multiplier < 3.0 (moderate leverage)
- Exclude Financial Services and Utilities

**Comparison portfolios**:
- Margin-Driven: Top quartile net margin within ROE > 15%
- Leverage-Driven: Top quartile equity multiplier within ROE > 15%
- All High ROE: Everything with ROE > 15%

## Parameters

| Parameter | Value |
|-----------|-------|
| Rebalancing | Annual (April 1) |
| Filing lag | 45 days |
| Weighting | Equal weight |
| Transaction costs | Size-tiered (0.1-0.5% per trade) |
| Market cap | Exchange-specific thresholds |
| Benchmark | S&P 500 (SPY) |

## Data Tables

| Table | Columns | Usage |
|-------|---------|-------|
| income_statement (FY) | netIncome, revenue | Net profit margin |
| balance_sheet (FY) | totalAssets, totalStockholdersEquity | Asset turnover, equity multiplier |
| key_metrics (FY) | marketCap | Size filter |
| profile | exchange, sector | Universe, sector exclusion |
| stock_eod | adjClose | Returns |

## Usage

```bash
# US backtest
python3 roe-dupont/backtest.py --verbose

# India
python3 roe-dupont/backtest.py --preset india --verbose

# All exchanges
python3 roe-dupont/backtest.py --global --output roe-dupont/results/exchange_comparison.json

# Current screen
python3 roe-dupont/screen.py
python3 roe-dupont/screen.py --preset india
python3 roe-dupont/screen.py --simple  # ROE > 15% only, no quality filter

# Charts
python3 roe-dupont/generate_charts.py
```

## References

- Soliman, M.T. (2008). "The Use of DuPont Analysis by Market Participants." *The Accounting Review*, 83(3), 823-853.
- Fairfield, P.M. and Yohn, T.L. (2001). "Using Asset Turnover and Profit Margin to Forecast Changes in Profitability." *Review of Accounting Studies*, 6(4), 371-385.
