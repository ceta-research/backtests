# FCF Compounders

Backtest for companies that consistently grow free cash flow year after year.

## Strategy

**Signal:** FCF grew in >= 4 of last 5 FY years, all FCF positive, ROIC > 15%, Operating Margin > 15%

**Portfolio:** Top 30 by highest ROIC, equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Annual (July), 45-day filing lag for point-in-time data.

**Period:** 2000-2025.

## Academic Reference

Mohanram, P. (2005) "Separating Winners from Losers among Low Book-to-Market Stocks using Financial Statement Analysis", *Review of Accounting Studies* 10(2-3), 133-170. Combining growth signals with quality metrics creates a powerful predictor of future returns.

## Results (US, 2000-2025)

| Metric | FCF Compounders | S&P 500 |
|--------|-----------------|---------|
| CAGR | 11.70% | 7.83% |
| Max Drawdown | -27.87% | -36.27% |
| Sharpe Ratio | 0.586 | 0.360 |
| Sortino Ratio | 1.398 | 0.654 |
| Excess CAGR | +3.86% | -- |
| Win Rate | 64% | -- |
| Down Capture | 66.7% | -- |

## Usage

```bash
# US stocks (default)
python3 fcf-compounders/backtest.py

# German stocks
python3 fcf-compounders/backtest.py --preset germany

# All exchanges
python3 fcf-compounders/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen
python3 fcf-compounders/screen.py
python3 fcf-compounders/screen.py --preset india

# Generate charts
python3 fcf-compounders/generate_charts.py
```

## Data

*Data: Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)*
