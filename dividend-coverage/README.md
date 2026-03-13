# Dividend Coverage Backtest

Screen stocks by free cash flow coverage of dividend payments. Companies with FCF between 1.5x and 20x their dividends, yield above 2%, rebalanced annually.

## Signal

```
FCF Coverage = Free Cash Flow / ABS(Common Dividends Paid)
```

| Filter | Threshold |
|--------|-----------|
| FCF Coverage | 1.5x to 20x |
| Dividend Yield | > 2% |
| Market Cap | > exchange threshold |

Portfolio: Top 30 by coverage descending, equal weight. Annual rebalance (July).

## Results (2000-2025)

| Exchange | CAGR | Excess vs SPY | Sharpe | Max Drawdown |
|----------|------|---------------|--------|-------------|
| India (BSE+NSE) | 17.90% | +10.07% | 0.420 | -20.11% |
| Germany (XETRA) | 12.43% | +4.60% | 0.486 | -41.37% |
| UK (LSE) | 11.52% | +3.69% | 0.409 | -23.62% |
| US (NYSE+NAS+AMEX) | 11.19% | +3.36% | 0.407 | -41.57% |
| Sweden (STO) | 10.94% | +3.11% | 0.490 | -36.05% |
| Canada (TSX) | 10.50% | +2.67% | 0.478 | -25.03% |
| Australia (ASX) | 9.45% | +1.61% | 0.371 | -40.95% |
| Japan (JPX) | 8.12% | +0.29% | 0.353 | -48.05% |

8 of 13 tested exchanges beat the S&P 500 benchmark.

## Usage

```bash
# Run on US stocks
python3 dividend-coverage/backtest.py --preset us --output results.json --verbose

# Run on all exchanges
python3 dividend-coverage/backtest.py --global --output results/exchange_comparison.json

# Current stock screen
python3 dividend-coverage/screen.py --preset us

# Generate charts
python3 dividend-coverage/generate_charts.py
```

## Academic Reference

Benartzi, S., Michaely, R. & Thaler, R. (1997). "Do Changes in Dividends Signal the Future or the Past?" *Journal of Finance*, 52(3), 1007-1034.

## Data

Ceta Research (FMP financial data warehouse). Point-in-time annual financial statements with 45-day lag.
