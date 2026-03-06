# Dogs of the Dow

Buy the 10 highest-yielding Dow stocks every January. Hold for a year. Repeat.

For non-US exchanges, the strategy generalizes: take the top 30 stocks by market cap (blue chips), pick the 10 highest-yielding. Same logic, different universe.

## Strategy

| Parameter | Value |
|-----------|-------|
| Universe | Dow 30 (US) / Top 30 by market cap (other exchanges) |
| Signal | Highest trailing dividend yield |
| Portfolio | Top 10 by yield, equal weight |
| Rebalancing | Annual (January) |
| Min stocks | 5 (hold cash if fewer qualify) |
| Transaction costs | Size-tiered (0.1% for >$10B, 0.3% for $2-10B, 0.5% for <$2B) |
| Benchmark | SPY (S&P 500 Total Return) |

## Usage

```bash
# Backtest US (true Dogs of the Dow using Dow 30)
python3 dogs-of-dow/backtest.py --verbose

# Backtest India
python3 dogs-of-dow/backtest.py --preset india --output dogs-of-dow/results/returns_India.json --verbose

# Current screen
python3 dogs-of-dow/screen.py
python3 dogs-of-dow/screen.py --preset india

# Run all exchanges
python3 dogs-of-dow/run_all_exchanges.py --verbose

# Generate charts (after backtests)
python3 dogs-of-dow/generate_charts.py
```

## Academic Reference

Michael O'Higgins, "Beating the Dow" (1991). The strategy assumes mean reversion within blue-chip stocks: high yield signals temporary underperformance, and blue chips tend to recover.

## Data

- FMP financial data warehouse via Ceta Research API
- `dowjones_constituent` for Dow 30 membership (US)
- `financial_ratios` (FY) for historical dividend yield
- `key_metrics` (FY) for market cap (blue-chip selection on non-US)
- `stock_eod` for adjusted close prices

*Data: Ceta Research (FMP financial data warehouse).*
