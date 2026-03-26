# Income Quality (OCF / Net Income)

Backtest the income quality signal: Operating Cash Flow divided by Net Income. Based on Sloan (1996), which showed that stocks with high accruals (earnings not backed by cash) underperform by ~10% annually.

## Signal

- **Income Quality** = Operating Cash Flow / Net Income (FMP `incomeQuality`)
- **High quality**: IQ > 1.2 (cash-backed earnings)
- **Medium quality**: 0.5 to 1.2
- **Low quality**: IQ < 0.5 (accrual-heavy)
- **Filter**: Net Income > 0 (excludes negative earners to avoid misleading ratios)

## Setup

```bash
# From backtests/ directory
python3 income-quality/backtest.py                          # US default
python3 income-quality/backtest.py --preset india --verbose  # India
python3 income-quality/backtest.py --global --output income-quality/results/exchange_comparison.json

# Current screen
python3 income-quality/screen.py
python3 income-quality/screen.py --preset india

# Generate charts (after global run)
python3 income-quality/generate_charts.py
```

## Methodology

- **Rebalancing**: Annual (April 1), 45-day filing lag for point-in-time data
- **Universe**: Full exchange (not index-constrained), market cap thresholds per exchange
- **Costs**: Size-tiered (0.1% large-cap, 0.3% mid, 0.5% small)
- **Benchmark**: S&P 500 (SPY)
- **Period**: 2000-2025

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest |
| `screen.py` | Current stock screen |
| `generate_charts.py` | Chart generation from results |
| `results/` | JSON output per exchange |
| `charts/` | Generated PNG charts |
