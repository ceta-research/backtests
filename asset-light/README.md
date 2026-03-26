# Asset-Light Business Models

Composite asset-light score backtest. Ranks stocks by asset turnover, inverse capex intensity, and gross margin. Top quintile (asset-light) vs bottom quintile (asset-heavy).

## Signal

Three metrics, each PERCENT_RANK'd across the universe:

| Metric | Definition | Asset-Light Direction |
|--------|-----------|----------------------|
| Asset Turnover | Revenue / Total Assets | Higher |
| Capex Intensity | ABS(CapEx) / Revenue | Lower |
| Gross Margin | Gross Profit / Revenue | Higher |

Composite score = average of the three PERCENT_RANKs. Top 20% = asset-light, bottom 20% = asset-heavy.

## Usage

```bash
# US backtest
python3 asset-light/backtest.py --preset us --verbose

# India
python3 asset-light/backtest.py --preset india --verbose

# All exchanges
python3 asset-light/backtest.py --global --output results/exchange_comparison.json

# Current screen (TTM data)
python3 asset-light/screen.py --preset us

# Generate charts from results
python3 asset-light/generate_charts.py
```

## Key Results (2000-2025)

| Exchange | Light CAGR | Heavy CAGR | Spread |
|----------|------------|------------|--------|
| US | 2.82% | -13.40% | +16.22% |
| China | 3.17% | -1.29% | +4.46% |
| India | 5.39% | 1.60% | +3.79% |
| Germany | 5.90% | 1.79% | +4.11% |

The spread is positive on all 13 exchanges tested. Asset-light doesn't beat SPY as a standalone portfolio, but reliably separates compounders from capital destroyers.

## Parameters

- **Rebalancing:** Annual (April 1)
- **Filing lag:** 45 days
- **Market cap:** Exchange-specific thresholds (see `cli_utils.py`)
- **Sector exclusions:** Financial Services, Utilities
- **Transaction costs:** Size-tiered (0.1-0.5% per trade)
- **Weighting:** Equal weight
- **Portfolio cap:** 50 stocks max per quintile

## Academic Reference

- Eisfeldt & Papanikolaou (2013). "Organization Capital and the Cross-Section of Expected Returns." *Journal of Finance*.
- Novy-Marx (2013). "The Other Side of Value: The Gross Profitability Premium." *Journal of Financial Economics*.
