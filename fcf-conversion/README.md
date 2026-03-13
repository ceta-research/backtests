# FCF Conversion Quality

Systematic quality screen based on free cash flow conversion rate.

## Signal

Companies that convert a high percentage of reported earnings into actual free cash flow, with quality filters to eliminate statistical noise.

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| FCF / Net Income | 100% - 300% | Cash backs reported earnings. Cap at 300% filters tiny-NI noise |
| FCF / Revenue | > 10% | Ensures meaningful absolute cash flow, not just ratio math |
| Return on Equity | > 10% | Profitable business, not just high conversion on low base |
| Operating Margin | > 10% | Real pricing power, sustainable cash generation |
| Net Income | > 0 | Negative denominators make ratio meaningless |
| Free Cash Flow | > 0 | Only companies generating cash |
| Market Cap | > exchange threshold | Liquid stocks only |

**Selection:** Top 30 by highest FCF conversion, equal weight.
**Rebalancing:** Annual (July). Cash if fewer than 10 stocks qualify.

## Academic Basis

- Sloan (1996): Low-accrual firms outperform high-accrual firms by ~10% annually
- Richardson et al. (2005): Accrual reliability predicts earnings persistence
- Dechow (1994): Cash flows better measure firm performance than accruals

## Data Tables

| Table | Columns Used |
|-------|-------------|
| `cash_flow_statement` (FY) | `freeCashFlow` |
| `income_statement` (FY) | `netIncome`, `revenue` |
| `key_metrics` (FY) | `returnOnEquity`, `marketCap` |
| `financial_ratios` (FY) | `operatingProfitMargin` |

## Usage

```bash
# Backtest US stocks
python3 fcf-conversion/backtest.py

# Screen current stocks (TTM data)
python3 fcf-conversion/screen.py

# All exchanges
python3 fcf-conversion/backtest.py --global --output results/exchange_comparison.json --verbose

# Generate charts from results
python3 fcf-conversion/generate_charts.py
```

## Distinction from value-04-fcf-yield

| | FCF Conversion (this) | FCF Yield (value-04) |
|---|---|---|
| Formula | FCF / Net Income | FCF / Market Cap |
| Question | Is the cash real? | Is it cheap? |
| Signal type | Quality | Valuation |
| Academic root | Sloan (1996) accrual anomaly | Gray & Vogel (2012) |
