# Margin Expansion Backtest

Screens stocks by operating profit margin expansion over a 3-year lookback period. Companies with improving margins outperform those with deteriorating margins.

## Signal

**Margin Expansion = Current Year OPM - Average(Prior 3 FY OPMs)**

- **OPM** = Operating Income / Revenue (from annual income statements)
- **Expanding** (>+1pp): Margins improving vs 3-year average
- **Stable** (-1pp to +1pp): Flat margins
- **Contracting** (<-1pp): Margins deteriorating

Secondary signal: **Consecutive Expanding** = expansion > 0 AND current OPM > prior year OPM (2+ years of improvement).

## Academic Basis

- Novy-Marx (2013) "The Other Side of Value: The Gross Profitability Premium" - *Journal of Financial Economics*
- Haugen & Baker (1996) "Commonality in the Determinants of Expected Stock Returns" - *Journal of Financial Economics*
- Asness et al. (2019) "Quality Minus Junk" - *Review of Accounting Studies*

## Parameters

| Parameter | Value |
|-----------|-------|
| Universe | Non-financial stocks above exchange-specific market cap threshold |
| Rebalancing | Annual (April 1) |
| Filing lag | 45 days |
| Weighting | Equal weight, top 30 expanders |
| Transaction costs | Size-tiered (0.1-0.5% per trade) |
| Benchmark | S&P 500 (SPY) |
| Data requirement | 4+ fiscal years per company (current + 3 prior) |

## Usage

```bash
# US backtest
python3 margin-expansion/backtest.py --verbose

# India
python3 margin-expansion/backtest.py --preset india --verbose

# All exchanges
python3 margin-expansion/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (TTM)
python3 margin-expansion/screen.py --preset us

# Generate charts
python3 margin-expansion/generate_charts.py
```

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
