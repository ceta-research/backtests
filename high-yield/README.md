# High Dividend Yield Quality

Screen for stocks with high dividend yields (4-15%) that pass quality filters to avoid yield traps.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Dividend yield | 4% to 15% | `financial_ratios` FY |
| Payout ratio | 0% to 80% | `financial_ratios` FY |
| Free cash flow | > 0 | `cash_flow_statement` FY |
| Return on equity | > 8% | `key_metrics` FY |
| Debt to equity | < 2.0 | `financial_ratios` FY |
| Market cap | > exchange threshold | `key_metrics` FY |

**Selection:** Top 30 by highest dividend yield, equal weight.
**Rebalancing:** Annual (July). Cash if fewer than 10 stocks qualify.

## Academic Reference

Fama, E. & French, K. (1998). "Value versus Growth: The International Evidence." *Journal of Finance*, 53(6), 1975-1999.

High-yield stocks as a value proxy outperformed growth across 13 international markets. Quality filters (payout, FCF, ROE) separate sustainable yields from yield traps.

## Usage

```bash
# US backtest
python3 high-yield/backtest.py

# India
python3 high-yield/backtest.py --preset india

# All exchanges
python3 high-yield/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen
python3 high-yield/screen.py --preset us
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current qualifying stocks |
| `generate_charts.py` | Chart generation from results |

*Data: Ceta Research (FMP financial data warehouse).*
