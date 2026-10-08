# Sustained ROIC Backtest

Screens for companies with ROIC above 12% in at least 3 of the last 5 fiscal years. Tests whether persistent capital efficiency predicts future returns.

## Signal

- **ROIC** = NOPAT / Invested Capital
- **NOPAT** = Operating Income x (1 - effective tax rate)
- **Invested Capital** = Total Assets - Current Liabilities - Cash
- **Sustained** = ROIC > 12% in 3+ of last 5 FY periods

## Key Results (US, 2000-2025)

| Portfolio | CAGR | Sharpe | Max DD |
|-----------|------|--------|--------|
| Sustained ROIC | 9.29% | 0.314 | -33.1% |
| Single-year ROIC | 9.27% | 0.266 | -39.2% |
| Low ROIC | 6.20% | 0.164 | -45.2% |
| S&P 500 | 7.33% | 0.253 | -39.3% |

Excess CAGR: +1.96% vs SPY. Down capture: 74.1%. Alpha: +1.96%.

From `results/exchange_comparison.json` (`US_MAJOR`, 2026-10-08 run). Every other market in that file is measured against its own local index; South Africa, which has no local index in the data, against the S&P 500 in USD.

## Usage

```bash
# Screen current stocks
python3 sustained-roic/screen.py
python3 sustained-roic/screen.py --preset india

# Run backtest
python3 sustained-roic/backtest.py --preset us --verbose
python3 sustained-roic/backtest.py --global --output sustained-roic/results/exchange_comparison.json

# Generate charts
python3 sustained-roic/generate_charts.py
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (3 portfolio tracks) |
| `screen.py` | Current stock screen |
| `generate_charts.py` | Chart generation from results |
| `results/` | Computed results (JSON) |

## References

- Greenblatt, J. (2006). *The Little Book That Beats the Market.* Wiley.
- Novy-Marx, R. (2013). *The Other Side of Value: The Gross Profitability Premium.* Journal of Financial Economics.
