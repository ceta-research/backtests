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
| Sustained ROIC | 8.86% | 0.279 | -36.3% |
| Single-year ROIC | 9.34% | 0.266 | -41.8% |
| Low ROIC | 5.52% | 0.132 | -48.3% |
| S&P 500 | 7.30% | 0.242 | -40.8% |

Excess CAGR: +1.56% vs SPY. Down capture: 77.5%. Alpha: +1.44%.

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
