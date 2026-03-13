# Dividend Sustainability Score

A 5-component composite score (0-10) that predicts which dividends can survive 5-10 years, not just whether they can be paid today.

## Strategy

Most dividend safety analysis checks one ratio: FCF coverage. That tells you if the company can afford its current payment. It doesn't tell you if the payment will survive the next recession, debt cycle, or competitive shift.

The sustainability score combines five financial dimensions into a single number:

| # | Component | Source | Score 2 | Score 1 | Score 0 |
|---|-----------|--------|---------|---------|---------|
| 1 | Payout Ratio | financial_ratios | < 50% | 50-80% | > 80% |
| 2 | Debt/Equity | financial_ratios | < 0.5 | 0.5-1.5 | > 1.5 |
| 3 | FCF Coverage | cash_flow_statement | > 2x | 1-2x | < 1x |
| 4 | ROE | key_metrics | > 15% | 8-15% | < 8% |
| 5 | Piotroski F-Score | computed from statements | >= 7 | 5-6 | < 5 |

**Portfolio construction:** Top 30 stocks by score DESC (yield tiebreak), minimum score 7, yield >= 2%, equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Annual (July). Uses 45-day lag for point-in-time data.

**Piotroski computation:** The F-Score is computed from historical income statements, balance sheets, and cash flow statements (9 binary signals). This avoids look-ahead bias from using snapshot score tables.

## Academic References

- DeAngelo, DeAngelo & Skinner (1992). "Dividends and Losses." Companies cutting dividends showed multi-dimensional deterioration beforehand.
- Piotroski (2000). "Value Investing: Historical Financial Statement Information." Composite scores from simple signals outperform single metrics.
- Benartzi, Michaely & Thaler (1997). "Do Changes in Dividends Signal the Future or the Past?"

## Usage

```bash
# Run backtest (US default)
python3 dividend-sustainability/backtest.py

# Run for specific exchange
python3 dividend-sustainability/backtest.py --preset india --verbose

# Run all exchanges
python3 dividend-sustainability/backtest.py --global --output results/exchange_comparison.json

# Current screen
python3 dividend-sustainability/screen.py
python3 dividend-sustainability/screen.py --preset india
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current qualifying stocks |
| `generate_charts.py` | Chart generation from results |
| `results/` | Backtest output (JSON/CSV) |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
