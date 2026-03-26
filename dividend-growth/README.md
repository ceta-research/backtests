# Dividend Growth (Aristocrats) Backtest

Screen for companies with consecutive years of annual dividend increases and quality filters. Tests whether dividend growth streaks predict risk-adjusted outperformance.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Consecutive dividend increase years | >= 5 | `dividend_calendar` (adjDividend summed annually) |
| Dividend payout ratio | 0-80% | `financial_ratios` |
| Free cash flow | > 0 | `cash_flow_statement` |
| Market cap | > exchange threshold | `key_metrics` |

**Ranking:** Longest streak first, then largest market cap as tiebreaker.

**Portfolio:** Top 30 qualifying stocks, equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Annual (July). Full prior year dividend data and financial statements available by then.

## Data Tables

| Table | Key Columns |
|-------|-------------|
| `dividend_calendar` | symbol, date, adjDividend |
| `financial_ratios` | dividendPayoutRatio, dateEpoch, period |
| `key_metrics` | marketCap, dateEpoch, period |
| `cash_flow_statement` | freeCashFlow, dateEpoch, period |
| `stock_eod` | symbol, dateEpoch, adjClose |
| `profile` | symbol, exchange |

## Usage

```bash
# US stocks (default)
python3 dividend-growth/backtest.py

# Specific exchange
python3 dividend-growth/backtest.py --preset india
python3 dividend-growth/backtest.py --exchange XETRA

# All exchanges
python3 dividend-growth/backtest.py --global --output results/exchange_comparison.json --verbose

# Without transaction costs
python3 dividend-growth/backtest.py --no-costs

# Current screen
python3 dividend-growth/screen.py --preset us
```

## Academic References

- Lintner, J. (1956). "Distribution of Incomes of Corporations Among Dividends, Retained Earnings, and Taxes." *American Economic Review*, 46(2), 97-113.
- Arnott, R. & Asness, C. (2003). "Surprise! Higher Dividends = Higher Earnings Growth." *Financial Analysts Journal*, 59(1), 70-87.
- Siegel, J. (2005). *The Future for Investors.* Crown Business.
