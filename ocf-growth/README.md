# OCF Growth / Cash Flow Momentum Strategy

Buy stocks where operating cash flow is growing faster than earnings. When cash flow outpaces reported profits, it signals improving earnings quality. When earnings grow faster than cash flow, reported profits may not be sustainable.

## The Signal

A stock qualifies when all filters pass:

| Filter | Threshold | Why |
|--------|-----------|-----|
| OCF Growth | > 10% YoY | Cash generation is meaningfully improving |
| OCF Growth | < 500% | Caps recovery-from-zero artifacts |
| Divergence | OCF Growth > NI Growth | Cash flow quality is improving, not deteriorating |
| Return on Equity | > 10% | Already profitable (filters out noisy turnarounds) |
| Operating Margin | > 5% | Real operating business (not thin-margin noise) |

Portfolio: top 30 by divergence (OCF growth minus NI growth, highest first), equal weight. Rebalance annually in July.

**Why divergence over raw OCF growth:** Raw OCF growth catches base-effect recoveries and cyclical bounces. The divergence signal isolates genuine quality improvement: cash is growing faster than the accounting numbers. This is Sloan's (1996) accrual anomaly applied as a momentum signal.

## Academic Basis

Chan, Chan, Jegadeesh & Lakonishok (2006) studied earnings quality and stock returns. They found that measures of accruals and cash flow momentum predict future performance. Stocks with improving cash flow quality outperformed.

Sloan (1996) showed that the accrual component of earnings is less persistent than the cash component. High-accrual stocks (earnings >> cash flow) underperform. Low-accrual stocks (cash flow >> earnings) outperform.

Dechow (1994) found that over longer measurement intervals, cash flows become more informative than earnings. Annual OCF growth is more reliable than quarterly.

## Usage

```bash
# Screen current stocks (US default)
python3 ocf-growth/screen.py

# Screen India
python3 ocf-growth/screen.py --preset india

# Run backtest (US, annual, 2000-2025)
python3 ocf-growth/backtest.py

# Run backtest all exchanges
python3 ocf-growth/backtest.py --global --output results/exchange_comparison.json --verbose

# Run without transaction costs
python3 ocf-growth/backtest.py --no-costs
```

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen (live TTM + latest FY growth data) |
| `generate_charts.py` | Chart generation from results |
| `results/` | JSON/CSV output from backtest runs |
| `charts/` | Generated PNG charts |

## Methodology

- **Universe**: Full exchange (NYSE+NASDAQ+AMEX for US). Not index-constrained.
- **Data**: FMP financial data via Ceta Research warehouse
- **Rebalancing**: Annual, July. FY growth data with 45-day lag.
- **Costs**: Size-tiered transaction costs (0.05-0.15% per trade, varies by market cap)
- **Min holding**: 10 stocks required. Cash position if fewer qualify.
- **Period**: 2000-2025 (25 annual periods)

See [METHODOLOGY.md](../METHODOLOGY.md) for full methodology documentation.

## Data Source

Data: Ceta Research (FMP financial data warehouse). Growth metrics from `cash_flow_statement_growth` and `income_statement_growth` tables. Quality filters from `key_metrics` and `financial_ratios`.
