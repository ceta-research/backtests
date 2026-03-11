# Free Cash Flow Growth Strategy

Annual rebalancing backtest using YoY FCF growth as the primary signal.

## Strategy

**Signal:** Companies with:
- FCF growth YoY > 15% (free cash flow = operating cash flow - capital expenditures)
- OCF growth YoY > 0% (operating cash flow also growing — guards against capex-cut manipulation)
- ROE > 10%
- Debt-to-equity < 1.5
- Market cap > local threshold (per-exchange, ~$200-500M USD-equivalent)

**Selection:** Top 30 by FCF growth, equal weight. Cash if fewer than 10 qualify.

**Rebalancing:** Annual (July). FY filings available by then with 45-day reporting lag.

**Costs:** Size-tiered transaction cost model (costs.py).

## Academic Basis

Sloan (1996) documented the accrual anomaly: companies with earnings backed by cash flow outperform those with earnings driven by accruals. The market systematically overprices accrual-heavy earnings and underprices cash-backed earnings.

FCF growth is a quality signal — companies growing real cash generation tend to compound value. The OCF confirmation filter separates genuine operational improvement from capex-cut engineering.

## Usage

```bash
# US stocks (NYSE + NASDAQ + AMEX)
python3 fcf-growth/backtest.py

# India
python3 fcf-growth/backtest.py --preset india --verbose

# All exchanges
python3 fcf-growth/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (live data)
python3 fcf-growth/screen.py --preset us

# Without transaction costs
python3 fcf-growth/backtest.py --no-costs
```

## Files

| File | Description |
|------|-------------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen using latest data |
| `generate_charts.py` | Generate charts from results |
| `results/exchange_comparison.json` | Multi-exchange backtest results |

## Key Parameters

| Parameter | Value |
|-----------|-------|
| FCF growth threshold | > 15% YoY |
| OCF growth threshold | > 0% YoY |
| ROE minimum | > 10% |
| D/E maximum | < 1.5 |
| Portfolio size | Top 30, equal weight |
| Min to invest | 10 stocks |
| Rebalance | Annual (July) |
| Data lag | 45 days |

## Excluded Exchanges

- **ASX** (Australia): adjClose split data artifacts
- **SAO** (Brazil): adjClose data artifacts

## References

- Sloan, R. (1996). "Do Stock Prices Fully Reflect Information in Accruals and Cash Flows About Future Earnings?" *The Accounting Review*, 71(3), 289-315.
- Gray, W. & Vogel, J. (2016). *Quantitative Value.* Wiley.
