# Owner Earnings Yield Strategy

Buffett's Owner Earnings metric applied as a quantitative value screen. Selects stocks with high Owner Earnings yield, quality profitability, and adequate size.

## Concept

Warren Buffett introduced Owner Earnings in his 1986 Berkshire Hathaway letter. The formula strips out growth capex to isolate what a business actually earns for its owners:

**Owner Earnings = Net Income + D&A - Maintenance Capex**

Where maintenance capex is estimated as `min(|Capex|, D&A)`. If a company spends more on capex than its depreciation, the excess is treated as growth investment and excluded.

**OE Yield = Owner Earnings / Market Cap**

This differs from FCF yield (which deducts all capex) by not penalizing companies that invest heavily in growth.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| OE Yield | > 5% | Less than 20x owner earnings |
| OE Yield | < 50% | Removes data artifacts |
| ROE | > 10% | Solid returns on shareholder equity |
| Operating Margin | > 10% | Genuine pricing power |
| Market Cap | > exchange threshold | Liquidity filter |

## Parameters

- **Rebalancing**: Annual (July, after FY filings + 45-day lag)
- **Portfolio size**: Top 30 by OE yield, equal weight
- **Minimum stocks**: 10 (holds cash if fewer qualify)
- **Transaction costs**: Size-tiered model (0.1-0.5% one-way)

## Usage

```bash
# Backtest US stocks
python3 owner-earnings/backtest.py --preset us --output results/returns_US.json --verbose

# Screen current qualifying stocks
python3 owner-earnings/screen.py --preset us

# Run all exchanges
python3 owner-earnings/backtest.py --global --output results/exchange_comparison.json --verbose

# Generate charts from results
python3 owner-earnings/generate_charts.py
```

## Data Sources

- `income_statement` (FY): netIncome, depreciationAndAmortization
- `cash_flow_statement` (FY): capitalExpenditure
- `key_metrics` (FY): marketCap
- `financial_ratios` (FY): returnOnEquity, operatingProfitMargin
- `stock_eod`: adjClose for return calculation

## References

- Buffett, W. (1986). "Berkshire Hathaway Annual Letter to Shareholders."
- Greenwald, B. et al. (2001). *Value Investing: From Graham to Buffett and Beyond.*
- Greenblatt, J. (2006). *The Little Book That Beats the Market.*

*Data: Ceta Research (FMP financial data warehouse).*
