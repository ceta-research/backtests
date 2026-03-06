# Net Debt to EBITDA Screen

Conservative balance sheet screen: buy companies with low net leverage and high return on equity. The signal private equity analysts use to evaluate acquisition targets applied as a public equity screen.

**Signal:** Net Debt/EBITDA < 2x, > -5x (excludes extreme net-cash anomalies), ROE > 10%, Market Cap > $1B
**Portfolio:** Top 30 by lowest Net Debt/EBITDA, equal weight. Cash if fewer than 10 qualify.
**Rebalancing:** Quarterly (Jan/Apr/Jul/Oct), 2000–2025.
**Transaction costs:** Size-tiered model (see `costs.py`). Applied by default.

## What the Ratio Measures

Net Debt = Total Debt − Cash & Cash Equivalents

Net Debt / EBITDA = how many years of operating earnings to repay debt after using existing cash.

- Negative: net cash (more cash than debt)
- 0–2x: conservative to healthy
- 2–3x: moderate leverage
- Above 3x: elevated credit risk; above 5x: distress territory

Companies below 2x carry limited refinancing risk and have more flexibility to deploy capital during downturns.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000–2025, multi-exchange) |
| `screen.py` | Current TTM screen (live data) |
| `results/` | Output JSON and CSV files |

## Usage

```bash
# Backtest US stocks
python3 net-debt-ebitda/backtest.py

# Backtest India
python3 net-debt-ebitda/backtest.py --preset india

# All exchanges
python3 net-debt-ebitda/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (TTM data)
python3 net-debt-ebitda/screen.py --preset us

# Without transaction costs (academic baseline)
python3 net-debt-ebitda/backtest.py --no-costs
```

## Exchange Notes

JPX (Japan) and LSE (UK) are excluded from the global run — zero FY records in key_metrics as of 2025-03. Singapore uses exchange code SES (not SGX — FMP data convention).

## Data

Source: Ceta Research (FMP financial data warehouse)
Full methodology: [backtests/METHODOLOGY.md](../METHODOLOGY.md)
