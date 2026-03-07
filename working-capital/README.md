# Working Capital Efficiency

**Strategy slug:** `working-capital`
**Category:** Balance Sheet Quality
**Rebalancing:** Annual (June), 2000-2025
**Universe:** Full exchange (NYSE+NASDAQ+AMEX for US, not index-constrained)

---

## What It Does

Screen for companies that generate revenue with minimal working capital tied up in the business. The ratio `workingCapital / revenue` measures how much current capital a company needs per dollar of revenue. Low is better.

**Signal:** Lowest WC/Revenue ratio, with quality guardrails to exclude distressed companies.

### Filters

| Filter | Threshold | Purpose |
|--------|-----------|---------|
| `workingCapital / revenue` | < 50% | Core signal: capital efficiency |
| `workingCapital` | > 0 | Exclude negative WC (different regime) |
| Revenue growth (YoY) | Positive | Exclude shrinking companies |
| ROE | > 8% | Quality filter: profitable business |
| Operating profit margin | > 10% | Quality filter: operating efficiency |
| Market cap | > $500M | Liquidity filter |

### Portfolio Construction

- **Selection:** Top 30 by lowest WC/Revenue, equal weight
- **Cash:** Hold cash if fewer than 10 stocks qualify
- **Costs:** Size-tiered transaction cost model (see `../costs.py`)
- **Benchmark:** SPY (S&P 500)

---

## Academic Foundation

**Core paper:** Sloan, R. (1996). "Do Stock Prices Fully Reflect Information in Accruals and Cash Flows About Future Earnings?" *The Accounting Review*, 71(3), 289-315.

The Sloan accrual anomaly: companies with high accruals (bloated working capital relative to cash flows) have lower earnings quality and tend to underperform. Working capital efficiency is a direct measure of accrual intensity.

**Supporting:**
- Hirshleifer, Hou, Teoh & Zhang (2004). "Do Investors Overvalue Firms with Bloated Balance Sheets?" *Journal of Accounting and Economics*, 38, 297-331.
- Richardson et al. (2005). "Accrual Reliability, Earnings Persistence and Stock Prices." *Journal of Accounting and Economics*, 39(3), 437-485.

---

## Files

```
working-capital/
  backtest.py         -- Full historical backtest (2000-2025, annual rebalance)
  screen.py           -- Current stock screen (live TTM data)
  generate_charts.py  -- Chart generation from results/exchange_comparison.json
  README.md           -- This file
  results/            -- Computed results (exchange_comparison.json, returns_*.json)
  charts/             -- Generated charts
```

---

## Usage

```bash
# From backtests/ root directory

# US backtest (NYSE+NASDAQ+AMEX)
python3 working-capital/backtest.py --verbose

# Save results
python3 working-capital/backtest.py --output working-capital/results/returns_US_MAJOR.json

# All exchanges (global mode)
python3 working-capital/backtest.py --global \
  --output working-capital/results/exchange_comparison.json \
  --verbose

# India
python3 working-capital/backtest.py --preset india

# Without transaction costs (academic baseline)
python3 working-capital/backtest.py --no-costs

# Current screen (live data)
python3 working-capital/screen.py
python3 working-capital/screen.py --preset india

# Generate charts (after running global backtest)
python3 working-capital/generate_charts.py
```

---

## Data Tables

| Table | Columns Used | Purpose |
|-------|-------------|---------|
| `balance_sheet` (FY) | `workingCapital`, `totalCurrentAssets`, `totalCurrentLiabilities` | Core signal |
| `income_statement` (FY) | `revenue` | Efficiency denominator, growth check |
| `key_metrics` (FY) | `returnOnEquity`, `returnOnAssets`, `marketCap` | Quality filters |
| `financial_ratios` (FY) | `operatingProfitMargin` | Quality filter |
| `stock_eod` | `adjClose` | Price returns |
| `profile` | `exchange` | Universe filter |

**Data lag:** 45-day lag applied to all fundamental data (point-in-time compliance for annual filings).

---

## Why WC/Revenue < 50%?

The threshold allows approximately the top quintile of the market to qualify, ensuring enough portfolio diversity. The sort + top-30 selection does the real work. A company at 49% WC/Revenue may still qualify if it has superior quality metrics vs competitors at 15%.

Companies with negative WC are excluded (common in retail/subscription businesses) because they represent a fundamentally different capital structure, not operational inefficiency.

---

## Notes

- **Financials excluded** in `screen.py` (current screen). In historical backtest, the quality filters (positive WC + OPM > 10%) naturally exclude most financial companies where WC is meaningless.
- **No S&P 500 constraint.** Full exchange universe captures more qualifying stocks and avoids index selection bias.
- **Currency:** Returns computed in local currency. Cross-exchange comparison uses SPY USD as benchmark, so currency effects are included in excess return.
