# Low Debt Quality

**Category:** Risk / Balance Sheet
**Signal:** Debt/Equity < 0.5 + Piotroski F-Score >= 7
**Rebalancing:** Annual (July)
**Universe:** NYSE + NASDAQ + AMEX (US default), 15 exchanges globally
**Backtest period:** 2000–2025

---

## Strategy Overview

The Low Debt Quality strategy screens for companies with two properties: conservative balance sheets (Debt/Equity below 0.5) and strong financial health (Piotroski F-Score of 7 or higher out of 9).

The D/E filter targets companies that have chosen conservative financing — not companies that can't access debt markets. The Piotroski filter ensures those companies are also financially healthy: growing profitability, improving liquidity, no dilution, cash-backed earnings.

The combination screens out two failure modes:
1. Companies with low debt because they're struggling (low D/E + low Piotroski)
2. Companies with high Piotroski scores but levered balance sheets (high Piotroski + high D/E)

---

## Signal Definition

**Step 1: D/E filter**
- `debtToEquityRatio < 0.5` (FY financial_ratios table)
- `debtToEquityRatio >= 0` (exclude negative equity companies)

**Step 2: Piotroski F-Score >= 7** (computed from FY statements)

| # | Factor | Condition |
|---|--------|-----------|
| F1 | Profitability | Net income > 0 |
| F2 | Cash quality | Operating cash flow > 0 |
| F3 | ROA trend | ROA improved vs prior year |
| F4 | Accrual quality | OCF > Net income |
| F5 | Leverage trend | Long-term debt ratio decreased |
| F6 | Liquidity trend | Current ratio improved |
| F7 | No dilution | Equity >= prior year equity |
| F8 | Asset efficiency | Asset turnover improved |
| F9 | Gross margin | Gross margin improved |

Score range: 0–9. Threshold: 7+.

**Step 3: Market cap > exchange threshold**
- Uses `cli_utils.get_mktcap_threshold()` (local currency per exchange)

**Portfolio:** Equal weight, all qualifying stocks. Hold cash if < 10 qualify.

---

## Academic Foundation

- **Piotroski (2000):** "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers." Journal of Accounting Research, 38, 1–41. The 9-factor F-Score separates financially strong from financially weak stocks within the value universe. This strategy applies it without the P/B filter — as a standalone financial health screen.
- **Graham & Dodd (1934):** "Security Analysis." Emphasized margin of safety through conservative balance sheets. D/E < 0.5 operationalizes their principle that companies should not be excessively leveraged.
- **George & Hwang (2010):** "A Resolution of the Distress Risk and Leverage Puzzles." Journal of Financial Economics. Low leverage stocks earn higher risk-adjusted returns, particularly during downturns.

---

## Data Sources

| Table | Columns Used |
|-------|-------------|
| `financial_ratios` (FY) | `debtToEquityRatio` |
| `income_statement` (FY) | `netIncome`, `grossProfit`, `revenue` |
| `balance_sheet` (FY) | `totalAssets`, `totalCurrentAssets`, `totalCurrentLiabilities`, `longTermDebt`, `totalStockholdersEquity` |
| `cash_flow_statement` (FY) | `operatingCashFlow` |
| `key_metrics` (FY) | `marketCap` |
| `stock_eod` | `adjClose` |

---

## Running the Backtest

```bash
# US stocks (default)
python3 low-debt/backtest.py --verbose

# Single exchange
python3 low-debt/backtest.py --preset india --verbose

# All exchanges
python3 low-debt/backtest.py --global --output results/exchange_comparison.json --verbose

# Without transaction costs
python3 low-debt/backtest.py --no-costs

# Current stock screen (TTM data)
python3 low-debt/screen.py

# Generate charts (after running --global)
python3 low-debt/generate_charts.py
```

---

## Key Design Decisions

**Why annual rebalancing (July)?**
Annual FY filings are published by April/May for most companies. July gives 45+ days of buffer for data availability (point-in-time methodology). Annual rebalancing also keeps transaction costs manageable given equal weighting with no size cap.

**Why no maximum stock count?**
The combined D/E + Piotroski filter is strict enough to produce a focused portfolio naturally. Capping at 30 would arbitrarily exclude qualifying stocks. Taking all qualifying stocks produces better diversification.

**Why no P/B filter?**
The original Piotroski strategy applies F-Score to value stocks (bottom quintile by P/B). This strategy uses Piotroski as a pure financial health filter — quality screen, not value screen. Combined with D/E < 0.5, it selects quality-conservative companies across all valuation levels.

**Market cap thresholds (local currency):**
FMP stores marketCap in local currency. Thresholds use `get_mktcap_threshold()` from `cli_utils.py` to apply exchange-appropriate filters (~$200–500M USD equivalent).

---

## Notes

- **ASX excluded:** adjClose split/adjustment issues in FMP data
- **SAO (Brazil) excluded:** adjClose split/adjustment issues
- **Point-in-time:** 45-day lag at rebalance date (July = data through ~May 17)
- **Transaction costs:** Size-tiered model from `costs.py`
