# DCF Threshold — Intrinsic Value with Quality Filters

**Category:** Timing / Value
**Exchanges:** 15 global markets
**Rebalancing:** Annual (January)
**Signal:** FCF yield >= 8.78% (= 20% DCF discount) + ROE > 8% + D/E < 1.5 + OCF > 0

---

## What It Does

Uses the Gordon Growth Model to estimate each stock's intrinsic value, then buys the
30 deepest-discount stocks that also pass quality filters. The goal: capture the margin-of-safety
value premium without owning distressed companies hiding behind temporarily elevated FCF.

**Gordon Growth Model:**

```
DCF_value  = FCF * (1+g)/(r-g) = FCF * 13.67
discount%  = 1 - marketCap / (FCF * 13.67)
```

With g=2.5%, r=10%, a 20% discount threshold translates to FCF/MarketCap >= 8.78%.
This is a price-independent signal — it depends only on fundamentals and market cap.

---

## Why Quality Filters?

Pure FCF yield screens can capture distressed companies selling assets, cutting capex,
or about to collapse. Adding three guards:

- **ROE > 8%:** Profitable on equity — cash isn't from asset liquidation
- **D/E < 1.5:** Not dangerously leveraged (NULL D/E is allowed — non-financials in some markets)
- **OCF > 0:** Operating model genuinely generates cash

Together these eliminate most value traps while preserving genuine undervaluation.

---

## Signal Parameters

| Parameter          | Value  | Rationale                                      |
|--------------------|--------|------------------------------------------------|
| Growth rate (g)    | 2.5%   | Conservative terminal growth                   |
| Discount rate (r)  | 10%    | Equity cost of capital                         |
| DCF multiple       | 13.67x | (1+g)/(r-g)                                    |
| Discount threshold | 20%    | Graham-style margin of safety                  |
| FCF yield minimum  | 8.78%  | Equivalent price-independent signal            |
| ROE minimum        | 8%     | Profitability quality gate                     |
| D/E maximum        | 1.5    | Leverage quality gate                          |
| Min stocks         | 10     | Hold cash if fewer qualify                     |
| Max stocks         | 30     | Concentrated quality value                     |
| Rebalance          | Annual, January | Post-tax-loss-selling entry         |

---

## Data Sources

| Table                    | Columns Used                            |
|--------------------------|-----------------------------------------|
| `cash_flow_statement` FY | `freeCashFlow`, `operatingCashFlow`     |
| `key_metrics` FY         | `marketCap`, `returnOnEquity`           |
| `financial_ratios` FY    | `debtToEquityRatio`                     |
| `stock_eod`              | `adjClose` (at rebalance dates)         |

---

## Backtest Setup

- **Period:** 2000–2025 (26 annual periods)
- **Filing lag:** 45 days (point-in-time data integrity)
- **Staleness limit:** 18 months (uses most recent annual filing within window)
- **Transaction costs:** Size-tiered (0.1% for large-cap, 0.5% for small-cap), one-way
- **Benchmark:** S&P 500 (SPY)
- **Exchanges:** 15 global markets (ASX/SAO excluded — adjClose quality issues)

---

## How It Differs from Related Strategies

| Strategy          | Signal                   | Quality | Size | Rebalance |
|-------------------|--------------------------|---------|------|-----------|
| `dcf-discount`    | FCF yield >= 8.78%       | None    | 50   | April     |
| `dcf-threshold`   | FCF yield >= 8.78%       | ROE + D/E + OCF | 30 | January |
| `pe-mean-revert`  | P/E < 60% sector median  | ROE + D/E | 30  | January  |
| `qarp`            | P/E 5-25 + 6 other       | Piotroski >= 7 | 30 | Jan/Jul |

DCF threshold sits between dcf-discount (pure value) and QARP (multi-factor quality+value).

---

## Academic Reference

- Gordon, M.J. (1959). Dividends, Earnings, and Stock Prices. *Review of Economics and Statistics*, 41(2), 99–105.
- Graham, B. & Dodd, D. (1934). *Security Analysis*. — Margin of safety concept.
- Piotroski, J.D. (2000). Value Investing: The Use of Historical Financial Statement Information. *Journal of Accounting Research*, 38, 1–41. — Quality filters improve value returns.
