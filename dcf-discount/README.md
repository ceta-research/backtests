# DCF Discount Value Strategy

**Category:** Value
**Signal:** Stocks trading below intrinsic value (computed DCF)
**Academic reference:** Gordon (1959); Graham & Dodd (1934) *Security Analysis*

---

## Strategy Overview

Buys stocks where the current market price is at least 20% below a computed intrinsic value derived from the Gordon Growth Model:

```
DCF_per_share = FCF × 13.67 × price / marketCap
discount      = 1 - marketCap / (FCF × 13.67)
```

Where `FCF` is the most recent annual free cash flow and `13.67` is the perpetuity multiple at 10% discount rate and 2.5% terminal growth.

Key insight: a 20% discount is equivalent to an FCF yield ≥ 8.78% (FCF/MarketCap). The signal is price-independent — it depends only on how much FCF a company generates relative to its market cap.

---

## Signal

| Filter | Value |
|--------|-------|
| FCF/MarketCap (FCF yield) | ≥ 8.78% (= 20% DCF discount) |
| Annual free cash flow | Positive (FCF > 0) |
| Market cap | ≥ $1B |
| Filing lag | 45 days (point-in-time) |
| Filing staleness | ≤ 18 months |

**Portfolio construction:** Top 50 stocks by FCF yield, equal weight. Hold cash if fewer than 10 qualify.

**Rebalancing:** Annual (April). Annual filings are available for most companies by April.

---

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Live screen showing current qualifying stocks |
| `generate_charts.py` | Chart generation from results |
| `README.md` | This file |

---

## Usage

```bash
# Screen current stocks (US, top 30)
python3 dcf-discount/screen.py

# Screen with quality filters (ROE > 10%, D/E < 1.5)
python3 dcf-discount/screen.py --quality

# Screen India
python3 dcf-discount/screen.py --preset india

# Backtest US (default)
python3 dcf-discount/backtest.py --verbose

# Backtest Germany
python3 dcf-discount/backtest.py --preset germany --verbose

# All exchanges
python3 dcf-discount/backtest.py --global --output results/exchange_comparison.json --verbose

# Without transaction costs
python3 dcf-discount/backtest.py --no-costs --verbose

# Cloud execution
python3 dcf-discount/screen.py --cloud
python3 dcf-discount/backtest.py --cloud --preset us

# Generate charts after backtest
python3 dcf-discount/generate_charts.py
```

---

## Model Assumptions

The Gordon Growth Model uses universal assumptions:
- **Discount rate (r):** 10% — long-run expected equity return
- **Terminal growth rate (g):** 2.5% — approximate long-run GDP growth
- **Perpetuity multiple:** 13.67 = (1 + 0.025) / (0.10 - 0.025)

These parameters apply globally. For high-growth economies (India, China), the 2.5% terminal growth assumption is conservative — a higher assumed growth rate would make more stocks appear undervalued. For high-inflation markets (Brazil), the 10% discount rate may be too low. These are model limitations documented in the content.

---

## Data Notes

- **FCF source:** `cash_flow_statement.freeCashFlow` (annual/FY filings)
- **Market cap source:** `key_metrics.marketCap` (annual/FY filings)
- **Price source:** `stock_eod.adjClose` (for portfolio return calculation)
- **Currency:** FCF and marketCap are in the company's functional currency (local). The discount percentage is dimensionless, so the signal is currency-independent.

---

## US Results (existing, quarterly backtest 2010-2025)

| Metric | DCF Discount | S&P 500 (SPY) |
|--------|-------------|---------------|
| CAGR | 16.18% | 13.63% |
| Excess CAGR | +2.55% | — |
| Max Drawdown | -45.5% | -23.8% |
| Sharpe Ratio | 0.563 | 0.699 |
| Win Rate vs SPY | 58.3% | — |

*Note: Existing US results used quarterly rebalancing and $500M MCap threshold (old backtest.py). The shared framework uses annual rebalancing and $1B MCap threshold — results will differ.*

---

## Publishing

Content lives in: `ts-content-creator/content/_current/value-06-dcf-discount/`
Blog, LinkedIn, Reddit posts, and video scripts are in that directory.

Data source attribution: *Data: Ceta Research (FMP financial data warehouse).*
