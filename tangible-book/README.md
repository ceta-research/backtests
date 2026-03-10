# Price-to-Tangible-Book Strategy

**Category:** Balance Sheet
**Slug:** `tangible-book`
**Academic Reference:** Fama & French (1992), "The Cross-Section of Expected Stock Returns"

---

## Strategy Overview

Price-to-Tangible-Book (P/TBV) strips goodwill and intangible assets from book value, leaving only the company's hard, physical asset base: property, equipment, inventory, receivables.

The signal:
- **Tangible equity** = totalStockholdersEquity − goodwill − intangibleAssets
- **P/TBV ratio** = marketCap / tangible_equity
- **Buy:** stocks with the lowest P/TBV (cheapest relative to hard assets)

Quality filters keep the portfolio away from distressed low-tangible-book traps:
- ROE > 8% (profitable business)
- ROA > 3% (asset efficiency)
- OPM > 10% (operating strength)

---

## Signal

```
Tangible equity = totalStockholdersEquity - COALESCE(goodwill, 0) - COALESCE(intangibleAssets, 0)
P/TBV = marketCap / tangible_equity

Filter:
  - tangible_equity > 0 (positive tangible book)
  - returnOnEquity > 8%
  - returnOnAssets > 3%
  - operatingProfitMargin > 10%
  - marketCap > exchange-specific threshold

Select: Top 30 by P/TBV ASC (lowest first), equal weight
Rebalance: Annual (July), 45-day filing lag
```

---

## Parameters

| Parameter | Value |
|-----------|-------|
| Rebalancing | Annual (July) |
| Filing lag | 45 days (FY filings) |
| Min stocks | 10 (hold cash if fewer qualify) |
| Max stocks | 30 |
| Weighting | Equal weight |
| Transaction costs | Size-tiered (from costs.py) |
| Benchmark | SPY |

---

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2000-2025) |
| `screen.py` | Current stock screen (live data, TTM metrics) |
| `generate_charts.py` | Chart generation from results |
| `results/exchange_comparison.json` | Full multi-exchange results |

---

## Usage

```bash
# Backtest US stocks (default)
python3 tangible-book/backtest.py

# Backtest Indian stocks
python3 tangible-book/backtest.py --preset india

# Backtest all exchanges
python3 tangible-book/backtest.py --global --output results/exchange_comparison.json --verbose

# Current stock screen
python3 tangible-book/screen.py

# Screen Indian stocks
python3 tangible-book/screen.py --preset india

# Generate charts (requires results/exchange_comparison.json)
python3 tangible-book/generate_charts.py
```

---

## Why Tangible Book vs Standard Book

Standard P/B includes goodwill — the premium paid for acquisitions. When acquisitions underperform, goodwill gets written down, collapsing reported book value. Companies with high goodwill-to-equity ratios are vulnerable to this write-down risk.

P/TBV:
- Removes acquisition premium from the denominator
- More conservative valuation floor
- Particularly relevant for industrials, financials, basic materials
- Less relevant for tech (intangibles are the product)

The gap between P/B and P/TBV is a direct signal for acquisition risk. A narrow gap means most book value is tangible. A wide gap means management has paid heavily for intangibles that may or may not hold their value.

---

## Academic Context

Fama & French (1992) documented that book-to-market ratio (inverse of P/B) predicts cross-sectional stock returns. The tangible version is a natural extension: if book value includes goodwill that's at risk of write-down, the reported ratio understates true valuation risk.

Barth et al. (1998) found that brand values and intangibles have reduced reliability as predictors of firm value, supporting the case for separating tangible from intangible components.

---

## Data Source

All data from Ceta Research (FMP financial data warehouse). Historical FY financials, adjusted close prices, point-in-time via 45-day filing lag.
