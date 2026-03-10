# Deleveraging Strategy

**Signal:** Companies reducing debt-to-equity ratio by 10%+ year-over-year
**Rebalancing:** Quarterly (Jan/Apr/Jul/Oct)
**Universe:** Full exchange (NYSE+NASDAQ+AMEX for US), 2001-2025

## Strategy Logic

Companies actively paying down debt signal management discipline, improved cash flows,
and reduced financial distress risk. The strategy finds companies where the debt-to-equity
ratio dropped at least 10% compared to the prior fiscal year filing.

**Filters:**
- D/E YoY change < -10% (meaningful debt reduction)
- Prior D/E > 0.1 (company had actual debt to reduce)
- Current D/E >= 0 (not in technical distress with negative equity)
- ROE > 8% (healthy deleveraging from profitability, not asset fire-sales)
- Market cap > exchange-specific threshold (~$200-500M USD equivalent)

**Selection:** Top 30 by magnitude of deleveraging (largest % D/E reduction)
**Weighting:** Equal weight
**Min positions:** 10 (hold cash if fewer qualify)

## Data Sources

- `financial_ratios` (FY): debtToEquityRatio for current and prior year D/E
- `key_metrics` (FY): returnOnEquity, marketCap
- `stock_eod`: adjClose for price returns
- Point-in-time: 45-day filing lag for current year, ~410-day lookback for prior year

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full historical backtest (2001-2025) |
| `screen.py` | Current stock screen (live FY data) |
| `generate_charts.py` | Chart generation from results JSON |
| `results/exchange_comparison.json` | Backtest results (all exchanges) |

## Usage

```bash
# Backtest US (default)
python3 deleveraging/backtest.py

# Backtest India
python3 deleveraging/backtest.py --preset india

# All exchanges
python3 deleveraging/backtest.py --global --output results/exchange_comparison.json --verbose

# Current screen (US)
python3 deleveraging/screen.py

# Generate charts (after running global backtest)
python3 deleveraging/generate_charts.py
```

## Signal Interpretation

**Healthy deleveraging** (what this strategy targets):
- Strong operating cash flow paying down debt
- Strategic decision to improve balance sheet
- ROE remains high (>8%) while reducing leverage

**Distressed deleveraging** (filtered out by ROE):
- Asset fire-sales to repay debt
- Equity dilution to retire debt
- ROE typically low or negative

## Notes

- Requires prior-year D/E data → first valid backtest periods are 2001 Q1
- Quarterly rebalancing captures companies as they file annual reports throughout the year
- Higher portfolio turnover than static screens (deleveraging is a temporary state)
- ASX and SAO excluded: adjClose split artifacts affect return calculation accuracy
- China (SHZ+SHH) and Korea (KSC) have thin qualifying universes historically
