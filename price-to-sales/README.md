# Price-to-Sales (P/S) Value Screen

**Category:** Value
**Rebalancing:** Quarterly (Jan/Apr/Jul/Oct)
**Period:** 2000-2025
**Signal:** P/S < 1.0 + quality filters

---

## Strategy

Screens for stocks trading below 1x revenue with quality filters that exclude unprofitable companies. Kenneth Fisher popularized this metric in *Super Stocks* (1984). Academic research (Barbee et al. 1996, Gray & Vogel 2012) confirms the P/S effect persists after controlling for size and book-to-market.

**Signal:**
- Price-to-Sales < 1.0 (below 1x revenue)
- Gross Profit Margin > 20% (excludes thin-margin businesses)
- Operating Margin > 5% (operationally profitable)
- Return on Equity > 10% (quality filter)
- Market cap > local currency threshold (exchange-specific)

**Why quality filters?** Raw P/S screens fill with retailers, commodity distributors, and industrials operating on 1-2% margins. A company with $1B revenue and $1.05B in costs looks identical to one with $1B revenue and $800M in costs. The gross and operating margin filters eliminate businesses where low P/S reflects genuinely low-quality economics.

**Portfolio:** Top 30 by lowest P/S, equal weight. Hold cash if fewer than 10 qualify.

---

## Usage

```bash
# US stocks (default)
python3 price-to-sales/backtest.py

# Indian stocks
python3 price-to-sales/backtest.py --preset india

# All exchanges
python3 price-to-sales/backtest.py --global --output results/exchange_comparison.json

# Live screen (current qualifying stocks)
python3 price-to-sales/screen.py
python3 price-to-sales/screen.py --preset india

# Generate charts (after running backtest)
python3 price-to-sales/generate_charts.py
```

---

## Academic References

- Fisher, K. (1984). *Super Stocks.* Dow Jones-Irwin.
- Barbee, W., Mukherji, S. & Raines, G. (1996). "Do Sales-Price and Debt-Equity Explain Stock Returns Better than Book-Market and Firm Size?" *Financial Analysts Journal*, 52(2), 56-60.
- Gray, W. & Vogel, J. (2012). "Analyzing Valuation Measures: A Performance Horse-Race over the Past 40 Years." *Journal of Portfolio Management*, 39(1), 112-121.
- Novy-Marx, R. (2013). "The Other Side of Value: The Gross Profitability Premium." *Journal of Financial Economics*, 108(1), 1-28.

---

## Data Notes

- `priceToSalesRatio` from `financial_ratios` table (FY data, confirmed)
- `grossProfitMargin`, `operatingProfitMargin` from `financial_ratios` table (FY)
- `returnOnEquity`, `marketCap` from `key_metrics` table (FY)
- TTM screen uses `financial_ratios_ttm` and `key_metrics_ttm` tables
- 45-day lag applied to all fundamental data (point-in-time integrity)
- Market cap thresholds per exchange via `cli_utils.get_mktcap_threshold()`

---

## Files

```
price-to-sales/
├── backtest.py          # Full historical backtest
├── screen.py            # Live stock screen (TTM data)
├── generate_charts.py   # Chart generation from results
├── README.md            # This file
├── results/             # Backtest results (JSON + CSV)
└── charts/              # Generated charts (PNG)
```
