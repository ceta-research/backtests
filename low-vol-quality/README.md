# Low Volatility + Quality

Quarterly-rebalanced portfolio of the 30 lowest-volatility stocks that pass quality filters (ROE > 10%, operating margin > 10%).

## Strategy

**Signal:** Quality filter + lowest 252-day realized volatility
**Selection:** Top 30 by lowest vol, equal weight
**Rebalancing:** Quarterly (Jan/Apr/Jul/Oct)
**Universe:** All stocks on the exchange with market cap above exchange-specific threshold

### Filters

| Filter | Threshold | Source |
|--------|-----------|--------|
| Return on equity | > 10% | key_metrics FY |
| Operating profit margin | > 10% | financial_ratios FY |
| Market cap | > exchange threshold | key_metrics FY |
| 252-day realized volatility | Rank ASC, top 30 | stock_eod daily returns |
| Minimum trading days | >= 200 in lookback | stock_eod |

### Volatility Computation

Annualized volatility = STDDEV(daily log returns) * SQRT(252)

Uses 14-month lookback window (~252+ trading days). Log returns computed as LN(adjClose_t / adjClose_t-1). Stocks with fewer than 200 trading days in the window are excluded.

## Academic References

- Baker, Bradley & Wurgler (2011) "Benchmarks as Limits to Arbitrage" - explains why low-vol anomaly persists
- Ang, Hodrick, Xing & Zhang (2006) "The Cross-Section of Volatility and Expected Returns" - documents anomaly across 23 markets
- Frazzini & Pedersen (2014) "Betting Against Beta" - leverage constraints create low-beta premium
- Novy-Marx (2013) "The Other Side of Value" - profitability as quality overlay

## Usage

```bash
# Run backtest (US)
python3 low-vol-quality/backtest.py --preset us --output low-vol-quality/results/returns_US.json --verbose

# Run backtest (all exchanges)
python3 low-vol-quality/backtest.py --global --output low-vol-quality/results/exchange_comparison.json --verbose

# Current screen
python3 low-vol-quality/screen.py --preset us

# Generate charts
python3 low-vol-quality/generate_charts.py
```

## Results

14 exchanges tested. Key findings:

| Exchange | CAGR | Excess | Sharpe | MaxDD |
|----------|------|--------|--------|-------|
| India (BSE+NSE) | 18.0% | +9.9% | 0.675 | -24.4% |
| China (SHZ+SHH) | 11.0% | +3.0% | 0.325 | -53.5% |
| Canada (TSX) | 9.8% | +1.8% | 0.642 | -28.7% |
| US (NYSE+NAS+AMEX) | 6.2% | -1.8% | 0.419 | -27.1% |

Full results in `results/exchange_comparison.json`.

Low-vol anomaly confirmed: US Sharpe 0.419 vs SPY 0.354. MaxDD -27.1% vs SPY -45.5%. Bear market alpha in 2000 (+39%), 2002 (+24%), 2008 (+19%), 2020 (+9%).

## Files

```
low-vol-quality/
├── backtest.py          # Full historical backtest
├── screen.py            # Current TTM screen
├── generate_charts.py   # Chart generation from results
├── README.md            # This file
├── results/
│   ├── exchange_comparison.json
│   └── returns_*.json   # Per-exchange results
└── charts/              # Generated charts (gitignored)
```
