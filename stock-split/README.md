# Post-Stock Split Performance

Event study measuring cumulative abnormal returns (CAR) around forward stock splits, 2000-2025.

**Key finding:** Stocks gain +3.2% vs SPY in the 5 days *before* a split. After the split, they underperform by -3.1% over 126 trading days. The traditional "buy on split" signal does not hold in 2000-2025 US data.

---

## Strategy

**Type:** Event study (not a portfolio backtest)
**Data:** FMP splits_calendar + stock_eod + key_metrics via Ceta Research API
**Universe:** US stocks with market cap > $500M, forward splits only (numerator > denominator)
**Period:** 2000-2025 (20,119 events)
**Benchmark:** SPY

### Academic Basis

- Fama, Fisher, Jensen & Roll (1969) documented positive post-split abnormal returns in *International Economic Review*
- Ikenberry, Rankine & Stice (1996) confirmed 7.9% abnormal first-year returns for 2:1 splits in *JFQA*
- Our 2000-2025 data finds the opposite: negative post-split drift, significant at p<0.01

---

## Results Summary

| Window | Mean CAR | t-stat | N |
|--------|----------|--------|----|
| T-5 (pre-split) | +3.22% | 14.22 | 18,938 |
| T+1 | +0.42% | 3.78 | 19,521 |
| T+5 | -0.32% | -2.12 | 19,282 |
| T+21 | -1.49% | -7.28 | 19,221 |
| T+63 | -2.52% | -9.58 | 19,177 |
| T+126 | -3.06% | -9.45 | 19,018 |
| T+252 | -2.98% | -6.64 | 18,853 |

Higher split ratios (5-for-1+) underperform more: -7.37% at T+252 vs -0.42% for 2-for-1.

---

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Full event study (fetch data via API, compute CAR) |
| `screen.py` | Live screen for recent forward stock splits |
| `generate_charts.py` | Generate charts from results JSON |
| `results/` | Output from backtest.py (generated) |

---

## Usage

```bash
# Set API key
export CR_API_KEY="your-key-here"  # get at cetaresearch.com

# Run event study (all exchanges, 2000-2025, ~15-30 min first run)
python3 stock-split/backtest.py --output stock-split/results --verbose

# Run with $1B+ market cap filter
python3 stock-split/backtest.py --min-mktcap 1000000000 --verbose

# Screen for recent splits
python3 stock-split/screen.py --days 90

# Screen: $1B+ companies, 2-for-1 and above
python3 stock-split/screen.py --days 180 --min-mktcap 1000000000 --min-ratio 2.0

# Generate charts
python3 stock-split/generate_charts.py
```

---

## Data Notes

- `splits_calendar` covers US stocks extensively; non-US coverage is limited in FMP
- `adjClose` in `stock_eod` is adjusted for both splits and dividends
- The effective split date (not announcement date) is used as T0
- Pre-split returns (T-5) likely capture announcement-period drift since companies announce weeks before the effective date

---

*Data: Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)*
