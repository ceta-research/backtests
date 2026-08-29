# Post-Stock Split Performance

Event study measuring cumulative abnormal returns (CAR) around forward stock splits, 2000-2025.

**Key finding:** Stocks gain +1.3% vs SPY in the 5 days *before* a split (t=3.8). After the split, with next-day MOC entry, there is no significant drift in either direction (+0.58% at T+252, t=0.72). Standard 2-for-1 splits outperform mildly at one year (+2.58%, t=2.45); the apparent -11.7% underperformance of 5-for-1+ splits decomposes into fund share splits and mis-encoded records, not a tradable effect. The traditional "buy on split" signal does not hold in 2000-2025 US data, and neither does "avoid extreme splits."

---

## Strategy

**Type:** Event study (not a portfolio backtest)
**Data:** FMP splits_calendar + stock_eod + key_metrics via Ceta Research API
**Universe:** US listings (NYSE/NASDAQ/AMEX) with market cap > $500M, forward splits only
**Period:** 2000-2025 (1,968 events with complete price data)
**Benchmark:** SPY
**Execution:** MOC — the base price is the close of the trading day *after* the effective split date, so T+1 through T+252 measure only what you could capture after realistic entry

### Academic Basis

- Fama, Fisher, Jensen & Roll (1969) documented positive post-split abnormal returns in *International Economic Review*
- Ikenberry, Rankine & Stice (1996) confirmed 7.9% abnormal first-year returns for 2:1 splits in *JFQA*
- Our 2000-2025 data, with next-day entry, finds no significant post-split drift in either direction

---

## Results Summary

Numbers below are the published run (the one the [blog post](https://blog.tradingstudio.finance/stock-split-performance-us-backtest/) quotes), from `results/summary_metrics.json`:

| Window | Mean CAR | t-stat | N |
|--------|----------|--------|----|
| T-5 (pre-split) | +1.31% | 3.81 | 1,953 |
| T+5 | -0.25% | -1.76 | 1,965 |
| T+21 | -0.42% | -1.47 | 1,965 |
| T+63 | -0.64% | -1.54 | 1,958 |
| T+126 | +0.38% | 0.66 | 1,932 |
| T+252 | +0.58% | 0.72 | 1,918 |

By ratio at one year: 2-for-1 +2.58% (t=2.45, n=1,046); 5-for-1+ -11.67% (t=-4.00, n=207).

**The 5-for-1+ number is composition, not signal.** A 2026-08 audit decomposed it: 102 of the 207
events are fund/ETF share splits and 11 more sit above a 25:1 ratio, a stratum dominated by
mis-encoded reverse splits and IPO share conversions. The 86 genuine common-stock splits in the
bucket return -7.14% (t=-1.47), not significant; US-domiciled names alone, -2.16% (t=-0.37).
`backtest.py` now guards both groups by default (`--include-funds` and `--max-ratio` restore the
old universe), so a fresh run produces a smaller, cleaner sample than the committed artifacts —
on the guarded universe the 5-for-1+ bucket is -0.92% (t=-0.21, n=84). See
`RESULTS_PROVENANCE.md` before quoting anything from `results/`.

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
