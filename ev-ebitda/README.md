# EV/EBITDA Value Screen

Backtest of a low EV/EBITDA value strategy across global exchanges, 2000-2025.

**Signal:** EV/EBITDA < 10x, ROE > 10%, market cap > exchange threshold
**Portfolio:** Top 30 by lowest EV/EBITDA, equal weight. Cash if < 10 qualify.
**Rebalancing:** Annual (January)
**Costs:** Size-tiered transaction costs (from costs.py)

EV/EBITDA captures what P/E misses: debt. Enterprise Value = Market Cap + Net Debt, so the multiple reflects the total cost of acquiring the business, not just the equity. Private equity firms use EV/EBITDA as the primary valuation metric in M&A. Academic research (Gray & Vogel 2012, Loughran & Wellman 2011) confirms it outperforms P/E as a return predictor.

## Data Source

Ceta Research (FMP financial data warehouse). Annual FY financials from `key_metrics` table. Point-in-time with 45-day filing lag (no look-ahead bias).

## Usage

```bash
# US backtest (default)
python3 ev-ebitda/backtest.py

# India
python3 ev-ebitda/backtest.py --preset india

# Germany
python3 ev-ebitda/backtest.py --preset germany

# All exchanges
python3 ev-ebitda/backtest.py --global --output results/exchange_comparison.json --verbose

# No transaction costs (academic baseline)
python3 ev-ebitda/backtest.py --no-costs

# Live screen (current stocks)
python3 ev-ebitda/screen.py
python3 ev-ebitda/screen.py --preset india

# Generate charts
python3 ev-ebitda/generate_charts.py
```

## Results (2000-2025)

| Exchange | CAGR | Excess | Sharpe | MaxDD | Cash% | AvgStk |
|----------|------|--------|--------|-------|-------|--------|
| India (BSE+NSE) | 11.73% | +4.09% | 0.156 | -54.1% | 20% | 27.2 |
| US (NYSE+NASDAQ+AMEX) | 10.25% | +2.61% | 0.399 | -36.2% | 0% | 21.3 |
| Canada (TSX) | 9.80% | +2.16% | 0.400 | -45.6% | 0% | 26.0 |
| Sweden (STO) | 9.49% | +1.84% | 0.318 | -43.2% | 16% | 26.6 |
| Italy (MIL) | 8.12% | +0.47% | 0.211 | -47.2% | 20% | 18.4 |
| Germany (XETRA) | 7.71% | +0.06% | 0.257 | -50.3% | 0% | 19.2 |
| Switzerland (SIX) | 6.91% | -0.74% | 0.297 | -40.6% | 0% | 14.8 |
| China (SHZ+SHH) | 6.70% | -0.94% | 0.102 | -65.0% | 0% | 21.1 |
| South Africa (JNB) | 6.51% | -1.13% | -0.140 | -16.8% | 24% | 22.7 |
| Thailand (SET) | 5.52% | -2.12% | 0.134 | -46.0% | 20% | 27.4 |
| Norway (OSL) | 5.39% | -2.26% | 0.117 | -48.8% | 52% | 12.2 |
| Taiwan (TAI) | 4.86% | -2.78% | 0.156 | -44.3% | 24% | 27.7 |
| Israel (TLV) | 4.84% | -2.81% | 0.065 | -48.9% | 28% | 21.2 |
| Korea (KSC) | 4.45% | -3.19% | 0.079 | -31.6% | 24% | 26.2 |
| Hong Kong (HKSE) | 3.24% | -4.40% | 0.009 | -56.6% | 4% | 21.0 |
| Singapore (SES) | 1.42% | -6.22% | -0.028 | -45.4% | 24% | 10.9 |

SPY benchmark: 7.64% CAGR.

## Exclusions

| Exchange | Reason |
|----------|--------|
| ASX (Australia) | adjClose price data artifacts — fatal data quality issue |
| SAO (Brazil) | adjClose price data artifacts — fatal data quality issue |
| JPX (Japan) | FY data confirmed present (2026-03-09). adjClose quality unverified — excluded pending price data check. |
| LSE (UK) | FY data confirmed present (2026-03-09). adjClose quality unverified — excluded pending price data check. |

See `backtests/DATA_QUALITY_ISSUES.md` for full documentation.

## Signal Notes

- **EV/EBITDA threshold (< 10)**: S&P 500 historical average ranges 11-17x. Below 10 is meaningfully cheap.
- **ROE filter (> 10%)**: Avoids value traps — companies with positive EBITDA but no real earnings power.
- **Market cap filter**: Exchange-specific thresholds (see `cli_utils.MKTCAP_THRESHOLD_MAP`). Targets $200-500M USD-equivalent to ensure liquid mid-to-large cap stocks.
- **Excluded financials**: Not a hard filter in this backtest. Banks/insurers typically have negative EV/EBITDA (not applicable) and are naturally filtered out by the EV/EBITDA > 0 condition.
- **Look-ahead bias prevention**: 45-day lag applied. Data from annual filing dated before (rebalance_date - 45 days).

## References

- Gray, W. & Vogel, J. (2012). "Analyzing Valuation Measures." *Journal of Portfolio Management*, 39(1), 112-121.
- Loughran, T. & Wellman, J. (2011). "New Evidence on the Enterprise Multiple." *JFQA*, 46(6), 1629-1650.
