# GARP: Growth at a Reasonable Price

**Factor:** Growth-Value Hybrid | **Rebalancing:** Quarterly | **Coverage:** 17 exchanges

## Strategy

GARP (Growth at a Reasonable Price) identifies companies that are growing fast enough to justify their valuations. The signal uses the PEG ratio — P/E divided by earnings growth rate — as a single number that prices growth into valuation.

A PEG below 1.5 means you're paying at most 1.5x the company's growth rate in P/E terms. Combined with a revenue growth requirement (>15% YoY), the screen targets companies that are demonstrably growing and still priced reasonably relative to that growth.

**Academic basis:** Peter Lynch, *One Up on Wall Street* (1989). Lynch coined the GARP approach and popularized the PEG ratio as a quick way to evaluate whether you're overpaying for growth.

## Signal

All conditions must be met at rebalance date (45-day lag on annual filings):

| Filter | Value | Rationale |
|--------|-------|-----------|
| PEG ratio | 0 < PEG < 1.5 | Not paying more than 1.5x the growth rate |
| P/E ratio | 5 < P/E < 50 | Exclude distressed (<5) and speculative (>50) |
| Revenue growth | > 15% YoY | Must be demonstrably growing |
| ROE | > 10% | Quality filter — profitable on equity |
| Debt/Equity | < 2.0 | Not excessively leveraged |
| Market cap | Exchange-specific | Liquid mid-to-large cap only |

**Portfolio construction:** Top 30 by lowest PEG ratio, equal weight. Cash if fewer than 10 qualify.

**Transaction costs:** Size-tiered model from `costs.py`. Applied on entry and exit per period.

## Usage

```bash
# US stocks (default)
python3 garp/backtest.py

# Indian stocks
python3 garp/backtest.py --preset india

# All exchanges
python3 garp/backtest.py --global --output results/exchange_comparison.json

# Current stock screen
python3 garp/screen.py
python3 garp/screen.py --preset india

# Without transaction costs (academic baseline)
python3 garp/backtest.py --no-costs
```

## Results Summary (2000-2025)

| Exchange | CAGR | vs SPY | Sharpe | MaxDD | Cash% | Avg Stocks |
|----------|------|--------|--------|-------|-------|------------|
| India (BSE+NSE) | 11.12% | +3.11% | 0.131 | -72.67% | 20% | 26.9 |
| Germany (XETRA) | 7.50% | -0.51% | 0.266 | -47.52% | 0% | 19.7 |
| South Africa (JNB) | 7.15% | -0.86% | -0.090 | -45.46% | 31% | 15.6 |
| US (NYSE+NASDAQ+AMEX) | 7.12% | -0.89% | 0.221 | -57.61% | 0% | 25.7 |
| Indonesia (JKT) | 6.16% | -1.85% | 0.184 | -43.19% | 32% | 24.9 |
| Canada (TSX) | 5.80% | -2.21% | 0.142 | -69.99% | 0% | 24.7 |
| UK (LSE) | 5.36% | -2.65% | 0.082 | -53.54% | 0% | 21.9 |
| Sweden (STO) | 4.91% | -3.10% | 0.132 | -60.25% | 32% | 21.7 |
| China (SHZ+SHH) | 4.53% | -3.48% | 0.055 | -71.11% | 0% | 25.9 |
| Korea (KSC) | 4.06% | -3.95% | 0.064 | -41.65% | 33% | 27.4 |
| Thailand (SET) | 3.81% | -4.20% | 0.053 | -56.89% | 20% | 26.0 |
| Switzerland (SIX) | 3.78% | -4.23% | 0.168 | -67.71% | 5% | 15.7 |
| Japan (JPX) | 3.19% | -4.82% | 0.143 | -67.77% | 23% | 27.5 |
| Hong Kong (HKSE) | 2.88% | -5.13% | -0.004 | -70.25% | 3% | 20.1 |
| Norway (OSL) | 2.40% | -5.61% | -0.039 | -50.04% | 71% | 11.3 |
| Taiwan (TAI+TWO) | 1.55% | -6.47% | 0.029 | -53.93% | 28% | 28.4 |
| Singapore (SES) | 0.71% | -7.30% | -0.091 | -64.96% | 21% | 10.6 |

*SPY benchmark: 8.01% CAGR, -45.53% MaxDD, Sharpe 0.354 over same period.*

**Key finding:** India is the only market where GARP meaningfully outperforms (+3.11% excess CAGR). Growth-oriented screens consistently underperform value benchmarks in developed markets, particularly in the post-2012 era when growth stocks became expensive relative to their growth rates.

## Data Notes

- **China 2007:** Annual return of +223.9% (portfolio) vs +4.4% (SPY). China's Shanghai Composite rose ~97% in 2007 during the pre-Olympics bull market. The GARP screen, concentrated in high-growth stocks, amplified this significantly. This is the speculative bubble effect, not a data artifact.
- **Norway:** 71% cash periods. The GARP signal rarely fires historically — few Norwegian companies combine PEG < 1.5, ROE > 10%, and 15%+ revenue growth. Comparison-only exchange.
- **ASX/SAO:** Excluded (adjusted close price artifacts, per `DATA_QUALITY_ISSUES.md`).

## Files

```
garp/
  backtest.py          # Full historical backtest (2000-2025, all exchanges)
  screen.py            # Current stock screen (TTM data)
  generate_charts.py   # Chart generation from results/exchange_comparison.json
  README.md            # This file
  results/
    exchange_comparison.json   # All exchanges combined
    returns_*.json             # Per-exchange detailed results
```

## Methodology

See `backtests/METHODOLOGY.md` for complete details on:
- Data sources and point-in-time compliance (45-day filing lag)
- Transaction cost model
- Return calculation and benchmark
- Survivorship bias handling
