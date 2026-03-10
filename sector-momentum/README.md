# Sector Momentum Rotation

**Slug:** `sector-01-rotation`
**Type:** Sector · Momentum
**Rebalancing:** Quarterly (Jan, Apr, Jul, Oct)
**Universe:** Exchange-filtered by market cap threshold (local currency)
**Signal:** Pure price — no fundamental data required

## Strategy

At each quarterly rebalance, rank all sectors by equal-weighted 12-month trailing return. Buy all qualifying stocks in the top 2 sectors. Hold cash if fewer than 5 sectors have 5+ valid stocks, or fewer than 10 stocks pass data quality filters.

Academic basis: Moskowitz & Grinblatt (1999), *"Do Industries Explain Momentum?"* document that industry-level momentum explains a substantial portion of individual stock momentum. Buying top-performing industries captures the persistence of sector trends driven by earnings cycles, capital flows, and analyst attention.

## Parameters

| Parameter | Value |
|---|---|
| Sectors selected | Top 2 by 12-month trailing return |
| Lookback | 12 months |
| Rebalancing | Quarterly |
| Weighting | Equal weight |
| Min sector stocks | 5 (else sector excluded) |
| Min qualifying sectors | 5 (else cash) |
| Min portfolio stocks | 10 (else cash) |
| Market cap | Exchange-specific (local currency) |
| Transaction costs | Size-tiered: 0.1% (>10B), 0.3% (2-10B), 0.5% (<2B), one-way |

## Usage

```bash
# Single exchange
python3 sector-momentum/backtest.py --preset us
python3 sector-momentum/backtest.py --preset india
python3 sector-momentum/backtest.py --preset korea

# All exchanges
python3 sector-momentum/backtest.py --global --output sector-momentum/results/exchange_comparison.json

# Current screen (what to buy today)
python3 sector-momentum/screen.py --preset us

# Top 3 sectors
python3 sector-momentum/backtest.py --preset us --n-best 3
```

## Results (2000-2025)

| Exchange | CAGR | Excess | Sharpe | MaxDD | Cash% | Avg Stocks |
|---|---|---|---|---|---|---|
| BSE_NSE (India) | 26.95% | +18.93% | 0.560 | -63.9% | 3% | 186.7 |
| KSC (Korea) | 24.44% | +16.41% | 0.747 | -39.9% | 7% | 55.2 |
| TSX (Canada) | 18.25% | +10.23% | 0.612 | -50.2% | 3% | 70.7 |
| SET (Thailand) | 16.55% | +8.53% | 0.674 | -35.9% | 9% | 26.2 |
| HKSE (HK) | 13.22% | +5.20% | 0.356 | -65.0% | 7% | 66.3 |
| XETRA (Germany) | 13.07% | +5.05% | 0.620 | -52.9% | 3% | 64.6 |
| LSE (UK) | 12.71% | +4.69% | 0.454 | -54.0% | 3% | 75.5 |
| TAI_TWO (Taiwan) | 12.57% | +4.54% | 0.495 | -48.4% | 7% | 124.6 |
| STO (Sweden) | 11.40% | +3.37% | 0.444 | -55.8% | 7% | 29.6 |
| JPX (Japan) | 11.26% | +3.24% | 0.605 | -46.3% | 7% | 134.0 |
| NYSE_NASDAQ_AMEX (US) | 10.42% | +2.40% | 0.405 | -42.0% | 3% | 315.8 |
| JNB (S.Africa) | 9.34% | +1.31% | 0.011 | -62.0% | 12% | 16.7 |
| SHH_SHZ (China) | 8.03% | +0.01% | 0.166 | -70.4% | 3% | 325.3 |
| SIX (Switzerland) | 3.48% | -4.54% | 0.170 | -58.9% | 4% | 36.5 |

SPY reference: 8.02% CAGR

## Key Findings

- **Emerging and Asian markets dominate.** India (+18.9%), Korea (+16.4%), Canada (+10.2%), Thailand (+8.5%) show the largest excess CAGR. Developed Western markets show moderate alpha.
- **Korea: best risk-adjusted result.** Sharpe 0.747, MaxDD only -39.9%, down capture 16.5% — the strategy barely loses when SPY falls.
- **Thailand: near-zero down capture.** Only 8.0% down capture means the strategy is largely uncorrelated with US market drawdowns.
- **US Healthcare, Energy, Basic Materials** dominate sector frequency (top 2 most often). Technology appears during tech bull markets; defensive sectors appear during stress.
- **Switzerland underperforms** (-4.54% excess). Technology dominates Swiss sector frequency (42/104 quarters) — momentum in a concentrated tech-heavy market doesn't rotate enough to add value.
- **China and South Africa: no meaningful alpha.** China matches SPY CAGR exactly (+0.01% excess) with extreme volatility. South Africa has too few stocks for reliable sector rotation (16.7 avg).

## Data Notes

- **Excluded:** ASX (adjClose split artifacts), SAO/Brazil (same), SES/Singapore (61% cash — insufficient sector diversity)
- **Data quality:** `filter_returns()` removes penny stocks (<$0.50), caps individual returns at 200%
- Sector assignments from FMP `profile` table (current snapshot, static over backtest)
- Price data from `stock_eod` (adjClose, quarter-start windows only)
