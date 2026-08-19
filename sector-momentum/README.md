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

Excess and alpha are measured against each exchange's LOCAL index, not SPY.

| Exchange | CAGR | Benchmark | Excess | Alpha | Beta | Sharpe | MaxDD | Cash | Avg Stocks |
|---|---|---|---|---|---|---|---|---|---|
| NSE (India) | 21.45% | Sensex 11.24% | +10.21% | +8.88% | 1.28 | 0.411 | -66.3% | 0/104 | 95.6 |
| KSC (Korea) | 19.76% | KOSPI 5.55% | +14.21% | +14.29% | 0.97 | 0.585 | -39.9% | 4/104 | 54.6 |
| TSX (Canada) | 16.76% | TSX Composite 5.26% | +11.51% | +11.06% | 1.16 | 0.591 | -49.0% | 0/104 | 64.3 |
| SET (Thailand) | 13.03% | SET Index 3.69% | +9.34% | +9.62% | 0.76 | 0.466 | -39.7% | 6/104 | 22.7 |
| HKSE (HK) | 12.48% | Hang Seng 1.61% | +10.87% | +10.94% | 1.05 | 0.333 | -62.5% | 4/104 | 65.6 |
| STO (Sweden) | 11.79% | OMX Stockholm 30 3.39% | +8.41% | +8.54% | 0.90 | 0.456 | -56.1% | 4/104 | 27.7 |
| LSE (UK) | 11.09% | FTSE 100 1.55% | +9.54% | +9.39% | 0.93 | 0.433 | -51.6% | 0/104 | 69.0 |
| NYSE_NASDAQ_AMEX (US) | 10.96% | S&P 500 8.02% | +2.94% | +3.08% | 0.98 | 0.438 | -39.0% | 0/104 | 308.4 |
| TAI_TWO (Taiwan) | 10.85% | TAIEX 4.76% | +6.08% | +7.29% | 0.68 | 0.432 | -49.6% | 4/104 | 130.3 |
| XETRA (Germany) | 9.41% | DAX 5.19% | +4.22% | +5.20% | 0.69 | 0.390 | -60.5% | 0/104 | 59.4 |
| JPX (Japan) | 8.73% | Nikkei 225 3.93% | +4.80% | +6.05% | 0.67 | 0.461 | -47.5% | 4/104 | 122.9 |
| SIX (Switzerland) | 7.71% | SMI 2.31% | +5.40% | +5.37% | 1.01 | 0.402 | -50.2% | 5/104 | 35.9 |
| SHH_SHZ (China) | 5.50% | SSE Composite 4.24% | +1.26% | +1.36% | 0.94 | 0.093 | -73.4% | 0/104 | 319.5 |

## Key Findings

- **All 13 markets beat their local benchmark on both excess return and Jensen alpha.**
- **Korea: largest excess (+14.21%) and largest alpha (+14.29%)**, achieved at a beta below 1.0 with a 59.5% down capture and a shallower drawdown than the KOSPI.
- **Canada: best Sharpe (0.591)** and the largest excess of the developed-market set (+11.51%), driven by Basic Materials (38/104 quarters) and Energy (37/104).
- **India: highest CAGR (21.45%)** and the study's largest single-year margin (+97.52% over the Sensex in 2003), but the only market alongside China whose down capture exceeds 100%. Beta of 1.28 means excess overstates alpha by 1.3 points.
- **Japan: lowest down capture (45.4%) and lowest up capture (82.4%).** Low-beta profile, shallower drawdown than the Nikkei.
- **Switzerland is not a failure.** The earlier -4.54% reading came from measuring CHF returns against SPY. Against the SMI it returns +5.40% excess.
- **China is last on CAGR, excess, alpha, Sharpe and Sortino,** with the deepest drawdown (-73.4%).
- **US sector frequency:** Energy (33), Healthcare (31), Basic Materials (26) lead. Financial Services never reached the top 2 in 26 years, partly because FMP files closed-end funds and ETFs under that sector, which drags its equal-weighted score toward the market average.

## Data Notes

- **Excluded:** ASX (adjClose split artifacts), SAO/Brazil (same), SES/Singapore (61% cash, insufficient sector diversity)
- **JNB/South Africa dropped 2026-08-19.** The signal needs 5 sectors each holding 5+ stocks with a valid 12-month return. The JNB large-cap universe never reaches that before 2017, so 85 of 104 quarters force to cash and only 2018-2025 is investable. Same shape as the SES exclusion. See DATA_QUALITY_ISSUES.md.
- **Data quality guards:** `remove_price_oscillations()` strips phantom holiday rows and broken split adjustments before any price lookup. `filter_returns()` removes entry prices below 0.50 and caps individual single-period returns at 200%.
- **Execution:** entry at the next-day close after each rebalance date (`offset_days=1`). Pass `--no-next-day` for the old same-day behaviour.
- **Costs:** size-tiered one-way (0.1% >$10B, 0.3% $2-10B, 0.5% <$2B) with thresholds converted to local currency via `costs.get_fx_per_usd`, so non-US names are charged their true size tier.
- **Opt-in flags:** `--domicile-filter` restricts to companies headquartered in the exchange's home country; `--exclude-funds` drops isFund/isEtf rows. Both default OFF, and published figures use the listed, funds-included universe.
- Sector assignments from FMP `profile` table (current snapshot, static over backtest)
- Price data from `stock_eod` (adjClose, quarter-start windows only)

## Rebuilding the comparison file

`--global` writes both the per-exchange `returns_*.json` and the combined
`exchange_comparison.json`. A market that dies mid-run on a transient parquet error
leaves an `{"error": ...}` entry in the combined file. Re-run that market alone, then:

```bash
python3 scripts/rebuild_comparison.py sector-momentum          # fold per-exchange files back in
python3 scripts/rebuild_comparison.py sector-momentum --check  # gate before publishing
```
