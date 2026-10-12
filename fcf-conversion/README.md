# FCF Conversion Quality

Systematic quality screen based on free cash flow conversion rate.

## Signal

Companies that convert a high percentage of reported earnings into actual free cash flow, with quality filters to eliminate statistical noise.

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| FCF / Net Income | 100% - 300% | Cash backs reported earnings. Cap at 300% filters tiny-NI noise |
| FCF / Revenue | > 10% | Ensures meaningful absolute cash flow, not ratio math alone |
| Return on Equity | > 10% | Profitable business, not high conversion on a low base alone |
| Operating Margin | > 10% | Real pricing power, sustainable cash generation |
| Net Income | > 0 | Negative denominators make ratio meaningless |
| Free Cash Flow | > 0 | Only companies generating cash |
| Market Cap | > exchange threshold | Liquid stocks only |

**Selection:** Top 30 by highest FCF conversion, equal weight.
**Rebalancing:** Annual (July). Cash if fewer than 10 stocks qualify, or fewer than 10 of them have a usable entry price.

## Backtest Setup

| Parameter | Value |
|-----------|-------|
| Period | 25 annual rebalances, July 2000 to July 2024, each held one year (last exit July 2025) |
| Financial data lag | 45 days before each rebalance |
| Execution | Next-day close after the rebalance date |
| Costs | Size-tiered transaction costs |
| Entry floor | Entry price of at least 1.0 in the exchange's price unit (pence on LSE) |
| Return filter | A single-stock return above 200% is dropped as a likely data error |
| Missing exit price | Last traded price on or before the exit date |
| Benchmark | Local index per market, in local currency (S&P 500 in USD for Saudi Arabia) |

## Results (2000-2025)

The screen beat its local index in 10 of 15 markets. In the US it returned 9.91% a year against 7.85% for the S&P 500, with a Sharpe ratio of 0.402 against 0.352 and a slightly deeper maximum drawdown (-39.76% against -38.01%). India had the highest CAGR (14.94% against the Sensex's 12.06%) and Hong Kong the largest margin over its index (+5.78 percentage points). It trailed in Taiwan, Thailand, Germany, Brazil and Korea. Saudi Arabia is shown but not counted: there's no local index series, so it runs against the S&P 500 in USD.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max drawdown | Cash years | Avg stocks |
|---|---|---|---|---|---|---|---|---|
| Hong Kong (HKSE) | Hang Seng | 7.42% | 1.64% | +5.78 | 0.241 | -37.97% | 3 of 25 | 17.5 |
| UK (LSE) | FTSE 100 | 6.12% | 1.23% | +4.89 | 0.158 | -30.92% | 5 of 25 | 19.4 |
| Canada (TSX) | TSX Composite | 8.11% | 3.95% | +4.16 | 0.357 | -24.32% | 4 of 25 | 21.9 |
| China (SHZ, SHH) | SSE Composite | 6.01% | 2.43% | +3.58 | 0.099 | -43.71% | 0 of 25 | 20.0 |
| Switzerland (SIX) | SMI | 4.83% | 1.74% | +3.09 | 0.362 | -26.82% | 12 of 25 | 14.6 |
| India (NSE) | Sensex | 14.94% | 12.06% | +2.88 | 0.403 | -12.81% | 6 of 25 | 20.0 |
| Australia (ASX) | ASX 200 | 6.76% | 3.89% | +2.86 | 0.217 | -24.13% | 9 of 25 | 20.2 |
| US (NYSE, NASDAQ, AMEX) | S&P 500 | 9.91% | 7.85% | +2.06 | 0.402 | -39.76% | 0 of 25 | 24.0 |
| Japan (JPX) | Nikkei 225 | 4.99% | 3.31% | +1.68 | 0.261 | -41.64% | 6 of 25 | 24.7 |
| Sweden (STO) | OMX Stockholm 30 | 3.84% | 2.55% | +1.29 | 0.147 | -19.98% | 18 of 25 | 19.6 |
| Taiwan (TAI) | TAIEX | 3.56% | 4.09% | -0.53 | 0.199 | -24.16% | 7 of 25 | 24.1 |
| Thailand (SET) | SET Index | 3.69% | 5.13% | -1.45 | 0.088 | -39.26% | 9 of 25 | 19.2 |
| Germany (XETRA) | DAX | 0.21% | 5.04% | -4.83 | -0.120 | -48.00% | 12 of 25 | 14.9 |
| Brazil (SAO) | Bovespa | 2.93% | 8.70% | -5.77 | -0.653 | -13.07% | 13 of 25 | 14.0 |
| Korea (KSC) | KOSPI | -1.71% | 5.35% | -7.06 | -0.675 | -43.22% | 18 of 25 | 12.6 |
| Saudi Arabia (SAU) | S&P 500 (USD, not counted) | 1.43% | 7.85% | -6.42 | -0.137 | -33.21% | 10 of 25 | 15.7 |

Returns are in local currency. A cash year earns 0% and still counts in the CAGR. Avg stocks is the average book size in invested years; it sits below 30 when fewer than 30 names pass the screen or have a usable entry price, or when a realised return above 200% is dropped (29 names across 19 invested years).

Nine of the ten wins are against price indices that leave dividends out; the S&P 500 is the only total-return benchmark among them. Those margins are overstated by roughly each index's dividend yield, and the thinnest, Sweden (+1.29) and Japan (+1.68), may not survive a total-return comparison. The same omission flatters the Sharpe comparison below. The DAX and Bovespa include dividends, so two of the five losses are against total-return series. Details in [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations) (Benchmark Limitations).

The Sharpe ratio beat the benchmark's in 11 of 15 markets, Taiwan included despite its lower CAGR, and the maximum drawdown was shallower in 12 of 15. Read both against the cash column. Sweden and Korea held cash in 18 of 25 years, Brazil in 13, Germany and Switzerland in 12, and a flat 0% year damps volatility and can't add to a drawdown. Those markets' figures rest on 7 to 13 invested years.

Cash years by market (the July rebalance that went to cash):

| Market | Cash rebalances (July of) |
|---|---|
| Korea (KSC) | 2000-2007, 2009-2015, 2018-2020 |
| Sweden (STO) | 2000-2009, 2011-2015, 2017, 2019, 2023 |
| Brazil (SAO) | 2000-2007, 2010-2012, 2015, 2022 |
| Germany (XETRA) | 2000-2006, 2009, 2012, 2014-2016 |
| Switzerland (SIX) | 2000-2009, 2012, 2019 |
| Saudi Arabia (SAU) | 2000-2007, 2009, 2019 |
| Australia (ASX) | 2000-2005, 2007, 2008, 2010 |
| Thailand (SET) | 2000-2005, 2007-2009 |
| Taiwan (TAI) | 2000-2006 |
| India (NSE) | 2000-2003, 2005, 2009 |
| Japan (JPX) | 2000-2004, 2009 |
| UK (LSE) | 2000-2004 |
| Canada (TSX) | 2000-2003 |
| Hong Kong (HKSE) | 2000-2002 |

The US and China were invested in all 25 years.

## Rerun October 2026

These results replace the previously published set. Three things changed, all described in [METHODOLOGY.md](../METHODOLOGY.md):

- **Last-traded-price exits.** A holding with no price in the 10-day window after the exit date now exits at its last traded price instead of being dropped from the period. 28 holdings across the 16 markets exited this way, and none was left without an exit price. See [Survivorship Bias](../METHODOLOGY.md#survivorship-bias).
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only. See [Universe Construction](../METHODOLOGY.md#universe-construction).
- **Same-ISIN dedup.** On every market, a security listed under more than one ticker (same ISIN) is held once, before the top-30 cut. Share classes have their own ISINs and can both be held: the US book holds both Wiley classes, WLY and WLYB, in 5 of its 25 years.

Separately, selection is now deterministic: ties at the top-30 cut break on the symbol (see [Reproducibility](../METHODOLOGY.md#reproducibility)).

The three markets under the listing rules moved the most. Germany fell from 6.88% to 0.21% CAGR and now trails the DAX; its cash years rose from 3 to 12. The UK rose from -0.30% to 6.12% and now beats the FTSE 100; its cash years fell from 17 to 5. Switzerland fell from 5.64% to 4.83% and still beats the SMI; its cash years rose from 9 to 12. Germany and Switzerland had no last-traded-price exits, so their moves come from the changes to the universe and selection, not from exits. The UK had 2 of the 28 last-traded-price exits. Every other market moved by 0.6 percentage points of CAGR or less. The count of markets beating their index is 10 of 15 before and after, with Germany and the UK trading places.

## Academic Basis

- Sloan (1996): Low-accrual firms outperform high-accrual firms by ~10% annually
- Richardson et al. (2005): Accrual reliability predicts earnings persistence
- Dechow (1994): Cash flows better measure firm performance than accruals

## Data Tables

| Table | Columns Used |
|-------|-------------|
| `cash_flow_statement` (FY) | `freeCashFlow` |
| `income_statement` (FY) | `netIncome`, `revenue` |
| `key_metrics` (FY) | `returnOnEquity`, `marketCap` |
| `financial_ratios` (FY) | `operatingProfitMargin` |

## Usage

Run from the repository root.

```bash
# One market
python3 fcf-conversion/backtest.py --exchange NSE --output fcf-conversion/results/returns_NSE.json

# All 16 published markets, then fold them into exchange_comparison.json
for ex in NYSE,NASDAQ,AMEX NSE XETRA STO TSX SHZ,SHH HKSE JPX LSE ASX KSC SAO SIX TAI SET SAU; do
  python3 fcf-conversion/backtest.py --exchange "$ex" --output "fcf-conversion/results/returns_${ex//,/_}.json"
done
python3 scripts/rebuild_comparison.py fcf-conversion

# Screen current stocks (TTM data)
python3 fcf-conversion/screen.py

# Generate charts from the results files
python3 fcf-conversion/generate_charts.py
```

- `--exit-method` defaults to `ltp` (last traded price). `--exit-method drop` drops a holding with no exit price instead.
- The listing rules apply automatically on the XETRA, SIX and LSE runs. There's nothing to pass.
- Use `--exchange` as above rather than `--preset`: the Canada and Taiwan presets add TSXV and TWO to the universe.
- `--global` runs the same 16 markets plus SGX, which matches no listings and writes no file.
- Each run reads the live data warehouse, so a rerun can differ slightly from the committed results.

## Distinction from fcf-yield

| | FCF Conversion (this) | [FCF Yield](../fcf-yield/) |
|---|---|---|
| Formula | FCF / Net Income | FCF / Market Cap |
| Question | Is the cash real? | Is it cheap? |
| Signal type | Quality | Valuation |
| Academic root | Sloan (1996) accrual anomaly | Gray & Vogel (2012) |
