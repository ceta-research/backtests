# FCF Compounders

Backtest of a screen for companies whose free cash flow has grown year after year, ranked by return on invested capital.

## Academic Reference

Mohanram, P. (2005) "Separating Winners from Losers among Low Book-to-Market Stocks using Financial Statement Analysis", *Review of Accounting Studies* 10(2-3), 133-170. Combining growth signals with quality metrics creates a powerful predictor of future returns.

## Signal

| Filter | Threshold | Read from |
|--------|-----------|-----------|
| FCF growth | Up in at least 4 of the last 5 fiscal years | The 6 most recent fiscal-year rows of `cash_flow_statement`. A year counts as growth when FCF is above the prior year's and the prior year's is positive |
| FCF level | Positive in all 6 rows | Same rows |
| Return on invested capital | > 15% | Latest fiscal-year row of `key_metrics` |
| Operating margin | > 15% | Latest fiscal-year row of `financial_ratios` |
| Market cap | Above the market's floor (table under Usage) | Same `key_metrics` row |

**Ranking:** highest ROIC first, ticker as the tie-break. Top 30.

Every filter reads fiscal-year rows dated at least 45 days before the rebalance. There is no maximum filing age and no check that the 6 cash-flow rows are consecutive years, so a company that stopped filing can keep passing on its last rows.

## Parameters

- **Rebalancing:** Annual, on July 1, with entry and exit at the first close after the rebalance date. 25 periods, July 2000 to July 2025. A year in this file is the year of its July rebalance.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a first close within 10 days of the rebalance date, at 1.00 or more). A cash year earns 0%. The floor is checked at entry only. A name that fails a data check at exit (a return above the 200% single-stock cap, or no exit price at all) is left out of that year's average and not replaced.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted, so on NSE, JPX and TAI/TWO, where the market-cap floor is at least 10 billion local units, every holding pays the 0.1% tier.
- **Data lag:** 45 days, counted from the fiscal year end (the warehouse row's date), not from the filing date.
- **Data guards:** Entry price of at least 1.00 in the listing's quote unit (1 penny on LSE), and single-stock returns above 200% for the year left out as price artifacts.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's ordinary and preference shares can both be held.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY, which includes dividends, as do the DAX and the Oslo All Share. The other local indices are price indices: they leave dividends out, while the portfolio's returns include them. South Africa, Saudi Arabia and Israel have no local index configured and are measured against the S&P 500 in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (table under Usage). Market-cap floors come from `MKTCAP_THRESHOLD_MAP` in the same file.

## Results (July 2000 to July 2025)

18 markets were run. One, the US, was invested in at least half of the 25 annual periods, and it is the only result. 6 markets were invested in fewer than half the periods and 11 never invested. Those 17 are too thin to test. They are listed separately below and are not results.

### US (NYSE + NASDAQ + AMEX)

| Metric | FCF Compounders | S&P 500 (SPY) |
|--------|-----------------|---------------|
| CAGR | 12.19% | 7.85% |
| Total return | 1,673.71% | 561.67% |
| Max drawdown | -31.44% | -38.01% |
| Annualized volatility | 15.77% | 16.63% |
| Sharpe ratio | 0.646 | 0.352 |
| Sortino ratio | 1.655 | 0.628 |
| Excess CAGR | +4.34 points | n/a |
| Win rate | 68% (17 of 25 years) | n/a |
| Up capture | 118.21% | n/a |
| Down capture | 49.95% | n/a |
| Cash years | 2 of 25 (2000, 2001) | n/a |
| Avg stocks when invested | 27.3 | n/a |

Returns are in USD, after costs, with dividends on both sides.

- **Cash years:** no name passed the screen in 2000 or 2001, so the portfolio earned 0% while SPY lost 14.78% and 22.45%. Those 2 years are counted in the 17 wins and are part of the lead. Over the 23 invested years (July 2002 to July 2025) the portfolio returned 13.32% a year against 10.54% for SPY, a lead of 2.78 points. That figure is compounded from the annual returns in the results file.
- **Drawdown:** measured on annual returns. The -31.44% runs from July 2007 to July 2009 (-12.44%, then -21.70%).
- **Book size:** 18 to 30 names in every invested year, and 30 names passed the screen in every year from 2005 on.

### Too thin to test: mostly cash

A market invested in fewer than half of its 25 periods has no result here. These 6 were run and are listed for the record.

| Market | Listing rule | Benchmark | Years invested (of 25) | Which years | Avg stocks when invested | Cash years: fewer than 10 passed the screen | Cash years: 10 or more passed, fewer than 10 had an entry price |
|--------|--------------|-----------|------------------------|-------------|--------------------------|---------------------------------------------|------------------------------------------------------------------|
| China (SHZ + SHH) | none | SSE Composite | 12 | 2010-2012, 2015, 2017-2024 | 21.7 | 13 | 0 |
| India (NSE) | none | Sensex | 11 | 2010-2011, 2016-2024 | 20.9 | 14 | 0 |
| UK (LSE) | pence-quoted lines only | FTSE 100 | 7 | 2017-2023 | 16.3 | 13 | 5 (2010, 2011, 2014, 2015, 2024) |
| Hong Kong (HKSE) | none | Hang Seng | 6 | 2013, 2018-2022 | 12.2 | 19 | 0 |
| Japan (JPX) | none | Nikkei 225 | 3 | 2018, 2022-2023 | 11.3 | 21 | 1 (2017) |
| Taiwan (TAI + TWO) | none | TAIEX | 1 | 2022 | 10.0 | 24 | 0 |

The results files of these markets still carry CAGR, Sharpe, drawdown and excess fields. With 1 to 12 invested years averaged with 13 to 24 zeros, those fields don't describe the strategy, and a small positive or negative excess in them is not a beat or a trail. The comparison charts draw these 6 as hatched bars labelled "too thin to test", after the ranked markets.

### Too thin to test: never invested

In these 11 markets fewer than 10 names passed the screen in every one of the 25 years.

| Market | Listing rule | Most names passing the screen in one year |
|--------|--------------|-------------------------------------------|
| Canada (TSX + TSXV) | none | 9 (2008) |
| Sweden (STO) | none | 9 (2020) |
| Switzerland (SIX) | Switzerland-domiciled only | 8 (2022) |
| Germany (XETRA) | Germany-domiciled only | 7 (2023) |
| Thailand (SET) | none | 7 (2020) |
| South Africa (JNB) | none | 6 (2018) |
| Korea (KSC) | none | 5 (2023) |
| Saudi Arabia (SAU) | none | 5 (2013) |
| Singapore (SES) | none | 4 (2023) |
| Israel (TLV) | none | 3 (2022) |
| Norway (OSL) | none | 1 (2022, 2023) |

Their results files report a CAGR of 0.00% and an excess equal to minus the benchmark's CAGR. That is the return of holding cash, not a test of the screen. Norway's benchmark (Oslo All Share) starts in 2013, so only 12 of its 25 periods are measured against it. The comparison charts leave these 11 out and name them in the footer, and no Germany chart is drawn.

## Rerun October 2026

These results were rerun on 2026-10-13 with three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias):

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 9 holdings in 4 markets exited this way (US 4, India 2, UK 2, Hong Kong 1); each results file lists them under its `ltp` key. Run with both exit rules on the same selection (`--exit-method both`), the rule alone moves CAGR by +0.03 points in the US, +0.14 in India, -0.12 in the UK and +0.11 in Hong Kong.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.

The earlier results dated from May 2026. They also predate the entry-price check on the 10-stock floor (September 2026): they went to cash only when fewer than 10 names passed the screen, and averaged whatever had prices otherwise. Smaller changes between the two runs (deterministic tie-breaks, a fix to the price-oscillation filter, 4.5 months of data revisions) are not separated out below. What changed:

- **Germany** had 5 cash years and 8.84% a year in the earlier results, on every line listed on XETRA. With Germany-domiciled companies only, at most 7 names pass the screen in any year, so the market never invests.
- **UK** had 5 cash years and 10.71% a year in the earlier results, on every line listed on LSE. It now has 18 cash years: fewer than 10 pence-quoted names passed the screen in 13 years (2000-2009, 2012, 2013, 2016), and in 5 more (2010, 2011, 2014, 2015, 2024) 10 to 13 passed but only 7 to 9 had a usable entry price. The 7 invested years are 2017 to 2023.
- **Switzerland** was invested in 5 years (2008-2011, 2022) in the earlier results, averaging 5.2 names, on every line listed on SIX. With Switzerland-domiciled companies only, at most 8 names pass the screen in any year, so the market never invests.
- **Japan** lost one invested year: in 2017, 10 names passed the screen but only 9 had a usable entry price. It was invested in 4 years before and 3 now.
- **No cash year changed** in the US (12.15% to 12.19% a year), India, China, Hong Kong or Taiwan, and the other 8 markets never invested in either run. Singapore is new: the earlier market list named an exchange code that matched no rows.
- Benchmark CAGRs are unchanged to 0.01 points on all 17 markets that had an earlier file. Some benchmark year rows for 2000 to 2007 moved in India, Japan, Korea, Sweden and China, in offsetting pairs.

3 of the 9 last-traded-price exits have no delisting date in the data: TIPSINDLTD.NS and GLS.NS (India, entered July 2024) and 4338.HK (Hong Kong, entered July 2022). They have not been checked against a source. If a line was renamed and kept trading, the fill closes a live holding early at the old ticker's last price, so India's +0.14 points should not be read as the effect of delistings.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

Each committed results file is one market run on its own:

```bash
# One market, written to the committed results file
python3 fcf-compounders/backtest.py --exchange LSE --output fcf-compounders/results/returns_LSE.json

# All 18, one market at a time
for ex in NYSE,NASDAQ,AMEX NSE XETRA LSE SHZ,SHH HKSE JPX KSC SIX STO OSL SET SES JNB SAU TLV; do
  python3 fcf-compounders/backtest.py --exchange "$ex" --output "fcf-compounders/results/returns_${ex//,/_}.json"
done
python3 fcf-compounders/backtest.py --preset canada --output fcf-compounders/results/returns_TSX.json
python3 fcf-compounders/backtest.py --preset taiwan --output fcf-compounders/results/returns_TAI.json

# The comparison file the charts read: 18 markets, each node a copy of its per-market file
python3 scripts/rebuild_comparison.py fcf-compounders
python3 scripts/rebuild_comparison.py fcf-compounders --check

# Charts from results/exchange_comparison.json (the committed PNGs match a run on the Agg backend)
MPLBACKEND=Agg python3 fcf-compounders/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup and entry-price check still apply)
python3 fcf-compounders/backtest.py --exchange LSE --exit-method drop

# Current screen
python3 fcf-compounders/screen.py
python3 fcf-compounders/screen.py --preset india
```

Don't use `--global` to reproduce the files. It runs TSX and TAI alone, while the committed `returns_TSX.json` and `returns_TAI.json` are the `canada` (TSX + TSXV) and `taiwan` (TAI + TWO) presets. It writes its per-market files into the directory of `--output`, so `--output results/exchange_comparison.json` from the repository root writes to `results/` and not to `fcf-compounders/results/`. A market that raises an error is recorded as an `error` entry in the combined file and its old per-market file stays on disk.

A rerun reads the warehouse as it is on the day, so figures can differ from the committed files after a data refresh.

| Market | Arguments | Market cap floor | Risk-free rate | Benchmark | Listing rule | Output file | Status |
|--------|-----------|------------------|----------------|-----------|--------------|-------------|--------|
| US | `--exchange NYSE,NASDAQ,AMEX` | USD 1B | 2.0% | S&P 500 (SPY) | none | `returns_NYSE_NASDAQ_AMEX.json` | invested 23 of 25 years |
| India | `--exchange NSE` | INR 20B | 6.5% | Sensex | none | `returns_NSE.json` | too thin: invested 11 of 25 |
| Germany | `--exchange XETRA` | EUR 500M | 2.0% | DAX | Germany-domiciled only | `returns_XETRA.json` | too thin: never invested |
| UK | `--exchange LSE` | GBP 500M | 3.5% | FTSE 100 | GBp lines only | `returns_LSE.json` | too thin: invested 7 of 25 |
| Canada | `--preset canada` (TSX, TSXV) | CAD 500M | 2.5% | TSX Composite | none | `returns_TSX.json` | too thin: never invested |
| China | `--exchange SHZ,SHH` | CNY 2B | 2.5% | SSE Composite | none | `returns_SHZ_SHH.json` | too thin: invested 12 of 25 |
| Hong Kong | `--exchange HKSE` | HKD 2B | 3.0% | Hang Seng | none | `returns_HKSE.json` | too thin: invested 6 of 25 |
| Japan | `--exchange JPX` | JPY 100B | 0.1% | Nikkei 225 | none | `returns_JPX.json` | too thin: invested 3 of 25 |
| Korea | `--exchange KSC` | KRW 500B | 3.0% | KOSPI | none | `returns_KSC.json` | too thin: never invested |
| Switzerland | `--exchange SIX` | CHF 500M | 0.5% | SMI | Switzerland-domiciled only | `returns_SIX.json` | too thin: never invested |
| Sweden | `--exchange STO` | SEK 5B | 2.0% | OMX Stockholm 30 | none | `returns_STO.json` | too thin: never invested |
| Norway | `--exchange OSL` | NOK 5B | 3.0% | Oslo All Share (from 2013) | none | `returns_OSL.json` | too thin: never invested |
| Taiwan | `--preset taiwan` (TAI, TWO) | TWD 10B | 1.0% | TAIEX | none | `returns_TAI.json` | too thin: invested 1 of 25 |
| Thailand | `--exchange SET` | THB 10B | 2.5% | SET Index | none | `returns_SET.json` | too thin: never invested |
| Singapore | `--exchange SES` | SGD 500M | 2.5% | STI | none | `returns_SES.json` | too thin: never invested |
| South Africa | `--exchange JNB` | ZAR 10B | 9.0% | S&P 500 (SPY, in USD) | none | `returns_JNB.json` | too thin: never invested |
| Saudi Arabia | `--exchange SAU` | SAR 1B | 3.5% | S&P 500 (SPY, in USD) | none | `returns_SAU.json` | too thin: never invested |
| Israel | `--exchange TLV` | ILS 1B | 3.0% | S&P 500 (SPY, in USD) | none | `returns_TLV.json` | too thin: never invested |

`--preset germany`, `--preset uk` and the other presets run the same exchanges as the `--exchange` forms above. Only the `universe` label written to the file differs for the US (`US_MAJOR`), China (`China`) and South Africa (`JSE`). `--preset germany` therefore writes 25 cash years.

The market-cap floor is compared with the filing's market cap in the company's reporting currency.

Not run: Brazil (SAO) and Australia (ASX) for split-adjustment errors in `adjClose`, and France (PAR) for a gap in annual filings.

## Data Requirements

- `cash_flow_statement` (FY): freeCashFlow
- `key_metrics` (FY): returnOnInvestedCapital, marketCap
- `financial_ratios` (FY): operatingProfitMargin
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (TTM ROIC and margin). Lists every line on the exchange, without the listing rules or dedup |
| `generate_charts.py` | Charts from `results/exchange_comparison.json`: 2 for the US, 2 for the UK (titled "too thin to test") and 3 comparisons. A market that never invested gets no chart |
| `README.md` | This file |
| `results/` | `returns_<KEY>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py`: 7 PNGs |

*Data: Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)*
