# Deleveraging

Companies that cut their debt-to-equity ratio by at least 10% in a year while staying profitable. Each quarter the screen buys the 30 largest reductions on a market, at equal weight.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| D/E change, year over year | Below -10% | A real reduction in leverage |
| Prior-year D/E | Above 0.1 | The company had debt to reduce |
| Current D/E | At least 0.01 | Drops zero-D/E rows, a known error in FY2012 data |
| Return on equity | Above 8% | Deleveraging out of profit, not out of distress |
| Market cap | Above the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency ($1B in the US, ₹20B on NSE, ¥100B on JPX, €500M on XETRA) | Liquid, investable |

**Sorting:** largest D/E reduction first. Top 30.

**D/E change** = (D/E latest fiscal year - D/E prior fiscal year) / D/E prior fiscal year. The latest row is the newest fiscal-year filing at least 45 days old on the rebalance date; the prior row is the newest one at least 410 days old.

## Parameters

- **Rebalancing:** Quarterly, on the first day of January, April, July and October, executed at the next available close. 103 periods, January 2000 to July 2025, the last one exiting in October 2025.
- **Min stocks:** 10. The quarter is held in cash if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price. A cash quarter earns 0% and counts in the CAGR. The floor is checked at entry only.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data filters:** An entry price below 1.0 in the exchange's price unit is unusable (pence on LSE). There is no return cap. The data checks (`dc-1`, [METHODOLOGY.md](../METHODOLOGY.md#data-checks-deleveraging-and-cyclical-timing)) move holiday rows, zero-volume rows of lines that traded, stale lines with no volume, a thin share class's replacement bar off its sibling's scale and non-positive prices out of each price window, hold a Thai NVDR line and its ordinary line once, test the market-cap floor on a cap checked against later reverse splits, and flag adjustment-factor steps, splits and every return above +100% or below -50% for review. The results below come from the rerun with these checks, which replaced a 200% cap on a single stock's quarterly return; [Rerun October 2026](#rerun-october-2026) lists what moved.
- **Exits:** A holding with no price after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa has no local index series and is measured against the S&P 500 in USD. Norway's index series starts in 2013.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 9.0% South Africa).

## Results (January 2000 to October 2025)

Of the 13 markets that can be ranked, the screen beat its own index in 7 (UK, Hong Kong, Sweden, India, Canada, Japan, US), tied it in Taiwan (+0.01 points), and trailed in 5 (Switzerland, Korea, China, Germany, Thailand). The US leads SPY by 0.46 points a year. Three markets are shown but not ranked: South Africa and Singapore were in cash for more than half the quarters, and Norway's benchmark covers only 50 of the 103 quarters.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max drawdown | Cash quarters | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------------|---------------|------------|
| UK (LSE, pence lines) | FTSE 100 | 6.84% | 1.36% | +5.48 | 0.172 | -44.61% | 0 of 103 | 20.4 |
| Hong Kong (HKSE) | Hang Seng | 5.60% | 1.77% | +3.83 | 0.096 | -67.86% | 15 of 103 | 19.1 |
| Sweden (STO) | OMX Stockholm 30 | 6.86% | 3.17% | +3.69 | 0.240 | -57.37% | 18 of 103 | 20.5 |
| India (NSE) | Sensex | 14.48% | 11.12% | +3.36 | 0.264 | -67.51% | 22 of 103 | 21.8 |
| Canada (TSX + TSXV) | TSX Composite | 8.38% | 5.08% | +3.29 | 0.357 | -43.51% | 1 of 103 | 23.4 |
| Japan (JPX) | Nikkei 225 | 6.11% | 3.40% | +2.71 | 0.285 | -59.32% | 10 of 103 | 26.0 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 8.48% | 8.02% | +0.46 | 0.298 | -52.11% | 0 of 103 | 23.9 |
| Taiwan (TAI + TWO) | TAIEX | 4.38% | 4.38% | +0.01 | 0.182 | -56.29% | 29 of 103 | 26.1 |
| Switzerland (SIX, Swiss companies) | SMI | 1.76% | 2.10% | -0.35 | 0.092 | -56.22% | 33 of 103 | 17.3 |
| Korea (KSC) | KOSPI | 2.85% | 4.81% | -1.96 | -0.008 | -46.83% | 36 of 103 | 26.3 |
| China (SHZ + SHH) | SSE Composite | 1.96% | 4.19% | -2.23 | -0.017 | -67.32% | 1 of 103 | 21.9 |
| Germany (XETRA, German companies) | DAX | 2.51% | 5.12% | -2.61 | 0.027 | -57.24% | 26 of 103 | 20.8 |
| Thailand (SET) | SET Index | 0.76% | 3.76% | -3.00 | -0.087 | -56.84% | 29 of 103 | 22.6 |
| South Africa (JNB)\* | S&P 500 (USD) | 1.72% | 8.02% | -6.30 | -1.122 | -9.68% | 76 of 103 | 13.9 |
| Singapore (SES)\* | STI | -3.08% | 2.17% | -5.25 | -0.589 | -68.09% | 80 of 103 | 11.3 |
| Norway (OSL)\*\* | Oslo All Share | 5.52% | 10.91% | -5.39 | 0.268 | -24.25% | 77 of 103 | 14.7 |

\* Too thin to test: in cash for more than half of the 103 quarters. South Africa was invested in 27 quarters and Singapore in 23, so those CAGRs mostly measure the cash. South Africa's benchmark is also in another currency.

\*\* Short window, not ranked. The Oslo All Share series starts in 2013, so Norway's CAGR, Sharpe and drawdown cover the 50 quarters from April 2013 to October 2025; the other 53 rebalances are unmeasured. The book was invested in 26 of the 50 measured quarters, so it isn't too thin to test over its window. It was in cash for the other 24 and for all 53 rebalances before the window opened in April 2013, 77 of 103 in all. That splits the cash by window; the cash-quarter table below splits the same 77 by cause, also 53 and 24, but not the same quarters.

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Avg stocks is the average book in invested quarters.

- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). The S&P 500 (through SPY), the DAX and the Oslo All Share include them too, so the US, Germany, South Africa and Norway rows are like for like on that point. The other twelve indices are price indices, so excess against them is overstated by roughly the index's dividend yield. Six of the seven wins are against price indices; the US lead over SPY is the one like for like. See [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations) (Benchmark Limitations).
- **Ties and thin leads:** Taiwan's +0.01 is a tie, and against a price index. Switzerland trails the SMI by 0.35 points before any allowance for dividends.
- **Drawdowns:** four books lost more than 60% from peak to trough: Singapore (-68.09%), Hong Kong (-67.86%), India (-67.51%) and China (-67.32%). India's and Hong Kong's are the 2008 crash, against -51.34% for the Sensex and -50.50% for the Hang Seng. China's runs from 2001 to 2005, and the SSE Composite's own worst is -66.96%. Singapore's starts in 2007 and never recovers, because most later quarters are cash.
- **Sharpe:** South Africa's is -1.122 at a positive CAGR because its risk-free rate is 9.0% and the book sat in cash for 76 quarters.

### Cash quarters

A quarter is in cash for one of two reasons: fewer than 10 names passed the screen, or 10 or more passed but fewer than 10 had a usable entry price. Each results file records both counts for every quarter (`screened`, `entry_buyable`). Quarters are named by their rebalance month; the number in brackets is the length of the run.

| Market | Fewer than 10 passed the screen | 10 or more passed, fewer than 10 buyable |
|--------|--------------------------------|------------------------------------------|
| Singapore (SES) | 31: 2000-01 to 2007-01 (29), 2020-04 (1), 2025-04 (1) | 49: 2008-10 to 2010-07 (8), 2011-10 to 2013-01 (6), 2014-04 (1), 2015-04 to 2020-01 (20), 2020-07 to 2021-01 (3), 2022-10 to 2025-01 (10), 2025-07 (1) |
| Norway (OSL) | 53: 2000-01 to 2008-01 (33), 2009-04 to 2010-01 (4), 2012-04 to 2014-01 (8), 2019-04 to 2021-01 (8) | 24: 2008-04 to 2009-01 (4), 2010-04 to 2012-01 (8), 2014-04 to 2016-01 (8), 2017-04 to 2018-01 (4) |
| South Africa (JNB) | 40: 2000-01 to 2006-01 (25), 2006-07 (1), 2008-07 to 2010-04 (8), 2013-10 (1), 2020-07 to 2021-07 (5) | 36: 2006-04 (1), 2006-10 to 2008-04 (7), 2010-07 to 2010-10 (2), 2012-04 to 2013-04 (5), 2014-01 (1), 2016-10 to 2017-07 (4), 2018-01 to 2018-07 (3), 2019-01 to 2020-04 (6), 2023-04 to 2023-07 (2), 2024-04 to 2025-04 (5) |
| Korea (KSC) | 29: 2000-01 to 2006-01 (25), 2007-04 to 2008-01 (4) | 7: 2006-04 to 2006-07 (2), 2007-01 (1), 2009-04 to 2010-01 (4) |
| Switzerland (SIX) | 21: 2000-01 to 2004-01 (17), 2019-04 to 2020-01 (4) | 12: 2004-04 (1), 2005-04 to 2006-01 (4), 2009-04 to 2010-04 (5), 2016-10 to 2017-01 (2) |
| Taiwan (TAI + TWO) | 25: 2000-01 to 2006-01 (25) | 4: 2006-04 to 2007-01 (4) |
| Thailand (SET) | 25: 2000-01 to 2005-01 (21), 2009-04 to 2010-01 (4) | 4: 2005-04 to 2006-01 (4) |
| Germany (XETRA) | 25: 2000-01 to 2005-01 (21), 2009-04 to 2010-01 (4) | 1: 2021-01 (1) |
| India (NSE) | 22: 2000-01 to 2005-04 (22) | 0 |
| Sweden (STO) | 13: 2000-01 (1), 2001-04 to 2004-01 (12) | 5: 2009-01 (1), 2012-04 to 2013-01 (4) |
| Hong Kong (HKSE) | 0 | 15: 2000-01 to 2003-07 (15) |
| Japan (JPX) | 7: 2000-01 to 2001-04 (6), 2003-01 (1) | 3: 2002-07 to 2002-10 (2), 2003-04 (1) |
| China (SHZ + SHH) | 0 | 1: 2006-04 (1) |
| Canada (TSX + TSXV) | 0 | 1: 2000-01 (1) |

The US and the UK were invested in all 103 quarters.

## Rerun October 2026

These results replace the set published in June 2026. This topic's screen didn't change. Six things around it did. The first four are described in [METHODOLOGY.md](../METHODOLOGY.md) (Universe Construction, and Limitations: Survivorship Bias); the fifth is a fix to the shared price filter in `data_utils.py`; the sixth is the data checks ([METHODOLOGY.md](../METHODOLOGY.md#data-checks-deleveraging-and-cyclical-timing), Data Checks):

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the quarter. 85 holdings in 12 markets exited this way, and none was left without an exit price; each results file lists them under its `ltp` key. In the first October run, before the data checks, 44 did, and on the same selection the rule moved CAGR by between -0.13 points (UK) and +0.08 points (South Africa).
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market, before the top-30 cut.
- **Entry-side cash rule.** The June results went to cash only when fewer than 10 names passed the screen, and otherwise averaged however many of them had prices. A quarter is now also cash when fewer than 10 of the screened names have a usable entry price. That rule accounts for 162 cash quarters across 13 markets.
- **Price-filter fix and price window.** The June run's price filter could delete valid prices. `remove_price_oscillations` compared each price row with the rows before and after it without checking that they were close in time. This backtest fetches prices only for the days after each rebalance date, so the row before the first price of a quarter was a quarter old. A large move between one window and the next (30% or more) could then look like a one-day spike, and the filter deleted the row at the window edge for any stock with five or more such moves. A stock whose whole window was deleted dropped out of that quarter. Since commit `b815bb7` (21 August 2026) the filter only compares rows that are within 10 days of each other. The price window after each rebalance date also grew from 10 days to 11. No run isolates these two changes, so their effect on this topic is not measured. They can account for part of the difference in any invested quarter in any market: in its return, in how many names each run could price, and in benchmark prices, which pass through the same filter.
- **Data checks in place of the return cap.** The first October run still dropped a single stock's quarterly return above 200%. A second October run replaced that cap with the data checks, and these results come from it. Against the first run, the US went from 7.79% to 8.48% (excess -0.23 to +0.46), Hong Kong from 4.76% to 5.60%, Sweden from 7.53% to 6.86%, China from 3.50% to 1.96% and Thailand from 2.01% to 0.76%; no other market moved by more than 0.21 points. Four markets gained cash quarters. Thailand gained 8, April 2004 to January 2006, because check N holds an NVDR line and its ordinary line once, so fewer names pass the screen or can be bought. Singapore gained 6, October 2022 to January 2024, because check M lowers U09.SI's cap by its price factor of 10, under the floor. Korea gained 3 (April and July 2006, January 2007) and China 1 (April 2006), each where check T lost an entry and left 9 names buyable.

Selection is also deterministic now: ties at the top-30 cut break on the symbol (see [Reproducibility](../METHODOLOGY.md#reproducibility)).

The large moves:

- **Germany** fell from 6.66% to 2.51% and now trails the DAX by 2.61 points, after leading it by 1.54. Cash quarters rose from 0 to 26: with Germany-domiciled companies only, fewer than 10 names pass the screen from January 2000 to January 2005 and from April 2009 to January 2010.
- **Switzerland** moved from 2.62% to 1.76% and from 0.51 points ahead of the SMI to 0.35 behind. Cash quarters rose from 0 to 33. Its maximum drawdown improved from -75.20% to -56.22%, because the book is now in cash from 2000 to 2003.
- **UK** rose from 5.29% to 6.84%. It has no cash quarters in either set; the average book grew from 13.2 to 20.4 names. Two changes can fill the book: the listing rule (lines not quoted in pence no longer take one of the 30 slots) and the price-filter fix (price rows the June run's filter could delete are now kept). The split between them is not determined.
- **Singapore** fell from 0.73% to -3.08% and **South Africa** from 3.77% to 1.72%. Cash quarters rose from 31 to 80 and from 39 to 76, with 49 and 36 of them under the entry-side rule, so both are now too thin to test. The June Singapore book averaged 8.3 names when invested, under the 10-name floor.
- **Norway** fell from 9.85% to 5.52% over its measured window: 2014 and 2015, which returned +24.67% and +20.00% in June, are now cash, seven of their eight quarters under the entry-side rule.
- **Thailand** (3.99% to 0.76%) and **Taiwan** (5.74% to 4.38%) lost their leads over the SET Index and the TAIEX: +0.23 became -3.00, and +1.36 became +0.01. Taiwan's change is four cash quarters under the entry-side rule, April 2006 to January 2007. Thailand's cash quarters rose from 17 to 29, all 12 after check N's twin merge: April 2004 to January 2006 (8), and April 2009 to January 2010 (4), which the first October run held in cash under the entry-side rule and which now have fewer than 10 names passing. **Korea** (4.21% to 2.85%) lost the same four quarters, April 2009 to January 2010, under the entry-side rule, and three more (April and July 2006, January 2007) to entries check T lost.
- **Hong Kong** rose from 3.02% to 5.60%: the 15 quarters from January 2000 to July 2003 are now cash, and the June book had lost 35.25% in 2000 and 21.45% in 2001 on the few names it could price.
- **China** fell from 3.73% to 1.96% (excess -0.46 to -2.23). Its one cash quarter, April 2006, costs about 1.0 point a year: held with its 9 buyable names, the leg's excess reads -1.23 (METHODOLOGY.md, Data Checks).
- The other five markets moved by less than one point: Japan -0.85, Sweden -0.72, India -0.49, Canada 0.00 and the US +0.69. India and the US have no new cash quarters, so their moves sit in invested quarters. The US moved 0.00 in the first October run and +0.69 with the data checks. India's cause is not determined (dedup, tie-breaks, the price-filter fix, the price window, revised data or the data checks).
- **Benchmarks.** Benchmark CAGRs are unchanged at two decimals in all 16 markets, and in 15 of them every annual benchmark figure is unchanged too. China's SSE Composite moved in four years: 2006 from +123.66% to +129.96%, 2007 from +99.63% to +94.16%, 2014 from +58.88% to +58.84% and 2015 from -1.65% to -1.62%. Each pair offsets, so the index's CAGR (4.19%) and total return (187.87%) are the same; its maximum drawdown moved from -67.10% to -66.96%. That pattern is what one changed index price at the January 2007 rebalance and one at January 2015 would give. The price-filter fix is a possible cause, not a confirmed one. China's excess in those years is now -79.67 points (2006), +89.85 (2007), -31.49 (2014) and +57.66 (2015).

On the 13 ranked markets, 10 had a positive excess in June and 8 have one now, Taiwan's +0.01 among them: Germany, Thailand and Switzerland went from ahead to behind, Taiwan from ahead to a tie, and the US from behind to ahead.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

```bash
# US (default universe: NYSE + NASDAQ + AMEX)
python3 deleveraging/backtest.py --verbose

# One market, written to the committed results file
python3 deleveraging/backtest.py --preset germany --output deleveraging/results/returns_XETRA.json

# After any market is rerun, rebuild the combined file the charts read, then check it
python3 scripts/rebuild_comparison.py deleveraging
python3 scripts/rebuild_comparison.py deleveraging --check

# Charts from results/exchange_comparison.json
python3 deleveraging/generate_charts.py

# The old exit rule (doesn't reproduce the June results: the listing rules, dedup, cash rule and price-filter fix still apply)
python3 deleveraging/backtest.py --preset uk --exit-method drop

# Every data check off and no return cap: measures what the checks change, never published
python3 deleveraging/backtest.py --preset thailand --data-checks uncapped
```

The committed files were made one market at a time, each with `--preset <name> --output deleveraging/results/<file>`:

| Market | `--preset` | Exchanges | Output file | Listing rule |
|--------|------------|-----------|-------------|--------------|
| US | `us` | NYSE, NASDAQ, AMEX | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `india` | NSE | `returns_NSE.json` | |
| Japan | `japan` | JPX | `returns_JPX.json` | |
| UK | `uk` | LSE | `returns_LSE.json` | GBp lines only |
| Germany | `germany` | XETRA | `returns_XETRA.json` | Germany-domiciled only |
| Canada | `canada` | TSX, TSXV | `returns_TSX.json` | |
| Thailand | `thailand` | SET | `returns_SET.json` | |
| Sweden | `sweden` | STO | `returns_STO.json` | |
| Hong Kong | `hongkong` | HKSE | `returns_HKSE.json` | |
| China | `china` | SHZ, SHH | `returns_SHZ_SHH.json` | |
| Switzerland | `switzerland` | SIX | `returns_SIX.json` | Switzerland-domiciled only |
| South Africa | `southafrica` | JNB | `returns_JNB.json` | |
| Norway | `norway` | OSL | `returns_OSL.json` | |
| Singapore | `singapore` | SES | `returns_SES.json` | |
| Taiwan | `taiwan` | TAI, TWO | `returns_TAI_TWO.json` | |
| Korea | `korea` | KSC | `returns_KSC.json` | |

`--global` doesn't reproduce this set. It runs Canada as TSX alone and writes that to `returns_TSX.json`, the file that holds the TSX + TSXV book. If one market fails it also leaves that market's old file in place and records an error in `exchange_comparison.json`. Each run reads the live data warehouse, so a rerun can differ slightly from the committed results.

## Data Requirements

- `financial_ratios` (FY): debtToEquityRatio, latest and prior fiscal year
- `key_metrics` (FY): returnOnEquity, marketCap
- `stock_eod`: adjClose, close and volume after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)
- Data checks: `income_statement` (FY net income, EPS, weighted shares), `stock_eod` close and volume before each FY period end, `stock_prices_non_split_adjusted_daily` and `splits_calendar` (market-cap check and split flags)

## Known Limits

- The filing date used is the fiscal period end, so the 45-day lag counts from the period end. A fiscal year that ended 45 to 90 days before a rebalance can be used before the annual report was public.
- The size floor compares the filing's market cap, in the company's reporting currency, with a threshold set in the listing currency.
- The market-cap check catches only caps whose price was later scaled up 3 times or more by reverse splits. Factor flags record adjustment steps the close doesn't mirror but don't repair them. Both are listed in METHODOLOGY.md (Data Checks).
- ASX and SAO are not run: adjClose split artifacts distort returns there.
- `screen.py` does not start at present: a syntax error in its `--cloud` branch stops Python from loading the file. The backtest doesn't use it, and no result here depends on it.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen, from the latest two fiscal-year filings. Doesn't run at present (see Known Limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py` |
