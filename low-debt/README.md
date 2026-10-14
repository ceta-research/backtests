# Low Debt Quality

Buys companies with little debt and improving fundamentals: debt to equity below 0.5 and a Piotroski F-Score of 7 or more out of 9. The debt filter picks conservative balance sheets. The F-Score keeps out companies that carry little debt because the business is shrinking. Every company that passes is held at equal weight for a year.

## Academic Basis

- Piotroski (2000), "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers", *Journal of Accounting Research* 38, 1-41. The 9-signal F-Score separates financially strong from weak companies among cheap stocks. This strategy uses it without the price-to-book filter, as a financial health screen.
- Graham & Dodd (1934), *Security Analysis*. Margin of safety through a conservative balance sheet.
- George & Hwang (2010), "A Resolution of the Distress Risk and Leverage Puzzles in the Cross Section of Stock Returns", *Journal of Financial Economics* 96(1). Low-leverage companies earned higher returns than highly levered ones.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Debt to equity | 0 to 0.5 (negative equity is excluded) | `financial_ratios` FY |
| Piotroski F-Score | 7 or more of 9 | computed from `income_statement`, `balance_sheet`, `cash_flow_statement` FY |
| Market cap | Above the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`): $1B in the US, ₹20B on NSE, ¥100B on JPX, €500M on XETRA | `key_metrics` FY |

The nine F-Score signals, one point each:

| # | Signal | Condition |
|---|--------|-----------|
| F1 | Profit | Net income above 0 |
| F2 | Cash flow | Operating cash flow above 0 |
| F3 | Return on assets | Net income / total assets higher than the year before |
| F4 | Accruals | Operating cash flow above net income |
| F5 | Leverage | Long-term debt / total assets lower than the year before |
| F6 | Liquidity | Current ratio higher than the year before |
| F7 | Equity | Stockholders' equity not lower than the year before |
| F8 | Asset turnover | Revenue / total assets higher than the year before |
| F9 | Gross margin | Higher than the year before |

On each rebalance date, for every company on the market's exchanges:

1. Take the company's latest fiscal-year row of each statement whose fiscal year ended at least 45 days before the rebalance date and less than 595 days before it. The market cap comes from the latest `key_metrics` row that is at least 45 days old, with no upper age limit.
2. Take the prior year: the latest fiscal-year row whose year-end is 300 to 430 days before the current row's. That is the company's own previous fiscal year, whatever month its year ends. A company that skipped a year has no prior row and isn't scored.
3. Keep the company if its debt to equity, F-Score and market cap pass the three filters above.
4. Keep one line per ISIN.
5. Hold every company left, at equal weight. There is no maximum.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a close of at least 1.00 in local units in the 10-day entry window). A cash year earns 0%. The floor is checked at entry only, after the one-line-per-ISIN step.
- **Max stocks:** None. In the 13 markets in the results table the average book runs from 17.6 names (Switzerland) to 305.4 (China).
- **Weighting:** Equal weight.
- **Return cap:** A single holding's yearly return above 200% is treated as a data error and left out of that year's average.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days after the fiscal year end (point-in-time).
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's two share lines can both be held.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa has no local index configured and is measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK).

## Results (July 2000 to July 2025)

Of the 13 markets invested in at least half of the 25 years, the screen beat its own market's index in 7: Sweden (+4.16pp a year), the UK (+4.09), Canada (+3.95), Hong Kong (+3.68), China (+3.04), Japan (+2.80) and Switzerland (+0.49). All seven indices are price indices that leave dividends out, while the portfolio's returns include them, so each margin is overstated by roughly the index's dividend yield. Switzerland's 0.49pp is unlikely to survive a total-return comparison.

India is level with the Sensex: 0.19pp behind, which is a tie at under 0.2pp. The US (0.21pp behind SPY) and Thailand (0.25pp behind the SET Index) sit just past that line. The Sensex and the SET Index are price indices, so with dividends counted India and Thailand would be further behind. Germany (-0.97pp), Taiwan (-1.18pp) and Korea (-1.28pp) trail. The two markets measured against a series that includes dividends, the US against SPY and Germany against the DAX, are both behind it. Singapore and South Africa are too thin to test and are reported separately below.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 11.87% | 12.06% | -0.19 | 0.256 | -15.81% | 6 (2000-2005) | 73.1 |
| Canada (TSX) | TSX Composite | 7.90% | 3.95% | +3.95 | 0.312 | -32.73% | 2 (2000-2001) | 39.6 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 7.64% | 7.85% | -0.21 | 0.338 | -30.48% | 0 | 232.4 |
| Sweden (STO) | OMX Stockholm 30 | 6.71% | 2.55% | +4.16 | 0.223 | -34.98% | 9 (2000-2003, 2006, 2008-2009, 2012-2013) | 21.7 |
| Japan (JPX) | Nikkei 225 | 6.11% | 3.31% | +2.80 | 0.321 | -43.23% | 1 (2000) | 121.5 |
| China (SHZ + SHH) | SSE Composite | 5.47% | 2.43% | +3.04 | 0.092 | -56.46% | 0 | 305.4 |
| Hong Kong (HKSE) | Hang Seng | 5.32% | 1.64% | +3.68 | 0.108 | -33.91% | 2 (2000-2001) | 66.3 |
| UK (LSE) | FTSE 100 | 5.31% | 1.23% | +4.09 | 0.109 | -30.85% | 5 (2000-2003, 2009) | 39.6 |
| Thailand (SET) | SET Index | 4.88% | 5.13% | -0.25 | 0.162 | -31.85% | 9 (2000-2005, 2007-2009) | 24.6 |
| Korea (KSC) | KOSPI | 4.07% | 5.35% | -1.28 | 0.073 | -26.18% | 9 (2000-2008) | 38.9 |
| Germany (XETRA) | DAX | 4.07% | 5.04% | -0.97 | 0.112 | -42.91% | 6 (2000-2003, 2005-2006) | 22.1 |
| Taiwan (TAI + TWO) | TAIEX | 2.90% | 4.09% | -1.18 | 0.139 | -25.57% | 8 (2000-2006, 2009) | 61.7 |
| Switzerland (SIX) | SMI | 2.23% | 1.74% | +0.49 | 0.121 | -34.02% | 8 (2000-2004, 2006, 2009, 2012) | 17.6 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance. Avg stocks is the average number of lines held in invested years; two share lines of one company count as two (see Known limits).

- **US:** 7.64% a year against 7.85% for SPY, with a Sharpe of 0.338 against 0.352, a max drawdown of -30.48% against -38.01%, and no cash years. The return is about the index's, with a shallower drawdown.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Two of the 13 markets are measured against a series that does too: the US against SPY and Germany against the DAX. The other eleven are measured against price indices, so their excess is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Books at the floor:** Japan 2001, Canada 2002, Germany 2004 and 2009, Switzerland 2010 and Sweden 2014 were invested with exactly 10 buyable names. No invested year averages fewer than 10 lines.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Cash years

A year is in cash for one of two reasons, and each period record says which (`screened`, `entry_buyable`). The second column lists, in brackets, how many of the screened names had a usable entry price. The US and China were invested in all 25 years.

| Market | Screen passed fewer than 10 | Fewer than 10 buyable at entry (buyable of screened) |
|--------|-----------------------------|------------------------------------------------------|
| India (NSE) | 2000-2004 | 2005 (9 of 11) |
| Canada (TSX) | none | 2000 (9 of 15), 2001 (7 of 14) |
| Sweden (STO) | 2000-2003, 2008-2009, 2013 | 2006 (9 of 11), 2012 (8 of 12) |
| Japan (JPX) | 2000 | none |
| Hong Kong (HKSE) | none | 2000 (6 of 15), 2001 (5 of 24) |
| UK (LSE) | none | 2000 (8 of 17), 2001 (6 of 15), 2002 (6 of 12), 2003 (8 of 13), 2009 (4 of 11) |
| Thailand (SET) | 2000-2005 | 2007 (6 of 11), 2008 (6 of 14), 2009 (6 of 11) |
| Korea (KSC) | 2000-2008 | none |
| Germany (XETRA) | 2000-2003, 2006 | 2005 (9 of 12) |
| Taiwan (TAI + TWO) | 2000-2006 | 2009 (8 of 10) |
| Switzerland (SIX) | 2000-2003, 2012 | 2004 (9 of 14), 2006 (5 of 11), 2009 (8 of 12) |
| Singapore (SES) | 2000-2007, 2009, 2012 | 2008 (4 of 13), 2010 (2 of 10), 2011 (7 of 21), 2013 (5 of 11), 2014 (5 of 11), 2015 (4 of 12), 2016 (6 of 18), 2017 (3 of 14), 2018 (8 of 22), 2019 (6 of 14), 2020 (6 of 14), 2021 (7 of 12), 2023 (8 of 17) |
| South Africa (JNB) | 2000-2006, 2008-2010, 2012-2013, 2019-2021, 2023-2024 | 2007 (4 of 10), 2011 (6 of 10), 2014 (6 of 13), 2015 (9 of 14), 2017 (6 of 12) |

A cash year earns 0% and is counted in every 25-year figure above, against whatever the index did that year. The next table sets the 25-year excess beside the excess over the invested years only: the portfolio's returns in the years it held stocks, compounded and annualised over that many years, against the index over the same years. The second reading is not a return anyone earned. It shows how the selected stocks did when there was a book.

| Market | Cash years | Index in the cash years (% a year) | Excess, 25 years (pp) | Portfolio, invested years | Index, invested years | Excess, invested years (pp) |
|--------|------------|------------------------------------|-----------------------|---------------------------|-----------------------|-----------------------------|
| India (NSE) | 6 of 25 | +14.11 | -0.19 | 15.90% | 11.42% | +4.49 |
| Canada (TSX) | 2 of 25 | -17.06 | +3.95 | 8.62% | 6.01% | +2.60 |
| Sweden (STO) | 9 of 25 | +0.65 | +4.16 | 10.68% | 3.64% | +7.04 |
| Japan (JPX) | 1 of 25 | -27.61 | +2.80 | 6.37% | 4.85% | +1.52 |
| Hong Kong (HKSE) | 2 of 25 | -19.34 | +3.68 | 5.80% | 3.70% | +2.09 |
| UK (LSE) | 5 of 25 | -4.89 | +4.09 | 6.68% | 2.82% | +3.87 |
| Thailand (SET) | 9 of 25 | +9.17 | -0.25 | 7.73% | 2.93% | +4.81 |
| Korea (KSC) | 9 of 25 | +6.00 | -1.28 | 6.44% | 4.99% | +1.45 |
| Germany (XETRA) | 6 of 25 | -0.18 | -0.97 | 5.40% | 6.74% | -1.35 |
| Taiwan (TAI + TWO) | 8 of 25 | +2.14 | -1.18 | 4.30% | 5.01% | -0.72 |
| Switzerland (SIX) | 8 of 25 | +3.88 | +0.49 | 3.30% | 0.75% | +2.55 |

The sign changes in three markets. India goes from 0.19pp behind the Sensex over 25 years to 4.49pp ahead over its 19 invested years: the Sensex returned 14.11% a year in the six years the portfolio sat in cash. Thailand goes from 0.25pp behind to 4.81pp ahead, and Korea from 1.28pp behind to 1.45pp ahead. Germany and Taiwan trail on both readings. All of these indices except the DAX are price indices, so the invested-years margins are overstated by the dividend yield as well.

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the screen. Its figures are listed so the files can be checked, not as results, and it is left out of the comparison charts.

| Market | Years in cash | Returns in the invested years | Avg stocks when invested | CAGR incl. cash years |
|--------|---------------|-------------------------------|--------------------------|-----------------------|
| Singapore (SES) | 23 of 25 | 2022: +3.43% (10 names), 2024: +21.73% (11 names) | 10.5 | 0.93% |
| South Africa (JNB) | 22 of 25 | 2016: +6.62% (11 names), 2018: +18.37% (12 names), 2022: -9.62% (10 names) | 11.0 | 0.53% |

- **Singapore:** fewer than 10 names passed the screen in 10 years. In 13 more, 10 to 22 passed but only 2 to 8 had an entry close of at least 1.00. The floor is in local units, and many Singapore shares trade below one Singapore dollar.
- **South Africa:** fewer than 10 names passed the screen in 17 years, and in 5 more 10 to 14 passed but only 4 to 9 had a usable entry price.

## Rerun October 2026

These results were rerun with the campaign's changes, described in METHODOLOGY.md (Universe Construction, Filing and Selection Rules, and Limitations: Survivorship Bias), and with one fix to this screen.

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 125 holdings in 13 markets exited this way, 44 of them in the US; each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.12pp to +0.07pp in the 13 markets above, and by -0.27pp in Singapore, where one of the 125 sits in a book invested for two years.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **Prior year fix.** The F-Score compares two fiscal years. The prior year used to be a row dated 410 to 595 days before the rebalance date, a window that no company with a year-end between May 18 and November 13 could meet, so those companies were never screened. The prior year is now the company's own previous fiscal year. DATA_QUALITY_ISSUES.md gives the share of company-years this had excluded: 55% on JNB, 20% on LSE pence lines, 12% in the US. In the US, where no listing rule applies and no year is in cash, the average book went from 196.0 names to 232.4 and the CAGR from 7.57% to 7.64%.
- **Entry-side cash rule.** The results published before this rerun predated the check that at least 10 screened names have a usable entry price. They decided cash on the screen count alone and averaged whatever names had prices: Singapore's published books averaged 5.7 names.

What moved by more than 1pp, and the verdict changes:

- **Switzerland** fell from 8.08% to 2.23%, and its lead over the SMI from 6.34pp to 0.49pp. Cash years rose from 1 to 8. Fewer than 10 Switzerland-domiciled companies passed the screen in 2000, 2001, 2003 and 2012, and in 2004, 2006 and 2009 11 to 14 passed but only 5 to 9 had a usable entry price.
- **Sweden** fell from 9.39% to 6.71% because of two years. 2006 and 2012 passed 11 and 12 names with 9 and 8 buyable, so they are now cash. The earlier files reported +52.60% and +22.03% for them.
- **Germany** fell from 6.62% to 4.07% and went from 1.58pp ahead of the DAX to 0.97pp behind it. It had no cash years before and has 6 now: fewer than 10 Germany-domiciled companies passed in 2000-2003 and 2006, and in 2005 12 passed with 9 buyable. The average book went from 41.9 names to 22.1.
- **Canada** fell from 9.80% to 7.90%. 2000 and 2001 are now cash (9 of 15 and 7 of 14 names buyable); the earlier files reported +31.19% and +16.00% for them.
- **India** fell from 13.65% to 11.87%, which turns a 1.60pp lead over the Sensex into a tie. One year does it: 2005 passed 11 names with 9 buyable and is now cash, where the earlier files reported +51.27%.
- **Hong Kong** rose from 3.65% to 5.32%. 2000 and 2001 are now cash (6 of 15 and 5 of 24 names buyable); the earlier files reported -14.79% and -20.82% for them.
- **UK** fell from 6.91% to 5.31%. 2000-2003 and 2009 are now cash: 11 to 17 pence-quoted names passed and 4 to 8 were buyable. The average book went from 54.8 names to 39.6.
- **Thailand** rose from 3.85% to 4.88%, and its shortfall against the SET Index narrowed from 1.28pp to 0.25pp. 2007 and 2008 are now cash (6 buyable each); the earlier files reported +12.74% and -26.66% for them.
- **Singapore** went from 12 cash years to 23 and became too thin to test, through the entry-price floor.
- **South Africa**, the market where the prior year fix adds the most companies, went from 1 invested year to 3 and stays too thin to test. 2016, 2018 and 2022 are invested now, with 11, 12 and 10 names. 2014, its one invested year before, is cash (6 of 13 names buyable).
- The other five markets moved by -0.41pp to +0.15pp. Benchmark CAGRs are unchanged in all 15.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 low-debt/backtest.py --exchange XETRA --output low-debt/results/returns_XETRA.json
python3 low-debt/backtest.py --exchange NYSE,NASDAQ,AMEX --output low-debt/results/returns_NYSE_NASDAQ_AMEX.json

# After all 15: rebuild the comparison file the charts read, then check it against the per-market files
python3 scripts/rebuild_comparison.py low-debt
python3 scripts/rebuild_comparison.py low-debt --check

# Charts from results/exchange_comparison.json, written to low-debt/charts/
python3 low-debt/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup, prior year fix and cash rule still apply)
python3 low-debt/backtest.py --exchange LSE --exit-method drop
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| India | `NSE` | `returns_NSE.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Thailand | `SET` | `returns_SET.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Taiwan | `TAI,TWO` | `returns_TAI_TWO.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Singapore | `SES` | `returns_SES.json` | |
| South Africa | `JNB` | `returns_JNB.json` | |

Use `--exchange` as shown. The exchange list sets the universe name written into the file, so `--preset us`, `--preset china`, `--preset taiwan` and `--preset southafrica` write a different name (US_MAJOR, China, Taiwan, JSE), and `--preset canada` adds TSXV.

`--global` isn't how these files were made. It loops over the same 15 exchange sets and writes the same file names next to whatever path `--output` gives, so it needs `--output low-debt/results/exchange_comparison.json`; `--output results/exchange_comparison.json` from the repository root lands outside this directory. A market that fails inside the loop is recorded as an error entry and its old file stays on disk, so run the `--check` above afterwards.

## Known limits

- The 45-day lag counts from the fiscal year end. Many companies publish annual results later than that, so a row can be used before it was public.
- The size floor compares the filing's market cap, in the company's reporting currency, with a floor set in the listing currency. Each period record lists the first 20 names held, largest filing market cap first, and the US records show the effect: the 2024 record starts with KSPI, NVO and GGAL, ahead of GOOG and NVDA. See DATA_QUALITY_ISSUES.md.
- The US market is every line on NYSE, NASDAQ and AMEX, so it holds foreign companies' depositary receipts as well as US companies.
- The entry floor of 1.00 is in local units: one Singapore dollar, one Hong Kong dollar, one penny on the LSE. Counting the years when 10 or more names passed the screen, 141 of Singapore's 233 screened names (60.5%) had no usable entry price. In the other markets that share runs from 3.9% (Japan) to 38.5% (South Africa). That is why Singapore is too thin to test. The files don't say how many of those names were under the floor and how many had no price at all.
- The screen has no fund filter. Among the first 20 recorded names, the US file holds mutual-fund-style tickers (five letters ending in X) in 3 of 25 years: ABALX in 2014 and 2015 and CDOCX in 2016. These were picked out by ticker, not by a fund flag in the data, and their effect on the results isn't measured.
- One line per ISIN isn't one line per company. Second share classes, preference shares and Thai `-R` lines have their own ISINs, so they are held beside the ordinary share and each counts as a holding. Among the first 20 recorded names, two or more lines with the same ticker root appear in all 16 of Thailand's invested years (PTTEP.BK with PTTEP-R.BK, for example), all 16 of Sweden's (ATCO-A.ST with ATCO-B.ST), 21 of Canada's 23 (MFC.TO with MFC-PB.TO and MFC-PC.TO), 13 of the US's 25 (BRK-A with BRK-B) and 2 of the UK's 20 (BP.L with BP-B.L in 2005 and 2006). Germany's 2024 record holds HEN.DE with HEN3.DE, FPE.DE with FPE3.DE and KSB.DE with KSB3.DE. Avg stocks counts lines, not companies. Each record lists 20 names only, so these counts are lower bounds.
- The four statements are each taken at their own latest row and aren't forced onto the same fiscal period.
- Japan's 2024 book holds 4298.T, whose prices in the data sit around 19 billion yen a share. Entry and exit are on the same scale, so its return for the year is usable, but the price levels are corrupt upstream.
- `screen.py` (the current-day screen) doesn't start at present: a syntax error stops Python from loading the file. It also predates the listing rules, the dedup and the prior year fix. The backtest doesn't use it.

## Data Requirements

- `financial_ratios` (FY): debtToEquityRatio
- `income_statement` (FY, two consecutive years): netIncome, grossProfit, revenue
- `balance_sheet` (FY, two consecutive years): totalAssets, totalCurrentAssets, totalCurrentLiabilities, longTermDebt, totalStockholdersEquity
- `cash_flow_statement` (FY): operatingCashFlow
- `key_metrics` (FY): marketCap
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen. Doesn't run at present (see Known limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for each market, plus CAGR and max drawdown for the 13 markets fit to rank. Chart files aren't committed, so run it locally |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
