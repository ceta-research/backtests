# Rising Dividend Yield

Stocks whose dividend yield has risen for three fiscal years in a row because the dividend grew, not because the price fell. The strategy buys the 30 with the largest two-year rise in yield in each market once a year, after quality and size filters.

## Academic Basis

- Campbell, J.Y. & Shiller, R.J. (1988), "The Dividend-Price Ratio and Expectations of Future Dividends and Discount Factors", *Review of Financial Studies* 1(3): 195-228.
- Cochrane, J.H. (2008), "The Dog That Did Not Bark: A Defense of Return Predictability", *Review of Financial Studies* 21(4): 1533-1575.

Rising yield is not the same as dividend growth. A company can raise its dividend 10% a year and still see its yield fall if the price rises 15%. This screen follows the yield itself and then requires that the dividend per share went up too.

## Signal

On each rebalance date, for every company on the market's exchanges:

1. Take the company's latest fiscal-year row in `financial_ratios` whose fiscal year ended at least 45 days before the rebalance date and at most 540 days before it. A company with no row in that window is out for the year.
2. **Rising yield.** That row's dividend yield must be above the previous fiscal year's, and that one above the year before, on three consecutive fiscal years with none missing. The earliest of the three must be above zero, so a zero or missing yield breaks the run.
3. **Driver.** Dividend per share on that row must be above the previous fiscal year's.
4. **Payout.** The row's payout ratio must be between 0% and 75%.
5. **Quality and size.** On the latest `key_metrics` row in the same window, return on equity must be above 10% and market cap above the market's floor in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`): $1B in the US, ₹20B on NSE, ¥100B on JPX, £500M on LSE, C$500M on TSX, €500M on XETRA.
6. Keep one line per ISIN.
7. Rank by the two-year change in dividend yield (the latest yield minus the yield two fiscal years earlier), largest first, then symbol, and buy the top 30 at equal weight.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a close of at least 1.00 in local units in the 10-day entry window). A cash year earns 0%. The floor is checked at entry only.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Return cap:** A single holding's yearly return above 200% is treated as a data error and left out of that year's average.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days after the fiscal year end (point-in-time).
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's ordinary and preference shares can both be held.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. China (SHZ + SHH) is measured against the SSE Composite. South Africa has no local index configured and is measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 3.5% UK, 2.5% Canada).

## Results (July 2000 to July 2025)

Of 17 markets, 8 were invested in at least half of the 25 years. In those 8 the strategy beat its own market's index in 5 (the US, Canada, China, the UK and Hong Kong), tied it in 1 (Japan, +0.19pp, under the 0.2pp that counts as a difference) and trailed it in 2 (India and Taiwan). The other 9 markets are too thin to test and are reported separately below.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 10.06% | 7.85% | +2.21 | 0.397 | -34.46% | 0 | 25.0 |
| India (NSE) | Sensex | 8.26% | 12.06% | -3.80 | 0.071 | -20.23% | 9 (2000-2006, 2011, 2022) | 22.5 |
| Canada (TSX) | TSX Composite | 6.62% | 3.95% | +2.67 | 0.318 | -22.70% | 7 (2001-2002, 2004-2006, 2011, 2014) | 17.6 |
| China (SHZ + SHH) | SSE Composite | 5.57% | 2.43% | +3.14 | 0.084 | -54.70% | 1 (2000) | 21.7 |
| UK (LSE) | FTSE 100 | 4.01% | 1.23% | +2.78 | 0.031 | -41.09% | 8 (2000-2002, 2004-2006, 2011, 2021) | 17.9 |
| Japan (JPX) | Nikkei 225 | 3.50% | 3.31% | +0.19 | 0.173 | -41.44% | 9 (2000-2006, 2010-2011) | 25.8 |
| Taiwan (TAI) | TAIEX | 3.46% | 4.09% | -0.63 | 0.218 | -17.93% | 12 (2000-2008, 2010-2011, 2014) | 19.9 |
| Hong Kong (HKSE) | Hang Seng | 3.06% | 1.64% | +1.42 | 0.003 | -42.69% | 6 (2000-2005) | 22.6 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance. Avg stocks is the average number of lines held in invested years; two share lines of one company count as two (see Known limits).

- **US:** 10.06% a year against 7.85% for SPY, with a Sharpe of 0.397 against 0.352, a max drawdown of -34.46% against -38.01%, and no cash years. It is the only one of the 8 measured against a benchmark that includes dividends.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). The other seven markets are measured against price indices, so their excess is overstated by roughly the index's dividend yield. None of the four non-US leads (China +3.14pp, the UK +2.78pp, Canada +2.67pp, Hong Kong +1.42pp) has been checked against a total-return index. India and Taiwan trail a price index even with that advantage. See METHODOLOGY.md (Benchmark Limitations).
- **Japan is a tie:** 3.50% against 3.31% for the Nikkei 225, a price index, after 9 cash years.
- **Taiwan is at the boundary:** it was invested in 13 of 25 years, the fewest that still counts as half.
- **Cash years:** the 8 markets hold 52 cash years between them. 38 are cash because fewer than 10 names passed the screen. 14 are cash because 10 or more passed but fewer than 10 had a usable entry price: all 8 of the UK's (2 to 9 priced names out of 10 to 24 screened), India 2011 (7 of 15) and 2022 (5 of 11), Canada 2004 (9 of 15) and 2005 (8 of 11), and Hong Kong 2003 (7 of 11) and 2005 (8 of 14). The cash years aren't confined to the early part of the window: the UK was in cash in 2021 and India in 2022. The US was invested in all 25 years.
- **Books at the floor:** Canada 2021, the UK 2014 and Japan 2014 were invested with exactly 10 buyable names. No invested year averages fewer than 10 lines. Canada 2021's 10 lines sit on 7 ticker roots (QBR-A.TO with QBR-B.TO, RCI-A.TO with RCI-B.TO, CCL-A.TO with CCL-B.TO), and its return rounds to 0.00%; it is an invested year, not a cash year.
- **Names left out at exit:** a holding whose yearly return is above the 200% cap is left out of that year's average and not replaced. China 2006 averaged 17 of its 24 buyable names, India 2023 averaged 25 of 28.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the strategy. Its figures are listed so the files can be checked, not as results.

| Market | Years in cash | Years invested | Avg stocks when invested | CAGR incl. cash years |
|--------|---------------|----------------|--------------------------|-----------------------|
| Korea (KSC) | 14 of 25 | 2012-2013, 2015-2020, 2022-2024 | 18.3 | 2.29% |
| Germany (XETRA) | 15 of 25 | 2008-2009, 2013, 2016-2020, 2023-2024 | 14.5 | 0.66% |
| Sweden (STO) | 17 of 25 | 2008-2009, 2012-2013, 2018-2019, 2023-2024 | 14.9 | 4.73% |
| Australia (ASX) | 19 of 25 | 2013, 2016, 2019, 2022-2024 | 16.5 | 2.77% |
| Thailand (SET) | 19 of 25 | 2017-2020, 2022, 2024 | 17.2 | -0.51% |
| Brazil (SAO) | 20 of 25 | 2012, 2019, 2022-2024 | 13.0 | 3.05% |
| Switzerland (SIX) | 21 of 25 | 2008-2009, 2016, 2024 | 13.8 | 1.72% |
| South Africa (JNB) | 25 of 25 | none | 0 | 0.00% |
| Singapore (SES) | 25 of 25 | none | 0 | 0.00% |

- **Germany:** in 14 of the 15 cash years, 0 to 8 Germany-domiciled companies passed the screen. In 2010 exactly 10 passed and 9 had a usable entry price.
- **Switzerland:** in 19 of the 21 cash years, 0 to 9 Switzerland-domiciled companies passed. In 2019 and 2023 exactly 10 passed and 9 and 8 had a usable entry price.
- **Australia:** 8 cash years on the screen count (4 to 9 names passed) and 11 where 11 to 16 passed but only 6 to 9 had a usable entry price. All six invested years had a positive return, so the file's max drawdown is 0.00%. That says nothing about the strategy's risk.
- **Sweden, Korea, Brazil and Thailand:** most cash years are on the screen count (15 of 17, 13 of 14, 14 of 20 and 14 of 19). The rest passed the count and had fewer than 10 priced names.
- **South Africa and Singapore** were never invested. South Africa's screen passed 0 to 9 names in 22 years and 10 to 15 in three (2013, 2018, 2019), when 5 to 8 had a usable entry price. Singapore's passed 0 to 8 in 24 years and 10 in 2008, when 8 did.
- Thailand's books hold the ordinary line and the non-voting depositary receipt (`-R`) of the same company as two holdings, so its 17.2 average lines are fewer companies (see Known limits).

## Rerun October 2026

These results were rerun with the campaign's three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias), and one fix to this topic's screen.

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 29 holdings in 10 markets exited this way, and none stayed unpriced; each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.10pp to +0.12pp.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **Latest filing first (this topic's fix).** The screen used to take each company's newest row that passed the three-year rising test, with no age limit, and it dropped non-paying years before counting "consecutive" years. A company kept qualifying on its last rising year, however old: an audit before the fix found 59% to 77% of top-30 slots on a row older than the last complete fiscal year (DATA_QUALITY_ISSUES.md). The test must now hold on the latest fiscal-year row, at most 540 days old, over three consecutive fiscal years, and a zero or missing yield breaks the run. The earlier screen also read only fiscal years ending in the calendar year before the rebalance or earlier. The rerun reads the latest year end at least 45 days old, so a company with a March year end, as most in India and Japan have, is screened in July on the year that ended three months earlier, not fifteen.

The earlier files also predated the entry-price half of the cash rule: they decided cash on the screen count alone and averaged whatever names had prices.

The fix removes most of what the earlier results reported. Cash years across the 17 markets rose from 65 to 227 of 425. Before, 12 of 17 markets beat their index and none was too thin to test; now 5 of 8 do and 9 are too thin. CAGR fell in 15 of 17 markets. The earlier results files record no holdings, so the split between the fix, the listing rules, the dedup and the cash rule can't be read from the files. A replay of the old and the fixed screen made before the rerun put most of the added cash years on the fix.

- **Germany** fell from 8.94% to 0.66% and went from 0 cash years to 15. It led the DAX by 3.90pp and is now too thin to test.
- **Switzerland** went from 0 cash years to 21, **Australia** from 0 to 19, **Sweden** from 5 to 17, **Brazil** from 6 to 20, **Thailand** from 6 to 19 and **Korea** from 10 to 14. All six are now too thin to test. **South Africa** and **Singapore** (8 cash years each before) are never invested.
- **India** fell from 13.44% to 8.26%, which turns a 1.38pp lead over the Sensex into a 3.80pp shortfall. Two more years are cash (2011 and 2022), and the books in the years invested both times hold different names.
- **Japan** fell from 8.94% to 3.50%, and its lead over the Nikkei 225 from 5.63pp to 0.19pp, a tie. Cash years rose from 4 (2000-2003) to 9: 5 to 9 names passed the screen in 2004-2006, 2010 and 2011.
- **Taiwan** fell from 7.02% to 3.46% and now trails the TAIEX by 0.63pp, after leading it by 2.93pp. Cash years rose from 8 to 12.
- **Hong Kong** fell from 5.89% to 3.06% (cash years 3 to 6) and the **UK** from 6.56% to 4.01%. The UK's cash years rose from 0 to 8, all of them years where the screen passed 10 or more pence-quoted names and fewer than 10 had a usable entry price. Its average book in invested years rose from 14.5 names to 17.9.
- **Canada** fell from 8.48% to 6.62% and went from 0 cash years to 7.
- **The US** rose from 9.54% to 10.06% with no cash years before or after, and its max drawdown deepened from -28.42% to -34.46%. **China** moved from 5.53% to 5.57%.
- Benchmark CAGRs are unchanged in all 17 markets.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 rising-yield/backtest.py --exchange XETRA --output rising-yield/results/returns_XETRA.json

# After all 17: rebuild the comparison file the charts read, then check it against the per-market files
python3 scripts/rebuild_comparison.py rising-yield
python3 scripts/rebuild_comparison.py rising-yield --check

# Charts from results/exchange_comparison.json
python3 rising-yield/generate_charts.py

# The old exit rule (doesn't reproduce old results: the fix, listing rules, dedup and cash rule still apply)
python3 rising-yield/backtest.py --exchange LSE --exit-method drop

# Current screen (see Known limits)
python3 rising-yield/screen.py
python3 rising-yield/screen.py --preset india
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Japan | `JPX` | `returns_JPX.json` | |
| Taiwan | `TAI` | `returns_TAI.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Sweden | `STO` | `returns_STO.json` | |
| Australia | `ASX` | `returns_ASX.json` | |
| Thailand | `SET` | `returns_SET.json` | |
| Brazil | `SAO` | `returns_SAO.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| South Africa | `JNB` | `returns_JNB.json` | |
| Singapore | `SES` | `returns_SES.json` | |

Use `--exchange` as shown. `--preset canada` adds TSXV and `--preset taiwan` adds TWO, so neither reproduces the TSX or TAI file, and `--preset us`, `--preset china` and `--preset southafrica` write a different `universe` name into the file than the charts expect.

`--global` isn't how these files were made. It loops over the same 17 exchange sets and writes the per-market files next to whatever path `--output` gives, so from the repository root it needs `--output rising-yield/results/exchange_comparison.json`. A market that raises an error in the loop is recorded in the combined file and its earlier per-market file stays on disk.

The committed charts were drawn with matplotlib's default backend on macOS. Under `MPLBACKEND=Agg` the same charts come out 1 to 2 pixels narrower.

## Known limits

- The 45-day lag counts from the fiscal year end. Many companies publish annual results later than that, so a row can be used before it was public.
- The size floor compares the filing's market cap, in the company's reporting currency, with a floor set in the listing currency.
- The ranking key is the absolute two-year change in yield with no cap, so a one-off distribution or a wrong yield in the data can take a top slot.
- The screen has no fund filter. A fund whose ticker carries company fundamentals in the data can pass and be held. The US file holds mutual-fund-style tickers (five letters ending in X) in 6 of the 25 years: FMIHX, ICMAX and FTEMX in 2016, TGABX and FTEMX in 2017, TGABX and DHLYX in 2018, FLCSX in 2019, FLATX in 2022, and JTSSX, SMTAX, FNILX and ICMAX in 2023. These were picked out by ticker, not by a fund flag in the data. The backtest has no option to leave funds out, so their effect on the results isn't measured.
- One line per ISIN isn't one line per company. Preference shares, second share classes and Thai non-voting depositary receipts have their own ISINs, so they are held beside the ordinary share and each takes one of the 30 slots. Grouped by ticker root, two or more lines of one issuer appear in all 18 of Canada's invested years, all 8 of Sweden's, all 6 of Thailand's, 10 of Korea's 11, 8 of Germany's 10, 4 of Brazil's 5, 4 of the US's 25 and 2 of the UK's 17. Canada 2019 held 20 lines on 11 roots: GWO.TO with GWO-PI.TO, GWO-PM.TO and GWO-PN.TO, MFC.TO with MFC-PB.TO, MFC-PC.TO and MFC-PM.TO, NA.TO with NA-PC.TO and NA-PE.TO, and CCL-A.TO with CCL-B.TO. Korea 2024 held 25 lines on 18 roots, four of them one issuer's (005380.KS with its preference lines 005385.KS, 005387.KS and 005389.KS). Germany 2019 held BMW.DE with BMW3.DE, HEN.DE with HEN3.DE and VOW.DE with VOW3.DE. The US 2024 book holds three preference lines of one issuer (AHL-PC, AHL-PD, AHL-PE). Avg stocks counts lines, not companies.
- Lines are grouped by ticker root, so the counts in the point above are lower bounds, and a shared root isn't proof of one issuer.
- South Africa has no local index in the data, so its file compares a rand portfolio with the S&P 500 in USD. It was never invested, so nothing rests on that comparison here.
- `generate_charts.py` leaves Australia and Brazil off the two comparison charts by name, and leaves every too-thin market out of the ranking with a note under the chart. Germany's two charts are stamped "TOO THIN TO TEST".
- `screen.py` (the current-day screen) still has the old shape: it drops non-paying years, takes each company's newest row that passes the rising test with no age limit, and applies neither the listing rules nor the dedup.

## Data Requirements

- `financial_ratios` (FY): dividendYield, dividendPerShare, dividendPayoutRatio (every fiscal-year row, paying or not)
- `key_metrics` (FY): returnOnEquity, marketCap
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (see Known limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for 6 markets (US, India, Germany, Canada, Japan, UK), plus CAGR and max drawdown by market |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
