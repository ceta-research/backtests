# Defensive Sector Quality

Buys profitable, dividend-paying companies in three defensive sectors (Consumer Defensive, Utilities and Healthcare) and holds the 30 with the highest dividend yield, at equal weight, for a year. The quality filters (return on equity, operating margin, debt to equity) are there to keep out companies whose yield is high because the business is in trouble.

## Academic Basis

- Novy-Marx (2013), "The Other Side of Value: The Gross Profitability Premium", *Journal of Financial Economics* 108(1), 1-28. Profitable companies earned higher returns than unprofitable ones, controlling for value.
- The screen combines that profitability tilt with a sector choice and a dividend-yield ranking. It is not a replication of a published portfolio.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Sector | Consumer Defensive, Utilities or Healthcare | `profile` (today's sector, applied to every year) |
| Return on equity | Above 6% | `key_metrics` FY |
| Operating margin | Above 8% | `financial_ratios` FY |
| Debt to equity | Below 2.5 (a missing value passes) | `financial_ratios` FY |
| Dividend yield | Above 0.5% (a missing value passes) | `financial_ratios` FY |
| Market cap | Above the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency: $1B in the US, ₹20B on NSE, ¥100B on JPX, £500M on LSE, €500M on XETRA, CHF 500M on SIX | `key_metrics` FY |

On each rebalance date, for every ordinary share on the market's exchanges:

1. Take the company's latest fiscal-year `key_metrics` row and latest fiscal-year `financial_ratios` row whose fiscal year ended at least 45 days and at most 540 days before the rebalance date.
2. Keep the company if it is in one of the three sectors and passes the five filters above.
3. Drop a dividend yield the screen can't trust: one above 25%, or one computed across two currencies that disagrees with the company's own dividends paid that year.
4. Keep one line per security (ISIN; a Thai NVDR shares its ordinary line's).
5. Rank by dividend yield, highest first, and hold the top 30 at equal weight.

Ordinary shares only: preferred and preference shares, notes, bonds, units, warrants, rights, funds, and second lines at their own price on a company's financials (Roche's ROG.SW beside RO.SW) never enter the universe. Both universe rules are described in METHODOLOGY.md (Universe Rules), and every results file records them and what they removed under `universe_rules`.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a close of at least 1.00 in local units in the 10-day entry window). A cash year earns 0%. The floor is checked at entry only, after the one-line-per-ISIN step.
- **Max stocks:** 30.
- **Weighting:** Equal weight.
- **Data checks:** No return cap. The data checks (`dc-1`, [METHODOLOGY.md](../METHODOLOGY.md#data-checks)) move holiday rows, zero-volume rows of lines that traded, stale lines with no volume, a thin share class's bar off its sibling's scale and non-positive prices out of each price window, hold a Thai NVDR line and its ordinary line once, test the market-cap floor on a cap checked against later reverse splits, and flag adjustment-factor steps, splits and every return above +100% or below -50% for review.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days after the fiscal year end (point-in-time). A fiscal year that ended more than 540 days before the rebalance isn't used.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's two classes can both be held (see What the books hold).
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Malaysia, Indonesia and South Africa have no local index configured and are measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK; 2.0% for Malaysia and Indonesia, which the table doesn't list).

## Results (July 2000 to July 2025)

Nine markets were invested in at least half of the 25 years; China appears twice below, as the Shenzhen plus Shanghai run the charts use and the Shanghai-only run the comparison post quotes. Of the eight with a local index, the screen beat its own market's index in seven: Hong Kong (+6.55pp a year), India (+6.31), China (+5.01 Shanghai only, +3.73 both exchanges), the UK (+4.65), Japan (+3.85), the US (+2.39) and Canada (+1.14). Thailand trails (-2.48pp). Indonesia is left out of the count because its benchmark is the S&P 500 in USD; it trailed that by 5.05pp. Germany, Malaysia, Taiwan, Switzerland, Korea, Sweden and South Africa are too thin to test and are reported separately below.

Six of the seven beats are against price indices that leave dividends out, while the portfolio's returns include them, so each margin is overstated by roughly the index's dividend yield. The one measured against a series that includes dividends is the US (SPY, +2.39pp).

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 18.36% | 12.06% | +6.31 | 0.539 | -2.84% | 4 (2000-2003) | 25.9 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 10.25% | 7.85% | +2.39 | 0.666 | -19.58% | 0 | 27.7 |
| Hong Kong (HKSE) | Hang Seng | 8.19% | 1.64% | +6.55 | 0.227 | -26.71% | 5 (2000-2004) | 22.5 |
| China, Shanghai only (SHH) | SSE Composite | 7.44% | 2.43% | +5.01 | 0.134 | -55.68% | 0 | 25.4 |
| Japan (JPX) | Nikkei 225 | 7.16% | 3.31% | +3.85 | 0.456 | -24.90% | 4 (2000-2003) | 27.8 |
| China (SHZ + SHH) | SSE Composite | 6.16% | 2.43% | +3.73 | 0.091 | -58.10% | 0 | 25.8 |
| UK (LSE) | FTSE 100 | 5.88% | 1.23% | +4.65 | 0.269 | -5.93% | 7 (2000-2004, 2009-2010) | 14.6 |
| Canada (TSX) | TSX Composite | 5.09% | 3.95% | +1.14 | 0.207 | -17.18% | 10 (2000-2003, 2005-2006, 2012, 2016, 2022, 2024) | 12.0 |
| Indonesia (JKT) | S&P 500 (SPY, in USD) | 2.80% | 7.85% | -5.05 | 0.076 | -15.21% | 11 (2000-2010) | 17.1 |
| Thailand (SET) | SET Index | 2.65% | 5.13% | -2.48 | 0.014 | -29.60% | 11 (2000-2010) | 20.0 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance. Avg stocks is the average number of lines held in invested years; two classes of one company count as two (see What the books hold).

- **US:** 10.25% a year against 7.85% for SPY, with a Sharpe of 0.666 against 0.352, a max drawdown of -19.58% against -38.01%, and no cash years. The screen passes 30 names every year; 26 to 30 of them have a usable entry price.
- **Canada:** the lead is 1.14pp with 10 cash years; in five of them 10 to 13 names passed and only 9 had a usable entry price, and five invested years sit at the floor of 10.
- **No return cap:** the 22 held returns above +200% are kept and listed in the results (`data_checks.large_moves`, and `ltp.large_moves` for two last-traded-price exits): India 4, China (SHZ + SHH) 7, China Shanghai only 6, Hong Kong 5, Malaysia 2. Among them Aurobindo Pharma from July 2013 (+310%), Top Glove from July 2019 (+269%) and YTL Power from July 2023 (+294%). Three carry an adjustment-factor flag (000423.SZ and 600380.SS from July 2006, during China's share-structure reform, and 1830.HK from July 2020); on the close alone they rose 239%, 166% and 175%.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Only SPY among these benchmarks includes them too. See METHODOLOGY.md (Benchmark Limitations).
- **Books at the floor:** India 2004, Thailand 2012, the UK 2016 and Canada 2004, 2010, 2011, 2013 and 2015 were invested with 10 buyable names, the floor. No invested year averages fewer than 10 lines.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Cash years

A year is in cash for one of two reasons, and each period record says which (`screened`, `entry_buyable`). The second column lists, in brackets, how many of the screened names had a usable entry price. The US and both China runs were invested in all 25 years.

| Market | Screen passed fewer than 10 | Fewer than 10 buyable at entry (buyable of screened) |
|--------|-----------------------------|------------------------------------------------------|
| India (NSE) | 2000-2003 | none |
| Canada (TSX) | 2000-2003, 2016 | 2005 (9 of 10), 2006 (9 of 13), 2012 (9 of 10), 2022 (9 of 10), 2024 (9 of 10) |
| Japan (JPX) | 2000-2003 | none |
| Hong Kong (HKSE) | 2000-2003 | 2004 (9 of 13) |
| UK (LSE) | 2000-2002, 2004 | 2003 (7 of 10), 2009 (9 of 10), 2010 (9 of 11) |
| Thailand (SET) | 2000-2009 | 2010 (9 of 10) |
| Indonesia (JKT) | 2000-2010 | none |
| Germany (XETRA) | 2000-2005, 2008-2010 | 2007 (9 of 10), 2019 (9 of 10), 2021 (8 of 10), 2022 (9 of 11) |
| Malaysia (KLS) | 2000-2006, 2009 | 2007 (9 of 11), 2008 (8 of 10), 2010 (6 of 10), 2011 (9 of 14), 2012 (9 of 14), 2013 (9 of 15) |
| Taiwan (TAI + TWO) | 2000-2016, 2018-2019 | 2017 (9 of 10) |
| Switzerland (SIX) | 2000-2006, 2009, 2012-2013 | 2007 (9 of 12), 2008 (8 of 11), 2010 (8 of 11), 2011 (8 of 11), 2014 (6 of 10), 2015 (9 of 13), 2016 (9 of 14), 2018 (9 of 12), 2021 (8 of 11), 2022 (8 of 10), 2024 (9 of 12) |
| Korea (KSC) | 2000-2012, 2014, 2018-2020, 2022-2023 | 2013 (9 of 11), 2021 (9 of 11), 2024 (9 of 10) |
| Sweden (STO) | 2000-2020, 2023 | 2021 (9 of 10), 2022 (9 of 10) |
| South Africa (JNB) | 2000-2024 | none |

A cash year earns 0% and is counted in every 25-year figure above, against whatever the index did that year. The next table sets the 25-year excess beside the excess over the invested years only: the portfolio's returns in the years it held stocks, compounded and annualised over that many years, against the index over the same years. The second reading is not a return anyone earned. It shows how the selected stocks did when there was a book.

| Market | Cash years | Index in the cash years (% a year) | Excess, 25 years (pp) | Portfolio, invested years | Index, invested years | Excess, invested years (pp) |
|--------|------------|------------------------------------|-----------------------|---------------------------|-----------------------|-----------------------------|
| India (NSE) | 4 of 25 | +0.13 | +6.31 | 22.23% | 14.48% | +7.74 |
| Canada (TSX) | 10 of 25 | +5.26 | +1.14 | 8.63% | 3.09% | +5.54 |
| Japan (JPX) | 4 of 25 | -9.68 | +3.85 | 8.58% | 5.99% | +2.59 |
| Hong Kong (HKSE) | 5 of 25 | -2.54 | +6.55 | 10.34% | 2.71% | +7.63 |
| UK (LSE) | 7 of 25 | +1.87 | +4.65 | 8.26% | 0.98% | +7.28 |
| Thailand (SET) | 11 of 25 | +11.81 | -2.48 | 4.79% | 0.16% | +4.62 |
| Indonesia (JKT) | 11 of 25 | +0.94 | -5.05 | 5.06% | 13.61% | -8.55 |

The sign changes in Thailand, from 2.48pp behind the SET Index over 25 years to 4.62pp ahead over its 14 invested years: the index rose 11.81% a year in the years the portfolio sat in cash. Canada's lead widens from 1.14pp to 5.54pp the same way, with the TSX up 5.26% a year in its cash years. Indonesia's index is the S&P 500 in USD in both readings. All of these indices are price indices, so the invested-years margins are overstated by the dividend yield as well.

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the screen. Its figures are listed so the files can be checked, not as results. In brackets: the number of names held that year.

| Market | Years in cash | Returns in the invested years | Avg stocks when invested | CAGR incl. cash years | Benchmark CAGR |
|--------|---------------|-------------------------------|--------------------------|-----------------------|----------------|
| Germany (XETRA) | 13 of 25 | 2006: +33.50% (10), 2011: +6.55% (12), 2012: +23.49% (10), 2013: +11.92% (10), 2014: +24.45% (11), 2015: +17.52% (11), 2016: +11.66% (11), 2017: +1.11% (10), 2018: +4.82% (10), 2020: +36.28% (10), 2023: -5.24% (11), 2024: -13.14% (11) | 10.6 | 5.50% | 5.04% (DAX) |
| Malaysia (KLS) | 14 of 25 | 2014: -10.09% (12), 2015: +5.36% (12), 2016: -1.78% (10), 2017: +11.61% (11), 2018: -6.17% (15), 2019: +41.12% (11), 2020: -6.67% (15), 2021: -7.27% (17), 2022: +2.84% (21), 2023: +27.78% (20), 2024: -2.32% (18) | 14.7 | 1.71% | 7.85% (S&P 500 in USD) |
| Taiwan (TAI + TWO) | 20 of 25 | 2020: +23.00% (11), 2021: -11.38% (13), 2022: +13.31% (12), 2023: +7.80% (11), 2024: -10.45% (19) | 13.2 | 0.71% | 4.09% (TAIEX) |
| Switzerland (SIX) | 21 of 25 | 2017: +5.76% (10), 2019: +7.55% (10), 2020: +34.89% (11), 2023: -3.73% (12) | 10.8 | 1.57% | 1.74% (SMI) |
| Korea (KSC) | 22 of 25 | 2015: -3.54% (11), 2016: -17.55% (13), 2017: -10.95% (10) | 11.3 | -1.37% | 5.35% (KOSPI) |
| Sweden (STO) | 24 of 25 | 2024: +0.69% (12) | 12.0 | 0.03% | 2.55% (OMX Stockholm 30) |
| South Africa (JNB) | 25 of 25 | none | 0 | 0.00% | 7.85% (S&P 500 in USD) |

- **Germany:** fewer than 10 Germany-domiciled companies passed the screen in 2000-2005 and 2008-2010, and in 2007, 2019, 2021 and 2022 10 or 11 passed but only 8 or 9 had a usable entry price. Six of its 12 invested books sit at the floor of 10. Before the rerun its books also held four preference lines beside their ordinary lines (HEN3, SRT3, DRW3, BIO3), which kept three of these years invested.
- **Switzerland:** fewer than 10 Switzerland-domiciled companies passed the screen in 2000-2006, 2009 and 2012-2013. In 11 more years 10 to 14 passed but only 6 to 9 had a usable entry price. Before the rerun every invested book held Roche twice (RO.SW and ROG.SW); with Roche once, six of those ten years are in cash.
- **Korea:** with its preferred lines out, fewer than 10 names pass the screen in 19 of the 25 years.
- **Malaysia:** in 6 of its 14 cash years 10 to 15 names passed and only 6 to 9 had a usable entry price.
- **Taiwan, Sweden, South Africa:** fewer than 10 names pass the screen in almost every year. South Africa never reaches 10.

### What the books hold

`holdings` in each period record is the priced book. The universe rules keep preferred lines, notes, bonds, units, warrants, rights, funds and second lines at their own price out of it, and each results file lists what they removed (`universe_rules`). What the books still hold that isn't one line per company:

- **Share classes:** Canada holds two classes of one company together (ACO-X and ACO-Y in 13 years, CSW-A and CSW-B in 8, CU and CU-X in 8, ADW-A and ADW-B once; 30 of its 180 line-years are a company's second class). China (SHZ + SHH) holds Guangdong Electric's A and B lines (000539.SZ, 200539.SZ) together in 11 years, and Sweden Essity A and B in its one invested year.
- **Thai NVDRs:** check N collapsed 255 ordinary and NVDR pairs in the screens to one line each. In 20 the NVDR stood in, because the ordinary line had no usable entry price.

What the rules took out, by market (lines leave the universe; yield drops are counted over every screen, so one line can drop in several years):

| Market | Not ordinary shares (lines) | Yield drops (line-years) | Held in the books before the rerun |
|--------|-----------------------------|--------------------------|------------------------------------|
| US | 329: 219 NASDAQ fifth letters, 53 hyphen suffixes, 29 hand-listed, 27 by name, 1 fund | 95: 58 cross-currency, 19 above 25%, 18 unverifiable | notes, bonds, units and preferred ADRs in 67 of 542 line-years (12.4%, 10 to 12 of 24 to 28 lines a year in 2020-2024); by the yield rule FEMSA (FMX) in 24 years, Embotelladora Andina (AKO-A, AKO-B) 14, Sabesp (SBS) 4, BTI 3 and 7 others once or twice |
| Korea | 27 preferred | none | 43 of 165 line-years (26.1%) |
| Canada | 12 preferred series, 1 fund | 3 | 17 of 226 line-years (7.5%) |
| Germany | 5 preference lines | 1 | 27 of 181 line-years (14.9%) |
| Switzerland | 3 hand-listed (ROG.SW, ROP.SW, LISP.SW) | 4 unverifiable | ROG.SW in all 10 invested years; Energiedienst (EDHN.SW) twice by the yield rule |
| UK | 1 fund (TRIG.L), 1 preference line, 1 by name | 20: 11 cross-currency, 5 unverifiable, 4 above 25% | TRIG.L in 6 of 270 line-years; 3 lines once each by the yield rule |
| Hong Kong | none | 38: 27 cross-currency, 6 unverifiable, 5 above 25% | H-shares by the yield rule: China Longyuan Power (0916.HK) in 11 years, CGN Power (1816.HK) 4, and 9 others once or twice |
| China (SHZ + SHH) | none | 22 cross-currency | the B-share lines 200869.SZ in 10 years and 200539.SZ in 3 |
| India | 2 funds | none | an infrastructure trust unit once |
| Japan | 1 preferred, by name | 1 above 25% | none |

Thailand had 2 yield drops above 25%, South Africa 3 unverifiable, and Taiwan, Sweden, Malaysia and Indonesia 1 each. China (SHH alone) lost nothing to either rule.

## Rerun November 2026

These results replace the October 2026 files. The screen's filters didn't change; three things around them did:

- **Data checks in place of the 200% cap.** A holding's return above 200% used to be dropped as a data error. It's now kept, and the data checks remove the errors (METHODOLOGY.md, Data Checks). The 22 kept returns above +200% are the gains the cap dropped before: India 4, China (SHZ + SHH) 7, China Shanghai only 6, Hong Kong 5, Malaysia 2. Korea's two from July 2014 don't arise, because 2014 is now a cash year.
- **Ordinary shares only.** Preferred lines, notes, bonds, units, warrants, rights, funds and second lines at their own price leave the universe (What the books hold).
- **The yield rule.** A yield above 25%, or a cross-currency yield that disagrees with the company's own dividends paid, no longer ranks.

What moved, and the verdict changes:

- **US:** the lead over SPY went from +2.77pp to +2.39pp. FEMSA's mis-scaled yield and the notes, bonds and units are out, and the average book went from 21.7 lines to 27.7: before, warrants, units and rights with no prices took slots and left them empty.
- **Germany** went from +0.88pp ahead of the DAX to too thin to test: 13 cash years instead of 10. In 2007, 2019 and 2022 a preference line had made up the tenth name.
- **Korea** went from 1.88pp behind the KOSPI to too thin to test: with its preferred lines out, 22 cash years instead of 11.
- **Switzerland** went from +2.18pp ahead of the SMI to a tie at -0.17pp, still too thin: with Roche held once, 21 cash years instead of 15.
- **Canada**'s lead narrowed from +3.35pp to +1.14pp: its preferred series are out, and it has 10 cash years instead of 7.
- **Hong Kong** rose from +5.24pp to +6.55pp, **China (SHZ + SHH)** from +2.85pp to +3.73pp, **China Shanghai only** from +3.52pp to +5.01pp, **India** from +5.45pp to +6.31pp and **Malaysia** from -7.32pp to -6.14pp, mostly on the gains the cap used to drop. Hong Kong also lost H-shares and China its B-share lines to the yield rule.
- **Thailand** went from -2.26pp to -2.48pp: check N holds each ordinary and NVDR pair once, and 2008 and 2010 are now cash years too.
- Japan, the UK, Taiwan, Indonesia, Sweden and South Africa moved by 0.15pp or less. Benchmark CAGRs are unchanged.

## Rerun October 2026

These results were rerun with the campaign's changes, described in METHODOLOGY.md (Universe Construction, Filing and Selection Rules, and Limitations: Survivorship Bias). The previous files were the April 2026 runs (China SHZ + SHH: March), made before several code changes, so the move from them combines all of these:

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 15 holdings in 8 of the 17 results files exited this way (600795.SS counts in both China runs); each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.07pp to +0.08pp.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market, before the top-30 cut.
- **Delisted names can be selected.** The universe used to be today's actively traded lines only (`isActivelyTrading = true`), which left out every company that has since stopped trading. That filter is gone, and a filing older than 540 days is no longer used, so a delisted company can't hold a slot on its last filing for years.
- **Entry-side cash rule.** The previous files decided cash on the screen count alone and averaged whatever names had prices: Switzerland's books averaged 8.3 names. A year now needs 10 screened names with a usable entry price.
- **Price cleaning and local benchmarks.** Prices pass the oscillation filter for phantom holiday rows. The China (SHZ + SHH) file was measured against the S&P 500 before; it is now measured against the SSE Composite, like every other market with a local index.

What moved by more than 1pp, and the verdict changes:

- **UK** fell from 11.19% to 5.73%, and its lead over the FTSE 100 from 9.96pp to 4.51pp. It had no cash years before and has 7 now: fewer than 10 pence-quoted names passed the screen in 2000-2002, and in 2003, 2004, 2009 and 2010 10 to 12 passed but only 7 to 9 had a usable entry price. The earlier file reported +25.71% to +27.52% in five of those seven years.
- **Malaysia** fell from 2.90% to 0.53% and is now too thin to test: 2007, 2008 and 2010-2013 are cash (6 to 9 of 10 to 15 names buyable).
- **Thailand** fell from 4.99% to 2.88%, which turns a tie with the SET Index (0.14pp behind) into a 2.26pp shortfall. 2006, 2007 and 2009 are now cash (7, 9 and 8 names buyable); the earlier file reported +43.02%, -6.42% and +18.79% for them.
- **Hong Kong** fell from 8.67% to 6.88%. 2003 and 2004 are now cash (6 of 10 and 9 of 14 names buyable); the earlier file reported +44.42% and +5.75% for them.
- **Switzerland** fell from 5.45% to 3.92% and is now too thin to test, with 15 cash years instead of 5.
- **China (SHZ + SHH)** moved from 5.14% to 5.28%, and from 2.69pp behind the S&P 500 to 2.85pp ahead of the SSE Composite: the benchmark changed, not the result.
- **Germany** fell from 6.78% to 5.92% and has 10 cash years instead of none, under the listing rule; its lead over the DAX narrowed from 1.74pp to 0.88pp. The average book went from 21.8 names to 12.1.
- The other markets moved by -0.74pp to +0.34pp. Benchmark CAGRs are unchanged everywhere except China (SHZ + SHH).

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 defensive-quality/backtest.py --exchange LSE --output defensive-quality/results/returns_LSE.json
python3 defensive-quality/backtest.py --exchange NYSE,NASDAQ,AMEX --output defensive-quality/results/returns_NYSE_NASDAQ_AMEX.json

# After all 17: rebuild the comparison file the charts read (16 markets: SHH is left out,
# the charts' China is SHZ_SHH), then check it against the per-market files
python3 scripts/rebuild_comparison.py defensive-quality --keep NYSE_NASDAQ_AMEX NSE SHZ_SHH HKSE SET JPX LSE XETRA TAI_TWO KSC TSX SIX STO JNB KLS JKT
python3 scripts/rebuild_comparison.py defensive-quality --keep NYSE_NASDAQ_AMEX NSE SHZ_SHH HKSE SET JPX LSE XETRA TAI_TWO KSC TSX SIX STO JNB KLS JKT --check

# Charts, written to defensive-quality/charts/
python3 defensive-quality/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup, cash rule, data checks and
# universe rules still apply)
python3 defensive-quality/backtest.py --exchange LSE --exit-method drop

# Every booked name per invested year (holdings_full), for review. --data-checks uncapped turns the data
# checks off (no cap either): an attribution arm only, never published
python3 defensive-quality/backtest.py --exchange LSE --full-holdings

# Current screen (what passes today)
python3 defensive-quality/screen.py --exchange NYSE,NASDAQ,AMEX
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| China, Shanghai only | `SHH` | `returns_SHH.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Thailand | `SET` | `returns_SET.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Taiwan | `TAI,TWO` | `returns_TAI_TWO.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Sweden | `STO` | `returns_STO.json` | |
| Malaysia | `KLS` | `returns_KLS.json` | |
| Indonesia | `JKT` | `returns_JKT.json` | |
| South Africa | `JNB` | `returns_JNB.json` | |

Use `--exchange` as shown. The exchange list sets the universe name written into the file, so a `--preset` run writes a different name (`--preset us` writes US_MAJOR) and `--preset canada` adds TSXV.

`--global` isn't how these files were made. It runs 15 of the 17 markets (not SHH or JNB), writes the per-market files next to whatever path `--output` gives, and writes all 15 into that file, so it needs `--output defensive-quality/results/exchange_comparison.json` and is then followed by the `--keep` rebuild above. A market that fails inside the loop is recorded as an error entry and its old file stays on disk, so run the `--check` afterwards.

## Known limits

- **Survivorship.** Not removed. The data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), so every result for 2000-2014 in the US, and for 2000-2019 elsewhere, is measured on companies that survived at least to 2015 or 2020. Removing the active-listings filter lets the screen pick a company that stopped trading after those dates; it can't bring back the earlier ones. See METHODOLOGY.md (Limitations: Survivorship Bias).
- **Sectors are today's.** The sector comes from the current `profile` snapshot and is applied to every year. A company that moved into or out of the three sectors is classified by where it sits now.
- The 45-day lag counts from the fiscal year end. Many companies publish annual results later than that, so a row can be used before it was public.
- The size floor compares the filing's market cap, in the company's reporting currency, with a floor set in the listing currency. A US-listed ADR of a company reporting in pesos or yen is measured in those units. See DATA_QUALITY_ISSUES.md.
- **The ranking key is a vendor ratio.** The yield rule removes yields above 25% and cross-currency yields that disagree with the company's own dividends paid. A wrong yield that agrees with itself under 25% stays, and a cross-currency line whose cash flow shows no dividends paid that year drops even when its yield is right.
- **The ordinary-shares rule is a list of patterns plus a hand list.** A plain-named note, unit or second line the probe didn't find gets in; for US lines the results' `cik_shared` report shows candidates.
- The US market is every line on NYSE, NASDAQ and AMEX, so it holds foreign companies' depositary receipts of ordinary shares as well as US companies.
- One line per security isn't one line per company. Second share classes (ACO-X and ACO-Y, Essity A and B) have their own ISINs, so both can be held and each counts as a holding. Avg stocks counts lines, not companies.
- **Unpriced names.** A screened name with no usable entry price takes its top-30 slot and isn't replaced; each period's `entry_unpriced` lists them. Some are delisted names with no price rows; some, like Tyson Foods (TSN) and Essential Utilities (WTRG), are lines whose price history starts on 2026-04-01 in the data (DATA_QUALITY_ISSUES.md, Price history that starts in April 2026).
- The entry floor of 1.00 is in local units: one Hong Kong dollar, one penny on the LSE, one baht.
- `screen.py` (the current-day screen) keeps ordinary shares only and the 25% yield ceiling, but not the listing rules, the dedup or the cross-currency check (its TTM rows give no same-year dividends paid and cap). The backtest doesn't use it.

## Data Requirements

- `key_metrics` (FY): returnOnEquity, marketCap
- `financial_ratios` (FY): operatingProfitMargin, debtToEquityRatio, dividendYield, reportedCurrency
- `cash_flow_statement` (FY): commonDividendsPaid, netDividendsPaid, reportedCurrency (the yield rule)
- `stock_eod`: adjClose, close and volume after each rebalance date, and the last traded price for exits
- `income_statement`, `stock_prices_non_split_adjusted_daily`, `splits_calendar`: check M's inputs and check F
- `profile`: sector, exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break, yield rule), isin and isActivelyTrading (dedup tie-break), companyName, isFund, isEtf and cik (ordinary-shares rule)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` and, for India and the UK, `results/returns_NSE.json` and `results/returns_LSE.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` (16 markets) |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for the US, India, China (SHZ + SHH) and the UK, each against its own index with the cash years marked, plus CAGR and max drawdown for every market invested in at least half of the 25 years. The too-thin markets are named in the footer of those two charts, not drawn. Chart files aren't committed, so run it locally |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
