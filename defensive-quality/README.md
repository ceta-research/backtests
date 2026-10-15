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

On each rebalance date, for every company on the market's exchanges:

1. Take the company's latest fiscal-year `key_metrics` row and latest fiscal-year `financial_ratios` row whose fiscal year ended at least 45 days and at most 540 days before the rebalance date.
2. Keep the company if it is in one of the three sectors and passes the five filters above.
3. Keep one line per ISIN.
4. Rank by dividend yield, highest first, and hold the top 30 at equal weight.

## Parameters

- **Rebalancing:** Annual, on July 1, executed at the next available close. 25 periods, July 2000 to July 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a close of at least 1.00 in local units in the 10-day entry window). A cash year earns 0%. The floor is checked at entry only, after the one-line-per-ISIN step.
- **Max stocks:** 30.
- **Weighting:** Equal weight.
- **Return cap:** A single holding's yearly return above 200% is treated as a data error and left out of that year's average.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days after the fiscal year end (point-in-time). A fiscal year that ended more than 540 days before the rebalance isn't used.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes, preference shares and Thai NVDR lines have their own ISINs, so a company's two lines can both be held (see What the books hold).
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. Malaysia, Indonesia and South Africa have no local index configured and are measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK; 2.0% for Malaysia and Indonesia, which the table doesn't list).

## Results (July 2000 to July 2025)

Eleven markets were invested in at least half of the 25 years; China appears twice below, as the Shenzhen plus Shanghai run the charts use and the Shanghai-only run the comparison post quotes. Of the ten with a local index, the screen beat its own market's index in eight: India (+5.45pp a year), Hong Kong (+5.24), the UK (+4.51), Japan (+3.78), China (+3.52 Shanghai only, +2.85 both exchanges), Canada (+3.35), the US (+2.77) and Germany (+0.88). Korea (-1.88pp) and Thailand (-2.26pp) trail. Indonesia is left out of the count because its benchmark is the S&P 500 in USD; it trailed that by 5.04pp. Switzerland, Taiwan, Malaysia, Sweden and South Africa are too thin to test and are reported separately below.

Six of the eight beats are against price indices that leave dividends out, while the portfolio's returns include them, so each margin is overstated by roughly the index's dividend yield. The two measured against a series that includes dividends are the US (SPY, +2.77pp) and Germany (DAX, +0.88pp).

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 17.50% | 12.06% | +5.45 | 0.540 | -2.84% | 4 (2000-2003) | 25.8 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 10.62% | 7.85% | +2.77 | 0.631 | -20.64% | 0 | 21.7 |
| Canada (TSX) | TSX Composite | 7.31% | 3.95% | +3.35 | 0.361 | -16.60% | 7 (2000-2003, 2005, 2016, 2022) | 12.6 |
| Japan (JPX) | Nikkei 225 | 7.09% | 3.31% | +3.78 | 0.453 | -24.90% | 4 (2000-2003) | 27.8 |
| Hong Kong (HKSE) | Hang Seng | 6.88% | 1.64% | +5.24 | 0.197 | -20.96% | 5 (2000-2004) | 22.4 |
| China, Shanghai only (SHH) | SSE Composite | 5.95% | 2.43% | +3.52 | 0.102 | -54.40% | 0 | 25.5 |
| Germany (XETRA) | DAX | 5.92% | 5.04% | +0.88 | 0.293 | -21.36% | 10 (2000-2005, 2008-2010, 2021) | 12.1 |
| UK (LSE) | FTSE 100 | 5.73% | 1.23% | +4.51 | 0.258 | -5.93% | 7 (2000-2004, 2009-2010) | 15.0 |
| China (SHZ + SHH) | SSE Composite | 5.28% | 2.43% | +2.85 | 0.079 | -54.38% | 0 | 26.0 |
| Korea (KSC) | KOSPI | 3.48% | 5.35% | -1.88 | 0.017 | -64.85% | 11 (2000-2007, 2019-2020, 2023) | 11.8 |
| Thailand (SET) | SET Index | 2.88% | 5.13% | -2.26 | 0.030 | -29.70% | 9 (2000-2007, 2009) | 21.9 |
| Indonesia (JKT) | S&P 500 (SPY, in USD) | 2.81% | 7.85% | -5.04 | 0.077 | -15.21% | 11 (2000-2010) | 17.3 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance. Avg stocks is the average number of lines held in invested years; two lines of one company count as two (see What the books hold).

- **US:** 10.62% a year against 7.85% for SPY, with a Sharpe of 0.631 against 0.352, a max drawdown of -20.64% against -38.01%, and no cash years. The screen passes 30 names every year; 13 to 28 of them have a usable entry price.
- **Germany:** the 0.88pp lead comes from the years in cash. Over its 15 invested years the portfolio returned 10.06% a year against 10.25% for the DAX, and in the 10 cash years the DAX averaged -2.32% a year (next table).
- **Korea:** the -64.85% drawdown runs from July 2016 to July 2023 (the 2016, 2017, 2018, 2021 and 2022 periods, with two cash years between). Its books hold 10 to 15 lines and often two lines of one company, ordinary and preferred.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Only SPY and the DAX among these benchmarks include them too. See METHODOLOGY.md (Benchmark Limitations).
- **Books at the floor:** India 2004, Canada 2004, 2006 and 2010-2012, Germany 2019 and 2022, the UK 2016 and Korea 2010 and 2024 were invested with exactly 10 buyable names. No invested year averages fewer than 10 lines.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Cash years

A year is in cash for one of two reasons, and each period record says which (`screened`, `entry_buyable`). The second column lists, in brackets, how many of the screened names had a usable entry price. The US and both China runs were invested in all 25 years.

| Market | Screen passed fewer than 10 | Fewer than 10 buyable at entry (buyable of screened) |
|--------|-----------------------------|------------------------------------------------------|
| India (NSE) | 2000-2003 | none |
| Canada (TSX) | 2000-2002, 2016 | 2003 (7 of 11), 2005 (9 of 13), 2022 (9 of 10) |
| Japan (JPX) | 2000-2003 | none |
| Hong Kong (HKSE) | 2000-2002 | 2003 (6 of 10), 2004 (9 of 14) |
| Germany (XETRA) | 2000-2005, 2009-2010 | 2008 (9 of 10), 2021 (9 of 11) |
| UK (LSE) | 2000-2002 | 2003 (7 of 10), 2004 (8 of 10), 2009 (9 of 11), 2010 (9 of 12) |
| Korea (KSC) | 2000-2007, 2019-2020, 2023 | none |
| Thailand (SET) | 2000-2005 | 2006 (7 of 10), 2007 (9 of 13), 2009 (8 of 11) |
| Indonesia (JKT) | 2000-2010 | none |
| Switzerland (SIX) | 2000-2004 | 2005 (7 of 11), 2006 (8 of 12), 2008 (9 of 14), 2009 (7 of 11), 2010 (8 of 12), 2011 (8 of 12), 2013 (8 of 12), 2014 (7 of 13), 2021 (9 of 14), 2022 (9 of 13) |
| Taiwan (TAI + TWO) | 2000-2016, 2018-2019 | 2017 (9 of 10) |
| Malaysia (KLS) | 2000-2006, 2009 | 2007 (9 of 11), 2008 (8 of 10), 2010 (6 of 10), 2011 (9 of 14), 2012 (9 of 14), 2013 (9 of 15) |
| Sweden (STO) | 2000-2020, 2023 | 2021 (9 of 10), 2022 (9 of 10) |
| South Africa (JNB) | 2000-2024 | none |

A cash year earns 0% and is counted in every 25-year figure above, against whatever the index did that year. The next table sets the 25-year excess beside the excess over the invested years only: the portfolio's returns in the years it held stocks, compounded and annualised over that many years, against the index over the same years. The second reading is not a return anyone earned. It shows how the selected stocks did when there was a book.

| Market | Cash years | Index in the cash years (% a year) | Excess, 25 years (pp) | Portfolio, invested years | Index, invested years | Excess, invested years (pp) |
|--------|------------|------------------------------------|-----------------------|---------------------------|-----------------------|-----------------------------|
| India (NSE) | 4 of 25 | +0.13 | +5.45 | 21.17% | 14.48% | +6.69 |
| Canada (TSX) | 7 of 25 | +1.46 | +3.35 | 10.29% | 4.94% | +5.35 |
| Japan (JPX) | 4 of 25 | -9.68 | +3.78 | 8.50% | 5.99% | +2.51 |
| Hong Kong (HKSE) | 5 of 25 | -2.54 | +5.24 | 8.67% | 2.71% | +5.96 |
| Germany (XETRA) | 10 of 25 | -2.32 | +0.88 | 10.06% | 10.25% | -0.19 |
| UK (LSE) | 7 of 25 | +1.87 | +4.51 | 8.05% | 0.98% | +7.08 |
| Korea (KSC) | 11 of 25 | +11.19 | -1.88 | 6.29% | 0.98% | +5.31 |
| Thailand (SET) | 9 of 25 | +14.09 | -2.26 | 4.53% | 0.41% | +4.12 |
| Indonesia (JKT) | 11 of 25 | +0.94 | -5.04 | 5.07% | 13.61% | -8.55 |

The sign changes in three markets. Korea goes from 1.88pp behind the KOSPI over 25 years to 5.31pp ahead over its 14 invested years, and Thailand from 2.26pp behind to 4.12pp ahead: both indices rose more than 11% a year in the years the portfolio sat in cash. Germany goes from 0.88pp ahead of the DAX to level with it (0.19pp behind, a tie at under 0.2pp). Indonesia's index is the S&P 500 in USD in both readings. All of these indices except the DAX and SPY are price indices, so the invested-years margins are overstated by the dividend yield as well.

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the screen. Its figures are listed so the files can be checked, not as results. In brackets: the number of names held that year.

| Market | Years in cash | Returns in the invested years | Avg stocks when invested | CAGR incl. cash years | Benchmark CAGR |
|--------|---------------|-------------------------------|--------------------------|-----------------------|----------------|
| Switzerland (SIX) | 15 of 25 | 2007: -10.93% (10), 2012: +25.07% (10), 2015: +15.90% (10), 2016: +21.83% (10), 2017: +4.51% (11), 2018: +22.07% (10), 2019: +8.94% (11), 2020: +32.64% (12), 2023: -3.87% (13), 2024: -6.24% (10) | 10.7 | 3.92% | 1.74% (SMI) |
| Malaysia (KLS) | 14 of 25 | 2014: -10.09% (12), 2015: +5.36% (12), 2016: -1.78% (10), 2017: +11.61% (11), 2018: -6.17% (15), 2019: +18.39% (10), 2020: -6.67% (15), 2021: -7.27% (17), 2022: +2.84% (21), 2023: +13.78% (19), 2024: -2.32% (18) | 14.5 | 0.53% | 7.85% (S&P 500 in USD) |
| Taiwan (TAI + TWO) | 20 of 25 | 2020: +23.00% (11), 2021: -10.70% (14), 2022: +13.31% (12), 2023: +7.80% (11), 2024: -10.45% (19) | 13.4 | 0.74% | 4.09% (TAIEX) |
| Sweden (STO) | 24 of 25 | 2024: +0.69% (12) | 12.0 | 0.03% | 2.55% (OMX Stockholm 30) |
| South Africa (JNB) | 25 of 25 | none | 0 | 0.00% | 7.85% (S&P 500 in USD) |

- **Switzerland:** fewer than 10 Switzerland-domiciled companies passed the screen in 2000-2004. In ten more years 11 to 14 passed but only 7 to 9 had a usable entry price. Six of its ten invested books sit exactly at the floor of 10, and every one of them holds Roche twice (RO.SW and ROG.SW).
- **Malaysia:** in 6 of its 14 cash years 10 to 15 names passed and only 6 to 9 had a usable entry price.
- **Taiwan, Sweden, South Africa:** fewer than 10 names pass the screen in almost every year. South Africa never reaches 10.

### What the books hold

`holdings` in each period record is the priced book. Counted over every held line of every invested year, by ticker pattern (not checked against the data's instrument type), several books hold lines that are not ordinary shares, or a second line of a company already held. The files don't record a single name's return, so the effect on CAGR isn't measured.

| Market | Lines that are not ordinary shares | Line-years | Share of the book | Years present |
|--------|------------------------------------|-----------:|------------------:|---------------|
| Thailand (SET) | NVDR lines (`-R.BK`) | 143 of 351 | 40.7% | all 16 invested years, up to 13 a year; 133 of the 143 sit beside the company's ordinary line |
| Korea (KSC) | Preferred lines (code not ending in 0: 051905, 002795, 090435 and others) | 43 of 165 | 26.1% | all 14 invested years, up to 5 a year; 35 beside the ordinary line |
| Germany (XETRA) | Preference shares (HEN3, SRT3, DRW3, BIO3) | 27 of 181 | 14.9% | all 15 invested years, up to 4 a year; every one beside the ordinary line |
| US | Exchange-traded notes and bonds of utilities (SOJC, SOJD, CMSC, CMSD, DTB, EAI and others), corporate units (DTP, SOLN), ADRs of Brazilian preferred shares (CIG, ELP, EBR-B) | 67 of 542 | 12.4% | 10 to 12 of the 24 to 28 lines held in each year from 2020 to 2024 |
| Switzerland (SIX) | Roche's non-voting certificate (ROG.SW) | 10 of 107 | 9.3% | all 10 invested years, beside RO.SW |
| Canada (TSX) | Preferred series (EMA-PC, EMA-PH, AQN-PA) | 17 of 226 | 7.5% | 10 of 18 invested years, up to 3 a year |

Smaller: the UK book holds a listed investment trust (TRIG.L) in 6 of 270 line-years; China (SHZ + SHH) holds the B-share line of two companies beside their A shares in 16 of 651; India holds an infrastructure trust unit once and Taiwan a depositary receipt once. Canada also holds two share classes of one company together (ACO-X and ACO-Y in 15 years, CSW-A and CSW-B in 9, CU and CU-X in 8). Japan, Hong Kong, China (SHH), Indonesia and Malaysia hold none of these by ticker.

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

# The old exit rule (doesn't reproduce old results: the listing rules, dedup and cash rule still apply)
python3 defensive-quality/backtest.py --exchange LSE --exit-method drop

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
- **The ranking key is a vendor ratio.** `dividendYield` is wrongly scaled for some symbols, and a top-30-by-yield cut gives such a symbol a slot. FEMSA's ADR (FMX) is in the US book in 24 of 25 years; a check before the rerun found its recorded yield at a median of 26.5% against a US cut-off of 3-5%.
- The US market is every line on NYSE, NASDAQ and AMEX, so it holds foreign companies' depositary receipts as well as US companies, and the exchange-traded notes listed under What the books hold.
- One line per ISIN isn't one line per company. Second share classes, preference shares and Thai NVDR lines have their own ISINs, so they are held beside the ordinary share and each counts as a holding. Avg stocks counts lines, not companies.
- The entry floor of 1.00 is in local units: one Hong Kong dollar, one penny on the LSE, one baht.
- `screen.py` (the current-day screen) predates the listing rules and the dedup. It screens today's active lines, which is right for a current screen. The backtest doesn't use it.

## Data Requirements

- `key_metrics` (FY): returnOnEquity, marketCap
- `financial_ratios` (FY): operatingProfitMargin, debtToEquityRatio, dividendYield
- `stock_eod`: adjClose and volume after each rebalance date, and the last traded price for exits
- `profile`: sector, exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup tie-break only)

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
