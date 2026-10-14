# P/E Mean Reversion (Sector-Relative)

Buys companies whose P/E is at least 40% below the median P/E of their sector on the same market, provided they are profitable and not heavily indebted. The idea is that a quality company priced far below its peers gets re-rated toward the sector's level.

## Academic Basis

Fama, E.F. & French, K.R. (1992), "The Cross-Section of Expected Stock Returns", *Journal of Finance* 47(2), 427-465. The value premium also holds within industries: a low valuation relative to sector peers predicts returns beyond the market-wide P/E.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| P/E | 3 to 50 | Profitable, not priced on hope |
| P/E relative to sector median | below 60% | At least 40% cheaper than peers |
| Return on equity | above 8% | Cheap for a reason is excluded |
| Debt/equity | 0 to below 2.0, or not reported | Not heavily leveraged |
| Market cap | above the market's floor in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency: $1B in the US, ₹20B on NSE, ¥100B on JPX, £500M on LSE, €500M on XETRA | Liquid, investable |

On each January rebalance date, for every company on the market's exchanges:

1. Take the company's latest fiscal-year `financial_ratios` row and latest fiscal-year `key_metrics` row whose fiscal year ended at least 45 days before the rebalance date and no more than 540 days before it. A company with no such row drops out for that year.
2. Keep it only if that row passes the P/E, return on equity, debt/equity and market cap tests.
3. Take the median P/E of the companies left in each sector. A sector with fewer than 5 of them has no median, and its companies drop out.
4. Keep the companies whose P/E is below 60% of their sector's median.
5. Keep one line per ISIN, so a security listed under two tickers counts once.
6. Rank by P/E divided by the sector median, lowest first, then symbol, and buy the top 30 at equal weight.

This differs from a P/E compression screen, which compares a stock's P/E with its own 5-year average. Here the baseline is the sector's current median, so no P/E history is needed.

## Parameters

- **Rebalancing:** Annual, on January 1, executed at the next available close. 25 periods, January 2000 to January 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a close of at least 1.00 in local units in the 10-day entry window). A cash year earns 0%. The floor is checked at entry only.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Return cap:** A single holding's yearly return above 200% is treated as a data error and left out of that year's average.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. The tier thresholds are in US dollars and market caps aren't converted.
- **Data lag:** 45 days after the fiscal year end (point-in-time). A filing more than 540 days old is not used.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once. Share classes have their own ISINs, so a company's ordinary and preference shares can both be held.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 3.5% UK, 0.5% Switzerland).

## Results (January 2000 to January 2025)

Eight of the 13 markets were invested in at least half of the 25 years. In those eight the strategy beat its own market's index in 3 (UK, US, Japan), tied it within 0.2pp a year in 2 (Hong Kong, Canada) and trailed it in 3 (India, Korea, China). Germany, Taiwan, Sweden, Switzerland and Thailand are too thin to test and are reported separately below.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 10.72% | 11.40% | -0.68 | 0.124 | -59.76% | 6 (2000-2005) | 23.6 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 9.00% | 7.64% | +1.36 | 0.319 | -44.71% | 0 | 22.7 |
| UK (LSE) | FTSE 100 | 6.06% | 0.86% | +5.20 | 0.123 | -50.88% | 5 (2000-2004) | 16.9 |
| Canada (TSX) | TSX Composite | 4.43% | 4.44% | -0.00 | 0.109 | -47.55% | 9 (2000-2006, 2011-2012) | 17.4 |
| Japan (JPX) | Nikkei 225 | 4.19% | 2.95% | +1.24 | 0.236 | -50.68% | 7 (2000-2005, 2011) | 21.3 |
| Korea (KSC) | KOSPI | 2.28% | 3.32% | -1.05 | -0.043 | -26.43% | 11 (2000-2008, 2010, 2015) | 19.1 |
| China (SHZ + SHH) | SSE Composite | 1.74% | 3.54% | -1.80 | -0.021 | -75.53% | 2 (2002, 2009) | 22.4 |
| Hong Kong (HKSE) | Hang Seng | 0.52% | 0.49% | +0.04 | -0.101 | -65.33% | 6 (2000-2005) | 22.5 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their January rebalance. Avg stocks is the average number of lines held in invested years; two share lines of one company count as two (see Known limits).

- **US:** 9.00% a year against 7.64% for SPY, with no cash years. The lead is in return only: the Sharpe ratio is 0.319 against 0.322 for SPY, and the max drawdown is -44.71% against -34.90%.
- **UK:** 6.06% a year against 0.86% for the FTSE 100, with 2000-2004 in cash. The FTSE 100 is a price index, so part of the 5.20pp lead is dividends the index leaves out. The max drawdown is -50.88% against -39.85% for the index.
- **Japan:** a 1.24pp lead over the Nikkei 225, also a price index. A lead of that size may not survive a total-return comparison.
- **Ties:** Hong Kong leads the Hang Seng by 0.04pp and Canada trails the TSX Composite by less than 0.01pp. Both indices are price indices, so with dividends counted in the index both markets would trail.
- **Trailing:** India has the highest CAGR of the eight, 10.72%, and still trails the Sensex (11.40%). Korea trails the KOSPI by 1.05pp and China trails the SSE Composite by 1.80pp. All three indices are price indices, so the real shortfalls are larger.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Of the eight markets only the US is measured against a series that does too. See METHODOLOGY.md (Benchmark Limitations).
- **Drawdowns:** China's -75.53% runs from the 2008 rebalance to the 2011 one: -61.78% in 2008, a cash year in 2009, +1.90% in 2010 and -37.18% in 2011. Hong Kong's -65.33% and India's -59.76% are their 2008 years. Each is deeper than its index's worst drawdown (-64.33%, -46.08% and -51.34%).
- **Cash years:** most are cash because fewer than 10 names passed the screen. Seven are cash because 10 or more passed but fewer than 10 had a usable entry price: UK 2003 and 2004 (7 of 10 and 8 of 11 priced), Canada 2006 and 2012 (6 of 11 and 8 of 14), China 2002 and 2009 (9 of 19 and 9 of 30) and Hong Kong 2005 (9 of 11). China's 2009 is one name short of the floor, and the SSE Composite returned +72.47% that year.
- **Books at the floor:** Japan 2008 and 2010, UK 2014, China 2003 and Hong Kong 2009 were invested with exactly 10 buyable names. Hong Kong 2009 returned +62.62% on 10 of 30 screened names. No invested year holds fewer than 10 lines.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the strategy. Its figures are listed so the files can be checked, not as results.

| Market | Years in cash | Years invested | Avg stocks when invested | CAGR incl. cash years |
|--------|---------------|----------------|--------------------------|-----------------------|
| Germany (XETRA) | 17 of 25 | 2012-2013, 2019-2024 | 14.8 | 3.58% |
| Taiwan (TAI + TWO) | 16 of 25 | 2011, 2016, 2018-2024 | 17.6 | 2.04% |
| Sweden (STO) | 16 of 25 | 2008, 2017-2024 | 12.8 | 1.60% |
| Switzerland (SIX) | 24 of 25 | 2013 | 11.0 | 0.97% |
| Thailand (SET) | 17 of 25 | 2012, 2017-2021, 2023-2024 | 16.5 | 0.96% |

- **Germany:** fewer than 10 Germany-domiciled companies passed the screen in 15 years (none at all in 2000-2003). In 2014 and 2017, 10 and 11 passed but only 8 and 9 had a usable entry price.
- **Switzerland:** 10 or more Switzerland-domiciled companies passed in 4 years, and in 3 of those (2009, 2012, 2024) fewer than 10 had a usable entry price. 2013 is the only invested year: 11 names, +27.35%. The file's CAGR, Sharpe and 0.00% max drawdown describe that one year.
- **Sweden:** fewer than 10 names passed in 15 years, and in 2016 exactly 10 passed but 8 had a usable entry price. Two of the nine invested years (2017, 2020) held exactly 10 lines, and the books hold A and B shares of one company side by side.
- **Taiwan:** fewer than 10 names passed in all 16 cash years.
- **Thailand:** fewer than 10 names passed in 13 years. In 4 more (2013, 2014, 2016, 2022) 11 to 14 passed but only 6 to 9 had a usable entry price. The invested books hold a company's ordinary line and its `-R` depositary line as two holdings.

## Rerun October 2026

These results were rerun with the campaign's three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias), and one change to this screen.

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 27 holdings in 9 markets exited this way, and none stayed unpriced; each results file lists them under its `ltp` key. On identical selection the exit rule moves CAGR by -0.19pp to +0.19pp.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **Latest filing first.** The screen used to pick each company's latest fiscal-year row whose P/E was already inside 3 to 50, with no age limit. A company whose latest P/E had left the band, or which had stopped filing, kept qualifying on its last in-band year. The screen now takes the latest row within 540 days first and tests the band on it.

The earlier files (August 2026) also predated the entry-price half of the cash rule: they decided cash on the screen count alone and averaged whatever names had prices. CAGR fell in 11 of the 13 markets. The larger moves and the verdict changes:

- **Germany** fell from 10.37% with no cash years to 3.58% with 17, and is now too thin to test. The earlier book was drawn from every line listed on XETRA. Restricted to Germany-domiciled companies, the screen passes 10 or more names in 10 of 25 years.
- **Switzerland** went from 3 cash years to 24 for the same reason and is now too thin to test.
- **Sweden** fell from 6.68% to 1.60% and went from 10 cash years to 16, so it is now too thin to test. No listing rule applies there. The six newly cash years are 2007, 2009, 2012 and 2014-2016; the earlier files reported +87.81% for 2009 and +30.75% for 2012.
- **UK** fell from 10.87% to 6.06%. 2000-2004 are now cash: 5, 7 and 8 pence-quoted names passed in 2000-2002, and in 2003 and 2004 only 7 of 10 and 8 of 11 had a usable entry price. The average book in invested years rose from 11.8 names to 16.9.
- **Thailand** fell from 4.10% to 0.96% and went from 13 cash years to 17. All four new cash years passed the screen count and fail the entry-price test; the earlier files reported +30.51% and +35.51% for two of them. It was too thin to test before as well.
- **China** fell from 3.85% to 1.74%, which turns a 0.31pp lead over the SSE Composite into a 1.80pp shortfall. 2002 and 2009 are now cash with 9 priced names each; the earlier files reported +100.79% for 2009. Its max drawdown went from -65.60% to -75.53%.
- **Canada** fell from 5.69% to 4.43%, from a 1.25pp lead over the TSX Composite to a tie. 2005, 2006, 2011 and 2012 are now cash; the earlier files reported +43.13% for 2005.
- **Hong Kong** fell from 1.43% to 0.52%, from a 0.94pp lead over the Hang Seng to a tie. Its cash years are the same six.
- **US** fell from 10.25% to 9.00%, and its lead over SPY from 2.61pp to 1.36pp. Its Sharpe went from 0.371 to 0.319 and its max drawdown from -40.29% to -44.71%.
- The other markets moved by -0.44pp to +0.89pp: Japan and Taiwan down, India and Korea up. Benchmark CAGRs are unchanged.

A replay of the screen in the data warehouse, made before the rerun, gives the same cash years as these files in all 13 markets. It attributes five of the 60 new cash years to the latest-filing change (China 2002 and 2009, Germany 2016, Sweden 2015 and 2016). Without that change, 17 of the other 55 pass the screen count and fail the entry-price test, 34 have fewer than 10 names passing on the three markets with a listing rule, and 4 have fewer than 10 passing on markets without one (Taiwan 2012; Sweden 2007, 2012 and 2014). Those 4 involve no listing rule, no dedup removal and no entry-price test, so they come from changes in the data or in other code since the earlier files. The dedup moves no year into cash on its own. The result files can't separate those causes: the only pair of runs on identical selection is the exit rule.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 pe-mean-revert/backtest.py --exchange XETRA --output pe-mean-revert/results/returns_XETRA.json
python3 pe-mean-revert/backtest.py --exchange NYSE,NASDAQ,AMEX --output pe-mean-revert/results/returns_NYSE_NASDAQ_AMEX.json

# After all 13: rebuild the comparison file the charts read, then check it against the per-market files
python3 scripts/rebuild_comparison.py pe-mean-revert
python3 scripts/rebuild_comparison.py pe-mean-revert --check

# Charts from results/exchange_comparison.json (written to charts/, which isn't committed)
python3 pe-mean-revert/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup, cash rule and latest-filing pick still apply)
python3 pe-mean-revert/backtest.py --exchange LSE --exit-method drop

# Current screen
python3 pe-mean-revert/screen.py
python3 pe-mean-revert/screen.py --preset india
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Taiwan | `TAI,TWO` | `returns_TAI_TWO.json` | |
| Thailand | `SET` | `returns_SET.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Korea | `KSC` | `returns_KSC.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |

Use `--exchange` as shown. `--preset canada` adds TSXV, so it doesn't reproduce the TSX file. `--global --output pe-mean-revert/results/exchange_comparison.json` runs the same 13 exchange sets in one loop and writes the same file names, but a market that raises is recorded as an error entry and its old file stays on disk, so run the `--check` above afterwards.

## Known limits

- The 45-day lag counts from the fiscal year end. Many companies publish annual results later than that, so a row can be used before it was public.
- "Latest row" means the latest fiscal-year row that carries a P/E, a return on equity and a market cap.
- The size floor compares the filing's market cap, in the company's reporting currency, with a floor set in the listing currency.
- The sector median is taken before the same-ISIN dedup, so a security listed under two tickers counts twice in its sector's median and toward the 5-company minimum.
- The data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets), so those years are measured on survivors. See METHODOLOGY.md (Limitations: Survivorship Bias).
- The screen has no fund filter. A fund whose ticker carries company fundamentals in the data can pass and be held. Each period record lists the first 10 names held. In the US file those include closed-end or mutual fund tickers in 8 of the 25 years (ADX, TY, DNP, RVT, CEF, GDV, USA, GAM, TGABX), 4 of the 10 recorded names in 2015. In the LSE file they include investment trusts in 11 of the 20 invested years (EDIN.L, WTAN.L, RCP.L, CLDN.L, JII.L, JFJ.L, PIN.L, FCSS.L, BRWM.L, 3IN.L). These were picked out by ticker, not by a fund flag in the data, and their effect on the results isn't measured.
- One line per ISIN isn't one line per company. Preference shares, second share classes and Thai `-R` depositary lines have their own ISINs, so they are held beside the ordinary share and each takes one of the 30 slots. Among the first 10 recorded names, two lines with the same ticker root appear in 13 of Canada's 16 invested years (WN.TO with WN-PA.TO, L.TO with L-PB.TO), 12 of Korea's 14 (097950.KS with 097955.KS), all 8 of Germany's (VOW.DE with VOW3.DE, BMW.DE with BMW3.DE), all 9 of Sweden's (NCC-A.ST with NCC-B.ST) and all 8 of Thailand's (PTL.BK with PTL-R.BK). The 10-name floor and Avg stocks count lines, not companies.
- Each period record lists the first 10 names only, and lines are grouped by ticker root, so the counts in the two points above are lower bounds.
- Entry prices are missing for many screened names in some years: 9 of 30 screened names had one in China's 2009 and 10 of 30 in Hong Kong's 2009. The reason hasn't been checked.
- `screen.py` (the current-day screen) uses trailing-twelve-month tables, takes the sector median per exchange, and applies neither the listing rules nor the dedup.

## Data Requirements

- `financial_ratios` (FY): priceToEarningsRatio, debtToEquityRatio
- `key_metrics` (FY): returnOnEquity, marketCap
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, sector, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen (see Known limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for each market, plus CAGR and max drawdown by market. Not committed |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
