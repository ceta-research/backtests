# Dividend Sustainability Score

A 5-component score (0-10) for how well a dividend is covered: payout, leverage, free cash flow, profitability and financial trend. The strategy buys the highest-scoring dividend payers in each market once a year.

## Academic Basis

- DeAngelo, DeAngelo & Skinner (1992), "Dividends and Losses", *Journal of Finance* 47(5). Companies that cut dividends showed deterioration on several measures beforehand, not one.
- Piotroski (2000), "Value Investing: The Use of Historical Financial Statement Information to Separate Winners from Losers", *Journal of Accounting Research* 38. Composite scores built from simple signals beat single metrics.
- Benartzi, Michaely & Thaler (1997), "Do Changes in Dividends Signal the Future or the Past?", *Journal of Finance* 52(3).

## Signal

Each component scores 0, 1 or 2. The total runs from 0 to 10.

| # | Component | Source | Score 2 | Score 1 | Score 0 |
|---|-----------|--------|---------|---------|---------|
| 1 | Payout ratio | `financial_ratios` | below 50% | 50-80% | above 80% |
| 2 | Debt/equity | `financial_ratios` | below 0.5 | 0.5-1.5 | above 1.5 |
| 3 | FCF coverage (free cash flow / dividends paid) | `cash_flow_statement` | above 2x | 1-2x | below 1x |
| 4 | Return on equity | `key_metrics` | above 15% | 8-15% | below 8% |
| 5 | Piotroski F-Score | computed from statements | 7 or more | 5-6 | below 5 |

On each rebalance date, for every company on the market's exchanges:

1. Take the latest fiscal-year row of each table whose fiscal year ended at least 45 days before the rebalance date.
2. Keep the company if it paid a dividend (payout ratio above 0, dividends paid in the cash flow statement), its dividend yield is above 2% and its market cap is above the market's floor in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`): $1B in the US, ₹20B on NSE, ¥100B on JPX, €500M on XETRA.
3. Score it. The F-Score is computed from the income statement, balance sheet and cash flow statement of that year and the year before (9 yes/no signals), not read from a snapshot table.
4. Keep scores of 7 or more, and one line per ISIN.
5. Rank by score, then dividend yield (highest first), then symbol, and buy the top 30 at equal weight.

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
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa and France have no local index configured and are measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 0.5% Switzerland, 3.5% UK).

## Results (July 2000 to July 2025)

Of the 13 markets invested in at least half of the 25 years, the strategy beat its own market's index in 12. Korea trailed the KOSPI by 0.46pp. South Africa and France are too thin to test and are reported separately below.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|
| India (NSE) | Sensex | 15.51% | 12.06% | +3.45 | 0.389 | -10.75% | 5 (2000-2004) | 24.2 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 10.76% | 7.85% | +2.91 | 0.468 | -32.75% | 0 | 27.2 |
| Sweden (STO) | OMX Stockholm 30 | 10.48% | 2.55% | +7.93 | 0.438 | -32.45% | 4 (2000-2003) | 22.4 |
| Germany (XETRA) | DAX | 8.68% | 5.04% | +3.64 | 0.345 | -33.32% | 4 (2000-2003) | 21.1 |
| UK (LSE) | FTSE 100 | 8.14% | 1.23% | +6.92 | 0.287 | -16.81% | 4 (2000-2003) | 23.2 |
| Canada (TSX + TSXV) | TSX Composite | 7.89% | 3.95% | +3.94 | 0.336 | -30.30% | 3 (2001-2003) | 23.2 |
| China (SHZ + SHH) | SSE Composite | 7.73% | 2.43% | +5.30 | 0.165 | -32.58% | 1 (2001) | 24.1 |
| Hong Kong (HKSE) | Hang Seng | 6.20% | 1.64% | +4.56 | 0.161 | -27.49% | 2 (2000-2001) | 23.5 |
| Australia (ASX) | ASX 200 | 6.09% | 3.89% | +2.20 | 0.173 | -31.70% | 6 (2000-2005) | 23.2 |
| Japan (JPX) | Nikkei 225 | 5.67% | 3.31% | +2.36 | 0.288 | -35.92% | 7 (2000-2006) | 28.7 |
| Switzerland (SIX) | SMI | 5.33% | 1.74% | +3.59 | 0.352 | -34.54% | 7 (2000-2006) | 17.5 |
| Korea (KSC) | KOSPI | 4.89% | 5.35% | -0.46 | 0.133 | -27.83% | 10 (2000-2008, 2014) | 22.5 |
| Taiwan (TAI + TWO) | TAIEX | 4.72% | 4.09% | +0.64 | 0.231 | -28.26% | 7 (2000-2006) | 28.5 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Cash years are labelled by their July rebalance. Avg stocks is the average number of lines held in invested years; two share lines of one company count as two (see Known limits).

- **US:** 10.76% a year against 7.85% for SPY, with a Sharpe of 0.468 against 0.352, a max drawdown of -32.75% against -38.01%, and no cash years.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). Two of the 13 markets are measured against a series that does too: the US against SPY and Germany against the DAX. The other eleven are measured against price indices, so their excess is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Thin lead:** Taiwan beats the TAIEX by 0.64pp. The TAIEX is a price index, so that lead may not survive a total-return comparison. Korea trails the KOSPI, also a price index, even with that advantage.
- **Cash years:** every cash year in these 13 markets is a 2014 or earlier rebalance, and all but Korea's 2014 fall in 2000-2008. Most are cash because fewer than 10 names passed the screen. Eight are cash because 10 or more passed but fewer than 10 had a usable entry price: Canada 2001-2003 (8, 5 and 7 priced names), China 2001 (7), Hong Kong 2001 (4), Australia 2003 (8) and Switzerland 2005-2006 (7 each). The US was invested in all 25 years.
- **Books at the floor:** Germany 2006, Sweden 2009 and Switzerland 2007 and 2008 were invested with exactly 10 buyable names. No invested year holds fewer than 10 lines. Sweden 2009's 10 lines are 8 companies: SSAB-A.ST with SSAB-B.ST, and ATCO-A.ST with ATCO-B.ST. In the other three books no two lines share a ticker root.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets, because the risk-free rate differs (6.5% in India, 0.1% in Japan).

### Too thin to test

A market invested in fewer than half of the 25 years isn't a test of the strategy. Its figures are listed so the files can be checked, not as results.

| Market | Years in cash | Years invested | Avg stocks when invested | CAGR incl. cash years |
|--------|---------------|----------------|--------------------------|-----------------------|
| South Africa (JNB) | 19 of 25 | 2016-2019, 2022-2023 | 11.5 | 0.26% |
| France (PAR) | 25 of 25 | none | 0 | 0.00% |

- **South Africa:** fewer than 10 names passed the screen in 14 years, and in 5 more (2011, 2013-2015, 2024) 11 to 16 passed but only 5 to 9 had a usable entry price. Two of the six invested years (2016, 2022) held exactly 10 names.
- **France:** no Paris-listed name passed the screen in any year. That's a gap in the fundamentals data for PAR, not a finding about French dividends.

## Rerun October 2026

These results were rerun with the campaign's three changes, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias). The screen itself wasn't changed for this topic.

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 23 holdings in 10 markets exited this way; each results file lists them under its `ltp` key. One Australian holding had no usable last price and is still left out of its year. On identical selection the exit rule moves CAGR by -0.09pp to +0.08pp.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.

The earlier files also predated the entry-price half of the cash rule: they decided cash on the screen count alone and averaged whatever names had prices. The moves of more than 1pp, and the one verdict change:

- **Germany** fell from 12.27% to 8.68%, and its lead over the DAX from 7.23pp to 3.64pp. Its Sharpe fell from 0.652 to 0.345. 2000-2003 are now cash: 5, 4, 3 and 7 Germany-domiciled companies passed the screen. A replay of the earlier screen, made before the rerun, found that most of its top-30 slots were foreign companies' lines listed on XETRA. The earlier results file records no holdings, so that can't be checked from the files.
- **Switzerland** fell from 8.72% to 5.33%. Cash years rose from 2 (2000-2001) to 7 (2000-2006): 1, 4 and 7 Switzerland-domiciled companies passed in 2002-2004, and in 2005 and 2006 exactly 10 passed but 7 had a usable entry price.
- **Canada** fell from 9.96% to 7.89%. 2001-2003 are now cash: 14, 12 and 14 names passed the screen but only 8, 5 and 7 had a usable entry price. The earlier files reported the average of those few names.
- **UK** fell from 9.38% to 8.14%. 2000-2003 are now cash (1, 3, 2 and 9 pence-quoted names passed), and the average book in invested years rose from 13.0 names to 23.2.
- **South Africa** went from 14 cash years to 19 for the same entry-price reason, and stays too thin to test.
- **Korea** moved from 5.47% to 4.89%, which turns a 0.12pp lead over the KOSPI (a tie at that size) into a 0.46pp shortfall. It's the one market whose verdict changed. Its cash years are the same ten.
- The other markets moved by -0.44pp to +0.83pp. Benchmark CAGRs are unchanged.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market, as in the table below.

The committed files were made one market at a time, each with `--preset` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 dividend-sustainability/backtest.py --preset germany --output dividend-sustainability/results/returns_XETRA.json

# After all 15: rebuild the comparison file the charts read, then check it against the per-market files
python3 scripts/rebuild_comparison.py dividend-sustainability
python3 scripts/rebuild_comparison.py dividend-sustainability --check

# Charts from results/exchange_comparison.json (the committed PNGs were drawn with the Agg backend)
MPLBACKEND=Agg python3 dividend-sustainability/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup and cash rule still apply)
python3 dividend-sustainability/backtest.py --preset uk --exit-method drop

# Current screen
python3 dividend-sustainability/screen.py
python3 dividend-sustainability/screen.py --preset india
```

| Market | `--preset` | Exchanges | Output file | Listing rule |
|--------|------------|-----------|-------------|--------------|
| US | `us` | NYSE, NASDAQ, AMEX | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `india` | NSE | `returns_NSE.json` | |
| Germany | `germany` | XETRA | `returns_XETRA.json` | Germany-domiciled only |
| Canada | `canada` | TSX, TSXV | `returns_TSX.json` | |
| Sweden | `sweden` | STO | `returns_STO.json` | |
| Switzerland | `switzerland` | SIX | `returns_SIX.json` | Switzerland-domiciled only |
| UK | `uk` | LSE | `returns_LSE.json` | GBp lines only |
| Japan | `japan` | JPX | `returns_JPX.json` | |
| Hong Kong | `hongkong` | HKSE | `returns_HKSE.json` | |
| China | `china` | SHZ, SHH | `returns_SHZ_SHH.json` | |
| Korea | `korea` | KSC | `returns_KSC.json` | |
| Taiwan | `taiwan` | TAI, TWO | `returns_TAI.json` | |
| Australia | `australia` | ASX | `returns_ASX.json` | |
| South Africa | `southafrica` | JNB | `returns_JNB.json` | |
| France | `france` | PAR | `returns_PAR.json` | |

`--global` isn't how these files were made. It runs Canada as TSX alone and Taiwan as TAI alone and writes them under the same two file names, and it writes next to whatever path `--output` gives, so `--output results/exchange_comparison.json` from the repository root lands outside this directory.

## Known limits

- The 45-day lag counts from the fiscal year end. Many companies publish annual results later than that, so a row can be used before it was public.
- The size floor compares the filing's market cap, in the company's reporting currency, with a floor set in the listing currency.
- The screen has no maximum filing age: a company that stopped filing keeps its last fiscal-year row.
- The screen has no fund filter. A fund whose ticker carries company fundamentals in the data can pass and be held. Each period record lists the first 10 names held, and in the US file those include mutual-fund-style tickers (five letters ending in X) in 4 of the 25 years: FTEMX in 2014, 2016 and 2018, TGABX in 2016 and 2018, FLCSX in 2017 and PRGTX in 2018. In 2018 they are 3 of the 10 recorded names. The LSE file records the listed investment funds PSH.L (2020), TFIF.L (2021), NESF.L (2022) and UKW.L (2023). These were picked out by ticker, not by a fund flag in the data. The backtest has no option to leave funds out, so their effect on the results isn't measured.
- One line per ISIN isn't one line per company. Preference shares and second share classes have their own ISINs, so they are held beside the ordinary share and each takes one of the 30 slots. Among the first 10 recorded names, two or more lines with the same ticker root appear in 15 of Canada's 22 invested years, 20 of Sweden's 21 (an A and a B share in 19 of them), 10 of Germany's 21 and 2 of the UK's 21. Canada 2016 held GWO.TO with GWO-PI.TO, GWO-PM.TO and GWO-PN.TO, and TCL-A.TO with TCL-B.TO. Canada 2021 held IFC.TO with IFC-PA.TO, IFC-PE.TO and IFC-PG.TO, and CF.TO with CF-PC.TO. Germany's pairs include FPE.DE with FPE3.DE and HEN.DE with HEN3.DE, and the UK's is BP.L with BP-B.L in 2005 and 2006. Germany 2011 also held BAS.DE with BASA.DE, which looks like a second line of the same company and hasn't been checked against a source. Avg stocks counts lines, not companies.
- Each period record lists the first 10 names only, and lines are grouped by ticker root, so the counts in the two points above are lower bounds.
- `screen.py` (the current-day screen) reads the F-Score from the `scores` snapshot table and applies neither the listing rules nor the dedup.

## Data Requirements

- `financial_ratios` (FY): dividendPayoutRatio, debtToEquityRatio, dividendYield
- `cash_flow_statement` (FY): freeCashFlow, commonDividendsPaid, operatingCashFlow
- `key_metrics` (FY): returnOnEquity, marketCap
- `income_statement` and `balance_sheet` (FY, two consecutive years): the F-Score inputs
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
| `charts/` | Output of `generate_charts.py`: growth and yearly returns for 12 markets, plus CAGR and max drawdown by market |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
