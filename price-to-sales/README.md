# Price-to-Sales (P/S) Value Screen

Companies trading below one times revenue that still earn a real margin and a real return on equity. Each quarter the screen buys the 30 lowest P/S names on a market, at equal weight.

Kenneth Fisher popularized the ratio in *Super Stocks* (1984). Barbee et al. (1996) and Gray & Vogel (2012) found that the P/S effect persists after controlling for size and book-to-market.

## Signal

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Price-to-sales | Above 0 and below 1.0 | Less than one unit of price per unit of revenue |
| Gross profit margin | Above 20% | Drops distributors and commodity traders, which are cheap on sales because they keep little of each sale |
| Operating margin | Above 5% | The gross margin survives operating costs |
| Return on equity | Above 10% | Quality filter |
| Market cap | Above the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`), in local currency ($1B in the US, ₹20B on NSE, ¥100B on JPX, €500M on XETRA) | Liquid, investable |

**Sorting:** lowest P/S first, ties broken on the symbol. Top 30.

**Which filing is tested:** each company's latest fiscal-year filing that is at least 45 days old and no more than 540 days old on the rebalance date. The latest filing has to pass every test. A company whose latest filing fails is out, even if an older filing passed, and a company with no filing inside the 540 days is out. P/S and the two margins come from `financial_ratios`; ROE and market cap come from `key_metrics`.

## Parameters

- **Rebalancing:** Quarterly, on the first day of January, April, July and October, executed at the next available close. 103 periods, January 2000 to July 2025, the last one exiting in October 2025.
- **Min stocks:** 10. The quarter is held in cash if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price. A cash quarter earns 0% and counts in the CAGR. The floor is checked at entry only.
- **Max stocks:** 30
- **Weighting:** Equal weight
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit. They are in every figure below. The tier thresholds are in US dollars and market caps aren't converted.
- **Data filters:** An entry price below 1.0 in the exchange's price unit is unusable (pence on LSE). A single-stock return above 200% in a quarter is dropped as a likely data error.
- **Exits:** A holding with no price after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa and Malaysia have no local index series and are measured against the S&P 500 in USD.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 9.0% South Africa).

## Results (January 2000 to October 2025)

Sixteen markets were run. Nine were invested in at least half of the 103 quarters and can be ranked. All nine finished ahead of their benchmark, but only two of those leads are against an index that includes dividends, and several rest on when the book happened to be in cash (see the notes under the table). The other seven markets were invested in fewer than half of the quarters. They are too thin to test, are listed separately, and are not results.

### Ranked markets

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max drawdown | Cash quarters | Avg share lines |
|--------|-----------|------|----------------|-------------|--------|--------------|---------------|-----------------|
| China (SHZ + SHH) | SSE Composite | 11.28% | 4.19% | +7.09 | 0.354 | -28.23% | 35 of 103 | 24.8 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 14.41% | 8.02% | +6.39 | 0.508 | -46.01% | 0 of 103 | 23.5 |
| UK (LSE, pence lines) | FTSE 100 | 7.68% | 1.36% | +6.32 | 0.216 | -54.19% | 6 of 103 | 18.1 |
| Hong Kong (HKSE) | Hang Seng | 6.33% | 1.77% | +4.56 | 0.127 | -63.47% | 28 of 103 | 22.0 |
| India (NSE) | Sensex | 14.27% | 11.12% | +3.15 | 0.266 | -58.01% | 30 of 103 | 24.4 |
| Japan (JPX) | Nikkei 225 | 6.08% | 3.40% | +2.68 | 0.299 | -61.82% | 18 of 103 | 27.0 |
| Korea (KSC) | KOSPI | 6.94% | 4.81% | +2.13 | 0.280 | -34.47% | 37 of 103 | 20.5 |
| Germany (XETRA, German companies) | DAX | 6.86% | 5.12% | +1.74 | 0.230 | -61.18% | 13 of 103 | 21.7 |
| Canada (TSX + TSXV) | TSX Composite | 5.94% | 5.08% | +0.85 | 0.193 | -53.15% | 10 of 103 | 17.7 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. "Avg share lines" is the average number of tickers held in invested quarters.

- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). The S&P 500 (through SPY) and the DAX include them too, so the US and Germany rows are like for like on that point. The other seven indices are price indices, so excess against them is overstated by roughly the index's dividend yield. Canada's +0.85 would not survive an allowance of one point a year, so it is not a clear lead. See [METHODOLOGY.md](../METHODOLOGY.md#benchmark-limitations) (Benchmark Limitations).
- **Cash timing:** a cash quarter earns 0% whatever the index does, so some leads come from which quarters were cash. Germany's 13 cash quarters run from January 2000 to January 2003, a stretch in which the DAX lost 61.64%; Japan's 18 run from January 2000 to April 2004, in which the Nikkei 225 lost 38.31%. China was in cash from April 2008 to January 2009 because only 2 names passed the screen, and its 2008 reads -27.93% against -64.33% for the SSE Composite. Korea was in cash in the first quarter of 2020, with 9 buyable names of 12 screened, while the KOSPI fell 20.70%. In India and Canada the cash quarters fell in rising markets and cost the book.
- **US book includes fund share classes:** mutual-fund share classes (five-letter tickers ending in X, such as PRGTX and JTSSX) are among the ten lowest-P/S holdings in 38 of the 103 quarters, from April 2013 to April 2025, and are 5 of those ten from April 2019 to January 2020. Their effect on the US result is not measured: `--exclude-funds` exists as a diagnostic flag and no run with it has been made. Until one is, the US figure can't be credited to the screen alone.
- **Share lines, not companies:** separate share classes and preference shares have their own ISINs, so the same-ISIN rule leaves them in, and each takes a slot. Among the ten lowest-P/S holdings recorded per quarter, two or more lines of one issuer appear in 92 of Canada's 93 invested quarters (for example QBR-A.TO with QBR-B.TO, and the preference line WN-PA.TO with WN.TO) and in 28 of the UK's 97 (AV-A.L and AV-B.L with AV.L). The average book sizes are counts of lines.
- **Drawdowns:** three books lost more than 60% from peak to trough: Hong Kong (-63.47%, January 2020 to January 2024), Japan (-61.82%, July 2007 to January 2009) and Germany (-61.18%, July 2007 to April 2009). The US and India maxima are not the 2008 fall. The US -46.01% runs from July 2018 to April 2020 and India's -58.01% from January 2018 to April 2020, with no cash quarter in either. Their falls through 2008 were smaller: -43.01% for the US (July 2007 to April 2009) and -53.17% for India (April 2006 to January 2009, with 8 of those 11 quarters in cash). The UK and Canada maxima run from July 2007 to April 2009 and Korea's from April 2008 to January 2009. China's -28.23% runs from October 2007 to April 2008 and is shallow because the book was in cash from 2000 to 2005 and for most of 2008.

### Too thin to test

These seven markets were invested in fewer than half of the 103 quarters. The CAGR column is shown only for completeness: it mostly measures the cash, and none of these rows is a result for the strategy.

| Market | Invested quarters | When invested | Avg share lines | CAGR, mostly cash |
|--------|-------------------|---------------|-----------------|-------------------|
| Sweden (STO) | 50 of 103 | 2005-04 to 2006-01 (4), 2008-04 to 2010-01 (8), 2011-04 to 2014-01 (12), 2016-04 to 2020-01 (16), 2023-04 to 2025-07 (10) | 12.5 | 3.53% |
| Taiwan (TAI + TWO) | 33 of 103 | 2010-04 to 2011-01 (4), 2012-07 to 2013-01 (3), 2017-04 to 2018-01 (4), 2019-04 to 2020-01 (4), 2021-04 to 2025-07 (18) | 16.5 | 1.74% |
| Switzerland (SIX, Swiss companies) | 17 of 103 | 2007-04 to 2008-01 (4), 2008-10 to 2010-01 (6), 2012-04 to 2013-01 (4), 2024-04 (1), 2025-04 to 2025-07 (2) | 10.1 | 0.79% |
| South Africa (JNB) | 13 of 103 | 2022-07 to 2025-07 (13) | 11.8 | 1.07% |
| Thailand (SET) | 2 of 103 | 2025-04 to 2025-07 (2) | 10.0 | 0.14% |
| Malaysia (KLS) | 0 of 103 | never | none | 0.00% |
| Singapore (SES) | 0 of 103 | never | none | 0.00% |

Quarters are named by their rebalance month; the number in brackets is the length of the run. Sweden's book is smaller than its line count: an A and a B line of one company (for example VOLV-A.ST with VOLV-B.ST) are held together in 46 of its 50 invested quarters, and in at least 16 of them the book held fewer than 10 companies. Thailand's two invested quarters hold 10 lines of 7 companies. Malaysia and Singapore never reached 10 buyable names. Singapore's file is in `results/`, but the market is left out of `exchange_comparison.json` and the two comparison charts, which hold the same 15 markets as the June comparison.

### Cash quarters

A quarter is in cash for one of two reasons: fewer than 10 names passed the screen, or 10 or more passed but fewer than 10 had a usable entry price. Each results file records both counts for every quarter (`screened`, `entry_buyable`).

| Market | Fewer than 10 passed the screen | 10 or more passed, fewer than 10 buyable |
|--------|--------------------------------|------------------------------------------|
| Malaysia (KLS) | 103: 2000-01 to 2025-07 (103) | 0 |
| Singapore (SES) | 95: 2000-01 to 2022-01 (89), 2024-04 to 2025-07 (6) | 8: 2022-04 to 2024-01 (8) |
| Thailand (SET) | 76: 2000-01 to 2008-01 (33), 2009-04 to 2010-01 (4), 2011-04 to 2016-01 (20), 2017-04 to 2019-10 (11), 2020-04 to 2021-01 (4), 2024-04 to 2025-01 (4) | 25: 2008-04 to 2009-01 (4), 2010-04 to 2011-01 (4), 2016-04 to 2017-01 (4), 2020-01 (1), 2021-04 to 2024-01 (12) |
| South Africa (JNB) | 88: 2000-01 to 2008-07 (35), 2009-01 to 2022-01 (53) | 2: 2008-10 (1), 2022-04 (1) |
| Switzerland (SIX, Swiss companies) | 61: 2000-01 to 2005-01 (21), 2013-04 to 2015-01 (8), 2016-04 to 2024-01 (32) | 25: 2005-04 to 2007-01 (8), 2008-04 to 2008-07 (2), 2010-04 to 2012-01 (8), 2015-04 to 2016-01 (4), 2024-07 to 2025-01 (3) |
| Taiwan (TAI + TWO) | 69: 2000-01 to 2010-01 (41), 2011-04 to 2012-01 (4), 2013-04 to 2017-01 (16), 2018-04 to 2019-01 (4), 2020-04 to 2021-01 (4) | 1: 2012-04 (1) |
| Sweden (STO) | 49: 2000-01 to 2005-01 (21), 2006-04 to 2008-01 (8), 2010-04 to 2011-01 (4), 2014-04 to 2016-01 (8), 2020-04 to 2022-01 (8) | 4: 2022-04 to 2023-01 (4) |
| Korea (KSC) | 33: 2000-01 to 2008-01 (33) | 4: 2019-04 to 2020-01 (4) |
| China (SHZ + SHH) | 25: 2000-01 to 2003-01 (13), 2004-04 to 2005-01 (4), 2008-04 to 2009-01 (4), 2010-04 to 2011-01 (4) | 10: 2003-04 to 2004-01 (4), 2005-04 to 2006-01 (4), 2011-04 to 2011-07 (2) |
| India (NSE) | 26: 2000-01 to 2005-04 (22), 2006-07 to 2007-04 (4) | 4: 2007-07 to 2008-04 (4) |
| Hong Kong (HKSE) | 21: 2000-01 to 2005-01 (21) | 7: 2006-04 to 2006-10 (3), 2007-04 (1), 2008-04 (1), 2008-10 to 2009-01 (2) |
| Japan (JPX) | 18: 2000-01 to 2004-04 (18) | 0 |
| Germany (XETRA, German companies) | 13: 2000-01 to 2003-01 (13) | 0 |
| Canada (TSX + TSXV) | 0 | 10: 2000-01 (1), 2004-04 to 2005-04 (5), 2006-04 to 2007-01 (4) |
| UK (LSE, pence lines) | 1: 2000-01 (1) | 5: 2001-04 to 2002-01 (4), 2007-04 (1) |

The US was invested in all 103 quarters.

## Rerun October 2026

These results replace the set published in June 2026. Six things changed. The first two below are specific to this screen's code. The listing rules, the dedup and the exit rule are described in [METHODOLOGY.md](../METHODOLOGY.md) (Universe Construction, and Limitations: Survivorship Bias). The sixth is a fix to the shared price filter in `data_utils.py`.

- **Latest filing first.** The June code kept only filings that passed the tests and then took each company's most recent one, however old, so a company stayed in the book on a filing from years earlier whose successor had failed. The screen now takes the latest fiscal-year filing no older than 540 days and tests that one. This is the largest change. Far fewer names pass, and it is why seven markets are now mostly cash.
- **Entry-side cash rule.** The June results went to cash only when fewer than 10 names passed the screen, and otherwise averaged however many of them had prices. A quarter is now also cash when fewer than 10 of the screened names have a usable entry price.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market, before the top-30 cut.
- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the quarter. 28 holdings in 8 markets exited this way, and none was left without an exit price; each results file lists them under its `ltp` key. On the same selection this moves CAGR by between -0.10 points (US) and +0.26 points (China).
- **Price-filter fix and price window.** The June run's price filter could delete valid prices. `remove_price_oscillations` compared each price row with the rows before and after it without checking that they were close in time. This backtest fetches prices only for the days after each rebalance date, so the row before the first price of a quarter was a quarter old. A rise of 30% to 100% or a fall of 23% to 50% from one window to the next could then look like a one-day spike, and for any stock or index with five or more such moves the filter deleted the first price of the new window. The run then used the next price in that window, and a stock with no other price there dropped out of the quarter. Since commit `b815bb7` (21 August 2026) the filter only compares rows that are within 10 days of each other. The price window fetched after each rebalance date also grew from 10 days to 11. No run isolates these two changes, so their effect on this topic is not measured. They can account for part of the difference in any invested quarter in any market: in its return, in how many names each run could price, and in benchmark prices, which pass through the same filter. The June figures quoted below carry the bug.

Selection is also deterministic now: ties at the top-30 cut break on the symbol (see [Reproducibility](../METHODOLOGY.md#reproducibility)).

What moved:

- **Sweden** fell from 10.96% to 3.53% and is now too thin to test: cash quarters rose from 13 to 53. Its June Sharpe of 0.452, the highest of any market then, no longer stands.
- **Thailand** (5.30% to 0.14%, cash 21 to 101 quarters), **South Africa** (6.24% to 1.07%, 25 to 90), **Switzerland** (2.88% to 0.79%, 4 to 86), **Taiwan** (3.45% to 1.74%, 37 to 70) and **Malaysia** (3.77% to 0.00%, 41 to 103) are also too thin now. None of them was in June.
- **China** rose from 6.00% to 11.28%. The June book was invested in every quarter; the new one is in cash for 35, including all of 2000 to 2005 and most of 2008.
- **UK** rose from 3.08% to 7.68%, and its average book grew from 9.1 to 18.1 lines. Which change fills the book is not determined. The listing rule (lines not quoted in pence no longer take one of the 30 slots), the latest-filing rule, the dedup and the price-filter fix (prices the June filter deleted are now kept) can each do it.
- **Korea** rose from 4.09% to 6.94%, from 0.72 points behind the KOSPI to 2.13 ahead. It is the only ranked market that changed side.
- **US** rose from 11.66% to 14.41% and **India** from 12.27% to 14.27%.
- **Hong Kong** fell from 9.52% to 6.33%, **Canada** from 8.42% to 5.94%, **Japan** from 7.71% to 6.08% and **Germany** from 8.13% to 6.86%. Canada and Germany had no cash quarters in June and have 10 and 13 now.
- **Singapore** was not in the June set. It is run here and never invests. Its file is committed, and it is left out of `exchange_comparison.json` and the comparison charts. Norway and Brazil were not rerun, and their result files, left from the March 2026 run, are removed.
- Benchmark CAGRs are unchanged. Two pairs of SSE Composite year figures read differently: 2006 (+123.66% to +129.96%) with 2007 (+99.63% to +94.16%), and 2014 (+58.88% to +58.84%) with 2015 (-1.65% to -1.62%). Each pair compounds to the same two-year return in both sets, to rounding, so the two runs used a different index price at the January boundary inside each pair. That fits the price-filter fix above: the June filter could delete the first index price after a rebalance, and the run then used the next one. It has not been checked against the price data, and the index data is not known to have changed.

In June, 11 of 15 markets were ahead of their benchmark. Of the 9 that can be ranked now, all 9 are ahead; the other 7 are too thin to say.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

The committed files were made one market at a time:

```bash
python3 price-to-sales/backtest.py --preset us --output price-to-sales/results/returns_NYSE_NASDAQ_AMEX.json
python3 price-to-sales/backtest.py --preset india --output price-to-sales/results/returns_NSE.json
python3 price-to-sales/backtest.py --preset sweden --output price-to-sales/results/returns_STO.json
python3 price-to-sales/backtest.py --preset korea --output price-to-sales/results/returns_KSC.json
python3 price-to-sales/backtest.py --preset hongkong --output price-to-sales/results/returns_HKSE.json
python3 price-to-sales/backtest.py --preset germany --output price-to-sales/results/returns_XETRA.json
python3 price-to-sales/backtest.py --preset taiwan --output price-to-sales/results/returns_TAI.json
python3 price-to-sales/backtest.py --preset canada --output price-to-sales/results/returns_TSX.json
python3 price-to-sales/backtest.py --preset thailand --output price-to-sales/results/returns_SET.json
python3 price-to-sales/backtest.py --preset china --output price-to-sales/results/returns_SHZ_SHH.json
python3 price-to-sales/backtest.py --preset uk --output price-to-sales/results/returns_LSE.json
python3 price-to-sales/backtest.py --preset japan --output price-to-sales/results/returns_JPX.json
python3 price-to-sales/backtest.py --preset southafrica --output price-to-sales/results/returns_JNB.json
python3 price-to-sales/backtest.py --preset switzerland --output price-to-sales/results/returns_SIX.json
python3 price-to-sales/backtest.py --exchange KLS --output price-to-sales/results/returns_KLS.json
python3 price-to-sales/backtest.py --exchange SES --output price-to-sales/results/returns_SES.json

# After any market is rerun, rebuild the combined file the charts read, then check it.
# --drop SES leaves Singapore out of the combined file (see Too thin to test).
python3 scripts/rebuild_comparison.py price-to-sales --drop SES
python3 scripts/rebuild_comparison.py price-to-sales --drop SES --check

# Charts from results/exchange_comparison.json
python3 price-to-sales/generate_charts.py
```

| Market | Arguments | Exchanges | Output file | Listing rule |
|--------|-----------|-----------|-------------|--------------|
| US | `--preset us` | NYSE, NASDAQ, AMEX | `returns_NYSE_NASDAQ_AMEX.json` |  |
| India | `--preset india` | NSE | `returns_NSE.json` |  |
| Sweden | `--preset sweden` | STO | `returns_STO.json` |  |
| Korea | `--preset korea` | KSC | `returns_KSC.json` |  |
| Hong Kong | `--preset hongkong` | HKSE | `returns_HKSE.json` |  |
| Germany | `--preset germany` | XETRA | `returns_XETRA.json` | Germany-domiciled only |
| Taiwan | `--preset taiwan` | TAI, TWO | `returns_TAI.json` |  |
| Canada | `--preset canada` | TSX, TSXV | `returns_TSX.json` |  |
| Thailand | `--preset thailand` | SET | `returns_SET.json` |  |
| China | `--preset china` | SHZ, SHH | `returns_SHZ_SHH.json` |  |
| UK | `--preset uk` | LSE | `returns_LSE.json` | GBp lines only |
| Japan | `--preset japan` | JPX | `returns_JPX.json` |  |
| South Africa | `--preset southafrica` | JNB | `returns_JNB.json` |  |
| Switzerland | `--preset switzerland` | SIX | `returns_SIX.json` | Switzerland-domiciled only |
| Malaysia | `--exchange KLS` | KLS | `returns_KLS.json` |  |
| Singapore | `--exchange SES` | SES | `returns_SES.json` |  |

`--global` doesn't reproduce this set. It runs Canada as TSX alone and Taiwan as TAI alone and writes them to `returns_TSX.json` and `returns_TAI.json`, the files that hold the TSX + TSXV and TAI + TWO books. It also runs Norway and Brazil, which are not part of this set, and if one market fails it leaves that market's old file in place and records an error in `exchange_comparison.json`. Each run reads the live data warehouse, so a rerun can differ slightly from the committed results.

Other commands:

```bash
# US, printed to the terminal (default universe: NYSE + NASDAQ + AMEX)
python3 price-to-sales/backtest.py --verbose

# Diagnostic: the same run without funds and ETFs. No committed result uses it.
python3 price-to-sales/backtest.py --preset us --exclude-funds

# The old exit rule (doesn't reproduce the June results: the other five changes still apply)
python3 price-to-sales/backtest.py --preset uk --exit-method drop

# Current screen, on trailing-twelve-month data
python3 price-to-sales/screen.py
python3 price-to-sales/screen.py --preset india
```

## Academic References

- Fisher, K. (1984). *Super Stocks.* Dow Jones-Irwin.
- Barbee, W., Mukherji, S. & Raines, G. (1996). "Do Sales-Price and Debt-Equity Explain Stock Returns Better than Book-Market and Firm Size?" *Financial Analysts Journal*, 52(2), 56-60.
- Gray, W. & Vogel, J. (2012). "Analyzing Valuation Measures: A Performance Horse-Race over the Past 40 Years." *Journal of Portfolio Management*, 39(1), 112-121.
- Novy-Marx, R. (2013). "The Other Side of Value: The Gross Profitability Premium." *Journal of Financial Economics*, 108(1), 1-28.

## Data Requirements

- `financial_ratios` (FY): priceToSalesRatio, grossProfitMargin, operatingProfitMargin
- `key_metrics` (FY): returnOnEquity, marketCap
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup)
- `screen.py` reads `financial_ratios_ttm` and `key_metrics_ttm`

## Known Limits

- The filing date used is the fiscal period end, so the 45-day lag counts from the period end. A fiscal year that ended 45 to 90 days before a rebalance can be used before the annual report was public.
- Funds are not excluded. The US book holds mutual-fund share classes in 38 of 103 quarters and the effect is unmeasured (see Results).
- Share classes and preference shares of one issuer are separate ISINs and are all held. Whether a preference line belongs in a price-to-sales book is an open question; the results above include them.
- The size floor compares the filing's market cap, in the company's reporting currency, with a threshold set in the listing currency.
- The cost tiers are in US dollars and are applied to local-currency market caps.
- `screen.py` lists every line on the exchange, without the listing rules or dedup, and tests trailing-twelve-month figures, not the latest fiscal-year filing.

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen, on trailing-twelve-month data |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market (16) and `exchange_comparison.json` (15, without Singapore) |
| `charts/` | Output of `generate_charts.py` |
