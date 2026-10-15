# Pairs Trading: Fundamentals Backtest

Correlation-based pairs trading on 15 markets, 2005 to 2024. Each year the backtest takes the same-sector pairs of stocks whose daily returns moved together most closely over the previous calendar year, and trades the ones whose price spread ended that year stretched: short the leg that ran ahead, long the one that lagged, in equal amounts, held for one calendar year. The book is market-neutral by construction: its beta to the local index runs from -0.033 to 0.290 in the 11 markets in the results table.

## Academic Basis

- Gatev, Goetzmann & Rouwenhorst (2006), "Pairs Trading: Performance of a Relative-Value Arbitrage Rule", *Review of Financial Studies* 19(3), 797-827. On US stocks from 1962 to 2002 a distance-based pairs rule earned about 11% a year in excess returns before costs. This backtest uses correlation, an annual z-score check and a one-year hold instead of their daily rule.
- Do & Faff (2010), "Does Simple Pairs Trading Still Work?", *Financial Analysts Journal* 66(4), 83-95. The same rule's profits fell after 2002.
- Do & Faff (2012), "Are Pairs Trading Profits Robust to Trading Costs?", *Journal of Financial Research* 35(2), 261-287.

## Signal

Trading year Y runs from 2005 to 2024; its formation year is Y-1.

| Step | Rule |
|------|------|
| Candidates | The year's universe: the top 30 lines of each sector (see below) |
| Pairs | Two candidates of the same sector whose daily returns correlated at 0.70 or more over the formation year, on at least 200 common trading days. A pair without a finite correlation is out. The 20 most correlated pairs are formed |
| Hedge ratio | OLS beta of log prices over the formation year |
| Signal | z = the spread log(A) - beta x log(B) on the pair's last common bar of the formation year, against that year's mean and standard deviation of the spread |
| Entry | \|z\| of 1.5 or more, and a close of at least 1.00 (local units) on both legs at entry: the first close in the 10 days after January 1 (next-day close, MOC) |
| Position | z above 0: short A, long B. z below 0: long A, short B. Equal amounts on each side |
| Pair return | -sign(z) x (return of A - return of B) / 2 over the year, to the first close in the 15 days after December 31. Each leg's gain is capped at 200% |
| Portfolio | Equal weight across the pairs entered |

### The candidate universe, rebuilt each year

For each trading year, a line on the market's exchanges is a candidate when:

1. It has a profile sector, isn't flagged as an ETF or a fund, and has at least one fiscal-year `key_metrics` row with a market cap. This operating-company test keeps out notes, preferred lines, warrants and when-issued lines. The filing can be dated at any time (see Known limits).
2. It was listed through the formation year and traded: at least 200 bars in the formation year, the last one within the year's final 10 days, and a positive median daily close x volume.
3. It passes the market's listing rule: XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only.
4. It is the one line kept for its ISIN, and then the one line kept for its company. Lines are one company when they share an ISIN, a cik, a normalised company name (unless both carry a cik) or a ticker root on the same exchange suffix, so share classes, preferred lines and Thai NVDR twins collapse to one line. The line kept is the one buyable at entry, then the shorter ticker, then the larger traded value.
5. It ranks in the top 30 of its sector by the formation year's median daily traded value (close x volume), converted to US dollars at the formation year-end rate.

So the list for 2010 is drawn from what was listed and traded in 2009, ranked by how much traded in 2009. No name needs to be among today's largest, and a name that has since stopped trading stays in the years it was listed. That removes the size look-ahead. It reduces survivorship bias but doesn't fix it, because the price data itself holds almost no company that stopped trading before 2015 in the US or before 2020 elsewhere (see Known limits).

The US list holds 330 candidates every year (30 in each of 11 sectors). The smallest lists are South Africa (63 to 142 a year), Switzerland (95 to 147) and Singapore (104 to 186). Each results file records, for every year, the candidates and sectors, the lines each step removed, the pairs formed and entered, and the 20 formed pairs with their correlations.

## Parameters

- **Rebalancing:** Annual. 20 trading years, 2005 to 2024.
- **Cash:** A year is in cash when fewer than 3 pairs are entered. The count is taken at entry, so what happens to a pair later in the year doesn't change it. A cash year earns 0%.
- **Pairs:** 20 formed a year at most, so at most 20 entered. In the 11 markets of the results table invested years average 4.8 (Taiwan) to 6.7 (US, China) pairs. Each pair is two lines, one long and one short.
- **Return cap:** A leg's gain above 200% is capped at 200% and counted (`legs_capped`): 9 legs in 5 markets over the 20 years.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B. Each leg holds half the pair's capital, so a pair's round trip costs the two legs' one-way rates added together, on the pair's capital. The market cap is the latest fiscal-year one whose fiscal year ended 45 to 540 days before the trading year, in US dollars at the formation year-end rate. A line without one pays 0.5%.
- **Exits:** A pair whose leg has no close in the exit window closes at the last bar both legs traded (`--exit-method ltp`, the default). 4 pairs exited this way: BBL/BHP and BBL/RIO in the US in 2022, 000002.SZ/600048.SS in China in 2015, CPG.TO/WCP.TO in Canada in 2024. Each results file lists them under its `ltp` key. Three of the four follow a delisting or a ticker change. The China one follows a trading suspension: 000002.SZ (China Vanke) has no delisting date, stops at 18 December 2015 and forms pairs again in 2021, 2022 and 2024. The pair closes at Vanke's last price before the suspension, 69% above its entry price, a price the position couldn't have been sold at until trading resumed. Without this pair China's 2015 return is +0.76% instead of +6.59%, and its CAGR -0.16% instead of 0.12%.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa has no local index in the data and is measured against SPY in US dollars.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 0.1% Japan, 3.5% UK, 6.5% India, 9.0% South Africa).

## Results (2005 to 2024)

No market beat its own index. Of the 11 markets invested in at least half of the 20 years, seven earned a positive CAGR, from 0.12% (China) to 2.21% (UK), and four lost money, down to -1.62% a year (Korea). China's 0.12% rests on one pair closed at a suspended share's last price (see Exits); without it China is at -0.16%. The UK came closest to its index, 0.50 points a year behind the FTSE 100. Japan has the only positive Sharpe ratio of the 11 (0.307, at Japan's 0.1% risk-free rate). South Africa, Thailand, Switzerland and Singapore are too thin to test and are reported separately below.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Volatility | Cash years | Avg pairs |
|--------|-----------|------|----------------|-------------|--------|--------|------------|------------|-----------|
| UK (LSE) | FTSE 100 | 2.21% | 2.70% | -0.50 | -0.189 | -10.58% | 6.84% | 2 (2006, 2018) | 6.5 |
| Sweden (STO) | OMX Stockholm 30 | 1.87% | 6.24% | -4.37 | -0.027 | -7.98% | 4.81% | 7 (2006, 2008, 2014, 2019-2021, 2024) | 6.2 |
| Germany (XETRA) | DAX | 1.41% | 8.01% | -6.59 | -0.103 | -10.32% | 5.69% | 9 (2005-2008, 2010, 2014, 2018, 2022, 2024) | 5.7 |
| Japan (JPX) | Nikkei 225 | 1.09% | 6.33% | -5.24 | 0.307 | -7.70% | 3.23% | 2 (2009-2010) | 5.7 |
| Taiwan (TAI + TWO) | TAIEX | 0.65% | 6.76% | -6.11 | -0.069 | -8.41% | 5.01% | 8 (2005, 2007, 2010, 2014-2016, 2018, 2020) | 4.8 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 0.14% | 10.28% | -10.14 | -0.467 | -14.94% | 3.97% | 1 (2010) | 6.7 |
| China (SHZ + SHH) | SSE Composite | 0.12% | 4.94% | -4.82 | -0.382 | -15.97% | 6.23% | 5 (2005-2007, 2009, 2017) | 6.7 |
| Canada (TSX) | TSX Composite | -0.38% | 5.14% | -5.51 | -0.425 | -15.64% | 6.77% | 4 (2005, 2018, 2022-2023) | 5.6 |
| Hong Kong (HKSE) | Hang Seng | -0.69% | 1.65% | -2.34 | -0.724 | -23.98% | 5.10% | 5 (2006-2007, 2009, 2015, 2022) | 5.6 |
| India (NSE) | Sensex | -1.18% | 13.11% | -14.29 | -1.135 | -30.11% | 6.77% | 3 (2006, 2018, 2024) | 5.9 |
| Korea (KSC) | KOSPI | -1.62% | 5.02% | -6.65 | -0.614 | -32.15% | 7.53% | 5 (2009-2010, 2014-2015, 2021) | 5.5 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Avg pairs is the average number of pairs entered in invested years.

- **US:** 0.14% a year against 10.28% for SPY, with a Sharpe of -0.467 against 0.490 and a max drawdown of -14.94% against -34.31%. It beat SPY in 3 of the 4 years SPY fell: 2008 (-10.38% against -34.31%), 2018 and 2022. In 2015 SPY fell 0.12% and the book fell 1.42%.
- **Market-neutral:** volatility runs from 3.23% a year (Japan) to 7.53% (Korea). The indices' own worst drawdowns over the period run from -28.91% (FTSE 100) to -64.33% (SSE Composite).
- **Dividends in benchmarks:** returns use dividend-adjusted prices on both legs. SPY and the DAX include dividends; the other local indices are price indices. A pair book is long and short in equal amounts, so dividends roughly cancel inside it. Against a total-return version of each price index the gap would be wider by about that index's dividend yield.
- **Sharpe:** compare it with the index's Sharpe in the same market, not across markets: the risk-free rate is 0.1% in Japan and 6.5% in India.
- **Short selling:** China and India restrict it. The results there assume both legs could be traded. Borrow costs aren't modelled in any market.

### Too thin to test

A market invested in fewer than half of the 20 years isn't a test of the strategy. Its figures are listed so the files can be checked, not as results.

| Market | Years in cash | Returns in the invested years | Avg pairs when invested | CAGR incl. cash years | Benchmark CAGR |
|--------|---------------|-------------------------------|-------------------------|-----------------------|----------------|
| South Africa (JNB) | 11 of 20 | 2005: +12.73% (5 pairs), 2012: -7.95% (5 pairs), 2016: -6.28% (4 pairs), 2017: +5.21% (5 pairs), 2018: +0.26% (6 pairs), 2021: +2.63% (5 pairs), 2022: -3.68% (3 pairs), 2023: +11.91% (3 pairs), 2024: +13.52% (4 pairs) | 4.4 | 1.27% | S&P 500 (SPY, USD) 10.28% |
| Thailand (SET) | 12 of 20 | 2005: +5.38% (3 pairs), 2007: +12.99% (4 pairs), 2012: -8.01% (3 pairs), 2019: -19.53% (3 pairs), 2020: +12.09% (4 pairs), 2021: -19.29% (3 pairs), 2023: +3.84% (6 pairs), 2024: -5.34% (4 pairs) | 3.8 | -1.21% | SET Index 3.56% |
| Switzerland (SIX) | 14 of 20 | 2009: +29.35% (4 pairs), 2012: +7.67% (10 pairs), 2016: +0.56% (11 pairs), 2017: -3.46% (3 pairs), 2019: -0.12% (12 pairs), 2022: +5.64% (5 pairs) | 7.5 | 1.79% | SMI 3.57% |
| Singapore (SES) | 16 of 20 | 2018: -9.14% (3 pairs), 2020: -2.16% (4 pairs), 2021: +4.63% (4 pairs), 2024: -2.29% (3 pairs) | 3.5 | -0.48% | STI 3.26% |

- **South Africa:** 63 to 142 candidates a year in 9 or 10 sectors. In 2006-2011 and 2013-2015 only 0 to 3 pairs were formed a year. It is one of the 12 markets in `exchange_comparison.json`. The CAGR chart doesn't draw it and names it in its footer as too thin to test; the cash chart draws its 55% and labels it too thin to test.
- **Switzerland:** 95 to 147 Switzerland-domiciled candidates a year; 4 or fewer pairs formed in 8 of the 20 years.
- **Thailand and Singapore:** fewer than 3 pairs entered in 12 and 16 years. Singapore formed 20 pairs in 11 of the 13 years from 2012, but in most of them fewer than 3 passed the entry test (\|z\| of 1.5 or more and both legs at 1.00 or more; many Singapore shares trade below one Singapore dollar). The files don't record which of the two tests a pair failed.

Switzerland, Thailand and Singapore are run and their files committed, but they aren't in `exchange_comparison.json` or the charts.

## Rerun October 2026

These results were rerun with the campaign's changes, described in METHODOLOGY.md, and with a rebuilt universe and five fixes in this topic:

- **Per-year universe.** The old backtest built one list from today's actively trading names, the top 30 of each sector by today's market cap, and used it for every year. That carried size look-ahead and left out every name that has since stopped trading. The list is now rebuilt each year from the formation year's listings and traded value, as described above.
- **One line per company.** A pair can no longer join two lines of one company (share classes, preferred lines, NVDR twins).
- **Fixes:** a pair without a finite correlation (a leg with a frozen price) used to rank first and take a slot it couldn't trade; costs were charged twice for each round trip and are now charged once; the cost tier came from today's market cap in the reporting currency and now comes from a point-in-time cap in US dollars; the z-score used the entry bar's prices and now uses the formation year's last bar, with entry on the next bar; a pair with a leg up more than 200% used to be dropped and that leg's gain is now capped at 200%. The cash decision used to count pairs priced at year end and now counts pairs entered.
- **Last-common-bar exits, listing rules and same-ISIN dedup,** as listed under Parameters and above.
- **Benchmarks.** Sweden is now measured against the OMX Stockholm 30 (before: the S&P 500). Switzerland, Thailand and Singapore, whose earlier files were measured against the S&P 500, now use the SMI, the SET Index and the STI.

On identical selection the exit rule alone moves CAGR by -0.10 points in the US, +0.28 in China (one pair marked at a suspended share's last price, see Exits) and -0.03 in Canada, and by 0.00 in the other 12 markets. The old files record no pairs, so the rest of each move can't be split between the new universe and the fixes. What moved:

- **Cash years** fell in 10 of the 12 markets in the comparison file: the UK from 13 to 2, Germany from 14 to 9, Taiwan from 13 to 8, the US from 6 to 1 and India from 8 to 3. The UK, Taiwan and Germany were invested in fewer than half of the years before and pass that test now. Japan rose from 1 to 2 and South Africa from 5 to 11.
- **South Africa** went from -0.47% to 1.27% and became too thin to test. Its old 2011 (-30.32%) and 2015 (-25.29%) years are now cash, so its old -46.91% max drawdown is gone.
- **Thailand** fell from 2.07% to -1.21%, too thin before and after. The old +44.29% of 2009 is now a cash year.
- **Taiwan** rose from -2.03% to 0.65%. Its old 2006 return was -27.78%; it is now -0.37%.
- **Germany** rose from -0.83% to 1.41%. It is invested in 2009, 2012-2013, 2015-2017 and 2019-2020, years the old run spent in cash.
- **Sweden** rose from -0.25% to 1.87%. With the change of benchmark its excess went from -10.53 to -4.37 points.
- **India** fell from 0.77% to -1.18% (the old comparison figure; the old `returns_BSE_NSE.json`, a BSE and NSE run against the S&P 500, read 0.72%). Its max drawdown went from -22.40% to -30.11%.
- **US** rose from -0.50% to 0.14%. 2008 went from -2.50% to -10.38%, now its worst year.
- **Japan** rose from 0.61% to 1.09%.
- Hong Kong (0.09% to -0.69%), Korea (-0.90% to -1.62%), China (-0.25% to 0.12%; -0.16% without the 2015 fill under Exits) and Canada (0.19% to -0.38%) moved by less than 1 point. Switzerland (2.90% to 1.79%) and Singapore (-0.74% to -0.48%) were too thin before and still are.
- Benchmark CAGRs are unchanged in the 11 published markets whose index didn't change.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 pairs-fundamentals/backtest.py --exchange NYSE,NASDAQ,AMEX --output pairs-fundamentals/results/returns_NYSE_NASDAQ_AMEX.json
python3 pairs-fundamentals/backtest.py --exchange XETRA --output pairs-fundamentals/results/returns_XETRA.json

# After all 15: rebuild the comparison file the charts read (12 markets), check it, check the pairs files
python3 scripts/rebuild_comparison.py pairs-fundamentals --keep NYSE_NASDAQ_AMEX JPX LSE HKSE KSC SHZ_SHH TAI_TWO TSX STO XETRA JNB NSE
python3 scripts/rebuild_comparison.py pairs-fundamentals --keep NYSE_NASDAQ_AMEX JPX LSE HKSE KSC SHZ_SHH TAI_TWO TSX STO XETRA JNB NSE --check
python3 scripts/check_pairs_results.py pairs-fundamentals

# Charts from results/exchange_comparison.json, written to pairs-fundamentals/charts/
python3 pairs-fundamentals/generate_charts.py

# The old exit rule, or both books in one run (doesn't reproduce old results: the new universe and fixes still apply)
python3 pairs-fundamentals/backtest.py --exchange LSE --exit-method drop
python3 pairs-fundamentals/backtest.py --exchange LSE --exit-method both

# Current pairs screen (see Known limits)
python3 pairs-fundamentals/screen.py
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Taiwan | `TAI,TWO` | `returns_TAI_TWO.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Canada | `TSX` | `returns_TSX.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| Switzerland | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Thailand | `SET` | `returns_SET.json` | |
| South Africa | `JNB` | `returns_JNB.json` | |
| Singapore | `SES` | `returns_SES.json` | |

Use `--exchange` as shown: the exchange list sets the universe name written into the file, and a `--preset` writes a different one. `--global` loops over the same 15 exchange sets and writes the same file names next to its `--output` path, but it also writes all 15 markets into that `--output` file, so run the rebuild above afterwards. A market that fails inside the loop is recorded as an error entry and its old file stays on disk; the `--check` and `check_pairs_results.py` steps catch both.

## Known limits

- **Survivorship bias is reduced, not fixed.** The price data holds almost no company that stopped trading before 2015 in the US or before 2020 elsewhere, so in the earlier years the universe is drawn from companies that survived at least that long. A company that stopped trading after that is a candidate in the years it was listed, and a pair whose leg stops trading closes at its last common bar.
- **The operating-company test isn't point-in-time.** A line needs a fiscal-year filing with a market cap, dated at any time, so a line whose first filing comes years after a formation year is still a candidate in it. A point-in-time test would empty the early Korean and Taiwanese years.
- **Sector is today's.** The profile sector is the one fetched now, not the one at the time.
- **One company under two tickers.** The company collapse doesn't join an old and a new ticker that each carry the same price history, or a dual listing. Formed pairs with a correlation of 0.995 or more: Canada 51 of 374 (CPG.TO/VRN.TO in all 20 years, ATRL.TO/SNC.TO in 19, MATR.TO/SCL.TO in 11, AOI.TO/MER.TO in 1), Sweden 25 of 320 (COPP-B.ST/VISC.ST, AOI.ST/MER.ST, ARCANE.ST/K33.ST), Japan 24 of 400 (6501.T/9086.T in 16 years, 5844.T/8369.T, 9347.T/9728.T), the US 13 of 400 (IAC/MTCH, DINO/HFC), the UK 11 of 389 (SKG.L/SWR.L) and Singapore 1 of 295. 6501.T and 9086.T are two companies with matching price series in the data. Such a pair earns about nothing but its cost. The files name the formed pairs and count the entered ones, so whether these were entered isn't recorded, except BBL/BHP (BHP's two US listings), entered in 2022 and closed at its last common bar.
- **Which pairs were held:** each year's record names the 20 formed pairs and counts the ones entered. Only the pairs that closed at their last common bar are named as entered.
- **Lines that aren't ordinary shares.** Germany's formed pairs hold Porsche SE's preferred shares (PAH3.DE, Porsche SE's only listed line) in 23 of 460 leg slots, and VVV3.DE in 1. Thailand's hold NVDR lines (BBL-R.BK 11, AP-R.BK 2, MINT-R.BK 2) in 15 of 354 slots: the NVDR line is the company's line in the candidate list, and the files don't record which step put it there. The US universe holds ADRs as well as US companies.
- **Taiwan** is run as `TAI,TWO`, but none of its 296 formed pairs holds a TWO line.
- **Missing cost caps.** A candidate without a fiscal-year market cap whose year ended 45 to 540 days before the trading year pays the 0.5% tier. In Korea 2005-2008 that is 194 to 234 of 243 to 245 candidates a year, in Taiwan 2005-2007 183 to 219 of 225 to 229, in India 2005 177 of 259 and in Japan 2005 174 of 311.
- **Annual signal.** One z-score check a year misses most of the year's divergences. A daily rule would trade differently and pay more in costs.
- **Short selling and borrow costs** aren't modelled; China and India restrict short selling.
- `screen.py` (the current-day screen) ranks today's actively trading names with a latest fiscal-year market cap above a fixed floor. It doesn't use the per-year universe, the listing rules or the company collapse. The backtest doesn't use it.

## Data Requirements

- `profile`: exchange, sector, isin, cik, companyName, currency, country (XETRA and SIX rule), isEtf, isFund
- `key_metrics` (FY): marketCap and its date (operating-company test and cost tier)
- `stock_eod`: daily adjClose and close x volume from 2004, for candidates and benchmarks
- `forex_prices_daily`: USD rates at each formation year end
- `delisted_companies`: delisting dates, which bound the last common bar

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2005-2024) |
| `screen.py` | Current-day pairs screen (see Known limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `generate_share_links.py` | Share links for the posts' queries |
| `README.md` | This file |
| `results/` | `returns_<market>.json` for the 15 markets and `exchange_comparison.json` (the 12 markets the charts read) |
| `charts/` | Growth and yearly returns for the US and Japan; CAGR for the markets of `exchange_comparison.json` invested in at least half the years (11), and share of years in cash for all 12 |

The per-year universe is built by `pairs_universe.py` at the repository root, shared with pairs-zscore and pairs-multi-pair.

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
