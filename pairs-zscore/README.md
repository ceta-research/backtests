# Z-Score Pairs Trading

Trades the price spread between two stocks of the same sector that moved together the year before. Each January the backtest forms up to 20 pairs from the previous year's prices, then checks every pair's spread daily through the year. When the spread sits 2 standard deviations from its recent mean it shorts the expensive leg and buys the cheap one, and it closes when the spread comes back within 0.5 standard deviations, after 60 trading days, or at a 5% loss. The question is whether a spread that converges most of the time makes money after costs. In 12 markets from 2005 to 2024 it didn't: every market's CAGR is negative, although 76.3% to 87.9% of trades closed on convergence.

## Academic Basis

- Gatev, Goetzmann & Rouwenhorst (2006), "Pairs Trading: Performance of a Relative-Value Arbitrage Rule", *Review of Financial Studies* 19(3), 797-827. Pairs matched on 12 months of prices and traded when they diverged by two standard deviations earned excess returns of up to 11% a year in US stocks over 1962-2002.
- Do & Faff (2010), "Does Simple Pairs Trading Still Work?", *Financial Analysts Journal* 66(4), 83-95, and Do & Faff (2012), "Are Pairs Trading Profits Robust to Trading Costs?", *Journal of Financial Research* 35(2), 261-287: the same rule tested on later data and after costs.

## Signal

| Step | Rule |
|------|------|
| Universe | Built for each trading year (see Universe below) |
| Pair formation | In the calendar year before the trading year: two candidates of the same sector whose daily returns correlate at 0.70 or more over at least 200 common days, ranked by correlation |
| Hedge ratio | Slope of log price A on log price B over the formation year, fixed for the trading year. A pair with a slope of zero or below is dropped |
| Half-life filter | The formation-year spread's AR(1) half-life must be 5 to 60 trading days |
| Pairs held | The first 20 by correlation that pass the half-life filter |
| Z-score | Log spread against its 40-bar rolling mean and standard deviation, on a full window of 40 common bars |
| Entry | \|z\| of 2.0 or more with no position open in the pair: short the expensive leg, buy the cheap one, half the pair's capital in each. Filled at the next bar's close |
| Exit | \|z\| of 0.5 or less (convergence), 60 trading days held (time stop), or a pair loss of 5% or more (loss stop), each filled at the next bar's close. A position still open closes at the pair's last common bar: the last trading day of the year, or earlier when a leg stops trading |
| Year's return | Each pair's trade returns are summed (not compounded) and the total is divided by the number of pairs formed. A formed pair that isn't entered, or never signals, earns 0% on its share |
| Cash year | Fewer than 3 formed pairs are entered: both legs need a first bar of at least 1.00 in local units in the 10 days from 2 January. The test reads the entry bars only. A cash year earns 0% |

## Parameters

- **Formation and trading:** formation year Y-1, trading year Y, for Y = 2005 to 2024 (20 years). Prices start in 2004.
- **Transaction costs:** size-tiered one-way rate per leg: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B. Half the pair's capital sits in each leg, so a round trip costs each leg's rate once on the pair's capital: 0.2% for two legs in the 0.1% tier. The cap is the leg's latest fiscal-year market cap dated 45 to 540 days before the trading year, converted to USD at the formation year-end rate. A leg with no such filing pays 0.5%.
- **Return floor:** a trade's pair return is floored at -200%.
- **Exits:** `--exit-method ltp`, the default and the only mode. A pair is one position and closes at the last bar both legs traded, so there is no drop book: `--exit-method drop` and `both` stop with an error.
- **Benchmark:** each market's own index, in local currency: S&P 500 (SPY) for the US, Nikkei 225, TSX Composite, Hang Seng, SSE Composite, KOSPI, TAIEX, OMX Stockholm 30, Sensex, FTSE 100 and DAX. South Africa has no index configured and is measured against SPY in USD. The results files store the benchmark under the key `spy` whatever the index, with `benchmark_symbol` null; the index is named in the run's log.
- **Risk-free rate (Sharpe):** fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 0.1% Japan, 6.5% India, 9.0% South Africa).

## Universe

Built separately for each trading year from the lines on the market's exchanges:

1. The line has a sector, isn't flagged as an ETF or fund, and has at least one fiscal-year `key_metrics` row with a market cap. That row can be dated at any time, so a company whose first filing came later can still be a candidate in an earlier year: this test isn't point-in-time.
2. It was listed through the formation year: at least 200 daily bars in that year, the last one no more than 10 days before year end. A name that stopped trading before then is out, and a name that listed during the year waits for the next one.
3. It traded: its formation-year median of daily close x volume is above zero. That median, in USD at the year-end rate, is its size. A line whose currency has no rate in the last 7 days of the year is out.
4. One line per security (same ISIN) and one per company (lines linked by ISIN, CIK, normalised name or ticker root), keeping a line that can be bought at entry, then the shortest symbol, then the larger size. The key misses a company carried under two tickers with different ISINs and names, so some pairs are one company twice (see Known limits).
5. The 30 largest by size in each sector. There is no size floor: a sector with fewer than 30 lines admits them all.

Sector is today's profile sector, not the sector at the time. XETRA holds Germany-domiciled companies only and LSE pence-quoted (GBp) lines only. Each year's record in the results lists the candidate count, the sector count, the smallest admitted size, each sector's 30th size, the lines removed by each step, and the pairs formed with their correlations.

The list no longer needs a name to be among today's 30 largest of its sector, so the size look-ahead of the earlier universe is removed. Survivorship is reduced, not fixed: the price data holds almost no company that stopped trading before 2015 in the US or 2020 elsewhere, so earlier years are measured on companies that survived to about then (see Known limits).

## Results (2005 to 2024)

Every one of the 12 markets lost money over the 20 years, from -0.37% a year (Sweden) to -4.65% (Taiwan), and each trails its benchmark, by 1.89pp (Hong Kong) to 13.95pp (India). The book is market-neutral by construction (beta to the benchmark between -0.064 and 0.024), so the index comparison mainly shows what holding the market earned; against zero it still loses everywhere. The average trade is negative in all 12, from -0.084% (Sweden) to -0.646% (Taiwan). No market is too thin to test: the most cash is South Africa's 9 years of 20.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Pairs traded | Trades a year | Convergence | Avg trade |
|--------|-----------|------|----------------|-------------|--------|--------|------------|--------------|---------------|-------------|-----------|
| Sweden (STO) * | OMX Stockholm 30 | -0.37% | 5.52% | -5.88 | -0.618 | -15.71% | 0 | 14.6 | 98.0 | 86.0% | -0.084% |
| South Africa (JNB) * | S&P 500 (USD) | -0.55% | 9.81% | -10.36 | -3.404 | -17.10% | 9 (2006-2011, 2013-2015) | 8.9 | 34.0 | 79.6% | -0.262% |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | -0.59% | 9.81% | -10.41 | -2.132 | -12.38% | 0 | 20.0 | 133.8 | 87.9% | -0.088% |
| Hong Kong (HKSE) | Hang Seng | -0.81% | 1.09% | -1.89 | -1.925 | -15.82% | 1 (2006) | 18.2 | 120.2 | 81.9% | -0.119% |
| Germany (XETRA) | DAX | -1.05% | 7.48% | -8.54 | -0.953 | -26.47% | 3 (2005-2007) | 12.6 | 73.5 | 80.4% | -0.209% |
| India (NSE) * | Sensex | -1.31% | 12.64% | -13.95 | -2.175 | -30.45% | 0 | 15.7 | 113.7 | 76.6% | -0.264% |
| Japan (JPX) | Nikkei 225 | -1.38% | 6.32% | -7.70 | -0.815 | -24.33% | 0 | 20.0 | 126.8 | 85.3% | -0.216% |
| UK (LSE) | FTSE 100 | -1.51% | 1.88% | -3.39 | -1.267 | -34.16% | 0 | 18.3 | 124.5 | 84.6% | -0.281% |
| China (SHZ + SHH) * | SSE Composite | -2.89% | 4.68% | -7.57 | -2.069 | -44.35% | 0 | 18.6 | 123.5 | 77.2% | -0.415% |
| Canada (TSX) | TSX Composite | -2.96% | 4.53% | -7.49 | -1.619 | -45.76% | 1 (2005) | 18.7 | 122.5 | 76.3% | -0.472% |
| Korea (KSC) * | KOSPI | -3.56% | 4.99% | -8.55 | -1.683 | -51.53% | 0 | 17.2 | 121.7 | 77.9% | -0.416% |
| Taiwan (TAI + TWO) | TAIEX | -4.65% | 6.92% | -11.57 | -1.920 | -61.43% | 1 (2020) | 14.2 | 96.2 | 81.0% | -0.646% |

Returns are in local currency, after costs. Pairs traded is the average number of pairs with at least one trade per invested year; each pair holds two names. Convergence is the share of trades closed at \|z\| of 0.5 or less. Markets marked * aren't drawn in the comparison charts (see below).

- **US:** -0.59% a year against 9.81% for SPY, with volatility of 1.22% against 17.04% and a max drawdown of -12.38% against -36.23%. $1 at the start of 2005 ends at $0.89 (total return -11.25%). Six years are positive (2008 +2.57%, 2009, 2012, 2022, 2023, 2024) and 14 negative, the worst 2014 at -2.46%. 2,676 trades: 87.9% closed on convergence, 6.0% on the loss stop, 0.4% on the time stop and 5.7% at year end.
- **Charted markets:** the two comparison charts draw seven markets (`CONTENT_EXCHANGES` in `generate_charts.py`): the US, Japan, the UK, Hong Kong, Taiwan, Germany and Canada. Their convergence runs from 76.3% (Canada) to 87.9% (US) and their CAGR from -0.59% (US) to -4.65% (Taiwan). The other five are left out of the charts. The posts gave extreme 2005-2006 returns as the reason; those years came from an earlier results file and aren't in these: the largest positive year of the five is now +9.81% (India, 2008).
- **South Africa** is the thinnest book: 11 invested years, 8.9 pairs traded and 34.0 trades a year. Its candidate list is 63 to 142 names a year in 9 to 10 sectors, and in 9 years 0 to 2 pairs formed. Some recorded pairs are distinct banks correlating at 0.97 to 0.99 (SBK/SLM 0.9927 and NED/SNT 0.9796 in 2005, JSE/SBK 0.9741 in 2012), which points at bad price bars. Its benchmark is the S&P 500 in USD and its Sharpe uses a 9.0% risk-free rate.
- **Dividends in benchmarks:** returns use `adjClose`, which includes dividends. A pair receives the long leg's dividends and pays the short leg's, so most of them cancel. SPY and the DAX include dividends; the other nine benchmarks are price indices. Every market trails by at least 1.89pp, so no dividend yield changes a sign here.
- **Sharpe:** volatility is 1.22% to 3.95% a year, so a small loss gives a large negative Sharpe. Compare it within a market, not across them.
- **Last-bar closes:** 8 positions in 6 markets (3 in India, 1 each in the US, Canada, China, the UK and South Africa) closed at the pair's last common bar more than 10 days before year end, listed under `ltp.fills` in each file. Three are Indian pairs whose two companies merged (HDFC with HDFCBANK, INOXLEISUR with PVR, IDFC with IDFCFIRSTB). In the US BBL's recorded delisting on 2022-01-31 ended its pair with BHP, and the UK's is SKG.L/SWR.L in 2024, one price series under two tickers (see Known limits).

### Cash years

| Market | Cash years | Pairs formed in those years |
|--------|------------|-----------------------------|
| South Africa (JNB) | 2006-2011, 2013-2015 | 0, 1, 1, 1, 2, 1, 0, 1, 2 |
| Germany (XETRA) | 2005-2007 | 2, 0, 2 |
| Canada (TSX) | 2005 | 2 |
| Hong Kong (HKSE) | 2006 | 1 |
| Taiwan (TAI + TWO) | 2020 | 2 |

In every cash year fewer than 3 pairs formed at all; no year went to cash because formed pairs failed the entry test. A cash year earns 0% and counts in every 20-year figure. The other seven markets were invested in all 20 years.

## Rerun October 2026

These results replace the files published before. METHODOLOGY.md describes the campaign's XETRA and LSE listing rules and same-ISIN dedup (Universe Construction) and the gap in the price data (Limitations: Survivorship Bias). The per-year universe is this topic's own and is described under Universe above. For this topic:

- **Per-year universe** (above), in place of one list of today's 30 largest active names per sector by latest market cap, used for all 20 years. Same-ISIN and same-company dedup, and the XETRA and LSE listing rules.
- **Costs** are charged once per leg per round trip. The earlier code charged each leg's rate twice, 0.4% a trade for two 0.1% legs, and tiered a leg by today's market cap in its reporting currency against dollar thresholds.
- **Cash** is decided on the entered pairs before the year trades. Earlier it was decided on the pairs that had traded by the end of the year.
- **Defects fixed:** a pair with an undefined correlation (a leg whose price didn't move) could rank first; the z-score could start on a window of fewer than 40 bars.
- **Markets:** India is NSE alone and Canada is TSX alone. The comparison file published before held BSE+NSE for India, and the earlier Canada file was TSX+TSXV.

Against the per-market files published before, which carried the same local benchmarks:

- **Korea** fell from -1.08% to -3.56% and its max drawdown went from -19.52% to -51.53%. The average trade went from -0.136% to -0.416%. Two changes act here and the files can't separate them: company dedup removes 59 to 104 Korean lines a year, and its ticker-root rule links a preferred line such as 005935.KS to its common share 005930.KS, so share-class pairs can no longer form; and 194 to 234 of the 243 to 245 candidates in 2005-2008 have no filing in the cost window and pay 0.5% a leg.
- **Taiwan** fell from -2.36% to -4.65% and its max drawdown went from -38.00% to -61.43%. No dedup acts on Taiwan. The average trade went from -0.295% to -0.646%, 19 of 20 years are negative and the other is cash, so the drawdown builds over the whole period. 183 to 219 candidates a year in 2005-2007 pay the 0.5% tier for want of a filing. Cash years fell from 3 to 1.
- **South Africa** went from +27.94% to -0.55%. The earlier file held four years above +100% (+331.02% in 2006), not plausible for a market-neutral book, and the posts left South Africa out of their results.
- **US** rose from -1.23% to -0.59% and its max drawdown improved from -22.62% to -12.38%. The average trade went from -0.183% to -0.088%. For two legs in the 0.1% tier the halved cost is worth 0.2pp a trade; the average trade gained 0.095pp, so the new pairs or higher cost tiers took back part of it. The files can't split the two.
- **Germany** rose from -2.58% to -1.05%, the **UK** from -2.66% to -1.51% and **Hong Kong** from -1.47% to -0.81%. **China** fell from -1.32% to -2.89%: its 2005 was +33.03% on 7 pairs and is -0.21% on 20.
- Japan (+0.05pp), Sweden (-0.24pp), Canada (-0.44pp) and India (-0.73pp) moved by less than 1pp. Benchmark CAGRs are unchanged.

The README and charts published before showed a still older comparison file, benchmarked on SPY in every market. It had Taiwan at -0.09%, Korea at +5.98%, Sweden at +4.83% and India (BSE+NSE) at +6.45%.

## Usage

Run from the repository root. `--exit-method` defaults to `ltp`, the only mode. The listing rules need no flag: they apply whenever XETRA or LSE is run as its own market.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`:

```bash
# One market, written to the committed results file
python3 pairs-zscore/backtest.py --exchange NYSE,NASDAQ,AMEX --output pairs-zscore/results/returns_NYSE_NASDAQ_AMEX.json
python3 pairs-zscore/backtest.py --exchange TAI,TWO --output pairs-zscore/results/returns_TAI_TWO.json

# After all 12: rebuild the comparison file the charts read, check it, then check the pairs invariants
python3 scripts/rebuild_comparison.py pairs-zscore --keep NYSE_NASDAQ_AMEX JPX TSX HKSE SHZ_SHH KSC TAI_TWO STO JNB NSE LSE XETRA
python3 scripts/rebuild_comparison.py pairs-zscore --keep NYSE_NASDAQ_AMEX JPX TSX HKSE SHZ_SHH KSC TAI_TWO STO JNB NSE LSE XETRA --check
python3 scripts/check_pairs_results.py pairs-zscore

# Charts from results/exchange_comparison.json, written to pairs-zscore/charts/
python3 pairs-zscore/generate_charts.py

# Without transaction costs (not committed)
python3 pairs-zscore/backtest.py --exchange NYSE,NASDAQ,AMEX --no-costs
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| Canada | `TSX` | `returns_TSX.json` | |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Taiwan | `TAI,TWO` | `returns_TAI_TWO.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| South Africa | `JNB` | `returns_JNB.json` | |
| India | `NSE` | `returns_NSE.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |

Use `--exchange` as shown. The exchange list sets the universe name written into the file; `--preset canada` adds TSXV. `--global` runs the same 12 exchange sets and writes `returns_<KEY>.json` next to the `--output` path, but it isn't how these files were made: a market that fails inside the loop is recorded as an error entry and its old file stays on disk, so run the checks above afterwards.

## Known limits

- **Survivorship:** the price data holds almost no company that stopped trading before 2015 (US) or 2020 (other markets). Years before then are measured on companies that survived to about then, and the effect on these results isn't measured. The per-year universe removes the size look-ahead and admits names that stopped trading later; it reduces survivorship, it doesn't remove it.
- **Operating-company test:** a line needs a fiscal-year filing of any date, so the test isn't point-in-time. Sector is today's.
- **Twins the company key misses:** the key links lines by ISIN, CIK, normalised name or ticker root, so one company under two tickers with different ISINs and names stays in as two names, and `check_pairs_results.py` passes the pair. Named from the tickers:
  - Japan, 6501.T/9086.T: Hitachi against Hitachi Transport System's code. Two companies whose series correlate at 0.996 or more in 14 of the years formed, which points at a price-data fault. Formed in 18 of 20 years (all but 2013 and 2021), the most correlated pair in 17 of them (rank 2 in 2009), at 0.9621 to 0.9997.
  - Japan, 5844.T/8369.T: Kyoto Financial Group against Bank of Kyoto, one bank before and after a holding-company reorganisation. Formed in 2006, 2009, 2012 and 2016, rank 1 or 2, at 0.9939 to 0.9998.
  - UK, SKG.L/SWR.L: Smurfit Kappa against Smurfit Westrock, which carries Smurfit Kappa's price history. The most correlated pair in all 10 years it is formed (2013-2015 and 2018-2024), at 0.9917 to 1.0000.
  - India, TATAMTRDVR.NS/TMPV.NS: Tata Motors' differential-voting-rights (DVR) line against its ordinary line (under the ticker TMPV since the 2025 demerger). Formed in 10 years (2012-2014, 2016-2019, 2021-2023), at 0.7554 to 0.8913. In 2012 it is 1 of 8 formed pairs, an eighth of the book.
  - US, BBL/BHP: BHP's two dual-listed lines, in 2011, 2012 and 2022, at 0.9059 to 0.9863.
  - Canada, BEP-UN.TO/BEPC.TO: Brookfield Renewable partnership units against its exchangeable shares, in 2022 and 2023, at 0.8173 to 0.9032.

  A trade returns half the difference of its legs' returns, so a pair of one price series under two tickers earns about nothing before costs and pays the cost of every trade. Each such pair holds a twentieth of the book in the years Japan and the UK form it (an eighteenth in the UK's 2014). The files record no per-pair returns, so what these pairs cost each book isn't measured. Germany pairs PAH3.DE (Porsche SE, which controls Volkswagen) with VOW.DE in 7 years: a holding company against its main asset, not one company twice.
- **Lines that aren't ordinary shares,** classified by ticker: Canada holds REIT and partnership units (`-UN`) in 22 of 712 invested slots (3.1%), 16 of the 40 slots in 2024. Germany holds PAH3.DE, a preference share, in 20 of 430 (4.7%). India holds TATAMTRDVR.NS, Tata Motors' DVR line, in 10 of 626 (1.6%). No other market holds any by that test. The universe drops lines flagged as ETFs or funds.
- **US** is every line on NYSE, NASDAQ and AMEX, so it holds foreign companies' depositary receipts (VALE, ITUB, BP, SHEL among them).
- **Taiwan** is run as TAI + TWO, but no TWO line is held in any year.
- **Cost tier fallback:** a candidate with no filing in the 45-540-day window pays 0.5%. That covers most Korean (234 of 243 in 2005) and Taiwanese (219 of 225 in 2005) candidates in the early years, and 174 of 311 Japanese and 177 of 259 Indian candidates in 2005.
- **UK 2005** returned +12.10% on 7 pairs, the largest year in any market. Its pairs are ordinary UK companies (SSE with UU, CNA with UU, BLND with LAND), and the files don't show whether a bad bar is behind it.
- **Short selling** is assumed in every market with no borrow cost. China and India restrict it in practice.
- **Hedge ratio and half-life:** the ratio is fixed for the year, and the AR(1) half-life filter isn't a formal cointegration test.
- **Charts:** the convergence chart prints a fixed note, "All exchanges: 77-87% convergence"; the seven markets it draws run from 76.3% to 87.9%. It labels both the US and the UK "United". The CAGR chart draws one dashed line at SPY's 9.81%, though each market has its own benchmark.
- `screen.py` (the current-day screen) predates the per-year universe: it takes today's active listings above a local-currency market cap floor, with no company dedup and no guard against undefined correlations. The backtest doesn't use it.

## Data Requirements

- `profile`: exchange, sector, isin, cik, companyName, currency, country, isEtf, isFund
- `key_metrics` (FY): marketCap, reportedCurrency, dateEpoch (operating-company test and cost tier)
- `stock_eod`: close and volume (size), adjClose (returns, correlations, spreads)
- `forex_prices_daily`: USD rates for size and the cost tier
- `delisted_companies`: bounds a leg's bars at its delisting date

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2005-2024); the universe comes from `pairs_universe.py` at the repository root |
| `screen.py` | Current z-score screen (see Known limits) |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `charts/` | US growth and annual returns, CAGR by market and convergence against average trade for the seven charted markets |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
