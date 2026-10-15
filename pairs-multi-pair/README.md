# Pairs Trading: Multi-Pair Portfolio Construction

**Strategy ID:** pairs-06-multi-pair
**Series:** Pairs Trading. The portfolio layer on top of pairs-zscore.

Each year the strategy finds up to 20 pairs of same-sector stocks whose daily returns moved together over the previous year, trades each pair with a daily z-score rule (long one line, short the other, in equal amounts), and combines the pairs into books of 5, 10, 15 and 20 with equal or inverse-volatility weights. The primary book is 20 pairs with inverse-volatility weights. It asks whether holding many pairs at once smooths the returns of single-pair trading, and which weighting does better.

## Academic Basis

Gatev, E., Goetzmann, W. & Rouwenhorst, K. (2006). "Pairs Trading: Performance of a Relative-Value Arbitrage Rule." *Review of Financial Studies*, 19(3), 797-827.

## Universe

The candidates are built again for every trading year from the year before it, the formation year (`pairs_universe.py`, shared with pairs-zscore and pairs-fundamentals):

1. Lines on the market's exchanges that have a sector in `profile` and at least one FY `key_metrics` row with a market cap. That filing can be dated any time, a later one included, so this test is not point-in-time. Lines that `profile` flags as an ETF or a fund are out. XETRA holds Germany-domiciled companies only and LSE pence-quoted (GBp) lines only.
2. Listed and traded through the formation year: at least 200 daily bars in it, the last one no more than 10 days before the year end, and a positive median daily traded value (close x volume).
3. One line per security (same ISIN), then one line per company (same ISIN, CIK, company name or ticker root), keeping the line with a usable January entry price.
4. The top 30 of each sector by the formation year's median daily traded value, in USD at the year-end exchange rate.

This replaces a fixed list of today's 30 largest companies per sector, which held the same names in every year from 2005 and could not hold a company that had stopped trading. Size look-ahead is removed. Survivorship is reduced, not fixed: the price data holds almost no company that stopped trading before 2015 in the US or 2020 elsewhere, so earlier years are measured on companies that survived at least that long (METHODOLOGY.md, Limitations: Survivorship Bias). Sectors are today's `profile` sectors.

## Signal

| Parameter | Value |
|---|---|
| Pair formation | Same-sector pairs of the year's candidates, daily-return correlation 0.70 or more over the formation year, at least 200 common days; NaN correlations are excluded |
| Half-life filter | AR(1) half-life of the log-price spread (beta from the formation year) between 5 and 60 days |
| Pairs formed | The 20 most correlated pairs that pass the half-life filter (the top 200 by correlation are tested) |
| Entry | Each leg's first bar in the 10 days after 1 January must print 1.00 or more and fall inside the pair's delisting bound. A formed pair that is not entered sits the year out and is not replaced |
| Z-score | 40-bar rolling window of the spread; a full 40 common bars are required |
| Trade entry | \|z\| above 2.0, executed at the next bar's close |
| Trade exit | \|z\| below 0.5, 60 trading days, or pair P&L of -5% or worse, each executed at the next bar's close. A position still open at the year end closes at the year's last common bar |
| Min entered pairs | 3. A year with fewer than 3 entered pairs is cash and earns 0% |

## Portfolio Construction

- **Sizes:** 5, 10, 15 and 20 pairs: the top N entered pairs by correlation.
- **Equal weight:** each of the N pairs gets 1/N, whether or not it trades.
- **Inverse volatility:** weight proportional to 1 / the annualised standard deviation of daily spread changes in the formation year, normalised to sum to 1. A pair with no volatility estimate gets an equal share.
- **Returns:** a trade returns direction x (return of A - return of B) / 2, after costs. A pair's year is the sum of its trade returns, and the book's year is the weighted sum of its pairs. Capital not in a trade earns nothing.
- **Rebalancing:** pairs are formed once a year (1 January, from the previous calendar year); positions are monitored daily.

## Parameters

- **Period:** 2005 to 2024, 20 annual periods.
- **Transaction costs:** size-tiered, one way per leg: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B. Half the pair's capital sits in each leg, so a round trip costs the two legs' one-way rates once. The tier reads each leg's latest FY market cap whose fiscal year ended 45 to 540 days before the trading year, in USD at the formation year-end rate; a leg with none pays 0.5%. Short-borrow costs aren't modelled, and short selling is assumed to be available in every market.
- **Exits:** a pair closes at the last bar both legs traded (`--exit-method ltp`, the only mode). A leg's bars stop 7 days after a recorded delisting. There is no drop book for a pair, so the exit rule has no separate effect to measure.
- **Benchmark:** each market's own index, in local currency. The US is measured against SPY. South Africa has no local index configured and is measured against the S&P 500 (SPY) in USD.
- **Risk-free rate (Sharpe):** fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 3.5% UK, 9.0% South Africa).

## Results (2005 to 2024, 20 pairs, inverse-volatility weights)

The strategy trailed its own market's index in all 12 markets, by 1.72 points a year (Hong Kong) to 13.88 (India), and every Sharpe ratio is negative. It earned a negative CAGR everywhere, from -0.45% (South Africa) and -0.46% (Sweden) to -4.56% (Taiwan). No market is too thin to test: South Africa, the least invested, held pairs in 11 of the 20 years. Japan's and the UK's results include a data fault, one price series under two tickers held as their top pair in most years (see below).

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Sharpe | Max DD | Cash years | Avg pairs |
|--------|-----------|------|----------------|-------------|--------|--------|------------|-----------|
| Sweden (STO) | OMX Stockholm 30 | -0.46% | 5.52% | -5.98 | -0.678 | -17.42% | 0 | 14.6 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | -0.62% | 9.81% | -10.43 | -2.242 | -12.25% | 0 | 20.0 |
| Hong Kong (HKSE) | Hang Seng | -0.63% | 1.09% | -1.72 | -1.639 | -14.26% | 1 (2006) | 18.2 |
| Germany (XETRA) | DAX | -0.82% | 7.48% | -8.30 | -0.910 | -24.31% | 3 (2005-2007) | 12.6 |
| India (NSE) | Sensex | -1.25% | 12.64% | -13.88 | -2.076 | -31.03% | 0 | 15.7 |
| UK (LSE) | FTSE 100 | -1.61% | 1.88% | -3.49 | -1.729 | -31.80% | 0 | 18.3 |
| Japan (JPX) | Nikkei 225 | -2.05% | 6.32% | -8.37 | -1.707 | -33.96% | 0 | 20.0 |
| Canada (TSX) | TSX Composite | -2.55% | 4.53% | -7.08 | -1.644 | -40.93% | 1 (2005) | 18.7 |
| China (SHZ + SHH) | SSE Composite | -3.25% | 4.68% | -7.93 | -1.813 | -48.36% | 0 | 18.6 |
| Korea (KSC) | KOSPI | -3.81% | 4.99% | -8.80 | -1.731 | -54.06% | 0 | 17.2 |
| Taiwan (TAI + TWO) | TAIEX | -4.56% | 6.92% | -11.48 | -1.809 | -60.67% | 1 (2020) | 14.2 |
| South Africa (JNB) | S&P 500 (SPY, USD) | -0.45% | 9.81% | -10.26 | -3.208 | -16.02% | 9 (2006-2011, 2013-2015) | 8.9 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. Avg pairs is the average number of pairs entered in invested years; every entered pair traded at least once, so it is also the average of pairs that traded. Each pair is two lines. South Africa is listed last: see below.

- **US:** -0.62% a year against 9.81% for SPY, a max drawdown of -12.25% against -36.23%, beta -0.02, and no cash years. 2,676 trades, 87.9% of them closed on convergence, with an average trade of -0.088% after costs.
- **Deeper than the index:** the market-neutral book drew down further than its own index in Taiwan (-60.67% against -44.84%), Korea (-54.06% against -39.33%), Canada (-40.93% against -35.46%) and the UK (-31.80% against -30.90%).
- **Dividends in benchmarks:** returns use dividend-adjusted prices on both legs. The US and South Africa are measured against SPY, which includes dividends, and Germany against the DAX, a performance index. The other nine are price indices, so excess against them is overstated by roughly the local dividend yield. Every excess here is negative, so counting the index dividends would only widen the gaps.
- **Sharpe:** the books' annual volatility runs from 1.17% (US) to 3.94% (Korea), so any CAGR below the market's risk-free rate gives a large negative Sharpe. Compare it with the index's Sharpe in the same market, not across markets.
- **Thin books:** a year is invested with as few as 3 entered pairs. Invested years with 5 or fewer: Taiwan 2014 and 2018 (3 each), South Africa 2005 (3) and 2012 (5), Germany 2008 and 2014 (4 each) and 2018 (5), India 2006 (4), Sweden 2014 (4), Korea 2014 and 2015 (5 each), Hong Kong 2007 (5) and Canada 2006 (5). Those years are concentrated bets on a few spreads, whatever the 20-pair label says.
- **Japan and the UK:** each book's most correlated pair in most years is one price series under two tickers: Japan's 6501.T/9086.T in 17 of 20 years, the UK's SKG.L/SWR.L in 10 (see Known limits). A trade returns half the difference of its legs' returns, so on such a pair it earns about nothing before costs, and inverse-volatility gives the pair a large weight. Japan's -2.05% and the UK's -1.61% include that fault and aren't a clean measure of the strategy. Japan's inverse-volatility book trails its equal-weight book by 0.67 to 0.72 points at every size.
- **South Africa:** invested in 11 of 20 years, above the half line, so it isn't too thin by rule. But the run formed 0 to 2 pairs in each of its 9 cash years, held 10 or fewer pairs in 9 of its 11 invested years, and its 2009 prices show several distinct companies correlating at 0.96 to 0.98, a sign of market-wide bad bars. Its benchmark is a US index in dollars. Treat it as unreliable.

### Portfolio size

US books by size and weighting (the file `results/diversification_analysis.json`):

| Pairs | Equal CAGR | Inv-vol CAGR | Equal Sharpe | Inv-vol Sharpe | Equal Max DD | Inv-vol Max DD |
|-------|-----------|--------------|--------------|----------------|--------------|----------------|
| 5 | +0.48% | +0.23% | -0.324 | -0.420 | -5.90% | -7.32% |
| 10 | -0.04% | -0.13% | -0.590 | -0.658 | -12.64% | -10.67% |
| 15 | -0.47% | -0.53% | -1.023 | -1.097 | -13.44% | -12.54% |
| 20 | -0.59% | -0.62% | -2.132 | -2.242 | -12.38% | -12.25% |

In the US, adding pairs lowers the CAGR at every step, from +0.48% with 5 pairs to -0.59% with 20 (equal weight), and lowers the Sharpe at every step. Equal weight beats inverse-volatility on CAGR and Sharpe at every size; inverse-volatility has the shallower drawdown at 10, 15 and 20 pairs. The pairs added after the first five are less correlated and on average lose after costs, so more of them pull the book's return down.

The US pattern doesn't hold everywhere. CAGR of the 5-pair and 20-pair books:

| Market | 5 equal | 5 inv-vol | 20 equal | 20 inv-vol |
|--------|---------|-----------|----------|------------|
| US | +0.48% | +0.23% | -0.59% | -0.62% |
| India | -0.20% | -0.43% | -1.31% | -1.25% |
| Hong Kong | -0.38% | -0.27% | -0.79% | -0.63% |
| Canada | -2.79% | -2.36% | -2.96% | -2.55% |
| Sweden | -0.38% | -0.45% | -0.38% | -0.46% |
| China | -3.01% | -3.19% | -3.03% | -3.25% |
| Taiwan | -4.73% | -4.56% | -4.65% | -4.56% |
| Japan | -1.54% | -2.24% | -1.38% | -2.05% |
| South Africa | -0.29% | -0.37% | -0.55% | -0.45% |
| Germany | -1.26% | -1.18% | -1.05% | -0.82% |
| UK | -2.18% | -2.13% | -1.51% | -1.61% |
| Korea | -4.75% | -4.91% | -3.56% | -3.81% |

The two US 5-pair books are the only ones in any market with a positive CAGR. On the primary inverse-volatility weighting, going from 5 pairs to 20 lowers the CAGR by more than 0.2 points in three markets (US, India, Hong Kong), moves it by less than 0.2 points in six (Canada, Sweden, China, Taiwan, Japan, South Africa) and raises it in three (Germany, UK, Korea). Japan's and the UK's rows carry the duplicate-series pair at rank 1 or 2 in most years, in every book size (see Known limits).

## Rerun October 2026

These results were rerun with the campaign's changes (METHODOLOGY.md, Universe Construction and Limitations: Survivorship Bias) and with fixes to this backtest:

- **Per-year universe** in place of today's 30 largest companies per sector (see Universe), with same-ISIN and same-company dedup and the XETRA and LSE listing rules.
- **Defect fixes:** a NaN correlation (a leg with a frozen price) passed the 0.70 filter and ranked first; it is now excluded. The cost charge was doubled; a round trip now costs each leg's one-way rate once. The cost tier read today's market cap in the reporting currency, which put every Japanese and Korean leg in the 0.1% tier; it now reads the filing market cap in USD. Exits executed on the signal bar; they now execute at the next bar, like entries. The z-score could signal on a partial window; it needs 40 bars. A year was cash when fewer than 3 pairs traded during it, which is known only in December; it is now cash when fewer than 3 pairs are entered in January.
- **Exits:** a pair whose leg stops trading closes at the last bar both legs traded. 8 positions in 6 markets closed early this way (3 in India, 1 each in the US, UK, Canada, China and South Africa); each results file lists them under its `ltp` key.
- **Legs:** Canada is TSX alone (it was the TSX + TSXV preset), and India's file is NSE alone (a BSE + NSE file is removed).

Benchmark CAGRs are unchanged in all 12 markets. Every market trailed its index before and after; no verdict changed. What moved by more than 2 points:

- **Korea** fell from -0.68% to -3.81% and its max drawdown deepened from -12.75% to -54.06%. The new return is lower in 16 of 20 years, the average trade fell from -0.116% to -0.416% and loss-stop exits rose from 11.8% to 17.1% of trades. Korean legs now pay the USD cost tier (the design review put the average one-way rate of the 2015 candidates at 0.42%, against 0.10% before), and the company dedup removes common-preferred pairs of one company, which made up 14 of the old list's top 20 Korean pairs in formation 2019. The files can't split the change between the two.
- **China** fell from -0.59% to -3.25%. Most of it is 2005 and 2006: the old book held 7 and 3 pairs then and returned +28.93% and +21.75%. The per-year universe has 320 and 319 candidates in those years, forms 20 pairs in each and returns -0.86% and -1.58%. Over 2007-2024 the CAGR moved from -3.11% to -3.47%.
- **Taiwan** fell from -1.67% to -4.56%, and its max drawdown deepened from -30.10% to -60.67%. The decline is broad: the new return is lower in 18 of 20 years (2019 is higher, and 2020 is cash in both). 2007 and 2018 were cash before because only 2 pairs traded in each; on the entry-side rule they hold 6 and 3 pairs and lose 5.44% and 7.49%. Holding those two years at 0% gives -3.92%, so they account for about 0.6 of the 2.9 points. The average trade fell from -0.228% to -0.646%. Taiwanese legs now pay the USD cost tier, and the universe changed too (225 to 263 candidates a year, against a fixed list of 263 names before). The files can't split the change between the two.
- **South Africa** rose from -13.18% to -0.45%, and its max drawdown went from -94.09% to -16.02%. The old 2006 (-80.74% on 6 pairs), 2010 (-25.00%) and 2015 (-23.09%) are now cash years: the new run forms 0 to 2 pairs in 2006-2011 and 2013-2015.
- The other eight markets moved by -0.76 to +1.47 points. Germany's cash years moved from 2006, 2015 and 2017 to 2005-2007, and the UK's single cash year (2005) is now invested.

## Usage

Run from the repository root. `--exit-method` accepts only `ltp` (the default); `drop` and `both` stop at the parser. The listing rules need no flag: they apply whenever XETRA or LSE is run as its own market.

The committed files were made one market at a time, each with `--exchange` and an explicit `--output`. A run also writes `diversification_<output file name>` beside its output:

```bash
# One market, written to the committed results file
python3 pairs-multi-pair/backtest.py --exchange NYSE,NASDAQ,AMEX --output pairs-multi-pair/results/returns_NYSE_NASDAQ_AMEX.json
python3 pairs-multi-pair/backtest.py --exchange XETRA --output pairs-multi-pair/results/returns_XETRA.json

# The US run's diversification file is committed under its own name; the other markets' are not committed
mv pairs-multi-pair/results/diversification_returns_NYSE_NASDAQ_AMEX.json pairs-multi-pair/results/diversification_analysis.json

# After all 12: rebuild the comparison file the charts read, check it, then check the pairs invariants
python3 scripts/rebuild_comparison.py pairs-multi-pair
python3 scripts/rebuild_comparison.py pairs-multi-pair --check
python3 scripts/check_pairs_results.py pairs-multi-pair

# Charts from results/exchange_comparison.json, written to pairs-multi-pair/charts/
python3 pairs-multi-pair/generate_charts.py

# Without transaction costs (gross of costs)
python3 pairs-multi-pair/backtest.py --exchange NYSE,NASDAQ,AMEX --no-costs
```

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` | |
| India | `NSE` | `returns_NSE.json` | |
| Korea | `KSC` | `returns_KSC.json` | |
| Sweden | `STO` | `returns_STO.json` | |
| UK | `LSE` | `returns_LSE.json` | GBp lines only |
| Hong Kong | `HKSE` | `returns_HKSE.json` | |
| Japan | `JPX` | `returns_JPX.json` | |
| Germany | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Canada | `TSX` | `returns_TSX.json` | |
| China | `SHZ,SHH` | `returns_SHZ_SHH.json` | |
| Taiwan | `TAI,TWO` | `returns_TAI_TWO.json` | |
| South Africa | `JNB` | `returns_JNB.json` | |

Use `--exchange` as shown. The exchange list sets the universe name written into the file, so `--preset us`, `--preset china`, `--preset taiwan` and `--preset southafrica` write a different name (US_MAJOR, China, Taiwan, JSE), and `--preset canada` adds TSXV.

`--global` runs the same 12 exchange sets and writes `returns_<KEY>.json` next to whatever path `--output` gives, plus that file as the combined comparison. It isn't how the committed files were made: a market that fails inside the loop is recorded as an error entry and its old file stays on disk, so run the rebuild and both checks above afterwards.

Current pairs: `python3 pairs-multi-pair/screen.py` (US) or `python3 pairs-multi-pair/screen.py --preset japan --top-n 30`. The screen predates the per-year universe, the dedup and the listing rules, and the backtest doesn't use it.

## Known limits

- **Survivorship:** reduced, not fixed. The universe is rebuilt each year from the lines that traded then, but the price data holds almost no company that stopped trading before 2015 (US) or 2020 (elsewhere).
- **Filing test and sectors:** a line qualifies on an FY filing of any date, so a company that first filed after the formation year can be a candidate in it. Sectors are today's.
- **Same-entity pairs and duplicate price series:** the company dedup joins lines by ISIN, CIK, company name or ticker root. Six pairs that are one company, or one price series under two tickers, get through it, and one Canadian company is held as two lines. These were identified from the tickers and the formation-year correlations, not from a data field, and `scripts/check_pairs_results.py` passes them because their company keys differ.
  - Japan, 6501.T/9086.T: Hitachi against Hitachi Transport System's code. Two companies whose series correlate at 0.996 or more in 14 years, which points at a price-data fault. Formed in 18 of 20 years (all but 2013 and 2021), the most correlated pair in 17 of them (rank 2 in 2009), at 0.9621 to 0.9997.
  - Japan, 5844.T/8369.T: Kyoto Financial Group against Bank of Kyoto, one bank before and after a holding-company reorganisation. Formed in 2006, 2009, 2012 and 2016, rank 1 or 2, at 0.9939 to 0.9998.
  - UK, SKG.L/SWR.L: Smurfit Kappa against Smurfit Westrock, which carries Smurfit Kappa's price history. The most correlated pair in all 10 years it is formed (2013-2015 and 2018-2024), at 0.9917 to 1.0000. Its 2024 trade is one of the 8 ltp fills.
  - US, BBL/BHP: the two ADRs of BHP's dual-listed structure. Formed in 2011, 2012 (the most correlated pair both years) and 2022.
  - India, TATAMTRDVR.NS/TMPV.NS: Tata Motors' differential-voting-rights and ordinary lines. Formed in 10 years, the most correlated pair in 2012, 2017 and 2019.
  - Canada, BEP-UN.TO/BEPC.TO: Brookfield Renewable's partnership units and exchangeable shares. Formed in 2022 and 2023 (rank 1 in 2023).
  - Canada, CPG.TO and VRN.TO: Crescent Point under its old ticker and under Veren, its 2024 name, both candidates in 7 years (2007, 2015, 2018-2020, 2023, 2024). They aren't paired with each other, but 14 spreads are held twice, such as BTE.TO/CPG.TO and BTE.TO/VRN.TO at ranks 5 and 6 in 2015; in 2023, 10 of the 20 pairs are 5 doubled spreads. The two lines fill 31 of 712 leg slots (4.4%).

  Every formed pair was entered in each of those years, so each pair sits in every book size it ranks within. Inverse-volatility weights a pair by 1 / the volatility of its daily spread change. Assuming the two legs are about equally volatile, that volatility for two series correlating at 0.996 is about a seventh of a 0.80-correlation pair's, and at 0.9999 about a fortieth. Against 19 pairs like that, such a pair would take roughly a quarter of the 20-pair inverse-volatility book at 0.996 and about 70% at 0.9999, and more of the 5-pair book. Per-pair weights aren't in the results, so this is an estimate. It fits Japan, whose inverse-volatility CAGR trails its equal-weight CAGR by 0.67 to 0.72 points at every size (US: 0.03 to 0.25).
- **Lines that aren't ordinary shares:** Germany's legs include PAH3.DE, Porsche SE's preference share and its only listed line, in 20 of 430 leg slots (4.7%) of formed pairs in invested years; it is paired with VOW.DE in 7 years. Canada's include REIT and partnership units (`-UN.TO`) in 22 of 712 slots (3.1%), and India's the Tata Motors DVR line in 10 of 626 (1.6%). Judged by ticker, no formed pair holds a fund share class, an ETF, another preferred line or an NVDR line.
- **Taiwan** is run on TAI and TWO, but none of the 115 lines in its formed pairs is a TWO line.
- **Short selling** is assumed possible at no borrow cost in every market, including those with restrictions on shorting (India, China, Korea, Taiwan).
- **Primary book:** the 20-pair book holds fewer than 20 pairs in years when fewer pass the filters (see Thin books above).
- **Charts:** `generate_charts.py` draws the S&P 500 (SPY) as the reference line for every market in the exchange comparison chart, leaves South Africa out of it by name, and carries a fixed subtitle stating 40% cash for Germany, which is 3 of 20 years (15%) in these results.

## Data Requirements

- `profile`: exchange, sector, country (XETRA rule), currency (LSE rule, FX), isin, cik, companyName (dedup), isEtf, isFund
- `key_metrics` (FY): marketCap and its period end (operating-company test, cost tier)
- `stock_eod`: adjClose, close and volume from 2004 (formation, trading, traded value)
- `forex_prices_daily`: USD rates at each formation year end
- `delisted_companies`: delisting dates (pair exits)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2005-2024) |
| `screen.py` | Current pairs screen; predates the per-year universe |
| `generate_charts.py` | Charts from `results/exchange_comparison.json` |
| `README.md` | This file |
| `results/returns_<KEY>.json` | One per market: the 20-pair inverse-volatility book, per-year universe counts, formed pairs, trade stats and `diversification_analysis` |
| `results/exchange_comparison.json` | The 12 per-market files, keyed by market |
| `results/diversification_analysis.json` | The US run's size and weighting table |
| `charts/` | Output of `generate_charts.py`: US growth, US annual returns, US size curve, CAGR by market, India size curve. Chart files aren't committed, so run it locally |

## Related Topics

| Topic | What it covers |
|-------|----------------|
| pairs-screening | Which stocks move together |
| pairs-cointegration | Statistical tests for mean reversion |
| pairs-fundamentals | Fundamental filters on statistical pairs; same per-year universe |
| pairs-zscore | Single-pair daily z-score signal; same per-year universe |
| **pairs-multi-pair** | **Books of 5 to 20 simultaneous pairs** |

## Data Source

Ceta Research (FMP financial data warehouse). Full methodology: [METHODOLOGY.md](../METHODOLOGY.md)
