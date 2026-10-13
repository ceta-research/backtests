# Revenue Acceleration

Buys companies whose revenue growth is speeding up: this year's growth rate is higher than last year's. A company going from 10% to 15% growth is accelerating. One going from 20% to 15% is decelerating, although it is still growing fast. The screen ranks accelerating companies by how much their growth rate rose and holds the top 30, after quality and size filters.

## Academic Basis

- Chan, Jegadeesh & Lakonishok (1996), "Momentum Strategies", *Journal of Finance* 51(5), 1681-1713. Past returns and past earnings surprises each predict drift in future returns, which is the underreaction case for trading on improving fundamentals.
- Lakonishok, Shleifer & Vishny (1994), "Contrarian Investment, Extrapolation, and Risk", *Journal of Finance* 49(5), 1541-1578. The opposing case: investors extrapolate past growth too far, which leaves the fastest growers overpriced.

## Signal

| Filter | Threshold | Source |
|--------|-----------|--------|
| Revenue growth, latest year against the year before | Higher (acceleration > 0) | `income_statement` FY |
| Latest revenue growth | > 5% | `income_statement` FY |
| Return on equity | > 10% | `key_metrics` FY |
| Debt to equity | 0 to 1.5 | `financial_ratios` FY |
| Market cap | Above the market's entry in `MKTCAP_THRESHOLD_MAP` (`cli_utils.py`): $1B in the US, ₹20B on NSE, ¥100B on JPX, €500M on XETRA | `key_metrics` FY |

**Sorting:** Largest acceleration first. Top 30.

**Acceleration** = growth_current - growth_prior, where growth_current is the latest fiscal year's revenue against the year before, and growth_prior is that year against the one before it. A company needs three fiscal-year revenue rows.

## Parameters

- **Rebalancing:** Annual, on April 1, executed at the next session's close. 25 periods, April 2000 to April 2025.
- **Min stocks:** 10. The portfolio holds cash for the year if fewer than 10 names pass the screen, or if fewer than 10 of those have a usable entry price (a price in the 10 days after the rebalance of at least 1.0 in local units). A cash year earns 0%. The floor is checked at entry only.
- **Max stocks:** 30. The top 30 is ranked on acceleration alone. Before the cut, prices are read only to choose between two lines of the same security. A name with no usable entry price keeps its slot and is not replaced, so an invested year holds between 10 and 30 names. The table gives the average per market.
- **Weighting:** Equal weight.
- **Transaction costs:** Size-tiered, one-way: 0.1% above $10B market cap, 0.3% for $2-10B, 0.5% below $2B, charged on entry and on exit (0.2% to 1.0% round trip). The tier thresholds are in US dollars and market caps aren't converted, so on NSE, JPX, KSC, TAI + TWO, SET and JNB, where the market-cap floor is at least 10 billion local units, every holding pays the 0.1% tier.
- **Data lag:** 45 days, counted from the fiscal year end.
- **Exits:** A holding with no price in the 10-day window after the exit date exits at its last traded price (`--exit-method ltp`, the default).
- **Listing rules:** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted (GBp) lines only. On every market, a security listed under more than one ticker (same ISIN) is held once.
- **Data checks:** Price rows where the adjusted close spikes and reverts within one or two days are dropped. A position is left out of the year's average if it enters below 1.0 in local units or posts a single-year return above 200%.
- **Benchmark:** Each market's own index, in local currency. The US is measured against SPY. South Africa has no local index in the price data and is measured against the S&P 500 in USD. China (SHZ + SHH) is measured against the SSE Composite. Every file measured against a local index also carries the S&P 500 in USD (`spy_usd`, `excess_cagr_vs_spy`) as a second, common yardstick.
- **Risk-free rate (Sharpe):** Fixed per market, from `REGIONAL_RISK_FREE_RATES` in `cli_utils.py` (2.0% US, 6.5% India, 0.1% Japan, 9.0% South Africa).

## Results (April 2000 to April 2025)

Thirteen of the 15 markets were invested in at least half of the 25 years and are reported here. Singapore and South Africa were not. They are listed further down as too thin to test.

The screen beat its own market's index in 4 of the 13: the UK (+5.92 points a year), Canada (+4.74), Sweden (+3.92) and Japan (+1.70). All four indices are price indices that leave dividends out, while the portfolio's returns include them, so each margin is overstated by roughly the index's dividend yield. The other nine trailed, among them the two measured against a benchmark that includes dividends: the US (3.55% a year against 7.33% for SPY) and Germany (3.51% against 4.51% for the DAX). Against the S&P 500 in US dollars, only Canada is ahead (+1.47 points). India (-0.21) and the UK (-0.25) are just behind it.

Ten of the 13 sat in cash for at least one year. A cash year counts as 0% against whatever the index did that year, so part of each 25-year margin is the index's own return in the cash years, not the return of the stocks. Japan is the clearest case: it was in cash from 2000 to 2004, the Nikkei 225 fell in four of those five years, and over its 20 invested years the portfolio returned 4.90% a year against 5.75% for the index, 0.86 points behind. Korea and Switzerland go the other way: both trail their index over 25 years and are ahead of it over the years they were invested. The second table under "Cash years" gives both readings for each market.

| Market | Benchmark | CAGR | Benchmark CAGR | Excess (pp) | Excess vs S&P 500 (pp) | Sharpe | Max DD | Cash years | Avg stocks |
|--------|-----------|------|----------------|-------------|------------------------|--------|--------|------------|------------|
| Canada (TSX) | TSX Composite | 8.80% | 4.05% | +4.74 | +1.47 | 0.262 | -33.98% | 1 of 25 | 22.3 |
| India (NSE) | Sensex | 7.12% | 11.49% | -4.37 | -0.21 | 0.020 | -47.57% | 7 of 25 | 22.5 |
| UK (LSE) | FTSE 100 | 7.08% | 1.15% | +5.92 | -0.25 | 0.163 | -45.88% | 0 of 25 | 17.6 |
| Sweden (STO) | OMX Stockholm 30 | 6.32% | 2.40% | +3.92 | -1.00 | 0.201 | -49.41% | 7 of 25 | 20.6 |
| Japan (JPX) | Nikkei 225 | 3.90% | 2.20% | +1.70 | -3.43 | 0.144 | -57.36% | 5 of 25 | 25.2 |
| US (NYSE + NASDAQ + AMEX) | S&P 500 (SPY) | 3.55% | 7.33% | -3.78 | -3.78 | 0.056 | -43.08% | 0 of 25 | 23.9 |
| Germany (XETRA) | DAX | 3.51% | 4.51% | -1.01 | -3.82 | 0.064 | -41.91% | 7 of 25 | 20.3 |
| China (SHZ + SHH) | SSE Composite | 1.38% | 2.51% | -1.14 | -5.95 | -0.033 | -53.02% | 0 of 25 | 23.0 |
| Taiwan (TAI + TWO) | TAIEX | 1.35% | 3.05% | -1.70 | -5.97 | 0.019 | -32.64% | 8 of 25 | 25.9 |
| Switzerland (SIX) | SMI | 1.00% | 2.08% | -1.08 | -6.33 | 0.028 | -59.87% | 11 of 25 | 17.6 |
| Korea (KSC) | KOSPI | 0.97% | 4.44% | -3.46 | -6.35 | -0.108 | -35.79% | 9 of 25 | 24.1 |
| Thailand (SET) | SET Index | -2.04% | 4.41% | -6.45 | -9.37 | -0.210 | -54.16% | 7 of 25 | 22.3 |
| Hong Kong (HKSE) | Hang Seng | -2.48% | 1.28% | -3.76 | -9.81 | -0.224 | -69.24% | 4 of 25 | 20.0 |

Returns are in local currency, after costs. Excess is computed from unrounded CAGRs, so it can differ by 0.01 from the difference of the two rounded columns. "Excess vs S&P 500" puts a local-currency return against SPY in US dollars, with no currency conversion. Avg stocks is the average number held in invested years.

- **US:** 3.55% a year against 7.33% for SPY, with a Sharpe of 0.056 against 0.253, a max drawdown of -43.08% against -39.33%, and no cash years.
- **Canada:** 8.80% a year against 4.05% for the TSX Composite, with a Sharpe of 0.262 against 0.082. It is the one market ahead of the S&P 500, by 1.47 points over 25 years and by 0.45 over its 24 invested years. It sat in cash in 2002, when the S&P 500 fell 21.34%.
- **Japan:** 3.90% a year against 2.20% for the Nikkei 225, with five cash years, 2000 to 2004. The Nikkei fell in four of them, and that is where the margin comes from: over the 20 invested years the portfolio returned 4.90% a year against 5.75%.
- **Germany:** 3.51% a year against 4.51% for the DAX, the one local index here that reinvests dividends. Seven years are in cash because fewer than 10 Germany-domiciled companies passed the screen, and the DAX fell in three of them (2000 to 2002). Over the 18 invested years the portfolio returned 4.90% a year against 7.40% for the DAX.
- **Switzerland:** 1.00% a year against 2.08% for the SMI, in cash for 11 of 25 years. With 14 invested years it stays in the table; 12 or fewer would make it too thin. Over those 14 years it returned 1.79% a year against 0.71% for the SMI, so the 25-year shortfall comes from the cash years, when the SMI returned 3.85% a year.
- **Hong Kong:** the lowest return and the deepest drawdown of the 13, -2.48% a year and -69.24%.
- **Dividends in benchmarks:** the portfolio's returns include dividends (`adjClose`). SPY and the DAX do too. The other eleven local indices are price indices, so excess against them is overstated by roughly the index's dividend yield. See METHODOLOGY.md (Benchmark Limitations).
- **Sharpe:** each market uses its own risk-free rate, so compare a Sharpe with the index's Sharpe in the same market, not across markets.

### Cash years

A year is in cash for one of two reasons, and each period record says which (`screened`, `entry_buyable`). Years are labelled by their April rebalance. The US, the UK and China were invested in all 25 years.

| Market | Screen passed fewer than 10 | Fewer than 10 buyable at entry |
|--------|-----------------------------|--------------------------------|
| India (NSE) | 2000-2006 | none |
| Japan (JPX) | 2000-2004 | none |
| Hong Kong (HKSE) | none | 2000-2003 |
| Korea (KSC) | 2000-2007, 2009 | none |
| Taiwan (TAI + TWO) | 2000-2006 | 2007 |
| Germany (XETRA) | 2000-2004, 2010, 2014 | none |
| Canada (TSX) | none | 2002 |
| Thailand (SET) | 2000-2004 | 2005, 2009 |
| Sweden (STO) | 2000-2004, 2009 | 2010 |
| Switzerland (SIX) | 2000-2004, 2010 | 2005, 2009, 2012, 2014-2015 |
| Singapore (SES) | 2000-2004, 2016 | 2005-2006, 2009-2010, 2012, 2014, 2017-2022, 2024 |
| South Africa (JNB) | 2000-2006, 2009-2010 | 2011-2012, 2018-2019, 2021 |

A cash year earns 0% and is counted in every 25-year figure above. The next table sets the 25-year excess beside the excess over the invested years only: the portfolio's returns in the years it held stocks, compounded and annualised over that many years, against the index over the same years. The second reading is not a return anyone earned. It shows how the selected stocks did when there was a book. Excess is computed before rounding, as in the results table. Singapore and South Africa are left out of it as too thin to test.

| Market | Cash years | Index in the cash years (% a year) | Excess, 25 years (pp) | Portfolio, invested years | Index, invested years | Excess, invested years (pp) | Excess vs S&P 500, 25 years (pp) | Excess vs S&P 500, invested years (pp) |
|--------|------------|------------------------------------|-----------------------|---------------------------|-----------------------|-----------------------------|----------------------------------|----------------------------------------|
| India (NSE) | 7 of 25 | +13.76 | -4.37 | 10.02% | 10.62% | -0.60 | -0.21 | 0.00 |
| Japan (JPX) | 5 of 25 | -10.86 | +1.70 | 4.90% | 5.75% | -0.86 | -3.43 | -5.31 |
| Hong Kong (HKSE) | 4 of 25 | -6.82 | -3.76 | -2.95% | 2.90% | -5.84 | -9.81 | -12.88 |
| Korea (KSC) | 9 of 25 | +12.02 | -3.46 | 1.53% | 0.40% | +1.13 | -6.35 | -7.38 |
| Taiwan (TAI + TWO) | 8 of 25 | -1.92 | -1.70 | 2.00% | 5.48% | -3.48 | -5.97 | -8.77 |
| Germany (XETRA) | 7 of 25 | -2.56 | -1.01 | 4.90% | 7.40% | -2.50 | -3.82 | -5.01 |
| Canada (TSX) | 1 of 25 | -18.78 | +4.74 | 9.18% | 5.13% | +4.05 | +1.47 | +0.45 |
| Thailand (SET) | 7 of 25 | +18.85 | -6.45 | -2.82% | -0.72% | -2.10 | -9.37 | -11.25 |
| Sweden (STO) | 7 of 25 | -1.28 | +3.92 | 8.89% | 3.87% | +5.02 | -1.00 | +0.58 |
| Switzerland (SIX) | 11 of 25 | +3.85 | -1.08 | 1.79% | 0.71% | +1.08 | -6.33 | -6.15 |

Against its own index the sign changes in three markets: Japan goes from 1.70 points ahead to 0.86 behind, Korea from 3.46 behind to 1.13 ahead, and Switzerland from 1.08 behind to 1.08 ahead. Against the S&P 500 it changes in Sweden, from 1.00 points behind to 0.58 ahead, and India is level with it over its 18 invested years. Canada stays ahead of the S&P 500 on both readings.

### Too thin to test

Singapore and South Africa were invested in fewer than half of the 25 years. Their numbers describe a handful of years, not the screen, and they are left out of the results table, of the invested-years table above and of the comparison charts.

| Market | Years in cash | Years invested | Avg stocks in invested years | Returns in the invested years |
|--------|---------------|----------------|------------------------------|-------------------------------|
| Singapore (SES) | 19 of 25 | 6 | 12.5 | 2007: -7.10%, 2008: -48.43%, 2011: -8.96%, 2013: -1.67%, 2015: -12.56%, 2023: -6.95% |
| South Africa (JNB) | 14 of 25 | 11 | 12.5 | 2007: -2.50%, 2008: -24.12%, 2013: +17.45%, 2014: +12.44%, 2015: -9.13%, 2016: +5.41%, 2017: +1.48%, 2020: +67.78%, 2022: -10.82%, 2023: +7.72%, 2024: +17.28% |

In Singapore the screen passed 10 or more names in 13 of the 19 cash years, but fewer than 10 of them had an entry price of at least 1.0. The floor is in local units, and many Singapore share prices sit below one Singapore dollar. The results files still carry a CAGR for each market (-4.12% for Singapore, 2.37% for South Africa). Each compounds mostly cash years and is not a result.

## Rerun October 2026

These results were rerun with the changes below, described in METHODOLOGY.md (Universe Construction, and Limitations: Survivorship Bias). The screen's own filters and thresholds did not change.

- **Last-traded-price exits.** A holding with no price after the exit date now exits at its last traded price instead of being dropped from the period. 20 holdings in 10 markets exited this way, 7 of them in Germany; each results file lists them under its `ltp` key.
- **Listing rules.** XETRA holds Germany-domiciled companies only, SIX Switzerland-domiciled companies only, and LSE pence-quoted lines only.
- **Same-ISIN dedup.** A security listed under more than one ticker is held once, on every market.
- **Entry-side cash rule.** The results published before this rerun predated the check that at least 10 screened names have a usable entry price. A year that used to average a few priced survivors is now a cash year.

What moved:

- **Germany** went from ahead of the DAX to 1.01 points behind it, and from no cash years to 7. Before the listing rule most of the XETRA book was foreign companies' German lines.
- **Switzerland** went from ahead of the SMI to 1.08 points behind it, and from no cash years to 11: six where fewer than 10 Switzerland-domiciled companies passed the screen, five where fewer than 10 were buyable. Over its 14 invested years it is 1.08 points ahead of the SMI, so the shortfall comes from the cash years.
- **Singapore and South Africa** became too thin to test, both through the entry-side cash rule.
- **The UK** holds 17.6 names a year on pence lines only and is now 0.25 points behind the S&P 500, where it was level.
- **Hong Kong** sits in cash in 2000-2003 and **Canada** in 2002, both through the entry-side cash rule. Canada stays ahead of both benchmarks.

## Funds-excluded diagnostic

Closed-end funds and investment trusts report investment income in the `revenue` field, so they can post a large "revenue acceleration" and enter the top 30. The published results keep them. `--exclude-funds` (off by default) reruns a market without funds and ETFs (`isFund`, `isEtf`); it does not filter on `isActivelyTrading`, which would add survivorship bias. The four runs are in `results/diagnostics/`.

| Market | As screened | Funds excluded | Difference (pp) | Excess vs S&P 500, as screened (pp) | Excess vs S&P 500, funds excluded (pp) |
|--------|-------------|----------------|-----------------|-------------------------------------|----------------------------------------|
| US (NYSE + NASDAQ + AMEX) | 3.55% | 4.27% | +0.72 | -3.78 | -3.05 |
| UK (LSE) | 7.08% | 8.24% | +1.16 | -0.25 | +0.92 |
| Canada (TSX) | 8.80% | 8.59% | -0.21 | +1.47 | +1.26 |
| Germany (XETRA) | 3.51% | 3.51% | 0.00 | -3.82 | -3.82 |

The direction is not the same everywhere: excluding funds helps in the US and the UK and costs 0.21 points in Canada. In the UK it changes the reading against the S&P 500, from 0.25 points behind to 0.92 ahead, because investment trusts are pence-quoted and stay in the book under the listing rule. Germany's funds-excluded file is identical to its main file: no fund or ETF reaches the Germany-domiciled top 30 in any year.

## Known limits

- The 45-day lag is counted from the fiscal year end, not from the filing date. Many companies publish annual results later than that, so an April 1 rebalance can use a December year-end report before it was public.
- The size floor compares `key_metrics.marketCap`, which is in the company's reporting currency, with a floor set in the listing currency.
- The entry-price floor of 1.0 is in local units. It is a bad-price guard, but in Singapore dollars and Hong Kong dollars it also removes real low-priced shares.
- The three revenue rows are the three most recent in the window. Their spacing is not checked, so a stub period can produce a large growth rate.
- Fiscal-year rows up to five years old can still be used for revenue and debt to equity.
- ASX and SAO are not run: their adjusted closes carry split and consolidation artifacts. See `DATA_QUALITY_ISSUES.md`.

## Usage

Run from the repository root, with `CR_API_KEY` or `TS_API_KEY` set. `--exit-method` defaults to `ltp`. The listing rules need no flag: they apply whenever XETRA, SIX or LSE is run as its own market. A mixed leg such as `--exchange XETRA,LSE` gets no listing rule.

The committed files were produced one market at a time, like this:

```bash
# One market, written to the committed results file
python3 revenue-accel/backtest.py --exchange XETRA --output revenue-accel/results/returns_XETRA.json

# Funds-excluded diagnostic for one market (US, UK, Canada and Germany are committed)
python3 revenue-accel/backtest.py --exchange LSE --exclude-funds \
  --output revenue-accel/results/diagnostics/returns_nofunds_LSE.json

# Fold the 15 per-market files into results/exchange_comparison.json, then check it
python3 scripts/rebuild_comparison.py revenue-accel
python3 scripts/rebuild_comparison.py revenue-accel --check

# Charts from results/exchange_comparison.json, written to revenue-accel/charts/
python3 revenue-accel/generate_charts.py

# The old exit rule (doesn't reproduce old results: the listing rules, dedup and entry-side cash rule still apply)
python3 revenue-accel/backtest.py --exchange LSE --exit-method drop
```

Per-market runs that reproduce the committed files, each with `--output revenue-accel/results/<output file>`:

| Market | `--exchange` | Output file | Listing rule |
|--------|--------------|-------------|--------------|
| US (NYSE + NASDAQ + AMEX) | `NYSE,NASDAQ,AMEX` | `returns_NYSE_NASDAQ_AMEX.json` |  |
| India (NSE) | `NSE` | `returns_NSE.json` |  |
| Japan (JPX) | `JPX` | `returns_JPX.json` |  |
| UK (LSE) | `LSE` | `returns_LSE.json` | GBp lines only |
| China (SHZ + SHH) | `SHZ,SHH` | `returns_SHZ_SHH.json` |  |
| Hong Kong (HKSE) | `HKSE` | `returns_HKSE.json` |  |
| Korea (KSC) | `KSC` | `returns_KSC.json` |  |
| Taiwan (TAI + TWO) | `TAI,TWO` | `returns_TAI_TWO.json` |  |
| Germany (XETRA) | `XETRA` | `returns_XETRA.json` | Germany-domiciled only |
| Canada (TSX) | `TSX` | `returns_TSX.json` |  |
| Thailand (SET) | `SET` | `returns_SET.json` |  |
| Sweden (STO) | `STO` | `returns_STO.json` |  |
| Switzerland (SIX) | `SIX` | `returns_SIX.json` | Switzerland-domiciled only |
| Singapore (SES) | `SES` | `returns_SES.json` |  |
| South Africa (JNB) | `JNB` | `returns_JNB.json` |  |

Use `--exchange` as shown. `--preset canada` adds TSXV, and `--preset us`, `--preset taiwan` and `--preset southafrica` write a different universe name into the file.

`--global --output revenue-accel/results/exchange_comparison.json` runs the same 15 markets in one loop and writes the same file names. A market that fails inside the loop is recorded as an error in the combined file while its old per-market file stays on disk, so run `scripts/rebuild_comparison.py revenue-accel --check` afterwards. Don't combine `--global` with `--exclude-funds` and an output under `results/`: the loop writes `returns_<KEY>.json`, over the main files.

## Data Requirements

- `income_statement` (FY): revenue, three fiscal years per company
- `key_metrics` (FY): returnOnEquity, marketCap
- `financial_ratios` (FY): debtToEquityRatio
- `stock_eod`: adjClose after each rebalance date, and the last traded price for exits
- `profile`: exchange, country (XETRA and SIX rule), currency (LSE rule, dedup tie-break), isin and isActivelyTrading (dedup), isFund and isEtf (diagnostic only)

## Files

| File | Purpose |
|------|---------|
| `backtest.py` | Historical backtest (2000-2025) |
| `screen.py` | Current stock screen. Not maintained with this rerun: it has no listing rules or dedup, and it stops on a syntax error at line 169 |
| `generate_charts.py` | Charts from `results/exchange_comparison.json`. Markets that are too thin to test are left off the comparison charts and named in their footer |
| `README.md` | This file |
| `results/` | `returns_<market>.json` per market and `exchange_comparison.json` |
| `results/diagnostics/` | `returns_nofunds_<market>.json`, the four funds-excluded runs |
| `charts/` | Output of `generate_charts.py`. Not committed, so run it locally |
